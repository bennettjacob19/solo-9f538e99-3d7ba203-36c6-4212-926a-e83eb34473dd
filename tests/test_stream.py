"""End-to-end tests against a real uvicorn server.

Covers the three hard requirements:
  * disconnect + resume replays exactly the lines after the last shown one,
  * a viewer that falls out of the 200-line window is told the missing range,
  * lines arriving during replay are neither lost nor duplicated,
  * a stalled SSE reader does not hold up log collection.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import httpx
import pytest

from app.buildlog import BuildRunner
from app.main import create_app
from conftest import SSEClient, wait_for

pytestmark = pytest.mark.asyncio

HERE = Path(__file__).resolve().parent


@pytest.fixture
async def make_server():
    """Factory: start an app whose sample build is a synthetic script."""
    started = []

    async def _make(capacity: int = 20, env: dict | None = None, **kwargs):
        import uvicorn

        old_env = {}
        for key, value in (env or {}).items():
            old_env[key] = os.environ.get(key)
            os.environ[key] = str(value)
        runner = BuildRunner(
            capacity=capacity,
            sample_cmd=[sys.executable, "-u", str(HERE / "synthetic_build.py")],
            **kwargs,
        )
        app = create_app(runner)
        config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="on")
        srv = uvicorn.Server(config)
        task = asyncio.create_task(srv.serve())
        await wait_for(lambda: srv.started, timeout=10)
        port = srv.servers[0].sockets[0].getsockname()[1]
        entry = {
            "base": f"http://127.0.0.1:{port}",
            "runner": runner,
            "srv": srv,
            "task": task,
            "env": old_env,
        }
        started.append(entry)
        return entry

    try:
        yield _make
    finally:
        for entry in started:
            await entry["runner"].shutdown()
            entry["srv"].should_exit = True
            try:
                await asyncio.wait_for(entry["task"], timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):  # pragma: no cover
                entry["task"].cancel()
            for key, value in entry["env"].items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


async def collect_until(client: SSEClient, stop, limit: int = 4000):
    """Read SSE events until stop(events) is true; return all events."""
    events = []
    while not stop(events):
        events.append(await client.next_event())
        if len(events) > limit:  # pragma: no cover - guard
            raise AssertionError("too many SSE events")
    return events


def flatten_lines(events):
    out = []
    for name, _, data in events:
        if name == "lines":
            out.extend(data["lines"])
    return out


def ended(events) -> bool:
    """The viewer has seen the tail: a line ending in 'done', or a terminal status."""
    for name, _, data in events:
        if name == "lines" and any(line["text"].endswith("done") for line in data["lines"]):
            return True
        if name == "status" and data.get("status") != "running":
            return True
    return False


async def wait_status(base: str, wanted: str, timeout: float = 20.0) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    async with httpx.AsyncClient() as client:
        while True:
            payload = (await client.get(f"{base}/api/status")).json()
            if payload["status"] == wanted:
                return payload
            if asyncio.get_running_loop().time() > deadline:  # pragma: no cover
                raise AssertionError(f"status stuck at {payload['status']}")
            await asyncio.sleep(0.02)


async def test_stream_replays_from_zero_with_build_output(make_server):
    srv = await make_server(capacity=50, env={"SYNTH_LINES": 12, "SYNTH_DELAY": 0.005})
    base = srv["base"]

    async with httpx.AsyncClient() as client:
        await client.post(f"{base}/api/build/start")
        await wait_status(base, "succeeded")

    async with SSEClient(f"{base}/api/logs/stream?after=0") as stream:
        events = await collect_until(stream, ended)

    hello = events[0]
    assert hello[0] == "hello"
    lines = flatten_lines(events)
    seqs = [line["seq"] for line in lines]
    assert seqs == list(range(1, len(lines) + 1))  # dense, ordered, no duplicates
    texts = [line["text"] for line in lines]
    assert any("line 1" == t for t in texts)
    assert any("line 12" == t for t in texts)
    assert any(line["stream"] == "stderr" and "warn line 10" in line["text"] for line in lines)


async def test_disconnect_and_resume_continues_after_last_shown_line(make_server):
    srv = await make_server(capacity=100, env={"SYNTH_LINES": 40, "SYNTH_DELAY": 0.05})
    base = srv["base"]

    async with httpx.AsyncClient() as client:
        await client.post(f"{base}/api/build/start")

    # First viewer watches a few lines, then "drops offline".
    async with SSEClient(f"{base}/api/logs/stream?after=0") as stream:
        events = await collect_until(stream, lambda evs: len(flatten_lines(evs)) >= 5)
    first = flatten_lines(events)
    last_seen = first[-1]["seq"]
    assert [line["seq"] for line in first] == list(range(1, last_seen + 1))

    # Wait while more lines are produced with nobody watching; collection must
    # continue server-side.
    await asyncio.sleep(0.6)
    async with httpx.AsyncClient() as client:
        tail = (await client.get(f"{base}/api/logs/tail", params={"after": last_seen})).json()
    assert tail["lines"], "logs must be collected while nobody is watching"
    assert tail["lines"][0]["seq"] == last_seen + 1

    # Reconnect exactly like the page does: resume after the last shown line.
    async with SSEClient(f"{base}/api/logs/stream?after={last_seen}") as stream:
        events2 = await collect_until(stream, ended)
    second = flatten_lines(events2)
    seqs = [line["seq"] for line in second]
    assert seqs[0] == last_seen + 1
    assert seqs == list(range(last_seen + 1, seqs[-1] + 1))
    # The union is every line exactly once.
    all_seqs = [line["seq"] for line in first + second]
    assert all_seqs == list(range(1, max(all_seqs) + 1))
    assert len(all_seqs) == len(set(all_seqs))


async def test_resume_during_active_build_has_no_gap_no_dup(make_server):
    """Reconnect while the build is still emitting: replay + live must splice."""
    srv = await make_server(capacity=100, env={"SYNTH_LINES": 80, "SYNTH_DELAY": 0.02})
    base = srv["base"]

    async with httpx.AsyncClient() as client:
        await client.post(f"{base}/api/build/start")

    async with SSEClient(f"{base}/api/logs/stream?after=0") as stream:
        events = await collect_until(stream, lambda evs: len(flatten_lines(evs)) >= 3)
    first = flatten_lines(events)
    cursor = first[-1]["seq"]

    # Immediately reconnect; the build keeps emitting during the handoff.
    async with SSEClient(f"{base}/api/logs/stream?after={cursor}") as stream:
        events2 = await collect_until(stream, ended)
    second = flatten_lines(events2)
    assert not any(name == "gap" for name, _, _ in events2), "buffer was big enough; no gap expected"
    seqs = [line["seq"] for line in second]
    assert seqs[0] == cursor + 1
    assert seqs == list(range(cursor + 1, seqs[-1] + 1))
    union = [line["seq"] for line in first + second]
    assert union == list(range(1, union[-1] + 1))
    assert len(union) == len(set(union))


async def test_cursor_can_be_sent_as_last_event_id_header(make_server):
    srv = await make_server(capacity=100, env={"SYNTH_LINES": 30, "SYNTH_DELAY": 0.005})
    base = srv["base"]
    async with httpx.AsyncClient() as client:
        await client.post(f"{base}/api/build/start")
        await wait_status(base, "succeeded")

    async with SSEClient(f"{base}/api/logs/stream", headers={"Last-Event-ID": "4"}) as stream:
        hello = await stream.next_event()
    assert hello[0] == "hello"
    assert hello[2]["cursor"] == 4

    async with httpx.AsyncClient() as client:
        tail = (await client.get(f"{base}/api/logs/tail", params={"after": 4, "limit": 3})).json()
    assert [line["seq"] for line in tail["lines"]] == [5, 6, 7]
    assert tail["has_more"] is True


async def test_slow_reader_gets_gap_range_then_oldest_retained(make_server):
    """Stuck viewer + tiny buffer: explicit missing range, then resume."""
    srv = await make_server(capacity=5, env={"SYNTH_LINES": 40, "SYNTH_DELAY": 0.005})
    base = srv["base"]

    async with httpx.AsyncClient() as client:
        await client.post(f"{base}/api/build/start")
        status = await wait_status(base, "succeeded")

    last = status["last_seq"]
    first_retained = status["first_seq"]
    assert last - first_retained + 1 == 5
    assert first_retained > 1  # eviction definitely happened

    async with SSEClient(f"{base}/api/logs/stream?after=1") as stream:
        events = await collect_until(
            stream,
            lambda evs: len(flatten_lines(evs)) >= 5,
        )

    gap_events = [data for name, _, data in events if name == "gap"]
    assert len(gap_events) == 1
    gap = gap_events[0]
    assert (gap["first"], gap["last"]) == (2, first_retained - 1)

    lines = flatten_lines(events)
    assert lines[0]["seq"] == first_retained
    seqs = [line["seq"] for line in lines]
    assert seqs == list(range(first_retained, first_retained + len(seqs)))
    # The viewer had already displayed line 1; adding the explicitly reported
    # gap and the resumed lines covers 1..last exactly, with no overlap.
    covered = {1} | set(seqs) | set(range(gap["first"], gap["last"] + 1))
    assert covered == set(range(1, last + 1))
    assert len(covered) == len(set(seqs)) + (gap["last"] - gap["first"] + 1) + 1


async def test_gap_is_reported_once_per_reader(make_server):
    srv = await make_server(capacity=3, env={"SYNTH_LINES": 20, "SYNTH_DELAY": 0.0})
    base = srv["base"]
    async with httpx.AsyncClient() as client:
        await client.post(f"{base}/api/build/start")
        await wait_status(base, "succeeded")

    async with SSEClient(f"{base}/api/logs/stream?after=0") as stream:
        events = await collect_until(
            stream, lambda evs: any(name == "status" and data["status"] == "succeeded" for name, _, data in evs)
        )
    assert sum(1 for name, _, _ in events if name == "gap") == 1


async def test_slow_reader_does_not_stall_collection_or_other_viewers(make_server):
    """A viewer that stops reading must not block the build or a fast viewer."""
    srv = await make_server(capacity=40, env={"SYNTH_LINES": 120, "SYNTH_DELAY": 0.01})
    base = srv["base"]

    # Open a stream but never drain it.  The server-side generator is paused by
    # the transport; collection must keep running regardless.
    stuck = httpx.AsyncClient(timeout=httpx.Timeout(60.0, read=60.0))
    client = await stuck.__aenter__()
    req = client.stream("GET", f"{base}/api/logs/stream?after=0")
    await req.__aenter__()  # attached, but deliberately never read from
    try:
        async with httpx.AsyncClient() as http:
            await http.post(f"{base}/api/build/start")
            started = asyncio.get_running_loop().time()
            await wait_status(base, "succeeded", timeout=25)
            elapsed = asyncio.get_running_loop().time() - started
            # ~120 lines at 10ms is ~1.2s; the stuck viewer must not add
            # back-pressure that stretches collection.
            assert elapsed < 12, f"build took {elapsed:.1f}s with a stuck viewer attached"

            # A second, well-behaved viewer still gets the tail promptly.
            async with SSEClient(f"{base}/api/logs/stream?after=100") as fast:
                ev = await fast.next_event()
                assert ev[0] == "hello"
                data = await fast.next_event()
                assert data[0] in {"gap", "lines", "status"}
    finally:
        await req.__aexit__(None, None, None)
        await stuck.__aexit__(None, None, None)


async def test_concurrent_viewers_get_identical_line_content(make_server):
    srv = await make_server(capacity=50, env={"SYNTH_LINES": 25, "SYNTH_DELAY": 0.004})
    base = srv["base"]
    async with httpx.AsyncClient() as client:
        await client.post(f"{base}/api/build/start")

    async def watch():
        async with SSEClient(f"{base}/api/logs/stream?after=0") as stream:
            events = await collect_until(stream, ended)
        return [(line["seq"], line["text"]) for line in flatten_lines(events)]

    a, b = await asyncio.gather(watch(), watch())
    assert a == b
    assert [seq for seq, _ in a] == list(range(1, len(a) + 1))


async def test_start_conflicts_and_cancel(make_server):
    srv = await make_server(capacity=50, env={"SYNTH_LINES": 300, "SYNTH_DELAY": 0.05})
    base = srv["base"]
    async with httpx.AsyncClient() as client:
        first = await client.post(f"{base}/api/build/start")
        assert first.status_code == 202
        conflict = await client.post(f"{base}/api/build/start")
        assert conflict.status_code == 409
        assert "already running" in conflict.json()["detail"]

        cancelled = await client.post(f"{base}/api/build/cancel")
        assert cancelled.json()["cancelled"] is True
        assert cancelled.json()["status"] == "cancelled"

        status = await wait_status(base, "cancelled")
        assert status["exit_code"] is None


async def test_failed_build_reports_exit_code(make_server):
    srv = await make_server(capacity=50, env={"SYNTH_LINES": 5, "SYNTH_DELAY": 0.0, "SYNTH_FAIL": 1})
    base = srv["base"]
    async with httpx.AsyncClient() as client:
        await client.post(f"{base}/api/build/start")
        status = await wait_status(base, "failed")
        assert status["exit_code"] == 7
        tail = (await client.get(f"{base}/api/logs/tail", params={"after": 0})).json()
    texts = [line["text"] for line in tail["lines"]]
    assert any("failed" in t for t in texts)


async def test_partial_line_without_newline_is_flushed(make_server):
    srv = await make_server(
        capacity=50, env={"SYNTH_LINES": 3, "SYNTH_DELAY": 0.0, "SYNTH_PARTIAL": 1}
    )
    base = srv["base"]
    async with httpx.AsyncClient() as client:
        await client.post(f"{base}/api/build/start")
        await wait_status(base, "succeeded")
        tail = (await client.get(f"{base}/api/logs/tail")).json()
    texts = [line["text"] for line in tail["lines"]]
    assert "partial tail without newline" in texts
    # ...and the unterminated line arrives before the build summary.
    assert texts.index("partial tail without newline") < len(texts) - 1


async def test_invalid_cursor_rejected(make_server):
    srv = await make_server(capacity=10)
    base = srv["base"]
    async with httpx.AsyncClient() as client:
        bad = await client.get(f"{base}/api/logs/stream?after=nope")
        assert bad.status_code == 400
        negative = await client.get(f"{base}/api/logs/stream?after=-3")
        assert negative.status_code == 400


async def test_stale_epoch_resumes_at_oldest_retained_line(make_server):
    """After a restart the client's old cursor may be ahead of the new buffer."""
    srv = await make_server(capacity=10, env={"SYNTH_LINES": 25, "SYNTH_DELAY": 0.0})
    base = srv["base"]
    async with httpx.AsyncClient() as client:
        await client.post(f"{base}/api/build/start")
        status = await wait_status(base, "succeeded")

    async with SSEClient(
        f"{base}/api/logs/stream",
        headers={"Last-Event-ID": str(status["last_seq"] + 500)},
    ) as stream:
        hello = await stream.next_event()
    assert hello[0] == "hello"
    assert hello[2]["cursor"] == status["first_seq"] - 1


async def test_resume_with_matching_epoch_keeps_cursor(make_server):
    srv = await make_server(capacity=50, env={"SYNTH_LINES": 15, "SYNTH_DELAY": 0.0})
    base = srv["base"]
    async with httpx.AsyncClient() as client:
        await client.post(f"{base}/api/build/start")
        status = await wait_status(base, "succeeded")
        epoch = status["session_id"]

    async with SSEClient(f"{base}/api/logs/stream?after=5&epoch={epoch}") as stream:
        hello = await stream.next_event()
    assert hello[2]["cursor"] == 5


async def test_index_and_health(make_server):
    srv = await make_server(capacity=10)
    base = srv["base"]
    async with httpx.AsyncClient() as client:
        index = await client.get(f"{base}/")
        assert index.status_code == 200
        assert "构建日志" in index.text
        css = await client.get(f"{base}/static/style.css")
        assert css.status_code == 200
        js = await client.get(f"{base}/static/app.js")
        assert js.status_code == 200
        assert (await client.get(f"{base}/healthz")).json() == {"ok": True}
