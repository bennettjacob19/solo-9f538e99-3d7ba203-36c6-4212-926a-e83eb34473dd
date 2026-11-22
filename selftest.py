#!/usr/bin/env python3
"""Headless self-test of the deliverable.

Starts the real app, drives a sample build, and exercises the two behaviours
that are hard to get right:

  1. 掉线续看: a viewer disconnects mid-build, the build keeps producing with
     nobody watching, then the viewer reconnects from its last line and must
     see every remaining line exactly once, in order.
  2. 缓冲溢出: a viewer falls more than one buffer behind, and must be told the
     exact missing line range, then continue from the oldest retained line.

Exit code 0 means both scenarios behaved as specified.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import httpx  # noqa: E402
import uvicorn  # noqa: E402

from app.buildlog import BuildRunner  # noqa: E402
from app.main import create_app  # noqa: E402
from tests.conftest import SSEClient, wait_for  # noqa: E402

OK = "\033[32mok\033[0m"
BAD = "\033[31mFAILED\033[0m"
failures: list[str] = []


def check(condition: bool, message: str) -> None:
    if condition:
        print(f"  {OK}  {message}")
    else:
        print(f"  {BAD}  {message}")
        failures.append(message)


async def read_lines(stream: SSEClient, stop, timeout: float = 25.0):
    """Collect SSE events until stop(events) is true."""
    events = []
    while not stop(events):
        events.append(await stream.next_event(timeout=timeout))
    return events


def line_seqs(events):
    out = []
    for name, _, data in events:
        if name == "lines":
            out.extend(line["seq"] for line in data["lines"])
    return out


def ended(events) -> bool:
    for name, _, data in events:
        if name == "lines" and any(l["text"].endswith("done") for l in data["lines"]):
            return True
        if name == "status" and data.get("status") in {"succeeded", "failed", "cancelled"}:
            return True
    return False


async def start_server(capacity: int, env: dict[str, str]):
    script = str(ROOT / "tests" / "synthetic_build.py")
    saved = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    runner = BuildRunner(capacity=capacity, sample_cmd=[sys.executable, "-u", script])
    app = create_app(runner)
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="on")
    )
    task = asyncio.create_task(server.serve())
    await wait_for(lambda: server.started, timeout=10)
    port = server.servers[0].sockets[0].getsockname()[1]

    async def stop():
        await runner.shutdown()
        server.should_exit = True
        try:
            await asyncio.wait_for(task, timeout=10)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            task.cancel()
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    return runner, f"http://127.0.0.1:{port}", stop


async def scenario_resume() -> None:
    print("\n[1] 断线续看 (disconnect → server keeps collecting → resume)")
    runner, base, stop = await start_server(200, {"SYNTH_LINES": "60", "SYNTH_DELAY": "0.03"})
    try:
        async with httpx.AsyncClient() as http:
            await http.post(f"{base}/api/build/start")

        async with SSEClient(f"{base}/api/logs/stream?after=0") as stream:
            first = await read_lines(stream, lambda evs: len(line_seqs(evs)) >= 6)
        seen = line_seqs(first)
        cursor = seen[-1]
        print(f"     first viewer watched lines 1..{cursor}, then dropped")

        # Nobody watching: collection must continue.
        await asyncio.sleep(1.0)
        status = (await httpx.AsyncClient().get(f"{base}/api/status")).json()
        check(status["last_seq"] > cursor, f"build kept collecting offline (tail={status['last_seq']} > {cursor})")
        check(status["status"] == "running", "build is still running while no viewer is attached")

        async with SSEClient(f"{base}/api/logs/stream?after={cursor}") as stream:
            second = await read_lines(stream, ended)
        resumed = line_seqs(second)
        check(bool(resumed) and resumed[0] == cursor + 1, f"resume starts at line {cursor + 1}")
        check(resumed == list(range(cursor + 1, cursor + 1 + len(resumed))), "resumed lines are dense and ordered")
        check(not any(name == "gap" for name, _, _ in second), "no gap reported when the buffer was big enough")
        union = seen + resumed
        check(union == list(range(1, len(union) + 1)), "each line shown exactly once across the reconnect")
    finally:
        await stop()


async def scenario_overflow() -> None:
    print("\n[2] 缓冲溢出 (viewer falls behind a small ring)")
    runner, base, stop = await start_server(200, {"SYNTH_LINES": "260", "SYNTH_DELAY": "0.0"})
    try:
        async with httpx.AsyncClient() as client:
            await client.post(f"{base}/api/build/start")
            status = None
            while True:
                status = (await client.get(f"{base}/api/status")).json()
                if status["status"] == "succeeded":
                    break
                await asyncio.sleep(0.05)
        last, first_retained = status["last_seq"], status["first_seq"]
        check(last - first_retained + 1 == 200, f"server retains exactly 200 lines ({first_retained}..{last})")
        check(first_retained > 1, "older lines were evicted")

        async with SSEClient(f"{base}/api/logs/stream?after=1") as stream:
            events = await read_lines(stream, lambda evs: len(line_seqs(evs)) >= 200)
        gaps = [data for name, _, data in events if name == "gap"]
        check(len(gaps) == 1, "exactly one gap notice is emitted")
        gap = gaps[0]
        check(
            (gap["first"], gap["last"]) == (2, first_retained - 1),
            f"gap flags the exact missing range 2..{first_retained - 1}",
        )
        resumed = line_seqs(events)
        check(resumed[0] == first_retained, f"playback continues at the oldest retained line {first_retained}")
        check(resumed == list(range(first_retained, last + 1)), "retained lines are dense and ordered")
        covered = {1} | set(resumed) | set(range(gap["first"], gap["last"] + 1))
        check(covered == set(range(1, last + 1)), "shown+flagged ranges cover every line exactly once")
    finally:
        await stop()


async def scenario_slow_viewer() -> None:
    print("\n[3] 慢连接不拖住采集")
    runner, base, stop = await start_server(40, {"SYNTH_LINES": "120", "SYNTH_DELAY": "0.01"})
    try:
        stuck_client = httpx.AsyncClient(timeout=httpx.Timeout(60.0, read=60.0))
        client = await stuck_client.__aenter__()
        request = client.stream("GET", f"{base}/api/logs/stream?after=0")
        await request.__aenter__()  # attach but never read from the response

        async with httpx.AsyncClient() as http:
            await http.post(f"{base}/api/build/start")
            started = asyncio.get_running_loop().time()
            while True:
                status = (await http.get(f"{base}/api/status")).json()
                if status["status"] == "succeeded":
                    break
                if asyncio.get_running_loop().time() - started > 25:
                    break
                await asyncio.sleep(0.05)
            elapsed = asyncio.get_running_loop().time() - started
        check(status["status"] == "succeeded", "build finished despite a stalled viewer")
        check(elapsed < 12, f"collection was not slowed down (took {elapsed:.1f}s)")
        async with SSEClient(f"{base}/api/logs/stream?after=0") as fast:
            hello = await fast.next_event()
            check(hello[0] == "hello", "a second viewer still connects immediately")
            nxt = await fast.next_event()
            check(nxt[0] in {"gap", "lines", "status"}, "and receives log data")
        await request.__aexit__(None, None, None)
        await stuck_client.__aexit__(None, None, None)
    finally:
        await stop()


async def main() -> int:
    print("self-test: build log viewer")
    await scenario_resume()
    await scenario_overflow()
    await scenario_slow_viewer()
    print()
    if failures:
        print(f"RESULT: {len(failures)} check(s) failed")
        return 1
    print("RESULT: all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
