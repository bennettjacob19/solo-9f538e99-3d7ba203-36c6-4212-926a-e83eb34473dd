"""End-to-end tests for test reports and failed-case reruns.

Runs the real sample build + sample test runner as subprocesses against a
live uvicorn server.  Timing knobs are environment variables read by the
*subprocesses at spawn time*, so a test can change them between actions:
a slow rerun keeps its delay even after the env is reset for a fast build.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import httpx
import pytest

from app.buildlog import BuildRunner
from app.main import create_app
from app.reports import Report, ReportManager, RerunRecord
from conftest import wait_for

pytestmark = pytest.mark.asyncio

FAST = {
    "SAMPLE_BUILD_STEPS": "3",
    "SAMPLE_BUILD_DELAY": "0.02",
    "SAMPLE_TEST_DELAY": "0.05",
}


@pytest.fixture
def env_vars():
    """Set subprocess-timing env vars; restore everything afterwards."""
    saved: dict[str, str | None] = {}

    def set(**kwargs):
        for key, value in kwargs.items():
            if key not in saved:
                saved[key] = os.environ.get(key)
            os.environ[key] = str(value)

    yield set
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


@pytest.fixture
async def make_server(tmp_path):
    """Factory: app with the real sample build and a tmp artifacts dir."""
    started = []

    async def _make(capacity: int = 200):
        import uvicorn

        runner = BuildRunner(capacity=capacity, artifacts_dir=str(tmp_path / "artifacts"))
        app = create_app(runner)
        config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="on")
        srv = uvicorn.Server(config)
        task = asyncio.create_task(srv.serve())
        await wait_for(lambda: srv.started, timeout=10)
        port = srv.servers[0].sockets[0].getsockname()[1]
        entry = {"base": f"http://127.0.0.1:{port}", "runner": runner, "srv": srv, "task": task}
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


async def wait_report(base: str, build_id: int, pred, timeout: float = 25.0) -> dict:
    """Poll GET /api/reports/<id> until pred(payload) is true."""
    deadline = asyncio.get_running_loop().time() + timeout
    last = None
    async with httpx.AsyncClient() as client:
        while True:
            res = await client.get(f"{base}/api/reports/{build_id}")
            if res.status_code == 200:
                last = res.json()
                if pred(last):
                    return last
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"report {build_id} did not reach expected state: {last}")
            await asyncio.sleep(0.05)


async def run_build(base: str, config: str, build_id: int) -> dict:
    """Start a build and wait for its report to appear."""
    async with httpx.AsyncClient() as client:
        res = await client.post(f"{base}/api/build/start", json={"config": config})
        assert res.status_code == 202, res.text
    return await wait_report(base, build_id, lambda r: True)


def case(report: dict, name: str) -> dict:
    return next(c for c in report["cases"] if c["name"] == name)


async def test_build_produces_report_with_per_case_results(make_server, env_vars, tmp_path):
    env_vars(**FAST)
    srv = await make_server()
    base = srv["base"]

    report = await run_build(base, "debug", 1)
    assert report["config"] == "debug"
    assert report["verdict"] == "failed"
    assert report["counts"] == {"total": 5, "passed": 4, "failed": 1}
    flaky = case(report, "test_flaky_cache")
    assert flaky["initial"]["status"] == "fail"
    assert flaky["current"]["status"] == "fail"
    assert flaky["current"]["source"] == "initial"
    assert case(report, "test_checksum")["current"]["status"] == "pass"

    # Artifacts really exist on disk for this build number.
    build_dir = tmp_path / "artifacts" / "build-1"
    assert (build_dir / "app.bin").is_file()
    meta = json.loads((build_dir / "meta.json").read_text())
    assert meta["config"] == "debug"

    # Status payload advertises the chosen config and the two choices.
    async with httpx.AsyncClient() as client:
        status = (await client.get(f"{base}/api/status")).json()
    assert status["config"] == "debug"
    assert status["configs"] == ["debug", "release"]


async def test_unknown_config_rejected(make_server, env_vars):
    env_vars(**FAST)
    srv = await make_server()
    async with httpx.AsyncClient() as client:
        res = await client.post(f"{srv['base']}/api/build/start", json={"config": "nope"})
    assert res.status_code == 400


async def test_rerun_only_failed_cases_flips_verdict_and_keeps_initial(make_server, env_vars):
    env_vars(**FAST)
    srv = await make_server()
    base = srv["base"]
    await run_build(base, "debug", 1)

    async with httpx.AsyncClient() as client:
        res = await client.post(f"{base}/api/reports/1/rerun")
        assert res.status_code == 202, res.text
        payload = res.json()
    # Only the currently failing case is in the batch.
    assert payload["active_rerun"]["cases"] == ["test_flaky_cache"]

    report = await wait_report(base, 1, lambda r: r["verdict"] == "passed")
    flaky = case(report, "test_flaky_cache")
    assert flaky["current"]["status"] == "pass"
    assert flaky["current"]["source"] == "rerun#1"
    # The initial failure record is kept.
    assert flaky["initial"]["status"] == "fail"
    assert report["reruns"][0]["status"] == "completed"
    assert report["reruns"][0]["exit_code"] == 0

    # Nothing left to rerun.
    async with httpx.AsyncClient() as client:
        again = await client.post(f"{base}/api/reports/1/rerun")
    assert again.status_code == 400


async def test_release_config_keeps_persistent_failure_after_rerun(make_server, env_vars):
    env_vars(**FAST)
    srv = await make_server()
    base = srv["base"]
    report = await run_build(base, "release", 1)
    failed = {c["name"] for c in report["cases"] if c["current"]["status"] == "fail"}
    assert failed == {"test_debug_symbols", "test_flaky_cache"}

    async with httpx.AsyncClient() as client:
        res = await client.post(f"{base}/api/reports/1/rerun")
        assert res.status_code == 202
    report = await wait_report(base, 1, lambda r: r["reruns"] and r["reruns"][0]["status"] == "completed")
    # Flaky fixed, real (config-dependent) failure remains -> verdict stays failed.
    assert report["verdict"] == "failed"
    assert case(report, "test_flaky_cache")["current"]["status"] == "pass"
    assert case(report, "test_debug_symbols")["current"]["status"] == "fail"
    assert case(report, "test_debug_symbols")["current"]["source"] == "rerun#1"


async def test_second_rerun_while_running_conflicts(make_server, env_vars):
    env_vars(**FAST)
    srv = await make_server()
    base = srv["base"]
    await run_build(base, "debug", 1)

    env_vars(SAMPLE_TEST_DELAY="1.0")  # slow rerun so the second attempt overlaps
    async with httpx.AsyncClient() as client:
        first = await client.post(f"{base}/api/reports/1/rerun")
        assert first.status_code == 202
        conflict = await client.post(f"{base}/api/reports/1/rerun")
        assert conflict.status_code == 409
        # A different (unknown) report is a 404, not a 409.
        missing = await client.post(f"{base}/api/reports/99/rerun")
        assert missing.status_code == 404
        # Clean up: interrupt the slow rerun.
        cancelled = await client.post(f"{base}/api/reports/1/rerun/cancel")
        assert cancelled.json()["cancelled"] is True


async def test_rerun_interleaved_with_new_build_uses_original_artifacts(
    make_server, env_vars, tmp_path
):
    env_vars(**FAST)
    srv = await make_server()
    base = srv["base"]
    await run_build(base, "debug", 1)

    # Slow rerun of build #1's flaky case; it spawns with the slow delay...
    env_vars(SAMPLE_TEST_DELAY="1.0")
    async with httpx.AsyncClient() as client:
        res = await client.post(f"{base}/api/reports/1/rerun")
        assert res.status_code == 202

    # ...then switch config and run a whole new build while the rerun runs.
    env_vars(SAMPLE_TEST_DELAY="0.05", SAMPLE_BUILD_STEPS="2")
    async with httpx.AsyncClient() as client:
        res = await client.post(f"{base}/api/build/start", json={"config": "release"})
        assert res.status_code == 202, "new build must start while a rerun is in flight"

    # The rerun is still in flight (or just finishing) and belongs to build #1.
    async with httpx.AsyncClient() as client:
        mid = (await client.get(f"{base}/api/reports/1")).json()
    assert mid["verdict"] == "failed"  # not yet committed while incomplete

    report2 = await wait_report(base, 2, lambda r: True)
    report1 = await wait_report(base, 1, lambda r: r["verdict"] == "passed")

    # Rerun results went only to the original report.
    flaky1 = case(report1, "test_flaky_cache")
    assert flaky1["current"]["status"] == "pass"
    assert flaky1["current"]["source"] == "rerun#1"
    assert flaky1["initial"]["status"] == "fail"

    # The new build has its own independent report, untouched by the rerun.
    assert report2["config"] == "release"
    assert report2["verdict"] == "failed"
    flaky2 = case(report2, "test_flaky_cache")
    assert flaky2["current"]["status"] == "fail"
    assert flaky2["current"]["source"] == "initial"

    # The rerun wrote into build #1's artifact dir; build #2 has no rerun files.
    assert (tmp_path / "artifacts" / "build-1" / "rerun-1.json").is_file()
    assert not list((tmp_path / "artifacts" / "build-2").glob("rerun-*.json"))


async def test_cancelled_rerun_is_interrupted_and_keeps_verdict(make_server, env_vars):
    env_vars(**FAST)
    srv = await make_server()
    base = srv["base"]
    await run_build(base, "debug", 1)

    env_vars(SAMPLE_TEST_DELAY="3.0")  # slow enough to cancel mid-case
    async with httpx.AsyncClient() as client:
        await client.post(f"{base}/api/reports/1/rerun")
        await wait_report(base, 1, lambda r: r["active_rerun"] is not None)
        res = await client.post(f"{base}/api/reports/1/rerun/cancel")
        assert res.json()["cancelled"] is True

    report = await wait_report(base, 1, lambda r: r["reruns"] and r["reruns"][-1]["status"] == "interrupted")
    assert report["verdict"] == "failed", "interrupted rerun must keep the previous verdict"
    flaky = case(report, "test_flaky_cache")
    assert flaky["current"]["status"] == "fail"
    assert flaky["current"]["source"] == "initial"
    assert flaky["initial"]["status"] == "fail"

    # Cancelling again is a no-op, not an error.
    async with httpx.AsyncClient() as client:
        res = await client.post(f"{base}/api/reports/1/rerun/cancel")
        assert res.json()["cancelled"] is False


async def test_crashed_rerun_is_interrupted_and_keeps_verdict(make_server, env_vars):
    env_vars(**FAST)
    srv = await make_server()
    base = srv["base"]
    await run_build(base, "debug", 1)

    # The rerun's test runner dies before finishing its single case.
    env_vars(SAMPLE_TEST_DIE_AFTER="0")
    async with httpx.AsyncClient() as client:
        res = await client.post(f"{base}/api/reports/1/rerun")
        assert res.status_code == 202

    report = await wait_report(base, 1, lambda r: r["reruns"] and r["reruns"][-1]["status"] == "interrupted")
    assert report["verdict"] == "failed"
    assert case(report, "test_flaky_cache")["current"]["status"] == "fail"
    assert report["reruns"][-1]["exit_code"] == 2


async def test_report_endpoints_404_for_unknown_build(make_server, env_vars):
    env_vars(**FAST)
    srv = await make_server()
    async with httpx.AsyncClient() as client:
        assert (await client.get(f"{srv['base']}/api/reports/7")).status_code == 404
        assert (await client.post(f"{srv['base']}/api/reports/7/rerun/cancel")).status_code == 404


async def test_reports_disabled_with_custom_sample_cmd(server):
    """The classic streaming fixture (synthetic build) has no report manager."""
    async with httpx.AsyncClient() as client:
        res = await client.get(f"{server['base']}/api/reports")
    assert res.status_code == 503


# ---- unit-level: the commit rule -------------------------------------------


def make_report() -> Report:
    return Report(
        build_id=1,
        config="debug",
        artifact_dir="/tmp/x",
        created_at=time.time(),
        initial={
            "a": {"status": "fail", "detail": "boom"},
            "b": {"status": "pass", "detail": ""},
        },
        effective={
            "a": {"status": "fail", "detail": "boom", "source": "initial"},
            "b": {"status": "pass", "detail": "", "source": "initial"},
        },
        verdict="failed",
    )


def write_results(path: Path, cases: dict, finished: bool = True) -> None:
    path.write_text(
        json.dumps({"finished_at": time.time() if finished else None, "cases": cases})
    )


async def test_finalize_commits_only_when_batch_complete(tmp_path):
    mgr = ReportManager(tmp_path)
    report = make_report()
    mgr.reports[1] = report
    rerun = RerunRecord(id=1, cases=["a"], started_at=time.time())

    # Complete batch, case now passes -> committed, verdict flips.
    results = tmp_path / "rerun-1.json"
    write_results(results, {"a": {"status": "pass", "detail": "ok now"}})
    rerun.exit_code = 0
    mgr._finalize(report, rerun, results)
    assert rerun.status == "completed"
    assert report.verdict == "passed"
    assert report.effective["a"]["status"] == "pass"
    assert report.effective["a"]["source"] == "rerun#1"
    assert report.initial["a"]["status"] == "fail"  # initial record kept


async def test_finalize_keeps_verdict_when_results_incomplete(tmp_path):
    mgr = ReportManager(tmp_path)
    report = make_report()
    mgr.reports[1] = report
    rerun = RerunRecord(id=1, cases=["a"], started_at=time.time())

    # Exit 0 but the results file has no case results at all -> interrupted.
    results = tmp_path / "rerun-1.json"
    write_results(results, {}, finished=False)
    rerun.exit_code = 0
    mgr._finalize(report, rerun, results)
    assert rerun.status == "interrupted"
    assert report.verdict == "failed"
    assert report.effective["a"]["status"] == "fail"


async def test_finalize_commits_still_failing_batch(tmp_path):
    mgr = ReportManager(tmp_path)
    report = make_report()
    mgr.reports[1] = report
    rerun = RerunRecord(id=1, cases=["a"], started_at=time.time())

    # Complete run (exit 1 = failures), case still fails -> committed, verdict stays.
    results = tmp_path / "rerun-1.json"
    write_results(results, {"a": {"status": "fail", "detail": "still broken"}})
    rerun.exit_code = 1
    mgr._finalize(report, rerun, results)
    assert rerun.status == "completed"
    assert report.verdict == "failed"
    assert report.effective["a"]["detail"] == "still broken"
    assert report.effective["a"]["source"] == "rerun#1"
