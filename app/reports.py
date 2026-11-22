"""Per-build test reports and failed-case reruns.

A build's test phase writes ``test-results.json`` into its artifact directory;
the manager turns that into a :class:`Report` keyed by build number.  A report
keeps the *initial* per-case results forever; a rerun re-executes only the
currently failing cases against the *original* artifacts and, only when every
case in the batch has a result, commits them back to the same report and
recomputes the verdict.  A rerun that exits early (cancelled, crashed, or an
incomplete results file) is marked ``interrupted`` and the previous verdict
is kept.
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

TESTS_SCRIPT = Path(__file__).with_name("sample_tests.py")

PASS = "pass"
FAIL = "fail"

RERUN_RUNNING = "running"
RERUN_COMPLETED = "completed"
RERUN_INTERRUPTED = "interrupted"

VERDICT_PASSED = "passed"
VERDICT_FAILED = "failed"

CASE_LINE = re.compile(r"CASE\s+(\S+)\s+(PASS|FAIL)")


class ReportError(RuntimeError):
    pass


class ReportNotFound(ReportError):
    pass


class RerunBusy(ReportError):
    pass


class NoFailedCases(ReportError):
    pass


class ArtifactsMissing(ReportError):
    pass


@dataclass
class RerunRecord:
    id: int
    cases: List[str]  # the batch: failing cases at start time
    started_at: float
    status: str = RERUN_RUNNING
    completed: List[str] = field(default_factory=list)  # live progress only
    finished_at: Optional[float] = None
    exit_code: Optional[int] = None


@dataclass
class Report:
    build_id: int
    config: str
    artifact_dir: str
    created_at: float
    initial: Dict[str, dict]  # name -> {status, detail}; never mutated
    effective: Dict[str, dict]  # name -> {status, detail, source}
    verdict: str
    reruns: List[RerunRecord] = field(default_factory=list)


def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


class ReportManager:
    """Owns all reports and the (at most one per report) rerun tasks."""

    def __init__(self, artifacts_dir, buffer=None, tests_script=None) -> None:
        self.artifacts_dir = Path(artifacts_dir)
        self.tests_script = Path(tests_script) if tests_script else TESTS_SCRIPT
        self._buffer = buffer  # shared log buffer; rerun output is visible there
        self.reports: Dict[int, Report] = {}
        self._tasks: Dict[int, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    # ---- report creation (called by BuildRunner after each build) ---------

    def create_report(self, build_id: int, config: Optional[str] = None) -> Optional[Report]:
        """Build a report from a build's results file; None if incomplete."""
        artifact_dir = self.artifacts_dir / f"build-{build_id}"
        data = _read_json(artifact_dir / "test-results.json")
        if not data or not data.get("finished_at"):
            return None  # test phase never ran to completion -> no report
        cases = data.get("cases") or {}
        if not cases:
            return None
        initial = {
            name: {"status": r.get("status", FAIL), "detail": r.get("detail", "")}
            for name, r in cases.items()
        }
        effective = {name: {**r, "source": "initial"} for name, r in initial.items()}
        verdict = (
            VERDICT_PASSED
            if all(r["status"] == PASS for r in initial.values())
            else VERDICT_FAILED
        )
        report = Report(
            build_id=build_id,
            config=data.get("config") or config or "unknown",
            artifact_dir=str(artifact_dir),
            created_at=time.time(),
            initial=initial,
            effective=effective,
            verdict=verdict,
        )
        self.reports[build_id] = report
        return report

    # ---- rerun ------------------------------------------------------------

    def get(self, build_id: int) -> Report:
        report = self.reports.get(build_id)
        if report is None:
            raise ReportNotFound(f"no report for build #{build_id}")
        return report

    def active_rerun(self, build_id: int) -> Optional[RerunRecord]:
        report = self.reports.get(build_id)
        if report and report.reruns and report.reruns[-1].status == RERUN_RUNNING:
            return report.reruns[-1]
        return None

    async def start_rerun(self, build_id: int) -> Report:
        """Rerun the report's currently failing cases against its artifacts.

        Only one rerun per report may be in flight; reruns of *different*
        reports (and a concurrent new build) are independent and allowed.
        """
        async with self._lock:
            report = self.get(build_id)
            task = self._tasks.get(build_id)
            if task is not None and not task.done():
                raise RerunBusy(f"build #{build_id} already has a rerun in flight")
            failed = [n for n, r in report.effective.items() if r["status"] != PASS]
            if not failed:
                raise NoFailedCases(f"build #{build_id} has no failing cases to rerun")
            if not Path(report.artifact_dir).is_dir():
                raise ArtifactsMissing(f"artifacts for build #{build_id} are gone")
            rerun = RerunRecord(id=len(report.reruns) + 1, cases=failed, started_at=time.time())
            report.reruns.append(rerun)
            self._tasks[build_id] = asyncio.create_task(
                self._run_rerun(report, rerun), name=f"rerun-{build_id}-{rerun.id}"
            )
        return report

    async def cancel_rerun(self, build_id: int) -> bool:
        self.get(build_id)  # 404 for unknown builds
        task = self._tasks.get(build_id)
        if task is None or task.done():
            return False
        task.cancel()
        # wait() so a cancellation of *this* caller is not mistaken for the
        # rerun task finishing.
        await asyncio.wait({task}, timeout=10)
        return True

    async def shutdown(self) -> None:
        for task in self._tasks.values():
            if not task.done():
                task.cancel()
        pending = {t for t in self._tasks.values() if not t.done()}
        if pending:
            await asyncio.wait(pending, timeout=10)

    async def _run_rerun(self, report: Report, rerun: RerunRecord) -> None:
        results_file = Path(report.artifact_dir) / f"rerun-{rerun.id}.json"
        cmd = [
            sys.executable,
            "-u",
            str(self.tests_script),
            "--artifact-dir",
            report.artifact_dir,
            "--config",
            report.config,  # original config + original artifact dir
            "--cases",
            ",".join(rerun.cases),
            "--results-file",
            str(results_file),
        ]
        self._log(
            f"──── rerun #{rerun.id} for build #{report.build_id} started: "
            f"{len(rerun.cases)} failing case(s) on {report.artifact_dir} ────"
        )
        proc: Optional[asyncio.subprocess.Process] = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            assert proc.stdout is not None
            while True:
                raw = await proc.stdout.readline()
                if not raw:
                    break
                text = raw.decode("utf-8", "replace").rstrip()
                self._log(f"[rerun b{report.build_id}.r{rerun.id}] {text}")
                match = CASE_LINE.search(text)
                if match:
                    rerun.completed.append(match.group(1))
            rerun.exit_code = await proc.wait()
        except asyncio.CancelledError:
            # Cancelled (user or shutdown): kill the runner, mark interrupted,
            # keep the previous verdict.  The cancellation *is* the outcome of
            # this task, so it is handled here rather than re-raised.
            if proc is not None and proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            rerun.status = RERUN_INTERRUPTED
            rerun.finished_at = time.time()
            self._log(
                f"──── rerun #{rerun.id} for build #{report.build_id} cancelled: "
                f"results incomplete, previous verdict kept ────"
            )
            return
        except Exception as exc:  # defensive: runner could not even start
            rerun.status = RERUN_INTERRUPTED
            rerun.finished_at = time.time()
            self._log(
                f"──── rerun #{rerun.id} for build #{report.build_id} errored: {exc}; "
                f"previous verdict kept ────"
            )
            return
        self._finalize(report, rerun, results_file)

    def _finalize(self, report: Report, rerun: RerunRecord, results_file: Path) -> None:
        """Commit the batch only when *every* case has a result.

        Exit 0/1 means the runner ran to completion; anything else (signal,
        crash) or a results file without ``finished_at`` / missing cases means
        the batch is incomplete: mark interrupted, keep the previous verdict.
        """
        data = _read_json(results_file)
        cases = (data or {}).get("cases") or {}
        complete = bool(data and data.get("finished_at")) and all(
            name in cases for name in rerun.cases
        )
        if rerun.exit_code in (0, 1) and complete:
            for name in rerun.cases:
                result = cases[name]
                report.effective[name] = {
                    "status": result.get("status", FAIL),
                    "detail": result.get("detail", ""),
                    "source": f"rerun#{rerun.id}",
                }
            report.verdict = (
                VERDICT_PASSED
                if all(r["status"] == PASS for r in report.effective.values())
                else VERDICT_FAILED
            )
            rerun.status = RERUN_COMPLETED
            self._log(
                f"──── rerun #{rerun.id} for build #{report.build_id} committed: "
                f"verdict now {report.verdict} ────"
            )
        else:
            rerun.status = RERUN_INTERRUPTED
            self._log(
                f"──── rerun #{rerun.id} for build #{report.build_id} interrupted "
                f"(exit {rerun.exit_code}): results incomplete, previous verdict kept ────"
            )
        rerun.finished_at = time.time()

    # ---- introspection ----------------------------------------------------

    def list_payload(self) -> dict:
        return {"reports": [self.payload(b) for b in sorted(self.reports)]}

    def payload(self, build_id: int) -> dict:
        report = self.get(build_id)
        cases = [
            {"name": name, "initial": report.initial[name], "current": report.effective[name]}
            for name in report.initial
        ]
        passed = sum(1 for c in cases if c["current"]["status"] == PASS)
        active = self.active_rerun(build_id)
        return {
            "build_id": report.build_id,
            "config": report.config,
            "verdict": report.verdict,
            "created_at": report.created_at,
            "cases": cases,
            "counts": {"total": len(cases), "passed": passed, "failed": len(cases) - passed},
            "active_rerun": None
            if active is None
            else {
                "id": active.id,
                "cases": active.cases,
                "completed": active.completed,
                "started_at": active.started_at,
            },
            "reruns": [
                {
                    "id": r.id,
                    "status": r.status,
                    "cases": r.cases,
                    "completed": r.completed,
                    "started_at": r.started_at,
                    "finished_at": r.finished_at,
                    "exit_code": r.exit_code,
                }
                for r in report.reruns
            ],
        }

    def _log(self, text: str) -> None:
        if self._buffer is None or self._buffer.closed:
            return
        try:
            self._buffer.append(text, "system")
        except RuntimeError:  # buffer closed while shutting down
            pass
