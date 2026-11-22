"""Build execution and the bounded, replayable log buffer behind the SSE stream."""

from __future__ import annotations

import asyncio
import codecs
import os
import shlex
import sys
import time
import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, List, Optional, Sequence, Tuple

from .reports import ReportManager

CAPACITY = 200
MAX_PENDING_CHARS = 16 * 1024

IDLE = "idle"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"
CANCELLED = "cancelled"

SAMPLE_SCRIPT = Path(__file__).with_name("sample_build.py")

# The two build configurations the page offers; the sample build produces a
# different artifact per config and the test report is tagged with it.
CONFIGS = ("debug", "release")
DEFAULT_CONFIG = "debug"


class BuildBusy(RuntimeError):
    pass


@dataclass(frozen=True)
class Line:
    seq: int
    text: str
    stream: str  # "stdout" | "stderr" | "system"
    ts: float


@dataclass(frozen=True)
class Gap:
    """A contiguous range of line numbers that is no longer retained."""

    first: int
    last: int

    @property
    def count(self) -> int:
        return self.last - self.first + 1


class LogBuffer:
    """A bounded ring of log lines with globally monotonic line numbers.

    Sequence numbers start at 1 for the lifetime of the process and are never
    reused.  Once the ring is full the oldest line is dropped; readers that
    still point at the dropped region get a single `Gap` describing exactly
    which line numbers vanished, then continue with the oldest retained line.
    """

    def __init__(self, capacity: int = CAPACITY) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._lines: Deque[Line] = deque(maxlen=capacity)
        self._next_seq = 1
        self._evicted: List[Tuple[int, int]] = []  # merged ascending evicted ranges
        self._changed = asyncio.Event()
        self._closed = False

    # ---- producer -----------------------------------------------------

    def append(self, text: str, stream: str = "stdout") -> Line:
        if self._closed:
            raise RuntimeError("log buffer is closed")
        if len(self._lines) == self.capacity:
            oldest = self._lines[0].seq
            if self._evicted and self._evicted[-1][1] + 1 == oldest:
                self._evicted[-1] = (self._evicted[-1][0], oldest)
            else:
                self._evicted.append((oldest, oldest))
        line = Line(seq=self._next_seq, text=text, stream=stream, ts=time.time())
        self._next_seq += 1
        self._lines.append(line)
        self._wake()
        return line

    def close(self) -> None:
        self._closed = True
        self._wake()

    def _wake(self) -> None:
        # Swap out the event instead of clear(): every waiter holds the old
        # event, so an append that races with a waiter is never lost.
        old, self._changed = self._changed, asyncio.Event()
        old.set()

    # ---- consumer -----------------------------------------------------

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def last_seq(self) -> int:
        return self._next_seq - 1

    @property
    def first_seq(self) -> Optional[int]:
        return self._lines[0].seq if self._lines else None

    @property
    def buffered(self) -> int:
        return len(self._lines)

    def change_event(self) -> asyncio.Event:
        """Event that is set on the next append/close.

        Callers must grab this *before* reading so an append between the read
        and the wait cannot be missed.
        """
        return self._changed

    def read_after(self, after_seq: int) -> Tuple[List[Line], Optional[Gap]]:
        """Return retained lines with seq > after_seq, plus any lost range.

        At most one gap is possible: a reader's cursor is a single point, and
        eviction only ever happens at the front of the ring, so everything
        missing to it is one contiguous run.  The gap covers
        `after_seq + 1 .. first_retained - 1` (clipped to what was actually
        evicted, so a cursor that never advanced past a restart is handled).
        """
        if after_seq < 0:
            after_seq = 0
        lines = [line for line in self._lines if line.seq > after_seq]
        gap: Optional[Gap] = None
        first = self.first_seq
        if first is not None and after_seq < first - 1:
            gap_first = after_seq + 1
            gap_last = first - 1
            for evicted_first, evicted_last in self._evicted:
                if evicted_last >= gap_first and evicted_first <= gap_last:
                    gap_first = max(gap_first, evicted_first)
                    break
            if gap_first <= gap_last:
                gap = Gap(gap_first, gap_last)
        return lines, gap

    def cursor_from_oldest(self) -> int:
        """Cursor just before the oldest retained line (no gap will be shown)."""
        first = self.first_seq
        if first is None:
            return self.last_seq
        return first - 1

    def snapshot(self) -> List[Line]:
        return list(self._lines)


@dataclass
class RunInfo:
    run_id: int = 0
    status: str = IDLE
    command: Optional[List[str]] = None
    config: Optional[str] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    exit_code: Optional[int] = None
    stdout_bytes: int = 0
    stderr_bytes: int = 0
    pid: Optional[int] = None


class BuildRunner:
    """Runs one local sample build at a time, appending into a shared buffer.

    Log collection is deliberately independent of viewers: the subprocess
    pumps run as their own tasks and only ever touch the bounded ring.  A slow
    SSE client therefore cannot apply back-pressure to the build, and can at
    worst fall out of the ring and be told which lines it missed.
    """

    def __init__(
        self,
        capacity: int = CAPACITY,
        sample_cmd: Optional[Sequence[str]] = None,
        report_manager: Optional[ReportManager] = None,
        artifacts_dir: Optional[str] = None,
    ) -> None:
        self.buffer = LogBuffer(capacity)
        self.capacity = capacity
        self.run = RunInfo()
        # A custom sample_cmd is a fixed command (used by tests); without one
        # the runner drives the real sample build with --config/--build-id.
        self._custom_cmd = list(sample_cmd) if sample_cmd else None
        self.artifacts_dir = Path(
            artifacts_dir or os.environ.get("BUILDLOG_ARTIFACTS_DIR") or "artifacts"
        )
        if report_manager is None and self._custom_cmd is None:
            report_manager = ReportManager(self.artifacts_dir, buffer=self.buffer)
        self.reports = report_manager
        # Line numbers are per server lifetime. A browser that reconnects
        # after a restart sends an epoch from the previous lifetime plus a
        # cursor that may be far ahead of the new buffer; the epoch lets the
        # stream start it over instead of waiting forever at that cursor.
        self.session_id = uuid.uuid4().hex
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    # ---- lifecycle ----------------------------------------------------

    async def start(self, config: Optional[str] = None) -> None:
        async with self._lock:
            if self.running:
                raise BuildBusy("a build is already running")
            run_id = self.run.run_id + 1
            if self._custom_cmd is not None:
                command = list(self._custom_cmd)
                run_config = None
            else:
                run_config = config or DEFAULT_CONFIG
                if run_config not in CONFIGS:
                    raise ValueError(
                        f"unknown config {run_config!r}; choose one of {', '.join(CONFIGS)}"
                    )
                command = [
                    sys.executable,
                    "-u",
                    str(SAMPLE_SCRIPT),
                    "--config",
                    run_config,
                    "--build-id",
                    str(run_id),
                    "--artifacts-dir",
                    str(self.artifacts_dir),
                ]
            self.run = RunInfo(
                run_id=run_id,
                status=RUNNING,
                command=command,
                config=run_config,
                started_at=time.time(),
            )
            self.buffer.append(
                f"──── build #{self.run.run_id} started: "
                + " ".join(shlex.quote(p) for p in command)
                + " ────",
                "system",
            )
            self._task = asyncio.create_task(self._run(self.run), name=f"build-{self.run.run_id}")

    async def cancel(self) -> bool:
        task = self._task
        if task is None or task.done():
            return False
        task.cancel()
        # wait() (not wait_for(shield(...))) so a cancellation of *this* caller
        # is never mistaken for the build task finishing, and vice versa.
        await asyncio.wait({task}, timeout=10)
        return True

    async def shutdown(self) -> None:
        await self.cancel()
        if self.reports is not None:
            await self.reports.shutdown()
        self.buffer.close()

    # ---- worker -------------------------------------------------------

    async def _run(self, run: RunInfo) -> None:
        proc: Optional[asyncio.subprocess.Process] = None
        pumps: List[asyncio.Task] = []
        try:
            proc = await asyncio.create_subprocess_exec(
                *run.command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            run.pid = proc.pid
            pumps = [
                asyncio.create_task(self._pump(proc.stdout, run, "stdout")),
                asyncio.create_task(self._pump(proc.stderr, run, "stderr")),
            ]
            run.exit_code = await proc.wait()
            # Give the pumps a moment to drain anything left in the pipes.
            try:
                await asyncio.wait_for(asyncio.gather(*pumps), timeout=10)
            except asyncio.TimeoutError:
                for pump in pumps:
                    pump.cancel()
                await asyncio.gather(*pumps, return_exceptions=True)
            run.status = SUCCEEDED if run.exit_code == 0 else FAILED
        except asyncio.CancelledError:
            run.status = CANCELLED
            run.exit_code = None
            for pump in pumps:
                pump.cancel()
            if pumps:
                await asyncio.gather(*pumps, return_exceptions=True)
            if proc is not None and proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
        except FileNotFoundError:
            run.status = FAILED
            run.exit_code = 127
            self._safe_append("[error] sample build executable not found")
        except Exception as exc:  # pragma: no cover - defensive
            run.status = FAILED
            run.exit_code = 1
            self._safe_append(f"[error] build failed to run: {exc}")
        finally:
            run.finished_at = time.time()
            self._append_summary(run)
            self._maybe_create_report(run)
            # Any waiter should re-check the finished status immediately.
            self.buffer._wake()

    def _maybe_create_report(self, run: RunInfo) -> None:
        """Turn the build's test results file into a report, if there is one.

        Runs for both succeeded and failed builds: exit 1 means the build is
        fine but some test cases failed, and that is exactly when a report
        matters.  Cancelled builds and crashed test runners leave no complete
        results file, so no report appears.
        """
        if self.reports is None or self._custom_cmd is not None:
            return
        if run.status not in (SUCCEEDED, FAILED):
            return
        try:
            report = self.reports.create_report(run.run_id, run.config)
        except Exception as exc:  # pragma: no cover - defensive
            self._safe_append(f"[error] could not create test report: {exc}")
            return
        if report is None:
            self._safe_append(f"──── build #{run.run_id} produced no complete test report ────")
            return
        total = len(report.effective)
        failed = sum(1 for r in report.effective.values() if r["status"] != "pass")
        self._safe_append(
            f"──── build #{run.run_id} 测试报告已生成：判定 {report.verdict}"
            f"（{total - failed}/{total} 通过）────"
        )

    def _safe_append(self, text: str, stream: str = "system") -> None:
        try:
            self.buffer.append(text, stream)
        except RuntimeError:  # buffer already closed (app shutting down)
            pass

    def _append_summary(self, run: RunInfo) -> None:
        if self.buffer.closed:
            return
        if run.status == CANCELLED:
            text = f"──── build #{run.run_id} cancelled ────"
        elif run.status == SUCCEEDED:
            text = f"──── build #{run.run_id} finished: succeeded (exit 0) ────"
        elif run.status == FAILED:
            text = f"──── build #{run.run_id} finished: failed (exit {run.exit_code}) ────"
        else:  # pragma: no cover - only cancelled/failed/succeeded reach here
            text = f"──── build #{run.run_id} ended: {run.status} ────"
        self._safe_append(text)

    async def _pump(self, stream: Optional[asyncio.StreamReader], run: RunInfo, name: str) -> None:
        if stream is None:
            return
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        pending = ""
        while True:
            chunk = await stream.read(4096)
            if not chunk:
                break
            if name == "stdout":
                run.stdout_bytes += len(chunk)
            else:
                run.stderr_bytes += len(chunk)
            pending += decoder.decode(chunk)
            pending = self._drain_lines(pending, name)
        pending += decoder.decode(b"", final=True)
        pending = self._drain_lines(pending, name, eof=True)
        if pending:
            self.buffer.append(pending, name)

    def _drain_lines(self, pending: str, stream: str, eof: bool = False) -> str:
        """Emit complete lines; support \\n, \\r and \\r\\n; cap partial lines."""
        while True:
            i_n = pending.find("\n")
            i_r = pending.find("\r")
            candidates = [i for i in (i_n, i_r) if i >= 0]
            if not candidates:
                break
            i = min(candidates)
            if pending[i] == "\r":
                if i == len(pending) - 1 and not eof:
                    break  # might be the first half of \r\n
                step = 2 if i + 1 < len(pending) and pending[i + 1] == "\n" else 1
            else:
                step = 1
            self.buffer.append(pending[:i], stream)
            pending = pending[i + step :]
        if len(pending) > MAX_PENDING_CHARS:
            self.buffer.append(pending[:MAX_PENDING_CHARS], stream)
            pending = pending[MAX_PENDING_CHARS:]
        return pending

    # ---- introspection ------------------------------------------------

    def status_payload(self) -> dict:
        run = self.run
        return {
            "status": run.status,
            "session_id": self.session_id,
            "run_id": run.run_id,
            "pid": run.pid if self.running else None,
            "command": run.command,
            "config": run.config,
            "configs": list(CONFIGS),
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "exit_code": run.exit_code,
            "stdout_bytes": run.stdout_bytes,
            "stderr_bytes": run.stderr_bytes,
            "buffered": self.buffer.buffered,
            "capacity": self.capacity,
            "first_seq": self.buffer.first_seq,
            "last_seq": self.buffer.last_seq,
            "server_time": time.time(),
        }
