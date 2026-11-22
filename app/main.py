"""FastAPI app factory: local sample build control plus a resumable SSE log stream."""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .buildlog import CAPACITY, BuildRunner, Line

STATIC_DIR = Path(__file__).with_name("static")
HEARTBEAT_SECONDS = 15.0
TICK_SECONDS = 0.25
MAX_BATCH = 200  # one server buffer's worth per SSE frame; keeps frames small


def create_app(runner: Optional[BuildRunner] = None) -> FastAPI:
    """Build the ASGI app. Passing a runner lets tests pin buffers/commands."""

    state: dict[str, BuildRunner] = {}

    def get_runner() -> BuildRunner:
        if runner is not None:
            return runner
        if "runner" not in state:
            capacity = int(os.environ.get("BUILDLOG_CAPACITY", CAPACITY))
            state["runner"] = BuildRunner(capacity=capacity)
        return state["runner"]

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        get_runner()
        yield
        await get_runner().shutdown()

    app = FastAPI(title="Build Log Viewer", version="1.0.0", lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/status")
    async def status() -> JSONResponse:
        return JSONResponse(get_runner().status_payload())

    @app.post("/api/build/start")
    async def start_build() -> JSONResponse:
        current = get_runner()
        try:
            await current.start()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return JSONResponse(current.status_payload(), status_code=202)

    @app.post("/api/build/cancel")
    async def cancel_build() -> JSONResponse:
        current = get_runner()
        cancelled = await current.cancel()
        return JSONResponse({**current.status_payload(), "cancelled": cancelled})

    def sse(event: str, data: dict, event_id: Optional[int] = None) -> str:
        payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        head = f"event: {event}\n"
        if event_id is not None:
            head += f"id: {event_id}\n"
        return f"{head}data: {payload}\n\n"

    def line_payload(line: Line) -> dict:
        return {"seq": line.seq, "text": line.text, "stream": line.stream, "ts": line.ts}

    def parse_cursor(request: Request) -> Optional[int]:
        """Resume cursor: explicit ?after= wins, else the SSE Last-Event-ID.

        The page always sends `after` (the exact line it last displayed). The
        header is the fallback for a native EventSource reconnect, where the
        last delivered event id *is* that same line number.
        """
        raw = request.query_params.get("after")
        if raw is None or raw == "":
            raw = request.headers.get("last-event-id")
        if raw is None or raw == "":
            return None
        try:
            value = int(raw)
        except ValueError:
            raise HTTPException(status_code=400, detail="cursor must be an integer line number")
        if value < 0:
            raise HTTPException(status_code=400, detail="cursor must be >= 0")
        return value

    def parse_epoch(request: Request) -> Optional[str]:
        epoch = request.query_params.get("epoch")
        return epoch or None

    async def log_events(request: Request, after: int) -> AsyncIterator[str]:
        current = get_runner()
        buffer = current.buffer
        cursor = after
        loop = asyncio.get_running_loop()
        last_write = loop.time()
        last_status: Optional[str] = None
        acked_run = current.run.run_id

        yield sse("hello", {**current.status_payload(), "cursor": cursor})

        while True:
            if await request.is_disconnected():
                return

            # Grab the wake-up event *before* reading so an append cannot slip
            # in between the read and the wait.
            waiter = buffer.change_event()
            lines, gap = buffer.read_after(cursor)

            if gap is not None:
                # The lines this viewer needs have been evicted. Tell it
                # exactly which numbers are gone, then resume from the oldest
                # retained line.
                cursor = gap.last
                yield sse("gap", {"first": gap.first, "last": gap.last}, event_id=cursor)
                last_write = loop.time()

            if lines:
                batch = lines[:MAX_BATCH]
                cursor = batch[-1].seq
                yield sse("lines", {"lines": [line_payload(line) for line in batch]}, event_id=cursor)
                last_write = loop.time()
                # Yield to the event loop so one client draining a backlog
                # cannot starve the build pumps or other viewers.
                await asyncio.sleep(0)
                continue

            if buffer.closed:
                yield sse("end", {"status": current.run.status, "cursor": cursor})
                return
            if current.run.status != last_status or current.run.run_id != acked_run:
                last_status = current.run.status
                acked_run = current.run.run_id
                yield sse("status", {**current.status_payload(), "cursor": cursor})
                last_write = loop.time()

            try:
                await asyncio.wait_for(waiter.wait(), timeout=TICK_SECONDS)
            except asyncio.TimeoutError:
                pass

            now = loop.time()
            if now - last_write >= HEARTBEAT_SECONDS:
                yield ": keep-alive\n\n"
                last_write = now

    @app.get("/api/logs/stream")
    async def stream_logs(request: Request) -> StreamingResponse:
        current = get_runner()
        after = parse_cursor(request)
        epoch = parse_epoch(request)
        if epoch is not None and epoch != current.session_id:
            # The server restarted since this viewer's last connection, so its
            # line numbers refer to a dead session. Start over at the oldest
            # retained line; the client clears its rows on epoch mismatch.
            after = current.buffer.cursor_from_oldest()
        elif after is None:
            # No cursor at all means a fresh viewer: start just before the
            # oldest retained line, so it paints the retained window without a
            # spurious "missing lines 1-N" banner for a build it never saw.
            after = current.buffer.cursor_from_oldest()
        elif after > current.buffer.last_seq:
            # A cursor beyond anything this process ever produced cannot be
            # satisfied (typically a reconnect from before a restart, where a
            # proxy dropped the epoch). Don't leave the viewer hanging forever.
            after = current.buffer.cursor_from_oldest()
        headers = {
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
        return StreamingResponse(
            log_events(request, after),
            media_type="text/event-stream",
            headers=headers,
        )

    @app.get("/api/logs/tail")
    async def tail(after: int = 0, limit: int = 200) -> JSONResponse:
        """Non-streaming read; handy for scripts and tests without SSE."""
        buffer = get_runner().buffer
        lines, gap = buffer.read_after(after)
        cap = max(1, min(limit, get_runner().capacity))
        chunk = lines[:cap]
        return JSONResponse(
            {
                "lines": [line_payload(line) for line in chunk],
                "gap": None if gap is None else {"first": gap.first, "last": gap.last},
                "cursor": chunk[-1].seq if chunk else after,
                "has_more": len(lines) > cap,
                "status": get_runner().status_payload(),
            }
        )

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True}

    return app


app = create_app()
