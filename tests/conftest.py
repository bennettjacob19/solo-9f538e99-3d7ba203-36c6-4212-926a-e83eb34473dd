"""Shared fixtures for build log tests."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.buildlog import BuildRunner  # noqa: E402
from app.main import create_app  # noqa: E402


async def wait_for(predicate, timeout: float = 15.0, interval: float = 0.02) -> None:
    """Poll until predicate() is truthy or fail the test."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if predicate():
            return
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met before timeout")
        await asyncio.sleep(interval)


class SSEClient:
    """Minimal live SSE reader on top of a real uvicorn server."""

    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None) -> None:
        self.url = url
        self.headers = headers or {}
        self._resp: Optional[httpx.Response] = None
        self._ctx = None

    async def __aenter__(self) -> "SSEClient":
        self._ctx = httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=30.0))
        self._client = await self._ctx.__aenter__()
        self._req = self._client.stream("GET", self.url, headers=self.headers)
        self._resp = await self._req.__aenter__()
        assert self._resp.status_code == 200, self._resp.status_code
        self._lines = self._resp.aiter_lines().__aiter__()
        return self

    async def next_event(self, timeout: float = 20.0) -> Tuple[str, Optional[int], dict]:
        async def _read():
            event, event_id, data = "message", None, []
            async for line in self._lines:
                if line == "":
                    if data:
                        return event, event_id, json.loads("\n".join(data))
                    event, event_id, data = "message", None, []
                    continue
                if line.startswith(":"):
                    continue
                if line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("id:"):
                    event_id = int(line[3:].strip())
                elif line.startswith("data:"):
                    data.append(line[5:].lstrip())
            raise EOFError("stream ended")

        return await asyncio.wait_for(_read(), timeout=timeout)

    async def __aexit__(self, *exc) -> None:
        await self._req.__aexit__(*exc)
        await self._ctx.__aexit__(*exc)


@pytest.fixture
async def server():
    """Run the app on an ephemeral port with a fast synthetic build command."""
    script = str(Path(__file__).with_name("synthetic_build.py"))
    runner = BuildRunner(capacity=20, sample_cmd=[sys.executable, "-u", script])
    app = create_app(runner)
    import uvicorn

    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="on")
    srv = uvicorn.Server(config)
    task = asyncio.create_task(srv.serve())
    await wait_for(lambda: srv.started, timeout=10)
    port = srv.servers[0].sockets[0].getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    try:
        yield {"base": base, "runner": runner, "app": app}
    finally:
        await runner.shutdown()
        srv.should_exit = True
        try:
            await asyncio.wait_for(task, timeout=10)
        except asyncio.TimeoutError:  # pragma: no cover
            task.cancel()
