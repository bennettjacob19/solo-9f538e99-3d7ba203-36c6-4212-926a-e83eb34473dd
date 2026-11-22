#!/usr/bin/env python3
"""Sample local build used by the demo page.

Two phases:
  1. "compile": prints progress (stdout) and occasional warnings (stderr),
     then writes a config-specific artifact into
     ``<artifacts-dir>/build-<id>/`` (app.bin + sha256 + meta.json).
  2. "test": runs the local sample test suite against that artifact as a
     subprocess, so its output flows through the same log stream.

Exit code is the test run's exit code: 0 when every case passes, 1 when the
build itself is fine but some cases failed (the report is still produced).

Environment (for demos/tests):
  SAMPLE_BUILD_STEPS   number of compile steps (default 36)
  SAMPLE_BUILD_DELAY   seconds between compile steps (default 0.32)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

CONFIGS = ("debug", "release")
FLAGS = {"debug": "-O0 -g", "release": "-O2 -DNDEBUG"}
TESTS_SCRIPT = Path(__file__).with_name("sample_tests.py")


def say(message: str) -> None:
    print(message, flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", choices=CONFIGS, default="debug")
    parser.add_argument("--build-id", type=int, required=True)
    parser.add_argument("--artifacts-dir", required=True)
    args = parser.parse_args()

    steps = int(os.environ.get("SAMPLE_BUILD_STEPS", "36"))
    delay = float(os.environ.get("SAMPLE_BUILD_DELAY", "0.32"))

    out_dir = Path(args.artifacts_dir) / f"build-{args.build_id}"
    if out_dir.exists():
        # Fresh artifacts per build. Only this build's own directory is ever
        # removed; other builds' artifacts (and running reruns) are untouched.
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)

    say(f"sample build #{args.build_id}: configuring (config={args.config}, flags={FLAGS[args.config]})")
    time.sleep(0.3)
    for step in range(1, steps + 1):
        say(f"[{step:02d}/{steps:02d}] compiling module_{step:02d}.c")
        if step % 7 == 0:
            print(
                f"warning: module_{step:02d}.c: unused variable 'scratch'",
                file=sys.stderr,
                flush=True,
            )
        time.sleep(delay)
    say("sample build: linking")
    time.sleep(0.4)

    payload = "\n".join(
        [
            "sample-app",
            f"config={args.config}",
            f"build={args.build_id}",
            f"flags={FLAGS[args.config]}",
            "DEBUG-SYMBOLS" if args.config == "debug" else "STRIPPED",
            "",
        ]
    ).encode()
    artifact = out_dir / "app.bin"
    artifact.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    (out_dir / "app.bin.sha256").write_text(f"{digest}  app.bin\n", encoding="utf-8")
    meta = {
        "build_id": args.build_id,
        "config": args.config,
        "flags": FLAGS[args.config],
        "created_at": time.time(),
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    say(f"sample build: artifact written to {out_dir}")

    say("sample build: running local tests")
    completed = subprocess.run(
        [
            sys.executable,
            "-u",
            str(TESTS_SCRIPT),
            "--artifact-dir",
            str(out_dir),
            "--config",
            args.config,
            "--results-file",
            str(out_dir / "test-results.json"),
        ]
    )
    if completed.returncode == 0:
        say("sample build: all tests passed")
        return 0
    if completed.returncode == 1:
        say("sample build: some tests failed (report written)")
        return 1
    say(f"sample build: test runner did not finish (exit {completed.returncode})")
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
