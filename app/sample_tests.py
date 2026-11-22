#!/usr/bin/env python3
"""Sample local test runner: runs named cases against a build's artifacts.

Self-contained on purpose: it is executed as a subprocess by both the sample
build (initial run) and the server (failed-case rerun), so it must not rely on
importing the app package.

Each case prints one line `CASE <name> PASS|FAIL <detail>` and the runner
writes a JSON results file *incrementally* (after every case), so a crash
leaves a visibly incomplete file behind:

  {
    "config": "debug",
    "artifact_dir": "...",
    "started_at": 123.0,
    "finished_at": 123.4,          # only present when the run completed
    "cases": {"test_x": {"status": "pass", "detail": "..."}}
  }

Exit code: 0 = all selected cases passed, 1 = ran to completion with failures,
2 = usage error or simulated crash.

Environment (for demos/tests):
  SAMPLE_TEST_DELAY       seconds per case (default 0.4)
  SAMPLE_TEST_DIE_AFTER   if set, os._exit(2) after N case results (crash)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Callable, Dict, Tuple

Result = Tuple[str, str]  # (status, detail), status in {"pass", "fail"}


def case_artifact_exists(ctx: dict) -> Result:
    artifact = Path(ctx["artifact_dir"]) / "app.bin"
    if artifact.is_file() and artifact.stat().st_size > 0:
        return "pass", f"app.bin 存在（{artifact.stat().st_size} 字节）"
    return "fail", "app.bin 缺失或为空"


def case_config_stamp(ctx: dict) -> Result:
    meta_path = Path(ctx["artifact_dir"]) / "meta.json"
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return "fail", "meta.json 缺失或不可读"
    actual = meta.get("config")
    if actual == ctx["config"]:
        return "pass", f"产物配置标记为 {actual}"
    return "fail", f"产物配置为 {actual}，期望 {ctx['config']}"


def case_checksum(ctx: dict) -> Result:
    artifact = Path(ctx["artifact_dir"]) / "app.bin"
    sum_file = Path(ctx["artifact_dir"]) / "app.bin.sha256"
    if not artifact.is_file() or not sum_file.is_file():
        return "fail", "缺少 app.bin 或 app.bin.sha256"
    actual = hashlib.sha256(artifact.read_bytes()).hexdigest()
    expected = sum_file.read_text(encoding="utf-8").split()[0]
    if actual == expected:
        return "pass", "sha256 校验一致"
    return "fail", "sha256 与记录不一致"


def case_debug_symbols(ctx: dict) -> Result:
    artifact = Path(ctx["artifact_dir"]) / "app.bin"
    if not artifact.is_file():
        return "fail", "app.bin 缺失"
    has_symbols = b"DEBUG-SYMBOLS" in artifact.read_bytes()
    if has_symbols:
        return "pass", "包含调试符号"
    return "fail", "调试符号缺失（release 产物已剥离，重跑无法修复）"


def case_flaky_cache(ctx: dict) -> Result:
    """Deterministic flaky case: fails on a cold cache, passes on rerun."""
    marker = Path(ctx["artifact_dir"]) / ".warm-cache"
    if marker.exists():
        return "pass", "缓存已预热"
    marker.write_text("warmed\n", encoding="utf-8")
    return "fail", "冷缓存：首次运行失败，重跑可通过"


CASES: Dict[str, Callable[[dict], Result]] = {
    "test_artifact_exists": case_artifact_exists,
    "test_config_stamp": case_config_stamp,
    "test_checksum": case_checksum,
    "test_debug_symbols": case_debug_symbols,
    "test_flaky_cache": case_flaky_cache,
}


def write_results(path: Path, results: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--cases", default="", help="comma-separated; default: all")
    parser.add_argument("--results-file", default="")
    args = parser.parse_args()

    artifact_dir = Path(args.artifact_dir)
    results_file = Path(args.results_file) if args.results_file else artifact_dir / "test-results.json"

    if args.cases.strip():
        selected = [name.strip() for name in args.cases.split(",") if name.strip()]
    else:
        selected = list(CASES)
    unknown = [name for name in selected if name not in CASES]
    if unknown:
        print(f"unknown case(s): {', '.join(unknown)}", file=sys.stderr)
        return 2

    delay = float(os.environ.get("SAMPLE_TEST_DELAY", "0.4"))
    die_after = os.environ.get("SAMPLE_TEST_DIE_AFTER")
    die_after = int(die_after) if die_after not in (None, "") else None

    ctx = {"artifact_dir": str(artifact_dir), "config": args.config}
    results = {
        "config": args.config,
        "artifact_dir": str(artifact_dir),
        "started_at": time.time(),
        "finished_at": None,
        "cases": {},
    }
    results_file.parent.mkdir(parents=True, exist_ok=True)
    write_results(results_file, results)

    print(f"test run: {len(selected)} case(s) against {artifact_dir} (config={args.config})", flush=True)
    for index, name in enumerate(selected):
        if die_after is not None and index >= die_after:
            print(f"simulated crash after {die_after} case result(s)", file=sys.stderr, flush=True)
            os._exit(2)  # hard exit: no finished_at, results file stays incomplete
        time.sleep(delay)
        try:
            status, detail = CASES[name](ctx)
        except Exception as exc:  # a broken case is a failing case
            status, detail = "fail", f"用例执行异常: {exc}"
        results["cases"][name] = {"status": status, "detail": detail}
        write_results(results_file, results)
        print(f"CASE {name} {status.upper()} {detail}", flush=True)

    results["finished_at"] = time.time()
    write_results(results_file, results)
    failed = [n for n, r in results["cases"].items() if r["status"] != "pass"]
    if failed:
        print(f"test run finished: {len(failed)} failed: {', '.join(failed)}", flush=True)
        return 1
    print(f"test run finished: all {len(selected)} case(s) passed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
