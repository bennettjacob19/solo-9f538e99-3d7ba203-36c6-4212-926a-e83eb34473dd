#!/usr/bin/env python3
"""Deterministic synthetic build for tests.

Environment:
  SYNTH_LINES     total numbered lines to emit (default 60)
  SYNTH_DELAY     seconds between lines (default 0.01)
  SYNTH_FAST      if set, emit every line as fast as possible
  SYNTH_FAIL      if set, exit non-zero after the final line
  SYNTH_PARTIAL   if set, the last line has no trailing newline
"""

from __future__ import annotations

import os
import sys
import time

total = int(os.environ.get("SYNTH_LINES", "60"))
delay = float(os.environ.get("SYNTH_DELAY", "0.01"))

for i in range(1, total + 1):
    if i % 10 == 0:
        print(f"warn line {i} on stderr", file=sys.stderr, flush=True)
    print(f"line {i}", flush=True)
    if not os.environ.get("SYNTH_FAST"):
        time.sleep(delay)

if os.environ.get("SYNTH_PARTIAL"):
    print("partial tail without newline", end="", flush=True)
else:
    print("synthetic build done", flush=True)

raise SystemExit(7 if os.environ.get("SYNTH_FAIL") else 0)
