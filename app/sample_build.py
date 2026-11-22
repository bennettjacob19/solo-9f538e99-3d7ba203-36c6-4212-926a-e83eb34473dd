#!/usr/bin/env python3
"""Sample local build used by the demo page.

Prints progress to stdout and an occasional warning to stderr so both streams
are visible in the log view.  Takes roughly 15 seconds end to end.
"""

from __future__ import annotations

import sys
import time

TOTAL = 40
STEPS = 36


def say(message: str) -> None:
    print(message, flush=True)


def main() -> int:
    say("sample build: configuring")
    time.sleep(0.3)
    for step in range(1, STEPS + 1):
        say(f"[{step:02d}/{STEPS:02d}] compiling module_{step:02d}.c")
        if step % 7 == 0:
            print(
                f"warning: module_{step:02d}.c: unused variable 'scratch'",
                file=sys.stderr,
                flush=True,
            )
        time.sleep(0.32)
    say("sample build: linking")
    time.sleep(0.4)
    say("sample build: done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
