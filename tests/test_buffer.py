"""LogBuffer semantics: bounded ring, monotonic seq, exact gap reporting."""

from __future__ import annotations

import asyncio

import pytest

from app.buildlog import LogBuffer

pytestmark = pytest.mark.asyncio


async def test_sequence_numbers_are_monotonic_and_never_reused():
    buf = LogBuffer(capacity=3)
    seqs = [buf.append(f"l{i}").seq for i in range(5)]
    assert seqs == [1, 2, 3, 4, 5]
    assert [line.seq for line in buf.snapshot()] == [3, 4, 5]


async def test_reader_behind_watermark_gets_one_exact_gap():
    buf = LogBuffer(capacity=4)
    for i in range(1, 10):
        buf.append(f"line {i}")

    lines, gap = buf.read_after(0)
    assert gap is not None and (gap.first, gap.last) == (1, 5)
    assert [line.seq for line in lines] == [6, 7, 8, 9]

    # After the reader acknowledges the gap it must not be reported again.
    lines, gap = buf.read_after(gap.last)
    assert gap is None
    assert [line.seq for line in lines] == [6, 7, 8, 9]


async def test_no_gap_when_reader_is_inside_the_window():
    buf = LogBuffer(capacity=5)
    for i in range(1, 8):
        buf.append(f"line {i}")
    lines, gap = buf.read_after(4)
    assert gap is None
    assert [line.seq for line in lines] == [5, 6, 7]


async def test_gap_only_covers_evicted_tail_not_new_lines():
    buf = LogBuffer(capacity=3)
    for i in range(1, 6):
        buf.append("x")  # seq 1..5, retained 3..5
    lines, gap = buf.read_after(1)  # only seq 2 was evicted for this reader
    assert (gap.first, gap.last) == (2, 2)
    assert [line.seq for line in lines] == [3, 4, 5]


async def test_reader_at_exact_window_edge_gets_no_gap():
    buf = LogBuffer(capacity=3)
    for i in range(1, 5):
        buf.append("x")  # retained 2..4
    lines, gap = buf.read_after(1)  # seq 2 is still retained
    assert gap is None
    assert [line.seq for line in lines] == [2, 3, 4]


async def test_empty_buffer_has_no_gap():
    buf = LogBuffer(capacity=2)
    lines, gap = buf.read_after(0)
    assert lines == [] and gap is None


async def test_close_wakes_waiters():
    buf = LogBuffer(capacity=2)
    waiter = buf.change_event()
    loop = asyncio.get_running_loop()
    started = loop.time()
    task = asyncio.create_task(waiter.wait())
    await asyncio.sleep(0.01)
    buf.close()
    await asyncio.wait_for(task, timeout=1)
    assert loop.time() - started < 1


async def test_change_event_swaps_so_no_wakeup_is_lost():
    buf = LogBuffer(capacity=2)
    first = buf.change_event()
    buf.append("a")
    assert first.is_set()
    second = buf.change_event()
    assert not second.is_set()
    buf.append("b")
    assert second.is_set()
