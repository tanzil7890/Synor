from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import aclosing

import pytest
import synor as syn


@pytest.mark.asyncio
async def test_map_allows_more_than_256_items_to_rendezvous() -> None:
    item_count = 300
    all_started = asyncio.Event()
    started = 0

    async def work(value: int) -> int:
        nonlocal started
        started += 1
        if started == item_count:
            all_started.set()
        await all_started.wait()
        return value * 2

    result = await asyncio.wait_for(syn.map(work, range(item_count)), timeout=5)

    assert started == item_count
    assert result == [value * 2 for value in range(item_count)]


@pytest.mark.asyncio
async def test_map_preserves_item_cancellation_as_cancellation() -> None:
    async def work(value: int) -> int:
        if value == 1:
            raise asyncio.CancelledError("item cancelled")
        return value

    with pytest.raises(asyncio.CancelledError, match="item cancelled"):
        await syn.map(work, [0, 1, 2])


@pytest.mark.asyncio
async def test_map_bounded_limits_task_and_async_iterator_admission() -> None:
    limit = 3
    yielded = 0
    running = 0
    max_running = 0
    first_window_started = asyncio.Event()
    release = asyncio.Event()

    async def items() -> AsyncIterator[int]:
        nonlocal yielded
        for value in range(12):
            yielded += 1
            yield value

    async def work(value: int) -> int:
        nonlocal running, max_running
        running += 1
        max_running = max(max_running, running)
        if running == limit:
            first_window_started.set()
        await release.wait()
        running -= 1
        return value * 2

    task = asyncio.create_task(syn.map_bounded(work, items(), limit))
    await asyncio.wait_for(first_window_started.wait(), timeout=2)
    await asyncio.sleep(0)

    assert yielded == limit
    assert max_running == limit

    release.set()
    assert await task == [value * 2 for value in range(12)]
    assert max_running == limit


@pytest.mark.asyncio
async def test_map_bounded_stops_pulling_after_failure_and_drains_window() -> None:
    yielded: list[int] = []
    drained: list[int] = []
    release = asyncio.Event()

    async def items() -> AsyncIterator[int]:
        for value in range(20):
            yielded.append(value)
            yield value

    async def work(value: int) -> int:
        if value == 1:
            raise RuntimeError("bounded failure")
        await release.wait()
        drained.append(value)
        return value

    task = asyncio.create_task(syn.map_bounded(work, items(), 3))
    for _ in range(200):
        if task.done() or yielded == [0, 1, 2]:
            break
        await asyncio.sleep(0.01)

    assert yielded == [0, 1, 2]
    assert not task.done()
    release.set()
    with pytest.raises(RuntimeError, match="bounded failure"):
        await task
    assert sorted(drained) == [0, 2]


@pytest.mark.asyncio
async def test_map_bounded_interrupts_blocked_async_iterator_after_failure() -> None:
    second_pull_started = asyncio.Event()
    never_yield = asyncio.Event()

    async def items() -> AsyncIterator[int]:
        yield 0
        second_pull_started.set()
        await never_yield.wait()
        yield 1

    async def work(value: int) -> int:
        assert value == 0
        await second_pull_started.wait()
        raise RuntimeError("worker failed while iterator was blocked")

    with pytest.raises(RuntimeError, match="iterator was blocked"):
        await asyncio.wait_for(syn.map_bounded(work, items(), 2), timeout=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [0, -1, True, 1.5])
async def test_map_bounded_rejects_invalid_limit(value: object) -> None:
    async def work(item: int) -> int:
        return item

    with pytest.raises(ValueError, match="positive integer"):
        await syn.map_bounded(work, [1], value)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_map_stream_yields_completion_order_with_bounded_admission() -> None:
    limit = 3
    pulled: list[int] = []
    releases = [asyncio.Event() for _ in range(4)]
    started = [asyncio.Event() for _ in range(4)]

    async def items() -> AsyncIterator[int]:
        for value in range(4):
            pulled.append(value)
            yield value

    async def work(value: int) -> int:
        started[value].set()
        await releases[value].wait()
        return value

    stream = syn.map_stream(work, items(), limit)
    first_result = asyncio.ensure_future(anext(stream))
    await asyncio.wait_for(
        asyncio.gather(*(event.wait() for event in started[:limit])), timeout=2
    )

    assert pulled == [0, 1, 2]

    releases[2].set()
    assert await asyncio.wait_for(first_result, timeout=2) == 2
    await asyncio.wait_for(started[3].wait(), timeout=2)

    # One result was yielded before the fourth input crossed the iterator,
    # leaving exactly three pulled-but-not-yielded items in the window.
    assert pulled == [0, 1, 2, 3]

    releases[3].set()
    assert await asyncio.wait_for(anext(stream), timeout=2) == 3
    releases[1].set()
    assert await asyncio.wait_for(anext(stream), timeout=2) == 1
    releases[0].set()
    assert await asyncio.wait_for(anext(stream), timeout=2) == 0
    with pytest.raises(StopAsyncIteration):
        await anext(stream)


@pytest.mark.asyncio
async def test_map_stream_early_close_cancels_window_and_closes_sync_source() -> None:
    limit = 3
    pulled: list[int] = []
    started: set[int] = set()
    stopped: set[int] = set()
    source_closed = False
    release_first = asyncio.Event()
    all_started = asyncio.Event()

    def items() -> Iterator[int]:
        nonlocal source_closed
        try:
            for value in range(20):
                pulled.append(value)
                yield value
        finally:
            source_closed = True

    async def work(value: int) -> int:
        started.add(value)
        if len(started) == limit:
            all_started.set()
        try:
            if value == 0:
                await release_first.wait()
                return value
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        finally:
            stopped.add(value)

    async with aclosing(syn.map_stream(work, items(), limit)) as stream:
        first_result = asyncio.ensure_future(anext(stream))
        await asyncio.wait_for(all_started.wait(), timeout=2)
        assert pulled == [0, 1, 2]
        release_first.set()
        assert await asyncio.wait_for(first_result, timeout=2) == 0

        for _ in range(100):
            if len(pulled) == limit + 1:
                break
            await asyncio.sleep(0.01)
        assert pulled == [0, 1, 2, 3]

    assert source_closed
    assert stopped == set(pulled)


@pytest.mark.asyncio
async def test_map_stream_failure_interrupts_blocked_async_input_pull() -> None:
    second_pull_started = asyncio.Event()
    source_closed = asyncio.Event()
    never_yield = asyncio.Event()

    async def items() -> AsyncIterator[int]:
        try:
            yield 0
            second_pull_started.set()
            await never_yield.wait()
            yield 1
        finally:
            source_closed.set()

    async def work(value: int) -> int:
        assert value == 0
        await second_pull_started.wait()
        raise RuntimeError("stream worker failed")

    stream = syn.map_stream(work, items(), 2)
    with pytest.raises(RuntimeError, match="stream worker failed"):
        await asyncio.wait_for(anext(stream), timeout=1)

    assert source_closed.is_set()


@pytest.mark.asyncio
async def test_map_stream_input_failure_cancels_admitted_worker() -> None:
    worker_started = asyncio.Event()
    worker_cancelled = asyncio.Event()

    async def items() -> AsyncIterator[int]:
        yield 0
        await worker_started.wait()
        raise RuntimeError("input failed")

    async def work(value: int) -> int:
        assert value == 0
        worker_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            worker_cancelled.set()
        raise AssertionError("unreachable")

    stream = syn.map_stream(work, items(), 2)
    with pytest.raises(RuntimeError, match="input failed"):
        await asyncio.wait_for(anext(stream), timeout=1)

    assert worker_cancelled.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("async_source", [False, True])
async def test_map_stream_only_treats_protocol_sentinel_as_exhaustion(
    async_source: bool,
) -> None:
    class InvalidSyncIterator:
        def __iter__(self) -> Iterator[int]:
            return self

        def __next__(self) -> int:
            raise StopAsyncIteration("async sentinel from sync iterator")

    class InvalidAsyncIterator:
        def __aiter__(self) -> InvalidAsyncIterator:
            return self

        def __anext__(self) -> object:
            raise StopIteration("sync sentinel from async iterator")

    async def work(value: int) -> int:
        return value

    source = InvalidAsyncIterator() if async_source else InvalidSyncIterator()
    stream = syn.map_stream(work, source, 1)  # type: ignore[arg-type]
    expected_cause = StopIteration if async_source else StopAsyncIteration

    # StopIteration and StopAsyncIteration may not escape an async generator;
    # Python wraps the invalid cross-protocol source failure in RuntimeError.
    with pytest.raises(RuntimeError) as exc_info:
        await anext(stream)

    assert isinstance(exc_info.value.__cause__, expected_cause)


@pytest.mark.asyncio
async def test_map_stream_drops_item_returned_after_failure_cancel() -> None:
    second_pull_started = asyncio.Event()
    cancellation_caught = asyncio.Event()
    never_yield = asyncio.Event()
    started: list[int] = []

    async def items() -> AsyncIterator[int]:
        yield 0
        second_pull_started.set()
        try:
            await never_yield.wait()
        except asyncio.CancelledError:
            # A defensive/custom iterator may translate cancellation into a
            # final value. The already-failed map must not start that value.
            cancellation_caught.set()
        yield 1

    async def work(value: int) -> int:
        started.append(value)
        assert value == 0
        await second_pull_started.wait()
        raise RuntimeError("failure won the pull race")

    stream = syn.map_stream(work, items(), 2)
    with pytest.raises(RuntimeError, match="failure won the pull race"):
        await asyncio.wait_for(anext(stream), timeout=1)

    assert cancellation_caught.is_set()
    assert started == [0]


@pytest.mark.asyncio
async def test_map_stream_failure_cancels_other_admitted_workers() -> None:
    started = 0
    all_started = asyncio.Event()
    cancelled: set[int] = set()

    async def work(value: int) -> int:
        nonlocal started
        started += 1
        if started == 3:
            all_started.set()
        await all_started.wait()
        if value == 1:
            raise RuntimeError("fail fast")
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.add(value)
        raise AssertionError("unreachable")

    stream = syn.map_stream(work, range(20), 3)
    with pytest.raises(RuntimeError, match="fail fast"):
        await asyncio.wait_for(anext(stream), timeout=2)

    assert started == 3
    assert cancelled == {0, 2}


@pytest.mark.asyncio
async def test_map_stream_caller_cancellation_drains_admitted_work() -> None:
    limit = 2
    all_started = asyncio.Event()
    source_closed = asyncio.Event()
    started = 0
    stopped = 0

    async def items() -> AsyncIterator[int]:
        try:
            for value in range(20):
                yield value
        finally:
            source_closed.set()

    async def work(value: int) -> int:
        nonlocal started, stopped
        started += 1
        if started == limit:
            all_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped += 1
        return value

    stream = syn.map_stream(work, items(), limit)
    next_result = asyncio.ensure_future(anext(stream))
    await asyncio.wait_for(all_started.wait(), timeout=2)
    next_result.cancel()
    with pytest.raises(asyncio.CancelledError):
        await next_result

    assert stopped == limit
    assert source_closed.is_set()


@pytest.mark.asyncio
async def test_map_stream_repeated_cancellation_cannot_orphan_worker_cleanup() -> None:
    worker_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def work(value: int) -> int:
        worker_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await release_cleanup.wait()
            cleanup_finished.set()
            raise RuntimeError("worker cleanup failed after cancellation")
        return value

    stream = syn.map_stream(work, [0], 1)
    next_result = asyncio.create_task(anext(stream))
    await asyncio.wait_for(worker_started.wait(), timeout=2)

    next_result.cancel("first cancellation")
    await asyncio.wait_for(cleanup_started.wait(), timeout=2)
    next_result.cancel("second cancellation")
    await asyncio.sleep(0)
    next_result.cancel("third cancellation")
    await asyncio.sleep(0)

    assert not next_result.done()
    assert not cleanup_finished.is_set()

    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError) as exc_info:
        await asyncio.wait_for(next_result, timeout=2)

    assert exc_info.value.args == ("first cancellation",)
    assert cleanup_finished.is_set()
    notes = "\n".join(getattr(exc_info.value, "__notes__", ()))
    assert "worker cleanup failed after cancellation" in notes


@pytest.mark.asyncio
async def test_map_stream_repeated_cancellation_cannot_orphan_source_cleanup() -> None:
    pull_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def items() -> AsyncIterator[int]:
        try:
            pull_started.set()
            await asyncio.Event().wait()
            yield 0
        finally:
            cleanup_started.set()
            await release_cleanup.wait()
            cleanup_finished.set()

    async def work(value: int) -> int:
        return value

    stream = syn.map_stream(work, items(), 1)
    next_result = asyncio.create_task(anext(stream))
    await asyncio.wait_for(pull_started.wait(), timeout=2)

    next_result.cancel("first cancellation")
    await asyncio.wait_for(cleanup_started.wait(), timeout=2)
    next_result.cancel("second cancellation")
    await asyncio.sleep(0)
    next_result.cancel("third cancellation")
    await asyncio.sleep(0)

    assert not next_result.done()
    assert not cleanup_finished.is_set()

    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError) as exc_info:
        await asyncio.wait_for(next_result, timeout=2)

    assert exc_info.value.args == ("first cancellation",)
    assert cleanup_finished.is_set()


@pytest.mark.asyncio
async def test_map_stream_cancelled_explicit_close_still_drains_cleanup() -> None:
    both_started = asyncio.Event()
    release_first = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()
    started = 0

    async def work(value: int) -> int:
        nonlocal started
        started += 1
        if started == 2:
            both_started.set()
        await both_started.wait()
        if value == 0:
            await release_first.wait()
            return value
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await release_cleanup.wait()
            cleanup_finished.set()
        raise AssertionError("unreachable")

    stream = syn.map_stream(work, [0, 1], 2)
    first_result = asyncio.create_task(anext(stream))
    await asyncio.wait_for(both_started.wait(), timeout=2)
    release_first.set()
    assert await asyncio.wait_for(first_result, timeout=2) == 0

    close_task = asyncio.ensure_future(stream.aclose())
    await asyncio.wait_for(cleanup_started.wait(), timeout=2)
    close_task.cancel("first close cancellation")
    await asyncio.sleep(0)
    close_task.cancel("second close cancellation")
    await asyncio.sleep(0)

    assert not close_task.done()
    assert not cleanup_finished.is_set()

    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError) as exc_info:
        await asyncio.wait_for(close_task, timeout=2)

    assert exc_info.value.args == ("first close cancellation",)
    assert cleanup_finished.is_set()


@pytest.mark.asyncio
async def test_map_stream_explicit_close_surfaces_source_close_failure() -> None:
    source_started = False

    def items() -> Iterator[int]:
        nonlocal source_started
        try:
            value = 0
            while True:
                source_started = True
                yield value
                value += 1
        finally:
            raise RuntimeError("source close failed")

    async def work(value: int) -> int:
        if value == 0:
            return value
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    stream = syn.map_stream(work, items(), 2)
    assert await asyncio.wait_for(anext(stream), timeout=2) == 0

    with pytest.raises(RuntimeError, match="source close failed"):
        await stream.aclose()

    assert source_started


@pytest.mark.asyncio
async def test_map_stream_explicit_close_surfaces_worker_cleanup_failure() -> None:
    both_started = asyncio.Event()
    release_first = asyncio.Event()
    started = 0

    async def work(value: int) -> int:
        nonlocal started
        started += 1
        if started == 2:
            both_started.set()
        await both_started.wait()
        if value == 0:
            await release_first.wait()
            return value
        try:
            await asyncio.Event().wait()
        finally:
            raise RuntimeError("worker cancellation cleanup failed")

    stream = syn.map_stream(work, [0, 1], 2)
    first_result = asyncio.create_task(anext(stream))
    await asyncio.wait_for(both_started.wait(), timeout=2)
    release_first.set()
    assert await asyncio.wait_for(first_result, timeout=2) == 0

    with pytest.raises(RuntimeError, match="worker cancellation cleanup failed"):
        await stream.aclose()


@pytest.mark.asyncio
async def test_map_stream_primary_failure_survives_cleanup_failures() -> None:
    both_started = asyncio.Event()
    started = 0

    def items() -> Iterator[int]:
        try:
            value = 0
            while True:
                yield value
                value += 1
        finally:
            raise RuntimeError("secondary source close failure")

    async def work(value: int) -> int:
        nonlocal started
        started += 1
        if started == 2:
            both_started.set()
        await both_started.wait()
        if value == 0:
            raise ValueError("primary worker failure")
        try:
            await asyncio.Event().wait()
        finally:
            raise RuntimeError("secondary worker cleanup failure")

    stream = syn.map_stream(work, items(), 2)
    with pytest.raises(ValueError, match="primary worker failure") as exc_info:
        await asyncio.wait_for(anext(stream), timeout=2)

    notes = "\n".join(getattr(exc_info.value, "__notes__", ()))
    assert "secondary worker cleanup failure" in notes
    assert "secondary source close failure" in notes


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_map_stream_rejects_invalid_limit_at_construction(value: object) -> None:
    async def work(item: int) -> int:
        return item

    with pytest.raises(ValueError, match="positive integer"):
        syn.map_stream(work, [1], value)  # type: ignore[arg-type]
