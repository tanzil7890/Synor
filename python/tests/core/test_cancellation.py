"""Tests for the cancellation pipeline (see specs/core/cancellation.md).

Verifies that `_core.cancel_all()` (the same call the CLI's SIGINT handler
makes) propagates from the global token through the AppContext token, into
the per-component spawned tasks, and ultimately reaches Python coroutines
via CancelOnDropPy.

The live-component variant lives alongside other live-component tests in
test_live_component.py.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest
import pytest_asyncio

import synor as syn

from tests import common


@pytest_asyncio.fixture
async def synor_env(request: pytest.FixtureRequest) -> syn.Environment:
    """Bind the Environment to the test's own running event loop.

    These tests assert on `asyncio.Event`s that the app coroutine sets. A
    module-level Environment runs that coroutine on its own background loop,
    and a cross-loop `Event.set()` never wakes the test's loop — the waiter
    only resumes when its own timeout timer fires, which both slows the tests
    to their timeout values and makes the flags race with the assertions.
    """
    return common.create_test_env(__file__, suffix=request.node.name)


@pytest.mark.asyncio
async def test_non_live_global_cancel_terminates_update(
    synor_env: syn.Environment,
) -> None:
    """Global cancellation must reach a non-live component's Python coroutine.

    Regression test for the case where Component::run / run_in_background
    spawned detached tokio tasks that did not watch any cancellation token,
    so dropping the outer App::update future left the spawned task running
    and the Python `process()` coroutine never received CancelledError.
    """
    from synor._internal import core as _core

    started = asyncio.Event()
    cancelled_in_python = asyncio.Event()

    async def _blocking_main() -> None:
        started.set()
        try:
            await asyncio.Event().wait()  # block forever
        except asyncio.CancelledError:
            cancelled_in_python.set()
            raise

    _core.reset_global_cancellation()
    app = syn.App(
        syn.AppConfig(
            name="test_non_live_global_cancel_terminates", environment=synor_env
        ),
        _blocking_main,
    )
    handle = app.update()
    result_task = asyncio.create_task(handle.result())
    try:
        await asyncio.wait_for(started.wait(), timeout=5.0)

        _core.cancel_all()  # simulates SIGINT handler in cli.py

        # Wait for cancellation to actually reach Python. The outer App::update
        # task may return Err immediately when the app token fires, but the
        # inner spawned task that drops the work future and triggers
        # CancelOnDropPy runs async — we need to wait for that propagation.
        await asyncio.wait_for(cancelled_in_python.wait(), timeout=5.0)

        # And the update task itself should also terminate quickly.
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(result_task, timeout=5.0)
    finally:
        if not result_task.done():
            result_task.cancel()
        _core.reset_global_cancellation()


@pytest.mark.asyncio
async def test_app_drop_interrupts_in_flight_update(
    synor_env: syn.Environment,
) -> None:
    """App.drop() must interrupt a concurrent update.

    The app token is shared between update and drop_app. drop_app cancels
    it, which fires the cancel arm in App::update and the per-component
    spawned tasks, propagating CancelledError into Python.
    """
    from synor._internal import core as _core

    started = asyncio.Event()
    cancelled_in_python = asyncio.Event()

    async def _blocking_main() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled_in_python.set()
            raise

    _core.reset_global_cancellation()
    app = syn.App(
        syn.AppConfig(name="test_app_drop_interrupts_update", environment=synor_env),
        _blocking_main,
    )
    handle = app.update()
    result_task = asyncio.create_task(handle.result())
    try:
        await asyncio.wait_for(started.wait(), timeout=5.0)

        # drop_app cancels the app token, which interrupts the running update.
        # Run drop concurrently — it should complete after the update terminates.
        await asyncio.wait_for(app.drop(), timeout=5.0)

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(result_task, timeout=5.0)

        # Same propagation caveat as the global-cancel test above: drop_app
        # returns once the Rust update task exits, while the inner spawned task
        # that drops the work future and triggers CancelOnDropPy still has to
        # deliver CancelledError into Python. Wait for it rather than sampling.
        await asyncio.wait_for(cancelled_in_python.wait(), timeout=5.0)

        assert cancelled_in_python.is_set(), (
            "process coroutine never received CancelledError — "
            "App.drop did not interrupt the in-flight update"
        )
    finally:
        if not result_task.done():
            result_task.cancel()
        _core.reset_global_cancellation()


@pytest.mark.asyncio
async def test_cancelling_result_waits_for_live_callback_cleanup(
    synor_env: syn.Environment,
) -> None:
    """Caller cancellation is a quiescence barrier, not a detached update."""
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    class _BlockingLive:
        async def process(self) -> None:
            return None

        async def process_live(self, operator: syn.LiveComponentOperator) -> None:
            await operator.update_full()
            await operator.mark_ready()
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanup_started.set()
                await release_cleanup.wait()
                cleanup_finished.set()

    @syn.task
    async def _main() -> None:
        await syn.spawn(syn.unit_path("live"), _BlockingLive)

    app = syn.App(
        syn.AppConfig(name="test_result_cancel_drains_live", environment=synor_env),
        _main,
    )
    result_task = asyncio.create_task(app.update(live=True).result())
    try:
        await asyncio.wait_for(started.wait(), timeout=5.0)
        result_task.cancel()
        await asyncio.wait_for(cleanup_started.wait(), timeout=5.0)

        # The public awaiter must remain pending until the cancelled Python
        # callback has completed its asynchronous finally block.
        await asyncio.sleep(0)
        assert not result_task.done()

        release_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(result_task, timeout=5.0)
        assert cleanup_finished.is_set()
    finally:
        release_cleanup.set()
        if not result_task.done():
            result_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await result_task


@pytest.mark.asyncio
async def test_stale_completed_handle_cannot_cancel_later_update(
    synor_env: syn.Environment,
) -> None:
    """Each admitted update owns a distinct cancellation-token generation."""
    run_count = 0
    second_started = asyncio.Event()
    release_second = asyncio.Event()

    async def _main() -> int:
        nonlocal run_count
        run_count += 1
        current = run_count
        if current == 2:
            second_started.set()
            await release_second.wait()
        return current

    app = syn.App(
        syn.AppConfig(
            name="test_stale_handle_cancel_generation", environment=synor_env
        ),
        _main,
    )
    old_handle = app.update()
    assert await old_handle.result() == 1
    assert old_handle._core_handle is not None

    later_result = asyncio.create_task(app.update().result())
    try:
        await asyncio.wait_for(second_started.wait(), timeout=5.0)

        # Simulate a delayed cancellation callback from the already-completed
        # first result future. Its retained token must not address run two.
        old_handle._core_handle.request_cancel()
        await asyncio.sleep(0)
        assert not later_result.done()

        release_second.set()
        assert await asyncio.wait_for(later_result, timeout=5.0) == 2
    finally:
        release_second.set()
        if not later_result.done():
            later_result.cancel()
            with pytest.raises(asyncio.CancelledError):
                await later_result


@pytest.mark.asyncio
async def test_cancel_before_python_task_install_never_starts_coroutine(
    synor_env: syn.Environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dropping the Rust bridge before call_soon runs must not orphan work."""
    from synor._internal import core as _core

    loop = asyncio.get_running_loop()
    original_call_soon_threadsafe = loop.call_soon_threadsafe
    bridge_scheduled = threading.Event()
    queued_callbacks: list[tuple[Any, tuple[Any, ...], dict[str, Any]]] = []
    started = asyncio.Event()

    def _delay_bridge_callback(
        callback: Any, *args: Any, **kwargs: Any
    ) -> asyncio.Handle | None:
        if type(callback).__name__ == "CreateTaskAndBridge":
            queued_callbacks.append((callback, args, kwargs))
            bridge_scheduled.set()
            return None
        return original_call_soon_threadsafe(callback, *args, **kwargs)

    monkeypatch.setattr(loop, "call_soon_threadsafe", _delay_bridge_callback)

    async def _must_not_start() -> None:
        started.set()

    _core.reset_global_cancellation()
    app = syn.App(
        syn.AppConfig(name="test_cancel_before_task_install", environment=synor_env),
        _must_not_start,
    )
    result_task = asyncio.create_task(app.update().result())
    try:
        assert await asyncio.to_thread(bridge_scheduled.wait, 5.0)
        assert len(queued_callbacks) == 1

        _core.cancel_all()
        await asyncio.sleep(0.05)
        assert not result_task.done(), (
            "update reported quiescence before its scheduled Python callback drained"
        )

        # Install after cancellation. The handshake must cancel the newly
        # created asyncio Task before its first step, so _must_not_start() can
        # never execute. Completion may report only after that cancelled task's
        # done callback releases the host-callback lease.
        callback, args, kwargs = queued_callbacks.pop()
        original_call_soon_threadsafe(callback, *args, **kwargs)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(result_task, timeout=5.0)

        assert not started.is_set()
    finally:
        if queued_callbacks:
            callback, args, kwargs = queued_callbacks.pop()
            original_call_soon_threadsafe(callback, *args, **kwargs)
        if not result_task.done():
            result_task.cancel()
        _core.reset_global_cancellation()


@pytest.mark.asyncio
async def test_task_callback_registration_failure_cancels_created_task(
    synor_env: syn.Environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A setup error after task creation must not leave the task runnable."""
    original_ensure_future = asyncio.ensure_future
    cancel_called = asyncio.Event()
    started = asyncio.Event()

    class _RejectingFuture:
        def __init__(self, task: asyncio.Future[Any]) -> None:
            self._task = task

        def add_done_callback(self, _callback: Any) -> None:
            raise RuntimeError("forced add_done_callback failure")

        def cancel(self) -> bool:
            cancel_called.set()
            return self._task.cancel()

    def _reject_registration(awaitable: Any, *args: Any, **kwargs: Any) -> Any:
        return _RejectingFuture(original_ensure_future(awaitable, *args, **kwargs))

    monkeypatch.setattr(asyncio, "ensure_future", _reject_registration)

    async def _must_not_start() -> None:
        started.set()

    app = syn.App(
        syn.AppConfig(name="test_callback_registration_failure", environment=synor_env),
        _must_not_start,
    )
    result_task = asyncio.create_task(app.update().result())
    await cancel_called.wait()
    monkeypatch.setattr(asyncio, "ensure_future", original_ensure_future)

    with pytest.raises(Exception, match="forced add_done_callback failure"):
        await result_task
    await asyncio.sleep(0)
    assert not started.is_set()


@pytest.mark.asyncio
async def test_cancel_drains_running_sync_python_callback(
    synor_env: syn.Environment,
) -> None:
    """Operation completion must wait for an unabortable spawn_blocking call."""
    from synor._internal import core as _core

    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def _blocking_main() -> None:
        started.set()
        release.wait(timeout=5.0)
        finished.set()

    _core.reset_global_cancellation()
    app = syn.App(
        syn.AppConfig(name="test_cancel_drains_sync_callback", environment=synor_env),
        _blocking_main,
    )
    result_task = asyncio.create_task(app.update().result())
    try:
        assert await asyncio.to_thread(started.wait, 5.0)
        _core.cancel_all()
        await asyncio.sleep(0.05)

        assert not result_task.done(), (
            "update reported quiescence while its sync Python callback was still running"
        )

        release.set()
        assert await asyncio.to_thread(finished.wait, 5.0)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(result_task, timeout=5.0)
    finally:
        release.set()
        if not result_task.done():
            result_task.cancel()
        _core.reset_global_cancellation()


@pytest.mark.asyncio
async def test_cancel_does_not_wait_for_unrelated_app_callback(
    synor_env: syn.Environment,
) -> None:
    """An app quiescence barrier must not include another app's callbacks."""
    other_started = threading.Event()
    release_other = threading.Event()
    this_started = asyncio.Event()
    this_cleaned = asyncio.Event()

    def _blocking_other_app() -> None:
        other_started.set()
        release_other.wait(timeout=10.0)

    async def _this_app() -> None:
        this_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            this_cleaned.set()

    other_app = syn.App(
        syn.AppConfig(name="test_scoped_drain_other", environment=synor_env),
        _blocking_other_app,
    )
    this_app = syn.App(
        syn.AppConfig(name="test_scoped_drain_this", environment=synor_env),
        _this_app,
    )
    other_task = asyncio.create_task(other_app.update().result())
    this_task: asyncio.Task[None] | None = None
    try:
        assert await asyncio.to_thread(other_started.wait, 5.0)
        this_task = asyncio.create_task(this_app.update().result())
        await asyncio.wait_for(this_started.wait(), timeout=5.0)

        this_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(this_task, timeout=5.0)
        assert this_cleaned.is_set()
        assert not other_task.done()
    finally:
        release_other.set()
        if this_task is not None and not this_task.done():
            this_task.cancel()
        await asyncio.wait_for(other_task, timeout=5.0)


@pytest.mark.asyncio
async def test_cancelling_watch_waits_for_live_callback_cleanup(
    synor_env: syn.Environment,
) -> None:
    """Cancelling the watch consumer must not detach a live update."""
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    class _BlockingLive:
        async def process(self) -> None:
            return None

        async def process_live(self, operator: syn.LiveComponentOperator) -> None:
            await operator.update_full()
            await operator.mark_ready()
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanup_started.set()
                await release_cleanup.wait()
                cleanup_finished.set()

    @syn.task
    async def _main() -> None:
        await syn.spawn(syn.unit_path("live"), _BlockingLive)

    app = syn.App(
        syn.AppConfig(name="test_watch_cancel_drains_live", environment=synor_env),
        _main,
    )
    handle = app.update(live=True)

    async def _consume() -> None:
        async for _snapshot in handle.watch():
            pass

    watch_task = asyncio.create_task(_consume())
    try:
        await asyncio.wait_for(started.wait(), timeout=5.0)
        # Let the consumer request the next change after the ready snapshot.
        await asyncio.sleep(0)
        watch_task.cancel()
        await asyncio.wait_for(cleanup_started.wait(), timeout=5.0)

        await asyncio.sleep(0)
        assert not watch_task.done()

        release_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(watch_task, timeout=5.0)
        assert cleanup_finished.is_set()
    finally:
        release_cleanup.set()
        if not watch_task.done():
            watch_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await watch_task


@pytest.mark.asyncio
async def test_closing_watch_is_observational_and_result_remains_usable(
    synor_env: syn.Environment,
) -> None:
    """An ordinary watch close must not cancel the update it observes."""
    started = asyncio.Event()
    cleanup_started = asyncio.Event()

    class _BlockingLive:
        async def process(self) -> None:
            return None

        async def process_live(self, operator: syn.LiveComponentOperator) -> None:
            await operator.update_full()
            await operator.mark_ready()
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanup_started.set()

    @syn.task
    async def _main() -> None:
        await syn.spawn(syn.unit_path("live"), _BlockingLive)

    app = syn.App(
        syn.AppConfig(name="test_watch_close_is_observational", environment=synor_env),
        _main,
    )
    handle = app.update(live=True)
    watcher = handle.watch()
    await watcher.__anext__()
    await asyncio.wait_for(started.wait(), timeout=5.0)
    await getattr(watcher, "aclose")()
    await asyncio.sleep(0)
    assert not cleanup_started.is_set()

    result_task = asyncio.create_task(handle.result())
    await asyncio.sleep(0)
    result_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(result_task, timeout=5.0)
    assert cleanup_started.is_set()
