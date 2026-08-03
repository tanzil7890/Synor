"""Regression tests for LazyEnvironment stop/restart generations."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest
import synor as syn
import synor.inspect as syn_inspect
from synor._internal.component_ctx import get_context_from_ctx
from synor._internal.environment import LazyEnvironment


@dataclass(frozen=True)
class _Generation:
    value: int


_GENERATION_KEY = syn.ContextKey[_Generation](
    "test_lazy_environment_restart/generation"
)


def test_sync_app_rebinds_after_lazy_environment_restart(tmp_path: Path) -> None:
    lazy = LazyEnvironment("sync-restart")
    starts = 0
    active = 0

    def lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
        nonlocal starts, active
        starts += 1
        active += 1
        builder.settings.db_path = tmp_path / "state"
        builder.provide(_GENERATION_KEY, _Generation(starts))
        try:
            yield
        finally:
            active -= 1

    lazy.lifespan(lifespan)

    @syn.task
    def main() -> int:
        return syn.use_context(_GENERATION_KEY).value

    app = syn.App(syn.AppConfig(name="sync-restart-app", environment=lazy), main)
    try:
        assert app.update_blocking() == 1
        assert active == 1

        asyncio.run(lazy.stop())
        assert active == 0

        assert app.update_blocking() == 2
        assert active == 1
    finally:
        asyncio.run(lazy.stop())
    assert active == 0


@pytest.mark.asyncio
async def test_async_app_rebinds_after_lazy_environment_restart(
    tmp_path: Path,
) -> None:
    lazy = LazyEnvironment("async-restart")
    starts = 0
    active = 0

    async def lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
        nonlocal starts, active
        starts += 1
        active += 1
        builder.settings.db_path = tmp_path / "state"
        builder.provide(_GENERATION_KEY, _Generation(starts))
        try:
            yield
        finally:
            active -= 1

    lazy.lifespan(lifespan)

    @syn.task
    async def main() -> int:
        return syn.use_context(_GENERATION_KEY).value

    app = syn.App(syn.AppConfig(name="async-restart-app", environment=lazy), main)
    try:
        first_env = await lazy.start()
        first_generation = first_env._generation
        assert await app.update() == 1

        await lazy.stop()
        assert active == 0
        del first_env

        second_env = await lazy.start()
        assert second_env._generation == first_generation + 1
        assert await app.update() == 2
        assert active == 1
    finally:
        await lazy.stop()
    assert active == 0


@pytest.mark.asyncio
async def test_external_stop_cancels_live_update_before_operation_drain(
    tmp_path: Path,
) -> None:
    lazy = LazyEnvironment("external-stop-live-update")
    starts = 0
    resource_active = False
    live_started = asyncio.Event()
    live_cancelled = asyncio.Event()

    async def lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
        nonlocal starts, resource_active
        starts += 1
        # Use one path per generation here because this test deliberately
        # retains the cancelled result traceback long enough to inspect it.
        # Existing restart tests cover reopening the same LMDB path after all
        # explicit first-generation references are released.
        builder.settings.db_path = tmp_path / f"state-{starts}"
        builder.provide(_GENERATION_KEY, _Generation(starts))
        resource_active = True
        try:
            yield
        finally:
            resource_active = False

    lazy.lifespan(lifespan)

    class _BlockingLive:
        async def process(self) -> None:
            return None

        async def process_live(self, operator: syn.LiveComponentOperator) -> None:
            await operator.update_full()
            await operator.mark_ready()
            live_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                live_cancelled.set()
                raise

    @syn.task
    async def main() -> int:
        await syn.spawn(syn.unit_path("live"), _BlockingLive)
        return syn.use_context(_GENERATION_KEY).value

    app = syn.App(
        syn.AppConfig(name="external-stop-live-update-app", environment=lazy), main
    )
    result_task = asyncio.create_task(app.update(live=True).result())
    try:
        await asyncio.wait_for(live_started.wait(), timeout=5.0)
        assert resource_active

        await asyncio.wait_for(lazy.stop(), timeout=5.0)
        assert live_cancelled.is_set()
        assert not resource_active
        with pytest.raises(asyncio.CancelledError):
            await result_task
        # A stopped live generation must not poison the cached App or the next
        # environment generation. Catch-up mode completes ordinarily.
        assert await asyncio.wait_for(app.update().result(), timeout=5.0) == 2
        assert resource_active
    finally:
        await lazy.stop()


@pytest.mark.asyncio
async def test_callback_triggered_stop_is_rejected_without_claiming_transition(
    tmp_path: Path,
) -> None:
    lazy = LazyEnvironment("callback-stop-rejection")
    resource_active = False

    async def lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
        nonlocal resource_active
        builder.settings.db_path = tmp_path / "state"
        resource_active = True
        try:
            yield
        finally:
            resource_active = False

    lazy.lifespan(lifespan)

    @syn.task
    async def main() -> str:
        try:
            await lazy.stop()
        except RuntimeError as error:
            return str(error)
        raise AssertionError("re-entrant stop unexpectedly completed")

    app = syn.App(
        syn.AppConfig(name="callback-stop-rejection-app", environment=lazy), main
    )
    first_env = await lazy.start()
    message = await asyncio.wait_for(app.update().result(), timeout=5.0)
    assert "cannot be called from an operation callback" in message
    assert lazy._env is first_env
    assert lazy._transition is None
    assert resource_active

    await asyncio.wait_for(lazy.stop(), timeout=5.0)
    assert not resource_active


@pytest.mark.asyncio
async def test_sink_callback_stop_is_rejected_before_lifecycle_mutation(
    tmp_path: Path,
) -> None:
    lazy = LazyEnvironment("sink-callback-stop-rejection")
    resource_active = False

    async def lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
        nonlocal resource_active
        builder.settings.db_path = tmp_path / "state"
        resource_active = True
        try:
            yield
        finally:
            resource_active = False

    lazy.lifespan(lifespan)

    class _StopFromSink:
        def __init__(self) -> None:
            self.stop_error: str | None = None
            self.had_component_context: bool | None = None
            self.sink = syn.TargetActionSink.from_async_fn(self._apply)

        async def _apply(self, _context: syn.ContextProvider, _actions: Any, /) -> None:
            try:
                get_context_from_ctx()
            except RuntimeError:
                self.had_component_context = False
            else:
                self.had_component_context = True
            async with asyncio.timeout(1.0):
                try:
                    await lazy.stop()
                except RuntimeError as error:
                    self.stop_error = str(error)
                else:
                    raise AssertionError(
                        "sink callback unexpectedly stopped its environment"
                    )

        def reconcile(
            self,
            key: syn.StableKey,
            desired: str | syn.AbsentType,
            _previous: Any,
            _prev_may_be_missing: bool,
            /,
        ) -> Any:
            if syn.is_absent(desired):
                return None
            return syn.TargetReconcileOutput(
                action=(key, desired), sink=self.sink, tracking_record=desired
            )

    store = _StopFromSink()
    provider = syn.register_root_target_states_provider(
        "test_lazy_environment_restart/sink-stop", store
    )

    @syn.task
    async def main() -> None:
        syn.ensure_target_state(provider.target_state("key", "value"))

    app = syn.App(
        syn.AppConfig(name="sink-callback-stop-rejection-app", environment=lazy),
        main,
    )
    first_env = await lazy.start()

    await asyncio.wait_for(app.update().result(), timeout=5.0)
    assert store.had_component_context is False
    assert store.stop_error is not None
    assert "cannot be called from an operation callback" in store.stop_error
    assert lazy._env is first_env
    assert lazy._transition is None
    assert resource_active

    await asyncio.wait_for(app.update().result(), timeout=5.0)
    assert lazy._env is first_env
    assert lazy._transition is None

    await asyncio.wait_for(lazy.stop(), timeout=5.0)
    assert not resource_active


@pytest.mark.asyncio
async def test_lifespan_startup_rejects_same_environment_start_and_stop(
    tmp_path: Path,
) -> None:
    lazy = LazyEnvironment("startup-lifecycle-reentrancy")
    errors: dict[str, str] = {}

    async def lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
        builder.settings.db_path = tmp_path / "state"

        with pytest.raises(RuntimeError) as start_error:
            async with asyncio.timeout(1.0):
                await lazy.start()
        errors["start"] = str(start_error.value)

        with pytest.raises(RuntimeError) as stop_error:
            async with asyncio.timeout(1.0):
                await lazy.stop()
        errors["stop"] = str(stop_error.value)
        yield

    lazy.lifespan(lifespan)

    env = await asyncio.wait_for(lazy.start(), timeout=5.0)
    assert "LazyEnvironment.start() cannot be called re-entrantly" in errors["start"]
    assert "LazyEnvironment.stop() cannot be called re-entrantly" in errors["stop"]
    assert lazy._env is env
    assert lazy._transition is None

    await asyncio.wait_for(lazy.stop(), timeout=5.0)


@pytest.mark.asyncio
async def test_lifespan_cleanup_rejects_same_environment_start_and_stop(
    tmp_path: Path,
) -> None:
    lazy = LazyEnvironment("cleanup-lifecycle-reentrancy")
    errors: dict[str, str] = {}
    cleanup_finished = asyncio.Event()

    async def lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
        builder.settings.db_path = tmp_path / "state"
        yield

        with pytest.raises(RuntimeError) as start_error:
            async with asyncio.timeout(1.0):
                await lazy.start()
        errors["start"] = str(start_error.value)

        with pytest.raises(RuntimeError) as stop_error:
            async with asyncio.timeout(1.0):
                await lazy.stop()
        errors["stop"] = str(stop_error.value)
        cleanup_finished.set()

    lazy.lifespan(lifespan)

    await asyncio.wait_for(lazy.start(), timeout=5.0)
    await asyncio.wait_for(lazy.stop(), timeout=5.0)

    assert cleanup_finished.is_set()
    assert "LazyEnvironment.start() cannot be called re-entrantly" in errors["start"]
    assert "LazyEnvironment.stop() cannot be called re-entrantly" in errors["stop"]
    assert lazy._env is None
    assert lazy._transition is None


@pytest.mark.asyncio
async def test_concurrent_starts_share_one_published_generation(
    tmp_path: Path,
) -> None:
    lazy = LazyEnvironment("concurrent-start")
    lifespan_entered = asyncio.Event()
    allow_start = asyncio.Event()
    starts = 0

    async def lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
        nonlocal starts
        starts += 1
        builder.settings.db_path = tmp_path / "state"
        lifespan_entered.set()
        await allow_start.wait()
        yield

    lazy.lifespan(lifespan)
    start_tasks = [asyncio.create_task(lazy.start()) for _ in range(8)]
    await asyncio.wait_for(lifespan_entered.wait(), timeout=5.0)
    allow_start.set()

    environments = await asyncio.gather(*start_tasks)
    try:
        assert starts == 1
        assert all(env is environments[0] for env in environments)
        assert {env._generation for env in environments} == {1}
    finally:
        del environments
        await lazy.stop()


@pytest.mark.asyncio
async def test_restart_waits_for_prior_generation_cleanup(tmp_path: Path) -> None:
    lazy = LazyEnvironment("serialized-restart")
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    starts = 0
    active = 0
    max_active = 0

    async def lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
        nonlocal starts, active, max_active
        starts += 1
        generation = starts
        active += 1
        max_active = max(max_active, active)
        builder.settings.db_path = tmp_path / "state"
        builder.provide(_GENERATION_KEY, _Generation(generation))
        try:
            yield
        finally:
            if generation == 1:
                cleanup_started.set()
                await allow_cleanup.wait()
            active -= 1

    lazy.lifespan(lifespan)

    @syn.task
    async def main() -> int:
        return syn.use_context(_GENERATION_KEY).value

    app = syn.App(syn.AppConfig(name="serialized-restart-app", environment=lazy), main)
    assert await app.update() == 1

    stop_task = asyncio.create_task(lazy.stop())
    await asyncio.wait_for(cleanup_started.wait(), timeout=5.0)
    restart_task = asyncio.create_task(app.update().result())
    await asyncio.sleep(0)

    assert not restart_task.done()
    assert active == 1
    assert max_active == 1

    allow_cleanup.set()
    await asyncio.wait_for(stop_task, timeout=5.0)
    assert await asyncio.wait_for(restart_task, timeout=5.0) == 2
    assert max_active == 1

    await lazy.stop()
    assert active == 0


@pytest.mark.asyncio
async def test_cancelled_stop_still_drains_cleanup_before_restart(
    tmp_path: Path,
) -> None:
    lazy = LazyEnvironment("cancelled-stop")
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    starts = 0
    active = 0
    max_active = 0

    async def lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
        nonlocal starts, active, max_active
        starts += 1
        generation = starts
        active += 1
        max_active = max(max_active, active)
        builder.settings.db_path = tmp_path / "state"
        builder.provide(_GENERATION_KEY, _Generation(generation))
        try:
            yield
        finally:
            if generation == 1:
                cleanup_started.set()
                await allow_cleanup.wait()
            active -= 1

    lazy.lifespan(lifespan)

    @syn.task
    async def main() -> int:
        return syn.use_context(_GENERATION_KEY).value

    app = syn.App(syn.AppConfig(name="cancelled-stop-app", environment=lazy), main)
    assert await app.update() == 1

    stop_task = asyncio.create_task(lazy.stop())
    await asyncio.wait_for(cleanup_started.wait(), timeout=5.0)
    stop_task.cancel()
    restart_task = asyncio.create_task(app.update().result())
    await asyncio.sleep(0)

    assert not stop_task.done()
    assert not restart_task.done()
    assert active == 1

    allow_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await stop_task
    assert await asyncio.wait_for(restart_task, timeout=5.0) == 2
    assert max_active == 1

    await lazy.stop()
    assert active == 0


@pytest.mark.asyncio
async def test_environment_stop_drains_running_sync_callback(tmp_path: Path) -> None:
    lazy = LazyEnvironment("stop-callback-drain")
    callback_started = threading.Event()
    release_callback = threading.Event()
    callback_finished = threading.Event()
    resource_active = False

    async def lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
        nonlocal resource_active
        builder.settings.db_path = tmp_path / "state"
        resource_active = True
        try:
            yield
        finally:
            resource_active = False

    lazy.lifespan(lifespan)

    def main() -> None:
        callback_started.set()
        release_callback.wait(timeout=5.0)
        callback_finished.set()

    app = syn.App(syn.AppConfig(name="stop-callback-drain-app", environment=lazy), main)
    update_task = asyncio.create_task(app.update().result())
    assert await asyncio.to_thread(callback_started.wait, 5.0)

    stop_task = asyncio.create_task(lazy.stop())
    await asyncio.sleep(0.05)
    assert not stop_task.done()
    assert resource_active

    release_callback.set()
    assert await asyncio.to_thread(callback_finished.wait, 5.0)
    await asyncio.wait_for(update_task, timeout=5.0)
    await asyncio.wait_for(stop_task, timeout=5.0)
    assert not resource_active


@pytest.mark.asyncio
async def test_cancelled_stop_still_drains_callback_before_cleanup(
    tmp_path: Path,
) -> None:
    lazy = LazyEnvironment("cancelled-stop-callback-drain")
    callback_started = threading.Event()
    release_callback = threading.Event()
    resource_active = False
    starts = 0

    async def lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
        nonlocal resource_active, starts
        starts += 1
        builder.settings.db_path = tmp_path / "state"
        resource_active = True
        try:
            yield
        finally:
            resource_active = False

    lazy.lifespan(lifespan)

    def main() -> None:
        callback_started.set()
        release_callback.wait(timeout=5.0)

    app = syn.App(
        syn.AppConfig(name="cancelled-stop-callback-drain-app", environment=lazy),
        main,
    )
    update_task = asyncio.create_task(app.update().result())
    try:
        assert await asyncio.to_thread(callback_started.wait, 5.0)
        stop_task = asyncio.create_task(lazy.stop())
        await asyncio.sleep(0.05)
        stop_task.cancel()
        await asyncio.sleep(0.05)

        assert not stop_task.done()
        assert resource_active

        release_callback.set()
        await asyncio.wait_for(update_task, timeout=5.0)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(stop_task, timeout=5.0)
        assert not resource_active

        await lazy.start()
        assert starts == 2
        assert resource_active
    finally:
        release_callback.set()
        if not update_task.done():
            update_task.cancel()
        await lazy.stop()


@pytest.mark.asyncio
async def test_closing_generation_rejects_late_operation_entry(tmp_path: Path) -> None:
    env = syn.Environment(syn.Settings(db_path=tmp_path / "state"))
    started = False

    def main() -> None:
        nonlocal started
        started = True

    # LazyEnvironment.stop() closes this gate before it drains admitted
    # operations. The callback gate is a separate, later shutdown phase.
    env._async_context.close_operation_admission()
    await env._async_context.drain_operations_async()
    app = syn.App(syn.AppConfig(name="closed-operation-gate", environment=env), main)

    with pytest.raises(Exception, match="environment is shutting down"):
        await app.update()
    assert not started


@pytest.mark.asyncio
async def test_admitted_update_can_schedule_callback_while_stop_drains_operations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Stop keeps callbacks open for an update admitted before its barrier."""
    lazy = LazyEnvironment("admitted-operation-callback-race")
    starts = 0
    main_started = False
    operation_admitted = asyncio.Event()
    release_operation = asyncio.Event()

    async def lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
        nonlocal starts
        starts += 1
        builder.settings.db_path = tmp_path / "state"
        builder.provide(_GENERATION_KEY, _Generation(starts))
        yield

    lazy.lifespan(lifespan)

    @syn.task
    def main() -> int:
        nonlocal main_started
        main_started = True
        return syn.use_context(_GENERATION_KEY).value

    app = syn.App(
        syn.AppConfig(name="admitted-operation-callback-race-app", environment=lazy),
        main,
    )

    async def _pause_after_admission(self: Any) -> Any:
        env = await self._environment._get_env()
        operation_lease = env._async_context.acquire_operation()
        if self is app and not release_operation.is_set():
            operation_admitted.set()
            await release_operation.wait()
        # Construct/cache the core App only after stop has begun. This also
        # proves shutdown invalidates after the operation drain, preventing a
        # late cache write from retaining the closing generation.
        _bound_env, core_app = self._ensure_core_env_app(env)
        return env, core_app, operation_lease

    monkeypatch.setattr(
        type(app), "_get_core_env_app_for_operation", _pause_after_admission
    )

    admitted_update = asyncio.create_task(app.update().result())
    await asyncio.wait_for(operation_admitted.wait(), timeout=5.0)
    stop_task = asyncio.create_task(lazy.stop())

    async def _wait_until_stop_claims_generation() -> None:
        while True:
            with lazy._lifecycle_lock:
                if lazy._env is None and lazy._transition is not None:
                    return
            await asyncio.sleep(0)

    await asyncio.wait_for(_wait_until_stop_claims_generation(), timeout=5.0)
    assert not stop_task.done()

    release_operation.set()
    assert await asyncio.wait_for(admitted_update, timeout=5.0) == 1
    await asyncio.wait_for(stop_task, timeout=5.0)
    assert main_started
    assert app._core_env_app is None

    assert await app.update() == 2
    await lazy.stop()


@pytest.mark.asyncio
async def test_stop_waits_for_open_app_inspection_iterator(tmp_path: Path) -> None:
    lazy = LazyEnvironment("inspection-operation-admission")

    def lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = tmp_path / "state"
        yield

    lazy.lifespan(lifespan)

    @syn.task
    async def child() -> None:
        return None

    @syn.task
    async def main() -> None:
        await syn.spawn(syn.unit_path("child"), child)

    app = syn.App(
        syn.AppConfig(name="inspection-operation-admission-app", environment=lazy),
        main,
    )
    await app.update()

    entries = cast(AsyncGenerator[Any, None], syn_inspect.iter_stable_paths(app))
    await anext(entries)
    stop_task = asyncio.create_task(lazy.stop())
    await asyncio.sleep(0.05)
    assert not stop_task.done()

    await entries.aclose()
    await asyncio.wait_for(stop_task, timeout=5.0)
