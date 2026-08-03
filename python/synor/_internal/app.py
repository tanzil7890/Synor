from __future__ import annotations

import asyncio
import os
import threading
from collections.abc import AsyncIterator, Awaitable
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    Callable,
    Generic,
    NamedTuple,
    ParamSpec,
    TypeVar,
    overload,
)

from . import core
from .deadline import deadline_for_engine as _deadline_for_engine
from .environment import Environment, LazyEnvironment, _default_env
from .function import (
    AnyCallable,
    AsyncCallable,
    create_core_component_processor,
    fn_ret_deserializer,
)
from .update_stats import (
    _TERMINATED_VERSION,
    UpdateSnapshot,
    UpdateStats,
    UpdateStatus,
    _decode_update_stats,
    _resolve_report_to_stdout,
    _StatsView,
)

P = ParamSpec("P")
R = TypeVar("R")


class _StatsSnapshot(NamedTuple):
    version: int
    ready: bool
    stats: UpdateStats | None


_ENV_MAX_INFLIGHT_COMPONENTS = "SYNOR_MAX_INFLIGHT_COMPONENTS"
_DEFAULT_MAX_INFLIGHT_COMPONENTS = 1024


def _core_result_future(result: Awaitable[Any]) -> asyncio.Future[Any]:
    # PyO3 returns an asyncio Future. Preserve it directly: wrapping an
    # existing Future is unnecessary, and application code may monkeypatch
    # ``asyncio.ensure_future`` while the Rust callback bridge is being tested
    # or instrumented.
    return (
        result if isinstance(result, asyncio.Future) else asyncio.ensure_future(result)
    )


async def _cancel_and_drain_core_update(
    handle: core.UpdateHandle,
    core_result: asyncio.Future[Any],
    caller_cancelled: asyncio.CancelledError,
) -> Any:
    handle.request_cancel()
    while not core_result.done():
        try:
            await asyncio.shield(core_result)
        except asyncio.CancelledError:
            # A repeated Task.cancel() request must not punch through the
            # cleanup barrier. A core-side cancellation ends the loop via
            # done(), while caller-side repeats are simply deferred.
            continue
        except BaseException:
            break

    # Retrieve the terminal result so asyncio does not report an unhandled
    # exception. The caller's cancellation remains the public outcome.
    if core_result.done():
        try:
            core_result.result()
        except BaseException:
            pass
    raise caller_cancelled


async def _await_core_update_result(
    handle: core.UpdateHandle, result: Awaitable[Any]
) -> Any:
    """Await a core update without letting caller cancellation orphan it.

    The Rust operation owns the actual quiescence barrier. Shielding preserves
    that future long enough to request cooperative cancellation and observe the
    barrier, including cancellation-before-task-install and blocking callback
    drains, before propagating the caller's ``CancelledError``.
    """
    core_result = _core_result_future(result)
    try:
        return await asyncio.shield(core_result)
    except asyncio.CancelledError as caller_cancelled:
        # A terminal core cancellation also arrives as CancelledError. If the
        # core future is already done, there is no outer cancellation to defer.
        if core_result.done():
            raise

        return await _cancel_and_drain_core_update(
            handle, core_result, caller_cancelled
        )


class UpdateHandle(Generic[R]):
    """Handle for a running or completed update, providing access to stats and results.

    The handle is also ``Awaitable[R]``, so ``result = await app.update()`` works
    for backward compatibility.
    """

    def __init__(
        self,
        init_coro: Any,  # Coroutine that returns core.UpdateHandle
        main_fn: Any = None,
        preview: bool = False,
    ) -> None:
        self._init_coro = init_coro
        self._core_handle: core.UpdateHandle | None = None
        self._main_fn = main_fn  # used for return type inspection
        self._preview = preview

    async def _ensure_started(self) -> core.UpdateHandle:
        if self._core_handle is None:
            self._core_handle = await self._init_coro
            self._init_coro = None
        return self._core_handle

    def _snapshot_from_handle(
        self,
        handle: core.UpdateHandle,
    ) -> _StatsSnapshot:
        version, ready, raw = handle.stats_snapshot()
        if not raw:
            return _StatsSnapshot(version, ready, None)
        return _StatsSnapshot(version, ready, _decode_update_stats(raw))

    def stats(self) -> UpdateStats | None:
        """Returns a snapshot of the latest stats, or None if not yet started."""
        if self._core_handle is None:
            return None
        return self._snapshot_from_handle(self._core_handle).stats

    async def watch(self) -> AsyncIterator[UpdateSnapshot[R]]:
        """Async iterator that yields progress snapshots.

        Yields UpdateSnapshot with status:
        - RUNNING while the update is in progress (not yet ready)
        - READY when the root component is ready (initial processing caught up)

        In live mode, after the initial READY, continues yielding RUNNING snapshots
        as stats update from incremental processing. When terminated, yields a final
        READY snapshot with the result set.

        On error, raises the exception directly from the iterator.
        """
        if self._preview:
            raise TypeError("watch() is not supported when preview=True")
        handle = await self._ensure_started()
        last_version = 0
        while True:
            try:
                version = await handle.changed()
            except asyncio.CancelledError as caller_cancelled:
                # `watch()` is a consuming wait just like `result()`. If its
                # caller is cancelled while waiting for the next snapshot,
                # cancel the update and observe the same quiescence barrier.
                # GeneratorExit from an ordinary `break`/`aclose` is left
                # observational so callers may switch from watch to result.
                core_result = _core_result_future(handle.result())
                await _cancel_and_drain_core_update(
                    handle, core_result, caller_cancelled
                )
                raise AssertionError("cancellation drain unexpectedly returned")

            # Check termination before dedup — notify_terminated() sends
            # TERMINATED_VERSION on the watch channel without updating the
            # stats version, so the dedup check would skip it.
            if version >= _TERMINATED_VERSION:
                snap = self._snapshot_from_handle(handle)
                pyvalue: Any = await _await_core_update_result(handle, handle.result())
                result: R = pyvalue.get(fn_ret_deserializer(self._main_fn))
                if snap.stats is not None:
                    yield UpdateSnapshot(
                        stats=snap.stats, status=UpdateStatus.READY, result=result
                    )
                return

            # Snapshot the actual stats (version may differ from notification)
            snap = self._snapshot_from_handle(handle)

            if snap.version == last_version:
                continue  # no actual change since last yield
            last_version = snap.version

            if snap.stats is not None:
                status = UpdateStatus.READY if snap.ready else UpdateStatus.RUNNING
                yield UpdateSnapshot(stats=snap.stats, status=status, result=None)

    async def result(self) -> R:
        """Await the update result. Raises on error."""
        handle = await self._ensure_started()
        if self._preview:
            await _await_core_update_result(handle, handle.result())
            from .target_state import _unwrap_target_action

            return [  # type: ignore[return-value]
                _unwrap_target_action(action)
                for action in handle.take_preview_actions()
            ]
        pyvalue: Any = await _await_core_update_result(handle, handle.result())
        return pyvalue.get(fn_ret_deserializer(self._main_fn))  # type: ignore[no-any-return]

    def __await__(self) -> Any:
        return self.result().__await__()


class DropHandle(_StatsView[core.DropHandle]):
    """Handle for a running or completed drop operation."""

    def __init__(self, core_handle: core.DropHandle) -> None:
        self._core_handle = core_handle

    async def result(self) -> None:
        """Await the drop completion. Raises on error."""
        await self._core_handle.result()

    def __await__(self) -> Any:
        return self.result().__await__()


async def show_progress(
    handle: UpdateHandle[R], *, refresh_interval: timedelta | None = None
) -> R:
    """Run the operation with progress display (async). Consumes the handle.

    ``refresh_interval`` overrides the default refresh interval.
    """
    core_handle = await handle._ensure_started()
    refresh_interval_secs = (
        refresh_interval.total_seconds() if refresh_interval is not None else None
    )
    pyvalue: Any = await _await_core_update_result(
        core_handle,
        core.show_progress(core_handle, refresh_interval_secs),
    )
    return pyvalue.get(fn_ret_deserializer(handle._main_fn))  # type: ignore[no-any-return]


@dataclass(frozen=True)
class AppConfig:
    name: str
    environment: Environment | LazyEnvironment = _default_env
    max_inflight_components: int | None = None


class App(Generic[P, R]):
    """Unified App class with both async and sync methods."""

    _name: str
    _main_fn: AnyCallable[P, R]
    _app_args: tuple[Any, ...]
    _app_kwargs: dict[str, Any]
    _environment: Environment | LazyEnvironment

    _lock: threading.Lock
    _core_env_app: tuple[Environment, core.App] | None
    _core_env_app_generation: int | None

    @overload
    def __init__(
        self,
        name_or_config: str | AppConfig,
        main_fn: AsyncCallable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None: ...
    @overload
    def __init__(
        self,
        name_or_config: str | AppConfig,
        main_fn: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None: ...
    def __init__(
        self,
        name_or_config: str | AppConfig,
        main_fn: Any,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        if isinstance(name_or_config, str):
            config = AppConfig(name=name_or_config)
        else:
            config = name_or_config

        self._name = config.name
        self._main_fn = main_fn
        self._app_args = tuple(args)
        self._app_kwargs = dict(kwargs)
        self._environment = config.environment

        max_inflight = config.max_inflight_components
        if max_inflight is None:
            env_val = os.environ.get(_ENV_MAX_INFLIGHT_COMPONENTS)
            if env_val is not None:
                max_inflight = int(env_val)
            else:
                max_inflight = _DEFAULT_MAX_INFLIGHT_COMPONENTS
        if type(max_inflight) is not int:
            raise TypeError("max_inflight_components must be an int")
        if max_inflight < 1:
            raise ValueError("max_inflight_components must be at least 1")
        self._max_inflight_components = max_inflight

        self._lock = threading.Lock()
        self._core_env_app = None
        self._core_env_app_generation = None

        # Register this app with its environment's info
        config.environment._info.register_app(self._name, self)

    async def _get_core_env_app_for_operation(
        self,
    ) -> tuple[Environment, core.App, core.OperationLease]:
        while True:
            env = await self._environment._get_env()
            try:
                operation_lease = env._async_context.acquire_operation()
            except RuntimeError:
                if isinstance(self._environment, LazyEnvironment):
                    # Stop won the admission race. Resolve the next published
                    # generation and retry instead of touching the closing one.
                    continue
                raise
            _bound_env, core_app = self._ensure_core_env_app(env)
            return env, core_app, operation_lease

    def _get_core_env_app_sync_for_operation(
        self,
    ) -> tuple[Environment, core.App, core.OperationLease]:
        while True:
            env = self._environment._get_env_sync()
            try:
                operation_lease = env._async_context.acquire_operation()
            except RuntimeError:
                if isinstance(self._environment, LazyEnvironment):
                    continue
                raise
            _bound_env, core_app = self._ensure_core_env_app(env)
            return env, core_app, operation_lease

    def _ensure_core_env_app(self, env: Environment) -> tuple[Environment, core.App]:
        with self._lock:
            if (
                self._core_env_app is None
                or self._core_env_app[0] is not env
                or self._core_env_app_generation != env._generation
            ):
                self._core_env_app = (
                    env,
                    core.App(self._name, env._core_env, self._max_inflight_components),
                )
                self._core_env_app_generation = env._generation
            return self._core_env_app

    def _invalidate_environment(self, env: Environment) -> None:
        """Drop a cached core binding when a lazy environment stops."""
        with self._lock:
            if self._core_env_app is not None and self._core_env_app[0] is env:
                self._core_env_app = None
                self._core_env_app_generation = None

    def update(
        self,
        *,
        full_reprocess: bool = False,
        live: bool = False,
        preview: bool = False,
    ) -> UpdateHandle[R]:
        """
        Start an update and return a handle for tracking progress and awaiting the result.

        The handle is ``Awaitable[R]``, so ``result = await app.update()`` works
        for backward compatibility.

        Args:
            full_reprocess: If True, reprocess everything and invalidate existing caches.
            live: If True, run in live mode (live components continue processing
                after mark_ready).
            preview: If True, compute target actions without applying them.
                The handle's result will be a list of raw action objects.

        Returns:
            An UpdateHandle that provides access to stats(), watch(), and result().
        """
        return self._update_controlled(
            full_reprocess=full_reprocess,
            live=live,
            preview=preview,
            _strict_effects=False,
        )

    def _update_controlled(
        self,
        *,
        full_reprocess: bool = False,
        live: bool = False,
        preview: bool = False,
        _strict_effects: bool,
    ) -> UpdateHandle[R]:
        """Start an update with the runtime's internal target-effect policy."""

        if type(_strict_effects) is not bool:
            raise TypeError("_strict_effects must be a bool")
        deadline_context = _deadline_for_engine()

        async def _init() -> core.UpdateHandle:
            (
                env,
                core_app,
                operation_lease,
            ) = await self._get_core_env_app_for_operation()
            root_path = core.StablePath()
            processor = create_core_component_processor(
                self._main_fn,
                env,
                root_path,
                self._app_args,
                self._app_kwargs,
                deadline_context=deadline_context,
            )
            return core_app.update_async(
                processor,
                full_reprocess=full_reprocess,
                live=live,
                preview=preview,
                strict_effects=_strict_effects,
                host_ctx=env._context_provider,
                deadline=deadline_context,
                operation_lease=operation_lease,
            )

        return UpdateHandle(
            _init(),
            main_fn=self._main_fn,
            preview=preview,
        )

    def update_blocking(
        self,
        *,
        report_to_stdout: bool | timedelta = False,
        full_reprocess: bool = False,
        live: bool = False,
        preview: bool = False,
    ) -> R | list[Any]:
        """
        Update the app synchronously (run the app once to process all pending changes).

        Args:
            report_to_stdout: If truthy, periodically report processing stats to
                stdout. Pass a ``timedelta`` to set the refresh interval; ``True``
                uses the default interval.
            full_reprocess: If True, reprocess everything and invalidate existing caches.
            live: If True, run in live mode (live components continue processing
                after mark_ready).
            preview: If True, compute target actions without applying them.
                Returns a list of raw action objects instead of the main function result.

        Returns:
            The result of the main function, or a list of actions in preview mode.
        """
        env, core_app, operation_lease = self._get_core_env_app_sync_for_operation()
        root_path = core.StablePath()
        deadline_context = _deadline_for_engine()
        processor = create_core_component_processor(
            self._main_fn,
            env,
            root_path,
            self._app_args,
            self._app_kwargs,
            deadline_context=deadline_context,
        )
        report, refresh_interval_secs = _resolve_report_to_stdout(report_to_stdout)
        pyvalue: Any = core_app.update(
            processor,
            full_reprocess=full_reprocess,
            host_ctx=env._context_provider,
            report_to_stdout=report,
            refresh_interval_secs=refresh_interval_secs,
            live=live,
            preview=preview,
            deadline=deadline_context,
            operation_lease=operation_lease,
        )
        if preview:
            from .target_state import _unwrap_target_action

            return [_unwrap_target_action(action) for action in pyvalue]
        return pyvalue.get(fn_ret_deserializer(self._main_fn))  # type: ignore[no-any-return]

    async def drop(self) -> None:
        """
        Drop the app asynchronously, reverting all its target states and clearing its database.

        This will:
        - Delete all target states created by the app (e.g., drop tables, delete rows)
        - Clear the app's internal state database
        """
        env, core_app, operation_lease = await self._get_core_env_app_for_operation()
        drop_handle = core_app.drop_async(
            env._context_provider, operation_lease=operation_lease
        )
        await drop_handle.result()

    def drop_blocking(self, *, report_to_stdout: bool | timedelta = False) -> None:
        """
        Drop the app synchronously, reverting all its target states and clearing its database.

        This will:
        - Delete all target states created by the app (e.g., drop tables, delete rows)
        - Clear the app's internal state database

        Args:
            report_to_stdout: If truthy, periodically report processing stats to
                stdout. Pass a ``timedelta`` to set the refresh interval; ``True``
                uses the default interval.
        """
        env, core_app, operation_lease = self._get_core_env_app_sync_for_operation()
        report, refresh_interval_secs = _resolve_report_to_stdout(report_to_stdout)
        core_app.drop(
            env._context_provider,
            report_to_stdout=report,
            refresh_interval_secs=refresh_interval_secs,
            operation_lease=operation_lease,
        )
