from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterable, Coroutine
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Concatenate,
    Generic,
    Iterable,
    ParamSpec,
    TypeVar,
    cast,
    overload,
)

from . import core, environment
from .app import App, AppConfig, DropHandle, UpdateHandle, show_progress
from .deadline import (
    DeadlineExceededError,
    deadline_for_engine as _deadline_for_engine,
    check_cancellation,
    timeout,
)
from .update_stats import (
    ComponentStats,
    StatsGroupHandle,
    UpdateSnapshot,
    UpdateStats,
    UpdateStatus,
)
from .pending_marker import ResolvesTo
from .component_ctx import (
    UnitPath,
    ExceptionContext,
    ExceptionHandler,
    build_child_path,
    get_context_from_ctx,
    exception_handler,
    stats_group,
)


from .stable_path import StableKey
from .batching import RetryWithSmallerBatch
from .function import (
    AnyCallable,
    AsyncCallable,
    LogicTracking,
    create_core_component_processor,
    task,
    fn_ret_deserializer,
)
from .stable_path import Symbol
from .target_state import (
    TargetState,
    TargetStateProvider,
    TargetHandler,
    ensure_target_state_with_child,
)
from .live_component import (
    LiveComponent,
    LiveComponentOperator,
    LiveMapFeed,
    LiveMapView,
    LiveMapSubscriber,
    _MountEachLiveComponent,
    auto_refresh,
    check_not_in_process_live,
    is_live_component_class,
)
from synor.connectorkits import default_subpath_name as _default_subpath_name

# ============================================================================
# Re-exports from internal modules (shared types)
# ============================================================================

from .context_keys import ContextKey, ContextProvider

from .target_state import (
    ChildTargetDef,
    TargetReconcileOutput,
    TargetActionSink,
    TargetSinkCapabilities,
    TargetSinkQueueStats,
    PendingTargetStateProvider,
    ensure_target_state,
    register_root_target_states_provider,
)

from .environment import Environment, EnvironmentBuilder, LifespanFn
from .environment import lifespan

from .runner import (
    GPU,
    GPUPool,
    GPURunner,
    Runner,
    configure_gpu_pool,
    current_gpu,
    current_gpus,
    current_gpu_fraction,
)

from .memo_fingerprint import (
    memo_fingerprint,
    register_memo_key_function,
    NotMemoKeyable,
)

from .serde import (
    unpickle_safe,
    serialize_by_pickle,
    make_deserialize_fn,
    get_deserialize_fn,
    DeserializeFn,
)

from .pending_marker import PendingS, ResolvedS, MaybePendingS

from .component_ctx import (
    ComponentContext,
    unit_path,
    use_context,
    get_component_context,
)

from .setting import Settings, LmdbSettings

from .stable_path import ROOT_PATH, StablePath

from .typing import AbsentType, ABSENT, is_absent, MemoStateOutcome


# ============================================================================
# Mount APIs (async only)
# ============================================================================

P = ParamSpec("P")
K = TypeVar("K")
T = TypeVar("T")
ReturnT = TypeVar("ReturnT")
ResolvedT = TypeVar("ResolvedT")

_ValueT = TypeVar("_ValueT")
_ChildHandlerT = TypeVar("_ChildHandlerT", bound="TargetHandler[Any, Any, Any] | None")
_REPORTED_FAILURE_MARKER = "__synor_failure_reported_v1__"


def _read_reported_failure_marker(error: BaseException) -> bool:
    """Read Synor's internal one-crossing reporting marker defensively."""
    try:
        return bool(getattr(error, _REPORTED_FAILURE_MARKER, False))
    except Exception:  # noqa: BLE001 - arbitrary exception attribute behavior
        return False


def _take_reported_failure_marker(error: BaseException) -> bool:
    """Remove and return Synor's internal one-crossing reporting marker."""
    reported = _read_reported_failure_marker(error)
    try:
        delattr(error, _REPORTED_FAILURE_MARKER)
    except AttributeError:
        pass
    except Exception:  # noqa: BLE001, S110 - unusual immutable exception
        pass
    return reported


def _restore_reported_failure_marker(error: BaseException) -> None:
    """Restore internal routing state before the same operation re-raises."""
    try:
        setattr(error, _REPORTED_FAILURE_MARKER, True)
    except Exception:  # noqa: BLE001, S110 - unusual immutable exception
        pass


class ReadinessOutcome:
    """Base type for an explicit component readiness result."""

    __slots__ = ()


@dataclass(frozen=True, slots=True)
class Succeeded(ReadinessOutcome):
    """The component and its durable downstream sync completed."""


@dataclass(frozen=True, slots=True)
class Failed(ReadinessOutcome):
    """The component failed with ``error`` as its terminal cause."""

    error: BaseException


@dataclass(frozen=True, slots=True)
class Cancelled(ReadinessOutcome):
    """The component was cancelled before durable success."""


@dataclass(frozen=True, slots=True)
class Superseded(ReadinessOutcome):
    """A newer live operation displaced this operation at the same path."""


class SpawnHandle:
    """Handle for components started with ``spawn()`` or ``spawn_each()``.

    ``ready()`` returns ``None`` after durable success and raises after any
    component failure. Exception handlers observe failures; they cannot turn a
    failed component into a successful readiness result.
    """

    __slots__ = (
        "_cores",
        "_failure_reported",
        "_outcome",
        "_outcome_lock",
    )

    _cores: list[core.ComponentMountHandle]
    _failure_reported: bool
    _outcome: ReadinessOutcome | None
    _outcome_lock: asyncio.Lock

    def __init__(self, core_handles: list[core.ComponentMountHandle]) -> None:
        self._cores = core_handles
        self._failure_reported = False
        self._outcome = None
        self._outcome_lock = asyncio.Lock()

    async def ready(self) -> None:
        """Wait until all processing units are ready. Can be called multiple times."""
        check_cancellation()
        async with self._outcome_lock:
            check_cancellation()
            outcome = await self._resolve_outcome_locked()
            # Resolve through the explicit terminal state so caller
            # cancellation remains distinguishable from component
            # cancellation. Once terminal, no native future is needed for
            # repeatability or routing state.
            self._cores.clear()

        check_cancellation()
        if isinstance(outcome, Failed):
            if self._failure_reported:
                _restore_reported_failure_marker(outcome.error)
            raise outcome.error
        if isinstance(outcome, Cancelled):
            raise asyncio.CancelledError(
                "component was cancelled before reaching durable readiness"
            )

    async def _resolve_outcome_locked(self) -> ReadinessOutcome:
        """Populate the typed outcome while ``_outcome_lock`` is held."""
        if self._outcome is None:
            first_failure: Failed | None = None
            first_failure_reported = False
            saw_cancelled = False
            saw_succeeded = False
            for core_handle in self._cores:
                kind, error = await core_handle.outcome_async()
                if kind == "failed":
                    if error is None:
                        raise RuntimeError(
                            "native failed readiness outcome omitted its error"
                        )
                    reported = _take_reported_failure_marker(error)
                    if first_failure is None:
                        first_failure = Failed(error)
                        first_failure_reported = reported
                elif kind == "cancelled":
                    saw_cancelled = True
                elif kind == "succeeded":
                    saw_succeeded = True
                elif kind == "superseded":
                    pass
                else:
                    raise RuntimeError(f"unknown native readiness outcome: {kind!r}")
            if first_failure is not None:
                self._outcome = first_failure
                self._failure_reported = first_failure_reported
            elif saw_cancelled:
                self._outcome = Cancelled()
            elif saw_succeeded or not self._cores:
                self._outcome = Succeeded()
            else:
                self._outcome = Superseded()
        return self._outcome

    async def outcome(self) -> ReadinessOutcome:
        """Wait and return the typed terminal outcome. Can be called repeatedly.

        Unlike :meth:`ready`, this method does not raise for component failure,
        cancellation, or supersession. Cancellation of the *calling task* and
        an active Synor deadline still interrupt the wait normally.
        """

        check_cancellation()
        if self._outcome is not None:
            if isinstance(self._outcome, Failed):
                _take_reported_failure_marker(self._outcome.error)
            return self._outcome

        async with self._outcome_lock:
            check_cancellation()
            outcome = await self._resolve_outcome_locked()
            # A terminal Python outcome is now cached independently. Release
            # native shared futures before application code can retain this
            # handle (and, transitively, a completed child context).
            self._cores.clear()
            if isinstance(outcome, Failed):
                # Reported/unreported is internal routing state, not public
                # exception data. ready() restores it from `_failure_reported`
                # if this same operation is subsequently awaited.
                _take_reported_failure_marker(outcome.error)

        check_cancellation()
        return outcome


@overload
async def call(
    subpath: UnitPath,
    processor_fn: AsyncCallable[P, ResolvesTo[ReturnT]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def call(
    subpath: UnitPath,
    processor_fn: Callable[P, ResolvesTo[ReturnT]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def call(
    subpath: UnitPath,
    processor_fn: AsyncCallable[P, ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def call(
    subpath: UnitPath,
    processor_fn: Callable[P, ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def call(
    processor_fn: AsyncCallable[P, ResolvesTo[ReturnT]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def call(
    processor_fn: Callable[P, ResolvesTo[ReturnT]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def call(
    processor_fn: AsyncCallable[P, ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
@overload
async def call(
    processor_fn: Callable[P, ReturnT],
    *args: P.args,
    **kwargs: P.kwargs,
) -> ReturnT: ...
async def call(*pos_args: Any, **kwargs: Any) -> Any:
    """
    Mount a dependent processing component and return its result.

    The child component cannot refresh independently — re-executing the child
    requires re-executing the parent. The ``use_`` prefix (consistent with
    ``use_context()``) signals that the caller creates a dependency on the
    child's result.

    Accepts an optional ``UnitPath`` as the first argument. When omitted,
    the subpath is auto-derived from ``Symbol(fn.__name__)``.

    Args:
        subpath: Optional component subpath. Auto-derived from fn.__name__ when omitted.
        processor_fn: The function to run as the processing unit processor.
        *args: Arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The return value of processor_fn.

    Example:
        target = await syn.call(declare_table_target, table_name)

        # With explicit subpath:
        target = await syn.call(
            syn.unit_path("setup"), declare_table_target, table_name
        )
    """
    check_cancellation()
    if pos_args and isinstance(pos_args[0], UnitPath):
        subpath = pos_args[0]
        processor_fn = pos_args[1]
        args = pos_args[2:]
    else:
        processor_fn = pos_args[0]
        args = pos_args[1:]
        name = _default_subpath_name(processor_fn)
        if name is None:
            raise TypeError(
                "use_mount() requires a UnitPath when the function has no "
                "__name__. Provide an explicit subpath as the first argument."
            )
        subpath = UnitPath(Symbol(name))

    check_not_in_process_live("syn.call")

    if is_live_component_class(processor_fn):
        raise TypeError(
            "LiveComponent classes cannot be used with use_mount(). "
            "Use mount() instead."
        )

    parent_ctx = get_context_from_ctx()
    child_path = build_child_path(parent_ctx, subpath)
    # One explicit value travels through core: capture once, pass to both
    # the processor (ContextVar restore in the wrapper) and the engine call.
    deadline_context = _deadline_for_engine()

    processor = create_core_component_processor(
        processor_fn,
        parent_ctx._env,
        child_path,
        args,
        kwargs,
        deadline_context=deadline_context,
    )
    core_handle = await core.use_mount_async(
        processor,
        child_path,
        parent_ctx._core_processor_ctx,
        parent_ctx._core_fn_call_ctx,
        deadline_context,
    )
    pyvalue = await core_handle.result_async(parent_ctx._core_processor_ctx)
    return pyvalue.get(fn_ret_deserializer(processor_fn))


async def _mount_live_component(
    parent_ctx: ComponentContext,
    child_path: core.StablePath,
    instance: Any,
    *,
    core_parent_ctx: core.ComponentProcessorContext | None = None,
) -> SpawnHandle:
    """Mount a pre-constructed LiveComponent instance.

    Wraps `instance.process_live(operator)` in `_process_live_wrapper` so
    `_in_process_live = True` is set inside the asyncio Task that runs
    the body (the wrapper Coroutine inherits this `Context` value through
    asyncio's standard Task-context inheritance, and any `syn.spawn*`
    call within will raise).
    """
    from .live_component import _process_live_wrapper

    controller, readiness_handle = await core.mount_live_async(
        child_path,
        core_parent_ctx or parent_ctx._core_processor_ctx,
        parent_ctx._core_fn_call_ctx,
        parent_ctx._core_processor_ctx.live,
    )

    operator = LiveComponentOperator(controller, instance, parent_ctx._env, child_path)

    controller.start(_process_live_wrapper(instance, operator))

    return SpawnHandle([readiness_handle])


@overload
async def spawn(
    subpath: UnitPath,
    processor_fn: AnyCallable[P, Any],
    *args: P.args,
    **kwargs: P.kwargs,
) -> SpawnHandle: ...


@overload
async def spawn(
    processor_fn: AnyCallable[P, Any],
    *args: P.args,
    **kwargs: P.kwargs,
) -> SpawnHandle: ...


async def spawn(*pos_args: Any, **kwargs: Any) -> SpawnHandle:
    """
    Mount a processing unit in the background and return a handle to wait until ready.

    Accepts an optional ``UnitPath`` as the first argument. When omitted,
    the subpath is auto-derived from ``Symbol(fn.__name__)``.

    Args:
        subpath: Optional component subpath. Auto-derived from fn.__name__ when omitted.
        processor_fn: The function to run as the processing unit processor.
            Can also be a LiveComponent class.
        *args: Arguments to pass to the function (or LiveComponent constructor).
        **kwargs: Keyword arguments to pass to the function (or LiveComponent constructor).

    Returns:
        A handle that can be used to wait until the processing unit is ready.

    Example:
        await syn.spawn(process_file, file, target)

        # With explicit subpath:
        await syn.spawn(syn.unit_path("process", filename), process_file, file, target)
    """
    check_cancellation()
    check_not_in_process_live("syn.spawn")

    if pos_args and isinstance(pos_args[0], UnitPath):
        subpath = pos_args[0]
        processor_fn = pos_args[1]
        args = pos_args[2:]
    else:
        processor_fn = pos_args[0]
        args = pos_args[1:]
        name = _default_subpath_name(processor_fn)
        if name is None:
            raise TypeError(
                "mount() requires a UnitPath when the function has no "
                "__name__. Provide an explicit subpath as the first argument."
            )
        subpath = UnitPath(Symbol(name))

    parent_ctx = get_context_from_ctx()
    child_path = build_child_path(parent_ctx, subpath)

    if is_live_component_class(processor_fn):
        instance = processor_fn(*args, **kwargs)
        return await _mount_live_component(parent_ctx, child_path, instance)

    processor = create_core_component_processor(
        processor_fn, parent_ctx._env, child_path, args, kwargs
    )
    resolved = parent_ctx.resolve_exception_handler(
        stable_path=child_path.to_string(),
        processor_name=getattr(processor_fn, "__qualname__", None),
        mount_kind="mount",
    )
    core_handle = await core.mount_async(
        processor,
        child_path,
        parent_ctx._core_processor_ctx,
        parent_ctx._core_fn_call_ctx,
        resolved,
    )
    return SpawnHandle([core_handle])


_ItemsType = (
    Iterable[tuple[StableKey, T]]
    | AsyncIterable[tuple[StableKey, T]]
    | LiveMapFeed[StableKey, T]
)


@overload
async def spawn_each(
    subpath: UnitPath,
    fn: AnyCallable[Concatenate[T, P], Any],
    items: _ItemsType[T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> SpawnHandle: ...


@overload
async def spawn_each(
    fn: AnyCallable[Concatenate[T, P], Any],
    items: _ItemsType[T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> SpawnHandle: ...


async def spawn_each(*pos_args: Any, **kwargs: Any) -> SpawnHandle:
    """
    Mount one independent component per item in a keyed iterable.

    Accepts an optional ``UnitPath`` as the first argument. When omitted,
    the subpath is auto-derived from ``Symbol(fn.__name__)``.

    When *items* is a ``LiveMapFeed`` or ``LiveMapView``, an internal
    ``LiveComponent`` is created to handle live watching automatically.

    Args:
        subpath: Optional component subpath. Auto-derived from fn.__name__ when omitted.
        fn: The function to run for each item — the item value is passed as the
            first argument. May also be a LiveComponent class, in which case one
            live component instance is created per item (the item value is passed
            as the first constructor argument, mirroring the plain-function shape).
        items: A keyed iterable of (key, value) pairs, or a LiveMapFeed/LiveMapView for live mode.
        *args: Additional arguments passed to fn after the item value.
        **kwargs: Additional keyword arguments passed to fn.

    Returns:
        A handle that can be used to wait until all processing units are ready.
    """
    check_cancellation()
    check_not_in_process_live("syn.spawn_each")

    if pos_args and isinstance(pos_args[0], UnitPath):
        subpath = pos_args[0]
        fn = pos_args[1]
        items = pos_args[2]
        extra_args = pos_args[3:]
    else:
        fn = pos_args[0]
        items = pos_args[1]
        extra_args = pos_args[2:]
        name = _default_subpath_name(fn)
        if name is None:
            raise TypeError(
                "mount_each() requires a UnitPath when the function has no "
                "__name__. Provide an explicit subpath as the first argument."
            )
        subpath = UnitPath(Symbol(name))

    parent_ctx = get_context_from_ctx()
    child_path = build_child_path(parent_ctx, subpath)

    if isinstance(items, LiveMapFeed):
        # Live data source: the per-item `fn` (whether a plain function or a
        # LiveComponent class) is dispatched through `mount()` / `operator.update()`
        # inside `_MountEachLiveComponent`, both of which already handle live
        # component classes — so no special-casing of `fn` is needed here.
        instance = _MountEachLiveComponent(items, fn, extra_args, kwargs)
        return await _mount_live_component(parent_ctx, child_path, instance)

    # Static data source: mount one component per item. When `fn` is a
    # LiveComponent class, each item gets its own live component instance
    # (same path as `mount(LiveCompClass)`, just looped per item).
    fn_is_live = is_live_component_class(fn)
    group_ctx, group_handle = parent_ctx._core_processor_ctx.begin_mount_group()

    async def _mount_one(key: StableKey, item: Any) -> None:
        item_path = child_path.concat(key)
        if fn_is_live:
            instance = fn(item, *extra_args, **kwargs)
            await _mount_live_component(
                parent_ctx,
                item_path,
                instance,
                core_parent_ctx=group_ctx,
            )
            return
        processor = create_core_component_processor(
            fn, parent_ctx._env, item_path, (item, *extra_args), kwargs
        )
        resolved = parent_ctx.resolve_exception_handler(
            stable_path=item_path.to_string(),
            processor_name=getattr(fn, "__qualname__", None),
            mount_kind="mount_each",
        )
        core_handle = await core.mount_async(
            processor,
            item_path,
            group_ctx,
            parent_ctx._core_fn_call_ctx,
            resolved,
        )
        # Readiness is registered in `group_ctx` before mount_async returns.
        # Dropping the individual handle does not cancel the component; the
        # group owns the aggregate outcome with constant host-side state.
        del core_handle

    try:
        if isinstance(items, AsyncIterable):
            async for key, item in items:
                await _mount_one(key, item)
        else:
            for key, item in items:
                await _mount_one(key, item)
    finally:
        group_ctx.end_mount_group()
    return SpawnHandle([group_handle])


# Keep map task failures wrapped so a user function can return an Exception
# object as a normal value without being confused with a failed task.
@dataclass(frozen=True, slots=True)
class _MapTaskSuccess(Generic[ReturnT]):
    value: ReturnT


@dataclass(frozen=True, slots=True)
class _MapTaskFailure:
    error: Exception


@dataclass(frozen=True, slots=True)
class _MapTaskCancelled:
    error: asyncio.CancelledError


async def map(
    fn: Callable[Concatenate[T, P], Coroutine[Any, Any, ReturnT]],
    items: Iterable[T] | AsyncIterable[T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> list[ReturnT]:
    """
    Run a function concurrently on each item in an iterable.
    No processing components are created — this is pure concurrent execution
    (async tasks) within the current component.

    Once a task is started, ``map`` waits for it to finish even if another
    task hits a deadline. If multiple tasks fail, the first failure in input
    order is raised. All item tasks are scheduled without an internal
    concurrency ceiling; both the task set and ordered result list therefore
    retain O(n) state until the call completes.

    Args:
        fn: The function to apply to each item. The item is passed as the first argument.
        items: The items to iterate (sync or async).
        *args: Additional passthrough arguments to fn (appended after the item).
        **kwargs: Additional passthrough keyword arguments to fn.

    Returns:
        Results from each invocation.
    """
    check_cancellation()

    indexed_results: list[
        _MapTaskSuccess[ReturnT] | _MapTaskFailure | _MapTaskCancelled | None
    ] = []

    async def _run_one(index: int, item: T) -> None:
        try:
            # A task may start after the caller's deadline moved forward while
            # earlier tasks were being scheduled.
            check_cancellation()
            result = await fn(item, *args, **kwargs)
            # Do not let a value returned after the deadline look successful to
            # the map caller.
            check_cancellation()
            indexed_results[index] = _MapTaskSuccess(result)
        except asyncio.CancelledError as exc:
            indexed_results[index] = _MapTaskCancelled(exc)
        except Exception as exc:  # noqa: BLE001 - preserve arbitrary user failures
            indexed_results[index] = _MapTaskFailure(exc)

    # Raised after the TaskGroup exits, so started tasks finish first.
    schedule_error: Exception | None = None

    async with asyncio.TaskGroup() as tg:

        def _schedule_one(item: T) -> None:
            # Fail before enqueueing new work. Already-started tasks are still
            # drained by the TaskGroup below.
            check_cancellation()
            index = len(indexed_results)
            indexed_results.append(None)
            try:
                tg.create_task(_run_one(index, item))
            except BaseException:
                indexed_results.pop()
                raise

        try:
            if isinstance(items, AsyncIterable):
                async for item in items:
                    _schedule_one(item)
            else:
                for item in items:
                    _schedule_one(item)
        except Exception as exc:  # noqa: BLE001 - iterators may raise arbitrary failures
            schedule_error = exc

    if schedule_error is not None:
        raise schedule_error

    for outcome in indexed_results:
        if outcome is None:
            raise RuntimeError("mapped task completed without recording a result")
        if isinstance(outcome, _MapTaskCancelled):
            raise outcome.error
        if not isinstance(outcome, _MapTaskFailure):
            continue
        if isinstance(outcome.error, DeadlineExceededError):
            raise DeadlineExceededError(
                "Synor timeout deadline exceeded"
            ) from outcome.error
        raise outcome.error
    # All started tasks completed successfully; check the caller's deadline
    # before returning their values.
    check_cancellation()
    return [
        cast(_MapTaskSuccess[ReturnT], outcome).value for outcome in indexed_results
    ]


async def map_bounded(
    fn: Callable[Concatenate[T, P], Coroutine[Any, Any, ReturnT]],
    items: Iterable[T] | AsyncIterable[T],
    max_in_flight: int,
    *args: P.args,
    **kwargs: P.kwargs,
) -> list[ReturnT]:
    """Run an async function with a bounded number of admitted item tasks.

    Results preserve input order. If an admitted item fails, no additional
    items are pulled, all already-admitted tasks are drained, and the first
    failure in input order is raised. The concurrency bound covers task and
    iterator admission; the returned ordered list still necessarily retains
    O(n) successful results.

    Use :func:`map` instead when item coroutines must rendezvous with the full
    input set, because a barrier wider than ``max_in_flight`` cannot complete.

    Args:
        fn: Async function to apply to each item.
        items: Synchronous or asynchronous input iterable.
        max_in_flight: Maximum admitted item tasks. Must be a positive integer.
        *args: Additional positional arguments passed to every invocation.
        **kwargs: Additional keyword arguments passed to every invocation.
    """
    if (
        not isinstance(max_in_flight, int)
        or isinstance(max_in_flight, bool)
        or max_in_flight <= 0
    ):
        raise ValueError("max_in_flight must be a positive integer")
    check_cancellation()

    indexed_results: list[
        _MapTaskSuccess[ReturnT] | _MapTaskFailure | _MapTaskCancelled | None
    ] = []
    permits = asyncio.Semaphore(max_in_flight)
    admission_stopped = asyncio.Event()

    async def _run_one(index: int, item: T) -> None:
        try:
            check_cancellation()
            result = await fn(item, *args, **kwargs)
            check_cancellation()
            indexed_results[index] = _MapTaskSuccess(result)
        except asyncio.CancelledError as exc:
            indexed_results[index] = _MapTaskCancelled(exc)
            admission_stopped.set()
        except Exception as exc:  # noqa: BLE001 - preserve arbitrary user failures
            indexed_results[index] = _MapTaskFailure(exc)
            admission_stopped.set()
        finally:
            permits.release()

    schedule_error: Exception | None = None
    async with asyncio.TaskGroup() as tg:

        def _schedule_one(item: T) -> None:
            index = len(indexed_results)
            indexed_results.append(None)
            try:
                tg.create_task(_run_one(index, item))
            except BaseException:
                indexed_results.pop()
                permits.release()
                raise

        if isinstance(items, AsyncIterable):
            async_iterator = aiter(items)
            while not admission_stopped.is_set():
                await permits.acquire()
                if admission_stopped.is_set():
                    permits.release()
                    break
                next_item: asyncio.Future[T] = asyncio.ensure_future(
                    anext(async_iterator)
                )
                stop_waiter = asyncio.create_task(admission_stopped.wait())
                try:
                    check_cancellation()
                    done, _pending = await asyncio.wait(
                        (next_item, stop_waiter),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    # An async iterator is allowed to block between items. A
                    # failed worker must still stop admission promptly instead
                    # of waiting forever for that next item to arrive.
                    # If both complete in the same event-loop turn, the item
                    # has already crossed the iterator boundary and must not
                    # be silently discarded. Admit that item, then let the
                    # next loop check stop further pulls.
                    if (
                        next_item not in done
                        and stop_waiter in done
                        and admission_stopped.is_set()
                    ):
                        permits.release()
                        break
                    item = await next_item
                except StopAsyncIteration:
                    permits.release()
                    break
                except Exception as exc:  # noqa: BLE001 - iterators may fail arbitrarily
                    permits.release()
                    schedule_error = exc
                    break
                finally:
                    for waiter in (next_item, stop_waiter):
                        if not waiter.done():
                            waiter.cancel()
                    await asyncio.gather(
                        next_item,
                        stop_waiter,
                        return_exceptions=True,
                    )
                _schedule_one(item)
        else:
            sync_iterator = iter(items)
            while not admission_stopped.is_set():
                await permits.acquire()
                if admission_stopped.is_set():
                    permits.release()
                    break
                try:
                    check_cancellation()
                    item = next(sync_iterator)
                except StopIteration:
                    permits.release()
                    break
                except Exception as exc:  # noqa: BLE001 - iterators may fail arbitrarily
                    permits.release()
                    schedule_error = exc
                    break
                _schedule_one(item)

    if schedule_error is not None:
        raise schedule_error

    for outcome in indexed_results:
        if outcome is None:
            raise RuntimeError("mapped task completed without recording a result")
        if isinstance(outcome, _MapTaskCancelled):
            raise outcome.error
        if not isinstance(outcome, _MapTaskFailure):
            continue
        if isinstance(outcome.error, DeadlineExceededError):
            raise DeadlineExceededError(
                "Synor timeout deadline exceeded"
            ) from outcome.error
        raise outcome.error

    check_cancellation()
    return [
        cast(_MapTaskSuccess[ReturnT], outcome).value for outcome in indexed_results
    ]


def map_stream(
    fn: Callable[Concatenate[T, P], Coroutine[Any, Any, ReturnT]],
    items: Iterable[T] | AsyncIterable[T],
    max_in_flight: int,
    *args: P.args,
    **kwargs: P.kwargs,
) -> AsyncGenerator[ReturnT, None]:
    """Stream completion-order results through a bounded admission window.

    Unlike :func:`map_bounded`, this helper does not build an ordered result
    list. At most ``max_in_flight`` items have crossed the input iterator
    without a result having been yielded, so Synor retains O(max_in_flight)
    item tasks and results regardless of total input size. Results are yielded
    in completion order, not input order.

    Worker, input-iterator, and caller failures stop admission immediately and
    cancel and drain the admitted window. When a consumer intentionally stops
    before exhaustion, close the returned iterator (for example with
    :class:`contextlib.aclosing`) so admitted work is cancelled promptly. An
    ordinary explicit close raises the first cleanup failure in deterministic
    producer/worker/source order. When an operation already has a primary
    failure, that failure remains primary and cleanup failures are attached as
    exception notes.

    Args:
        fn: Async function to apply to each item.
        items: Synchronous or asynchronous input iterable.
        max_in_flight: Maximum pulled-but-not-yielded items. Must be positive.
        *args: Additional positional arguments passed to every invocation.
        **kwargs: Additional keyword arguments passed to every invocation.

    Returns:
        An async iterator of results in completion order.
    """
    if (
        not isinstance(max_in_flight, int)
        or isinstance(max_in_flight, bool)
        or max_in_flight <= 0
    ):
        raise ValueError("max_in_flight must be a positive integer")

    async def _iterate() -> AsyncGenerator[ReturnT, None]:
        check_cancellation()

        slots = asyncio.Semaphore(max_in_flight)
        results: deque[ReturnT] = deque()
        state_changed = asyncio.Event()
        admission_stopped = asyncio.Event()
        workers: set[asyncio.Task[None]] = set()
        producer_task: asyncio.Task[None] | None = None
        failure_error: BaseException | None = None
        producer_cleanup_error: BaseException | None = None
        worker_cleanup_errors: dict[int, BaseException] = {}
        next_worker_index = 0
        outstanding = 0
        source_exhausted = False

        if isinstance(items, AsyncIterable):
            async_iterator = aiter(items)
            sync_iterator = None
        else:
            async_iterator = None
            sync_iterator = iter(items)

        def _record_failure(error: BaseException) -> None:
            """Publish the first failure and stop all other admitted work."""
            nonlocal failure_error

            if failure_error is not None:
                return
            failure_error = error
            admission_stopped.set()
            state_changed.set()

            current = asyncio.current_task()
            if producer_task is not None and producer_task is not current:
                producer_task.cancel()
            for worker in tuple(workers):
                if worker is not current:
                    worker.cancel()

        async def _run_one(worker_index: int, item: T) -> None:
            try:
                check_cancellation()
                result = await fn(item, *args, **kwargs)
                check_cancellation()
                if not admission_stopped.is_set():
                    results.append(result)
                    state_changed.set()
            except asyncio.CancelledError as exc:
                if not admission_stopped.is_set():
                    _record_failure(exc)
            except BaseException as exc:  # noqa: BLE001 - preserve user failures
                if admission_stopped.is_set():
                    # A worker can replace its injected cancellation with an
                    # error from ``finally``. Retain it until the owning stream
                    # drains the bounded window; a done callback must not make
                    # that cleanup failure disappear.
                    worker_cleanup_errors[worker_index] = exc
                else:
                    _record_failure(exc)

        def _worker_done(worker: asyncio.Task[None]) -> None:
            workers.discard(worker)

        async def _produce() -> None:
            nonlocal next_worker_index
            nonlocal outstanding
            nonlocal producer_cleanup_error
            nonlocal source_exhausted

            slot_reserved = False
            try:
                while not admission_stopped.is_set():
                    await slots.acquire()
                    slot_reserved = True
                    check_cancellation()

                    if async_iterator is not None:
                        try:
                            item = await anext(async_iterator)
                        except StopAsyncIteration:
                            source_exhausted = True
                            state_changed.set()
                            return
                    else:
                        assert sync_iterator is not None
                        try:
                            item = next(sync_iterator)
                        except StopIteration:
                            source_exhausted = True
                            state_changed.set()
                            return

                    # A custom async iterator can catch cancellation while
                    # another worker is failing. The pulled item belongs to
                    # the aborted operation, but must never start new work.
                    if admission_stopped.is_set():
                        return
                    check_cancellation()
                    worker_index = next_worker_index
                    next_worker_index += 1
                    worker = asyncio.create_task(_run_one(worker_index, item))
                    workers.add(worker)
                    worker.add_done_callback(_worker_done)
                    outstanding += 1
                    slot_reserved = False
            except asyncio.CancelledError as exc:
                if not admission_stopped.is_set():
                    _record_failure(exc)
            except BaseException as exc:  # noqa: BLE001 - iterators may fail arbitrarily
                if admission_stopped.is_set():
                    # Cancelling an in-progress async pull can execute source
                    # cleanup code. Preserve a replacement failure so explicit
                    # stream closure can report it.
                    producer_cleanup_error = exc
                else:
                    _record_failure(exc)
            finally:
                if slot_reserved:
                    slots.release()

        async def _close_source() -> BaseException | None:
            if source_exhausted:
                return None
            try:
                if async_iterator is not None:
                    close = getattr(async_iterator, "aclose", None)
                    if close is not None:
                        await close()
                elif sync_iterator is not None:
                    close = getattr(sync_iterator, "close", None)
                    if close is not None:
                        close()
            except BaseException as exc:  # noqa: BLE001 - report during drain
                return exc
            return None

        async def _cancel_and_drain() -> list[BaseException]:
            admission_stopped.set()

            # Cancel the producer and the complete current worker window
            # before yielding. A failing task's done callback may remove it
            # from ``workers``, but `_run_one` retains any replacement cleanup
            # failure above before the callback can run.
            remaining_workers = tuple(workers)
            if producer_task is not None:
                producer_task.cancel()
            for worker in remaining_workers:
                worker.cancel()

            tasks_to_drain = (
                () if producer_task is None else (producer_task,)
            ) + remaining_workers
            if tasks_to_drain:
                # All expected task outcomes are captured in the producer and
                # worker cleanup slots. Gathering still retrieves any
                # defensive/unexpected task exception.
                drained = await asyncio.gather(
                    *tasks_to_drain,
                    return_exceptions=True,
                )
            else:
                drained = []

            unexpected_errors = [
                result
                for result in drained
                if isinstance(result, BaseException)
                and not isinstance(result, asyncio.CancelledError)
            ]
            close_error = await _close_source()

            cleanup_errors: list[BaseException] = []
            if producer_cleanup_error is not None:
                cleanup_errors.append(producer_cleanup_error)
            # Admission order makes simultaneous worker cleanup failures
            # deterministic instead of depending on set or scheduler order.
            cleanup_errors.extend(
                worker_cleanup_errors[index] for index in sorted(worker_cleanup_errors)
            )
            for error in unexpected_errors:
                if not any(error is known for known in cleanup_errors):
                    cleanup_errors.append(error)
            if close_error is not None:
                cleanup_errors.append(close_error)
            return cleanup_errors

        async def _await_cleanup_barrier() -> tuple[
            list[BaseException], asyncio.CancelledError | None
        ]:
            """Drain through repeated caller cancellation without orphaning work."""
            cleanup_task = asyncio.create_task(_cancel_and_drain())
            deferred_cancellation: asyncio.CancelledError | None = None

            while not cleanup_task.done():
                try:
                    await asyncio.shield(cleanup_task)
                except asyncio.CancelledError as exc:
                    # ``shield`` leaves the cleanup task owned here. Repeated
                    # Task.cancel() calls are deferred until all producer,
                    # worker, and source cleanup has reached quiescence.
                    if cleanup_task.cancelled():
                        break
                    if deferred_cancellation is None:
                        deferred_cancellation = exc
                except BaseException:
                    # A cleanup-task failure is retrieved below. An unrelated
                    # BaseException must not detach a still-running barrier.
                    if not cleanup_task.done():
                        raise
                    break

            try:
                cleanup_errors = cleanup_task.result()
            except BaseException as exc:  # noqa: BLE001 - cleanup is evidence
                cleanup_errors = [exc]
            return cleanup_errors, deferred_cancellation

        def _add_cleanup_note(
            primary_error: BaseException,
            cleanup_error: BaseException,
        ) -> None:
            try:
                detail = str(cleanup_error)
            except BaseException:  # noqa: BLE001 - hostile exception formatter
                detail = "<unprintable error>"
            try:
                primary_error.add_note(
                    "map_stream cleanup also failed with "
                    f"{type(cleanup_error).__name__}: {detail}"
                )
            except BaseException:  # noqa: BLE001, S110 - preserve primary failure
                pass

        producer_task = asyncio.create_task(_produce())
        primary_error: BaseException | None = None
        try:
            while True:
                check_cancellation()
                if failure_error is not None:
                    error = failure_error
                    if isinstance(error, DeadlineExceededError):
                        raise DeadlineExceededError(
                            "Synor timeout deadline exceeded"
                        ) from error
                    raise error
                if results:
                    result = results.popleft()
                    outstanding -= 1
                    slots.release()
                    check_cancellation()
                    yield result
                    continue
                if source_exhausted and outstanding == 0:
                    return

                # Clear then recheck all state before sleeping. This avoids a
                # lost wakeup if a worker completes between the checks above
                # and the clear, while still allocating no waiter per result.
                state_changed.clear()
                if (
                    failure_error is not None
                    or results
                    or (source_exhausted and outstanding == 0)
                ):
                    continue
                await state_changed.wait()
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            cleanup_errors, cleanup_cancellation = await _await_cleanup_barrier()
            effective_primary = primary_error
            if cleanup_cancellation is not None and (
                effective_primary is None
                or isinstance(effective_primary, GeneratorExit)
            ):
                # Cancellation of an explicit aclose (or of normal terminal
                # cleanup) is a real caller outcome; GeneratorExit is only the
                # async-generator protocol's internal close signal.
                effective_primary = cleanup_cancellation

            if cleanup_errors:
                # ``GeneratorExit`` is the implementation detail used by an
                # ordinary explicit ``aclose()``. It must not hide a real
                # failure raised while cancelling workers or closing input.
                if effective_primary is None or isinstance(
                    effective_primary, GeneratorExit
                ):
                    first_cleanup_error = cleanup_errors[0]
                    for cleanup_error in cleanup_errors[1:]:
                        _add_cleanup_note(first_cleanup_error, cleanup_error)
                    raise first_cleanup_error

                # A worker/input/deadline/caller failure is the operation's
                # primary outcome. Keep it intact and make every cleanup
                # failure visible without allowing cleanup to rewrite it.
                for cleanup_error in cleanup_errors:
                    _add_cleanup_note(effective_primary, cleanup_error)

            if effective_primary is not primary_error:
                # This is cancellation received while closing an otherwise
                # successful/explicitly closed stream. Raise it only after the
                # quiescence barrier and cleanup-error annotation above.
                raise cast(BaseException, effective_primary)

    return _iterate()


_MOUNT_TARGET_SYMBOL = Symbol("synor/mount_target")


async def attach_target(
    target_state: TargetState[TargetHandler[_ValueT, Any, _ChildHandlerT]],
) -> TargetStateProvider[_ValueT, _ChildHandlerT]:
    """
    Mount a target, ensuring its container target state is applied before returning
    the child TargetStateProvider.

    Sugar over ``use_mount()`` combined with ``declare_target_state_with_child()``.
    The component subpath is derived automatically from the target's globally unique key.

    Args:
        target_state: A TargetState with a child handler, as created by
            ``TargetStateProvider.target_state(key, value)``. The key must be globally
            unique (target connectors ensure this by construction).

    Returns:
        The resolved child TargetStateProvider, ready to use for declaring child
        target states.

    Example::

        provider = await syn.attach_target(
            target_db.table_target(table_name=TABLE_NAME, table_schema=schema)
        )
    """
    check_cancellation()
    subpath = UnitPath(_MOUNT_TARGET_SYMBOL) / (
        *target_state._provider._core.stable_key_chain(),
        target_state._key,
    )
    return await call(subpath, ensure_target_state_with_child, target_state)  # type: ignore[no-any-return, return-value]


# ============================================================================
# Start / Stop / Runtime
# ============================================================================


async def start() -> None:
    """Start the default environment (and enter its lifespan, if any)."""
    await environment.start()


async def stop() -> None:
    """Stop the default environment (and exit its lifespan, if any)."""
    await environment.stop()


def start_blocking() -> None:
    """Start the default environment synchronously (and enter its lifespan, if any)."""
    environment.start_sync()


def stop_blocking() -> None:
    """Stop the default environment synchronously (and exit its lifespan, if any)."""
    environment.stop_sync()


async def default_env() -> environment.Environment:
    """Get the default environment (starting it if needed)."""
    return await environment.start()


class _DualModeRuntime:
    """Context manager that works with both `with` and `async with`."""

    def __enter__(self) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass  # No running loop — sync usage is fine
        else:
            raise RuntimeError(
                "Cannot use sync 'with syn.runtime()' from within an async event loop. "
                "Use 'async with syn.runtime()' instead."
            )
        start_blocking()
        return None

    def __exit__(self, *exc: Any) -> None:
        stop_blocking()

    async def __aenter__(self) -> None:
        await start()
        return None

    async def __aexit__(self, *exc: Any) -> None:
        await stop()


def runtime() -> _DualModeRuntime:
    """
    Dual-mode context manager that calls start/stop.

    Use ``with syn.runtime():`` for sync code, or
    ``async with syn.runtime():`` for async code.
    """
    return _DualModeRuntime()


# ============================================================================
# use_state
# ============================================================================

_StateT = TypeVar("_StateT")

# State has no static type hint (the handle is generic), so decode as Any.
_DESERIALIZE_ANY = make_deserialize_fn(Any)


class StateHandle(Generic[_StateT]):
    """
    Handle for a persistent per-component state value.

    Returned by `syn.use_state()`. Read the current value via `.value`;
    assign to `.value` to persist a new value for the next run.
    """

    __slots__ = ("_key", "_stored", "_deserializer", "_core_processor_ctx")

    def __init__(
        self,
        key: StableKey,
        stored: core.StoredValue,
        deserializer: DeserializeFn,
        core_processor_ctx: core.ComponentProcessorContext,
    ) -> None:
        self._key = key
        self._stored = stored
        self._deserializer = deserializer
        self._core_processor_ctx = core_processor_ctx

    @property
    def value(self) -> _StateT:
        # Lazy read: object-backed returns directly; bytes-backed deserializes once, cached.
        return self._stored.get(self._deserializer)  # type: ignore[no-any-return]

    @value.setter
    def value(self, new_value: _StateT) -> None:
        # Hand the object to Rust without serializing (serialization is deferred
        # to commit). Keep the returned object-backed cell so later in-run reads
        # return this value directly.
        self._stored = self._core_processor_ctx.update_user_state(self._key, new_value)


@overload
def use_state(key: StableKey, initial_value: Any = None) -> StateHandle[Any]: ...
@overload
def use_state(
    key: StableKey,
    initial_value: _StateT | None = None,
    *,
    type_hint: type[_StateT],
) -> StateHandle[_StateT]: ...
def use_state(
    key: StableKey,
    initial_value: Any = None,
    *,
    type_hint: type[Any] | None = None,
) -> StateHandle[Any]:
    """
    Declare a persistent state for the current component.

    On the first run, the returned handle's `.value` is `initial_value`
    (or `None` if omitted). On subsequent runs, `.value` is the value
    stored at the end of the previous run. Assign to `handle.value`
    during the run to persist a new value.

    The value is serialized lazily, once, when the component commits — not at
    assignment. Two consequences: (1) if the value is not serializable, the
    error surfaces at commit (identifying the state key) rather than at the
    `handle.value = ...` line; (2) the persisted value reflects the object as it
    is at commit, so mutating it in place after assignment is captured.

    Args:
        key: Unique StableKey within this component (None, bool, int, str,
             bytes, uuid.UUID, Symbol, or a tuple of these). Must be declared
             at most once per component run.
        initial_value: Value to use when no stored state exists for `key`.
                       Defaults to `None`.
        type_hint: Optional type to deserialize the stored value into. When
                   provided, `.value` is decoded via the registered
                   serialization framework (msgspec for dataclasses /
                   NamedTuples / msgspec.Structs / primitives, pickle for
                   types decorated with ``@syn.serialize_by_pickle``, pydantic
                   for ``BaseModel`` subclasses). When omitted, the value is
                   decoded generically (``Any``) — i.e. whatever object the
                   deserializer produces from the stored bytes.

    Returns:
        A StateHandle wrapping the current value.

    Raises:
        RuntimeError: In the following cases, which surface as component build
                      failures. They are logged or reported through a configured
                      exception handler and remain terminal for `app.update()`:

                      - Inside a `with syn.unit_path()` block: state
                        is owned by the component's stable path, not the shifted
                        subpath, so the key would silently read/write under the
                        wrong identity.
                      - Inside a memoized function body: on a cache hit the body
                        is skipped entirely, so the key would never be declared
                        and would be garbage-collected as stale on the next commit.
                      - If `key` is declared more than once in the same component
                        run: each key maps to exactly one state slot; a second
                        declaration would be ambiguous.

    Example::

        # Plain (value typed as Any)
        counter = syn.use_state("counter", 0)

        # Typed — handle.value is Cursor, with full type inference
        @dataclass
        class Cursor:
            pos: int
            tag: str

        cur = syn.use_state("cursor", type_hint=Cursor, initial_value=Cursor(0, "init"))
        cur.value.pos += 1
        cur.value = Cursor(cur.value.pos, "next")
    """
    ctx = get_context_from_ctx()
    if ctx._core_path != ctx._core_processor_ctx.stable_path:
        raise RuntimeError(
            "syn.use_state() cannot be called inside a `with syn.unit_path()` block"
        )

    if ctx._in_memo_fn:
        raise RuntimeError(
            "syn.use_state() cannot be called inside a memoized function"
        )
    try:
        # initial_value passed unserialized; engine core drops it if a value is
        # already stored on the previous run for this key.
        stored = ctx._core_processor_ctx.use_state(key, initial_value)
    except ValueError as e:
        # Rust client errors surface as ValueError; normalize to RuntimeError so
        # all use_state usage errors have a consistent type for callers.
        raise RuntimeError(str(e)) from None
    if type_hint is not None:
        deserializer = get_deserialize_fn(
            type_hint,  # type: ignore[arg-type]  # type objects are hashable at runtime
            source_label=f"use_state key {key!r}",
        )
    else:
        deserializer = _DESERIALIZE_ANY
    return StateHandle(key, stored, deserializer, ctx._core_processor_ctx)


# ============================================================================
# __all__
# ============================================================================

__all__ = [
    # .app
    "App",
    "AppConfig",
    "DropHandle",
    "UpdateHandle",
    "show_progress",
    # .update_stats
    "ComponentStats",
    "StatsGroupHandle",
    "UpdateSnapshot",
    "UpdateStats",
    "UpdateStatus",
    # .function
    "task",
    "LogicTracking",
    "timeout",
    "check_cancellation",
    "DeadlineExceededError",
    # .batching
    "RetryWithSmallerBatch",
    # .context_keys
    "ContextKey",
    "ContextProvider",
    # .target_state
    "ChildTargetDef",
    "TargetState",
    "TargetStateProvider",
    "TargetReconcileOutput",
    "TargetHandler",
    "TargetActionSink",
    "TargetSinkCapabilities",
    "TargetSinkQueueStats",
    "PendingTargetStateProvider",
    "ensure_target_state",
    "ensure_target_state_with_child",
    "register_root_target_states_provider",
    # .environment
    "Environment",
    "EnvironmentBuilder",
    "LifespanFn",
    "lifespan",
    # .runner
    "GPU",
    "GPUPool",
    "GPURunner",
    "Runner",
    "configure_gpu_pool",
    "current_gpu",
    "current_gpus",
    "current_gpu_fraction",
    # .serde
    "unpickle_safe",
    "serialize_by_pickle",
    # .memo_fingerprint
    "memo_fingerprint",
    "register_memo_key_function",
    "NotMemoKeyable",
    # .pending_marker
    "MaybePendingS",
    "PendingS",
    "ResolvedS",
    "ResolvesTo",
    # .component_ctx
    "ComponentContext",
    "UnitPath",
    "ExceptionContext",
    "ExceptionHandler",
    "unit_path",
    "exception_handler",
    "stats_group",
    "use_context",
    "get_component_context",
    # .setting
    "Settings",
    "LmdbSettings",
    # .stable_path
    "ROOT_PATH",
    "StablePath",
    "StableKey",
    "Symbol",
    # .typing
    "ABSENT",
    "AbsentType",
    "is_absent",
    "MemoStateOutcome",
    # .live_component
    "LiveComponent",
    "LiveComponentOperator",
    "LiveMapFeed",
    "LiveMapView",
    "LiveMapSubscriber",
    "auto_refresh",
    # use_state
    "StateHandle",
    "use_state",
    # Mount APIs
    "ReadinessOutcome",
    "Succeeded",
    "Failed",
    "Cancelled",
    "Superseded",
    "SpawnHandle",
    "spawn",
    "spawn_each",
    "attach_target",
    "map",
    "map_bounded",
    "map_stream",
    "call",
    # Start/stop/runtime
    "start",
    "stop",
    "start_blocking",
    "stop_blocking",
    "default_env",
    "runtime",
]
