from __future__ import annotations

import asyncio
import datetime
import inspect
import traceback
from collections.abc import AsyncIterator, Awaitable, Callable
from contextvars import ContextVar
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Generic,
    ParamSpec,
    Protocol,
    TypeVar,
    final,
    runtime_checkable,
)

from . import core
from .component_ctx import (
    UnitPath,
    get_context_from_ctx,
)
from .deadline import without_deadline as _without_deadline
from .environment import Environment
from .function import AnyCallable, create_core_component_processor
from .serde import deserialize, serialize
from .typing import StableKey

if TYPE_CHECKING:
    from .api import ReadinessOutcome, SpawnHandle

_P = ParamSpec("_P")
_K = TypeVar("_K")
_V = TypeVar("_V")
_M_co = TypeVar("_M_co", covariant=True)
_M_contra = TypeVar("_M_contra", contravariant=True)


# ============================================================================
# `_in_process_live` ContextVar enforcement
# ============================================================================
#
# Per the design (specs/live_component/requirement.md):
#   "process_live() may not call syn.spawn() / mount_each() / use_mount() —
#    those create children whose parent_ctx leak from outside the controller,
#    breaking the per-controller cancellation cascade. Use operator.update /
#    operator.delete instead."
#
# Enforcement: a Python `ContextVar` set to `True` for the duration of
# `process_live(operator)`'s asyncio Task. The three mount entry points
# (`syn.spawn`, `syn.spawn_each`, `syn.call`) check this var and
# raise if set.
#
# Symmetric reset for `process()`: `process()` is invoked from inside
# `update_full()` which is called from `process_live` — so the asyncio
# Task running `process()` would inherit its parent Task's `Context`
# (where `_in_process_live = True`) and `syn.spawn(...)` inside `process()`
# would falsely raise. Reset is done inline at the top of
# `LiveComponentOperator.update_full`: we save/restore `_in_process_live`
# around the `update_full_async` call, so the new Task that Rust spawns
# to run `instance.process` snapshots the current `Context` (with `False`)
# at spawn time.
#
# Task-identity invariant (load-bearing, see design.md):
#   `_process_live_wrapper` and the inline reset in `update_full` must
#   execute on asyncio Tasks whose Context was forked from the caller's
#   Context (so the var's value at the wrapper's / reset's `set()` call
#   is visible when the body runs). PyO3's `from_py_future` schedules
#   onto the current event loop without forcing a fresh Context, so the
#   inheritance chain holds. Future-proofing: integration tests verify both:
#     (a) `syn.spawn(...)` directly inside `process_live` raises
#     (b) `syn.spawn(...)` inside `process()` of a live component does NOT raise
#   If (a) silently stops raising, the wrapper isn't installed; if (b)
#   starts raising, the symmetric reset isn't taking effect — both are
#   load-bearing for the live-component installer machinery.
_in_process_live: ContextVar[bool] = ContextVar("_in_process_live", default=False)


def check_not_in_process_live(api_name: str) -> None:
    """Raise if called from inside `process_live`.

    Called by `syn.spawn`, `syn.spawn_each`, `syn.call` at entry.
    Outside of `process_live`, `_in_process_live.get()` returns the
    default `False` (e.g. inside `process()` after `update_full`'s
    inline reset, or in any non-live-component context).
    """
    if _in_process_live.get():
        raise RuntimeError(
            f"{api_name}() is not allowed inside process_live; "
            f"use operator.update / operator.delete instead. "
            f"(See specs/live_component/requirement.md for the rationale.)"
        )


async def _process_live_wrapper(instance: Any, operator: LiveComponentOperator) -> None:
    """Wrap a `process_live` invocation to set `_in_process_live = True`
    and detach the operator's controller on exit.

    Used by `_mount_live_component` (api.py) and the LiveCompClass branch
    of `LiveComponentOperator.update`.

    Live components run independently from the parent update, so parent
    cooperative deadlines do not apply to ``process_live`` or the
    ``operator.update_full()`` calls it starts.

    Save/restore the prior value rather than using `ContextVar.reset(token)`:
    on cancellation the finally block can run in a different `Context`
    than where the Token was created (asyncio Task cancellation can
    swap Contexts during cleanup), and `reset(token)` then raises
    `ValueError("Token was created in a different Context")`. Direct
    `set(prev)` always works because we're mutating whatever the
    current Context is.

    The ``operator._detach()`` in the same finally is the framework's
    fix for a subtle live-mode leak: if user code in ``process_live``
    catches an exception and retains it (``self.last_err = e`` /
    re-raises later), the exception's traceback holds the calling
    frame's locals, which include ``operator`` — and ``operator`` owns a
    Rust ``Arc`` to the live component's ``Component``. Without
    detach, ``App.update``'s ``wait_until_inactive`` poll never observes
    the live component as inactive. After detach, the Rust controller
    drops on schedule; later operator method calls raise. See
    ``specs/core/error_handling.md`` §4.1.
    """
    prev = _in_process_live.get()
    _in_process_live.set(True)
    try:
        with _without_deadline():
            await instance.process_live(operator)
    finally:
        _in_process_live.set(prev)
        operator._detach()


@runtime_checkable
class ReadyAwaitable(Protocol):
    """A handle that reports a typed, terminal downstream readiness outcome.

    Broker-backed sources acknowledge external progress only for
    :class:`~synor.Succeeded`. ``ready()`` remains part of the compatibility
    surface for callers that want raising semantics, but source connectors use
    ``outcome()`` so failure, cancellation, and supersession cannot be mistaken
    for durable success.
    """

    async def ready(self) -> None: ...
    async def outcome(self) -> ReadinessOutcome: ...


@final
class _ImmediateReady:
    """Pre-resolved ``ReadyAwaitable``: ``ready()`` returns immediately.

    Singleton used as ``_IMMEDIATE_READY``; subscribers return it to mean
    "ack this message now; no further work needed." The ``LiveStream``
    implementation may identity-check (``handle is _IMMEDIATE_READY``) to
    skip per-message task allocation.
    """

    __slots__ = ()

    async def ready(self) -> None:
        return

    async def outcome(self) -> ReadinessOutcome:
        # Local import avoids the api -> live_component import cycle.
        from .api import Succeeded

        return Succeeded()


_IMMEDIATE_READY: Final[_ImmediateReady] = _ImmediateReady()


@final
class _ReadinessPermit:
    """One idempotently releasable slot in a source readiness window."""

    __slots__ = ("_admission",)

    def __init__(self, admission: _ReadinessAdmission) -> None:
        self._admission: _ReadinessAdmission | None = admission

    def release(self) -> None:
        admission = self._admission
        if admission is None:
            return
        self._admission = None
        admission._release()


@final
class _ReadinessAdmission:
    """Bound messages admitted but not past a source's ordered frontier.

    A permit is acquired before a source pulls/delivers a message and is
    transferred to its offset tracker. A successful typed outcome alone does
    not release it: the tracker holds later-completed permits until every
    earlier offset is also ready and the contiguous commit/store frontier can
    advance. Failure and shutdown release retained entries during draining.
    The idempotent permit makes every cancellation and error cleanup path safe
    to call without over-releasing the bounded semaphore.
    """

    __slots__ = ("_in_flight", "_semaphore", "limit")

    def __init__(self, limit: int) -> None:
        if type(limit) is not int:
            raise TypeError("readiness admission limit must be an int")
        if limit < 1:
            raise ValueError("readiness admission limit must be at least 1")
        self.limit = limit
        self._in_flight = 0
        self._semaphore = asyncio.BoundedSemaphore(limit)

    @property
    def in_flight(self) -> int:
        return self._in_flight

    @property
    def is_full(self) -> bool:
        return self._semaphore.locked()

    async def acquire(self) -> _ReadinessPermit:
        await self._semaphore.acquire()
        self._in_flight += 1
        return _ReadinessPermit(self)

    def _release(self) -> None:
        if self._in_flight < 1:
            raise RuntimeError("readiness admission permit released without ownership")
        self._in_flight -= 1
        self._semaphore.release()


async def _await_readiness_succeeded(handle: ReadyAwaitable) -> None:
    """Return only after ``handle`` reports durable downstream success.

    Source acknowledgements require an explicit typed outcome. A legacy handle
    with only ``ready()`` cannot distinguish durable success from cancellation,
    supersession, or an implementation that swallowed failure, so accepting it
    here could advance external progress without proof.

    The import is local because :mod:`api` defines ``SpawnHandle`` and imports
    this module's live-stream protocols.
    """
    if handle is _IMMEDIATE_READY:
        return

    outcome_fn = getattr(handle, "outcome", None)
    if outcome_fn is None:
        raise TypeError(
            "source acknowledgement requires a typed readiness handle with outcome()"
        )
    if not callable(outcome_fn):
        raise TypeError("readiness handle outcome attribute is not callable")

    from .api import Cancelled, Failed, Succeeded, Superseded

    outcome = await outcome_fn()
    if isinstance(outcome, Succeeded):
        return
    if isinstance(outcome, Failed):
        raise outcome.error
    if isinstance(outcome, Cancelled):
        raise asyncio.CancelledError(
            "downstream readiness was cancelled before durable success"
        )
    if isinstance(outcome, Superseded):
        raise RuntimeError(  # noqa: TRY004 - a known terminal state, not a bad type
            "downstream readiness was superseded before durable success"
        )
    raise TypeError(f"unknown readiness outcome: {outcome!r}")


@runtime_checkable
class LiveStream(Protocol[_M_co]):
    """A keyless stream of messages with in-memory processing watermark tracking.

    The implementation buffers inflight messages, acks the underlying source
    once earlier messages have been processed, and signals readiness when it
    has caught up to its initial watermark.

    Single-subscriber: one active ``watch()`` at a time (implementations enforce
    this via ``synor.connectorkits.SingleWatcherGuard``). Fan-out to multiple
    subscribers, if ever needed, belongs in a layer above the stream.
    """

    async def watch(self, subscriber: LiveStreamSubscriber[_M_co]) -> None: ...


@runtime_checkable
class LiveStreamSubscriber(Protocol[_M_contra]):
    """Callback interface for ``LiveStream.watch()``."""

    async def send(self, message: _M_contra) -> ReadyAwaitable:
        """Process a message; return a handle whose ``ready()`` awaits completion.

        Return :data:`_IMMEDIATE_READY` to mean "no work needed; ack now."
        """
        ...

    async def mark_ready(self) -> None:
        """Signal that the stream has caught up to its initial watermark.

        Contract: implementations MUST return promptly. The stream awaits
        ``mark_ready()`` inline in its poll loop, so a long-running
        implementation stalls polling. Subscribers that need to wait on
        further preconditions (e.g. a pending scan) should spawn a
        background task and return.
        """
        ...


@runtime_checkable
class LiveComponent(Protocol):
    """Protocol for live components that process continuously."""

    async def process(self) -> None: ...
    async def process_live(self, operator: LiveComponentOperator) -> None: ...


def is_live_component_class(cls: Any) -> bool:
    """Check if cls is a class with process and process_live methods."""
    return (
        isinstance(cls, type)
        and hasattr(cls, "process")
        and hasattr(cls, "process_live")
        and callable(getattr(cls, "process"))
        and callable(getattr(cls, "process_live"))
    )


class LiveComponentOperator:
    """Passed to process_live(). Wraps the Rust LiveComponentController.

    Lifecycle: the operator is **scoped to one invocation of process_live**.
    After process_live returns (normally or via exception), the wrapper that
    invoked it calls :meth:`_detach` to release the Rust controller. This
    matters because the Rust ``LiveComponentController`` holds a strong
    ``Arc`` to the live component's ``Component``, and the framework's
    ``wait_until_inactive`` poll (used by ``App.update`` in live mode to
    detect "all done, safe to terminate") tracks that strong count.

    Without detach, user code in ``process_live`` that catches an
    exception and stores it (e.g. ``self.last_err = e``) would
    accidentally pin the live component forever — Python exception
    objects retain their traceback, which retains the caller's frame
    locals, which retains ``operator``. See
    ``specs/core/error_handling.md`` §4.1.
    """

    __slots__ = ("_controller", "_instance", "_env", "_path")

    _controller: core.LiveComponentController | None

    def __init__(
        self,
        controller: core.LiveComponentController,
        instance: Any,  # The LiveComponent instance
        env: Environment,
        path: core.StablePath,
    ) -> None:
        self._controller = controller
        self._instance = instance
        self._env = env
        self._path = path

    def _detach(self) -> None:
        """Release the Rust controller; subsequent operator calls raise.

        Called by ``_process_live_wrapper`` in a ``finally`` block once
        ``process_live`` returns. After detach, the operator is usable only
        for inspecting its own metadata (``_env``, ``_path``); the
        controller-backed methods (:meth:`update_full`, :meth:`update`,
        :meth:`delete`, :meth:`mark_ready`) raise :class:`RuntimeError`.
        """
        self._controller = None

    def _require_controller(self) -> core.LiveComponentController:
        """Return the controller or raise if already detached."""
        ctrl = self._controller
        if ctrl is None:
            raise RuntimeError(
                "LiveComponentOperator is no longer active. Operator "
                "methods are only valid inside the body of process_live."
            )
        return ctrl

    def _resolve_exception_handler(self) -> Callable[[str], Awaitable[None]]:
        """Build a resolver for the parent's exception handler chain.

        Delegates to :meth:`ComponentContext.resolve_exception_handler`
        — the same path used by ``syn.spawn`` / ``syn.spawn_each`` —
        so component-failure logs go through one canonical Python
        fallback. Always non-None. Used both by :meth:`update_full`
        (passes to Rust as ``on_error``) and :meth:`report_exception`
        (invokes directly with a stringified exception).
        """
        return get_context_from_ctx().resolve_exception_handler(
            stable_path=self._path.to_string(),
            processor_name=type(self._instance).__name__,
            mount_kind="process_live",
        )

    async def update_full(self) -> None:
        """Trigger a full update via instance.process(). Blocks until fully ready.

        Resets `_in_process_live = False` for the duration of the call so
        `syn.spawn(...)` from inside `process()` of a live component
        does NOT raise (we're not strictly in `process_live`'s body
        anymore — `process()` is a separate concern). The new Task that
        Rust spawns to run the processor's coroutine snapshots the
        current `Context` at spawn time, so it inherits `False`.

        Exceptions raised inside `process()` (or its descendants) are
        routed via the parent's exception handler chain — same shape as
        background `syn.spawn()` failures — and propagate to this caller
        after the handler observes them. Periodic-refresh patterns that choose
        to tolerate a failed cycle catch that terminal result explicitly.
        """
        controller = self._require_controller()
        processor = create_core_component_processor(
            self._instance.process, self._env, self._path, (), {}
        )
        on_error = self._resolve_exception_handler()
        prev = _in_process_live.get()
        _in_process_live.set(False)
        try:
            await controller.update_full_async(processor, on_error)
        finally:
            _in_process_live.set(prev)

    async def update(
        self,
        subpath: UnitPath,
        processor_fn: AnyCallable[_P, Any],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> Any:  # Returns SpawnHandle
        from .api import SpawnHandle

        child_path = self._path
        for part in subpath.parts:
            child_path = child_path.concat(part)

        controller = self._require_controller()
        # Slice F: branch on processor type. A LiveComponent class triggers
        # the nested-mount path — we install a fresh inner controller at the
        # child path under the parent's `update_full_lock`, then spawn the
        # inner's `process_live`. A plain processor goes through the
        # queued/coalesced dispatch path on the existing controller.
        if is_live_component_class(processor_fn):
            instance: Any = processor_fn(*args, **kwargs)
            (
                inner_controller,
                readiness_handle,
            ) = await controller.mount_inner_live_async(child_path)
            inner_operator = LiveComponentOperator(
                inner_controller, instance, self._env, child_path
            )
            # Wrap the inner `process_live` in `_process_live_wrapper` so
            # `_in_process_live = True` is observable inside the inner's
            # body. Recursive case: an outer's `_in_process_live = True`
            # already holds; the inner's wrapper saves the prior value and
            # restores it on exit (save/restore rather than `Token.reset`
            # to survive cancellation across Context boundaries — see
            # `_process_live_wrapper`'s docstring).
            inner_controller.start(_process_live_wrapper(instance, inner_operator))
            return SpawnHandle([readiness_handle])

        processor = create_core_component_processor(
            processor_fn, self._env, child_path, args, kwargs
        )
        # Build on_error using the CHILD's path/processor_name (not the live
        # component's own) so handlers attribute failures to the right unit.
        on_error = get_context_from_ctx().resolve_exception_handler(
            stable_path=child_path.to_string(),
            processor_name=getattr(processor_fn, "__qualname__", None),
            mount_kind="process_live",
        )
        core_handle = await controller.update_async(child_path, processor, on_error)
        return SpawnHandle([core_handle])

    async def delete(self, subpath: UnitPath) -> Any:
        """Delete a child component.

        Symmetric with :meth:`update`: failures route through the
        parent's exception handler chain, then propagate back through
        ``handle.ready()``. A handler reports the failure but cannot convert
        it into success. With no handler registered, the framework logs at
        ``ERROR`` before readiness raises.

        Even when the delete fails, the tombstone is already written
        synchronously by the framework — the next reconcile's GC sweep
        retries the underlying target-state cleanup.
        """
        from .api import SpawnHandle

        controller = self._require_controller()
        child_path = self._path
        for part in subpath.parts:
            child_path = child_path.concat(part)
        on_error = get_context_from_ctx().resolve_exception_handler(
            stable_path=child_path.to_string(),
            processor_name=None,
            mount_kind="process_live",
        )
        core_handle = await controller.delete_async(child_path, on_error)
        return SpawnHandle([core_handle])

    async def mark_ready(self) -> None:
        """Signal readiness. In catch-up mode, this never returns (terminates process_live)."""
        await self._require_controller().mark_ready_async()

    async def read_committed_state(self, key: StableKey) -> Any | None:
        """Read state committed under ``key`` by a prior run's ``process()``.

        Read-only counterpart to :func:`syn.use_state`, callable inside
        ``process_live``. A durable connector persists a bootstrap flag /
        logic version via ``syn.use_state`` inside ``process()``; on a
        later run it reads that value back here — before the first
        ``update_full`` — to decide whether the startup full scan can be
        skipped.

        Returns the deserialized value (the same object a subsequent
        ``use_state`` call would observe), or ``None`` if no value was
        committed for ``key`` (e.g. the component never bootstrapped). The
        value is read from a fresh standalone snapshot and reflects the most
        recently committed run, not any in-flight ``update_full`` of the
        current ``process_live``.
        """
        controller = self._require_controller()
        raw = await controller.read_committed_state_async(key)
        if raw is None:
            return None
        return deserialize(raw)

    async def write_committed_state(self, key: StableKey, value: Any) -> None:
        """Commit ``value`` under ``key`` for a later run to read back.

        Write-side counterpart to :meth:`read_committed_state`. The live
        machinery uses this to persist a durable bootstrap flag / logic
        version (the value a subsequent :meth:`read_committed_state` will
        observe) without going through :func:`syn.use_state`, whose
        ``Regular`` keyspace is set-reduced by the component's own
        ``process()`` flush. The write commits to a fresh standalone
        transaction; it does not participate in any in-flight
        ``update_full``.
        """
        controller = self._require_controller()
        await controller.write_committed_state_async(key, serialize(value))

    async def report_exception(self, exc: BaseException) -> None:
        """Route an exception raised during ``process_live`` to the parent's exception handler chain.

        Walks the exception handler chain on the parent's
        :class:`ComponentContext` (inherited via the asyncio Task that runs
        ``process_live``). The constructed :class:`ExceptionContext` uses
        this live component's own ``stable_path`` (not the parent's), so
        handlers can attribute the failure to the correct component, and
        ``mount_kind="process_live"`` so handlers can distinguish runtime
        cycle failures from initial build failures (``"mount"`` /
        ``"mount_each"``).

        The exception is formatted via :func:`traceback.format_exception`
        so handlers and the fallback log both see the full Python
        traceback (when ``exc.__traceback__`` is set — i.e. when the
        caller is reporting a caught exception). This matches the
        text-with-trace shape that the Rust-side ``on_error`` path
        produces for background ``mount`` / ``mount_each`` failures.

        Falls back to ERROR-level logging if no handler is registered or
        every handler re-raises. Intended for surfacing recoverable errors
        (e.g. an external watcher emits a malformed event) without
        tearing down the live component.
        """
        err_text = "".join(traceback.format_exception(exc))
        await self._resolve_exception_handler()(err_text)


@runtime_checkable
class LiveMapFeed(Protocol[_K, _V]):
    """A feed of changes to a live map. Watch only.

    For sources like Kafka that stream change events but have no scannable snapshot.
    Consumed by ``mount_each()``.

    Single-subscriber: one active ``watch()`` at a time (implementations enforce
    this via ``synor.connectorkits.SingleWatcherGuard``, except ``LiveMap``
    whose watcher queue already serves as the guard). Fan-out belongs in a layer
    above the feed.
    """

    async def watch(self, subscriber: LiveMapSubscriber[_K, _V]) -> None: ...


@runtime_checkable
class LiveMapView(LiveMapFeed[_K, _V], Protocol[_K, _V]):
    """A live map view: scannable current state + watchable changes.

    Extends ``LiveMapFeed`` by adding async iteration over current items.
    For sources like localfs that have a scannable current state.
    Consumed by ``mount_each()``.
    """

    def __aiter__(self) -> AsyncIterator[tuple[_K, _V]]: ...


class LiveMapSubscriber(Generic[_K, _V]):
    """Callback interface for ``LiveMapFeed.watch()`` to deliver changes.

    Wraps a ``LiveComponentOperator`` at a higher level of abstraction — callers
    provide keys and values instead of component subpaths and processor functions.
    """

    __slots__ = ("_operator", "_fn", "_args", "_kwargs")

    def __init__(
        self,
        operator: LiveComponentOperator,
        fn: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        self._operator = operator
        self._fn = fn
        self._args = args
        self._kwargs = kwargs

    async def update_all(self) -> None:
        """Trigger a full re-iteration of all items."""
        await self._operator.update_full()

    async def mark_ready(self) -> None:
        """Signal readiness. In catch-up mode, this terminates ``watch()``."""
        await self._operator.mark_ready()

    async def update(self, key: _K, value: _V) -> SpawnHandle:
        """Incrementally update a single entry."""
        return await self._operator.update(  # type: ignore[no-any-return]
            UnitPath(key),  # type: ignore[arg-type]
            self._fn,
            value,
            *self._args,
            **self._kwargs,
        )

    async def delete(self, key: _K) -> SpawnHandle:
        """Incrementally delete a single entry."""
        return await self._operator.delete(UnitPath(key))  # type: ignore[no-any-return,arg-type]

    async def read_committed_state(self, key: StableKey) -> Any | None:
        """Read prior-run committed state for ``key``.

        Delegates to :meth:`LiveComponentOperator.read_committed_state` so a
        ``LiveMapView``/``LiveMapFeed`` ``watch()`` (which receives this
        subscriber, not the operator) can gate its startup ``update_all()``
        on persisted bootstrap state.
        """
        return await self._operator.read_committed_state(key)

    async def write_committed_state(self, key: StableKey, value: Any) -> None:
        """Commit ``value`` under ``key`` for a later run to read back.

        Delegates to :meth:`LiveComponentOperator.write_committed_state` so a
        ``watch()`` implementation can persist bootstrap state (e.g. after a
        successful initial scan) that a later run reads via
        :meth:`read_committed_state`.
        """
        await self._operator.write_committed_state(key, value)


class _MountEachLiveComponent:
    """Internal LiveComponent created by mount_each() for LiveMapFeed/LiveMapView items."""

    def __init__(
        self,
        items: LiveMapFeed[Any, Any],
        fn: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        self._items = items
        self._fn = fn
        self._args = args
        self._kwargs = kwargs

    async def process(self) -> None:
        if not isinstance(self._items, LiveMapView):
            raise TypeError(
                "LiveMapFeed sources require live mode. "
                "Pass live=True to app.update() or use a LiveMapView source that "
                "supports full scans."
            )
        from .api import spawn

        async for key, value in self._items:
            await spawn(UnitPath(key), self._fn, value, *self._args, **self._kwargs)  # type: ignore[arg-type]

    async def process_live(self, operator: LiveComponentOperator) -> None:
        subscriber: LiveMapSubscriber[Any, Any] = LiveMapSubscriber(
            operator, self._fn, self._args, self._kwargs
        )
        await self._items.watch(subscriber)


def auto_refresh(
    process_fn: AnyCallable[_P, None],
    *,
    interval: datetime.timedelta,
) -> type[LiveComponent]:
    """Wrap a process function as a LiveComponent that re-runs every ``interval``.

    The returned class can be passed to :func:`syn.spawn` (and
    :meth:`LiveComponentOperator.update`) wherever a LiveComponent class is
    accepted. Its ``__init__`` accepts the same positional and keyword
    arguments as ``process_fn`` and forwards them to ``process_fn`` on each
    invocation.

    Semantics:

    - ``process()`` calls ``process_fn(*args, **kwargs)``.
    - ``process_live(operator)`` runs ``update_full`` once, ``mark_ready``,
      then loops ``sleep(interval) -> update_full`` with a **fixed delay**
      (the sleep happens after each cycle, so cycles never overlap).
    - In catch-up mode (``live=False``), ``mark_ready`` terminates the live
      component after the first full pass — observationally identical to
      mounting ``process_fn`` directly; the interval is ignored.
    - Cycle exceptions raised inside ``process_fn`` are routed via the
      parent's exception handler chain (same shape as background
      ``syn.spawn`` failures — see ``advanced_topics/exception_handlers``).
      The auto-refresh loop explicitly catches failures after initial
      readiness so one transient cycle does not stop later cycles. The first
      cycle still fails the component and prevents readiness.

    Args:
        process_fn: Async process function — same shape as a function passed
            directly to ``syn.spawn``.
        interval: Delay between cycles. Applied between the end of one cycle
            and the start of the next (fixed delay, not fixed rate).

    Example::

        await syn.spawn(
            syn.auto_refresh(sync_users, interval=datetime.timedelta(minutes=5)),
            db, target,
        )
    """
    sleep_seconds = interval.total_seconds()
    fn_name = getattr(process_fn, "__name__", "auto_refresh")

    class _AutoRefresh:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._args = args
            self._kwargs = kwargs

        async def process(self) -> None:
            result: Any = process_fn(*self._args, **self._kwargs)
            if inspect.isawaitable(result):
                await result

        async def process_live(self, operator: LiveComponentOperator) -> None:
            await operator.update_full()
            await operator.mark_ready()
            while True:
                await asyncio.sleep(sleep_seconds)
                try:
                    await operator.update_full()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # `update_full` already routed the terminal failure
                    # through the exception-handler chain. Auto-refresh owns
                    # the explicit retry policy for post-readiness cycles.
                    continue

    _AutoRefresh.__synor_subpath_name__ = fn_name  # type: ignore[attr-defined]
    return _AutoRefresh
