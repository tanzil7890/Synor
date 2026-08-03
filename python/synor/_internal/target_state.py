from __future__ import annotations

import threading
import weakref
from dataclasses import dataclass
from typing import (
    Any,
    Collection,
    Generic,
    Literal,
    NamedTuple,
    Protocol,
    Sequence,
    TypeAlias,
    cast,
    overload,
)

from typing_extensions import TypeVar

from . import core
from .component_ctx import get_context_from_ctx
from .context_keys import ContextProvider
from .pending_marker import MaybePendingS, PendingS, ResolvesTo
from .serde import (
    get_param_annotation,
    make_deserialize_fn,
    qualified_name,
    unwrap_element_type,
)
from .typing import AbsentType, StableKey

ActionT = TypeVar("ActionT")
ActionT_co = TypeVar("ActionT_co", covariant=True)
ActionT_contra = TypeVar("ActionT_contra", contravariant=True)
_NativeEffectDescriptorTuple: TypeAlias = tuple[str, str, str, int, str]
_SinkCapabilitiesTuple: TypeAlias = tuple[
    int,
    str,
    str,
    str,
    str,
    str,
    str,
    int | None,
    int | None,
]
_SinkQueueStatsTuple: TypeAlias = tuple[int, int, int, int, int, int]
_SinkBatchAtomicity: TypeAlias = Literal["unknown", "none", "per_action", "per_apply"]
_SinkCapabilitySupport: TypeAlias = Literal["unknown", "unsupported", "supported"]
_SinkApplyOrdering: TypeAlias = Literal["unknown", "unordered", "input_order"]
_SinkCompletionVerification: TypeAlias = Literal[
    "unknown", "unverified", "acknowledged", "query_verified"
]

ValueT = TypeVar("ValueT", default=Any)
ValueT_contra = TypeVar("ValueT_contra", contravariant=True, default=Any)
TrackingRecordT = TypeVar("TrackingRecordT", default=Any)
TrackingRecordT_co = TypeVar("TrackingRecordT_co", covariant=True, default=Any)
HandlerT_co = TypeVar(
    "HandlerT_co", covariant=True, bound="TargetHandler[Any, Any, Any]"
)
OptChildHandlerT = TypeVar(
    "OptChildHandlerT",
    bound="TargetHandler[Any, Any, Any] | None",
    default=None,
    covariant=True,
)
OptChildHandlerT_co = TypeVar(
    "OptChildHandlerT_co",
    bound="TargetHandler[Any, Any, Any] | None",
    default=None,
    covariant=True,
)


def _unwrap_target_action(action: Any) -> Any:
    return core._unwrap_target_action(action)


class _TypedTargetHandlerWrapper:
    """Wraps a TargetHandler to auto-deserialize tracking records (StoredValue → typed objects)."""

    __slots__ = ("_handler", "_deserializer")

    def __init__(self, handler: Any) -> None:
        self._handler = handler
        # reconcile(self, key, desired, prev_possible_records, ...) — position 3
        reconcile_label = qualified_name(type(handler).reconcile)
        try:
            ann = get_param_annotation(type(handler).reconcile, 3)
            record_type = unwrap_element_type(ann)
        except Exception:
            record_type = Any
        self._deserializer = make_deserialize_fn(
            record_type,
            source_label=f"prev_possible_records param of {reconcile_label}()",
        )

    def reconcile(
        self,
        key: Any,
        desired: Any,
        prev_possible_records: Any,
        prev_may_be_missing: bool,
        /,
    ) -> Any:
        records = [r.get(self._deserializer) for r in prev_possible_records]
        return self._handler.reconcile(key, desired, records, prev_may_be_missing)

    def attachments(self) -> dict[str, Any]:
        if not hasattr(self._handler, "attachments"):
            return {}
        return {
            k: _TypedTargetHandlerWrapper(v)
            for k, v in self._handler.attachments().items()
        }


class ChildTargetDef(Generic[HandlerT_co], NamedTuple):
    handler: HandlerT_co


class TargetActionSinkFn(Protocol[ActionT_contra, OptChildHandlerT_co]):
    # Case 1: No child handler
    @overload
    def __call__(
        self: TargetActionSinkFn[ActionT_contra, None],
        context_provider: ContextProvider,
        actions: Sequence[ActionT_contra],
        /,
    ) -> None: ...
    # Case 2: With child handler
    @overload
    def __call__(
        self: TargetActionSinkFn[ActionT_contra, HandlerT_co],
        context_provider: ContextProvider,
        actions: Sequence[ActionT_contra],
        /,
    ) -> Sequence[ChildTargetDef[HandlerT_co] | None] | None: ...
    def __call__(
        self, context_provider: ContextProvider, actions: Sequence[ActionT_contra], /
    ) -> Sequence[ChildTargetDef[Any] | None] | None: ...


class AsyncTargetActionSinkFn(Protocol[ActionT_contra, OptChildHandlerT_co]):
    # Case 1: No child handler
    @overload
    async def __call__(
        self: AsyncTargetActionSinkFn[ActionT_contra, None],
        context_provider: ContextProvider,
        actions: Sequence[ActionT_contra],
        /,
    ) -> None: ...
    # Case 2: With child handler
    @overload
    async def __call__(
        self: AsyncTargetActionSinkFn[ActionT_contra, HandlerT_co],
        context_provider: ContextProvider,
        actions: Sequence[ActionT_contra],
        /,
    ) -> Sequence[ChildTargetDef[HandlerT_co] | None] | None: ...
    async def __call__(
        self, context_provider: ContextProvider, actions: Sequence[ActionT_contra], /
    ) -> Sequence[ChildTargetDef[Any] | None] | None: ...


@dataclass(frozen=True, slots=True)
class TargetSinkCapabilities:
    """Machine-readable operational guarantees for one target sink.

    Defaults are deliberately conservative. ``unknown`` means the connector
    has not certified the behavior; it is not treated as support.
    """

    schema_version: int = 1
    batch_atomicity: _SinkBatchAtomicity = "unknown"
    idempotent_replay: _SinkCapabilitySupport = "unknown"
    segmented_replay_safe: _SinkCapabilitySupport = "unknown"
    apply_ordering: _SinkApplyOrdering = "unknown"
    cancellation_safe: _SinkCapabilitySupport = "unknown"
    completion_verification: _SinkCompletionVerification = "unknown"
    max_batch_actions: int | None = None
    max_batch_bytes: int | None = None

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ValueError("unsupported target sink capability schema_version")
        if self.batch_atomicity not in {
            "unknown",
            "none",
            "per_action",
            "per_apply",
        }:
            raise ValueError("invalid target sink batch_atomicity")
        for name in (
            "idempotent_replay",
            "segmented_replay_safe",
            "cancellation_safe",
        ):
            if getattr(self, name) not in {"unknown", "unsupported", "supported"}:
                raise ValueError(f"invalid target sink {name}")
        if (
            self.segmented_replay_safe == "supported"
            and self.idempotent_replay != "supported"
        ):
            raise ValueError(
                "segmented_replay_safe requires idempotent_replay='supported'"
            )
        if self.apply_ordering not in {"unknown", "unordered", "input_order"}:
            raise ValueError("invalid target sink apply_ordering")
        if self.completion_verification not in {
            "unknown",
            "unverified",
            "acknowledged",
            "query_verified",
        }:
            raise ValueError("invalid target sink completion_verification")
        for name in ("max_batch_actions", "max_batch_bytes"):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 1
            ):
                raise ValueError(f"{name} must be a positive integer or None")

    def _core_tuple(self) -> _SinkCapabilitiesTuple:
        return (
            self.schema_version,
            self.batch_atomicity,
            self.idempotent_replay,
            self.segmented_replay_safe,
            self.apply_ordering,
            self.cancellation_safe,
            self.completion_verification,
            self.max_batch_actions,
            self.max_batch_bytes,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "batch_atomicity": self.batch_atomicity,
            "idempotent_replay": self.idempotent_replay,
            "segmented_replay_safe": self.segmented_replay_safe,
            "apply_ordering": self.apply_ordering,
            "cancellation_safe": self.cancellation_safe,
            "completion_verification": self.completion_verification,
            "max_batch_actions": self.max_batch_actions,
            "max_batch_bytes": self.max_batch_bytes,
        }


@dataclass(frozen=True, slots=True)
class TargetSinkQueueStats:
    """Point-in-time target sink queue pressure metrics."""

    ongoing_batches: int
    queued_batches: int
    queued_inputs: int
    in_flight_inputs: int
    in_flight_bytes: int
    capacity_waiters: int


class TargetActionSink(Generic[ActionT_contra, OptChildHandlerT_co]):
    __slots__ = ("_core",)
    _core: core.TargetActionSink

    def __init__(self, core_action_sink: core.TargetActionSink):
        self._core = core_action_sink

    @staticmethod
    def from_fn(
        fn: TargetActionSinkFn[ActionT_contra, OptChildHandlerT_co],
        *,
        capabilities: TargetSinkCapabilities | None = None,
    ) -> "TargetActionSink[ActionT_contra, OptChildHandlerT_co]":
        canonical = _SYNC_FN_DEDUPER.get_canonical(fn)
        return TargetActionSink(
            core.TargetActionSink.new_sync(
                canonical, None if capabilities is None else capabilities._core_tuple()
            )
        )

    @staticmethod
    def from_async_fn(
        fn: AsyncTargetActionSinkFn[ActionT_contra, OptChildHandlerT_co],
        *,
        capabilities: TargetSinkCapabilities | None = None,
    ) -> "TargetActionSink[ActionT_contra, OptChildHandlerT_co]":
        canonical = _ASYNC_FN_DEDUPER.get_canonical(fn)
        return TargetActionSink(
            core.TargetActionSink.new_async(
                canonical, None if capabilities is None else capabilities._core_tuple()
            )
        )

    @staticmethod
    def _from_verified_wrapper(
        fn: AsyncTargetActionSinkFn[ActionT_contra, OptChildHandlerT_co],
    ) -> "TargetActionSink[ActionT_contra, OptChildHandlerT_co]":
        canonical = _ASYNC_FN_DEDUPER.get_canonical(fn)
        return TargetActionSink(core.TargetActionSink._new_verified_wrapper(canonical))

    @property
    def capabilities(self) -> TargetSinkCapabilities:
        (
            schema_version,
            batch_atomicity,
            idempotent_replay,
            segmented_replay_safe,
            apply_ordering,
            cancellation_safe,
            completion_verification,
            max_batch_actions,
            max_batch_bytes,
        ) = self._core.capabilities()
        return TargetSinkCapabilities(
            schema_version=schema_version,
            batch_atomicity=cast(_SinkBatchAtomicity, batch_atomicity),
            idempotent_replay=cast(_SinkCapabilitySupport, idempotent_replay),
            segmented_replay_safe=cast(_SinkCapabilitySupport, segmented_replay_safe),
            apply_ordering=cast(_SinkApplyOrdering, apply_ordering),
            cancellation_safe=cast(_SinkCapabilitySupport, cancellation_safe),
            completion_verification=cast(
                _SinkCompletionVerification, completion_verification
            ),
            max_batch_actions=max_batch_actions,
            max_batch_bytes=max_batch_bytes,
        )

    @property
    def queue_stats(self) -> TargetSinkQueueStats:
        stats: _SinkQueueStatsTuple = self._core.queue_stats()
        return TargetSinkQueueStats(*stats)


class _ObjectDeduper:
    __slots__ = ("_lock", "_map")
    _lock: threading.Lock
    _map: weakref.WeakValueDictionary[Any, Any]

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._map = weakref.WeakValueDictionary()

    def get_canonical(self, obj: Any) -> Any:
        with self._lock:
            value = self._map.get(obj)
            if value is not None:
                return value

            self._map[obj] = obj
            return obj


_SYNC_FN_DEDUPER = _ObjectDeduper()
_ASYNC_FN_DEDUPER = _ObjectDeduper()


class TargetReconcileOutput(
    Generic[ActionT, TrackingRecordT_co, OptChildHandlerT_co], NamedTuple
):
    action: ActionT
    sink: TargetActionSink[ActionT, OptChildHandlerT_co]
    tracking_record: TrackingRecordT_co | AbsentType
    child_invalidation: Literal["destructive", "lossy"] | None = None


class TargetHandler(Protocol[ValueT_contra, TrackingRecordT, OptChildHandlerT_co]):
    def reconcile(
        self,
        key: StableKey,
        desired_target_state: ValueT_contra | AbsentType,
        prev_possible_records: Collection[TrackingRecordT],
        prev_may_be_missing: bool,
        /,
    ) -> TargetReconcileOutput[Any, TrackingRecordT, OptChildHandlerT_co] | None: ...


class TargetStateProvider(
    Generic[ValueT, OptChildHandlerT, MaybePendingS],
    ResolvesTo["TargetStateProvider[ValueT, OptChildHandlerT]"],
):
    __slots__ = ("_core",)
    _core: core.TargetStateProvider

    def __init__(self, core_provider: core.TargetStateProvider):
        self._core = core_provider

    @property
    def memo_key(self) -> str:
        return self._core.synor_memo_key()

    def target_state(
        self: TargetStateProvider[ValueT, OptChildHandlerT],
        key: StableKey,
        value: ValueT,
    ) -> "TargetState[OptChildHandlerT]":
        return TargetState(self, key, value)

    def attachment(
        self: TargetStateProvider[ValueT, OptChildHandlerT],
        att_type: str,
    ) -> "TargetStateProvider":
        ctx = get_context_from_ctx()
        provider = self._core.register_attachment_provider(
            ctx._core_processor_ctx, att_type
        )
        return TargetStateProvider(provider)

    def __synor_memo_key__(self) -> str:
        return self._core.synor_memo_key()


PendingTargetStateProvider: TypeAlias = TargetStateProvider[
    ValueT, OptChildHandlerT, PendingS
]


class TargetState(Generic[OptChildHandlerT]):
    __slots__ = ("_provider", "_key", "_value")
    _provider: TargetStateProvider[Any, OptChildHandlerT]
    _key: Any
    _value: Any

    def __init__(
        self,
        provider: TargetStateProvider[ValueT, OptChildHandlerT],
        key: StableKey,
        value: ValueT,
    ):
        self._provider = provider
        self._key = key
        self._value = value


def ensure_target_state(target_state: TargetState[None]) -> None:
    """
    Declare a target state within the current component context.

    Args:
        target_state: The target state to declare.
    """
    ctx = get_context_from_ctx()
    core.declare_target_state(
        ctx._core_processor_ctx,
        ctx._core_fn_call_ctx,
        target_state._provider._core,
        target_state._key,
        target_state._value,
    )


def ensure_target_state_with_child(
    target_state: TargetState[TargetHandler[ValueT, Any, OptChildHandlerT]],
) -> PendingTargetStateProvider[ValueT, OptChildHandlerT]:
    """
    Declare a target state with a child handler within the current component context.

    Args:
        target_state: The target state to declare.

    Returns:
        A TargetStateProvider for the child target states.
    """
    ctx = get_context_from_ctx()
    provider = core.declare_target_state_with_child(
        ctx._core_processor_ctx,
        ctx._core_fn_call_ctx,
        target_state._provider._core,
        target_state._key,
        target_state._value,
    )
    return TargetStateProvider(provider)


def register_root_target_states_provider(
    name: str, handler: TargetHandler[ValueT, Any, OptChildHandlerT]
) -> TargetStateProvider[ValueT, OptChildHandlerT]:
    wrapped = _TypedTargetHandlerWrapper(handler)
    provider = core.register_root_target_states_provider(name, wrapped)
    return TargetStateProvider(provider)
