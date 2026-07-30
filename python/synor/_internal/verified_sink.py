"""Apply-and-verify target action sinks for strict revocation workflows.

This module deliberately wraps the existing :class:`TargetActionSink` callback
contract instead of extending ``TargetReconcileOutput``.  The Rust engine still
sees the same four-field reconciliation tuple and the callback still returns
only the original optional child-handler sequence.

The wrapper's security boundary is intentionally narrow:

* actions must provide a privacy-safe, stable effect descriptor before apply;
* apply completes before bounded read-back verification starts;
* verification results are correlated to every action in engine batch order;
* only normalized descriptor fields and status/detail codes reach a recorder;
* a failed verification or recorder write raises, so the engine cannot final-
  commit the target tracking record as a successful strict effect.

Raw target actions and remote exception messages are never interpolated into
errors produced by this module.
"""

from __future__ import annotations

import enum
import math
import re
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, Protocol, TypeAlias, TypeVar, cast, runtime_checkable

from . import deadline
from .context_keys import ContextProvider
from .revocation_model import EffectDescriptor, EffectOperation, VerificationOutcome
from .revocation_policy import RevocationPolicy
from .target_state import (
    AsyncTargetActionSinkFn,
    ChildTargetDef,
    TargetActionSink,
    TargetHandler,
    _NativeEffectDescriptorTuple,
)

_SAFE_CODE_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]*\Z")
_MAX_SAFE_CODE_LENGTH = 256
_ALLOWED_DETAIL_CODES = frozenset(
    {
        "read_replica_stale",
        "replica_stale",
        "verifier_exception",
        *(f"last_{outcome.value}" for outcome in VerificationOutcome),
    }
)

ActionT = TypeVar("ActionT")
ActionT_contra = TypeVar("ActionT_contra", contravariant=True)

_AnyTargetHandler: TypeAlias = TargetHandler[Any, Any, Any]
TargetSinkApplyResult: TypeAlias = (
    Sequence[ChildTargetDef[_AnyTargetHandler] | None] | None
)

ApplyResultT = TypeVar("ApplyResultT", bound=TargetSinkApplyResult)
ApplyResultT_co = TypeVar(
    "ApplyResultT_co", bound=TargetSinkApplyResult, covariant=True
)
ApplyResultT_contra = TypeVar(
    "ApplyResultT_contra", bound=TargetSinkApplyResult, contravariant=True
)


class VerificationProtocolCode(str, enum.Enum):
    """Stable reason codes for malformed strict-sink inputs or results."""

    ACTION_DESCRIPTOR_MISSING = "action_descriptor_missing"
    ACTION_DESCRIPTOR_INVALID = "action_descriptor_invalid"
    ACTION_NOT_REVOCATION = "action_not_revocation"
    DUPLICATE_ACTION_ID = "duplicate_action_id"
    RESULT_INVALID = "result_invalid"
    RESULT_COUNT_MISMATCH = "result_count_mismatch"
    RESULT_CORRELATION_MIXED = "result_correlation_mixed"
    RESULT_ID_DUPLICATE = "result_id_duplicate"
    RESULT_ID_MISMATCH = "result_id_mismatch"


@runtime_checkable
class EffectDescribedAction(Protocol):
    """An action able to return the shared privacy-safe effect descriptor."""

    def __synor_effect_descriptor__(self, /) -> EffectDescriptor: ...


@dataclass(frozen=True, slots=True)
class TargetVerificationResult:
    """One verifier observation.

    ``action_id=None`` selects positional correlation and therefore requires
    exactly one result per input action.  When IDs are supplied, every result
    must supply one and the ID set must exactly match the action descriptors;
    results may then arrive in any order.

    ``detail_code`` is optional machine-readable metadata such as
    ``"read_replica_stale"``.  It is subject to the same safe-code validation
    and must never contain a remote exception message.
    """

    status: VerificationOutcome
    action_id: str | None = None
    detail_code: str | None = None
    operation_id: str | None = None
    affected_count: int | None = None

    @property
    def required_postcondition_holds(self) -> bool:
        """Whether the observation can prove some revocation postcondition.

        The normalized outcome additionally checks the effect operation:
        ``retained_isolated`` is valid only for an isolation effect.
        """

        return self.status in (
            VerificationOutcome.ABSENT,
            VerificationOutcome.RETAINED_ISOLATED,
        )


@dataclass(frozen=True, slots=True)
class TargetVerificationOutcome:
    """Recorder-safe final outcome, normalized into engine action order."""

    action_id: str
    operation: EffectOperation
    source_digest: str
    source_generation: int
    target_locator_digest: str
    status: VerificationOutcome
    attempt_count: int
    detail_code: str | None = None
    operation_id: str | None = None
    affected_count: int | None = None

    @property
    def required_postcondition_holds(self) -> bool:
        if self.operation is EffectOperation.ISOLATE:
            return self.status is VerificationOutcome.RETAINED_ISOLATED
        return self.status is VerificationOutcome.ABSENT


@dataclass(frozen=True, slots=True)
class VerificationRetryPolicy:
    """Internal total bound and polling schedule for target verification.

    Set ``initial_backoff=0``, ``max_backoff=0``, and ``jitter=0`` for exact,
    sleep-free unit tests.  Production callers should retain non-zero backoff
    and jitter to avoid synchronized read-back load.
    """

    timeout: timedelta = timedelta(seconds=30)
    max_attempts: int = 8
    initial_backoff: float = 0.05
    backoff_multiplier: float = 2.0
    max_backoff: float = 1.0
    jitter: float = 0.2

    @classmethod
    def from_revocation_policy(
        cls,
        policy: RevocationPolicy,
        *,
        max_attempts: int = 8,
        backoff_multiplier: float = 2.0,
    ) -> "VerificationRetryPolicy":
        """Derive polling mechanics from the shared revocation policy.

        ``RevocationPolicy`` owns the product/security preset.  This smaller
        type adds the attempt cap and exponential multiplier needed by the
        shared deadline retry primitive without duplicating those policy
        values.
        """

        return cls(
            timeout=timedelta(seconds=policy.verification_timeout_seconds),
            max_attempts=max_attempts,
            initial_backoff=policy.initial_backoff_seconds,
            backoff_multiplier=backoff_multiplier,
            max_backoff=policy.maximum_backoff_seconds,
            jitter=policy.jitter_ratio,
        )

    def __post_init__(self) -> None:
        if not isinstance(self.timeout, timedelta) or self.timeout <= timedelta(0):
            raise ValueError("verification timeout must be a positive timedelta")
        if (
            not isinstance(self.max_attempts, int)
            or isinstance(self.max_attempts, bool)
            or self.max_attempts < 1
        ):
            raise ValueError("verification max_attempts must be at least 1")
        _validate_finite_number("initial_backoff", self.initial_backoff, minimum=0.0)
        _validate_finite_number(
            "backoff_multiplier", self.backoff_multiplier, minimum=1.0
        )
        _validate_finite_number("max_backoff", self.max_backoff, minimum=0.0)
        _validate_finite_number("jitter", self.jitter, minimum=0.0, maximum=1.0)
        if self.max_backoff < self.initial_backoff:
            raise ValueError(
                "verification max_backoff must be at least initial_backoff"
            )


class AsyncApplyFn(Protocol[ActionT_contra, ApplyResultT_co]):
    async def __call__(
        self,
        context_provider: ContextProvider,
        actions: Sequence[ActionT_contra],
        /,
    ) -> ApplyResultT_co: ...


class AsyncVerifyFn(Protocol[ActionT_contra, ApplyResultT_contra]):
    async def __call__(
        self,
        context_provider: ContextProvider,
        actions: Sequence[ActionT_contra],
        applied: ApplyResultT_contra,
        /,
    ) -> Sequence[TargetVerificationResult]: ...


class AsyncOutcomeRecorder(Protocol):
    async def __call__(
        self,
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None: ...


class VerifiedSinkError(RuntimeError):
    """Base class for safe errors raised by a verified target sink."""


class TargetActionDescriptionError(VerifiedSinkError):
    """An action did not provide a valid privacy-safe effect descriptor."""

    code: VerificationProtocolCode

    def __init__(self, code: VerificationProtocolCode) -> None:
        self.code = code
        super().__init__(f"verified target action description failed: {code.value}")


class TargetActionApplyError(VerifiedSinkError):
    """The external apply callback failed before verification."""

    def __init__(self) -> None:
        super().__init__("verified target action application failed")


class TargetVerificationProtocolError(VerifiedSinkError):
    """The verifier did not return a complete, correlatable result set."""

    code: VerificationProtocolCode

    def __init__(self, code: VerificationProtocolCode) -> None:
        self.code = code
        super().__init__(f"target verification protocol error: {code.value}")


class TargetVerificationError(VerifiedSinkError):
    """At least one target action failed its required postcondition."""

    outcomes: tuple[TargetVerificationOutcome, ...]

    def __init__(self, outcomes: Sequence[TargetVerificationOutcome]) -> None:
        self.outcomes = tuple(outcomes)
        counts = Counter(outcome.status for outcome in self.outcomes)
        summary = ", ".join(
            f"{status.value}={counts[status]}"
            for status in VerificationOutcome
            if counts[status]
        )
        super().__init__(
            "target verification failed"
            if not summary
            else f"target verification failed: {summary}"
        )


class TargetVerificationRecorderError(VerifiedSinkError):
    """Persisting the verification outcomes failed."""

    outcomes: tuple[TargetVerificationOutcome, ...]

    def __init__(self, outcomes: Sequence[TargetVerificationOutcome]) -> None:
        self.outcomes = tuple(outcomes)
        super().__init__("target verification outcome recording failed")


class _VerificationPending(Exception):
    outcomes: tuple[TargetVerificationOutcome, ...]

    def __init__(self, outcomes: Sequence[TargetVerificationOutcome]) -> None:
        self.outcomes = tuple(outcomes)
        # This fixed message can pass through the shared retry helper without
        # leaking a target action, locator, or remote error.
        super().__init__("target verification postcondition is pending")


class _VerificationTerminal(Exception):
    outcomes: tuple[TargetVerificationOutcome, ...]

    def __init__(self, outcomes: Sequence[TargetVerificationOutcome]) -> None:
        self.outcomes = tuple(outcomes)
        super().__init__("target verification cannot satisfy the postcondition")


class VerifiedTargetActionSink(Generic[ActionT, ApplyResultT]):
    """Add bounded verification and durable outcome recording to an async sink.

    ``sink`` is the ordinary :class:`TargetActionSink` consumed by
    ``TargetReconcileOutput``.  Calling this wrapper directly has identical
    behavior and is useful for connector conformance tests.
    """

    __slots__ = (
        "_apply",
        "_describe_action",
        "_invocation_scope",
        "_policy",
        "_record",
        "_sink",
        "_verify",
        "__weakref__",
    )

    _apply: AsyncApplyFn[ActionT, ApplyResultT]
    _verify: AsyncVerifyFn[ActionT, ApplyResultT]
    _record: AsyncOutcomeRecorder
    _describe_action: Callable[[ActionT], EffectDescriptor]
    _policy: VerificationRetryPolicy
    _sink: TargetActionSink[ActionT, Any]

    def __init__(
        self,
        *,
        apply: AsyncApplyFn[ActionT, ApplyResultT],
        verify: AsyncVerifyFn[ActionT, ApplyResultT],
        record: AsyncOutcomeRecorder,
        describe_action: Callable[[ActionT], EffectDescriptor] | None = None,
        policy: VerificationRetryPolicy | None = None,
        invocation_scope: Callable[[], Callable[[], None]] | None = None,
    ) -> None:
        self._apply = apply
        self._verify = verify
        self._record = record
        self._describe_action = describe_action or _default_describe_action
        self._policy = policy or VerificationRetryPolicy()
        self._invocation_scope = invocation_scope
        callback = cast(AsyncTargetActionSinkFn[Any, Any], self)
        self._sink = TargetActionSink._from_verified_wrapper(callback)

    @property
    def sink(self) -> TargetActionSink[ActionT, Any]:
        """The unchanged core-facing target action sink."""

        return self._sink

    async def __call__(
        self,
        context_provider: ContextProvider,
        actions: Sequence[ActionT],
        /,
    ) -> ApplyResultT:
        cleanup = (
            self._invocation_scope() if self._invocation_scope is not None else None
        )
        try:
            return await self._call(context_provider, actions)
        finally:
            if cleanup is not None:
                cleanup()

    async def _call(
        self,
        context_provider: ContextProvider,
        actions: Sequence[ActionT],
        /,
    ) -> ApplyResultT:
        action_batch = tuple(actions)
        return await self._call_described(
            context_provider,
            action_batch,
            self._describe_actions(action_batch),
        )

    async def _call_described(
        self,
        context_provider: ContextProvider,
        action_batch: tuple[ActionT, ...],
        descriptors: tuple[EffectDescriptor, ...],
        /,
    ) -> ApplyResultT:
        apply_failed = False
        try:
            applied = await self._apply(context_provider, action_batch)
        except Exception:
            # Cancellation is a BaseException on supported Python versions
            # and therefore still propagates.  Ordinary connector exceptions
            # are normalized after leaving the exception handler so the raw
            # error is not retained through ``__context__``.
            apply_failed = True
        if apply_failed:
            raise TargetActionApplyError() from None

        outcomes = await self._verify_bounded(
            context_provider,
            action_batch,
            descriptors,
            applied,
        )
        record_failed = False
        try:
            await self._record(context_provider, outcomes)
        except Exception:
            # A receipt/ledger failure is strict-sink failure.  The target may
            # already have converged, but raising prevents final tracking
            # commit and makes the idempotent action retryable.  Raise after
            # the handler so the raw callback error is not chained.
            record_failed = True
        if record_failed:
            raise TargetVerificationRecorderError(outcomes) from None

        if any(not outcome.required_postcondition_holds for outcome in outcomes):
            raise TargetVerificationError(outcomes)
        return applied

    async def _call_bound_for_core(
        self,
        context_provider: ContextProvider,
        actions: Sequence[ActionT],
        descriptor_values: Sequence[_NativeEffectDescriptorTuple],
        /,
    ) -> ApplyResultT:
        action_batch = tuple(actions)
        descriptors = _descriptors_from_core(descriptor_values, len(action_batch))
        cleanup = (
            self._invocation_scope() if self._invocation_scope is not None else None
        )
        try:
            return await self._call_described(
                context_provider,
                action_batch,
                descriptors,
            )
        finally:
            if cleanup is not None:
                cleanup()

    def _describe_actions(
        self, actions: Sequence[ActionT]
    ) -> tuple[EffectDescriptor, ...]:
        descriptors: list[EffectDescriptor] = []
        for action in actions:
            description_error_code: VerificationProtocolCode | None = None
            try:
                descriptor = self._describe_action(action)
                _validate_descriptor(descriptor)
            except TargetActionDescriptionError as error:
                description_error_code = (
                    error.code
                    if isinstance(error.code, VerificationProtocolCode)
                    else VerificationProtocolCode.ACTION_DESCRIPTOR_INVALID
                )
            except Exception:
                description_error_code = (
                    VerificationProtocolCode.ACTION_DESCRIPTOR_INVALID
                )
            if description_error_code is not None:
                raise TargetActionDescriptionError(description_error_code) from None
            descriptors.append(descriptor)

        if len({descriptor.action_id for descriptor in descriptors}) != len(
            descriptors
        ):
            raise TargetActionDescriptionError(
                VerificationProtocolCode.DUPLICATE_ACTION_ID
            )
        return tuple(descriptors)

    def _describe_for_core(
        self,
        action: ActionT,
        /,
    ) -> tuple[str, str, str, int, str]:
        descriptor = self._describe_actions((action,))[0]
        return _descriptor_to_core_tuple(descriptor)

    async def _verify_bounded(
        self,
        context_provider: ContextProvider,
        actions: Sequence[ActionT],
        descriptors: Sequence[EffectDescriptor],
        applied: ApplyResultT,
    ) -> tuple[TargetVerificationOutcome, ...]:
        attempt_count = 0
        last_outcomes: tuple[TargetVerificationOutcome, ...] | None = None

        async def verify_once() -> tuple[TargetVerificationOutcome, ...]:
            nonlocal attempt_count, last_outcomes
            attempt_count += 1
            try:
                raw_results = await self._verify(context_provider, actions, applied)
            except Exception:
                # Do not retain or expose a possibly sensitive exception
                # string.  Transport diagnostics belong in the connector's
                # own redacted telemetry, not in a revocation receipt.
                outcomes = tuple(
                    _outcome(
                        descriptor,
                        status=VerificationOutcome.TRANSPORT_FAILURE,
                        attempt_count=attempt_count,
                        detail_code="verifier_exception",
                    )
                    for descriptor in descriptors
                )
            else:
                outcomes = _correlate_results(
                    raw_results,
                    descriptors,
                    attempt_count=attempt_count,
                )

            last_outcomes = outcomes
            if all(outcome.required_postcondition_holds for outcome in outcomes):
                return outcomes
            if any(
                outcome.status
                in (VerificationOutcome.UNSUPPORTED, VerificationOutcome.TIMEOUT)
                for outcome in outcomes
            ):
                raise _VerificationTerminal(outcomes)
            raise _VerificationPending(outcomes)

        backoff = deadline.exponential_backoff(
            initial=self._policy.initial_backoff,
            multiplier=self._policy.backoff_multiplier,
            max_delay=self._policy.max_backoff,
            jitter=self._policy.jitter,
        )
        try:
            return await deadline.retry_transient(
                verify_once,
                retry_on=(_VerificationPending,),
                max_attempts=self._policy.max_attempts,
                timeout=self._policy.timeout,
                backoff=backoff,
                bound_attempt=True,
            )
        except _VerificationPending as error:
            return error.outcomes
        except _VerificationTerminal as error:
            return error.outcomes
        except deadline.DeadlineExceededError:
            return _timeout_outcomes(
                descriptors,
                last_outcomes=last_outcomes,
                attempt_count=attempt_count,
            )


def _descriptor_to_core_tuple(
    descriptor: EffectDescriptor,
) -> _NativeEffectDescriptorTuple:
    return (
        descriptor.operation_kind.value,
        descriptor.action_id,
        descriptor.source_digest,
        descriptor.source_generation,
        descriptor.target_locator_digest,
    )


def _descriptor_from_core_tuple(
    value: _NativeEffectDescriptorTuple,
) -> EffectDescriptor:
    operation, action_id, source_digest, source_generation, target_locator_digest = (
        value
    )
    try:
        descriptor = EffectDescriptor(
            operation_kind=EffectOperation(operation),
            action_id=action_id,
            source_digest=source_digest,
            source_generation=source_generation,
            target_locator_digest=target_locator_digest,
        )
        _validate_descriptor(descriptor)
    except (TypeError, ValueError):
        raise TargetActionDescriptionError(
            VerificationProtocolCode.ACTION_DESCRIPTOR_INVALID
        ) from None
    return descriptor


def _descriptors_from_core(
    values: Sequence[_NativeEffectDescriptorTuple],
    action_count: int,
) -> tuple[EffectDescriptor, ...]:
    if len(values) != action_count:
        raise TargetActionDescriptionError(
            VerificationProtocolCode.ACTION_DESCRIPTOR_INVALID
        ) from None
    try:
        descriptors = tuple(_descriptor_from_core_tuple(value) for value in values)
    except (TypeError, ValueError):
        raise TargetActionDescriptionError(
            VerificationProtocolCode.ACTION_DESCRIPTOR_INVALID
        ) from None
    if len({descriptor.action_id for descriptor in descriptors}) != len(descriptors):
        raise TargetActionDescriptionError(VerificationProtocolCode.DUPLICATE_ACTION_ID)
    return descriptors


def _default_describe_action(action: ActionT) -> EffectDescriptor:
    if not isinstance(action, EffectDescribedAction):
        raise TargetActionDescriptionError(
            VerificationProtocolCode.ACTION_DESCRIPTOR_MISSING
        )
    description_failed = False
    try:
        descriptor = action.__synor_effect_descriptor__()
    except Exception:
        description_failed = True
    if description_failed:
        raise TargetActionDescriptionError(
            VerificationProtocolCode.ACTION_DESCRIPTOR_INVALID
        ) from None
    return descriptor


def _validate_descriptor(descriptor: object) -> None:
    # This is a privacy-safe value boundary, not an extension point.  Reject
    # subclasses whose property access can execute arbitrary connector code.
    if type(descriptor) is not EffectDescriptor:
        raise TargetActionDescriptionError(
            VerificationProtocolCode.ACTION_DESCRIPTOR_INVALID
        )
    try:
        _validate_safe_code(descriptor.action_id)
        _validate_safe_code(descriptor.operation_kind.value)
        _validate_safe_code(descriptor.source_digest)
        _validate_safe_code(descriptor.target_locator_digest)
    except (TypeError, ValueError):
        raise TargetActionDescriptionError(
            VerificationProtocolCode.ACTION_DESCRIPTOR_INVALID
        ) from None
    if not descriptor.destructive:
        raise TargetActionDescriptionError(
            VerificationProtocolCode.ACTION_NOT_REVOCATION
        )


def _correlate_results(
    raw_results: Sequence[TargetVerificationResult],
    descriptors: Sequence[EffectDescriptor],
    *,
    attempt_count: int,
) -> tuple[TargetVerificationOutcome, ...]:
    protocol_error_code: VerificationProtocolCode | None = None
    try:
        results = tuple(raw_results)
        return _correlate_materialized_results(
            results,
            descriptors,
            attempt_count=attempt_count,
        )
    except TargetVerificationProtocolError as error:
        protocol_error_code = error.code
    except Exception:
        # ``Sequence`` does not guarantee eager storage: connector adapters
        # may return proxy/lazy sequences whose iterator, item accessors, or
        # result properties raise.  Treat every ordinary failure inside this
        # untrusted protocol boundary as malformed verifier output.  Never
        # retain its exception message or chain because either can contain
        # remote payloads, credentials, or target locators.
        protocol_error_code = VerificationProtocolCode.RESULT_INVALID

    # Raise outside the handler so Python does not retain the untrusted
    # exception as ``__context__`` even though it would be display-suppressed
    # by ``raise ... from None``.
    assert protocol_error_code is not None
    raise TargetVerificationProtocolError(protocol_error_code) from None


def _correlate_materialized_results(
    results: Sequence[TargetVerificationResult],
    descriptors: Sequence[EffectDescriptor],
    *,
    attempt_count: int,
) -> tuple[TargetVerificationOutcome, ...]:
    for result in results:
        if not isinstance(result, TargetVerificationResult) or not isinstance(
            result.status, VerificationOutcome
        ):
            raise TargetVerificationProtocolError(
                VerificationProtocolCode.RESULT_INVALID
            )
        try:
            if result.action_id is not None:
                _validate_safe_code(result.action_id)
            if result.detail_code is not None:
                _validate_detail_code(result.detail_code)
            if result.operation_id is not None:
                _validate_safe_code(result.operation_id)
            if result.affected_count is not None and (
                not isinstance(result.affected_count, int)
                or isinstance(result.affected_count, bool)
                or result.affected_count < 0
            ):
                raise ValueError("affected_count must be non-negative")
        except (TypeError, ValueError):
            raise TargetVerificationProtocolError(
                VerificationProtocolCode.RESULT_INVALID
            ) from None

    has_id = [result.action_id is not None for result in results]
    if any(has_id) and not all(has_id):
        raise TargetVerificationProtocolError(
            VerificationProtocolCode.RESULT_CORRELATION_MIXED
        )

    if not any(has_id):
        if len(results) != len(descriptors):
            raise TargetVerificationProtocolError(
                VerificationProtocolCode.RESULT_COUNT_MISMATCH
            )
        return tuple(
            _outcome(
                descriptor,
                status=result.status,
                attempt_count=attempt_count,
                detail_code=result.detail_code,
                operation_id=result.operation_id,
                affected_count=result.affected_count,
            )
            for descriptor, result in zip(descriptors, results)
        )

    by_id: dict[str, TargetVerificationResult] = {}
    for result in results:
        # The all-or-none branch above proves action_id is present here.
        action_id = cast(str, result.action_id)
        if action_id in by_id:
            raise TargetVerificationProtocolError(
                VerificationProtocolCode.RESULT_ID_DUPLICATE
            )
        by_id[action_id] = result

    expected_ids = {descriptor.action_id for descriptor in descriptors}
    if set(by_id) != expected_ids:
        raise TargetVerificationProtocolError(
            VerificationProtocolCode.RESULT_ID_MISMATCH
        )

    return tuple(
        _outcome(
            descriptor,
            status=by_id[descriptor.action_id].status,
            attempt_count=attempt_count,
            detail_code=by_id[descriptor.action_id].detail_code,
            operation_id=by_id[descriptor.action_id].operation_id,
            affected_count=by_id[descriptor.action_id].affected_count,
        )
        for descriptor in descriptors
    )


def _timeout_outcomes(
    descriptors: Sequence[EffectDescriptor],
    *,
    last_outcomes: Sequence[TargetVerificationOutcome] | None,
    attempt_count: int,
) -> tuple[TargetVerificationOutcome, ...]:
    last_by_id = (
        {outcome.action_id: outcome for outcome in last_outcomes}
        if last_outcomes is not None
        else {}
    )
    outcomes: list[TargetVerificationOutcome] = []
    for descriptor in descriptors:
        previous = last_by_id.get(descriptor.action_id)
        if previous is not None and previous.required_postcondition_holds:
            outcomes.append(previous)
            continue
        detail_code = f"last_{previous.status.value}" if previous is not None else None
        outcomes.append(
            _outcome(
                descriptor,
                status=VerificationOutcome.TIMEOUT,
                attempt_count=attempt_count,
                detail_code=detail_code,
                operation_id=(previous.operation_id if previous is not None else None),
                affected_count=(
                    previous.affected_count if previous is not None else None
                ),
            )
        )
    return tuple(outcomes)


def _outcome(
    descriptor: EffectDescriptor,
    *,
    status: VerificationOutcome,
    attempt_count: int,
    detail_code: str | None,
    operation_id: str | None = None,
    affected_count: int | None = None,
) -> TargetVerificationOutcome:
    return TargetVerificationOutcome(
        action_id=descriptor.action_id,
        operation=descriptor.operation_kind,
        source_digest=descriptor.source_digest,
        source_generation=descriptor.source_generation,
        target_locator_digest=descriptor.target_locator_digest,
        status=status,
        attempt_count=attempt_count,
        detail_code=detail_code,
        operation_id=operation_id,
        affected_count=affected_count,
    )


def _validate_safe_code(value: object) -> None:
    if (
        not isinstance(value, str)
        or len(value) < 1
        or len(value) > _MAX_SAFE_CODE_LENGTH
        or _SAFE_CODE_PATTERN.fullmatch(value) is None
    ):
        raise ValueError("value is not a safe opaque code")


def _validate_detail_code(value: object) -> None:
    if not isinstance(value, str) or value not in _ALLOWED_DETAIL_CODES:
        raise ValueError("value is not a controlled verification detail code")


def _validate_finite_number(
    name: str,
    value: float,
    *,
    minimum: float,
    maximum: float | None = None,
) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < minimum
        or (maximum is not None and value > maximum)
    ):
        bounds = f">= {minimum}" if maximum is None else f"in [{minimum}, {maximum}]"
        raise ValueError(f"verification {name} must be finite and {bounds}")
