"""Public control-plane API for evidence-backed index revocation.

The public types in this module are aliases of the proven internal contracts.
No second schema or conversion layer is introduced.  The repository provides
metadata-only operator views; reversible source and target locators remain the
responsibility of a separately protected connector/operator implementation.
"""

from __future__ import annotations

import dataclasses as _dataclasses
import datetime as _datetime
import re as _re
import typing as _typing

from . import state as _state
from ._internal import context_keys as _context_keys
from ._internal import revocation_ledger as _ledger
from ._internal import revocation_model as _model
from ._internal import revocation_policy as _policy
from ._internal import revocation_runtime as _runtime
from ._internal import suppression as _suppression
from ._internal import verified_sink as _verified

AccessSnapshot = _model.AccessSnapshot
AssuranceLevel = _model.AssuranceLevel
EffectDescriptor = _model.EffectDescriptor
EffectOperation = _model.EffectOperation
GovernedSourceItem = _model.GovernedSourceItem
RevocationCase = _model.RevocationCase
RevocationPolicyDecision = _model.RevocationPolicyDecision
RevocationReceipt = _model.RevocationReceipt
RevocationSchemaError = _model.RevocationSchemaError
RevocationStage = _model.RevocationStage
SafeRevocationErrorCode = _model.SafeRevocationErrorCode
SnapshotResult = _model.SnapshotResult
SourceEventKind = _model.SourceEventKind
SourceIdentity = _model.SourceIdentity
TargetRevocationCapabilities = _model.TargetRevocationCapabilities
VerificationOutcome = _model.VerificationOutcome
make_observation_id = _model.make_observation_id
make_tenant_digest = _model.make_tenant_digest

LedgerRepairReport = _ledger.LedgerRepairReport
RevocationLedgerConflict = _ledger.RevocationLedgerConflict
RevocationLedgerCorruption = _ledger.RevocationLedgerCorruption
RevocationLedgerError = _ledger.RevocationLedgerError

RevocationCapabilityError = _policy.RevocationCapabilityError
RevocationPolicy = _policy.RevocationPolicy
RevocationPolicyMode = _policy.RevocationPolicyMode

RevocationRequest = _runtime.RevocationRequest
RevocationRuntimeError = _runtime.RevocationRuntimeError
RevocationRuntimeProtocolError = _runtime.RevocationRuntimeProtocolError
RevocationRuntimeStateError = _runtime.RevocationRuntimeStateError
TargetObligation = _runtime.TargetObligation

SuppressionCorruptionError = _suppression.SuppressionCorruptionError
SuppressionRecord = _suppression.SuppressionRecord
StateStoreSuppressionIndex = _suppression.StateStoreSuppressionIndex

TargetVerificationOutcome = _verified.TargetVerificationOutcome

__all__ = [
    "AccessSnapshot",
    "AssuranceLevel",
    "EffectDescriptor",
    "EffectOperation",
    "GovernedSourceItem",
    "LedgerRepairReport",
    "OperatorResult",
    "RevocationCapabilityError",
    "RevocationCase",
    "RevocationController",
    "RevocationHealth",
    "RevocationLedgerConflict",
    "RevocationLedgerCorruption",
    "RevocationLedgerError",
    "RevocationOperator",
    "RevocationOperatorResult",
    "RevocationPolicy",
    "RevocationPolicyDecision",
    "RevocationPolicyMode",
    "RevocationReceipt",
    "RevocationRepository",
    "RevocationRequest",
    "RevocationRuntimeError",
    "RevocationRuntimeProtocolError",
    "RevocationRuntimeStateError",
    "RevocationSchemaError",
    "RevocationStage",
    "RevocationSummary",
    "RevocationScanResult",
    "SafeRevocationErrorCode",
    "ScanResult",
    "SnapshotResult",
    "SourceEventKind",
    "SourceIdentity",
    "StateStoreSuppressionIndex",
    "SuppressionCorruptionError",
    "SuppressionRecord",
    "TargetObligation",
    "TargetRevocationCapabilities",
    "TargetVerificationOutcome",
    "VerificationOutcome",
    "make_observation_id",
    "make_tenant_digest",
]

_SCHEMA_VERSION = 1
_SAFE_TOKEN = _re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
_HEALTH_SAFE_ERROR_CODES = frozenset(
    {
        "revocation.deadline_overdue",
        "revocation.state_corrupt",
        "revocation.state_unavailable",
        "revocation.suppression_unconfirmed",
    }
)
_OPERATOR_SAFE_ERROR_CODES = frozenset(code.value for code in SafeRevocationErrorCode)
_SUPPRESSED_STAGES = frozenset(
    {
        RevocationStage.SUPPRESSED,
        RevocationStage.PLANNED,
        RevocationStage.DISPATCHED,
        RevocationStage.ACKNOWLEDGED,
        RevocationStage.FENCE_REACHED,
    }
)
_TERMINAL_STAGES = frozenset(
    {
        RevocationStage.VERIFIED,
        RevocationStage.RETAINED_ISOLATED,
        RevocationStage.CLOSED,
    }
)
_VERIFIED_DECISIONS = frozenset(
    {
        RevocationPolicyDecision.DESTROY,
        RevocationPolicyDecision.RESTRICT,
    }
)


def _utc_now() -> _datetime.datetime:
    return _datetime.datetime.now(_datetime.timezone.utc)


def _utc(value: _datetime.datetime, name: str) -> _datetime.datetime:
    if (
        not isinstance(value, _datetime.datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(_datetime.timezone.utc)


def _utc_text(value: _datetime.datetime) -> str:
    return _utc(value, "timestamp").isoformat().replace("+00:00", "Z")


def _safe_token(value: str, name: str) -> str:
    if not isinstance(value, str) or _SAFE_TOKEN.fullmatch(value) is None:
        raise ValueError(f"{name} must be an opaque safe token")
    return value


def _controlled_optional_token(
    value: str | None,
    name: str,
    allowed: frozenset[str],
) -> str | None:
    if value is not None:
        _safe_token(value, name)
        if value not in allowed:
            raise ValueError(f"{name} must use the controlled registry")
    return value


def _nonnegative(value: int, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _coerce_stage(value: RevocationStage | str | None) -> RevocationStage | None:
    if value is None or isinstance(value, RevocationStage):
        return value
    if not isinstance(value, str):
        raise TypeError("status must be a RevocationStage, string, or None")
    try:
        return RevocationStage(value)
    except ValueError:
        raise ValueError("unsupported revocation status") from None


def _is_open(case: RevocationCase) -> bool:
    return case.stage not in _TERMINAL_STAGES


def _is_overdue(case: RevocationCase, now: _datetime.datetime) -> bool:
    if not _is_open(case):
        return False
    if case.stage is RevocationStage.OBSERVED and now > case.suppress_by:
        return True
    return now > case.verify_by


@_dataclasses.dataclass(frozen=True, slots=True)
class RevocationSummary:
    """Low-cardinality classification of current revocation case summaries.

    ``overdue`` overlaps the lifecycle buckets. ``open`` is the number of
    nonterminal cases, not a separate persisted counter.
    """

    observed: int = 0
    suppressed: int = 0
    verified: int = 0
    retained: int = 0
    failed: int = 0
    blocked: int = 0
    overdue: int = 0
    schema_version: int = _SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != _SCHEMA_VERSION:
            raise ValueError("unsupported revocation summary schema version")
        for field in _dataclasses.fields(self):
            if field.name != "schema_version":
                _nonnegative(getattr(self, field.name), field.name)

    @property
    def open(self) -> int:
        """Return the number of nonterminal revocation cases."""

        return self.observed + self.suppressed + self.failed + self.blocked

    @classmethod
    def from_cases(
        cls,
        cases: _typing.Iterable[RevocationCase],
        *,
        now: _datetime.datetime | None = None,
    ) -> "RevocationSummary":
        """Classify immutable case summaries without reading target content."""

        observed_at = _utc(now, "now") if now is not None else _utc_now()
        observed = suppressed = verified = retained = failed = blocked = overdue = 0
        for case in cases:
            if not isinstance(case, RevocationCase):
                raise TypeError("cases must contain RevocationCase values")
            if case.stage is RevocationStage.OBSERVED:
                observed += 1
            elif case.stage in _SUPPRESSED_STAGES:
                suppressed += 1
            elif case.stage is RevocationStage.VERIFIED or (
                case.stage is RevocationStage.CLOSED
                and case.policy_decision in _VERIFIED_DECISIONS
            ):
                verified += 1
            elif case.stage is RevocationStage.RETAINED_ISOLATED or (
                case.stage is RevocationStage.CLOSED
                and case.policy_decision is RevocationPolicyDecision.PRESERVE_ON_HOLD
            ):
                retained += 1
            elif case.stage is RevocationStage.FAILED:
                failed += 1
            elif case.stage is RevocationStage.BLOCKED:
                blocked += 1
            else:
                raise ValueError("revocation case has an unclassified lifecycle stage")
            if _is_overdue(case, observed_at):
                overdue += 1
        return cls(
            observed=observed,
            suppressed=suppressed,
            verified=verified,
            retained=retained,
            failed=failed,
            blocked=blocked,
            overdue=overdue,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the versioned JSON-safe summary."""

        return {
            "schema_version": self.schema_version,
            "observed": self.observed,
            "suppressed": self.suppressed,
            "verified": self.verified,
            "retained": self.retained,
            "failed": self.failed,
            "blocked": self.blocked,
            "overdue": self.overdue,
            "open": self.open,
        }


@_dataclasses.dataclass(frozen=True, slots=True)
class RevocationHealth:
    """Fail-closed startup view of ledger and serving-suppression state."""

    ready: bool
    summary: RevocationSummary
    open_case_ids: tuple[str, ...] = ()
    overdue_case_ids: tuple[str, ...] = ()
    unsafe_case_ids: tuple[str, ...] = ()
    safe_error_code: str | None = None
    schema_version: int = _SCHEMA_VERSION

    def __post_init__(self) -> None:
        if type(self.ready) is not bool:
            raise TypeError("ready must be a bool")
        if not isinstance(self.summary, RevocationSummary):
            raise TypeError("summary must be a RevocationSummary")
        if self.schema_version != _SCHEMA_VERSION:
            raise ValueError("unsupported revocation health schema version")
        for name in ("open_case_ids", "overdue_case_ids", "unsafe_case_ids"):
            values = getattr(self, name)
            if len(values) != len(set(values)) or values != tuple(sorted(values)):
                raise ValueError(f"{name} must be sorted and unique")
            for value in values:
                _safe_token(value, name)
        _controlled_optional_token(
            self.safe_error_code,
            "safe_error_code",
            _HEALTH_SAFE_ERROR_CODES,
        )
        if self.ready and (
            self.overdue_case_ids
            or self.unsafe_case_ids
            or self.safe_error_code is not None
        ):
            raise ValueError("ready health cannot contain a fail-closed condition")

    def to_dict(self) -> dict[str, object]:
        """Return versioned health metadata with no source or target locators."""

        return {
            "schema_version": self.schema_version,
            "ready": self.ready,
            "summary": self.summary.to_dict(),
            "open_case_ids": list(self.open_case_ids),
            "overdue_case_ids": list(self.overdue_case_ids),
            "unsafe_case_ids": list(self.unsafe_case_ids),
            "safe_error_code": self.safe_error_code,
        }


@_dataclasses.dataclass(frozen=True, slots=True)
class RevocationOperatorResult:
    """Metadata-only result of an explicitly configured operator action."""

    case_id: str
    operation: _typing.Literal["verify", "retry"]
    stage: RevocationStage
    mutated: bool
    attempt: int | None = None
    receipt_ids: tuple[str, ...] = ()
    safe_error_code: str | None = None
    schema_version: int = _SCHEMA_VERSION

    def __post_init__(self) -> None:
        _safe_token(self.case_id, "case_id")
        if self.operation not in {"verify", "retry"}:
            raise ValueError("operation must be verify or retry")
        if not isinstance(self.stage, RevocationStage):
            raise TypeError("stage must be a RevocationStage")
        if type(self.mutated) is not bool:
            raise TypeError("mutated must be a bool")
        if self.operation == "verify" and self.mutated:
            raise ValueError("verification is read-only")
        if self.attempt is not None:
            _nonnegative(self.attempt, "attempt")
        if self.receipt_ids != tuple(sorted(set(self.receipt_ids))):
            raise ValueError("receipt_ids must be sorted and unique")
        for receipt_id in self.receipt_ids:
            _safe_token(receipt_id, "receipt_id")
        _controlled_optional_token(
            self.safe_error_code,
            "safe_error_code",
            _OPERATOR_SAFE_ERROR_CODES,
        )
        if self.schema_version != _SCHEMA_VERSION:
            raise ValueError("unsupported operator-result schema version")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "case_id": self.case_id,
            "operation": self.operation,
            "stage": self.stage.value,
            "mutated": self.mutated,
            "attempt": self.attempt,
            "receipt_ids": list(self.receipt_ids),
            "safe_error_code": self.safe_error_code,
        }


@_dataclasses.dataclass(frozen=True, slots=True)
class RevocationScanResult:
    """Metadata-only result of an operator-owned target scan."""

    target_id: str
    scanned_count: int
    matching_count: int
    drift_count: int
    case_ids: tuple[str, ...] = ()
    safe_error_code: str | None = None
    schema_version: int = _SCHEMA_VERSION

    def __post_init__(self) -> None:
        _safe_token(self.target_id, "target_id")
        _nonnegative(self.scanned_count, "scanned_count")
        _nonnegative(self.matching_count, "matching_count")
        _nonnegative(self.drift_count, "drift_count")
        if self.matching_count > self.scanned_count:
            raise ValueError("matching_count cannot exceed scanned_count")
        if self.drift_count > self.scanned_count:
            raise ValueError("drift_count cannot exceed scanned_count")
        if self.case_ids != tuple(sorted(set(self.case_ids))):
            raise ValueError("case_ids must be sorted and unique")
        for case_id in self.case_ids:
            _safe_token(case_id, "case_id")
        _controlled_optional_token(
            self.safe_error_code,
            "safe_error_code",
            _OPERATOR_SAFE_ERROR_CODES,
        )
        if self.schema_version != _SCHEMA_VERSION:
            raise ValueError("unsupported scan-result schema version")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "target_id": self.target_id,
            "scanned_count": self.scanned_count,
            "matching_count": self.matching_count,
            "drift_count": self.drift_count,
            "case_ids": list(self.case_ids),
            "safe_error_code": self.safe_error_code,
        }


OperatorResult = RevocationOperatorResult
ScanResult = RevocationScanResult


@_typing.runtime_checkable
class RevocationOperator(_typing.Protocol):
    """Configured provider boundary used by mutating/read-back CLI commands.

    The ledger deliberately stores only digests. Implementations are responsible
    for reconstructing the protected provider, target instance, and locator.
    ``verify`` and ``scan`` must use provider credentials/capabilities that
    cannot mutate the destination; the generic CLI can prove only that its
    revocation control bytes remained unchanged.
    """

    async def verify(self, case_id: str) -> RevocationOperatorResult:
        """Read and verify the destination without mutating it."""

        ...

    async def retry(self, case_id: str) -> RevocationOperatorResult:
        """Perform an explicit external retry and record a new attempt."""

        ...

    async def scan(self, target_id: str) -> RevocationScanResult:
        """Inspect one configured destination for governed drift."""

        ...


class RevocationRepository:
    """Read-mostly public repository over the versioned revocation ledger."""

    def __init__(self, store: _state.StateStore) -> None:
        if not isinstance(store, _state.StateStore):
            raise TypeError("store must implement StateStore")
        self._store = store
        self._ledger = _ledger.StateStoreRevocationLedger(store)
        self._suppression = StateStoreSuppressionIndex(store)

    async def get(self, case_id: str) -> RevocationCase | None:
        """Return one validated case summary."""

        _safe_token(case_id, "case_id")
        return await self._ledger.get_case(case_id)

    async def list(
        self,
        *,
        status: RevocationStage | str | None = None,
    ) -> tuple[RevocationCase, ...]:
        """List validated cases, optionally at one exact lifecycle stage."""

        return await self._ledger.list_cases(stage=_coerce_stage(status))

    async def receipts(self, case_id: str) -> tuple[RevocationReceipt, ...]:
        """Return the validated receipt chain for one case."""

        _safe_token(case_id, "case_id")
        return await self._ledger.list_receipts(case_id)

    async def repair(self) -> LedgerRepairReport:
        """Rebuild mutable projections from immutable ledger events."""

        return await self._ledger.repair()

    async def summary(
        self,
        *,
        now: _datetime.datetime | None = None,
    ) -> RevocationSummary:
        """Return lifecycle counts from validated case summaries."""

        return RevocationSummary.from_cases(await self._ledger.list_cases(), now=now)

    async def startup_health(
        self,
        *,
        now: _datetime.datetime | None = None,
    ) -> RevocationHealth:
        """Validate strict startup state without claiming provider capabilities.

        Open cases are safe to start only when their exact suppression generation
        is durably present. Corrupt or unavailable control state always returns
        ``ready=False`` with a controlled error code.
        """

        observed_at = _utc(now, "now") if now is not None else _utc_now()
        try:
            cases = await self._ledger.list_cases()
            records = await self._suppression.records()
        except (
            _ledger.RevocationLedgerError,
            _model.RevocationSchemaError,
            _suppression.SuppressionCorruptionError,
            _state.StateDecryptionError,
        ):
            return RevocationHealth(
                ready=False,
                summary=RevocationSummary(),
                safe_error_code="revocation.state_corrupt",
            )
        except Exception:
            return RevocationHealth(
                ready=False,
                summary=RevocationSummary(),
                safe_error_code="revocation.state_unavailable",
            )

        summary = RevocationSummary.from_cases(cases, now=observed_at)
        open_cases = tuple(case for case in cases if _is_open(case))
        open_case_ids = tuple(sorted(case.case_id for case in open_cases))
        overdue_case_ids = tuple(
            sorted(
                case.case_id for case in open_cases if _is_overdue(case, observed_at)
            )
        )
        records_by_source = {record.source_digest: record for record in records}
        unsafe_case_ids = tuple(
            sorted(
                case.case_id
                for case in open_cases
                if not self._suppression_matches(
                    case,
                    records_by_source.get(case.source_digest),
                )
            )
        )
        if overdue_case_ids:
            safe_error_code = "revocation.deadline_overdue"
        elif unsafe_case_ids:
            safe_error_code = "revocation.suppression_unconfirmed"
        else:
            safe_error_code = None
        return RevocationHealth(
            ready=not overdue_case_ids and not unsafe_case_ids,
            summary=summary,
            open_case_ids=open_case_ids,
            overdue_case_ids=overdue_case_ids,
            unsafe_case_ids=unsafe_case_ids,
            safe_error_code=safe_error_code,
        )

    @staticmethod
    def _suppression_matches(
        case: RevocationCase,
        record: SuppressionRecord | None,
    ) -> bool:
        return (
            record is not None
            and record.source_digest == case.source_digest
            and record.tenant_digest == case.tenant_digest
            and record.policy_id == case.policy_id
            and record.generation == case.suppression_generation
            and record.suppressed
            and not record.verified_authorization
            and record.policy_revision == case.policy_revision
            and record.group_graph_revision == case.group_graph_revision
            and record.reason == case.reason.value
            and record.case_id == case.case_id
        )

    def case_metadata(self, case: RevocationCase) -> dict[str, object]:
        """Project a case for operator output without its raw source revision."""

        if not isinstance(case, RevocationCase):
            raise TypeError("case must be a RevocationCase")
        return case.__synor_audit_metadata__()

    def receipt_metadata(self, receipt: RevocationReceipt) -> dict[str, object]:
        """Project a receipt while omitting its provider operation identifier."""

        if not isinstance(receipt, RevocationReceipt):
            raise TypeError("receipt must be a RevocationReceipt")
        return receipt.__synor_audit_metadata__()


class RevocationController:
    """Advanced strict coordinator for governed source and target connectors.

    The controller remains single-process and single-event-loop, matching the
    underlying Phase 2 durability contract. Provider reconstruction across a
    process restart remains an operator/connector responsibility.
    """

    def __init__(
        self,
        *,
        state_store: _state.StateStore,
        policy: RevocationPolicy,
        clock: _runtime.Clock | None = None,
        boundary_hook: _runtime.BoundaryHook | None = None,
    ) -> None:
        if not isinstance(state_store, _state.StateStore):
            raise TypeError("state_store must implement StateStore")
        if not isinstance(policy, RevocationPolicy):
            raise TypeError("policy must be a RevocationPolicy")
        self._store = state_store
        self._ledger = _ledger.StateStoreRevocationLedger(state_store)
        self._suppression = StateStoreSuppressionIndex(state_store)
        runtime_options: dict[str, object] = {}
        if clock is not None:
            runtime_options["clock"] = clock
        if boundary_hook is not None:
            runtime_options["boundary_hook"] = boundary_hook
        self._runtime = _runtime.RevocationRuntime(
            ledger=self._ledger,
            suppression=self._suppression,
            policy=policy,
            **_typing.cast(dict[str, _typing.Any], runtime_options),
        )
        self._repository = RevocationRepository(state_store)
        self._controlled_case_ids: set[str] | None = None
        self._pending_finalization: set[str] = set()

    @property
    def repository(self) -> RevocationRepository:
        return self._repository

    @property
    def suppression_lookup(self) -> StateStoreSuppressionIndex:
        """Return the fail-closed lookup used by guarded query integrations."""

        return self._suppression

    @property
    def suppression(self) -> StateStoreSuppressionIndex:
        """Alias for :attr:`suppression_lookup` used by connector adapters."""

        return self._suppression

    def begin_controlled_run(self) -> None:
        """Reset case tracking immediately before one controlled app run."""

        self._controlled_case_ids = set()
        self._pending_finalization.clear()

    def pending_finalization_case_ids(self) -> tuple[str, ...]:
        """Return cases proved terminal during the current controlled run."""

        return tuple(sorted(self._pending_finalization))

    def _track_case(self, case: RevocationCase) -> None:
        if self._controlled_case_ids is not None:
            self._controlled_case_ids.add(case.case_id)

    def _track_terminal(self, case: RevocationCase) -> None:
        if (
            self._controlled_case_ids is not None
            and case.case_id in self._controlled_case_ids
            and case.stage
            in {RevocationStage.VERIFIED, RevocationStage.RETAINED_ISOLATED}
        ):
            self._pending_finalization.add(case.case_id)

    async def begin_case(self, request: RevocationRequest) -> RevocationCase:
        case = await self._runtime.begin_case(request)
        self._track_case(case)
        # A prior process/run may have persisted the terminal target
        # postcondition but stopped before Synor's engine commit could close the
        # case. Re-register it for post-commit finalization on the next
        # successful controlled run.
        self._track_terminal(case)
        return case

    async def prepare_retry(self, request: RevocationRequest) -> RevocationCase:
        case = await self._runtime.prepare_retry(request)
        self._track_case(case)
        return case

    async def descriptors_for(self, case_id: str) -> tuple[EffectDescriptor, ...]:
        return await self._runtime.descriptors_for(case_id)

    async def assert_action_fence(self, case_id: str, obligation_id: str) -> None:
        await self._runtime.assert_action_fence(case_id, obligation_id)

    async def get_case(self, case_id: str) -> RevocationCase | None:
        return await self._runtime.get_case(case_id)

    async def list_receipts(self, case_id: str) -> tuple[RevocationReceipt, ...]:
        return await self._ledger.list_receipts(case_id)

    async def notify_synor_precommit(self, case_id: str, obligation_id: str) -> None:
        await self._runtime.notify_synor_precommit(case_id, obligation_id)

    async def notify_target_effect_applied(
        self, case_id: str, obligation_id: str
    ) -> None:
        await self._runtime.notify_target_effect_applied(case_id, obligation_id)

    async def mark_target_applied(
        self, case_id: str, obligation_id: str
    ) -> RevocationCase:
        return await self._runtime.mark_target_applied(case_id, obligation_id)

    async def mark_acknowledged(
        self, case_id: str, obligation_id: str
    ) -> RevocationCase:
        return await self._runtime.mark_acknowledged(case_id, obligation_id)

    def outcome_recorder(
        self,
        case_id: str,
        *,
        attempt: int,
        attempted_at: _datetime.datetime | None = None,
    ) -> _typing.Callable[
        [
            _context_keys.ContextProvider,
            _typing.Sequence[TargetVerificationOutcome],
        ],
        _typing.Awaitable[None],
    ]:
        async def record(
            context_provider: _context_keys.ContextProvider,
            outcomes: _typing.Sequence[TargetVerificationOutcome],
            /,
        ) -> None:
            del context_provider
            await self.record_outcomes(
                case_id,
                outcomes,
                attempt=attempt,
                attempted_at=attempted_at,
            )

        return record

    async def record_outcomes(
        self,
        case_id: str,
        outcomes: _typing.Sequence[TargetVerificationOutcome],
        *,
        attempt: int,
        attempted_at: _datetime.datetime | None = None,
    ) -> RevocationCase:
        case = await self._runtime.record_outcomes(
            case_id,
            outcomes,
            attempt=attempt,
            attempted_at=attempted_at,
        )
        self._track_terminal(case)
        return case

    async def finalize_after_engine_commit(self, case_id: str) -> RevocationCase:
        case = await self._runtime.finalize_after_engine_commit(case_id)
        self._pending_finalization.discard(case_id)
        return case

    async def record_verified_reauthorization(
        self,
        request: RevocationRequest,
        *,
        generation: int,
        policy_id: str,
        policy_revision: str,
        group_graph_revision: str,
        observed_at: _datetime.datetime | None = None,
    ) -> None:
        await self._runtime.record_verified_reauthorization(
            request,
            generation=generation,
            policy_id=policy_id,
            policy_revision=policy_revision,
            group_graph_revision=group_graph_revision,
            observed_at=observed_at,
        )

    async def authorize_generation(
        self,
        *,
        source_digest: str,
        tenant_digest: str,
        policy_id: str,
        generation: int,
        policy_revision: str,
        group_graph_revision: str,
        observed_at: _datetime.datetime | None = None,
    ) -> SuppressionRecord:
        """Record one verified source generation for guarded retrieval."""

        return await self._suppression.authorize(
            source_digest=source_digest,
            tenant_digest=tenant_digest,
            policy_id=policy_id,
            generation=generation,
            policy_revision=policy_revision,
            group_graph_revision=group_graph_revision,
            observed_at=observed_at,
        )
