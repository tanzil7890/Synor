"""Internal coordinator for the synthetic strict-revocation vertical slice.

The runtime composes the existing ledger, suppression index, verified sink,
and target capability contracts.  It deliberately does not alter Synor's core
reconciliation protocol: connector callbacks notify this coordinator at the
same boundaries at which they already apply and verify target actions.

This first implementation is single-process and single-event-loop. Durability
comes from the event-first :class:`RevocationLedger` and monotonic
:class:`SuppressionIndex`; external target actions must remain idempotent under
their stable action ID.
"""

from __future__ import annotations

import asyncio
import datetime
import enum
import hashlib
import inspect
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass

from .context_keys import ContextProvider
from .revocation_ledger import RevocationLedger
from .revocation_model import (
    AccessSnapshot,
    AssuranceLevel,
    EffectDescriptor,
    EffectOperation,
    RevocationCase,
    RevocationPolicyDecision,
    RevocationReceipt,
    RevocationStage,
    SafeRevocationErrorCode,
    SnapshotResult,
    SourceEventKind,
    SourceIdentity,
    TargetRevocationCapabilities,
    VerificationOutcome,
    make_action_id,
    make_case_id,
    make_observation_id,
    make_proof_contract_digest,
    make_receipt_id,
    make_tenant_digest,
    transition_case,
    verification_outcome_error_code,
)
from .revocation_policy import RevocationCapabilityError, RevocationPolicy
from .suppression import StateStoreSuppressionIndex, SuppressionRecord
from .verified_sink import TargetVerificationOutcome


_HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
_LEGAL_HOLD_STATE = "legal_hold"
_BLOCKABLE_MATERIALIZATION_STAGES = frozenset(
    {
        RevocationStage.OBSERVED,
        RevocationStage.SUPPRESSED,
        RevocationStage.PLANNED,
        RevocationStage.DISPATCHED,
        RevocationStage.ACKNOWLEDGED,
        RevocationStage.FENCE_REACHED,
        RevocationStage.FAILED,
    }
)
_DESCRIPTOR_STAGES = frozenset(
    {
        RevocationStage.PLANNED,
        RevocationStage.DISPATCHED,
        RevocationStage.ACKNOWLEDGED,
        RevocationStage.FENCE_REACHED,
        RevocationStage.VERIFIED,
        RevocationStage.RETAINED_ISOLATED,
    }
)


class RevocationRuntimeError(RuntimeError):
    """Base error for invalid strict-runtime coordination."""


class RevocationRuntimeStateError(RevocationRuntimeError):
    """The requested callback does not match the persisted lifecycle stage."""


class RevocationRuntimeProtocolError(RevocationRuntimeError):
    """A verified-sink result does not match a registered obligation."""


class LifecycleBoundary(str, enum.Enum):
    """Named fault-injection boundaries in the Phase 2 lifecycle."""

    OBSERVATION_PERSISTED = "observation_persisted"
    SUPPRESSION_PERSISTED = "suppression_persisted"
    SYNOR_PRECOMMIT = "synor_precommit"
    TARGET_APPLIED = "target_applied"
    ACKNOWLEDGEMENT_PERSISTED = "acknowledgement_persisted"
    VERIFICATION_COMPLETED = "verification_completed"
    ENGINE_FINAL_COMMIT = "engine_final_commit"
    RECEIPT_APPENDED = "receipt_appended"
    CASE_SUMMARY_UPDATED = "case_summary_updated"


BoundaryHook = Callable[
    [LifecycleBoundary, str, str | None],
    Awaitable[None] | None,
]
Clock = Callable[[], datetime.datetime]


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _utc(value: datetime.datetime, name: str) -> datetime.datetime:
    if (
        not isinstance(value, datetime.datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(datetime.timezone.utc)


def _digest(value: str, name: str) -> str:
    if not isinstance(value, str) or _HEX_DIGEST.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _token(value: str, name: str) -> str:
    if not isinstance(value, str) or _SAFE_TOKEN.fullmatch(value) is None:
        raise ValueError(f"{name} must be an opaque safe token")
    return value


def _request_fingerprint(
    case_id: str,
    obligation_id: str,
    attempt: int,
) -> str:
    payload = (
        b"synor-revocation-request-v1\x00"
        + len(case_id).to_bytes(8, "big")
        + case_id.encode()
        + len(obligation_id).to_bytes(8, "big")
        + obligation_id.encode()
        + attempt.to_bytes(8, "big")
    )
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class TargetObligation:
    """Target identity, proof contract, and current provider capability.

    ``capabilities=None`` represents a target provider registration that is
    currently unavailable.  Its stable identity still remains in the case, so
    cleanup is blocked rather than silently forgotten and can resume when the
    provider returns.
    """

    target_provider_id: str
    target_instance_digest: str
    target_locator_digest: str
    operation_kind: EffectOperation
    proof_capabilities: TargetRevocationCapabilities
    capabilities: TargetRevocationCapabilities | None
    verifier_kind: str
    consistency_contract: str

    def __post_init__(self) -> None:
        _token(self.target_provider_id, "target_provider_id")
        _digest(self.target_instance_digest, "target_instance_digest")
        _digest(self.target_locator_digest, "target_locator_digest")
        if not isinstance(self.operation_kind, EffectOperation):
            raise TypeError("operation_kind must be an EffectOperation")
        if not isinstance(self.proof_capabilities, TargetRevocationCapabilities):
            raise TypeError("proof_capabilities must be TargetRevocationCapabilities")
        if self.capabilities is not None and not isinstance(
            self.capabilities, TargetRevocationCapabilities
        ):
            raise TypeError("capabilities must be TargetRevocationCapabilities or None")
        _token(self.verifier_kind, "verifier_kind")
        _token(self.consistency_contract, "consistency_contract")

    @property
    def proof_contract_digest(self) -> str:
        return make_proof_contract_digest(
            self.verifier_kind,
            self.consistency_contract,
            self.proof_capabilities.contract_digest(),
        )

    def action_id(self, case_id: str) -> str:
        return make_action_id(
            case_id,
            self.target_provider_id,
            self.target_instance_digest,
            self.target_locator_digest,
            self.operation_kind,
            self.proof_contract_digest,
        )

    def descriptor(self, case: RevocationCase) -> EffectDescriptor:
        return EffectDescriptor(
            operation_kind=self.operation_kind,
            action_id=self.action_id(case.case_id),
            source_digest=case.source_digest,
            source_generation=case.suppression_generation,
            target_locator_digest=self.target_locator_digest,
        )


@dataclass(frozen=True, slots=True)
class RevocationRequest:
    """Immutable inputs needed to create or resume one revocation case."""

    identity: SourceIdentity
    observation_id: str
    source_revision: str
    access: AccessSnapshot
    observation_generation: str | None
    tenant_digest: str
    policy_id: str
    policy_revision: str
    policy_digest: str
    group_graph_revision: str
    reason: SourceEventKind
    policy_decision: RevocationPolicyDecision
    suppression_generation: int
    observed_at: datetime.datetime
    suppress_by: datetime.datetime
    verify_by: datetime.datetime
    obligations: tuple[TargetObligation, ...]
    snapshot: SnapshotResult | None = None
    require_complete_snapshot: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.identity, SourceIdentity):
            raise TypeError("identity must be a SourceIdentity")
        _token(self.observation_id, "observation_id")
        if not isinstance(self.source_revision, str) or not self.source_revision:
            raise ValueError("source_revision must be non-empty")
        if not isinstance(self.access, AccessSnapshot):
            raise TypeError("access must be an AccessSnapshot")
        if self.observation_generation is not None:
            _token(self.observation_generation, "observation_generation")
        _digest(self.tenant_digest, "tenant_digest")
        _token(self.policy_id, "policy_id")
        _token(self.policy_revision, "policy_revision")
        _digest(self.policy_digest, "policy_digest")
        _token(self.group_graph_revision, "group_graph_revision")
        if not isinstance(self.reason, SourceEventKind):
            raise TypeError("reason must be a SourceEventKind")
        if not isinstance(self.policy_decision, RevocationPolicyDecision):
            raise TypeError("policy_decision must be a RevocationPolicyDecision")
        if (
            self.tenant_digest != make_tenant_digest(self.access.tenant_id)
            or self.policy_id != self.access.policy_id
            or self.policy_revision != self.access.policy_revision
            or self.policy_digest != self.access.policy_digest
            or self.group_graph_revision != self.access.group_graph_revision
        ):
            raise ValueError(
                "revocation governance fields must match the access snapshot"
            )
        expected_observation_id = make_observation_id(
            self.identity,
            self.source_revision,
            self.reason,
            self.access,
            observation_generation=self.observation_generation,
        )
        if self.observation_id != expected_observation_id:
            raise ValueError(
                "observation_id must bind the complete governed observation"
            )
        if (
            not isinstance(self.suppression_generation, int)
            or isinstance(self.suppression_generation, bool)
            or self.suppression_generation < 1
        ):
            raise ValueError("suppression_generation must be positive")
        observed_at = _utc(self.observed_at, "observed_at")
        suppress_by = _utc(self.suppress_by, "suppress_by")
        verify_by = _utc(self.verify_by, "verify_by")
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "suppress_by", suppress_by)
        object.__setattr__(self, "verify_by", verify_by)
        if suppress_by < observed_at or verify_by < suppress_by:
            raise ValueError("revocation deadlines must be ordered")
        if not self.obligations:
            raise ValueError("strict revocation requires at least one obligation")
        if not all(
            isinstance(obligation, TargetObligation) for obligation in self.obligations
        ):
            raise TypeError("obligations must contain TargetObligation values")
        identities = {
            (
                obligation.target_provider_id,
                obligation.target_instance_digest,
                obligation.target_locator_digest,
                obligation.operation_kind,
            )
            for obligation in self.obligations
        }
        if len(identities) != len(self.obligations):
            raise ValueError("target obligations must have unique identities")
        if self.snapshot is not None:
            if not isinstance(self.snapshot, SnapshotResult):
                raise TypeError("snapshot must be SnapshotResult or None")
            if (
                self.snapshot.connector_instance_id
                != self.identity.connector_instance_id
                or self.snapshot.source_scope_id != self.identity.source_scope_id
            ):
                raise ValueError(
                    "snapshot authority must match the source identity scope"
                )
        if type(self.require_complete_snapshot) is not bool:
            raise TypeError("require_complete_snapshot must be a bool")

    @property
    def case_id(self) -> str:
        return make_case_id(
            self.identity,
            self.source_revision,
            self.reason,
            self.policy_decision,
            self.observation_id,
        )

    @property
    def expected_obligation_ids(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                obligation.action_id(self.case_id) for obligation in self.obligations
            )
        )

    def observed_case(self) -> RevocationCase:
        return RevocationCase(
            case_id=self.case_id,
            observation_id=self.observation_id,
            source_digest=self.identity.evidence_digest(),
            source_revision=self.source_revision,
            tenant_digest=self.tenant_digest,
            policy_id=self.policy_id,
            policy_revision=self.policy_revision,
            policy_digest=self.policy_digest,
            group_graph_revision=self.group_graph_revision,
            legal_state=self.access.legal_state,
            suppression_generation=self.suppression_generation,
            reason=self.reason,
            policy_decision=self.policy_decision,
            stage=RevocationStage.OBSERVED,
            observed_at=self.observed_at,
            suppress_by=self.suppress_by,
            verify_by=self.verify_by,
            expected_targets=self.expected_obligation_ids,
            version=1,
        )


class RevocationRuntime:
    """Coordinate one-process, one-event-loop revocation without changing ABI."""

    def __init__(
        self,
        *,
        ledger: RevocationLedger,
        suppression: StateStoreSuppressionIndex,
        policy: RevocationPolicy,
        boundary_hook: BoundaryHook | None = None,
        clock: Clock = _utc_now,
    ) -> None:
        if not policy.is_strict:
            raise ValueError("RevocationRuntime requires a strict policy")
        self._ledger = ledger
        self._suppression = suppression
        self._policy = policy
        self._boundary_hook = boundary_hook
        self._clock = clock
        self._lock = asyncio.Lock()
        self._obligations: dict[str, dict[str, TargetObligation]] = {}

    async def _boundary(
        self,
        boundary: LifecycleBoundary,
        case_id: str,
        obligation_id: str | None = None,
    ) -> None:
        hook = self._boundary_hook
        if hook is None:
            return
        result = hook(boundary, case_id, obligation_id)
        if inspect.isawaitable(result):
            await result

    def _remember_request(self, request: RevocationRequest) -> None:
        case_id = request.case_id
        obligations = {
            obligation.action_id(case_id): obligation
            for obligation in request.obligations
        }
        self._obligations[case_id] = obligations

    async def _case(self, case_id: str) -> RevocationCase:
        case = await self._ledger.get_case(case_id)
        if case is None:
            raise RevocationRuntimeStateError("revocation case is not persisted")
        return case

    async def _append_transition(
        self,
        case: RevocationCase,
        stage: RevocationStage,
        *,
        safe_error_code: SafeRevocationErrorCode | None = None,
    ) -> RevocationCase:
        transitioned = transition_case(
            case,
            stage,
            safe_error_code=(
                safe_error_code.value if safe_error_code is not None else None
            ),
        )
        await self._ledger.append_case(transitioned)
        return transitioned

    def _registered_obligations(
        self,
        case: RevocationCase,
    ) -> tuple[TargetObligation, ...]:
        try:
            registrations = self._obligations[case.case_id]
        except KeyError as error:
            raise RevocationRuntimeProtocolError(
                "target obligations are not registered"
            ) from error
        if set(registrations) != set(case.expected_targets):
            raise RevocationRuntimeProtocolError(
                "registered obligations do not match the durable case"
            )
        return tuple(
            registrations[obligation_id] for obligation_id in case.expected_targets
        )

    def _validate_obligations(
        self,
        case: RevocationCase,
        obligations: Sequence[TargetObligation],
    ) -> SafeRevocationErrorCode | None:
        expected_decision = self._policy.decide(
            case.reason,
            legal_hold=case.legal_state == _LEGAL_HOLD_STATE,
        )
        if expected_decision is None or expected_decision is not case.policy_decision:
            return SafeRevocationErrorCode.POLICY_BLOCKED

        allowed_operations: Mapping[
            RevocationPolicyDecision, frozenset[EffectOperation]
        ] = {
            RevocationPolicyDecision.DESTROY: frozenset({EffectOperation.DELETE}),
            # Phase 2 safely implements ACL narrowing as verified purge.  An
            # in-place ACL rewrite remains blocked until a principal-aware
            # read-back outcome and connector capability are defined.
            RevocationPolicyDecision.RESTRICT: frozenset({EffectOperation.DELETE}),
            RevocationPolicyDecision.PRESERVE_ON_HOLD: frozenset(
                {EffectOperation.ISOLATE}
            ),
            RevocationPolicyDecision.INVESTIGATE_AMBIGUOUS: frozenset(),
        }
        for obligation in obligations:
            if obligation.capabilities is None:
                return SafeRevocationErrorCode.PROVIDER_MISSING
            if obligation.capabilities != obligation.proof_capabilities:
                return SafeRevocationErrorCode.CAPABILITY_UNSUPPORTED
            if (
                obligation.operation_kind
                not in allowed_operations[case.policy_decision]
            ):
                return SafeRevocationErrorCode.POLICY_BLOCKED
            try:
                self._policy.validate_capabilities(
                    obligation.capabilities,
                    decision=case.policy_decision,
                )
            except RevocationCapabilityError:
                return SafeRevocationErrorCode.CAPABILITY_UNSUPPORTED
        return None

    def _requires_serving_suppression(self, case: RevocationCase) -> bool:
        """Return whether the governed observation must fail closed at query time."""

        return (
            self._policy.decide(
                case.reason,
                legal_hold=case.legal_state == _LEGAL_HOLD_STATE,
            )
            is not None
        )

    def _request_requires_serving_suppression(
        self,
        request: RevocationRequest,
    ) -> bool:
        return (
            self._policy.decide(
                request.reason,
                legal_hold=request.access.legal_state == _LEGAL_HOLD_STATE,
            )
            is not None
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

    async def _write_active_suppression(
        self,
        case: RevocationCase,
    ) -> bool:
        record = await self._suppression.suppress(
            source_digest=case.source_digest,
            tenant_digest=case.tenant_digest,
            policy_id=case.policy_id,
            generation=case.suppression_generation,
            policy_revision=case.policy_revision,
            group_graph_revision=case.group_graph_revision,
            reason=case.reason.value,
            case_id=case.case_id,
            observed_at=case.observed_at,
        )
        return self._suppression_matches(case, record)

    async def _has_active_suppression(
        self,
        case: RevocationCase,
    ) -> bool:
        record = await self._suppression.get(case.source_digest)
        return self._suppression_matches(case, record)

    async def _reconcile_serving_fence(self, case: RevocationCase) -> None:
        """Release an emergency fence only through non-conflicting durable state."""

        current = await self._suppression.durable_state(case.source_digest)
        if current is None:
            return
        if self._suppression_matches(case, current) or (
            current.generation > case.suppression_generation
        ):
            await self._suppression.clear_fail_closed(
                case.source_digest,
            )

    async def _block_case(
        self,
        case: RevocationCase,
        code: SafeRevocationErrorCode,
    ) -> RevocationCase:
        if case.stage is RevocationStage.BLOCKED:
            return case
        if case.stage not in _BLOCKABLE_MATERIALIZATION_STAGES:
            return case
        return await self._append_transition(
            case,
            RevocationStage.BLOCKED,
            safe_error_code=code,
        )

    async def _block_suppressed_case(
        self,
        case: RevocationCase,
        code: SafeRevocationErrorCode,
    ) -> RevocationCase:
        """Record the current blocker after serving suppression is confirmed."""

        if case.stage is RevocationStage.BLOCKED:
            if case.safe_error_code == code.value:
                return case
            case = await self._append_transition(
                case,
                RevocationStage.SUPPRESSED,
            )
            await self._boundary(
                LifecycleBoundary.SUPPRESSION_PERSISTED,
                case.case_id,
            )
        return await self._block_case(case, code)

    @staticmethod
    def _request_matches_case(
        request: RevocationRequest,
        case: RevocationCase,
    ) -> bool:
        observed = request.observed_case()
        return (
            case.observation_id == observed.observation_id
            and case.source_digest == observed.source_digest
            and case.source_revision == observed.source_revision
            and case.tenant_digest == observed.tenant_digest
            and case.policy_id == observed.policy_id
            and case.policy_revision == observed.policy_revision
            and case.policy_digest == observed.policy_digest
            and case.group_graph_revision == observed.group_graph_revision
            and case.legal_state == observed.legal_state
            and case.suppression_generation == observed.suppression_generation
            and case.reason is observed.reason
            and case.policy_decision is observed.policy_decision
            and case.expected_targets == observed.expected_targets
        )

    async def begin_case(self, request: RevocationRequest) -> RevocationCase:
        """Persist observation and suppression, then produce a strict plan.

        Repeating this method after a crash is idempotent.  A partial required
        snapshot or unavailable/unsupported target is represented as a durable
        blocked case, never as successful cleanup.
        """

        if self._request_requires_serving_suppression(request):
            self._suppression.fail_closed(
                request.identity.evidence_digest(),
                request.suppression_generation,
            )
            await self._suppression.persist_fail_closed(
                source_digest=request.identity.evidence_digest(),
                tenant_digest=request.tenant_digest,
                policy_id=request.policy_id,
                generation=request.suppression_generation,
                policy_revision=request.policy_revision,
                group_graph_revision=request.group_graph_revision,
                reason=request.reason.value,
                case_id=request.case_id,
                observed_at=request.observed_at,
            )
        self._remember_request(request)
        async with self._lock:
            case = await self._ledger.get_case(request.case_id)
            observed = request.observed_case()
            if case is None:
                await self._ledger.append_case(observed)
                case = observed
                await self._boundary(
                    LifecycleBoundary.OBSERVATION_PERSISTED,
                    case.case_id,
                )
            elif not self._request_matches_case(request, case):
                raise RevocationRuntimeStateError(
                    "persisted revocation case does not match the request"
                )

            if case.stage in {
                RevocationStage.CLOSED,
                RevocationStage.VERIFIED,
                RevocationStage.RETAINED_ISOLATED,
                RevocationStage.FAILED,
            }:
                await self._reconcile_serving_fence(case)
                return case
            await self._reconcile_serving_fence(case)

            obligations = self._registered_obligations(case)
            blocked_reason = self._validate_obligations(case, obligations)
            if (
                blocked_reason is SafeRevocationErrorCode.POLICY_BLOCKED
                and not self._requires_serving_suppression(case)
            ):
                return await self._block_case(case, blocked_reason)

            snapshot_incomplete = request.require_complete_snapshot and (
                request.snapshot is None
                or not request.snapshot.authorizes_missing_item_cleanup
            )
            if snapshot_incomplete:
                if self._requires_serving_suppression(case):
                    if not await self._write_active_suppression(case):
                        return await self._block_case(
                            case,
                            SafeRevocationErrorCode.POLICY_BLOCKED,
                        )
                    if case.stage is RevocationStage.OBSERVED:
                        case = await self._append_transition(
                            case,
                            RevocationStage.SUPPRESSED,
                        )
                        await self._boundary(
                            LifecycleBoundary.SUPPRESSION_PERSISTED,
                            case.case_id,
                        )
                if case.stage is not RevocationStage.BLOCKED:
                    case = await self._block_case(
                        case,
                        SafeRevocationErrorCode.SNAPSHOT_INCOMPLETE,
                    )
                elif self._requires_serving_suppression(case):
                    case = await self._block_suppressed_case(
                        case,
                        SafeRevocationErrorCode.SNAPSHOT_INCOMPLETE,
                    )
                return case

            if case.stage in {
                RevocationStage.PLANNED,
                RevocationStage.DISPATCHED,
                RevocationStage.ACKNOWLEDGED,
                RevocationStage.FENCE_REACHED,
            }:
                if blocked_reason is not None:
                    if (
                        blocked_reason is SafeRevocationErrorCode.POLICY_BLOCKED
                        and not await self._write_active_suppression(case)
                    ):
                        return await self._block_case(
                            case,
                            SafeRevocationErrorCode.POLICY_BLOCKED,
                        )
                    return await self._block_case(case, blocked_reason)
                if not await self._has_active_suppression(case):
                    return await self._block_case(
                        case,
                        SafeRevocationErrorCode.POLICY_BLOCKED,
                    )
                return case

            if case.stage in {
                RevocationStage.BLOCKED,
                RevocationStage.OBSERVED,
            }:
                if not await self._write_active_suppression(case):
                    return await self._block_case(
                        case,
                        SafeRevocationErrorCode.POLICY_BLOCKED,
                    )
                case = await self._append_transition(
                    case,
                    RevocationStage.SUPPRESSED,
                )
                await self._boundary(
                    LifecycleBoundary.SUPPRESSION_PERSISTED,
                    case.case_id,
                )

            if blocked_reason is not None:
                case = await self._append_transition(
                    case,
                    RevocationStage.BLOCKED,
                    safe_error_code=blocked_reason,
                )
                return case

            if case.stage is RevocationStage.SUPPRESSED:
                case = await self._append_transition(case, RevocationStage.PLANNED)
            return case

    async def prepare_retry(self, request: RevocationRequest) -> RevocationCase:
        """Validate current providers and return an open case to ``PLANNED``."""

        if self._request_requires_serving_suppression(request):
            self._suppression.fail_closed(
                request.identity.evidence_digest(),
                request.suppression_generation,
            )
        self._remember_request(request)
        async with self._lock:
            case = await self._case(request.case_id)
            if not self._request_matches_case(request, case):
                raise RevocationRuntimeStateError(
                    "persisted revocation case does not match the request"
                )
            # ``fail_closed`` is installed before the first await. Reconcile it
            # against durable state before any stage validation can return or
            # raise, otherwise an invalid retry of an old terminal case could
            # hide a newer verified authorization for the rest of the process.
            await self._reconcile_serving_fence(case)
            if case.stage is RevocationStage.CLOSED:
                raise RevocationRuntimeStateError("a closed case cannot be retried")
            if case.stage not in {
                RevocationStage.PLANNED,
                RevocationStage.FAILED,
                RevocationStage.BLOCKED,
            }:
                raise RevocationRuntimeStateError(
                    "only planned, failed, or blocked cases can be prepared for retry"
                )
            if request.require_complete_snapshot and (
                request.snapshot is None
                or not request.snapshot.authorizes_missing_item_cleanup
            ):
                if self._requires_serving_suppression(case) and not (
                    await self._write_active_suppression(case)
                ):
                    return await self._block_case(
                        case,
                        SafeRevocationErrorCode.POLICY_BLOCKED,
                    )
                if case.stage is not RevocationStage.BLOCKED:
                    case = await self._block_case(
                        case,
                        SafeRevocationErrorCode.SNAPSHOT_INCOMPLETE,
                    )
                elif self._requires_serving_suppression(case):
                    case = await self._block_suppressed_case(
                        case,
                        SafeRevocationErrorCode.SNAPSHOT_INCOMPLETE,
                    )
                return case

            obligations = self._registered_obligations(case)
            blocked_reason = self._validate_obligations(case, obligations)
            if (
                blocked_reason is SafeRevocationErrorCode.POLICY_BLOCKED
                and not self._requires_serving_suppression(case)
            ):
                return await self._block_case(case, blocked_reason)

            if not await self._write_active_suppression(case):
                return await self._block_case(
                    case,
                    SafeRevocationErrorCode.POLICY_BLOCKED,
                )
            if blocked_reason is not None:
                return await self._block_suppressed_case(case, blocked_reason)

            if case.stage is RevocationStage.PLANNED:
                return case
            if case.stage is RevocationStage.BLOCKED:
                case = await self._append_transition(
                    case,
                    RevocationStage.SUPPRESSED,
                )
                await self._boundary(
                    LifecycleBoundary.SUPPRESSION_PERSISTED,
                    case.case_id,
                )
            case = await self._append_transition(case, RevocationStage.PLANNED)
            return case

    async def descriptors_for(self, case_id: str) -> tuple[EffectDescriptor, ...]:
        async with self._lock:
            case = await self._case(case_id)
            if case.stage not in _DESCRIPTOR_STAGES:
                raise RevocationRuntimeStateError(
                    "target actions cannot materialize in the current case stage"
                )
            obligations = self._registered_obligations(case)
            blocked_reason = self._validate_obligations(case, obligations)
            if blocked_reason is not None:
                case = await self._block_case(case, blocked_reason)
                raise RevocationRuntimeStateError(
                    "current target registration cannot satisfy strict policy"
                )
            if not await self._has_active_suppression(case):
                await self._block_case(
                    case,
                    SafeRevocationErrorCode.POLICY_BLOCKED,
                )
                raise RevocationRuntimeStateError(
                    "revocation generation is no longer the active suppression fence"
                )
            return tuple(obligation.descriptor(case) for obligation in obligations)

    async def assert_action_fence(
        self,
        case_id: str,
        obligation_id: str,
    ) -> None:
        """Revalidate generation and provider state immediately before apply.

        Real connectors must additionally pass ``source_generation`` to a
        provider-native conditional write/delete when their network call can
        race a newer authorization.
        """

        _token(obligation_id, "obligation_id")
        async with self._lock:
            case = await self._case(case_id)
            if case.stage not in {
                RevocationStage.PLANNED,
                RevocationStage.DISPATCHED,
                RevocationStage.ACKNOWLEDGED,
                RevocationStage.FENCE_REACHED,
            }:
                raise RevocationRuntimeStateError(
                    "target action is not allowed in the current case stage"
                )
            obligations = self._registered_obligations(case)
            if obligation_id not in case.expected_targets:
                raise RevocationRuntimeProtocolError(
                    "target action is not a registered case obligation"
                )
            blocked_reason = self._validate_obligations(case, obligations)
            if blocked_reason is not None:
                await self._block_case(case, blocked_reason)
                raise RevocationRuntimeStateError(
                    "current target registration cannot satisfy strict policy"
                )
            if not await self._has_active_suppression(case):
                await self._block_case(
                    case,
                    SafeRevocationErrorCode.POLICY_BLOCKED,
                )
                raise RevocationRuntimeStateError(
                    "revocation generation is no longer the active suppression fence"
                )

    def _obligation_for(
        self,
        case: RevocationCase,
        obligation_id: str,
    ) -> TargetObligation:
        obligations = self._registered_obligations(case)
        by_id = {
            obligation.action_id(case.case_id): obligation for obligation in obligations
        }
        try:
            return by_id[obligation_id]
        except KeyError as error:
            raise RevocationRuntimeProtocolError(
                "target action is not a registered case obligation"
            ) from error

    async def get_case(self, case_id: str) -> RevocationCase | None:
        """Return the durable case summary for status and retry decisions."""

        return await self._ledger.get_case(case_id)

    async def notify_synor_precommit(
        self,
        case_id: str,
        obligation_id: str,
    ) -> None:
        """Mark the sink-entry boundary reached after Synor's real precommit.

        A connector calls this as its first sink operation.  Entering a target
        sink is the existing engine's observable proof that its uncertainty
        precommit completed.
        """

        await self.assert_action_fence(case_id, obligation_id)
        await self._boundary(
            LifecycleBoundary.SYNOR_PRECOMMIT,
            case_id,
            obligation_id,
        )

    async def notify_target_effect_applied(
        self,
        case_id: str,
        obligation_id: str,
    ) -> None:
        """Expose the effect-before-dispatch-persistence crash window."""

        _token(obligation_id, "obligation_id")
        case = await self._case(case_id)
        self._obligation_for(case, obligation_id)
        await self._boundary(
            LifecycleBoundary.TARGET_APPLIED,
            case_id,
            obligation_id,
        )

    async def mark_target_applied(
        self,
        case_id: str,
        obligation_id: str,
    ) -> RevocationCase:
        """Persist dispatch after an idempotent external apply returns."""

        _token(obligation_id, "obligation_id")
        async with self._lock:
            case = await self._case(case_id)
            if case.stage in {
                RevocationStage.DISPATCHED,
                RevocationStage.ACKNOWLEDGED,
                RevocationStage.FENCE_REACHED,
                RevocationStage.VERIFIED,
                RevocationStage.RETAINED_ISOLATED,
                RevocationStage.CLOSED,
            }:
                return case
            if case.stage not in {
                RevocationStage.PLANNED,
            }:
                raise RevocationRuntimeStateError(
                    "target apply requires a capability-validated planned case"
                )
            obligations = self._registered_obligations(case)
            self._obligation_for(case, obligation_id)
            blocked_reason = self._validate_obligations(case, obligations)
            if blocked_reason is not None:
                await self._block_case(case, blocked_reason)
                raise RevocationRuntimeStateError(
                    "current target registration cannot satisfy strict policy"
                )
            if not await self._has_active_suppression(case):
                await self._block_case(
                    case,
                    SafeRevocationErrorCode.POLICY_BLOCKED,
                )
                raise RevocationRuntimeStateError(
                    "revocation generation is no longer the active suppression fence"
                )
            case = await self._append_transition(
                case,
                RevocationStage.DISPATCHED,
            )
            return case

    async def mark_acknowledged(
        self,
        case_id: str,
        obligation_id: str,
    ) -> RevocationCase:
        """Persist target acknowledgement before read-back verification."""

        _token(obligation_id, "obligation_id")
        async with self._lock:
            case = await self._case(case_id)
            if case.stage in {
                RevocationStage.ACKNOWLEDGED,
                RevocationStage.FENCE_REACHED,
                RevocationStage.VERIFIED,
                RevocationStage.RETAINED_ISOLATED,
                RevocationStage.CLOSED,
            }:
                return case
            if case.stage is not RevocationStage.DISPATCHED:
                raise RevocationRuntimeStateError(
                    "acknowledgement requires a dispatched case"
                )
            self._obligation_for(case, obligation_id)
            case = await self._append_transition(
                case,
                RevocationStage.ACKNOWLEDGED,
            )
            await self._boundary(
                LifecycleBoundary.ACKNOWLEDGEMENT_PERSISTED,
                case.case_id,
                obligation_id,
            )
            return case

    def outcome_recorder(
        self,
        case_id: str,
        *,
        attempt: int,
        attempted_at: datetime.datetime | None = None,
    ) -> Callable[
        [ContextProvider, Sequence[TargetVerificationOutcome]],
        Awaitable[None],
    ]:
        """Build the callback consumed by :class:`VerifiedTargetActionSink`."""

        if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 0:
            raise ValueError("attempt must be a non-negative integer")
        fixed_attempted_at = (
            _utc(attempted_at, "attempted_at") if attempted_at is not None else None
        )

        async def record(
            context_provider: ContextProvider,
            outcomes: Sequence[TargetVerificationOutcome],
            /,
        ) -> None:
            del context_provider
            await self.record_outcomes(
                case_id,
                outcomes,
                attempt=attempt,
                attempted_at=fixed_attempted_at,
            )

        return record

    async def record_outcomes(
        self,
        case_id: str,
        outcomes: Sequence[TargetVerificationOutcome],
        *,
        attempt: int,
        attempted_at: datetime.datetime | None = None,
    ) -> RevocationCase:
        """Append privacy-safe receipts and advance only on proved outcomes."""

        if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 0:
            raise ValueError("attempt must be a non-negative integer")
        outcome_batch = tuple(outcomes)
        if not outcome_batch:
            raise RevocationRuntimeProtocolError(
                "verification must return at least one outcome"
            )
        if not all(
            isinstance(outcome, TargetVerificationOutcome) for outcome in outcome_batch
        ):
            raise RevocationRuntimeProtocolError(
                "verification returned an invalid outcome"
            )
        if len({outcome.action_id for outcome in outcome_batch}) != len(outcome_batch):
            raise RevocationRuntimeProtocolError(
                "verification returned duplicate obligation IDs"
            )

        async with self._lock:
            case = await self._case(case_id)
            if case.stage is RevocationStage.CLOSED:
                return case
            if case.stage is RevocationStage.DISPATCHED:
                case = await self._append_transition(
                    case,
                    RevocationStage.ACKNOWLEDGED,
                )
                await self._boundary(
                    LifecycleBoundary.ACKNOWLEDGEMENT_PERSISTED,
                    case.case_id,
                )
            if case.stage not in {
                RevocationStage.ACKNOWLEDGED,
                RevocationStage.FAILED,
                RevocationStage.VERIFIED,
                RevocationStage.RETAINED_ISOLATED,
            }:
                raise RevocationRuntimeStateError(
                    "outcome recording requires an acknowledged case"
                )

            try:
                registrations = self._obligations[case_id]
            except KeyError as error:
                raise RevocationRuntimeProtocolError(
                    "target obligations are not registered"
                ) from error
            for outcome in outcome_batch:
                obligation = registrations.get(outcome.action_id)
                if (
                    obligation is None
                    or outcome.action_id not in case.expected_targets
                    or outcome.operation is not obligation.operation_kind
                    or outcome.source_digest != case.source_digest
                    or outcome.source_generation != case.suppression_generation
                    or outcome.target_locator_digest != obligation.target_locator_digest
                ):
                    raise RevocationRuntimeProtocolError(
                        "verification outcome does not match its obligation"
                    )

            existing_receipts = await self._ledger.list_receipts(case_id)
            if case.stage in {
                RevocationStage.FAILED,
                RevocationStage.VERIFIED,
                RevocationStage.RETAINED_ISOLATED,
            }:
                existing_ids = {receipt.receipt_id for receipt in existing_receipts}
                replay_ids = {
                    make_receipt_id(
                        outcome.action_id,
                        self._receipt_stage(outcome),
                        outcome.status,
                        attempt,
                    )
                    for outcome in outcome_batch
                }
                if replay_ids.issubset(existing_ids):
                    return case
                raise RevocationRuntimeStateError(
                    "a persisted outcome requires explicit retry preparation"
                )

            await self._boundary(
                LifecycleBoundary.VERIFICATION_COMPLETED,
                case.case_id,
            )
            attempted = (
                _utc(attempted_at, "attempted_at")
                if attempted_at is not None
                else _utc(self._clock(), "clock result")
            )
            verified = _utc(self._clock(), "clock result")
            if verified < attempted:
                verified = attempted
            receipts_by_id = {
                receipt.receipt_id: receipt for receipt in existing_receipts
            }
            previous_digest = (
                existing_receipts[-1].evidence_digest() if existing_receipts else None
            )
            for outcome in outcome_batch:
                obligation = registrations[outcome.action_id]
                receipt = self._receipt(
                    case=case,
                    obligation=obligation,
                    outcome=outcome,
                    attempt=attempt,
                    attempted_at=attempted,
                    verified_at=verified,
                    previous_receipt_digest=previous_digest,
                )
                existing = receipts_by_id.get(receipt.receipt_id)
                if existing is None:
                    await self._ledger.append_receipt(receipt)
                    receipts_by_id[receipt.receipt_id] = receipt
                    previous_digest = receipt.evidence_digest()
                    await self._boundary(
                        LifecycleBoundary.RECEIPT_APPENDED,
                        case.case_id,
                        outcome.action_id,
                    )
                else:
                    if (
                        existing.obligation_id != receipt.obligation_id
                        or existing.observed_outcome != receipt.observed_outcome
                        or existing.stage != receipt.stage
                        or existing.attempt != receipt.attempt
                    ):
                        raise RevocationRuntimeProtocolError(
                            "existing receipt conflicts with verification outcome"
                        )

            any_failed = any(
                not outcome.required_postcondition_holds for outcome in outcome_batch
            )
            if any_failed:
                if case.stage in {
                    RevocationStage.ACKNOWLEDGED,
                    RevocationStage.FAILED,
                }:
                    if case.stage is not RevocationStage.FAILED:
                        case = await self._append_transition(
                            case,
                            RevocationStage.FAILED,
                            safe_error_code=self._failure_code(outcome_batch),
                        )
                return case

            latest_receipts: dict[str, RevocationReceipt] = {}
            for receipt in receipts_by_id.values():
                latest_receipts[receipt.obligation_id] = receipt
            satisfied = {
                obligation_id
                for obligation_id, receipt in latest_receipts.items()
                if self._receipt_is_terminal(receipt)
            }
            if not set(case.expected_targets).issubset(satisfied):
                return case

            if case.stage in {
                RevocationStage.VERIFIED,
                RevocationStage.RETAINED_ISOLATED,
            }:
                return case
            if self._policy.require_consistency_fence:
                case = await self._append_transition(
                    case,
                    RevocationStage.FENCE_REACHED,
                )
            target_stage = (
                RevocationStage.RETAINED_ISOLATED
                if case.policy_decision is RevocationPolicyDecision.PRESERVE_ON_HOLD
                else RevocationStage.VERIFIED
            )
            case = await self._append_transition(case, target_stage)
            return case

    async def finalize_after_engine_commit(self, case_id: str) -> RevocationCase:
        """Close only a terminal, evidenced case after Synor final commit."""

        async with self._lock:
            case = await self._case(case_id)
            if case.stage is RevocationStage.CLOSED:
                return case
            if case.stage not in {
                RevocationStage.VERIFIED,
                RevocationStage.RETAINED_ISOLATED,
            }:
                raise RevocationRuntimeStateError(
                    "engine final commit cannot close an unverified case"
                )
            await self._boundary(
                LifecycleBoundary.ENGINE_FINAL_COMMIT,
                case.case_id,
            )
            case = await self._append_transition(case, RevocationStage.CLOSED)
            await self._boundary(
                LifecycleBoundary.CASE_SUMMARY_UPDATED,
                case.case_id,
            )
            return case

    async def record_verified_reauthorization(
        self,
        request: RevocationRequest,
        *,
        generation: int,
        policy_id: str,
        policy_revision: str,
        group_graph_revision: str,
        observed_at: datetime.datetime | None = None,
    ) -> None:
        """Lift suppression after a newer policy/derivative was verified.

        The caller must install and verify the new access state before invoking
        this method.  Any still-open older case is durably blocked before the
        serving suppression is superseded.
        """

        if (
            not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation <= request.suppression_generation
        ):
            raise RevocationRuntimeStateError(
                "reauthorization must use a newer suppression generation"
            )
        async with self._lock:
            case = await self._case(request.case_id)
            if not self._request_matches_case(request, case):
                raise RevocationRuntimeStateError(
                    "persisted revocation case does not match the request"
                )
            current = await self._suppression.get(case.source_digest)
            if current is not None:
                if generation < current.generation:
                    raise RevocationRuntimeStateError(
                        "reauthorization must use a newer suppression generation"
                    )
                if generation == current.generation:
                    if (
                        current.source_digest == case.source_digest
                        and current.tenant_digest == case.tenant_digest
                        and current.policy_id == policy_id
                        and not current.suppressed
                        and current.verified_authorization
                        and current.policy_revision == policy_revision
                        and current.group_graph_revision == group_graph_revision
                    ):
                        if case.stage in _BLOCKABLE_MATERIALIZATION_STAGES:
                            await self._block_case(
                                case,
                                SafeRevocationErrorCode.POLICY_BLOCKED,
                            )
                        return
                    raise RevocationRuntimeStateError(
                        "suppression generation has conflicting authorization"
                    )
            if case.stage in _BLOCKABLE_MATERIALIZATION_STAGES:
                case = await self._block_case(
                    case,
                    SafeRevocationErrorCode.POLICY_BLOCKED,
                )
            record = await self._suppression.authorize(
                source_digest=case.source_digest,
                tenant_digest=case.tenant_digest,
                policy_id=policy_id,
                generation=generation,
                policy_revision=policy_revision,
                group_graph_revision=group_graph_revision,
                observed_at=observed_at,
            )
            if (
                record.source_digest != case.source_digest
                or record.tenant_digest != case.tenant_digest
                or record.policy_id != policy_id
                or record.generation != generation
                or record.suppressed
                or not record.verified_authorization
                or record.policy_revision != policy_revision
                or record.group_graph_revision != group_graph_revision
            ):
                raise RevocationRuntimeStateError(
                    "newer authorization did not become the active generation"
                )

    def _receipt(
        self,
        *,
        case: RevocationCase,
        obligation: TargetObligation,
        outcome: TargetVerificationOutcome,
        attempt: int,
        attempted_at: datetime.datetime,
        verified_at: datetime.datetime,
        previous_receipt_digest: str | None,
    ) -> RevocationReceipt:
        stage = self._receipt_stage(outcome)
        if outcome.operation is EffectOperation.ISOLATE:
            assurance = (
                AssuranceLevel.RETAINED_ISOLATED
                if outcome.required_postcondition_holds
                else AssuranceLevel.ACKNOWLEDGED
            )
        else:
            assurance = (
                AssuranceLevel.QUERY_VERIFIED
                if outcome.required_postcondition_holds
                else AssuranceLevel.ACKNOWLEDGED
            )
        safe_error_code = (
            None
            if outcome.required_postcondition_holds
            else self._outcome_error_code(outcome.status).value
        )
        return RevocationReceipt(
            schema_version=1,
            receipt_id=make_receipt_id(
                outcome.action_id,
                stage,
                outcome.status,
                attempt,
            ),
            case_id=case.case_id,
            obligation_id=outcome.action_id,
            attempt=attempt,
            source_digest=case.source_digest,
            target_provider_id=obligation.target_provider_id,
            target_instance_digest=obligation.target_instance_digest,
            target_locator_digest=obligation.target_locator_digest,
            operation_kind=obligation.operation_kind.value,
            reason=case.reason.value,
            policy_decision=case.policy_decision.value,
            stage=stage.value,
            assurance_level=assurance.value,
            request_fingerprint=_request_fingerprint(
                case.case_id,
                outcome.action_id,
                attempt,
            ),
            operation_id=outcome.operation_id,
            affected_count=outcome.affected_count,
            capability_digest=obligation.proof_capabilities.contract_digest(),
            consistency_contract=obligation.consistency_contract,
            verifier_kind=obligation.verifier_kind,
            observed_outcome=outcome.status.value,
            attempted_at=attempted_at,
            verified_at=(verified_at if outcome.required_postcondition_holds else None),
            safe_error_code=safe_error_code,
            previous_receipt_digest=previous_receipt_digest,
        )

    @staticmethod
    def _receipt_stage(
        outcome: TargetVerificationOutcome,
    ) -> RevocationStage:
        if not outcome.required_postcondition_holds:
            return RevocationStage.ACKNOWLEDGED
        if outcome.operation is EffectOperation.ISOLATE:
            return RevocationStage.RETAINED_ISOLATED
        return RevocationStage.VERIFIED

    @staticmethod
    def _receipt_is_terminal(receipt: RevocationReceipt) -> bool:
        return (
            receipt.stage == RevocationStage.VERIFIED.value
            and receipt.assurance_level
            in {
                AssuranceLevel.QUERY_VERIFIED.value,
                AssuranceLevel.ERASURE_ATTESTED.value,
            }
            and receipt.observed_outcome == VerificationOutcome.ABSENT.value
        ) or (
            receipt.stage == RevocationStage.RETAINED_ISOLATED.value
            and receipt.assurance_level == AssuranceLevel.RETAINED_ISOLATED.value
            and receipt.observed_outcome == VerificationOutcome.RETAINED_ISOLATED.value
        )

    @staticmethod
    def _outcome_error_code(
        outcome: VerificationOutcome,
    ) -> SafeRevocationErrorCode:
        return verification_outcome_error_code(outcome)

    @classmethod
    def _failure_code(
        cls,
        outcomes: Sequence[TargetVerificationOutcome],
    ) -> SafeRevocationErrorCode:
        for outcome in outcomes:
            if not outcome.required_postcondition_holds:
                return cls._outcome_error_code(outcome.status)
        return SafeRevocationErrorCode.VERIFICATION_PROTOCOL


StrictRevocationRuntime = RevocationRuntime
