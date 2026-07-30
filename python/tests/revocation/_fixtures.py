from __future__ import annotations

import datetime
import hashlib

from synor._internal.revocation_model import (
    AssuranceLevel,
    EffectOperation,
    RevocationCase,
    RevocationPolicyDecision,
    RevocationReceipt,
    RevocationStage,
    SourceEventKind,
    SourceIdentity,
    VerificationOutcome,
    make_action_id,
    make_case_id,
    make_observation_id,
    make_proof_contract_digest,
    make_receipt_id,
)


NOW = datetime.datetime(2026, 7, 29, 12, 0, tzinfo=datetime.timezone.utc)
IDENTITY = SourceIdentity("connector-a", "scope-a", "provider-item-17")
SOURCE_DIGEST = IDENTITY.evidence_digest()
TENANT_DIGEST = hashlib.sha256(b"tenant-a").hexdigest()
POLICY_DIGEST = hashlib.sha256(b"policy-v7").hexdigest()
TARGET_INSTANCE_DIGEST = hashlib.sha256(b"target-instance").hexdigest()
TARGET_LOCATOR_DIGEST = hashlib.sha256(b"target-locator").hexdigest()
REQUEST_FINGERPRINT = hashlib.sha256(b"request").hexdigest()
CAPABILITY_DIGEST = hashlib.sha256(b"capability-contract").hexdigest()
PROOF_CONTRACT_DIGEST = make_proof_contract_digest(
    "exact-id-query",
    "strong-read",
    CAPABILITY_DIGEST,
)


def make_case() -> RevocationCase:
    decision = RevocationPolicyDecision.DESTROY
    observation_id = make_observation_id(
        IDENTITY,
        "source-revision-7",
        SourceEventKind.ACCESS_LOST,
    )
    case_id = make_case_id(
        IDENTITY,
        "source-revision-7",
        SourceEventKind.ACCESS_LOST,
        decision,
        observation_id,
    )
    obligation_id = make_action_id(
        case_id,
        "qdrant",
        TARGET_INSTANCE_DIGEST,
        TARGET_LOCATOR_DIGEST,
        EffectOperation.DELETE,
        PROOF_CONTRACT_DIGEST,
    )
    return RevocationCase(
        case_id=case_id,
        observation_id=observation_id,
        source_digest=SOURCE_DIGEST,
        source_revision="source-revision-7",
        tenant_digest=TENANT_DIGEST,
        policy_id="policy-a",
        policy_revision="policy-v7",
        policy_digest=POLICY_DIGEST,
        group_graph_revision="groups-v3",
        legal_state=None,
        suppression_generation=7,
        reason=SourceEventKind.ACCESS_LOST,
        policy_decision=decision,
        stage=RevocationStage.OBSERVED,
        observed_at=NOW,
        suppress_by=NOW,
        verify_by=NOW + datetime.timedelta(minutes=5),
        expected_targets=(obligation_id,),
        version=1,
    )


def make_receipt(
    case: RevocationCase,
    *,
    attempt: int,
    previous_receipt_digest: str | None,
    stage: RevocationStage = RevocationStage.VERIFIED,
    outcome: VerificationOutcome = VerificationOutcome.ABSENT,
) -> RevocationReceipt:
    action_id = make_action_id(
        case.case_id,
        "qdrant",
        TARGET_INSTANCE_DIGEST,
        TARGET_LOCATOR_DIGEST,
        EffectOperation.DELETE,
        PROOF_CONTRACT_DIGEST,
    )
    return RevocationReceipt(
        schema_version=1,
        receipt_id=make_receipt_id(action_id, stage, outcome, attempt),
        case_id=case.case_id,
        obligation_id=action_id,
        attempt=attempt,
        source_digest=case.source_digest,
        target_provider_id="qdrant",
        target_instance_digest=TARGET_INSTANCE_DIGEST,
        target_locator_digest=TARGET_LOCATOR_DIGEST,
        operation_kind=EffectOperation.DELETE.value,
        reason=case.reason.value,
        policy_decision=case.policy_decision.value,
        stage=stage.value,
        assurance_level=AssuranceLevel.QUERY_VERIFIED.value,
        request_fingerprint=REQUEST_FINGERPRINT,
        operation_id="operation-opaque-1",
        affected_count=1,
        capability_digest=CAPABILITY_DIGEST,
        consistency_contract="strong-read",
        verifier_kind="exact-id-query",
        observed_outcome=outcome.value,
        attempted_at=NOW + datetime.timedelta(seconds=attempt),
        verified_at=NOW + datetime.timedelta(seconds=attempt, milliseconds=10),
        safe_error_code=None,
        previous_receipt_digest=previous_receipt_digest,
    )
