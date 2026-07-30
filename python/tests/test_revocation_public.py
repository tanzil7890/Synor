from __future__ import annotations

import dataclasses
import datetime
import hashlib
import json

import pytest

import synor.revocation as revocation
from synor.state import MemoryStateStore
from synor._internal import revocation_model as internal_model
from synor._internal.revocation_ledger import StateStoreRevocationLedger

from .revocation._fixtures import NOW, make_case, make_receipt


def _case_at(
    stage: revocation.RevocationStage,
    sequence: int,
    *,
    decision: revocation.RevocationPolicyDecision = (
        revocation.RevocationPolicyDecision.DESTROY
    ),
) -> revocation.RevocationCase:
    case_id = f"case1_{hashlib.sha256(f'case-{sequence}'.encode()).hexdigest()}"
    return dataclasses.replace(
        make_case(),
        case_id=case_id,
        policy_decision=decision,
        stage=stage,
        version=sequence,
    )


def test_public_model_exports_alias_the_versioned_internal_contracts() -> None:
    assert revocation.AccessSnapshot is internal_model.AccessSnapshot
    assert revocation.GovernedSourceItem is internal_model.GovernedSourceItem
    assert revocation.RevocationCase is internal_model.RevocationCase
    assert revocation.RevocationReceipt is internal_model.RevocationReceipt
    assert revocation.SourceIdentity is internal_model.SourceIdentity
    assert revocation.make_observation_id is internal_model.make_observation_id
    assert revocation.make_tenant_digest is internal_model.make_tenant_digest

    expected_names = {
        "AccessSnapshot",
        "GovernedSourceItem",
        "RevocationCase",
        "RevocationController",
        "RevocationHealth",
        "RevocationOperator",
        "RevocationPolicy",
        "RevocationReceipt",
        "RevocationRepository",
        "RevocationSummary",
        "SourceIdentity",
        "make_observation_id",
        "make_tenant_digest",
    }
    assert expected_names <= set(revocation.__all__)


def test_revocation_summary_classifies_every_lifecycle_bucket_and_deadline() -> None:
    cases = (
        _case_at(revocation.RevocationStage.OBSERVED, 1),
        _case_at(revocation.RevocationStage.SUPPRESSED, 2),
        _case_at(revocation.RevocationStage.PLANNED, 3),
        _case_at(revocation.RevocationStage.DISPATCHED, 4),
        _case_at(revocation.RevocationStage.ACKNOWLEDGED, 5),
        _case_at(revocation.RevocationStage.FENCE_REACHED, 6),
        _case_at(revocation.RevocationStage.VERIFIED, 7),
        _case_at(revocation.RevocationStage.CLOSED, 8),
        _case_at(
            revocation.RevocationStage.RETAINED_ISOLATED,
            9,
            decision=revocation.RevocationPolicyDecision.PRESERVE_ON_HOLD,
        ),
        _case_at(
            revocation.RevocationStage.CLOSED,
            10,
            decision=revocation.RevocationPolicyDecision.PRESERVE_ON_HOLD,
        ),
        _case_at(revocation.RevocationStage.FAILED, 11),
        _case_at(revocation.RevocationStage.BLOCKED, 12),
    )

    summary = revocation.RevocationSummary.from_cases(
        cases,
        now=NOW + datetime.timedelta(minutes=6),
    )

    assert summary == revocation.RevocationSummary(
        observed=1,
        suppressed=5,
        verified=2,
        retained=2,
        failed=1,
        blocked=1,
        overdue=8,
    )
    assert summary.open == 8
    assert summary.to_dict() == {
        "schema_version": 1,
        "observed": 1,
        "suppressed": 5,
        "verified": 2,
        "retained": 2,
        "failed": 1,
        "blocked": 1,
        "overdue": 8,
        "open": 8,
    }


@pytest.mark.asyncio
async def test_repository_round_trip_filter_and_fail_closed_startup_health() -> None:
    store = MemoryStateStore()
    case = make_case()
    await StateStoreRevocationLedger(store).append_case(case)
    repository = revocation.RevocationRepository(store)

    assert await repository.get(case.case_id) == case
    assert await repository.list(status=revocation.RevocationStage.OBSERVED) == (case,)
    assert await repository.list(status="observed") == (case,)
    assert await repository.list(status="failed") == ()

    unsafe = await repository.startup_health(now=NOW)
    assert unsafe.ready is False
    assert unsafe.open_case_ids == (case.case_id,)
    assert unsafe.unsafe_case_ids == (case.case_id,)
    assert unsafe.safe_error_code == "revocation.suppression_unconfirmed"

    await revocation.StateStoreSuppressionIndex(store).suppress(
        source_digest=case.source_digest,
        tenant_digest=case.tenant_digest,
        policy_id=case.policy_id,
        generation=case.suppression_generation,
        policy_revision=case.policy_revision,
        group_graph_revision=case.group_graph_revision,
        reason=case.reason.value,
        case_id=case.case_id,
        observed_at=NOW,
    )
    healthy = await repository.startup_health(now=NOW)
    assert healthy.ready is True
    assert healthy.open_case_ids == (case.case_id,)
    assert healthy.overdue_case_ids == ()
    assert healthy.unsafe_case_ids == ()
    assert healthy.safe_error_code is None

    overdue = await repository.startup_health(now=NOW + datetime.timedelta(minutes=6))
    assert overdue.ready is False
    assert overdue.overdue_case_ids == (case.case_id,)
    assert overdue.safe_error_code == "revocation.deadline_overdue"
    assert overdue.summary.overdue == 1


@pytest.mark.asyncio
async def test_repository_health_turns_corruption_into_a_controlled_failure() -> None:
    store = MemoryStateStore()
    case = make_case()
    await StateStoreRevocationLedger(store).append_case(case)
    await store.put(f"revocation/v1/cases/{case.case_id}.json", b"{")

    health = await revocation.RevocationRepository(store).startup_health(now=NOW)

    assert health == revocation.RevocationHealth(
        ready=False,
        summary=revocation.RevocationSummary(),
        safe_error_code="revocation.state_corrupt",
    )


def test_repository_operator_views_are_explicitly_metadata_only() -> None:
    repository = revocation.RevocationRepository(MemoryStateStore())
    case = make_case()
    receipt = make_receipt(case, attempt=0, previous_receipt_digest=None)

    case_metadata = repository.case_metadata(case)
    receipt_metadata = repository.receipt_metadata(receipt)
    rendered = json.dumps(
        {"case": case_metadata, "receipt": receipt_metadata},
        sort_keys=True,
    )

    assert case.source_revision not in rendered
    assert "source_revision" not in case_metadata
    assert len(str(case_metadata["source_revision_digest"])) == 64
    assert receipt.operation_id is not None
    assert receipt.operation_id not in rendered
    assert "operation_id" not in receipt_metadata
    assert receipt_metadata["operation_id_present"] is True


class _ExampleOperator:
    async def verify(self, case_id: str) -> revocation.RevocationOperatorResult:
        return revocation.RevocationOperatorResult(
            case_id=case_id,
            operation="verify",
            stage=revocation.RevocationStage.VERIFIED,
            mutated=False,
        )

    async def retry(self, case_id: str) -> revocation.RevocationOperatorResult:
        return revocation.RevocationOperatorResult(
            case_id=case_id,
            operation="retry",
            stage=revocation.RevocationStage.DISPATCHED,
            mutated=True,
            attempt=1,
        )

    async def scan(self, target_id: str) -> revocation.RevocationScanResult:
        return revocation.RevocationScanResult(
            target_id=target_id,
            scanned_count=2,
            matching_count=1,
            drift_count=0,
        )


def test_operator_protocol_and_results_keep_verify_read_only() -> None:
    assert isinstance(_ExampleOperator(), revocation.RevocationOperator)
    with pytest.raises(ValueError, match="verification is read-only"):
        revocation.RevocationOperatorResult(
            case_id=make_case().case_id,
            operation="verify",
            stage=revocation.RevocationStage.DISPATCHED,
            mutated=True,
        )
    with pytest.raises(ValueError, match="cannot exceed"):
        revocation.RevocationScanResult(
            target_id="qdrant-main",
            scanned_count=1,
            matching_count=2,
            drift_count=0,
        )
    with pytest.raises(ValueError, match="controlled registry"):
        revocation.RevocationOperatorResult(
            case_id=make_case().case_id,
            operation="verify",
            stage=revocation.RevocationStage.FAILED,
            mutated=False,
            safe_error_code="patient_secret",
        )
    with pytest.raises(ValueError, match="controlled registry"):
        revocation.RevocationHealth(
            ready=False,
            summary=revocation.RevocationSummary(),
            safe_error_code="patient_secret",
        )


def test_controller_is_keyword_only_strict_and_exposes_one_suppression_index() -> None:
    store = MemoryStateStore()
    policy = revocation.RevocationPolicy.strict_query_verified()
    controller = revocation.RevocationController(
        state_store=store,
        policy=policy,
    )

    assert controller.suppression is controller.suppression_lookup
    assert isinstance(controller.repository, revocation.RevocationRepository)
    controller.begin_controlled_run()
    assert controller.pending_finalization_case_ids() == ()

    with pytest.raises(TypeError):
        revocation.RevocationController(store, policy)  # type: ignore[misc]
    with pytest.raises(ValueError, match="strict policy"):
        revocation.RevocationController(
            state_store=store,
            policy=revocation.RevocationPolicy.compatibility(),
        )


def test_capability_profile_is_inspectable_and_digest_bound() -> None:
    capabilities = revocation.TargetRevocationCapabilities(
        atomic_serving_suppression=True,
        exact_id_delete=True,
        source_id_bulk_delete=True,
        query_time_acl_filter=True,
        tenant_isolation=True,
        synchronous_acknowledgement=True,
        consistency_fence=True,
        negative_read_verification=True,
        external_enumeration=True,
        legal_hold_isolation=True,
        physical_erasure_attestation=False,
        capability_version="test-v1",
    )

    metadata = capabilities.to_dict()

    assert metadata["schema_version"] == 1
    assert metadata["negative_read_verification"] is True
    assert metadata["physical_erasure_attestation"] is False
    assert metadata["contract_digest"] == capabilities.contract_digest()
