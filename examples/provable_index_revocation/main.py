"""Runnable service-free flagship for public provable-index revocation APIs."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import json
import os
import pathlib
import sys
import tempfile
import typing

import synor as syn
from dotenv import load_dotenv
from synor import governance, retrieval, revocation

if typing.TYPE_CHECKING:
    from .fake_index import EventuallyConsistentIndex
    from .fake_source import (
        CONTENT_SENTINEL,
        CONTROL_CONTENT_SENTINEL,
        PRINCIPAL_ALPHA,
        PRINCIPAL_BETA,
        DemoDocument,
        demo_documents,
        deterministic_chunks,
        digest,
    )
elif __package__:
    from .fake_index import EventuallyConsistentIndex
    from .fake_source import (
        CONTENT_SENTINEL,
        CONTROL_CONTENT_SENTINEL,
        PRINCIPAL_ALPHA,
        PRINCIPAL_BETA,
        DemoDocument,
        demo_documents,
        deterministic_chunks,
        digest,
    )
else:
    from fake_index import EventuallyConsistentIndex
    from fake_source import (
        CONTENT_SENTINEL,
        CONTROL_CONTENT_SENTINEL,
        PRINCIPAL_ALPHA,
        PRINCIPAL_BETA,
        DemoDocument,
        demo_documents,
        deterministic_chunks,
        digest,
    )


@dataclasses.dataclass(frozen=True, slots=True)
class DemoResult:
    controlled_passes: int
    startup_ready: bool
    startup_safe_error_code: str | None
    runtime_status: str
    execution_guarantee: str
    case_id: str
    partial_case_id: str
    initial_alpha_results: int
    initial_beta_results: int
    suppressed_results: int
    suppressed_scored: int
    stale_verification_stage: str
    final_stage: str
    receipt_count: int
    partial_stage: str
    partial_deleted_points: int
    restored_raw_points: int
    restored_guarded_results: int
    unaffected_results: int
    effective_deletes: int
    evidence_keys: int
    content_unchanged: bool

    def to_dict(self) -> dict[str, object]:
        return dataclasses.asdict(self)


def _capabilities() -> revocation.TargetRevocationCapabilities:
    return revocation.TargetRevocationCapabilities(
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
    )


def _obligation(
    identity: governance.SourceIdentity,
) -> revocation.TargetObligation:
    capabilities = _capabilities()
    source_digest = identity.evidence_digest()
    return revocation.TargetObligation(
        target_provider_id="fake-index",
        target_instance_digest=digest("fake-index-instance-v1"),
        target_locator_digest=digest(f"fake-index-locator:{source_digest}"),
        operation_kind=revocation.EffectOperation.DELETE,
        proof_capabilities=capabilities,
        capabilities=capabilities,
        verifier_kind="fake-exact-id",
        consistency_contract="eventual-readback",
    )


def _request(
    item: governance.GovernedSourceItem[bytes],
    *,
    generation: int,
    observed_at: datetime.datetime,
    decision: revocation.RevocationPolicyDecision,
    snapshot: governance.SnapshotResult | None = None,
    require_complete_snapshot: bool = False,
) -> revocation.RevocationRequest:
    if item.access is None:
        raise ValueError("the demo requires an access snapshot")
    return revocation.RevocationRequest(
        identity=item.identity,
        observation_id=item.observation_id,
        source_revision=item.source_revision,
        access=item.access,
        observation_generation=(
            "acl-change-v2"
            if item.event is governance.SourceEventKind.ACL_CHANGED
            else "partial-scan-v1"
        ),
        tenant_digest=revocation.make_tenant_digest(item.access.tenant_id),
        policy_id=item.access.policy_id,
        policy_revision=item.access.policy_revision,
        policy_digest=item.access.policy_digest,
        group_graph_revision=item.access.group_graph_revision,
        reason=item.event,
        policy_decision=decision,
        suppression_generation=generation,
        observed_at=observed_at,
        suppress_by=observed_at,
        verify_by=observed_at + datetime.timedelta(minutes=5),
        obligations=(_obligation(item.identity),),
        snapshot=snapshot,
        require_complete_snapshot=require_complete_snapshot,
    )


def _context(document: DemoDocument) -> retrieval.RetrievalContext:
    return retrieval.RetrievalContext(
        tenant_id=document.tenant_id,
        principal_id=document.principal_id,
        group_graph_revision="groups-v1",
    )


def _policy(document: DemoDocument) -> retrieval.AccessPolicy:
    return retrieval.AccessPolicy(
        policy_id=document.policy_id,
        tenant_id=document.tenant_id,
        revision="policy-v1",
        group_graph_revision="groups-v1",
        allowed_principal_ids=frozenset({document.principal_id}),
    )


async def _authorize_initial_generation(
    controller: revocation.RevocationController,
    document: DemoDocument,
    *,
    observed_at: datetime.datetime,
) -> None:
    await controller.authorize_generation(
        source_digest=document.identity.evidence_digest(),
        tenant_digest=revocation.make_tenant_digest(document.tenant_id),
        policy_id=document.policy_id,
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=observed_at,
    )


def _outcome(
    descriptor: revocation.EffectDescriptor,
    *,
    status: revocation.VerificationOutcome,
    attempt: int,
) -> revocation.TargetVerificationOutcome:
    return revocation.TargetVerificationOutcome(
        action_id=descriptor.action_id,
        operation=descriptor.operation_kind,
        source_digest=descriptor.source_digest,
        source_generation=descriptor.source_generation,
        target_locator_digest=descriptor.target_locator_digest,
        status=status,
        attempt_count=attempt,
        operation_id=f"fake-operation-{attempt}",
    )


async def _target_attempt(
    *,
    controller: revocation.RevocationController,
    index: EventuallyConsistentIndex,
    case_id: str,
    attempt: int,
    attempted_at: datetime.datetime,
) -> revocation.RevocationCase:
    (descriptor,) = await controller.descriptors_for(case_id)
    await controller.notify_synor_precommit(case_id, descriptor.action_id)
    index.begin_delete(
        action_id=descriptor.action_id,
        source_digest=descriptor.source_digest,
    )
    await controller.notify_target_effect_applied(case_id, descriptor.action_id)
    await controller.mark_target_applied(case_id, descriptor.action_id)
    await controller.mark_acknowledged(case_id, descriptor.action_id)
    absent = index.verify_absent(descriptor.action_id)
    return await controller.record_outcomes(
        case_id,
        (
            _outcome(
                descriptor,
                status=(
                    revocation.VerificationOutcome.ABSENT
                    if absent
                    else revocation.VerificationOutcome.PRESENT
                ),
                attempt=attempt,
            ),
        ),
        attempt=attempt,
        attempted_at=attempted_at,
    )


async def _assert_private_evidence(store: syn.StateStore) -> int:
    keys = await store.list()
    sensitive = (
        CONTENT_SENTINEL,
        CONTROL_CONTENT_SENTINEL,
        PRINCIPAL_ALPHA,
        PRINCIPAL_BETA,
    )
    for key in keys:
        payload = await store.get(key)
        if payload is None:
            continue
        for sentinel in sensitive:
            if sentinel.encode() in payload:
                raise AssertionError("sensitive source data entered control evidence")
    return len(keys)


async def run_fake_scenario(
    state_root: pathlib.Path,
) -> DemoResult:
    """Run baseline and ACL-revocation passes in one async event loop."""

    observed_at = datetime.datetime.now(datetime.timezone.utc)
    store = syn.FileStateStore(state_root)
    runtime = syn.SynorRuntime(
        state_store=store,
        revocation_policy=revocation.RevocationPolicy.strict_query_verified(),
        audit_dir=state_root / "run-evidence",
    )
    startup_health = await runtime.revocation_health()
    if startup_health.safe_error_code not in {
        None,
        "revocation.deadline_overdue",
    }:
        raise AssertionError(
            "strict runtime did not establish safe serving suppression"
        )
    controller = runtime.revocation_controller
    governed, control = demo_documents()
    governed_initial = governed.observed_item(
        event=governance.SourceEventKind.PRESENT,
        policy_revision="policy-v1",
        allowed=True,
        observation_generation="baseline-v1",
    )
    governed_revoked = governed.observed_item(
        event=governance.SourceEventKind.ACL_CHANGED,
        policy_revision="policy-v2",
        allowed=False,
        observation_generation="acl-change-v2",
    )
    if (
        governed_initial.resource != governed_revoked.resource
        or governed_initial.content_fingerprint != governed_revoked.content_fingerprint
        or governed_initial.source_revision != governed_revoked.source_revision
    ):
        raise AssertionError("the ACL-only fixture unexpectedly changed content")
    request = _request(
        governed_revoked,
        generation=2,
        observed_at=observed_at,
        decision=revocation.RevocationPolicyDecision.RESTRICT,
    )
    existing_case = await controller.get_case(request.case_id)
    if existing_case is not None:
        observed_at = existing_case.observed_at
        request = _request(
            governed_revoked,
            generation=2,
            observed_at=observed_at,
            decision=revocation.RevocationPolicyDecision.RESTRICT,
        )

    await _authorize_initial_generation(
        controller,
        governed,
        observed_at=observed_at,
    )
    await _authorize_initial_generation(
        controller,
        control,
        observed_at=observed_at,
    )
    index = EventuallyConsistentIndex()
    governed_points = index.index_document(
        governed,
        deterministic_chunks(governed),
        generation=1,
        policy_revision="policy-v1",
    )
    index.index_document(
        control,
        deterministic_chunks(control),
        generation=1,
        policy_revision="policy-v1",
    )
    policies = retrieval.InMemoryAccessPolicyLookup(
        (_policy(governed), _policy(control))
    )
    guard = retrieval.RetrievalGuard(
        suppression_lookup=controller.suppression,
        policy_lookup=policies,
    )

    initial_alpha = await index.guarded_query(
        "launch checklist",
        guard=guard,
        context=_context(governed),
    )
    initial_beta = await index.guarded_query(
        "independent tenant",
        guard=guard,
        context=_context(control),
    )
    if existing_case is None and (not initial_alpha or not initial_beta):
        raise AssertionError("baseline authorization did not return both tenants")
    if existing_case is not None and (
        existing_case.stage is not revocation.RevocationStage.CLOSED
        or initial_alpha
        or not initial_beta
    ):
        raise AssertionError("persisted revocation did not fail closed on replay")

    partial_identity = governance.SourceIdentity(
        connector_instance_id="demo-source",
        source_scope_id="tenant-alpha-drive",
        item_id="unseen-partial-item",
    )
    partial_document = DemoDocument(
        identity=partial_identity,
        tenant_id=governed.tenant_id,
        policy_id="policy-partial",
        principal_id=governed.principal_id,
        content=b"not-indexed",
    )
    partial_item = partial_document.observed_item(
        event=governance.SourceEventKind.SOURCE_DELETED,
        policy_revision="policy-v1",
        allowed=False,
        observation_generation="partial-scan-v1",
    )
    partial_snapshot = governance.SnapshotResult(
        connector_instance_id=partial_identity.connector_instance_id,
        source_scope_id=partial_identity.source_scope_id,
        epoch="partial-epoch-v1",
        cursor_before="cursor-v1",
        cursor_after=None,
        status="partial",
        item_count=1,
        inaccessible_scope_digests=(digest("inaccessible-folder"),),
    )
    partial_request = _request(
        partial_item,
        generation=1,
        observed_at=observed_at,
        decision=revocation.RevocationPolicyDecision.DESTROY,
        snapshot=partial_snapshot,
        require_complete_snapshot=True,
    )
    suppressed_counts: list[int] = []
    suppressed_scored_counts: list[int] = []
    stale_verification_stages: list[str] = []
    receipt_chains: list[tuple[revocation.RevocationReceipt, ...]] = []
    point_ids_before_partial: list[frozenset[str]] = []
    partial_cases: list[revocation.RevocationCase] = []

    async def _run_controlled_apps(
        engine_root: pathlib.Path,
    ) -> tuple[syn.ExecutionReport, syn.ExecutionReport]:
        environment = syn.Environment(syn.Settings(db_path=engine_root / "engine"))

        @syn.task
        async def apply_revocation() -> None:
            planned = await controller.begin_case(request)
            suppressed = await index.guarded_query(
                "launch checklist",
                guard=guard,
                context=_context(governed),
            )
            suppressed_scored = len(index.last_scored_ids)
            if suppressed or suppressed_scored:
                raise AssertionError("revoked candidates reached the scorer")
            suppressed_counts.append(len(suppressed))
            suppressed_scored_counts.append(suppressed_scored)

            if planned.stage is revocation.RevocationStage.PLANNED:
                stale = await _target_attempt(
                    controller=controller,
                    index=index,
                    case_id=request.case_id,
                    attempt=1,
                    attempted_at=observed_at + datetime.timedelta(seconds=1),
                )
                if stale.stage is not revocation.RevocationStage.FAILED:
                    raise AssertionError(
                        "stale target presence did not keep the case open"
                    )
                stale_verification_stages.append(stale.stage.value)
                if not index.points_for_source(governed.identity.evidence_digest()):
                    raise AssertionError(
                        "the fake target did not simulate delayed deletion"
                    )

                await controller.prepare_retry(request)
                verified = await _target_attempt(
                    controller=controller,
                    index=index,
                    case_id=request.case_id,
                    attempt=2,
                    attempted_at=observed_at + datetime.timedelta(seconds=2),
                )
                if verified.stage is not revocation.RevocationStage.VERIFIED:
                    raise AssertionError("retry did not verify target absence")
            elif planned.stage is revocation.RevocationStage.CLOSED:
                stale_verification_stages.append(
                    revocation.RevocationStage.FAILED.value
                )
            else:
                raise AssertionError("strict revocation was neither planned nor closed")

            receipts = await controller.list_receipts(request.case_id)
            if len(receipts) < 2:
                raise AssertionError("revocation is missing its receipt chain")
            if [receipt.observed_outcome for receipt in receipts[:2]] != [
                revocation.VerificationOutcome.PRESENT.value,
                revocation.VerificationOutcome.ABSENT.value,
            ]:
                raise AssertionError("revocation has an unexpected verification chain")
            receipt_chains.append(receipts)

            point_ids_before_partial.append(index.point_ids)
            partial_case = await controller.begin_case(partial_request)
            if partial_case.stage is not revocation.RevocationStage.BLOCKED:
                raise AssertionError("partial snapshot did not block cleanup")
            partial_cases.append(partial_case)

        controlled_app = syn.App(
            syn.AppConfig(
                name="ProvableIndexRevocationDemo",
                environment=environment,
            ),
            apply_revocation,
        )
        first_report = await runtime.run(
            controlled_app,
            app_target="examples/provable_index_revocation/main.py",
        )

        @syn.task
        async def replay_closed_case() -> None:
            replayed = await controller.begin_case(request)
            if replayed.stage is not revocation.RevocationStage.CLOSED:
                raise AssertionError(
                    "a repeated controlled pass regressed the closed case"
                )

        replay_app = syn.App(
            syn.AppConfig(
                name="ProvableIndexRevocationReplay",
                environment=environment,
            ),
            replay_closed_case,
        )
        second_report = await runtime.run(
            replay_app,
            app_target="examples/provable_index_revocation/main.py",
        )
        return first_report, second_report

    # On Windows, LMDB keeps files open, so we need to ignore cleanup errors
    ignore_cleanup = sys.platform == "win32"

    with tempfile.TemporaryDirectory(
        prefix="synor-revocation-demo-", ignore_cleanup_errors=ignore_cleanup
    ) as engine_root:
        first_report, second_report = await _run_controlled_apps(
            pathlib.Path(engine_root)
        )

    if (
        first_report.status is not syn.ExecutionStatus.DEGRADED
        or second_report.status is not syn.ExecutionStatus.DEGRADED
    ):
        raise AssertionError("the intentionally blocked partial case was not reported")
    closed = await controller.get_case(request.case_id)
    if closed is None or closed.stage is not revocation.RevocationStage.CLOSED:
        raise AssertionError("engine commit did not close the verified case")
    if not (
        len(suppressed_counts)
        == len(suppressed_scored_counts)
        == len(stale_verification_stages)
        == len(receipt_chains)
        == len(point_ids_before_partial)
        == len(partial_cases)
        == 1
    ):
        raise AssertionError("controlled app did not record one deterministic result")
    receipts = receipt_chains[0]
    partial_case = partial_cases[0]
    suppressed_results = suppressed_counts[0]
    suppressed_scored = suppressed_scored_counts[0]
    stale_verification_stage = stale_verification_stages[0]
    partial_deleted_points = len(point_ids_before_partial[0] - index.point_ids)

    index.restore(governed_points)
    restored_raw_points = len(
        index.points_for_source(governed.identity.evidence_digest())
    )
    restored = await index.guarded_query(
        "launch checklist",
        guard=guard,
        context=_context(governed),
    )
    if restored or index.last_scored_ids:
        raise AssertionError("restored stale points became retrievable")
    unaffected = await index.guarded_query(
        "independent tenant",
        guard=guard,
        context=_context(control),
    )
    if not unaffected:
        raise AssertionError("unaffected tenant stopped being retrievable")

    evidence_keys = await _assert_private_evidence(store)
    return DemoResult(
        controlled_passes=2,
        startup_ready=startup_health.ready,
        startup_safe_error_code=startup_health.safe_error_code,
        runtime_status=second_report.status.value,
        execution_guarantee=second_report.execution_guarantee.value,
        case_id=request.case_id,
        partial_case_id=partial_request.case_id,
        initial_alpha_results=len(initial_alpha),
        initial_beta_results=len(initial_beta),
        suppressed_results=suppressed_results,
        suppressed_scored=suppressed_scored,
        stale_verification_stage=stale_verification_stage,
        final_stage=closed.stage.value,
        receipt_count=len(receipts),
        partial_stage=partial_case.stage.value,
        partial_deleted_points=partial_deleted_points,
        restored_raw_points=restored_raw_points,
        restored_guarded_results=len(restored),
        unaffected_results=len(unaffected),
        effective_deletes=index.effective_deletes,
        evidence_keys=evidence_keys,
        content_unchanged=True,
    )


async def _real_configuration_probe() -> dict[str, object]:
    if typing.TYPE_CHECKING:
        from .real_mode import configure_real_components
    elif __package__:
        from .real_mode import configure_real_components
    else:
        from real_mode import configure_real_components

    store = syn.state_store_from_env()
    runtime = syn.SynorRuntime(
        state_store=store,
        revocation_policy=revocation.RevocationPolicy.strict_query_verified(),
    )
    health = await runtime.revocation_health()
    if health.safe_error_code in {
        "revocation.state_corrupt",
        "revocation.state_unavailable",
    }:
        raise RuntimeError("strict runtime could not read its control-plane state")
    controller = runtime.revocation_controller
    configured = configure_real_components(
        state_store=store,
        suppression_lookup=controller.suppression,
    )
    return {
        "mode": "real_configuration_only",
        "source": type(configured.source).__name__,
        "target": type(configured.target).__name__,
        "declared_target_capabilities": configured.target.capabilities.to_dict(),
        "execution_guarantee": runtime.execution_guarantee.value,
        "live_certified": False,
        "next_step": "run documented disposable Drive/Qdrant acceptance",
    }


async def _main() -> None:
    mode = os.getenv("SYNOR_REVOCATION_DEMO_MODE", "fake").strip().lower()
    if mode == "fake":
        state_store = os.getenv(
            "SYNOR_STATE_STORE",
            "file://.synor/control",
        )
        if not state_store.startswith("file://"):
            raise ValueError("fake demo requires a file:// SYNOR_STATE_STORE")
        demo_result = await run_fake_scenario(
            pathlib.Path(state_store.removeprefix("file://"))
        )
        payload = demo_result.to_dict()
    elif mode == "real":
        payload = await _real_configuration_probe()
    else:
        raise ValueError("SYNOR_REVOCATION_DEMO_MODE must be fake or real")
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(_main())
