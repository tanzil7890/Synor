from __future__ import annotations

import datetime
import hashlib
import pathlib
from collections.abc import Collection, Sequence
from dataclasses import dataclass, replace
from typing import Any

import pytest

import synor as syn
from synor import state
from synor._internal.context_keys import ContextProvider
from synor._internal.retrieval_guard import (
    AccessPolicy,
    GuardedInMemoryRetriever,
    InMemoryAccessPolicyLookup,
    RetrievalCandidate,
    RetrievalContext,
    RetrievalGuard,
)
from synor._internal.revocation_ledger import StateStoreRevocationLedger
from synor._internal.revocation_model import (
    AccessSnapshot,
    EffectDescriptor,
    EffectOperation,
    RevocationPolicyDecision,
    RevocationStage,
    SafeRevocationErrorCode,
    SnapshotResult,
    SourceEventKind,
    SourceIdentity,
    TargetRevocationCapabilities,
    VerificationOutcome,
    make_observation_id,
    make_tenant_digest,
    transition_case,
)
from synor._internal.revocation_policy import RevocationPolicy
from synor._internal.revocation_runtime import (
    LifecycleBoundary,
    RevocationRequest,
    RevocationRuntime,
    RevocationRuntimeStateError,
    TargetObligation,
)
from synor._internal.suppression import (
    StateStoreSuppressionIndex,
    SuppressionGenerationConflict,
)
from synor._internal.verified_sink import (
    TargetVerificationError,
    TargetVerificationOutcome,
    TargetVerificationResult,
    VerificationRetryPolicy,
    VerifiedTargetActionSink,
)
from tests import common


_NOW = datetime.datetime(2026, 7, 29, 12, 0, tzinfo=datetime.timezone.utc)
_TENANT_DIGEST = make_tenant_digest("tenant-a")
_POLICY_DIGEST = hashlib.sha256(b"policy-a-v1").hexdigest()
_TARGET_INSTANCE_DIGEST = hashlib.sha256(b"synthetic-index").hexdigest()


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _capabilities(
    *,
    negative_read_verification: bool = True,
) -> TargetRevocationCapabilities:
    return TargetRevocationCapabilities(
        atomic_serving_suppression=True,
        exact_id_delete=True,
        source_id_bulk_delete=True,
        query_time_acl_filter=True,
        tenant_isolation=True,
        synchronous_acknowledgement=True,
        consistency_fence=True,
        negative_read_verification=negative_read_verification,
        external_enumeration=True,
        legal_hold_isolation=True,
        physical_erasure_attestation=False,
    )


def _obligation(
    identity: SourceIdentity,
    *,
    operation: EffectOperation = EffectOperation.DELETE,
    capabilities: TargetRevocationCapabilities | None = None,
) -> TargetObligation:
    resolved_capabilities = _capabilities() if capabilities is None else capabilities
    return TargetObligation(
        target_provider_id="synthetic-index",
        target_instance_digest=_TARGET_INSTANCE_DIGEST,
        target_locator_digest=_digest(
            f"locator:{identity.connector_instance_id}:"
            f"{identity.source_scope_id}:{identity.item_id}"
        ),
        operation_kind=operation,
        proof_capabilities=resolved_capabilities,
        capabilities=resolved_capabilities,
        verifier_kind="exact-id-query",
        consistency_contract="strong-read",
    )


def _request(
    identity: SourceIdentity,
    *,
    generation: int = 2,
    decision: RevocationPolicyDecision = RevocationPolicyDecision.RESTRICT,
    reason: SourceEventKind = SourceEventKind.ACL_CHANGED,
    operation: EffectOperation = EffectOperation.DELETE,
    capabilities: TargetRevocationCapabilities | None = None,
    provider_missing: bool = False,
    snapshot: SnapshotResult | None = None,
    require_complete_snapshot: bool = False,
    observation_generation: str = "event-2",
    legal_state: str | None = None,
) -> RevocationRequest:
    access = AccessSnapshot(
        tenant_id="tenant-a",
        policy_id="policy-a",
        policy_revision="policy-v1",
        policy_digest=_POLICY_DIGEST,
        group_graph_revision="groups-v1",
        legal_state=legal_state,
    )
    observation_id = make_observation_id(
        identity,
        "content-revision-1",
        reason,
        access,
        observation_generation=observation_generation,
    )
    obligation = _obligation(
        identity,
        operation=operation,
        capabilities=capabilities,
    )
    if provider_missing:
        obligation = replace(obligation, capabilities=None)
    return RevocationRequest(
        identity=identity,
        observation_id=observation_id,
        source_revision="content-revision-1",
        access=access,
        observation_generation=observation_generation,
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy-a",
        policy_revision="policy-v1",
        policy_digest=_POLICY_DIGEST,
        group_graph_revision="groups-v1",
        reason=reason,
        policy_decision=decision,
        suppression_generation=generation,
        observed_at=_NOW,
        suppress_by=_NOW,
        verify_by=_NOW + datetime.timedelta(minutes=5),
        obligations=(obligation,),
        snapshot=snapshot,
        require_complete_snapshot=require_complete_snapshot,
    )


def _runtime(
    store: state.StateStore,
    *,
    boundary_hook: Any = None,
) -> tuple[
    RevocationRuntime,
    StateStoreRevocationLedger,
    StateStoreSuppressionIndex,
]:
    ledger = StateStoreRevocationLedger(store)
    suppression = StateStoreSuppressionIndex(store)
    runtime = RevocationRuntime(
        ledger=ledger,
        suppression=suppression,
        policy=RevocationPolicy._for_test(),
        boundary_hook=boundary_hook,
        clock=lambda: _NOW + datetime.timedelta(seconds=30),
    )
    return runtime, ledger, suppression


@dataclass(frozen=True, slots=True)
class _DeleteAction:
    descriptor: EffectDescriptor

    def __synor_effect_descriptor__(self) -> EffectDescriptor:
        return self.descriptor


class _SyntheticIndex:
    """External store whose mutation is idempotent by stable action ID."""

    def __init__(self, source_digests: set[str]) -> None:
        self.source_digests = set(source_digests)
        self.source_generations = {source_digest: 0 for source_digest in source_digests}
        self.isolated_source_digests: set[str] = set()
        self.applied_action_ids: set[str] = set()
        self.apply_calls = 0
        self.effective_deletes = 0
        self.false_success = False

    def apply(self, action: _DeleteAction) -> None:
        self.apply_calls += 1
        if self.false_success:
            return
        descriptor = action.descriptor
        current_generation = self.source_generations.get(
            descriptor.source_digest,
            0,
        )
        if descriptor.source_generation < current_generation:
            return
        self.source_generations[descriptor.source_digest] = descriptor.source_generation
        if descriptor.action_id in self.applied_action_ids:
            return
        self.applied_action_ids.add(descriptor.action_id)
        if descriptor.operation_kind is EffectOperation.ISOLATE:
            if descriptor.source_digest in self.source_digests:
                self.isolated_source_digests.add(descriptor.source_digest)
            return
        if descriptor.source_digest in self.source_digests:
            self.source_digests.remove(descriptor.source_digest)
            self.isolated_source_digests.discard(descriptor.source_digest)
            self.effective_deletes += 1

    def authorize_generation(self, source_digest: str, generation: int) -> None:
        self.source_digests.add(source_digest)
        self.isolated_source_digests.discard(source_digest)
        self.source_generations[source_digest] = generation

    def is_servable(self, source_digest: str) -> bool:
        return (
            source_digest in self.source_digests
            and source_digest not in self.isolated_source_digests
        )

    def outcome(
        self, action: _DeleteAction, *, attempt: int
    ) -> TargetVerificationOutcome:
        descriptor = action.descriptor
        if descriptor.operation_kind is EffectOperation.ISOLATE:
            status = (
                VerificationOutcome.RETAINED_ISOLATED
                if descriptor.source_digest in self.isolated_source_digests
                else VerificationOutcome.PRESENT
            )
        else:
            status = (
                VerificationOutcome.PRESENT
                if descriptor.source_digest in self.source_digests
                else VerificationOutcome.ABSENT
            )
        return TargetVerificationOutcome(
            action_id=descriptor.action_id,
            operation=descriptor.operation_kind,
            source_digest=descriptor.source_digest,
            source_generation=descriptor.source_generation,
            target_locator_digest=descriptor.target_locator_digest,
            status=status,
            attempt_count=attempt,
        )


def _policy() -> AccessPolicy:
    return AccessPolicy(
        policy_id="policy-a",
        tenant_id="tenant-a",
        revision="policy-v1",
        group_graph_revision="groups-v1",
        allowed_principal_ids=frozenset({"principal-a"}),
    )


def _candidate(
    identity: SourceIdentity,
    label: str,
    *,
    source_generation: int = 1,
) -> RetrievalCandidate[str]:
    return RetrievalCandidate(
        candidate_id=label,
        source_digest=identity.evidence_digest(),
        source_generation=source_generation,
        tenant_id="tenant-a",
        policy_id="policy-a",
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        payload=f"unchanged-content:{label}",
    )


def _retrieval_context() -> RetrievalContext:
    return RetrievalContext(
        tenant_id="tenant-a",
        principal_id="principal-a",
        group_graph_revision="groups-v1",
    )


@pytest.mark.asyncio
async def test_two_item_synor_lifecycle_suppresses_before_false_success_and_retries() -> (
    None
):
    """Exercise the Phase 2 lifecycle through Synor's real target callback."""

    store = state.MemoryStateStore()
    observed_boundaries: list[LifecycleBoundary] = []

    async def observe_boundary(
        boundary: LifecycleBoundary,
        case_id: str,
        obligation_id: str | None,
    ) -> None:
        del case_id, obligation_id
        observed_boundaries.append(boundary)

    runtime, ledger, suppression = _runtime(
        store,
        boundary_hook=observe_boundary,
    )
    identity_a = SourceIdentity("source", "scope", "item-a")
    identity_b = SourceIdentity("source", "scope", "item-b")
    request = _request(identity_a)
    await suppression.authorize(
        source_digest=identity_a.evidence_digest(),
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy-a",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=_NOW - datetime.timedelta(minutes=1),
    )
    await suppression.authorize(
        source_digest=identity_b.evidence_digest(),
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy-a",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=_NOW - datetime.timedelta(minutes=1),
    )
    index = _SyntheticIndex(set())
    include_a = True
    previous_deleted_states: list[tuple[str, ...]] = []
    delete_action: _DeleteAction | None = None
    verified_sink: VerifiedTargetActionSink[_DeleteAction, None] | None = None

    async def apply_upserts(
        context_provider: ContextProvider,
        actions: Sequence[tuple[str, str]],
        /,
    ) -> None:
        del context_provider
        for source_digest, _desired_state in actions:
            index.authorize_generation(source_digest, 1)

    upsert_sink = syn.TargetActionSink.from_async_fn(apply_upserts)

    class _Handler:
        def reconcile(
            self,
            key: syn.StableKey,
            desired_state: str | syn.AbsentType,
            prev_possible_records: Collection[str],
            prev_may_be_missing: bool,
            /,
        ) -> syn.TargetReconcileOutput[Any, str] | None:
            assert isinstance(key, str)
            if syn.is_absent(desired_state):
                if not prev_possible_records:
                    return None
                if key != "artifact-a":
                    raise AssertionError("only the revoked artifact may disappear")
                if delete_action is None or verified_sink is None:
                    raise AssertionError("strict delete was not configured")
                previous_deleted_states.append(tuple(prev_possible_records))
                return syn.TargetReconcileOutput(
                    action=delete_action,
                    sink=verified_sink.sink,
                    tracking_record=syn.ABSENT,
                )

            if (
                prev_possible_records
                and not prev_may_be_missing
                and all(previous == desired_state for previous in prev_possible_records)
            ):
                return None
            source_digest = {
                "artifact-a": identity_a.evidence_digest(),
                "artifact-b": identity_b.evidence_digest(),
            }[key]
            return syn.TargetReconcileOutput(
                action=(source_digest, desired_state),
                sink=upsert_sink,
                tracking_record=desired_state,
            )

    provider = syn.register_root_target_states_provider(
        "test/revocation/vertical_slice/two_item",
        _Handler(),
    )

    async def main() -> None:
        if include_a:
            syn.ensure_target_state(provider.target_state("artifact-a", "owned-a"))
        syn.ensure_target_state(provider.target_state("artifact-b", "owned-b"))

    environment = common.create_test_env(__file__, suffix="strict_vertical_slice")
    app = syn.App(
        syn.AppConfig(name="strict_vertical_slice", environment=environment),
        main,
    )

    # Both derivatives first enter the real Synor ownership/tracking graph.
    await app.update()
    assert index.source_digests == {
        identity_a.evidence_digest(),
        identity_b.evidence_digest(),
    }

    candidates = (_candidate(identity_a, "a"), _candidate(identity_b, "b"))
    guard = RetrievalGuard(
        suppression_lookup=suppression,
        policy_lookup=InMemoryAccessPolicyLookup((_policy(),)),
    )
    scored_ids: list[str] = []

    def score(
        query: str,
        candidate: RetrievalCandidate[str],
    ) -> float:
        scored_ids.append(candidate.candidate_id)
        return float(len(query))

    retriever: GuardedInMemoryRetriever[str, str] = GuardedInMemoryRetriever(
        candidates=candidates,
        guard=guard,
        scorer=score,
    )
    before = await retriever.search("query", context=_retrieval_context())
    assert [item.candidate.candidate_id for item in before] == ["a", "b"]

    case = await runtime.begin_case(request)
    assert case.stage is RevocationStage.PLANNED
    # Query denial happens before the target has even received a delete.
    scored_ids.clear()
    suppressed_results = await retriever.search("query", context=_retrieval_context())
    assert [item.candidate.candidate_id for item in suppressed_results] == ["b"]
    assert scored_ids == ["b"]

    (descriptor,) = await runtime.descriptors_for(case.case_id)
    delete_action = _DeleteAction(descriptor)
    index.false_success = True
    attempt = 1

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_DeleteAction],
        /,
    ) -> None:
        del context_provider
        for target_action in actions:
            await runtime.notify_synor_precommit(
                case.case_id,
                target_action.descriptor.action_id,
            )
            index.apply(target_action)
            await runtime.notify_target_effect_applied(
                case.case_id,
                target_action.descriptor.action_id,
            )
        await runtime.mark_target_applied(case.case_id, descriptor.action_id)

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_DeleteAction],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, applied
        await runtime.mark_acknowledged(case.case_id, descriptor.action_id)
        return [
            TargetVerificationResult(
                status=index.outcome(
                    target_action,
                    attempt=attempt,
                ).status,
                action_id=target_action.descriptor.action_id,
            )
            for target_action in actions
        ]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        await runtime.record_outcomes(
            case.case_id,
            outcomes,
            attempt=attempt,
            attempted_at=_NOW + datetime.timedelta(seconds=attempt),
        )

    verified_sink = VerifiedTargetActionSink[_DeleteAction, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=VerificationRetryPolicy(
            timeout=datetime.timedelta(seconds=1),
            max_attempts=1,
            initial_backoff=0,
            max_backoff=0,
            jitter=0,
        ),
    )

    # Removing only A from desired state forces the real engine cleanup path.
    include_a = False
    with pytest.raises(TargetVerificationError):
        await app.update()
    failed = await ledger.get_case(case.case_id)
    assert failed is not None
    assert failed.stage is RevocationStage.FAILED
    assert failed.safe_error_code == SafeRevocationErrorCode.TARGET_PRESENT.value
    assert identity_a.evidence_digest() in index.source_digests

    index.false_success = False
    attempt = 2
    await runtime.prepare_retry(request)
    await app.update()
    verified_case = await ledger.get_case(case.case_id)
    assert verified_case is not None
    assert verified_case.stage is RevocationStage.VERIFIED
    closed = await runtime.finalize_after_engine_commit(case.case_id)

    assert closed.stage is RevocationStage.CLOSED
    assert index.effective_deletes == 1
    assert identity_a.evidence_digest() not in index.source_digests
    assert identity_b.evidence_digest() in index.source_digests
    assert previous_deleted_states == [("owned-a",), ("owned-a",)]
    assert LifecycleBoundary.SYNOR_PRECOMMIT in observed_boundaries
    assert LifecycleBoundary.TARGET_APPLIED in observed_boundaries
    assert observed_boundaries[-2:] == [
        LifecycleBoundary.ENGINE_FINAL_COMMIT,
        LifecycleBoundary.CASE_SUMMARY_UPDATED,
    ]
    receipts = await ledger.list_receipts(case.case_id)
    assert [receipt.observed_outcome for receipt in receipts] == [
        VerificationOutcome.PRESENT.value,
        VerificationOutcome.ABSENT.value,
    ]
    assert [receipt.attempt for receipt in receipts] == [1, 2]
    after = await retriever.search("query", context=_retrieval_context())
    assert [item.candidate.candidate_id for item in after] == ["b"]


class _InjectedCrash(RuntimeError):
    pass


class _FailReceiptOnceStore:
    def __init__(self) -> None:
        self.underlying = state.MemoryStateStore()
        self.failed = False

    async def get(self, key: str) -> bytes | None:
        return await self.underlying.get(key)

    async def put(self, key: str, value: bytes) -> None:
        if not self.failed and key.startswith("revocation/v1/receipts/"):
            self.failed = True
            raise OSError("planted receipt write interruption")
        await self.underlying.put(key, value)

    async def delete(self, key: str) -> bool:
        return await self.underlying.delete(key)

    async def list(self, prefix: str = "") -> tuple[str, ...]:
        return await self.underlying.list(prefix)


class _FailReceiptAfterWriteOnceStore(_FailReceiptOnceStore):
    async def put(self, key: str, value: bytes) -> None:
        if not self.failed and key.startswith("revocation/v1/receipts/"):
            self.failed = True
            await self.underlying.put(key, value)
            raise OSError("planted lost receipt-write response")
        await self.underlying.put(key, value)


class _FailSuppressionOnceStore:
    def __init__(self) -> None:
        self.underlying = state.MemoryStateStore()
        self.armed = False
        self.failed = False

    async def get(self, key: str) -> bytes | None:
        return await self.underlying.get(key)

    async def put(self, key: str, value: bytes) -> None:
        if (
            self.armed
            and not self.failed
            and key.startswith("revocation/v1/suppression/")
        ):
            self.failed = True
            raise OSError("planted suppression write interruption")
        await self.underlying.put(key, value)

    async def delete(self, key: str) -> bool:
        return await self.underlying.delete(key)

    async def list(self, prefix: str = "") -> tuple[str, ...]:
        return await self.underlying.list(prefix)


@pytest.mark.asyncio
async def test_receipt_write_failure_keeps_verified_effect_case_open_for_retry() -> (
    None
):
    store = _FailReceiptOnceStore()
    runtime, _, _ = _runtime(store)
    identity = SourceIdentity("source", "scope", "receipt-failure")
    request = _request(identity)
    case = await runtime.begin_case(request)
    (descriptor,) = await runtime.descriptors_for(case.case_id)
    await runtime.mark_target_applied(case.case_id, descriptor.action_id)
    await runtime.mark_acknowledged(case.case_id, descriptor.action_id)
    outcome = TargetVerificationOutcome(
        action_id=descriptor.action_id,
        operation=descriptor.operation_kind,
        source_digest=descriptor.source_digest,
        source_generation=descriptor.source_generation,
        target_locator_digest=descriptor.target_locator_digest,
        status=VerificationOutcome.ABSENT,
        attempt_count=1,
    )

    with pytest.raises(OSError, match="receipt write"):
        await runtime.record_outcomes(
            case.case_id,
            (outcome,),
            attempt=1,
            attempted_at=_NOW + datetime.timedelta(seconds=1),
        )

    still_open = await runtime.get_case(case.case_id)
    assert still_open is not None
    assert still_open.stage is RevocationStage.ACKNOWLEDGED

    recovered, ledger, _ = _runtime(store.underlying)
    await recovered.begin_case(request)
    terminal = await recovered.record_outcomes(
        case.case_id,
        (outcome,),
        attempt=1,
        attempted_at=_NOW + datetime.timedelta(seconds=1),
    )
    assert terminal.stage is RevocationStage.VERIFIED
    assert (await recovered.finalize_after_engine_commit(case.case_id)).stage is (
        RevocationStage.CLOSED
    )
    assert len(await ledger.list_receipts(case.case_id)) == 1


@pytest.mark.asyncio
async def test_lost_receipt_write_response_repairs_and_deduplicates() -> None:
    store = _FailReceiptAfterWriteOnceStore()
    runtime, _, _ = _runtime(store)
    identity = SourceIdentity("source", "scope", "receipt-lost-response")
    request = _request(identity)
    case = await runtime.begin_case(request)
    (descriptor,) = await runtime.descriptors_for(case.case_id)
    await runtime.mark_target_applied(case.case_id, descriptor.action_id)
    await runtime.mark_acknowledged(case.case_id, descriptor.action_id)
    outcome = TargetVerificationOutcome(
        action_id=descriptor.action_id,
        operation=descriptor.operation_kind,
        source_digest=descriptor.source_digest,
        source_generation=descriptor.source_generation,
        target_locator_digest=descriptor.target_locator_digest,
        status=VerificationOutcome.ABSENT,
        attempt_count=1,
    )

    with pytest.raises(OSError, match="lost receipt"):
        await runtime.record_outcomes(
            case.case_id,
            (outcome,),
            attempt=1,
            attempted_at=_NOW + datetime.timedelta(seconds=1),
        )

    receipt_keys = await store.underlying.list(
        f"revocation/v1/receipts/{case.case_id}/"
    )
    assert len(receipt_keys) == 1
    assert (
        await store.underlying.get(f"revocation/v1/receipt_heads/{case.case_id}.json")
        is None
    )

    recovered, ledger, _ = _runtime(store.underlying)
    report = await ledger.repair()
    assert report.receipt_heads_rebuilt == 1
    await recovered.begin_case(request)
    terminal = await recovered.record_outcomes(
        case.case_id,
        (outcome,),
        attempt=1,
        attempted_at=_NOW + datetime.timedelta(seconds=1),
    )

    assert terminal.stage is RevocationStage.VERIFIED
    assert len(await ledger.list_receipts(case.case_id)) == 1


class _CrashOnce:
    def __init__(self, boundary: LifecycleBoundary) -> None:
        self.boundary = boundary
        self.injected = False

    async def __call__(
        self,
        boundary: LifecycleBoundary,
        case_id: str,
        obligation_id: str | None,
    ) -> None:
        del case_id, obligation_id
        if not self.injected and boundary is self.boundary:
            self.injected = True
            raise _InjectedCrash(boundary.value)


@pytest.mark.asyncio
async def test_suppression_write_failure_retains_process_serving_fence() -> None:
    store = _FailSuppressionOnceStore()
    runtime, ledger, suppression = _runtime(store)
    identity = SourceIdentity("source", "scope", "suppression-write-failure")
    request = _request(identity)
    await suppression.authorize(
        source_digest=identity.evidence_digest(),
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy-a",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=_NOW - datetime.timedelta(minutes=1),
    )
    store.armed = True

    with pytest.raises(OSError, match="suppression write"):
        await runtime.begin_case(request)

    observed = await ledger.get_case(request.case_id)
    assert observed is not None
    assert observed.stage is RevocationStage.OBSERVED
    scored_ids: list[str] = []

    def score(
        query: str,
        candidate: RetrievalCandidate[str],
    ) -> float:
        del query
        scored_ids.append(candidate.candidate_id)
        return 1.0

    retriever: GuardedInMemoryRetriever[str, str] = GuardedInMemoryRetriever(
        candidates=(_candidate(identity, "old-authorized-generation"),),
        guard=RetrievalGuard(
            suppression_lookup=StateStoreSuppressionIndex(store),
            policy_lookup=InMemoryAccessPolicyLookup((_policy(),)),
        ),
        scorer=score,
    )
    assert await retriever.search("query", context=_retrieval_context()) == ()
    assert scored_ids == []

    planned = await runtime.begin_case(request)
    assert planned.stage is RevocationStage.PLANNED
    assert await suppression.is_suppressed(identity.evidence_digest())


@pytest.mark.asyncio
async def test_observation_boundary_failure_retains_durable_serving_fence(
    tmp_path: pathlib.Path,
) -> None:
    store_path = tmp_path / "observation-boundary-serving-fence"
    store = state.FileStateStore(store_path)
    runtime, _, suppression = _runtime(
        store,
        boundary_hook=_CrashOnce(LifecycleBoundary.OBSERVATION_PERSISTED),
    )
    identity = SourceIdentity("source", "scope", "observation-boundary-failure")
    request = _request(identity)
    await suppression.authorize(
        source_digest=identity.evidence_digest(),
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy-a",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=_NOW - datetime.timedelta(minutes=1),
    )

    with pytest.raises(_InjectedCrash, match="observation_persisted"):
        await runtime.begin_case(request)

    # Reconstruct the store facade before recovery. The old authorization must
    # already be denied by the crash-surviving pre-observation fence.
    recovered_store = state.FileStateStore(store_path)
    scored_ids: list[str] = []

    def score(
        query: str,
        candidate: RetrievalCandidate[str],
    ) -> float:
        del query
        scored_ids.append(candidate.candidate_id)
        return 1.0

    retriever: GuardedInMemoryRetriever[str, str] = GuardedInMemoryRetriever(
        candidates=(_candidate(identity, "old-authorized-generation"),),
        guard=RetrievalGuard(
            suppression_lookup=StateStoreSuppressionIndex(recovered_store),
            policy_lookup=InMemoryAccessPolicyLookup((_policy(),)),
        ),
        scorer=score,
    )
    assert await retriever.search("query", context=_retrieval_context()) == ()
    assert scored_ids == []

    recovered, _, recovered_suppression = _runtime(recovered_store)
    assert (await recovered.begin_case(request)).stage is RevocationStage.PLANNED
    assert await recovered_suppression.is_suppressed(identity.evidence_digest())


@pytest.mark.asyncio
async def test_same_generation_authorization_conflict_keeps_serving_fence() -> None:
    store = state.MemoryStateStore()
    runtime, _, suppression = _runtime(store)
    identity = SourceIdentity("source", "scope", "same-generation-conflict")
    request = _request(identity, generation=2)
    await suppression.authorize(
        source_digest=identity.evidence_digest(),
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy-a",
        generation=2,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=_NOW - datetime.timedelta(minutes=1),
    )

    with pytest.raises(SuppressionGenerationConflict, match="conflicting"):
        await runtime.begin_case(request)

    scored_ids: list[str] = []

    def score(
        query: str,
        candidate: RetrievalCandidate[str],
    ) -> float:
        del query
        scored_ids.append(candidate.candidate_id)
        return 1.0

    retriever: GuardedInMemoryRetriever[str, str] = GuardedInMemoryRetriever(
        candidates=(
            _candidate(
                identity,
                "conflicting-authorized-generation",
                source_generation=2,
            ),
        ),
        guard=RetrievalGuard(
            suppression_lookup=suppression,
            policy_lookup=InMemoryAccessPolicyLookup((_policy(),)),
        ),
        scorer=score,
    )
    assert await retriever.search("query", context=_retrieval_context()) == ()
    assert scored_ids == []


async def _converge(
    runtime: RevocationRuntime,
    request: RevocationRequest,
    index: _SyntheticIndex,
) -> None:
    case = await runtime.begin_case(request)
    if case.stage is RevocationStage.CLOSED:
        return
    if case.stage in {RevocationStage.FAILED, RevocationStage.BLOCKED}:
        case = await runtime.prepare_retry(request)
    (descriptor,) = await runtime.descriptors_for(case.case_id)
    action = _DeleteAction(descriptor)
    persisted = await runtime.get_case(case.case_id)
    assert persisted is not None
    case = persisted
    if case.stage is RevocationStage.PLANNED:
        await runtime.notify_synor_precommit(
            case.case_id,
            descriptor.action_id,
        )
        index.apply(action)
        await runtime.notify_target_effect_applied(
            case.case_id,
            descriptor.action_id,
        )
        case = await runtime.mark_target_applied(
            case.case_id,
            descriptor.action_id,
        )
    if case.stage is RevocationStage.DISPATCHED:
        case = await runtime.mark_acknowledged(
            case.case_id,
            descriptor.action_id,
        )
    if case.stage is RevocationStage.ACKNOWLEDGED:
        case = await runtime.record_outcomes(
            case.case_id,
            (index.outcome(action, attempt=1),),
            attempt=1,
            attempted_at=_NOW + datetime.timedelta(seconds=1),
        )
    if case.stage in {
        RevocationStage.VERIFIED,
        RevocationStage.RETAINED_ISOLATED,
    }:
        await runtime.finalize_after_engine_commit(case.case_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", tuple(LifecycleBoundary))
async def test_restart_from_every_lifecycle_boundary_is_idempotent(
    boundary: LifecycleBoundary,
    tmp_path: pathlib.Path,
) -> None:
    store_path = tmp_path / boundary.value
    store = state.FileStateStore(store_path)
    crash = _CrashOnce(boundary)
    runtime, _, _ = _runtime(store, boundary_hook=crash)
    identity = SourceIdentity("source", "scope", f"item-{boundary.value}")
    request = _request(identity)
    index = _SyntheticIndex({identity.evidence_digest()})

    with pytest.raises(_InjectedCrash, match=boundary.value):
        await _converge(runtime, request, index)

    # Recreate every in-process coordinator over the same durable store.
    recovered_store = state.FileStateStore(store_path)
    recovered, ledger, _ = _runtime(recovered_store)
    await ledger.repair()
    await _converge(recovered, request, index)

    case = await ledger.get_case(request.case_id)
    assert case is not None
    assert case.stage is RevocationStage.CLOSED
    assert index.effective_deletes == 1
    assert index.apply_calls == (
        2 if boundary is LifecycleBoundary.TARGET_APPLIED else 1
    )
    receipts = await ledger.list_receipts(case.case_id)
    terminal = [
        receipt
        for receipt in receipts
        if receipt.observed_outcome == VerificationOutcome.ABSENT.value
    ]
    assert len(terminal) == 1
    assert terminal[0].obligation_id == case.expected_targets[0]


@pytest.mark.asyncio
async def test_partial_snapshot_blocks_cleanup_after_typed_revocation_suppression() -> (
    None
):
    store = state.MemoryStateStore()
    runtime, _, suppression = _runtime(store)
    identity = SourceIdentity("source", "scope", "partial-item")
    partial = SnapshotResult(
        connector_instance_id="source",
        source_scope_id="scope",
        epoch="epoch-1",
        cursor_before="cursor-1",
        cursor_after=None,
        status="partial",
        item_count=4,
        inaccessible_scope_digests=(_digest("inaccessible"),),
    )
    request = _request(
        identity,
        snapshot=partial,
        require_complete_snapshot=True,
    )
    await suppression.authorize(
        source_digest=identity.evidence_digest(),
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy-a",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=_NOW - datetime.timedelta(minutes=1),
    )
    scored_ids: list[str] = []

    def score(
        query: str,
        candidate: RetrievalCandidate[str],
    ) -> float:
        del query
        scored_ids.append(candidate.candidate_id)
        return 1.0

    case = await runtime.begin_case(request)
    retriever: GuardedInMemoryRetriever[str, str] = GuardedInMemoryRetriever(
        candidates=(_candidate(identity, "previously-authorized"),),
        guard=RetrievalGuard(
            suppression_lookup=suppression,
            policy_lookup=InMemoryAccessPolicyLookup((_policy(),)),
        ),
        scorer=score,
    )
    results = await retriever.search("query", context=_retrieval_context())

    assert case.stage is RevocationStage.BLOCKED
    assert case.safe_error_code == SafeRevocationErrorCode.SNAPSHOT_INCOMPLETE.value
    record = await suppression.get(identity.evidence_digest())
    assert record is not None
    assert record.suppressed
    assert record.generation == request.suppression_generation
    assert record.case_id == case.case_id
    assert results == ()
    assert scored_ids == []


@pytest.mark.asyncio
async def test_complete_matching_snapshot_recovers_a_partial_snapshot_case() -> None:
    store = state.MemoryStateStore()
    runtime, _, suppression = _runtime(store)
    identity = SourceIdentity("source", "scope", "partial-then-complete")
    partial = SnapshotResult(
        connector_instance_id="source",
        source_scope_id="scope",
        epoch="epoch-1",
        cursor_before="cursor-1",
        cursor_after=None,
        status="partial",
        item_count=4,
        inaccessible_scope_digests=(_digest("inaccessible"),),
    )
    blocked_request = _request(
        identity,
        snapshot=partial,
        require_complete_snapshot=True,
    )
    assert (await runtime.begin_case(blocked_request)).stage is (
        RevocationStage.BLOCKED
    )

    complete = SnapshotResult(
        connector_instance_id="source",
        source_scope_id="scope",
        epoch="epoch-1",
        cursor_before="cursor-1",
        cursor_after="cursor-2",
        status="complete",
        item_count=4,
    )
    recovered_request = _request(
        identity,
        snapshot=complete,
        require_complete_snapshot=True,
    )
    planned = await runtime.begin_case(recovered_request)

    assert planned.stage is RevocationStage.PLANNED
    assert await suppression.is_suppressed(identity.evidence_digest())


@pytest.mark.asyncio
async def test_prepare_retry_records_changed_blocker_after_snapshot_failure() -> None:
    store = state.MemoryStateStore()
    runtime, _, _ = _runtime(store)
    identity = SourceIdentity("source", "scope", "changed-retry-blocker")
    partial = SnapshotResult(
        connector_instance_id="source",
        source_scope_id="scope",
        epoch="epoch-1",
        cursor_before="cursor-1",
        cursor_after=None,
        status="partial",
        item_count=4,
        inaccessible_scope_digests=(_digest("inaccessible"),),
    )
    blocked = await runtime.begin_case(
        _request(
            identity,
            snapshot=partial,
            require_complete_snapshot=True,
        )
    )
    assert blocked.safe_error_code == SafeRevocationErrorCode.SNAPSHOT_INCOMPLETE.value
    complete = SnapshotResult(
        connector_instance_id="source",
        source_scope_id="scope",
        epoch="epoch-1",
        cursor_before="cursor-1",
        cursor_after="cursor-2",
        status="complete",
        item_count=4,
    )

    changed = await runtime.prepare_retry(
        _request(
            identity,
            snapshot=complete,
            require_complete_snapshot=True,
            provider_missing=True,
        )
    )

    assert changed.stage is RevocationStage.BLOCKED
    assert changed.safe_error_code == SafeRevocationErrorCode.PROVIDER_MISSING.value
    assert changed.version == blocked.version + 2

    changed_again = await runtime.prepare_retry(
        _request(
            identity,
            snapshot=partial,
            require_complete_snapshot=True,
        )
    )
    assert changed_again.stage is RevocationStage.BLOCKED
    assert (
        changed_again.safe_error_code
        == SafeRevocationErrorCode.SNAPSHOT_INCOMPLETE.value
    )
    assert changed_again.version == changed.version + 2


def test_complete_snapshot_from_another_scope_cannot_authorize_cleanup() -> None:
    identity = SourceIdentity("source", "scope-a", "item")
    unrelated = SnapshotResult(
        connector_instance_id="source",
        source_scope_id="scope-b",
        epoch="epoch-1",
        cursor_before=None,
        cursor_after="cursor-1",
        status="complete",
        item_count=1,
    )

    with pytest.raises(ValueError, match="identity scope"):
        _request(
            identity,
            snapshot=unrelated,
            require_complete_snapshot=True,
        )


def test_request_rejects_observation_not_bound_to_access_snapshot() -> None:
    identity = SourceIdentity("source", "scope", "unbound-observation")
    request = _request(identity)
    ungoverned_observation = make_observation_id(
        identity,
        request.source_revision,
        request.reason,
        observation_generation=request.observation_generation,
    )

    with pytest.raises(ValueError, match="complete governed observation"):
        replace(request, observation_id=ungoverned_observation)


@pytest.mark.asyncio
async def test_non_revocation_event_cannot_be_overridden_to_destroy() -> None:
    store = state.MemoryStateStore()
    runtime, _, suppression = _runtime(store)
    identity = SourceIdentity("source", "scope", "still-present")
    request = _request(
        identity,
        reason=SourceEventKind.PRESENT,
        decision=RevocationPolicyDecision.DESTROY,
    )

    case = await runtime.begin_case(request)

    assert case.stage is RevocationStage.BLOCKED
    assert case.safe_error_code == SafeRevocationErrorCode.POLICY_BLOCKED.value
    assert await suppression.get(identity.evidence_digest()) is None


@pytest.mark.asyncio
async def test_in_place_acl_restriction_is_blocked_until_it_has_readback_semantics() -> (
    None
):
    store = state.MemoryStateStore()
    runtime, _, suppression = _runtime(store)
    identity = SourceIdentity("source", "scope", "restrict-operation")
    request = _request(identity, operation=EffectOperation.RESTRICT)
    await suppression.authorize(
        source_digest=identity.evidence_digest(),
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy-a",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=_NOW - datetime.timedelta(minutes=1),
    )
    scored_ids: list[str] = []

    def score(
        query: str,
        candidate: RetrievalCandidate[str],
    ) -> float:
        del query
        scored_ids.append(candidate.candidate_id)
        return 1.0

    case = await runtime.begin_case(request)
    retriever: GuardedInMemoryRetriever[str, str] = GuardedInMemoryRetriever(
        candidates=(_candidate(identity, "previously-authorized"),),
        guard=RetrievalGuard(
            suppression_lookup=suppression,
            policy_lookup=InMemoryAccessPolicyLookup((_policy(),)),
        ),
        scorer=score,
    )
    results = await retriever.search(
        "query",
        context=_retrieval_context(),
    )

    assert case.stage is RevocationStage.BLOCKED
    assert case.safe_error_code == SafeRevocationErrorCode.POLICY_BLOCKED.value
    assert await suppression.is_suppressed(identity.evidence_digest())
    record = await suppression.get(identity.evidence_digest())
    assert record is not None
    assert record.generation == request.suppression_generation
    assert record.case_id == case.case_id
    assert results == ()
    assert scored_ids == []


@pytest.mark.asyncio
async def test_prepare_retry_restores_suppression_before_policy_block() -> None:
    store = state.MemoryStateStore()
    runtime, ledger, suppression = _runtime(store)
    identity = SourceIdentity("source", "scope", "restrict-operation-retry")
    request = _request(identity, operation=EffectOperation.RESTRICT)
    await suppression.authorize(
        source_digest=identity.evidence_digest(),
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy-a",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=_NOW - datetime.timedelta(minutes=1),
    )
    observed = request.observed_case()
    await ledger.append_case(observed)
    blocked = transition_case(
        observed,
        RevocationStage.BLOCKED,
        safe_error_code=SafeRevocationErrorCode.POLICY_BLOCKED.value,
    )
    await ledger.append_case(blocked)

    retried = await runtime.prepare_retry(request)

    assert retried == blocked
    record = await suppression.get(identity.evidence_digest())
    assert record is not None
    assert record.suppressed
    assert record.generation == request.suppression_generation
    assert record.case_id == blocked.case_id


@pytest.mark.asyncio
async def test_missing_provider_is_suppressed_and_durably_blocked() -> None:
    store = state.MemoryStateStore()
    runtime, _, suppression = _runtime(store)
    identity = SourceIdentity("source", "scope", "missing-provider")
    request = _request(identity, provider_missing=True)

    case = await runtime.begin_case(request)

    assert case.stage is RevocationStage.BLOCKED
    assert case.safe_error_code == SafeRevocationErrorCode.PROVIDER_MISSING.value
    assert await suppression.is_suppressed(identity.evidence_digest())


@pytest.mark.asyncio
async def test_provider_disappearing_after_plan_blocks_apply_and_can_recover() -> None:
    store = state.MemoryStateStore()
    runtime, _, _ = _runtime(store)
    identity = SourceIdentity("source", "scope", "provider-race")
    available = _request(identity)
    planned = await runtime.begin_case(available)
    assert planned.stage is RevocationStage.PLANNED

    missing = _request(identity, provider_missing=True)
    blocked = await runtime.begin_case(missing)
    assert blocked.stage is RevocationStage.BLOCKED
    assert blocked.safe_error_code == SafeRevocationErrorCode.PROVIDER_MISSING.value
    with pytest.raises(RevocationRuntimeStateError, match="current case stage"):
        await runtime.descriptors_for(blocked.case_id)

    recovered = await runtime.prepare_retry(available)
    assert recovered.stage is RevocationStage.PLANNED
    (descriptor,) = await runtime.descriptors_for(recovered.case_id)
    assert descriptor.source_generation == available.suppression_generation


@pytest.mark.asyncio
async def test_restart_cannot_replace_persisted_target_proof_contract() -> None:
    store = state.MemoryStateStore()
    runtime, ledger, _ = _runtime(store)
    identity = SourceIdentity("source", "scope", "proof-contract")
    original = _request(identity)
    planned = await runtime.begin_case(original)
    original_action_id = planned.expected_targets[0]
    changed_obligation = replace(
        original.obligations[0],
        verifier_kind="different-verifier",
    )
    changed = replace(original, obligations=(changed_obligation,))

    with pytest.raises(
        RevocationRuntimeStateError,
        match="does not match the request",
    ):
        await runtime.begin_case(changed)

    persisted = await ledger.get_case(planned.case_id)
    assert persisted is not None
    assert persisted.expected_targets == (original_action_id,)
    assert changed.expected_obligation_ids != persisted.expected_targets
    recovered = await runtime.prepare_retry(original)
    assert recovered.stage is RevocationStage.PLANNED


@pytest.mark.asyncio
async def test_provider_capability_profile_must_match_persisted_proof_contract() -> (
    None
):
    store = state.MemoryStateStore()
    runtime, _, _ = _runtime(store)
    identity = SourceIdentity("source", "scope", "capability-contract")
    original = _request(identity)
    obligation = original.obligations[0]
    drifted_capabilities = replace(
        obligation.proof_capabilities,
        capability_version="2",
    )
    drifted = replace(
        original,
        obligations=(
            replace(
                obligation,
                capabilities=drifted_capabilities,
            ),
        ),
    )

    case = await runtime.begin_case(drifted)

    assert case.stage is RevocationStage.BLOCKED
    assert case.safe_error_code == SafeRevocationErrorCode.CAPABILITY_UNSUPPORTED.value


@pytest.mark.asyncio
async def test_unsupported_capability_blocks_before_target_materialization() -> None:
    store = state.MemoryStateStore()
    runtime, _, _ = _runtime(store)
    identity = SourceIdentity("source", "scope", "unsupported-target")
    request = _request(
        identity,
        capabilities=_capabilities(negative_read_verification=False),
    )
    index = _SyntheticIndex({identity.evidence_digest()})

    case = await runtime.begin_case(request)

    assert case.stage is RevocationStage.BLOCKED
    assert case.safe_error_code == SafeRevocationErrorCode.CAPABILITY_UNSUPPORTED.value
    assert index.apply_calls == 0


@pytest.mark.asyncio
async def test_governed_legal_hold_blocks_destroy_decision() -> None:
    store = state.MemoryStateStore()
    runtime, _, suppression = _runtime(store)
    identity = SourceIdentity("source", "scope", "held-destroy")
    request = _request(
        identity,
        decision=RevocationPolicyDecision.DESTROY,
        reason=SourceEventKind.SOURCE_DELETED,
        operation=EffectOperation.DELETE,
        legal_state="legal_hold",
    )
    await suppression.authorize(
        source_digest=identity.evidence_digest(),
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy-a",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=_NOW - datetime.timedelta(minutes=1),
    )
    scored_ids: list[str] = []

    def score(
        query: str,
        candidate: RetrievalCandidate[str],
    ) -> float:
        del query
        scored_ids.append(candidate.candidate_id)
        return 1.0

    case = await runtime.begin_case(request)
    retriever: GuardedInMemoryRetriever[str, str] = GuardedInMemoryRetriever(
        candidates=(_candidate(identity, "held-previously-authorized"),),
        guard=RetrievalGuard(
            suppression_lookup=suppression,
            policy_lookup=InMemoryAccessPolicyLookup((_policy(),)),
        ),
        scorer=score,
    )
    results = await retriever.search(
        "query",
        context=_retrieval_context(),
    )

    assert case.legal_state == "legal_hold"
    assert case.stage is RevocationStage.BLOCKED
    assert case.safe_error_code == SafeRevocationErrorCode.POLICY_BLOCKED.value
    record = await suppression.get(identity.evidence_digest())
    assert record is not None
    assert record.suppressed
    assert record.generation == request.suppression_generation
    assert record.case_id == case.case_id
    assert results == ()
    assert scored_ids == []


@pytest.mark.asyncio
async def test_legal_hold_closes_only_after_verified_isolation() -> None:
    store = state.MemoryStateStore()
    runtime, ledger, _ = _runtime(store)
    identity = SourceIdentity("source", "scope", "held-item")
    request = _request(
        identity,
        decision=RevocationPolicyDecision.PRESERVE_ON_HOLD,
        operation=EffectOperation.ISOLATE,
        legal_state="legal_hold",
    )
    case = await runtime.begin_case(request)
    (descriptor,) = await runtime.descriptors_for(case.case_id)
    action = _DeleteAction(descriptor)
    index = _SyntheticIndex({identity.evidence_digest()})
    await runtime.notify_synor_precommit(case.case_id, descriptor.action_id)
    index.apply(action)
    await runtime.notify_target_effect_applied(
        case.case_id,
        descriptor.action_id,
    )
    await runtime.mark_target_applied(case.case_id, descriptor.action_id)
    await runtime.mark_acknowledged(case.case_id, descriptor.action_id)
    isolated = index.outcome(action, attempt=1)

    terminal = await runtime.record_outcomes(
        case.case_id,
        (isolated,),
        attempt=1,
        attempted_at=_NOW + datetime.timedelta(seconds=1),
    )
    closed = await runtime.finalize_after_engine_commit(case.case_id)

    assert terminal.stage is RevocationStage.RETAINED_ISOLATED
    assert closed.stage is RevocationStage.CLOSED
    assert identity.evidence_digest() in index.source_digests
    assert identity.evidence_digest() in index.isolated_source_digests
    assert not index.is_servable(identity.evidence_digest())
    (receipt,) = await ledger.list_receipts(case.case_id)
    assert receipt.observed_outcome == VerificationOutcome.RETAINED_ISOLATED.value
    assert receipt.stage == RevocationStage.RETAINED_ISOLATED.value


@pytest.mark.asyncio
async def test_preexisting_newer_authorization_blocks_delayed_revocation() -> None:
    store = state.MemoryStateStore()
    runtime, _, suppression = _runtime(store)
    identity = SourceIdentity("source", "scope", "delayed-revocation")
    request = _request(identity, generation=2)
    await suppression.authorize(
        source_digest=identity.evidence_digest(),
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy-a",
        generation=3,
        policy_revision="policy-v2",
        group_graph_revision="groups-v2",
        observed_at=_NOW + datetime.timedelta(minutes=1),
    )

    case = await runtime.begin_case(request)

    assert case.stage is RevocationStage.BLOCKED
    assert case.safe_error_code == SafeRevocationErrorCode.POLICY_BLOCKED.value
    with pytest.raises(RevocationRuntimeStateError, match="current case stage"):
        await runtime.descriptors_for(case.case_id)


@pytest.mark.asyncio
async def test_closed_retry_cannot_fence_newer_verified_authorization() -> None:
    store = state.MemoryStateStore()
    runtime, ledger, suppression = _runtime(store)
    identity = SourceIdentity("source", "scope", "closed-retry-reauthorized")
    request = _request(identity, generation=2)
    index = _SyntheticIndex({identity.evidence_digest()})
    await _converge(runtime, request, index)
    closed = await ledger.get_case(request.case_id)
    assert closed is not None
    assert closed.stage is RevocationStage.CLOSED

    index.authorize_generation(identity.evidence_digest(), 3)
    await runtime.record_verified_reauthorization(
        request,
        generation=3,
        policy_id="policy-a",
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=_NOW + datetime.timedelta(minutes=1),
    )

    with pytest.raises(RevocationRuntimeStateError, match="closed case"):
        await runtime.prepare_retry(request)

    current = await suppression.get(identity.evidence_digest())
    assert current is not None
    assert current.generation == 3
    assert current.verified_authorization
    assert not current.suppressed

    scored_ids: list[str] = []

    def score(
        query: str,
        candidate: RetrievalCandidate[str],
    ) -> float:
        scored_ids.append(candidate.candidate_id)
        return float(len(query))

    replacement = RetrievalCandidate(
        candidate_id="replacement-generation-3",
        source_digest=identity.evidence_digest(),
        source_generation=3,
        tenant_id="tenant-a",
        policy_id="policy-a",
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        payload="verified-replacement-derivative",
    )
    retriever: GuardedInMemoryRetriever[str, str] = GuardedInMemoryRetriever(
        candidates=(replacement,),
        guard=RetrievalGuard(
            suppression_lookup=suppression,
            policy_lookup=InMemoryAccessPolicyLookup((_policy(),)),
        ),
        scorer=score,
    )

    results = await retriever.search("query", context=_retrieval_context())
    assert [item.candidate.candidate_id for item in results] == [
        replacement.candidate_id
    ]
    assert scored_ids == [replacement.candidate_id]


@pytest.mark.asyncio
async def test_old_closed_case_cannot_clear_equal_generation_revocation_fence(
    tmp_path: pathlib.Path,
) -> None:
    store_path = tmp_path / "closed-case-equal-generation-fence"
    store = state.FileStateStore(store_path)
    runtime, ledger, _ = _runtime(store)
    identity = SourceIdentity("source", "scope", "closed-case-fence")
    old_request = _request(identity, generation=2)
    index = _SyntheticIndex({identity.evidence_digest()})
    await _converge(runtime, old_request, index)
    old_case = await ledger.get_case(old_request.case_id)
    assert old_case is not None
    assert old_case.stage is RevocationStage.CLOSED

    index.authorize_generation(identity.evidence_digest(), 3)
    await runtime.record_verified_reauthorization(
        old_request,
        generation=3,
        policy_id="policy-a",
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=_NOW + datetime.timedelta(minutes=1),
    )

    conflicting_request = _request(
        identity,
        generation=3,
        observation_generation="event-3",
    )
    with pytest.raises(SuppressionGenerationConflict, match="conflicting"):
        await runtime.begin_case(conflicting_request)
    conflicting_case = await ledger.get_case(conflicting_request.case_id)
    assert conflicting_case is not None
    assert conflicting_case.stage is RevocationStage.OBSERVED

    # Replaying the older terminal case sees generation 3 authorization, but
    # it must not use that equal-generation authorization to clear the pending
    # generation 3 revocation.
    assert (await runtime.begin_case(old_request)).stage is RevocationStage.CLOSED

    reconstructed_store = state.FileStateStore(store_path)
    scored_ids: list[str] = []

    def score(
        query: str,
        candidate: RetrievalCandidate[str],
    ) -> float:
        del query
        scored_ids.append(candidate.candidate_id)
        return 1.0

    candidate = _candidate(
        identity,
        "generation-3-before-conflict-resolution",
        source_generation=3,
    )
    retriever: GuardedInMemoryRetriever[str, str] = GuardedInMemoryRetriever(
        candidates=(candidate,),
        guard=RetrievalGuard(
            suppression_lookup=StateStoreSuppressionIndex(reconstructed_store),
            policy_lookup=InMemoryAccessPolicyLookup((_policy(),)),
        ),
        scorer=score,
    )

    assert await retriever.search("query", context=_retrieval_context()) == ()
    assert scored_ids == []


@pytest.mark.asyncio
async def test_only_newer_verified_generation_lifts_suppression_and_fences_old_action() -> (
    None
):
    store = state.MemoryStateStore()
    runtime, ledger, suppression = _runtime(store)
    identity = SourceIdentity("source", "scope", "reauthorized-item")
    request = _request(identity, generation=2)
    case = await runtime.begin_case(request)
    (old_descriptor,) = await runtime.descriptors_for(case.case_id)
    assert await suppression.is_suppressed(identity.evidence_digest())

    with pytest.raises(RevocationRuntimeStateError, match="newer"):
        await runtime.record_verified_reauthorization(
            request,
            generation=2,
            policy_id="policy-a",
            policy_revision="policy-v1",
            group_graph_revision="groups-v1",
        )

    old_candidate = _candidate(
        identity,
        "old-generation-1",
        source_generation=1,
    )
    replacement_candidate = RetrievalCandidate(
        candidate_id="replacement-generation-3",
        source_digest=identity.evidence_digest(),
        source_generation=3,
        tenant_id="tenant-a",
        policy_id="policy-a",
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        payload="verified-replacement-derivative",
    )
    current_policy = AccessPolicy(
        policy_id="policy-a",
        tenant_id="tenant-a",
        revision="policy-v1",
        group_graph_revision="groups-v1",
        allowed_principal_ids=frozenset({"principal-a"}),
    )

    await runtime.record_verified_reauthorization(
        request,
        generation=3,
        policy_id="policy-a",
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=_NOW + datetime.timedelta(minutes=1),
    )
    # A retry after a lost successful response is idempotent, while reusing
    # the generation for a different policy identity fails closed.
    await runtime.record_verified_reauthorization(
        request,
        generation=3,
        policy_id="policy-a",
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        observed_at=_NOW + datetime.timedelta(minutes=1),
    )
    with pytest.raises(RevocationRuntimeStateError, match="conflicting"):
        await runtime.record_verified_reauthorization(
            request,
            generation=3,
            policy_id="policy-b",
            policy_revision="policy-v1",
            group_graph_revision="groups-v1",
            observed_at=_NOW + datetime.timedelta(minutes=1),
        )

    blocked = await ledger.get_case(case.case_id)
    assert blocked is not None
    assert blocked.stage is RevocationStage.BLOCKED
    record = await suppression.get(identity.evidence_digest())
    assert record is not None
    assert record.generation == 3
    assert record.verified_authorization
    assert not await suppression.is_suppressed(identity.evidence_digest())
    assert old_descriptor.source_generation == 2
    with pytest.raises(RevocationRuntimeStateError, match="current case stage"):
        await runtime.descriptors_for(case.case_id)
    with pytest.raises(RevocationRuntimeStateError, match="current case stage"):
        await runtime.assert_action_fence(
            case.case_id,
            old_descriptor.action_id,
        )

    scored_candidate_ids: list[str] = []

    def score(
        query: str,
        candidate: RetrievalCandidate[str],
    ) -> float:
        scored_candidate_ids.append(candidate.candidate_id)
        return float(len(query))

    retriever: GuardedInMemoryRetriever[str, str] = GuardedInMemoryRetriever(
        candidates=(old_candidate, replacement_candidate),
        guard=RetrievalGuard(
            suppression_lookup=suppression,
            policy_lookup=InMemoryAccessPolicyLookup((current_policy,)),
        ),
        scorer=score,
    )
    results = await retriever.search(
        "query",
        context=RetrievalContext(
            tenant_id="tenant-a",
            principal_id="principal-a",
            group_graph_revision="groups-v1",
        ),
    )
    assert [result.candidate.candidate_id for result in results] == [
        "replacement-generation-3"
    ]
    assert scored_candidate_ids == ["replacement-generation-3"]

    # The synthetic provider also fences by the generation carried in the
    # descriptor, covering a race after the runtime check but before apply.
    index = _SyntheticIndex(set())
    index.authorize_generation(identity.evidence_digest(), 3)
    index.apply(_DeleteAction(old_descriptor))
    assert identity.evidence_digest() in index.source_digests
    assert index.effective_deletes == 0
