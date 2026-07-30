"""Restart-safe governed Google Drive identity to certified Qdrant deletion."""

from __future__ import annotations

import datetime
import hashlib
from collections.abc import Sequence
from types import SimpleNamespace
from typing import cast

import pytest

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qdrant_models

    HAS_QDRANT = True
except ImportError:
    HAS_QDRANT = False

requires_qdrant = pytest.mark.skipif(
    not HAS_QDRANT, reason="qdrant-client is not installed"
)

if HAS_QDRANT:
    from synor import state
    from synor._internal.context_keys import ContextProvider
    from synor._internal.revocation_ledger import StateStoreRevocationLedger
    from synor._internal.revocation_model import (
        AccessSnapshot,
        EffectOperation,
        RevocationPolicyDecision,
        RevocationStage,
        SourceEventKind,
        SourceIdentity,
        make_observation_id,
        make_tenant_digest,
    )
    from synor._internal.revocation_policy import RevocationPolicy
    from synor._internal.revocation_runtime import (
        RevocationRequest,
        RevocationRuntime,
        TargetObligation,
    )
    from synor._internal.suppression import StateStoreSuppressionIndex
    from synor._internal.verified_sink import (
        AsyncOutcomeRecorder,
        TargetActionApplyError,
        TargetVerificationOutcome,
        TargetVerificationRecorderError,
        VerificationRetryPolicy,
    )
    from synor.connectors import qdrant


_NOW = datetime.datetime(2026, 7, 29, 12, 0, tzinfo=datetime.timezone.utc)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class _CertifiedLocalClient:
    """Expose a contract version while delegating to Qdrant local mode."""

    def __init__(self, client: QdrantClient) -> None:
        self._client = client

    def info(self) -> object:
        return type("_Version", (), {"version": "1.18.0"})()

    def get_collection(self, collection_name: str) -> object:
        info = self._client.get_collection(collection_name)
        return SimpleNamespace(
            status=info.status,
            # Qdrant local mode reports these distributed settings as None.
            # This test-only adapter explicitly models the RF=1/WCF=1 profile
            # used by the in-process simulation; production preflight never
            # substitutes missing provider configuration.
            config=SimpleNamespace(
                params=SimpleNamespace(
                    replication_factor=1,
                    write_consistency_factor=1,
                ),
                optimizer_config=info.config.optimizer_config,
                strict_mode_config=info.config.strict_mode_config,
            ),
            payload_schema={
                field: SimpleNamespace(data_type=data_type)
                for field, data_type in {
                    "synor.contract_version": (qdrant_models.PayloadSchemaType.KEYWORD),
                    "synor.source_digest": (qdrant_models.PayloadSchemaType.KEYWORD),
                    "synor.policy_id": qdrant_models.PayloadSchemaType.KEYWORD,
                    "synor.policy_revision": (qdrant_models.PayloadSchemaType.KEYWORD),
                    "synor.group_graph_revision": (
                        qdrant_models.PayloadSchemaType.KEYWORD
                    ),
                    "synor.tenant": qdrant_models.PayloadSchemaType.KEYWORD,
                    "synor.generation": qdrant_models.PayloadSchemaType.INTEGER,
                    "synor.principal_digests": (
                        qdrant_models.PayloadSchemaType.KEYWORD
                    ),
                    "synor.servable": qdrant_models.PayloadSchemaType.BOOL,
                    "synor.retention_state": (qdrant_models.PayloadSchemaType.KEYWORD),
                }.items()
            },
        )

    def __getattr__(self, name: str) -> object:
        return getattr(self._client, name)


async def _authorize_lineage(
    lineage: qdrant.GovernedPointLineage,
) -> bool:
    del lineage
    return True


async def _verify_query_context(
    context: qdrant.CertifiedQueryContext,
) -> bool:
    del context
    return True


@requires_qdrant
@pytest.mark.asyncio
async def test_local_qdrant_generation_conditions_prevent_resurrection_races() -> None:
    client = QdrantClient(path=":memory:")
    collection_name = "generation_fence"
    client.create_collection(
        collection_name=collection_name,
        vectors_config=qdrant_models.VectorParams(
            size=2,
            distance=qdrant_models.Distance.COSINE,
        ),
    )
    target = qdrant.CertifiedQdrantTarget(
        cast(QdrantClient, _CertifiedLocalClient(client)),
        collection_name,
        lineage_authorizer=_authorize_lineage,
        query_context_verifier=_verify_query_context,
    )
    source_digest = _digest("source")

    def point(generation: int) -> qdrant_models.PointStruct:
        return qdrant.governed_point(
            lineage=qdrant.GovernedPointLineage(
                source_digest=source_digest,
                source_revision=f"revision-v{generation}",
                policy_id="policy",
                policy_revision=f"policy-v{generation}",
                group_graph_revision="groups-v1",
                tenant_digest=_digest("tenant"),
                owner_component_digest=_digest("component"),
                generation=generation,
                principal_digests=(_digest("principal"),),
            ),
            chunk_digest=_digest("chunk"),
            vector=[0.1, 0.2],
        )

    newer = point(3)
    older = point(2)
    await target.upsert_governed([newer], mode="insert_only")
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.upsert_governed([older], mode="update_only")
    assert raised.value.code == "upsert_lineage_unverified"

    point_id = cast(str, newer.id)
    action = target.delete_action(
        point_id=point_id,
        query_context=qdrant.CertifiedQueryContext(
            source_digest=source_digest,
            tenant_digest=_digest("tenant"),
            policy_id="policy",
            policy_revision="policy-v2",
            group_graph_revision="groups-v1",
            generation=2,
            principal_digest=_digest("principal"),
        ),
        action_id=_digest("stale-delete"),
        source_digest=source_digest,
        source_generation=2,
    )

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider, outcomes

    with pytest.raises(TargetActionApplyError):
        await target.verified_delete_sink(
            record=record,
            policy=VerificationRetryPolicy(
                timeout=datetime.timedelta(seconds=1),
                max_attempts=1,
                initial_backoff=0,
                max_backoff=0,
                jitter=0,
            ),
        )(ContextProvider(), [action])

    (recorded,) = client.retrieve(collection_name, [point_id], with_payload=True)
    assert recorded.payload is not None
    assert recorded.payload["synor"]["generation"] == 3
    assert recorded.payload["synor"]["servable"] is True


@requires_qdrant
@pytest.mark.asyncio
async def test_drive_to_qdrant_delete_reconstructs_after_receipt_failure() -> None:
    """An applied effect is retried after in-process runtime reconstruction."""

    store = state.MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    suppression = StateStoreSuppressionIndex(store)
    policy = RevocationPolicy._for_test()
    runtime = RevocationRuntime(
        ledger=ledger,
        suppression=suppression,
        policy=policy,
        clock=lambda: _NOW + datetime.timedelta(seconds=30),
    )

    # The immutable Drive file ID is represented only through SourceIdentity;
    # Qdrant and receipts receive its evidence digest, never the raw ID.
    identity = SourceIdentity(
        connector_instance_id="google-drive-connector",
        source_scope_id="drive-scope",
        item_id="immutable-drive-file-id",
    )
    access = AccessSnapshot(
        tenant_id="tenant-a",
        policy_id="drive-policy",
        policy_revision="policy-v2",
        policy_digest=_digest("drive-policy-v2"),
        group_graph_revision="groups-v2",
    )
    observation_id = make_observation_id(
        identity,
        "drive-revision-v3",
        SourceEventKind.SOURCE_DELETED,
        access,
        observation_generation="drive-change-token-v4",
    )

    client = QdrantClient(path=":memory:")
    collection_name = "governed_drive_chunks"
    client.create_collection(
        collection_name=collection_name,
        vectors_config=qdrant_models.VectorParams(
            size=2,
            distance=qdrant_models.Distance.COSINE,
        ),
    )
    source_digest = identity.evidence_digest()
    lineage = qdrant.GovernedPointLineage(
        source_digest=source_digest,
        source_revision="drive-revision-v3",
        policy_id=access.policy_id,
        policy_revision=access.policy_revision,
        group_graph_revision=access.group_graph_revision,
        tenant_digest=make_tenant_digest(access.tenant_id),
        owner_component_digest=_digest("drive-component"),
        generation=1,
        principal_digests=(_digest("principal-a"),),
    )
    await suppression.authorize(
        source_digest=lineage.source_digest,
        tenant_digest=lineage.tenant_digest,
        policy_id=lineage.policy_id,
        generation=lineage.generation,
        policy_revision=lineage.policy_revision,
        group_graph_revision=lineage.group_graph_revision,
        observed_at=_NOW,
    )
    verifier = qdrant.SuppressionBackedQdrantVerifier(suppression)
    target = qdrant.CertifiedQdrantTarget(
        cast(QdrantClient, _CertifiedLocalClient(client)),
        collection_name,
        lineage_authorizer=verifier.authorize_lineage,
        query_context_verifier=verifier.verify_query_context,
    )
    point = qdrant.governed_point(
        lineage=lineage,
        chunk_digest=_digest("chunk-zero"),
        vector=[0.1, 0.2],
    )
    await target.upsert_governed([point], mode="insert_only")
    point_id = cast(str, point.id)

    obligation = TargetObligation(
        target_provider_id="qdrant-certified",
        target_instance_digest=_digest("qdrant-instance"),
        target_locator_digest=target.point_locator_digest(point_id),
        operation_kind=EffectOperation.DELETE,
        proof_capabilities=target.capabilities,
        capabilities=target.capabilities,
        verifier_kind="qdrant-exact-id-retrieve",
        consistency_contract="wait.strong-write.full-ack.guarded-all-read",
    )
    request = RevocationRequest(
        identity=identity,
        observation_id=observation_id,
        source_revision="drive-revision-v3",
        access=access,
        observation_generation="drive-change-token-v4",
        tenant_digest=make_tenant_digest(access.tenant_id),
        policy_id=access.policy_id,
        policy_revision=access.policy_revision,
        policy_digest=access.policy_digest,
        group_graph_revision=access.group_graph_revision,
        reason=SourceEventKind.SOURCE_DELETED,
        policy_decision=RevocationPolicyDecision.DESTROY,
        suppression_generation=2,
        observed_at=_NOW,
        suppress_by=_NOW,
        verify_by=_NOW + datetime.timedelta(minutes=5),
        obligations=(obligation,),
    )
    case = await runtime.begin_case(request)
    assert case.stage is RevocationStage.PLANNED
    (descriptor,) = await runtime.descriptors_for(case.case_id)
    action = qdrant.QdrantRevocationAction(
        point_id=point_id,
        query_context=qdrant.CertifiedQueryContext(
            source_digest=lineage.source_digest,
            tenant_digest=lineage.tenant_digest,
            policy_id=lineage.policy_id,
            policy_revision=lineage.policy_revision,
            group_graph_revision=lineage.group_graph_revision,
            generation=lineage.generation,
            principal_digest=lineage.principal_digests[0],
        ),
        descriptor=descriptor,
    )

    async def before_apply(
        context_provider: ContextProvider,
        target_action: qdrant.QdrantRevocationAction,
    ) -> None:
        del context_provider
        await runtime.notify_synor_precommit(
            case.case_id,
            target_action.descriptor.action_id,
        )

    async def after_apply(
        context_provider: ContextProvider,
        target_action: qdrant.QdrantRevocationAction,
    ) -> None:
        del context_provider
        await runtime.notify_target_effect_applied(
            case.case_id,
            target_action.descriptor.action_id,
        )
        await runtime.mark_target_applied(
            case.case_id,
            target_action.descriptor.action_id,
        )

    async def fail_receipt_recording(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider, outcomes
        raise RuntimeError("simulated receipt persistence failure")

    retry_policy = VerificationRetryPolicy(
        timeout=datetime.timedelta(seconds=1),
        max_attempts=2,
        initial_backoff=0,
        max_backoff=0,
        jitter=0,
    )
    first_sink = target.verified_delete_sink(
        record=fail_receipt_recording,
        before_apply=before_apply,
        after_apply=after_apply,
        policy=retry_policy,
    )
    with pytest.raises(TargetVerificationRecorderError):
        await first_sink(ContextProvider(), [action])

    assert not client.retrieve(collection_name, [point_id])
    interrupted = await runtime.get_case(case.case_id)
    assert interrupted is not None
    assert interrupted.stage is RevocationStage.DISPATCHED

    # Reconstruct the runtime over the same in-memory StateStore records. This
    # models logical recovery, not an OS process or power-loss test. Reapplying
    # the exact-ID delete is safe even though the point is already absent.
    recovered = RevocationRuntime(
        ledger=StateStoreRevocationLedger(store),
        suppression=StateStoreSuppressionIndex(store),
        policy=policy,
        clock=lambda: _NOW + datetime.timedelta(seconds=60),
    )
    resumed = await recovered.begin_case(request)
    assert resumed.stage is RevocationStage.DISPATCHED
    (recovered_descriptor,) = await recovered.descriptors_for(case.case_id)
    recovered_action = qdrant.QdrantRevocationAction(
        point_id=point_id,
        query_context=action.query_context,
        descriptor=recovered_descriptor,
    )

    async def recovered_before(
        context_provider: ContextProvider,
        target_action: qdrant.QdrantRevocationAction,
    ) -> None:
        del context_provider
        await recovered.notify_synor_precommit(
            case.case_id,
            target_action.descriptor.action_id,
        )

    async def recovered_after(
        context_provider: ContextProvider,
        target_action: qdrant.QdrantRevocationAction,
    ) -> None:
        del context_provider
        await recovered.notify_target_effect_applied(
            case.case_id,
            target_action.descriptor.action_id,
        )
        await recovered.mark_target_applied(
            case.case_id,
            target_action.descriptor.action_id,
        )

    recovered_sink = target.verified_delete_sink(
        record=cast(
            AsyncOutcomeRecorder,
            recovered.outcome_recorder(case.case_id, attempt=2),
        ),
        before_apply=recovered_before,
        after_apply=recovered_after,
        policy=retry_policy,
    )
    await recovered_sink(ContextProvider(), [recovered_action])
    terminal = await recovered.finalize_after_engine_commit(case.case_id)

    assert terminal.stage is RevocationStage.CLOSED
    receipts = await ledger.list_receipts(case.case_id)
    assert len(receipts) == 1
    assert receipts[0].observed_outcome == "absent"
    assert receipts[0].operation_id is not None
    assert receipts[0].operation_id.isdecimal()
    evidence = repr(receipts[0].to_dict())
    assert "immutable-drive-file-id" not in evidence
    assert "principal-a" not in evidence
