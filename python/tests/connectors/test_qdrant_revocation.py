"""Conformance tests for the certified Qdrant revocation boundary."""

from __future__ import annotations

import asyncio
import hashlib
import os
import threading
import time
import uuid
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

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
requires_qdrant_url = pytest.mark.skipif(
    not os.environ.get("QDRANT_URL"), reason="QDRANT_URL is not set"
)
requires_qdrant_cluster_url = pytest.mark.skipif(
    not os.environ.get("QDRANT_CLUSTER_URL"),
    reason="QDRANT_CLUSTER_URL is not set",
)

if HAS_QDRANT:
    from synor import state
    from synor._internal.context_keys import ContextProvider
    from synor._internal.suppression import StateStoreSuppressionIndex
    from synor._internal.verified_sink import (
        TargetActionDescriptionError,
        TargetActionApplyError,
        TargetVerificationError,
        TargetVerificationOutcome,
        VerificationRetryPolicy,
    )
    from synor.connectors import qdrant
    from synor.connectors.qdrant import _revocation as qdrant_revocation


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _lineage(
    *,
    principals: tuple[str, ...] | None = None,
    policy_revision: str = "policy-v2",
    generation: int = 2,
    source: str = "source",
) -> qdrant.GovernedPointLineage:
    return qdrant.GovernedPointLineage(
        source_digest=_digest(source),
        source_revision="source-v1",
        policy_id="policy-a",
        policy_revision=policy_revision,
        group_graph_revision="groups-v2",
        tenant_digest=_digest("tenant"),
        owner_component_digest=_digest("component"),
        generation=generation,
        principal_digests=principals or (_digest("principal-allowed"),),
    )


def _context(
    principal: str = "principal-allowed",
    *,
    policy_revision: str = "policy-v2",
    generation: int = 2,
    source: str = "source",
) -> qdrant.CertifiedQueryContext:
    return qdrant.CertifiedQueryContext(
        source_digest=_digest(source),
        tenant_digest=_digest("tenant"),
        policy_id="policy-a",
        policy_revision=policy_revision,
        group_graph_revision="groups-v2",
        generation=generation,
        principal_digest=_digest(principal),
    )


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


def _target(
    client: _FakeClient,
    collection_name: str = "governed",
) -> qdrant.CertifiedQdrantTarget:
    return qdrant.CertifiedQdrantTarget(
        cast(QdrantClient, client),
        collection_name,
        lineage_authorizer=_authorize_lineage,
        query_context_verifier=_verify_query_context,
    )


def _policy(max_attempts: int = 3) -> VerificationRetryPolicy:
    return VerificationRetryPolicy(
        timeout=timedelta(seconds=1),
        max_attempts=max_attempts,
        initial_backoff=0,
        max_backoff=0,
        jitter=0,
    )


class _FakeClient:
    def __init__(self) -> None:
        self.payloads: dict[str, dict[str, Any]] = {}
        self.vectors: dict[str, Any] = {}
        self.operation_id = 0
        self.pending_delete_reads: dict[str, int] = {}
        self.delete_visibility_reads = 0
        self.ignore_suppression = False
        self.apply_error: Exception | None = None
        self.upsert_error_after_apply: Exception | None = None
        self.query_kwargs: dict[str, Any] | None = None
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.collection_present = True
        self.delete_collection_result: object = True
        self.collection_exists_result: object | None = None
        self.update_status = qdrant_models.UpdateStatus.COMPLETED
        self.server_version = "1.18.0"
        self.replication_factor: object = 1
        self.write_consistency_factor: object = 1
        self.collection_status = qdrant_models.CollectionStatus.GREEN
        self.prevent_unoptimized: object = False
        self.strict_mode_enabled: object = False
        self.strict_max_timeout: object = None
        self.strict_max_query_limit: object = None
        self.strict_max_batchsize: object = None
        self.strict_filter_max_conditions: object = None
        self.strict_condition_max_size: object = None
        self.topology_healthy = True
        self.payload_schema = {
            field: SimpleNamespace(data_type=data_type)
            for field, data_type in {
                "synor.contract_version": qdrant_models.PayloadSchemaType.KEYWORD,
                "synor.source_digest": qdrant_models.PayloadSchemaType.KEYWORD,
                "synor.policy_id": qdrant_models.PayloadSchemaType.KEYWORD,
                "synor.policy_revision": qdrant_models.PayloadSchemaType.KEYWORD,
                "synor.group_graph_revision": (qdrant_models.PayloadSchemaType.KEYWORD),
                "synor.tenant": qdrant_models.PayloadSchemaType.KEYWORD,
                "synor.generation": qdrant_models.PayloadSchemaType.INTEGER,
                "synor.principal_digests": (qdrant_models.PayloadSchemaType.KEYWORD),
                "synor.servable": qdrant_models.PayloadSchemaType.BOOL,
                "synor.retention_state": qdrant_models.PayloadSchemaType.KEYWORD,
            }.items()
        }
        self.query_call_count = 0
        self.query_error_after: int | None = None
        self.info_delay = 0.0
        self.info_error: Exception | None = None
        self.block_serving_enable = False
        self.serving_enable_started = threading.Event()
        self.serving_enable_release = threading.Event()
        self.block_collection_delete = False
        self.collection_delete_started = threading.Event()
        self.collection_delete_release = threading.Event()

    def add(self, point_id: str, lineage: qdrant.GovernedPointLineage) -> None:
        self.payloads[point_id] = {"synor": lineage.payload()}
        self.vectors[point_id] = [0.1]

    def add_point(self, point: qdrant_models.PointStruct) -> None:
        point_id = str(point.id)
        self.payloads[point_id] = cast(dict[str, Any], point.payload)
        self.vectors[point_id] = point.vector

    def _result(self) -> qdrant_models.UpdateResult:
        self.operation_id += 1
        return qdrant_models.UpdateResult(
            operation_id=self.operation_id,
            status=self.update_status,
        )

    def set_payload(self, **kwargs: Any) -> qdrant_models.UpdateResult:
        self.calls.append(("set_payload", kwargs))
        if self.apply_error is not None:
            raise self.apply_error
        if kwargs["payload"] == {"servable": True} and self.block_serving_enable:
            self.serving_enable_started.set()
            if not self.serving_enable_release.wait(timeout=2):
                raise TimeoutError("test serving-enable release timed out")
        points = kwargs["points"]
        if isinstance(points, qdrant_models.Filter):
            point_ids = [
                point_id
                for point_id, payload in self.payloads.items()
                if _filter_matches(points, point_id, payload)
            ]
        else:
            point_ids = [str(raw_id) for raw_id in points]
        for point_id in point_ids:
            if point_id not in self.payloads or self.ignore_suppression:
                continue
            key = kwargs.get("key")
            if key == "synor":
                self.payloads[point_id].setdefault("synor", {}).update(
                    kwargs["payload"]
                )
            else:
                self.payloads[point_id].update(kwargs["payload"])
        return self._result()

    def delete(self, **kwargs: Any) -> qdrant_models.UpdateResult:
        self.calls.append(("delete", kwargs))
        if self.apply_error is not None:
            raise self.apply_error
        selector = kwargs["points_selector"]
        if isinstance(selector, qdrant_models.Filter):
            point_ids = [
                point_id
                for point_id, payload in self.payloads.items()
                if _filter_matches(selector, point_id, payload)
            ]
        else:
            point_ids = [str(raw_id) for raw_id in selector.points]
        for point_id in point_ids:
            if point_id in self.payloads:
                self.pending_delete_reads[point_id] = self.delete_visibility_reads
                if self.delete_visibility_reads == 0:
                    self.payloads.pop(point_id, None)
                    self.vectors.pop(point_id, None)
        return self._result()

    def retrieve(self, **kwargs: Any) -> list[SimpleNamespace]:
        self.calls.append(("retrieve", kwargs))
        records: list[SimpleNamespace] = []
        for raw_id in kwargs["ids"]:
            point_id = str(raw_id)
            remaining = self.pending_delete_reads.get(point_id)
            if remaining is not None:
                if remaining <= 0:
                    self.pending_delete_reads.pop(point_id, None)
                    self.payloads.pop(point_id, None)
                    self.vectors.pop(point_id, None)
                else:
                    self.pending_delete_reads[point_id] = remaining - 1
            if point_id in self.payloads:
                records.append(
                    SimpleNamespace(
                        id=point_id,
                        payload=(
                            self.payloads[point_id]
                            if kwargs.get("with_payload", True)
                            else None
                        ),
                        vector=(
                            self.vectors[point_id]
                            if kwargs.get("with_vectors", False)
                            else None
                        ),
                    )
                )
        return records

    def scroll(self, **kwargs: Any) -> tuple[list[SimpleNamespace], None]:
        self.calls.append(("scroll", kwargs))
        records = [
            SimpleNamespace(id=point_id)
            for point_id, payload in self.payloads.items()
            if _filter_matches(kwargs["scroll_filter"], point_id, payload)
        ]
        return records[: kwargs["limit"]], None

    def query_points(self, **kwargs: Any) -> qdrant_models.QueryResponse:
        self.calls.append(("query_points", kwargs))
        self.query_call_count += 1
        if (
            self.query_error_after is not None
            and self.query_call_count >= self.query_error_after
        ):
            raise TimeoutError("Bearer secret customer@example.test")
        self.query_kwargs = kwargs
        records = [
            qdrant_models.ScoredPoint(
                id=point_id,
                version=1,
                score=1.0,
                payload=payload,
                vector=None,
            )
            for point_id, payload in self.payloads.items()
            if _filter_matches(kwargs["query_filter"], point_id, payload)
        ]
        return qdrant_models.QueryResponse(points=records)

    def upsert(self, **kwargs: Any) -> qdrant_models.UpdateResult:
        self.calls.append(("upsert", kwargs))
        for point in kwargs["points"]:
            point_id = str(point.id)
            update_filter = kwargs.get("update_filter")
            update_mode = kwargs.get("update_mode")
            existing = self.payloads.get(point_id)
            if existing is None and update_mode is qdrant_models.UpdateMode.UPDATE_ONLY:
                continue
            if (
                existing is not None
                and update_mode is qdrant_models.UpdateMode.INSERT_ONLY
            ):
                continue
            if (
                existing is not None
                and isinstance(update_filter, qdrant_models.Filter)
                and not _filter_matches(update_filter, point_id, existing)
            ):
                continue
            self.payloads[point_id] = cast(dict[str, Any], point.payload)
            self.vectors[point_id] = point.vector
        if self.upsert_error_after_apply is not None:
            raise self.upsert_error_after_apply
        return self._result()

    def info(self) -> SimpleNamespace:
        time.sleep(self.info_delay)
        if self.info_error is not None:
            raise self.info_error
        return SimpleNamespace(version=self.server_version)

    def get_collection(self, **kwargs: Any) -> SimpleNamespace:
        self.calls.append(("get_collection", kwargs))
        return SimpleNamespace(
            status=self.collection_status,
            config=SimpleNamespace(
                params=SimpleNamespace(
                    replication_factor=self.replication_factor,
                    write_consistency_factor=self.write_consistency_factor,
                ),
                optimizer_config=SimpleNamespace(
                    prevent_unoptimized=self.prevent_unoptimized
                ),
                strict_mode_config=SimpleNamespace(
                    enabled=self.strict_mode_enabled,
                    max_timeout=self.strict_max_timeout,
                    max_query_limit=self.strict_max_query_limit,
                    upsert_max_batchsize=self.strict_max_batchsize,
                    filter_max_conditions=self.strict_filter_max_conditions,
                    condition_max_size=self.strict_condition_max_size,
                ),
            ),
            payload_schema=self.payload_schema,
        )

    def create_payload_index(self, **kwargs: Any) -> qdrant_models.UpdateResult:
        self.calls.append(("create_payload_index", kwargs))
        self.payload_schema[kwargs["field_name"]] = SimpleNamespace(
            data_type=kwargs["field_schema"]
        )
        return self._result()

    def collection_cluster_info(self, **kwargs: Any) -> SimpleNamespace:
        self.calls.append(("collection_cluster_info", kwargs))
        remote_state = (
            qdrant_models.ReplicaState.ACTIVE
            if self.topology_healthy
            else qdrant_models.ReplicaState.DEAD
        )
        return SimpleNamespace(
            shard_count=1,
            local_shards=[
                SimpleNamespace(
                    shard_id=0,
                    shard_key=None,
                    state=qdrant_models.ReplicaState.ACTIVE,
                )
            ],
            remote_shards=[
                SimpleNamespace(
                    shard_id=0,
                    shard_key=None,
                    state=remote_state,
                )
            ],
            shard_transfers=[],
            resharding_operations=[],
        )

    def delete_collection(self, **kwargs: Any) -> bool:
        self.calls.append(("delete_collection", kwargs))
        if self.block_collection_delete:
            self.collection_delete_started.set()
            if not self.collection_delete_release.wait(timeout=2):
                raise TimeoutError("test collection-delete release timed out")
        if self.delete_collection_result is not True:
            return cast(bool, self.delete_collection_result)
        self.collection_present = False
        return True

    def collection_exists(self, **kwargs: Any) -> object:
        self.calls.append(("collection_exists", kwargs))
        if self.collection_exists_result is not None:
            return self.collection_exists_result
        return self.collection_present


def _filter_matches(
    query_filter: qdrant_models.Filter,
    point_id: str,
    payload: dict[str, Any],
) -> bool:
    should = query_filter.should
    if should is not None:
        should_conditions = should if isinstance(should, list) else [should]
        if not any(
            isinstance(condition, qdrant_models.Filter)
            and _filter_matches(condition, point_id, payload)
            for condition in should_conditions
        ):
            return False
    conditions = query_filter.must
    if conditions is None:
        return True
    if not isinstance(conditions, list):
        conditions = [conditions]
    for condition in conditions:
        if isinstance(condition, qdrant_models.HasIdCondition):
            if point_id not in {str(value) for value in condition.has_id}:
                return False
            continue
        if isinstance(condition, qdrant_models.Filter):
            if not _filter_matches(condition, point_id, payload):
                return False
            continue
        if not isinstance(condition, qdrant_models.FieldCondition):
            raise AssertionError(f"unsupported fake filter: {type(condition)}")
        value: Any = payload
        for part in condition.key.split("."):
            if not isinstance(value, dict) or part not in value:
                return False
            value = value[part]
        if condition.range is not None:
            value_range = condition.range
            if value_range.lt is not None and not value < value_range.lt:
                return False
            if value_range.lte is not None and not value <= value_range.lte:
                return False
            if value_range.gt is not None and not value > value_range.gt:
                return False
            if value_range.gte is not None and not value >= value_range.gte:
                return False
        else:
            match = condition.match
            if isinstance(match, qdrant_models.MatchAny):
                expected_values = set(match.any)
                if isinstance(value, list):
                    if expected_values.isdisjoint(value):
                        return False
                elif value not in expected_values:
                    return False
                continue
            expected = cast(qdrant_models.MatchValue, match).value
            if isinstance(value, list):
                if expected not in value:
                    return False
            elif value != expected:
                return False
    return True


async def _record_noop(
    context_provider: ContextProvider,
    outcomes: Sequence[TargetVerificationOutcome],
    /,
) -> None:
    del context_provider, outcomes


@requires_qdrant
def test_deterministic_governed_point_contract_and_reserved_payload() -> None:
    lineage = _lineage()
    chunk_digest = _digest("chunk-1")

    first = qdrant.governed_point(
        lineage=lineage,
        chunk_digest=chunk_digest,
        vector=[0.1, 0.2],
        payload={"text": "content"},
    )
    second = qdrant.governed_point(
        lineage=lineage,
        chunk_digest=chunk_digest,
        vector=[0.3, 0.4],
    )

    assert first.id == second.id
    assert first.id == qdrant.deterministic_point_id(
        lineage.source_digest, chunk_digest
    )
    assert first.payload is not None
    governed = first.payload["synor"]
    assert isinstance(governed, dict)
    assert governed["chunk_digest"] == chunk_digest
    assert len(cast(str, governed["content_fingerprint"])) == 64
    assert {
        key: value
        for key, value in governed.items()
        if key not in {"chunk_digest", "content_fingerprint"}
    } == lineage.payload()
    assert "principal-allowed" not in repr(governed)

    with pytest.raises(ValueError, match="reserved"):
        qdrant.governed_point(
            lineage=lineage,
            chunk_digest=chunk_digest,
            vector=[0.1],
            payload={"synor": {}},
        )


@requires_qdrant
def test_point_locator_digest_canonicalizes_equivalent_uuid_forms() -> None:
    target = _target(_FakeClient())
    point_id = uuid.uuid4()

    assert {
        target.point_locator_digest(cast(Any, point_id)),
        target.point_locator_digest(str(point_id)),
        target.point_locator_digest(point_id.hex),
        target.point_locator_digest(point_id.urn),
    } == {target.point_locator_digest(str(point_id))}


@requires_qdrant
def test_governed_lineage_rejects_unsafe_retention_states() -> None:
    with pytest.raises(ValueError, match="retention_state"):
        replace(_lineage(), retention_state=cast(Any, "public"))
    with pytest.raises(ValueError, match="must not be servable"):
        replace(_lineage(), retention_state="retained_isolated")

    isolated = replace(
        _lineage(),
        retention_state="retained_isolated",
        servable=False,
    )
    assert isolated.payload()["retention_state"] == "retained_isolated"
    with pytest.raises(ValueError, match="must not be servable"):
        isolated.payload(servable=True)


@requires_qdrant
def test_certified_filter_requires_full_context_and_exact_fields() -> None:
    context = _context()
    query_filter = qdrant.certified_query_filter(
        context,
        point_ids=["550e8400-e29b-41d4-a716-446655440000"],
    )
    serialized = query_filter.model_dump(mode="json")
    text = repr(serialized)

    for field in (
        "synor.contract_version",
        "synor.source_digest",
        "synor.tenant",
        "synor.servable",
        "synor.retention_state",
        "synor.policy_id",
        "synor.policy_revision",
        "synor.group_graph_revision",
        "synor.generation",
        "synor.principal_digests",
    ):
        assert field in text
    with pytest.raises(TypeError, match="CertifiedQueryContext"):
        qdrant.certified_query_filter(cast(Any, None))


@requires_qdrant
@pytest.mark.asyncio
async def test_guarded_query_controls_source_filter_and_read_consistency() -> None:
    client = _FakeClient()
    point = qdrant.governed_point(
        lineage=_lineage(),
        chunk_digest=_digest("chunk"),
        vector=[0.1],
    )
    client.add(str(point.id), _lineage())
    target = _target(client)

    result = await target.query_points(context=_context(), query=[0.1], limit=10)

    assert [str(item.id) for item in result.points] == [str(point.id)]
    assert client.query_kwargs is not None
    assert client.query_kwargs["consistency"] is qdrant_models.ReadConsistencyType.ALL
    assert client.query_kwargs["timeout"] == 30

    sparse_query = qdrant_models.SparseVector(indices=[1, 9], values=[0.2, 0.8])
    await target.query_points(context=_context(), query=sparse_query)
    assert client.query_kwargs is not None
    normalized_sparse = cast(
        qdrant_models.SparseVector,
        client.query_kwargs["query"],
    )
    assert normalized_sparse == sparse_query
    assert normalized_sparse is not sparse_query

    multidense_query = [[1, 0], [0, 1]]
    await target.query_points(context=_context(), query=multidense_query)
    assert client.query_kwargs is not None
    assert client.query_kwargs["query"] == [[1.0, 0.0], [0.0, 1.0]]
    assert client.query_kwargs["query"] is not multidense_query

    with pytest.raises(ValueError, match="controls filter"):
        await target.query_points(
            context=_context(),
            query_filter=qdrant_models.Filter(),
        )


@requires_qdrant
@pytest.mark.asyncio
async def test_guarded_query_rejects_unfiltered_universal_query_shapes() -> None:
    client = _FakeClient()
    target = _target(client)

    unsafe_queries: list[object] = [
        7,
        str(uuid.uuid4()),
        uuid.uuid4(),
        qdrant_models.NearestQuery(nearest=7),
        qdrant_models.FusionQuery(fusion=qdrant_models.Fusion.RRF),
        [[0.1], qdrant_models.NearestQuery(nearest=7)],
    ]
    for query in unsafe_queries:
        with pytest.raises(ValueError, match="only raw dense or sparse vectors"):
            await target.query_points(context=_context(), query=query)

    unsupported_kwargs = (
        {"prefetch": qdrant_models.Prefetch(query=[0.1])},
        {"lookup_from": qdrant_models.LookupLocation(collection="other")},
        {"future_query_alias": [0.1]},
    )
    for kwargs in unsupported_kwargs:
        with pytest.raises(ValueError, match="compound or unknown arguments"):
            await target.query_points(context=_context(), **kwargs)

    assert client.query_call_count == 0


@requires_qdrant
@pytest.mark.asyncio
async def test_guarded_query_fails_closed_without_current_state_verification() -> None:
    client = _FakeClient()
    client.add(str(uuid.uuid4()), _lineage())
    missing = qdrant.CertifiedQdrantTarget(
        cast(QdrantClient, client),
        "governed",
        lineage_authorizer=_authorize_lineage,
    )
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await missing.query_points(context=_context())
    assert raised.value.code == "query_context_verifier_missing"

    async def stale(context: qdrant.CertifiedQueryContext) -> bool:
        del context
        return False

    stale_target = qdrant.CertifiedQdrantTarget(
        cast(QdrantClient, client),
        "governed",
        lineage_authorizer=_authorize_lineage,
        query_context_verifier=stale,
    )
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await stale_target.query_points(context=_context())
    assert raised.value.code == "query_context_stale"


@requires_qdrant
@pytest.mark.asyncio
async def test_suppression_backed_verifier_tracks_exact_current_generation() -> None:
    suppression = StateStoreSuppressionIndex(state.MemoryStateStore())
    verifier = qdrant.SuppressionBackedQdrantVerifier(suppression)
    lineage = _lineage(generation=3)
    context = _context(generation=3)

    assert not await verifier.authorize_lineage(lineage)
    assert not await verifier.verify_query_context(context)

    await suppression.authorize(
        source_digest=lineage.source_digest,
        tenant_digest=lineage.tenant_digest,
        policy_id=lineage.policy_id,
        generation=lineage.generation,
        policy_revision=lineage.policy_revision,
        group_graph_revision=lineage.group_graph_revision,
        observed_at=datetime(2026, 7, 29, tzinfo=UTC),
    )
    assert await verifier.authorize_lineage(lineage)
    assert await verifier.verify_query_context(context)
    assert not await verifier.verify_query_context(
        replace(context, policy_revision="stale-policy")
    )

    await suppression.suppress(
        source_digest=lineage.source_digest,
        tenant_digest=lineage.tenant_digest,
        policy_id=lineage.policy_id,
        generation=4,
        policy_revision="policy-v3",
        group_graph_revision=lineage.group_graph_revision,
        reason="source_deleted",
        case_id="case-4",
        observed_at=datetime(2026, 7, 29, 0, 1, tzinfo=UTC),
    )
    assert not await verifier.authorize_lineage(lineage)
    assert not await verifier.verify_query_context(context)


@requires_qdrant
@pytest.mark.asyncio
async def test_guarded_query_is_explicitly_source_scoped() -> None:
    client = _FakeClient()
    first_id = str(uuid.uuid4())
    second_id = str(uuid.uuid4())
    client.add(first_id, _lineage(source="first-source"))
    client.add(second_id, _lineage(source="second-source", generation=7))
    target = _target(client)

    response = await target.query_points(
        context=_context(source="first-source"),
    )

    assert [str(point.id) for point in response.points] == [first_id]


@requires_qdrant
@pytest.mark.asyncio
async def test_guarded_query_rechecks_authorization_after_provider_await() -> None:
    client = _FakeClient()
    client.add(str(uuid.uuid4()), _lineage())
    verification_count = 0

    async def revoked_during_query(
        context: qdrant.CertifiedQueryContext,
    ) -> bool:
        nonlocal verification_count
        del context
        verification_count += 1
        return verification_count == 1

    target = qdrant.CertifiedQdrantTarget(
        cast(QdrantClient, client),
        "governed",
        lineage_authorizer=_authorize_lineage,
        query_context_verifier=revoked_during_query,
    )

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.query_points(context=_context())

    assert raised.value.code == "query_context_stale"
    assert client.query_call_count == 1
    assert verification_count == 2


@requires_qdrant
@pytest.mark.asyncio
async def test_guarded_query_redacts_provider_failures() -> None:
    client = _FakeClient()
    client.query_error_after = 1
    target = _target(client)

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.query_points(context=_context())

    assert raised.value.code == "query_failed"
    assert "Bearer secret" not in str(raised.value)
    assert raised.value.__cause__ is None


@requires_qdrant
@pytest.mark.asyncio
async def test_exact_readback_rejects_malformed_unexpected_and_duplicate_ids() -> None:
    requested_id = str(uuid.uuid4())
    unexpected_id = str(uuid.uuid4())

    class ControlledRetrieveClient(_FakeClient):
        def __init__(self, response: object) -> None:
            super().__init__()
            self.response = response

        def retrieve(self, **kwargs: Any) -> list[SimpleNamespace]:
            del kwargs
            return cast(list[SimpleNamespace], self.response)

    malformed = _target(ControlledRetrieveClient(None))
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await malformed._retrieve_point(requested_id, with_vectors=False)
    assert raised.value.code == "readback_shape_invalid"

    unexpected = _target(ControlledRetrieveClient([SimpleNamespace(id=unexpected_id)]))
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await unexpected._retrieve_ids([requested_id])
    assert raised.value.code == "readback_id_unexpected"

    duplicate = _target(
        ControlledRetrieveClient(
            [
                SimpleNamespace(id=requested_id),
                SimpleNamespace(id=requested_id),
            ]
        )
    )
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await duplicate._retrieve_ids([requested_id])
    assert raised.value.code == "readback_id_duplicate"


@requires_qdrant
@pytest.mark.asyncio
async def test_filter_readback_rejects_an_unrequested_point_id() -> None:
    requested_id = str(uuid.uuid4())
    unexpected_id = str(uuid.uuid4())

    class WrongQueryClient(_FakeClient):
        def query_points(self, **kwargs: Any) -> qdrant_models.QueryResponse:
            del kwargs
            return qdrant_models.QueryResponse(
                points=[
                    qdrant_models.ScoredPoint(
                        id=unexpected_id,
                        version=1,
                        score=1.0,
                    )
                ]
            )

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(WrongQueryClient())._point_matches_filter(
            requested_id,
            _context(),
        )

    assert raised.value.code == "readback_id_unexpected"


@requires_qdrant
@pytest.mark.asyncio
async def test_governed_upsert_cannot_downgrade_newer_generation() -> None:
    client = _FakeClient()
    target = _target(client)
    chunk_digest = _digest("same-chunk")
    newer = qdrant.governed_point(
        lineage=_lineage(generation=3),
        chunk_digest=chunk_digest,
        vector=[0.3],
    )
    older = qdrant.governed_point(
        lineage=_lineage(generation=2),
        chunk_digest=chunk_digest,
        vector=[0.2],
    )
    await target.upsert_governed([newer], mode="insert_only")

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.upsert_governed([older], mode="update_only")

    assert raised.value.code == "upsert_lineage_unverified"
    assert client.payloads[str(newer.id)]["synor"]["generation"] == 3


@requires_qdrant
@pytest.mark.asyncio
async def test_governed_insert_retry_after_lost_response_is_idempotent() -> None:
    client = _FakeClient()
    target = _target(client)
    point = qdrant.governed_point(
        lineage=_lineage(generation=3),
        chunk_digest=_digest("idempotent-insert"),
        vector=[0.3],
    )

    await target.upsert_governed([point], mode="insert_only")
    await target.upsert_governed([point], mode="insert_only")

    assert client.payloads[str(point.id)]["synor"] == _lineage(
        generation=3
    ).payload() | {
        "chunk_digest": _digest("idempotent-insert"),
        "content_fingerprint": cast(dict[str, Any], point.payload)["synor"][
            "content_fingerprint"
        ],
    }


@requires_qdrant
@pytest.mark.asyncio
async def test_idempotent_insert_is_refenced_if_authorization_turns_stale() -> None:
    client = _FakeClient()
    authorization_current = True

    async def current(lineage: qdrant.GovernedPointLineage) -> bool:
        del lineage
        return authorization_current

    target = qdrant.CertifiedQdrantTarget(
        cast(QdrantClient, client),
        "governed",
        lineage_authorizer=current,
        query_context_verifier=_verify_query_context,
    )
    point = qdrant.governed_point(
        lineage=_lineage(generation=3),
        chunk_digest=_digest("idempotent-insert-revoked"),
        vector=[0.3],
    )
    await target.upsert_governed([point], mode="insert_only")

    verification_count = 0

    async def revoked_after_preflight(
        lineage: qdrant.GovernedPointLineage,
    ) -> bool:
        nonlocal verification_count
        del lineage
        verification_count += 1
        return verification_count == 1

    target = qdrant.CertifiedQdrantTarget(
        cast(QdrantClient, client),
        "governed",
        lineage_authorizer=revoked_after_preflight,
        query_context_verifier=_verify_query_context,
    )
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.upsert_governed([point], mode="insert_only")

    assert raised.value.code == "lineage_authorization_stale"
    assert client.payloads[str(point.id)]["synor"]["servable"] is False


@requires_qdrant
@pytest.mark.asyncio
async def test_governed_insert_retry_rejects_different_point_content() -> None:
    client = _FakeClient()
    target = _target(client)
    chunk_digest = _digest("same-governed-id")
    first = qdrant.governed_point(
        lineage=_lineage(generation=3),
        chunk_digest=chunk_digest,
        vector=[0.1],
        payload={"text": "first"},
    )
    conflicting = qdrant.governed_point(
        lineage=_lineage(generation=3),
        chunk_digest=chunk_digest,
        vector=[0.9],
        payload={"text": "conflicting"},
    )

    await target.upsert_governed([first], mode="insert_only")
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.upsert_governed([conflicting], mode="insert_only")

    assert raised.value.code == "upsert_lineage_unverified"
    assert client.payloads[str(first.id)]["text"] == "first"
    assert client.vectors[str(first.id)] == [0.1]


@requires_qdrant
@pytest.mark.asyncio
async def test_governed_upsert_rejects_tampered_id_or_content_fingerprint() -> None:
    client = _FakeClient()
    target = _target(client)
    point = qdrant.governed_point(
        lineage=_lineage(generation=3),
        chunk_digest=_digest("bound-chunk"),
        vector=[0.3],
        payload={"text": "original"},
    )
    wrong_id = point.model_copy(update={"id": str(uuid.uuid4())})
    with pytest.raises(ValueError, match="point ID"):
        await target.upsert_governed([wrong_id], mode="insert_only")

    tampered_payload = dict(point.payload or {})
    tampered_payload["text"] = "tampered"
    tampered = qdrant_models.PointStruct(
        id=point.id,
        vector=point.vector,
        payload=tampered_payload,
    )
    with pytest.raises(ValueError, match="content fingerprint"):
        await target.upsert_governed([tampered], mode="insert_only")

    assert not any(name == "upsert" for name, _ in client.calls)


@requires_qdrant
@pytest.mark.asyncio
async def test_governed_upsert_rejects_duplicate_point_ids_before_write() -> None:
    client = _FakeClient()
    target = _target(client)
    point = qdrant.governed_point(
        lineage=_lineage(generation=3),
        chunk_digest=_digest("duplicate-governed-id"),
        vector=[0.3],
    )

    with pytest.raises(ValueError, match="duplicate governed IDs"):
        await target.upsert_governed([point, point], mode="insert_only")

    assert not any(name == "upsert" for name, _ in client.calls)


@requires_qdrant
@pytest.mark.asyncio
async def test_lost_upsert_response_is_redacted_and_leaves_point_fenced() -> None:
    client = _FakeClient()
    client.upsert_error_after_apply = TimeoutError(
        "Bearer secret customer@example.test"
    )
    target = _target(client)
    point = qdrant.governed_point(
        lineage=_lineage(generation=3),
        chunk_digest=_digest("lost-upsert-response"),
        vector=[0.3],
    )

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.upsert_governed([point], mode="insert_only")

    assert raised.value.code == "upsert_staging_failed"
    assert "Bearer secret" not in str(raised.value)
    assert raised.value.__cause__ is None
    assert client.payloads[str(point.id)]["synor"]["servable"] is False


@requires_qdrant
@pytest.mark.asyncio
async def test_upsert_cancellation_cannot_be_overtaken_by_late_enable() -> None:
    client = _FakeClient()
    client.block_serving_enable = True
    target = _target(client)
    point = qdrant.governed_point(
        lineage=_lineage(generation=3),
        chunk_digest=_digest("cancelled-serving-enable"),
        vector=[0.3],
    )

    update = asyncio.create_task(target.upsert_governed([point], mode="insert_only"))
    assert await asyncio.to_thread(client.serving_enable_started.wait, 1)
    update.cancel()
    await asyncio.sleep(0.01)
    client.serving_enable_release.set()

    with pytest.raises(asyncio.CancelledError):
        await update
    assert client.payloads[str(point.id)]["synor"]["servable"] is False


@requires_qdrant
@pytest.mark.asyncio
async def test_stale_insert_cannot_resurrect_a_deleted_point() -> None:
    client = _FakeClient()
    current_generation = 3

    async def current(lineage: qdrant.GovernedPointLineage) -> bool:
        return lineage.generation == current_generation

    target = qdrant.CertifiedQdrantTarget(
        cast(QdrantClient, client),
        "governed",
        lineage_authorizer=current,
        query_context_verifier=_verify_query_context,
    )
    chunk_digest = _digest("resurrection-chunk")
    current_point = qdrant.governed_point(
        lineage=_lineage(generation=3),
        chunk_digest=chunk_digest,
        vector=[0.3],
    )
    await target.upsert_governed([current_point], mode="insert_only")
    point_id = cast(str, current_point.id)
    action = target.delete_action(
        point_id=point_id,
        query_context=_context(generation=3),
        action_id=_digest("delete-current"),
        source_digest=_lineage().source_digest,
        source_generation=4,
    )
    await target.verified_delete_sink(
        record=_record_noop,
        policy=_policy(),
    )(ContextProvider(), [action])
    assert point_id not in client.payloads

    current_generation = 4
    stale_point = qdrant.governed_point(
        lineage=_lineage(generation=2),
        chunk_digest=chunk_digest,
        vector=[0.2],
    )
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.upsert_governed([stale_point], mode="insert_only")
    assert raised.value.code == "lineage_authorization_stale"
    assert point_id not in client.payloads


def _delete_setup(
    client: _FakeClient,
    *,
    point_name: str = "point-a",
    stored_generation: int = 2,
    action_generation: int = 2,
) -> tuple[qdrant.CertifiedQdrantTarget, qdrant.QdrantRevocationAction, str]:
    lineage = _lineage(generation=stored_generation)
    point = qdrant.governed_point(
        lineage=lineage,
        chunk_digest=_digest(point_name),
        vector=[0.1],
        payload={"text": point_name},
    )
    point_id = str(point.id)
    client.add_point(point)
    target = _target(client)
    action = target.delete_action(
        point_id=point_id,
        query_context=_context(generation=stored_generation),
        action_id=_digest(f"action-{point_name}"),
        source_digest=_lineage().source_digest,
        source_generation=action_generation,
    )
    return target, action, point_id


@requires_qdrant
@pytest.mark.asyncio
async def test_new_revocation_generation_deletes_older_derivative() -> None:
    client = _FakeClient()
    target, action, point_id = _delete_setup(
        client,
        stored_generation=1,
        action_generation=2,
    )

    await target.verified_delete_sink(
        record=_record_noop,
        policy=_policy(),
    )(ContextProvider(), [action])

    assert point_id not in client.payloads


@requires_qdrant
@pytest.mark.asyncio
async def test_destructive_flow_suppresses_deletes_and_records_correlated_evidence() -> (
    None
):
    client = _FakeClient()
    target, first, first_id = _delete_setup(client, point_name="first")
    _, second, second_id = _delete_setup(client, point_name="second")
    operation_evidence: list[qdrant.QdrantOperationEvidence] = []
    outcomes: list[TargetVerificationOutcome] = []

    async def record_operations(
        context_provider: ContextProvider,
        evidence: Sequence[qdrant.QdrantOperationEvidence],
        /,
    ) -> None:
        del context_provider
        operation_evidence.extend(evidence)

    async def record_outcomes(
        context_provider: ContextProvider,
        values: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        outcomes.extend(values)

    sink = target.verified_delete_sink(
        record=record_outcomes,
        operation_evidence_recorder=record_operations,
        policy=_policy(),
    )
    await sink(ContextProvider(), [first, second])

    assert first_id not in client.payloads
    assert second_id not in client.payloads
    assert [outcome.action_id for outcome in outcomes] == [
        first.descriptor.action_id,
        second.descriptor.action_id,
    ]
    assert all(outcome.status.value == "absent" for outcome in outcomes)
    delete_operation_ids = {
        evidence.action_id: evidence.operation_id
        for evidence in operation_evidence
        if evidence.operation_kind == "delete"
    }
    assert all(
        outcome.operation_id == str(delete_operation_ids[outcome.action_id])
        for outcome in outcomes
    )
    assert {
        (evidence.action_id, evidence.operation_kind) for evidence in operation_evidence
    } == {
        (first.descriptor.action_id, "suppress"),
        (first.descriptor.action_id, "delete"),
        (second.descriptor.action_id, "suppress"),
        (second.descriptor.action_id, "delete"),
    }
    mutation_calls = [
        kwargs for name, kwargs in client.calls if name in {"set_payload", "delete"}
    ]
    assert mutation_calls
    assert all(kwargs["wait"] is True for kwargs in mutation_calls)
    assert all(
        kwargs["ordering"] is qdrant_models.WriteOrdering.STRONG
        for kwargs in mutation_calls
    )


@requires_qdrant
@pytest.mark.asyncio
async def test_readback_failure_retains_completed_delete_operation_id() -> None:
    class FailingFinalReadClient(_FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.retrieve_count = 0

        def retrieve(self, **kwargs: Any) -> list[SimpleNamespace]:
            self.retrieve_count += 1
            if self.retrieve_count >= 2:
                raise TimeoutError("Bearer secret customer@example.test")
            return super().retrieve(**kwargs)

    client = FailingFinalReadClient()
    target, action, _ = _delete_setup(client)
    recorded: list[TargetVerificationOutcome] = []

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.extend(outcomes)

    with pytest.raises(TargetVerificationError):
        await target.verified_delete_sink(
            record=record,
            policy=_policy(1),
        )(ContextProvider(), [action])

    assert len(recorded) == 1
    assert recorded[0].status.value == "transport_failure"
    assert recorded[0].operation_id is not None
    assert recorded[0].operation_id.isdecimal()


@requires_qdrant
@pytest.mark.asyncio
async def test_unrequested_final_readback_id_never_closes_as_absent() -> None:
    class WrongFinalReadClient(_FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.retrieve_count = 0

        def retrieve(self, **kwargs: Any) -> list[SimpleNamespace]:
            self.retrieve_count += 1
            if self.retrieve_count >= 2:
                return [SimpleNamespace(id=str(uuid.uuid4()))]
            return super().retrieve(**kwargs)

    client = WrongFinalReadClient()
    target, action, _ = _delete_setup(client)
    recorded: list[TargetVerificationOutcome] = []

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.extend(outcomes)

    with pytest.raises(TargetVerificationError):
        await target.verified_delete_sink(
            record=record,
            policy=_policy(1),
        )(ContextProvider(), [action])

    assert len(recorded) == 1
    assert recorded[0].status.value == "transport_failure"
    assert recorded[0].operation_id is not None


@requires_qdrant
@pytest.mark.asyncio
async def test_concurrent_sink_invocations_keep_operation_evidence_isolated() -> None:
    client = _FakeClient()
    target, action, _ = _delete_setup(client)
    recorded: list[tuple[TargetVerificationOutcome, ...]] = []

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.append(tuple(outcomes))

    sink = target.verified_delete_sink(record=record, policy=_policy())
    await asyncio.gather(
        sink(ContextProvider(), [action]),
        sink(ContextProvider(), [action]),
    )

    assert len(recorded) == 2
    assert all(
        len(outcomes) == 1 and outcomes[0].operation_id is not None
        for outcomes in recorded
    )
    assert len({outcomes[0].operation_id for outcomes in recorded}) == 2


@requires_qdrant
@pytest.mark.asyncio
async def test_reentrant_sink_invocation_restores_outer_operation_evidence() -> None:
    client = _FakeClient()
    target, action, _ = _delete_setup(client)
    recorded: list[tuple[TargetVerificationOutcome, ...]] = []
    reentered = False

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.append(tuple(outcomes))

    async def after_apply(
        context_provider: ContextProvider,
        applied_action: qdrant.QdrantRevocationAction,
        /,
    ) -> None:
        nonlocal reentered
        if not reentered:
            reentered = True
            await sink(context_provider, [applied_action])

    sink = target.verified_delete_sink(
        record=record,
        after_apply=after_apply,
        policy=_policy(),
    )
    await sink(ContextProvider(), [action])

    assert len(recorded) == 2
    assert all(
        len(outcomes) == 1 and outcomes[0].operation_id is not None
        for outcomes in recorded
    )
    assert len({outcomes[0].operation_id for outcomes in recorded}) == 2


@requires_qdrant
@pytest.mark.asyncio
async def test_failed_reentrant_description_does_not_pop_outer_evidence() -> None:
    client = _FakeClient()
    target, action, _ = _delete_setup(client)
    recorded: list[TargetVerificationOutcome] = []
    reentered = False

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.extend(outcomes)

    async def before_apply(
        context_provider: ContextProvider,
        applied_action: qdrant.QdrantRevocationAction,
        /,
    ) -> None:
        nonlocal reentered
        del applied_action
        if not reentered:
            reentered = True
            with pytest.raises(TargetActionDescriptionError):
                await sink(context_provider, [cast(Any, object())])

    sink = target.verified_delete_sink(
        record=record,
        before_apply=before_apply,
        policy=_policy(),
    )
    await sink(ContextProvider(), [action])

    assert len(recorded) == 1
    assert recorded[0].operation_id is not None


@requires_qdrant
@pytest.mark.asyncio
async def test_operation_completed_but_point_visible_keeps_effect_retryable() -> None:
    client = _FakeClient()
    client.delete_visibility_reads = 100
    target, action, _ = _delete_setup(client)
    recorded: list[TargetVerificationOutcome] = []

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.extend(outcomes)

    sink = target.verified_delete_sink(record=record, policy=_policy())
    with pytest.raises(TargetVerificationError):
        await sink(ContextProvider(), [action])

    assert recorded[-1].status.value == "present"
    assert recorded[-1].attempt_count == 3
    assert recorded[-1].detail_code == "read_replica_stale"


@requires_qdrant
@pytest.mark.asyncio
async def test_eventual_absence_after_stale_reads_converges() -> None:
    client = _FakeClient()
    client.delete_visibility_reads = 2
    target, action, point_id = _delete_setup(client)
    outcomes: list[TargetVerificationOutcome] = []

    async def record(
        context_provider: ContextProvider,
        values: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        outcomes.extend(values)

    sink = target.verified_delete_sink(record=record, policy=_policy(4))
    await sink(ContextProvider(), [action])

    assert point_id not in client.payloads
    assert outcomes[-1].status.value == "absent"
    assert outcomes[-1].attempt_count == 3
    retrieve_calls = [kwargs for name, kwargs in client.calls if name == "retrieve"]
    assert all(
        call["consistency"] is qdrant_models.ReadConsistencyType.ALL
        for call in retrieve_calls
    )


@requires_qdrant
@pytest.mark.asyncio
async def test_delayed_old_generation_cannot_delete_reauthorized_point() -> None:
    client = _FakeClient()
    target, action, point_id = _delete_setup(client)
    client.payloads[point_id]["synor"] = _lineage(generation=3).payload()
    sink = target.verified_delete_sink(record=_record_noop, policy=_policy(1))
    with pytest.raises(TargetActionApplyError):
        await sink(ContextProvider(), [action])

    assert point_id in client.payloads
    assert client.payloads[point_id]["synor"]["generation"] == 3
    assert client.payloads[point_id]["synor"]["servable"] is True


@requires_qdrant
@pytest.mark.asyncio
async def test_already_absent_delete_is_idempotent_success() -> None:
    client = _FakeClient()
    target, action, point_id = _delete_setup(client)
    client.payloads.pop(point_id)

    sink = target.verified_delete_sink(record=_record_noop, policy=_policy())
    await sink(ContextProvider(), [action])

    assert any(name == "delete" for name, _ in client.calls)


@requires_qdrant
@pytest.mark.asyncio
async def test_auth_or_transport_error_is_redacted_and_retryable() -> None:
    secret = "Bearer secret customer@example.test"
    client = _FakeClient()
    client.apply_error = PermissionError(secret)
    target, action, _ = _delete_setup(client)
    sink = target.verified_delete_sink(record=_record_noop, policy=_policy())

    with pytest.raises(TargetActionApplyError) as raised:
        await sink(ContextProvider(), [action])

    assert secret not in str(raised.value)
    assert secret not in repr(raised.value)
    assert raised.value.__cause__ is None


@requires_qdrant
@pytest.mark.asyncio
async def test_acknowledged_without_completion_is_not_strict_success() -> None:
    client = _FakeClient()
    client.update_status = qdrant_models.UpdateStatus.ACKNOWLEDGED
    target, action, _ = _delete_setup(client)
    sink = target.verified_delete_sink(record=_record_noop, policy=_policy())

    with pytest.raises(TargetActionApplyError):
        await sink(ContextProvider(), [action])


@requires_qdrant
@pytest.mark.asyncio
async def test_acl_narrowing_verifies_allowed_and_denied_before_serving() -> None:
    client = _FakeClient()
    target, _, point_id = _delete_setup(client)
    new_lineage = _lineage(principals=(_digest("new-allowed"),), generation=3)

    evidence = await target.narrow_acl(
        point_id=point_id,
        new_lineage=new_lineage,
        previous_generation=2,
        denied_principal_digests=(_digest("principal-allowed"),),
        policy=_policy(),
    )

    governed = client.payloads[point_id]["synor"]
    assert governed["servable"] is True
    assert governed["generation"] == 3
    assert governed["principal_digests"] == [_digest("new-allowed")]
    assert governed["chunk_digest"] == _digest("point-a")
    assert isinstance(governed["content_fingerprint"], str)
    assert len(evidence) == 2
    assert await target._point_matches_filter(
        point_id, _context("new-allowed", generation=3)
    )
    assert not await target._point_matches_filter(
        point_id, _context("principal-allowed", generation=3)
    )


@requires_qdrant
@pytest.mark.asyncio
async def test_acl_narrowing_retry_is_idempotent() -> None:
    client = _FakeClient()
    target, _, point_id = _delete_setup(client)
    new_lineage = _lineage(principals=(_digest("new-allowed"),), generation=3)

    first = await target.narrow_acl(
        point_id=point_id,
        new_lineage=new_lineage,
        previous_generation=2,
        denied_principal_digests=(_digest("principal-allowed"),),
        policy=_policy(),
    )
    second = await target.narrow_acl(
        point_id=point_id,
        new_lineage=new_lineage,
        previous_generation=2,
        denied_principal_digests=(_digest("principal-allowed"),),
        policy=_policy(),
    )

    assert len(first) == len(second) == 2
    governed = client.payloads[point_id]["synor"]
    assert {
        key: value
        for key, value in governed.items()
        if key not in {"chunk_digest", "content_fingerprint"}
    } == new_lineage.payload()
    assert governed["chunk_digest"] == _digest("point-a")


@requires_qdrant
@pytest.mark.asyncio
async def test_acl_narrowing_requires_every_removed_principal_to_be_denied() -> None:
    client = _FakeClient()
    target, _, point_id = _delete_setup(client)
    retained = _digest("retained")
    removed = _digest("removed")
    client.payloads[point_id]["synor"].update(
        _lineage(principals=(retained, removed), generation=2).payload()
    )

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.narrow_acl(
            point_id=point_id,
            new_lineage=_lineage(principals=(retained,), generation=3),
            previous_generation=2,
            denied_principal_digests=(_digest("unrelated"),),
            policy=_policy(),
        )

    assert raised.value.code == "acl_denied_principals_incomplete"
    assert client.payloads[point_id]["synor"]["generation"] == 2
    assert removed in client.payloads[point_id]["synor"]["principal_digests"]


@requires_qdrant
@pytest.mark.asyncio
async def test_acl_same_generation_retry_rejects_conflicting_lineage() -> None:
    client = _FakeClient()
    target, _, point_id = _delete_setup(client)
    conflicting = _digest("conflicting-current")
    client.payloads[point_id]["synor"].update(
        _lineage(principals=(conflicting,), generation=3).payload()
    )

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.narrow_acl(
            point_id=point_id,
            new_lineage=_lineage(
                principals=(_digest("requested-current"),),
                generation=3,
            ),
            previous_generation=2,
            denied_principal_digests=(conflicting,),
            policy=_policy(),
        )

    assert raised.value.code == "acl_retry_conflict"
    assert client.payloads[point_id]["synor"]["principal_digests"] == [conflicting]


@requires_qdrant
@pytest.mark.asyncio
async def test_acl_post_enable_exception_restores_target_fence() -> None:
    client = _FakeClient()
    target, _, point_id = _delete_setup(client)
    client.query_error_after = 3

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.narrow_acl(
            point_id=point_id,
            new_lineage=_lineage(
                principals=(_digest("new-allowed"),),
                generation=3,
            ),
            previous_generation=2,
            denied_principal_digests=(_digest("principal-allowed"),),
            policy=_policy(1),
        )

    assert raised.value.code == "acl_verification_failed"
    assert "Bearer secret" not in str(raised.value)
    assert client.payloads[point_id]["synor"]["servable"] is False


@requires_qdrant
@pytest.mark.asyncio
async def test_acl_cancellation_cannot_be_overtaken_by_late_enable() -> None:
    client = _FakeClient()
    target, _, point_id = _delete_setup(client)
    client.block_serving_enable = True
    new_lineage = _lineage(principals=(_digest("new-allowed"),), generation=3)

    update = asyncio.create_task(
        target.narrow_acl(
            point_id=point_id,
            new_lineage=new_lineage,
            previous_generation=2,
            denied_principal_digests=(_digest("principal-allowed"),),
            policy=_policy(),
        )
    )
    assert await asyncio.to_thread(client.serving_enable_started.wait, 1)
    update.cancel()
    await asyncio.sleep(0.01)
    client.serving_enable_release.set()

    with pytest.raises(asyncio.CancelledError):
        await update
    assert client.payloads[point_id]["synor"]["servable"] is False


@requires_qdrant
@pytest.mark.asyncio
async def test_acl_pre_enable_provider_error_is_redacted_and_fenced() -> None:
    client = _FakeClient()
    target, _, point_id = _delete_setup(client)
    client.query_error_after = 1

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.narrow_acl(
            point_id=point_id,
            new_lineage=_lineage(
                principals=(_digest("new-allowed"),),
                generation=3,
            ),
            previous_generation=2,
            denied_principal_digests=(_digest("principal-allowed"),),
            policy=_policy(1),
        )

    assert raised.value.code == "acl_suppression_failed"
    assert "Bearer secret" not in str(raised.value)
    assert raised.value.__cause__ is None
    assert client.payloads[point_id]["synor"]["servable"] is False


@requires_qdrant
@pytest.mark.asyncio
async def test_acl_suppression_and_compensating_fence_failure_is_reported() -> None:
    client = _FakeClient()
    target, _, point_id = _delete_setup(client)
    client.ignore_suppression = True

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.narrow_acl(
            point_id=point_id,
            new_lineage=_lineage(principals=(_digest("new-allowed"),), generation=3),
            previous_generation=2,
            denied_principal_digests=(_digest("principal-allowed"),),
            policy=_policy(1),
        )

    assert raised.value.code == "acl_fence_unverified"


@requires_qdrant
@pytest.mark.asyncio
async def test_acl_compensation_preserves_a_newer_authorized_generation() -> None:
    client = _FakeClient()
    target, _, point_id = _delete_setup(client)
    newer_lineage = _lineage(
        principals=(_digest("newer-allowed"),),
        generation=4,
    )
    client.payloads[point_id]["synor"] = newer_lineage.payload()

    await target._refence_acl_transition(
        point_id,
        source_digest=newer_lineage.source_digest,
        previous_generation=2,
        new_generation=3,
        policy=_policy(),
    )

    governed = client.payloads[point_id]["synor"]
    assert governed["generation"] == 4
    assert governed["servable"] is True


@requires_qdrant
@pytest.mark.asyncio
async def test_multi_point_compensation_uses_one_total_verification_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient()
    target, _, first_id = _delete_setup(client, point_name="deadline-first")
    _, _, second_id = _delete_setup(client, point_name="deadline-second")
    observed_reads = 0

    async def slow_fenced_payload(
        self: qdrant.CertifiedQdrantTarget,
        point_id: str | int,
    ) -> dict[str, Any]:
        nonlocal observed_reads
        del self, point_id
        observed_reads += 1
        await asyncio.sleep(0.06)
        return {
            "synor": {
                "source_digest": _digest("source"),
                "generation": 2,
                "servable": False,
            }
        }

    monkeypatch.setattr(
        qdrant.CertifiedQdrantTarget,
        "_retrieve_payload",
        slow_fenced_payload,
    )
    policy = VerificationRetryPolicy(
        timeout=timedelta(seconds=0.1),
        max_attempts=1,
        initial_backoff=0,
        max_backoff=0,
        jitter=0,
    )

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target._refence_points(
            [first_id, second_id],
            source_digest=_digest("source"),
            generation=2,
            policy=policy,
        )

    assert raised.value.code == "target_fence_unverified"
    assert observed_reads == 2


@requires_qdrant
@pytest.mark.asyncio
async def test_strict_collection_delete_requires_negative_verification() -> None:
    client = _FakeClient()
    target = _target(client)

    await target.delete_collection_verified(policy=_policy())

    assert not client.collection_present
    assert [name for name, _ in client.calls][-2:] == [
        "delete_collection",
        "collection_exists",
    ]

    client = _FakeClient()
    client.delete_collection_result = False
    target = _target(client)
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.delete_collection_verified(policy=_policy())
    assert raised.value.code == "collection_delete_unconfirmed"

    client = _FakeClient()
    client.collection_exists_result = 0
    target = _target(client)
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.delete_collection_verified(policy=_policy())
    assert raised.value.code == "collection_presence_unconfirmed"


@requires_qdrant
@pytest.mark.asyncio
async def test_collection_absence_polling_caps_backoff_to_total_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient()
    client.collection_exists_result = True
    target = _target(client)
    observed_sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        observed_sleeps.append(delay)

    monkeypatch.setattr(
        "synor.connectors.qdrant._revocation.asyncio.sleep",
        record_sleep,
    )
    policy = VerificationRetryPolicy(
        timeout=timedelta(seconds=1),
        max_attempts=2,
        initial_backoff=5,
        max_backoff=5,
        jitter=0,
    )

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.delete_collection_verified(policy=policy)

    assert raised.value.code == "collection_absence_unverified"
    assert len(observed_sleeps) == 1
    assert 0 < observed_sleeps[0] <= policy.timeout.total_seconds()


@requires_qdrant
@pytest.mark.asyncio
async def test_collection_delete_drains_worker_before_reporting_cancellation() -> None:
    client = _FakeClient()
    client.block_collection_delete = True
    target = _target(client)

    deletion = asyncio.create_task(target.delete_collection_verified())
    assert await asyncio.to_thread(client.collection_delete_started.wait, 1)
    deletion.cancel()
    await asyncio.sleep(0.01)
    assert not deletion.done()

    client.collection_delete_release.set()
    with pytest.raises(asyncio.CancelledError):
        await deletion

    # A caller can recreate only after observing cancellation. The drained old
    # worker has already finished and therefore cannot delete this replacement.
    client.collection_present = True
    await asyncio.sleep(0.01)
    assert client.collection_present


@requires_qdrant
def test_benchmark_provider_request_batch_shapes_are_bounded() -> None:
    for size in (1, 100, 10_000, 100_000):
        actions = cast(Sequence[qdrant.QdrantRevocationAction], range(size))
        batches = qdrant.iter_revocation_batches(actions)
        batch_lengths = [len(batch) for batch in batches]
        assert sum(batch_lengths) == size
        assert max(batch_lengths) <= 256


@requires_qdrant
@pytest.mark.asyncio
async def test_capability_report_binds_detected_versions() -> None:
    target = _target(_FakeClient())
    report = await target.capability_report()

    assert report.client_version == "1.18.0"
    assert report.server_version == "1.18.0"
    assert report.replication_factor == 1
    assert report.write_consistency_factor == 1
    assert report.topology_verified
    assert report.blocking_updates_disabled
    assert report.operation_timeout_seconds == 30
    assert report.write_ordering == "strong"
    assert report.read_consistency == "all"
    assert report.capabilities.exact_id_delete
    assert not report.capabilities.external_enumeration
    assert not report.capabilities.physical_erasure_attestation
    metadata = report.to_dict()
    assert metadata["schema_version"] == 1
    assert metadata["read_consistency"] == "all"
    assert metadata["capabilities"] == report.capabilities.to_dict()


@requires_qdrant
@pytest.mark.asyncio
async def test_capability_metadata_calls_have_a_connector_owned_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient()
    client.info_delay = 0.05
    monkeypatch.setattr(qdrant_revocation, "_REQUEST_TIMEOUT_SECONDS", 0.001)

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()

    assert raised.value.code == "provider_timeout"
    assert raised.value.__cause__ is None


@requires_qdrant
@pytest.mark.asyncio
async def test_capability_preflight_redacts_provider_exception_text() -> None:
    secret = "Bearer secret customer@example.test"
    client = _FakeClient()
    client.info_error = RuntimeError(secret)

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).query_points(context=_context())

    assert raised.value.code == "provider_metadata_failed"
    assert secret not in str(raised.value)
    assert raised.value.__cause__ is None


@requires_qdrant
@pytest.mark.asyncio
async def test_capability_report_rejects_unsafe_collection_configuration() -> None:
    client = _FakeClient()
    client.replication_factor = 2
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()
    assert raised.value.code == "write_consistency_incomplete"

    client = _FakeClient()
    client.payload_schema.pop("synor.servable")
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()
    assert raised.value.code == "payload_indexes_missing"

    client = _FakeClient()
    client.payload_schema["synor.servable"] = SimpleNamespace(
        data_type=qdrant_models.PayloadSchemaType.KEYWORD
    )
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()
    assert raised.value.code == "payload_index_type_mismatch"

    client = _FakeClient()
    client.collection_status = qdrant_models.CollectionStatus.YELLOW
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()
    assert raised.value.code == "collection_not_green"

    client = _FakeClient()
    client.prevent_unoptimized = True
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()
    assert raised.value.code == "blocking_updates_enabled"

    client = _FakeClient()
    client.strict_mode_enabled = True
    client.strict_max_timeout = 10
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()
    assert raised.value.code == "strict_timeout_too_small"

    client = _FakeClient()
    client.strict_mode_enabled = True
    client.strict_max_query_limit = 9
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()
    assert raised.value.code == "strict_query_limit_too_small"

    client = _FakeClient()
    client.strict_mode_enabled = True
    client.strict_max_batchsize = 100
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()
    assert raised.value.code == "strict_batch_too_small"

    client = _FakeClient()
    client.strict_mode_enabled = True
    client.strict_filter_max_conditions = 10
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()
    assert raised.value.code == "strict_filter_conditions_too_small"

    client = _FakeClient()
    client.strict_mode_enabled = True
    client.strict_condition_max_size = 255
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()
    assert raised.value.code == "strict_condition_size_too_small"

    client = _FakeClient()
    client.replication_factor = None
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()
    assert raised.value.code == "replication_factor_invalid"

    client = _FakeClient()
    client.write_consistency_factor = None
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()
    assert raised.value.code == "write_consistency_factor_invalid"

    client = _FakeClient()
    client.replication_factor = 2
    client.write_consistency_factor = 2
    report = await _target(client).capability_report()
    assert report.topology_verified

    client.topology_healthy = False
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await _target(client).capability_report()
    assert raised.value.code == "replica_topology_unhealthy"


@requires_qdrant
@pytest.mark.asyncio
async def test_unsupported_server_fails_before_strict_mutation() -> None:
    client = _FakeClient()
    client.server_version = "1.15.9"
    target = _target(client)
    point = qdrant.governed_point(
        lineage=_lineage(),
        chunk_digest=_digest("unsupported-server"),
        vector=[0.1],
    )

    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.upsert_governed([point], mode="insert_only")

    assert raised.value.code == "server_version_unsupported"
    assert not any(name == "upsert" for name, _ in client.calls)

    client.server_version = "1.18.0-rc1"
    with pytest.raises(qdrant.QdrantCertificationError) as raised:
        await target.upsert_governed([point], mode="insert_only")
    assert raised.value.code == "server_version_invalid"


@requires_qdrant
@requires_qdrant_url
@pytest.mark.asyncio
async def test_live_certified_logical_absence() -> None:
    client = qdrant.create_client(
        os.environ["QDRANT_URL"],
        prefer_grpc=True,
        timeout=30,
    )
    collection_name = f"synor_revocation_{uuid.uuid4().hex}"
    client.create_collection(
        collection_name=collection_name,
        vectors_config=qdrant_models.VectorParams(
            size=2,
            distance=qdrant_models.Distance.COSINE,
        ),
    )
    target = qdrant.CertifiedQdrantTarget(
        client,
        collection_name,
        lineage_authorizer=_authorize_lineage,
        query_context_verifier=_verify_query_context,
    )
    lineage = _lineage()
    point = qdrant.governed_point(
        lineage=lineage,
        chunk_digest=_digest("live-chunk"),
        vector=[0.1, 0.2],
    )
    action = target.delete_action(
        point_id=cast(str, point.id),
        query_context=_context(),
        action_id=_digest("live-action"),
        source_digest=lineage.source_digest,
        source_generation=lineage.generation,
    )
    try:
        await target.provision_payload_indexes()
        await target.capability_report()
        await target.upsert_governed([point], mode="insert_only")
        assert len((await target.query_points(context=_context())).points) == 1
        await target.verified_delete_sink(
            record=_record_noop,
            policy=_policy(8),
        )(ContextProvider(), [action])
        assert not (await target.query_points(context=_context())).points
    finally:
        await target.delete_collection_verified(policy=_policy(8))


@requires_qdrant
@requires_qdrant_cluster_url
@pytest.mark.asyncio
async def test_live_cluster_full_write_acknowledgement_and_logical_absence() -> None:
    client = qdrant.create_client(
        os.environ["QDRANT_CLUSTER_URL"],
        prefer_grpc=True,
        timeout=30,
    )
    collection_name = f"synor_revocation_cluster_{uuid.uuid4().hex}"
    client.create_collection(
        collection_name=collection_name,
        vectors_config=qdrant_models.VectorParams(
            size=2,
            distance=qdrant_models.Distance.COSINE,
        ),
        replication_factor=2,
        write_consistency_factor=2,
    )
    target = qdrant.CertifiedQdrantTarget(
        client,
        collection_name,
        lineage_authorizer=_authorize_lineage,
        query_context_verifier=_verify_query_context,
    )
    lineage = _lineage()
    point = qdrant.governed_point(
        lineage=lineage,
        chunk_digest=_digest("cluster-chunk"),
        vector=[0.1, 0.2],
    )
    action = target.delete_action(
        point_id=cast(str, point.id),
        query_context=_context(),
        action_id=_digest("cluster-action"),
        source_digest=lineage.source_digest,
        source_generation=lineage.generation,
    )
    try:
        await target.provision_payload_indexes()
        await target.capability_report()
        await target.upsert_governed([point], mode="insert_only")
        await target.verified_delete_sink(
            record=_record_noop,
            policy=_policy(8),
        )(ContextProvider(), [action])
        assert not client.retrieve(
            collection_name,
            [cast(str, point.id)],
            consistency=qdrant_models.ReadConsistencyType.ALL,
        )
    finally:
        await target.delete_collection_verified(policy=_policy(8))
