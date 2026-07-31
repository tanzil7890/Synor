"""Certified Qdrant operations for governed index revocation.

The compatibility target in :mod:`synor.connectors.qdrant._target` remains
unchanged.  This module is an additive, opt-in boundary for strict workflows:

* governed points carry deterministic, filterable lineage;
* every supported query is source-scoped and requires a current-state verifier;
* destructive effects suppress serving before exact-ID deletion;
* writes wait for completion with strong ordering;
* verification combines full write acknowledgements with exact-ID read-back;
* the shared verified sink records outcomes before Synor can final-commit.

The connector proves non-return through its documented, guarded Qdrant query
contract.  It does not claim that an empty ``consistency=all`` read proves
physical absence on every replica, or physical erasure from storage segments,
snapshots, or backups.
"""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import importlib.metadata
import json
import math
import random
import re
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, ParamSpec, Protocol, TypeVar, cast

import grpc  # type: ignore[import-untyped]
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from qdrant_client.http.exceptions import UnexpectedResponse

from synor._internal.context_keys import ContextProvider
from synor._internal.revocation_model import (
    EffectDescriptor,
    EffectOperation,
    TargetRevocationCapabilities,
    VerificationOutcome,
)
from synor._internal.verified_sink import (
    AsyncOutcomeRecorder,
    TargetVerificationResult,
    VerificationRetryPolicy,
    VerifiedTargetActionSink,
)
from ._target import _PointId, _validate_point_id


_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
_POINT_NAMESPACE = uuid.UUID("d59bd27a-9ee0-5c7f-b64d-c8ed9b8c850e")
_DEFAULT_BATCH_SIZE = 256
_REQUEST_TIMEOUT_SECONDS = 30
_MIN_STRICT_FILTER_CONDITIONS = 11
_MIN_STRICT_QUERY_LIMIT = 10
_CERTIFIED_QUERY_KWARGS = frozenset(
    {
        "using",
        "search_params",
        "limit",
        "offset",
        "with_payload",
        "with_vectors",
        "score_threshold",
        "shard_key_selector",
    }
)
_CERTIFIED_CLIENT_MIN = (1, 17, 0)
_CERTIFIED_CLIENT_MAX = (1, 19, 0)
_CERTIFIED_SERVER_MIN = (1, 17, 0)
_CERTIFIED_SERVER_MAX = (1, 19, 0)
_P = ParamSpec("_P")
_T = TypeVar("_T")
_GOVERNANCE_PAYLOAD_INDEXES: dict[str, qdrant_models.PayloadSchemaType] = {
    "synor.contract_version": qdrant_models.PayloadSchemaType.KEYWORD,
    "synor.source_digest": qdrant_models.PayloadSchemaType.KEYWORD,
    "synor.policy_id": qdrant_models.PayloadSchemaType.KEYWORD,
    "synor.policy_revision": qdrant_models.PayloadSchemaType.KEYWORD,
    "synor.group_graph_revision": qdrant_models.PayloadSchemaType.KEYWORD,
    "synor.tenant": qdrant_models.PayloadSchemaType.KEYWORD,
    "synor.generation": qdrant_models.PayloadSchemaType.INTEGER,
    "synor.principal_digests": qdrant_models.PayloadSchemaType.KEYWORD,
    "synor.servable": qdrant_models.PayloadSchemaType.BOOL,
    "synor.retention_state": qdrant_models.PayloadSchemaType.KEYWORD,
}

QDRANT_REVOCATION_CAPABILITIES = TargetRevocationCapabilities(
    # This means the governed serving boundary is fenced before deletion and
    # verified before success. It is not a distributed transaction or an
    # all-or-nothing multi-point/replica write claim.
    atomic_serving_suppression=True,
    exact_id_delete=True,
    source_id_bulk_delete=False,
    query_time_acl_filter=True,
    tenant_isolation=True,
    synchronous_acknowledgement=True,
    consistency_fence=True,
    negative_read_verification=True,
    external_enumeration=False,
    legal_hold_isolation=False,
    physical_erasure_attestation=False,
    capability_version="qdrant-revocation-v1",
)


class QdrantCertificationError(RuntimeError):
    """A privacy-safe certified-connector failure."""

    code: str

    def __init__(self, code: str) -> None:
        if not _SAFE_TOKEN.fullmatch(code):
            raise ValueError("Qdrant certification error code must be safe")
        self.code = code
        super().__init__(f"Qdrant certified operation failed: {code}")


@dataclass(frozen=True, slots=True)
class QdrantCapabilityReport:
    """Detected versions and the factual strict capability contract."""

    client_version: str
    server_version: str
    collection_status: str
    replication_factor: int
    write_consistency_factor: int
    indexed_fields: frozenset[str]
    topology_verified: bool
    blocking_updates_disabled: bool
    capabilities: TargetRevocationCapabilities = QDRANT_REVOCATION_CAPABILITIES
    write_ordering: str = qdrant_models.WriteOrdering.STRONG.value
    read_consistency: str = qdrant_models.ReadConsistencyType.ALL.value
    operation_timeout_seconds: int = _REQUEST_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        _validate_version_pair(self.client_version, self.server_version)
        if self.collection_status != "green":
            raise QdrantCertificationError("collection_not_green")
        for name, value in (
            ("replication_factor", self.replication_factor),
            ("write_consistency_factor", self.write_consistency_factor),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise QdrantCertificationError(f"{name}_invalid")
        if self.write_consistency_factor != self.replication_factor:
            raise QdrantCertificationError("write_consistency_incomplete")
        if self.topology_verified is not True:
            raise QdrantCertificationError("replica_topology_unhealthy")
        if self.blocking_updates_disabled is not True:
            raise QdrantCertificationError("blocking_updates_enabled")
        missing_indexes = _GOVERNANCE_PAYLOAD_INDEXES.keys() - self.indexed_fields
        if missing_indexes:
            raise QdrantCertificationError("payload_indexes_missing")

    def to_dict(self) -> dict[str, object]:
        """Return the versioned, metadata-only deployment capability report."""

        return {
            "schema_version": 1,
            "client_version": self.client_version,
            "server_version": self.server_version,
            "collection_status": self.collection_status,
            "replication_factor": self.replication_factor,
            "write_consistency_factor": self.write_consistency_factor,
            "indexed_fields": sorted(self.indexed_fields),
            "topology_verified": self.topology_verified,
            "blocking_updates_disabled": self.blocking_updates_disabled,
            "capabilities": self.capabilities.to_dict(),
            "write_ordering": self.write_ordering,
            "read_consistency": self.read_consistency,
            "operation_timeout_seconds": self.operation_timeout_seconds,
        }


@dataclass(frozen=True, slots=True)
class GovernedPointLineage:
    """Filterable Qdrant lineage containing only opaque governance metadata."""

    source_digest: str
    source_revision: str
    policy_id: str
    policy_revision: str
    group_graph_revision: str
    tenant_digest: str
    owner_component_digest: str
    generation: int
    principal_digests: tuple[str, ...]
    retention_state: Literal["active", "retained_isolated"] = "active"
    servable: bool = True

    def __post_init__(self) -> None:
        _require_digest("source_digest", self.source_digest)
        _require_token("source_revision", self.source_revision)
        _require_token("policy_id", self.policy_id)
        _require_token("policy_revision", self.policy_revision)
        _require_token("group_graph_revision", self.group_graph_revision)
        _require_digest("tenant_digest", self.tenant_digest)
        _require_digest("owner_component_digest", self.owner_component_digest)
        if (
            not isinstance(self.generation, int)
            or isinstance(self.generation, bool)
            or self.generation < 1
        ):
            raise ValueError("generation must be a positive integer")
        if not self.principal_digests:
            raise ValueError("principal_digests must not be empty")
        if len(set(self.principal_digests)) != len(self.principal_digests):
            raise ValueError("principal_digests must be unique")
        for principal_digest in self.principal_digests:
            _require_digest("principal_digest", principal_digest)
        if self.retention_state not in ("active", "retained_isolated"):
            raise ValueError("retention_state is not supported")
        if type(self.servable) is not bool:
            raise TypeError("servable must be a bool")
        if self.retention_state == "retained_isolated" and self.servable:
            raise ValueError("retained-isolated points must not be servable")

    def payload(self, *, servable: bool | None = None) -> dict[str, object]:
        """Return the controlled nested ``synor`` payload."""

        serving_state = self.servable if servable is None else servable
        if type(serving_state) is not bool:
            raise TypeError("servable override must be a bool")
        if self.retention_state == "retained_isolated" and serving_state:
            raise ValueError("retained-isolated points must not be servable")
        return {
            "source_digest": self.source_digest,
            "source_revision": self.source_revision,
            "policy_id": self.policy_id,
            "policy_revision": self.policy_revision,
            "group_graph_revision": self.group_graph_revision,
            "tenant": self.tenant_digest,
            "owner_component": self.owner_component_digest,
            "generation": self.generation,
            "principal_digests": list(self.principal_digests),
            "servable": serving_state,
            "retention_state": self.retention_state,
            "contract_version": "1",
        }


@dataclass(frozen=True, slots=True)
class _GovernedPointMetadata:
    """Connector-owned metadata that binds lineage to one exact point body."""

    lineage: GovernedPointLineage
    chunk_digest: str
    content_fingerprint: str

    def __post_init__(self) -> None:
        _require_digest("chunk_digest", self.chunk_digest)
        _require_digest("content_fingerprint", self.content_fingerprint)

    def payload(self, *, servable: bool | None = None) -> dict[str, object]:
        payload = self.lineage.payload(servable=servable)
        payload["chunk_digest"] = self.chunk_digest
        payload["content_fingerprint"] = self.content_fingerprint
        return payload


@dataclass(frozen=True, slots=True)
class CertifiedQueryContext:
    """Trusted, source-scoped authorization context for one guarded query."""

    source_digest: str
    tenant_digest: str
    policy_id: str
    policy_revision: str
    group_graph_revision: str
    generation: int
    principal_digest: str

    def __post_init__(self) -> None:
        _require_digest("source_digest", self.source_digest)
        _require_digest("tenant_digest", self.tenant_digest)
        _require_token("policy_id", self.policy_id)
        _require_token("policy_revision", self.policy_revision)
        _require_token("group_graph_revision", self.group_graph_revision)
        _require_digest("principal_digest", self.principal_digest)
        if (
            not isinstance(self.generation, int)
            or isinstance(self.generation, bool)
            or self.generation < 1
        ):
            raise ValueError("generation must be a positive integer")


@dataclass(frozen=True, slots=True)
class QdrantRevocationAction:
    """One exact-point destructive effect correlated to a runtime obligation."""

    point_id: _PointId
    query_context: CertifiedQueryContext
    descriptor: EffectDescriptor

    def __post_init__(self) -> None:
        object.__setattr__(self, "point_id", _validate_point_id(self.point_id))
        if self.descriptor.operation_kind is not EffectOperation.DELETE:
            raise ValueError("Qdrant strict revocation supports exact delete effects")

    def __synor_effect_descriptor__(self) -> EffectDescriptor:
        return self.descriptor

    def __synor_audit_metadata__(self) -> dict[str, object]:
        """Exclude the provider-native point ID from general run evidence."""

        return {
            "operation_kind": self.descriptor.operation_kind.value,
            "action_id": self.descriptor.action_id,
            "source_digest": self.descriptor.source_digest,
            "source_generation": self.descriptor.source_generation,
            "target_locator_digest": self.descriptor.target_locator_digest,
        }


@dataclass(frozen=True, slots=True)
class QdrantOperationEvidence:
    """Privacy-safe provider acknowledgement captured before final commit."""

    action_id: str
    operation_kind: Literal["suppress", "delete", "acl_update"]
    operation_id: int | None
    status: str

    def __post_init__(self) -> None:
        _require_token("action_id", self.action_id)
        if self.operation_id is not None and (
            not isinstance(self.operation_id, int)
            or isinstance(self.operation_id, bool)
            or self.operation_id < 0
        ):
            raise ValueError("operation_id must be a non-negative integer or None")
        if self.status != qdrant_models.UpdateStatus.COMPLETED.value:
            raise ValueError("only completed Qdrant operations may be evidence")


OperationEvidenceRecorder = Callable[
    [ContextProvider, Sequence[QdrantOperationEvidence]], Awaitable[None]
]
ActionBoundaryHook = Callable[
    [ContextProvider, QdrantRevocationAction], Awaitable[None]
]
LineageAuthorizationVerifier = Callable[[GovernedPointLineage], Awaitable[bool]]
QueryContextVerifier = Callable[[CertifiedQueryContext], Awaitable[bool]]


class _SuppressionSnapshotProvider(Protocol):
    async def snapshot_many(
        self,
        source_digests: tuple[str, ...],
    ) -> object: ...


class SuppressionBackedQdrantVerifier:
    """Bind governed Qdrant writes and reads to Phase 2 suppression state."""

    __slots__ = ("_state",)

    def __init__(self, state: _SuppressionSnapshotProvider) -> None:
        if not callable(getattr(state, "snapshot_many", None)):
            raise TypeError("state must provide snapshot_many()")
        self._state = state

    async def authorize_lineage(self, lineage: GovernedPointLineage) -> bool:
        """Return whether this lineage matches current Phase 2 suppression state."""

        if not isinstance(lineage, GovernedPointLineage):
            raise TypeError("lineage must be GovernedPointLineage")
        return await self._matches_current(
            source_digest=lineage.source_digest,
            tenant_digest=lineage.tenant_digest,
            policy_id=lineage.policy_id,
            policy_revision=lineage.policy_revision,
            group_graph_revision=lineage.group_graph_revision,
            generation=lineage.generation,
        )

    async def verify_query_context(self, context: CertifiedQueryContext) -> bool:
        """Return whether this context matches current Phase 2 suppression state."""

        if not isinstance(context, CertifiedQueryContext):
            raise TypeError("context must be CertifiedQueryContext")
        return await self._matches_current(
            source_digest=context.source_digest,
            tenant_digest=context.tenant_digest,
            policy_id=context.policy_id,
            policy_revision=context.policy_revision,
            group_graph_revision=context.group_graph_revision,
            generation=context.generation,
        )

    async def _matches_current(
        self,
        *,
        source_digest: str,
        tenant_digest: str,
        policy_id: str,
        policy_revision: str,
        group_graph_revision: str,
        generation: int,
    ) -> bool:
        snapshot = await self._state.snapshot_many((source_digest,))
        epoch = getattr(snapshot, "epoch", None)
        records = getattr(snapshot, "records", None)
        if (
            not isinstance(epoch, int)
            or isinstance(epoch, bool)
            or epoch < 0
            or not isinstance(records, Mapping)
        ):
            raise QdrantCertificationError("suppression_snapshot_invalid")
        record = records.get(source_digest)
        if record is None:
            return False
        return (
            getattr(record, "source_digest", None) == source_digest
            and getattr(record, "tenant_digest", None) == tenant_digest
            and getattr(record, "policy_id", None) == policy_id
            and getattr(record, "policy_revision", None) == policy_revision
            and getattr(record, "group_graph_revision", None) == group_graph_revision
            and getattr(record, "generation", None) == generation
            and getattr(record, "suppressed", None) is False
            and getattr(record, "verified_authorization", None) is True
        )


def deterministic_point_id(source_digest: str, chunk_digest: str) -> str:
    """Derive a stable UUID without storing raw source or chunk identifiers."""

    _require_digest("source_digest", source_digest)
    _require_digest("chunk_digest", chunk_digest)
    return str(uuid.uuid5(_POINT_NAMESPACE, f"{source_digest}:{chunk_digest}"))


def governed_point(
    *,
    lineage: GovernedPointLineage,
    chunk_digest: str,
    vector: Any,
    payload: dict[str, Any] | None = None,
) -> qdrant_models.PointStruct:
    """Build a deterministic point with connector-owned governed metadata."""

    point_payload = dict(payload or {})
    if "synor" in point_payload:
        raise ValueError("payload key 'synor' is reserved by the governed connector")
    point_id = deterministic_point_id(lineage.source_digest, chunk_digest)
    normalized = qdrant_models.PointStruct(
        id=point_id,
        vector=vector,
        payload=point_payload,
    )
    normalized_payload = dict(normalized.payload or {})
    metadata = _GovernedPointMetadata(
        lineage=lineage,
        chunk_digest=chunk_digest,
        content_fingerprint=_point_content_fingerprint(
            normalized.vector,
            normalized_payload,
        ),
    )
    normalized_payload["synor"] = metadata.payload()
    return qdrant_models.PointStruct(
        id=deterministic_point_id(lineage.source_digest, chunk_digest),
        vector=normalized.vector,
        payload=normalized_payload,
    )


def certified_query_filter(
    context: CertifiedQueryContext,
    *,
    additional_filter: qdrant_models.Filter | None = None,
    point_ids: Sequence[_PointId] | None = None,
) -> qdrant_models.Filter:
    """Build the mandatory query guard for a governed Qdrant collection."""

    if not isinstance(context, CertifiedQueryContext):
        raise TypeError("context must be a CertifiedQueryContext")
    must: list[qdrant_models.Condition] = [
        _match("synor.contract_version", "1"),
        _match("synor.source_digest", context.source_digest),
        _match("synor.tenant", context.tenant_digest),
        _match("synor.servable", True),
        _match("synor.retention_state", "active"),
        _match("synor.policy_id", context.policy_id),
        _match("synor.policy_revision", context.policy_revision),
        _match("synor.group_graph_revision", context.group_graph_revision),
        _match("synor.generation", context.generation),
        _match("synor.principal_digests", context.principal_digest),
    ]
    if point_ids is not None:
        validated = [_validate_point_id(point_id) for point_id in point_ids]
        if not validated:
            raise ValueError("point_ids must not be empty when supplied")
        must.append(
            qdrant_models.HasIdCondition(
                has_id=cast(list[qdrant_models.ExtendedPointId], validated)
            )
        )
    if additional_filter is not None:
        if not isinstance(additional_filter, qdrant_models.Filter):
            raise TypeError("additional_filter must be a Qdrant Filter")
        must.append(additional_filter)
    return qdrant_models.Filter(must=must)


def _normalize_certified_query(
    query: object,
) -> list[float] | list[list[float]] | qdrant_models.SparseVector | None:
    """Snapshot the raw-vector-only query surface used by the strict guard."""

    error = "certified query accepts only raw dense or sparse vectors"
    if query is None:
        return None
    if isinstance(query, qdrant_models.SparseVector):
        indices = list(query.indices)
        values = list(query.values)
        if (
            not indices
            or len(indices) != len(values)
            or any(
                not isinstance(index, int) or isinstance(index, bool) or index < 0
                for index in indices
            )
            or len(set(indices)) != len(indices)
        ):
            raise ValueError(error)
        normalized_values = _normalize_dense_vector(values, error=error)
        return qdrant_models.SparseVector(
            indices=indices,
            values=normalized_values,
        )
    if not isinstance(query, list) or not query:
        raise ValueError(error)
    if all(isinstance(item, list) for item in query):
        rows = cast(list[list[object]], query)
        if any(not row for row in rows) or len({len(row) for row in rows}) != 1:
            raise ValueError(error)
        return [_normalize_dense_vector(row, error=error) for row in rows]
    if any(isinstance(item, list) for item in query):
        raise ValueError(error)
    return _normalize_dense_vector(query, error=error)


def _normalize_dense_vector(values: Sequence[object], *, error: str) -> list[float]:
    normalized: list[float] = []
    for value in values:
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
        ):
            raise ValueError(error)
        normalized.append(float(value))
    return normalized


def iter_revocation_batches(
    actions: Sequence[QdrantRevocationAction],
    *,
    batch_size: int = _DEFAULT_BATCH_SIZE,
) -> Iterator[tuple[QdrantRevocationAction, ...]]:
    """Yield bounded batches for individual provider requests and read-backs."""

    if (
        not isinstance(batch_size, int)
        or isinstance(batch_size, bool)
        or batch_size < 1
        or batch_size > 4096
    ):
        raise ValueError("batch_size must be between 1 and 4096")
    for start in range(0, len(actions), batch_size):
        yield tuple(actions[start : start + batch_size])


class CertifiedQdrantTarget:
    """Opt-in strict mutation and retrieval boundary for one collection."""

    __slots__ = (
        "_batch_size",
        "_client",
        "_collection_name",
        "_lineage_authorizer",
        "_query_context_verifier",
    )

    def __init__(
        self,
        client: QdrantClient,
        collection_name: str,
        *,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        lineage_authorizer: LineageAuthorizationVerifier | None = None,
        query_context_verifier: QueryContextVerifier | None = None,
    ) -> None:
        if not collection_name or collection_name != collection_name.strip():
            raise ValueError("collection_name must be non-empty and canonical")
        # Validate once using the same bounded contract as the public batch helper.
        tuple(iter_revocation_batches((), batch_size=batch_size))
        self._client = client
        self._collection_name = collection_name
        self._batch_size = batch_size
        self._lineage_authorizer = lineage_authorizer
        self._query_context_verifier = query_context_verifier

    @property
    def capabilities(self) -> TargetRevocationCapabilities:
        return QDRANT_REVOCATION_CAPABILITIES

    async def capability_report(self) -> QdrantCapabilityReport:
        """Revalidate versions and collection invariants for a strict operation."""

        client_version, server_version = await self._require_supported_versions()
        collection_info = await _bounded_metadata_call(
            self._client.get_collection,
            collection_name=self._collection_name,
        )
        status = _enum_value(getattr(collection_info, "status", None))
        config = getattr(collection_info, "config", None)
        params = getattr(config, "params", None)
        replication_factor = _positive_config_value(
            getattr(params, "replication_factor", None),
            field="replication_factor",
        )
        write_consistency_factor = _positive_config_value(
            getattr(params, "write_consistency_factor", None),
            field="write_consistency_factor",
        )
        if write_consistency_factor != replication_factor:
            raise QdrantCertificationError("write_consistency_incomplete")
        payload_schema = getattr(collection_info, "payload_schema", None)
        if not isinstance(payload_schema, Mapping):
            raise QdrantCertificationError("payload_schema_invalid")
        indexed_fields = frozenset(
            key for key in payload_schema if isinstance(key, str)
        )
        if len(indexed_fields) != len(payload_schema):
            raise QdrantCertificationError("payload_schema_invalid")
        if _GOVERNANCE_PAYLOAD_INDEXES.keys() - indexed_fields:
            raise QdrantCertificationError("payload_indexes_missing")
        for field_name, expected_type in _GOVERNANCE_PAYLOAD_INDEXES.items():
            index_info = payload_schema.get(field_name)
            observed_type = getattr(index_info, "data_type", None)
            if observed_type is not expected_type:
                raise QdrantCertificationError("payload_index_type_mismatch")
        optimizer_config = getattr(config, "optimizer_config", None)
        prevent_unoptimized = getattr(
            optimizer_config,
            "prevent_unoptimized",
            None,
        )
        if prevent_unoptimized not in (None, False, True):
            raise QdrantCertificationError("prevent_unoptimized_invalid")
        blocking_updates_disabled = prevent_unoptimized is not True
        strict_mode_config = getattr(config, "strict_mode_config", None)
        if strict_mode_config is not None:
            strict_enabled = getattr(strict_mode_config, "enabled", None)
            if strict_enabled not in (None, False, True):
                raise QdrantCertificationError("strict_mode_invalid")
            if strict_enabled is True:
                strict_max_timeout = getattr(
                    strict_mode_config,
                    "max_timeout",
                    None,
                )
                if strict_max_timeout is not None and (
                    not isinstance(strict_max_timeout, int)
                    or isinstance(strict_max_timeout, bool)
                    or strict_max_timeout < _REQUEST_TIMEOUT_SECONDS
                ):
                    raise QdrantCertificationError("strict_timeout_too_small")
                strict_max_query_limit = getattr(
                    strict_mode_config,
                    "max_query_limit",
                    None,
                )
                if strict_max_query_limit is not None and (
                    not isinstance(strict_max_query_limit, int)
                    or isinstance(strict_max_query_limit, bool)
                    or strict_max_query_limit < _MIN_STRICT_QUERY_LIMIT
                ):
                    raise QdrantCertificationError("strict_query_limit_too_small")
                strict_max_batch = getattr(
                    strict_mode_config,
                    "upsert_max_batchsize",
                    None,
                )
                if strict_max_batch is not None and (
                    not isinstance(strict_max_batch, int)
                    or isinstance(strict_max_batch, bool)
                    or strict_max_batch < self._batch_size
                ):
                    raise QdrantCertificationError("strict_batch_too_small")
                strict_filter_max_conditions = getattr(
                    strict_mode_config,
                    "filter_max_conditions",
                    None,
                )
                required_filter_conditions = max(
                    self._batch_size,
                    _MIN_STRICT_FILTER_CONDITIONS,
                )
                if strict_filter_max_conditions is not None and (
                    not isinstance(strict_filter_max_conditions, int)
                    or isinstance(strict_filter_max_conditions, bool)
                    or strict_filter_max_conditions < required_filter_conditions
                ):
                    raise QdrantCertificationError("strict_filter_conditions_too_small")
                strict_condition_max_size = getattr(
                    strict_mode_config,
                    "condition_max_size",
                    None,
                )
                if strict_condition_max_size is not None and (
                    not isinstance(strict_condition_max_size, int)
                    or isinstance(strict_condition_max_size, bool)
                    or strict_condition_max_size < self._batch_size
                ):
                    raise QdrantCertificationError("strict_condition_size_too_small")
        topology_verified = True
        if replication_factor > 1:
            cluster_info = await _bounded_metadata_call(
                self._client.collection_cluster_info,
                collection_name=self._collection_name,
            )
            topology_verified = _cluster_topology_is_healthy(
                cluster_info,
                replication_factor=replication_factor,
            )
        return QdrantCapabilityReport(
            client_version=client_version,
            server_version=server_version,
            collection_status=status,
            replication_factor=replication_factor,
            write_consistency_factor=write_consistency_factor,
            indexed_fields=indexed_fields,
            topology_verified=topology_verified,
            blocking_updates_disabled=blocking_updates_disabled,
        )

    async def provision_payload_indexes(
        self,
    ) -> tuple[tuple[int | None, str], ...]:
        """Create the fixed governance indexes, then revalidate the collection."""

        await self._require_supported_versions()

        results: list[tuple[int | None, str]] = []
        for field_name, field_schema in _GOVERNANCE_PAYLOAD_INDEXES.items():
            result = await asyncio.to_thread(
                self._client.create_payload_index,
                collection_name=self._collection_name,
                field_name=field_name,
                field_schema=field_schema,
                wait=True,
                ordering=qdrant_models.WriteOrdering.STRONG,
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            results.append(_completed_result(result))
        await self.capability_report()
        return tuple(results)

    def point_locator_digest(self, point_id: _PointId) -> str:
        """Return the receipt-safe digest bound to this collection and point."""

        validated = _validate_point_id(point_id)
        return hashlib.sha256(
            f"qdrant:{self._collection_name}:{_canonical_point_id(validated)}".encode()
        ).hexdigest()

    def delete_action(
        self,
        *,
        point_id: _PointId,
        query_context: CertifiedQueryContext,
        action_id: str,
        source_digest: str,
        source_generation: int,
    ) -> QdrantRevocationAction:
        """Construct a descriptor-bound exact delete action."""

        descriptor = EffectDescriptor(
            operation_kind=EffectOperation.DELETE,
            action_id=action_id,
            source_digest=source_digest,
            source_generation=source_generation,
            target_locator_digest=self.point_locator_digest(point_id),
        )
        return QdrantRevocationAction(
            point_id=point_id,
            query_context=query_context,
            descriptor=descriptor,
        )

    async def upsert_governed(
        self,
        points: Sequence[qdrant_models.PointStruct],
        *,
        mode: Literal["insert_only", "update_only"],
    ) -> tuple[tuple[int | None, str], ...]:
        """Write governed points without an ambiguous provider upsert."""

        if mode not in ("insert_only", "update_only"):
            raise ValueError("mode must be 'insert_only' or 'update_only'")
        point_batch = tuple(points)
        if not point_batch:
            raise ValueError("points must not be empty")
        await self._require_certified()
        grouped: dict[
            tuple[str, int],
            list[tuple[qdrant_models.PointStruct, _GovernedPointMetadata]],
        ] = {}
        point_ids_seen: set[str] = set()
        for point in point_batch:
            metadata = _metadata_from_point(point)
            canonical_id = _canonical_point_id(point.id)
            if canonical_id in point_ids_seen:
                raise ValueError("points must not contain duplicate governed IDs")
            point_ids_seen.add(canonical_id)
            lineage = metadata.lineage
            await self._require_lineage_authorized(metadata.lineage)
            grouped.setdefault(
                (lineage.source_digest, lineage.generation),
                [],
            ).append((point, metadata))

        results: list[tuple[int | None, str]] = []
        for (source_digest, generation), entries in grouped.items():
            expected_lineage = entries[0][1].lineage
            if any(metadata.lineage != expected_lineage for _, metadata in entries):
                raise ValueError("one source generation must use one governed lineage")
            for start in range(0, len(entries), self._batch_size):
                entry_batch = entries[start : start + self._batch_size]
                point_ids = [_validate_point_id(point.id) for point, _ in entry_batch]
                update_filter = qdrant_models.Filter(
                    must=[
                        _match("synor.source_digest", source_digest),
                        qdrant_models.FieldCondition(
                            key="synor.generation",
                            range=qdrant_models.Range(lte=generation),
                        ),
                    ]
                )
                try:
                    result = await asyncio.to_thread(
                        self._client.upsert,
                        collection_name=self._collection_name,
                        points=[
                            _point_with_servable(point, metadata, servable=False)
                            for point, metadata in entry_batch
                        ],
                        wait=True,
                        ordering=qdrant_models.WriteOrdering.STRONG,
                        update_filter=update_filter,
                        update_mode=qdrant_models.UpdateMode(mode),
                        timeout=_REQUEST_TIMEOUT_SECONDS,
                    )
                    results.append(_completed_result(result))
                    for point_id, (_, metadata) in zip(
                        point_ids,
                        entry_batch,
                        strict=True,
                    ):
                        record = await self._retrieve_point(
                            point_id,
                            with_vectors=False,
                        )
                        governed = _governed_payload(record)
                        staged = governed == metadata.payload(servable=False)
                        idempotent_insert = (
                            mode == "insert_only" and governed == metadata.payload()
                        )
                        if not staged and not idempotent_insert:
                            raise QdrantCertificationError("upsert_lineage_unverified")
                        await self._require_lineage_authorized(metadata.lineage)
                except asyncio.CancelledError:
                    await asyncio.shield(
                        self._refence_points(
                            point_ids,
                            source_digest=source_digest,
                            generation=generation,
                        )
                    )
                    raise
                except QdrantCertificationError:
                    try:
                        await self._refence_points(
                            point_ids,
                            source_digest=source_digest,
                            generation=generation,
                        )
                    except Exception:
                        raise QdrantCertificationError(
                            "upsert_fence_unverified"
                        ) from None
                    raise
                except Exception:
                    try:
                        await self._refence_points(
                            point_ids,
                            source_digest=source_digest,
                            generation=generation,
                        )
                    except Exception:
                        raise QdrantCertificationError(
                            "upsert_fence_unverified"
                        ) from None
                    raise QdrantCertificationError("upsert_staging_failed") from None

                if not expected_lineage.servable:
                    continue
                try:
                    result = await _drained_thread_call(
                        self._client.set_payload,
                        collection_name=self._collection_name,
                        payload={"servable": True},
                        key="synor",
                        points=_lineage_points_filter(
                            point_ids,
                            source_digest,
                            generation,
                        ),
                        wait=True,
                        ordering=qdrant_models.WriteOrdering.STRONG,
                        timeout=_REQUEST_TIMEOUT_SECONDS,
                    )
                    results.append(_completed_result(result))
                    for point_id, (_, metadata) in zip(
                        point_ids,
                        entry_batch,
                        strict=True,
                    ):
                        payload = await self._retrieve_payload(point_id)
                        if (
                            payload is None
                            or payload.get("synor") != metadata.payload()
                        ):
                            raise QdrantCertificationError(
                                "upsert_activation_unverified"
                            )
                    await self._require_lineage_authorized(expected_lineage)
                except asyncio.CancelledError:
                    await asyncio.shield(
                        self._refence_points(
                            point_ids,
                            source_digest=source_digest,
                            generation=generation,
                        )
                    )
                    raise
                except Exception:
                    try:
                        await self._refence_points(
                            point_ids,
                            source_digest=source_digest,
                            generation=generation,
                        )
                    except Exception:
                        raise QdrantCertificationError(
                            "upsert_fence_unverified"
                        ) from None
                    raise QdrantCertificationError("upsert_activation_failed") from None
        return tuple(results)

    async def query_points(
        self,
        *,
        context: CertifiedQueryContext,
        query: Any = None,
        additional_filter: qdrant_models.Filter | None = None,
        **kwargs: Any,
    ) -> qdrant_models.QueryResponse:
        """Run a raw-vector query through the authorization/consistency guard."""

        controlled = {"query_filter", "consistency", "collection_name", "timeout"}
        if controlled.intersection(kwargs):
            raise ValueError("certified query controls filter and consistency")
        unsupported = kwargs.keys() - _CERTIFIED_QUERY_KWARGS
        if unsupported:
            raise ValueError(
                "certified query does not support compound or unknown arguments"
            )
        normalized_query = _normalize_certified_query(query)
        query_filter = certified_query_filter(
            context,
            additional_filter=additional_filter,
        )
        await self._require_certified()
        await self._require_query_context_current(context)
        try:
            response = await asyncio.to_thread(
                self._client.query_points,
                collection_name=self._collection_name,
                query=normalized_query,
                query_filter=query_filter,
                consistency=qdrant_models.ReadConsistencyType.ALL,
                timeout=_REQUEST_TIMEOUT_SECONDS,
                **kwargs,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise QdrantCertificationError("query_failed") from None
        # Recheck after the provider await so a concurrent revocation cannot
        # release results authorized only by a stale pre-query snapshot.
        await self._require_query_context_current(context)
        return response

    def verified_delete_sink(
        self,
        *,
        record: AsyncOutcomeRecorder,
        operation_evidence_recorder: OperationEvidenceRecorder | None = None,
        before_apply: ActionBoundaryHook | None = None,
        after_apply: ActionBoundaryHook | None = None,
        policy: VerificationRetryPolicy | None = None,
    ) -> VerifiedTargetActionSink[QdrantRevocationAction, None]:
        """Build a core-compatible strict sink for exact point revocations."""

        retry_policy = policy or VerificationRetryPolicy()
        invocation_evidence: contextvars.ContextVar[
            tuple[dict[str, QdrantOperationEvidence], ...]
        ] = contextvars.ContextVar(
            "qdrant_delete_evidence",
            default=(),
        )

        async def apply(
            context_provider: ContextProvider,
            actions: Sequence[QdrantRevocationAction],
            /,
        ) -> None:
            evidence_stack = invocation_evidence.get()
            if not evidence_stack:
                raise QdrantCertificationError("delete_invocation_missing")
            delete_evidence_by_action = evidence_stack[-1]
            await self._require_certified()
            for action in actions:
                expected_locator = self.point_locator_digest(action.point_id)
                if action.descriptor.target_locator_digest != expected_locator:
                    raise QdrantCertificationError("target_locator_mismatch")
                if (
                    action.query_context.source_digest
                    != action.descriptor.source_digest
                ):
                    raise QdrantCertificationError("source_digest_mismatch")
                if (
                    action.query_context.generation
                    > action.descriptor.source_generation
                ):
                    raise QdrantCertificationError("source_generation_mismatch")
                if before_apply is not None:
                    await before_apply(context_provider, action)

            for action_batch in iter_revocation_batches(
                actions,
                batch_size=self._batch_size,
            ):
                evidence = await self._suppress_then_delete(
                    action_batch,
                    retry_policy,
                )
                delete_evidence = {
                    item.action_id: item
                    for item in evidence
                    if item.operation_kind == "delete"
                }
                expected_action_ids = {
                    action.descriptor.action_id for action in action_batch
                }
                if set(delete_evidence) != expected_action_ids:
                    raise QdrantCertificationError("delete_evidence_incomplete")
                delete_evidence_by_action.update(delete_evidence)
                if operation_evidence_recorder is not None and evidence:
                    await operation_evidence_recorder(
                        context_provider,
                        tuple(evidence),
                    )
                if after_apply is not None:
                    for action in action_batch:
                        await after_apply(context_provider, action)

        async def verify(
            context_provider: ContextProvider,
            actions: Sequence[QdrantRevocationAction],
            applied: None,
            /,
        ) -> Sequence[TargetVerificationResult]:
            del context_provider, applied
            evidence_stack = invocation_evidence.get()
            delete_evidence_by_action = evidence_stack[-1] if evidence_stack else {}

            def failed_results(
                failed_actions: Sequence[QdrantRevocationAction],
            ) -> list[TargetVerificationResult]:
                return [
                    TargetVerificationResult(
                        status=VerificationOutcome.TRANSPORT_FAILURE,
                        action_id=action.descriptor.action_id,
                        operation_id=_operation_id_for(
                            delete_evidence_by_action.get(action.descriptor.action_id)
                        ),
                        detail_code="verifier_exception",
                    )
                    for action in failed_actions
                ]

            try:
                await self._require_certified()
            except Exception:
                return failed_results(actions)

            outcomes: list[TargetVerificationResult] = []
            for action_batch in iter_revocation_batches(
                actions,
                batch_size=self._batch_size,
            ):
                try:
                    present = await self._retrieve_ids(
                        [action.point_id for action in action_batch]
                    )
                except Exception:
                    outcomes.extend(failed_results(action_batch))
                    continue
                for action in action_batch:
                    action_id = action.descriptor.action_id
                    evidence = delete_evidence_by_action.get(action_id)
                    point_present = _canonical_point_id(action.point_id) in present
                    outcomes.append(
                        TargetVerificationResult(
                            status=(
                                VerificationOutcome.PRESENT
                                if point_present
                                else VerificationOutcome.ABSENT
                            ),
                            action_id=action_id,
                            operation_id=_operation_id_for(evidence),
                            detail_code=(
                                "read_replica_stale" if point_present else None
                            ),
                        )
                    )
            return outcomes

        def begin_invocation() -> Callable[[], None]:
            evidence: dict[str, QdrantOperationEvidence] = {}
            token = invocation_evidence.set((*invocation_evidence.get(), evidence))

            def cleanup() -> None:
                invocation_evidence.reset(token)

            return cleanup

        return VerifiedTargetActionSink[
            QdrantRevocationAction,
            None,
        ](
            apply=apply,
            verify=verify,
            record=record,
            policy=retry_policy,
            invocation_scope=begin_invocation,
        )

    async def narrow_acl(
        self,
        *,
        point_id: _PointId,
        new_lineage: GovernedPointLineage,
        previous_generation: int,
        denied_principal_digests: Sequence[str],
        policy: VerificationRetryPolicy | None = None,
    ) -> tuple[QdrantOperationEvidence, ...]:
        """Replace ACL metadata while keeping the point suppressed until verified."""

        await self._require_certified()
        validated_id = _validate_point_id(point_id)
        if not new_lineage.servable or new_lineage.retention_state != "active":
            raise ValueError("ACL narrowing requires an active servable lineage")
        await self._require_lineage_authorized(new_lineage)
        if (
            not isinstance(previous_generation, int)
            or isinstance(previous_generation, bool)
            or previous_generation < 1
            or previous_generation >= new_lineage.generation
        ):
            raise ValueError(
                "previous_generation must be positive and lower than the new generation"
            )
        denied = tuple(denied_principal_digests)
        if not denied:
            raise ValueError("denied_principal_digests must not be empty")
        if len(set(denied)) != len(denied):
            raise ValueError("denied_principal_digests must be unique")
        for principal_digest in denied:
            _require_digest("denied_principal_digest", principal_digest)
        if set(denied).intersection(new_lineage.principal_digests):
            raise ValueError("denied and allowed principals must not overlap")
        try:
            existing = await self._retrieve_point(validated_id, with_vectors=False)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise QdrantCertificationError("acl_read_failed") from None
        if existing is None:
            raise QdrantCertificationError("acl_point_absent")
        try:
            existing_metadata = _metadata_from_record(existing)
        except ValueError:
            raise QdrantCertificationError("acl_point_ungoverned") from None
        existing_lineage = existing_metadata.lineage
        if (
            existing_lineage.source_digest != new_lineage.source_digest
            or existing_lineage.generation
            not in (previous_generation, new_lineage.generation)
        ):
            raise QdrantCertificationError("acl_lineage_mismatch")
        new_metadata = _GovernedPointMetadata(
            lineage=new_lineage,
            chunk_digest=existing_metadata.chunk_digest,
            content_fingerprint=existing_metadata.content_fingerprint,
        )
        if existing_lineage.generation == previous_generation:
            removed_principals = set(existing_lineage.principal_digests) - set(
                new_lineage.principal_digests
            )
            if not removed_principals.issubset(denied):
                raise QdrantCertificationError("acl_denied_principals_incomplete")
        elif existing_metadata.payload() not in (
            new_metadata.payload(),
            new_metadata.payload(servable=False),
        ):
            raise QdrantCertificationError("acl_retry_conflict")
        retry_policy = policy or VerificationRetryPolicy()
        verification_deadline = time.monotonic() + retry_policy.timeout.total_seconds()
        action_id = hashlib.sha256(
            (
                f"acl:{self._collection_name}:{validated_id}:"
                f"{new_lineage.policy_revision}:{new_lineage.generation}"
            ).encode()
        ).hexdigest()

        contexts = [
            _query_context(new_lineage, principal)
            for principal in (*new_lineage.principal_digests, *denied)
        ]
        evidence: list[QdrantOperationEvidence] = []
        try:
            result = await asyncio.to_thread(
                self._client.set_payload,
                collection_name=self._collection_name,
                payload=new_metadata.payload(servable=False),
                key="synor",
                points=_lineage_transition_filter(
                    validated_id,
                    new_lineage.source_digest,
                    previous_generation,
                    new_lineage.generation,
                ),
                wait=True,
                ordering=qdrant_models.WriteOrdering.STRONG,
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            operation_id, status = _completed_result(result)
            evidence.append(
                QdrantOperationEvidence(
                    action_id,
                    "acl_update",
                    operation_id,
                    status,
                )
            )
            lineage_written = await self._wait_for_lineage(
                validated_id,
                new_metadata,
                servable=False,
                policy=retry_policy,
                deadline=verification_deadline,
            )
            filter_suppressed = await self._wait_for_access(
                validated_id,
                contexts,
                expected_visible=False,
                policy=retry_policy,
                deadline=verification_deadline,
            )
        except asyncio.CancelledError:
            await asyncio.shield(
                self._refence_acl_transition(
                    validated_id,
                    source_digest=new_lineage.source_digest,
                    previous_generation=previous_generation,
                    new_generation=new_lineage.generation,
                    policy=retry_policy,
                )
            )
            raise
        except Exception:
            try:
                await self._refence_acl_transition(
                    validated_id,
                    source_digest=new_lineage.source_digest,
                    previous_generation=previous_generation,
                    new_generation=new_lineage.generation,
                    policy=retry_policy,
                )
            except Exception:
                raise QdrantCertificationError("acl_fence_unverified") from None
            raise QdrantCertificationError("acl_suppression_failed") from None
        if not lineage_written or not filter_suppressed:
            try:
                await self._refence_acl_transition(
                    validated_id,
                    source_digest=new_lineage.source_digest,
                    previous_generation=previous_generation,
                    new_generation=new_lineage.generation,
                    policy=retry_policy,
                )
            except Exception:
                raise QdrantCertificationError("acl_fence_unverified") from None
            raise QdrantCertificationError("acl_suppression_unverified") from None

        allowed_contexts = [
            _query_context(new_lineage, principal)
            for principal in new_lineage.principal_digests
        ]
        denied_contexts = [
            _query_context(new_lineage, principal) for principal in denied
        ]
        try:
            result = await _drained_thread_call(
                self._client.set_payload,
                collection_name=self._collection_name,
                payload={"servable": True},
                key="synor",
                points=_lineage_filter(
                    validated_id,
                    new_lineage.source_digest,
                    new_lineage.generation,
                ),
                wait=True,
                ordering=qdrant_models.WriteOrdering.STRONG,
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            operation_id, status = _completed_result(result)
            evidence.append(
                QdrantOperationEvidence(
                    action_id,
                    "acl_update",
                    operation_id,
                    status,
                )
            )
            allowed = await self._wait_for_access(
                validated_id,
                allowed_contexts,
                expected_visible=True,
                policy=retry_policy,
                deadline=verification_deadline,
            )
            denied_absent = await self._wait_for_access(
                validated_id,
                denied_contexts,
                expected_visible=False,
                policy=retry_policy,
                deadline=verification_deadline,
            )
            if not allowed or not denied_absent:
                raise QdrantCertificationError("acl_query_verification_failed")
            await self._require_lineage_authorized(new_lineage)
        except asyncio.CancelledError:
            await asyncio.shield(
                self._refence_points(
                    [validated_id],
                    source_digest=new_lineage.source_digest,
                    generation=new_lineage.generation,
                    policy=retry_policy,
                )
            )
            raise
        except Exception:
            try:
                refence_result = await self._refence_points(
                    [validated_id],
                    source_digest=new_lineage.source_digest,
                    generation=new_lineage.generation,
                    policy=retry_policy,
                )
            except Exception:
                raise QdrantCertificationError("acl_fence_unverified") from None
            operation_id, status = refence_result
            evidence.append(
                QdrantOperationEvidence(
                    action_id,
                    "acl_update",
                    operation_id,
                    status,
                )
            )
            raise QdrantCertificationError("acl_verification_failed") from None
        return tuple(evidence)

    async def delete_collection_verified(
        self,
        *,
        policy: VerificationRetryPolicy | None = None,
    ) -> None:
        """Delete the collection and prove provider-confirmed absence."""

        await self._require_supported_versions()
        try:
            deleted = await _drained_thread_call(
                self._client.delete_collection,
                collection_name=self._collection_name,
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
        except (UnexpectedResponse, grpc.RpcError) as error:
            if not _is_not_found(error):
                raise
        else:
            if deleted is not True:
                raise QdrantCertificationError("collection_delete_unconfirmed")

        retry_policy = policy or VerificationRetryPolicy()
        deadline = time.monotonic() + retry_policy.timeout.total_seconds()
        for attempt in range(retry_policy.max_attempts):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                collection_exists = await asyncio.wait_for(
                    self._collection_exists_strict(),
                    timeout=remaining,
                )
            except TimeoutError:
                break
            if not collection_exists:
                return
            if attempt + 1 < retry_policy.max_attempts:
                await _backoff(retry_policy, attempt, deadline=deadline)
        raise QdrantCertificationError("collection_absence_unverified")

    async def _suppress_then_delete(
        self,
        actions: Sequence[QdrantRevocationAction],
        policy: VerificationRetryPolicy,
    ) -> list[QdrantOperationEvidence]:
        action_filter = qdrant_models.Filter(
            should=[_effect_filter(action) for action in actions]
        )
        result = await asyncio.to_thread(
            self._client.set_payload,
            collection_name=self._collection_name,
            payload={"servable": False},
            key="synor",
            points=action_filter,
            wait=True,
            ordering=qdrant_models.WriteOrdering.STRONG,
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        suppression_operation_id, suppression_status = _completed_result(result)
        evidence = [
            QdrantOperationEvidence(
                action.descriptor.action_id,
                "suppress",
                suppression_operation_id,
                suppression_status,
            )
            for action in actions
        ]

        deadline = time.monotonic() + policy.timeout.total_seconds()
        for action in actions:
            target_fenced = await self._wait_for_effect_suppression(
                action,
                policy=policy,
                deadline=deadline,
            )
            query_fenced = await self._wait_for_access(
                action.point_id,
                [action.query_context],
                expected_visible=False,
                policy=policy,
                deadline=deadline,
            )
            if not target_fenced or not query_fenced:
                raise QdrantCertificationError("serving_suppression_unverified")

        result = await asyncio.to_thread(
            self._client.delete,
            collection_name=self._collection_name,
            points_selector=action_filter,
            wait=True,
            ordering=qdrant_models.WriteOrdering.STRONG,
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        delete_operation_id, delete_status = _completed_result(result)
        evidence.extend(
            QdrantOperationEvidence(
                action.descriptor.action_id,
                "delete",
                delete_operation_id,
                delete_status,
            )
            for action in actions
        )
        return evidence

    async def _wait_for_effect_suppression(
        self,
        action: QdrantRevocationAction,
        *,
        policy: VerificationRetryPolicy,
        deadline: float | None = None,
    ) -> bool:
        expires_at = deadline or (time.monotonic() + policy.timeout.total_seconds())
        for attempt in range(policy.max_attempts):
            remaining = expires_at - time.monotonic()
            if remaining <= 0:
                return False
            try:
                payload = await asyncio.wait_for(
                    self._retrieve_payload(action.point_id),
                    timeout=remaining,
                )
            except TimeoutError:
                return False
            if payload is None:
                return True
            governed = payload.get("synor")
            if isinstance(governed, dict):
                source_digest = governed.get("source_digest")
                generation = governed.get("generation")
                servable = governed.get("servable")
                if (
                    source_digest == action.descriptor.source_digest
                    and isinstance(generation, int)
                    and not isinstance(generation, bool)
                    and generation <= action.descriptor.source_generation
                    and servable is False
                ):
                    return True
            if attempt + 1 < policy.max_attempts:
                await _backoff(policy, attempt, deadline=expires_at)
        return False

    async def _wait_for_access(
        self,
        point_id: _PointId,
        contexts: Sequence[CertifiedQueryContext],
        *,
        expected_visible: bool,
        policy: VerificationRetryPolicy,
        deadline: float | None = None,
    ) -> bool:
        if not contexts:
            return False
        expires_at = deadline or (time.monotonic() + policy.timeout.total_seconds())
        for attempt in range(policy.max_attempts):
            observed: list[bool] = []
            for context in contexts:
                remaining = expires_at - time.monotonic()
                if remaining <= 0:
                    return False
                try:
                    value = await asyncio.wait_for(
                        self._point_matches_filter(point_id, context),
                        timeout=remaining,
                    )
                except TimeoutError:
                    return False
                observed.append(value)
            if all(value is expected_visible for value in observed):
                return True
            if attempt + 1 < policy.max_attempts:
                await _backoff(policy, attempt, deadline=expires_at)
        return False

    async def _point_matches_filter(
        self,
        point_id: _PointId,
        context: CertifiedQueryContext,
    ) -> bool:
        response = await _bounded_thread_call(
            self._client.query_points,
            collection_name=self._collection_name,
            query=None,
            query_filter=certified_query_filter(context, point_ids=[point_id]),
            limit=1,
            with_payload=False,
            with_vectors=False,
            consistency=qdrant_models.ReadConsistencyType.ALL,
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        response_points = getattr(response, "points", None)
        observed = _strict_response_ids(
            response_points,
            requested_ids=[point_id],
        )
        return bool(observed)

    async def _wait_for_lineage(
        self,
        point_id: _PointId,
        metadata: _GovernedPointMetadata,
        *,
        servable: bool,
        policy: VerificationRetryPolicy,
        deadline: float | None = None,
    ) -> bool:
        expected = metadata.payload(servable=servable)
        expires_at = deadline or (time.monotonic() + policy.timeout.total_seconds())
        for attempt in range(policy.max_attempts):
            remaining = expires_at - time.monotonic()
            if remaining <= 0:
                return False
            try:
                payload = await asyncio.wait_for(
                    self._retrieve_payload(point_id),
                    timeout=remaining,
                )
            except TimeoutError:
                return False
            if payload is not None and payload.get("synor") == expected:
                return True
            if attempt + 1 < policy.max_attempts:
                await _backoff(policy, attempt, deadline=expires_at)
        return False

    async def _retrieve_point(
        self,
        point_id: _PointId,
        *,
        with_vectors: bool,
    ) -> object | None:
        records = await _bounded_thread_call(
            self._client.retrieve,
            collection_name=self._collection_name,
            ids=[point_id],
            with_payload=True,
            with_vectors=with_vectors,
            consistency=qdrant_models.ReadConsistencyType.ALL,
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        observed = _strict_response_ids(records, requested_ids=[point_id])
        if not observed:
            return None
        assert isinstance(records, list)
        record: object = records[0]
        return record

    async def _retrieve_payload(self, point_id: _PointId) -> dict[str, Any] | None:
        record = await self._retrieve_point(point_id, with_vectors=False)
        if record is None:
            return None
        payload = getattr(record, "payload", None)
        if payload is None:
            return {}
        if not isinstance(payload, Mapping):
            raise QdrantCertificationError("retrieve_payload_invalid")
        return dict(payload)

    async def _retrieve_ids(self, point_ids: Sequence[_PointId]) -> set[str]:
        records = await _bounded_thread_call(
            self._client.retrieve,
            collection_name=self._collection_name,
            ids=point_ids,
            with_payload=False,
            with_vectors=False,
            consistency=qdrant_models.ReadConsistencyType.ALL,
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        return set(_strict_response_ids(records, requested_ids=point_ids))

    async def _collection_exists_strict(self) -> bool:
        collection_exists = getattr(self._client, "collection_exists", None)
        try:
            if callable(collection_exists):
                result = await _bounded_thread_call(
                    collection_exists,
                    collection_name=self._collection_name,
                )
                if type(result) is not bool:
                    raise QdrantCertificationError("collection_presence_unconfirmed")
                return result
            await _bounded_thread_call(
                self._client.get_collection,
                collection_name=self._collection_name,
            )
        except (UnexpectedResponse, grpc.RpcError) as error:
            if _is_not_found(error):
                return False
            raise QdrantCertificationError("collection_presence_failed") from None
        except QdrantCertificationError:
            raise
        except Exception:
            raise QdrantCertificationError("collection_presence_failed") from None
        return True

    async def _require_lineage_authorized(
        self,
        lineage: GovernedPointLineage,
    ) -> None:
        verifier = self._lineage_authorizer
        if verifier is None:
            raise QdrantCertificationError("lineage_authorizer_missing")
        try:
            authorized = await verifier(lineage)
        except Exception:
            raise QdrantCertificationError(
                "lineage_authorization_unavailable"
            ) from None
        if authorized is not True:
            raise QdrantCertificationError("lineage_authorization_stale")

    async def _require_query_context_current(
        self,
        context: CertifiedQueryContext,
    ) -> None:
        if not isinstance(context, CertifiedQueryContext):
            raise TypeError("context must be a CertifiedQueryContext")
        verifier = self._query_context_verifier
        if verifier is None:
            raise QdrantCertificationError("query_context_verifier_missing")
        try:
            current = await verifier(context)
        except Exception:
            raise QdrantCertificationError(
                "query_context_verification_unavailable"
            ) from None
        if current is not True:
            raise QdrantCertificationError("query_context_stale")

    async def _refence_points(
        self,
        point_ids: Sequence[_PointId],
        *,
        source_digest: str,
        generation: int,
        policy: VerificationRetryPolicy | None = None,
    ) -> tuple[int | None, str]:
        result = await asyncio.to_thread(
            self._client.set_payload,
            collection_name=self._collection_name,
            payload={"servable": False},
            key="synor",
            points=_lineage_points_upto_filter(
                point_ids,
                source_digest,
                generation,
            ),
            wait=True,
            ordering=qdrant_models.WriteOrdering.STRONG,
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        completed = _completed_result(result)
        retry_policy = policy or VerificationRetryPolicy()
        verification_deadline = time.monotonic() + retry_policy.timeout.total_seconds()
        for point_id in point_ids:
            if not await self._wait_for_source_generation_fence(
                point_id,
                source_digest=source_digest,
                generation=generation,
                policy=retry_policy,
                deadline=verification_deadline,
            ):
                raise QdrantCertificationError("target_fence_unverified")
        return completed

    async def _refence_acl_transition(
        self,
        point_id: _PointId,
        *,
        source_digest: str,
        previous_generation: int,
        new_generation: int,
        policy: VerificationRetryPolicy,
    ) -> tuple[int | None, str]:
        result = await asyncio.to_thread(
            self._client.set_payload,
            collection_name=self._collection_name,
            payload={"servable": False},
            key="synor",
            points=_lineage_transition_filter(
                point_id,
                source_digest,
                previous_generation,
                new_generation,
            ),
            wait=True,
            ordering=qdrant_models.WriteOrdering.STRONG,
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
        completed = _completed_result(result)
        if not await self._wait_for_source_generation_fence(
            point_id,
            source_digest=source_digest,
            generation=new_generation,
            policy=policy,
        ):
            raise QdrantCertificationError("target_fence_unverified")
        return completed

    async def _wait_for_source_generation_fence(
        self,
        point_id: _PointId,
        *,
        source_digest: str,
        generation: int,
        policy: VerificationRetryPolicy,
        deadline: float | None = None,
    ) -> bool:
        expires_at = (
            deadline
            if deadline is not None
            else time.monotonic() + policy.timeout.total_seconds()
        )
        for attempt in range(policy.max_attempts):
            remaining = expires_at - time.monotonic()
            if remaining <= 0:
                return False
            try:
                payload = await asyncio.wait_for(
                    self._retrieve_payload(point_id),
                    timeout=remaining,
                )
            except TimeoutError:
                return False
            if payload is None:
                return True
            governed = payload.get("synor")
            if isinstance(governed, dict):
                observed_source = governed.get("source_digest")
                observed_generation = governed.get("generation")
                if (
                    observed_source == source_digest
                    and isinstance(observed_generation, int)
                    and not isinstance(observed_generation, bool)
                ):
                    if observed_generation > generation:
                        return True
                    if governed.get("servable") is False:
                        return True
            if attempt + 1 < policy.max_attempts:
                await _backoff(policy, attempt, deadline=expires_at)
        return False

    async def _require_supported_versions(self) -> tuple[str, str]:
        client_version = importlib.metadata.version("qdrant-client")
        server_info = await _bounded_metadata_call(self._client.info)
        server_version = getattr(server_info, "version", None)
        if not isinstance(server_version, str):
            raise QdrantCertificationError("server_version_missing")
        _validate_version_pair(client_version, server_version)
        return client_version, server_version

    async def _require_certified(self) -> QdrantCapabilityReport:
        """Fail closed unless the connected client/server contract is supported."""

        return await self.capability_report()


def _query_context(
    lineage: GovernedPointLineage,
    principal_digest: str,
) -> CertifiedQueryContext:
    return CertifiedQueryContext(
        source_digest=lineage.source_digest,
        tenant_digest=lineage.tenant_digest,
        policy_id=lineage.policy_id,
        policy_revision=lineage.policy_revision,
        group_graph_revision=lineage.group_graph_revision,
        generation=lineage.generation,
        principal_digest=principal_digest,
    )


def _effect_filter(action: QdrantRevocationAction) -> qdrant_models.Filter:
    """Match the revoked generation and older, without touching a newer point."""

    return _lineage_upto_filter(
        action.point_id,
        action.descriptor.source_digest,
        action.descriptor.source_generation,
    )


def _lineage_filter(
    point_id: _PointId,
    source_digest: str,
    generation: int,
) -> qdrant_models.Filter:
    return qdrant_models.Filter(
        must=[
            qdrant_models.HasIdCondition(has_id=[point_id]),
            _match("synor.source_digest", source_digest),
            _match("synor.generation", generation),
        ]
    )


def _lineage_upto_filter(
    point_id: _PointId,
    source_digest: str,
    generation: int,
) -> qdrant_models.Filter:
    return qdrant_models.Filter(
        must=[
            qdrant_models.HasIdCondition(has_id=[point_id]),
            _match("synor.source_digest", source_digest),
            qdrant_models.FieldCondition(
                key="synor.generation",
                range=qdrant_models.Range(lte=generation),
            ),
        ]
    )


def _lineage_transition_filter(
    point_id: _PointId,
    source_digest: str,
    previous_generation: int,
    new_generation: int,
) -> qdrant_models.Filter:
    return qdrant_models.Filter(
        must=[
            qdrant_models.HasIdCondition(has_id=[point_id]),
            _match("synor.source_digest", source_digest),
            qdrant_models.FieldCondition(
                key="synor.generation",
                match=qdrant_models.MatchAny(any=[previous_generation, new_generation]),
            ),
        ]
    )


def _lineage_points_filter(
    point_ids: Sequence[_PointId],
    source_digest: str,
    generation: int,
) -> qdrant_models.Filter:
    validated = [_validate_point_id(point_id) for point_id in point_ids]
    if not validated:
        raise ValueError("point_ids must not be empty")
    return qdrant_models.Filter(
        must=[
            qdrant_models.HasIdCondition(
                has_id=cast(
                    list[qdrant_models.ExtendedPointId],
                    validated,
                )
            ),
            _match("synor.source_digest", source_digest),
            _match("synor.generation", generation),
        ]
    )


def _lineage_points_upto_filter(
    point_ids: Sequence[_PointId],
    source_digest: str,
    generation: int,
) -> qdrant_models.Filter:
    validated = [_validate_point_id(point_id) for point_id in point_ids]
    if not validated:
        raise ValueError("point_ids must not be empty")
    return qdrant_models.Filter(
        must=[
            qdrant_models.HasIdCondition(
                has_id=cast(
                    list[qdrant_models.ExtendedPointId],
                    validated,
                )
            ),
            _match("synor.source_digest", source_digest),
            qdrant_models.FieldCondition(
                key="synor.generation",
                range=qdrant_models.Range(lte=generation),
            ),
        ]
    )


def _point_with_servable(
    point: qdrant_models.PointStruct,
    metadata: _GovernedPointMetadata,
    *,
    servable: bool,
) -> qdrant_models.PointStruct:
    payload = dict(point.payload or {})
    payload["synor"] = metadata.payload(servable=servable)
    return qdrant_models.PointStruct(
        id=point.id,
        vector=point.vector,
        payload=payload,
    )


def _metadata_from_point(
    point: qdrant_models.PointStruct,
) -> _GovernedPointMetadata:
    if not isinstance(point, qdrant_models.PointStruct):
        raise TypeError("points must contain Qdrant PointStruct values")
    return _metadata_from_values(point.id, point.vector, point.payload)


def _metadata_from_record(record: object) -> _GovernedPointMetadata:
    missing = object()
    point_id = getattr(record, "id", missing)
    payload = getattr(record, "payload", missing)
    if point_id is missing or payload is missing:
        raise ValueError("stored governed point has an invalid shape")
    return _metadata_from_values(
        point_id,
        None,
        payload,
        verify_content=False,
    )


def _metadata_from_values(
    point_id: object,
    vector: object,
    payload: object,
    *,
    verify_content: bool = True,
) -> _GovernedPointMetadata:
    if not isinstance(payload, dict):
        raise ValueError("every strict upsert must contain governed metadata")
    governed = payload.get("synor")
    if not isinstance(governed, dict) or governed.get("contract_version") != "1":
        raise ValueError("every strict upsert must contain governed metadata")
    try:
        principals = governed["principal_digests"]
        if not isinstance(principals, list):
            raise TypeError
        lineage = GovernedPointLineage(
            source_digest=cast(str, governed["source_digest"]),
            source_revision=cast(str, governed["source_revision"]),
            policy_id=cast(str, governed["policy_id"]),
            policy_revision=cast(str, governed["policy_revision"]),
            group_graph_revision=cast(str, governed["group_graph_revision"]),
            tenant_digest=cast(str, governed["tenant"]),
            owner_component_digest=cast(str, governed["owner_component"]),
            generation=cast(int, governed["generation"]),
            principal_digests=tuple(cast(list[str], principals)),
            retention_state=cast(
                Literal["active", "retained_isolated"],
                governed["retention_state"],
            ),
            servable=cast(bool, governed["servable"]),
        )
        metadata = _GovernedPointMetadata(
            lineage=lineage,
            chunk_digest=cast(str, governed["chunk_digest"]),
            content_fingerprint=cast(str, governed["content_fingerprint"]),
        )
    except (KeyError, TypeError, ValueError):
        raise ValueError(
            "every strict upsert must contain valid governed metadata"
        ) from None
    if governed != metadata.payload():
        raise ValueError("strict governed metadata contains unsupported fields")
    validated_id = _validate_point_id(cast(_PointId, point_id))
    expected_id = deterministic_point_id(lineage.source_digest, metadata.chunk_digest)
    if _canonical_point_id(validated_id) != expected_id:
        raise ValueError("strict point ID does not match its governed chunk lineage")
    if verify_content:
        user_payload = dict(payload)
        user_payload.pop("synor")
        try:
            observed_content_fingerprint = _point_content_fingerprint(
                vector,
                user_payload,
            )
        except Exception:
            raise ValueError("strict point content fingerprint is invalid") from None
        if observed_content_fingerprint != metadata.content_fingerprint:
            raise ValueError("strict point content fingerprint is invalid")
    return metadata


def _point_content_fingerprint(vector: Any, payload: object) -> str:
    if not isinstance(payload, Mapping):
        raise TypeError("point payload must be a mapping")
    normalized = qdrant_models.PointStruct(
        id=0,
        vector=vector,
        payload=dict(payload),
    )
    canonical = json.dumps(
        normalized.model_dump(mode="json", exclude={"id"}),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


def _governed_payload(record: object | None) -> object | None:
    if record is None:
        return None
    payload = getattr(record, "payload", None)
    if not isinstance(payload, Mapping):
        return None
    return payload.get("synor")


def _match(key: str, value: str | int | bool) -> qdrant_models.FieldCondition:
    return qdrant_models.FieldCondition(
        key=key,
        match=qdrant_models.MatchValue(value=value),
    )


def _completed_result(result: object) -> tuple[int | None, str]:
    status = getattr(result, "status", None)
    if status is not qdrant_models.UpdateStatus.COMPLETED:
        raise QdrantCertificationError("operation_not_completed")
    operation_id = getattr(result, "operation_id", None)
    if operation_id is not None and (
        not isinstance(operation_id, int)
        or isinstance(operation_id, bool)
        or operation_id < 0
    ):
        raise QdrantCertificationError("operation_id_invalid")
    return operation_id, status.value


def _operation_id_for(evidence: QdrantOperationEvidence | None) -> str | None:
    if evidence is None or evidence.operation_id is None:
        return None
    return str(evidence.operation_id)


def _canonical_point_id(point_id: object) -> str:
    if isinstance(point_id, uuid.UUID):
        return str(point_id)
    if isinstance(point_id, int) and not isinstance(point_id, bool):
        return str(point_id)
    if isinstance(point_id, str):
        try:
            return str(uuid.UUID(point_id))
        except ValueError:
            return point_id
    return str(point_id)


def _is_not_found(error: BaseException) -> bool:
    if isinstance(error, UnexpectedResponse):
        return bool(error.status_code == 404)
    if isinstance(error, grpc.RpcError):
        code = getattr(error, "code", None)
        return callable(code) and code() == grpc.StatusCode.NOT_FOUND
    return False


async def _backoff(
    policy: VerificationRetryPolicy,
    attempt: int,
    *,
    deadline: float | None = None,
) -> None:
    delay = min(
        policy.initial_backoff * (policy.backoff_multiplier**attempt),
        policy.max_backoff,
    )
    if delay and policy.jitter:
        delay *= random.uniform(1.0 - policy.jitter, 1.0 + policy.jitter)
    if deadline is not None:
        delay = min(delay, max(0.0, deadline - time.monotonic()))
    if delay:
        await asyncio.sleep(delay)


async def _bounded_metadata_call(
    function: Callable[_P, _T],
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _T:
    """Bound and redact provider metadata reads."""

    try:
        return await _bounded_thread_call(function, *args, **kwargs)
    except asyncio.CancelledError:
        raise
    except QdrantCertificationError:
        raise
    except Exception:
        raise QdrantCertificationError("provider_metadata_failed") from None


async def _bounded_thread_call(
    function: Callable[_P, _T],
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _T:
    """Bound caller latency while preserving typed provider errors."""

    try:
        return await asyncio.wait_for(
            asyncio.to_thread(function, *args, **kwargs),
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        raise QdrantCertificationError("provider_timeout") from None


def _strict_response_ids(
    records: object,
    *,
    requested_ids: Sequence[_PointId],
) -> tuple[str, ...]:
    """Validate exact-ID provider responses before treating omissions as absent."""

    if not isinstance(records, list):
        raise QdrantCertificationError("readback_shape_invalid")
    requested = {
        _canonical_point_id(_validate_point_id(point_id)) for point_id in requested_ids
    }
    observed: list[str] = []
    seen: set[str] = set()
    for record in records:
        missing = object()
        raw_id = getattr(record, "id", missing)
        if raw_id is missing:
            raise QdrantCertificationError("readback_shape_invalid")
        try:
            point_id = _canonical_point_id(_validate_point_id(cast(_PointId, raw_id)))
        except (TypeError, ValueError):
            raise QdrantCertificationError("readback_id_invalid") from None
        if point_id not in requested:
            raise QdrantCertificationError("readback_id_unexpected")
        if point_id in seen:
            raise QdrantCertificationError("readback_id_duplicate")
        seen.add(point_id)
        observed.append(point_id)
    return tuple(observed)


async def _drained_thread_call(
    function: Callable[_P, _T],
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _T:
    """Finish a non-repeatable threaded call before propagating cancellation.

    Cancelling ``asyncio.to_thread`` does not stop its worker. Draining the
    worker first prevents a late mutation from overtaking compensation or
    deleting a newly recreated collection after cancellation was observed.
    """

    call = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    cancellation_requested = False
    while True:
        try:
            result = await asyncio.shield(call)
        except asyncio.CancelledError:
            cancellation_requested = True
            continue
        except Exception:
            if cancellation_requested:
                raise asyncio.CancelledError from None
            raise
        if cancellation_requested:
            raise asyncio.CancelledError
        return result


def _version_tuple(value: str, field: str) -> tuple[int, int, int]:
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", value)
    if match is None:
        raise QdrantCertificationError(f"{field}_invalid")
    return cast(tuple[int, int, int], tuple(int(part) for part in match.groups()))


def _validate_version_pair(client_version: str, server_version: str) -> None:
    client = _version_tuple(client_version, "client_version")
    server = _version_tuple(server_version, "server_version")
    if not (_CERTIFIED_CLIENT_MIN <= client < _CERTIFIED_CLIENT_MAX):
        raise QdrantCertificationError("client_version_unsupported")
    if not (_CERTIFIED_SERVER_MIN <= server < _CERTIFIED_SERVER_MAX):
        raise QdrantCertificationError("server_version_unsupported")
    if client[0] != server[0] or abs(client[1] - server[1]) > 1:
        raise QdrantCertificationError("client_server_version_mismatch")


def _enum_value(value: object) -> str:
    raw = getattr(value, "value", value)
    if not isinstance(raw, str):
        raise QdrantCertificationError("collection_status_invalid")
    return raw.lower()


def _positive_config_value(
    value: object,
    *,
    field: str,
) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise QdrantCertificationError(f"{field}_invalid")
    return value


def _cluster_topology_is_healthy(
    cluster_info: object,
    *,
    replication_factor: int,
) -> bool:
    local_shards = getattr(cluster_info, "local_shards", None)
    remote_shards = getattr(cluster_info, "remote_shards", None)
    transfers = getattr(cluster_info, "shard_transfers", None)
    resharding = getattr(cluster_info, "resharding_operations", None)
    shard_count = getattr(cluster_info, "shard_count", None)
    if (
        not isinstance(local_shards, list)
        or not isinstance(remote_shards, list)
        or not isinstance(transfers, list)
        or transfers
        or (resharding is not None and resharding != [])
        or not isinstance(shard_count, int)
        or isinstance(shard_count, bool)
        or shard_count < 1
    ):
        return False
    replicas: dict[tuple[int, object], int] = {}
    for shard in (*local_shards, *remote_shards):
        state = getattr(shard, "state", None)
        shard_id = getattr(shard, "shard_id", None)
        shard_key = getattr(shard, "shard_key", None)
        if (
            state is not qdrant_models.ReplicaState.ACTIVE
            or not isinstance(shard_id, int)
            or isinstance(shard_id, bool)
            or (
                shard_key is not None
                and (
                    not isinstance(shard_key, (int, str)) or isinstance(shard_key, bool)
                )
            )
        ):
            return False
        key = (shard_id, shard_key)
        replicas[key] = replicas.get(key, 0) + 1
    return len(replicas) == shard_count and all(
        count == replication_factor for count in replicas.values()
    )


def _require_digest(name: str, value: str) -> None:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def _require_token(name: str, value: str) -> None:
    if not isinstance(value, str) or not _SAFE_TOKEN.fullmatch(value):
        raise ValueError(f"{name} must be an opaque safe token")


__all__ = [
    "CertifiedQdrantTarget",
    "CertifiedQueryContext",
    "GovernedPointLineage",
    "LineageAuthorizationVerifier",
    "QDRANT_REVOCATION_CAPABILITIES",
    "QdrantCapabilityReport",
    "QdrantCertificationError",
    "QdrantOperationEvidence",
    "QdrantRevocationAction",
    "QueryContextVerifier",
    "SuppressionBackedQdrantVerifier",
    "certified_query_filter",
    "deterministic_point_id",
    "governed_point",
    "iter_revocation_batches",
]
