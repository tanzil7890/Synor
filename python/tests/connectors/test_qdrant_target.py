"""Tests for Qdrant target connector.

Helper-level tests run without a Qdrant service.

Live tests are gated on the ``QDRANT_URL`` env var; they are skipped when it
isn't set.
"""

from __future__ import annotations

import os
import uuid
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
    import grpc  # type: ignore[import-untyped]
    from httpx import Headers
    import numpy as np
    from qdrant_client.http.exceptions import (
        ResponseHandlingException,
        UnexpectedResponse,
    )

    import synor as syn
    from synor.connectors import qdrant
    import msgspec

    from synor._internal import serde
    from synor.connectors.qdrant._target import (
        _CollectionAction,
        _CollectionDeleteNotConfirmedError,
        _CollectionHandler,
        _CollectionKey,
        _CollectionTrackingRecordCore,
        _PointAction,
        _PointHandler,
        _ResolvedQdrantNamedVectorsDef,
        _ResolvedQdrantSparseVectorDef,
        _ResolvedQdrantVectorDef,
        _sparse_vector_params_from_def,
        _distance_from_spec,
        _multivector_comparator,
        _validate_point_id,
        _vector_params_from_def,
    )
    from synor.resources.schema import MultiVectorSchema, VectorSchema
    from tests import common

requires_qdrant_url = pytest.mark.skipif(
    not os.environ.get("QDRANT_URL"), reason="QDRANT_URL is not set"
)

# A fixed valid point ID for tests (Qdrant only accepts u64 ints and UUIDs).
_POINT_UUID = "550e8400-e29b-41d4-a716-446655440000"


# =============================================================================
# Unit tests — _distance_from_spec (no service needed)
# =============================================================================


@requires_qdrant
class TestDistanceFromSpec:
    def test_cosine(self) -> None:
        assert _distance_from_spec("cosine") == qdrant_models.Distance.COSINE

    def test_dot(self) -> None:
        assert _distance_from_spec("dot") == qdrant_models.Distance.DOT

    def test_dotproduct_alias(self) -> None:
        assert _distance_from_spec("dotproduct") == qdrant_models.Distance.DOT

    def test_euclid(self) -> None:
        assert _distance_from_spec("euclid") == qdrant_models.Distance.EUCLID

    def test_euclidean_alias(self) -> None:
        assert _distance_from_spec("euclidean") == qdrant_models.Distance.EUCLID

    def test_l2_alias(self) -> None:
        assert _distance_from_spec("l2") == qdrant_models.Distance.EUCLID

    def test_case_insensitive(self) -> None:
        assert _distance_from_spec("COSINE") == qdrant_models.Distance.COSINE
        assert _distance_from_spec("DOT") == qdrant_models.Distance.DOT
        assert _distance_from_spec("EUCLID") == qdrant_models.Distance.EUCLID

    def test_unsupported_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported Qdrant distance metric"):
            _distance_from_spec("manhattan")


# =============================================================================
# Unit tests — _multivector_comparator (no service needed)
# =============================================================================


@requires_qdrant
class TestMultivectorComparator:
    def test_max_sim(self) -> None:
        result = _multivector_comparator("max_sim")
        assert result == qdrant_models.MultiVectorComparator.MAX_SIM

    def test_case_insensitive(self) -> None:
        result = _multivector_comparator("MAX_SIM")
        assert result == qdrant_models.MultiVectorComparator.MAX_SIM

    def test_unsupported_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported multivector comparator"):
            _multivector_comparator("min_sim")


# =============================================================================
# Unit tests — _vector_params_from_def (no service needed)
# =============================================================================


@requires_qdrant
class TestVectorParamsFromDef:
    def test_vector_schema_cosine(self) -> None:
        vector_def = _ResolvedQdrantVectorDef(
            schema=VectorSchema(dtype=np.dtype(np.float32), size=128),
            distance="cosine",
            multivector_comparator="max_sim",
        )
        params = _vector_params_from_def(vector_def)
        assert params.size == 128
        assert params.distance == qdrant_models.Distance.COSINE
        assert params.multivector_config is None

    def test_vector_schema_dot(self) -> None:
        vector_def = _ResolvedQdrantVectorDef(
            schema=VectorSchema(dtype=np.dtype(np.float32), size=64),
            distance="dot",
            multivector_comparator="max_sim",
        )
        params = _vector_params_from_def(vector_def)
        assert params.size == 64
        assert params.distance == qdrant_models.Distance.DOT
        assert params.multivector_config is None

    def test_vector_schema_euclid(self) -> None:
        vector_def = _ResolvedQdrantVectorDef(
            schema=VectorSchema(dtype=np.dtype(np.float32), size=32),
            distance="euclid",
            multivector_comparator="max_sim",
        )
        params = _vector_params_from_def(vector_def)
        assert params.size == 32
        assert params.distance == qdrant_models.Distance.EUCLID
        assert params.multivector_config is None

    def test_multivector_schema(self) -> None:
        inner = VectorSchema(dtype=np.dtype(np.float32), size=256)
        multi_schema = MultiVectorSchema(vector_schema=inner)
        vector_def = _ResolvedQdrantVectorDef(
            schema=multi_schema,
            distance="cosine",
            multivector_comparator="max_sim",
        )
        params = _vector_params_from_def(vector_def)
        assert params.size == 256
        assert params.distance == qdrant_models.Distance.COSINE
        assert params.multivector_config is not None
        assert (
            params.multivector_config.comparator
            == qdrant_models.MultiVectorComparator.MAX_SIM
        )


# =============================================================================
# Unit tests — sparse vectors
# =============================================================================


@requires_qdrant
class TestSparseVectorSupport:
    @pytest.mark.asyncio
    async def test_collection_schema_create_resolves_sparse_vector_params(self) -> None:
        schema = await qdrant.CollectionSchema.create(
            vectors={
                "dense": qdrant.QdrantVectorDef(
                    schema=VectorSchema(dtype=np.dtype(np.float32), size=4)
                ),
                "sparse": qdrant.QdrantSparseVectorDef(modifier="idf"),
            },
        )

        assert isinstance(schema.vectors, _ResolvedQdrantNamedVectorsDef)
        sparse_def = schema.vectors.vectors["sparse"]
        assert isinstance(sparse_def, _ResolvedQdrantSparseVectorDef)
        params = _sparse_vector_params_from_def(sparse_def)
        assert isinstance(params, qdrant_models.SparseVectorParams)
        assert params.modifier == qdrant_models.Modifier.IDF

    @pytest.mark.asyncio
    async def test_sparse_only_schema_and_bare_sparse_rejected(self) -> None:
        schema = await qdrant.CollectionSchema.create(
            vectors={"sparse": qdrant.QdrantSparseVectorDef()},
        )
        assert isinstance(schema.vectors, _ResolvedQdrantNamedVectorsDef)

        with pytest.raises(ValueError, match="always named"):
            await qdrant.CollectionSchema.create(
                vectors=qdrant.QdrantSparseVectorDef(),  # type: ignore[arg-type]
            )
        with pytest.raises(ValueError, match="at least one vector"):
            await qdrant.CollectionSchema.create()

    @pytest.mark.asyncio
    async def test_create_collection_splits_dense_and_sparse_configs(self) -> None:
        class FakeQdrantClient:
            def __init__(self) -> None:
                self.create_kwargs: dict[str, object] | None = None

            def create_collection(self, **kwargs: object) -> bool:
                self.create_kwargs = kwargs
                return True

        schema = await qdrant.CollectionSchema.create(
            vectors={
                "dense": qdrant.QdrantVectorDef(
                    schema=VectorSchema(dtype=np.dtype(np.float32), size=4)
                ),
                "sparse": qdrant.QdrantSparseVectorDef(modifier="idf"),
            },
        )
        client = FakeQdrantClient()

        await _CollectionHandler()._create_collection(
            client,  # type: ignore[arg-type]
            "test_sparse_config",
            schema,
            if_not_exists=False,
        )

        assert client.create_kwargs is not None
        dense_config = client.create_kwargs["vectors_config"]
        assert isinstance(dense_config, dict)
        assert set(dense_config) == {"dense"}
        sparse_config = client.create_kwargs["sparse_vectors_config"]
        assert isinstance(sparse_config, dict)
        assert set(sparse_config) == {"sparse"}
        assert isinstance(sparse_config["sparse"], qdrant_models.SparseVectorParams)
        assert sparse_config["sparse"].modifier == qdrant_models.Modifier.IDF

    @pytest.mark.asyncio
    async def test_create_collection_sparse_only_passes_no_dense_config(self) -> None:
        class FakeQdrantClient:
            def __init__(self) -> None:
                self.create_kwargs: dict[str, object] | None = None

            def create_collection(self, **kwargs: object) -> bool:
                self.create_kwargs = kwargs
                return True

        schema = await qdrant.CollectionSchema.create(
            vectors={"sparse": qdrant.QdrantSparseVectorDef()},
        )
        client = FakeQdrantClient()
        await _CollectionHandler()._create_collection(
            client,  # type: ignore[arg-type]
            "test_sparse_only",
            schema,
            if_not_exists=False,
        )
        assert client.create_kwargs is not None
        assert client.create_kwargs["vectors_config"] is None
        sparse_config = client.create_kwargs["sparse_vectors_config"]
        assert isinstance(sparse_config, dict)
        assert set(sparse_config) == {"sparse"}

    def test_point_fingerprint_changes_when_sparse_vector_changes(self) -> None:
        handler = _PointHandler(
            client=cast(QdrantClient, object()),
            collection_name="test_sparse_fingerprint",
        )
        point_v1 = qdrant.PointStruct(
            id=_POINT_UUID,
            vector={
                "dense": [0.1, 0.2, 0.3, 0.4],
                "sparse": qdrant_models.SparseVector(indices=[1, 7], values=[0.5, 0.9]),
            },
            payload={"text": "hello"},
        )
        point_v2 = qdrant.PointStruct(
            id=_POINT_UUID,
            vector={
                "dense": [0.1, 0.2, 0.3, 0.4],
                "sparse": qdrant_models.SparseVector(indices=[1, 7], values=[0.5, 1.1]),
            },
            payload={"text": "hello"},
        )

        out_v1 = handler.reconcile(_POINT_UUID, point_v1, [], True)
        out_v2 = handler.reconcile(_POINT_UUID, point_v2, [], True)

        assert out_v1 is not None
        assert out_v2 is not None
        assert out_v1.tracking_record != out_v2.tracking_record, (
            "sparse vector indices/values must participate in point change detection"
        )


# =============================================================================
# Unit tests — point ID validation (mirrors Qdrant's server-side rules)
# =============================================================================


@requires_qdrant
class TestPointIdValidation:
    """Matrix verified against a live Qdrant 1.18 server over REST and gRPC."""

    def test_valid_ids_pass_through(self) -> None:
        assert _validate_point_id(0) == 0
        assert _validate_point_id(2**64 - 1) == 2**64 - 1
        assert _validate_point_id(_POINT_UUID) == _POINT_UUID
        hex_form = uuid.UUID(_POINT_UUID).hex
        assert _validate_point_id(hex_form) == hex_form
        urn_form = f"urn:uuid:{_POINT_UUID}"
        assert _validate_point_id(urn_form) == urn_form

    def test_uuid_instance_converted_to_string(self) -> None:
        assert _validate_point_id(uuid.UUID(_POINT_UUID)) == _POINT_UUID

    def test_arbitrary_string_rejected(self) -> None:
        with pytest.raises(ValueError, match="strings must be UUIDs"):
            _validate_point_id("chunk-1")

    def test_out_of_range_ints_rejected(self) -> None:
        with pytest.raises(ValueError, match="unsigned 64-bit range"):
            _validate_point_id(-1)
        with pytest.raises(ValueError, match="unsigned 64-bit range"):
            _validate_point_id(2**64)

    def test_other_types_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid Qdrant point ID of type"):
            _validate_point_id(True)
        with pytest.raises(ValueError, match="Invalid Qdrant point ID of type"):
            _validate_point_id(False)
        with pytest.raises(ValueError, match="Invalid Qdrant point ID of type"):
            _validate_point_id(1.5)


# =============================================================================
# Unit tests — tracking record upgrade compatibility
# =============================================================================


@requires_qdrant
class TestTrackingRecordUpgradeCompat:
    def test_pre_sparse_tracking_record_compat(self) -> None:
        """Records written before sparse-vector support must decode equal to a
        new dense-only record, and a new dense-only record must encode to the
        same bytes as before — otherwise existing collections would be
        destructively replaced (and their data lost) on upgrade."""

        # Pre-sparse on-disk shape: inner dict values were the bare dense
        # struct, not the dense|sparse tagged union. The explicit tag pins
        # the historical class name so the encoding matches old records.
        class PreSparseNamed(
            msgspec.Struct, frozen=True, tag="_ResolvedQdrantNamedVectorsDef"
        ):
            vectors: dict[str, _ResolvedQdrantVectorDef]

        class PreSparseCore(msgspec.Struct, frozen=True, array_like=True):
            vectors: _ResolvedQdrantVectorDef | PreSparseNamed

        dense_def = _ResolvedQdrantVectorDef(
            schema=VectorSchema(dtype=np.dtype(np.float32), size=4),
            distance="cosine",
            multivector_comparator="max_sim",
        )
        old_bytes = serde._msgspec_encoder.encode(
            PreSparseCore(vectors=PreSparseNamed(vectors={"dense": dense_def}))
        )

        new_record = _CollectionTrackingRecordCore(
            vectors=_ResolvedQdrantNamedVectorsDef(vectors={"dense": dense_def})
        )
        decoded = msgspec.msgpack.Decoder(
            type=_CollectionTrackingRecordCore,
            ext_hook=serde._ext_hook,
            dec_hook=serde._dec_hook,
        ).decode(old_bytes)
        assert decoded == new_record, "old records must decode equal to new"

        new_bytes = serde._msgspec_encoder.encode(new_record)
        assert new_bytes == old_bytes, (
            "dense-only records must encode byte-identically to pre-sparse "
            "records; a byte change would flip the statediff and destroy "
            "existing collections on upgrade"
        )


# =============================================================================
# Unit tests — collection deletion failures must remain retryable
# =============================================================================


@requires_qdrant
class TestCollectionDeleteFailureHandling:
    class _DeleteCollectionClient:
        def __init__(self, error: BaseException) -> None:
            self._error = error
            self.deleted_collections: list[str] = []

        def delete_collection(self, *, collection_name: str) -> bool:
            self.deleted_collections.append(collection_name)
            raise self._error

    class _RpcError(grpc.RpcError if HAS_QDRANT else Exception):  # type: ignore[misc]
        def __init__(self, status_code: grpc.StatusCode) -> None:
            super().__init__()
            self._status_code = status_code

        def code(self) -> grpc.StatusCode:
            return self._status_code

    class _DeleteCollectionResultClient:
        def __init__(self, result: object) -> None:
            self._result = result
            self.deleted_collections: list[str] = []

        def delete_collection(self, *, collection_name: str) -> bool:
            self.deleted_collections.append(collection_name)
            return cast(bool, self._result)

    class _ContextProvider:
        def __init__(
            self, client: "TestCollectionDeleteFailureHandling._DeleteCollectionClient"
        ) -> None:
            self._client = client

        def get(self, *args: object) -> QdrantClient:
            return cast(QdrantClient, self._client)

    @staticmethod
    def _unexpected_response(status_code: int) -> UnexpectedResponse:
        return UnexpectedResponse(
            status_code=status_code,
            reason_phrase="Not Found" if status_code == 404 else "Request Failed",
            content=b"",
            headers=Headers(),
        )

    @pytest.mark.asyncio
    async def test_http_not_found_is_idempotent_success(self) -> None:
        client = self._DeleteCollectionClient(self._unexpected_response(404))

        await _CollectionHandler()._delete_collection(
            cast(QdrantClient, client),
            "already_deleted",
        )

        assert client.deleted_collections == ["already_deleted"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code", [401, 403, 500, 503])
    async def test_http_auth_and_server_failures_propagate(
        self, status_code: int
    ) -> None:
        error = self._unexpected_response(status_code)
        client = self._DeleteCollectionClient(error)

        with pytest.raises(UnexpectedResponse) as exc_info:
            await _CollectionHandler()._delete_collection(
                cast(QdrantClient, client),
                "must_remain_retryable",
            )

        assert exc_info.value is error

    @pytest.mark.asyncio
    async def test_http_transport_failure_propagates(self) -> None:
        error = ResponseHandlingException(ConnectionError("network unavailable"))
        client = self._DeleteCollectionClient(error)

        with pytest.raises(ResponseHandlingException) as exc_info:
            await _CollectionHandler()._delete_collection(
                cast(QdrantClient, client),
                "must_remain_retryable",
            )

        assert exc_info.value is error

    @pytest.mark.asyncio
    async def test_grpc_not_found_is_idempotent_success(self) -> None:
        client = self._DeleteCollectionClient(self._RpcError(grpc.StatusCode.NOT_FOUND))

        await _CollectionHandler()._delete_collection(
            cast(QdrantClient, client),
            "already_deleted",
        )

        assert client.deleted_collections == ["already_deleted"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status_code",
        (
            [
                grpc.StatusCode.PERMISSION_DENIED,
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.INTERNAL,
            ]
            if HAS_QDRANT
            else []
        ),
    )
    async def test_grpc_auth_network_and_server_failures_propagate(
        self, status_code: grpc.StatusCode
    ) -> None:
        error = self._RpcError(status_code)
        client = self._DeleteCollectionClient(error)

        with pytest.raises(self._RpcError) as exc_info:
            await _CollectionHandler()._delete_collection(
                cast(QdrantClient, client),
                "must_remain_retryable",
            )

        assert exc_info.value is error

    @pytest.mark.asyncio
    async def test_unknown_client_failure_propagates(self) -> None:
        error = RuntimeError("unexpected client failure")
        client = self._DeleteCollectionClient(error)

        with pytest.raises(RuntimeError) as exc_info:
            await _CollectionHandler()._delete_collection(
                cast(QdrantClient, client),
                "must_remain_retryable",
            )

        assert exc_info.value is error

    @pytest.mark.asyncio
    async def test_literal_true_confirms_collection_deletion(self) -> None:
        client = self._DeleteCollectionResultClient(True)

        await _CollectionHandler()._delete_collection(
            cast(QdrantClient, client),
            "deleted",
        )

        assert client.deleted_collections == ["deleted"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("result", [False, None, 1])
    async def test_normal_return_without_literal_confirmation_is_retryable(
        self,
        result: object,
    ) -> None:
        client = self._DeleteCollectionResultClient(result)

        with pytest.raises(
            _CollectionDeleteNotConfirmedError,
            match="deletion was not confirmed",
        ):
            await _CollectionHandler()._delete_collection(
                cast(QdrantClient, client),
                "must_remain_retryable",
            )

        assert client.deleted_collections == ["must_remain_retryable"]

    @pytest.mark.asyncio
    async def test_collection_action_sink_propagates_delete_failure(self) -> None:
        error = self._unexpected_response(503)
        client = self._DeleteCollectionClient(error)
        action = _CollectionAction(
            key=_CollectionKey(
                db_key="test_qdrant",
                collection_name="must_remain_retryable",
            ),
            spec=syn.ABSENT,
            main_action="delete",
        )

        with pytest.raises(UnexpectedResponse) as exc_info:
            await _CollectionHandler()._apply_actions(
                cast(syn.ContextProvider, self._ContextProvider(client)),
                [action],
            )

        assert exc_info.value is error


# =============================================================================
# Compatibility regression — mixed point action batches remain available
# =============================================================================


@requires_qdrant
@pytest.mark.asyncio
async def test_compatibility_point_handler_preserves_mixed_upsert_delete_batch() -> (
    None
):
    class _Client:
        def __init__(self) -> None:
            self.upserted: list[qdrant_models.PointStruct] = []
            self.deleted: list[str | int] = []

        def upsert(self, **kwargs: object) -> None:
            self.upserted.extend(
                cast(list[qdrant_models.PointStruct], kwargs["points"])
            )

        def delete(self, **kwargs: object) -> None:
            selector = cast(qdrant_models.PointIdsList, kwargs["points_selector"])
            self.deleted.extend(cast(list[str | int], selector.points))

    client = _Client()
    upserted = qdrant_models.PointStruct(
        id=_POINT_UUID,
        vector=[0.1, 0.2],
        payload={"kind": "compatibility"},
    )
    deleted = "2f1f67a3-2dbf-4ee8-9af0-676b38e2989a"

    await _PointHandler(
        cast(QdrantClient, client),
        "compatibility",
    )._apply_actions(
        cast(syn.ContextProvider, object()),
        [
            _PointAction(point_id=_POINT_UUID, point=upserted),
            _PointAction(point_id=deleted, point=None),
        ],
    )

    assert client.upserted == [upserted]
    assert client.deleted == [deleted]


# =============================================================================
# Live test — Qdrant service required
# =============================================================================


@requires_qdrant
@requires_qdrant_url
def test_live_dense_sparse_vectors_and_hybrid_query() -> None:
    qdrant_url = os.environ["QDRANT_URL"]
    client = qdrant.create_client(qdrant_url, prefer_grpc=True)
    collection_name = f"synor_sparse_{uuid.uuid4().hex}"
    db_key = syn.ContextKey[QdrantClient](f"test_qdrant_sparse_{uuid.uuid4().hex}")
    env = common.create_test_env(__file__, suffix=collection_name)
    env.context_provider.provide(db_key, client)

    @syn.task
    async def app_main() -> None:
        target = await qdrant.mount_collection_target(
            db_key,
            collection_name,
            await qdrant.CollectionSchema.create(
                vectors={
                    "dense": qdrant.QdrantVectorDef(
                        schema=VectorSchema(dtype=np.dtype(np.float32), size=4)
                    ),
                    "sparse": qdrant.QdrantSparseVectorDef(modifier="idf"),
                },
            ),
        )
        target.declare_point(
            qdrant.PointStruct(
                id=1,
                vector={
                    "dense": [0.1, 0.2, 0.3, 0.4],
                    "sparse": qdrant_models.SparseVector(
                        indices=[1, 7], values=[0.5, 0.9]
                    ),
                },
                payload={"text": "hybrid sparse dense"},
            )
        )

    app = syn.App(
        syn.AppConfig(name="test_qdrant_sparse_hybrid", environment=env),
        app_main,
    )

    try:
        app.update_blocking()

        dense_result = client.query_points(
            collection_name=collection_name,
            query=[0.1, 0.2, 0.3, 0.4],
            using="dense",
            limit=1,
            with_payload=True,
        )
        assert [p.id for p in dense_result.points] == [1]

        sparse_query = qdrant_models.SparseVector(indices=[1, 7], values=[0.5, 0.9])
        sparse_result = client.query_points(
            collection_name=collection_name,
            query=sparse_query,
            using="sparse",
            limit=1,
            with_payload=True,
        )
        assert [p.id for p in sparse_result.points] == [1]

        hybrid_result = client.query_points(
            collection_name=collection_name,
            prefetch=[
                qdrant_models.Prefetch(
                    query=[0.1, 0.2, 0.3, 0.4], using="dense", limit=10
                ),
                qdrant_models.Prefetch(query=sparse_query, using="sparse", limit=10),
            ],
            query=qdrant_models.FusionQuery(fusion=qdrant_models.Fusion.RRF),
            limit=1,
            with_payload=True,
        )
        assert [p.id for p in hybrid_result.points] == [1]
    finally:
        try:
            client.delete_collection(collection_name=collection_name)
        except Exception:
            pass
