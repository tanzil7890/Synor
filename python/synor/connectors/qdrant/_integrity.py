"""Read-only governed-point inspection for Qdrant integrity scans."""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

try:
    import grpc  # type: ignore[import-untyped]
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qdrant_models
    from qdrant_client.http.exceptions import UnexpectedResponse
except ImportError as error:
    raise ImportError(
        "qdrant-client is required to use the Qdrant integrity inspector. "
        "Please install synor[qdrant]."
    ) from error
from synor.integrity._model import (
    InspectionIssue,
    InspectionIssueCode,
    InspectionPage,
    IntegrityFact,
    SnapshotConsistency,
    SnapshotDescriptor,
    _metadata_digest,
)

from ._revocation import _metadata_from_record


class QdrantIntegrityInspector:
    """Enumerate governed Qdrant payload metadata without mutation.

    Only points created through :func:`governed_point` form valid facts.
    Unmanaged or malformed points make coverage incomplete instead of being
    silently treated as healthy.
    """

    __slots__ = (
        "_client",
        "_collection_name",
        "_descriptor_digest",
        "_snapshot",
    )

    def __init__(
        self,
        client: QdrantClient,
        collection_name: str,
        *,
        connector_instance_id: str,
        consistent_snapshot_token: str | None = None,
    ) -> None:
        for name, value in (
            ("collection_name", collection_name),
            ("connector_instance_id", connector_instance_id),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be a non-empty string")
        if consistent_snapshot_token is not None and (
            not isinstance(consistent_snapshot_token, str)
            or not consistent_snapshot_token
        ):
            raise ValueError(
                "consistent_snapshot_token must be a non-empty string or None"
            )
        self._client = client
        self._collection_name = collection_name
        if consistent_snapshot_token is None:
            self._snapshot = SnapshotDescriptor(
                token_digest=_metadata_digest(
                    "qdrant_live_snapshot", connector_instance_id, collection_name
                ),
                consistency=SnapshotConsistency.BEST_EFFORT,
            )
        else:
            self._snapshot = SnapshotDescriptor(
                token_digest=_metadata_digest(
                    "qdrant_consistent_snapshot", consistent_snapshot_token
                ),
                consistency=SnapshotConsistency.CONSISTENT,
            )
        self._descriptor_digest = _metadata_digest(
            "qdrant_inspector",
            connector_instance_id,
            collection_name,
            self._snapshot.token_digest,
            self._snapshot.consistency.value,
        )

    @property
    def descriptor_digest(self) -> str:
        return self._descriptor_digest

    async def inspect_page(
        self,
        cursor: str | None,
        *,
        limit: int,
    ) -> InspectionPage:
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("limit must be a positive integer")
        offset = self._decode_cursor(cursor)
        arguments: dict[str, object] = {
            "collection_name": self._collection_name,
            "limit": limit,
            "with_payload": ["synor"],
            "with_vectors": False,
            "consistency": qdrant_models.ReadConsistencyType.ALL,
            "timeout": 30,
        }
        if offset is not None:
            arguments["offset"] = offset
        try:
            records, next_offset = await asyncio.to_thread(
                self._client.scroll,
                **arguments,
            )
        except (UnexpectedResponse, grpc.RpcError) as error:
            issue = self._issue_from_provider_error(error, cursor is not None)
            if issue is None:
                raise
            return InspectionPage((), self._snapshot, issues=(issue,))

        issues: list[InspectionIssue] = []
        facts: list[IntegrityFact] = []
        if not isinstance(records, list):
            records = []
            issues.append(InspectionIssue(InspectionIssueCode.MALFORMED_FACT))
        for record in records:
            try:
                metadata = _metadata_from_record(record)
                item_id = self._canonical_point_id(record.id)
                facts.append(
                    IntegrityFact(
                        identity_digest=metadata.lineage.source_digest,
                        item_digest=_metadata_digest("qdrant_point", item_id),
                        part_digest=metadata.chunk_digest,
                        revision_digest=_metadata_digest(
                            "source_revision", metadata.lineage.source_revision
                        ),
                        content_digest=metadata.content_fingerprint,
                    )
                )
            except (AttributeError, TypeError, ValueError):
                point_id = getattr(record, "id", "invalid")
                issues.append(
                    InspectionIssue(
                        InspectionIssueCode.MALFORMED_FACT,
                        evidence_digest=_metadata_digest(
                            "qdrant_malformed_point", str(point_id)
                        ),
                    )
                )
        if len(facts) > limit:
            facts = facts[:limit]
            issues.append(InspectionIssue(InspectionIssueCode.INCONSISTENT_PAGINATION))
        try:
            next_cursor = self._encode_cursor(next_offset)
        except (TypeError, ValueError):
            next_cursor = None
            issues.append(InspectionIssue(InspectionIssueCode.INCONSISTENT_PAGINATION))
        return InspectionPage(
            facts=tuple(sorted(facts, key=IntegrityFact.sort_key)),
            snapshot=self._snapshot,
            next_cursor=next_cursor,
            issues=tuple(issues),
        )

    @staticmethod
    def _canonical_point_id(value: object) -> str:
        if isinstance(value, bool):
            raise TypeError("Qdrant point ID has an invalid type")
        if isinstance(value, (str, int, uuid.UUID)):
            return str(value)
        raise TypeError("Qdrant point ID has an invalid type")

    @staticmethod
    def _encode_cursor(value: object | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, bool):
            raise TypeError("Qdrant cursor has an invalid type")
        if isinstance(value, int):
            payload = {"kind": "int", "value": str(value)}
        elif isinstance(value, uuid.UUID):
            payload = {"kind": "uuid", "value": str(value)}
        elif isinstance(value, str) and value:
            payload = {"kind": "str", "value": value}
        else:
            raise TypeError("Qdrant cursor has an invalid type")
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _decode_cursor(value: str | None) -> str | int | uuid.UUID | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value:
            raise ValueError("cursor must be a non-empty string or None")
        try:
            payload = json.loads(value)
            kind = payload["kind"]
            raw = payload["value"]
            if not isinstance(raw, str) or not raw:
                raise ValueError
            if kind == "str":
                return raw
            if kind == "int":
                return int(raw)
            if kind == "uuid":
                return uuid.UUID(raw)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            pass
        raise ValueError("Qdrant integrity cursor is invalid") from None

    @staticmethod
    def _issue_from_provider_error(
        error: UnexpectedResponse | grpc.RpcError,
        had_cursor: bool,
    ) -> InspectionIssue | None:
        if isinstance(error, UnexpectedResponse):
            status = error.status_code
            if status in {401, 403}:
                return InspectionIssue(InspectionIssueCode.PERMISSION_DENIED)
            if status in {429, 500, 502, 503, 504}:
                return InspectionIssue(InspectionIssueCode.RATE_LIMIT_EXHAUSTED)
            if had_cursor and status in {400, 404}:
                return InspectionIssue(InspectionIssueCode.CURSOR_EXPIRED)
            return None
        code_fn: Any = getattr(error, "code", None)
        code = code_fn() if callable(code_fn) else None
        if code in {grpc.StatusCode.PERMISSION_DENIED, grpc.StatusCode.UNAUTHENTICATED}:
            return InspectionIssue(InspectionIssueCode.PERMISSION_DENIED)
        if code in {
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
        }:
            return InspectionIssue(InspectionIssueCode.RATE_LIMIT_EXHAUSTED)
        if had_cursor and code in {
            grpc.StatusCode.INVALID_ARGUMENT,
            grpc.StatusCode.NOT_FOUND,
        }:
            return InspectionIssue(InspectionIssueCode.CURSOR_EXPIRED)
        return None


__all__ = ["QdrantIntegrityInspector"]
