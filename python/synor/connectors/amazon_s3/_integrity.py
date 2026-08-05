"""Read-only metadata inspection for Amazon S3 integrity scans."""

from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any

try:
    from aiobotocore.client import AioBaseClient  # type: ignore[import-untyped]
    from botocore.exceptions import ClientError  # type: ignore[import-untyped]
except ImportError as error:
    raise ImportError(
        "aiobotocore is required to use the Amazon S3 integrity inspector. "
        "Please install synor[amazon_s3]."
    ) from error
from synor._internal.revocation_model import SourceIdentity
from synor.integrity._model import (
    InspectionIssue,
    InspectionIssueCode,
    InspectionPage,
    IntegrityFact,
    SnapshotConsistency,
    SnapshotDescriptor,
    _metadata_digest,
)


class S3IntegrityInspector:
    """Enumerate S3 object metadata without reading bodies or mutating S3.

    Source identities use Synor's existing ``SourceIdentity`` canonical form
    with the full S3 object key as ``item_id``. Governed target lineage must use
    the same connector instance ID, source scope ID, and item ID. The source
    revision convention is the S3 ETag with one surrounding quote pair removed.

    Live S3 listing is marked best-effort. Pass ``consistent_snapshot_token``
    only when the caller has independently frozen the enumerated inventory.
    """

    __slots__ = (
        "_bucket_name",
        "_client",
        "_connector_instance_id",
        "_descriptor_digest",
        "_prefix",
        "_snapshot",
        "_source_scope_id",
    )

    def __init__(
        self,
        client: AioBaseClient,
        bucket_name: str,
        *,
        connector_instance_id: str,
        source_scope_id: str,
        prefix: str = "",
        consistent_snapshot_token: str | None = None,
    ) -> None:
        for name, value in (
            ("bucket_name", bucket_name),
            ("connector_instance_id", connector_instance_id),
            ("source_scope_id", source_scope_id),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be a non-empty string")
        if not isinstance(prefix, str):
            raise TypeError("prefix must be a string")
        if consistent_snapshot_token is not None and (
            not isinstance(consistent_snapshot_token, str)
            or not consistent_snapshot_token
        ):
            raise ValueError(
                "consistent_snapshot_token must be a non-empty string or None"
            )
        self._client = client
        self._bucket_name = bucket_name
        self._connector_instance_id = connector_instance_id
        self._source_scope_id = source_scope_id
        self._prefix = prefix
        if consistent_snapshot_token is None:
            self._snapshot = SnapshotDescriptor(
                token_digest=_metadata_digest("s3_live_snapshot", bucket_name, prefix),
                consistency=SnapshotConsistency.BEST_EFFORT,
            )
        else:
            self._snapshot = SnapshotDescriptor(
                token_digest=_metadata_digest(
                    "s3_consistent_snapshot", consistent_snapshot_token
                ),
                consistency=SnapshotConsistency.CONSISTENT,
            )
        self._descriptor_digest = _metadata_digest(
            "s3_inspector",
            connector_instance_id,
            source_scope_id,
            bucket_name,
            prefix,
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
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise ValueError("cursor must be a non-empty string or None")
        arguments: dict[str, object] = {
            "Bucket": self._bucket_name,
            "MaxKeys": limit,
        }
        if self._prefix:
            arguments["Prefix"] = self._prefix
        if cursor is not None:
            arguments["ContinuationToken"] = cursor
        try:
            response = await self._client.list_objects_v2(**arguments)
        except ClientError as error:
            issue = self._issue_from_client_error(error, cursor is not None)
            if issue is None:
                raise
            return InspectionPage((), self._snapshot, issues=(issue,))

        raw_contents = response.get("Contents", [])
        issues: list[InspectionIssue] = []
        facts: list[IntegrityFact] = []
        if not isinstance(raw_contents, list):
            raw_contents = []
            issues.append(InspectionIssue(InspectionIssueCode.MALFORMED_FACT))
        for raw_object in raw_contents:
            fact = self._fact_from_object(raw_object)
            if fact is None:
                evidence = _metadata_digest(
                    "s3_malformed_object", str(len(facts) + len(issues))
                )
                issues.append(
                    InspectionIssue(
                        InspectionIssueCode.MALFORMED_FACT,
                        evidence_digest=evidence,
                    )
                )
            else:
                facts.append(fact)
        if len(facts) > limit:
            facts = facts[:limit]
            issues.append(InspectionIssue(InspectionIssueCode.INCONSISTENT_PAGINATION))
        next_cursor = response.get("NextContinuationToken")
        if next_cursor is not None and (
            not isinstance(next_cursor, str) or not next_cursor
        ):
            next_cursor = None
            issues.append(InspectionIssue(InspectionIssueCode.INCONSISTENT_PAGINATION))
        if bool(response.get("IsTruncated")) and next_cursor is None:
            issues.append(InspectionIssue(InspectionIssueCode.INCONSISTENT_PAGINATION))
        return InspectionPage(
            facts=tuple(sorted(facts, key=IntegrityFact.sort_key)),
            snapshot=self._snapshot,
            next_cursor=next_cursor,
            issues=tuple(issues),
        )

    def _fact_from_object(self, value: object) -> IntegrityFact | None:
        if not isinstance(value, Mapping):
            return None
        key = value.get("Key")
        if not isinstance(key, str) or not key or key.endswith("/"):
            return None
        etag = value.get("ETag")
        revision = (
            etag[1:-1]
            if isinstance(etag, str)
            and len(etag) >= 2
            and etag.startswith('"')
            and etag.endswith('"')
            else etag
        )
        revision_digest = (
            _metadata_digest("source_revision", revision)
            if isinstance(revision, str) and revision
            else None
        )
        modified = value.get("LastModified")
        modified_text = (
            modified.astimezone(datetime.UTC).isoformat()
            if isinstance(modified, datetime.datetime)
            and modified.tzinfo is not None
            and modified.utcoffset() is not None
            else ""
        )
        size = value.get("Size")
        content_digest = (
            _metadata_digest("s3_content", etag)
            if isinstance(etag, str) and etag
            else (
                _metadata_digest("s3_content_fallback", modified_text, str(size))
                if modified_text
                and isinstance(size, int)
                and not isinstance(size, bool)
                and size >= 0
                else None
            )
        )
        identity = SourceIdentity(
            connector_instance_id=self._connector_instance_id,
            source_scope_id=self._source_scope_id,
            item_id=key,
        ).evidence_digest()
        return IntegrityFact(
            identity_digest=identity,
            item_digest=_metadata_digest("s3_object", self._bucket_name, key),
            revision_digest=revision_digest,
            content_digest=content_digest,
        )

    @staticmethod
    def _issue_from_client_error(
        error: ClientError,
        had_cursor: bool,
    ) -> InspectionIssue | None:
        response: Any = getattr(error, "response", {})
        metadata = (
            response.get("ResponseMetadata", {}) if isinstance(response, dict) else {}
        )
        status = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
        error_data = response.get("Error", {}) if isinstance(response, dict) else {}
        code = error_data.get("Code") if isinstance(error_data, dict) else None
        if status in {401, 403} or code in {"AccessDenied", "InvalidAccessKeyId"}:
            return InspectionIssue(InspectionIssueCode.PERMISSION_DENIED)
        if status in {429, 500, 502, 503, 504} or code in {
            "SlowDown",
            "RequestTimeout",
            "Throttling",
        }:
            return InspectionIssue(InspectionIssueCode.RATE_LIMIT_EXHAUSTED)
        if had_cursor and code in {"InvalidArgument", "InvalidToken"}:
            return InspectionIssue(InspectionIssueCode.CURSOR_EXPIRED)
        return None


__all__ = ["S3IntegrityInspector"]
