from __future__ import annotations

import datetime

import pytest
from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from synor._internal.revocation_model import SourceIdentity
from synor.connectors.amazon_s3 import S3IntegrityInspector
from synor.integrity import InspectionIssueCode, SnapshotConsistency


class _FakeS3Client:
    def __init__(self, pages: dict[str | None, dict[str, object]]) -> None:
        self.pages = pages
        self.calls: list[dict[str, object]] = []

    async def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        cursor = kwargs.get("ContinuationToken")
        if cursor is not None and not isinstance(cursor, str):
            raise TypeError("test continuation token must be a string")
        return self.pages[cursor]


@pytest.mark.asyncio
async def test_s3_inspector_is_paginated_read_only_and_matches_source_identity() -> (
    None
):
    modified = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    client = _FakeS3Client(
        {
            None: {
                "Contents": [
                    {
                        "Key": "docs/a.txt",
                        "ETag": '"etag-a"',
                        "LastModified": modified,
                        "Size": 12,
                    }
                ],
                "IsTruncated": True,
                "NextContinuationToken": "next",
            },
            "next": {
                "Contents": [
                    {
                        "Key": "docs/b.txt",
                        "ETag": '"etag-b"',
                        "LastModified": modified,
                        "Size": 13,
                    }
                ],
                "IsTruncated": False,
            },
        }
    )
    inspector = S3IntegrityInspector(  # type: ignore[arg-type]
        client,
        "bucket",
        connector_instance_id="s3-production",
        source_scope_id="bucket-docs",
        prefix="docs/",
        consistent_snapshot_token="inventory-42",
    )

    first = await inspector.inspect_page(None, limit=1)
    second = await inspector.inspect_page(first.next_cursor, limit=1)

    assert first.snapshot.consistency is SnapshotConsistency.CONSISTENT
    assert (
        first.facts[0].identity_digest
        == SourceIdentity(
            "s3-production", "bucket-docs", "docs/a.txt"
        ).evidence_digest()
    )
    assert second.next_cursor is None
    assert [set(call) for call in client.calls] == [
        {"Bucket", "MaxKeys", "Prefix"},
        {"Bucket", "MaxKeys", "Prefix", "ContinuationToken"},
    ]


class _DeniedS3Client:
    async def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        del kwargs
        raise ClientError(
            {
                "Error": {"Code": "AccessDenied", "Message": "private path"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            "ListObjectsV2",
        )


@pytest.mark.asyncio
async def test_s3_permission_failure_is_incomplete_without_provider_message() -> None:
    inspector = S3IntegrityInspector(  # type: ignore[arg-type]
        _DeniedS3Client(),
        "bucket",
        connector_instance_id="s3-production",
        source_scope_id="bucket-docs",
    )
    page = await inspector.inspect_page(None, limit=10)
    assert page.facts == ()
    assert page.issues[0].code is InspectionIssueCode.PERMISSION_DENIED
    assert "private path" not in repr(page)


@pytest.mark.asyncio
async def test_s3_very_long_identifier_remains_digest_only() -> None:
    raw_key = "private/" + "x" * 10_000
    client = _FakeS3Client(
        {
            None: {
                "Contents": [
                    {
                        "Key": raw_key,
                        "ETag": '"etag"',
                        "LastModified": datetime.datetime(
                            2026, 1, 1, tzinfo=datetime.UTC
                        ),
                        "Size": 1,
                    }
                ],
                "IsTruncated": False,
            }
        }
    )
    page = await S3IntegrityInspector(  # type: ignore[arg-type]
        client,
        "bucket",
        connector_instance_id="s3-production",
        source_scope_id="bucket-docs",
    ).inspect_page(None, limit=10)
    assert len(page.facts[0].identity_digest) == 64
    assert raw_key not in repr(page)
