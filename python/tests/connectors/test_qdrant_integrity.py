from __future__ import annotations

import datetime
import hashlib
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("grpc")
pytest.importorskip("qdrant_client")

from synor._internal.revocation_model import SourceIdentity
from synor.connectors.amazon_s3 import S3IntegrityInspector
from synor.connectors.qdrant import (
    GovernedPointLineage,
    QdrantIntegrityInspector,
    governed_point,
)
from synor.integrity import (
    InspectionIssueCode,
    IntegrityProfile,
    IntegrityScanConfig,
    SnapshotConsistency,
    scan,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _lineage() -> GovernedPointLineage:
    return GovernedPointLineage(
        source_digest=SourceIdentity(
            "s3-production", "bucket-docs", "docs/a.txt"
        ).evidence_digest(),
        source_revision="etag-a",
        policy_id="policy",
        policy_revision="v1",
        group_graph_revision="v1",
        tenant_digest=_digest("tenant"),
        owner_component_digest=_digest("owner"),
        generation=1,
        principal_digests=(_digest("principal"),),
    )


class _FakeQdrantClient:
    def __init__(self, records: list[object]) -> None:
        self.records = records
        self.calls: list[dict[str, Any]] = []

    def scroll(self, **kwargs: Any) -> tuple[list[object], None]:
        self.calls.append(kwargs)
        return self.records[: kwargs["limit"]], None


@pytest.mark.asyncio
async def test_qdrant_inspector_reads_only_strict_governed_metadata() -> None:
    point = governed_point(
        lineage=_lineage(),
        chunk_digest=_digest("chunk"),
        vector=[0.1, 0.2],
        payload={"private": "content"},
    )
    record = SimpleNamespace(id=point.id, payload=point.payload)
    client = _FakeQdrantClient([record])
    inspector = QdrantIntegrityInspector(  # type: ignore[arg-type]
        client,
        "collection",
        connector_instance_id="qdrant-production",
    )

    page = await inspector.inspect_page(None, limit=10)

    assert page.snapshot.consistency is SnapshotConsistency.BEST_EFFORT
    assert page.facts[0].identity_digest == _lineage().source_digest
    assert page.facts[0].part_digest == _digest("chunk")
    assert page.issues == ()
    assert len(client.calls) == 1
    assert client.calls[0]["with_vectors"] is False
    assert client.calls[0]["with_payload"] == ["synor"]
    assert not hasattr(client, "upsert")
    assert "private" not in repr(page)


@pytest.mark.asyncio
async def test_qdrant_unmanaged_point_makes_coverage_incomplete() -> None:
    client = _FakeQdrantClient(
        [SimpleNamespace(id="bad-point", payload={"user": "customer@example.test"})]
    )
    inspector = QdrantIntegrityInspector(  # type: ignore[arg-type]
        client,
        "collection",
        connector_instance_id="qdrant-production",
    )
    page = await inspector.inspect_page(None, limit=10)
    assert page.facts == ()
    assert page.issues[0].code is InspectionIssueCode.MALFORMED_FACT
    assert "customer@example.test" not in repr(page)


class _OneObjectS3:
    async def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        assert set(kwargs) == {"Bucket", "MaxKeys"}
        return {
            "Contents": [
                {
                    "Key": "docs/a.txt",
                    "ETag": '"etag-a"',
                    "LastModified": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
                    "Size": 12,
                }
            ],
            "IsTruncated": False,
        }


@pytest.mark.asyncio
async def test_s3_to_governed_qdrant_mapping_is_healthy() -> None:
    point = governed_point(
        lineage=_lineage(),
        chunk_digest=_digest("chunk"),
        vector=[0.1, 0.2],
    )
    report = await scan(
        IntegrityScanConfig(
            source=S3IntegrityInspector(  # type: ignore[arg-type]
                _OneObjectS3(),
                "bucket",
                connector_instance_id="s3-production",
                source_scope_id="bucket-docs",
                consistent_snapshot_token="s3-inventory",
            ),
            target=QdrantIntegrityInspector(  # type: ignore[arg-type]
                _FakeQdrantClient(
                    [SimpleNamespace(id=point.id, payload=point.payload)]
                ),
                "collection",
                connector_instance_id="qdrant-production",
                consistent_snapshot_token="qdrant-snapshot",
            ),
            profile=IntegrityProfile(
                name="s3_qdrant",
                version="v1",
                report_key=b"r" * 32,
            ),
        )
    )
    assert report.summary.healthy_sources == 1
    assert report.findings == ()
