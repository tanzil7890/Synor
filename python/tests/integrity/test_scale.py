from __future__ import annotations

import hashlib
import tracemalloc

import pytest
from synor.integrity import (
    InspectionPage,
    IntegrityFact,
    IntegrityProfile,
    IntegrityScanConfig,
    SnapshotConsistency,
    SnapshotDescriptor,
    scan,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class _ScaleInspector:
    def __init__(self, name: str, facts: tuple[IntegrityFact, ...]) -> None:
        self.descriptor_digest = _digest(f"descriptor:{name}")
        self._facts = facts
        self._snapshot = SnapshotDescriptor(
            _digest(f"snapshot:{name}"), SnapshotConsistency.CONSISTENT
        )
        self.calls = 0

    async def inspect_page(
        self,
        cursor: str | None,
        *,
        limit: int,
    ) -> InspectionPage:
        self.calls += 1
        start = int(cursor or "0")
        facts = self._facts[start : start + limit]
        next_index = start + len(facts)
        return InspectionPage(
            facts=facts,
            snapshot=self._snapshot,
            next_cursor=(str(next_index) if next_index < len(self._facts) else None),
        )


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_100k_healthy_facts_stay_within_scan_budget() -> None:
    source_facts: list[IntegrityFact] = []
    target_facts: list[IntegrityFact] = []
    revision = _digest("revision")
    for index in range(100_000):
        identity = _digest(f"identity:{index:06}")
        source_facts.append(
            IntegrityFact(
                identity_digest=identity,
                item_digest=_digest(f"source:{index:06}"),
                revision_digest=revision,
            )
        )
        target_facts.append(
            IntegrityFact(
                identity_digest=identity,
                item_digest=_digest(f"target:{index:06}"),
                part_digest=_digest(f"part:{index:06}"),
                revision_digest=revision,
            )
        )
    source_facts.sort(key=IntegrityFact.sort_key)
    target_facts.sort(key=IntegrityFact.sort_key)

    source = _ScaleInspector("source", tuple(source_facts))
    target = _ScaleInspector("target", tuple(target_facts))
    tracemalloc.start()
    try:
        report = await scan(
            IntegrityScanConfig(
                source=source,
                target=target,
                profile=IntegrityProfile(
                    name="scale",
                    version="v1",
                    report_key=b"scale-test-report-key-0000000000",
                ),
                page_size=10_000,
            )
        )
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak_bytes < 32 * 1024 * 1024
    assert source.calls == 10
    assert target.calls == 10
    assert report.summary.source_facts == 100_000
    assert report.summary.target_facts == 100_000
    assert report.summary.healthy_sources == 100_000
    assert report.findings == ()
