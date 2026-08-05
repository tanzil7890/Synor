from __future__ import annotations

import asyncio
import hashlib

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


def _fact(
    identity: str,
    item: str,
    *,
    revision: str,
    part: str | None = None,
) -> IntegrityFact:
    return IntegrityFact(
        identity_digest=_digest(f"identity:{identity}"),
        item_digest=_digest(f"item:{item}"),
        part_digest=_digest(f"part:{part}") if part is not None else None,
        revision_digest=_digest(f"revision:{revision}"),
    )


class _FixtureInspector:
    def __init__(self, name: str, facts: tuple[IntegrityFact, ...]) -> None:
        self.descriptor_digest = _digest(f"descriptor:{name}")
        self._facts = tuple(sorted(facts, key=IntegrityFact.sort_key))
        self._snapshot = SnapshotDescriptor(
            token_digest=_digest(f"snapshot:{name}"),
            consistency=SnapshotConsistency.CONSISTENT,
        )

    async def inspect_page(
        self,
        cursor: str | None,
        *,
        limit: int,
    ) -> InspectionPage:
        start = int(cursor or "0")
        facts = self._facts[start : start + limit]
        next_index = start + len(facts)
        next_cursor = str(next_index) if next_index < len(self._facts) else None
        return InspectionPage(
            facts=facts,
            snapshot=self._snapshot,
            next_cursor=next_cursor,
        )


async def _main() -> None:
    source = _FixtureInspector(
        "source",
        (
            _fact("healthy", "source-healthy", revision="v2"),
            _fact("missing", "source-missing", revision="v1"),
            _fact("stale", "source-stale", revision="v2"),
        ),
    )
    target = _FixtureInspector(
        "target",
        (
            _fact("healthy", "target-healthy", revision="v2", part="chunk-a"),
            _fact("stale", "target-stale", revision="v1", part="chunk-b"),
            _fact("orphan", "target-orphan", revision="v1", part="chunk-c"),
        ),
    )
    report = await scan(
        IntegrityScanConfig(
            source=source,
            target=target,
            profile=IntegrityProfile(
                name="local_demo",
                version="v1",
                report_key=hashlib.sha256(b"test-only-local-demo-key").digest(),
            ),
            page_size=2,
        )
    )
    assert report.summary.healthy_sources == 1
    assert report.summary.missing == 1
    assert report.summary.stale == 1
    assert report.summary.orphan == 1
    print(report.to_json(), end="")


if __name__ == "__main__":
    asyncio.run(_main())
