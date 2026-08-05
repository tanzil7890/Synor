from __future__ import annotations

import hashlib

from synor.integrity import (
    InspectionIssue,
    InspectionIssueCode,
    InspectionPage,
    IntegrityFact,
    IntegrityProfile,
    IntegrityScanConfig,
    SnapshotConsistency,
    SnapshotDescriptor,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class _Inspector:
    def __init__(self, name: str, *, incomplete: bool = False) -> None:
        self.descriptor_digest = _digest(f"descriptor:{name}")
        self._snapshot = SnapshotDescriptor(
            _digest(f"snapshot:{name}"), SnapshotConsistency.CONSISTENT
        )
        self._incomplete = incomplete

    async def inspect_page(self, cursor: str | None, *, limit: int) -> InspectionPage:
        assert cursor is None
        assert limit > 0
        identity = _digest("identity")
        facts = (
            IntegrityFact(
                identity_digest=identity,
                item_digest=_digest("item"),
                revision_digest=_digest("revision"),
            ),
        )
        issues = (
            (InspectionIssue(InspectionIssueCode.PERMISSION_DENIED),)
            if self._incomplete
            else ()
        )
        return InspectionPage(facts, self._snapshot, issues=issues)


_PROFILE = IntegrityProfile(
    name="cli_test",
    version="v1",
    report_key=b"test-only-integrity-report-key-00",
)

SCAN = IntegrityScanConfig(
    source=_Inspector("source"),
    target=_Inspector("target"),
    profile=_PROFILE,
)

INCOMPLETE_SCAN = IntegrityScanConfig(
    source=_Inspector("source-incomplete", incomplete=True),
    target=_Inspector("target-incomplete"),
    profile=_PROFILE,
)
