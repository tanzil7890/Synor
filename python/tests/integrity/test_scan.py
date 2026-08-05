from __future__ import annotations

import asyncio
import hashlib
import json
import os
import threading
from pathlib import Path

import pytest
from synor.integrity import (
    FindingConfidence,
    FindingType,
    InspectionIssue,
    InspectionIssueCode,
    InspectionPage,
    IntegrityFact,
    IntegrityProfile,
    IntegrityResourceLimitError,
    IntegrityResumeError,
    IntegrityScanConfig,
    IntegrityScanError,
    ScanStatus,
    SnapshotConsistency,
    SnapshotDescriptor,
    scan,
)
from synor.integrity._scan import _run_blocking


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _fact(
    identity: str,
    item: str,
    *,
    part: str | None = None,
    revision: str | None = "v1",
    content: str | None = None,
) -> IntegrityFact:
    return IntegrityFact(
        identity_digest=_digest(f"identity:{identity}"),
        item_digest=_digest(f"item:{item}"),
        part_digest=_digest(f"part:{part}") if part is not None else None,
        revision_digest=(
            _digest(f"revision:{revision}") if revision is not None else None
        ),
        content_digest=_digest(f"content:{content}") if content is not None else None,
    )


class _PagedInspector:
    def __init__(
        self,
        name: str,
        facts: list[IntegrityFact],
        *,
        consistency: SnapshotConsistency = SnapshotConsistency.CONSISTENT,
        issues: tuple[InspectionIssue, ...] = (),
        fail_call: int | None = None,
        repeated_cursor: bool = False,
    ) -> None:
        self.descriptor_digest = _digest(f"descriptor:{name}")
        self._facts = sorted(facts, key=IntegrityFact.sort_key)
        self._snapshot = SnapshotDescriptor(_digest(f"snapshot:{name}"), consistency)
        self._issues = issues
        self._fail_call = fail_call
        self._repeated_cursor = repeated_cursor
        self.calls: list[str | None] = []

    async def inspect_page(
        self,
        cursor: str | None,
        *,
        limit: int,
    ) -> InspectionPage:
        self.calls.append(cursor)
        if self._fail_call == len(self.calls):
            self._fail_call = None
            raise RuntimeError("Bearer secret customer@example.test")
        start = int(cursor or "0")
        values = self._facts[start : start + limit]
        next_index = start + len(values)
        next_cursor = str(next_index) if next_index < len(self._facts) else None
        if self._repeated_cursor and next_cursor is not None and cursor is not None:
            next_cursor = cursor
        return InspectionPage(
            facts=tuple(values),
            snapshot=self._snapshot,
            next_cursor=next_cursor,
            issues=self._issues if cursor is None else (),
        )


def _profile(**changes: object) -> IntegrityProfile:
    values: dict[str, object] = {
        "name": "test_profile",
        "version": "v1",
        "report_key": b"r" * 32,
    }
    values.update(changes)
    return IntegrityProfile(**values)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_scan_classifies_all_supported_findings() -> None:
    sources = [
        _fact("healthy", "source-healthy"),
        _fact("missing", "source-missing"),
        _fact("stale", "source-stale"),
        _fact("ambiguous", "source-ambiguous", revision=None),
        _fact("duplicate-source", "source-duplicate-a"),
        _fact("duplicate-source", "source-duplicate-b"),
        _fact("duplicate-target", "source-duplicate-target"),
    ]
    targets = [
        _fact("healthy", "target-healthy", part="healthy"),
        _fact("stale", "target-stale", part="stale", revision="old"),
        _fact("ambiguous", "target-ambiguous", part="ambiguous"),
        _fact("duplicate-source", "target-duplicate-source", part="one"),
        _fact("duplicate-target", "target-duplicate-a", part="same"),
        _fact("duplicate-target", "target-duplicate-b", part="same"),
        _fact("orphan", "target-orphan", part="orphan"),
    ]

    report = await scan(
        IntegrityScanConfig(
            source=_PagedInspector("source", sources),
            target=_PagedInspector("target", targets),
            profile=_profile(),
            page_size=2,
        )
    )

    assert report.coverage.status is ScanStatus.COMPLETE
    assert report.summary.source_facts == 7
    assert report.summary.target_facts == 7
    assert report.summary.healthy_sources == 1
    assert report.summary.missing == 1
    assert report.summary.orphan == 1
    assert report.summary.stale == 1
    assert report.summary.duplicate == 2
    assert report.summary.ambiguous == 2
    assert {finding.kind for finding in report.findings} == set(FindingType)
    assert all(
        finding.confidence is FindingConfidence.PROVEN for finding in report.findings
    )


@pytest.mark.asyncio
async def test_empty_scan_is_complete_and_healthy() -> None:
    report = await scan(
        IntegrityScanConfig(
            source=_PagedInspector("source", []),
            target=_PagedInspector("target", []),
            profile=_profile(),
        )
    )
    assert report.coverage.status is ScanStatus.COMPLETE
    assert report.summary.source_facts == 0
    assert report.summary.target_facts == 0
    assert report.summary.healthy_sources == 0
    assert report.findings == ()


@pytest.mark.asyncio
async def test_best_effort_snapshot_produces_heuristic_findings() -> None:
    report = await scan(
        IntegrityScanConfig(
            source=_PagedInspector(
                "source",
                [_fact("missing", "source")],
                consistency=SnapshotConsistency.BEST_EFFORT,
            ),
            target=_PagedInspector("target", []),
            profile=_profile(),
        )
    )
    assert report.findings[0].confidence is FindingConfidence.HEURISTIC


@pytest.mark.asyncio
async def test_inspection_issue_never_reports_clean_coverage() -> None:
    report = await scan(
        IntegrityScanConfig(
            source=_PagedInspector(
                "source",
                [],
                issues=(InspectionIssue(InspectionIssueCode.PERMISSION_DENIED),),
            ),
            target=_PagedInspector("target", []),
            profile=_profile(),
        )
    )
    assert report.coverage.status is ScanStatus.INCOMPLETE
    assert report.coverage.issues[0].code is InspectionIssueCode.PERMISSION_DENIED


@pytest.mark.asyncio
async def test_report_is_deterministic_and_contains_no_raw_identifiers() -> None:
    raw_identity = "customer@example.test/private/document.txt"
    source = _PagedInspector("source", [_fact(raw_identity, "source")])
    target = _PagedInspector("target", [])
    first = await scan(
        IntegrityScanConfig(source=source, target=target, profile=_profile())
    )
    second = await scan(
        IntegrityScanConfig(
            source=_PagedInspector("source", [_fact(raw_identity, "source")]),
            target=_PagedInspector("target", []),
            profile=_profile(),
        )
    )
    encoded = first.to_json()
    assert first.to_dict() == second.to_dict()
    assert raw_identity not in encoded
    assert "customer@example.test" not in encoded
    assert _digest("snapshot:source") not in encoded
    assert _digest("snapshot:target") not in encoded
    assert encoded.endswith("\n")


@pytest.mark.asyncio
async def test_finding_projection_is_bounded_but_counts_are_complete() -> None:
    sources = [_fact(f"missing-{index}", f"source-{index}") for index in range(5)]
    report = await scan(
        IntegrityScanConfig(
            source=_PagedInspector("source", sources),
            target=_PagedInspector("target", []),
            profile=_profile(),
            max_findings=2,
        )
    )
    assert report.summary.missing == 5
    assert len(report.findings) == 2
    assert report.coverage.findings_truncated is True


@pytest.mark.asyncio
async def test_issue_projection_is_bounded_and_explicitly_truncated() -> None:
    issues = tuple(
        InspectionIssue(
            InspectionIssueCode.MALFORMED_FACT,
            evidence_digest=_digest(f"issue:{index}"),
        )
        for index in range(5)
    )
    report = await scan(
        IntegrityScanConfig(
            source=_PagedInspector("source", [], issues=issues),
            target=_PagedInspector("target", []),
            profile=_profile(),
            max_issues=2,
        )
    )
    assert report.coverage.status is ScanStatus.INCOMPLETE
    assert len(report.coverage.issues) == 2
    assert report.coverage.issues_truncated is True
    assert report.to_dict()["coverage"]["issues_truncated"] is True  # type: ignore[index]


@pytest.mark.asyncio
async def test_spill_disk_budget_fails_closed() -> None:
    sources = [_fact(f"source-{index}", f"source-{index}") for index in range(10_000)]
    with pytest.raises(IntegrityResourceLimitError, match="spill-disk budget"):
        await scan(
            IntegrityScanConfig(
                source=_PagedInspector("source", sources),
                target=_PagedInspector("target", []),
                profile=_profile(),
                page_size=10_000,
                max_disk_bytes=1_048_576,
            )
        )


@pytest.mark.asyncio
async def test_blocking_cleanup_finishes_before_cancellation_propagates() -> None:
    started = threading.Event()
    release = threading.Event()

    def _operation() -> None:
        started.set()
        assert release.wait(timeout=5)

    task = asyncio.create_task(_run_blocking(_operation))
    assert await asyncio.to_thread(started.wait, 5)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_checkpoint_resumes_after_safe_failure(tmp_path: Path) -> None:
    checkpoint = tmp_path / "scan.sqlite3"
    sources = [_fact(f"source-{index}", f"source-{index}") for index in range(5)]
    targets = [
        _fact(f"source-{index}", f"target-{index}", part=f"part-{index}")
        for index in range(5)
    ]
    source = _PagedInspector("source", sources)
    target = _PagedInspector("target", targets, fail_call=2)
    config = IntegrityScanConfig(
        source=source,
        target=target,
        profile=_profile(),
        checkpoint_path=checkpoint,
        page_size=2,
    )

    with pytest.raises(IntegrityScanError, match="target inspector failed") as caught:
        await scan(config)
    assert "secret" not in str(caught.value)
    assert "customer@example.test" not in str(caught.value)
    assert checkpoint.exists()
    if os.name != "nt":
        assert checkpoint.stat().st_mode & 0o077 == 0

    report = await scan(config)
    assert report.summary.healthy_sources == 5
    assert target.calls[-1] == "4"
    assert not checkpoint.exists()


@pytest.mark.parametrize(
    ("failing_side", "fail_call"),
    [
        ("source", 1),
        ("source", 2),
        ("source", 3),
        ("target", 1),
        ("target", 2),
        ("target", 3),
    ],
)
@pytest.mark.asyncio
async def test_interruption_at_every_page_resumes(
    tmp_path: Path,
    failing_side: str,
    fail_call: int,
) -> None:
    facts = [_fact(f"source-{index}", f"source-{index}") for index in range(5)]
    targets = [
        _fact(f"source-{index}", f"target-{index}", part=f"part-{index}")
        for index in range(5)
    ]
    source = _PagedInspector(
        "source",
        facts,
        fail_call=fail_call if failing_side == "source" else None,
    )
    target = _PagedInspector(
        "target",
        targets,
        fail_call=fail_call if failing_side == "target" else None,
    )
    config = IntegrityScanConfig(
        source=source,
        target=target,
        profile=_profile(),
        checkpoint_path=tmp_path / f"{failing_side}-{fail_call}.sqlite3",
        page_size=2,
    )
    with pytest.raises(IntegrityScanError):
        await scan(config)
    report = await scan(config)
    assert report.summary.healthy_sources == 5
    assert report.findings == ()


@pytest.mark.asyncio
async def test_resume_rejects_changed_profile(tmp_path: Path) -> None:
    checkpoint = tmp_path / "scan.sqlite3"
    source = _PagedInspector("source", [_fact("source", "source")])
    target = _PagedInspector("target", [_fact("source", "target")], fail_call=1)
    with pytest.raises(IntegrityScanError):
        await scan(
            IntegrityScanConfig(
                source=source,
                target=target,
                profile=_profile(),
                checkpoint_path=checkpoint,
            )
        )

    with pytest.raises(IntegrityResumeError, match="different scan configuration"):
        await scan(
            IntegrityScanConfig(
                source=source,
                target=target,
                profile=_profile(version="v2"),
                checkpoint_path=checkpoint,
            )
        )


@pytest.mark.asyncio
async def test_repeated_cursor_terminates_with_incomplete_coverage() -> None:
    source = _PagedInspector(
        "source",
        [_fact(f"source-{index}", f"source-{index}") for index in range(5)],
        repeated_cursor=True,
    )
    report = await scan(
        IntegrityScanConfig(
            source=source,
            target=_PagedInspector("target", []),
            profile=_profile(),
            page_size=2,
        )
    )
    assert report.coverage.status is ScanStatus.INCOMPLETE
    assert InspectionIssueCode.INCONSISTENT_PAGINATION in {
        issue.code for issue in report.coverage.issues
    }


class _DuplicatePageInspector:
    descriptor_digest = _digest("duplicate-page-inspector")

    def __init__(self) -> None:
        self._fact = _fact("identity", "item")
        self._snapshot = SnapshotDescriptor(
            _digest("duplicate-page-snapshot"), SnapshotConsistency.CONSISTENT
        )

    async def inspect_page(
        self,
        cursor: str | None,
        *,
        limit: int,
    ) -> InspectionPage:
        assert limit > 0
        return InspectionPage(
            (self._fact,),
            self._snapshot,
            next_cursor="second" if cursor is None else None,
        )


@pytest.mark.asyncio
async def test_fact_repeated_across_pages_is_incomplete() -> None:
    report = await scan(
        IntegrityScanConfig(
            source=_DuplicatePageInspector(),
            target=_PagedInspector("target", []),
            profile=_profile(),
        )
    )
    assert report.coverage.status is ScanStatus.INCOMPLETE
    assert InspectionIssueCode.INCONSISTENT_PAGINATION in {
        issue.code for issue in report.coverage.issues
    }


def test_page_rejects_reorder_and_duplicates() -> None:
    first = _fact("b", "first")
    second = _fact("a", "second")
    snapshot = SnapshotDescriptor(_digest("snapshot"), SnapshotConsistency.CONSISTENT)
    ordered = sorted((first, second), key=IntegrityFact.sort_key)
    with pytest.raises(ValueError, match="sorted"):
        InspectionPage(tuple(reversed(ordered)), snapshot)
    with pytest.raises(ValueError, match="unique"):
        InspectionPage((first, first), snapshot)


def test_profile_and_fact_validate_weak_input_early() -> None:
    with pytest.raises(ValueError, match="at least 32"):
        IntegrityProfile("profile", "v1", b"short")
    with pytest.raises(ValueError, match="SHA-256"):
        IntegrityFact("raw", _digest("item"))


def test_configuration_repr_hides_keys_and_inspectors() -> None:
    canary = "credential-canary-must-never-appear"

    class _CanaryInspector(_PagedInspector):
        def __repr__(self) -> str:
            return canary

    profile = IntegrityProfile("profile", "v1", canary.encode())
    config = IntegrityScanConfig(
        source=_CanaryInspector("source", []),
        target=_CanaryInspector("target", []),
        profile=profile,
    )
    assert canary not in repr(profile)
    assert canary not in repr(config)


@pytest.mark.asyncio
async def test_versioned_golden_fixture_contract() -> None:
    fixture_path = Path(__file__).parent / "fixtures" / "v1" / "golden.json"
    fixture = json.loads(fixture_path.read_text())
    assert fixture["schema"] == "synor.integrity.fixture"
    assert fixture["schema_version"] == 1
    sources = [
        _fact(
            value["identity"],
            value["item"],
            revision=value["revision"],
        )
        for value in fixture["sources"]
    ]
    targets = [
        _fact(
            value["identity"],
            value["item"],
            part=value["part"],
            revision=value["revision"],
        )
        for value in fixture["targets"]
    ]
    report = await scan(
        IntegrityScanConfig(
            source=_PagedInspector("fixture-source", sources),
            target=_PagedInspector("fixture-target", targets),
            profile=_profile(),
        )
    )
    assert report.summary.to_dict() == fixture["expected_summary"]
