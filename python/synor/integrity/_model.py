from __future__ import annotations

import enum
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SAFE_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


def _metadata_digest(domain: str, *values: str) -> str:
    if not isinstance(domain, str) or _SAFE_CODE.fullmatch(domain) is None:
        raise ValueError("digest domain must be a safe lowercase identifier")
    encoded = bytearray(b"synor-integrity-metadata-v1\x00")
    encoded.extend(domain.encode("ascii"))
    encoded.extend(b"\x00")
    for value in values:
        if not isinstance(value, str):
            raise TypeError("metadata digest values must be strings")
        payload = value.encode("utf-8")
        encoded.extend(len(payload).to_bytes(8, "big"))
        encoded.extend(payload)
    return hashlib.sha256(encoded).hexdigest()


def _require_digest(name: str, value: str) -> None:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def _require_safe_code(name: str, value: str) -> None:
    if not isinstance(value, str) or _SAFE_CODE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a safe lowercase identifier")


class SnapshotConsistency(str, enum.Enum):
    """Strength of the provider view used by an inspector."""

    CONSISTENT = "consistent"
    BEST_EFFORT = "best_effort"


class InspectionIssueCode(str, enum.Enum):
    """Controlled reasons why scan coverage is incomplete."""

    PERMISSION_DENIED = "permission_denied"
    RATE_LIMIT_EXHAUSTED = "rate_limit_exhausted"
    CURSOR_EXPIRED = "cursor_expired"
    INCONSISTENT_PAGINATION = "inconsistent_pagination"
    MALFORMED_FACT = "malformed_fact"
    MAPPING_AMBIGUOUS = "mapping_ambiguous"


class FindingType(str, enum.Enum):
    """Supported source-to-target integrity findings."""

    MISSING = "missing"
    ORPHAN = "orphan"
    STALE = "stale"
    DUPLICATE = "duplicate"
    AMBIGUOUS = "ambiguous"


class FindingConfidence(str, enum.Enum):
    """Evidence strength for an individual finding."""

    PROVEN = "proven"
    HEURISTIC = "heuristic"
    UNKNOWN = "unknown"


class ScanStatus(str, enum.Enum):
    """Whether all configured source and target facts were inspected."""

    COMPLETE = "complete"
    INCOMPLETE = "incomplete"


@dataclass(frozen=True, slots=True)
class SnapshotDescriptor:
    """Redacted identity and consistency contract for one provider view."""

    token_digest: str
    consistency: SnapshotConsistency

    def __post_init__(self) -> None:
        _require_digest("token_digest", self.token_digest)
        if not isinstance(self.consistency, SnapshotConsistency):
            raise TypeError("consistency must be SnapshotConsistency")


@dataclass(frozen=True, slots=True)
class IntegrityFact:
    """One metadata-only fact emitted by a read-only inspector.

    ``identity_digest`` joins a source item to its derived target items.
    ``item_digest`` identifies this exact source object or target record.
    ``part_digest`` identifies a derived part, such as a chunk, and enables
    duplicate detection without exporting its raw identifier.
    """

    identity_digest: str
    item_digest: str
    part_digest: str | None = None
    revision_digest: str | None = None
    content_digest: str | None = None

    def __post_init__(self) -> None:
        _require_digest("identity_digest", self.identity_digest)
        _require_digest("item_digest", self.item_digest)
        for name, value in (
            ("part_digest", self.part_digest),
            ("revision_digest", self.revision_digest),
            ("content_digest", self.content_digest),
        ):
            if value is not None:
                _require_digest(name, value)

    def sort_key(self) -> tuple[str, str, str]:
        return (self.identity_digest, self.part_digest or "", self.item_digest)


@dataclass(frozen=True, slots=True)
class InspectionIssue:
    """Privacy-safe evidence that an inspector could not prove full coverage."""

    code: InspectionIssueCode
    evidence_digest: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.code, InspectionIssueCode):
            raise TypeError("code must be InspectionIssueCode")
        if self.evidence_digest is not None:
            _require_digest("evidence_digest", self.evidence_digest)


@dataclass(frozen=True, slots=True)
class InspectionPage:
    """One bounded, sorted page returned by an integrity inspector."""

    facts: tuple[IntegrityFact, ...]
    snapshot: SnapshotDescriptor
    next_cursor: str | None = None
    issues: tuple[InspectionIssue, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.facts, tuple) or not all(
            isinstance(fact, IntegrityFact) for fact in self.facts
        ):
            raise TypeError("facts must be a tuple of IntegrityFact values")
        if tuple(sorted(self.facts, key=IntegrityFact.sort_key)) != self.facts:
            raise ValueError("inspection page facts must be sorted")
        item_digests = [fact.item_digest for fact in self.facts]
        if len(item_digests) != len(set(item_digests)):
            raise ValueError("inspection page item digests must be unique")
        if not isinstance(self.snapshot, SnapshotDescriptor):
            raise TypeError("snapshot must be SnapshotDescriptor")
        if self.next_cursor is not None and (
            not isinstance(self.next_cursor, str) or not self.next_cursor
        ):
            raise ValueError("next_cursor must be a non-empty string or None")
        if not isinstance(self.issues, tuple) or not all(
            isinstance(issue, InspectionIssue) for issue in self.issues
        ):
            raise TypeError("issues must be a tuple of InspectionIssue values")


@dataclass(frozen=True, slots=True)
class IntegrityFinding:
    """A deterministic, content-free source/target integrity finding."""

    finding_id: str
    kind: FindingType
    confidence: FindingConfidence
    source_digest: str | None
    target_digests: tuple[str, ...]
    target_count: int
    evidence_digest: str
    detail_code: str

    def __post_init__(self) -> None:
        if not self.finding_id.startswith("int1_"):
            raise ValueError("finding_id must use the int1_ schema prefix")
        _require_digest("finding_id", self.finding_id.removeprefix("int1_"))
        if not isinstance(self.kind, FindingType):
            raise TypeError("kind must be FindingType")
        if not isinstance(self.confidence, FindingConfidence):
            raise TypeError("confidence must be FindingConfidence")
        if self.source_digest is not None:
            _require_digest("source_digest", self.source_digest)
        if (
            not isinstance(self.target_digests, tuple)
            or not all(isinstance(value, str) for value in self.target_digests)
            or tuple(sorted(set(self.target_digests))) != self.target_digests
        ):
            raise ValueError("target_digests must be a sorted unique tuple")
        for value in self.target_digests:
            _require_digest("target_digest", value)
        if (
            not isinstance(self.target_count, int)
            or isinstance(self.target_count, bool)
            or self.target_count < len(self.target_digests)
        ):
            raise ValueError(
                "target_count must be an integer at least as large as target_digests"
            )
        _require_digest("evidence_digest", self.evidence_digest)
        _require_safe_code("detail_code", self.detail_code)

    def to_dict(self) -> dict[str, object]:
        return {
            "finding_id": self.finding_id,
            "kind": self.kind.value,
            "confidence": self.confidence.value,
            "source_digest": self.source_digest,
            "target_digests": list(self.target_digests),
            "target_count": self.target_count,
            "evidence_digest": self.evidence_digest,
            "detail_code": self.detail_code,
        }


@dataclass(frozen=True, slots=True)
class ScanSummary:
    source_facts: int
    target_facts: int
    healthy_sources: int
    missing: int
    orphan: int
    stale: int
    duplicate: int
    ambiguous: int

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")

    def to_dict(self) -> dict[str, int]:
        return {
            "source_facts": self.source_facts,
            "target_facts": self.target_facts,
            "healthy_sources": self.healthy_sources,
            "missing": self.missing,
            "orphan": self.orphan,
            "stale": self.stale,
            "duplicate": self.duplicate,
            "ambiguous": self.ambiguous,
        }


@dataclass(frozen=True, slots=True)
class ScanCoverage:
    status: ScanStatus
    source_snapshot: SnapshotDescriptor
    target_snapshot: SnapshotDescriptor
    issues: tuple[InspectionIssue, ...]
    issues_truncated: bool
    findings_truncated: bool

    def __post_init__(self) -> None:
        if not isinstance(self.status, ScanStatus):
            raise TypeError("status must be ScanStatus")
        if not isinstance(self.source_snapshot, SnapshotDescriptor):
            raise TypeError("source_snapshot must be SnapshotDescriptor")
        if not isinstance(self.target_snapshot, SnapshotDescriptor):
            raise TypeError("target_snapshot must be SnapshotDescriptor")
        if not isinstance(self.issues, tuple) or not all(
            isinstance(issue, InspectionIssue) for issue in self.issues
        ):
            raise TypeError("issues must be a tuple of InspectionIssue values")
        if type(self.issues_truncated) is not bool:
            raise TypeError("issues_truncated must be a bool")
        if type(self.findings_truncated) is not bool:
            raise TypeError("findings_truncated must be a bool")

    def to_dict(self) -> dict[str, object]:
        def _snapshot(value: SnapshotDescriptor) -> dict[str, str]:
            return {
                "token_digest": value.token_digest,
                "consistency": value.consistency.value,
            }

        return {
            "status": self.status.value,
            "source_snapshot": _snapshot(self.source_snapshot),
            "target_snapshot": _snapshot(self.target_snapshot),
            "issues": [
                {
                    "code": issue.code.value,
                    "evidence_digest": issue.evidence_digest,
                }
                for issue in self.issues
            ],
            "issues_truncated": self.issues_truncated,
            "findings_truncated": self.findings_truncated,
        }


@dataclass(frozen=True, slots=True)
class IntegrityReport:
    """Versioned deterministic report returned by :func:`scan`."""

    profile_name: str
    profile_version: str
    profile_digest: str
    coverage: ScanCoverage
    summary: ScanSummary
    findings: tuple[IntegrityFinding, ...]
    schema_version: int = 1

    def __post_init__(self) -> None:
        _require_safe_code("profile_name", self.profile_name)
        _require_safe_code("profile_version", self.profile_version)
        _require_digest("profile_digest", self.profile_digest)
        if not isinstance(self.coverage, ScanCoverage):
            raise TypeError("coverage must be ScanCoverage")
        if not isinstance(self.summary, ScanSummary):
            raise TypeError("summary must be ScanSummary")
        if not isinstance(self.findings, tuple) or not all(
            isinstance(finding, IntegrityFinding) for finding in self.findings
        ):
            raise TypeError("findings must be a tuple of IntegrityFinding values")
        if (
            tuple(sorted(self.findings, key=lambda item: item.finding_id))
            != self.findings
        ):
            raise ValueError("findings must be sorted by finding_id")
        if self.schema_version != 1:
            raise ValueError("unsupported integrity report schema version")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "synor.integrity.report",
            "schema_version": self.schema_version,
            "profile": {
                "name": self.profile_name,
                "version": self.profile_version,
                "digest": self.profile_digest,
            },
            "coverage": self.coverage.to_dict(),
            "summary": self.summary.to_dict(),
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        """Encode a deterministic JSON projection with a trailing newline."""

        if indent is not None and (
            not isinstance(indent, int) or isinstance(indent, bool) or indent < 0
        ):
            raise ValueError("indent must be a non-negative integer or None")
        separators = (",", ":") if indent is None else None
        return (
            json.dumps(
                self.to_dict(),
                ensure_ascii=True,
                indent=indent,
                separators=separators,
                sort_keys=True,
            )
            + "\n"
        )
