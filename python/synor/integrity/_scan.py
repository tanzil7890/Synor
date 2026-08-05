from __future__ import annotations

import asyncio
import hashlib
import itertools
import json
import os
import pathlib
import sqlite3
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Literal, TypeVar, cast

from synor.connectorkits.integrity import IntegrityInspector

from ._model import (
    FindingConfidence,
    FindingType,
    InspectionIssue,
    InspectionIssueCode,
    InspectionPage,
    IntegrityFinding,
    IntegrityReport,
    ScanCoverage,
    ScanStatus,
    ScanSummary,
    SnapshotConsistency,
    SnapshotDescriptor,
    _require_digest,
)
from ._profile import IntegrityProfile

_Side = Literal["source", "target"]
_T = TypeVar("_T")
_MAX_TARGET_REFERENCES = 32


class IntegrityScanError(RuntimeError):
    """A privacy-safe failure at the local scan boundary."""


class IntegrityResumeError(IntegrityScanError):
    """The checkpoint does not match the requested scan or provider view."""


class IntegrityResourceLimitError(IntegrityScanError):
    """The configured local spill-disk budget was exceeded."""


@dataclass(frozen=True, slots=True)
class IntegrityScanConfig:
    """Runtime configuration for one bounded read-only integrity scan."""

    source: IntegrityInspector = field(repr=False)
    target: IntegrityInspector = field(repr=False)
    profile: IntegrityProfile = field(repr=False)
    checkpoint_path: pathlib.Path | None = None
    page_size: int = 1_000
    max_disk_bytes: int = 1_073_741_824
    max_issues: int = 10_000
    max_findings: int = 100_000

    def __post_init__(self) -> None:
        for name, inspector in (("source", self.source), ("target", self.target)):
            if not isinstance(inspector, IntegrityInspector):
                raise TypeError(f"{name} must implement IntegrityInspector")
            try:
                _require_digest(
                    f"{name}.descriptor_digest", inspector.descriptor_digest
                )
            except (AttributeError, TypeError, ValueError):
                raise ValueError(
                    f"{name}.descriptor_digest must be a lowercase SHA-256 digest"
                ) from None
        if not isinstance(self.profile, IntegrityProfile):
            raise TypeError("profile must be IntegrityProfile")
        if self.checkpoint_path is not None and not isinstance(
            self.checkpoint_path, pathlib.Path
        ):
            raise TypeError("checkpoint_path must be pathlib.Path or None")
        if (
            not isinstance(self.page_size, int)
            or isinstance(self.page_size, bool)
            or not 1 <= self.page_size <= 10_000
        ):
            raise ValueError("page_size must be between 1 and 10000")
        if (
            not isinstance(self.max_disk_bytes, int)
            or isinstance(self.max_disk_bytes, bool)
            or self.max_disk_bytes < 1_048_576
        ):
            raise ValueError("max_disk_bytes must be at least 1 MiB")
        for name, value in (
            ("max_issues", self.max_issues),
            ("max_findings", self.max_findings),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be a positive integer")


class _ItemEvidence:
    """Constant-memory evidence for an ordered set of item digests."""

    __slots__ = ("_digest", "_references", "count")

    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self._references: list[str] = []
        self.count = 0

    def add(self, item_digest: str) -> None:
        if self.count:
            self._digest.update(b"\x00")
        self._digest.update(item_digest.encode("ascii"))
        if len(self._references) < _MAX_TARGET_REFERENCES:
            self._references.append(item_digest)
        self.count += 1

    @property
    def aggregate_digest(self) -> str:
        return self._digest.hexdigest()

    @property
    def references(self) -> tuple[str, ...]:
        return tuple(self._references)


class _CheckpointStore:
    def __init__(self, path: pathlib.Path, *, scan_digest: str) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and not path.is_file():
            raise IntegrityResumeError("integrity checkpoint is not a regular file")
        if not path.exists():
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(descriptor)
        elif path.stat().st_mode & 0o077:
            raise IntegrityResumeError(
                "integrity checkpoint permissions must deny group and other access"
            )
        database: sqlite3.Connection | None = None
        try:
            database = sqlite3.connect(path, timeout=30, check_same_thread=False)
            self._db = database
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.execute("PRAGMA foreign_keys=ON")
            self._create_schema()
            previous = self.get_metadata("scan_digest")
            if previous is None:
                self.set_metadata("scan_digest", scan_digest)
            elif previous != scan_digest:
                raise IntegrityResumeError(
                    "integrity checkpoint belongs to a different scan configuration"
                )
        except sqlite3.DatabaseError:
            if database is not None:
                database.close()
            raise IntegrityResumeError("integrity checkpoint is invalid") from None
        except BaseException:
            if database is not None:
                database.close()
            raise

    def _create_schema(self) -> None:
        with self._db:
            self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS facts (
                    side TEXT NOT NULL CHECK (side IN ('source', 'target')),
                    identity_digest TEXT NOT NULL,
                    item_digest TEXT NOT NULL,
                    part_digest TEXT,
                    revision_digest TEXT,
                    content_digest TEXT,
                    PRIMARY KEY (side, item_digest)
                );
                CREATE INDEX IF NOT EXISTS facts_side_identity
                    ON facts(side, identity_digest);
                CREATE INDEX IF NOT EXISTS facts_target_part
                    ON facts(side, identity_digest, part_digest);
                CREATE TABLE IF NOT EXISTS issues (
                    side TEXT NOT NULL CHECK (side IN ('source', 'target')),
                    code TEXT NOT NULL,
                    evidence_digest TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (side, code, evidence_digest)
                );
                CREATE TABLE IF NOT EXISTS cursors (
                    side TEXT NOT NULL CHECK (side IN ('source', 'target')),
                    cursor_digest TEXT NOT NULL,
                    PRIMARY KEY (side, cursor_digest)
                );
                """
            )

    def close(self) -> None:
        self._db.close()

    def get_metadata(self, key: str) -> str | None:
        row = self._db.execute(
            "SELECT value FROM metadata WHERE key = ?", (key,)
        ).fetchone()
        return cast(str, row[0]) if row is not None else None

    def set_metadata(self, key: str, value: str) -> None:
        with self._db:
            self._db.execute(
                """
                INSERT INTO metadata(key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def side_done(self, side: _Side) -> bool:
        return self.get_metadata(f"{side}_done") == "1"

    def side_cursor(self, side: _Side) -> str | None:
        return self.get_metadata(f"{side}_cursor")

    def snapshot(self, side: _Side) -> SnapshotDescriptor | None:
        raw = self.get_metadata(f"{side}_snapshot")
        if raw is None:
            return None
        try:
            value = json.loads(raw)
            return SnapshotDescriptor(
                token_digest=value["token_digest"],
                consistency=SnapshotConsistency(value["consistency"]),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            raise IntegrityResumeError(
                "integrity checkpoint contains an invalid snapshot"
            ) from None

    def ingest_page(self, side: _Side, page: InspectionPage) -> None:
        previous_snapshot = self.snapshot(side)
        if previous_snapshot is not None and previous_snapshot != page.snapshot:
            raise IntegrityResumeError(
                f"{side} provider snapshot changed while resuming the scan"
            )
        next_cursor = page.next_cursor
        next_cursor_digest = (
            hashlib.sha256(next_cursor.encode("utf-8")).hexdigest()
            if next_cursor is not None
            else None
        )
        cursor_repeated = False
        with self._db:
            if previous_snapshot is None:
                self._db.execute(
                    "INSERT INTO metadata(key, value) VALUES (?, ?)",
                    (
                        f"{side}_snapshot",
                        json.dumps(
                            {
                                "token_digest": page.snapshot.token_digest,
                                "consistency": page.snapshot.consistency.value,
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )
            before = self._db.total_changes
            self._db.executemany(
                """
                INSERT OR IGNORE INTO facts(
                    side, identity_digest, item_digest, part_digest,
                    revision_digest, content_digest
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        side,
                        fact.identity_digest,
                        fact.item_digest,
                        fact.part_digest,
                        fact.revision_digest,
                        fact.content_digest,
                    )
                    for fact in page.facts
                ],
            )
            inserted = self._db.total_changes - before
            if inserted != len(page.facts):
                self._db.execute(
                    "INSERT OR IGNORE INTO issues(side, code) VALUES (?, ?)",
                    (side, InspectionIssueCode.INCONSISTENT_PAGINATION.value),
                )
            self._db.executemany(
                """
                INSERT OR IGNORE INTO issues(side, code, evidence_digest)
                VALUES (?, ?, ?)
                """,
                [
                    (side, issue.code.value, issue.evidence_digest or "")
                    for issue in page.issues
                ],
            )
            if next_cursor_digest is not None:
                result = self._db.execute(
                    """
                    INSERT OR IGNORE INTO cursors(side, cursor_digest)
                    VALUES (?, ?)
                    """,
                    (side, next_cursor_digest),
                )
                cursor_repeated = result.rowcount == 0
            if cursor_repeated:
                self._db.execute(
                    "INSERT OR IGNORE INTO issues(side, code) VALUES (?, ?)",
                    (side, InspectionIssueCode.INCONSISTENT_PAGINATION.value),
                )
                next_cursor = None
            self._db.execute(
                """
                INSERT INTO metadata(key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (f"{side}_cursor", next_cursor or ""),
            )
            if next_cursor is None:
                self._db.execute(
                    """
                    INSERT INTO metadata(key, value) VALUES (?, '1')
                    ON CONFLICT(key) DO UPDATE SET value = '1'
                    """,
                    (f"{side}_done",),
                )

    def disk_bytes(self) -> int:
        total = 0
        for suffix in ("", "-wal", "-shm"):
            path = pathlib.Path(f"{self.path}{suffix}")
            if path.exists():
                total += path.stat().st_size
        return total

    def build_report(
        self,
        profile: IntegrityProfile,
        *,
        max_issues: int,
        max_findings: int,
    ) -> IntegrityReport:
        source_snapshot = self.snapshot("source")
        target_snapshot = self.snapshot("target")
        if source_snapshot is None or target_snapshot is None:
            raise IntegrityScanError("integrity scan did not establish both snapshots")

        raw_issues = self._db.execute(
            """
            SELECT side, code, NULLIF(evidence_digest, '')
            FROM issues
            ORDER BY side, code, evidence_digest
            LIMIT ?
            """,
            (max_issues + 1,),
        ).fetchall()
        issues_truncated = len(raw_issues) > max_issues
        raw_issues = raw_issues[:max_issues]
        issues = tuple(
            InspectionIssue(
                code=InspectionIssueCode(code),
                evidence_digest=(
                    profile.export_digest("issue", f"{side}:{evidence}")
                    if evidence is not None
                    else None
                ),
            )
            for side, code, evidence in raw_issues
        )
        confidence = (
            FindingConfidence.UNKNOWN
            if issues
            else (
                FindingConfidence.PROVEN
                if source_snapshot.consistency is SnapshotConsistency.CONSISTENT
                and target_snapshot.consistency is SnapshotConsistency.CONSISTENT
                else FindingConfidence.HEURISTIC
            )
        )

        findings: list[IntegrityFinding] = []
        finding_counts = {kind: 0 for kind in FindingType}

        def _add_finding(
            kind: FindingType,
            *,
            source_identity: str | None,
            target_evidence: _ItemEvidence,
            detail_code: str,
        ) -> None:
            finding_counts[kind] += 1
            exported_source = (
                profile.export_digest("source", source_identity)
                if source_identity is not None
                else None
            )
            exported_targets = sorted(
                {
                    profile.export_digest("target", item)
                    for item in target_evidence.references
                }
            )
            canonical = json.dumps(
                {
                    "kind": kind.value,
                    "source": source_identity,
                    "target_aggregate": target_evidence.aggregate_digest,
                    "target_count": target_evidence.count,
                    "detail": detail_code,
                    "profile": profile.digest(),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            if len(findings) < max_findings:
                findings.append(
                    IntegrityFinding(
                        finding_id=f"int1_{profile.export_digest('finding', canonical)}",
                        kind=kind,
                        confidence=confidence,
                        source_digest=exported_source,
                        target_digests=tuple(exported_targets),
                        target_count=target_evidence.count,
                        evidence_digest=profile.export_digest("evidence", canonical),
                        detail_code=detail_code,
                    )
                )

        empty_evidence = _ItemEvidence()

        duplicate_source_cursor = self._db.execute(
            """
            WITH duplicate_sources AS (
                SELECT identity_digest
                FROM facts
                WHERE side = 'source'
                GROUP BY identity_digest
                HAVING COUNT(*) > 1
            )
            SELECT facts.identity_digest, facts.item_digest
            FROM facts
            JOIN duplicate_sources USING (identity_digest)
            WHERE facts.side = 'source'
            ORDER BY facts.identity_digest, facts.item_digest
            """
        )
        for raw_identity, rows in itertools.groupby(
            duplicate_source_cursor, key=lambda row: row[0]
        ):
            source_identity = cast(str, raw_identity)
            evidence = _ItemEvidence()
            for row in rows:
                evidence.add(cast(str, row[1]))
            _add_finding(
                FindingType.DUPLICATE,
                source_identity=source_identity,
                target_evidence=evidence,
                detail_code="duplicate_source_identity",
            )
            _add_finding(
                FindingType.AMBIGUOUS,
                source_identity=source_identity,
                target_evidence=empty_evidence,
                detail_code="source_mapping_ambiguous",
            )

        duplicate_target_cursor = self._db.execute(
            """
            WITH duplicate_parts AS (
                SELECT identity_digest, part_digest
                FROM facts
                WHERE side = 'target' AND part_digest IS NOT NULL
                GROUP BY identity_digest, part_digest
                HAVING COUNT(*) > 1
            )
            SELECT facts.identity_digest, facts.part_digest, facts.item_digest
            FROM facts
            JOIN duplicate_parts USING (identity_digest, part_digest)
            WHERE facts.side = 'target'
            ORDER BY facts.identity_digest, facts.part_digest, facts.item_digest
            """
        )
        for raw_key, rows in itertools.groupby(
            duplicate_target_cursor, key=lambda row: (row[0], row[1])
        ):
            source_identity = cast(str, raw_key[0])
            evidence = _ItemEvidence()
            for row in rows:
                evidence.add(cast(str, row[2]))
            _add_finding(
                FindingType.DUPLICATE,
                source_identity=source_identity,
                target_evidence=evidence,
                detail_code="duplicate_target_part",
            )

        target_cursor = self._db.execute(
            """
            WITH source_info AS (
                SELECT identity_digest, COUNT(*) AS source_count,
                       MIN(revision_digest) AS source_revision,
                       MIN(content_digest) AS source_content
                FROM facts
                WHERE side = 'source'
                GROUP BY identity_digest
            )
            SELECT target.identity_digest, target.item_digest,
                   target.revision_digest, target.content_digest,
                   source_info.source_count, source_info.source_revision,
                   source_info.source_content
            FROM facts AS target
            LEFT JOIN source_info USING (identity_digest)
            WHERE target.side = 'target'
            ORDER BY target.identity_digest, target.item_digest
            """
        )
        for raw_identity, grouped_rows in itertools.groupby(
            target_cursor, key=lambda row: row[0]
        ):
            source_identity = cast(str, raw_identity)
            target_evidence = _ItemEvidence()
            stale_revision_evidence = _ItemEvidence()
            stale_content_evidence = _ItemEvidence()
            revision_missing = False
            content_missing = False
            source_count: int | None = None
            source_revision: str | None = None
            source_content: str | None = None
            for row in grouped_rows:
                item_digest = cast(str, row[1])
                target_revision = cast(str | None, row[2])
                target_content = cast(str | None, row[3])
                source_count = cast(int | None, row[4])
                source_revision = cast(str | None, row[5])
                source_content = cast(str | None, row[6])
                target_evidence.add(item_digest)
                if target_revision is None:
                    revision_missing = True
                elif source_revision is not None and target_revision != source_revision:
                    stale_revision_evidence.add(item_digest)
                if target_content is None:
                    content_missing = True
                elif source_content is not None and target_content != source_content:
                    stale_content_evidence.add(item_digest)

            if source_count is None:
                _add_finding(
                    FindingType.ORPHAN,
                    source_identity=None,
                    target_evidence=target_evidence,
                    detail_code="source_identity_absent",
                )
                continue
            if target_evidence.count < profile.minimum_targets:
                _add_finding(
                    FindingType.MISSING,
                    source_identity=source_identity,
                    target_evidence=target_evidence,
                    detail_code="target_cardinality_below_minimum",
                )
                continue
            if source_count != 1:
                continue

            if profile.compare_revisions:
                if source_revision is None or revision_missing:
                    _add_finding(
                        FindingType.AMBIGUOUS,
                        source_identity=source_identity,
                        target_evidence=target_evidence,
                        detail_code="revision_unavailable",
                    )
                elif stale_revision_evidence.count:
                    _add_finding(
                        FindingType.STALE,
                        source_identity=source_identity,
                        target_evidence=stale_revision_evidence,
                        detail_code="revision_mismatch",
                    )
            if profile.compare_content:
                if source_content is None or content_missing:
                    _add_finding(
                        FindingType.AMBIGUOUS,
                        source_identity=source_identity,
                        target_evidence=target_evidence,
                        detail_code="content_fingerprint_unavailable",
                    )
                elif stale_content_evidence.count:
                    _add_finding(
                        FindingType.STALE,
                        source_identity=source_identity,
                        target_evidence=stale_content_evidence,
                        detail_code="content_fingerprint_mismatch",
                    )

        missing_source_cursor = self._db.execute(
            """
            SELECT source.identity_digest
            FROM facts AS source
            WHERE source.side = 'source'
              AND NOT EXISTS (
                  SELECT 1 FROM facts AS target
                  WHERE target.side = 'target'
                    AND target.identity_digest = source.identity_digest
              )
            GROUP BY source.identity_digest
            ORDER BY source.identity_digest
            """
        )
        for (raw_identity,) in missing_source_cursor:
            _add_finding(
                FindingType.MISSING,
                source_identity=cast(str, raw_identity),
                target_evidence=empty_evidence,
                detail_code="target_cardinality_below_minimum",
            )

        source_fact_count, target_fact_count = (
            cast(int, value)
            for value in self._db.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN side = 'source' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN side = 'target' THEN 1 ELSE 0 END), 0)
                FROM facts
                """
            ).fetchone()
        )
        healthy_sources = cast(
            int,
            self._db.execute(
                """
                WITH source_stats AS (
                    SELECT identity_digest, COUNT(*) AS source_count,
                           MIN(revision_digest) AS revision_digest,
                           MIN(content_digest) AS content_digest
                    FROM facts
                    WHERE side = 'source'
                    GROUP BY identity_digest
                ),
                target_stats AS (
                    SELECT identity_digest, COUNT(*) AS target_count,
                           SUM(revision_digest IS NULL) AS missing_revisions,
                           MIN(revision_digest) AS min_revision,
                           MAX(revision_digest) AS max_revision,
                           SUM(content_digest IS NULL) AS missing_content,
                           MIN(content_digest) AS min_content,
                           MAX(content_digest) AS max_content
                    FROM facts
                    WHERE side = 'target'
                    GROUP BY identity_digest
                ),
                duplicate_parts AS (
                    SELECT DISTINCT identity_digest
                    FROM facts
                    WHERE side = 'target' AND part_digest IS NOT NULL
                    GROUP BY identity_digest, part_digest
                    HAVING COUNT(*) > 1
                )
                SELECT COUNT(*)
                FROM source_stats
                JOIN target_stats USING (identity_digest)
                LEFT JOIN duplicate_parts USING (identity_digest)
                WHERE source_count = 1
                  AND target_count >= ?
                  AND duplicate_parts.identity_digest IS NULL
                  AND (
                      ? = 0 OR (
                          source_stats.revision_digest IS NOT NULL
                          AND missing_revisions = 0
                          AND min_revision = source_stats.revision_digest
                          AND max_revision = source_stats.revision_digest
                      )
                  )
                  AND (
                      ? = 0 OR (
                          source_stats.content_digest IS NOT NULL
                          AND missing_content = 0
                          AND min_content = source_stats.content_digest
                          AND max_content = source_stats.content_digest
                      )
                  )
                """,
                (
                    profile.minimum_targets,
                    int(profile.compare_revisions),
                    int(profile.compare_content),
                ),
            ).fetchone()[0],
        )
        findings.sort(key=lambda item: item.finding_id)
        status = ScanStatus.INCOMPLETE if issues else ScanStatus.COMPLETE
        return IntegrityReport(
            profile_name=profile.name,
            profile_version=profile.version,
            profile_digest=profile.digest(),
            coverage=ScanCoverage(
                status=status,
                source_snapshot=SnapshotDescriptor(
                    token_digest=profile.export_digest(
                        "source_snapshot", source_snapshot.token_digest
                    ),
                    consistency=source_snapshot.consistency,
                ),
                target_snapshot=SnapshotDescriptor(
                    token_digest=profile.export_digest(
                        "target_snapshot", target_snapshot.token_digest
                    ),
                    consistency=target_snapshot.consistency,
                ),
                issues=issues,
                issues_truncated=issues_truncated,
                findings_truncated=sum(finding_counts.values()) > len(findings),
            ),
            summary=ScanSummary(
                source_facts=source_fact_count,
                target_facts=target_fact_count,
                healthy_sources=healthy_sources,
                missing=finding_counts[FindingType.MISSING],
                orphan=finding_counts[FindingType.ORPHAN],
                stale=finding_counts[FindingType.STALE],
                duplicate=finding_counts[FindingType.DUPLICATE],
                ambiguous=finding_counts[FindingType.AMBIGUOUS],
            ),
            findings=tuple(findings),
        )


def _scan_digest(config: IntegrityScanConfig) -> str:
    payload = {
        "schema_version": 1,
        "profile": config.profile.digest(),
        "source": config.source.descriptor_digest,
        "target": config.target.descriptor_digest,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


async def _run_blocking(operation: Callable[[], _T]) -> _T:
    """Finish a SQLite operation before propagating cancellation.

    ``asyncio.to_thread`` cannot stop an active call. Shielding and joining the
    worker prevents the connection from being closed concurrently in
    ``scan()``'s cleanup path.
    """

    task = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        waiter = asyncio.create_task(asyncio.wait((task,)))
        while not waiter.done():
            try:
                await asyncio.shield(waiter)
            except asyncio.CancelledError:
                continue
        if not task.cancelled():
            task.exception()
        raise


async def _scan_side(
    store: _CheckpointStore,
    side: _Side,
    inspector: IntegrityInspector,
    config: IntegrityScanConfig,
) -> None:
    if store.side_done(side):
        return
    cursor = store.side_cursor(side)
    if cursor == "":
        cursor = None
    while True:
        try:
            page = await inspector.inspect_page(cursor, limit=config.page_size)
        except asyncio.CancelledError:
            raise
        except IntegrityScanError:
            raise
        except Exception:  # noqa: BLE001 - sanitize the external inspector boundary
            raise IntegrityScanError(f"{side} inspector failed") from None
        if not isinstance(page, InspectionPage):
            raise IntegrityScanError(f"{side} inspector returned an invalid page")
        if len(page.facts) > config.page_size:
            raise IntegrityScanError(f"{side} inspector exceeded the page limit")
        await _run_blocking(partial(store.ingest_page, side, page))
        disk_bytes = await _run_blocking(store.disk_bytes)
        if disk_bytes > config.max_disk_bytes:
            raise IntegrityResourceLimitError(
                "integrity scan exceeded the configured spill-disk budget"
            )
        if page.next_cursor is None or store.side_done(side):
            return
        cursor = page.next_cursor


def _delete_checkpoint_files(path: pathlib.Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        pathlib.Path(f"{path}{suffix}").unlink(missing_ok=True)


async def scan(config: IntegrityScanConfig) -> IntegrityReport:
    """Run or resume a bounded, read-only source-to-target integrity scan.

    A supplied checkpoint survives cancellation or failure. It is removed only
    after a complete report has been constructed. Unexpected provider
    exceptions deliberately propagate without their values being copied into a
    report; callers should sanitize provider errors at their own boundary.
    """

    if not isinstance(config, IntegrityScanConfig):
        raise TypeError("config must be IntegrityScanConfig")
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if config.checkpoint_path is None:
        temporary = tempfile.TemporaryDirectory(prefix="synor-integrity-")
        checkpoint_path = pathlib.Path(temporary.name) / "scan.sqlite3"
    else:
        checkpoint_path = config.checkpoint_path.resolve()

    store: _CheckpointStore | None = None
    succeeded = False
    try:
        store = await _run_blocking(
            lambda: _CheckpointStore(
                checkpoint_path,
                scan_digest=_scan_digest(config),
            )
        )
        await _scan_side(store, "source", config.source, config)
        await _scan_side(store, "target", config.target, config)
        report = await _run_blocking(
            lambda: store.build_report(
                config.profile,
                max_issues=config.max_issues,
                max_findings=config.max_findings,
            )
        )
        succeeded = True
        return report
    finally:
        if store is not None:
            await _run_blocking(store.close)
        if succeeded and config.checkpoint_path is not None:
            await _run_blocking(lambda: _delete_checkpoint_files(checkpoint_path))
        if temporary is not None:
            temporary.cleanup()
