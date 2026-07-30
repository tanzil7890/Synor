"""Google Drive change-feed parsing and ambiguity classification."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Literal, cast

from synor._internal.revocation_model import SourceEventKind

_CHANGE_FILE_FIELDS = (
    "id,name,parents,driveId,mimeType,size,modifiedTime,version,md5Checksum,"
    "trashed,inheritedPermissionsDisabled,hasAugmentedPermissions,"
    "capabilities(canDownload,canListChildren,canReadDrive,canShare)"
)
CHANGE_LIST_FIELDS = (
    "nextPageToken,newStartPageToken,"
    "changes(fileId,driveId,removed,time,changeType,"
    "file(" + _CHANGE_FILE_FIELDS + "),drive(id,name))"
)


class DriveFullSnapshotRequired(RuntimeError):
    """The stored change cursor is unusable and a full scan is required."""


@dataclass(frozen=True, slots=True)
class DriveCorpusCursor:
    corpus_id: str
    page_token: str


@dataclass(frozen=True, slots=True)
class DriveChange:
    change_type: Literal["file", "drive"]
    file_id: str | None
    drive_id: str | None
    removed: bool
    file: Mapping[str, object] | None
    drive: Mapping[str, object] | None
    change_time: str | None
    corpus_id: str
    ordinal: int

    @property
    def version(self) -> int | None:
        if self.file is None:
            return None
        value = self.file.get("version")
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return None
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        return None

    @property
    def is_file_change(self) -> bool:
        return self.change_type == "file"

    @property
    def is_drive_change(self) -> bool:
        return self.change_type == "drive"


@dataclass(frozen=True, slots=True)
class DriveChangePage:
    changes: tuple[DriveChange, ...]
    next_page_token: str | None
    new_start_page_token: str | None


def parse_change_page(
    response: Mapping[str, object],
    *,
    corpus_id: str,
    ordinal_start: int,
) -> DriveChangePage:
    raw_changes = response.get("changes", [])
    if not isinstance(raw_changes, list):
        raise ValueError("Drive changes response is malformed")
    changes: list[DriveChange] = []
    for offset, raw_change in enumerate(raw_changes):
        if not isinstance(raw_change, Mapping):
            raise ValueError("Drive change entry is malformed")
        raw_change_type = raw_change.get("changeType")
        file_id = raw_change.get("fileId")
        drive_id = raw_change.get("driveId")
        if raw_change_type is None:
            # Older fixtures and responses produced without an explicit field
            # mask can omit changeType. Infer only when the resource identity
            # makes the result unambiguous.
            if isinstance(file_id, str) and file_id:
                change_type: Literal["file", "drive"] = "file"
            elif isinstance(drive_id, str) and drive_id:
                change_type = "drive"
            else:
                raise ValueError("Drive change type is malformed")
        elif raw_change_type in {"file", "drive"}:
            change_type = cast(Literal["file", "drive"], raw_change_type)
        else:
            raise ValueError("Drive change type is malformed")
        if change_type == "file":
            if not isinstance(file_id, str) or not file_id:
                raise ValueError("Drive change file ID is malformed")
        elif file_id is not None:
            if not isinstance(file_id, str) or not file_id:
                raise ValueError("Drive change file ID is malformed")
        if drive_id is not None and (not isinstance(drive_id, str) or not drive_id):
            raise ValueError("Drive change shared-drive ID is malformed")
        if change_type == "drive" and not isinstance(drive_id, str):
            raise ValueError("Drive change shared-drive ID is malformed")
        raw_file = raw_change.get("file")
        if raw_file is not None and not isinstance(raw_file, Mapping):
            raise ValueError("Drive change file resource is malformed")
        if change_type == "drive" and raw_file is not None:
            raise ValueError("Drive-level change cannot contain a file resource")
        raw_drive = raw_change.get("drive")
        if raw_drive is not None and not isinstance(raw_drive, Mapping):
            raise ValueError("Drive change shared-drive resource is malformed")
        if change_type == "file" and raw_drive is not None:
            raise ValueError("File-level change cannot contain a shared-drive resource")
        change_time = raw_change.get("time")
        changes.append(
            DriveChange(
                change_type=change_type,
                file_id=file_id if isinstance(file_id, str) else None,
                drive_id=drive_id if isinstance(drive_id, str) else None,
                removed=raw_change.get("removed") is True,
                file=raw_file,
                drive=raw_drive,
                change_time=change_time if isinstance(change_time, str) else None,
                corpus_id=corpus_id,
                ordinal=ordinal_start + offset,
            )
        )
    next_page = response.get("nextPageToken")
    new_start = response.get("newStartPageToken")
    if next_page is not None and (not isinstance(next_page, str) or not next_page):
        raise ValueError("Drive next change token is malformed")
    if new_start is not None and (not isinstance(new_start, str) or not new_start):
        raise ValueError("Drive new start token is malformed")
    if next_page is not None and new_start is not None:
        raise ValueError("Drive change page cannot contain both cursor fields")
    return DriveChangePage(
        changes=tuple(changes),
        next_page_token=next_page,
        new_start_page_token=new_start,
    )


def coalesce_changes(changes: Sequence[DriveChange]) -> tuple[DriveChange, ...]:
    """Coalesce file state without discarding cross-corpus move evidence.

    A user log and a shared-drive log can report the same move differently:
    one contains accessible current file state while the other contains a
    tombstone. Provider polling order must never let the tombstone erase the
    accessible state. Drive-level changes are preserved independently.
    """

    file_changes: dict[str, list[DriveChange]] = {}
    drive_changes: dict[str, DriveChange] = {}
    for change in changes:
        if change.is_drive_change:
            assert change.drive_id is not None
            previous_drive = drive_changes.get(change.drive_id)
            if previous_drive is None or change.ordinal > previous_drive.ordinal:
                drive_changes[change.drive_id] = change
            continue
        assert change.file_id is not None
        file_changes.setdefault(change.file_id, []).append(change)

    coalesced: list[DriveChange] = list(drive_changes.values())
    for candidates in file_changes.values():
        # Change order is authoritative inside one corpus. Collapse each log
        # independently first so a later tombstone cannot be displaced by an
        # earlier live record from that same log.
        latest_by_corpus: dict[str, DriveChange] = {}
        for candidate in candidates:
            previous = latest_by_corpus.get(candidate.corpus_id)
            if previous is None or candidate.ordinal > previous.ordinal:
                latest_by_corpus[candidate.corpus_id] = candidate

        current_candidates = tuple(latest_by_corpus.values())
        live = [
            candidate for candidate in current_candidates if candidate.file is not None
        ]
        if not live:
            coalesced.append(
                max(current_candidates, key=lambda candidate: candidate.ordinal)
            )
            continue

        # Prefer the highest provider version when available. For equal or
        # absent versions, the latest accessible state is the best evidence
        # across corpus logs, whose polling order is not a provider chronology.
        current = max(
            live,
            key=lambda candidate: (
                candidate.version is not None,
                candidate.version if candidate.version is not None else -1,
                candidate.ordinal,
            ),
        )
        cross_corpus_removal = any(
            candidate.removed and candidate.corpus_id != current.corpus_id
            for candidate in current_candidates
        )
        coalesced.append(
            replace(
                current,
                removed=current.removed or cross_corpus_removal,
            )
        )
    return tuple(sorted(coalesced, key=lambda change: change.ordinal))


def classify_removed_change(
    change: DriveChange,
    *,
    previous_corpus_id: str | None,
    current_corpus_id: str | None,
    still_in_scope: bool,
) -> SourceEventKind:
    if not change.is_file_change or not change.removed:
        raise ValueError("removed-change classifier requires removed=true")
    if (
        change.file is not None
        and still_in_scope
        and previous_corpus_id is not None
        and current_corpus_id is not None
        and previous_corpus_id != current_corpus_id
    ):
        return SourceEventKind.MOVED_SCOPE
    return SourceEventKind.AMBIGUOUS_REMOVAL


def is_rejected_cursor_error(error: BaseException) -> bool:
    if getattr(error, "status_code", None) in {400, 410}:
        return True
    response = getattr(error, "resp", None)
    return getattr(response, "status", None) in {400, 410}


__all__ = [
    "CHANGE_LIST_FIELDS",
    "DriveChange",
    "DriveChangePage",
    "DriveCorpusCursor",
    "DriveFullSnapshotRequired",
    "classify_removed_change",
    "coalesce_changes",
    "is_rejected_cursor_error",
    "parse_change_page",
]
