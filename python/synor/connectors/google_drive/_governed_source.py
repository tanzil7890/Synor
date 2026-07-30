"""Governed Google Drive observations, snapshots, and change checkpoints.

This module is intentionally connector-local while the controlled public
revocation runtime remains internal.  It produces the Phase 0 value contracts
without fabricating target obligations or calling the revocation coordinator.
"""

from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import PurePath
from typing import Any, Literal, cast

from synor import state
from synor._internal.revocation_model import (
    AccessSnapshot,
    GovernedSourceItem,
    SnapshotResult,
    SourceEventKind,
    SourceIdentity,
    make_observation_id,
)
from synor._internal.state_store_lock import state_store_writer_lock
from synor._internal.typing import StableKey

from ._changes import (
    CHANGE_LIST_FIELDS,
    DriveChange,
    DriveFullSnapshotRequired,
    classify_removed_change,
    coalesce_changes,
    is_rejected_cursor_error,
    parse_change_page,
)
from ._permissions import (
    DrivePermissionGrant,
    DrivePermissionResolution,
    DrivePermissionResolver,
)
from ._source import (
    _DRIVE_SCOPE,
    _FOLDER_MIME,
    DriveFile,
    DriveFileInfo,
    _build_service,
    _parse_modified_time,
)


_SCHEMA_VERSION = 2
_STATE_PREFIX = "google-drive/governed/v1"
_MAX_RETRIES = 5
_MAX_DESCENDANT_RECOMPUTATIONS = 500
_FILE_FIELDS = (
    "id,name,parents,driveId,mimeType,size,modifiedTime,version,md5Checksum,"
    "trashed,inheritedPermissionsDisabled,hasAugmentedPermissions,"
    "capabilities(canDownload,canListChildren,canReadDrive,canShare)"
)
_LIST_FIELDS = f"nextPageToken,incompleteSearch,files({_FILE_FIELDS})"
_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


class DriveGovernanceError(RuntimeError):
    """Base error for governed source preparation or checkpointing."""


class DriveCheckpointConflict(DriveGovernanceError):
    """The durable source state changed after a batch was prepared."""


class DriveStateCorruption(DriveGovernanceError):
    """Persisted governed Drive state cannot be trusted."""


class DriveSnapshotStateError(DriveGovernanceError):
    """A staged snapshot or replay was consumed or committed unsafely."""


class DriveRequestError(DriveGovernanceError):
    """Sanitized provider failure suitable for control-plane handling."""

    def __init__(self, *, status_code: int | None, retryable: bool) -> None:
        super().__init__("Google Drive request failed")
        self.status_code = status_code
        self.retryable = retryable


class _DriveCorpusAuthorityError(DriveGovernanceError):
    """A corpus endpoint proved that its configured authority is unavailable."""

    def __init__(self, corpus_id: str, event: SourceEventKind) -> None:
        super().__init__("configured Google Drive corpus is unavailable")
        self.corpus_id = corpus_id
        self.event = event


@dataclass(frozen=True, slots=True)
class DriveRetryPolicy:
    """Bounded retry policy for transient Drive responses."""

    max_attempts: int = _MAX_RETRIES
    initial_backoff_seconds: float = 0.1
    max_backoff_seconds: float = 2.0

    def __post_init__(self) -> None:
        if (
            not isinstance(self.max_attempts, int)
            or isinstance(self.max_attempts, bool)
            or self.max_attempts < 1
        ):
            raise ValueError("max_attempts must be a positive integer")
        if self.initial_backoff_seconds < 0 or self.max_backoff_seconds < 0:
            raise ValueError("retry backoff cannot be negative")
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError("maximum retry backoff cannot be smaller than initial")


async def _execute_request(
    request_factory: Callable[[], Any],
    retry_policy: DriveRetryPolicy,
    *,
    sleep: Sleep | None = None,
) -> Mapping[str, object]:
    """Execute with bounded retry and no provider-controlled error text."""

    sleeper = sleep or asyncio.sleep
    delay = retry_policy.initial_backoff_seconds
    for attempt in range(retry_policy.max_attempts):
        try:
            request = request_factory()
            response = await asyncio.to_thread(request.execute)
            if not isinstance(response, Mapping):
                raise DriveRequestError(status_code=None, retryable=False)
            return cast(Mapping[str, object], response)
        except asyncio.CancelledError:
            raise
        except DriveRequestError:
            raise
        except Exception as error:
            status_code = _status(error)
            retryable = status_code in _RETRYABLE_STATUSES
            if not retryable or attempt + 1 >= retry_policy.max_attempts:
                raise DriveRequestError(
                    status_code=status_code,
                    retryable=retryable,
                ) from None
            if delay:
                await sleeper(delay)
            delay = min(
                max(delay * 2, retry_policy.initial_backoff_seconds),
                retry_policy.max_backoff_seconds,
            )
    raise AssertionError("bounded Drive retry loop did not terminate")


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _utc_text(value: datetime.datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _read_time(value: object) -> datetime.datetime:
    if not isinstance(value, str):
        raise DriveStateCorruption("governed Drive state contains an invalid timestamp")
    try:
        result = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        result = None
    if result is None or result.tzinfo is None or result.utcoffset() is None:
        raise DriveStateCorruption(
            "governed Drive state contains an invalid timestamp"
        ) from None
    return result.astimezone(datetime.timezone.utc)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _digest(domain: str, value: object) -> str:
    return hashlib.sha256(
        b"synor-google-drive-"
        + domain.encode("ascii")
        + b"-v1\x00"
        + _canonical_json(value)
    ).hexdigest()


def _safe_generation(kind: str, value: object) -> str:
    return f"{kind}1_{_digest(kind, value)}"


def _status(error: BaseException) -> int | None:
    if isinstance(error, DriveRequestError):
        return error.status_code
    response = getattr(error, "resp", None)
    status = getattr(response, "status", None)
    return status if isinstance(status, int) else None


def _terminal_authority_event(error: BaseException) -> SourceEventKind | None:
    status = _status(error)
    if status in {401, 403}:
        return SourceEventKind.ACCESS_LOST
    if status == 404:
        # Drive intentionally conflates "not found" with "not readable."
        # Do not assert deletion or access loss when either is possible.
        return SourceEventKind.AMBIGUOUS_REMOVAL
    return None


def _stronger_event(
    previous: SourceEventKind | None,
    current: SourceEventKind,
) -> SourceEventKind:
    if previous is SourceEventKind.ACCESS_LOST:
        return previous
    if current is SourceEventKind.ACCESS_LOST:
        return current
    return previous or current


def _authority_digest(kind: str, authority_id: str) -> str:
    return _digest("inaccessible-authority", (kind, authority_id))


def _cursor_digest(cursors: Mapping[str, str]) -> str | None:
    if not cursors:
        return None
    return f"drivecursor1_{_digest('cursor-set', sorted(cursors.items()))}"


def _content_revision(info: DriveFileInfo) -> str:
    return "drivecontent1_" + _digest(
        "content-revision",
        (
            info.mime_type,
            _utc_text(info.modified_time),
            info.size,
            info.md5_checksum or "",
            info.version or "",
        ),
    )


def _content_fingerprint(info: DriveFileInfo) -> bytes | None:
    if info.content_fingerprint is not None:
        return info.content_fingerprint
    if info.md5_checksum is None:
        return None
    try:
        return bytes.fromhex(info.md5_checksum)
    except ValueError:
        return None


def _corpus_id(info: DriveFileInfo) -> str:
    return f"drive:{info.drive_id}" if info.drive_id else "user"


def _file_info(raw: Mapping[str, object]) -> DriveFileInfo:
    file_id = raw.get("id")
    name = raw.get("name")
    mime_type = raw.get("mimeType")
    if not isinstance(file_id, str) or not file_id:
        raise ValueError("Drive file ID is malformed")
    if not isinstance(name, str) or not name:
        raise ValueError("Drive file name is malformed")
    if not isinstance(mime_type, str) or not mime_type:
        raise ValueError("Drive MIME type is malformed")
    parents_raw = raw.get("parents", [])
    if not isinstance(parents_raw, list) or not all(
        isinstance(parent, str) and parent for parent in parents_raw
    ):
        raise ValueError("Drive parent list is malformed")
    size_raw = raw.get("size")
    try:
        if isinstance(size_raw, (str, int)) and not isinstance(size_raw, bool):
            size = int(size_raw)
        elif size_raw is None:
            size = 0
        else:
            raise ValueError
    except (TypeError, ValueError):
        raise ValueError("Drive file size is malformed") from None
    if size < 0:
        raise ValueError("Drive file size is malformed")
    version_raw = raw.get("version")
    if version_raw is not None and not isinstance(version_raw, (str, int)):
        raise ValueError("Drive file version is malformed")
    drive_id = raw.get("driveId")
    md5_checksum = raw.get("md5Checksum")
    if drive_id is not None and not isinstance(drive_id, str):
        raise ValueError("Drive corpus ID is malformed")
    if md5_checksum is not None and not isinstance(md5_checksum, str):
        raise ValueError("Drive content checksum is malformed")
    capabilities_raw = raw.get("capabilities", {})
    if not isinstance(capabilities_raw, Mapping):
        raise ValueError("Drive capabilities are malformed")
    capabilities = tuple(
        sorted(
            (key, value)
            for key, value in capabilities_raw.items()
            if isinstance(key, str) and isinstance(value, bool)
        )
    )
    content_fingerprint: bytes | None = None
    if isinstance(md5_checksum, str):
        try:
            content_fingerprint = bytes.fromhex(md5_checksum)
        except ValueError:
            content_fingerprint = None
    modified_raw = raw.get("modifiedTime")
    modified_time = modified_raw if isinstance(modified_raw, str) else None
    return DriveFileInfo(
        file_id=file_id,
        name=name,
        mime_type=mime_type,
        size=size,
        modified_time=_parse_modified_time(modified_time),
        content_fingerprint=content_fingerprint,
        parents=tuple(cast(list[str], parents_raw)),
        drive_id=drive_id,
        version=str(version_raw) if version_raw is not None else None,
        md5_checksum=md5_checksum,
        trashed=raw.get("trashed") is True,
        inherited_permissions_disabled=(
            raw.get("inheritedPermissionsDisabled") is True
        ),
        capabilities=capabilities,
    )


def _grant_to_dict(grant: DrivePermissionGrant) -> dict[str, object]:
    return {
        "permission_id": grant.permission_id,
        "subject_type": grant.subject_type,
        "role": grant.role,
        "inherited": grant.inherited,
        "inherited_from": grant.inherited_from,
        "permission_type": grant.permission_type,
        "expires_at": (
            _utc_text(grant.expires_at) if grant.expires_at is not None else None
        ),
        "allow_file_discovery": grant.allow_file_discovery,
        "deleted": grant.deleted,
        "view": grant.view,
        "inherited_permissions_disabled": grant.inherited_permissions_disabled,
    }


def _grant_from_dict(value: Mapping[str, object]) -> DrivePermissionGrant:
    try:
        permission_id = value["permission_id"]
        subject_type = value["subject_type"]
        role = value["role"]
        inherited = value["inherited"]
        deleted = value["deleted"]
        inherited_permissions_disabled = value["inherited_permissions_disabled"]
    except KeyError:
        raise DriveStateCorruption(
            "governed Drive state contains an invalid permission"
        ) from None
    if (
        not isinstance(permission_id, str)
        or not isinstance(subject_type, str)
        or not isinstance(role, str)
        or not isinstance(inherited, bool)
        or not isinstance(deleted, bool)
        or not isinstance(inherited_permissions_disabled, bool)
    ):
        raise DriveStateCorruption(
            "governed Drive state contains an invalid permission"
        )
    inherited_from = value.get("inherited_from")
    permission_type = value.get("permission_type")
    allow_file_discovery = value.get("allow_file_discovery")
    view = value.get("view")
    expires_at = value.get("expires_at")
    for optional in (inherited_from, permission_type, view):
        if optional is not None and not isinstance(optional, str):
            raise DriveStateCorruption(
                "governed Drive state contains an invalid permission"
            )
    if allow_file_discovery is not None and not isinstance(allow_file_discovery, bool):
        raise DriveStateCorruption(
            "governed Drive state contains an invalid permission"
        )
    return DrivePermissionGrant(
        permission_id=permission_id,
        subject_type=subject_type,
        role=role,
        inherited=inherited,
        inherited_from=cast(str | None, inherited_from),
        permission_type=cast(str | None, permission_type),
        expires_at=_read_time(expires_at) if expires_at is not None else None,
        allow_file_discovery=allow_file_discovery,
        deleted=deleted,
        view=cast(str | None, view),
        inherited_permissions_disabled=inherited_permissions_disabled,
    )


@dataclass(frozen=True, slots=True)
class _StoredNode:
    info: DriveFileInfo
    source_revision: str
    access: AccessSnapshot
    grants: tuple[DrivePermissionGrant, ...]
    policy_authority: Literal["complete", "partial"]
    limitation_codes: tuple[str, ...]
    active: bool = True
    last_event: SourceEventKind = SourceEventKind.PRESENT
    expiry_emitted_for: str | None = None

    @property
    def is_folder(self) -> bool:
        return self.info.mime_type == _FOLDER_MIME

    def to_dict(self) -> dict[str, object]:
        return {
            "file_id": self.info.file_id,
            "name": self.info.name,
            "mime_type": self.info.mime_type,
            "size": self.info.size,
            "modified_time": _utc_text(self.info.modified_time),
            "display_path": self.info.display_path,
            "parents": self.info.parents,
            "drive_id": self.info.drive_id,
            "version": self.info.version,
            "md5_checksum": self.info.md5_checksum,
            "trashed": self.info.trashed,
            "inherited_permissions_disabled": (
                self.info.inherited_permissions_disabled
            ),
            "capabilities": self.info.capabilities,
            "source_revision": self.source_revision,
            "access": {
                "tenant_id": self.access.tenant_id,
                "policy_id": self.access.policy_id,
                "policy_revision": self.access.policy_revision,
                "policy_digest": self.access.policy_digest,
                "group_graph_revision": self.access.group_graph_revision,
                "inherited_from": self.access.inherited_from,
                "valid_until": (
                    _utc_text(self.access.valid_until)
                    if self.access.valid_until is not None
                    else None
                ),
            },
            "grants": [_grant_to_dict(grant) for grant in self.grants],
            "policy_authority": self.policy_authority,
            "limitation_codes": self.limitation_codes,
            "active": self.active,
            "last_event": self.last_event.value,
            "expiry_emitted_for": self.expiry_emitted_for,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "_StoredNode":
        try:
            capabilities_raw = value["capabilities"]
            if not isinstance(capabilities_raw, list):
                raise ValueError
            capabilities: dict[str, bool] = {}
            for pair in capabilities_raw:
                if (
                    not isinstance(pair, list)
                    or len(pair) != 2
                    or not isinstance(pair[0], str)
                    or not isinstance(pair[1], bool)
                ):
                    raise ValueError
                capabilities[pair[0]] = pair[1]
            raw_info: dict[str, object] = {
                "id": value["file_id"],
                "name": value["name"],
                "mimeType": value["mime_type"],
                "size": value["size"],
                "modifiedTime": value["modified_time"],
                "parents": value["parents"],
                "driveId": value.get("drive_id"),
                "version": value.get("version"),
                "md5Checksum": value.get("md5_checksum"),
                "trashed": value["trashed"],
                "inheritedPermissionsDisabled": value["inherited_permissions_disabled"],
                "capabilities": capabilities,
            }
            info = _file_info(raw_info)
            display_path = value.get("display_path")
            if display_path is not None and not isinstance(display_path, str):
                raise ValueError
            info.display_path = display_path
            access_raw = value["access"]
            if not isinstance(access_raw, Mapping):
                raise ValueError
            valid_until = access_raw.get("valid_until")
            inherited_from = access_raw["inherited_from"]
            if not isinstance(inherited_from, list):
                inherited_from = list(cast(tuple[object, ...], inherited_from))
            access = AccessSnapshot(
                tenant_id=cast(str, access_raw["tenant_id"]),
                policy_id=cast(str, access_raw["policy_id"]),
                policy_revision=cast(str, access_raw["policy_revision"]),
                policy_digest=cast(str, access_raw["policy_digest"]),
                group_graph_revision=cast(str, access_raw["group_graph_revision"]),
                inherited_from=tuple(cast(list[str], inherited_from)),
                valid_until=(
                    _read_time(valid_until) if valid_until is not None else None
                ),
            )
            grants_raw = value["grants"]
            if not isinstance(grants_raw, list):
                raise ValueError
            grants = tuple(
                _grant_from_dict(grant)
                for grant in grants_raw
                if isinstance(grant, Mapping)
            )
            if len(grants) != len(grants_raw):
                raise ValueError
            policy_authority = value["policy_authority"]
            limitations = value["limitation_codes"]
            active = value["active"]
            last_event = value["last_event"]
            source_revision = value["source_revision"]
            expiry_emitted_for = value.get("expiry_emitted_for")
            if (
                policy_authority not in {"complete", "partial"}
                or not isinstance(limitations, (list, tuple))
                or not all(isinstance(item, str) for item in limitations)
                or not isinstance(active, bool)
                or not isinstance(last_event, str)
                or not isinstance(source_revision, str)
                or (
                    expiry_emitted_for is not None
                    and not isinstance(expiry_emitted_for, str)
                )
            ):
                raise ValueError
            return cls(
                info=info,
                source_revision=source_revision,
                access=access,
                grants=grants,
                policy_authority=cast(Literal["complete", "partial"], policy_authority),
                limitation_codes=tuple(cast(Sequence[str], limitations)),
                active=active,
                last_event=SourceEventKind(last_event),
                expiry_emitted_for=expiry_emitted_for,
            )
        except (KeyError, TypeError, ValueError):
            raise DriveStateCorruption(
                "governed Drive state contains an invalid inventory node"
            ) from None


@dataclass(frozen=True, slots=True)
class _PendingDescendants:
    cursor_after: tuple[tuple[str, str], ...]
    remaining_file_ids: tuple[str, ...]
    observation_generation: str

    def to_dict(self) -> dict[str, object]:
        return {
            "cursor_after": self.cursor_after,
            "remaining_file_ids": self.remaining_file_ids,
            "observation_generation": self.observation_generation,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "_PendingDescendants":
        cursors = value.get("cursor_after")
        remaining = value.get("remaining_file_ids")
        generation = value.get("observation_generation")
        if (
            not isinstance(cursors, (list, tuple))
            or not isinstance(remaining, (list, tuple))
            or not isinstance(generation, str)
        ):
            raise DriveStateCorruption(
                "governed Drive state contains an invalid pending replay"
            )
        cursor_pairs: list[tuple[str, str]] = []
        for pair in cursors:
            if (
                not isinstance(pair, (list, tuple))
                or len(pair) != 2
                or not all(isinstance(item, str) for item in pair)
            ):
                raise DriveStateCorruption(
                    "governed Drive state contains an invalid pending replay"
                )
            cursor_pairs.append((pair[0], pair[1]))
        if not all(isinstance(item, str) for item in remaining):
            raise DriveStateCorruption(
                "governed Drive state contains an invalid pending replay"
            )
        return cls(
            cursor_after=tuple(cursor_pairs),
            remaining_file_ids=tuple(cast(Sequence[str], remaining)),
            observation_generation=generation,
        )


@dataclass(frozen=True, slots=True)
class _StateEnvelope:
    generation: int
    cursors: tuple[tuple[str, str], ...]
    nodes: tuple[_StoredNode, ...]
    pending: _PendingDescendants | None = None
    authority_digest: str = ""
    semantic_digest: str = ""

    @property
    def cursor_map(self) -> dict[str, str]:
        return dict(self.cursors)

    @property
    def node_map(self) -> dict[str, _StoredNode]:
        return {node.info.file_id: node for node in self.nodes}

    def to_bytes(self) -> bytes:
        return _canonical_json(
            {
                "schema_version": _SCHEMA_VERSION,
                "generation": self.generation,
                "authority_digest": self.authority_digest,
                "semantic_digest": self.semantic_digest,
                "cursors": self.cursors,
                "nodes": [node.to_dict() for node in self.nodes],
                "pending": self.pending.to_dict() if self.pending is not None else None,
            }
        )

    @classmethod
    def empty(
        cls,
        authority_digest: str,
        semantic_digest: str,
    ) -> "_StateEnvelope":
        return cls(
            generation=0,
            cursors=(),
            nodes=(),
            authority_digest=authority_digest,
            semantic_digest=semantic_digest,
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> "_StateEnvelope":
        try:
            raw = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise DriveStateCorruption(
                "governed Drive state is not valid JSON"
            ) from None
        if not isinstance(raw, Mapping) or raw.get("schema_version") not in {1, 2}:
            raise DriveStateCorruption("governed Drive state has an unsupported schema")
        generation = raw.get("generation")
        authority_digest = raw.get("authority_digest")
        semantic_digest = raw.get("semantic_digest", "")
        cursors = raw.get("cursors")
        nodes = raw.get("nodes")
        if (
            not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation < 0
            or not isinstance(cursors, list)
            or not isinstance(nodes, list)
            or not isinstance(authority_digest, str)
            or len(authority_digest) != 64
            or not isinstance(semantic_digest, str)
            or (semantic_digest != "" and len(semantic_digest) != 64)
        ):
            raise DriveStateCorruption("governed Drive state is malformed")
        cursor_pairs: list[tuple[str, str]] = []
        for pair in cursors:
            if (
                not isinstance(pair, list)
                or len(pair) != 2
                or not all(isinstance(item, str) for item in pair)
            ):
                raise DriveStateCorruption("governed Drive state is malformed")
            cursor_pairs.append((pair[0], pair[1]))
        parsed_nodes = tuple(
            _StoredNode.from_dict(node) for node in nodes if isinstance(node, Mapping)
        )
        if len(parsed_nodes) != len(nodes):
            raise DriveStateCorruption("governed Drive state is malformed")
        if len({node.info.file_id for node in parsed_nodes}) != len(parsed_nodes):
            raise DriveStateCorruption(
                "governed Drive state contains duplicate file IDs"
            )
        pending_raw = raw.get("pending")
        if pending_raw is not None and not isinstance(pending_raw, Mapping):
            raise DriveStateCorruption("governed Drive state is malformed")
        return cls(
            generation=generation,
            cursors=tuple(sorted(cursor_pairs)),
            nodes=tuple(sorted(parsed_nodes, key=lambda node: node.info.file_id)),
            pending=(
                _PendingDescendants.from_dict(pending_raw)
                if isinstance(pending_raw, Mapping)
                else None
            ),
            authority_digest=authority_digest,
            semantic_digest=semantic_digest,
        )


@dataclass(frozen=True, slots=True)
class DriveGovernedObservation:
    """A provider observation plus the context needed by a later coordinator."""

    item: GovernedSourceItem[DriveFile]
    observation_generation: str
    previous_access: AccessSnapshot | None
    previous_corpus_id: str | None
    current_corpus_id: str | None
    policy_authority: Literal["complete", "partial"]
    limitation_codes: tuple[str, ...] = ()

    @property
    def key(self) -> StableKey:
        return self.item.identity.component_key()

    def evidence_summary(self) -> dict[str, object]:
        return {
            "source_digest": self.item.identity.evidence_digest(),
            "event": self.item.event.value,
            "policy_digest": (
                self.item.access.policy_digest
                if self.item.access is not None
                else (
                    self.previous_access.policy_digest
                    if self.previous_access is not None
                    else None
                )
            ),
            "policy_authority": self.policy_authority,
            "limitation_codes": self.limitation_codes,
        }


CommitBuilder = Callable[[_StateEnvelope], _StateEnvelope]


class DrivePreparedBatch:
    """A staged batch whose cursor changes are inert until :meth:`commit`.

    Callers should either use ``apply_*`` helpers or invoke ``commit`` only
    after every returned observation is durably handled downstream.
    """

    def __init__(
        self,
        *,
        source: "GovernedGoogleDriveSource",
        base_generation: int,
        kind: Literal["snapshot", "changes", "expiry"],
        observations: Sequence[DriveGovernedObservation],
        commit_builder: CommitBuilder,
        snapshot_result: SnapshotResult | None = None,
        has_more: bool = False,
        requires_full_snapshot: bool = False,
    ) -> None:
        self._source = source
        self._base_generation = base_generation
        self._commit_builder = commit_builder
        self._committed = False
        self.kind = kind
        self.observations = tuple(observations)
        self.snapshot_result = snapshot_result
        self.has_more = has_more
        self.requires_full_snapshot = requires_full_snapshot

    @property
    def committed(self) -> bool:
        return self._committed

    def items(
        self,
    ) -> tuple[tuple[StableKey, GovernedSourceItem[DriveFile]], ...]:
        return tuple(
            (observation.key, observation.item) for observation in self.observations
        )

    async def commit(self) -> None:
        if self._committed:
            return
        if (
            self.kind == "snapshot"
            and self.snapshot_result is not None
            and self.snapshot_result.status != "complete"
        ):
            raise DriveSnapshotStateError(
                "partial or failed snapshot cannot advance its checkpoint"
            )
        if self.kind == "changes" and self.requires_full_snapshot:
            raise DriveFullSnapshotRequired(
                "Drive replay cannot commit; full snapshot required"
            )
        await self._source._commit_prepared(self)
        self._committed = True


DownstreamReady = Callable[[DrivePreparedBatch], Awaitable[None]]
Sleep = Callable[[float], Awaitable[None]]


class GovernedGoogleDriveSource:
    """Stable-ID, ACL-aware Google Drive observation source.

    Compatibility :class:`GoogleDriveSource` remains unchanged.  This source
    requires explicit connector, scope, tenant, and durable state identities so
    that rename, duplicate names, partial scans, and cursor replay cannot be
    mistaken for authoritative deletion.
    """

    def __init__(
        self,
        *,
        service_account_credential_path: str,
        root_folder_ids: Sequence[str],
        state_store: state.StateStore,
        connector_instance_id: str,
        source_scope_id: str,
        tenant_id: str,
        shared_drive_ids: Sequence[str] = (),
        mime_types: Sequence[str] | None = None,
        policy_id: str = "google-drive-observed-acl-v1",
        policy_revision: str = "policy-v1",
        group_graph_revision: str = "unresolved",
        delegated_subject: str | None = None,
        retry_policy: DriveRetryPolicy = DriveRetryPolicy(),
        _service: Any | None = None,
        _sleep: Sleep = asyncio.sleep,
    ) -> None:
        if not root_folder_ids and not shared_drive_ids:
            raise ValueError(
                "governed Drive source requires at least one root or shared drive"
            )
        for name, value in (
            ("connector_instance_id", connector_instance_id),
            ("source_scope_id", source_scope_id),
            ("tenant_id", tenant_id),
            ("policy_id", policy_id),
            ("policy_revision", policy_revision),
            ("group_graph_revision", group_graph_revision),
        ):
            if not isinstance(value, str) or not value or value != value.strip():
                raise ValueError(f"{name} must be a non-empty canonical string")
        if len(set(root_folder_ids)) != len(root_folder_ids):
            raise ValueError("root_folder_ids must not contain duplicates")
        if len(set(shared_drive_ids)) != len(shared_drive_ids):
            raise ValueError("shared_drive_ids must not contain duplicates")
        if not isinstance(retry_policy, DriveRetryPolicy):
            raise TypeError("retry_policy must be a DriveRetryPolicy")
        self._credential_path = service_account_credential_path
        self._root_folder_ids = tuple(root_folder_ids)
        self._shared_drive_ids = tuple(shared_drive_ids)
        self._store = state_store
        self._connector_instance_id = connector_instance_id
        self._source_scope_id = source_scope_id
        self._tenant_id = tenant_id
        self._policy_id = policy_id
        self._policy_revision = policy_revision
        self._mime_types = frozenset(mime_types) if mime_types is not None else None
        self._group_graph_revision = group_graph_revision
        self._delegated_subject = delegated_subject
        self._service = _service
        self._sleep = _sleep
        self._retry_policy = retry_policy
        self._lock = state_store_writer_lock(state_store)
        self._authority_digest = _digest(
            "authority-config",
            {
                "connector_instance_id": connector_instance_id,
                "source_scope_id": source_scope_id,
                "tenant_id": tenant_id,
                "root_folder_ids": sorted(root_folder_ids),
                "shared_drive_ids": sorted(shared_drive_ids),
                "mime_types": sorted(mime_types) if mime_types is not None else None,
                "delegated_subject": delegated_subject,
                "oauth_scopes": [_DRIVE_SCOPE],
            },
        )
        self._semantic_digest = _digest(
            "semantic-config",
            {
                "policy_id": policy_id,
                "policy_revision": policy_revision,
                "group_graph_revision": group_graph_revision,
            },
        )
        self._state_key = (
            f"{_STATE_PREFIX}/"
            + _digest(
                "state-key",
                (connector_instance_id, source_scope_id),
            )
            + "/cursors/checkpoint.json"
        )

    def _drive_service(self) -> Any:
        if self._service is None:
            self._service = _build_service(
                self._credential_path,
                delegated_subject=self._delegated_subject,
                scopes=(_DRIVE_SCOPE,),
            )
        return self._service

    async def _execute(self, request: Any) -> Mapping[str, object]:
        return await _execute_request(
            lambda: request,
            self._retry_policy,
            sleep=self._sleep,
        )

    async def _load_state(self) -> _StateEnvelope:
        payload = await self._store.get(self._state_key)
        if payload is None:
            return _StateEnvelope.empty(
                self._authority_digest,
                self._semantic_digest,
            )
        current = _StateEnvelope.from_bytes(payload)
        if current.authority_digest != self._authority_digest:
            raise DriveCheckpointConflict(
                "governed Drive state belongs to a different authority configuration"
            )
        return current

    async def _commit_prepared(self, batch: DrivePreparedBatch) -> None:
        async with self._lock:
            current = await self._load_state()
            if current.generation != batch._base_generation:
                raise DriveCheckpointConflict(
                    "governed Drive state changed after batch preparation"
                )
            next_state = batch._commit_builder(current)
            if next_state.generation != current.generation + 1:
                raise DriveGovernanceError(
                    "prepared Drive checkpoint has an invalid generation"
                )
            await self._store.put(self._state_key, next_state.to_bytes())

    def _corpora(self) -> tuple[tuple[str, str | None], ...]:
        return (("user", None),) + tuple(
            (f"drive:{drive_id}", drive_id) for drive_id in self._shared_drive_ids
        )

    async def _start_tokens(
        self,
    ) -> tuple[
        dict[str, str],
        tuple[str, ...],
        dict[str, SourceEventKind],
    ]:
        service = self._drive_service()
        tokens: dict[str, str] = {}
        inaccessible: list[str] = []
        authority_events: dict[str, SourceEventKind] = {}
        for corpus_id, drive_id in self._corpora():
            kwargs: dict[str, object] = {"supportsAllDrives": True}
            if drive_id is not None:
                kwargs["driveId"] = drive_id
            try:
                response = await self._execute(
                    service.changes().getStartPageToken(**kwargs)
                )
                token = response.get("startPageToken")
                if not isinstance(token, str) or not token:
                    raise ValueError("Drive start page token is malformed")
                tokens[corpus_id] = token
            except asyncio.CancelledError:
                raise
            except Exception as error:
                inaccessible.append(_authority_digest("corpus", corpus_id))
                event = _terminal_authority_event(error)
                if event is not None:
                    authority_events[corpus_id] = _stronger_event(
                        authority_events.get(corpus_id),
                        event,
                    )
        return tokens, tuple(sorted(inaccessible)), authority_events

    async def _get_file(self, file_id: str) -> DriveFileInfo:
        response = await self._execute(
            self._drive_service()
            .files()
            .get(
                fileId=file_id,
                fields=_FILE_FIELDS,
                supportsAllDrives=True,
            )
        )
        return _file_info(response)

    async def _scan_root(
        self,
        root_id: str,
        records: dict[str, DriveFileInfo],
        inaccessible: list[str],
        authority_events: dict[tuple[str, str], SourceEventKind],
    ) -> None:
        try:
            root = await self._get_file(root_id)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            inaccessible.append(_authority_digest("root", root_id))
            event = _terminal_authority_event(error)
            if event is not None:
                authority_events[("root", root_id)] = event
            return
        records[root.file_id] = root
        folders = [root.file_id] if root.mime_type == _FOLDER_MIME else []
        visited: set[str] = set()
        while folders:
            folder_id = folders.pop(0)
            if folder_id in visited:
                continue
            visited.add(folder_id)
            folder_info = records[folder_id]
            if dict(folder_info.capabilities).get("canListChildren") is False:
                inaccessible.append(_authority_digest("folder", folder_id))
                authority_events[("folder", folder_id)] = SourceEventKind.ACCESS_LOST
                continue
            page_token: str | None = None
            while True:
                try:
                    list_kwargs: dict[str, object] = {
                        "q": f"'{folder_id}' in parents and trashed = false",
                        "fields": _LIST_FIELDS,
                        "pageSize": 1000,
                        "pageToken": page_token,
                        "supportsAllDrives": True,
                        "includeItemsFromAllDrives": True,
                    }
                    if folder_info.drive_id is not None:
                        list_kwargs["corpora"] = "drive"
                        list_kwargs["driveId"] = folder_info.drive_id
                    response = await self._execute(
                        self._drive_service().files().list(**list_kwargs)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    inaccessible.append(_authority_digest("folder", folder_id))
                    event = _terminal_authority_event(error)
                    if event is not None:
                        authority_events[("folder", folder_id)] = event
                    return
                if response.get("incompleteSearch") is True:
                    inaccessible.append(_authority_digest("folder", folder_id))
                raw_files = response.get("files", [])
                if not isinstance(raw_files, list):
                    inaccessible.append(_authority_digest("folder", folder_id))
                    break
                try:
                    for raw_file in raw_files:
                        if not isinstance(raw_file, Mapping):
                            raise ValueError
                        info = _file_info(raw_file)
                        records[info.file_id] = info
                        if info.mime_type == _FOLDER_MIME:
                            folders.append(info.file_id)
                except ValueError:
                    inaccessible.append(_authority_digest("folder", folder_id))
                    break
                next_page = response.get("nextPageToken")
                if next_page is None:
                    break
                if not isinstance(next_page, str) or not next_page:
                    inaccessible.append(_authority_digest("folder", folder_id))
                    break
                page_token = next_page

    async def _scan_shared_drive(
        self,
        drive_id: str,
        records: dict[str, DriveFileInfo],
        inaccessible: list[str],
        authority_events: dict[tuple[str, str], SourceEventKind],
    ) -> None:
        page_token: str | None = None
        while True:
            try:
                response = await self._execute(
                    self._drive_service()
                    .files()
                    .list(
                        corpora="drive",
                        driveId=drive_id,
                        q="trashed = false",
                        fields=_LIST_FIELDS,
                        pageSize=1000,
                        pageToken=page_token,
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                inaccessible.append(_authority_digest("shared-drive", drive_id))
                event = _terminal_authority_event(error)
                if event is not None:
                    authority_events[("shared-drive", drive_id)] = event
                return
            if response.get("incompleteSearch") is True:
                inaccessible.append(_authority_digest("shared-drive", drive_id))
            raw_files = response.get("files", [])
            if not isinstance(raw_files, list):
                inaccessible.append(_authority_digest("shared-drive", drive_id))
                return
            try:
                for raw_file in raw_files:
                    if not isinstance(raw_file, Mapping):
                        raise ValueError
                    info = _file_info(raw_file)
                    records[info.file_id] = info
            except ValueError:
                inaccessible.append(_authority_digest("shared-drive", drive_id))
                return
            next_page = response.get("nextPageToken")
            if next_page is None:
                return
            if not isinstance(next_page, str) or not next_page:
                inaccessible.append(_authority_digest("shared-drive", drive_id))
                return
            page_token = next_page

    async def _list_descendants_strict(
        self,
        folder: DriveFileInfo,
    ) -> tuple[DriveFileInfo, ...]:
        """Enumerate a newly in-scope folder without permitting partial state."""

        discovered: list[DriveFileInfo] = []
        discovered_ids: set[str] = set()
        folders = [folder]
        visited: set[str] = set()
        while folders:
            parent = folders.pop(0)
            if parent.file_id in visited:
                continue
            visited.add(parent.file_id)
            if dict(parent.capabilities).get("canListChildren") is False:
                raise DriveGovernanceError(
                    "newly in-scope Drive folder cannot be enumerated"
                )
            page_token: str | None = None
            while True:
                kwargs: dict[str, object] = {
                    "q": f"'{parent.file_id}' in parents and trashed = false",
                    "fields": _LIST_FIELDS,
                    "pageSize": 1000,
                    "pageToken": page_token,
                    "supportsAllDrives": True,
                    "includeItemsFromAllDrives": True,
                }
                if parent.drive_id is not None:
                    kwargs["corpora"] = "drive"
                    kwargs["driveId"] = parent.drive_id
                response = await self._execute(
                    self._drive_service().files().list(**kwargs)
                )
                if response.get("incompleteSearch") is True:
                    raise DriveGovernanceError(
                        "newly in-scope Drive folder search is incomplete"
                    )
                raw_files = response.get("files", [])
                if not isinstance(raw_files, list):
                    raise DriveGovernanceError(
                        "newly in-scope Drive folder response is malformed"
                    )
                for raw_file in raw_files:
                    if not isinstance(raw_file, Mapping):
                        raise DriveGovernanceError(
                            "newly in-scope Drive folder response is malformed"
                        )
                    try:
                        info = _file_info(raw_file)
                    except ValueError:
                        raise DriveGovernanceError(
                            "newly in-scope Drive file metadata is malformed"
                        ) from None
                    if info.file_id in discovered_ids:
                        continue
                    discovered_ids.add(info.file_id)
                    discovered.append(info)
                    if info.mime_type == _FOLDER_MIME:
                        folders.append(info)
                next_page = response.get("nextPageToken")
                if next_page is None:
                    break
                if not isinstance(next_page, str) or not next_page:
                    raise DriveGovernanceError(
                        "newly in-scope Drive folder page token is malformed"
                    )
                page_token = next_page
        return tuple(discovered)

    @staticmethod
    def _assign_display_paths(records: Mapping[str, DriveFileInfo]) -> None:
        cache: dict[str, PurePath] = {}

        def resolve(file_id: str, visiting: frozenset[str]) -> PurePath:
            cached = cache.get(file_id)
            if cached is not None:
                return cached
            info = records[file_id]
            if file_id in visiting:
                path = PurePath(info.name)
            else:
                known_parents = sorted(
                    parent for parent in info.parents if parent in records
                )
                if known_parents:
                    path = resolve(known_parents[0], visiting | {file_id}) / info.name
                else:
                    path = PurePath(info.name)
            cache[file_id] = path
            return path

        for file_id, info in records.items():
            info.display_path = resolve(file_id, frozenset()).as_posix()

    def _final_access(
        self,
        info: DriveFileInfo,
        resolution: DrivePermissionResolution,
    ) -> AccessSnapshot:
        policy_digest = _digest(
            "effective-policy-input",
            {
                "declared_policy_id": self._policy_id,
                "declared_policy_revision": self._policy_revision,
                "permission_digest": resolution.policy_digest,
                "capabilities": info.capabilities,
                "inherited_permissions_disabled": (info.inherited_permissions_disabled),
            },
        )
        return AccessSnapshot(
            tenant_id=self._tenant_id,
            policy_id=self._policy_id,
            policy_revision=f"driveacl1_{policy_digest}",
            policy_digest=policy_digest,
            group_graph_revision=self._group_graph_revision,
            inherited_from=resolution.inherited_from,
            valid_until=resolution.valid_until,
        )

    async def _resolve_node(
        self,
        info: DriveFileInfo,
        resolver: DrivePermissionResolver,
    ) -> _StoredNode:
        resolution = await resolver.resolve(
            info.file_id,
            inherited_permissions_disabled=info.inherited_permissions_disabled,
        )
        access = self._final_access(info, resolution)
        return _StoredNode(
            info=info,
            source_revision=_content_revision(info),
            access=access,
            grants=resolution.grants,
            policy_authority=resolution.authority,
            limitation_codes=resolution.limitation_codes,
        )

    def _identity(self, file_id: str) -> SourceIdentity:
        return SourceIdentity(
            connector_instance_id=self._connector_instance_id,
            source_scope_id=self._source_scope_id,
            item_id=file_id,
        )

    def _selected_file(self, node: _StoredNode) -> bool:
        return (
            not node.is_folder
            and not node.info.trashed
            and (self._mime_types is None or node.info.mime_type in self._mime_types)
        )

    def _event_for_node(
        self,
        node: _StoredNode,
        previous: _StoredNode | None,
    ) -> SourceEventKind:
        if previous is None or not previous.active:
            return SourceEventKind.PRESENT
        if _corpus_id(previous.info) != _corpus_id(node.info):
            return SourceEventKind.MOVED_SCOPE
        if previous.access.group_graph_revision != node.access.group_graph_revision:
            return SourceEventKind.GROUP_GRAPH_CHANGED
        if (
            previous.access.policy_digest != node.access.policy_digest
            or previous.access.valid_until != node.access.valid_until
        ):
            return SourceEventKind.ACL_CHANGED
        if previous.source_revision != node.source_revision:
            return SourceEventKind.CONTENT_CHANGED
        return SourceEventKind.PRESENT

    def _observation(
        self,
        node: _StoredNode,
        *,
        previous: _StoredNode | None,
        event: SourceEventKind,
        generation: str,
        resource: DriveFile | None = None,
        access: AccessSnapshot | None = None,
    ) -> DriveGovernedObservation:
        effective_access = access if access is not None else node.access
        identity = self._identity(node.info.file_id)
        item = GovernedSourceItem(
            identity=identity,
            resource=(
                resource
                if resource is not None
                else (
                    DriveFile(self._drive_service(), node.info)
                    if event
                    in {
                        SourceEventKind.PRESENT,
                        SourceEventKind.CONTENT_CHANGED,
                        SourceEventKind.ACL_CHANGED,
                        SourceEventKind.GROUP_GRAPH_CHANGED,
                        SourceEventKind.MOVED_SCOPE,
                    }
                    and node.active
                    else None
                )
            ),
            source_revision=node.source_revision,
            content_fingerprint=_content_fingerprint(node.info),
            access=effective_access,
            event=event,
            observation_id=make_observation_id(
                identity,
                node.source_revision,
                event,
                effective_access,
                observation_generation=generation,
            ),
        )
        return DriveGovernedObservation(
            item=item,
            observation_generation=generation,
            previous_access=previous.access if previous is not None else None,
            previous_corpus_id=(
                _corpus_id(previous.info) if previous is not None else None
            ),
            current_corpus_id=_corpus_id(node.info) if node.active else None,
            policy_authority=node.policy_authority,
            limitation_codes=node.limitation_codes,
        )

    async def prepare_snapshot(self) -> DrivePreparedBatch:
        """Prepare a full governed inventory without advancing any cursor."""

        previous_state = await self._load_state()
        previous_nodes = previous_state.node_map
        cursor_before = previous_state.cursor_map
        (
            start_tokens,
            token_failures,
            corpus_authority_events,
        ) = await self._start_tokens()
        inaccessible = list(token_failures)
        authority_events: dict[tuple[str, str], SourceEventKind] = {}
        records: dict[str, DriveFileInfo] = {}
        for root_id in self._root_folder_ids:
            await self._scan_root(
                root_id,
                records,
                inaccessible,
                authority_events,
            )
        for drive_id in self._shared_drive_ids:
            await self._scan_shared_drive(
                drive_id,
                records,
                inaccessible,
                authority_events,
            )
        # A root inside a shared drive cannot be governed from the user log
        # alone. Refuse authority unless that drive was explicitly configured,
        # which also prevents silently broadening a rooted scope.
        for required_drive_id in sorted(
            {
                info.drive_id
                for info in records.values()
                if info.drive_id is not None
                and info.drive_id not in self._shared_drive_ids
            }
        ):
            inaccessible.append(
                _authority_digest("unconfigured-shared-drive", required_drive_id)
            )
        self._assign_display_paths(records)

        resolver = DrivePermissionResolver(
            self._drive_service(),
            execute=self._execute,
            group_graph_revision=self._group_graph_revision,
        )
        resolved: dict[str, _StoredNode] = {}
        for file_id in sorted(records):
            try:
                resolved[file_id] = await self._resolve_node(records[file_id], resolver)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                inaccessible.append(_authority_digest("permissions", file_id))
                event = _terminal_authority_event(error)
                if event is not None:
                    authority_events[("permissions", file_id)] = event

        # Drive file listing is not a transactional snapshot. Fence it with
        # the change tokens captured before enumeration, apply every current
        # state observed during the scan, and checkpoint only the terminal
        # tokens. A failed catch-up makes the entire inventory partial.
        catchup_events: dict[str, SourceEventKind] = {}
        if len(start_tokens) == len(self._corpora()):
            try:
                catchup_changes, terminal_tokens = await self._read_changes(
                    start_tokens
                )
                changed_folders: set[str] = set()
                entering_folders: set[str] = set()
                revoked_catchup_folders: set[str] = set()
                explicit_file_ids = {
                    change.file_id
                    for change in catchup_changes
                    if change.is_file_change and change.file_id is not None
                }

                def remove_catchup_tree(
                    file_id: str,
                    event: SourceEventKind,
                ) -> None:
                    graph = dict(previous_nodes)
                    graph.update(resolved)
                    affected = (file_id,) + self._descendants({file_id}, graph)
                    for affected_id in affected:
                        affected_node = graph.get(affected_id)
                        if affected_node is not None and affected_node.is_folder:
                            revoked_catchup_folders.add(affected_id)
                        resolved.pop(affected_id, None)
                        catchup_events[affected_id] = _stronger_event(
                            catchup_events.get(affected_id),
                            event,
                        )

                for change in catchup_changes:
                    if change.is_drive_change:
                        assert change.drive_id is not None
                        if change.drive_id not in self._shared_drive_ids:
                            continue
                        inaccessible.append(
                            _authority_digest("drive-change", change.drive_id)
                        )
                        event = (
                            SourceEventKind.ACCESS_LOST
                            if change.removed or change.drive is None
                            else SourceEventKind.ACL_CHANGED
                        )
                        corpus_id = f"drive:{change.drive_id}"
                        corpus_authority_events[corpus_id] = _stronger_event(
                            corpus_authority_events.get(corpus_id),
                            event,
                        )
                        continue
                    assert change.file_id is not None
                    file_id = change.file_id
                    previous_node = previous_nodes.get(file_id)
                    if change.file is None:
                        if change.removed:
                            remove_catchup_tree(
                                file_id,
                                SourceEventKind.AMBIGUOUS_REMOVAL,
                            )
                        continue
                    info = _file_info(change.file)
                    info.display_path = self._display_path_for_info(info, resolved)
                    if any(
                        parent_id in revoked_catchup_folders
                        for parent_id in info.parents
                    ) or not self._is_in_scope(info, resolved):
                        if previous_node is not None or file_id in resolved:
                            remove_catchup_tree(
                                file_id,
                                SourceEventKind.MOVED_SCOPE,
                            )
                        continue
                    try:
                        current = await self._resolve_node(info, resolver)
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        event = _terminal_authority_event(error)
                        if event is not None:
                            authority_events[("permissions", file_id)] = event
                        raise
                    current.info.display_path = info.display_path
                    if current.info.trashed:
                        remove_catchup_tree(
                            file_id,
                            SourceEventKind.SOURCE_DELETED,
                        )
                        continue
                    was_resolved = file_id in resolved
                    resolved[file_id] = current
                    if change.removed:
                        catchup_events[file_id] = classify_removed_change(
                            change,
                            previous_corpus_id=(
                                _corpus_id(previous_node.info)
                                if previous_node is not None
                                else None
                            ),
                            current_corpus_id=_corpus_id(current.info),
                            still_in_scope=True,
                        )
                    if current.is_folder:
                        if not was_resolved:
                            entering_folders.add(file_id)
                        else:
                            changed_folders.add(file_id)

                for folder_id in sorted(entering_folders):
                    folder_node = resolved.get(folder_id)
                    if folder_node is None:
                        continue
                    for info in await self._list_descendants_strict(folder_node.info):
                        explicit_node = resolved.get(info.file_id)
                        if (
                            info.file_id in explicit_file_ids
                            and explicit_node is not None
                            and explicit_node.active
                        ):
                            continue
                        info.display_path = self._display_path_for_info(
                            info,
                            resolved,
                        )
                        current = await self._resolve_node(info, resolver)
                        current.info.display_path = info.display_path
                        resolved[info.file_id] = current

                # A parent permission change has no per-descendant change
                # entries. Refresh the bounded dependency graph before this
                # snapshot can call itself complete.
                descendant_ids = self._descendants(changed_folders, resolved)
                for file_id in descendant_ids:
                    descendant_node = resolved.get(file_id)
                    if descendant_node is None:
                        continue
                    try:
                        info = await self._get_file(file_id)
                        info.display_path = self._display_path_for_info(info, resolved)
                        resolved[file_id] = await self._resolve_node(info, resolver)
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        event = _terminal_authority_event(error)
                        if event is not None:
                            authority_events[("item", file_id)] = event
                        raise
                    resolved[file_id].info.display_path = info.display_path
                start_tokens = terminal_tokens
            except asyncio.CancelledError:
                raise
            except _DriveCorpusAuthorityError as error:
                inaccessible.append(
                    _authority_digest(
                        "snapshot-change-corpus",
                        error.corpus_id,
                    )
                )
                corpus_authority_events[error.corpus_id] = _stronger_event(
                    corpus_authority_events.get(error.corpus_id),
                    error.event,
                )
            except Exception as error:
                inaccessible.append(
                    _authority_digest("snapshot-change-fence", self._source_scope_id)
                )
                event = _terminal_authority_event(error)
                if event is not None:
                    for corpus_id, _drive_id in self._corpora():
                        corpus_authority_events[corpus_id] = _stronger_event(
                            corpus_authority_events.get(corpus_id),
                            event,
                        )

        forced_events: dict[str, SourceEventKind] = {}

        def assign_forced(
            file_ids: Sequence[str],
            event: SourceEventKind,
        ) -> None:
            for file_id in file_ids:
                if file_id not in previous_nodes:
                    continue
                forced_events[file_id] = _stronger_event(
                    forced_events.get(file_id),
                    event,
                )

        for corpus_id, event in corpus_authority_events.items():
            assign_forced(
                [
                    file_id
                    for file_id, node in previous_nodes.items()
                    if _corpus_id(node.info) == corpus_id
                ],
                event,
            )
        for (kind, authority_id), event in authority_events.items():
            if kind == "shared-drive":
                affected_ids = [
                    file_id
                    for file_id, node in previous_nodes.items()
                    if node.info.drive_id == authority_id
                ]
            else:
                affected_ids = [authority_id]
                affected_ids.extend(self._descendants({authority_id}, previous_nodes))
            assign_forced(affected_ids, event)

        epoch = uuid.uuid4().hex
        generation = _safe_generation(
            "snapshot",
            (epoch, _cursor_digest(start_tokens), sorted(resolved)),
        )
        observations: list[DriveGovernedObservation] = []
        for file_id, event in sorted(forced_events.items()):
            forced_previous = previous_nodes[file_id]
            if not forced_previous.active:
                continue
            suppressed = replace(
                forced_previous,
                active=False,
                last_event=event,
            )
            resolved[file_id] = suppressed
            if self._selected_file(forced_previous):
                observations.append(
                    self._observation(
                        suppressed,
                        previous=forced_previous,
                        event=event,
                        generation=generation,
                        access=forced_previous.access,
                    )
                )
        for file_id, node in sorted(resolved.items()):
            if file_id in forced_events:
                continue
            if not self._selected_file(node):
                previous = previous_nodes.get(file_id)
                if (
                    previous is not None
                    and previous.active
                    and self._selected_file(previous)
                    and not node.is_folder
                ):
                    filtered = replace(
                        node,
                        active=False,
                        last_event=SourceEventKind.MOVED_SCOPE,
                    )
                    resolved[file_id] = filtered
                    observations.append(
                        self._observation(
                            filtered,
                            previous=previous,
                            event=SourceEventKind.MOVED_SCOPE,
                            generation=generation,
                            access=previous.access,
                        )
                    )
                continue
            previous = previous_nodes.get(file_id)
            event = catchup_events.get(
                file_id,
                self._event_for_node(node, previous),
            )
            observed_node = replace(node, last_event=event)
            resolved[file_id] = observed_node
            observations.append(
                self._observation(
                    observed_node,
                    previous=previous,
                    event=event,
                    generation=generation,
                )
            )

        unique_inaccessible = tuple(sorted(set(inaccessible)))
        complete = not unique_inaccessible
        if complete:
            for file_id, previous in sorted(previous_nodes.items()):
                if (
                    previous.active
                    and self._selected_file(previous)
                    and file_id not in resolved
                ):
                    removal_event = catchup_events.get(
                        file_id,
                        SourceEventKind.AMBIGUOUS_REMOVAL,
                    )
                    removed = replace(
                        previous,
                        active=False,
                        last_event=removal_event,
                    )
                    resolved[file_id] = removed
                    observations.append(
                        self._observation(
                            removed,
                            previous=previous,
                            event=removal_event,
                            generation=generation,
                            access=previous.access,
                        )
                    )
            for file_id, previous in previous_nodes.items():
                if not previous.active and file_id not in resolved:
                    resolved[file_id] = previous
        status: Literal["complete", "partial", "failed"]
        if complete:
            status = "complete"
        elif not resolved and not observations:
            status = "failed"
        else:
            status = "partial"
        snapshot_result = SnapshotResult(
            connector_instance_id=self._connector_instance_id,
            source_scope_id=self._source_scope_id,
            epoch=epoch,
            cursor_before=_cursor_digest(cursor_before),
            cursor_after=_cursor_digest(start_tokens) if complete else None,
            status=status,
            item_count=sum(
                node.active and self._selected_file(node) for node in resolved.values()
            ),
            inaccessible_scope_digests=unique_inaccessible,
        )

        def commit_builder(current: _StateEnvelope) -> _StateEnvelope:
            if complete:
                next_nodes = resolved
                next_cursors = start_tokens
            else:
                # A partial scan may refresh observed items but cannot erase a
                # previously known node or advance the authoritative cursor.
                next_nodes = current.node_map
                next_nodes.update(resolved)
                next_cursors = current.cursor_map
            return _StateEnvelope(
                generation=current.generation + 1,
                cursors=tuple(sorted(next_cursors.items())),
                nodes=tuple(
                    sorted(next_nodes.values(), key=lambda node: node.info.file_id)
                ),
                pending=None if complete else current.pending,
                authority_digest=current.authority_digest,
                semantic_digest=(
                    self._semantic_digest if complete else current.semantic_digest
                ),
            )

        return DrivePreparedBatch(
            source=self,
            base_generation=previous_state.generation,
            kind="snapshot",
            observations=observations,
            commit_builder=commit_builder,
            snapshot_result=snapshot_result,
            requires_full_snapshot=not complete,
        )

    async def apply_snapshot(
        self,
        downstream_ready: DownstreamReady,
    ) -> SnapshotResult:
        """Prepare, durably handle, then atomically checkpoint one snapshot."""

        batch = await self.prepare_snapshot()
        await downstream_ready(batch)
        await batch.commit()
        assert batch.snapshot_result is not None
        return batch.snapshot_result

    async def open_governed_snapshot(self) -> "GovernedDriveSnapshotSession":
        """Open an explicit prepare/read/ack snapshot session."""

        return GovernedDriveSnapshotSession(await self.prepare_snapshot())

    def _descendants(
        self,
        parent_ids: set[str],
        nodes: Mapping[str, _StoredNode],
    ) -> tuple[str, ...]:
        descendants: set[str] = set()
        frontier = set(parent_ids)
        while frontier:
            next_frontier: set[str] = set()
            for file_id, node in nodes.items():
                if file_id in descendants or file_id in parent_ids:
                    continue
                if frontier.intersection(node.info.parents):
                    descendants.add(file_id)
                    next_frontier.add(file_id)
            frontier = next_frontier
        return tuple(sorted(descendants))

    def _deactivate_known_tree(
        self,
        root_id: str,
        *,
        event: SourceEventKind,
        nodes: Mapping[str, _StoredNode],
        updates: dict[str, _StoredNode],
        observations: list[DriveGovernedObservation],
        generation: str,
    ) -> None:
        combined = dict(nodes)
        combined.update(updates)
        affected = (root_id,) + self._descendants({root_id}, combined)
        for file_id in affected:
            current = combined.get(file_id)
            if current is None or not current.active:
                continue
            inactive = replace(
                current,
                active=False,
                last_event=event,
            )
            updates[file_id] = inactive
            previous = nodes.get(file_id)
            if (
                previous is None
                or not previous.active
                or not self._selected_file(previous)
            ):
                continue
            observations.append(
                self._observation(
                    inactive,
                    previous=previous,
                    event=event,
                    generation=generation,
                    access=previous.access,
                )
            )

    def _full_snapshot_required_batch(
        self,
        previous_state: _StateEnvelope,
        *,
        reason: str,
        events: Mapping[str, SourceEventKind] | None = None,
    ) -> DrivePreparedBatch:
        generation = _safe_generation(
            "changes",
            (
                reason,
                _cursor_digest(previous_state.cursor_map),
                self._semantic_digest,
            ),
        )
        effective_events = (
            events
            if events is not None
            else {
                node.info.file_id: SourceEventKind.SCAN_INCOMPLETE
                for node in previous_state.nodes
                if node.active
            }
        )
        observations: list[DriveGovernedObservation] = []
        for file_id, event in sorted(effective_events.items()):
            previous = previous_state.node_map.get(file_id)
            if (
                previous is None
                or not previous.active
                or not self._selected_file(previous)
            ):
                continue
            suppressed = replace(
                previous,
                active=False,
                last_event=event,
            )
            observations.append(
                self._observation(
                    suppressed,
                    previous=previous,
                    event=event,
                    generation=generation,
                    access=previous.access,
                )
            )

        def commit_builder(current: _StateEnvelope) -> _StateEnvelope:
            # The batch is deliberately non-committable. Keep this builder
            # structurally valid so all prepared batches share one type.
            return replace(current, generation=current.generation + 1)

        return DrivePreparedBatch(
            source=self,
            base_generation=previous_state.generation,
            kind="changes",
            observations=observations,
            commit_builder=commit_builder,
            requires_full_snapshot=True,
        )

    def _is_in_scope(
        self,
        info: DriveFileInfo,
        nodes: Mapping[str, _StoredNode],
    ) -> bool:
        if info.file_id in self._root_folder_ids:
            return True
        frontier = list(info.parents)
        visited: set[str] = set()
        has_inactive_ancestor = False
        while frontier:
            parent_id = frontier.pop()
            if parent_id in visited:
                continue
            visited.add(parent_id)
            parent = nodes.get(parent_id)
            if parent is not None and not parent.active:
                # A folder-level removal or authority loss is a fail-closed
                # scope fence for descendants in the same replay batch. A
                # later child entry may escape only by proving a different
                # active parent path.
                has_inactive_ancestor = True
                continue
            if parent_id in self._root_folder_ids:
                return True
            if parent is not None:
                frontier.extend(parent.info.parents)
        if has_inactive_ancestor:
            return False
        return info.drive_id in self._shared_drive_ids

    @staticmethod
    def _display_path_for_info(
        info: DriveFileInfo,
        nodes: Mapping[str, _StoredNode],
    ) -> str:
        for parent_id in sorted(info.parents):
            parent = nodes.get(parent_id)
            if parent is None or parent.info.display_path is None:
                continue
            return (PurePath(parent.info.display_path) / info.name).as_posix()
        return info.name

    async def _read_changes(
        self,
        cursors: Mapping[str, str],
    ) -> tuple[tuple[DriveChange, ...], dict[str, str]]:
        all_changes: list[DriveChange] = []
        cursor_after: dict[str, str] = {}
        ordinal = 0
        for corpus_id, drive_id in self._corpora():
            page_token = cursors.get(corpus_id)
            if page_token is None:
                raise DriveFullSnapshotRequired(
                    "governed Drive cursor set is incomplete"
                )
            while True:
                kwargs: dict[str, object] = {
                    "pageToken": page_token,
                    "spaces": "drive",
                    "includeRemoved": True,
                    "includeCorpusRemovals": True,
                    "includeItemsFromAllDrives": True,
                    "supportsAllDrives": True,
                    "pageSize": 1000,
                    "fields": CHANGE_LIST_FIELDS,
                }
                if drive_id is not None:
                    kwargs["driveId"] = drive_id
                try:
                    response = await self._execute(
                        self._drive_service().changes().list(**kwargs)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    if is_rejected_cursor_error(error):
                        raise DriveFullSnapshotRequired(
                            "Drive rejected a stored cursor; full snapshot required"
                        ) from None
                    event = _terminal_authority_event(error)
                    if event is not None:
                        raise _DriveCorpusAuthorityError(
                            corpus_id,
                            event,
                        ) from None
                    raise
                page = parse_change_page(
                    response,
                    corpus_id=corpus_id,
                    ordinal_start=ordinal,
                )
                all_changes.extend(page.changes)
                ordinal += len(page.changes)
                if page.next_page_token is not None:
                    page_token = page.next_page_token
                    continue
                if page.new_start_page_token is None:
                    raise DriveGovernanceError(
                        "terminal Drive change page omitted newStartPageToken"
                    )
                cursor_after[corpus_id] = page.new_start_page_token
                break
        return coalesce_changes(all_changes), cursor_after

    async def _refresh_descendants(
        self,
        file_ids: Sequence[str],
        *,
        previous_state: _StateEnvelope,
        cursor_after: Mapping[str, str],
        observation_generation: str,
    ) -> DrivePreparedBatch:
        selected = tuple(file_ids[:_MAX_DESCENDANT_RECOMPUTATIONS])
        remaining = tuple(file_ids[_MAX_DESCENDANT_RECOMPUTATIONS:])
        nodes = previous_state.node_map
        updates: dict[str, _StoredNode] = {}
        observations: list[DriveGovernedObservation] = []
        resolver = DrivePermissionResolver(
            self._drive_service(),
            execute=self._execute,
            group_graph_revision=self._group_graph_revision,
        )
        for file_id in selected:
            previous = nodes.get(file_id)
            if previous is None or not previous.active:
                continue
            try:
                info = await self._get_file(file_id)
                if not self._is_in_scope(info, nodes):
                    self._deactivate_known_tree(
                        file_id,
                        event=SourceEventKind.MOVED_SCOPE,
                        nodes=nodes,
                        updates=updates,
                        observations=observations,
                        generation=observation_generation,
                    )
                    continue
                if info.trashed:
                    self._deactivate_known_tree(
                        file_id,
                        event=SourceEventKind.SOURCE_DELETED,
                        nodes=nodes,
                        updates=updates,
                        observations=observations,
                        generation=observation_generation,
                    )
                    continue
                info.display_path = previous.info.display_path
                refreshed = await self._resolve_node(info, resolver)
                event = self._event_for_node(refreshed, previous)
                if event is SourceEventKind.PRESENT:
                    event = SourceEventKind.ACL_CHANGED
                refreshed = replace(refreshed, last_event=event)
                updates[file_id] = refreshed
                if self._selected_file(refreshed):
                    observations.append(
                        self._observation(
                            refreshed,
                            previous=previous,
                            event=event,
                            generation=observation_generation,
                        )
                    )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                terminal_event = _terminal_authority_event(error)
                if terminal_event is None:
                    raise
                self._deactivate_known_tree(
                    file_id,
                    event=terminal_event,
                    nodes=nodes,
                    updates=updates,
                    observations=observations,
                    generation=observation_generation,
                )

        def commit_builder(current: _StateEnvelope) -> _StateEnvelope:
            next_nodes = current.node_map
            next_nodes.update(updates)
            if remaining:
                pending = _PendingDescendants(
                    cursor_after=tuple(sorted(cursor_after.items())),
                    remaining_file_ids=remaining,
                    observation_generation=observation_generation,
                )
                next_cursors = current.cursor_map
            else:
                pending = None
                next_cursors = dict(cursor_after)
            return _StateEnvelope(
                generation=current.generation + 1,
                cursors=tuple(sorted(next_cursors.items())),
                nodes=tuple(
                    sorted(next_nodes.values(), key=lambda node: node.info.file_id)
                ),
                pending=pending,
                authority_digest=current.authority_digest,
                semantic_digest=current.semantic_digest,
            )

        return DrivePreparedBatch(
            source=self,
            base_generation=previous_state.generation,
            kind="changes",
            observations=observations,
            commit_builder=commit_builder,
            has_more=bool(remaining),
        )

    async def prepare_changes(self) -> DrivePreparedBatch:
        """Prepare one bounded replay batch without advancing its cursor."""

        previous_state = await self._load_state()
        if previous_state.semantic_digest != self._semantic_digest:
            semantic_events: dict[str, SourceEventKind] = {}
            for node in previous_state.nodes:
                if not node.active:
                    continue
                semantic_events[node.info.file_id] = (
                    SourceEventKind.GROUP_GRAPH_CHANGED
                    if node.access.group_graph_revision != self._group_graph_revision
                    else SourceEventKind.ACL_CHANGED
                )
            return self._full_snapshot_required_batch(
                previous_state,
                reason="semantic-config-changed",
                events=semantic_events,
            )
        if previous_state.pending is not None:
            pending = previous_state.pending
            return await self._refresh_descendants(
                pending.remaining_file_ids,
                previous_state=previous_state,
                cursor_after=dict(pending.cursor_after),
                observation_generation=pending.observation_generation,
            )
        if not previous_state.cursors:
            raise DriveFullSnapshotRequired(
                "governed Drive changes require a committed full snapshot"
            )
        try:
            changes, cursor_after = await self._read_changes(previous_state.cursor_map)
        except _DriveCorpusAuthorityError as error:
            authority_events = {
                node.info.file_id: error.event
                for node in previous_state.nodes
                if _corpus_id(node.info) == error.corpus_id
            }
            return self._full_snapshot_required_batch(
                previous_state,
                reason=f"corpus-authority-{error.corpus_id}",
                events=authority_events,
            )
        except DriveFullSnapshotRequired:
            return self._full_snapshot_required_batch(
                previous_state,
                reason="rejected-cursor",
            )
        drive_changes = [
            change
            for change in changes
            if change.is_drive_change and change.drive_id in self._shared_drive_ids
        ]
        if drive_changes:
            drive_events: dict[str, SourceEventKind] = {}
            for change in drive_changes:
                assert change.drive_id is not None
                event = (
                    SourceEventKind.ACCESS_LOST
                    if change.removed or change.drive is None
                    else SourceEventKind.ACL_CHANGED
                )
                for node in previous_state.nodes:
                    if node.info.drive_id == change.drive_id:
                        drive_events[node.info.file_id] = _stronger_event(
                            drive_events.get(node.info.file_id),
                            event,
                        )
            return self._full_snapshot_required_batch(
                previous_state,
                reason="shared-drive-authority-changed",
                events=drive_events,
            )
        changes = tuple(change for change in changes if change.is_file_change)
        generation = _safe_generation(
            "changes",
            (
                _cursor_digest(previous_state.cursor_map),
                _cursor_digest(cursor_after),
            ),
        )
        nodes = previous_state.node_map
        updates: dict[str, _StoredNode] = {}
        observations: list[DriveGovernedObservation] = []
        changed_folders: set[str] = set()
        entering_folders: set[str] = set()
        explicit_file_ids = {
            change.file_id for change in changes if change.file_id is not None
        }
        resolver = DrivePermissionResolver(
            self._drive_service(),
            execute=self._execute,
            group_graph_revision=self._group_graph_revision,
        )
        for change in changes:
            assert change.is_file_change
            assert change.file_id is not None
            file_id = change.file_id
            previous = nodes.get(file_id)
            if change.file is None:
                if not change.removed or previous is None or not previous.active:
                    continue
                event = classify_removed_change(
                    change,
                    previous_corpus_id=_corpus_id(previous.info),
                    current_corpus_id=None,
                    still_in_scope=False,
                )
                self._deactivate_known_tree(
                    file_id,
                    event=event,
                    nodes=nodes,
                    updates=updates,
                    observations=observations,
                    generation=generation,
                )
                continue

            try:
                info = _file_info(change.file)
            except ValueError:
                raise DriveGovernanceError(
                    "Drive change resource is malformed"
                ) from None
            combined_nodes = dict(nodes)
            combined_nodes.update(updates)
            in_scope = self._is_in_scope(info, combined_nodes)
            if not in_scope:
                if previous is not None and previous.active:
                    self._deactivate_known_tree(
                        file_id,
                        event=SourceEventKind.MOVED_SCOPE,
                        nodes=nodes,
                        updates=updates,
                        observations=observations,
                        generation=generation,
                    )
                continue
            try:
                current = await self._resolve_node(info, resolver)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                terminal_event = _terminal_authority_event(error)
                if terminal_event is None:
                    raise
                if previous is not None and previous.active:
                    self._deactivate_known_tree(
                        file_id,
                        event=terminal_event,
                        nodes=nodes,
                        updates=updates,
                        observations=observations,
                        generation=generation,
                    )
                continue
            current.info.display_path = self._display_path_for_info(
                current.info,
                combined_nodes,
            )
            event = self._event_for_node(current, previous)
            if change.removed:
                event = classify_removed_change(
                    change,
                    previous_corpus_id=(
                        _corpus_id(previous.info) if previous is not None else None
                    ),
                    current_corpus_id=_corpus_id(current.info),
                    still_in_scope=True,
                )
            elif current.info.trashed:
                event = SourceEventKind.SOURCE_DELETED
                current = replace(current, active=False)
            current = replace(current, last_event=event)
            updates[file_id] = current
            if current.is_folder:
                if event is SourceEventKind.SOURCE_DELETED:
                    self._deactivate_known_tree(
                        file_id,
                        event=event,
                        nodes=nodes,
                        updates=updates,
                        observations=observations,
                        generation=generation,
                    )
                    continue
                # Drive emits no descendant entries when inherited access is
                # affected. Recompute on every observed folder state change;
                # the descendant queue is bounded and resumable below.
                if previous is None or not previous.active:
                    entering_folders.add(file_id)
                else:
                    changed_folders.add(file_id)
                continue
            if event is not SourceEventKind.SOURCE_DELETED and not self._selected_file(
                current
            ):
                if previous is not None and self._selected_file(previous):
                    filtered = replace(
                        current,
                        active=False,
                        last_event=SourceEventKind.MOVED_SCOPE,
                    )
                    updates[file_id] = filtered
                    observations.append(
                        self._observation(
                            filtered,
                            previous=previous,
                            event=SourceEventKind.MOVED_SCOPE,
                            generation=generation,
                            access=previous.access,
                        )
                    )
                continue
            if event is SourceEventKind.SOURCE_DELETED and (
                previous is None or not self._selected_file(previous)
            ):
                continue
            observations.append(
                self._observation(
                    current,
                    previous=previous,
                    event=event,
                    generation=generation,
                    access=(
                        previous.access
                        if event is SourceEventKind.SOURCE_DELETED
                        and previous is not None
                        else current.access
                    ),
                )
            )

        for folder_id in sorted(entering_folders):
            folder_node = updates.get(folder_id)
            if folder_node is None or not folder_node.active:
                continue
            for info in await self._list_descendants_strict(folder_node.info):
                explicit_node = updates.get(info.file_id)
                if (
                    info.file_id in explicit_file_ids
                    and explicit_node is not None
                    and explicit_node.active
                ):
                    continue
                combined_nodes = dict(nodes)
                combined_nodes.update(updates)
                if not self._is_in_scope(info, combined_nodes):
                    continue
                info.display_path = self._display_path_for_info(
                    info,
                    combined_nodes,
                )
                current = await self._resolve_node(info, resolver)
                current.info.display_path = info.display_path
                previous = nodes.get(info.file_id)
                event = self._event_for_node(current, previous)
                current = replace(current, last_event=event)
                updates[info.file_id] = current
                if self._selected_file(current):
                    observations.append(
                        self._observation(
                            current,
                            previous=previous,
                            event=event,
                            generation=generation,
                        )
                    )

        all_nodes = dict(nodes)
        all_nodes.update(updates)
        descendant_ids = self._descendants(changed_folders, all_nodes)
        selected_descendants = descendant_ids[:_MAX_DESCENDANT_RECOMPUTATIONS]
        remaining_descendants = descendant_ids[_MAX_DESCENDANT_RECOMPUTATIONS:]
        for file_id in selected_descendants:
            previous = all_nodes.get(file_id)
            if previous is None or not previous.active:
                continue
            try:
                info = await self._get_file(file_id)
                if not self._is_in_scope(info, all_nodes):
                    self._deactivate_known_tree(
                        file_id,
                        event=SourceEventKind.MOVED_SCOPE,
                        nodes=nodes,
                        updates=updates,
                        observations=observations,
                        generation=generation,
                    )
                    continue
                if info.trashed:
                    self._deactivate_known_tree(
                        file_id,
                        event=SourceEventKind.SOURCE_DELETED,
                        nodes=nodes,
                        updates=updates,
                        observations=observations,
                        generation=generation,
                    )
                    continue
                info.display_path = previous.info.display_path
                refreshed = await self._resolve_node(info, resolver)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                terminal_event = _terminal_authority_event(error)
                if terminal_event is None:
                    raise
                self._deactivate_known_tree(
                    file_id,
                    event=terminal_event,
                    nodes=nodes,
                    updates=updates,
                    observations=observations,
                    generation=generation,
                )
                continue
            event = (
                SourceEventKind.GROUP_GRAPH_CHANGED
                if previous.access.group_graph_revision
                != refreshed.access.group_graph_revision
                else SourceEventKind.ACL_CHANGED
            )
            refreshed = replace(refreshed, last_event=event)
            updates[file_id] = refreshed
            if self._selected_file(refreshed):
                observations.append(
                    self._observation(
                        refreshed,
                        previous=previous,
                        event=event,
                        generation=generation,
                    )
                )

        def commit_builder(current_state: _StateEnvelope) -> _StateEnvelope:
            next_nodes = current_state.node_map
            next_nodes.update(updates)
            if remaining_descendants:
                pending = _PendingDescendants(
                    cursor_after=tuple(sorted(cursor_after.items())),
                    remaining_file_ids=tuple(remaining_descendants),
                    observation_generation=generation,
                )
                next_cursors = current_state.cursor_map
            else:
                pending = None
                next_cursors = cursor_after
            return _StateEnvelope(
                generation=current_state.generation + 1,
                cursors=tuple(sorted(next_cursors.items())),
                nodes=tuple(
                    sorted(next_nodes.values(), key=lambda node: node.info.file_id)
                ),
                pending=pending,
                authority_digest=current_state.authority_digest,
                semantic_digest=current_state.semantic_digest,
            )

        return DrivePreparedBatch(
            source=self,
            base_generation=previous_state.generation,
            kind="changes",
            observations=observations,
            commit_builder=commit_builder,
            has_more=bool(remaining_descendants),
        )

    async def apply_changes(self, downstream_ready: DownstreamReady) -> int:
        """Replay through all bounded descendant batches and commit afterward."""

        batches = 0
        while True:
            batch = await self.prepare_changes()
            await downstream_ready(batch)
            if batch.requires_full_snapshot:
                raise DriveFullSnapshotRequired(
                    "Drive replay requires a new authoritative snapshot"
                )
            await batch.commit()
            batches += 1
            if not batch.has_more:
                return batches

    async def open_change_session(self) -> "GovernedDriveChangeSession":
        """Open one bounded change-feed session."""

        return GovernedDriveChangeSession(await self.prepare_changes())

    async def next_permission_expiry(self) -> datetime.datetime | None:
        """Return the next unhandled local ACL-expiry suppression deadline."""

        current = await self._load_state()
        deadlines = [
            node.access.valid_until
            for node in current.nodes
            if node.active
            and self._selected_file(node)
            and node.access.valid_until is not None
            and node.expiry_emitted_for != _utc_text(node.access.valid_until)
        ]
        return min(deadlines) if deadlines else None

    async def prepare_due_permission_expirations(
        self,
        *,
        now: datetime.datetime | None = None,
    ) -> DrivePreparedBatch:
        """Stage permission-expiry observations without changing Drive cursors."""

        observed_at = now or _utc_now()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        observed_at = observed_at.astimezone(datetime.timezone.utc)
        previous_state = await self._load_state()
        updates: dict[str, _StoredNode] = {}
        observations: list[DriveGovernedObservation] = []
        for node in previous_state.nodes:
            deadline = node.access.valid_until
            if (
                not node.active
                or not self._selected_file(node)
                or deadline is None
                or deadline > observed_at
                or node.expiry_emitted_for == _utc_text(deadline)
            ):
                continue
            expired = replace(
                node,
                last_event=SourceEventKind.PERMISSION_EXPIRED,
                expiry_emitted_for=_utc_text(deadline),
            )
            updates[node.info.file_id] = expired
            observations.append(
                self._observation(
                    expired,
                    previous=node,
                    event=SourceEventKind.PERMISSION_EXPIRED,
                    generation=_safe_generation(
                        "expiry",
                        (
                            node.info.file_id,
                            _utc_text(deadline),
                            node.access.policy_digest,
                            node.access.group_graph_revision,
                        ),
                    ),
                    access=node.access,
                )
            )

        def commit_builder(current: _StateEnvelope) -> _StateEnvelope:
            nodes = current.node_map
            nodes.update(updates)
            return _StateEnvelope(
                generation=current.generation + 1,
                cursors=current.cursors,
                nodes=tuple(sorted(nodes.values(), key=lambda node: node.info.file_id)),
                pending=current.pending,
                authority_digest=current.authority_digest,
                semantic_digest=current.semantic_digest,
            )

        return DrivePreparedBatch(
            source=self,
            base_generation=previous_state.generation,
            kind="expiry",
            observations=observations,
            commit_builder=commit_builder,
        )

    def open_permission_expiry_session(
        self,
        *,
        now: datetime.datetime | None = None,
    ) -> "GovernedDrivePermissionExpirySession":
        """Open a local expiry session without eagerly changing state."""

        return GovernedDrivePermissionExpirySession(self, now=now)

    async def apply_due_permission_expirations(
        self,
        downstream_ready: DownstreamReady,
        *,
        now: datetime.datetime | None = None,
    ) -> int:
        batch = await self.prepare_due_permission_expirations(now=now)
        await downstream_ready(batch)
        await batch.commit()
        return len(batch.observations)


@dataclass(frozen=True, slots=True)
class DriveChangeReplayResult:
    """Authority outcome for one bounded change replay."""

    status: Literal["complete", "partial", "failed"]
    event_count: int
    full_snapshot_required: bool

    @property
    def authorizes_checkpoint_advance(self) -> bool:
        return self.status == "complete" and not self.full_snapshot_required


class _PreparedSession:
    def __init__(self, batch: DrivePreparedBatch) -> None:
        self._batch = batch
        self._started = False
        self._finished = False

    async def items(
        self,
    ) -> AsyncIterator[tuple[str, GovernedSourceItem[DriveFile]]]:
        if self._started:
            raise DriveSnapshotStateError("session items can be consumed only once")
        self._started = True
        for observation in self._batch.observations:
            yield observation.item.identity.item_id, observation.item
        self._finished = True

    def _require_finished(self) -> None:
        if not self._finished:
            raise DriveSnapshotStateError(
                "session must be exhausted before checkpoint commit"
            )


class GovernedDriveSnapshotSession(_PreparedSession):
    """Explicit authoritative snapshot session."""

    @property
    def result(self) -> SnapshotResult:
        result = self._batch.snapshot_result
        if result is None:
            raise DriveSnapshotStateError("snapshot result is unavailable")
        return result

    async def commit_after_downstream_ready(self) -> None:
        self._require_finished()
        if self.result.status != "complete":
            raise DriveSnapshotStateError(
                "partial or failed snapshot cannot advance its checkpoint"
            )
        await self._batch.commit()


class GovernedDriveChangeSession(_PreparedSession):
    """Explicit change replay with durable-readiness acknowledgement."""

    @property
    def result(self) -> DriveChangeReplayResult:
        self._require_finished()
        if self._batch.requires_full_snapshot:
            status: Literal["complete", "partial", "failed"] = "failed"
        elif self._batch.has_more:
            status = "partial"
        else:
            status = "complete"
        return DriveChangeReplayResult(
            status=status,
            event_count=len(self._batch.observations),
            full_snapshot_required=self._batch.requires_full_snapshot,
        )

    async def commit_after_downstream_ready(self) -> None:
        self._require_finished()
        if self._batch.requires_full_snapshot:
            raise DriveFullSnapshotRequired(
                "Drive replay cannot commit; full snapshot required"
            )
        await self._batch.commit()


class GovernedDrivePermissionExpirySession:
    """Explicit local permission-expiry session."""

    def __init__(
        self,
        source: GovernedGoogleDriveSource,
        *,
        now: datetime.datetime | None,
    ) -> None:
        self._source = source
        self._now = now
        self._batch: DrivePreparedBatch | None = None
        self._started = False
        self._finished = False

    async def items(
        self,
    ) -> AsyncIterator[tuple[str, GovernedSourceItem[DriveFile]]]:
        if self._started:
            raise DriveSnapshotStateError("session items can be consumed only once")
        self._started = True
        self._batch = await self._source.prepare_due_permission_expirations(
            now=self._now
        )
        for observation in self._batch.observations:
            yield observation.item.identity.item_id, observation.item
        self._finished = True

    async def commit_after_downstream_ready(self) -> None:
        if not self._finished or self._batch is None:
            raise DriveSnapshotStateError(
                "session must be exhausted before checkpoint commit"
            )
        await self._batch.commit()


__all__ = [
    "DriveCheckpointConflict",
    "DriveChangeReplayResult",
    "DriveFullSnapshotRequired",
    "DriveGovernanceError",
    "DriveGovernedObservation",
    "DrivePreparedBatch",
    "DriveRequestError",
    "DriveRetryPolicy",
    "DriveSnapshotStateError",
    "DriveStateCorruption",
    "GovernedDriveChangeSession",
    "GovernedDrivePermissionExpirySession",
    "GovernedDriveSnapshotSession",
    "GovernedGoogleDriveSource",
]
