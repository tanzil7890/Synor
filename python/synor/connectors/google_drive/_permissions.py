"""Permission normalization for the governed Google Drive source.

The Drive API returns display names and email addresses alongside opaque
permission IDs.  Governed state deliberately retains only the opaque ID and
authorization semantics.  Evidence callers can use :attr:`policy_digest` and
the aggregate counts without copying principal data into receipts or logs.
"""

from __future__ import annotations

import datetime
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from synor._internal.revocation_model import (
    AccessEffect,
    AccessRule,
    AccessSnapshot,
)


_PERMISSION_FIELDS = (
    "nextPageToken,"
    "permissions("
    "id,type,role,allowFileDiscovery,expirationTime,deleted,view,"
    "inheritedPermissionsDisabled,"
    "permissionDetails(permissionType,inheritedFrom,role,inherited)"
    ")"
)
_SCHEMA_VERSION = 1
_SUPPORTED_SUBJECT_TYPES = frozenset({"user", "group", "domain", "anyone"})
_SUPPORTED_ROLES = frozenset(
    {
        "owner",
        "organizer",
        "fileOrganizer",
        "writer",
        "commenter",
        "reader",
        "publishedReader",
    }
)


def _utc(value: str | None) -> datetime.datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(datetime.timezone.utc)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _required_string(value: Mapping[str, object], key: str) -> str | None:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        return None
    return item


def _opaque_missing_permission_id(permission: Mapping[str, object]) -> str:
    """Return a deterministic non-principal placeholder for malformed input."""

    safe_shape = {
        "type": permission.get("type"),
        "role": permission.get("role"),
        "deleted": permission.get("deleted"),
        "view": permission.get("view"),
    }
    return "unresolved_" + hashlib.sha256(_canonical_json(safe_shape)).hexdigest()


@dataclass(frozen=True, slots=True)
class DrivePermissionGrant:
    """One canonical Drive grant without display names or addresses."""

    permission_id: str
    subject_type: str
    role: str
    inherited: bool
    inherited_from: str | None
    permission_type: str | None
    expires_at: datetime.datetime | None
    allow_file_discovery: bool | None
    deleted: bool
    view: str | None
    inherited_permissions_disabled: bool

    def canonical_tuple(
        self,
    ) -> tuple[str, str, str, bool, str, str, str, bool | None, bool, str, bool]:
        return (
            self.permission_id,
            self.subject_type,
            self.role,
            self.inherited,
            self.inherited_from or "",
            self.permission_type or "",
            (
                self.expires_at.isoformat().replace("+00:00", "Z")
                if self.expires_at is not None
                else ""
            ),
            self.allow_file_discovery,
            self.deleted,
            self.view or "",
            self.inherited_permissions_disabled,
        )

    def access_rule(self) -> AccessRule:
        """Project the observed grant onto Synor's provider-neutral ACL type."""

        return AccessRule(
            effect=AccessEffect.GRANT,
            subject_type=self.subject_type,
            subject_id=self.permission_id,
            role=self.role,
            inherited_from=self.inherited_from,
            expires_at=self.expires_at,
        )


@dataclass(frozen=True, slots=True)
class DrivePermissionResolution:
    """Canonical ACL input and an honest statement of its authority."""

    grants: tuple[DrivePermissionGrant, ...]
    policy_digest: str
    valid_until: datetime.datetime | None
    inherited_from: tuple[str, ...]
    authority: Literal["complete", "partial"]
    limitation_codes: tuple[str, ...]

    @property
    def permission_count(self) -> int:
        return len(self.grants)

    @property
    def inherited_count(self) -> int:
        return sum(grant.inherited for grant in self.grants)

    def access_snapshot(
        self,
        *,
        tenant_id: str,
        policy_id: str,
        policy_revision: str,
        group_graph_revision: str,
    ) -> AccessSnapshot:
        """Build the provider-neutral governed access snapshot."""

        return AccessSnapshot(
            tenant_id=tenant_id,
            policy_id=policy_id,
            policy_revision=f"{policy_revision}.driveacl1_{self.policy_digest}",
            policy_digest=self.policy_digest,
            group_graph_revision=group_graph_revision,
            inherited_from=self.inherited_from,
            valid_until=self.valid_until,
        )

    def evidence_summary(self) -> dict[str, object]:
        """Return privacy-safe, count-and-digest-only evidence."""

        return {
            "schema_version": _SCHEMA_VERSION,
            "policy_digest": self.policy_digest,
            "permission_count": self.permission_count,
            "inherited_count": self.inherited_count,
            "authority": self.authority,
            "limitation_codes": self.limitation_codes,
        }


def normalize_permissions(
    permissions: Sequence[Mapping[str, object]],
    *,
    file_inherited_permissions_disabled: bool,
    group_graph_revision: str,
) -> DrivePermissionResolution:
    """Normalize all permission pages into a stable, ordering-independent ACL.

    Missing provider fields are never guessed.  A usable canonical digest is
    still produced, but the resolution is marked ``partial`` with controlled
    limitation codes.
    """

    grants: list[DrivePermissionGrant] = []
    limitations: set[str] = set()
    for permission in permissions:
        permission_id = _required_string(permission, "id")
        if permission_id is None:
            permission_id = _opaque_missing_permission_id(permission)
            limitations.add("permission_id_unavailable")

        subject_type = _required_string(permission, "type")
        if subject_type not in _SUPPORTED_SUBJECT_TYPES:
            subject_type = "unknown"
            limitations.add("permission_type_unsupported")

        permission_role = _required_string(permission, "role")
        if permission_role not in _SUPPORTED_ROLES:
            permission_role = "unknown"
            limitations.add("permission_role_unsupported")

        expiration_value = permission.get("expirationTime")
        expiration_text = (
            expiration_value if isinstance(expiration_value, str) else None
        )
        expires_at = _utc(expiration_text)
        if permission.get("expirationTime") is not None and expires_at is None:
            limitations.add("permission_expiration_invalid")

        raw_details = permission.get("permissionDetails")
        if not isinstance(raw_details, list) or not raw_details:
            raw_details = [{}]
            limitations.add("permission_details_unavailable")

        for raw_detail in raw_details:  # type: object
            detail = raw_detail if isinstance(raw_detail, Mapping) else {}
            if not isinstance(raw_detail, Mapping):
                limitations.add("permission_detail_invalid")
            inherited_raw = detail.get("inherited")
            inherited = inherited_raw if isinstance(inherited_raw, bool) else False
            if not isinstance(inherited_raw, bool):
                limitations.add("permission_inheritance_unknown")
            inherited_from = _required_string(detail, "inheritedFrom")
            if inherited and inherited_from is None:
                # Google does not expose the inheritance origin for every My
                # Drive permission. Keep that uncertainty explicit.
                limitations.add("inheritance_origin_unavailable")
            detail_role = _required_string(detail, "role") or permission_role
            if detail_role not in _SUPPORTED_ROLES:
                detail_role = "unknown"
                limitations.add("permission_role_unsupported")
            permission_type = _required_string(detail, "permissionType")
            discovery_value = permission.get("allowFileDiscovery")
            allow_file_discovery = (
                discovery_value if isinstance(discovery_value, bool) else None
            )
            view_value = permission.get("view")
            view = view_value if isinstance(view_value, str) else None
            grants.append(
                DrivePermissionGrant(
                    permission_id=permission_id,
                    subject_type=subject_type,
                    role=detail_role,
                    inherited=inherited,
                    inherited_from=inherited_from,
                    permission_type=permission_type,
                    expires_at=expires_at,
                    allow_file_discovery=allow_file_discovery,
                    deleted=permission.get("deleted") is True,
                    view=view,
                    inherited_permissions_disabled=(
                        file_inherited_permissions_disabled
                        or permission.get("inheritedPermissionsDisabled") is True
                    ),
                )
            )

    ordered = tuple(sorted(grants, key=DrivePermissionGrant.canonical_tuple))
    if any(grant.subject_type == "group" for grant in ordered):
        if group_graph_revision == "unresolved":
            limitations.add("group_membership_unresolved")
    payload = {
        "schema_version": _SCHEMA_VERSION,
        "file_inherited_permissions_disabled": (file_inherited_permissions_disabled),
        "grants": [grant.canonical_tuple() for grant in ordered],
    }
    digest = hashlib.sha256(_canonical_json(payload)).hexdigest()
    expirations = tuple(
        grant.expires_at for grant in ordered if grant.expires_at is not None
    )
    inherited_sources = tuple(
        sorted(
            {
                grant.inherited_from
                for grant in ordered
                if grant.inherited_from is not None
            }
        )
    )
    limitation_codes = tuple(sorted(limitations))
    return DrivePermissionResolution(
        grants=ordered,
        policy_digest=digest,
        valid_until=min(expirations) if expirations else None,
        inherited_from=inherited_sources,
        authority="partial" if limitation_codes else "complete",
        limitation_codes=limitation_codes,
    )


RequestExecutor = Callable[[Any], Awaitable[Mapping[str, object]]]


class DrivePermissionResolver:
    """Fetch and normalize a file or folder's complete permission listing."""

    def __init__(
        self,
        service: Any,
        *,
        execute: RequestExecutor,
        group_graph_revision: str,
    ) -> None:
        self._service = service
        self._execute = execute
        self._group_graph_revision = group_graph_revision

    async def resolve(
        self,
        file_id: str,
        *,
        inherited_permissions_disabled: bool,
    ) -> DrivePermissionResolution:
        permissions: list[Mapping[str, object]] = []
        page_token: str | None = None
        while True:
            request = self._service.permissions().list(
                fileId=file_id,
                fields=_PERMISSION_FIELDS,
                pageSize=100,
                pageToken=page_token,
                supportsAllDrives=True,
            )
            response = await self._execute(request)
            raw_permissions = response.get("permissions", [])
            if not isinstance(raw_permissions, list):
                raise ValueError("Drive permissions response is malformed")
            for permission in raw_permissions:
                if not isinstance(permission, Mapping):
                    raise ValueError("Drive permission entry is malformed")
                permissions.append(permission)
            next_page = response.get("nextPageToken")
            if next_page is None:
                break
            if not isinstance(next_page, str) or not next_page:
                raise ValueError("Drive permissions page token is malformed")
            page_token = next_page
        return normalize_permissions(
            permissions,
            file_inherited_permissions_disabled=inherited_permissions_disabled,
            group_graph_revision=self._group_graph_revision,
        )


__all__ = [
    "DrivePermissionGrant",
    "DrivePermissionResolution",
    "DrivePermissionResolver",
]
