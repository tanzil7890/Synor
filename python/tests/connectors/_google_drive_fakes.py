from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass


class FakeHttpError(RuntimeError):
    def __init__(self, status: int) -> None:
        super().__init__("sensitive provider message")
        self.resp = type("Response", (), {"status": status})()


class FakeRequest:
    def __init__(self, action: Callable[[], Mapping[str, object]]) -> None:
        self._action = action

    def execute(self) -> Mapping[str, object]:
        return self._action()


@dataclass
class FakeDriveData:
    files: dict[str, dict[str, object]]
    children: dict[tuple[str, str | None], Mapping[str, object] | BaseException]
    permissions: dict[tuple[str, str | None], Mapping[str, object] | BaseException]
    start_tokens: dict[str | None, str]
    changes: dict[tuple[str | None, str], Mapping[str, object] | BaseException]


def folder(
    file_id: str,
    name: str,
    *,
    parents: list[str] | None = None,
    drive_id: str | None = None,
    version: str = "1",
) -> dict[str, object]:
    output: dict[str, object] = {
        "id": file_id,
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "modifiedTime": "2026-01-01T00:00:00Z",
        "version": version,
        "parents": parents or [],
        "trashed": False,
        "inheritedPermissionsDisabled": False,
        "capabilities": {"canListChildren": True},
    }
    if drive_id is not None:
        output["driveId"] = drive_id
    return output


def drive_file(
    file_id: str,
    name: str,
    parent_id: str,
    *,
    drive_id: str | None = None,
    version: str = "1",
    trashed: bool = False,
) -> dict[str, object]:
    output: dict[str, object] = {
        "id": file_id,
        "name": name,
        "mimeType": "text/plain",
        "size": "5",
        "modifiedTime": "2026-01-01T00:00:00Z",
        "version": version,
        "md5Checksum": "5d41402abc4b2a76b9719d911017c592",
        "parents": [parent_id],
        "trashed": trashed,
        "inheritedPermissionsDisabled": False,
        "capabilities": {"canDownload": True},
    }
    if drive_id is not None:
        output["driveId"] = drive_id
    return output


def permission(
    permission_id: str,
    *,
    role: str = "reader",
    subject_type: str = "user",
    inherited_from: str | None = None,
    inherited_without_origin: bool = False,
    expiration: str | None = None,
    view: str | None = None,
) -> dict[str, object]:
    detail: dict[str, object] = {
        "permissionType": "user",
        "role": role,
        "inherited": inherited_from is not None or inherited_without_origin,
    }
    if inherited_from is not None:
        detail["inheritedFrom"] = inherited_from
    output: dict[str, object] = {
        "id": permission_id,
        "type": subject_type,
        "role": role,
        "permissionDetails": [detail],
    }
    if expiration is not None:
        output["expirationTime"] = expiration
    if view is not None:
        output["view"] = view
    return output


class _Files:
    def __init__(self, owner: "FakeDriveService") -> None:
        self._owner = owner

    def get(self, **kwargs: object) -> FakeRequest:
        file_id = str(kwargs["fileId"])
        self._owner.calls.append(("files.get", dict(kwargs)))

        def action() -> Mapping[str, object]:
            value = self._owner.data.files.get(file_id)
            if value is None:
                raise FakeHttpError(404)
            return dict(value)

        return FakeRequest(action)

    def list(self, **kwargs: object) -> FakeRequest:
        self._owner.calls.append(("files.list", dict(kwargs)))
        query = str(kwargs["q"])
        match = re.search(r"'([^']+)' in parents", query)
        page_token = kwargs.get("pageToken")

        def action() -> Mapping[str, object]:
            if match is None:
                drive_id = kwargs.get("driveId")
                return {
                    "files": [
                        value
                        for value in self._owner.data.files.values()
                        if value.get("driveId") == drive_id
                    ]
                }
            value = self._owner.data.children.get(
                (match.group(1), cast_token(page_token)),
                {"files": []},
            )
            if isinstance(value, BaseException):
                raise value
            return value

        return FakeRequest(action)


def cast_token(value: object) -> str | None:
    return value if isinstance(value, str) else None


class _Permissions:
    def __init__(self, owner: "FakeDriveService") -> None:
        self._owner = owner

    def list(self, **kwargs: object) -> FakeRequest:
        self._owner.calls.append(("permissions.list", dict(kwargs)))
        key = (str(kwargs["fileId"]), cast_token(kwargs.get("pageToken")))

        def action() -> Mapping[str, object]:
            value = self._owner.data.permissions.get(key, {"permissions": []})
            if isinstance(value, BaseException):
                raise value
            return value

        return FakeRequest(action)


class _Changes:
    def __init__(self, owner: "FakeDriveService") -> None:
        self._owner = owner

    def getStartPageToken(self, **kwargs: object) -> FakeRequest:
        self._owner.calls.append(("changes.start", dict(kwargs)))
        drive_raw = kwargs.get("driveId")
        corpus_id = drive_raw if isinstance(drive_raw, str) else None

        def action() -> Mapping[str, object]:
            return {"startPageToken": self._owner.data.start_tokens[corpus_id]}

        return FakeRequest(action)

    def list(self, **kwargs: object) -> FakeRequest:
        self._owner.calls.append(("changes.list", dict(kwargs)))
        drive_raw = kwargs.get("driveId")
        drive_id = drive_raw if isinstance(drive_raw, str) else None
        key = (drive_id, str(kwargs["pageToken"]))

        def action() -> Mapping[str, object]:
            value = self._owner.data.changes.get(
                key,
                {"changes": [], "newStartPageToken": key[1]},
            )
            if isinstance(value, BaseException):
                raise value
            return value

        return FakeRequest(action)


class FakeDriveService:
    def __init__(self, data: FakeDriveData) -> None:
        self.data = data
        self.calls: list[tuple[str, dict[str, object]]] = []
        self._files = _Files(self)
        self._permissions = _Permissions(self)
        self._changes = _Changes(self)

    def files(self) -> _Files:
        return self._files

    def permissions(self) -> _Permissions:
        return self._permissions

    def changes(self) -> _Changes:
        return self._changes
