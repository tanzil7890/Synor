"""Failure quarantine and explicit manual review."""

from __future__ import annotations

import dataclasses as _dataclasses
import datetime as _datetime
import enum as _enum
import json as _json
import typing as _typing
import uuid as _uuid

from . import audit as _audit
from . import state as _state

__all__ = [
    "QuarantineCase",
    "QuarantineRepository",
    "QuarantineStatus",
]

_SCHEMA_VERSION = 1


def _utc_text() -> str:
    return (
        _datetime.datetime.now(_datetime.timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


class QuarantineStatus(str, _enum.Enum):
    """Manual-review state."""

    OPEN = "open"
    APPROVED = "approved"
    REJECTED = "rejected"


@_dataclasses.dataclass(frozen=True, slots=True)
class QuarantineCase:
    """Metadata-only record for a failed or policy-blocked run."""

    case_id: str
    status: QuarantineStatus
    reason: str
    app_name: str
    app_target: str | None
    run_id: str | None
    error_type: str
    created_at: str
    reviewed_at: str | None = None
    reviewer_note: str | None = None

    def to_dict(self) -> dict[str, _typing.Any]:
        """Return redacted JSON data."""

        return _typing.cast(
            dict[str, _typing.Any],
            _audit.redact_metadata(
                {
                    "schema_version": _SCHEMA_VERSION,
                    **_dataclasses.asdict(self),
                    "status": self.status.value,
                }
            ),
        )

    @classmethod
    def from_dict(cls, value: _typing.Mapping[str, _typing.Any]) -> "QuarantineCase":
        """Validate and decode one stored case."""

        if value.get("schema_version") != _SCHEMA_VERSION:
            raise ValueError("unsupported quarantine schema")
        return cls(
            case_id=str(value["case_id"]),
            status=QuarantineStatus(str(value["status"])),
            reason=str(value["reason"]),
            app_name=str(value["app_name"]),
            app_target=(
                str(value["app_target"])
                if value.get("app_target") is not None
                else None
            ),
            run_id=str(value["run_id"]) if value.get("run_id") is not None else None,
            error_type=str(value["error_type"]),
            created_at=str(value["created_at"]),
            reviewed_at=(
                str(value["reviewed_at"])
                if value.get("reviewed_at") is not None
                else None
            ),
            reviewer_note=(
                str(value["reviewer_note"])
                if value.get("reviewer_note") is not None
                else None
            ),
        )


class QuarantineRepository:
    """Quarantine cases stored through an injected :class:`StateStore`."""

    def __init__(self, store: _state.StateStore) -> None:
        self._store = store

    @staticmethod
    def _key(case_id: str) -> str:
        if not case_id or any(
            char not in "abcdefghijklmnopqrstuvwxyz0123456789-" for char in case_id
        ):
            raise ValueError("invalid quarantine case id")
        return f"quarantine/{case_id}.json"

    async def create(
        self,
        *,
        reason: str,
        app_name: str,
        error: BaseException,
        app_target: str | None = None,
        run_id: str | None = None,
    ) -> QuarantineCase:
        """Create a case without storing the exception message."""

        timestamp = _datetime.datetime.now(_datetime.timezone.utc)
        case_id = (
            timestamp.strftime("%Y%m%dT%H%M%S%fZ") + "-" + _uuid.uuid4().hex[:10]
        ).lower()
        case = QuarantineCase(
            case_id=case_id,
            status=QuarantineStatus.OPEN,
            reason=reason,
            app_name=app_name,
            app_target=app_target,
            run_id=run_id,
            error_type=f"{type(error).__module__}.{type(error).__qualname__}",
            created_at=timestamp.isoformat().replace("+00:00", "Z"),
        )
        await self._store.put(
            self._key(case.case_id),
            (_json.dumps(case.to_dict(), indent=2, sort_keys=True) + "\n").encode(),
        )
        return case

    async def get(self, case_id: str) -> QuarantineCase | None:
        """Read one case."""

        payload = await self._store.get(self._key(case_id))
        if payload is None:
            return None
        value = _json.loads(payload)
        if not isinstance(value, dict):
            raise ValueError("invalid quarantine record")
        return QuarantineCase.from_dict(value)

    async def list(
        self, *, status: QuarantineStatus | None = None
    ) -> tuple[QuarantineCase, ...]:
        """List cases newest first."""

        cases: list[QuarantineCase] = []
        for key in await self._store.list("quarantine/"):
            payload = await self._store.get(key)
            if payload is None:
                continue
            value = _json.loads(payload)
            if not isinstance(value, dict):
                continue
            case = QuarantineCase.from_dict(value)
            if status is None or case.status is status:
                cases.append(case)
        return tuple(sorted(cases, key=lambda item: item.created_at, reverse=True))

    async def review(
        self,
        case_id: str,
        *,
        status: QuarantineStatus,
        note: str | None = None,
    ) -> QuarantineCase:
        """Approve or reject an open case without executing pipeline code."""

        if status is QuarantineStatus.OPEN:
            raise ValueError("manual review must approve or reject a case")
        current = await self.get(case_id)
        if current is None:
            raise KeyError(case_id)
        if current.status is not QuarantineStatus.OPEN:
            raise ValueError(f"case is already {current.status.value}")
        reviewed = _dataclasses.replace(
            current,
            status=status,
            reviewed_at=_utc_text(),
            reviewer_note=note,
        )
        await self._store.put(
            self._key(case_id),
            (_json.dumps(reviewed.to_dict(), indent=2, sort_keys=True) + "\n").encode(),
        )
        return reviewed
