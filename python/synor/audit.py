"""Local run manifests and metadata-only audit logs."""

from __future__ import annotations

import dataclasses as _dataclasses
import datetime as _datetime
import enum as _enum
import hashlib as _hashlib
import json as _json
import os as _os
import pathlib as _pathlib
import platform as _platform
import types as _types
import typing as _typing
import uuid as _uuid

from ._version import __version__ as _synor_version
from . import pii as _pii

__all__ = [
    "RunManifest",
    "RunRecorder",
    "latest_run_manifest",
    "read_run_manifest",
    "redact_metadata",
    "resolve_audit_root",
]

_SCHEMA_VERSION = 1
_SECRET_MARKERS = (
    "api_key",
    "authorization",
    "credential",
    "password",
    "private_key",
    "secret",
    "token",
)
_MAX_COLLECTION_ITEMS = 50
_MAX_STRING_LENGTH = 500


def _utc_now() -> _datetime.datetime:
    return _datetime.datetime.now(_datetime.timezone.utc)


def _utc_text(value: _datetime.datetime | None = None) -> str:
    return (value or _utc_now()).isoformat().replace("+00:00", "Z")


def _is_secret_field(name: str) -> bool:
    normalized = name.lower().replace("-", "_")
    return any(marker in normalized for marker in _SECRET_MARKERS)


def redact_metadata(value: _typing.Any, *, _depth: int = 0) -> _typing.Any:
    """Convert a value into deterministic JSON-safe metadata.

    Unknown objects are represented by type, never by ``repr``. Byte content is
    represented by length and SHA-256 digest.
    """

    if _depth > 8:
        return {"type": type(value).__qualname__, "truncated": True}
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        value = _pii.redact_known_pii(value)
        if len(value) <= _MAX_STRING_LENGTH:
            return value
        return value[:_MAX_STRING_LENGTH] + "…"
    if isinstance(value, (bytes, bytearray, memoryview)):
        data = bytes(value)
        return {
            "type": "bytes",
            "length": len(data),
            "sha256": _hashlib.sha256(data).hexdigest(),
        }
    if isinstance(value, (_pathlib.Path, _pathlib.PurePath)):
        return str(value)
    if isinstance(value, _enum.Enum):
        return redact_metadata(value.value, _depth=_depth + 1)
    try:
        audit_projection = getattr(value, "__synor_audit_metadata__", None)
    except Exception:
        audit_projection = None
    if callable(audit_projection):
        try:
            projected = audit_projection()
        except Exception:
            return {
                "type": f"{type(value).__module__}.{type(value).__qualname__}",
                "audit_projection": "failed",
            }
        if projected is value:
            return {
                "type": f"{type(value).__module__}.{type(value).__qualname__}",
                "audit_projection": "invalid",
            }
        return redact_metadata(projected, _depth=_depth + 1)
    if _dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: (
                "<redacted>"
                if _is_secret_field(field.name)
                else redact_metadata(getattr(value, field.name), _depth=_depth + 1)
            )
            for field in _dataclasses.fields(value)
        }
    if isinstance(value, tuple) and hasattr(value, "_asdict"):
        return {
            str(key): (
                "<redacted>"
                if _is_secret_field(str(key))
                else redact_metadata(item, _depth=_depth + 1)
            )
            for key, item in value._asdict().items()
        }
    if isinstance(value, _typing.Mapping):
        result: dict[str, _typing.Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= _MAX_COLLECTION_ITEMS:
                result["<truncated>"] = len(value) - _MAX_COLLECTION_ITEMS
                break
            key_text = str(key)
            result[key_text] = (
                "<redacted>"
                if _is_secret_field(key_text)
                else redact_metadata(item, _depth=_depth + 1)
            )
        return result
    if isinstance(value, (list, tuple, set, frozenset)):
        items = list(value)
        converted = [
            redact_metadata(item, _depth=_depth + 1)
            for item in items[:_MAX_COLLECTION_ITEMS]
        ]
        if len(items) > _MAX_COLLECTION_ITEMS:
            converted.append({"truncated_items": len(items) - _MAX_COLLECTION_ITEMS})
        return converted
    if isinstance(value, _types.MappingProxyType):
        return redact_metadata(dict(value), _depth=_depth + 1)
    return {
        "type": f"{type(value).__module__}.{type(value).__qualname__}",
    }


def resolve_audit_root(
    root: _os.PathLike[str] | str | None = None,
) -> _pathlib.Path:
    """Resolve the run-evidence root without creating it."""

    configured = root if root is not None else _os.getenv("SYNOR_AUDIT_DIR")
    return _pathlib.Path(configured) if configured else _pathlib.Path(".synor/runs")


@_dataclasses.dataclass(slots=True)
class RunManifest:
    """Manifest for one controlled Synor operation."""

    schema_version: int
    run_id: str
    command: str
    status: str
    app_name: str
    app_target: str | None
    environment: str
    db_path: str | None
    started_at: str
    finished_at: str | None
    duration_ms: int | None
    synor_version: str
    python_version: str
    policy: dict[str, _typing.Any]
    options: dict[str, _typing.Any]
    action_count: int | None = None
    artifact_count: int | None = None
    replay_digest: str | None = None
    stats: _typing.Any = None
    error_type: str | None = None
    execution_guarantee: str = "compatibility"
    revocations: dict[str, _typing.Any] | None = None

    def to_dict(self) -> dict[str, _typing.Any]:
        """Return a JSON-safe manifest mapping."""

        return _typing.cast(
            dict[str, _typing.Any], redact_metadata(_dataclasses.asdict(self))
        )


class RunRecorder:
    """Writes one manifest and append-only JSONL audit stream."""

    def __init__(
        self,
        run_dir: _pathlib.Path,
        manifest: RunManifest,
        started_at: _datetime.datetime,
    ) -> None:
        self.run_dir = run_dir
        self.manifest = manifest
        self._started_at = started_at
        self.manifest_path = run_dir / "manifest.json"
        self.audit_path = run_dir / "audit.jsonl"

    @property
    def run_id(self) -> str:
        return self.manifest.run_id

    @classmethod
    def start(
        cls,
        *,
        command: str,
        app_name: str,
        environment: str,
        db_path: _os.PathLike[str] | str | None,
        policy: _typing.Mapping[str, _typing.Any],
        options: _typing.Mapping[str, _typing.Any] | None = None,
        app_target: str | None = None,
        audit_root: _os.PathLike[str] | str | None = None,
        execution_guarantee: str = "compatibility",
    ) -> "RunRecorder":
        """Create the run directory and write its initial manifest."""

        started = _utc_now()
        run_id = started.strftime("%Y%m%dT%H%M%S.%fZ") + "-" + _uuid.uuid4().hex[:10]
        run_dir = resolve_audit_root(audit_root) / run_id
        run_dir.mkdir(parents=True, exist_ok=False)
        manifest = RunManifest(
            schema_version=_SCHEMA_VERSION,
            run_id=run_id,
            command=command,
            status="running",
            app_name=app_name,
            app_target=app_target,
            environment=environment,
            db_path=str(db_path) if db_path is not None else None,
            started_at=_utc_text(started),
            finished_at=None,
            duration_ms=None,
            synor_version=_synor_version,
            python_version=_platform.python_version(),
            policy=_typing.cast(dict[str, _typing.Any], redact_metadata(dict(policy))),
            options=_typing.cast(
                dict[str, _typing.Any], redact_metadata(dict(options or {}))
            ),
            execution_guarantee=execution_guarantee,
        )
        recorder = cls(run_dir, manifest, started)
        recorder._write_manifest()
        recorder.record("run_started")
        return recorder

    def _write_manifest(self) -> None:
        temporary = self.manifest_path.with_name(
            f".{self.manifest_path.name}.{_uuid.uuid4().hex}.tmp"
        )
        temporary.write_text(
            _json.dumps(self.manifest.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _os.replace(temporary, self.manifest_path)

    def record(self, event: str, **fields: _typing.Any) -> None:
        """Append a metadata-only audit event."""

        item = {
            "schema_version": _SCHEMA_VERSION,
            "timestamp": _utc_text(),
            "run_id": self.run_id,
            "event": event,
            **fields,
        }
        safe = redact_metadata(item)
        with self.audit_path.open("a", encoding="utf-8") as stream:
            stream.write(_json.dumps(safe, sort_keys=True) + "\n")

    def record_policy_decision(self, event: dict[str, _typing.Any]) -> None:
        """Audit callback accepted by :func:`synor.policy.policy_scope`."""

        self.record("policy_decision", **event)

    def finish(
        self,
        *,
        status: str,
        action_count: int | None = None,
        artifact_count: int | None = None,
        replay_digest: str | None = None,
        stats: _typing.Any = None,
        error: BaseException | None = None,
        revocations: _typing.Mapping[str, _typing.Any] | None = None,
    ) -> None:
        """Finalize the run manifest."""

        if status not in {
            "succeeded",
            "succeeded_with_open_revocations",
            "degraded",
            "failed",
            "cancelled",
        }:
            raise ValueError("status is not a supported execution status")
        finished = _utc_now()
        self.manifest.status = status
        self.manifest.finished_at = _utc_text(finished)
        self.manifest.duration_ms = round(
            (finished - self._started_at).total_seconds() * 1000
        )
        self.manifest.action_count = action_count
        self.manifest.artifact_count = artifact_count
        self.manifest.replay_digest = replay_digest
        self.manifest.stats = redact_metadata(stats)
        self.manifest.revocations = (
            _typing.cast(
                dict[str, _typing.Any],
                redact_metadata(dict(revocations)),
            )
            if revocations is not None
            else None
        )
        self.manifest.error_type = (
            f"{type(error).__module__}.{type(error).__qualname__}"
            if error is not None
            else None
        )
        self.record(
            "run_finished",
            status=status,
            action_count=action_count,
            artifact_count=artifact_count,
            error_type=self.manifest.error_type,
            revocations=self.manifest.revocations,
        )
        self._write_manifest()


def read_run_manifest(
    path: _os.PathLike[str] | str,
) -> dict[str, _typing.Any]:
    """Read and minimally validate one manifest."""

    manifest_path = _pathlib.Path(path)
    if manifest_path.is_dir():
        manifest_path = manifest_path / "manifest.json"
    data = _json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("schema_version") != _SCHEMA_VERSION:
        raise ValueError(f"unsupported Synor run manifest: {manifest_path}")
    return _typing.cast(dict[str, _typing.Any], data)


def latest_run_manifest(
    *,
    audit_root: _os.PathLike[str] | str | None = None,
    app_name: str | None = None,
) -> dict[str, _typing.Any] | None:
    """Return the newest readable manifest, optionally filtered by app."""

    root = resolve_audit_root(audit_root)
    if not root.is_dir():
        return None
    for run_dir in sorted(
        (path for path in root.iterdir() if path.is_dir()), reverse=True
    ):
        manifest_path = run_dir / "manifest.json"
        if not manifest_path.is_file():
            continue
        try:
            manifest = read_run_manifest(manifest_path)
        except (OSError, ValueError, _json.JSONDecodeError):
            continue
        if app_name is None or manifest.get("app_name") == app_name:
            return manifest
    return None
