"""Artifact-level ownership provenance for controlled Synor runs."""

from __future__ import annotations

import dataclasses as _dataclasses
import datetime as _datetime
import hashlib as _hashlib
import json as _json
import os as _os
import pathlib as _pathlib
import typing as _typing
import uuid as _uuid

from . import audit as _audit
from . import inspect as _inspect
from . import packaging as _packaging
from . import state as _state
from ._internal import app as _app

__all__ = [
    "ArtifactProvenance",
    "canonical_digest",
    "capture_artifact_provenance",
    "pipeline_source_digest",
    "pipeline_dependency_digest",
    "store_artifact_provenance",
    "write_artifact_provenance",
]

_SCHEMA_VERSION = 1


def _utc_text() -> str:
    return (
        _datetime.datetime.now(_datetime.timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def canonical_digest(value: _typing.Any) -> str:
    """Hash a redacted value using deterministic canonical JSON."""

    safe = _audit.redact_metadata(value)
    payload = _json.dumps(
        safe,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return _hashlib.sha256(payload).hexdigest()


def _pipeline_lock(app_target: str | None) -> _packaging.PipelineLock | None:
    if app_target is None:
        return None
    try:
        return _packaging.build_pipeline_lock(app_target)
    except (OSError, ValueError):
        return None


def pipeline_source_digest(app_target: str | None) -> str | None:
    """Hash selected local pipeline source files when APP_TARGET is a path."""

    lock = _pipeline_lock(app_target)
    if lock is None:
        return None
    return canonical_digest([_dataclasses.asdict(item) for item in lock.files])


def pipeline_dependency_digest(app_target: str | None) -> str | None:
    """Hash installed direct dependency versions for a local pipeline."""

    lock = _pipeline_lock(app_target)
    if lock is None:
        return None
    return canonical_digest(lock.distributions)


@_dataclasses.dataclass(frozen=True, slots=True)
class ArtifactProvenance:
    """Ownership and execution evidence for one tracked target state."""

    artifact_id: str
    run_id: str
    app_name: str
    target_state_path: str
    fingerprint_path: str
    owner_component_path: str
    dangling: bool
    recorded_at: str
    pipeline_digest: str | None
    package_digest: str | None = None

    def to_dict(self) -> dict[str, _typing.Any]:
        """Return a metadata-only record."""

        return _typing.cast(
            dict[str, _typing.Any],
            _audit.redact_metadata(
                {
                    "schema_version": _SCHEMA_VERSION,
                    **_dataclasses.asdict(self),
                }
            ),
        )


async def capture_artifact_provenance(
    app: _app.App[_typing.Any, _typing.Any],
    *,
    run_id: str,
    app_target: str | None,
    package_digest: str | None = None,
) -> tuple[ArtifactProvenance, ...]:
    """Capture target-state ownership after a successful apply."""

    source_digest = pipeline_source_digest(app_target)
    records: list[ArtifactProvenance] = []
    async for entry in _inspect.iter_target_states(app):
        identity = f"{app._name}\0{entry.fingerprint_path}".encode()
        records.append(
            ArtifactProvenance(
                artifact_id=_hashlib.sha256(identity).hexdigest(),
                run_id=run_id,
                app_name=app._name,
                target_state_path=entry.readable_path,
                fingerprint_path=entry.fingerprint_path,
                owner_component_path=str(entry.owner_component_path),
                dangling=entry.dangling,
                recorded_at=_utc_text(),
                pipeline_digest=source_digest,
                package_digest=package_digest,
            )
        )
    return tuple(records)


def write_artifact_provenance(
    run_dir: _os.PathLike[str] | str,
    records: _typing.Iterable[ArtifactProvenance],
) -> _pathlib.Path:
    """Atomically write metadata-only JSONL provenance for a run."""

    path = _pathlib.Path(run_dir) / "provenance.jsonl"
    temporary = path.with_name(f".{path.name}.{_uuid.uuid4().hex}.tmp")
    items = tuple(records)
    temporary.write_text(
        "".join(_json.dumps(item.to_dict(), sort_keys=True) + "\n" for item in items),
        encoding="utf-8",
    )
    _os.replace(temporary, path)
    return path


async def store_artifact_provenance(
    store: _state.StateStore,
    records: _typing.Iterable[ArtifactProvenance],
) -> None:
    """Persist provenance through a pluggable state store."""

    for item in records:
        payload = (_json.dumps(item.to_dict(), sort_keys=True) + "\n").encode()
        await store.put(
            f"runs/{item.run_id}/provenance/{item.artifact_id}.json",
            payload,
        )
