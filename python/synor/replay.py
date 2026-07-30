"""Deterministic preview replay envelopes and verification."""

from __future__ import annotations

import dataclasses as _dataclasses
import json as _json
import os as _os
import pathlib as _pathlib
import platform as _platform
import typing as _typing
import uuid as _uuid

from . import audit as _audit
from . import provenance as _provenance
from . import state as _state
from ._version import __version__ as _synor_version

__all__ = [
    "ReplayEnvelope",
    "ReplayVerification",
    "build_replay_envelope",
    "load_replay_envelope",
    "store_replay_envelope",
    "verify_replay",
    "write_replay_envelope",
]

_SCHEMA_VERSION = 1


@_dataclasses.dataclass(frozen=True, slots=True)
class ReplayEnvelope:
    """Evidence required to reproduce and verify a preview."""

    source_run_id: str
    app_name: str
    app_target: str
    source_digest: str | None
    dependency_digest: str | None
    action_digest: str
    action_count: int
    synor_version: str
    python_version: str
    options: dict[str, _typing.Any]
    policy: dict[str, _typing.Any]

    def to_dict(self) -> dict[str, _typing.Any]:
        """Return redacted JSON data."""

        return _typing.cast(
            dict[str, _typing.Any],
            _audit.redact_metadata(
                {
                    "schema_version": _SCHEMA_VERSION,
                    **_dataclasses.asdict(self),
                }
            ),
        )

    @classmethod
    def from_dict(cls, value: _typing.Mapping[str, _typing.Any]) -> "ReplayEnvelope":
        """Validate and decode one envelope."""

        if value.get("schema_version") != _SCHEMA_VERSION:
            raise ValueError("unsupported replay envelope")
        options = value.get("options")
        policy = value.get("policy")
        if not isinstance(options, dict) or not isinstance(policy, dict):
            raise ValueError("invalid replay options or policy")
        return cls(
            source_run_id=str(value["source_run_id"]),
            app_name=str(value["app_name"]),
            app_target=str(value["app_target"]),
            source_digest=(
                str(value["source_digest"])
                if value.get("source_digest") is not None
                else None
            ),
            dependency_digest=(
                str(value["dependency_digest"])
                if value.get("dependency_digest") is not None
                else None
            ),
            action_digest=str(value["action_digest"]),
            action_count=int(value["action_count"]),
            synor_version=str(value["synor_version"]),
            python_version=str(value["python_version"]),
            options=_typing.cast(dict[str, _typing.Any], options),
            policy=_typing.cast(dict[str, _typing.Any], policy),
        )


@_dataclasses.dataclass(frozen=True, slots=True)
class ReplayVerification:
    """Result of comparing a captured preview with a new preview."""

    matched: bool
    source_matched: bool
    dependencies_matched: bool
    actions_matched: bool
    policy_matched: bool
    runtime_matched: bool
    expected_action_digest: str
    actual_action_digest: str
    expected_action_count: int
    actual_action_count: int


def _change_payload(changes: _typing.Iterable[_typing.Any]) -> list[_typing.Any]:
    payload: list[_typing.Any] = []
    for change in changes:
        if _dataclasses.is_dataclass(change) and not isinstance(change, type):
            payload.append(_dataclasses.asdict(change))
        else:
            payload.append(change)
    return payload


def build_replay_envelope(
    *,
    run_id: str,
    app_name: str,
    app_target: str,
    changes: _typing.Iterable[_typing.Any],
    options: _typing.Mapping[str, _typing.Any],
    policy: _typing.Mapping[str, _typing.Any],
) -> ReplayEnvelope:
    """Create a replay envelope from a successful preview."""

    change_items = _change_payload(changes)
    return ReplayEnvelope(
        source_run_id=run_id,
        app_name=app_name,
        app_target=app_target,
        source_digest=_provenance.pipeline_source_digest(app_target),
        dependency_digest=_provenance.pipeline_dependency_digest(app_target),
        action_digest=_provenance.canonical_digest(change_items),
        action_count=len(change_items),
        synor_version=_synor_version,
        python_version=_platform.python_version(),
        options=_typing.cast(
            dict[str, _typing.Any],
            _audit.redact_metadata(dict(options)),
        ),
        policy=_typing.cast(
            dict[str, _typing.Any],
            _audit.redact_metadata(dict(policy)),
        ),
    )


def write_replay_envelope(
    run_dir: _os.PathLike[str] | str,
    envelope: ReplayEnvelope,
) -> _pathlib.Path:
    """Atomically write ``replay.json`` beside a run manifest."""

    path = _pathlib.Path(run_dir) / "replay.json"
    temporary = path.with_name(f".{path.name}.{_uuid.uuid4().hex}.tmp")
    temporary.write_text(
        _json.dumps(envelope.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _os.replace(temporary, path)
    return path


async def store_replay_envelope(
    store: _state.StateStore,
    envelope: ReplayEnvelope,
) -> None:
    """Persist an envelope through a pluggable state store."""

    await store.put(
        f"runs/{envelope.source_run_id}/replay.json",
        (_json.dumps(envelope.to_dict(), sort_keys=True) + "\n").encode(),
    )


def load_replay_envelope(path: _os.PathLike[str] | str) -> ReplayEnvelope:
    """Read one replay envelope from a file or run directory."""

    replay_path = _pathlib.Path(path)
    if replay_path.is_dir():
        replay_path = replay_path / "replay.json"
    value = _json.loads(replay_path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("invalid replay envelope")
    return ReplayEnvelope.from_dict(value)


def verify_replay(
    envelope: ReplayEnvelope,
    *,
    app_target: str,
    changes: _typing.Iterable[_typing.Any],
    policy: _typing.Mapping[str, _typing.Any],
) -> ReplayVerification:
    """Verify source and preview digests without applying changes."""

    change_items = _change_payload(changes)
    actual_action_digest = _provenance.canonical_digest(change_items)
    actual_source_digest = _provenance.pipeline_source_digest(app_target)
    actual_dependency_digest = _provenance.pipeline_dependency_digest(app_target)
    source_matched = envelope.source_digest == actual_source_digest
    dependencies_matched = envelope.dependency_digest == actual_dependency_digest
    actions_matched = envelope.action_digest == actual_action_digest
    policy_matched = _provenance.canonical_digest(
        envelope.policy
    ) == _provenance.canonical_digest(dict(policy))
    runtime_matched = (
        envelope.synor_version == _synor_version
        and envelope.python_version == _platform.python_version()
    )
    return ReplayVerification(
        matched=source_matched
        and dependencies_matched
        and actions_matched
        and policy_matched
        and runtime_matched,
        source_matched=source_matched,
        dependencies_matched=dependencies_matched,
        actions_matched=actions_matched,
        policy_matched=policy_matched,
        runtime_matched=runtime_matched,
        expected_action_digest=envelope.action_digest,
        actual_action_digest=actual_action_digest,
        expected_action_count=envelope.action_count,
        actual_action_count=len(change_items),
    )
