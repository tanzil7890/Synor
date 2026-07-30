"""High-level, compatibility-preserving execution API."""

from __future__ import annotations

import asyncio as _asyncio
import dataclasses as _dataclasses
import enum as _enum
import os as _os
import pathlib as _pathlib
import typing as _typing

from . import audit as _audit
from . import pii as _pii
from . import policy as _policy
from . import provenance as _provenance
from . import quarantine as _quarantine
from . import replay as _replay
from . import revocation as _revocation
from . import state as _state
from ._internal import app as _app
from . import inspect as _inspect

__all__ = [
    "AppExplanation",
    "ExecutionGuarantee",
    "ExecutionReport",
    "ExecutionStatus",
    "PlannedChange",
    "RevocationStartupError",
    "RunMode",
    "SynorRuntime",
]


class RunMode(str, _enum.Enum):
    """Execution mode used by :class:`SynorRuntime`."""

    APPLY = "apply"
    PLAN = "plan"


class ExecutionStatus(str, _enum.Enum):
    """Controlled-run outcome, including unresolved revocation state."""

    SUCCEEDED = "succeeded"
    SUCCEEDED_WITH_OPEN_REVOCATIONS = "succeeded_with_open_revocations"
    DEGRADED = "degraded"
    FAILED = "failed"


class ExecutionGuarantee(str, _enum.Enum):
    """Evidence boundary established for one controlled operation.

    ``PROVABLE_INDEX_REVOCATION_V1`` is reserved for a future declarative
    integration that can attest governed source, certified target, and guarded
    query registration together. The Phase 5 runtime emits
    ``STRICT_REVOCATION_CONTROL_V1`` because policy selection alone cannot prove
    that an arbitrary app used all three boundaries.
    """

    COMPATIBILITY = "compatibility"
    STRICT_REVOCATION_CONTROL_V1 = "strict_revocation_control_v1"
    PROVABLE_INDEX_REVOCATION_V1 = "provable_index_revocation_v1"


class RevocationStartupError(RuntimeError):
    """Raised when strict control-plane state cannot be read or repaired."""

    def __init__(self, safe_error_code: str) -> None:
        self.safe_error_code = safe_error_code
        super().__init__(f"strict revocation startup health failed: {safe_error_code}")


@_dataclasses.dataclass(frozen=True, slots=True)
class PlannedChange:
    """Redacted representation of one connector-specific target action."""

    index: int
    operation: str
    action_type: str
    details: _typing.Any


@_dataclasses.dataclass(frozen=True, slots=True)
class ExecutionReport:
    """Result of a controlled run or plan."""

    run_id: str
    mode: RunMode
    app_name: str
    output: _typing.Any
    planned_changes: tuple[PlannedChange, ...]
    stats: _typing.Any
    manifest_path: _pathlib.Path
    provenance: tuple[_provenance.ArtifactProvenance, ...] = ()
    replay_path: _pathlib.Path | None = None
    status: ExecutionStatus = ExecutionStatus.SUCCEEDED
    revocations: _revocation.RevocationSummary | None = None
    execution_guarantee: ExecutionGuarantee = ExecutionGuarantee.COMPATIBILITY


@_dataclasses.dataclass(frozen=True, slots=True)
class AppExplanation:
    """Current local evidence describing an app."""

    app_name: str
    environment: str
    db_path: str
    stable_path_count: int
    target_state_count: int
    policy: dict[str, _typing.Any]
    latest_run: dict[str, _typing.Any] | None
    revocation_health: _revocation.RevocationHealth | None = None
    execution_guarantee: ExecutionGuarantee = ExecutionGuarantee.COMPATIBILITY


def _field_value(action: _typing.Any, name: str) -> _typing.Any:
    if hasattr(action, "_fields") and name in action._fields:
        return getattr(action, name)
    if _dataclasses.is_dataclass(action) and not isinstance(action, type):
        if any(field.name == name for field in _dataclasses.fields(action)):
            return getattr(action, name)
    return None


def _infer_operation(action: _typing.Any) -> str:
    for field_name in ("operation", "action", "kind"):
        value = _field_value(action, field_name)
        if isinstance(value, str) and value.lower() in {
            "create",
            "delete",
            "insert",
            "remove",
            "replace",
            "update",
            "upsert",
        }:
            return value.lower()
    for field_name in ("content", "value", "row", "point", "document"):
        if hasattr(action, "_fields") and field_name in action._fields:
            return "delete" if getattr(action, field_name) is None else "change"
    return "change"


def _planned_changes(
    actions: _typing.Iterable[_typing.Any],
    *,
    pii_policy: _pii.PIIPolicy,
) -> tuple[PlannedChange, ...]:
    result: list[PlannedChange] = []
    for index, action in enumerate(actions, start=1):
        action_type = type(action).__qualname__.lstrip("_")
        policy_safe_action = _pii.enforce_pii(action, policy=pii_policy)
        result.append(
            PlannedChange(
                index=index,
                operation=_infer_operation(action),
                action_type=action_type,
                details=_audit.redact_metadata(policy_safe_action),
            )
        )
    return tuple(result)


class SynorRuntime:
    """Controlled execution facade over the existing :class:`synor.App`.

    The facade adds policy enforcement, typed reports, run manifests, and audit
    events. It does not replace or alter ``App.update()``.
    """

    def __init__(
        self,
        *,
        policy: _policy.EgressPolicy | None = None,
        pii_policy: _pii.PIIPolicy | None = None,
        audit_dir: _os.PathLike[str] | str | None = None,
        state_store: _state.StateStore | None = None,
        revocation_policy: _revocation.RevocationPolicy | None = None,
    ) -> None:
        self.policy = policy or _policy.policy_from_env()
        self.pii_policy = pii_policy or _pii.pii_policy_from_env()
        self.audit_dir = _pathlib.Path(audit_dir) if audit_dir is not None else None
        self.state_store = state_store
        self.revocation_policy = (
            revocation_policy or _revocation.RevocationPolicy.compatibility()
        )
        if not isinstance(self.revocation_policy, _revocation.RevocationPolicy):
            raise TypeError("revocation_policy must be a RevocationPolicy")
        if self.revocation_policy.is_strict and state_store is None:
            raise ValueError("strict revocation policy requires a state_store")
        self.execution_guarantee = (
            ExecutionGuarantee.STRICT_REVOCATION_CONTROL_V1
            if self.revocation_policy.is_strict
            else ExecutionGuarantee.COMPATIBILITY
        )
        self._revocation_repository = (
            _revocation.RevocationRepository(state_store)
            if state_store is not None
            else None
        )
        self._revocation_controller = (
            _revocation.RevocationController(
                state_store=state_store,
                policy=self.revocation_policy,
            )
            if state_store is not None and self.revocation_policy.is_strict
            else None
        )
        self._strict_blocking_call_used = False
        self._quarantine = (
            _quarantine.QuarantineRepository(state_store)
            if state_store is not None
            else None
        )

    @property
    def revocation_controller(self) -> _revocation.RevocationController:
        """Return the strict coordinator used by governed connector code."""

        if self._revocation_controller is None:
            raise RuntimeError(
                "revocation_controller requires a strict revocation policy "
                "and state_store"
            )
        return self._revocation_controller

    async def revocation_health(
        self,
        *,
        repair: bool = True,
    ) -> _revocation.RevocationHealth:
        """Validate the ledger, suppression state, deadlines, and open cases."""

        repository = self._revocation_repository
        if repository is None:
            return _revocation.RevocationHealth(
                ready=True,
                summary=_revocation.RevocationSummary(),
            )
        if repair:
            try:
                await repository.repair()
            except Exception:
                raise RevocationStartupError(
                    "revocation.ledger_repair_failed"
                ) from None
        return await repository.startup_health()

    @staticmethod
    def _execution_status(
        health: _revocation.RevocationHealth | None,
    ) -> ExecutionStatus:
        if health is None:
            return ExecutionStatus.SUCCEEDED
        summary = health.summary
        if not health.ready or summary.failed or summary.blocked or summary.overdue:
            return ExecutionStatus.DEGRADED
        if summary.open:
            return ExecutionStatus.SUCCEEDED_WITH_OPEN_REVOCATIONS
        return ExecutionStatus.SUCCEEDED

    async def _strict_preflight(self) -> _revocation.RevocationHealth | None:
        if self._revocation_repository is None:
            return None
        health = await self.revocation_health(
            repair=self.revocation_policy.is_strict,
        )
        if self.revocation_policy.is_strict and health.safe_error_code in {
            "revocation.state_corrupt",
            "revocation.state_unavailable",
        }:
            raise RevocationStartupError(
                health.safe_error_code or "revocation.state_unavailable"
            )
        return health

    async def _finalize_current_revocations(self) -> None:
        controller = self._revocation_controller
        if controller is None:
            return
        for case_id in controller.pending_finalization_case_ids():
            await controller.finalize_after_engine_commit(case_id)

    def _begin_controlled_revocation_run(self) -> None:
        if self._revocation_controller is not None:
            self._revocation_controller.begin_controlled_run()

    def _check_strict_blocking_reuse(self) -> None:
        if not self.revocation_policy.is_strict:
            return
        if self._strict_blocking_call_used:
            raise RuntimeError(
                "strict blocking runtime instances cannot be reused across "
                "event loops; run repeated controlled operations in one async loop"
            )
        self._strict_blocking_call_used = True

    async def _persist_manifest(self, recorder: _audit.RunRecorder) -> None:
        if self.state_store is None:
            return
        await self.state_store.put(
            f"runs/{recorder.run_id}/manifest.json",
            recorder.manifest_path.read_bytes(),
        )

    async def _quarantine_failure(
        self,
        *,
        error: BaseException,
        app: _app.App[_typing.Any, _typing.Any],
        app_target: str | None,
        run_id: str | None,
    ) -> None:
        if self._quarantine is None:
            return
        reason = (
            "pii_policy"
            if isinstance(error, (_pii.PIIViolation, _pii.PIIQuarantineRequired))
            else "run_failure"
        )
        await self._quarantine.create(
            reason=reason,
            app_name=app._name,
            app_target=app_target,
            run_id=run_id,
            error=error,
        )

    async def _record_failure_best_effort(
        self,
        *,
        recorder: _audit.RunRecorder,
        error: BaseException,
        app: _app.App[_typing.Any, _typing.Any],
        app_target: str | None,
    ) -> None:
        """Preserve the original controlled failure across evidence outages."""

        try:
            recorder.finish(status="failed", error=error)
        except Exception:
            pass
        try:
            await self._persist_manifest(recorder)
        except Exception as persistence_error:
            try:
                recorder.record(
                    "control_state_persistence_failed",
                    error_type=(
                        f"{type(persistence_error).__module__}."
                        f"{type(persistence_error).__qualname__}"
                    ),
                )
            except Exception:
                pass
        try:
            await self._quarantine_failure(
                error=error,
                app=app,
                app_target=app_target,
                run_id=recorder.run_id,
            )
        except Exception as quarantine_error:
            try:
                recorder.record(
                    "quarantine_persistence_failed",
                    error_type=(
                        f"{type(quarantine_error).__module__}."
                        f"{type(quarantine_error).__qualname__}"
                    ),
                )
            except Exception:
                pass

    async def run(
        self,
        app: _app.App[_typing.Any, _typing.Any],
        *,
        full_reprocess: bool = False,
        live: bool = False,
        app_target: str | None = None,
    ) -> ExecutionReport:
        """Apply an app update under policy and record local run evidence."""

        with _policy.policy_scope(self.policy), _pii.pii_scope(self.pii_policy):
            env = await app._environment._get_env()
        recorder = _audit.RunRecorder.start(
            command="run",
            app_name=app._name,
            app_target=app_target,
            environment=env.name,
            db_path=env.settings.db_path,
            policy={
                "egress": self.policy.to_dict(),
                "pii": self.pii_policy.to_dict(),
                "revocation": self.revocation_policy.to_dict(),
            },
            options={
                "full_reprocess": full_reprocess,
                "live": live,
            },
            audit_root=self.audit_dir,
            execution_guarantee=self.execution_guarantee.value,
        )
        try:
            await self._strict_preflight()
            self._begin_controlled_revocation_run()
            with (
                _policy.policy_scope(
                    self.policy, audit_sink=recorder.record_policy_decision
                ),
                _pii.pii_scope(self.pii_policy),
            ):
                if self.pii_policy.action in {
                    _pii.PIIAction.DENY,
                    _pii.PIIAction.QUARANTINE,
                }:
                    preview_handle = app.update(
                        full_reprocess=full_reprocess,
                        preview=True,
                    )
                    preview_actions = await preview_handle.result()
                    _planned_changes(
                        _typing.cast(list[_typing.Any], preview_actions),
                        pii_policy=self.pii_policy,
                    )
                    recorder.record(
                        "pii_preflight_passed",
                        action_count=len(preview_actions),
                    )
                handle = app.update(
                    full_reprocess=full_reprocess,
                    live=live,
                )
                output = await handle.result()
                stats = handle.stats()
                await self._finalize_current_revocations()
                provenance = await _provenance.capture_artifact_provenance(
                    app,
                    run_id=recorder.run_id,
                    app_target=app_target,
                )
            _provenance.write_artifact_provenance(recorder.run_dir, provenance)
            if self.state_store is not None:
                await _provenance.store_artifact_provenance(
                    self.state_store,
                    provenance,
                )
            revocation_health = await self._strict_preflight()
            revocations = (
                revocation_health.summary if revocation_health is not None else None
            )
            execution_status = self._execution_status(revocation_health)
            recorder.finish(
                status=execution_status.value,
                artifact_count=len(provenance),
                stats=stats,
                revocations=(
                    revocations.to_dict() if revocations is not None else None
                ),
            )
            await self._persist_manifest(recorder)
            return ExecutionReport(
                run_id=recorder.run_id,
                mode=RunMode.APPLY,
                app_name=app._name,
                output=output,
                planned_changes=(),
                stats=stats,
                manifest_path=recorder.manifest_path,
                provenance=provenance,
                status=execution_status,
                revocations=revocations,
                execution_guarantee=self.execution_guarantee,
            )
        except BaseException as error:
            await self._record_failure_best_effort(
                recorder=recorder,
                error=error,
                app=app,
                app_target=app_target,
            )
            raise

    async def plan(
        self,
        app: _app.App[_typing.Any, _typing.Any],
        *,
        full_reprocess: bool = False,
        app_target: str | None = None,
        command: str = "plan",
    ) -> ExecutionReport:
        """Compute target changes without applying or persisting them."""

        with _policy.policy_scope(self.policy), _pii.pii_scope(self.pii_policy):
            env = await app._environment._get_env()
        recorder = _audit.RunRecorder.start(
            command=command,
            app_name=app._name,
            app_target=app_target,
            environment=env.name,
            db_path=env.settings.db_path,
            policy={
                "egress": self.policy.to_dict(),
                "pii": self.pii_policy.to_dict(),
                "revocation": self.revocation_policy.to_dict(),
            },
            options={"full_reprocess": full_reprocess, "preview": True},
            audit_root=self.audit_dir,
            execution_guarantee=self.execution_guarantee.value,
        )
        try:
            await self._strict_preflight()
            with (
                _policy.policy_scope(
                    self.policy, audit_sink=recorder.record_policy_decision
                ),
                _pii.pii_scope(self.pii_policy),
            ):
                handle = app.update(full_reprocess=full_reprocess, preview=True)
                actions = await handle.result()
                stats = handle.stats()
            changes = _planned_changes(
                _typing.cast(list[_typing.Any], actions),
                pii_policy=self.pii_policy,
            )
            replay_path: _pathlib.Path | None = None
            replay_digest: str | None = None
            if app_target is not None:
                envelope = _replay.build_replay_envelope(
                    run_id=recorder.run_id,
                    app_name=app._name,
                    app_target=app_target,
                    changes=changes,
                    options={"full_reprocess": full_reprocess},
                    policy={
                        "egress": self.policy.to_dict(),
                        "pii": self.pii_policy.to_dict(),
                    },
                )
                replay_path = _replay.write_replay_envelope(
                    recorder.run_dir,
                    envelope,
                )
                replay_digest = _provenance.canonical_digest(envelope.to_dict())
                if self.state_store is not None:
                    await _replay.store_replay_envelope(self.state_store, envelope)
            revocation_health = await self._strict_preflight()
            revocations = (
                revocation_health.summary if revocation_health is not None else None
            )
            execution_status = self._execution_status(revocation_health)
            recorder.finish(
                status=execution_status.value,
                action_count=len(changes),
                replay_digest=replay_digest,
                stats=stats,
                revocations=(
                    revocations.to_dict() if revocations is not None else None
                ),
            )
            await self._persist_manifest(recorder)
            return ExecutionReport(
                run_id=recorder.run_id,
                mode=RunMode.PLAN,
                app_name=app._name,
                output=None,
                planned_changes=changes,
                stats=stats,
                manifest_path=recorder.manifest_path,
                replay_path=replay_path,
                status=execution_status,
                revocations=revocations,
                execution_guarantee=self.execution_guarantee,
            )
        except BaseException as error:
            await self._record_failure_best_effort(
                recorder=recorder,
                error=error,
                app=app,
                app_target=app_target,
            )
            raise

    async def explain(
        self,
        app: _app.App[_typing.Any, _typing.Any],
        *,
        app_target: str | None = None,
    ) -> AppExplanation:
        """Explain local state, ownership counts, policy, and recent evidence."""

        latest = _audit.latest_run_manifest(
            audit_root=self.audit_dir,
            app_name=app._name,
        )
        revocation_health = await self._strict_preflight()
        with _policy.policy_scope(self.policy), _pii.pii_scope(self.pii_policy):
            env = await app._environment._get_env()
            stable_path_count = 0
            async for _stable_path in _inspect.iter_stable_paths(app):
                stable_path_count += 1
            target_state_count = 0
            async for _target_state in _inspect.iter_target_states(app):
                target_state_count += 1

        recorder = _audit.RunRecorder.start(
            command="explain",
            app_name=app._name,
            app_target=app_target,
            environment=env.name,
            db_path=env.settings.db_path,
            policy={
                "egress": self.policy.to_dict(),
                "pii": self.pii_policy.to_dict(),
                "revocation": self.revocation_policy.to_dict(),
            },
            options={"read_only": True},
            audit_root=self.audit_dir,
            execution_guarantee=self.execution_guarantee.value,
        )
        execution_status = self._execution_status(revocation_health)
        recorder.finish(
            status=execution_status.value,
            revocations=(
                revocation_health.summary.to_dict()
                if revocation_health is not None
                else None
            ),
        )
        await self._persist_manifest(recorder)
        return AppExplanation(
            app_name=app._name,
            environment=env.name,
            db_path=str(env.settings.db_path),
            stable_path_count=stable_path_count,
            target_state_count=target_state_count,
            policy={
                "egress": self.policy.to_dict(),
                "pii": self.pii_policy.to_dict(),
                "revocation": self.revocation_policy.to_dict(),
            },
            latest_run=latest,
            revocation_health=revocation_health,
            execution_guarantee=self.execution_guarantee,
        )

    async def replay(
        self,
        app: _app.App[_typing.Any, _typing.Any],
        envelope: _replay.ReplayEnvelope,
        *,
        app_target: str | None = None,
    ) -> _replay.ReplayVerification:
        """Rerun preview and verify captured source/action digests."""

        selected_target = app_target or envelope.app_target
        report = await self.plan(
            app,
            full_reprocess=bool(envelope.options.get("full_reprocess", False)),
            app_target=selected_target,
            command="replay",
        )
        return _replay.verify_replay(
            envelope,
            app_target=selected_target,
            changes=report.planned_changes,
            policy={
                "egress": self.policy.to_dict(),
                "pii": self.pii_policy.to_dict(),
            },
        )

    def run_blocking(
        self,
        app: _app.App[_typing.Any, _typing.Any],
        *,
        full_reprocess: bool = False,
        live: bool = False,
        app_target: str | None = None,
    ) -> ExecutionReport:
        """Blocking wrapper for :meth:`run`."""

        self._check_strict_blocking_reuse()
        return _asyncio.run(
            self.run(
                app,
                full_reprocess=full_reprocess,
                live=live,
                app_target=app_target,
            )
        )

    def plan_blocking(
        self,
        app: _app.App[_typing.Any, _typing.Any],
        *,
        full_reprocess: bool = False,
        app_target: str | None = None,
    ) -> ExecutionReport:
        """Blocking wrapper for :meth:`plan`."""

        self._check_strict_blocking_reuse()
        return _asyncio.run(
            self.plan(
                app,
                full_reprocess=full_reprocess,
                app_target=app_target,
            )
        )
