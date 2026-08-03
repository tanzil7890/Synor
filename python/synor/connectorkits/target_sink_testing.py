"""Failure-injection certification helpers for target connector sinks.

This module is intentionally independent of pytest so connector packages can
reuse the same checks in their own test suites.
"""

from __future__ import annotations

import asyncio as _asyncio
import dataclasses as _dataclasses
import typing as _typing

from synor._internal import target_state as _target_state

__all__ = [
    "TargetSinkCertificationError",
    "TargetSinkCertificationReport",
    "TargetSinkCertificationScenario",
    "certify_target_sink",
]

_ActionT = _typing.TypeVar("_ActionT")
_SnapshotT = _typing.TypeVar("_SnapshotT")
_CompletionEvidence = _typing.Literal["unverified", "acknowledged", "query_verified"]


class _MissingType:
    __slots__ = ()


_MISSING = _MissingType()


class TargetSinkCertificationError(AssertionError):
    """Raised when observed sink behavior contradicts its capability contract."""


@_dataclasses.dataclass(frozen=True, slots=True)
class TargetSinkCertificationReport:
    """The checks completed for a certification scenario."""

    scenario_name: str
    checks: tuple[str, ...]


@_dataclasses.dataclass(frozen=True, slots=True)
class TargetSinkCertificationScenario(_typing.Generic[_ActionT, _SnapshotT]):
    """Black-box hooks used to certify one target sink implementation.

    ``apply_with_failure_after`` must fail after exactly the requested number
    of externally completed actions. ``apply_segmented_with_failure_after``
    must durably complete the requested number of bounded segments and then
    fail, so certification can replay the complete action set. ``cancel_apply``
    must arrange a real cancellation while the sink is inside external I/O and
    re-raise :class:`asyncio.CancelledError`. Hooks are required only for
    guarantees the capability contract actually claims.
    """

    name: str
    capabilities: _target_state.TargetSinkCapabilities
    actions: tuple[_ActionT, ...]
    reset: _typing.Callable[[], _typing.Awaitable[None]]
    apply: _typing.Callable[[tuple[_ActionT, ...]], _typing.Awaitable[None]]
    snapshot: _typing.Callable[[], _typing.Awaitable[_SnapshotT]]
    expected_final_snapshot: _SnapshotT
    apply_with_failure_after: (
        _typing.Callable[[tuple[_ActionT, ...], int], _typing.Awaitable[None]] | None
    ) = None
    apply_segmented_with_failure_after: (
        _typing.Callable[[tuple[_ActionT, ...], int], _typing.Awaitable[None]] | None
    ) = None
    expected_failure_snapshot: _SnapshotT | _MissingType = _MISSING
    cancel_apply: (
        _typing.Callable[[tuple[_ActionT, ...]], _typing.Awaitable[None]] | None
    ) = None
    observed_order: (
        _typing.Callable[[], _typing.Awaitable[tuple[object, ...]]] | None
    ) = None
    expected_order: tuple[object, ...] | None = None
    observed_batches: (
        _typing.Callable[[], _typing.Awaitable[tuple[tuple[_ActionT, ...], ...]]] | None
    ) = None
    action_size_bytes: _typing.Callable[[_ActionT], int] | None = None
    completion_evidence: (
        _typing.Callable[[], _typing.Awaitable[_CompletionEvidence]] | None
    ) = None
    apply_with_completion_failure: (
        _typing.Callable[[tuple[_ActionT, ...]], _typing.Awaitable[None]] | None
    ) = None


def _fail(scenario: str, message: str) -> _typing.NoReturn:
    raise TargetSinkCertificationError(f"{scenario}: {message}")


async def certify_target_sink(
    scenario: TargetSinkCertificationScenario[_ActionT, _SnapshotT],
) -> TargetSinkCertificationReport:
    """Exercise and validate every non-unknown guarantee a sink declares.

    The function performs a successful apply first, then conditionally checks
    replay, failure atomicity, replay after a completed segment, ordering,
    cancellation recovery, batch limits, and completion evidence. It never
    treats an omitted hook as a pass.
    """

    capabilities = scenario.capabilities
    checks: list[str] = []

    await scenario.reset()
    baseline = await scenario.snapshot()
    await scenario.apply(scenario.actions)
    if await scenario.snapshot() != scenario.expected_final_snapshot:
        _fail(scenario.name, "successful apply produced the wrong external state")
    checks.append("success")

    if capabilities.idempotent_replay == "supported":
        await scenario.apply(scenario.actions)
        if await scenario.snapshot() != scenario.expected_final_snapshot:
            _fail(
                scenario.name, "replaying an acknowledged batch changed external state"
            )
        checks.append("idempotent_replay")

    if capabilities.batch_atomicity in {"per_action", "per_apply"}:
        if scenario.apply_with_failure_after is None:
            _fail(
                scenario.name, "atomicity is claimed without a failure-injection hook"
            )
        await scenario.reset()
        failed = False
        try:
            await scenario.apply_with_failure_after(scenario.actions, 1)
        except Exception:  # noqa: BLE001 - connector failures are intentionally generic
            failed = True
        if not failed:
            _fail(scenario.name, "failure injection did not fail the sink")
        failure_snapshot = await scenario.snapshot()
        if capabilities.batch_atomicity == "per_apply":
            expected_failure_snapshot = baseline
        else:
            if isinstance(scenario.expected_failure_snapshot, _MissingType):
                _fail(
                    scenario.name,
                    "per-action atomicity needs the expected durable prefix snapshot",
                )
            expected_failure_snapshot = scenario.expected_failure_snapshot
        if failure_snapshot != expected_failure_snapshot:
            _fail(
                scenario.name, "failure atomicity does not match the declared boundary"
            )
        checks.append("failure_atomicity")

    if capabilities.segmented_replay_safe == "supported":
        if scenario.apply_segmented_with_failure_after is None:
            _fail(
                scenario.name,
                "replay-safe segmentation is claimed without a segment-failure hook",
            )
        await scenario.reset()
        failed = False
        try:
            await scenario.apply_segmented_with_failure_after(scenario.actions, 1)
        except Exception:  # noqa: BLE001 - connector failures are intentionally generic
            failed = True
        if not failed:
            _fail(scenario.name, "segment failure injection did not fail the sink")
        await scenario.apply(scenario.actions)
        if await scenario.snapshot() != scenario.expected_final_snapshot:
            _fail(
                scenario.name,
                "full replay after a completed segment did not recover external state",
            )
        checks.append("segmented_replay")

    if capabilities.apply_ordering == "input_order":
        if scenario.observed_order is None or scenario.expected_order is None:
            _fail(scenario.name, "input ordering is claimed without ordering evidence")
        await scenario.reset()
        await scenario.apply(scenario.actions)
        if await scenario.observed_order() != scenario.expected_order:
            _fail(
                scenario.name,
                "externally visible actions were not applied in input order",
            )
        checks.append("input_order")

    if capabilities.cancellation_safe == "supported":
        if scenario.cancel_apply is None:
            _fail(
                scenario.name,
                "cancellation safety is claimed without a cancellation-injection hook",
            )
        await scenario.reset()
        cancelled = False
        try:
            await scenario.cancel_apply(scenario.actions)
        except _asyncio.CancelledError:
            cancelled = True
        if not cancelled:
            _fail(scenario.name, "cancellation injection did not cancel the sink")
        await scenario.apply(scenario.actions)
        if await scenario.snapshot() != scenario.expected_final_snapshot:
            _fail(
                scenario.name, "retry after cancellation did not recover external state"
            )
        checks.append("cancellation_recovery")

    if (
        capabilities.max_batch_actions is not None
        or capabilities.max_batch_bytes is not None
    ):
        if scenario.observed_batches is None:
            _fail(
                scenario.name,
                "batch limits are claimed without observed engine batches",
            )
        batches = await scenario.observed_batches()
        if capabilities.max_batch_actions is not None and any(
            len(batch) > capabilities.max_batch_actions for batch in batches
        ):
            _fail(
                scenario.name, "engine delivered more actions than the declared limit"
            )
        if capabilities.max_batch_bytes is not None:
            if scenario.action_size_bytes is None:
                _fail(
                    scenario.name,
                    "byte limit is claimed without an action size function",
                )
            for batch in batches:
                batch_bytes = sum(
                    scenario.action_size_bytes(action) for action in batch
                )
                if batch_bytes > capabilities.max_batch_bytes:
                    _fail(
                        scenario.name,
                        "engine delivered more bytes than the declared limit",
                    )
        checks.append("batch_limits")

    if capabilities.completion_verification not in {"unknown", "unverified"}:
        if scenario.completion_evidence is None:
            _fail(
                scenario.name,
                "completion verification is claimed without provider evidence",
            )
        await scenario.reset()
        completion_failure = scenario.apply_with_completion_failure
        if completion_failure is None and scenario.apply_with_failure_after is not None:

            async def completion_failure(
                actions: tuple[_ActionT, ...],
            ) -> None:
                assert scenario.apply_with_failure_after is not None
                await scenario.apply_with_failure_after(actions, 1)

        if completion_failure is None:
            _fail(
                scenario.name,
                "completion verification is claimed without a provider-failure hook",
            )
        provider_failed = False
        try:
            await completion_failure(scenario.actions)
        except Exception:  # noqa: BLE001 - provider failures are intentionally generic
            provider_failed = True
        if not provider_failed:
            _fail(scenario.name, "provider failure was rewritten into sink success")
        await scenario.reset()
        await scenario.apply(scenario.actions)
        observed = await scenario.completion_evidence()
        evidence_rank = {
            "unverified": 0,
            "acknowledged": 1,
            "query_verified": 2,
        }
        required = capabilities.completion_verification
        if evidence_rank[observed] < evidence_rank[required]:
            _fail(
                scenario.name,
                "completion evidence is weaker than the declared contract",
            )
        checks.append("completion_verification")

    return TargetSinkCertificationReport(
        scenario_name=scenario.name,
        checks=tuple(checks),
    )
