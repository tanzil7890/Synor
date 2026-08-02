from __future__ import annotations

import hashlib
import pathlib
from collections.abc import Awaitable, Callable, Collection, Iterator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast

import pytest
import synor as syn
import synor._internal.deadline as deadline_module
import synor._internal.target_state as target_state_module
import synor._internal.verified_sink as verified_sink_module
import synor.inspect as synor_inspect
from synor._internal import core as synor_core
from synor._internal.context_keys import ContextProvider
from synor._internal.revocation_model import (
    EffectDescriptor,
    EffectOperation,
    VerificationOutcome,
)
from synor._internal.revocation_policy import RevocationPolicy
from synor._internal.target_state import TargetActionSink, TargetReconcileOutput
from synor._internal.verified_sink import (
    TargetActionApplyError,
    TargetActionDescriptionError,
    TargetVerificationError,
    TargetVerificationOutcome,
    TargetVerificationProtocolError,
    TargetVerificationRecorderError,
    TargetVerificationResult,
    VerificationProtocolCode,
    VerificationRetryPolicy,
    VerifiedTargetActionSink,
)

from tests import common

_PLANTED_SECRET = "raw-payload customer@example.test bearer-secret"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class _Action:
    action_id: str
    secret_payload: str = _PLANTED_SECRET

    def __synor_effect_descriptor__(self) -> EffectDescriptor:
        return EffectDescriptor(
            action_id=self.action_id,
            operation_kind=EffectOperation.DELETE,
            source_digest=_digest(f"source-{self.action_id}"),
            source_generation=1,
            target_locator_digest=_digest(f"target-{self.action_id}"),
        )


@dataclass(frozen=True, slots=True)
class _OperationAction:
    action_id: str
    operation: EffectOperation

    def __synor_effect_descriptor__(self) -> EffectDescriptor:
        return EffectDescriptor(
            action_id=self.action_id,
            operation_kind=self.operation,
            source_digest=_digest(f"source-{self.action_id}"),
            source_generation=1,
            target_locator_digest=_digest(f"target-{self.action_id}"),
        )


class _InvalidVerificationResult:
    def __repr__(self) -> str:
        return _PLANTED_SECRET


@dataclass(slots=True)
class _VerifiedPreviewHarness:
    app: syn.App[[], None]
    action: _Action
    applied_batches: list[tuple[_Action, ...]]
    verified_batches: list[tuple[_Action, ...]]
    recorded_outcomes: list[tuple[TargetVerificationOutcome, ...]]


def _make_verified_preview_harness(
    name: str,
    *,
    verification_outcome: VerificationOutcome = VerificationOutcome.ABSENT,
    describe_action: Callable[[_Action], EffectDescriptor] | None = None,
    persist_tracking_record: bool = True,
) -> _VerifiedPreviewHarness:
    action = _Action(f"{name}-action")
    applied_batches: list[tuple[_Action, ...]] = []
    verified_batches: list[tuple[_Action, ...]] = []
    recorded_outcomes: list[tuple[TargetVerificationOutcome, ...]] = []

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        del context_provider
        applied_batches.append(tuple(actions))

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, applied
        verified_batches.append(tuple(actions))
        return [TargetVerificationResult(verification_outcome) for _ in actions]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded_outcomes.append(tuple(outcomes))

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        describe_action=describe_action,
        policy=_test_policy(max_attempts=1),
    )

    class Handler:
        def reconcile(
            self,
            key: syn.StableKey,
            desired_state: str | syn.AbsentType,
            prev_possible_records: Collection[str],
            prev_may_be_missing: bool,
            /,
        ) -> syn.TargetReconcileOutput[_Action, str] | None:
            del key, prev_possible_records, prev_may_be_missing
            if syn.is_absent(desired_state):
                return None
            return syn.TargetReconcileOutput(
                action=action,
                sink=verified.sink,
                tracking_record=(
                    desired_state if persist_tracking_record else syn.ABSENT
                ),
            )

    provider = syn.register_root_target_states_provider(
        f"test/revocation/verified_sink/preview_lineage/{name}",
        Handler(),
    )

    async def main() -> None:
        syn.ensure_target_state(provider.target_state("artifact", "owned"))

    environment = common.create_test_env(
        __file__,
        suffix=f"verified_sink_preview_lineage_{name}",
    )
    app = syn.App(
        syn.AppConfig(
            name=f"test_verified_sink_preview_lineage_{name}",
            environment=environment,
        ),
        main,
    )
    return _VerifiedPreviewHarness(
        app=app,
        action=action,
        applied_batches=applied_batches,
        verified_batches=verified_batches,
        recorded_outcomes=recorded_outcomes,
    )


def _test_policy(
    *,
    max_attempts: int = 3,
    timeout: timedelta = timedelta(seconds=1),
) -> VerificationRetryPolicy:
    return VerificationRetryPolicy(
        timeout=timeout,
        max_attempts=max_attempts,
        initial_backoff=0,
        max_backoff=0,
        jitter=0,
    )


async def _assert_protocol_failure(
    result_factory: Callable[[], Sequence[TargetVerificationResult]],
    *,
    expected_code: VerificationProtocolCode,
    actions: Sequence[_Action] | None = None,
) -> None:
    record_called = False
    actions_to_verify = (
        (_Action("a-1"), _Action("a-2")) if actions is None else tuple(actions)
    )

    async def apply(
        context_provider: ContextProvider,
        action_batch: Sequence[_Action],
        /,
    ) -> None:
        del context_provider, action_batch

    async def verify(
        context_provider: ContextProvider,
        action_batch: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, action_batch, applied
        return result_factory()

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        nonlocal record_called
        del context_provider, outcomes
        record_called = True

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(max_attempts=1),
    )

    with pytest.raises(TargetVerificationProtocolError) as raised:
        await verified(ContextProvider(), actions_to_verify)

    assert raised.value.code is expected_code
    assert not record_called
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert _PLANTED_SECRET not in str(raised.value)
    assert _PLANTED_SECRET not in repr(raised.value)


@pytest.mark.asyncio
async def test_apply_verify_record_order_and_explicit_id_correlation() -> None:
    events: list[str] = []
    recorded: list[tuple[TargetVerificationOutcome, ...]] = []
    applied_result: list[None] = [None, None]

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> list[None]:
        del context_provider
        events.append("apply:" + ",".join(action.action_id for action in actions))
        return applied_result

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: list[None],
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions
        assert applied is applied_result
        events.append("verify")
        # Explicit stable IDs allow a backend to return results out of order.
        return [
            TargetVerificationResult(VerificationOutcome.ABSENT, action_id="a-2"),
            TargetVerificationResult(VerificationOutcome.ABSENT, action_id="a-1"),
        ]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        events.append("record")
        recorded.append(tuple(outcomes))

    verified = VerifiedTargetActionSink[_Action, list[None]](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(),
    )
    actions = [_Action("a-1"), _Action("a-2")]

    result = await verified(ContextProvider(), actions)

    assert result is applied_result
    assert events == ["apply:a-1,a-2", "verify", "record"]
    assert [outcome.action_id for outcome in recorded[0]] == ["a-1", "a-2"]
    assert all(
        outcome.required_postcondition_holds
        and outcome.attempt_count == 1
        and outcome.source_generation == 1
        for outcome in recorded[0]
    )
    assert isinstance(verified.sink, TargetActionSink)


def test_verified_wrapper_preserves_four_field_reconciliation_contract() -> None:
    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        del context_provider, actions

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        return [TargetVerificationResult(VerificationOutcome.ABSENT)]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider, outcomes

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(),
    )
    output = TargetReconcileOutput(
        action=_Action("a-1"),
        sink=verified.sink,
        tracking_record="tracking",
    )

    assert output._fields == (
        "action",
        "sink",
        "tracking_record",
        "child_invalidation",
    )
    assert len(output) == 4
    assert output.child_invalidation is None


def test_arbitrary_callback_cannot_claim_verified_core_assurance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert not hasattr(synor_core.TargetActionSink, "new_verified_async")

    class FakeVerifiedSink:
        async def __call__(self, *args: object) -> None:
            del args

        def _describe_for_core(self, action: object) -> object:
            del action
            return _PLANTED_SECRET

    with pytest.raises(
        TypeError,
        match="must be created by VerifiedTargetActionSink",
    ) as raised:
        synor_core.TargetActionSink._new_verified_wrapper(FakeVerifiedSink())

    assert _PLANTED_SECRET not in str(raised.value)
    assert _PLANTED_SECRET not in repr(raised.value)

    monkeypatch.setattr(
        verified_sink_module,
        "VerifiedTargetActionSink",
        FakeVerifiedSink,
    )
    with pytest.raises(
        TypeError,
        match="must be created by VerifiedTargetActionSink",
    ):
        synor_core.TargetActionSink._new_verified_wrapper(FakeVerifiedSink())


@pytest.mark.asyncio
async def test_strict_core_sanitizes_native_descriptor_failure_before_apply() -> None:
    apply_calls = 0

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        nonlocal apply_calls
        del context_provider, actions
        apply_calls += 1

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        raise AssertionError("verification must not run")

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider, outcomes
        raise AssertionError("recording must not run")

    def describe(action: _Action, /) -> EffectDescriptor:
        del action
        raise RuntimeError(_PLANTED_SECRET)

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        describe_action=describe,
        policy=_test_policy(max_attempts=1),
    )

    class Handler:
        def reconcile(
            self,
            key: syn.StableKey,
            desired_state: str | syn.AbsentType,
            prev_possible_records: Collection[str],
            prev_may_be_missing: bool,
            /,
        ) -> syn.TargetReconcileOutput[_Action, str] | None:
            del key, prev_possible_records, prev_may_be_missing
            if syn.is_absent(desired_state):
                return None
            return syn.TargetReconcileOutput(
                action=_Action("native-descriptor-failure"),
                sink=verified.sink,
                tracking_record=desired_state,
            )

    provider = syn.register_root_target_states_provider(
        "test/revocation/verified_sink/native_descriptor_failure",
        Handler(),
    )

    async def main() -> None:
        syn.ensure_target_state(provider.target_state("artifact", "owned"))

    environment = common.create_test_env(
        __file__,
        suffix="verified_sink_native_descriptor_failure",
    )
    app = syn.App(
        syn.AppConfig(
            name="test_verified_sink_native_descriptor_failure",
            environment=environment,
        ),
        main,
    )

    with pytest.raises(
        ValueError,
        match="verified target effect descriptor is invalid",
    ) as raised:
        await app._update_controlled(_strict_effects=True)

    assert apply_calls == 0
    assert _PLANTED_SECRET not in str(raised.value)
    assert _PLANTED_SECRET not in repr(raised.value)


@pytest.mark.asyncio
async def test_core_sink_failure_preserves_effect_for_verified_retry() -> None:
    target_is_present = True
    apply_calls = 0
    verify_calls = 0
    previous_states: list[tuple[str, ...]] = []
    recorded: list[tuple[TargetVerificationOutcome, ...]] = []

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        nonlocal apply_calls
        del context_provider, actions
        # Synthetic false success: apply returns while the target remains
        # present. Verification, not the apply acknowledgement, decides.
        apply_calls += 1

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        nonlocal verify_calls
        del context_provider, actions, applied
        verify_calls += 1
        return [
            TargetVerificationResult(
                VerificationOutcome.PRESENT
                if target_is_present
                else VerificationOutcome.ABSENT
            )
        ]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.append(tuple(outcomes))

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(max_attempts=1),
    )

    class Handler:
        def reconcile(
            self,
            key: syn.StableKey,
            desired_state: str | syn.AbsentType,
            prev_possible_records: Collection[str],
            prev_may_be_missing: bool,
            /,
        ) -> syn.TargetReconcileOutput[_Action, str] | None:
            del key, prev_may_be_missing
            if syn.is_absent(desired_state):
                return None
            previous_states.append(tuple(prev_possible_records))
            return syn.TargetReconcileOutput(
                action=_Action("core-action"),
                sink=verified.sink,
                tracking_record=desired_state,
            )

    provider = syn.register_root_target_states_provider(
        "test/revocation/verified_sink/core_retry",
        Handler(),
    )

    async def main() -> None:
        syn.ensure_target_state(provider.target_state("artifact", "owned"))

    environment = common.create_test_env(__file__, suffix="verified_sink_core_retry")
    app = syn.App(
        syn.AppConfig(
            name="test_verified_sink_core_retry",
            environment=environment,
        ),
        main,
    )

    with pytest.raises(TargetVerificationError):
        await app.update()

    # The failed strict callback must not let the engine forget the effect.
    failed_counts = await synor_inspect.native_effect_counts(app)
    assert (
        failed_counts.pending,
        failed_counts.verified,
        failed_counts.failed,
        failed_counts.blocked,
        failed_counts.completed,
    ) == (0, 0, 1, 0, 0)

    target_is_present = False
    await app.update()

    completed_counts = await synor_inspect.native_effect_counts(app)
    assert (
        completed_counts.pending,
        completed_counts.verified,
        completed_counts.failed,
        completed_counts.blocked,
        completed_counts.completed,
    ) == (0, 0, 0, 0, 1)
    assert apply_calls == 2
    assert verify_calls == 2
    assert [batch[0].status for batch in recorded] == [
        VerificationOutcome.PRESENT,
        VerificationOutcome.ABSENT,
    ]
    assert previous_states[0] == ()
    assert "owned" in previous_states[1]


@pytest.mark.asyncio
async def test_strict_runtime_verified_delete_finalizes_without_dangling_owner(
    tmp_path: pathlib.Path,
) -> None:
    should_declare = True
    target_is_present = False
    recorded: list[tuple[TargetVerificationOutcome, ...]] = []

    async def apply_upsert(
        context_provider: ContextProvider,
        actions: Sequence[str],
        /,
    ) -> None:
        nonlocal target_is_present
        del context_provider, actions
        target_is_present = True

    async def apply_delete(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        nonlocal target_is_present
        del context_provider, actions
        target_is_present = False

    async def verify_delete(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        return [
            TargetVerificationResult(
                VerificationOutcome.PRESENT
                if target_is_present
                else VerificationOutcome.ABSENT
            )
        ]

    async def record_delete(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.append(tuple(outcomes))

    upsert_sink: TargetActionSink[str, None] = TargetActionSink.from_async_fn(
        apply_upsert
    )
    delete_sink = VerifiedTargetActionSink[_Action, None](
        apply=apply_delete,
        verify=verify_delete,
        record=record_delete,
        policy=_test_policy(max_attempts=1),
    )

    class Handler:
        def reconcile(
            self,
            key: syn.StableKey,
            desired_state: str | syn.AbsentType,
            prev_possible_records: Collection[str],
            prev_may_be_missing: bool,
            /,
        ) -> syn.TargetReconcileOutput[Any, str] | None:
            del key
            if syn.is_absent(desired_state):
                return syn.TargetReconcileOutput(
                    action=_Action("strict-native-delete"),
                    sink=delete_sink.sink,
                    tracking_record=syn.ABSENT,
                )
            if desired_state in prev_possible_records and not prev_may_be_missing:
                return None
            return syn.TargetReconcileOutput(
                action="upsert",
                sink=upsert_sink,
                tracking_record=desired_state,
            )

    provider = syn.register_root_target_states_provider(
        "test/revocation/verified_sink/strict_native_delete",
        Handler(),
    )

    @syn.task
    def build() -> None:
        if should_declare:
            syn.ensure_target_state(provider.target_state("artifact", "owned"))

    environment = common.create_test_env(
        __file__,
        suffix="verified_sink_strict_native_delete",
    )
    app = syn.App(
        syn.AppConfig(
            name="test_verified_sink_strict_native_delete",
            environment=environment,
        ),
        build,
    )
    runtime = syn.SynorRuntime(
        state_store=syn.MemoryStateStore(),
        revocation_policy=RevocationPolicy.strict_query_verified(),
        audit_dir=tmp_path / "runs",
    )

    await runtime.run(app)
    assert target_is_present
    initial_counts = await synor_inspect.native_effect_counts(app)
    assert (
        initial_counts.pending,
        initial_counts.verified,
        initial_counts.failed,
        initial_counts.blocked,
        initial_counts.completed,
    ) == (0, 0, 0, 0, 0)

    should_declare = False
    await runtime.run(app)

    assert not target_is_present
    assert recorded[0][0].status is VerificationOutcome.ABSENT
    completed_counts = await synor_inspect.native_effect_counts(app)
    assert (
        completed_counts.pending,
        completed_counts.verified,
        completed_counts.failed,
        completed_counts.blocked,
        completed_counts.completed,
    ) == (0, 0, 0, 0, 1)
    assert [entry async for entry in synor_inspect.iter_target_states(app)] == []


@pytest.mark.parametrize(
    "sink_variant",
    (
        "native",
        "rewrapped",
        "flag_mutation",
        "subclass_override",
        "class_method_monkeypatch",
    ),
)
@pytest.mark.asyncio
async def test_core_and_receipt_share_one_bound_descriptor_instance(
    sink_variant: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    action = _Action("raw-action-id")
    describe_calls = 0
    applied_actions: list[_Action] = []
    recorded: list[tuple[TargetVerificationOutcome, ...]] = []

    def describe(candidate: _Action, /) -> EffectDescriptor:
        nonlocal describe_calls
        assert candidate is action
        describe_calls += 1
        suffix = "first" if describe_calls == 1 else "divergent"
        return EffectDescriptor(
            action_id=f"bound-descriptor-{suffix}",
            operation_kind=EffectOperation.DELETE,
            source_digest=_digest(f"source-{suffix}"),
            source_generation=describe_calls,
            target_locator_digest=_digest(f"target-{suffix}"),
        )

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        del context_provider
        applied_actions.extend(actions)

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        return [TargetVerificationResult(VerificationOutcome.ABSENT)]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.append(tuple(outcomes))

    if sink_variant == "class_method_monkeypatch":

        async def forged_apply_method(*args: object) -> None:
            del args
            raise AssertionError("mutable class method must not become core callback")

        def forged_descriptor_method(*args: object) -> object:
            del args
            return _PLANTED_SECRET

        monkeypatch.setattr(
            VerifiedTargetActionSink,
            "_call_bound_for_core",
            forged_apply_method,
        )
        monkeypatch.setattr(
            VerifiedTargetActionSink,
            "_describe_for_core",
            forged_descriptor_method,
        )

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        describe_action=describe,
        policy=_test_policy(max_attempts=1),
    )

    if sink_variant in {"native", "class_method_monkeypatch"}:
        adversarial_sink = verified.sink
    elif sink_variant == "rewrapped":
        adversarial_sink = TargetActionSink(verified.sink._core)
    elif sink_variant == "flag_mutation":

        class FlagMutationSink(TargetActionSink[_Action, Any]):
            pass

        adversarial_sink = FlagMutationSink(verified.sink._core)
        adversarial_sink._bind_verified_actions = False  # type: ignore[attr-defined]
    else:

        class OverrideSink(TargetActionSink[_Action, Any]):
            def _bind_action_for_core(self, candidate: _Action) -> _Action:
                return candidate

        adversarial_sink = OverrideSink(verified.sink._core)

    class Handler:
        def reconcile(
            self,
            key: syn.StableKey,
            desired_state: str | syn.AbsentType,
            prev_possible_records: Collection[str],
            prev_may_be_missing: bool,
            /,
        ) -> syn.TargetReconcileOutput[_Action, str] | None:
            del key, prev_possible_records, prev_may_be_missing
            if syn.is_absent(desired_state):
                return None
            return syn.TargetReconcileOutput(
                action=action,
                sink=adversarial_sink,
                tracking_record=desired_state,
            )

    provider = syn.register_root_target_states_provider(
        f"test/revocation/verified_sink/bound_descriptor/{sink_variant}",
        Handler(),
    )

    async def main() -> None:
        syn.ensure_target_state(provider.target_state("artifact", "owned"))

    environment = common.create_test_env(
        __file__,
        suffix=f"verified_sink_bound_descriptor_{sink_variant}",
    )
    app = syn.App(
        syn.AppConfig(
            name=f"test_verified_sink_bound_descriptor_{sink_variant}",
            environment=environment,
        ),
        main,
    )

    await app._update_controlled(_strict_effects=True)

    counts = await synor_inspect.native_effect_counts(app)
    assert (
        counts.pending,
        counts.verified,
        counts.failed,
        counts.blocked,
        counts.completed,
    ) == (0, 0, 0, 0, 1)
    assert describe_calls == 1
    assert applied_actions == [action]
    assert len(recorded) == 1
    assert recorded[0][0].action_id == "bound-descriptor-first"
    assert recorded[0][0].source_digest == _digest("source-first")
    assert recorded[0][0].source_generation == 1
    assert recorded[0][0].target_locator_digest == _digest("target-first")


@pytest.mark.asyncio
async def test_verified_preview_returns_the_original_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    action = _Action("preview-raw-action")
    describe_calls = 0

    def describe(candidate: _Action, /) -> EffectDescriptor:
        nonlocal describe_calls
        assert candidate is action
        describe_calls += 1
        return candidate.__synor_effect_descriptor__()

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        del context_provider, actions
        raise AssertionError("preview must not apply actions")

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        raise AssertionError("preview must not verify actions")

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider, outcomes
        raise AssertionError("preview must not record actions")

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        describe_action=describe,
        policy=_test_policy(max_attempts=1),
    )

    class Handler:
        def reconcile(
            self,
            key: syn.StableKey,
            desired_state: str | syn.AbsentType,
            prev_possible_records: Collection[str],
            prev_may_be_missing: bool,
            /,
        ) -> syn.TargetReconcileOutput[_Action, str] | None:
            del key, prev_possible_records, prev_may_be_missing
            if syn.is_absent(desired_state):
                return None
            return syn.TargetReconcileOutput(
                action=action,
                sink=verified.sink,
                tracking_record=desired_state,
            )

    provider = syn.register_root_target_states_provider(
        "test/revocation/verified_sink/preview_original_action",
        Handler(),
    )

    async def main() -> None:
        syn.ensure_target_state(provider.target_state("artifact", "owned"))

    environment = common.create_test_env(
        __file__,
        suffix="verified_sink_preview_original_action",
    )
    app = syn.App(
        syn.AppConfig(
            name="test_verified_sink_preview_original_action",
            environment=environment,
        ),
        main,
    )

    captured_core_actions: list[object] = []
    unwrap = target_state_module._unwrap_target_action

    def capture_and_unwrap(core_action: object) -> object:
        captured_core_actions.append(core_action)
        return unwrap(core_action)

    monkeypatch.setattr(
        target_state_module,
        "_unwrap_target_action",
        capture_and_unwrap,
    )
    preview = await app._update_controlled(
        preview=True,
        _strict_effects=True,
    )

    assert len(preview) == 1
    assert preview[0] is action
    assert len(captured_core_actions) == 1
    carrier = captured_core_actions[0]
    assert carrier is not action
    assert synor_core._unwrap_target_action(carrier) is action
    assert not hasattr(carrier, "descriptor")
    with pytest.raises((AttributeError, TypeError)):
        cast(Any, carrier).descriptor = (_PLANTED_SECRET,)
    with pytest.raises(TypeError):
        cast(Any, type(carrier))(action)
    assert describe_calls == 1
    counts = await synor_inspect.native_effect_counts(app)
    assert (
        counts.pending,
        counts.verified,
        counts.failed,
        counts.blocked,
        counts.completed,
    ) == (0, 0, 0, 0, 0)


@pytest.mark.asyncio
async def test_strict_verified_preview_then_real_update_applies_once() -> None:
    harness = _make_verified_preview_harness("preview_then_apply")

    preview = await harness.app._update_controlled(
        preview=True,
        _strict_effects=True,
    )

    assert preview == [harness.action]
    assert harness.applied_batches == []
    assert harness.verified_batches == []
    assert harness.recorded_outcomes == []
    preview_counts = await synor_inspect.native_effect_counts(harness.app)
    assert (
        preview_counts.pending,
        preview_counts.verified,
        preview_counts.failed,
        preview_counts.blocked,
        preview_counts.completed,
    ) == (0, 0, 0, 0, 0)

    await harness.app._update_controlled(_strict_effects=True)

    assert harness.applied_batches == [(harness.action,)]
    assert harness.verified_batches == [(harness.action,)]
    assert len(harness.recorded_outcomes) == 1
    assert harness.recorded_outcomes[0][0].status is VerificationOutcome.ABSENT
    completed_counts = await synor_inspect.native_effect_counts(harness.app)
    assert (
        completed_counts.pending,
        completed_counts.verified,
        completed_counts.failed,
        completed_counts.blocked,
        completed_counts.completed,
    ) == (0, 0, 0, 0, 1)


@pytest.mark.asyncio
async def test_new_verified_action_without_tracking_record_is_rejected_in_preview_and_update() -> (
    None
):
    harness = _make_verified_preview_harness(
        "new_action_without_tracking",
        persist_tracking_record=False,
    )

    for preview in (True, False):
        with pytest.raises(
            ValueError,
            match="a new verified target action must persist a tracking record",
        ):
            await harness.app._update_controlled(
                preview=preview,
                _strict_effects=True,
            )

        assert harness.applied_batches == []
        assert harness.verified_batches == []
        assert harness.recorded_outcomes == []
        counts = await synor_inspect.native_effect_counts(harness.app)
        assert (
            counts.pending,
            counts.verified,
            counts.failed,
            counts.blocked,
            counts.completed,
        ) == (0, 0, 0, 0, 0)
        assert [
            entry async for entry in synor_inspect.iter_target_states(harness.app)
        ] == []


@pytest.mark.asyncio
async def test_two_strict_verified_previews_remain_side_effect_free() -> None:
    harness = _make_verified_preview_harness("two_previews")

    first = await harness.app._update_controlled(
        preview=True,
        _strict_effects=True,
    )
    second = await harness.app._update_controlled(
        preview=True,
        _strict_effects=True,
    )

    assert first == [harness.action]
    assert second == [harness.action]
    assert harness.applied_batches == []
    assert harness.verified_batches == []
    assert harness.recorded_outcomes == []
    counts = await synor_inspect.native_effect_counts(harness.app)
    assert (
        counts.pending,
        counts.verified,
        counts.failed,
        counts.blocked,
        counts.completed,
    ) == (0, 0, 0, 0, 0)
    assert [
        entry async for entry in synor_inspect.iter_target_states(harness.app)
    ] == []


@pytest.mark.asyncio
async def test_strict_preview_rejects_changed_unresolved_proof_descriptor() -> None:
    proof_generation = 1

    def describe(action: _Action, /) -> EffectDescriptor:
        return EffectDescriptor(
            action_id=action.action_id,
            operation_kind=EffectOperation.DELETE,
            source_digest=_digest(f"preview-proof-source-{proof_generation}"),
            source_generation=proof_generation,
            target_locator_digest=_digest("preview-proof-target"),
        )

    harness = _make_verified_preview_harness(
        "unresolved_proof_drift",
        verification_outcome=VerificationOutcome.PRESENT,
        describe_action=describe,
    )

    with pytest.raises(TargetVerificationError):
        await harness.app._update_controlled(_strict_effects=True)

    failed_counts = await synor_inspect.native_effect_counts(harness.app)
    assert (
        failed_counts.pending,
        failed_counts.verified,
        failed_counts.failed,
        failed_counts.blocked,
        failed_counts.completed,
    ) == (0, 0, 1, 0, 0)
    proof_generation = 2

    with pytest.raises(
        ValueError,
        match="native effect retry changed its persisted proof contract",
    ):
        await harness.app._update_controlled(
            preview=True,
            _strict_effects=True,
        )

    assert harness.applied_batches == [(harness.action,)]
    assert harness.verified_batches == [(harness.action,)]
    assert len(harness.recorded_outcomes) == 1
    retained_counts = await synor_inspect.native_effect_counts(harness.app)
    assert (
        retained_counts.pending,
        retained_counts.verified,
        retained_counts.failed,
        retained_counts.blocked,
        retained_counts.completed,
    ) == (0, 0, 1, 0, 0)


@pytest.mark.asyncio
async def test_native_effect_lmdb_excludes_action_and_remote_error_payloads() -> None:
    raw_sentinels = (
        "native-content-sentinel-90c483",
        "native-principal-sentinel-a741de",
        "native-credential-sentinel-38be12",
        "native-raw-locator-sentinel-e2d510",
        "native-remote-error-sentinel-1f95aa",
    )
    source_digest = _digest(raw_sentinels[0])
    locator_digest = _digest(raw_sentinels[3])
    action = _Action(
        "native-privacy-scan",
        secret_payload="|".join(raw_sentinels[:-1]),
    )

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        del context_provider, actions
        raise RuntimeError(raw_sentinels[-1])

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        raise AssertionError("verification must not run after apply failure")

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider, outcomes
        raise AssertionError("recording must not run after apply failure")

    def describe_action(value: _Action) -> EffectDescriptor:
        assert value is action
        return EffectDescriptor(
            action_id=value.action_id,
            operation_kind=EffectOperation.DELETE,
            source_digest=source_digest,
            source_generation=1,
            target_locator_digest=locator_digest,
        )

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        describe_action=describe_action,
        policy=_test_policy(max_attempts=1),
    )

    class Handler:
        def reconcile(
            self,
            key: syn.StableKey,
            desired_state: str | syn.AbsentType,
            prev_possible_records: Collection[str],
            prev_may_be_missing: bool,
            /,
        ) -> syn.TargetReconcileOutput[_Action, str] | None:
            del key, prev_possible_records, prev_may_be_missing
            if syn.is_absent(desired_state):
                return None
            return syn.TargetReconcileOutput(
                action=action,
                sink=verified.sink,
                tracking_record=desired_state,
            )

    provider = syn.register_root_target_states_provider(
        "test/revocation/verified_sink/native_privacy_scan",
        Handler(),
    )

    async def main() -> None:
        syn.ensure_target_state(provider.target_state("artifact", "owned"))

    environment = common.create_test_env(
        __file__,
        suffix="verified_sink_native_privacy_scan",
    )
    app = syn.App(
        syn.AppConfig(
            name="test_verified_sink_native_privacy_scan",
            environment=environment,
        ),
        main,
    )

    with pytest.raises(TargetActionApplyError):
        await app.update()

    configured_db_path = environment.settings.db_path
    assert configured_db_path is not None
    db_path = pathlib.Path(configured_db_path)
    persisted = b"".join(
        path.read_bytes() for path in db_path.rglob("*") if path.is_file()
    )
    assert action.action_id.encode() in persisted
    assert source_digest.encode() in persisted
    assert locator_digest.encode() in persisted
    for sentinel in raw_sentinels:
        assert sentinel.encode() not in persisted


@pytest.mark.asyncio
async def test_core_retries_lost_apply_response_without_repeating_effect() -> None:
    external_target_present = True
    apply_calls = 0
    effective_delete_count = 0
    reconcile_history: list[tuple[tuple[str, ...], bool]] = []
    recorded: list[tuple[TargetVerificationOutcome, ...]] = []

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        nonlocal external_target_present, apply_calls, effective_delete_count
        del context_provider, actions
        apply_calls += 1
        if external_target_present:
            external_target_present = False
            effective_delete_count += 1
        if apply_calls == 1:
            # The remote delete committed, but its response was lost.  The
            # engine must retry the effect, and connector idempotency must
            # make that retry a no-op instead of a second mutation.
            raise RuntimeError(_PLANTED_SECRET)

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        return [
            TargetVerificationResult(
                VerificationOutcome.PRESENT
                if external_target_present
                else VerificationOutcome.ABSENT
            )
        ]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.append(tuple(outcomes))

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(max_attempts=1),
    )

    class Handler:
        def reconcile(
            self,
            key: syn.StableKey,
            desired_state: str | syn.AbsentType,
            prev_possible_records: Collection[str],
            prev_may_be_missing: bool,
            /,
        ) -> syn.TargetReconcileOutput[_Action, str] | None:
            del key
            if syn.is_absent(desired_state):
                return None
            previous = tuple(prev_possible_records)
            reconcile_history.append((previous, prev_may_be_missing))
            if desired_state in previous and not prev_may_be_missing:
                return None
            return syn.TargetReconcileOutput(
                action=_Action("lost-response-action"),
                sink=verified.sink,
                tracking_record=desired_state,
            )

    provider = syn.register_root_target_states_provider(
        "test/revocation/verified_sink/lost_apply_response",
        Handler(),
    )

    async def main() -> None:
        syn.ensure_target_state(provider.target_state("artifact", "owned"))

    environment = common.create_test_env(
        __file__,
        suffix="verified_sink_lost_apply_response",
    )
    app = syn.App(
        syn.AppConfig(
            name="test_verified_sink_lost_apply_response",
            environment=environment,
        ),
        main,
    )

    with pytest.raises(TargetActionApplyError) as raised:
        await app.update()
    assert _PLANTED_SECRET not in str(raised.value)

    await app.update()
    await app.update()

    assert not external_target_present
    assert apply_calls == 2
    assert effective_delete_count == 1
    assert len(recorded) == 1
    assert recorded[0][0].status is VerificationOutcome.ABSENT
    assert reconcile_history[0][0] == ()
    assert reconcile_history[1] == (("owned",), True)
    assert reconcile_history[2] == (("owned",), False)


@pytest.mark.asyncio
async def test_positional_results_retry_until_every_action_is_verified() -> None:
    verify_calls = 0
    recorded: list[tuple[TargetVerificationOutcome, ...]] = []

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        del context_provider, actions

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        nonlocal verify_calls
        del context_provider, actions, applied
        verify_calls += 1
        if verify_calls == 1:
            return [
                TargetVerificationResult(VerificationOutcome.PRESENT),
                TargetVerificationResult(VerificationOutcome.WRONG_ACL),
            ]
        return [
            TargetVerificationResult(VerificationOutcome.ABSENT),
            TargetVerificationResult(VerificationOutcome.ABSENT),
        ]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.append(tuple(outcomes))

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(max_attempts=2),
    )

    await verified(ContextProvider(), [_Action("a-1"), _Action("a-2")])

    assert verify_calls == 2
    assert len(recorded) == 1
    assert [outcome.action_id for outcome in recorded[0]] == ["a-1", "a-2"]
    assert {outcome.attempt_count for outcome in recorded[0]} == {2}
    assert {outcome.status for outcome in recorded[0]} == {VerificationOutcome.ABSENT}


@pytest.mark.asyncio
async def test_attempt_bound_records_failure_then_raises_without_payload() -> None:
    apply_calls = 0
    verify_calls = 0
    recorded: list[tuple[TargetVerificationOutcome, ...]] = []

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        nonlocal apply_calls
        del context_provider, actions
        apply_calls += 1

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        nonlocal verify_calls
        del context_provider, actions, applied
        verify_calls += 1
        return [
            TargetVerificationResult(
                VerificationOutcome.PRESENT,
                detail_code="replica_stale",
            )
        ]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.append(tuple(outcomes))

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(max_attempts=3),
    )

    with pytest.raises(TargetVerificationError) as raised:
        await verified(ContextProvider(), [_Action("safe-action")])

    assert apply_calls == 1
    assert verify_calls == 3
    assert len(recorded) == 1
    assert recorded[0][0].status is VerificationOutcome.PRESENT
    assert recorded[0][0].attempt_count == 3
    assert recorded[0][0].detail_code == "replica_stale"
    assert raised.value.outcomes == recorded[0]
    assert _PLANTED_SECRET not in str(raised.value)
    assert "present=1" in str(raised.value)


@pytest.mark.asyncio
async def test_verifier_exception_becomes_safe_transport_failure() -> None:
    verify_calls = 0
    recorded: list[tuple[TargetVerificationOutcome, ...]] = []

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        del context_provider, actions

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        nonlocal verify_calls
        del context_provider, actions, applied
        verify_calls += 1
        raise RuntimeError(_PLANTED_SECRET)

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.append(tuple(outcomes))

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(max_attempts=2),
    )

    with pytest.raises(TargetVerificationError) as raised:
        await verified(ContextProvider(), [_Action("safe-action")])

    assert verify_calls == 2
    assert recorded[0][0].status is VerificationOutcome.TRANSPORT_FAILURE
    assert recorded[0][0].detail_code == "verifier_exception"
    assert _PLANTED_SECRET not in str(raised.value)


@pytest.mark.asyncio
async def test_unsupported_is_terminal_and_distinct() -> None:
    verify_calls = 0
    recorded: list[tuple[TargetVerificationOutcome, ...]] = []

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        del context_provider, actions

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        nonlocal verify_calls
        del context_provider, actions, applied
        verify_calls += 1
        return [TargetVerificationResult(VerificationOutcome.UNSUPPORTED)]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.append(tuple(outcomes))

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(max_attempts=10),
    )

    with pytest.raises(TargetVerificationError) as raised:
        await verified(ContextProvider(), [_Action("safe-action")])

    assert verify_calls == 1
    assert recorded[0][0].status is VerificationOutcome.UNSUPPORTED
    assert "unsupported=1" in str(raised.value)


@pytest.mark.parametrize(
    ("operation", "observation", "succeeds"),
    [
        (EffectOperation.DELETE, VerificationOutcome.ABSENT, True),
        (
            EffectOperation.DELETE,
            VerificationOutcome.RETAINED_ISOLATED,
            False,
        ),
        (
            EffectOperation.ISOLATE,
            VerificationOutcome.RETAINED_ISOLATED,
            True,
        ),
        (EffectOperation.ISOLATE, VerificationOutcome.ABSENT, False),
    ],
)
@pytest.mark.asyncio
async def test_success_outcome_must_match_revocation_operation(
    operation: EffectOperation,
    observation: VerificationOutcome,
    succeeds: bool,
) -> None:
    recorded: list[tuple[TargetVerificationOutcome, ...]] = []

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_OperationAction],
        /,
    ) -> None:
        del context_provider, actions

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_OperationAction],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        return [TargetVerificationResult(observation)]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.append(tuple(outcomes))

    verified = VerifiedTargetActionSink[_OperationAction, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(max_attempts=1),
    )
    call = verified(ContextProvider(), [_OperationAction("operation-1", operation)])

    if succeeds:
        await call
    else:
        with pytest.raises(TargetVerificationError):
            await call

    assert recorded[0][0].required_postcondition_holds is succeeds


@pytest.mark.asyncio
async def test_non_revocation_effect_is_blocked_before_apply() -> None:
    apply_called = False

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_OperationAction],
        /,
    ) -> None:
        nonlocal apply_called
        del context_provider, actions
        apply_called = True

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_OperationAction],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        return [TargetVerificationResult(VerificationOutcome.ABSENT)]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider, outcomes

    verified = VerifiedTargetActionSink[_OperationAction, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(),
    )

    with pytest.raises(TargetActionDescriptionError) as raised:
        await verified(
            ContextProvider(),
            [_OperationAction("operation-1", EffectOperation.UPDATE)],
        )

    assert not apply_called
    assert raised.value.code is VerificationProtocolCode.ACTION_NOT_REVOCATION


@pytest.mark.asyncio
async def test_total_deadline_turns_unfinished_readback_into_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: list[tuple[TargetVerificationOutcome, ...]] = []

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        del context_provider, actions

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        return [TargetVerificationResult(VerificationOutcome.PRESENT)]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider
        recorded.append(tuple(outcomes))

    async def expire_after_first_attempt(
        fn: Callable[[], Awaitable[tuple[TargetVerificationOutcome, ...]]],
        **kwargs: Any,
    ) -> tuple[TargetVerificationOutcome, ...]:
        del kwargs
        with pytest.raises(Exception):
            await fn()
        raise deadline_module.DeadlineExceededError("deterministic test deadline")

    monkeypatch.setattr(
        deadline_module,
        "retry_transient",
        expire_after_first_attempt,
    )
    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(
            max_attempts=100,
            timeout=timedelta(seconds=1),
        ),
    )

    with pytest.raises(TargetVerificationError) as raised:
        await verified(ContextProvider(), [_Action("safe-action")])

    assert recorded[0][0].status is VerificationOutcome.TIMEOUT
    assert recorded[0][0].attempt_count == 1
    assert "timeout=1" in str(raised.value)


@pytest.mark.asyncio
async def test_recorder_failure_is_strict_failure_and_is_redacted() -> None:
    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        del context_provider, actions

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        return [TargetVerificationResult(VerificationOutcome.ABSENT)]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider, outcomes
        raise RuntimeError(_PLANTED_SECRET)

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(),
    )

    with pytest.raises(TargetVerificationRecorderError) as raised:
        await verified(ContextProvider(), [_Action("safe-action")])

    assert raised.value.outcomes[0].required_postcondition_holds
    assert _PLANTED_SECRET not in str(raised.value)
    assert raised.value.__context__ is None
    assert raised.value.__cause__ is None


@pytest.mark.asyncio
async def test_apply_failure_is_typed_redacted_and_skips_verify_record() -> None:
    verify_called = False
    record_called = False

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        del context_provider, actions
        raise RuntimeError(_PLANTED_SECRET)

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        nonlocal verify_called
        del context_provider, actions, applied
        verify_called = True
        return [TargetVerificationResult(VerificationOutcome.ABSENT)]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        nonlocal record_called
        del context_provider, outcomes
        record_called = True

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(),
    )

    with pytest.raises(TargetActionApplyError) as raised:
        await verified(ContextProvider(), [_Action("safe-action")])

    assert not verify_called
    assert not record_called
    assert _PLANTED_SECRET not in str(raised.value)
    assert raised.value.__context__ is None
    assert raised.value.__cause__ is None


@pytest.mark.asyncio
async def test_descriptor_callback_failure_has_no_secret_exception_chain() -> None:
    apply_called = False

    @dataclass(frozen=True, slots=True)
    class UnsafeAction:
        def __synor_effect_descriptor__(self) -> EffectDescriptor:
            raise RuntimeError(_PLANTED_SECRET)

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[UnsafeAction],
        /,
    ) -> None:
        nonlocal apply_called
        del context_provider, actions
        apply_called = True

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[UnsafeAction],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        return []

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider, outcomes

    verified = VerifiedTargetActionSink[UnsafeAction, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(),
    )

    with pytest.raises(TargetActionDescriptionError) as raised:
        await verified(ContextProvider(), [UnsafeAction()])

    assert not apply_called
    assert raised.value.code is VerificationProtocolCode.ACTION_DESCRIPTOR_INVALID
    assert _PLANTED_SECRET not in str(raised.value)
    assert raised.value.__context__ is None
    assert raised.value.__cause__ is None


@pytest.mark.asyncio
async def test_descriptor_subclass_cannot_expose_property_exception() -> None:
    apply_called = False

    class UnsafeDescriptor(EffectDescriptor):
        @property
        def destructive(self) -> bool:
            raise RuntimeError(_PLANTED_SECRET)

    descriptor = UnsafeDescriptor(
        action_id="safe-action",
        operation_kind=EffectOperation.DELETE,
        source_digest=_digest("source"),
        source_generation=1,
        target_locator_digest=_digest("target"),
    )

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        nonlocal apply_called
        del context_provider, actions
        apply_called = True

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        return []

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider, outcomes

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        describe_action=lambda action: descriptor,
        policy=_test_policy(),
    )

    with pytest.raises(TargetActionDescriptionError) as raised:
        await verified(ContextProvider(), [_Action("safe-action")])

    assert not apply_called
    assert raised.value.code is VerificationProtocolCode.ACTION_DESCRIPTOR_INVALID
    assert _PLANTED_SECRET not in str(raised.value)
    assert raised.value.__context__ is None
    assert raised.value.__cause__ is None


@pytest.mark.asyncio
async def test_invalid_descriptor_is_rejected_before_apply_without_repr() -> None:
    apply_called = False

    @dataclass(frozen=True, slots=True)
    class UnsafeAction:
        def __synor_effect_descriptor__(self) -> EffectDescriptor:
            return EffectDescriptor(
                action_id="raw customer@example.test",
                operation_kind=EffectOperation.DELETE,
                source_digest=_digest("source"),
                source_generation=1,
                target_locator_digest=_digest("target"),
            )

        def __repr__(self) -> str:
            return _PLANTED_SECRET

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[UnsafeAction],
        /,
    ) -> None:
        nonlocal apply_called
        del context_provider, actions
        apply_called = True

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[UnsafeAction],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        return []

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider, outcomes

    verified = VerifiedTargetActionSink[UnsafeAction, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(),
    )

    with pytest.raises(TargetActionDescriptionError) as raised:
        await verified(ContextProvider(), [UnsafeAction()])

    assert not apply_called
    assert raised.value.code is VerificationProtocolCode.ACTION_DESCRIPTOR_INVALID
    assert _PLANTED_SECRET not in str(raised.value)


@pytest.mark.asyncio
async def test_result_count_or_id_mismatch_is_typed_protocol_failure() -> None:
    recorded = False

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        del context_provider, actions

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        # Positional mode requires exactly one result per action.
        return [TargetVerificationResult(VerificationOutcome.ABSENT)]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        nonlocal recorded
        del context_provider, outcomes
        recorded = True

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(),
    )

    with pytest.raises(TargetVerificationProtocolError) as raised:
        await verified(ContextProvider(), [_Action("a-1"), _Action("a-2")])

    assert raised.value.code is VerificationProtocolCode.RESULT_COUNT_MISMATCH
    assert not recorded


@pytest.mark.parametrize(
    ("results", "expected_code"),
    [
        pytest.param(
            [
                TargetVerificationResult(
                    VerificationOutcome.ABSENT,
                    action_id="a-1",
                )
            ],
            VerificationProtocolCode.RESULT_ID_MISMATCH,
            id="missing-expected-id",
        ),
        pytest.param(
            [
                TargetVerificationResult(
                    VerificationOutcome.ABSENT,
                    action_id="a-1",
                ),
                TargetVerificationResult(
                    VerificationOutcome.ABSENT,
                    action_id="unknown-action",
                ),
            ],
            VerificationProtocolCode.RESULT_ID_MISMATCH,
            id="unknown-id",
        ),
        pytest.param(
            [
                TargetVerificationResult(
                    VerificationOutcome.ABSENT,
                    action_id="a-1",
                ),
                TargetVerificationResult(
                    VerificationOutcome.ABSENT,
                    action_id="a-1",
                ),
            ],
            VerificationProtocolCode.RESULT_ID_DUPLICATE,
            id="duplicate-id",
        ),
        pytest.param(
            [
                TargetVerificationResult(
                    VerificationOutcome.ABSENT,
                    action_id="a-1",
                ),
                TargetVerificationResult(VerificationOutcome.ABSENT),
            ],
            VerificationProtocolCode.RESULT_CORRELATION_MIXED,
            id="mixed-positional-and-id",
        ),
    ],
)
@pytest.mark.asyncio
async def test_explicit_result_correlation_rejects_incomplete_or_ambiguous_sets(
    results: Sequence[TargetVerificationResult],
    expected_code: VerificationProtocolCode,
) -> None:
    await _assert_protocol_failure(
        lambda: results,
        expected_code=expected_code,
    )


@pytest.mark.asyncio
async def test_duplicate_action_descriptors_fail_before_external_callbacks() -> None:
    apply_called = False
    verify_called = False
    record_called = False

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        nonlocal apply_called
        del context_provider, actions
        apply_called = True

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        nonlocal verify_called
        del context_provider, actions, applied
        verify_called = True
        return []

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        nonlocal record_called
        del context_provider, outcomes
        record_called = True

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(max_attempts=1),
    )

    with pytest.raises(TargetActionDescriptionError) as raised:
        await verified(
            ContextProvider(),
            [_Action("duplicate-action"), _Action("duplicate-action")],
        )

    assert raised.value.code is VerificationProtocolCode.DUPLICATE_ACTION_ID
    assert not apply_called
    assert not verify_called
    assert not record_called
    assert _PLANTED_SECRET not in str(raised.value)


@pytest.mark.parametrize(
    "result",
    [
        pytest.param(
            cast(TargetVerificationResult, _InvalidVerificationResult()),
            id="invalid-object",
        ),
        pytest.param(
            TargetVerificationResult(
                cast(VerificationOutcome, _PLANTED_SECRET),
            ),
            id="invalid-status",
        ),
        pytest.param(
            TargetVerificationResult(
                VerificationOutcome.PRESENT,
                detail_code=_PLANTED_SECRET,
            ),
            id="invalid-detail",
        ),
        pytest.param(
            TargetVerificationResult(
                VerificationOutcome.PRESENT,
                operation_id=_PLANTED_SECRET,
            ),
            id="invalid-operation-id",
        ),
        pytest.param(
            TargetVerificationResult(
                VerificationOutcome.PRESENT,
                affected_count=-1,
            ),
            id="invalid-affected-count",
        ),
    ],
)
@pytest.mark.asyncio
async def test_invalid_result_object_status_or_detail_is_redacted(
    result: TargetVerificationResult,
) -> None:
    await _assert_protocol_failure(
        lambda: [result],
        expected_code=VerificationProtocolCode.RESULT_INVALID,
        actions=[_Action("a-1")],
    )


@pytest.mark.parametrize("yield_valid_result_first", [False, True])
@pytest.mark.asyncio
async def test_lazy_result_iteration_failure_is_typed_and_redacted(
    yield_valid_result_first: bool,
) -> None:
    def result_factory() -> Sequence[TargetVerificationResult]:
        def iterate() -> Iterator[TargetVerificationResult]:
            if yield_valid_result_first:
                yield TargetVerificationResult(VerificationOutcome.ABSENT)
            raise RuntimeError(_PLANTED_SECRET)

        return cast(Sequence[TargetVerificationResult], iterate())

    await _assert_protocol_failure(
        result_factory,
        expected_code=VerificationProtocolCode.RESULT_INVALID,
        actions=[_Action("a-1")],
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_attempts": 0}, "max_attempts"),
        ({"timeout": timedelta(0)}, "timeout"),
        ({"jitter": -0.1}, "jitter"),
        ({"jitter": 1.1}, "jitter"),
        (
            {"initial_backoff": 2.0, "max_backoff": 1.0},
            "max_backoff",
        ),
    ],
)
def test_retry_policy_rejects_unbounded_or_invalid_values(
    kwargs: dict[str, Any],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        VerificationRetryPolicy(**kwargs)


def test_retry_policy_derives_shared_revocation_policy_values() -> None:
    revocation_policy = RevocationPolicy._for_test(
        verification_timeout_seconds=0.25,
        initial_backoff_seconds=0.01,
    )

    retry_policy = VerificationRetryPolicy.from_revocation_policy(
        revocation_policy,
        max_attempts=4,
        backoff_multiplier=3,
    )

    assert retry_policy == VerificationRetryPolicy(
        timeout=timedelta(milliseconds=250),
        max_attempts=4,
        initial_backoff=0.01,
        backoff_multiplier=3,
        max_backoff=0.01,
        jitter=0,
    )


@pytest.mark.asyncio
async def test_token_shaped_verifier_detail_is_rejected_before_recording() -> None:
    recorded = False

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        del context_provider, actions

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, actions, applied
        return [
            TargetVerificationResult(
                VerificationOutcome.PRESENT,
                detail_code="sk-planted-secret-token",
            )
        ]

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        nonlocal recorded
        del context_provider, outcomes
        recorded = True

    verified = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=_test_policy(max_attempts=1),
    )

    with pytest.raises(TargetVerificationProtocolError):
        await verified(ContextProvider(), [_Action("safe-action")])
    assert not recorded
