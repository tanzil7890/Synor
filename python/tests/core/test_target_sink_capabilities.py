from __future__ import annotations

from collections.abc import Collection, Sequence
from pathlib import Path
from typing import Any, Literal

import pytest
import synor as syn


def _apply(_context: syn.ContextProvider, _actions: Sequence[str]) -> None:
    return None


def test_legacy_sink_reports_conservative_capabilities() -> None:
    sink = syn.TargetActionSink.from_fn(_apply)

    assert sink.capabilities == syn.TargetSinkCapabilities()
    assert sink.capabilities.to_dict() == {
        "schema_version": 1,
        "batch_atomicity": "unknown",
        "idempotent_replay": "unknown",
        "segmented_replay_safe": "unknown",
        "apply_ordering": "unknown",
        "cancellation_safe": "unknown",
        "completion_verification": "unknown",
        "max_batch_actions": None,
        "max_batch_bytes": None,
    }
    assert sink.queue_stats == syn.TargetSinkQueueStats(
        ongoing_batches=0,
        queued_batches=0,
        queued_inputs=0,
        in_flight_inputs=0,
        in_flight_bytes=0,
        capacity_waiters=0,
    )


def test_sink_capability_contract_round_trips_through_core() -> None:
    capabilities = syn.TargetSinkCapabilities(
        batch_atomicity="per_apply",
        idempotent_replay="supported",
        segmented_replay_safe="supported",
        apply_ordering="input_order",
        cancellation_safe="unsupported",
        completion_verification="acknowledged",
        max_batch_actions=500,
        max_batch_bytes=1024 * 1024,
    )

    sync_sink = syn.TargetActionSink.from_fn(_apply, capabilities=capabilities)

    async def apply_async(
        _context: syn.ContextProvider, _actions: Sequence[str]
    ) -> None:
        return None

    async_sink = syn.TargetActionSink.from_async_fn(
        apply_async, capabilities=capabilities
    )
    assert sync_sink.capabilities == capabilities
    assert async_sink.capabilities == capabilities


@pytest.mark.parametrize(
    "completion_verification",
    ["unknown", "unverified", "acknowledged", "query_verified"],
)
def test_completion_verification_contract_round_trips_through_core(
    completion_verification: Literal[
        "unknown", "unverified", "acknowledged", "query_verified"
    ],
) -> None:
    capabilities = syn.TargetSinkCapabilities(
        completion_verification=completion_verification
    )

    sink = syn.TargetActionSink.from_fn(_apply, capabilities=capabilities)

    assert sink.capabilities.completion_verification == completion_verification


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", 2),
        ("schema_version", True),
        ("schema_version", 1.0),
        ("batch_atomicity", "transactional"),
        ("idempotent_replay", "yes"),
        ("segmented_replay_safe", "yes"),
        ("apply_ordering", "sorted"),
        ("cancellation_safe", "yes"),
        ("completion_verification", "eventually_consistent"),
        ("max_batch_actions", 0),
        ("max_batch_actions", True),
        ("max_batch_bytes", -1),
    ],
)
def test_sink_capability_contract_rejects_invalid_values(
    field: str, value: Any
) -> None:
    with pytest.raises(ValueError):
        syn.TargetSinkCapabilities(**{field: value})


def test_segmented_replay_requires_idempotent_replay() -> None:
    with pytest.raises(ValueError, match="requires idempotent_replay"):
        syn.TargetSinkCapabilities(segmented_replay_safe="supported")


class _SegmentedStore:
    def __init__(
        self,
        max_batch_actions: int | None,
        *,
        max_batch_bytes: int | None = None,
        allow_segmentation: bool = False,
        batch_atomicity: Literal["unknown", "per_apply"] = "unknown",
    ) -> None:
        self.batches: list[list[tuple[str, str]]] = []
        self.sink = syn.TargetActionSink.from_fn(
            self._apply,
            capabilities=syn.TargetSinkCapabilities(
                batch_atomicity=batch_atomicity,
                idempotent_replay=("supported" if allow_segmentation else "unknown"),
                segmented_replay_safe=(
                    "supported" if allow_segmentation else "unknown"
                ),
                apply_ordering="input_order",
                max_batch_actions=max_batch_actions,
                max_batch_bytes=max_batch_bytes,
            ),
        )

    def _apply(
        self,
        _context: syn.ContextProvider,
        actions: Sequence[tuple[str, str]],
        /,
    ) -> None:
        self.batches.append(list(actions))

    def reconcile(
        self,
        key: syn.StableKey,
        desired: str | syn.AbsentType,
        previous: Collection[str],
        prev_may_be_missing: bool,
        /,
    ) -> syn.TargetReconcileOutput[tuple[str, str], str] | None:
        del previous, prev_may_be_missing
        if syn.is_absent(desired):
            return None
        assert isinstance(key, str)
        return syn.TargetReconcileOutput(
            action=(key, desired),
            sink=self.sink,
            tracking_record=desired,
        )


class _ReplaySafeSegmentedStore(_SegmentedStore):
    def __init__(self) -> None:
        self.state: dict[str, str] = {}
        self.apply_calls = 0
        self.fail_on_apply: int | None = None
        super().__init__(max_batch_actions=2, allow_segmentation=True)

    def _apply(
        self,
        _context: syn.ContextProvider,
        actions: Sequence[tuple[str, str]],
        /,
    ) -> None:
        self.apply_calls += 1
        self.batches.append(list(actions))
        if self.apply_calls == self.fail_on_apply:
            raise RuntimeError("injected segmented sink failure")
        self.state.update(actions)


_SEGMENTED_STORE = _SegmentedStore(max_batch_actions=2, allow_segmentation=True)
_SEGMENTED_PROVIDER = syn.register_root_target_states_provider(
    "test_target_sink_capabilities/segmented", _SEGMENTED_STORE
)

_DECLARED_LIMIT_STORE = _SegmentedStore(
    max_batch_actions=2, batch_atomicity="per_apply"
)
_DECLARED_LIMIT_PROVIDER = syn.register_root_target_states_provider(
    "test_target_sink_capabilities/declared-limit", _DECLARED_LIMIT_STORE
)

_DEFAULT_LIMIT_STORE = _SegmentedStore(max_batch_actions=None)
_DEFAULT_LIMIT_PROVIDER = syn.register_root_target_states_provider(
    "test_target_sink_capabilities/default-limit", _DEFAULT_LIMIT_STORE
)

_BYTE_SEGMENTED_STORE = _SegmentedStore(
    max_batch_actions=None,
    max_batch_bytes=10_000,
    allow_segmentation=True,
)
_BYTE_SEGMENTED_PROVIDER = syn.register_root_target_states_provider(
    "test_target_sink_capabilities/byte-segmented", _BYTE_SEGMENTED_STORE
)

_BYTE_OVERSIZED_STORE = _SegmentedStore(
    max_batch_actions=None,
    max_batch_bytes=512,
    allow_segmentation=True,
)
_BYTE_OVERSIZED_PROVIDER = syn.register_root_target_states_provider(
    "test_target_sink_capabilities/byte-oversized", _BYTE_OVERSIZED_STORE
)

_REPLAY_SAFE_SEGMENTED_STORE = _ReplaySafeSegmentedStore()
_REPLAY_SAFE_SEGMENTED_PROVIDER = syn.register_root_target_states_provider(
    "test_target_sink_capabilities/replay-safe-segmented",
    _REPLAY_SAFE_SEGMENTED_STORE,
)


@pytest.mark.asyncio
async def test_declared_sink_limits_segment_reconciliation_without_reordering(
    tmp_path: Path,
) -> None:
    _SEGMENTED_STORE.batches.clear()

    @syn.task
    async def main() -> None:
        for index in range(5):
            syn.ensure_target_state(
                _SEGMENTED_PROVIDER.target_state(str(index), f"value-{index}")
            )

    app = syn.App(
        syn.AppConfig(
            name="target-sink-segmented-reconciliation",
            environment=syn.Environment(syn.Settings(db_path=tmp_path / "state")),
        ),
        main,
    )
    await app.update()

    assert [len(batch) for batch in _SEGMENTED_STORE.batches] == [2, 2, 1]
    assert {key for batch in _SEGMENTED_STORE.batches for key, _value in batch} == {
        "0",
        "1",
        "2",
        "3",
        "4",
    }


@pytest.mark.asyncio
async def test_declared_byte_limit_segments_sink_calls(tmp_path: Path) -> None:
    _BYTE_SEGMENTED_STORE.batches.clear()

    @syn.task
    async def main() -> None:
        for index in range(3):
            syn.ensure_target_state(
                _BYTE_SEGMENTED_PROVIDER.target_state(
                    str(index), f"{index}:" + ("x" * 6_000)
                )
            )

    app = syn.App(
        syn.AppConfig(
            name="target-sink-byte-segmented-reconciliation",
            environment=syn.Environment(syn.Settings(db_path=tmp_path / "state")),
        ),
        main,
    )
    await app.update()

    assert [len(batch) for batch in _BYTE_SEGMENTED_STORE.batches] == [1, 1, 1]


@pytest.mark.asyncio
async def test_segmented_sink_rejects_one_oversized_action_before_side_effects(
    tmp_path: Path,
) -> None:
    _BYTE_OVERSIZED_STORE.batches.clear()

    @syn.task
    async def main() -> None:
        syn.ensure_target_state(
            _BYTE_OVERSIZED_PROVIDER.target_state("record", "x" * 4_096)
        )

    app = syn.App(
        syn.AppConfig(
            name="target-sink-byte-oversized-preflight",
            environment=syn.Environment(syn.Settings(db_path=tmp_path / "state")),
        ),
        main,
    )
    with pytest.raises(Exception, match="approximately .* bytes.*limit of 512"):
        await app.update()

    assert _BYTE_OVERSIZED_STORE.batches == []


@pytest.mark.asyncio
async def test_segment_failure_does_not_commit_component_tracking_state(
    tmp_path: Path,
) -> None:
    store = _REPLAY_SAFE_SEGMENTED_STORE
    store.state.clear()
    store.batches.clear()
    store.apply_calls = 0
    store.fail_on_apply = 2

    @syn.task
    async def main() -> None:
        for index in range(5):
            syn.ensure_target_state(
                _REPLAY_SAFE_SEGMENTED_PROVIDER.target_state(
                    str(index), f"value-{index}"
                )
            )

    app = syn.App(
        syn.AppConfig(
            name="target-sink-segmented-retry",
            environment=syn.Environment(syn.Settings(db_path=tmp_path / "state")),
        ),
        main,
    )
    with pytest.raises(RuntimeError, match="injected segmented sink failure"):
        await app.update()

    assert [len(batch) for batch in store.batches] == [2, 2]
    assert set(store.state) == {key for key, _value in store.batches[0]}

    store.batches.clear()
    store.apply_calls = 0
    store.fail_on_apply = None
    await app.update()

    assert [len(batch) for batch in store.batches] == [2, 2, 1]
    assert {key for batch in store.batches for key, _value in batch} == {
        str(index) for index in range(5)
    }
    assert store.state == {str(index): f"value-{index}" for index in range(5)}


@pytest.mark.asyncio
async def test_declared_limit_without_segmentation_fails_before_sink_side_effects(
    tmp_path: Path,
) -> None:
    _DECLARED_LIMIT_STORE.batches.clear()

    @syn.task
    async def main() -> None:
        for index in range(5):
            syn.ensure_target_state(
                _DECLARED_LIMIT_PROVIDER.target_state(str(index), f"value-{index}")
            )

    app = syn.App(
        syn.AppConfig(
            name="target-sink-declared-limit-preflight",
            environment=syn.Environment(syn.Settings(db_path=tmp_path / "state")),
        ),
        main,
    )
    with pytest.raises(Exception, match="target sink batch has 5 actions"):
        await app.update()

    assert _DECLARED_LIMIT_STORE.batches == []


@pytest.mark.asyncio
async def test_legacy_sink_preserves_one_apply_call_within_default_limit(
    tmp_path: Path,
) -> None:
    _DEFAULT_LIMIT_STORE.batches.clear()

    @syn.task
    async def main() -> None:
        for index in range(5):
            syn.ensure_target_state(
                _DEFAULT_LIMIT_PROVIDER.target_state(str(index), f"value-{index}")
            )

    app = syn.App(
        syn.AppConfig(
            name="target-sink-legacy-single-apply",
            environment=syn.Environment(syn.Settings(db_path=tmp_path / "state")),
        ),
        main,
    )
    await app.update()

    assert [len(batch) for batch in _DEFAULT_LIMIT_STORE.batches] == [5]


@pytest.mark.asyncio
async def test_legacy_sink_preserves_one_apply_call_above_internal_packing_threshold(
    tmp_path: Path,
) -> None:
    _DEFAULT_LIMIT_STORE.batches.clear()

    @syn.task
    async def main() -> None:
        for index in range(4_097):
            syn.ensure_target_state(
                _DEFAULT_LIMIT_PROVIDER.target_state(str(index), f"value-{index}")
            )

    app = syn.App(
        syn.AppConfig(
            name="target-sink-legacy-above-packing-threshold",
            environment=syn.Environment(syn.Settings(db_path=tmp_path / "state")),
        ),
        main,
    )
    await app.update()

    assert [len(batch) for batch in _DEFAULT_LIMIT_STORE.batches] == [4_097]
