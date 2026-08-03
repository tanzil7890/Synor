"""Tests for Kafka source connector: offset tracking and watch behavior.

These tests mock the AIOConsumer to verify behavior without a real Kafka broker.
"""

from __future__ import annotations

import asyncio
import sys
from collections import deque
from typing import Any, Literal, cast
from unittest.mock import MagicMock

import pytest

# --- Mock confluent_kafka before importing the connector ---


class MockTopicPartition:
    """Mock confluent_kafka.TopicPartition."""

    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int = -1,
        *,
        err: object = None,
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.err = err


class MockKafkaError:
    """Mock confluent_kafka.KafkaError with the partition-EOF sentinel."""

    _PARTITION_EOF = -191

    def __init__(self, code: int, message: str) -> None:
        self._code = code
        self._message = message

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return self._message


class MockMessage:
    """Mock confluent_kafka.Message."""

    def __init__(
        self,
        *,
        topic: str = "test-topic",
        partition: int = 0,
        offset: int = 0,
        key: bytes | str | None = None,
        value: bytes | str | None = None,
        error_val: object = None,
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._error_val = error_val

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | str | None:
        return self._key

    def value(self) -> bytes | str | None:
        return self._value

    def error(self) -> object:
        return self._error_val


class MockComponentMountHandle:
    """Mock SpawnHandle with controllable readiness."""

    def __init__(self) -> None:
        self._ready_event = asyncio.Event()
        self._error: BaseException | None = None

    async def ready(self) -> None:
        await self._ready_event.wait()
        if self._error is not None:
            raise self._error

    async def outcome(self) -> ReadinessOutcome:
        await self._ready_event.wait()
        if self._error is not None:
            return Failed(self._error)
        return Succeeded()

    def set_ready(self) -> None:
        self._ready_event.set()

    def set_error(self, error: BaseException) -> None:
        self._error = error
        self._ready_event.set()


class MockAIOConsumer:
    """Mock AIOConsumer with controllable message delivery."""

    def __init__(self, config: dict[str, object] | None = None) -> None:
        self._config = dict(config or {})
        self._messages: deque[MockMessage | None] = deque()
        self._committed: list[MockTopicPartition] = []
        self._subscribed_topics: list[str] = []
        self._on_assign: Any = None
        self._on_revoke: Any = None
        self._on_lost: Any = None
        self._assignment: list[MockTopicPartition] = []
        self._paused: set[tuple[str, int]] = set()
        self._paused_polls = 0
        self._resume_calls = 0
        self._resume_error: BaseException | None = None
        self._watermarks: dict[tuple[str, int], tuple[int, int]] = {}
        self._watermark_error: BaseException | None = None
        self._committed_lookup_result: list[MockTopicPartition] | None = None
        self._commit_gate: asyncio.Event | None = None
        self._commit_error: BaseException | None = None
        self._commit_partition_error: object = None
        self._commit_ack_override: list[MockTopicPartition] | None = None
        self._commit_attempts: list[int] = []
        self._active_commits = 0
        self._max_active_commits = 0
        self._close_calls = 0
        self._close_gate: asyncio.Event | None = None
        self._close_started = asyncio.Event()
        self._closed = False
        self._lifecycle_events: list[str] = []

    def enqueue(self, *messages: MockMessage | None) -> None:
        """Add messages to the poll queue."""
        self._messages.extend(messages)

    async def subscribe(
        self,
        topics: list[str],
        *,
        on_assign: Any = None,
        on_revoke: Any = None,
        on_lost: Any = None,
    ) -> None:
        self._lifecycle_events.append("subscribe")
        self._subscribed_topics = topics
        self._on_assign = on_assign
        self._on_revoke = on_revoke
        self._on_lost = on_lost

    async def trigger_assign(self, partitions: list[MockTopicPartition]) -> None:
        """Simulate partition assignment."""
        self._assignment = list(partitions)
        if self._on_assign is not None:
            await self._on_assign(self, partitions)

    async def trigger_revoke(self, partitions: list[MockTopicPartition]) -> None:
        """Simulate partition revocation."""
        if self._on_revoke is not None:
            await self._on_revoke(self, partitions)
        revoked = {(tp.topic, tp.partition) for tp in partitions}
        self._assignment = [
            tp for tp in self._assignment if (tp.topic, tp.partition) not in revoked
        ]
        self._paused.difference_update(revoked)

    async def trigger_lost(self, partitions: list[MockTopicPartition]) -> None:
        """Simulate partitions lost without a final commit opportunity."""
        if self._on_lost is not None:
            await self._on_lost(self, partitions)
        lost = {(tp.topic, tp.partition) for tp in partitions}
        self._assignment = [
            tp for tp in self._assignment if (tp.topic, tp.partition) not in lost
        ]
        self._paused.difference_update(lost)

    def set_watermarks(self, topic: str, partition: int, low: int, high: int) -> None:
        """Set watermark offsets for a partition."""
        self._watermarks[(topic, partition)] = (low, high)

    async def unsubscribe(self) -> None:
        self._lifecycle_events.append("unsubscribe")
        self._subscribed_topics = []
        self._on_assign = None
        self._on_revoke = None
        self._on_lost = None
        self._assignment = []
        self._paused.clear()

    async def close(self) -> None:
        self._close_calls += 1
        self._lifecycle_events.append("close")
        self._close_started.set()
        if self._close_gate is not None:
            await self._close_gate.wait()
        self._closed = True

    async def committed(self, partitions: list[Any]) -> list[MockTopicPartition]:
        """Return committed offsets (defaults to -1001 — no commit yet)."""
        if self._committed_lookup_result is not None:
            return self._committed_lookup_result
        return [MockTopicPartition(tp.topic, tp.partition, -1001) for tp in partitions]

    async def get_watermark_offsets(self, tp: Any) -> tuple[int, int]:
        if self._watermark_error is not None:
            raise self._watermark_error
        key = (tp.topic, tp.partition)
        return self._watermarks.get(key, (0, 0))

    async def poll(self, timeout: float = 1.0) -> MockMessage | None:
        if self._messages:
            message = self._messages[0]
            if (
                message is not None
                and (
                    message.topic(),
                    message.partition(),
                )
                in self._paused
            ):
                self._paused_polls += 1
                await asyncio.sleep(0)
                return None
            return self._messages.popleft()
        # Signal end of messages by raising CancelledError after delivering all
        raise asyncio.CancelledError

    async def assignment(self) -> list[MockTopicPartition]:
        return list(self._assignment)

    async def pause(self, partitions: list[MockTopicPartition]) -> None:
        self._paused.update((tp.topic, tp.partition) for tp in partitions)

    async def resume(self, partitions: list[MockTopicPartition]) -> None:
        self._resume_calls += 1
        if self._resume_error is not None:
            raise self._resume_error
        self._paused.difference_update((tp.topic, tp.partition) for tp in partitions)

    async def commit(
        self, *, offsets: list[Any], asynchronous: bool = False
    ) -> list[MockTopicPartition]:
        assert not asynchronous
        self._active_commits += 1
        self._max_active_commits = max(self._max_active_commits, self._active_commits)
        self._commit_attempts.append(offsets[0].offset)
        try:
            if self._commit_gate is not None:
                await self._commit_gate.wait()
            if self._commit_error is not None:
                raise self._commit_error
            if self._commit_partition_error is not None:
                tp = offsets[0]
                return [
                    MockTopicPartition(
                        tp.topic,
                        tp.partition,
                        tp.offset,
                        err=self._commit_partition_error,
                    )
                ]
            if self._commit_ack_override is not None:
                return self._commit_ack_override
            self._committed.extend(offsets)
            return list(offsets)
        finally:
            self._active_commits -= 1


async def _wait_until(predicate: Any) -> None:
    """Yield until a deterministic async side effect becomes observable."""
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition did not become true")


# Install mocks before importing the source module
_mock_tp = MagicMock()
_mock_tp.TopicPartition = MockTopicPartition

_mock_aio = MagicMock()
_mock_aio.AIOConsumer = MockAIOConsumer

_mock_module = MagicMock()
_mock_module.aio = _mock_aio
_mock_module.KafkaError = MockKafkaError
_mock_module.TopicPartition = MockTopicPartition

sys.modules.setdefault("confluent_kafka", _mock_module)
sys.modules.setdefault("confluent_kafka.aio", _mock_aio)

from synor._internal.api import (
    Cancelled,
    Failed,
    ReadinessOutcome,
    Succeeded,
    Superseded,
)
from synor._internal.live_component import _IMMEDIATE_READY
from synor.connectors.kafka._source import (
    TopicStream,
    _OffsetTracker,
    _PartitionState,
    _TopicMapFeed,
    create_consumer,
    topic_as_stream,
)


def _make_source_consumer(
    config: dict[str, object] | None = None,
) -> MockAIOConsumer:
    # Most watch tests enqueue retained history and therefore explicitly model
    # an earliest-starting source. Passing an explicit mapping still exercises
    # the helper's production default (latest when the key is absent).
    selected_config = {"auto.offset.reset": "earliest"} if config is None else config
    return cast(MockAIOConsumer, create_consumer(selected_config))


class MockOutcomeHandle:
    """Typed handle that catches accidental use of compatibility ``ready()``."""

    def __init__(self, outcome: ReadinessOutcome) -> None:
        self._outcome = outcome

    async def ready(self) -> None:
        raise AssertionError("typed source acknowledgement must use outcome()")

    async def outcome(self) -> ReadinessOutcome:
        return self._outcome


# ============================================================================
# Source-consumer configuration safety
# ============================================================================


@pytest.mark.parametrize(
    "configured",
    [
        {},
        {"enable.auto.commit": True, "enable.auto.offset.store": True},
        {"enable.auto.commit": "true", "enable.auto.offset.store": "true"},
    ],
)
def test_create_consumer_forces_manual_offset_ownership(
    configured: dict[str, object],
) -> None:
    original = dict(configured)

    consumer = _make_source_consumer(configured)

    assert configured == original
    assert consumer._config["enable.auto.commit"] is False
    assert consumer._config["enable.auto.offset.store"] is False
    assert topic_as_stream(consumer, ["test-topic"])  # type: ignore[arg-type]


def test_raw_consumer_is_rejected_when_offset_safety_cannot_be_verified() -> None:
    consumer = MockAIOConsumer(
        {
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )

    with pytest.raises(ValueError, match="effective offset configuration"):
        topic_as_stream(consumer, ["test-topic"])  # type: ignore[arg-type]


def test_helper_consumer_can_only_be_bound_once() -> None:
    consumer = _make_source_consumer()

    topic_as_stream(consumer, ["first-topic"])  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="single-use.*already owned"):
        topic_as_stream(consumer, ["second-topic"])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("configured", "normalized"),
    [
        ("earliest", "earliest"),
        ("smallest", "earliest"),
        ("beginning", "earliest"),
        ("latest", "latest"),
        ("largest", "latest"),
        ("end", "latest"),
        ("error", "error"),
    ],
)
def test_helper_preserves_normalized_offset_reset_policy(
    configured: str, normalized: str
) -> None:
    stream = topic_as_stream(
        _make_source_consumer(  # type: ignore[arg-type]
            {"auto.offset.reset": configured}
        ),
        ["test-topic"],
    )

    assert isinstance(stream, TopicStream)
    assert stream._offset_reset == normalized


def test_unknown_offset_reset_policy_is_rejected_before_consumer_creation() -> None:
    with pytest.raises(ValueError, match="support auto.offset.reset"):
        create_consumer({"auto.offset.reset": "surprise"})


# ============================================================================
# Unit tests: _PartitionState offset tracking
# ============================================================================


class TestPartitionStateOffsetTracking:
    """Tests for per-partition offset tracking and commit logic."""

    @pytest.mark.asyncio
    async def test_in_order_completion(self) -> None:
        """Offsets completing in consumption order are committed immediately."""
        consumer = MockAIOConsumer()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=0,
            committed_offset=0,
            on_commit=lambda: None,
        )

        handles = [MockComponentMountHandle() for _ in range(3)]
        for i, h in enumerate(handles):
            state.track(i, h)

        # Complete in order: 0, 1, 2
        for h in handles:
            h.set_ready()
            await asyncio.sleep(0)  # let task run

        await asyncio.sleep(0.05)  # let commit propagate

        committed_offsets = [tp.offset for tp in consumer._committed]
        assert committed_offsets[-1] == 3  # offset 2 + 1

    @pytest.mark.asyncio
    async def test_out_of_order_completion(self) -> None:
        """Later offsets completing first don't trigger commit until front drains."""
        consumer = MockAIOConsumer()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=0,
            committed_offset=0,
            on_commit=lambda: None,
        )

        handles = [MockComponentMountHandle() for _ in range(3)]
        for i, h in enumerate(handles):
            state.track(i, h)

        # Complete offset 2 first
        handles[2].set_ready()
        await asyncio.sleep(0.05)
        assert len(consumer._committed) == 0  # nothing committable yet

        # Complete offset 1
        handles[1].set_ready()
        await asyncio.sleep(0.05)
        assert len(consumer._committed) == 0  # still blocked on offset 0

        # Complete offset 0 — all three drain
        handles[0].set_ready()
        await asyncio.sleep(0.05)
        committed_offsets = [tp.offset for tp in consumer._committed]
        assert committed_offsets[-1] == 3

    @pytest.mark.asyncio
    async def test_partial_drain(self) -> None:
        """Only contiguous completed offsets from the front drain."""
        consumer = MockAIOConsumer()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=0,
            committed_offset=0,
            on_commit=lambda: None,
        )

        handles = [MockComponentMountHandle() for _ in range(4)]
        for i, h in enumerate(handles):
            state.track(i, h)

        # Complete 0 and 1, leave 2 pending, complete 3
        handles[0].set_ready()
        handles[1].set_ready()
        handles[3].set_ready()
        await asyncio.sleep(0.05)

        # Should commit offset 2 (after draining 0, 1)
        committed_offsets = [tp.offset for tp in consumer._committed]
        assert committed_offsets[-1] == 2

        # Now complete 2 — should drain 2 and 3, commit 4
        handles[2].set_ready()
        await asyncio.sleep(0.05)
        committed_offsets = [tp.offset for tp in consumer._committed]
        assert committed_offsets[-1] == 4

    @pytest.mark.asyncio
    async def test_skip_null_key(self) -> None:
        """Skipped offsets (null key) are immediately completed."""
        consumer = MockAIOConsumer()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=0,
            committed_offset=0,
            on_commit=lambda: None,
        )

        h0 = MockComponentMountHandle()
        state.track(0, h0)
        state.track(1, _IMMEDIATE_READY)  # null-key fast path

        # Complete offset 0 — should drain 0 and 1
        h0.set_ready()
        await asyncio.sleep(0.05)

        committed_offsets = [tp.offset for tp in consumer._committed]
        assert committed_offsets[-1] == 2

        # Add and complete offset 2
        h2 = MockComponentMountHandle()
        state.track(2, h2)
        h2.set_ready()
        await asyncio.sleep(0.05)
        committed_offsets = [tp.offset for tp in consumer._committed]
        assert committed_offsets[-1] == 3

    @pytest.mark.asyncio
    async def test_downstream_failure_is_terminal_and_never_committed(self) -> None:
        consumer = MockAIOConsumer()
        failed = asyncio.Event()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=2,
            committed_offset=0,
            on_commit=lambda: None,
            on_failure=lambda _error: failed.set(),
        )
        first = MockComponentMountHandle()
        second = MockComponentMountHandle()
        state.track(0, first)
        state.track(1, second)

        second.set_ready()
        first.set_error(ValueError("downstream failed"))
        await asyncio.wait_for(failed.wait(), timeout=1)
        await state.close()

        with pytest.raises(ValueError, match="downstream failed"):
            state.raise_if_failed()
        assert consumer._committed == []
        assert not state.is_caught_up()

    @pytest.mark.asyncio
    async def test_typed_succeeded_is_the_only_outcome_that_commits(self) -> None:
        consumer = MockAIOConsumer()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=1,
            committed_offset=0,
            on_commit=lambda: None,
        )

        state.track(0, MockOutcomeHandle(Succeeded()))  # type: ignore[arg-type]
        await _wait_until(lambda: [tp.offset for tp in consumer._committed] == [1])

        assert state.is_caught_up()

    @pytest.mark.asyncio
    async def test_untyped_readiness_fails_closed_without_committing(self) -> None:
        consumer = MockAIOConsumer()
        failed = asyncio.Event()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=1,
            committed_offset=0,
            on_commit=lambda: None,
            on_failure=lambda _error: failed.set(),
        )

        class LegacyReadyOnly:
            async def ready(self) -> None:
                return

        state.track(0, LegacyReadyOnly())  # type: ignore[arg-type]
        await asyncio.wait_for(failed.wait(), timeout=1)
        await state.close()

        with pytest.raises(TypeError, match="typed readiness"):
            state.raise_if_failed()
        assert consumer._committed == []
        assert not state.is_caught_up()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("outcome", "error_type", "message"),
        [
            (Failed(ValueError("typed downstream failure")), ValueError, "failure"),
            (Cancelled(), asyncio.CancelledError, "cancelled"),
            (Superseded(), RuntimeError, "superseded"),
        ],
    )
    async def test_non_success_typed_outcomes_never_commit(
        self,
        outcome: ReadinessOutcome,
        error_type: type[BaseException],
        message: str,
    ) -> None:
        consumer = MockAIOConsumer()
        failed = asyncio.Event()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=1,
            committed_offset=0,
            on_commit=lambda: None,
            on_failure=lambda _error: failed.set(),
        )

        state.track(0, MockOutcomeHandle(outcome))  # type: ignore[arg-type]
        await asyncio.wait_for(failed.wait(), timeout=1)
        await state.close()

        with pytest.raises(error_type, match=message):
            state.raise_if_failed()
        assert consumer._committed == []
        assert not state.is_caught_up()

    @pytest.mark.asyncio
    async def test_progress_waits_for_broker_acknowledgement(self) -> None:
        consumer = MockAIOConsumer()
        consumer._commit_gate = asyncio.Event()
        commit_callbacks = 0

        def on_commit() -> None:
            nonlocal commit_callbacks
            commit_callbacks += 1

        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=1,
            committed_offset=0,
            on_commit=on_commit,
        )
        state.track(0, _IMMEDIATE_READY)
        await _wait_until(lambda: consumer._commit_attempts == [1])

        assert not state.is_caught_up()
        assert commit_callbacks == 0
        assert consumer._committed == []

        consumer._commit_gate.set()
        await state.close()
        assert state.is_caught_up()
        assert commit_callbacks == 1
        assert [tp.offset for tp in consumer._committed] == [1]

    @pytest.mark.asyncio
    async def test_commit_exception_does_not_advance_progress(self) -> None:
        consumer = MockAIOConsumer()
        consumer._commit_error = RuntimeError("broker unavailable")
        failed = asyncio.Event()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=1,
            committed_offset=0,
            on_commit=lambda: None,
            on_failure=lambda _error: failed.set(),
        )

        state.track(0, _IMMEDIATE_READY)
        await asyncio.wait_for(failed.wait(), timeout=1)

        with pytest.raises(RuntimeError, match="broker unavailable"):
            state.raise_if_failed()
        assert not state.is_caught_up()
        assert consumer._committed == []

    @pytest.mark.asyncio
    async def test_partition_level_commit_error_is_not_acknowledgement(self) -> None:
        consumer = MockAIOConsumer()
        consumer._commit_partition_error = "rebalance in progress"
        failed = asyncio.Event()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=1,
            committed_offset=0,
            on_commit=lambda: None,
            on_failure=lambda _error: failed.set(),
        )

        state.track(0, _IMMEDIATE_READY)
        await asyncio.wait_for(failed.wait(), timeout=1)

        with pytest.raises(RuntimeError, match="broker rejected offset commit"):
            state.raise_if_failed()
        assert not state.is_caught_up()
        assert consumer._committed == []

    @pytest.mark.asyncio
    async def test_missing_sync_commit_acknowledgement_is_terminal(self) -> None:
        consumer = MockAIOConsumer()
        consumer._commit_ack_override = []
        failed = asyncio.Event()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=1,
            committed_offset=0,
            on_commit=lambda: None,
            on_failure=lambda _error: failed.set(),
        )

        state.track(0, _IMMEDIATE_READY)
        await asyncio.wait_for(failed.wait(), timeout=1)

        with pytest.raises(RuntimeError, match="invalid acknowledgement count"):
            state.raise_if_failed()
        assert not state.is_caught_up()

    @pytest.mark.asyncio
    async def test_higher_sync_commit_acknowledgement_is_terminal(self) -> None:
        consumer = MockAIOConsumer()
        consumer._commit_ack_override = [MockTopicPartition("t", 0, 2)]
        failed = asyncio.Event()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=1,
            committed_offset=0,
            on_commit=lambda: None,
            on_failure=lambda _error: failed.set(),
        )

        state.track(0, _IMMEDIATE_READY)
        await asyncio.wait_for(failed.wait(), timeout=1)

        with pytest.raises(RuntimeError, match="mismatched acknowledgement"):
            state.raise_if_failed()
        assert not state.is_caught_up()

    @pytest.mark.asyncio
    async def test_coalesced_commits_are_serialized_and_monotonic(self) -> None:
        consumer = MockAIOConsumer()
        consumer._commit_gate = asyncio.Event()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=3,
            committed_offset=0,
            on_commit=lambda: None,
        )

        state.track(0, _IMMEDIATE_READY)
        await _wait_until(lambda: consumer._commit_attempts == [1])
        state.track(1, _IMMEDIATE_READY)
        state.track(2, _IMMEDIATE_READY)
        consumer._commit_gate.set()
        await state.close()

        assert consumer._commit_attempts == [1, 3]
        assert [tp.offset for tp in consumer._committed] == [1, 3]
        assert consumer._max_active_commits == 1

    @pytest.mark.asyncio
    async def test_close_follows_commit_worker_handoff(self) -> None:
        second_started = asyncio.Event()
        second_gate = asyncio.Event()

        class HandoffConsumer(MockAIOConsumer):
            async def commit(
                self, *, offsets: list[Any], asynchronous: bool = False
            ) -> list[MockTopicPartition]:
                assert not asynchronous
                offset = offsets[0].offset
                self._commit_attempts.append(offset)
                if offset == 2:
                    second_started.set()
                    await second_gate.wait()
                self._committed.extend(offsets)
                return list(offsets)

        consumer = HandoffConsumer()

        class HandoffState(_PartitionState):
            _handed_off = False

            async def _run_commit_worker(self) -> None:
                await super()._run_commit_worker()
                if not self._handed_off:
                    self._handed_off = True
                    self._pending_commit_offset = 2
                    self._ensure_commit_worker()

        state = HandoffState(
            consumer,  # type: ignore[arg-type]
            "t",
            0,
            high_watermark=2,
            committed_offset=0,
            on_commit=lambda: None,
        )
        state.track(0, _IMMEDIATE_READY)

        close_task = asyncio.create_task(state.close())
        await asyncio.wait_for(second_started.wait(), timeout=1)
        assert not close_task.done()

        second_gate.set()
        await asyncio.wait_for(close_task, timeout=1)
        assert consumer._commit_attempts == [1, 2]
        assert [tp.offset for tp in consumer._committed] == [1, 2]

    @pytest.mark.asyncio
    async def test_revoke_fences_and_drains_before_returning(self) -> None:
        consumer = MockAIOConsumer()
        tracker = _OffsetTracker(consumer)  # type: ignore[arg-type]
        assignment_epoch = tracker.begin_assignment()
        tracker.complete_assignment(assignment_epoch, [("t", 0, 1, 0)])
        state = tracker.get("t", 0)
        handle = MockComponentMountHandle()
        state.track(0, handle)

        revoke = asyncio.create_task(
            tracker.on_revoke([MockTopicPartition("t", 0)])  # type: ignore[list-item]
        )
        await asyncio.sleep(0)
        assert not revoke.done()
        assert consumer._committed == []

        handle.set_ready()
        await asyncio.wait_for(revoke, timeout=1)
        assert [tp.offset for tp in consumer._committed] == [1]
        with pytest.raises(RuntimeError, match="unassigned partition"):
            tracker.get("t", 0)

    @pytest.mark.asyncio
    async def test_revoke_surfaces_commit_failure(self) -> None:
        consumer = MockAIOConsumer()
        consumer._commit_error = RuntimeError("generation lost")
        tracker = _OffsetTracker(consumer)  # type: ignore[arg-type]
        assignment_epoch = tracker.begin_assignment()
        tracker.complete_assignment(assignment_epoch, [("t", 0, 1, 0)])
        state = tracker.get("t", 0)
        state.track(0, _IMMEDIATE_READY)

        with pytest.raises(RuntimeError, match="generation lost"):
            await tracker.on_revoke(
                [MockTopicPartition("t", 0)]  # type: ignore[list-item]
            )
        assert consumer._committed == []

    @pytest.mark.asyncio
    async def test_lost_partition_cancels_readiness_without_committing(self) -> None:
        consumer = MockAIOConsumer()
        tracker = _OffsetTracker(consumer)  # type: ignore[arg-type]
        assignment_epoch = tracker.begin_assignment()
        tracker.complete_assignment(assignment_epoch, [("t", 0, 1, 0)])
        state = tracker.get("t", 0)
        state.track(0, MockComponentMountHandle())

        await tracker.on_lost(
            [MockTopicPartition("t", 0)]  # type: ignore[list-item]
        )

        assert consumer._commit_attempts == []
        assert consumer._committed == []
        with pytest.raises(RuntimeError, match="unassigned partition"):
            tracker.get("t", 0)
        await tracker.close_all()

    @pytest.mark.asyncio
    async def test_lost_partition_cancels_inflight_commit_without_acknowledging(
        self,
    ) -> None:
        consumer = MockAIOConsumer()
        consumer._commit_gate = asyncio.Event()
        tracker = _OffsetTracker(consumer)  # type: ignore[arg-type]
        assignment_epoch = tracker.begin_assignment()
        tracker.complete_assignment(assignment_epoch, [("t", 0, 1, 0)])
        tracker.get("t", 0).track(0, _IMMEDIATE_READY)
        await _wait_until(lambda: consumer._commit_attempts == [1])

        await tracker.on_lost(
            [MockTopicPartition("t", 0)]  # type: ignore[list-item]
        )
        consumer._commit_gate.set()
        await asyncio.sleep(0)

        assert consumer._committed == []
        assert not tracker.ready_event.is_set()
        await tracker.close_all()

    @pytest.mark.asyncio
    async def test_in_progress_assignment_cannot_signal_ready(self) -> None:
        consumer = MockAIOConsumer()
        consumer._commit_gate = asyncio.Event()
        tracker = _OffsetTracker(consumer)  # type: ignore[arg-type]
        first_epoch = tracker.begin_assignment()
        tracker.complete_assignment(first_epoch, [("t", 0, 1, 0)])

        retained_state = tracker.get("t", 0)
        retained_state.track(0, _IMMEDIATE_READY)
        await _wait_until(lambda: consumer._commit_attempts == [1])

        # Model cooperative assignment metadata lookup: the retained partition
        # finishes while a newly assigned partition is not installed yet.
        second_epoch = tracker.begin_assignment()
        consumer._commit_gate.set()
        await _wait_until(lambda: [tp.offset for tp in consumer._committed] == [1])

        assert not tracker.is_assigned()
        assert not tracker.ready_event.is_set()

        tracker.complete_assignment(second_epoch, [("t", 1, 1, 0)])
        assert tracker.is_assigned()
        assert not tracker.ready_event.is_set()

        tracker.get("t", 1).track(0, _IMMEDIATE_READY)
        await asyncio.wait_for(tracker.ready_event.wait(), timeout=1)
        await tracker.close_all()

    @pytest.mark.asyncio
    async def test_revoked_commit_cannot_signal_ready_for_empty_assignment(
        self,
    ) -> None:
        consumer = MockAIOConsumer()
        consumer._commit_gate = asyncio.Event()
        tracker = _OffsetTracker(consumer)  # type: ignore[arg-type]
        assignment_epoch = tracker.begin_assignment()
        tracker.complete_assignment(assignment_epoch, [("t", 0, 1, 0)])

        tracker.get("t", 0).track(0, _IMMEDIATE_READY)
        await _wait_until(lambda: consumer._commit_attempts == [1])
        revoke_task = asyncio.create_task(
            tracker.on_revoke([MockTopicPartition("t", 0)])  # type: ignore[list-item]
        )
        await _wait_until(lambda: not tracker.is_assigned())

        consumer._commit_gate.set()
        await asyncio.wait_for(revoke_task, timeout=1)

        assert not tracker.ready_event.is_set()
        assert not tracker.is_assigned()
        assert tracker._partitions == {}
        await tracker.close_all()

    @pytest.mark.asyncio
    async def test_close_all_drains_after_repeated_cancellation(self) -> None:
        consumer = MockAIOConsumer()
        consumer._commit_gate = asyncio.Event()
        tracker = _OffsetTracker(consumer)  # type: ignore[arg-type]
        assignment_epoch = tracker.begin_assignment()
        tracker.complete_assignment(assignment_epoch, [("t", 0, 1, 0)])
        tracker.get("t", 0).track(0, _IMMEDIATE_READY)
        await _wait_until(lambda: consumer._commit_attempts == [1])

        close_task = asyncio.create_task(tracker.close_all())
        await _wait_until(lambda: bool(tracker._close_tasks))

        close_task.cancel("first cancellation")
        await asyncio.sleep(0)
        assert not close_task.done()

        close_task.cancel("second cancellation")
        await asyncio.sleep(0)
        assert not close_task.done()

        consumer._commit_gate.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(close_task, timeout=1)

        assert [tp.offset for tp in consumer._committed] == [1]
        assert tracker._close_tasks == set()

    @pytest.mark.asyncio
    async def test_close_all_drains_every_partition_before_first_failure(self) -> None:
        consumer = MockAIOConsumer()
        tracker = _OffsetTracker(consumer)  # type: ignore[arg-type]
        closed: list[str] = []

        class FailingCloseState:
            def __init__(self, name: str, error: BaseException) -> None:
                self._name = name
                self._error = error

            async def close(self) -> None:
                if self._name == "second":
                    await asyncio.sleep(0.01)
                closed.append(self._name)
                raise self._error

            def raise_if_failed(self) -> None:
                return

        tracker._partitions[("t", 0)] = FailingCloseState(  # type: ignore[assignment]
            "first", ValueError("first close failed")
        )
        tracker._partitions[("t", 1)] = FailingCloseState(  # type: ignore[assignment]
            "second", RuntimeError("second close failed")
        )

        with pytest.raises(ValueError, match="first close failed"):
            await tracker.close_all()

        assert closed == ["first", "second"]


# ============================================================================
# E2E tests: watch behavior
# ============================================================================


class MockSubscriber:
    """Mock LiveMapSubscriber that records calls and returns controllable handles."""

    def __init__(self, *, auto_ready: bool = True) -> None:
        self.updates: list[tuple[bytes | str, MockMessage]] = []
        self.deletes: list[bytes | str] = []
        self.ready_called = False
        self.update_all_called = False
        self._auto_ready = auto_ready

    async def update(
        self, key: bytes | str, value: MockMessage
    ) -> MockComponentMountHandle:
        self.updates.append((key, value))
        h = MockComponentMountHandle()
        if self._auto_ready:
            h.set_ready()
        return h

    async def delete(self, key: bytes | str) -> MockComponentMountHandle:
        self.deletes.append(key)
        h = MockComponentMountHandle()
        if self._auto_ready:
            h.set_ready()
        return h

    async def update_all(self) -> None:
        self.update_all_called = True

    async def mark_ready(self) -> None:
        self.ready_called = True


async def _watch_until_done(feed: _TopicMapFeed, sub: MockSubscriber) -> None:
    """Run watch() and suppress CancelledError (raised by mock when messages are exhausted)."""
    try:
        await feed.watch(sub)  # type: ignore[arg-type]
    except asyncio.CancelledError:
        pass


class TestWatchBehavior:
    """E2E tests for _TopicMapFeed.watch()."""

    @pytest.mark.asyncio
    async def test_new_group_latest_starts_ready_at_current_high_watermark(
        self,
    ) -> None:
        consumer = _make_source_consumer({"auto.offset.reset": "latest"})
        consumer.set_watermarks("test-topic", 0, 0, 7)
        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        subscriber = MockSubscriber()
        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            assert consumer._on_lost is not None
            assert consumer._on_lost is not consumer._on_revoke
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, subscriber)

        assert subscriber.ready_called
        assert subscriber.updates == []
        assert consumer._committed == []

    @pytest.mark.asyncio
    async def test_empty_retained_log_uses_nonzero_low_watermark_as_ready(
        self,
    ) -> None:
        consumer = _make_source_consumer({"auto.offset.reset": "earliest"})
        consumer.set_watermarks("test-topic", 0, 11, 11)
        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        subscriber = MockSubscriber()
        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, subscriber)

        assert subscriber.ready_called
        assert subscriber.updates == []
        assert consumer._committed == []

    @pytest.mark.asyncio
    async def test_new_group_with_error_reset_fails_closed(self) -> None:
        consumer = _make_source_consumer({"auto.offset.reset": "error"})
        consumer.set_watermarks("test-topic", 0, 0, 7)
        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        subscriber = MockSubscriber()
        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        with pytest.raises(RuntimeError, match="effective starting offset"):
            await feed.watch(subscriber)  # type: ignore[arg-type]

        assert not subscriber.ready_called
        assert consumer._committed == []

    @pytest.mark.asyncio
    async def test_basic_consumption(self) -> None:
        """Messages are delivered as subscriber.update() calls."""
        consumer = _make_source_consumer()
        consumer.set_watermarks("test-topic", 0, 0, 3)
        consumer.enqueue(
            MockMessage(key=b"k1", value=b"v1", offset=0),
            MockMessage(key=b"k2", value=b"v2", offset=1),
            MockMessage(key=b"k3", value=b"v3", offset=2),
        )

        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        sub = MockSubscriber()

        # Trigger assign manually since subscribe is called inside watch
        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, sub)

        assert [(k, m.value()) for k, m in sub.updates] == [
            (b"k1", b"v1"),
            (b"k2", b"v2"),
            (b"k3", b"v3"),
        ]
        assert sub.ready_called

    @pytest.mark.asyncio
    async def test_non_eof_consumer_message_error_is_terminal(self) -> None:
        consumer = _make_source_consumer()
        consumer.set_watermarks("test-topic", 0, 0, 0)
        consumer.enqueue(
            MockMessage(error_val=MockKafkaError(7, "broker transport failed"))
        )
        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        subscriber = MockSubscriber()
        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        with pytest.raises(RuntimeError, match="broker transport failed"):
            await feed.watch(subscriber)  # type: ignore[arg-type]
        assert consumer._committed == []
        assert consumer._subscribed_topics == []
        assert consumer._closed
        assert consumer._lifecycle_events[-2:] == ["unsubscribe", "close"]

    @pytest.mark.asyncio
    async def test_partition_eof_message_is_informational(self) -> None:
        consumer = _make_source_consumer()
        consumer.set_watermarks("test-topic", 0, 0, 0)
        consumer.enqueue(
            MockMessage(
                error_val=MockKafkaError(MockKafkaError._PARTITION_EOF, "partition eof")
            )
        )
        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        subscriber = MockSubscriber()
        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, subscriber)
        assert subscriber.updates == []
        assert subscriber.ready_called

    @pytest.mark.asyncio
    async def test_watch_surfaces_failed_ready_handle_without_committing(self) -> None:
        consumer = _make_source_consumer()
        consumer.set_watermarks("test-topic", 0, 0, 1)
        consumer.enqueue(MockMessage(key=b"k1", value=b"v1", offset=0))

        class FailingSubscriber(MockSubscriber):
            async def update(
                self, key: bytes | str, value: MockMessage
            ) -> MockComponentMountHandle:
                handle = await super().update(key, value)
                handle.set_error(ValueError("sink was not durable"))
                return handle

        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        subscriber = FailingSubscriber(auto_ready=False)
        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        with pytest.raises(ValueError, match="sink was not durable"):
            await feed.watch(subscriber)  # type: ignore[arg-type]
        assert consumer._committed == []
        assert not subscriber.ready_called

    @pytest.mark.asyncio
    async def test_assignment_watermark_failure_is_terminal(self) -> None:
        consumer = _make_source_consumer()
        consumer._watermark_error = RuntimeError("metadata unavailable")
        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        subscriber = MockSubscriber()
        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        with pytest.raises(RuntimeError, match="high-watermark lookup failed"):
            await feed.watch(subscriber)  # type: ignore[arg-type]
        assert not subscriber.ready_called
        assert consumer._subscribed_topics == []

    @pytest.mark.asyncio
    async def test_assignment_committed_result_cardinality_is_validated(self) -> None:
        consumer = _make_source_consumer()
        consumer._committed_lookup_result = []
        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        subscriber = MockSubscriber()
        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        with pytest.raises(RuntimeError, match="invalid partition result count"):
            await feed.watch(subscriber)  # type: ignore[arg-type]
        assert not subscriber.ready_called

    @pytest.mark.asyncio
    async def test_assignment_committed_partition_error_is_terminal(self) -> None:
        consumer = _make_source_consumer()
        consumer._committed_lookup_result = [
            MockTopicPartition("test-topic", 0, err="coordinator unavailable")
        ]
        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        subscriber = MockSubscriber()
        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        with pytest.raises(RuntimeError, match="committed-offset lookup failed"):
            await feed.watch(subscriber)  # type: ignore[arg-type]
        assert not subscriber.ready_called

    @pytest.mark.asyncio
    async def test_tombstone_deletion(self) -> None:
        """Messages with None value trigger subscriber.delete()."""
        consumer = _make_source_consumer()
        consumer.set_watermarks("test-topic", 0, 0, 2)
        consumer.enqueue(
            MockMessage(key=b"k1", value=b"v1", offset=0),
            MockMessage(key=b"k1", value=None, offset=1),
        )

        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        sub = MockSubscriber()

        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, sub)

        assert [(k, m.value()) for k, m in sub.updates] == [(b"k1", b"v1")]
        assert sub.deletes == [b"k1"]

    @pytest.mark.asyncio
    async def test_custom_is_deletion(self) -> None:
        """Custom is_deletion predicate triggers subscriber.delete()."""
        consumer = _make_source_consumer()
        consumer.set_watermarks("test-topic", 0, 0, 2)
        consumer.enqueue(
            MockMessage(key=b"k1", value=b"DELETED", offset=0),
            MockMessage(key=b"k2", value=b"normal", offset=1),
        )

        feed = _TopicMapFeed(
            TopicStream(consumer, ["test-topic"]),  # type: ignore[arg-type]
            is_deletion=lambda msg: msg.value() == b"DELETED",
        )
        sub = MockSubscriber()

        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, sub)

        assert sub.deletes == [b"k1"]
        assert [(k, m.value()) for k, m in sub.updates] == [(b"k2", b"normal")]

    @pytest.mark.asyncio
    async def test_null_key_skipped(self) -> None:
        """Messages with None key are skipped."""
        consumer = _make_source_consumer()
        consumer.set_watermarks("test-topic", 0, 0, 2)
        consumer.enqueue(
            MockMessage(key=None, value=b"v1", offset=0),
            MockMessage(key=b"k2", value=b"v2", offset=1),
        )

        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        sub = MockSubscriber()

        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, sub)

        assert [(k, m.value()) for k, m in sub.updates] == [(b"k2", b"v2")]
        assert len(sub.deletes) == 0

    @pytest.mark.asyncio
    async def test_readiness_after_watermark(self) -> None:
        """mark_ready() called only after all partitions reach watermarks."""
        consumer = _make_source_consumer()
        consumer.set_watermarks("test-topic", 0, 0, 2)
        consumer.set_watermarks("test-topic", 1, 0, 1)

        ready_at_offset: list[int] = []

        class TrackingSubscriber(MockSubscriber):
            async def update(
                self, key: bytes | str, value: MockMessage
            ) -> MockComponentMountHandle:
                h = await super().update(key, value)
                return h

            async def mark_ready(self) -> None:
                ready_at_offset.append(len(self.updates) + len(self.deletes))
                await super().mark_ready()

        consumer.enqueue(
            MockMessage(
                topic="test-topic", partition=0, key=b"k1", value=b"v1", offset=0
            ),
            MockMessage(
                topic="test-topic", partition=1, key=b"k2", value=b"v2", offset=0
            ),
            MockMessage(
                topic="test-topic", partition=0, key=b"k3", value=b"v3", offset=1
            ),
        )

        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        sub = TrackingSubscriber()

        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign(
                [
                    MockTopicPartition("test-topic", 0),
                    MockTopicPartition("test-topic", 1),
                ]
            )

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, sub)

        assert sub.ready_called
        # Ready should be called after all 3 messages (both partitions caught up)
        assert ready_at_offset == [3]

    @pytest.mark.asyncio
    async def test_partition_rebalance_discards_state(self) -> None:
        """Partition revoke discards tracking state."""
        consumer = _make_source_consumer()
        consumer.set_watermarks("test-topic", 0, 0, 0)

        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        sub = MockSubscriber()

        # We need to test rebalance during consumption.
        # Inject messages that trigger a rebalance mid-stream.
        msg_count = 0

        async def patched_poll(timeout: float = 1.0) -> MockMessage | None:
            nonlocal msg_count
            msg_count += 1
            if msg_count == 1:
                # First: assign partition 0
                await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])
                return MockMessage(key=b"k1", value=b"v1", partition=0, offset=0)
            elif msg_count == 2:
                # Revoke partition 0, assign partition 1
                await consumer.trigger_revoke([MockTopicPartition("test-topic", 0)])
                consumer.set_watermarks("test-topic", 1, 0, 1)
                await consumer.trigger_assign([MockTopicPartition("test-topic", 1)])
                return MockMessage(key=b"k2", value=b"v2", partition=1, offset=0)
            else:
                raise asyncio.CancelledError

        consumer.poll = patched_poll  # type: ignore[assignment]

        # Don't auto-trigger assign from subscribe
        await _watch_until_done(feed, sub)

        update_kvs = [(k, m.value()) for k, m in sub.updates]
        assert (b"k1", b"v1") in update_kvs
        assert (b"k2", b"v2") in update_kvs

    @pytest.mark.asyncio
    async def test_already_set_ready_event_is_revalidated_after_rebalance(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ready waiter cannot publish readiness from a superseded assignment."""
        consumer = _make_source_consumer()
        consumer.set_watermarks("test-topic", 0, 0, 0)
        poll_gate = asyncio.Event()

        async def blocked_poll(timeout: float = 1.0) -> MockMessage | None:
            await poll_gate.wait()
            raise asyncio.CancelledError

        consumer.poll = blocked_poll  # type: ignore[assignment]
        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        class RebalanceAfterObservingReady(asyncio.Event):
            def __init__(self, tracker: _OffsetTracker) -> None:
                super().__init__()
                self._tracker = tracker

            async def wait(self) -> Literal[True]:
                observed = await super().wait()
                # The initial empty assignment made the event ready before the
                # watch loop installed its waiter. Supersede that generation in
                # the precise seam between wait() and watch's mark_ready().
                self._tracker.begin_assignment()
                poll_gate.set()
                return observed

        class RacingOffsetTracker(_OffsetTracker):
            def __init__(self, source_consumer: Any) -> None:
                super().__init__(source_consumer)
                self.ready_event = RebalanceAfterObservingReady(self)

        source_module = sys.modules[TopicStream.__module__]
        monkeypatch.setattr(source_module, "_OffsetTracker", RacingOffsetTracker)

        subscriber = MockSubscriber()
        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]

        await _watch_until_done(feed, subscriber)

        assert not subscriber.ready_called

    @pytest.mark.asyncio
    async def test_watch_cancellation_waits_for_in_progress_revoke_close(self) -> None:
        consumer = _make_source_consumer()
        consumer.set_watermarks("test-topic", 0, 0, 1)
        consumer.enqueue(MockMessage(key=b"k1", value=b"v1", offset=0))

        poll_gate = asyncio.Event()

        async def blocking_poll(timeout: float = 1.0) -> MockMessage | None:
            if consumer._messages:
                return consumer._messages.popleft()
            await poll_gate.wait()
            return None

        consumer.poll = blocking_poll  # type: ignore[assignment]

        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        unsubscribe_started = asyncio.Event()
        original_unsubscribe = consumer.unsubscribe

        async def observed_unsubscribe() -> None:
            unsubscribe_started.set()
            await original_unsubscribe()

        consumer.unsubscribe = observed_unsubscribe  # type: ignore[assignment]

        class BlockingSubscriber(MockSubscriber):
            def __init__(self) -> None:
                super().__init__(auto_ready=False)
                self.received = asyncio.Event()
                self.handle: MockComponentMountHandle | None = None

            async def update(
                self, key: bytes | str, value: MockMessage
            ) -> MockComponentMountHandle:
                handle = await super().update(key, value)
                self.handle = handle
                self.received.set()
                return handle

        subscriber = BlockingSubscriber()
        feed = _TopicMapFeed(TopicStream(consumer, ["test-topic"]), None)  # type: ignore[arg-type]
        watch_task = asyncio.create_task(feed.watch(subscriber))  # type: ignore[arg-type]
        await asyncio.wait_for(subscriber.received.wait(), timeout=1)

        revoke_entered = asyncio.Event()

        async def trigger_revoke() -> None:
            revoke_entered.set()
            assert consumer._on_revoke is not None
            await consumer._on_revoke(consumer, [MockTopicPartition("test-topic", 0)])

        revoke_task = asyncio.create_task(trigger_revoke())
        await revoke_entered.wait()
        # The callback has no suspension point before it registers the close.
        await asyncio.sleep(0)
        assert not revoke_task.done()

        watch_task.cancel()
        await asyncio.sleep(0)
        assert not watch_task.done()
        assert not unsubscribe_started.is_set()

        assert subscriber.handle is not None
        subscriber.handle.set_ready()
        await asyncio.wait_for(revoke_task, timeout=1)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(watch_task, timeout=1)
        assert unsubscribe_started.is_set()
        assert consumer._closed
        assert consumer._lifecycle_events[-2:] == ["unsubscribe", "close"]

    @pytest.mark.asyncio
    async def test_tombstone_always_deletion_even_with_custom_predicate(self) -> None:
        """None value is always deletion even when is_deletion returns False."""
        consumer = _make_source_consumer()
        consumer.set_watermarks("test-topic", 0, 0, 1)
        consumer.enqueue(
            MockMessage(key=b"k1", value=None, offset=0),
        )

        # is_deletion always returns False, but None value should still be deletion
        feed = _TopicMapFeed(
            TopicStream(consumer, ["test-topic"]),  # type: ignore[arg-type]
            is_deletion=lambda msg: False,
        )
        sub = MockSubscriber()

        original_subscribe = consumer.subscribe

        async def patched_subscribe(topics: list[str], **kw: Any) -> None:
            await original_subscribe(topics, **kw)
            await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

        consumer.subscribe = patched_subscribe  # type: ignore[assignment]

        await _watch_until_done(feed, sub)

        assert sub.deletes == [b"k1"]
        assert len(sub.updates) == 0


# ============================================================================
# Fast path: _IMMEDIATE_READY in track()
# ============================================================================


@pytest.mark.asyncio
async def test_track_immediate_ready_fast_path() -> None:
    """track(offset, _IMMEDIATE_READY) advances commit synchronously, no task spawn."""
    consumer = MockAIOConsumer()
    state = _PartitionState(
        consumer,  # type: ignore[arg-type]
        "t",
        0,
        high_watermark=0,
        committed_offset=0,
        on_commit=lambda: None,
    )

    state.track(0, _IMMEDIATE_READY)
    state.track(1, _IMMEDIATE_READY)
    state.track(2, _IMMEDIATE_READY)

    # No task spawn — verify immediately, before any sleep.
    assert len(state._tasks) == 0
    # Drain happens synchronously, but commit is dispatched via ensure_future.
    await asyncio.sleep(0.05)

    committed_offsets = [tp.offset for tp in consumer._committed]
    assert committed_offsets[-1] == 3


# ============================================================================
# topic_as_stream + payloads()
# ============================================================================


class _StreamSubscriber:
    """Mock LiveStreamSubscriber recording received messages."""

    def __init__(self) -> None:
        self.messages: list[Any] = []
        self.ready_called = False

    async def send(self, message: Any) -> Any:
        self.messages.append(message)
        return _IMMEDIATE_READY

    async def mark_ready(self) -> None:
        self.ready_called = True


async def _watch_stream_until_done(stream: Any, sub: _StreamSubscriber) -> None:
    try:
        await stream.watch(sub)
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_helper_consumer_closes_after_successful_watch() -> None:
    consumer = _make_source_consumer()

    class SuccessfulTopicStream(TopicStream):
        async def _watch(self, subscriber: Any) -> None:
            await subscriber.mark_ready()

    stream = SuccessfulTopicStream(consumer, ["test-topic"])  # type: ignore[arg-type]
    subscriber = _StreamSubscriber()

    await stream.watch(subscriber)  # type: ignore[arg-type]

    assert subscriber.ready_called
    assert consumer._closed
    assert consumer._close_calls == 1
    assert consumer._lifecycle_events == ["unsubscribe", "close"]
    with pytest.raises(RuntimeError, match="single-use"):
        await stream.watch(subscriber)  # type: ignore[arg-type]
    assert consumer._close_calls == 1


def test_raw_consumer_is_rejected_before_subscription_or_close() -> None:
    consumer = MockAIOConsumer(
        {
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    with pytest.raises(ValueError, match="create_consumer"):
        topic_as_stream(consumer, ["test-topic"])  # type: ignore[arg-type]

    assert consumer._lifecycle_events == []
    assert consumer._close_calls == 0
    assert not consumer._closed


@pytest.mark.asyncio
async def test_helper_consumer_close_survives_repeated_watch_cancellation() -> None:
    consumer = _make_source_consumer()
    consumer._close_gate = asyncio.Event()
    consumer.set_watermarks("test-topic", 0, 0, 0)
    poll_started = asyncio.Event()
    poll_gate = asyncio.Event()

    async def blocking_poll(timeout: float = 1.0) -> MockMessage | None:
        poll_started.set()
        await poll_gate.wait()
        return None

    consumer.poll = blocking_poll  # type: ignore[assignment]
    original_subscribe = consumer.subscribe

    async def patched_subscribe(topics: list[str], **kw: Any) -> None:
        await original_subscribe(topics, **kw)
        await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

    consumer.subscribe = patched_subscribe  # type: ignore[assignment]
    stream = topic_as_stream(consumer, ["test-topic"])  # type: ignore[arg-type]
    watch_task = asyncio.create_task(stream.watch(_StreamSubscriber()))
    await asyncio.wait_for(poll_started.wait(), timeout=1)

    watch_task.cancel("stop watching")
    await asyncio.wait_for(consumer._close_started.wait(), timeout=1)
    assert consumer._lifecycle_events[-2:] == ["unsubscribe", "close"]
    assert not watch_task.done()

    watch_task.cancel("cancel cleanup again")
    await asyncio.sleep(0)
    assert not watch_task.done()
    watch_task.cancel("cancel cleanup a third time")
    await asyncio.sleep(0)
    assert not watch_task.done()

    consumer._close_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(watch_task, timeout=1)

    assert consumer._closed
    assert consumer._close_calls == 1


@pytest.mark.asyncio
async def test_topic_as_stream_basic_consumption() -> None:
    """topic_as_stream forwards every valid message to subscriber.send()."""
    consumer = _make_source_consumer()
    consumer.set_watermarks("test-topic", 0, 0, 3)
    msgs = [
        MockMessage(key=b"k1", value=b"v1", offset=0),
        MockMessage(key=b"k2", value=b"v2", offset=1),
        MockMessage(key=b"k3", value=b"v3", offset=2),
    ]
    consumer.enqueue(*msgs)

    original_subscribe = consumer.subscribe

    async def patched_subscribe(topics: list[str], **kw: Any) -> None:
        await original_subscribe(topics, **kw)
        await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

    consumer.subscribe = patched_subscribe  # type: ignore[assignment]

    stream = topic_as_stream(consumer, ["test-topic"])  # type: ignore[arg-type]
    sub = _StreamSubscriber()
    await _watch_stream_until_done(stream, sub)

    assert [m.value() for m in sub.messages] == [b"v1", b"v2", b"v3"]
    assert sub.ready_called
    assert consumer._closed
    assert consumer._lifecycle_events[-2:] == ["unsubscribe", "close"]


@pytest.mark.asyncio
async def test_topic_stream_payloads_filters_null_values() -> None:
    """payloads() unwraps Message.value() and filters None values."""
    consumer = _make_source_consumer()
    consumer.set_watermarks("test-topic", 0, 0, 3)
    consumer.enqueue(
        MockMessage(key=b"k1", value=b"a", offset=0),
        MockMessage(key=b"k2", value=None, offset=1),
        MockMessage(key=b"k3", value=b"c", offset=2),
    )

    original_subscribe = consumer.subscribe

    async def patched_subscribe(topics: list[str], **kw: Any) -> None:
        await original_subscribe(topics, **kw)
        await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

    consumer.subscribe = patched_subscribe  # type: ignore[assignment]

    payloads_stream = topic_as_stream(consumer, ["test-topic"]).payloads()  # type: ignore[arg-type]
    sub = _StreamSubscriber()
    await _watch_stream_until_done(payloads_stream, sub)

    # Only non-null values reach the bytes subscriber.
    assert sub.messages == [b"a", b"c"]
    # All offsets advance — the null-value message acks via _IMMEDIATE_READY.
    await asyncio.sleep(0.05)
    final_committed = max((tp.offset for tp in consumer._committed), default=-1)
    assert final_committed == 3


@pytest.mark.asyncio
async def test_topic_stream_bounds_completed_gap_and_keeps_polling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A slow first offset bounds later completed offsets at the hard window."""
    source_module = sys.modules[TopicStream.__module__]
    monkeypatch.setattr(source_module, "_DEFAULT_MAX_INFLIGHT_READINESS", 2)

    consumer = _make_source_consumer()
    consumer.set_watermarks("test-topic", 0, 0, 4)
    consumer.enqueue(
        *(MockMessage(value=str(offset), offset=offset) for offset in range(4))
    )
    original_subscribe = consumer.subscribe

    async def patched_subscribe(topics: list[str], **kw: Any) -> None:
        await original_subscribe(topics, **kw)
        await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

    consumer.subscribe = patched_subscribe  # type: ignore[assignment]

    first = MockComponentMountHandle()

    class GapSubscriber:
        def __init__(self) -> None:
            self.messages: list[MockMessage] = []

        async def send(self, message: MockMessage) -> Any:
            self.messages.append(message)
            return first if message.offset() == 0 else _IMMEDIATE_READY

        async def mark_ready(self) -> None:
            return

    subscriber = GapSubscriber()
    watch_task = asyncio.create_task(
        _watch_stream_until_done(
            topic_as_stream(consumer, ["test-topic"]),  # type: ignore[arg-type]
            subscriber,  # type: ignore[arg-type]
        )
    )

    await _wait_until(lambda: len(subscriber.messages) == 2)
    await _wait_until(lambda: consumer._paused_polls > 0)
    assert len(subscriber.messages) == 2
    assert len(consumer._messages) == 2
    assert consumer._committed == []

    first.set_ready()
    await asyncio.wait_for(watch_task, timeout=1)

    assert [message.offset() for message in subscriber.messages] == [0, 1, 2, 3]
    assert consumer._paused_polls > 0
    assert [partition.offset for partition in consumer._committed][-1] == 4


@pytest.mark.asyncio
async def test_backpressure_resume_failure_does_not_mask_poll_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_module = sys.modules[TopicStream.__module__]
    monkeypatch.setattr(source_module, "_DEFAULT_MAX_INFLIGHT_READINESS", 1)

    consumer = _make_source_consumer()
    consumer.set_watermarks("test-topic", 0, 0, 2)
    first_message = MockMessage(value=b"first", offset=0)
    consumer._resume_error = RuntimeError("resume cleanup failed")
    poll_count = 0

    async def poll_with_error_while_paused(
        timeout: float = 1.0,
    ) -> MockMessage | None:
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return first_message
        return MockMessage(error_val=MockKafkaError(7, "primary transport failure"))

    consumer.poll = poll_with_error_while_paused  # type: ignore[assignment]
    original_subscribe = consumer.subscribe

    async def patched_subscribe(topics: list[str], **kw: Any) -> None:
        await original_subscribe(topics, **kw)
        await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

    consumer.subscribe = patched_subscribe  # type: ignore[assignment]
    first = MockComponentMountHandle()

    class BlockingSubscriber:
        async def send(self, message: Any) -> MockComponentMountHandle:
            return first

        async def mark_ready(self) -> None:
            return

    watch_task = asyncio.create_task(
        topic_as_stream(consumer, ["test-topic"]).watch(  # type: ignore[arg-type]
            BlockingSubscriber()
        )
    )
    await _wait_until(lambda: consumer._resume_calls == 1)
    assert not watch_task.done()

    first.set_ready()
    with pytest.raises(RuntimeError, match="primary transport failure") as raised:
        await asyncio.wait_for(watch_task, timeout=1)
    assert any(
        "resume cleanup failed" in note
        for note in getattr(raised.value, "__notes__", ())
    )


@pytest.mark.asyncio
async def test_backpressure_resume_failure_is_terminal_without_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_module = sys.modules[TopicStream.__module__]
    monkeypatch.setattr(source_module, "_DEFAULT_MAX_INFLIGHT_READINESS", 1)

    consumer = _make_source_consumer()
    consumer.set_watermarks("test-topic", 0, 0, 2)
    consumer.enqueue(
        MockMessage(value=b"first", offset=0),
        MockMessage(value=b"second", offset=1),
    )
    consumer._resume_error = RuntimeError("resume cleanup failed")
    original_subscribe = consumer.subscribe

    async def patched_subscribe(topics: list[str], **kw: Any) -> None:
        await original_subscribe(topics, **kw)
        await consumer.trigger_assign([MockTopicPartition("test-topic", 0)])

    consumer.subscribe = patched_subscribe  # type: ignore[assignment]
    first = MockComponentMountHandle()

    class BlockingSubscriber:
        def __init__(self) -> None:
            self.messages: list[MockMessage] = []

        async def send(self, message: Any) -> MockComponentMountHandle:
            self.messages.append(message)
            return first

        async def mark_ready(self) -> None:
            return

    subscriber = BlockingSubscriber()
    watch_task = asyncio.create_task(
        topic_as_stream(consumer, ["test-topic"]).watch(  # type: ignore[arg-type]
            subscriber
        )
    )
    await _wait_until(lambda: len(subscriber.messages) == 1)
    await _wait_until(lambda: consumer._paused_polls > 0)
    first.set_ready()

    with pytest.raises(RuntimeError, match="resume cleanup failed"):
        await asyncio.wait_for(watch_task, timeout=1)
    assert len(subscriber.messages) == 1
    assert [partition.offset for partition in consumer._committed] == [1]


@pytest.mark.asyncio
async def test_single_watcher_raises() -> None:
    """A second concurrent watch() on one TopicStream fails loudly."""
    stream = TopicStream(_make_source_consumer(), ["test-topic"])  # type: ignore[arg-type]
    with (
        stream._watch_guard,  # simulate an already-active watch
        pytest.raises(RuntimeError, match="single active watch"),
    ):
        await stream.watch(MagicMock())  # type: ignore[arg-type]
