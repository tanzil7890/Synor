"""Tests for the Iggy source connector.

These tests mock the Python Iggy SDK to verify Synor source semantics
without a real Iggy server.
"""

from __future__ import annotations

import asyncio
import sys
from collections import deque
from typing import Any
from unittest.mock import MagicMock

import pytest


class MockAutoCommit:
    class Disabled:
        pass


class MockPollingStrategy:
    class Next:
        pass


class MockReceiveMessage:
    def __init__(
        self,
        *,
        payload: bytes,
        offset: int,
        partition_id: int = 0,
    ) -> None:
        self._payload = payload
        self._offset = offset
        self._partition_id = partition_id

    def payload(self) -> bytes:
        return self._payload

    def offset(self) -> int:
        return self._offset

    def partition_id(self) -> int:
        return self._partition_id


class MockTopicDetails:
    def __init__(self, *, messages_count: int, partitions_count: int = 1) -> None:
        self.messages_count = messages_count
        self.partitions_count = partitions_count


class MockReadyHandle:
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


class MockMessageIterator:
    def __init__(self, messages: deque[MockReceiveMessage]) -> None:
        self._messages = messages

    def __aiter__(self) -> MockMessageIterator:
        return self

    async def __anext__(self) -> MockReceiveMessage:
        if self._messages:
            return self._messages.popleft()
        raise StopAsyncIteration


class MockIggyConsumer:
    def __init__(
        self,
        messages: list[MockReceiveMessage],
        *,
        stored_offset: int | None = None,
    ) -> None:
        self._messages = deque(messages)
        self._stored_offset = stored_offset
        self.stored_offsets: list[tuple[int, int | None]] = []
        self.store_attempts: list[tuple[int, int | None]] = []
        self.store_gate: asyncio.Event | None = None
        self.store_error: BaseException | None = None
        self.active_stores = 0
        self.max_active_stores = 0

    def get_last_stored_offset(self, partition_id: int) -> int | None:
        return self._stored_offset

    async def store_offset(self, offset: int, partition_id: int | None) -> None:
        self.active_stores += 1
        self.max_active_stores = max(self.max_active_stores, self.active_stores)
        self.store_attempts.append((offset, partition_id))
        try:
            if self.store_gate is not None:
                await self.store_gate.wait()
            if self.store_error is not None:
                raise self.store_error
            self._stored_offset = offset
            self.stored_offsets.append((offset, partition_id))
        finally:
            self.active_stores -= 1

    def iter_messages(self) -> MockMessageIterator:
        return MockMessageIterator(self._messages)


async def _wait_until(predicate: Any) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition did not become true")


class MockIggyClient:
    def __init__(
        self,
        consumer: MockIggyConsumer,
        *,
        messages_count: int,
        partitions_count: int = 1,
    ) -> None:
        self.consumer = consumer
        self.topic = MockTopicDetails(
            messages_count=messages_count,
            partitions_count=partitions_count,
        )
        self.consumer_group_calls: list[dict[str, Any]] = []

    async def get_topic(self, stream: str, topic: str) -> MockTopicDetails:
        return self.topic

    async def consumer_group(self, **kwargs: Any) -> MockIggyConsumer:
        self.consumer_group_calls.append(kwargs)
        return self.consumer


_mock_module = MagicMock()
_mock_module.AutoCommit = MockAutoCommit
_mock_module.IggyClient = MockIggyClient
_mock_module.IggyConsumer = MockIggyConsumer
_mock_module.PollingStrategy = MockPollingStrategy
_mock_module.ReceiveMessage = MockReceiveMessage
_mock_module.SendMessage = MagicMock()
sys.modules["apache_iggy"] = _mock_module

from synor._internal.api import (
    Cancelled,
    Failed,
    ReadinessOutcome,
    Succeeded,
    Superseded,
)
from synor._internal.live_component import _IMMEDIATE_READY
from synor.connectors.iggy._source import (
    TopicStream,
    _PartitionState,
    topic_as_map,
    topic_as_stream,
)


class MockOutcomeHandle:
    """Typed handle that catches accidental use of compatibility ``ready()``."""

    def __init__(self, outcome: ReadinessOutcome) -> None:
        self._outcome = outcome

    async def ready(self) -> None:
        raise AssertionError("typed source acknowledgement must use outcome()")

    async def outcome(self) -> ReadinessOutcome:
        return self._outcome


class MockStreamSubscriber:
    def __init__(self, *, auto_ready: bool = True) -> None:
        self.messages: list[MockReceiveMessage] = []
        self.handles: list[MockReadyHandle] = []
        self.ready_called = False
        self._auto_ready = auto_ready

    async def send(self, message: MockReceiveMessage) -> MockReadyHandle:
        self.messages.append(message)
        handle = MockReadyHandle()
        self.handles.append(handle)
        if self._auto_ready:
            handle.set_ready()
        return handle

    async def mark_ready(self) -> None:
        self.ready_called = True


class MockMapSubscriber:
    def __init__(self) -> None:
        self.updates: list[tuple[bytes | str, MockReceiveMessage]] = []
        self.deletes: list[bytes | str] = []
        self.ready_called = False

    async def update(
        self, key: bytes | str, value: MockReceiveMessage
    ) -> MockReadyHandle:
        self.updates.append((key, value))
        handle = MockReadyHandle()
        handle.set_ready()
        return handle

    async def delete(self, key: bytes | str) -> MockReadyHandle:
        self.deletes.append(key)
        handle = MockReadyHandle()
        handle.set_ready()
        return handle

    async def update_all(self) -> None:
        pass

    async def mark_ready(self) -> None:
        self.ready_called = True


class TestPartitionState:
    @pytest.mark.asyncio
    async def test_stores_last_consumed_offset_after_contiguous_completion(
        self,
    ) -> None:
        consumer = MockIggyConsumer([])
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "stream",
            "topic",
            0,
            high_watermark=3,
            committed_next_offset=0,
            on_commit=lambda: None,
        )

        handles = [MockReadyHandle() for _ in range(3)]
        for offset, handle in enumerate(handles):
            state.track(offset, handle)

        handles[2].set_ready()
        await asyncio.sleep(0.01)
        assert consumer.stored_offsets == []

        handles[0].set_ready()
        handles[1].set_ready()
        await asyncio.sleep(0.05)

        assert consumer.stored_offsets[-1] == (2, 0)

    @pytest.mark.asyncio
    async def test_immediate_ready_fast_path(self) -> None:
        consumer = MockIggyConsumer([])
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "stream",
            "topic",
            0,
            high_watermark=1,
            committed_next_offset=0,
            on_commit=lambda: None,
        )

        state.track(0, _IMMEDIATE_READY)
        await asyncio.sleep(0.05)

        assert consumer.stored_offsets == [(0, 0)]

    @pytest.mark.asyncio
    async def test_downstream_failure_is_terminal_and_never_stored(self) -> None:
        consumer = MockIggyConsumer([])
        failed = asyncio.Event()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "stream",
            "topic",
            0,
            high_watermark=2,
            committed_next_offset=0,
            on_commit=lambda: None,
            on_failure=lambda _error: failed.set(),
        )
        first = MockReadyHandle()
        second = MockReadyHandle()
        state.track(0, first)
        state.track(1, second)

        second.set_ready()
        first.set_error(ValueError("downstream failed"))
        await asyncio.wait_for(failed.wait(), timeout=1)
        await state.close()

        with pytest.raises(ValueError, match="downstream failed"):
            state.raise_if_failed()
        assert consumer.stored_offsets == []
        assert not state.is_caught_up()

    @pytest.mark.asyncio
    async def test_typed_succeeded_is_the_only_outcome_that_stores(self) -> None:
        consumer = MockIggyConsumer([])
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "stream",
            "topic",
            0,
            high_watermark=1,
            committed_next_offset=0,
            on_commit=lambda: None,
        )

        state.track(0, MockOutcomeHandle(Succeeded()))  # type: ignore[arg-type]
        await _wait_until(lambda: consumer.stored_offsets == [(0, 0)])

        assert state.is_caught_up()

    @pytest.mark.asyncio
    async def test_untyped_readiness_fails_closed_without_storing(self) -> None:
        consumer = MockIggyConsumer([])
        failed = asyncio.Event()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "stream",
            "topic",
            0,
            high_watermark=1,
            committed_next_offset=0,
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
        assert consumer.stored_offsets == []
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
    async def test_non_success_typed_outcomes_never_store(
        self,
        outcome: ReadinessOutcome,
        error_type: type[BaseException],
        message: str,
    ) -> None:
        consumer = MockIggyConsumer([])
        failed = asyncio.Event()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "stream",
            "topic",
            0,
            high_watermark=1,
            committed_next_offset=0,
            on_commit=lambda: None,
            on_failure=lambda _error: failed.set(),
        )

        state.track(0, MockOutcomeHandle(outcome))  # type: ignore[arg-type]
        await asyncio.wait_for(failed.wait(), timeout=1)
        await state.close()

        with pytest.raises(error_type, match=message):
            state.raise_if_failed()
        assert consumer.stored_offsets == []
        assert not state.is_caught_up()

    @pytest.mark.asyncio
    async def test_progress_waits_for_store_acknowledgement(self) -> None:
        consumer = MockIggyConsumer([])
        consumer.store_gate = asyncio.Event()
        commit_callbacks = 0

        def on_commit() -> None:
            nonlocal commit_callbacks
            commit_callbacks += 1

        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "stream",
            "topic",
            0,
            high_watermark=1,
            committed_next_offset=0,
            on_commit=on_commit,
        )
        state.track(0, _IMMEDIATE_READY)
        await _wait_until(lambda: consumer.store_attempts == [(0, 0)])

        assert not state.is_caught_up()
        assert commit_callbacks == 0
        assert consumer.stored_offsets == []

        consumer.store_gate.set()
        await state.close()
        assert state.is_caught_up()
        assert commit_callbacks == 1
        assert consumer.stored_offsets == [(0, 0)]

    @pytest.mark.asyncio
    async def test_store_failure_does_not_advance_progress(self) -> None:
        consumer = MockIggyConsumer([])
        consumer.store_error = RuntimeError("store rejected")
        failed = asyncio.Event()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "stream",
            "topic",
            0,
            high_watermark=1,
            committed_next_offset=0,
            on_commit=lambda: None,
            on_failure=lambda _error: failed.set(),
        )
        state.track(0, _IMMEDIATE_READY)
        await asyncio.wait_for(failed.wait(), timeout=1)

        with pytest.raises(RuntimeError, match="store rejected"):
            state.raise_if_failed()
        assert consumer.stored_offsets == []
        assert not state.is_caught_up()

    @pytest.mark.asyncio
    async def test_coalesced_stores_are_serialized_and_monotonic(self) -> None:
        consumer = MockIggyConsumer([])
        consumer.store_gate = asyncio.Event()
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "stream",
            "topic",
            0,
            high_watermark=3,
            committed_next_offset=0,
            on_commit=lambda: None,
        )

        state.track(0, _IMMEDIATE_READY)
        await _wait_until(lambda: consumer.store_attempts == [(0, 0)])
        state.track(1, _IMMEDIATE_READY)
        state.track(2, _IMMEDIATE_READY)
        consumer.store_gate.set()
        await state.close()

        assert consumer.store_attempts == [(0, 0), (2, 0)]
        assert consumer.stored_offsets == [(0, 0), (2, 0)]
        assert consumer.max_active_stores == 1

    @pytest.mark.asyncio
    async def test_close_follows_store_worker_handoff(self) -> None:
        second_started = asyncio.Event()
        second_gate = asyncio.Event()

        class HandoffConsumer(MockIggyConsumer):
            async def store_offset(self, offset: int, partition_id: int | None) -> None:
                self.store_attempts.append((offset, partition_id))
                if offset == 1:
                    second_started.set()
                    await second_gate.wait()
                self.stored_offsets.append((offset, partition_id))

        consumer = HandoffConsumer([])

        class HandoffState(_PartitionState):
            _handed_off = False

            async def _run_store_worker(self) -> None:
                await super()._run_store_worker()
                if not self._handed_off:
                    self._handed_off = True
                    self._pending_store_offset = 1
                    self._ensure_store_worker()

        state = HandoffState(
            consumer,  # type: ignore[arg-type]
            "stream",
            "topic",
            0,
            high_watermark=2,
            committed_next_offset=0,
            on_commit=lambda: None,
        )
        state.track(0, _IMMEDIATE_READY)

        close_task = asyncio.create_task(state.close())
        await asyncio.wait_for(second_started.wait(), timeout=1)
        assert not close_task.done()

        second_gate.set()
        await asyncio.wait_for(close_task, timeout=1)
        assert consumer.store_attempts == [(0, 0), (1, 0)]
        assert consumer.stored_offsets == [(0, 0), (1, 0)]

    @pytest.mark.asyncio
    async def test_close_drains_readiness_before_storing(self) -> None:
        consumer = MockIggyConsumer([])
        state = _PartitionState(
            consumer,  # type: ignore[arg-type]
            "stream",
            "topic",
            0,
            high_watermark=1,
            committed_next_offset=0,
            on_commit=lambda: None,
        )
        handle = MockReadyHandle()
        state.track(0, handle)

        close = asyncio.create_task(state.close())
        await asyncio.sleep(0)
        assert not close.done()
        assert consumer.stored_offsets == []

        handle.set_ready()
        await asyncio.wait_for(close, timeout=1)
        assert consumer.stored_offsets == [(0, 0)]


class TestTopicStream:
    @pytest.mark.asyncio
    async def test_stream_consumes_and_stores_offsets_after_ready(self) -> None:
        consumer = MockIggyConsumer(
            [
                MockReceiveMessage(payload=b"v1", offset=0),
                MockReceiveMessage(payload=b"v2", offset=1),
            ]
        )
        client = MockIggyClient(consumer, messages_count=2)
        stream = topic_as_stream(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
        )
        subscriber = MockStreamSubscriber()

        await stream.watch(subscriber)  # type: ignore[arg-type]
        await asyncio.sleep(0.05)

        assert [m.payload() for m in subscriber.messages] == [b"v1", b"v2"]
        assert consumer.stored_offsets[-1] == (1, 0)
        assert subscriber.ready_called
        assert client.consumer_group_calls[0]["auto_commit"].__class__.__name__ == (
            "Disabled"
        )

    @pytest.mark.asyncio
    async def test_stream_surfaces_failed_ready_handle_without_storing(self) -> None:
        consumer = MockIggyConsumer([MockReceiveMessage(payload=b"v1", offset=0)])
        client = MockIggyClient(consumer, messages_count=1)
        stream = topic_as_stream(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
        )

        class FailingSubscriber(MockStreamSubscriber):
            async def send(self, message: MockReceiveMessage) -> MockReadyHandle:
                handle = await super().send(message)
                handle.set_error(ValueError("sink was not durable"))
                return handle

        subscriber = FailingSubscriber(auto_ready=False)
        with pytest.raises(ValueError, match="sink was not durable"):
            await stream.watch(subscriber)  # type: ignore[arg-type]

        assert consumer.stored_offsets == []
        assert not subscriber.ready_called

    @pytest.mark.asyncio
    async def test_ready_race_reuses_pending_iterator_task(self) -> None:
        class BlockingIterator(MockMessageIterator):
            def __init__(self) -> None:
                self.calls = 0
                self.concurrent = 0
                self.max_concurrent = 0
                self.started = asyncio.Event()

            def __aiter__(self) -> BlockingIterator:
                return self

            async def __anext__(self) -> MockReceiveMessage:
                self.calls += 1
                self.concurrent += 1
                self.max_concurrent = max(self.max_concurrent, self.concurrent)
                self.started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    self.concurrent -= 1
                raise StopAsyncIteration

        iterator = BlockingIterator()

        class BlockingConsumer(MockIggyConsumer):
            def iter_messages(self) -> BlockingIterator:
                return iterator

        consumer = BlockingConsumer([])
        client = MockIggyClient(consumer, messages_count=0)
        stream = topic_as_stream(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
        )
        subscriber = MockStreamSubscriber()

        watch = asyncio.create_task(stream.watch(subscriber))  # type: ignore[arg-type]
        await asyncio.wait_for(iterator.started.wait(), timeout=1)
        await _wait_until(lambda: subscriber.ready_called)
        await asyncio.sleep(0)

        assert iterator.calls == 1
        assert iterator.max_concurrent == 1
        watch.cancel()
        with pytest.raises(asyncio.CancelledError):
            await watch

    @pytest.mark.asyncio
    async def test_finite_eof_marks_ready_after_final_store_ack(self) -> None:
        consumer = MockIggyConsumer([MockReceiveMessage(payload=b"v1", offset=0)])
        client = MockIggyClient(consumer, messages_count=1)
        stream = topic_as_stream(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
        )
        subscriber = MockStreamSubscriber(auto_ready=False)

        watch = asyncio.create_task(stream.watch(subscriber))  # type: ignore[arg-type]
        await _wait_until(lambda: len(subscriber.handles) == 1)
        await asyncio.sleep(0)
        assert not subscriber.ready_called
        assert not watch.done()

        subscriber.handles[0].set_ready()
        await asyncio.wait_for(watch, timeout=1)

        assert consumer.stored_offsets == [(0, 0)]
        assert subscriber.ready_called

    @pytest.mark.asyncio
    async def test_stream_skips_duplicate_offsets_from_live_consumer(self) -> None:
        consumer = MockIggyConsumer(
            [
                MockReceiveMessage(payload=b"v1", offset=0),
                MockReceiveMessage(payload=b"v2", offset=1),
                MockReceiveMessage(payload=b"v2-duplicate", offset=1),
                MockReceiveMessage(payload=b"v3", offset=2),
            ]
        )
        client = MockIggyClient(consumer, messages_count=3)
        stream = topic_as_stream(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
        )
        subscriber = MockStreamSubscriber()

        await stream.watch(subscriber)  # type: ignore[arg-type]
        await asyncio.sleep(0.05)

        assert [(m.offset(), m.payload()) for m in subscriber.messages] == [
            (0, b"v1"),
            (1, b"v2"),
            (2, b"v3"),
        ]
        assert consumer.stored_offsets[-1] == (2, 0)
        assert subscriber.ready_called

    @pytest.mark.asyncio
    async def test_stream_bounds_completed_gap_before_pulling_more(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        source_module = sys.modules[TopicStream.__module__]
        monkeypatch.setattr(source_module, "_DEFAULT_MAX_INFLIGHT_READINESS", 2)
        consumer = MockIggyConsumer(
            [
                MockReceiveMessage(payload=str(offset).encode(), offset=offset)
                for offset in range(4)
            ]
        )
        client = MockIggyClient(consumer, messages_count=4)
        first = MockReadyHandle()

        class GapSubscriber:
            def __init__(self) -> None:
                self.messages: list[MockReceiveMessage] = []

            async def send(self, message: MockReceiveMessage) -> Any:
                self.messages.append(message)
                return first if message.offset() == 0 else _IMMEDIATE_READY

            async def mark_ready(self) -> None:
                return

        subscriber = GapSubscriber()
        stream = topic_as_stream(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
        )
        watch_task = asyncio.create_task(
            stream.watch(subscriber)  # type: ignore[arg-type]
        )

        await _wait_until(lambda: len(subscriber.messages) == 2)
        await asyncio.sleep(0)
        assert len(subscriber.messages) == 2
        assert len(consumer._messages) == 2
        assert consumer.stored_offsets == []

        first.set_ready()
        await asyncio.wait_for(watch_task, timeout=1)

        assert [message.offset() for message in subscriber.messages] == [0, 1, 2, 3]
        assert consumer.stored_offsets[-1] == (3, 0)

    @pytest.mark.asyncio
    async def test_stream_shutdown_drains_after_repeated_cancellation(self) -> None:
        consumer = MockIggyConsumer([MockReceiveMessage(payload=b"v1", offset=0)])
        consumer.store_gate = asyncio.Event()
        client = MockIggyClient(consumer, messages_count=1)
        subscriber = MockStreamSubscriber(auto_ready=False)
        stream = topic_as_stream(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
        )
        watch_task = asyncio.create_task(
            stream.watch(subscriber)  # type: ignore[arg-type]
        )
        await _wait_until(lambda: len(subscriber.handles) == 1)

        watch_task.cancel("first cancellation")
        await asyncio.sleep(0)
        assert not watch_task.done()
        watch_task.cancel("second cancellation")
        await asyncio.sleep(0)
        assert not watch_task.done()

        subscriber.handles[0].set_ready()
        await _wait_until(lambda: consumer.store_attempts == [(0, 0)])
        watch_task.cancel("third cancellation")
        await asyncio.sleep(0)
        assert not watch_task.done()

        consumer.store_gate.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(watch_task, timeout=1)
        assert consumer.stored_offsets == [(0, 0)]

    @pytest.mark.asyncio
    async def test_payloads_view_forwards_bytes(self) -> None:
        consumer = MockIggyConsumer([MockReceiveMessage(payload=b"payload", offset=0)])
        client = MockIggyClient(consumer, messages_count=1)
        payloads = topic_as_stream(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
        ).payloads()

        class BytesSubscriber:
            def __init__(self) -> None:
                self.payloads: list[bytes] = []
                self.ready_called = False

            async def send(self, payload: bytes) -> MockReadyHandle:
                self.payloads.append(payload)
                handle = MockReadyHandle()
                handle.set_ready()
                return handle

            async def mark_ready(self) -> None:
                self.ready_called = True

        subscriber = BytesSubscriber()
        await payloads.watch(subscriber)
        await asyncio.sleep(0.05)

        assert subscriber.payloads == [b"payload"]
        assert subscriber.ready_called

    @pytest.mark.asyncio
    async def test_multi_partition_requires_explicit_watermark(self) -> None:
        consumer = MockIggyConsumer([])
        client = MockIggyClient(consumer, messages_count=10, partitions_count=2)
        stream = TopicStream(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
        )

        with pytest.raises(RuntimeError, match="per-partition high watermarks"):
            await stream.watch(MockStreamSubscriber())  # type: ignore[arg-type]


class TestTopicMap:
    @pytest.mark.asyncio
    async def test_map_uses_application_key_and_deletion_predicate(self) -> None:
        consumer = MockIggyConsumer(
            [
                MockReceiveMessage(payload=b"k1:v1", offset=0),
                MockReceiveMessage(payload=b"k1:DELETE", offset=1),
            ]
        )
        client = MockIggyClient(consumer, messages_count=2)
        feed = topic_as_map(
            client,  # type: ignore[arg-type]
            "group",
            "stream",
            "topic",
            key=lambda msg: msg.payload().split(b":", 1)[0],
            is_deletion=lambda msg: msg.payload().endswith(b"DELETE"),
        )
        subscriber = MockMapSubscriber()

        await feed.watch(subscriber)  # type: ignore[arg-type]
        await asyncio.sleep(0.05)

        assert [(k, m.payload()) for k, m in subscriber.updates] == [(b"k1", b"k1:v1")]
        assert subscriber.deletes == [b"k1"]
        assert subscriber.ready_called
