"""
Iggy source for Synor.

Exposes an Iggy stream/topic/partition as a :class:`LiveStream` of raw
``ReceiveMessage`` objects. It also provides a keyed-map adapter for payloads
that carry an application-level key.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Callable
from datetime import timedelta
from typing import Any

try:
    from apache_iggy import (  # type: ignore[import-not-found]
        AutoCommit,
        IggyClient,
        IggyConsumer,
        PollingStrategy,
        ReceiveMessage,
    )
except ImportError as e:
    raise ImportError(
        "apache-iggy is required to use the Iggy connector. Please install synor[iggy]."
    ) from e

from synor._internal.live_component import (
    _IMMEDIATE_READY,
    LiveMapSubscriber,
    LiveStream,
    LiveStreamSubscriber,
    ReadyAwaitable,
    _await_readiness_succeeded,
    _ReadinessAdmission,
    _ReadinessPermit,
)
from synor._internal.typing import StableKey
from synor.connectorkits import SingleWatcherGuard

_logger = logging.getLogger(__name__)

# Private until deployments demonstrate a need for a public connector knob.
# This bounds messages pulled from Iggy that have not crossed the ordered
# readiness frontier.
_DEFAULT_MAX_INFLIGHT_READINESS = 256


# --- Public type aliases ---

IsDeleteFn = Callable[[ReceiveMessage], bool]
KeyFn = Callable[[ReceiveMessage], StableKey | None]


# --- Per-partition state ---


class _PartitionState:
    """Tracks inflight offsets and stores offsets when safe.

    Iggy stores the last consumed offset, while Kafka commits the next offset.
    Internally this class tracks ``_committed_next_offset`` so readiness uses the
    same comparison as Kafka: caught up when the next offset to consume has
    reached the initial high watermark.
    """

    __slots__ = (
        "_accepting",
        "_committed_next_offset",
        "_completed",
        "_consumer",
        "_failure",
        "_high_watermark",
        "_inflight",
        "_last_tracked_offset",
        "_on_commit",
        "_on_failure",
        "_partition",
        "_pending_store_offset",
        "_store_task",
        "_stream",
        "_tasks",
        "_topic",
    )

    def __init__(
        self,
        consumer: IggyConsumer,
        stream: str,
        topic: str,
        partition: int,
        high_watermark: int,
        committed_next_offset: int,
        on_commit: Callable[[], None],
        on_failure: Callable[[BaseException], None] | None = None,
    ) -> None:
        self._consumer = consumer
        self._stream = stream
        self._topic = topic
        self._partition = partition
        self._inflight: deque[tuple[int, _ReadinessPermit | None]] = deque()
        self._completed: set[int] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self._store_task: asyncio.Task[None] | None = None
        self._pending_store_offset: int | None = None
        self._last_tracked_offset: int | None = None
        self._accepting = True
        self._failure: BaseException | None = None
        self._high_watermark = high_watermark
        self._committed_next_offset = committed_next_offset
        self._on_commit = on_commit
        self._on_failure = on_failure

    def is_caught_up(self) -> bool:
        """Whether this partition has consumed up to its initial high watermark."""
        return self._committed_next_offset >= self._high_watermark

    def track(
        self,
        offset: int,
        handle: ReadyAwaitable,
        permit: _ReadinessPermit | None = None,
    ) -> None:
        """Register an inflight offset with its readiness handle."""
        try:
            if not self._accepting:
                raise RuntimeError(
                    "Iggy partition "
                    f"{self._stream}/{self._topic}/{self._partition} is closed"
                )
            self.raise_if_failed()
            if (
                self._last_tracked_offset is not None
                and offset <= self._last_tracked_offset
            ):
                raise RuntimeError(
                    "Iggy delivered a non-monotonic offset for "
                    f"{self._stream}/{self._topic}/{self._partition}: {offset} after "
                    f"{self._last_tracked_offset}"
                )
            self._last_tracked_offset = offset
            self._inflight.append((offset, permit))
            if handle is _IMMEDIATE_READY:
                self._completed.add(offset)
                self._try_drain_and_store()
                return

            task = asyncio.create_task(self._await_handle(offset, handle))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        except BaseException:
            if permit is not None:
                permit.release()
            raise

    async def _await_handle(
        self,
        offset: int,
        handle: ReadyAwaitable,
    ) -> None:
        """Await a readiness handle and mark the offset as completed."""
        try:
            await _await_readiness_succeeded(handle)
        except asyncio.CancelledError as error:
            self._record_failure(error)
            return
        # Readiness failures are opaque user exceptions; all are terminal.
        except BaseException as error:  # noqa: BLE001
            self._record_failure(error)
            return
        self._completed.add(offset)
        self._try_drain_and_store()

    def _try_drain_and_store(self) -> None:
        """Drain contiguous completed offsets from the front and store the last."""
        last_drained: int | None = None
        while self._inflight and self._inflight[0][0] in self._completed:
            offset, permit = self._inflight.popleft()
            self._completed.discard(offset)
            if permit is not None:
                permit.release()
            last_drained = offset

        if last_drained is not None:
            if (
                self._pending_store_offset is None
                or last_drained > self._pending_store_offset
            ):
                self._pending_store_offset = last_drained
            self._ensure_store_worker()

    def _ensure_store_worker(self) -> None:
        """Start the one serialized, monotonic offset-store worker."""
        if self._failure is not None:
            return
        if self._store_task is None or self._store_task.done():
            self._store_task = asyncio.create_task(self._run_store_worker())

    async def _run_store_worker(self) -> None:
        """Coalesce stores and publish progress only after Iggy acknowledges."""
        try:
            while self._pending_store_offset is not None:
                offset = self._pending_store_offset
                self._pending_store_offset = None
                try:
                    await self._consumer.store_offset(offset, self._partition)
                except asyncio.CancelledError:
                    raise
                # Broker/client implementations expose heterogeneous errors.
                except Exception as error:
                    _logger.exception(
                        "Failed to store offset %d for Iggy %s/%s partition %d",
                        offset,
                        self._stream,
                        self._topic,
                        self._partition,
                    )
                    self._record_failure(error)
                    return

                next_offset = offset + 1
                if next_offset > self._committed_next_offset:
                    self._committed_next_offset = next_offset
                    self._on_commit()
        finally:
            self._store_task = None
            # Linearize worker handoff: progress can become storeable while
            # the current worker is exiting, so pending work must acquire a
            # successor after the task slot is cleared.
            if self._failure is None and self._pending_store_offset is not None:
                self._ensure_store_worker()

    def _record_failure(self, error: BaseException) -> None:
        if self._failure is not None:
            return
        self._failure = error
        if self._on_failure is not None:
            self._on_failure(error)

    def raise_if_failed(self) -> None:
        if self._failure is not None:
            raise self._failure

    async def close(self) -> None:
        """Fence the partition, drain downstream work, then store offsets."""
        self._accepting = False
        tasks = tuple(self._tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        # Follow successor workers as well as the instance observed at entry.
        # Returning after only one task can expose readiness before the final
        # offset-store acknowledgement.
        while True:
            store_task = self._store_task
            if store_task is None:
                if self._failure is None and self._pending_store_offset is not None:
                    self._ensure_store_worker()
                    continue
                break
            await store_task
        while self._inflight:
            _, permit = self._inflight.popleft()
            if permit is not None:
                permit.release()
        self._completed.clear()


# --- Offset tracker ---


class _OffsetTracker:
    """Tracks all partitions this stream is consuming.

    The current Python Iggy SDK does not expose Kafka-style assignment callbacks
    or per-partition high watermarks. This connector therefore requires a known
    initial high watermark for each consumed partition before watching starts.
    """

    __slots__ = (
        "_close_task",
        "_closed",
        "_failure",
        "_initialized",
        "_partitions",
        "failure_event",
        "ready_event",
    )

    def __init__(self) -> None:
        self._partitions: dict[int, _PartitionState] = {}
        self._close_task: asyncio.Task[None] | None = None
        self._closed = False
        self._initialized = False
        self._failure: BaseException | None = None
        self.failure_event = asyncio.Event()
        self.ready_event = asyncio.Event()

    def _record_failure(self, error: BaseException) -> None:
        if self._failure is None:
            self._failure = error
            self.failure_event.set()

    def raise_if_failed(self) -> None:
        if self._failure is not None:
            raise self._failure

    def _check_ready(self) -> None:
        if self._initialized and all(
            state.is_caught_up() for state in self._partitions.values()
        ):
            self.ready_event.set()

    def add(
        self,
        consumer: IggyConsumer,
        stream: str,
        topic: str,
        partition: int,
        high_watermark: int,
        committed_next_offset: int,
    ) -> _PartitionState:
        """Create and register a partition state."""
        state = _PartitionState(
            consumer=consumer,
            stream=stream,
            topic=topic,
            partition=partition,
            high_watermark=high_watermark,
            committed_next_offset=committed_next_offset,
            on_commit=self._check_ready,
            on_failure=self._record_failure,
        )
        self._partitions[partition] = state
        return state

    def get(self, partition: int) -> _PartitionState:
        """Get an initialized partition state."""
        try:
            return self._partitions[partition]
        except KeyError as e:
            raise RuntimeError(
                "Received an Iggy message for an untracked partition. "
                "Use an explicit partition_id per TopicStream instance."
            ) from e

    def mark_initialized(self) -> None:
        """Mark initial partition state loaded and check readiness."""
        self._initialized = True
        self._check_ready()

    async def close_all(self) -> None:
        """Fence and drain all work despite repeated caller cancellation."""
        if not self._closed:
            self._closed = True
            states = tuple(self._partitions.values())
            self._partitions.clear()
            if states:
                self._close_task = asyncio.create_task(self._close_states(states))

        close_task = self._close_task
        if close_task is None:
            self.raise_if_failed()
            return

        cancelled: asyncio.CancelledError | None = None
        while not close_task.done():
            try:
                await asyncio.shield(close_task)
            except asyncio.CancelledError as error:
                # Preserve the caller's first cancellation, while every later
                # cancellation remains unable to detach the readiness/store
                # graph from stream shutdown.
                if cancelled is None:
                    cancelled = error
        close_task.result()
        self.raise_if_failed()
        if cancelled is not None:
            raise cancelled

    async def _close_states(self, states: tuple[_PartitionState, ...]) -> None:
        """Drain every partition and retain the first terminal failure."""
        close_results = await asyncio.gather(
            *(state.close() for state in states),
            return_exceptions=True,
        )
        for result in close_results:
            if isinstance(result, BaseException):
                self._record_failure(result)
        for state in states:
            try:
                state.raise_if_failed()
            except asyncio.CancelledError as error:
                self._record_failure(error)
            # Connector/user failures have no shared concrete base type.
            except Exception as error:  # noqa: BLE001
                self._record_failure(error)
        self.raise_if_failed()


def _committed_next_offset(stored_offset: int | None) -> int:
    """Convert Iggy's last-stored offset into the next offset to consume."""
    return 0 if stored_offset is None else stored_offset + 1


# --- TopicStream: LiveStream[ReceiveMessage] primitive ---


class TopicStream:
    """A :class:`LiveStream` of raw Iggy ``ReceiveMessage`` objects.

    The stream creates an Iggy consumer group with auto-commit disabled, sends
    messages to Synor, and stores offsets only after the returned
    ``ReadyAwaitable`` reports typed success. This mirrors the Kafka connector's
    at-least-once processing contract.
    """

    __slots__ = (
        "_allow_replay",
        "_batch_length",
        "_client",
        "_consumer_group",
        "_init_retries",
        "_init_retry_interval",
        "_initial_high_watermark",
        "_partition_id",
        "_poll_interval",
        "_polling_retry_interval",
        "_stream",
        "_topic",
        "_watch_guard",
    )

    def __init__(
        self,
        client: IggyClient,
        consumer_group: str,
        stream: str,
        topic: str,
        *,
        partition_id: int = 0,
        batch_length: int = 100,
        poll_interval: timedelta | None = None,
        polling_retry_interval: timedelta | None = None,
        init_retries: int | None = None,
        init_retry_interval: timedelta | None = None,
        allow_replay: bool = False,
        initial_high_watermark: int | None = None,
    ) -> None:
        self._client = client
        self._consumer_group = consumer_group
        self._stream = stream
        self._topic = topic
        self._partition_id = partition_id
        self._batch_length = batch_length
        self._poll_interval = poll_interval
        self._polling_retry_interval = polling_retry_interval
        self._init_retries = init_retries
        self._init_retry_interval = init_retry_interval
        self._allow_replay = allow_replay
        self._initial_high_watermark = initial_high_watermark
        self._watch_guard = SingleWatcherGuard("Iggy TopicStream")

    def payloads(self) -> LiveStream[bytes]:
        """View of this stream yielding each message payload as bytes."""
        return _TopicPayloadsStream(self)

    async def _resolve_initial_high_watermark(self) -> int:
        """Resolve the initial next-offset watermark for readiness."""
        if self._initial_high_watermark is not None:
            return self._initial_high_watermark

        topic = await self._client.get_topic(self._stream, self._topic)
        if topic is None:
            raise RuntimeError(
                f"Iggy topic {self._stream}/{self._topic} does not exist."
            )
        if topic.partitions_count != 1:
            raise RuntimeError(
                "The Python Iggy SDK does not expose per-partition high watermarks. "
                "Pass initial_high_watermark for multi-partition topics, or consume "
                "a single-partition topic."
            )
        return int(topic.messages_count)

    async def _create_consumer(self) -> IggyConsumer:
        """Create an Iggy consumer group configured for manual offset storage."""
        return await self._client.consumer_group(
            name=self._consumer_group,
            stream=self._stream,
            topic=self._topic,
            partition_id=self._partition_id,
            polling_strategy=PollingStrategy.Next(),
            batch_length=self._batch_length,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=self._poll_interval,
            polling_retry_interval=self._polling_retry_interval,
            init_retries=self._init_retries,
            init_retry_interval=self._init_retry_interval,
            allow_replay=self._allow_replay,
        )

    async def watch(self, subscriber: LiveStreamSubscriber[ReceiveMessage]) -> None:
        """Consume messages and deliver them to the subscriber."""
        with self._watch_guard:
            await self._watch(subscriber)

    async def _watch(self, subscriber: LiveStreamSubscriber[ReceiveMessage]) -> None:
        high_watermark = await self._resolve_initial_high_watermark()
        consumer = await self._create_consumer()
        tracker = _OffsetTracker()
        admission = _ReadinessAdmission(_DEFAULT_MAX_INFLIGHT_READINESS)
        tracker.add(
            consumer=consumer,
            stream=self._stream,
            topic=self._topic,
            partition=self._partition_id,
            high_watermark=high_watermark,
            committed_next_offset=_committed_next_offset(
                consumer.get_last_stored_offset(self._partition_id)
            ),
        )
        tracker.mark_initialized()

        ready_signaled = False
        active_next_task: (
            asyncio.Task[tuple[ReceiveMessage, _ReadinessPermit]] | None
        ) = None
        active_failure_task: asyncio.Task[bool] | None = None
        last_delivered_offsets: dict[int, int] = {}
        iterator = consumer.iter_messages().__aiter__()

        async def _next_message() -> tuple[ReceiveMessage, _ReadinessPermit]:
            """Reserve capacity before pulling the next Iggy message."""
            if admission.is_full:
                _logger.debug(
                    "Applying Iggy source backpressure at the %d-message "
                    "readiness limit",
                    admission.limit,
                )
            permit = await admission.acquire()
            try:
                message = await anext(iterator)
            except BaseException:
                permit.release()
                raise
            return message, permit

        async def _process_message(
            message: ReceiveMessage, permit: _ReadinessPermit
        ) -> None:
            partition = message.partition_id()
            offset = message.offset()
            last_delivered_offset = last_delivered_offsets.get(partition)
            if last_delivered_offset is not None and offset <= last_delivered_offset:
                _logger.debug(
                    "Skipping duplicate Iggy message for %s/%s partition %d "
                    "offset %d; last delivered offset is %d",
                    self._stream,
                    self._topic,
                    partition,
                    offset,
                    last_delivered_offset,
                )
                permit.release()
                return
            last_delivered_offsets[partition] = offset
            try:
                part_state = tracker.get(partition)
                handle = await subscriber.send(message)
                part_state.track(offset, handle, permit)
            except BaseException:
                permit.release()
                raise

        try:
            while True:
                tracker.raise_if_failed()
                if not ready_signaled:
                    if active_next_task is None:
                        active_next_task = asyncio.create_task(_next_message())
                    ready_task: asyncio.Future[bool] = asyncio.ensure_future(
                        tracker.ready_event.wait()
                    )
                    active_failure_task = asyncio.ensure_future(
                        tracker.failure_event.wait()
                    )
                    wait_set: set[asyncio.Future[Any]] = {
                        active_next_task,
                        ready_task,
                        active_failure_task,
                    }
                    done, _ = await asyncio.wait(
                        wait_set,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if active_failure_task in done:
                        active_next_task.cancel()
                        ready_task.cancel()
                        await asyncio.gather(
                            active_next_task, ready_task, return_exceptions=True
                        )
                        tracker.raise_if_failed()
                    if ready_task in done:
                        await subscriber.mark_ready()
                        ready_signaled = True
                    else:
                        ready_task.cancel()
                    active_failure_task.cancel()
                    await asyncio.gather(
                        ready_task, active_failure_task, return_exceptions=True
                    )

                    if active_next_task in done:
                        message, permit = await active_next_task
                        active_next_task = None
                        await _process_message(message, permit)
                    active_failure_task = None
                    continue

                if active_next_task is None:
                    active_next_task = asyncio.create_task(_next_message())
                active_failure_task = asyncio.ensure_future(
                    tracker.failure_event.wait()
                )
                done, _ = await asyncio.wait(
                    {active_next_task, active_failure_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if active_failure_task in done:
                    active_next_task.cancel()
                    await asyncio.gather(active_next_task, return_exceptions=True)
                    tracker.raise_if_failed()
                active_failure_task.cancel()
                await asyncio.gather(active_failure_task, return_exceptions=True)
                message, permit = await active_next_task
                active_next_task = None
                active_failure_task = None
                await _process_message(message, permit)
        except StopAsyncIteration:
            # A finite/mock iterator must not report completion before all
            # eligible offsets are durably stored.
            await tracker.close_all()
            tracker.raise_if_failed()
            if not ready_signaled and tracker.ready_event.is_set():
                await subscriber.mark_ready()
                ready_signaled = True
            return
        finally:

            async def _shutdown() -> None:
                if active_next_task is not None:
                    if not active_next_task.done():
                        active_next_task.cancel()
                    next_result = await asyncio.gather(
                        active_next_task, return_exceptions=True
                    )
                    if next_result and isinstance(next_result[0], tuple):
                        # A completed iterator pull may have raced readiness,
                        # failure, or cancellation before delivery.
                        next_result[0][1].release()
                if active_failure_task is not None:
                    active_failure_task.cancel()
                    await asyncio.gather(active_failure_task, return_exceptions=True)
                await tracker.close_all()
                tracker.raise_if_failed()

            shutdown_task = asyncio.create_task(_shutdown())
            cancelled: asyncio.CancelledError | None = None
            while not shutdown_task.done():
                try:
                    await asyncio.shield(shutdown_task)
                except asyncio.CancelledError as error:
                    if cancelled is None:
                        cancelled = error
            shutdown_task.result()
            if cancelled is not None:
                raise cancelled


class _TopicPayloadsStream:
    """``LiveStream[bytes]`` view over a :class:`TopicStream`."""

    __slots__ = ("_source",)

    def __init__(self, source: TopicStream) -> None:
        self._source = source

    async def watch(self, subscriber: LiveStreamSubscriber[bytes]) -> None:
        await self._source.watch(_PayloadsAdapter(subscriber))


class _PayloadsAdapter:
    """Adapts a ``LiveStreamSubscriber[bytes]`` to receive Iggy messages."""

    __slots__ = ("_downstream",)

    def __init__(self, downstream: LiveStreamSubscriber[bytes]) -> None:
        self._downstream = downstream

    async def send(self, message: ReceiveMessage) -> ReadyAwaitable:
        return await self._downstream.send(message.payload())

    async def mark_ready(self) -> None:
        await self._downstream.mark_ready()


# --- LiveMapFeed implementation ---


class _StreamToMapSubscriber:
    """Adapts a :class:`LiveMapSubscriber` to consume a ``LiveStream``."""

    __slots__ = ("_is_deletion", "_key", "_map_sub")

    def __init__(
        self,
        map_sub: LiveMapSubscriber[StableKey, ReceiveMessage],
        key: KeyFn,
        is_deletion: IsDeleteFn | None,
    ) -> None:
        self._map_sub = map_sub
        self._key = key
        self._is_deletion = is_deletion

    async def send(self, message: ReceiveMessage) -> ReadyAwaitable:
        key = self._key(message)
        if key is None:
            _logger.error(
                "Skipping Iggy message without application key at partition %d "
                "offset %d",
                message.partition_id(),
                message.offset(),
            )
            return _IMMEDIATE_READY
        if self._is_deletion is not None and self._is_deletion(message):
            return await self._map_sub.delete(key)
        return await self._map_sub.update(key, message)

    async def mark_ready(self) -> None:
        await self._map_sub.mark_ready()


class _TopicMapFeed:
    """``LiveMapFeed`` view over a :class:`TopicStream`."""

    __slots__ = ("_is_deletion", "_key", "_stream")

    def __init__(
        self,
        stream: TopicStream,
        key: KeyFn,
        is_deletion: IsDeleteFn | None,
    ) -> None:
        self._stream = stream
        self._key = key
        self._is_deletion = is_deletion

    async def watch(
        self, subscriber: LiveMapSubscriber[StableKey, ReceiveMessage]
    ) -> None:
        await self._stream.watch(
            _StreamToMapSubscriber(subscriber, self._key, self._is_deletion)
        )


# --- Public API ---


def topic_as_stream(
    client: IggyClient,
    consumer_group: str,
    stream: str,
    topic: str,
    *,
    partition_id: int = 0,
    batch_length: int = 100,
    poll_interval: timedelta | None = None,
    polling_retry_interval: timedelta | None = None,
    init_retries: int | None = None,
    init_retry_interval: timedelta | None = None,
    allow_replay: bool = False,
    initial_high_watermark: int | None = None,
) -> TopicStream:
    """
    Treat an Iggy stream/topic/partition as a :class:`LiveStream`.

    ``initial_high_watermark`` is the initial next offset for readiness. It is
    optional for single-partition topics because the connector can use
    ``TopicDetails.messages_count``. For multi-partition topics the Python SDK
    does not currently expose per-partition watermarks, so callers must provide
    the exact partition watermark to preserve Kafka-strength readiness semantics.
    """
    return TopicStream(
        client,
        consumer_group,
        stream,
        topic,
        partition_id=partition_id,
        batch_length=batch_length,
        poll_interval=poll_interval,
        polling_retry_interval=polling_retry_interval,
        init_retries=init_retries,
        init_retry_interval=init_retry_interval,
        allow_replay=allow_replay,
        initial_high_watermark=initial_high_watermark,
    )


def topic_as_map(
    client: IggyClient,
    consumer_group: str,
    stream: str,
    topic: str,
    *,
    key: KeyFn,
    partition_id: int = 0,
    batch_length: int = 100,
    poll_interval: timedelta | None = None,
    polling_retry_interval: timedelta | None = None,
    init_retries: int | None = None,
    init_retry_interval: timedelta | None = None,
    allow_replay: bool = False,
    initial_high_watermark: int | None = None,
    is_deletion: IsDeleteFn | None = None,
) -> _TopicMapFeed:
    """
    Treat an Iggy stream/topic/partition as a live keyed map.

    Iggy Python messages do not expose Kafka-style message keys or tombstones,
    so callers must provide ``key`` to extract an application-level key from the
    message payload or metadata. Use ``is_deletion`` for application-level
    delete events.
    """
    return _TopicMapFeed(
        topic_as_stream(
            client,
            consumer_group,
            stream,
            topic,
            partition_id=partition_id,
            batch_length=batch_length,
            poll_interval=poll_interval,
            polling_retry_interval=polling_retry_interval,
            init_retries=init_retries,
            init_retry_interval=init_retry_interval,
            allow_replay=allow_replay,
            initial_high_watermark=initial_high_watermark,
        ),
        key,
        is_deletion,
    )


__all__ = [
    "IsDeleteFn",
    "KeyFn",
    "TopicStream",
    "topic_as_map",
    "topic_as_stream",
]
