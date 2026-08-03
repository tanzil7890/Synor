"""
Kafka source for Synor.

Exposes a Kafka topic as a :class:`LiveStream` of raw messages and as a
:class:`LiveMapFeed` of keyed change events. ``topic_as_stream`` returns the
primitive stream (with a ``payloads()`` view yielding bytes), and
``topic_as_map`` interprets messages as a keyed map for use with ``mount_each``.

User-facing docs and worked examples are stored locally at
``docs/src/content/docs/connectors/kafka.mdx``.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
import weakref
from collections import deque
from collections.abc import Callable, Iterable, Mapping

try:
    from confluent_kafka import (  # type: ignore[import-not-found]
        KafkaError,
        Message,
        TopicPartition,
    )
    from confluent_kafka.aio import AIOConsumer  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "confluent_kafka is required to use the Kafka connector. "
        "Please install synor[kafka]."
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
# This bounds messages that have not crossed the ordered readiness frontier;
# Kafka partition pausing keeps the window hard-bounded without starving poll.
_DEFAULT_MAX_INFLIGHT_READINESS = 256
_POLL_TIMEOUT = 1.0


# --- Public type aliases ---

IsDeleteFn = Callable[[Message], bool]


# Helper-created consumers transfer ownership to the first TopicStream that binds
# them. Keep both states so a claimed helper consumer can never fall through and
# be mistaken for an unregistered consumer on a later bind. The lock makes the
# claim atomic even when streams are assembled from two threads.
_available_helper_consumers: weakref.WeakSet[AIOConsumer] = weakref.WeakSet()
_claimed_helper_consumers: weakref.WeakSet[AIOConsumer] = weakref.WeakSet()
_helper_consumer_offset_resets: weakref.WeakKeyDictionary[AIOConsumer, str] = (
    weakref.WeakKeyDictionary()
)
_consumer_ownership_lock = threading.Lock()


def _normalize_offset_reset(value: object) -> str:
    """Normalize librdkafka reset aliases used to infer an assignment start."""
    if not isinstance(value, str):
        raise TypeError("Kafka auto.offset.reset must be a string")
    normalized = value.strip().lower()
    if normalized in {"earliest", "smallest", "beginning"}:
        return "earliest"
    if normalized in {"latest", "largest", "end"}:
        return "latest"
    if normalized == "error":
        return "error"
    raise ValueError(
        "Kafka sources support auto.offset.reset values earliest, latest, or "
        "error (including librdkafka aliases)"
    )


def create_consumer(config: Mapping[str, object]) -> AIOConsumer:
    """Create a single-use ``AIOConsumer`` owned by its Kafka source stream.

    The caller's mapping is copied. Both Confluent automatic offset mechanisms
    are then forced off so only Synor's acknowledged downstream-completion path
    can advance the consumer-group offset. Once passed to ``topic_as_stream()``
    or ``topic_as_map()``, the consumer is owned and eventually closed by that
    stream; it cannot be bound or watched again.
    """
    safe_config = dict(config)
    safe_config["enable.auto.commit"] = False
    safe_config["enable.auto.offset.store"] = False
    offset_reset = _normalize_offset_reset(
        safe_config.get("auto.offset.reset", "latest")
    )
    consumer = AIOConsumer(safe_config)
    with _consumer_ownership_lock:
        _available_helper_consumers.add(consumer)
        _helper_consumer_offset_resets[consumer] = offset_reset
    return consumer


def _claim_helper_consumer(consumer: AIOConsumer) -> str:
    """Claim a safely configured helper consumer and return its reset policy."""
    with _consumer_ownership_lock:
        try:
            if consumer in _claimed_helper_consumers:
                raise RuntimeError(
                    "A Kafka consumer returned by create_consumer() is single-use "
                    "and is already owned by another TopicStream"
                )
            if consumer not in _available_helper_consumers:
                raise ValueError(
                    "Kafka sources require an AIOConsumer returned by "
                    "synor.connectors.kafka.create_consumer(); Confluent does "
                    "not expose an existing consumer's effective offset "
                    "configuration, so Synor cannot safely accept a directly "
                    "constructed consumer"
                )
            try:
                offset_reset = _helper_consumer_offset_resets[consumer]
            except KeyError as error:
                raise RuntimeError(
                    "Kafka helper consumer is missing its verified offset-reset "
                    "configuration"
                ) from error
            _claimed_helper_consumers.add(consumer)
            _available_helper_consumers.discard(consumer)
            return offset_reset
        except TypeError as error:
            raise ValueError(
                "Kafka sources require the hashable AIOConsumer returned by "
                "synor.connectors.kafka.create_consumer()"
            ) from error


def _effective_start_offset(
    committed_offset: int,
    low_watermark: int,
    high_watermark: int,
    offset_reset: str,
) -> int:
    """Resolve the first consumable offset for readiness accounting.

    Kafka returns a negative sentinel for a new group, and retention can move
    an older committed offset below the current low watermark. In both cases
    the consumer follows ``auto.offset.reset``; readiness must use the same
    effective position or a latest-starting/empty retained log can wait
    forever for records Kafka will never deliver.
    """
    if low_watermark < 0 or high_watermark < low_watermark:
        raise RuntimeError(
            "Kafka watermark lookup returned an invalid range: "
            f"low={low_watermark}, high={high_watermark}"
        )
    if low_watermark <= committed_offset <= high_watermark:
        return committed_offset
    if offset_reset == "earliest":
        return low_watermark
    if offset_reset == "latest":
        return high_watermark
    if offset_reset == "error":
        raise RuntimeError(
            "Kafka has no valid committed offset and auto.offset.reset=error"
        )
    raise RuntimeError(f"unknown Kafka auto.offset.reset policy: {offset_reset!r}")


def _topic_partition_error(partition: TopicPartition) -> object:
    """Return either spelling used by Confluent partition results."""
    error = getattr(partition, "err", None)
    if error is None:
        error = getattr(partition, "error", None)
    return error() if callable(error) else error


def _is_partition_eof(error: object) -> bool:
    """Whether a message error is Kafka's informational partition EOF."""
    code = getattr(error, "code", None)
    eof_code = getattr(KafkaError, "_PARTITION_EOF", None)
    return eof_code is not None and callable(code) and code() == eof_code


# --- Per-partition state ---


class _PartitionState:
    """Tracks inflight offsets for a single partition and commits when safe.

    Stores the high watermark and committed offset at assignment time for
    readiness tracking. Notifies the parent tracker when committed offset advances.
    """

    __slots__ = (
        "_accepting",
        "_commit_task",
        "_commits_enabled",
        "_committed_offset",
        "_completed",
        "_consumer",
        "_failure",
        "_high_watermark",
        "_inflight",
        "_last_tracked_offset",
        "_on_commit",
        "_on_failure",
        "_partition",
        "_pending_commit_offset",
        "_tasks",
        "_topic",
    )

    def __init__(
        self,
        consumer: AIOConsumer,
        topic: str,
        partition: int,
        high_watermark: int,
        committed_offset: int,
        on_commit: Callable[[], None],
        on_failure: Callable[[BaseException], None] | None = None,
    ) -> None:
        self._consumer = consumer
        self._topic = topic
        self._partition = partition
        self._inflight: deque[tuple[int, _ReadinessPermit | None]] = deque()
        self._completed: set[int] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self._commit_task: asyncio.Task[None] | None = None
        self._pending_commit_offset: int | None = None
        self._last_tracked_offset: int | None = None
        self._accepting = True
        self._commits_enabled = True
        self._failure: BaseException | None = None
        self._high_watermark = high_watermark
        self._committed_offset = committed_offset
        self._on_commit = on_commit
        self._on_failure = on_failure

    def is_caught_up(self) -> bool:
        """Whether this partition has consumed up to its initial high watermark."""
        return self._committed_offset >= self._high_watermark

    def track(
        self,
        offset: int,
        handle: ReadyAwaitable,
        permit: _ReadinessPermit | None = None,
    ) -> None:
        """Register an inflight offset with its readiness handle.

        Fast path: if ``handle is _IMMEDIATE_READY``, record completion
        synchronously without spawning a task.
        """
        try:
            if not self._accepting:
                raise RuntimeError(
                    f"Kafka partition {self._topic}/{self._partition} is no longer assigned"
                )
            self.raise_if_failed()
            if (
                self._last_tracked_offset is not None
                and offset <= self._last_tracked_offset
            ):
                raise RuntimeError(
                    "Kafka delivered a non-monotonic offset for "
                    f"{self._topic}/{self._partition}: {offset} after "
                    f"{self._last_tracked_offset}"
                )
            self._last_tracked_offset = offset
            self._inflight.append((offset, permit))
            if handle is _IMMEDIATE_READY:
                self._completed.add(offset)
                self._try_drain_and_commit()
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
            if not self._commits_enabled:
                return
            self._record_failure(error)
            return
        # Readiness failures are opaque user exceptions; all are terminal.
        except BaseException as error:  # noqa: BLE001
            self._record_failure(error)
            return
        if not self._commits_enabled:
            return
        self._completed.add(offset)
        self._try_drain_and_commit()

    def _try_drain_and_commit(self) -> None:
        """Drain contiguous completed offsets from the front and commit."""
        if not self._commits_enabled:
            return
        last_drained: int | None = None
        while self._inflight and self._inflight[0][0] in self._completed:
            offset, permit = self._inflight.popleft()
            self._completed.discard(offset)
            if permit is not None:
                permit.release()
            last_drained = offset

        if last_drained is not None:
            commit_offset = last_drained + 1
            if (
                self._pending_commit_offset is None
                or commit_offset > self._pending_commit_offset
            ):
                self._pending_commit_offset = commit_offset
            self._ensure_commit_worker()

    def _ensure_commit_worker(self) -> None:
        """Start the one serialized, monotonic commit worker if needed."""
        if self._failure is not None or not self._commits_enabled:
            return
        if self._commit_task is None or self._commit_task.done():
            self._commit_task = asyncio.create_task(self._run_commit_worker())

    async def _run_commit_worker(self) -> None:
        """Coalesce commit requests and acknowledge them in increasing order."""
        try:
            while self._commits_enabled and self._pending_commit_offset is not None:
                offset = self._pending_commit_offset
                self._pending_commit_offset = None
                try:
                    committed = await self._consumer.commit(
                        offsets=[TopicPartition(self._topic, self._partition, offset)],
                        asynchronous=False,
                    )
                    # Synchronous commit can return successfully while one
                    # partition carries a broker error. Treat that as a failed
                    # acknowledgement; advancing local progress would allow a
                    # rebalance to replay from an older broker offset while the
                    # stream had already advertised readiness.
                    if committed is None or len(committed) != 1:
                        raise RuntimeError(
                            "Kafka synchronous offset commit returned an invalid "
                            f"acknowledgement count for {self._topic}/{self._partition}"
                        )
                    acknowledged = committed[0]
                    if (
                        acknowledged.topic != self._topic
                        or acknowledged.partition != self._partition
                        or acknowledged.offset != offset
                    ):
                        raise RuntimeError(
                            "Kafka synchronous offset commit returned a mismatched "
                            f"acknowledgement for {self._topic}/{self._partition} "
                            f"at offset {offset}"
                        )
                    if partition_error := _topic_partition_error(acknowledged):
                        raise RuntimeError(
                            "Kafka broker rejected offset commit for "
                            f"{acknowledged.topic}/{acknowledged.partition}: "
                            f"{partition_error}"
                        )
                except asyncio.CancelledError:
                    raise
                # Broker/client implementations expose heterogeneous errors.
                except Exception as error:
                    _logger.exception(
                        "Failed to commit offset %d for %s/%d",
                        offset,
                        self._topic,
                        self._partition,
                    )
                    self._record_failure(error)
                    return

                # Readiness and externally visible progress advance only after
                # the broker acknowledges the synchronous commit.
                if self._commits_enabled and offset > self._committed_offset:
                    self._committed_offset = offset
                    self._on_commit()
        finally:
            self._commit_task = None
            # A completion can publish more contiguous progress while this
            # worker is handing off ownership. Re-check after clearing the
            # task slot so pending work always acquires a successor worker.
            if (
                self._commits_enabled
                and self._failure is None
                and self._pending_commit_offset is not None
            ):
                self._ensure_commit_worker()

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
        """Fence the assignment, drain downstream work, then finish commits.

        Revocation must not cancel readiness waiters: a handle may already
        represent durable downstream work that is about to succeed. Waiting
        preserves the last safe synchronous commit opportunity before Kafka
        transfers ownership to another consumer.
        """
        self._accepting = False
        tasks = tuple(self._tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        # Follow the complete worker chain. A worker may hand pending progress
        # to a successor as it exits; snapshotting this field once can return
        # before that successor receives broker acknowledgement.
        while True:
            commit_task = self._commit_task
            if commit_task is None:
                if self._failure is None and self._pending_commit_offset is not None:
                    self._ensure_commit_worker()
                    continue
                break
            await commit_task
        while self._inflight:
            _, permit = self._inflight.popleft()
            if permit is not None:
                permit.release()
        self._completed.clear()

    async def abort_lost(self) -> None:
        """Fence a lost assignment and cancel work without issuing commits.

        A lost partition may already belong to another consumer. Unlike normal
        revocation, there is no safe final-commit window, so pending readiness
        and commit work is abandoned rather than drained.
        """
        self._accepting = False
        self._commits_enabled = False
        self._pending_commit_offset = None

        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        commit_task = self._commit_task
        if commit_task is not None:
            commit_task.cancel()
        if tasks or commit_task is not None:
            await asyncio.gather(
                *tasks,
                *(() if commit_task is None else (commit_task,)),
                return_exceptions=True,
            )

        self._tasks.clear()
        while self._inflight:
            _, permit = self._inflight.popleft()
            if permit is not None:
                permit.release()
        self._completed.clear()


# --- Offset tracker ---


class _OffsetTracker:
    """Manages _PartitionState objects across partitions with rebalance support.

    Sets ``ready_event`` when all partitions have consumed up to their initial
    high watermarks.
    """

    __slots__ = (
        "_close_tasks",
        "_closed",
        "_consumer",
        "_failure",
        "_partitions",
        "_ready_epoch",
        "_rebalance_epoch",
        "_settled_assignment_epoch",
        "failure_event",
        "ready_event",
    )

    def __init__(self, consumer: AIOConsumer) -> None:
        self._consumer = consumer
        self._partitions: dict[tuple[str, int], _PartitionState] = {}
        self._rebalance_epoch = 0
        self._settled_assignment_epoch: int | None = None
        self._ready_epoch: int | None = None
        self._close_tasks: set[asyncio.Task[None]] = set()
        self._closed = False
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
        """Set ready_event if all partitions are caught up."""
        if self._settled_assignment_epoch == self._rebalance_epoch and all(
            s.is_caught_up() for s in self._partitions.values()
        ):
            self._ready_epoch = self._rebalance_epoch
            self.ready_event.set()

    def _current_ready_epoch(self) -> int | None:
        """Return the ready assignment epoch, rejecting every stale signal."""
        epoch = self._ready_epoch
        if (
            epoch is None
            or epoch != self._rebalance_epoch
            or self._settled_assignment_epoch != epoch
            or not all(state.is_caught_up() for state in self._partitions.values())
        ):
            return None
        return epoch

    async def wait_until_ready(self) -> int:
        """Wait for and return the currently caught-up assignment epoch."""
        while True:
            await self.ready_event.wait()
            epoch = self._current_ready_epoch()
            if epoch is not None:
                return epoch

    def is_ready_epoch(self, epoch: int) -> bool:
        """Revalidate a readiness token immediately before external signalling."""
        return self._current_ready_epoch() == epoch

    def _begin_rebalance(self) -> int:
        """Invalidate readiness until one complete assignment is installed."""
        self._rebalance_epoch += 1
        self._settled_assignment_epoch = None
        self._ready_epoch = None
        self.ready_event.clear()
        return self._rebalance_epoch

    def begin_assignment(self) -> int:
        """Start a rebalance assignment and return its generation token."""
        if self._closed:
            raise RuntimeError("Kafka assignment started during shutdown")
        return self._begin_rebalance()

    def complete_assignment(
        self,
        epoch: int,
        partitions: Iterable[tuple[str, int, int, int]],
    ) -> None:
        """Atomically install assignment metadata and enable readiness checks."""
        if self._closed:
            raise RuntimeError("Kafka assignment completed during shutdown")
        if epoch != self._rebalance_epoch:
            raise RuntimeError(
                "Kafka assignment completed after a newer rebalance started"
            )
        for topic, partition, high_watermark, committed_offset in partitions:
            self.add(
                topic,
                partition,
                high_watermark=high_watermark,
                committed_offset=committed_offset,
            )
        self._settled_assignment_epoch = epoch
        self._check_ready()

    def get(self, topic: str, partition: int) -> _PartitionState:
        """Get state only while this consumer owns the partition."""
        key = (topic, partition)
        try:
            return self._partitions[key]
        except KeyError as error:
            raise RuntimeError(
                f"Received Kafka message for unassigned partition {topic}/{partition}"
            ) from error

    def add(
        self,
        topic: str,
        partition: int,
        high_watermark: int,
        committed_offset: int,
    ) -> _PartitionState:
        """Create and register a partition state."""
        if self._closed:
            raise RuntimeError(
                f"Kafka partition {topic}/{partition} was assigned during shutdown"
            )
        key = (topic, partition)
        if key in self._partitions:
            raise RuntimeError(
                f"Kafka partition {topic}/{partition} was assigned twice without revocation"
            )
        state = _PartitionState(
            self._consumer,
            topic,
            partition,
            high_watermark=high_watermark,
            committed_offset=committed_offset,
            on_commit=self._check_ready,
            on_failure=self._record_failure,
        )
        self._partitions[key] = state
        return state

    async def on_revoke(self, partitions: list[TopicPartition]) -> None:
        """Fence revoked partitions and finish acknowledged commits."""
        # Invalidate readiness before removing any state. A close task from a
        # revoked partition can acknowledge its final commit while the mapping
        # is empty or incomplete; that must not make the source ready.
        self._begin_rebalance()
        states: list[_PartitionState] = []
        for tp in partitions:
            key = (tp.topic, tp.partition)
            state = self._partitions.pop(key, None)
            if state is not None:
                states.append(state)
        close_task = self._start_close(states)
        if close_task is not None:
            # Rebalance callback cancellation must not orphan the states it
            # already fenced. ``close_all`` also observes and drains this task.
            await asyncio.shield(close_task)
        self.raise_if_failed()

    async def on_lost(self, partitions: list[TopicPartition]) -> None:
        """Fence lost partitions immediately without dispatching new commits."""
        # Lost ownership is stronger than revocation: Kafka provides no safe
        # final-commit window because another consumer may already own these
        # partitions. Invalidate readiness before touching partition state.
        self._begin_rebalance()
        states: list[_PartitionState] = []
        for tp in partitions:
            key = (tp.topic, tp.partition)
            state = self._partitions.pop(key, None)
            if state is not None:
                states.append(state)
        abort_task = self._start_abort(states)
        if abort_task is not None:
            # Callback cancellation must not leave readiness or commit tasks
            # alive after ownership has been lost. ``close_all`` also drains
            # this registered cleanup task.
            await asyncio.shield(abort_task)
        self.raise_if_failed()

    def is_assigned(self) -> bool:
        """Whether the current rebalance has a fully installed assignment."""
        return self._settled_assignment_epoch == self._rebalance_epoch

    async def close_all(self) -> None:
        """Fence every partition and drain all active revoke/close work."""
        self._closed = True
        self._begin_rebalance()
        states = tuple(self._partitions.values())
        self._partitions.clear()
        self._start_close(states)

        cancelled: asyncio.CancelledError | None = None
        while self._close_tasks:
            tasks = tuple(self._close_tasks)
            waiter = asyncio.gather(*tasks, return_exceptions=True)
            while not waiter.done():
                try:
                    await asyncio.shield(waiter)
                except asyncio.CancelledError as error:
                    # Preserve caller cancellation, but keep shielding the
                    # close graph from every later cancel request too. The
                    # consumer cannot be released while durable work remains.
                    if cancelled is None:
                        cancelled = error
            # Retrieve the gather result even if cancellation won every race.
            # ``return_exceptions=True`` keeps close failures in tracker state.
            waiter.result()

        self.raise_if_failed()
        if cancelled is not None:
            raise cancelled

    def _start_close(
        self, states: Iterable[_PartitionState]
    ) -> asyncio.Task[None] | None:
        partition_states = tuple(states)
        if not partition_states:
            return None
        task = asyncio.create_task(self._close_states(partition_states))
        self._close_tasks.add(task)
        task.add_done_callback(self._close_done)
        return task

    def _start_abort(
        self, states: Iterable[_PartitionState]
    ) -> asyncio.Task[None] | None:
        partition_states = tuple(states)
        if not partition_states:
            return None
        task = asyncio.create_task(self._abort_states(partition_states))
        self._close_tasks.add(task)
        task.add_done_callback(self._close_done)
        return task

    def _close_done(self, task: asyncio.Task[None]) -> None:
        """Retain every background close outcome after its task leaves the set."""
        self._close_tasks.discard(task)
        if task.cancelled():
            self._record_failure(
                asyncio.CancelledError("Kafka partition close was cancelled")
            )
            return
        if error := task.exception():
            self._record_failure(error)

    async def _close_states(self, states: Iterable[_PartitionState]) -> None:
        """Drain every state and surface the first terminal failure afterward."""
        partition_states = tuple(states)
        if not partition_states:
            return

        # A failure in one partition must not let shutdown/revocation return
        # while another partition still owns readiness or commit work.
        close_results = await asyncio.gather(
            *(state.close() for state in partition_states),
            return_exceptions=True,
        )
        for result in close_results:
            if isinstance(result, BaseException):
                self._record_failure(result)
        for state in partition_states:
            try:
                state.raise_if_failed()
            except asyncio.CancelledError as error:
                self._record_failure(error)
            # Connector/user failures have no shared concrete base type.
            except Exception as error:  # noqa: BLE001
                self._record_failure(error)
        self.raise_if_failed()

    async def _abort_states(self, states: Iterable[_PartitionState]) -> None:
        """Cancel every lost state without opening a new commit path."""
        abort_results = await asyncio.gather(
            *(state.abort_lost() for state in states),
            return_exceptions=True,
        )
        for result in abort_results:
            if isinstance(result, BaseException):
                self._record_failure(result)
        self.raise_if_failed()


# --- TopicStream: LiveStream[Message] primitive ---


class TopicStream:
    """A :class:`LiveStream` of raw Kafka :class:`Message` objects.

    Owns the consumer subscription and delivers every valid polled message to
    the subscriber's ``send()``. ``mark_ready()`` is called once per
    ``watch()`` invocation, when all initially-assigned partitions have been
    consumed up to their initial high watermarks.

    At most one of ``watch()`` and ``payloads().watch()`` (across all
    ``payloads()`` views) may be active concurrently. The consumer must be
    returned by :func:`create_consumer`; it transfers ownership to this stream,
    is single-use, and is closed when watching ends. Requiring the factory is
    what lets Synor guarantee that automatic offset commit and store are off.
    """

    __slots__ = (
        "_consumer",
        "_offset_reset",
        "_topics",
        "_watch_guard",
        "_watch_started",
    )

    def __init__(self, consumer: AIOConsumer, topics: list[str]) -> None:
        self._consumer = consumer
        self._offset_reset = _claim_helper_consumer(consumer)
        self._topics = topics
        self._watch_guard = SingleWatcherGuard("Kafka TopicStream")
        self._watch_started = False

    def payloads(self) -> LiveStream[bytes]:
        """View of this stream yielding each message's payload as bytes.

        Null-valued messages (Kafka tombstones) are filtered out of the bytes
        view; consumers that need tombstone semantics should subscribe at the
        ``Message`` level.
        """
        return _TopicPayloadsStream(self)

    async def watch(self, subscriber: LiveStreamSubscriber[Message]) -> None:
        """Consume messages from the topics and deliver them to the subscriber."""
        with self._watch_guard:
            if self._watch_started:
                raise RuntimeError(
                    "A TopicStream backed by create_consumer() is single-use"
                )
            self._watch_started = True
            try:
                await self._watch(subscriber)
            finally:
                await self._cleanup_consumer()

    async def _cleanup_consumer(self) -> None:
        """Finish unsubscribe/owned-close despite repeated caller cancellation."""

        async def _cleanup() -> None:
            try:
                await self._consumer.unsubscribe()
            finally:
                await self._consumer.close()

        cleanup_task = asyncio.create_task(_cleanup())
        cancelled: asyncio.CancelledError | None = None
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError as error:
                if cancelled is None:
                    cancelled = error
        # Retrieve failures even if cancellation won the final completion race.
        cleanup_task.result()
        if cancelled is not None:
            raise cancelled

    async def _watch(self, subscriber: LiveStreamSubscriber[Message]) -> None:
        tracker = _OffsetTracker(self._consumer)
        admission = _ReadinessAdmission(_DEFAULT_MAX_INFLIGHT_READINESS)
        ready_signaled = False

        async def _on_assign(
            _consumer: AIOConsumer, partitions: list[TopicPartition]
        ) -> None:
            assignment_epoch = tracker.begin_assignment()
            assignment: list[tuple[str, int, int, int]] = []
            if partitions:
                committed = await self._consumer.committed(partitions)
                if committed is None or len(committed) != len(partitions):
                    raise RuntimeError(
                        "Kafka committed-offset lookup returned an invalid "
                        "partition result count"
                    )
                for tp, committed_tp in zip(partitions, committed):
                    if (
                        committed_tp.topic != tp.topic
                        or committed_tp.partition != tp.partition
                    ):
                        raise RuntimeError(
                            "Kafka committed-offset lookup returned a mismatched "
                            f"partition for {tp.topic}/{tp.partition}"
                        )
                    if committed_error := _topic_partition_error(committed_tp):
                        raise RuntimeError(
                            "Kafka committed-offset lookup failed for "
                            f"{tp.topic}/{tp.partition}: {committed_error}"
                        )
                    try:
                        low, high = await self._consumer.get_watermark_offsets(tp)
                    except Exception as error:
                        raise RuntimeError(
                            "Kafka high-watermark lookup failed for "
                            f"{tp.topic}/{tp.partition}"
                        ) from error
                    try:
                        effective_start = _effective_start_offset(
                            committed_tp.offset,
                            low,
                            high,
                            self._offset_reset,
                        )
                    except RuntimeError as error:
                        raise RuntimeError(
                            "Kafka could not determine the effective starting "
                            f"offset for {tp.topic}/{tp.partition}"
                        ) from error
                    assignment.append(
                        (
                            tp.topic,
                            tp.partition,
                            high,
                            effective_start,
                        )
                    )
            # Install all metadata without an await between the generation
            # validation and readiness check. This prevents partially-built
            # assignments from becoming externally ready.
            tracker.complete_assignment(assignment_epoch, assignment)
            if partitions and admission.is_full:
                # A new assignment can arrive inside a heartbeat poll while
                # older revoked work still occupies the global window. Fence
                # it before that poll is allowed to yield application data.
                await self._consumer.pause(partitions)

        async def _on_revoke(
            _consumer: AIOConsumer, partitions: list[TopicPartition]
        ) -> None:
            await tracker.on_revoke(partitions)

        async def _on_lost(
            _consumer: AIOConsumer, partitions: list[TopicPartition]
        ) -> None:
            await tracker.on_lost(partitions)

        async def _process_message(
            msg: Message | None, permit: _ReadinessPermit
        ) -> None:
            """Forward a polled message to the subscriber and track its offset."""
            if msg is None:
                permit.release()
                return
            if (consumer_error := msg.error()) is not None:
                # Partition EOF is an informational event emitted only when
                # the client enables it. Every other message-level consumer
                # error is terminal: ignoring it could allow readiness and
                # offsets to advance after a broker/transport failure.
                if _is_partition_eof(consumer_error):
                    permit.release()
                    return
                permit.release()
                raise RuntimeError(f"Kafka consumer poll failed: {consumer_error}")

            topic: str = msg.topic()  # type: ignore[assignment]
            partition: int = msg.partition()  # type: ignore[assignment]
            offset: int = msg.offset()  # type: ignore[assignment]

            try:
                part_state = tracker.get(topic, partition)
                handle = await subscriber.send(msg)
                part_state.track(offset, handle, permit)
            except BaseException:
                # ``track`` also releases on its own validation errors. Permits
                # are idempotent so this covers failures before the transfer.
                permit.release()
                raise

        async def _acquire_poll_permit() -> _ReadinessPermit:
            """Acquire capacity while continuing Kafka's poll heartbeat path.

            Once the readiness window is full, pause the current assignment so
            heartbeat polls cannot admit more records. Polling continues at the
            normal short timeout, allowing rebalances and client callbacks to
            run until a terminal readiness outcome releases capacity.
            """
            if not admission.is_full:
                return await admission.acquire()

            _logger.debug(
                "Pausing Kafka source at the %d-message readiness limit",
                admission.limit,
            )
            paused = False
            returning_permit: _ReadinessPermit | None = None
            try:
                while True:
                    tracker.raise_if_failed()
                    assignment = await self._consumer.assignment()
                    if assignment:
                        await self._consumer.pause(assignment)
                        paused = True

                    permit_task = asyncio.create_task(admission.acquire())
                    heartbeat_task = asyncio.ensure_future(
                        self._consumer.poll(timeout=_POLL_TIMEOUT)
                    )
                    failure_task = asyncio.create_task(tracker.failure_event.wait())
                    transferred = False
                    permit: _ReadinessPermit | None = None
                    try:
                        done, _ = await asyncio.wait(
                            {permit_task, heartbeat_task, failure_task},
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if permit_task in done:
                            permit = permit_task.result()
                        if permit is not None and not heartbeat_task.done():
                            # Do not cancel an executor-backed poll when a slot
                            # wins the race: librdkafka could still consume a
                            # prefetched record and the cancelled Future would
                            # discard its result. A paused poll is bounded by
                            # ``_POLL_TIMEOUT`` and must finish empty before we
                            # resume application delivery.
                            await heartbeat_task
                        if failure_task.done():
                            tracker.raise_if_failed()

                        heartbeat = await heartbeat_task
                        if heartbeat is not None:
                            consumer_error = heartbeat.error()
                            if consumer_error is not None and _is_partition_eof(
                                consumer_error
                            ):
                                heartbeat = None
                            elif consumer_error is not None:
                                raise RuntimeError(
                                    f"Kafka consumer poll failed: {consumer_error}"
                                )
                            else:
                                # pause() is required to fence application
                                # records. Failing closed leaves this offset
                                # uncommitted for replay instead of silently
                                # exceeding the bound.
                                raise RuntimeError(
                                    "Kafka returned an application message while "
                                    "its assignment was paused for readiness "
                                    "backpressure"
                                )
                        if permit is not None:
                            transferred = True
                            returning_permit = permit
                            return permit
                    finally:
                        for task in (permit_task, heartbeat_task, failure_task):
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(
                            permit_task,
                            heartbeat_task,
                            failure_task,
                            return_exceptions=True,
                        )
                        if (
                            not transferred
                            and permit is None
                            and not permit_task.cancelled()
                            and permit_task.exception() is None
                        ):
                            # Capacity may have become available after
                            # ``asyncio.wait`` selected the heartbeat task but
                            # before cleanup cancelled the permit waiter.
                            permit = permit_task.result()
                        if not transferred and permit is not None:
                            permit.release()
            finally:
                # Assignment may have changed inside a heartbeat poll. Resume
                # every partition the consumer still owns, including one that
                # ``on_assign`` paused before this helper observed it.
                primary_error = sys.exception()
                try:
                    assignment = await self._consumer.assignment()
                    if assignment:
                        await self._consumer.resume(assignment)
                    if paused:
                        _logger.debug(
                            "Resumed Kafka source after readiness backpressure"
                        )
                except BaseException as resume_error:
                    if primary_error is None:
                        if returning_permit is not None:
                            returning_permit.release()
                        raise
                    # Resume is cleanup. Preserve the transport/downstream
                    # primary, but make this second failure inspectable both in
                    # the exception and connector logs.
                    BaseException.add_note(
                        primary_error,
                        "Kafka readiness-backpressure cleanup also failed: "
                        f"{resume_error!r}",
                    )
                    _logger.exception(
                        "Failed to resume Kafka assignment while propagating %r",
                        primary_error,
                    )

        async def _poll_message() -> tuple[Message | None, _ReadinessPermit]:
            """Reserve one hard admission slot before polling a message."""
            permit = await _acquire_poll_permit()
            try:
                message = await self._consumer.poll(timeout=_POLL_TIMEOUT)
            except BaseException:
                permit.release()
                raise
            return message, permit

        # AIOConsumer.poll() runs Consumer.poll() in a ThreadPoolExecutor.
        # The short timeout keeps cancellation and capacity-heartbeat polls
        # responsive; the executor waits for its worker during shutdown.

        active_poll_task: (
            asyncio.Task[tuple[Message | None, _ReadinessPermit]] | None
        ) = None
        active_failure_task: asyncio.Task[bool] | None = None
        try:
            await self._consumer.subscribe(
                self._topics,
                on_assign=_on_assign,
                on_revoke=_on_revoke,
                on_lost=_on_lost,
            )
            # Phase 1: Wait for initial partition assignment.
            while not tracker.is_assigned():
                await _process_message(*(await _poll_message()))

            # Phase 2: Consume messages, racing poll against the readiness event.
            while True:
                tracker.raise_if_failed()
                if not ready_signaled:
                    active_poll_task = asyncio.create_task(_poll_message())
                    ready_task = asyncio.ensure_future(tracker.wait_until_ready())
                    active_failure_task = asyncio.ensure_future(
                        tracker.failure_event.wait()
                    )
                    done, _ = await asyncio.wait(
                        {active_poll_task, ready_task, active_failure_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if active_failure_task in done:
                        active_poll_task.cancel()
                        ready_task.cancel()
                        await asyncio.gather(
                            active_poll_task, ready_task, return_exceptions=True
                        )
                        tracker.raise_if_failed()
                    if ready_task in done:
                        ready_epoch = await ready_task
                        # The poll task can execute a rebalance callback after
                        # the readiness waiter wakes but before this task gets
                        # CPU again. Never export an event from that superseded
                        # assignment generation.
                        if tracker.is_ready_epoch(ready_epoch):
                            await subscriber.mark_ready()
                            ready_signaled = True
                    else:
                        ready_task.cancel()
                    active_failure_task.cancel()
                    await asyncio.gather(
                        ready_task, active_failure_task, return_exceptions=True
                    )
                    await _process_message(*(await active_poll_task))
                    active_poll_task = None
                    active_failure_task = None
                else:
                    active_poll_task = asyncio.create_task(_poll_message())
                    active_failure_task = asyncio.ensure_future(
                        tracker.failure_event.wait()
                    )
                    done, _ = await asyncio.wait(
                        {active_poll_task, active_failure_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if active_failure_task in done:
                        active_poll_task.cancel()
                        await asyncio.gather(active_poll_task, return_exceptions=True)
                        tracker.raise_if_failed()
                    active_failure_task.cancel()
                    await asyncio.gather(active_failure_task, return_exceptions=True)
                    await _process_message(*(await active_poll_task))
                    active_poll_task = None
                    active_failure_task = None

        finally:

            async def _shutdown() -> None:
                if active_poll_task is not None:
                    if not active_poll_task.done():
                        active_poll_task.cancel()
                    poll_result = await asyncio.gather(
                        active_poll_task, return_exceptions=True
                    )
                    if poll_result and isinstance(poll_result[0], tuple):
                        # A completed poll may have won a race with readiness,
                        # failure, or cancellation before its result was consumed.
                        poll_result[0][1].release()
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
    """Adapts a ``LiveStreamSubscriber[bytes]`` to receive Kafka ``Message`` objects."""

    __slots__ = ("_downstream",)

    def __init__(self, downstream: LiveStreamSubscriber[bytes]) -> None:
        self._downstream = downstream

    async def send(self, message: Message) -> ReadyAwaitable:
        value = message.value()
        if value is None:
            return _IMMEDIATE_READY
        return await self._downstream.send(value)

    async def mark_ready(self) -> None:
        await self._downstream.mark_ready()


# --- LiveMapFeed implementation ---


class _StreamToMapSubscriber:
    """Adapts a :class:`LiveMapSubscriber` to consume a ``LiveStream[Message]``."""

    __slots__ = ("_is_deletion", "_map_sub")

    def __init__(
        self,
        map_sub: LiveMapSubscriber[StableKey, Message],
        is_deletion: IsDeleteFn | None,
    ) -> None:
        self._map_sub = map_sub
        self._is_deletion = is_deletion

    async def send(self, message: Message) -> ReadyAwaitable:
        msg = message
        key = msg.key()
        if key is None:
            _logger.error(
                "Skipping message with null key at %s/%d offset %d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )
            return _IMMEDIATE_READY
        if msg.value() is None or (
            self._is_deletion is not None and self._is_deletion(msg)
        ):
            return await self._map_sub.delete(key)
        return await self._map_sub.update(key, msg)

    async def mark_ready(self) -> None:
        await self._map_sub.mark_ready()


class _TopicMapFeed:
    """``LiveMapFeed`` view over a :class:`TopicStream`."""

    __slots__ = ("_is_deletion", "_stream")

    def __init__(
        self,
        stream: TopicStream,
        is_deletion: IsDeleteFn | None,
    ) -> None:
        self._stream = stream
        self._is_deletion = is_deletion

    async def watch(self, subscriber: LiveMapSubscriber[StableKey, Message]) -> None:
        await self._stream.watch(_StreamToMapSubscriber(subscriber, self._is_deletion))


# --- Public API ---


def topic_as_stream(consumer: AIOConsumer, topics: list[str]) -> TopicStream:
    """
    Treat a Kafka topic as a :class:`LiveStream` of raw messages.

    The returned :class:`TopicStream` implements ``LiveStream[Message]`` and
    exposes ``.payloads()`` for a ``LiveStream[bytes]`` view of message values
    — the typical input for sources that consume opaque event payloads (e.g.
    the OCI Object Storage source's live mode).

    The consumer must be **unsubscribed** — ``topic_as_stream()`` handles
    subscription internally to register partition rebalance callbacks.

    Args:
        consumer: An unsubscribed ``AIOConsumer`` returned by
            :func:`create_consumer`. It transfers ownership to the stream and
            is single-use. Directly constructed consumers are rejected because
            their effective offset configuration cannot be verified.
        topics: Topics to subscribe to.

    Returns:
        A :class:`TopicStream` (single-watcher; bind to one consumer).
    """
    return TopicStream(consumer, topics)


def topic_as_map(
    consumer: AIOConsumer,
    topics: list[str],
    *,
    is_deletion: IsDeleteFn | None = None,
) -> _TopicMapFeed:
    """
    Treat a Kafka topic as a live keyed map.

    Returns a ``LiveMapFeed`` that streams change events (updates/deletes) from the
    given topics. Each item is keyed by the message key, and the value is the full
    ``confluent_kafka.Message`` object. Suitable for passing to ``mount_each()`` for
    parallel processing with automatic offset management.

    The consumer must be **unsubscribed** — ``topic_as_map()`` handles subscription
    internally to register partition rebalance callbacks.

    Args:
        consumer: An unsubscribed ``AIOConsumer`` returned by
            :func:`create_consumer`. It transfers ownership to the stream and
            is single-use. Directly constructed consumers are rejected because
            their effective offset configuration cannot be verified.
        topics: Topics to subscribe to.
        is_deletion: Optional predicate ``(message) -> bool`` for custom deletion
            detection on non-tombstone messages. Messages with ``None`` value (Kafka
            tombstones) are always treated as deletions regardless of this predicate.

    Returns:
        A ``LiveMapFeed[bytes | str, Message]`` for use with ``mount_each()``.
    """
    return _TopicMapFeed(topic_as_stream(consumer, topics), is_deletion)


__all__ = [
    "IsDeleteFn",
    "TopicStream",
    "create_consumer",
    "topic_as_map",
    "topic_as_stream",
]
