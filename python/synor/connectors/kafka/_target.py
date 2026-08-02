"""
Kafka target for Synor.

This module provides a two-level target state system for Kafka:
1. Topic level: Lightweight container for generation tracking (user-managed topic)
2. Message level: Produces messages to the topic for upserts/deletes
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Callable, Collection, Generic, NamedTuple, Sequence

try:
    from confluent_kafka.aio import AIOProducer  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "confluent_kafka is required to use the Kafka connector. "
        "Please install synor[kafka]."
    ) from e

import synor as syn
from synor.connectorkits.fingerprint import fingerprint_bytes, fingerprint_str
from synor._internal.context_keys import ContextKey, ContextProvider
from synor._internal.datatype import TypeChecker

# --- Type aliases ---

_MessageFingerprint = bytes

# --- Internal types ---


class _TopicKey(NamedTuple):
    producer_key: str
    topic: str


_TOPIC_KEY_CHECKER = TypeChecker(tuple[str, str])


@dataclass
class _TopicSpec:
    deletion_value_fn: Callable[[bytes | str], bytes | str] | None


class _TopicAction(NamedTuple):
    key: _TopicKey
    spec: _TopicSpec | syn.AbsentType


class _MessageAction(NamedTuple):
    key: bytes | str
    value: bytes | str | None  # None = tombstone (no deletion_value_fn)


# --- Message handler (child level) ---


class _MessageHandler:
    """Handler for message-level target states within a topic."""

    __slots__ = ("_producer", "_topic", "_deletion_value_fn", "_sink")

    _producer: AIOProducer
    _topic: str
    _deletion_value_fn: Callable[[bytes | str], bytes | str] | None
    _sink: syn.TargetActionSink[_MessageAction]

    def __init__(
        self,
        producer: AIOProducer,
        topic: str,
        deletion_value_fn: Callable[[bytes | str], bytes | str] | None,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._deletion_value_fn = deletion_value_fn
        self._sink = syn.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_MessageAction],
        /,
    ) -> None:
        if not actions:
            return
        # AIOProducer.produce() is an async method that enqueues the message into
        # the batch buffer and returns an asyncio.Future resolved by the delivery
        # report. We must await the produce coroutines to obtain those futures,
        # then await the futures to ensure messages are actually delivered.
        delivery_futures = await asyncio.gather(
            *(
                self._producer.produce(self._topic, key=action.key, value=action.value)
                for action in actions
            )
        )
        await asyncio.gather(*delivery_futures)

    def reconcile(
        self,
        key: syn.StableKey,
        desired_target_state: bytes | str | syn.AbsentType,
        prev_possible_records: Collection[_MessageFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> syn.TargetReconcileOutput[_MessageAction, _MessageFingerprint] | None:
        assert isinstance(key, (bytes, str))

        if syn.is_absent(desired_target_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            deletion_value: bytes | str | None = None
            if self._deletion_value_fn is not None:
                deletion_value = self._deletion_value_fn(key)
            return syn.TargetReconcileOutput(
                action=_MessageAction(key=key, value=deletion_value),
                sink=self._sink,
                tracking_record=syn.ABSENT,
            )

        # Upsert case
        if isinstance(desired_target_state, bytes):
            target_fp = fingerprint_bytes(desired_target_state)
        else:
            target_fp = fingerprint_str(desired_target_state)

        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_records
        ):
            return None

        return syn.TargetReconcileOutput(
            action=_MessageAction(key=key, value=desired_target_state),
            sink=self._sink,
            tracking_record=target_fp,
        )


# --- Topic handler (root level) ---


class _TopicHandler:
    """Handler for topic-level target states. Always returns output for generation tracking."""

    __slots__ = ("_sink",)

    _sink: syn.TargetActionSink[_TopicAction, _MessageHandler]

    def __init__(self) -> None:
        self._sink = syn.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_TopicAction],
        /,
    ) -> list[syn.ChildTargetDef[_MessageHandler] | None]:
        outputs: list[syn.ChildTargetDef[_MessageHandler] | None] = []
        for action in actions:
            if syn.is_absent(action.spec):
                outputs.append(None)
            else:
                producer = context_provider.get(action.key.producer_key, AIOProducer)
                handler = _MessageHandler(
                    producer=producer,
                    topic=action.key.topic,
                    deletion_value_fn=action.spec.deletion_value_fn,
                )
                outputs.append(syn.ChildTargetDef(handler=handler))
        return outputs

    def reconcile(
        self,
        key: syn.StableKey,
        desired_target_state: _TopicSpec | syn.AbsentType,
        prev_possible_records: Collection[None],
        prev_may_be_missing: bool,
        /,
    ) -> syn.TargetReconcileOutput[_TopicAction, None, _MessageHandler]:
        topic_key = _TopicKey(*_TOPIC_KEY_CHECKER.check(key))

        tracking_record: None | syn.AbsentType
        if syn.is_absent(desired_target_state):
            tracking_record = syn.ABSENT
        else:
            tracking_record = None

        return syn.TargetReconcileOutput(
            action=_TopicAction(key=topic_key, spec=desired_target_state),
            sink=self._sink,
            tracking_record=tracking_record,
        )


# --- Root provider registration ---

_topic_provider = syn.register_root_target_states_provider(
    "synor/kafka/topic", _TopicHandler()
)

# --- Public API ---

DeletionValueFn = Callable[[bytes | str], bytes | str]


class KafkaTopicTarget(Generic[syn.MaybePendingS], syn.ResolvesTo["KafkaTopicTarget"]):
    """
    A target for producing messages to a Kafka topic.

    The topic is user-managed (Synor does not create/drop topics).
    Messages are produced for upserts and deletes of declared target states.
    """

    _provider: syn.TargetStateProvider[
        bytes | str | syn.AbsentType, None, syn.MaybePendingS
    ]

    def __init__(
        self,
        provider: syn.TargetStateProvider[
            bytes | str | syn.AbsentType, None, syn.MaybePendingS
        ],
    ) -> None:
        self._provider = provider

    def ensure_target_state(
        self: "KafkaTopicTarget", *, key: bytes | str, value: bytes | str
    ) -> None:
        """
        Declare a target state backed by a Kafka message.

        On commit, Synor will produce a message with the given key and value
        if the state has changed since the last run.

        Args:
            key: The message key (used as the stable identity).
            value: The message value.
        """
        syn.ensure_target_state(self._provider.target_state(key, value))

    def __synor_memo_key__(self) -> str:
        return self._provider.memo_key


def kafka_topic_target(
    producer: ContextKey[AIOProducer],
    topic: str,
    *,
    deletion_value_fn: DeletionValueFn | None = None,
) -> syn.TargetState[_MessageHandler]:
    """
    Create a TargetState for a Kafka topic target.

    Use with ``syn.attach_target()`` to mount and get a child provider,
    or with ``mount_kafka_topic_target()`` for a convenience wrapper.

    Args:
        producer: ContextKey for the AIOProducer connection.
        topic: The Kafka topic name.
        deletion_value_fn: Optional callback to produce a deletion value for a given key.
            If not provided, deletions produce messages with no value (tombstone).

    Returns:
        A TargetState that can be passed to ``mount_target()``.
    """
    key = _TopicKey(producer_key=producer.key, topic=topic)
    spec = _TopicSpec(deletion_value_fn=deletion_value_fn)
    return _topic_provider.target_state(key, spec)


@syn.task
def declare_kafka_topic_target(
    producer: ContextKey[AIOProducer],
    topic: str,
    *,
    deletion_value_fn: DeletionValueFn | None = None,
) -> KafkaTopicTarget[syn.PendingS]:
    """
    Declare a Kafka topic target and return a KafkaTopicTarget for declaring messages.

    Args:
        producer: ContextKey for the AIOProducer connection.
        topic: The Kafka topic name.
        deletion_value_fn: Optional callback to produce a deletion value for a given key.
            If not provided, deletions produce messages with no value (tombstone).

    Returns:
        A KafkaTopicTarget that can be used to declare target states.
    """
    provider = syn.ensure_target_state_with_child(
        kafka_topic_target(producer, topic, deletion_value_fn=deletion_value_fn)
    )
    return KafkaTopicTarget(provider)


async def mount_kafka_topic_target(
    producer: ContextKey[AIOProducer],
    topic: str,
    *,
    deletion_value_fn: DeletionValueFn | None = None,
) -> KafkaTopicTarget[syn.ResolvedS]:
    """
    Mount a Kafka topic target and return a ready-to-use KafkaTopicTarget.

    Sugar over ``kafka_topic_target()`` + ``syn.attach_target()`` + wrapping.

    Args:
        producer: ContextKey for the AIOProducer connection.
        topic: The Kafka topic name.
        deletion_value_fn: Optional callback to produce a deletion value for a given key.
            If not provided, deletions produce messages with no value (tombstone).

    Returns:
        A KafkaTopicTarget that can be used to declare target states.
    """
    provider = await syn.attach_target(
        kafka_topic_target(producer, topic, deletion_value_fn=deletion_value_fn)
    )
    return KafkaTopicTarget(provider)


__all__ = [
    "DeletionValueFn",
    "KafkaTopicTarget",
    "declare_kafka_topic_target",
    "kafka_topic_target",
    "mount_kafka_topic_target",
]
