"""A deterministic index with a deliberately stale first delete read."""

from __future__ import annotations

import dataclasses
import hashlib
import math
import typing

from synor import retrieval

if typing.TYPE_CHECKING:
    from .fake_source import DemoChunk, DemoDocument
elif __package__:
    from .fake_source import DemoChunk, DemoDocument
else:
    from fake_source import DemoChunk, DemoDocument


def deterministic_vector(text: str, *, dimensions: int = 8) -> tuple[float, ...]:
    if dimensions < 1:
        raise ValueError("dimensions must be positive")
    payload = hashlib.sha256(text.encode()).digest()
    values = tuple((payload[index] - 127.5) / 127.5 for index in range(dimensions))
    length = math.sqrt(sum(value * value for value in values))
    return tuple(value / length for value in values)


@dataclasses.dataclass(frozen=True, slots=True)
class FakePoint:
    candidate: retrieval.RetrievalCandidate[str]
    vector: tuple[float, ...]


class EventuallyConsistentIndex:
    """In-memory target whose first verification observes stale presence."""

    def __init__(self) -> None:
        self._points: dict[str, FakePoint] = {}
        self._pending_deletes: dict[str, tuple[str, int]] = {}
        self._applied_actions: set[str] = set()
        self.effective_deletes = 0
        self.last_scored_ids: tuple[str, ...] = ()

    @property
    def point_ids(self) -> frozenset[str]:
        return frozenset(self._points)

    def points_for_source(self, source_digest: str) -> tuple[FakePoint, ...]:
        return tuple(
            point
            for point in self._points.values()
            if point.candidate.source_digest == source_digest
        )

    def index_document(
        self,
        document: DemoDocument,
        chunks: tuple[DemoChunk, ...],
        *,
        generation: int,
        policy_revision: str,
    ) -> tuple[FakePoint, ...]:
        indexed: list[FakePoint] = []
        for chunk in chunks:
            candidate = retrieval.RetrievalCandidate(
                candidate_id=chunk.point_id,
                source_digest=document.identity.evidence_digest(),
                source_generation=generation,
                tenant_id=document.tenant_id,
                policy_id=document.policy_id,
                policy_revision=policy_revision,
                group_graph_revision="groups-v1",
                payload=chunk.text,
            )
            point = FakePoint(
                candidate=candidate,
                vector=deterministic_vector(chunk.text),
            )
            self._points[chunk.point_id] = point
            indexed.append(point)
        return tuple(indexed)

    def begin_delete(
        self,
        *,
        action_id: str,
        source_digest: str,
        stale_verifications: int = 1,
    ) -> None:
        if stale_verifications < 0:
            raise ValueError("stale_verifications cannot be negative")
        if action_id in self._applied_actions:
            return
        self._applied_actions.add(action_id)
        self._pending_deletes[action_id] = (source_digest, stale_verifications)

    def verify_absent(self, action_id: str) -> bool:
        source_digest, stale_reads = self._pending_deletes[action_id]
        if stale_reads:
            self._pending_deletes[action_id] = (source_digest, stale_reads - 1)
            return False
        removed = [
            point_id
            for point_id, point in self._points.items()
            if point.candidate.source_digest == source_digest
        ]
        for point_id in removed:
            del self._points[point_id]
        if removed:
            self.effective_deletes += 1
        return not self.points_for_source(source_digest)

    def restore(self, points: tuple[FakePoint, ...]) -> None:
        for point in points:
            self._points[point.candidate.candidate_id] = point

    async def guarded_query(
        self,
        query: str,
        *,
        guard: retrieval.RetrievalGuard,
        context: retrieval.RetrievalContext,
    ) -> tuple[str, ...]:
        query_vector = deterministic_vector(query)
        scored: list[str] = []

        def score(
            _query: str,
            candidate: retrieval.RetrievalCandidate[str],
        ) -> float:
            scored.append(candidate.candidate_id)
            point = self._points[candidate.candidate_id]
            return sum(
                left * right
                for left, right in zip(query_vector, point.vector, strict=True)
            )

        retriever: retrieval.GuardedInMemoryRetriever[str, str] = (
            retrieval.GuardedInMemoryRetriever(
                candidates=(
                    point.candidate
                    for point in sorted(
                        self._points.values(),
                        key=lambda item: item.candidate.candidate_id,
                    )
                ),
                guard=guard,
                scorer=score,
            )
        )
        results = await retriever.search(query, context=context)
        self.last_scored_ids = tuple(scored)
        return tuple(result.candidate.candidate_id for result in results)


__all__ = [
    "EventuallyConsistentIndex",
    "FakePoint",
    "deterministic_vector",
]
