"""Fail-closed retrieval authorization for the revocation conformance harness.

This implementation module backs the public :mod:`synor.retrieval` facade. It
defines the smallest contract needed to prove that suppression and current
access-policy state are evaluated before an indexed payload is scored or
returned.
"""

from __future__ import annotations

import inspect
import math
import threading
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Generic, Protocol, TypeVar, runtime_checkable

from .revocation_model import make_tenant_digest
from .suppression import SuppressionRecord, SuppressionSnapshot


_PayloadT = TypeVar("_PayloadT")
_QueryT = TypeVar("_QueryT")
_MAX_SNAPSHOT_ATTEMPTS = 3


def _require_nonempty(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def _require_nonempty_members(values: frozenset[str], field_name: str) -> None:
    if not isinstance(values, frozenset):
        raise TypeError(f"{field_name} must be a frozenset")
    for value in values:
        _require_nonempty(value, field_name)


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))


@dataclass(frozen=True, slots=True)
class RetrievalContext:
    """Authenticated principal and the group graph used for this query.

    This context must be constructed by a trusted authentication and
    group-resolution boundary, never directly from caller-supplied group IDs.
    """

    tenant_id: str
    principal_id: str
    group_graph_revision: str
    group_ids: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        _require_nonempty(self.tenant_id, "tenant_id")
        _require_nonempty(self.principal_id, "principal_id")
        _require_nonempty(self.group_graph_revision, "group_graph_revision")
        _require_nonempty_members(self.group_ids, "group_ids")


@dataclass(frozen=True, slots=True)
class AccessPolicy:
    """Authoritative access state returned by an access-policy lookup."""

    policy_id: str
    tenant_id: str
    revision: str
    group_graph_revision: str
    allowed_principal_ids: frozenset[str] = field(default_factory=frozenset)
    allowed_group_ids: frozenset[str] = field(default_factory=frozenset)
    denied_principal_ids: frozenset[str] = field(default_factory=frozenset)
    denied_group_ids: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        _require_nonempty(self.policy_id, "policy_id")
        _require_nonempty(self.tenant_id, "tenant_id")
        _require_nonempty(self.revision, "revision")
        _require_nonempty(self.group_graph_revision, "group_graph_revision")
        _require_nonempty_members(self.allowed_principal_ids, "allowed_principal_ids")
        _require_nonempty_members(self.allowed_group_ids, "allowed_group_ids")
        _require_nonempty_members(self.denied_principal_ids, "denied_principal_ids")
        _require_nonempty_members(self.denied_group_ids, "denied_group_ids")

    def allows(self, context: RetrievalContext) -> bool:
        """Evaluate this policy with explicit deny taking precedence."""

        if (
            context.principal_id in self.denied_principal_ids
            or not context.group_ids.isdisjoint(self.denied_group_ids)
        ):
            return False
        return (
            context.principal_id in self.allowed_principal_ids
            or not context.group_ids.isdisjoint(self.allowed_group_ids)
        )


@dataclass(frozen=True, slots=True)
class RetrievalCandidate(Generic[_PayloadT]):
    """One indexed derivative and the authorization revisions it carries."""

    candidate_id: str
    source_digest: str
    source_generation: int
    tenant_id: str
    policy_id: str
    policy_revision: str
    group_graph_revision: str
    payload: _PayloadT

    def __post_init__(self) -> None:
        _require_nonempty(self.candidate_id, "candidate_id")
        _require_nonempty(self.source_digest, "source_digest")
        if (
            not isinstance(self.source_generation, int)
            or isinstance(self.source_generation, bool)
            or self.source_generation < 1
        ):
            raise ValueError("source_generation must be a positive integer")
        _require_nonempty(self.tenant_id, "tenant_id")
        _require_nonempty(self.policy_id, "policy_id")
        _require_nonempty(self.policy_revision, "policy_revision")
        _require_nonempty(self.group_graph_revision, "group_graph_revision")


@runtime_checkable
class SuppressionLookup(Protocol):
    """Batch lookup boundary implemented by the durable suppression index.

    The returned mapping must contain an exact ``bool`` for every requested
    source digest.  An omitted or malformed decision is unknown and therefore
    denied by :class:`RetrievalGuard`.
    """

    async def is_suppressed_many(
        self, source_digests: tuple[str, ...]
    ) -> Mapping[str, bool]: ...


@runtime_checkable
class MonotonicSuppressionLookup(SuppressionLookup, Protocol):
    """Suppression lookup with a lock-linearized, revision-bearing snapshot."""

    async def snapshot_many(
        self, source_digests: tuple[str, ...]
    ) -> SuppressionSnapshot: ...


@runtime_checkable
class AccessPolicyLookup(Protocol):
    """Batch lookup for current, authoritative access policies."""

    async def get_policies(
        self, policy_ids: tuple[str, ...]
    ) -> Mapping[str, AccessPolicy | None]: ...


class DenialReason(str, Enum):
    """Fixed, low-cardinality metric labels for denied candidates."""

    TENANT_MISMATCH = "tenant_mismatch"
    SUPPRESSED = "suppressed"
    SUPPRESSION_STATE_UNAVAILABLE = "suppression_state_unavailable"
    SUPPRESSION_AUTHORIZATION_STALE = "suppression_authorization_stale"
    SUPPRESSION_STATE_CHANGED = "suppression_state_changed"
    POLICY_LOOKUP_FAILED = "policy_lookup_failed"
    POLICY_MISSING = "policy_missing"
    POLICY_CORRUPT = "policy_corrupt"
    POLICY_STALE = "policy_stale"
    GROUP_GRAPH_REVISION_STALE = "group_graph_revision_stale"
    ACCESS_DENIED = "access_denied"


_FAIL_CLOSED_REASONS = frozenset(
    {
        DenialReason.SUPPRESSION_STATE_UNAVAILABLE,
        DenialReason.SUPPRESSION_AUTHORIZATION_STALE,
        DenialReason.SUPPRESSION_STATE_CHANGED,
        DenialReason.POLICY_LOOKUP_FAILED,
        DenialReason.POLICY_MISSING,
        DenialReason.POLICY_CORRUPT,
        DenialReason.POLICY_STALE,
        DenialReason.GROUP_GRAPH_REVISION_STALE,
    }
)


@dataclass(frozen=True, slots=True)
class RetrievalMetricsSnapshot:
    """Immutable aggregate metrics with no identity-bearing dimensions."""

    queries: int
    allowed_candidates: int
    denied_candidates: int
    fail_closed_candidates: int
    denial_counts: Mapping[DenialReason, int]


class RetrievalGuardMetrics:
    """Thread-safe, bounded-cardinality retrieval counters."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._queries = 0
        self._allowed_candidates = 0
        self._denied_candidates = 0
        self._fail_closed_candidates = 0
        self._denial_counts: Counter[DenialReason] = Counter()

    def record(
        self, *, allowed_candidates: int, denial_counts: Counter[DenialReason]
    ) -> None:
        denied_candidates = denial_counts.total()
        fail_closed_candidates = sum(
            count
            for reason, count in denial_counts.items()
            if reason in _FAIL_CLOSED_REASONS
        )
        with self._lock:
            self._queries += 1
            self._allowed_candidates += allowed_candidates
            self._denied_candidates += denied_candidates
            self._fail_closed_candidates += fail_closed_candidates
            self._denial_counts.update(denial_counts)

    def snapshot(self) -> RetrievalMetricsSnapshot:
        with self._lock:
            denial_counts = MappingProxyType(dict(self._denial_counts))
            return RetrievalMetricsSnapshot(
                queries=self._queries,
                allowed_candidates=self._allowed_candidates,
                denied_candidates=self._denied_candidates,
                fail_closed_candidates=self._fail_closed_candidates,
                denial_counts=denial_counts,
            )


class InMemoryAccessPolicyLookup:
    """Small in-memory authoritative policy index for conformance tests."""

    def __init__(self, policies: Iterable[AccessPolicy] = ()) -> None:
        self._lock = threading.RLock()
        self._policies: dict[str, AccessPolicy] = {}
        for policy in policies:
            self.put(policy)

    def put(self, policy: AccessPolicy) -> None:
        with self._lock:
            self._policies[policy.policy_id] = policy

    def remove(self, policy_id: str) -> None:
        _require_nonempty(policy_id, "policy_id")
        with self._lock:
            self._policies.pop(policy_id, None)

    async def get_policies(
        self, policy_ids: tuple[str, ...]
    ) -> Mapping[str, AccessPolicy | None]:
        with self._lock:
            return {
                policy_id: self._policies.get(policy_id) for policy_id in policy_ids
            }


@dataclass(frozen=True, slots=True)
class _AuthorizationResult(Generic[_PayloadT]):
    candidates: tuple[RetrievalCandidate[_PayloadT], ...]
    denial_counts: Counter[DenialReason]
    suppression_snapshot: SuppressionSnapshot | None


class RetrievalGuard:
    """Filter indexed candidates using current authorization state.

    Tenant rejection happens before external lookups.  Suppression is checked
    before policy evaluation.  Any incomplete, malformed, stale, or failed
    dependency result denies the affected candidate.

    When the suppression lookup implements :class:`MonotonicSuppressionLookup`,
    authorization is linearized at a confirmed suppression snapshot.  A
    concurrent suppression causes a bounded re-evaluation; continued churn
    fails closed.  Simple boolean lookups remain supported for compatibility,
    but cannot provide this concurrency guarantee.
    """

    def __init__(
        self,
        *,
        suppression_lookup: SuppressionLookup,
        policy_lookup: AccessPolicyLookup,
        metrics: RetrievalGuardMetrics | None = None,
    ) -> None:
        self._suppression_lookup = suppression_lookup
        self._policy_lookup = policy_lookup
        self._metrics = metrics if metrics is not None else RetrievalGuardMetrics()

    @property
    def metrics(self) -> RetrievalGuardMetrics:
        return self._metrics

    def _monotonic_lookup(self) -> MonotonicSuppressionLookup | None:
        if isinstance(self._suppression_lookup, MonotonicSuppressionLookup):
            return self._suppression_lookup
        return None

    async def filter_candidates(
        self,
        context: RetrievalContext,
        candidates: Sequence[RetrievalCandidate[_PayloadT]],
    ) -> tuple[RetrievalCandidate[_PayloadT], ...]:
        """Return authorized candidates in input order."""

        result = await self._authorize_candidates(context, candidates)
        self._record_result(result)
        return result.candidates

    def _record_result(self, result: _AuthorizationResult[_PayloadT]) -> None:
        self._metrics.record(
            allowed_candidates=len(result.candidates),
            denial_counts=result.denial_counts,
        )

    async def _authorize_candidates(
        self,
        context: RetrievalContext,
        candidates: Sequence[RetrievalCandidate[_PayloadT]],
    ) -> _AuthorizationResult[_PayloadT]:
        base_denials: Counter[DenialReason] = Counter()
        tenant_candidates: list[RetrievalCandidate[_PayloadT]] = []
        for candidate in candidates:
            if candidate.tenant_id != context.tenant_id:
                base_denials[DenialReason.TENANT_MISMATCH] += 1
            else:
                tenant_candidates.append(candidate)

        monotonic_lookup = self._monotonic_lookup()
        if monotonic_lookup is None:
            unsuppressed_candidates = await self._filter_suppressed(
                tenant_candidates, base_denials
            )
            authorized_candidates = await self._filter_policy(
                context, unsuppressed_candidates, base_denials
            )
            return _AuthorizationResult(
                candidates=tuple(authorized_candidates),
                denial_counts=base_denials,
                suppression_snapshot=None,
            )

        for attempt in range(_MAX_SNAPSHOT_ATTEMPTS):
            denial_counts = base_denials.copy()
            snapshot = await self._read_snapshot(
                monotonic_lookup,
                tenant_candidates,
                denial_counts,
            )
            if snapshot is None:
                return _AuthorizationResult(
                    candidates=(),
                    denial_counts=denial_counts,
                    suppression_snapshot=None,
                )
            unsuppressed_candidates = self._filter_snapshot(
                context,
                tenant_candidates,
                snapshot,
                denial_counts,
            )
            authorized_candidates = await self._filter_policy(
                context, unsuppressed_candidates, denial_counts
            )
            if not authorized_candidates:
                return _AuthorizationResult(
                    candidates=(),
                    denial_counts=denial_counts,
                    suppression_snapshot=snapshot,
                )

            confirmation = await self._read_snapshot(
                monotonic_lookup,
                authorized_candidates,
                denial_counts,
                count_failure=False,
            )
            if confirmation is not None and self._snapshots_match(
                snapshot,
                confirmation,
                authorized_candidates,
            ):
                return _AuthorizationResult(
                    candidates=tuple(authorized_candidates),
                    denial_counts=denial_counts,
                    suppression_snapshot=confirmation,
                )
            if attempt == _MAX_SNAPSHOT_ATTEMPTS - 1:
                denial_counts[DenialReason.SUPPRESSION_STATE_CHANGED] += len(
                    authorized_candidates
                )
                return _AuthorizationResult(
                    candidates=(),
                    denial_counts=denial_counts,
                    suppression_snapshot=None,
                )

        raise AssertionError("bounded suppression snapshot loop did not terminate")

    async def _revalidate_after_scoring(
        self,
        context: RetrievalContext,
        result: _AuthorizationResult[_PayloadT],
    ) -> _AuthorizationResult[_PayloadT]:
        """Reauthorize candidates against current suppression and policy state.

        For a monotonic suppression lookup, ``_authorize_candidates`` reads a
        suppression snapshot, refreshes authoritative policy, then confirms
        the suppression snapshot.  This establishes the before-return
        authorization point after an asynchronous scorer has yielded control.
        """

        if not result.candidates:
            return result

        refreshed = await self._authorize_candidates(context, result.candidates)
        combined_denials = result.denial_counts.copy()
        combined_denials.update(refreshed.denial_counts)
        return _AuthorizationResult(
            candidates=refreshed.candidates,
            denial_counts=combined_denials,
            suppression_snapshot=refreshed.suppression_snapshot,
        )

    async def _read_snapshot(
        self,
        lookup: MonotonicSuppressionLookup,
        candidates: Sequence[RetrievalCandidate[_PayloadT]],
        denial_counts: Counter[DenialReason],
        *,
        count_failure: bool = True,
    ) -> SuppressionSnapshot | None:
        source_digests = _unique(candidate.source_digest for candidate in candidates)
        try:
            snapshot = await lookup.snapshot_many(source_digests)
        except Exception:
            if count_failure:
                denial_counts[DenialReason.SUPPRESSION_STATE_UNAVAILABLE] += len(
                    candidates
                )
            return None
        if not isinstance(snapshot, SuppressionSnapshot):
            if count_failure:
                denial_counts[DenialReason.SUPPRESSION_STATE_UNAVAILABLE] += len(
                    candidates
                )
            return None
        return snapshot

    @staticmethod
    def _snapshots_match(
        first: SuppressionSnapshot,
        second: SuppressionSnapshot,
        candidates: Sequence[RetrievalCandidate[_PayloadT]],
    ) -> bool:
        if first.epoch != second.epoch:
            return False
        missing = object()
        for source_digest in _unique(
            candidate.source_digest for candidate in candidates
        ):
            try:
                first_record = first.records.get(source_digest, missing)
                second_record = second.records.get(source_digest, missing)
            except Exception:
                return False
            if first_record is missing or first_record != second_record:
                return False
        return True

    @staticmethod
    def _filter_snapshot(
        context: RetrievalContext,
        candidates: Sequence[RetrievalCandidate[_PayloadT]],
        snapshot: SuppressionSnapshot,
        denial_counts: Counter[DenialReason],
    ) -> list[RetrievalCandidate[_PayloadT]]:
        unsuppressed: list[RetrievalCandidate[_PayloadT]] = []
        missing = object()
        for candidate in candidates:
            try:
                record = snapshot.records.get(candidate.source_digest, missing)
            except Exception:
                record = missing
            if record is missing or record is None:
                denial_counts[DenialReason.SUPPRESSION_STATE_UNAVAILABLE] += 1
            elif not isinstance(record, SuppressionRecord):
                denial_counts[DenialReason.SUPPRESSION_STATE_UNAVAILABLE] += 1
            elif record.source_digest != candidate.source_digest:
                denial_counts[DenialReason.SUPPRESSION_STATE_UNAVAILABLE] += 1
            elif record.suppressed:
                denial_counts[DenialReason.SUPPRESSED] += 1
            elif not record.verified_authorization:
                denial_counts[DenialReason.SUPPRESSION_STATE_UNAVAILABLE] += 1
            elif record.tenant_digest != make_tenant_digest(context.tenant_id):
                denial_counts[DenialReason.SUPPRESSION_AUTHORIZATION_STALE] += 1
            elif record.policy_id != candidate.policy_id:
                denial_counts[DenialReason.SUPPRESSION_AUTHORIZATION_STALE] += 1
            elif record.generation != candidate.source_generation:
                denial_counts[DenialReason.SUPPRESSION_AUTHORIZATION_STALE] += 1
            elif record.policy_revision != candidate.policy_revision:
                denial_counts[DenialReason.SUPPRESSION_AUTHORIZATION_STALE] += 1
            elif (
                record.group_graph_revision != candidate.group_graph_revision
                or record.group_graph_revision != context.group_graph_revision
            ):
                denial_counts[DenialReason.SUPPRESSION_AUTHORIZATION_STALE] += 1
            else:
                unsuppressed.append(candidate)
        return unsuppressed

    async def _filter_suppressed(
        self,
        candidates: Sequence[RetrievalCandidate[_PayloadT]],
        denial_counts: Counter[DenialReason],
    ) -> list[RetrievalCandidate[_PayloadT]]:
        if not candidates:
            return []

        source_digests = _unique(candidate.source_digest for candidate in candidates)
        try:
            decisions = await self._suppression_lookup.is_suppressed_many(
                source_digests
            )
        except Exception:
            denial_counts[DenialReason.SUPPRESSION_STATE_UNAVAILABLE] += len(candidates)
            return []

        if not isinstance(decisions, Mapping):
            denial_counts[DenialReason.SUPPRESSION_STATE_UNAVAILABLE] += len(candidates)
            return []

        unsuppressed: list[RetrievalCandidate[_PayloadT]] = []
        missing = object()
        for candidate in candidates:
            try:
                decision = decisions.get(candidate.source_digest, missing)
            except Exception:
                decision = missing
            if type(decision) is not bool:
                denial_counts[DenialReason.SUPPRESSION_STATE_UNAVAILABLE] += 1
            elif decision:
                denial_counts[DenialReason.SUPPRESSED] += 1
            else:
                unsuppressed.append(candidate)
        return unsuppressed

    async def _filter_policy(
        self,
        context: RetrievalContext,
        candidates: Sequence[RetrievalCandidate[_PayloadT]],
        denial_counts: Counter[DenialReason],
    ) -> list[RetrievalCandidate[_PayloadT]]:
        if not candidates:
            return []

        policy_ids = _unique(candidate.policy_id for candidate in candidates)
        try:
            policies = await self._policy_lookup.get_policies(policy_ids)
        except Exception:
            denial_counts[DenialReason.POLICY_LOOKUP_FAILED] += len(candidates)
            return []

        if not isinstance(policies, Mapping):
            denial_counts[DenialReason.POLICY_LOOKUP_FAILED] += len(candidates)
            return []

        authorized: list[RetrievalCandidate[_PayloadT]] = []
        missing = object()
        for candidate in candidates:
            try:
                policy = policies.get(candidate.policy_id, missing)
            except Exception:
                policy = missing

            if policy is missing or policy is None:
                denial_counts[DenialReason.POLICY_MISSING] += 1
            elif not isinstance(policy, AccessPolicy):
                denial_counts[DenialReason.POLICY_CORRUPT] += 1
            elif policy.policy_id != candidate.policy_id:
                denial_counts[DenialReason.POLICY_CORRUPT] += 1
            elif policy.tenant_id != context.tenant_id:
                denial_counts[DenialReason.TENANT_MISMATCH] += 1
            elif policy.revision != candidate.policy_revision:
                denial_counts[DenialReason.POLICY_STALE] += 1
            elif (
                policy.group_graph_revision != candidate.group_graph_revision
                or policy.group_graph_revision != context.group_graph_revision
            ):
                denial_counts[DenialReason.GROUP_GRAPH_REVISION_STALE] += 1
            elif not policy.allows(context):
                denial_counts[DenialReason.ACCESS_DENIED] += 1
            else:
                authorized.append(candidate)
        return authorized


@dataclass(frozen=True, slots=True)
class ScoredCandidate(Generic[_PayloadT]):
    candidate: RetrievalCandidate[_PayloadT]
    score: float


class GuardedInMemoryRetriever(Generic[_QueryT, _PayloadT]):
    """Reference retriever that authorizes every candidate before scoring."""

    def __init__(
        self,
        *,
        candidates: Iterable[RetrievalCandidate[_PayloadT]],
        guard: RetrievalGuard,
        scorer: Callable[
            [_QueryT, RetrievalCandidate[_PayloadT]],
            float | Awaitable[float],
        ],
    ) -> None:
        self._candidates = tuple(candidates)
        self._guard = guard
        self._scorer = scorer

    async def search(
        self,
        query: _QueryT,
        *,
        context: RetrievalContext,
        limit: int | None = None,
    ) -> tuple[ScoredCandidate[_PayloadT], ...]:
        """Authorize, then score and rank the surviving candidates."""

        if limit is not None and (isinstance(limit, bool) or limit < 1):
            raise ValueError("limit must be a positive integer or None")

        authorization = await self._guard._authorize_candidates(
            context, self._candidates
        )
        scored: list[ScoredCandidate[_PayloadT]] = []
        for candidate in authorization.candidates:
            score = self._scorer(query, candidate)
            if inspect.isawaitable(score):
                score = await score
            if isinstance(score, bool) or not isinstance(score, (int, float)):
                raise TypeError("scorer must return a finite number")
            score = float(score)
            if not math.isfinite(score):
                raise ValueError("scorer must return a finite number")
            scored.append(ScoredCandidate(candidate=candidate, score=score))

        authorization = await self._guard._revalidate_after_scoring(
            context,
            authorization,
        )
        allowed_object_ids = {id(candidate) for candidate in authorization.candidates}
        scored = [
            result for result in scored if id(result.candidate) in allowed_object_ids
        ]
        self._guard._record_result(authorization)

        scored.sort(key=lambda result: result.score, reverse=True)
        if limit is not None:
            del scored[limit:]
        return tuple(scored)
