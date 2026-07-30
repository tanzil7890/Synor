from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping
from typing import cast

import pytest

from synor._internal.suppression import StateStoreSuppressionIndex
from synor._internal.revocation_model import make_tenant_digest
from synor._internal.retrieval_guard import (
    AccessPolicy,
    AccessPolicyLookup,
    DenialReason,
    GuardedInMemoryRetriever,
    RetrievalCandidate,
    RetrievalContext,
    RetrievalGuard,
)
from synor.state import MemoryStateStore


_SOURCE_DIGEST = hashlib.sha256(b"guard-source").hexdigest()
_TENANT_DIGEST = make_tenant_digest("tenant-a")


class _SuppressionLookup:
    def __init__(self, suppressed: Mapping[str, bool] | None = None) -> None:
        self.suppressed = dict(suppressed or {})
        self.calls: list[tuple[str, ...]] = []

    async def is_suppressed_many(
        self, source_digests: tuple[str, ...]
    ) -> Mapping[str, bool]:
        self.calls.append(source_digests)
        return {
            source_digest: self.suppressed.get(source_digest, False)
            for source_digest in source_digests
        }


class _PolicyLookup:
    def __init__(
        self, policies: Mapping[str, AccessPolicy | None] | None = None
    ) -> None:
        self.policies = dict(policies or {})
        self.calls: list[tuple[str, ...]] = []

    async def get_policies(
        self, policy_ids: tuple[str, ...]
    ) -> Mapping[str, AccessPolicy | None]:
        self.calls.append(policy_ids)
        return {policy_id: self.policies.get(policy_id) for policy_id in policy_ids}


class _BlockingPolicyLookup(_PolicyLookup):
    def __init__(self, policies: Mapping[str, AccessPolicy | None]) -> None:
        super().__init__(policies)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self._blocked = False

    async def get_policies(
        self, policy_ids: tuple[str, ...]
    ) -> Mapping[str, AccessPolicy | None]:
        self.calls.append(policy_ids)
        if not self._blocked:
            self._blocked = True
            self.entered.set()
            await self.release.wait()
        return {policy_id: self.policies.get(policy_id) for policy_id in policy_ids}


class _BrokenSuppressionLookup:
    async def is_suppressed_many(
        self, source_digests: tuple[str, ...]
    ) -> Mapping[str, bool]:
        del source_digests
        raise RuntimeError("planted-sensitive-remote-error")


class _MalformedSuppressionLookup:
    async def is_suppressed_many(
        self, source_digests: tuple[str, ...]
    ) -> Mapping[str, bool]:
        del source_digests
        return cast(Mapping[str, bool], {"source-corrupt": 1})


class _BrokenPolicyLookup:
    async def get_policies(
        self, policy_ids: tuple[str, ...]
    ) -> Mapping[str, AccessPolicy | None]:
        del policy_ids
        raise RuntimeError("planted-sensitive-remote-error")


def _context(
    *,
    tenant_id: str = "tenant-a",
    principal_id: str = "principal-a",
    group_graph_revision: str = "groups-v1",
    group_ids: frozenset[str] = frozenset({"group-a"}),
) -> RetrievalContext:
    return RetrievalContext(
        tenant_id=tenant_id,
        principal_id=principal_id,
        group_graph_revision=group_graph_revision,
        group_ids=group_ids,
    )


def _policy(
    policy_id: str,
    *,
    tenant_id: str = "tenant-a",
    revision: str = "policy-v1",
    group_graph_revision: str = "groups-v1",
    allowed_principal_ids: frozenset[str] = frozenset({"principal-a"}),
    allowed_group_ids: frozenset[str] = frozenset(),
    denied_principal_ids: frozenset[str] = frozenset(),
    denied_group_ids: frozenset[str] = frozenset(),
) -> AccessPolicy:
    return AccessPolicy(
        policy_id=policy_id,
        tenant_id=tenant_id,
        revision=revision,
        group_graph_revision=group_graph_revision,
        allowed_principal_ids=allowed_principal_ids,
        allowed_group_ids=allowed_group_ids,
        denied_principal_ids=denied_principal_ids,
        denied_group_ids=denied_group_ids,
    )


def _candidate(
    candidate_id: str,
    policy_id: str,
    *,
    source_digest: str | None = None,
    source_generation: int = 1,
    tenant_id: str = "tenant-a",
    policy_revision: str = "policy-v1",
    group_graph_revision: str = "groups-v1",
) -> RetrievalCandidate[str]:
    return RetrievalCandidate(
        candidate_id=candidate_id,
        source_digest=source_digest or f"source-{candidate_id}",
        source_generation=source_generation,
        tenant_id=tenant_id,
        policy_id=policy_id,
        policy_revision=policy_revision,
        group_graph_revision=group_graph_revision,
        payload=f"payload-{candidate_id}",
    )


@pytest.mark.parametrize("field", ["tenant_id", "principal_id"])
def test_context_requires_authenticated_nonempty_identity(field: str) -> None:
    values = {
        "tenant_id": "tenant-a",
        "principal_id": "principal-a",
        "group_graph_revision": "groups-v1",
    }
    values[field] = " \t "
    with pytest.raises(ValueError, match=field):
        RetrievalContext(
            tenant_id=values["tenant_id"],
            principal_id=values["principal_id"],
            group_graph_revision=values["group_graph_revision"],
        )


@pytest.mark.asyncio
async def test_guard_allows_direct_and_group_grants_and_applies_deny_first() -> None:
    policies = {
        "direct": _policy("direct"),
        "group": _policy(
            "group",
            allowed_principal_ids=frozenset(),
            allowed_group_ids=frozenset({"group-a"}),
        ),
        "denied": _policy(
            "denied",
            denied_group_ids=frozenset({"group-a"}),
        ),
    }
    guard = RetrievalGuard(
        suppression_lookup=_SuppressionLookup(),
        policy_lookup=_PolicyLookup(policies),
    )
    candidates = (
        _candidate("direct", "direct"),
        _candidate("group", "group"),
        _candidate("denied", "denied"),
    )

    allowed = await guard.filter_candidates(_context(), candidates)

    assert [candidate.candidate_id for candidate in allowed] == ["direct", "group"]
    metrics = guard.metrics.snapshot()
    assert metrics.queries == 1
    assert metrics.allowed_candidates == 2
    assert metrics.denied_candidates == 1
    assert metrics.fail_closed_candidates == 0
    assert metrics.denial_counts == {DenialReason.ACCESS_DENIED: 1}


@pytest.mark.asyncio
async def test_tenant_mismatch_is_rejected_before_dependency_lookups() -> None:
    suppression = _SuppressionLookup()
    policies = _PolicyLookup({"policy": _policy("policy", tenant_id="tenant-b")})
    guard = RetrievalGuard(
        suppression_lookup=suppression,
        policy_lookup=policies,
    )

    allowed = await guard.filter_candidates(
        _context(), (_candidate("foreign", "policy", tenant_id="tenant-b"),)
    )

    assert allowed == ()
    assert suppression.calls == []
    assert policies.calls == []
    assert guard.metrics.snapshot().denial_counts == {DenialReason.TENANT_MISMATCH: 1}


@pytest.mark.asyncio
async def test_missing_corrupt_and_stale_policy_state_fails_closed() -> None:
    corrupt_policy = cast(AccessPolicy, object())
    policies: dict[str, AccessPolicy | None] = {
        "missing": None,
        "corrupt": corrupt_policy,
        "stale-policy": _policy("stale-policy", revision="policy-v2"),
        "stale-groups": _policy("stale-groups", group_graph_revision="groups-v2"),
    }
    guard = RetrievalGuard(
        suppression_lookup=_SuppressionLookup(),
        policy_lookup=_PolicyLookup(policies),
    )

    allowed = await guard.filter_candidates(
        _context(),
        (
            _candidate("missing", "missing"),
            _candidate("corrupt", "corrupt"),
            _candidate("stale-policy", "stale-policy"),
            _candidate("stale-groups", "stale-groups"),
        ),
    )

    assert allowed == ()
    metrics = guard.metrics.snapshot()
    assert metrics.denial_counts == {
        DenialReason.POLICY_MISSING: 1,
        DenialReason.POLICY_CORRUPT: 1,
        DenialReason.POLICY_STALE: 1,
        DenialReason.GROUP_GRAPH_REVISION_STALE: 1,
    }
    assert metrics.fail_closed_candidates == 4


@pytest.mark.asyncio
async def test_suppression_denies_before_policy_lookup() -> None:
    suppression = _SuppressionLookup({"source-blocked": True})
    policies = _PolicyLookup(
        {
            "blocked-policy": _policy("blocked-policy"),
            "clear-policy": _policy("clear-policy"),
        }
    )
    guard = RetrievalGuard(
        suppression_lookup=suppression,
        policy_lookup=policies,
    )

    allowed = await guard.filter_candidates(
        _context(),
        (
            _candidate(
                "blocked",
                "blocked-policy",
                source_digest="source-blocked",
            ),
            _candidate("clear", "clear-policy", source_digest="source-clear"),
        ),
    )

    assert [candidate.candidate_id for candidate in allowed] == ["clear"]
    assert suppression.calls == [("source-blocked", "source-clear")]
    assert policies.calls == [("clear-policy",)]
    assert guard.metrics.snapshot().denial_counts == {DenialReason.SUPPRESSED: 1}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "authorization_policy_id",
        "authorization_policy_revision",
        "authorization_group_revision",
    ),
    [
        ("other-policy", "policy-v1", "groups-v1"),
        ("policy", "policy-v0", "groups-v1"),
        ("policy", "policy-v1", "groups-v0"),
    ],
)
async def test_authorized_suppression_policy_and_revision_must_match_candidate(
    authorization_policy_id: str,
    authorization_policy_revision: str,
    authorization_group_revision: str,
) -> None:
    index = StateStoreSuppressionIndex(MemoryStateStore())
    await index.authorize(
        source_digest=_SOURCE_DIGEST,
        tenant_digest=_TENANT_DIGEST,
        policy_id=authorization_policy_id,
        generation=1,
        policy_revision=authorization_policy_revision,
        group_graph_revision=authorization_group_revision,
    )
    policies = _PolicyLookup({"policy": _policy("policy")})
    guard = RetrievalGuard(
        suppression_lookup=index,
        policy_lookup=policies,
    )

    allowed = await guard.filter_candidates(
        _context(),
        (
            _candidate(
                "candidate",
                "policy",
                source_digest=_SOURCE_DIGEST,
            ),
        ),
    )

    assert allowed == ()
    assert policies.calls == []
    assert guard.metrics.snapshot().denial_counts == {
        DenialReason.SUPPRESSION_AUTHORIZATION_STALE: 1
    }


@pytest.mark.asyncio
async def test_only_candidate_from_active_authorization_generation_can_serve() -> None:
    index = StateStoreSuppressionIndex(MemoryStateStore())
    await index.authorize(
        source_digest=_SOURCE_DIGEST,
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy",
        generation=2,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )
    guard = RetrievalGuard(
        suppression_lookup=index,
        policy_lookup=_PolicyLookup({"policy": _policy("policy")}),
    )
    old_candidate = _candidate(
        "generation-1",
        "policy",
        source_digest=_SOURCE_DIGEST,
        source_generation=1,
    )
    current_candidate = _candidate(
        "generation-2",
        "policy",
        source_digest=_SOURCE_DIGEST,
        source_generation=2,
    )

    allowed = await guard.filter_candidates(
        _context(),
        (old_candidate, current_candidate),
    )

    assert allowed == (current_candidate,)
    assert guard.metrics.snapshot().denial_counts == {
        DenialReason.SUPPRESSION_AUTHORIZATION_STALE: 1
    }


@pytest.mark.asyncio
async def test_authorized_suppression_tenant_must_match_query_context() -> None:
    index = StateStoreSuppressionIndex(MemoryStateStore())
    await index.authorize(
        source_digest=_SOURCE_DIGEST,
        tenant_digest=make_tenant_digest("tenant-b"),
        policy_id="policy",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )
    policies = _PolicyLookup({"policy": _policy("policy")})
    guard = RetrievalGuard(
        suppression_lookup=index,
        policy_lookup=policies,
    )

    allowed = await guard.filter_candidates(
        _context(),
        (
            _candidate(
                "candidate",
                "policy",
                source_digest=_SOURCE_DIGEST,
            ),
        ),
    )

    assert allowed == ()
    assert policies.calls == []
    assert guard.metrics.snapshot().denial_counts == {
        DenialReason.SUPPRESSION_AUTHORIZATION_STALE: 1
    }


@pytest.mark.asyncio
async def test_suppression_persisted_during_policy_lookup_cannot_return() -> None:
    index = StateStoreSuppressionIndex(MemoryStateStore())
    await index.authorize(
        source_digest=_SOURCE_DIGEST,
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )
    policies = _BlockingPolicyLookup({"policy": _policy("policy")})
    guard = RetrievalGuard(
        suppression_lookup=index,
        policy_lookup=policies,
    )
    candidate = _candidate(
        "candidate",
        "policy",
        source_digest=_SOURCE_DIGEST,
    )

    filtering = asyncio.create_task(guard.filter_candidates(_context(), (candidate,)))
    await policies.entered.wait()
    await index.suppress(
        source_digest=_SOURCE_DIGEST,
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy",
        generation=2,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        reason="access_lost",
        case_id="case1_policy_race",
    )
    policies.release.set()

    assert await filtering == ()
    assert guard.metrics.snapshot().denial_counts == {DenialReason.SUPPRESSED: 1}


@pytest.mark.asyncio
async def test_missing_and_malformed_suppression_state_fails_closed() -> None:
    policies = _PolicyLookup(
        {
            "missing-policy": _policy("missing-policy"),
            "corrupt-policy": _policy("corrupt-policy"),
        }
    )
    guard = RetrievalGuard(
        suppression_lookup=_MalformedSuppressionLookup(),
        policy_lookup=policies,
    )

    allowed = await guard.filter_candidates(
        _context(),
        (
            _candidate(
                "missing",
                "missing-policy",
                source_digest="source-missing",
            ),
            _candidate(
                "corrupt",
                "corrupt-policy",
                source_digest="source-corrupt",
            ),
        ),
    )

    assert allowed == ()
    assert policies.calls == []
    assert guard.metrics.snapshot().denial_counts == {
        DenialReason.SUPPRESSION_STATE_UNAVAILABLE: 2
    }


@pytest.mark.asyncio
async def test_guard_batches_and_deduplicates_dependency_lookups() -> None:
    suppression = _SuppressionLookup()
    policies = _PolicyLookup(
        {
            "policy-one": _policy("policy-one"),
            "policy-two": _policy("policy-two"),
        }
    )
    guard = RetrievalGuard(
        suppression_lookup=suppression,
        policy_lookup=policies,
    )
    candidates = (
        _candidate("one-a", "policy-one", source_digest="source-one"),
        _candidate("one-b", "policy-one", source_digest="source-one"),
        _candidate("two-a", "policy-two", source_digest="source-two"),
        _candidate("two-b", "policy-two", source_digest="source-two"),
    )

    allowed = await guard.filter_candidates(_context(), candidates)

    assert allowed == candidates
    assert suppression.calls == [("source-one", "source-two")]
    assert policies.calls == [("policy-one", "policy-two")]


@pytest.mark.asyncio
async def test_retriever_filters_before_scoring() -> None:
    suppression = _SuppressionLookup({"source-suppressed": True})
    policies = _PolicyLookup(
        {
            "allowed": _policy("allowed"),
            "denied": _policy(
                "denied",
                allowed_principal_ids=frozenset({"other-principal"}),
            ),
            "suppressed": _policy("suppressed"),
        }
    )
    guard = RetrievalGuard(
        suppression_lookup=suppression,
        policy_lookup=policies,
    )
    scored_ids: list[str] = []

    def scorer(query: str, candidate: RetrievalCandidate[str]) -> float:
        scored_ids.append(candidate.candidate_id)
        if candidate.candidate_id != "allowed":
            raise AssertionError("a denied payload reached the scorer")
        return float(candidate.payload.endswith(query))

    retriever = GuardedInMemoryRetriever(
        candidates=(
            _candidate("denied", "denied"),
            _candidate(
                "suppressed",
                "suppressed",
                source_digest="source-suppressed",
            ),
            RetrievalCandidate(
                candidate_id="allowed",
                source_digest="source-allowed",
                source_generation=1,
                tenant_id="tenant-a",
                policy_id="allowed",
                policy_revision="policy-v1",
                group_graph_revision="groups-v1",
                payload="payload-needle",
            ),
        ),
        guard=guard,
        scorer=scorer,
    )

    results = await retriever.search("needle", context=_context())

    assert scored_ids == ["allowed"]
    assert [result.candidate.candidate_id for result in results] == ["allowed"]
    assert results[0].score == 1.0


@pytest.mark.asyncio
async def test_suppression_persisted_while_scoring_cannot_return() -> None:
    index = StateStoreSuppressionIndex(MemoryStateStore())
    await index.authorize(
        source_digest=_SOURCE_DIGEST,
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )
    guard = RetrievalGuard(
        suppression_lookup=index,
        policy_lookup=_PolicyLookup({"policy": _policy("policy")}),
    )
    scoring_started = asyncio.Event()
    release_score = asyncio.Event()
    scored_ids: list[str] = []

    async def scorer(query: str, candidate: RetrievalCandidate[str]) -> float:
        del query
        scored_ids.append(candidate.candidate_id)
        scoring_started.set()
        await release_score.wait()
        return 1.0

    retriever = GuardedInMemoryRetriever(
        candidates=(
            _candidate(
                "candidate",
                "policy",
                source_digest=_SOURCE_DIGEST,
            ),
        ),
        guard=guard,
        scorer=scorer,
    )
    searching = asyncio.create_task(retriever.search("query", context=_context()))
    await scoring_started.wait()
    await index.suppress(
        source_digest=_SOURCE_DIGEST,
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy",
        generation=2,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        reason="access_lost",
        case_id="case1_scoring_race",
    )
    release_score.set()

    assert await searching == ()
    assert scored_ids == ["candidate"]
    metrics = guard.metrics.snapshot()
    assert metrics.queries == 1
    assert metrics.allowed_candidates == 0
    assert metrics.denial_counts == {DenialReason.SUPPRESSED: 1}


@pytest.mark.asyncio
async def test_policy_changed_while_scoring_cannot_return() -> None:
    index = StateStoreSuppressionIndex(MemoryStateStore())
    await index.authorize(
        source_digest=_SOURCE_DIGEST,
        tenant_digest=_TENANT_DIGEST,
        policy_id="policy",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )
    policies = _PolicyLookup({"policy": _policy("policy")})
    guard = RetrievalGuard(
        suppression_lookup=index,
        policy_lookup=policies,
    )
    scoring_started = asyncio.Event()
    release_score = asyncio.Event()
    scored_ids: list[str] = []

    async def scorer(query: str, candidate: RetrievalCandidate[str]) -> float:
        del query
        scored_ids.append(candidate.candidate_id)
        scoring_started.set()
        await release_score.wait()
        return 1.0

    retriever = GuardedInMemoryRetriever(
        candidates=(
            _candidate(
                "candidate",
                "policy",
                source_digest=_SOURCE_DIGEST,
            ),
        ),
        guard=guard,
        scorer=scorer,
    )
    searching = asyncio.create_task(retriever.search("query", context=_context()))
    await scoring_started.wait()
    policies.policies["policy"] = _policy(
        "policy",
        revision="policy-v2",
        allowed_principal_ids=frozenset(),
    )
    release_score.set()

    assert await searching == ()
    assert scored_ids == ["candidate"]
    assert policies.calls == [("policy",), ("policy",)]
    metrics = guard.metrics.snapshot()
    assert metrics.queries == 1
    assert metrics.allowed_candidates == 0
    assert metrics.denial_counts == {DenialReason.POLICY_STALE: 1}


@pytest.mark.asyncio
async def test_dependency_failures_deny_without_exposing_remote_error() -> None:
    candidate = _candidate("candidate", "policy")
    suppression_guard = RetrievalGuard(
        suppression_lookup=_BrokenSuppressionLookup(),
        policy_lookup=_PolicyLookup({"policy": _policy("policy")}),
    )
    policy_guard = RetrievalGuard(
        suppression_lookup=_SuppressionLookup(),
        policy_lookup=_BrokenPolicyLookup(),
    )

    assert await suppression_guard.filter_candidates(_context(), (candidate,)) == ()
    assert await policy_guard.filter_candidates(_context(), (candidate,)) == ()
    suppression_snapshot = suppression_guard.metrics.snapshot()
    policy_snapshot = policy_guard.metrics.snapshot()
    assert suppression_snapshot.denial_counts == {
        DenialReason.SUPPRESSION_STATE_UNAVAILABLE: 1
    }
    assert policy_snapshot.denial_counts == {DenialReason.POLICY_LOOKUP_FAILED: 1}
    assert "planted-sensitive-remote-error" not in repr(suppression_snapshot)
    assert "planted-sensitive-remote-error" not in repr(policy_snapshot)


def test_policy_lookup_fake_satisfies_structural_contract() -> None:
    assert isinstance(_PolicyLookup(), AccessPolicyLookup)
