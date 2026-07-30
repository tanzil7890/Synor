"""Public fail-closed retrieval authorization contracts.

The guard authorizes candidates before scoring and revalidates them after an
asynchronous scorer returns. Callers must construct :class:`RetrievalContext`
at a trusted authentication and group-resolution boundary.
"""

from __future__ import annotations

from ._internal.retrieval_guard import (
    AccessPolicy as AccessPolicy,
    AccessPolicyLookup as AccessPolicyLookup,
    DenialReason as DenialReason,
    GuardedInMemoryRetriever as GuardedInMemoryRetriever,
    InMemoryAccessPolicyLookup as InMemoryAccessPolicyLookup,
    MonotonicSuppressionLookup as MonotonicSuppressionLookup,
    RetrievalCandidate as RetrievalCandidate,
    RetrievalContext as RetrievalContext,
    RetrievalGuard as RetrievalGuard,
    RetrievalGuardMetrics as RetrievalGuardMetrics,
    RetrievalMetricsSnapshot as RetrievalMetricsSnapshot,
    ScoredCandidate as ScoredCandidate,
    SuppressionLookup as SuppressionLookup,
)

__all__ = [
    "AccessPolicy",
    "AccessPolicyLookup",
    "DenialReason",
    "GuardedInMemoryRetriever",
    "InMemoryAccessPolicyLookup",
    "MonotonicSuppressionLookup",
    "RetrievalCandidate",
    "RetrievalContext",
    "RetrievalGuard",
    "RetrievalGuardMetrics",
    "RetrievalMetricsSnapshot",
    "ScoredCandidate",
    "SuppressionLookup",
]
