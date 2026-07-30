"""Public, access-sensitive source governance contracts.

These contracts are additive. Existing sources may continue yielding their
current resource types; governed sources opt in by yielding
:class:`GovernedSourceItem` values with stable identity and access state.
"""

from __future__ import annotations

from ._internal.revocation_model import (
    AccessEffect as AccessEffect,
    AccessRule as AccessRule,
    AccessSnapshot as AccessSnapshot,
    GovernedSourceItem as GovernedSourceItem,
    SnapshotResult as SnapshotResult,
    SourceEventKind as SourceEventKind,
    SourceIdentity as SourceIdentity,
    canonical_access_digest as canonical_access_digest,
    make_observation_id as make_observation_id,
)

__all__ = [
    "AccessEffect",
    "AccessRule",
    "AccessSnapshot",
    "GovernedSourceItem",
    "SnapshotResult",
    "SourceEventKind",
    "SourceIdentity",
    "canonical_access_digest",
    "make_observation_id",
]
