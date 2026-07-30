"""
Synor is a framework for building and running indexing pipelines.
"""

# Version check
from ._version import __version__ as __version__
from . import _version_check as _version_check  # noqa: F401


# Re-export APIs from internal modules

from . import _internal
from ._internal.api import *  # noqa: F403
from .execution import (
    AppExplanation as AppExplanation,
    ExecutionGuarantee as ExecutionGuarantee,
    ExecutionReport as ExecutionReport,
    ExecutionStatus as ExecutionStatus,
    PlannedChange as PlannedChange,
    RevocationStartupError as RevocationStartupError,
    RunMode as RunMode,
    SynorRuntime as SynorRuntime,
)
from .governance import (
    AccessSnapshot as AccessSnapshot,
    GovernedSourceItem as GovernedSourceItem,
    SourceIdentity as SourceIdentity,
)
from .policy import (
    DataClassification as DataClassification,
    EgressPolicy as EgressPolicy,
    EgressRequest as EgressRequest,
    NetworkAccess as NetworkAccess,
    PolicyDecision as PolicyDecision,
    PolicyViolation as PolicyViolation,
    authorize_egress as authorize_egress,
    current_policy as current_policy,
    network_is_denied as network_is_denied,
    policy_from_env as policy_from_env,
    policy_scope as policy_scope,
)
from .pii import (
    PIIAction as PIIAction,
    PIICategory as PIICategory,
    PIIFinding as PIIFinding,
    PIIPolicy as PIIPolicy,
    PIIQuarantineRequired as PIIQuarantineRequired,
    PIIViolation as PIIViolation,
    current_pii_policy as current_pii_policy,
    enforce_pii as enforce_pii,
    find_pii as find_pii,
    pii_policy_from_env as pii_policy_from_env,
    pii_scope as pii_scope,
)
from .provenance import ArtifactProvenance as ArtifactProvenance
from .quarantine import (
    QuarantineCase as QuarantineCase,
    QuarantineRepository as QuarantineRepository,
    QuarantineStatus as QuarantineStatus,
)
from .replay import (
    ReplayEnvelope as ReplayEnvelope,
    ReplayVerification as ReplayVerification,
)
from .revocation import (
    RevocationCase as RevocationCase,
    RevocationHealth as RevocationHealth,
    RevocationPolicy as RevocationPolicy,
    RevocationReceipt as RevocationReceipt,
    RevocationSummary as RevocationSummary,
)
from .retrieval import RetrievalGuard as RetrievalGuard
from .state import (
    EncryptedStateStore as EncryptedStateStore,
    FileStateStore as FileStateStore,
    MemoryStateStore as MemoryStateStore,
    StateDecryptionError as StateDecryptionError,
    StateStore as StateStore,
    decode_state_key as decode_state_key,
    generate_state_key as generate_state_key,
    state_store_from_env as state_store_from_env,
)

__all__ = [
    *_internal.api.__all__,
    "AppExplanation",
    "AccessSnapshot",
    "ArtifactProvenance",
    "DataClassification",
    "EncryptedStateStore",
    "EgressPolicy",
    "EgressRequest",
    "ExecutionReport",
    "ExecutionGuarantee",
    "ExecutionStatus",
    "FileStateStore",
    "MemoryStateStore",
    "NetworkAccess",
    "PIIAction",
    "PIICategory",
    "PIIFinding",
    "PIIPolicy",
    "PIIQuarantineRequired",
    "PIIViolation",
    "PlannedChange",
    "PolicyDecision",
    "PolicyViolation",
    "QuarantineCase",
    "QuarantineRepository",
    "QuarantineStatus",
    "GovernedSourceItem",
    "ReplayEnvelope",
    "ReplayVerification",
    "RetrievalGuard",
    "RevocationCase",
    "RevocationHealth",
    "RevocationPolicy",
    "RevocationReceipt",
    "RevocationStartupError",
    "RevocationSummary",
    "RunMode",
    "StateDecryptionError",
    "StateStore",
    "SourceIdentity",
    "SynorRuntime",
    "authorize_egress",
    "current_pii_policy",
    "current_policy",
    "decode_state_key",
    "enforce_pii",
    "find_pii",
    "generate_state_key",
    "network_is_denied",
    "pii_policy_from_env",
    "pii_scope",
    "policy_from_env",
    "policy_scope",
    "state_store_from_env",
]
