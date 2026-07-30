"""Internal strict-revocation policy presets and capability validation."""

from __future__ import annotations

import enum
import math
from dataclasses import dataclass

from .revocation_model import (
    RevocationPolicyDecision,
    SourceEventKind,
    TargetRevocationCapabilities,
)


class RevocationPolicyMode(str, enum.Enum):
    COMPATIBILITY = "compatibility"
    STRICT_QUERY_VERIFIED = "strict_query_verified"
    TEST = "test"


class RevocationCapabilityError(RuntimeError):
    """Raised before apply when a strict target cannot meet its contract."""

    def __init__(self, missing_capabilities: tuple[str, ...]) -> None:
        self.missing_capabilities = missing_capabilities
        super().__init__(
            "strict revocation target is missing capabilities: "
            + ", ".join(missing_capabilities)
        )


@dataclass(frozen=True, slots=True)
class RevocationPolicy:
    """Policy mechanics, not a legal determination.

    The strict preset is deliberately opinionated.  It never downgrades to
    acknowledgement-only cleanup and it requires a guarded query boundary.
    """

    mode: RevocationPolicyMode
    require_suppression: bool
    require_current_acl: bool
    require_tenant_isolation: bool
    require_negative_verification: bool
    require_consistency_fence: bool
    verification_timeout_seconds: float
    initial_backoff_seconds: float
    maximum_backoff_seconds: float
    jitter_ratio: float

    def __post_init__(self) -> None:
        if not isinstance(self.mode, RevocationPolicyMode):
            raise TypeError("mode must be a RevocationPolicyMode")
        for name in (
            "require_suppression",
            "require_current_acl",
            "require_tenant_isolation",
            "require_negative_verification",
            "require_consistency_fence",
        ):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be a bool")
        for name in (
            "verification_timeout_seconds",
            "initial_backoff_seconds",
            "maximum_backoff_seconds",
            "jitter_ratio",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValueError(f"{name} must be a finite number")
        if self.verification_timeout_seconds <= 0:
            raise ValueError("verification_timeout_seconds must be positive")
        if self.initial_backoff_seconds < 0:
            raise ValueError("initial_backoff_seconds must not be negative")
        if self.maximum_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError(
                "maximum_backoff_seconds cannot be smaller than initial backoff"
            )
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between zero and one")

    @classmethod
    def compatibility(cls) -> "RevocationPolicy":
        return cls(
            mode=RevocationPolicyMode.COMPATIBILITY,
            require_suppression=False,
            require_current_acl=False,
            require_tenant_isolation=False,
            require_negative_verification=False,
            require_consistency_fence=False,
            verification_timeout_seconds=30.0,
            initial_backoff_seconds=0.05,
            maximum_backoff_seconds=1.0,
            jitter_ratio=0.2,
        )

    @classmethod
    def strict_query_verified(cls) -> "RevocationPolicy":
        return cls(
            mode=RevocationPolicyMode.STRICT_QUERY_VERIFIED,
            require_suppression=True,
            require_current_acl=True,
            require_tenant_isolation=True,
            require_negative_verification=True,
            require_consistency_fence=True,
            verification_timeout_seconds=30.0,
            initial_backoff_seconds=0.05,
            maximum_backoff_seconds=1.0,
            jitter_ratio=0.2,
        )

    @classmethod
    def _for_test(
        cls,
        *,
        verification_timeout_seconds: float = 0.1,
        initial_backoff_seconds: float = 0.0,
    ) -> "RevocationPolicy":
        return cls(
            mode=RevocationPolicyMode.TEST,
            require_suppression=True,
            require_current_acl=True,
            require_tenant_isolation=True,
            require_negative_verification=True,
            require_consistency_fence=True,
            verification_timeout_seconds=verification_timeout_seconds,
            initial_backoff_seconds=initial_backoff_seconds,
            maximum_backoff_seconds=max(initial_backoff_seconds, 0.001),
            jitter_ratio=0.0,
        )

    @property
    def is_strict(self) -> bool:
        return self.mode is not RevocationPolicyMode.COMPATIBILITY

    def to_dict(self) -> dict[str, bool | float | str]:
        """Return the stable, connector-independent policy contract."""

        return {
            "mode": self.mode.value,
            "require_suppression": self.require_suppression,
            "require_current_acl": self.require_current_acl,
            "require_tenant_isolation": self.require_tenant_isolation,
            "require_negative_verification": self.require_negative_verification,
            "require_consistency_fence": self.require_consistency_fence,
            "verification_timeout_seconds": self.verification_timeout_seconds,
            "initial_backoff_seconds": self.initial_backoff_seconds,
            "maximum_backoff_seconds": self.maximum_backoff_seconds,
            "jitter_ratio": self.jitter_ratio,
        }

    def validate_capabilities(
        self,
        capabilities: TargetRevocationCapabilities,
        *,
        decision: RevocationPolicyDecision = RevocationPolicyDecision.DESTROY,
    ) -> None:
        """Block unsupported strict work before target materialization."""

        if not self.is_strict:
            return
        if not isinstance(capabilities, TargetRevocationCapabilities):
            raise TypeError("capabilities must be TargetRevocationCapabilities")
        if not isinstance(decision, RevocationPolicyDecision):
            raise TypeError("decision must be a RevocationPolicyDecision")

        missing: list[str] = []
        if self.require_suppression and not capabilities.atomic_serving_suppression:
            missing.append("atomic_serving_suppression")
        if self.require_current_acl and not capabilities.query_time_acl_filter:
            missing.append("query_time_acl_filter")
        if self.require_tenant_isolation and not capabilities.tenant_isolation:
            missing.append("tenant_isolation")
        if (
            self.require_negative_verification
            and not capabilities.negative_read_verification
        ):
            missing.append("negative_read_verification")
        if self.require_consistency_fence and not capabilities.consistency_fence:
            missing.append("consistency_fence")

        if decision in {
            RevocationPolicyDecision.DESTROY,
            RevocationPolicyDecision.RESTRICT,
        } and not (capabilities.exact_id_delete or capabilities.source_id_bulk_delete):
            missing.append("exact_id_delete|source_id_bulk_delete")
        if (
            decision is RevocationPolicyDecision.PRESERVE_ON_HOLD
            and not capabilities.legal_hold_isolation
        ):
            missing.append("legal_hold_isolation")

        if missing:
            raise RevocationCapabilityError(tuple(missing))

    def decide(
        self,
        event: SourceEventKind,
        *,
        legal_hold: bool = False,
    ) -> RevocationPolicyDecision | None:
        """Return a mechanical default that callers may override."""

        if not isinstance(event, SourceEventKind):
            raise TypeError("event must be a SourceEventKind")
        if type(legal_hold) is not bool:
            raise TypeError("legal_hold must be a bool")
        if legal_hold:
            return RevocationPolicyDecision.PRESERVE_ON_HOLD
        if event in {
            SourceEventKind.PRESENT,
            SourceEventKind.CONTENT_CHANGED,
            SourceEventKind.SCAN_INCOMPLETE,
        }:
            return None
        if event is SourceEventKind.AMBIGUOUS_REMOVAL:
            return RevocationPolicyDecision.INVESTIGATE_AMBIGUOUS
        if event in {
            SourceEventKind.ACL_CHANGED,
            SourceEventKind.GROUP_GRAPH_CHANGED,
            SourceEventKind.PERMISSION_EXPIRED,
        }:
            return RevocationPolicyDecision.RESTRICT
        return RevocationPolicyDecision.DESTROY
