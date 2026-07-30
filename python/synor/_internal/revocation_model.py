"""Internal data contracts for evidence-backed index revocation.

The stable Phase 5 subset is re-exported through public facade modules; storage
and transition implementation remains internal. Serialized evidence contains
only opaque identifiers, digests, counts, timestamps, and controlled error
codes.
"""

from __future__ import annotations

import datetime
import enum
import hashlib
import json
import re
from dataclasses import dataclass, fields, replace
from typing import Any, Generic, Literal, Mapping, NamedTuple, TypeVar, cast

from .typing import (
    MemoStateOutcome,
    NonExistenceType,
    StableKey,
    is_non_existence,
)


_SCHEMA_VERSION = 1
_HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SAFE_TOKEN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,255}$")
_CASE_EXTENSION_KEYS = frozenset({"x_safe_retry_class", "x_safe_retry_count"})
_T = TypeVar("_T")
_EnumT = TypeVar("_EnumT", bound=enum.Enum)
_JsonScalar = str | int | float | bool | None


class RevocationSchemaError(ValueError):
    """Raised when persisted revocation state has an unsupported schema."""


class InvalidRevocationTransition(ValueError):
    """Raised when a case attempts an illegal lifecycle transition."""


class SourceEventKind(str, enum.Enum):
    PRESENT = "present"
    CONTENT_CHANGED = "content_changed"
    ACL_CHANGED = "acl_changed"
    GROUP_GRAPH_CHANGED = "group_graph_changed"
    PERMISSION_EXPIRED = "permission_expired"
    ACCESS_LOST = "access_lost"
    SOURCE_DELETED = "source_deleted"
    MOVED_SCOPE = "moved_scope"
    RETENTION_EXPIRED = "retention_expired"
    AMBIGUOUS_REMOVAL = "ambiguous_removal"
    SCAN_INCOMPLETE = "scan_incomplete"


class AccessEffect(str, enum.Enum):
    GRANT = "grant"
    DENY = "deny"


class RevocationPolicyDecision(str, enum.Enum):
    DESTROY = "destroy"
    RESTRICT = "restrict"
    PRESERVE_ON_HOLD = "preserve_on_hold"
    INVESTIGATE_AMBIGUOUS = "investigate_ambiguous"


class RevocationStage(str, enum.Enum):
    OBSERVED = "observed"
    SUPPRESSED = "suppressed_from_serving"
    PLANNED = "deletion_planned"
    DISPATCHED = "dispatched"
    ACKNOWLEDGED = "acknowledged"
    FENCE_REACHED = "consistency_fence_reached"
    VERIFIED = "absence_verified"
    RETAINED_ISOLATED = "retained_isolated"
    CLOSED = "closed"
    FAILED = "failed"
    BLOCKED = "blocked"


class AssuranceLevel(str, enum.Enum):
    UNVERIFIED = "unverified"
    ACKNOWLEDGED = "acknowledged"
    QUERY_VERIFIED = "query_verified"
    ERASURE_ATTESTED = "erasure_attested"
    RETAINED_ISOLATED = "retained_isolated"


class VerificationOutcome(str, enum.Enum):
    ABSENT = "absent"
    PRESENT = "present"
    WRONG_ACL = "wrong_acl"
    RETAINED_ISOLATED = "retained_isolated"
    UNSUPPORTED = "unsupported"
    TIMEOUT = "timeout"
    TRANSPORT_FAILURE = "transport_failure"


class SafeRevocationErrorCode(str, enum.Enum):
    TARGET_PRESENT = "target.present"
    TARGET_WRONG_ACL = "target.wrong_acl"
    TARGET_UNSUPPORTED = "target.unsupported"
    TARGET_TIMEOUT = "target.timeout"
    TARGET_TRANSPORT_FAILURE = "target.transport_failure"
    TARGET_APPLY_FAILURE = "target.apply_failure"
    TARGET_PERMISSION_DENIED = "target.permission_denied"
    EVIDENCE_WRITE_FAILURE = "evidence.write_failure"
    PROVIDER_MISSING = "runtime.provider_missing"
    CAPABILITY_UNSUPPORTED = "runtime.capability_unsupported"
    POLICY_BLOCKED = "runtime.policy_blocked"
    SNAPSHOT_INCOMPLETE = "runtime.snapshot_incomplete"
    VERIFICATION_PROTOCOL = "runtime.verification_protocol"


_VERIFICATION_FAILURE_ERROR_CODES = {
    VerificationOutcome.PRESENT: SafeRevocationErrorCode.TARGET_PRESENT,
    VerificationOutcome.WRONG_ACL: SafeRevocationErrorCode.TARGET_WRONG_ACL,
    VerificationOutcome.UNSUPPORTED: SafeRevocationErrorCode.TARGET_UNSUPPORTED,
    VerificationOutcome.TIMEOUT: SafeRevocationErrorCode.TARGET_TIMEOUT,
    VerificationOutcome.TRANSPORT_FAILURE: (
        SafeRevocationErrorCode.TARGET_TRANSPORT_FAILURE
    ),
}


def verification_outcome_error_code(
    outcome: VerificationOutcome,
) -> SafeRevocationErrorCode:
    """Return the canonical safe error code for a verification outcome."""

    if not isinstance(outcome, VerificationOutcome):
        raise TypeError("outcome must be a VerificationOutcome")
    return _VERIFICATION_FAILURE_ERROR_CODES.get(
        outcome,
        SafeRevocationErrorCode.VERIFICATION_PROTOCOL,
    )


class EffectOperation(str, enum.Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    RESTRICT = "restrict"
    ISOLATE = "isolate"


def _required_text(name: str, value: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty canonical string")
    return value


def _safe_token(name: str, value: str) -> str:
    if not isinstance(value, str) or not _SAFE_TOKEN.fullmatch(value):
        raise ValueError(f"{name} must be an opaque safe token")
    return value


def _digest(name: str, value: str) -> str:
    if not isinstance(value, str) or not _HEX_DIGEST.fullmatch(value):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _action_identifier(name: str, value: str) -> str:
    prefix = "action1_"
    if (
        not isinstance(value, str)
        or not value.startswith(prefix)
        or not _HEX_DIGEST.fullmatch(value.removeprefix(prefix))
    ):
        raise ValueError(f"{name} must be a deterministic action identifier")
    return value


def _prefixed_digest_identifier(name: str, value: str, prefix: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith(prefix)
        or not _HEX_DIGEST.fullmatch(value.removeprefix(prefix))
    ):
        raise ValueError(f"{name} must be a deterministic versioned identifier")
    return value


def _observation_identifier(name: str, value: str) -> str:
    prefix = "obs1_"
    if (
        not isinstance(value, str)
        or not value.startswith(prefix)
        or not _HEX_DIGEST.fullmatch(value.removeprefix(prefix))
    ):
        raise ValueError(f"{name} must be a deterministic observation identifier")
    return value


def _aware_utc(name: str, value: datetime.datetime) -> datetime.datetime:
    if (
        not isinstance(value, datetime.datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(datetime.timezone.utc)


def _utc_text(value: datetime.datetime) -> str:
    return _aware_utc("timestamp", value).isoformat().replace("+00:00", "Z")


def _datetime_from_text(name: str, value: object) -> datetime.datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO-8601 string")
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        parsed = None
    if parsed is None:
        # Stored values are untrusted. Raise after leaving the handler so the
        # parser exception (which can retain the raw value) is not reachable
        # through ``__context__`` or a formatted traceback.
        raise ValueError(f"{name} must be an ISO-8601 string") from None
    return _aware_utc(name, parsed)


def _string_field(value: Mapping[str, object], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str):
        raise ValueError(f"{name} must be a string")
    return item


def _optional_string_field(value: Mapping[str, object], name: str) -> str | None:
    item = value.get(name)
    if item is None:
        return None
    if not isinstance(item, str):
        raise ValueError(f"{name} must be a string or null")
    return item


def _integer_field(value: Mapping[str, object], name: str) -> int:
    item = value.get(name)
    if not isinstance(item, int) or isinstance(item, bool):
        raise ValueError(f"{name} must be an integer")
    return item


def _controlled_enum(
    value: str,
    enum_type: type[_EnumT],
    error_message: str,
) -> _EnumT:
    try:
        result = enum_type(value)
    except ValueError:
        result = None
    if result is None:
        # Enum's ValueError includes the rejected value. It may be persisted
        # attacker-controlled metadata, so expose only a controlled message
        # and deliberately sever the implicit exception context.
        raise ValueError(error_message) from None
    return result


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _length_delimited(domain: bytes, values: tuple[str | bytes, ...]) -> bytes:
    output = bytearray(domain)
    output.extend(b"\x00")
    for value in values:
        payload = value.encode("utf-8") if isinstance(value, str) else value
        output.extend(len(payload).to_bytes(8, "big"))
        output.extend(payload)
    return bytes(output)


def _source_revision_metadata_digest(source_revision: str) -> str:
    return hashlib.sha256(
        _length_delimited(
            b"synor-revocation-source-revision-metadata-v1",
            (_required_text("source_revision", source_revision),),
        )
    ).hexdigest()


def _stable_id(prefix: str, domain: str, values: tuple[str | bytes, ...]) -> str:
    payload = _length_delimited(f"synor-revocation-{domain}-v1".encode("ascii"), values)
    return f"{prefix}1_{hashlib.sha256(payload).hexdigest()}"


def _extras(
    value: Mapping[str, object],
    known: frozenset[str],
    allowed: frozenset[str],
) -> tuple[tuple[str, _JsonScalar], ...]:
    preserved: list[tuple[str, _JsonScalar]] = []
    for key in sorted(set(value) - known):
        if key not in allowed:
            continue
        item = value[key]
        if key == "x_safe_retry_count":
            if isinstance(item, int) and not isinstance(item, bool) and item >= 0:
                preserved.append((key, item))
        elif (
            key == "x_safe_retry_class"
            and isinstance(item, str)
            and item in {"bounded", "retryable", "terminal"}
        ):
            preserved.append((key, item))
    return tuple(preserved)


def _validate_extensions(
    extensions: tuple[tuple[str, _JsonScalar], ...],
    allowed: frozenset[str],
) -> None:
    keys: set[str] = set()
    for key, item in extensions:
        if key not in allowed:
            raise ValueError("extension key is not approved for revocation evidence")
        if key in keys:
            raise ValueError("extension keys must be unique")
        keys.add(key)
        if key == "x_safe_retry_count" and (
            not isinstance(item, int) or isinstance(item, bool) or item < 0
        ):
            raise ValueError("retry-count extension must be a non-negative integer")
        if key == "x_safe_retry_class" and item not in {
            "bounded",
            "retryable",
            "terminal",
        }:
            raise ValueError("retry-class extension has an unsupported value")


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    connector_instance_id: str
    source_scope_id: str
    item_id: str

    def __post_init__(self) -> None:
        _required_text("connector_instance_id", self.connector_instance_id)
        _required_text("source_scope_id", self.source_scope_id)
        _required_text("item_id", self.item_id)

    def canonical_bytes(self) -> bytes:
        """Return collision-resistant, explicitly versioned canonical bytes."""

        return _length_delimited(
            b"synor-source-identity-v1",
            (
                self.connector_instance_id,
                self.source_scope_id,
                self.item_id,
            ),
        )

    def evidence_digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def component_key(self) -> StableKey:
        return (
            "governed-source-v1",
            self.connector_instance_id,
            self.source_scope_id,
            self.item_id,
        )

    def __synor_audit_metadata__(self) -> dict[str, object]:
        """Project raw source identifiers to their evidence digest."""

        return {"source_digest": self.evidence_digest()}


@dataclass(frozen=True, slots=True)
class AccessRule:
    """One normalized operational ACL entry.

    ``subject_id`` is an opaque provider identity, never a display name.
    """

    effect: AccessEffect
    subject_type: str
    subject_id: str
    role: str
    inherited_from: str | None = None
    expires_at: datetime.datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.effect, AccessEffect):
            raise TypeError("effect must be an AccessEffect")
        _safe_token("subject_type", self.subject_type)
        _required_text("subject_id", self.subject_id)
        _safe_token("role", self.role)
        if self.inherited_from is not None:
            _required_text("inherited_from", self.inherited_from)
        if self.expires_at is not None:
            object.__setattr__(
                self, "expires_at", _aware_utc("expires_at", self.expires_at)
            )

    def canonical_tuple(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.effect.value,
            self.subject_type,
            self.subject_id,
            self.role,
            self.inherited_from or "",
            _utc_text(self.expires_at) if self.expires_at is not None else "",
        )

    def __synor_audit_metadata__(self) -> dict[str, object]:
        """Exclude the operational subject and inheritance identifiers."""

        return {
            "effect": self.effect.value,
            "subject_type": self.subject_type,
            "subject_digest": hashlib.sha256(
                _length_delimited(
                    b"synor-access-subject-v1",
                    (self.subject_type, self.subject_id),
                )
            ).hexdigest(),
            "role": self.role,
            "inherited": self.inherited_from is not None,
            "expires_at": (
                _utc_text(self.expires_at) if self.expires_at is not None else None
            ),
        }


def canonical_access_digest(
    *,
    tenant_id: str,
    policy_id: str,
    policy_revision: str,
    rules: tuple[AccessRule, ...],
) -> str:
    """Hash a semantic ACL without depending on remote ordering."""

    _required_text("tenant_id", tenant_id)
    _required_text("policy_id", policy_id)
    _required_text("policy_revision", policy_revision)
    payload = {
        "schema_version": _SCHEMA_VERSION,
        "tenant_id": tenant_id,
        "policy_id": policy_id,
        "policy_revision": policy_revision,
        "rules": sorted(rule.canonical_tuple() for rule in rules),
    }
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


@dataclass(frozen=True, slots=True)
class AccessSnapshot:
    tenant_id: str
    policy_id: str
    policy_revision: str
    policy_digest: str
    group_graph_revision: str
    inherited_from: tuple[str, ...] = ()
    valid_until: datetime.datetime | None = None
    retention_class: str | None = None
    legal_state: str | None = None

    def __post_init__(self) -> None:
        _required_text("tenant_id", self.tenant_id)
        _required_text("policy_id", self.policy_id)
        _required_text("policy_revision", self.policy_revision)
        _digest("policy_digest", self.policy_digest)
        _required_text("group_graph_revision", self.group_graph_revision)
        if len(set(self.inherited_from)) != len(self.inherited_from):
            raise ValueError("inherited_from must not contain duplicates")
        if tuple(sorted(self.inherited_from)) != self.inherited_from:
            raise ValueError("inherited_from must be sorted")
        if self.valid_until is not None:
            object.__setattr__(
                self, "valid_until", _aware_utc("valid_until", self.valid_until)
            )
        if self.retention_class is not None:
            _safe_token("retention_class", self.retention_class)
        if self.legal_state is not None:
            _safe_token("legal_state", self.legal_state)

    def __synor_audit_metadata__(self) -> dict[str, object]:
        """Return policy evidence without the raw tenant or parent IDs."""

        return {
            "tenant_digest": make_tenant_digest(self.tenant_id),
            "policy_id": self.policy_id,
            "policy_revision": self.policy_revision,
            "policy_digest": self.policy_digest,
            "group_graph_revision": self.group_graph_revision,
            "inherited_from_count": len(self.inherited_from),
            "valid_until": (
                _utc_text(self.valid_until) if self.valid_until is not None else None
            ),
            "retention_class": self.retention_class,
            "legal_state": self.legal_state,
        }


class GovernedMemoState(NamedTuple):
    source_revision: str
    content_fingerprint: bytes | None
    tenant_id: str | None
    policy_id: str | None
    policy_revision: str | None
    policy_digest: str | None
    group_graph_revision: str | None
    inherited_from: tuple[str, ...]
    valid_until: str | None
    retention_class: str | None
    legal_state: str | None
    event: str


@dataclass(frozen=True, slots=True)
class GovernedSourceItem(Generic[_T]):
    identity: SourceIdentity
    resource: _T | None
    source_revision: str
    content_fingerprint: bytes | None
    access: AccessSnapshot | None
    event: SourceEventKind
    observation_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.event, SourceEventKind):
            raise TypeError("event must be a SourceEventKind")
        _required_text("source_revision", self.source_revision)
        _safe_token("observation_id", self.observation_id)

    def __synor_memo_key__(self) -> object:
        return self.identity.component_key()

    async def __synor_memo_state__(
        self, previous: GovernedMemoState | NonExistenceType
    ) -> MemoStateOutcome:
        access = self.access
        current = GovernedMemoState(
            source_revision=self.source_revision,
            content_fingerprint=self.content_fingerprint,
            tenant_id=access.tenant_id if access is not None else None,
            policy_id=access.policy_id if access is not None else None,
            policy_revision=access.policy_revision if access is not None else None,
            policy_digest=access.policy_digest if access is not None else None,
            group_graph_revision=(
                access.group_graph_revision if access is not None else None
            ),
            inherited_from=access.inherited_from if access is not None else (),
            valid_until=(
                _utc_text(access.valid_until)
                if access is not None and access.valid_until is not None
                else None
            ),
            retention_class=(access.retention_class if access is not None else None),
            legal_state=access.legal_state if access is not None else None,
            event=self.event.value,
        )
        return MemoStateOutcome(
            state=current,
            memo_valid=not is_non_existence(previous) and previous == current,
        )

    def __synor_audit_metadata__(self) -> dict[str, object]:
        """Return governance evidence and deliberately omit ``resource``."""

        return {
            "source_digest": self.identity.evidence_digest(),
            "source_revision_digest": hashlib.sha256(
                _length_delimited(
                    b"synor-source-revision-v1",
                    (self.source_revision,),
                )
            ).hexdigest(),
            "content_fingerprint": self.content_fingerprint,
            "access": self.access,
            "event": self.event.value,
            "observation_id": self.observation_id,
        }


@dataclass(frozen=True, slots=True)
class SnapshotResult:
    connector_instance_id: str
    source_scope_id: str
    epoch: str
    cursor_before: str | None
    cursor_after: str | None
    status: Literal["complete", "partial", "failed"]
    item_count: int
    inaccessible_scope_digests: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _required_text("connector_instance_id", self.connector_instance_id)
        _required_text("source_scope_id", self.source_scope_id)
        _required_text("epoch", self.epoch)
        if not isinstance(self.status, str) or self.status not in {
            "complete",
            "partial",
            "failed",
        }:
            raise ValueError("invalid snapshot status")
        if (
            not isinstance(self.item_count, int)
            or isinstance(self.item_count, bool)
            or self.item_count < 0
        ):
            raise ValueError("item_count must not be negative")
        for digest in self.inaccessible_scope_digests:
            _digest("inaccessible_scope_digest", digest)
        if self.status == "complete" and self.inaccessible_scope_digests:
            raise ValueError("a complete snapshot cannot contain inaccessible scopes")

    @property
    def authorizes_missing_item_cleanup(self) -> bool:
        return self.status == "complete" and not self.inaccessible_scope_digests

    @property
    def authorizes_checkpoint_advance(self) -> bool:
        return self.authorizes_missing_item_cleanup


@dataclass(frozen=True, slots=True)
class DerivativeRef:
    source_digest: str
    target_provider_id: str
    target_instance_digest: str
    target_locator_digest: str
    owner_component_path: str
    derivative_kind: str
    content_revision: str
    policy_revision: str
    group_graph_revision: str

    def __post_init__(self) -> None:
        _digest("source_digest", self.source_digest)
        _safe_token("target_provider_id", self.target_provider_id)
        _digest("target_instance_digest", self.target_instance_digest)
        _digest("target_locator_digest", self.target_locator_digest)
        _required_text("owner_component_path", self.owner_component_path)
        _safe_token("derivative_kind", self.derivative_kind)
        _required_text("content_revision", self.content_revision)
        _required_text("policy_revision", self.policy_revision)
        _required_text("group_graph_revision", self.group_graph_revision)


@dataclass(frozen=True, slots=True)
class TargetRevocationCapabilities:
    atomic_serving_suppression: bool
    exact_id_delete: bool
    source_id_bulk_delete: bool
    query_time_acl_filter: bool
    tenant_isolation: bool
    synchronous_acknowledgement: bool
    consistency_fence: bool
    negative_read_verification: bool
    external_enumeration: bool
    legal_hold_isolation: bool
    physical_erasure_attestation: bool
    capability_version: str = "1"

    def __post_init__(self) -> None:
        for field in fields(self):
            if (
                field.name != "capability_version"
                and type(getattr(self, field.name)) is not bool
            ):
                raise TypeError(f"{field.name} capability must be a bool")
        _safe_token("capability_version", self.capability_version)

    def contract_digest(self) -> str:
        """Bind the exact factual capability profile used by a proof contract."""

        return hashlib.sha256(
            _canonical_json(
                {field.name: getattr(self, field.name) for field in fields(self)}
            )
        ).hexdigest()

    def to_dict(self) -> dict[str, object]:
        """Return inspectable factual capabilities plus their contract digest."""

        return {
            "schema_version": _SCHEMA_VERSION,
            **{field.name: getattr(self, field.name) for field in fields(self)},
            "contract_digest": self.contract_digest(),
        }


@dataclass(frozen=True, slots=True)
class EffectDescriptor:
    operation_kind: EffectOperation
    action_id: str
    source_digest: str
    source_generation: int
    target_locator_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.operation_kind, EffectOperation):
            raise TypeError("operation_kind must be an EffectOperation")
        _safe_token("action_id", self.action_id)
        _digest("source_digest", self.source_digest)
        if (
            not isinstance(self.source_generation, int)
            or isinstance(self.source_generation, bool)
            or self.source_generation < 1
        ):
            raise ValueError("source_generation must be a positive integer")
        _digest("target_locator_digest", self.target_locator_digest)

    @property
    def destructive(self) -> bool:
        # Phase 2 has a proof contract for purge and legal-hold isolation.
        # In-place ACL restriction needs a distinct, principal-aware read-back
        # outcome and remains blocked until a real connector implements it.
        return self.operation_kind in {
            EffectOperation.DELETE,
            EffectOperation.ISOLATE,
        }


@dataclass(frozen=True, slots=True)
class RevocationCase:
    case_id: str
    observation_id: str
    source_digest: str
    source_revision: str
    tenant_digest: str
    policy_id: str
    policy_revision: str
    policy_digest: str
    group_graph_revision: str
    legal_state: str | None
    suppression_generation: int
    reason: SourceEventKind
    policy_decision: RevocationPolicyDecision
    stage: RevocationStage
    observed_at: datetime.datetime
    suppress_by: datetime.datetime
    verify_by: datetime.datetime
    expected_targets: tuple[str, ...]
    version: int
    safe_error_code: str | None = None
    extensions: tuple[tuple[str, _JsonScalar], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.reason, SourceEventKind):
            raise TypeError("reason must be a SourceEventKind")
        if not isinstance(self.policy_decision, RevocationPolicyDecision):
            raise TypeError("policy_decision must be a RevocationPolicyDecision")
        if not isinstance(self.stage, RevocationStage):
            raise TypeError("stage must be a RevocationStage")
        if self.stage is RevocationStage.VERIFIED and self.policy_decision not in {
            RevocationPolicyDecision.DESTROY,
            RevocationPolicyDecision.RESTRICT,
        }:
            raise ValueError(
                "absence_verified case stage requires a destroy or restrict "
                "policy decision"
            )
        if (
            self.stage is RevocationStage.RETAINED_ISOLATED
            and self.policy_decision is not RevocationPolicyDecision.PRESERVE_ON_HOLD
        ):
            raise ValueError(
                "retained_isolated case stage requires preserve_on_hold policy decision"
            )
        _prefixed_digest_identifier("case_id", self.case_id, "case1_")
        _observation_identifier("observation_id", self.observation_id)
        _digest("source_digest", self.source_digest)
        _required_text("source_revision", self.source_revision)
        _digest("tenant_digest", self.tenant_digest)
        _safe_token("policy_id", self.policy_id)
        _safe_token("policy_revision", self.policy_revision)
        _digest("policy_digest", self.policy_digest)
        _safe_token("group_graph_revision", self.group_graph_revision)
        if self.legal_state is not None:
            _safe_token("legal_state", self.legal_state)
        if (
            not isinstance(self.suppression_generation, int)
            or isinstance(self.suppression_generation, bool)
            or self.suppression_generation < 1
        ):
            raise ValueError("suppression_generation must be positive")
        object.__setattr__(
            self, "observed_at", _aware_utc("observed_at", self.observed_at)
        )
        object.__setattr__(
            self, "suppress_by", _aware_utc("suppress_by", self.suppress_by)
        )
        object.__setattr__(self, "verify_by", _aware_utc("verify_by", self.verify_by))
        if self.suppress_by < self.observed_at:
            raise ValueError("suppress_by cannot precede observed_at")
        if self.verify_by < self.suppress_by:
            raise ValueError("verify_by cannot precede suppress_by")
        for obligation_id in self.expected_targets:
            _action_identifier("expected_target", obligation_id)
        if len(set(self.expected_targets)) != len(self.expected_targets):
            raise ValueError("expected_targets must not contain duplicates")
        if tuple(sorted(self.expected_targets)) != self.expected_targets:
            raise ValueError("expected_targets must be sorted")
        if (
            not isinstance(self.version, int)
            or isinstance(self.version, bool)
            or self.version < 1
        ):
            raise ValueError("case version must be positive")
        if self.safe_error_code is not None:
            _controlled_enum(
                self.safe_error_code,
                SafeRevocationErrorCode,
                "safe_error_code must use the controlled registry",
            )
        _validate_extensions(self.extensions, _CASE_EXTENSION_KEYS)

    def to_dict(self) -> dict[str, object]:
        value: dict[str, object] = {
            "schema_version": _SCHEMA_VERSION,
            "case_id": self.case_id,
            "observation_id": self.observation_id,
            "source_digest": self.source_digest,
            "source_revision": self.source_revision,
            "tenant_digest": self.tenant_digest,
            "policy_id": self.policy_id,
            "policy_revision": self.policy_revision,
            "policy_digest": self.policy_digest,
            "group_graph_revision": self.group_graph_revision,
            "legal_state": self.legal_state,
            "suppression_generation": self.suppression_generation,
            "reason": self.reason.value,
            "policy_decision": self.policy_decision.value,
            "stage": self.stage.value,
            "observed_at": _utc_text(self.observed_at),
            "suppress_by": _utc_text(self.suppress_by),
            "verify_by": _utc_text(self.verify_by),
            "expected_targets": list(self.expected_targets),
            "version": self.version,
            "safe_error_code": self.safe_error_code,
        }
        value.update(self.extensions)
        return value

    def __synor_audit_metadata__(self) -> dict[str, object]:
        """Project case evidence without its reversible source revision."""

        return {
            "schema_version": _SCHEMA_VERSION,
            "kind": "revocation_case",
            "case_id": self.case_id,
            "observation_id": self.observation_id,
            "source_digest": self.source_digest,
            "source_revision_digest": _source_revision_metadata_digest(
                self.source_revision
            ),
            "tenant_digest": self.tenant_digest,
            "policy_id": self.policy_id,
            "policy_revision": self.policy_revision,
            "policy_digest": self.policy_digest,
            "group_graph_revision": self.group_graph_revision,
            "legal_state": self.legal_state,
            "suppression_generation": self.suppression_generation,
            "reason": self.reason.value,
            "policy_decision": self.policy_decision.value,
            "stage": self.stage.value,
            "observed_at": _utc_text(self.observed_at),
            "suppress_by": _utc_text(self.suppress_by),
            "verify_by": _utc_text(self.verify_by),
            "expected_targets": list(self.expected_targets),
            "version": self.version,
            "safe_error_code": self.safe_error_code,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "RevocationCase":
        _require_schema(value)
        targets = value.get("expected_targets")
        if not isinstance(targets, list) or not all(
            isinstance(item, str) for item in targets
        ):
            raise ValueError("expected_targets must be a string list")
        known = frozenset(
            {
                "schema_version",
                "case_id",
                "observation_id",
                "source_digest",
                "source_revision",
                "tenant_digest",
                "policy_id",
                "policy_revision",
                "policy_digest",
                "group_graph_revision",
                "legal_state",
                "suppression_generation",
                "reason",
                "policy_decision",
                "stage",
                "observed_at",
                "suppress_by",
                "verify_by",
                "expected_targets",
                "version",
                "safe_error_code",
            }
        )
        return cls(
            case_id=_string_field(value, "case_id"),
            observation_id=_string_field(value, "observation_id"),
            source_digest=_string_field(value, "source_digest"),
            source_revision=_string_field(value, "source_revision"),
            tenant_digest=_string_field(value, "tenant_digest"),
            policy_id=_string_field(value, "policy_id"),
            policy_revision=_string_field(value, "policy_revision"),
            policy_digest=_string_field(value, "policy_digest"),
            group_graph_revision=_string_field(value, "group_graph_revision"),
            legal_state=_optional_string_field(value, "legal_state"),
            suppression_generation=_integer_field(value, "suppression_generation"),
            reason=_controlled_enum(
                _string_field(value, "reason"),
                SourceEventKind,
                "reason must use a controlled source event kind",
            ),
            policy_decision=_controlled_enum(
                _string_field(value, "policy_decision"),
                RevocationPolicyDecision,
                "policy_decision must use a controlled revocation decision",
            ),
            stage=_controlled_enum(
                _string_field(value, "stage"),
                RevocationStage,
                "stage must use a controlled revocation stage",
            ),
            observed_at=_datetime_from_text("observed_at", value["observed_at"]),
            suppress_by=_datetime_from_text("suppress_by", value["suppress_by"]),
            verify_by=_datetime_from_text("verify_by", value["verify_by"]),
            expected_targets=tuple(cast(list[str], targets)),
            version=_integer_field(value, "version"),
            safe_error_code=_optional_string_field(value, "safe_error_code"),
            extensions=_extras(value, known, _CASE_EXTENSION_KEYS),
        )


_LEGAL_TRANSITIONS: Mapping[RevocationStage, frozenset[RevocationStage]] = {
    RevocationStage.OBSERVED: frozenset(
        {
            RevocationStage.SUPPRESSED,
            RevocationStage.RETAINED_ISOLATED,
            RevocationStage.BLOCKED,
            RevocationStage.FAILED,
        }
    ),
    RevocationStage.SUPPRESSED: frozenset(
        {
            RevocationStage.PLANNED,
            RevocationStage.BLOCKED,
            RevocationStage.FAILED,
        }
    ),
    RevocationStage.PLANNED: frozenset(
        {
            RevocationStage.DISPATCHED,
            RevocationStage.BLOCKED,
            RevocationStage.FAILED,
        }
    ),
    RevocationStage.DISPATCHED: frozenset(
        {
            RevocationStage.ACKNOWLEDGED,
            RevocationStage.BLOCKED,
            RevocationStage.FAILED,
        }
    ),
    RevocationStage.ACKNOWLEDGED: frozenset(
        {
            RevocationStage.FENCE_REACHED,
            RevocationStage.VERIFIED,
            RevocationStage.RETAINED_ISOLATED,
            RevocationStage.BLOCKED,
            RevocationStage.FAILED,
        }
    ),
    RevocationStage.FENCE_REACHED: frozenset(
        {
            RevocationStage.VERIFIED,
            RevocationStage.RETAINED_ISOLATED,
            RevocationStage.BLOCKED,
            RevocationStage.FAILED,
        }
    ),
    RevocationStage.VERIFIED: frozenset({RevocationStage.CLOSED}),
    RevocationStage.RETAINED_ISOLATED: frozenset({RevocationStage.CLOSED}),
    RevocationStage.FAILED: frozenset(
        {
            RevocationStage.SUPPRESSED,
            RevocationStage.PLANNED,
            RevocationStage.DISPATCHED,
            RevocationStage.ACKNOWLEDGED,
            RevocationStage.FENCE_REACHED,
            RevocationStage.VERIFIED,
            RevocationStage.RETAINED_ISOLATED,
            RevocationStage.BLOCKED,
        }
    ),
    RevocationStage.BLOCKED: frozenset(
        {
            RevocationStage.SUPPRESSED,
            RevocationStage.PLANNED,
            RevocationStage.DISPATCHED,
            RevocationStage.RETAINED_ISOLATED,
            RevocationStage.FAILED,
        }
    ),
    RevocationStage.CLOSED: frozenset(),
}


def transition_case(
    case: RevocationCase,
    stage: RevocationStage,
    *,
    safe_error_code: str | None = None,
) -> RevocationCase:
    """Return the next immutable case version after validating the transition."""

    if stage is case.stage:
        if safe_error_code == case.safe_error_code:
            return case
        raise InvalidRevocationTransition("same-stage mutation is not permitted")
    if stage is RevocationStage.VERIFIED and case.policy_decision not in {
        RevocationPolicyDecision.DESTROY,
        RevocationPolicyDecision.RESTRICT,
    }:
        raise InvalidRevocationTransition(
            "absence_verified requires a destroy or restrict policy decision"
        )
    if (
        stage is RevocationStage.RETAINED_ISOLATED
        and case.policy_decision is not RevocationPolicyDecision.PRESERVE_ON_HOLD
    ):
        raise InvalidRevocationTransition(
            "retained_isolated requires preserve_on_hold policy decision"
        )
    if stage not in _LEGAL_TRANSITIONS[case.stage]:
        raise InvalidRevocationTransition(
            f"illegal revocation transition: {case.stage.value} -> {stage.value}"
        )
    return replace(
        case,
        stage=stage,
        version=case.version + 1,
        safe_error_code=safe_error_code,
    )


def make_case_id(
    identity: SourceIdentity,
    source_revision: str,
    reason: SourceEventKind,
    policy_decision: RevocationPolicyDecision,
    observation_id: str,
) -> str:
    return _stable_id(
        "case",
        "case",
        (
            identity.canonical_bytes(),
            _required_text("source_revision", source_revision),
            reason.value,
            policy_decision.value,
            _observation_identifier("observation_id", observation_id),
        ),
    )


def make_observation_id(
    identity: SourceIdentity,
    source_revision: str,
    event: SourceEventKind,
    access: AccessSnapshot | None = None,
    *,
    observation_generation: str | None = None,
) -> str:
    access_values: tuple[str | bytes, ...] = (
        (
            access.tenant_id,
            access.policy_id,
            access.policy_revision,
            access.policy_digest,
            access.group_graph_revision,
            _length_delimited(
                b"synor-inherited-policy-sources-v1",
                access.inherited_from,
            ),
            _utc_text(access.valid_until) if access.valid_until is not None else "",
            access.retention_class or "",
            access.legal_state or "",
        )
        if access is not None
        else ("", "", "", "", "", "", "", "", "")
    )
    return _stable_id(
        "obs",
        "observation",
        (
            identity.canonical_bytes(),
            _required_text("source_revision", source_revision),
            event.value,
            *access_values,
            (
                _required_text("observation_generation", observation_generation)
                if observation_generation is not None
                else ""
            ),
        ),
    )


def make_tenant_digest(tenant_id: str) -> str:
    """Return the versioned evidence digest used for one opaque tenant ID."""

    return hashlib.sha256(
        _length_delimited(
            b"synor-tenant-identity-v1",
            (_required_text("tenant_id", tenant_id),),
        )
    ).hexdigest()


def make_proof_contract_digest(
    verifier_kind: str,
    consistency_contract: str,
    capability_digest: str,
) -> str:
    """Bind an obligation to its verifier, consistency, and capability profile."""

    return hashlib.sha256(
        _length_delimited(
            b"synor-target-proof-contract-v1",
            (
                _safe_token("verifier_kind", verifier_kind),
                _safe_token("consistency_contract", consistency_contract),
                _digest("capability_digest", capability_digest),
            ),
        )
    ).hexdigest()


def make_action_id(
    case_id: str,
    target_provider_id: str,
    target_instance_digest: str,
    target_locator_digest: str,
    operation: EffectOperation,
    proof_contract_digest: str,
) -> str:
    return _stable_id(
        "action",
        "action",
        (
            _safe_token("case_id", case_id),
            _safe_token("target_provider_id", target_provider_id),
            _digest("target_instance_digest", target_instance_digest),
            _digest("target_locator_digest", target_locator_digest),
            operation.value,
            _digest("proof_contract_digest", proof_contract_digest),
        ),
    )


def make_receipt_id(
    action_id: str,
    stage: RevocationStage,
    observed_outcome: VerificationOutcome,
    attempt: int,
) -> str:
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 0:
        raise ValueError("attempt must be a non-negative integer")
    return _stable_id(
        "receipt",
        "receipt",
        (
            _safe_token("action_id", action_id),
            stage.value,
            observed_outcome.value,
            str(attempt),
        ),
    )


@dataclass(frozen=True, slots=True)
class RevocationReceipt:
    schema_version: int
    receipt_id: str
    case_id: str
    obligation_id: str
    attempt: int
    source_digest: str
    target_provider_id: str
    target_instance_digest: str
    target_locator_digest: str
    operation_kind: str
    reason: str
    policy_decision: str
    stage: str
    assurance_level: str
    request_fingerprint: str
    operation_id: str | None
    affected_count: int | None
    capability_digest: str
    consistency_contract: str
    verifier_kind: str
    observed_outcome: str
    attempted_at: datetime.datetime
    verified_at: datetime.datetime | None
    safe_error_code: str | None
    previous_receipt_digest: str | None
    extensions: tuple[tuple[str, _JsonScalar], ...] = ()

    def __post_init__(self) -> None:
        if (
            not isinstance(self.schema_version, int)
            or isinstance(self.schema_version, bool)
            or self.schema_version != _SCHEMA_VERSION
        ):
            raise RevocationSchemaError("unsupported receipt schema major version")
        _prefixed_digest_identifier("receipt_id", self.receipt_id, "receipt1_")
        _prefixed_digest_identifier("case_id", self.case_id, "case1_")
        _action_identifier("obligation_id", self.obligation_id)
        if (
            not isinstance(self.attempt, int)
            or isinstance(self.attempt, bool)
            or self.attempt < 0
        ):
            raise ValueError("attempt must be a non-negative integer")
        _digest("source_digest", self.source_digest)
        _safe_token("target_provider_id", self.target_provider_id)
        _digest("target_instance_digest", self.target_instance_digest)
        _digest("target_locator_digest", self.target_locator_digest)
        operation = _controlled_enum(
            self.operation_kind,
            EffectOperation,
            "operation_kind must use a controlled effect operation",
        )
        _digest("capability_digest", self.capability_digest)
        _safe_token("consistency_contract", self.consistency_contract)
        _safe_token("verifier_kind", self.verifier_kind)
        proof_contract_digest = make_proof_contract_digest(
            self.verifier_kind,
            self.consistency_contract,
            self.capability_digest,
        )
        expected_obligation_id = make_action_id(
            self.case_id,
            self.target_provider_id,
            self.target_instance_digest,
            self.target_locator_digest,
            operation,
            proof_contract_digest,
        )
        if self.obligation_id != expected_obligation_id:
            raise ValueError(
                "obligation_id does not match the receipt target operation"
            )
        lifecycle_error = "receipt lifecycle fields must use known controlled values"
        _controlled_enum(self.reason, SourceEventKind, lifecycle_error)
        decision = _controlled_enum(
            self.policy_decision,
            RevocationPolicyDecision,
            lifecycle_error,
        )
        stage = _controlled_enum(self.stage, RevocationStage, lifecycle_error)
        assurance = _controlled_enum(
            self.assurance_level,
            AssuranceLevel,
            lifecycle_error,
        )
        outcome = _controlled_enum(
            self.observed_outcome,
            VerificationOutcome,
            lifecycle_error,
        )
        allowed_operations = {
            RevocationPolicyDecision.DESTROY: frozenset({EffectOperation.DELETE}),
            RevocationPolicyDecision.RESTRICT: frozenset(
                {EffectOperation.DELETE, EffectOperation.RESTRICT}
            ),
            RevocationPolicyDecision.PRESERVE_ON_HOLD: frozenset(
                {EffectOperation.ISOLATE}
            ),
            RevocationPolicyDecision.INVESTIGATE_AMBIGUOUS: frozenset(),
        }
        if operation not in allowed_operations[decision]:
            raise ValueError(
                "operation_kind is incompatible with the receipt policy decision"
            )
        expected_receipt_id = make_receipt_id(
            self.obligation_id,
            stage,
            outcome,
            self.attempt,
        )
        if self.receipt_id != expected_receipt_id:
            raise ValueError(
                "receipt_id does not match its obligation, stage, outcome, and attempt"
            )
        _digest("request_fingerprint", self.request_fingerprint)
        if self.operation_id is not None:
            _safe_token("operation_id", self.operation_id)
        if self.affected_count is not None and (
            not isinstance(self.affected_count, int)
            or isinstance(self.affected_count, bool)
            or self.affected_count < 0
        ):
            raise ValueError("affected_count must not be negative")
        object.__setattr__(
            self, "attempted_at", _aware_utc("attempted_at", self.attempted_at)
        )
        if self.verified_at is not None:
            object.__setattr__(
                self, "verified_at", _aware_utc("verified_at", self.verified_at)
            )
        if self.safe_error_code is not None:
            _controlled_enum(
                self.safe_error_code,
                SafeRevocationErrorCode,
                "safe_error_code must use the controlled registry",
            )
        expected_failure_code = _VERIFICATION_FAILURE_ERROR_CODES.get(outcome)
        if (
            expected_failure_code is not None
            and self.safe_error_code != expected_failure_code.value
        ):
            raise ValueError(
                "failed observed_outcome requires its canonical safe_error_code"
            )
        if (
            stage
            in {
                RevocationStage.VERIFIED,
                RevocationStage.RETAINED_ISOLATED,
            }
            and self.safe_error_code is not None
        ):
            raise ValueError(
                "terminal success receipt cannot contain a safe_error_code"
            )
        if self.previous_receipt_digest is not None:
            _digest("previous_receipt_digest", self.previous_receipt_digest)
        _validate_extensions(self.extensions, frozenset())
        if self.verified_at is not None and self.verified_at < self.attempted_at:
            raise ValueError("verified_at cannot precede attempted_at")
        if (
            assurance
            in {
                AssuranceLevel.QUERY_VERIFIED,
                AssuranceLevel.ERASURE_ATTESTED,
                AssuranceLevel.RETAINED_ISOLATED,
            }
            and self.verified_at is None
        ):
            raise ValueError("verified assurance requires verified_at")
        if (
            stage is RevocationStage.VERIFIED
            and outcome is not VerificationOutcome.ABSENT
        ):
            raise ValueError("absence_verified requires an absent observation")
        if stage is RevocationStage.VERIFIED and assurance not in {
            AssuranceLevel.QUERY_VERIFIED,
            AssuranceLevel.ERASURE_ATTESTED,
        }:
            raise ValueError("absence_verified requires verified deletion assurance")
        if (
            stage is RevocationStage.VERIFIED
            and operation is not EffectOperation.DELETE
        ):
            raise ValueError(
                "absence_verified requires a delete operation postcondition"
            )
        if stage is RevocationStage.RETAINED_ISOLATED and (
            outcome is not VerificationOutcome.RETAINED_ISOLATED
            or assurance is not AssuranceLevel.RETAINED_ISOLATED
            or operation is not EffectOperation.ISOLATE
        ):
            raise ValueError(
                "retained_isolated requires an isolate operation with matching "
                "outcome and assurance"
            )
        if assurance in {
            AssuranceLevel.QUERY_VERIFIED,
            AssuranceLevel.ERASURE_ATTESTED,
        } and (
            outcome is not VerificationOutcome.ABSENT
            or stage is not RevocationStage.VERIFIED
            or operation is not EffectOperation.DELETE
        ):
            raise ValueError(
                "successful deletion assurance requires a verified absent "
                "delete operation"
            )
        if assurance is AssuranceLevel.RETAINED_ISOLATED and (
            stage is not RevocationStage.RETAINED_ISOLATED
            or outcome is not VerificationOutcome.RETAINED_ISOLATED
            or operation is not EffectOperation.ISOLATE
        ):
            raise ValueError(
                "retained-isolated assurance requires a terminal isolate postcondition"
            )
        if assurance is AssuranceLevel.ACKNOWLEDGED and stage not in {
            RevocationStage.DISPATCHED,
            RevocationStage.ACKNOWLEDGED,
        }:
            raise ValueError(
                "acknowledged assurance requires a dispatch or acknowledgement stage"
            )
        if outcome in {
            VerificationOutcome.PRESENT,
            VerificationOutcome.WRONG_ACL,
            VerificationOutcome.UNSUPPORTED,
            VerificationOutcome.TIMEOUT,
            VerificationOutcome.TRANSPORT_FAILURE,
        } and stage in {
            RevocationStage.VERIFIED,
            RevocationStage.RETAINED_ISOLATED,
            RevocationStage.CLOSED,
        }:
            raise ValueError("failed verification cannot use a terminal success stage")

    def to_dict(self) -> dict[str, object]:
        value: dict[str, object] = {
            "schema_version": self.schema_version,
            "receipt_id": self.receipt_id,
            "case_id": self.case_id,
            "obligation_id": self.obligation_id,
            "attempt": self.attempt,
            "source_digest": self.source_digest,
            "target_provider_id": self.target_provider_id,
            "target_instance_digest": self.target_instance_digest,
            "target_locator_digest": self.target_locator_digest,
            "operation_kind": self.operation_kind,
            "reason": self.reason,
            "policy_decision": self.policy_decision,
            "stage": self.stage,
            "assurance_level": self.assurance_level,
            "request_fingerprint": self.request_fingerprint,
            "operation_id": self.operation_id,
            "affected_count": self.affected_count,
            "capability_digest": self.capability_digest,
            "consistency_contract": self.consistency_contract,
            "verifier_kind": self.verifier_kind,
            "observed_outcome": self.observed_outcome,
            "attempted_at": _utc_text(self.attempted_at),
            "verified_at": (
                _utc_text(self.verified_at) if self.verified_at is not None else None
            ),
            "safe_error_code": self.safe_error_code,
            "previous_receipt_digest": self.previous_receipt_digest,
        }
        value.update(self.extensions)
        return value

    def __synor_audit_metadata__(self) -> dict[str, object]:
        """Project receipt evidence without its provider operation identifier."""

        return {
            "schema_version": self.schema_version,
            "kind": "revocation_receipt",
            "receipt_id": self.receipt_id,
            "case_id": self.case_id,
            "obligation_id": self.obligation_id,
            "attempt": self.attempt,
            "source_digest": self.source_digest,
            "target_provider_id": self.target_provider_id,
            "target_instance_digest": self.target_instance_digest,
            "target_locator_digest": self.target_locator_digest,
            "operation_kind": self.operation_kind,
            "reason": self.reason,
            "policy_decision": self.policy_decision,
            "stage": self.stage,
            "assurance_level": self.assurance_level,
            "request_fingerprint": self.request_fingerprint,
            "operation_id_present": self.operation_id is not None,
            "affected_count": self.affected_count,
            "capability_digest": self.capability_digest,
            "consistency_contract": self.consistency_contract,
            "verifier_kind": self.verifier_kind,
            "observed_outcome": self.observed_outcome,
            "attempted_at": _utc_text(self.attempted_at),
            "verified_at": (
                _utc_text(self.verified_at) if self.verified_at is not None else None
            ),
            "safe_error_code": self.safe_error_code,
            "previous_receipt_digest": self.previous_receipt_digest,
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.to_dict())

    def evidence_digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "RevocationReceipt":
        _require_schema(value)
        known = frozenset(
            {
                "schema_version",
                "receipt_id",
                "case_id",
                "obligation_id",
                "attempt",
                "source_digest",
                "target_provider_id",
                "target_instance_digest",
                "target_locator_digest",
                "operation_kind",
                "reason",
                "policy_decision",
                "stage",
                "assurance_level",
                "request_fingerprint",
                "operation_id",
                "affected_count",
                "capability_digest",
                "consistency_contract",
                "verifier_kind",
                "observed_outcome",
                "attempted_at",
                "verified_at",
                "safe_error_code",
                "previous_receipt_digest",
            }
        )
        return cls(
            schema_version=_integer_field(value, "schema_version"),
            receipt_id=_string_field(value, "receipt_id"),
            case_id=_string_field(value, "case_id"),
            obligation_id=_string_field(value, "obligation_id"),
            attempt=_integer_field(value, "attempt"),
            source_digest=_string_field(value, "source_digest"),
            target_provider_id=_string_field(value, "target_provider_id"),
            target_instance_digest=_string_field(value, "target_instance_digest"),
            target_locator_digest=_string_field(value, "target_locator_digest"),
            operation_kind=_string_field(value, "operation_kind"),
            reason=_string_field(value, "reason"),
            policy_decision=_string_field(value, "policy_decision"),
            stage=_string_field(value, "stage"),
            assurance_level=_string_field(value, "assurance_level"),
            request_fingerprint=_string_field(value, "request_fingerprint"),
            operation_id=_optional_string_field(value, "operation_id"),
            affected_count=(
                _integer_field(value, "affected_count")
                if value.get("affected_count") is not None
                else None
            ),
            capability_digest=_string_field(value, "capability_digest"),
            consistency_contract=_string_field(value, "consistency_contract"),
            verifier_kind=_string_field(value, "verifier_kind"),
            observed_outcome=_string_field(value, "observed_outcome"),
            attempted_at=_datetime_from_text("attempted_at", value["attempted_at"]),
            verified_at=(
                _datetime_from_text("verified_at", value["verified_at"])
                if value.get("verified_at") is not None
                else None
            ),
            safe_error_code=_optional_string_field(value, "safe_error_code"),
            previous_receipt_digest=_optional_string_field(
                value, "previous_receipt_digest"
            ),
            extensions=_extras(value, known, frozenset()),
        )


def _require_schema(value: Mapping[str, object]) -> None:
    schema = value.get("schema_version")
    if not isinstance(schema, int) or isinstance(schema, bool):
        raise RevocationSchemaError("missing revocation schema version")
    if schema != _SCHEMA_VERSION:
        raise RevocationSchemaError("unsupported revocation schema major version")


def json_bytes(value: Mapping[str, object]) -> bytes:
    """Serialize an internal record deterministically."""

    return _canonical_json(value)


def json_mapping(payload: bytes) -> Mapping[str, object]:
    """Decode a JSON object while rejecting non-object roots."""

    try:
        value: Any = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        value = None
        decode_failed = True
    else:
        decode_failed = False
    if decode_failed:
        # Decoder exceptions can retain raw bytes and document fragments.
        # Raise outside the handler to keep both cause and context empty.
        raise RevocationSchemaError("corrupt revocation JSON") from None
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise RevocationSchemaError("revocation record must be a JSON object")
    return cast(Mapping[str, object], value)
