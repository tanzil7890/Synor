"""Structured PII detection and policy enforcement."""

from __future__ import annotations

import contextlib as _contextlib
import contextvars as _contextvars
import dataclasses as _dataclasses
import enum as _enum
import hashlib as _hashlib
import hmac as _hmac
import os as _os
import re as _re
import typing as _typing

__all__ = [
    "PIIAction",
    "PIICategory",
    "PIIFinding",
    "PIIPolicy",
    "PIIQuarantineRequired",
    "PIIViolation",
    "current_pii_policy",
    "enforce_pii",
    "find_pii",
    "pii_policy_from_env",
    "pii_scope",
    "redact_known_pii",
]


class PIICategory(str, _enum.Enum):
    """Built-in structured identifier categories."""

    EMAIL = "email"
    PHONE = "phone"
    US_SSN = "us_ssn"
    PAYMENT_CARD = "payment_card"


class PIIAction(str, _enum.Enum):
    """Action taken when enabled PII categories are found."""

    ALLOW = "allow"
    REDACT = "redact"
    DENY = "deny"
    QUARANTINE = "quarantine"


@_dataclasses.dataclass(frozen=True, slots=True)
class PIIFinding:
    """Metadata-only PII finding; the matched value is deliberately omitted."""

    category: PIICategory
    start: int
    end: int
    evidence_digest: str


class PIIViolation(PermissionError):
    """Raised when a PII policy denies a value."""

    def __init__(self, findings: tuple[PIIFinding, ...]) -> None:
        self.findings = findings
        categories = ", ".join(sorted({item.category.value for item in findings}))
        super().__init__(f"PII policy denied {len(findings)} finding(s): {categories}")


class PIIQuarantineRequired(PIIViolation):
    """Raised when data requires manual review before it can proceed."""


_EMAIL_PATTERN = _re.compile(
    r"(?<![\w.+-])[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,63}(?![\w.-])",
    _re.IGNORECASE,
)
_PHONE_PATTERN = _re.compile(
    r"(?<!\d)(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]\d{3}[\s.-]\d{4}(?!\d)"
)
_SSN_PATTERN = _re.compile(
    r"(?<!\d)(?!000|666|9\d\d)\d{3}[- ](?!00)\d{2}[- ](?!0000)\d{4}(?!\d)"
)
_CARD_CANDIDATE_PATTERN = _re.compile(r"(?<!\d)(?:\d[ -]?){13,19}(?!\d)")
_EVIDENCE_KEY = _os.urandom(32)
_REDACTION = {
    PIICategory.EMAIL: "[REDACTED_EMAIL]",
    PIICategory.PHONE: "[REDACTED_PHONE]",
    PIICategory.US_SSN: "[REDACTED_SSN]",
    PIICategory.PAYMENT_CARD: "[REDACTED_PAYMENT_CARD]",
}


def _luhn_valid(candidate: str) -> bool:
    digits = [int(char) for char in candidate if char.isdigit()]
    if not 13 <= len(digits) <= 19 or len(set(digits)) == 1:
        return False
    checksum = 0
    parity = len(digits) % 2
    for index, digit in enumerate(digits):
        value = digit
        if index % 2 == parity:
            value *= 2
            if value > 9:
                value -= 9
        checksum += value
    return checksum % 10 == 0


def _finding(
    category: PIICategory,
    text: str,
    start: int,
    end: int,
) -> PIIFinding:
    digest = _hmac.new(
        _EVIDENCE_KEY,
        text[start:end].encode("utf-8"),
        _hashlib.sha256,
    ).hexdigest()
    return PIIFinding(
        category=category,
        start=start,
        end=end,
        evidence_digest=digest,
    )


def find_pii(
    text: str,
    *,
    categories: frozenset[PIICategory] | None = None,
) -> tuple[PIIFinding, ...]:
    """Find common structured identifiers without retaining matched values."""

    enabled = frozenset(PIICategory) if categories is None else categories
    findings: list[PIIFinding] = []
    patterns = (
        (PIICategory.EMAIL, _EMAIL_PATTERN),
        (PIICategory.PHONE, _PHONE_PATTERN),
        (PIICategory.US_SSN, _SSN_PATTERN),
    )
    for category, pattern in patterns:
        if category not in enabled:
            continue
        findings.extend(
            _finding(category, text, match.start(), match.end())
            for match in pattern.finditer(text)
        )
    if PIICategory.PAYMENT_CARD in enabled:
        for match in _CARD_CANDIDATE_PATTERN.finditer(text):
            if _luhn_valid(match.group()):
                findings.append(
                    _finding(
                        PIICategory.PAYMENT_CARD,
                        text,
                        match.start(),
                        match.end(),
                    )
                )
    findings.sort(key=lambda item: (item.start, item.end, item.category.value))
    return tuple(findings)


def _redact_findings(text: str, findings: tuple[PIIFinding, ...]) -> str:
    result = text
    for item in reversed(findings):
        result = result[: item.start] + _REDACTION[item.category] + result[item.end :]
    return result


@_dataclasses.dataclass(frozen=True, slots=True)
class PIIPolicy:
    """PII policy applied to policy-aware operations and controlled previews."""

    action: PIIAction = PIIAction.ALLOW
    categories: frozenset[PIICategory] = _dataclasses.field(
        default_factory=lambda: frozenset(PIICategory)
    )

    def enforce_text(self, text: str) -> str:
        """Return an allowed/redacted value or raise a metadata-only violation."""

        if self.action is PIIAction.ALLOW:
            return text
        findings = find_pii(text, categories=self.categories)
        if not findings:
            return text
        if self.action is PIIAction.REDACT:
            return _redact_findings(text, findings)
        if self.action is PIIAction.QUARANTINE:
            raise PIIQuarantineRequired(findings)
        raise PIIViolation(findings)

    def to_dict(self) -> dict[str, _typing.Any]:
        """Return a manifest-safe policy representation."""

        return {
            "action": self.action.value,
            "categories": sorted(item.value for item in self.categories),
        }


_policy_var: _contextvars.ContextVar[PIIPolicy] = _contextvars.ContextVar(
    "synor_pii_policy",
    default=PIIPolicy(),
)


def current_pii_policy() -> PIIPolicy:
    """Return the active PII policy."""

    return _policy_var.get()


@_contextlib.contextmanager
def pii_scope(policy: PIIPolicy) -> _typing.Iterator[None]:
    """Apply a PII policy to the current async/thread context."""

    token = _policy_var.set(policy)
    try:
        yield
    finally:
        _policy_var.reset(token)


def enforce_pii(value: _typing.Any, *, policy: PIIPolicy | None = None) -> _typing.Any:
    """Enforce PII policy recursively for ordinary JSON-like values."""

    active = policy or current_pii_policy()
    if isinstance(value, str):
        return active.enforce_text(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            return value
        enforced = active.enforce_text(text).encode("utf-8")
        if isinstance(value, bytearray):
            return bytearray(enforced)
        if isinstance(value, memoryview):
            return memoryview(enforced)
        return enforced
    if _dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: enforce_pii(getattr(value, field.name), policy=active)
            for field in _dataclasses.fields(value)
        }
    if isinstance(value, tuple) and hasattr(value, "_asdict"):
        return {
            key: enforce_pii(item, policy=active)
            for key, item in value._asdict().items()
        }
    if isinstance(value, list):
        return [enforce_pii(item, policy=active) for item in value]
    if isinstance(value, tuple):
        return tuple(enforce_pii(item, policy=active) for item in value)
    if isinstance(value, _typing.Mapping):
        return {key: enforce_pii(item, policy=active) for key, item in value.items()}
    return value


def redact_known_pii(text: str) -> str:
    """Always redact recognized PII before text enters audit evidence."""

    return _redact_findings(text, find_pii(text))


def pii_policy_from_env() -> PIIPolicy:
    """Build a PII policy from ``SYNOR_PII_ACTION`` and categories."""

    action_text = _os.getenv("SYNOR_PII_ACTION", PIIAction.ALLOW.value)
    try:
        action = PIIAction(action_text.strip().lower())
    except ValueError as error:
        allowed = ", ".join(item.value for item in PIIAction)
        raise ValueError(f"SYNOR_PII_ACTION must be one of: {allowed}") from error
    raw_categories = _os.getenv("SYNOR_PII_CATEGORIES")
    if raw_categories is None:
        categories = frozenset(PIICategory)
    else:
        try:
            categories = frozenset(
                PIICategory(item.strip().lower())
                for item in raw_categories.split(",")
                if item.strip()
            )
        except ValueError as error:
            allowed = ", ".join(item.value for item in PIICategory)
            raise ValueError(
                f"SYNOR_PII_CATEGORIES entries must be one of: {allowed}"
            ) from error
    return PIIPolicy(action=action, categories=categories)
