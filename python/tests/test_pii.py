from __future__ import annotations

import json

import pytest

import synor as syn
from synor import audit


def test_pii_detection_never_exposes_matched_value() -> None:
    findings = syn.find_pii(
        "Email alice@example.com, SSN 123-45-6789, card 4111 1111 1111 1111."
    )
    assert {item.category for item in findings} == {
        syn.PIICategory.EMAIL,
        syn.PIICategory.US_SSN,
        syn.PIICategory.PAYMENT_CARD,
    }
    encoded = json.dumps(
        [
            {
                "category": item.category.value,
                "digest": item.evidence_digest,
            }
            for item in findings
        ]
    )
    assert "alice@example.com" not in encoded
    assert "123-45-6789" not in encoded


def test_pii_policy_redacts_denies_and_quarantines() -> None:
    value = "Contact alice@example.com"
    redacted = syn.PIIPolicy(action=syn.PIIAction.REDACT).enforce_text(value)
    assert redacted == "Contact [REDACTED_EMAIL]"

    with pytest.raises(syn.PIIViolation):
        syn.PIIPolicy(action=syn.PIIAction.DENY).enforce_text(value)

    with pytest.raises(syn.PIIQuarantineRequired):
        syn.PIIPolicy(action=syn.PIIAction.QUARANTINE).enforce_text(value)


def test_empty_pii_category_set_disables_detection() -> None:
    policy = syn.PIIPolicy(
        action=syn.PIIAction.DENY,
        categories=frozenset(),
    )
    assert policy.enforce_text("Contact alice@example.com") == (
        "Contact alice@example.com"
    )


def test_audit_redacts_known_pii_even_when_policy_allows() -> None:
    safe = audit.redact_metadata(
        {
            "email": "alice@example.com",
            "note": "Call 415-555-0198",
        }
    )
    encoded = json.dumps(safe)
    assert "alice@example.com" not in encoded
    assert "415-555-0198" not in encoded
    assert "[REDACTED_EMAIL]" in encoded
