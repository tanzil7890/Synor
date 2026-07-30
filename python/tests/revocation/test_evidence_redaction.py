from __future__ import annotations

import dataclasses
import hashlib

import pytest

from synor._internal.revocation_model import (
    AssuranceLevel,
    RevocationReceipt,
    RevocationStage,
    SafeRevocationErrorCode,
    VerificationOutcome,
    make_receipt_id,
)

from ._fixtures import make_case, make_receipt


def test_receipt_serialization_contains_only_metadata_safe_evidence() -> None:
    planted_source_phrase = "patient says the blue orchid is secret"
    planted_email = "alice@example.com"
    planted_token = "sk-planted-secret-token"
    planted_vector = "[0.123456789, -9.87654321]"
    planted_remote_error = "403 body: alice@example.com sk-planted-secret-token"
    sensitive_values = (
        planted_source_phrase,
        planted_email,
        planted_token,
        planted_vector,
        planted_remote_error,
    )
    # Sensitive operational inputs are represented only by one-way digests or
    # controlled codes in general evidence.
    _request_digest = hashlib.sha256("\x00".join(sensitive_values).encode()).hexdigest()
    receipt = dataclasses.replace(
        make_receipt(make_case(), attempt=0, previous_receipt_digest=None),
        request_fingerprint=_request_digest,
    )

    serialized = receipt.canonical_bytes()

    for planted in sensitive_values:
        assert planted.encode() not in serialized
    assert RevocationReceipt.from_dict(receipt.to_dict()) == receipt


def test_raw_remote_error_cannot_be_used_as_safe_error_code() -> None:
    with pytest.raises(ValueError, match="safe_error_code"):
        dataclasses.replace(
            make_receipt(make_case(), attempt=0, previous_receipt_digest=None),
            safe_error_code="403 response body alice@example.com token=secret",
        )
    with pytest.raises(ValueError, match="safe_error_code"):
        dataclasses.replace(
            make_receipt(make_case(), attempt=0, previous_receipt_digest=None),
            safe_error_code="sk-planted-secret-token",
        )


def test_receipt_rejects_unsafe_future_extension_strings() -> None:
    with pytest.raises(ValueError, match="extension"):
        dataclasses.replace(
            make_receipt(make_case(), attempt=0, previous_receipt_digest=None),
            extensions=(("x_safe_note", "alice@example.com"),),
        )


def test_receipt_rejects_incoherent_success_claim() -> None:
    receipt = make_receipt(make_case(), attempt=0, previous_receipt_digest=None)

    with pytest.raises(ValueError, match="terminal success receipt"):
        dataclasses.replace(
            receipt,
            receipt_id=make_receipt_id(
                receipt.obligation_id,
                RevocationStage.VERIFIED,
                VerificationOutcome.PRESENT,
                receipt.attempt,
            ),
            observed_outcome=VerificationOutcome.PRESENT.value,
            safe_error_code=SafeRevocationErrorCode.TARGET_PRESENT.value,
        )
    with pytest.raises(ValueError, match="verified assurance"):
        dataclasses.replace(receipt, verified_at=None)
    with pytest.raises(ValueError, match="known controlled"):
        dataclasses.replace(receipt, assurance_level="trust_me")
    with pytest.raises(ValueError, match="successful deletion"):
        dataclasses.replace(
            receipt,
            receipt_id=make_receipt_id(
                receipt.obligation_id,
                RevocationStage.FAILED,
                VerificationOutcome.WRONG_ACL,
                receipt.attempt,
            ),
            stage=RevocationStage.FAILED.value,
            assurance_level=AssuranceLevel.QUERY_VERIFIED.value,
            observed_outcome=VerificationOutcome.WRONG_ACL.value,
            safe_error_code=SafeRevocationErrorCode.TARGET_WRONG_ACL.value,
        )
