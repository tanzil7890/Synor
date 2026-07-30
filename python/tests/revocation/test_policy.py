from __future__ import annotations

import dataclasses
import math

import pytest

from synor._internal.revocation_model import (
    RevocationPolicyDecision,
    SourceEventKind,
    TargetRevocationCapabilities,
)
from synor._internal.revocation_policy import (
    RevocationCapabilityError,
    RevocationPolicy,
)


def _capabilities(**overrides: bool) -> TargetRevocationCapabilities:
    values: dict[str, bool] = {
        "atomic_serving_suppression": True,
        "exact_id_delete": True,
        "source_id_bulk_delete": False,
        "query_time_acl_filter": True,
        "tenant_isolation": True,
        "synchronous_acknowledgement": True,
        "consistency_fence": True,
        "negative_read_verification": True,
        "external_enumeration": True,
        "legal_hold_isolation": True,
        "physical_erasure_attestation": False,
    }
    values.update(overrides)
    return TargetRevocationCapabilities(
        atomic_serving_suppression=values["atomic_serving_suppression"],
        exact_id_delete=values["exact_id_delete"],
        source_id_bulk_delete=values["source_id_bulk_delete"],
        query_time_acl_filter=values["query_time_acl_filter"],
        tenant_isolation=values["tenant_isolation"],
        synchronous_acknowledgement=values["synchronous_acknowledgement"],
        consistency_fence=values["consistency_fence"],
        negative_read_verification=values["negative_read_verification"],
        external_enumeration=values["external_enumeration"],
        legal_hold_isolation=values["legal_hold_isolation"],
        physical_erasure_attestation=values["physical_erasure_attestation"],
    )


def test_strict_capability_validation_blocks_before_apply() -> None:
    policy = RevocationPolicy.strict_query_verified()
    policy.validate_capabilities(_capabilities())

    with pytest.raises(RevocationCapabilityError) as raised:
        policy.validate_capabilities(
            _capabilities(
                negative_read_verification=False,
                exact_id_delete=False,
                source_id_bulk_delete=False,
            )
        )
    assert raised.value.missing_capabilities == (
        "negative_read_verification",
        "exact_id_delete|source_id_bulk_delete",
    )


def test_capability_flags_require_exact_booleans() -> None:
    with pytest.raises(TypeError, match="negative_read_verification"):
        dataclasses.replace(
            _capabilities(),
            negative_read_verification="yes",  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_policy_rejects_non_finite_deadlines(value: float) -> None:
    with pytest.raises(ValueError, match="finite"):
        dataclasses.replace(
            RevocationPolicy.strict_query_verified(),
            verification_timeout_seconds=value,
        )


def test_policy_never_maps_non_revocation_observation_to_destroy() -> None:
    policy = RevocationPolicy.strict_query_verified()

    assert policy.decide(SourceEventKind.PRESENT) is None
    assert policy.decide(SourceEventKind.CONTENT_CHANGED) is None
    assert policy.decide(SourceEventKind.SCAN_INCOMPLETE) is None
    assert (
        policy.decide(SourceEventKind.ACL_CHANGED) is RevocationPolicyDecision.RESTRICT
    )
    with pytest.raises(TypeError):
        policy.decide("present")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        policy.validate_capabilities(
            _capabilities(),
            decision="destroy",  # type: ignore[arg-type]
        )
