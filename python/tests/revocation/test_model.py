from __future__ import annotations

import dataclasses
import datetime
import hashlib

import pytest

import synor as syn
from synor._internal.revocation_model import (
    AccessEffect,
    AccessRule,
    AccessSnapshot,
    EffectOperation,
    GovernedSourceItem,
    InvalidRevocationTransition,
    RevocationPolicyDecision,
    RevocationSchemaError,
    RevocationStage,
    SnapshotResult,
    SourceEventKind,
    SourceIdentity,
    VerificationOutcome,
    canonical_access_digest,
    make_action_id,
    make_case_id,
    make_observation_id,
    make_receipt_id,
    transition_case,
)
from synor._internal.typing import ABSENT
from tests import common

from ._fixtures import (
    IDENTITY,
    NOW,
    PROOF_CONTRACT_DIGEST,
    TARGET_LOCATOR_DIGEST,
    make_case,
)


_governed_item: GovernedSourceItem[str] | None = None
_governed_calls = 0


@syn.task(cache=True)
def _consume_governed_item(item: GovernedSourceItem[str]) -> str:
    global _governed_calls
    _governed_calls += 1
    return item.identity.evidence_digest()


@syn.task
def _governed_memo_app() -> None:
    if _governed_item is None:
        raise RuntimeError("test governed item is not configured")
    _consume_governed_item(_governed_item)


def test_source_identity_is_length_delimited_and_rename_independent() -> None:
    left = SourceIdentity("ab", "c", "d")
    right = SourceIdentity("a", "bc", "d")
    duplicate_name_a = "Quarterly plan"
    duplicate_name_b = "Quarterly plan"

    assert left.canonical_bytes() != right.canonical_bytes()
    assert left.evidence_digest() != right.evidence_digest()
    assert duplicate_name_a == duplicate_name_b
    assert (
        IDENTITY.component_key()
        == SourceIdentity("connector-a", "scope-a", "provider-item-17").component_key()
    )


def test_acl_digest_is_semantic_order_independent_and_preserves_denies() -> None:
    expiry = NOW + datetime.timedelta(days=1)
    direct_grant = AccessRule(
        AccessEffect.GRANT,
        "user",
        "opaque-principal-1",
        "reader",
        expires_at=expiry,
    )
    inherited_deny = AccessRule(
        AccessEffect.DENY,
        "group",
        "opaque-group-9",
        "reader",
        inherited_from="opaque-parent-2",
    )
    forward = canonical_access_digest(
        tenant_id="tenant-opaque",
        policy_id="policy-4",
        policy_revision="revision-2",
        rules=(direct_grant, inherited_deny),
    )
    reversed_digest = canonical_access_digest(
        tenant_id="tenant-opaque",
        policy_id="policy-4",
        policy_revision="revision-2",
        rules=(inherited_deny, direct_grant),
    )
    grant_instead = canonical_access_digest(
        tenant_id="tenant-opaque",
        policy_id="policy-4",
        policy_revision="revision-2",
        rules=(
            direct_grant,
            AccessRule(
                AccessEffect.GRANT,
                "group",
                "opaque-group-9",
                "reader",
                inherited_from="opaque-parent-2",
            ),
        ),
    )

    assert forward == reversed_digest
    assert forward != grant_instead


@pytest.mark.asyncio
async def test_acl_only_change_invalidates_governed_memo_state() -> None:
    digest_v1 = hashlib.sha256(b"policy-v1").hexdigest()
    digest_v2 = hashlib.sha256(b"policy-v2").hexdigest()
    access_v1 = AccessSnapshot(
        tenant_id="tenant-a",
        policy_id="policy-a",
        policy_revision="policy-v1",
        policy_digest=digest_v1,
        group_graph_revision="groups-v1",
    )
    access_v2 = AccessSnapshot(
        tenant_id="tenant-a",
        policy_id="policy-a",
        policy_revision="policy-v2",
        policy_digest=digest_v2,
        group_graph_revision="groups-v1",
    )
    observation_v1 = make_observation_id(
        IDENTITY, "content-v1", SourceEventKind.PRESENT, access_v1
    )
    item_v1 = GovernedSourceItem(
        identity=IDENTITY,
        resource={"display_name": "before"},
        source_revision="content-v1",
        content_fingerprint=b"same-content",
        access=access_v1,
        event=SourceEventKind.PRESENT,
        observation_id=observation_v1,
    )
    first = await item_v1.__synor_memo_state__(ABSENT)
    unchanged = await item_v1.__synor_memo_state__(first.state)
    item_v2 = GovernedSourceItem(
        identity=IDENTITY,
        resource={"display_name": "after rename"},
        source_revision="content-v1",
        content_fingerprint=b"same-content",
        access=access_v2,
        event=SourceEventKind.ACL_CHANGED,
        observation_id=make_observation_id(
            IDENTITY, "content-v1", SourceEventKind.ACL_CHANGED, access_v2
        ),
    )
    changed = await item_v2.__synor_memo_state__(first.state)

    assert unchanged.memo_valid
    assert not changed.memo_valid
    assert changed.state.content_fingerprint == first.state.content_fingerprint
    assert changed.state.policy_revision == "policy-v2"


def test_governed_memo_state_round_trips_through_engine() -> None:
    global _governed_calls, _governed_item
    _governed_calls = 0
    access_v1 = AccessSnapshot(
        tenant_id="tenant-a",
        policy_id="policy-a",
        policy_revision="policy-v1",
        policy_digest=hashlib.sha256(b"policy-v1").hexdigest(),
        group_graph_revision="groups-v1",
    )
    access_v2 = AccessSnapshot(
        tenant_id="tenant-a",
        policy_id="policy-a",
        policy_revision="policy-v2",
        policy_digest=hashlib.sha256(b"policy-v2").hexdigest(),
        group_graph_revision="groups-v1",
    )
    _governed_item = GovernedSourceItem(
        identity=IDENTITY,
        resource="unchanged bytes",
        source_revision="content-v1",
        content_fingerprint=b"same-content",
        access=access_v1,
        event=SourceEventKind.PRESENT,
        observation_id=make_observation_id(
            IDENTITY,
            "content-v1",
            SourceEventKind.PRESENT,
            access_v1,
        ),
    )
    app = syn.App(
        syn.AppConfig(
            name="test_governed_memo_state_round_trip",
            environment=common.create_test_env(
                __file__, suffix="governed_memo_state_round_trip"
            ),
        ),
        _governed_memo_app,
    )

    app.update_blocking()
    app.update_blocking()
    assert _governed_calls == 1

    _governed_item = GovernedSourceItem(
        identity=IDENTITY,
        resource="unchanged bytes",
        source_revision="content-v1",
        content_fingerprint=b"same-content",
        access=access_v2,
        event=SourceEventKind.ACL_CHANGED,
        observation_id=make_observation_id(
            IDENTITY,
            "content-v1",
            SourceEventKind.ACL_CHANGED,
            access_v2,
        ),
    )
    app.update_blocking()
    app.update_blocking()
    assert _governed_calls == 2


def test_case_transitions_are_immutable_and_illegal_skips_fail() -> None:
    observed = make_case()
    suppressed = transition_case(observed, RevocationStage.SUPPRESSED)

    assert observed.stage is RevocationStage.OBSERVED
    assert observed.version == 1
    assert suppressed.stage is RevocationStage.SUPPRESSED
    assert suppressed.version == 2
    with pytest.raises(InvalidRevocationTransition):
        transition_case(observed, RevocationStage.CLOSED)
    with pytest.raises(InvalidRevocationTransition):
        transition_case(suppressed, RevocationStage.CLOSED)


def test_terminal_case_stage_must_match_policy_decision() -> None:
    destroy = make_case()
    with pytest.raises(InvalidRevocationTransition, match="preserve_on_hold"):
        transition_case(destroy, RevocationStage.RETAINED_ISOLATED)
    with pytest.raises(ValueError, match="preserve_on_hold"):
        dataclasses.replace(destroy, stage=RevocationStage.RETAINED_ISOLATED)

    preserve = dataclasses.replace(
        destroy,
        policy_decision=RevocationPolicyDecision.PRESERVE_ON_HOLD,
    )
    for stage in (
        RevocationStage.SUPPRESSED,
        RevocationStage.PLANNED,
        RevocationStage.DISPATCHED,
        RevocationStage.ACKNOWLEDGED,
    ):
        preserve = transition_case(preserve, stage)
    with pytest.raises(InvalidRevocationTransition, match="destroy or restrict"):
        transition_case(preserve, RevocationStage.VERIFIED)
    with pytest.raises(ValueError, match="destroy or restrict"):
        dataclasses.replace(preserve, stage=RevocationStage.VERIFIED)


def test_deterministic_ids_are_domain_separated() -> None:
    observation_id = make_observation_id(
        IDENTITY,
        "revision-1",
        SourceEventKind.SOURCE_DELETED,
    )
    case_id = make_case_id(
        IDENTITY,
        "revision-1",
        SourceEventKind.SOURCE_DELETED,
        RevocationPolicyDecision.DESTROY,
        observation_id,
    )
    action_id = make_action_id(
        case_id,
        "qdrant",
        hashlib.sha256(b"target-instance").hexdigest(),
        TARGET_LOCATOR_DIGEST,
        EffectOperation.DELETE,
        PROOF_CONTRACT_DIGEST,
    )
    receipt_id = make_receipt_id(
        action_id,
        RevocationStage.VERIFIED,
        VerificationOutcome.ABSENT,
        0,
    )

    assert case_id == make_case_id(
        IDENTITY,
        "revision-1",
        SourceEventKind.SOURCE_DELETED,
        RevocationPolicyDecision.DESTROY,
        observation_id,
    )
    assert len({case_id, action_id, receipt_id}) == 3
    assert case_id.startswith("case1_")
    assert action_id.startswith("action1_")
    assert receipt_id.startswith("receipt1_")


def test_acl_only_observations_create_distinct_case_ids() -> None:
    access_v2 = AccessSnapshot(
        tenant_id="tenant-a",
        policy_id="policy-a",
        policy_revision="policy-v2",
        policy_digest=hashlib.sha256(b"policy-v2").hexdigest(),
        group_graph_revision="groups-v1",
        inherited_from=("parent-a", "parent-b"),
    )
    access_v3 = AccessSnapshot(
        tenant_id="tenant-a",
        policy_id="policy-a",
        policy_revision="policy-v3",
        policy_digest=hashlib.sha256(b"policy-v3").hexdigest(),
        group_graph_revision="groups-v2",
        inherited_from=("parent-a\x1fparent-b",),
    )
    observation_v2 = make_observation_id(
        IDENTITY,
        "unchanged-content",
        SourceEventKind.ACL_CHANGED,
        access_v2,
    )
    observation_v3 = make_observation_id(
        IDENTITY,
        "unchanged-content",
        SourceEventKind.ACL_CHANGED,
        access_v3,
    )

    assert observation_v2 != observation_v3
    assert make_case_id(
        IDENTITY,
        "unchanged-content",
        SourceEventKind.ACL_CHANGED,
        RevocationPolicyDecision.RESTRICT,
        observation_v2,
    ) != make_case_id(
        IDENTITY,
        "unchanged-content",
        SourceEventKind.ACL_CHANGED,
        RevocationPolicyDecision.RESTRICT,
        observation_v3,
    )


def test_only_complete_snapshot_authorizes_cleanup_and_checkpoint() -> None:
    complete = SnapshotResult(
        connector_instance_id="connector-a",
        source_scope_id="scope-a",
        epoch="epoch-1",
        cursor_before="cursor-1",
        cursor_after="cursor-2",
        status="complete",
        item_count=2,
    )
    partial = SnapshotResult(
        connector_instance_id="connector-a",
        source_scope_id="scope-a",
        epoch="epoch-2",
        cursor_before="cursor-1",
        cursor_after=None,
        status="partial",
        item_count=1,
        inaccessible_scope_digests=(hashlib.sha256(b"scope-gap").hexdigest(),),
    )

    assert complete.authorizes_missing_item_cleanup
    assert complete.authorizes_checkpoint_advance
    assert not partial.authorizes_missing_item_cleanup
    assert not partial.authorizes_checkpoint_advance


def test_schema_rejects_unknown_major_and_preserves_safe_extension() -> None:
    value = make_case().to_dict()
    value["x_safe_retry_class"] = "bounded"
    value["future_raw_field"] = "alice@example.com"
    restored = type(make_case()).from_dict(value)

    assert restored.extensions == (("x_safe_retry_class", "bounded"),)
    assert "future_raw_field" not in restored.to_dict()
    value["schema_version"] = 2
    with pytest.raises(RevocationSchemaError):
        type(make_case()).from_dict(value)
