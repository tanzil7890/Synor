from __future__ import annotations

import asyncio
import dataclasses
import pathlib
from collections.abc import Callable

import pytest

from synor.state import (
    EncryptedStateStore,
    FileStateStore,
    MemoryStateStore,
    StateStore,
)
from synor._internal.revocation_ledger import (
    RevocationLedgerConflict,
    RevocationLedgerCorruption,
    StateStoreRevocationLedger,
)
from synor._internal.revocation_model import (
    AssuranceLevel,
    EffectOperation,
    RevocationStage,
    SafeRevocationErrorCode,
    VerificationOutcome,
    json_bytes,
    make_action_id,
    make_receipt_id,
    transition_case,
)

from ._fixtures import (
    PROOF_CONTRACT_DIGEST,
    TARGET_INSTANCE_DIGEST,
    TARGET_LOCATOR_DIGEST,
    make_case,
    make_receipt,
)


class _FailOnceStore:
    def __init__(
        self,
        store: StateStore,
        should_fail: Callable[[str], bool],
    ) -> None:
        self.store = store
        self.should_fail = should_fail
        self.failed = False

    async def get(self, key: str) -> bytes | None:
        return await self.store.get(key)

    async def put(self, key: str, value: bytes) -> None:
        if not self.failed and self.should_fail(key):
            self.failed = True
            raise OSError("planted metadata store interruption")
        await self.store.put(key, value)

    async def delete(self, key: str) -> bool:
        return await self.store.delete(key)

    async def list(self, prefix: str = "") -> tuple[str, ...]:
        return await self.store.list(prefix)


@pytest.mark.asyncio
async def test_event_first_case_round_trip_and_idempotency() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    second_adapter = StateStoreRevocationLedger(store)
    observed = make_case()
    first, duplicate = await asyncio.gather(
        ledger.append_case(observed),
        second_adapter.append_case(observed),
    )

    assert first == duplicate
    assert await ledger.get_case(observed.case_id) == observed
    assert len(await store.list(f"revocation/v1/events/{observed.case_id}/")) == 1

    suppressed = transition_case(observed, RevocationStage.SUPPRESSED)
    planned = transition_case(suppressed, RevocationStage.PLANNED)
    await ledger.append_case(suppressed)
    await ledger.append_case(planned)
    assert await ledger.get_case(observed.case_id) == planned
    await ledger.append_case(observed)
    assert await ledger.get_case(observed.case_id) == planned
    assert [case.stage for case in await ledger.list_cases()] == [
        RevocationStage.PLANNED
    ]


@pytest.mark.asyncio
async def test_interrupted_summary_write_is_repaired_from_event_stream() -> None:
    underlying = MemoryStateStore()
    failing = _FailOnceStore(
        underlying,
        lambda key: key.startswith("revocation/v1/cases/"),
    )
    ledger = StateStoreRevocationLedger(failing)
    observed = make_case()

    with pytest.raises(OSError, match="interruption"):
        await ledger.append_case(observed)

    assert await underlying.list(f"revocation/v1/events/{observed.case_id}/")
    assert await underlying.get(f"revocation/v1/cases/{observed.case_id}.json") is None
    recovered = StateStoreRevocationLedger(underlying)
    report = await recovered.repair()
    assert report.cases_rebuilt == 1
    assert report.events_validated == 1
    assert report.receipt_heads_rebuilt == 0
    assert await recovered.get_case(observed.case_id) == observed


@pytest.mark.asyncio
async def test_retry_after_interrupted_summary_write_is_idempotent() -> None:
    underlying = MemoryStateStore()
    failing = _FailOnceStore(
        underlying,
        lambda key: key.startswith("revocation/v1/cases/"),
    )
    ledger = StateStoreRevocationLedger(failing)
    observed = make_case()

    with pytest.raises(OSError):
        await ledger.append_case(observed)
    event = await ledger.append_case(observed)

    assert event.case == observed
    assert len(await underlying.list(f"revocation/v1/events/{observed.case_id}/")) == 1
    assert await ledger.get_case(observed.case_id) == observed


@pytest.mark.asyncio
async def test_illegal_or_divergent_event_version_is_rejected() -> None:
    ledger = StateStoreRevocationLedger(MemoryStateStore())
    observed = make_case()
    await ledger.append_case(observed)

    illegal = transition_case(observed, RevocationStage.SUPPRESSED)
    illegal = transition_case(illegal, RevocationStage.PLANNED)
    with pytest.raises(RevocationLedgerConflict):
        await ledger.append_case(illegal)


@pytest.mark.asyncio
async def test_receipt_hash_chain_detects_missing_evidence() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    case = make_case()
    await ledger.append_case(case)
    first = make_receipt(case, attempt=0, previous_receipt_digest=None)
    second = make_receipt(
        case,
        attempt=1,
        previous_receipt_digest=first.evidence_digest(),
    )
    await ledger.append_receipt(first)
    await ledger.append_receipt(second)

    assert await ledger.list_receipts(case.case_id) == (first, second)
    await ledger.append_receipt(second)
    assert len(await ledger.list_receipts(case.case_id)) == 2

    first_key = f"revocation/v1/receipts/{case.case_id}/{first.receipt_id}.json"
    assert await store.delete(first_key)
    with pytest.raises(RevocationLedgerCorruption):
        await ledger.list_receipts(case.case_id)


@pytest.mark.asyncio
async def test_receipt_head_detects_deleted_tail() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    case = make_case()
    await ledger.append_case(case)
    first = make_receipt(case, attempt=0, previous_receipt_digest=None)
    second = make_receipt(
        case,
        attempt=1,
        previous_receipt_digest=first.evidence_digest(),
    )
    await ledger.append_receipt(first)
    await ledger.append_receipt(second)
    assert await store.delete(
        f"revocation/v1/receipts/{case.case_id}/{second.receipt_id}.json"
    )

    with pytest.raises(RevocationLedgerCorruption, match="receipt"):
        await ledger.list_receipts(case.case_id)
    with pytest.raises(RevocationLedgerCorruption, match="receipt"):
        await ledger.repair()


@pytest.mark.asyncio
async def test_interrupted_receipt_head_write_is_repaired() -> None:
    underlying = MemoryStateStore()
    failing = _FailOnceStore(
        underlying,
        lambda key: key.startswith("revocation/v1/receipt_heads/"),
    )
    ledger = StateStoreRevocationLedger(failing)
    case = make_case()
    await ledger.append_case(case)
    receipt = make_receipt(case, attempt=0, previous_receipt_digest=None)

    with pytest.raises(OSError, match="interruption"):
        await ledger.append_receipt(receipt)

    recovered = StateStoreRevocationLedger(underlying)
    report = await recovered.repair()
    assert report.receipt_heads_rebuilt == 1
    assert await recovered.list_receipts(case.case_id) == (receipt,)


@pytest.mark.asyncio
async def test_divergent_receipt_head_is_not_silently_repaired() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    case = make_case()
    await ledger.append_case(case)
    first = make_receipt(case, attempt=0, previous_receipt_digest=None)
    second = make_receipt(
        case,
        attempt=1,
        previous_receipt_digest=first.evidence_digest(),
    )
    await ledger.append_receipt(first)
    await ledger.append_receipt(second)
    await store.put(
        f"revocation/v1/receipt_heads/{case.case_id}.json",
        b'{"count":1,"schema_version":1,"tip_digest":"' + b"0" * 64 + b'"}',
    )

    with pytest.raises(RevocationLedgerCorruption, match="diverge"):
        await ledger.repair()


@pytest.mark.asyncio
async def test_orphan_receipt_head_is_corruption() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    case = make_case()
    await store.put(
        f"revocation/v1/receipt_heads/{case.case_id}.json",
        b'{"count":1,"schema_version":1,"tip_digest":"' + b"0" * 64 + b'"}',
    )

    with pytest.raises(RevocationLedgerCorruption, match="no immutable"):
        await ledger.repair()


@pytest.mark.asyncio
async def test_receipt_rejects_wrong_hash_chain_tip() -> None:
    ledger = StateStoreRevocationLedger(MemoryStateStore())
    case = make_case()
    await ledger.append_case(case)
    first = make_receipt(case, attempt=0, previous_receipt_digest=None)
    await ledger.append_receipt(first)
    wrong = make_receipt(
        case,
        attempt=1,
        previous_receipt_digest="0" * 64,
    )

    with pytest.raises(RevocationLedgerConflict):
        await ledger.append_receipt(wrong)


@pytest.mark.asyncio
async def test_close_requires_latest_terminal_receipt_for_every_obligation() -> None:
    ledger = StateStoreRevocationLedger(MemoryStateStore())
    case = make_case()
    await ledger.append_case(case)
    for stage in (
        RevocationStage.SUPPRESSED,
        RevocationStage.PLANNED,
        RevocationStage.DISPATCHED,
        RevocationStage.ACKNOWLEDGED,
        RevocationStage.FENCE_REACHED,
    ):
        case = transition_case(case, stage)
        await ledger.append_case(case)
    verified = transition_case(case, RevocationStage.VERIFIED)

    with pytest.raises(RevocationLedgerConflict, match="terminal evidence"):
        await ledger.append_case(verified)

    success = make_receipt(case, attempt=0, previous_receipt_digest=None)
    await ledger.append_receipt(success)
    await ledger.append_case(verified)
    closed = transition_case(verified, RevocationStage.CLOSED)
    later_failure = dataclasses.replace(
        make_receipt(
            verified,
            attempt=1,
            previous_receipt_digest=success.evidence_digest(),
        ),
        receipt_id=make_receipt_id(
            success.obligation_id,
            RevocationStage.FAILED,
            VerificationOutcome.PRESENT,
            1,
        ),
        stage=RevocationStage.FAILED.value,
        assurance_level=AssuranceLevel.UNVERIFIED.value,
        observed_outcome=VerificationOutcome.PRESENT.value,
        verified_at=None,
        safe_error_code="target.present",
    )
    await ledger.append_receipt(later_failure)
    with pytest.raises(RevocationLedgerConflict, match="terminal evidence"):
        await ledger.append_case(closed)

    final_success = make_receipt(
        verified,
        attempt=2,
        previous_receipt_digest=later_failure.evidence_digest(),
    )
    await ledger.append_receipt(final_success)
    await ledger.append_case(closed)
    assert (await ledger.get_case(case.case_id)) == closed


@pytest.mark.asyncio
async def test_success_state_read_and_repair_require_terminal_evidence() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    case = make_case()
    await ledger.append_case(case)
    for stage in (
        RevocationStage.SUPPRESSED,
        RevocationStage.PLANNED,
        RevocationStage.DISPATCHED,
        RevocationStage.ACKNOWLEDGED,
        RevocationStage.FENCE_REACHED,
    ):
        case = transition_case(case, stage)
        await ledger.append_case(case)
    receipt = make_receipt(case, attempt=0, previous_receipt_digest=None)
    await ledger.append_receipt(receipt)
    verified = transition_case(case, RevocationStage.VERIFIED)
    await ledger.append_case(verified)

    assert await store.delete(
        f"revocation/v1/receipts/{case.case_id}/{receipt.receipt_id}.json"
    )
    assert await store.delete(f"revocation/v1/receipt_heads/{case.case_id}.json")

    with pytest.raises(RevocationLedgerCorruption, match="terminal evidence"):
        await ledger.get_case(case.case_id)
    with pytest.raises(RevocationLedgerCorruption, match="terminal obligation"):
        await ledger.repair()


@pytest.mark.asyncio
async def test_receipt_obligation_must_match_target_operation() -> None:
    ledger = StateStoreRevocationLedger(MemoryStateStore())
    case = make_case()
    await ledger.append_case(case)
    receipt = make_receipt(case, attempt=0, previous_receipt_digest=None)
    other_obligation = make_action_id(
        case.case_id,
        "qdrant",
        TARGET_INSTANCE_DIGEST,
        TARGET_LOCATOR_DIGEST,
        EffectOperation.RESTRICT,
        PROOF_CONTRACT_DIGEST,
    )

    with pytest.raises(ValueError, match="obligation_id"):
        dataclasses.replace(receipt, obligation_id=other_obligation)


def test_receipt_terminal_evidence_must_match_operation_and_decision() -> None:
    case = make_case()
    receipt = make_receipt(case, attempt=0, previous_receipt_digest=None)

    with pytest.raises(ValueError, match="isolate operation"):
        dataclasses.replace(
            receipt,
            receipt_id=make_receipt_id(
                receipt.obligation_id,
                RevocationStage.RETAINED_ISOLATED,
                VerificationOutcome.RETAINED_ISOLATED,
                receipt.attempt,
            ),
            stage=RevocationStage.RETAINED_ISOLATED.value,
            assurance_level=AssuranceLevel.RETAINED_ISOLATED.value,
            observed_outcome=VerificationOutcome.RETAINED_ISOLATED.value,
        )

    isolate_obligation = make_action_id(
        case.case_id,
        "qdrant",
        TARGET_INSTANCE_DIGEST,
        TARGET_LOCATOR_DIGEST,
        EffectOperation.ISOLATE,
        PROOF_CONTRACT_DIGEST,
    )
    with pytest.raises(ValueError, match="delete operation"):
        dataclasses.replace(
            receipt,
            receipt_id=make_receipt_id(
                isolate_obligation,
                RevocationStage.VERIFIED,
                VerificationOutcome.ABSENT,
                receipt.attempt,
            ),
            obligation_id=isolate_obligation,
            operation_kind=EffectOperation.ISOLATE.value,
            policy_decision="preserve_on_hold",
        )

    with pytest.raises(ValueError, match="policy decision"):
        dataclasses.replace(
            receipt,
            policy_decision="preserve_on_hold",
        )

    with pytest.raises(ValueError, match="terminal success"):
        dataclasses.replace(
            receipt,
            safe_error_code=SafeRevocationErrorCode.TARGET_PRESENT.value,
        )

    failed_receipt_id = make_receipt_id(
        receipt.obligation_id,
        RevocationStage.ACKNOWLEDGED,
        VerificationOutcome.PRESENT,
        receipt.attempt,
    )
    for wrong_code in (
        SafeRevocationErrorCode.TARGET_TIMEOUT.value,
        None,
    ):
        with pytest.raises(ValueError, match="canonical safe_error_code"):
            dataclasses.replace(
                receipt,
                receipt_id=failed_receipt_id,
                stage=RevocationStage.ACKNOWLEDGED.value,
                assurance_level=AssuranceLevel.ACKNOWLEDGED.value,
                observed_outcome=VerificationOutcome.PRESENT.value,
                verified_at=None,
                safe_error_code=wrong_code,
            )


@pytest.mark.asyncio
async def test_corrupt_event_blocks_repair() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    case = make_case()
    event = await ledger.append_case(case)
    event_key = (
        f"revocation/v1/events/{case.case_id}/"
        f"{event.sequence:020d}-{event.event_id}.json"
    )
    await store.put(event_key, b'{"schema_version":2}')

    with pytest.raises(RevocationLedgerCorruption):
        await ledger.repair()


@pytest.mark.asyncio
async def test_corrupt_summary_is_rebuilt_from_valid_events() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    case = make_case()
    await ledger.append_case(case)
    await store.put(
        f"revocation/v1/cases/{case.case_id}.json",
        b'{"schema_version":true}',
    )

    report = await ledger.repair()

    assert report.cases_rebuilt == 1
    assert await ledger.get_case(case.case_id) == case


@pytest.mark.asyncio
async def test_structurally_valid_stale_summary_is_rejected_and_rebuilt() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    observed = make_case()
    suppressed = transition_case(observed, RevocationStage.SUPPRESSED)
    planned = transition_case(suppressed, RevocationStage.PLANNED)
    await ledger.append_case(observed)
    await ledger.append_case(suppressed)
    await ledger.append_case(planned)
    await store.put(
        f"revocation/v1/cases/{observed.case_id}.json",
        json_bytes(suppressed.to_dict()),
    )

    with pytest.raises(RevocationLedgerCorruption, match="event-stream tip"):
        await ledger.get_case(observed.case_id)

    report = await ledger.repair()
    assert report.cases_rebuilt == 1
    assert await ledger.get_case(observed.case_id) == planned


@pytest.mark.asyncio
async def test_renamed_immutable_event_key_blocks_repair() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    case = make_case()
    event = await ledger.append_case(case)
    event_key = (
        f"revocation/v1/events/{case.case_id}/"
        f"{event.sequence:020d}-{event.event_id}.json"
    )
    payload = await store.get(event_key)
    assert payload is not None
    assert await store.delete(event_key)
    await store.put(
        f"revocation/v1/events/{case.case_id}/00000000000000000001-renamed.json",
        payload,
    )

    with pytest.raises(RevocationLedgerCorruption):
        await ledger.repair()


@pytest.mark.asyncio
async def test_ledger_operates_over_plaintext_and_encrypted_stores(
    tmp_path: pathlib.Path,
) -> None:
    stores: tuple[StateStore, ...] = (
        FileStateStore(tmp_path / "plain"),
        EncryptedStateStore(
            FileStateStore(tmp_path / "encrypted"),
            bytes(reversed(range(32))),
        ),
    )
    for store in stores:
        ledger = StateStoreRevocationLedger(store)
        case = make_case()
        await ledger.append_case(case)
        assert await ledger.get_case(case.case_id) == case
