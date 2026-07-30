from __future__ import annotations

import traceback

import pytest

from synor.state import MemoryStateStore
from synor._internal.revocation_ledger import (
    RevocationLedgerCorruption,
    StateStoreRevocationLedger,
)
from synor._internal.revocation_model import (
    RevocationCase,
    RevocationReceipt,
    RevocationSchemaError,
    json_bytes,
    json_mapping,
)
from synor._internal.suppression import (
    StateStoreSuppressionIndex,
    SuppressionCorruptionError,
)

from ._fixtures import SOURCE_DIGEST, TENANT_DIGEST, make_case, make_receipt


_PLANTED_SECRET = "patient-alice@example.com-token-sk-planted-secret"


def _assert_secret_is_not_reachable(
    error: BaseException,
    secret: str = _PLANTED_SECRET,
) -> None:
    assert secret not in str(error)
    assert error.__cause__ is None
    assert error.__context__ is None
    assert secret not in "".join(traceback.format_exception(error))


def test_malformed_json_schema_error_severs_decoder_exception() -> None:
    payload = f'{{"private":"{_PLANTED_SECRET}"'.encode()

    with pytest.raises(RevocationSchemaError) as raised:
        json_mapping(payload)

    _assert_secret_is_not_reachable(raised.value)


def test_unsupported_schema_does_not_echo_raw_version() -> None:
    planted_numeric_secret = 8675309

    with pytest.raises(RevocationSchemaError) as raised:
        RevocationCase.from_dict({"schema_version": planted_numeric_secret})

    _assert_secret_is_not_reachable(raised.value, str(planted_numeric_secret))


def test_model_enum_validation_severs_rejected_value_exception() -> None:
    case_value = make_case().to_dict()
    case_value["reason"] = _PLANTED_SECRET

    with pytest.raises(ValueError, match="controlled source event") as case_error:
        RevocationCase.from_dict(case_value)

    _assert_secret_is_not_reachable(case_error.value)

    receipt_value = make_receipt(
        make_case(),
        attempt=0,
        previous_receipt_digest=None,
    ).to_dict()
    receipt_value["observed_outcome"] = _PLANTED_SECRET

    with pytest.raises(ValueError, match="known controlled") as receipt_error:
        RevocationReceipt.from_dict(receipt_value)

    _assert_secret_is_not_reachable(receipt_error.value)


@pytest.mark.asyncio
async def test_corrupt_case_summary_redacts_secret_from_public_error() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    case = make_case()
    await ledger.append_case(case)
    case_value = case.to_dict()
    case_value["reason"] = _PLANTED_SECRET
    await store.put(
        f"revocation/v1/cases/{case.case_id}.json",
        json_bytes(case_value),
    )

    with pytest.raises(
        RevocationLedgerCorruption,
        match="case summary is corrupt",
    ) as raised:
        await ledger.get_case(case.case_id)

    _assert_secret_is_not_reachable(raised.value)


@pytest.mark.asyncio
async def test_corrupt_event_timestamp_redacts_secret_from_public_error() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    case = make_case()
    event = await ledger.append_case(case)
    event_value = event.to_dict()
    event_value["occurred_at"] = _PLANTED_SECRET
    await store.put(
        (
            f"revocation/v1/events/{case.case_id}/"
            f"{event.sequence:020d}-{event.event_id}.json"
        ),
        json_bytes(event_value),
    )

    with pytest.raises(
        RevocationLedgerCorruption,
        match="invalid ledger timestamp",
    ) as raised:
        await ledger.get_case(case.case_id)

    _assert_secret_is_not_reachable(raised.value)


@pytest.mark.asyncio
async def test_corrupt_receipt_enum_redacts_secret_from_public_error() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    case = make_case()
    receipt = make_receipt(case, attempt=0, previous_receipt_digest=None)
    await ledger.append_case(case)
    await ledger.append_receipt(receipt)
    receipt_value = receipt.to_dict()
    receipt_value["observed_outcome"] = _PLANTED_SECRET
    await store.put(
        f"revocation/v1/receipts/{case.case_id}/{receipt.receipt_id}.json",
        json_bytes(receipt_value),
    )

    with pytest.raises(
        RevocationLedgerCorruption,
        match="immutable receipt is corrupt",
    ) as raised:
        await ledger.list_receipts(case.case_id)

    _assert_secret_is_not_reachable(raised.value)


@pytest.mark.asyncio
async def test_corrupt_receipt_head_redacts_decoder_context() -> None:
    store = MemoryStateStore()
    ledger = StateStoreRevocationLedger(store)
    case = make_case()
    receipt = make_receipt(case, attempt=0, previous_receipt_digest=None)
    await ledger.append_case(case)
    await ledger.append_receipt(receipt)
    await store.put(
        f"revocation/v1/receipt_heads/{case.case_id}.json",
        f'{{"private":"{_PLANTED_SECRET}"'.encode(),
    )

    with pytest.raises(
        RevocationLedgerCorruption,
        match="receipt head is corrupt",
    ) as raised:
        await ledger.list_receipts(case.case_id)

    _assert_secret_is_not_reachable(raised.value)


@pytest.mark.asyncio
async def test_corrupt_suppression_timestamp_redacts_parser_context() -> None:
    store = MemoryStateStore()
    index = StateStoreSuppressionIndex(store)
    await index.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id="policy-a",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )
    key = f"revocation/v1/suppression/{SOURCE_DIGEST}.json"
    payload = await store.get(key)
    assert payload is not None
    value = dict(json_mapping(payload))
    value["observed_at"] = _PLANTED_SECRET
    await store.put(key, json_bytes(value))

    with pytest.raises(
        SuppressionCorruptionError,
        match="stored suppression state is corrupt",
    ) as raised:
        await index.get(SOURCE_DIGEST)

    _assert_secret_is_not_reachable(raised.value)


@pytest.mark.asyncio
async def test_corrupt_durable_serving_fence_redacts_decoder_context() -> None:
    store = MemoryStateStore()
    index = StateStoreSuppressionIndex(store)
    await index.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id="policy-a",
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )
    await store.put(
        f"revocation/v1/serving_fences/{SOURCE_DIGEST}.json",
        f'{{"private":"{_PLANTED_SECRET}"'.encode(),
    )

    with pytest.raises(
        SuppressionCorruptionError,
        match="stored serving fence is corrupt",
    ) as raised:
        await index.get(SOURCE_DIGEST)

    _assert_secret_is_not_reachable(raised.value)
