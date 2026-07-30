from __future__ import annotations

import pytest

import synor as syn


@pytest.mark.asyncio
async def test_quarantine_requires_explicit_one_way_review() -> None:
    store = syn.MemoryStateStore()
    repository = syn.QuarantineRepository(store)
    case = await repository.create(
        reason="run_failure",
        app_name="PatientPipeline",
        app_target="./main.py",
        run_id="run-1",
        error=RuntimeError("patient payload must never be stored"),
    )

    assert case.status is syn.QuarantineStatus.OPEN
    payload = await store.get(f"quarantine/{case.case_id}.json")
    assert payload is not None
    assert b"patient payload must never be stored" not in payload
    assert len(await repository.list(status=syn.QuarantineStatus.OPEN)) == 1

    reviewed = await repository.review(
        case.case_id,
        status=syn.QuarantineStatus.APPROVED,
        note="metadata reviewed",
    )
    assert reviewed.status is syn.QuarantineStatus.APPROVED
    with pytest.raises(ValueError):
        await repository.review(
            case.case_id,
            status=syn.QuarantineStatus.REJECTED,
        )
