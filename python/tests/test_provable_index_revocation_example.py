from __future__ import annotations

import pathlib
import sys

import pytest

_REPOSITORY_ROOT = pathlib.Path(__file__).parents[2]
sys.path.insert(0, str(_REPOSITORY_ROOT))

from examples.provable_index_revocation.fake_source import (  # noqa: E402
    CONTENT_SENTINEL,
    CONTROL_CONTENT_SENTINEL,
    PRINCIPAL_ALPHA,
    PRINCIPAL_BETA,
)
from examples.provable_index_revocation.main import run_fake_scenario  # noqa: E402


def _assert_control_evidence_is_private(root: pathlib.Path) -> None:
    sensitive = (
        CONTENT_SENTINEL,
        CONTROL_CONTENT_SENTINEL,
        PRINCIPAL_ALPHA,
        PRINCIPAL_BETA,
    )
    evidence = b"".join(path.read_bytes() for path in root.rglob("*") if path.is_file())
    for sentinel in sensitive:
        assert sentinel.encode() not in evidence


@pytest.mark.asyncio
async def test_flagship_fake_scenario_is_repeatable_and_privacy_safe(
    tmp_path: pathlib.Path,
) -> None:
    state_root = tmp_path / "state"

    first = await run_fake_scenario(state_root)
    second = await run_fake_scenario(state_root)

    assert first.case_id == second.case_id
    assert first.partial_case_id == second.partial_case_id
    assert first.controlled_passes == second.controlled_passes == 2
    assert first.startup_ready and second.startup_ready
    assert first.startup_safe_error_code is None
    assert second.startup_safe_error_code is None
    assert first.runtime_status == second.runtime_status == "degraded"
    assert (
        first.execution_guarantee
        == second.execution_guarantee
        == "strict_revocation_control_v1"
    )
    assert first.content_unchanged and second.content_unchanged
    assert first.initial_alpha_results > 0
    assert first.initial_beta_results > 0
    assert first.suppressed_results == 0
    assert first.suppressed_scored == 0
    assert first.stale_verification_stage == "failed"
    assert first.final_stage == "closed"
    assert first.receipt_count == 2
    assert first.partial_stage == "blocked"
    assert first.partial_deleted_points == 0
    assert first.restored_raw_points > 0
    assert first.restored_guarded_results == 0
    assert first.unaffected_results > 0
    assert first.effective_deletes == 1
    assert first.evidence_keys > 0
    assert second.initial_alpha_results == 0
    assert second.initial_beta_results > 0
    assert second.suppressed_results == 0
    assert second.suppressed_scored == 0
    assert second.stale_verification_stage == "failed"
    assert second.final_stage == "closed"
    assert second.receipt_count == 2
    assert second.partial_stage == "blocked"
    assert second.partial_deleted_points == 0
    assert second.restored_raw_points > 0
    assert second.restored_guarded_results == 0
    assert second.unaffected_results > 0
    assert second.effective_deletes == 0
    assert second.evidence_keys > first.evidence_keys

    _assert_control_evidence_is_private(state_root)
