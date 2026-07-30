from __future__ import annotations

import dataclasses
import pathlib

import pytest

import synor as syn
from synor import replay
from synor.connectors import localfs


@pytest.mark.asyncio
async def test_runtime_records_provenance_and_verifies_replay(
    tmp_path: pathlib.Path,
) -> None:
    env = syn.Environment(syn.Settings(db_path=tmp_path / "native"))
    output = tmp_path / "out.txt"
    app_source = tmp_path / "main.py"
    app_source.write_text("# replay source\n", encoding="utf-8")

    @syn.fn(memo=True)
    def build() -> None:
        localfs.declare_file(
            output,
            b"artifact bytes",
            create_parent_dirs=True,
        )

    app = syn.App(syn.AppConfig(name="Phase3App", environment=env), build)
    store = syn.MemoryStateStore()
    runtime = syn.SynorRuntime(
        policy=syn.EgressPolicy.offline(),
        audit_dir=tmp_path / "runs",
        state_store=store,
    )

    plan = await runtime.plan(app, app_target=str(app_source))
    assert plan.replay_path is not None
    assert plan.replay_path.is_file()
    envelope = replay.load_replay_envelope(plan.replay_path)
    verification = await runtime.replay(app, envelope, app_target=str(app_source))
    assert verification.matched
    assert verification.dependencies_matched
    assert verification.policy_matched
    assert verification.runtime_matched

    policy_mismatch = replay.verify_replay(
        envelope,
        app_target=str(app_source),
        changes=plan.planned_changes,
        policy={
            "egress": syn.EgressPolicy().to_dict(),
            "pii": syn.PIIPolicy().to_dict(),
        },
    )
    assert not policy_mismatch.matched
    assert not policy_mismatch.policy_matched

    runtime_mismatch = replay.verify_replay(
        dataclasses.replace(envelope, python_version="0.0.0"),
        app_target=str(app_source),
        changes=plan.planned_changes,
        policy={
            "egress": syn.EgressPolicy.offline().to_dict(),
            "pii": syn.PIIPolicy().to_dict(),
        },
    )
    assert not runtime_mismatch.matched
    assert not runtime_mismatch.runtime_matched

    dependency_mismatch = replay.verify_replay(
        dataclasses.replace(envelope, dependency_digest="0" * 64),
        app_target=str(app_source),
        changes=plan.planned_changes,
        policy={
            "egress": syn.EgressPolicy.offline().to_dict(),
            "pii": syn.PIIPolicy().to_dict(),
        },
    )
    assert not dependency_mismatch.matched
    assert not dependency_mismatch.dependencies_matched

    app_source.write_text("# replay source changed\n", encoding="utf-8")
    mismatch = await runtime.replay(app, envelope, app_target=str(app_source))
    assert not mismatch.matched
    assert not mismatch.source_matched
    assert mismatch.actions_matched

    applied = await runtime.run(app, app_target=str(app_source))
    assert output.read_bytes() == b"artifact bytes"
    assert applied.status is syn.ExecutionStatus.SUCCEEDED
    assert applied.execution_guarantee is syn.ExecutionGuarantee.COMPATIBILITY
    assert applied.revocations is not None
    assert applied.revocations.open == 0
    assert len(applied.provenance) == 1
    artifact = applied.provenance[0]
    assert artifact.app_name == "Phase3App"
    assert artifact.owner_component_path
    assert await store.get(
        f"runs/{applied.run_id}/provenance/{artifact.artifact_id}.json"
    )
    provenance_text = (applied.manifest_path.parent / "provenance.jsonl").read_text(
        encoding="utf-8"
    )
    assert "artifact bytes" not in provenance_text


@pytest.mark.asyncio
async def test_runtime_quarantines_pii_preview_without_applying(
    tmp_path: pathlib.Path,
) -> None:
    env = syn.Environment(syn.Settings(db_path=tmp_path / "native"))
    output = tmp_path / "pii.txt"

    @syn.fn
    def build() -> None:
        localfs.declare_file(
            output,
            "alice@example.com",
            create_parent_dirs=True,
        )

    app = syn.App(syn.AppConfig(name="PIIApp", environment=env), build)
    store = syn.MemoryStateStore()
    runtime = syn.SynorRuntime(
        policy=syn.EgressPolicy.offline(),
        pii_policy=syn.PIIPolicy(action=syn.PIIAction.QUARANTINE),
        audit_dir=tmp_path / "runs",
        state_store=store,
    )

    with pytest.raises(syn.PIIQuarantineRequired):
        await runtime.run(app, app_target=str(tmp_path / "main.py"))
    assert not output.exists()
    cases = await syn.QuarantineRepository(store).list()
    assert len(cases) == 1
    assert cases[0].reason == "pii_policy"
