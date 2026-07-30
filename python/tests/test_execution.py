from __future__ import annotations

import datetime
import hashlib
import pathlib

import pytest

import synor as syn
from synor import audit
from synor import governance
from synor import revocation
from synor.connectors import localfs


class _UnavailableStateStore:
    async def get(self, key: str) -> bytes | None:
        raise RuntimeError("backend-path-MUST-NOT-ESCAPE")

    async def put(self, key: str, value: bytes) -> None:
        raise RuntimeError("backend-path-MUST-NOT-ESCAPE")

    async def delete(self, key: str) -> bool:
        raise RuntimeError("backend-path-MUST-NOT-ESCAPE")

    async def list(self, prefix: str = "") -> tuple[str, ...]:
        raise RuntimeError("backend-path-MUST-NOT-ESCAPE")


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _revocation_request(
    *,
    provider_available: bool = True,
) -> revocation.RevocationRequest:
    now = datetime.datetime.now(datetime.timezone.utc)
    identity = governance.SourceIdentity(
        connector_instance_id="execution-test-source",
        source_scope_id="tenant-a-scope",
        item_id="stable-item-a",
    )
    access = governance.AccessSnapshot(
        tenant_id="tenant-a",
        policy_id="policy-a",
        policy_revision="policy-v2",
        policy_digest=_digest("policy-a-v2"),
        group_graph_revision="groups-v2",
    )
    capabilities = revocation.TargetRevocationCapabilities(
        atomic_serving_suppression=True,
        exact_id_delete=True,
        source_id_bulk_delete=True,
        query_time_acl_filter=True,
        tenant_isolation=True,
        synchronous_acknowledgement=True,
        consistency_fence=True,
        negative_read_verification=True,
        external_enumeration=True,
        legal_hold_isolation=True,
        physical_erasure_attestation=False,
    )
    obligation = revocation.TargetObligation(
        target_provider_id="synthetic-index",
        target_instance_digest=_digest("execution-test-target"),
        target_locator_digest=_digest("execution-test-locator"),
        operation_kind=revocation.EffectOperation.DELETE,
        proof_capabilities=capabilities,
        capabilities=capabilities if provider_available else None,
        verifier_kind="exact-id-query",
        consistency_contract="strong-read",
    )
    observation_id = governance.make_observation_id(
        identity,
        "content-v1",
        governance.SourceEventKind.ACL_CHANGED,
        access,
        observation_generation="change-v2",
    )
    return revocation.RevocationRequest(
        identity=identity,
        observation_id=observation_id,
        source_revision="content-v1",
        access=access,
        observation_generation="change-v2",
        tenant_digest=revocation.make_tenant_digest(access.tenant_id),
        policy_id=access.policy_id,
        policy_revision=access.policy_revision,
        policy_digest=access.policy_digest,
        group_graph_revision=access.group_graph_revision,
        reason=revocation.SourceEventKind.ACL_CHANGED,
        policy_decision=revocation.RevocationPolicyDecision.RESTRICT,
        suppression_generation=2,
        observed_at=now,
        suppress_by=now,
        verify_by=now + datetime.timedelta(minutes=5),
        obligations=(obligation,),
    )


async def _record_absent_outcome(
    controller: revocation.RevocationController,
    request: revocation.RevocationRequest,
) -> revocation.RevocationCase:
    (descriptor,) = await controller.descriptors_for(request.case_id)
    await controller.notify_synor_precommit(request.case_id, descriptor.action_id)
    await controller.notify_target_effect_applied(
        request.case_id,
        descriptor.action_id,
    )
    await controller.mark_target_applied(request.case_id, descriptor.action_id)
    await controller.mark_acknowledged(request.case_id, descriptor.action_id)
    return await controller.record_outcomes(
        request.case_id,
        (
            revocation.TargetVerificationOutcome(
                action_id=descriptor.action_id,
                operation=descriptor.operation_kind,
                source_digest=descriptor.source_digest,
                source_generation=descriptor.source_generation,
                target_locator_digest=descriptor.target_locator_digest,
                status=revocation.VerificationOutcome.ABSENT,
                attempt_count=1,
                operation_id="execution-test-operation",
            ),
        ),
        attempt=1,
        attempted_at=request.observed_at + datetime.timedelta(seconds=1),
    )


@pytest.mark.asyncio
async def test_runtime_plan_run_and_explain(tmp_path: pathlib.Path) -> None:
    env = syn.Environment(syn.Settings(db_path=tmp_path / "state"))
    output_path = tmp_path / "out.txt"

    @syn.fn(memo=True)
    def build() -> None:
        localfs.declare_file(
            output_path,
            "local result\n",
            create_parent_dirs=True,
        )

    app = syn.App(syn.AppConfig(name="ControlledLocalApp", environment=env), build)
    runtime = syn.SynorRuntime(
        policy=syn.EgressPolicy.offline(),
        audit_dir=tmp_path / "runs",
    )

    plan = await runtime.plan(app, app_target="./main.py")
    assert plan.mode is syn.RunMode.PLAN
    assert len(plan.planned_changes) == 1
    assert not output_path.exists()
    assert plan.manifest_path.is_file()

    run = await runtime.run(app, app_target="./main.py")
    assert run.mode is syn.RunMode.APPLY
    assert output_path.read_text() == "local result\n"
    assert run.manifest_path.is_file()

    settled = await runtime.plan(app)
    assert settled.planned_changes == ()

    explanation = await runtime.explain(app)
    assert explanation.app_name == "ControlledLocalApp"
    assert explanation.stable_path_count >= 1
    assert explanation.target_state_count == 1
    assert explanation.latest_run is not None


@pytest.mark.asyncio
async def test_runtime_records_failed_policy_run(tmp_path: pathlib.Path) -> None:
    env = syn.Environment(syn.Settings(db_path=tmp_path / "state"))

    @syn.fn
    def build() -> None:
        syn.authorize_egress(
            syn.EgressRequest(destination="example.test", purpose="test")
        )

    app = syn.App(syn.AppConfig(name="DeniedApp", environment=env), build)
    runtime = syn.SynorRuntime(
        policy=syn.EgressPolicy.offline(),
        audit_dir=tmp_path / "runs",
    )

    with pytest.raises(syn.PolicyViolation):
        await runtime.run(app)

    from synor import audit

    manifest = audit.latest_run_manifest(
        audit_root=tmp_path / "runs",
        app_name="DeniedApp",
    )
    assert manifest is not None
    assert manifest["status"] == "failed"
    assert manifest["error_type"].endswith("PolicyViolation")


def test_strict_runtime_requires_control_state() -> None:
    with pytest.raises(ValueError, match="state_store"):
        syn.SynorRuntime(revocation_policy=syn.RevocationPolicy.strict_query_verified())


@pytest.mark.asyncio
async def test_strict_runtime_reports_open_revocations_without_plain_success(
    tmp_path: pathlib.Path,
) -> None:
    store = syn.MemoryStateStore()
    policy = syn.RevocationPolicy.strict_query_verified()
    controller = revocation.RevocationController(
        state_store=store,
        policy=policy,
    )
    case = await controller.begin_case(_revocation_request())
    assert case.stage is revocation.RevocationStage.PLANNED

    env = syn.Environment(syn.Settings(db_path=tmp_path / "state"))

    @syn.fn
    def build() -> None:
        return None

    app = syn.App(syn.AppConfig(name="StrictOpenCase", environment=env), build)
    runtime = syn.SynorRuntime(
        state_store=store,
        revocation_policy=policy,
        audit_dir=tmp_path / "runs",
    )

    report = await runtime.run(app)

    assert report.status is syn.ExecutionStatus.SUCCEEDED_WITH_OPEN_REVOCATIONS
    assert report.execution_guarantee is (
        syn.ExecutionGuarantee.STRICT_REVOCATION_CONTROL_V1
    )
    assert report.revocations is not None
    assert report.revocations.suppressed == 1
    manifest = __import__("json").loads(report.manifest_path.read_text())
    assert manifest["status"] == "succeeded_with_open_revocations"
    assert manifest["revocations"]["open"] == 1
    assert manifest["execution_guarantee"] == "strict_revocation_control_v1"


@pytest.mark.asyncio
async def test_strict_runtime_reports_blocked_cases_as_degraded(
    tmp_path: pathlib.Path,
) -> None:
    store = syn.MemoryStateStore()
    policy = syn.RevocationPolicy.strict_query_verified()
    controller = revocation.RevocationController(
        state_store=store,
        policy=policy,
    )
    case = await controller.begin_case(_revocation_request(provider_available=False))
    assert case.stage is revocation.RevocationStage.BLOCKED

    env = syn.Environment(syn.Settings(db_path=tmp_path / "state"))

    @syn.fn
    def build() -> None:
        return None

    app = syn.App(syn.AppConfig(name="StrictBlockedCase", environment=env), build)
    report = await syn.SynorRuntime(
        state_store=store,
        revocation_policy=policy,
        audit_dir=tmp_path / "runs",
    ).run(app)

    assert report.status is syn.ExecutionStatus.DEGRADED
    assert report.revocations is not None
    assert report.revocations.blocked == 1


@pytest.mark.asyncio
async def test_corrupt_revocation_state_stops_strict_apply(
    tmp_path: pathlib.Path,
) -> None:
    store = syn.MemoryStateStore()
    await store.put(
        "revocation/v1/cases/case1_" + "a" * 64 + ".json",
        b"{not-json",
    )
    output = tmp_path / "must-not-exist.txt"
    env = syn.Environment(syn.Settings(db_path=tmp_path / "state"))

    @syn.fn
    def build() -> None:
        localfs.declare_file(
            output,
            "unsafe",
            create_parent_dirs=True,
        )

    app = syn.App(syn.AppConfig(name="StrictCorruptState", environment=env), build)
    runtime = syn.SynorRuntime(
        state_store=store,
        revocation_policy=syn.RevocationPolicy.strict_query_verified(),
        audit_dir=tmp_path / "runs",
    )

    with pytest.raises(syn.RevocationStartupError):
        await runtime.run(app)

    assert not output.exists()


@pytest.mark.asyncio
async def test_strict_startup_error_survives_state_evidence_outage(
    tmp_path: pathlib.Path,
) -> None:
    env = syn.Environment(syn.Settings(db_path=tmp_path / "state"))

    @syn.fn
    def build() -> None:
        raise AssertionError("the app must not run")

    app = syn.App(syn.AppConfig(name="StrictUnavailableState", environment=env), build)
    runtime = syn.SynorRuntime(
        state_store=_UnavailableStateStore(),
        revocation_policy=syn.RevocationPolicy.strict_query_verified(),
        audit_dir=tmp_path / "runs",
    )

    with pytest.raises(syn.RevocationStartupError) as raised:
        await runtime.run(app)

    assert raised.value.safe_error_code == "revocation.ledger_repair_failed"
    assert "backend-path-MUST-NOT-ESCAPE" not in str(raised.value)
    manifest = audit.latest_run_manifest(
        audit_root=tmp_path / "runs",
        app_name="StrictUnavailableState",
    )
    assert manifest is not None
    assert manifest["status"] == "failed"


@pytest.mark.asyncio
async def test_strict_runtime_closes_verified_case_after_engine_commit(
    tmp_path: pathlib.Path,
) -> None:
    store = syn.MemoryStateStore()
    request = _revocation_request()
    runtime = syn.SynorRuntime(
        state_store=store,
        revocation_policy=syn.RevocationPolicy.strict_query_verified(),
        audit_dir=tmp_path / "runs",
    )
    observed_stages: list[revocation.RevocationStage] = []
    env = syn.Environment(syn.Settings(db_path=tmp_path / "state"))

    @syn.fn
    async def build() -> None:
        controller = runtime.revocation_controller
        planned = await controller.begin_case(request)
        assert planned.stage is revocation.RevocationStage.PLANNED
        verified = await _record_absent_outcome(controller, request)
        observed_stages.append(verified.stage)

    app = syn.App(syn.AppConfig(name="StrictVerifiedCase", environment=env), build)

    report = await runtime.run(app)
    closed = await runtime.revocation_controller.get_case(request.case_id)

    assert observed_stages == [revocation.RevocationStage.VERIFIED]
    assert closed is not None
    assert closed.stage is revocation.RevocationStage.CLOSED
    assert report.status is syn.ExecutionStatus.SUCCEEDED
    assert report.revocations is not None
    assert report.revocations.verified == 1


@pytest.mark.asyncio
async def test_strict_runtime_recovers_verified_case_after_failed_engine_commit(
    tmp_path: pathlib.Path,
) -> None:
    store = syn.MemoryStateStore()
    request = _revocation_request()
    failing_runtime = syn.SynorRuntime(
        state_store=store,
        revocation_policy=syn.RevocationPolicy.strict_query_verified(),
        audit_dir=tmp_path / "failed-runs",
    )
    failing_env = syn.Environment(syn.Settings(db_path=tmp_path / "failed-state"))

    @syn.fn
    async def verify_then_fail() -> None:
        controller = failing_runtime.revocation_controller
        await controller.begin_case(request)
        verified = await _record_absent_outcome(controller, request)
        assert verified.stage is revocation.RevocationStage.VERIFIED
        raise RuntimeError("fail before engine commit")

    failing_app = syn.App(
        syn.AppConfig(name="StrictInterruptedFinalization", environment=failing_env),
        verify_then_fail,
    )

    with pytest.raises(RuntimeError, match="fail before engine commit"):
        await failing_runtime.run(failing_app)
    stranded = await failing_runtime.revocation_controller.get_case(request.case_id)
    assert stranded is not None
    assert stranded.stage is revocation.RevocationStage.VERIFIED

    recovered_runtime = syn.SynorRuntime(
        state_store=store,
        revocation_policy=syn.RevocationPolicy.strict_query_verified(),
        audit_dir=tmp_path / "recovered-runs",
    )
    recovered_env = syn.Environment(syn.Settings(db_path=tmp_path / "recovered-state"))

    @syn.fn
    async def recover() -> None:
        case = await recovered_runtime.revocation_controller.begin_case(request)
        assert case.stage is revocation.RevocationStage.VERIFIED

    recovered_app = syn.App(
        syn.AppConfig(name="StrictRecoveredFinalization", environment=recovered_env),
        recover,
    )
    report = await recovered_runtime.run(recovered_app)
    closed = await recovered_runtime.revocation_controller.get_case(request.case_id)

    assert closed is not None
    assert closed.stage is revocation.RevocationStage.CLOSED
    assert report.status is syn.ExecutionStatus.SUCCEEDED


def test_execution_report_defaults_preserve_old_construction() -> None:
    report = syn.ExecutionReport(
        "legacy-run",
        syn.RunMode.PLAN,
        "LegacyApp",
        None,
        (),
        None,
        pathlib.Path("manifest.json"),
    )

    assert report.status is syn.ExecutionStatus.SUCCEEDED
    assert report.revocations is None
    assert report.execution_guarantee is syn.ExecutionGuarantee.COMPATIBILITY
