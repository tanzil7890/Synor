from __future__ import annotations

import dataclasses
import hashlib
import json
import pathlib

from synor import audit
from synor import governance

from .revocation._fixtures import make_case, make_receipt


class _UnsafeRepr:
    def __repr__(self) -> str:
        return "PROMPT-MUST-NOT-APPEAR"


def test_redact_metadata_omits_secrets_and_payloads() -> None:
    result = audit.redact_metadata(
        {
            "api_key": "secret-value",
            "payload": b"private bytes",
            "object": _UnsafeRepr(),
        }
    )
    encoded = json.dumps(result)
    assert "secret-value" not in encoded
    assert "private bytes" not in encoded
    assert "PROMPT-MUST-NOT-APPEAR" not in encoded
    assert result["api_key"] == "<redacted>"
    assert result["payload"]["length"] == 13


def test_run_recorder_writes_atomic_manifest_and_audit(
    tmp_path: pathlib.Path,
) -> None:
    recorder = audit.RunRecorder.start(
        command="plan",
        app_name="Notes",
        app_target="./main.py",
        environment="default",
        db_path="./synor.db",
        policy={"network_access": "deny"},
        options={"preview": True},
        audit_root=tmp_path,
    )
    recorder.record_policy_decision(
        {
            "allowed": False,
            "destination": "example.test",
            "purpose": "test",
        }
    )
    recorder.finish(status="succeeded", action_count=2)

    manifest = audit.read_run_manifest(recorder.manifest_path)
    assert manifest["status"] == "succeeded"
    assert manifest["action_count"] == 2
    assert not list(recorder.run_dir.glob("*.tmp"))
    events = [
        json.loads(line)
        for line in recorder.audit_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [event["event"] for event in events] == [
        "run_started",
        "policy_decision",
        "run_finished",
    ]
    latest = audit.latest_run_manifest(audit_root=tmp_path, app_name="Notes")
    assert latest is not None
    assert latest["run_id"] == recorder.run_id


def test_failed_manifest_records_type_not_exception_message(
    tmp_path: pathlib.Path,
) -> None:
    recorder = audit.RunRecorder.start(
        command="run",
        app_name="Notes",
        app_target=None,
        environment="default",
        db_path="./synor.db",
        policy={},
        audit_root=tmp_path,
    )
    recorder.finish(
        status="failed",
        error=RuntimeError("sensitive document contents"),
    )
    text = recorder.manifest_path.read_text(encoding="utf-8")
    assert "RuntimeError" in text
    assert "sensitive document contents" not in text


def test_governed_public_types_use_metadata_only_audit_projections() -> None:
    raw_item_id = "raw-drive-item-id-MUST-NOT-APPEAR"
    raw_tenant_id = "raw-tenant-MUST-NOT-APPEAR"
    raw_resource = "source-content-MUST-NOT-APPEAR"
    identity = governance.SourceIdentity(
        connector_instance_id="drive-connector",
        source_scope_id="shared-drive",
        item_id=raw_item_id,
    )
    access = governance.AccessSnapshot(
        tenant_id=raw_tenant_id,
        policy_id="policy-a",
        policy_revision="revision-2",
        policy_digest=hashlib.sha256(b"policy-a-v2").hexdigest(),
        group_graph_revision="groups-2",
    )
    item = governance.GovernedSourceItem(
        identity=identity,
        resource=raw_resource,
        source_revision="raw-source-revision-MUST-NOT-APPEAR",
        content_fingerprint=b"content fingerprint",
        access=access,
        event=governance.SourceEventKind.ACL_CHANGED,
        observation_id="obs1_" + "a" * 64,
    )

    encoded = json.dumps(audit.redact_metadata(item), sort_keys=True)

    assert raw_item_id not in encoded
    assert raw_tenant_id not in encoded
    assert raw_resource not in encoded
    assert "raw-source-revision" not in encoded
    assert identity.evidence_digest() in encoded


def test_revocation_case_and_receipt_audit_omit_reversible_identifiers() -> None:
    source_revision = "drive-revision-MUST-NOT-APPEAR"
    operation_id = "provider-operation-MUST-NOT-APPEAR"
    case = dataclasses.replace(make_case(), source_revision=source_revision)
    receipt = dataclasses.replace(
        make_receipt(case, attempt=0, previous_receipt_digest=None),
        operation_id=operation_id,
    )

    case_metadata = audit.redact_metadata(case)
    receipt_metadata = audit.redact_metadata(receipt)
    encoded = json.dumps(
        {"case": case_metadata, "receipt": receipt_metadata},
        sort_keys=True,
    )

    assert source_revision not in encoded
    assert operation_id not in encoded
    assert "source_revision" not in case_metadata
    assert "operation_id" not in receipt_metadata
    assert len(case_metadata["source_revision_digest"]) == 64
    assert receipt_metadata["operation_id_present"] is True


def test_old_manifest_without_revocation_fields_remains_readable(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "run_id": "legacy-run",
                "status": "succeeded",
            }
        ),
        encoding="utf-8",
    )

    manifest = audit.read_run_manifest(path)

    assert manifest["status"] == "succeeded"
    assert "revocations" not in manifest
