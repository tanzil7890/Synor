from __future__ import annotations

import json
import pathlib
import threading
import urllib.request

import pytest

from synor import audit
from synor import dashboard
from synor.state import MemoryStateStore


def test_dashboard_is_loopback_read_only_and_serves_redacted_evidence(
    tmp_path: pathlib.Path,
) -> None:
    recorder = audit.RunRecorder.start(
        command="plan",
        app_name="LocalDashboard",
        app_target="./main.py",
        environment="default",
        db_path="./synor.db",
        policy={},
        audit_root=tmp_path / "runs",
    )
    recorder.finish(status="succeeded", action_count=0)

    server = dashboard.DashboardServer(
        host="127.0.0.1",
        port=0,
        audit_root=tmp_path / "runs",
        store=MemoryStateStore(),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with urllib.request.urlopen(
            server.address.url + "api/snapshot",
            timeout=5,
        ) as response:
            payload = json.load(response)
        assert payload["runs"][0]["app_name"] == "LocalDashboard"
        assert payload["quarantine"] == []
        with urllib.request.urlopen(
            server.address.url + "api/runs/" + recorder.run_id,
            timeout=5,
        ) as response:
            detail = json.load(response)
        assert detail["manifest"]["run_id"] == recorder.run_id
        assert detail["artifacts"] == []
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_dashboard_rejects_non_loopback_by_default() -> None:
    with pytest.raises(ValueError):
        dashboard.DashboardServer(host="0.0.0.0", port=0)


def test_dashboard_run_detail_rejects_parent_traversal(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match="escapes"):
        dashboard._run_detail(tmp_path / "runs", "..")
