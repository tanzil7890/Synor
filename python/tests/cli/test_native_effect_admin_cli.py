import json
import pathlib
import stat
from types import SimpleNamespace
from typing import Any

from click.testing import CliRunner
from synor._internal import core as _core


def _record(
    evidence_id: str,
    *,
    status: str = "completed",
    updated_at_unix_ms: int = 100,
) -> SimpleNamespace:
    return SimpleNamespace(
        record_version=2,
        evidence_id=evidence_id,
        action_id=f"action:{evidence_id}",
        operation="delete",
        source_digest="a" * 64,
        source_generation=3,
        target_locator_digest="b" * 64,
        tracking_locator="Y29tcGFjdC10ZXN0",
        verification_policy="query_verified",
        cause="explicit",
        status=status,
        created_at_unix_ms=50,
        updated_at_unix_ms=updated_at_unix_ms,
        attempt_count=1,
        last_error_code=None,
    )


def _install_fake_environment(monkeypatch: Any, cli_module: Any) -> None:
    monkeypatch.setattr(
        cli_module,
        "_native_effect_environment",
        lambda _path: SimpleNamespace(_core_env=object()),
    )


def test_native_effect_export_writes_private_metadata_archive(
    monkeypatch: Any,
    tmp_path: pathlib.Path,
) -> None:
    import synor.cli as cli_module

    _install_fake_environment(monkeypatch, cli_module)
    snapshot = SimpleNamespace(
        schema_version=3,
        effects=[_record("effect:1")],
    )
    monkeypatch.setattr(
        _core,
        "native_effect_snapshot_by_name",
        lambda _env, _app_name: snapshot,
    )
    source = tmp_path / "source"
    source.mkdir()
    archive = tmp_path / "native-effects.json"

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "native-effects",
            "export",
            "--db",
            str(source),
            "--app-name",
            "search-index",
            "--output",
            str(archive),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(archive.read_text())
    assert payload["schema"] == "synor.native-effects.archive"
    assert payload["default_retention"] == "indefinite"
    assert payload["apps"][0]["native_schema_version"] == 3
    assert payload["apps"][0]["effects"][0]["evidence_id"] == "effect:1"
    assert stat.S_IMODE(archive.stat().st_mode) == 0o600
    assert "action:effect:1" not in result.output


def test_native_effect_export_rejects_archive_inside_database(
    monkeypatch: Any,
    tmp_path: pathlib.Path,
) -> None:
    import synor.cli as cli_module

    _install_fake_environment(monkeypatch, cli_module)
    source = tmp_path / "source"
    source.mkdir()
    archive = source / "unsafe-archive.json"

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "native-effects",
            "export",
            "--db",
            str(source),
            "--app-name",
            "search-index",
            "--output",
            str(archive),
        ],
    )

    assert result.exit_code != 0
    assert "outside the source database" in result.output
    assert not archive.exists()


def test_native_effect_compaction_requires_confirmation_and_archives_exact_candidates(
    monkeypatch: Any,
    tmp_path: pathlib.Path,
) -> None:
    import synor.cli as cli_module

    _install_fake_environment(monkeypatch, cli_module)
    source = tmp_path / "source"
    source.mkdir()
    archive = tmp_path / "compaction.json"
    snapshot = SimpleNamespace(
        schema_version=3,
        effects=[
            _record("old", updated_at_unix_ms=100),
            _record("new", updated_at_unix_ms=300),
            _record("legacy-time", updated_at_unix_ms=0),
            _record("pending", status="pending", updated_at_unix_ms=50),
        ],
    )
    monkeypatch.setattr(
        _core,
        "native_effect_snapshot_by_name",
        lambda _env, _app_name: snapshot,
    )
    captured: list[str] = []

    def _compact(_env: object, _app_name: str, evidence_ids: list[str]) -> Any:
        assert archive.exists()
        captured.extend(evidence_ids)
        return SimpleNamespace(
            requested=len(evidence_ids),
            deleted=len(evidence_ids),
            protected=0,
            already_absent=0,
        )

    monkeypatch.setattr(
        _core,
        "compact_native_effects_by_name",
        _compact,
    )
    runner = CliRunner()
    unconfirmed = runner.invoke(
        cli_module.cli,
        [
            "native-effects",
            "compact",
            "--db",
            str(source),
            "--app-name",
            "search-index",
            "--completed-before",
            "1970-01-01T00:00:00.200Z",
            "--archive",
            str(archive),
        ],
    )
    assert unconfirmed.exit_code != 0
    assert not archive.exists()

    unsafe_archive = source / "unsafe-compaction.json"
    unsafe = runner.invoke(
        cli_module.cli,
        [
            "native-effects",
            "compact",
            "--db",
            str(source),
            "--app-name",
            "search-index",
            "--completed-before",
            "1970-01-01T00:00:00.200Z",
            "--archive",
            str(unsafe_archive),
            "--confirm-compaction",
        ],
    )
    assert unsafe.exit_code != 0
    assert "outside the source database" in unsafe.output
    assert not unsafe_archive.exists()
    assert captured == []

    compacted = runner.invoke(
        cli_module.cli,
        [
            "native-effects",
            "compact",
            "--db",
            str(source),
            "--app-name",
            "search-index",
            "--completed-before",
            "1970-01-01T00:00:00.200Z",
            "--archive",
            str(archive),
            "--confirm-compaction",
            "--json",
        ],
    )
    assert compacted.exit_code == 0, compacted.output
    assert captured == ["old"]
    payload = json.loads(archive.read_text())
    assert payload["compaction_candidate_evidence_ids"] == ["old"]
    assert payload["retention"]["unknown_timestamp_records"] == "retained"


def test_native_effect_downgrade_archives_before_publishing_copy(
    monkeypatch: Any,
    tmp_path: pathlib.Path,
) -> None:
    import synor.cli as cli_module

    _install_fake_environment(monkeypatch, cli_module)
    source = tmp_path / "source"
    source.mkdir()
    output = tmp_path / "downgraded"
    archive = tmp_path / "downgrade.json"

    def _prepare(_env: object, staging: str) -> Any:
        staging_path = pathlib.Path(staging)
        (staging_path / "mdb").mkdir(parents=True)
        (staging_path / "mdb" / "data.mdb").write_bytes(b"consistent-copy")
        return SimpleNamespace(
            apps=[
                SimpleNamespace(
                    app_name="search-index",
                    schema_version=3,
                    effects=[_record("effect:1")],
                )
            ],
            removed_schema_markers=1,
            removed_effects=1,
            removed_obligation_cursors=0,
            removed_lineage_cursors=1,
            removed_live_generation_keys=1,
        )

    monkeypatch.setattr(_core, "prepare_native_downgrade", _prepare)
    unsafe = CliRunner().invoke(
        cli_module.cli,
        [
            "native-effects",
            "prepare-downgrade",
            "--db",
            str(source),
            "--output-db",
            str(output),
            "--archive",
            str(source / "unsafe-archive.json"),
            "--confirm-downgrade",
        ],
    )
    assert unsafe.exit_code != 0
    assert "outside the source database" in unsafe.output
    assert not output.exists()

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "native-effects",
            "prepare-downgrade",
            "--db",
            str(source),
            "--output-db",
            str(output),
            "--archive",
            str(archive),
            "--confirm-downgrade",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert source.exists()
    assert (output / "mdb" / "data.mdb").read_bytes() == b"consistent-copy"
    ready = json.loads((output / "DOWNGRADE_READY.json").read_text())
    archive_payload = json.loads(archive.read_text())
    assert ready["removed_effects"] == 1
    assert archive_payload["purpose"] == "downgrade_preparation"
    assert archive_payload["downgrade"]["source_database_modified"] is False
    assert archive_payload["apps"][0]["effects"][0]["evidence_id"] == "effect:1"
