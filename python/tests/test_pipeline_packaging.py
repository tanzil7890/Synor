from __future__ import annotations

import pathlib
import shutil
import zipfile

import pytest

from synor import packaging


def _project(tmp_path: pathlib.Path) -> pathlib.Path:
    (tmp_path / "pyproject.toml").write_text(
        """
[project]
name = "locked-pipeline"
version = "0.1.0"
dependencies = ["synor>=0.1.0a1"]
""".strip()
        + "\n",
        encoding="utf-8",
    )
    main = tmp_path / "main.py"
    main.write_text("VALUE = 1\n", encoding="utf-8")
    (tmp_path / ".env").write_text("SECRET=never-package\n", encoding="utf-8")
    (tmp_path / "data.txt").write_text("patient data\n", encoding="utf-8")
    return main


def test_lock_and_package_are_deterministic_and_exclude_data(
    tmp_path: pathlib.Path,
) -> None:
    main = _project(tmp_path)
    lock = packaging.build_pipeline_lock(str(main))
    assert packaging.verify_pipeline_lock(lock).ok
    selected_lock = packaging.build_pipeline_lock(f"{main}:LockedApp")
    assert selected_lock.entrypoint == lock.entrypoint
    assert selected_lock.files == lock.files

    first = packaging.create_pipeline_package(lock, tmp_path / "first.synor")
    second = packaging.create_pipeline_package(lock, tmp_path / "second.synor")
    assert first.read_bytes() == second.read_bytes()
    verification = packaging.verify_pipeline_package(first)
    assert verification.ok
    package_bytes = first.read_bytes()
    assert b"never-package" not in package_bytes
    assert b"patient data" not in package_bytes

    with_extra = tmp_path / "with-extra.synor"
    shutil.copyfile(first, with_extra)
    with zipfile.ZipFile(with_extra, "a") as archive:
        archive.writestr("unindexed.txt", b"not allowed")
    extra_verification = packaging.verify_pipeline_package(with_extra)
    assert not extra_verification.ok
    assert "unexpected package entry: unindexed.txt" in extra_verification.errors

    main.write_text("VALUE = 2\n", encoding="utf-8")
    changed = packaging.verify_pipeline_lock(lock)
    assert not changed.ok
    assert changed.source_mismatches == ("main.py: modified",)
    with pytest.raises(ValueError, match="does not match"):
        packaging.create_pipeline_package(lock, tmp_path / "stale.synor")


def test_pipeline_lock_rejects_unsafe_source_paths() -> None:
    with pytest.raises(ValueError, match="locked source"):
        packaging.PipelineLock.from_dict(
            {
                "schema_version": 1,
                "app_target": "main.py",
                "entrypoint": "../secret.py",
                "synor_version": "0.1.0a1",
                "files": [
                    {
                        "path": "../secret.py",
                        "sha256": "0" * 64,
                        "size": 1,
                    }
                ],
                "distributions": {},
            }
        )
