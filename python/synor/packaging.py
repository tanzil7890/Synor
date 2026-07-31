"""Offline pipeline lockfiles and deterministic Synor packages."""

from __future__ import annotations

import dataclasses as _dataclasses
import hashlib as _hashlib
import importlib.metadata as _metadata
import json as _json
import os as _os
import pathlib as _pathlib
import re as _re
import tomllib as _tomllib
import typing as _typing
import uuid as _uuid
import zipfile as _zipfile

from ._internal.app_target import split_app_target as _split_app_target
from ._version import __version__ as _synor_version

__all__ = [
    "LockedFile",
    "LockVerification",
    "PackageVerification",
    "PipelineLock",
    "build_pipeline_lock",
    "create_pipeline_package",
    "load_pipeline_lock",
    "verify_pipeline_lock",
    "verify_pipeline_package",
    "write_pipeline_lock",
]

_LOCK_SCHEMA_VERSION = 1
_PACKAGE_SCHEMA_VERSION = 1
_DEPENDENCY_NAME = _re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*")
_EXCLUDED_PARTS = {
    "__pycache__",
    "build",
    "catalog",
    "dist",
    "node_modules",
    "output",
    "outputs",
    "synor.db",
}
_FIXED_ZIP_TIME = (1980, 1, 1, 0, 0, 0)


def _sha256(payload: bytes) -> str:
    return _hashlib.sha256(payload).hexdigest()


def _safe_relative_path(value: str) -> bool:
    if not value or "\\" in value or value.startswith("./") or "//" in value:
        return False
    path = _pathlib.PurePosixPath(value)
    return (
        not path.is_absolute() and ".." not in path.parts and path.as_posix() == value
    )


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _module_path(app_target: str) -> _pathlib.Path:
    module_ref, _app_selection = _split_app_target(app_target)
    path = _pathlib.Path(module_ref).resolve()
    if not path.is_file() or path.suffix != ".py":
        raise ValueError("pipeline packaging requires a local Python APP_TARGET")
    return path


def _project_root(module_path: _pathlib.Path) -> _pathlib.Path:
    current = module_path.parent
    for directory in (current, *current.parents):
        if (directory / "pyproject.toml").is_file():
            return directory
    return current


def _is_source_file(path: _pathlib.Path, root: _pathlib.Path) -> bool:
    relative = path.relative_to(root)
    if any(part.startswith(".") or part in _EXCLUDED_PARTS for part in relative.parts):
        return False
    return path.suffix == ".py" or relative.as_posix() == "pyproject.toml"


def _source_files(root: _pathlib.Path) -> tuple[_pathlib.Path, ...]:
    return tuple(
        sorted(
            (
                path
                for path in root.rglob("*")
                if path.is_file() and _is_source_file(path, root)
            ),
            key=lambda path: path.relative_to(root).as_posix(),
        )
    )


def _declared_dependencies(root: _pathlib.Path) -> tuple[str, ...]:
    pyproject = root / "pyproject.toml"
    if not pyproject.is_file():
        return ("synor",)
    value = _tomllib.loads(pyproject.read_text(encoding="utf-8"))
    project = value.get("project")
    dependencies = project.get("dependencies", []) if isinstance(project, dict) else []
    names = {"synor"}
    if isinstance(dependencies, list):
        for dependency in dependencies:
            if not isinstance(dependency, str):
                continue
            match = _DEPENDENCY_NAME.match(dependency.strip())
            if match:
                names.add(match.group().lower().replace("_", "-"))
    return tuple(sorted(names))


@_dataclasses.dataclass(frozen=True, slots=True)
class LockedFile:
    """One source file pinned by content digest."""

    path: str
    sha256: str
    size: int


@_dataclasses.dataclass(frozen=True, slots=True)
class PipelineLock:
    """Offline-verifiable pipeline source and environment lock."""

    app_target: str
    entrypoint: str
    synor_version: str
    files: tuple[LockedFile, ...]
    distributions: dict[str, str]

    def to_dict(self) -> dict[str, _typing.Any]:
        """Return deterministic JSON data."""

        return {
            "schema_version": _LOCK_SCHEMA_VERSION,
            "app_target": self.app_target,
            "entrypoint": self.entrypoint,
            "synor_version": self.synor_version,
            "files": [_dataclasses.asdict(item) for item in self.files],
            "distributions": dict(sorted(self.distributions.items())),
        }

    @classmethod
    def from_dict(cls, value: _typing.Mapping[str, _typing.Any]) -> "PipelineLock":
        """Validate and decode one lockfile."""

        if value.get("schema_version") != _LOCK_SCHEMA_VERSION:
            raise ValueError("unsupported Synor pipeline lock")
        raw_files = value.get("files")
        raw_distributions = value.get("distributions")
        if not isinstance(raw_files, list) or not isinstance(raw_distributions, dict):
            raise ValueError("invalid pipeline lock contents")
        files: list[LockedFile] = []
        seen_paths: set[str] = set()
        for item in raw_files:
            if not isinstance(item, dict):
                raise ValueError("invalid locked source entry")
            path = str(item["path"])
            digest = str(item["sha256"])
            size = int(item["size"])
            if (
                not _safe_relative_path(path)
                or path in seen_paths
                or not _is_sha256(digest)
                or size < 0
            ):
                raise ValueError("invalid locked source entry")
            seen_paths.add(path)
            files.append(LockedFile(path=path, sha256=digest, size=size))
        distributions = {
            str(name): str(version) for name, version in raw_distributions.items()
        }
        entrypoint = str(value["entrypoint"])
        if not _safe_relative_path(entrypoint) or entrypoint not in seen_paths:
            raise ValueError("invalid pipeline entrypoint")
        return cls(
            app_target=str(value["app_target"]),
            entrypoint=entrypoint,
            synor_version=str(value["synor_version"]),
            files=tuple(files),
            distributions=distributions,
        )


@_dataclasses.dataclass(frozen=True, slots=True)
class LockVerification:
    """Local source/dependency verification result."""

    ok: bool
    source_mismatches: tuple[str, ...]
    distribution_mismatches: tuple[str, ...]


@_dataclasses.dataclass(frozen=True, slots=True)
class PackageVerification:
    """Deterministic package integrity result."""

    ok: bool
    package_digest: str
    errors: tuple[str, ...]


def build_pipeline_lock(app_target: str) -> PipelineLock:
    """Capture source hashes and installed direct dependency versions offline."""

    module_path = _module_path(app_target)
    root = _project_root(module_path)
    files = tuple(
        LockedFile(
            path=path.relative_to(root).as_posix(),
            sha256=_sha256(path.read_bytes()),
            size=path.stat().st_size,
        )
        for path in _source_files(root)
    )
    distributions: dict[str, str] = {}
    for name in _declared_dependencies(root):
        try:
            distributions[name] = _metadata.version(name)
        except _metadata.PackageNotFoundError:
            distributions[name] = "<missing>"
    return PipelineLock(
        app_target=app_target,
        entrypoint=module_path.relative_to(root).as_posix(),
        synor_version=_synor_version,
        files=files,
        distributions=distributions,
    )


def write_pipeline_lock(
    lock: PipelineLock,
    path: _os.PathLike[str] | str,
) -> _pathlib.Path:
    """Atomically write a pipeline lockfile."""

    output = _pathlib.Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{_uuid.uuid4().hex}.tmp")
    temporary.write_text(
        _json.dumps(lock.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _os.replace(temporary, output)
    return output


def load_pipeline_lock(path: _os.PathLike[str] | str) -> PipelineLock:
    """Read one pipeline lockfile."""

    value = _json.loads(_pathlib.Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("invalid pipeline lock")
    return PipelineLock.from_dict(value)


def verify_pipeline_lock(
    lock: PipelineLock,
    *,
    root: _os.PathLike[str] | str | None = None,
) -> LockVerification:
    """Verify source and installed direct dependencies without networking."""

    project_root = (
        _pathlib.Path(root).resolve()
        if root is not None
        else _project_root(_module_path(lock.app_target))
    )
    source_mismatches: list[str] = []
    for item in lock.files:
        if not _safe_relative_path(item.path):
            source_mismatches.append(f"{item.path}: invalid path")
            continue
        path = project_root / item.path
        if not path.is_file():
            source_mismatches.append(f"{item.path}: missing")
        elif (
            path.stat().st_size != item.size
            or _sha256(path.read_bytes()) != item.sha256
        ):
            source_mismatches.append(f"{item.path}: modified")
    distribution_mismatches: list[str] = []
    for name, expected in lock.distributions.items():
        try:
            actual = _metadata.version(name)
        except _metadata.PackageNotFoundError:
            actual = "<missing>"
        if actual != expected:
            distribution_mismatches.append(
                f"{name}: expected {expected}, found {actual}"
            )
    return LockVerification(
        ok=not source_mismatches and not distribution_mismatches,
        source_mismatches=tuple(source_mismatches),
        distribution_mismatches=tuple(distribution_mismatches),
    )


def _writestr(
    archive: _zipfile.ZipFile,
    name: str,
    payload: bytes,
) -> None:
    info = _zipfile.ZipInfo(name, date_time=_FIXED_ZIP_TIME)
    info.compress_type = _zipfile.ZIP_DEFLATED
    info.external_attr = 0o100644 << 16
    archive.writestr(info, payload)


def create_pipeline_package(
    lock: PipelineLock,
    output: _os.PathLike[str] | str,
) -> _pathlib.Path:
    """Create a deterministic source package without reading secrets or data."""

    root = _project_root(_module_path(lock.app_target))
    verification = verify_pipeline_lock(lock, root=root)
    if not verification.ok:
        details = (
            *verification.source_mismatches,
            *verification.distribution_mismatches,
        )
        raise ValueError(
            "pipeline lock does not match the current project: " + "; ".join(details)
        )
    output_path = _pathlib.Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.{_uuid.uuid4().hex}.tmp")
    lock_payload = (
        _json.dumps(lock.to_dict(), indent=2, sort_keys=True) + "\n"
    ).encode()
    manifest = {
        "schema_version": _PACKAGE_SCHEMA_VERSION,
        "format": "synor-pipeline",
        "entrypoint": lock.entrypoint,
        "lock_sha256": _sha256(lock_payload),
        "files": {
            item.path: item.sha256
            for item in sorted(lock.files, key=lambda item: item.path)
        },
    }
    try:
        with _zipfile.ZipFile(temporary, "w") as archive:
            _writestr(
                archive,
                "synor-package.json",
                (_json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode(),
            )
            _writestr(archive, "synor.lock.json", lock_payload)
            for item in sorted(lock.files, key=lambda item: item.path):
                _writestr(archive, f"src/{item.path}", (root / item.path).read_bytes())
        _os.replace(temporary, output_path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return output_path


def verify_pipeline_package(
    path: _os.PathLike[str] | str,
) -> PackageVerification:
    """Verify package paths, lock digest, and every packaged source digest."""

    package = _pathlib.Path(path)
    errors: list[str] = []
    digest = _sha256(package.read_bytes())
    try:
        with _zipfile.ZipFile(package) as archive:
            names = archive.namelist()
            if len(names) != len(set(names)):
                errors.append("package contains duplicate archive entries")
            for name in names:
                pure = _pathlib.PurePosixPath(name)
                if pure.is_absolute() or ".." in pure.parts:
                    errors.append(f"unsafe archive path: {name}")
            manifest_value = _json.loads(archive.read("synor-package.json"))
            lock_payload = archive.read("synor.lock.json")
            lock_value = _json.loads(lock_payload)
            if not isinstance(lock_value, dict):
                errors.append("invalid packaged lockfile")
                lock = None
            else:
                lock = PipelineLock.from_dict(lock_value)
            if not isinstance(manifest_value, dict):
                errors.append("invalid package manifest")
            elif manifest_value.get("schema_version") != _PACKAGE_SCHEMA_VERSION:
                errors.append("unsupported package manifest")
            else:
                if manifest_value.get("format") != "synor-pipeline":
                    errors.append("unsupported package format")
                if _sha256(lock_payload) != manifest_value.get("lock_sha256"):
                    errors.append("lockfile digest mismatch")
                files = manifest_value.get("files")
                if not isinstance(files, dict):
                    errors.append("invalid package file index")
                else:
                    indexed_names = {
                        "synor-package.json",
                        "synor.lock.json",
                    }
                    indexed_files: dict[str, str] = {}
                    for relative, expected in files.items():
                        if not isinstance(relative, str) or not isinstance(
                            expected, str
                        ):
                            errors.append("invalid package file index entry")
                            continue
                        if not _safe_relative_path(relative):
                            errors.append(f"unsafe indexed source path: {relative}")
                            continue
                        if not _is_sha256(expected):
                            errors.append(f"invalid source digest: {relative}")
                            continue
                        name = f"src/{relative}"
                        indexed_names.add(name)
                        indexed_files[relative] = expected
                        try:
                            payload = archive.read(name)
                        except KeyError:
                            errors.append(f"missing packaged source: {relative}")
                            continue
                        if _sha256(payload) != expected:
                            errors.append(f"source digest mismatch: {relative}")
                    unexpected = sorted(set(names) - indexed_names)
                    missing = sorted(indexed_names - set(names))
                    errors.extend(
                        f"unexpected package entry: {name}" for name in unexpected
                    )
                    errors.extend(f"missing package entry: {name}" for name in missing)
                    if lock is not None:
                        locked_files = {item.path: item.sha256 for item in lock.files}
                        if locked_files != indexed_files:
                            errors.append("package file index does not match lockfile")
                        if manifest_value.get("entrypoint") != lock.entrypoint:
                            errors.append("package entrypoint does not match lockfile")
                        for item in lock.files:
                            try:
                                payload = archive.read(f"src/{item.path}")
                            except KeyError:
                                continue
                            if len(payload) != item.size:
                                errors.append(f"source size mismatch: {item.path}")
    except (
        _zipfile.BadZipFile,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        errors.append(f"invalid package: {type(error).__name__}")
    return PackageVerification(
        ok=not errors,
        package_digest=digest,
        errors=tuple(errors),
    )
