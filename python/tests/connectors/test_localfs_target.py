"""Unit tests for localfs target helper logic.

Tests cover:
- _execute_entry_action: file/directory creation, deletion, content writing
- _get_base_dir_key: base directory key extraction
- _EntrySpec / _DirSpec: specification construction
"""

from __future__ import annotations

import ctypes
import os
import pathlib
from pathlib import Path

import pytest
import synor as syn
from synor._internal.context_keys import ContextKey
from synor.connectors import localfs
from synor.connectors.localfs import _target as _localfs_target
from synor.connectors.localfs._common import FilePath
from synor.connectors.localfs._target import (
    DirTarget,
    _DirSpec,
    _EntryAction,
    _EntrySpec,
    _execute_entry_action,
    _get_base_dir_key,
    _validate_child_path,
)


def _symlink_or_skip(
    link: Path, target: Path, *, target_is_directory: bool = False
) -> None:
    try:
        link.symlink_to(target, target_is_directory=target_is_directory)
    except OSError as exc:
        pytest.skip(f"symbolic links are unavailable in this test environment: {exc}")


# =============================================================================
# _get_base_dir_key
# =============================================================================


class TestGetBaseDirKey:
    def test_returns_none_for_cwd_path(self) -> None:
        fp = FilePath("some/path.txt")
        assert _get_base_dir_key(fp) is None

    def test_returns_key_for_context_key_base(self) -> None:
        key: ContextKey[pathlib.Path] = ContextKey("test_base_dir_key_unique_42")
        fp = FilePath("some/path.txt", base_dir=key)
        assert _get_base_dir_key(fp) == key.key

    def test_key_string_matches_context_key_name(self) -> None:
        key: ContextKey[pathlib.Path] = ContextKey("my_source_dir")
        fp = FilePath("file.txt", base_dir=key)
        result = _get_base_dir_key(fp)
        assert result == "my_source_dir"


# =============================================================================
# _EntrySpec and _DirSpec construction
# =============================================================================


class TestEntrySpecConstruction:
    def test_file_spec_stores_content(self) -> None:
        content = b"hello"
        spec = _EntrySpec(entry_spec=content, create_parent_dirs=False)
        assert spec.entry_spec == content
        assert spec.create_parent_dirs is False

    def test_dir_spec_is_sentinel(self) -> None:
        spec = _EntrySpec(entry_spec=_DirSpec(), create_parent_dirs=False)
        assert isinstance(spec.entry_spec, _DirSpec)

    def test_create_parent_dirs_flag(self) -> None:
        spec = _EntrySpec(entry_spec=b"data", create_parent_dirs=True)
        assert spec.create_parent_dirs is True


# =============================================================================
# Child path validation
# =============================================================================


class TestChildPathValidation:
    @pytest.mark.parametrize(
        "path",
        [
            "",
            ".",
            "..",
            "../outside.txt",
            "nested/../outside.txt",
            "/absolute.txt",
            "C:\\absolute.txt",
            "C:drive-relative.txt",
            "\\\\server\\share\\file.txt",
            "nested\\file.txt",
            "nested//file.txt",
            "nested/./file.txt",
            "nul\x00file.txt",
        ],
    )
    def test_rejects_unsafe_or_ambiguous_paths(self, path: str) -> None:
        with pytest.raises(ValueError):
            _validate_child_path(path)

    @pytest.mark.parametrize("path", ["file.txt", "nested/file.txt", "a/b/c.bin"])
    def test_accepts_documented_nested_relative_paths(self, path: str) -> None:
        assert _validate_child_path(path) == path

    @pytest.mark.parametrize(
        "path",
        [
            "nested/file:stream",
            "nested/CON.txt",
            "nested/COM¹.log",
            "nested/LPT³",
            "nested/aux",
            "nested/trailing.",
            "nested/trailing ",
            "nested/question?.txt",
            "nested/control\x01.txt",
        ],
    )
    def test_applies_windows_filename_rules_only_on_windows(self, path: str) -> None:
        if os.name == "nt":
            with pytest.raises(ValueError):
                _validate_child_path(path)
        else:
            assert _validate_child_path(path) == path

    def test_rejects_unsafe_file_before_provider_access(self) -> None:
        target = DirTarget(object())  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="segments"):
            target.ensure_file("../outside.txt", b"unsafe")

    def test_rejects_unsafe_directory_before_provider_access(self) -> None:
        target = DirTarget(object())  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="relative paths"):
            target.ensure_dir_target("/outside")


@pytest.mark.asyncio
async def test_context_root_preserves_nested_relative_file_contract(
    tmp_path: Path,
) -> None:
    output_key = ContextKey[Path]("test_localfs_target/safe_output_root")
    output_dir = tmp_path / "output"
    env = syn.Environment(syn.Settings(db_path=tmp_path / "state"))
    env.context_provider.provide(output_key, output_dir)

    @syn.task
    async def main() -> None:
        target = await syn.call(localfs.ensure_dir_target, output_key)
        target.ensure_file("nested/result.txt", "safe", create_parent_dirs=True)

    app = syn.App(syn.AppConfig(name="localfs-nested-relative", environment=env), main)
    await app.update()

    assert (output_dir / "nested" / "result.txt").read_text() == "safe"


# =============================================================================
# _execute_entry_action
# =============================================================================


class TestExecuteEntryAction:
    # --- File creation ---

    def test_creates_file_with_content(self, tmp_path: Path) -> None:
        target = tmp_path / "output.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"hello world",
            create_parents=False,
        )
        result = _execute_entry_action(target, action)
        assert result is None
        assert target.read_bytes() == b"hello world"

    def test_creates_file_with_empty_content(self, tmp_path: Path) -> None:
        target = tmp_path / "empty.bin"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"",
            create_parents=False,
        )
        _execute_entry_action(target, action)
        assert target.exists()
        assert target.read_bytes() == b""

    def test_overwrites_existing_file(self, tmp_path: Path) -> None:
        target = tmp_path / "data.txt"
        target.write_bytes(b"old content")
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"new content",
            create_parents=False,
        )
        _execute_entry_action(target, action)
        assert target.read_bytes() == b"new content"

    def test_file_with_create_parents(self, tmp_path: Path) -> None:
        target = tmp_path / "a" / "b" / "c.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"nested",
            create_parents=True,
        )
        _execute_entry_action(target, action)
        assert target.read_bytes() == b"nested"

    def test_file_without_create_parents_raises(self, tmp_path: Path) -> None:
        target = tmp_path / "missing_dir" / "file.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"x",
            create_parents=False,
        )
        with pytest.raises(FileNotFoundError):
            _execute_entry_action(target, action)

    # --- Directory creation ---

    def test_creates_directory(self, tmp_path: Path) -> None:
        target = tmp_path / "newdir"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="dir",
            content=b"",
            create_parents=False,
        )
        result = _execute_entry_action(target, action)
        assert result == target
        assert target.is_dir()

    def test_creates_directory_with_parents(self, tmp_path: Path) -> None:
        target = tmp_path / "a" / "b" / "newdir"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="dir",
            content=b"",
            create_parents=True,
        )
        result = _execute_entry_action(target, action)
        assert result == target
        assert target.is_dir()

    def test_create_dir_is_idempotent(self, tmp_path: Path) -> None:
        target = tmp_path / "existing"
        target.mkdir()
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="dir",
            content=b"",
            create_parents=False,
        )
        # Should not raise even if directory already exists
        result = _execute_entry_action(target, action)
        assert result == target

    # --- Deletion ---

    def test_deletes_existing_file(self, tmp_path: Path) -> None:
        target = tmp_path / "to_delete.txt"
        target.write_bytes(b"bye")
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=None,
            create_parents=False,
        )
        result = _execute_entry_action(target, action)
        assert result is None
        assert not target.exists()

    def test_delete_missing_file_is_noop(self, tmp_path: Path) -> None:
        target = tmp_path / "nonexistent.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=None,
            create_parents=False,
        )
        # Should not raise
        result = _execute_entry_action(target, action)
        assert result is None

    def test_deletes_existing_directory(self, tmp_path: Path) -> None:
        target = tmp_path / "dir_to_delete"
        target.mkdir()
        (target / "child.txt").write_bytes(b"x")
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="dir",
            content=None,
            create_parents=False,
        )
        result = _execute_entry_action(target, action)
        assert result is None
        assert not target.exists()

    def test_delete_missing_directory_is_noop(self, tmp_path: Path) -> None:
        target = tmp_path / "ghost_dir"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="dir",
            content=None,
            create_parents=False,
        )
        # Should not raise
        result = _execute_entry_action(target, action)
        assert result is None

    # --- Containment and symbolic links ---

    def test_contained_write_rejects_parent_symlink_escape(
        self, tmp_path: Path
    ) -> None:
        root = tmp_path / "root"
        outside = tmp_path / "outside"
        root.mkdir()
        outside.mkdir()
        _symlink_or_skip(root / "link", outside, target_is_directory=True)
        target = root / "link" / "escaped.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"must not escape",
            create_parents=False,
            containment_root=str(root),
        )

        with pytest.raises(ValueError, match="symbolic link"):
            _execute_entry_action(target, action)

        assert not (outside / "escaped.txt").exists()

    @pytest.mark.skipif(
        not _localfs_target._SECURE_DIR_FD_IO,
        reason="descriptor-relative no-follow I/O is unavailable",
    )
    def test_contained_write_rejects_symlink_above_containment_root(
        self, tmp_path: Path
    ) -> None:
        actual_base = tmp_path / "actual-base"
        actual_root = actual_base / "managed"
        actual_root.mkdir(parents=True)
        linked_base = tmp_path / "linked-base"
        _symlink_or_skip(linked_base, actual_base, target_is_directory=True)
        target = linked_base / "managed" / "escaped.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"must not follow a linked root ancestor",
            create_parents=False,
            containment_root=str(linked_base / "managed"),
        )

        with pytest.raises(ValueError, match="symbolic link"):
            _execute_entry_action(target, action)

        assert not (actual_root / "escaped.txt").exists()

    def test_contained_write_rejects_leaf_symlink(self, tmp_path: Path) -> None:
        root = tmp_path / "root"
        root.mkdir()
        outside = tmp_path / "outside.txt"
        outside.write_bytes(b"original")
        target = root / "result.txt"
        _symlink_or_skip(target, outside)
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"replacement",
            create_parents=False,
            containment_root=str(root),
        )

        with pytest.raises(ValueError, match="symbolic link"):
            _execute_entry_action(target, action)

        assert outside.read_bytes() == b"original"

    @pytest.mark.skipif(
        not _localfs_target._SECURE_DIR_FD_IO,
        reason="descriptor-relative no-follow I/O is unavailable",
    )
    def test_contained_write_cannot_escape_parent_swapped_after_open(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root = tmp_path / "root"
        parent = root / "nested"
        displaced_parent = root / "displaced"
        outside = tmp_path / "outside"
        parent.mkdir(parents=True)
        outside.mkdir()
        target = parent / "result.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"contained",
            create_parents=False,
            containment_root=str(root),
        )
        original_atomic_write = _localfs_target._atomic_write_at

        def _swap_parent_then_write(fd: int, name: str, content: bytes) -> None:
            parent.rename(displaced_parent)
            _symlink_or_skip(parent, outside, target_is_directory=True)
            original_atomic_write(fd, name, content)

        monkeypatch.setattr(
            _localfs_target, "_atomic_write_at", _swap_parent_then_write
        )

        _execute_entry_action(target, action)

        assert not (outside / "result.txt").exists()
        assert (displaced_parent / "result.txt").read_bytes() == b"contained"

    @pytest.mark.skipif(
        not _localfs_target._SECURE_DIR_FD_IO,
        reason="descriptor-relative no-follow I/O is unavailable",
    )
    def test_contained_write_cannot_escape_root_ancestor_swapped_during_open(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        base = tmp_path / "base"
        root = base / "managed"
        displaced_base = tmp_path / "displaced-base"
        outside = tmp_path / "outside"
        root.mkdir(parents=True)
        (outside / "managed").mkdir(parents=True)
        target = root / "result.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"contained",
            create_parents=False,
            containment_root=str(root),
        )
        original_open = _localfs_target._open_directory_no_follow
        base_stat = base.stat()
        base_identity = (base_stat.st_dev, base_stat.st_ino)
        swapped = False

        def _swap_ancestor_while_opening(
            path: str | Path, *, dir_fd: int | None = None
        ) -> int:
            nonlocal swapped

            # The pre-fix implementation attempted one absolute open of root;
            # swap immediately before it to reproduce the escape.  The secure
            # implementation instead reaches the fixed "base" component via
            # a pinned tmp_path descriptor; swap only after that fd is open.
            if not swapped and Path(path) == root:
                base.rename(displaced_base)
                _symlink_or_skip(base, outside, target_is_directory=True)
                swapped = True

            fd = original_open(path, dir_fd=dir_fd)
            opened_stat = os.fstat(fd)
            opened_identity = (opened_stat.st_dev, opened_stat.st_ino)
            if not swapped and opened_identity == base_identity:
                base.rename(displaced_base)
                _symlink_or_skip(base, outside, target_is_directory=True)
                swapped = True
            return fd

        monkeypatch.setattr(
            _localfs_target,
            "_open_directory_no_follow",
            _swap_ancestor_while_opening,
        )

        _execute_entry_action(target, action)

        assert swapped
        assert not (outside / "managed" / "result.txt").exists()
        assert (displaced_base / "managed" / "result.txt").read_bytes() == b"contained"

    @pytest.mark.parametrize("entry_type", ["file", "dir"])
    def test_contained_delete_unlinks_leaf_symlink_without_touching_referent(
        self, tmp_path: Path, entry_type: str
    ) -> None:
        root = tmp_path / "root"
        outside = tmp_path / "outside"
        root.mkdir()
        outside.mkdir()
        protected = outside / "protected.txt"
        protected.write_bytes(b"keep")
        target = root / "link"
        _symlink_or_skip(target, outside, target_is_directory=True)
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type=entry_type,  # type: ignore[arg-type]
            content=None,
            create_parents=False,
            containment_root=str(root),
        )

        _execute_entry_action(target, action)

        assert not target.exists()
        assert protected.read_bytes() == b"keep"

    @pytest.mark.skipif(
        not _localfs_target._SECURE_DIR_FD_IO,
        reason="descriptor-relative no-follow I/O is unavailable",
    )
    def test_contained_recursive_delete_does_not_follow_internal_symlink(
        self, tmp_path: Path
    ) -> None:
        root = tmp_path / "root"
        target = root / "tree"
        outside = tmp_path / "outside"
        target.mkdir(parents=True)
        outside.mkdir()
        (target / "owned.txt").write_bytes(b"remove")
        protected = outside / "protected.txt"
        protected.write_bytes(b"keep")
        _symlink_or_skip(
            target / "external",
            outside,
            target_is_directory=True,
        )
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="dir",
            content=None,
            create_parents=False,
            containment_root=str(root),
        )

        _execute_entry_action(target, action)

        assert not target.exists()
        assert protected.read_bytes() == b"keep"

    @pytest.mark.skipif(
        not _localfs_target._SECURE_DIR_FD_IO,
        reason="descriptor-relative no-follow I/O is unavailable",
    )
    @pytest.mark.parametrize("entry_type", ["file", "dir"])
    def test_contained_delete_cannot_escape_parent_swapped_after_open(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        entry_type: str,
    ) -> None:
        root = tmp_path / "root"
        parent = root / "nested"
        displaced_parent = root / "displaced"
        outside = tmp_path / "outside"
        parent.mkdir(parents=True)
        outside.mkdir()
        target = parent / "entry"
        outside_entry = outside / "entry"
        if entry_type == "file":
            target.write_bytes(b"remove")
            outside_entry.write_bytes(b"keep")
        else:
            target.mkdir()
            (target / "owned.txt").write_bytes(b"remove")
            outside_entry.mkdir()
            (outside_entry / "protected.txt").write_bytes(b"keep")
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type=entry_type,  # type: ignore[arg-type]
            content=None,
            create_parents=False,
            containment_root=str(root),
        )
        original_lstat = _localfs_target._lstat_at
        swapped = False

        def _swap_parent_then_lstat(parent_fd: int, name: str) -> os.stat_result | None:
            nonlocal swapped
            if not swapped:
                parent.rename(displaced_parent)
                _symlink_or_skip(parent, outside, target_is_directory=True)
                swapped = True
            return original_lstat(parent_fd, name)

        monkeypatch.setattr(_localfs_target, "_lstat_at", _swap_parent_then_lstat)

        _execute_entry_action(target, action)

        assert swapped
        assert not (displaced_parent / "entry").exists()
        if entry_type == "file":
            assert outside_entry.read_bytes() == b"keep"
        else:
            assert (outside_entry / "protected.txt").read_bytes() == b"keep"

    def test_contained_action_rejects_forged_traversal(self, tmp_path: Path) -> None:
        root = tmp_path / "root"
        root.mkdir()
        target = root / ".." / "escaped.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"unsafe",
            create_parents=False,
            containment_root=str(root),
        )

        with pytest.raises(ValueError, match="traversal"):
            _execute_entry_action(target, action)

        assert not (tmp_path / "escaped.txt").exists()

    def test_contained_action_rejects_traversal_that_normalizes_inside_root(
        self, tmp_path: Path
    ) -> None:
        root = tmp_path / "root"
        root.mkdir()
        target = root / "nested" / ".." / "victim.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"unsafe alias",
            create_parents=True,
            containment_root=str(root),
        )

        with pytest.raises(ValueError, match="traversal"):
            _execute_entry_action(target, action)

        assert not (root / "victim.txt").exists()

    def test_contained_action_fails_closed_without_secure_handle_io(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root = tmp_path / "root"
        root.mkdir()
        target = root / "result.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"must not be written through pathname fallback",
            create_parents=False,
            containment_root=str(root),
        )
        monkeypatch.setattr(_localfs_target, "_SECURE_DIR_FD_IO", False)
        monkeypatch.setattr(_localfs_target, "_SECURE_WINDOWS_IO", False)

        with pytest.raises(RuntimeError, match="pinned-handle"):
            _execute_entry_action(target, action)

        assert not target.exists()

    def test_contained_action_uses_secure_windows_handle_backend(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root = tmp_path / "root"
        root.mkdir()
        target = root / "result.txt"
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type="file",
            content=b"handled by the Windows backend",
            create_parents=False,
            containment_root=str(root),
        )
        calls: list[tuple[Path, _EntryAction, Path]] = []

        def _fake_windows_backend(
            path: Path, entry_action: _EntryAction, containment_root: Path
        ) -> None:
            calls.append((path, entry_action, containment_root))

        monkeypatch.setattr(_localfs_target, "_SECURE_DIR_FD_IO", False)
        monkeypatch.setattr(_localfs_target, "_SECURE_WINDOWS_IO", True)
        monkeypatch.setattr(
            _localfs_target,
            "_execute_contained_entry_action_windows",
            _fake_windows_backend,
        )

        assert _execute_entry_action(target, action) is None
        assert calls == [(target, action, root)]

    @pytest.mark.parametrize(
        ("tracked_type", "actual_type"),
        [("file", "dir"), ("dir", "file")],
    )
    def test_contained_delete_rejects_actual_type_mismatch(
        self, tmp_path: Path, tracked_type: str, actual_type: str
    ) -> None:
        root = tmp_path / "root"
        root.mkdir()
        target = root / "entry"
        if actual_type == "dir":
            target.mkdir()
            (target / "keep.txt").write_bytes(b"keep")
        else:
            target.write_bytes(b"keep")
        action = _EntryAction(
            base_dir_key=None,
            path=str(target),
            entry_type=tracked_type,  # type: ignore[arg-type]
            content=None,
            create_parents=False,
            containment_root=str(root),
        )

        with pytest.raises(ValueError, match="actual type"):
            _execute_entry_action(target, action)

        assert target.exists()


class TestWindowsHandleBackend:
    def test_file_disposition_info_uses_one_byte_boolean(self) -> None:
        assert ctypes.sizeof(_localfs_target._WindowsFileDispositionInformation) == 1

    def test_delete_handle_denies_write_and_delete_sharing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        open_calls: list[dict[str, int]] = []

        def _fake_open(path: Path, **kwargs: int) -> int:
            open_calls.append(kwargs)
            return 17

        monkeypatch.setattr(_localfs_target, "_windows_open_handle", _fake_open)
        monkeypatch.setattr(_localfs_target, "_windows_handle_attributes", lambda _: 0)
        monkeypatch.setattr(
            _localfs_target, "_windows_mark_handle_for_deletion", lambda _: None
        )
        monkeypatch.setattr(_localfs_target, "_windows_close_handle", lambda _: None)

        _localfs_target._windows_delete_tree_by_handle(Path("entry"), "file")

        assert open_calls == [
            {
                "access": (
                    _localfs_target._WIN_FILE_READ_ATTRIBUTES
                    | _localfs_target._WIN_DELETE
                ),
                "share": _localfs_target._WIN_FILE_SHARE_READ,
            }
        ]

    def test_handle_rename_uses_simple_utf16_name(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[tuple[int, int, int, int | None, int, bytes]] = []

        class _FakeKernel32:
            def SetFileInformationByHandle(
                self,
                handle: int,
                information_class: int,
                buffer: object,
                buffer_size: int,
            ) -> bool:
                address = ctypes.addressof(buffer)  # type: ignore[arg-type]
                information = ctypes.cast(
                    address,
                    ctypes.POINTER(_localfs_target._WindowsFileRenameInformation),
                ).contents
                encoded_name = ctypes.string_at(
                    address
                    + _localfs_target._WindowsFileRenameInformation.FileName.offset,
                    information.FileNameLength,
                )
                calls.append(
                    (
                        handle,
                        information_class,
                        information.ReplaceIfExists,
                        information.RootDirectory,
                        buffer_size,
                        encoded_name,
                    )
                )
                return True

        monkeypatch.setattr(_localfs_target, "_SECURE_WINDOWS_IO", True)
        monkeypatch.setattr(_localfs_target, "_windows_kernel32_dll", _FakeKernel32())

        _localfs_target._windows_rename_handle(41, "résult.txt")

        assert len(calls) == 1
        handle, information_class, replace, root, buffer_size, encoded_name = calls[0]
        assert handle == 41
        assert information_class == _localfs_target._WIN_FILE_RENAME_INFO
        assert replace == 1
        assert root is None
        assert buffer_size >= (
            _localfs_target._WindowsFileRenameInformation.FileName.offset
            + len(encoded_name)
        )
        assert encoded_name.decode("utf-16-le") == "résult.txt"

    @pytest.mark.skipif(os.name != "nt", reason="requires Win32 file handles")
    def test_contained_write_and_recursive_delete_use_win32_handles(
        self, tmp_path: Path
    ) -> None:
        root = tmp_path / "root"
        nested = root / "nested"
        nested.mkdir(parents=True)
        target = nested / "result.txt"
        target.write_bytes(b"old")

        _execute_entry_action(
            target,
            _EntryAction(
                base_dir_key=None,
                path=str(target),
                entry_type="file",
                content=b"new",
                create_parents=False,
                containment_root=str(root),
            ),
        )
        assert target.read_bytes() == b"new"

        _execute_entry_action(
            nested,
            _EntryAction(
                base_dir_key=None,
                path=str(nested),
                entry_type="dir",
                content=None,
                create_parents=False,
                containment_root=str(root),
            ),
        )
        assert not nested.exists()

    @pytest.mark.skipif(os.name != "nt", reason="requires Win32 file handles")
    def test_failed_atomic_install_deletes_exact_open_temp_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root = tmp_path / "root"
        root.mkdir()
        target = root / "result.txt"

        def _fail_install(handle: int, name: str) -> None:
            raise OSError(f"injected rename failure for {handle}:{name}")

        monkeypatch.setattr(_localfs_target, "_windows_rename_handle", _fail_install)

        with pytest.raises(OSError, match="injected rename failure"):
            _execute_entry_action(
                target,
                _EntryAction(
                    base_dir_key=None,
                    path=str(target),
                    entry_type="file",
                    content=b"new",
                    create_parents=False,
                    containment_root=str(root),
                ),
            )

        assert not target.exists()
        assert list(root.glob(".synor-*.tmp")) == []

    @pytest.mark.skipif(os.name != "nt", reason="requires Win32 file handles")
    def test_intermediate_reparse_point_above_root_is_rejected(
        self, tmp_path: Path
    ) -> None:
        actual_parent = tmp_path / "actual-parent"
        actual_root = actual_parent / "root"
        actual_root.mkdir(parents=True)
        linked_parent = tmp_path / "linked-parent"
        _symlink_or_skip(
            linked_parent,
            actual_parent,
            target_is_directory=True,
        )
        target = linked_parent / "root" / "result.txt"

        with pytest.raises(ValueError, match="reparse point"):
            _execute_entry_action(
                target,
                _EntryAction(
                    base_dir_key=None,
                    path=str(target),
                    entry_type="file",
                    content=b"must not follow the linked root prefix",
                    create_parents=False,
                    containment_root=str(linked_parent / "root"),
                ),
            )

        assert not (actual_root / "result.txt").exists()
