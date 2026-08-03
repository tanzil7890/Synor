"""Local filesystem target utilities."""

from __future__ import annotations

import ctypes
import ctypes.wintypes
import os
import pathlib
import secrets
import shutil
import stat
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import Any, Generic, Literal, NamedTuple, cast

import msgspec
import synor as syn
from synor._internal.context_keys import ContextKey, ContextProvider
from synor._internal.datatype import TypeChecker
from synor.connectorkits.fingerprint import fingerprint_bytes

from ._common import FilePath, to_file_path

# =============================================================================
# Shared types and helpers
# =============================================================================

_EntryName = str  # File or directory name (path segment)
_ENTRY_NAME_CHECKER = TypeChecker(str)
_FileContent = bytes
_FileFingerprint = bytes
_WINDOWS_RESERVED_ENTRY_NAMES = {
    "AUX",
    "CLOCK$",
    "CON",
    "CONIN$",
    "CONOUT$",
    "NUL",
    "PRN",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
    *(f"COM{index}" for index in "¹²³"),
    *(f"LPT{index}" for index in "¹²³"),
}
_WINDOWS_FORBIDDEN_ENTRY_CHARS = frozenset('<>:"|?*')


class _EntryAction(NamedTuple):
    """Action to perform on a file or directory entry."""

    base_dir_key: (
        str | None
    )  # Context key for base dir; None means path is already absolute
    path: str  # Absolute path string if base_dir_key is None; relative path otherwise
    entry_type: Literal["file", "dir"]
    content: _FileContent | None  # For files; None means delete
    create_parents: bool  # Whether to create parent directories
    containment_root: str | None = None  # Root that non-root entries must stay in


@dataclass(frozen=True, slots=True)
class _DirSpec:
    """Marker for a directory entry (no content)."""

    pass


@dataclass(frozen=True, slots=True)
class _EntrySpec:
    """Specification for an entry: content/type plus options."""

    entry_spec: _FileContent | _DirSpec
    create_parent_dirs: bool


def _validate_child_path(path: str, *, allow_root: bool = False) -> str:
    """Validate a child target key without changing its stable identity.

    Nested relative paths are part of ``DirTarget``'s documented contract, but
    anchored paths and traversal segments are not.  Check both native and
    Windows anchor syntax on every platform so a state created on one platform
    cannot become an escape when replayed on another. Windows-only filename
    aliases and character restrictions remain platform-specific so valid POSIX
    filenames keep working.
    """
    if "\x00" in path:
        raise ValueError("localfs child paths must not contain NUL bytes")
    if "\\" in path:
        raise ValueError(
            "localfs child paths must use '/' and must not contain backslashes"
        )
    if allow_root and path == ".":
        return path

    native_path = pathlib.PurePath(path)
    windows_path = pathlib.PureWindowsPath(path)
    if (
        not path
        or native_path == pathlib.PurePath(".")
        or native_path.is_absolute()
        or native_path.anchor
        or windows_path.is_absolute()
        or windows_path.drive
        or windows_path.root
    ):
        raise ValueError(
            f"localfs child paths must be non-empty relative paths: {path!r}"
        )

    # Splitting both separator forms also rejects aliases such as ``a//b`` and
    # ``a/./b``.  Besides traversal safety, this prevents two target-state keys
    # from naming the same filesystem entry.
    parts = path.replace("\\", "/").split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(
            f"localfs child paths must not contain empty, '.' or '..' segments: {path!r}"
        )
    if os.name == "nt":
        for part in parts:
            if part.endswith((" ", ".")):
                raise ValueError(
                    "localfs child path segments must not end with a space or dot: "
                    f"{path!r}"
                )
            if any(ord(char) < 32 for char in part) or any(
                char in _WINDOWS_FORBIDDEN_ENTRY_CHARS for char in part
            ):
                raise ValueError(
                    "localfs child path segments contain characters that are unsafe "
                    f"on Windows: {path!r}"
                )
            windows_basename = part.split(".", 1)[0].upper()
            if windows_basename in _WINDOWS_RESERVED_ENTRY_NAMES:
                raise ValueError(
                    f"localfs child path uses a reserved Windows device name: {path!r}"
                )
    return path


def _validate_contained_path(
    path: pathlib.Path,
    containment_root: pathlib.Path,
    *,
    deleting: bool,
) -> None:
    """Fail closed if a child mutation could escape through a symlink.

    The public boundary validation protects newly declared keys.  This second
    check is intentionally performed immediately before I/O as defense in depth
    for persisted state and filesystem changes between reconciliation and apply.
    """
    root = containment_root.resolve(strict=False)
    if root.exists() and not root.is_dir():
        raise NotADirectoryError(f"localfs target root is not a directory: {root}")

    absolute_path = path.absolute()
    try:
        relative = absolute_path.relative_to(root)
    except ValueError as exc:
        raise ValueError(
            f"localfs child path escapes its target directory: {path}"
        ) from exc

    current = root
    for part in relative.parts[:-1]:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"localfs child path traverses a symbolic link: {current}")

    # Removing a leaf symlink is safe: unlink() removes the directory entry and
    # never touches its referent.  Writes and directory creation must not follow
    # a leaf symlink, even when it currently points back inside the target root.
    if path.is_symlink():
        if deleting:
            return
        raise ValueError(f"localfs refuses to write through a symbolic link: {path}")

    resolved = path.resolve(strict=False)
    if not resolved.is_relative_to(root):
        raise ValueError(f"localfs child path escapes its target directory: {path}")


_SECURE_DIR_FD_IO = (
    os.name == "posix"
    and hasattr(os, "O_DIRECTORY")
    and hasattr(os, "O_NOFOLLOW")
    and all(
        fn in os.supports_dir_fd
        for fn in (os.open, os.mkdir, os.stat, os.unlink, os.rename)
    )
    and os.stat in os.supports_follow_symlinks
    and shutil.rmtree.avoids_symlink_attacks
)

# Windows does not expose openat-style ``dir_fd`` operations through Python,
# but its native handles can provide the same safety properties.  The Windows
# path below pins every existing ancestor without delete sharing, opens reparse
# points themselves instead of following them, and performs deletion through
# the opened handle.  This keeps managed children usable on every platform the
# package supports without falling back to check-then-use pathname I/O.
_SECURE_WINDOWS_IO = os.name == "nt"

_WIN_DELETE = 0x00010000
_WIN_FILE_READ_ATTRIBUTES = 0x00000080
_WIN_GENERIC_WRITE = 0x40000000
_WIN_FILE_SHARE_READ = 0x00000001
_WIN_FILE_SHARE_WRITE = 0x00000002
_WIN_CREATE_NEW = 1
_WIN_OPEN_EXISTING = 3
_WIN_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_WIN_FILE_ATTRIBUTE_NORMAL = 0x00000080
_WIN_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_WIN_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_WIN_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_WIN_FILE_FLAG_WRITE_THROUGH = 0x80000000
_WIN_FILE_RENAME_INFO = 3
_WIN_FILE_DISPOSITION_INFO = 4
_WIN_O_BINARY = 0x8000
_WIN_O_NOINHERIT = 0x0080
_WIN_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
_windows_kernel32_dll: Any | None = None


class _WindowsByHandleFileInformation(ctypes.Structure):
    _fields_ = [
        ("dwFileAttributes", ctypes.wintypes.DWORD),
        ("ftCreationTime", ctypes.wintypes.FILETIME),
        ("ftLastAccessTime", ctypes.wintypes.FILETIME),
        ("ftLastWriteTime", ctypes.wintypes.FILETIME),
        ("dwVolumeSerialNumber", ctypes.wintypes.DWORD),
        ("nFileSizeHigh", ctypes.wintypes.DWORD),
        ("nFileSizeLow", ctypes.wintypes.DWORD),
        ("nNumberOfLinks", ctypes.wintypes.DWORD),
        ("nFileIndexHigh", ctypes.wintypes.DWORD),
        ("nFileIndexLow", ctypes.wintypes.DWORD),
    ]


class _WindowsFileDispositionInformation(ctypes.Structure):
    # FILE_DISPOSITION_INFO uses the one-byte NT BOOLEAN type, not the
    # four-byte Win32 BOOL type exposed as ctypes.wintypes.BOOL.
    _fields_ = [("DeleteFile", ctypes.c_ubyte)]


class _WindowsFileRenameInformation(ctypes.Structure):
    """Fixed header and first code unit of Win32 FILE_RENAME_INFO."""

    _fields_ = [
        ("ReplaceIfExists", ctypes.c_ubyte),
        ("RootDirectory", ctypes.c_void_p),
        ("FileNameLength", ctypes.c_uint32),
        ("FileName", ctypes.c_uint16 * 1),
    ]


def _windows_kernel32() -> Any:
    """Return configured kernel32 bindings, importing no Windows-only module."""
    global _windows_kernel32_dll
    if not _SECURE_WINDOWS_IO:
        raise RuntimeError("Windows filesystem handles are unavailable")
    if _windows_kernel32_dll is not None:
        return _windows_kernel32_dll

    win_dll = ctypes.WinDLL  # type: ignore[attr-defined]
    kernel32 = win_dll("kernel32", use_last_error=True)
    kernel32.CreateFileW.argtypes = [
        ctypes.wintypes.LPCWSTR,
        ctypes.wintypes.DWORD,
        ctypes.wintypes.DWORD,
        ctypes.c_void_p,
        ctypes.wintypes.DWORD,
        ctypes.wintypes.DWORD,
        ctypes.wintypes.HANDLE,
    ]
    kernel32.CreateFileW.restype = ctypes.wintypes.HANDLE
    kernel32.GetFileInformationByHandle.argtypes = [
        ctypes.wintypes.HANDLE,
        ctypes.POINTER(_WindowsByHandleFileInformation),
    ]
    kernel32.GetFileInformationByHandle.restype = ctypes.wintypes.BOOL
    kernel32.SetFileInformationByHandle.argtypes = [
        ctypes.wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.wintypes.DWORD,
    ]
    kernel32.SetFileInformationByHandle.restype = ctypes.wintypes.BOOL
    kernel32.CloseHandle.argtypes = [ctypes.wintypes.HANDLE]
    kernel32.CloseHandle.restype = ctypes.wintypes.BOOL
    _windows_kernel32_dll = kernel32
    return kernel32


def _windows_last_error() -> OSError:
    get_last_error = ctypes.get_last_error  # type: ignore[attr-defined]
    return cast(OSError, ctypes.WinError(get_last_error()))  # type: ignore[attr-defined]


def _windows_close_handle(handle: int) -> None:
    if not _windows_kernel32().CloseHandle(handle):
        raise _windows_last_error()


def _windows_open_handle(
    path: pathlib.Path,
    *,
    access: int = _WIN_FILE_READ_ATTRIBUTES,
    creation: int = _WIN_OPEN_EXISTING,
    flags: int = _WIN_FILE_FLAG_OPEN_REPARSE_POINT | _WIN_FILE_FLAG_BACKUP_SEMANTICS,
    share: int = _WIN_FILE_SHARE_READ | _WIN_FILE_SHARE_WRITE,
) -> int:
    """Open a path itself and deny concurrent rename/delete while it is held."""
    handle = _windows_kernel32().CreateFileW(
        str(path),
        access,
        share,
        None,
        creation,
        flags,
        None,
    )
    if handle == _WIN_INVALID_HANDLE_VALUE:
        raise _windows_last_error()
    return cast(int, handle)


def _windows_handle_attributes(handle: int) -> int:
    info = _WindowsByHandleFileInformation()
    if not _windows_kernel32().GetFileInformationByHandle(handle, ctypes.byref(info)):
        raise _windows_last_error()
    return int(info.dwFileAttributes)


def _windows_mark_handle_for_deletion(handle: int) -> None:
    disposition = _WindowsFileDispositionInformation(True)
    if not _windows_kernel32().SetFileInformationByHandle(
        handle,
        _WIN_FILE_DISPOSITION_INFO,
        ctypes.byref(disposition),
        ctypes.sizeof(disposition),
    ):
        raise _windows_last_error()


def _windows_rename_handle(handle: int, name: str) -> None:
    """Atomically rename an open file within its current directory.

    Keeping the source handle open removes the pathname race that would exist
    between closing a completed temporary file and calling ``MoveFileExW``.
    A simple name with a null ``RootDirectory`` tells Windows to rename within
    the source handle's current directory.
    """
    encoded_name = name.encode("utf-16-le", errors="surrogatepass")
    buffer_size = ctypes.sizeof(_WindowsFileRenameInformation) + len(encoded_name)
    buffer = ctypes.create_string_buffer(buffer_size)
    information = ctypes.cast(
        buffer, ctypes.POINTER(_WindowsFileRenameInformation)
    ).contents
    information.ReplaceIfExists = True
    information.RootDirectory = None
    information.FileNameLength = len(encoded_name)
    ctypes.memmove(
        ctypes.addressof(buffer) + _WindowsFileRenameInformation.FileName.offset,
        encoded_name,
        len(encoded_name),
    )
    if not _windows_kernel32().SetFileInformationByHandle(
        handle,
        _WIN_FILE_RENAME_INFO,
        buffer,
        buffer_size,
    ):
        raise _windows_last_error()


def _windows_open_real_directory(path: pathlib.Path) -> int:
    # Denying write and delete sharing prevents a verified empty directory from
    # being converted into a reparse point, renamed, or removed while a child
    # pathname is resolved through it. Read sharing retains normal enumeration.
    handle = _windows_open_handle(path, share=_WIN_FILE_SHARE_READ)
    try:
        attributes = _windows_handle_attributes(handle)
        if attributes & _WIN_FILE_ATTRIBUTE_REPARSE_POINT:
            raise ValueError(
                f"localfs child path traverses a Windows reparse point: {path}"
            )
        if not attributes & _WIN_FILE_ATTRIBUTE_DIRECTORY:
            raise NotADirectoryError(
                f"localfs target parent is not a directory: {path}"
            )
        return handle
    except BaseException:
        _windows_close_handle(handle)
        raise


def _windows_pin_parent_chain(
    root: pathlib.Path,
    parent_parts: tuple[str, ...],
    *,
    create_parents: bool,
) -> tuple[list[int], pathlib.Path]:
    """Pin every replaceable absolute-path prefix and managed child ancestor.

    ``FILE_FLAG_OPEN_REPARSE_POINT`` applies only to the final component of a
    ``CreateFileW`` path. Opening just ``root`` would therefore still follow a
    junction or symlink in one of its parent components. Walk from the volume or
    UNC-share anchor so every component is inspected as the final component of
    one open call and remains pinned for the mutation. Drive and UNC-share
    anchors are namespace roots rather than replaceable path components, so
    they only need a handle when the anchor itself is the containment root.
    """
    handles: list[int] = []
    absolute_root = pathlib.Path(os.path.abspath(root))
    if not absolute_root.anchor:
        raise ValueError(f"localfs target root must be absolute: {root}")
    current = pathlib.Path(absolute_root.anchor)
    try:
        root_parts = absolute_root.relative_to(current).parts
        for part in root_parts:
            current /= part
            handles.append(_windows_open_real_directory(current))
        if not root_parts:
            # A drive or UNC-share root is itself the containment root.
            handles.append(_windows_open_real_directory(current))
        for part in parent_parts:
            current /= part
            try:
                handle = _windows_open_real_directory(current)
            except FileNotFoundError:
                if not create_parents:
                    raise
                try:
                    os.mkdir(current)
                except FileExistsError:
                    pass
                handle = _windows_open_real_directory(current)
            handles.append(handle)
        return handles, current
    except BaseException:
        for handle in reversed(handles):
            _windows_close_handle(handle)
        raise


def _windows_atomic_write(parent: pathlib.Path, name: str, content: bytes) -> None:
    """Write and atomically install a regular file under a pinned parent."""
    target = parent / name
    try:
        existing_handle = _windows_open_handle(target)
    except FileNotFoundError:
        existing_handle = None
    if existing_handle is not None:
        try:
            attributes = _windows_handle_attributes(existing_handle)
            if attributes & _WIN_FILE_ATTRIBUTE_REPARSE_POINT:
                raise ValueError(
                    f"localfs refuses to write through a Windows reparse point: {target}"
                )
            if attributes & _WIN_FILE_ATTRIBUTE_DIRECTORY:
                raise FileExistsError(
                    f"localfs file target is not a regular file: {target}"
                )
        finally:
            _windows_close_handle(existing_handle)

    temporary_path: pathlib.Path | None = None
    temporary_handle: int | None = None
    for _ in range(32):
        candidate = parent / f".synor-{secrets.token_hex(16)}.tmp"
        try:
            temporary_handle = _windows_open_handle(
                candidate,
                access=_WIN_GENERIC_WRITE | _WIN_DELETE,
                creation=_WIN_CREATE_NEW,
                flags=_WIN_FILE_ATTRIBUTE_NORMAL | _WIN_FILE_FLAG_WRITE_THROUGH,
                share=0,
            )
        except FileExistsError:
            continue
        temporary_path = candidate
        break
    if temporary_path is None or temporary_handle is None:
        raise FileExistsError("could not allocate a temporary localfs target file")

    fd: int | None = None
    transferred_handle: int | None = None
    try:
        import msvcrt

        fd = msvcrt.open_osfhandle(  # type: ignore[attr-defined]
            temporary_handle,
            os.O_WRONLY | _WIN_O_BINARY | _WIN_O_NOINHERIT,
        )
        transferred_handle = temporary_handle
        temporary_handle = None
        try:
            output = os.fdopen(fd, "wb")
        except BaseException:
            _windows_mark_handle_for_deletion(transferred_handle)
            raise
        with output:
            fd = None
            try:
                output.write(content)
                output.flush()
                os.fsync(output.fileno())
                _windows_rename_handle(transferred_handle, name)
            except BaseException:
                # The CRT owns the handle now, so schedule deletion while it is
                # still open. Closing ``output`` then removes exactly this temp
                # object without trusting its pathname again.
                _windows_mark_handle_for_deletion(transferred_handle)
                raise
    finally:
        if fd is not None:
            os.close(fd)
        if temporary_handle is not None:
            _windows_close_handle(temporary_handle)


def _windows_delete_tree_by_handle(
    path: pathlib.Path, expected_type: Literal["file", "dir"] | None
) -> None:
    """Delete one entry without following file symlinks or directory junctions."""
    try:
        handle = _windows_open_handle(
            path,
            access=_WIN_FILE_READ_ATTRIBUTES | _WIN_DELETE,
            share=_WIN_FILE_SHARE_READ,
        )
    except FileNotFoundError:
        return
    try:
        attributes = _windows_handle_attributes(handle)
        is_reparse = bool(attributes & _WIN_FILE_ATTRIBUTE_REPARSE_POINT)
        is_directory = bool(attributes & _WIN_FILE_ATTRIBUTE_DIRECTORY)
        if not is_reparse:
            if expected_type == "file" and is_directory:
                raise ValueError(
                    f"localfs refuses to delete directory tracked as file: {path}"
                )
            if expected_type == "dir" and not is_directory:
                raise ValueError(
                    f"localfs refuses to delete file tracked as directory: {path}"
                )
            if is_directory:
                with os.scandir(path) as entries:
                    child_names = [entry.name for entry in entries]
                for child_name in child_names:
                    _windows_delete_tree_by_handle(path / child_name, None)
        # A reparse point is always deleted as the leaf entry itself. For a
        # real directory this succeeds only after every child is gone; a
        # concurrent creator therefore causes a safe failure, never traversal.
        _windows_mark_handle_for_deletion(handle)
    finally:
        _windows_close_handle(handle)


def _execute_contained_entry_action_windows(
    path: pathlib.Path,
    action: _EntryAction,
    containment_root: pathlib.Path,
) -> pathlib.Path | None:
    root, absolute_path, parts = _contained_relative_parts(path, containment_root)
    if not parts:
        _validate_contained_path(path, root, deleting=action.content is None)
        return _execute_entry_action_uncontained(path, action)

    handles, parent = _windows_pin_parent_chain(
        root,
        parts[:-1],
        create_parents=action.create_parents and action.content is not None,
    )
    try:
        leaf = parent / parts[-1]
        if action.content is None:
            _windows_delete_tree_by_handle(leaf, action.entry_type)
            return None
        if action.entry_type == "file":
            _windows_atomic_write(parent, parts[-1], action.content)
            return None

        try:
            os.mkdir(leaf)
        except FileExistsError:
            pass
        child_handle = _windows_open_real_directory(leaf)
        _windows_close_handle(child_handle)
        return absolute_path
    finally:
        for handle in reversed(handles):
            _windows_close_handle(handle)


def _contained_relative_parts(
    path: pathlib.Path, containment_root: pathlib.Path
) -> tuple[pathlib.Path, pathlib.Path, tuple[str, ...]]:
    """Return normalized lexical paths and a validated relative component list."""
    if ".." in path.parts:
        raise ValueError(
            f"localfs child path must not contain traversal segments: {path}"
        )
    root = pathlib.Path(os.path.abspath(containment_root))
    absolute_path = pathlib.Path(os.path.abspath(path))
    try:
        relative = absolute_path.relative_to(root)
    except ValueError as exc:
        raise ValueError(
            f"localfs child path escapes its target directory: {path}"
        ) from exc

    parts = relative.parts
    if parts:
        _validate_child_path(pathlib.PurePosixPath(*parts).as_posix())
    return root, absolute_path, parts


def _open_directory_no_follow(
    path: str | pathlib.Path, *, dir_fd: int | None = None
) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    flags |= getattr(os, "O_CLOEXEC", 0)
    try:
        return os.open(path, flags, dir_fd=dir_fd)
    except OSError as exc:
        try:
            info = os.stat(path, dir_fd=dir_fd, follow_symlinks=False)
        except OSError:
            raise exc
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(
                f"localfs child path traverses a symbolic link: {path}"
            ) from exc
        raise


def _open_absolute_directory_no_follow(path: pathlib.Path) -> int:
    """Open an absolute directory by pinning every component from its anchor.

    Passing an absolute path directly to ``open(..., O_NOFOLLOW)`` protects only
    the final component.  Any earlier component can still be exchanged for a
    symlink before the kernel resolves it.  Walking relative to an already-open
    parent descriptor makes each successfully opened component the authority
    for resolving the next one.
    """
    if not path.is_absolute() or not path.anchor:
        raise ValueError(f"localfs containment root must be absolute: {path}")

    current_fd = _open_directory_no_follow(path.anchor)
    try:
        for part in path.parts[1:]:
            next_fd = _open_directory_no_follow(part, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
        return current_fd
    except BaseException:
        os.close(current_fd)
        raise


def _open_contained_parent(
    root: pathlib.Path,
    parent_parts: tuple[str, ...],
    *,
    create_parents: bool,
) -> int:
    """Open a child parent directory without ever resolving an untrusted path."""
    current_fd = _open_absolute_directory_no_follow(root)
    try:
        for part in parent_parts:
            try:
                next_fd = _open_directory_no_follow(part, dir_fd=current_fd)
            except FileNotFoundError:
                if not create_parents:
                    raise
                try:
                    os.mkdir(part, dir_fd=current_fd)
                except FileExistsError:
                    # A concurrent creator is fine only if it made a real
                    # directory. The no-follow open below performs that check.
                    pass
                next_fd = _open_directory_no_follow(part, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
        return current_fd
    except BaseException:
        os.close(current_fd)
        raise


def _lstat_at(parent_fd: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None


def _sync_directory(fd: int) -> None:
    """Make a completed entry mutation durable before reporting success."""
    os.fsync(fd)


def _atomic_write_at(parent_fd: int, name: str, content: bytes) -> None:
    """Atomically replace a regular file without following a symlink or hard link."""
    previous = _lstat_at(parent_fd, name)
    if previous is not None:
        if stat.S_ISLNK(previous.st_mode):
            raise ValueError(
                f"localfs refuses to write through a symbolic link: {name}"
            )
        if not stat.S_ISREG(previous.st_mode):
            raise FileExistsError(f"localfs file target is not a regular file: {name}")

    temporary_name: str | None = None
    temporary_fd: int | None = None
    for _ in range(32):
        candidate = f".synor-{secrets.token_hex(16)}.tmp"
        try:
            temporary_fd = os.open(
                candidate,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | os.O_NOFOLLOW
                | getattr(os, "O_CLOEXEC", 0),
                0o666,
                dir_fd=parent_fd,
            )
        except FileExistsError:
            continue
        temporary_name = candidate
        break
    if temporary_fd is None or temporary_name is None:
        raise FileExistsError("could not allocate a temporary localfs target file")

    installed = False
    try:
        if previous is not None:
            os.fchmod(temporary_fd, stat.S_IMODE(previous.st_mode))
        with os.fdopen(temporary_fd, "wb") as output:
            temporary_fd = None
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.rename(
            temporary_name,
            name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        installed = True
        _sync_directory(parent_fd)
    finally:
        if temporary_fd is not None:
            os.close(temporary_fd)
        if not installed:
            try:
                os.unlink(temporary_name, dir_fd=parent_fd)
            except FileNotFoundError:
                pass


def _execute_contained_entry_action(
    path: pathlib.Path,
    action: _EntryAction,
    containment_root: pathlib.Path,
) -> pathlib.Path | None:
    """Apply an action with descriptor-relative, no-follow POSIX I/O."""
    root, absolute_path, parts = _contained_relative_parts(path, containment_root)
    if not parts:
        # A ContextKey can identify the managed directory itself using ``.``.
        # This is root setup, not a child-key mutation, so retain root behavior.
        _validate_contained_path(path, root, deleting=action.content is None)
        return _execute_entry_action_uncontained(path, action)

    parent_fd = _open_contained_parent(
        root,
        parts[:-1],
        create_parents=action.create_parents and action.content is not None,
    )
    name = parts[-1]
    try:
        previous = _lstat_at(parent_fd, name)
        if action.content is None:
            if previous is None:
                return None
            if stat.S_ISLNK(previous.st_mode) or (
                action.entry_type == "file" and stat.S_ISREG(previous.st_mode)
            ):
                os.unlink(name, dir_fd=parent_fd)
            elif action.entry_type == "dir" and stat.S_ISDIR(previous.st_mode):
                shutil.rmtree(name, dir_fd=parent_fd)
            else:
                raise ValueError(
                    "localfs refuses to delete an entry whose actual type "
                    f"does not match tracked type {action.entry_type!r}: {name}"
                )
            _sync_directory(parent_fd)
            return None

        if action.entry_type == "file":
            _atomic_write_at(parent_fd, name, action.content)
            return None

        if previous is None:
            os.mkdir(name, dir_fd=parent_fd)
        elif stat.S_ISLNK(previous.st_mode):
            raise ValueError(
                f"localfs refuses to create a directory through a symbolic link: {name}"
            )
        elif not stat.S_ISDIR(previous.st_mode):
            raise FileExistsError(
                f"localfs directory target is not a directory: {name}"
            )
        child_fd = _open_directory_no_follow(name, dir_fd=parent_fd)
        os.close(child_fd)
        _sync_directory(parent_fd)
        return absolute_path
    finally:
        os.close(parent_fd)


def _write_bytes_without_following_symlinks(path: pathlib.Path, content: bytes) -> None:
    """Write bytes while refusing to follow a leaf symlink where supported."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o666)
    try:
        with os.fdopen(fd, "wb") as output:
            fd = -1
            output.write(content)
    finally:
        if fd >= 0:
            os.close(fd)


def _execute_entry_action_uncontained(
    path: pathlib.Path, action: _EntryAction
) -> pathlib.Path | None:
    """Apply an explicit root action or the guarded non-POSIX fallback."""
    if action.content is None:
        if path.is_symlink():
            path.unlink(missing_ok=True)
            return None
        if not path.exists():
            return None
        if action.entry_type == "file":
            if not path.is_file():
                raise ValueError(
                    "localfs refuses to delete an entry whose actual type "
                    f"does not match tracked type 'file': {path}"
                )
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
        else:
            raise ValueError(
                "localfs refuses to delete an entry whose actual type "
                f"does not match tracked type 'dir': {path}"
            )
        return None

    if action.entry_type == "file":
        if action.create_parents:
            path.parent.mkdir(parents=True, exist_ok=True)
        _write_bytes_without_following_symlinks(path, action.content)
        return None

    if action.create_parents:
        path.mkdir(parents=True, exist_ok=True)
    else:
        path.mkdir(exist_ok=True)
    return path.resolve(strict=True)


def _execute_entry_action(
    path: pathlib.Path,
    action: _EntryAction,
    *,
    containment_root: pathlib.Path | None = None,
) -> pathlib.Path | None:
    """
    Execute a single entry action.

    Returns the path for directories (to create child handler), None otherwise.
    """
    root = containment_root
    if root is None and action.containment_root is not None:
        root = pathlib.Path(action.containment_root)
    if root is not None:
        if _SECURE_DIR_FD_IO:
            return _execute_contained_entry_action(path, action, root)
        if _SECURE_WINDOWS_IO:
            return _execute_contained_entry_action_windows(path, action, root)
        # A pathname check followed by pathname I/O is inherently racy. Fail
        # closed on an unknown platform instead of claiming containment.
        raise RuntimeError(
            "secure localfs child mutations require descriptor-relative or "
            "pinned-handle no-follow filesystem operations on this platform"
        )
    return _execute_entry_action_uncontained(path, action)


def _apply_actions_with_child(
    context_provider: ContextProvider,
    actions: Sequence[_EntryAction],
    /,
) -> list[syn.ChildTargetDef["_EntryHandler"] | None]:
    """Apply actions and return child handlers for directories."""
    outputs: list[syn.ChildTargetDef[_EntryHandler] | None] = []
    for action in actions:
        containment_root: pathlib.Path | None = None
        if action.base_dir_key is not None:
            _validate_child_path(
                action.path,
                allow_root=action.entry_type == "dir",
            )
            base = pathlib.Path(
                os.path.abspath(context_provider.get(action.base_dir_key, pathlib.Path))
            )
            path = base / action.path
            containment_root = base
        else:
            path = pathlib.Path(action.path)  # already absolute
        result_path = _execute_entry_action(
            path,
            action,
            containment_root=containment_root,
        )
        if result_path is not None:
            outputs.append(syn.ChildTargetDef(handler=_EntryHandler(result_path)))
        else:
            outputs.append(None)
    return outputs


# Shared action sink
_action_sink_with_child = syn.TargetActionSink["_EntryAction", "_EntryHandler"].from_fn(
    _apply_actions_with_child,
    capabilities=syn.TargetSinkCapabilities(
        batch_atomicity="none",
        apply_ordering="unordered",
    ),
)


def _reconcile_entry(
    base_dir_key: str | None,
    path_str: str,
    desired_state: _EntrySpec | syn.AbsentType,
    prev_possible_records: Collection[_EntryTrackingRecord],
    prev_may_be_missing: bool,
    containment_root: str | None = None,
) -> (
    syn.TargetReconcileOutput[_EntryAction, _EntryTrackingRecord, "_EntryHandler"]
    | None
):
    """Common reconcile logic for both root and non-root entries."""
    if syn.is_absent(desired_state):
        # Determine entry type from previous state (None fingerprint = dir)
        entry_type: Literal["file", "dir"] = "file"
        for prev in prev_possible_records:
            if prev.fingerprint is None:
                entry_type = "dir"
                break

        return syn.TargetReconcileOutput(
            action=_EntryAction(
                base_dir_key,
                path_str,
                entry_type,
                None,
                False,
                containment_root,
            ),
            sink=_action_sink_with_child,
            tracking_record=syn.ABSENT,
        )

    entry_spec = desired_state.entry_spec
    create_parents = desired_state.create_parent_dirs

    if isinstance(entry_spec, _DirSpec):
        # Directory entry (fingerprint=None means directory)
        return syn.TargetReconcileOutput(
            action=_EntryAction(
                base_dir_key,
                path_str,
                "dir",
                b"",
                create_parents,
                containment_root,
            ),
            sink=_action_sink_with_child,
            tracking_record=_EntryTrackingRecord(fingerprint=None),
        )

    # File entry
    target_fp = fingerprint_bytes(entry_spec)

    # Check if update needed
    if not prev_may_be_missing and all(
        prev.fingerprint == target_fp for prev in prev_possible_records
    ):
        return None

    return syn.TargetReconcileOutput(
        action=_EntryAction(
            base_dir_key,
            path_str,
            "file",
            entry_spec,
            create_parents,
            containment_root,
        ),
        sink=_action_sink_with_child,
        tracking_record=_EntryTrackingRecord(fingerprint=target_fp),
    )


# =============================================================================
# Entry handler (for non-root entries within a directory)
# =============================================================================


class _EntryTrackingRecord(msgspec.Struct, frozen=True):
    """Tracking record for an entry. If fingerprint is None, it's a directory."""

    fingerprint: _FileFingerprint | None


class _EntryHandler(
    syn.TargetHandler[_EntrySpec, _EntryTrackingRecord, "_EntryHandler"]
):
    """Handler for file and directory entries within a parent directory."""

    __slots__ = ("_base_path",)

    _base_path: pathlib.Path

    def __init__(self, base_path: pathlib.Path) -> None:
        # Keep the lexical managed path. Resolving here would turn a symlink
        # swapped in after directory creation into a trusted external root.
        self._base_path = pathlib.Path(os.path.abspath(base_path))

    def reconcile(
        self,
        key: syn.StableKey,
        desired_state: _EntrySpec | syn.AbsentType,
        prev_possible_records: Collection[_EntryTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        syn.TargetReconcileOutput[_EntryAction, _EntryTrackingRecord, "_EntryHandler"]
        | None
    ):
        key = _validate_child_path(_ENTRY_NAME_CHECKER.check(key))
        path = self._base_path / key
        return _reconcile_entry(
            None,
            str(path),
            desired_state,
            prev_possible_records,
            prev_may_be_missing,
            str(self._base_path),
        )


# =============================================================================
# Root-level types (shared key)
# =============================================================================


class _RootKey(NamedTuple):
    """Key for root-level entries: (base_dir_key, path_string)."""

    base_dir_key: str | None  # None for CWD
    path: str


_ROOT_KEY_CHECKER = TypeChecker(tuple[str | None, str])


def _get_base_dir_key(file_path: FilePath) -> str | None:
    """Get the base directory key, returning None for CWD."""
    base_dir = file_path.base_dir
    return base_dir.key if base_dir is not None else None


# =============================================================================
# Root handler (for root-level files and directories)
# =============================================================================


class _RootHandler(syn.TargetHandler[_EntrySpec, _EntryTrackingRecord, _EntryHandler]):
    """Handler for root-level entries (files and directories)."""

    def reconcile(
        self,
        key: syn.StableKey,
        desired_state: _EntrySpec | syn.AbsentType,
        prev_possible_records: Collection[_EntryTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        syn.TargetReconcileOutput[_EntryAction, _EntryTrackingRecord, _EntryHandler]
        | None
    ):
        root_key = _RootKey(*_ROOT_KEY_CHECKER.check(key))
        if root_key.base_dir_key is None:
            path_str = os.path.abspath(root_key.path)
        else:
            path_str = _validate_child_path(root_key.path, allow_root=True)
        return _reconcile_entry(
            root_key.base_dir_key,
            path_str,
            desired_state,
            prev_possible_records,
            prev_may_be_missing,
        )


# =============================================================================
# Register root provider
# =============================================================================

_root_provider = syn.register_root_target_states_provider(
    "synor/localfs", _RootHandler()
)


# =============================================================================
# Public API
# =============================================================================


class DirTarget(Generic[syn.MaybePendingS], syn.ResolvesTo["DirTarget"]):
    """
    A target for writing files and subdirectories to a local directory.

    The directory is managed as a target state, with automatic cleanup of
    files and directories that are no longer declared.
    """

    _provider: syn.TargetStateProvider[_EntrySpec, _EntryHandler, syn.MaybePendingS]

    def __init__(
        self,
        provider: syn.TargetStateProvider[_EntrySpec, _EntryHandler, syn.MaybePendingS],
    ) -> None:
        self._provider = provider

    def ensure_file(
        self: "DirTarget",
        filename: str | pathlib.PurePath,
        content: bytes | str,
        *,
        create_parent_dirs: bool = False,
    ) -> None:
        """
        Declare a file to be written to this directory.

        Args:
            filename: A relative file path within this directory. Nested paths are
                supported; absolute paths, traversal segments, NUL bytes, and
                ambiguous empty or ``.`` segments are rejected.
            content: The content of the file (bytes or str).
            create_parent_dirs: If True, create parent directories if they don't exist.
                Defaults to False.
        """
        if isinstance(content, str):
            content = content.encode()
        name = str(filename) if isinstance(filename, pathlib.PurePath) else filename
        name = _validate_child_path(name)
        spec = _EntrySpec(entry_spec=content, create_parent_dirs=create_parent_dirs)
        # Files don't have children, but the provider type allows for them (for directories).
        # Cast is safe since file entries never produce child handlers at runtime.
        target_state = cast(
            syn.TargetState[None], self._provider.target_state(name, spec)
        )
        syn.ensure_target_state(target_state)

    def ensure_dir_target(
        self: "DirTarget",
        path: str | pathlib.PurePath,
        *,
        create_parent_dirs: bool = False,
    ) -> "DirTarget[syn.PendingS]":
        """
        Declare a subdirectory target within this directory.

        Args:
            path: A relative subdirectory path within this directory. Nested paths
                are supported; absolute paths, traversal segments, NUL bytes, and
                ambiguous empty or ``.`` segments are rejected.
            create_parent_dirs: If True, create parent directories if they don't exist.
                Defaults to False.

        Returns:
            A DirTarget for the subdirectory.
        """
        name = str(path) if isinstance(path, pathlib.PurePath) else path
        name = _validate_child_path(name)
        spec = _EntrySpec(entry_spec=_DirSpec(), create_parent_dirs=create_parent_dirs)
        provider = syn.ensure_target_state_with_child(
            self._provider.target_state(name, spec)
        )
        return DirTarget(provider)

    def __synor_memo_key__(self) -> object:
        return self._provider.memo_key


@syn.task
def ensure_dir_target(
    path: FilePath | pathlib.Path | ContextKey[pathlib.Path],
    *,
    create_parent_dirs: bool = True,
) -> DirTarget[syn.PendingS]:
    """
    Declare a directory target for writing files.

    Args:
        path: The filesystem path for the directory. Can be a FilePath, a
            pathlib.Path (uses CWD as base directory), or a ContextKey[Path]
            (equivalent to FilePath(base_dir=path)).
        create_parent_dirs: If True, create parent directories if they don't exist.
            Defaults to True.

    Returns:
        A DirTarget that can be used to declare files and subdirectories.

    Example:
        ```python
        target = syn.call(
            syn.unit_path("setup"),
            localfs.declare_dir_target,
            Path("./output"),
        )

        target.declare_file("hello.txt", content="Hello, world!")
        ```
    """
    provider = syn.ensure_target_state_with_child(
        dir_target(path, create_parent_dirs=create_parent_dirs)
    )
    return DirTarget(provider)


def dir_target(
    path: FilePath | pathlib.Path | ContextKey[pathlib.Path],
    *,
    create_parent_dirs: bool = True,
) -> syn.TargetState[_EntryHandler]:
    """
    Create a TargetState for a local directory target.

    Use with ``syn.attach_target()`` to mount and get a child provider,
    or with ``mount_dir_target()`` for a convenience wrapper.

    Args:
        path: The filesystem path for the directory. Can be a FilePath, a
            pathlib.Path (uses CWD as base directory), or a ContextKey[Path]
            (equivalent to FilePath(base_dir=path)).
        create_parent_dirs: If True, create parent directories if they don't exist.
            Defaults to True.

    Returns:
        A TargetState that can be passed to ``mount_target()``.
    """
    file_path = to_file_path(path)
    if file_path.base_dir is not None:
        _validate_child_path(file_path.path.as_posix(), allow_root=True)
    key = _RootKey(
        base_dir_key=_get_base_dir_key(file_path),
        path=file_path.path.as_posix(),
    )
    spec = _EntrySpec(
        entry_spec=_DirSpec(),
        create_parent_dirs=create_parent_dirs,
    )
    return _root_provider.target_state(key, spec)


async def mount_dir_target(
    path: FilePath | pathlib.Path | ContextKey[pathlib.Path],
    *,
    create_parent_dirs: bool = True,
) -> DirTarget[syn.ResolvedS]:
    """
    Mount a directory target and return a ready-to-use DirTarget.

    Sugar over ``dir_target()`` + ``syn.attach_target()`` + wrapping.

    Args:
        path: The filesystem path for the directory. Can be a FilePath, a
            pathlib.Path (uses CWD as base directory), or a ContextKey[Path]
            (equivalent to FilePath(base_dir=path)).
        create_parent_dirs: If True, create parent directories if they don't exist.
            Defaults to True.

    Returns:
        A DirTarget that can be used to declare files and subdirectories.
    """
    provider = await syn.attach_target(
        dir_target(path, create_parent_dirs=create_parent_dirs)
    )
    return DirTarget(provider)


@syn.task
def ensure_file(
    path: FilePath | pathlib.Path | ContextKey[pathlib.Path],
    content: bytes | str,
    *,
    create_parent_dirs: bool = False,
) -> None:
    """
    Declare a single file target.

    This is a convenience function for declaring a single file without
    first creating a directory target.

    Args:
        path: The filesystem path for the file. Can be a FilePath, a
            pathlib.Path (uses CWD as base directory), or a ContextKey[Path]
            (equivalent to FilePath(base_dir=path)).
        content: The content of the file (bytes or str).
        create_parent_dirs: If True, create parent directories if they don't exist.
            Defaults to False.

    Example:
        ```python
        syn.spawn(
            syn.unit_path("output"),
            localfs.declare_file,
            Path("./output/hello.txt"),
            content="Hello, world!",
            create_parent_dirs=True,
        )
        ```
    """
    if isinstance(content, str):
        content = content.encode()

    file_path = to_file_path(path)
    if file_path.base_dir is not None:
        _validate_child_path(file_path.path.as_posix())
    key = _RootKey(
        base_dir_key=_get_base_dir_key(file_path),
        path=file_path.path.as_posix(),
    )
    spec = _EntrySpec(
        entry_spec=content,
        create_parent_dirs=create_parent_dirs,
    )
    # Files don't have children, but the provider type allows for them (for directories).
    # Cast is safe since file entries never produce child handlers at runtime.
    target_state = cast(syn.TargetState[None], _root_provider.target_state(key, spec))
    syn.ensure_target_state(target_state)


__all__ = [
    "DirTarget",
    "ensure_dir_target",
    "ensure_file",
    "dir_target",
    "mount_dir_target",
]
