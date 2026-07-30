"""Pluggable and optionally encrypted control-plane state stores."""

from __future__ import annotations

import asyncio as _asyncio
import base64 as _base64
import hashlib as _hashlib
import hmac as _hmac
import json as _json
import os as _os
import pathlib as _pathlib
import threading as _threading
import typing as _typing
import uuid as _uuid

from cryptography.exceptions import InvalidTag as _InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM as _AESGCM

__all__ = [
    "EncryptedStateStore",
    "FileStateStore",
    "MemoryStateStore",
    "StateDecryptionError",
    "StateStore",
    "decode_state_key",
    "generate_state_key",
    "state_store_from_env",
]

_ENCRYPTED_MAGIC = b"SYNOR-STATE\x01"
_AAD_PREFIX = b"synor-control-state-v1:"


@_typing.runtime_checkable
class StateStore(_typing.Protocol):
    """Async byte store used by Synor's control plane.

    Keys are slash-separated logical identifiers. Implementations must replace
    one value atomically, return ``None`` for a missing key, and return sorted
    keys from :meth:`list`.
    """

    async def get(self, key: str) -> bytes | None:
        """Read one value."""

    async def put(self, key: str, value: bytes) -> None:
        """Atomically create or replace one value."""

    async def delete(self, key: str) -> bool:
        """Delete one value and report whether it existed."""

    async def list(self, prefix: str = "") -> tuple[str, ...]:
        """List logical keys beginning with ``prefix``."""


def _validate_key(key: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    if not key:
        if allow_empty:
            return ()
        raise ValueError("state key must not be empty")
    path = _pathlib.PurePosixPath(key)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"invalid state key: {key!r}")
    if "\\" in key:
        raise ValueError("state keys must use '/' separators")
    return path.parts


class FileStateStore:
    """Atomic filesystem-backed state store."""

    def __init__(self, root: _os.PathLike[str] | str) -> None:
        self.root = _pathlib.Path(root)
        self._lock = _threading.RLock()

    def _path(self, key: str) -> _pathlib.Path:
        return self.root.joinpath(*_validate_key(key))

    def _get_sync(self, key: str) -> bytes | None:
        path = self._path(key)
        try:
            return path.read_bytes()
        except FileNotFoundError:
            return None

    async def get(self, key: str) -> bytes | None:
        return await _asyncio.to_thread(self._get_sync, key)

    def _put_sync(self, key: str, value: bytes) -> None:
        path = self._path(key)
        with self._lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{path.name}.{_uuid.uuid4().hex}.tmp")
            try:
                temporary.write_bytes(value)
                try:
                    temporary.chmod(0o600)
                except OSError:
                    pass
                _os.replace(temporary, path)
            finally:
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass

    async def put(self, key: str, value: bytes) -> None:
        await _asyncio.to_thread(self._put_sync, key, bytes(value))

    def _delete_sync(self, key: str) -> bool:
        path = self._path(key)
        with self._lock:
            try:
                path.unlink()
            except FileNotFoundError:
                return False
            parent = path.parent
            while parent != self.root:
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = parent.parent
            return True

    async def delete(self, key: str) -> bool:
        return await _asyncio.to_thread(self._delete_sync, key)

    def _list_sync(self, prefix: str) -> tuple[str, ...]:
        _validate_key(prefix, allow_empty=True)
        if not self.root.is_dir():
            return ()
        keys = (
            path.relative_to(self.root).as_posix()
            for path in self.root.rglob("*")
            if path.is_file()
            and not (path.name.startswith(".") and path.name.endswith(".tmp"))
        )
        return tuple(sorted(key for key in keys if key.startswith(prefix)))

    async def list(self, prefix: str = "") -> tuple[str, ...]:
        return await _asyncio.to_thread(self._list_sync, prefix)


class MemoryStateStore:
    """In-memory state store for tests and ephemeral runs."""

    def __init__(self) -> None:
        self._items: dict[str, bytes] = {}
        self._lock = _threading.RLock()

    async def get(self, key: str) -> bytes | None:
        _validate_key(key)
        with self._lock:
            value = self._items.get(key)
            return bytes(value) if value is not None else None

    async def put(self, key: str, value: bytes) -> None:
        _validate_key(key)
        with self._lock:
            self._items[key] = bytes(value)

    async def delete(self, key: str) -> bool:
        _validate_key(key)
        with self._lock:
            return self._items.pop(key, None) is not None

    async def list(self, prefix: str = "") -> tuple[str, ...]:
        _validate_key(prefix, allow_empty=True)
        with self._lock:
            return tuple(sorted(key for key in self._items if key.startswith(prefix)))


class StateDecryptionError(ValueError):
    """Raised when encrypted state is corrupt or the key is wrong."""


def generate_state_key() -> str:
    """Generate a URL-safe base64 AES-256 key suitable for ``SYNOR_STATE_KEY``."""

    return _base64.urlsafe_b64encode(_os.urandom(32)).decode("ascii")


def decode_state_key(value: str) -> bytes:
    """Decode a URL-safe base64 or 64-character hexadecimal AES-256 key."""

    text = value.strip()
    if len(text) == 64:
        try:
            raw = bytes.fromhex(text)
        except ValueError:
            raw = b""
        if len(raw) == 32:
            return raw
    try:
        raw = _base64.urlsafe_b64decode(text.encode("ascii"))
    except (ValueError, UnicodeEncodeError) as error:
        raise ValueError("state key must be URL-safe base64 or hexadecimal") from error
    if len(raw) != 32:
        raise ValueError("state key must decode to exactly 32 bytes")
    return raw


class EncryptedStateStore:
    """AES-256-GCM wrapper that also hides logical keys with HMAC-SHA-256."""

    def __init__(self, store: StateStore, key: bytes) -> None:
        if len(key) != 32:
            raise ValueError("encrypted state requires a 32-byte key")
        self._store = store
        self._key = bytes(key)
        self._cipher = _AESGCM(self._key)

    def _physical_key(self, key: str) -> str:
        _validate_key(key)
        digest = _hmac.new(self._key, key.encode("utf-8"), _hashlib.sha256).hexdigest()
        return f"encrypted/{digest[:2]}/{digest}.bin"

    def _encrypt(self, key: str, value: bytes) -> bytes:
        physical = self._physical_key(key)
        envelope = _json.dumps(
            {
                "key": key,
                "value": _base64.b64encode(value).decode("ascii"),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        nonce = _os.urandom(12)
        ciphertext = self._cipher.encrypt(
            nonce,
            envelope,
            _AAD_PREFIX + physical.encode("ascii"),
        )
        return _ENCRYPTED_MAGIC + nonce + ciphertext

    def _decrypt(self, physical: str, payload: bytes) -> tuple[str, bytes]:
        if (
            not payload.startswith(_ENCRYPTED_MAGIC)
            or len(payload) < len(_ENCRYPTED_MAGIC) + 13
        ):
            raise StateDecryptionError("encrypted state has an invalid header")
        offset = len(_ENCRYPTED_MAGIC)
        nonce = payload[offset : offset + 12]
        try:
            plaintext = self._cipher.decrypt(
                nonce,
                payload[offset + 12 :],
                _AAD_PREFIX + physical.encode("ascii"),
            )
            envelope = _json.loads(plaintext)
            key = envelope["key"]
            value = _base64.b64decode(envelope["value"], validate=True)
        except (
            _InvalidTag,
            KeyError,
            TypeError,
            ValueError,
            _json.JSONDecodeError,
        ) as error:
            raise StateDecryptionError(
                "encrypted state authentication failed; the key may be wrong"
            ) from error
        if not isinstance(key, str):
            raise StateDecryptionError(
                "encrypted state contains an invalid logical key"
            )
        return key, value

    async def get(self, key: str) -> bytes | None:
        physical = self._physical_key(key)
        payload = await self._store.get(physical)
        if payload is None:
            return None
        stored_key, value = self._decrypt(physical, payload)
        if not _hmac.compare_digest(stored_key, key):
            raise StateDecryptionError("encrypted state logical key mismatch")
        return value

    async def put(self, key: str, value: bytes) -> None:
        physical = self._physical_key(key)
        await self._store.put(physical, self._encrypt(key, bytes(value)))

    async def delete(self, key: str) -> bool:
        return await self._store.delete(self._physical_key(key))

    async def list(self, prefix: str = "") -> tuple[str, ...]:
        _validate_key(prefix, allow_empty=True)
        logical: list[str] = []
        for physical in await self._store.list("encrypted/"):
            payload = await self._store.get(physical)
            if payload is None:
                continue
            key, _value = self._decrypt(physical, payload)
            if key.startswith(prefix):
                logical.append(key)
        return tuple(sorted(logical))


def state_store_from_env() -> StateStore:
    """Build a control-plane store from local environment configuration.

    ``SYNOR_STATE_STORE`` accepts ``file://PATH`` (the default is
    ``file://.synor/control``) or ``memory://``. If ``SYNOR_STATE_KEY`` is set,
    the resulting store is wrapped with authenticated encryption.
    """

    location = _os.getenv("SYNOR_STATE_STORE", "file://.synor/control").strip()
    if location == "memory://":
        store: StateStore = MemoryStateStore()
    elif location.startswith("file://"):
        path = location.removeprefix("file://")
        if not path:
            raise ValueError("file state store requires a path")
        store = FileStateStore(path)
    else:
        raise ValueError("SYNOR_STATE_STORE must use file:// or memory://")
    key_text = _os.getenv("SYNOR_STATE_KEY")
    if key_text:
        store = EncryptedStateStore(store, decode_state_key(key_text))
    return store
