from __future__ import annotations

import pathlib

import pytest

import synor as syn


@pytest.mark.asyncio
async def test_file_and_memory_state_store_contract(tmp_path: pathlib.Path) -> None:
    for store in (syn.FileStateStore(tmp_path / "state"), syn.MemoryStateStore()):
        assert await store.get("runs/missing.json") is None
        await store.put("runs/one.json", b"one")
        await store.put("runs/two.json", b"two")
        assert await store.get("runs/one.json") == b"one"
        assert await store.list("runs/") == ("runs/one.json", "runs/two.json")
        await store.put("runs/one.json", b"replaced")
        assert await store.get("runs/one.json") == b"replaced"
        assert await store.delete("runs/one.json")
        assert not await store.delete("runs/one.json")


@pytest.mark.asyncio
async def test_file_state_store_rejects_path_traversal(tmp_path: pathlib.Path) -> None:
    store = syn.FileStateStore(tmp_path / "state")
    with pytest.raises(ValueError):
        await store.put("../outside", b"no")
    assert not (tmp_path / "outside").exists()


@pytest.mark.asyncio
async def test_encrypted_store_hides_values_and_logical_keys(
    tmp_path: pathlib.Path,
) -> None:
    root = tmp_path / "encrypted"
    encrypted = syn.EncryptedStateStore(
        syn.FileStateStore(root),
        bytes(range(32)),
    )
    await encrypted.put("runs/private-patient/manifest.json", b"alice@example.com")

    assert (
        await encrypted.get("runs/private-patient/manifest.json")
        == b"alice@example.com"
    )
    assert await encrypted.list("runs/") == ("runs/private-patient/manifest.json",)
    files = [path for path in root.rglob("*") if path.is_file()]
    assert len(files) == 1
    assert "private-patient" not in files[0].as_posix()
    ciphertext = files[0].read_bytes()
    assert b"alice@example.com" not in ciphertext
    assert b"private-patient" not in ciphertext

    files[0].write_bytes(ciphertext[:-1] + bytes([ciphertext[-1] ^ 1]))
    with pytest.raises(syn.StateDecryptionError):
        await encrypted.get("runs/private-patient/manifest.json")


def test_state_key_round_trip() -> None:
    encoded = syn.generate_state_key()
    assert len(syn.decode_state_key(encoded)) == 32
    assert syn.decode_state_key(bytes(range(32)).hex()) == bytes(range(32))
