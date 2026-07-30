from __future__ import annotations

import asyncio
import datetime
import hashlib
import pathlib

import pytest

from synor.state import EncryptedStateStore, FileStateStore, MemoryStateStore
from synor._internal.suppression import (
    StateStoreSuppressionIndex,
    SuppressionCorruptionError,
    SuppressionGenerationConflict,
)

from ._fixtures import SOURCE_DIGEST


TENANT_DIGEST = hashlib.sha256(b"tenant-a").hexdigest()
OTHER_TENANT_DIGEST = hashlib.sha256(b"tenant-b").hexdigest()
OTHER_SOURCE_DIGEST = hashlib.sha256(b"other-source").hexdigest()
POLICY_ID = "policy-a"
NOW = datetime.datetime(2026, 7, 29, 12, 0, tzinfo=datetime.timezone.utc)


@pytest.mark.asyncio
async def test_missing_suppression_state_fails_closed() -> None:
    index = StateStoreSuppressionIndex(MemoryStateStore())

    assert await index.is_suppressed(SOURCE_DIGEST)
    assert await index.is_suppressed_many((SOURCE_DIGEST,)) == {SOURCE_DIGEST: True}


@pytest.mark.asyncio
async def test_monotonic_generation_requires_verified_reauthorization() -> None:
    store = MemoryStateStore()
    index = StateStoreSuppressionIndex(store)
    authorized = await index.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )
    assert not authorized.suppressed
    assert not await index.is_suppressed(SOURCE_DIGEST)

    suppressed = await index.suppress(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=2,
        policy_revision="policy-v2",
        group_graph_revision="groups-v1",
        reason="access_lost",
        case_id="case1_opaque",
    )
    assert suppressed.suppressed
    assert await index.is_suppressed(SOURCE_DIGEST)

    stale = await index.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )
    assert stale == suppressed
    assert await index.is_suppressed(SOURCE_DIGEST)

    restored = await index.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=3,
        policy_revision="policy-v3",
        group_graph_revision="groups-v2",
    )
    assert not restored.suppressed
    assert restored.verified_authorization
    assert not await index.is_suppressed(SOURCE_DIGEST)

    # Even accidental record deletion does not restore access: missing is
    # interpreted as unknown and therefore suppressed.
    assert await store.delete(f"revocation/v1/suppression/{SOURCE_DIGEST}.json")
    assert await index.is_suppressed(SOURCE_DIGEST)


@pytest.mark.asyncio
async def test_durable_serving_fence_survives_store_facade_reconstruction(
    tmp_path: pathlib.Path,
) -> None:
    store_path = tmp_path / "durable-serving-fence"
    first = StateStoreSuppressionIndex(FileStateStore(store_path))
    await first.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )
    await first.persist_fail_closed(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=2,
        policy_revision="policy-v2",
        group_graph_revision="groups-v1",
        reason="access_lost",
        case_id="case1_durable_fence",
        observed_at=NOW,
    )

    reconstructed = StateStoreSuppressionIndex(FileStateStore(store_path))
    assert await reconstructed.get(SOURCE_DIGEST) is None
    assert await reconstructed.is_suppressed(SOURCE_DIGEST)

    suppressed = await reconstructed.suppress(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=2,
        policy_revision="policy-v2",
        group_graph_revision="groups-v1",
        reason="access_lost",
        case_id="case1_durable_fence",
    )
    assert suppressed.suppressed

    restored = await reconstructed.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=3,
        policy_revision="policy-v3",
        group_graph_revision="groups-v2",
    )
    assert not restored.suppressed
    after_restart = StateStoreSuppressionIndex(FileStateStore(store_path))
    assert await after_restart.get(SOURCE_DIGEST) == restored


@pytest.mark.asyncio
async def test_same_generation_authorization_cannot_clear_durable_fence(
    tmp_path: pathlib.Path,
) -> None:
    store_path = tmp_path / "conflicting-durable-serving-fence"
    first = StateStoreSuppressionIndex(FileStateStore(store_path))
    authorized = await first.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=2,
        policy_revision="policy-v2",
        group_graph_revision="groups-v1",
    )
    await first.persist_fail_closed(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=2,
        policy_revision="policy-v2",
        group_graph_revision="groups-v1",
        reason="access_lost",
        case_id="case1_same_generation_fence",
        observed_at=NOW,
    )

    with pytest.raises(SuppressionGenerationConflict):
        await first.suppress(
            source_digest=SOURCE_DIGEST,
            tenant_digest=TENANT_DIGEST,
            policy_id=POLICY_ID,
            generation=2,
            policy_revision="policy-v2",
            group_graph_revision="groups-v1",
            reason="access_lost",
            case_id="case1_same_generation_fence",
        )

    # Replaying the old authorization is not proof of a newer generation and
    # must not clear a pending revocation fence.
    assert (
        await first.authorize(
            source_digest=SOURCE_DIGEST,
            tenant_digest=TENANT_DIGEST,
            policy_id=POLICY_ID,
            generation=2,
            policy_revision="policy-v2",
            group_graph_revision="groups-v1",
        )
        == authorized
    )
    reconstructed = StateStoreSuppressionIndex(FileStateStore(store_path))
    assert await reconstructed.get(SOURCE_DIGEST) is None

    newer = await reconstructed.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=3,
        policy_revision="policy-v3",
        group_graph_revision="groups-v2",
    )
    assert (
        await StateStoreSuppressionIndex(FileStateStore(store_path)).get(SOURCE_DIGEST)
        == newer
    )


@pytest.mark.asyncio
async def test_conflicting_same_generation_is_rejected() -> None:
    index = StateStoreSuppressionIndex(MemoryStateStore())
    await index.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )

    with pytest.raises(SuppressionGenerationConflict):
        await index.suppress(
            source_digest=SOURCE_DIGEST,
            tenant_digest=TENANT_DIGEST,
            policy_id=POLICY_ID,
            generation=1,
            policy_revision="policy-v1",
            group_graph_revision="groups-v1",
            reason="access_lost",
            case_id="case1_conflict",
        )

    with pytest.raises(SuppressionGenerationConflict):
        await index.authorize(
            source_digest=SOURCE_DIGEST,
            tenant_digest=TENANT_DIGEST,
            policy_id="policy-b",
            generation=1,
            policy_revision="policy-v1",
            group_graph_revision="groups-v1",
        )


@pytest.mark.asyncio
async def test_source_identity_cannot_move_between_tenants() -> None:
    index = StateStoreSuppressionIndex(MemoryStateStore())
    await index.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )

    with pytest.raises(SuppressionGenerationConflict, match="tenants"):
        await index.authorize(
            source_digest=SOURCE_DIGEST,
            tenant_digest=OTHER_TENANT_DIGEST,
            policy_id=POLICY_ID,
            generation=2,
            policy_revision="policy-v2",
            group_graph_revision="groups-v1",
        )


@pytest.mark.asyncio
async def test_concurrent_generations_converge_to_newest() -> None:
    store = MemoryStateStore()
    first_index = StateStoreSuppressionIndex(store)
    second_index = StateStoreSuppressionIndex(store)

    await asyncio.gather(
        *(
            (first_index if generation % 2 else second_index).suppress(
                source_digest=SOURCE_DIGEST,
                tenant_digest=TENANT_DIGEST,
                policy_id=POLICY_ID,
                generation=generation,
                policy_revision=f"policy-v{generation}",
                group_graph_revision="groups-v1",
                reason="access_lost",
                case_id=f"case1_generation_{generation}",
            )
            for generation in range(1, 21)
        )
    )

    current = await first_index.get(SOURCE_DIGEST)
    assert current is not None
    assert current.generation == 20
    assert current.suppressed


@pytest.mark.asyncio
async def test_shared_store_writer_lock_rejects_another_event_loop() -> None:
    index = StateStoreSuppressionIndex(MemoryStateStore())
    assert await index.current_epoch() == 0

    def read_from_another_loop() -> int:
        return asyncio.run(index.current_epoch())

    with pytest.raises(RuntimeError, match="must share one event loop"):
        await asyncio.to_thread(read_from_another_loop)


@pytest.mark.asyncio
async def test_plaintext_and_encrypted_state_store_adapters(
    tmp_path: pathlib.Path,
) -> None:
    for store in (
        MemoryStateStore(),
        EncryptedStateStore(
            FileStateStore(tmp_path / "encrypted"),
            bytes(range(32)),
        ),
    ):
        index = StateStoreSuppressionIndex(store)
        await index.suppress(
            source_digest=SOURCE_DIGEST,
            tenant_digest=TENANT_DIGEST,
            policy_id=POLICY_ID,
            generation=4,
            policy_revision="policy-v4",
            group_graph_revision="groups-v2",
            reason="source_deleted",
            case_id="case1_encrypted",
        )
        assert await index.is_suppressed(SOURCE_DIGEST)
        assert (await index.records())[0].generation == 4

    ciphertext = b"".join(
        path.read_bytes()
        for path in (tmp_path / "encrypted").rglob("*")
        if path.is_file()
    )
    assert SOURCE_DIGEST.encode() not in ciphertext
    assert b"source_deleted" not in ciphertext


@pytest.mark.asyncio
async def test_corrupt_suppression_state_is_not_treated_as_clear() -> None:
    store = MemoryStateStore()
    index = StateStoreSuppressionIndex(store)
    await store.put(
        f"revocation/v1/suppression/{SOURCE_DIGEST}.json",
        b'{"schema_version":1,"suppressed":false}',
    )

    with pytest.raises(SuppressionCorruptionError):
        await index.is_suppressed_many((SOURCE_DIGEST,))


@pytest.mark.asyncio
async def test_valid_unsuppressed_record_copied_under_wrong_key_fails_closed() -> None:
    store = MemoryStateStore()
    index = StateStoreSuppressionIndex(store)
    await index.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )
    original_key = f"revocation/v1/suppression/{SOURCE_DIGEST}.json"
    copied_key = f"revocation/v1/suppression/{OTHER_SOURCE_DIGEST}.json"
    payload = await store.get(original_key)
    assert payload is not None
    await store.put(copied_key, payload)

    with pytest.raises(SuppressionCorruptionError, match="storage key"):
        await index.get(OTHER_SOURCE_DIGEST)
    with pytest.raises(SuppressionCorruptionError, match="stored suppression state"):
        await index.records()


@pytest.mark.asyncio
async def test_snapshot_epoch_advances_only_for_accepted_generations() -> None:
    index = StateStoreSuppressionIndex(MemoryStateStore())
    assert await index.current_epoch() == 0

    await index.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )
    first = await index.snapshot_many((SOURCE_DIGEST,))
    assert first.epoch == 1
    assert first.records[SOURCE_DIGEST] is not None

    await index.authorize(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=1,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
    )
    assert await index.current_epoch() == 1

    await index.suppress(
        source_digest=SOURCE_DIGEST,
        tenant_digest=TENANT_DIGEST,
        policy_id=POLICY_ID,
        generation=2,
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        reason="access_lost",
        case_id="case1_epoch",
    )
    second = await index.snapshot_many((SOURCE_DIGEST,))
    assert second.epoch == 2
    second_record = second.records[SOURCE_DIGEST]
    assert second_record is not None
    assert second_record.suppressed
