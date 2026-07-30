from __future__ import annotations

import pytest

from synor import state
from synor._internal.revocation_model import SourceEventKind
from synor.connectors.google_drive import (
    DriveFullSnapshotRequired,
    DriveRequestError,
    DriveRetryPolicy,
    GovernedGoogleDriveSource,
)

from ._google_drive_fakes import (
    FakeDriveData,
    FakeDriveService,
    FakeHttpError,
    drive_file,
    folder,
    permission,
)


def _source(
    service: FakeDriveService,
    store: state.StateStore,
    *,
    shared: bool = False,
) -> GovernedGoogleDriveSource:
    return GovernedGoogleDriveSource(
        service_account_credential_path="unused.json",
        connector_instance_id="connector-change-edges",
        source_scope_id="scope-change-edges",
        tenant_id="tenant-a",
        policy_id="drive-policy",
        policy_revision="policy-v1",
        group_graph_revision="groups-v1",
        root_folder_ids=("root",),
        shared_drive_ids=("shared",) if shared else (),
        state_store=store,
        retry_policy=DriveRetryPolicy(
            max_attempts=1,
            initial_backoff_seconds=0,
            max_backoff_seconds=0,
        ),
        _service=service,
    )


def _data(*, child_drive_id: str | None = None) -> FakeDriveData:
    root = folder("root", "Root")
    child = drive_file(
        "file-a",
        "a.txt",
        "root",
        drive_id=child_drive_id,
    )
    return FakeDriveData(
        files={"root": root, "file-a": child},
        children={("root", None): {"files": [child]}},
        permissions={
            ("root", None): {"permissions": [permission("root-reader")]},
            ("file-a", None): {"permissions": [permission("reader-a")]},
        },
        start_tokens={None: "user-1", "shared": "shared-1"},
        changes={},
    )


async def _seed(source: GovernedGoogleDriveSource) -> None:
    snapshot = await source.open_governed_snapshot()
    _ = [item async for item in snapshot.items()]
    assert snapshot.result.authorizes_missing_item_cleanup
    await snapshot.commit_after_downstream_ready()


async def _persisted_state(
    store: state.StateStore,
) -> tuple[tuple[str, bytes | None], ...]:
    output: list[tuple[str, bytes | None]] = []
    for key in await store.list():
        output.append((key, await store.get(key)))
    return tuple(output)


@pytest.mark.asyncio
async def test_bare_removed_folder_suppresses_active_child_as_ambiguous() -> None:
    data = _data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    await _seed(source)
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "root",
                "changeType": "file",
                "removed": True,
            }
        ],
        "newStartPageToken": "user-2",
    }

    batch = await source.prepare_changes()

    assert [
        (observation.item.identity.item_id, observation.item.event)
        for observation in batch.observations
    ] == [("file-a", SourceEventKind.AMBIGUOUS_REMOVAL)]
    assert batch.observations[0].item.resource is None
    assert batch.observations[0].item.access == batch.observations[0].previous_access


@pytest.mark.asyncio
async def test_trashed_folder_suppresses_active_child_as_source_deleted() -> None:
    data = _data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    await _seed(source)
    trashed_root = dict(data.files["root"])
    trashed_root["trashed"] = True
    trashed_root["version"] = "2"
    data.files["root"] = trashed_root
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "root",
                "changeType": "file",
                "removed": False,
                "file": trashed_root,
            }
        ],
        "newStartPageToken": "user-2",
    }

    batch = await source.prepare_changes()

    assert [
        (observation.item.identity.item_id, observation.item.event)
        for observation in batch.observations
    ] == [("file-a", SourceEventKind.SOURCE_DELETED)]
    assert batch.observations[0].item.resource is None
    assert batch.observations[0].item.access == batch.observations[0].previous_access


@pytest.mark.asyncio
async def test_parent_permission_503_aborts_without_changing_checkpoint() -> None:
    data = _data()
    store = state.MemoryStateStore()
    source = _source(FakeDriveService(data), store)
    await _seed(source)
    before = await _persisted_state(store)
    data.permissions[("root", None)] = FakeHttpError(503)
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "root",
                "changeType": "file",
                "removed": False,
                "file": data.files["root"],
            }
        ],
        "newStartPageToken": "user-2",
    }

    with pytest.raises(DriveRequestError) as raised:
        await source.prepare_changes()

    assert raised.value.status_code == 503
    assert raised.value.retryable
    assert await _persisted_state(store) == before


@pytest.mark.asyncio
async def test_descendant_permission_503_aborts_without_changing_checkpoint() -> None:
    data = _data()
    store = state.MemoryStateStore()
    source = _source(FakeDriveService(data), store)
    await _seed(source)
    before = await _persisted_state(store)
    data.permissions[("file-a", None)] = FakeHttpError(503)
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "root",
                "changeType": "file",
                "removed": False,
                "file": data.files["root"],
            }
        ],
        "newStartPageToken": "user-2",
    }

    with pytest.raises(DriveRequestError) as raised:
        await source.prepare_changes()

    assert raised.value.status_code == 503
    assert raised.value.retryable
    assert await _persisted_state(store) == before


@pytest.mark.asyncio
async def test_parent_permission_403_suppresses_descendants_as_access_lost() -> None:
    data = _data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    await _seed(source)
    data.permissions[("root", None)] = FakeHttpError(403)
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "root",
                "changeType": "file",
                "removed": False,
                "file": data.files["root"],
            }
        ],
        "newStartPageToken": "user-2",
    }

    batch = await source.prepare_changes()

    assert [
        (observation.item.identity.item_id, observation.item.event)
        for observation in batch.observations
    ] == [("file-a", SourceEventKind.ACCESS_LOST)]
    assert batch.observations[0].item.resource is None
    assert batch.observations[0].item.access == batch.observations[0].previous_access


@pytest.mark.asyncio
async def test_same_corpus_latest_tombstone_beats_earlier_live_record() -> None:
    data = _data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    await _seed(source)
    live = drive_file("file-a", "a.txt", "root", version="2")
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "file-a",
                "changeType": "file",
                "removed": False,
                "file": live,
            }
        ],
        "nextPageToken": "user-middle",
    }
    data.changes[(None, "user-middle")] = {
        "changes": [
            {
                "fileId": "file-a",
                "changeType": "file",
                "removed": True,
            }
        ],
        "newStartPageToken": "user-2",
    }

    batch = await source.prepare_changes()

    assert len(batch.observations) == 1
    observation = batch.observations[0]
    assert observation.item.identity.item_id == "file-a"
    assert observation.item.event is SourceEventKind.AMBIGUOUS_REMOVAL
    assert observation.item.resource is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("initial_drive_id", "live_drive_id", "expected_corpus_id"),
    [
        ("shared", None, "user"),
        (None, "shared", "drive:shared"),
    ],
    ids=["shared-to-user", "user-to-shared"],
)
async def test_cross_corpus_live_record_wins_over_tombstone_regardless_of_poll_order(
    initial_drive_id: str | None,
    live_drive_id: str | None,
    expected_corpus_id: str,
) -> None:
    data = _data(child_drive_id=initial_drive_id)
    source = _source(
        FakeDriveService(data),
        state.MemoryStateStore(),
        shared=True,
    )
    await _seed(source)
    live = drive_file(
        "file-a",
        "a.txt",
        "root",
        drive_id=live_drive_id,
        version="2",
    )
    data.files["file-a"] = live
    tombstone = {
        "fileId": "file-a",
        "changeType": "file",
        "removed": True,
    }
    live_change = {
        "fileId": "file-a",
        "changeType": "file",
        "removed": False,
        "file": live,
    }
    data.changes[(None, "user-1")] = {
        "changes": [live_change if live_drive_id is None else tombstone],
        "newStartPageToken": "user-2",
    }
    data.changes[("shared", "shared-1")] = {
        "changes": [live_change if live_drive_id == "shared" else tombstone],
        "newStartPageToken": "shared-2",
    }

    batch = await source.prepare_changes()

    assert len(batch.observations) == 1
    observation = batch.observations[0]
    assert observation.item.identity.item_id == "file-a"
    assert observation.item.event is SourceEventKind.MOVED_SCOPE
    assert observation.item.resource is not None
    assert observation.current_corpus_id == expected_corpus_id


@pytest.mark.asyncio
async def test_shared_drive_level_loss_suppresses_known_nodes_and_requires_snapshot() -> (
    None
):
    data = _data(child_drive_id="shared")
    source = _source(
        FakeDriveService(data),
        state.MemoryStateStore(),
        shared=True,
    )
    await _seed(source)
    data.changes[(None, "user-1")] = {
        "changes": [],
        "newStartPageToken": "user-2",
    }
    data.changes[("shared", "shared-1")] = {
        "changes": [
            {
                "changeType": "drive",
                "driveId": "shared",
                "removed": True,
            }
        ],
        "newStartPageToken": "shared-2",
    }

    session = await source.open_change_session()
    observations = [item async for item in session.items()]

    assert [(key, item.event) for key, item in observations] == [
        ("file-a", SourceEventKind.ACCESS_LOST)
    ]
    assert observations[0][1].resource is None
    assert session.result.full_snapshot_required
    assert not session.result.authorizes_checkpoint_advance
    with pytest.raises(DriveFullSnapshotRequired):
        await session.commit_after_downstream_ready()


@pytest.mark.asyncio
async def test_corpus_404_is_ambiguous_authority_loss_and_requires_snapshot() -> None:
    data = _data()
    store = state.MemoryStateStore()
    source = _source(FakeDriveService(data), store)
    await _seed(source)
    before = await _persisted_state(store)
    data.changes[(None, "user-1")] = FakeHttpError(404)

    session = await source.open_change_session()
    observations = [item async for item in session.items()]

    assert [(key, item.event) for key, item in observations] == [
        ("file-a", SourceEventKind.AMBIGUOUS_REMOVAL)
    ]
    assert session.result.full_snapshot_required
    with pytest.raises(DriveFullSnapshotRequired):
        await session.commit_after_downstream_ready()
    assert await _persisted_state(store) == before


@pytest.mark.asyncio
async def test_item_permission_404_is_ambiguous_not_proven_access_loss() -> None:
    data = _data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    await _seed(source)
    data.permissions[("file-a", None)] = FakeHttpError(404)
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "file-a",
                "changeType": "file",
                "removed": False,
                "file": drive_file("file-a", "a.txt", "root", version="2"),
            }
        ],
        "newStartPageToken": "user-2",
    }

    batch = await source.prepare_changes()

    assert [entry.item.event for entry in batch.observations] == [
        SourceEventKind.AMBIGUOUS_REMOVAL
    ]
    assert batch.observations[0].item.resource is None


@pytest.mark.asyncio
async def test_folder_revocation_wins_over_later_live_child_in_same_batch() -> None:
    data = _data(child_drive_id="shared")
    data.files["root"] = folder("root", "Root", drive_id="shared")
    data.children[("root", None)] = {"files": [data.files["file-a"]]}
    source = _source(
        FakeDriveService(data),
        state.MemoryStateStore(),
        shared=True,
    )
    await _seed(source)
    data.changes[(None, "user-1")] = {
        "changes": [],
        "newStartPageToken": "user-2",
    }
    data.changes[("shared", "shared-1")] = {
        "changes": [
            {
                "fileId": "root",
                "changeType": "file",
                "removed": True,
            },
            {
                "fileId": "file-a",
                "changeType": "file",
                "removed": False,
                "file": drive_file(
                    "file-a",
                    "a.txt",
                    "root",
                    drive_id="shared",
                    version="2",
                ),
            },
        ],
        "newStartPageToken": "shared-2",
    }

    batch = await source.prepare_changes()

    assert [
        (entry.item.identity.item_id, entry.item.event) for entry in batch.observations
    ] == [("file-a", SourceEventKind.AMBIGUOUS_REMOVAL)]
    assert batch.observations[0].item.resource is None


@pytest.mark.asyncio
async def test_folder_entering_scope_enumerates_existing_descendants() -> None:
    data = _data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    await _seed(source)
    incoming = folder("incoming", "Incoming", parents=["root"], version="2")
    nested = drive_file("file-new", "new.txt", "incoming")
    data.files.update({"incoming": incoming, "file-new": nested})
    data.children[("incoming", None)] = {"files": [nested]}
    data.permissions.update(
        {
            ("incoming", None): {"permissions": [permission("incoming-reader")]},
            ("file-new", None): {"permissions": [permission("new-reader")]},
        }
    )
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "incoming",
                "changeType": "file",
                "removed": False,
                "file": incoming,
            }
        ],
        "newStartPageToken": "user-2",
    }

    batch = await source.prepare_changes()

    assert [
        (entry.item.identity.item_id, entry.item.event) for entry in batch.observations
    ] == [("file-new", SourceEventKind.PRESENT)]


@pytest.mark.asyncio
async def test_child_before_entering_parent_is_materialized_by_subtree_scan() -> None:
    data = _data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    await _seed(source)
    incoming = folder("incoming", "Incoming", parents=["root"], version="2")
    nested = drive_file("file-new", "new.txt", "incoming")
    data.files.update({"incoming": incoming, "file-new": nested})
    data.children[("incoming", None)] = {"files": [nested]}
    data.permissions.update(
        {
            ("incoming", None): {"permissions": [permission("incoming-reader")]},
            ("file-new", None): {"permissions": [permission("new-reader")]},
        }
    )
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "file-new",
                "changeType": "file",
                "removed": False,
                "file": nested,
            },
            {
                "fileId": "incoming",
                "changeType": "file",
                "removed": False,
                "file": incoming,
            },
        ],
        "newStartPageToken": "user-2",
    }

    batch = await source.prepare_changes()

    assert [
        (entry.item.identity.item_id, entry.item.event) for entry in batch.observations
    ] == [("file-new", SourceEventKind.PRESENT)]


@pytest.mark.asyncio
async def test_folder_entry_partial_enumeration_does_not_advance_cursor() -> None:
    data = _data()
    store = state.MemoryStateStore()
    source = _source(FakeDriveService(data), store)
    await _seed(source)
    before = await _persisted_state(store)
    incoming = folder("incoming", "Incoming", parents=["root"], version="2")
    nested = drive_file("file-new", "new.txt", "incoming")
    data.files.update({"incoming": incoming, "file-new": nested})
    data.children[("incoming", None)] = {
        "files": [nested],
        "nextPageToken": "middle",
    }
    data.children[("incoming", "middle")] = FakeHttpError(503)
    data.permissions[("incoming", None)] = {
        "permissions": [permission("incoming-reader")]
    }
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "incoming",
                "changeType": "file",
                "removed": False,
                "file": incoming,
            }
        ],
        "newStartPageToken": "user-2",
    }

    with pytest.raises(DriveRequestError):
        await source.prepare_changes()

    assert await _persisted_state(store) == before
