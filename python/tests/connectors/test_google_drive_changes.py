from __future__ import annotations

import pytest

from synor import state
from synor._internal.revocation_model import SourceEventKind
from synor.connectors.google_drive import (
    DriveFullSnapshotRequired,
    DriveGovernanceError,
    DriveRequestError,
    DriveRetryPolicy,
    GovernedGoogleDriveSource,
)
from synor.connectors.google_drive import _governed_source

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
        connector_instance_id="connector-changes",
        source_scope_id="scope-changes",
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


def _data(*, shared: bool = False) -> FakeDriveData:
    drive_id = "shared" if shared else None
    root = folder("root", "Root", drive_id=drive_id)
    child = drive_file("file-a", "a.txt", "root", drive_id=drive_id)
    tokens: dict[str | None, str] = {None: "user-1"}
    if shared:
        tokens["shared"] = "shared-1"
    return FakeDriveData(
        files={"root": root, "file-a": child},
        children={("root", None): {"files": [child]}},
        permissions={
            ("root", None): {"permissions": [permission("root-reader")]},
            ("file-a", None): {"permissions": [permission("reader-a")]},
        },
        start_tokens=tokens,
        changes={},
    )


async def _seed(source: GovernedGoogleDriveSource) -> None:
    snapshot = await source.open_governed_snapshot()
    _ = [item async for item in snapshot.items()]
    await snapshot.commit_after_downstream_ready()


@pytest.mark.asyncio
async def test_acl_only_change_invalidates_and_cursor_waits_for_readiness() -> None:
    data = _data()
    service = FakeDriveService(data)
    store = state.MemoryStateStore()
    source = _source(service, store)
    await _seed(source)
    before = {key: await store.get(key) for key in await store.list()}
    data.permissions[("file-a", None)] = {
        "permissions": [permission("reader-a", role="writer")]
    }
    changed_file = drive_file("file-a", "a.txt", "root", version="2")
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "file-a",
                "removed": False,
                "file": changed_file,
            }
        ],
        "newStartPageToken": "user-2",
    }

    batch = await source.prepare_changes()
    assert batch.observations[0].item.event is SourceEventKind.ACL_CHANGED
    assert "user-1" not in batch.observations[0].observation_generation
    assert {key: await store.get(key) for key in await store.list()} == before
    await batch.commit()
    assert {key: await store.get(key) for key in await store.list()} != before


@pytest.mark.asyncio
async def test_user_and_shared_drive_logs_use_required_flags() -> None:
    data = _data(shared=True)
    service = FakeDriveService(data)
    source = _source(service, state.MemoryStateStore(), shared=True)
    await _seed(source)
    data.changes[(None, "user-1")] = {
        "changes": [],
        "newStartPageToken": "user-2",
    }
    data.changes[("shared", "shared-1")] = {
        "changes": [],
        "newStartPageToken": "shared-2",
    }

    session = await source.open_change_session()
    assert [item async for item in session.items()] == []
    calls = [kwargs for name, kwargs in service.calls if name == "changes.list"]
    assert {call.get("driveId") for call in calls} == {None, "shared"}
    assert all(call["includeRemoved"] is True for call in calls)
    assert all(call["includeCorpusRemovals"] is True for call in calls)
    assert all(call["supportsAllDrives"] is True for call in calls)
    assert all(call["includeItemsFromAllDrives"] is True for call in calls)
    for call in calls:
        fields = call.get("fields")
        assert isinstance(fields, str)
        assert "canListChildren" in fields


@pytest.mark.asyncio
async def test_parent_change_recomputes_descendant_policy() -> None:
    data = _data()
    service = FakeDriveService(data)
    source = _source(service, state.MemoryStateStore())
    await _seed(source)
    data.permissions[("file-a", None)] = {
        "permissions": [permission("different-reader", inherited_from="root")]
    }
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "root",
                "removed": False,
                "file": data.files["root"],
            }
        ],
        "newStartPageToken": "user-2",
    }

    session = await source.open_change_session()
    events = [item async for item in session.items()]
    assert [(key, item.event) for key, item in events] == [
        ("file-a", SourceEventKind.ACL_CHANGED)
    ]


@pytest.mark.asyncio
async def test_bare_tombstone_stays_ambiguous_and_keeps_last_access() -> None:
    data = _data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    await _seed(source)
    data.changes[(None, "user-1")] = {
        "changes": [{"fileId": "file-a", "removed": True}],
        "newStartPageToken": "user-2",
    }

    batch = await source.prepare_changes()
    observation = batch.observations[0]
    assert observation.item.event is SourceEventKind.AMBIGUOUS_REMOVAL
    assert observation.item.resource is None
    assert observation.item.access == observation.previous_access


@pytest.mark.asyncio
async def test_included_current_corpus_state_can_prove_move() -> None:
    data = _data(shared=True)
    service = FakeDriveService(data)
    source = _source(service, state.MemoryStateStore(), shared=True)
    await _seed(source)
    moved = drive_file("file-a", "a.txt", "root", drive_id=None)
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "file-a",
                "removed": True,
                "file": moved,
            }
        ],
        "newStartPageToken": "user-2",
    }
    data.changes[("shared", "shared-1")] = {
        "changes": [],
        "newStartPageToken": "shared-2",
    }

    session = await source.open_change_session()
    events = [item async for item in session.items()]
    assert events[0][1].event is SourceEventKind.MOVED_SCOPE


@pytest.mark.asyncio
async def test_visible_trashed_file_is_source_deleted_not_ambiguous() -> None:
    data = _data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    await _seed(source)
    trashed = drive_file("file-a", "a.txt", "root", trashed=True)
    data.changes[(None, "user-1")] = {
        "changes": [{"fileId": "file-a", "removed": False, "file": trashed}],
        "newStartPageToken": "user-2",
    }

    session = await source.open_change_session()
    events = [item async for item in session.items()]
    assert events[0][1].event is SourceEventKind.SOURCE_DELETED
    assert events[0][1].resource is None


@pytest.mark.asyncio
async def test_rejected_cursor_requires_full_snapshot_and_cannot_commit() -> None:
    data = _data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    await _seed(source)
    data.changes[(None, "user-1")] = FakeHttpError(410)

    session = await source.open_change_session()
    events = [item async for item in session.items()]
    assert events[0][1].event is SourceEventKind.SCAN_INCOMPLETE
    assert session.result.full_snapshot_required is True
    with pytest.raises(DriveFullSnapshotRequired):
        await session.commit_after_downstream_ready()


@pytest.mark.asyncio
async def test_change_pages_are_coalesced_to_latest_file_state() -> None:
    data = _data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    await _seed(source)
    first = drive_file("file-a", "a.txt", "root", version="2")
    latest = drive_file("file-a", "renamed.txt", "root", version="3")
    data.changes[(None, "user-1")] = {
        "changes": [{"fileId": "file-a", "file": first}],
        "nextPageToken": "middle",
    }
    data.changes[(None, "middle")] = {
        "changes": [{"fileId": "file-a", "file": latest}],
        "newStartPageToken": "user-2",
    }

    batch = await source.prepare_changes()
    assert len(batch.observations) == 1
    assert batch.observations[0].item.resource is not None
    assert batch.observations[0].item.resource.file_path.path.name == "renamed.txt"


@pytest.mark.asyncio
async def test_transient_permission_failure_aborts_without_advancing_cursor() -> None:
    data = _data()
    store = state.MemoryStateStore()
    source = _source(FakeDriveService(data), store)
    await _seed(source)
    before = {key: await store.get(key) for key in await store.list()}
    data.permissions[("file-a", None)] = FakeHttpError(503)
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "file-a",
                "removed": False,
                "file": drive_file("file-a", "a.txt", "root", version="2"),
            }
        ],
        "newStartPageToken": "user-2",
    }

    with pytest.raises(DriveRequestError) as raised:
        await source.prepare_changes()

    assert raised.value.status_code == 503
    assert {key: await store.get(key) for key in await store.list()} == before


@pytest.mark.asyncio
async def test_malformed_change_resource_aborts_without_advancing_cursor() -> None:
    data = _data()
    store = state.MemoryStateStore()
    source = _source(FakeDriveService(data), store)
    await _seed(source)
    before = {key: await store.get(key) for key in await store.list()}
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "file-a",
                "removed": False,
                "file": {"id": "file-a"},
            }
        ],
        "newStartPageToken": "user-2",
    }

    with pytest.raises(DriveGovernanceError, match="change resource is malformed"):
        await source.prepare_changes()

    assert {key: await store.get(key) for key in await store.list()} == before


@pytest.mark.asyncio
async def test_permission_denial_emits_access_lost_and_can_advance_cursor() -> None:
    data = _data()
    store = state.MemoryStateStore()
    source = _source(FakeDriveService(data), store)
    await _seed(source)
    data.permissions[("file-a", None)] = FakeHttpError(403)
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "file-a",
                "removed": False,
                "file": drive_file("file-a", "a.txt", "root", version="2"),
            }
        ],
        "newStartPageToken": "user-2",
    }

    batch = await source.prepare_changes()

    assert [item.item.event for item in batch.observations] == [
        SourceEventKind.ACCESS_LOST
    ]
    await batch.commit()
    replay = await source.prepare_changes()
    assert replay.observations == ()


@pytest.mark.asyncio
async def test_transient_descendant_refresh_failure_preserves_cursor() -> None:
    data = _data()
    store = state.MemoryStateStore()
    source = _source(FakeDriveService(data), store)
    await _seed(source)
    before = {key: await store.get(key) for key in await store.list()}
    data.permissions[("file-a", None)] = FakeHttpError(503)
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "root",
                "removed": False,
                "file": data.files["root"],
            }
        ],
        "newStartPageToken": "user-2",
    }

    with pytest.raises(DriveRequestError):
        await source.prepare_changes()

    assert {key: await store.get(key) for key in await store.list()} == before


@pytest.mark.asyncio
async def test_descendant_queue_is_bounded_resumable_and_cursor_fenced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = _data()
    second = drive_file("file-b", "b.txt", "root")
    data.files["file-b"] = second
    data.children[("root", None)] = {
        "files": [data.files["file-a"], second],
    }
    data.permissions[("file-b", None)] = {"permissions": [permission("reader-b")]}
    service = FakeDriveService(data)
    source = _source(service, state.MemoryStateStore())
    await _seed(source)
    monkeypatch.setattr(_governed_source, "_MAX_DESCENDANT_RECOMPUTATIONS", 1)
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "fileId": "root",
                "removed": False,
                "file": data.files["root"],
            }
        ],
        "newStartPageToken": "user-2",
    }

    first = await source.prepare_changes()
    assert first.has_more
    assert [item.item.identity.item_id for item in first.observations] == ["file-a"]
    await first.commit()

    second_batch = await source.prepare_changes()
    assert not second_batch.has_more
    assert [item.item.identity.item_id for item in second_batch.observations] == [
        "file-b"
    ]
    await second_batch.commit()

    replay = await source.prepare_changes()
    assert replay.observations == ()
    latest_call = [kwargs for name, kwargs in service.calls if name == "changes.list"][
        -1
    ]
    assert latest_call["pageToken"] == "user-2"


@pytest.mark.asyncio
async def test_shared_drive_authority_change_requires_full_snapshot() -> None:
    data = _data(shared=True)
    source = _source(
        FakeDriveService(data),
        state.MemoryStateStore(),
        shared=True,
    )
    await _seed(source)
    data.changes[(None, "user-1")] = {
        "changes": [
            {
                "changeType": "drive",
                "driveId": "shared",
                "removed": True,
            }
        ],
        "newStartPageToken": "user-2",
    }
    data.changes[("shared", "shared-1")] = {
        "changes": [],
        "newStartPageToken": "shared-2",
    }

    session = await source.open_change_session()
    events = [item async for item in session.items()]

    assert [(key, item.event) for key, item in events] == [
        ("file-a", SourceEventKind.ACCESS_LOST)
    ]
    assert session.result.full_snapshot_required
    with pytest.raises(DriveFullSnapshotRequired):
        await session.commit_after_downstream_ready()
