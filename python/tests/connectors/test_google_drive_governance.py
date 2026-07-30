from __future__ import annotations

import asyncio
import datetime
import json

import pytest

from synor import state
from synor._internal.revocation_model import SourceEventKind
from synor.connectors.google_drive import (
    DriveCheckpointConflict,
    DriveFullSnapshotRequired,
    DriveRequestError,
    DriveRetryPolicy,
    DriveSnapshotStateError,
    GovernedGoogleDriveSource,
)
from synor.connectors.google_drive._governed_source import _execute_request
from synor.connectors.google_drive._permissions import normalize_permissions

from ._google_drive_fakes import (
    FakeDriveData,
    FakeDriveService,
    FakeHttpError,
    FakeRequest,
    drive_file,
    folder,
    permission,
)


def _source(
    service: FakeDriveService,
    store: state.StateStore,
    *,
    shared_drive_ids: tuple[str, ...] = (),
    group_graph_revision: str = "groups-v1",
) -> GovernedGoogleDriveSource:
    return GovernedGoogleDriveSource(
        service_account_credential_path="unused.json",
        connector_instance_id="connector-a",
        source_scope_id="scope-a",
        tenant_id="tenant-a",
        policy_id="drive-policy",
        policy_revision="policy-v1",
        group_graph_revision=group_graph_revision,
        root_folder_ids=("root",),
        shared_drive_ids=shared_drive_ids,
        state_store=store,
        retry_policy=DriveRetryPolicy(
            max_attempts=3,
            initial_backoff_seconds=0,
            max_backoff_seconds=0,
        ),
        _service=service,
    )


def _basic_data() -> FakeDriveData:
    root = folder("root", "Root")
    first = drive_file("file-a", "same.txt", "root")
    second = drive_file("file-b", "same.txt", "root")
    return FakeDriveData(
        files={"root": root, "file-a": first, "file-b": second},
        children={("root", None): {"files": [first, second]}},
        permissions={
            ("root", None): {"permissions": [permission("root-reader")]},
            ("file-a", None): {"permissions": [permission("reader-a")]},
            ("file-b", None): {"permissions": [permission("reader-b")]},
        },
        start_tokens={None: "user-1"},
        changes={},
    )


def test_permission_normalization_is_order_stable_and_redacts_principal_data() -> None:
    first = permission("opaque-a", role="writer")
    first["emailAddress"] = "secret@example.com"
    first["displayName"] = "Sensitive Name"
    second = permission("opaque-b", inherited_from="parent")

    left = normalize_permissions(
        [first, second],
        file_inherited_permissions_disabled=True,
        group_graph_revision="groups-a",
    )
    right = normalize_permissions(
        [second, first],
        file_inherited_permissions_disabled=True,
        group_graph_revision="groups-b",
    )

    assert left.policy_digest == right.policy_digest
    evidence = json.dumps(left.evidence_summary())
    assert "secret@example.com" not in evidence
    assert "Sensitive Name" not in evidence
    assert left.inherited_from == ("parent",)


def test_permission_unknown_inheritance_and_group_membership_are_explicit() -> None:
    resolution = normalize_permissions(
        [
            permission(
                "group-a",
                subject_type="group",
                inherited_without_origin=True,
                view="metadata",
            )
        ],
        file_inherited_permissions_disabled=True,
        group_graph_revision="unresolved",
    )

    assert resolution.authority == "partial"
    assert resolution.limitation_codes == (
        "group_membership_unresolved",
        "inheritance_origin_unavailable",
    )
    assert resolution.grants[0].view == "metadata"
    assert resolution.grants[0].inherited_permissions_disabled


@pytest.mark.asyncio
async def test_snapshot_uses_stable_ids_for_duplicate_names_and_rename() -> None:
    data = _basic_data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())

    first_session = await source.open_governed_snapshot()
    first_items = [item async for item in first_session.items()]
    assert [key for key, _ in first_items] == ["file-a", "file-b"]
    assert first_items[0][1].resource is not None
    assert first_items[0][1].resource.file_path.path.as_posix() == "Root/same.txt"
    first_keys = [item.identity.component_key() for _, item in first_items]
    await first_session.commit_after_downstream_ready()

    renamed = drive_file("file-a", "renamed.txt", "root", version="2")
    data.files["file-a"] = renamed
    data.children[("root", None)] = {"files": [renamed, data.files["file-b"]]}
    data.start_tokens[None] = "user-2"

    second_session = await source.open_governed_snapshot()
    second_items = [item async for item in second_session.items()]
    assert [key for key, _ in second_items] == ["file-a", "file-b"]
    assert second_items[0][1].event is SourceEventKind.CONTENT_CHANGED
    assert second_items[0][1].resource is not None
    assert second_items[0][1].resource.file_path.path.as_posix() == "Root/renamed.txt"
    assert second_items[0][1].identity.component_key() == first_keys[0]


@pytest.mark.asyncio
async def test_group_graph_revision_invalidates_unchanged_drive_items() -> None:
    data = _basic_data()
    store = state.MemoryStateStore()
    first_source = _source(
        FakeDriveService(data),
        store,
        group_graph_revision="groups-v1",
    )
    baseline = await first_source.open_governed_snapshot()
    _ = [item async for item in baseline.items()]
    await baseline.commit_after_downstream_ready()

    second_source = _source(
        FakeDriveService(data),
        store,
        group_graph_revision="groups-v2",
    )
    changed = await second_source.open_governed_snapshot()
    items = [item async for item in changed.items()]

    assert all(item.event is SourceEventKind.GROUP_GRAPH_CHANGED for _, item in items)


@pytest.mark.asyncio
async def test_group_graph_revision_change_blocks_incremental_replay_until_snapshot() -> (
    None
):
    data = _basic_data()
    store = state.MemoryStateStore()
    first_source = _source(
        FakeDriveService(data),
        store,
        group_graph_revision="groups-v1",
    )
    baseline = await first_source.open_governed_snapshot()
    _ = [item async for item in baseline.items()]
    await baseline.commit_after_downstream_ready()

    second_source = _source(
        FakeDriveService(data),
        store,
        group_graph_revision="groups-v2",
    )
    replay = await second_source.prepare_changes()

    assert replay.requires_full_snapshot
    assert {observation.item.event for observation in replay.observations} == {
        SourceEventKind.GROUP_GRAPH_CHANGED
    }
    with pytest.raises(DriveFullSnapshotRequired):
        await replay.commit()


@pytest.mark.asyncio
async def test_inaccessible_root_404_suppresses_known_descendants_as_ambiguous() -> (
    None
):
    data = _basic_data()
    store = state.MemoryStateStore()
    source = _source(FakeDriveService(data), store)
    baseline = await source.open_governed_snapshot()
    _ = [item async for item in baseline.items()]
    await baseline.commit_after_downstream_ready()
    before = {key: await store.get(key) for key in await store.list()}
    del data.files["root"]

    partial = await source.open_governed_snapshot()
    observations = [item async for item in partial.items()]

    assert partial.result.status == "partial"
    assert {(key, item.event) for key, item in observations} == {
        ("file-a", SourceEventKind.AMBIGUOUS_REMOVAL),
        ("file-b", SourceEventKind.AMBIGUOUS_REMOVAL),
    }
    with pytest.raises(DriveSnapshotStateError):
        await partial.commit_after_downstream_ready()
    assert {key: await store.get(key) for key in await store.list()} == before


@pytest.mark.asyncio
async def test_inaccessible_known_subtree_suppresses_only_affected_descendants() -> (
    None
):
    data = _basic_data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    baseline = await source.open_governed_snapshot()
    _ = [item async for item in baseline.items()]
    await baseline.commit_after_downstream_ready()
    data.children[("root", None)] = FakeHttpError(403)

    partial = await source.open_governed_snapshot()
    observations = [item async for item in partial.items()]

    assert partial.result.status == "partial"
    assert {(key, item.event) for key, item in observations} == {
        ("file-a", SourceEventKind.ACCESS_LOST),
        ("file-b", SourceEventKind.ACCESS_LOST),
    }


@pytest.mark.asyncio
async def test_snapshot_change_fence_cascades_folder_tombstone() -> None:
    data = _basic_data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    baseline = await source.open_governed_snapshot()
    _ = [item async for item in baseline.items()]
    await baseline.commit_after_downstream_ready()
    data.start_tokens[None] = "fence-folder-before"
    data.changes[(None, "fence-folder-before")] = {
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
                "file": data.files["file-a"],
            },
        ],
        "newStartPageToken": "fence-folder-after",
    }

    fenced = await source.open_governed_snapshot()
    events = {
        key: item.event for key, item in [entry async for entry in fenced.items()]
    }

    assert fenced.result.status == "complete"
    assert events["file-a"] is SourceEventKind.AMBIGUOUS_REMOVAL
    assert events["file-b"] is SourceEventKind.AMBIGUOUS_REMOVAL


@pytest.mark.asyncio
async def test_snapshot_change_fence_enumerates_folder_entering_scope() -> None:
    data = _basic_data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    baseline = await source.open_governed_snapshot()
    _ = [item async for item in baseline.items()]
    await baseline.commit_after_downstream_ready()
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
    data.start_tokens[None] = "fence-entry-before"
    data.changes[(None, "fence-entry-before")] = {
        "changes": [
            {
                "fileId": "incoming",
                "changeType": "file",
                "removed": False,
                "file": incoming,
            }
        ],
        "newStartPageToken": "fence-entry-after",
    }

    fenced = await source.open_governed_snapshot()
    events = {
        key: item.event for key, item in [entry async for entry in fenced.items()]
    }

    assert fenced.result.status == "complete"
    assert events["file-new"] is SourceEventKind.PRESENT


@pytest.mark.asyncio
async def test_snapshot_fence_materializes_child_seen_before_entering_parent() -> None:
    data = _basic_data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    baseline = await source.open_governed_snapshot()
    _ = [item async for item in baseline.items()]
    await baseline.commit_after_downstream_ready()
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
    data.start_tokens[None] = "fence-entry-order-before"
    data.changes[(None, "fence-entry-order-before")] = {
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
        "newStartPageToken": "fence-entry-order-after",
    }

    fenced = await source.open_governed_snapshot()
    events = {
        key: item.event for key, item in [entry async for entry in fenced.items()]
    }

    assert fenced.result.status == "complete"
    assert events["file-new"] is SourceEventKind.PRESENT


@pytest.mark.asyncio
async def test_partial_snapshot_never_emits_cleanup_or_commits_checkpoint() -> None:
    data = _basic_data()
    store = state.MemoryStateStore()
    source = _source(FakeDriveService(data), store)
    baseline = await source.open_governed_snapshot()
    _ = [item async for item in baseline.items()]
    await baseline.commit_after_downstream_ready()
    before = {key: await store.get(key) for key in await store.list()}

    data.children[("root", None)] = {
        "files": [data.files["file-a"]],
        "nextPageToken": "middle",
    }
    data.children[("root", "middle")] = FakeHttpError(503)
    partial = await source.open_governed_snapshot()
    observations = [item async for item in partial.items()]

    assert partial.result.status == "partial"
    assert all(
        item.event is not SourceEventKind.AMBIGUOUS_REMOVAL for _, item in observations
    )
    with pytest.raises(DriveSnapshotStateError):
        await partial.commit_after_downstream_ready()
    assert {key: await store.get(key) for key in await store.list()} == before


@pytest.mark.asyncio
async def test_incomplete_search_is_non_authoritative() -> None:
    data = _basic_data()
    data.children[("root", None)] = {
        "files": [data.files["file-a"], data.files["file-b"]],
        "incompleteSearch": True,
    }
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    session = await source.open_governed_snapshot()
    _ = [item async for item in session.items()]

    assert session.result.status == "partial"
    assert not session.result.authorizes_missing_item_cleanup


@pytest.mark.asyncio
async def test_limited_access_folder_without_list_capability_is_partial() -> None:
    data = _basic_data()
    data.files["root"]["capabilities"] = {"canListChildren": False}
    service = FakeDriveService(data)
    source = _source(service, state.MemoryStateStore())
    session = await source.open_governed_snapshot()
    observations = [item async for item in session.items()]

    assert observations == []
    assert session.result.status == "partial"
    assert not session.result.authorizes_missing_item_cleanup
    assert not any(name == "files.list" for name, _ in service.calls)


@pytest.mark.asyncio
async def test_shared_drive_root_without_configured_drive_log_is_partial() -> None:
    root = folder("root", "Root", drive_id="shared")
    child = drive_file("file-a", "a.txt", "root", drive_id="shared")
    data = FakeDriveData(
        files={"root": root, "file-a": child},
        children={("root", None): {"files": [child]}},
        permissions={
            ("root", None): {"permissions": [permission("root-reader")]},
            ("file-a", None): {"permissions": [permission("reader-a")]},
        },
        start_tokens={None: "user-1"},
        changes={},
    )
    source = _source(FakeDriveService(data), state.MemoryStateStore())

    session = await source.open_governed_snapshot()
    _ = [item async for item in session.items()]

    assert session.result.status == "partial"
    assert not session.result.authorizes_missing_item_cleanup
    with pytest.raises(DriveSnapshotStateError):
        await session.commit_after_downstream_ready()


@pytest.mark.asyncio
async def test_snapshot_replays_change_fence_before_authorizing_cleanup() -> None:
    data = _basic_data()
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    baseline = await source.open_governed_snapshot()
    _ = [item async for item in baseline.items()]
    await baseline.commit_after_downstream_ready()

    data.start_tokens[None] = "fence-before"
    data.changes[(None, "fence-before")] = {
        "changes": [{"fileId": "file-b", "removed": True}],
        "newStartPageToken": "fence-after",
    }
    fenced = await source.open_governed_snapshot()
    events = {
        key: item.event
        for key, item in [observation async for observation in fenced.items()]
    }

    assert fenced.result.status == "complete"
    assert events["file-b"] is SourceEventKind.AMBIGUOUS_REMOVAL
    assert fenced.result.cursor_after is not None


@pytest.mark.asyncio
async def test_snapshot_cursor_waits_for_readiness_and_conflicts_are_detected() -> None:
    store = state.MemoryStateStore()
    source = _source(FakeDriveService(_basic_data()), store)
    first = await source.open_governed_snapshot()
    second = await source.open_governed_snapshot()
    _ = [item async for item in first.items()]
    _ = [item async for item in second.items()]

    assert not any("/cursors/" in key for key in await store.list())
    await first.commit_after_downstream_ready()
    assert any("/cursors/" in key for key in await store.list())
    with pytest.raises(DriveCheckpointConflict):
        await second.commit_after_downstream_ready()


@pytest.mark.asyncio
async def test_state_cannot_be_reused_with_different_authority_configuration() -> None:
    data = _basic_data()
    store = state.MemoryStateStore()
    source = _source(FakeDriveService(data), store)
    baseline = await source.open_governed_snapshot()
    _ = [item async for item in baseline.items()]
    await baseline.commit_after_downstream_ready()

    different_tenant = GovernedGoogleDriveSource(
        service_account_credential_path="unused.json",
        connector_instance_id="connector-a",
        source_scope_id="scope-a",
        tenant_id="tenant-b",
        root_folder_ids=("root",),
        state_store=store,
        _service=FakeDriveService(data),
    )
    with pytest.raises(DriveCheckpointConflict):
        await different_tenant.open_governed_snapshot()


@pytest.mark.asyncio
async def test_permission_expiry_replays_until_durably_acknowledged() -> None:
    data = _basic_data()
    data.children[("root", None)] = {"files": [data.files["file-a"]]}
    data.permissions[("file-a", None)] = {
        "permissions": [permission("reader-a", expiration="2026-01-02T00:00:00Z")]
    }
    source = _source(FakeDriveService(data), state.MemoryStateStore())
    snapshot = await source.open_governed_snapshot()
    _ = [item async for item in snapshot.items()]
    await snapshot.commit_after_downstream_ready()
    now = datetime.datetime(2026, 1, 3, tzinfo=datetime.timezone.utc)

    first = source.open_permission_expiry_session(now=now)
    first_events = [item async for item in first.items()]
    assert first_events[0][1].event is SourceEventKind.PERMISSION_EXPIRED
    replay = source.open_permission_expiry_session(now=now + datetime.timedelta(days=1))
    replay_events = [item async for item in replay.items()]
    assert len(replay_events) == 1
    assert replay_events[0][1].observation_id == first_events[0][1].observation_id
    await first.commit_after_downstream_ready()
    acknowledged = source.open_permission_expiry_session(now=now)
    assert [item async for item in acknowledged.items()] == []


@pytest.mark.asyncio
async def test_retry_is_bounded_and_provider_error_text_is_sanitized() -> None:
    attempts = 0

    def action() -> dict[str, object]:
        nonlocal attempts
        attempts += 1
        raise FakeHttpError(429)

    with pytest.raises(DriveRequestError) as raised:
        await _execute_request(
            lambda: FakeRequest(action),
            DriveRetryPolicy(
                max_attempts=3,
                initial_backoff_seconds=0,
                max_backoff_seconds=0,
            ),
        )
    assert attempts == 3
    assert "sensitive provider message" not in str(raised.value)


@pytest.mark.asyncio
async def test_cancellation_is_not_converted_to_provider_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def cancelled(_function: object) -> object:
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "to_thread", cancelled)
    with pytest.raises(asyncio.CancelledError):
        await _execute_request(
            lambda: FakeRequest(lambda: {}),
            DriveRetryPolicy(),
        )
