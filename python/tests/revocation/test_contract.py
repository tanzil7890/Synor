from __future__ import annotations

import hashlib

from synor._internal.revocation_model import (
    SnapshotResult,
    SourceIdentity,
)


def test_stable_source_id_survives_rename_and_duplicate_display_name() -> None:
    identity = SourceIdentity("drive-connection-1", "shared-drive-9", "file-42")
    before = ("Quarterly plan", identity.component_key())
    after = ("Renamed plan", identity.component_key())
    duplicate = (
        "Quarterly plan",
        SourceIdentity(
            "drive-connection-1", "shared-drive-9", "file-43"
        ).component_key(),
    )

    assert before[1] == after[1]
    assert before[1] != duplicate[1]


def test_partial_snapshot_never_authorizes_missing_component_cleanup() -> None:
    partial = SnapshotResult(
        connector_instance_id="drive-connection-1",
        source_scope_id="shared-drive-9",
        epoch="epoch-5",
        cursor_before="cursor-4",
        cursor_after=None,
        status="partial",
        item_count=17,
        inaccessible_scope_digests=(
            hashlib.sha256(b"opaque-inaccessible-scope").hexdigest(),
        ),
    )

    assert not partial.authorizes_missing_item_cleanup
