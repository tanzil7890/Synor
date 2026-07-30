"""Manual acceptance probe for a dedicated governed Google Drive test scope.

The probe never reads document bodies and prints only source/policy digests,
event kinds, group revisions, and controlled status.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import os
from collections.abc import AsyncIterator

from synor import state
from synor._internal.revocation_model import GovernedSourceItem
from synor.connectors import google_drive


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def _ids(name: str) -> tuple[str, ...]:
    value = os.environ.get(name, "")
    return tuple(item.strip() for item in value.split(",") if item.strip())


async def _print_items(
    items: AsyncIterator[tuple[str, GovernedSourceItem[google_drive.DriveFile]]],
) -> int:
    count = 0
    async for _file_id, item in items:
        count += 1
        access = item.access
        print(
            {
                "source_digest": item.identity.evidence_digest(),
                "event": item.event.value,
                "policy_digest": (access.policy_digest if access is not None else None),
                "group_graph_revision": (
                    access.group_graph_revision if access is not None else None
                ),
            }
        )
    return count


def _parse_time(value: str | None) -> datetime.datetime:
    if value is None:
        return datetime.datetime.now(datetime.timezone.utc)
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("--now must be an RFC 3339 timestamp") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("--now must include a UTC offset")
    return parsed.astimezone(datetime.timezone.utc)


async def _main(
    *,
    mode: str,
    commit: bool,
    now: str | None,
) -> None:
    source = google_drive.GovernedGoogleDriveSource(
        service_account_credential_path=_required_env("GOOGLE_DRIVE_CREDENTIALS"),
        root_folder_ids=_ids("GOOGLE_DRIVE_ROOT_IDS"),
        shared_drive_ids=_ids("GOOGLE_DRIVE_SHARED_DRIVE_IDS"),
        connector_instance_id=_required_env("SYNOR_DRIVE_CONNECTOR_ID"),
        source_scope_id=_required_env("SYNOR_DRIVE_SCOPE_ID"),
        tenant_id=_required_env("SYNOR_TENANT_ID"),
        policy_revision=os.environ.get(
            "SYNOR_DRIVE_POLICY_REVISION",
            "manual-v1",
        ),
        group_graph_revision=os.environ.get(
            "SYNOR_GROUP_GRAPH_REVISION",
            "unresolved",
        ),
        delegated_subject=(os.environ.get("GOOGLE_DRIVE_DELEGATED_SUBJECT") or None),
        state_store=state.state_store_from_env(),
    )

    if mode == "snapshot":
        snapshot = await source.open_governed_snapshot()
        count = await _print_items(snapshot.items())
        print(
            {
                "mode": mode,
                "status": snapshot.result.status,
                "event_count": count,
                "cleanup_authorized": (snapshot.result.authorizes_missing_item_cleanup),
            }
        )
        if commit:
            await snapshot.commit_after_downstream_ready()
    elif mode == "changes":
        changes = await source.open_change_session()
        count = await _print_items(changes.items())
        print(
            {
                "mode": mode,
                "status": changes.result.status,
                "event_count": count,
                "full_snapshot_required": (changes.result.full_snapshot_required),
            }
        )
        if commit:
            await changes.commit_after_downstream_ready()
    elif mode == "expiry":
        expiry = source.open_permission_expiry_session(now=_parse_time(now))
        count = await _print_items(expiry.items())
        print({"mode": mode, "event_count": count})
        if commit:
            await expiry.commit_after_downstream_ready()
    else:
        raise AssertionError("argparse accepted an unsupported mode")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=("snapshot", "changes", "expiry"),
        help=(
            "bootstrap/reconcile inventory, replay the existing cursor, "
            "or evaluate local permission deadlines"
        ),
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="commit the prepared checkpoint after reviewing output",
    )
    parser.add_argument(
        "--now",
        help="RFC 3339 evaluation time for expiry mode (defaults to current UTC)",
    )
    arguments = parser.parse_args()
    asyncio.run(
        _main(
            mode=arguments.mode,
            commit=arguments.commit,
            now=arguments.now,
        )
    )
