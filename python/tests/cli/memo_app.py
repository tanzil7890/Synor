"""Test module with a memoized app for testing --full-reprocess flag."""

from __future__ import annotations

import pathlib
from datetime import datetime, timezone
from typing import Iterator

import synor as syn
from synor.connectors.localfs import ensure_dir_target, DirTarget


@syn.lifespan
def synor_lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./synor.db")
    yield


@syn.task(cache=True)
def write_timestamp(target: DirTarget) -> None:
    # If memoization hits, this function won't re-run and the file won't change.
    now = datetime.now(timezone.utc).isoformat()
    target.ensure_file("stamp.txt", now)


@syn.task
async def app_main() -> None:
    target = await syn.call(
        syn.unit_path("setup"),
        ensure_dir_target,
        pathlib.Path("./out_memo"),
    )
    await syn.spawn(syn.unit_path("write"), write_timestamp, target)


app = syn.App(
    syn.AppConfig(name="MemoApp"),
    app_main,
)
