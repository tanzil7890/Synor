"""Test module with a memoized app for testing --full-reprocess flag."""

from __future__ import annotations

import pathlib
from datetime import datetime, timezone
from typing import Iterator

import synor as syn
from synor.connectors.localfs import declare_dir_target, DirTarget


@syn.lifespan
def synor_lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./synor.db")
    yield


@syn.fn(memo=True)
def write_timestamp(target: DirTarget) -> None:
    # If memoization hits, this function won't re-run and the file won't change.
    now = datetime.now(timezone.utc).isoformat()
    target.declare_file("stamp.txt", now)


@syn.fn
async def app_main() -> None:
    target = await syn.use_mount(
        syn.component_subpath("setup"),
        declare_dir_target,
        pathlib.Path("./out_memo"),
    )
    await syn.mount(syn.component_subpath("write"), write_timestamp, target)


app = syn.App(
    syn.AppConfig(name="MemoApp"),
    app_main,
)
