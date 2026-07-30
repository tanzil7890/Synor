"""Test module for full reprocess behavior."""

from __future__ import annotations

import pathlib
from typing import Iterator

import synor as syn
from synor.connectors.localfs import declare_dir_target, DirTarget


@syn.lifespan
def synor_lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./synor_full_reprocess.db")
    yield


@syn.fn
def create_targets(target: DirTarget, create_b: bool) -> None:
    """Create target files A and optionally B."""
    target.declare_file("target_a.txt", "content_a")
    if create_b:
        target.declare_file("target_b.txt", "content_b")


@syn.fn
async def app_main(create_b: bool = True) -> None:
    """Main app function that creates targets A and optionally B."""
    target = await syn.use_mount(
        syn.component_subpath("setup"),
        declare_dir_target,
        pathlib.Path("./out_full_reprocess"),
    )
    await syn.mount(syn.component_subpath("create"), create_targets, target, create_b)


app = syn.App(
    syn.AppConfig(name="FullReprocessApp"),
    app_main,
)
