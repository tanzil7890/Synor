"""Test module that uses default db path from SYNOR_DB environment variable."""

from __future__ import annotations

import pathlib

import synor as syn
from synor.connectors.localfs import declare_dir_target

_HERE = pathlib.Path(__file__).resolve().parent
OUT_DIR = _HERE / "out_default_db"


@syn.fn
async def build() -> None:
    dir_target = await syn.use_mount(
        syn.component_subpath("out"),
        declare_dir_target,
        OUT_DIR,
    )
    dir_target.declare_file("default_db.txt", "Hello from DefaultDbApp\n")


app = syn.App(syn.AppConfig(name="DefaultDbApp"), build)
