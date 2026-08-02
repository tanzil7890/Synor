"""Test module that uses default db path from SYNOR_DB environment variable."""

from __future__ import annotations

import pathlib

import synor as syn
from synor.connectors.localfs import ensure_dir_target

_HERE = pathlib.Path(__file__).resolve().parent
OUT_DIR = _HERE / "out_default_db"


@syn.task
async def build() -> None:
    dir_target = await syn.call(
        syn.unit_path("out"),
        ensure_dir_target,
        OUT_DIR,
    )
    dir_target.ensure_file("default_db.txt", "Hello from DefaultDbApp\n")


app = syn.App(syn.AppConfig(name="DefaultDbApp"), build)
