"""Test module with a SINGLE app - should auto-select without specifier."""

from __future__ import annotations

import pathlib

import synor as syn
from synor.connectors.localfs import ensure_dir_target

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "synor.db"
OUT_DIR = _HERE / "out_single"

env = syn.Environment(syn.Settings.from_env(db_path=DB_PATH))


@syn.task
async def build() -> None:
    dir_target = await syn.call(
        syn.unit_path("out"),
        ensure_dir_target,
        OUT_DIR,
    )
    dir_target.ensure_file("single.txt", "Hello from SingleApp\n")


# Single app - should be auto-selected even without :app_name specifier
only_app = syn.App(syn.AppConfig(name="SingleApp", environment=env), build)
