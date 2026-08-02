"""Simple test app 1 - shares database with app2."""

from __future__ import annotations

import pathlib

import synor as syn
from synor.connectors.localfs import ensure_dir_target

# Shared database path for both apps
_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "synor.db"
OUT_DIR = _HERE / "out_app1"

env = syn.Environment(syn.Settings.from_env(db_path=DB_PATH))


@syn.task
async def build() -> None:
    dir_target = await syn.call(
        syn.unit_path("out"),
        ensure_dir_target,
        OUT_DIR,
    )
    dir_target.ensure_file("hello.txt", "Hello from App1\n")


app = syn.App(syn.AppConfig(name="TestApp1", environment=env), build)
