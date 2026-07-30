"""Simple test app 1 - shares database with app2."""

from __future__ import annotations

import pathlib

import synor as syn
from synor.connectors.localfs import declare_dir_target

# Shared database path for both apps
_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "synor.db"
OUT_DIR = _HERE / "out_app1"

env = syn.Environment(syn.Settings.from_env(db_path=DB_PATH))


@syn.fn
async def build() -> None:
    dir_target = await syn.use_mount(
        syn.component_subpath("out"),
        declare_dir_target,
        OUT_DIR,
    )
    dir_target.declare_file("hello.txt", "Hello from App1\n")


app = syn.App(syn.AppConfig(name="TestApp1", environment=env), build)
