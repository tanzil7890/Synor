"""Test module with multiple environments using same db filename in different directories.

This tests the ls output grouping when both envs use 'synor.db' but in different paths:
  ./db1/synor.db:
    App1
  ./db2/synor.db:
    App2
"""

from __future__ import annotations

import pathlib

import synor as syn
from synor.connectors.localfs import declare_dir_target

_HERE = pathlib.Path(__file__).resolve().parent

# Two different directories, both using synor.db as the filename
DB_DIR_1 = _HERE / "db1"
DB_DIR_2 = _HERE / "db2"
DB_PATH_1 = DB_DIR_1 / "synor.db"
DB_PATH_2 = DB_DIR_2 / "synor.db"
OUT_DIR_1 = _HERE / "out_db1"
OUT_DIR_2 = _HERE / "out_db2"

# Create directories if they don't exist
DB_DIR_1.mkdir(exist_ok=True)
DB_DIR_2.mkdir(exist_ok=True)

env1 = syn.Environment(syn.Settings.from_env(db_path=DB_PATH_1))
env2 = syn.Environment(syn.Settings.from_env(db_path=DB_PATH_2))


@syn.fn
async def build1() -> None:
    dir_target = await syn.use_mount(
        syn.component_subpath("out"),
        declare_dir_target,
        OUT_DIR_1,
    )
    dir_target.declare_file("db1.txt", "Hello from DB1App\n")


@syn.fn
async def build2() -> None:
    dir_target = await syn.use_mount(
        syn.component_subpath("out"),
        declare_dir_target,
        OUT_DIR_2,
    )
    dir_target.declare_file("db2.txt", "Hello from DB2App\n")


# Two apps in different environments (different directories, same db filename)
app1 = syn.App(syn.AppConfig(name="DB1App", environment=env1), build1)
app2 = syn.App(syn.AppConfig(name="DB2App", environment=env2), build2)
