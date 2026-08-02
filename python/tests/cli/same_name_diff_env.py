"""Test module with same app name in different environments.

This tests that apps with the same name can coexist in different environments,
one using an explicit named environment and one using the default environment:
  alpha (./db_alpha/synor.db):
    MyApp
  default (synor.db):
    MyApp

Apps can be disambiguated using the @env_name syntax:
  synor update ./same_name_diff_env.py:MyApp@alpha
  synor update ./same_name_diff_env.py:MyApp@default
"""

from __future__ import annotations

import pathlib
from typing import Iterator

import synor as syn
from synor.connectors.localfs import ensure_dir_target

_HERE = pathlib.Path(__file__).resolve().parent

# Explicit environment with a name
DB_DIR_ALPHA = _HERE / "db_alpha"
DB_PATH_ALPHA = DB_DIR_ALPHA / "synor.db"
OUT_DIR_ALPHA = _HERE / "out_alpha"
OUT_DIR_DEFAULT = _HERE / "out_default"

# Create directory for alpha env
DB_DIR_ALPHA.mkdir(exist_ok=True)

# Named environment
env_alpha = syn.Environment(syn.Settings.from_env(db_path=DB_PATH_ALPHA), name="alpha")


# Configure the default environment via lifespan
@syn.lifespan
def _lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = _HERE / "synor.db"
    yield


@syn.task
async def build_alpha() -> None:
    dir_target = await syn.call(
        syn.unit_path("out"),
        ensure_dir_target,
        OUT_DIR_ALPHA,
    )
    dir_target.ensure_file("output.txt", "From Alpha env\n")


@syn.task
async def build_default() -> None:
    dir_target = await syn.call(
        syn.unit_path("out"),
        ensure_dir_target,
        OUT_DIR_DEFAULT,
    )
    dir_target.ensure_file("output.txt", "From Default env\n")


# Two apps with THE SAME NAME but in different environments
# One uses explicit named environment, one uses default environment
app_alpha = syn.App(syn.AppConfig(name="MyApp", environment=env_alpha), build_alpha)
app_default = syn.App("MyApp", build_default)  # Uses default environment
