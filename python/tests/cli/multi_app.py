"""Test module with multiple apps to demonstrate app specifier syntax."""

from __future__ import annotations

import pathlib
from typing import AsyncGenerator

import synor as syn
from synor.connectors.localfs import ensure_dir_target


_ROOT_PATH = syn.ContextKey[pathlib.Path]("root_path")


@syn.lifespan
async def lifespan(builder: syn.EnvironmentBuilder) -> AsyncGenerator[None]:
    root_path = pathlib.Path(__file__).resolve().parent

    builder.provide(_ROOT_PATH, root_path)
    builder.settings.db_path = root_path / "synor.db"
    yield


@syn.task
async def build1() -> None:
    dir_target = await syn.call(
        syn.unit_path("out"),
        ensure_dir_target,
        syn.use_context(_ROOT_PATH) / "out_multi_1",
    )
    dir_target.ensure_file("hello.txt", "Hello from MultiApp1\n")


@syn.task
async def build2() -> None:
    dir_target = await syn.call(
        syn.unit_path("out"),
        ensure_dir_target,
        syn.use_context(_ROOT_PATH) / "out_multi_2",
    )
    dir_target.ensure_file("world.txt", "Hello from MultiApp2\n")


# Two apps in the same module
app1 = syn.App("MultiApp1", build1)
app2 = syn.App("MultiApp2", build2)

# Default app (what gets run if you don't specify :app_name)
app = app1
