"""Test module where App is NOT bound to a module-level variable.

This tests the WeakValueDictionary registry approach - apps created inside
functions should still be discoverable via the registry.
"""

from __future__ import annotations

import pathlib

import synor as syn
from synor.connectors.localfs import ensure_dir_target

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "synor_unbound.db"
OUT_DIR = _HERE / "out_unbound"

env = syn.Environment(syn.Settings.from_env(db_path=DB_PATH))


@syn.task
async def build() -> None:
    dir_target = await syn.call(
        syn.unit_path("out"),
        ensure_dir_target,
        OUT_DIR,
    )
    dir_target.ensure_file("unbound.txt", "Hello from UnboundApp\n")


def create_app() -> syn.App[[], None]:
    """Factory function that creates an app without binding to module-level variable."""
    return syn.App(syn.AppConfig(name="UnboundApp", environment=env), build)


# Create the app but DON'T bind it to a simple module-level name.
# The app should still be discoverable via the registry.
_internal_app_ref = create_app()

# Note: We keep _internal_app_ref to prevent garbage collection.
# In a real scenario, the app would be kept alive by being used somewhere.
