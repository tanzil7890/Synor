"""Test app with flat/leaf target states only (no child providers)."""

from __future__ import annotations

import pathlib
from typing import Any, Collection

import synor as syn

_HERE = pathlib.Path(__file__).resolve().parent
DB_PATH = _HERE / "synor.db"

env = syn.Environment(syn.Settings.from_env(db_path=DB_PATH))


class _FlatStore:
    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    def _sink(
        self,
        context_provider: syn.ContextProvider,
        actions: Collection[tuple[str, Any | syn.NonExistenceType]],
        /,
    ) -> None:
        for key, value in actions:
            if syn.is_non_existence(value):
                self.data.pop(key, None)
            else:
                self.data[key] = value

    def reconcile(
        self,
        key: syn.StableKey,
        desired_state: Any | syn.NonExistenceType,
        prev_possible_records: Collection[Any],
        prev_may_be_missing: bool,
    ) -> syn.TargetReconcileOutput[tuple[str, Any | syn.NonExistenceType], Any] | None:
        assert isinstance(key, str)
        return syn.TargetReconcileOutput(
            action=(key, desired_state),
            sink=syn.TargetActionSink.from_fn(self._sink),
            tracking_record=desired_state,
        )


_flat_store = _FlatStore()
_provider = syn.register_root_target_states_provider(
    "test_cli/flat_preview", _flat_store
)


@syn.fn
def build() -> None:
    syn.declare_target_state(_provider.target_state("x", 42))


app = syn.App(syn.AppConfig(name="FlatPreviewApp", environment=env), build)
