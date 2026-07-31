from __future__ import annotations

import hashlib
import os
import shutil
from collections.abc import Collection
from pathlib import Path
from typing import Any

import pytest
import synor as syn
import synor.inspect as synor_inspect

_EXTERNAL: dict[str, Any] = {}


def _apply(
    context_provider: syn.ContextProvider,
    actions: Collection[tuple[str, Any | syn.NonExistenceType]],
    /,
) -> None:
    del context_provider
    for key, value in actions:
        if syn.is_non_existence(value):
            _EXTERNAL.pop(key, None)
        else:
            _EXTERNAL[key] = value


_SINK = syn.TargetActionSink.from_fn(_apply)


class _Handler:
    def reconcile(
        self,
        key: syn.StableKey,
        desired_state: Any | syn.NonExistenceType,
        prev_possible_records: Collection[Any],
        prev_may_be_missing: bool,
        /,
    ) -> syn.TargetReconcileOutput[tuple[str, Any | syn.NonExistenceType], Any] | None:
        del prev_may_be_missing
        assert isinstance(key, str)
        if syn.is_non_existence(desired_state):
            if not prev_possible_records:
                return None
            value: Any | syn.NonExistenceType = syn.NON_EXISTENCE
        else:
            # Always reapply so this fresh process reconstructs the fake
            # external target from the copied tracking state.
            value = desired_state
        return syn.TargetReconcileOutput(
            action=(key, value),
            sink=_SINK,
            tracking_record=desired_state,
        )


_PROVIDER = syn.register_root_target_states_provider(
    "fixture/pre_native_63df53f",
    _Handler(),
)


@syn.fn(memo=True)
async def _legacy_child() -> None:
    syn.declare_target_state(
        _PROVIDER.target_state("beta", {"revision": 1, "value": "child"})
    )


async def _main() -> None:
    syn.declare_target_state(
        _PROVIDER.target_state("alpha", {"revision": 1, "value": "root"})
    )
    await syn.mount(syn.component_subpath("legacy-child"), _legacy_child)


@pytest.mark.asyncio
async def test_copied_pre_native_database_runs_compatibility_lifecycle(
    tmp_path: Path,
) -> None:
    page_size = os.sysconf("SC_PAGE_SIZE")
    expected_digest = {
        4096: "fcfdae440098563ee91939e77b535971034969d554a8a477d543fb61d20554bb",
        16384: "2153128f58e1b5ce2c667c8da86d963373cd8a2216c049c8d8ca281d21a25048",
    }.get(page_size)
    assert expected_digest is not None, (
        f"no certified pre-native fixture for {page_size}-byte pages"
    )
    fixture = (
        Path(__file__).resolve().parents[3]
        / "rust"
        / "core"
        / "tests"
        / "fixtures"
        / "pre_native_63df53f"
        / str(page_size)
        / "data.mdb"
    )
    fixture_bytes = fixture.read_bytes()
    assert hashlib.sha256(fixture_bytes).hexdigest() == expected_digest

    db_path = tmp_path / "copied-pre-native"
    (db_path / "mdb").mkdir(parents=True)
    shutil.copyfile(fixture, db_path / "mdb" / "data.mdb")
    environment = syn.Environment(
        syn.Settings(db_path=db_path, lmdb_map_size=4 * 1024 * 1024)
    )
    app = syn.App(
        syn.AppConfig(name="pre_native_fixture", environment=environment),
        _main,
    )

    _EXTERNAL.clear()
    await app.update()
    assert _EXTERNAL == {
        "alpha": {"revision": 1, "value": "root"},
        "beta": {"revision": 1, "value": "child"},
    }
    counts = await synor_inspect.native_effect_counts(app)
    assert (
        counts.pending,
        counts.verified,
        counts.failed,
        counts.blocked,
        counts.completed,
    ) == (0, 0, 0, 0, 0)

    await app.drop()
    assert _EXTERNAL == {}
    assert await synor_inspect.list_stable_paths(app) == []

    await app.update()
    assert _EXTERNAL == {
        "alpha": {"revision": 1, "value": "root"},
        "beta": {"revision": 1, "value": "child"},
    }
