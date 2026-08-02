"""Generate the pre-native LMDB fixture with Synor revision 63df53f.

Run this script with a wheel built from the revision recorded in README.md.
It intentionally uses public APIs so the fixture represents a real app run,
not a database assembled by the migration code under test.
"""

from __future__ import annotations

import argparse
import pathlib
from collections.abc import Collection
from typing import Any

import synor as syn


_EXTERNAL: dict[str, Any] = {}


def _apply(
    context_provider: syn.ContextProvider,
    actions: Collection[tuple[str, Any | syn.AbsentType]],
    /,
) -> None:
    del context_provider
    for key, value in actions:
        if syn.is_absent(value):
            _EXTERNAL.pop(key, None)
        else:
            _EXTERNAL[key] = value


_SINK = syn.TargetActionSink.from_fn(_apply)


class _Handler:
    def reconcile(
        self,
        key: syn.StableKey,
        desired_state: Any | syn.AbsentType,
        prev_possible_records: Collection[Any],
        prev_may_be_missing: bool,
        /,
    ) -> syn.TargetReconcileOutput[tuple[str, Any | syn.AbsentType], Any] | None:
        assert isinstance(key, str)
        if syn.is_absent(desired_state):
            if not prev_possible_records:
                return None
            value: Any | syn.AbsentType = syn.ABSENT
        else:
            if not prev_may_be_missing and all(
                previous == desired_state for previous in prev_possible_records
            ):
                return None
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


@syn.task(cache=True)
async def _legacy_child() -> None:
    syn.ensure_target_state(
        _PROVIDER.target_state("beta", {"revision": 1, "value": "child"})
    )


async def _main() -> None:
    syn.ensure_target_state(
        _PROVIDER.target_state("alpha", {"revision": 1, "value": "root"})
    )
    await syn.spawn(syn.unit_path("legacy-child"), _legacy_child)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", type=pathlib.Path)
    args = parser.parse_args()
    app = syn.App(
        syn.AppConfig(
            name="pre_native_fixture",
            environment=syn.Environment(
                syn.Settings(db_path=args.db_path, lmdb_map_size=4 * 1024 * 1024)
            ),
        ),
        _main,
    )
    app.update_blocking()
    assert _EXTERNAL == {
        "alpha": {"revision": 1, "value": "root"},
        "beta": {"revision": 1, "value": "child"},
    }


if __name__ == "__main__":
    main()
