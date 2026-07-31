"""Benchmark ordinary target reconciliation with no strict native effects."""

from __future__ import annotations

import argparse
import asyncio
import json
import pathlib
import platform
import statistics
import tempfile
import time
import uuid
from collections.abc import Collection, Sequence
from typing import Any

import synor as syn
from synor._internal.context_keys import ContextProvider
from synor._internal.environment import Environment
from synor._internal.setting import Settings
from synor._internal.target_state import TargetActionSink, TargetReconcileOutput


def _percentile(values: Sequence[float], percentile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * percentile))
    return ordered[index]


async def _run(rounds: int, warmup: int) -> dict[str, object]:
    desired = [0]
    applied_actions = 0

    def apply(
        context_provider: ContextProvider,
        actions: Sequence[int],
        /,
    ) -> None:
        nonlocal applied_actions
        del context_provider
        applied_actions += len(actions)

    sink = TargetActionSink[int, None].from_fn(apply)

    class Handler:
        def reconcile(
            self,
            key: syn.StableKey,
            desired_state: int | syn.NonExistenceType,
            prev_possible_records: Collection[int],
            prev_may_be_missing: bool,
            /,
        ) -> TargetReconcileOutput[int, int, None] | None:
            del key, prev_may_be_missing
            if syn.is_non_existence(desired_state):
                return None
            if desired_state in prev_possible_records:
                return None
            return TargetReconcileOutput(
                action=desired_state,
                sink=sink,
                tracking_record=desired_state,
            )

    identity = uuid.uuid4().hex
    provider = syn.register_root_target_states_provider(
        f"benchmark/revocation/compatibility/{identity}",
        Handler(),
    )

    @syn.fn
    async def main() -> None:
        syn.declare_target_state(provider.target_state("item", desired[0]))

    with tempfile.TemporaryDirectory() as directory:
        environment = Environment(
            Settings(db_path=pathlib.Path(directory) / "state"),
            event_loop=asyncio.get_running_loop(),
        )
        app = syn.App(
            syn.AppConfig(
                name=f"native-compatibility-{identity}",
                environment=environment,
            ),
            main,
        )
        await app.update()
        for _ in range(warmup):
            desired[0] += 1
            await app.update()

        samples_ms: list[float] = []
        for _ in range(rounds):
            desired[0] += 1
            started = time.perf_counter()
            await app.update()
            samples_ms.append((time.perf_counter() - started) * 1000)

    expected_actions = 1 + warmup + rounds
    if applied_actions != expected_actions:
        raise RuntimeError(
            f"expected {expected_actions} compatibility actions, got {applied_actions}"
        )
    return {
        "rounds": rounds,
        "warmup": warmup,
        "applied_actions": applied_actions,
        "update_ms": {
            "median": statistics.median(samples_ms),
            "p95": _percentile(samples_ms, 0.95),
            "mean": statistics.fmean(samples_ms),
        },
        "python": platform.python_version(),
        "platform": platform.platform(),
        "scope": (
            "public App.update compatibility reconciliation with one legacy "
            "target action and no strict effects or remote I/O"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=1_000)
    parser.add_argument("--warmup", type=int, default=100)
    args = parser.parse_args()
    if args.rounds < 1 or args.warmup < 0:
        parser.error("--rounds must be positive and --warmup must be non-negative")
    result: dict[str, Any] = asyncio.run(_run(args.rounds, args.warmup))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
