"""Measure certified Qdrant verification-batch planning at launch sizes."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from collections.abc import Sequence
from typing import cast

from synor.connectors.qdrant import QdrantRevocationAction, iter_revocation_batches


_DEFAULT_SIZES = (1, 100, 10_000, 100_000)


def _run(size: int, rounds: int, batch_size: int) -> dict[str, object]:
    # Batch planning only slices the input Sequence; action materialization,
    # payloads, vectors, and remote Qdrant responses are intentionally absent.
    actions = cast(Sequence[QdrantRevocationAction], range(size))
    elapsed_ms: list[float] = []
    observed_batches = 0
    observed_peak = 0
    for _ in range(rounds):
        started = time.perf_counter()
        batches = tuple(iter_revocation_batches(actions, batch_size=batch_size))
        elapsed_ms.append((time.perf_counter() - started) * 1000)
        observed_batches = len(batches)
        observed_peak = max(len(batch) for batch in batches)
        assert sum(len(batch) for batch in batches) == size
    return {
        "actions": size,
        "batch_size": batch_size,
        "batches": observed_batches,
        "peak_actions_per_batch": observed_peak,
        "planning_ms": {
            "median": statistics.median(elapsed_ms),
            "max": max(elapsed_ms),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--actions", type=int, action="append")
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("--rounds must be positive")
    sizes = tuple(args.actions) if args.actions else _DEFAULT_SIZES
    if any(size < 1 for size in sizes):
        parser.error("--actions values must be positive")
    print(
        json.dumps(
            {
                "cases": [_run(size, args.rounds, args.batch_size) for size in sizes],
                "scope": (
                    "in-process bounded-batch planning only; excludes remote "
                    "Qdrant latency and response payload memory"
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
