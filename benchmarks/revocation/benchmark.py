"""Microbenchmark compatibility apply versus strict apply-and-verify latency."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import statistics
import time
from collections.abc import Sequence
from dataclasses import dataclass

from synor._internal.context_keys import ContextProvider
from synor._internal.revocation_model import (
    EffectDescriptor,
    EffectOperation,
    VerificationOutcome,
)
from synor._internal.verified_sink import (
    TargetVerificationOutcome,
    TargetVerificationResult,
    VerificationRetryPolicy,
    VerifiedTargetActionSink,
)


@dataclass(frozen=True, slots=True)
class _Action:
    descriptor: EffectDescriptor

    def __synor_effect_descriptor__(self) -> EffectDescriptor:
        return self.descriptor


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _percentile(values: Sequence[float], percentile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * percentile))
    return ordered[index]


async def _run(actions_count: int, rounds: int) -> dict[str, object]:
    actions = tuple(
        _Action(
            EffectDescriptor(
                operation_kind=EffectOperation.DELETE,
                action_id=f"action-{index}",
                source_digest=_digest(f"source-{index}"),
                source_generation=1,
                target_locator_digest=_digest(f"target-{index}"),
            )
        )
        for index in range(actions_count)
    )
    context = ContextProvider()

    async def apply(
        context_provider: ContextProvider,
        batch: Sequence[_Action],
        /,
    ) -> None:
        del context_provider, batch

    async def verify(
        context_provider: ContextProvider,
        batch: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        del context_provider, applied
        return tuple(
            TargetVerificationResult(
                VerificationOutcome.ABSENT,
                action_id=action.descriptor.action_id,
            )
            for action in batch
        )

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        del context_provider, outcomes

    strict = VerifiedTargetActionSink[_Action, None](
        apply=apply,
        verify=verify,
        record=record,
        policy=VerificationRetryPolicy(
            max_attempts=1,
            initial_backoff=0,
            max_backoff=0,
            jitter=0,
        ),
    )

    await apply(context, actions)
    await strict(context, actions)
    compatibility_ms: list[float] = []
    strict_ms: list[float] = []
    for _ in range(rounds):
        started = time.perf_counter()
        await apply(context, actions)
        compatibility_ms.append((time.perf_counter() - started) * 1000)

        started = time.perf_counter()
        await strict(context, actions)
        strict_ms.append((time.perf_counter() - started) * 1000)

    return {
        "actions_per_round": actions_count,
        "rounds": rounds,
        "compatibility_apply_ms": {
            "median": statistics.median(compatibility_ms),
            "p95": _percentile(compatibility_ms, 0.95),
        },
        "strict_apply_verify_record_ms": {
            "median": statistics.median(strict_ms),
            "p95": _percentile(strict_ms, 0.95),
        },
        "strict_microseconds_per_action": {
            "median": statistics.median(strict_ms) * 1000 / actions_count,
            "p95": _percentile(strict_ms, 0.95) * 1000 / actions_count,
        },
        "scope": (
            "in-process framework overhead only; excludes remote target "
            "latency and durable receipt I/O"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actions", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=100)
    args = parser.parse_args()
    if args.actions < 1 or args.rounds < 1:
        parser.error("--actions and --rounds must be positive")
    print(
        json.dumps(
            asyncio.run(_run(args.actions, args.rounds)),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
