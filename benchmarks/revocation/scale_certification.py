"""Certify one million Phase 2 descriptor-to-receipt correlations in batches."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import resource
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


def _correlation_digest(
    *,
    action_id: str,
    operation: EffectOperation,
    source_digest: str,
    source_generation: int,
    target_locator_digest: str,
) -> bytes:
    hasher = hashlib.sha256()
    hasher.update(b"synor-phase6-scale-receipt-v1\x00")
    hasher.update(action_id.encode())
    hasher.update(b"\x00")
    hasher.update(operation.value.encode())
    hasher.update(b"\x00")
    hasher.update(source_digest.encode())
    hasher.update(source_generation.to_bytes(8, "big"))
    hasher.update(target_locator_digest.encode())
    return hasher.digest()


async def _run(action_count: int, batch_size: int) -> dict[str, object]:
    context = ContextProvider()
    expected = hashlib.sha256()
    observed = hashlib.sha256()
    current_actions: tuple[_Action, ...] = ()
    applied_count = 0
    verified_count = 0
    recorded_count = 0

    async def apply(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        /,
    ) -> None:
        nonlocal applied_count
        del context_provider
        applied_count += len(actions)

    async def verify(
        context_provider: ContextProvider,
        actions: Sequence[_Action],
        applied: None,
        /,
    ) -> Sequence[TargetVerificationResult]:
        nonlocal verified_count
        del context_provider, applied
        verified_count += len(actions)
        return tuple(
            TargetVerificationResult(
                VerificationOutcome.ABSENT,
                action_id=action.descriptor.action_id,
            )
            for action in reversed(actions)
        )

    async def record(
        context_provider: ContextProvider,
        outcomes: Sequence[TargetVerificationOutcome],
        /,
    ) -> None:
        nonlocal recorded_count
        del context_provider
        if len(outcomes) != len(current_actions):
            raise RuntimeError("recorder batch length did not match descriptor batch")
        for action, outcome in zip(current_actions, outcomes, strict=True):
            descriptor = action.descriptor
            if (
                outcome.action_id != descriptor.action_id
                or outcome.operation is not descriptor.operation_kind
                or outcome.source_digest != descriptor.source_digest
                or outcome.source_generation != descriptor.source_generation
                or outcome.target_locator_digest != descriptor.target_locator_digest
                or outcome.status is not VerificationOutcome.ABSENT
                or not outcome.required_postcondition_holds
            ):
                raise RuntimeError("descriptor-to-receipt correlation mismatch")
            observed.update(
                _correlation_digest(
                    action_id=outcome.action_id,
                    operation=outcome.operation,
                    source_digest=outcome.source_digest,
                    source_generation=outcome.source_generation,
                    target_locator_digest=outcome.target_locator_digest,
                )
            )
        recorded_count += len(outcomes)

    verified_sink = VerifiedTargetActionSink[_Action, None](
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

    started = time.perf_counter()
    for batch_start in range(0, action_count, batch_size):
        batch_end = min(batch_start + batch_size, action_count)
        current_actions = tuple(
            _Action(
                EffectDescriptor(
                    action_id=f"scale:{index}",
                    operation_kind=EffectOperation.DELETE,
                    source_digest=_digest(f"source:{index}"),
                    source_generation=1,
                    target_locator_digest=_digest(f"target:{index}"),
                )
            )
            for index in range(batch_start, batch_end)
        )
        for action in current_actions:
            descriptor = action.descriptor
            expected.update(
                _correlation_digest(
                    action_id=descriptor.action_id,
                    operation=descriptor.operation_kind,
                    source_digest=descriptor.source_digest,
                    source_generation=descriptor.source_generation,
                    target_locator_digest=descriptor.target_locator_digest,
                )
            )
        await verified_sink(context, current_actions)

    elapsed = time.perf_counter() - started
    if not (applied_count == verified_count == recorded_count == action_count):
        raise RuntimeError("scale certification lost an action at a batch boundary")
    if observed.digest() != expected.digest():
        raise RuntimeError("scale certification correlation digest mismatch")
    return {
        "actions": action_count,
        "batch_size": batch_size,
        "batches": (action_count + batch_size - 1) // batch_size,
        "applied": applied_count,
        "verified": verified_count,
        "recorded": recorded_count,
        "correlation_sha256": observed.hexdigest(),
        "elapsed_seconds": elapsed,
        "actions_per_second": action_count / elapsed,
        "max_rss_platform_units": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "scope": (
            "in-process Phase 2 descriptor, out-of-order verification, and "
            "normalized receipt correlation; excludes remote and durable I/O"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actions", type=int, default=1_000_000)
    parser.add_argument("--batch-size", type=int, default=10_000)
    args = parser.parse_args()
    if args.actions < 1 or args.batch_size < 1:
        parser.error("--actions and --batch-size must be positive")
    print(
        json.dumps(
            asyncio.run(_run(args.actions, args.batch_size)),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
