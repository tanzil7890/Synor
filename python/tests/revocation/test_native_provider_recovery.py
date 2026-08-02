from __future__ import annotations

import asyncio
import hashlib
import json
import pathlib
import subprocess
import sys
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast

import synor as syn
import synor.inspect as synor_inspect
from synor._internal.context_keys import ContextProvider
from synor._internal.revocation_model import (
    EffectDescriptor,
    EffectOperation,
    VerificationOutcome,
)
from synor._internal.target_state import TargetActionSink
from synor._internal.verified_sink import (
    TargetVerificationOutcome,
    TargetVerificationResult,
    VerificationRetryPolicy,
    VerifiedTargetActionSink,
)

_APP_NAME = "test_native_provider_recovery"
_PROVIDER_NAME = "test/revocation/native-provider-recovery/root"
_CHILD_SENTINEL = "--native-provider-recovery-child"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class _DeleteAction:
    action_id: str = "provider-recovery-delete"

    def __synor_effect_descriptor__(self) -> EffectDescriptor:
        return EffectDescriptor(
            action_id=self.action_id,
            operation_kind=EffectOperation.DELETE,
            source_digest=_digest("provider-recovery-source"),
            source_generation=1,
            target_locator_digest=_digest("provider-recovery-target"),
        )


def _counts_tuple(counts: synor_inspect.NativeEffectCounts) -> tuple[int, ...]:
    return (
        counts.pending,
        counts.verified,
        counts.failed,
        counts.blocked,
        counts.completed,
    )


async def _run_child_phase(
    phase: str,
    db_path: pathlib.Path,
    target_path: pathlib.Path,
    receipt_path: pathlib.Path,
) -> dict[str, Any]:
    provider: Any | None = None

    if phase not in {"missing", "compat_missing"}:

        async def apply_upsert(
            context_provider: ContextProvider,
            actions: Sequence[str],
            /,
        ) -> None:
            del context_provider, actions
            target_path.write_text("present")

        async def apply_delete(
            context_provider: ContextProvider,
            actions: Sequence[_DeleteAction],
            /,
        ) -> None:
            del context_provider, actions
            target_path.unlink(missing_ok=True)

        async def verify_delete(
            context_provider: ContextProvider,
            actions: Sequence[_DeleteAction],
            applied: None,
            /,
        ) -> Sequence[TargetVerificationResult]:
            del context_provider, applied
            outcome = (
                VerificationOutcome.PRESENT
                if target_path.exists()
                else VerificationOutcome.ABSENT
            )
            return [
                TargetVerificationResult(outcome, action_id=action.action_id)
                for action in actions
            ]

        async def record_delete(
            context_provider: ContextProvider,
            outcomes: Sequence[TargetVerificationOutcome],
            /,
        ) -> None:
            del context_provider
            with receipt_path.open("a") as receipt:
                for outcome in outcomes:
                    receipt.write(f"{outcome.action_id}:{outcome.status.value}\n")

        upsert_sink: TargetActionSink[str, None] = TargetActionSink.from_async_fn(
            apply_upsert
        )
        delete_sink = VerifiedTargetActionSink[_DeleteAction, None](
            apply=apply_delete,
            verify=verify_delete,
            record=record_delete,
            policy=VerificationRetryPolicy(
                timeout=timedelta(seconds=1),
                max_attempts=1,
                initial_backoff=0,
                max_backoff=0,
                jitter=0,
            ),
        )

        class Handler:
            def reconcile(
                self,
                key: syn.StableKey,
                desired_state: str | syn.AbsentType,
                prev_possible_records: Collection[str],
                prev_may_be_missing: bool,
                /,
            ) -> syn.TargetReconcileOutput[Any, str] | None:
                del key
                if syn.is_absent(desired_state):
                    return syn.TargetReconcileOutput(
                        action=_DeleteAction(),
                        sink=delete_sink.sink,
                        tracking_record=syn.ABSENT,
                    )
                if desired_state in prev_possible_records and not prev_may_be_missing:
                    return None
                return syn.TargetReconcileOutput(
                    action="upsert",
                    sink=upsert_sink,
                    tracking_record=desired_state,
                )

        provider = syn.register_root_target_states_provider(
            _PROVIDER_NAME,
            Handler(),
        )

    async def main() -> None:
        if phase == "seed":
            assert provider is not None
            syn.ensure_target_state(provider.target_state("artifact", "owned"))

    environment = syn.Environment(
        syn.Settings.from_env(db_path=db_path),
    )
    app = syn.App(
        syn.AppConfig(name=_APP_NAME, environment=environment),
        main,
    )

    error: str | None = None
    try:
        await app._update_controlled(_strict_effects=phase != "compat_missing")
    except ValueError as exc:
        error = str(exc)

    counts = await synor_inspect.native_effect_counts(app)
    tracking_count = len(
        [entry async for entry in synor_inspect.iter_target_states(app)]
    )
    return {
        "counts": _counts_tuple(counts),
        "error": error,
        "receipt_lines": (
            receipt_path.read_text().splitlines() if receipt_path.exists() else []
        ),
        "target_exists": target_path.exists(),
        "tracking_count": tracking_count,
    }


def _invoke_child(
    phase: str,
    db_path: pathlib.Path,
    target_path: pathlib.Path,
    receipt_path: pathlib.Path,
) -> dict[str, Any]:
    result = subprocess.run(
        [
            sys.executable,
            __file__,
            _CHILD_SENTINEL,
            phase,
            str(db_path),
            str(target_path),
            str(receipt_path),
        ],
        cwd=pathlib.Path(__file__).parents[3],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"child phase {phase!r} failed\nstdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    payload = json.loads(result.stdout.splitlines()[-1])
    assert isinstance(payload, dict)
    return cast(dict[str, Any], payload)


def test_strict_provider_missing_obligation_survives_restart_and_recovers(
    tmp_path: pathlib.Path,
) -> None:
    db_path = tmp_path / "native-state"
    target_path = tmp_path / "remote-target"
    receipt_path = tmp_path / "verification-receipts"

    seeded = _invoke_child("seed", db_path, target_path, receipt_path)
    assert seeded == {
        "counts": [0, 0, 0, 0, 0],
        "error": None,
        "receipt_lines": [],
        "target_exists": True,
        "tracking_count": 1,
    }

    missing = _invoke_child("missing", db_path, target_path, receipt_path)
    assert missing["counts"] == [0, 0, 0, 1, 0]
    assert missing["tracking_count"] == 1
    assert missing["target_exists"]
    assert missing["receipt_lines"] == []
    assert "provider is unavailable" in missing["error"]

    compatibility_retry = _invoke_child(
        "compat_missing",
        db_path,
        target_path,
        receipt_path,
    )
    assert compatibility_retry == {
        "counts": [0, 0, 0, 1, 0],
        "error": None,
        "receipt_lines": [],
        "target_exists": True,
        "tracking_count": 1,
    }

    recovered = _invoke_child("recover", db_path, target_path, receipt_path)
    assert recovered == {
        "counts": [0, 0, 0, 0, 2],
        "error": None,
        "receipt_lines": ["provider-recovery-delete:absent"],
        "target_exists": False,
        "tracking_count": 0,
    }

    steady = _invoke_child("recover", db_path, target_path, receipt_path)
    assert steady == recovered


if __name__ == "__main__" and len(sys.argv) == 6:
    _, sentinel, child_phase, child_db, child_target, child_receipt = sys.argv
    if sentinel != _CHILD_SENTINEL:
        raise SystemExit("invalid child sentinel")
    payload = asyncio.run(
        _run_child_phase(
            child_phase,
            pathlib.Path(child_db),
            pathlib.Path(child_target),
            pathlib.Path(child_receipt),
        )
    )
    print(json.dumps(payload))
