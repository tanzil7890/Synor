from __future__ import annotations

import ast
import asyncio
import dataclasses
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

import pytest
import synor as syn
from synor.connectorkits.target_sink_testing import (
    TargetSinkCertificationError,
    TargetSinkCertificationScenario,
    certify_target_sink,
)


@dataclass(frozen=True, slots=True)
class _Action:
    key: str
    value: str


class _TransactionalProbe:
    def __init__(self) -> None:
        self.state: dict[str, str] = {}
        self.order: list[str] = []
        self.batches: list[tuple[_Action, ...]] = []
        self.evidence: Literal["query_verified"] = "query_verified"

    async def reset(self) -> None:
        self.state.clear()
        self.order.clear()
        self.batches.clear()

    async def apply(self, actions: tuple[_Action, ...]) -> None:
        for offset in range(0, len(actions), 2):
            batch = actions[offset : offset + 2]
            self.batches.append(batch)
            working = dict(self.state)
            for action in batch:
                working[action.key] = action.value
                self.order.append(action.key)
            self.state = working

    async def apply_with_failure_after(
        self, actions: tuple[_Action, ...], completed_actions: int
    ) -> None:
        working = dict(self.state)
        for index, action in enumerate(actions):
            if index == completed_actions:
                raise RuntimeError("injected write failure")
            working[action.key] = action.value
        self.state = working

    async def apply_segmented_with_failure_after(
        self, actions: tuple[_Action, ...], completed_segments: int
    ) -> None:
        for segment_index, offset in enumerate(range(0, len(actions), 2)):
            if segment_index == completed_segments:
                raise RuntimeError("injected segment failure")
            batch = actions[offset : offset + 2]
            self.batches.append(batch)
            working = dict(self.state)
            for action in batch:
                working[action.key] = action.value
            self.state = working

    async def cancel_apply(self, actions: tuple[_Action, ...]) -> None:
        entered_io = asyncio.Event()

        async def blocked_apply() -> None:
            working = dict(self.state)
            working[actions[0].key] = actions[0].value
            entered_io.set()
            await asyncio.Event().wait()
            self.state = working

        task = asyncio.create_task(blocked_apply())
        await entered_io.wait()
        task.cancel()
        await task

    async def snapshot(self) -> dict[str, str]:
        return dict(self.state)

    async def observed_order(self) -> tuple[object, ...]:
        return tuple(self.order)

    async def observed_batches(self) -> tuple[tuple[_Action, ...], ...]:
        return tuple(self.batches)

    async def completion_evidence(
        self,
    ) -> Literal["query_verified"]:
        return self.evidence


def _scenario() -> TargetSinkCertificationScenario[_Action, dict[str, str]]:
    probe = _TransactionalProbe()
    actions = (
        _Action("a", "one"),
        _Action("b", "two"),
        _Action("c", "three"),
    )
    return TargetSinkCertificationScenario(
        name="transactional-probe",
        capabilities=syn.TargetSinkCapabilities(
            batch_atomicity="per_apply",
            idempotent_replay="supported",
            segmented_replay_safe="supported",
            apply_ordering="input_order",
            cancellation_safe="supported",
            completion_verification="query_verified",
            max_batch_actions=2,
            max_batch_bytes=8,
        ),
        actions=actions,
        reset=probe.reset,
        apply=probe.apply,
        snapshot=probe.snapshot,
        expected_final_snapshot={"a": "one", "b": "two", "c": "three"},
        apply_with_failure_after=probe.apply_with_failure_after,
        apply_segmented_with_failure_after=probe.apply_segmented_with_failure_after,
        cancel_apply=probe.cancel_apply,
        observed_order=probe.observed_order,
        expected_order=("a", "b", "c"),
        observed_batches=probe.observed_batches,
        action_size_bytes=lambda _action: 4,
        completion_evidence=probe.completion_evidence,
    )


@pytest.mark.asyncio
async def test_common_certification_exercises_every_claimed_guarantee() -> None:
    report = await certify_target_sink(_scenario())

    assert report.checks == (
        "success",
        "idempotent_replay",
        "failure_atomicity",
        "segmented_replay",
        "input_order",
        "cancellation_recovery",
        "batch_limits",
        "completion_verification",
    )


@pytest.mark.asyncio
async def test_common_certification_rejects_weaker_completion_evidence() -> None:
    scenario = _scenario()

    async def acknowledged_only() -> Literal["acknowledged"]:
        return "acknowledged"

    with pytest.raises(
        TargetSinkCertificationError,
        match="completion evidence is weaker",
    ):
        await certify_target_sink(
            dataclasses.replace(
                scenario,
                completion_evidence=acknowledged_only,
            )
        )


@pytest.mark.asyncio
async def test_common_certification_requires_failure_injection_for_atomicity() -> None:
    with pytest.raises(
        TargetSinkCertificationError,
        match="atomicity is claimed without a failure-injection hook",
    ):
        await certify_target_sink(
            dataclasses.replace(_scenario(), apply_with_failure_after=None)
        )


@pytest.mark.asyncio
async def test_common_certification_requires_segment_failure_injection() -> None:
    with pytest.raises(
        TargetSinkCertificationError,
        match="segmentation is claimed without a segment-failure hook",
    ):
        await certify_target_sink(
            dataclasses.replace(_scenario(), apply_segmented_with_failure_after=None)
        )


@pytest.mark.asyncio
async def test_common_certification_rejects_batches_over_declared_limits() -> None:
    scenario = _scenario()

    async def oversized_batches() -> tuple[tuple[_Action, ...], ...]:
        return (scenario.actions,)

    with pytest.raises(
        TargetSinkCertificationError,
        match="more actions than the declared limit",
    ):
        await certify_target_sink(
            dataclasses.replace(scenario, observed_batches=oversized_batches)
        )


def _sink_factory_calls(path: Path) -> list[tuple[str, ast.Call]]:
    tree = ast.parse(path.read_text(), filename=str(path))
    parents = {
        child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)
    }
    calls: list[tuple[str, ast.Call]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr in {"from_fn", "from_async_fn"}:
            owner: str | None = None
            ancestor: ast.AST = node
            while ancestor in parents:
                ancestor = parents[ancestor]
                if isinstance(ancestor, ast.ClassDef):
                    owner = ancestor.name
                    break
                if isinstance(ancestor, (ast.Assign, ast.AnnAssign)):
                    targets = (
                        ancestor.targets
                        if isinstance(ancestor, ast.Assign)
                        else [ancestor.target]
                    )
                    if targets and isinstance(targets[0], ast.Name):
                        owner = targets[0].id
            assert owner is not None, (
                f"could not identify sink owner at {path}:{node.lineno}"
            )
            calls.append((f"{path.parent.name}:{owner}", node))
    return calls


def _positive_capability_claims(call: ast.Call) -> set[str]:
    capabilities = next(
        (keyword.value for keyword in call.keywords if keyword.arg == "capabilities"),
        None,
    )
    assert isinstance(capabilities, ast.Call)
    values = {
        keyword.arg: keyword.value.value
        for keyword in capabilities.keywords
        if keyword.arg is not None and isinstance(keyword.value, ast.Constant)
    }
    positive: set[str] = set()
    if values.get("batch_atomicity") in {"per_action", "per_apply"}:
        positive.add("batch_atomicity")
    if values.get("idempotent_replay") == "supported":
        positive.add("idempotent_replay")
    if values.get("segmented_replay_safe") == "supported":
        positive.add("segmented_replay_safe")
    if values.get("apply_ordering") == "input_order":
        positive.add("apply_ordering")
    if values.get("cancellation_safe") == "supported":
        positive.add("cancellation_safe")
    if values.get("completion_verification") in {
        "acknowledged",
        "query_verified",
    }:
        positive.add("completion_verification")
    if values.get("max_batch_actions") is not None:
        positive.add("max_batch_actions")
    if values.get("max_batch_bytes") is not None:
        positive.add("max_batch_bytes")
    return positive


def test_every_builtin_target_sink_has_an_explicit_inline_contract() -> None:
    connectors_dir = Path(__file__).parents[2] / "synor" / "connectors"
    target_files = sorted(connectors_dir.glob("*/_target.py"))
    seen_connectors: set[str] = set()
    sink_calls: dict[str, ast.Call] = {}
    sink_count = 0

    for path in target_files:
        calls = _sink_factory_calls(path)
        if not calls:
            continue
        seen_connectors.add(path.parent.name)
        sink_count += len(calls)
        for sink_id, call in calls:
            assert sink_id not in sink_calls
            sink_calls[sink_id] = call
            capabilities = next(
                (
                    keyword.value
                    for keyword in call.keywords
                    if keyword.arg == "capabilities"
                ),
                None,
            )
            assert isinstance(capabilities, ast.Call), (
                f"{path}:{call.lineno} must pass an explicit TargetSinkCapabilities"
            )
            assert isinstance(capabilities.func, ast.Attribute)
            assert capabilities.func.attr == "TargetSinkCapabilities"

    assert seen_connectors == {
        "bigquery",
        "doris",
        "falkordb",
        "iggy",
        "kafka",
        "lancedb",
        "localfs",
        "neo4j",
        "postgres",
        "qdrant",
        "snowflake",
        "sqlite",
        "surrealdb",
        "turbopuffer",
        "valkey",
        "zvec",
    }
    assert sink_count == 39

    repository_root = Path(__file__).parents[3]
    manifest_path = repository_root / "dev" / "target-sink-certification.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["schema_version"] == 1
    records: dict[str, dict[str, object]] = manifest["sinks"]
    assert records.keys() == sink_calls.keys()

    for sink_id, call in sink_calls.items():
        record = records[sink_id]
        certified = set(cast(list[str], record["certified_capabilities"]))
        evidence = cast(list[str], record["evidence"])
        positive_claims = _positive_capability_claims(call)

        assert certified == positive_claims, (
            f"{sink_id} has positive claims without matching certification evidence"
        )
        assert record["status"] == ("partially_certified" if certified else "declared")
        assert record["runtime_tier"] in {"local", "external_service"}
        assert bool(evidence) == bool(certified)

        for evidence_id in evidence:
            evidence_path, *test_parts = evidence_id.split("::")
            source = repository_root / evidence_path
            assert source.is_file(), f"missing certification evidence {evidence_id}"
            assert test_parts and f"def {test_parts[-1]}" in source.read_text(), (
                f"missing certification test {evidence_id}"
            )
