from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import synor as syn


@pytest.mark.asyncio
async def test_spawn_each_reduces_member_readiness_to_one_handle(
    tmp_path: Path,
) -> None:
    retained_handle_counts: list[int] = []

    @syn.task
    async def child(value: int) -> None:
        del value

    @syn.task
    async def main() -> None:
        handle = await syn.spawn_each(
            syn.unit_path("items"),
            child,
            ((index, index) for index in range(2_000)),
        )
        retained_handle_counts.append(len(handle._cores))
        await handle.ready()

    app = syn.App(
        syn.AppConfig(
            name="spawn-each-constant-readiness-state",
            environment=syn.Environment(syn.Settings(db_path=tmp_path / "state")),
        ),
        main,
    )

    await app.update()
    assert retained_handle_counts == [1]


@pytest.mark.asyncio
async def test_spawn_each_group_preserves_member_failure(tmp_path: Path) -> None:
    @syn.task
    async def child(value: int) -> None:
        if value == 17:
            raise ValueError("grouped child failed")

    @syn.task
    async def main() -> None:
        await syn.spawn_each(
            syn.unit_path("items"),
            child,
            ((index, index) for index in range(32)),
        )

    app = syn.App(
        syn.AppConfig(
            name="spawn-each-grouped-failure",
            environment=syn.Environment(syn.Settings(db_path=tmp_path / "state")),
        ),
        main,
    )

    with pytest.raises(ValueError, match="grouped child failed"):
        await app.update()


@pytest.mark.asyncio
async def test_spawn_each_stops_pulling_async_input_at_component_admission_window(
    tmp_path: Path,
) -> None:
    pulled = 0
    started = 0
    both_slots_started = asyncio.Event()
    release = asyncio.Event()

    async def items() -> AsyncIterator[tuple[int, int]]:
        nonlocal pulled
        for index in range(25):
            pulled += 1
            yield index, index

    @syn.task
    async def child(value: int) -> None:
        nonlocal started
        del value
        started += 1
        if started == 2:
            both_slots_started.set()
        await release.wait()

    @syn.task
    async def main() -> None:
        await syn.spawn_each(syn.unit_path("items"), child, items())

    app = syn.App(
        syn.AppConfig(
            name="spawn-each-async-input-admission",
            environment=syn.Environment(syn.Settings(db_path=tmp_path / "state")),
            max_inflight_components=2,
        ),
        main,
    )

    async def run_update() -> None:
        await app.update()

    update = asyncio.create_task(run_update())
    await asyncio.wait_for(both_slots_started.wait(), timeout=2)
    await asyncio.sleep(0.05)

    # The next item may be pulled before its mount waits for a permit, but the
    # async iterator cannot be drained beyond that single admission candidate.
    assert pulled <= 3
    assert not update.done()

    release.set()
    await update
    assert pulled == 25
    assert started == 25
