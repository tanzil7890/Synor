"""Tests for concurrency control (max_inflight_components)."""

import os
import threading
import time

import pytest

import synor as syn

from tests.common import create_test_env

synor_env = create_test_env(__file__)


# ── Concurrency tracking ────────────────────────────────────────────────


class ConcurrencyTracker:
    """Thread-safe tracker for peak concurrent execution."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current = 0
        self._peak = 0
        self._total = 0

    def enter(self) -> None:
        with self._lock:
            self._current += 1
            self._total += 1
            self._peak = max(self._peak, self._current)

    def exit(self) -> None:
        with self._lock:
            self._current -= 1

    @property
    def peak(self) -> int:
        with self._lock:
            return self._peak

    @property
    def total(self) -> int:
        with self._lock:
            return self._total

    def reset(self) -> None:
        with self._lock:
            self._current = 0
            self._peak = 0
            self._total = 0


_tracker = ConcurrencyTracker()
_SLEEP = 0.1  # 100ms — enough to guarantee overlap between concurrent children


# ── Component functions ─────────────────────────────────────────────────


@syn.fn
def _slow_leaf() -> None:
    """Leaf component that sleeps to create overlapping execution windows."""
    _tracker.enter()
    try:
        time.sleep(_SLEEP)
    finally:
        _tracker.exit()


@syn.fn
def _noop() -> None:
    pass


@syn.fn
async def _child_mounts_grandchild() -> None:
    """Child that mounts a grandchild — tests permit release on first child mount."""
    await syn.mount(syn.component_subpath("gc"), _noop)


# ── Root functions ───────────────────────────────────────────────────────


async def _main_flat(count: int) -> None:
    """Mount *count* independent slow children."""
    for i in range(count):
        await syn.mount(syn.component_subpath(str(i)), _slow_leaf)


async def _main_nested() -> None:
    """Root → child → grandchild nesting."""
    for i in range(4):
        await syn.mount(syn.component_subpath(str(i)), _child_mounts_grandchild)


# ── Test 1: Quota enforcement ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_quota_limits_concurrency() -> None:
    """With max_inflight_components=2, at most 2 leaf components run at once."""
    _tracker.reset()
    app = syn.App(
        syn.AppConfig(
            name="test_quota_limits_concurrency",
            environment=synor_env,
            max_inflight_components=2,
        ),
        _main_flat,
        6,
    )
    await app.update()
    assert _tracker.total == 6
    assert _tracker.peak <= 2


def test_quota_one_serializes() -> None:
    """With max_inflight_components=1, components execute one at a time."""
    _tracker.reset()
    app = syn.App(
        syn.AppConfig(
            name="test_quota_one_serializes",
            environment=synor_env,
            max_inflight_components=1,
        ),
        _main_flat,
        4,
    )
    app.update_blocking()
    assert _tracker.total == 4
    assert _tracker.peak == 1


# ── Test 2: Deadlock prevention ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_deadlock_prevention() -> None:
    """Nested mount (parent → child → grandchild) with quota=2 completes without deadlock."""
    app = syn.App(
        syn.AppConfig(
            name="test_deadlock_prevention",
            environment=synor_env,
            max_inflight_components=2,
        ),
        _main_nested,
    )
    # If permit release on first child mount is broken, this deadlocks (test timeout fires).
    await app.update()


def test_deadlock_prevention_quota_one() -> None:
    """Even with quota=1, nested mount completes because parent releases permit."""
    app = syn.App(
        syn.AppConfig(
            name="test_deadlock_prevention_quota_one",
            environment=synor_env,
            max_inflight_components=1,
        ),
        _main_nested,
    )
    app.update_blocking()


# ── Test 3: Default limit (1024) ──────────────────────────────────────────


def test_default_limit() -> None:
    """Without max_inflight_components, the default limit of 1024 applies."""
    _tracker.reset()
    app = syn.App(
        syn.AppConfig(name="test_default_limit", environment=synor_env),
        _main_flat,
        6,
    )
    app.update_blocking()
    assert _tracker.total == 6
    # Default limit is 1024, far above 6, so all 6 children overlap → peak should exceed 2
    assert _tracker.peak > 2


# ── Test 4: Env var fallback ─────────────────────────────────────────────


def test_env_var_fallback() -> None:
    """SYNOR_MAX_INFLIGHT_COMPONENTS env var is used when AppConfig omits it."""
    _tracker.reset()
    original = os.environ.get("SYNOR_MAX_INFLIGHT_COMPONENTS")
    try:
        os.environ["SYNOR_MAX_INFLIGHT_COMPONENTS"] = "2"
        app = syn.App(
            syn.AppConfig(name="test_env_var_fallback", environment=synor_env),
            _main_flat,
            6,
        )
        app.update_blocking()
    finally:
        if original is None:
            os.environ.pop("SYNOR_MAX_INFLIGHT_COMPONENTS", None)
        else:
            os.environ["SYNOR_MAX_INFLIGHT_COMPONENTS"] = original

    assert _tracker.total == 6
    assert _tracker.peak <= 2
