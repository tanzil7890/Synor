"""Tests for use_context() inside __synor_memo_state__().

Reproduces a bug where __synor_memo_state__() calls use_context() but the
component context is not properly plumbed when the memoized function is
mounted as a component via mount().
"""

from dataclasses import dataclass
from typing import Any

import synor as syn

from tests.common.environment import get_env_db_path
from tests.common.target_states import GlobalDictTarget, Metrics


_TEST_CTX_KEY = syn.ContextKey[str]("_test_memo_state_ctx_key", detect_change=True)


def _create_env(db_name: str, ctx_value: str) -> syn.Environment:
    ctx = syn.ContextProvider()
    ctx.provide(_TEST_CTX_KEY, ctx_value)
    settings = syn.Settings.from_env(db_path=get_env_db_path(db_name))
    return syn.Environment(settings, context_provider=ctx)


@dataclass
class ContextAwareEntry:
    """Entry whose __synor_memo_state__ calls use_context()."""

    name: str
    state_value: int
    content: str

    def __synor_memo_key__(self) -> object:
        return self.name

    async def __synor_memo_state__(self, prev_state: Any) -> syn.MemoStateOutcome:
        # This call is the crux of the bug: use_context() needs a valid
        # component context, which may not be set when the engine evaluates
        # memo state for mount() arguments.
        _ctx_val = syn.use_context(_TEST_CTX_KEY)
        memo_valid = not syn.is_absent(prev_state) and self.state_value == prev_state
        return syn.MemoStateOutcome(state=self.state_value, memo_valid=memo_valid)


# ============================================================================
# Test 1: Memoized function called directly (not mounted)
# ============================================================================

_source_data: dict[str, ContextAwareEntry] = {}
_metrics = Metrics()


@syn.task(cache=True)
def _transform_entry(entry: ContextAwareEntry) -> str:
    _metrics.increment("call.transform")
    return f"processed: {entry.content}"


@syn.task
def _process_data() -> None:
    for key, value in _source_data.items():
        transformed = _transform_entry(value)
        syn.ensure_target_state(GlobalDictTarget.target_state(key, transformed))


def test_memo_state_use_context_function_call() -> None:
    """use_context() inside __synor_memo_state__ works when function is called directly."""
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    env = _create_env("test_memo_state_ctx_fn", "val1")
    app = syn.App(
        syn.AppConfig(name="test_memo_state_ctx_fn", environment=env),
        _process_data,
    )

    _source_data["A"] = ContextAwareEntry(name="A", state_value=100, content="hello")
    app.update_blocking()
    assert _metrics.collect() == {"call.transform": 1}

    # Same state → memo valid, 0 calls
    app.update_blocking()
    assert _metrics.collect() == {}

    # State changes → re-executes
    _source_data["A"] = ContextAwareEntry(name="A", state_value=101, content="world")
    app.update_blocking()
    assert _metrics.collect() == {"call.transform": 1}


# ============================================================================
# Test 2: Memoized function mounted as component
# ============================================================================


@syn.task(cache=True)
def _declare_entry(entry: ContextAwareEntry) -> None:
    _metrics.increment("call.declare")
    syn.ensure_target_state(
        GlobalDictTarget.target_state(entry.name, f"comp: {entry.content}")
    )


@syn.task
async def _mount_entries() -> None:
    for key, value in _source_data.items():
        await syn.spawn(syn.unit_path(key), _declare_entry, value)


def test_memo_state_use_context_component_mount() -> None:
    """use_context() inside __synor_memo_state__ works when function is mounted as component."""
    GlobalDictTarget.store.clear()
    _source_data.clear()
    _metrics.clear()

    env = _create_env("test_memo_state_ctx_mount", "val1")
    app = syn.App(
        syn.AppConfig(name="test_memo_state_ctx_mount", environment=env),
        _mount_entries,
    )

    _source_data["A"] = ContextAwareEntry(name="A", state_value=100, content="hello")
    app.update_blocking()
    assert _metrics.collect() == {"call.declare": 1}

    # Same state → memo valid, 0 calls
    app.update_blocking()
    assert _metrics.collect() == {}

    # State changes → re-executes
    _source_data["A"] = ContextAwareEntry(name="A", state_value=101, content="world")
    app.update_blocking()
    assert _metrics.collect() == {"call.declare": 1}
