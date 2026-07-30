"""Module v3 for transitive logic_tracking chain tests.

baz changed (vs v1). bar_self and foo_full unchanged.
"""

import synor as syn
from tests.common.target_states import Metrics

_metrics: Metrics | None = None


def set_metrics(metrics: Metrics) -> None:
    global _metrics
    _metrics = metrics


@syn.fn
def baz(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("baz")
    return "baz_v3: " + s


@syn.fn(memo=True, logic_tracking="self")
def bar_self(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("bar_self")
    return baz(s)


@syn.fn(memo=True, logic_tracking="full")
def foo_full(key: str, value: str) -> str:
    assert _metrics is not None
    _metrics.increment("foo_full")
    return bar_self(value)
