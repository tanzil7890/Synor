"""Module v2 for logic_tracking="self" tests.

bar changed (vs v1). foo_self unchanged (same source as v1).
"""

import synor as syn
from tests.common.target_states import Metrics

_metrics: Metrics | None = None


def set_metrics(metrics: Metrics) -> None:
    global _metrics
    _metrics = metrics


@syn.task
def bar(s: str) -> str:
    assert _metrics is not None
    _metrics.increment("bar")
    return "bar_v2: " + s


@syn.task(cache=True, logic_tracking="self")
def foo_self(key: str, value: str) -> str:
    assert _metrics is not None
    _metrics.increment("foo_self")
    return bar(value)
