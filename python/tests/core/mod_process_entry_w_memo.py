"""Module version with memo=True for testing memoization invalidation."""

import synor as syn
from tests.common.target_states import GlobalDictTarget, Metrics

# Shared metrics object to track calls across module reloads.
_metrics: Metrics | None = None


def set_metrics(metrics: Metrics) -> None:
    global _metrics
    _metrics = metrics


@syn.fn(memo=True)
def process_entry(key: str, value: str) -> None:
    assert _metrics is not None
    _metrics.increment("calls")
    syn.declare_target_state(GlobalDictTarget.target_state(key, value))
