import pytest
import synor as syn
import synor.inspect as synor_inspect

from tests import common


@syn.task
async def _empty_app() -> None:
    pass


def _counts_tuple(
    counts: synor_inspect.NativeEffectCounts,
) -> tuple[int, int, int, int, int]:
    return (
        counts.pending,
        counts.verified,
        counts.failed,
        counts.blocked,
        counts.completed,
    )


@pytest.mark.asyncio
async def test_native_effect_counts_expose_only_aggregate_statuses() -> None:
    env = common.create_test_env(__file__)
    app = syn.App(
        syn.AppConfig(name="native_effect_counts", environment=env),
        _empty_app,
    )
    await app.update()

    counts = await synor_inspect.native_effect_counts(app)
    assert isinstance(counts, synor_inspect.NativeEffectCounts)
    assert _counts_tuple(counts) == (0, 0, 0, 0, 0)
    assert not hasattr(counts, "action_id")
    assert not hasattr(counts, "descriptor")
    assert not hasattr(counts, "effects")

    counts_by_name = await synor_inspect.native_effect_counts_by_name(
        env, "native_effect_counts"
    )
    assert counts_by_name is not None
    assert _counts_tuple(counts_by_name) == (0, 0, 0, 0, 0)

    missing = await synor_inspect.native_effect_counts_by_name(
        env, "missing_native_effect_app"
    )
    assert missing is None
