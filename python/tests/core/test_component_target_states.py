from __future__ import annotations

import synor as syn
import synor.inspect as synor_inspect
import pytest

import dataclasses
from typing import Any, Collection, Generic

from tests import common
from tests.common.target_states import (
    DictsTarget,
    DictDataWithPrev,
    AsyncDictsTarget,
    AtMost,
)

synor_env = common.create_test_env(__file__)

_source_data: dict[str, dict[str, Any]] = {}


##################################################################################


async def _declare_dicts_data_together() -> None:
    with syn.unit_path("dict"):
        for name, data in _source_data.items():
            single_dict_provider = await syn.call(
                syn.unit_path(name),
                DictsTarget.declare_dict_target,
                name,
            )
            for key, value in data.items():
                syn.ensure_target_state(single_dict_provider.target_state(key, value))


def test_dicts_data_together_insert() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(name="test_dicts_data_together_insert", environment=synor_env),
        _declare_dicts_data_together,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(2), "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(3), "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(2), "upsert": 2}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "dict",
        syn.ROOT_PATH / "dict" / "D1",
        syn.ROOT_PATH / "dict" / "D2",
        syn.ROOT_PATH / "dict" / "D3",
    ]


def test_dicts_data_together_delete_dict() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(
            name="test_dicts_data_together_delete_dict", environment=synor_env
        ),
        _declare_dicts_data_together,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(2), "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "dict",
        syn.ROOT_PATH / "dict" / "D1",
        syn.ROOT_PATH / "dict" / "D2",
    ]

    del _source_data["D1"]
    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {
        "sink": AtMost(3),
        "insert": 1,
        "delete": 1,
    }
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(2), "upsert": 2}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "dict",
        syn.ROOT_PATH / "dict" / "D2",
        syn.ROOT_PATH / "dict" / "D3",
    ]

    # Re-insert after deletion
    _source_data["D1"] = {"a": 3, "c": 4}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(3), "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "dict",
        syn.ROOT_PATH / "dict" / "D1",
        syn.ROOT_PATH / "dict" / "D2",
        syn.ROOT_PATH / "dict" / "D3",
    ]


def test_dicts_data_together_delete_entry() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(
            name="test_dicts_data_together_delete_entry", environment=synor_env
        ),
        _declare_dicts_data_together,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1), "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}

    del _source_data["D1"]["a"]
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1)}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "delete": 1}

    # Re-insert after deletion
    _source_data["D1"]["a"] = 3
    _source_data["D1"]["c"] = 4
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1)}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "dict",
        syn.ROOT_PATH / "dict" / "D1",
    ]


##################################################################################


async def _declare_one_dict(name: str) -> None:
    dict_provider = await syn.call(
        syn.unit_path("setup"), DictsTarget.declare_dict_target, name
    )
    for key, value in _source_data[name].items():
        syn.ensure_target_state(dict_provider.target_state(key, value))


async def _declare_dicts_in_sub_components() -> None:
    for name in _source_data.keys():
        await syn.spawn(syn.unit_path(name), _declare_one_dict, name)


def test_dicts_in_sub_components_insert() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(
            name="test_dicts_in_sub_components_insert", environment=synor_env
        ),
        _declare_dicts_in_sub_components,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(2), "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(3), "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(2), "upsert": 2}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "D1",
        syn.ROOT_PATH / "D1" / "setup",
        syn.ROOT_PATH / "D2",
        syn.ROOT_PATH / "D2" / "setup",
        syn.ROOT_PATH / "D3",
        syn.ROOT_PATH / "D3" / "setup",
    ]


def test_dicts_in_sub_components_delete_dict() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(
            name="test_dicts_in_sub_components_delete_dict", environment=synor_env
        ),
        _declare_dicts_in_sub_components,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(2), "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "D1",
        syn.ROOT_PATH / "D1" / "setup",
        syn.ROOT_PATH / "D2",
        syn.ROOT_PATH / "D2" / "setup",
    ]

    del _source_data["D1"]
    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {
        "sink": AtMost(3),
        "insert": 1,
        "delete": 1,
    }
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(2), "upsert": 2}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "D2",
        syn.ROOT_PATH / "D2" / "setup",
        syn.ROOT_PATH / "D3",
        syn.ROOT_PATH / "D3" / "setup",
    ]

    # Re-insert after deletion
    _source_data["D1"] = {"a": 3, "c": 4}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(3), "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "D1",
        syn.ROOT_PATH / "D1" / "setup",
        syn.ROOT_PATH / "D2",
        syn.ROOT_PATH / "D2" / "setup",
        syn.ROOT_PATH / "D3",
        syn.ROOT_PATH / "D3" / "setup",
    ]


def test_dicts_in_sub_components_delete_entry() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(
            name="test_dicts_in_sub_components_delete_entry", environment=synor_env
        ),
        _declare_dicts_in_sub_components,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1), "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}

    del _source_data["D1"]["a"]
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1)}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "delete": 1}

    # Re-insert after deletion
    _source_data["D1"]["a"] = 3
    _source_data["D1"]["c"] = 4
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1)}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "D1",
        syn.ROOT_PATH / "D1" / "setup",
    ]


##################################################################################


@dataclasses.dataclass
class _DictContainers(Generic[syn.MaybePendingS], syn.ResolvesTo["_DictContainers"]):
    providers: dict[str, syn.TargetStateProvider[str, None, syn.MaybePendingS]]


def _declare_dict_containers(
    names: Collection[str],
) -> _DictContainers[syn.PendingS]:
    return _DictContainers[syn.PendingS](
        providers={name: DictsTarget.declare_dict_target(name) for name in names}
    )


def _declare_one_dict_data(name: str, provider: syn.TargetStateProvider[str]) -> None:
    for key, value in _source_data[name].items():
        syn.ensure_target_state(provider.target_state(key, value))


async def _declare_dict_containers_together() -> None:
    containers = await syn.call(
        syn.unit_path("setup"), _declare_dict_containers, _source_data.keys()
    )
    for name, provider in containers.providers.items():
        await syn.spawn(
            syn.unit_path(name), _declare_one_dict_data, name, provider
        )


@pytest.mark.asyncio
async def test_dicts_containers_together_insert() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(
            name="test_dicts_containers_together_insert", environment=synor_env
        ),
        _declare_dict_containers_together,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    await app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1), "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    await app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1), "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(2), "upsert": 2}
    assert await synor_inspect.list_stable_paths(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "D1",
        syn.ROOT_PATH / "D2",
        syn.ROOT_PATH / "D3",
        syn.ROOT_PATH / "setup",
    ]


@pytest.mark.asyncio
async def test_dicts_containers_together_delete_dict() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(
            name="test_dicts_containers_together_delete_dict",
            environment=synor_env,
        ),
        _declare_dict_containers_together,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    await app.update()
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1), "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}
    assert await synor_inspect.list_stable_paths(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "D1",
        syn.ROOT_PATH / "D2",
        syn.ROOT_PATH / "setup",
    ]

    del _source_data["D1"]
    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    await app.update()
    assert DictsTarget.store.data == {
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {
        "sink": AtMost(1),
        "insert": 1,
        "delete": 1,
    }
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(2), "upsert": 2}
    assert await synor_inspect.list_stable_paths(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "D2",
        syn.ROOT_PATH / "D3",
        syn.ROOT_PATH / "setup",
    ]

    # Re-insert after deletion
    _source_data["D1"] = {"a": 3, "c": 4}
    await app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1), "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}
    assert await synor_inspect.list_stable_paths(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "D1",
        syn.ROOT_PATH / "D2",
        syn.ROOT_PATH / "D3",
        syn.ROOT_PATH / "setup",
    ]


@pytest.mark.asyncio
async def test_dicts_containers_together_delete_entry() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(
            name="test_dicts_containers_together_delete_entry",
            environment=synor_env,
        ),
        _declare_dict_containers_together,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    await app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1), "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}

    del _source_data["D1"]["a"]
    await app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1)}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "delete": 1}

    # Re-insert after deletion
    _source_data["D1"]["a"] = 3
    _source_data["D1"]["c"] = 4
    await app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1)}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}
    assert await synor_inspect.list_stable_paths(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "D1",
        syn.ROOT_PATH / "setup",
    ]


##################################################################################
# Test for proceeding with failed creation


def test_proceed_with_failed_creation() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(name="test_proceed_with_failed_creation", environment=synor_env),
        _declare_dicts_data_together,
    )

    _source_data["D1"] = {"a": 1}
    try:
        DictsTarget.store.sink_exception = True
        with pytest.raises(Exception):
            app.update_blocking()
    finally:
        DictsTarget.store.sink_exception = False
    assert DictsTarget.store.data == {}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(1), "upsert": 1}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "dict",
        syn.ROOT_PATH / "dict" / "D1",
    ]


##################################################################################
# Test for prev_may_be_missing after a failed update


def test_prev_may_be_missing_after_failed_update() -> None:
    """A failed sink_apply leaves the target-state item with multiple possible
    states on disk (the value it had before the failed attempt, plus the value
    it tried to write). Both of those are real prior sink states — the actual
    sink content is guaranteed to be one of them — so a later run that observes
    these possible states must NOT set ``prev_may_be_missing=True``.

    Scenario:
      t1: declare a=1 (committed) -> possible states [1].
      t2: declare a=2, leaf sink fails -> possible states left as [1, 2].
      t3: declare a=2 again, sink ok -> reconcile must see prev=[1, 2] with
          prev_may_be_missing=False (not True).
    """
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(
            name="test_prev_may_be_missing_after_failed_update", environment=synor_env
        ),
        _declare_dicts_data_together,
    )

    # t1: insert a=1.
    _source_data["D1"] = {"a": 1}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {"a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True)},
    }

    # t2: update a=2, but make the leaf sink for D1 fail. The failed attempt
    # leaves the item with two possible states [1, 2] on disk; the sink itself
    # is untouched, so a=1 is still what's stored.
    _source_data["D1"]["a"] = 2
    leaf_store = DictsTarget.store._stores["D1"]
    try:
        leaf_store.sink_exception = True
        with pytest.raises(Exception):
            app.update_blocking()
    finally:
        leaf_store.sink_exception = False
    assert DictsTarget.store.data["D1"]["a"].data == 1

    # t3: declare a=2 again; the sink now succeeds. The item carries possible
    # states [1, 2], but both are real prior sink values, so prev_may_be_missing
    # must be False.
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {"a": DictDataWithPrev(data=2, prev=[1, 2], prev_may_be_missing=False)},
    }


##################################################################################
# Test for cleanup of partially-built components


async def _declare_one_dict_w_exception(name: str) -> None:
    dict_provider = await syn.call(
        syn.unit_path("setup"), DictsTarget.declare_dict_target, name
    )
    for key, value in _source_data[name].items():
        syn.ensure_target_state(dict_provider.target_state(key, value))
    raise ValueError("injected test exception (which is expected)")


async def _declare_dicts_in_sub_components_w_exception() -> None:
    for name in _source_data.keys():
        await syn.spawn(
            syn.unit_path(name), _declare_one_dict_w_exception, name
        )


def test_cleanup_partially_built_components() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(
            name="test_cleanup_partially_built_components", environment=synor_env
        ),
        _declare_dicts_in_sub_components_w_exception,
    )

    _source_data["D1"] = {"a": 1}
    app.update_blocking()
    assert DictsTarget.store.data == {"D1": {}}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "D1",
        syn.ROOT_PATH / "D1" / "setup",
    ]

    del _source_data["D1"]
    app.update_blocking()
    assert DictsTarget.store.data == {}
    assert synor_inspect.list_stable_paths_sync(app) == [syn.ROOT_PATH]


##################################################################################
# Test for restoring from GC-failed components


def test_retry_from_gc_failed_components() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(
            name="test_retry_from_gc_failed_components", environment=synor_env
        ),
        _declare_dicts_data_together,
    )

    _source_data["D1"] = {}
    app.update_blocking()
    assert DictsTarget.store.data == {"D1": {}}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "dict",
        syn.ROOT_PATH / "dict" / "D1",
    ]

    # Inject an exception for GC
    del _source_data["D1"]
    try:
        DictsTarget.store.sink_exception = True
        app.update_blocking()
    finally:
        DictsTarget.store.sink_exception = False
    assert DictsTarget.store.data == {"D1": {}}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "dict" / "D1",
    ]

    # After retry, it should proceed with GC
    app.update_blocking()
    assert DictsTarget.store.data == {}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
    ]


def test_restore_from_gc_failed_components() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(
            name="test_restore_from_gc_failed_components", environment=synor_env
        ),
        _declare_dicts_data_together,
    )

    _source_data["D1"] = {}
    app.update_blocking()
    assert DictsTarget.store.data == {"D1": {}}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "dict",
        syn.ROOT_PATH / "dict" / "D1",
    ]

    # Inject an exception for GC
    del _source_data["D1"]
    DictsTarget.store.sink_exception = True
    try:
        app.update_blocking()
    finally:
        DictsTarget.store.sink_exception = False
    assert DictsTarget.store.data == {"D1": {}}
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "dict" / "D1",
    ]

    # The entry reappears, and the previous failed GC shouldn't affect it
    _source_data["D1"] = {"a": 1}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {"a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True)}
    }
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "dict",
        syn.ROOT_PATH / "dict" / "D1",
    ]


##################################################################################
# Test for mount_each


async def _declare_dicts_in_sub_components_mount_each() -> None:
    await syn.spawn_each(_declare_one_dict, [(name, name) for name in _source_data])


@pytest.mark.asyncio
async def test_mount_each_insert() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(name="test_mount_each_insert", environment=synor_env),
        _declare_dicts_in_sub_components_mount_each,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    await app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(2), "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    await app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(3), "insert": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(2), "upsert": 2}
    _me = syn.Symbol("_declare_one_dict")
    assert await synor_inspect.list_stable_paths(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / _me,
        syn.ROOT_PATH / _me / "D1",
        syn.ROOT_PATH / _me / "D1" / "setup",
        syn.ROOT_PATH / _me / "D2",
        syn.ROOT_PATH / _me / "D2" / "setup",
        syn.ROOT_PATH / _me / "D3",
        syn.ROOT_PATH / _me / "D3" / "setup",
    ]


@pytest.mark.asyncio
async def test_mount_each_delete() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(name="test_mount_each_delete", environment=synor_env),
        _declare_dicts_in_sub_components_mount_each,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    await app.update()
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(2), "insert": 2}

    del _source_data["D1"]
    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    await app.update()
    assert DictsTarget.store.data == {
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {
        "sink": AtMost(3),
        "insert": 1,
        "delete": 1,
    }
    _me = syn.Symbol("_declare_one_dict")
    assert await synor_inspect.list_stable_paths(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / _me,
        syn.ROOT_PATH / _me / "D2",
        syn.ROOT_PATH / _me / "D2" / "setup",
        syn.ROOT_PATH / _me / "D3",
        syn.ROOT_PATH / _me / "D3" / "setup",
    ]

    # Re-insert after deletion
    _source_data["D1"] = {"a": 3, "c": 4}
    await app.update()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(3), "insert": 1}
    assert await synor_inspect.list_stable_paths(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / _me,
        syn.ROOT_PATH / _me / "D1",
        syn.ROOT_PATH / _me / "D1" / "setup",
        syn.ROOT_PATH / _me / "D2",
        syn.ROOT_PATH / _me / "D2" / "setup",
        syn.ROOT_PATH / _me / "D3",
        syn.ROOT_PATH / _me / "D3" / "setup",
    ]


##################################################################################
# Test for async target states


async def _declare_async_dicts_data_together() -> None:
    for name, data in _source_data.items():
        single_dict_provider = await syn.call(
            syn.unit_path("dict", name),
            AsyncDictsTarget.declare_dict_target,
            name,
        )
        for key, value in data.items():
            syn.ensure_target_state(single_dict_provider.target_state(key, value))


@pytest.mark.asyncio
async def test_async_dicts() -> None:
    AsyncDictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(name="test_async_dicts", environment=synor_env),
        _declare_async_dicts_data_together,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    await app.update()
    assert AsyncDictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert AsyncDictsTarget.store.metrics.collect() == {"sink": AtMost(2), "insert": 2}
    assert AsyncDictsTarget.store.collect_child_metrics() == {
        "sink": AtMost(1),
        "upsert": 2,
    }

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    await app.update()
    assert AsyncDictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert AsyncDictsTarget.store.metrics.collect() == {"sink": AtMost(3), "insert": 1}
    assert AsyncDictsTarget.store.collect_child_metrics() == {
        "sink": AtMost(2),
        "upsert": 2,
    }
    assert await synor_inspect.list_stable_paths(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "dict",
        syn.ROOT_PATH / "dict" / "D1",
        syn.ROOT_PATH / "dict" / "D2",
        syn.ROOT_PATH / "dict" / "D3",
    ]


def test_async_dicts_sync_app() -> None:
    AsyncDictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(name="test_async_dicts_sync_app", environment=synor_env),
        _declare_async_dicts_data_together,
    )

    _source_data["D1"] = {"a": 1, "b": 2}
    _source_data["D2"] = {}
    app.update_blocking()
    assert AsyncDictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert AsyncDictsTarget.store.metrics.collect() == {"sink": AtMost(2), "insert": 2}
    assert AsyncDictsTarget.store.collect_child_metrics() == {
        "sink": AtMost(1),
        "upsert": 2,
    }

    _source_data["D2"]["c"] = 3
    _source_data["D3"] = {"a": 4}
    app.update_blocking()
    assert AsyncDictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
        "D3": {
            "a": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert AsyncDictsTarget.store.metrics.collect() == {"sink": AtMost(3), "insert": 1}
    assert AsyncDictsTarget.store.collect_child_metrics() == {
        "sink": AtMost(2),
        "upsert": 2,
    }
    assert synor_inspect.list_stable_paths_sync(app) == [
        syn.ROOT_PATH,
        syn.ROOT_PATH / "dict",
        syn.ROOT_PATH / "dict" / "D1",
        syn.ROOT_PATH / "dict" / "D2",
        syn.ROOT_PATH / "dict" / "D3",
    ]


##################################################################################
# Tests for syn.attach_target()
##################################################################################


_mount_target_source_data: dict[str, dict[str, Any]] = {}


async def _declare_dicts_with_mount_target() -> None:
    with syn.unit_path("dict"):
        for name, data in _mount_target_source_data.items():
            single_dict_provider = await syn.attach_target(DictsTarget.dict_target(name))
            for key, value in data.items():
                syn.ensure_target_state(single_dict_provider.target_state(key, value))


def test_mount_target_insert() -> None:
    DictsTarget.store.clear()
    _mount_target_source_data.clear()

    app = syn.App(
        syn.AppConfig(name="test_mount_target_insert", environment=synor_env),
        _declare_dicts_with_mount_target,
    )

    _mount_target_source_data["D1"] = {"a": 1, "b": 2}
    _mount_target_source_data["D2"] = {}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {},
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(2), "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 2}

    # Verify stable paths contain the mount_target symbol
    paths = synor_inspect.list_stable_paths_sync(app)
    assert syn.ROOT_PATH in paths
    assert syn.ROOT_PATH / "dict" in paths


def test_mount_target_delete() -> None:
    DictsTarget.store.clear()
    _mount_target_source_data.clear()

    app = syn.App(
        syn.AppConfig(name="test_mount_target_delete", environment=synor_env),
        _declare_dicts_with_mount_target,
    )

    _mount_target_source_data["D1"] = {"a": 1, "b": 2}
    _mount_target_source_data["D2"] = {"c": 3}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
        "D2": {
            "c": DictDataWithPrev(data=3, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(2), "insert": 2}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(2), "upsert": 3}

    # Delete D2, modify D1
    del _mount_target_source_data["D2"]
    _mount_target_source_data["D1"]["c"] = 4
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
            "c": DictDataWithPrev(data=4, prev=[], prev_may_be_missing=True),
        },
    }
    assert DictsTarget.store.metrics.collect() == {"sink": AtMost(2), "delete": 1}
    assert DictsTarget.store.collect_child_metrics() == {"sink": AtMost(1), "upsert": 1}


##################################################################################
# Test: preview rejects child target providers
##################################################################################


def test_preview_rejects_child_target_providers() -> None:
    DictsTarget.store.clear()
    _source_data.clear()

    app = syn.App(
        syn.AppConfig(
            name="test_preview_rejects_child_target_providers", environment=synor_env
        ),
        _declare_dicts_data_together,
    )

    _source_data["D1"] = {"a": 1}
    with pytest.raises(Exception, match="child target providers"):
        app.update_blocking(preview=True)


##################################################################################
# Test: Directory -> Component transition child existence reconciliation
##################################################################################

_transition_to_component_mode = False


@syn.task
async def _dummy_leaf_component() -> None:
    pass


async def _declare_transition_to_component() -> None:
    with syn.unit_path("transition_test"):
        if not _transition_to_component_mode:
            single_dict_provider = await syn.attach_target(DictsTarget.dict_target("D1"))
            for key, value in _mount_target_source_data.get("D1", {}).items():
                syn.ensure_target_state(single_dict_provider.target_state(key, value))
        else:
            await syn.spawn(syn.unit_path("D1"), _dummy_leaf_component)


def test_directory_to_component_transition() -> None:
    global _transition_to_component_mode
    DictsTarget.store.clear()
    _mount_target_source_data.clear()
    _transition_to_component_mode = False

    app = syn.App(
        syn.AppConfig(
            name="test_directory_to_component_transition", environment=synor_env
        ),
        _declare_transition_to_component,
    )

    _mount_target_source_data["D1"] = {"a": 1, "b": 2}
    app.update_blocking()
    assert DictsTarget.store.data == {
        "D1": {
            "a": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
            "b": DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
        },
    }

    # Transition from Directory target to Component mount
    _transition_to_component_mode = True
    app.update_blocking()

    # The old children should be deleted from the target state
    assert DictsTarget.store.data == {}

    # Verify stable paths reflect the component is still there, but children are gone
    paths = synor_inspect.list_stable_paths_sync(app)
    assert syn.ROOT_PATH in paths
    assert syn.ROOT_PATH / "transition_test" in paths
    assert syn.ROOT_PATH / "transition_test" / "D1" in paths
    assert syn.ROOT_PATH / "transition_test" / "D1" / "a" not in paths
