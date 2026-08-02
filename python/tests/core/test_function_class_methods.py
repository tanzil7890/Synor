"""Tests for @syn.task decorator on class methods."""

from dataclasses import dataclass

import synor as syn

from tests import common
from tests.common.target_states import (
    DictDataWithPrev,
    GlobalDictTarget,
    Metrics,
)

synor_env = common.create_test_env(__file__)

_metrics = Metrics()


@dataclass(frozen=True)
class SourceDataEntry:
    name: str
    version: int
    content: str

    def __synor_memo_key__(self) -> object:
        return (self.name, self.version)


# ============================================================================
# Test 1: Regular Instance Methods
# ============================================================================


class Processor:
    """Test class with regular instance methods."""

    def __init__(self, prefix: str):
        self.prefix = prefix

    @syn.task(cache=True)
    def transform(self, entry: SourceDataEntry) -> str:
        _metrics.increment("call.transform")
        return f"{self.prefix}: {entry.content}"

    @syn.task
    def process_entry(self, key: str, entry: SourceDataEntry) -> None:
        transformed = self.transform(entry)  # type: ignore[call-arg, arg-type]
        syn.ensure_target_state(GlobalDictTarget.target_state(key, transformed))


def test_regular_method() -> None:
    """Test @syn.task on regular instance methods."""
    GlobalDictTarget.store.clear()
    _metrics.clear()

    processor = Processor("processed")
    source_data = {
        "A": SourceDataEntry(name="A", version=1, content="contentA"),
        "B": SourceDataEntry(name="B", version=1, content="contentB"),
    }

    @syn.task
    def process_all() -> None:
        for key, entry in source_data.items():
            processor.process_entry(key, entry)  # type: ignore[call-arg, arg-type]

    app = syn.App(
        syn.AppConfig(name="test_regular_method", environment=synor_env),
        process_all,
    )

    app.update_blocking()
    assert _metrics.collect() == {"call.transform": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="processed: contentA", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="processed: contentB", prev=[], prev_may_be_missing=True
        ),
    }

    app.update_blocking()
    assert _metrics.collect() == {}


def test_regular_method_memo_key_on_self() -> None:
    """memo_key should allow method memoization to depend on selected self state."""
    _metrics.clear()

    processor = MemoKeyProcessor("processed")

    @syn.task
    def run() -> None:
        first = processor.transform(
            SourceDataEntry(name="A", version=1, content="contentA1")
        )
        second = processor.transform(
            SourceDataEntry(name="A", version=1, content="contentA2")
        )
        processor.noise = "changed"
        third = processor.transform(
            SourceDataEntry(name="A", version=1, content="contentA3")
        )
        processor.prefix = "updated"
        fourth = processor.transform(
            SourceDataEntry(name="A", version=1, content="contentA4")
        )

        assert first == "processed: contentA1"
        assert second == "processed: contentA1"
        assert third == "processed: contentA1"
        assert fourth == "updated: contentA4"

    app = syn.App(
        syn.AppConfig(
            name="test_regular_method_memo_key_on_self", environment=synor_env
        ),
        run,
    )

    app.update_blocking()
    assert _metrics.collect() == {"call.memo_key_transform": 2}


class MemoKeyProcessor:
    def __init__(self, prefix: str):
        self.prefix = prefix
        self.noise = "noise"

    @syn.task(cache=True, memo_key={"self": lambda self: self.prefix})
    def transform(self, entry: SourceDataEntry) -> str:
        _metrics.increment("call.memo_key_transform")
        return f"{self.prefix}: {entry.content}"


# ============================================================================
# Test 2: Static Methods
# ============================================================================


class StaticProcessor:
    """Test class with static methods."""

    @staticmethod
    @syn.task(cache=True)
    def transform(entry: SourceDataEntry) -> str:
        """Static method with memoization."""
        _metrics.increment("call.static_transform")
        return f"static: {entry.content}"

    @staticmethod
    @syn.task
    def process_entry(key: str, entry: SourceDataEntry) -> None:
        """Static method that uses another memoized static method."""
        transformed = StaticProcessor.transform(entry)
        syn.ensure_target_state(GlobalDictTarget.target_state(key, transformed))


def test_static_method() -> None:
    """Test @syn.task on static methods."""
    GlobalDictTarget.store.clear()
    _metrics.clear()

    source_data = {
        "A": SourceDataEntry(name="A", version=1, content="contentA"),
        "B": SourceDataEntry(name="B", version=1, content="contentB"),
    }

    @syn.task
    def process_all() -> None:
        for key, entry in source_data.items():
            StaticProcessor.process_entry(key, entry)

    app = syn.App(
        syn.AppConfig(name="test_static_method", environment=synor_env),
        process_all,
    )

    app.update_blocking()
    assert _metrics.collect() == {"call.static_transform": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="static: contentA", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="static: contentB", prev=[], prev_may_be_missing=True
        ),
    }

    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# Test 3: Class Methods
# ============================================================================


class ClassProcessor:
    """Test class with class methods."""

    default_prefix = "class"

    @classmethod
    @syn.task(cache=True)
    def transform(cls, entry: SourceDataEntry) -> str:
        """Class method with memoization."""
        _metrics.increment("call.class_transform")
        return f"{cls.default_prefix}: {entry.content}"

    @classmethod
    @syn.task
    def process_entry(cls, key: str, entry: SourceDataEntry) -> None:
        """Class method that uses another memoized class method."""
        transformed = cls.transform(entry)  # type: ignore[call-arg, arg-type]
        syn.ensure_target_state(GlobalDictTarget.target_state(key, transformed))


def test_class_method() -> None:
    """Test @syn.task on class methods."""
    GlobalDictTarget.store.clear()
    _metrics.clear()

    source_data = {
        "A": SourceDataEntry(name="A", version=1, content="contentA"),
        "B": SourceDataEntry(name="B", version=1, content="contentB"),
    }

    @syn.task
    def process_all() -> None:
        for key, entry in source_data.items():
            ClassProcessor.process_entry(key, entry)  # type: ignore[call-arg, arg-type]

    app = syn.App(
        syn.AppConfig(name="test_class_method", environment=synor_env),
        process_all,
    )

    app.update_blocking()
    assert _metrics.collect() == {"call.class_transform": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="class: contentA", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="class: contentB", prev=[], prev_may_be_missing=True
        ),
    }

    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# Test 4: Async Instance Methods
# ============================================================================


class AsyncProcessor:
    """Test class with async instance methods."""

    def __init__(self, prefix: str):
        self.prefix = prefix

    @syn.task.as_async(cache=True)
    async def transform(self, entry: SourceDataEntry) -> str:
        """Async instance method with memoization."""
        _metrics.increment("call.async_transform")
        return f"{self.prefix}: {entry.content}"

    @syn.task
    async def process_entry(self, key: str, entry: SourceDataEntry) -> None:
        """Async instance method that uses another memoized async method."""
        transformed = await self.transform(entry)
        syn.ensure_target_state(GlobalDictTarget.target_state(key, transformed))


def test_async_method() -> None:
    """Test @syn.task on async instance methods."""
    GlobalDictTarget.store.clear()
    _metrics.clear()

    processor = AsyncProcessor("async")
    source_data = {
        "A": SourceDataEntry(name="A", version=1, content="contentA"),
        "B": SourceDataEntry(name="B", version=1, content="contentB"),
    }

    @syn.task
    async def process_all() -> None:
        for key, entry in source_data.items():
            await processor.process_entry(key, entry)

    app = syn.App(
        syn.AppConfig(name="test_async_method", environment=synor_env),
        process_all,
    )

    app.update_blocking()
    assert _metrics.collect() == {"call.async_transform": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="async: contentA", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="async: contentB", prev=[], prev_may_be_missing=True
        ),
    }

    app.update_blocking()
    assert _metrics.collect() == {}


# ============================================================================
# Test 5: Async Class Methods
# ============================================================================


class AsyncClassProcessor:
    """Test class with async class methods."""

    default_prefix = "async_class"

    @classmethod
    @syn.task.as_async(cache=True)
    async def transform(cls, entry: SourceDataEntry) -> str:
        """Async class method with memoization."""
        _metrics.increment("call.async_class_transform")
        return f"{cls.default_prefix}: {entry.content}"

    @classmethod
    @syn.task
    async def process_entry(cls, key: str, entry: SourceDataEntry) -> None:
        """Async class method that uses another memoized async class method."""
        transformed = await cls.transform(entry)  # type: ignore[call-arg, arg-type]
        syn.ensure_target_state(GlobalDictTarget.target_state(key, transformed))


def test_async_class_method() -> None:
    """Test @syn.task on async class methods."""
    GlobalDictTarget.store.clear()
    _metrics.clear()

    source_data = {
        "A": SourceDataEntry(name="A", version=1, content="contentA"),
        "B": SourceDataEntry(name="B", version=1, content="contentB"),
    }

    @syn.task
    async def process_all() -> None:
        for key, entry in source_data.items():
            await AsyncClassProcessor.process_entry(key, entry)  # type: ignore[call-arg, arg-type]

    app = syn.App(
        syn.AppConfig(name="test_async_class_method", environment=synor_env),
        process_all,
    )

    app.update_blocking()
    assert _metrics.collect() == {"call.async_class_transform": 2}
    assert GlobalDictTarget.store.data == {
        "A": DictDataWithPrev(
            data="async_class: contentA", prev=[], prev_may_be_missing=True
        ),
        "B": DictDataWithPrev(
            data="async_class: contentB", prev=[], prev_may_be_missing=True
        ),
    }

    app.update_blocking()
    assert _metrics.collect() == {}
