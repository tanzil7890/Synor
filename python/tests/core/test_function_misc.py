import pytest
import synor as syn


@syn.task.as_async
def async_wrapped_fn_1(s: str, i: int) -> str:
    return f"{s} {i}"


@syn.task.as_async()
def async_wrapped_fn_2(s: str, i: int) -> str:
    return f"{s} {i}"


@pytest.mark.asyncio
async def test_async_wrapped_fn() -> None:
    assert await async_wrapped_fn_1("Hello", 3) == "Hello 3"
    assert await async_wrapped_fn_2("Hello", 3) == "Hello 3"


@syn.task
def sync_fn(s: str, i: int) -> str:
    return f"{s} {i}"


def test_sync_fn_callable_standalone() -> None:
    # A @syn.task is callable outside any component context: it runs the raw
    # function with no memoization, mirroring the async __call__ path.
    assert sync_fn("Hello", 3) == "Hello 3"


class _StandaloneHolder:
    def __init__(self, factor: int) -> None:
        self._factor = factor

    @syn.task(version=1, logic_tracking="self")
    def run(self, x: int) -> int:
        return x * self._factor


def test_sync_method_fn_standalone() -> None:
    # A versioned, self-tracked @syn.task method is directly callable outside a
    # component context.
    assert _StandaloneHolder(2).run(21) == 42
