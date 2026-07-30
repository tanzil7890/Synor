from __future__ import annotations

import pathlib
import typing

import pytest

from synor.models import CallableLocalModel, LlamaCppLocalModel, LocalModelResponse


@pytest.mark.asyncio
async def test_callable_local_model_supports_sync_and_async() -> None:
    sync_model = CallableLocalModel(lambda prompt: prompt.upper(), name="upper")

    async def reverse(prompt: str) -> str:
        return prompt[::-1]

    async_model = CallableLocalModel(reverse, name="reverse")
    assert await sync_model.generate("local") == LocalModelResponse(
        text="LOCAL",
        model="upper",
    )
    assert await async_model.generate("local") == LocalModelResponse(
        text="lacol",
        model="reverse",
    )


@pytest.mark.asyncio
async def test_callable_local_model_rejects_unsupported_result() -> None:
    invalid_callable = typing.cast(typing.Any, lambda _prompt: 42)
    model = CallableLocalModel(invalid_callable)
    with pytest.raises(TypeError, match="must return"):
        await model.generate("test")


def test_llama_cpp_requires_existing_local_file(tmp_path: pathlib.Path) -> None:
    with pytest.raises(FileNotFoundError):
        LlamaCppLocalModel(tmp_path / "missing.gguf")
