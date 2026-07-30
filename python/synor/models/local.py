"""In-process model adapters for local-only inference."""

from __future__ import annotations

import asyncio as _asyncio
import dataclasses as _dataclasses
import inspect as _inspect
import pathlib as _pathlib
import typing as _typing

__all__ = [
    "CallableLocalModel",
    "LlamaCppLocalModel",
    "LocalModel",
    "LocalModelResponse",
]


@_dataclasses.dataclass(frozen=True, slots=True)
class LocalModelResponse:
    """Text returned by a local model."""

    text: str
    model: str


class LocalModel(_typing.Protocol):
    """Protocol shared by Synor local text-generation adapters."""

    async def generate(self, prompt: str) -> LocalModelResponse:
        """Generate text without sending the prompt to a network service."""


_LocalCallable = _typing.Callable[
    [str],
    str | LocalModelResponse | _typing.Awaitable[str | LocalModelResponse],
]


class CallableLocalModel:
    """Adapt an in-process sync or async function as a local model."""

    def __init__(self, fn: _LocalCallable, *, name: str = "local-callable") -> None:
        if not callable(fn):
            raise TypeError("fn must be callable")
        if not name.strip():
            raise ValueError("name must not be empty")
        self._fn = fn
        self._name = name

    async def generate(self, prompt: str) -> LocalModelResponse:
        """Run the function in process.

        Synchronous functions run in a worker thread so they do not block the
        pipeline's event loop.
        """

        if not isinstance(prompt, str):
            raise TypeError("prompt must be a string")
        if _inspect.iscoroutinefunction(self._fn):
            value = await self._fn(prompt)
        else:
            value = await _asyncio.to_thread(self._fn, prompt)
            if _inspect.isawaitable(value):
                value = await value
        if isinstance(value, LocalModelResponse):
            return value
        if not isinstance(value, str):
            raise TypeError(
                "local model callable must return str or LocalModelResponse"
            )
        return LocalModelResponse(text=value, model=self._name)

    def __synor_memo_key__(self) -> object:
        return (self._name, self._fn)


class LlamaCppLocalModel:
    """Load a GGUF model file with the optional ``llama-cpp-python`` package."""

    def __init__(
        self,
        model_path: str | _pathlib.Path,
        *,
        context_size: int = 4096,
    ) -> None:
        path = _pathlib.Path(model_path).expanduser()
        if not path.is_file():
            raise FileNotFoundError(f"local model file does not exist: {path}")
        if context_size <= 0:
            raise ValueError("context_size must be positive")
        self._model_path = path.resolve()
        self._context_size = context_size
        self._model: _typing.Any = None

    def _get_model(self) -> _typing.Any:
        if self._model is None:
            try:
                from llama_cpp import Llama  # type: ignore[import-not-found]
            except ImportError as error:
                raise ImportError(
                    "LlamaCppLocalModel requires llama-cpp-python. "
                    "Install the Synor local_models extra."
                ) from error
            self._model = Llama(
                model_path=str(self._model_path),
                n_ctx=self._context_size,
                verbose=False,
            )
        return self._model

    def _generate_blocking(self, prompt: str) -> LocalModelResponse:
        result = self._get_model()(prompt)
        try:
            text = result["choices"][0]["text"]
        except (KeyError, IndexError, TypeError) as error:
            raise RuntimeError(
                "llama.cpp returned an unsupported response shape"
            ) from error
        if not isinstance(text, str):
            raise RuntimeError("llama.cpp response text is not a string")
        return LocalModelResponse(text=text, model=self._model_path.name)

    async def generate(self, prompt: str) -> LocalModelResponse:
        """Generate text from the local model in a worker thread."""

        if not isinstance(prompt, str):
            raise TypeError("prompt must be a string")
        return await _asyncio.to_thread(self._generate_blocking, prompt)

    def __synor_memo_key__(self) -> object:
        return (str(self._model_path), self._context_size)
