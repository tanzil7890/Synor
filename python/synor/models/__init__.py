"""Local model adapters that never require a hosted inference service."""

from .local import (
    CallableLocalModel,
    LlamaCppLocalModel,
    LocalModel,
    LocalModelResponse,
)

__all__ = [
    "CallableLocalModel",
    "LlamaCppLocalModel",
    "LocalModel",
    "LocalModelResponse",
]
