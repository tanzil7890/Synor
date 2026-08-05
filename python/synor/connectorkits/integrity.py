"""Read-only connector contract for experimental integrity scans."""

from __future__ import annotations

__all__ = ["IntegrityInspector"]

from typing import TYPE_CHECKING as _TYPE_CHECKING
from typing import Protocol as _Protocol
from typing import runtime_checkable as _runtime_checkable

if _TYPE_CHECKING:
    from synor.integrity._model import InspectionPage as _InspectionPage


@_runtime_checkable
class IntegrityInspector(_Protocol):
    """A bounded, resumable, metadata-only provider inspector.

    Implementations must perform no provider mutation. ``descriptor_digest``
    identifies the configured provider scope and requested snapshot without
    exposing their raw names. Pages must honor ``limit`` and return facts sorted by
    :meth:`IntegrityFact.sort_key`.
    """

    @property
    def descriptor_digest(self) -> str: ...

    async def inspect_page(
        self,
        cursor: str | None,
        *,
        limit: int,
    ) -> _InspectionPage: ...
