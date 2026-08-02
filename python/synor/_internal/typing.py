from __future__ import annotations

import uuid
from typing import Any, Mapping, NamedTuple, Sequence, TYPE_CHECKING, Union
from typing_extensions import TypeIs

if TYPE_CHECKING:
    from synor._internal.core import Fingerprint, Symbol

# --- StableKey type alias (accepted by StablePath.concat) ---
StableKey = Union[
    None, bool, int, str, bytes, uuid.UUID, "Symbol", tuple["StableKey", ...]
]

# --- Fingerprintable type alias (accepted by fingerprint_simple_object) ---
Fingerprintable = Union[
    None,
    bool,
    int,
    float,
    str,
    bytes,
    uuid.UUID,
    "Fingerprint",
    Sequence["Fingerprintable"],
    Mapping["Fingerprintable", "Fingerprintable"],
    set["Fingerprintable"],
]


class NotSetType:
    __slots__ = ()
    _instance: NotSetType | None = None

    def __new__(cls) -> NotSetType:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "NOT_SET"


NOT_SET = NotSetType()


def is_not_set(obj: Any) -> TypeIs[NotSetType]:
    return obj is NOT_SET


class AbsentType:
    __slots__ = ()
    _instance: AbsentType | None = None

    def __new__(cls) -> AbsentType:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "ABSENT"


ABSENT = AbsentType()


def is_absent(obj: Any) -> TypeIs[AbsentType]:
    return obj is ABSENT


class MemoStateOutcome(NamedTuple):
    """Return type for memo state functions (``__synor_memo_state__`` / registered ``state_fn``)."""

    state: Any
    """The current state value. Synor stores it for the next run."""

    memo_valid: bool = False
    """Whether the cached result is still valid."""
