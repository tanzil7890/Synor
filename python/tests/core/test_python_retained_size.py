from __future__ import annotations

import dataclasses
from collections.abc import Collection, Sequence
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pytest
import synor as syn
from numpy.typing import NDArray


class _PayloadAction(NamedTuple):
    payload: bytes


@dataclasses.dataclass(frozen=True, slots=True)
class _DataclassPayloadAction:
    payload: bytes


@dataclasses.dataclass(frozen=True, slots=True)
class _NumpyPayloadAction:
    payload: NDArray[np.uint8]


_Action = _PayloadAction | _DataclassPayloadAction | _NumpyPayloadAction


class _SizeLimitedStore:
    def __init__(self) -> None:
        self.applied = False
        self.sink = syn.TargetActionSink.from_fn(
            self._apply,
            capabilities=syn.TargetSinkCapabilities(max_batch_bytes=512),
        )

    def _apply(
        self,
        _context: syn.ContextProvider,
        _actions: Sequence[_Action],
        /,
    ) -> None:
        self.applied = True

    def reconcile(
        self,
        _key: syn.StableKey,
        desired: _Action | syn.AbsentType,
        _previous: Collection[_Action],
        _prev_may_be_missing: bool,
        /,
    ) -> syn.TargetReconcileOutput[_Action, _Action] | None:
        if syn.is_absent(desired):
            return None
        return syn.TargetReconcileOutput(
            action=desired,
            sink=self.sink,
            tracking_record=desired,
        )


_STORE = _SizeLimitedStore()
_PROVIDER = syn.register_root_target_states_provider(
    "test_python_retained_size/limited", _STORE
)


@pytest.mark.asyncio
async def test_named_tuple_bytes_payload_is_rejected_by_sink_byte_limit(
    tmp_path: Path,
) -> None:
    _STORE.applied = False

    @syn.task
    async def main() -> None:
        syn.ensure_target_state(
            _PROVIDER.target_state("record", _PayloadAction(bytes(4 * 1024)))
        )

    app = syn.App(
        syn.AppConfig(
            name="named-tuple-retained-size-limit",
            environment=syn.Environment(syn.Settings(db_path=tmp_path / "state")),
        ),
        main,
    )

    with pytest.raises(Exception, match="approximately .* bytes.*limit of 512"):
        await app.update()

    assert not _STORE.applied


@pytest.mark.asyncio
async def test_slot_dataclass_bytes_payload_is_rejected_by_sink_byte_limit(
    tmp_path: Path,
) -> None:
    _STORE.applied = False

    @syn.task
    async def main() -> None:
        syn.ensure_target_state(
            _PROVIDER.target_state(
                "slot-record", _DataclassPayloadAction(bytes(4 * 1024))
            )
        )

    app = syn.App(
        syn.AppConfig(
            name="slot-dataclass-retained-size-limit",
            environment=syn.Environment(syn.Settings(db_path=tmp_path / "state")),
        ),
        main,
    )

    with pytest.raises(Exception, match="approximately .* bytes.*limit of 512"):
        await app.update()

    assert not _STORE.applied


@pytest.mark.asyncio
async def test_numpy_native_buffer_payload_is_rejected_by_sink_byte_limit(
    tmp_path: Path,
) -> None:
    _STORE.applied = False

    @syn.task
    async def main() -> None:
        syn.ensure_target_state(
            _PROVIDER.target_state(
                "numpy-record", _NumpyPayloadAction(np.zeros(4 * 1024, dtype=np.uint8))
            )
        )

    app = syn.App(
        syn.AppConfig(
            name="numpy-retained-size-limit",
            environment=syn.Environment(syn.Settings(db_path=tmp_path / "state")),
        ),
        main,
    )

    with pytest.raises(Exception, match="approximately .* bytes.*limit of 512"):
        await app.update()

    assert not _STORE.applied
