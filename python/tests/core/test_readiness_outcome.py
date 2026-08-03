import asyncio
import gc
import inspect
import traceback
import weakref
from types import TracebackType
from typing import Any, cast

import pytest
import synor as syn

from tests import common


def test_spawn_handle_outcome_is_repeatable_and_ready_stays_compatible() -> None:
    environment = common.create_test_env(__file__, suffix="succeeded")
    observed: list[syn.ReadinessOutcome] = []

    @syn.task
    async def child() -> None:
        pass

    @syn.task
    async def root() -> None:
        handle = await syn.spawn(syn.unit_path("child"), child)
        first = await handle.outcome()
        second = await handle.outcome()
        assert isinstance(first, syn.Succeeded)
        assert second is first
        await handle.ready()
        await handle.ready()
        observed.append(first)

    app = syn.App(
        syn.AppConfig(name="typed-readiness-succeeded", environment=environment),
        root,
    )
    app.update_blocking()

    assert len(observed) == 1
    assert isinstance(observed[0], syn.Succeeded)


def test_failed_outcome_preserves_exception_without_leaking_routing_marker() -> None:
    environment = common.create_test_env(__file__, suffix="failed")
    observed: list[syn.Failed] = []

    @syn.task
    async def child() -> None:
        raise ValueError("typed readiness failure")

    @syn.task
    async def root() -> None:
        handle = await syn.spawn(syn.unit_path("child"), child)
        outcome = await handle.outcome()
        assert isinstance(outcome, syn.Failed)
        assert isinstance(outcome.error, ValueError)
        assert str(outcome.error) == "typed readiness failure"
        assert not hasattr(outcome.error, "__synor_failure_reported_v1__")
        observed.append(outcome)

        with pytest.raises(ValueError, match="typed readiness failure"):
            await handle.ready()
        with pytest.raises(ValueError, match="typed readiness failure"):
            await handle.ready()

    app = syn.App(
        syn.AppConfig(name="typed-readiness-failed", environment=environment),
        root,
    )
    with pytest.raises(ValueError, match="typed readiness failure"):
        app.update_blocking()

    assert len(observed) == 1

    # The exception is public data now, not an internal delivery token. Raising
    # the same object in an independent operation must report that new failure.
    reports: list[str] = []

    def report(exc: BaseException, _ctx: syn.ExceptionContext) -> None:
        reports.append(str(exc))

    retry_environment = common.create_test_env(
        __file__, suffix="failed-reraised", exception_handler=report
    )
    reused_error = observed[0].error

    @syn.task
    async def reraised_child() -> None:
        raise reused_error

    @syn.task
    async def reraised_root() -> None:
        await syn.spawn(syn.unit_path("reraised-child"), reraised_child)

    retry_app = syn.App(
        syn.AppConfig(
            name="typed-readiness-failed-reraised", environment=retry_environment
        ),
        reraised_root,
    )
    with pytest.raises(ValueError, match="typed readiness failure"):
        retry_app.update_blocking()

    assert len(reports) == 1
    assert "typed readiness failure" in reports[0]


def test_failed_outcome_preserves_custom_exception_identity_and_attributes() -> None:
    environment = common.create_test_env(__file__, suffix="custom-failed")

    class NeedsKeyword(Exception):
        def __init__(self, message: str, *, code: int) -> None:
            super().__init__(message)
            self.code = code

    failure = NeedsKeyword("custom readiness failure", code=7)
    observed: list[syn.Failed] = []
    payload_refs: list[weakref.ReferenceType[object]] = []
    raise_lines: list[int] = []

    class FramePayload:
        pass

    @syn.task
    async def child() -> None:
        frame_payload = FramePayload()
        payload_refs.append(weakref.ref(frame_payload))
        frame = inspect.currentframe()
        assert frame is not None
        raise_lines.append(frame.f_lineno + 1)
        raise failure

    @syn.task
    async def root() -> None:
        handle = await syn.spawn(syn.unit_path("child"), child)
        outcome = await handle.outcome()
        assert isinstance(outcome, syn.Failed)
        assert outcome.error is failure
        assert isinstance(outcome.error, NeedsKeyword)
        assert outcome.error.code == 7
        assert outcome.error.__traceback__ is not None
        frames = traceback.extract_tb(outcome.error.__traceback__)
        child_frame = next(frame for frame in frames if frame.name == "child")
        assert child_frame.lineno == raise_lines[0]
        assert child_frame.line == "raise failure"

        cursor: TracebackType | None = outcome.error.__traceback__
        while cursor is not None:
            if cursor.tb_frame.f_code.co_name == "child":
                assert "frame_payload" not in cursor.tb_frame.f_locals
            cursor = cursor.tb_next
        gc.collect()
        assert payload_refs[0]() is None
        assert any(
            note.startswith("Original component traceback:")
            for note in getattr(outcome.error, "__notes__", ())
        )
        observed.append(outcome)

        with pytest.raises(NeedsKeyword) as caught:
            await handle.ready()
        assert caught.value is failure
        assert caught.value.code == 7

    app = syn.App(
        syn.AppConfig(name="typed-readiness-custom-failed", environment=environment),
        root,
    )
    with pytest.raises(NeedsKeyword) as caught:
        app.update_blocking()

    assert caught.value is failure
    assert caught.value.code == 7
    assert len(observed) == 1


def test_detached_traceback_fallback_bypasses_hostile_exception_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = common.create_test_env(__file__, suffix="hostile-traceback")

    class HostileTracebackError(Exception):
        def __init__(self, message: str, *, token: object) -> None:
            super().__init__(message)
            self.token = token

        def __getattribute__(self, name: str) -> Any:
            if name in {"__traceback__", "__cause__", "__context__"}:
                raise RuntimeError(f"hostile read of {name}")
            return super().__getattribute__(name)

        def __setattr__(self, name: str, value: Any) -> None:
            # Context managers legitimately restore a non-None traceback while
            # propagating the exception. Reject only the clearing operation
            # that the old Python-level fallback attempted.
            if name == "__traceback__" and value is None:
                raise RuntimeError("hostile traceback write")
            super().__setattr__(name, value)

    token = object()
    failure = HostileTracebackError("hostile readiness failure", token=token)
    payload_refs: list[weakref.ReferenceType[object]] = []
    observed: list[syn.Failed] = []

    class FramePayload:
        pass

    def reject_frame_clear(_traceback: TracebackType) -> None:
        raise RuntimeError("synthetic clear_frames failure")

    # Force the native fallback path. It must bypass both hostile attribute
    # hooks and clear the linked cause's C-level traceback slot as well.
    monkeypatch.setattr(traceback, "clear_frames", reject_frame_clear)

    @syn.task
    async def child() -> None:
        frame_payload = FramePayload()
        payload_refs.append(weakref.ref(frame_payload))
        try:
            raise LookupError("linked readiness cause")
        except LookupError as cause:
            raise failure from cause

    @syn.task
    async def root() -> None:
        handle = await syn.spawn(syn.unit_path("child"), child)
        outcome = await handle.outcome()

        # Successfully returning the typed outcome also proves the recovery
        # path did not leak a pending PyErr from hostile introspection.
        assert isinstance(outcome, syn.Failed)
        assert outcome.error is failure
        assert outcome.error.token is token
        assert BaseException.__getattribute__(failure, "__traceback__") is None
        cause = BaseException.__getattribute__(failure, "__cause__")
        assert isinstance(cause, LookupError)
        assert BaseException.__getattribute__(cause, "__traceback__") is None
        gc.collect()
        assert payload_refs[0]() is None
        observed.append(outcome)

        with pytest.raises(HostileTracebackError) as caught:
            await handle.ready()
        assert caught.value is failure

    app = syn.App(
        syn.AppConfig(
            name="typed-readiness-hostile-traceback", environment=environment
        ),
        root,
    )
    with pytest.raises(HostileTracebackError) as caught:
        app.update_blocking()

    assert caught.value is failure
    assert len(observed) == 1


def test_live_update_outcome_exposes_superseded_operation() -> None:
    environment = common.create_test_env(__file__, suffix="superseded")
    observed: list[syn.ReadinessOutcome] = []

    @syn.task
    async def no_op() -> None:
        pass

    class LiveUpdates:
        async def process(self) -> None:
            pass

        async def process_live(self, operator: syn.LiveComponentOperator) -> None:
            await operator.update_full()
            started = asyncio.Event()
            release = asyncio.Event()

            @syn.task
            async def blocked() -> None:
                started.set()
                await release.wait()

            first = await operator.update(syn.unit_path("item"), blocked)
            await started.wait()
            displaced = await operator.update(syn.unit_path("item"), no_op)
            replacement = await operator.update(syn.unit_path("item"), no_op)

            outcome = await displaced.outcome()
            observed.append(outcome)
            assert isinstance(outcome, syn.Superseded)
            await displaced.ready()  # Compatibility: supersession is a no-op success.

            release.set()
            await first.ready()
            await replacement.ready()
            await operator.mark_ready()

    @syn.task
    async def root() -> None:
        await syn.spawn(syn.unit_path("live"), LiveUpdates)

    app = syn.App(
        syn.AppConfig(name="typed-readiness-superseded", environment=environment),
        root,
    )
    app.update_blocking()

    assert len(observed) == 1
    assert isinstance(observed[0], syn.Superseded)


class _StubCoreHandle:
    def __init__(self, kind: str, error: BaseException | None = None) -> None:
        self._kind = kind
        self._error = error

    async def outcome_async(self) -> tuple[str, BaseException | None]:
        return self._kind, self._error

    async def ready_async(self) -> None:
        if self._kind == "failed":
            assert self._error is not None
            raise self._error
        if self._kind == "cancelled":
            raise asyncio.CancelledError


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "expected_type"),
    [
        ("succeeded", syn.Succeeded),
        ("cancelled", syn.Cancelled),
        ("superseded", syn.Superseded),
    ],
)
async def test_spawn_handle_maps_native_terminal_variants(
    kind: str, expected_type: type[syn.ReadinessOutcome]
) -> None:
    native = cast(Any, _StubCoreHandle(kind))
    handle = syn.SpawnHandle([native])
    outcome = await handle.outcome()
    assert isinstance(outcome, expected_type)
    assert handle._cores == []


@pytest.mark.asyncio
async def test_public_outcome_releases_native_failure_and_ready_restores_routing() -> (
    None
):
    marker = "__synor_failure_reported_v1__"
    error = ValueError("reported failure")
    setattr(error, marker, True)
    native = cast(Any, _StubCoreHandle("failed", error))
    handle = syn.SpawnHandle([native])

    outcome = await handle.outcome()

    assert isinstance(outcome, syn.Failed)
    assert outcome.error is error
    assert handle._cores == []
    assert not hasattr(error, marker)

    with pytest.raises(ValueError) as caught:
        await handle.ready()
    assert caught.value is error
    assert getattr(error, marker) is True
