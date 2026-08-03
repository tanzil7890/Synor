from collections.abc import Iterator

import pytest
import synor as syn
from synor._internal import environment as envmod

from tests import common
from tests.common.target_states import DictDataWithPrev, GlobalDictTarget


def test_global_exception_handler_invoked_for_background_mount() -> None:
    envmod.reset_default_env_for_tests()

    seen: list[tuple[str, str]] = []

    @syn.lifespan
    def _lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_global"
        )

        def handler(exc: BaseException, ctx: syn.ExceptionContext) -> None:
            seen.append((type(exc).__name__, ctx.mount_kind))

        builder.set_exception_handler(handler)
        yield

    @syn.task
    async def _child() -> None:
        raise ValueError("boom")

    @syn.task
    async def _root() -> None:
        await syn.spawn(syn.unit_path("child"), _child)

    app = syn.App("test_exception_handlers_global", _root)
    with pytest.raises(ValueError, match="boom"):
        app.update_blocking()

    assert seen == [("RuntimeError", "mount")]


def test_scoped_handler_overrides_global_and_fallback_on_handler_error() -> None:
    envmod.reset_default_env_for_tests()

    calls: list[str] = []

    @syn.lifespan
    def _lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_scoped"
        )

        def global_handler(exc: BaseException, ctx: syn.ExceptionContext) -> None:
            calls.append(f"global:{ctx.source}:{type(exc).__name__}")

        builder.set_exception_handler(global_handler)
        yield

    @syn.task
    async def _child() -> None:
        raise ValueError("boom")

    @syn.task
    async def _root() -> None:
        def inner_handler(exc: BaseException, ctx: syn.ExceptionContext) -> None:
            calls.append(f"inner:{ctx.source}:{type(exc).__name__}")
            raise RuntimeError("handler failed")

        async with syn.exception_handler(inner_handler):
            await syn.spawn(syn.unit_path("child"), _child)

    app = syn.App("test_exception_handlers_scoped", _root)
    with pytest.raises(ValueError, match="boom"):
        app.update_blocking()

    # Inner sees component exception, then raises; global receives handler exception.
    assert calls == [
        "inner:component:RuntimeError",
        "global:handler:RuntimeError",
    ]


def _raise_for_trace_test() -> None:
    raise ValueError("traceful boom")


_orphan_source: dict[str, int] = {}


def test_orphan_delete_failure_routes_through_parent_handler() -> None:
    """Build-mode cascade: when a parent's commit-phase GC sweep deletes a
    child component that's no longer mounted (orphan) and the cleanup
    sink fails, the parent's exception handler chain should see the
    failure — not the framework's default `error!` log.

    The reporting handler observes the failure exactly once, while the
    terminal sink error still propagates from the app update.
    """
    envmod.reset_default_env_for_tests()
    GlobalDictTarget.store.clear()
    GlobalDictTarget.store.sink_exception = False

    seen: list[tuple[str, str]] = []

    @syn.lifespan
    def _lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_orphan_cascade"
        )

        def handler(exc: BaseException, ctx: syn.ExceptionContext) -> None:
            seen.append((type(exc).__name__, ctx.mount_kind))

        builder.set_exception_handler(handler)
        yield

    @syn.task
    async def _child(value: int) -> None:
        syn.ensure_target_state(GlobalDictTarget.target_state("k", value))

    @syn.task
    async def _parent() -> None:
        for name, value in _orphan_source.items():
            await syn.spawn(syn.unit_path(name), _child, value)

    @syn.task
    async def _root() -> None:
        await syn.spawn(syn.unit_path("parent"), _parent)

    app = syn.App("test_exception_handlers_orphan_cascade", _root)

    # First update: mount child "A", child declares target state. Sink healthy.
    _orphan_source.clear()
    _orphan_source["A"] = 1
    app.update_blocking()
    assert GlobalDictTarget.store.data == {
        "k": DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
    }
    assert seen == []

    # Second update: source empty, child "A" is now an orphan. The parent's
    # commit-phase GC sweep deletes A; the cleanup sink fails. With §4.2,
    # the cascaded on_error fires the parent's handler.
    _orphan_source.clear()
    GlobalDictTarget.store.sink_exception = True
    try:
        with pytest.raises(ValueError, match="injected sink exception"):
            app.update_blocking()
    finally:
        GlobalDictTarget.store.sink_exception = False

    # Exactly one failure surfaces — the orphan-delete of "A". The handler's
    # mount_kind is "mount" because the on_error was wired through
    # `syn.spawn(..., parent_fn)` at the parent's mount time.
    assert len(seen) == 1, f"expected one handler call; got {seen}"
    exc_name, mount_kind = seen[0]
    assert exc_name == "RuntimeError"
    assert mount_kind == "mount"


def test_background_mount_failure_surfaces_python_traceback() -> None:
    """The handler should see the full Python traceback for a background mount failure,
    not just the exception message — the trace is what makes the error actionable."""
    envmod.reset_default_env_for_tests()

    seen_messages: list[str] = []

    @syn.lifespan
    def _lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_trace"
        )

        def handler(exc: BaseException, ctx: syn.ExceptionContext) -> None:
            seen_messages.append(str(exc))

        builder.set_exception_handler(handler)
        yield

    @syn.task
    async def _failing() -> None:
        _raise_for_trace_test()

    @syn.task
    async def _root() -> None:
        await syn.spawn(syn.unit_path("child"), _failing)

    app = syn.App("test_exception_handlers_trace", _root)
    with pytest.raises(ValueError, match="traceful boom"):
        app.update_blocking()

    assert len(seen_messages) == 1
    msg = seen_messages[0]
    assert "ValueError" in msg
    assert "traceful boom" in msg
    assert "Traceback (most recent call last)" in msg
    assert "_raise_for_trace_test" in msg


def test_background_failure_propagates_without_handler(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Default logging never converts failed background work into success."""
    envmod.reset_default_env_for_tests()

    @syn.lifespan
    def _lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_no_handler"
        )
        yield

    @syn.task
    async def _child() -> None:
        raise ValueError("unhandled background boom")

    @syn.task
    async def _root() -> None:
        await syn.spawn(syn.unit_path("child"), _child)

    app = syn.App("test_exception_handlers_no_handler", _root)
    with (
        caplog.at_level("ERROR"),
        pytest.raises(ValueError, match="unhandled background boom"),
    ):
        app.update_blocking()

    assert any(
        "unhandled background boom" in record.getMessage() for record in caplog.records
    )


def test_returning_handler_cannot_swallow_explicit_ready_failure() -> None:
    """Catching one handle await does not erase the aggregate terminal result."""
    envmod.reset_default_env_for_tests()
    reported: list[str] = []
    ready_errors: list[str] = []

    @syn.lifespan
    def _lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_explicit_ready"
        )

        def handler(exc: BaseException, ctx: syn.ExceptionContext) -> None:
            reported.append(ctx.stable_path)

        builder.set_exception_handler(handler)
        yield

    @syn.task
    async def _child() -> None:
        raise ValueError("ready boom")

    @syn.task
    async def _root() -> None:
        handle = await syn.spawn(syn.unit_path("child"), _child)
        try:
            await handle.ready()
        except ValueError as error:
            ready_errors.append(str(error))

    app = syn.App("test_exception_handlers_explicit_ready", _root)
    with pytest.raises(ValueError, match="ready boom"):
        app.update_blocking()

    assert ready_errors == ["ready boom"]
    assert len(reported) == 1


def test_reported_failure_stays_single_delivery_across_foreground_call() -> None:
    """A descendant failure keeps its delivery marker through Python.

    The grandchild is background work, while its readiness error crosses a
    foreground ``syn.call`` before reaching another background component. The
    same terminal failure must not be reported again at that outer boundary.
    """
    envmod.reset_default_env_for_tests()
    reported: list[str] = []

    @syn.lifespan
    def _lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_reported_failure_foreground_tunnel"
        )

        def handler(exc: BaseException, ctx: syn.ExceptionContext) -> None:
            reported.append(ctx.stable_path)

        builder.set_exception_handler(handler)
        yield

    @syn.task
    async def _grandchild() -> None:
        raise ValueError("single-delivery boom")

    @syn.task
    async def _foreground_child() -> None:
        handle = await syn.spawn(syn.unit_path("grandchild"), _grandchild)
        await handle.ready()

    @syn.task
    async def _background_parent() -> None:
        await syn.call(syn.unit_path("foreground"), _foreground_child)

    @syn.task
    async def _root() -> None:
        await syn.spawn(syn.unit_path("background"), _background_parent)

    app = syn.App("test_reported_failure_foreground_tunnel", _root)
    with pytest.raises(ValueError, match="single-delivery boom"):
        app.update_blocking()

    assert len(reported) == 1
    assert "grandchild" in reported[0]


def test_reported_failure_marker_does_not_survive_independent_reraise() -> None:
    """Internal delivery state must not become user-exception state.

    The first app reports a background failure and returns that same exception
    to application code. Raising the captured exception from a new background
    component is a new failure and must be observed by the handler again.
    """
    envmod.reset_default_env_for_tests()
    reported: list[str] = []

    @syn.lifespan
    def _lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_reported_failure_marker_lifetime"
        )

        def handler(exc: BaseException, ctx: syn.ExceptionContext) -> None:
            reported.append(ctx.stable_path)

        builder.set_exception_handler(handler)
        yield

    original = ValueError("independently raised boom")

    @syn.task
    async def _first_child() -> None:
        raise original

    @syn.task
    async def _first_root() -> None:
        await syn.spawn(syn.unit_path("first_child"), _first_child)

    first_app = syn.App("test_reported_marker_first_app", _first_root)
    with pytest.raises(ValueError, match="independently raised boom") as raised:
        first_app.update_blocking()

    escaped = raised.value

    @syn.task
    async def _second_child() -> None:
        raise escaped

    @syn.task
    async def _second_root() -> None:
        await syn.spawn(syn.unit_path("second_child"), _second_child)

    second_app = syn.App("test_reported_marker_second_app", _second_root)
    with pytest.raises(ValueError, match="independently raised boom"):
        second_app.update_blocking()

    assert len(reported) == 2
    assert "first_child" in reported[0]
    assert "second_child" in reported[1]


def test_raising_handler_does_not_replace_component_failure() -> None:
    """A reporting failure is secondary to the original component failure."""
    envmod.reset_default_env_for_tests()

    @syn.lifespan
    def _lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_raising"
        )

        def handler(exc: BaseException, ctx: syn.ExceptionContext) -> None:
            raise RuntimeError("reporting failed")

        builder.set_exception_handler(handler)
        yield

    @syn.task
    async def _child() -> None:
        raise ValueError("original component failure")

    @syn.task
    async def _root() -> None:
        await syn.spawn(syn.unit_path("child"), _child)

    app = syn.App("test_exception_handlers_raising", _root)
    with pytest.raises(ValueError, match="original component failure"):
        app.update_blocking()


def test_successful_ready_still_returns_none() -> None:
    envmod.reset_default_env_for_tests()
    ready_results: list[object] = []

    @syn.lifespan
    def _lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_exception_handlers_success_ready"
        )
        yield

    @syn.task
    async def _child() -> None:
        return None

    @syn.task
    async def _root() -> None:
        handle = await syn.spawn(syn.unit_path("child"), _child)
        # This deliberately exercises the runtime return value even though
        # the public annotation correctly declares ``None``.
        ready_results.append(await handle.ready())  # type: ignore[func-returns-value]

    app = syn.App("test_exception_handlers_success_ready", _root)
    app.update_blocking()

    assert ready_results == [None]


def test_caught_foreground_call_failure_does_not_fail_app_or_cache_parent() -> None:
    """A foreground call error belongs to its awaiting caller.

    Catching it must let the parent and app succeed, while the failed child and
    every memoized ancestor remain uncached so the failed path is retried.
    """
    envmod.reset_default_env_for_tests()
    calls = {"root": 0, "parent": 0, "child": 0}
    caught: list[str] = []

    @syn.lifespan
    def _lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
        builder.settings.db_path = common.get_env_db_path(
            "test_caught_foreground_call_failure"
        )
        yield

    @syn.task(cache=True)
    async def _child() -> None:
        calls["child"] += 1
        raise ValueError("expected foreground failure")

    @syn.task(cache=True)
    async def _parent() -> int:
        calls["parent"] += 1
        try:
            await syn.call(syn.unit_path("child"), _child)
        except ValueError as error:
            caught.append(str(error))
        return 7

    @syn.task(cache=True)
    async def _root() -> int:
        calls["root"] += 1
        return await syn.call(syn.unit_path("parent"), _parent)

    app = syn.App("test_caught_foreground_call_failure", _root)
    assert app.update_blocking() == 7
    assert app.update_blocking() == 7

    assert calls == {"root": 2, "parent": 2, "child": 2}
    assert caught == ["expected foreground failure", "expected foreground failure"]
