"""
Environment module.
"""

from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import threading
import warnings
import weakref
from contextlib import AsyncExitStack, contextmanager
from contextvars import ContextVar, Token
from inspect import isasyncgenfunction
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncGenerator,
    AsyncIterator,
    Callable,
    ContextManager,
    Iterator,
    TypeVar,
    overload,
)

from . import core, setting
from .context_keys import ContextKey, ContextProvider

if TYPE_CHECKING:
    from synor._internal.app import App

    from .component_ctx import ExceptionHandler

T = TypeVar("T")


class _LoopRunner:
    """
    Owns an event loop and optionally a daemon thread running it.

    This is used both for:
    - Per-Environment loops (when a non-running loop is provided or created)
    - The global background loop used for sync / cross-thread scheduling
    """

    _loop: asyncio.AbstractEventLoop
    _thread: threading.Thread | None

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        self._thread = None

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def thread(self) -> threading.Thread | None:
        return self._thread

    def ensure_running(self) -> None:
        if self._loop.is_running() or self._loop.is_closed():
            return

        def _runner(loop: asyncio.AbstractEventLoop) -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        self._thread = threading.Thread(target=_runner, args=(self._loop,), daemon=True)
        self._thread.start()

    @classmethod
    def from_running_loop(cls, loop: asyncio.AbstractEventLoop) -> "_LoopRunner":
        runner = cls(loop)
        # Already running; no thread needed.
        return runner

    @classmethod
    def create_new_running(cls) -> "_LoopRunner":
        runner = cls(asyncio.new_event_loop())
        runner.ensure_running()
        return runner


class EnvironmentBuilder:
    """Builder for the Environment."""

    _settings: setting.Settings
    _context_provider: ContextProvider
    _exception_handler: ExceptionHandler | None

    def __init__(self, settings: setting.Settings | None = None):
        self._settings = settings or setting.Settings.from_env()
        self._context_provider = ContextProvider()
        self._exception_handler = None

    @property
    def settings(self) -> setting.Settings:
        return self._settings

    def provide(self, key: ContextKey[Any], value: Any) -> Any:
        return self._context_provider.provide(key, value)

    def provide_with(self, key: ContextKey[Any], cm: ContextManager[Any]) -> Any:
        return self._context_provider.provide_with(key, cm)

    async def provide_async_with(
        self, key: ContextKey[Any], cm: AsyncContextManager[Any]
    ) -> Any:
        return await self._context_provider.provide_async_with(key, cm)

    def set_exception_handler(self, handler: ExceptionHandler) -> None:
        self._exception_handler = handler


LifespanFn = (
    Callable[[EnvironmentBuilder], Iterator[None]]
    | Callable[[EnvironmentBuilder], AsyncIterator[None]]
)


def _noop_lifespan_fn(_builder: EnvironmentBuilder) -> Iterator[None]:
    yield


_environment_info_lock = threading.Lock()
_environment_infos: list[EnvironmentInfo] = []


class EnvironmentInfo:
    """
    Per-environment information, for both already initialized and not-yet (lazily) initialized environments.
    """

    __slots__ = ("_env_ref", "_app_registry", "_app_registry_lock")

    _env_ref: weakref.ReferenceType[Environment | LazyEnvironment]
    _app_registry: weakref.WeakValueDictionary[str, App[Any, Any]]
    _app_registry_lock: threading.Lock

    def __init__(self, env: Environment | LazyEnvironment) -> None:
        self._env_ref = weakref.ref(env)
        self._app_registry = weakref.WeakValueDictionary()
        self._app_registry_lock = threading.Lock()
        with _environment_info_lock:
            _environment_infos.append(self)

    def register_app(self, name: str, app: App[Any, Any]) -> None:
        """Register an app with this environment."""
        with self._app_registry_lock:
            if name in self._app_registry:
                raise ValueError(
                    f"An app named '{name}' is already registered in this environment."
                )
            self._app_registry[name] = app

    def get_apps(self) -> list[App[Any, Any]]:
        """Get all registered apps for this environment."""
        with self._app_registry_lock:
            return list(self._app_registry.values())

    @property
    def env(self) -> Environment | LazyEnvironment | None:
        """The environment, or None if it has been garbage collected."""
        return self._env_ref()

    @property
    def env_name(self) -> str | None:
        """The environment name, or None if it has been garbage collected."""
        env = self.env
        return env.name if env else None


_environment_callback_owners: ContextVar[tuple[int, ...]] = ContextVar(
    "synor_environment_callback_owners", default=()
)


def _enter_environment_callback(owner: int) -> Token[tuple[int, ...]]:
    return _environment_callback_owners.set(
        (*_environment_callback_owners.get(), owner)
    )


def _exit_environment_callback(token: Token[tuple[int, ...]]) -> None:
    _environment_callback_owners.reset(token)


def get_registered_environment_infos() -> list[EnvironmentInfo]:
    """Get all registered environment infos (that haven't been garbage collected)."""
    with _environment_info_lock:
        # Filter out infos whose environments have been garbage collected
        return [info for info in _environment_infos if info.env is not None]


class Environment:
    """
    Synor runtime environment.

    Note: lifecycle is NOT driven by this class. Use `start()` / `stop()` (or the
    API `runtime()` context managers) to control the default environment lifespan.
    """

    __slots__ = (
        "_name",
        "_core_env",
        "_settings",
        "_context_provider",
        "_loop_runner",
        "_async_context",
        "_exception_handler",
        "_generation",
        "_info",
        "__weakref__",
    )

    _name: str
    _core_env: core.Environment
    _settings: setting.Settings
    _context_provider: ContextProvider
    _loop_runner: _LoopRunner
    _async_context: core.AsyncContext
    _exception_handler: ExceptionHandler | None
    _generation: int
    _info: EnvironmentInfo

    def __init__(
        self,
        settings: setting.Settings,
        *,
        name: str | None = None,
        context_provider: ContextProvider | None = None,
        event_loop: asyncio.AbstractEventLoop | None = None,
        exception_handler: ExceptionHandler | None = None,
        info: EnvironmentInfo | None = None,
        _generation: int = 0,
    ):
        if not settings.db_path:
            raise ValueError("Settings.db_path must be provided")
        self._name = name or ""
        self._settings = settings
        self._context_provider = context_provider or ContextProvider()

        if event_loop is None:
            event_loop = get_event_loop_or_default()

        if event_loop.is_running():
            self._loop_runner = _LoopRunner.from_running_loop(event_loop)
        else:
            # Keep a loop running for sync users (needed for async callbacks).
            runner = _LoopRunner(event_loop)
            runner.ensure_running()
            self._loop_runner = runner

        self._info = info or EnvironmentInfo(self)
        self._async_context = core.AsyncContext(self._loop_runner.loop, id(self._info))
        self._core_env = core.Environment(
            settings._to_engine_dict(), self._async_context
        )
        self._context_provider.set_core_env(self._core_env)
        self._exception_handler = exception_handler
        self._generation = _generation

    @property
    def name(self) -> str:
        return self._name

    @property
    def settings(self) -> setting.Settings:
        return self._settings

    @property
    def context_provider(self) -> ContextProvider:
        return self._context_provider

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        return self._loop_runner.loop

    @property
    def async_context(self) -> core.AsyncContext:
        """Get the AsyncContext for this environment's event loop."""
        return self._async_context

    @property
    def exception_handler(self) -> ExceptionHandler | None:
        return self._exception_handler

    def get_context(self, key: ContextKey[T]) -> T:
        """Get a context value provided during this environment's lifespan.

        Use this to access context values outside of Synor processing components,
        e.g., to share a database connection pool between indexing and query/serving logic.
        """
        return self._context_provider.get(key)

    async def _get_env(self) -> "Environment":
        return self

    def _get_env_sync(self) -> "Environment":
        return self


_lifecycle_callback_transitions: ContextVar[
    tuple[tuple[LazyEnvironment, concurrent.futures.Future[None]], ...]
] = ContextVar("synor_lifecycle_callback_transitions", default=())


class LazyEnvironment:
    """
    Lazy-initialized environment. To be initialized using lifespan function.

    Each successfully started ``Environment`` receives a monotonic generation
    number scoped to this lazy environment. Shutdown is a full barrier: the next
    generation is not published until the previous lifespan has finished cleanup.
    """

    __slots__ = (
        "_name",
        "_lifespan_fn_lock",
        "_lifespan_fn",
        "_lifecycle_lock",
        "_lifecycle_loop",
        "_transition",
        "_exit_stack",
        "_env",
        "_generation",
        "_info",
        "__weakref__",
    )

    _name: str
    _lifespan_fn_lock: threading.Lock
    _lifespan_fn: LifespanFn | None
    _lifecycle_lock: threading.Lock
    _lifecycle_loop: asyncio.AbstractEventLoop | None
    _transition: concurrent.futures.Future[None] | None
    _exit_stack: AsyncExitStack | None
    _env: Environment | None
    _generation: int
    _info: EnvironmentInfo

    def __init__(self, name: str = "default") -> None:
        self._name = name
        self._lifespan_fn_lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._lifecycle_loop = None
        self._transition = None
        self._lifespan_fn = None
        self._exit_stack = None
        self._env = None
        self._generation = 0
        self._info = EnvironmentInfo(self)

    async def _wait_for_transition(
        self,
        transition: concurrent.futures.Future[None],
        *,
        operation: str,
    ) -> None:
        self._check_transition_reentrancy(transition, operation=operation)
        # Shield the shared future: cancellation of one waiter must not cancel
        # the lifecycle transition for every other sync or async caller.
        await asyncio.shield(asyncio.wrap_future(transition))

    def _check_transition_reentrancy(
        self,
        transition: concurrent.futures.Future[None],
        *,
        operation: str,
    ) -> None:
        if any(
            owner is self and owned_transition is transition
            for owner, owned_transition in _lifecycle_callback_transitions.get()
        ):
            raise RuntimeError(
                f"LazyEnvironment.{operation}() cannot be called re-entrantly "
                "from the same environment's lifespan startup or cleanup callback"
            )

    def _check_current_transition_reentrancy(self, *, operation: str) -> None:
        with self._lifecycle_lock:
            transition = self._transition
        if transition is not None:
            self._check_transition_reentrancy(transition, operation=operation)

    @contextmanager
    def _lifecycle_callback_scope(
        self, transition: concurrent.futures.Future[None]
    ) -> Iterator[None]:
        previous = _lifecycle_callback_transitions.get()
        _lifecycle_callback_transitions.set((*previous, (self, transition)))
        try:
            yield
        finally:
            # Save/restore the value rather than a ContextVar token because
            # lifecycle cleanup may be delegated to another asyncio Task/loop.
            _lifecycle_callback_transitions.set(previous)

    def _complete_transition(
        self,
        transition: concurrent.futures.Future[None],
        *,
        error: BaseException | None = None,
        clear_lifecycle_loop: bool = False,
    ) -> None:
        with self._lifecycle_lock:
            if self._transition is not transition:
                raise RuntimeError("LazyEnvironment lifecycle transition was replaced")
            self._transition = None
            if clear_lifecycle_loop:
                self._lifecycle_loop = None
        if error is None:
            transition.set_result(None)
        else:
            transition.set_exception(error)

    async def _close_exit_stack(
        self,
        exit_stack: AsyncExitStack,
        owner_loop: asyncio.AbstractEventLoop,
        transition: concurrent.futures.Future[None],
    ) -> None:
        async def _close() -> None:
            with self._lifecycle_callback_scope(transition):
                await exit_stack.aclose()

        current_loop = asyncio.get_running_loop()
        if (
            owner_loop is current_loop
            or owner_loop.is_closed()
            or not owner_loop.is_running()
        ):
            # A closed owner loop cannot run cleanup.  The current loop is the
            # only possible best-effort fallback (and is sufficient for sync
            # generators and loop-agnostic async resources).
            cleanup_task = asyncio.create_task(_close())
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                # Shutdown is a barrier: honor caller cancellation only after
                # every registered cleanup callback has completed.
                await cleanup_task
                raise
            return

        cleanup = asyncio.run_coroutine_threadsafe(_close(), owner_loop)
        wrapped_cleanup = asyncio.wrap_future(cleanup)
        try:
            await asyncio.shield(wrapped_cleanup)
        except asyncio.CancelledError:
            await asyncio.shield(wrapped_cleanup)
            raise

    def _lifecycle_loop_for_sync(self) -> asyncio.AbstractEventLoop:
        with self._lifecycle_lock:
            loop = self._lifecycle_loop
        if loop is not None and loop.is_running() and not loop.is_closed():
            return loop
        return default_env_loop()

    @property
    def name(self) -> str:
        return self._name

    def lifespan(self, fn: LifespanFn) -> None:
        with self._lifespan_fn_lock:
            if self._lifespan_fn is not None:
                warnings.warn(
                    f"Overriding the default lifespan function {self._lifespan_fn} with {fn}."
                )
            self._lifespan_fn = fn

    async def _reset(self) -> None:
        await self.stop()
        with self._lifespan_fn_lock:
            self._lifespan_fn = None

    async def _get_env(self) -> Environment:
        """
        Start the default environment (executes on the default environment's event loop).
        """
        loop = asyncio.get_running_loop()
        while True:
            starter = False
            with self._lifecycle_lock:
                if self._env is not None:
                    return self._env
                transition = self._transition
                if transition is None:
                    transition = concurrent.futures.Future()
                    self._transition = transition
                    self._lifecycle_loop = loop
                    next_generation = self._generation + 1
                    starter = True
            if starter:
                break
            await self._wait_for_transition(transition, operation="start")

        with self._lifespan_fn_lock:
            fn = self._lifespan_fn or _noop_lifespan_fn

        env_builder = EnvironmentBuilder()
        exit_stack = AsyncExitStack()

        try:
            if isasyncgenfunction(fn):
                # Start async generator and register cleanup
                async_gen: AsyncGenerator[None, None] = fn(env_builder)  # type: ignore[assignment]
                with self._lifecycle_callback_scope(transition):
                    await anext(async_gen)

                async def _aclose() -> None:
                    try:
                        await anext(async_gen)
                    except StopAsyncIteration:
                        pass
                    finally:
                        await async_gen.aclose()

                exit_stack.push_async_callback(_aclose)
            else:
                # Start sync generator and register cleanup
                sync_gen: Iterator[None] = fn(env_builder)  # type: ignore[assignment]
                with self._lifecycle_callback_scope(transition):
                    next(sync_gen)

                def _close() -> None:
                    try:
                        next(sync_gen)
                    except StopIteration:
                        pass
                    finally:
                        close_fn = getattr(sync_gen, "close", None)
                        if callable(close_fn):
                            close_fn()

                exit_stack.callback(_close)

            built_settings = env_builder.settings
            if not built_settings.db_path:
                default_db_path = setting.get_default_db_path()
                if default_db_path:
                    built_settings.db_path = default_db_path
                else:
                    raise ValueError(
                        "Environment settings must provide Settings.db_path "
                        "(or set SYNOR_DB environment variable)"
                    )

            context_provider = env_builder._context_provider
            exit_stack.push_async_callback(context_provider.aclose)

            env = Environment(
                built_settings,
                name=self._name,
                context_provider=context_provider,
                event_loop=loop,
                exception_handler=env_builder._exception_handler,
                info=self._info,
                _generation=next_generation,
            )
        except BaseException as exc:
            completion_error: BaseException | None = (
                None if isinstance(exc, asyncio.CancelledError) else exc
            )
            try:
                await self._close_exit_stack(exit_stack, loop, transition)
            except asyncio.CancelledError:
                # This is the same cancellation that interrupted startup.  The
                # transition itself is complete after cleanup, so unrelated
                # waiters should retry rather than inherit that cancellation.
                completion_error = None
            except BaseException as cleanup_exc:
                completion_error = cleanup_exc
            finally:
                self._complete_transition(
                    transition,
                    error=completion_error,
                    clear_lifecycle_loop=True,
                )
            if completion_error is not None and completion_error is not exc:
                raise completion_error from exc
            raise

        with self._lifecycle_lock:
            # Publish the monotonic generation only after both the lifespan and
            # Environment have started successfully. Successful generations are
            # never reused by this LazyEnvironment.
            self._generation = next_generation
            self._env = env
            self._exit_stack = exit_stack
        self._complete_transition(transition)
        return env

    def _get_env_sync(self) -> Environment:
        with self._lifecycle_lock:
            if self._env is not None and self._transition is None:
                return self._env
            transition = self._transition
        if transition is not None:
            self._check_transition_reentrancy(transition, operation="start")
        env_loop = self._lifecycle_loop_for_sync()
        fut = asyncio.run_coroutine_threadsafe(self._get_env(), env_loop)
        return fut.result()

    async def start(self) -> Environment:
        """
        Start the default environment (executes on the default environment's event loop).
        """
        return await self._get_env()

    async def stop(self) -> None:
        """
        Stop the default environment (executes on the default environment's event loop).
        """
        # Waiting for this environment's own operation lease can never make
        # progress when stop is invoked from one of its Python callbacks.
        # Reject before claiming or mutating the lifecycle transition so an
        # external owner can still stop the generation normally.
        if id(self._info) in _environment_callback_owners.get():
            raise RuntimeError(
                "LazyEnvironment.stop() cannot be called from an operation callback "
                "using the same environment"
            )

        # Preserve the component-context guard for manually attached contexts
        # and other host-side paths that do not cross the Rust callback bridge.
        from .component_ctx import get_context_from_ctx

        try:
            current_component = get_context_from_ctx()
        except RuntimeError:
            current_component = None
        if current_component is not None and current_component._env._info is self._info:
            raise RuntimeError(
                "LazyEnvironment.stop() cannot be called from an operation callback "
                "using the same environment"
            )

        env: Environment | None = None
        exit_stack: AsyncExitStack | None = None
        owner_loop: asyncio.AbstractEventLoop | None = None
        while True:
            stopper = False
            with self._lifecycle_lock:
                transition = self._transition
                if transition is None:
                    env = self._env
                    exit_stack = self._exit_stack
                    owner_loop = self._lifecycle_loop
                    if env is None:
                        return
                    if exit_stack is None or owner_loop is None:
                        raise RuntimeError(
                            "LazyEnvironment has inconsistent lifecycle state"
                        )
                    transition = concurrent.futures.Future()
                    self._transition = transition
                    self._env = None
                    self._exit_stack = None
                    # This synchronous gate close linearizes with Rust-side
                    # operation acquisition. Operations admitted first remain
                    # tracked and may still schedule callbacks while draining;
                    # operations racing after this point are rejected.
                    env._async_context.close_operation_admission()
                    # Live operations intentionally remain active after their
                    # initial result is ready, so they cannot be passively
                    # drained. Cancel only this generation's live operations;
                    # ordinary admitted operations retain finish-before-stop
                    # semantics and are drained below unchanged.
                    env._async_context.cancel_live_operations()
                    stopper = True
            if stopper:
                break
            await self._wait_for_transition(transition, operation="stop")

        assert env is not None
        assert exit_stack is not None
        assert owner_loop is not None
        error: BaseException | None = None
        cancelled: asyncio.CancelledError | None = None

        async def _shutdown_generation(
            closing_env: Environment,
            closing_exit_stack: AsyncExitStack,
            closing_owner_loop: asyncio.AbstractEventLoop,
        ) -> None:
            # Core app construction/update/drop leases cover the full operation,
            # including eager LMDB initialization and terminal teardown. Keep
            # callback admission open until every admitted operation has
            # finished, because a pre-stop operation may not have scheduled its
            # first Python callback yet.
            await closing_env._async_context.drain_operations_async()

            # An admitted operation can populate an App's cached Rust binding
            # while the operation drain is in progress. Invalidate only after
            # that drain so no late completion can recache this generation.
            for app in self._info.get_apps():
                app._invalidate_environment(closing_env)

            # No admitted operation remains able to create callback work. Close
            # the second gate, then drain every callback admitted before it.
            closing_env._async_context.close_callback_admission()
            await closing_env._async_context.drain_callbacks_async()
            await self._close_exit_stack(
                closing_exit_stack, closing_owner_loop, transition
            )

        shutdown_task = asyncio.create_task(
            _shutdown_generation(env, exit_stack, owner_loop)
        )
        try:
            while True:
                try:
                    await asyncio.shield(shutdown_task)
                    break
                except asyncio.CancelledError as exc:
                    # Caller cancellation is deferred until callback drain and
                    # lifespan cleanup are both complete. Repeated cancellation
                    # requests likewise cannot cancel the shared shutdown task.
                    if cancelled is None:
                        cancelled = exc
                    if shutdown_task.done():
                        try:
                            shutdown_task.result()
                        except asyncio.CancelledError as shutdown_exc:
                            cancelled = shutdown_exc
                        except BaseException as shutdown_exc:
                            error = shutdown_exc
                        break
                except BaseException as exc:
                    error = exc
                    break
        finally:
            # Drop the final local references before waking a concurrent starter.
            del shutdown_task
            del env
            del exit_stack
            self._complete_transition(
                transition,
                error=error,
                clear_lifecycle_loop=True,
            )
        if error is not None:
            raise error
        if cancelled is not None:
            raise cancelled


_default_env = LazyEnvironment()


@overload
def lifespan(fn: LifespanFn) -> LifespanFn: ...
@overload
def lifespan(fn: None) -> Callable[[LifespanFn], LifespanFn]: ...
def lifespan(fn: LifespanFn | None = None) -> Any:
    """
    Decorate a function that returns a lifespan.
    It registers the function as a lifespan provider.
    """

    def _inner(fn: LifespanFn) -> LifespanFn:
        _default_env.lifespan(fn)
        return fn

    if fn is not None:
        return _inner(fn)
    else:
        return _inner


async def start() -> Environment:
    """
    Start the default environment (executes on the default environment's event loop).
    """
    return await _default_env._get_env()


async def stop() -> None:
    """
    Stop the default environment (executes on the default environment's event loop).
    """
    await _default_env.stop()


_bg_loop_lock: threading.Lock = threading.Lock()
_bg_loop_runner: _LoopRunner | None = None


def default_env_loop() -> asyncio.AbstractEventLoop:
    """
    Ensure we have a long-lived background event loop for sync / cross-loop callers.

    Important: we do NOT reuse a "currently running" loop here, because callers
    (e.g. pytest-asyncio) may create short-lived loops that get closed.
    """
    global _bg_loop_runner  # pylint: disable=global-statement

    with _bg_loop_lock:
        if _bg_loop_runner is not None and not _bg_loop_runner.loop.is_closed():
            return _bg_loop_runner.loop

        _bg_loop_runner = _LoopRunner.create_new_running()
        return _bg_loop_runner.loop


def _shutdown_background_loop_at_exit() -> None:
    """
    Atexit hook: stop and close the default env background loop if it was started.

    Prevents the background loop thread from outliving the interpreter, which can
    cause SIGSEGV or GIL errors during finalization (e.g. on Ubuntu CI). Safe to
    call even if the loop was never started (no-op).
    """
    global _bg_loop_runner  # pylint: disable=global-statement

    with _bg_loop_lock:
        runner = _bg_loop_runner
        _bg_loop_runner = None

    if runner is None:
        return

    loop = runner.loop
    if loop.is_closed():
        return

    try:
        thread = runner.thread
        if thread is not None and thread.is_alive():
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=1.0)
        if not loop.is_closed():
            loop.close()
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)
    except Exception:
        pass


def _shutdown_at_exit() -> None:
    """
    Atexit hook: shut down the Tokio runtime first, then stop the background
    asyncio loop.

    Order matters: the Tokio runtime is shut down before the asyncio loop so
    that no asyncio callbacks can race with runtime shutdown. With
    ManuallyDrop<Runtime>, Tokio threads stay alive indefinitely after the main
    work completes. On Python < 3.13, `_PyGILState_Fini` (called during
    `Py_Finalize()`) deletes `autoTSSkey`; if any idle Tokio thread then calls
    `PyGILState_Release`, it reads a deleted TLS key and triggers the fatal
    error: "PyGILState_Release: thread state X must be current when releasing".
    """
    try:
        core.shutdown_tokio_runtime()
    except Exception:
        pass
    _shutdown_background_loop_at_exit()


atexit.register(_shutdown_at_exit)


def start_sync() -> Environment:
    return _default_env._get_env_sync()


def stop_sync() -> None:
    _default_env._check_current_transition_reentrancy(operation="stop")
    loop = _default_env._lifecycle_loop_for_sync()
    fut = asyncio.run_coroutine_threadsafe(_default_env.stop(), loop)
    fut.result()


def default_env_lazy() -> LazyEnvironment:
    """Get the default lazy environment."""
    return _default_env


def default_env_sync() -> Environment:
    return start_sync()


def reset_default_env_for_tests() -> None:
    """
    Reset the registered default lifespan function.

    This is intended for tests so lifespan registration does not leak across test modules.
    """
    asyncio.run(_default_env._reset())


def get_event_loop_or_default() -> asyncio.AbstractEventLoop:
    """Get the running event loop, or the default background loop if none.

    Returns the currently running event loop if called from an async context.
    Otherwise, returns the shared background loop (from default_env_loop()).
    """
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return default_env_loop()
