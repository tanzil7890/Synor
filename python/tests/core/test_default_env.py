from contextlib import contextmanager
import os
from typing import Iterator
import pytest

import synor as syn
from synor._internal.environment import reset_default_env_for_tests
from tests.common import get_env_db_path

_env_db_path = get_env_db_path("_default")
_env_db_path_from_env_var = get_env_db_path("_default_from_env_var")


class _Resource:
    pass


_RESOURCE_KEY = syn.ContextKey[_Resource]("test_default_env/resource")

_num_active_resources = 0


@contextmanager
def _acquire_resource() -> Iterator[_Resource]:
    global _num_active_resources
    _num_active_resources += 1
    yield _Resource()
    _num_active_resources -= 1


@pytest.fixture(scope="module")
def _default_env() -> Iterator[None]:
    try:

        @syn.lifespan
        def default_lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
            builder.settings.db_path = _env_db_path
            builder.provide_with(_RESOURCE_KEY, _acquire_resource())
            yield

        yield
    finally:
        reset_default_env_for_tests()


def test_default_env(_default_env: None) -> None:
    assert not _env_db_path.exists()
    with syn.runtime():
        pass
    assert _env_db_path.exists()


def _trivial_fn(s: str, i: int) -> str:
    assert isinstance(syn.use_context(_RESOURCE_KEY), _Resource)
    return f"{s} {i}"


def test_app(_default_env: None) -> None:
    app = syn.App(
        syn.AppConfig(name="trivial_app"),
        _trivial_fn,
        "Hello",
        1,
    )

    assert _num_active_resources == 0
    with syn.runtime():
        assert app.update_blocking() == "Hello 1"
        assert _num_active_resources == 1
    assert _num_active_resources == 0


def test_app_implicit_startup(_default_env: None) -> None:
    app = syn.App(
        syn.AppConfig(name="trivial_app_implicit_startup"),
        _trivial_fn,
        "Hello",
        1,
    )

    assert _num_active_resources == 0
    assert app.update_blocking() == "Hello 1"
    assert _num_active_resources == 1


# =============================================================================
# Test: Default DB path from SYNOR_DB environment variable
# =============================================================================


@pytest.fixture(scope="function")
def _default_env_from_env_var() -> Iterator[None]:
    """
    Fixture that sets SYNOR_DB env var and uses a lifespan that does NOT
    set db_path explicitly.
    """
    # Reset any previously initialized default environment
    reset_default_env_for_tests()

    old_env = os.environ.get("SYNOR_DB")
    os.environ["SYNOR_DB"] = str(_env_db_path_from_env_var)

    try:
        # Lifespan that does NOT set db_path - relies on SYNOR_DB env variable
        @syn.lifespan
        def lifespan_without_db_path(
            _builder: syn.EnvironmentBuilder,
        ) -> Iterator[None]:
            yield

        yield
    finally:
        reset_default_env_for_tests()
        if old_env is not None:
            os.environ["SYNOR_DB"] = old_env
        else:
            os.environ.pop("SYNOR_DB", None)


def _simple_fn(s: str) -> str:
    return f"result: {s}"


@pytest.mark.asyncio
async def test_default_env_uses_synor_db_env_var(
    _default_env_from_env_var: None,
) -> None:
    """Test that default env uses SYNOR_DB when lifespan doesn't set db_path."""
    assert not _env_db_path_from_env_var.exists()
    async with syn.runtime():
        env = await syn.default_env()
        assert env.settings.db_path == _env_db_path_from_env_var
    assert _env_db_path_from_env_var.exists()


def test_app_uses_synor_db_env_var(_default_env_from_env_var: None) -> None:
    """Test that app works when using SYNOR_DB env var for db_path."""
    app = syn.App(
        syn.AppConfig(name="app_with_env_var_db"),
        _simple_fn,
        "test",
    )

    with syn.runtime():
        result = app.update_blocking()
        assert result == "result: test"
