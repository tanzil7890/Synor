import asyncio
import datetime
import hashlib
import json
import logging
import os
import pathlib
import shutil
import signal
import sys
import tempfile
import uuid
from collections.abc import AsyncIterator, Mapping
from typing import Any, NamedTuple, NoReturn

import click
from dotenv import find_dotenv, load_dotenv

import synor as syn
from synor._internal import core as _core
from synor._internal.app import App
from synor._internal.app_target import split_app_target
from synor._internal.environment import (
    Environment,
    EnvironmentInfo,
    LazyEnvironment,
    default_env_lazy,
    get_registered_environment_infos,
)
from synor._internal.setting import get_default_db_path
from synor._internal.stable_path import StablePath
from synor.inspect import (
    iter_stable_path_details,
    iter_stable_path_details_by_name,
    iter_stable_paths,
    iter_stable_paths_by_name,
    iter_target_states,
    iter_target_states_by_name,
    query_stable_path_details,
    query_stable_path_details_by_name,
)

from .user_app_loader import Error as UserAppLoaderError
from .user_app_loader import load_user_app

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


def _setup_logging() -> None:
    """Configure Python's root logger for CLI use.

    Level is taken from the ``SYNOR_LOG_LEVEL`` env var (default ``WARNING``).
    Uses ``force=True`` so re-invocation (e.g. tests) replaces any prior config.
    """
    level = os.environ.get("SYNOR_LOG_LEVEL", "WARNING").upper()
    logging.basicConfig(
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        level=level,
        force=True,
    )


# ---------------------------------------------------------------------------
# Graceful cancellation helpers
# ---------------------------------------------------------------------------


def _run_async_cmd(coro_fn: Any, *, quiet: bool = False) -> None:
    """Run an async CLI command with graceful Ctrl+C cancellation.

    On first Ctrl+C: fires the global Rust cancellation token so the engine
    exits promptly, then lets ``asyncio.run()`` shut down normally.
    On second Ctrl+C: kills the process immediately (default SIGINT).
    """
    cancelled = False

    def _on_sigint(signum: int, frame: Any) -> None:
        nonlocal cancelled
        cancelled = True
        _core.cancel_all()
        if not quiet:
            print("\nStopping...")
        # Restore default handler so a second Ctrl+C kills immediately.
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    async def _wrapper() -> None:
        _core.reset_global_cancellation()
        try:
            await coro_fn(cancelled=lambda: cancelled)
        except Exception:
            if not cancelled:
                raise

    prev_handler = signal.signal(signal.SIGINT, _on_sigint)
    try:
        asyncio.run(_wrapper())
    except KeyboardInterrupt:
        if not quiet:
            print("\nStopping...")
    finally:
        signal.signal(signal.SIGINT, prev_handler)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


class AppSpecifier(NamedTuple):
    """Parsed app specifier."""

    module_ref: str
    app_name: str | None = None
    env_name: str | None = None


def _parse_app_target(specifier: str) -> AppSpecifier:
    """
    Parse 'module_or_path[:app_name[@env_name]]' into AppSpecifier.

    Examples:
        './main.py' -> AppSpecifier('./main.py', None, None)
        './main.py:app2' -> AppSpecifier('./main.py', 'app2', None)
        './main.py:app2@alpha' -> AppSpecifier('./main.py', 'app2', 'alpha')
        'mymodule:my_app@default' -> AppSpecifier('mymodule', 'my_app', 'default')
    """
    module_ref, app_part = split_app_target(specifier)

    if not module_ref:
        raise click.BadParameter(
            f"Module/path part is missing in specifier: '{specifier}'. "
            "Expected format like 'myapp.py' or 'myapp.py:app_name'.",
            param_hint="APP_TARGET",
        )

    if app_part is None:
        return AppSpecifier(module_ref=module_ref)

    if not app_part:
        return AppSpecifier(module_ref=module_ref)

    # Parse app_name[@env_name]
    if "@" in app_part:
        app_name, env_name = app_part.split("@", 1)
        if not env_name:
            raise click.BadParameter(
                f"Environment name is missing after '@' in specifier '{specifier}'.",
                param_hint="APP_TARGET",
            )
    else:
        app_name = app_part
        env_name = None

    if app_name and not app_name.isidentifier():
        raise click.BadParameter(
            f"Invalid app name '{app_name}' in specifier '{specifier}'. "
            "App name must be a valid Python identifier.",
            param_hint="APP_TARGET",
        )

    return AppSpecifier(module_ref=module_ref, app_name=app_name, env_name=env_name)


def _get_persisted_app_names(env: Environment) -> set[str]:
    """Get the set of app names persisted in the given environment's database."""
    try:
        names = _core.list_app_names(env._core_env)
        return set(names) if names else set()
    except Exception:
        return set()


def _format_db_path(env: Environment) -> str:
    """Format the database path for display."""
    if not env.settings.db_path:
        return "(unknown)"
    path = env.settings.db_path
    try:
        cwd = os.getcwd()
        abs_path = os.path.abspath(str(path))
        if abs_path.startswith(cwd + os.sep):
            return "./" + os.path.relpath(abs_path, cwd)
        return str(path)
    except Exception:
        return str(path)


def _confirm_yes(prompt: str) -> bool:
    """Prompt user to type 'yes' explicitly. Returns True only if user types 'yes'."""
    response: str = click.prompt(prompt, default="", show_default=False)
    return response.lower() == "yes"


def _format_env_header(env_name: str, db_path: str) -> str:
    """Format the environment header for display."""
    if env_name:
        return f"{env_name} ({db_path}):"
    return f"{db_path}:"


def _print_app_group(
    env_name: str,
    db_path: str,
    apps: list[App[Any, Any]],
    persisted_names: set[str],
) -> bool:
    """Print a group of apps under an environment. Returns True if any app is not persisted."""
    has_missing = False
    click.echo(_format_env_header(env_name, db_path))
    for app in sorted(apps, key=lambda a: a._name):
        if app._name in persisted_names:
            click.echo(f"  {app._name}")
        else:
            click.echo(f"  {app._name} [+]")
            has_missing = True
    return has_missing


async def _ls_from_module_async(module_ref: str) -> None:
    """List apps from a loaded module, grouped by environment. Uses async env access so CLI never starts the background loop."""
    try:
        load_user_app(module_ref)
    except UserAppLoaderError as e:
        raise RuntimeError(f"Failed to load module '{module_ref}'") from e

    try:
        env_infos = get_registered_environment_infos()
        if not env_infos:
            click.echo(f"No apps are defined in '{module_ref}'.")
            return

        # Sort: explicit environments first (by name), default environment last
        def sort_key(info: EnvironmentInfo) -> tuple[int, str]:
            env = info.env
            if env is default_env_lazy():
                return (1, "")
            return (0, info.env_name or "")

        sorted_infos = sorted(env_infos, key=sort_key)

        has_missing = False
        first_group = True

        for info in sorted_infos:
            apps = info.get_apps()
            if not apps:
                continue

            env = info.env
            if env is None:
                continue

            if not first_group:
                click.echo("")
            first_group = False

            env_name = info.env_name or ""
            if isinstance(env, LazyEnvironment):
                actual_env = await env._get_env()
            else:
                actual_env = env
            db_path = _format_db_path(actual_env)
            persisted_names = _get_persisted_app_names(actual_env)
            has_missing |= _print_app_group(env_name, db_path, apps, persisted_names)

        if first_group:
            click.echo(f"No apps are defined in '{module_ref}'.")
            return

        if has_missing:
            click.echo("")
            click.echo("Notes:")
            click.echo(
                "  [+]: Apps present in module, but not yet run (no persisted state)."
            )
    finally:
        await _stop_all_environments()


async def _ls_from_database_async(db_path: str) -> None:
    """List all persisted apps from a specific database. Passes the running loop explicitly so the CLI never starts the background loop."""
    db_path_obj = pathlib.Path(db_path)
    if not db_path_obj.exists():
        raise click.ClickException(f"Database path does not exist: {db_path}")

    try:
        from synor._internal.setting import Settings

        env = Environment(
            Settings(db_path=db_path_obj),
            event_loop=asyncio.get_running_loop(),
        )
        persisted_names = _get_persisted_app_names(env)
    except Exception as e:
        raise click.ClickException(f"Failed to open database: {e}") from e

    if not persisted_names:
        click.echo("No persisted apps found in the database.")
        return

    formatted_path = _format_db_path(env)
    click.echo(f"{formatted_path}:")
    for name in sorted(persisted_names):
        click.echo(f"  {name}")


def _load_app(app_target: str) -> App[Any, Any]:
    """
    Load an app from a specifier.

    Supports formats:
        - 'path/to/app.py' - loads the only registered app
        - 'path/to/app.py:app_name' - loads the app with 'app_name'
        - 'path/to/app.py:app_name@env_name' - loads the app with 'app_name' in environment 'env_name'
    """
    spec = _parse_app_target(app_target)

    try:
        load_user_app(spec.module_ref)
    except UserAppLoaderError as e:
        raise RuntimeError(f"Failed to load module '{spec.module_ref}'") from e

    # Get target environments (filter by env_name if specified)
    env_infos = get_registered_environment_infos()
    if spec.env_name:
        env_infos = [info for info in env_infos if info.env_name == spec.env_name]
        if not env_infos:
            raise click.ClickException(
                f"No environment named '{spec.env_name}' found after loading '{spec.module_ref}'."
            )

    # Get all apps from target environments
    apps: list[App[Any, Any]] = []
    for info in env_infos:
        apps.extend(info.get_apps())

    # Filter by app name if specified
    if spec.app_name:
        matching = [a for a in apps if a._name == spec.app_name]
        if not matching:
            available = ", ".join(sorted(set(a._name for a in apps))) or "none"
            raise click.ClickException(
                f"No app named '{spec.app_name}' found after loading '{spec.module_ref}'. "
                f"Available apps: {available}"
            )

        if len(matching) > 1:
            # Multiple apps with the same name in different environments
            available_envs = ", ".join(
                a._environment.name or "(unnamed)" for a in matching
            )
            raise click.ClickException(
                f"Multiple apps named '{spec.app_name}' found in different environments: {available_envs}. "
                f"Please specify environment with ':app_name@env_name' syntax."
            )
        app = matching[0]
    else:
        # No app name specified
        if len(apps) == 1:
            app = apps[0]
        elif len(apps) > 1:
            available = ", ".join(sorted(set(a._name for a in apps)))
            raise click.ClickException(
                f"Multiple apps found in '{spec.module_ref}': {available}. "
                "Please specify which app to use with ':app_name' syntax."
            )
        else:
            raise click.ClickException(
                f"No apps found after loading '{spec.module_ref}'. "
                "Make sure the module creates a syn.App(...) instance."
            )

    return app


def _create_project_files(project_name: str, project_dir: str) -> None:
    """Create project files for a new Synor project."""

    project_path = pathlib.Path(project_dir)
    project_path.mkdir(parents=True, exist_ok=True)

    # Create main.py
    main_py_content = f'''"""Synor app template."""
import pathlib
from typing import Iterator

import synor as syn


@syn.lifespan
def synor_lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
    """Configure the Synor environment."""
    builder.settings.db_path = pathlib.Path("./synor.db")
    yield


@syn.task
async def app_main() -> None:
    """Define your main pipeline here.

    Common pattern:
      1) Declare targets/target states under stable 'setup/...' paths.
      2) Enumerate inputs (files, DB rows, etc.).
      3) Spawn per input processing unit using a stable path.

    Note: app_main can accept parameters (e.g., sourcedir/outdir) passed via syn.App(...)
    """

    # 1) Declare targets/target states
    # Example (local filesystem):
    #   target = await syn.call(
    #       syn.unit_path("setup"),
    #       localfs.ensure_dir_target,
    #       outdir,
    #   )

    # 2) Enumerate inputs
    # Example (walk a directory):
    #   files = localfs.walk_dir(
    #       sourcedir,
    #       path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
    #   )

    # 3) Spawn a processing unit for each input under a stable path
    # Example:
    #   for f in files:
    #       await syn.spawn(
    #           syn.unit_path("process", str(f.relative_path)),
    #           process_file_function,
    #           f,
    #           target,
    #       )

    pass


app = syn.App(
    syn.AppConfig(name="{project_name}"),
    app_main,
)
'''
    (project_path / "main.py").write_text(main_py_content)

    # Create pyproject.toml
    pyproject_toml_content = f"""[project]
name = "{project_name}"
version = "0.1.0"
description = "A Synor application"
requires-python = ">=3.11"
dependencies = [
    "synor>={syn.__version__}",
]
"""
    (project_path / "pyproject.toml").write_text(pyproject_toml_content)

    # Create README.md
    readme_content = f"""# {project_name}

A Synor application.

## Getting Started

Run the app:
```bash
uv run synor update main.py
```

## Project Structure

- `main.py` - Main application file with your Synor app definition
- `pyproject.toml` - Project metadata and dependencies
"""
    (project_path / "README.md").write_text(readme_content)


async def _print_tree_streaming(
    items: AsyncIterator[Any],
    component_node_type: Any,
) -> None:
    """
    Print stable paths as a simple indented bullet list. No lookahead or
    "last sibling" logic; each line is "  " * (depth - 1) + "- " + label.
    """
    click.echo("Stable paths:")
    count = 0
    async for item in items:
        path = StablePath(item.path)
        parts = path.parts()
        is_component = item.node_type == component_node_type
        if not parts:
            line = "- /"
        else:
            indent = "  " * (len(parts) - 1)
            label = str(parts[-1])
            line = f"{indent}- {label}"
        if is_component:
            line += " [component]"
        click.echo(line)
        count += 1
    if count == 0:
        click.echo("(none)")


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group()
@click.pass_context
@click.version_option(
    None,
    "-V",
    "--version",
    package_name="synor",
    message="%(prog)s version %(version)s",
)
@click.option(
    "-e",
    "--env-file",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True
    ),
    help="Path to a .env file to load environment variables from. "
    "If not provided, attempts to load '.env' from the current directory.",
    default=None,
    show_default=False,
)
@click.option(
    "-d",
    "--app-dir",
    help="Load apps from the specified directory. Default to the current directory.",
    default="",
    show_default=True,
)
@click.option(
    "--offline",
    is_flag=True,
    default=False,
    help="Deny guarded Python networking and policy-aware egress.",
)
def cli(
    ctx: click.Context,
    env_file: str | None = None,
    app_dir: str | None = "",
    offline: bool = False,
) -> None:
    """CLI for Synor."""
    _setup_logging()

    dotenv_path = env_file or find_dotenv(usecwd=True)

    if load_dotenv(dotenv_path=dotenv_path):
        loaded_env_path = os.path.abspath(dotenv_path)
        click.echo(f"Loaded environment variables from: {loaded_env_path}\n", err=True)

    if app_dir is not None:
        sys.path.insert(0, app_dir)

    if offline:
        os.environ["SYNOR_OFFLINE"] = "1"
    try:
        active_policy = syn.policy_from_env(offline=offline)
    except ValueError as error:
        raise click.ClickException(str(error)) from error
    scope = syn.policy_scope(active_policy)
    scope.__enter__()
    ctx.call_on_close(lambda: scope.__exit__(None, None, None))
    ctx.ensure_object(dict)
    ctx.obj["policy"] = active_policy


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("app_target", type=str, required=False)
@click.option(
    "--db",
    type=str,
    default=None,
    help="Path to database to list apps from (only used when APP_TARGET is not specified).",
)
def ls(app_target: str | None, db: str | None) -> None:
    """
    List all apps.

    If `APP_TARGET` (`path/to/app.py` or `module`) is provided, lists apps defined in that module and their persisted status, grouped by environment.

    If `APP_TARGET` is omitted and `--db` is provided, lists all apps from the specified database.
    """
    if app_target:
        if db:
            click.echo(
                "Warning: --db is ignored when APP_TARGET is specified.", err=True
            )
        spec = _parse_app_target(app_target)
        asyncio.run(_ls_from_module_async(spec.module_ref))
    elif db:
        asyncio.run(_ls_from_database_async(db))
    else:
        # Try to use default db path from environment variable
        default_db = get_default_db_path()
        if default_db:
            asyncio.run(_ls_from_database_async(str(default_db)))
        else:
            raise click.ClickException(
                "Please specify either APP_TARGET or --db option "
                "(or set SYNOR_DB environment variable).\n"
                "  synor ls ./app.py        # List apps from module\n"
                "  synor ls --db ./my.db    # List apps from database"
            )


@cli.command()
@click.argument("app_target", type=str, required=False)
@click.option(
    "--db",
    type=str,
    default=None,
    help="Path to database (used with --app-name when APP_TARGET is not specified).",
)
@click.option(
    "--app-name",
    type=str,
    default=None,
    help="App name to inspect (used with --db when APP_TARGET is not specified).",
)
@click.option(
    "--tree",
    is_flag=True,
    default=False,
    help="Display stable paths (or --target-states entries) as a tree.",
)
@click.option(
    "-l",
    "--long",
    "long_format",
    is_flag=True,
    default=False,
    help="Display detailed information in multi-line format.",
)
@click.argument("stable_path", type=str, required=False)
@click.option(
    "-r",
    "--recursive",
    is_flag=True,
    default=False,
    help="Show all children recursively (requires stable_path).",
)
@click.option(
    "-p",
    "--parents",
    is_flag=True,
    default=False,
    help="Show all parent paths (requires stable_path).",
)
@click.option(
    "--fingerprints",
    "fingerprints",
    is_flag=True,
    default=False,
    help="Show target-state paths as raw fingerprints (as stored) instead of readable keys.",
)
@click.option(
    "--target-states",
    "target_states",
    is_flag=True,
    default=False,
    help="List all tracked target states with their owner components.",
)
def show(
    app_target: str | None,
    db: str | None,
    app_name: str | None,
    tree: bool,
    long_format: bool,
    stable_path: str | None,
    recursive: bool,
    parents: bool,
    fingerprints: bool,
    target_states: bool,
) -> None:
    """
    Show the app's stable paths.

    \b
    If `APP_TARGET` is provided, loads the app from the module.
    Otherwise, `--db` and `--app-name` can be used to inspect an app
    directly from its database without loading the module.

    """
    if (recursive or parents) and not stable_path:
        raise click.ClickException(
            "-r/--recursive and -p/--parents require a stable_path argument."
        )
    if target_states and (stable_path or long_format or recursive or parents):
        raise click.ClickException(
            "--target-states cannot be combined with stable_path, -l, -r or -p."
        )

    if app_target:
        if db or app_name:
            click.echo(
                "Warning: --db/--app-name are ignored when APP_TARGET is specified.",
                err=True,
            )
        if target_states:
            asyncio.run(
                _show_target_states_from_app(
                    _load_app(app_target), tree=tree, fingerprints=fingerprints
                )
            )
            return
        asyncio.run(
            _show_from_app(
                _load_app(app_target),
                tree=tree,
                long_format=long_format,
                stable_path=stable_path,
                recursive=recursive,
                parents=parents,
                fingerprints=fingerprints,
            )
        )
    elif db and app_name:
        if target_states:
            asyncio.run(
                _show_target_states_from_database(
                    db, app_name, tree=tree, fingerprints=fingerprints
                )
            )
            return
        asyncio.run(
            _show_from_database(
                db,
                app_name,
                tree=tree,
                long_format=long_format,
                stable_path=stable_path,
                recursive=recursive,
                parents=parents,
                fingerprints=fingerprints,
            )
        )
    elif db or app_name:
        raise click.ClickException(
            "Both --db and --app-name are required when APP_TARGET is not specified."
        )
    else:
        raise click.ClickException(
            "Please specify APP_TARGET, or --db and --app-name.\n"
            "  synor show ./app.py              # from module\n"
            "  synor show --db ./my.db --app-name MyApp  # from database"
        )


def _parse_stable_path(path_str: str) -> StablePath:
    """Parse a CLI stable path string into a StablePath.

    Accepts formats like:
      /"files"/"file1.txt"   (quoted parts, as displayed by StablePath.__str__)
      /files/file1.txt       (unquoted parts)
    """
    path = StablePath()
    # Strip leading slash
    stripped = path_str.strip("/")
    if not stripped:
        return path
    for part in stripped.split("/"):
        # Strip surrounding quotes if present
        if len(part) >= 2 and part.startswith('"') and part.endswith('"'):
            part = part[1:-1]
        path = path / part
    return path


async def _show_from_app(
    app: App[Any, Any],
    tree: bool = False,
    long_format: bool = False,
    stable_path: str | None = None,
    recursive: bool = False,
    parents: bool = False,
    fingerprints: bool = False,
) -> None:
    try:
        if stable_path is not None:
            # Targeted query — no scan needed
            path_obj = _parse_stable_path(stable_path)
            details = await query_stable_path_details(
                app,
                path_obj,
                include_children=recursive,
                recursive=recursive,
                include_parents=parents,
            )
            _print_details(details, fingerprints)
        elif long_format:
            # Stream details in one read txn with one shared resolver
            # (no buffering, no per-path txn/resolver).
            click.echo("Stable paths:")
            count = 0
            async for detail in iter_stable_path_details(app):
                _print_one_detail(detail, fingerprints)
                count += 1
            if count == 0:
                click.echo("  (none)")
        elif tree:
            component_node_type = _core.StablePathNodeType.component()
            await _print_tree_streaming(iter_stable_paths(app), component_node_type)
        else:
            click.echo("Stable paths:")
            async for item in iter_stable_paths(app):
                click.echo(f"  {StablePath(item.path)}")
    finally:
        await _stop_all_environments()


async def _show_from_database(
    db_path: str,
    app_name: str,
    tree: bool = False,
    long_format: bool = False,
    stable_path: str | None = None,
    recursive: bool = False,
    parents: bool = False,
    fingerprints: bool = False,
) -> None:
    db_path_obj = pathlib.Path(db_path)
    if not db_path_obj.exists():
        raise click.ClickException(f"Database path does not exist: {db_path}")

    from synor._internal.setting import Settings

    env = Environment(
        Settings(db_path=db_path_obj),
        event_loop=asyncio.get_running_loop(),
    )

    if stable_path is not None:
        path_obj = _parse_stable_path(stable_path)
        details = await query_stable_path_details_by_name(
            env,
            app_name,
            path_obj,
            include_children=recursive,
            recursive=recursive,
            include_parents=parents,
        )
        _print_details(details, fingerprints)
    elif long_format:
        click.echo("Stable paths:")
        count = 0
        async for detail in iter_stable_path_details_by_name(env, app_name):
            _print_one_detail(detail, fingerprints)
            count += 1
        if count == 0:
            click.echo("  (none)")
    elif tree:
        component_node_type = _core.StablePathNodeType.component()
        await _print_tree_streaming(
            iter_stable_paths_by_name(env, app_name), component_node_type
        )
    else:
        click.echo("Stable paths:")
        async for item in iter_stable_paths_by_name(env, app_name):
            click.echo(f"  {StablePath(item.path)}")


async def _show_target_states_from_app(
    app: App[Any, Any],
    tree: bool = False,
    fingerprints: bool = False,
) -> None:
    try:
        await _print_target_states(iter_target_states(app), fingerprints, tree)
    finally:
        await _stop_all_environments()


async def _show_target_states_from_database(
    db_path: str,
    app_name: str,
    tree: bool = False,
    fingerprints: bool = False,
) -> None:
    db_path_obj = pathlib.Path(db_path)
    if not db_path_obj.exists():
        raise click.ClickException(f"Database path does not exist: {db_path}")

    from synor._internal.setting import Settings

    env = Environment(
        Settings(db_path=db_path_obj),
        event_loop=asyncio.get_running_loop(),
    )
    await _print_target_states(
        iter_target_states_by_name(env, app_name), fingerprints, tree
    )


async def _print_target_states(
    entries: AsyncIterator[_core.TargetStateEntry], fingerprints: bool, tree: bool
) -> None:
    click.echo("Target states:")
    count = 0
    if tree:
        # Stored order yields a parent's entry before its descendants and keeps
        # subtrees contiguous, so comparing against the previously printed path
        # is enough to place each entry (same shape as _print_tree_streaming).
        # Segment identity uses fingerprint segments (fixed-form, safe to
        # split); labels come from readable_segments, which may contain "/".
        prev_segments: list[str] = []
        async for entry in entries:
            count += 1
            fp_segments = entry.fingerprint_path.lstrip("/").split("/")
            labels = fp_segments if fingerprints else entry.readable_segments
            common = 0
            while (
                common < len(prev_segments)
                and common < len(fp_segments) - 1
                and prev_segments[common] == fp_segments[common]
            ):
                common += 1
            # Ancestor segments that have no entry of their own (e.g. root
            # providers) still get a node line the first time they appear.
            for depth in range(common, len(fp_segments) - 1):
                click.echo("  " * depth + f"- {labels[depth]}")
            depth = len(fp_segments) - 1
            marker = " [dangling]" if entry.dangling else ""
            owner = str(StablePath(entry.owner_component_path))
            click.echo("  " * depth + f"- {labels[depth]}{marker} owner:{owner or '/'}")
            prev_segments = fp_segments
    else:
        async for entry in entries:
            count += 1
            path = entry.fingerprint_path if fingerprints else entry.readable_path
            marker = " [dangling]" if entry.dangling else ""
            click.echo(f"  {path}{marker}")
            owner = str(StablePath(entry.owner_component_path))
            click.echo(f"    owner:{owner or '/'}")
    if count == 0:
        click.echo("  (none)")


def _print_details(
    details: list[_core.StablePathDetail], fingerprints: bool = False
) -> None:
    """Print a list of StablePathDetail in multi-line format."""
    if not details:
        click.echo("Stable paths:")
        click.echo("  (none)")
        return

    click.echo("Stable paths:")
    for detail in details:
        _print_one_detail(detail, fingerprints)


def _print_one_detail(
    detail: _core.StablePathDetail, fingerprints: bool = False
) -> None:
    """Print a single StablePathDetail in multi-line format."""
    path = StablePath(detail.path)
    node_type = (
        "component"
        if detail.node_type == _core.StablePathNodeType.component()
        else "directory"
    )
    click.echo(f"  {path}")
    click.echo(
        f"    type:{node_type} version:{detail.version}"
        f" processor:{detail.processor_name or '-'}"
    )
    click.echo(
        f"    has_memoization:{'true' if detail.has_memoization else 'false'}"
        f" target_state_count:{detail.target_state_count}"
    )
    if detail.target_state_items:
        click.echo("    Target states:")
        for item_summary in detail.target_state_items:
            provider_gen = (
                f"{item_summary.provider_generation.provider_id}"
                f".{item_summary.provider_generation.provider_schema_version}"
                if item_summary.provider_generation is not None
                else "None"
            )
            states = ", ".join(f"{s.version}:{s.state}" for s in item_summary.states)
            path_str = (
                item_summary.fingerprint_path
                if fingerprints
                else item_summary.target_state_path
            )
            click.echo(f"      - path:{path_str}")
            click.echo(
                f"        states:{states or '-'}"
                f" schema_version:{item_summary.provider_schema_version}"
                f" generation:{provider_gen}"
            )
    click.echo()


async def _stop_all_environments() -> None:
    for env_info in get_registered_environment_infos():
        env = env_info.env
        if isinstance(env, LazyEnvironment):
            await env.stop()


def _command_policy(offline: bool) -> syn.EgressPolicy:
    try:
        return syn.policy_from_env(offline=offline)
    except ValueError as error:
        raise click.ClickException(str(error)) from error


def _command_pii_policy() -> syn.PIIPolicy:
    try:
        return syn.pii_policy_from_env()
    except ValueError as error:
        raise click.ClickException(str(error)) from error


def _command_state_store() -> syn.StateStore:
    try:
        return syn.state_store_from_env()
    except ValueError as error:
        raise click.ClickException(str(error)) from error


_NATIVE_EFFECT_ARCHIVE_SCHEMA_VERSION = 1


class _NativeEffectArchiveSummary(NamedTuple):
    archive_sha256: str
    app_count: int
    effect_count: int


class _NativeEffectCompactionSummary(NamedTuple):
    archive: _NativeEffectArchiveSummary
    requested: int
    deleted: int
    protected: int
    already_absent: int


class _NativeEffectDowngradeSummary(NamedTuple):
    archive: _NativeEffectArchiveSummary
    removed_schema_markers: int
    removed_effects: int
    removed_obligation_cursors: int
    removed_lineage_cursors: int
    removed_live_generation_keys: int


def _native_effect_environment(db_path: pathlib.Path) -> Environment:
    from synor._internal.setting import Settings

    if not db_path.exists():
        raise click.ClickException(f"Database path does not exist: {db_path}")
    return Environment(
        Settings(db_path=db_path),
        event_loop=asyncio.get_running_loop(),
    )


def _resolve_operator_path_outside_database(
    db_path: pathlib.Path,
    path: pathlib.Path,
    *,
    kind: str,
) -> pathlib.Path:
    database = db_path.resolve()
    resolved = path.resolve()
    if resolved == database or resolved.is_relative_to(database):
        raise click.ClickException(f"{kind} must be outside the source database")
    return resolved


def _native_effect_record_payload(record: _core._NativeEffectRecord) -> dict[str, Any]:
    return {
        "record_version": record.record_version,
        "evidence_id": record.evidence_id,
        "action_id": record.action_id,
        "operation": record.operation,
        "source_digest": record.source_digest,
        "source_generation": record.source_generation,
        "target_locator_digest": record.target_locator_digest,
        "tracking_locator": record.tracking_locator,
        "verification_policy": record.verification_policy,
        "cause": record.cause,
        "status": record.status,
        "created_at_unix_ms": record.created_at_unix_ms,
        "updated_at_unix_ms": record.updated_at_unix_ms,
        "attempt_count": record.attempt_count,
        "last_error_code": record.last_error_code,
    }


def _native_effect_app_payload(
    app_name: str,
    schema_version: int | None,
    effects: list[_core._NativeEffectRecord],
) -> dict[str, Any]:
    records = [_native_effect_record_payload(effect) for effect in effects]
    status_counts: dict[str, int] = {}
    for record in records:
        status = str(record["status"])
        status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "app_name": app_name,
        "native_schema_version": schema_version,
        "effect_count": len(records),
        "status_counts": dict(sorted(status_counts.items())),
        "effects": records,
    }


def _native_effect_archive_payload(
    apps: list[dict[str, Any]],
    *,
    purpose: str,
    retention: Mapping[str, Any],
    compaction_candidate_evidence_ids: list[str] | None = None,
    downgrade: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    canonical_apps = json.dumps(
        apps,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    payload: dict[str, Any] = {
        "schema": "synor.native-effects.archive",
        "schema_version": _NATIVE_EFFECT_ARCHIVE_SCHEMA_VERSION,
        "created_at": datetime.datetime.now(datetime.UTC)
        .isoformat()
        .replace("+00:00", "Z"),
        "purpose": purpose,
        "metadata_only": True,
        "default_retention": "indefinite",
        "retention": dict(retention),
        "app_count": len(apps),
        "effect_count": sum(int(app["effect_count"]) for app in apps),
        "apps_sha256": hashlib.sha256(canonical_apps).hexdigest(),
        "apps": apps,
    }
    if compaction_candidate_evidence_ids is not None:
        payload["compaction_candidate_evidence_ids"] = sorted(
            compaction_candidate_evidence_ids
        )
    if downgrade is not None:
        payload["downgrade"] = dict(downgrade)
    return payload


def _native_effect_archive_summary(
    archive: Mapping[str, Any],
    digest: str,
) -> _NativeEffectArchiveSummary:
    return _NativeEffectArchiveSummary(
        archive_sha256=digest,
        app_count=int(archive["app_count"]),
        effect_count=int(archive["effect_count"]),
    )


def _fsync_directory(path: pathlib.Path) -> None:
    if os.name == "nt":
        # Windows does not expose directory handles through os.open().
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_private_json_archive(
    path: pathlib.Path,
    payload: Mapping[str, Any],
) -> str:
    path = path.resolve()
    if path.exists():
        raise click.ClickException(f"Archive path already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            raise click.ClickException(f"Archive path already exists: {path}") from None
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return hashlib.sha256(encoded).hexdigest()


def _parse_completed_before(value: str) -> tuple[datetime.datetime, int]:
    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError:
        raise click.ClickException(
            "--completed-before must be an ISO-8601 timestamp with a timezone"
        ) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise click.ClickException(
            "--completed-before must include Z or an explicit UTC offset"
        )
    parsed = parsed.astimezone(datetime.UTC)
    milliseconds = int(parsed.timestamp() * 1000)
    if milliseconds < 0:
        raise click.ClickException("--completed-before must not predate Unix epoch")
    return parsed, milliseconds


async def _native_effect_export(
    db_path: pathlib.Path,
    app_name: str,
    output: pathlib.Path,
) -> _NativeEffectArchiveSummary:
    output = _resolve_operator_path_outside_database(
        db_path,
        output,
        kind="Archive path",
    )
    env = _native_effect_environment(db_path)
    snapshot = _core.native_effect_snapshot_by_name(env._core_env, app_name)
    if snapshot is None:
        raise click.ClickException(f"App database does not exist: {app_name}")
    app = _native_effect_app_payload(
        app_name,
        snapshot.schema_version,
        snapshot.effects,
    )
    archive = _native_effect_archive_payload(
        [app],
        purpose="operator_export",
        retention={"policy": "indefinite"},
    )
    digest = _write_private_json_archive(output, archive)
    return _native_effect_archive_summary(archive, digest)


async def _native_effect_compact(
    db_path: pathlib.Path,
    app_name: str,
    archive_path: pathlib.Path,
    completed_before: str,
) -> _NativeEffectCompactionSummary:
    archive_path = _resolve_operator_path_outside_database(
        db_path,
        archive_path,
        kind="Archive path",
    )
    cutoff, cutoff_ms = _parse_completed_before(completed_before)
    env = _native_effect_environment(db_path)
    snapshot = _core.native_effect_snapshot_by_name(env._core_env, app_name)
    if snapshot is None:
        raise click.ClickException(f"App database does not exist: {app_name}")
    app = _native_effect_app_payload(
        app_name,
        snapshot.schema_version,
        snapshot.effects,
    )
    candidates = sorted(
        record["evidence_id"]
        for record in app["effects"]
        if record["status"] == "completed"
        and int(record["updated_at_unix_ms"]) != 0
        and int(record["updated_at_unix_ms"]) <= cutoff_ms
    )
    archive = _native_effect_archive_payload(
        [app],
        purpose="retention_compaction",
        retention={
            "policy": "explicit_completed_before",
            "completed_before": cutoff.isoformat().replace("+00:00", "Z"),
            "unknown_timestamp_records": "retained",
            "cursor_referenced_records": "retained",
        },
        compaction_candidate_evidence_ids=candidates,
    )
    digest = _write_private_json_archive(archive_path, archive)
    result = _core.compact_native_effects_by_name(
        env._core_env,
        app_name,
        candidates,
    )
    return _NativeEffectCompactionSummary(
        archive=_native_effect_archive_summary(archive, digest),
        requested=result.requested,
        deleted=result.deleted,
        protected=result.protected,
        already_absent=result.already_absent,
    )


async def _prepare_native_downgrade(
    db_path: pathlib.Path,
    output_db: pathlib.Path,
    archive_path: pathlib.Path,
) -> _NativeEffectDowngradeSummary:
    source = db_path.resolve()
    output = output_db.resolve()
    archive = _resolve_operator_path_outside_database(
        source,
        archive_path,
        kind="Archive path",
    )
    if output == source or output.is_relative_to(source):
        raise click.ClickException(
            "Downgrade output must be outside the source database"
        )
    if output.exists():
        raise click.ClickException(f"Downgrade output already exists: {output}")
    if archive.exists():
        raise click.ClickException(f"Archive path already exists: {archive}")
    if archive.is_relative_to(output):
        raise click.ClickException("Archive path must be outside the downgrade output")

    output.parent.mkdir(parents=True, exist_ok=True)
    staging = output.parent / f".{output.name}.synor-staging-{uuid.uuid4().hex}"
    publish_lock = output.parent / f".{output.name}.synor-publish.lock"
    env = _native_effect_environment(source)
    try:
        result = _core.prepare_native_downgrade(env._core_env, str(staging))
        apps = [
            _native_effect_app_payload(
                app.app_name,
                app.schema_version,
                app.effects,
            )
            for app in result.apps
        ]
        removed = {
            "removed_schema_markers": result.removed_schema_markers,
            "removed_effects": result.removed_effects,
            "removed_obligation_cursors": result.removed_obligation_cursors,
            "removed_lineage_cursors": result.removed_lineage_cursors,
            "removed_live_generation_keys": result.removed_live_generation_keys,
        }
        archive_payload = _native_effect_archive_payload(
            apps,
            purpose="downgrade_preparation",
            retention={"policy": "external_archive_before_native_metadata_strip"},
            downgrade={
                "source_database_modified": False,
                "copy_native_metadata_removed": True,
                **removed,
            },
        )
        archive_digest = _write_private_json_archive(archive, archive_payload)
        ready_payload = {
            "schema": "synor.native-effects.downgrade-ready",
            "schema_version": 1,
            "archive_sha256": archive_digest,
            **removed,
        }
        _write_private_json_archive(
            staging / "DOWNGRADE_READY.json",
            ready_payload,
        )

        lock_descriptor = os.open(
            publish_lock,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            if output.exists():
                raise click.ClickException(f"Downgrade output already exists: {output}")
            os.rename(staging, output)
            _fsync_directory(output.parent)
        finally:
            os.close(lock_descriptor)
            publish_lock.unlink(missing_ok=True)
        return _NativeEffectDowngradeSummary(
            archive=_native_effect_archive_summary(archive_payload, archive_digest),
            removed_schema_markers=result.removed_schema_markers,
            removed_effects=result.removed_effects,
            removed_obligation_cursors=result.removed_obligation_cursors,
            removed_lineage_cursors=result.removed_lineage_cursors,
            removed_live_generation_keys=result.removed_live_generation_keys,
        )
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise


_REVOCATION_JSON_SCHEMA_VERSION = 1
_REVOCATION_CONTROL_PREFIX = "revocation/v1/"
_REVOCATION_SUPPRESSION_PREFIX = "revocation/v1/suppression/"
_REVOCATION_SUPPRESSION_EPOCH_KEY = "revocation/v1/suppression_epoch.json"
_REVOCATION_SERVING_FENCE_PREFIX = "revocation/v1/serving_fences/"


class _SafeRevocationCommandError(RuntimeError):
    """Privacy-safe operator/repository failure for Click conversion."""


def _revocation_api() -> Any:
    from synor import revocation as syn_revocation

    return syn_revocation


def _revocation_repository(store: syn.StateStore) -> Any:
    return _revocation_api().RevocationRepository(store)


def _resolve_revocation_operator_target(explicit: str | None) -> str:
    target = explicit or os.environ.get("SYNOR_REVOCATION_OPERATOR", "")
    target = target.strip()
    if not target:
        raise click.ClickException(
            "Configure --operator MODULE:OBJECT or SYNOR_REVOCATION_OPERATOR."
        )
    return target


def _load_revocation_operator(target: str) -> Any:
    module_ref, separator, object_name = target.rpartition(":")
    if not separator or not module_ref or not object_name.isidentifier():
        raise _SafeRevocationCommandError("operator must use the MODULE:OBJECT format")
    try:
        module = load_user_app(module_ref)
        operator = getattr(module, object_name)
    except Exception:
        raise _SafeRevocationCommandError(
            "the configured revocation operator could not be loaded"
        ) from None
    if isinstance(operator, type):
        raise _SafeRevocationCommandError(
            "the configured revocation operator must be an object instance"
        )

    operator_type = _revocation_api().RevocationOperator
    try:
        matches_protocol = isinstance(operator, operator_type)
    except TypeError:
        matches_protocol = all(
            callable(getattr(operator, method, None))
            for method in ("verify", "retry", "scan")
        )
    if not matches_protocol:
        raise _SafeRevocationCommandError(
            "the configured object does not implement RevocationOperator"
        )
    return operator


async def _snapshot_revocation_control(
    store: syn.StateStore,
) -> tuple[tuple[str, bytes], ...]:
    """Read all versioned control bytes or fail on a concurrent disappearance."""

    snapshot: list[tuple[str, bytes]] = []
    for key in sorted(await store.list(_REVOCATION_CONTROL_PREFIX)):
        value = await store.get(key)
        if value is None:
            raise _SafeRevocationCommandError(
                "revocation control state changed while it was being inspected"
            )
        snapshot.append((key, bytes(value)))
    return tuple(snapshot)


def _serving_suppression_snapshot(
    snapshot: tuple[tuple[str, bytes], ...],
) -> dict[str, bytes]:
    return {
        key: value
        for key, value in snapshot
        if (
            key.startswith(_REVOCATION_SUPPRESSION_PREFIX)
            or key == _REVOCATION_SUPPRESSION_EPOCH_KEY
            or key.startswith(_REVOCATION_SERVING_FENCE_PREFIX)
        )
    }


def _versioned_revocation_payload(
    schema: str,
    **fields: Any,
) -> dict[str, Any]:
    return {
        "schema": f"synor.revocations.{schema}",
        "schema_version": _REVOCATION_JSON_SCHEMA_VERSION,
        **fields,
    }


def _echo_revocation_json(schema: str, **fields: Any) -> None:
    click.echo(
        json.dumps(
            _versioned_revocation_payload(schema, **fields),
            indent=2,
            sort_keys=True,
        )
    )


def _safe_result_payload(result: Any, expected_type: type[Any]) -> dict[str, Any]:
    if not isinstance(result, expected_type):
        raise _SafeRevocationCommandError(
            "the revocation operator returned an invalid result"
        )
    try:
        payload = result.to_dict()
    except Exception:
        raise _SafeRevocationCommandError(
            "the revocation operator returned an invalid result"
        ) from None
    if not isinstance(payload, dict):
        raise _SafeRevocationCommandError(
            "the revocation operator returned an invalid result"
        )
    return payload


def _raise_safe_revocation_click_error(
    action: str,
    error: BaseException,
) -> NoReturn:
    if isinstance(error, click.ClickException):
        raise error
    if isinstance(error, _SafeRevocationCommandError):
        detail = str(error)
    else:
        detail = f"{type(error).__name__}"
    raise click.ClickException(f"{action} failed: {detail}") from None


def _metadata_value(metadata: Mapping[str, object], name: str) -> str:
    value = metadata.get(name)
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value)


def _load_app_with_policy(app_target: str, policy: syn.EgressPolicy) -> App[Any, Any]:
    with syn.policy_scope(policy):
        return _load_app(app_target)


def _change_line(change: syn.PlannedChange) -> str:
    details = json.dumps(change.details, sort_keys=True, separators=(",", ":"))
    if len(details) > 240:
        details = details[:237] + "..."
    return f"[{change.index}] {change.operation} {change.action_type} {details}"


@cli.command()
@click.argument("app_target", type=str, required=False)
@click.option(
    "--model",
    "model_path",
    type=click.Path(path_type=pathlib.Path),
    default=None,
    help="Validate that a local model file exists.",
)
@click.option(
    "--offline",
    is_flag=True,
    default=False,
    help="Run app checks with guarded networking denied.",
)
@click.option("--json", "json_output", is_flag=True, help="Emit JSON.")
def doctor(
    app_target: str | None,
    model_path: pathlib.Path | None,
    offline: bool,
    json_output: bool,
) -> None:
    """Check whether this machine can run Synor safely."""

    from synor import audit as syn_audit

    checks: list[dict[str, str]] = []

    def add(name: str, status: str, detail: str) -> None:
        checks.append({"name": name, "status": status, "detail": detail})

    if sys.version_info >= (3, 11):
        add("python", "pass", sys.version.split()[0])
    else:
        add("python", "fail", "Synor requires Python 3.11 or newer")

    core_path = pathlib.Path(getattr(_core, "__file__", ""))
    if core_path.is_file():
        add("native_core", "pass", str(core_path))
    else:
        add("native_core", "fail", "synor._internal.core is not installed")

    policy = _command_policy(offline)
    add(
        "egress_policy",
        "pass",
        (
            "offline: guarded network denied"
            if policy.network_access is syn.NetworkAccess.DENY
            else "network allowed by policy"
        ),
    )
    pii_policy = _command_pii_policy()
    add(
        "pii_policy",
        "pass",
        f"{pii_policy.action.value}: "
        + ",".join(sorted(item.value for item in pii_policy.categories)),
    )

    state_store = _command_state_store()

    async def _check_control_store() -> None:
        probe_key = f"doctor/probe-{os.getpid()}"
        await state_store.put(probe_key, b"synor-doctor")
        if await state_store.get(probe_key) != b"synor-doctor":
            raise RuntimeError("control store returned different probe data")
        await state_store.delete(probe_key)

    try:
        asyncio.run(_check_control_store())
        encrypted = isinstance(state_store, syn.EncryptedStateStore)
        add(
            "control_state",
            "pass",
            f"{type(state_store).__name__}; encrypted={'yes' if encrypted else 'no'}",
        )
    except Exception as error:
        add("control_state", "fail", f"{type(error).__name__}: {error}")

    audit_root = syn_audit.resolve_audit_root()
    try:
        audit_root.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=audit_root):
            pass
        add("audit_directory", "pass", str(audit_root.resolve()))
    except OSError as error:
        add("audit_directory", "fail", f"{type(error).__name__}: {error}")

    if model_path is not None:
        resolved_model = model_path.expanduser()
        if resolved_model.is_file():
            add("local_model", "pass", str(resolved_model.resolve()))
        else:
            add("local_model", "fail", f"file does not exist: {resolved_model}")
    else:
        add("local_model", "skip", "no --model path supplied")

    async def _check_app() -> None:
        if app_target is None:
            add("app", "skip", "no APP_TARGET supplied")
            return
        try:
            app = _load_app_with_policy(app_target, policy)
            with syn.policy_scope(policy):
                env = await app._environment._get_env()
            add("app", "pass", f"{app._name} in environment {env.name}")
            if env.settings.db_path is None:
                add("state_database", "skip", "no persistent database path")
            else:
                db_path = pathlib.Path(env.settings.db_path)
                add("state_database", "pass", str(db_path.resolve()))
        except Exception as error:
            add("app", "fail", f"{type(error).__name__}: {error}")
        finally:
            await _stop_all_environments()

    asyncio.run(_check_app())
    failed = any(item["status"] == "fail" for item in checks)
    if json_output:
        click.echo(json.dumps({"ok": not failed, "checks": checks}, indent=2))
    else:
        click.echo("Synor doctor")
        for item in checks:
            marker = {
                "pass": "PASS",
                "fail": "FAIL",
                "skip": "SKIP",
            }[item["status"]]
            click.echo(f"  {marker:4} {item['name']}: {item['detail']}")
        click.echo("Ready." if not failed else "Not ready.")
    if failed:
        raise click.exceptions.Exit(1)


async def _run_plan_command(
    *,
    app_target: str,
    full_reprocess: bool,
    offline: bool,
    command: str,
) -> syn.ExecutionReport:
    policy = _command_policy(offline)
    app = _load_app_with_policy(app_target, policy)
    runtime = syn.SynorRuntime(
        policy=policy,
        pii_policy=_command_pii_policy(),
        state_store=_command_state_store(),
    )
    try:
        return await runtime.plan(
            app,
            full_reprocess=full_reprocess,
            app_target=app_target,
            command=command,
        )
    finally:
        await _stop_all_environments()


@cli.command()
@click.argument("app_target", type=str)
@click.option(
    "--full-reprocess",
    is_flag=True,
    default=False,
    help="Plan with all memoized work invalidated.",
)
@click.option(
    "--offline",
    is_flag=True,
    default=False,
    help="Deny guarded Python networking and policy-aware egress.",
)
@click.option("--json", "json_output", is_flag=True, help="Emit JSON.")
def plan(
    app_target: str,
    full_reprocess: bool,
    offline: bool,
    json_output: bool,
) -> None:
    """Show what an app would change without applying it."""

    report = asyncio.run(
        _run_plan_command(
            app_target=app_target,
            full_reprocess=full_reprocess,
            offline=offline,
            command="plan",
        )
    )
    if json_output:
        click.echo(
            json.dumps(
                {
                    "run_id": report.run_id,
                    "app": report.app_name,
                    "mode": report.mode.value,
                    "changes": [
                        {
                            "index": change.index,
                            "operation": change.operation,
                            "action_type": change.action_type,
                            "details": change.details,
                        }
                        for change in report.planned_changes
                    ],
                    "manifest": str(report.manifest_path),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return
    click.echo(f"Plan for {report.app_name}")
    if report.planned_changes:
        for change in report.planned_changes:
            click.echo("  " + _change_line(change))
    else:
        click.echo("  No target changes.")
    click.echo(f"{len(report.planned_changes)} planned change(s); nothing applied.")
    click.echo(f"Manifest: {report.manifest_path}")


@cli.command()
@click.argument("app_target", type=str)
@click.option(
    "--full-reprocess",
    is_flag=True,
    default=False,
    help="Diff with all memoized work invalidated.",
)
@click.option(
    "--offline",
    is_flag=True,
    default=False,
    help="Deny guarded Python networking and policy-aware egress.",
)
@click.option("--json", "json_output", is_flag=True, help="Emit JSON.")
def diff(
    app_target: str,
    full_reprocess: bool,
    offline: bool,
    json_output: bool,
) -> None:
    """Show a redacted target-action diff without applying it."""

    report = asyncio.run(
        _run_plan_command(
            app_target=app_target,
            full_reprocess=full_reprocess,
            offline=offline,
            command="diff",
        )
    )
    if json_output:
        click.echo(
            json.dumps(
                {
                    "run_id": report.run_id,
                    "app": report.app_name,
                    "changes": [
                        {
                            "index": change.index,
                            "operation": change.operation,
                            "action_type": change.action_type,
                            "details": change.details,
                        }
                        for change in report.planned_changes
                    ],
                    "manifest": str(report.manifest_path),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return
    click.echo(f"--- {report.app_name}: current")
    click.echo(f"+++ {report.app_name}: planned")
    if report.planned_changes:
        for change in report.planned_changes:
            marker = "-" if change.operation == "delete" else "~"
            click.echo(f"{marker} {_change_line(change)}")
    else:
        click.echo("  No target changes.")
    click.echo(f"Manifest: {report.manifest_path}")


@cli.command()
@click.argument("app_target", type=str)
@click.option(
    "--offline",
    is_flag=True,
    default=False,
    help="Deny guarded Python networking and policy-aware egress.",
)
@click.option("--json", "json_output", is_flag=True, help="Emit JSON.")
def explain(app_target: str, offline: bool, json_output: bool) -> None:
    """Explain an app's local state, ownership, policy, and latest run."""

    policy = _command_policy(offline)
    app = _load_app_with_policy(app_target, policy)
    runtime = syn.SynorRuntime(
        policy=policy,
        pii_policy=_command_pii_policy(),
        state_store=_command_state_store(),
    )

    async def _do() -> syn.AppExplanation:
        try:
            return await runtime.explain(app, app_target=app_target)
        finally:
            await _stop_all_environments()

    explanation = asyncio.run(_do())
    payload = {
        "app": explanation.app_name,
        "environment": explanation.environment,
        "db_path": explanation.db_path,
        "stable_path_count": explanation.stable_path_count,
        "target_state_count": explanation.target_state_count,
        "policy": explanation.policy,
        "latest_run": explanation.latest_run,
    }
    if json_output:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(f"App: {explanation.app_name}")
    click.echo(f"Environment: {explanation.environment}")
    click.echo(f"State database: {explanation.db_path}")
    click.echo(f"Stable paths: {explanation.stable_path_count}")
    click.echo(f"Target states: {explanation.target_state_count}")
    click.echo(
        "Guarded network: "
        + (
            "denied"
            if policy.network_access is syn.NetworkAccess.DENY
            else "allowed by policy"
        )
    )
    if explanation.latest_run is None:
        click.echo("Latest run: none")
    else:
        click.echo(
            "Latest run: "
            f"{explanation.latest_run.get('run_id')} "
            f"({explanation.latest_run.get('status')})"
        )


@cli.command()
@click.argument(
    "replay_path",
    type=click.Path(exists=True, path_type=pathlib.Path),
)
@click.option(
    "--app-target",
    type=str,
    default=None,
    help="Override the APP_TARGET captured in the replay envelope.",
)
@click.option(
    "--offline",
    is_flag=True,
    default=False,
    help="Deny guarded Python networking and policy-aware egress.",
)
@click.option("--json", "json_output", is_flag=True, help="Emit JSON.")
def replay(
    replay_path: pathlib.Path,
    app_target: str | None,
    offline: bool,
    json_output: bool,
) -> None:
    """Re-run a captured preview and verify deterministic evidence."""

    from synor import replay as syn_replay

    try:
        envelope = syn_replay.load_replay_envelope(replay_path)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise click.ClickException(f"Cannot load replay: {error}") from error
    selected_target = app_target or envelope.app_target
    policy = _command_policy(offline)
    app = _load_app_with_policy(selected_target, policy)
    runtime = syn.SynorRuntime(
        policy=policy,
        pii_policy=_command_pii_policy(),
        state_store=_command_state_store(),
    )

    async def _do() -> syn.ReplayVerification:
        try:
            return await runtime.replay(
                app,
                envelope,
                app_target=selected_target,
            )
        finally:
            await _stop_all_environments()

    verification = asyncio.run(_do())
    payload = {
        "matched": verification.matched,
        "source_matched": verification.source_matched,
        "dependencies_matched": verification.dependencies_matched,
        "actions_matched": verification.actions_matched,
        "policy_matched": verification.policy_matched,
        "runtime_matched": verification.runtime_matched,
        "expected_action_digest": verification.expected_action_digest,
        "actual_action_digest": verification.actual_action_digest,
        "expected_action_count": verification.expected_action_count,
        "actual_action_count": verification.actual_action_count,
    }
    if json_output:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
    else:
        click.echo("Replay verification")
        click.echo(
            f"  source: {'MATCH' if verification.source_matched else 'MISMATCH'}"
        )
        click.echo(
            "  dependencies: "
            f"{'MATCH' if verification.dependencies_matched else 'MISMATCH'}"
        )
        click.echo(
            f"  actions: {'MATCH' if verification.actions_matched else 'MISMATCH'}"
        )
        click.echo(
            f"  policy: {'MATCH' if verification.policy_matched else 'MISMATCH'}"
        )
        click.echo(
            f"  runtime: {'MATCH' if verification.runtime_matched else 'MISMATCH'}"
        )
        click.echo(
            "Verified; nothing applied." if verification.matched else "Mismatch."
        )
    if not verification.matched:
        raise click.exceptions.Exit(1)


@cli.command("lock")
@click.argument("app_target", type=str)
@click.option(
    "--output",
    type=click.Path(path_type=pathlib.Path),
    default=pathlib.Path("synor.lock.json"),
    show_default=True,
)
def lock_pipeline(app_target: str, output: pathlib.Path) -> None:
    """Create an offline-verifiable pipeline lockfile."""

    from synor import packaging as syn_packaging

    try:
        lock = syn_packaging.build_pipeline_lock(app_target)
        path = syn_packaging.write_pipeline_lock(lock, output)
        verification = syn_packaging.verify_pipeline_lock(lock)
    except (OSError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(
        f"Locked {len(lock.files)} source file(s) and "
        f"{len(lock.distributions)} distribution(s): {path}"
    )
    if not verification.ok:
        raise click.ClickException("new lockfile did not verify against this workspace")


@cli.command("package")
@click.argument("app_target", type=str)
@click.option(
    "--output",
    type=click.Path(path_type=pathlib.Path),
    default=pathlib.Path("pipeline.synor"),
    show_default=True,
)
@click.option(
    "--lockfile",
    type=click.Path(path_type=pathlib.Path),
    default=pathlib.Path("synor.lock.json"),
    show_default=True,
)
def package_pipeline(
    app_target: str,
    output: pathlib.Path,
    lockfile: pathlib.Path,
) -> None:
    """Build a deterministic local pipeline package and lockfile."""

    from synor import packaging as syn_packaging

    try:
        lock = syn_packaging.build_pipeline_lock(app_target)
        syn_packaging.write_pipeline_lock(lock, lockfile)
        package_path = syn_packaging.create_pipeline_package(lock, output)
        verification = syn_packaging.verify_pipeline_package(package_path)
    except (OSError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    if not verification.ok:
        raise click.ClickException("; ".join(verification.errors))
    click.echo(f"Package: {package_path}")
    click.echo(f"Lockfile: {lockfile}")
    click.echo(f"SHA-256: {verification.package_digest}")


@cli.command("package-verify")
@click.argument(
    "package_path",
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
)
def package_verify(package_path: pathlib.Path) -> None:
    """Verify a Synor pipeline package without extracting or downloading."""

    from synor import packaging as syn_packaging

    try:
        verification = syn_packaging.verify_pipeline_package(package_path)
    except OSError as error:
        raise click.ClickException(str(error)) from error
    if not verification.ok:
        raise click.ClickException("; ".join(verification.errors))
    click.echo(f"Verified: {package_path}")
    click.echo(f"SHA-256: {verification.package_digest}")


@cli.command("state-key")
def state_key() -> None:
    """Generate an encryption key for SYNOR_STATE_KEY."""

    click.echo(syn.generate_state_key())


_REVOCATION_STATUS_CHOICES = (
    "open",
    "overdue",
    "observed",
    "suppressed_from_serving",
    "deletion_planned",
    "dispatched",
    "acknowledged",
    "consistency_fence_reached",
    "absence_verified",
    "retained_isolated",
    "closed",
    "failed",
    "blocked",
)


async def _revocations_list_payload(
    store: syn.StateStore,
    *,
    status: str | None,
) -> list[dict[str, object]]:
    repository = _revocation_repository(store)
    revocation = _revocation_api()
    if status in {"open", "overdue"}:
        cases = await repository.list()
    else:
        cases = await repository.list(status=status)
    now = datetime.datetime.now(datetime.timezone.utc)
    terminal = {
        revocation.RevocationStage.VERIFIED,
        revocation.RevocationStage.RETAINED_ISOLATED,
        revocation.RevocationStage.CLOSED,
    }

    def is_overdue(case: Any) -> bool:
        if case.stage in terminal:
            return False
        if case.stage is revocation.RevocationStage.OBSERVED and now > case.suppress_by:
            return True
        return bool(now > case.verify_by)

    if status == "open":
        cases = tuple(case for case in cases if case.stage not in terminal)
    elif status == "overdue":
        cases = tuple(case for case in cases if is_overdue(case))
    results: list[dict[str, object]] = []
    for case in cases:
        metadata = repository.case_metadata(case)
        metadata["overdue"] = is_overdue(case)
        results.append(metadata)
    return results


async def _revocations_show_payload(
    store: syn.StateStore,
    *,
    case_id: str,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    repository = _revocation_repository(store)
    case = await repository.get(case_id)
    if case is None:
        raise _SafeRevocationCommandError("revocation case was not found")
    receipts = await repository.receipts(case_id)
    return (
        repository.case_metadata(case),
        [repository.receipt_metadata(receipt) for receipt in receipts],
    )


def _operator_exception(error: BaseException) -> _SafeRevocationCommandError:
    if isinstance(error, _SafeRevocationCommandError):
        return error
    return _SafeRevocationCommandError(f"operator raised {type(error).__name__}")


async def _verify_revocation(
    store: syn.StateStore,
    *,
    case_id: str,
    operator_target: str,
) -> tuple[Any, dict[str, Any]]:
    revocation = _revocation_api()
    repository = _revocation_repository(store)
    if await repository.get(case_id) is None:
        raise _SafeRevocationCommandError("revocation case was not found")
    before = await _snapshot_revocation_control(store)
    try:
        operator = _load_revocation_operator(operator_target)
        result = await operator.verify(case_id)
    except Exception as error:
        after_failure = await _snapshot_revocation_control(store)
        if after_failure != before:
            raise _SafeRevocationCommandError(
                "verify operator mutated revocation control state"
            ) from None
        raise _operator_exception(error) from None
    after = await _snapshot_revocation_control(store)
    if after != before:
        raise _SafeRevocationCommandError(
            "verify operator mutated revocation control state"
        )
    payload = _safe_result_payload(result, revocation.RevocationOperatorResult)
    if result.case_id != case_id or result.operation != "verify" or result.mutated:
        raise _SafeRevocationCommandError(
            "verify operator returned a result that violates its contract"
        )
    return result, payload


async def _retry_revocation(
    store: syn.StateStore,
    *,
    case_id: str,
    operator_target: str,
) -> tuple[Any, dict[str, Any]]:
    revocation = _revocation_api()
    repository = _revocation_repository(store)
    before_case = await repository.get(case_id)
    if before_case is None:
        raise _SafeRevocationCommandError("revocation case was not found")
    if before_case.stage in {
        revocation.RevocationStage.VERIFIED,
        revocation.RevocationStage.RETAINED_ISOLATED,
        revocation.RevocationStage.CLOSED,
    }:
        raise _SafeRevocationCommandError(
            "a terminal revocation case cannot be retried"
        )
    before_health = await repository.startup_health()
    if before_health.safe_error_code in {
        "revocation.state_corrupt",
        "revocation.state_unavailable",
    }:
        raise _SafeRevocationCommandError("revocation control state is unavailable")
    if case_id in before_health.unsafe_case_ids:
        raise _SafeRevocationCommandError(
            "retry requires an exact active serving suppression"
        )
    before_receipts = await repository.receipts(case_id)
    before_control = await _snapshot_revocation_control(store)
    try:
        operator = _load_revocation_operator(operator_target)
        result = await operator.retry(case_id)
    except Exception as error:
        raise _operator_exception(error) from None

    after_case = await repository.get(case_id)
    if after_case is None:
        raise _SafeRevocationCommandError("retry operator removed the revocation case")
    after_receipts = await repository.receipts(case_id)
    after_control = await _snapshot_revocation_control(store)
    after_health = await repository.startup_health()
    payload = _safe_result_payload(result, revocation.RevocationOperatorResult)

    before_ids = {receipt.receipt_id for receipt in before_receipts}
    after_ids = {receipt.receipt_id for receipt in after_receipts}
    if not before_ids.issubset(after_ids):
        raise _SafeRevocationCommandError(
            "retry operator removed immutable receipt evidence"
        )
    new_receipts = tuple(
        receipt for receipt in after_receipts if receipt.receipt_id not in before_ids
    )
    prior_attempt = max(
        (receipt.attempt for receipt in before_receipts),
        default=-1,
    )
    new_attempt = max(
        (receipt.attempt for receipt in new_receipts),
        default=-1,
    )
    if (
        not new_receipts
        or new_attempt <= prior_attempt
        or any(receipt.attempt != new_attempt for receipt in new_receipts)
    ):
        raise _SafeRevocationCommandError(
            "retry operator did not record a new receipt attempt"
        )
    new_ids = {receipt.receipt_id for receipt in new_receipts}
    if (
        result.case_id != case_id
        or result.operation != "retry"
        or not result.mutated
        or result.attempt != new_attempt
        or new_ids != set(result.receipt_ids)
        or result.stage is not after_case.stage
    ):
        raise _SafeRevocationCommandError(
            "retry operator result does not match its durable receipt attempt"
        )
    before_suppression = _serving_suppression_snapshot(before_control)
    after_suppression = _serving_suppression_snapshot(after_control)
    if before_suppression != after_suppression:
        raise _SafeRevocationCommandError(
            "retry operator changed serving-suppression control state"
        )
    if case_id in after_health.unsafe_case_ids:
        raise _SafeRevocationCommandError(
            "retry no longer has an exact active serving suppression"
        )
    if (
        before_case.stage is not revocation.RevocationStage.CLOSED
        and after_case.stage is revocation.RevocationStage.CLOSED
    ):
        raise _SafeRevocationCommandError(
            "retry operator closed a case outside engine final commit"
        )
    return result, payload


async def _scan_revocations(
    store: syn.StateStore,
    *,
    target_id: str,
    operator_target: str,
) -> tuple[Any, dict[str, Any]]:
    revocation = _revocation_api()
    before = await _snapshot_revocation_control(store)
    try:
        operator = _load_revocation_operator(operator_target)
        result = await operator.scan(target_id)
    except Exception as error:
        after_failure = await _snapshot_revocation_control(store)
        if after_failure != before:
            raise _SafeRevocationCommandError(
                "scan operator mutated revocation control state"
            ) from None
        raise _operator_exception(error) from None
    after = await _snapshot_revocation_control(store)
    if after != before:
        raise _SafeRevocationCommandError(
            "scan operator mutated revocation control state"
        )
    payload = _safe_result_payload(result, revocation.RevocationScanResult)
    if result.target_id != target_id:
        raise _SafeRevocationCommandError(
            "scan operator returned a result for a different target"
        )
    return result, payload


@cli.group("native-effects")
def native_effects_group() -> None:
    """Export, compact, and prepare downgrade copies of native evidence."""


@native_effects_group.command("export")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    required=True,
    help="Source Synor database directory.",
)
@click.option("--app-name", required=True, help="Persisted app name.")
@click.option(
    "--output",
    type=click.Path(file_okay=True, dir_okay=False, path_type=pathlib.Path),
    required=True,
    help="New private JSON archive path.",
)
@click.option("--json", "json_output", is_flag=True, help="Emit result JSON.")
def native_effects_export(
    db_path: pathlib.Path,
    app_name: str,
    output: pathlib.Path,
    json_output: bool,
) -> None:
    """Export a metadata-only native-effect archive without mutation."""

    try:
        exported = asyncio.run(_native_effect_export(db_path, app_name, output))
    except click.ClickException:
        raise
    except Exception as error:  # noqa: BLE001 - translate the native boundary for Click
        raise click.ClickException(f"native effect export failed: {error}") from None
    result = {
        "schema": "synor.native-effects.export-result",
        "schema_version": 1,
        "archive": str(output.resolve()),
        "archive_sha256": exported.archive_sha256,
        "effect_count": exported.effect_count,
    }
    if json_output:
        click.echo(json.dumps(result, indent=2, sort_keys=True))
    else:
        click.echo(f"Exported {result['effect_count']} native effect record(s).")
        click.echo(f"Archive: {result['archive']}")
        click.echo(f"SHA-256: {exported.archive_sha256}")


@native_effects_group.command("compact")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    required=True,
    help="Source Synor database directory.",
)
@click.option("--app-name", required=True, help="Persisted app name.")
@click.option(
    "--completed-before",
    required=True,
    help="ISO-8601 cutoff with Z or an explicit UTC offset.",
)
@click.option(
    "--archive",
    "archive_path",
    type=click.Path(file_okay=True, dir_okay=False, path_type=pathlib.Path),
    required=True,
    help="New private archive written and fsynced before compaction.",
)
@click.option(
    "--confirm-compaction",
    is_flag=True,
    help="Confirm deletion of archived, unreferenced completed evidence.",
)
@click.option("--json", "json_output", is_flag=True, help="Emit result JSON.")
def native_effects_compact(
    db_path: pathlib.Path,
    app_name: str,
    completed_before: str,
    archive_path: pathlib.Path,
    confirm_compaction: bool,
    json_output: bool,
) -> None:
    """Archive, then compact eligible completed native-effect history."""

    if not confirm_compaction:
        raise click.ClickException(
            "Pass --confirm-compaction to modify native evidence."
        )
    try:
        compacted = asyncio.run(
            _native_effect_compact(
                db_path,
                app_name,
                archive_path,
                completed_before,
            )
        )
    except click.ClickException:
        raise
    except Exception as error:  # noqa: BLE001 - translate the native boundary for Click
        raise click.ClickException(
            f"native effect compaction failed: {error}"
        ) from None
    result = {
        "schema": "synor.native-effects.compaction-result",
        "schema_version": 1,
        "archive": str(archive_path.resolve()),
        "archive_sha256": compacted.archive.archive_sha256,
        "archived_effect_count": compacted.archive.effect_count,
        "requested": compacted.requested,
        "deleted": compacted.deleted,
        "protected": compacted.protected,
        "already_absent": compacted.already_absent,
    }
    if json_output:
        click.echo(json.dumps(result, indent=2, sort_keys=True))
    else:
        click.echo(f"Archived {result['archived_effect_count']} record(s).")
        click.echo(
            f"Compaction: deleted={result['deleted']} "
            f"protected={result['protected']} "
            f"already_absent={result['already_absent']}"
        )
        click.echo(f"Archive: {result['archive']}")
        click.echo(f"SHA-256: {compacted.archive.archive_sha256}")


@native_effects_group.command("prepare-downgrade")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    required=True,
    help="Source Synor database directory.",
)
@click.option(
    "--output-db",
    type=click.Path(file_okay=False, path_type=pathlib.Path),
    required=True,
    help="New downgraded database directory.",
)
@click.option(
    "--archive",
    "archive_path",
    type=click.Path(file_okay=True, dir_okay=False, path_type=pathlib.Path),
    required=True,
    help="New private archive for metadata removed from the copy.",
)
@click.option(
    "--confirm-downgrade",
    is_flag=True,
    help="Confirm creation of a pre-native compatibility copy.",
)
@click.option("--json", "json_output", is_flag=True, help="Emit result JSON.")
def native_effects_prepare_downgrade(
    db_path: pathlib.Path,
    output_db: pathlib.Path,
    archive_path: pathlib.Path,
    confirm_downgrade: bool,
    json_output: bool,
) -> None:
    """Archive evidence and publish a separate pre-native database copy."""

    if not confirm_downgrade:
        raise click.ClickException(
            "Pass --confirm-downgrade to create a downgrade copy."
        )
    try:
        prepared = asyncio.run(
            _prepare_native_downgrade(
                db_path,
                output_db,
                archive_path,
            )
        )
    except click.ClickException:
        raise
    except Exception as error:  # noqa: BLE001 - translate the native boundary for Click
        raise click.ClickException(
            f"native downgrade preparation failed: {error}"
        ) from None
    result = {
        "schema": "synor.native-effects.downgrade-result",
        "schema_version": 1,
        "output_database": str(output_db.resolve()),
        "archive": str(archive_path.resolve()),
        "archive_sha256": prepared.archive.archive_sha256,
        "app_count": prepared.archive.app_count,
        "archived_effect_count": prepared.archive.effect_count,
        "removed_schema_markers": prepared.removed_schema_markers,
        "removed_effects": prepared.removed_effects,
        "removed_obligation_cursors": prepared.removed_obligation_cursors,
        "removed_lineage_cursors": prepared.removed_lineage_cursors,
        "removed_live_generation_keys": prepared.removed_live_generation_keys,
    }
    if json_output:
        click.echo(json.dumps(result, indent=2, sort_keys=True))
    else:
        click.echo(
            f"Prepared downgrade copy for {result['app_count']} app(s); "
            f"archived {result['archived_effect_count']} effect record(s)."
        )
        click.echo(f"Database copy: {result['output_database']}")
        click.echo(f"Archive: {result['archive']}")
        click.echo(f"SHA-256: {prepared.archive.archive_sha256}")


@cli.group("revocations")
def revocations_group() -> None:
    """Inspect and operate evidence-backed index revocations."""


@revocations_group.command("list")
@click.option(
    "--status",
    type=click.Choice(_REVOCATION_STATUS_CHOICES),
    default=None,
    help="Filter by lifecycle stage, open cases, or overdue cases.",
)
@click.option("--json", "json_output", is_flag=True, help="Emit versioned JSON.")
def revocations_list(status: str | None, json_output: bool) -> None:
    """List redacted revocation case summaries."""

    try:
        cases = asyncio.run(
            _revocations_list_payload(
                _command_state_store(),
                status=status,
            )
        )
    except Exception as error:
        _raise_safe_revocation_click_error("revocations list", error)
    if json_output:
        _echo_revocation_json(
            "list",
            status=status,
            count=len(cases),
            cases=cases,
        )
        return
    if not cases:
        click.echo("No revocation cases.")
        return
    for case in cases:
        marker = " OVERDUE" if case.get("overdue") is True else ""
        click.echo(
            f"{_metadata_value(case, 'case_id')}  "
            f"{_metadata_value(case, 'stage')}  "
            f"{_metadata_value(case, 'reason')}{marker}"
        )


@revocations_group.command("show")
@click.argument("case_id", type=str)
@click.option("--json", "json_output", is_flag=True, help="Emit versioned JSON.")
def revocations_show(case_id: str, json_output: bool) -> None:
    """Show one redacted case and its receipt chain."""

    try:
        case, receipts = asyncio.run(
            _revocations_show_payload(
                _command_state_store(),
                case_id=case_id,
            )
        )
    except Exception as error:
        _raise_safe_revocation_click_error("revocations show", error)
    if json_output:
        _echo_revocation_json(
            "show",
            case=case,
            receipt_count=len(receipts),
            receipts=receipts,
        )
        return
    click.echo(f"Revocation case {_metadata_value(case, 'case_id')}")
    for name in sorted(case):
        if name != "case_id":
            click.echo(f"  {name}: {_metadata_value(case, name)}")
    click.echo("Receipts:")
    if not receipts:
        click.echo("  (none)")
    for receipt in receipts:
        click.echo(
            f"  {_metadata_value(receipt, 'receipt_id')}  "
            f"attempt={_metadata_value(receipt, 'attempt')}  "
            f"outcome={_metadata_value(receipt, 'observed_outcome')}"
        )


@revocations_group.command("verify")
@click.argument("case_id", type=str)
@click.option(
    "--operator",
    "operator_target",
    type=str,
    default=None,
    help="Explicit MODULE:OBJECT implementing RevocationOperator.",
)
@click.option("--json", "json_output", is_flag=True, help="Emit versioned JSON.")
def revocations_verify(
    case_id: str,
    operator_target: str | None,
    json_output: bool,
) -> None:
    """Request read-only destination verification from a trusted operator."""

    try:
        result, payload = asyncio.run(
            _verify_revocation(
                _command_state_store(),
                case_id=case_id,
                operator_target=_resolve_revocation_operator_target(operator_target),
            )
        )
    except Exception as error:
        _raise_safe_revocation_click_error("revocations verify", error)
    if json_output:
        _echo_revocation_json("verify", result=payload)
    else:
        click.echo(
            f"{result.case_id}: verify -> {result.stage.value}"
            + (
                f" ({result.safe_error_code})"
                if result.safe_error_code is not None
                else ""
            )
        )
        click.echo(
            "Revocation control state was unchanged. Destination read-only "
            "behavior is enforced by the configured operator and its credentials."
        )
    if result.safe_error_code is not None or result.stage.value in {
        "failed",
        "blocked",
    }:
        raise click.exceptions.Exit(1)


@revocations_group.command("retry")
@click.argument("case_id", type=str)
@click.option(
    "--operator",
    "operator_target",
    type=str,
    default=None,
    help="Explicit MODULE:OBJECT implementing RevocationOperator.",
)
@click.option("--json", "json_output", is_flag=True, help="Emit versioned JSON.")
def revocations_retry(
    case_id: str,
    operator_target: str | None,
    json_output: bool,
) -> None:
    """Explicitly retry target mutation and require a new durable receipt attempt."""

    try:
        result, payload = asyncio.run(
            _retry_revocation(
                _command_state_store(),
                case_id=case_id,
                operator_target=_resolve_revocation_operator_target(operator_target),
            )
        )
    except Exception as error:
        _raise_safe_revocation_click_error("revocations retry", error)
    if json_output:
        _echo_revocation_json("retry", result=payload)
    else:
        click.echo(
            f"{result.case_id}: retry attempt {result.attempt} -> {result.stage.value}"
        )
        click.echo(f"Recorded {len(result.receipt_ids)} receipt(s).")
    if result.safe_error_code is not None or result.stage.value in {
        "failed",
        "blocked",
    }:
        raise click.exceptions.Exit(1)


@revocations_group.command("scan")
@click.option("--target", "target_id", type=str, required=True)
@click.option(
    "--operator",
    "operator_target",
    type=str,
    default=None,
    help="Explicit MODULE:OBJECT implementing RevocationOperator.",
)
@click.option("--json", "json_output", is_flag=True, help="Emit versioned JSON.")
def revocations_scan(
    target_id: str,
    operator_target: str | None,
    json_output: bool,
) -> None:
    """Request a read-only drift scan from a trusted target operator."""

    try:
        result, payload = asyncio.run(
            _scan_revocations(
                _command_state_store(),
                target_id=target_id,
                operator_target=_resolve_revocation_operator_target(operator_target),
            )
        )
    except Exception as error:
        _raise_safe_revocation_click_error("revocations scan", error)
    if json_output:
        _echo_revocation_json("scan", result=payload)
    else:
        click.echo(
            f"{result.target_id}: scanned={result.scanned_count} "
            f"matching={result.matching_count} drift={result.drift_count}"
        )
        click.echo(
            "Revocation control state was unchanged. Target read-only behavior "
            "is enforced by the configured operator and its credentials."
        )
    if result.safe_error_code is not None or result.drift_count > 0:
        raise click.exceptions.Exit(1)


@revocations_group.command("repair-ledger")
@click.option("--json", "json_output", is_flag=True, help="Emit versioned JSON.")
def revocations_repair_ledger(json_output: bool) -> None:
    """Rebuild derived ledger summaries from immutable events and receipts."""

    async def _repair() -> Any:
        return await _revocation_repository(_command_state_store()).repair()

    try:
        report = asyncio.run(_repair())
    except Exception as error:
        _raise_safe_revocation_click_error("revocations repair-ledger", error)
    payload = {
        "cases_rebuilt": report.cases_rebuilt,
        "events_validated": report.events_validated,
        "receipt_heads_rebuilt": report.receipt_heads_rebuilt,
    }
    if json_output:
        _echo_revocation_json("repair-ledger", report=payload)
    else:
        click.echo("Revocation ledger repaired.")
        click.echo(f"  cases rebuilt: {report.cases_rebuilt}")
        click.echo(f"  events validated: {report.events_validated}")
        click.echo(f"  receipt heads rebuilt: {report.receipt_heads_rebuilt}")
        click.echo("Serving suppression was not lifted.")


@cli.group("quarantine")
def quarantine_group() -> None:
    """Inspect and manually review quarantined failures."""


@quarantine_group.command("list")
@click.option(
    "--status",
    type=click.Choice(["open", "approved", "rejected"]),
    default=None,
)
@click.option("--json", "json_output", is_flag=True, help="Emit JSON.")
def quarantine_list(status: str | None, json_output: bool) -> None:
    """List local quarantine cases."""

    selected = syn.QuarantineStatus(status) if status is not None else None
    repository = syn.QuarantineRepository(_command_state_store())
    cases = asyncio.run(repository.list(status=selected))
    if json_output:
        click.echo(json.dumps([item.to_dict() for item in cases], indent=2))
        return
    if not cases:
        click.echo("No quarantine cases.")
        return
    for item in cases:
        click.echo(
            f"{item.case_id}  {item.status.value:8}  {item.app_name}  {item.reason}"
        )


@quarantine_group.command("show")
@click.argument("case_id", type=str)
def quarantine_show(case_id: str) -> None:
    """Show one metadata-only quarantine case."""

    repository = syn.QuarantineRepository(_command_state_store())
    try:
        case = asyncio.run(repository.get(case_id))
    except ValueError as error:
        raise click.ClickException(str(error)) from error
    if case is None:
        raise click.ClickException(f"quarantine case not found: {case_id}")
    click.echo(json.dumps(case.to_dict(), indent=2, sort_keys=True))


@quarantine_group.command("review")
@click.argument("case_id", type=str)
@click.option(
    "--decision",
    type=click.Choice(["approve", "reject"]),
    required=True,
)
@click.option(
    "--note",
    type=str,
    default=None,
    help="Optional redacted reviewer note.",
)
def quarantine_review(
    case_id: str,
    decision: str,
    note: str | None,
) -> None:
    """Approve or reject a case without executing pipeline code."""

    repository = syn.QuarantineRepository(_command_state_store())
    status = (
        syn.QuarantineStatus.APPROVED
        if decision == "approve"
        else syn.QuarantineStatus.REJECTED
    )
    try:
        reviewed = asyncio.run(repository.review(case_id, status=status, note=note))
    except (KeyError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"{reviewed.case_id}: {reviewed.status.value}")
    click.echo("No pipeline code was executed.")


@cli.command()
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8765, type=click.IntRange(0, 65535), show_default=True)
@click.option(
    "--allow-remote",
    is_flag=True,
    help="Allow a non-loopback bind. The dashboard has no authentication.",
)
@click.option(
    "--open-browser",
    is_flag=True,
    help="Open the local dashboard URL in the default browser.",
)
def dashboard(
    host: str,
    port: int,
    allow_remote: bool,
    open_browser: bool,
) -> None:
    """Serve the read-only local run dashboard."""

    import webbrowser

    from synor.dashboard import DashboardServer

    try:
        server = DashboardServer(
            host=host,
            port=port,
            store=_command_state_store(),
            allow_remote=allow_remote,
        )
    except (OSError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Synor dashboard: {server.address.url}")
    click.echo("Read-only metadata view. Press Ctrl+C to stop.")
    if open_browser:
        webbrowser.open(server.address.url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        click.echo("\nStopping dashboard.")
    finally:
        server.shutdown()


@cli.command()
@click.argument("app_target", type=str)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    show_default=True,
    default=False,
    help="Skip confirmation prompt.",
)
@click.option(
    "-q",
    "--quiet",
    is_flag=True,
    show_default=True,
    default=False,
    help="Avoid printing anything to the standard output, e.g. statistics.",
)
@click.option(
    "--reset",
    is_flag=True,
    show_default=True,
    default=False,
    help="Drop existing setup before updating (equivalent to running 'synor drop' first).",
)
@click.option(
    "--full-reprocess",
    is_flag=True,
    show_default=True,
    default=False,
    help="Reprocess everything and invalidate existing caches.",
)
@click.option(
    "--live",
    "-L",
    is_flag=True,
    show_default=True,
    default=False,
    help="Run in live mode (live components continue processing after initial update).",
)
@click.option(
    "--preview",
    is_flag=True,
    show_default=True,
    default=False,
    help="Compute target actions without applying them. Prints planned actions.",
)
@click.option(
    "--offline",
    is_flag=True,
    default=False,
    help="Deny guarded Python networking and policy-aware egress.",
)
def update(
    app_target: str,
    force: bool,
    quiet: bool,
    reset: bool,
    full_reprocess: bool,
    live: bool,
    preview: bool,
    offline: bool,
) -> None:
    """
    Run an app in catch-up mode. With --live, run in live mode.

    `APP_TARGET`: `path/to/app.py`, `module`, `path/to/app.py:app_name`, or `module:app_name`.
    """
    if preview and reset:
        raise click.UsageError("--preview and --reset cannot be used together.")
    if preview and live:
        raise click.UsageError("--preview and --live cannot be used together.")

    from synor import audit as syn_audit
    from synor import provenance as syn_provenance

    policy = _command_policy(offline)
    pii_policy = _command_pii_policy()
    state_store = _command_state_store()
    quarantine_repository = syn.QuarantineRepository(state_store)
    app = _load_app_with_policy(app_target, policy)

    async def _do(cancelled: Any) -> None:
        from synor._internal.app import show_progress

        recorder: syn_audit.RunRecorder | None = None
        finished = False
        try:
            with syn.policy_scope(policy), syn.pii_scope(pii_policy):
                env = await app._environment._get_env()
            recorder = syn_audit.RunRecorder.start(
                command="preview" if preview else "update",
                app_name=app._name,
                app_target=app_target,
                environment=env.name,
                db_path=env.settings.db_path,
                policy={
                    "egress": policy.to_dict(),
                    "pii": pii_policy.to_dict(),
                },
                options={
                    "full_reprocess": full_reprocess,
                    "live": live,
                    "preview": preview,
                    "reset": reset,
                },
            )
            if not quiet:
                print(
                    f"Running app '{app._name}' from environment '{env.name}' (db path: {env.settings.db_path})"
                )

            with (
                syn.policy_scope(policy, audit_sink=recorder.record_policy_decision),
                syn.pii_scope(pii_policy),
            ):
                if preview:
                    handle = app.update(
                        full_reprocess=full_reprocess,
                        preview=True,
                    )
                    actions: list[Any] = await handle.result()
                    click.echo("Preview: planned target actions")
                    if actions:
                        for action in actions:
                            safe_action = syn.enforce_pii(
                                action,
                                policy=pii_policy,
                            )
                            display_action = syn_audit.redact_metadata(safe_action)
                            if (
                                isinstance(action, tuple)
                                and not hasattr(action, "_asdict")
                                and isinstance(display_action, list)
                            ):
                                display_action = tuple(display_action)
                            click.echo(f"  {display_action!r}")
                    else:
                        click.echo("  No target actions planned.")
                    recorder.finish(
                        status="succeeded",
                        action_count=len(actions),
                        stats=handle.stats(),
                    )
                    await state_store.put(
                        f"runs/{recorder.run_id}/manifest.json",
                        recorder.manifest_path.read_bytes(),
                    )
                    finished = True
                    return

                if pii_policy.action in {
                    syn.PIIAction.DENY,
                    syn.PIIAction.QUARANTINE,
                }:
                    pii_preview = app.update(
                        full_reprocess=full_reprocess,
                        preview=True,
                    )
                    pii_actions: list[Any] = await pii_preview.result()
                    for action in pii_actions:
                        syn.enforce_pii(action, policy=pii_policy)
                    recorder.record(
                        "pii_preflight_passed",
                        action_count=len(pii_actions),
                    )

                # --reset: drop existing state first (equivalent to `synor drop ...`)
                if reset:
                    if not force:
                        if not _confirm_yes(
                            f"Type 'yes' to reset app '{app._name}' (drop existing state)"
                        ):
                            if not quiet:
                                click.echo("Update operation aborted.")
                            recorder.finish(status="cancelled")
                            finished = True
                            return

                    persisted_names = _get_persisted_app_names(env)
                    if app._name in persisted_names:
                        await app.drop()

                handle = app.update(
                    full_reprocess=full_reprocess,
                    live=live,
                )
                if not quiet:
                    await show_progress(handle)
                else:
                    await handle.result()
                provenance = await syn_provenance.capture_artifact_provenance(
                    app,
                    run_id=recorder.run_id,
                    app_target=app_target,
                )
                syn_provenance.write_artifact_provenance(
                    recorder.run_dir,
                    provenance,
                )
                await syn_provenance.store_artifact_provenance(
                    state_store,
                    provenance,
                )
                recorder.finish(
                    status="succeeded",
                    artifact_count=len(provenance),
                    stats=handle.stats(),
                )
                await state_store.put(
                    f"runs/{recorder.run_id}/manifest.json",
                    recorder.manifest_path.read_bytes(),
                )
                finished = True
        except BaseException as error:
            if recorder is not None and not finished:
                recorder.finish(status="failed", error=error)
                finished = True
                try:
                    await state_store.put(
                        f"runs/{recorder.run_id}/manifest.json",
                        recorder.manifest_path.read_bytes(),
                    )
                    await quarantine_repository.create(
                        reason=(
                            "pii_policy"
                            if isinstance(error, syn.PIIViolation)
                            else "run_failure"
                        ),
                        app_name=app._name,
                        app_target=app_target,
                        run_id=recorder.run_id,
                        error=error,
                    )
                except Exception as control_error:
                    recorder.record(
                        "control_state_failure",
                        error_type=(
                            f"{type(control_error).__module__}."
                            f"{type(control_error).__qualname__}"
                        ),
                    )
            raise
        finally:
            await _stop_all_environments()

    _run_async_cmd(_do, quiet=quiet)


@cli.command()
@click.argument("app_target", type=str)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Skip confirmation prompt.",
)
@click.option(
    "-q",
    "--quiet",
    is_flag=True,
    show_default=True,
    default=False,
    help="Avoid printing anything to the standard output, e.g. statistics.",
)
@click.option(
    "--offline",
    is_flag=True,
    default=False,
    help="Deny guarded Python networking and policy-aware egress.",
)
def drop(
    app_target: str,
    force: bool = False,
    quiet: bool = False,
    offline: bool = False,
) -> None:
    """
    Drop an app and all its target states.

    This will:

    \b
    - Revert all target states created by the app (e.g., drop tables, delete rows)
    - Clear the app's internal state database

    `APP_TARGET`: `path/to/app.py`, `module`, `path/to/app.py:app_name`, or `module:app_name`.
    """
    from synor import audit as syn_audit

    policy = _command_policy(offline)
    app = _load_app_with_policy(app_target, policy)

    async def _do(cancelled: Any) -> None:
        recorder: syn_audit.RunRecorder | None = None
        finished = False
        try:
            with syn.policy_scope(policy):
                env = await app._environment._get_env()
            recorder = syn_audit.RunRecorder.start(
                command="drop",
                app_name=app._name,
                app_target=app_target,
                environment=env.name,
                db_path=env.settings.db_path,
                policy=policy.to_dict(),
                options={"force": force},
            )
            persisted_names = _get_persisted_app_names(env)

            if not quiet:
                click.echo(
                    f"Preparing to drop app '{app._name}' from environment '{env.name}' (db path: {env.settings.db_path})"
                )

            if app._name not in persisted_names:
                if not quiet:
                    click.echo(
                        f"App '{app._name}' has no persisted state. Nothing to drop."
                    )
                recorder.finish(status="succeeded", action_count=0)
                finished = True
                return

            if not force:
                if not _confirm_yes(
                    f"Type 'yes' to drop app '{app._name}' and all its target states"
                ):
                    if not quiet:
                        click.echo("Drop operation aborted.")
                    recorder.finish(status="cancelled")
                    finished = True
                    return

            with syn.policy_scope(policy, audit_sink=recorder.record_policy_decision):
                await app.drop()
            recorder.finish(status="succeeded")
            finished = True
            if not quiet:
                click.echo(
                    f"Dropped app '{app._name}' from environment '{env.name}' and reverted its target states."
                )
        except BaseException as error:
            if recorder is not None and not finished:
                recorder.finish(status="failed", error=error)
                finished = True
            raise
        finally:
            await _stop_all_environments()

    _run_async_cmd(_do, quiet=quiet)


@cli.command()
@click.argument("project_name", type=str, required=False)
@click.option(
    "--dir",
    type=click.Path(file_okay=False, dir_okay=True, writable=True),
    default=None,
    help="Directory to create the project in.",
)
def init(project_name: str | None, dir: str | None) -> None:
    """
    Initialize a new Synor project.

    Creates a new project directory with starter files:
    1. main.py (Main application file)
    2. pyproject.toml (Project metadata and dependencies)
    3. README.md (Quick start guide)

    `PROJECT_NAME`: Name of the project (defaults to current directory name if not specified).
    """
    # Determine project directory
    if dir:
        project_dir = dir
        if not project_name:
            project_name = pathlib.Path(dir).resolve().name
    elif project_name:
        project_dir = project_name
    else:
        # Use current directory
        project_dir = "."
        project_name = pathlib.Path.cwd().resolve().name

    # Validate project name
    if project_name and not project_name.replace("_", "").replace("-", "").isalnum():
        raise click.BadParameter(
            f"Invalid project name '{project_name}'. "
            "Project name must contain only alphanumeric characters, hyphens, and underscores.",
            param_hint="PROJECT_NAME",
        )

    project_path = pathlib.Path(project_dir)

    # Check if directory exists and has files
    if project_path.exists() and any(project_path.iterdir()):
        if not click.confirm(
            f"Directory '{project_dir}' already exists and is not empty. "
            "Continue and overwrite existing files?"
        ):
            click.echo("Init cancelled.")
            return

    try:
        _create_project_files(project_name, project_dir)
        click.echo(f"Created Synor project '{project_name}' in '{project_dir}'")
        click.echo("\nNext steps:")
        if project_dir != ".":
            click.echo(f"  1. cd {project_dir}")
            click.echo("  2. uv run synor update main.py")
        else:
            click.echo("  1. uv run synor update main.py")
    except Exception as e:
        raise click.ClickException(f"Failed to create project: {e}") from e


if __name__ == "__main__":
    cli()
