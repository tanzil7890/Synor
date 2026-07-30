from __future__ import annotations

import sys
from ._internal import core as _core
from ._version import CORE_VERSION as _CORE_VERSION


def _sanity_check_engine() -> None:
    engine_file = getattr(_core, "__file__", "<unknown>")
    engine_version = getattr(_core, "__version__", None)

    problems: list[str] = []

    # Version mismatch (if the engine exposes its own version)
    if engine_version is not None and engine_version != _CORE_VERSION:
        problems.append(
            f"Version mismatch: Python package expects core version {_CORE_VERSION!r}, "
            f"but synor._internal.core reports {engine_version!r}."
        )

    if problems:
        # Helpful diagnostic message for users
        msg_lines = [
            "Inconsistent synor installation detected:",
            *[f"  - {p}" for p in problems],
            "",
            f"Python executable: {sys.executable}",
            f"synor package file: {__file__}",
            f"synor._engine file: {engine_file}",
            "",
            "This usually happens when:",
            "  * An old 'synor._engine' .pyd is still present in the",
            "    package directory, or",
            "  * Multiple 'synor' copies exist on sys.path",
            "    (e.g. a local checkout + an installed wheel).",
            "",
            "Suggested fix:",
            "  1. Uninstall synor completely:",
            "       pip uninstall synor",
            "  2. Reinstall it cleanly:",
            "       pip install --no-cache-dir synor",
            "  3. Ensure there is no local 'synor' directory or old",
            "     .pyd shadowing the installed package.",
        ]
        raise RuntimeError("\n".join(msg_lines))


_sanity_check_engine()
del _sanity_check_engine
