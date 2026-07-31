"""Shared parsing for Synor application targets."""

from __future__ import annotations

import ntpath
import os


def split_app_target(specifier: str) -> tuple[str, str | None]:
    """Split ``module_or_path[:app_selection]`` without splitting a drive."""

    if os.path.isfile(specifier):
        return specifier, None

    drive, remainder = ntpath.splitdrive(specifier)
    module_part, separator, app_selection = remainder.rpartition(":")
    if not separator:
        return drive + remainder, None
    return drive + module_part, app_selection
