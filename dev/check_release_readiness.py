#!/usr/bin/env python3
"""Fail closed when legal/provenance release requirements are incomplete."""

from __future__ import annotations

import pathlib
import re
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
TEXT_SUFFIXES = {
    ".html",
    ".json",
    ".md",
    ".mdx",
    ".mjs",
    ".py",
    ".rs",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".yaml",
    ".yml",
}
SKIP_PARTS = {
    ".astro",
    ".git",
    ".venv",
    "dist",
    "node_modules",
    "site-packages",
    "target",
}
SELF_DOCUMENTING = {
    ROOT / "BRAND_CLEARANCE.md",
    ROOT / "CHANGES_FROM_UPSTREAM.md",
    ROOT / "ORIGIN.md",
    ROOT / "URL_AUDIT.md",
    pathlib.Path(__file__).resolve(),
}
FORBIDDEN = {
    "placeholder version": re.compile(r"\b999\.0\.0\b"),
    "production classifier": re.compile(r"Production/Stable"),
    "copied docs brand package": re.compile(r"@synor/brand"),
    "Scarf tracking": re.compile(r"(?:static|gateway)\.scarf\.sh|SCARF_PIXEL"),
    "unowned analytics SDK": re.compile(r"\b(?:mixpanel|posthog)\b", re.I),
    "upstream Discord identifiers": re.compile(r"zpA9S2DR7s|1314801574169673738"),
    "upstream Trendshift identifier": re.compile(r"trendshift|13939", re.I),
    "unverified project URL": re.compile(
        r"https?://(?:www\.)?synor\.io|github\.com/synor-io"
    ),
    "unverified project email": re.compile(
        r"(?:synor\.io@gmail|[A-Za-z0-9._%+-]+@synor\.io)"
    ),
}


def iter_text_files() -> list[pathlib.Path]:
    result: list[pathlib.Path] = []
    for path in ROOT.rglob("*"):
        if not path.is_file() or path in SELF_DOCUMENTING:
            continue
        if SKIP_PARTS.intersection(path.relative_to(ROOT).parts):
            continue
        if path.suffix.lower() in TEXT_SUFFIXES or path.name in {"NOTICE"}:
            result.append(path)
    return result


def main() -> int:
    errors: list[str] = []
    for required in (
        "LICENSE",
        "NOTICE",
        "ORIGIN.md",
        "CHANGES_FROM_UPSTREAM.md",
        "BRAND_CLEARANCE.md",
        "URL_AUDIT.md",
        "THIRD_PARTY_NOTICES.html",
    ):
        path = ROOT / required
        if not path.is_file() or path.stat().st_size == 0:
            errors.append(f"missing or empty required file: {required}")

    if (ROOT / "docs" / "brand").exists():
        errors.append("proprietary docs/brand package is present")

    docs_runner = (ROOT / "docs" / "scripts" / "run-astro.mjs").read_text()
    if "ASTRO_TELEMETRY_DISABLED: '1'" not in docs_runner:
        errors.append("Astro telemetry is not explicitly disabled")

    clearance = (ROOT / "BRAND_CLEARANCE.md").read_text()
    if "Status: **APPROVED**" not in clearance:
        errors.append("name/trademark clearance is not approved")

    origin = (ROOT / "ORIGIN.md").read_text()
    if "Upstream immutable commit: **PENDING**" in origin:
        errors.append("ORIGIN.md does not record an immutable upstream commit hash")

    notices = (ROOT / "THIRD_PARTY_NOTICES.html").read_text()
    if (
        "Synor third-party notices" not in notices
        or "<li><a href=" not in notices
        or "UNKNOWN" in notices.upper()
    ):
        errors.append("THIRD_PARTY_NOTICES.html is not a verified cargo-about report")

    for path in iter_text_files():
        try:
            content = path.read_text()
        except UnicodeDecodeError:
            continue
        for label, pattern in FORBIDDEN.items():
            if pattern.search(content):
                errors.append(f"{path.relative_to(ROOT)}: {label}")

    if errors:
        print("Release blocked:")
        for error in sorted(set(errors)):
            print(f"- {error}")
        return 1

    print("Release provenance and brand checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
