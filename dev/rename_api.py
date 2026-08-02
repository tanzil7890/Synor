"""
Tier-C public API codemod.

Renames the distinctive Synor vocabulary via LibCST so that only genuine
references are rewritten. A regex pass is unsafe here: `python/` contained 175
bare `fn` identifiers used as ordinary parameter and variable names against
809 real `syn.fn` attribute accesses.

Scope of this script (the safe, mechanical part):

  * attribute access on a module alias -- `syn.mount` -> `syn.spawn`
  * keyword arguments on renamed decorators -- `@syn.task(memo=True)` -> `cache=True`
  * connector target verbs -- `table.declare_row` -> `table.ensure_row`

Deliberately NOT handled here; see `--report` and do these by hand:

  * definition sites in `python/synor/_internal/` and their `__all__` entries
  * `python/synor/_internal/core.pyi` (806 lines of stubs)
  * `#[pyo3(name = "...")]` attributes in `rust/py/src/`
  * `.md` / `.mdx` prose and fenced code blocks

Usage:
    uv run python dev/rename_api.py --report          # what would change, no writes
    uv run python dev/rename_api.py --diff path.py    # unified diff for one file
    uv run python dev/rename_api.py --apply           # rewrite in place
"""

from __future__ import annotations

import argparse
import difflib
import pathlib
import re
import sys
from typing import Iterator

import libcst as cst

# --- rename table -------------------------------------------------------------
# Attribute renames on the synor module alias (syn.X -> syn.Y).
MODULE_ATTRS: dict[str, str] = {
    "fn": "task",
    "mount": "spawn",
    "mount_each": "spawn_each",
    "use_mount": "call",
    "mount_target": "attach_target",
    "component_subpath": "unit_path",
    "ComponentSubpath": "UnitPath",
    "ComponentMountHandle": "SpawnHandle",
    # `TargetState` is deliberately NOT renamed. "Target" is the connector
    # vocabulary throughout (TableTarget, DirTarget, attach_target), so renaming
    # the state type while keeping the target types would split one concept
    # across two names. The declare_* -> ensure_* verbs below still apply.
    "declare_target_state": "ensure_target_state",
    "declare_target_state_with_child": "ensure_target_state_with_child",
    "NON_EXISTENCE": "ABSENT",
    "NonExistenceType": "AbsentType",
    "is_non_existence": "is_absent",
}

# Verbs called on connector target objects (table.declare_row -> table.ensure_row).
# Matched on the attribute name alone; these names are distinctive enough that a
# false positive is implausible, but `--report` lists every hit for review.
TARGET_VERBS: dict[str, str] = {
    "declare_row": "ensure_row",
    "declare_file": "ensure_file",
    "declare_dir_target": "ensure_dir_target",
    "declare_table_target": "ensure_table_target",
}

# Keyword arguments, rewritten only inside calls to a renamed decorator.
DECORATOR_KWARGS: dict[str, str] = {"memo": "cache"}

# Attribute names that, when called, may carry DECORATOR_KWARGS.
DECORATOR_ATTRS = {"fn", "task"}

# Symbols renamed ONLY as `syn.<name>` attribute access, never as bare names.
# `fn` is a common local/parameter identifier: `python/synor` has 175 bare `fn`
# uses that are ordinary parameters (e.g. `spawn_each(fn, ...)` in api.py) against
# 809 real `syn.task` references. Bare-name renaming here would rewrite the
# parameter and shadow the decorator. Its definition sites are edited by hand.
ATTRIBUTE_ONLY = {"fn"}

SKIP_DIRS = {".git", ".venv", "target", "node_modules", "__pycache__", "dist"}

# --- docs pass ----------------------------------------------------------------
# `.mdx` is not Python, so LibCST cannot parse it. Docs are rewritten with
# regexes deliberately scoped to two contexts where a match is unambiguous:
# fenced ```python blocks, and inline `backtick` spans. Narrative prose is
# reported but never rewritten -- "memoization" -> "caching" is a wording
# decision, not a mechanical rename.
_FENCE = re.compile(r"(```(?:python|py)\n)(.*?)(```)", re.DOTALL)
_INLINE = re.compile(r"`([^`\n]+)`")

# Names distinctive enough to rewrite anywhere in a document, including plain
# prose outside code fences. Restricting the pass to fences and backticks left
# 232 stale bare names in narrative text ("use mount_each to ..."), which is
# broken documentation. Deliberately EXCLUDED: `fn`, `mount`, `map`, `memo` on
# their own -- they are ordinary English or too short to disambiguate, so they
# stay fence/backtick-scoped and get a manual review pass.
_PROSE_SAFE = {
    "mount_each",
    "use_mount",
    "mount_target",
    "component_subpath",
    "ComponentSubpath",
    "ComponentMountHandle",
    "declare_target_state",
    "declare_target_state_with_child",
    "NON_EXISTENCE",
    "NonExistenceType",
    "is_non_existence",
}


def _prose_substitutions(text: str) -> tuple[str, int]:
    """Substitutions safe to apply to a whole document, prose included."""
    hits = 0
    # `syn.X` / `synor.X` is unambiguous wherever it appears.
    for old, new in MODULE_ATTRS.items():
        text, n = re.subn(rf"\b(_?syn|synor)\.{re.escape(old)}\b", rf"\1.{new}", text)
        hits += n
    # Distinctive bare names, plus every target verb (all are `declare_*`).
    for old, new in {**MODULE_ATTRS, **TARGET_VERBS}.items():
        if old not in _PROSE_SAFE and old not in TARGET_VERBS:
            continue
        text, n = re.subn(rf"\b{re.escape(old)}\b", new, text)
        hits += n
    for old, new in DECORATOR_KWARGS.items():
        text, n = re.subn(rf"\b{re.escape(old)}=", f"{new}=", text)
        hits += n
    return text, hits


def _docs_substitutions(text: str) -> tuple[str, int]:
    """Apply the rename table to one chunk of code-ish text."""
    hits = 0
    for old, new in MODULE_ATTRS.items():
        pattern = re.compile(rf"\b(_?syn|synor)\.{re.escape(old)}\b")
        text, n = pattern.subn(rf"\1.{new}", text)
        hits += n
    for old, new in TARGET_VERBS.items():
        text, n = re.subn(rf"\b{re.escape(old)}\b", new, text)
        hits += n
    for old, new in DECORATOR_KWARGS.items():
        text, n = re.subn(rf"\b{re.escape(old)}=", f"{new}=", text)
        hits += n
    return text, hits


def rewrite_doc(source: str) -> tuple[str, int]:
    # Document-wide pass first (prose included), then the fence/backtick-scoped
    # pass picks up the ambiguous names that are only safe inside code.
    source, total = _prose_substitutions(source)

    def fence_sub(m: "re.Match[str]") -> str:
        nonlocal total
        body, n = _docs_substitutions(m.group(2))
        total += n
        return m.group(1) + body + m.group(3)

    out = _FENCE.sub(fence_sub, source)

    def inline_sub(m: "re.Match[str]") -> str:
        nonlocal total
        body, n = _docs_substitutions(m.group(1))
        total += n
        return f"`{body}`"

    return _INLINE.sub(inline_sub, out), total


def doc_files(root: pathlib.Path) -> Iterator[pathlib.Path]:
    for pattern in ("*.md", "*.mdx"):
        for path in root.rglob(pattern):
            if not any(part in SKIP_DIRS for part in path.parts):
                yield path


def _module_aliases(tree: cst.Module) -> set[str]:
    """Names bound to the synor package in this module (`import synor as syn`)."""
    aliases: set[str] = set()

    class Visitor(cst.CSTVisitor):
        def visit_Import(self, node: cst.Import) -> None:
            for item in node.names:
                name = item.evaluated_name
                if name == "synor" or name.startswith("synor."):
                    aliases.add(item.evaluated_alias or name.split(".")[0])

    tree.visit(Visitor())
    return aliases


def _is_synor_import(node: cst.ImportFrom) -> bool:
    """True for `from . import x` and `from synor... import x`."""
    if node.relative:
        return True
    return isinstance(node.module, (cst.Name, cst.Attribute)) and cst.Module(
        []
    ).code_for_node(node.module).startswith("synor")


def _imported_names(tree: cst.Module) -> set[str]:
    """
    Renameable names pulled directly into module scope by a `from ... import X`.

    Only relative imports and imports rooted at `synor` are considered, so a
    same-named symbol from a third-party package is never touched. A local
    variable shadowing one of these would be renamed too; that is accepted
    because mypy and the test suite gate every stage.
    """
    found: set[str] = set()

    class Visitor(cst.CSTVisitor):
        def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
            if isinstance(node.names, cst.ImportStar):
                return
            if not _is_synor_import(node):
                return
            for item in node.names:
                name = item.evaluated_name
                # An `as` alias rebinds the symbol; the local name is the user's
                # choice, so leave it alone.
                if (
                    item.evaluated_alias is None
                    and name not in ATTRIBUTE_ONLY
                    and (name in MODULE_ATTRS or name in TARGET_VERBS)
                ):
                    found.add(name)

    tree.visit(Visitor())
    return found


class Renamer(cst.CSTTransformer):
    def __init__(self, aliases: set[str], imported: set[str] | None = None) -> None:
        self.aliases = aliases
        self.imported = imported or set()
        self.hits: list[str] = []
        # Name nodes sitting in the `.attr` slot of an Attribute. LibCST visits
        # children before parents, so without this guard `leave_Name` would fire
        # on the `fn` in an unrelated `other_obj.fn` and rewrite it.
        self._attr_names: set[int] = set()
        self._in_synor_import = False

    def visit_Attribute(self, node: cst.Attribute) -> None:
        self._attr_names.add(id(node.attr))

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        self._in_synor_import = _is_synor_import(node)

    def leave_ImportFrom(
        self, original: cst.ImportFrom, updated: cst.ImportFrom
    ) -> cst.ImportFrom:
        self._in_synor_import = False
        return updated

    def leave_ImportAlias(
        self, original: cst.ImportAlias, updated: cst.ImportAlias
    ) -> cst.ImportAlias:
        """
        Rename the *source* name in `from synor... import Old as _local`.

        `_imported_names` deliberately ignores aliased imports so the local
        binding keeps the name its author chose -- but the symbol being pulled
        out of the package still has to track the rename, or the import breaks.
        Gated on `_in_synor_import` so `from elsewhere import fn as _fn` is safe.
        """
        if not self._in_synor_import:
            return updated
        if updated.asname is None or not isinstance(updated.name, cst.Name):
            return updated
        new = MODULE_ATTRS.get(updated.name.value) or TARGET_VERBS.get(
            updated.name.value
        )
        if new is None:
            return updated
        self.hits.append(f"{updated.name.value} -> {new} (aliased import)")
        return updated.with_changes(name=cst.Name(new))

    def leave_Name(self, original: cst.Name, updated: cst.Name) -> cst.BaseExpression:
        # Bare references to symbols imported directly from the synor package.
        # Attribute access (`syn.task`) is handled in leave_Attribute instead.
        if id(original) in self._attr_names:
            return updated
        if updated.value in self.imported:
            new = MODULE_ATTRS.get(updated.value) or TARGET_VERBS[updated.value]
            self.hits.append(f"{updated.value} -> {new} (bare)")
            return updated.with_changes(value=new)
        return updated

    def leave_Attribute(
        self, original: cst.Attribute, updated: cst.Attribute
    ) -> cst.BaseExpression:
        attr = updated.attr.value

        # syn.<name> -- only when the receiver is a known module alias.
        if isinstance(updated.value, cst.Name) and updated.value.value in self.aliases:
            new = MODULE_ATTRS.get(attr)
            if new is not None:
                self.hits.append(f"{updated.value.value}.{attr} -> {new}")
                return updated.with_changes(attr=cst.Name(new))
            return updated

        # <anything>.declare_row -- connector target verbs.
        new = TARGET_VERBS.get(attr)
        if new is not None:
            self.hits.append(f".{attr} -> {new}")
            return updated.with_changes(attr=cst.Name(new))
        return updated

    def _is_decorator_call(self, func: cst.BaseExpression) -> bool:
        """
        True for `syn.task(...)` and for chained forms like
        `syn.task.as_async(...)`, which takes the same kwargs. Without the
        chained case, 19 live `@syn.task.as_async(memo=True)` sites are missed.
        """
        if not isinstance(func, cst.Attribute):
            return False
        # syn.task(...)
        if isinstance(func.value, cst.Name):
            return (
                func.value.value in self.aliases
                and func.attr.value in DECORATOR_ATTRS
            )
        # syn.task.as_async(...)
        inner = func.value
        return (
            isinstance(inner, cst.Attribute)
            and isinstance(inner.value, cst.Name)
            and inner.value.value in self.aliases
            and inner.attr.value in DECORATOR_ATTRS
        )

    def leave_Call(self, original: cst.Call, updated: cst.Call) -> cst.BaseExpression:
        if not self._is_decorator_call(updated.func):
            return updated

        args = []
        changed = False
        for arg in updated.args:
            if arg.keyword is not None:
                new_kw = DECORATOR_KWARGS.get(arg.keyword.value)
                if new_kw is not None:
                    self.hits.append(f"{arg.keyword.value}= -> {new_kw}=")
                    args.append(arg.with_changes(keyword=cst.Name(new_kw)))
                    changed = True
                    continue
            args.append(arg)
        return updated.with_changes(args=args) if changed else updated


def rewrite(source: str, defs: bool = False) -> tuple[str, list[str]]:
    """
    Rewrite one module.

    With `defs=True`, every symbol in the rename table is treated as a bare
    name in scope, so `class Foo` / `def foo` / plain references are rewritten
    too. This is how definition sites get renamed. It is safe to combine with
    the Rust-backed names because `leave_Name` skips anything in the `.attr`
    slot of an Attribute -- so `core.SpawnHandle` (the Rust class) is
    left alone while the Python wrapper of the same name is renamed.
    """
    tree = cst.parse_module(source)
    imported = _imported_names(tree)
    if defs:
        # TARGET_VERBS too: `def declare_row` is a bare name at its definition
        # site even though every call site reaches it as an attribute.
        # ATTRIBUTE_ONLY stays excluded even here -- see its docstring.
        imported = (
            imported | set(MODULE_ATTRS) | set(TARGET_VERBS)
        ) - ATTRIBUTE_ONLY
    renamer = Renamer(_module_aliases(tree), imported)
    return tree.visit(renamer).code, renamer.hits


def py_files(root: pathlib.Path) -> Iterator[pathlib.Path]:
    for path in root.rglob("*.py"):
        if not any(part in SKIP_DIRS for part in path.parts):
            yield path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=".", type=pathlib.Path)
    ap.add_argument("--report", action="store_true", help="summarise, write nothing")
    ap.add_argument("--diff", type=pathlib.Path, help="unified diff for one file")
    ap.add_argument("--apply", action="store_true", help="rewrite files in place")
    ap.add_argument(
        "--docs",
        action="store_true",
        help="operate on .md/.mdx (fenced code blocks and inline spans) instead of .py",
    )
    ap.add_argument(
        "--defs",
        action="store_true",
        help=(
            "also rename definition sites (class/def names and bare references) "
            "in the files processed. Point this at the defining module only, "
            "e.g. --defs --diff python/synor/_internal/api.py"
        ),
    )
    ap.add_argument(
        "--only",
        help=(
            "comma-separated symbols to rename this pass, e.g. "
            "'ABSENT,AbsentType,is_absent'. Enables staging: "
            "one symbol group per commit, tests green at each."
        ),
    )
    args = ap.parse_args()

    if args.only:
        keep = {s.strip() for s in args.only.split(",") if s.strip()}
        unknown = keep - set(MODULE_ATTRS) - set(TARGET_VERBS) - set(DECORATOR_KWARGS)
        if unknown:
            ap.error(f"unknown symbol(s): {', '.join(sorted(unknown))}")
        for table in (MODULE_ATTRS, TARGET_VERBS, DECORATOR_KWARGS):
            for name in list(table):
                if name not in keep:
                    del table[name]

    if not (args.report or args.diff or args.apply):
        ap.error("pick one of --report, --diff, --apply")

    if args.diff:
        targets = [args.diff]
    elif args.root.is_file():
        # A single-file root is how `--defs` gets scoped to the defining module
        # without renaming bare names across the whole tree.
        targets = [args.root]
    elif args.docs:
        targets = sorted(doc_files(args.root))
    else:
        targets = sorted(py_files(args.root))
    total_files = 0
    total_hits = 0

    for path in targets:
        source = path.read_text(encoding="utf-8")
        if args.docs or path.suffix in {".md", ".mdx"}:
            new_source, n = rewrite_doc(source)
            hits = [""] * n
        else:
            try:
                new_source, hits = rewrite(source, defs=args.defs)
            except cst.ParserSyntaxError as exc:
                print(f"SKIP (unparseable): {path}: {exc}", file=sys.stderr)
                continue
        if not hits:
            continue

        total_files += 1
        total_hits += len(hits)

        if args.diff:
            sys.stdout.writelines(
                difflib.unified_diff(
                    source.splitlines(keepends=True),
                    new_source.splitlines(keepends=True),
                    fromfile=str(path),
                    tofile=f"{path} (renamed)",
                )
            )
        elif args.report:
            print(f"{path}: {len(hits)}")
        elif args.apply:
            path.write_text(new_source, encoding="utf-8")

    verb = "would change" if (args.report or args.diff) else "changed"
    print(f"\n{verb}: {total_files} files, {total_hits} references", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
