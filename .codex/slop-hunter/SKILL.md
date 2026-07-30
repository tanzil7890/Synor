---
name: slop-hunter
description: Find dead code, unused APIs, temporary debug scaffolding, and unnecessary complexity in a branch or working-tree diff. Use when the user explicitly asks for a dead-code, cleanup, AI-slop, or final pre-PR pass. Review and report by default; remove code only when the user asks for cleanup.
---

# Slop Hunter

Find removable code from evidence, not aesthetics. Unfamiliar, verbose, or abstract code is not dead merely because it looks suspicious.

## Establish scope

1. Read `AGENTS.md`.
2. Prefer the current PR base from `gh pr view --json baseRefName`.
3. Otherwise use the remote default branch, then `main` as the final fallback.
4. Review the branch diff, staged and unstaged changes, and relevant untracked files.
5. If Git metadata is unavailable, use the user-provided files or diff and state that limitation.

Do not hard-code a branch such as `stg`, and do not fetch or change remote state unless the task requires current remote context.

## Hunt protocol

Inspect changed code for:

- definitions with no callers, imports, registrations, serialization use, reflection use, or public export
- obsolete compatibility branches, duplicate helpers, or wrappers that no longer add behavior
- debug panels, playground routes, mock data, temporary feature bypasses, `debugger`, noisy logging, and commented-out experiments
- dependencies used only by removed or development-only code
- unnecessary configuration knobs, callbacks, abstractions, or classes added without a concrete use
- tests and documentation that cover code which should be removed rather than preserved

Use `ccc search` for conceptual usage when available and `rg` for exact symbols. Fall back to `rg` and directory inspection when `ccc` is unavailable. Check dynamic registration, string-based lookup, macros, PyO3 exposure, type stubs, tests, examples, and docs before declaring anything dead.

## Evidence standard

For every finding, report:

1. the exact symbol or file
2. where it is defined
3. the searches performed
4. why dynamic or external use is unlikely
5. the smallest safe deletion or simplification
6. tests or checks needed after removal

Label uncertainty. Do not recommend deleting a public API solely because this repository has no internal caller.

## Changes

Treat the first pass as review-only. If the user asked to remove the findings, make focused edits, preserve unrelated worktree changes, and run the checks required by `AGENTS.md`.
