---
name: pr-review
description: Review a pull request or branch diff for correctness, regressions, API compatibility, concurrency, security, production readiness, and test quality. Use when the user asks to review a PR, check changes, assess merge readiness, or run a pre-merge review. This is review-only unless the user separately asks for fixes.
---

# Pull Request Review

Find actionable defects from the actual diff and repository contracts. Do not edit code, post comments, resolve threads, or change PR state during a review unless the user explicitly requests those actions.

## Establish context

1. Read `AGENTS.md` and any closer instructions.
2. If a PR exists, read its title, body, base branch, head branch, changed files, checks, and existing review context through the connected GitHub capability or `gh`.
3. Otherwise identify the remote default branch and review the current branch or working-tree diff.
4. Never hard-code another repository’s base branch.
5. Inspect untracked files when they are part of the requested change.
6. If Git metadata or the referenced PR is unavailable, state the limitation and review the artifacts that are available.

Read the complete changed functions and their relevant callers, not only isolated diff hunks. Use `ccc search` for conceptual relationships and `rg` for exact symbols.

## Review dimensions

Prioritize defects that could change behavior:

### Correctness

- wrong state transitions, cleanup, ownership, or change-detection behavior
- races, cancellation bugs, deadlocks, partial writes, retry mistakes, and non-idempotent effects
- error paths that lose information or leave inconsistent state
- boundary conversion errors between Python and Rust

### Synor contracts

- declarative target states remain the source of truth
- component paths remain stable across runs
- missing components clean up owned target states correctly
- async APIs do not block event loops
- LMDB writes use `Storage::run_txn`
- PyO3 changes update `core.pyi`
- user-facing modules preserve export and underscore conventions
- public API additions are necessary and minimal

### Compatibility and security

- public API, serialized state, schema, and migration compatibility
- path traversal, unsafe deserialization, command injection, secret leakage, and untrusted input handling
- resource lifecycle, connection cleanup, and platform behavior

### Tests

Ask: how confident should a reviewer be that the change works and does not regress existing behavior?

Check whether tests cover the user-facing path, failure modes, concurrency, cleanup, and compatibility. Prefer end-to-end tests through public APIs, with focused unit tests for dense internal edge cases.

## Validation

Run checks proportionate to the diff when the environment supports them:

- Python: `uv run ruff format --check .`, `uv run ruff check .`, `uv run mypy`, and `uv run pytest python/`
- Rust: `cargo fmt --check`, `uv run maturin develop`, and `cargo test`
- Before a merge-ready conclusion: `prek run --all-files` when dependencies such as `protoc` are available

Do not claim a check passed unless it ran successfully. Distinguish code failures from missing tools, unavailable services, or sandbox restrictions.

## Findings

Report only actionable findings introduced by the change. Order them by severity:

- **Critical:** data loss, security boundary failure, broad breakage
- **High:** likely user-visible failure or major regression
- **Medium:** real defect with narrower impact
- **Low:** maintainability issue likely to cause future errors

Each finding must include:

1. concise title
2. exact file and line
3. triggering scenario
4. concrete impact
5. smallest credible fix direction

Avoid style-only comments already enforced by formatters. Do not inflate hypothetical concerns without a reachable failure path.

## Output

Lead with findings. Then state:

- checks run and their results
- test-confidence level
- unresolved risks or environment limitations
- whether the PR appears ready, needs work, or is blocked

If no actionable findings exist, say so explicitly and still report validation gaps.
