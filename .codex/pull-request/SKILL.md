---
name: pull-request
description: Publish the current scoped changes as a GitHub pull request by validating the diff, committing intentionally when needed, pushing the branch, and creating or updating a clear PR. Use when the user asks to create, open, publish, or update a pull request for this repository.
---

# Create or Update a Pull Request

Opening a PR authorizes the normal in-scope Git operations required to publish it. Preserve unrelated user changes and avoid repository-specific assumptions that are not documented locally.

## Inspect

1. Read `AGENTS.md`.
2. Confirm the directory is a Git repository with a configured remote.
3. Inspect the current branch, status, staged changes, unstaged changes, and untracked files.
4. Identify the files that belong to the user’s requested change.
5. Check whether a PR already exists for the branch.
6. Resolve the base from the existing PR, the remote default branch, or `main` as the final fallback.
7. Review commits and the full diff against that base.

Do not hard-code `stg`, another repo’s labels, toolchain, paths, ticket prefix, or CI rules.

If unrelated changes overlap the intended commit and cannot be separated safely, stop and ask for direction.

## Validate

Choose checks from the changed files and `AGENTS.md`:

### Python changes

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest python/
```

### Rust changes

```bash
cargo fmt --check
uv run maturin develop
cargo test
```

### Before publishing

```bash
prek run --all-files
```

Run the full pre-commit suite when its dependencies are available. If a check cannot run, record the exact reason in the handoff and the PR test plan. Do not hide failures.

## Commit and push

1. Stage only the intended files. Never use `git add .`.
2. Group separate concerns into separate commits when that improves reviewability.
3. Use concise commit messages matching repository conventions.
4. Push the current feature branch and set its upstream if necessary.
5. Never force-push unless the user explicitly asks and the exact target is confirmed.

Do not commit generated caches, local environment files, credentials, or unrelated artifacts.

## PR content

Use a title that describes the outcome. Follow a documented title convention when one exists; do not invent a restricted type list.

Write a compact body:

```markdown
## Summary
- [What changed]
- [Why]

## Behavior and design
[Only the architecture, state, compatibility, or migration context reviewers need]

## Test plan
- [Command and result]
- [Manual verification]

## Risks and follow-ups
- [Known risk, rollout note, or deferred work]
```

Include issue links only when they are real and relevant. Preserve useful existing PR content when updating a PR.

## Create or update

Prefer the installed GitHub publishing workflow or connector. Use `gh pr create` or `gh pr edit` only when connector coverage is insufficient.

- Honor an explicit draft or ready-for-review request.
- Do not add labels, reviewers, projects, milestones, or release toggles unless the user asks or repository guidance clearly requires them.
- Do not add AI attribution.
- Do not post screenshots, videos, or generated images unless the user asks.

After creation or update, verify the PR title, body, base, head, URL, and visible checks. Return the URL, commits included, validation results, and any remaining blocker.
