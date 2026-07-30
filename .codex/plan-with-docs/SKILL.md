---
name: plan-with-docs
description: Create an implementation plan grounded in repository evidence and current primary documentation. Use when the user explicitly asks for a plan, design, spec, migration strategy, or research-first implementation approach for a non-trivial change. Do not trigger for small, well-scoped edits that can be implemented directly.
---

# Plan With Docs

Build a plan from evidence, not remembered APIs or speculative architecture.

## Research

1. Read `AGENTS.md` and any closer instructions.
2. Use `ccc search` to locate the concept and existing implementation pattern when available. Otherwise use `rg` and the repository map.
3. Inspect the relevant source, tests, examples, docs, manifests, and generated bindings.
4. For external dependencies, use current primary documentation. Browse when the API or behavior may have changed.
5. Record confirmed facts, assumptions, and unknowns separately.

Use subagents only when the user or applicable repository instructions explicitly ask for delegation or parallel agent work.

## Decide scope

State:

- problem and desired outcome
- current behavior
- constraints and non-goals
- compatibility and migration requirements
- decisions that materially affect the approach

Ask only for decisions that cannot be discovered locally and would change the plan. Continue with a clearly labeled safe assumption when the choice is reversible.

## Plan

Include only sections that help execution:

1. Proposed approach and why it fits existing patterns.
2. Component, data, or state flow.
3. Ordered implementation steps with dependencies.
4. Exact files or modules likely to change.
5. Public API and backward-compatibility impact.
6. Failure modes, concurrency, security, and cleanup behavior.
7. Tests, type checks, formatting, docs, and generated artifacts.
8. Rollout, migration, and rollback when applicable.
9. Open questions that genuinely block work.

Use a diagram only when it materially clarifies a multi-component relationship or state transition.

## Synor checklist

When relevant, verify:

- declarative target-state ownership and cleanup semantics
- stable component paths and memoization behavior
- async behavior across Python, PyO3, and Rust
- `core.pyi` updates for PyO3 API changes
- LMDB writes remain behind `Storage::run_txn`
- public exports and underscore conventions
- end-to-end tests through user-facing APIs
- commands required by `AGENTS.md`

Do not create a persistent spec file unless the user asks for one or the repository already requires it.
