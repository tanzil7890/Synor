---
name: codebase-audit
description: Audit a cross-cutting concern across a codebase and report its implementations, consistency, ownership, duplication, documentation, and consolidation opportunities. Use when asked to audit, check consistency, find all patterns, assess fragmentation, or examine codebase health for concerns such as errors, configuration, caching, tracing, concurrency, or public API conventions.
---

# Codebase Audit

Produce an evidence-backed inventory before recommending consolidation.

## Scope

1. Read `AGENTS.md` and the repository map.
2. Clarify the concern and boundaries. If none are given, choose the smallest useful scope and state it.
3. Use `ccc search` for conceptual discovery when available, then `rg` for exact symbols, imports, calls, configuration, and tests. Fall back to `rg` and directory inspection when `ccc` is unavailable.
4. Inspect source, tests, examples, docs, manifests, generated bindings, and relevant skill guidance.
5. Track confirmed implementations separately from possible matches and excluded cases.

Do not mandate subagents. Delegate only when the user or applicable repository instructions explicitly request parallel agent work.

## Analysis

For each implementation, capture:

- file and owning layer
- public entry point and consumers
- dependency or primitive used
- initialization and lifecycle
- error, concurrency, and cleanup behavior
- tests and documentation
- whether it follows a canonical pattern

For Synor, check Python public modules, Python internals, PyO3 bindings and stubs, Rust crates, connectors, examples, and docs when relevant.

Assess fragmentation from evidence:

- **Consistent:** one canonical implementation, routinely reused
- **Mostly consistent:** a canonical path with justified exceptions
- **Fragmented:** multiple overlapping approaches without clear ownership
- **Highly fragmented:** repeated independent implementations with incompatible behavior

Do not treat every variation as duplication. Different runtime, performance, compatibility, or ownership requirements can justify separate implementations.

## Output

Lead with the answer, then provide:

1. Scope and method.
2. Inventory table with file references.
3. Canonical pattern and exceptions.
4. Gaps in tests, docs, `AGENTS.md`, or `.agents/skills`.
5. Ranked recommendations:
   - quick corrections
   - focused consolidation
   - longer-term architectural work
6. Residual uncertainty and searches that could not be completed.

Use a diagram only when three or more components or flows are materially easier to understand visually.

Do not edit code during an audit unless the user also asks for fixes.
