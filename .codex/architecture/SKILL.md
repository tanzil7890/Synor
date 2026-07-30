---
name: architecture
description: Design, review, and document software architecture, system boundaries, data flows, runtime behavior, migrations, and architectural decisions. Use for architecture proposals, ADRs, system or integration design, scalability plans, and changes spanning multiple Synor components or trust boundaries.
---

# Architecture

Design from repository evidence and explicit constraints. Separate confirmed behavior, assumptions, and recommendations.

## Gather context

1. Read `AGENTS.md` and the nearest product or architecture source of truth.
2. Use `ccc search` to locate the concept when `ccc` is available; otherwise use `rg` and the repository map. Then inspect the concrete entry points, interfaces, schemas, state transitions, tests, examples, and docs.
3. Identify ownership, external systems, compatibility promises, trust boundaries, and operational constraints.
4. State the problem, success criteria, constraints, and non-goals.
5. Ask for a decision only when different answers materially change the design.

For Synor changes, trace the relevant boundary across the public Python API, Python internals, PyO3 bridge, Rust core, state storage, and connector implementations. Do not assume every change crosses all layers.

## Model the current system

Describe only relationships needed for the decision:

- component ownership and responsibilities
- synchronous and asynchronous call paths
- authoritative and derived state
- change detection, target-state ownership, cleanup, and atomic sync boundaries
- failure, retry, cancellation, replay, and recovery behavior
- Python/Rust type and error conversion
- deployment, observability, and support responsibilities when applicable

Use a small diagram only when it makes a multi-component relationship materially easier to understand.

## Evaluate options

Compare viable options against the same criteria:

- correctness and API fit
- backward compatibility and migration risk
- durability and failure recovery
- operability and debuggability
- implementation and maintenance cost
- performance, scalability, and reversibility

Prefer the smallest API surface that solves the concrete need. Do not introduce a service, database, queue, framework, abstraction, or public knob when an existing boundary is adequate.

## Produce the decision

Document:

1. Context and decision drivers.
2. Chosen design and ownership boundaries.
3. Data flow, state ownership, and API contracts.
4. Failure modes, recovery, idempotency, and reconciliation.
5. Alternatives considered and why they were rejected.
6. Consequences and accepted tradeoffs.
7. Incremental rollout, migration, rollback, and compatibility plan.
8. Tests, observability, and documentation required.
9. Open questions that genuinely block implementation.

## Synor guardrails

- Preserve the declarative target-state model and stable component paths.
- Keep I/O async-first across Rust and Python boundaries.
- Route LMDB writes through `Storage::run_txn`.
- Keep public exports intentional and update `core.pyi` when PyO3 APIs change.
- Validate weak identifiers or raw values at the earliest boundary.
- Avoid speculative public parameters, callbacks, or configuration.

Do not implement the design unless the user also asks for implementation.
