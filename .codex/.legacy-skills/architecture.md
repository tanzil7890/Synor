<!-- Preserved pre-Codex/Synor import. -->
---
name: architecture
description: Design, review, and document software architecture, system boundaries, data flows, runtime behavior, and architectural decisions. Use when Codex is asked for an architecture proposal, ADR, system design, integration design, scalability plan, migration architecture, or review of a change that affects multiple components or trust boundaries.
---

# Architecture

Design from repository evidence and explicit constraints. Separate confirmed facts from recommendations and assumptions.

## Gather context

1. Read `AGENTS.md` and the product or architecture source of truth.
2. Inspect the affected entry points, interfaces, schemas, deployments, and tests.
3. Identify actors, tenants, trust boundaries, sensitive data, external systems, and operational owners.
4. State the problem, success criteria, constraints, and non-goals.
5. Ask for a decision only when different answers materially change the design.

## Model the current system

Describe only relationships needed for the decision:

- component ownership and responsibilities
- synchronous and asynchronous request paths
- authoritative state and derived state
- failure, retry, timeout, replay, and recovery behavior
- authentication, authorization, tenant isolation, and audit boundaries
- deployment, observability, and support responsibilities

Use a small Mermaid diagram when it makes a multi-component relationship materially clearer.

## Evaluate options

Compare viable options against the same criteria:

- correctness and product fit
- security, privacy, and compliance
- durability and failure recovery
- operability and observability
- delivery complexity and migration risk
- cost, scalability, and reversibility

Do not introduce a service, database, queue, framework, or vendor when an existing boundary solves the problem adequately.

## Produce the decision

Document:

1. Context and decision drivers.
2. Chosen design and component responsibilities.
3. Data flow, state ownership, and API or event contracts.
4. Security, tenant isolation, and sensitive-data handling.
5. Failure modes, recovery, idempotency, and reconciliation.
6. Alternatives considered and why they were rejected.
7. Consequences and accepted tradeoffs.
8. Incremental rollout, migration, rollback, and compatibility plan.
9. Tests, observability, alerts, and operational runbooks required.
10. Open questions with an owner or decision deadline.

## Review guardrails

- Preserve immutable published workflow versions and pinned executions.
- Keep workflow state in a durable execution system, not an incidental queue.
- Make external effects idempotent, deduplicated, or reconciled.
- Enforce tenant authorization at every protected boundary.
- Keep PHI and credentials out of logs, fixtures, diagrams, and examples.
- Route clinical uncertainty and high-risk exceptions to authorized humans.
- Include a transition plan; a target diagram alone is not an implementable architecture.

Do not implement the design unless the user also asks for implementation.
