<!-- Preserved pre-Codex/Synor import. -->
---
name: database
description: Design, review, migrate, and troubleshoot application databases and data-access code with production-safe sequencing. Use when Codex is asked to change schemas, write migrations or queries, model tenant data, analyze indexes and performance, protect data integrity, plan backfills, or diagnose database correctness and concurrency problems.
---

# Database

Treat schema and data changes as production operations. Prefer compatible, observable, reversible steps.

## Inspect before changing

1. Read `AGENTS.md`, schema definitions, migration history, and data-access code.
2. Identify the database engine, version, migration tool, deployment model, and transaction boundaries.
3. Trace all readers and writers of affected tables, columns, indexes, and constraints.
4. State data volume, tenant boundaries, sensitivity, retention, and availability requirements when known.
5. Confirm invariants that must remain true before choosing a migration.

Never access or mutate production data without explicit authorization.

## Design the change

Evaluate:

- primary keys, foreign keys, uniqueness, nullability, and check constraints
- tenant scoping and prevention of cross-tenant reads or writes
- transaction isolation, locking, races, and idempotency
- query plans, selectivity, indexes, write amplification, and storage cost
- deletion, retention, audit, and sensitive-data requirements
- compatibility with old and new application versions during rollout

Keep business invariants in enforceable constraints when the database can express them safely. Keep authorization in the application boundary even when row-level security adds defense in depth.

## Use expand-migrate-contract

For incompatible or high-volume changes:

1. Expand with nullable columns, new tables, compatible indexes, or additive constraints.
2. Deploy code that can work with both old and new representations.
3. Backfill in bounded, resumable, idempotent batches.
4. Reconcile counts and invariants and monitor errors and latency.
5. Switch reads and writes deliberately.
6. Enforce final constraints only after validation.
7. Contract obsolete columns or paths in a later deployment.

Avoid long table locks. Use the database engine's concurrent or online index and constraint-validation features when available.

## Verification

Include tests for:

- forward migration from representative existing data
- application behavior during mixed-version deployment
- tenant-isolation and authorization failures
- uniqueness, nullability, foreign-key, and check constraints
- concurrent writers, retry behavior, and duplicate delivery
- backfill restart, partial failure, and reconciliation
- query plans and performance for expected cardinality

Run the repository's migration and test commands. Do not claim production safety from syntax validation alone.

## Delivery notes

Document:

- execution order and responsible owner
- expected duration, locks, and resource impact
- dashboards, alerts, and reconciliation queries
- rollback boundaries and whether rollback would lose data
- backup or restore requirements
- cleanup that must happen in a later release

Stop and request approval before destructive changes, irreversible backfills, production execution, or access to sensitive customer data.
