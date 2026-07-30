---
name: database
description: Design, review, migrate, and troubleshoot databases, persistence layers, and Synor database connectors with production-safe sequencing. Use for schema changes, migrations, queries, indexes, backfills, data integrity, transaction or concurrency bugs, and database-backed source or target connectors.
---

# Database

Treat schema and data changes as production operations. Prefer compatible, observable, reversible steps.

## Inspect first

1. Read `AGENTS.md`, schema definitions, migration history, data-access code, and tests.
2. Identify the engine, version, migration tool, deployment model, and transaction boundaries.
3. Trace every reader and writer of affected tables, columns, indexes, and constraints.
4. Establish data volume, sensitivity, retention, and availability requirements when known.
5. Confirm invariants before choosing a migration or query change.

Never access or mutate production data without explicit authorization. If the task creates a Synor target connector, also load the `target-connector` skill.

## Design

Evaluate:

- keys, uniqueness, nullability, foreign keys, and check constraints
- transaction isolation, locking, races, retry behavior, and idempotency
- query plans, selectivity, indexes, write amplification, and storage cost
- deletion, retention, cleanup, audit, and sensitive-data requirements
- compatibility between old and new application versions
- connector ownership, target-state identity, automatic cleanup, and atomic sync behavior

Keep enforceable invariants in database constraints when safe. Resolve raw identifiers and connection keys to strong types at the earliest boundary.

## Migrate safely

For incompatible or high-volume changes:

1. Expand with additive schema.
2. Deploy code compatible with old and new representations.
3. Backfill in bounded, resumable, idempotent batches.
4. Reconcile counts and invariants.
5. Switch reads and writes deliberately.
6. Enforce final constraints after validation.
7. Contract obsolete schema in a later deployment.

Avoid long table locks. Use the engine’s online or concurrent operations when available and verified for the deployed version.

## Verify

Cover:

- forward migration from representative existing data
- mixed-version behavior
- constraint failures and concurrent writers
- retry and duplicate-delivery behavior
- backfill restart, partial failure, and reconciliation
- expected query plans and cardinality
- connector create, update, delete, and cleanup behavior

Run the repository commands from `AGENTS.md`. Syntax validation alone is not evidence of production safety.

## Deliver

Document execution order, expected locks and duration, observability, reconciliation queries, rollback boundaries, backup requirements, and deferred cleanup.

Stop for explicit approval before destructive schema changes, irreversible backfills, production execution, or sensitive-data access.
