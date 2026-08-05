# ADR-0004: Keep cloud orchestration separate and use Clerk plus Neon

- **Status:** Accepted
- **Date:** 2026-08-04
- **Decision owners:** Synor maintainers and cloud-platform owner
- **Related plan:** [Synor Cloud platform implementation plan](synor-cloud-platform-implementation-plan.md)

## Context

Synor's current repository is an Apache-2.0 Python/Rust engine, SDK, connector,
and local-runtime project. Its execution model treats LMDB state, stable
component paths, target-state ownership, and reconciliation as local engine
authority. It has no hosted organization, user, API-key, tenant database,
runner fleet, scheduler, billing, or managed execution service.

The commercial platform needs human authentication, organization membership,
customer API keys, multi-tenant control state, audit, scheduling, and fleet
coordination. Adding those dependencies directly to the SDK would couple local
execution to hosted availability, mix commercial records into an
Apache-licensed repository, and risk confusing cloud control state with engine
execution truth.

## Decision

1. Keep this `synor` repository responsible for the open engine, SDKs,
   connectors, read-only integrity scanner, local controlled execution, and
   language-neutral evidence/contracts.
2. Implement the hosted modular monolith, console, workers, migrations,
   billing, and production infrastructure in a separately governed
   `synor-cloud` repository. Do not create a partial cloud service under this
   repository merely to make Phase 2 look started.
3. Prefer a separately versioned, inspectable `synor-agent` repository for the
   outbound-only customer runner.
4. Use Clerk as external authority for human sessions, Organizations,
   enterprise federation/Directory Sync, and Clerk-issued Organization API-key
   validity. Synor remains authority for internal service accounts, product
   roles, project restrictions, approvals, and local key denial.
5. Use Neon PostgreSQL for hosted control-plane domain state. API/workers use
   pooled connections and transaction-local tenant context; migrations and
   logical backups use direct connections. Standard PostgreSQL RLS is defense
   in depth. Neon Auth, Neon Data API, and browser database access are excluded.
6. Keep runner identity on short-lived mTLS and connector credentials inside
   customer or managed secret boundaries. Neither uses a human or customer API
   key.
7. Keep LMDB authoritative for incremental execution state. Neon coordinates
   cloud operations but does not replace or asynchronously mirror LMDB
   ownership.

## Consequences

- Phase 1 integrity work can land additively without changing `App.update()`,
  PyO3, Rust storage, or connector mutation paths.
- Phase 2 cannot be truthfully marked complete in this repository. Completion
  requires the separate service repository, real non-production Clerk and Neon
  resources, cross-tenant tests, security review, and restore evidence.
- Contracts crossing repositories must be versioned and tested with golden
  fixtures before either producer requires a new field.
- Clerk and Neon outages have explicit fail-closed/degraded behavior; webhook
  projections and queues never become authorization or domain-state authority.
- Moving commercial service code into this repository later requires a new
  licensing, release, and ownership decision.

## Alternatives rejected

### Put a FastAPI service inside `python/synor`

Rejected because it couples the local package to server/auth/database
dependencies and encourages customer Python execution inside the API trust
boundary.

### Build authentication and API-key storage ourselves

Rejected because session security, federation, credential verification, and
enterprise lifecycle are not Synor's product differentiation. Synor still
keeps the local authorization and denial state needed for domain correctness.

### Use Neon Auth/Data API with Clerk

Rejected because two client authentication paths would fragment authorization
and permit browser-to-database access around Synor's resource checks. The
server API is the only customer-facing data boundary.

### Store engine ownership in Neon

Rejected because cloud-row replication cannot preserve Synor's existing local
atomic reconciliation and stable-path cleanup semantics without a separate,
proven engine migration.

## Validation required before Phase 2 completion

- contracted Clerk feature, support, data-handling, and API-key cost review;
- contracted Neon region, protected-branch, network, restore, compute, storage,
  and transfer review;
- separate development/staging/production resource topology;
- wrong-Organization tests on every public object route;
- pooled-connection RLS tests alternating tenants and omitting context;
- Clerk API-key create/use/local-deny/remote-revoke/reconcile drill;
- signed webhook replay/reorder/outage tests;
- Neon point-in-time and independent logical-backup restore drills;
- external review of the deployed authentication and tenant-isolation paths.
