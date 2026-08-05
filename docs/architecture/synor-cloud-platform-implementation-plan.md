# Synor Cloud platform implementation plan

- **Status:** Proposed implementation blueprint
- **Repository baseline:** `c9aac3c828de1dbbca11fcb2e1b34adddec12418` (`origin/main`, 2026-08-02)
- **Document date:** 2026-08-04
- **Current product version:** Python `0.1.0a1`; Rust `0.1.0-alpha.1`
- **Selected managed services:** Clerk for human and customer API authentication; Neon for control-plane PostgreSQL
- **Audience:** Synor maintainers, cloud-platform engineers, security reviewers, and product leadership
- **Scope:** Convert Synor from a local-first SDK/runtime into an enterprise cloud product without weakening the existing execution, ownership, reconciliation, or privacy guarantees

This is a development guide, not a statement that the hosted product already
exists. It separates confirmed repository behavior from proposed cloud work and
uses explicit release gates so later contributors do not mistake an internal
milestone for a production guarantee.

The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** describe implementation
requirements in this plan. A phase is complete only when its exit criteria and
validation gates pass.

## Verified implementation status (2026-08-04)

| Phase | Status | Verified meaning |
|---|---|---|
| Phase 0 — decisions and baseline | ⏳ In progress | ✅ Repository audit, accepted Clerk/Neon boundary ADR, versioned fixture, and 100k-source/100k-target scale gate; human owners, interviews, design partners, and reference-pipeline validation remain. |
| Phase 1 — local read-only integrity | ✅ Repository implementation / ⏳ product validation | ✅ Experimental API, streaming bounded scanner, S3 and Qdrant inspectors, CLI, documentation, example, and automated gates are implemented here; live partner scans and independent privacy review remain exit criteria. |
| Phase 2 — cloud identity, tenancy, and API keys | ⛔ Separate repository / ⏳ not implemented | ✅ Clerk and Neon architecture is specified and accepted; the deployable `synor-cloud` service, real vendor environments, tenant migrations, console, security tests, and restore drills do not exist in this repository. |

The ✅ symbol marks evidence that exists now. It never substitutes for a phase's
unmet exit criteria, especially external security review, design-partner proof,
or a deployed Clerk/Neon environment.

## How to use this guide

- A contributor new to Synor should read sections 3, 4, and 20 before editing.
- A cloud/control-plane contributor should read sections 5–10, 13, 14, and 19.
- An engine/connector contributor should read sections 2.2, 3, 4, 10, 11,
  14.3, 16, 18, and 20.
- A product/security leader should read sections 1, 2, 12–16, 21–23.
- Before starting a phase, re-check the repository baseline and update the
  “confirmed facts” if main has moved. Do not treat proposed files as existing
  files.

The file map deliberately enumerates every cloud-sensitive public module,
internal subsystem, connector directory, native engine layer, PyO3 boundary,
SDK, test family, and release file. Files inside a subsystem are changed only
through that subsystem's owner/invariants; section 16 names every proposed new
file and section 17 breaks those changes into reviewable PRs.

### Contents

1. Executive decision
2. Outcomes, constraints, non-goals, and customer onboarding
3. Confirmed current architecture
4. Repository map and change rules
5. Target system architecture
6. Tenancy and control-plane data model
7. Identity, API keys, authorization, and secrets
8. Public API and runner protocol
9. Immutable deployment and software supply chain
10. Execution, planning, leases, and retry semantics
11. Index Integrity product boundary
12. Evidence, observability, privacy, and audit
13. Security architecture and threat model
14. Reliability, state durability, and disaster recovery
15. Commercial model and enterprise onboarding
16. Phase-by-phase implementation roadmap
17. Recommended pull-request sequence
18. Comprehensive test and validation strategy
19. Rollout, compatibility, migration, and kill switches
20. Safe development checklist
21. Risks, decisions, and open questions
22. Release gates
23. Confirmed facts, assumptions, and unknowns
24. Primary references
25. Engineering compass

---

## 1. Executive decision

Build Synor Cloud as a **hybrid control plane with customer-hosted runners
first**, followed by an optional managed execution plane.

Do not turn the Python package into a generic multi-tenant endpoint that runs
customer code inside the API service. Synor pipelines are ordinary Python and
may load native libraries, create subprocesses, contact external systems, and
hold highly privileged source and target credentials. Running them beside the
control plane would combine the most sensitive trust boundaries before the
product has proved demand.

The chosen shape is:

```text
Developer, CI, or web console
              |
              | Human session or scoped service-account API key
              v
+-----------------------------------------------------------------+
| Synor Cloud control plane                                       |
|                                                                 |
| Org/project/deployment registry | plans and approvals           |
| Scheduler and leases            | evidence and audit            |
| Clerk authentication            | Neon control database         |
| Runner fleet management         | usage, billing, and support   |
+-------------------------------+---------------------------------+
                                |
                                | Outbound authenticated polling
                                | and presigned artifact transfer
                                v
+-----------------------------------------------------------------+
| Synor runner                                                     |
| Customer VPC/on-prem first; isolated managed task later         |
|                                                                  |
| Verifies immutable deployment | injects local secret references |
| Owns one execution attempt    | opens persistent LMDB state      |
| Runs SynorRuntime             | spools redacted evidence         |
+-------------------------------+----------------------------------+
                                |
                                v
                    Customer sources and targets
```

The cloud API key authenticates the developer or CI process to the control
plane. It is not a replacement for S3, database, vector-store, model-provider,
or runner credentials.

Clerk is the selected authentication provider for human sessions,
Organizations, and organization-scoped customer API keys. Neon is the selected
managed PostgreSQL provider for cloud control state. Neither vendor enters the
Synor execution core: runner identity remains short-lived mTLS, connector
credentials remain local/managed secrets, and LMDB remains authoritative for
incremental execution state.

### 1.1 Product decision

The initial commercial product SHOULD be **Index Integrity**, not “hosted
incremental Python.” The repository's product analysis already identifies the
strongest wedge: a read-only auditor that finds stale, missing, duplicated, or
orphaned derived AI state, followed by a paid repair plane. Verified Erasure is
the later enterprise tier. See [PRODUCT_IDEAS.md](../../PRODUCT_IDEAS.md).

The product ladder is:

1. Free, local, read-only integrity scan.
2. Cloud history, scheduling, alerts, and fleet visibility.
3. Approval-gated repair using Synor reconciliation.
4. Enterprise governance, private deployment, support, and verified-erasure
   workflows through explicitly governed boundaries.

The API key makes adoption convenient. Correctness, evidence, operations, and
support are what customers pay for.

### 1.2 Repository decision

Use three ownership boundaries:

| Codebase | Visibility | Responsibility |
|---|---|---|
| **This `synor` repository** | Apache-2.0 | Rust engine, Python and Rust SDKs, connectors, local integrity auditor, local controlled execution, evidence formats, consistent state snapshot primitives |
| **`synor-agent`** | Prefer public/open source | Outbound-only customer runner, workload enrollment, deployment verification, execution supervision, local evidence spool, updater |
| **`synor-cloud`** | Private commercial service | API, console, identity, tenancy, scheduler, runner gateway, deployment registry, build service, billing, notifications, infrastructure |

Do not place hosted billing, customer records, production infrastructure, or
private service implementation under this Apache-licensed repository by
accident. If company strategy later chooses one monorepo, make that an explicit
licensing and release decision first.

---

## 2. Outcomes, constraints, and non-goals

### 2.1 Desired outcomes

The completed platform must let a customer:

1. Create an organization and project.
2. Create a scoped service account and API key.
3. Enroll a runner in a customer-controlled environment.
4. Package and deploy an existing Synor app without rewriting pipeline code.
5. Generate a plan against an immutable deployment.
6. Review and approve the exact planned action digest.
7. Run on demand or on a schedule.
8. Observe status, statistics, evidence, provenance, and unresolved cases.
9. Rotate credentials, revoke runners, and export audit records.
10. Pay through a predictable platform and usage model.

For large customers, the platform must additionally support SSO, SCIM, RBAC,
private networking, regional placement, audit export, backups, incident
response, support escalation, and a bounded security/compliance narrative.

### 2.2 Non-negotiable Synor constraints

The cloud work MUST preserve these existing invariants:

1. **Stable component paths remain the ownership key.** Cloud run IDs must not
   enter component paths or memo fingerprints.
2. **LMDB remains authoritative for incremental execution** until a separate
   engine migration is designed and proven. PostgreSQL cloud records are not a
   substitute for engine ownership state.
3. **All LMDB writes flow through `Storage::run_txn`.** Do not open a separate
   `heed` write transaction or wrap LMDB in a new application mutex.
4. **`App.update()` remains compatible.** Cloud control is additive and uses
   the existing `SynorRuntime`/controlled boundary.
5. **Preview uses native reconciliation.** Never reimplement plan logic in the
   service.
6. **Preview is not called a sandbox.** It executes ordinary Python.
7. **Python and Rust I/O remain async-first.** Blocking wrappers remain only at
   established boundaries.
8. **Target atomicity is connector-specific.** Never claim a cross-target
   distributed transaction.
9. **At-least-once dispatch is explicit.** Do not market remote execution as
   exactly once.
10. **Evidence remains metadata-only by default.** “Metadata-only” is still
    treated as confidential operational data.
11. **Strict revocation claims remain bounded** to governed source, certified
    target, and guarded retrieval boundaries.
12. **Native schema activation is a one-way boundary for the original LMDB
    database.** Follow the existing downgrade-copy runbook.
13. **Public API expansion stays minimal.** Internal hooks are preferred until
    a concrete external use case is stable.
14. **Existing Python public-module conventions remain in force.** Declare
    `__all__`, underscore non-public imports and symbols, and update
    `core.pyi` for every PyO3 surface change.

### 2.3 First-year non-goals

- Rewriting the engine around a distributed database.
- Running arbitrary customer Python in the control-plane web process.
- Sharing one Python interpreter between tenants.
- Claiming cross-target exactly-once effects.
- Supporting every connector in managed mode.
- Multi-region active/active execution against one LMDB state directory.
- Universal operating-system sandboxing from a Python policy hook.
- Automatic retry of a connector whose partial-commit behavior is uncertified.
- Legal-compliance, physical-media-erasure, or model-unlearning claims.
- Storing raw documents, embeddings, prompts, target payloads, or connector
  credentials in cloud evidence by default.
- A public ingestion API that hides all pipeline concepts. An opinionated
  document-ingestion API may be added later on top of a proven product.

### 2.4 Proposed customer onboarding and API-key experience

This is the target golden path, not an existing CLI contract. Final command and
package names require an API/CLI ADR.

**Interactive setup:**

1. User signs in through the console and creates an organization, project, and
   environment.
2. The console asks where data should run:
   - “My cloud/VPC” creates a runner enrollment;
   - “Synor managed” appears only after managed execution is generally
     available in the selected region.
3. User installs the runner with a generated Helm/Terraform command containing
   a one-use, 15-minute enrollment token. The page shows network endpoints,
   permissions, and exactly which metadata leaves the environment.
4. Runner preflight checks architecture, storage, time synchronization, DNS,
   proxy/CA, outbound TLS, secret providers, and connector reachability; it
   returns a readable pass/fail report.
5. User uploads an existing Synor app. The service builds an immutable signed
   deployment and reports its package/image/SBOM/provenance digests.
6. User creates a plan. The console shows source coverage, creates/updates/
   deletes, unresolved cases, connector guarantees, policy, and expiration.
7. After required approval, user starts the run or adds a schedule. Evidence
   and post-run verification link back to that exact plan.

**CI/service-account setup:**

```bash
# Proposed separate cloud client; not part of the current synor package.
synor-cloud service-accounts create deploy-ci \
  --project project_123 \
  --scopes deployments:write,plans:create,runs:create

# Complete value is shown exactly once.
synor-cloud api-keys create --service-account service_account_123

export SYNOR_CLOUD_API_KEY='value-returned-once'
export SYNOR_CLOUD_API_URL='https://api-host-selected-by-deployment'

synor-cloud deployments create \
  --project project_123 \
  --environment staging \
  --source . \
  --entry-point app:app

synor-cloud plans create --deployment deployment_123
synor-cloud runs create --plan plan_123
synor-cloud runs watch run_123
```

Production automation should normally create a deployment and request a plan;
a separately scoped human or service account approves it. Do not give routine
CI a wildcard organization key or approval scope by default.

**Direct API use:**

```http
POST /v1/runs HTTP/1.1
Authorization: Bearer <Clerk-issued-Organization-API-key>
Idempotency-Key: <caller-generated-unique-value>
Content-Type: application/json

{
  "plan_id": "plan_123"
}
```

The response is `202 Accepted` with a run resource. The same idempotency key
and request returns the same logical result; reusing it for another plan fails.

A small generated `synor-cloud` client package may wrap the API:

```python
from synor_cloud import Client

client = Client.from_env()
run = client.runs.create(plan_id="plan_123", idempotency_key="deploy-7842")
print(run.id)
```

This client is a control-plane client. The existing `synor` package remains the
pipeline SDK/runtime, so upgrading the hosted client cannot alter local engine
semantics.

Target onboarding objectives after the product stabilizes:

- account to first healthy runner in under 15 minutes for the reference path;
- first read-only integrity result in under one hour for a representative small
  corpus;
- no production write permission until a reviewed plan and restore/recovery
  setup exist;
- API-key rotation without deployment or runner downtime.

---

## 3. Confirmed current architecture

### 3.1 Runtime path

```text
User pipeline
  App + @syn.task + spawn/call/spawn_each + target declarations
      |
      v
Python internals
  environment, component context, function fingerprints, target wrappers
      |
      v
PyO3 bridge
  async Python/Tokio conversion and typed target/action bridge
      |
      v
Rust core
  scheduling, memoization, stable ownership, reconciliation, native effects
      |
      +---------------------> connector target action sink
      |
      v
LMDB
  app namespaces, fingerprints, tracking, ownership, tombstones, effects
```

The core behavior is documented in [README.md](../../README.md),
[reading.md](../../reading.md), and [AGENTS.md](../../AGENTS.md).

### 3.2 Authoritative versus derived state

| State | Current owner | Authority | Cloud treatment |
|---|---|---|---|
| Stable paths, fingerprints, memo entries, ownership, target tracking | Native LMDB | Authoritative for execution | Remains with runner; snapshot and restore as a unit |
| Native effect obligations and schema markers | Native LMDB | Authoritative for strict engine effects | Remains with runner; never reconstructed from cloud summaries |
| Run manifests and audit JSONL | Local controlled run directory | Local evidence | Transform through a strict allow-list before upload |
| Provenance records | Local run directory and optional `StateStore` | Ownership-level evidence | Mirror immutable, versioned records; do not upgrade claim to byte lineage |
| Quarantine, replay, revocation cases and receipts | Control-plane `StateStore` | Review/evidence state | Keep local in hybrid mode; mirror safe projections to cloud |
| Cloud identity mappings, deployments, schedules, leases, approvals, usage | Does not exist yet | Future control-plane authority | Clerk for external human/key identity; Neon PostgreSQL for Synor domain state |
| Packages, images, SBOMs, evidence bundles, snapshots | Partial local package support | Future artifact authority | Content-addressed object/OCI storage |

### 3.3 Current execution-control seam

[`python/synor/execution.py`](../../python/synor/execution.py) is the preferred
integration point. `SynorRuntime` already wraps `App._update_controlled()` and
provides typed `plan`, `run`, `explain`, replay, PII preflight, evidence, and
strict-revocation health behavior.

Cloud code MUST call this facade or a deliberately extracted internal service
boundary. It MUST NOT call connector handlers directly, invent target actions,
or duplicate reconciliation in a web worker.

### 3.4 Current local durability boundary

[`python/synor/state.py`](../../python/synor/state.py) exposes a four-operation
async byte-store protocol with file and memory implementations plus an
AES-256-GCM wrapper. It is intentionally separate from LMDB. The revocation
writer lock in
[`python/synor/_internal/state_store_lock.py`](../../python/synor/_internal/state_store_lock.py)
is event-loop and process local; it explicitly requires a transactional backend
for distributed writers.

Therefore:

- Do not point multiple cloud workers at the same file-backed `StateStore`.
- Do not treat a remote object-store adapter as transactional merely because
  individual object replacements are atomic.
- Do not let a cloud mirror become the source of truth for local suppression
  unless a new fenced/transactional protocol is designed.
- A managed multi-writer ledger requires append semantics, uniqueness,
  compare-and-swap/fencing, and recovery tests beyond the current protocol.

### 3.5 Current package boundary

[`python/synor/packaging.py`](../../python/synor/packaging.py) creates a
deterministic ZIP containing selected Python/project files, a manifest, and a
lock of installed direct distributions. It deliberately excludes hidden
directories, `.env`, databases, output, caches, and data.

This is a strong source-transport seed, but it is not yet a deployment image:

- it does not resolve a complete transitive environment;
- it does not download or vendor dependencies;
- it does not define a base operating-system image;
- it does not generate an SBOM;
- it is hashed but not signed by a trusted builder; and
- it does not prove build isolation or provenance.

### 3.6 Current reliability boundary

The engine has strong local crash, cancellation, lease, resize, and native
effect work. The release gate in
[`docs/architecture/reliability-hardening-status.md`](reliability-hardening-status.md)
also records what remains:

- complete reconciliation still materializes O(component state);
- a durable paged action journal/cursor is required for truly bounded
  million-item reconciliation; and
- the sink capability inventory is broad, but positive failure-injection
  certification is currently partial.

The cloud scheduler MUST consume the factual capability contract rather than
assuming every sink is safely retryable.

---

## 4. Repository map and change rules

This section is the file-level navigation guide. “Preserve” means cloud work
should not modify the file during early phases. “Extend” means a later bounded
change is expected. “Reference” means the file defines behavior or tests a
contract but is not itself the cloud integration point.

### 4.1 Repository root, build, and release

| Path | Current responsibility | Cloud implementation rule |
|---|---|---|
| [`AGENTS.md`](../../AGENTS.md) | Repository invariants, build/test commands, conventions | Update only when a new permanent workflow or invariant is accepted |
| [`README.md`](../../README.md) | Public project description and quick start | Do not market hosted capabilities until the corresponding release gate passes |
| [`reading.md`](../../reading.md) | Deeper execution-model explanation | Preserve as the local-engine mental model |
| [`PRODUCT_IDEAS.md`](../../PRODUCT_IDEAS.md) | Product hypotheses and wedge analysis | Treat as strategy input, not an implementation contract |
| [`pyproject.toml`](../../pyproject.toml) | Python package, optional connector dependencies, mypy groups | Do not add cloud-server dependencies; only add runtime dependencies required by OSS features |
| [`Cargo.toml`](../../Cargo.toml) | Rust workspace and shared dependency versions | Add an OSS agent crate here only after the repository-boundary ADR approves it |
| [`.pre-commit-config.yaml`](../../.pre-commit-config.yaml) | Full local validation and generated CLI docs | Every new source family must be covered by appropriate hooks |
| [`.github/workflows/CI.yml`](../../.github/workflows/CI.yml) | Cross-platform package CI entry | Preserve Python/Rust platform coverage; add protocol/schema checks additively |
| [`.github/workflows/release.yml`](../../.github/workflows/release.yml) | Wheels, sdist, attestations, PyPI release | Reuse the attestation pattern; do not publish runner or cloud images from this job accidentally |
| [`dev/check_release_readiness.py`](../../dev/check_release_readiness.py) | Fail-closed legal/provenance/brand release checks | Extend when cloud/agent artifacts become official release outputs |
| [`dev/target-sink-certification.json`](../../dev/target-sink-certification.json) | Machine-readable sink capability evidence | Scheduler retry policy must derive from this contract or a versioned exported projection |

### 4.2 Python public control and evidence modules

| Path | Current responsibility | Cloud stance |
|---|---|---|
| [`python/synor/__init__.py`](../../python/synor/__init__.py) | Deliberate public exports | Do not export cloud clients or experimental callbacks initially |
| [`python/synor/cli.py`](../../python/synor/cli.py) | Local CLI and operator commands | Keep local commands stable; prefer a separate `synor-cloud` CLI until the hosted contract is stable |
| [`python/synor/execution.py`](../../python/synor/execution.py) | `SynorRuntime`, plan/run/explain, typed reports | Extend only with an internal, typed event sink or execution context after local no-op compatibility tests exist |
| [`python/synor/audit.py`](../../python/synor/audit.py) | Local manifest recorder and metadata redaction | Reference the redaction rules; define a stricter network allow-list schema instead of uploading arbitrary output |
| [`python/synor/provenance.py`](../../python/synor/provenance.py) | Ownership-level artifact provenance | Version transport separately; keep claim bounded to connector-visible ownership |
| [`python/synor/replay.py`](../../python/synor/replay.py) | Preview replay envelope and digest verification | Bind cloud approvals to its canonical action/source/policy digests; add source cursor/snapshot binding where supported |
| [`python/synor/packaging.py`](../../python/synor/packaging.py) | Deterministic local source package | Extend with backward-compatible package schema v2 only after build-service contract tests exist |
| [`python/synor/state.py`](../../python/synor/state.py) | Local control-plane store protocol and encryption | Do not add a naive HTTP store; a distributed implementation needs a separate transactional contract |
| [`python/synor/quarantine.py`](../../python/synor/quarantine.py) | Metadata-only review cases | Mirror status safely; approval must never trigger an implicit retry |
| [`python/synor/policy.py`](../../python/synor/policy.py) | Process-wide Python egress policy | Preserve as defense in depth; enforce cloud egress at container/network boundaries |
| [`python/synor/pii.py`](../../python/synor/pii.py) | Structured PII guardrail | Preserve non-claim that this is not complete DLP |
| [`python/synor/governance.py`](../../python/synor/governance.py) | Public governed-source model re-exports | Reuse stable source identity in integrity records |
| [`python/synor/revocation.py`](../../python/synor/revocation.py) | Public revocation repository/controller/operator API | Do not distribute across workers until a transactional ledger design is accepted |
| [`python/synor/retrieval.py`](../../python/synor/retrieval.py) | Guarded retrieval public API | Cloud UI/API must not imply direct vector queries are covered |
| [`python/synor/dashboard.py`](../../python/synor/dashboard.py) | Loopback-only unauthenticated local dashboard | Do not expose over the internet; build a separate authenticated cloud console |
| [`python/synor/inspect.py`](../../python/synor/inspect.py) | Read-only engine-state inspection | Prefer these APIs over direct LMDB reads in runner diagnostics |
| [`python/synor/user_app_loader.py`](../../python/synor/user_app_loader.py) | Imports local app targets | Managed builds execute it only inside an isolated deployment environment |
| [`python/synor/engine_object.py`](../../python/synor/engine_object.py) | Engine-backed Python object support | Preserve unless a concrete runtime transport needs it |

### 4.3 Python processing, connector-kit, resource, and operation modules

| Area | Paths | Cloud rule |
|---|---|---|
| Public processing API | `python/synor/_internal/api.py`, `app.py`, `function.py`, `component_ctx.py` | Preserve stable paths, memo fingerprints, cancellation, and `App` compatibility. Cloud IDs remain outside user component keys. |
| App loading/runner | `app_target.py`, `runner.py` | Runner invokes these only inside the isolated pipeline process; keep app-target validation separate from cloud artifact trust. |
| Environment lifecycle | `environment.py`, `setting.py`, `context_keys.py` | One runner deployment gets a stable DB path and isolated environment. Never share a mutable default environment across tenants. |
| Target protocol | `target_state.py`, `pending_marker.py` | Reuse capability facts; do not add cloud retry knobs to public target APIs. |
| Live processing | `live_component.py`, `auto_refresh` paths in `api.py` | Managed live mode is a later long-running-worker product, not a batch-run option. |
| Deadlines/cancellation | `deadline.py`, `app.py`, `environment.py` | Cloud cancellation is complete only after Synor's quiescence barrier returns. |
| Batching and stats | `batching.py`, `update_stats.py` | Observe through bounded typed snapshots; do not make internal batch counters a billing contract. |
| Inspection | `inspect_api.py` | Extend native inspection through this wrapper rather than reading LMDB from agent/cloud code. |
| Synchronization | `rwlock.py`, `state_store_lock.py` | Preserve their local scope; neither becomes a distributed lock by placing it behind an API. |
| Serialization/fingerprints | `serde.py`, `datatype.py`, `memo_fingerprint.py`, `stable_path.py` | Never deserialize untrusted cloud bytes through pickle; cloud contracts use validated JSON/Protobuf-like schemas. |
| Type helpers | `typing.py`, `_internal/__init__.py` | Keep internal and do not use as a public cloud protocol surface. |
| Connector kit | `python/synor/connectorkits/*.py` | Add read-only integrity inspection as a separate narrow protocol; do not overload target mutation handlers. |
| Resources | `python/synor/resources/*.py` | Reuse stable file/source identities; avoid placing cloud tenant IDs in memoized content. |
| Operations | `python/synor/ops/*.py`, `ops/entity_resolution/*.py` | Pipeline execution remains local/isolated; hosted model credentials follow runner secret policy. |
| Models | `python/synor/models/*.py` | Preserve local/offline behavior; cloud does not force a hosted model. |

The internal revocation files
`revocation_model.py`, `revocation_policy.py`, `revocation_ledger.py`,
`revocation_runtime.py`, `suppression.py`, `retrieval_guard.py`,
`verified_sink.py`, and `state_store_lock.py` form one bounded assurance system.
Do not extract one file into a remote service without tracing the entire
ordering and recovery contract in
[`ADR-0003-provable-index-revocation.md`](ADR-0003-provable-index-revocation.md).

### 4.4 Connector-by-connector map

| Connector directory | Current role | First cloud use | Required before automatic retries/repair |
|---|---|---|---|
| `amazon_s3` | Source | Read-only corpus inventory | Pagination, version/delete semantics, partial-snapshot tests |
| `azure_blob` | Source | Later enterprise source | Same source completeness and identity certification |
| `bigquery` | Target | Later warehouse output | Failure-injection and idempotent replay certification |
| `doris` | Target | Later analytical output | Failure-injection certification |
| `falkordb` | Target | Later graph integrity | Enumeration/verification plus target certification |
| `google_drive` | Source and governed source | First governed reference source | Live provider acceptance and permission/group limitations remain explicit |
| `iggy` | Source and target | Later streaming | Broker restart/rebalance and target acknowledgement tests |
| `kafka` | Source and target | Streaming after batch GA | Live-broker saturation/rebalance and complete sink certification |
| `lancedb` | Local target | Local auditor and repair pilot | Enumeration and crash/replay certification |
| `localfs` | Source and target | Development and air-gapped demo | Preserve path-containment tests |
| `neo4j` | Target | Later graph integrity | Read-only inventory and mutation certification |
| `oci_object_storage` | Source | Later enterprise source | Source snapshot completeness certification |
| `postgres` | Source and target | First SQL/pgvector reference | Add explicit pgvector inventory and target failure-injection coverage |
| `qdrant` | Target and strict revocation adapter | First vector reference | Operator-approved live deployment tests; preserve generation fencing |
| `snowflake` | Target | Later warehouse output | Failure-injection certification |
| `sqlite` | Local target | Local reference and tests | Already the strongest common sink certification; keep as conformance baseline |
| `surrealdb` | Target | Later document/graph output | Inspection and failure-injection certification |
| `turbopuffer` | Target | Later managed vector output | Inspection and failure-injection certification |
| `valkey` | Target | Later search output | Inspection and failure-injection certification |
| `zvec` | Local target | Local vector auditor | Enumeration and crash/replay certification |

Start the product reference path with Google Drive or S3 into Qdrant, then add
Postgres/pgvector. Google Drive plus Qdrant has the most existing governance
machinery; S3 plus Qdrant is the simpler non-ACL audit path.

### 4.5 Rust core file map

| Path | Responsibility | Cloud rule |
|---|---|---|
| `rust/core/src/engine/app.rs` | App update/drop lifecycle, controlled effect mode, operation lease | Extend only for engine-owned snapshot or progress contracts; never add tenant auth here |
| `engine/component.rs` | Component execution/readiness | Preserve stable ownership and completion semantics |
| `engine/context.rs` | Component context, declarations, commit path | Any progress hook must be non-blocking and unable to convert failure into success |
| `engine/environment.rs` | Storage and host runtime ownership | Preserve async environment creation and `run_txn` delegation |
| `engine/execution.rs` | Mount/call execution helpers | Preserve async-first behavior |
| `engine/function.rs`, `logic_registry.rs` | Function calls and logic invalidation | Cloud deployment metadata must not alter fingerprints accidentally |
| `engine/id_sequencer.rs` | Stable execution-local identifier sequencing | Preserve ordering; do not substitute remotely allocated IDs |
| `engine/live_component.rs` | Live lifecycle and handoff | Managed live workers require separate lifecycle/SLO design |
| `engine/runtime.rs`, `profile.rs`, `mod.rs` | Runtime/profile composition and module surface | Add cloud-neutral engine features only; no hosted identity/config dependencies |
| `engine/target_state.rs` | Sink capabilities, effect mode, target reconciliation | Scheduler policy may consume exported facts but must not bypass this path |
| `engine/stats.rs`, `progress_display.rs` | Processing statistics and local display | Add typed observation without coupling engine success to network delivery |
| `state/stable_path.rs`, `stable_path_set.rs`, `target_state_path.rs` | Durable identity types | Never encode cloud run/attempt IDs into these identities |
| `state/native_effect.rs` | Strict-effect descriptors, statuses, summaries | Preserve metadata-only schema and one-way migration rules |
| `state/db_schema.rs` | LMDB key schema | Every new keyspace needs an ADR, migration, corruption tests, and downgrade story |
| `state_store/storage.rs` | LMDB environment, OS leases, resize, write batcher, snapshots | All new writes use `Storage::run_txn`; add consistent snapshot API here if needed |
| `state_store/app_store.rs`, `submit_session.rs`, `txn.rs` | App namespace and transactional operations | Preserve batching and rollback semantics |
| `state_store/mod.rs`, `test_support.rs` | Module exports and core storage fixtures | Extend fixtures with backup/restore and multi-process cases; keep test support non-public |
| `inspect/*` | Read-only engine inspection | Use for safe runner health/evidence; do not parse LMDB externally |

Other native crates remain cloud-neutral: `rust/utils` supplies shared errors,
batching, fingerprints, rate limiting and runtime helpers; `rust/py_utils`
owns Python/Rust conversion and future/error bridging; `rust/ops_text` provides
text operations. Change them only when a reusable engine/SDK requirement exists,
not to host an API client or runner gateway.

### 4.6 PyO3 boundary

`rust/py/src/*.rs` maps native async handles, environments, target actions,
inspection, deadlines, values, and errors into Python:

| Files | Boundary |
|---|---|
| `lib.rs`, `prelude.rs`, `runtime.rs`, `profile.rs` | Extension composition, shared types, Tokio/Python runtime/profile |
| `app.rs`, `environment.rs`, `component.rs`, `context.rs` | App/environment/component lifecycles and context |
| `function.rs`, `code.rs`, `logic_registry.rs`, `memo_fingerprint.rs`, `fingerprint.rs` | Callable/code identity, registry, and memo fingerprints |
| `target_state.rs`, `stable_path.rs`, `value.rs` | Target/action keys, stable paths, cross-language values |
| `deadline.rs`, `live_component.rs`, `batching.rs`, `ratelimit.rs`, `rwlock.rs` | Lifecycle/concurrency/backpressure helpers |
| `inspect.rs`, `ops.rs` | Read-only inspection and native operations |

When a native snapshot or observation API is added:

1. Implement the async core operation first.
2. Add the smallest PyO3 wrapper.
3. Update [`python/synor/_internal/core.pyi`](../../python/synor/_internal/core.pyi)
   in the same change.
4. Add a user-facing Python integration test.
5. Rebuild with `uv run maturin develop` before Python tests.

Do not add HTTP, API keys, organization IDs, or cloud database code to the PyO3
bridge.

### 4.7 Rust SDK

`rust/sdk/synor/` is a first-class SDK sharing the Rust engine without PyO3.
Cloud contracts must therefore be language-neutral even if the first managed
runtime supports Python pipelines. Do not encode Python pickles or Python class
names into runner-control protocols.

The Rust SDK already documents that separate environments/DB paths may coexist
for isolation. That is useful for tests and dedicated tenants, but it is not a
distributed tenancy mechanism.

### 4.8 Tests and documentation anchors

| Area | Existing anchors | New cloud-related coverage |
|---|---|---|
| Core lifecycle | `python/tests/core/`, Rust core tests | No-op compatibility, cancellation, stale lease, snapshot/restore |
| Control plane | `test_execution.py`, `test_phase3_execution.py`, `test_state_store.py` | Event sink outage, evidence spool, schema compatibility |
| Packages | `test_pipeline_packaging.py` | v1/v2 compatibility, tamper, build manifest, path safety |
| Revocation | `python/tests/revocation/`, Qdrant tests | Cloud mirror failure, restore/drift, transactional backend contract |
| Connectors | `python/tests/connectors/` | Read-only inspector contracts and failure injection |
| CLI | `python/tests/cli/` | Optional cloud handoff only after client contract stabilizes |
| Docs | controlled/trustworthy/revocation guides | Cloud guide and explicit guarantee matrix |
| Benchmarks | `benchmarks/`, validation records | snapshot duration/size, evidence throughput, runner overhead |

---

## 5. Target system architecture

### 5.1 Logical control-plane components

Start as a modular monolith plus workers, not a fleet of microservices:

| Module/process | Responsibility | Must not do |
|---|---|---|
| Public API | Authentication, authorization, validation, CRUD, idempotency | Execute user pipelines |
| Web console | Projects, deployments, plans, runs, runners, evidence, billing | Receive raw connector secrets in browser logs |
| Scheduler worker | Due schedules, run creation, outbox publication | Assume queue delivery is exactly once |
| Runner gateway | Enrollment, heartbeat, long-poll claim, lease renewal, completion | Grant cross-tenant work |
| Build worker | Isolated dependency resolution, image build, SBOM, signing | Receive runtime source/target credentials |
| Evidence worker | Validate allowed schemas, persist indexes and bundles | Deserialize arbitrary Python objects |
| Usage worker | Immutable internal usage ledger and billing export | Make Stripe the primary usage database |
| Notification worker | Email/webhook/chat delivery with retries | Change run truth based on notification success |

Reference infrastructure:

- Clerk for browser sign-in, session verification, Organizations, enterprise
  federation/Directory Sync, and organization-scoped customer API keys.
- Neon PostgreSQL for transactional Synor control-plane data, RLS, migrations,
  outbox, leases, audit metadata, and usage events.
- Object storage for packages, evidence bundles, reports, logs, and snapshots.
- OCI registry for immutable execution images.
- Durable queue for outbox events and workers.
- Managed secret store plus KMS for hosted credentials.
- OpenTelemetry collector/gateway for traces, metrics, and logs.

AWS can be the first compute implementation using S3, ECR, SQS, Secrets
Manager/KMS, and ECS/EKS, with the control services connecting to a Neon
project in the matching AWS region. Use Neon Private Networking/AWS
PrivateLink for production when the selected Neon plan and region support it;
otherwise use TLS plus strict IP Allow while private connectivity is being
qualified. Keep service interfaces provider-neutral so another execution
provider does not require engine changes.

### 5.2 Customer-hosted data plane

The customer-hosted runner is the first production data plane because it:

- keeps connector credentials inside the customer boundary;
- keeps LMDB beside the workload;
- supports private endpoints and on-prem systems;
- lowers the hosted service's data-processing scope; and
- fits Synor's local-first differentiation.

Use Kubernetes as the reference enterprise installation:

1. An agent/controller runs with a namespace-scoped service account.
2. It makes outbound HTTPS connections to the runner gateway.
3. It creates one restricted Job per execution attempt.
4. The Job mounts one deployment-specific persistent state volume.
5. Connector credentials arrive through the customer's secret mechanism.
6. NetworkPolicy limits destinations to the declared connector/model endpoints
   and the Synor control plane.
7. The job writes evidence to a local spool; the agent uploads allowed records.
8. The controller cannot read jobs or secrets outside its runner namespace.

Kubernetes workloads SHOULD meet the current Restricted Pod Security Standard:
non-root, no privilege escalation, dropped capabilities, read-only root
filesystem where possible, seccomp, bounded resources, and no host namespace or
host-path access. See the
[Kubernetes Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/).

A Docker/VM installer MAY exist for trials, but mounting an unrestricted Docker
socket is root-equivalent. Do not describe that mode as the hardened enterprise
deployment.

### 5.3 Managed data plane

Managed execution is a later, separate trust tier:

- one isolated task or pod per attempt;
- no shared Python interpreter between organizations;
- one persistent encrypted block volume per deployment/environment;
- one active LMDB writer per deployment;
- private subnet and per-task network policy;
- just-in-time secret access limited to the task identity;
- no inbound public network path to the task;
- immutable image digest and signature verification; and
- forced termination after deadline plus Synor cancellation/quiescence handling.

AWS ECS/Fargate gives each task its own network interface and permits private
network controls and flow logging. EBS can provide encrypted durable block
storage for transaction-intensive tasks. See
[Fargate task networking](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-task-networking.html)
and
[EBS volumes for ECS](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ebs-volumes.html).

Do not use a shared network filesystem for mutable LMDB state until its exact
locking, mmap, durability, failover, and performance behavior is certified on
every supported platform. A local block volume with explicit snapshots is the
reference design.

### 5.4 Regional cells

The eventual enterprise topology is cell-based:

```text
Global identity / commercial metadata (minimal)
                 |
      +----------+----------+
      |                     |
  US control cell       EU control cell
  Postgres/object       Postgres/object
  runners/builders      runners/builders
```

An organization is pinned to one data region. Run metadata, evidence, secrets,
and managed state stay in that cell. Cross-cell failover is an explicit restore
operation, not concurrent active/active execution.

---

## 6. Tenancy and control-plane data model

### 6.1 Neon deployment and connection model

Neon is the selected provider for the control-plane PostgreSQL database. It is
not used for Synor's per-environment LMDB engine state, customer source/target
databases, artifact storage, or the durable work queue.

Initial topology:

```text
Clerk production instance
          |
          v
Synor API/workers in one regional cell
          |
          +-- pooled TLS/PrivateLink --> Neon production project/branch
          +-- queue/object/registry --> regional managed services

Clerk development instance
          |
          v
development/staging/PR services
          +-- isolated Neon projects or sanitized/schema-only branches
```

Requirements:

1. Use separate Clerk instances and separate Neon projects/accounts or
   equivalently strong boundaries for development, staging, and production.
   A database branch is convenient isolation, but it is not the primary
   production-versus-development security boundary.
2. Pin each production regional cell to a Neon project in the selected region.
   Do not place a tenant in a region until Clerk, Neon, object storage, logs,
   backups, and runner metadata satisfy its residency contract.
3. Mark the production Neon branch protected. Enable IP Allow immediately and
   Private Networking when available for the selected contracted plan/region.
4. Give API and workers a pooled connection URL. Neon uses PgBouncer in
   transaction mode, so all tenant work must run inside one explicit database
   transaction and must not rely on session state, `LISTEN`, session advisory
   locks, or holdable cursors.
5. Give only the migration/backup job a direct, non-pooled URL. Alembic,
   `pg_dump`, administrative DDL, and any operation requiring session semantics
   use this path. The direct credential never enters the API or worker runtime.
6. Establish the tenant context at the beginning of the same transaction with
   `SELECT set_config('app.organization_id', :organization_id, true)`. The
   `true` argument makes it transaction-local. RLS policies default-deny when
   that value is missing or invalid. Test this exact pattern through Neon's
   pooled endpoint before production.
7. Use distinct PostgreSQL roles: a bootstrap/database owner, `synor_migrator`,
   `synor_api`, `synor_worker`, `synor_readonly`, and backup/restore automation.
   Runtime roles are neither relation owners nor `SUPERUSER`/`BYPASSRLS` and
   receive only the required table/function privileges.
8. Disable scale-to-zero for the production primary when the API/claim-latency
   SLO cannot tolerate a cold start. Development, preview, and low-traffic
   staging computes may scale to zero. Every client reconnects with bounded
   jitter because compute setting changes/restarts can drop connections.
9. Use Neon branches for migration rehearsal and ephemeral integration tests.
   Create them from a schema-only or approved anonymized base. Never clone raw
   production customer data into a developer/PR environment by default.
10. Do not enable Neon Auth, the Neon Data API, or direct browser-to-database
    access for this control plane. Clerk authenticates clients; the Synor API
    authorizes domain actions; standard PostgreSQL RLS is defense in depth.

Use two separately stored connection secrets:

```text
NEON_DATABASE_URL_POOLED   # API/workers; host includes the Neon pooler endpoint
NEON_DATABASE_URL_DIRECT   # migration, pg_dump, controlled administration only
```

Neon's pooler runs in transaction mode and recommends a direct URL for schema
migrations and `pg_dump`; see
[Neon connection pooling](https://neon.com/docs/connect/connection-pooling).
Production branch controls are documented in
[Neon protected branches](https://neon.com/docs/guides/protected-branches), and
the network boundary is described in the
[Neon security overview](https://neon.com/docs/security/security-overview).

### 6.2 Tenant rule

Every customer-owned row MUST carry `organization_id` directly, even when it
can be derived through another table. This makes authorization review,
partitioning, deletion, audit, and row-level security explicit.

Use PostgreSQL row-level security as defense in depth. PostgreSQL applies a
default-deny policy when RLS is enabled and no applicable policy exists, but
table owners normally bypass RLS. Therefore:

- migrations use a separate owner role;
- the API role is not owner, superuser, or `BYPASSRLS`;
- `FORCE ROW LEVEL SECURITY` is considered for tenant tables;
- the organization context is set transaction-locally, never session-globally
  on a pooled connection; and
- every service-layer query still performs an explicit authorization check.

See the
[PostgreSQL row-security documentation](https://www.postgresql.org/docs/current/ddl-rowsecurity.html).

This design deliberately uses ordinary server-side PostgreSQL RLS rather than
Neon Data API/Neon RLS JWT access. Clerk session/API-key credentials never
become database credentials, and browsers/runners never connect directly to the
control database.

### 6.3 Initial schema

| Table | Essential fields/invariants |
|---|---|
| `organizations` | Internal ID, unique Clerk Organization ID, name/slug projection, home region, status, retention policy, created/deleted timestamps |
| `users` | Internal ID, unique Clerk user ID, minimal display projection, status; no password/session-token storage |
| `memberships` | Clerk Organization membership projection and sync version/status; not the sole high-risk authorization authority |
| `role_bindings` | Internal user/service-account principal, organization, optional project, product role, source and version |
| `projects` | Organization, name, slug, status; unique per organization |
| `environments` | Organization, project, name, region, execution mode, policy version |
| `service_accounts` | Organization/project scope, display name, status |
| `api_keys` | Local binding from Clerk API-key ID to service account, Clerk Organization ID, required scopes/claims version, project restrictions, status, expiry, last-used, revoke state; never plaintext secret or custom digest |
| `clerk_webhook_events` | Clerk event ID, type, source timestamp/version, processing state and digest; unique event ID for idempotency |
| `runner_pools` | Organization/environment, mode, scheduling labels, required capabilities |
| `runners` | Pool, workload identity, protocol range, engine versions, capabilities, status, last heartbeat, revocation generation |
| `pipelines` | Organization/project, stable name, source repository metadata, status |
| `deployment_versions` | Pipeline, immutable package/image digest, entrypoint, Synor version, build status, SBOM/attestation refs |
| `schedules` | Deployment/environment, cron/timezone, enabled, concurrency policy, next-fire timestamp |
| `plans` | Deployment, policy/config versions, action digest, source cursor/snapshot, status, expiry |
| `plan_approvals` | Plan, approver identity, decision, reason code, immutable timestamp |
| `runs` | Requested operation and desired deployment; one logical customer request |
| `run_attempts` | Runner lease, fencing generation, state transitions, engine result, timestamps |
| `run_events` | Immutable ordered safe events; unique `(attempt_id, sequence)` |
| `artifacts` | Object key, digest, size, media type, encryption/retention metadata |
| `audit_events` | Actor, action, tenant/resource, outcome, request ID, safe metadata |
| `usage_events` | Organization, metric, quantity, source event ID, occurred timestamp; immutable and idempotent |
| `subscriptions` | Billing customer/subscription refs, plan, status; no card data |
| `webhook_endpoints` | Encrypted secret reference, event filters, status |
| `webhook_deliveries` | Event, attempt, status, response class/digest, retry timestamp |

Use foreign keys and unique constraints for correctness, but do not rely on
foreign-key behavior as a tenant-authorization boundary. Avoid broad cascading
deletes on evidence. Organization deletion is an explicit, restartable
retention workflow with a manifest of what was deleted or retained.

Clerk owns passwordless/password/OAuth factors, sessions, external Organization
membership, and the validity of Clerk-issued API keys. Neon owns Synor's
internal IDs, project/environment model, service accounts, project-scoped role
bindings, approvals, audit, and the local allow/deny state for a Clerk key. A
Clerk identifier is always mapped through a unique typed column; email and slug
are never authorization keys.

### 6.4 Object storage layout

Use opaque IDs, content digests, and tenant-scoped authorization:

```text
org/<organization-id>/
  packages/<sha256>/source.synor
  images/<deployment-id>/attestations/...
  runs/<run-id>/attempts/<attempt-id>/...
  evidence/<bundle-id>/...
  state-snapshots/<deployment-id>/<snapshot-id>/...
```

Object keys must not include customer document paths, emails, database URLs, or
provider-native identifiers. Every database artifact row records digest, size,
media type, tenant, retention policy, and object version where available.

For high-assurance evidence, an optional WORM tier can use object versioning and
retention controls. AWS S3 Object Lock implements versioned write-once-read-many
retention, including governance and compliance modes. Enabling it is an
operator/legal-policy decision, not a default claim. See
[S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html).

---

## 7. Identity, API keys, authorization, and secrets

Identity is split by actor type. Human sessions, CI clients, runners, and
internal workloads have different compromise and rotation properties; they
must not share one credential mechanism.

### 7.1 Human identity

Use Clerk for browser sign-in, account recovery/MFA, sessions, Organizations,
invitations, and enterprise SSO/Directory Sync. The console uses the Clerk
React SDK/components where they fit the product design. The FastAPI backend
uses Clerk's official `clerk-backend-api` package and `authenticate_request()`;
do not implement Clerk JWT parsing or password/session storage in Synor.

Configure separate Clerk applications for development, staging, and production
with separate values for:

```text
CLERK_PUBLISHABLE_KEY
CLERK_SECRET_KEY
CLERK_JWT_KEY
CLERK_WEBHOOK_SIGNING_SECRET
Clerk issuer, audience, authorized parties, and API version
```

The publishable key may reach the browser. The secret key and webhook signing
secret remain server-only. Pin and test the Clerk Backend API version. Use the
current session-token claim version through the SDK rather than manually
decoding its compact Organization claim.

Clerk Organizations are the external human-membership boundary. Every Synor
organization has one unique `clerk_organization_id`; every user projection has
one unique `clerk_user_id`. An authenticated request must have an active Clerk
Organization and map it to the requested internal organization. Never map by
email, domain, slug, or a client-supplied organization header.

Organization provisioning is a retryable saga:

1. Clerk creates or returns the Organization and first membership.
2. Synor transactionally upserts the internal organization/mapping and creator
   role binding by Clerk IDs.
3. Failure leaves a Clerk Organization in `provisioning` from Synor's
   perspective; retry repairs it. It never grants access to unrelated internal
   rows.
4. Organization deletion enters Synor's retention state machine first. A Clerk
   deletion webhook never cascades directly into destructive data deletion.

The initial human roles are deliberately small:

| Role | Allowed actions |
|---|---|
| `owner` | Organization settings, identity federation, all projects, destructive organization workflows |
| `admin` | Members, service accounts, runners, projects, deployments, policies |
| `deployer` | Packages, deployments, plans, schedules, plan approval when policy permits |
| `operator` | Start/cancel runs, inspect evidence, acknowledge alerts; cannot change deployment content |
| `auditor` | Read-only plans, runs, evidence, audit log, and exports |
| `billing_admin` | Subscription, invoices, usage; no pipeline or credential access |

Clerk owns active Organization membership and a coarse Organization role.
Synor owns the product roles above, project-scoped bindings, approval
separation, support grants, and every resource authorization decision in Neon.
Clerk custom roles/permissions may mirror coarse Synor organization roles for
UI and enterprise group mapping, but a session claim alone never grants access
to a project or high-risk action. Authorization evaluates both the verified
Clerk principal/membership and the narrowest Synor role binding.

Model identity explicitly:

```text
identity = (clerk_instance, clerk_user_id)
organization = (internal_organization_id, clerk_organization_id)
membership = Clerk authority + Neon projection/version
role_binding = (internal organization, optional project, principal, product role)
```

Clerk webhooks reconcile user, Organization, and membership projections. The
webhook endpoint is public but verifies the Clerk/Svix signature over the raw
request before parsing, deduplicates by event ID, tolerates retries and
out-of-order delivery, and stores only fields Synor needs. Webhooks are
asynchronous and are never a synchronous onboarding prerequisite. For plan
approval, tenant deletion, support elevation, and other high-risk actions,
confirm current Clerk membership/role through the backend API or a sufficiently
fresh verified session under an explicit policy; fail closed when that check is
unavailable.

Clerk Enterprise Connections provide organization-scoped SAML/OIDC. Clerk
Directory Sync provides SCIM provisioning/deprovisioning and group-to-role
mapping. Enable them in the enterprise phase, disable JIT provisioning when a
customer requires pre-provisioning only, and reconcile resulting role changes
to Synor bindings with an auditable policy.

See [Clerk session tokens](https://clerk.com/docs/guides/sessions/session-tokens),
[Clerk Organizations](https://clerk.com/docs/guides/organizations/create-and-manage),
[Clerk webhooks](https://clerk.com/docs/guides/development/webhooks/overview),
and [Clerk Directory Sync](https://clerk.com/docs/guides/configure/auth-strategies/enterprise-connections/directory-sync).
Follow the underlying OAuth security best practice in
[RFC 9700](https://datatracker.ietf.org/doc/html/rfc9700).

### 7.2 Service accounts and API keys

Use Clerk's **Organization API keys** for developer/CI authentication. Disable
user-scoped API keys for Synor's production API. A Synor service account still
belongs to exactly one internal organization and carries project/environment
restrictions; one or more Clerk Organization API keys bind to it through a
local Neon record.

Do not promise or parse a Synor-defined secret prefix. Clerk owns token format,
secret generation, verification, expiry, and remote revocation. Synor owns the
service account, effective scopes, project restrictions, local disabled state,
audit, and product authorization.

Use a custom Synor key-management screen/API, not the generic Clerk profile key
UI. Hiding Clerk's UI does not by itself prevent frontend-created keys. Synor
accepts only a Clerk Organization API key that has all of:

- a valid Clerk verification result;
- the expected production Clerk instance and Organization subject;
- a backend-issued schema/version claim;
- a matching active local key binding and service account in Neon; and
- sufficient Clerk scopes **and** effective local restrictions for the route.

The local table stores:

```text
api_key_id                 internal ID
clerk_api_key_id           unique external credential ID
clerk_organization_id      must map to organization_id
service_account_id
organization_id
display_name / description
required_claims_version
scopes_snapshot            normalized and bounded
project_restrictions
expires_at
last_used_at               sampled asynchronously
status                     provisioning | active | revocation_pending | revoked
created_by / created_at
revoked_by / revoked_at / revocation_reason
```

No plaintext API-key secret or custom HMAC digest is stored in Neon, logs,
analytics, or support tooling.

Creation is a compensating workflow because Clerk and Neon cannot share a
transaction:

1. Authorize the human/service-account management action in Synor and insert a
   local `provisioning` record with an idempotency key.
2. Ask Clerk to create an Organization API key with the Clerk Organization as
   subject, normalized Synor scopes, required Synor claims, `createdBy`, and an
   explicit expiry unless policy permits otherwise.
3. Store the returned Clerk key ID and metadata, activate the binding, and
   commit audit/outbox state in Neon.
4. Return Clerk's secret exactly once only after the local commit succeeds.
5. If local activation fails after Clerk creation, revoke the Clerk key and let
   a reconciliation worker revoke any backend-marked Clerk key with no active
   local binding. Never attempt to recover/show its secret.

Request authentication is:

1. Parse one Bearer credential without logging it.
2. Use the Clerk backend SDK/request authenticator and require token type
   `api_key`; opaque API-key verification fails closed when Clerk is unavailable.
3. Validate Clerk key ID, Organization subject, scopes, claims version, expiry,
   and revoked state.
4. Resolve `clerk_organization_id` to the internal organization and load the
   local key/service-account binding in the same tenant-safe Neon path.
5. Reject provisioning, locally disabled, revocation-pending, expired,
   out-of-project, or out-of-scope bindings even if Clerk says the key is valid.
6. Run the normal centralized product authorization check.

Operational requirements:

1. Enable Clerk Organization API keys only in the intended Clerk instance.
2. Require backend-issued claims/local registration; reject keys created
   through an uncontrolled frontend flow.
3. Return the complete key exactly once over TLS; never provide a Synor secret
   recovery endpoint.
4. Default to expiry; require a recorded policy exception for a non-expiring
   credential even though Clerk permits one.
5. Redact `Authorization`, cookies, Clerk secrets, enrollment tokens, webhook
   secrets, and
   presigned URLs at the first logging boundary.
6. Rate-limit by Clerk key ID, organization, route class, and source network.
   Authentication failures get a separate low ceiling.
7. Support overlapping keys so customers can rotate without downtime. Show
   sampled last-use and make the local deny effective before calling Clerk.
8. Revoke by transactionally marking `revocation_pending` first, so Synor
   rejects immediately; call Clerk idempotently; then mark `revoked`. Reconcile
   failures until both systems agree.
9. Verify opaque keys with Clerk on each request initially. Do not introduce a
   positive cache that breaks the revocation SLO without a reviewed bounded
   policy and purge path.
10. Monitor Clerk verification latency, errors, usage cost, and local/remote
    drift without recording secrets or key prefixes in metric labels.

Initial service-account scopes:

| Scope | Capability |
|---|---|
| `projects:read` | Read project and environment metadata |
| `deployments:read` / `deployments:write` | Inspect or create immutable deployments |
| `plans:read` / `plans:create` / `plans:approve` | Separate planning from approval |
| `runs:read` / `runs:create` / `runs:cancel` | Operate runs without changing deployments |
| `evidence:read` | Read sanitized evidence; deliberately separate from run status |
| `runners:read` / `runners:manage` | Fleet inventory or enrollment/revocation |
| `audit:read` | Audit export |
| `integrity:read` / `integrity:repair` | Separate observation from mutation |

Do not start with wildcard permissions in the CLI examples. A UI-created key
must show the exact effective scope and resources before confirmation.

Clerk documents Organization subjects, scopes, optional claims, one-time secret
return, verification, and revocation in
[Using API keys](https://clerk.com/docs/guides/development/machine-auth/api-keys).
Clerk M2M tokens are not used for customer API access or runner identity in the
initial architecture; cloud service-to-service traffic uses cloud workload
identity, and runners use the mTLS design below.

### 7.3 Runner enrollment and workload identity

A runner is not given a user API key. Enrollment is a bootstrap exchange:

1. An administrator creates a one-use enrollment token scoped to an
   organization, project/environment, runner pool, and allowed labels.
2. The cloud stores only its digest. The token expires in at most 15 minutes.
3. The runner generates its private key locally and sends a CSR plus runner
   metadata over TLS.
4. The gateway consumes the token atomically and issues a short-lived workload
   certificate bound to `runner_id`, organization, pool, and protocol version.
5. Subsequent control traffic uses mutual TLS. The private key never leaves
   the runner.
6. The runner renews well before expiry. Revocation disables new leases and
   terminates the next heartbeat/claim operation.
7. Re-enrollment creates a new runner identity; it does not resurrect a
   revoked private key.

Certificate lifetime should begin at 24 hours or less and shorten after
rotation behavior is proven. For a larger service mesh, adopt SPIFFE-compatible
identities rather than inventing a second workload format; see
[SPIFFE concepts](https://spiffe.io/docs/latest/spiffe/concepts/) and the
[X.509-SVID specification](https://spiffe.io/docs/latest/spiffe-specs/x509-svid/).

Every claimed attempt receives a narrower, short-lived attempt token and
presigned artifact URLs. A runner certificate by itself must not grant access
to every package or organization object.

### 7.4 Connector credentials and secret references

The first customer-hosted runner release keeps connector credentials in the
customer environment:

```yaml
secrets:
  source_database:
    provider: kubernetes
    reference: namespace/name#dsn
  qdrant:
    provider: vault
    reference: secret/data/synor/qdrant#api_key
```

The cloud receives the logical secret name and provider type, never the value.
The runner resolves the reference immediately before starting an attempt and
injects it through a connector-specific or process-local mechanism. Values
must not be serialized into deployment packages, plan records, environment
manifests, exception details, or evidence.

Managed execution may store encrypted secrets in a cloud secret manager. Use
envelope encryption with separate authorization to decrypt and rotate. The
runtime fetches by immutable secret version, places it in memory or an
ephemeral file, and destroys the task after use. AWS Secrets Manager documents
its KMS envelope-encryption model in
[Secret encryption and decryption](https://docs.aws.amazon.com/secretsmanager/latest/userguide/security-encryption.html).

Clerk and Neon credentials are control-plane secrets, not connector secrets.
The API may receive the Clerk secret/JWT verification key and pooled Neon URL;
only migration/backup automation receives the direct Neon URL. Build workers,
customer runners, and pipeline containers receive none of them. Rotate Clerk
webhook/API credentials and Neon role passwords through a staged dual-secret
deployment with connection drain and explicit verification.

Secret handling follows these operational rules:

- redact by key name and value fingerprint before log/event serialization;
- never put secrets in command-line arguments, labels, trace attributes, or
  URLs;
- rotate without rebuilding package content;
- record reference/version use, not secret value, in audit evidence;
- make break-glass reads exceptional, time-bound, approved, and audited;
- test crash dumps, support bundles, and error pages for secret leakage.

The broader rotation and least-privilege guidance should track the
[OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html).

### 7.5 Authorization implementation rule

Authorization belongs in one typed service boundary shared by HTTP handlers,
background workers, and support tooling:

```text
authorize(
    principal,
    action,
    organization_id,
    project_id?,
    resource_id?,
    resource_attributes,
) -> decision + policy_version + reason
```

Each sensitive audit event records the decision's policy version and reason.
Never put ad-hoc `if role == ...` branches throughout route handlers. Add table
tests for every `(role, action, scope)` combination and negative cross-tenant
tests for every object route.

---

## 8. Public API and runner protocol

### 8.1 API conventions

The external API is HTTPS JSON under `/v1`. Define it in OpenAPI first and
generate request/response clients and conformance tests from the same contract;
the authoritative format is the current
[OpenAPI Specification](https://spec.openapis.org/oas/).

The authentication middleware accepts either a verified Clerk human session
token or a Clerk Organization API key and records the Clerk token type in the
typed principal. OAuth, M2M, user-scoped API-key, testing, and sign-in tokens are
rejected unless a future route explicitly opts into that token type. A valid
Clerk credential proves identity; the route still performs Synor resource
authorization using the Clerk-to-internal mapping and Neon role bindings.

Rules:

- Resource IDs are opaque UUIDv7/ULID-style identifiers with explicit resource
  types in response objects.
- Timestamps are UTC RFC 3339 with subsecond precision.
- Money uses an ISO currency and integer minor units; usage quantities use
  integer base units.
- Create/mutate endpoints accept an `Idempotency-Key`. The stored key is scoped
  to actor, organization, method, route, and a canonical request digest.
- Reusing a key with a different request digest returns `409`.
- Long operations return `202` and an operation/run resource rather than hold
  an HTTP request open.
- List routes use opaque cursor pagination and a deterministic `(created_at,
  id)` order. Never use offset pagination for event streams.
- Updates use an explicit version/ETag and reject stale writes with `412`.
- Error bodies use `application/problem+json` with stable machine codes based
  on [RFC 9457](https://datatracker.ietf.org/doc/html/rfc9457).
- Every response carries `X-Request-Id`; accept a valid client request ID but
  generate a trusted server trace ID separately.
- Version evolution is additive within `/v1`. Breaking changes require a new
  version and an overlap window.
- Never expose Python exception strings as an API contract.

Minimum public surface:

| Method and route | Purpose |
|---|---|
| `POST /v1/organizations` | Create organization during controlled signup |
| `GET /v1/organizations/{id}` | Organization metadata and entitlements |
| `POST /v1/service-accounts` | Create machine principal |
| `POST /v1/service-accounts/{id}/keys` | Backend-create a bound Clerk Organization API key and reveal its secret once |
| `DELETE /v1/api-keys/{id}` | Locally deny, then reconcile Clerk revocation |
| `POST /v1/projects` | Create project and default environment |
| `POST /v1/packages` | Initiate upload; returns scoped upload URL |
| `POST /v1/deployments` | Build/register immutable package and configuration |
| `POST /v1/deployments/{id}/plans` | Generate a plan |
| `POST /v1/plans/{id}/approvals` | Approve/reject the exact action digest |
| `POST /v1/runs` | Start an approved deployment/operation |
| `POST /v1/runs/{id}/cancel` | Request cooperative cancellation |
| `GET /v1/runs/{id}` | Logical run and current attempt status |
| `GET /v1/runs/{id}/events` | Cursor-paginated safe event stream |
| `GET /v1/runs/{id}/evidence` | Authorized evidence manifest |
| `POST /v1/runner-enrollments` | Create one-use enrollment token |
| `GET /v1/runners` | Fleet inventory and health |
| `POST /v1/runners/{id}/revoke` | Disable future claims/renewals |
| `POST /v1/schedules` | Register schedule tied to immutable deployment |
| `GET /v1/audit-events` | Cursor-paginated audit export |
| `GET /v1/usage` | Transparent measured usage |

Keep organization scope in the authenticated URL/resource relationship, not a
client-supplied header that silently changes database tenancy. An optional
organization selector is only a lookup convenience and must match the
authorized resource.

### 8.2 Idempotency and transactional outbox

For a mutating request:

1. Acquire a pooled Neon connection and begin an explicit PostgreSQL
   transaction.
2. Establish transaction-local tenant context with `set_config(..., true)`.
3. Lock or insert the idempotency record.
4. Validate the canonical request digest.
5. Apply the state transition.
6. Insert audit, usage candidate, and outbox rows in the same transaction.
7. Commit.
8. A dispatcher publishes outbox messages and marks them delivered
   idempotently.

Consumers deduplicate by immutable event ID. A queue acknowledgement is never
the source of truth for a customer-visible state transition. This prevents the
classic “database committed but job was not sent” and “job was sent but HTTP
returned an error” split.

### 8.3 Runner protocol

Start with outbound long polling over HTTPS because it works through enterprise
proxies and provides a simple failure model. WebSocket or gRPC streaming may
be added later without changing lease semantics.

The runner API is private/versioned separately from the public API:

| Operation | Required behavior |
|---|---|
| `enroll` | One-use bootstrap exchange described above |
| `capabilities` | Report agent/protocol/engine versions, OS/arch, execution modes, connector capability facts, pool labels |
| `claim` | Long poll; atomically allocate one eligible attempt and return lease generation |
| `heartbeat` | Extend lease only if attempt, runner, and generation still match; send bounded progress |
| `artifact-access` | Return attempt-scoped presigned URLs and expected digests |
| `append-events` | Ordered idempotent batches with `(attempt_id, sequence)` |
| `complete` | Compare-and-set terminal result using the current generation |
| `renew-identity` | Rotate workload certificate after policy checks |

Protocol messages include:

```text
protocol_min / protocol_max
runner_id / runner_pool_id / organization_id
attempt_id / lease_generation / lease_expires_at
deployment_digest / image_digest / policy_version
required_capabilities / reported_capabilities
event_sequence / payload_schema_version
```

Compatibility policy:

- control plane supports the current and previous two released agent protocol
  minors;
- an agent advertises a range, never only a marketing version;
- incompatible agents remain healthy but receive no work and show an explicit
  upgrade reason;
- unknown optional fields are ignored, unknown required capabilities fail
  closed;
- schemas have golden fixtures exercised in cloud and agent CI.

### 8.4 Webhooks and notifications

Webhook deliveries are derived from committed outbox events. Sign the exact
body with a per-endpoint secret and include event ID and timestamp. The receiver
can reject stale timestamps and deduplicate event IDs. Retry only network
errors, `408`, `429`, and `5xx` with capped exponential backoff; disable an
endpoint after a documented failure window. Never include raw evidence in a
webhook—send resource IDs and safe summaries.

Email, Slack, PagerDuty, and ticketing integrations are notification adapters,
not part of run correctness. Their failure cannot roll back or change a run.

---

## 9. Immutable deployment and software supply chain

`python/synor/packaging.py` currently creates a useful deterministic source
archive, but it is not yet a production deployment artifact: dependency
resolution is not a fully locked transitive build, the package is not an OCI
runtime image, and there is no package-level SBOM/signature policy.

### 9.1 Deployment pipeline

The cloud deployment path is:

```text
local source
  -> deterministic Synor source package + manifest
  -> digest and upload
  -> isolated builder
  -> full dependency resolution from approved sources
  -> tests/import probe and entry-point inspection
  -> OCI image with pinned base digest
  -> vulnerability and secret scan
  -> SBOM + build provenance
  -> signature
  -> immutable deployment record by digest
  -> runner verifies before execution
```

The source manifest v2 should contain only deterministic, non-secret data:

```text
format_version
app_entry_point
source_file digests and modes
Python and Synor version constraints
supported OS/architecture if constrained
declared connector/runtime capabilities
dependency lock digest
build policy version
```

Do not accept arbitrary Dockerfiles in the first managed release. A controlled
build profile with a small set of pinned base images is easier to secure,
support, and reproduce. Customer-specific native packages require a reviewed
custom build profile, not hidden shell hooks.

### 9.2 Builder isolation

Treat source and dependency build steps as hostile:

- each build gets a fresh ephemeral worker and unprivileged user namespace;
- no control-plane database, cloud metadata service, production secrets, or
  other customer's cache is reachable;
- network access is denied by default, then limited to approved package mirrors
  during dependency fetch;
- download and build are separate stages so hashes can be verified before
  executing build backends;
- resource/time/output limits are enforced by the host, not Python;
- cache entries are content-addressed and never contain credentials;
- build logs pass through secret and PII filters and have a byte limit;
- the worker is destroyed after upload regardless of outcome.

### 9.3 OCI, SBOM, signature, and provenance

Store runtime images by immutable digest and follow the
[OCI image manifest](https://github.com/opencontainers/image-spec/blob/main/manifest.md)
and [descriptor](https://github.com/opencontainers/image-spec/blob/main/descriptor.md)
formats. Produce CycloneDX or SPDX SBOMs for the image and Python dependencies.

The existing GitHub release workflow already generates artifact attestations;
reuse that release identity for official SDK/runner inputs. Build-service
provenance should describe source digest, builder identity, parameters,
materials, and output digest according to
[SLSA provenance](https://slsa.dev/spec/v1.2/provenance). Sign the resulting
image/attestation and verify it with an explicit trusted identity policy;
[Cosign verification](https://docs.sigstore.dev/cosign/verifying/verify/)
documents the verification model.

Verification occurs twice:

1. The control plane validates all attestations before a deployment becomes
   runnable.
2. The runner validates the image digest and signature policy again before
   starting the attempt.

A mutable registry tag may aid humans, but it never appears as the executed
identity. Rollback creates or selects a deployment pointing to an earlier
verified digest; it does not overwrite the current image.

### 9.4 Compatibility and promotion

Deployment promotion from development to staging to production preserves the
package/image digest. Only environment configuration, secret references,
policy, and schedules may differ. If source or dependencies change, it is a new
deployment.

Before promotion, evaluate:

- agent and engine version range;
- required connector capabilities;
- native LMDB schema compatibility;
- environment policy and egress profile;
- build scan thresholds;
- whether the plan must be regenerated in the target environment.

Never copy an LMDB state directory as part of image promotion. Deployment code
and per-environment engine state have different lifecycles.

---

## 10. Execution, planning, leases, and retry semantics

### 10.1 Run and attempt are distinct

A **run** is the customer's logical request. An **attempt** is one allocation of
that request to a runner. This distinction lets the platform retry dispatch
without rewriting history or pretending the first attempt never existed.

Run states:

```text
created -> awaiting_approval -> queued -> running
        -> succeeded | failed | cancelled | expired | blocked
```

Attempt states:

```text
pending -> leased -> starting -> executing -> uploading_evidence
        -> succeeded | failed | cancelled | lease_lost | infrastructure_lost
```

Only the transactional state machine may move these values. Every transition
has an allowed-from set, actor, reason, timestamp, version, and audit event.
Terminal states are immutable except for a separately recorded administrative
annotation.

### 10.2 Plan/apply contract

The existing `SynorRuntime.plan()` and `run()` boundary is the required engine
seam. A cloud plan record binds:

```text
organization/project/environment/deployment
package and image digest
engine and agent version
configuration digest and secret-reference versions
policy and connector-capability snapshot versions
source snapshot/cursor/fingerprint where available
native action digest returned by Synor
safe count/summary fields
created_at and expires_at
```

Approval signs this exact tuple, not merely a deployment name. Apply must fail
closed and require a new plan when any bound field changes, the plan expires,
or the source connector reports that its consistency token is no longer valid.
The runner calls native apply/reconciliation; the service must not reconstruct
an action list from the summary.

Default production policy requires two different principals for high-risk
repair or deletion. Self-approval may be allowed in development and low-risk
tiers but must be an explicit environment policy visible in evidence.

### 10.3 Lease and fencing protocol

Queue delivery is at least once. Correctness comes from a database lease and
generation fence:

1. Scheduler creates a pending attempt through the transactional outbox.
2. An eligible runner claims it with `SELECT ... FOR UPDATE SKIP LOCKED` or an
   equivalent atomic operation.
3. Claim increments `lease_generation`, stores `runner_id`, and sets expiry.
4. Heartbeat extends only when attempt, runner, generation, and non-terminal
   state match.
5. Completion uses the same compare-and-set. A stale runner cannot overwrite a
   newer attempt.
6. Lease expiry marks the attempt `lease_lost`; policy decides whether a new
   attempt is safe.

The generation is also passed into the runner supervisor and state-volume lock
record. It does not replace the Rust app's existing OS file lease; the two
layers protect different boundaries:

```text
cloud generation: prevents stale remote ownership
Rust/LMDB app lease: prevents concurrent local app writers
```

Exactly one active writer may open a deployment/environment LMDB state volume.
Read-only inspection uses an engine-supported consistent snapshot or read
transaction, never an unsafely copied live directory.

### 10.4 Retry policy comes from capabilities

The platform must consume the factual connector capability inventory already
represented in the repository. It must not label every failed update
retryable.

| Failure point | Default retry decision |
|---|---|
| Before runner claim | Safe to redispatch logical work |
| Claimed but process never started | Safe only after lease expiry/fence |
| Build/download failure before engine start | Retry infrastructure failures; do not retry deterministic validation failures |
| Preview/read-only inspection | Retry when source connector declares repeatable/read-safe behavior |
| Apply before any sink effect | Retry only if engine reports this boundary explicitly |
| Apply after possible sink effect | Manual or connector-certified recovery; never generic retry |
| Evidence upload after engine success | Retry upload idempotently; do not rerun engine |
| Webhook/notification failure | Retry notification independently |

Connector certification must record idempotency keys, partial-commit behavior,
atomicity boundary, retryable error taxonomy, verification read path, delete
semantics, rate limits, and reconciliation limits. Unknown means fail closed or
require operator review.

### 10.5 Cancellation, deadlines, and shutdown

Cancellation is cooperative first:

1. Mark `cancellation_requested_at` in the control plane.
2. Runner receives it on heartbeat and signals the supervised process.
3. Existing Synor deadline/cancellation handling gets a grace window.
4. Runner sends a stronger termination signal after the grace period.
5. Container/task destruction is the final boundary.

The result distinguishes customer cancellation, deadline exceeded,
infrastructure termination, and process crash. A killed apply is not assumed
rolled back. Its environment becomes `needs_inspection` unless the connector
and engine can prove the effect boundary.

### 10.6 Control-plane outage behavior

When the cloud is unreachable, a runner:

- starts no new unleased work;
- may finish the currently valid attempt;
- writes bounded, encrypted, checksummed event batches to a local spool;
- keeps the attempt output/state volume intact;
- retries heartbeats/uploads with jitter without extending its own lease;
- does not infer that an expired lease grants more authority;
- uploads events idempotently when connectivity returns.

If its lease expires during a cloud outage, it must not start an apply phase it
has not already entered. For an apply already in progress, the explicit
environment policy selects `finish_current_operation` or `terminate_on_lease_loss`.
The selected policy is recorded in the plan and evidence.

---

## 11. Index Integrity product boundary

### 11.1 Start with facts, not mutation

The free auditor is read-only. It compares source facts, index facts, and when
available Synor ownership facts:

```text
source inventory + stable source identity + version/fingerprint
index inventory + stable point/document identity + stored provenance fields
Synor LMDB ownership and native-effect facts (optional)
mapping policy + scan watermark
                         |
                         v
missing | stale | orphaned | duplicated | unverifiable | healthy
```

It must be useful against an index Synor did not create. Such a scan is
necessarily less certain: it can report evidence and confidence without
claiming provenance it cannot prove.

### 11.2 New read-only inspector interface

Do not overload `TargetHandler` with arbitrary fleet inspection. Target
handlers represent desired-state reconciliation and may require write
credentials. Add a smaller, separately permissioned internal/public-candidate
protocol only after two real connectors prove it:

```python
class IntegrityInspector(Protocol):
    async def snapshot(self) -> IntegritySnapshot: ...
    async def iter_facts(
        self, *, cursor: IntegrityCursor | None
    ) -> AsyncIterator[IntegrityFactPage]: ...
```

Required properties:

- read-only credential scope can be used;
- pages are bounded and resumable;
- a snapshot/consistency token identifies what was observed;
- provider-native IDs enter as typed values and are hashed/redacted before
  cloud export;
- rate limits and partial permissions become explicit unresolved results;
- no row/document content is fetched unless the connector-specific mapping
  requires it and local policy allows it.

Prove the interface first with one source and one vector target. Recommended
pilot paths:

1. Google Drive to Qdrant, because the repository already has governed-source
   and revocation machinery around that path.
2. S3 to Qdrant, because object version/ETag facts produce a simpler operational
   design.
3. PostgreSQL/pgvector after the fact model and permission design stabilize.

### 11.3 Mapping and confidence

An integrity profile declares how a target record maps to a source identity and
version. Never guess silently. The report records:

```text
profile_version
source_snapshot token and scan bounds
target_snapshot token and scan bounds
mapping rule digest
facts scanned / denied / unresolved
finding kind and stable finding ID
evidence fields used
confidence: proven | strong | heuristic | unverifiable
```

A finding ID is deterministic for `(profile, source/target identity digest,
finding kind, relevant versions)` so repeated scans update history instead of
creating alert storms.

### 11.4 Repair is a separate paid capability

Repair requires:

1. Certified source and target connector capabilities.
2. A current read-only scan.
3. Native Synor plan generation against the same bounded snapshot where the
   connector supports it.
4. Policy classification of creates, updates, and deletes.
5. Approval of the exact plan digest.
6. A fenced runner with write credentials.
7. Post-apply verification and evidence upload.

If target state is unmanaged and Synor lacks historical ownership, the first
repair must use an adoption/import workflow. It cannot interpret every unknown
target record as an orphan and delete it. Adoption writes an explicit baseline
and produces a reviewable ownership manifest.

### 11.5 Connector certification levels

| Level | Meaning | Product permission |
|---|---|---|
| `inventory` | Bounded read-only enumeration and stable fact model tested | Integrity scan |
| `plan` | Source consistency and desired-state mapping tested | Generate plan |
| `apply` | Create/update/delete, idempotency, partial failure, and verification tested | Approval-gated repair |
| `governed` | Revocation markers, retrieval guard, audit/provenance, and negative paths tested | Governed erasure workflow |

Certification is per connector version and operation, not a permanent badge on
the provider name. The cloud scheduler rejects a job whose required level is
not present in the runner's factual capability document.

### 11.6 Scale boundary

Current reconciliation can still require work proportional to all component
state. “Million-document support” is not earned by adding larger runners. The
scale roadmap needs:

- paged source and target cursors with consistency semantics;
- bounded fact batches and spill-to-disk;
- resumable native reconciliation journals;
- explicit high-water marks and generation fences;
- rate-budget-aware scheduling;
- scan partitioning that preserves ownership semantics;
- performance fixtures at 10 thousand, 100 thousand, 1 million, and the target
  enterprise percentile;
- measured memory, LMDB growth, plan latency, apply throughput, and recovery
  time.

Do not market a scale tier until its complete scan, interrupted resume,
incremental update, and restore drill pass under production-like limits.

---

## 12. Evidence, observability, privacy, and audit

### 12.1 Typed evidence boundary

Existing execution, governance, provenance, revocation, PII, quarantine, and
audit modules provide useful local primitives. The cloud must not upload their
arbitrary Python dictionaries directly. Add a strict versioned export schema
whose fields are individually reviewed.

Example event envelope:

```text
schema_version
organization/project/environment/deployment/run/attempt IDs
sequence and occurred_at
event_type and severity
engine/agent/package/policy versions
safe dimensions (connector kind, operation kind, count, duration bucket)
typed payload selected by event_type
payload_digest
previous_event_digest (optional evidence chain)
```

Unknown fields are rejected at the agent export boundary, not “redacted later.”
Set byte, item-count, nesting, and string-length limits before serialization.

### 12.2 Data that leaves the runner

| Data | Default cloud handling |
|---|---|
| Organization/project/run opaque IDs | Allowed |
| Package/image/policy/action digests | Allowed |
| Counts, durations, status, connector type/version | Allowed with cardinality limits |
| Hashed provider-native IDs | Allowed only with a tenant-specific keyed digest |
| Source paths, filenames, table/collection names | Local by default; opt-in masked diagnostic field |
| Exception class and stable Synor error code | Allowed |
| Arbitrary exception message/traceback | Local; filtered support bundle by explicit consent |
| Documents, chunks, embeddings, prompts, generated text | Never in standard telemetry/evidence |
| Database URLs, tokens, cookies, headers, presigned URLs | Never |
| PII detector sample text | Never; category/count only |
| Local state snapshots | Encrypted artifact under separate backup policy, never analytics |

Use a per-tenant keyed digest for sensitive stable identifiers so an operator
cannot rainbow-table common filenames and two organizations cannot be
correlated. Rotation rules must define whether historical matching is retained.

### 12.3 Audit versus run evidence

Keep two records:

- **Control-plane audit:** who changed membership, keys, policy, deployment,
  approval, schedule, runner, billing, or retention.
- **Execution evidence:** what a runner and engine observed/did for a specific
  plan and attempt.

They have different producers, schemas, permissions, and retention. Link by
IDs and digests, but never allow a runner to write a human administrative audit
event. Audit entries are append-only; corrections are new annotation events.

High-assurance exports include a signed manifest of ordered record digests and
artifact digests. An optional WORM object copy strengthens retention, but the
product must state precisely which components are cryptographically signed,
which are access-controlled, and which are externally timestamped.

### 12.4 OpenTelemetry deployment

Instrument API, workers, runner gateway, scheduler, build service, and agent
with OpenTelemetry. Send locally to a collector and then through a regional
gateway; do not embed vendor-specific exporters in core business code. The
[OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) and
[agent-to-gateway deployment](https://opentelemetry.io/docs/collector/deploy/other/agent-to-gateway/)
document this topology.

Required correlation:

```text
request_id -> operation_id -> run_id -> attempt_id -> runner_id
trace_id on transient operations; durable IDs on stored events
```

Telemetry constraints:

- no organization name, email, source path, document ID, Clerk key ID, or
  credential fragment in metric labels;
- fixed low-cardinality status/error/connector dimensions;
- trace sampling is head-based normally and tail-promoted for errors, with
  payload fields still filtered;
- log ingestion has tenant and service rate limits;
- dashboards distinguish customer-code, connector, agent, cloud, and
  infrastructure failure domains;
- observability outage cannot block engine state commits.

### 12.5 Minimum operating dashboards and alerts

Dashboards:

1. API request rate, latency, error class, authorization denials, and Clerk
   session/API-key verification latency/failure.
2. Queue age, claim latency, active/stale leases, attempts by state.
3. Runner connected/compatible/healthy counts and version distribution.
4. Build queue, duration, failure class, scan/signature failures.
5. Plan/apply duration and connector-specific result counts.
6. Event-spool backlog and evidence upload delay.
7. Neon compute/pooler client/server connections, query/queue latency,
   restarts/resumes, storage/history usage, object-store and registry failures.
8. Usage-event lag and billing reconciliation variance.

Page only on symptoms that need timely human action: sustained API unavailability,
oldest runnable job age, widespread lease loss, inability to authenticate
runners, database durability risk, evidence data loss risk, or security alerts.
Customer-code failures create product notifications, not infrastructure pages.

---

## 13. Security architecture and threat model

### 13.1 Trust boundaries

The security review must draw and test these boundaries independently:

1. Browser or CI client to public API.
2. Public edge to authenticated control-plane service.
3. One organization/project to another in application and database layers.
4. Control plane to build worker.
5. Control plane to customer-hosted runner through the runner gateway.
6. Runner supervisor to untrusted customer pipeline process.
7. Pipeline process to connector secrets and customer data systems.
8. Runner to artifact/evidence object storage.
9. Human/support operator to production administrative tools.
10. Software publisher to package, image, agent, and update consumer.

Apply zero-trust principles: network location alone grants no access; every
actor and workload has an authenticated identity, authorization is
resource-specific, and access is continuously bounded. See
[NIST SP 800-207](https://csrc.nist.gov/pubs/sp/800/207/final).

### 13.2 Threat-to-control matrix

| Threat | Preventive controls | Detection and required test |
|---|---|---|
| Cross-tenant object access/BOLA | Verified Clerk Organization mapping, Synor resource authorization, `organization_id` on every row, RLS | Negative test every API route with another tenant's valid Clerk session/key and resource UUID; alert repeated denials |
| API key theft | Clerk opaque Organization keys, one-time display, local binding, narrow scopes, expiry, rate limit | Seed canary credential; verify it never appears in logs/support bundles; local and Clerk revocation-latency test |
| Session theft/CSRF | Clerk short-lived verified session, authorized-party/issuer/audience checks, secure cookies, CSRF/re-auth policy | Browser security suite, wrong-instance/authorized-party and session-replay tests |
| Forged or stale Clerk sync | Signed raw-body webhook verification, event dedupe/versioning, no destructive webhook cascade, live check for high risk | Replay/reorder/forge webhook events and remove membership immediately before approval |
| Clerk outage or account drift | Local JWT verification for valid human sessions, fail-closed opaque key/high-risk verification, reconciliation worker | Inject Clerk latency/outage and compare Clerk Organizations/keys with Neon mappings |
| Runner impersonation | One-use enrollment, local keypair, short-lived mTLS certificate, attempt token | Replay consumed token and revoked certificate; prove claim rejection |
| Stale runner applies work | Lease generation, database compare-and-set, local app lease | Partition runner during apply; start replacement; stale completion must be rejected and mutation policy inspected |
| Malicious deployment code | Isolated build/runtime, no control-plane network, least privilege, immutable verified image | Escape tests, metadata-service test, filesystem/process/network policy tests |
| Build supply-chain compromise | Approved mirrors, hashes/locks, ephemeral builders, SBOM, signed provenance and image | Tamper image/layer/attestation; runner must reject before start |
| SSRF and arbitrary egress | Network policy/proxy allow-list, metadata endpoint block, DNS controls; Python policy only defense in depth | Redirect/DNS-rebinding/IPv6/private-range suite |
| Connector secret leakage | Local secret references, ephemeral injection, first-boundary redaction, no raw exceptions | Fault every connector with token-like values and scan all logs/events/artifacts |
| Evidence tampering | Ordered event IDs, digests, signed manifests, restricted writer identity, optional WORM | Delete/reorder/mutate bundle and verify validation fails |
| Queue replay or duplicate delivery | Idempotency records, run/attempt split, generation fence, deduplicating consumers | Duplicate every message at every transition in integration tests |
| Database-owner or pooled-context bypasses RLS | Separate Neon migration owner; runtime role has neither ownership nor `BYPASSRLS`; transaction-local `set_config` | CI inspects grants; pooled Neon tests alternate tenants and missing context on reused connections |
| Production data copied to Neon preview branch | Protected production branch, schema-only/anonymized base, CI service-account restrictions and branch TTL | Canary production PII scan on every preview branch and audit Neon branch creation/reset events |
| Compromised support account | Just-in-time access, phishing-resistant MFA, approval, session recording, no secret read by default | Quarterly access review and break-glass drill |
| Resource exhaustion | Per-org quotas, bounded payloads/events/logs, build/run CPU-memory-time limits, queue fairness | Oversize/fan-out/load tests; noisy-neighbor test |
| Unsafe deserialization | Versioned JSON/Protobuf-like contracts, no pickle from cloud, strict limits | Fuzz parsers and corpus of malicious payloads |
| Backup exposure | Per-environment encryption, restricted backup role, retention and deletion workflow | Restore with backup-only credentials; cross-tenant restore denial |

OWASP calls object-level authorization the leading API risk; use the
[BOLA guidance](https://owasp.org/API-Security/editions/2023/en/0xa1-broken-object-level-authorization/)
and [REST Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/REST_Security_Cheat_Sheet.html)
as minimum API review checklists.

### 13.3 Managed runtime hardening

Each managed attempt runs in a new task/container with:

- a non-root user and read-only root filesystem;
- dropped Linux capabilities and `no_new_privileges`;
- no privileged mode, host PID/IPC/network namespace, host path, or container
  runtime socket;
- a small writable ephemeral work directory plus exactly one mounted state
  volume when needed;
- seccomp/AppArmor/SELinux defaults appropriate to the platform;
- explicit CPU, memory, PID, file, output, and wall-clock limits;
- default-deny ingress and egress, with connector-specific destinations;
- blocked instance-metadata endpoints;
- task-scoped workload credentials, never node credentials;
- separate tenant tasks and state volumes; no shared Python process;
- image digest and signature verification before launch.

For Kubernetes, the namespace and workload must meet the Restricted
[Pod Security Standard](https://kubernetes.io/docs/concepts/security/pod-security-standards/)
plus provider-specific network and identity controls. For an AWS-first batch
implementation, Fargate provides task isolation and per-task networking; its
[security model](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/security-fargate.html)
and [task networking](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-task-networking.html)
are reference inputs, not substitutes for Synor-level controls.

Customer-hosted Docker mode is trial-only if it mounts the Docker socket; that
socket is effectively root-equivalent on the host. The enterprise reference
runner uses a dedicated Kubernetes namespace/service account or dedicated VM
pool with documented host hardening.

### 13.4 Administrative and support controls

Production access must use company identity, phishing-resistant MFA, short-lived
credentials, and audited just-in-time elevation. Engineers do not query tenant
tables from laptops or receive shared production credentials.

Build explicit support capabilities:

- tenant support grant requested/approved by a customer administrator;
- time-limited scope to named resources;
- default metadata-only view;
- reason/ticket required;
- visible audit event and optional customer notification;
- no API key, runner key, connector secret, document, embedding, or raw state
  access through the normal support UI;
- break-glass path separately approved, alerted, and reviewed.

### 13.5 Security program gates

Before enterprise GA, complete:

1. Data-flow diagrams and data classification register.
2. Threat model reviewed after every major boundary change.
3. Secure development policy, dependency automation, SAST, secret scanning,
   container/IaC scanning, and signed release path.
4. External penetration test with tracked remediation.
5. Incident-response plan and at least one tabletop exercise.
6. Vulnerability disclosure and patch severity/response policy.
7. Vendor/subprocessor register, DPA, privacy policy, retention schedule, and
   customer deletion workflow.
8. Access reviews, production change controls, backup/restore evidence, and
   security awareness evidence suitable for a SOC 2 program.

Do not claim a certification before the external audit/report exists. Describe
implemented controls and roadmap dates precisely.

---

## 14. Reliability, state durability, and disaster recovery

### 14.1 Availability model

The cloud control plane is stateless at the HTTP layer and horizontally
replicated. Clerk is the external identity/key authority, Neon PostgreSQL is
the system of record for Synor control state, the queue is delivery
infrastructure, object storage/registry hold immutable artifacts, and LMDB
remains the per-environment execution state.

Component behavior:

| Component failure | Required behavior |
|---|---|
| API instance | Load balancer retries safe connection failures; idempotency handles ambiguous mutations |
| Clerk | Existing human session JWTs can be verified locally until expiry when configured; new login, membership checks, webhook sync, and opaque API-key verification fail closed or degrade explicitly—never fall back to unverified claims |
| Scheduler worker | Lease/outbox state lets another worker continue; no singleton in-memory cron ownership |
| Queue | Committed jobs remain reconstructable from outbox/database; duplicate delivery is safe |
| Neon compute/proxy | Clients reconnect with bounded jitter after restart/resume; no state transition is acknowledged before commit; production avoids scale-to-zero when latency objectives require it |
| Neon control-plane/service outage | Freeze mutations/claims that require current state, preserve durable queue/outbox/artifacts, and recover through contracted Neon support/restore runbook |
| Object store/registry | New runs/builds pause; existing local artifacts may continue only when digest and policy are already verified |
| Runner gateway | Runners spool bounded events and stop accepting new claims |
| Customer runner | Lease expires; environment enters policy-driven inspection/retry path |
| State volume/node | Restore most recent verified snapshot to a fenced replacement; do not mount simultaneously |
| Telemetry backend | Drop/sample bounded telemetry; never fail an engine commit |
| Billing provider | Retain immutable usage events and reconcile later; never block customer execution solely on transient billing API failure |

### 14.2 Initial service objectives

These are engineering objectives to validate, not customer promises:

| Indicator | Internal objective after beta |
|---|---|
| Authenticated control API availability | 99.9% monthly |
| API read latency | p95 < 300 ms excluding artifact transfer |
| Accepted run to runner claim | p95 < 60 s when an eligible healthy runner has capacity |
| Event visibility after runner receipt | p95 < 30 s |
| API-key local deny effective at public API | p99 < 10 s |
| Clerk API-key revocation converged | p99 objective measured separately; local deny remains authoritative for Synor access |
| Runner revocation effective for new claims | p99 < 30 s |
| Neon control-plane RPO/RTO | RPO <= 5 min, RTO <= 60 min, proven against configured history retention plus independent backup |
| Managed execution-state RPO/RTO | Product-tier specific and proven by state restore drill |

Publish an SLO only after at least 30 days of representative measurement and an
incident/on-call process exists. Exclude neither planned maintenance nor
customer-caused failures silently; define every availability calculation.

### 14.3 LMDB snapshot contract

Never filesystem-copy a live LMDB directory with ordinary recursive copy. The
current native downgrade flow in `rust/core/src/state_store/storage.rs` already
demonstrates the safe shape: acquire the whole-environment operation lease,
prove apps are quiescent, use LMDB's compact snapshot-copy facility, and stage
atomically. General backup needs a separate API that preserves all metadata
rather than stripping native fields.

Required implementation:

1. Add a core `prepare_backup_snapshot(destination)` operation near
   `Storage::prepare_native_downgrade_copy`, reusing path-containment,
   symlink-rejection, environment-lease, map-resize, compact-copy, and staging
   disciplines.
2. Keep the entire operation asynchronous at the engine interface and run the
   blocking LMDB copy through the established blocking boundary.
3. Produce a manifest with format/schema version, environment ID, app list,
   file digest/size, engine version, created time, and fencing generation.
4. Validate the copied environment read-only before publishing the artifact.
5. Encrypt before or during upload and write through a temporary object key;
   publish the database snapshot row only after the final object digest is
   verified.
6. Expose the smallest PyO3/Rust SDK methods and update `core.pyi`.
7. Restore only into a new empty path under an exclusive environment fence;
   validate digest, schema, manifest, and engine compatibility before opening.
8. Never restore over the current directory. Retain the old volume until the
   restored environment passes inspection and a controlled no-op/plan.

AWS EBS application-consistent snapshots require application coordination, as
described in its
[application-consistent snapshot guidance](https://docs.aws.amazon.com/ebs/latest/userguide/automate-app-consistent-backups.html).
A block snapshot alone does not replace the Synor quiescence and LMDB contract.

### 14.4 State-volume ownership

For customer-hosted runners, the customer provisions persistent storage and
the agent verifies:

- stable path is bound to exactly one environment/deployment identity;
- permissions prevent other workload service accounts from mounting it;
- filesystem type and durability meet LMDB requirements;
- available bytes and inode thresholds are monitored;
- no NFS-like backend is assumed compatible without explicit testing;
- backup/restore hooks are configured before production apply.

For managed execution, start with one encrypted block volume per environment,
attached to one task/node at a time. AWS ECS/Fargate EBS integration is
documented in
[Amazon EBS volumes for ECS](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ebs-volumes.html),
but the orchestrator must still own detach/attach fencing and Synor leases.

### 14.5 Backup, restore, and deletion

Backup policy is tiered:

```text
Neon control DB: configured PITR/history retention + independent logical backup
immutable packages/images: registry/object versioning and replication
managed LMDB: snapshot after successful apply plus periodic snapshot policy
customer-hosted LMDB: customer-owned by default; optional encrypted export
audit/evidence: retention policy, optional WORM export
```

Protect the Neon production branch and configure the contractual restore
window. Rehearse recovery by creating/restoring into a separate branch, running
schema and domain invariants, and cutting services over only after validation;
do not destructively reset production during a drill. Take periodic encrypted
logical backups through the **direct** endpoint into an independently
controlled object-store/account so a Neon account/project incident is not the
only recovery path. Neon branches/PITR are operational recovery, not an
independent legal archive.

Every quarter, restore a randomly selected Neon point in time and independent
logical backup into an isolated project/branch, and restore representative LMDB
snapshots across supported schema versions. Measure actual RPO/RTO and inspect
leases, approvals, outbox, audit, native-effect, and revocation state—not only
whether files exist. See
[Neon branching and recovery](https://neon.com/docs/guides/branching-intro).

Customer deletion is a state machine:

```text
requested -> grace_period -> access_disabled -> active_data_deleted
          -> backup_expiry_pending -> completed_with_manifest
```

It enumerates Neon PostgreSQL rows, object versions, registry references, runner
identities, secrets, billing links, telemetry retention, and backups. Legal
holds stop relevant deletion and are visible. The completion manifest records
categories and timestamps without retaining the deleted content.

For Neon, deletion completion distinguishes active-row deletion from expiration
of copies still reachable through branches, history retention/PITR, logical
backups, and support/forensic holds. Do not claim that customer data is fully
gone when it remains recoverable inside an applicable retention window.

### 14.6 Capacity and overload

Use explicit limits at every boundary:

- maximum package, log line, event batch, evidence bundle, API body, and list
  page size;
- organization quotas for concurrent builds/runs and stored artifacts;
- weighted fair queueing so one tenant cannot monopolize runners/workers;
- Neon pooled/direct connection budgets per role/service, PgBouncer queue time,
  compute utilization, and bounded worker concurrency;
- backpressure from runner spool and cloud event ingestion;
- connector-specific API rate budgets;
- state-volume soft/hard fullness thresholds;
- admission control before work is leased, not only after a container starts.

Load tests must demonstrate graceful rejection (`429`/queued state), not process
OOM or unbounded queue age.

---

## 15. Commercial model and enterprise onboarding

### 15.1 Open-source versus paid boundary

Keep the engine valuable and credible without the service:

| Open-source/local | Cloud/paid |
|---|---|
| Declarative engine and local state | Organization/project/deployment registry |
| Python and Rust SDKs | Hosted scheduling and fleet control |
| Connectors and connector kits | Historical integrity findings and alerts |
| Local read-only integrity scan | Approval workflows and remote repair operations |
| Local plan/run/explain/replay | Immutable cloud build/signing pipeline |
| Local evidence export | Central evidence/audit retention and exports |
| Customer-operated runner binary | Managed runners and managed state durability |
| Local verification tools | SSO/SCIM/RBAC, support, SLA, private networking |

Do not intentionally cripple correctness in the open-source engine. Charge for
coordination, managed operations, visibility, assurance, and support.

### 15.2 Pricing hypothesis

Start paid design partners with a simple contract:

1. Monthly platform fee per organization/environment.
2. Included documents or target records under integrity management.
3. Metered overage on records scanned/reconciled, not raw API calls.
4. Managed-compute CPU/memory/runtime and storage charged separately.
5. Enterprise minimum for SSO, private networking, audit export, SLA, and
   support response.

Avoid charging per tiny engine task or target-state action; customers cannot
predict that from business volume and optimizations would change bills. Expose
usage daily, provide budgets/alerts, and include stable metric definitions in
the contract.

### 15.3 Usage ledger and billing

The internal immutable usage ledger is authoritative. A usage event has a
stable source event ID, organization, metric version, integer quantity,
occurred time, run/scan reference, and correction relationship. Aggregation is
recomputable. Corrections append negative/positive events; they never mutate
history.

Stripe or another billing provider is downstream. Idempotently transmit
aggregates and reconcile internal totals, provider totals, and invoices. Stripe
documents the model in
[usage-based billing](https://docs.stripe.com/billing/subscriptions/usage-based/how-it-works)
and recommends idempotency identifiers when
[recording usage](https://docs.stripe.com/billing/subscriptions/usage-based/recording-usage-api).

No execution correctness path waits on the billing provider. Entitlement
changes are cached briefly, signed/versioned where appropriate, and fail
according to a documented grace policy rather than unpredictably shutting down
a customer pipeline.

### 15.4 Paid design-partner onboarding

For the first five customers, use a high-touch checklist:

1. Confirm source/target, volume, change rate, regions, network path, and current
   incident pain.
2. Classify data and agree what never leaves their environment.
3. Document connector permissions with a read-only scan credential first.
4. Install the runner in a non-production project/namespace and verify outbound
   endpoints, proxy, DNS, certificate renewal, and upgrades.
5. Baseline a representative corpus and manually review integrity findings.
6. Agree mapping/adoption rules and false-positive handling.
7. Configure alert routing, retention, support contacts, and escalation.
8. Exercise runner revocation, API-key rotation, failed scan, and evidence
   export before enabling repair.
9. Generate and approve a no-op or low-risk repair in staging.
10. Define success metrics and 30/60/90-day review.

Pilot success metrics should be commercial and technical: time to first scan,
confirmed drift found, false-positive rate, repair time saved, scan cost,
runner uptime, and whether the customer will convert or expand.

### 15.5 Enterprise questionnaire package

Prepare one versioned evidence room containing:

- architecture and data-flow diagrams;
- tenant isolation and encryption description;
- subprocessors and regional data locations;
- retention/deletion and backup policies;
- SDLC, vulnerability, incident-response, and access-control policies;
- latest penetration-test executive summary and remediation status;
- SBOM/signature/provenance verification instructions;
- uptime/status history and support SLA definitions;
- DPA, security addendum, BCP/DR summary, and audit/certification status;
- exact guarantee matrix for integrity and verified erasure.

Answers must cite deployed control versions. Avoid aspirational “yes” answers
that only refer to this plan.

---

## 16. Phase-by-phase implementation roadmap

### 16.1 Planning assumptions and sequencing

The elapsed ranges below assume a focused team of five to seven engineers:
two engine/connector, two cloud/backend, one agent/infrastructure, one
frontend/product, with shared security/SRE support. They are sequencing guides,
not commitments. A smaller team should reduce connector/product scope, not
remove security and recovery gates.

```text
Phase 0: decisions and baseline
   |
   +--> Phase 1: local read-only integrity
   |
   +--> Phase 2: cloud identity/tenancy/API
              |
              +--> Phase 3: customer-hosted runner
              |           |
              |           +--> Phase 5: remote plan/approve/apply
              |
              +--> Phase 4: immutable build supply chain
                          |
                          +--> Phase 5

Phase 5 + certified integrity path -> Phase 6 paid pilots
Phase 6 operational proof          -> Phase 7 managed compute
Phase 7 security/reliability proof -> Phase 8 enterprise GA
Existing revocation gaps + Phase 8 -> Phase 9 Verified Erasure GA
```

No phase is complete because code merged. Completion means the exit criteria
pass in the intended environment and the rollback/recovery path was exercised.

Status notation used below:

- ✅ implemented and verified by repository evidence;
- ⏳ still requires implementation or external evidence;
- ⛔ intentionally belongs outside this repository.

### 16.2 Phase 0 — Decisions, baselines, and design partners (weeks 0–2)

**Evidence status (2026-08-04): ⏳ phase exit criteria are not complete.**

- ✅ The repository baseline and Phase 0–2 implementation gap were audited.
- ✅ [ADR-0004](ADR-0004-cloud-repository-clerk-neon-boundaries.md) accepts the
  repository, Clerk, Neon, identity, database, runner-identity, and LMDB
  authority boundaries.
- ✅ A versioned integrity contract fixture now lives under
  `python/tests/integrity/fixtures/v1/`.
- ✅ A 100k source plus 100k target metadata-fact scan is checked by an
  automated 32 MiB Python-allocation and 20-provider-call scale gate.
- ⏳ Reference-pipeline full/no-op/change/delete/restore benchmarks, outbound
  field inventory, named organizational owners, five interviews, and two
  signed design partners still require human and deployed-environment evidence.

**Goal:** remove architecture ambiguity before adding public APIs or cloud
dependencies.

**Work:**

1. Accept or revise ADRs for:
   - repository/licensing boundaries (`synor`, `synor-agent`, `synor-cloud`);
   - hybrid runner first and managed-compute trust boundary;
   - API and runner protocol/versioning;
   - Clerk authority boundaries, Organization mapping, API-key lifecycle, and
     outage/reconciliation behavior;
   - Neon project/branch/role/connection/backup topology;
   - tenant data model and RLS role design;
   - artifact format/build/signature policy;
   - evidence allow-list and data classification;
   - LMDB state ownership, backup, and restore;
   - initial source/target reference path.
2. Write a one-page guarantee matrix distinguishing read-only integrity,
   Synor-owned repair, adopted state, governed erasure, and unsupported paths.
3. Interview at least five qualified users and secure two design partners with
   representative, non-trivial datasets. Confirm budget owner and buying event,
   not only technical interest.
4. Capture baseline benchmarks for a reference pipeline at 10k and 100k source
   objects: full update, no-op update, one-percent change, delete, interrupted
   update, LMDB size, and restore/reopen time.
5. Build a small versioned fixture corpus with expected ownership and target
   facts. It will become the cross-repository contract fixture.
6. Inventory every outbound field in current execution/audit/provenance reports
   and classify `never export`, `typed export`, or `explicit support bundle`.
7. Record the selected cloud implementation stack: Clerk; Neon PostgreSQL;
   Python 3.12, FastAPI, Pydantic v2, SQLAlchemy 2 async and Alembic; an
   SQS-compatible queue, S3-compatible object store, OCI registry,
   OpenTelemetry, Terraform, and a TypeScript/React console using Clerk's
   frontend SDK. Confirm the required Clerk and Neon paid-plan entitlements,
   support, region, restore window, protected branches, IP Allow/Private
   Networking, API-key usage cost, SSO, and Directory Sync before promising
   them to a design partner.
8. Assign named owners for API/auth, runner, engine/connector, infrastructure,
   security/privacy, billing, and product onboarding.

**Repository changes:** documentation and fixtures only. Add accepted ADRs to
`docs/architecture/`; add benchmark inputs under an existing benchmark/test
fixture convention rather than a new public module.

**Validation:** architecture review, threat-model review, baseline results
checked into a dated validation record, and a design-partner problem statement
signed off by product and engineering.

**Exit criteria:** all architecture decisions above have an owner and accepted record; two
design partners match the selected connector path; the team can state exactly
which data crosses the runner boundary.

**Rollback:** no runtime behavior has changed. Revise ADRs and product scope
before Phase 1/2 public contracts merge.

### 16.3 Phase 1 — Open-source read-only Integrity MVP (weeks 2–6)

**Evidence status (2026-08-04): ⏳ repository implementation is ✅; product exit
criteria are not complete.**

- ✅ Experimental `synor.integrity` models, keyed report projection, profiles,
  bounded SQLite spill/checkpoint/resume scanner, streaming report merge,
  bounded issue/finding projections, and deterministic JSON.
- ✅ Read-only S3 and governed-Qdrant inspectors, connector exports, local CLI,
  generated CLI reference, programming guide, and service-free example.
- ✅ Golden classifications, privacy canaries, pagination faults, every-page
  interruption/resume, permissions, long identifiers, no-mutation fakes,
  cancellation-safe cleanup, spill-disk enforcement, public typing, and the
  100k source plus 100k target memory/provider-call scale gate.
- ⏳ Live opt-in S3/Qdrant acceptance, two design-partner scans, manual
  false-positive/negative labeling, and independent privacy review remain
  required before the complete Phase 1 badge may become ✅.

**Goal:** prove customers have the drift problem and that Synor can report it
safely before building remote mutation.

**Current-repository file plan:**

| Status | File/change | Purpose |
|---|---|---|
| ✅ | `python/synor/integrity/__init__.py` | Small public export surface: scan config, report, finding types, scan entry point |
| ✅ | `python/synor/integrity/_model.py` | Typed immutable IDs, cursors, facts, findings, confidence and summaries |
| ✅ | `python/synor/integrity/_scan.py` | Bounded merge/diff engine, deterministic finding IDs, checkpoint/resume |
| ✅ | `python/synor/integrity/_profile.py` | Explicit source-to-target mapping rules and validation |
| ✅ | `python/synor/connectorkits/integrity.py` | Narrow read-only inspector protocol shared by two connector implementations |
| ✅ | `python/synor/connectors/qdrant/_integrity.py` | Read-only governed-point inventory and lineage fact extraction |
| ✅ | `python/synor/connectors/amazon_s3/_integrity.py` | First source inventory and explicit consistency token |
| ✅ | connector `__init__.py` files | Export only stable user-facing constructors/types |
| ✅ | `python/synor/cli.py` | `synor integrity scan local` command and private JSON report path |
| ✅ | `docs/src/content/docs/cli.mdx` | Regenerated through the repository hook after CLI changes |
| ✅ | `python/tests/integrity/` | Pure scan/profile/cursor/resume/privacy/scale tests and versioned fixture |
| ✅ | `python/tests/connectors/` | Inspector permissions, pagination, no-mutation, malformed-data and mapping tests |
| ✅ | `examples/index_integrity_local/` | Reproducible non-cloud demo using deterministic fixtures |

**Implementation steps:**

1. Define typed facts and report schema before connector code. A fact carries
   opaque local identity, keyed export digest, version/fingerprint, observation
   token, and typed optional provenance—not content.
2. Make the scan algorithm accept sorted bounded pages and spill large
   intermediate sets to a temporary local store. Enforce max memory and disk.
3. Define incomplete scan states for denied items, inconsistent pagination,
   expired cursors, rate-limit exhaustion, and mapping ambiguity. Do not report
   a clean bill of health when coverage is incomplete.
4. Add the target inspector with read-only credentials. Verify the connector
   makes no write API calls in unit/fake and live opt-in tests.
5. Add one source inspector and bind source/target snapshots in the report.
6. Implement deterministic JSON export with `schema_version`; human CLI output
   is a projection of the same report.
7. Add resume checkpoints that include profile and snapshot tokens. Reject a
   resume against changed mapping or incompatible snapshots.
8. Add local examples and docs that clearly label heuristic versus proven
   findings.
9. Benchmark full and incremental scans. Profile memory and provider calls.
10. Run scans with design partners in read-only mode; manually label findings
    and record false positives/negatives.

**Required tests:**

- duplicate, missing, stale, orphan, ambiguous, and healthy golden cases;
- empty source/target and very long identifiers;
- page duplication, omission, reorder, token expiry, permission denial, and
  rate-limit recovery;
- interruption at every page followed by resume;
- content/credential canaries absent from JSON, logs, and exceptions;
- no target mutating method invoked;
- 100k fixture within the agreed memory/call budget;
- public import/type-check tests following external-module conventions.

**Exit criteria:** two design partners complete a read-only scan; at least one
material real finding is confirmed or the wedge is reconsidered; false-positive
classification is understood; no write credential is needed; exported schema
passes privacy review.

**Rollback:** the feature stays behind an experimental CLI namespace and is not
exported from top-level `synor` until the schema and two inspectors stabilize.
Removing it cannot affect `App.update()` or connector target paths.

### 16.4 Phase 2 — Cloud foundation, tenancy, and API keys (weeks 3–8)

**Evidence status (2026-08-04): ⛔ no Phase 2 service code is implemented in
this OSS repository; the phase remains ⏳.**

- ✅ Clerk/Neon authority, tenancy, connection, API-key, failure, test, and
  recovery decisions are specified here and accepted in ADR-0004.
- ⛔ The service belongs in the separately governed `synor-cloud` repository;
  placing a partial FastAPI/Clerk/Neon implementation in `python/synor` would
  violate the accepted licensing and trust boundary.
- ⏳ The `synor-cloud` repository, real Clerk/Neon environments, migrations,
  API/worker/console, IaC, cross-tenant suite, external security review, and
  restore drills must exist before Phase 2 can be marked ✅.

**Goal:** a secure control plane can create tenants/projects/service accounts,
issue scoped keys, accept immutable metadata/artifacts, and emit audit events.
It does not run customer code yet.

**New `synor-cloud` repository shape:**

```text
synor-cloud/
  apps/
    api/                    HTTP composition root
    worker/                 outbox, artifact, webhook, billing workers
    console/                Clerk-authenticated web UI
  packages/
    domain/                 state machines, policies, typed IDs
    auth/                   Clerk adapters, typed principals, product authorization
    db/                     Neon models/repositories/tenant transaction helper
    contracts/              generated OpenAPI models and event schemas
    observability/          safe logging/tracing wrappers
  openapi/v1.yaml
  migrations/
  infra/modules/            network, DB, queue, object store, KMS, service
  tests/
    contract/ integration/ security/ migration/
  runbooks/
```

Use a modular monolith: one API artifact and one worker artifact may share
domain packages, but modules own tables and state transitions. Do not begin
with independently deployed microservices and distributed transactions.

**Implementation steps:**

1. Provision separate development, staging, and production cloud accounts,
   Clerk applications, and Neon projects. Establish Terraform state, CI
   identity, KMS keys, regional networking, queue, object store, registry,
   secret manager, and OpenTelemetry collectors. Record Clerk/Neon resources in
   IaC or a reviewed bootstrap manifest without committing credentials.
2. Configure Clerk sign-in/MFA/session policy and Organizations. Integrate the
   React frontend and FastAPI `clerk-backend-api` authentication with pinned
   issuer, audience, authorized parties, JWT key, and Backend API version.
3. Implement retryable Clerk Organization provisioning and typed
   `clerk_user_id`/`clerk_organization_id` mapping. Disable uncontrolled
   personal-account access to organization resources.
4. Create the core schema from section 6 on Neon using expand-only Alembic
   migrations through the direct endpoint. Every tenant table has non-null
   `organization_id`, tenant-aware constraints, RLS, and a default-deny test.
5. Bootstrap separate Neon roles for migration, API, workers, read-only
   operations, and backup. API/workers use pooled URLs; migration/backup use
   direct URLs. Prove runtime roles are not owners and lack `BYPASSRLS`.
6. Add the explicit transaction helper that calls
   `set_config('app.organization_id', ..., true)` on the pooled connection and
   rejects repository calls outside its transaction. Alternate tenant contexts
   on reused pooler connections in tests.
7. Implement centralized Synor authorization and organization/project role
   bindings. Clerk proves principal and active Organization; Neon owns product
   authorization.
8. Enable Clerk Organization API keys and implement Synor service accounts,
   backend-only key creation/binding, one-time reveal, verification, rotation,
   local-first revocation, reconciliation, expiry, scope/project restrictions,
   rate limiting, and Clerk-outage behavior.
9. Publish the minimal `/v1` organization/project/service-account/deployment
   metadata routes and generated client.
10. Add presigned package upload initialization/finalization. Finalization
   verifies size/digest/media type and moves to an immutable object key.
11. Add signed Clerk webhook ingestion with event dedupe/versioning plus
    audit/outbox tables and workers. Audit every identity mapping, policy, key,
    project, and artifact change transactionally. Webhooks reconcile; they do
    not authorize a synchronous request.
12. Protect the Neon production branch, configure network restrictions,
    production scale-to-zero/autoscaling policy, history retention, monitoring,
    and independent direct-endpoint logical backup.
13. Add RFC 9457 errors, idempotency middleware, cursor pagination, request
    limits, safe structured logging, and OpenTelemetry.
14. Build console screens for Clerk Organization switching, internal
    organization/project roles, service accounts/key rotation, and audit
    history. Never render a recovered API-key secret.

**Required tests:**

- every object endpoint called using a second Clerk Organization's valid human
  session and Organization API key;
- wrong Clerk instance, issuer, audience, authorized party, token type, inactive
  Organization, deleted membership, and missing internal mapping;
- signed Clerk webhook success plus forged, replayed, duplicated, reordered,
  delayed, and unavailable webhook paths;
- direct and pooled Neon connections with actual runtime roles prove default
  RLS denial, transaction-local context, no cross-tenant reuse, and correct
  grants;
- migration upgrade from empty/previous schema on an ephemeral Neon branch,
  schema diff, and rollback strategy for the last expand step;
- Clerk key backend-issued claim, Organization subject, scope/local binding,
  expiry, disabled service account, ambiguous creation cleanup, rotation,
  local-first revocation, Clerk reconciliation, and outage behavior;
- idempotency replay, request mismatch, concurrent duplicate create, and
  committed-response-loss;
- upload URL cannot write/read another key or exceed size/type/expiry;
- audit/outbox atomicity with process killed before/after commit;
- fuzz/oversize input and auth rate-limit tests;
- production branch protection/network policy and preview branch
  schema-only/anonymized-data enforcement;
- log snapshot contains no Clerk secret, session/API key, Neon URL/password,
  cookie, webhook signature, or upload signature value.

**Exit criteria:** external security review of Clerk/Neon auth and tenant paths;
automated cross-tenant suite covers every route; Clerk Organization API key can
be created, used, rotated, locally denied, and remotely revoked; Clerk sync and
Neon pooled RLS withstand fault injection; backup restore is rehearsed; no
customer code executes.

**Rollback:** deploy application rollback only while schema remains
expand-compatible. Disable new routes/keys with a server feature flag; do not
drop new columns/tables until all old binaries are gone and retention permits.

### 16.5 Phase 3 — Customer-hosted runner and safe evidence (weeks 6–12)

**Goal:** enroll an outbound-only runner, execute a pinned sample deployment in
a dedicated customer environment, and return typed safe evidence. Apply remains
disabled for production data.

**New `synor-agent` repository shape:**

```text
synor-agent/
  crates/
    agent/                   binary and composition root
    protocol/                generated runner contracts + compatibility
    identity/                enrollment, key storage, certificate renewal
    supervisor/              process/container lifecycle and resource limits
    artifacts/               digest/signature/download/cache
    spool/                   encrypted ordered local event batches
    executor-python/         Synor Python deployment adapter
    updater/                 signed staged agent update
  deploy/
    kubernetes/              Helm/manifests and restricted RBAC
    vm/                      systemd and hardening reference
  tests/
    protocol/ e2e/ failure/ security/
```

Rust is the recommended agent language: it aligns with the engine, produces a
single supervisor binary, and reduces the chance that untrusted pipeline code
can mutate the supervisor. The executor is a separate child/container; never
import a customer's Python app into the agent process.

**Implementation steps:**

1. Define and generate protocol schemas/golden fixtures with `protocol_min` and
   `protocol_max`.
2. Implement one-use enrollment, local key generation, workload certificate
   storage, renewal, revocation, and clock-skew behavior.
3. Implement outbound long-poll claim/heartbeat/complete using a development
   runner gateway. Support proxy configuration and custom enterprise CA bundle
   without an insecure “skip verification” production flag.
4. Add durable encrypted spool with monotonic sequence, checksum, byte ceiling,
   acknowledged compaction, and crash recovery. Use a key protected by the host
   or Kubernetes secret mechanism.
5. Download artifacts only with attempt-scoped URLs. Verify digest before
   extracting; reject absolute paths, traversal, symlinks/device entries, file
   count/size bombs, and incompatible manifest versions.
6. Implement a Python executor that creates an isolated environment and invokes
   `SynorRuntime` through a small versioned local protocol. First support
   read-only plan/inspection against fixtures.
7. Bind one local state directory to organization/project/environment and take
   the existing Synor app operation lease. Reject a conflicting identity file.
8. Apply OS limits, child environment allow-list, signal/deadline handling,
   bounded stdout/stderr capture, and secret redaction.
9. Translate only reviewed typed events to the spool. Keep raw diagnostic logs
   local with a retention/size limit.
10. Add signed agent releases, staged update, version compatibility reporting,
    and rollback to the previous verified binary. Never self-update during an
    active attempt.
11. Publish Kubernetes and dedicated-VM install guides. Trial Docker instructions
    must state the Docker-socket risk if applicable.
12. Add runner fleet pages: online/offline, last heartbeat, version compatibility,
    capacity, pool/labels, certificate expiry, and revoke.

**Current-repository integration:** initially none beyond consuming released
Synor APIs and fixtures. If typed event/snapshot support is missing, add it in a
small separate PR to `python/synor/execution.py` and/or Rust core. The callback
must be internal, bounded, non-blocking, and unable to make a failed engine run
look successful. Update `core.pyi` for native changes.

**Required tests:**

- enrollment token double-use, expiry, wrong tenant/pool, certificate renewal,
  revocation, CA rotation, and clock skew;
- proxy/no-proxy, connection loss, TLS interception failure, and backoff;
- protocol current and previous two minor fixtures;
- archive traversal/symlink/bomb/tamper rejection;
- kill agent and child at each lifecycle boundary; recover spool without
  duplicate/lost accepted events;
- stale lease generation cannot heartbeat or complete;
- child cannot read supervisor identity/spool/other environment files;
- secret canaries absent from cloud events and normal logs;
- 24-hour soak with repeated plan-only fixture executions;
- signed updater rejects unsigned/wrong-identity/downgrade artifacts.

**Exit criteria:** runner operates for one week in two design-partner
non-production environments; no inbound network is required; renewal and
revocation drills pass; event loss/duplication is within explicit idempotent
contract; customer pipeline remains isolated from agent credentials.

**Rollback:** revoke runner identity, stop agent, and leave customer data/state
in place. Roll back agent to previous signed version. The cloud marks pending
attempts blocked rather than assigning them to an incompatible version.

### 16.6 Phase 4 — Reproducible builds and immutable deployments (weeks 8–14)

**Goal:** turn a deterministic source package into a verified OCI deployment
without exposing the control plane to build code.

**Cloud repository work:**

1. Add a build domain module and build state machine:

   ```text
   received -> queued -> fetching -> building -> scanning -> signing
            -> ready | rejected | failed | expired
   ```

2. Add an isolated builder worker image and Terraform module in a dedicated
   subnet/account/project with no production data-plane access.
3. Mirror/allow-list package indexes. Resolve a complete transitive dependency
   lock and retain all input hashes. Separate networked download from offline
   build/install.
4. Pin official base images by digest and support a minimal matrix of Python,
   OS, and architecture versions.
5. Run an import/entry-point probe with strict resource/network limits. It may
   verify that the app loads but must not contact customer systems.
6. Generate OCI image, SBOM, vulnerability report, build log digest, and SLSA
   provenance; sign the image and attestations with the builder workload
   identity.
7. Store immutable deployment rows binding all digests and compatibility facts.
   A failed/rejected build is never runnable by an override that merely changes
   its status field.
8. Add a policy decision layer for vulnerability severity, license allow/deny,
   unpinned dependency, yanked package, secret detection, base-image age, and
   required signature identity.
9. Add deployment promotion and rollback-by-prior-digest. Promotion never
   rebuilds.
10. Make the agent verify digest, signature identity, attestation subject,
    manifest version, and engine/agent compatibility before launch.

**Current-repository work, only if proven necessary:**

| Path | Bounded change |
|---|---|
| `python/synor/packaging.py` | Add package format v2 with explicit entry point, file/dependency-lock digests, constraints, and capability declaration while retaining v1 read support |
| `python/tests/test_pipeline_packaging.py` | Golden v1/v2 bytes, deterministic rebuild, traversal/symlink, malformed manifest, secret exclusion |
| `python/synor/user_app_loader.py` | Accept a validated manifest entry point if current loading cannot express it; do not add network behavior |
| `dev/check_release_readiness.py` | Validate official package/agent provenance only when those artifacts enter this repo's release scope |

Do not add the builder, OCI registry client, scanner, web API, or billing SDK to
this repository's `pyproject.toml`.

**Required tests:**

- same source/lock/policy produces equivalent manifest and image inputs; output
  differences are explained by captured builder metadata;
- dependency confusion, malicious build backend, package hash mismatch,
  unapproved index, build-network redirect, and metadata-service attempts;
- source archive traversal, symlink/hardlink/device file, decompression bomb,
  excessive file count, and Unicode path collision;
- tampered image/SBOM/provenance/signature rejected by both service and agent;
- builder cannot reach control DB, secret manager production paths, runner
  gateway, or another build;
- policy version change creates a new evaluation and audit record;
- promotion preserves digest; rollback starts the prior verified digest;
- build-worker termination at every state leaves a resumable or terminal record
  and no partially trusted deployment.

**Exit criteria:** official example packages build reproducibly on the supported
matrix; two partner apps build without unreviewed shell escape hatches; agent
independently rejects tampered artifacts; SBOM/provenance are downloadable and
verify with documented commands.

**Rollback:** deployments are immutable. Disable a bad build policy/base image,
revoke its signing identity if compromised, mark affected digests blocked, and
promote a previously verified digest. Never mutate or retag the affected image
as “fixed.”

### 16.7 Phase 5 — Remote plan, approval, apply, and schedules (weeks 12–18)

**Goal:** safely operate the existing Synor controlled execution boundary from
the cloud, first in staging and then for narrowly certified production repair.

**Implementation steps:**

1. Implement deployment environment configuration containing only typed
   non-secret settings, secret references, runner-pool constraints, egress
   profile, retry/cancellation policy, and state-volume identity.
2. Add runner capability documents and scheduler eligibility matching. Reject
   missing/unknown required capabilities.
3. Implement logical run and attempt state machines, transactional outbox,
   atomic claim, heartbeats, lease generation, stale completion rejection, and
   queue reconstruction.
4. Add a **plan-only** job that invokes `SynorRuntime.plan()` on the runner and
   uploads a strict plan envelope. Bind all fields from section 10.2.
5. Render plan summaries in API/console without losing creates/updates/deletes,
   unresolved cases, connector atomicity, source coverage, policy warnings, or
   expiry.
6. Implement environment approval rules and immutable approval/rejection
   records. Require distinct approver for deletion/high-risk production plans.
7. Implement **apply-approved-plan**. Immediately before apply, verify lease,
   deployment/artifact digests, policy/capability versions, plan expiry, source
   consistency token, and action digest.
8. Call the native controlled apply/reconciliation path. Do not replay a JSON
   action list in cloud or agent code.
9. Upload post-run evidence separately from completion so an upload retry never
   re-executes sink effects.
10. Add cooperative cancellation and deadline handling. Mark uncertain
    interrupted apply environments `needs_inspection`.
11. Add schedule calculation in the cloud using stored time zone and DST-aware
    rules. A schedule creates ordinary runs; it never bypasses approval policy.
12. Add manual retry UI that explains why a connector/attempt is or is not safe
    to retry.
13. Add consistent managed/customer-exportable LMDB snapshot and restore
    primitives before the first production apply.

**Current-repository file plan:**

| Path | Change |
|---|---|
| `python/synor/execution.py` | Versioned plan/evidence envelope extensions or internal observer required by runner; preserve direct/local behavior |
| `python/synor/replay.py` | Add backward-compatible source snapshot/cursor binding and digest verification if the current envelope lacks it |
| `python/synor/_internal/app.py` | Only if needed to pass an internal controlled observer/deadline through the established app boundary |
| `rust/core/src/engine/app.rs` | Only engine-owned typed progress/snapshot operation; preserve operation lease and quiescence |
| `rust/core/src/state_store/storage.rs` | General LMDB-consistent backup-copy/restore-validation API; all writes still through `run_txn` |
| `rust/py/src/environment.rs` or the closest existing environment bridge | Minimal async/sync wrapper for backup/restore validation |
| `python/synor/_internal/core.pyi` | Exact type stubs for every PyO3 addition |
| `rust/sdk/synor/` | Language-neutral equivalent backup/observation API where the SDK exposes environments |
| `python/tests/`, Rust core/SDK tests | Compatibility, lease, snapshot, restore, observer failure, controlled execution |

Before editing, confirm the exact existing bridge file and avoid creating a
parallel environment abstraction. Keep cloud IDs out of stable paths and
fingerprints.

**Required tests:**

- plan/apply happy path and every bound-field mismatch;
- source snapshot changes between plan and apply;
- approval expired/revoked, self-approval denied, approver loses role;
- duplicate claim/message/heartbeat/complete and stale generation;
- kill API, worker, gateway, agent, child, and network at each attempt state;
- apply interruption before, during, and after possible sink effects for every
  certified connector;
- event upload fails after engine success: run remains successful with evidence
  pending and no engine rerun;
- state volume double mount/app open rejected;
- snapshot during active app waits/fails correctly; restore to new path; corrupt,
  truncated, wrong-schema, wrong-environment, and older backup rejected;
- current local `App.update()` outputs and public signatures unchanged;
- old package/agent protocol reads new optional fields safely;
- schedule DST gap/fold, leap day, missed execution, pause/resume, and duplicate
  scheduler worker;
- 72-hour fault-injection soak on a certified staging connector.

**Exit criteria:** plan-only works on all pilot environments; production apply
is enabled only for a connector operation with `apply` certification; approval
binding and stale fences pass adversarial review; restore drill succeeds; local
SDK behavior is unchanged.

**Rollback:** server feature flags disable apply while retaining plan/inspect.
Revoke/deactivate schedules and new leases. Roll back cloud/agent through the
protocol overlap. Never in-place downgrade an LMDB environment that crossed the
native schema boundary; use the documented verified copy workflow where
eligible.

### 16.8 Phase 6 — Integrity repair pilots, billing, and product proof (weeks 16–24)

**Goal:** convert the read-only wedge into a paid, supportable workflow for a
small connector matrix and learn whether customers will renew.

**Implementation steps:**

1. Promote the first source/target inspectors from experimental after live
   permission, pagination, snapshot, rate-limit, and scale certification.
2. Build cloud integrity profiles, scan history, deterministic finding
   lifecycle, acknowledgment/suppression with reason/expiry, and alert routing.
3. Add adoption/baseline workflow for target data not previously owned by
   Synor. Require preview and explicit ownership manifest approval.
4. Link findings to a native repair plan. Preserve confidence and unresolved
   coverage; do not convert heuristic findings into automatic deletes.
5. Enable repair only under a policy template appropriate to the certified
   connector operation. Start with creates/updates; enable deletes after
   separate live failure and restore evidence.
6. Add post-repair verification scan and compare expected versus observed
   finding closure. A successful API response alone is insufficient.
7. Implement immutable usage ledger, daily customer usage view, internal
   aggregation/reconciliation, billing-provider integration, plan entitlement,
   budget alerts, and invoice preview.
8. Add support grants, safe diagnostic bundle request/consent, customer-visible
   status page, incident workflow, and on-call rotation.
9. Create repeatable onboarding automation and reference Terraform/Helm modules;
   keep a named solutions owner for every design partner.
10. Measure unit economics: scan calls, runner/cloud CPU, event/storage bytes,
    support time, build time, and gross-margin sensitivity.
11. Publish an honest product guarantee table and exclusions tied to connector
    certification versions.

**Current-repository work:** finish the chosen inspectors/conformance harness,
mapping/report schemas, and any source/target verification readers. Extend
`dev/target-sink-certification.json` or add a sibling machine-readable
integrity certification document; use schema validation and dated evidence.
Do not mark a provider certified from mock-only tests.

**Required tests and drills:**

- representative live-service acceptance for each advertised connector;
- provider permission removed mid-scan/apply, throttling, timeout, partial
  result, stale cursor, restored target data, and out-of-band drift;
- adoption cannot delete an unapproved target record;
- repair approval digest changes when mapping/finding/policy changes;
- post-repair verification detects silent provider failure;
- usage event duplicate/late/correction and provider outage;
- invoice reconstruction from raw immutable events;
- entitlement expiry/grace behavior without corrupting active work;
- API-key rotation, runner revocation, backup restore, security incident, and
  customer deletion drills performed with pilot tenants;
- support operator cannot view content/secrets and every grant is visible.

**Exit criteria:** at least three paying design partners; two complete a repair
workflow and post-verification; measured false positives and support cost are
within agreed thresholds; billing reconciles; one customer commits to renewal
or expansion based on observed value.

**Rollback:** downgrade an organization to scan-only, disable unsafe connector
operations/certification versions globally, preserve evidence and billing
history, and perform connector-specific recovery. Commercial credits do not
rewrite usage or run history.

### 16.9 Phase 7 — Managed batch execution (months 6–9)

**Goal:** offer an optional Synor-hosted data plane for batch plans/scans/repairs
without changing the hybrid product's control protocol.

**Implementation steps:**

1. Reuse the same agent protocol and execution contract inside managed tasks;
   do not create a privileged shortcut around claims, leases, evidence, or
   artifact verification.
2. Provision regional execution cells with dedicated subnets, default-deny
   security groups/network policies, task workload identity, KMS, object store,
   registry access, and one encrypted block volume per environment.
3. Launch one isolated task/container per attempt with no shared tenant Python
   process. Enforce the hardening in section 13.3.
4. Build a state-volume controller with attach/detach generation, cloud lease,
   Synor environment/app lease, snapshot policy, and orphan-volume reconciler.
5. Implement private network options incrementally: static egress IP first,
   customer allow-list, then peering/transit/private endpoints based on demand.
6. Store managed connector secrets using customer/project-scoped references and
   KMS authorization. Fetch only into the attempt task.
7. Add admission/resource profiles, per-org concurrency quota, fair scheduling,
   cost budgets, image cache controls, and idle resource cleanup.
8. Add managed snapshot restore and a human-reviewed disaster failover. No
   active/active writer for one environment.
9. Run a capacity model and chaos suite across scheduler, tasks, volumes,
   object store, and regional control dependencies.
10. Price compute/storage transparently and compare actual gross margin.

**Required tests:**

- container escape and cross-task network/filesystem/identity attempts;
- cloud metadata and control-plane endpoint denied from pipeline;
- two tasks cannot mount/write one environment; stale volume generation fails;
- task/node/AZ loss during each lifecycle stage;
- snapshot restore into replacement volume and controlled validation plan;
- connector private endpoint, DNS, CA, proxy, and egress allow-list paths;
- malicious output/log/event volume remains bounded;
- noisy neighbor at organization and regional cell level;
- signed-but-policy-revoked image denied;
- full regional-cell recovery exercise with measured RPO/RTO.

**Exit criteria:** external penetration test of managed execution is remediated;
state durability drill meets measured objective; isolation tests pass; on-call
can diagnose customer/connector/platform failures; two pilots choose managed
mode and unit economics are acceptable.

**Rollback:** stop new managed claims, let safe active work finish under policy,
snapshot/detach volumes, and offer customer-hosted runner fallback. Preserve the
same deployment/run/evidence identities so migration does not rewrite history.

### 16.10 Phase 8 — Enterprise readiness and regional cells (months 9–12)

**Goal:** onboard a large regulated customer without bespoke unsafe access or
unbounded operational promises.

**Implementation steps:**

1. Enable Clerk Enterprise Connections for organization-scoped SAML/OIDC,
   domain verification, self-serve SSO where contracted, and Clerk Directory
   Sync for SCIM user/group lifecycle. Map coarse Clerk roles to versioned Synor
   role-binding policy; retain project RBAC and approvals in Neon. Add session
   policy and automated access reviews.
2. Add private networking options, customer-managed DNS/CA support, regional
   residency, encryption-key policy, and audit/evidence export destinations.
3. Split control execution into regional cells when scale/residency requires
   it. A global directory routes by immutable tenant home region; run/state
   writes remain within the home cell.
4. Add zero-downtime expand/migrate/backfill/contract database workflow and
   protocol compatibility dashboards.
5. Implement customer-configurable retention, legal hold, deletion state
   machine, backup expiry reporting, and export manifests.
6. Add enterprise rate/quotas, priority/fairness tiers, maintenance windows,
   deployment approvals, change-management integrations, and audit webhooks.
7. Complete external penetration test, incident tabletop, DR exercise,
   dependency/supply-chain review, and the selected assurance audit program.
8. Publish SLA/support terms backed by measured SLOs, error-budget process,
   paging, status communication, and service-credit calculation.
9. Create a compatibility/support matrix for agent, engine, Python, OS/arch,
   connector/capability version, and state schema.
10. Automate the enterprise questionnaire evidence room and track every answer
    to a deployed control/evidence artifact.

**Required tests/drills:**

- Clerk enterprise-connection metadata/key rotation, JIT-disabled Directory
  Sync provisioning, deprovisioning/session revocation, group-role mapping,
  and webhook/SCIM replay or out-of-order propagation;
- cross-region tenant routing and hard denial from the wrong cell;
- customer audit export outage/replay and immutable ordering;
- schema migration with old/new API, worker, agent, and background backfill
  concurrently active;
- regional evacuation, database point-in-time restore, object/artifact restore,
  runner reconnect, and customer communication;
- legal hold overriding deletion and release resuming deletion;
- support grant/break-glass review and customer-visible audit trail;
- full reference enterprise onboarding from contract to first verified scan.

**Exit criteria:** one enterprise customer passes security/procurement and
production onboarding without an undocumented bypass; DR and incident exercises
meet published objectives; support coverage and vulnerability response are
staffed; claims match current audit status.

**Rollback:** tenant remains pinned to its original home cell until migration is
verified; federation can fall back only to pre-enrolled break-glass owners under
a documented policy; schema changes use expand/contract and service versions
within compatibility windows.

### 16.11 Phase 9 — Verified Erasure general availability (after enterprise gates)

**Goal:** sell a narrowly defined, evidence-backed governed-erasure workflow,
not a universal deletion promise.

At this repository baseline, Phase 7 (drift/orphan/cache/restore assurance) and
Phase 8 (connector expansion/GA hardening) of
[`provable-index-revocation-implementation-plan.md`](provable-index-revocation-implementation-plan.md)
are not started. Complete and re-validate them before cloud GA for this tier.

**Implementation steps:**

1. Finish drift, orphan, cache-recipient, restore, and replay assurance in the
   existing local design before distributing control.
2. Replace local file/memory control-store assumptions with a transactional
   multi-process design that preserves per-source sequencing, suppression-first
   ordering, operator generation fencing, and crash recovery. Do not implement
   the current `StateStore` protocol over HTTP and assume transactions appear.
3. Prove at least two governed sources and two certified targets through the
   conformance and live failure-injection suites.
4. Model retrieval gateways/cache recipients as explicit governed boundaries
   with acknowledgements and expiry. Direct target queries stay excluded.
5. Bind revocation requests, approval/policy, source observation, native effect,
   target verification, retrieval suppression, and receipt into a signed
   evidence graph.
6. Add restore gates: a restored snapshot cannot serve until revocation/drift
   revalidation and reconciliation complete.
7. Add legal-hold/retention outcomes as explicit policy states; never report
   physical deletion when a permitted retention copy remains.
8. Have security, privacy/legal, and connector owners approve the exact claim
   language and exclusions.
9. Operate an extended beta with adversarial restore, out-of-band mutation,
   provider outage, stale worker, and cache tests.
10. Publish customer-verifiable receipt validation and auditor export tooling.

**Claim boundary:** the strongest permitted statement is equivalent to “for
this governed source identity, within these certified target and retrieval
boundaries, under this policy/version, Synor observed and verified these
logical effects at these times.” It is not physical-media erasure, model
unlearning, deletion from unregistered copies, or a legal conclusion.

**Exit criteria:** all GA gates in the dedicated revocation plan pass;
transactional distributed ordering is formally reviewed and fault-injected;
restore and cache paths are verified; receipts validate independently; legal
and security approve the exact published guarantee.

**Rollback:** disable new revocation applies but keep suppression/guarded
retrieval and pending recovery; never remove a serving suppression simply
because the cloud is rolling back. Follow the dedicated plan's safe rollback
and copy-based native-schema downgrade rules.

---

## 17. Recommended pull-request sequence

Keep each PR independently reviewable and releasable. Do not combine a core
schema change, a PyO3 change, a connector implementation, and a cloud service
feature in one patch. Cross-repository dependencies move through versioned
contracts/releases, not unpublished branch references.

### 17.1 This `synor` repository

| PR | Scope | Depends on | Must prove |
|---|---|---|---|
| `OSS-00` | Accept architecture/data/evidence/integrity ADRs and guarantee matrix | None | No runtime change; decisions and owners explicit |
| `OSS-01` | Integrity typed model, report schema, golden fixtures, failing scan tests | `OSS-00` | Schema has no raw content/credential fields; public surface minimal |
| `OSS-02` | Internal bounded scan/diff/checkpoint implementation | `OSS-01` | Deterministic findings, incomplete coverage, resume and memory limits |
| `OSS-03` | Read-only inspector protocol plus fake conformance harness | `OSS-01` | No mutation method in protocol; pagination/permission/rate-limit contract |
| `OSS-04` | Qdrant read-only inspector | `OSS-03` | Live opt-in inventory and no-write evidence |
| `OSS-05` | One source inspector: S3 or governed Google Drive | `OSS-03` | Snapshot/cursor/completeness behavior proven |
| `OSS-06` | Local integrity CLI, docs, example | `OSS-02`, `OSS-04`, `OSS-05` | Human/JSON output, exit codes, privacy and CLI docs generation |
| `OSS-07` | Package schema v2 and compatibility tests, only after build contract is accepted | Cloud build contract | Determinism, v1 read compatibility, malicious archive rejection |
| `OSS-08` | Versioned typed controlled-execution observer/envelope, if agent requires it | Agent protocol spike | Observer outage cannot alter engine outcome; local no-op unchanged |
| `OSS-09` | Rust LMDB backup-snapshot API and core tests | Backup ADR | Quiescence, compact copy, manifest, corruption/path safety |
| `OSS-10` | PyO3/Rust SDK snapshot bindings and end-to-end restore validation | `OSS-09` | `core.pyi` parity, async/sync behavior, restore into new path |
| `OSS-11` | Connector certification document/schema and chosen apply/verification improvements | Integrity pilots | Live failure injection and factual scheduler projection |
| `OSS-12` | Complete remaining revocation assurance PRs from its dedicated plan | Phases 6–8 | Drift/cache/restore/conformance gates, no broadened claim |

Each public Python PR checks `__all__` and private import aliases. Each Rust-core
write PR demonstrates that every mutation goes through `Storage::run_txn`.

### 17.2 `synor-cloud` private repository

| PR group | Scope |
|---|---|
| `CLOUD-00` | Repository policy, service skeleton, CI, IaC state, environments, threat/data model |
| `CLOUD-01` | Neon projects/branches, direct-versus-pooled roles, schema/migrations, tenant transaction helper, RLS and cross-tenant tests |
| `CLOUD-02` | Clerk React/FastAPI session auth, Organizations/provisioning, signed webhooks, identity projection and centralized product RBAC |
| `CLOUD-03` | Synor service accounts plus Clerk Organization API-key creation/binding/verification/scopes/local-first revocation/reconciliation |
| `CLOUD-04` | OpenAPI conventions, idempotency, problem responses, audit and outbox |
| `CLOUD-05` | Projects, artifacts/presigned upload, immutable deployment metadata |
| `CLOUD-06` | Runner enrollment CA/gateway and protocol conformance server |
| `CLOUD-07` | Runs/attempts/claims/heartbeats/lease fence and scheduler |
| `CLOUD-08` | Typed event ingestion, evidence manifest, audit export, observability |
| `CLOUD-09` | Isolated build service, OCI/SBOM/provenance/signature policy |
| `CLOUD-10` | Plan resources, approval policies, controlled apply and cancellation |
| `CLOUD-11` | Schedules, webhooks/alerts and operational console |
| `CLOUD-12` | Integrity profiles/findings/adoption/repair verification |
| `CLOUD-13` | Usage ledger, entitlements, billing sync/reconciliation |
| `CLOUD-14` | Managed execution cell/state-volume controller |
| `CLOUD-15` | Clerk Enterprise Connections/Directory Sync, Neon Private Networking/retention, and regional routing |

Within each group, merge database expansion before writers, writers before
readers depend on new values, and cleanup/constraints last.

### 17.3 `synor-agent` repository

| PR group | Scope |
|---|---|
| `AGENT-00` | Rust workspace, policy, CI, release signing, protocol golden fixtures |
| `AGENT-01` | Enrollment, private-key storage, mTLS renewal/revocation |
| `AGENT-02` | Long poll, capabilities, claims/heartbeat/complete and compatibility |
| `AGENT-03` | Encrypted durable event spool and idempotent upload |
| `AGENT-04` | Secure artifact download/extraction/digest/signature verification |
| `AGENT-05` | Process supervisor, limits, deadlines/signals and bounded logs |
| `AGENT-06` | Python Synor plan-only executor and state-directory identity |
| `AGENT-07` | Controlled apply, secrets, local app lease and evidence finalization |
| `AGENT-08` | Kubernetes/VM deployment and restricted permissions |
| `AGENT-09` | Signed staged updater and compatibility/rollback telemetry |

### 17.4 Cross-repository contract release rule

For every changed contract:

1. Add an additive schema and golden request/response/event fixtures to the
   owning repository.
2. Update producers to be able to emit it behind a flag while still accepting
   the prior schema.
3. Update consumers to accept both.
4. Release the consumer first when needed.
5. Enable new production emission gradually.
6. Observe protocol-version and unknown-field metrics.
7. Remove the old schema only after the support window and migration evidence.

Never coordinate a breaking production deployment by “merge these three PRs at
the same time.”

---

## 18. Comprehensive test and validation strategy

### 18.1 Test layers

| Layer | Purpose | Examples |
|---|---|---|
| Pure/unit | State machines, digests, policy, parsing, pagination merge | Exhaustive transition and property tests |
| Contract | OpenAPI/runner/evidence/package plus Clerk/Neon adapter compatibility | Golden fixtures across current/prior versions and vendor responses |
| Component | Service/agent/engine with real local dependencies | Local PostgreSQL plus Neon pooled/direct contract suite, queue, object store, LMDB |
| Connector conformance | Provider semantics and error taxonomy | Read-only inventory, partial write, retry/verification |
| End-to-end | Customer action through API, runner, engine, connector, evidence | Plan/approve/apply/verify and key revocation |
| Security/adversarial | Isolation and hostile inputs | Cross-tenant, SSRF, archive, secret leakage, stale fence |
| Failure/chaos | Crash and network ambiguity | Kill before/after commits/effects/acks |
| Performance/soak | Capacity, leaks and boundedness | 100k/1m facts, 24/72-hour runner and scheduler |
| Migration/restore | Compatibility and recoverability | old/new binaries, DB migrations, LMDB snapshot restore |
| Live acceptance | Provider behavior not reproducible with fakes | Opt-in, isolated service accounts and disposable resources |

Test invariants, not only examples:

- no principal can observe or mutate another tenant resource;
- a plan approval can authorize only one immutable action/config/policy tuple;
- no stale generation can extend or complete an attempt;
- no duplicate delivery creates a second logical state transition;
- no cloud event contains a forbidden canary value;
- no uncertified connector operation is automatically retried or repaired;
- no engine observer/network failure changes native commit success;
- no two processes/tasks write the same app/environment state concurrently;
- every acknowledged state transition survives process restart;
- every published artifact is digest- and signature-verifiable;
- every backup accepted for restore opens and passes engine inspection.

### 18.2 Existing repository commands

Use the repository's required workflow by changed layer:

```bash
# Python-only changes
uv run mypy
uv run pytest python/
uv run ruff format --check .
uv run ruff check .

# Rust changes
uv run maturin develop
cargo test
cargo fmt --check

# Rust + Python boundary changes
uv run maturin develop
cargo test
uv run mypy
uv run pytest python/

# Before publishing any PR
prek run --all-files
```

If `python/synor/cli.py` changes, the pre-commit suite must regenerate and
validate `docs/src/content/docs/cli.mdx`. Database connector tests use the
repository's `common.create_test_env(__file__)` and testcontainers conventions.

For a documentation-only change such as this plan, at minimum run Markdown/link
checks available in the repository, `git diff --check`, and the release-readiness
guard if the document introduces brand or assurance statements.

### 18.3 Cloud CI gates

Every cloud PR must run:

1. Formatter, linter, strict type checker, unit/property tests.
2. OpenAPI lint and generated-client drift check.
3. Clerk session/API-key/webhook contract tests using captured versioned
   fixtures, plus a live test-instance smoke for the pinned SDK/API version.
4. Fresh-database migration and upgrade-from-last-release test locally and on
   an ephemeral Neon branch through the direct endpoint.
5. PostgreSQL/RLS suite through Neon's pooled endpoint with the actual API and
   worker roles, alternating tenant contexts on reused connections.
6. Contract fixtures against supported agent releases.
7. IaC format/validate/policy/security scans and plan review, including Neon
   production branch protection/network rules and Clerk instance settings.
8. Dependency, secret, SAST, container and SBOM scans.
9. Image/provenance generation and signature verification.
10. Cross-tenant route test generator coverage check for Clerk sessions and
    Organization API keys.
11. Targeted end-to-end smoke in an ephemeral environment.

Nightly/weekly jobs run fault injection, longer migrations, restore, connector
live acceptance, high-volume, and soak suites. A flaky correctness/security
test is release-blocking work; do not normalize rerunning until green.

### 18.4 Agent CI gates

Build and test Linux `amd64` and `arm64` at minimum. Run:

- Rust format/lint/unit/property/fuzz targets;
- current/prior protocol fixtures against a cloud test gateway;
- archive parser corpus and signature failures;
- process tree, signal, limit, spool crash/recovery tests;
- restricted Kubernetes end-to-end using a disposable cluster;
- VM/systemd packaging smoke;
- software composition, SBOM, provenance, signing and updater verification;
- an install/upgrade/rollback matrix from the previous supported release.

Release artifacts are immutable, signed, and promoted from CI output; they are
not rebuilt manually for production.

### 18.5 Failure-injection grid

For every durable operation, inject failure at these points:

```text
before validation
after validation / before transaction
inside transaction before durable state
after commit / before outbox publish
after publish / before acknowledgement
after runner claim / before child start
after child start / before engine open
during plan/source enumeration
before first possible sink effect
during partial sink batch
after sink success / before engine state commit
after engine commit / before evidence spool
after spool / before cloud receipt
after cloud receipt / before runner acknowledgement
```

The expected state, permitted retry, customer-visible result, operator action,
and evidence must be asserted for each supported operation. If the effect
boundary cannot be determined, the expected result is `needs_inspection`, not
automatic success or retry.

### 18.6 Test data and privacy

Default CI uses generated data containing recognizable canaries for secrets,
PII, paths, and tenant IDs. After every end-to-end run, scan logs, traces,
events, database fields, object keys, build logs, and support bundles for those
canaries.

Live provider tests use dedicated test accounts/projects, minimum permissions,
disposable resource prefixes, bounded cost, and a cleanup verifier. Clerk tests
use the non-production Clerk instance. Neon tests use short-lived branches from
a schema-only or approved anonymized seed with an expiry/cleanup assertion.
Customer production data and a raw production Neon branch are never copied into
CI or developer fixtures.

### 18.7 Performance gates

Record hardware, versions, dataset, connector settings, and percentiles. At
minimum measure:

- engine full/no-op/incremental/delete update;
- integrity full/resume/incremental scan and provider calls;
- LMDB map size, snapshot duration/size, reopen and restore validation;
- event serialization/spool/upload overhead and backpressure;
- scheduler claim latency/fairness at concurrent tenant loads;
- control API read/write latency, Clerk verification latency/cost, Neon pooler
  wait/direct connection count, compute utilization, and cold starts/restarts;
- build cold/warm duration, image size and cache hit rate;
- managed task startup and state-volume attach time;
- billing/evidence backlog recovery.

A performance claim is tied to a checked-in validation record. A regression
budget should be set after the baseline, not guessed in code.

### 18.8 Definition of done for an implementation PR

A PR is done only when:

- scope and invariant are clear;
- success, negative, concurrency, failure and compatibility tests are present;
- migrations/contracts are additive or have an approved transition;
- privacy/security logging review is complete;
- observability exposes outcome without sensitive cardinality;
- operator failure and recovery action are documented;
- public docs/API stubs are synchronized where relevant;
- the correct local/CI commands pass;
- release/feature flag and rollback path are named;
- no unrelated user/worktree changes are bundled.

---

## 19. Rollout, compatibility, migration, and kill switches

### 19.1 Feature-flag dimensions

Server-side flags are typed policy records, not scattered environment-variable
booleans. Support these dimensions:

```text
organization / project / environment
runner pool and agent version
connector + connector version + operation certification
deployment digest/build policy
plan-only / apply-create / apply-update / apply-delete
managed execution region
evidence schema and retention tier
```

A flag evaluation is recorded with version in run/plan evidence. The emergency
kill path must disable new builds, new claims, all apply, a specific connector
operation, a signing identity, or a compromised agent range without deploying
code.

### 19.2 Exposure progression

For each risky capability:

1. Unit/contract and isolated integration only.
2. Maintainer dogfood with synthetic data.
3. Design-partner development environment, plan/read-only.
4. Design-partner staging, manual approval.
5. One production tenant with on-call observation and low concurrency.
6. Small allow-list by connector/capability version.
7. Paid beta with published limits.
8. General availability only after reliability/security/support gates.

Do not use percentage rollout alone for connector effects; select tenants and
operations whose recovery path is understood.

### 19.3 Neon/PostgreSQL expand/contract

Use this deployment order:

1. Create an ephemeral Neon branch from the approved schema-only/anonymized
   base, apply the migration over its direct endpoint, and review schema diff,
   locks, runtime and rollback behavior.
2. **Expand:** add nullable/default-safe columns, new tables/indexes/RLS
   policies without removing old representation.
3. **Dual-read/write only if necessary:** make ownership and precedence
   explicit; prefer a single writer plus derived backfill.
4. **Backfill:** idempotent, resumable, rate-limited, tenant-scoped batches with
   progress/audit metrics.
5. **Validate:** compare counts/digests and exercise old/new binaries through
   the pooled runtime endpoint and real non-owner roles.
6. Apply the production migration through the direct endpoint under the
   reviewed operational window.
7. **Switch reads:** feature flag and observe.
8. **Stop old writes.**
9. **Contract:** after support/rollback window, remove old fields/constraints in
   a separate release.

Never combine a destructive migration with the first binary that stops using
the old data. Large indexes use online/concurrent provider mechanisms and a
separate operational review.

### 19.4 Agent/control-plane upgrade order

The control plane first learns to accept the new optional protocol. Then agents
that can emit it are released. Emission is enabled after fleet compatibility is
visible. Breaking requirements wait until unsupported agents are drained or
explicitly blocked.

Agent updates are staged by runner pool, preserve the previous binary/config,
never occur during an attempt, and auto-roll back only for supervisor health—not
for ambiguous customer sink outcomes.

### 19.5 Package and evidence schema compatibility

- readers accept the current and previous supported schemas;
- manifests are immutable and self-identifying;
- unknown optional fields are preserved/ignored as specified;
- unknown required features fail closed with an upgrade instruction;
- canonical digest rules have golden byte fixtures across languages;
- evidence is never rewritten in place to a new schema—derive a new signed
  projection that references the original.

### 19.6 LMDB/native schema boundary

Cloud rollout does not change the existing one-way native activation rule. A
runner checks the environment schema before opening it and refuses an
unsupported newer schema. Upgrade sequence:

1. Take and verify a consistent backup.
2. Confirm no unresolved operation and no active writer.
3. Upgrade agent/engine that can read old and write new.
4. Perform a controlled plan/update.
5. Validate inspection/evidence and take a post-upgrade backup.

Rollback uses the original pre-upgrade volume or the repository's verified
downgrade-copy process when its preconditions allow. Never run an old engine
against the original database after native schema activation.

### 19.7 Emergency response matrix

| Incident | Immediate safe action | Preserve |
|---|---|---|
| Clerk/auth vulnerability | Disable affected routes or Clerk token types, locally deny API keys, revoke/rotate Clerk credentials and sessions, keep runners from new claims | Clerk/Neon mapping, audit, run state, evidence |
| Compromised signing key | Revoke identity/policy, block affected digests, stop new claims | Artifacts/attestations for investigation |
| Bad agent release | Block version and staged rollback while idle | Active-attempt state; do not blindly rerun |
| Connector causing destructive effects | Disable that operation/certification globally | State volume, plan, attempt, target evidence |
| Tenant isolation concern | Stop affected service path, preserve forensic logs, notify per policy | Database/object versions and access records |
| Neon corruption/outage | Freeze mutations/claims, preserve queues, restore a point-in-time branch or independent backup under runbook, validate before cutover | Outbox/audit, branch history, independent backups |
| Evidence leakage | Stop ingestion/access, revoke URLs/keys, retain forensic copy under incident controls | Source of leak and access audit |
| Managed state-volume loss | Fence environment, restore to new volume, inspect before apply | Last known volume/snapshots and generations |

Kill switches must be tested quarterly. A kill switch that has never been
exercised is not a reliable control.

---

## 20. Safe development checklist for future contributors

### 20.1 Before changing code

1. Read [`AGENTS.md`](../../AGENTS.md), then the relevant architecture ADR and
   the nearest public documentation.
2. Confirm the baseline and unrelated work:

   ```bash
   git status --short
   git diff --stat origin/main...
   git diff origin/main... -- <relevant-path>
   ```

3. Use semantic search first when available:

   ```bash
   ccc status
   ccc search "where is the relevant invariant implemented"
   ```

   Fall back to `rg`/`rg --files` when the local index is unavailable.
4. Trace one real end-to-end path from public entry point through Python
   internals, PyO3, Rust core, storage/connector, and tests. Do not infer a
   contract from a filename alone.
5. Write down:
   - the user-visible outcome;
   - the existing invariant being preserved;
   - the authoritative state owner;
   - whether persisted schema/public API/protocol changes;
   - failure boundaries and ambiguous side effects;
   - compatibility and rollback route.
6. Find the closest existing end-to-end tests and use their environment/fixture
   pattern. Check whether a public doc or generated CLI page must change.
7. If the change crosses a repository contract, land the schema/golden fixtures
   and compatibility readers before the producer emits it.

### 20.2 Layer-specific rules

**Rust core or LMDB:**

- design async first;
- preserve stable-path ownership and action reconciliation;
- all writes use `Storage::run_txn`;
- new keyspace/schema requires migration, corruption and downgrade analysis;
- no network/control-plane dependency inside engine commit;
- operation/environment leases are acquired in their existing order;
- test crash/restart and concurrent processes, not only one task.

**PyO3:**

- implement the core operation first;
- expose the smallest wrapper in the nearest existing bridge module;
- update `python/synor/_internal/core.pyi` in the same PR;
- rebuild with `uv run maturin develop`;
- add Python behavior and type-check coverage.

**Public Python:**

- keep `__all__` explicit;
- prefix non-public standard-library, third-party and internal imports in
  external modules;
- prefer specific types and convert weak identifiers at entry;
- do not expose speculative callbacks/options;
- keep async-first I/O and provide blocking calls only at established edges;
- never deserialize cloud-controlled bytes using pickle.

**Connector:**

- source inspection and target mutation use separate least-privilege protocols;
- declare snapshot/pagination/rate-limit/identity semantics;
- define effect atomicity, idempotency, partial failure, retry classification,
  deletion and verification behavior;
- add fake conformance and opt-in live acceptance evidence;
- unknown behavior is a capability limitation, not a favorable default.

**CLI:**

- preserve existing commands and exit-code conventions;
- separate human output from deterministic machine schema;
- never print a secret except the one-time explicit key creation result;
- update tests and regenerate `docs/src/content/docs/cli.mdx` through hooks.

**Cloud service:**

- authenticate with the pinned Clerk backend SDK, require the expected token
  type/instance/Organization, map Clerk IDs to internal IDs, authorize the
  exact resource, then load/mutate through a tenant-scoped Neon transaction;
- API/workers use the Neon pooled URL and transaction-local tenant context;
  only migration/backup automation uses the direct URL;
- mutations use idempotency and state-machine compare-and-set;
- audit/outbox are inserted in the same commit;
- Clerk webhooks are signature-verified, deduplicated reconciliation inputs,
  never the sole synchronous authorization check;
- secrets and customer payloads are rejected from structured telemetry;
- a queue/message is not authoritative state;
- every background job is restartable and deduplicating.

**Agent:**

- pipeline is a child/container, never an imported plugin;
- no user API key or cloud administrative credential;
- verify identity, lease generation, artifact digest/signature and policy before
  work;
- spool accepted evidence durably before acknowledging it locally;
- enforce resource/network policy outside Python;
- stop safely on incompatibility and preserve state for inspection.

### 20.3 When changing a cross-language path

Use this order:

```text
Rust data/behavior and tests
  -> PyO3 conversion/error boundary
  -> core.pyi
  -> Python internal wrapper
  -> smallest public surface, if proven necessary
  -> end-to-end Python test
  -> docs/example
  -> full Rust + Python checks
```

Do not duplicate the Rust decision logic in Python to avoid a binding change.
Do not expose raw native structs directly as a cloud wire format.

### 20.4 Before merging

Check:

- [ ] The diff contains only the intended task and preserves user changes.
- [ ] Public API, persisted state and wire-format changes are explicitly called
      out.
- [ ] Stable paths/fingerprints are unaffected or have migration proof.
- [ ] Failure before/after durable commit and external effect is tested.
- [ ] Retry and cancellation behavior is explicit.
- [ ] Logs/events/artifacts pass secret/PII canary scans.
- [ ] Tenant and negative authorization cases exist where relevant.
- [ ] Metrics have bounded cardinality and no sensitive labels.
- [ ] Rollback works with deployed schema/protocol/state versions.
- [ ] Correct formatting, type, unit, integration and pre-commit checks pass.
- [ ] Docs state only guarantees supported by the tested boundary.

### 20.5 Patterns that would break the project

Do not:

- add `organization_id` or `run_id` to component paths to make cloud records
  unique;
- replace LMDB ownership with rows asynchronously mirrored to PostgreSQL;
- use Neon Auth/Data API or a browser database connection alongside Clerk and
  the server-side authorization boundary;
- rely on session-level `SET`, `LISTEN`, or advisory locks through Neon's
  transaction pooler;
- create PR/test Neon branches from raw production data without an approved
  anonymization or schema-only policy;
- call connector targets from the API service;
- make engine commit wait on a telemetry/evidence network request;
- retry an ambiguous apply because a queue message was redelivered;
- copy a live LMDB directory with generic filesystem tooling;
- mount one state volume read-write into two runners/tasks;
- treat local Python egress checks as a sandbox;
- upload arbitrary dictionaries after best-effort key-name redaction;
- give a runner a human/service-account API key;
- accept a Clerk key without an active local service-account binding, or store
  its plaintext secret/custom digest in Neon;
- put connector credentials in package/config/evidence records;
- expose the loopback local dashboard as the cloud console;
- add cloud server/billing dependencies to the OSS SDK package;
- claim strict erasure for direct target queries or unregistered caches;
- mark a connector certified using only mocks.

---

## 21. Risks, decisions, and open questions

### 21.1 Principal risks

| Risk | Impact | Early signal | Mitigation/owner |
|---|---|---|---|
| Integrity wedge is not budget-worthy | Product is technically strong but not purchased | Scans find little material drift; no design partner will pay | Product: validate in Phase 0/1 before managed compute |
| Arbitrary Python broadens security scope | Control/tenant compromise | Requests for in-process hosted execution or privileged custom images | Security/platform: isolated runner/task boundary and controlled build profiles |
| LMDB conflicts with horizontal execution | Corruption or stale ownership | Multiple runners scheduled for one environment; volume attach ambiguity | Engine/platform: one writer, dual leases, generation fence, restore drills |
| Connector semantics are overgeneralized | Duplicate/destructive effects | Ambiguous failure routinely auto-retried | Connector owner: factual per-operation certification and fail closed |
| Evidence leaks customer data | Security/privacy incident | High-cardinality paths/messages in logs/events | Security: typed allow-list, canaries, local raw logs, access/retention controls |
| Clerk outage or identity/key drift | Login/API-key outage or stale access | Verification errors, unmatched Organization/key IDs, webhook backlog | Identity owner: local session JWT verification, fail-closed key/high-risk checks, signed idempotent reconciliation and vendor SLO monitoring |
| Neon pooled tenant context leaks | Cross-tenant database access | Query succeeds without context or after alternating tenant reuse | Database owner: explicit transaction + `set_config(..., true)`, non-owner roles, RLS default deny and live pooled tests |
| Neon branches copy sensitive production data | Privacy/security incident | PR branch contains production canaries or persists past TTL | Database/security: schema-only/anonymized base, branch policy/audit/expiry and protected production branch |
| Clerk/Neon plan or pricing changes | Feature gap or margin regression | API-key verification, retained organizations, compute/storage/network usage exceed model | Product/platform: contracted tiers, usage dashboards, abstraction at adapters and quarterly exit-cost review |
| Reconciliation cannot meet enterprise scale | Slow scans/runs and excess memory | 100k fixtures near limits; full scan after every small change | Engine: paged journals/cursors and scale gates before claims |
| Supply-chain compromise | Malicious code runs with data credentials | Unsigned inputs, networked build hooks, mutable tags | Platform security: isolated builds, hashes, SBOM/provenance/signature verification |
| Too many services too early | Slow delivery and fragile operations | Team spends more time on deployment than pilot value | Architecture: modular monolith plus workers; split only on measured need |
| Enterprise promises precede operations | Churn/liability | SLA/erasure answers rely on roadmap | Leadership: release gates, guarantee matrix, evidence-linked questionnaire |
| Cloud costs exceed pricing | Negative margin | Long scans/builds, large logs/state snapshots | Product/SRE: metering, quotas, unit-cost dashboard and simple pricing |
| Runner install is too difficult | Pilots stall at security/network setup | >1 day to healthy heartbeat; proxy/CA failures | Agent/solutions: Helm/Terraform, preflight, outbound-only protocol, clear diagnostics |
| Distributed revocation weakens ordering | Serving returns revoked data | Multiple workers share non-transactional store | Revocation owner: separate transactional design before cloud erasure tier |
| OSS/commercial boundary creates trust concern | Adoption/community resistance | Hidden protocols or crippled local tool | Leadership: open engine/agent/contracts, transparent guarantees and licensing ADR |
| Support load scales with connectors | Poor margin and reliability | Bespoke provider debugging per tenant | Product: small certified matrix, versioned conformance and priced enterprise support |

### 21.2 Decisions made by this plan

1. Hybrid customer-hosted runners precede managed execution.
2. API keys are scoped service-account credentials for the control plane, not
   connector or runner credentials.
3. Clerk owns human authentication, Organizations, enterprise federation/
   Directory Sync, and organization-scoped customer API-key validity; Synor
   owns product authorization and local key bindings; runner identity uses
   short-lived mTLS.
4. Neon hosts the control-plane PostgreSQL. Runtime uses pooled connections,
   migration/backup uses direct connections, and server-side standard RLS
   defends tenant isolation. Neon Auth/Data API are not used.
5. Control plane starts as a modular monolith with Neon PostgreSQL,
   outbox/queue, object storage and OCI registry.
6. This repository stays the OSS engine/SDK/connectors; cloud service code is
   private and agent code is preferably public.
7. LMDB remains the execution source of truth with one fenced writer per
   environment.
8. Native Synor generates/applies plans; the cloud coordinates and approves.
9. Delivery is at least once; run/attempt/idempotency/fencing provide safety.
10. Evidence crosses the runner only through typed allow-listed schemas.
11. Index Integrity is the initial product wedge; Verified Erasure is gated by
    the dedicated remaining assurance work.
12. Managed execution uses isolated per-attempt workloads, not a shared Python
    service.
13. Connector certification controls advertised behavior and automated retry.

### 21.3 Questions that must be resolved by evidence

| Question | Decision deadline | Default if evidence is absent |
|---|---|---|
| First reference source: S3 or Google Drive? | End Phase 0 | S3 for simpler integrity MVP; Google Drive next for governed path |
| Is Qdrant the first target customers will pay to repair? | End Phase 0 | Qdrant, then PostgreSQL/pgvector |
| Does `synor-agent` live in this workspace or a separate public repo? | Before `AGENT-00` | Separate public repo and versioned protocol |
| Which AWS/Neon region is required by first customers? | Before Phase 2 IaC | One matching US execution/Neon region, with tenant-home-region pinning |
| Which Clerk and Neon plans are required? | Before `CLOUD-00` | Do not launch production until Organization API keys, required custom roles, protected branches, restore window, network controls, SSO/SCIM roadmap, support, and cost are contractually verified |
| Which Clerk coarse roles map to each Synor product role? | Before `CLOUD-02` | Clerk admin/member for membership administration; Neon role bindings remain product authority |
| What Neon restore window and independent logical-backup cadence meet pilot RPO? | Before production mutations | Configure the measured pilot RPO and rehearse both PITR branch and independent restore |
| Kubernetes Job, dedicated VM/containerd, or both for reference runner? | Phase 3 spike | Kubernetes reference, dedicated VM fallback; no generic Docker-socket enterprise mode |
| What exact record identity exists in unmanaged customer indexes? | Phase 1 pilots | Require explicit mapping/adoption; classify unknown as unverifiable |
| What is the safe multi-process revocation ledger backend? | Before Phase 9 | No distributed revocation apply until transactional design is accepted |
| What volumes and change rates define the first paid tier? | End Phase 1 | Publish conservative measured limits, not aspirational limits |
| What data/retention/residency contract do pilots require? | Before storing pilot evidence | Minimum metadata retention, Clerk/Neon subprocessor disclosure, and tenant-home-region pinning |
| What pricing metric best predicts value and cost? | Phase 6 | Records/documents under management plus platform and managed compute |
| Is live/continuous mode required in year one? | After paid batch pilots | Batch/scheduled only; design live workers separately |

Record answers as ADR/product-decision updates. Do not silently encode them as
hardcoded behavior in a connector or scheduler.

---

## 22. Release gates

Calendar dates never override these gates.

### 22.1 Internal developer preview

- local read-only scanner works on synthetic/disposable provider data;
- Clerk development-instance session authentication, Organization mapping, and
  wrong-Organization negative tests pass;
- Neon pooled runtime transactions default-deny without tenant context and
  remain isolated while alternating tenants on reused connections;
- enrolled runner executes plan-only fixtures;
- typed event allow-list passes canary scan;
- no production customer credentials or data;
- all capabilities labeled experimental.

### 22.2 Design-partner alpha

- two non-production customer runners healthy for one week;
- immutable signed deployment verified by runner;
- read-only scan results manually validated;
- Clerk Organization API-key create/use/local-deny/remote-revoke/reconcile and
  runner mTLS identity rotation/revocation drills pass;
- a Neon point-in-time branch restore and an independent logical-backup restore
  are both validated without using a raw production branch for testing;
- support, incident contact, backup ownership and data flow agreed;
- plan-only default; repair requires per-tenant engineering approval.

### 22.3 Paid beta

- three paying partners and a documented onboarding path;
- one apply-certified connector path with plan/approval/post-verification;
- state backup/restore and ambiguous-apply runbook exercised;
- billing ledger reconciles and usage is visible before invoice;
- on-call, status communication, vulnerability/incident policies active;
- external security review of public API and customer runner completed;
- published limits and guarantee matrix, including unsupported cases.

### 22.4 Production general availability

- representative 30-day SLO measurements and error-budget process;
- cross-tenant suite covers every public route and object access path;
- Clerk login, API-key verification, webhook delay/replay, and provider-outage
  behavior meet the documented fail-open/fail-closed matrix;
- Neon pooled/direct connection budgets, protected-branch controls, failover,
  point-in-time restore, and independent restore meet the production RPO/RTO;
- connector live acceptance, fault injection, scale and restore gates current;
- supply-chain attestations/signatures verified at build and runner;
- DR and customer deletion drills pass;
- upgrade/rollback compatibility covers supported agent/engine versions;
- support staffing and severity response commitments are operational;
- no critical/high unaccepted penetration-test finding.

### 22.5 Managed execution GA

- all production GA gates plus isolated workload penetration test;
- state-volume fence/snapshot/restore and regional recovery proven;
- private networking and managed secret lifecycle documented/tested;
- unit economics and quotas prevent uncontrolled loss/noisy neighbor;
- customer-hosted runner remains a supported fallback.

### 22.6 Enterprise GA

- Clerk Enterprise Connections SAML/OIDC and Directory Sync lifecycle tests,
  including deprovisioning, group/role drift, domain ownership, and break-glass;
- regional residency, Neon Private Networking/PrivateLink, Clerk/Neon
  subprocessor disclosures, and audit export paths;
- access review, support grants, legal hold, retention/deletion;
- assurance audit status represented accurately;
- contract SLA backed by measured operations and DR;
- one reference customer completes procurement and production onboarding.

### 22.7 Verified Erasure GA

- dedicated revocation plan's remaining Phase 7/8 and GA gates complete;
- distributed transactional ordering, retrieval/cache and restore gates proven;
- at least two source/target boundaries certified at the advertised level;
- receipts independently verify and evidence retention is agreed;
- legal/security approve the exact bounded claim and exclusions.

---

## 23. Confirmed facts, planning assumptions, and unknowns

### 23.1 Confirmed at the repository baseline

- Synor is an Apache-2.0 Python/Rust project at Python `0.1.0a1` and Rust
  `0.1.0-alpha.1`.
- The Python package contains 126 tracked Python/stub files, tests contain 167
  tracked Python modules, and Rust source contains 232 files at the documented
  baseline.
- There are 20 connector directories spanning file/object, SQL, vector, graph,
  streaming, and warehouse systems.
- The public model is declarative processing with stable component paths,
  target-state ownership, memoization, reconciliation, and cleanup.
- The engine persists authoritative execution state in LMDB and batches writes
  through `Storage::run_txn`.
- `SynorRuntime` already provides the local controlled plan/run/explain/replay
  seam plus PII, audit, provenance and revocation integrations.
- The state-store protocol has only simple key operations; its lock explicitly
  does not make a file/memory store a transactional distributed backend.
- The deterministic source packager is useful but is not a fully locked,
  signed OCI deployment pipeline.
- The local dashboard is intentionally loopback-oriented and unauthenticated;
  it is not a hosted console.
- Preview executes ordinary Python and is not an operating-system sandbox.
- Target atomicity/retry behavior varies by connector. The capability inventory
  has many sink paths but only a limited subset has stronger certification.
- Rust SDK code exists independently of PyO3, so cloud contracts should be
  language-neutral.
- Native effect schema activation and downgrade already have strict copy-based
  operational rules.
- The release workflow already uses GitHub artifact attestations.
- No organization/user/API-key, runner fleet, hosted scheduler, multi-tenant
  control database, billing system, or managed execution plane exists in this
  repository.
- The dedicated provable-revocation plan marks drift/cache/restore assurance and
  connector expansion/GA hardening as not started at this baseline.

### 23.2 Planning assumptions

- A small team wants commercial evidence within one or two quarters.
- Customers prefer source/target credentials and sensitive content to remain in
  their environment initially.
- Scheduled batch integrity and repair can prove value before managed live
  processing.
- Neon PostgreSQL, a managed queue, S3-compatible object storage, and OCI
  registry are acceptable control-plane primitives.
- AWS is a reference deployment, not an engine-level dependency.
- Clerk is acceptable as the external human, Organization, and customer API-key
  provider; Synor still owns every domain authorization decision and local deny
  rule.
- Clerk and Neon are selected architecture dependencies for the future cloud
  product, not capabilities already implemented in this OSS repository.
- A managed billing provider is preferable to building payment collection.
- The first product can support a narrow certified connector matrix.
- Customers accept at-least-once dispatch when effect boundaries, idempotency,
  approvals and recovery are honest.
- The agent may be released openly to improve inspectability and adoption.

### 23.3 Unknown until measured or decided

- Which connector pair has the strongest urgent paid demand.
- Real customer corpus sizes, churn rates, permission complexity and provider
  throttling behavior.
- Acceptable false-positive/unknown rate for unmanaged integrity scans.
- Whether current full reconciliation meets the first enterprise percentile.
- Exact state snapshot frequency, size, upload time and recovery cost.
- First-customer regional, private-network, customer-managed-key and compliance
  requirements.
- Contracted Clerk plan limits/cost for Organization API keys, custom roles,
  Enterprise Connections, Directory Sync, support, and data handling.
- Contracted Neon plan limits/cost for regions, protected branches, restore
  history, Private Networking, compute, storage, and network transfer.
- Required live/streaming versus scheduled-batch semantics.
- The team composition, budget, cloud-provider commitments and target launch
  date.
- Final billing, scanning/signing, and notification vendors.
- Exact transactional backend and distributed ordering model for cloud
  revocation.
- Commercial packaging, included usage, SLA and support staffing.

Unknowns stay visible in the backlog and release gates. They are not permission
to make a broader reliability or compliance claim.

---

## 24. Primary references

### 24.1 Repository sources

- [README](../../README.md)
- [Execution-model reading guide](../../reading.md)
- [Product ideas and market wedge](../../PRODUCT_IDEAS.md)
- [ADR-0001: execution control](ADR-0001-phase-2-execution-control.md)
- [ADR-0002: trustworthy local execution](ADR-0002-phase-3-trustworthy-local-execution.md)
- [ADR-0003: provable index revocation](ADR-0003-provable-index-revocation.md)
- [ADR-0004: cloud repository, Clerk, and Neon boundaries](ADR-0004-cloud-repository-clerk-neon-boundaries.md)
- [Provable index revocation implementation plan](provable-index-revocation-implementation-plan.md)
- [Reliability hardening status](reliability-hardening-status.md)
- [Native effect operational runbook](native-effect-operations-runbook.md)
- [Phase 6 validation](revocation-phase6-validation.md)
- [Target sink certification data](../../dev/target-sink-certification.json)

### 24.2 External primary guidance

- [Clerk: Organizations](https://clerk.com/docs/guides/organizations/create-and-manage)
- [Clerk: Organization roles and permissions](https://clerk.com/docs/guides/organizations/control-access/roles-and-permissions)
- [Clerk: session tokens](https://clerk.com/docs/guides/sessions/session-tokens)
- [Clerk: authenticate a backend request](https://clerk.com/docs/reference/backend/authenticate-request)
- [Clerk: Python backend authentication](https://clerk.com/articles/how-to-add-authentication-to-a-python-backend)
- [Clerk: API keys](https://clerk.com/docs/guides/development/machine-auth/api-keys)
- [Clerk: machine authentication](https://clerk.com/docs/guides/development/machine-auth/overview)
- [Clerk: webhooks](https://clerk.com/docs/guides/development/webhooks/overview)
- [Clerk: syncing data with webhooks](https://clerk.com/docs/guides/development/webhooks/syncing)
- [Clerk: Enterprise Connections](https://clerk.com/docs/guides/configure/auth-strategies/enterprise-connections/overview)
- [Clerk: Directory Sync](https://clerk.com/docs/guides/configure/auth-strategies/enterprise-connections/directory-sync)
- [Clerk: self-serve SSO](https://clerk.com/docs/guides/configure/auth-strategies/enterprise-connections/self-serve-sso)
- [Neon: connection pooling](https://neon.com/docs/connect/connection-pooling)
- [Neon: projects](https://neon.com/docs/manage/projects)
- [Neon: compute lifecycle](https://neon.com/docs/manage/endpoints/)
- [Neon: scale to zero](https://neon.com/docs/introduction/scale-to-zero)
- [Neon: branching](https://neon.com/docs/guides/branching-intro)
- [Neon: protected branches](https://neon.com/docs/guides/protected-branches)
- [Neon: security overview](https://neon.com/docs/security/security-overview)
- [Neon: row-level security](https://neon.com/docs/guides/row-level-security)
- [Neon: network transfer](https://neon.com/docs/introduction/network-transfer)
- [Neon: point-in-time restore](https://neon.com/blog/announcing-point-in-time-restore)
- [OAuth 2.0 Security Best Current Practice — RFC 9700](https://datatracker.ietf.org/doc/html/rfc9700)
- [Problem Details for HTTP APIs — RFC 9457](https://datatracker.ietf.org/doc/html/rfc9457)
- [OpenAPI Specification](https://spec.openapis.org/oas/)
- [PostgreSQL row security policies](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)
- [NIST SP 800-207 Zero Trust Architecture](https://csrc.nist.gov/pubs/sp/800/207/final)
- [SPIFFE concepts](https://spiffe.io/docs/latest/spiffe/concepts/)
- [SPIFFE X.509-SVID](https://spiffe.io/docs/latest/spiffe-specs/x509-svid/)
- [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)
- [OpenTelemetry agent-to-gateway deployment](https://opentelemetry.io/docs/collector/deploy/other/agent-to-gateway/)
- [OCI image manifest](https://github.com/opencontainers/image-spec/blob/main/manifest.md)
- [OCI content descriptor](https://github.com/opencontainers/image-spec/blob/main/descriptor.md)
- [SLSA provenance](https://slsa.dev/spec/v1.2/provenance)
- [Sigstore Cosign verification](https://docs.sigstore.dev/cosign/verifying/verify/)
- [GitHub artifact attestations](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations)
- [Kubernetes Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/)
- [OWASP REST Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/REST_Security_Cheat_Sheet.html)
- [OWASP API1: Broken Object Level Authorization](https://owasp.org/API-Security/editions/2023/en/0xa1-broken-object-level-authorization/)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [AWS Secrets Manager encryption](https://docs.aws.amazon.com/secretsmanager/latest/userguide/security-encryption.html)
- [AWS Fargate security](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/security-fargate.html)
- [AWS Fargate task networking](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-task-networking.html)
- [Amazon EBS volumes for ECS](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ebs-volumes.html)
- [Application-consistent EBS snapshots](https://docs.aws.amazon.com/ebs/latest/userguide/automate-app-consistent-backups.html)
- [Amazon S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
- [Stripe usage-based billing](https://docs.stripe.com/billing/subscriptions/usage-based/how-it-works)
- [Stripe usage recording and idempotency](https://docs.stripe.com/billing/subscriptions/usage-based/recording-usage-api)

External guidance informs the design but does not prove implementation. Synor's
tests, validation records, operational drills, and deployed controls remain the
evidence for product claims.

---

## 25. Engineering compass

**Keep Synor's deterministic local ownership and reconciliation as the trusted
execution core; put identity, approval, scheduling, evidence, billing, and fleet
operations around it; and never let cloud convenience weaken isolation,
fencing, privacy, recoverability, or the precision of the product's claims.**
