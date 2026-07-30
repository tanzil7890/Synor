# Provable Index Revocation: implementation plan

- **Status:** Phases 0–5 ✅ complete for their explicitly bounded milestones.
  Phase 6 now has a bounded native implementation milestone: additive LMDB
  schema v3/effect-record v2 state, immutable evidence lineages,
  verified-sink capability, write-free strict preview, strict provider
  blockers with fresh-process recovery, retained evidence, rich tombstones,
  and in-process live queue/generation fencing are implemented. Phase 6 is not
  production/GA complete because kill/power-loss certification, remote or
  multi-process fencing, the copied-database migration drill, scale and
  compatibility-overhead benchmarks, and the complete live delete/reinsert
  race remain open.
  Phase 5 now includes the opt-in public governance/revocation/retrieval API,
  strict `SynorRuntime` policy and startup health, schema-compatible reports,
  privacy-safe operator CLI, generated documentation, and the reproducible
  flagship reference product. Live Workspace/Qdrant acceptance, a real
  Drive-to-Qdrant declarative handler, corpus-wide guarded retrieval, native
  durability certification, drift repair, and end-to-end memory certification
  remain open and are not implied by this public/reference milestone.
- **Date:** 2026-07-30
- **Target line:** Synor v1 / `0.1.0a1`
- **Primary vertical slice:** Google Drive source → governed text chunks → Qdrant
- **Audience:** Synor maintainers, connector authors, security reviewers, and
  operators

This document is the development map for making the following problem Synor's
flagship:

> Deleted, expired, moved, or de-permissioned source content can remain
> retrievable from AI indexes, vector stores, caches, and other derivatives
> after the source says it should no longer be available.

The intended product capability is **Provable Index Revocation**:

> For supported strict-mode connectors, Synor blocks retrieval as soon as a
> revocation is observed, propagates the required change to every Synor-owned
> derivative, verifies the destination postcondition, and retains privacy-safe
> evidence that the revocation completed or remains blocked.

This is deliberately narrower and more defensible than “instant deletion
everywhere” or “automatic compliance.” It creates an industry-grade foundation
that later sources, targets, caches, policy engines, and evidence stores can
extend without replacing Synor's reconciliation engine.

---

## 1. How to use this plan

Implement the phases in order. Each phase has a compatibility boundary, tests,
an exit gate, and a rollback rule. Do not publish the strict-mode product claim
until all launch gates in section 22 pass.

The words **current**, **confirmed**, and **existing** describe repository
behavior observed through 2026-07-30. The words **implemented** and checked
status items describe the bounded Phase 0–6 source state validated on that
date. The words **proposed**, **must**, and **should** describe work not yet
completed unless a nearby implementation-status block says otherwise.

### 1.1 Repository-verified completion snapshot

| Phase | Status | Repository-verified scope |
|---|---|---|
| Phase 0 | **Complete for the internal milestone** | Contract, failure harnesses, invariant tests, Qdrant collection-delete exception safety, and compatibility baseline are complete. Two explicitly labeled native/public assertions remain deferred to Phases 5–6 and are not Phase 0–2 exit blockers. |
| Phase 1 | **Complete** | Internal governance model, deterministic identity, monotonic suppression, StateStore-persisted ledger, receipt integrity, corruption handling, and recovery under the documented single-process/single-event-loop boundary are implemented and tested. |
| Phase 2 | **✅ Complete and revalidated for the internal synthetic milestone** | Verified sink, guarded retrieval, StateStore-persisted pre-observation serving fences, retry/reconstruction convergence, legal-hold handling, privacy-safe evidence, and the service-free end-to-end slice are implemented and tested. The pre-Phase-6 baseline was re-run on 2026-07-30. This is not power-loss or multi-process certification. |
| Phase 3 | **✅ Complete for the governed source milestone** | Stable Drive-ID identity, privacy-safe ACL normalization, authoritative snapshots, user/shared-drive and drive-authority change replay, bounded descendant invalidation, strict newly-in-scope subtree discovery, permission expiry, semantic-policy rescan fencing, explicit ambiguity, and durable-readiness checkpointing are implemented and fake-service tested. Live Workspace acceptance remains operator-gated. |
| Phase 4 | **✅ Complete for the internal certified-adapter milestone** | Additive governed point lineage/content binding, exact Phase 2 suppression-state freshness/current-generation checks, mandatory source/tenant/policy/generation/principal filters, configured-`WCF == RF` preflight plus strong completed writes, collection/index and RF>1 replicated-topology preflight, target-native suppression, exact-ID delete, ACL narrowing, strict collection absence, durable provider operation evidence, a 30-second Qdrant operation timeout, caller-bounded metadata reads, compatibility coverage, and runtime reconstruction are locally tested. Principal authentication/context issuance, live acceptance, and a production declarative Qdrant handler remain open. `consistency=all` proves guarded non-return for the verified operation, not physical per-replica absence. |
| Phase 5 | **✅ Complete for the public/reference milestone** | Minimal public types alias the proven schemas; strict `SynorRuntime` adds startup repair/health, controlled finalization, explicit outcome status, and optional revocation summaries; the redacted/versioned operator CLI and generated docs are tested; and the two-tenant local flagship runs twice with ACL-only revocation, pre-score suppression, delayed convergence, receipts, partial-scan safety, restore non-resurrection, and content-free evidence. Optional real Drive/Qdrant mode is configuration-only; live acceptance and a production declarative handler remain open. |
| Phase 6 | **✅ Implemented for the bounded native milestone; certification gates open** | Schema-v3/effect-record-v2 LMDB effects with immutable per-locator evidence lineages, safe status-count inspection, verified-sink/PyO3 integration, write-free strict preview, precommit → verified → final-commit ordering, strict provider blockers and fresh-process recovery, drop protection, tracking-owner deletion repair, rich backward-compatible tombstones, durable local live generations, and serialized live/plain/delete transitions are implemented. `SIGKILL`/power-loss certification, remote CAS or multi-process fencing, copied real pre-feature migration, the million-action correlation run, native concurrency/performance benchmarks, and the complete live delete/reinsert race remain open. |
| Phase 7 | **Not started** | Drift/orphan scanning, cache-recipient assurance, and restore gating remain planned. |
| Phase 8 | **Not started** | Connector expansion, conformance kit, SLOs, and GA hardening remain planned. |

Status notation used below:

- `[x]` / `✅` means implemented and verified in the repository.
- `[ ]` means not implemented.
- **Deferred** means intentionally assigned to a later phase and excluded from
  the current bounded milestone gates.

Keep the following development rules throughout:

1. Preserve `App.update()`, stable component paths, target ownership,
   reconciliation, and retry behavior.
2. Add the new capability around the current engine first. Do not rewrite
   reconciliation or introduce a second source of target truth.
3. Keep compatibility mode as the default until the strict connector contract
   is stable and documented.
4. In strict mode, an unverified delete is not success.
5. Never turn a partial, failed, or permission-denied source scan into an
   authoritative mass deletion.
6. Keep evidence metadata-only. Do not copy document text, chunks, vectors,
   raw credentials, emails, or display names into receipts.
7. Make each pull request independently reviewable and reversible.
8. If Rust or LMDB changes are eventually needed, all LMDB writes must use
   `Storage::run_txn`, and the Python bridge stubs in
   `python/synor/_internal/core.pyi` must stay synchronized.

---

## 2. Executive decision

### 2.1 The flagship promise

The launch promise should be:

> Synor makes a governed source item non-retrievable when its authorization is
> revoked, then proves that every configured Synor-owned derivative is absent
> or legally isolated.

“Non-retrievable” and “physically erased” are different postconditions:

- **Serving suppression** means the item cannot pass the supported retrieval
  boundary.
- **Logical deletion** means the active destination no longer returns the
  artifact through its documented query path.
- **Physical erasure** means storage versions, compaction history, replicas,
  backups, and other retained bytes have been removed according to a
  destination-specific retention contract.

The first release must guarantee serving suppression and verified logical
deletion for its certified connector pair. Physical-erasure claims require a
separate connector capability and retention proof.

### 2.2 Why this is a strong startup wedge

Enterprise RAG and AI-search teams commonly solve ingestion but struggle to
answer four security-review questions:

1. How quickly does a source deletion or permission change stop retrieval?
2. How do you know every chunk, embedding, row, and cache entry was affected?
3. What happens when a source feed is incomplete or a vector database is only
   eventually consistent?
4. What evidence remains after the deleted artifact itself is gone?

Major search products already support ACL-bearing documents. ACL propagation is
therefore table stakes. Synor can differentiate on the full lifecycle:
stable lineage, bounded revocation latency, fail-closed retrieval, connector
capability disclosure, negative verification, orphan scanning, restore safety,
and evidence receipts.

Likely initial users are:

- teams shipping enterprise copilots over Google Drive, SharePoint, S3, or
  internal databases;
- AI-platform teams blocked by security, privacy, or customer data-isolation
  reviews;
- regulated or contractually sensitive applications that need deletion and
  access-revocation evidence;
- connector and RAG vendors that need a reusable convergence and verification
  layer instead of implementing it independently for every destination.

### 2.3 Claims that must not be made

Do not claim that Synor:

- automatically satisfies GDPR, the EU AI Act, or another law;
- unlearns data from model weights;
- removes human-created copies or unmanaged exports;
- controls a destination's undisclosed backups or replication internals;
- provides instantaneous physical erasure on eventually consistent stores;
- protects queries that bypass the certified retrieval guard or target-native
  ACL filter;
- can distinguish physical source deletion from loss of access when the source
  API itself does not provide that fact.

### 2.4 Assurance vocabulary

Every source/target pair should report one of these evidence levels:

| Level | Meaning | May strict mode claim completion? |
|---|---|---|
| `unverified` | The connector returned without raising; the external postcondition was not read back. | No |
| `acknowledged` | The destination accepted or completed an operation, but query visibility was not verified. | No |
| `query_verified` | The supported retrieval/read path confirmed that the artifact or old ACL is no longer visible. | Yes |
| `erasure_attested` | The destination-specific retention/physical-erasure contract also completed. | Yes, only for that narrower contract |
| `retained_isolated` | A policy or legal hold requires retention, but the artifact is isolated from serving and its restriction was verified. | Yes, as restriction—not erasure |

Compatibility mode may continue using unverified connectors. Strict mode must
refuse a required postcondition that the selected connector cannot prove.

---

## 3. The concrete failure and compatibility boundary

Synor already has a valuable reconciliation model:

```text
desired component state
  → LMDB precommit and ownership claim
  → connector sink mutates the external target
  → LMDB final commit prunes old/deleted tracking
```

If a sink raises, Synor preserves multi-state uncertainty and retries an
idempotent action later. That is the right crash-recovery foundation.

For a legacy compatibility sink, the critical gap remains the success
boundary:

1. The engine calls `TargetActionSink.apply()`.
2. A normal return is treated as successful external application.
3. Final commit removes the deleted target tracking and owner-index entry.
4. No generic target read-back or deletion receipt occurs.

If a connector returns normally while the external artifact still exists,
Synor can forget the only managed path that would have retried the deletion.
The orphan is then both leaked and untracked.

Phase 6 closes this boundary only for described effects emitted by the
query-verified sink capability: strict cleanup rejects a legacy sink before
apply, native intent precedes apply, and completion occurs with final tracking
commit. Direct compatibility mode intentionally retains the legacy behavior
and makes no governed revocation claim.

Before Phase 0, the Qdrant collection handler in
`python/synor/connectors/qdrant/_target.py` caught every exception from
`delete_collection()` and continued. The Phase 0 regression first reproduced
that unsafe success boundary; the implementation now treats only a
provider-confirmed REST `404` or gRPC `NOT_FOUND` as idempotent absence and
requires a literal `True` confirmation after a normal return. Authentication,
transport, timeout, server, `False`, and malformed-return outcomes remain
retryable failures. Phase 4 now layers certified point deletion, target-native
suppression, source-scoped guarded filters, configured-`WCF == RF` preflight,
`consistency=all` non-return read-back, and verified collection absence on top
of that compatibility-safe behavior. This is not a physical per-replica
absence claim.

This is the flagship invariant:

> Synor may report a governed target effect as complete only when the
> connector has established the required external postcondition. A failed or
> unknown verification must raise before final tracking commit.

The other half is equally important:

> Synor may infer authoritative removal only from a trustworthy source signal
> or a successfully completed source snapshot. “Not seen” during an incomplete
> scan is not deletion.

---

## 4. Confirmed repository map

This is the relevant file-by-file map. It intentionally focuses on files that
participate in observation, ownership, application, evidence, and operator
behavior rather than listing unrelated operations and connectors.

| File or area | Current responsibility | Revocation implication |
|---|---|---|
| `rust/core/src/engine/execution.rs` | Reconciles desired target states, performs write-free preview planning, precommits tracking plus described native effects, applies sinks, records verification/failure, and final-commits tracking/effects. | Strict cleanup rejects legacy sinks before apply, retains missing-provider blockers, validates evidence lineage on retry, and completes effects only in final tracking commit. |
| `rust/core/src/engine/target_state.rs` | Batches flat sink actions and declares internal legacy/query-verified assurance plus safe effect description. | The existing action order and four-field reconcile output remain intact; legacy sinks stay compatible. |
| `rust/core/src/state_store/submit_session.rs` | Atomically writes ownership, tracking, native intents, final effect status, and existence/tombstone reconciliation inside LMDB transactions. | Every new write remains inside a caller-owned transaction opened through `Storage::run_txn`. |
| `rust/core/src/state/db_schema.rs` and `state/native_effect.rs` | Define target ownership, schema-v3/effect-record-v2 native evidence, `0x48` obligation and `0x50` lineage cursors, component existence, and rich compatible tombstones. | Connector action IDs remain receipt-correlation IDs; engine evidence IDs are epoch-derived and immutable. Legacy values have safe defaults, supported v1/v2 state upgrades before write, and future/corrupt native state fails closed. |
| `rust/core/src/engine/live_component.rs` | Applies live work with a durable local generation, generation-checked committed state, cancellation fence, one per-subpath live/plain/delete queue, and generation-bound tombstones. | The latest queued transition wins and a handoff timeout fails without installing a successor. Remote CAS, a multi-process live lease, and the complete deterministic delete/reinsert race remain open. |
| `rust/core/src/engine/app.rs` | Configures compatibility/strict root update behavior and protected drop. | Strict updates surface unresolved native effects; drop refuses every non-completed effect and otherwise retains native evidence. |
| `rust/py/src/target_state.rs` | Bridges the unchanged four-field reconcile output plus optional verified-sink descriptors. | Invalid descriptors become fixed redacted errors; no fifth tuple field was added. |
| `python/synor/_internal/target_state.py` and `_verified_sink.py` | Define legacy sink factories and the internal query-verified wrapper/describer. | A sink still returns optional child handlers; assurance/effect description uses a separate private capability. |
| `python/synor/connectorkits/statediff.py` | Produces idempotent insert/upsert/replace/delete actions from tracked state. | Reuse it. It compares Synor tracking, not external state, so strict connectors still need read-back. |
| `python/synor/_internal/live_component.py` | Exposes `LiveMapSubscriber.update()` and `.delete(key)`. | A later additive API may carry lifecycle metadata, but the first slice can record cause in the governance ledger before calling the existing delete path. |
| `python/synor/resources/file.py` | Defines `FileLike`, size/mtime/content metadata, and memo state. | ACL-only changes are invisible because memo validation uses modified time and content fingerprint only. Do not break every `FileLike`; introduce a governed wrapper first. |
| `python/synor/connectors/google_drive/_source.py` | Recursively lists and downloads Drive files. | No change feed or ACL model exists. `items()` uses name/path keys even though the immutable Drive ID is available. Duplicate names, renames, and access changes are unsafe for governed identity. |
| `python/synor/connectors/localfs/_source.py` | Scans and watches local directories. | Permission/stat errors can be skipped, making an incomplete scan look successful. Strict snapshots need explicit completeness. ACL/mode changes are not memo inputs. |
| `python/synor/connectors/amazon_s3/_source.py` | Lists objects using size, last-modified time, and ETag. | Policy and ACL changes do not necessarily change content metadata. Effective IAM must be resolved by an explicit policy adapter, not guessed from object listing. |
| `python/synor/connectors/kafka/_source.py` | Converts keyed messages and null tombstones into a live map. | This is a good later transport for canonical governed events, but access and group revisions are not standardized yet. |
| `python/synor/connectors/qdrant/_target.py` and `_revocation.py` | Preserve compatibility reconciliation while adding the certified governed boundary. | Compatibility collection deletion accepts only confirmed not-found/literal-success results. The additive strict adapter now owns governed lineage/content binding, Phase 2 current-state checks, source-scoped guarded queries, configured-`WCF == RF` preflight plus strong completed writes, `consistency=all` exact-ID non-return read-back, ACL narrowing, durable operation evidence, the Qdrant operation-timeout parameter, caller-bounded metadata reads, and verified collection absence. Mutation transport deadlines remain part of finite `QdrantClient` configuration. |
| `python/synor/connectors/postgres/_target.py` | Reconciles tables and rows with idempotent SQL. | Deletes return no governed receipt or read-back. It is a good deterministic second target and optional evidence store. |
| `python/synor/connectors/localfs/_target.py` | Reconciles directories/files. | Delete calls are idempotent but unverified. Target child paths also require root-containment validation before a strong deletion claim. |
| `python/synor/connectors/lancedb/_target.py` | Reconciles rows/tables and performs best-effort maintenance. | Logical deletion and physical version reclamation are different. The default pruning window and ignored maintenance failures preclude an immediate erasure claim. |
| `python/synor/_internal/inspect_api.py` and `rust/core/src/inspect/db_inspect.rs` | Inspect local target tracking/ownership and aggregate native effect status counts. | Counts expose no descriptor or payload. `dangling` still means internal inconsistency, not external drift; a target verifier/scanner remains required. |
| `python/synor/provenance.py` | Captures current target paths, owners, and pipeline/package digests after a successful run. | Deleted targets are already pruned when capture occurs. Add a separate retained revocation ledger rather than stretching current provenance into deletion evidence. |
| `python/synor/audit.py` | Writes metadata-only manifests and audit JSONL with redaction and atomic replacement. | Reuse its redaction and filesystem-write patterns. Add optional schema-compatible revocation summaries, not artifact content. |
| `python/synor/state.py` | Provides async filesystem, memory, and encrypted control-plane stores. | Use it for the first single-process, single-event-loop revocation ledger. Do not treat it as a multi-key transactional database. |
| `python/synor/execution.py` | Wraps `App` with controlled plan/apply, audit, policy, and provenance. | This is the opt-in strict-mode entry point. Direct `App.update()` remains compatible and cannot receive the full product guarantee. |
| `python/synor/cli.py` | Provides plan, update, drop, replay, quarantine, dashboard, and inspection commands. | Add revocation operations only after the internal vertical slice is stable. Regenerate `docs/src/content/docs/cli.mdx` whenever this file changes. |
| `python/synor/__init__.py` | Intentionally re-exports public APIs. | Top-level governance/runtime exports are Phase 5-owned; the connector-scoped Phase 4 Qdrant surface remains additive and intentional. |
| `examples/gdrive_text_embedding/` | Demonstrates Drive-to-Postgres embeddings. | It currently stores no source ID or ACL and offers no guarded retrieval. Replace or add a governed reference example; do not silently imply the existing example is secure. |

### 4.1 Behavior that must be preserved

- Stable processing-component paths remain the basis of ownership.
- Each target state keeps exactly one current owner.
- A missing component removes its owned targets through durable cleanup.
- Connector actions remain idempotent and handle `prev_may_be_missing`.
- Sink failure preserves retryable uncertainty.
- Preview uses the same reconciliation path and performs no target writes.
- Existing connectors and `App.update()` continue to work in compatibility
  mode.
- The existing native LMDB schema is not changed during the first vertical
  slice.
- Artifact evidence remains metadata-only.

### 4.2 Existing test anchors

Build new tests around these suites instead of duplicating the engine:

- `python/tests/core/test_flat_target_states.py`: insert/update/delete, preview,
  sink failure, and retry;
- `python/tests/core/test_component_target_states.py`: nested and missing
  component cleanup;
- `python/tests/core/test_ownership_transfer.py`: one-owner transfer behavior;
- `python/tests/core/test_app_drop.py`: cleanup failure and retry preservation;
- `python/tests/core/test_live_component.py`: live delete, GC, error routing, and
  retry;
- `python/tests/common/target_states.py`: service-free target harness;
- `python/tests/test_execution.py` and `test_phase3_execution.py`: controlled
  runtime, manifests, and provenance;
- `python/tests/connectors/test_google_drive_source.py`: Drive helper/live-test
  pattern;
- `python/tests/connectors/test_qdrant_target.py`: Qdrant helper/live-test
  pattern.

---

## 5. Online research and industry requirements

Research in this section was checked against primary or official sources on
2026-07-29.

### 5.1 Security guidance

The [OWASP RAG Security Cheat
Sheet](https://cheatsheetseries.owasp.org/cheatsheets/RAG_Security_Cheat_Sheet.html)
identifies access-control inheritance as a common enterprise RAG failure. It
recommends access metadata on every chunk, authorization at retrieval time,
tenant isolation, cascading deletion across vector stores/caches/indexes,
deletion logs, orphan scans, and fail-closed behavior. These recommendations
map directly to Synor's required invariants.

[OWASP LLM08:2025 — Vector and Embedding
Weaknesses](https://genai.owasp.org/llmrisk/llm082025-vector-and-embedding-weaknesses/)
also treats cross-context leakage from shared vector stores as a material risk.
Tenant partitioning and permission-aware filtering must therefore be target
capabilities, not conventions hidden inside examples.

### 5.2 Source APIs are inherently ambiguous

[Google Drive's change-log
guide](https://developers.google.com/workspace/drive/api/guides/about-changes)
states that:

- a tombstone for an unavailable item contains only its ID;
- it does not distinguish physical deletion from loss of access;
- a corpus move can also appear as removal;
- user and shared-drive change logs must both be replayed; and
- parent permission changes affect descendants without one change event per
  child.

The guide was updated 2026-07-16. A Drive connector must therefore retain typed
events such as `access_lost`, `moved_scope`, and `ambiguous_removal`, and must
invalidate descendants when an inherited policy changes.

[Microsoft Graph drive-item
delta](https://learn.microsoft.com/en-us/graph/api/driveitem-delta?view=graph-rest-1.0)
similarly returns stable IDs and deletion facets. This supports the general
rule that provider-native IDs, not names or paths, are identity.

### 5.3 Destination acknowledgements are not proof

[Qdrant consistency
documentation](https://qdrant.tech/documentation/scaling/consistency-guarantees/)
documents configurable write ordering, write acknowledgement, and read
consistency. Strict deletion must select and record a consistency contract,
wait for application, and perform negative read verification.

[Pinecone deletion
documentation](https://docs.pinecone.io/guides/manage-data/delete-data) and
[freshness
documentation](https://docs.pinecone.io/guides/index-data/check-data-freshness)
show the broader pattern: vector mutations can be eventually consistent, and
write/query sequence information may be required before query visibility is
known.

[Amazon Kendra batch-deletion
documentation](https://docs.aws.amazon.com/kendra/latest/dg/delete-batch-documents.html)
describes asynchronous deletion that must be monitored until the document is
`NOT_FOUND`; the same page says Kendra will stop accepting new customers on
2026-07-30. It remains useful evidence of a real managed-index deletion
contract, not a recommendation to build the product around Kendra. The generic
lifecycle must separate `dispatched`, `acknowledged`, and `verified`.

[RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html) makes DELETE idempotent
at the HTTP semantic level, while an HTTP `202 Accepted` can mean the action is
not yet enacted. Synor must retry idempotently and must not treat “accepted” as
“verified absent.”

### 5.4 ACL-bearing search is already table stakes

Official product documentation shows that permission propagation alone is not
a sufficient differentiator:

- [Microsoft Graph external-item
  ACLs](https://learn.microsoft.com/en-us/graph/api/resources/externalconnectors-acl?view=graph-rest-1.0)
  support grant/deny entries and multiple identity types.
- [Google Agent Search data-source access
  control](https://docs.cloud.google.com/generative-ai-app-builder/docs/data-source-access-control)
  uses an identity provider and document ACL metadata.
- [Amazon Kendra user-context
  filtering](https://docs.aws.amazon.com/kendra/latest/dg/create-index-access-control.html)
  filters results using request user context for supported index types.

Synor's market differentiation must be the evidence-backed transition from
source change to retrieval denial to verified target convergence.

[Unstructured's Elasticsearch connector
documentation](https://docs.unstructured.io/api-reference/workflow/sources/elasticsearch)
requires a `record_id` for intelligent record updates. That is another signal
that stable source-to-derivative identity is expected infrastructure. Synor
should make it a cross-connector invariant and attach revocation assurance,
rather than leaving it as an isolated destination convention.

### 5.5 Regulatory design input, not a compliance claim

[GDPR Article
17](https://eur-lex.europa.eu/eli/reg/2016/679/oj/eng) provides a right to
erasure in specified situations and also includes exceptions.
[Article 18](https://eur-lex.europa.eu/eli/reg/2016/679/oj/eng) distinguishes
restriction of processing from destruction. [Article
19](https://eur-lex.europa.eu/eli/reg/2016/679/oj/eng) addresses communication
of rectification, erasure, or restriction to recipients.

Engineering implications:

- downstream indexes and caches should be represented as derivative
  recipients;
- policy may choose `destroy`, `restrict`, or `preserve_on_hold`;
- a legal hold can preserve bytes only in an isolated non-serving state;
- evidence must show which configured recipients completed;
- Synor must not decide legal applicability or advertise legal compliance.

The voluntary [NIST Generative AI
Profile](https://www.nist.gov/publications/artificial-intelligence-risk-management-framework-generative-artificial-intelligence)
supports retention policies, provenance, monitoring, response-time validation,
and third-party service expectations. This reinforces explicit connector
capabilities and measurable revocation SLOs.

---

## 6. Vocabulary and scope

### 6.1 Required terms

- **Source identity:** Stable provider-native identity of one source item
  within a connector instance and source scope.
- **Source observation:** A typed statement about an item or a completed source
  snapshot, including its confidence and cursor.
- **Access snapshot:** Normalized policy state used to decide who may retrieve a
  derivative.
- **Derivative:** Any Synor-managed chunk, embedding, search row, vector point,
  cache record, generated summary, graph node, or other output derived from a
  source.
- **Revocation:** A decision that some or all retrieval authorization must end.
  It may be caused by deletion, ACL narrowing, permission expiry, scope
  movement, retention expiry, or an operator action.
- **Suppression:** Immediate fail-closed prevention at the supported retrieval
  boundary while slower propagation is pending.
- **Purge:** Removal of an active derivative from a destination.
- **Verification:** Target-specific observation that the required query or
  isolation postcondition holds.
- **Receipt:** Metadata-only evidence of one lifecycle transition or target
  verification.
- **Drift:** A difference between expected governed target state and externally
  observed target state.
- **Authoritative snapshot:** A source inventory for which every required
  page/scope completed successfully.
- **Ambiguous removal:** An item is no longer visible, but the source API cannot
  prove whether it was deleted, de-permissioned, or moved.

### 6.2 Initial in-scope derivatives

- Qdrant points and collections managed by Synor;
- PostgreSQL rows in the later second-target phase;
- the reference retrieval cache, if enabled;
- control-plane lineage, suppression, and evidence records;
- artifacts owned through Synor stable component paths.

### 6.3 Initial out-of-scope systems

- model training/fine-tuning data and weight unlearning;
- arbitrary caches or exports that were not registered with Synor;
- backup erasure beyond a connector's documented capability;
- third-party query paths that bypass the Synor guard;
- universal evaluation of Google Workspace, AWS IAM, or enterprise identity
  policy;
- shared physical artifacts without a single authoritative owner or a
  partitioned contribution model.

---

## 7. Threat and failure model

The implementation must handle accidental failure, ordinary distributed-system
behavior, and malicious attempts to exploit stale authorization.

| Scenario | Required response |
|---|---|
| Source item is physically deleted. | Suppress, delete every owned derivative, verify absence, retain evidence. |
| Connector identity loses access to an item. | Suppress immediately; record `access_lost` rather than falsely claiming physical deletion; purge derivatives owned under that source scope unless policy requires isolated retention. |
| ACL narrows but some principals remain. | Suppress the stale policy, update per-derivative ACL metadata, verify the new policy on the retrieval path, then lift temporary suppression for still-authorized principals. |
| Group membership changes with unchanged item ACL. | Advance the group-graph revision and fail closed until retrieval evaluates the new revision. Avoid rewriting every chunk solely to expand group membership. |
| A permission expires. | Treat expiry as a scheduled revocation event even if content and mtime do not change. |
| Full scan fails halfway. | Mark the observation incomplete, retain previously known children, emit an alert/case, and perform no missing-item GC. |
| Source API silently omits an inaccessible subtree. | The strict connector must detect/report the gap; absence without snapshot completeness cannot authorize deletion. |
| Source rename or duplicate name. | Preserve identity through the immutable provider ID; name/path is display metadata only. |
| Target accepts deletion asynchronously. | Record acknowledgement, wait for its consistency fence, then verify through the supported read/query path. |
| Target client returns success but item remains. | Verification raises; Synor does not final-commit deletion tracking. |
| Process crashes at any apply stage. | Durable intent/suppression survives; idempotent retry converges; no resurrection occurs. |
| Old live watcher writes after a newer delete. | A source/item generation fence rejects the stale write. |
| Connector/provider code disappears during cleanup. | Preserve a `blocked_provider_missing` obligation; do not silently prune the last cleanup handle. |
| Snapshot taken before deletion is restored later. | Retained revocation decisions are replayed before serving; the old artifact remains suppressed and is re-purged. |
| Legal hold prevents erasure. | Verify `retained_isolated`; never mark physical erasure complete. |
| Tenant/principal tries to query another tenant's chunks. | Filter before scoring/content return, using tenant isolation and current access policy. |
| Evidence store is inspected or leaked. | It contains opaque IDs/digests and safe operational metadata, not source content or raw principal details. |

The first release does not defend against a fully compromised target
administrator, a malicious connector that forges read-back, or query clients
that intentionally bypass the documented guard. Connector certification and
deployment controls must state this boundary.

---

## 8. Non-negotiable invariants

1. Source identity is:

   ```text
   (connector_instance_id, source_scope_id, immutable_source_item_id)
   ```

   Paths, names, URLs, and display labels are attributes.

2. A source item and every retrievable derivative carry the same source
   identity or an irreversible digest that can be mapped through the protected
   lineage store.
3. Every derivative records content revision, access-policy revision,
   group-graph revision, tenant/scope, owner component path, and retention
   state.
4. ACL-only, inherited-ACL, group, tenant, expiry, and legal-state changes
   invalidate memoized governed processing even if content is unchanged.
5. A source snapshot can remove missing children only after all required
   pages/scopes complete and its checkpoint is durably committed.
6. `scan_incomplete` and `ambiguous_removal` are explicit states, not empty
   result sets.
7. A revocation suppresses serving before slow physical cleanup begins.
8. Authorization is enforced before similarity scoring or content return for
   certified query paths. Post-retrieval filtering alone is not the reference
   design.
9. Target acknowledgement and target verification are separate states.
10. A verified sink raises when the postcondition is absent, unknown, timed
    out, or unsupported. Normal return means its documented postcondition
    holds.
11. Strict mode never silently downgrades to an unverified connector.
12. Target actions and verification are idempotent across process crashes.
13. One physical target key has one authoritative Synor owner. A derivative
    combining multiple sources must have an aggregate owner or independent
    source-owned target contributions.
14. Revocation evidence outlives deletion of normal target tracking.
15. Receipts contain no source bytes, chunks, vectors, raw secrets, or raw
    principal display values.
16. A retained legal-hold artifact cannot be served by the live retrieval
    path.
17. Restoring old target or engine state cannot automatically lift a newer
    suppression decision.
18. Compatibility mode remains behaviorally compatible; strict guarantees are
    explicit and opt-in until GA migration policy changes.

---

## 9. Chosen architecture

Use a hybrid additive architecture with five planes:

```text
                         ┌──────────────────────────┐
 source snapshot/feed ─►│ 1. Observation plane     │
                         │ identity, revision, ACL,  │
                         │ cursor, completeness      │
                         └────────────┬─────────────┘
                                      │ typed observation
                                      ▼
                         ┌──────────────────────────┐
                         │ 2. Decision/control      │
                         │ policy + revocation case │
                         │ + durable suppression    │
                         └──────┬───────────┬───────┘
                                │           │
                   fail-closed  │           │ desired state
                                ▼           ▼
 query/auth context ─► ┌──────────────┐   ┌──────────────────────────┐
                       │ 3. Serving   │   │ 4. Materialization       │
                       │ guard/filter │   │ Synor components/targets │
                       └──────┬───────┘   └────────────┬─────────────┘
                              │ allowed results         │ apply
                              ▼                         ▼
                         application        ┌─────────────────────────┐
                                            │ 5. Assurance plane      │
                                            │ ack/fence/read-back,     │
                                            │ receipts, drift scans    │
                                            └─────────────────────────┘
```

### 9.1 Observation plane

Normalizes connector-specific facts without pretending the source knows more
than it does. It produces immutable IDs, revisions, access snapshots, typed
lifecycle events, cursor/checkpoint data, and an explicit completeness result.

Use both:

- incremental feeds for low detection latency; and
- periodic authoritative snapshots for completeness and repair.

### 9.2 Decision/control plane

Converts observations into policy decisions:

- `destroy`
- `restrict`
- `preserve_on_hold`
- `investigate_ambiguous`

It creates a durable revocation case and serving-suppression record before
target cleanup. Policy is application-provided; Synor supplies safe mechanics
and never makes legal decisions.

### 9.3 Serving plane

Enforces current tenant and access state on every supported retrieval. It is
the fast security boundary while target deletion is pending.

Synor is currently an indexing framework, not a query server. Therefore the
product claim is conditional on one of these certified integrations:

- a Synor-provided guarded query adapter;
- target-native pre-query filters generated by Synor; or
- a customer retrieval service that passes the conformance suite.

Direct, unguarded use of a vector client is outside the guarantee.

### 9.4 Materialization plane

Keeps Synor's existing stable component paths and declarative target-state
ownership. A source-derived component either:

- re-declares the same derivative with a new access policy;
- declares no derivative / disappears, triggering deletion; or
- declares an isolated retention target rather than a serving target.

Do not create a second reconciliation engine.

### 9.5 Assurance plane

Makes connector success meaningful:

1. apply the idempotent action;
2. obtain an operation acknowledgement if available;
3. wait for the destination-specific consistency fence;
4. read/query the postcondition;
5. raise if it is not established;
6. record a content-free receipt;
7. periodically scan for external drift and orphans.

The first implementation performs apply-and-verify inside a Python wrapper
around the existing sink callback. That lets the existing engine retain
tracking when verification raises without changing the four-field
`TargetReconcileOutput`.

---

## 10. Proposed data contracts

These are logical contracts. Keep them under `synor._internal` during the first
two phases. Exact public names are accepted only after the vertical slice.

### 10.1 Source identity

```python
@dataclass(frozen=True, slots=True)
class SourceIdentity:
    connector_instance_id: str
    source_scope_id: str
    item_id: str

    def canonical_bytes(self) -> bytes: ...
    def evidence_digest(self) -> str: ...
    def component_key(self) -> StableKey: ...
```

Requirements:

- fields use opaque stable IDs, not display values;
- canonical encoding is length-delimited or structured, never ambiguous string
  concatenation;
- `component_key()` remains stable across rename or move within the same source
  scope;
- audit output uses `evidence_digest()`;
- changing connector configuration in a way that changes its security boundary
  creates a new `connector_instance_id`.

### 10.2 Typed lifecycle event

```python
class SourceEventKind(str, Enum):
    PRESENT = "present"
    CONTENT_CHANGED = "content_changed"
    ACL_CHANGED = "acl_changed"
    GROUP_GRAPH_CHANGED = "group_graph_changed"
    PERMISSION_EXPIRED = "permission_expired"
    ACCESS_LOST = "access_lost"
    SOURCE_DELETED = "source_deleted"
    MOVED_SCOPE = "moved_scope"
    RETENTION_EXPIRED = "retention_expired"
    AMBIGUOUS_REMOVAL = "ambiguous_removal"
    SCAN_INCOMPLETE = "scan_incomplete"
```

Do not use one `deleted: bool`. A Drive tombstone, for example, may mean
physical deletion, loss of visibility, or a corpus move.

### 10.3 Access snapshot

```python
@dataclass(frozen=True, slots=True)
class AccessSnapshot:
    tenant_id: str
    policy_id: str
    policy_revision: str
    policy_digest: str
    group_graph_revision: str
    inherited_from: tuple[str, ...]
    valid_until: datetime | None
    retention_class: str | None
    legal_state: str | None
```

The operational policy resolver may separately retain opaque subject/group
IDs, grants, and denies. The receipt contains only a digest, counts, policy ID,
and revision unless an explicitly configured evidence policy permits more.

Do not expand every group member onto every chunk. Keep group membership as a
separate versioned graph and evaluate both the item policy revision and the
group-graph revision.

### 10.4 Governed source item

```python
@dataclass(frozen=True, slots=True)
class GovernedSourceItem(Generic[T]):
    identity: SourceIdentity
    resource: T | None
    source_revision: str
    content_fingerprint: bytes | None
    access: AccessSnapshot | None
    event: SourceEventKind
    observation_id: str

    def __synor_memo_key__(self) -> object: ...
    async def __synor_memo_state__(self, previous: object) -> MemoStateOutcome: ...
```

Its memo state must include at least:

```text
source_revision
content_fingerprint
policy_revision
policy_digest
group_graph_revision
valid_until
legal_state
event
```

This wrapper avoids changing the memo-state tuple of every existing
`FileLike`. Strict source APIs yield the wrapper; compatibility APIs keep their
old behavior.

### 10.5 Snapshot completion

```python
@dataclass(frozen=True, slots=True)
class SnapshotResult:
    connector_instance_id: str
    source_scope_id: str
    epoch: str
    cursor_before: str | None
    cursor_after: str | None
    status: Literal["complete", "partial", "failed"]
    item_count: int
    inaccessible_scope_digests: tuple[str, ...]
```

Only `complete` authorizes missing-item cleanup and cursor advancement.
Checkpoint write occurs after downstream reconciliation is ready, not merely
after the remote listing call returns.

### 10.6 Derivative reference

```python
@dataclass(frozen=True, slots=True)
class DerivativeRef:
    source_digest: str
    target_provider_id: str
    target_instance_digest: str
    target_locator_digest: str
    owner_component_path: str
    derivative_kind: str
    content_revision: str
    policy_revision: str
    group_graph_revision: str
```

The protected operational lineage store may hold the reversible target locator
needed for deletion. General audit and receipts use its digest.

### 10.7 Revocation case

```python
class RevocationStage(str, Enum):
    OBSERVED = "observed"
    SUPPRESSED = "suppressed_from_serving"
    PLANNED = "deletion_planned"
    DISPATCHED = "dispatched"
    ACKNOWLEDGED = "acknowledged"
    FENCE_REACHED = "consistency_fence_reached"
    VERIFIED = "absence_verified"
    RETAINED_ISOLATED = "retained_isolated"
    CLOSED = "closed"
    FAILED = "failed"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class RevocationCase:
    case_id: str
    observation_id: str
    source_digest: str
    source_revision: str
    tenant_digest: str
    policy_id: str
    policy_revision: str
    policy_digest: str
    group_graph_revision: str
    legal_state: str | None
    suppression_generation: int
    reason: SourceEventKind
    policy_decision: Literal[
        "destroy", "restrict", "preserve_on_hold", "investigate_ambiguous"
    ]
    stage: RevocationStage
    observed_at: datetime
    suppress_by: datetime
    verify_by: datetime
    expected_targets: tuple[str, ...]  # sorted deterministic obligation IDs
    version: int
```

`case_id` must be deterministic for the same access-sensitive observation,
source identity/revision, reason, and policy decision so retries do not create
duplicate obligations while later ACL-only observations remain distinct.
Governance revisions are copied into the immutable event stream so a newer
authorization generation cannot erase the policy context of an older case.
`legal_state` is copied from the governed access snapshot, so callers cannot
self-assert a hold decision while ignoring the observation. The runtime derives
the legal-hold branch from this persisted state. `suppression_generation` is
also persisted in the case and copied into every effect descriptor. Both the
runtime and a strict target's provider-native conditional mutation must reject
an older destructive generation after a newer verified authorization.

### 10.8 Target capabilities

```python
@dataclass(frozen=True, slots=True)
class TargetRevocationCapabilities:
    atomic_serving_suppression: bool
    exact_id_delete: bool
    source_id_bulk_delete: bool
    query_time_acl_filter: bool
    tenant_isolation: bool
    synchronous_acknowledgement: bool
    consistency_fence: bool
    negative_read_verification: bool
    external_enumeration: bool
    legal_hold_isolation: bool
    physical_erasure_attestation: bool
    capability_version: str = "1"
```

Capabilities are factual, versioned, and connector-specific. Strict policy
validates them before apply. Unsupported requirements block the run rather than
downgrading silently.

### 10.9 Verification receipt

```python
@dataclass(frozen=True, slots=True)
class RevocationReceipt:
    schema_version: int
    receipt_id: str
    case_id: str
    obligation_id: str
    attempt: int
    source_digest: str
    target_provider_id: str
    target_instance_digest: str
    target_locator_digest: str
    operation_kind: str
    reason: str
    policy_decision: str
    stage: str
    assurance_level: str
    request_fingerprint: str
    operation_id: str | None
    affected_count: int | None
    capability_digest: str
    consistency_contract: str
    verifier_kind: str
    observed_outcome: str
    attempted_at: datetime
    verified_at: datetime | None
    safe_error_code: str | None
    previous_receipt_digest: str | None
```

Receipts are append-only events. `obligation_id` is recomputed from case,
provider, target-instance digest, target-locator digest, operation kind, and a
proof-contract digest that binds verifier kind, consistency contract, and the
exact factual capability profile. A restart with changed proof semantics
therefore cannot silently resume the durable action. `receipt_id` is recomputed
from obligation, stage, outcome, and attempt. A durable count/tip receipt head
anchors the latest local chain so accidental tail loss is detectable. The
immutable case-event stream is the source of truth for the mutable case
summary. A case can enter `closed` only when the latest receipt for every
expected obligation is `query_verified`, `erasure_attested`, or
`retained_isolated`.

### 10.10 Evidence privacy classification

| Data | Operational lineage store | General receipt/audit |
|---|---|---|
| Source text/chunk/vector | Never | Never |
| Credentials/tokens | Never | Never |
| Raw email/display name | Avoid; only if a policy adapter absolutely needs it | Never |
| Opaque principal/group ID | Protected, optional | Digest/count only |
| Reversible target locator | Protected | Digest only |
| Source/provider ID | Protected | Digest only |
| Tenant ID | Protected opaque value | Digest or approved stable alias |
| Policy revision/digest | Yes | Yes |
| Operation ID and timestamps | Yes | Yes |
| Safe error class/code | Yes | Yes |
| Exception message/remote response body | Never by default | Never |

Use the existing audit redaction helpers, then add dedicated tests that scan
serialized receipts for source bytes, vector values, emails, secrets, and raw
exception text.

---

## 11. State machines and ordering

### 11.1 Revocation lifecycle

```text
observed
  → suppressed_from_serving
  → deletion_planned
  → dispatched
  → acknowledged
  → consistency_fence_reached
  → absence_verified
  → closed
```

Permitted branches:

```text
observed → retained_isolated → closed
observed → blocked(policy/provider/capability)
any nonterminal stage → failed(retryable)
ambiguous_removal → suppressed → investigate_ambiguous
```

Rules:

- suppression is monotonic for the case until a newer authorized source
  revision is verified;
- `acknowledged` cannot skip directly to `closed`;
- `closed` requires every expected target obligation to be
  `query_verified`, `erasure_attested`, or `retained_isolated`;
- timeouts produce `failed` or `blocked`, never success;
- retry uses the same case and action IDs;
- a newer source generation supersedes older updates, but it does not erase the
  evidence history.

### 11.2 Source snapshot lifecycle

```text
started
  → pages/scopes observed
  → remote enumeration complete
  → downstream items ready
  → missing-item diff authorized
  → checkpoint committed
```

Any page error, permission gap, cancellation, cursor invalidation, or unknown
scope changes the result to `partial`/`failed`. The old checkpoint remains
authoritative and missing-item cleanup is not authorized.

### 11.3 Target obligation lifecycle

Each derivative/destination has its own obligation:

```text
pending → applied → acknowledged → fenced → verified
    └──────→ failed_retryable
    └──────→ blocked_unsupported
    └──────→ retained_isolated
```

The case closes only when all obligations are terminal and acceptable.

### 11.4 Required operation ordering

For a destructive event:

1. persist the observation and revocation intent;
2. install serving suppression;
3. wait until the supported query boundary confirms suppression;
4. plan the materialization change;
5. dispatch target action;
6. record target acknowledgement/operation ID;
7. cross the target consistency fence;
8. perform negative read/query verification;
9. let the Synor sink return normally;
10. allow Synor final tracking commit;
11. record case closure only after all targets finish.

If steps 5–8 raise, the current engine's pending multi-state tracking remains
retryable. If the process dies after target verification but before final
commit, the idempotent delete and verifier run again.

### 11.5 Reauthorization

A later source observation may authorize content again. It must not simply
delete the suppression record. It creates a new source generation, rebuilds or
updates derivatives, verifies their new ACL, and only then supersedes the old
suppression. This prevents a stale restore or delayed event from resurrecting
content.

---

## 12. End-to-end behavior by event

### 12.1 Authoritative full-scan deletion

1. Start a source snapshot with an epoch and prior cursor.
2. Enumerate every required page and scope.
3. Mount each item using `SourceIdentity.component_key()`.
4. Wait for mounted work to become ready.
5. Mark the snapshot complete.
6. Allow the parent component to finish, causing missing child components to
   reconcile their targets to non-existence.
7. Suppression and revocation cases for missing identities must already exist
   before external delete actions.
8. Verified sinks delete/read back.
9. Commit the new source checkpoint only after downstream convergence.

If the enumeration is partial, raise before authoritative parent completion.
Previously known missing candidates remain present and suppressed only when
there is an independent revocation signal.

### 12.2 Google Drive tombstone or lost visibility

1. Replay both relevant user and shared-drive logs.
2. Receive a tombstone containing an item ID.
3. Record `ambiguous_removal` unless another authoritative fact proves a more
   specific reason.
4. Suppress derivatives for that source identity immediately.
5. Use `includeCorpusRemovals` and stored corpus/parent state to resolve a
   scope move where possible.
6. Under the connector's source-scope contract, purge the derivative if the
   pipeline identity is no longer authorized to retain/serve it; record
   `access_lost`, not physical deletion.
7. Keep the ambiguity visible to operators and evidence consumers.

### 12.3 ACL narrowing

1. Fetch and canonicalize the new direct/inherited policy.
2. Advance `policy_revision` and `policy_digest`.
3. Create temporary suppression for the stale revision.
4. Re-run memoized processing even if file content is unchanged.
5. Update every derivative's ACL metadata and target-native filter fields.
6. Verify through the guarded query path using allowed and newly denied
   principals.
7. Lift temporary suppression only for principals authorized by the new
   revision.
8. If nobody remains authorized, continue to purge.

### 12.4 Parent ACL or group membership change

For parent ACLs, invalidate descendants using a stored parent/child dependency
graph because Drive does not emit one child event per inherited change.

For group membership, advance a separately stored group-graph revision. Query
authorization evaluates the current group graph. Do not require rewriting
millions of chunks merely to expand a group into every item's ACL.

### 12.5 Permission expiry

Schedule a local event from `valid_until`; also reconcile when the source
reports the change. The local timer provides prompt suppression, while source
reconciliation remains authoritative for the next policy state.

### 12.6 Legal hold or retention exception

1. Policy chooses `preserve_on_hold`.
2. Suppress the serving derivative.
3. Move/copy only the required representation into an explicitly isolated
   retention target if necessary.
4. Verify the live query path cannot retrieve it.
5. Record `retained_isolated`, retention class, and policy decision digest.
6. Do not claim erasure.

### 12.7 Target verification timeout

Keep serving suppression active. Raise from the verified sink. Preserve Synor
tracking and the open case. Retry with bounded exponential backoff and jitter.
Alert when `verify_by` is exceeded. Never unblock retrieval because cleanup is
slow.

### 12.8 Provider missing

If a previously tracked target provider is no longer registered, retain an
operator-visible `blocked_provider_missing` obligation. Do not prune the final
cleanup locator. A provider alias/migration can resume cleanup.

### 12.9 Shared or aggregated derivative

Because current target ownership is singular, do not let independent source
components fight over one physical key. Choose one:

- deterministic point/row per source contribution;
- one aggregate component that owns the combined artifact and retains the full
  source set; or
- defer the use case until an explicit provenance-set ownership model exists.

The first Qdrant slice uses deterministic per-source, per-chunk point IDs.

### 12.10 Restore after deletion

Keep revocation receipts/suppression outside the ordinary target snapshot
retention boundary or replicate them to a monotonic ledger. On restore:

1. load current revocation decisions before enabling queries;
2. reject/suppress artifacts with a newer revoked generation;
3. replay outstanding purges idempotently;
4. run target drift verification;
5. enable serving only after the restore gate passes.

---

## 13. Alternatives considered

### 13.1 Connector-only delete conventions

**Rejected as the final design.** Asking connector authors to “remember to
delete” does not provide source completeness, ACL memo invalidation,
suppression, capability checks, target verification, retained evidence, or
restore safety.

Connector-side apply-and-verify is still the safest first implementation seam,
but it is governed by shared contracts and conformance tests.

### 13.2 Query-time ACL filtering only

**Rejected.** It reduces immediate exposure but leaves unauthorized content in
targets indefinitely, creates retention/compliance risk, and provides no
convergence or deletion evidence. Query-time enforcement is one plane, not the
whole feature.

### 13.3 Physical deletion only

**Rejected.** Eventually consistent targets create a window in which deleted
content remains retrievable. Some legal states require restriction rather than
destruction. Serving suppression must precede slow cleanup.

### 13.4 Rebuild the entire vector index after every permission change

**Rejected.** It is expensive, slow, difficult to prove, and creates large
exposure windows. Synor already has granular stable ownership and incremental
reconciliation.

### 13.5 Use source path/name as identity

**Rejected.** Renames become false delete/create cycles, duplicate names
collide, and moves lose lineage. Provider-native immutable IDs are required.

### 13.6 Add a fifth field to `TargetReconcileOutput` immediately

**Rejected.** The PyO3 bridge unpacks the output as exactly four fields.
Changing it would touch every connector and both language layers before the
receipt contract is proven. Use a verified wrapper around the existing sink
first.

### 13.7 Put all evidence in existing artifact provenance

**Rejected.** Current provenance is a post-success snapshot of still-tracked
targets. Deleted target tracking has already disappeared. Revocation evidence
has a different lifecycle and must outlive the artifact.

### 13.8 Rewrite LMDB reconciliation as a Python control plane

**Rejected.** It would duplicate the most mature part of Synor, weaken atomic
ownership behavior, and conflict with ADR-0001 and ADR-0002. The native engine
remains authoritative.

### 13.9 Chosen approach

Use:

- the existing engine for ownership, deletion, crash uncertainty, and retry;
- a typed source/access envelope for trustworthy observation;
- a durable control-plane ledger and suppression registry;
- apply-and-verify inside an additive verified sink wrapper;
- connector-specific query guards, consistency fences, and verifiers;
- the separately reviewed Phase 6 native effect/tombstone/generation seam for
  gaps that could not be solved safely around the engine.

---

## 14. Phase-by-phase implementation

Each numbered phase should be a milestone, not one enormous pull request. The
recommended PR sequence is in section 23.

### Phase 0 — Freeze the contract and add failing safety tests

#### Objective

Turn the product promise into executable security invariants before adding
public API or persistent schema.

#### Implementation status — internal contract milestone complete (2026-07-29)

**Milestone scope:** internal contract and failure-harness work, plus the
narrow Qdrant exception-safety correction. No public revocation API or native
state-schema change was made.

- [x] Accept ADR-0003 terminology, scope, non-claims, assurance levels, and
  five-plane architecture.
- [x] Preserve the compatibility behavior of direct `App.update()` and
  document that the complete guarantee requires a future opt-in controlled
  path.
- [x] Build independently controllable synthetic apply, false-success,
  eventual-consistency, read-back-failure, and missing-provider behavior.
- [x] Add invariant tests for false success, ACL-only memo invalidation,
  complete/partial snapshots, pending suppression, evidence redaction, and
  stable source identity.
- [x] Reproduce and fix Qdrant collection deletion so only confirmed
  not-found or a literal successful result is accepted.
- [ ] **Phase 6 certification gate — not a Phase 0–2 blocker:** the existing core
  suite proves compatibility orphan-cleanup retry, Phase 2 proves strict
  failure-before-final-commit for root target reconciliation, and Phase 5
  proves controlled post-commit finalization/recovery. Phase 6 adds native
  tracking-owner cleanup plus a fresh-process provider-missing/blocker/recovery
  lifecycle. Destructive child-component process-kill/power-loss coverage
  remains open.
- [ ] **Phase 6 certification gate — not a Phase 0–2 blocker:** Phase 2 proves
  stale-generation fencing in the control plane and synthetic provider, and
  Phase 6 adds native local live-incarnation generations and cancellation
  checks. The deterministic `LiveMapSubscriber`/live delete-reinsert race test
  remains open.
- [x] Record a compatibility-path baseline in
  `revocation-phase2-validation.md`.

**Evidence:** `ADR-0003-provable-index-revocation.md`,
`python/tests/revocation/test_contract.py`,
`python/tests/revocation/test_failure_windows.py`,
`python/tests/revocation/test_evidence_redaction.py`, and
`python/tests/connectors/test_qdrant_target.py`.

#### Files

Create:

- `docs/architecture/ADR-0003-provable-index-revocation.md`
- `python/tests/revocation/__init__.py`
- `python/tests/revocation/test_contract.py`
- `python/tests/revocation/test_failure_windows.py`
- `python/tests/revocation/test_evidence_redaction.py`

Extend narrowly:

- `python/tests/common/target_states.py`
- `python/tests/core/test_flat_target_states.py`
- `python/tests/core/test_component_target_states.py`
- `python/tests/core/test_live_component.py`
- `python/tests/connectors/test_qdrant_target.py`

#### Steps

1. Accept terminology, scope, non-claims, assurance levels, and the five-plane
   architecture in ADR-0003.
2. Document that compatibility `App.update()` remains unchanged and the full
   guarantee requires an opt-in controlled/strict path.
3. Add a synthetic target that can independently:
   - apply successfully;
   - falsely report success while leaving data present;
   - become consistent after N reads;
   - fail read-back;
   - lose its provider registration.
4. Add red tests for:
   - verification observes data still present, so deletion does not become
     final;
   - ACL-only change with unchanged content causes governed processing;
   - complete scan deletes a missing component;
   - partial scan retains it;
   - serving is suppressed while target verification is pending;
   - evidence serialization contains no planted source phrase, vector values,
     email, token, or raw remote error;
   - deterministic source ID survives rename and duplicate display name.
5. Add a regression test proving Qdrant collection deletion must not swallow a
   non-not-found exception. This regression failed against the pre-fix
   implementation and now passes after the Phase 0 correction.
6. Add a test for the existing root orphan-delete distinction: compatibility
   may finish with retryable cleanup, while proposed strict mode must surface a
   degraded/failed result.
7. Add a live race test in which an older source generation tries to update
   after a newer deletion. Keep it marked as an expected failure until the
   native fencing phase if necessary.
8. Record baseline performance for a normal non-governed pipeline so later
   strict-mode work can demonstrate no compatibility regression.

#### Exit criteria

- ADR-0003 is accepted.
- Every Phase 0–2 in-scope invariant has a named test. The public strict
  disappeared-child E2E and native live-incarnation race remain explicitly
  deferred above to Phases 5–6.
- The pre-fix false-success and Qdrant exception behavior is reproduced, and
  the completed regressions now pass.
- No production API or state schema has changed.

#### Rollback

Delete only the new tests/ADR if the design is rejected. There is no runtime
state to migrate.

---

### Phase 1 — Internal governance model and durable local ledger

#### Objective

Create stable internal types, legal state transitions, deterministic IDs,
privacy-safe receipts, and a recoverable single-process, single-event-loop
ledger without exposing an unproven public API.

#### Implementation status — complete (2026-07-29)

**Milestone scope:** internal versioned schemas and a recoverable local,
single-process, single-event-loop control-plane adapter. This is not a
cross-event-loop or multi-process transaction protocol and does not claim
sudden-power-loss durability.

- [x] Implement collision-resistant `SourceIdentity` canonical encoding.
- [x] Implement semantic ACL normalization and canonical hashing.
- [x] Implement access-sensitive `GovernedSourceItem` memo state.
- [x] Enforce typed legal case transitions without partial mutation.
- [x] Implement deterministic observation, case, action, event, and receipt
  identifiers bound to governance and target identity.
- [x] Define the internal `RevocationLedger` append/read/list/repair contract.
- [x] Implement event-first `StateStoreRevocationLedger` writes, shared
  single-event-loop serialization, idempotent retries, exact summary
  validation, projection repair, and fail-closed cross-loop rejection.
- [x] Implement the versioned `revocation/v1/` key layout.
- [x] Implement receipt hash links and a durable count/tip head that detects
  missing, reordered, renamed, divergent, orphaned, or tail-lost evidence.
- [x] Bind every receipt to its exact target operation and policy decision,
  require the canonical controlled error code for each failed read-back, and
  require terminal receipt evidence whenever a case is read or repaired in
  `absence_verified`, `retained_isolated`, or `closed`.
- [x] Implement monotonic suppression generations bound to source, tenant,
  policy identity/revision, and group-graph revision, plus a trusted
  newer-authorization restoration callback.
- [x] Verify plaintext and encrypted `StateStore` adapters.
- [x] Reject unknown major schema versions and preserve supported extensions.
- [x] Cover corruption, interrupted writes, duplicate events, lost write
  responses, and concurrent coroutine writers.
- [x] Normalize model, ledger, receipt-head, suppression, and serving-fence
  corruption without retaining raw decoder/parser exceptions in `__cause__`,
  `__context__`, or formatted tracebacks.

**Evidence:** `python/synor/_internal/revocation_model.py`,
`revocation_ledger.py`, `revocation_policy.py`, `state_store_lock.py`,
`suppression.py`, and the corresponding tests under
`python/tests/revocation/`.

#### Implemented files

Create:

- `python/synor/_internal/revocation_model.py`
- `python/synor/_internal/revocation_ledger.py`
- `python/synor/_internal/revocation_policy.py`
- `python/synor/_internal/suppression.py`
- `python/tests/revocation/test_model.py`
- `python/tests/revocation/test_ledger.py`
- `python/tests/revocation/test_suppression.py`

Reuse:

- `python/synor/state.py`
- `python/synor/audit.py`

Do not update `python/synor/__init__.py` yet.

#### Steps

1. Implement `SourceIdentity` canonical encoding with collision tests. Use
   explicit versioned serialization, not `repr()` or ambiguous delimiter
   concatenation.
2. Implement canonical ACL hashing:
   - normalize grant/deny type, opaque subject ID, role, inheritance source,
     expiry, tenant, and policy version;
   - sort semantically unordered entries;
   - preserve grant versus deny and direct versus inherited semantics;
   - never use display names or email casing as identity.
3. Implement `GovernedSourceItem` memo key/state. Include access and group
   revisions even when the underlying resource content is unchanged.
4. Implement legal state transition validation. Illegal transitions raise a
   typed internal error and do not modify state.
5. Define deterministic `case_id`, `observation_id`, `action_id`, and
   `receipt_id` algorithms with schema-version prefixes. Bind case identity to
   the full governed observation and bind action identity to the target
   instance, locator, verifier, consistency contract, and factual capability
   profile.
6. Define a specialized internal `RevocationLedger` protocol. It should expose
   append/read/list/rebuild semantics rather than pretending the existing
   `StateStore` has cross-key transactions.
7. Implement `StateStoreRevocationLedger` for the local single-process,
   single-event-loop case:
   - serialize writes through the shared event-loop-bound writer lock for one
     `StateStore` facade;
   - write immutable event/receipt first;
   - update the mutable case summary second;
   - treat the event stream as source of truth;
   - make event keys deterministic so retrying `put()` is idempotent;
   - provide `repair()` to rebuild summaries after a crash;
   - state clearly that this adapter is not a multi-process compare-and-swap
     database.
8. Suggested versioned key layout:

   ```text
   revocation/v1/events/<case-id>/<sequence>-<event-id>.json
   revocation/v1/cases/<case-id>.json
   revocation/v1/receipts/<case-id>/<receipt-id>.json
   revocation/v1/receipt_heads/<case-id>.json
   revocation/v1/suppression/<source-digest>.json
   revocation/v1/serving_fences/<source-digest>.json
   revocation/v1/checkpoints/<connector-digest>/<scope-digest>.json
   revocation/v1/lineage/<source-digest>/<derivative-id>.json
   ```

9. Add a hash chain (`previous_receipt_digest`) plus a durable count/tip head to
   make missing/reordered receipts, including accidental tail loss, detectable.
   Do not describe this as a digital signature.
10. Implement suppression as monotonic generations:
    - a record is keyed by source digest;
    - a newer generation supersedes older state;
    - deleting the record is not how access is restored;
    - restoration requires a verified newer authorization event.
11. Reuse `EncryptedStateStore` when configured. Test both plaintext and
    encrypted adapters.
12. Add schema readers that reject unknown major versions and preserve unknown
    optional fields when practical.
13. Add corruption, interrupted-summary-write, duplicate-event, and concurrent
    coroutine tests.

#### Exit criteria

- All Phase 0 model/evidence tests pass.
- Ledger repair produces the same summaries from immutable events.
- Evidence redaction tests pass with intentionally sensitive fixtures.
- No public import or engine behavior changes.

#### Rollback

Because no public API consumes the ledger yet, stop writing new records and
leave `revocation/v1/` untouched for inspection. Never delete suppression
records as part of rollback.

---

### Phase 2 — Synthetic strict vertical slice and retrieval guard ✅

#### Objective

Prove the entire lifecycle without a remote service: observation, suppression,
Synor deletion, apply/read-back, retry, receipt, and guarded retrieval.

#### ✅ Implementation status — complete for the internal synthetic milestone (2026-07-29)

- [x] ✅ Revalidate the Phase 2 baseline before native integration on
  2026-07-30: the revocation suite reported 155 passed and 2 operator-gated
  skips; the Phase 2-focused selection reported 101 passed; and the
  service-free flagship example completed from the repository environment
  with a closed case, two receipts, partial-snapshot blocking, and the
  unaffected tenant still visible.
- [x] ✅ Re-run the full revocation package after rebuilding the Phase 6 native
  extension on 2026-07-30: 172 tests passed on the recorded rebuilt snapshot.
  That run preceded a final minor compatibility-path adjustment; its
  post-adjustment source is covered by the final Python repository result of
  1,412 passed and 241 skipped. Neither result broadens this Phase 2
  milestone.
- [x] ✅ Implement compatibility, strict query-verified, and deterministic
  test-only policy presets.
- [x] ✅ Validate provider registration and factual target capabilities before
  materialization and again on resume/dispatch.
- [x] ✅ Require an explicit safe action descriptor with operation, action ID,
  source digest, source generation, and target-locator digest.
- [x] ✅ Keep arbitrary action-field inference out of the generic engine.
- [x] ✅ Implement tenant/principal-aware fail-closed retrieval with current
  policy, group-graph revision, monotonic suppression snapshots, bounded
  concurrent-change re-evaluation, and post-scoring revalidation.
- [x] ✅ Re-read policy as part of post-scoring authorization, so a policy update
  that lands while an asynchronous scorer is running cannot release a result.
- [x] ✅ Implement the guarded in-memory reference retriever and prove suppressed
  or stale-policy candidates are never scored or returned.
- [x] ✅ Install an in-process serving fence synchronously before the first await
  and a full-metadata durable serving-fence record before observation
  persistence. Guard reads consult both, so a crash or fresh `StateStore`
  facade cannot reopen the previously authorized derivative.
- [x] ✅ Retain the durable fence across observation/suppression-write failures
  and equal-generation conflicts. Release it only through the exact matching
  suppressed record or a strictly newer durable generation; equal-generation
  authorization and replay of an older terminal case cannot clear it.
- [x] ✅ Persist serving suppression before returning a policy, provider,
  capability, or incomplete-snapshot block for a typed revocation; non-events
  remain unsuppressed.
- [x] ✅ Run two real Synor-owned synthetic derivatives through ingest,
  ACL-only revocation, immediate query denial, false-success verification
  failure, retained retry tracking, eventual absence, receipt creation, final
  commit, and unaffected-neighbor retrieval.
- [x] ✅ Inject controlled process interruptions at all nine lifecycle
  boundaries using a filesystem-backed control-plane store.
- [x] ✅ Restart from all nine boundaries and prove stable IDs, convergent state,
  retry-safe evidence, and idempotent provider-native effects.
- [x] ✅ Cover partial and wrong-scope snapshots, governed legal-hold isolation,
  provider/capability loss and recovery, stale generations, and trusted
  newer-authorization callbacks. Even when policy/group labels are reused, the
  old derivative generation is denied before scoring, only the replacement
  generation is retrievable, and the old destructive action remains fenced.
- [x] ✅ Reject descriptor subclasses and normalize apply, descriptor, verifier,
  recorder, and corruption failures so untrusted payloads cannot escape
  through exception messages or chained tracebacks.
- [x] ✅ Prove retry blocker changes are durably represented and that an invalid
  retry of an already closed old generation cannot hide a newer verified
  authorization.
- [x] ✅ Record compatibility baseline and strict in-process framework latency
  separately.

**✅ Exit gate:** satisfied for Phases 0–2. The complete synthetic lifecycle
passes, serving denial precedes cleanup, false success cannot discard Synor
tracking, and no Rust/PyO3 or public API change was required. See
`revocation-phase2-validation.md` for commands, measurements, recovery limits,
exact non-claims, and the repository-wide pre-existing Ruff baseline.

**Deliberate boundary:** ACL narrowing currently converges by verified purge
and later replacement. In-place `RESTRICT` is blocked because Phase 2 has no
principal-aware positive read-back contract. Crash tests use controlled
exceptions and ordinary process reconstruction; they are not `SIGKILL`,
power-loss, cross-event-loop, or multi-process durability certification. Real
Google Drive and Qdrant support remained Phase 3/4 work at the Phase 2
validation boundary. Google Drive is now implemented in Phase 3; certified
Qdrant support is now implemented in Phase 4. The reauthorization method is a
trusted verifier callback: Phase 2 validates its policy identity,
revisions, monotonic generation, replacement-candidate serving, and destructive
action fencing, but the callback does not itself query a remote source or
target. Real positive authorization/derivative evidence belongs to the
certified source/target adapters.

The emergency serving fence has two layers. The first is process-local and
closes the interval before the first durable write. The second is a
metadata-only `revocation/v1/serving_fences/` record written before the
observation event and consulted by guarded reads after process/facade
reconstruction. If recovery has not run, an unresolved durable fence denies
serving rather than reopening an older authorization. Phase 5 now adds the
public strict runtime that repairs/replays open cases before reporting startup
health, so stranded fences converge and cleanup resumes. Cross-process writers
and `SIGKILL`/power-loss durability remain outside this internal milestone.

#### ✅ Implemented files

Created:

- ✅ `python/synor/_internal/verified_sink.py`
- ✅ `python/synor/_internal/revocation_runtime.py`
- ✅ `python/synor/_internal/retrieval_guard.py`
- ✅ `python/synor/_internal/state_store_lock.py`
- ✅ `python/tests/revocation/test_vertical_slice.py`
- ✅ `python/tests/revocation/test_guard.py`
- ✅ `python/tests/revocation/test_verified_sink.py`
- ✅ `python/tests/revocation/test_corruption_redaction.py`

Extend:

- `python/tests/common/target_states.py`
- `python/synor/execution.py` only if the runtime integration can remain
  additive and internal
- `python/tests/test_phase3_execution.py`

The Phase 2 implementation remained additive under `synor._internal` and
exercised the real engine through its existing target callback. It did not
need to modify `python/synor/execution.py`,
`python/tests/common/target_states.py`, or
`python/tests/test_phase3_execution.py` at that milestone. Phase 5
subsequently used the execution/test seams for the public controlled runtime
without changing the four-field reconcile contract or common target-state
fixtures.

#### ✅ Verified-sink design — implemented

Do not alter the four-field `TargetReconcileOutput`. Build an internal wrapper
around `TargetActionSink.from_async_fn()`:

```python
async def wrapped(context_provider, actions):
    applied = await apply(actions)
    results = await verify(actions, applied)
    validate_one_result_per_action(results, actions)
    await record(results)
    if any(not result.required_postcondition_holds for result in results):
        raise TargetVerificationError(...)
    return applied.child_handlers
```

The implemented wrapper:

- ✅ preserves the flat action order created by engine batching;
- ✅ requires either one result per action or an explicit stable `action_id`;
- ✅ never includes raw action payloads in errors/evidence;
- ✅ uses bounded polling, deadline, backoff, and jitter;
- ✅ distinguishes `unsupported`, `timeout`, `present`, `wrong_acl`, and transport
  failure;
- ✅ makes receipt write failure fail the strict sink so retry remains possible;
- ✅ returns normally only after the required postcondition holds.

#### Steps

1. ✅ Introduce internal `RevocationPolicy` presets:
   - `compatibility`: current behavior;
   - `strict_query_verified`: requires suppression, current ACL enforcement,
     and negative query verification;
   - an internal test-only policy for deterministic deadlines.
2. ✅ Implement capability validation before materialization. If preview cannot
   reliably identify a destructive connector action, block strict apply rather
   than relying on `_infer_operation()` heuristics.
3. ✅ Add a safe action-description protocol to synthetic actions, for example an
   internal `__synor_effect_descriptor__()` method. It returns only operation
   kind, stable action ID, source digest, source generation, and target locator
   digest.
4. ✅ Do not make the generic core infer security semantics from arbitrary action
   field names.
5. ✅ Implement a `SuppressionIndex` and `RetrievalGuard`:
   - requires tenant and authenticated principal context;
   - evaluates current item policy plus group-graph revision;
   - checks source suppression before returning candidates;
   - fails closed on missing/corrupt/stale policy state;
   - records safe denial/failed-closed metrics;
   - supports batch checks to avoid one state-store call per chunk.
6. ✅ Implement a guarded in-memory retriever that filters before scoring. Use it
   as the reference conformance harness.
7. ✅ Execute this synthetic scenario end to end:
   - ingest two source items;
   - retrieve both as an authorized principal;
   - revoke one ACL with unchanged content;
   - observe immediate suppression;
   - make target deletion falsely “apply” but verification report present;
   - prove Synor retains retryable tracking and the case stays open;
   - let the target become absent;
   - retry, verify, close, and confirm the other item remains retrievable.
8. ✅ Add controlled interruption injection after:
   - observation persistence;
   - suppression;
   - Synor precommit;
   - target apply;
   - acknowledgement;
   - verification;
   - engine final commit;
   - receipt append;
   - case-summary update.
9. ✅ Restart from every injection point and prove convergence without duplicate
   external effects or lost evidence.
10. ✅ Add partial snapshot, legal-hold isolation, provider-missing, and newer
    generation reauthorization scenarios.
11. ✅ Measure compatibility-mode overhead and strict-mode latency separately.

#### ✅ Exit criteria — satisfied

- ✅ The full synthetic lifecycle passes under controlled
  interruption/reconstruction at all nine Phase 2 boundaries and under
  simulated eventual consistency.
- ✅ Query denial occurs before physical cleanup.
- ✅ A false-success target cannot cause forgotten tracking.
- ✅ No Rust/PyO3 change is required.
- ✅ The internal API is sufficient to implement one real source and target.

#### Rollback

Disable internal strict runtime selection. Existing targets and direct
`App.update()` continue unchanged. Suppression stays active until an operator
uses a version-aware recovery tool; rollback must never unsuppress by deleting
state.

---

### Phase 3 — Governed Google Drive source ✅

#### Objective

Make Google Drive produce stable identity, ACL-sensitive memo state,
authoritative snapshots, incremental change observations, and explicit
ambiguity.

#### ✅ Implementation status — complete for the governed source milestone (2026-07-29)

The additive governed source is implemented while the compatibility
`GoogleDriveSource.items()` behavior remains unchanged. Repository validation
covers stable Drive-ID identity, permission normalization, authoritative and
partial snapshots, user/shared-drive change logs, durable-readiness cursor
ordering, parent/descendant ACL invalidation, permission expiry, tombstone
ambiguity, retries, cancellation, rejected-token recovery, and fail-closed
cursor preservation for malformed or exhausted transient change processing.
Shared-drive authority events and shared-drive roots missing their required
dedicated log are also handled fail-closed. Folder deletion, trashing, and
scope exit cascade to known descendants; a folder newly entering scope is
strictly enumerated before its replay cursor can advance. Policy or group-graph
semantic changes require an inventory-wide snapshot instead of silently
reusing old ACL conclusions. Same-corpus tombstone ordering, folder-versus-child
event ordering, child-before-parent entry, and retry-stable permission-expiry
identity are covered by adversarial regression tests. The documented live
acceptance probe remains credential/operator gated and is not a claim of
certification for every Workspace configuration.

#### ✅ Implemented files

Extended:

- ✅ `python/synor/connectors/google_drive/_source.py`
- ✅ `python/synor/connectors/google_drive/__init__.py`
- ✅ `python/tests/connectors/test_google_drive_source.py`
- ✅ `docs/src/content/docs/connectors/google_drive.mdx`

Created:

- ✅ `python/synor/connectors/google_drive/_governed_source.py`
- ✅ `python/synor/connectors/google_drive/_permissions.py`
- ✅ `python/synor/connectors/google_drive/_changes.py`
- ✅ `python/tests/connectors/_google_drive_fakes.py`
- ✅ `python/tests/connectors/test_google_drive_governance.py`
- ✅ `python/tests/connectors/test_google_drive_changes.py`
- ✅ `python/tests/connectors/test_google_drive_change_edge_cases.py`
- ✅ `dev/manual_google_drive_governance.py`

Completed later in Phase 5:

- ✅ `examples/provable_index_revocation/` provides the local governed
  flagship and an explicitly non-certified real Drive/Qdrant configuration
  probe. A live Drive-backed declarative source remains an external acceptance
  item.

#### Compatibility boundary

Keep current `GoogleDriveSource.items()` behavior initially. Add a clearly
named governed API that yields immutable file-ID keys and
`GovernedSourceItem[DriveFile]`. Do not silently change component identity for
existing users.

A later deprecation can move users from path identity to ID identity with an
explicit migration preview.

#### Steps

1. ✅ Split display path from identity:
   - `DriveFilePath.resolve()` already knows the Drive ID;
   - governed component keys use file ID;
   - name, parent, and reconstructed path remain display metadata.
2. ✅ Request and retain the fields needed for governance:
   - file ID, name, parents, drive/corpus identity, MIME type, size,
     modification/version data, trashed state, and permission/capability
     metadata supported by the authenticated principal.
3. ✅ Build a `DrivePermissionResolver`:
   - normalize user/group/domain/anyone grants;
   - preserve role, direct versus inherited origin, expiration, and limited
     access semantics;
   - store opaque IDs operationally;
   - emit digest/count-only evidence;
   - document which effective policies cannot be derived with the configured
     scope.
4. ✅ Reconstruct parent relationships by ID. Do not assume the current recursive
   traversal's file name is a globally unique path.
5. ✅ Compute `policy_digest` from canonical effective access input and include it
   in governed memo state.
6. ✅ Implement authoritative snapshot sessions:
   - cover every configured root and required shared drive;
   - track every page token and scope result;
   - return `partial` on any unobservable subtree/page;
   - checkpoint only after downstream readiness;
   - never convert a partial inventory into missing-item deletion.
7. ✅ Implement Drive Changes API replay:
   - persist start/new page tokens per user and shared drive;
   - request removed items and required shared-drive flags;
   - replay both user and shared-drive logs;
   - handle drive-level shared-drive authority events that have no file ID;
   - deduplicate/coalesce newer states for the same file ID independent of
     corpus polling order, preferring an accessible current state over a
     cross-corpus tombstone while preserving a later tombstone from the same
     corpus;
   - treat a missing or provider-rejected token as a required full snapshot;
   - advance cursor only after downstream event handling is durable.
8. ✅ Interpret a tombstone as `ambiguous_removal` by default. Use stored corpus
   state and `includeCorpusRemovals` to distinguish moves where possible.
9. ✅ On parent permission change, enqueue descendants for policy recomputation.
   The dependency graph is bounded, resumable, and versioned. Folder
   deletion, trashing, or scope exit deactivates its known subtree, while a
   newly in-scope folder is recursively enumerated with no partial-result
   checkpoint. An inactive ancestor remains a same-batch scope fence, and
   child-before-parent feed ordering cannot skip subtree materialization.
10. ✅ Represent permission expiry as both source metadata and a scheduled local
    suppression deadline. Retry observation identity is derived from the file,
    deadline, and policy state rather than the preparation wall clock.
11. ✅ For proven connector/corpus authority loss, emit the governed revocation
    event required for downstream suppression/purge and retain the honest
    reason `access_lost`. Keep Drive `404` as `ambiguous_removal`, because it
    cannot distinguish deletion from lost read access, and represent rejected
    cursors as `scan_incomplete` plus a required full snapshot rather than
    fabricating access loss.
12. ✅ Add fake-service tests for:
    - duplicate names and rename with stable ID;
    - pagination and failed middle page;
    - user plus shared-drive logs;
    - direct ACL change with unchanged content;
    - inherited parent ACL change;
    - permission expiry;
    - tombstone ambiguity and corpus move;
    - rejected change token/full rescan;
    - rate limit, retry, and cancellation;
    - malformed/transient replay failure without cursor advancement;
    - permission denial as explicit `access_lost` and `404` as ambiguous;
    - folder deletion/trash/scope-exit cascades;
    - folder revocation followed by a later live descendant in the same batch;
    - strict discovery of an existing subtree when a folder enters scope;
    - child-before-parent ordering during both replay and snapshot fencing;
    - bounded descendant queue resumption before final cursor publication;
    - shared-drive authority change/full-snapshot recovery;
    - shared-drive root without its required configured log;
    - same-corpus live state followed by a tombstone;
    - both cross-corpus move polling orders;
    - policy/group-graph semantic revision full-snapshot fencing;
    - stable permission-expiry identity across an uncommitted retry;
    - checkpoint ordering after downstream readiness.
13. ✅ Keep live credential tests optional, as the existing suite does, and add a
    documented manual acceptance script against a dedicated test drive.
14. ✅ Add least-privilege and domain-wide delegation documentation. Never broaden
    OAuth scope silently.

#### ✅ Validation

- ✅ Scoped Ruff formatting and lint checks pass.
- ✅ Scoped MyPy checks pass for the connector, fake service, tests, and manual
  acceptance probe.
- ✅ 60 Google Drive compatibility/governance/change tests pass; the one
  metadata-only live Workspace test is correctly skipped without configured
  credentials.
- ✅ The manual acceptance probe exposes separate `snapshot`, `changes`, and
  `expiry` workflows and passed its CLI smoke test.
- ✅ A final independent adversarial re-audit closed and rechecked all reported
  stale-serving paths; no critical/high defect remained in those paths.
- ✅ The broad Python suite, excluding the one known native
  sentence-transformer/Torch import crash, passes with 1,516 tests and 69
  skips. The unfiltered run reached 83% before that unrelated native abort.
- ✅ The full Rust workspace test suite passes.
- ✅ The documentation production build passes all 100 generated pages and
  the agent-facing artifact check.
- ℹ️ Repository-wide MyPy has one unrelated optional-example `baml_py`
  dependency error. Repository-wide Ruff retains unrelated pre-existing
  findings; every Phase 3 file is clean.
- ℹ️ Real Workspace mutation acceptance was not run because no test
  credentials were configured. The opt-in live metadata test and dedicated
  manual workflow are present for operator certification.

#### ✅ Exit criteria — satisfied

- ✅ Governed IDs survive rename and duplicates.
- ✅ ACL-only/inherited changes invalidate memoization.
- ✅ Partial scans perform no missing-item cleanup.
- ✅ Change cursor cannot advance ahead of durable downstream handling.
- ✅ Ambiguous source semantics remain visible in governed observations and
  privacy-safe evidence summaries for the Phase 4/5 case/receipt integration.
- ✅ Compatibility `items()` tests still pass.

#### Rollback

Existing `items()` remains available. Stop the governed watcher and keep its
last cursor/ledger. Keep suppression records active. Do not reuse a governed
LMDB app state with path-keyed identity without an explicit migration.

---

### Phase 4 — Certified Qdrant target ✅ (internal adapter milestone)

#### Objective

Make one real AI-index destination support deterministic lineage, target-native
suppression metadata, idempotent delete, consistency control, negative
verification, and receipts.

#### ✅ Implementation status — complete for the internal certified-adapter milestone (2026-07-30)

The additive `qdrant-revocation-v1` boundary is implemented without changing
the compatibility target or the engine's four-field reconciliation contract.
It includes deterministic governed point IDs, connector-owned chunk/content
binding, mandatory source-scoped guarded filters, current-generation and
suppression-state freshness checks through Phase 2 state, explicit
`insert_only`/`update_only` writes, target-native suppression, source-generation
fences, exact-ID `consistency=all` non-return read-back, ACL narrowing,
privacy-safe operation evidence carried into durable receipts, strict
collection absence, a 30-second Qdrant operation-timeout parameter,
caller-bounded metadata reads, total-deadline retry polling, and a fail-closed
raw-vector query allowlist that excludes point-ID, cross-collection, prefetch,
compound, inference, and unknown universal-query inputs.

Capability preflight now rejects unsupported or prerelease versions,
non-green collections, partial write consistency (`WCF != RF`), missing or
mistyped payload indexes, and—when `RF > 1`—unhealthy replicated topology or
transfers/resharding. Missing RF/WCF values fail closed. It also rejects
`prevent_unoptimized=true` and incompatible strict-mode timeout, query-limit,
batch, filter-condition-count, or condition-size limits.
Governed point writes, queries, deletes, and ACL transitions revalidate this
report instead of trusting a stale cached result. Index provisioning and
verified collection deletion use narrower version/provisioning or
version/absence checks.

This is deliberately an **internal adapter milestone**, not a production or
physical-erasure certification:

- Qdrant `consistency=all` has intersection semantics. An empty result proves
  non-return for that guarded operation/read policy at the verification
  instant; it is not a timeless guarantee and does not prove every replica or
  backup is physically empty.
- `WCF == RF` rejects an unreplicated successful completion, but a failed
  point mutation may still have partially applied. Strong ordering is ordering,
  not a distributed transaction. The adapter relies on idempotent,
  generation-fenced replay and verified serving suppression; it does not claim
  all-or-nothing mutation across points, shards, or replicas.
- The factual `atomic_serving_suppression` capability means the governed
  serving boundary is centrally fenced and rechecked before results can be
  released. It does not mean Qdrant provides a distributed or multi-point
  atomic transaction.
- `CertifiedQueryContext` is trusted input, not an authenticator. The Phase 2
  verifier checks source/tenant/policy/group/generation suppression-state
  freshness; trusted upstream authentication and policy code must derive the
  principal digest and current group inputs.
- `client.info()` validates the connected endpoint only. Operators must pin
  and verify every production peer, including rolling-upgrade states.
- Synor passes Qdrant's 30-second operation-timeout parameter and caller-bounds
  metadata reads at 30 seconds. Synchronous mutation transport deadlines come
  from finite `QdrantClient` configuration; timeout/unknown completion remains
  retryable because the operation may apply later.
- The local reconstruction slice uses the real Phase 2 runtime over one
  in-memory `StateStore` instance and Qdrant local mode with a stable Drive
  `SourceIdentity`; it is not an OS-process or power-loss test and does not yet
  execute the real `GovernedGoogleDriveSource` through a public
  `App`/declarative target handler.
- Incoming engine action and final outcome sequences are still materialized;
  only provider request/read-back batch size is bounded.
- Live single-node and replicated-cluster tests exist but require an isolated
  operator-owned Qdrant deployment and were not executed here.

#### ✅ Implemented files

- ✅ Preserved and regression-tested compatibility behavior in
  `python/synor/connectors/qdrant/_target.py`.
- ✅ Exported the additive connector surface through
  `python/synor/connectors/qdrant/__init__.py`.
- ✅ Created `python/synor/connectors/qdrant/_revocation.py`.
- ✅ Extended `python/tests/connectors/test_qdrant_target.py`.
- ✅ Created `python/tests/connectors/test_qdrant_revocation.py`.
- ✅ Created `python/tests/revocation/test_qdrant_vertical_slice.py`.
- ✅ Extended `docs/src/content/docs/connectors/qdrant.mdx`.
- ✅ Added `benchmarks/revocation/qdrant_benchmark.py` and benchmark guidance.
- ✅ Extended `python/synor/_internal/verified_sink.py` and
  `python/synor/_internal/revocation_runtime.py` so validated provider
  operation IDs can reach immutable receipts.

#### Immediate safety fix

Before the larger feature:

- [x] ✅ Replace broad `except Exception: pass` around collection deletion.
- [x] ✅ Treat only a confirmed “collection not found” response as idempotent
   success.
- [x] ✅ Re-raise authentication, authorization, transport, timeout, and server
   errors.
- [x] ✅ Verify collection absence before returning in strict mode.
- [x] ✅ Add regression tests with distinct HTTP/gRPC not-found,
  authentication/authorization, transport, server, unknown-client, and
  unconfirmed-result cases.

This fix is useful even if later phases are delayed.

#### Governed point contract

Use deterministic point IDs derived from the source identity plus chunk
identity. Payload must contain filterable, versioned fields such as:

```text
synor.source_digest
synor.source_revision
synor.policy_id
synor.policy_revision
synor.group_graph_revision
synor.tenant
synor.owner_component
synor.generation
synor.principal_digests
synor.servable
synor.retention_state
synor.contract_version
synor.chunk_digest
synor.content_fingerprint
```

Do not store emails or display names. Principal IDs, if required by the Qdrant
filter, must be opaque and tenancy-scoped.

#### Steps

1. ✅ Add Qdrant capability reporting for the exact detected client/server
   version, enforcing stable client and connected-server
   `>=1.17.0,<1.19.0`, with no more than one minor of skew. Production
   operators must independently pin and verify every peer.
2. ✅ Make strict mutation settings explicit:
   - wait for update/delete application;
   - select documented write ordering;
   - record the operation ID/status when exposed;
   - use a documented read consistency for verification;
   - require preflight observation of configured `WCF == RF`, green health,
     required typed indexes, and healthy replicated topology when `RF > 1`;
   - pass Qdrant's 30-second operation-timeout parameter, caller-bound metadata
     reads at 30 seconds, document and use a finite client transport timeout in
     the live profile, and reject conflicting optimizer/strict-mode settings.
3. ✅ For destructive revocation:
   - central suppression is already active;
   - set target-native `synor.servable=false` by exact point ID where
     supported;
   - wait and verify the certified query filter excludes it;
   - condition suppression and delete on exact point ID, source digest, and
     `stored_generation <= action_generation` so the normal N-1 derivative/N
     revocation is removed while a newer generation is preserved;
   - delete the point with explicit wait/ordering;
   - retrieve by exact ID using the chosen read-consistency contract;
   - return normally only when absent.
4. ✅ For ACL narrowing:
   - update normalized policy fields;
   - require the denied set to cover every principal removed from the stored
     previous ACL and reject conflicting same-generation retries;
   - verify denied-principal and allowed-principal query cases;
   - only then allow the new policy revision to serve;
   - preserve connector-owned chunk/content binding, make exact retries
     idempotent, and restore `servable=false` after post-enable exceptions or
     cancellation.
5. ✅ Make filter builders require source, tenant, contract version,
   `synor.servable=true`, active retention, current policy/group revisions,
   current generation, and the opaque principal digest. The Phase 2 verifier
   checks suppression-state freshness before and after the provider await. It
   does not authenticate the principal: trusted upstream code must issue the
   context and both adapter callbacks must be bound to the same trusted Phase 2
   state for the documented profile. Limit the certified serving surface to
   raw dense/multidense or sparse vectors (plus filtered enumeration with
   `query=None`); reject point-ID/model queries, prefetch, cross-collection
   lookup, compound queries, and unknown arguments until every referenced point
   and nested query stage can be recursively authorized.
6. ✅ Do not rely on record counts as proof of a specific point's absence.
7. ✅ Keep stale positive reads and unverified results retryable, use
   provider-native generation filters, and fail capability preflight and
   governed operations when replicated topology or configured write
   consistency cannot support the profile.
   Per-replica physical absence is intentionally not claimed.
8. ✅ Preserve action-to-receipt correlation when the engine batches actions from
   multiple components, and carry the completed Qdrant delete operation ID,
   when exposed, through the verified outcome into the immutable receipt.
9. Implement fake-client/adversarial tests:
   - [x] operation accepted but point still visible;
   - [x] eventual absence after N reads;
   - [x] stale positive read remains retryable;
   - [x] auth and transport failures are redacted and retryable;
   - [x] delete of an already absent point;
   - [x] compatibility point-handler mixed upsert/delete batching remains
     intact; the strict adapter remains additive and separate from the
     compatibility handler;
   - [x] collection delete not-found versus malformed/server failure;
   - [x] ACL update, complete removed-principal coverage, exact retry,
     conflicting-retry rejection, post-enable exception, and re-fencing;
   - [x] receipt redaction and durable provider operation ID;
   - [x] conflicting insert retry, stale resurrection, deterministic ID,
     content-fingerprint, topology, index, and configuration failures;
   - [x] stored-point-ID, prefetch/fusion, cross-collection lookup, mixed-vector,
     and unknown universal-query bypass attempts fail before provider access.
10. ✅ Add live and reconstruction acceptance tests:
    - `QDRANT_URL` single-node logical absence;
    - `QDRANT_CLUSTER_URL` replicated configured-`WCF == RF`/guarded-read
      behavior.
    - [x] Add a separate in-process runtime reconstruction test after apply
      and before receipt/engine final commit, using preserved
      `MemoryStateStore` records.
    - [ ] Execute an OS-level process-kill acceptance run against an isolated
      real cluster; the in-process reconstruction test does not satisfy this
      durability gate.
11. ✅ Record and runtime-check the supported Qdrant server/client ranges in
    the capability documentation.
12. Benchmark and memory:
    - [x] Add 1, 100, 10,000, and 100,000 action planning shapes and cap each
      provider request/read-back batch at 256 by default.
    - [ ] Prove end-to-end O(batch) memory after the public engine integration
      can stream descriptors/outcomes instead of materializing the incoming
      action sequence.

#### ✅ Validation evidence

- ✅ Focused Ruff formatting/lint and MyPy pass for every Phase 4 Python file.
- ✅ The seven-file Phase 2–4 suite collects 204 tests: 201 compatibility,
  conformance, Qdrant-local, verified-sink, runtime, ledger, model, and restart
  tests pass; three external Qdrant tests are skipped because no
  operator-owned live endpoints were configured.
- ✅ `qdrant-client` local mode exercises the conditional governed-upsert and
  delete logic and shows it cannot downgrade or erase a newer authorization
  generation; it does not exercise Qdrant server or replica semantics.
- ✅ The reconstruction test deletes a governed Drive-derived point, injects a
  receipt-recording failure after the target effect, reconstructs the runtime
  over the preserved in-memory `StateStore` records, retries the idempotent
  effect, and closes with a privacy-safe `absent` receipt.
- ✅ The four benchmark shapes cap provider requests at 256 actions; the
  100,000-action planning case produces 391 bounded request batches.
- ℹ️ Live single-node and cluster acceptance was not executed because no
  isolated operator-approved Qdrant resource was authorized.

#### ✅ Exit criteria — satisfied for the bounded Phase 4 internal milestone

- ✅ Qdrant never swallows a destructive failure.
- ✅ Strict point/collection success means the documented query postcondition was
  verified.
- ✅ Compatibility behavior remains available.
- ✅ The stable Drive-identity-to-Qdrant runtime slice survives an injected
  receipt-recording failure, in-process reconstruction, idempotent retry, and
  final close.
- ✅ Source-scoped guarded queries fail closed against stale Phase 2
  suppression state and prove no stale ACL or suppressed point is returned
  through the supported path when a trusted upstream issues the authenticated
  principal context.

#### ⏳ External/public certification gate — still open

- [ ] Run the operator-gated single-node and replicated-cluster suites and
  retain deployment/version/topology evidence.
- [ ] Wire `GovernedGoogleDriveSource` observations and the strict adapter into
  a public `App`/declarative target handler with real final-commit ownership.
- [ ] Add corpus-wide guarded retrieval without pretending one scalar source
  generation authorizes unrelated documents.
- [ ] Wire trusted authenticated-principal/context issuance and current
  group-membership resolution; untrusted caller-constructed
  `CertifiedQueryContext` values remain outside the assurance boundary.
- [ ] Execute OS process-kill and end-to-end memory/SLO acceptance.

#### Rollback

Keep the stricter exception handling. Disable governed declaration/query
helpers if necessary, but retain `synor.servable=false` and central suppression
for open cases. A rollback must not automatically set old points back to
servable.

---

### Phase 5 — Public controlled API, operator UX, and reference product ✅

#### Objective

Turn the proven internal slice into a minimal public product surface with clear
limits, operator visibility, and a compelling runnable example.

#### ✅ Implementation status — complete for the bounded public/reference milestone (2026-07-30)

- [x] ✅ Add the opt-in public governance, revocation, and retrieval facades as
  aliases of the proven internal schemas rather than duplicate models.
- [x] ✅ Add strict `SynorRuntime` policy selection, ledger repair/startup
  health, post-engine-commit finalization, interrupted-finalization recovery,
  controlled status, and optional summary fields.
- [x] ✅ Add a redacted, versioned operator CLI with an explicit trusted
  provider boundary and pre-mutation serving-suppression checks.
- [x] ✅ Build and run the service-free flagship through two real controlled
  engine commits, generated docs, and privacy/failure regression tests.

**Bounded claim:** this milestone is not live Google Drive → Qdrant
certification. `SynorRuntime` records `strict_revocation_control_v1`; it does
not derive the stronger end-to-end proof label merely because strict policy was
selected. A real declarative Drive/target/query registration, live topology
  evidence, and the native durability certification/drift work in Phases 6–7
  remain open.

#### ✅ Implemented public modules

Created:

- ✅ `python/synor/governance.py`
- ✅ `python/synor/revocation.py`
- ✅ `python/synor/retrieval.py`

Extended:

- ✅ `python/synor/execution.py`
- ✅ `python/synor/audit.py`
- ✅ `python/synor/__init__.py`
- ✅ `python/synor/cli.py`
- ✅ `python/tests/test_execution.py`
- ✅ `python/tests/test_phase3_execution.py`
- ✅ `python/tests/test_audit.py`
- ✅ `python/tests/test_revocation_public.py`
- ✅ `python/tests/cli/test_cli.py`
- ✅ `python/tests/test_provable_index_revocation_example.py`
- ✅ `docs/src/content/docs/cli.mdx` through the generator

`python/synor/provenance.py` was reviewed and its existing artifact capture is
consumed by the controlled runtime; no schema change was required.

Created:

- ✅ `docs/src/content/docs/programming_guide/provable_index_revocation.mdx`
- ✅ `examples/provable_index_revocation/`

#### ✅ Minimal public API

The deliberately small top-level surface is exported intentionally through
`__all__` and exercised through public-API tests:

- ✅ `SourceIdentity`
- ✅ `GovernedSourceItem`
- ✅ `AccessSnapshot`
- ✅ `RevocationPolicy`
- ✅ `RevocationCase`
- ✅ `RevocationReceipt`
- ✅ `RetrievalGuard`

Extend `SynorRuntime` with one policy object rather than many unrelated
booleans:

```python
runtime = syn.SynorRuntime(
    state_store=...,
    revocation_policy=syn.RevocationPolicy.strict_query_verified(),
)
```

Do not promote connector-specific tuning knobs until a real deployment needs
them. Connector capability profiles can own safe internal defaults.

#### ✅ Runtime behavior

1. ✅ Controller-routed strict work validates its governed request, snapshot,
   target proof contract, and factual target/query capabilities before
   destructive materialization and again on resume/dispatch. Arbitrary
   connector calls are not automatically inferred as governed.
2. ✅ It records pending revocation intent and fail-closed serving suppression
   before controller-coordinated destructive target work.
3. ✅ Successful `ExecutionReport` values and all run manifests report one of:
   - `succeeded`;
   - `succeeded_with_open_revocations`;
   - `degraded`;
   - `failed` (manifest/exception path; a failed run has no successful report).
4. ✅ An exceeded suppression/verification deadline is never plain success.
5. ✅ `ExecutionReport` has an optional schema-compatible revocation summary:
   observed, suppressed, verified, retained, failed, blocked, and overdue
   counts.
6. ✅ Existing reports and manifests remain readable through optional defaults.
7. ✅ Direct `App.update()` remains supported and is documented outside the
   controlled evidence boundary.
8. ✅ Strict policy selection emits the conservative
   `strict_revocation_control_v1` boundary. The reserved
   `provable_index_revocation_v1` value is not emitted until every governed
   boundary can be attested.

#### ✅ CLI

Add a `revocations` command group only after repository APIs are stable:

```text
synor revocations list [--status ...] [--json]
synor revocations show <case-id> [--json]
synor revocations verify <case-id>
synor revocations retry <case-id>
synor revocations scan --target <target-id>
synor revocations repair-ledger
```

Rules implemented and tested:

- [x] ✅ `list` and `show` are read-only and redact reversible source
  revisions, locators, principals, and provider operation IDs.
- [x] ✅ `verify` and `scan` require an explicit trusted
  `MODULE:OBJECT` operator, enforce unchanged revocation-control bytes, and
  document that provider non-mutation requires read-only credentials.
- [x] ✅ `retry` is an explicit external mutation; it requires the exact active
  serving suppression before invocation and accepts success only with an
  immutable higher-attempt receipt set.
- [x] ✅ No command closes a case or lifts suppression from an operator ticket.
- [x] ✅ JSON output uses versioned schemas and controlled error values.
- [x] ✅ CLI changes regenerate and test
  `docs/src/content/docs/cli.mdx`, including nested/hyphenated commands.

#### ✅ Reference example

Build `examples/provable_index_revocation/` as the flagship demo:

1. ✅ Fake/local source mode for reproducible CI and onboarding.
2. ✅ Optional real Google Drive and Qdrant configuration probe, explicitly
   labeled `live_certified=false`.
3. ✅ Two tenants and two principals.
4. ✅ Documents split into deterministic chunks and vectors.
5. ✅ Stable source/chunk IDs plus tenant, policy, revision, group, and
   generation metadata on every candidate.
6. ✅ Guarded queries before revocation.
7. ✅ Permission-only change with identical document bytes, fingerprint, and
   source revision.
8. ✅ Immediate suppression before scoring.
9. ✅ A deliberately stale first target verification simulates delayed
   consistency.
10. ✅ Verified deletion receipts plus documented CLI inspection.
11. ✅ Partial-source-scan simulation proves zero partial-scan deletions.
12. ✅ Restore simulation proves stale points cannot re-enter guarded serving.

The README must state the trust boundary and show an unsafe direct-Qdrant query
as an explicitly unsupported bypass.

#### ✅ Exit criteria

- [x] ✅ Public names are reviewed against the bounded surface and exported
  intentionally through `__all__`; aliases preserve one persisted schema.
- [x] ✅ Existing user code needs no changes unless it opts into governed
  behavior.
- [x] ✅ CLI and docs are generated and tested.
- [x] ✅ The reference demo runs twice, handles permission revocation through
  real controlled engine commits, and produces no source/principal sentinel in
  control or run evidence.
- [x] ✅ Security reviewers can inspect the conservative runtime boundary,
  factual capability profile/digest, exact trust limits, controlled status,
  and open cases through public docs/reports/CLI without reading implementation
  code.

#### ✅ Rollback compatibility preserved

Keep state readers and suppression enforcement even if public constructors are
temporarily withdrawn. Never ship a version that can write a strict state
schema but cannot safely read it on downgrade without an explicit downgrade
tool.

Phase 5 did not bump the persisted `revocation/v1` major schema. Readers,
repair, suppression enforcement, and legacy report/manifest defaults remain
present.

---

### Phase 6 — Native durability, cleanup, and live-generation hardening

#### Objective

Close engine-level gaps found by the vertical slice before calling the feature
production/GA ready. This phase is deliberately after the contract is proven.

#### Implementation status — bounded native milestone implemented; certification open (2026-07-30)

The additive native design is implemented across the Rust engine, LMDB state,
PyO3, and the internal Python controlled runtime. It establishes local effect
intent/finalization ordering, schema-v3 immutable evidence lineages, strict
verified-sink enforcement, write-free preview parity, strict provider-missing
blockers and fresh-process recovery, retained metadata-only evidence, richer
compatible tombstones, and in-process live queue/incarnation fencing.

This does **not** complete Phase 6's production/GA exit gate. Process-kill and
sudden-power-loss injection, connector-side or multi-process fencing, a copied
real pre-feature database migration drill, the million-action correlation run,
dedicated native-writer stress, compatibility-overhead benchmarks, and the
complete live delete/reinsert race remain open. Recorded scoped commands and
results are in `revocation-phase6-validation.md`.

Final repository validation for this source state reports a passing Cargo
workspace and 1,412 passed / 241 skipped Python tests. The focused core and
live modules report 103 and 37 passes respectively. These results validate the
implemented milestone; they do not satisfy any open certification checkbox.

#### Decision gate

- [x] Amend ADR-0003 with the exact additive native design and explicit
  non-claims.
- [x] Keep the Python `TargetReconcileOutput` shape at four fields.
- [x] Choose a separate native effect keyspace plus a verified-sink capability.
- [x] Use the Phase 2/4 descriptor, batching, verification, and receipt
  contracts rather than inferring effects from arbitrary action fields.

#### Implemented files

Native state and transaction integration:

- `rust/core/src/state/native_effect.rs`
- `rust/core/src/state/db_schema.rs`
- `rust/core/src/state_store/app_store.rs`
- `rust/core/src/state_store/storage.rs`
- `rust/core/src/state_store/submit_session.rs`
- `rust/core/src/inspect/db_inspect.rs`
- `rust/py/src/inspect.rs`
- `rust/py/src/lib.rs`

Engine and live lifecycle integration:

- `rust/core/src/engine/target_state.rs`
- `rust/core/src/engine/execution.rs`
- `rust/core/src/engine/app.rs`
- `rust/core/src/engine/component.rs`
- `rust/core/src/engine/context.rs`
- `rust/core/src/engine/live_component.rs`

PyO3 and internal Python integration:

- `rust/py/src/app.rs`
- `rust/py/src/target_state.rs`
- `python/synor/_internal/app.py`
- `python/synor/_internal/target_state.py`
- `python/synor/_internal/verified_sink.py`
- `python/synor/_internal/inspect_api.py`
- `python/synor/_internal/core.pyi`
- `python/synor/execution.py`
- `python/tests/core/test_native_effect_inspection.py`
- `python/tests/core/test_live_component.py`
- `python/tests/revocation/test_native_provider_recovery.py`
- `python/tests/revocation/test_verified_sink.py`

#### Required native outcomes

- [x] **Durable effect intent.** A described verified effect is written or
  reopened as `pending` in the same precommit transaction as ordinary tracking
  state, before the external sink runs.
- [x] **Verified sink capability.** Sinks default to `Legacy`; the internal
  verified Python wrapper registers `query_verified` assurance and a
  redacted, validated per-action descriptor through PyO3. Existing sinks need
  no migration. A Rust-owned carrier binds the descriptor consumed by native
  planning to the exact action consumed by the verified wrapper.
- [x] **Immutable effect identity.** The connector descriptor action ID remains
  the Phase 2 operation/receipt-correlation ID. The engine separately
  allocates an evidence ID from the opaque tracking locator and lineage epoch.
  Exact unresolved retries reuse it; a lifecycle after completion gets a new
  evidence ID without rewriting retained history.
- [x] **Finalization ordering.** Sink success moves native effects to
  `verified`; only the final tracking transaction can move them to
  `completed`. Sink failure records a controlled failure and retains
  retryable tracking.
- [x] **Provider-missing safety in strict mode.** A deterministic,
  metadata-only `blocked` cleanup effect is persisted and tracking is retained.
  A returning provider must produce a query-verified action before final
  commit resolves the blocker.
- [x] **Strict cleanup error surfacing.** Strict root updates fail while native
  effects are unresolved. `App.drop()` and storage-level app deletion refuse
  to mutate an app with any non-completed native effect.
- [x] **Compatibility boundary.** Direct `App.update()` stays in compatibility
  mode. The pre-existing compatibility behavior for an absent provider is
  intentionally unchanged and carries no governed-cleanup claim.
- [x] **Write-free preview parity.** Controlled preview runs native
  proof-contract planning but emits no precommit write plan and invokes no
  apply/verify/record callback. It rejects proof drift, missing recovery
  tracking, unavailable strict-cleanup providers, child-provider actions, and
  live mounts without mutating tracking, schema, evidence, cursors, or target
  state.
- [x] **Rich tombstone/generation schema.** Child existence and tombstone
  records support cause, optional source digest and generation, creation time,
  attempt count, safe last-error code, and verification policy. Empty legacy
  tombstones decode conservatively; stale known-generation cleanup cannot
  erase a newer tombstone.
- [x] **Local live-generation fencing and transition queue.** Live
  incarnations reserve a monotonic generation in LMDB, persist it before
  `process_live`, generation-check committed state, and propagate a
  cancellation fence through native submit boundaries. Incremental update,
  nested live mount, and delete use one per-subpath latest-operation-wins
  queue gated by `update_full`. A transition or successor handoff fails if the
  old incarnation cannot drain within the bounded timeout.
- [ ] **Distributed live-generation fencing.** Connector-side CAS/generation
  checks and an app-wide multi-process lease are not implemented. The local
  cancellation fence cannot recall a remote mutation already in flight.
- [x] **Retained evidence.** Completed effect records live outside target
  tracking, survive tracking reduction, and are retained by ordinary app drop.
- [x] **Tracking-owner cleanup repair.** Delete-mode commit now removes the
  inverted target-owner rows referenced by the component's tracking record
  instead of leaving dangling ownership.

#### Chosen internal design

The implemented design is the original Option A:

1. Keep the four-field reconcile output unchanged.
2. Let a sink declare `Legacy` or `Verified(query_verified)` assurance.
3. Extract an optional metadata-only descriptor from each opaque action.
4. Persist native intent in component precommit.
5. Treat normal verified-wrapper return as proof that its ordered actions
   reached the required postcondition and Phase 2 evidence was recorded.
6. Persist `verified` separately, then complete only in final tracking commit.

Detailed receipts remain in the Phase 2 control-plane ledger. Native state
stores only the bounded descriptor, opaque tracking fingerprint, controlled
status/cause/error fields, policy, timestamps, and attempt count. It never
serializes a target payload, raw locator, source content, principal, remote
response, credential, or free-form exception. Bounded-token validation is not
a PII classifier; certified profiles must still supply opaque, non-reversible
action IDs.

The connector action ID and engine evidence ID serve different contracts.
`descriptor.action_id` is stable connector input used for operation and receipt
correlation. Native effect record v2 adds `evidence_id`, an engine-owned
locator/epoch identity used for LMDB keys and lifecycle transitions. Record-v1
state has no separate field and falls back to the action ID. A retained
per-locator lineage cursor prevents a completed record from being reopened
when a later app lifecycle repeats the same connector action ID.

#### Schema and migration

- [x] Add the schema-v3 singleton at `0x38`; metadata-only effect records at
  `0x40`; provider-missing allocation cursors at `0x48`; and ordinary
  per-locator lineage cursors at `0x50` in each existing app database.
- [x] Use native effect record version 2 to persist a separate engine evidence
  ID while retaining connector action ID in the descriptor. Version-1 records
  decode with the action ID as their legacy evidence ID.
- [x] Treat a missing marker plus empty effect, obligation, and lineage
  keyspaces as an untouched pre-feature database. Install version 3 lazily in
  the first native-effect transaction.
- [x] Read supported schema-v1/v2 state and, before its next native write,
  perform one bounded evidence scan that builds every ordinary per-locator
  lineage cursor and atomically advances the marker to v3.
- [x] Make native-effect reads/writes, strict completion checks, inspection,
  and protected drop in the current binary refuse future native schema
  versions and any native metadata that exists without a schema marker.
- [x] Validate `0x48`/`0x50` cursor key bindings and referenced evidence
  metadata/status on schema reads. Missing or forged cursor evidence makes
  counts and protected drop fail closed without mutation.
- [x] Decode an empty legacy tombstone as `cause=undeclared`,
  `verification=legacy_unverified`, unknown generation, zero timestamps and
  attempts, and no last-error code.
- [x] Expose metadata-only native effect counts for
  `pending|verified|failed|blocked|completed`.
- [ ] Run a migration test against a copied, real pre-feature app database.
  Current coverage constructs untouched, empty-legacy, and schema-v1 upgrade
  states in unit tests; the same supported-version migration path accepts v2,
  but neither is the required real fixture.
- [ ] Publish a downgrade/export tool and deployment runbook. An older binary
  released before schema version 3 cannot be made to recognize or refuse that
  schema; a v3 upgrade is therefore a one-way operational boundary for this
  milestone.
- [ ] Define and validate completed-effect retention, export, and compaction.
  Completed records are retained indefinitely in this milestone.

#### Tests

- [ ] Kill the process after precommit, after apply, after verification, and
  during final commit. Controlled Phase 2 interruption tests do not substitute
  for native process-kill or sudden-power-loss injection.
- [x] Verify strict policy rejects a legacy cleanup before apply.
- [x] Verify malformed native descriptors are rejected with a fixed redacted
  error before apply.
- [x] Scan the serialized LMDB files after planting action content, principal,
  credential, raw-locator, and remote-error sentinels. The metadata-only native
  effect contains the expected opaque action/digests and none of the planted
  raw values.
- [ ] Add the equivalent planted-sentinel serialization scan for a rich child
  tombstone value. Existing tombstone schema/default tests do not plant every
  sensitive category.
- [x] Verify a verified sink failure keeps effect/tracking state retryable.
- [x] Verify the native `pending → verified → completed` state machine and
  finalization preconditions at the AppStore transaction boundary.
- [x] Verify exact proof-contract retry, connector action ID versus engine
  evidence ID separation, immutable completed evidence, and successor
  locator-epoch allocation.
- [x] Verify missing/mismatched lineage evidence and missing/forged obligation
  evidence make schema reads, counts, and protected drop fail closed without
  mutation.
- [x] Verify native evidence survives operational app drop and every
  non-completed effect makes drop non-mutating.
- [x] Verify future schema refusal, safe default decoding, and
  known-generation tombstone cleanup.
- [x] Add a direct Delete-mode regression that creates target ownership,
  deletes the owning component, and proves the inverted owner row is gone.
- [x] Remove a provider across fresh processes and prove, end to end,
  that strict tracking and the blocked obligation remain until verified
  recovery; a compatibility retry preserves the strict blocker, and repeated
  recovery is idempotent.
- [x] Verify strict preview returns original actions without apply, verify,
  record, native-effect, or target-state mutation; repeated preview stays
  empty, the real update applies once, and proof-drift/no-tracking cases reject
  in both planning and execution.
- [x] Verify preview rejects live mounts without stable-path or target-state
  mutation, and verify nested live→plain and plain→live transitions use the
  shared queue with latest-operation-wins behavior.
- [ ] Exercise one million batched synthetic actions and confirm descriptor,
  receipt, and native-effect correlation.
- [ ] Reproduce the complete live delete/reinsert race under deterministic
  scheduling and prove every old-incarnation write/effect boundary is fenced.
- [ ] Open a copied pre-feature LMDB database and run the full compatibility
  lifecycle unchanged.
- [x] Route every new LMDB write through `Storage::run_txn` or a caller-owned
  transaction opened by it.
- [ ] Run a dedicated concurrent single-writer stress test for the new native
  lifecycle.

#### Exit criteria

- [ ] Process-kill testing establishes that no supported crash boundary loses
  both target tracking and the native revocation obligation.
- [x] Strict provider removal cannot silently complete governed cleanup at the
  implemented engine boundary.
- [x] Strict root update surfaces outstanding native deletion failure.
- [x] Existing legacy sinks remain source-compatible and direct app updates
  keep compatibility semantics.
- [ ] Copied old-database compatibility and downgrade operations are validated.
- [ ] Rust benchmarks establish acceptable overhead outside strict mode.

**Phase 6 production/GA exit gate:** not satisfied. The checked items define
the bounded native milestone only.

#### Rollback

Disable creation of new strict effects while retaining read, retry, inspection,
and drop protection. Never downgrade by deleting the native keyspace or
suppression evidence. Do not open a schema-v3 database with a binary that
predates native schema version 3. A documented one-way export path for using
an older binary remains an open operational deliverable.

---

### Phase 7 — Drift, orphan, cache, and restore assurance

#### Objective

Prove continued convergence after external mutation, target restore, missed
events, and long-running operation.

#### Implementation status — not started (2026-07-29)

External drift/orphan enumeration, cache-recipient completion, restore replay
gating, and scheduled assurance scans are not implemented.

#### Proposed files

Create:

- `python/synor/revocation_scan.py` or an internal precursor
- `python/synor/_internal/target_verifier.py`
- `python/tests/revocation/test_drift_scan.py`
- `python/tests/revocation/test_restore.py`
- `python/tests/revocation/test_cache_invalidation.py`

Extend:

- `python/synor/cli.py`
- `python/synor/dashboard.py` if a read-only revocation view fits the existing
  dashboard boundary
- `python/synor/audit.py`

#### Steps

1. Define a target enumeration/verifier protocol separate from target mutation.
2. Let connectors report whether they support:
   - exact lookup;
   - source-ID filtered lookup;
   - full governed enumeration;
   - consistent snapshot/scroll;
   - cache enumeration;
   - physical-erasure status.
3. Implement source-to-target drift scan:
   - expected but absent;
   - present with stale ACL revision;
   - present while source is suppressed/revoked;
   - target artifact with no known source/owner;
   - duplicate target locators;
   - unknown/corrupt lineage.
4. Never delete an orphan solely because a scanner cannot resolve its source.
   Create an `ambiguous_orphan` case, suppress it when possible, and require
   policy/operator resolution.
5. Add registered cache recipients. Cache keys include source/access generation
   and are invalidated/suppressed with the same case.
6. Add periodic schedules with jitter and per-target concurrency/rate limits.
7. Add restore gates:
   - import current suppression/receipt state;
   - replay open obligations;
   - scan for resurrection;
   - only then mark the target ready for serving.
8. Add cryptographic digest validation for receipt chains and report missing or
   reordered events.
9. Export safe structured metrics and optional OpenTelemetry hooks without
   requiring a telemetry backend.
10. Add chaos tests that mutate Qdrant outside Synor, restore an older
    collection snapshot, drop cache invalidation, and lose a source event.

#### Exit criteria

- An externally resurrected or stale-ACL point is detected, suppressed, and
  repaired.
- Orphan scanning never turns uncertainty into unreviewed destruction.
- Restore cannot enable queries before revocation replay/drift checks.
- Cache recipients appear in case completion and evidence.

#### Rollback

Stop scheduled scans, but keep query suppression and open cases. Scanner
failure cannot lift suppression or mark cases complete.

---

### Phase 8 — Connector expansion and GA hardening

#### Objective

Turn one vertical slice into a reusable connector standard and credible
production offering.

#### Implementation status — not started (2026-07-29)

The second source/target pair, connector conformance kit, production SLOs,
multi-tenant chaos certification, and GA support policy are not implemented.

#### Recommended order

1. **PostgreSQL target**
   - exact deterministic keys;
   - `DELETE ... RETURNING`/read-back in strict mode;
   - transaction-scoped ACL update;
   - indexed tenant/source/policy columns;
   - documented guarded query/RLS pattern.
2. **Local filesystem source/target**
   - surface directory/stat permission failures;
   - snapshot completeness;
   - mode/owner/group/ACL fingerprint;
   - reject absolute and `..` child keys;
   - enforce resolved target-root containment;
   - atomic file replace;
   - distinguish logical absence from secure filesystem erasure.
3. **Kafka source**
   - standard governed event envelope;
   - source/access/group revisions;
   - per-key ordering contract;
   - durable offset only after downstream readiness and receipt persistence.
4. **Amazon S3 source**
   - event notifications plus periodic inventory;
   - stable bucket/key/version identity;
   - pluggable policy resolver;
   - explicit limits around IAM, access points, bucket policy, and Object Lock;
   - never claim universal effective IAM evaluation.
5. **LanceDB target**
   - clearly separate current-table invisibility from old-version reclamation;
   - retention/optimization status;
   - secure-erasure capability only when maintenance and storage lifecycle can
     be proved.
6. **Microsoft Graph / SharePoint**
   - delta tokens, stable drive/item IDs, sharing-only changes;
   - external group graph;
   - connector permission-gap disclosure.

#### Connector conformance kit

Every governed source must pass:

- immutable identity across rename/move;
- ACL-only and inherited-policy change;
- permission expiry;
- complete/partial snapshot behavior;
- cursor crash/replay;
- delete/access-loss ambiguity;
- rate-limit and cancellation;
- evidence redaction.

Every governed target must pass:

- idempotent apply/delete;
- false-success detection;
- eventual-consistency fence;
- exact negative verification;
- batch action/result correlation;
- ACL/tenant filter enforcement;
- already-absent behavior;
- auth/transport error propagation;
- drift enumeration or explicit unsupported capability;
- retention/legal-hold disclosure.

Every guarded retriever must pass:

- tenant required;
- filtering before scoring/content;
- current policy and group revision;
- suppressed source denial;
- stale/missing policy fail closed;
- cross-tenant adversarial queries;
- cache suppression;
- structured safe denial logs.

#### Exit criteria

- At least two governed sources and two governed targets pass the published
  conformance suite.
- Capability matrices identify every unsupported guarantee.
- Independent security review finds no silent downgrade path.
- Upgrade, downgrade, disaster-recovery, and incident runbooks are published.
- Strict mode has measured SLOs and an error-budget policy.

---

## 15. Backward compatibility, migration, and rollback

### 15.1 Compatibility modes

Use explicit modes; never infer strictness from the presence of some metadata:

| Mode | Existing engine/connector behavior | Revocation claim |
|---|---|---|
| `compatibility` | Current `App` and connector semantics; best-effort cleanup and normal retry. | None beyond existing declarative reconciliation. |
| `governed_observe` | Writes identity/lineage/evidence and measures capabilities, but does not block unsupported targets. | Observation only; no completion guarantee. |
| `strict_query_verified` | Requires source completeness, serving suppression, current ACL evaluation, verified sinks, and open-case reporting. | Certified logical non-retrievability for supported paths. |
| Future `strict_erasure_attested` | Also requires destination retention/physical-erasure proof. | Connector-specific erasure only. |

Existing applications remain in `compatibility` unless they opt in. Never
silently treat a compatibility run as strict merely because it produced some
receipts.

### 15.2 Source identity migration

Changing Google Drive component keys from name/path to file ID changes stable
ownership. Do not mutate existing `items()` in place.

Recommended migration:

1. Upgrade to a release that can read the new governance state while still
   running the old pipeline.
2. Back up the Synor LMDB directory and control-plane state.
3. Create a new governed app/environment or a new target namespace/collection.
4. Run a complete governed source snapshot into the new target.
5. Verify ACL behavior and every expected derivative.
6. Switch only the guarded query path to the new target.
7. Keep central suppression covering both old and new target identities.
8. Use a controlled, verified cleanup run to remove the old path-keyed target.
9. Retain migration receipts and the old-to-new source identity map for the
   configured evidence period.
10. Remove the old target only after drift scan returns zero governed
    artifacts.

Do not let both path-keyed and ID-keyed components declare the same target
point IDs. That would cause ownership transfer rather than safe dual operation.

### 15.3 Ledger schema migration

- Prefix every record with a major schema version.
- New optional fields have safe defaults.
- Unknown major versions fail closed for serving and strict mutation.
- An immutable receipt is never rewritten merely to upgrade formatting.
- Build a new summary projection from old events instead.
- State-store encryption keys remain external and are never migrated into the
  repository.

### 15.4 Native database migration

Phase 6 adds a versioned, prefixed native-effect keyspace to each existing app
database without repurposing target tracking:

- a missing schema marker plus empty `0x40` effect, `0x48` obligation, and
  `0x50` lineage keyspaces remains the valid pre-feature state;
- the first native effect write installs schema version 3 atomically with the
  effect and its cursor metadata;
- schema-v1/v2 databases remain readable and perform one bounded effect scan
  to create ordinary lineage cursors before their next native write installs
  v3;
- effect record version 2 separates the connector action ID from the
  engine-owned, locator/epoch-derived evidence ID while retaining v1 fallback;
- native-effect access, strict completion checks, inspection, and protected
  drop in the current binary fail closed on a future schema marker, native
  records with no marker, or corrupt cursor/evidence bindings;
- legacy child-existence records decode with unknown generation, and empty
  tombstones decode with conservative legacy defaults;
- strict provider blockers retain target tracking, and app drop is
  non-mutating while any native effect is non-completed;
- completed native evidence and the schema marker survive ordinary app drop.

Native activation is one-way for this milestone. A genuinely older executable
cannot know about a schema added after it was released, so it cannot be made to
refuse that schema retroactively. Do not downgrade a schema-v3 app database in
place. A copied real pre-feature database migration drill and a documented
one-way export path remain required before the production gate.

### 15.5 Provider rename/removal

Introduce stable provider IDs and explicit aliases before renaming a governed
connector provider. A deployment must be able to load the old cleanup handler
until all old target obligations close.

Provider removal checklist:

1. Inventory target states/effects owned by the provider.
2. Run verified cleanup or register a migration alias.
3. Confirm zero open cases and zero pending, verified, failed, or blocked
   native effects for that provider.
4. Run external orphan scan.
5. Only then remove the provider registration/code.

### 15.6 Safe rollback rule

Rollback is allowed to stop new observation or mutation. It is not allowed to
make a previously suppressed source retrievable.

A safe older release must either:

- understand and enforce the current suppression schema; or
- refuse to start the certified query path.

“Delete the revocation directory and retry” must never be a recovery
instruction.

---

## 16. Connector assurance roadmap

This matrix describes the confirmed current gap and intended certification
order. It is not a claim that proposed capabilities already exist.

| Connector | Stable governed identity today | ACL/access change today | Complete removal feed/snapshot today | Verified target postcondition today | Planned role |
|---|---:|---:|---:|---:|---|
| Google Drive source | Yes in governed mode: immutable file ID + connector/scope identity; compatibility `items()` remains path keyed | Governed ACL/policy/group revisions, expiry, inherited-permission resolution, and descendant invalidation are fake-service tested | Authoritative snapshot sessions and durable Changes replay are implemented; live Workspace acceptance remains open | N/A | First governed source; Phase 5 public types/configuration probe are complete, while a production declarative handler and live acceptance remain open |
| Local filesystem source | Path-based | No ACL/mode memo input | Watch + rescan, but permission/stat gaps can be skipped | N/A | Second source after completeness hardening |
| Amazon S3 source | Bucket/key convention; version handling varies | No policy resolver | Listing only; no standard live revocation path | N/A | Later, with explicit IAM-policy limits |
| Kafka source | Key-based tombstones | No governed access envelope | Strong incremental primitive; no authoritative inventory itself | N/A | Later governed-event transport |
| Qdrant target | Yes in the additive strict adapter: source+chunk UUID with connector-owned lineage/content binding | Source-scoped current-generation/trusted-context principal filter plus verified ACL narrowing | N/A | Full-WCF strong completed writes, target-native suppression, exact-ID `consistency=all` guarded non-return, immutable receipt evidence; live topology acceptance remains open | First certified AI-index adapter; production declarative handler remains open |
| PostgreSQL target | Primary-key based | User schema only | N/A | SQL success only; no governed receipt/read-back | Second certified target |
| Local filesystem target | Relative key intended | Filesystem permissions are external | N/A | Idempotent logical delete only | Later after containment/atomicity fixes |
| LanceDB target | Row key | User schema only | N/A | Logical delete; physical reclamation delayed/best effort | Logical-deletion tier first; erasure tier later |

Every published connector page must include:

- supported assurance levels;
- source/target version range tested;
- identity and tenancy boundary;
- permission model and known blind spots;
- incremental and full-reconciliation behavior;
- acknowledgement/consistency/verification semantics;
- retention and physical-erasure limits;
- rate limits and expected SLO envelope;
- whether direct client queries bypass the Synor guarantee.

---

## 17. Observability, SLOs, and operator response

### 17.1 Separate latency clocks

One “sync latency” metric hides the security-critical wait. Record:

- `revocation_detection_latency`
- `serving_suppression_latency`
- `target_plan_latency`
- `target_dispatch_latency`
- `target_ack_latency`
- `target_consistency_fence_latency`
- `target_verification_latency`
- `case_total_latency`

Also record:

- `open_revocation_cases`
- `overdue_revocation_cases`
- `stale_acl_artifact_count`
- `orphan_artifact_count`
- `ambiguous_removal_backlog`
- `provider_missing_backlog`
- `verification_retry_count`
- `failed_closed_query_count`
- `cross_tenant_denial_count`
- `receipt_chain_validation_failures`
- `restore_resurrection_test_failures`
- `source_snapshot_partial_count`
- `source_cursor_replay_count`

Labels must have bounded cardinality. Do not label metrics with raw source IDs,
target locators, principals, filenames, or case IDs.

### 17.2 Candidate launch objectives

These are engineering targets to benchmark, not universal customer promises:

| Clock | Reference-slice objective |
|---|---|
| Event observed → central suppression written | p99 ≤ 5 seconds |
| Suppression written → guarded query denies | p99 ≤ 1 second |
| Qdrant delete dispatched → query-verified absent | p99 ≤ 60 seconds under healthy supported deployment |
| Open case passes `verify_by` | Alert within 1 minute |
| Full external orphan scan | At least daily for reference deployment |
| Restore → governed serving enabled | Only after zero resurrection failures; no time-based bypass |

Detection time from the original source change depends on polling interval,
source event delivery, and source API limits. Publish that separately for each
connector. Do not combine it with Synor's observed-event handling time.

### 17.3 Required alerts

- suppression could not be installed;
- guarded query policy state is unavailable/corrupt;
- a strict target lacks a required capability;
- verification exceeded deadline;
- provider is missing for open cleanup;
- a source snapshot is partial or a cursor is invalid;
- receipt chain cannot be rebuilt;
- drift scan found a suppressed/revoked artifact still retrievable;
- restore gate found resurrection;
- group graph or ACL resolver is stale beyond its policy limit.

### 17.4 Operator runbook states

| Status | Meaning | Operator action |
|---|---|---|
| `suppressed_pending_delete` | Queries are blocked; cleanup in progress. | Monitor until deadline; no emergency access change. |
| `verification_retrying` | Destination has not proved postcondition. | Check target health/rate limits; preserve suppression. |
| `blocked_provider_missing` | Cleanup implementation is unavailable. | Restore provider/alias; do not clear local state. |
| `blocked_capability` | Strict guarantee cannot be met. | Choose a capable target/path or explicitly leave strict mode; never relabel success. |
| `ambiguous_removal` | Source cannot distinguish delete/access/move. | Investigate source scope; suppression remains. |
| `retained_isolated` | Policy prevents erasure but serving denial is verified. | Review hold/retention expiration; do not call it erased. |
| `drift_detected` | External target differs from expected state. | Reconcile and verify; assess exposure window. |
| `receipt_corrupt` | Evidence history is incomplete or inconsistent. | Fail closed where policy requires; repair from replicas/backups. |

---

## 18. Security, privacy, and compliance boundaries

### 18.1 Authorization

- Retrieval requires authenticated principal and tenant context.
- Tenant isolation is applied before semantic scoring.
- Grant/deny semantics are preserved; deny must not disappear during
  normalization.
- Policy and group revisions are freshness-checked.
- Missing policy is denial in strict mode.
- Source connector credentials authorize observation; they do not automatically
  define every end-user's effective access.
- The LLM is never asked to enforce access control.

### 18.2 Least privilege

- Drive credentials use only required read scopes and an explicitly configured
  delegation model.
- Qdrant credentials should be limited to the certified collection/namespace.
- Evidence stores use separate credentials/keys from target data.
- Query clients should not have mutation privileges.
- Direct write access to governed indexes is restricted or continuously
  scanned as drift.

### 18.3 Sensitive metadata

ACLs, group relationships, filenames, and target locators may themselves be
sensitive. Protect the operational lineage store with:

- `EncryptedStateStore` or an encrypted volume;
- restrictive filesystem/database permissions;
- explicit retention;
- separate backup policy;
- redacted operator output;
- no raw exception messages;
- no serialization via arbitrary object `repr()`.

### 18.4 Receipt integrity

A digest chain detects some accidental removal/reordering but does not prove
who authored an event. If customers require non-repudiation:

- add optional signed checkpoints using externally managed keys;
- rotate/sign keys through a documented policy;
- export checkpoints to immutable/WORM storage;
- keep signing outside the first minimal public API.

Do not market a hash chain alone as tamper-proof.

### 18.5 Retention and legal hold

The application supplies a policy decision. Synor enforces the resulting
mechanical state:

- `destroy`: remove and verify;
- `restrict`: keep only as policy permits, deny serving;
- `preserve_on_hold`: isolate and verify non-serving;
- `investigate_ambiguous`: suppress and wait for resolution.

Receipt retention can differ from artifact retention, but evidence itself may
be personal data. Configure minimization and deletion policies with counsel;
this design is not legal advice.

### 18.6 Denial-of-service controls

An attacker who can generate ACL churn or many source tombstones may trigger
large delete/verification workloads. Add:

- per-source and per-target rate limits;
- bounded queues with durable backlog;
- batch-size and memory limits;
- priority for serving suppression over physical purge;
- coalescing by source generation;
- circuit breakers that keep queries fail closed;
- fairness between tenants;
- alerting on malicious or accidental revocation storms.

### 18.7 Supply-chain and connector trust

Verification is only as trustworthy as the connector and target client:

- pin/test supported dependency versions;
- maintain connector-specific integration tests;
- review broad exception handling;
- record server/client version in receipts where safe;
- keep mutation and verifier code small;
- document any remote response that cannot be independently checked;
- run security review before certifying a connector.

---

## 19. Comprehensive test strategy

### 19.1 Test layers

| Layer | Required coverage |
|---|---|
| Pure model | Canonical IDs, ACL digest ordering, deterministic case/action IDs, legal transitions, generation comparison, receipt chain, schema decoding. |
| State store | Atomic single-key writes, interrupted projection update, ledger rebuild, encryption, corruption, duplicate replay, concurrent coroutines. |
| Synthetic engine E2E | Declare/update/delete, ACL-only memo invalidation, complete/partial snapshot, false success, eventual consistency, provider missing, ownership transfer. |
| Source contract | Identity, pagination, cursor replay, ACL/inheritance/expiry, ambiguity, partial scope, rate limit, cancellation. |
| Target contract | Idempotence, ack versus verify, consistency fence, exact read-back, already absent, batching/correlation, target-native ACL filtering. |
| Retrieval contract | Pre-score tenant/access filtering, suppression, group revision, stale/missing policy fail closed, cache behavior. |
| Runtime/CLI | Strict capability gate, status summaries, retry/verify semantics, JSON schemas, redaction, generated docs. |
| Crash/chaos | Kill at every lifecycle boundary; target/network errors; missed source event; out-of-band target mutation; old snapshot restore. |
| Migration | Old LMDB/control state, path-ID migration, compatibility mode, safe downgrade refusal. |
| Performance | Large inventory, revocation storm, group churn, batched deletes/read-back, ledger size, query guard latency. |
| Documentation | Flagship example, connector capability tables, CLI generation, docs build, link checks. |

### 19.2 Mandatory failure-injection points

Inject a process stop or exception:

1. before observation append;
2. after observation but before suppression;
3. after suppression but before query confirmation;
4. during source pagination;
5. after source cursor receipt but before checkpoint;
6. after Synor precommit but before target apply;
7. midway through a batched target apply;
8. after target acknowledgement but before consistency fence;
9. after fence but before negative read;
10. after verification but before sink return;
11. after sink return but before engine final commit;
12. after engine commit but before receipt append;
13. after receipt append but before case-summary projection;
14. during drift repair;
15. during restore gating.

After restart, assert:

- suppression is not lost;
- duplicate operations are idempotent;
- tracking or a durable effect obligation remains;
- case history is reconstructable;
- no older generation can serve;
- eventual successful retry closes exactly once.

### 19.3 Mandatory adversarial scenarios

- same filename in different Drive folders;
- rename without content change;
- source item moved between corpora;
- ACL changes from group A to group B with identical bytes;
- user removed from a group without item ACL change;
- direct parent ACL change affecting a deep subtree;
- expired permission timer races with a fresh extension;
- source returns an empty first page and fails on the second;
- Qdrant returns an operation ID while point remains queryable;
- one replica is stale;
- collection deletion receives auth failure;
- direct target client inserts an orphan point;
- old target snapshot is restored;
- two components attempt the same physical target key;
- evidence fixture contains secrets and PII in every possible error/action
  field;
- tenant A probes source IDs and similarity scores from tenant B;
- ledger event is missing, duplicated, reordered, or corrupted.

### 19.4 Property-style invariants

Use deterministic randomized tests even if the project does not add a property
testing dependency:

- canonical ACL digest is invariant to entry order;
- different grant/deny/inheritance semantics never hash to the same canonical
  representation in the test corpus;
- state-machine transitions never move backward except through an explicit new
  attempt/generation;
- a lower source generation cannot clear higher-generation suppression;
- every closed case has an acceptable terminal obligation for every expected
  target;
- no serialized evidence contains planted sensitive sentinel values;
- retrying any action N times yields the same external state.

### 19.5 Performance and scale gates

Benchmark at minimum:

- one million source identities in a complete snapshot;
- 100,000-point revocation wave;
- 10,000 ACL-only updates;
- 100,000-member group change without per-chunk membership expansion;
- one million retained receipts and summary rebuild;
- query guard batches of 1, 10, 100, and 1,000 candidate source IDs;
- strict versus compatibility run overhead;
- source and target rate-limit behavior.

The benchmark must measure memory, state growth, API calls, suppression latency,
verification latency, and backlog recovery—not only throughput.

### 19.6 Live-service testing

Normal CI stays deterministic with fake services and local targets. Add gated
tests for:

- Google Drive using a dedicated test workspace/drive;
- Qdrant using `QDRANT_URL`;
- PostgreSQL using the repository's testcontainer pattern;
- multi-replica Qdrant in scheduled/nightly CI if feasible.

Never run destructive acceptance tests against an unscoped customer target.

---

## 20. Validation commands

Use the repository's existing toolchain and run commands in proportion to the
phase.

### Documentation-only change

```bash
npm --prefix docs ci
npm --prefix docs run build
```

The architecture file is also covered by the repository's Markdown link
workflow.

### Python-only phases

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest python/
```

### Rust or PyO3 phase

```bash
uv run maturin develop
cargo fmt --check
cargo test
uv run mypy
uv run pytest python/
```

### CLI changes

```bash
uv run python dev/generate_cli_docs.py
uv run pytest python/tests/cli/
```

Confirm `docs/src/content/docs/cli.mdx` contains only intended generated
changes.

### Before publishing a PR

```bash
prek run --all-files
npm --prefix docs run build
```

Some hooks require `protoc` as described in `AGENTS.md`. Run connector live
tests only with dedicated test resources and their documented environment
variables.

### Phase-specific targeted suites

```bash
uv run pytest python/tests/revocation/
uv run pytest python/tests/core/test_flat_target_states.py
uv run pytest python/tests/core/test_component_target_states.py
uv run pytest python/tests/core/test_live_component.py
uv run pytest python/tests/connectors/test_google_drive_source.py
uv run pytest python/tests/connectors/test_qdrant_target.py
```

Add new files to these invocations as they are created.

---

## 21. Design decisions, defaults, and unresolved questions

The following defaults let development start without waiting for every future
product decision.

### 21.1 Decisions made by this plan

- **Product name:** Provable Index Revocation.
- **First source:** Google Drive.
- **First AI-index target:** Qdrant.
- **Identity:** connector instance + source scope + immutable provider item ID.
- **Immediate boundary:** central suppression plus a certified guarded query
  path.
- **Target success:** apply then target-specific read/query verification.
- **First implementation seam:** Python verified-sink wrapper; no fifth
  `TargetReconcileOutput` field.
- **First ledger:** single-process, single-event-loop adapter over
  `StateStore`, with immutable events and repairable summaries.
- **GA durability:** the bounded native effect state is implemented after the
  real vertical slice; process-kill/power-loss, remote fencing, migration, and
  performance certification remain release gates.
- **Ambiguous removal:** suppress and report honestly; do not label it physical
  deletion.
- **ACL narrowing:** update policy and query authorization; do not necessarily
  delete content for still-authorized principals.
- **Group memberships:** separate versioned graph, not expanded onto every
  chunk.
- **Shared derivative:** per-source contributions or one aggregate owner; no
  implicit shared ownership.
- **Physical erasure:** not part of the first launch claim.
- **Existing APIs:** compatibility by default; strict mode opt-in.

### 21.2 Current status of empirical questions

| # | Status on 2026-07-30 | Question and current answer |
|---|---|---|
| 1 | **✅ Answered for the bounded public runtime** | The verified wrapper accesses the run-scoped ledger through the internal Phase 2 runtime under the tested batching and coroutine-concurrency paths. Phase 5 adds opt-in public controlled-runtime policy, startup repair/health, post-engine-commit finalization, interrupted-finalization recovery, and public reports. Automatic inference of arbitrary connector calls and a production declarative Drive → Qdrant handler remain intentionally outside this answer. |
| 2 | **✅ Answered for the internal certified Qdrant adapter milestone** | The `qdrant-revocation-v1` profile runtime-checks a stable client and connected server `>=1.17.0,<1.19.0`, with at most one minor of skew, because explicit `insert_only`/`update_only` modes require Qdrant 1.17+. It requires a green collection, explicitly observed configured `WCF == RF`, typed payload indexes, compatible strict-mode query/filter/batch limits, and RF>1 replicated topology; it also passes Qdrant's 30-second operation timeout, caller-bounds metadata reads, and requires `wait=True`, strong ordering, completed results, durable operation-status evidence plus an operation ID when exposed, and source-scoped `consistency=all` guarded/exact-ID non-return. The locked client is 1.18.0; finite mutation transport timeout and verification of every cluster peer are operator responsibilities, the RF=1 adapter check does not inspect cluster-transfer state, preflight is not an atomic provider attestation, live server/topology acceptance remains operator-gated, and physical per-replica absence is not claimed. |
| 3 | **Answered for the internal slice** | Phase 2 uses explicit per-action descriptors and correlates one verification outcome with each tested flattened action. |
| 4 | **Partially answered by the bounded Phase 6 implementation** | Delete-mode removes inverted owner rows; a fresh-process provider-removal lifecycle proves strict blocked obligation/tracking retention, compatibility preservation, verified recovery, and idempotent steady state; rich tombstones carry optional generations; and local live work uses durable generations, generation-checked committed state, cancellation fences, and one serialized live/plain/delete queue. The deterministic complete live delete/reinsert race and remote/multi-process fencing remain outstanding. |
| 5 | **✅ Answered for the governed source milestone** | Phase 3 requests and fake-service tests the available file, capability, inheritance, permission-detail, expiration, and limited-access fields. Missing inheritance origin, unresolved group membership, and principal visibility are explicit limitation codes. The documented live acceptance probe remains required for each real service-account/delegation configuration. |
| 6 | **✅ Partially answered with a safe implementation bound** | Phase 3 uses a versioned, StateStore-persisted descendant queue and processes at most 500 recomputations per batch before resuming. Production sizing and the threshold for a dedicated external graph remain measurement work, not a correctness gap in cursor ordering. |
| 7 | **Answered only for the current local boundaries** | The control-plane `StateStore` ledger retains its tested single-process, single-event-loop reconstruction boundary. Phase 6 adds transactional LMDB effect ordering and a durable local live-generation number, but it has not been certified with sudden power loss or process kill and does not provide connector-side CAS, cross-event-loop coordination, or a multi-process live lease. |

### 21.3 Product choices required before public beta

- Whether Synor ships full Qdrant query wrappers or only a filter/guard library.
  The recommendation is to ship a small guarded wrapper so the reference
  guarantee is testable end to end.
- Default evidence retention and operator export format.
- Supported Drive identity/delegation models.
- Initial commercial SLOs after benchmarks.
- Whether multi-process/multi-host control requires PostgreSQL or another
  transactional ledger before beta.
- Which legal/retention policy interface is public. Keep it application-defined
  and mechanically narrow.

These choices do not block Phases 0–4.

---

## 22. Release gates and definition of done

### 22.1 Developer preview

**Repository status (2026-07-29):** [x] Engineering gate satisfied for the
internal, service-free developer preview. This does not certify the real
Google Drive → Qdrant path and is not the alpha flagship claim.

May be labeled developer preview when:

- synthetic vertical slice passes all nine controlled Phase 2
  interruption/reconstruction points;
- internal model/ledger/suppression are stable;
- Qdrant broad exception swallowing is fixed;
- the guarantee is clearly opt-in and not public-marketed as complete.

### 22.2 Alpha flagship

**Repository status (2026-07-30):** [ ] The bounded local/public criteria are
satisfied, but the Google Drive → Qdrant alpha claim is not. The governed
source, certified target adapter, public controlled path, operator UX, and
service-free flagship are implemented. One real declarative Drive → Qdrant
application, trusted principal-context issuance, and live provider acceptance
remain open.

May be labeled alpha when:

- Google Drive governed identity, ACL revision, complete snapshot, and change
  replay pass;
- Qdrant point deletion and ACL update are query-verified;
- the guarded query path fails closed;
- the reference example demonstrates deletion, depermission, partial scan,
  eventual consistency, and receipts;
- open/blocked cases are operator-visible;
- evidence redaction tests pass;
- capability limits and bypasses are prominent.

Suggested alpha claim:

> For the documented Google Drive → Qdrant reference path, Synor suppresses
> retrieval when revocation is observed and verifies logical removal or policy
> replacement before closing the case.

### 22.3 Beta

**Repository status (2026-07-30):** [ ] Not satisfied. The bounded public
API/CLI and native local durability implementation exist. Multi-process
durability decisions, Phase 6 certification, drift/restore assurance, cache
recipients, measured SLOs, and external security review remain outstanding.

Requires:

- public APIs and CLI;
- durable multi-process strategy decided;
- drift/orphan scan;
- restore gate;
- at least one cache recipient;
- measured SLOs and alerting;
- upgrade/downgrade runbooks;
- external security review of identity, ACL normalization, query filtering,
  evidence, and target verification.

### 22.4 General availability

**Repository status (2026-07-30):** [ ] Not satisfied. Phases 3–6 have bounded
implementation milestones, but the open Phase 6 certification work, Phases
7–8, and every gate below must complete before a GA claim.

Requires all of:

- no known crash window can lose both tracking and cleanup obligation;
- provider-missing cleanup is blocked, not silently pruned;
- live generation fencing passes;
- two sources and two targets pass the conformance kit;
- target/version capability matrices are published;
- strict mode never silently downgrades;
- daily drift and restore-resurrection tests pass in reference deployment;
- performance gates pass at documented scale;
- incident, legal-hold, key-loss, ledger-corruption, source-token-expiry, and
  target-outage runbooks exist;
- no unresolved high-severity security review findings;
- all Python, Rust, formatting, pre-commit, documentation, migration, and
  compatibility checks pass.

### 22.5 Final feature definition

**Repository status (2026-07-30):** [ ] Not yet true as the complete
production/GA feature. The Phase 5 public runtime, operator UX, and local
flagship are complete, and the stable Drive-identity → Phase 2 runtime →
Qdrant adapter path is proven through service-free reconstruction and guarded
non-return tests. It is not yet wired and accepted as one live declarative
Google Drive → Qdrant `App`; native durability certification, cross-process
fencing, production drift/restore repair, and the remaining GA gates stay open. The
bounded native implementation exists, but its process-kill, migration,
live-race, distributed-fencing, scale, and performance certification does not.

Implementation is complete only when this statement is true:

> Given a trustworthy deletion/access observation for a supported source, a
> configured policy, a certified retrieval path, and supported target
> connectors, Synor durably suppresses the affected source generation,
> reconciles every registered owned derivative, reports any incomplete or
> unsupported obligation, verifies the required destination/query
> postcondition, survives retry/crash/restore without resurrection, and emits
> privacy-safe evidence.

---

## 23. Recommended pull-request sequence

Keep these PRs separate unless a repository constraint makes a pair
inseparable.

### 23.1 Implementation-scope progress

These statuses describe whether the planned source/test scope exists in the
current workspace. They do not claim that the work was published as twelve
separate pull requests.

| Planned PR scope | Status on 2026-07-29 | Notes |
|---|---|---|
| PR 1 — ADR and red tests | **Complete for the internal Phase 0 milestone** | ADR, synthetic contract tests, and the Qdrant exception regression are complete. The public strict disappeared-child E2E and native live-incarnation race are deferred to Phases 5–6. |
| PR 2 — Internal model and ledger | **Complete** | Internal identity, access, cases, receipts, StateStore ledger/repair, suppression, corruption handling, encryption, and tests are implemented. |
| PR 3 — Synthetic verified sink | **Complete for the internal synthetic scope** | Apply/verify, capability and action-descriptor contracts, false-success, eventual-consistency, retry, and reconstruction tests are implemented. |
| PR 4 — Retrieval guard | **Complete for the internal synthetic scope** | Tenant/principal-aware fail-closed guarding, pre-score and post-score policy checks, serving fences, generation tests, and measurements are implemented. |
| PR 5 — Google Drive stable governed snapshot | **✅ Complete in the current workspace** | Phase 3 stable identity, ACL resolution, snapshot authority, compatibility, docs, and fake-service tests are implemented. |
| PR 6 — Google Drive change feed | **✅ Complete in the current workspace** | Phase 3 user/shared-drive and drive-authority replay, ambiguity, cross-corpus moves, subtree entry/exit handling, descendant invalidation, expiry scheduling, semantic rescan fencing, readiness checkpointing, docs, and tests are implemented. |
| PR 7 — Qdrant strict target | **✅ Complete for the internal adapter scope** | Phase 4 governed lineage/content binding, Phase 2 state verifier, strict point/collection verification, ACL narrowing, durable operation-status receipts with operation IDs when exposed, compatibility tests, operator-gated live tests, docs, and bounded provider-batch benchmarks are implemented. Live execution, trusted principal-context issuance, public handler wiring, and end-to-end memory certification remain open. |
| PR 8 — End-to-end flagship example | **✅ Complete for the bounded local/reference scope** | The two-tenant service-free example runs twice through real controlled engine commits and demonstrates ACL-only revocation, suppression, delayed verification, partial-scan safety, restore non-resurrection, receipts, and content-free evidence. Optional real mode is a non-certified configuration probe; live acceptance remains open. |
| PR 9 — Public runtime/API/CLI | **✅ Complete for the bounded public scope** | Minimal public aliases, strict policy, startup repair/health, controlled finalization/recovery, conservative guarantee/status reporting, redacted versioned CLI, generated docs, and public compatibility tests are implemented. Automatic arbitrary-connector attestation is not claimed. |
| PR 10 — Native durability | **✅ Implemented for the bounded native scope; certification open** | Schema-v3/effect-record-v2 evidence lineages, verified-sink integration, write-free preview, strict provider blockers and fresh-process recovery, retained evidence, rich tombstones, tracking-owner cleanup, and local live queue/generation fencing are implemented. Kill/power-loss, copied-real-database, complete live-race, multi-process/remote fencing, scale, native concurrency, and overhead gates remain open. |
| PR 11 — Drift, cache, and restore | **Not started** | Phase 7. |
| PR 12 — Expansion and conformance kit | **Not started** | Phase 8. |

### PR 1 — ADR and red tests

- Add ADR-0003.
- Add synthetic false-success, partial snapshot, ACL memo, evidence, and live
  generation tests.
- Add Qdrant swallowed-exception regression.
- No production behavior except the narrow Qdrant exception fix if reviewers
  prefer it here.

### PR 2 — Internal model and ledger

- Add internal identity/access/event/case/receipt types.
- Add canonical hashing and state transition validation.
- Add StateStore ledger, suppression index, repair, encryption, and redaction
  tests.
- No public exports.

### PR 3 — Synthetic verified sink

- Add internal apply/verify wrapper.
- Add capability and safe action descriptor contracts.
- Complete false-success/eventual-consistency/restart tests using the in-memory
  target.
- Keep Rust/PyO3 unchanged.

### PR 4 — Retrieval guard

- Add suppression and tenant/access evaluation.
- Add pre-score guarded in-memory retriever.
- Add cross-tenant, stale policy, group revision, and cache tests.
- Establish performance baseline.

### PR 5 — Google Drive stable governed snapshot ✅

- Add file-ID identity, parent relationships, ACL resolver/digest, governed
  memo state, and snapshot completeness.
- Keep old `items()` compatible.
- Add fake-service pagination/partial/rename/duplicate/inheritance tests.

### PR 6 — Google Drive change feed ✅

- Add user/shared-drive tokens, tombstones, ambiguity, corpus moves, descendant
  invalidation, drive-authority events, strict subtree discovery, semantic
  rescan fencing, expiry scheduling, and crash-safe checkpointing.
- Add optional live acceptance test instructions.

### PR 7 — Qdrant strict target

- Finish exception hardening.
- Add governed payload, target-native suppression, wait/ordering, exact
  read-back, receipts, query filters, and fake/live tests.
- Record supported versions/capabilities.

### PR 8 — End-to-end flagship example

- [x] ✅ Add reproducible local mode and optional non-certified real
  Drive/Qdrant configuration probe.
- [x] ✅ Demonstrate depermission, partial scan, delayed consistency,
  privacy-safe evidence, and restore non-resurrection.
- [x] ✅ Add documentation of unsafe bypasses and the exact trust boundary.

### PR 9 — Public runtime/API/CLI

- [x] ✅ Promote only proven types.
- [x] ✅ Add `RevocationPolicy` to `SynorRuntime`.
- [x] ✅ Add report/manifest optional fields and CLI group.
- [x] ✅ Update explicit exports, CLI generated docs, programming guide, and
  tests.

### PR 10 — Native durability ADR amendment and implementation

- [x] Add effect/tombstone/generation/strict-provider safety using the smallest
  proven native seam.
- [x] Update PyO3 and `core.pyi` without changing the four-field reconcile
  tuple or exposing a public `App` strictness knob.
- [x] Add native lifecycle, schema/default/upgrade/cursor-integrity,
  drop-retention, strict legacy-sink, preview parity/purity, evidence
  redaction, Delete owner cleanup, fresh-process provider recovery, retry, and
  live transition-queue tests.
- [ ] Add process-kill/power-loss, copied real database, deterministic complete
  live-race, scale, performance, and dedicated native concurrency validation.

### PR 11 — Drift, cache, and restore

- Add external verifier/enumerator protocol.
- Add scanner, cache recipients, restore gate, metrics, and chaos tests.

### PR 12 — Second source/target and conformance kit

- PostgreSQL target plus localfs source/target are the recommended next pair.
- Publish capability schemas, test harness, runbooks, and beta gates.

For each PR:

1. State which invariant becomes true.
2. State which behavior remains intentionally unsupported.
3. Add failure tests before or with implementation.
4. Include migration/rollback notes.
5. Run the phase-appropriate commands from section 20.
6. Avoid unrelated refactors.

---

## 24. Start-development checklist

**Status:** All Phase 0–5 checklist items and the bounded Phase 6 native
implementation items are complete for their documented scopes. The next
unchecked handoff is Phase 6 durability/live-race/migration/performance
certification, followed by Phase 7 assurance work. The separate live Google
Drive → Qdrant alpha acceptance gate also remains open.

The first maintainer can begin with this exact sequence:

- [x] Read ADR-0001 and ADR-0002 again; draft ADR-0003 without changing their
  accepted compatibility boundaries silently.
- [x] Create `python/tests/revocation/`.
- [x] Extend the in-memory target harness with an independently observable
  external store and a false-success mode.
- [x] Write the false-success deletion regression first.
- [x] Write complete-versus-partial snapshot tests.
- [x] Write governed ACL-only memo invalidation test.
- [x] Write source-ID rename/duplicate-name test.
- [x] Write evidence sensitive-sentinel test.
- [x] Fix Qdrant collection deletion to accept only verified not-found or a
  literal successful result.
- [x] Implement internal versioned `SourceIdentity` and canonical ACL digest.
- [x] Implement internal case/receipt state machine.
- [x] Implement immutable-event StateStore ledger and projection repair.
- [x] Implement monotonic suppression generations.
- [x] Implement verified sink wrapper over the existing sink API.
- [x] Make verification failure raise before engine final commit.
- [x] Add guarded in-memory retrieval and prove immediate denial.
- [x] Add controlled interruption/reconstruction coverage at all nine Phase 2
  ordering boundaries.
- [x] Review Phase 2 evidence before exposing any new public type.
- [x] ✅ Complete the Phase 3 governed Google Drive source milestone.
- [x] ✅ Complete the bounded Phase 4 internal certified-Qdrant-adapter
  milestone.
- [x] ✅ Complete the bounded Phase 5 public controlled API, operator
  workflow, and flagship reference product.
- [x] ✅ Add Phase 6 native effect intent/finalization, verified-sink
  integration, strict provider blockers, retained evidence, rich tombstones,
  tracking-owner cleanup, write-free preview, fresh-process provider recovery,
  evidence lineages, and local live queue/generation fencing.
- [ ] **Next Phase 6 gate:** run process-kill/power-loss injection, a copied
  real pre-feature database drill, the complete live delete/reinsert race, the
  million-action correlation run, compatibility-overhead benchmarks, and
  dedicated concurrent-writer validation.
- [ ] **Distributed safety:** add connector-side generation/CAS enforcement or
  an explicit multi-process lease before claiming remote or multi-process live
  fencing.

Do not start by:

- changing every connector;
- modifying the four-field reconcile tuple;
- changing current `FileLike` memo serialization globally;
- rewriting target ownership;
- adding CLI/public exports before the internal slice works;
- promising universal IAM evaluation or physical erasure.

---

## 25. Confirmed facts, assumptions, and unknowns

### 25.1 Confirmed repository facts

- The engine reconciles undeclared target states to deletion.
- Sink failures preserve retryable uncertainty.
- A legacy sink's normal return is trusted without generic read-back. The
  additive verified-sink capability declares query verification and supplies a
  safe per-action descriptor.
- Final deletion commit prunes normal tracking/owner state.
- Qdrant collection deletion now treats only provider-confirmed REST `404` or
  gRPC `NOT_FOUND` as idempotent absence, requires literal `True` after a
  normal return, and propagates all other outcomes. Compatibility
  `CollectionTarget` point deletion still has no certified negative read-back
  contract; the additive `CertifiedQdrantTarget` supplies guarded exact-ID
  negative read-back for its internal strict path.
- `TargetReconcileOutput` is a four-field Python tuple unpacked by PyO3.
- Target sinks still return child handlers or `None`; native effect
  descriptors use a separate capability rather than changing that return
  shape.
- One target state has one current component owner.
- normal root update can tolerate/retry orphan cleanup failure without raising;
  drop is stricter;
- a missing target provider can still skip cleanup in compatibility mode;
  strict effect mode persists a blocked obligation and retains tracking;
- current child existence/tombstones have compatible optional generation and
  controlled metadata fields; empty legacy tombstones decode conservatively;
- compatibility `GoogleDriveSource.items()` still has no ACL/change feed and
  keys by name/path; the additive governed source supplies stable Drive-ID
  identity, ACL snapshots, authoritative inventory, and change replay;
- current `FileLike` memo state does not include access policy;
- native inspection exposes effect status counts and completed native evidence
  survives tracking reduction, while external-state verification remains the
  certified sink's responsibility;
- current control-plane `StateStore` is async and pluggable but not a
  multi-key transactional protocol.

### 25.2 Assumptions adopted for planning

- The first certified deployment can require `SynorRuntime` strict mode and a
  guarded query integration.
- The local-first single-process, single-event-loop ledger is sufficient for
  the first vertical slice.
- Qdrant exact-ID read-back can establish the initial logical-absence
  postcondition under a documented consistency profile.
- Drive file ID is stable enough within the configured source scope to own a
  component.
- Source-scope loss of access authorizes suppression and removal of derivatives
  created under that connector identity, while evidence remains honest about
  the ambiguity.
- Legal/retention decisions are injected by the application.

### 25.3 Unknowns that require a spike or deployment evidence

- Exact multi-replica Qdrant behavior and latency for every intended topology.
- Drive effective-permission visibility across service-account, delegated-user,
  shared-drive, limited-access, and enterprise configurations.
- Scale of inherited ACL descendant invalidation in real customer drives.
- Required connector-side CAS/app-lease design for multi-process live fencing.
- Which multi-process ledger backend customers will require first.
- Commercial SLO values after source/target rate-limit testing.
- Physical-erasure evidence possible for each future target.

Unknowns must appear in capability output and release notes. They must not be
silently converted into “supported.”

---

## 26. Primary references

### Repository

- `AGENTS.md`
- `docs/architecture/ADR-0001-phase-2-execution-control.md`
- `docs/architecture/ADR-0002-phase-3-trustworthy-local-execution.md`
- `docs/architecture/ADR-0003-provable-index-revocation.md`
- `docs/architecture/revocation-phase2-validation.md`
- `docs/architecture/revocation-phase6-validation.md`
- `docs/src/content/docs/programming_guide/target_state.mdx`
- `docs/src/content/docs/programming_guide/trustworthy_execution.mdx`
- `docs/src/content/docs/programming_guide/controlled_runs.mdx`
- `docs/src/content/docs/programming_guide/live_mode.mdx`
- `docs/src/content/docs/advanced_topics/custom_target_connector.mdx`
- `docs/src/content/docs/advanced_topics/live_component.mdx`

### Security and risk guidance

- [OWASP RAG Security Cheat
  Sheet](https://cheatsheetseries.owasp.org/cheatsheets/RAG_Security_Cheat_Sheet.html)
- [OWASP LLM08:2025 — Vector and Embedding
  Weaknesses](https://genai.owasp.org/llmrisk/llm082025-vector-and-embedding-weaknesses/)
- [NIST AI RMF Generative AI
  Profile](https://doi.org/10.6028/NIST.AI.600-1)

### Source-system behavior

- [Google Drive: track changes for users and shared
  drives](https://developers.google.com/workspace/drive/api/guides/about-changes)
- [Google Drive `changes.list`
  reference](https://developers.google.com/workspace/drive/api/reference/rest/v3/changes/list)
- [Google Drive permissions
  resource](https://developers.google.com/workspace/drive/api/reference/rest/v3/permissions)
- [Microsoft Graph drive-item
  delta](https://learn.microsoft.com/en-us/graph/api/driveitem-delta?view=graph-rest-1.0)
- [Microsoft Graph external
  groups](https://learn.microsoft.com/en-us/graph/connecting-external-content-external-groups)

### Destination behavior and market baseline

- [Qdrant point deletion
  API](https://api.qdrant.tech/api-reference/points/delete-points)
- [Qdrant consistency
  guarantees](https://qdrant.tech/documentation/scaling/consistency-guarantees/)
- [Pinecone delete
  records](https://docs.pinecone.io/guides/manage-data/delete-data)
- [Pinecone data
  freshness](https://docs.pinecone.io/guides/index-data/check-data-freshness)
- [Elasticsearch delete by
  query](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html)
- [Amazon Kendra user-context
  filtering](https://docs.aws.amazon.com/kendra/latest/dg/create-index-access-control.html)
- [Amazon Kendra batch
  deletion](https://docs.aws.amazon.com/kendra/latest/dg/delete-batch-documents.html)
- [Google Agent Search data-source access
  control](https://docs.cloud.google.com/generative-ai-app-builder/docs/data-source-access-control)
- [Microsoft Graph external-item
  ACL](https://learn.microsoft.com/en-us/graph/api/resources/externalconnectors-acl?view=graph-rest-1.0)
- [AWS S3 Object
  Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
- [Unstructured Elasticsearch source and `record_id`
  behavior](https://docs.unstructured.io/api-reference/workflow/sources/elasticsearch)

### Legal/standards inputs

- [GDPR Articles 17–19, official
  text](https://eur-lex.europa.eu/eli/reg/2016/679/oj/eng)
- [EU AI Act, official
  text](https://eur-lex.europa.eu/eli/reg/2024/1689/oj/eng)
- [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html)

---

## 27. One-sentence engineering compass

When a developer faces a tradeoff not covered here, use this rule:

> Preserve Synor's declarative ownership and retry semantics, but never let
> uncertain source absence authorize destructive truth, never let an
> unverified target response erase the last cleanup obligation, and never let
> slow cleanup keep revoked content retrievable.
