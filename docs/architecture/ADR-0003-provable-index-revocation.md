# ADR-0003: Evidence-backed index revocation

- Status: Accepted for the internal Phase 0–2 implementation
- Date: 2026-07-29

## Context

Synor's declarative engine can remove target states when their owning component
or declaration disappears. That is necessary, but it is not a complete
revocation guarantee. A source may become inaccessible without reporting a
physical deletion; an ACL can change without content changing; a target can
acknowledge a mutation before it becomes query-visible; and ordinary target
tracking is intentionally removed after successful cleanup.

The product problem is therefore:

> Deleted, expired, moved, de-permissioned, or otherwise unauthorized source
> content must stop being served immediately, converge to the configured
> target postcondition, and leave privacy-safe evidence that the transition was
> verified.

The detailed threat model, research, and rollout plan are recorded in
`provable-index-revocation-implementation-plan.md`.

## Decision

### Compatibility boundary

Existing `App.update()`, `App.drop()`, connector APIs, LMDB data, and the
four-field `TargetReconcileOutput` remain unchanged. Compatibility mode keeps
its current acknowledgement semantics.

The stronger guarantee is an additive, opt-in controlled path. Phases 0–2 keep
all new contracts under `synor._internal`; no public type or configuration
surface is committed before the synthetic lifecycle passes.

### Vocabulary

- A **source identity** is the tuple of connector instance, source scope, and
  immutable provider item ID. Names, paths, URLs, and display labels are not
  identity.
- A **revocation** is a policy decision to destroy, restrict, isolate, or
  investigate a derivative after a typed source/access event.
- **Suppression** is the immediate fail-closed serving decision while slower
  target propagation is pending.
- **Acknowledgement** means a target accepted or applied an operation.
- **Verification** is a target-specific read/query observation that the
  required postcondition holds.
- A **receipt** is metadata-only evidence of one transition or verification.

### Five-plane architecture

1. The observation plane emits stable identity, content and access revisions,
   typed events, cursors, and explicit snapshot completeness.
2. The decision/control plane creates a deterministic case and durable,
   monotonic serving suppression before cleanup.
3. The serving plane requires tenant and authenticated-principal context,
   current policy and group-graph revisions, and suppression checks before
   scoring or returning content.
4. The materialization plane continues to use Synor's existing stable
   component ownership and target reconciliation.
5. The assurance plane applies idempotently, waits for a bounded consistency
   contract, verifies the postcondition, writes a receipt, and only then lets
   the existing sink return normally.

### Identity and change detection

Canonical source identities and deterministic case/action/receipt identifiers
use versioned, length-delimited encodings. ACL digests normalize and sort
semantic entries while preserving grants versus denies and direct versus
inherited rules.

Strict source items add access policy, group graph, expiry, legal state, and
typed event data to memo state. An ACL-only change therefore invalidates
governed processing even when content bytes do not change.

Only a complete authoritative snapshot may authorize missing-item cleanup or
checkpoint advancement. Partial, failed, or permission-gapped scans retain
previously known ownership.

### Durability and recovery

The initial local ledger is a single-process, single-event-loop adapter over
the existing async `StateStore`. Adapters on that event loop share a writer
lock when they share the same store facade. It does not claim multi-key
transactions, cross-event-loop or multi-process compare-and-swap, or
power-loss durability:

1. one event-loop-bound async lock serializes writers;
2. an immutable deterministic event/receipt is written first;
3. the mutable case summary is written second;
4. the immutable stream is the source of truth;
5. retry is idempotent and `repair()` reconstructs interrupted summaries and
   receipt-head writes after an ordinary process crash.

`FileStateStore` uses atomic temporary-file replacement but does not currently
`fsync` both file and parent directory. Phase 0–2 recovery claims therefore
cover ordinary single-process interruption on one event loop, not kernel
panic, sudden power loss, storage-controller failure, or concurrent writers
on other event loops or in other processes.
A transactional/fenced backend or an explicitly reviewed `fsync` policy is
required before broadening that durability claim.

Suppression uses monotonic source generations bound to tenant, policy identity
and revision, and group-graph revision. A synchronous in-process overlay closes
the interval before the first await. Before the observation event is persisted,
the runtime also writes a full-metadata pending fence under
`revocation/v1/serving_fences/`; guarded reads consult it after process or
store-facade reconstruction. The fence is removed only after the exact
same-generation suppressed record is durable or a strictly newer generation
supersedes it. Equal-generation authorization and replay of an older terminal
case cannot clear an unresolved revocation.

Retrieval candidates carry the source generation that materialized them; an
older derivative cannot become retrievable merely because a later
authorization reuses the same policy labels. Restoration never deletes the
main suppression record; it requires a trusted callback for a newer
authorization generation. The callback is a connector trust boundary in Phase
2—real source and target verification is required in Phases 3–4. An encrypted
`StateStore` can protect values and logical keys without changing the
revocation schema.

The case also persists governed legal state and derives hold behavior from it;
a caller cannot select destructive cleanup for an observation marked
`legal_hold`. Durable action identity binds the verifier, consistency contract,
and exact capability profile, preventing a restart from silently changing the
proof contract attached to an existing case.

### Assurance levels

- `unverified`: intent or dispatch evidence only;
- `acknowledged`: the destination accepted/applied the operation;
- `query_verified`: the certified query/read path established the
  postcondition;
- `erasure_attested`: a destination supplied a supported physical-erasure
  attestation;
- `retained_isolated`: bytes were intentionally preserved but verified outside
  the live serving path.

Strict query-verified mode never treats `unsupported`, `present`, `wrong_acl`,
timeout, transport failure, or evidence-write failure as success. The sink
raises, so Synor retains retryable tracking.

### Evidence boundary

General receipts and errors contain only schema versions, opaque IDs or
digests, controlled operation/outcome codes, counts, timestamps, consistency
contract names, and hash-chain links. They exclude source text, chunks,
vectors, raw principal/display values, tokens, credentials, reversible target
locators, exception messages, and remote response bodies.

The receipt hash chain detects missing or reordered local evidence. It is not a
digital signature and does not protect against a fully compromised state-store
administrator.

## Non-claims

This feature is not:

- model-weight unlearning, training-data erasure, or backup destruction;
- a legal determination or a claim of GDPR or other regulatory compliance;
- proof about targets, caches, exports, or query paths not registered and
  certified with Synor;
- protection against a malicious connector that forges read-back or a fully
  compromised destination administrator;
- a distributed, cross-event-loop, or multi-process transaction protocol in
  Phases 0–2.

Direct vector-client queries that bypass the supported retrieval guard are
outside the query-denial guarantee.

## Consequences

The design adds control-plane records and target read-back latency only to the
strict path. Normal pipelines incur no new state writes or verification calls.
Strict integrations must declare factual capabilities and provide safe action
descriptors; arbitrary field-name heuristics are insufficient for destructive
security semantics.

The internal vertical slice can prove false-success retry, eventual
consistency, immediate query denial, evidence repair, and legal-hold isolation
without a Rust/PyO3 change. Real source and target certification remains a
later milestone.

## Rollback

Disable selection of the internal strict runtime. Existing `App.update()`
behavior remains available. Leave `revocation/v1/` evidence in place for
inspection, and keep suppression active until a version-aware recovery
operation verifies a newer authorization. Rollback must never restore serving
by deleting suppression state.
