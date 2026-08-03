# ADR-0003: Evidence-backed index revocation

- Status: Accepted for the bounded Phase 0–6 implementation; production/GA
  certification gates remain open
- Date: 2026-07-29
- Native durability amendment: 2026-07-30

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

The public `App.update()` default, existing connector APIs, and the four-field
`TargetReconcileOutput` remain unchanged. Existing sinks default to legacy
acknowledgement semantics. The native schema is additive to each app's existing
LMDB database, and a pre-feature database with no native schema marker remains
valid.

The stronger guarantee is an additive, opt-in controlled path.
`SynorRuntime` selects an internal strict effect mode through the private
`App._update_controlled()` boundary; direct `App.update()` remains in
compatibility mode. `App.drop()` now refuses to erase an app while any native
effect is not `completed`. It otherwise removes operational state while
retaining native effect evidence and its schema marker.

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

Phase 6 adds a separate LMDB-native effect lifecycle for engine reconciliation.
This does not replace the control-plane ledger above and does not broaden the
`FileStateStore` claim. LMDB mutations continue to run through
`Storage::run_txn` and its single-writer batcher.

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

### Phase 6 native effect durability amendment

#### Additive keyspace and schema

Each existing per-app LMDB database can now contain:

- one `DbEntryKey::NativeSchemaVersion` singleton at top-level tag `0x38`,
  whose current value is schema version 4;
- a `DbEntryKey::NativeEffect` evidence keyspace under top-level tag `0x40`,
  keyed by a domain-separated fingerprint of an engine-owned evidence ID;
- a `DbEntryKey::NativeEffectObligation` allocation-cursor keyspace under
  top-level tag `0x48`, keyed by opaque tracking locator and source
  generation; and
- a `DbEntryKey::NativeEffectLineage` cursor keyspace under top-level tag
  `0x50`, keyed by opaque tracking locator; and
- one `DbEntryKey::NativeObligationSummary` singleton at top-level tag `0x58`,
  containing transactionally maintained unresolved-effect status totals and
  the count of query-verified cleanup tombstones.

Native effect record version 2 stores the safe descriptor, engine evidence ID,
opaque tracking fingerprint, verification policy, controlled cause and error
codes, timestamps, attempt count, and
`pending|verified|failed|blocked|completed` status. It stores no target payload,
source text, raw locator, principal, credential, remote response, or free-form
exception.

The connector-supplied descriptor action ID remains the operation and Phase 2
receipt-correlation ID. It is not the native evidence primary key. The engine
allocates an evidence ID from the opaque tracking locator and a monotonically
increasing lineage epoch. An unresolved retry must reproduce the exact proof
contract and reuses that evidence ID. A later lifecycle after completion gets
a successor evidence ID, so retained evidence remains immutable even if a
connector repeats an action ID. Version-1 effect records have no separate
evidence field and decode conservatively with their descriptor action ID as
the legacy evidence ID.

The first native effect write to an untouched database installs schema version
4 in the same transaction. Existing schema-v1 and schema-v2 databases remain
readable; their next native write performs one bounded evidence scan, creates
ordinary per-locator lineage cursors, builds the obligation summary, and
installs the v4 marker atomically. Existing schema-v3 databases validate their
cursors, scan retained evidence and tombstones once, and install the summary
and v4 marker atomically. A crash exposes either the complete old schema or the
complete v4 summary. Successful strict-run completion thereafter checks the
transactional summary instead of scanning all retained evidence.
A missing marker is the pre-feature compatibility state only when the effect,
obligation, and lineage keyspaces are all empty. Native reads/writes, strict
completion checks, inspection, and protected drop refuse a future schema or
native metadata without a marker. Schema validation also verifies both cursor
key bindings and their referenced evidence contract/status, so corrupted
lineage or obligation cursors fail closed.

A legacy compatibility update makes no native-schema or revocation guarantee.
The supported-version upgrade path has synthetic v1 coverage, and the
corrupt-cursor cases are tested. A migration drill using a copied real
pre-feature database remains a release gate.

Bounded-token validation is not a PII classifier. Certified host profiles
remain responsible for supplying opaque, non-reversible action IDs; the native
layer can enforce shape and digest fields but cannot prove that an otherwise
valid alphanumeric token does not encode sensitive data.

This is a lazy, one-way activation boundary. A genuinely older binary cannot
recognize a schema invented after that binary shipped, so this release cannot
make such a binary refuse it retroactively. Operators must not downgrade an
activated app database in place. A schema-aware current or future binary must
retain read, retry, inspection, and export capability even when creation of
new strict effects is disabled.

#### Verified sink capability

`TargetActionSink` now has an internal assurance capability and an optional
safe descriptor extractor. Existing sinks are `Legacy` and need no migration.
The Phase 2 `VerifiedTargetActionSink` registers
`query_verified` assurance and extracts one descriptor per action through the
PyO3 bridge. Descriptor parsing accepts only controlled operations, a bounded
safe action ID, a positive source generation, and lowercase SHA-256 source and
target-locator digests. Conversion failures cross the bridge as a fixed,
redacted error.

The four-field `TargetReconcileOutput` remains unchanged. A strict destructive
cleanup is rejected before apply if its sink does not provide both a valid
descriptor and query-verified assurance. A verified sink may also produce
native evidence during a compatibility update; strictness is never inferred
from the mere existence of that evidence.

The Rust-owned verified-action carrier binds the descriptor used by native
planning to the same action consumed by the Python verified wrapper. Python
subclassing, rewrapping, class-method replacement, or mutation of compatibility
attributes cannot substitute a second descriptor after native validation.

#### Preview semantics

Controlled preview executes the same reconciliation and native proof-contract
planning as a real update but returns no precommit write plan. It does not
write target tracking, schema markers, evidence, lineage/obligation cursors, or
provider-ID allocation; it also does not call target apply, verify, or record
callbacks. Flat leaf actions are returned as their original Python objects.

Strict preview rejects the same invalid new verified action without a recovery
tracking record and the same changed unresolved proof contract as execution.
If strict cleanup needs a missing provider, preview fails without creating the
blocker that a real update would persist. Repeated previews remain write-free,
and a following real update applies the effect once. Preview currently rejects
child-provider actions and every live-component mount rather than presenting a
partial plan.

#### Effect ordering

For every described native effect, the engine uses this order:

1. In the component precommit transaction, write or reopen the `pending`
   effect together with pending tracking and ownership state.
2. Apply the sink batch in the original flattened action order. A verified
   sink may return normally only after its wrapper has established every
   required postcondition and recorded its Phase 2 receipt.
3. In a separate batched LMDB transaction, move the associated native effects
   to `verified`. A sink error records a controlled `failed` state and clears
   the component stage marker, leaving tracking retryable.
4. In the final component tracking transaction, atomically move `verified`
   effects to `completed`, resolve any recovered provider blocker, and commit
   the normal tracking reduction.

Consequently, a final-commit failure leaves a verified effect and retryable
tracking instead of a falsely completed effect. Completed effect records are
not part of normal target tracking and survive tracking reduction and ordinary
app drop.

This ordering closes the application-level window that previously could lose
both tracking and the native obligation. The implementation has transaction
and retry tests; it has not yet been certified with `SIGKILL` or sudden
power-loss injection at every boundary.

#### Missing providers and drop safety

In strict effect mode, an orphaned target whose provider is unavailable gets a
deterministic opaque `blocked` cleanup record in the precommit transaction.
The engine advances the retained tracking record to the current version and
fails the strict root update. When the provider returns, only a
query-verified cleanup can resolve that blocker in the final tracking
transaction. `App.drop()` and the storage-level app drop both abort before
mutation while any native effect remains `pending`, `verified`, `failed`, or
`blocked`.

Compatibility mode deliberately preserves the pre-existing missing-provider
behavior. It therefore carries no governed-cleanup guarantee. Provider removal
is safe only through the strict controlled path or an operator workflow that
first closes every obligation.

#### Tombstones and live generations

Child existence records now optionally carry a live generation. New child
tombstones are versioned metadata records with a controlled cause, optional
source digest and generation, creation time, attempt count, safe last-error
code, and verification policy. An empty legacy tombstone decodes as
`cause=undeclared`, `verification=legacy_unverified`, unknown generation, and
zero/empty metadata. Conditional deletion prevents cleanup for an older known
generation from deleting a newer tombstone.

These are value-compatible extensions at the existing stable-path entry tags:
child existence remains `0xa0`, and child tombstones remain `0xb0`. Live
generation allocation uses the existing ID sequencer with the reserved stable
key `synor/_internal/live_component_generation`; it does not add a second
counter store.

Each live-component incarnation reserves a monotonic generation from the
app's durable LMDB ID sequencer. The generation and cancellation fence are
propagated through live child work. The existence row naming that generation
is durable before `process_live` can access committed state. Live committed
state reads validate the generation, and writes compare and write in one
batched transaction.

Incremental update, nested live mount, and delete share the same per-subpath
coalescing queue. The latest queued operation wins, `update_full` gates the
queue while it derives authoritative state, and live-to-plain or live-to-delete
transitions drain the exact old incarnation before the successor operation.
A bounded drain timeout fails the transition without installing a successor
or writing a delete tombstone.

Native submit checks the inherited cancellation fence before precommit, after
precommit, before each sink, and before final commit. A prior incarnation that
does not drain within the bounded handoff timeout causes successor installation
to fail. After live descendants quiesce, strict root completion also surfaces
latched post-readiness task failures and unresolved native/tombstone
obligations rather than reducing them to log-only success.

This fence protects competing incarnations in one running engine. It is not a
remote target compare-and-swap token, an app-wide multi-process lease, or a
distributed fencing protocol. A process that already crossed a remote
mutation boundary cannot be recalled without connector-side generation/CAS
support.

#### Inspection

The Rust and Python inspection boundaries expose status counts only:
`pending`, `verified`, `failed`, `blocked`, and `completed`, for a live app or
an app opened by name. They do not expose effect descriptors, locators, or
target payloads. CLI integration and retention policy remain later work.

## Non-claims

This feature is not:

- model-weight unlearning, training-data erasure, or backup destruction;
- a legal determination or a claim of GDPR or other regulatory compliance;
- proof about targets, caches, exports, or query paths not registered and
  certified with Synor;
- protection against a malicious connector that forges read-back or a fully
  compromised destination administrator;
- a distributed remote-effect transaction, connector-side CAS protocol, or
  multi-process live-component lease;
- `SIGKILL`, kernel-panic, sudden-power-loss, or storage-controller durability
  certification for the Phase 6 ordering;
- evidence that the one-million-action correlation benchmark or compatibility
  overhead benchmark has passed;
- a mechanism by which a binary released before native schema version 3 can
  discover and refuse that future schema. Schema-v3-aware binaries do reject
  current schema v4 as a future version.

Direct vector-client queries that bypass the supported retrieval guard are
outside the query-denial guarantee.

## Consequences

The design adds control-plane records and target read-back latency to the
strict path. Existing pipelines using legacy sinks incur no native effect
writes or verification calls. Verified sinks add native effect transitions
even when invoked through compatibility mode, but compatibility mode makes no
revocation claim. Strict integrations must declare factual capabilities and
provide safe action descriptors; arbitrary field-name heuristics are
insufficient for destructive security semantics.

The Phase 2 internal vertical slice proved false-success retry, eventual
consistency, immediate query denial, evidence repair, and legal-hold isolation
without a Rust/PyO3 change. Phase 6 adds the native seam described above while
preserving that Python contract. Governed Google Drive and the internal
certified Qdrant adapter exist, but live end-to-end provider acceptance remains
an operator-gated milestone.

Completed native effects are retained indefinitely in this milestone. That is
the fail-safe default for evidence, but it makes retention, export, compaction,
and long-running keyspace growth explicit follow-up work before GA.

## Rollback

Disable selection of the internal strict runtime and creation of new strict
effects. Existing compatibility `App.update()` behavior remains available.
Leave `revocation/v1/`, the native schema marker, native effects, and blocked
tombstones in place for inspection and retry. Keep suppression active until a
version-aware recovery operation verifies a newer authorization.

Do not open an app database whose native keyspace has been activated or
upgraded to schema version 3 or 4 with a binary that predates that schema.
No automatic downgrade/export tool is implemented in this milestone. Rollback
must never delete native evidence or suppression state to make the older
release start.
