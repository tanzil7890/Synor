# Provable Index Revocation Phase 2 validation record

- Date: 2026-07-29
- Scope: internal Phase 0–2 contracts and synthetic strict vertical slice
- Platform for recorded latency samples: arm64, macOS 26.5.1, CPython 3.12.13
- Platform for final validation: arm64, macOS, CPython 3.11.15
- Public API change: none
- Rust/PyO3 change: none

This record accompanies
`provable-index-revocation-implementation-plan.md`. It is evidence for the
internal milestone, not certification of Google Drive, Qdrant consistency, a
customer retrieval deployment, physical erasure, regulatory compliance, or
cross-event-loop/multi-process durability.

## Compatibility baseline

Command:

```bash
uv run --project benchmarks/state_store \
  python benchmarks/state_store/runner.py --n 100 --m 0 --format json
```

Observed once on the platform above:

| Pipeline | Cold | Warm | Drop |
|---|---:|---:|---:|
| Existing non-governed pipeline, 100 memoized components, zero targets | 2.258 s | 0.175 s | 0.150 s |

The strict implementation is not imported, selected, or called by ordinary
`App.update()` or existing target handlers. Consequently the compatibility
path has no new condition, control-plane state write, target read-back, or
receipt write. The baseline is retained for comparison by later phases that
integrate a public runtime or real connectors.

## Strict framework latency

Reproducible command:

```bash
uv run python benchmarks/revocation/benchmark.py \
  --actions 100 --rounds 100
```

Observed:

| Measurement | Median | p95 |
|---|---:|---:|
| Direct compatibility apply callback | 0.000166 ms/batch | 0.000292 ms/batch |
| Strict descriptor + apply + verify correlation + recorder callback | 0.382 ms/batch | 0.536 ms/batch |
| Strict framework overhead per action | 3.82 µs | 5.36 µs |

This is intentionally an in-process, one-read-success microbenchmark. It
excludes remote apply latency, eventual-consistency polling, negative
read-back, network variance, and durable receipt I/O. Those values must be
measured and published separately for each certified connector/version and
topology.

## Security and recovery assertions

The Phase 2 suite covers:

- versioned, collision-resistant source, observation, case, action, and receipt
  identity;
- ACL-only and group/access revision memo invalidation with unchanged content;
- complete versus partial source snapshot authority;
- event-first case persistence and repair after interrupted summary writes;
- receipt hash links plus a durable count/tip anchor, including missing-root,
  missing-tail, divergent-head, renamed-key, and orphan-record detection;
- receipt operation/policy-decision coupling, canonical failure codes, and
  terminal-evidence enforcement while reading, listing, or repairing cases in
  successful terminal stages;
- shared single-event-loop writer serialization for adapters using the same
  `StateStore` facade;
- deterministic fail-closed rejection if that facade is reused from another
  event loop;
- monotonic suppression bound to tenant, policy identity/revision, group
  revision, and derivative generation;
- a trusted reauthorization-callback conformance scenario that is idempotent,
  admits only the replacement generation, and fences delayed destructive
  actions; remote positive authorization verification remains Phase 3/4 work;
- encrypted and plaintext control-plane stores;
- capability preflight before strict apply;
- durable action identity bound to verifier, consistency, and exact capability
  profile so changed restart configuration cannot rewrite proof semantics;
- false-success apply followed by negative read-back failure and engine retry;
- bounded verification retry, timeout, unsupported, wrong-ACL, present, and
  transport outcomes;
- evidence-recorder failure preventing engine final commit;
- exact, non-subclassed action descriptors plus redacted apply, descriptor,
  verifier, recorder, model, ledger, receipt-head, suppression, and
  serving-fence failures with no secret-bearing exception cause/context chain;
- tenant, principal, current policy, group-graph, and suppression enforcement
  before scoring;
- a fresh policy lookup during post-scoring authorization, including a policy
  change that lands while an asynchronous scorer is blocked;
- synchronous process-local denial before the first await plus a full-metadata
  durable serving fence written before observation persistence and consulted
  after `StateStore` facade reconstruction;
- retention of that durable fence across interrupted writes and
  equal-generation conflicts, with release only by exact matching suppression
  or a strictly newer generation;
- suppression before policy/provider/capability/snapshot blocking for typed
  revocations, while source events that are not revocations remain
  unsuppressed;
- safe replay of old terminal cases without clearing an unresolved
  equal-generation revocation fence;
- metadata-only errors and receipts with planted content, email, vector, token,
  and remote-error sentinels;
- legal-hold decisions derived from persisted governed access state, verified
  isolation, provider-missing blocking, and crash/restart convergence in the
  synthetic runtime.

## Durability boundary

The local adapter is recoverable for ordinary single-process interruptions on
one event loop. Writers must share both that event loop and the same
`StateStore` facade to share its writer lock. Cross-loop, multi-process, and
multi-host writers require a transactional/fenced backend. `FileStateStore`
uses atomic replacement but does not `fsync` the file and parent directory, so
these results do not establish sudden-power-loss durability. The receipt
hash/head structure detects accidental local loss/reordering under the retained
anchor; it is not a digital signature and does not defend against a fully
compromised store administrator.

The synchronous emergency overlay is process-local, but every accepted
revocation attempt writes a metadata-only durable serving fence before its
observation event. A fresh facade consults that fence and denies the old
authorized generation even before ledger recovery begins. Recovery remains
necessary for liveness and cleanup convergence; unresolved or corrupt fences
stay fail-closed. The marker protocol is not a substitute for transactional
multi-process fencing.

## Recorded final validation

The final Phase 0–2 source state was validated with:

```bash
uv run ruff format --check \
  python/synor/_internal/revocation_*.py \
  python/synor/_internal/retrieval_guard.py \
  python/synor/_internal/state_store_lock.py \
  python/synor/_internal/suppression.py \
  python/synor/_internal/verified_sink.py \
  python/synor/connectors/qdrant/_target.py \
  python/tests/revocation \
  python/tests/connectors/test_qdrant_target.py \
  benchmarks/revocation
uv run ruff check <the same paths>
uv run mypy
uv run pytest -q \
  python/tests/revocation \
  python/tests/connectors/test_qdrant_target.py
uv run pytest python/
cargo test
```

Recorded results:

| Validation | Result |
|---|---|
| Phase 0–2 scoped Ruff format and lint | Passed |
| Mypy | Passed, 323 source files |
| Revocation plus Qdrant regression suite | 195 passed, 1 live-service test skipped |
| Full Python suite | 1,244 passed, 249 skipped, 2 existing warnings |
| Rust unit, integration, and doc tests | Passed; one existing `dead_code` warning in `rust/py/src/runtime.rs` |

After pytest completed, the interpreter also reported one pre-existing
unclosed-loopback-socket `ResourceWarning`; it did not fail the suite and is
unrelated to the Phase 0–2 paths.

### Repository-wide Ruff baseline

The workspace-wide Ruff gate is not clean independently of this milestone:

- `uv run ruff format --check .` reports 15 pre-existing generated/plugin or
  example files that would be reformatted, with 327 files already formatted.
- `uv run ruff check .` reports 60 pre-existing findings across examples,
  connectors, and existing tests.

No Phase 0–2 implementation or test file appears in those failures. The scoped
Ruff commands above pass. This record does not silently reformat or repair
unrelated user-owned/generated files.
