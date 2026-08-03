# Reliability and scalability hardening status

This document is the release gate for the engine hardening work. It separates
implemented invariants from compatibility bridges and from work that still
requires a storage-format or connector-specific design. A green unit test is
not treated as proof of a guarantee unless the corresponding failure or race is
injected at the boundary where it can occur.

## Current status

| Area | Status | Enforced invariant | Remaining release work |
|---|---|---|---|
| Background readiness | Implemented | Readiness is `Succeeded`, `Failed`, `Cancelled`, or `Superseded`; reporting callbacks cannot convert failure into success | Keep connector source commits gated on `Succeeded` |
| Kafka and Iggy source acknowledgement | Implemented | Source progress is acknowledged only after downstream durable success; Kafka uses an exact synchronous broker commit and validates the reply | Run broker-backed rebalance and restart tests in connector CI |
| Local filesystem containment | Implemented | Managed child mutations reject anchored/traversal/ambiguous paths and use no-follow, descriptor- or handle-pinned I/O | Keep Linux, macOS, and Windows filesystem race tests in the platform matrix |
| Python callback cancellation | Implemented | Task creation and cancellation are linearized; public update cancellation waits for descendant and callback cleanup | Keep cancellation-before-install, repeated cancellation, stale-handle, and cross-app isolation regressions |
| Environment restart | Implemented | Environment generations are monotonic; cached core apps are rebound; stop closes admission and drains work before lifespan resources | Stress concurrent start/stop/update in CI |
| LMDB resize | Implemented | Every transaction-opening path adopts `MapResized`; resize is coordinated within and across processes | Keep the two-process resize reproducer and compact-copy coverage |
| Strict completion lookup | Implemented for schema v4 | Current-schema unresolved native-effect checks use transactional counters/indexes and are O(1) in retained history | Benchmark the one-time migration from older schemas at million-effect scale |
| Kafka and Iggy source admission | Foundation implemented | A private 256-item window is held through the contiguous offset frontier; Kafka pauses assignments but continues heartbeat polls while full | Add payload-byte admission, public pressure telemetry/tuning, and live-broker saturation tests |
| General working-set bounds | Foundation implemented | Component admission, declaration ceilings, sink queue item/byte accounting, bounded map admission, and `map_stream()` backpressure are explicit | Reconciliation planning and tracking persistence are still O(component state); see below |
| Connector capability contract | Contract implemented; certification partial | All 39 built-in sinks are inventoried with machine-readable atomicity, replay, ordering, cancellation, verification, and batch semantics; 2 currently have partially certified positive claims | Expand real failure-injection certification beyond the currently evidenced Kafka and SQLite guarantees |

## Compatibility decisions

The hardening changes preserve existing APIs where doing so does not weaken a
correctness boundary:

- `SpawnHandle.ready()` remains success-or-raise. `outcome()` is additive and
  provides the typed terminal state.
- Existing `map()` remains an unbounded, input-ordered full-fan-in primitive.
  `map_bounded()` adds bounded admission while retaining an O(n) result list;
  `map_stream()` adds completion-order consumption with O(limit) retained
  framework state.
- LocalFS keeps its documented nested relative path feature. Backslashes,
  absolute/anchored paths, `..`, empty or `.` segments, NUL bytes, and symlink
  traversal are rejected on every platform. Windows additionally rejects its
  control bytes, forbidden characters, trailing-dot/space aliases, and device
  names. Nested components are traversed one pinned directory at a time instead
  of being joined and trusted as one pathname.
- A legacy sink without replay-safe segmentation still receives one component
  action set. Explicit connector batch limits are hard failures; internal
  packing thresholds do not silently split a sink that has not promised safe
  replay.

## Scalability boundary that remains

Sink segmentation now bounds individual apply calls and no longer builds a
second O(total actions) size vector. It does not make the complete
reconciliation path O(page size). Before the first external apply, the engine
still materializes component declarations, previous tracking state, grouped
actions, native-effect intents, ownership data, and the tracking write plan.

True bounded million-item reconciliation needs a schema-level protocol:

1. Store tracking state in ordered pages rather than one component blob.
2. Persist a durable reconciliation generation and action journal.
3. Commit an idempotent segment, its external completion evidence, and its
   resume cursor in a defined order.
4. Resume or compensate after process death without confusing an old
   generation with the current component owner.
5. Finalize ownership and delete stale pages only after all segments are
   acknowledged.

That work must be introduced as a migration with crash points at every fence;
turning the current in-memory vectors into chunks without a durable cursor would
only reduce call size while making partial failure ambiguous.

## Evidence and reproducibility

- Native-effect million-history and O(1) strict-check measurements are recorded
  in [revocation-phase6-validation.md](revocation-phase6-validation.md).
- The one-million-item bounded map command, checksum, timing, and memory
  observations are recorded in
  [processing-scalability-validation.md](processing-scalability-validation.md).
- Connector capability inventory and evidence paths are machine-readable in
  `dev/target-sink-certification.json` and enforced by the common certification
  tests.

## Promotion gates

Before describing the whole system as million-item bounded or every connector
as certified, require all of the following:

- A crash-restart test for each durable reconciliation fence.
- A million-item reconciliation benchmark that records peak RSS, LMDB growth,
  no-op completion latency, and changed-item latency.
- A million-effect old-schema migration benchmark, not only a current-schema
  lookup benchmark.
- A simultaneous two-process `MapFull` contention stress test in addition to
  the existing cross-process resize/adoption reproducer.
- Connector-specific failure injection for every positive capability claim,
  including acknowledgement loss, cancellation during I/O, replay after a
  committed prefix, and ordering where promised.
- The full Python/Rust test matrix on Linux, macOS, and Windows, plus the docs
  build, type checks, formatting checks, and generated-file checks.
