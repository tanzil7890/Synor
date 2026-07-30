# Provable Index Revocation Phase 6 validation record

- Date: 2026-07-30
- Scope: bounded native implementation milestone
- Platform: arm64, macOS 26.5.1, CPython 3.11.15
- Rust toolchain: rustc/cargo 1.89.0
- Public reconcile tuple change: none
- Public `App.update()` strictness option: none
- Native schema: v3 (`0x38` marker, `0x40` evidence, `0x48` obligation
  cursors, `0x50` lineage cursors)
- Native effect record: v2 (connector action ID and engine evidence ID are
  separate)

This record accompanies
`provable-index-revocation-implementation-plan.md`. It validates the native
source and focused integration tests listed below. It is not production/GA
certification and does not convert any unchecked Phase 6 gate into a supported
claim.

## Recorded scoped validation

The PyO3 extension was rebuilt before the focused Python runs. These are
scoped Phase 2/6 results, not a repository-wide test result.

```bash
cargo test
cargo test -p synor_core --lib

uv run maturin develop
uv run pytest -q \
  python/tests/core/test_native_effect_inspection.py \
  python/tests/revocation/test_verified_sink.py \
  python/tests/revocation/test_native_provider_recovery.py \
  python/tests/test_execution.py

uv run pytest -q python/tests/revocation/
uv run pytest -q python/tests/core/test_live_component.py
uv run pytest python/

uv run python examples/provable_index_revocation/main.py
```

| Validation | Result |
|---|---|
| Cargo workspace tests | Passed |
| `synor_core` library unit tests | 103 passed |
| PyO3 development build | Passed |
| Native inspection, verified-sink, provider-recovery, and controlled-runtime selection | 66 passed on the recorded rebuilt snapshot |
| Full revocation package | 172 passed on the recorded rebuilt snapshot |
| Full live-component module | 37 passed |
| Final Python repository suite after the last compatibility adjustment | 1,412 passed, 241 skipped |
| Service-free flagship smoke run | Completed with a closed case, 2 receipts, blocked partial snapshot, zero suppressed scoring, and 3 unaffected results |
| Whitespace/error-marker check for the documentation diff | Passed |

The 172-test revocation run was recorded immediately before a final minor
compatibility-path source adjustment. The final 1,412-pass repository run
covered the post-adjustment source. Operator/integration-dependent skips remain
skips; neither result converts an unchecked certification gate below.

The Rust run includes schema-v3 key encoding and supported-v1 upgrade,
effect-record-v2 action/evidence identity, ordinary lineage epoch allocation,
provider-obligation allocation, future-schema refusal, descriptor validation,
safe legacy defaults, native effect transition/finalization, cursor-integrity
failure, blocked recovery, metadata-only inspection, generation-aware
tombstone cleanup, retained effect evidence, non-mutating unresolved-effect
drop, coalesced-writer rejection isolation, and existing core compatibility
tests.

The focused Python run includes aggregate-only inspection, four-field
reconcile compatibility, strict legacy-sink rejection, descriptor redaction,
verified retry behavior, connector action correlation against one
Rust-bound descriptor, engine evidence lineage, strict controlled-runtime
selection, Delete-mode owner cleanup, interrupted control-plane finalization
recovery, and a fresh-process provider-missing lifecycle. That lifecycle
retains the strict blocker and tracking while the provider is absent, preserves
them across a compatibility retry, resolves both only after verified recovery,
and is idempotent on the next fresh process.

Strict preview coverage proves that original actions are returned without
apply/verify/record callbacks or native/target-state writes; repeated previews
remain empty; the following real update applies once; and an unresolved proof
contract change or new verified action with no recovery tracking record is
rejected in both planning and execution. Live preview rejects live mounts
without stable-path or target-state mutation. Live→plain and plain→live tests
exercise the shared latest-operation-wins queue and old-incarnation drain.

The serialized LMDB privacy test plants action payload, principal, credential,
raw-locator, and remote-error sentinels and finds none of them in native
storage. It does not substitute for a separate planted-sentinel scan of a rich
tombstone value.

The flagship's fake target is a reference-product smoke test, not a native
declarative-target certification. Its in-memory target/controller mutations
remain application-owned; native target-effect proof is exercised by the
engine/verified-sink tests above.

Before Phase 6 integration, the Phase 2 baseline was re-run separately:

| Baseline | Result |
|---|---|
| `uv run pytest python/tests/revocation/ -q` | 155 passed, 2 operator-gated skips |
| Phase 2-focused selection | 101 passed |
| Service-free flagship example from the repository environment | Completed |

Those baseline results, together with the later scoped revocation rerun above,
revalidate Phase 2's internal synthetic milestone. They do not establish
native crash, multi-process, or performance certification.

## Open certification gates

The following were not established by the commands above and remain unchecked
in the implementation plan:

- `SIGKILL`, kernel panic, sudden power loss, or storage-controller failure
  injection at native precommit/apply/verification/final-commit boundaries;
- connector-side generation/CAS enforcement or an app-wide multi-process
  lease;
- a destructive child-component process-kill lifecycle;
- a planted-sentinel serialization scan of a rich tombstone value;
- a deterministic complete live delete/reinsert race;
- migration using a copied real pre-feature app database;
- a one-million-action correlation run;
- compatibility-overhead and strict native performance benchmarks;
- a dedicated concurrent-writer native lifecycle stress test;
- downgrade/export tooling for an app database activated or upgraded at
  native schema version 3;
- completed-effect retention, export, and compaction policy.
