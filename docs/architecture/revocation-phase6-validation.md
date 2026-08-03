# Provable Index Revocation Phase 6 validation record

> **Schema-v4 amendment (2026-08-02):** The current binary upgrades native
> schema v1-v3 apps to v4 by installing a transactionally maintained `0x58`
> obligation summary. Strict no-op completion now reads that summary in
> constant time. The v3 million-effect timings later in this document are the
> preserved pre-summary baseline, not current-schema performance results.

- Date: 2026-07-30
- Scope: locally certifiable native milestone
- Platform: arm64, macOS 26.5.1, CPython 3.11.15
- Rust toolchain: rustc/cargo 1.89.0
- Public reconcile tuple change: none
- Public `App.update()` strictness option: none
- Native schema at the time of the recorded run: v3 (`0x38` marker, `0x40`
  evidence, `0x48` obligation cursors, `0x50` lineage cursors); current: v4,
  adding the `0x58` obligation summary
- Native effect record: v2

This record accompanies
`provable-index-revocation-implementation-plan.md`. It certifies the bounded
local implementation and records exact non-claims. It is not a production/GA
claim.

## Final repository validation

The PyO3 extension was rebuilt after the final Rust change.

```bash
uv run maturin develop
cargo test --quiet
uv run pytest -qq python/
cargo check -p synor_core --benches

uv run --group format ruff format --check \
  python/synor/cli.py \
  python/tests/cli/test_native_effect_admin_cli.py \
  python/tests/core/test_pre_native_fixture.py \
  python/tests/core/test_live_component.py \
  python/tests/connectors/test_qdrant_revocation.py \
  benchmarks/revocation/compatibility_benchmark.py \
  benchmarks/revocation/scale_certification.py

uv run mypy
```

| Validation | Result |
|---|---|
| PyO3 development build | Passed |
| Cargo workspace | Passed |
| `synor_core` | 115 passed, 1 intentionally ignored scale test |
| Native benchmark target | Passed `cargo check` |
| Focused native/live/Qdrant/provider Python selection | 150 passed, 2 live-Qdrant skips |
| CLI command tree and operator tests | 74 passed |
| Full Python repository | 1,420 passed, 241 integration-dependent skips |
| Full `prek run --all-files` | Functional hooks passed; repository baselines below prevent a clean all-hooks result |

The repository-wide Ruff-format and `cargo fmt --all -- --check` commands
still reproduce formatting differences in unrelated files on `origin/main`
under the current formatter versions. The changed Python files pass focused
format checks. Ruff check and mypy also retain documented baseline findings in
unchanged files; changed-file findings are resolved.

Strict `cargo clippy -p synor_core --all-targets -- -D warnings` is not a
clean repository gate on this source base. It first fails on the existing
`write_with_newline` finding in `rust/utils/src/error.rs:245`; a no-dependency
run exposes the existing `synor_core` warning backlog. This change does not
rewrite unrelated modules to establish a new Clippy baseline.

The full hook run passed case/merge/symlink/private-key checks, `uv-lock`,
`maturin develop`, Cargo workspace tests, SDK-without-PyO3 validation, the
Python suite, and CLI documentation generation. It reproduced eight mypy
errors in four unchanged files. Its Rust external-example check also reported
that the refreshed AWS dependency set requires Rust 1.91.1 while this
repository run uses Rust 1.89.0. Mutating all-files hooks exposed pre-existing
end-of-file and formatter drift; their unrelated rewrites were not retained in
this feature diff.

## Process-crash matrix

The native crash test launches a real child process against a copied database,
waits for a durable phase marker, then sends `SIGKILL`. No destructor or
application cleanup runs in the child.

| Kill boundary | Reopen invariant |
|---|---|
| After precommit | Pending native evidence and retry tracking survive |
| After external apply | Retryable uncertainty survives; replay is idempotent |
| After verification | Verified evidence survives until final tracking commit |
| During final commit | LMDB exposes the complete commit or the pre-commit state |

Every case reopens, converges, and retains one immutable completed evidence
lineage without losing both tracking and cleanup obligation. A separate child
holds the OS-backed app/environment lease and is killed; another process
cannot acquire it before death and can acquire it afterward.

This certifies process death at the tested boundaries. It does not certify
sudden physical power loss, kernel panic, filesystem firmware behavior, or
storage-controller failure.

## Writer and race certification

The native single-writer stress submits 4,096 concurrent effect lifecycles
through `Storage::run_txn`. All complete through the repository's LMDB
single-writer batcher.

The deterministic live test pauses an old delete inside its verified-effect
boundary, queues a same-subpath replacement, and proves the replacement cannot
start until delete apply, verification, evidence recording, and native
finalization finish. Final state contains only the replacement.

The Qdrant test pauses an old generation after remote delete apply, reinserts a
new authorized generation with the same deterministic point ID, then resumes
the old verifier. The stale attempt fails, its retry is generation-filtered,
and the new point remains servable.

Every update mode and app drop acquires an OS-backed exclusive per-app lease
while holding a shared environment lease. The downgrade snapshot acquires the
environment lease exclusively, so both existing and newly starting app
operations are fenced across the copy boundary. This relies on validated
filesystem lock semantics for all participating hosts.

## Migration and downgrade

The fixtures were generated from pre-native revision
`63df53f605a552547cc016ef879d7cdf582e76e8` and copied into the repository.

| Fixture | SHA-256 |
|---|---|
| 4 KiB page `data.mdb` | `fcfdae440098563ee91939e77b535971034969d554a8a477d543fb61d20554bb` |
| 16 KiB page `data.mdb` | `2153128f58e1b5ce2c667c8da86d963373cd8a2216c049c8d8ca281d21a25048` |

Rust opens each copied fixture, runs the compatibility lifecycle, lazily
activates the current native schema, and continues through drop and update.
Python runs the public compatibility lifecycle against the host-compatible
copy. Separate migration coverage starts from a populated v3 app and verifies
the atomic v4 summary rebuild.

`synor native-effects prepare-downgrade`:

1. resolves source, staging, archive, and output path boundaries;
2. excludes app operations with the environment lease;
3. creates an LMDB-consistent compacted copy;
4. refuses any unresolved effect or child tombstone;
5. strips only native metadata from the copy;
6. writes and fsyncs a metadata archive, with mode `0600` enforced on POSIX;
7. writes a readiness manifest with the archive hash;
8. publishes the copy only after the archive exists.

The source remains unchanged. A wheel built from the pre-native revision ran
`update -> drop -> update` against the prepared copy successfully.

## Retention and operator tooling

The default completed-effect retention is indefinite. Export is read-only.
Compaction requires an explicit UTC cutoff and confirmation, publishes a
private versioned archive before mutation, and submits the exact archived
candidate IDs. One LMDB transaction revalidates the records and protects every
unresolved record, zero/unknown timestamp, ordinary lineage head, and cleanup
obligation head. Repetition is idempotent.

Focused tests cover:

- archive schema, canonical hash, metadata-only fields, and POSIX mode `0600`;
- rejection of archives inside the source database;
- archive-before-compaction ordering and exact candidate selection;
- cursor-head retention and already-absent retries;
- symlink-safe staging validation;
- unresolved-effect and all-tombstone downgrade refusal;
- archive/readiness publication before output rename;
- preservation of non-native operational state in the copy.

The deployment and rollback procedure is in
`native-effect-operations-runbook.md`.

## Privacy certification

Serialized LMDB scans plant source content, action payload, principal,
credential, raw locator, and remote-error sentinels. Neither native effect
records nor rich child tombstones contain those values. Native state contains
only bounded IDs, digests, opaque fingerprints, controlled enums, timestamps,
attempt counts, and controlled error codes.

Archives use the same metadata-only boundary. Bounded identifiers are not a
PII classifier, so archives are created privately and remain sensitive
operational evidence.

## Million-action certification

The fast Phase 2 correlation harness:

```bash
uv run python benchmarks/revocation/scale_certification.py \
  --actions 1000000 \
  --batch-size 10000
```

| Measurement | Result |
|---|---|
| Actions | 1,000,000 |
| Batches | 100 |
| Applied/verified/recorded | 1,000,000 / 1,000,000 / 1,000,000 |
| Correlation SHA-256 | `f82e055f73778e363da8f860a737356dfda8cde01833bd867f1ab5062cc81259` |
| Elapsed | 6.217 seconds |
| Throughput | 160,847 actions/second |
| Maximum RSS reported by macOS | 65,323,008 bytes |

The real native lifecycle is intentionally ignored in ordinary CI and run
explicitly:

```bash
cargo test -p synor_core \
  million_action_descriptor_receipt_native_correlation \
  -- --ignored --nocapture
```

The preserved schema-v3 baseline persisted, verified, finalized, rescanned,
and recomputed correlations for 1,000,000 effects in 100 batches of 10,000. It
passed in 254.868 seconds and produced a 995,803,136-byte LMDB data file.

The schema-v4 implementation was rerun explicitly on 2026-08-02. This run
also completed all one million durable effect lifecycles and the exact
full-history correlation audit:

| Schema-v4 measurement | Result |
|---|---:|
| Complete lifecycle plus certification | 329.454 seconds |
| Strict no-op completion checks | 1,000 in 0.011830 seconds |
| Mean strict completion check | 0.000011830 seconds |
| Deliberate full retained-evidence scan | 64.687483 seconds |
| Completed summary count | 1,000,000 |
| LMDB data file | 994,721,792 bytes |

The strict path reads two fixed-size summary records, so its measured time is
independent of retained evidence count. Lifecycle construction, schema
migration, administrative evidence enumeration, and the deliberate audit
remain linear in the number of records; this result does not claim otherwise.

## Compatibility performance

Criterion compares the same operational LMDB write with native hooks inactive.

| Benchmark | 95% interval |
|---|---|
| Operational write control | 4.0232 to 4.0744 ms |
| Inactive native hooks | 4.0179 to 4.1227 ms |

The point-estimate overhead is 0.36%; intervals overlap.

A separate same-host, same-Python paired run compares 1,000 updates on the
pre-native wheel immediately followed by 1,000 updates on the current tree:

| Statistic | Current | Pre-native | Current overhead |
|---|---:|---:|---:|
| Mean | 11.3080 ms | 10.8007 ms | 4.70% |
| Median | 11.2559 ms | 10.8582 ms | 3.66% |
| p95 | 13.2826 ms | 12.5481 ms | 5.85% |

The documented local acceptance bound is less than 10% p95 overhead outside
strict mode, which this run satisfies. These measurements are a regression
baseline for this host, not a universal latency promise.

## Remaining external gates

The locally certifiable Phase 6 checklist is complete. The following claims
remain open:

- sudden physical power loss, kernel panic, filesystem/firmware failure, and
  storage-controller fault injection on supported deployment filesystems;
- connector-specific multi-host generation/CAS conformance beyond the
  certified Qdrant path;
- operator-gated live Qdrant single-node and replicated-cluster acceptance;
- the later drift, restore, cache-recipient, second-connector, conformance-kit,
  security-review, and GA gates in Phases 7 and 8.
