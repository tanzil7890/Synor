# Revocation strict-path latency benchmark

This microbenchmark records compatibility apply latency separately from the
internal strict sink's descriptor, verification-correlation, and recorder
overhead:

```bash
uv run python benchmarks/revocation/benchmark.py --actions 100 --rounds 100
```

It deliberately uses an in-memory one-read success path. The strict number is
framework overhead, not a target SLO: real latency additionally includes
destination apply, consistency-fence polling, negative read-back, and durable
receipt I/O.

The compatibility sample calls the same apply callback directly. The Phase 2
implementation is not referenced by normal `App.update()` or existing target
handlers, so it adds no branch, state write, import, or verification call to
that path. Use the existing `benchmarks/state_store` suite for end-to-end
compatibility regression tracking across commits.

Phase 6 also includes a public compatibility-update benchmark and an isolated
Rust commit benchmark:

```bash
uv run python benchmarks/revocation/compatibility_benchmark.py \
  --rounds 1000 --warmup 100

cargo bench -p synor_core --bench native_compatibility
```

Run the Python command with the same script, hardware, Python version, and
round counts against both the pre-native wheel and the candidate wheel. Run
the samples back-to-back on an otherwise idle host and report the paired
result rather than selecting the fastest sample. It performs one ordinary
legacy target action per update. The Rust benchmark compares an operational
LMDB commit with the same commit routed through the empty native
precommit/finalization hooks. Neither benchmark exercises strict effects or
remote I/O.

The Phase 6 scale certification uses bounded batches for one million
descriptor-to-receipt correlations, then runs the ignored Rust persistence
test for one million correlated native effects:

```bash
uv run python benchmarks/revocation/scale_certification.py \
  --actions 1000000 --batch-size 10000

cargo test -p synor_core \
  million_action_descriptor_receipt_native_correlation \
  -- --ignored --nocapture
```

The Python half exercises out-of-order verifier results and normalized Phase 2
receipt outcomes. The Rust half persists, verifies, finalizes, and scans every
native effect in bounded batches. Both compute the same descriptor-domain
correlation shape. The Rust test is ignored in ordinary CI because it creates
one million retained evidence and lineage records.

The certified Qdrant path has a separate bounded-batch planning benchmark for
the Phase 4 launch sizes:

```bash
uv run python benchmarks/revocation/qdrant_benchmark.py
```

It covers 1, 100, 10,000, and 100,000 actions by default and reports the
maximum actions materialized in any verification batch. Like the framework
benchmark above, it excludes remote Qdrant latency; deployment SLOs require
the gated live suite against the production-equivalent replica topology.
