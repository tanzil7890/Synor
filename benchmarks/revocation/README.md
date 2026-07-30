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

The certified Qdrant path has a separate bounded-batch planning benchmark for
the Phase 4 launch sizes:

```bash
uv run python benchmarks/revocation/qdrant_benchmark.py
```

It covers 1, 100, 10,000, and 100,000 actions by default and reports the
maximum actions materialized in any verification batch. Like the framework
benchmark above, it excludes remote Qdrant latency; deployment SLOs require
the gated live suite against the production-equivalent replica topology.
