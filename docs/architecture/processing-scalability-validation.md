# Processing scalability validation record

- Date: 2026-08-02
- Platform: arm64 macOS 26.5.1, CPython 3.11.15
- Scope: Python `map_stream()` scheduler and result-retention bound

This record is a reproducible, service-free scheduler smoke benchmark. It runs
one million trivial async item calls over a synchronous `range`, consumes each
completion into a constant-size checksum, and retains no caller-side result
list. The expected count and checksum guard against incomplete benchmark runs
without introducing an O(n) validation structure.

```bash
/usr/bin/time -l uv run python benchmarks/map_stream.py \
  --items 1000000 \
  --max-in-flight 64
```

The initial 2026-08-02 run used the same benchmark body inline with
`uv run python -c` and produced:

| Measurement | Result |
|---|---:|
| Items | 1,000,000 |
| Checksum | 499,999,500,000 |
| Wall time, including `uv` and interpreter startup | 3.42 seconds |
| Maximum RSS reported by macOS | 53,313,536 bytes |
| Peak memory footprint reported by macOS | 20,038,088 bytes |

After checking in the command above, an immediate rerun of that script
reported 3.317415 seconds inside the benchmark (301,439 items/second), 3.50
seconds from `/usr/bin/time`, 53,657,600 bytes maximum RSS, and a 20,021,704
byte peak memory footprint. The two observations establish the local baseline;
they are not timing assertions in the automated suite.

The helper reserves a slot before pulling from either a synchronous or
asynchronous input. That slot is released only when the corresponding result is
yielded, so running tasks and buffered results together cannot exceed
`max_in_flight`. Focused tests cover completion-order output, exact input
backpressure, early close, caller cancellation, worker failure, a blocked async
input pull, and an iterator that catches cancellation during a failure race:

```bash
uv run pytest -q python/tests/core/test_map_backpressure.py
```

This is a local scheduler baseline, not a connector-throughput SLO. Real
pipelines additionally pay for user work, serialization, reconciliation, and
destination I/O. The RSS value includes the Python interpreter and imported
Synor extension; compare future runs on the same platform and toolchain.
