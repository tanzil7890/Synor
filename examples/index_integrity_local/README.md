# Local index integrity scan

This service-free example exercises the experimental read-only
`synor.integrity` API with deterministic metadata fixtures. It reports one
healthy source, one missing target, one stale target, and one orphan target.
No provider account, network access, content bytes, or write credential is
used.

From the repository root:

```bash
uv run maturin develop
uv run python examples/index_integrity_local/main.py
```

The JSON report contains only keyed identifiers and controlled finding codes.
The example uses a fixed test-only report key so its output is reproducible;
production configurations must load a private key of at least 32 bytes from a
secret manager or protected local environment.

Real S3-to-Qdrant scans use `S3IntegrityInspector` and
`QdrantIntegrityInspector`. Live listings are explicitly best-effort unless an
operator supplies a consistent inventory/snapshot token. The scanners use only
S3 `ListObjectsV2` and Qdrant `scroll`; they never read S3 bodies, vectors, or
user payloads and never call a provider mutation method.
