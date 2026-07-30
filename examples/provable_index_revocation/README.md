# Provable index revocation

This is Synor’s service-free flagship demonstration of one narrow security
guarantee:

> After a governed access-revocation observation, the supported retrieval path
> suppresses the affected source generation before physical cleanup and keeps
> it suppressed until target absence has been verified.

The default mode needs no account, model download, database server, or network
access. It uses two tenants, two principals, deterministic chunks and vectors,
a deliberately stale first verification read, and the public governance,
revocation, retrieval, and controlled-runtime APIs.

## Trust boundary

The guarantee covers queries made through `RetrievalGuard` and mutations
coordinated by the controller owned by a strict `SynorRuntime`. The runtime
performs startup ledger health and repair checks before the scenario reads or
writes governed state. Its report records `strict_revocation_control_v1`, not a
blanket assertion that arbitrary application code used every governed
boundary. The control store contains source, tenant, policy, target-locator,
and principal **digests**, plus controlled status values. It does not contain
document bytes, emails, or display names.

The local fake index intentionally retains document text, just as a real vector
index commonly retains chunk payloads. That target is untrusted after
revocation; the retrieval guard is the serving boundary while deletion
converges.

An unmanaged direct target query bypasses that boundary and is unsupported:

```python
# UNSAFE: bypasses Synor suppression, tenant policy, and generation checks.
client.query_points(collection_name="documents", query=vector)
```

Use `CertifiedQdrantTarget.query_points(...)` with a query context derived by
trusted upstream authentication and policy code in real deployments.
Constructing `CertifiedQueryContext` does not authenticate a principal.
Restrict direct Qdrant credentials so application code cannot silently choose
the unsafe path.

The example proves logical non-return through the supported path. It does not
claim physical erasure from Qdrant segments, replicas, snapshots, or backups;
multi-process or power-loss durability; cache-recipient invalidation; or
automatic cleanup of externally restored data.

## Run the local scenario

From this directory:

```bash
python main.py
```

The program performs two real `SynorRuntime.run()` engine commits in one async
event loop:

1. Index one document for tenant alpha and an unaffected document for tenant
   beta, then verify each principal sees only its own tenant.
2. Change tenant alpha’s ACL without changing one source byte, install
   suppression, prove its chunks are not scored, observe one stale target read,
   retry, verify absence, and record a receipt.

The deliberately blocked partial-scan case makes the controlled report
`degraded`; the demo never disguises it as ordinary success. It then proves:

- a partial source snapshot cannot trigger mass deletion;
- restoring the old physical points does not make them retrievable;
- replaying the same controlled case is idempotent;
- evidence contains none of the source-content or principal sentinels.

Without configuration, evidence is stored below `.synor/control`. To make the
ignored local defaults explicit, copy the template first:

```bash
cp .env.example .env
```

Inspect the resulting control state with:

```bash
synor revocations list --json
synor revocations show <case-id> --json
synor revocations repair-ledger
```

`list` and `show` are read-only. A retry is an explicit external mutation and
requires an exact active suppression plus a registered target provider;
closing an operator ticket never lifts suppression. Configure `verify` and
`scan` operators with provider-enforced read-only credentials—the generic CLI
can prove its own control bytes stayed unchanged, not what arbitrary provider
code did.

## Optional Google Drive and Qdrant configuration

Real mode validates configuration and builds a
`GovernedGoogleDriveSource`/`CertifiedQdrantTarget` pair without running a
source scan or target mutation:

```bash
cp .env.example .env
# Fill the commented GOOGLE_DRIVE_*, QDRANT_*, and SYNOR_* values.
pip install -e '.[real]'
SYNOR_REVOCATION_DEMO_MODE=real python main.py
```

This is an operator-gated configuration probe, not live certification. Before
using it as a deployment acceptance test, provision Qdrant’s required payload
indexes, run the connector capability preflight, use a disposable Drive and
collection, exercise actual permission removal, and confirm the documented
version/topology constraints. The probe prints the declared static target
capability profile and digest; retain `await target.capability_report()` output
from the live environment for deployment evidence.
The real-mode factory configures a finite 30-second Qdrant client transport
timeout; the adapter's operation-timeout parameter is not itself a transport
deadline.

## Scenario coverage

| Safety property | Local assertion |
|---|---|
| Stable identity | Source and chunk IDs are derived deterministically. |
| ACL-only observation | Content bytes, content digest, and source revision stay identical. |
| Immediate denial | Revoked candidates are removed before scoring. |
| Eventual consistency | First target verification is stale and leaves the case open. |
| Verified close | Retry observes absence and produces an immutable receipt. |
| Partial scan | The case is blocked and no indexed point is deleted. |
| Restore safety | Restored generation-one points remain suppressed. |
| Privacy-safe evidence | Raw content and principal sentinels are absent from every control-store value. |
