# ADR-0002: Trustworthy local execution control plane

- Status: accepted
- Date: 2026-07-27

## Context

Synor's Rust engine and LMDB database are the authoritative implementation of
incremental reconciliation. Existing applications depend on their stable-path,
memoization, and target-state semantics. Phase 3 adds encrypted evidence,
pluggable stores, provenance, replay verification, PII controls, quarantine,
pipeline packages, and a local dashboard without changing those semantics.

The new records can contain sensitive operational metadata such as filesystem
paths, component ownership, or detected PII categories. They need a narrower
trust boundary than ordinary logs. Arbitrary Python pipeline code, native
libraries, subprocesses, and remote services cannot be made deterministic or
fully isolated by a Python library.

## Decision

Phase 3 is an opt-in Python control plane around the existing `App` API:

1. The native engine remains authoritative for incremental state. Its LMDB
   schema and connector protocol are unchanged.
2. `StateStore` is an async protocol for control-plane records. Synor provides
   filesystem and in-memory stores. Applications can inject another
   implementation without registering global plugins.
3. `EncryptedStateStore` wraps any `StateStore`. It uses AES-256-GCM with a
   fresh nonce per value and HMAC-SHA-256 opaque physical keys. Keys come from
   the caller or `SYNOR_STATE_KEY`; Synor never writes encryption keys to disk.
4. Artifact provenance records target-state identity, owning component,
   run/package/code digests, and timestamps. It does not claim byte-level
   lineage when a connector does not expose output bytes.
5. Replay captures a canonical preview digest, application target, source-code
   and installed direct-dependency digests, versions, policy, and options.
   Replay reruns preview and verifies the captured evidence. It does not
   automatically apply changes.
6. PII enforcement is explicit and policy-aware. Built-in detectors cover
   common structured identifiers. Findings contain category, count, and a
   one-way evidence digest, never the matched value. Policy-aware model
   operations enforce the active policy before egress.
7. Failed controlled runs and PII quarantine decisions create metadata-only
   quarantine cases. Review changes case status only; it never retries or
   applies a pipeline implicitly.
8. Pipeline lockfiles capture source hashes and exact installed Python
   distribution versions without resolving or downloading dependencies.
   Deterministic `.synor` ZIP packages contain selected source files, the
   lockfile, and a package manifest. `.env`, databases, outputs, and hidden
   directories are excluded.
9. The dashboard is a read-only HTTP server bound to `127.0.0.1` by default. It
   displays local run evidence, provenance, and quarantine metadata. Binding to
   a non-loopback address requires an explicit unsafe flag.

## Failure and recovery

- Atomic filesystem writes use a temporary sibling followed by `os.replace`.
- AES-GCM authentication failures are reported as corrupt or wrongly keyed
  state; ciphertext is never partially returned.
- Control-plane failures do not mutate native state. An explicitly configured
  store failure is surfaced to the controlled API.
- Run failure quarantine contains only exception type and safe identifiers, not
  exception messages or payloads.
- Replay mismatch reports which digest differed and performs no target action.
- Lock verification rejects modified source and dependency-version drift.
  Package creation rejects a stale lock; package verification rejects unsafe,
  duplicate, missing, unindexed, or digest-mismatched archive entries.

## Compatibility and rollout

Existing `App.update()`, CLI update/drop behavior, Rust/PyO3 APIs, connectors,
and LMDB files remain supported. Phase 3 APIs are additive. Existing Phase 2
manifests remain readable; new optional fields use a schema-compatible default.
Removing Phase 3 configuration returns execution to Phase 2 behavior.

## Accepted limits

- Native LMDB pages are not encrypted by this layer. Use an encrypted volume if
  native engine state itself must be encrypted at rest.
- Dashboard-compatible evidence under `.synor/runs` remains redacted plaintext
  JSON. Put `SYNOR_AUDIT_DIR` on an encrypted volume when it also requires
  encryption at rest.
- Regex and checksum detectors are not a complete data-loss-prevention system.
- Preview can execute arbitrary user Python and therefore is not a side-effect
  sandbox.
- Replay verifies captured local evidence; it cannot force nondeterministic
  external systems, clocks, random generators, native code, or subprocesses to
  reproduce results.
- The local dashboard is not an authenticated multi-user service.

## Alternatives rejected

- Replacing LMDB with a Python store abstraction: too risky for compatibility,
  atomic reconciliation, and performance.
- Home-grown encryption: rejected in favor of audited AES-GCM primitives.
- Capturing raw inputs/outputs for replay: rejected because it expands the
  sensitive-data boundary and conflicts with metadata-only auditing.
- Automatically retrying reviewed quarantine cases: rejected because review
  must not trigger external side effects.
