# ADR-0001: Execution control, egress policy, and run evidence

Date: 2026-07-27

Status: Accepted for the `0.1.0a1` development line

## Context

Synor already has a stable execution core:

- `App.update()` runs or resumes the declarative component tree.
- `preview=True` performs reconciliation without applying target actions or
  committing tracking state.
- the inspection API reads stable paths and target-state ownership from LMDB.
- the CLI loads existing `App` objects and calls those APIs directly.

Phase 2 needs a safer and more inspectable experience without replacing that
engine or forcing existing applications to migrate.

## Decision drivers

1. Existing `App`, connector, and `synor update` behavior must remain compatible.
2. Planning must use the same reconciliation logic as a real run.
3. Offline mode must apply to user code as well as Synor-owned HTTP helpers.
4. Audit evidence must not become a second store of prompts, rows, or secrets.
5. Local models must work without a service, account, or network request.
6. The new API should compose existing internals instead of exposing them.

## Decision

### Compatibility boundary

The existing `App` API remains authoritative. A new `SynorRuntime` facade calls
`App.update()` and `App.update(preview=True)` internally, then returns typed
execution reports. Existing applications do not need to use the facade.

The CLI adopts the same facade utilities for policy and run evidence while
retaining the old commands and flags.

### Planning and diffing

`synor plan` and `synor diff` both use the engine's preview mode.

- `plan` presents an operational summary.
- `diff` presents redacted, deterministic action shapes.

Neither command applies target actions or commits component tracking state.
Payload bytes are represented by length and digest. Fields with secret-like
names are redacted.

Python target handlers currently expose their connector-specific action value,
not a universal create/update/delete enum. The new layer therefore calls these
items “planned changes” unless the action itself provides a safe operation
label. This avoids guessing.

### Egress policy

`EgressPolicy` evaluates destination, purpose, data classification, and optional
byte count. Decisions are explicit values and denied decisions raise
`PolicyViolation`.

During a controlled run, `policy_scope()` guards standard Python socket
connections. `--offline` installs a deny-network policy before loading and
running the app. Unix-domain sockets remain local and are allowed.

Synor-owned model helpers call the policy engine explicitly before remote model
operations. Native libraries that bypass Python's socket API must also call
`authorize_egress()` at their integration boundary; the socket guard is a
defense-in-depth boundary, not a claim to intercept direct operating-system
syscalls from arbitrary native code.

### Run evidence

Controlled `plan`, `diff`, `update`, `drop`, `explain`, and `SynorRuntime`
executions create:

```text
.synor/runs/<run-id>/
├── manifest.json
└── audit.jsonl
```

The manifest records command, app, environment, state path, policy, timing,
status, counts, and error type. The append-only audit log records lifecycle and
policy-decision events.

Run evidence excludes action payloads, prompts, document text, credentials, and
environment-variable values. A configured `SYNOR_AUDIT_DIR` changes the root.

### Local models

The public `synor.models` package provides:

- `CallableLocalModel` for in-process sync or async inference functions.
- `LlamaCppLocalModel` for an optional, local `llama-cpp-python` model file.

Both return a small `LocalModelResponse`. Model paths must already exist.
Nothing is downloaded. Sentence-transformer loading also gains a
`local_files_only` option and automatically enables it under an offline policy.

## Failure and recovery

- Preview failure leaves targets and component tracking unchanged.
- A denied egress attempt raises before the guarded socket connects and is
  recorded without payload data.
- Run manifests are rewritten atomically after each lifecycle transition.
- A failed run is marked `failed` with the exception class only; exception
  messages are deliberately omitted because they can contain user data.
- Audit directory errors are surfaced before execution by the new runtime and
  detected by `synor doctor`.
- Existing direct `App.update()` calls keep their previous behavior and do not
  create run evidence unless the caller uses `SynorRuntime`.

## Alternatives considered

### Reimplement plan in Python

Rejected because it would drift from Rust reconciliation, ownership transfer,
and connector behavior.

### Make the new facade replace `App`

Rejected because migration would be unnecessary and risky. Composition keeps
rollback as simple as using `App` directly.

### Store audit records in the engine LMDB database

Rejected because manifests have a different retention and trust boundary.
Keeping them as local files also lets operators archive or delete evidence
without changing pipeline state.

### Log full action payloads

Rejected because an audit feature must not create an uncontrolled copy of user
data.

## Consequences

- There is one execution engine and two compatible public entry styles.
- Offline policy is immediately useful for local pipelines and model loading.
- Connector-native enforcement can be added incrementally at explicit egress
  boundaries without changing the policy contract.
- Preview output is intentionally described as connector-specific changes until
  the Python bridge exposes universal action kinds.

## Verification

- Unit tests cover policy decisions, socket denial, redaction, atomic manifests,
  local models, and the new execution facade.
- CLI tests cover `doctor`, `plan`, `diff`, `explain`, both `--offline`
  placements, and compatibility with `update --preview`.
- The local note catalog is run in offline mode twice to prove local execution
  and reuse.
- Full Python, Rust, type-checking, and documentation builds remain release
  gates.
