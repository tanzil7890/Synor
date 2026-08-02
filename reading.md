# Reading Synor

This is a deeper reading of the project overview in the README. It explains
where Synor fits, how data moves through it, and which guarantees come from the
engine rather than from a model or connector.

## 1. What kind of project is Synor?

Synor is best described as a **local-first incremental dataflow and
reconciliation framework**.

Calling it an "AI ETL platform" captures several real use cases, but misses the
core design:

- It is a Python framework and local runtime, not a hosted platform.
- AI models are optional. A pipeline may use an LLM or embedder, ordinary
  Python transformations, or no model at all.
- ETL is one workload. The same engine also maintains vector indexes, graph
  relationships, files, and stream outputs.
- The defining behavior is not data movement by itself. It is preserving
  ownership and correctness from one run to the next.

Traditional orchestrators focus on scheduling tasks. Traditional ETL tools
focus on moving records between systems. Synor focuses on the relationship
between changing inputs, stable work units, and the derived state those work
units own.

## 2. The complete data path

A Synor pipeline has four layers:

1. **Sources** expose keyed files, objects, rows, or events from systems such as
   local storage, S3, Google Drive, Postgres, Kafka, or Iggy.
2. **Processing components** run ordinary Python functions under stable paths.
   A component might represent one document, customer, row, event, or another
   durable domain object.
3. **Operations** perform the application-specific work. This may be plain
   Python or built-in text splitting, local embeddings, hosted model calls,
   transcription, or entity resolution.
4. **Targets** receive declared files, rows, vectors, graph nodes and edges, or
   stream messages through connectors.

The Rust engine sits underneath the Python API. It schedules components,
fingerprints inputs and code, stores incremental state in LMDB, and computes
the target-state changes that connectors apply.

This means Synor is not the model, database, vector store, or message broker.
It coordinates the work and keeps owned outcomes aligned across those systems.

## 3. Observe change

Synor starts with ordinary project inputs: source files, database rows, object
storage objects, vectors, streams, configuration, and code. A change can come
from data, from the Python function body, or from the policy and environment
around the run.

The important detail is that Synor does not treat the whole app as one opaque
job. It records enough local evidence to decide which stable processing
components are affected by the new inputs.

Change detection also follows runtime function dependencies. If a decorated
function calls another decorated function, a logic change can invalidate the
memoized callers that actually depended on it. Hidden dependencies still need
to be passed as arguments or declared through dependency and context APIs.

## 4. Own work

The app boundary is the Python API: `App`, `@syn.task`, `mount`, `call`, and
`spawn_each`. These calls create stable processing-component paths, such as one
component per file or one component per record.

That stable path is the ownership key. If the same component path appears again
with unchanged inputs and unchanged code, `cache=True` lets the engine reuse the
settled result. If a path disappears, Synor knows which outcomes no longer have
an owner.

The component path is therefore more than a task name. It is the durable link
between a source item, previous execution state, and every target state that
the component declared.

## 5. Reconcile target state

Functions declare the world they want through target states: files, rows,
vectors, graph edges, tables, collections, and similar outcomes. They do not
hand-write incremental create/update/delete loops.

When a component finishes, the Rust engine compares the newly declared target
states with the previous declaration for the same owner. It then applies the
minimal changes: create new outcomes, update changed outcomes, and delete
outcomes that disappeared.

Reconciliation is scoped. A component submits its changes after processing
finishes, and each target backend applies its batch atomically when supported.
Synor does not claim one distributed transaction across unrelated targets.

## 6. Keep control separate

The control plane sits beside the execution engine. It handles commands such as
`doctor`, `plan`, `diff`, `update`, `replay`, `lock`, `package`, and
`dashboard`. Its job is not to move user data directly; it makes runs
inspectable and policy-aware.

That separation matters because preview and review can happen before an update
is applied. The run evidence stays metadata-only: manifests, audit events,
provenance, quarantine records, and revocation records.

The native LMDB database remains the source of truth for incremental execution.
The control-plane state store holds evidence, review state, policy results, and
revocation records. Adding controlled execution does not replace the engine's
normal ownership and reconciliation model.

## 7. Propagate cleanup and revocation

Revocation and cleanup are part of the same ownership model. When an input,
component path, or declared target state disappears, Synor can issue deletes to
the connected systems that previously received owned outcomes.

This is the practical payoff of stable component ownership: second runs get
smaller, and deleted source material can remove its derived files, rows, vectors,
and graph edges instead of leaving stale artifacts behind.

Strict index revocation goes further than ordinary cleanup. Through registered
governed-source, certified-target, and guarded-retrieval boundaries, Synor can
suppress an affected source generation before destructive target work, verify
the target postcondition, and retain metadata-only evidence. Direct queries or
unmanaged copies bypass that guarantee.

## 8. What Synor does not claim

The boundaries are as important as the features:

- Preview does not apply target actions, but it still executes ordinary Python
  and is not a universal sandbox.
- Connector batches are not a cross-system distributed transaction.
- A connector that reaches a cloud service or hosted model still uses the
  network, even though the Synor engine itself is local.
- Provable revocation covers supported registered boundaries. It does not prove
  physical media erasure, remove unmanaged copies, or make direct database
  queries safe.
- Correct change detection depends on explicit function inputs and declared
  dependencies. Untracked global or external state cannot be inferred.

These limits keep the project description concrete. Synor provides a strong
execution and ownership model; it does not pretend to replace every system it
connects to.

## 9. Theme-aware README assets

The README uses monochrome assets with separate light and dark variants.

The header wordmark is SVG:

- `docs/public/images/synor-wordmark-light.svg`
- `docs/public/images/synor-wordmark-dark.svg`

The overview diagram is PNG:

- `docs/public/images/synor-project-overview-light.png`
- `docs/public/images/synor-project-overview-dark.png`

Both use `<picture>` and `prefers-color-scheme`, so the README follows the
reader's desktop or browser theme while keeping the same black-and-white visual
structure.
