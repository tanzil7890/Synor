<p align="center">
  <img src="docs/public/images/synor-wordmark.svg" alt="Synor" width="220">
</p>

<h1 align="center">Built for the second run.</h1>

<p align="center">
  <strong>Synor keeps every derived file, row, and index aligned with the
  inputs that produced it.</strong>
</p>

Synor is a local-first Python framework with a Rust execution engine. You write
ordinary functions that describe the outcomes your project needs. Synor
remembers settled work, runs the parts affected by a change, and reconciles
creates, updates, and removals for you.

This workspace is an alpha release intended for local evaluation. The current
version is `0.1.0a1`.

## Check before the first run

Synor can inspect the local setup, compute a dry-run plan, and enforce a
process-wide offline policy before it changes an outcome:

```bash
cd examples/local_note_catalog
../../.venv/bin/synor doctor main.py --offline
../../.venv/bin/synor plan main.py --offline
../../.venv/bin/synor diff main.py --offline
../../.venv/bin/synor update main.py --offline
../../.venv/bin/synor explain main.py --offline
```

`plan` and `diff` use the engine's preview path and do not apply target
actions. Controlled runs write metadata-only evidence under
`.synor/runs/<run-id>/`: a final `manifest.json` and an append-only
`audit.jsonl`. Prompts, file content, row values, credentials, and exception
messages are not written to those logs.

## Keep the run inspectable

Phase 3 adds opt-in controls around the same native engine:

```bash
export SYNOR_STATE_KEY="$(../../.venv/bin/synor state-key)"
../../.venv/bin/synor plan main.py --offline
../../.venv/bin/synor replay .synor/runs/<run-id>/replay.json --offline
../../.venv/bin/synor lock main.py
../../.venv/bin/synor package main.py --output local-note-catalog.synor
../../.venv/bin/synor dashboard
```

The control plane supports pluggable state stores and AES-256-GCM encryption,
records target-state ownership provenance, enforces structured PII policy,
quarantines metadata-only failure cases for explicit review, and builds
offline-verifiable deterministic pipeline packages. Existing `App.update()`
code and native LMDB reconciliation remain unchanged.

Encryption covers the `.synor/control` state-store mirror and review state.
Dashboard-compatible evidence in `.synor/runs` remains redacted, metadata-only
JSON, and native LMDB pages remain unchanged. Put either path on an encrypted
volume when it also requires encryption at rest.

## See the second run

The smallest useful demonstration is the local note catalog. It reads Markdown
notes and maintains one JSON record per note without a database server, model
API, or network request.

```bash
. "$HOME/.cargo/env"
uv run maturin develop
cd examples/local_note_catalog
../../.venv/bin/synor update main.py --offline
```

Run the final command twice. The first run creates the catalog. The second run
reuses both settled work units. Edit `notes/deploy.md`, run it again, and only
that note is refreshed.

The complete pipeline is short:

```python
@syn.fn(memo=True)
async def catalog_note(file: FileLike, catalog_dir: pathlib.Path) -> None:
    record = summarize(await file.read_text())
    localfs.declare_file(
        catalog_dir / f"{file.file_path.path.stem}.json",
        json.dumps(record, indent=2),
        create_parent_dirs=True,
    )


@syn.fn
async def app_main(notes_dir: pathlib.Path, catalog_dir: pathlib.Path) -> None:
    notes = localfs.walk_dir(
        notes_dir,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await syn.mount_each(catalog_note, notes.items(), catalog_dir)
```

Three questions describe every run:

1. What changed?
2. What work must run?
3. What outcome must be repaired?

Synor’s local change ledger answers the first question. Stable processing
components answer the second. Declared target states answer the third.

## What Synor owns

- **Work boundaries:** each processing component has a stable path and owns its
  declared outcomes.
- **Reuse:** `@syn.fn(memo=True)` skips a function when its inputs and code are
  unchanged.
- **Reconciliation:** new outcomes are created, changed outcomes are updated,
  and outcomes with no remaining owner are removed.
- **Local state:** the native engine keeps its execution record in a local LMDB
  database.
- **Python composition:** pipeline logic stays in Python and can use normal
  classes, functions, async code, and third-party libraries.
- **Reviewable operation:** plans, policy decisions, and metadata-only run
  evidence make local execution inspectable before and after an update.

Connectors cover local files, object storage, SQL databases, vector stores,
graphs, and streams. Individual connectors may require their own services, but
the Synor engine itself runs locally.

## Work in this repository

```text
python/synor/      Python API, connectors, resources, and operations
rust/              Incremental engine and Python bindings
examples/          Runnable pipelines, starting with local_note_catalog
docs/              Documentation site and the Synor identity system
skills/synor/      Bundled coding-agent guidance
```

Build and validate the local package:

```bash
uv sync --group build-test
uv run maturin develop
cargo test --workspace
uv run mypy
uv run pytest python/
cd docs && npm run build
```

Start with the [local note catalog](examples/local_note_catalog/), then read the
[documentation source](docs/src/content/docs/) or the
[identity system](docs/DESIGN_SYSTEM.md).
