---
name: synor
description: This skill should be used when building data processing pipelines with Synor, a Python library for incremental data transformation. Use when the task involves processing files/data into databases, creating vector embeddings, building knowledge graphs, ETL workflows, or any data pipeline requiring automatic change detection and incremental updates. Synor is Python-native, supports ordinary Python types, has no DSL, and uses version 0.1.0a1 or later.
---

# Synor

Synor keeps every derived file, row, and index aligned with the inputs that produced it. Pipeline logic stays in Python. Stable component paths own declared target states, and the local engine reconciles only the outcomes affected by a change.

## Overview

Synor enables building data pipelines that:

- **Automatically handle incremental updates**: Only reprocess changed data
- **Use declarative target states**: Declare what should exist, not how to update
- **Support any Python types**: No custom DSL -- use dataclasses, Pydantic, NamedTuple
- **Provide function memoization**: Skip expensive operations when inputs/code unchanged
- **Sync to multiple targets**: PostgreSQL, SQLite, LanceDB, Qdrant, SurrealDB, Apache Doris, file systems, Kafka

**Key principle**: stable work paths own declared outcomes.

## When to Use This Skill

Use this skill when building pipelines that involve:

- **Document processing**: PDF/Markdown conversion, text extraction, chunking
- **Vector embeddings**: Embedding documents/code for semantic search
- **Database transformations**: ETL from source DB to target DB
- **Knowledge graphs**: Extract entities and relationships from data
- **LLM-based extraction**: Structured data extraction using LLMs
- **File-based pipelines**: Transform files from one format to another
- **Incremental indexing**: Keep search indexes up-to-date with source changes
- **Streaming pipelines**: Kafka-based real-time data processing

## Quick Start: Creating a New Project

### Initialize Project

```bash
synor init my-project
cd my-project
```

This creates: `main.py`, `pyproject.toml`, `README.md`. The generated `main.py` sets the database location in its lifespan via `builder.settings.db_path = pathlib.Path("./synor.db")`.

### Add Dependencies

```toml
# For vector embeddings with PostgreSQL
dependencies = ["synor>=0.1.0a1", "sentence-transformers", "asyncpg"]

# For LLM extraction
dependencies = ["synor>=0.1.0a1", "litellm", "instructor", "pydantic>=2.0"]
```

See [references/setup_project.md](references/setup_project.md) for complete examples.

### Run the Pipeline

```bash
uv run synor doctor main.py --offline
uv run synor plan main.py --offline
uv run synor update main.py --offline
```

Use `diff` for a compact redacted action list and `explain` for local ownership,
policy, and recent-run evidence. Controlled operations write metadata-only
manifests and audit events under `.synor/runs/`.

Plans also write a replay envelope. Trustworthy local workflows can use:

```bash
synor replay .synor/runs/<run-id>/replay.json --offline
synor lock main.py
synor package main.py --output pipeline.synor
synor package-verify pipeline.synor
synor quarantine list --status open
synor dashboard
```

`SYNOR_STATE_STORE=file://.synor/control` selects the default pluggable
control-plane store. `SYNOR_STATE_KEY` wraps it with AES-256-GCM encryption.
This does not encrypt the native LMDB database.

`SYNOR_PII_ACTION` accepts `allow`, `redact`, `deny`, or `quarantine`.
Structured built-in detectors cover email, phone, US SSN, and checksum-valid
payment-card values. They are a guardrail, not a complete DLP system.

`--offline` denies standard Python socket and DNS operations. It is a
process-level application guard, not an operating-system firewall for
subprocesses or native code.

## Core Concepts

### 1. Apps

An **App** is the top-level executable that binds a main function with parameters:

```python
import synor as syn

@syn.task
async def app_main(sourcedir: pathlib.Path) -> None:
    ...

app = syn.App(
    syn.AppConfig(name="MyApp"),
    app_main,
    sourcedir=pathlib.Path("./data"),
)
```

### 2. Functions (`@syn.task`)

The `@syn.task` decorator marks functions as Synor processing functions. Add `cache=True` to skip re-execution when inputs/code are unchanged:

```python
@syn.task(cache=True)
async def expensive_operation(data: str) -> Result:
    # LLM call, embedding generation, heavy computation
    return await expensive_transform(data)
```

**Key parameters:**
- `cache=True` -- Enable memoization (skip if inputs/code unchanged)
- `version=1` -- Explicit version bump to force re-execution
- `batching=True` -- Auto-batch concurrent calls (async only)
- `runner=syn.GPU` -- Serialize GPU-bound execution

### 3. Processing Components

A **processing component** groups an item's processing with its target states.

**Mount components** with `spawn_each()` (preferred for lists) or `spawn()`:

```python
# One component per item (preferred for lists)
await syn.spawn_each(process_file, files.items(), target_table)

# Single component (subpath auto-derived from fn.__name__)
await syn.spawn(setup_fn, arg1)

# Dependent component (blocks until result returned)
result = await syn.call(init_fn)

# Explicit subpath (when you need a specific path, e.g. in loops)
await syn.spawn(syn.unit_path("item", item_id), process_item, item)
```

**Key points:**
- All mount APIs are `async`
- `spawn()`, `call()`, and `spawn_each()` auto-derive subpath from `fn.__name__`; optional explicit subpath as first arg
- Use `call()` when you need the return value
- Use stable component paths for proper memoization and cleanup

### 4. Target States

**Declare** what should exist -- Synor handles creation/update/deletion:

```python
# Database row target
table.ensure_row(row=MyRecord(id=1, name="example"))

# File target
localfs.ensure_file(outdir / "output.txt", content, create_parent_dirs=True)

# Kafka message target
topic_target.ensure_target_state(key="msg-1", value=json.dumps(data))
```

### 5. Context for Shared Resources

Use `ContextKey` to share expensive resources (DB connections, models) across components:

```python
EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder")

@syn.lifespan
async def synor_lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.provide(EMBEDDER, SentenceTransformerEmbedder("all-MiniLM-L6-v2"))
    yield

# In processing functions:
embedder = syn.use_context(EMBEDDER)
```

The `@syn.lifespan` decorator registers the function to the default Synor environment, which is shared among all apps by default. `ContextKey` also serves as the stable identity for sources/targets -- the `key` string must remain stable across runs.

### 6. ID Generation

Generate stable, unique identifiers that persist across incremental updates:

```python
from synor.resources.id import generate_id, IdGenerator

# Deterministic: same dep -> same ID
chunk_id = await generate_id(chunk.text)

# Always distinct: each call -> new ID, even with same dep
id_gen = IdGenerator()
for chunk in chunks:
    chunk_id = await id_gen.next_id(chunk.text)
```

### 7. Catch-Up vs Live Mode

By default, `app.update()` runs in **catch-up mode**: it scans all sources, processes what changed since the last run (memoized components are skipped), syncs target states, and returns. Each call still has to scan sources to discover changes.

**Live mode** keeps the app running after catch-up and lets components stream changes continuously from their sources (e.g., file watcher, Kafka consumer), applying them with very low latency.

```python
# Enable live mode
app.update_blocking(live=True)
# Or: synor update main.py -L
```

Two things are needed: (1) enable live mode on the app, and (2) use a source that supports live updates.

- **`LiveMapView`** sources (e.g., `localfs.walk_dir(..., live=True)`) scan current state first, then watch for changes. They also work in catch-up mode -- write the pipeline once, choose mode at run time.
- **`LiveMapFeed`** sources (e.g., `kafka.topic_as_map()`) only stream changes with no initial snapshot. `spawn_each()` auto-detects these and creates a live component internally.

```python
# LocalFS with live watching
files = localfs.walk_dir(sourcedir, live=True, ...)
await syn.spawn_each(process_file, files.items(), target)

# Kafka -- inherently live
items = kafka.topic_as_map(consumer, ["my-topic"])
await syn.spawn_each(process_message, items, target)
```

## Common Pipeline Patterns

### Pattern 1: File Transformation

```python
import pathlib
import synor as syn
from synor.connectors import localfs
from synor.resources.file import FileLike, PatternFilePathMatcher

@syn.task(cache=True)
async def process_file(file: FileLike, outdir: pathlib.Path) -> None:
    content = await file.read_text()
    transformed = transform_content(content)
    outname = file.file_path.path.stem + ".out"
    localfs.ensure_file(outdir / outname, transformed, create_parent_dirs=True)

@syn.task
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await syn.spawn_each(process_file, files.items(), outdir)

app = syn.App(
    syn.AppConfig(name="Transform"),
    app_main,
    sourcedir=pathlib.Path("./data"),
    outdir=pathlib.Path("./out"),
)
```

### Pattern 2: Vector Embedding Pipeline

```python
import pathlib
from dataclasses import dataclass
from typing import AsyncIterator, Annotated

import asyncpg
from numpy.typing import NDArray

import synor as syn
from synor.connectors import localfs, postgres
from synor.ops.text import RecursiveSplitter
from synor.ops.sentence_transformers import SentenceTransformerEmbedder
from synor.resources.chunk import Chunk
from synor.resources.file import FileLike, PatternFilePathMatcher
from synor.resources.id import IdGenerator

DATABASE_URL = "postgres://synor:synor@localhost/synor"
PG_DB = syn.ContextKey[asyncpg.Pool]("pg_db")
EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder")

_splitter = RecursiveSplitter()

@dataclass
class DocEmbedding:
    id: int
    filename: str
    text: str
    embedding: Annotated[NDArray, EMBEDDER]  # Dimensions inferred from ContextKey
    chunk_start: int
    chunk_end: int

@syn.lifespan
async def synor_lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
    async with await asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder("all-MiniLM-L6-v2"))
        yield

@syn.task
async def process_chunk(
    chunk: Chunk, filename: pathlib.PurePath,
    id_gen: IdGenerator, table: postgres.TableTarget[DocEmbedding],
) -> None:
    table.ensure_row(row=DocEmbedding(
        id=await id_gen.next_id(chunk.text),
        filename=str(filename),
        text=chunk.text,
        embedding=await syn.use_context(EMBEDDER).embed(chunk.text),
        chunk_start=chunk.start.char_offset,
        chunk_end=chunk.end.char_offset,
    ))

@syn.task(cache=True)
async def process_file(file: FileLike, table: postgres.TableTarget[DocEmbedding]) -> None:
    text = await file.read_text()
    chunks = _splitter.split(text, chunk_size=2000, chunk_overlap=500)
    id_gen = IdGenerator()
    await syn.map(process_chunk, chunks, file.file_path.path, id_gen, table)

@syn.task
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name="embeddings",
        table_schema=await postgres.TableSchema.from_class(DocEmbedding, primary_key=["id"]),
    )
    target_table.declare_vector_index(column="embedding")

    files = localfs.walk_dir(sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]))
    await syn.spawn_each(process_file, files.items(), target_table)

app = syn.App(syn.AppConfig(name="Embedding"), app_main, sourcedir=pathlib.Path("./data"))
```

### Pattern 3: LLM-Based Extraction

```python
import instructor
from pydantic import BaseModel
from litellm import acompletion

_instructor_client = instructor.from_litellm(acompletion, mode=instructor.Mode.JSON)

class ExtractionResult(BaseModel):
    title: str
    topics: list[str]

@syn.task(cache=True)  # Memo avoids re-calling LLM
async def extract_and_store(content: str, message_id: int, table) -> None:
    result = await _instructor_client.chat.completions.create(
        model="gpt-4",
        response_model=ExtractionResult,
        messages=[{"role": "user", "content": f"Extract topics: {content}"}],
    )
    table.ensure_row(row=Message(id=message_id, title=result.title, content=content))
```

## Connectors and Operations

Synor provides connectors for reading from and writing to external systems:

| Connector | Source | Target | Vectors | Use Case |
|-----------|--------|--------|---------|----------|
| **PostgreSQL** | Y | Y | pgvector | Production SQL + vectors |
| **SQLite** | - | Y | sqlite-vec | Local SQL + vectors |
| **LanceDB** | - | Y | Y | Cloud-native vector DB |
| **Qdrant** | - | Y | Y | Specialized vector DB |
| **SurrealDB** | - | Y | Y | Graph + document DB |
| **Apache Doris** | - | Y | Y | Analytical DB + vectors |
| **LocalFS** | Y | Y | N/A | File-based pipelines |
| **Amazon S3** | Y | - | N/A | Cloud object storage |
| **Kafka** | Y | Y | N/A | Streaming pipelines |
| **Google Drive** | Y | - | N/A | Cloud file source |

For detailed connector documentation, see [references/connectors.md](references/connectors.md).

## Text and Embedding Operations

### Text Splitting

```python
from synor.ops.text import RecursiveSplitter, detect_code_language

splitter = RecursiveSplitter()
language = detect_code_language(filename="example.py")
chunks = splitter.split(text, chunk_size=1000, chunk_overlap=200, language=language)
```

### Embeddings

```python
from synor.ops.sentence_transformers import SentenceTransformerEmbedder

embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
embedding = await embedder.embed(text)  # Returns NDArray
```

## CLI Commands

```bash
synor init my-project              # Create new project
synor update main.py               # Run app
synor update main.py:my_app        # Run specific app
synor update main.py -L            # Run in live mode (continuous)
synor update main.py --full-reprocess  # Reprocess everything
synor drop main.py [-f]            # Drop and reset all state
synor ls [main.py]                 # List apps
synor show main.py [--tree]        # Show component paths
```

## Best Practices

### 1. Use `@syn.task` on All Processing Functions

Every function that participates in the pipeline (declares target states, calls mount APIs, etc.) must be decorated with `@syn.task`.

### 2. Add Memoization for Expensive Operations

```python
@syn.task(cache=True)  # Skip re-execution when inputs/code unchanged
async def process_chunk(chunk, table):
    embedding = await embedder.embed(chunk.text)  # Expensive!
    table.ensure_row(...)
```

### 3. Use Stable Component Paths

```python
# Good: Stable identifiers
syn.unit_path("file", str(file.file_path.path))
syn.unit_path("record", record.id)

# Bad: Unstable identifiers
syn.unit_path("file", file)      # Object reference
syn.unit_path("idx", idx)        # Index changes
```

### 4. Use Context for Shared Resources

```python
@syn.lifespan
async def synor_lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
    async with await asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        yield
```

### 5. Use `Annotated[NDArray, CONTEXT_KEY]` for Vectors

```python
EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder")

@dataclass
class Record:
    vector: Annotated[NDArray, EMBEDDER]  # Auto-infer dimensions from ContextKey
```

### 6. Use Convenience APIs for Targets

```python
# Mount table target -- subpath is automatic
table = await postgres.mount_table_target(
    PG_DB,
    table_name="my_table",
    table_schema=await postgres.TableSchema.from_class(MyRecord, primary_key=["id"]),
)
```

## Troubleshooting

### Everything Reprocessing

Add `cache=True` to expensive functions:

```python
@syn.task(cache=True)  # Add this
async def process_item(item):
    ...
```

### Memoization Not Working

Check component paths are stable. Use stable IDs, not object references.

## Resources

### references/

- **[api_reference.md](references/api_reference.md)**: Quick API reference
- **[connectors.md](references/connectors.md)**: Complete connector reference
- **[patterns.md](references/patterns.md)**: Detailed pipeline patterns
- **[setup_project.md](references/setup_project.md)**: Project setup guide
- **[setup_database.md](references/setup_database.md)**: Database setup guide

### Runnable examples

Every pattern above has a complete, runnable app under
`examples/` — start
from the one closest to the task and adapt it. Each has its own `README.md` and
most Python examples have a `.env.example`; see
`examples/AGENTS.md`
for the full map, credentials, and per-example run commands. Good starting points:

- Vector search → `text_embedding` (Postgres), `text_embedding_qdrant` / `_lancedb` (other stores)
- Code search → `code_embedding`
- LLM extraction → `hn_trending_topics`, `patient_intake_extraction_baml`
- Knowledge graph → `conversation_to_knowledge`, `meeting_notes_graph_neo4j`
- Custom transform → `files_transform`, `pdf_to_markdown`

### External

- Synor Documentation — full text at llms-full.txt
- GitHub Examples

## Version Note

This skill is for Synor `>=1.0.0` (v1). It uses a completely different API from v0.

**v0 code is what you likely learned from training data — do not emit it.** If you find yourself writing any of these symbols, you are using the removed v0 API:

| v0 (removed) | v1 equivalent |
|---|---|
| `@synor.flow_def`, `FlowBuilder`, `Flow`, `open_flow` | `syn.App` + a `@syn.task` main function |
| `DataScope`, `DataSlice`, `add_collector()`, `collect()`, `export()` | declare target states via Target APIs (`ensure_row`, `ensure_file`) inside mounted components |
| `synor.sources.LocalFile`, `synor.sources.*` | connector APIs, e.g. `localfs.walk_dir(...)` |
| `synor.functions.SplitRecursively`, `synor.functions.*` | `synor.ops.*`, e.g. `RecursiveSplitter` |
| `synor.targets.Postgres`, `synor.targets.*` / `storages.*` | connector targets, e.g. `postgres.ensure_table_target(...)` |
| `transform_flow`, `synor.op.function()` | plain `@syn.task` functions |
| `synor.init()`, `settings`, `SYNOR_DATABASE_URL` | `syn.App(syn.AppConfig(...))`; state lives in a local db path |
| CLI `synor setup` | no setup step — just `synor update` (`-L`/`--live` for live mode) |

When reading third-party tutorials or model memory that mention these v0 symbols, disregard them and use the patterns in this skill and `references/api_reference.md` instead.
