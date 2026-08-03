# Synor Common Patterns

Common patterns and workflows for building Synor pipelines.

## Core pattern: observe, own, reconcile

All Synor pipelines follow this operating pattern:

1. Observe current inputs.
2. Run work under stable component paths.
3. Declare the complete outcome set each path owns.
4. Reconcile that set with the previous run.

Synor records settled work and handles the incremental reconciliation.

---

## Pattern 1: File Transformation Pipeline

**Use case**: Transform files from one format to another (e.g., Markdown -> HTML, PDF -> Markdown)

```python
import pathlib
import synor as syn
from synor.connectors import localfs
from synor.resources.file import FileLike, PatternFilePathMatcher
from markdown_it import MarkdownIt

_markdown_it = MarkdownIt("gfm-like")

@syn.task(cache=True)
async def process_file(file: FileLike, outdir: pathlib.Path) -> None:
    html = _markdown_it.render(await file.read_text())
    outname = "__".join(file.file_path.path.parts) + ".html"
    localfs.ensure_file(outdir / outname, html, create_parent_dirs=True)

@syn.task
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live=True,  # Enable live file watching
    )
    await syn.spawn_each(process_file, files.items(), outdir)

app = syn.App(
    syn.AppConfig(name="FilesTransform"),
    app_main,
    sourcedir=pathlib.Path("./data"),
    outdir=pathlib.Path("./output_html"),
)
```

**Key points:**
- `cache=True` -- Skip reprocessing unchanged files
- `spawn_each()` -- One component per file; keys from `files.items()` become component subpaths
- `live=True` on `walk_dir()` -- Enables file watching for live mode
- Auto-cleanup -- Deleting source file automatically removes output file

---

## Pattern 2: Vector Embedding Pipeline

**Use case**: Chunk and embed documents for semantic search

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
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, EMBEDDER]

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
        chunk_start=chunk.start.char_offset,
        chunk_end=chunk.end.char_offset,
        text=chunk.text,
        embedding=await syn.use_context(EMBEDDER).embed(chunk.text),
    ))

@syn.task(cache=True)
async def process_file(file: FileLike, table: postgres.TableTarget[DocEmbedding]) -> None:
    text = await file.read_text()
    chunks = _splitter.split(text, chunk_size=2000, chunk_overlap=500, language="markdown")
    id_gen = IdGenerator()
    await syn.map(process_chunk, chunks, file.file_path.path, id_gen, table)

@syn.task
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name="doc_embeddings",
        table_schema=await postgres.TableSchema.from_class(DocEmbedding, primary_key=["id"]),
    )
    target_table.declare_vector_index(column="embedding")

    files = localfs.walk_dir(sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]))
    await syn.spawn_each(process_file, files.items(), target_table)

app = syn.App(syn.AppConfig(name="TextEmbedding"), app_main,
    sourcedir=pathlib.Path("./markdown_files"))
```

**Key points:**
- `mount_table_target(PG_DB, ...)` -- Takes `ContextKey` as first argument
- `Annotated[NDArray, EMBEDDER]` -- Vector annotation uses ContextKey for auto dimension inference
- `map()` -- Unbounded full-fan-in execution within a component
- `map_bounded()` -- Bounded task and iterator admission for large inputs
- `map_stream()` -- Bounded completion-order iteration with O(limit) retained results
- `IdGenerator` -- Generates stable unique IDs for chunks across incremental updates
- `cache=True` on `process_file` -- Skips unchanged files entirely

---

## Pattern 3: Database Source -> Transform -> Database Target

**Use case**: Transform data from one database to another

```python
from dataclasses import dataclass
from typing import AsyncIterator

import asyncpg
import synor as syn
from synor.connectors import postgres

SOURCE_DB_URL = "postgres://localhost/source_db"
TARGET_DB_URL = "postgres://localhost/target_db"

SOURCE_DB = syn.ContextKey[asyncpg.Pool]("source_db")
TARGET_DB = syn.ContextKey[asyncpg.Pool]("target_db")

@dataclass
class SourceRecord:
    id: int
    name: str
    value: float

@dataclass
class TargetRecord:
    id: int
    name: str
    value: float
    processed: bool

@syn.lifespan
async def synor_lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
    async with (
        await asyncpg.create_pool(SOURCE_DB_URL) as source_pool,
        await asyncpg.create_pool(TARGET_DB_URL) as target_pool,
    ):
        builder.provide(SOURCE_DB, source_pool)
        builder.provide(TARGET_DB, target_pool)
        yield

@syn.task(cache=True)
async def process_record(record: SourceRecord, target_table: postgres.TableTarget[TargetRecord]) -> None:
    target_table.ensure_row(row=TargetRecord(
        id=record.id, name=record.name.upper(),
        value=record.value * 2, processed=True,
    ))

@syn.task
async def app_main() -> None:
    target_table = await postgres.mount_table_target(
        TARGET_DB,
        table_name="target_records",
        table_schema=await postgres.TableSchema.from_class(TargetRecord, primary_key=["id"]),
    )

    source = postgres.PgTableSource(
        syn.use_context(SOURCE_DB),
        table_name="source_records",
        row_type=SourceRecord,
    )
    await syn.spawn_each(
        syn.unit_path("record"),
        process_record,
        source.fetch_rows().items(key=lambda r: r.id),
        target_table,
    )

app = syn.App(syn.AppConfig(name="DatabaseTransform"), app_main)
```

---

## Pattern 4: LLM-Based Extraction Pipeline

**Use case**: Extract structured data using LLMs

```python
import instructor
from dataclasses import dataclass
from typing import AsyncIterator
from pydantic import BaseModel
from litellm import acompletion

import asyncpg
import synor as syn
from synor.connectors import postgres

DATABASE_URL = "postgres://synor:synor@localhost/synor"
PG_DB = syn.ContextKey[asyncpg.Pool]("pg_db")

_instructor_client = instructor.from_litellm(acompletion, mode=instructor.Mode.JSON)

class ExtractedTopic(BaseModel):
    name: str
    description: str

class ExtractionResult(BaseModel):
    title: str
    topics: list[ExtractedTopic]

@dataclass
class Message:
    id: int
    title: str
    content: str

@dataclass
class Topic:
    message_id: int
    name: str
    description: str

@syn.task(cache=True)
async def extract_and_store(
    content: str, message_id: int,
    messages_table: postgres.TableTarget[Message],
    topics_table: postgres.TableTarget[Topic],
) -> None:
    result = await _instructor_client.chat.completions.create(
        model="gpt-4",
        response_model=ExtractionResult,
        messages=[{"role": "user", "content": f"Extract topics:\n\n{content}"}],
    )
    messages_table.ensure_row(row=Message(id=message_id, title=result.title, content=content))
    for topic in result.topics:
        topics_table.ensure_row(row=Topic(
            message_id=message_id, name=topic.name, description=topic.description,
        ))

@syn.task
async def app_main(input_texts: list[str]) -> None:
    messages_table = await postgres.mount_table_target(
        PG_DB, table_name="messages",
        table_schema=await postgres.TableSchema.from_class(Message, primary_key=["id"]),
    )
    topics_table = await postgres.mount_table_target(
        PG_DB, table_name="topics",
        table_schema=await postgres.TableSchema.from_class(Topic, primary_key=["message_id", "name"]),
    )
    for idx, text in enumerate(input_texts):
        await syn.spawn(
            syn.unit_path("text", idx),
            extract_and_store, text, idx, messages_table, topics_table,
        )

app = syn.App(syn.AppConfig(name="LLMExtraction"), app_main, input_texts=["text1...", "text2..."])
```

**Key points:**
- `cache=True` on extraction -- Avoids re-calling LLM for unchanged inputs
- Multiple target tables -- Declare rows in multiple tables from single component
- Pydantic models -- For structured LLM outputs

---

## Pattern 5: Kafka Streaming Pipeline

**Use case**: Process Kafka messages and write to a database

```python
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass

from confluent_kafka import Message

import synor as syn
from synor.connectors import kafka, lancedb

LANCE_DB = syn.ContextKey[lancedb.LanceAsyncConnection]("lance_db")

@dataclass
class Product:
    sku: str
    name: str
    category: str
    price: float

@syn.lifespan
async def synor_lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
    conn = await lancedb.connect_async("./lancedb_data")
    builder.provide(LANCE_DB, conn)
    yield

@syn.task
async def process_message(msg: Message, table: lancedb.TableTarget[Product]) -> None:
    value = msg.value()
    if value is None:
        return
    row = json.loads(value.decode() if isinstance(value, bytes) else value)
    table.ensure_row(row=Product(**{**row, "price": float(row["price"])}))

@syn.task
async def app_main() -> None:
    products_table = await lancedb.mount_table_target(
        LANCE_DB, table_name="products",
        table_schema=await lancedb.TableSchema.from_class(Product, primary_key=["sku"]),
    )

    consumer = kafka.create_consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "my-group",
        "auto.offset.reset": "earliest",
    })
    items = kafka.topic_as_map(consumer, ["products-topic"])
    await syn.spawn_each(process_message, items, products_table)

app = syn.App(syn.AppConfig(name="KafkaToLanceDB"), app_main)
```

**Key points:**
- `kafka.topic_as_map()` returns a `LiveMapFeed` -- `spawn_each()` auto-detects for live mode
- Pipeline runs continuously in live mode, processing new messages as they arrive

---

## Pattern 6: Context Management for Shared Resources

**Use case**: Share expensive resources across components

```python
import synor as syn
from synor.ops.sentence_transformers import SentenceTransformerEmbedder

EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder")
CONFIG = syn.ContextKey[dict]("config")

@syn.lifespan
async def synor_lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.provide(EMBEDDER, SentenceTransformerEmbedder("all-MiniLM-L6-v2"))
    builder.provide(CONFIG, {"chunk_size": 1000, "overlap": 200})
    yield

@syn.task
async def process_item(text: str) -> None:
    embedder = syn.use_context(EMBEDDER)
    config = syn.use_context(CONFIG)
    embedding = await embedder.embed(text)
    ...
```

**Key points:**
- `detect_change=True` -- Opt in to invalidate memos when value changes (use for models/config affecting output)
- `detect_change=False` (default) -- For resources not affecting computation (DB connections, loggers)
- `ContextKey` name is stable identity -- avoid renaming across runs

---

## Common Anti-Patterns to Avoid

### Missing `@syn.task` Decorator

```python
# BAD: Missing decorator
async def process_file(file, table):
    table.ensure_row(...)

# GOOD:
@syn.task
async def process_file(file, table):
    table.ensure_row(...)
```

### Reprocessing Everything

```python
# BAD: No memoization
@syn.task  # Missing cache=True
async def process_file(file, table):
    embedding = await embedder.embed(await file.read_text())  # Expensive!

# GOOD:
@syn.task(cache=True)
async def process_file(file, table):
    ...
```

### Unstable Component Paths

```python
# BAD: Using object references or indices
for file in files:
    await syn.spawn(syn.unit_path(file), ...)     # Object ref
for idx, item in enumerate(items):
    await syn.spawn(syn.unit_path(idx), ...)      # Index changes

# GOOD: Use stable identifiers
await syn.spawn_each(process_file, files.items(), table)   # Keys from items()
```

### Loading Resources Per Component

```python
# BAD: Loading model in every component
@syn.task
async def process(text):
    model = SentenceTransformer("model")  # Loaded repeatedly!

# GOOD: Load once in lifespan, use via context
embedder = syn.use_context(EMBEDDER)
```

### Mixing Target State with Side Effects

```python
# BAD: Side effects not detected
@syn.task
async def process(data):
    requests.post("https://api.example.com", json=data)  # Not detected!

# GOOD: Only declare target states
table.ensure_row(row=result)
```

---

## See Also

- [Connectors Reference](./connectors.md)
- [API Reference](./api_reference.md)
- [Setup Project](./setup_project.md)
