---
title: Semantic Search 101
description: 'The simplest end-to-end vector index with Synor V1 — chunk Markdown files, embed each chunk, store the vectors in Postgres with pgvector, and search them in natural language.'
slug: text-embedding
image: /docs/images/synor-mark.svg
tags: [vector-index, semantic-search]
---



We'll take a folder of Markdown files and turn it into a [vector index](https://github.com/pgvector/pgvector) you can search in plain English — the foundation under every RAG and semantic-search system. Point it at your docs, and "how does incremental processing work?" finds the right passage even when it shares no keywords with the text.

The whole pipeline is ordinary `async` Python and your own types. The heavy lifting — incremental processing, change tracking, managed targets — runs in a Rust engine underneath, so only what changed gets re-embedded and re-upserted.


## Flow overview



From a high level, these are the steps:

1. Read Markdown files from a local directory.
2. Split each file into overlapping chunks, then embed every chunk.
3. Store the chunks and their embeddings in Postgres (as target states).

You declare the transformation logic with native Python, without worrying about how updates propagate. Think: **each work unit declares the outcomes it owns**.

> **New to embeddings?** An *embedding* is a list of numbers (a vector) that captures the *meaning* of a piece of text, so passages with similar meaning land close together in vector space. A *vector index* stores those vectors and finds the nearest ones to your query fast. That's what lets search match by meaning instead of exact words.

## Setup

- A running Postgres with the [pgvector](https://github.com/pgvector/pgvector) extension. Synor supports many targets, so you can pick another store.

  ```sh
  export POSTGRES_URL="postgres://synor:synor@localhost/synor"
  ```

- Install Synor and the dependencies this example uses:

  ```sh
  pip install -U "synor[postgres,sentence_transformers]" asyncpg pgvector numpy python-dotenv
  ```

- A few `.md` files to index. Grab the sample files from the repo, or drop your own notes into a `markdown_files/` directory.

## Define the data and shared resources

Apps are the top-level runnable unit in Synor. Before the App, we set up two things the rest of the code builds on. `DocEmbedding` defines one row of the output table — each chunk of text becomes one row, with its filename, location, text, and embedding vector. `synor_lifespan` provides the shared resources every step needs — the Postgres connection pool and the embedding model — once at startup.

```python
import os
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

DATABASE_URL = os.getenv("POSTGRES_URL", "postgres://synor:synor@localhost/synor")
TABLE_NAME = "doc_embeddings"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

PG_DB = syn.ContextKey[asyncpg.Pool]("text_embedding_db")
EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

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
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
        yield
```

`embedding: Annotated[NDArray, EMBEDDER]` ties the vector column to the embedder, so its dimensions are inferred automatically — and if you swap the model later, Synor notices (`detect_change=True`) and re-embeds.

## Process a file



`process_file` runs once per file. It reads the file, splits the text into overlapping chunks, and maps each chunk to `process_chunk`.

```python
@syn.fn(memo=True)
async def process_file(
    file: FileLike,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    text = await file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    id_gen = IdGenerator()
    await syn.map(process_chunk, chunks, file.file_path.path, id_gen, table)
```

Chunking keeps each embedded unit small and focused, and the overlap means an idea that straddles a boundary still lands whole in at least one chunk.

`@syn.fn` with `memo=True` is what makes this incremental: if a file's content and this function's code are both unchanged, the whole file is skipped on the next run. `syn.map` fans out to one `process_chunk` call per chunk.

## Process a chunk

`process_chunk` embeds the chunk with the shared embedder and declares the target row.

```python
@syn.fn
async def process_chunk(
    chunk: Chunk,
    filename: pathlib.PurePath,
    id_gen: IdGenerator,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    table.declare_row(
        row=DocEmbedding(
            id=await id_gen.next_id(chunk.text),
            filename=str(filename),
            chunk_start=chunk.start.char_offset,
            chunk_end=chunk.end.char_offset,
            text=chunk.text,
            embedding=await syn.use_context(EMBEDDER).embed(chunk.text),
        ),
    )
```

We use `SentenceTransformerEmbedder` with `all-MiniLM-L6-v2` — a small, fast model that runs locally with no API key. There are 12k+ sentence-transformer models on [Hugging Face](https://huggingface.co/models?other=sentence-transformers), so swap in whichever you prefer. `table.declare_row` declares the row as a target state; Synor handles inserting, updating, or deleting it to match.

## Define the main function



`app_main` wires the source to the target. It mounts the Postgres table (with a vector index), walks the source directory, and mounts one processing component per file.

```python
@syn.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            DocEmbedding, primary_key=["id"],
        ),
    )
    target_table.declare_vector_index(column="embedding")

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live=True,  # watch for changes; pass -L to `synor update` to run live
    )
    await syn.mount_each(process_file, files.items(), target_table)
```

`mount_table_target` creates and manages the Postgres table for you: schema, the pgvector index, idempotent upserts, and orphan cleanup when a file disappears. `live=True` makes the filesystem source watch for changes, and `mount_each` runs one component per file so the engine can track and update them independently.

## Create the App



Bind `app_main` into a `syn.App` and point it at the folder of Markdown files.

```python
app = syn.App(
    syn.AppConfig(name="TextEmbeddingV1"),
    app_main,
    sourcedir=pathlib.Path("./markdown_files"),
)
```

That is the entire indexing path.

## Run the pipeline

Run the `synor` CLI to build and update the index. Choose catch-up (scan, sync, exit) or live (catch up, then keep watching):

```sh
# Catch-up run
synor update main

# Live run: keep watching for file changes
synor update -L main
```

## Query the index

Match user text against the index with a plain SQL query, reusing the *same* embedder from the indexing flow so indexing and querying stay consistent.

```python
async def query_once(pool, embedder, query: str, *, top_k: int = 5) -> None:
    query_vec = await embedder.embed(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT filename, text, embedding <=> $1 AS distance
            FROM "{TABLE_NAME}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec, top_k,
        )
    for r in rows:
        score = 1.0 - float(r["distance"])
        print(f"[{score:.3f}] {r['filename']}")
        print(f"    {r['text']}")
        print("---")
```

The `<=>` operator is pgvector's cosine distance. We turn it into a similarity score and print the filename and the matching chunk. Run a search straight from the command line:

```bash
python main.py "what is self-attention?"
```

The most semantically similar chunks come back ranked — even when they share none of the words in your query. That's the whole point of a vector index.

## Incremental updates

Synor keeps the index in sync with your files and does the **minimum work** to get there. You never compute a diff or write update logic: you change something, and Synor works out exactly what to embed, upsert, and delete. Two pieces make this work. `@syn.fn(memo=True)` decides what to *recompute* — a file is skipped when its content and the function's code are both unchanged. `mount_table_target` decides what to *write* — each row's `id` is derived from its chunk's text, so it upserts only the rows that actually changed and deletes rows whose source is gone.

- **A file is added** — only that file is chunked and embedded, and its rows are inserted. The rest is untouched.
- **A file is edited** — it is re-chunked; chunks whose text is unchanged keep their `id` and embedding and are left as-is, genuinely new chunks are embedded and inserted, and chunks that no longer exist are deleted.
- **A file is deleted** — its rows are removed from the target automatically.

The same machinery covers **logic** changes too: tune the chunk size or swap the embedding model, and Synor compares the new output against what is already in Postgres and applies only the difference. A catch-up run (`synor update main`) does this once and exits; live mode (`synor update -L main`) keeps watching and applies each change with low latency.

## Run it

The full, runnable example is in this local workspace: examples/text_embedding. Once this clicks, Index Your Codebase is the natural next step — the same flow with syntax-aware chunking for code.
