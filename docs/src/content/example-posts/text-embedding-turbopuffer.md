---
title: Semantic Search with *Turbopuffer*
description: 'The Semantic Search 101 pipeline with Synor V1, pointed at a managed vector store — chunk Markdown, embed each chunk, and upsert the vectors into a Turbopuffer namespace.'
slug: text-embedding-turbopuffer
image: /docs/images/synor-mark.svg
tags: [vector-index, turbopuffer]
---



This is the Semantic Search 101 example with one thing swapped: instead of storing the vectors in Postgres with pgvector, we write them to a [Turbopuffer](https://turbopuffer.com/) namespace — a managed, serverless vector store, so there's no database to run yourself. The chunking and embedding are identical; only the target changes.

Everything else stays the same: ordinary `async` Python and your own types, with incremental processing, change tracking, and managed targets running in the Rust engine underneath — so when a file changes, only the affected chunks get re-embedded and re-upserted.


## Flow overview



From a high level, these are the steps:

1. Read Markdown files from a local directory.
2. Split each file into overlapping chunks, then embed every chunk.
3. Upsert each chunk and its embedding as a row in a Turbopuffer namespace (as target states).

You declare the transformation logic with native Python, without worrying about how updates propagate. Think: **each work unit declares the outcomes it owns**.

For the chunk-and-embed half of the pipeline — `process_file`, `RecursiveSplitter`, and `SentenceTransformerEmbedder` — read the base walkthrough. Here we focus on the one part that differs: the Turbopuffer target.

## Set up the Turbopuffer client

Turbopuffer is a cloud service, so the shared resource the pipeline needs is an API client rather than a database pool. We provide an `AsyncTurbopuffer` client in the lifespan, alongside the embedder, keyed off the `TURBOPUFFER_API_KEY` env var.

```python title="main.py"
TPUF_REGION = os.environ.get("TURBOPUFFER_REGION", "gcp-us-central1")
TPUF_NAMESPACE = "TextEmbedding"

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
TPUF_DB = syn.ContextKey[turbopuffer.AsyncTurbopuffer]("text_embedding_turbopuffer")
EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)


@syn.lifespan
async def synor_lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
    api_key = os.environ.get("TURBOPUFFER_API_KEY")
    if not api_key:
        raise RuntimeError("TURBOPUFFER_API_KEY is not set")
    client = turbopuffer.AsyncTurbopuffer(region=TPUF_REGION, api_key=api_key)
    builder.provide(TPUF_DB, client)
    builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
    yield
```

## Declare a row in the namespace

A Turbopuffer row is an `id`, a `vector`, and an open bag of `attributes`. Instead of a typed table column per field, the filename, text, and offsets ride along as attributes — the embedding is the indexed vector.

```python title="main.py"
@syn.task
async def process_chunk(
    chunk: Chunk,
    filename: pathlib.PurePath,
    id_gen: IdGenerator,
    target: turbopuffer.NamespaceTarget,
) -> None:
    embedding_vec = await syn.use_context(EMBEDDER).embed(chunk.text)
    target.ensure_row(
        turbopuffer.Row(
            id=str(await id_gen.next_id(chunk.text)),
            vector=embedding_vec,
            attributes={
                "filename": str(filename),
                "chunk_start": chunk.start.char_offset,
                "chunk_end": chunk.end.char_offset,
                "text": chunk.text,
            },
        )
    )
```

`target.ensure_row` declares the row as a target state; Synor handles upserting and deleting it to match. The `id` is derived from the chunk's text, so unchanged chunks keep their id and embedding across runs.

## Mount the namespace target

`app_main` mounts the namespace, then fans out one processing component per file. The vector schema comes straight from the embedder, so the namespace's dimension matches what we write.

```python title="main.py"
@syn.task
async def app_main(sourcedir: pathlib.Path) -> None:
    target_namespace = await turbopuffer.mount_namespace_target(
        TPUF_DB,
        namespace_name=TPUF_NAMESPACE,
        schema=await turbopuffer.NamespaceSchema.create(
            vectors=turbopuffer.VectorDef(schema=EMBEDDER),
        ),
    )
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live=True,  # watch for changes; pass -L to `synor update` to run live
    )
    await syn.spawn_each(process_file, files.items(), target_namespace)
```

`mount_namespace_target` creates and manages the Turbopuffer namespace for you: schema, idempotent upserts, and orphan cleanup when a file disappears. `live=True` makes the filesystem source watch for changes, and `spawn_each` runs one component per file so the engine can track and update them independently.

## Setup and run

- A free [Turbopuffer](https://turbopuffer.com/) API key. Copy `.env.example` to `.env` and fill it in (`TURBOPUFFER_REGION` defaults to `gcp-us-central1`):

  ```sh
  export TURBOPUFFER_API_KEY="tpuf_..."
  ```

- Install Synor with the Turbopuffer and embedding extras:

  ```sh
  pip install -U "synor[turbopuffer,sentence_transformers]" numpy python-dotenv
  ```

- A few `.md` files in a `markdown_files/` directory — grab the sample file from the repo or drop in your own.

Run the `synor` CLI to build and update the index, then query it from the command line:

```sh
# Catch-up run: scan, sync, exit
synor update main

# Live run: keep watching for file changes
synor update -L main

# Search the namespace
python main.py "what is self-attention?"
```

The query embeds your text with the *same* model and asks Turbopuffer for the nearest vectors with `rank_by=("vector", "ANN", ...)`, so indexing and querying stay consistent. The most semantically similar chunks come back ranked — even when they share none of the words in your query.

## Incremental updates

Synor keeps the namespace in sync with your files and does the **minimum work** to get there — you never compute a diff. `@syn.task(cache=True)` on `process_file` decides what to *recompute* (a file is skipped when its content and the function's code are unchanged), and `mount_namespace_target` decides what to *write* (each row's `id` is derived from its chunk's text, so only changed rows are upserted and rows whose source is gone are deleted).

- **A file is added** — only that file is chunked and embedded, and its rows are upserted. The rest is untouched.
- **A file is edited** — it is re-chunked; unchanged chunks keep their `id` and embedding, new chunks are embedded and upserted, and chunks that no longer exist are deleted.
- **A file is deleted** — its rows are removed from the namespace automatically.

The same machinery covers **logic** changes: tune the chunk size or swap the embedding model, and Synor applies only the difference. A catch-up run (`synor update main`) does this once and exits; live mode (`synor update -L main`) keeps watching.

## Run it
