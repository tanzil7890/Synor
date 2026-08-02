<h1 align="center">Semantic search over Markdown, stored in <em>Turbopuffer</em>.</h1>

<p align="center">
  <b>The Semantic Search 101 pipeline pointed at <em>Turbopuffer</em> — a managed, serverless vector store, so there's no database to run yourself.</b><br/>
  Walk, chunk, embed locally, upsert — incrementally — in plain async Python.
</p>

<br/>

This is Semantic Search 101 with one thing swapped: instead of storing the vectors in Postgres with pgvector, we write them to a [Turbopuffer](https://turbopuffer.com/) namespace — a managed, serverless vector store, so there's no database to run yourself. The chunking and embedding are identical; only the target changes. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so editing one file re-embeds one file, not the whole folder.

## How it works

Turbopuffer is a cloud service, so the shared resource is an `AsyncTurbopuffer` client (keyed off `TURBOPUFFER_API_KEY`) rather than a database pool. A Turbopuffer row is an `id`, a `vector`, and an open bag of `attributes` — the filename, text, and offsets ride along as attributes while the embedding is the indexed vector. Read it in [`main.py`](main.py):

```python
@syn.task
async def process_chunk(chunk, filename, id_gen, target: turbopuffer.NamespaceTarget) -> None:
    embedding_vec = await syn.use_context(EMBEDDER).embed(chunk.text)
    target.ensure_row(
        turbopuffer.Row(
            id=str(await id_gen.next_id(chunk.text)),   # stable id derived from chunk text
            vector=embedding_vec,
            attributes={"filename": str(filename), "chunk_start": chunk.start.char_offset,
                        "chunk_end": chunk.end.char_offset, "text": chunk.text},
        )
    )

@syn.task
async def app_main(sourcedir: pathlib.Path) -> None:
    target_namespace = await turbopuffer.mount_namespace_target(
        TPUF_DB, namespace_name=TPUF_NAMESPACE,
        schema=await turbopuffer.NamespaceSchema.create(vectors=turbopuffer.VectorDef(schema=EMBEDDER)),
    )
    files = localfs.walk_dir(sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]), live=True)
    await syn.spawn_each(process_file, files.items(), target_namespace)
```

`target.ensure_row` declares the row as a target state; Synor handles upserting and deleting it to match. The namespace's dimension comes straight from the embedder, so it always matches what you write.

## Why this example is useful

- **No database to run.** Turbopuffer is managed and serverless — bring an API key and the namespace is created and managed for you.
- **Managed namespace target.** A single `mount_namespace_target` handles schema, idempotent upserts, and orphan cleanup when a file disappears.
- **No hardcoded dimensions.** The namespace's vector size comes from `VectorDef(schema=EMBEDDER)`, so swapping the model carries the schema along.
- **Incremental by default.** `@syn.task(cache=True)` skips files whose content and code are unchanged; each row's `id` is derived from its chunk text, so only changed rows are upserted and vanished ones are deleted.
- **Same flow, different store.** The chunk-and-embed half is identical to the Postgres version; the query reuses the *same* local `all-MiniLM-L6-v2` embedder and asks Turbopuffer for the nearest vectors with `rank_by=("vector", "ANN", ...)`.

## Run it

**1. Get a Turbopuffer API key** — a free key from [turbopuffer.com](https://turbopuffer.com/).

**2. Configure & install:**

```sh
cp .env.example .env     # set TURBOPUFFER_API_KEY=tpuf_... (TURBOPUFFER_REGION defaults to gcp-us-central1)
pip install -e .
```

**3. Build the index** — the example ships a `markdown_files/` folder of sample docs:

```sh
synor update main          # catch-up: scan, sync, exit
synor update -L main       # live: keep watching for file changes
```

**4. Search** — embeds your query with the *same* model and asks Turbopuffer for the nearest vectors:

```sh
python main.py "what is self-attention?"
```

The most semantically similar chunks come back ranked — even when they share none of the words in your query.

---
