<h1 align="center">Semantic search over Markdown, stored in <em>LanceDB</em>.</h1>

<p align="center">
  <b>The Semantic Search 101 pipeline pointed at <em>LanceDB</em> — an embedded, file-based vector store with no server to stand up, no <code>POSTGRES_URL</code>, just a directory on disk you can copy to move.</b><br/>
  Walk, chunk, embed locally, store — incrementally — in plain async Python.
</p>

<br/>

This is Semantic Search 101 with one thing changed: the vectors land in [LanceDB](https://lancedb.github.io/lancedb/) instead of Postgres. LanceDB is an embedded, file-based vector store — no server to stand up, just a `./lancedb_data/` directory created on first run. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so editing one file re-embeds one file, not the whole folder.

## How it works

The chunk-and-embed half is byte-for-byte the base example — `RecursiveSplitter` cuts each file into overlapping Markdown chunks, and a local `SentenceTransformerEmbedder` (`all-MiniLM-L6-v2`, no API key) turns each into a vector. What changes is the resource and the target: a `LanceAsyncConnection` instead of an `asyncpg` pool, and `lancedb.mount_table_target` instead of the Postgres one — same call shape, same `table.declare_row(...)`. Read it in [`main.py`](main.py):

```python
@syn.lifespan
async def synor_lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
    conn = await lancedb.connect_async(LANCEDB_URI)   # the "connection" is just a path on disk
    builder.provide(LANCE_DB, conn)
    builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
    yield

@syn.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await lancedb.mount_table_target(
        LANCE_DB, table_name=TABLE_NAME,
        table_schema=await lancedb.TableSchema.from_class(DocEmbedding, primary_key=["id"]),
    )
    files = localfs.walk_dir(sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]), live=True)
    await syn.mount_each(process_file, files.items(), target_table)
```

`lancedb.mount_table_target` is the LanceDB counterpart to the Postgres `mount_table_target`: it creates and manages the table, handles idempotent upserts keyed on the primary key, and cleans up orphan rows when a file disappears. Only the import changed.

## Why this example is useful

- **Zero infrastructure.** No database to install, no `POSTGRES_URL` — LanceDB writes to `./lancedb_data/`, created on first run. To start fresh, delete the directory and re-run.
- **Portable by design.** Data lives in one directory on disk; copy it to move the whole index.
- **Managed table target.** `lancedb.mount_table_target` owns the schema, idempotent upserts, and orphan cleanup — the same guarantees the Postgres target gives, against a local store.
- **Incremental by default.** `@syn.fn(memo=True)` skips files whose content and code are unchanged; each row's `id` is derived from its chunk text, so only changed rows are upserted and vanished ones are deleted.
- **Same flow, different store.** The chunk-and-embed code is identical to the Postgres version — proof the target is a swappable detail. The same local embedder is reused at query time so indexing and search stay consistent.

## Run it

> No database to install — LanceDB is embedded and writes to `./lancedb_data/`, created on first run.

**1. Configure & install:**

```sh
cp .env.example .env     # no required secrets; optional LANCEDB_URI override
pip install -e .
```

**2. Build the index** — the example ships a `markdown_files/` folder of sample docs:

```sh
synor update main          # catch-up: scan, sync, exit
synor update -L main       # live: keep watching for file changes
```

**3. Search** — embeds your query with the *same* model and returns the nearest vectors via LanceDB's async search:

```sh
python main.py "what is self-attention?"
```

The most semantically similar chunks come back ranked — even when they share none of the words in your query.

---

