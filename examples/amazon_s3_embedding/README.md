<h1 align="center">Semantic search over Markdown in an <em>S3 bucket</em>.</h1>

<p align="center">
  <b>The Semantic Search 101 pipeline with one thing swapped: the source is an <em>Amazon S3</em> bucket instead of a local folder.</b><br/>
  List objects, chunk, embed locally, store in Postgres + pgvector — incrementally — in plain async Python.
</p>

<br/>

This is Semantic Search 101 with one thing swapped: the source is an Amazon S3 bucket instead of a local directory. Everything downstream — chunking, embedding, the Postgres/pgvector target, and the query — is identical. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so adding one object embeds one object, not the whole bucket.

## How it works

The S3 connector needs an [aiobotocore](https://github.com/aio-libs/aiobotocore) client, opened once in the lifespan alongside the Postgres pool and embedder. `app_main` mounts the Postgres table exactly as in the base example, then swaps `localfs.walk_dir` for `amazon_s3.list_objects` — same `path_matcher` glob, same `mount_each` fan-out. Read it in [`main.py`](main.py):

```python
@syn.fn
async def app_main() -> None:
    target_table = await postgres.mount_table_target(
        PG_DB, table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(DocEmbedding, primary_key=["id"]),
        pg_schema_name=PG_SCHEMA_NAME,
    )
    client = syn.use_context(S3_CLIENT)
    files = amazon_s3.list_objects(
        client, S3_BUCKET, prefix=S3_PREFIX,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await syn.mount_each(process_file, files.items(), target_table)
```

`list_objects` yields one `S3File` per matching object; `prefix` scopes the listing server-side, and the glob filters the rest. `process_file` then reads, chunks, and embeds each one — identical to the base example. `create_client("s3")` picks up standard AWS credentials (env vars, `~/.aws/credentials`, or an IAM role); set `AWS_ENDPOINT_URL` to point at an S3-compatible service like MinIO.

## Why this example is useful

- **S3 as a first-class source.** `amazon_s3.list_objects` drops into the same `mount_each` fan-out as a local folder — the source is a swappable detail, not a rewrite.
- **Scoped listing.** `prefix` filters server-side and the `**/*.md` glob filters the rest, so you index only what you mean to.
- **S3-compatible too.** Point `AWS_ENDPOINT_URL` at MinIO or any S3-compatible service; credentials come from the standard AWS chain.
- **Incremental by default.** `@syn.fn(memo=True)` skips objects whose content and code are unchanged; each row's `id` is derived from its chunk text, so re-running upserts only changed rows and deletes rows whose source object is gone.
- **Managed Postgres target.** A single `mount_table_target` owns the schema, idempotent upserts, and orphan cleanup; the same local `all-MiniLM-L6-v2` embedder is reused at query time so indexing and search stay consistent.

## Run it

**1. Start Postgres + pgvector:**

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install** — set the bucket, optional prefix, and your AWS credentials:

```sh
cp .env.example .env     # set S3_BUCKET, optional S3_PREFIX; AWS creds from env / ~/.aws / IAM role
pip install -e .
```

**3. Build the index** — the `amazon_s3` source does not support live mode, so this is a one-shot catch-up run (scan the bucket, sync, exit):

```sh
synor update main
```

**4. Search** — embeds your query with the *same* model and returns the nearest chunks by pgvector cosine distance:

```sh
python main.py "what is self-attention?"
```

The most semantically similar chunks come back ranked — even when they share none of the words in your query. Re-run `synor update main` to pick up bucket changes; the engine still applies just the difference.

---
