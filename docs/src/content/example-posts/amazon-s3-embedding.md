---
title: Embed Markdown from *Amazon S3*
description: 'The Semantic Search 101 pipeline with Amazon S3 as the source instead of a local folder — list Markdown objects from a bucket, chunk and embed each one, and store the vectors in Postgres with pgvector.'
slug: amazon-s3-embedding
image: /docs/images/synor-mark.svg
tags: [vector-index, amazon-s3]
---



This is the Semantic Search 101 example with one thing swapped: the source is an **Amazon S3 bucket** instead of a local directory. Everything downstream — chunking, embedding, the Postgres/pgvector target, and the query — is identical, so this post spends its words on the part that differs: the S3 connector, its env vars, and the catch-up run.

If you haven't read the base example yet, start there — it walks through the chunk-and-embed flow line by line. Here we'll move fast.


## Flow overview



From a high level, these are the steps:

1. List Markdown objects from an Amazon S3 bucket (filtered by prefix and glob).
2. Split each file into overlapping chunks, then embed every chunk.
3. Store the chunks and their embeddings in Postgres (as target states).

You declare the transformation logic with native Python, without worrying about how updates propagate. Think: **each work unit declares the outcomes it owns**.

The chunk-and-embed half of this pipeline is unchanged from Semantic Search 101 — same `RecursiveSplitter`, same `SentenceTransformerEmbedder` with `all-MiniLM-L6-v2`, same `DocEmbedding` row written to pgvector. The only new piece is the connector.

## Provide an S3 client

The S3 connector needs an [aiobotocore](https://github.com/aio-libs/aiobotocore) client. We open it once in the lifespan alongside the Postgres pool and embedder, and share it through a `ContextKey`.

```python title="main.py"
DATABASE_URL = os.getenv("POSTGRES_URL", "postgres://synor:synor@localhost/synor")
TABLE_NAME = "amazon_s3_doc_embeddings"
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.getenv("S3_PREFIX", "")

PG_DB = syn.ContextKey[asyncpg.Pool]("s3_embedding_db")
S3_CLIENT = syn.ContextKey[AioBaseClient]("s3_client")
EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)


@syn.lifespan
async def synor_lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))

        session = aiobotocore.session.get_session()
        async with session.create_client("s3") as s3_client:
            builder.provide(S3_CLIENT, s3_client)
            yield
```

`create_client("s3")` picks up standard AWS credentials — env vars, `~/.aws/credentials`, or an IAM role. Set `AWS_ENDPOINT_URL` to point at an S3-compatible service like MinIO.

## List objects from the bucket

`app_main` mounts the Postgres table exactly as in the base example, then swaps `localfs.walk_dir` for `amazon_s3.list_objects` — same `path_matcher` glob, same `spawn_each` fan-out.

```python title="main.py"
@syn.task
async def app_main() -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            DocEmbedding, primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    client = syn.use_context(S3_CLIENT)
    files = amazon_s3.list_objects(
        client,
        S3_BUCKET,
        prefix=S3_PREFIX,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await syn.spawn_each(process_file, files.items(), target_table)
```

`list_objects` yields one `S3File` per matching object; `prefix` scopes the listing server-side, and the glob filters the rest. `process_file` then reads, chunks, and embeds each one — that code is identical to the base example, so see it there.

## Setup

- A running Postgres with the [pgvector](https://github.com/pgvector/pgvector) extension.

  ```sh
  export POSTGRES_URL="postgres://synor:synor@localhost/synor"
  ```

- An S3 bucket with some `.md` files, plus AWS credentials and the bucket name:

  ```sh
  export S3_BUCKET="my-bucket"
  export S3_PREFIX="markdown_files/"   # optional: scope the listing
  ```

- Install Synor with the `amazon_s3` extra and the example's dependencies:

  ```sh
  pip install -U "synor[amazon_s3,postgres,sentence_transformers]" asyncpg pgvector numpy python-dotenv
  ```

## Run the pipeline

The `amazon_s3` source does not support live mode, so this is a one-shot catch-up run — scan the bucket, sync, exit:

```sh
synor update main
```

Then search straight from the command line, reusing the same embedder so indexing and querying stay consistent:

```sh
python main.py "what is self-attention?"
```

The most semantically similar chunks come back ranked — even when they share none of the words in your query.

## Incremental updates

Synor keeps the index in sync with the bucket and does the **minimum work** to get there. `@syn.task(cache=True)` decides what to *recompute* — a file is skipped when its content and the function's code are both unchanged — and `mount_table_target` decides what to *write*, deriving each row's `id` from its chunk text so it upserts only the rows that actually changed and deletes rows whose source object is gone.

- **An object is added** — only that file is chunked and embedded; the rest is untouched.
- **An object is edited** — it is re-chunked; unchanged chunks keep their `id` and embedding, new chunks are embedded and inserted, and stale ones are deleted.
- **An object is deleted** — its rows are removed from the target automatically.

Because S3 is catch-up only, you re-run `synor update main` to pick up bucket changes; the engine still applies just the difference.

## Run it
