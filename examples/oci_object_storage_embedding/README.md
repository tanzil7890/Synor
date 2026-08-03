<h1 align="center">Semantic search over an <em>OCI</em> Object Storage bucket.</h1>

<p align="center">
  <b>List Markdown objects from an Oracle Cloud bucket, <em>chunk</em> and <em>embed</em> each one, and store the vectors in Postgres pgvector — kept <em>live</em> off OCI Streaming.</b><br/>
  It's Semantic Search 101 with the source swapped for a bucket — in plain async Python.
</p>

<br/>

Most documents already live in object storage, not on your laptop. This pipeline lists Markdown objects from an OCI Object Storage bucket, splits each into overlapping chunks, embeds them with sentence-transformers, and stores the vectors in Postgres with pgvector. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so adding one object embeds one object, not the whole bucket.

## How it works

The chunk → embed → store half is identical to Semantic Search 101; the part that differs is the source. The OCI SDK is synchronous and you create the client yourself, so the example builds one from a config-file profile, hands it to the context, and lists objects with `oci_object_storage.list_objects` — the OCI analogue of `localfs.walk_dir`. Live mode is opt-in: when the four `OCI_STREAMING_*` env vars are set, it uses `kafka.create_consumer()` to build a Kafka-protocol consumer against OCI Streaming and passes it through as a `LiveStream[bytes]`. That transfer makes the stream responsible for draining, unsubscribing, and closing the helper-created consumer. Read it in [`main.py`](main.py):

```python
@syn.task
async def app_main() -> None:
    target_table = await postgres.mount_table_target(
        PG_DB, table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(DocEmbedding, primary_key=["id"]),
        pg_schema_name=PG_SCHEMA_NAME,
    )
    client = syn.use_context(OCI_CLIENT)

    # Live mode is opt-in: build a LiveStream[bytes] from OCI Streaming if configured.
    consumer = _build_streaming_consumer()
    live_stream = None
    if consumer is not None and OCI_STREAMING_TOPIC is not None:
        live_stream = kafka.topic_as_stream(consumer, [OCI_STREAMING_TOPIC]).payloads()

    files = oci_object_storage.list_objects(
        client, OCI_NAMESPACE, OCI_BUCKET, prefix=OCI_PREFIX,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live_stream=live_stream,
    )
    await syn.spawn_each(process_file, files.items(), target_table)
```

With `live_stream=None` (the default), `list_objects` does a one-shot catch-up scan. Pass a stream and the connector keeps watching, re-reading each post-cutoff object to apply an authoritative update or delete. `spawn_each` runs one processing component per object so the engine tracks each independently.

## Why this example is useful

- **Swap the source, keep the flow.** Only the source line changes from the local-folder example — `process_file` takes an `oci_object_storage.OCIFile` and reads it with `await file.read_text()`, just like a local `FileLike`.
- **Live without re-scanning.** OCI Streaming is Kafka-compatible, so object create/update/delete events ride the Kafka connector and drive incremental updates with no full bucket re-scan.
- **Authoritative, not event-trusting.** For each accepted event the connector re-reads the object (`head_object`) to determine current state, then issues an update (present) or delete (404) — the event type is never trusted as the dispatch signal.
- **Incremental by default.** `@syn.task(cache=True)` skips an object whose content and code are unchanged; `mount_table_target` upserts only changed rows and deletes rows whose source is gone.
- **Plain Python, your stack.** Local sentence-transformer embedder, no API key; swap `EMBED_MODEL` for any of the 12k+ models on Hugging Face.

## Run it

**1. Start Postgres + pgvector** (the repo ships a compose file):

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install** — point at a bucket with Markdown objects and an [OCI config file](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm) (default `~/.oci/config`):

```sh
cp .env.example .env     # set POSTGRES_URL, OCI_NAMESPACE, OCI_BUCKET (optional OCI_PREFIX)
pip install -e .
```

For live mode, also set `OCI_STREAMING_BOOTSTRAP_SERVERS`, `OCI_STREAMING_TOPIC`, `OCI_STREAMING_USERNAME`, and `OCI_STREAMING_AUTH_TOKEN` in `.env` (the `.env.example` documents each format). With those unset, the connector skips the subscription and just does the catch-up scan.

**3. Build the index** — catch-up (scan, sync, exit) or live (catch up, then keep watching the topic):

```sh
synor update main        # catch-up
synor update -L main     # live
```

**4. Search from the command line:**

```sh
python main.py "what is self-attention?"
```

This example keeps it minimal and doesn't declare a vector index, so queries do a sequential scan — fine for a few objects. For a larger corpus, add `target_table.declare_vector_index(column="embedding")` exactly as Semantic Search 101 does.

---
