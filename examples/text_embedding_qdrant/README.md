<h1 align="center">Semantic search over Markdown, stored in <em>Qdrant</em>.</h1>

<p align="center">
  <b>The Semantic Search 101 pipeline with one thing swapped: the vectors land in a managed <em>Qdrant</em> collection instead of Postgres.</b><br/>
  Walk, chunk, embed locally, upsert — incrementally — in plain async Python.
</p>

<br/>

This is Semantic Search 101 with one thing changed: instead of Postgres + pgvector, the vectors land in a [Qdrant](https://qdrant.tech/) collection. Walk Markdown, chunk, embed locally with `all-MiniLM-L6-v2` — all identical — and only the target differs. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so editing one file re-embeds one file, not the whole folder.

## How it works

Read each file, split into overlapping chunks, embed each chunk, then upsert it as a Qdrant point — text and offsets in the `payload`, the embedding as the `vector`. The one Qdrant-specific call is `mount_collection_target`, which derives the vector dimensions straight from the embedder (`QdrantVectorDef(schema=EMBEDDER)` — no hardcoded `384`) and manages the collection for you. Read it in [`main.py`](main.py):

```python
@syn.fn
async def process_chunk(chunk, filename, id_gen, target: qdrant.CollectionTarget) -> None:
    embedding_vec = await syn.use_context(EMBEDDER).embed(chunk.text)
    point = qdrant.PointStruct(
        id=await id_gen.next_id(chunk.text),           # stable id derived from chunk text
        vector=embedding_vec.tolist(),
        payload={"filename": str(filename), "chunk_start": chunk.start.char_offset,
                 "chunk_end": chunk.end.char_offset, "text": chunk.text},
    )
    target.declare_point(point)

@syn.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_collection = await qdrant.mount_collection_target(
        QDRANT_DB, collection_name=QDRANT_COLLECTION,
        schema=await qdrant.CollectionSchema.create(vectors=qdrant.QdrantVectorDef(schema=EMBEDDER)),
    )
    files = localfs.walk_dir(sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]), live=True)
    await syn.mount_each(process_file, files.items(), target_collection)
```

`target.declare_point` declares the point as a target state; Synor inserts, updates, or deletes it to match — you never write upsert calls yourself.

## Why this example is useful

- **Managed Qdrant target.** A single `mount_collection_target` handles collection creation, idempotent point upserts, and orphan cleanup when a file disappears — the same managed-target guarantees pgvector gets in the base example.
- **No hardcoded dimensions.** The collection's vector size comes straight from the embedder via `QdrantVectorDef(schema=EMBEDDER)`, so swap the model and the schema follows.
- **Incremental by default.** `@syn.fn(memo=True)` on `process_file` skips files whose content and code are unchanged; each point's `id` is derived from its chunk text, so only changed points are upserted and vanished ones are deleted.
- **Same flow, different store.** The chunk-and-embed half is byte-for-byte the Postgres version — proof that the target is a swappable detail, not a rewrite.
- **gRPC for fast upserts.** The client connects over gRPC (`prefer_grpc=True`); the same local `all-MiniLM-L6-v2` embedder is reused at query time so indexing and search stay consistent.

## Run it

**1. Start Qdrant** — HTTP on `6333`, gRPC on `6334`:

```sh
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

**2. Configure & install:**

```sh
cp .env.example .env     # no required secrets; optional QDRANT_URL override (default http://localhost:6334/)
pip install -e .
```

**3. Build the index** — the example ships a `markdown_files/` folder of sample docs:

```sh
synor update main          # catch-up: scan, sync, exit
synor update -L main       # live: keep watching for file changes
```

**4. Search** — embeds your query with the *same* model and asks Qdrant for the nearest points:

```sh
python main.py "what is self-attention?"
```

You can also browse the collection in the [Qdrant dashboard](http://localhost:6333/dashboard). The most semantically similar chunks come back ranked — even when they share none of the words in your query.

---

<p align="center">
  Already running Qdrant and want your docs searchable by meaning?  — it helps a lot.<br/>
   ·  ·  · 
</p>

