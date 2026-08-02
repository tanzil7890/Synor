<h1 align="center">Image search with <em>multi-vector</em> ColPali.</h1>

<p align="center">
  <b>Instead of one vector per image, ColPali emits a <em>bag</em> of patch vectors — and Qdrant's MaxSim scores a query against each image's best-matching patches, late-interaction style.</b><br/>
  Finer-grained retrieval on dense, text-heavy, busy images — live, in plain async Python.
</p>

<br/>

This is the multi-vector cousin of the CLIP image search example. Same idea — type *"long neck"*, get the giraffe back — but instead of squeezing each image into a *single* vector, [ColPali](https://huggingface.co/vidore/colpali-v1.2) emits a *bag* of vectors, one per image patch, and matches a query token-against-patch. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — while incremental processing, change tracking, and the managed Qdrant collection run in a Rust engine underneath, in **live mode** inside the API server. The cost is more vectors per image; the payoff is retrieval that holds up on dense, text-heavy, or busy images where a single embedding blurs everything together.

## How it works

The indexing path is short — there's no text to chunk, just one *multi-vector* embedding per image:

- **Walk** a local image folder (live), matching `.jpg` / `.jpeg` / `.png`.
- **Embed** each image with ColPali into a list of 128-d patch vectors — `list[list[float]]`, not one vector.
- **Store** it as a point in a Qdrant **multivector** collection configured for **MaxSim**, keyed by a stable `uuid5` of the path.

The store does the heavy lifting on the query side: the query's bag of token vectors and an image's bag of patch vectors are scored late-interaction style — each query vector finds its best-matching patch, summed across the query. The only difference from the CLIP version is the *shape* of the embedding. Read it in [`pipeline.py`](pipeline.py):

```python
@syn.task(cache=True)   # unchanged image is never re-embedded
async def process_file(file: FileLike, target: qdrant.CollectionTarget) -> None:
    content = await file.read()
    embedding = embed_image_bytes(content)              # list[list[float]] — multi-vector
    point = qdrant.PointStruct(
        id=_image_id(file.file_path.path),              # uuid5 of the path — stable
        vector=embedding,
        payload={"filename": str(file.file_path.path)},
    )
    target.declare_point(point)

# the collection itself carries the multi-vector setup:
schema = await qdrant.CollectionSchema.create(
    vectors=qdrant.QdrantVectorDef(
        schema=MultiVectorSchema(vector_schema=VectorSchema(dtype=np.dtype(np.float32), size=dim)),
        distance="cosine",
        multivector_comparator="max_sim",               # late-interaction MaxSim
    )
)
```

`api.py` is a FastAPI app whose [lifespan](https://fastapi.tiangolo.com/advanced/events/) starts the flow in live mode, blocks startup until the initial sweep is `READY`, then watches `img/` while serving `/search` — which hands Qdrant the query's *bag* of vectors and lets it do the MaxSim scoring.

## Why this example is useful

- **Multi-vector, not single.** ColPali emits a vector per patch and matches a query patch-by-patch — finer-grained than a single CLIP embedding on dense or text-heavy images.
- **MaxSim in the store.** A `MultiVectorSchema` + `multivector_comparator="max_sim"` makes Qdrant do the late-interaction scoring; the query side just hands over the query's bag of vectors.
- **Live by default.** The flow runs in live mode inside the API server; a new photo in `img/` is searchable within a second, no rebuild step.
- **Incremental & self-cleaning.** `@syn.task(cache=True)` skips unchanged images; each photo is its own processing component, so deleting one removes its Qdrant point automatically.
- **Drop-in swap from CLIP.** Same Qdrant target, same fan-out, same FastAPI + React app — only the encoder and the collection's vector schema change.

## Run it

> Needs **Qdrant** (vector store) plus the ColPali model deps (`torch`, `transformers`, `pillow`), all pulled in by `pip install -e .` (`synor[colpali,qdrant]`).

**1. Start Qdrant:**

```sh
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

**2. Configure & install:**

```sh
cp .env.example .env     # QDRANT_URL (defaults to the local container above)
pip install -e .
```

**3. Run it as a service** — the example ships an `img/` folder (a cat, a dog, an elephant, a giraffe). The server runs the index in live mode in the background and blocks startup until the first sweep finishes, so there's no separate indexing command:

```sh
python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

**4. Open the frontend:**

```sh
cd frontend && npm install && npm run dev   # http://localhost:5173
```

The React app posts your query to `/search`, which embeds the text into ColPali's per-token space and runs a MaxSim search in Qdrant — the match is by *meaning*, patch by patch, never by metadata.

---
