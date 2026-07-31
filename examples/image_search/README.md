<h1 align="center">Search a photo folder by <em>meaning</em>, not tags.</h1>

<p align="center">
  <b>CLIP embeds images <em>and</em> text into the <em>same</em> vector space — so "long neck" lands next to the giraffe, with no captions, no labels, no manual tagging.</b><br/>
  Vectors live in Qdrant, the index runs live inside a FastAPI app, and it's all plain async Python.
</p>

<br/>

A folder of photos is searchable by *meaning* the moment you stop relying on filenames and tags. [CLIP](https://huggingface.co/openai/clip-vit-large-patch14) is the trick: it embeds an image and its caption into the *same* space, so a text query and a matching picture land near each other. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, the managed Qdrant collection) runs in a Rust engine underneath, in **live mode** inside the API server, so dropping a new photo into the folder updates the index within a second.

## How it works

The indexing path is short — there's no text to chunk, just one embedding per image:

- **Walk** a local image folder (live), matching `.jpg` / `.jpeg` / `.png`.
- **Embed** each image with the CLIP image encoder.
- **Store** the vector as a Qdrant point, keyed by a stable `uuid5` of the path, with the filename in the payload.

The whole point is one shared space: the **same** CLIP model embeds images at index time and text at query time, so a cosine search with a text vector finds the nearest *image* vectors. Each image runs as its own processing component, so delete a photo and its point is removed automatically. Read it in [`pipeline.py`](pipeline.py):

```python
@syn.fn(memo=True)   # unchanged image is never re-embedded
async def process_file(file: FileLike, target: qdrant.CollectionTarget) -> None:
    content = await file.read()
    embedding = embed_image_bytes(content)
    point = qdrant.PointStruct(
        id=_image_id(file.file_path.path),                  # uuid5 of the path — stable
        vector=embedding,
        payload={"filename": str(file.file_path.path)},
    )
    target.declare_point(point)

def embed_query(text: str) -> list[float]:                  # query side — same model, text encoder
    model, processor = get_clip_model()
    inputs = processor(text=[text], return_tensors="pt", padding=True)
    with torch.no_grad():
        out = model.get_text_features(**inputs)
    return _projected_features(out)[0].tolist()
```

`api.py` is a FastAPI app whose [lifespan](https://fastapi.tiangolo.com/advanced/events/) starts the flow in live mode, blocks startup until the initial sweep is `READY`, then keeps watching `img/` while serving `/search`. There's no separate "build the index" step.

## Why this example is useful

- **One model, two encoders.** CLIP embeds images at index time and text at query time into the *same* 768-d space — search matches by meaning, never by metadata.
- **Live by default.** The flow runs in live mode inside the API server; drop a photo into `img/` and it's searchable within a second, no rebuild step.
- **Incremental & self-cleaning.** `@syn.fn(memo=True)` skips unchanged images; each photo is its own processing component, so deleting one removes its Qdrant point automatically.
- **Managed Qdrant target.** `mount_collection_target` creates and reconciles the collection — the vector size comes straight from `model.config.projection_dim`, so swapping CLIP variants just works.
- **Plain Python, your stack.** FastAPI + React + Qdrant, no DSL — the indexing logic is a handful of ordinary async functions.

## Run it

> Needs **Qdrant** (vector store) and the CLIP model deps (`torch`, `transformers`, `pillow`), all pulled in by `pip install -e .`.

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

Query *"long neck"* and the giraffe ranks first, then the other animals by CLIP similarity — none of which was ever tagged with a word. That's the whole point of a shared image-text space: the match is by *meaning*.

---
