<h1 align="center">Search your photos <em>by face</em>.</h1>

<p align="center">
  <b>Detect every face in a folder of photos, embed each into a 128-d vector with <code>face_recognition</code> (dlib), and index them in Qdrant — then a query face finds the same person across photos.</b><br/>
  The core of "find every photo of this person," with no labels or tags — in plain async Python.
</p>

<br/>

A folder of group photos has every person hiding in plain sight — the same face shows up across shots, but that knowledge is locked in pixels. This pipeline makes it searchable: detect every face, crop it, embed it into a 128-d vector with [`face_recognition`](https://github.com/ageitgey/face_recognition) (dlib), and index the faces in [Qdrant](https://qdrant.tech/). You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — while incremental processing, change tracking, and the managed Qdrant collection run in a Rust engine underneath, and the slow detection/embedding steps run on a GPU runner instead of blocking the event loop.

## How it works

Unlike a one-embedding-per-image index, an image here fans out to **many** faces — so the shape is *image → N faces → N points*:

- **Walk** a local image folder (live), matching `.jpg` / `.jpeg` / `.png`.
- **Detect** every face in each image (CNN detector, downscaling large images first), and crop it.
- **Embed** each face into a 128-d vector and store one Qdrant point per face, keyed by `(filename, bounding box)`, with the source filename and box in the payload.

The dlib calls are synchronous and CPU/GPU-heavy, so each is wrapped with `@syn.fn.as_async(runner=syn.GPU)`. `process_file` detects a photo's faces, then maps each through `process_face` with `syn.map`. Read it in [`main.py`](main.py):

```python
@syn.fn
async def process_face(face: Face, filename: str, target: qdrant.CollectionTarget) -> None:
    embedding = await embed_face(face.image)
    target.declare_point(
        qdrant.PointStruct(
            id=_face_id(filename, face.rect),     # uuid5 of (filename, box) — stable
            vector=embedding,
            payload={"filename": filename, "min_x": face.rect.min_x, "min_y": face.rect.min_y,
                     "max_x": face.rect.max_x, "max_y": face.rect.max_y},
        )
    )

@syn.fn(memo=True)   # unchanged photo is never re-detected
async def process_file(file: FileLike, target: qdrant.CollectionTarget) -> None:
    faces = await extract_faces(await file.read())
    await syn.map(process_face, faces, str(file.file_path.path), target)
```

The collection is sized to the 128-d face vector with **Euclidean** distance — dlib's own rule of thumb is that a distance under ~0.6 means "same person."

## Why this example is useful

- **Image → many faces.** Each photo fans out to one Qdrant point per detected face with `syn.map` — the multi-face equivalent of chunking a document.
- **Recognition without labels.** dlib's 128-d encodings put the same person close together; a Euclidean search under ~0.6 means "same person," with no tags or training.
- **The box travels with the match.** Each point's payload carries the bounding box, so a search hit tells you *where* in the source image the face is.
- **Incremental & self-cleaning.** `@syn.fn(memo=True)` skips unchanged photos; each image is its own processing component, so deleting a photo removes all its faces from Qdrant automatically.
- **Heavy work off the event loop.** CNN detection and embedding run on a `syn.GPU` runner; large images are downscaled for detection, then boxes are mapped back to full size.

## Run it

> Needs **Qdrant** plus the `face_recognition` library (it depends on **dlib** — see its [install notes](https://github.com/ageitgey/face_recognition#installation) if the build needs CMake/boost).

**1. Start Qdrant:**

```sh
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

**2. Configure & install:**

```sh
cp .env.example .env     # QDRANT_URL (defaults to the local container above)
pip install -e .
```

**3. Build the index** — the example ships a handful of famous group photos in `images/` (the 1927 Solvay physics conference, Steve Jobs & Bill Gates, …):

```sh
synor update main        # or: synor update -L main   (keep watching the folder)
```

On the sample set this indexes **36 faces** — 29 from the Solvay conference alone — each a Qdrant point keyed by `(filename, bounding box)`.

**4. Search by face** — embed a query face the same way and find the nearest indexed faces:

```sh
python main.py query images/einplanck3.jpg
```

Because Einstein appears in *both* the Einstein–Planck photo and the Solvay conference, the query pulls his Solvay face back as a close match — a Euclidean distance around `0.46`, comfortably under dlib's ~0.6 same-person threshold. That's face recognition across photos, with no labels or tags.

---
