<h1 align="center">Index PDFs and images <em>together</em>, no parsing.</h1>

<p align="center">
  <b>Render every PDF page to an image, embed pages and images alike with multi-vector ColPali, and retrieve the most relevant <em>page</em> with MaxSim — whatever format it came from.</b><br/>
  No OCR, no text extraction, no brittle per-format parsers — in plain async Python.
</p>

<br/>

Real document sets are a mix — scanned reports, slide exports, screenshots, and PDFs all jumbled together. Parsing each format into clean text is brittle and loses the layout (tables, charts, figures) that often *is* the answer. This pipeline sidesteps parsing entirely: render every PDF page to an image, embed pages and standalone images alike with the multi-vector [ColPali](https://huggingface.co/vidore/colpali-v1.2) model, and store them in one [Qdrant](https://qdrant.tech/) collection. You declare the transformation in native Python — each work unit declares the outcomes it owns — the slow per-page inference runs on a GPU runner, and the Rust engine handles incremental processing, so adding a document embeds only its pages.

## How it works

A file fans out to **pages**, so the shape is *file → N pages → N points*:

- **Walk** a folder of PDFs and images (live), matching `.pdf` / `.jpg` / `.jpeg` / `.png`.
- **Split** each file into pages — a PDF renders to one image per page via [`pdf2image`](https://github.com/Belval/pdf2image); a standalone image is a single page; anything else is skipped.
- **Embed** every page with ColPali into a multi-vector and store one MaxSim Qdrant point per page, tagged with filename and page number.

One file-splitting function handles every format, and `process_file` fans each page out with `syn.map`. Read it in [`main.py`](main.py):

```python
@syn.fn.as_async(runner=syn.GPU)
def file_to_pages(filename: str, content: bytes) -> list[Page]:
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type == "application/pdf":
        return [Page(page_number=i + 1, image=_to_png(img))
                for i, img in enumerate(convert_from_bytes(content, dpi=PDF_RENDER_DPI))]
    if mime_type and mime_type.startswith("image/"):
        return [Page(page_number=None, image=content)]
    return []

@syn.fn(memo=True)   # unchanged file is never re-rendered or re-embedded
async def process_file(file: FileLike, target: qdrant.CollectionTarget) -> None:
    filename = str(file.file_path.path)
    pages = await file_to_pages(filename, await file.read())
    await syn.map(process_page, pages, filename, target)   # one point per page
```

The Qdrant collection is declared with a `MultiVectorSchema` and `multivector_comparator="max_sim"`, so a text query is scored against the *best-matching patch* of each page — the same query reaches pages from PDFs and standalone images alike.

## Why this example is useful

- **One index, every format.** PDFs and images funnel into the same Qdrant collection through one `file_to_pages` path — a query reaches them all, no per-format retrievers.
- **No parsing, no OCR.** ColPali embeds the rendered *page image*, so tables, charts, and figures stay intact — exactly the layout that OCR-and-embed throws away.
- **MaxSim multi-vector retrieval.** A `MultiVectorSchema` + `max_sim` comparator scores a query against each page's best-matching patches, late-interaction style.
- **Fan-out done right.** A file expands to N pages with `syn.map`, each its own point keyed by `(filename, page)` — re-running reconciles cleanly instead of duplicating.
- **Incremental on a GPU runner.** Slow per-page inference runs on `syn.GPU`; `@syn.fn(memo=True)` means adding a document embeds only its pages and leaves the rest untouched.

## Run it

> Needs **Qdrant** plus the ColPali deps (`torch`, `transformers`, `pdf2image`). `pdf2image` needs **poppler** installed for PDF rendering (`brew install poppler` / `apt install poppler-utils`).

**1. Start Qdrant:**

```sh
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

**2. Configure & install:**

```sh
cp .env.example .env     # QDRANT_URL (defaults to the local container above)
pip install -e .
```

**3. Build the index** — the example ships a `source_files/` folder mixing PDFs (papers) and images (financial report pages). A PDF expands to one point per page (the sample BERT paper alone is 16 pages):

```sh
synor update main        # or: synor update -L main   (keep watching the folder)
```

**4. Search across formats** — embed a text query with ColPali; the same query reaches pages from PDFs and standalone images alike:

```sh
python main.py "revenue growth"
```

On the sample set, *"revenue growth"* ranks the two financial-report images at the top (Sweetgreen, then Restaurant Brands), above an unrelated healthcare page — MaxSim matching the query against the most relevant patches of each page, with zero text extraction.

---
