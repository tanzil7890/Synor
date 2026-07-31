<h1 align="center">Semantic search over a folder of <em>PDFs</em>.</h1>

<p align="center">
  <b>Convert each PDF to Markdown with <em>docling</em> on a GPU runner, <em>chunk</em> and <em>embed</em> it, and store the vectors in Postgres pgvector.</b><br/>
  Papers, RFCs, manuals, contracts — searchable in plain English, in plain async Python.
</p>

<br/>

Take a folder of PDFs and turn it into a [vector index](https://github.com/pgvector/pgvector) you can search in plain English. The trick PDFs add over plain text: they have to be *parsed* first. This pipeline uses [docling](https://github.com/docling-project/docling) to convert each PDF to clean Markdown, then chunks, embeds, and stores the vectors in Postgres. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so only changed PDFs get re-parsed and re-embedded.

## How it works

The one genuinely expensive step is PDF parsing, so it runs on a GPU runner and the docling converter is built once with `@functools.cache`. `process_file` converts the PDF to Markdown, splits it into overlapping chunks, and maps each chunk to `process_chunk` for embedding. Read it in [`main.py`](main.py):

```python
@syn.fn.as_async(runner=syn.GPU)
def pdf_to_markdown(content: bytes) -> str:
    source = DocumentStream(name="input.pdf", stream=io.BytesIO(content))
    return pdf_converter().convert(source).document.export_to_markdown()

@syn.fn(memo=True)
async def process_file(file: FileLike, table: postgres.TableTarget[PdfEmbedding]) -> None:
    markdown = await pdf_to_markdown(await file.read())
    chunks = _splitter.split(markdown, chunk_size=2000, chunk_overlap=500, language="markdown")
    id_gen = IdGenerator()
    await syn.map(process_chunk, chunks, file.file_path.path, id_gen, table)
```

`@syn.fn.as_async(runner=syn.GPU)` wraps the *synchronous*, GPU-heavy parse so it runs off the async event loop. Each chunk's row `id` is derived from its text, so a chunk that survives a re-parse keeps its row.

## Why this example is useful

- **Parsing where text embedding has none.** docling reads the PDF and exports Markdown that preserves headings, tables, and reading order — which is exactly what makes the downstream chunks coherent.
- **The slow step, off the event loop.** `@syn.fn.as_async(runner=syn.GPU)` offloads PDF parsing to a dedicated GPU runner; `@functools.cache` loads the docling model once, not per file.
- **Incremental by default.** `@syn.fn(memo=True)` skips a PDF whose bytes and code are unchanged, so docling never re-parses a file you've already converted; `mount_table_target` upserts only changed rows and deletes rows whose source is gone.
- **Live without re-scanning.** The filesystem source declares `live=True` — pass `-L` and added, replaced, or deleted PDFs are picked up as they change.
- **Plain Python, your stack.** Local `all-MiniLM-L6-v2` embedder, no API key; swap `EMBED_MODEL` for any of the 12k+ sentence-transformer models on Hugging Face.

## Run it

**1. Start Postgres + pgvector** (the repo ships a compose file):

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install** (docling pulls in the PDF parser):

```sh
cp .env.example .env     # set POSTGRES_URL
pip install -e .
```

**3. Build the index** — the example ships a `pdf_files/` folder of sample papers/RFCs; catch-up or live:

```sh
synor update main        # catch-up
synor update -L main     # live: keep watching for file changes
```

**4. Search from the command line:**

```sh
python main.py "what is attention?"
```

With the sample papers indexed, the most semantically similar passages come back ranked — even when they share none of the words in your query. This example keeps it minimal and doesn't declare a vector index, so queries do a sequential scan. For a larger corpus, add `target_table.declare_vector_index(column="embedding")` exactly as Semantic Search 101 does.

---
