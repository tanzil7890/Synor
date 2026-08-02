<h1 align="center">Turn a folder of papers into <em>structured</em> metadata.</h1>

<p align="center">
  <b>Read just the first page, LLM-extract <em>title, authors, abstract</em> into typed rows, then embed the metadata so you can search papers by <em>meaning</em> — in plain async Python.</b><br/>
  One PDF fans out into three Postgres tables, and Synor keeps all three in sync as the folder changes.
</p>

<br/>

The first page of a paper holds almost everything you'd want to query — title, authors, abstract — but it's locked in PDF prose. This pipeline reads just that page, hands the text to an LLM with a strict schema, and gets back clean typed JSON; the same metadata is then embedded so you can search by meaning, not exact words. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so only changed PDFs get re-extracted and re-embedded.

## How it works

One PDF flows through three small functions and fans into three tables:

1. **`extract_basic_info`** slices the first page out of the PDF and counts the pages; **`pdf_to_markdown`** pulls the text off that page with [pypdf](https://github.com/py-pdf/pypdf).
2. **`extract_metadata`** hands that text to `gpt-4o` (via the `openai` SDK) with `response_format={"type": "json_object"}` and `temperature=0`, then `model_validate_json` parses it into a typed `PaperMetadataModel` — a malformed response fails loudly instead of writing junk.
3. **`process_file`** declares the rows: one metadata row, one author-index row per author, one embedding row for the title plus one per abstract chunk.

Read it in [`main.py`](main.py):

```python
@syn.task(cache=True)
async def process_file(
    file: FileLike,
    metadata_table: postgres.TableTarget[PaperMetadataRow],
    author_table: postgres.TableTarget[AuthorPaperRow],
    embedding_table: postgres.TableTarget[MetadataEmbeddingRow],
) -> None:
    content = await file.read()
    basic_info = extract_basic_info(content)
    metadata = extract_metadata(pdf_to_markdown(basic_info.first_page))

    metadata_table.ensure_row(row=PaperMetadataRow(
        filename=str(file.file_path.path), title=metadata.title,
        authors=[a.model_dump() for a in metadata.authors],
        abstract=metadata.abstract, num_pages=basic_info.num_pages,
    ))
    for author in metadata.authors:
        if author.name:
            author_table.ensure_row(row=AuthorPaperRow(
                author_name=author.name, filename=str(file.file_path.path)))

    title_embedding = await syn.use_context(EMBEDDER).embed(metadata.title)
    embedding_table.ensure_row(row=MetadataEmbeddingRow(
        id=uuid.uuid4(), filename=str(file.file_path.path),
        location="title", text=metadata.title, embedding=title_embedding))
    for chunk in _abstract_splitter.split(metadata.abstract, chunk_size=500, ...):
        embedding_table.ensure_row(row=MetadataEmbeddingRow(
            id=uuid.uuid4(), filename=str(file.file_path.path), location="abstract",
            text=chunk.text, embedding=await syn.use_context(EMBEDDER).embed(chunk.text)))
```

`embedding: Annotated[NDArray, EMBEDDER]` ties the vector column to the embedder, so its dimensions are inferred automatically. `app_main` mounts the three tables (with different primary keys), walks the source for `*.pdf`, and runs one `process_file` component per file with `spawn_each`.

## Why this example is useful

- **One file, three tables, kept in sync.** Paper metadata, an author-to-paper index, and embeddings — `mount_table_target` upserts only what changed and removes rows whose PDF is gone, across all three.
- **First page only, capped at 4000 chars.** That's almost always enough for the title block and abstract, and it keeps token cost flat regardless of paper length.
- **Typed extraction, validated loud.** `gpt-4o` returns JSON, `PaperMetadataModel.model_validate_json` rejects anything off-schema — junk never reaches Postgres.
- **Incremental by default.** `@syn.task(cache=True)` skips a PDF entirely when its bytes and the function's code are unchanged, so you never re-pay for the LLM call or the embeddings on a file you've seen.
- **Honest cache busting.** `EMBEDDER` is declared with `detect_change=True`, so swapping the embedding model re-embeds everything with no cache to clear by hand.

## Run it

**1. Start Postgres (with pgvector):**

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install** — the example ships a `papers/` folder of well-known papers:

```sh
cp .env.example .env     # set POSTGRES_URL and OPENAI_API_KEY
pip install -e .
```

**3. Build the index** — catch-up (scan, sync, exit) or live (catch up, then keep watching):

```sh
synor update main       # catch-up run
synor update -L main    # live run — watch the papers/ folder for changes
```

This reads each PDF's first page, LLM-extracts the metadata, embeds the title and abstract chunks, and writes the `synor_examples_v1` schema's three tables.

**4. Search by meaning** — a plain SQL query over pgvector's cosine distance, reusing the *same* embedder:

```sh
python main.py "graph neural networks"
```

The most semantically similar titles and abstracts come back ranked — even when they share none of the query's words. Note: to keep the example minimal it declares **no vector index**, so queries do a sequential scan (fine for a handful of papers).

---

