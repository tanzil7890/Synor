---
title: Index Academic Papers and Extract Metadata
description: 'Turn a folder of academic PDFs into structured metadata with Synor V1 — read the first page, LLM-extract title, authors, and abstract into typed rows, embed the title and abstract for semantic search, and store it all in Postgres with pgvector.'
slug: paper-metadata
image: /docs/images/synor-mark.svg
tags: [llm-extraction, pdf]
---



We'll take a folder of academic PDFs and pull out the parts you actually want to query — **title, authors, abstract** — as structured, typed rows. The first page of a paper holds almost all of this, so we read just that page, hand the text to an LLM with a strict schema, and get back clean JSON. The same metadata is then embedded so you can search papers by meaning, not just by exact words.

The whole pipeline is ordinary `async` Python and your own types. The heavy lifting — incremental processing, change tracking, managed targets — runs in a Rust engine underneath, so only changed PDFs get re-extracted and re-embedded. One file fans out into three Postgres tables — paper metadata, an author-to-paper index, and embeddings — and Synor keeps all three in sync for you.


## Flow overview



From a high level, these are the steps:

1. Read PDF files from a local directory (live).
2. Pull the first page out of each PDF, [extract its text](https://github.com/py-pdf/pypdf), and ask an LLM to return `title`, `authors`, and `abstract` as structured JSON.
3. Embed the title and the abstract chunks, then declare the metadata, the author index, and the embeddings into Postgres (as target states).

You declare the transformation logic with native Python, without worrying about how updates propagate. Think: **each work unit declares the outcomes it owns**.

## Setup

- A running Postgres with the [pgvector](https://github.com/pgvector/pgvector) extension. The repo ships a compose file:

  ```sh
  docker compose -f dev/postgres.yaml up -d
  export POSTGRES_URL="postgres://synor:synor@localhost/synor"
  ```

- An [OpenAI API key](https://platform.openai.com/) for the extraction step:

  ```sh
  export OPENAI_API_KEY="your_key"
  ```

- Install Synor and the dependencies this example uses:

  ```sh
  pip install -U "synor[postgres,sentence_transformers]" asyncpg pgvector numpy pypdf openai pydantic python-dotenv
  ```

- A few PDFs to index. The example ships a `papers/` folder with a handful of well-known papers — or drop your own in.

## Define the schema you want back

Before touching the pipeline, pin down the shape of the metadata. These [Pydantic](https://docs.pydantic.dev/) models are what we ask the LLM to fill in — `model_validate_json` rejects anything that doesn't match, so a malformed response fails loudly instead of writing junk to the database.

```python title="models.py"
class AuthorModel(BaseModel):
    name: str
    email: str | None = None
    affiliation: str | None = None


class PaperMetadataModel(BaseModel):
    title: str
    authors: list[AuthorModel] = Field(default_factory=list)
    abstract: str
```

## Define the data and shared resources

Each output table maps to one dataclass: `PaperMetadataRow` is one row per paper, `AuthorPaperRow` is one row per (author, paper) pair — an index you can join against — and `MetadataEmbeddingRow` is one embedded chunk of text. `synor_lifespan` provides the shared resources every step needs — the Postgres connection pool and the embedding model — once at startup.

```python title="main.py"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PG_DB = syn.ContextKey[asyncpg.Pool]("paper_metadata_db")
EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)


@dataclass
class PaperMetadataRow:
    filename: str
    title: str
    authors: list[dict[str, str | None]]
    abstract: str
    num_pages: int


@dataclass
class AuthorPaperRow:
    author_name: str
    filename: str


@dataclass
class MetadataEmbeddingRow:
    id: uuid.UUID
    filename: str
    location: str
    text: str
    embedding: Annotated[NDArray, EMBEDDER]


@syn.lifespan
async def synor_lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
    async with asyncpg.create_pool(os.environ["POSTGRES_URL"]) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
        yield
```

`embedding: Annotated[NDArray, EMBEDDER]` ties the vector column to the embedder, so its dimensions are inferred automatically — and if you swap the model later, Synor notices (`detect_change=True`) and re-embeds.

## Read the first page and extract metadata

Three small functions do the extraction. `extract_basic_info` slices the first page out of the PDF (and counts the pages), `pdf_to_markdown` pulls the text off that page, and `extract_metadata` hands it to the LLM with a strict instruction to return only the three fields we want.

```python title="main.py"
@syn.task
def extract_basic_info(content: bytes) -> PaperBasicInfo:
    reader = PdfReader(io.BytesIO(content))
    output = io.BytesIO()
    writer = PdfWriter()
    writer.add_page(reader.pages[0])
    writer.write(output)
    return PaperBasicInfo(num_pages=len(reader.pages), first_page=output.getvalue())


@syn.task
def pdf_to_markdown(content: bytes) -> str:
    reader = PdfReader(io.BytesIO(content))
    return (reader.pages[0].extract_text() if reader.pages else "") or ""


@syn.task
def extract_metadata(markdown: str) -> PaperMetadataModel:
    response = openai_client().chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": (
                "You extract metadata from academic paper first pages. "
                "Return only JSON with keys: title, authors, abstract. "
                "authors is a list of {name, email, affiliation}. "
                "Use null for missing fields."
            )},
            {"role": "user", "content": markdown[:4000]},
        ],
        response_format={"type": "json_object"},
        temperature=0,
    )
    content = response.choices[0].message.content
    if not content:
        raise RuntimeError("LLM returned empty content.")
    return PaperMetadataModel.model_validate_json(content)
```

Only the first page is read, and the prompt is capped at `markdown[:4000]` characters — that's almost always enough to cover the title block and abstract, and it keeps the token cost flat regardless of how long the paper is. `response_format={"type": "json_object"}` with `temperature=0` makes the output deterministic JSON, and `PaperMetadataModel.model_validate_json` parses it straight into our typed model.

## Process a file



`process_file` runs once per PDF and ties the steps together. It extracts the metadata, then declares the rows: one metadata row, one author-index row per author, and one embedding row for the title plus one for each abstract chunk.

```python title="main.py"
@syn.task(cache=True)
async def process_file(
    file: FileLike,
    metadata_table: postgres.TableTarget[PaperMetadataRow],
    author_table: postgres.TableTarget[AuthorPaperRow],
    embedding_table: postgres.TableTarget[MetadataEmbeddingRow],
) -> None:
    content = await file.read()
    basic_info = extract_basic_info(content)
    first_page_md = pdf_to_markdown(basic_info.first_page)
    metadata = extract_metadata(first_page_md)

    metadata_table.ensure_row(
        row=PaperMetadataRow(
            filename=str(file.file_path.path),
            title=metadata.title,
            authors=[a.model_dump() for a in metadata.authors],
            abstract=metadata.abstract,
            num_pages=basic_info.num_pages,
        ),
    )

    for author in metadata.authors:
        if author.name:
            author_table.ensure_row(
                row=AuthorPaperRow(
                    author_name=author.name,
                    filename=str(file.file_path.path),
                ),
            )

    title_embedding = await syn.use_context(EMBEDDER).embed(metadata.title)
    embedding_table.ensure_row(
        row=MetadataEmbeddingRow(
            id=uuid.uuid4(), filename=str(file.file_path.path),
            location="title", text=metadata.title, embedding=title_embedding,
        ),
    )

    abstract_chunks = _abstract_splitter.split(
        metadata.abstract, chunk_size=500, min_chunk_size=200,
        chunk_overlap=150, language="abstract",
    )
    for chunk in abstract_chunks:
        embedding_table.ensure_row(
            row=MetadataEmbeddingRow(
                id=uuid.uuid4(), filename=str(file.file_path.path),
                location="abstract", text=chunk.text,
                embedding=await syn.use_context(EMBEDDER).embed(chunk.text),
            ),
        )
```

`@syn.task` with `cache=True` is what makes this incremental: if a PDF's content and this function's code are both unchanged, the whole file is skipped on the next run — so you never pay for the LLM call or the embeddings on a PDF you've already processed. We embed the title as one row and the abstract as a few overlapping chunks (a `RecursiveSplitter` tuned to break on sentence boundaries), and `location` marks which is which so a search can tell a title hit from an abstract hit. `table.ensure_row` declares each row as a target state; Synor handles inserting, updating, or deleting it to match.

## Define the main function

`app_main` wires the source to the targets. It mounts the three Postgres tables, walks the source directory for PDFs, and mounts one processing component per file.

```python title="main.py"
@syn.task
async def app_main(sourcedir: pathlib.Path) -> None:
    metadata_table = await postgres.mount_table_target(
        PG_DB, table_name=TABLE_METADATA,
        table_schema=await postgres.TableSchema.from_class(
            PaperMetadataRow, primary_key=["filename"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,   # "synor_examples_v1"
    )
    author_table = await postgres.mount_table_target(
        PG_DB, table_name=TABLE_AUTHOR_PAPERS,
        table_schema=await postgres.TableSchema.from_class(
            AuthorPaperRow, primary_key=["author_name", "filename"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )
    embedding_table = await postgres.mount_table_target(
        PG_DB, table_name=TABLE_EMBEDDINGS,
        table_schema=await postgres.TableSchema.from_class(
            MetadataEmbeddingRow, primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
        live=True,  # watch for changes; pass -L to `synor update` to run live
    )
    await syn.spawn_each(
        process_file, files.items(), metadata_table, author_table, embedding_table
    )


app = syn.App(
    syn.AppConfig(name="PaperMetadataV1"),
    app_main,
    sourcedir=pathlib.Path("./papers"),
)
```

Each `mount_table_target` creates and manages a Postgres table for you — schema, idempotent upserts, and orphan cleanup when a PDF disappears. Note the different primary keys: paper metadata is keyed by `filename`, the author index by the `(author_name, filename)` pair, and the embeddings by a generated `id`. `live=True` makes the filesystem source watch for changes, and `spawn_each` runs one component per file so the engine can track and update each PDF independently while writing into all three tables.

> **No vector index here.** To keep the example minimal, this flow doesn't declare a vector index, so queries do a sequential scan — fine for a few papers. For a larger corpus, add one line — `embedding_table.declare_vector_index(column="embedding")` — exactly as the Semantic Search 101 example does, and pgvector serves approximate-nearest-neighbor queries instead.

## Run the pipeline

Run the `synor` CLI to build and update the index. Choose catch-up (scan, sync, exit) or live (catch up, then keep watching):

```sh
# Catch-up run
synor update main

# Live run: keep watching for file changes
synor update -L main
```

## Query the index

Match user text against the embeddings with a plain SQL query, reusing the *same* embedder from the indexing flow so indexing and querying stay consistent.

```python title="main.py"
async def query_once(pool, embedder, query: str, *, top_k: int = 5) -> None:
    query_vec = await embedder.embed(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT filename, location, text, embedding <=> $1 AS distance
            FROM "{PG_SCHEMA_NAME}"."{TABLE_EMBEDDINGS}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec, top_k,
        )
    for r in rows:
        score = 1.0 - float(r["distance"])
        print(f"[{score:.3f}] {r['filename']} ({r['location']})")
        print(f"    {r['text']}")
        print("---")
```

The `<=>` operator is pgvector's cosine distance. We turn it into a similarity score and print the filename, whether the hit was a `title` or an `abstract` chunk, and the matching text. Run a search straight from the command line:

```bash
python main.py "graph neural networks"
```

With the sample papers indexed, the most semantically similar titles and abstracts come back ranked — even when they share none of the words in your query. That's the whole point of embedding the metadata.

## Incremental updates

Synor keeps the three tables in sync with your PDFs and does the **minimum work** to get there. You never compute a diff or write update logic. Two pieces make this work. `@syn.task(cache=True)` decides what to *recompute* — a PDF is skipped when its bytes and the function's code are both unchanged, so neither the LLM nor the embedder ever runs on an unchanged file. `mount_table_target` decides what to *write* — it upserts only the rows that actually changed and deletes rows whose source is gone, across all three tables.

- **A PDF is added** — only that file is read, extracted, and embedded; its metadata, author, and embedding rows are inserted. The rest is untouched.
- **A PDF is replaced** — it is re-extracted; the metadata row is updated, author rows are reconciled against the new author list, and the embeddings are recomputed.
- **A PDF is deleted** — all of its rows are removed from all three tables automatically.

The same machinery covers **logic** changes too: tweak the prompt, swap `gpt-4o` for another model, or change the embedding model, and Synor compares the new output against what's already in Postgres and applies only the difference. A catch-up run (`synor update main`) does this once and exits; live mode (`synor update -L main`) keeps watching and applies each change with low latency.

## Run it

The full, runnable example is in this local workspace: examples/paper_metadata. If you just want to search PDFs by meaning without the structured extraction, Semantic Search over PDFs chunks and embeds the full text instead; if you want the Markdown itself as the output, see PDF → Markdown.

