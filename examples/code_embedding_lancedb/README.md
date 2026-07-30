<h1 align="center">Index your <em>codebase</em> into LanceDB.</h1>

<p align="center">
  <b>A live, syntax-aware vector index over your repo — into embedded LanceDB, with no database to run — in ~100 lines of plain async Python.</b><br/>
  Point it at a directory, search it in natural language, and it re-embeds only what changes as you edit.
</p>

<br/>

This is the Tree-sitter code-embedding pipeline, targeting [LanceDB](https://lancedb.github.io/lancedb/) instead of Postgres. LanceDB is an embedded vector store — no server to start, no connection string; the index is just a `./lancedb_data/` directory you can copy or delete. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns. The heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so a one-line edit re-embeds one chunk, not the repo.

```python
query: "where do we embed chunks?"

[0.582] examples/code_embedding_lancedb/main.py (L66-L82)
    @syn.fn
    async def process_chunk(chunk, filename, id_gen, table):
        ... embedding=await syn.use_context(EMBEDDER).embed(chunk.text) ...
```

## How it works

Walk a repo → detect language → split along the **syntax tree** with Tree-sitter → embed each chunk → upsert into LanceDB. With `live=True`, the source keeps watching and the index stays fresh as you code.

The whole indexing path is the snippet below — read it top-to-bottom in [`main.py`](main.py):

```python
@syn.fn(memo=True)
async def process_file(file: FileLike, table: lancedb.TableTarget[CodeEmbedding]) -> None:
    text = await file.read_text()
    language = detect_code_language(filename=str(file.file_path.path.name))
    chunks = _splitter.split(text, chunk_size=1000, min_chunk_size=300,
                             chunk_overlap=300, language=language)   # Tree-sitter, syntax-aware
    id_gen = IdGenerator()
    await syn.map(process_chunk, chunks, file.file_path.path, id_gen, table)

@syn.fn
async def process_chunk(chunk, filename, id_gen, table) -> None:
    table.declare_row(row=CodeEmbedding(
        id=await id_gen.next_id(chunk.text), filename=str(filename), code=chunk.text,
        embedding=await syn.use_context(EMBEDDER).embed(chunk.text),
        start_line=chunk.start.line, end_line=chunk.end.line,
    ))

@syn.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    table = await lancedb.mount_table_target(
        LANCE_DB, table_name=TABLE_NAME,
        table_schema=await lancedb.TableSchema.from_class(CodeEmbedding, primary_key=["id"]),
    )
    files = localfs.walk_dir(sourcedir, recursive=True,
                             path_matcher=PatternFilePathMatcher(included_patterns=["**/*.py", ...]),
                             live=True)
    await syn.mount_each(process_file, files.items(), table)
```

## Why this example is useful

- **No database to run.** LanceDB is embedded — the index lives in `./lancedb_data/`. Nothing to start, nothing to connect to; copy the directory to move it, delete it to start fresh.
- **Syntax-aware chunking, built in.** Tree-sitter splits along real code structure — functions, classes, blocks — so retrieval returns whole units, not fragments cut mid-statement. Every major language; unknown types fall back to plain text.
- **Incremental by default.** `@syn.fn(memo=True)` skips unchanged files and reuses embeddings for unchanged chunks; `mount_table_target` upserts only the rows that moved and deletes orphans. Edit one function → one chunk is re-embedded.
- **Live updates.** `live=True` + `synor update -L` keeps watching the filesystem and applies changes with low latency — always-fresh context for an agent.
- **Plain Python, your stack.** Swap the embedding model (12k+ on Hugging Face), the chunking, or the vector store. No DSL.

## Run it

LanceDB is embedded, so there's no server to start — the index is created on first run.

**1. Install deps:**

```sh
pip install -e .
```

**2. Build / update the index** (writes rows into LanceDB at `./lancedb_data/`) — pick one:

```sh
synor update main       # catch-up: scan, sync changes, exit
synor update -L main    # live: catch up, then keep watching for edits
```

**3. Query it** — semantic search from the terminal:

```sh
python main.py "embedding"
```

Each result carries `start_line`/`end_line`, so hits point straight at the lines that matched. Query uses LanceDB's vector search, with the returned distance turned into a similarity score.

---
