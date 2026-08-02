<h1 align="center">Search your <em>AI coding sessions</em> in plain English.</h1>

<p align="center">
  <b>Walk a folder of <a href="https://entire.io">Entire</a> checkpoints, <em>route</em> each file by name, and <em>embed</em> transcripts, prompts, and context into Postgres pgvector.</b><br/>
  "How did I fix the auth bug" finds the right session even with zero shared keywords — in plain async Python.
</p>

<br/>

[Entire](https://entire.io) captures every AI coding session you run — the full transcript, the prompt you started from, an AI-written context summary, and metadata like token counts and files touched — as checkpoints on disk. This pipeline turns that folder into a [vector index](https://github.com/pgvector/pgvector) you can search in plain English. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so each new session you capture only embeds what changed.

## How it works

A checkpoint folder holds four file types, and `process_file` routes on the name: `full.jsonl` is parsed into per-turn transcript chunks, `prompt.txt` is embedded whole, `context.md` is split into overlapping chunks, and `metadata.json` becomes a structured row in a *second* table. The transcript and context paths fan out to many rows via `syn.map(process_chunk, ...)`; the prompt is a single short string embedded inline. Read it in [`main.py`](main.py):

```python
@syn.task(cache=True)
async def process_file(file, emb_table, meta_table) -> None:
    info = extract_session_info(file)
    filename = file.file_path.path.name
    id_gen = IdGenerator()

    if filename == "full.jsonl":
        chunks = parse_transcript(await file.read_text())
        await syn.map(process_chunk,
            [ChunkInput(text=c.text, content_type="transcript", role=c.role) for c in chunks],
            info, id_gen, emb_table)

    elif filename == "prompt.txt":
        text = (await file.read_text()).strip()
        if text:
            emb_table.ensure_row(row=SessionEmbeddingRow(
                id=await id_gen.next_id(text), ..., content_type="prompt", role="user",
                text=text, embedding=await syn.use_context(EMBEDDER).embed(text)))

    elif filename == "context.md":
        ...   # split into chunks, then syn.map(process_chunk, ..., content_type="context")

    elif filename == "metadata.json":
        meta = json.loads(await file.read_text())
        meta_table.ensure_row(row=SessionMetadataRow(..., total_tokens=..., files_touched=...))
```

Three content types and a structured record, all from one component. Each embedding row's `id` is derived from its text, so a turn that survives a re-parse keeps its row.

## Why this example is useful

- **One component, four file types.** A single `included_patterns` list pulls `full.jsonl`, `prompt.txt`, `context.md`, and `metadata.json` into the same `process_file`, which routes on the name — no four separate pipelines.
- **Two tables, one pass.** Searchable text lands in the embeddings table; structured fields (tokens, files touched, agent percentage) land in a metadata table — declared side by side.
- **Incremental by default.** `@syn.task(cache=True)` skips a file whose content and code are unchanged, so a finished session is never re-embedded; `id` derived from text means only genuinely new turns are inserted and vanished turns are deleted.
- **Live without re-scanning.** The filesystem source declares `live=True` — pass `-L` and new sessions are picked up and embedded as they're written.
- **Plain Python, your stack.** Local `all-MiniLM-L6-v2` embedder, no API key; swap `EMBED_MODEL` for any of the 12k+ sentence-transformer models on Hugging Face.

## Run it

**1. Start Postgres + pgvector** (the repo ships a compose file):

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install:**

```sh
cp .env.example .env     # set POSTGRES_URL (schema/table names are optional overrides)
pip install -e .
```

**3. Check out some checkpoints** — from any repo where [Entire](https://entire.io) is capturing sessions:

```sh
git worktree add entire_checkpoints entire/checkpoints/v1
```

Each session is laid out as `<checkpoint_id[:2]>/<checkpoint_id[2:]>/<session_idx>/` with `full.jsonl`, `prompt.txt`, `context.md`, and `metadata.json`.

**4. Build the index** — catch-up (scan, sync, exit) or live (catch up, then keep watching for new sessions):

```sh
synor update main        # catch-up
synor update -L main     # live
```

**5. Search from the command line:**

```sh
python main.py "how did I fix the auth bug"
```

Results print which session and content type matched, so a transcript turn, a prompt, and a context chunk are all distinguishable. This example keeps it minimal and doesn't declare a vector index, so queries do a sequential scan — fine for a personal history. For a larger corpus, add `emb_table.declare_vector_index(column="embedding")` exactly as Semantic Search 101 does.

---
