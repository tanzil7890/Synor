# AGENTS.md — Synor examples

Guidance for AI coding agents (Claude Code, Cursor, etc.) working in this `examples/`
directory. Most top-level Python subfolders are self-contained, runnable
Synor **v1** apps; `rust/` contains Rust ports with per-example READMEs.

## Before you write Synor code: use the local skill

Synor v1 is a fundamental redesign from v0. Without context, LLMs tend to
hallucinate the v0 flow-builder DSL and deprecated decorators. Install the
project skill first — it teaches the correct v1 API. This workspace already
contains the skill at `../skills/synor`; no download or repository clone is
needed. To copy it into another local project:

```sh
mkdir -p /path/to/project/.agents/skills
cp -R ../skills/synor /path/to/project/.agents/skills/synor
```

For Claude Code's native project path, use `.claude/skills/synor` instead
of `.agents/skills/synor`; the skill format is the same.

The skill itself lives at [`skills/synor/`](../skills/synor) (`SKILL.md` plus
`references/`). For Cursor, copy `SKILL.md` into `.cursor/rules/`. The complete
documentation source is available locally in `../docs/src/content/docs`.
Its companion references are `api_reference.md`, `connectors.md`,
`patterns.md`, `setup_database.md`, and `setup_project.md`.

## The v1 mental model

Ask three questions: what changed, which stable work unit owns the result, and
which declared outcome must be reconciled. The Rust engine answers those
questions across runs and reprocesses only affected work. State is tracked in a
local LMDB store, so **no database is required for the engine itself**; one is
needed only when an example writes to it. Key APIs: `@syn.fn`, `mount` /
`use_mount` / `mount_each`, `ContextKey`, and target-state declarations. See the
skill for details.

## Running examples

Most Python examples are standalone projects with their own `pyproject.toml`:

```sh
cd <example_dir>
cp .env.example .env          # if present — fill in the blanks (see below)
pip install -e .              # or: uv pip install -e .
synor update main         # catch-up: scan sources, sync, exit
synor update -L main      # live mode: catch up, then watch for changes (where supported)
```

Use the example's README as the source of truth. Known exceptions:

- `multi_codebase_summarization`, `audio_to_text`,
  `patient_intake_extraction_baml`, `patient_intake_extraction_dspy`:
  `synor update main.py`
- `conversation_to_knowledge`: `synor update conv_knowledge.app`
- `image_search`, `image_search_colpali`: start the API with
  `python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000`; it runs the
  Synor app in live mode, then start the frontend from `frontend/`.
- `csv_to_kafka`, `kafka_to_lancedb`: `synor update -L main.py`
- `rust/<example>`: follow that example's README. Many use
  `cargo run -- index` for indexing and `cargo run -- query "..."` for search;
  a few take custom paths or service-specific subcommands.

Some examples expose a query/CLI demo via `python main.py "<query>"`; check the
example's `README.md`. Examples that need extra services or a code-gen step
(e.g. `baml generate`) say so in their README.

## Environment / credentials

When an example needs credentials or service configuration, required env vars
are templated in that example's **`.env.example`** — `cp` it to `.env` and fill
in the blanks; both `python main.py` and the `synor` CLI load `.env`
automatically. Common ones:

- `POSTGRES_URL` — for Postgres/pgvector targets. Local instance:
  `docker compose -f ../../dev/postgres.yaml up -d` from inside an example
  directory.
- `OPENAI_API_KEY` / `GEMINI_API_KEY` — for examples that call an LLM.
- Service-specific (`QDRANT_URL`, `LANCEDB_URI`, `NEO4J_*`, `KAFKA_*`,
  `GOOGLE_SERVICE_ACCOUNT_CREDENTIAL`, …) — see that example's `.env.example`.

Examples with no `.env.example` (e.g. `files_transform`, `pdf_to_markdown`) run
fully locally with no credentials.

**Never commit secrets.** The `.env` files tracked in this repo hold only
non-secret defaults (`SYNOR_DB`, local service URLs); keep API keys and
credentials in your local `.env` edits and out of commits.

## The examples

A walkthrough name means there is a matching guide under
`../docs/src/content/example-posts`; otherwise start from the example's README.

### Governance and safety
- `provable_index_revocation` — local, deterministic access revocation with
  immediate guarded-query suppression, delayed target convergence, receipts,
  partial-scan safety, and restore non-resurrection. Optional Google
  Drive/Qdrant configuration is operator-gated.

### Vector indexes (embed → store → search by meaning)
- `text_embedding` — Markdown → pgvector; the simplest end-to-end index. *(walkthrough: text-embedding)*
- `code_embedding` — repo → Tree-sitter chunks → pgvector; query code in English. *(walkthrough: index-codebase)*
- `text_embedding_qdrant` / `text_embedding_lancedb` / `text_embedding_turbopuffer` — same flow, different vector store.
- `code_embedding_lancedb` — code chunks → LanceDB.
- `pdf_embedding` — PDFs → markdown → chunks → pgvector.
- `paper_metadata` — extract title/authors/abstract from PDFs → Postgres + embeddings.
- `amazon_s3_embedding` / `gdrive_text_embedding` / `oci_object_storage_embedding` — same flow, remote source (S3 / Google Drive / OCI).
- `postgres_source` — read from an existing Postgres table as the source.
- `entire_session_search` — semantic search over AI coding sessions captured by Entire.
- `sec_edgar_analytics` — multi-format SEC filings → Apache Doris with a vector **and** a full-text index; hybrid (semantic + keyword) RRF search. *(walkthrough: sec-edgar-analytics)*

### Multimodal
- `image_search` — CLIP embeddings + Qdrant, queried via FastAPI + React.
- `image_search_colpali` — ColPali multi-vector model + Qdrant MaxSim.
- `multi_format_indexing` — PDFs + images as page screenshots → ColPali → Qdrant; no OCR, no chunking. *(walkthrough: multi-format-indexing)*
- `face_recognition` — detect faces (dlib) → 128-d embeddings → Qdrant face search. *(walkthrough: face-recognition)*
- `audio_to_text` — transcribe audio with LiteLLM → Postgres.
- `slides_to_speech` — slides → vision-LLM notes → Pocket TTS narration → LanceDB semantic search. *(walkthrough: slides-to-speech)*

### Structured extraction (LLM / BAML / DSPy)
- `multi_codebase_summarization` — LLM per-file summaries across many repos. *(walkthrough: multi-codebase-summarization)*
- `hn_trending_topics` — scrape HackerNews → LLM topic extraction → Postgres.
- `manuals_llm_extraction` — PDF manuals → Markdown (docling) → typed module records → Postgres. *(walkthrough: manuals-llm-extraction)*
- `patient_intake_extraction_baml` / `patient_intake_extraction_dspy` — structured PDF extraction with BAML / DSPy (Gemini vision).

### Knowledge graphs
- `conversation_to_knowledge` — YouTube podcasts → SurrealDB knowledge graph. *(walkthrough: podcast-to-knowledge-graph)*
- `docs_to_knowledge_graph` — Markdown docs → Neo4j concept graph of LLM-extracted triples. *(walkthrough: docs-to-knowledge-graph)*
- `product_recommendation` — product catalog → LLM taxonomy extraction → Neo4j recommendation graph. *(walkthrough: product-recommendation)*
- `meeting_notes_graph_neo4j` / `meeting_notes_graph_falkordb` — Google Drive meeting notes → Neo4j / FalkorDB graph.

### Custom sources / targets / streaming
- `pdf_to_markdown` — incremental PDF → Markdown with docling (local, no services). *(walkthrough: pdf-to-markdown)*
- `files_transform` — watch Markdown files → HTML, live mode (local, no services).
- `csv_to_kafka` — watch CSVs → publish rows to Kafka.
- `kafka_to_lancedb` — consume Kafka → route to LanceDB tables.

### Rust
- `rust/` — Rust ports of many of the above, using the Synor Rust API.

## Conventions for edits

- Keep each Python example self-contained: its own `pyproject.toml` and
  `README.md`; add `.env.example` when credentials or configurable services are
  required. When you add an example, also add a line to `EXAMPLE_CATALOG` in the
  docs repo (`docs/src/data/examples.ts`) so it appears in `/docs/llms.txt`.
- Match the surrounding code's low comment density.
- Don't commit generated artifacts (`synor.db`, `__pycache__`, build output) —
  they're already git-ignored.
