<h1 align="center">Hybrid search over <em>multi-format</em> SEC filings.</h1>

<p align="center">
  <b>Scrub, chunk, embed, and tag 10-K text <em>and</em> XBRL JSON into one Apache Doris table — with a vector index <em>and</em> a full-text index, fused with RRF.</b><br/>
  Semantic relevance and keyword presence combined, not either alone — in plain async Python.
</p>

<br/>

SEC filings come in many shapes — narrative 10-K risk factors as text, structured financials as XBRL JSON, exhibits as PDF. This pipeline pulls those formats into a **single** searchable index in [Apache Doris](https://doris.apache.org/), with both a vector index for semantic search and a full-text index for keyword search. You declare the transformation in native Python — each work unit declares the outcomes it owns — and the Rust engine handles incremental processing, so adding a filing embeds and loads only its chunks.

## How it works

Two source formats fan into one chunk table:

1. **Sources** — `*.txt` 10-K filings and `*.json` XBRL company facts (the JSON is rendered to searchable text first).
2. **Scrub & chunk** — strip SSNs / phones / emails **before** indexing, then split into overlapping chunks.
3. **Embed & tag** — a sentence-transformer embeds each chunk; a keyword pass tags `RISK:*` / `TOPIC:*` labels.
4. **Load into Doris** — one row per chunk, into a table with a vector (ANN) index and a full-text (inverted) index.

The magic is in `mount_table_target` — the same table gets a **vector index** (for `l2_distance` semantic search) and an **inverted index** (for `MATCH_ANY` keyword search). Read it in [`main.py`](main.py):

```python
table = await doris.mount_table_target(
    DORIS_DB, TABLE,
    await doris.TableSchema.from_class(FilingChunk, primary_key=["chunk_id"]),
    vector_indexes=[doris.VectorIndexDef(field_name="embedding", metric_type="l2_distance")],
    inverted_indexes=[doris.InvertedIndexDef(field_name="text", parser="unicode")],
)

# PII is redacted *before* chunking, so it never enters the index. Both source
# formats funnel into one shared path — scrub, chunk, embed, tag, declare a row per chunk:
async def _index_text(text, source_type, filename, cik, filing_date, form_type, table):
    embedder = syn.use_context(EMBEDDER)
    for chunk in _splitter.split(_scrub_pii(text), chunk_size=1000, chunk_overlap=200, language="markdown"):
        table.declare_row(row=FilingChunk(
            chunk_id=_chunk_id(filename, chunk.start.char_offset, chunk.end.char_offset),
            source_type=source_type, doc_filename=filename, cik=cik, filing_date=filing_date,
            form_type=form_type, text=chunk.text, topics=_extract_topics(chunk.text),
            embedding=await embedder.embed(chunk.text),
        ))
```

Both sources `declare_row` into the **same** Doris table — `chunk_id` is a stable `uuid5` of the file and chunk offsets, so re-running reconciles cleanly instead of duplicating.

## Why this example is useful

- **One table, two index types.** A single `mount_table_target` declares both a vector (ANN) and a full-text (inverted) index — the foundation for hybrid retrieval, with no second store to keep in sync.
- **Many formats, one index.** Text 10-Ks and XBRL JSON facts fan into the same chunk table; the PDF path is the same shared `_index_text` (see Manuals to Structured Data).
- **PII never enters the index.** Scrubbing runs *before* chunking and embedding, so SSNs / phones / emails are gone before anything is stored.
- **Incremental by default.** Add a filing and only its chunks are scrubbed, embedded, tagged, and stream-loaded; `chunk_id` reconciles edits in place instead of duplicating.
- **Hybrid search that actually fuses.** `search.py` ranks by vector distance and by keyword match, then combines them with [Reciprocal Rank Fusion](https://en.wikipedia.org/wiki/Learning_to_rank) in one SQL query.

## Run it

> Needs **Apache Doris 4.0+** for vector-index support — a ready `docker-compose.yml` is included.

**1. Start Doris (FE + BE):**

```sh
docker compose up -d fe be
```

**2. Fetch sample data, configure & install** — synthetic 10-K filings + XBRL company facts:

```sh
python download.py
cp .env.example .env     # Doris host/ports
pip install -e .
```

**3. Build the index:**

```sh
synor update main
```

On the sample data this loads 4 chunks (2 filings + 2 company-facts) into Doris, creating both `idx_vec_embedding` (ANN) and `idx_inv_text` (INVERTED). Topics come out as you'd expect — Apple tagged `RISK:CYBER, RISK:CLIMATE, RISK:SUPPLY, RISK:REGULATORY, TOPIC:AI`; Microsoft `RISK:CYBER, RISK:REGULATORY, TOPIC:AI, TOPIC:CLOUD`.

**4. Hybrid search** — vector + keyword, fused with RRF:

```sh
python search.py "cloud computing and AI risk"
```

On the sample data that ranks **Microsoft's** cloud-and-AI filing first (it carries both `TOPIC:CLOUD` and `TOPIC:AI`), Apple's second, and the company-facts rows below.

---
