<h1 align="center">Turn a folder of docs into a <em>concept</em> knowledge graph.</h1>

<p align="center">
  <b>An LLM reads each Markdown doc and emits <em>(subject, predicate, object)</em> triples; the shared concept nodes and predicate edges become a graph you query in Cypher — in plain async Python.</b><br/>
  Point it at a docs folder, and it re-extracts only the doc you edited, then reconciles the graph.
</p>

<br/>

Documentation is a web of concepts pretending to be a list of files — "incremental processing relies on change detection", "a target receives declared target states" — every page asserts relationships, but they're locked in prose. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed graph targets) runs in a Rust engine underneath, so editing one doc re-extracts one doc, and the graph reconciles itself: no orphaned nodes, no stale edges, no cleanup scripts.

## How it works

Two node types, two relationship types, and the concept map falls out of the graph:

- **`Document`** nodes — one per Markdown file, keyed by filename, with an LLM-generated `title` and `summary`.
- **`Entity`** nodes — one per distinct concept named in any triple, keyed by the concept name and **shared** across documents.
- **`RELATIONSHIP`** edges — `Entity → Entity`, with the `predicate` stored as an edge property.
- **`MENTION`** edges — `Document → Entity`, recording which document named which concept.

Because entities are shared across documents, the pipeline runs in two phases — read it top-to-bottom in [`main.py`](main.py):

```python
@syn.task(cache=True)  # Phase 1 — per doc: declare the Document node, carry triples forward
async def process_file(file: localfs.File, document_table: neo4j.TableTarget[Document]) -> DocTriples:
    content = await file.read_text()
    filename = file.file_path.path.as_posix()
    summary = await extract_summary(content)
    document_table.declare_record(row=Document(filename=filename, title=summary.title, summary=summary.summary))
    triples = await extract_relationships(content)
    return DocTriples(filename=filename, triples=triples)

@syn.task              # Phase 2 — one pass owns the shared Entity nodes + both edge types
async def build_graph(docs, entity_table, relationship_rel, mention_rel) -> None:
    for doc in docs:
        for t in doc.triples:
            rel_id = await generate_id((t.subject, t.predicate, t.object))   # stable edge identity
            relationship_rel.declare_relation(from_id=t.subject, to_id=t.object,
                                              record=Relationship(id=rel_id, predicate=t.predicate))
            mention_rel.declare_relation(from_id=doc.filename, to_id=t.subject)
            ...
    for value in entities:  entity_table.declare_record(row=Entity(value=value))
```

Extraction is [instructor](https://github.com/instructor-ai/instructor) over [LiteLLM](https://docs.litellm.ai/) with your own Pydantic models; `MENTION` carries no payload, so the Neo4j connector derives its identity from the `(document, entity)` endpoints — one edge per pair.

## Why this example is useful

- **Shared nodes, done right.** Concepts are deduplicated and owned by a single graph pass, so `Incremental Processing` is one `Entity` node every doc can point at — not a copy per doc.
- **Incremental by default.** `@syn.task(cache=True)` caches each LLM extraction by content; edit one doc and only that doc re-extracts, then the graph diffs — adding new nodes/edges and removing ones no longer supported anywhere. A no-change re-run makes zero LLM calls.
- **Stable edge identity.** `generate_id` hashes each triple, so the same `(subject, predicate, object)` always maps to one edge — re-asserting a fact in another doc is a no-op, not a duplicate.
- **Plain Python, your stack.** Swap `LLM_MODEL` for any [LiteLLM provider](https://docs.litellm.ai/docs/providers) (OpenAI, Ollama, …). No DSL.
- **Honest cache busting.** `LLM_MODEL` is declared with `detect_change=True`, so swapping the model re-extracts the whole corpus against it with no cache to clear by hand.

## Run it

**1. Start Neo4j:**

```sh
docker run -d -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/synor --name synor-neo4j neo4j:5.26-community
```

**2. Configure & install:**

```sh
cp .env.example .env     # set OPENAI_API_KEY (or LLM_MODEL=ollama/llama3.2 to run locally)
pip install -e .
```

**3. Build the graph** — the example ships a `markdown_files/` folder of sample docs so it runs out of the box:

```sh
synor update main
```

To graph your own docs, drop `.md` / `.mdx` files into `markdown_files/` (or point `sourcedir` at your real docs folder) and re-run.

**4. Explore the graph** — open [Neo4j Browser](http://localhost:7474) (`neo4j` / `synor`) and ask:

```cypher
-- How concepts relate
MATCH (a:Entity)-[r:RELATIONSHIP]->(b:Entity)
RETURN a.value, r.predicate, b.value

-- Concepts mentioned in the most documents
MATCH (d:Document)-[:MENTION]->(e:Entity)
RETURN e.value, count(DISTINCT d) AS docs
ORDER BY docs DESC LIMIT 10
```

The LLM will sometimes name the same concept two ways ("PostgreSQL" vs "Postgres"). The meeting notes graph example adds an embedding + LLM entity-resolution pass that collapses near-duplicates — it drops into this pipeline between the two phases.

---
