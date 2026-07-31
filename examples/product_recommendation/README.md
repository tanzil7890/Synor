<h1 align="center">Turn a product catalog into a <em>recommendation</em> graph.</h1>

<p align="center">
  <b>An LLM tags what each product <em>is</em> and what <em>pairs</em> with it; the shared taxonomy edges become a "people who bought this also need…" engine — in plain async Python.</b><br/>
  Point it at a folder of product JSON, and it re-extracts only what changes as you edit the catalog.
</p>

<br/>

A pile of product listings has the recommendations hiding in plain sight — a *pen* pairs with *ink refills* and a *notebook*; a *monitor* pairs with a *stand* and an *HDMI cable*. But that knowledge is locked in prose. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed graph targets) runs in a Rust engine underneath, so editing one product re-extracts one product, not the catalog.

## How it works

Two node types, two relationship types, and the recommendation falls out of the graph:

- **`Product`** nodes — one per listing (title, price).
- **`Taxonomy`** nodes — one per distinct label (`gel pen`, `notebook`, `ink refill`), keyed by value and **shared** across products.
- **`PRODUCT_TAXONOMY`** edges — `Product → Taxonomy`: what the product is.
- **`PRODUCT_COMPLEMENTARY_TAXONOMY`** edges — `Product → Taxonomy`: what a buyer might also need.

Products whose *complementary* taxonomy matches another product's *is-a* taxonomy are the things to recommend together.

Because taxonomy labels are shared across products, the pipeline runs in two phases — read it top-to-bottom in [`main.py`](main.py):

```python
@syn.fn(memo=True)  # caches each extraction by content — re-tag only changed products
async def extract_taxonomy(detail: str) -> ProductTaxonomyInfo:
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    result = await client.chat.completions.create(
        model=syn.use_context(LLM_MODEL), response_model=ProductTaxonomyInfo,
        messages=[{"role": "system", "content": TAXONOMY_PROMPT}, {"role": "user", "content": detail}],
    )
    return ProductTaxonomyInfo.model_validate(result.model_dump())

@syn.fn(memo=True)   # Phase 1 — per product: declare the node, extract, carry labels forward
async def process_file(file: FileLike, product_table: neo4j.TableTarget[Product]) -> ProductTaxonomies:
    raw = json.loads(await file.read_text())
    product_id = file.file_path.path.name.removesuffix(".json")
    product_table.declare_record(row=Product(id=product_id, title=raw["title"], price=...))
    info = await extract_taxonomy(PRODUCT_TEMPLATE.render(**raw))
    return ProductTaxonomies(product_id, [t.name for t in info.taxonomies], ...)

@syn.fn              # Phase 2 — one pass owns the shared Taxonomy nodes + both edge types
async def build_graph(products, taxonomy_table, product_taxonomy_rel, complementary_rel) -> None:
    for value in {t for p in products for t in (*p.taxonomies, *p.complementary)}:
        taxonomy_table.declare_record(row=Taxonomy(value=value))
    for p in products:
        for t in set(p.taxonomies):    product_taxonomy_rel.declare_relation(from_id=p.product_id, to_id=t)
        for t in set(p.complementary): complementary_rel.declare_relation(from_id=p.product_id, to_id=t)
```

## Why this example is useful

- **Shared nodes, done right.** Taxonomy labels are deduplicated and owned by a single graph pass, so `gel pen` is one node every product can point at — not a copy per product.
- **Incremental by default.** `@syn.fn(memo=True)` caches each LLM extraction by content; edit one product and only that product re-extracts, then the graph diffs — adding new nodes/edges and removing ones no longer supported anywhere.
- **The graph IS the recommender.** No separate model. One Cypher query walks *complementary → is-a* edges to surface what to cross-sell.
- **Plain Python, your stack.** Extraction is [instructor](https://github.com/instructor-ai/instructor) over [LiteLLM](https://docs.litellm.ai/) — swap `LLM_MODEL` for any provider (OpenAI, Ollama, …). No DSL.
- **Honest cache busting.** `LLM_MODEL` is declared with `detect_change=True`, so swapping the model re-extracts everything against it with no cache to clear by hand.

## Run it

**1. Start Neo4j:**

```sh
docker run -d -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/synor --name synor-neo4j neo4j:5.26-community
```

**2. Configure & install:**

```sh
cp .env.example .env     # set OPENAI_API_KEY (or LLM_MODEL=ollama/llama3.2)
pip install -e .
```

**3. Build the graph** — the example ships a `products/` folder of sample listings (pens, notebooks, monitors, …):

```sh
synor update main
```

On the 9 sample products that's **9 `Product` nodes, ~40 `Taxonomy` nodes**, and the two edge types wired up.

**4. Explore the recommendations** — open [Neo4j Browser](http://localhost:7474) (`neo4j` / `synor`) and ask the graph:

```cypher
-- Recommend products to pair with anything that is a "gel pen":
-- find products whose is-a taxonomy matches a pen's complementary taxonomy
MATCH (:Taxonomy {value: "gel pen"})<-[:PRODUCT_TAXONOMY]-(:Product)
      -[:PRODUCT_COMPLEMENTARY_TAXONOMY]->(need:Taxonomy)
MATCH (rec:Product)-[:PRODUCT_TAXONOMY]->(need)
RETURN DISTINCT rec.title
```

On the sample data, recommending for a pen surfaces the notepad and the multipurpose paper — exactly the cross-sell you'd want.

---
