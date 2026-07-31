<h1 align="center">Turn an existing Postgres table into a <em>semantic</em> index.</h1>

<p align="center">
  <b>Read product rows from a Postgres table, <em>derive</em> fields and <em>embed</em> each one, and write the enriched rows plus their vectors back to Postgres with pgvector.</b><br/>
  Your structured data, searchable by meaning — in plain async Python.
</p>

<br/>

Most data already lives in a database. This example takes an existing Postgres table of products, reads it row by row, derives a couple of fields, embeds each row, and writes the result — including the vector — back into Postgres with [pgvector](https://github.com/pgvector/pgvector). You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so only the rows that changed get re-embedded and re-upserted.

## How it works

`app_main` wires the source to the target: it mounts the Postgres target table, opens the source table with `PgTableSource`, and mounts one processing component per source row. Passing `row_type=SourceProduct` maps each row straight into the dataclass; `items(...)` tags each one with its `(product_category, product_name)` composite key. Read it in [`main.py`](main.py):

```python
@syn.fn(memo=True)
async def process_product(product: SourceProduct, table: postgres.TableTarget[OutputProduct]) -> None:
    full_description = f"Category: {product.product_category}\nName: {product.product_name}\n\n{product.description}"
    total_value = product.price * product.amount
    embedding = await syn.use_context(EMBEDDER).embed(full_description)
    table.declare_row(row=OutputProduct(..., total_value=total_value, embedding=embedding))

@syn.fn
async def app_main() -> None:
    target_table = await postgres.mount_table_target(
        PG_DB, table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            OutputProduct, primary_key=["product_category", "product_name"]),
        pg_schema_name=PG_SCHEMA_NAME,
    )
    source = postgres.PgTableSource(
        syn.use_context(SOURCE_POOL), table_name="source_products", row_type=SourceProduct)
    await syn.mount_each(
        process_product,
        source.fetch_rows().items(lambda p: (p.product_category, p.product_name)),
        target_table,
    )
```

We embed the *composed* description — category and name included — so a search for "wireless audio" matches even when the body never says it. `embedding: Annotated[NDArray, EMBEDDER]` ties the vector column to the embedder, so its dimensions are inferred automatically.

## Why this example is useful

- **Your database is the source.** `PgTableSource` reads an existing table directly — point it at any table and you have a semantic index over your structured data, no export step.
- **Source and target, same engine.** The same Postgres instance can hold both, or set `SOURCE_DATABASE_URL` to read from a separate database. `mount_table_target` creates and manages the target table — schema, idempotent upserts, orphan cleanup.
- **Embed what matters.** The composed `full_description` carries the category and name into the vector, so meaning-based search works even when the query words never appear in the body.
- **Incremental by default.** `@syn.fn(memo=True)` skips a row whose content and code are unchanged; the output's primary key is derived from the source row, so only changed rows are re-embedded and upserted and vanished rows are deleted.
- **Plain Python, your stack.** Local `all-MiniLM-L6-v2` embedder, no API key; swap `EMBED_MODEL` for any of the 12k+ sentence-transformer models on Hugging Face.

## Run it

**1. Start Postgres + pgvector** (the repo ships a compose file):

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install:**

```sh
cp .env.example .env     # set POSTGRES_URL and SOURCE_DATABASE_URL (can be the same instance)
pip install -e .
```

**3. Seed the source table** — create `source_products` with the sample rows:

```sh
psql "$SOURCE_DATABASE_URL" -f ./prepare_source_data.sql
```

**4. Build the index** — the Postgres source runs as a one-shot catch-up (scan the source table, sync the target, exit):

```sh
synor update main
```

**5. Search from the command line:**

```sh
python main.py "wireless headphones"
```

The most semantically similar products come back ranked — even when they share none of the words in your query. That's the whole point of a vector index.

---
