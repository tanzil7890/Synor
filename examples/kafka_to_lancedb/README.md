<h1 align="center">Consume a Kafka topic into <em>LanceDB</em>, routed by shape.</h1>

<p align="center">
  <b>Each message parsed and dispatched to the table that matches it — a <em>sku</em> field becomes a <code>products</code> row, an <em>emp_id</em> field an <code>employees</code> row — in plain async Python.</b><br/>
  Synor commits each Kafka offset only after the row is durably written, so a crash mid-flight replays cleanly.
</p>

<br/>

A topic is often a firehose of heterogeneous events — orders, users, inventory — sharing a transport but not a schema. The consumer's job is to *sort the mail*: read each envelope, decide what it is, and put it where it belongs. This is the consumer side of csv-to-kafka: the same declarative flow that *produced* the topic now *consumes* it. You declare the transformation in native Python — each work unit declares the outcomes it owns — and the Rust engine consumes one message per processing component, writes the row, and only then commits the offset, so a crash replays from the last durably-written message.

## How it works

Kafka is a source you treat as a keyed map; each LanceDB table is a target you declare rows on. `process_message` runs once per message: decode the value, `json.loads` it, and dispatch on shape. Read it in [`main.py`](main.py):

```python
@syn.task
async def process_message(
    msg: Message,
    products_table: lancedb.TableTarget[Product],
    employees_table: lancedb.TableTarget[Employee],
) -> None:
    value = msg.value()
    if value is None:
        return
    text = value.decode() if isinstance(value, bytes) else value
    row = json.loads(text)
    if "sku" in row:
        products_table.ensure_row(row=Product(**{**row, "price": float(row["price"])}))
    elif "emp_id" in row:
        employees_table.ensure_row(row=Employee(**row))

@syn.task
async def app_main() -> None:
    products_table = await lancedb.mount_table_target(
        LANCE_DB, table_name="products",
        table_schema=await lancedb.TableSchema.from_class(Product, primary_key=["sku"]))
    employees_table = await lancedb.mount_table_target(
        LANCE_DB, table_name="employees",
        table_schema=await lancedb.TableSchema.from_class(Employee, primary_key=["emp_id"]))
    config = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "group.id": KAFKA_GROUP_ID,
              "auto.offset.reset": "earliest"}
    consumer = kafka.create_consumer(config)  # ownership transfers to the stream
    items = kafka.topic_as_map(consumer, [KAFKA_TOPIC])
    await syn.spawn_each(process_message, items, products_table, employees_table)
```

The line worth pausing on is `ensure_row` — deliberately *not* `upsert()`. A new or changed primary key is upserted; a tombstone (null value) removes that key's row; the same row declared again writes nothing. `kafka.create_consumer()` forces both automatic commit and automatic offset storage off: Synor commits each offset *after* the row is durably written, so the consumer group resumes from the last message it actually persisted. Passing this helper-created consumer to `topic_as_map()` transfers ownership to the single-use stream, which drains, unsubscribes, and closes it on every exit path.

## Why this example is useful

- **Sort the mail.** Branch on a discriminator field (`sku` vs `emp_id`) and declare a typed row — the same pattern whether the destination is LanceDB, Postgres, or a vector index. A message matching neither shape is quietly skipped.
- **At-least-once, your offsets won't drift.** Auto-commit off + offset-after-write means `__consumer_offsets` never runs ahead of the data; a crash mid-flight replays from the last committed offset.
- **The dataclass is the schema.** `TableSchema.from_class` maps your dataclass to LanceDB/PyArrow column types — `Product` keyed by `sku`, `Employee` by `emp_id`, the same keys the messages carried in.
- **Embedded target, no server.** LanceDB tables are just files under `./lancedb_data` — there's nothing to run alongside the consumer.
- **Live mode is one flag.** `-L` is the entire difference between draining the backlog and exiting versus consuming forever — `process_message` doesn't change.

## Run it

> Needs a running Kafka broker with a topic to consume. The easy way to populate it: run csv-to-kafka first.

**1. Configure & install** — both examples default to the same `KAFKA_TOPIC` (`synor-csv-rows`), so the producer and consumer line up out of the box; override it in `.env` only if you changed it on the producer side:

```sh
cp .env.example .env     # set KAFKA_BOOTSTRAP_SERVERS / KAFKA_TOPIC / LANCEDB_URI (+ SASL for a managed broker)
pip install -e .
```

**2. Run the pipeline** — choose catch-up (drain what's there, then exit) or live (catch up, then keep consuming):

```sh
# Catch-up run: consume everything up to now, write the rows, then exit
synor update main.py

# Live run: catch up, then keep consuming new messages
synor update -L main.py
```

**3. Look at the tables** — they're just files under `./lancedb_data`:

```python
import lancedb

db = lancedb.connect("./lancedb_data")
for row in db.open_table("products").to_arrow().to_pylist():
    print(row)
for row in db.open_table("employees").to_arrow().to_pylist():
    print(row)
```

Every `sku` message is a row in `products`, every `emp_id` message a row in `employees` — keyed exactly as it was on the topic, so re-consuming the same key updates the row in place rather than duplicating.

---
