<h1 align="center">Stream a folder of CSVs to <em>Kafka</em>, row by row.</h1>

<p align="center">
  <b>Each row published as a JSON message keyed by its primary key — edit one cell and exactly <em>one</em> message lands on the topic, within a second — in plain async Python.</b><br/>
  Declare the topic as a target state; Synor produces the upserts and deletes, never the no-ops.
</p>

<br/>

CSV is the format that shows up everywhere and gets respect nowhere — BI exports, vendor dumps, spreadsheets parked in a shared drive, dropped into a folder and edited at random with no schema contract. This pipeline turns a directory of them into a clean, row-keyed, diff-only [Kafka](https://kafka.apache.org/) stream. You declare the transformation in native Python — each work unit declares the outcomes it owns — and the Rust engine does the incremental processing underneath: it tracks what each row last looked like and produces a message only for rows that *actually changed* — no producer loop, no dedup bookkeeping.

## How it works

The Kafka topic is just a target you declare on, the same way you'd declare a Postgres table. `process_csv` runs once per file: parse rows with `csv.DictReader`, then declare each row as a target state — key from the first column, value the JSON-encoded row. Read it in [`main.py`](main.py):

```python
@syn.fn(memo=True)
async def process_csv(file: FileLike, topic_target: kafka.KafkaTopicTarget) -> None:
    reader = csv.DictReader(io.StringIO(await file.read_text()))
    headers = reader.fieldnames
    if not headers:
        return
    first_col = headers[0]
    for row in reader:
        key_value = row.get(first_col)
        if key_value is not None:
            topic_target.declare_target_state(key=key_value, value=json.dumps(row))

@syn.fn
async def app_main() -> None:
    topic_target = await kafka.mount_kafka_topic_target(KAFKA_PRODUCER, KAFKA_TOPIC)
    files = localfs.walk_dir(
        localfs.FilePath(path="./data"),
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.csv"]),
        live=True,  # watch for changes; pass -L to `synor update` to run live
    )
    await syn.mount_each(process_csv, files.items(), topic_target)
```

The one line worth pausing on is `declare_target_state` — deliberately *not* `produce()`. You describe what the topic *should be* as a function of the source; Synor turns the state transitions into wire messages. A new or changed key produces an upsert `(k, v)`; a key that's no longer declared produces a delete `(k, None)`; a key declared with the same value sends **nothing**.

## Why this example is useful

- **Declare states, not messages.** A topic is a log of events; you only ever talk about row states. Synor owns the gap — it produces the upserts and deletes a hand-rolled producer would, and skips the no-ops.
- **Live mode is one keyword + one flag.** `live=True` on `walk_dir` and `-L` on the CLI is the entire difference between a catch-up run and a streaming one — `process_csv` and the target don't change. No separate "streaming" code path.
- **Survives restarts.** An internal state store remembers the last value sent for every key, so stopping and restarting never re-broadcasts unchanged rows.
- **User-managed topic.** Synor never creates or deletes topics — it produces into one you already own, so it slots into existing Kafka ops.
- **Managed broker ready.** A SASL block in the lifespan covers managed brokers (StreamNative and similar); drop it for a local broker.

## Run it

> Needs a running Kafka broker. Synor never creates topics — you create the one it produces into.

**1. Start a broker & create the topic** — a single-container [Redpanda](https://redpanda.com/) (Kafka-API compatible) is the quickest local broker:

```sh
docker run -d --name redpanda -p 9092:9092 redpandadata/redpanda:latest \
  redpanda start --mode dev-container --smp 1 \
  --kafka-addr PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr PLAINTEXT://localhost:9092

docker exec redpanda rpk topic create synor-csv-rows
```

**2. Configure & install:**

```sh
cp .env.example .env     # set KAFKA_BOOTSTRAP_SERVERS / KAFKA_TOPIC (+ SASL creds for a managed broker)
pip install -e .
```

**3. Run the pipeline** — the example ships a `data/` folder of sample CSVs. Choose catch-up (scan, sync, exit) or live (catch up, then keep watching `./data`):

```sh
# Catch-up run: reconcile the topic up to now, then exit
synor update main.py

# Live run: catch up, then produce on every change
synor update -L main.py
```

**4. Look at the topic** — keys are each row's first column, values the JSON-encoded rows:

```sh
docker exec redpanda rpk topic consume synor-csv-rows --num 10
```

Edit a cell in `data/products.csv` while live mode runs, and a new message with the *same key* appears within a second. The consumer side — kafka_to_lancedb — reads these messages back off the topic and dispatches them into LanceDB tables.

---
