# Synor API Reference

Quick reference for the most commonly used Synor APIs.

## Import Convention

```python
import synor as syn
```

All APIs live directly on the `syn` module alias. There is no separate `synor.asyncio` module.

---

## `@syn.task` Decorator

Mark a function as a Synor processing function.

```python
@syn.task
async def my_function(arg1: str) -> None: ...

@syn.task(cache=True, version=1)
async def expensive_fn(data: str) -> Result: ...

# Force async interface for a sync function (useful for batching)
@syn.task.as_async(cache=True, batching=True)
def batch_embed(texts: list[str]) -> list[NDArray]: ...
```

**Parameters:**
- `memo: bool = False` -- Enable memoization (skip if inputs/code unchanged)
- `version: int | None = None` -- Explicit version bump to force re-execution
- `logic_tracking: "full" | "self" | None = "full"` -- How code changes are tracked
- `batching: bool = False` -- Auto-batch concurrent calls (async only)
- `max_batch_size: int | None = None` -- Max batch size
- `runner: Runner | None = None` -- e.g. `syn.GPU` for serialized GPU execution

---

## Mount APIs (all async)

All mount APIs accept an optional `UnitPath` as their first argument. When omitted, the subpath is auto-derived from `Symbol(fn.__name__)`. Provide an explicit subpath when mounting the same function multiple times, using multi-part paths, or needing a specific path name.

### `syn.spawn()`

Mount a processing component in the background.

```python
# Subpath auto-derived from fn.__name__
handle = await syn.spawn(processor_fn, *args, **kwargs)

# Explicit subpath
handle = await syn.spawn(syn.unit_path("name"), processor_fn, *args, **kwargs)

await handle.ready()  # Optional: wait until component finishes

# Inspect terminal state without raising for component failure/cancellation:
outcome = await handle.outcome()
# Succeeded | Failed(error) | Cancelled | Superseded
```

**Parameters:**
- `subpath` (optional) -- Component subpath. Auto-derived from `fn.__name__` when omitted.
- `processor_fn` -- Function (or LiveComponent class) to run.
- `*args, **kwargs` -- Arguments passed to the function.

**Returns:** `SpawnHandle`. `ready()` preserves the success-or-raise API;
`outcome()` returns a repeatable typed terminal result.

### `syn.call()`

Mount a dependent component and return its result. Parent depends on the child.

```python
# Subpath auto-derived from fn.__name__
result = await syn.call(init_fn, *args, **kwargs)

# Explicit subpath
result = await syn.call(syn.unit_path("setup"), init_fn, *args, **kwargs)
```

**Parameters:**
- `subpath` (optional) -- Component subpath. Auto-derived from `fn.__name__` when omitted.
- `processor_fn` -- Function to run.
- `*args, **kwargs` -- Arguments passed to the function.

**Returns:** The return value of `processor_fn`.

### `syn.spawn_each()`

Mount one component per item in a keyed iterable. Preferred for processing lists.

```python
# Subpath auto-derived from fn.__name__
await syn.spawn_each(process_file, files.items(), *extra_args)

# Explicit subpath
await syn.spawn_each(syn.unit_path("process"), process_file, files.items(), table)
```

**Parameters:**
- `subpath` (optional) -- Component subpath. Auto-derived from `fn.__name__` when omitted.
- `fn` -- Function to run per item. Item value is passed as first argument.
- `items` -- Keyed iterable of `(StableKey, T)` pairs, or a `LiveMapFeed` for live mode.
- `*args, **kwargs` -- Additional arguments passed to `fn` after the item.

**Returns:** `SpawnHandle`

### `syn.attach_target()`

Mount a target state, ensuring the container is applied before returning the child provider.

```python
provider = await syn.attach_target(target_state)
```

Prefer connector convenience methods (`postgres.mount_table_target()`, etc.) which call this internally.

### `syn.map()`

Run a function concurrently on each item. No processing components are created -- pure concurrent execution within the current component.

```python
results = await syn.map(process_chunk, chunks, *extra_args)
```

**Parameters:**
- `fn` -- Async function to apply. Item is passed as first argument.
- `items` -- Iterable or async iterable.
- `*args, **kwargs` -- Additional arguments passed to `fn` after the item.

**Returns:** `list[T]`

### `syn.map_bounded()`

Run a function with bounded task and iterator admission while preserving input
order. Use this for large inputs whose item coroutines do not need a full-group
barrier.

```python
results = await syn.map_bounded(process_chunk, chunks, 32, *extra_args)
```

`max_in_flight` is the third positional argument and must be a positive integer.
After an item failure, no new inputs are pulled and the already-admitted window
is drained. The returned result list remains O(n).

### `syn.map_stream()`

Stream bounded results without retaining an O(n) list. The iterator yields in
completion order and retains at most `max_in_flight` pulled-but-not-yielded
items, including both running tasks and completed results.

```python
async for result in syn.map_stream(process_chunk, chunks, 32, *extra_args):
    await consume(result)
```

Worker and input-iterator failures stop admission, cancel the admitted window,
and raise from the consumer's current or next pull. Caller cancellation does
the same. Fully consume the iterator when possible; when breaking early, wrap
it in `contextlib.aclosing(...)` so admitted work is cancelled promptly.

### `syn.unit_path()`

Create a stable component path for mounting.

```python
syn.unit_path("setup")
syn.unit_path("file", str(file_path))
syn.unit_path("record", record.id)

# Chaining with /
subpath = syn.unit_path("a") / "b" / "c"

# Context manager form (applies to all nested mount calls)
with syn.unit_path("process"):
    for f in files:
        await syn.spawn(syn.unit_path(str(f.path)), process_file, f)
```

**StableKey types:** `str | int | bool | bytes | uuid.UUID | Symbol | tuple[StableKey, ...]`

---

## Context System

### `syn.ContextKey[T]`

Type-safe key for sharing resources. The `key` string is the stable identity across runs.

```python
PG_DB = syn.ContextKey[asyncpg.Pool]("pg_db")
EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder")
```

- `detect_change=True` -- Opt in to auto-invalidate dependent memos when value changes (models, configs)
- `detect_change=False` (default) -- For resources not affecting computation (DB connections, loggers)

### `builder.provide()`

Register a resource in context (used in lifespan).

```python
builder.provide(PG_DB, pool)
builder.provide_with(KEY, context_manager)         # Sync CM
await builder.provide_async_with(KEY, async_cm)    # Async CM
```

### `syn.use_context()`

Retrieve a resource from context inside a processing function.

```python
pool = syn.use_context(PG_DB)
embedder = syn.use_context(EMBEDDER)
```

---

## Lifespan

### `@syn.lifespan`

Define environment setup/teardown. Registered to the default environment.

```python
@syn.lifespan
async def synor_lifespan(builder: syn.EnvironmentBuilder) -> AsyncIterator[None]:
    async with await asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(MODEL))
        yield
```

Can also be sync:

```python
@syn.lifespan
def synor_lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./my.db")
    yield
```

---

## App

### `syn.App`

```python
app = syn.App(
    syn.AppConfig(name="MyApp"),
    main_fn,
    **params,
)

# Async
await app.update()
handle = app.update(live=True)  # Live mode
await app.drop()

# Sync (blocking)
app.update_blocking(report_to_stdout=True)
app.drop_blocking()
```

### `syn.AppConfig`

```python
syn.AppConfig(
    name="MyApp",                    # Required
    environment=env,                 # Optional: custom Environment
    max_inflight_components=1024,    # Optional: concurrency limit
)
```

### Start/Stop (for programmatic usage)

```python
# Context manager (preferred)
async with syn.runtime():
    await app.update()

# Or manually
await syn.start()
try:
    await app.update()
finally:
    await syn.stop()

# Sync variants
with syn.runtime():
    app.update_blocking()
```

---

## Exception Handlers

### Global (in lifespan)

```python
builder.set_exception_handler(my_handler)
```

### Scoped (in processing functions)

```python
async with syn.exception_handler(my_handler):
    await syn.spawn_each(process_file, files.items(), table)
```

### Handler Signature

```python
async def my_handler(exc: BaseException, ctx: syn.ExceptionContext) -> None:
    logger.error(f"Error in {ctx.stable_path}: {exc}")
```

`ExceptionContext` provides: `env_name`, `stable_path`, `processor_name`, `mount_kind`, `parent_stable_path`, `is_background`, `source`, `original_exception`.

---

## Live Mode

Run the pipeline continuously, streaming changes from sources:

```python
# CLI
synor update main.py -L

# Programmatic
handle = app.update(live=True)
async for snapshot in handle.watch():
    print(snapshot.stats)
```

Sources that support live mode:
- `localfs.walk_dir(..., live=True)` -- File watching
- `kafka.create_consumer(config)` -- Kafka consumer construction with both automatic offset mechanisms forced off
- `kafka.topic_as_map(consumer, topics)` -- Kafka topic consumption

---

## CLI Commands

```bash
synor init [PROJECT_NAME]              # Create new project
synor update APP_TARGET                # Run app once
synor update APP_TARGET -L             # Run in live mode
synor update APP_TARGET --full-reprocess  # Force reprocess all
synor update APP_TARGET --reset        # Reset state before running
synor drop APP_TARGET [-f]             # Drop app and all state
synor ls [APP_TARGET] [--db PATH]      # List apps
synor show APP_TARGET [--tree]         # Show component paths
```

**APP_TARGET format:** `main.py`, `main.py:app_name`, `my_module:app_name`

---

## Text Operations

**Import:** `from synor.ops.text import ...`

### `detect_code_language()`

```python
language = detect_code_language(filename="example.py")  # -> "python"
```

### `RecursiveSplitter`

```python
splitter = RecursiveSplitter()
chunks = splitter.split(
    text,
    chunk_size=1000,
    chunk_overlap=200,
    min_chunk_size=300,
    language="python",  # Syntax-aware splitting
)
# Each chunk: Chunk(text=..., start=TextPosition(...), end=TextPosition(...))
```

### `SeparatorSplitter`

```python
splitter = SeparatorSplitter(separators_regex=[r"\n\n", r"\n"])
chunks = splitter.split(text)
```

---

## Embedding Operations

**Import:** `from synor.ops.sentence_transformers import SentenceTransformerEmbedder`

```python
embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
embedding = await embedder.embed(text)  # -> NDArray (float32)
```

**As VectorSchemaProvider:**

```python
EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder")

@dataclass
class Record:
    vector: Annotated[NDArray, EMBEDDER]  # Auto-infer dimensions
```

---

## File Resources

**Import:** `from synor.resources.file import ...`

### `FileLike` (async file object)

```python
text = await file.read_text()
data = await file.read()  # bytes, lazy/cached
fp = await file.content_fingerprint()
```

### `PatternFilePathMatcher`

```python
matcher = PatternFilePathMatcher(
    included_patterns=["**/*.py", "**/*.md"],
    excluded_patterns=[".*/**", "__pycache__/**"],
)
```

---

## ID Generation

**Import:** `from synor.resources.id import ...`

```python
# Deterministic: same dep -> same ID
chunk_id = await generate_id(chunk.text)
chunk_uuid = generate_uuid(chunk.text)

# Distinct per call (even with same dep)
id_gen = IdGenerator()
chunk_id = await id_gen.next_id(chunk.text)

uuid_gen = UuidGenerator()
chunk_uuid = uuid_gen.next_uuid(chunk.text)
```

---

## Vector Schema

**Import:** `from synor.resources.schema import VectorSchema`

```python
# Via ContextKey (preferred -- auto-infer from embedder)
EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder")

@dataclass
class Record:
    vector: Annotated[NDArray, EMBEDDER]

# Explicit dimensions
schema = VectorSchema(dtype=np.dtype(np.float32), size=384)

@dataclass
class Record:
    vector: Annotated[NDArray, schema]
```

---

## See Also

- [Connectors Reference](./connectors.md) -- Database and system connectors
- [Patterns Reference](./patterns.md) -- Common pipeline patterns
- [Setup Project](./setup_project.md) -- Project setup guide
- [Setup Database](./setup_database.md) -- Database setup guide
