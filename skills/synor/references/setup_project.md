# Project Setup Guide

Setting up Synor projects for different use cases.

## Creating a New Project

```bash
synor init my-project
cd my-project
```

This creates: `main.py`, `pyproject.toml`, `README.md`. The generated `main.py` sets the internal database location in its lifespan via `builder.settings.db_path = pathlib.Path("./synor.db")`.

```bash
uv run synor update main.py   # or: pip install -e . && synor update main.py
```

## Dependencies by Use Case

### Vector Embedding Pipeline

```toml
[project]
dependencies = [
    "synor>=0.1.0a1",
    "sentence-transformers",
    "asyncpg",
]
```

### PostgreSQL Integration

```toml
[project]
dependencies = [
    "synor>=0.1.0a1",
    "asyncpg",
]
```

### SQLite Integration

```toml
[project]
dependencies = [
    "synor>=0.1.0a1",
    "sqlite-vec",
]
```

### LanceDB Integration

```toml
[project]
dependencies = [
    "synor>=0.1.0a1",
    "lancedb",
]
```

### Qdrant Integration

```toml
[project]
dependencies = [
    "synor>=0.1.0a1",
    "qdrant-client",
]
```

### Kafka Integration

```toml
[project]
dependencies = [
    "synor>=0.1.0a1",
    "confluent-kafka",
]
```

### LLM-Based Extraction

```toml
[project]
dependencies = [
    "synor>=0.1.0a1",
    "litellm",
    "instructor",
    "pydantic>=2.0",
    "asyncpg",
]
```

---

## Environment Configuration

### `.env` File

The `synor` CLI automatically loads `.env` from the current directory (via `find_dotenv`).

```bash
# Synor internal database (optional fallback).
# Only used if the lifespan does not set builder.settings.db_path.
# The `synor init` template sets db_path in the lifespan instead, so this is not needed there.
SYNOR_DB=./synor.db

# PostgreSQL (if using)
POSTGRES_URL=postgres://user:pass@localhost/db

# Qdrant (if using)
QDRANT_URL=http://localhost:6333

# API keys (if using LLM extraction)
OPENAI_API_KEY=your-openai-api-key
ANTHROPIC_API_KEY=your-anthropic-api-key
```

### Manual Settings (in lifespan)

```python
@syn.lifespan
def synor_lifespan(builder: syn.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./custom.db")
    yield
```

---

## Running Your Pipeline

```bash
pip install -e .                    # Install dependencies
synor update main.py            # Run pipeline
synor update main.py -L         # Run in live mode
synor show main.py              # Show component paths
synor drop main.py -f           # Reset everything
```

---

## Common Issues

### Import Errors

```bash
pip install -e .
```

### Database Connection Errors

Verify database is running and `.env` has correct URLs. See [setup_database.md](./setup_database.md).

---

## See Also

- [Database Setup](./setup_database.md)
- [Patterns](./patterns.md)
- [API Reference](./api_reference.md)
