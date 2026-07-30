# Contributing

Synor is in alpha and is being prepared for its first independent release.
Contributions should preserve the declarative target-state model and avoid
adding new public API without a concrete use case.

## Local setup

```bash
uv sync --group dev
uv run maturin develop
```

## Checks

Run the checks that match your change:

```bash
cargo test
uv run mypy
uv run pytest python/
uv run ruff format --check .
uv run ruff check .
cargo fmt --check
```

Documentation changes should also pass:

```bash
cd docs
npm run build
```

The detailed code structure, conventions, and test guidance live in
[`AGENTS.md`](AGENTS.md).

Do not publish artifacts while `BRAND_CLEARANCE.md` is not approved or while
`python dev/check_release_readiness.py` reports a release blocker.
