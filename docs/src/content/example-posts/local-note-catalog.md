---
title: Local Note *Catalog*
description: 'Run Synor entirely locally: turn Markdown notes into JSON, repeat the run, then change one note and refresh one outcome.'
slug: local-note-catalog
image: /docs/images/synor-mark.svg
tags: [local, incremental, files]
---

This is the first field note because it makes Synor’s behavior visible without
adding infrastructure. Two Markdown files go in. Two JSON records belong in the
catalog. The interesting part begins when the command runs again.

## The three questions

1. What changed?
2. What work ran?
3. What outcome was repaired?

On the first run, both notes are new. On the second run, both work units are
settled and reusable. After `notes/deploy.md` changes, only that work unit runs
and only `catalog/deploy.json` is rewritten.

## The reusable work

```python
@syn.task(cache=True)
async def catalog_note(file: FileLike, catalog_dir: pathlib.Path) -> None:
    record = _note_record(await file.read_text())
    localfs.ensure_file(
        catalog_dir / f"{file.file_path.path.stem}.json",
        json.dumps(record, indent=2) + "\n",
        create_parent_dirs=True,
    )
```

`catalog_note` owns one JSON outcome. Memoization allows the function to remain
settled when its note and implementation are unchanged.

## The stable work paths

```python
@syn.task
async def app_main(notes_dir: pathlib.Path, catalog_dir: pathlib.Path) -> None:
    notes = localfs.walk_dir(
        notes_dir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await syn.spawn_each(catalog_note, notes.items(), catalog_dir)
```

The keyed items returned by `walk_dir` give each note a durable processing
component path. If a note disappears, the path disappears and its JSON outcome
is removed.

## Run it

From the repository root:

```bash
uv run maturin develop
cd examples/local_note_catalog
../../.venv/bin/synor update main.py
```

Run the update command twice. Then edit one note and run it a third time. The
stats report shows reuse and refresh separately.

The full source lives in `examples/local_note_catalog`.
