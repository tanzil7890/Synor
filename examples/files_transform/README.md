<h1 align="center">The smallest <em>observe → own → reconcile</em> pipeline.</h1>

<p align="center">
  <b>Watch a folder of Markdown, render each file to HTML with <em>markdown-it-py</em>, and write the <code>.html</code> outputs to a folder that stays in sync.</b><br/>
  No database, no embeddings, no API keys — files in, files out, in plain async Python.
</p>

<br/>

Take a folder of Markdown files, render each one to HTML, and write the results to a second folder that stays in sync with the source. It's the smallest complete Synor pipeline, and the cleanest way to see the **observe → own → reconcile** shape that every larger example is built from. You declare the transformation in native Python — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, watching the directory, keeping the output folder in sync) runs in a Rust engine underneath, so only the files that actually changed get re-rendered.

## How it works

The whole pipeline is about 25 lines. `process_file` reads the Markdown, renders it to HTML, derives a flat output name from the source path, and declares the output file as a target state; `app_main` walks the source folder for `*.md` and mounts one component per file. Read all of [`main.py`](main.py):

```python
_markdown_it = MarkdownIt("gfm-like")

@syn.fn(memo=True)
async def process_file(file: FileLike, outdir: pathlib.Path) -> None:
    html = _markdown_it.render(await file.read_text())
    outname = "__".join(file.file_path.path.parts) + ".html"
    localfs.declare_file(outdir / outname, html, create_parent_dirs=True)

@syn.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live=True,
    )
    await syn.mount_each(process_file, files.items(), outdir)
```

The transform itself is just two lines: read the text, render it. The output name joins the source path parts with `__`, so `subdir/file.md` becomes `subdir__file.html` — a flat, collision-free name. `localfs.declare_file` describes the file you *want to exist*; Synor writes it, overwrites it on change, and deletes it when the source Markdown is gone.

## Why this example is useful

- **The whole method, minimized.** Observe, own, and reconcile in about 25 lines, with no database or embeddings.
- **Your transform is just a function.** `_markdown_it.render` is plain Python; swap it for any function and you have a different pipeline.
- **Managed file targets.** `localfs.declare_file` handles writing, overwriting on change, and deleting the `.html` when the source `.md` disappears — you never write file I/O glue.
- **Incremental by default.** `@syn.fn(memo=True)` skips a file whose content and code are unchanged; add, edit, or delete one Markdown file and only that file's HTML moves.
- **Live without re-scanning.** The filesystem source declares `live=True` — pass `-L` and it keeps watching the directory, applying each change with low latency.

## Run it

**1. Install** (no external services required):

```sh
pip install -e .
```

**2. Add some Markdown** — the example ships a `data/` folder of sample files, or drop your own in. The `.env` sets `SYNOR_DB=./synor.db` for internal state.

**3. Build the output folder** — catch-up (scan, sync, exit) or live (catch up, then keep watching):

```sh
synor update main        # catch-up
synor update -L main     # live: keep watching for file changes
```

The converted files appear in `./output_html/`, one `.html` per source `.md` (named by the source path parts joined with `__`, e.g. `subdir__file.html`).

**4. Try incremental updates** — add, edit, or delete a `.md` in `data/` and re-run: only the changed file is re-rendered, and a removed source's `.html` is deleted automatically.

---
