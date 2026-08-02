<h1 align="center">Convert a folder of PDFs to <em>Markdown</em>.</h1>

<p align="center">
  <b>Walk a directory of PDFs, convert each one to clean Markdown with <em>docling</em>, and write the <code>.md</code> files to an output folder that stays in sync.</b><br/>
  No database, no embeddings — just files in, files out, in plain Python.
</p>

<br/>

Convert a folder of PDFs to Markdown and write the results to a second folder that stays in sync with the source. No database, no embeddings, no API keys — just files in, files out. You declare the transformation in native Python — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed file targets) runs in a Rust engine underneath, so switching parsers or replacing one PDF reprocesses only the minimum.

## How it works

A single docling `DocumentConverter` is built once and pinned to CPU for portability across machines. `process_file` runs once per PDF: it converts the file to Markdown, derives the output name by swapping the extension, and declares the `.md` file as a target state. Read it in [`main.py`](main.py):

```python
_converter = DocumentConverter(
    format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=_pipeline_options)}
)

@syn.task(cache=True)
def process_file(file: localfs.File, outdir: pathlib.Path) -> None:
    markdown = _converter.convert(file.file_path.resolve()).document.export_to_markdown()
    outname = file.file_path.path.stem + ".md"
    localfs.ensure_file(outdir / outname, markdown, create_parent_dirs=True)

@syn.task
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
    )
    await syn.spawn_each(process_file, files.items(), outdir)
```

`spawn_each` runs one processing component per PDF, so the engine tracks and updates each file independently — it's up to you to pick the granularity (directory, file, or page); file level is the natural choice here.

## Why this example is useful

- **Clean Markdown, not raw text dumps.** docling preserves headings, tables, and reading order — the structure that makes the output actually usable.
- **Managed file targets.** `localfs.ensure_file` describes the file you *want to exist*; Synor writes it, overwrites it when the source changes, and deletes its `.md` when the source PDF is gone.
- **Incremental by default.** `@syn.task(cache=True)` skips a PDF whose content and code are unchanged, so docling never re-parses a file you've already converted — add one PDF and only that file is processed.
- **Pick your granularity.** `spawn_each` mounts one component per file here, but the same shape works at directory or page level — your choice.
- **No services, runs anywhere.** Pure local CPU processing, no database or API keys to set up.

## Run it

**1. Install:**

```sh
pip install -e .
```

**2. Add some PDFs** — the example ships a `pdf_files/` folder (the "Attention Is All You Need" paper), or drop your own in. The `.env` sets `SYNOR_DB=./synor.db` for internal state.

**3. Convert** — writes Markdown into `out/`, one `.md` per input PDF:

```sh
synor update main
```

**4. Check the output:**

```sh
ls out/      # e.g. 1706.03762v7.md
```

Add, replace, or delete a PDF in `pdf_files/` and re-run `synor update main` — only the changed file is reprocessed, and a removed PDF's `.md` is deleted automatically.

---
