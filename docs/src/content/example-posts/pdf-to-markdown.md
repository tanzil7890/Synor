---
title: PDF to Markdown
description: 'Convert PDF files to Markdown with incremental processing'
slug: pdf-to-markdown
image: /docs/images/synor-mark.svg
tags: [pdf, custom-building-blocks]
---



In this tutorial, we'll build a simple app that converts PDF files to Markdown and saves them to a local directory.


## Overview




1. Read PDF files from a local directory
2. Convert each file to Markdown using Docling
3. Save the Markdown files to an output directory (as **target states**)

You declare the transformation logic with native Python without worrying about changes.

Think:
**each work unit declares the outcomes it owns**

When your source data is updated, or your processing logic is changed (for example, switching parsers or tweaking conversion settings), Synor performs smart incremental processing that only reprocesses the minimum. And it keeps your Markdown files always up to date in production.

## Setup

1. Install Synor and dependencies:

    ```bash
    pip install 'synor>=0.1.0a1' docling
    ```

2. Create a new directory for your project:

    ```bash
    mkdir pdf-to-markdown
    cd pdf-to-markdown
    ```

3. Create a `pdf_files/` directory and add your PDF files:

    ```bash
    mkdir pdf_files
    ```
    You can download sample PDF files from the git repo.

4. Create a `.env` file to configure the database path:

    ```bash
    echo "SYNOR_DB=./synor.db" > .env
    ```

## Define the app

Define a Synor App — the top-level runnable unit in Synor.



```python title="main.py"
import pathlib

import synor as syn
from synor.connectors import localfs
from synor.resources.file import PatternFilePathMatcher
from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
```

[→ Synor App](/docs/programming_guide/app)

### Define the main function



In the main function, we walk through each file in the source directory and process it.

```python title="main.py"
@syn.task
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
    )
    await syn.spawn_each(process_file, files.items(), outdir)
```
For each file, `syn.spawn_each()` mounts a processing component. It's up to
you to pick the process granularity, for example it can be at directory level,
file level, or page level.

In this example, because we want to independently convert each file to Markdown, it is the most natural to pick it at the file level.

[→ Processing Component](/docs/programming_guide/processing_component)


### Define file processing



For a file, we use Docling to convert it to Markdown. The converter follows
Docling's [explicit accelerator configuration](https://docling-project.github.io/docling/_generated/examples/run_with_accelerator/)
pattern and is pinned to CPU for portability across local machines. The
Docling accelerator docs were checked on 2026-05-31; Docling documents CPU as
the mode that works everywhere, while MPS/CUDA/XPU depend on compatible
hardware and PyTorch builds.

```python title="main.py"
_pipeline_options = PdfPipelineOptions(
    accelerator_options=AcceleratorOptions(device=AcceleratorDevice.CPU)
)
_converter = DocumentConverter(
    format_options={
        InputFormat.PDF: PdfFormatOption(pipeline_options=_pipeline_options)
    }
)

@syn.task(cache=True)
def process_file(
    file: localfs.File,
    outdir: pathlib.Path,
) -> None:
    markdown = _converter.convert(
        file.file_path.resolve()
    ).document.export_to_markdown()
    outname = file.file_path.path.stem + ".md"
    localfs.ensure_file(outdir / outname, markdown, create_parent_dirs=True)
```

We use `@syn.task` with `cache=True` to create a memoized function that processes each file.

[→ Function](/docs/programming_guide/function)

### Create the App

```python title="main.py"
app = syn.App(
    "PdfToMarkdown",
    app_main,
    sourcedir=pathlib.Path("./pdf_files"),
    outdir=pathlib.Path("./out"),
)
```

## Run the pipeline

Run the pipeline:

```bash
synor update main.py
```

Synor will:

1. Create the `out/` directory
2. Convert each PDF in `pdf_files/` to Markdown in `out/`

Check the output:

```bash
ls out/
# example.md (one .md file for each input PDF)
```

## Incremental updates

The power of Synor is **incremental processing**. Try these:

**Add a new file:**

Add a new PDF to `pdf_files/`, then run:

```bash
synor update main.py
```

Only the new file is processed.

**Modify a file:**

Replace a PDF in `pdf_files/` with an updated version, then run:

```bash
synor update main.py
```

Only the changed file is reprocessed.

**Delete a file:**

```bash
rm pdf_files/example.pdf
synor update main.py
```

The corresponding Markdown file is automatically removed.
