"""
PDF to Markdown (v1) - Synor pipeline example.

- Walk local PDF files
- Convert PDFs to markdown using docling
- Output markdown files to an output folder
"""

from __future__ import annotations

import pathlib

from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption

import synor as syn
from synor.connectors import localfs
from synor.resources.file import PatternFilePathMatcher


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
    # Get absolute path of the PDF file
    markdown = _converter.convert(
        file.file_path.resolve()
    ).document.export_to_markdown()
    # Replace .pdf extension with .md
    outname = file.file_path.path.stem + ".md"
    localfs.ensure_file(outdir / outname, markdown, create_parent_dirs=True)


@syn.task
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
    )
    await syn.spawn_each(process_file, files.items(), outdir)


app = syn.App(
    "PdfToMarkdown",
    app_main,
    sourcedir=pathlib.Path("./pdf_files"),
    outdir=pathlib.Path("./out"),
)
