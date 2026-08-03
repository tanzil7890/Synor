import base64
import pathlib

import synor as syn
from synor.resources.file import FileLike, PatternFilePathMatcher
from synor.connectors import localfs
from baml_client import b
from baml_client.types import Patient
import baml_py


@syn.task
async def extract_patient_info(content: bytes) -> Patient:
    """Extract patient information from PDF content using BAML."""
    pdf = baml_py.Pdf.from_base64(base64.b64encode(content).decode("utf-8"))
    return await b.ExtractPatientInfo(pdf)


@syn.task(cache=True)
async def process_patient_form(file: FileLike, outdir: pathlib.Path) -> None:
    """Process a patient intake form PDF and extract structured information."""
    content = await file.read()
    patient_info = await extract_patient_info(content)
    patient_json = patient_info.model_dump_json(indent=2)
    output_filename = file.file_path.path.stem + ".json"
    localfs.ensure_file(outdir / output_filename, patient_json, create_parent_dirs=True)


@syn.task
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    """Main application function that processes patient intake forms."""
    files = localfs.walk_dir(
        sourcedir,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
    )
    await syn.spawn_each(process_patient_form, files.items(), outdir)


app = syn.App(
    syn.AppConfig(name="PatientIntakeExtractionBaml"),
    app_main,
    sourcedir=pathlib.Path("./data/patient_forms"),
    outdir=pathlib.Path("./output_patients"),
)
