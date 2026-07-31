<h1 align="center">Patient intake forms to <em>typed</em> JSON, with BAML.</h1>

<p align="center">
  <b>Declare the <em>Patient</em> schema in BAML, run one type-safe Gemini vision extraction per intake PDF, and get back a validated JSON record — the same shape every time — in plain async Python.</b><br/>
  The hard part isn't reading the PDF; it's getting data that matches a schema so downstream code can trust it.
</p>

<br/>

Intake forms are messy, multi-section PDFs — demographics, insurance, medications, allergies, consent — and the real challenge is getting back data that *matches a schema* every time. This pipeline uses [BAML](https://boundaryml.com/) to declare that schema and run a single type-safe extraction per form against a Gemini vision model. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so only changed PDFs get re-extracted and the LLM call is skipped entirely for forms you've already processed.

## How it works

The schema lives in `baml_src/patient.baml`, not in Python. You describe the `Patient` record as BAML classes; the same file holds the extraction function and the `Gemini` client (pointing at `gemini-3.5-flash-lite`), which reads PDFs natively as vision input — no separate parse or OCR step. Running `baml generate` compiles this into a `baml_client/` package you import from Python: `b.ExtractPatientInfo(...)` and the `Patient` Pydantic model.

The Synor side is two short functions — wrap BAML in a `@syn.fn`, then declare one JSON file per form. Read it in [`main.py`](main.py):

```python
@syn.fn
async def extract_patient_info(content: bytes) -> Patient:
    """Extract patient information from PDF content using BAML."""
    pdf = baml_py.Pdf.from_base64(base64.b64encode(content).decode("utf-8"))
    return await b.ExtractPatientInfo(pdf)


@syn.fn(memo=True)
async def process_patient_form(file: FileLike, outdir: pathlib.Path) -> None:
    content = await file.read()
    patient_info = await extract_patient_info(content)
    patient_json = patient_info.model_dump_json(indent=2)
    output_filename = file.file_path.path.stem + ".json"
    localfs.declare_file(outdir / output_filename, patient_json, create_parent_dirs=True)
```

The return type is `Patient` — the actual Pydantic class BAML generated, not a dict — so everything downstream is typed and validated. There's no prompt engineering or response parsing here; that all lives in the BAML schema, and the LLM call is one `await`. `app_main` walks `data/patient_forms/` for `*.pdf` and runs one `process_patient_form` component per file with `mount_each`.

## Why this example is useful

- **The schema is the contract.** `ExtractPatientInfo(intake_form: pdf) -> Patient` is the whole spec — BAML forces the model's output to conform, so every record has the same shape, ready to load into a database or chart.
- **Native PDF vision, no OCR.** The `Gemini` client reads the PDF directly as vision input — checkboxes, hand-filled fields, tables — with no separate parse or Markdown step.
- **Typed all the way down.** `b.ExtractPatientInfo` returns a generated Pydantic `Patient`, not a string to parse — `model_dump_json` serializes the validated model straight to disk.
- **Incremental by default.** `@syn.fn(memo=True)` skips a form entirely when its bytes and the function's code are unchanged, so you never pay for a second Gemini call on a PDF you've already extracted.
- **Compare libraries on one flow.** A DSPy twin runs the exact same task with a DSPy signature instead of BAML — same input, same output, swap the extraction layer.

## Run it

**1. Install:**

```sh
pip install -e .
```

**2. Generate the BAML client** — compiles `baml_src/patient.baml` into the `baml_client/` package that `main.py` imports (required):

```sh
baml generate
```

**3. Configure** — the extraction uses a Gemini vision model:

```sh
cp .env.example .env     # set GEMINI_API_KEY
```

**4. Run the pipeline** — a catch-up run scans the forms, extracts, writes, and exits:

```sh
synor update main.py
```

This reads each PDF in `data/patient_forms/`, extracts a `Patient`, and writes one JSON file per form to `output_patients/`:

```sh
ls output_patients/
# Patient_Intake_Form_David_Artificial.json
# Patient_Intake_Form_Emily_Artificial.json
# ...one .json per intake PDF
```

Each file is a fully populated, schema-validated patient record — the same shape every time. Edit the BAML schema or swap the model, run `baml generate` again, and the next `synor update main.py` re-extracts only what changed.

---
