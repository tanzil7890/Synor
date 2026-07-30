<h1 align="center">Turn a slide deck into <em>narrated</em>, searchable audio.</h1>

<p align="center">
  <b>A vision LLM writes speaker notes for each slide, Pocket TTS synthesizes them to audio <em>locally on the CPU</em>, and the notes are embedded into LanceDB — so you search the deck by meaning and play back the narration for any hit.</b><br/>
  A deck is a great outline and a terrible thing to listen to or search; this fixes both — in plain async Python.
</p>

<br/>

A slide deck is a great outline and a terrible thing to *listen to* or *search*. This pipeline fixes both: for each slide, a vision LLM writes natural speaker notes, [Pocket TTS](https://github.com/kyutai-labs/pocket-tts) synthesizes them to audio locally on the CPU, and the notes are embedded into [LanceDB](https://lancedb.com/) so you can search the deck by meaning and play back the narration for any hit. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — the vision and TTS steps run on a `syn.GPU` runner that serializes them off the event loop, and the Rust engine handles incremental processing, so adding a deck processes only its slides.

## How it works

A deck fans out to **slides**, and each slide produces text, audio, and a vector:

- **Render** each slide of the PDF to an image (pymupdf).
- **Narrate** — a vision LLM (via DSPy) writes natural speaker notes for the slide image.
- **Voice + embed** — Pocket TTS synthesizes the notes to MP3 while a sentence-transformer embeds them, concurrently.
- **Store** one LanceDB row per slide — page, notes, audio (a binary column), and the embedding.

`process_file` renders the deck, then mounts one `process_slide` component per page. Each slide component runs the vision LLM, synthesizes audio *and* embeds the notes with `asyncio.gather`, and declares its own row. Read it in [`main.py`](main.py):

```python
@syn.fn(memo=True)  # unchanged slide replays its previously declared row
async def process_slide(slide: SlidePage, filename: str, table: lancedb.TableTarget[SlideRecord]) -> None:
    notes = await extract_speaker_notes(slide.image)                  # vision LLM
    voice, embedding = await asyncio.gather(
        text_to_speech(notes, syn.use_context(TTS_VOICE)),  # Pocket TTS — local CPU
        syn.use_context(EMBEDDER).embed(notes),     # sentence-transformer
    )
    table.declare_row(row=SlideRecord(
        id=f"{filename}#{slide.page_number}", filename=filename, page=slide.page_number,
        speaker_notes=notes, voice=voice, embedding=embedding,
    ))

@syn.fn(memo=True)  # unchanged deck skips reading and rendering entirely
async def process_file(file: FileLike, table: lancedb.TableTarget[SlideRecord]) -> None:
    slides = await pdf_to_slides(await file.read())
    await syn.mount_each(
        process_slide,
        ((slide.page_number, slide) for slide in slides),
        str(file.file_path.path),
        table,
    )
```

The MP3 audio is stored right in the LanceDB row, so a semantic-search hit comes with playable narration attached.

An unchanged PDF is skipped at the `process_file` boundary, before Synor reads or renders it. When a PDF does change, its pages are rendered again to discover the new slide inputs. Each page is then matched to its own memoized component, so unchanged slides carry their existing rows forward while changed slides recompute and synchronize independently.

## Why this example is useful

- **Three modalities, one row.** Each slide becomes text (LLM notes), audio (Pocket TTS MP3), and a vector (sentence-transformer) — declared as a single LanceDB `SlideRecord`.
- **Local TTS, no per-character billing.** Pocket TTS is a fast, ~100M-param neural voice that runs entirely on the CPU — no API, no GPU, no streaming costs; the model and voice state load once via `@functools.cache`.
- **Audio travels with the hit.** The MP3 lives in a binary LanceDB column, so a search result carries its own playable narration.
- **Concurrent per slide.** `asyncio.gather` runs TTS and embedding side by side; the heavy vision and TTS steps run on a `syn.GPU` runner.
- **Incremental & swappable.** Each slide is its own memoized processing component, so an unchanged slide replays its prior LanceDB row without rerunning vision, TTS, or embedding. `LLM_MODEL`, `EMBEDDER`, and `TTS_VOICE` use `detect_change=True`, so configuration changes invalidate the affected work.

## Run it

> Needs **LLM credentials** for the vision model (default `gemini/gemini-2.5-flash` → `GEMINI_API_KEY`) and **ffmpeg** for MP3 export. **Pocket TTS** runs locally on the CPU — its weights download automatically on first run, no GPU or API key required.

**1. Configure & install:**

```sh
cp .env.example .env     # set GEMINI_API_KEY (or swap LLM_MODEL, e.g. OpenAI)
pip install -e .
```

**2. Build the index** — drop a slide-deck PDF into `slides/`, then:

```sh
synor update main        # or: synor update -L main   (keep watching the folder)
```

The first run downloads the Pocket TTS weights (~100M params) from Hugging Face and caches them. On a 3-slide sample deck this produces three LanceDB rows, each with vision-LLM speaker notes and ~170–280 KB of MP3 narration. Pick a different voice with `POCKET_TTS_VOICE` (e.g. `alba`, `charles`, `vera`).

**3. Search the deck** — embed a query the same way and search LanceDB:

```sh
python main.py "reducing latency and reliability"
```

On the sample deck, that query ranks the **Engineering Priorities** slide first — above the roadmap and go-to-market slides — matching the spoken notes by meaning, not keywords. Each hit carries the slide's MP3 narration, ready to play.

---

