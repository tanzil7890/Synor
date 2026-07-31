<h1 align="center">Turn a folder of audio into a <em>transcript</em> table.</h1>

<p align="center">
  <b>Walk a directory of recordings, send each file to a LiteLLM speech-to-text model, and write one transcript row per file into Postgres — keyed by filename.</b><br/>
  A plain table you can query, join, or feed into an embedding pipeline — in plain async Python.
</p>

<br/>

A folder of voice memos, meeting recordings, and podcast clips is dead weight until it's text. Synor walks the directory, sends every file to a [LiteLLM](https://docs.litellm.ai/) transcription model, and writes the result to Postgres as one row per file, keyed by filename. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so only new or changed files get re-transcribed and removed files have their rows cleaned up automatically.

## How it works

The indexing path is the shortest there is — no chunking, one row per file:

- **Walk** a local directory (recursive), matching common audio extensions (`.mp3`, `.wav`, `.m4a`, `.flac`, `.ogg`, `.webm`, `.aac`, `.aiff`).
- **Transcribe** each file with a LiteLLM speech-to-text model (`whisper-1` by default).
- **Store** one `AudioTranscription` row per file in Postgres, keyed by filename.

`process_file` runs once per file: read the audio, transcribe it, declare a single target row. Read it in [`main.py`](main.py):

```python
_transcriber = LiteLLMTranscriber("whisper-1")

@dataclass
class AudioTranscription:
    filename: str
    text: str

@syn.fn(memo=True)   # unchanged file is never re-transcribed
async def process_file(
    file: localfs.File,
    table: postgres.TableTarget[AudioTranscription],
) -> None:
    transcript = await _transcriber.transcribe(file)
    table.declare_row(
        row=AudioTranscription(filename=str(file.file_path.path), text=transcript),
    )
```

`mount_table_target` creates and manages the Postgres table for you with `primary_key=["filename"]` — so each file maps to exactly one row, the table doubles as an index of what's been transcribed, and re-runs upsert only what changed.

## Why this example is useful

- **Swap the model with one string.** `LiteLLMTranscriber("whisper-1")` wraps LiteLLM's transcription API — change that string (and the matching credential) for `elevenlabs/scribe_v1`, a self-hosted endpoint, whatever.
- **The table is the index.** `filename` is the primary key, so the output table doubles as a record of which files have been transcribed — no separate bookkeeping.
- **Incremental by default.** `@syn.fn(memo=True)` skips a file when its content and the function's code are both unchanged, so you never pay for the same transcription twice.
- **Managed Postgres target.** `mount_table_target` handles schema, idempotent upserts, and orphan cleanup — delete a file and its row is removed automatically.
- **Logic changes reconcile too.** Swap the transcription model and Synor re-transcribes against it, compares with what's in Postgres, and applies only the difference.

## Run it

> Needs a running **Postgres** and **LiteLLM credentials** for the transcription model (the default `whisper-1` uses `OPENAI_API_KEY`).

**1. Start Postgres** — a ready compose file ships in the repo:

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install:**

```sh
cp .env.example .env     # set OPENAI_API_KEY; POSTGRES_URL defaults to the local container
pip install -e .
```

**3. Build the table** — drop a few audio files into `audio_files/`, then:

```sh
synor update main.py
```

This writes to `synor_examples.audio_transcriptions`, with `filename` as the primary key and `text` as the transcript.

**4. Check the results** with plain SQL:

```sh
psql "$POSTGRES_URL" -c \
  'SELECT filename, left(text, 200) AS preview FROM synor_examples.audio_transcriptions ORDER BY filename;'
```

Re-running `synor update main.py` incrementally processes only added, changed, and removed files.

---
