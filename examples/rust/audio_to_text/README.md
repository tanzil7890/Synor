# Audio to Text (Rust)

Rust port of the Python [`audio_to_text`](../../audio_to_text) example.

Walks local audio files, transcribes each with OpenAI Whisper, and stores one
row per file in Postgres — keyed by filename, so the table is an index of
available transcriptions.

## Parallel to the Python example

| Concern          | Python                                   | Rust (this example)                                  |
| ---------------- | ---------------------------------------- | ---------------------------------------------------- |
| Source           | `localfs.walk_dir`                       | `synor::fs::walk`                                |
| Per-file compute | `@syn.fn(memo=True) process_file`       | `#[synor::function(memo)] transcribe`            |
| Transcription    | `LiteLLMTranscriber("whisper-1")`        | OpenAI `/v1/audio/transcriptions` (`whisper-1`)      |
| Target           | `postgres.mount_table_target`            | `postgres::mount_table_target`                       |

Incrementality: unchanged audio files are memo-skipped; rows for removed files
are reconciled away (the managed `TableTarget` deletes orphaned rows).

Target table is `synor_examples.audio_transcriptions` with `filename` (primary
key) and `text`.

## Run

```bash
export POSTGRES_URL=postgres://synor:synor@localhost/synor
export OPENAI_API_KEY=...

cargo run                     # walk ./audio_files -> transcribe -> Postgres
cargo run -- /path/to/audio   # custom source directory
```
