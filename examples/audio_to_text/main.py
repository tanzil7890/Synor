"""
Audio to Text (v1) - Synor pipeline example.

- Walk local audio files
- Transcribe each file using LiteLLM
- Store transcripts in Postgres, keyed by filename
"""

from __future__ import annotations

import os
import pathlib
from dataclasses import dataclass
from typing import AsyncIterator

import asyncpg

import synor as syn
from synor.connectors import localfs, postgres
from synor.ops.litellm import LiteLLMTranscriber
from synor.resources.file import PatternFilePathMatcher


DATABASE_URL = os.getenv("POSTGRES_URL", "postgres://synor:synor@localhost/synor")
TABLE_NAME = "audio_transcriptions"
PG_SCHEMA_NAME = "synor_examples"
PG_DB = syn.ContextKey[asyncpg.Pool]("audio_to_text_db")

_transcriber = LiteLLMTranscriber("whisper-1")


@dataclass
class AudioTranscription:
    filename: str
    text: str


@syn.lifespan
async def synor_lifespan(
    builder: syn.EnvironmentBuilder,
) -> AsyncIterator[None]:
    async with await asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        yield


@syn.fn(memo=True)
async def process_file(
    file: localfs.File,
    table: postgres.TableTarget[AudioTranscription],
) -> None:
    transcript = await _transcriber.transcribe(file)
    table.declare_row(
        row=AudioTranscription(
            filename=str(file.file_path.path),
            text=transcript,
        ),
    )


@syn.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            AudioTranscription,
            primary_key=["filename"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=[
                "**/*.aac",
                "**/*.aiff",
                "**/*.flac",
                "**/*.m4a",
                "**/*.mp3",
                "**/*.ogg",
                "**/*.wav",
                "**/*.webm",
            ],
        ),
    )
    await syn.mount_each(process_file, files.items(), target_table)


app = syn.App(
    "AudioToText",
    app_main,
    sourcedir=pathlib.Path("./audio_files"),
)
