"""
Code Embedding (v1) - Synor pipeline example.

Index live (runs once and keeps watching for changes):
    synor update -L main

Query the index:
    python main.py "your query"

Pipeline: walk -> detect language -> chunk -> embed -> store in pgvector.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import sys
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import AsyncIterator, Annotated

import asyncpg
from pgvector.asyncpg import register_vector
from numpy.typing import NDArray

import synor as syn
from synor.connectors import localfs, postgres
from synor.ops.text import RecursiveSplitter, detect_code_language
from synor.ops.sentence_transformers import SentenceTransformerEmbedder
from synor.resources.chunk import Chunk
from synor.resources.file import FileLike, PatternFilePathMatcher
from synor.resources.id import IdGenerator


DATABASE_URL = os.getenv("POSTGRES_URL", "postgres://synor:synor@localhost/synor")
TABLE_NAME = "code_embeddings"
PG_SCHEMA_NAME = "synor_examples"
TOP_K = 5


EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PG_DB = syn.ContextKey[asyncpg.Pool]("code_embedding_db")
EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

_splitter = RecursiveSplitter()


@dataclass
class CodeEmbedding:
    id: int
    filename: str
    code: str
    embedding: Annotated[NDArray, EMBEDDER]
    start_line: int
    end_line: int


@syn.lifespan
async def synor_lifespan(
    builder: syn.EnvironmentBuilder,
) -> AsyncIterator[None]:
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
        yield


@syn.task
async def process_chunk(
    chunk: Chunk,
    filename: pathlib.PurePath,
    id_gen: IdGenerator,
    table: postgres.TableTarget[CodeEmbedding],
) -> None:
    embedding = await syn.use_context(EMBEDDER).embed(chunk.text)
    table.ensure_row(
        row=CodeEmbedding(
            id=await id_gen.next_id(chunk.text),
            filename=str(filename),
            code=chunk.text,
            embedding=embedding,
            start_line=chunk.start.line,
            end_line=chunk.end.line,
        ),
    )


@syn.task(cache=True)
async def process_file(
    file: FileLike,
    table: postgres.TableTarget[CodeEmbedding],
) -> None:
    text = await file.read_text()
    language = detect_code_language(filename=str(file.file_path.path.name))
    chunks = _splitter.split(
        text,
        chunk_size=1000,
        min_chunk_size=300,
        chunk_overlap=300,
        language=language,
    )
    id_gen = IdGenerator()
    await syn.map(process_chunk, chunks, file.file_path.path, id_gen, table)


@syn.task
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            CodeEmbedding,
            primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )
    target_table.declare_vector_index(column="embedding")

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=[
                "**/*.py",
                "**/*.rs",
                "**/*.toml",
                "**/*.md",
                "**/*.mdx",
            ],
            excluded_patterns=["**/.*", "**/target", "**/node_modules"],
        ),
        live=True,  # source supports live watch; pass -L to `synor update` to actually run live
    )
    await syn.spawn_each(process_file, files.items(), target_table)


app = syn.App(
    syn.AppConfig(name="CodeEmbeddingV1"),
    app_main,
    sourcedir=pathlib.Path(__file__).parent / ".." / "..",  # Index from repository root
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(
    pool: asyncpg.Pool,
    embedder: SentenceTransformerEmbedder,
    query: str,
    *,
    top_k: int = TOP_K,
) -> None:
    query_vec = await embedder.embed(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                filename,
                code,
                embedding <=> $1 AS distance,
                start_line,
                end_line
            FROM "{PG_SCHEMA_NAME}"."{TABLE_NAME}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec,
            top_k,
        )

    for r in rows:
        score = 1.0 - float(r["distance"])
        print(f"[{score:.3f}] {r['filename']} (L{r['start_line']}-L{r['end_line']})")
        print(f"    {r['code']}")
        print("---")


async def query(initial_query: str | None = None) -> None:
    embedder = SentenceTransformerEmbedder(EMBED_MODEL)
    async with asyncpg.create_pool(DATABASE_URL, init=register_vector) as pool:
        if initial_query is not None:
            await query_once(pool, embedder, initial_query)
            return

        while True:
            q = input("Enter search query (or Enter to quit): ").strip()
            if not q:
                break
            await query_once(pool, embedder, q)


if __name__ == "__main__":
    load_dotenv()
    initial = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else None
    asyncio.run(query(initial))
