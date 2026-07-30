"""
Text Embedding with LanceDB (v1) - Synor pipeline example.

Index (use `-L` for live mode, omit for one-shot catch-up):
    synor update main
    synor update -L main

Query the index:
    python main.py "your query"

Pipeline: walk local markdown files -> chunk -> embed -> store in LanceDB.
"""

from __future__ import annotations

import asyncio
import pathlib
import sys
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import AsyncIterator, Annotated

from numpy.typing import NDArray

import synor as syn
from synor.connectors import lancedb, localfs
from synor.ops.text import RecursiveSplitter
from synor.ops.sentence_transformers import SentenceTransformerEmbedder
from synor.resources.chunk import Chunk
from synor.resources.file import FileLike, PatternFilePathMatcher
from synor.resources.id import IdGenerator


LANCEDB_URI = "./lancedb_data"
TABLE_NAME = "doc_embeddings"
TOP_K = 5


EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
LANCE_DB = syn.ContextKey[lancedb.LanceAsyncConnection]("text_embedding_db")
EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

_splitter = RecursiveSplitter()


@dataclass
class DocEmbedding:
    id: int
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, EMBEDDER]


@syn.lifespan
async def synor_lifespan(
    builder: syn.EnvironmentBuilder,
) -> AsyncIterator[None]:
    conn = await lancedb.connect_async(LANCEDB_URI)
    builder.provide(LANCE_DB, conn)
    builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
    yield


@syn.fn
async def process_chunk(
    chunk: Chunk,
    filename: pathlib.PurePath,
    id_gen: IdGenerator,
    table: lancedb.TableTarget[DocEmbedding],
) -> None:
    table.declare_row(
        row=DocEmbedding(
            id=await id_gen.next_id(chunk.text),
            filename=str(filename),
            chunk_start=chunk.start.char_offset,
            chunk_end=chunk.end.char_offset,
            text=chunk.text,
            embedding=await syn.use_context(EMBEDDER).embed(chunk.text),
        ),
    )


@syn.fn(memo=True)
async def process_file(
    file: FileLike,
    table: lancedb.TableTarget[DocEmbedding],
) -> None:
    text = await file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    id_gen = IdGenerator()
    await syn.map(process_chunk, chunks, file.file_path.path, id_gen, table)


@syn.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await lancedb.mount_table_target(
        LANCE_DB,
        table_name=TABLE_NAME,
        table_schema=await lancedb.TableSchema.from_class(
            DocEmbedding, primary_key=["id"]
        ),
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live=True,  # source supports live watch; pass -L to `synor update` to actually run live
    )
    await syn.mount_each(process_file, files.items(), target_table)


app = syn.App(
    syn.AppConfig(name="TextEmbeddingLanceDBV1"),
    app_main,
    sourcedir=pathlib.Path("./markdown_files"),
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(
    conn: lancedb.LanceAsyncConnection,
    embedder: SentenceTransformerEmbedder,
    query_text: str,
    *,
    top_k: int = TOP_K,
) -> None:
    query_vec = await embedder.embed(query_text)

    table = await conn.open_table(TABLE_NAME)

    search = await table.search(query_vec, vector_column_name="embedding")
    results = await search.limit(top_k).to_list()

    for r in results:
        score = 1.0 - r["_distance"]
        print(f"[{score:.3f}] {r['filename']}")
        print(f"    {r['text']}")
        print("---")


async def query(initial_query: str | None = None) -> None:
    embedder = SentenceTransformerEmbedder(EMBED_MODEL)
    conn = await lancedb.connect_async(LANCEDB_URI)

    if initial_query is not None:
        await query_once(conn, embedder, initial_query)
        return

    while True:
        q = input("Enter search query (or Enter to quit): ").strip()
        if not q:
            break
        await query_once(conn, embedder, q)


if __name__ == "__main__":
    load_dotenv()
    initial = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else None
    asyncio.run(query(initial))
