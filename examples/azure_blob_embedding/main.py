"""
Azure Blob Storage text embedding example.

Index Markdown blobs:
    synor update main

Query the index:
    python main.py "your query"
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from typing import Annotated, AsyncIterator

import asyncpg
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import ContainerClient
from dotenv import load_dotenv
from numpy.typing import NDArray
from pgvector.asyncpg import register_vector

import synor as syn
from synor.connectors import azure_blob, postgres
from synor.ops.sentence_transformers import SentenceTransformerEmbedder
from synor.ops.text import RecursiveSplitter
from synor.resources.chunk import Chunk
from synor.resources.file import PatternFilePathMatcher
from synor.resources.id import IdGenerator


load_dotenv()

DATABASE_URL = os.getenv("POSTGRES_URL", "postgres://synor:synor@localhost/synor")
TABLE_NAME = "azure_blob_doc_embeddings"
PG_SCHEMA_NAME = "synor_examples"
TOP_K = 5

AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
AZURE_STORAGE_CONTAINER = os.environ["AZURE_STORAGE_CONTAINER"]
AZURE_BLOB_PREFIX = os.getenv("AZURE_BLOB_PREFIX", "")

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PG_DB = syn.ContextKey[asyncpg.Pool]("azure_blob_embedding_db")
AZURE_CONTAINER_CLIENT = syn.ContextKey[ContainerClient]("azure_container_client")
EMBEDDER = syn.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

_splitter = RecursiveSplitter()


@syn.lifespan
async def synor_lifespan(
    builder: syn.EnvironmentBuilder,
) -> AsyncIterator[None]:
    credential = DefaultAzureCredential()
    try:
        account_url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net"
        async with asyncpg.create_pool(DATABASE_URL) as pool:
            builder.provide(PG_DB, pool)
            builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))

            async with ContainerClient(
                account_url=account_url,
                container_name=AZURE_STORAGE_CONTAINER,
                credential=credential,
            ) as container_client:
                builder.provide(AZURE_CONTAINER_CLIENT, container_client)
                yield
    finally:
        await credential.close()


@dataclass
class DocEmbedding:
    id: int
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, EMBEDDER]


@syn.fn
async def process_chunk(
    chunk: Chunk,
    filename: str,
    id_gen: IdGenerator,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    table.declare_row(
        row=DocEmbedding(
            id=await id_gen.next_id(chunk.text),
            filename=filename,
            chunk_start=chunk.start.char_offset,
            chunk_end=chunk.end.char_offset,
            text=chunk.text,
            embedding=await syn.use_context(EMBEDDER).embed(chunk.text),
        ),
    )


@syn.fn(memo=True)
async def process_file(
    file: azure_blob.AzureBlobFile,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    text = await file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    id_gen = IdGenerator()
    await syn.map(process_chunk, chunks, file.file_path.path.as_posix(), id_gen, table)


@syn.fn
async def app_main() -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            DocEmbedding,
            primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    client = syn.use_context(AZURE_CONTAINER_CLIENT)
    files = azure_blob.list_blobs(
        client,
        prefix=AZURE_BLOB_PREFIX,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await syn.mount_each(process_file, files.items(), target_table)


app = syn.App(
    syn.AppConfig(name="AzureBlobEmbeddingV1"),
    app_main,
)


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
                text,
                embedding <=> $1 AS distance
            FROM "{PG_SCHEMA_NAME}"."{TABLE_NAME}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec,
            top_k,
        )

    for r in rows:
        score = 1.0 - float(r["distance"])
        print(f"[{score:.3f}] {r['filename']}")
        print(f"    {r['text']}")
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
    initial = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else None
    asyncio.run(query(initial))
