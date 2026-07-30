<h1 align="center">Semantic search over a <em>Google Drive</em> folder.</h1>

<p align="center">
  <b>The Semantic Search 101 pipeline with one thing swapped: it reads documents straight from a <em>Google Drive</em> folder instead of local disk.</b><br/>
  Export Docs/Sheets/Slides to text, chunk, embed locally, store in Postgres + pgvector — incrementally — in plain async Python.
</p>

<br/>

This is Semantic Search 101 with exactly one thing swapped: instead of reading Markdown off local disk, it reads documents straight from a Google Drive folder. Everything downstream — chunking, embedding, and storing the vectors in Postgres with pgvector — is identical. You declare the transformation in native Python and your own types — each work unit declares the outcomes it owns — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so adding one document embeds one document, not the whole folder.

## How it works

The Drive source needs two things, both read from environment variables so nothing is hardcoded: a Google Cloud **service-account JSON key** with Drive access, and the **folder id(s)** to index. `GoogleDriveSource` walks each root folder recursively and yields one `DriveFile` per document; native Docs, Sheets, and Slides are exported to text, and any other file is downloaded as-is. From there a `DriveFile` behaves like any other `FileLike`, so the rest is the base example. Read it in [`main.py`](main.py):

```python
@syn.fn
async def app_main() -> None:
    table = await postgres.mount_table_target(
        PG_DB, table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(DocEmbedding, primary_key=["id"]),
        pg_schema_name=PG_SCHEMA_NAME,
    )
    credential_path = os.environ["GOOGLE_SERVICE_ACCOUNT_CREDENTIAL"]
    root_folder_ids = [f.strip() for f in os.environ["GOOGLE_DRIVE_ROOT_FOLDER_IDS"].split(",") if f.strip()]

    source = google_drive.GoogleDriveSource(
        service_account_credential_path=credential_path,
        root_folder_ids=root_folder_ids,
    )
    await syn.mount_each(process_file, source.items(), table)
```

`source.items()` yields `(key, file)` pairs keyed by the file's name path — exactly what `mount_each` expects — so the engine tracks each Drive file as its own component and updates them independently. `mount_table_target` creates and manages the Postgres table: schema, idempotent upserts, and orphan cleanup when a file disappears from the folder.

## Why this example is useful

- **Drive as a first-class source.** `GoogleDriveSource.items()` drops into the same `mount_each` fan-out as a local folder — the source is a swappable detail, not a rewrite.
- **Docs/Sheets/Slides handled.** Native Google formats are exported to text (Markdown, CSV, plain text); everything else downloads as-is, then `await file.read_text()` works just like a local file.
- **Nothing hardcoded.** The service-account key path and folder id(s) come from environment variables, so forks and deployments configure their own.
- **Incremental by default.** `@syn.fn(memo=True)` skips Drive files whose content and code are unchanged; each row's `id` is derived from its chunk text, so re-running upserts only changed rows and deletes rows whose source is gone.
- **Managed Postgres target.** A single `mount_table_target` owns the schema, idempotent upserts, and orphan cleanup; the same local `all-MiniLM-L6-v2` embedder is reused at query time so indexing and search stay consistent.

## Run it

**1. Start Postgres + pgvector:**

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Set up Google Drive access** — create a Google Cloud service account with Drive access, then share the folder(s) you want to index with the service account's email.

**3. Configure & install:**

```sh
cp .env.example .env     # set GOOGLE_SERVICE_ACCOUNT_CREDENTIAL (JSON key path) and GOOGLE_DRIVE_ROOT_FOLDER_IDS
pip install -e .
```

**4. Build the index** — the `google_drive` source does not support live mode, so this is a one-shot catch-up run:

```sh
synor update main
```

**5. Search** — embeds your query with the *same* model and returns the nearest chunks by pgvector cosine distance:

```sh
python main.py "what is self-attention?"
```

The most semantically similar chunks come back ranked — even when they share none of the words in your query. Re-run `synor update main` to rescan the folders; the engine applies exactly the difference.

---

