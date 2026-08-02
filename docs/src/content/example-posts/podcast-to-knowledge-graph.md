---
title: Turn Podcasts into a Knowledge Graph
description: 'Turn YouTube podcasts into a queryable knowledge graph with Synor V1 — transcription with speaker diarization, two-step LLM extraction, entity resolution with embeddings, and a SurrealDB graph.'
slug: podcast-to-knowledge-graph
image: /docs/images/synor-mark.svg
tags: [knowledge-graph, llm-extraction]
---



Podcasts are one of the richest sources of expert knowledge on the internet. A single Lex Fridman or Dwarkesh Patel episode can contain dozens of substantive claims about people, technologies, and organizations — but it's all locked inside hours of audio. You can't query any of it, and you can't cross-reference what two different guests said about the same topic.

In this tutorial, we'll build a Synor pipeline that turns YouTube podcast episodes into a queryable knowledge graph. It downloads audio, transcribes with speaker diarization, uses an LLM to extract structured statements and entities, resolves duplicates across episodes, and stores everything in [SurrealDB](https://github.com/surrealdb/surrealdb) as a graph.



The whole pipeline is ordinary `async` Python and your own types. The heavy lifting — incremental processing, change tracking, managed graph targets — runs in a Rust engine underneath, so re-running only processes new or changed episodes.


## What we're building

Here's the knowledge graph schema — five node types connected by four relationship types:



A **session** is one podcast episode. A **statement** is a thematic claim extracted from the conversation — e.g. "Scaling laws suggest that larger models will continue to improve." Each statement is linked to who said it and what it mentions. **Person**, **tech**, and **org** are named entities.

The tricky part: the same entity appears under different names across episodes ("GPT-4", "GPT4", "OpenAI's GPT-4"). We collapse these with entity resolution — more on that below.

## Pipeline overview

The pipeline runs in three phases.



1. **Per-session processing** — for each episode: download, transcribe, and extract metadata, speakers, and statements with an LLM. Sessions and statements are written immediately; they don't need cross-episode dedup.
2. **Entity resolution** — collect every raw entity name across episodes and deduplicate them with embedding similarity + LLM confirmation.
3. **Knowledge base creation** — write the canonical entities and all relationships.



You declare the transformation with native Python; Synor works out what to insert, update, and delete. Think: **each work unit declares the outcomes it owns**.

## Phase 1: per-session processing



Each session goes through a multi-step pipeline, starting from a YouTube URL.

### Fetch the transcript

We download the audio with `yt-dlp` and transcribe it with AssemblyAI, which returns speaker-diarized utterances ("Speaker A", "Speaker B", …) plus YouTube metadata.

```python title="conv_knowledge/fetch.py"
@syn.task(cache=True)
async def fetch_transcript(youtube_id: str) -> SessionTranscript:
    url = f"https://www.youtube.com/watch?v={youtube_id}"
    with tempfile.TemporaryDirectory() as tmpdir:
        # Download audio via yt-dlp, convert to mp3 (FFmpeg)
        ydl_opts = {"format": "bestaudio/best", "outtmpl": f"{tmpdir}/audio.mp3", ...}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
        # Transcribe with AssemblyAI speaker diarization
        transcript = aai.Transcriber().transcribe(
            f"{tmpdir}/audio.mp3", aai.TranscriptionConfig(speaker_labels=True)
        )
    utterances = [Utterance(speaker=u.speaker, text=u.text) for u in transcript.utterances]
    return SessionTranscript(utterances=utterances, yt_title=info["title"], ...)
```

`@syn.task(cache=True)` **memoizes** the function: if you've already fetched and transcribed a video, re-running skips it entirely — essential when you're iterating on downstream extraction and don't want to re-download hours of audio every time.

### Two-step LLM extraction



There's a bootstrapping problem: to attribute statements correctly, the LLM needs to know who the speakers are — but the raw transcript only has generic labels like "Speaker A". So extraction runs in two passes, both using a shared `format_transcript()` that swaps diarization labels for names.

**Step 1 — identify speakers and extract metadata.** Format the transcript with generic labels, give the LLM the YouTube metadata as context, and get back typed speaker identifications. The output is a Pydantic model, enforced by [instructor](https://github.com/instructor-ai/instructor) over LiteLLM:

```python title="conv_knowledge/extract.py"
@syn.task(cache=True)
async def extract_metadata(reformatted_transcript: str, transcript: SessionTranscript) -> SessionMetadata:
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    return await client.chat.completions.create(
        model=syn.use_context(LLM_MODEL),
        response_model=SessionMetadata,
        messages=[{"role": "system", "content": METADATA_PROMPT}, {"role": "user", "content": ...}],
    )
```

```python title="conv_knowledge/models.py"
class SpeakerIdentification(pydantic.BaseModel):
    label: str   # "A", "B"
    name: str    # "Lex Fridman" — unidentifiable speakers are excluded

class SessionMetadata(pydantic.BaseModel):
    name: str
    description: str | None
    date: str | None
    speakers: list[SpeakerIdentification]
```

**Step 2 — extract statements with real names.** Now that "Speaker A" is "Lex Fridman", reformat the transcript with real names and extract thematic statements, each with its speakers and mentioned entities:

```python title="conv_knowledge/models.py"
class RawStatement(pydantic.BaseModel):
    statement: str               # "Scaling laws suggest larger models will improve"
    speakers: list[str]          # ["Lex Fridman"]
    mentioned_person: list[str]  # ["Ilya Sutskever"]
    mentioned_tech: list[str]    # ["Large language model"]
    mentioned_org: list[str]     # ["OpenAI"]
```

Every name must be **self-contained** — the prompt forbids pronouns, speaker labels, or contextual references — because statements from different episodes get cross-referenced later, and a name like "he" or "the host" is meaningless outside its transcript.

### Declare the session and statements

After extraction we declare the session and its statements as records in SurrealDB. IDs come from Synor's `IdGenerator`, which is stable — the same inputs always yield the same ID, so re-running never duplicates. `next_id(content)` folds the content in, so an ID stays stable even if statement order changes.

```python title="conv_knowledge/app.py"
id_gen = IdGenerator(youtube_id)
session_id = await id_gen.next_id()
session_table.declare_record(row=Session(id=session_id, youtube_id=youtube_id, name=metadata.name, transcript=step2_text, ...))

for stmt in stmt_extraction.statements:
    stmt_id = await id_gen.next_id(stmt.statement)
    statement_table.declare_record(row=Statement(id=stmt_id, statement=stmt.statement))
    session_statement_rel.declare_relation(from_id=session_id, to_id=stmt_id)
```

Each session runs as an independent processing component via `syn.call`, keyed by the YouTube ID — so adding an episode only processes that episode:

```python title="conv_knowledge/app.py"
raw = await syn.call(
    syn.unit_path("session", youtube_id),
    process_session, youtube_id,
    session_table, statement_table, session_statement_rel,
)
```

`process_session` returns the raw entity names and statement linkages that Phases 2 and 3 need. Sessions and statements are already in SurrealDB; the raw entities are carried forward for dedup.

## Phase 2: entity resolution



Now we have a pile of raw names from every episode, with the same entity under many spellings ("GPT-4" vs "GPT4", "Sam Altman" vs "Samuel Altman"). Synor ships an `entity_resolution` utility that collapses them: it embeds each name, finds near-matches by vector similarity, and asks an LLM to confirm only the close pairs — cheap embeddings filter the field, expensive LLM calls happen only where it's ambiguous.

```python title="conv_knowledge/app.py"
@syn.task(cache=True)
async def _resolve_entities(all_raw_entities: set[str]) -> dict[str, str | None]:
    result = await resolve_entities(
        entities=all_raw_entities,
        embedder=syn.use_context(EMBEDDER),               # Snowflake/snowflake-arctic-embed-xs
        resolve_pair=LlmPairResolver(model=syn.use_context(RESOLUTION_LLM_MODEL)),
    )
    return result.to_dict()  # {"Apple Inc.": None, "Apple": "Apple Inc.", "AAPL": "Apple Inc."}
```

Resolution runs independently per entity type, so Synor processes person, tech, and org concurrently:

```python title="conv_knowledge/app.py"
entity_dedup = dict(zip(
    [cfg.name for cfg in ENTITY_TYPES],
    await asyncio.gather(*(
        syn.call(syn.unit_path("resolve", cfg.name),
                       _resolve_entities, _collect_all_raw(all_session_raw, cfg.name))
        for cfg in ENTITY_TYPES
    )),
))
```

A small, cheaper model handles these confirmations (configurable via `RESOLUTION_LLM_MODEL`).

## Phase 3: knowledge base creation

With the dedup maps ready, we write the final graph. Canonical entities become nodes; every relationship uses resolved names. `resolve_canonical(name, dedup)` chases the dedup chain to the root — `resolve_canonical("AAPL", dedup)` → `"Apple Inc."`.

```python title="conv_knowledge/app.py"
@syn.task
async def create_knowledge_base(all_session_raw, entity_dedup, entity_tables, ...):
    # Canonical entity nodes (name is the id)
    for cfg in ENTITY_TYPES:
        for name, upstream in entity_dedup[cfg.name].items():
            if upstream is None:                      # this name is canonical
                entity_tables[cfg.name].declare_record(row=Entity(id=name, name=name))

    # Relationships, using canonical names
    for session_raw in all_session_raw:
        for stmt in session_raw.statements:
            for cfg in ENTITY_TYPES:
                dedup = entity_dedup[cfg.name]
                for canonical in {resolve_canonical(e, dedup) for e in getattr(stmt.raw, f"mentioned_{cfg.name}")}:
                    statement_mentions_rel.declare_relation(
                        from_id=stmt.id, to_id=canonical,
                        to_table=entity_tables[cfg.name])   # polymorphic target
```

The `statement_mentions` relationship is **polymorphic** — its target can be a person, tech, or org table — and `to_table` tells Synor which table the target ID belongs to. The targets themselves are mounted once in `app_main`:

```python title="conv_knowledge/app.py"
statement_mentions_rel = await surrealdb.mount_relation_target(
    SURREAL_DB, "statement_mentions", statement_table,
    [entity_tables[cfg.name] for cfg in ENTITY_TYPES],   # polymorphic TO
)
```

## Incremental updates

This isn't a one-shot job — you'll add episodes over time and evolve the schema. Synor's memoization and component model make both efficient.

**Adding episodes.** A new URL re-runs the pipeline, but only the new episode is processed: `fetch_transcript` and both extraction steps are memoized for existing episodes, entity resolution reuses cached embeddings and decisions and only makes fresh LLM calls for genuinely new names, and the declarative targets diff the rest. Removing an episode deletes its component — so its session, statements, and relationships are cleaned out of SurrealDB automatically.

**Evolving the schema.** Say you add a `Product` entity type:

| Pipeline step | What happens | Why |
|---|---|---|
| Fetch transcript | **Reused** | Memoized, input unchanged |
| Step 1: speaker identification | **Reused** | Prompt unchanged |
| Step 2: statement extraction | **Re-runs** | Extraction prompt changed |
| Entity resolution (person, tech, org) | **Reused** | Raw entities unchanged |
| Entity resolution (product) | **Runs fresh** | New type |
| Knowledge base creation | **Re-declared** | New nodes + relationships |

The expensive operations — download, transcription, speaker ID — are fully reused. Add one entity type across 50 episodes and you re-run only the statement-extraction calls plus resolution for the new type.

## Run the pipeline

You'll need Python 3.11+, [FFmpeg](https://ffmpeg.org/), Docker, an [AssemblyAI API key](https://www.assemblyai.com/) (transcription), and an OpenAI API key (extraction).

Start SurrealDB:

```sh
docker run -d --name surrealdb --user root -p 8787:8000 \
  -v surrealdb-data:/data surrealdb/surrealdb:latest \
  start --user root --pass root surrealkv:/data/database
```

Set keys and install:

```sh
export ASSEMBLYAI_API_KEY="..."
export OPENAI_API_KEY="sk-..."
pip install -e .
```

Add YouTube URLs to `input/sample.txt` (one per line, `#` for comments), then build the graph — incremental, so re-running skips episodes already processed:

```sh
synor update conv_knowledge.app
```

## Explore the results

SurrealDB ships [Surrealist](https://surrealdb.com/surrealist), a built-in explorer. Connect to `ws://localhost:8787`, namespace `synor`, database `yt_conversations`. The graph view shows persons (blue) linked to the statements (pink) they made:



You can also run analytical queries — for example, which technologies are mentioned by the most distinct people across every episode:

```surql
SELECT name,
  array::len(array::distinct(
    <-statement_mentions<-statement<-person_statement<-person.id
  )) AS person_count
FROM tech
ORDER BY person_count DESC
LIMIT 15;
```



A few more:

```surql
-- All statements a person made
SELECT <-person_statement<-person.name AS speaker, statement FROM statement;

-- Everything involved in each statement
SELECT statement,
  ->statement_mentions->person.name AS persons,
  ->statement_mentions->tech.name AS techs,
  ->statement_mentions->org.name AS orgs
FROM statement;
```

## Run it
