# What to Build on Synor

An in-depth product/business analysis, grounded in what the codebase actually
gives us and in current market research (August 2026).

Structure:

- **Part 0** — honest inventory: what Synor is worth, and what it isn't
- **Part 1** — ideas against the *data entropy / multimodal chaos* thesis
- **Part 2** — ideas against the *AI-native data stack* thesis
- **Part 3** — wildcards not tied to either thesis
- **Part 4** — scoring, recommendation, and a 90-day plan

---

## Part 0 — What we are actually starting with

Before ideating, be precise about the asset. Product strategy built on a
flattering read of your own codebase is how startups die.

### 0.1 The seven real capabilities

| # | Capability | Where it lives | Why it's rare |
|---|---|---|---|
| 1 | **Ownership-keyed lineage** — every output traces to a stable component path | `rust/core/src/state/stable_path.rs` | Most pipelines lose the link between source item and derived artifact. This is the single most valuable primitive here. |
| 2 | **Deletion propagation** — source vanishes → derived outputs are removed | `rust/core/src/engine/target_state.rs` | Almost nobody does this. It is the #1 unsolved RAG failure ("ghost vectors"). |
| 3 | **Code-aware invalidation** — change a function body, its cached results invalidate | `rust/utils/src/fingerprint.rs`, `_internal/function.py` | Tools cache on *data* hashes. Caching on *logic* hashes too is unusual and is what makes "we changed the chunker" safe. |
| 4 | **Reconciliation to 20 backends** | `python/synor/connectors/` | Postgres, Qdrant, LanceDB, Neo4j, Kafka, S3, Snowflake, BigQuery, Turbopuffer, FalkorDB, SurrealDB, Valkey, Iggy, Doris, zvec, sqlite, GDrive, Azure Blob, OCI |
| 5 | **Provable revocation with evidence** | `revocation.py`, `governance.py`, `provenance.py`, native effect layer in Rust | Genuinely differentiated. Nothing else open-source does verified-postcondition deletion with metadata-only audit records. |
| 6 | **Local-first, zero telemetry** | LMDB ledger, no control-plane dependency | Sells into defense, health, finance, EU — segments the cloud vendors structurally cannot serve. |
| 7 | **Plan/diff/explain UX** | `python/synor/cli.py` | Terraform's rhythm applied to data. Reviewable pipelines are a real enterprise ask. |

Plus two under-appreciated ones: **syntax-aware code chunking already exists**
(`rust/code_ast/`, `rust/code_match/`, and the shipped `ccc` CLI), and
**entity resolution ops** ship in the box (`python/synor/ops/entity_resolution/`).

### 0.2 What we do NOT have — say it out loud

- **No parsing/OCR/VLM of our own.** We orchestrate extraction; we don't do it.
- **No UI.** Everything is CLI + library. `dashboard.py` exists but is thin.
- **No hosted service, no auth, no multi-tenancy, no billing.** Zero SaaS scaffolding.
- **Alpha, 16 commits, no community.** `0.1.0a1`. The public API is *mid-rename*
  (`dev/rename_api.py` is sitting uncommitted, renaming `mount`→`spawn`,
  `memo`→`cache`). No stars, no users, no distribution.
- **No proprietary data, model, or benchmark.**

### 0.3 The positioning problem you must solve first

Synor is architecturally near-identical to [**CocoIndex**](https://cocoindex.io/)
— open-source, Rust core, Python interface, incremental processing, lineage,
connectors to vector stores. CocoIndex has the same pitch, is further along, and
peaked at ~160 GitHub stars/day. Pixeltable occupies adjacent ground with
[automatic incremental embedding indexes](https://www.pixeltable.com/blog/pixeltable-incremental-embedding-indexes).
LanceDB is pulling the same job into the storage layer via the
["multimodal lakehouse"](https://gradientflow.substack.com/p/the-rise-of-the-multimodal-lakehouse).

**Conclusion: "an incremental data framework for AI" is not an available
position.** It is taken, by a better-known project, on identical technical
claims. Every idea below is therefore constructed to use the engine as
*substrate* while positioning on a job that CocoIndex/Pixeltable/LanceDB are not
claiming — chiefly **correctness, deletion, and evidence**, which is where our
revocation/governance code is a genuine outlier.

### 0.4 The screen every idea below must pass

1. Does it need capability #1, #2, #3, or #5? (If not, our engine is irrelevant
   and we're competing with nothing.)
2. Is there a **budget line item** it attaches to today?
3. Can a 1–3 person team ship a credible v1 in 8 weeks?
4. Is there a wedge that does **not** require ripping out an existing pipeline?

---

# Part 1 — Ideas against the data-entropy thesis

> *"Enterprises need a continuous way to clean, structure, validate and govern
> their multimodal data so downstream AI workloads actually work."* — Jennifer Li

The key word in that quote is **continuous**. Everyone else in this market sells
a *transformation* (parse this PDF, extract these fields). Almost nobody sells
the *maintenance* of the result over time. That asymmetry is our opening.

The market is real: IDP is variously sized at
[$3.9B in 2026 growing 33.8% CAGR to $29.7B by 2033](https://www.grandviewresearch.com/industry-analysis/intelligent-document-processing-market-report)
(estimates range widely — [Precedence puts 2026 at $4.31B → $43.92B by 2034](https://www.precedenceresearch.com/intelligent-document-processing-market)).
Reducto alone has raised
[$108M across three rounds, $75M Series B led by a16z](https://reducto.ai/blog/reducto-series-b-funding).
That capital is chasing *extraction quality*. The lifecycle is unclaimed.

---

## Idea 1.1 — **Index Integrity**: a drift auditor & repair plane for RAG

**The strongest idea in this document.** Read this one carefully.

### The pain, in the industry's own words

Research is unusually unanimous here:

- *"Most teams discover their index is weeks or months stale only after a user
  complaint — and by then, users have already quietly stopped trusting it.
  Semantic similarity has no temporal dimension — stale embeddings score just as
  high as fresh ones."*
  ([TianPan, RAG freshness problem](https://tianpan.co/blog/2026-04-10-rag-freshness-problem-stale-embeddings-silent-failure))
- Oracle enumerates the exact failure taxonomy: *"a document is updated but the
  old chunks still rank, a source row is deleted but its embedding remains
  active, and re-ingestion jobs running twice create near-duplicates and
  conflicting evidence."*
  ([Oracle: How to Detect RAG Index Drift](https://blogs.oracle.com/developers/how-to-detect-rag-index-drift-deleted-docs-stale-chunks-and-duplicate-embeddings))
- *"The most underappreciated failure mode is the partial update: you start
  reindexing 10,000 documents, the pipeline crashes at 6,000, and now your index
  contains documents at different versions with the seam between them invisible
  to the retrieval layer."*
  ([aakashx](https://www.aakashx.com/blog/rag-in-production-enterprise-scale/))
- *"Silent drift between indexing and query pipelines where re-embedding
  strategy changed on the indexing side while the query-time embedder didn't get
  the memo, causing recall to quietly tank for two weeks before anyone noticed."*
- Academically live too: ["Ghost Vectors: Soft-Deleted Embeddings Remain
  Reconstructible in HNSW Vector Databases"](https://arxiv.org/pdf/2606.18497).

Map that list against Part 0: item-for-item, those are capabilities #1, #2, #3.
This is not a coincidence — Synor's design happens to be a direct answer to the
most-complained-about class of production RAG failure.

### The product

Two-stage, and the sequencing is the whole trick.

**Stage 1 — the Auditor (free / open-source, the wedge).**
A read-only tool. Point it at (a) your source of truth and (b) your vector
store. It reconciles the two and produces a **Drift Report**:

```
$ synor-audit --source s3://corp-docs --index qdrant://prod/kb

  DRIFT REPORT  ·  prod/kb  ·  1,284,301 vectors

  ✗  18,442 orphan vectors        source document deleted, embedding live
  ✗   3,109 stale vectors         source modified after embedding (avg 47d behind)
  ✗   1,876 duplicate chunks      double-ingestion, conflicting evidence
  ⚠  91,204 vectors               embedded with chunker v1.2 (current: v1.4)
  ⚠     412 documents             never indexed (ingestion silently failed)

  Estimated share of retrievals hitting a defective vector: 6.1%
```

That report is a **demo you can run in 20 minutes on a prospect's stack, and it
finds something every time.** It converts an abstract worry ("is our RAG
stale?") into a number with a dollar sign attached. It requires no migration and
no trust. It is the cheapest possible entry into an enterprise AI team.

**Stage 2 — the Repair Plane (paid).**
Once they've seen the number, they ask the obvious question: *fix it?* Now you
sell the actual product — Synor takes ownership of the sync. Because of
capability #2 it never orphans again; because of #3 a chunker change re-embeds
exactly the affected corpus and nothing else; because of the LMDB ledger a crash
at document 6,000 converges on the next run instead of leaving a seam.

**Pricing:** auditor free (open source, distribution engine). Repair plane:
usage-based on documents-under-management, ~$0.02–0.10/doc/month, floor $2k/mo.
Enterprise self-hosted tier at $60–150k/yr — which our local-first architecture
supports natively and most competitors cannot.

### Competitors and precisely where the gap is

| Player | What they do | The gap |
|---|---|---|
| [LangChain Indexing API](https://blog.langchain.com/syncing-data-sources-to-vector-stores/) | RecordManager dedupes and cleans up on re-ingest | Requires you to re-enumerate the full source every run; no code-change invalidation; no audit; no evidence; nobody trusts it for compliance |
| [Pixeltable](https://www.pixeltable.com/blog/pixeltable-incremental-embedding-indexes) | Incremental computed columns, real embedding indexes | You must adopt Pixeltable as your **storage layer**. Full-stack migration. We can sit beside an existing stack. |
| Unstructured.io Platform | Ingest + parse + push to vector DB | Extraction-centric; drift/deletion is not the product |
| Airbyte / Fivetran vector destinations | Move data to vector stores | Sync-forward only; no derived-artifact ownership; no chunk-level semantics |
| Vector DBs (Pinecone/Qdrant/Weaviate) | Store & search | Structurally cannot know your source truth. They will *never* build this. |
| Observability (Arize/Langfuse/Galileo) | Trace queries & responses | Watch the *query* side. Nobody watches the *index* side. |

**The gap is real and specific: there is no "Monte Carlo for the retrieval
index."** Data observability got a $2B+ category (Monte Carlo, Bigeye,
Soda, Anomalo) for exactly the analogous problem in the warehouse. The
equivalent for vector/derived state does not yet exist.

### Risks

- Vector DB vendors could ship "source sync" natively. Mitigation: be
  multi-store from day one; a customer with Qdrant *and* Postgres *and* Neo4j
  cannot be served by any single vendor.
- Auditing requires read access to production indexes — security friction.
  Mitigation: local-first execution is genuinely the answer here, and it's ours
  for free.
- Buyers may accept staleness. Mitigation: lead with the orphan-vector count,
  which reads as a *security/compliance* issue, not a quality one.

### First 8 weeks

Week 1–2: auditor CLI against Qdrant + Postgres/pgvector + S3/GDrive sources.
Week 3–4: the drift report as an HTML artifact worth screenshotting.
Week 5–6: publish. HN, r/LocalLLaMA, r/MachineLearning, LangChain Discord. The
title writes itself: *"We scanned 40 production RAG indexes. 6% of retrievals hit
a vector whose source no longer exists."* Get 20 real reports run.
Week 7–8: convert 3 of those into repair-plane pilots.

---

## Idea 1.2 — **Verified Erasure**: right-to-be-forgotten for derived AI state

The single most differentiated thing in this repo is
`python/synor/revocation.py` plus the native-effect verification layer. It is
almost inexplicably well-suited to a regulatory problem that becomes acutely
enforceable *right now*.

### Why now

- **EU AI Act high-risk obligations became enforceable 2 August 2026**
  ([Salt Security](https://salt.security/eu-ai-act-compliance),
  [Raconteur technical audit guide](https://www.raconteur.net/global-business/eu-ai-act-compliance-a-technical-audit-guide-for-the-2026-deadline)).
  Article 12 mandates automatic event logging retained ≥6 months.
- The critical framing from [Raconteur]: *"regulators don't ask for
  documentation — they ask for **evidence**: proof that the documentation
  reflects what is actually running in production at the moment of the audit."*
  Synor's metadata-only manifests under `.synor/runs/` are literally that.
- The technical gap is well documented: *"Right to erasure requires deletion to
  reach agent memory, vector stores, and logs, not just the source database row.
  Deleting the source row does nothing for a copy already embedded in a vector
  store weeks earlier. Teams must maintain a data provenance map that links
  personal data in source systems to all downstream agent memory stores."*
  ([Atlan](https://atlan.com/know/ai-agent/gdpr-compliance-for-ai-agents/))

**"Maintain a data provenance map linking source records to every downstream
derived store" is a one-sentence description of Synor's component-path model.**
We have already built the thing the compliance literature says teams need.

### The product

A **DSAR/erasure execution engine for derived AI state.**

When a subject-erasure request lands, the system:
1. Resolves the subject to source records across systems.
2. Walks the ownership graph to every derived artifact — chunks, embeddings,
   graph nodes/edges, extracted rows, caches, agent memories.
3. **Suppresses first** (source generation suppressed before destructive target
   work — this ordering is already implemented), so retrieval stops serving the
   data immediately, before deletes have finished propagating.
4. Executes deletes with **verified postconditions** — the sink returns only
   after the deletion is confirmed, not merely acknowledged.
5. Emits a signed, metadata-only certificate: what was deleted, from where,
   when, verified how. No personal data in the evidence.

### Buyer and budget

Privacy engineering / DPO / AI governance. This is *not* a data-team sale — it's
a legal-risk sale, which means it is (a) budgeted, (b) urgent, (c) far less
price-sensitive. GDPR erasure is a 30-day statutory clock; today most companies
answer it for vector stores with a shrug.

**Pricing:** $75–250k/yr enterprise. Regulated industries and any EU-exposed
company deploying high-risk AI.

### Competitors

| Player | Reach | Gap |
|---|---|---|
| OneTrust, Transcend, Ketch, DataGrail | Privacy ops, DSAR workflow orchestration | They *orchestrate the request* and stop at systems-of-record. They have no model of derived AI artifacts. They send a ticket; a human deletes something; nobody verifies. |
| Relyance, Securiti | Data mapping / lineage | Discovery-oriented; no execution, no verified deletion |
| Vector DBs | `delete(id)` | Requires you to already know every id. That's the entire hard part. Plus [ghost-vector reconstructability in HNSW](https://arxiv.org/pdf/2606.18497). |
| Machine-unlearning research startups | Model weights | Different (harder, less mature) problem. Ours is the retrieval/derived layer, which is where the practical exposure is. |

**The gap: privacy tooling is workflow software that assumes a human executes
deletion in each system. Nobody sells verified technical enforcement into the
derived AI layer.**

### Risks

- Long enterprise sales cycles; a 2-person team will feel this.
- Requires legal credibility. Mitigation: co-author the methodology with a
  privacy-law firm; publish it. The whitepaper *is* the marketing.
- "Certificate" claims must be scrupulously bounded — [reading.md §8](reading.md)
  already models the right honesty (we don't prove media erasure or reach
  unmanaged copies). **Keep that discipline; it is a selling point, not a
  weakness.** Overclaiming here is existential.

### Why this is arguably the best risk-adjusted idea

It is the only one where the hardest engineering is *already done* and where a
regulatory deadline creates urgency we don't have to manufacture. Its weakness
is GTM, not product.

---

## Idea 1.3 — **The extraction cost & lifecycle layer** (bring your own parser)

### Insight

Do **not** compete with Reducto ($108M raised), Extend, Unstructured, or Docling
on parse quality. You will lose. Instead, note what none of them sell: the
economics and lifecycle of *repeated* extraction.

A full multimodal RAG system reportedly runs
[$200k–500k, and enterprise multimodal platforms exceed $750k](https://www.runpod.io/articles/guides/multimodal-ai-development-building-systems-that-process-text-images-audio-and-video).
Much of that is recomputation: someone tweaks a prompt, a schema, or a model, and
1M pages get re-run through a VLM at full price.

### The product

**A memoizing, reconciling wrapper around any extractor.** Register Reducto,
Docling, Gemini, GPT-5, Whisper, or your own function as an op. Synor
fingerprints (document bytes × extractor version × prompt × schema) and re-runs
only true cache misses.

The demo that sells it: *change one field in your extraction schema and watch
the bill be 1.5% of the naive re-run.* Chunk-level content-addressable hashing
already yields [10–15% re-embedding instead of 100%](https://medium.com/@vasanthancomrads/incremental-indexing-strategies-for-large-rag-systems-e3e5a9e2ced7)
in the systems that do it; at schema granularity the saving is larger.

Second act — **extraction QA over time**: because we retain per-document
lineage, we can diff extraction results across model/prompt versions and surface
*regressions* ("847 documents changed their `total_amount` when you moved to
model v3; here are 20 to review"). That is the human-QA bottleneck Jennifer Li
names, addressed with data we get for free.

### Why the wedge works

It is **non-competitive with the well-funded players — it makes them
cheaper to use.** That makes partnership, not displacement, the path in.
Reducto's customers are exactly our customers.

### Risks

- Perceived as a caching library, not a company. Mitigation: lead with the
  regression-diffing (QA) story; caching is the hook, QA is the product.
- Parser vendors may add caching. They won't add cross-vendor caching, and
  enterprises use several.

---

## Idea 1.4 — Permission-aware index sync (ACL propagation)

### Pain

Vector stores don't model SharePoint/Drive ACLs, so *"the LLM can bypass
folder-level permissions"*
([Onyx buyer's guide](https://onyx.app/insights/enterprise-rag-platforms-2026)).
Glean's actual moat is its Enterprise Graph mirroring permission models with
real-time delta ACL sync. The academic framing exists too
([ConfusedPilot: Confused Deputy Risks in RAG-based LLMs](https://arxiv.org/pdf/2408.04870)).

### Fit

Permissions are derived state owned by a source object — exactly our
reconciliation model. Revoke a user's access to a folder → the components owning
those chunks re-derive → visibility rows reconcile → retrieval changes within one
run, with an audit record.

### Verdict: **strong technically, weak strategically for a small team.**

It requires deep, permanently-maintained connectors to SharePoint, Drive,
Confluence, Slack, Jira, Salesforce — that is a 10-engineer treadmill, and Glean
and Onyx are already on it. Include it as a *feature* of Idea 1.1 (permission
drift is a drift class) rather than a company.

---

## Idea 1.5 — Vertical: continuously reconciled document truth

Pick **one** document domain where documents *amend and contradict each other*
over time and the current answer requires reconciliation rather than retrieval:

- **Insurance claims** — FNOL, adjuster notes, medical records, estimates,
  supplements. The "current" claim value changes weekly.
- **Construction submittals/RFIs** — drawings revise; RFI answers supersede spec.
- **Clinical trial site documents** — protocol amendments cascade.
- **Contract families** — MSA + 6 amendments + 3 SOWs; which termination clause
  is live *today*?

Generic RAG fails badly here because it retrieves the original and the amendment
with equal confidence. Synor's model expresses it naturally: the current answer
is a **target state owned by the whole document family**, recomputed when any
member changes, with old conclusions actively removed.

**Verdict: highest ceiling, highest risk.** Vertical AI commands the best
margins and stickiest retention, but demands domain expertise the codebase
doesn't supply. Only do this if a co-founder brings the domain.

---

# Part 2 — Ideas against the AI-native data stack thesis

> *"How AI agents navigate 'the context problem': continuously accessing the
> right data context and semantic layers... that always have the correct
> business definitions across multiple systems of record."* — Jason Cui

Note the structural opening: Fivetran and dbt
[completed their merger 1 June 2026](https://www.fivetran.com/press/fivetran-dbt-labs-complete-merger-to-create-the-data-infrastructure-for-trusted-ai-agents),
explicitly to become "the data infrastructure for trusted AI agents." Their
stated remaining gaps are **orchestration beyond the SQL DAG** and non-SQL work
— *"dbt handles SQL orchestration, but knows nothing about what happens outside
its DAG"* ([Kestra](https://kestra.io/resources/data/fivetran-dbt-merger-fusion-engine)).
Unstructured and multimodal transformation is outside that DAG by definition.

---

## Idea 2.1 — **The context layer for unstructured enterprise knowledge**

### The framing

The semantic-layer market has crisply defined itself for structured data:
*"Text-to-SQL gives an agent access to your data; a semantic layer gives it
understanding... Pointed at raw tables, an LLM re-derives joins, grain, and
metric logic on every prompt — so the same question returns different answers"*
([Cube](https://cube.dev/articles/semantic-layer-for-ai-agents-2026)). The
measured payoff is large: grounding in a semantic layer moved dbt's 2026
benchmark from 90.0% → 98.2% for Claude Sonnet 4.6 and 84.1% → 100% for
GPT-5.3-Codex.

**There is no equivalent for unstructured context.** Cube, dbt SL, AtScale,
Snowflake Semantic Views all govern *metrics over tables*. The policy PDF, the
architecture decision record, the support macro, the contract — the material
agents most need — has no governed, versioned, freshness-tracked layer.

Meanwhile the MCP layer is a mess: *"Simply connecting an AI agent to data
sources does not ensure it uses approved business definitions, trusted datasets,
or governance policies"*
([OvalEdge](https://www.ovaledge.com/blog/mcp-server-for-enterprise-data)), and
*"MCP adoption has outpaced MCP governance"*
([GitGuardian](https://blog.gitguardian.com/mcp-governance-framework/)) — with
only ~24.4% of organizations having full visibility into agent-to-MCP
connections.

### The product

A **governed context store exposed as an MCP server**, continuously reconciled
by Synor from source systems (Confluence, Drive, Notion, Git, Slack, ticketing,
CRM). Every returned context chunk carries:

- **provenance** — the exact source document and version
- **freshness** — "derived from a doc last modified 3 days ago"
- **authority** — is this the canonical policy or a Slack opinion?
- **supersession** — this doc is superseded by that one
- **permissions** — the caller's identity is enforced at retrieval

The differentiating claim: **an agent should never be able to receive context
without knowing how stale it is and where it came from.** Today they routinely
do, and *65% of enterprise agent failures are attributed to context drift rather
than raw model capability* ([Value Add VC](https://valueaddvc.com/blog/the-ai-memory-problem-how-startups-are-solving-for-persistent-context)).

### Positioning

"**Cube for unstructured context.**" Explicitly complementary — integrate with
Cube/dbt SL for metrics, own everything they don't. Sell to platform teams who
have already bought a semantic layer and discovered it only covers 20% of what
their agents need.

### Risks

Category is forming fast and Atlan/Cube/Snowflake will extend downward. Speed
and open-source distribution matter more than depth in year one.

---

## Idea 2.2 — **Reconciled agent memory** (memory with a source of truth)

### Pain

Agent memory is a funded category — [Mem0 raised $24M and is AWS Agent SDK's
exclusive memory provider](https://valueaddvc.com/blog/the-ai-memory-problem-how-startups-are-solving-for-persistent-context);
Letta and Zep are well established. But look at the architecture:
*"Mem0 is a vector-first memory layer you bolt onto any agent stack, Zep (via
Graphiti) is a temporal knowledge graph, Letta is a full agent runtime"*
([AgenticWire](https://www.agenticwire.news/article/mem0-zep-letta-agent-memory)).

All three treat memory as **primary state** — facts the agent wrote down. None
treats memory as **derived state owned by a source of truth**. So when the
underlying fact changes, agent memory silently contradicts reality; when a
customer is deleted, their memories linger; when your extraction prompt changes,
old memories were derived by the old prompt and nobody knows which.

### The product

**Memory as a reconciled projection.** Memories are declared target states owned
by source records. Source changes → memory updates. Source deleted → memory
deleted (and this is the *only* clean answer to GDPR for agent memory, per
[Atlan](https://atlan.com/know/ai-agent/gdpr-compliance-for-ai-agents/)).
Extraction logic changes → affected memories re-derive. Contradictions between
memory and source are detected rather than served.

### Assessment

Technically the cleanest differentiation in this document — it's a genuinely
better architecture, not a feature gap. Commercially harder: the incumbents have
distribution and the buyer is a developer choosing an SDK, which is a
brand/ecosystem race. **Best executed as a component of Idea 2.1** (memory is
one kind of governed context) rather than as a standalone company.

---

## Idea 2.3 — Activation of AI-derived data (reverse-ETL with provenance)

LLM-derived fields — risk scores, extracted entities, summaries, classifications
— increasingly need to land back in Salesforce, HubSpot, Zendesk, Netsuite.
Census and Hightouch own reverse-ETL for *structured warehouse* data. Neither
handles:

- **Re-derivation** when the model or prompt changes (capability #3)
- **Retraction** when the source is deleted or the inference is invalidated (#2)
- **Provenance** on a written field: "this risk score came from doc X, model v4,
  2026-07-12" — required for any regulated decision
- **Confidence gating and human-review routing**

**Assessment: real gap, modest ceiling.** More likely a strong *feature* of 1.1
or 2.1 than a company. Worth building because it makes the platform sticky —
once we write into their CRM, we're load-bearing.

---

## Idea 2.4 — "dbt for unstructured data" (the platform play)

The maximal framing: Python-native declarative transformations over documents,
images, audio, and video, with lineage, tests, CI preview, and a package
ecosystem. `synor plan` / `synor diff` already gives the Terraform/dbt rhythm.
The Fivetran+dbt entity has explicitly conceded the non-SQL DAG.

**But:** this is precisely CocoIndex's stated position, and Pixeltable's, and
increasingly LanceDB's. Winning it requires a category-defining amount of
capital, community, and time.

**Verdict: this is the *destination*, not the *wedge*.** Ideas 1.1 and 1.2 are
credible paths to arriving here with revenue and users. Leading with it means
competing on GitHub stars against projects with a 12-month head start.

---

## Idea 2.5 — Data observability for the AI layer

Monte Carlo, Bigeye, Anomalo, and Soda built a large category on "is my
warehouse table healthy?" Nobody owns "is my *derived AI state* healthy?" —
freshness SLAs on embeddings, schema drift in extracted fields, distribution
shift in classifications, extraction-regression alerts on model upgrades,
orphan/duplicate detection.

Because Synor holds lineage and fingerprints natively, most of the signals come
free. **This is genuinely the same product as Idea 1.1 viewed from the
monitoring rather than the repair angle — and "observability" is the category
name buyers already have budget for.** Consider it a positioning option for 1.1
rather than a separate idea: *lead with observability language, sell repair.*

---

# Part 3 — Wildcards

## Idea 3.1 — Self-hosted code context engine for AI coding agents

We already have `rust/code_ast/`, `rust/code_match/`, and a shipped `ccc` CLI
doing local semantic code search (see [AGENTS.md](AGENTS.md) — it's used by this
repo's own agents).

The market signal is loud: when Augment exposed its Context Engine via MCP,
[Claude Code with Opus 4.5 saw an 80% quality improvement and Cursor with Opus
4.5 saw 71%](https://anthonywest.co.uk/research/code-intelligence-indexing-2026-openai).
Community appetite is extreme — CodeGraph hit 47.4k stars in five months;
GitNexus went ~1.2k → 42k between April and June 2026.

**Wedge:** the segment Sourcegraph/Augment/Cursor serve badly — organizations
that **cannot send source code to a vendor**. Defense, finance, health, EU. A
fully local, incrementally maintained, permission-aware code+docs+tickets index
exposed over MCP to whatever agent they run.

**Pros:** highest developer-love potential; a natural open-source flywheel; we
have a running head start.
**Cons:** brutally crowded, well-funded, and buyers expect it free. Monetization
is enterprise-only.

## Idea 3.2 — Local-first personal knowledge runtime

Notes, email, files, browser history, screenshots → a continuously reconciled
local index, private by construction, exposed to any local or hosted model.
Local-first with zero telemetry is a real, defensible differentiator here.

**Assessment:** small TAM, weak willingness to pay, but *excellent distribution
value* — this is the kind of thing that gets 10k GitHub stars and puts the engine
in front of the developers who make enterprise purchase decisions. Consider it
**marketing infrastructure**, not a business.

## Idea 3.3 — The regulated on-prem AI data plane

Bundle 1.1 + 1.2 + 2.1 as a self-contained appliance for air-gapped or
sovereignty-constrained environments. No telemetry, no cloud control plane, LMDB
ledger on their disk, complete audit evidence. Vendors are reportedly
[charging 20–30% more to reflect EU AI Act certification costs](https://www.raconteur.net/global-business/eu-ai-act-compliance-a-technical-audit-guide-for-the-2026-deadline)
— that premium is available to whoever can actually deliver.

**Assessment:** $250k–1M ACV, ~9–15 month sales cycles. Not a first move for a
small team; a very good *second* one, and it's where 1.1+1.2 naturally lead.

## Idea 3.4 — `synor plan` as a CI GitHub App

Small, cheap, viral-adjacent. Open a PR that changes a pipeline; the bot comments
with the blast radius: *"this chunker change invalidates 412k embeddings
(~$1,840 re-embed) and deletes 1,203 rows in `prod.entities`."* Nobody offers
this today, and it converts an invisible risk into a reviewable artifact.
**Not a company; an outstanding top-of-funnel asset for 1.1.**

## Idea 3.5 — License the engine (OEM)

Sell the reconciliation engine to companies building AI data products who don't
want to write incremental state management. Low-glamour, but a real path: it
turns the strongest asset (the engine) into revenue without solving GTM. Best as
opportunistic revenue, not a plan.

---

# Part 4 — Scoring and recommendation

## 4.1 The table

Scores 1–5. **Fit** = does it need our unique capabilities. **Wedge** = can we
get in without a migration. **Moat** = can we hold it in 24 months.

| Idea | Fit | Pain | Wedge | TAM | Moat | Speed | **Total** |
|---|---|---|---|---|---|---|---|
| **1.1 Index Integrity** | 5 | 5 | **5** | 4 | 3 | 5 | **27** |
| **1.2 Verified Erasure** | **5** | 4 | 4 | 3 | **5** | 4 | **25** |
| 2.1 Unstructured context layer | 4 | 5 | 3 | 5 | 3 | 3 | 23 |
| 1.3 Extraction cost/QA layer | 4 | 4 | 5 | 3 | 2 | 5 | 23 |
| 3.1 Code context engine | 3 | 4 | 4 | 4 | 2 | 4 | 21 |
| 2.2 Reconciled agent memory | 5 | 3 | 3 | 4 | 3 | 3 | 21 |
| 1.5 Vertical document truth | 4 | 5 | 2 | 3 | 5 | 1 | 20 |
| 2.3 AI-derived activation | 3 | 3 | 4 | 3 | 2 | 4 | 19 |
| 1.4 Permission-aware sync | 4 | 5 | 2 | 4 | 2 | 1 | 18 |
| 2.4 "dbt for unstructured" | 4 | 3 | 1 | 5 | 2 | 1 | 16 |
| 3.2 Personal knowledge OS | 2 | 2 | 5 | 1 | 1 | 5 | 16 |

## 4.2 Recommendation

**Build 1.1 (Index Integrity) as the company. Attach 1.2 (Verified Erasure) as
the enterprise tier. Use 3.4 and 3.2 as distribution.**

The reasoning:

1. **1.1 has the only genuinely frictionless wedge in the list.** A read-only
   auditor that finds a real defect in 20 minutes, on their existing stack, with
   no migration. Everything else requires someone to change how they build.
2. **The two ideas share one engine and one narrative.** 1.1 says *your index
   doesn't match your source*. 1.2 says *and when legal asks you to delete
   something, you can prove it happened*. Same lineage graph, same reconciler,
   same evidence layer — but they sell to two different budgets (AI platform and
   privacy/compliance), which doubles the ways into an account.
3. **It sidesteps the CocoIndex problem entirely.** We stop selling "an
   incremental framework" — an occupied position — and sell "correctness and
   evidence for derived AI state," which nobody is selling. The framework
   becomes an implementation detail, which is exactly what it should be.
4. **It monetizes the part of the codebase nobody else has.** Deletion
   propagation and provable revocation are the two capabilities where we're an
   outlier. Every other idea leans on capabilities that Pixeltable, LanceDB, or
   CocoIndex either have or can add in a quarter.

## 4.3 The 90-day plan

**Days 1–14 — Prove the wedge before building the product.**
Do not write product code yet. Get read access to 5 real production RAG indexes
(your network, AI eng Discords, "free RAG health check" posts). Write the drift
detection by hand if necessary. **The go/no-go question: does the audit find a
defect in ≥4 of 5?** If it doesn't, the thesis is wrong and you've spent two
weeks, not two quarters.

**Days 15–45 — Ship the auditor.**
Open-source, Apache-2.0, one-command install. Sources: S3, Google Drive,
Postgres, local. Indexes: Qdrant, pgvector, LanceDB, Pinecone. Output: a
shareable HTML drift report. Ruthlessly single-purpose — the temptation to
expose the whole framework will be strong; resist it. Also finish
`dev/rename_api.py` and freeze the public API before anyone depends on it.

**Days 46–60 — Distribution.**
Publish the aggregate findings across every index you've scanned. Data-backed
posts about a problem everyone privately has is the highest-yield content in
infra. Ship 3.4 (the CI bot) as the follow-up. Target: 1,000 GitHub stars, 50
audits run by strangers.

**Days 61–90 — Convert.**
Three design partners on the repair plane. Charge from day one, even $500/mo —
free pilots teach you nothing about willingness to pay. In parallel, take the
erasure story (1.2) to two privacy leaders and validate the compliance framing
before building further; the EU AI Act deadline has already passed, so the
urgency is live.

## 4.4 What would change this recommendation

- A co-founder with deep insurance/legal/clinical domain expertise → **1.5**
  instead, immediately. Vertical beats horizontal when you have unfair domain
  access.
- Evidence that teams knowingly tolerate index drift → the whole of Part 1
  weakens; pivot to **2.1**, where the pain (agent context failure) is currently
  louder and better funded.
- An enterprise design partner appearing with a real erasure mandate → invert
  to **1.2 first**; a paying, urgent compliance customer beats a better wedge.

## 4.5 Honest risks to the whole plan

- **We are 16 commits into an alpha with a half-finished API rename.** All of
  this assumes hardening effort that isn't in the 90-day plan above. Budget for
  it.
- **A vector DB vendor shipping "source sync" would hurt 1.1 badly.** The
  multi-store, source-agnostic position is the hedge — build it in from day one,
  not later.
- **Nothing here has a durable technical moat on a 5-year view.** The moat is
  being trusted on *correctness* — which comes from the evidence layer, the
  benchmark data you accumulate from audits, and the discipline of not
  overclaiming (see [reading.md §8](reading.md)). That discipline is a real
  asset. Protect it.
