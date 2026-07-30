<!-- Preserved pre-Codex/Synor import. -->
---
name: bug-report
description: >-
  File a high-quality Linear bug ticket: parse what the user gave (URL, screenshot,
  expected-vs-observed, time observed), interrogate only the gaps, optionally do
  a code-level root-cause pass with `file:line` refs, search Linear for dupes
  and regressions, then file via `linear-server` MCP with the right team, project,
  labels, and priority. Use when the user says "file a bug", "bug report",
  "this is a bug", "create a bug ticket", "report this", "log a bug",
  or pastes a screenshot of broken behaviour and asks to ticket it.
  Flags: `--quick` skips root-cause, `--no-search` skips dupe/regression search.
---

# bug-report — File a high-signal Linear bug ticket

A bug ticket is only as useful as the next person's ability to act on it without re-asking what you already had in front of you. This skill enforces the canonical shape, fills it from what's already in the message, asks only for the missing required fields, optionally root-causes it inline, and files it via the `linear-server` MCP.

The reference output shape is **[T-2532](https://linear.app/trifetch/issue/T-2532)** — every section in this skill maps to a section in that ticket.

## When to use this skill (and when not to)

| Use this skill | Use a different approach |
| -- | -- |
| "this looks wrong, file a bug" | "investigate auth flow" — full feature investigation |
| One observed misbehaviour | Whole-feature audit |
| Output: one Linear ticket | Output: analysis doc + spec + tests |
| Minutes | Hours, multi-phase |

If during the root-cause pass the surface is clearly bigger than one observed bug (cross-cutting, multi-feature), **stop and recommend a deeper investigation** — file the ticket as a stub describing the symptom and the suspected scope, and let the assignee scope a proper investigation rather than half-rooting it inline.

## Phase 0 — Parse what the user already gave

Before asking anything, extract these from the user's message and any attachments:

| Field | Where to look |
| -- | -- |
| **URL** | Any `https://...` in the prose, especially `*.trifetch.com` paths |
| **Screenshot path** | Image attachment in the message; `.png`/`.jpg` paths in temp dirs |
| **Time observed** | Phrases like "just now", "5 min ago", "this morning"; default to `now()` if absent |
| **Expected vs observed** | "expected X but got Y", "should be X, is Y", "instead of", side-by-side framings |
| **Environment / stack** | URL subdomain (`tanzil.dev`, `stg`, `prod`), env toggle in screenshot, "on staging" |
| **Workflow ID** | UUIDs in URL path: `/workflows/<uuid>`, `/dashboard/.../<uuid>` |
| **Org** | `?orgId=...`, screenshots showing the org switcher |
| **Repro steps** | Numbered/bulleted steps already in the message |
| **Severity cues** | "customer-facing", "blocks deploys", "I noticed", "low priority", "since X shipped" |

Read every image attachment with the Read tool — do not ask the user to describe a screenshot they already gave you.

If the URL looks like a Trifetch URL (`*.trifetch.com`, `*.app.trifetch.com`), treat the repo signals as present and plan to do a code-level root-cause pass (Phase 3).

## Phase 1 — Interrogate only the gaps

After parsing, list what's still missing from this required set:

- URL where observed
- Time observed (default = now)
- Expected behaviour
- Observed behaviour
- Environment / stack
- Screenshot (only required if the bug is visual)
- Workflow ID (only required if the URL implicates one and it's not in the URL already)

Ask **one batched message**, one question per missing field. Phrase each question so a one-line answer is enough. If the user says "skip" or "file with what we have", proceed with placeholders (`<unknown>`) and note the gaps in an "Unknowns" callout in the ticket.

Never ask twice. Never ask for fields you already have.

## Phase 2 — Search for dupes and regressions

Unless `--no-search` was passed, run `mcp__linear-server__list_issues`:

1. Pull 3–6 keywords from the draft title (drop stopwords, keep nouns and the broken behaviour).
2. Query Linear with `query: "<keywords>"`, sorted by `updatedAt`.
3. Inspect the top ~10 hits. Flag:
   - **Open issues** with overlapping description → likely dupe. Stop and confirm with the user before filing — offer to comment on the existing issue instead.
   - **Done issues** with overlapping description → **likely regression**. Capture the resolved issue ID; you'll link it via `relatedTo` and call it out in the ticket title and body ("regression of T-XXXX, marked Done on YYYY-MM-DD").

The T-2532 example is a regression of T-2204 — the title literally says so, and the body links it. That pattern matters: it tells the next engineer which fix broke and where to look first.

## Phase 3 — Optional root-cause pass

Skip if `--quick` was passed, or if there are no repo signals (e.g. user is reporting a third-party SaaS bug).

When repo signals are present, trace the bug end-to-end **before** filing. The depth target is the T-2532 shape:

1. **Frontend entry point.** Resolve the URL → React route → component file. Read it. Identify the query/mutation/handler that produced the observed output.
2. **Network hop.** Find the tRPC route or REST endpoint the component calls. Note input shape and any client-side input assembly (this is often where drift bugs live — see T-2532 where `FilterBar` builds a different input than `LogsListV2`).
3. **Backend service.** Read the handler. Find the actual data source call (ClickHouse query, Postgres select, S3 fetch, external API).
4. **Datastore.** If the bug is a data mismatch, write the verification query — the SQL/HogQL/etc. that proves the bug is in the query, not in the rendering.

Capture as you go:

- `file:line` refs for every claim ("the input only reads workflow + status filters" → `apps/frontend/src/components/observability/filters/FilterBar.tsx:59-77`)
- A **mismatch table** when two paths disagree by design (rows: each path; columns: each filter/input; cells: ✅ / ❌)
- The **actual failing clause** (the SQL `WHERE`, the missing `if` branch, the wrong default)
- A **suggested fix** with options and a recommendation. If this is the Nth time the same drift class has shipped, say so and recommend the structural fix (lift to shared hook, share builder, etc.) — single-bug fixes that don't kill the drift class are how regressions ship.

If the trace gets > ~30 minutes deep or branches into multiple unrelated areas, stop and recommend a deeper investigation — file the ticket with what you have, mark the root-cause section "Partial — recommend full investigation" and note the suspected scope so the assignee can scope it properly.

Use subagents for the trace when the surface spans 3+ unrelated files — `Agent(subagent_type=Explore)` for the frontend-route-to-handler walk, then synthesize. Don't double-search.

## Phase 4 — Assemble the ticket

Use this template. Omit any section that doesn't apply (e.g. no "Backend" subsection for a pure-frontend bug; no "Verification SQL" if there's no datastore).

````markdown
## Problem

<2–4 sentences: what the user did, what they expected, what they got.>

**Repro:**

* Stack: `<env>` (`<env-toggle>` in toggle)
* URL: `<full URL>`
* <Other concrete identifiers: workflow name/ID, org, account, etc.>
* <The wrong observation, quoted exactly>  ← wrong
* <The right observation if known>          ← correct

<Embed the screenshot here.>

## Root cause

<1–2 sentences naming the actual mismatch.>

**Frontend:** `<path>:<line>-<line>`

```ts
<minimal code excerpt — only the lines that show the bug>
```

<1–3 sentences explaining what this code is missing or doing wrong.>

**Backend:** `<path>:<line>-<line>`

<Code excerpt + explanation, same shape.>

## Why <observation A> looks right and <observation B> looks wrong

| Query / Path | Source file | Filter A | Filter B |
| -- | -- | -- | -- |
| <name> | `<path>:<line>` | ✅ / ❌ | ✅ / ❌ |
| <name> | `<path>:<line>` | ✅ / ❌ | ✅ / ❌ |

## Suggested fix

<Two options if there's a structural choice; one if there isn't. Recommend one.>

<If this is a regression class (Nth time this shape of bug has shipped), link the prior tickets and recommend the structural fix.>

## Verification

<Concrete check the next engineer can run to confirm the diagnosis — SQL, curl,
shell command, repro path. For DB queries, route through `scripts/secure-run.sh
--stack <stack>` per repo rules.>

## Unknowns

<Only include if the user skipped fields. List them so the next person knows
what's missing.>
````

For pure-frontend bugs, drop the "Backend" + "Verification SQL" sections. For non-Trifetch bugs, drop the env toggle row and skip the secure-run note.

## Phase 5 — File via linear-server MCP

Use `mcp__linear-server__save_issue`. Field inference rules:

### Team

- Default: **Engineering** (Trifetch has one engineering team).
- If the user explicitly names a team ("design bug", "ops bug"), use that.
- If `list_teams` returns multiple and the cue is ambiguous, ask once.

### Project

Match the affected area to a project. Cues → project mapping (extend as projects evolve — verify with `list_projects` if unsure):

| Cue in the bug | Likely project |
| -- | -- |
| logs / observability / ClickHouse / log count / log filter | "Clickhouse customer workflow observability" |
| sandbox / E2B / opencode / agent terminal | (sandbox project — query `list_projects` for current name) |
| workflow runtime / cron / webhook / handler crash | (workflow runtime project) |
| login / auth / org switching | (auth project) |
| billing / plan / usage | (billing project) |

If no project clearly matches, leave it unset rather than guessing — Linear will route via team default.

### Labels

- Always: `Bug`.
- Add `Regression` if Phase 2 found a Done issue with overlapping description.
- Add `customer-facing` if the user said so or the URL is a customer-facing page.
- Verify available labels with `list_issue_labels` before adding novel ones.

### Priority

| Cue | Priority value | Linear name |
| -- | -- | -- |
| "production down", "blocks deploys", "data loss", "all customers" | 1 | Urgent |
| "customer-facing", "<named customer> can't X", "blocks onboarding" | 2 | High |
| "I noticed", "looks wrong", default | 3 | Normal / Medium |
| "minor", "nit", "cosmetic" | 4 | Low |

Default to 3 (Medium) when in doubt.

### Title

Format: `<area>: <broken behaviour> (<extra qualifier if regression or scope>)`

Examples:
- `Filter chip log count badge ignores workflow + environment scope (regression of T-2204)`
- `Sandbox terminal stays "connecting" after pod scales to zero`
- `Org switcher resets viewport scroll on workflows page`

Lowercase first word, no trailing period, ≤ 100 chars when possible.

### Description

The Phase 4 markdown.

### Attachments

If the user provided a screenshot, upload it via `mcp__linear-server__create_attachment` after the issue is created (it requires the issue ID). Read the file, base64-encode, and pass with the right `contentType` (`image/png`, `image/jpeg`).

### Links

Always add a link to the source URL where the bug was observed (`{ url, title: "Observed at" }`). Add the screenshot URL too if it lives on a stable host.

### Relations

- For each regression candidate from Phase 2, pass `relatedTo: [<id>]`.
- For likely dupes, **don't auto-link** — confirm with the user first; if they say "yes file anyway as related", use `relatedTo`, not `duplicateOf` (let the assignee make that call).

## Phase 6 — Report back

After filing, return to the user with:

- Linear URL.
- One-sentence summary of what was filed.
- The dupe/regression candidates surfaced (if any), with one-line context each.
- Any "Unknowns" still in the ticket, so they know what's missing.
- If `--quick` was used, a one-line offer to come back and root-cause it.

Keep the report short — the ticket is the artifact; the chat reply is just a receipt.

## Flags

- `--quick` — skip Phase 3 (root-cause pass). Useful when the user just wants the ticket on the board and will triage later.
- `--no-search` — skip Phase 2 (Linear dupe/regression search). Useful when the user has already searched, or for novel bugs where dupe risk is zero.

Both flags can be combined for the fastest possible path: parse → ask gaps → file.

## Hard rules

- Never ask for a field you already have. Read attachments before asking.
- Never invent file paths or line numbers. Every `file:line` ref must come from a Read tool call you actually made.
- Never auto-mark a ticket as `duplicateOf` — leave that judgement to the assignee.
- For DB verification queries, always route through `scripts/secure-run.sh --stack <stack>` in the suggested verification section. Do not run them yourself unless the user asked.
- If the bug is a security issue (auth bypass, data leak, secret exposure), set priority to Urgent and **stop before filing** to ask the user whether the ticket should be filed publicly or escalated through a private channel.
- Do not include AI attribution in the ticket body or title.
- One ticket per invocation. If the user describes two unrelated bugs in one message, file two tickets — confirm first.

## Example invocations

```
file a bug: timeframe chip says 1.3k logs but list shows 2
[screenshot.png attached]
URL: https://trifetch.com/dashboard/workflows/a746b402-.../?view=Logs
```
→ Phase 0 extracts URL, screenshot, expected/observed, env (`tanzil.dev`), workflow ID. Phase 1 has no gaps. Phase 2 finds T-2204 (Done, same shape) → flag as regression. Phase 3 traces FilterBar → logs.getLogCount → ClickHouse, builds mismatch table. Phase 5 files as `Bug` + `Regression`, project "Clickhouse customer workflow observability", priority High (filter is customer-facing in product), `relatedTo: [T-2204]`. → produces T-2532-shaped ticket.

```
this is a bug --quick
the org switcher loses my scroll position
```
→ Phase 0 extracts behaviour. Phase 1 asks for URL + env. Phase 2 searches "org switcher scroll". Phase 3 skipped. Phase 5 files Bug, priority Medium, no project (UI nit).

```
report this --no-search
[screenshot of 500 page]
clicked "Run" on cron, got 500
```
→ Phase 0 extracts screenshot, action, error. Phase 1 asks for URL + workflow ID + env + time. Phase 2 skipped. Phase 3 traces handler. Phase 5 files.
