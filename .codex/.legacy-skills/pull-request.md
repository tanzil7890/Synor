<!-- Preserved pre-Codex/Synor import. -->
---
name: pull-request
description: |
  Create Pull Request & optionally run review. Creates a PR with a concise description,
  then asks the user if they want to run a review (and at what depth) based on PR size/complexity.
  K8s sandboxes are dead; PR descriptions must treat sandbox runtime as E2B-only unless
  they are explicitly describing legacy cleanup/removal.
  The repo's default toolchain is pnpm + Node 24. Use pnpm-based validation commands by
  default; only keep Bun in the loop for intentionally Bun-targeted packages such as
  bun-runner-service or ingress-service.
  Use when user asks to "create PR", "open PR", "make PR", "/pull-request", or when ready
  to submit changes for review.
author: Claude Code
version: 3.2.0
allowed-tools:
  - Read
  - Write
  - Edit
  - Grep
  - Glob
  - Bash
  - Task
  - Skill
last-verified: 2026-06-10
---

# Create Pull Request

Create a PR with a concise description, then offer a review proportional to the PR's size.

## Phase 1: Gather Context

**CRITICAL: Resolve the target branch first and diff against `origin/<target>`, never a bare local branch.** Local branch refs go stale and produce massively inflated diffs. Prefer an existing PR's base branch; otherwise use the workspace target (`CONDUCTOR_TARGET_BRANCH` or an explicit Conductor/system instruction) and only fall back to `stg` when no target is available.

Run these commands in parallel to understand the PR scope:

```bash
# Determine target branch: existing PR base, workspace target, then stg fallback
TARGET=$(gh pr view --json baseRefName -q '.baseRefName' 2>/dev/null || echo "")
if [ -z "$TARGET" ]; then
  TARGET="${CONDUCTOR_TARGET_BRANCH:-stg}"
fi

# ALWAYS fetch the target remote ref first
git fetch origin "$TARGET"

# Get commit history vs base branch (MUST use origin/<target>, not a bare local branch)
git log --oneline "origin/$TARGET..HEAD"

# Get diff stats (MUST use origin/<target>, not a bare local branch)
git diff --stat "origin/$TARGET...HEAD"

# Check if PR already exists
gh pr view 2>/dev/null || echo "No PR exists yet"
```

**WARNING:** If the diff shows dozens of files but you only made a few commits, you are likely diffing against the wrong base. Double-check you are using the actual PR/workspace target remote ref (`origin/<target>`), not a stale local branch or hard-coded default.

**Also gather external context:**

1. **Linear ticket** — Extract the ticket ID from the branch name (e.g., `t-1716-...` → `T-1716`). Use the Linear MCP (`mcp__claude_ai_Linear__get_issue`) to fetch the full issue description, acceptance criteria, and related issues. Weave this context into the PR description.
2. **Existing PR description** — If updating an existing PR, read the current body first (`gh pr view --json body`). Preserve any existing content the user wants to keep (videos, links, notes) and enhance it with the new information.
3. **User-provided context** — If the user has given you a description, video link, or other context, incorporate it directly. Don't discard user-provided text.

## Phase 1.5: Docs freshness check

Before assessing size, check whether the diff touches files covered by docs or skills. Stale docs are worse than missing docs.

```bash
# Files changed in this PR
TARGET=$(gh pr view --json baseRefName -q '.baseRefName' 2>/dev/null || echo "${CONDUCTOR_TARGET_BRANCH:-stg}")
git fetch origin "$TARGET"
CHANGED=$(git diff --name-only "origin/$TARGET...HEAD")

# Find docs that reference any changed file
for f in $CHANGED; do
  grep -rln --include="*.md" "$f" docs/ .agents/skills/ 2>/dev/null
done | sort -u
```

For each doc surfaced:

1. **If the doc is still accurate** (the change doesn't invalidate it): bump `last-verified: YYYY-MM-DD` in its frontmatter. This signals to future readers that someone confirmed it.
2. **If the doc is now stale** (your change makes part of it wrong): either fix it in this PR (preferred for small docs), or mark it `status: deprecated` with a one-line "superseded by <PR-link>" note and open a follow-up ticket. Don't ship a PR that silently makes a doc lie.
3. **If the change has no doc but probably should** (new public surface, new runbook scenario, new failure mode): add a short doc under `docs/runbooks/` or extend an existing one. Don't gold-plate — a stub with `status: snapshot` is better than nothing.

Skip this phase entirely for: dependency bumps, lint fixes, internal refactors that don't change any public surface, test-only changes.

Note the decision in the PR description's `## Changes` section ("Refreshed `docs/runbooks/foo.md` last-verified" or "Marked `docs/wiki/observability.md` deprecated, follow-up T-XXXX").

## Phase 1.6: Migration Drift Check

Before assessing size, check whether the PR contains migration files:

```bash
TARGET=$(gh pr view --json baseRefName -q '.baseRefName' 2>/dev/null || echo "${CONDUCTOR_TARGET_BRANCH:-stg}")
git fetch origin "$TARGET"
git diff --name-status "origin/$TARGET...HEAD" -- \
  packages/database/migrations \
  packages/database/clickhouse-migrations
```

Only keep migrations that are part of the current task. If the diff contains Postgres or ClickHouse migrations added solely because a dev stack had them recorded, remove those files from the branch and reconcile the dev stack instead. Dev-stack migration state is disposable; PR history is not.

## Phase 2: Assess PR Size & Complexity

Classify the PR before proceeding:

| Size | Files Changed | Complexity | Example |
|------|---------------|------------|---------|
| **Small** | 1-5 files | Config, docs, deps, simple fixes | README update, dependency bump |
| **Medium** | 5-15 files | Feature work, refactors with tests | New API endpoint, component rewrite |
| **Large** | 15+ files | Cross-cutting changes, architecture | New system, major refactor |

**Skip CI checks (Phase 3) for Small PRs.** Only run them for Medium/Large.

## Phase 3: Run CI Checks (Medium/Large PRs only)

```bash
pnpm run typecheck &
pnpm run lint &
pnpm run test &
wait
```

**If any check fails, fix before proceeding.**

If the repo-wide build graph still includes intentionally Bun-targeted packages, ensure the
relevant CI jobs install Bun explicitly rather than assuming the shared install action does.

## Phase 4: Create/Update PR

### Title Format

Follow Conventional Commits: `<type>(<scope>): <subject>`

- **Allowed types:** `feat`, `fix`, `refactor`, `chore` ONLY (CI rejects others)
- **Subject:** lowercase, imperative mood (e.g., "add retry logic", "fix race condition")
- **NO emojis in title** - CI will reject

> ⚠️ **Common failure: `docs:` is NOT allowed here.** This repo's `semantic-pr.yml` permits **only** `feat`/`fix`/`refactor`/`chore`, which is narrower than the standard Conventional Commits set. The standard types `docs`, `test`, `style`, `perf`, `ci`, `build`, `revert` all **fail** CI even though they're valid Conventional Commits. Map them like this before you set the title:
>
> | What changed | ❌ Don't use | ✅ Use |
> |---|---|---|
> | Docs / RFCs / wiki / comments only | `docs` | `chore` |
> | Tests only | `test` | `chore` |
> | Formatting / lint-only | `style` | `chore` |
> | CI / workflow / tooling | `ci`, `build` | `chore` |
> | Perf improvement | `perf` | `refactor` (or `fix` if it fixes a regression) |
> | New user-facing capability | — | `feat` |
> | Bug fix | — | `fix` |
> | Restructure without behavior change | — | `refactor` |
>
> The allowed set is the source of truth — confirm it against `.github/workflows/semantic-pr.yml` (`with.types`) if unsure rather than assuming the standard Conventional Commits list applies.

**After creating/editing the PR, verify the title check passed** before claiming the PR is ready:

```bash
gh pr checks <pr-number> 2>&1 | grep -i "PR Title"   # must show "pass", not "fail"
```

If it shows `fail`, the type is wrong — remap per the table above and `gh pr edit --title`.

### Body

**Every PR gets the same template** — Motivation, Changes, Test plan. Scale the *depth* of each section, not whether it exists:

- **Small PRs:** 1-2 sentences per section, keep it tight
- **Medium PRs:** A paragraph per section, explain key decisions
- **Large PRs:** Full detail, architecture diagrams, security notes

**Mine the commit history.** Commits should already tell the full story (see CLAUDE.md "Git & PRs"). Read through `git log origin/<target>..HEAD` — the subjects, bodies, and progression show what was asked, what approach was taken, and what decisions were made. Use this to inform the PR description so reviewers get the full context without digging through individual commits.

#### Template (ALL PRs)

```markdown
## Motivation
WHY this change exists. Link to Linear ticket if applicable.
Even for small PRs — one sentence explaining the problem or ask.

## Changes
What was done. For small PRs a few bullets is fine.
For medium/large, organize by area (backend, frontend, infra, database).
Explain key decisions, not just list files.

## Architecture (Large PRs only)
Include mermaid diagrams showing before/after architecture, data flow, or component relationships.

## Security (if applicable)
Call out IAM changes, secret handling, auth changes, or permission scoping.

## Demo
REQUIRED for any PR with UI changes (see Phase 5 "Visual Evidence Requirements"):
- Complex UI change (Medium/Large) → embed a Loom video walkthrough.
- Simple UI change (Small) → screenshots at minimum; GIF/Loom preferred.
- No UI changes → omit this section.

## Test plan
- [ ] Concrete verification steps
- [ ] Include both happy path and edge cases
- [ ] Use `✅` / `❌` for browser and manual verification results
```

#### Mermaid Diagrams

**Think about what's most impactful to visualize.** Mermaid diagrams aren't just for architecture PRs — use them whenever a visual helps reviewers understand the change faster than reading code or prose.

Consider adding diagrams for:
- **Before/after architecture** — infrastructure, component topology, system boundaries
- **Data flow** — how data moves through the system (API → service → DB → cache → external)
- **Request lifecycles** — what happens when a webhook fires, a user clicks deploy, etc.
- **State machines** — status transitions, feature flag logic, retry flows
- **Code topology** — which packages/modules depend on each other, what's new vs removed
- **Decision trees** — branching logic that's hard to follow in code

Pick 1-2 diagrams that add the most clarity. Don't diagram everything — diagram the thing that would take a reviewer 5 minutes to reconstruct mentally.

#### PR Overview Images

When the user asks for a PR overview image, GPT image gen, a visual PR summary, or a polished raster diagram of the PR, invoke the `pr-overview-image` skill. Use it after gathering the same PR context above so the image is grounded in the real commit history, diff, PR body, and code paths.

Use this for large architecture PRs when Mermaid alone would be too dense, especially when the reviewer needs to understand runtime flow, trust boundaries, and where the code lives by layer. Keep the generated image as supplemental visual context; the PR body still needs the standard Motivation, Changes, Architecture, Security, and Test plan sections.

```markdown
\`\`\`mermaid
graph LR
    A["Component A"] --> B["Component B"]
    B --> C["Component C"]
    style B fill:#51cf66,color:#fff
\`\`\`
```

### Create the PR

```bash
# If no PR exists, create one
gh pr create --title "<title>" --body "<body>"

# If PR exists, update it
gh pr edit --title "<title>" --body "<body>"
```

**Always use `gh` CLI with HEREDOC for the body to preserve formatting:**

```bash
gh pr edit --body "$(cat <<'EOF'
## Summary
...
EOF
)"
```

## Phase 4.5: Apply Labels

Labels give the PR list a lightweight triage lens. **Division of responsibility:** Linear is the source of truth for *what work this is* (initiative → project → issue, reached via the `T-XXXX` link); GitHub labels only answer *what this PR touches + is it healthy*. So don't try to encode the roadmap or initiative in a GitHub label — it will drift from Linear. Keep the label set to what's below.

Apply **exactly one roadmap bucket** (mirrors Linear's 4 project labels), plus optional area/health labels. Derive the bucket from the changed paths:

```bash
TARGET=$(gh pr view --json baseRefName -q '.baseRefName' 2>/dev/null || echo "${CONDUCTOR_TARGET_BRANCH:-stg}")
git fetch origin "$TARGET"
CHANGED=$(git diff --name-only "origin/$TARGET...HEAD")
```

| Roadmap label | Path signals |
|---|---|
| `Infra` | `infra/`, `.github/workflows/`, `infra/pulumi/`, `infra/docker/`, `packages/infrastructure/`, observability, RDS/EKS/K8s, runtime |
| `Agents & AI` | `packages/mcp-tools/`, `.agents/` (skills + subagent config), `.claude/`/`.codex/` tooling, agent/opencode services, MCP tools, agent prompting |
| `Operations` | customer-workflow ops, `sdk/`, SFTP/EDI, customer-facing operational fixes |
| `Core App` | everything else — `apps/frontend/`, most of `apps/api/`, `packages/database/`, auth, notifications, credentials, backend Effect refactors |

When a PR spans areas, pick the bucket matching its **primary intent**, not its incidental file touches (e.g. a frontend tweak that also edits one infra YAML is still `Core App`). If a `T-XXXX` ticket exists and you already fetched it in Phase 1, prefer the bucket matching its Linear **project label** when paths are ambiguous.

**Optional fine-area labels** (mirror Linear issue labels — add when clearly applicable): `Frontend`, `Builder`, `Notifications`, `Security`.

**Health labels:** add `size:xl` when the PR exceeds **50 files or 2k LOC changed**. This is a deliberately heavier bar than Phase 2's "Large" (15+ files) — `size:xl` flags genuine review-burden monsters, so a 20-file PR is "Large" for review purposes but does *not* get `size:xl`. Never add `stale` at creation — it's applied by triage to PRs idle >30 days.

**Do NOT auto-apply the mechanical labels** `deploy-stg`, `preview:app`, or `skip-wiki` — those are user-triggered CI toggles, not categorization.

**Enforce "exactly one roadmap bucket" on updates, not just creation.** `--add-label` only *appends* — when re-labeling an existing PR whose classification changed, a stale bucket would linger and the PR would carry two. Strip the other three roadmap labels before adding the chosen one:

```bash
BUCKET="Core App"   # the single roadmap label chosen above
# Remove any other roadmap bucket first so exactly one remains (no-op if not present)
for rb in "Infra" "Core App" "Agents & AI" "Operations"; do
  [ "$rb" != "$BUCKET" ] && gh pr edit <pr-number> --remove-label "$rb" 2>/dev/null
done
# Then add the chosen bucket. Add fine-area / health labels ONLY when they actually apply —
# don't copy the optional ones verbatim. e.g. "Builder" only for a builder change,
# "size:xl" only when >50 files / 2k LOC:
gh pr edit <pr-number> --add-label "$BUCKET"
# gh pr edit <pr-number> --add-label "Builder"   # ← only if this is a builder PR
# gh pr edit <pr-number> --add-label "size:xl"   # ← only if >50 files / 2k LOC
```

Fine-area and health labels are additive on creation. On an **update**, also drop any fine-area or health label that no longer matches the current diff — e.g. remove `size:xl` if the PR shrank below 50 files / 2k LOC, or `Builder`/`Frontend` if it no longer touches those paths. Same staleness reconciliation as the roadmap bucket, just non-exclusive (you may keep several). If a label doesn't exist yet, create it with `gh label create` (the roadmap/area set already exists in this repo). Then confirm:

```bash
gh pr view <pr-number> --json labels --jq '[.labels[].name] | join(", ")'
```

## Phase 5: Visual Evidence (required for UI changes)

Before offering a code review, check whether this PR likely contains visual/UI changes. Grep the diff for frontend signals:

```bash
TARGET=$(gh pr view --json baseRefName -q '.baseRefName' 2>/dev/null || echo "${CONDUCTOR_TARGET_BRANCH:-stg}")
git fetch origin "$TARGET"
git diff --name-only "origin/$TARGET...HEAD" | grep -E '(apps/frontend/|\.tsx?$|\.css$|\.scss$|tailwind\.config|components/ui/)'
```

**If nothing matches, skip this phase entirely** — infra-only, backend-only, docs, and dependency bumps don't need it. Only continue when the grep found UI signals or the diff mentions `className`, a new route, a new component, a modal, or any copy/layout change.

### Visual Evidence Requirements

When the grep matches, the PR **must** include visual evidence in the `## Demo` section before it counts as ready. The bar scales with the complexity classified in Phase 2:

| PR has UI changes AND… | Required evidence |
|------------------------|-------------------|
| Complexity is **Medium or Large** — new flow, new route, multi-step interaction, non-trivial state | **Loom video walkthrough — REQUIRED.** Screenshots or a GIF alone are not enough. |
| Complexity is **Small** — copy tweak, styling, single static component | **Screenshots — REQUIRED minimum.** A GIF or Loom is better but not mandatory. |

"Required" means you must not present the PR as done without it:

- **Complex UI change with no Loom** → do not treat the PR as finished. Explicitly tell the user the PR is blocked on a Loom walkthrough and ask them to record one (or provide the link). A `/user-test` GIF is a decent stand-in for *simple* flows, but a complex UI change still needs a real Loom so reviewers see intent and narration, not just clicks.
- **Any UI change with no screenshots** → at minimum, capture screenshots before the PR is considered ready.

Never silently skip this — if evidence is missing, surface it to the user.

When UI signals match, first check if a fresh test run already exists for this branch:

```bash
LATEST_RUN=$(ls -dt .context/user-tests/*/ 2>/dev/null | head -1)
HEAD_TIME=$(git show -s --format=%ct HEAD)
if [ -n "$LATEST_RUN" ] && [ "$(stat -f %m "$LATEST_RUN" 2>/dev/null || stat -c %Y "$LATEST_RUN")" -gt "$HEAD_TIME" ]; then
  echo "Existing fresh test run: $LATEST_RUN"
fi
```

If a fresh run exists, offer to attach *that* one directly:

> Found a fresh walkthrough at `$LATEST_RUN` — want me to attach it to the PR?

On yes: `.claude/skills/user-test/scripts/attach-to-pr.sh "$LATEST_RUN"`.

If no fresh run exists, ask whether to capture one:

> This PR touches the UI. Want me to `/user-test` it and attach a screenshot walkthrough + GIF to the PR comments?

On **yes**: invoke `/user-test` with a slug derived from the PR title. The `/user-test` skill will take screenshots as it walks the flow, build a GIF, and (per its Step 9) offer to attach everything as a PR comment.

On **no**: continue to Phase 6.

## Phase 6: Offer Review

After creating the PR (and handling the visual walkthrough), **tell the user the PR is created** with the link, then suggest an appropriate review level:

### Small PRs (config, docs, deps)
> PR created: <link>
>
> This is a small config/docs PR. A full review is probably overkill.
> Run `/pr-review` if you want one anyway.

### Medium PRs (features, refactors)
> PR created: <link>
>
> This is a medium-sized PR. I'd recommend a review.
> Run `/pr-review` to kick off the Staff Engineer review.

### Large PRs (architecture, cross-cutting)
> PR created: <link>
>
> This is a large PR. I'd strongly recommend a review before merging.
> Run `/pr-review` to kick off the Staff Engineer review.

**Do NOT auto-run the review.** Let the user decide. They can always run `/pr-review` later.

## Important Rules

- **NEVER add "Generated with Claude Code", "Co-Authored-By: Claude", or AI attribution**
- **Don't auto-run review** — suggest it, let the user decide
- **Every PR gets the full template** (Motivation, Changes, Test plan) — scale the depth per section, not whether it exists
- **Use `gh` CLI for diffs** — never use workspace diff MCP tools
- **Use `pnpm` for repo-wide validation** — do not default to `bun run ...` for root checks
- **Reject dev-stack migration drift** — if migration files appear only because a dev stack was ahead/diverged, remove them from the PR and fix that stack's migration metadata instead
- **Always fetch Linear context** — branch names encode the ticket ID, use it
- **Preserve user content** — videos, links, and descriptions the user provided must be kept
- **Include mermaid diagrams** whenever a visual adds clarity — architecture, data flow, state machines, code topology, not just infra PRs
- **Call out security implications** when IAM, secrets, or auth patterns change
- **K8s sandboxes are dead** — do not describe sandbox/runtime behavior as K8s-backed unless the PR is explicitly removing legacy K8s compatibility
- **Manual/browser validation should be explicit** — when you tested real flows, list them in the PR using `✅` and `❌`, for example `✅ create workflow`, `✅ deploy workflow`, `✅ builder chat send`, `❌ general agent session load`
- **Visual evidence is mandatory for UI PRs** — if the diff touches `apps/frontend/` or any `.tsx`/`.css` files, the PR's `## Demo` section must carry visual proof. A **complex** UI change (Medium/Large) **requires a Loom video walkthrough**; a **simple** UI change requires **screenshots at minimum**. Offer `/user-test` to capture screenshots + GIF, but if a complex change has no Loom, tell the user the PR is blocked on one rather than shipping it silently.
- **Distinguish required vs informational statuses** — GitHub Actions checks gate the PR; external review
  contexts like Devin may remain pending and should be called out separately instead of being confused
  with failing CI
- **Label every PR** — exactly one roadmap bucket (`Infra` / `Core App` / `Agents & AI` / `Operations`, mirroring Linear's project labels) from changed paths, plus optional area (`Frontend`/`Builder`/`Notifications`/`Security`) and `size:xl` for PRs over 50 files / 2k LOC. Labels are GitHub's lightweight "what + health" lens; the initiative/project lives in Linear via the `T-XXXX` link. Never auto-apply `stale` or the mechanical `deploy-stg`/`preview:app`/`skip-wiki` toggles.
