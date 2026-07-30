<!-- Preserved pre-Codex/Synor import. -->
---
name: codex
description: >-
  Delegate work to Codex (OpenAI's CLI agent) — code review, adversarial/challenge
  review, a second-opinion diagnosis or implementation pass, rescue when Claude is
  stuck, or PR overview image generation. Use whenever the user types "codex" or asks
  to "ask codex", "have codex review", "codex review", "adversarial review", "get a
  second opinion from codex", "hand this to codex", "codex this", or "let codex try".
  Encodes the routing policy: in Claude Code, run Codex as a BACKGROUND task; inside
  Codex itself, use a subagent instead of re-invoking Codex.
---

# Codex Delegation

Route any "codex" request to the Codex CLI agent using the right execution mode for the current runtime. This skill is a **router + policy**, not a reimplementation — it wraps the `codex` CLI (and, when present, the installed `/codex:*` plugin commands).

## Routing policy (the one rule)

```
Are you running inside Codex already?
├─ NO  → You are Claude Code. Run Codex as a BACKGROUND Bash task. Don't block the turn.
└─ YES → Do NOT shell out to `codex exec` recursively. Use a subagent (Task) instead,
         or just do the work inline.
```

- **In Claude Code (default here):** spawn Codex with `Bash({ ..., run_in_background: true })`. Tell the user it's running, then continue. The harness re-invokes you when the background task finishes — read its output then and relay/triage it. Never paste a raw `codex exec` into a foreground Bash call for a non-trivial review or task; it blocks the whole turn.
- **Inside Codex:** you *are* Codex — re-invoking `codex exec` is a recursive loop. Delegate to a subagent or do it directly.
- **Tiny, clearly-bounded asks** (1–2 files, a quick question): foreground is acceptable in Claude if the user explicitly asked to wait. When in doubt, background.

## Use cases & commands

All commands run from the repo root.

**Resolve the base ref first — don't hard-code `origin/stg`.** Most feature PRs target `stg`, but some target `prod` (or another branch), and reviewing against the wrong base produces a huge/unrelated diff and false findings. Derive it from the PR, fall back to `stg` only when there's no PR:

```bash
git fetch origin -q
BASE=$(gh pr view --json baseRefName -q .baseRefName 2>/dev/null || echo stg)
```

Then use `origin/$BASE` everywhere below. Always `git fetch` in the **outer** shell before launching Codex — never inside a read-only Codex sandbox (it has no network and can't write `.git`, so a fetch there fails or silently reviews a stale ref).

### 1. Code review (current branch vs base)

```bash
git fetch origin -q
BASE=$(gh pr view --json baseRefName -q .baseRefName 2>/dev/null || echo stg)
codex exec review --base "origin/$BASE"
```

Working-tree review (staged + unstaged + untracked):

```bash
codex exec review --uncommitted
```

### 2. Adversarial / challenge review

A challenge review that questions the *approach*, design choices, tradeoffs, and assumptions — not just defects. **Review-only: do not let Codex apply fixes here.**

`codex exec review --base <ref>` does **not** accept a positional prompt (the CLI errors with `the argument '--base <BRANCH>' cannot be used with '[PROMPT]'`). So for a branch-scoped adversarial review, fetch + compute the base in the outer shell, then pass the precomputed diff range into a plain read-only `codex exec`:

```bash
git fetch origin -q
BASE=$(gh pr view --json baseRefName -q .baseRefName 2>/dev/null || echo stg)
codex exec --sandbox read-only "Adversarial review of this branch against origin/$BASE. \
Run \`git diff origin/$BASE...HEAD\` to see the changes (already fetched — do not run git fetch; \
this sandbox has no network). Challenge whether this is the right approach — question the design, \
the tradeoffs, and the assumptions it depends on. Where does it fail under real-world load, \
concurrency, or failure modes? Critique only; do not propose or apply patches."
```

(A working-tree adversarial review *can* pass framing as custom instructions — `codex exec review "<challenge instructions>"` with no `--base` — but the plain `codex exec` form above is the portable way to challenge a branch against a base.)

### 3. Second-opinion / rescue / implementation pass

For diagnosis, a deeper root-cause hunt, or handing off a substantial coding task when Claude is stuck or the user wants a different engine.

```bash
# Read-only (diagnosis, research, review — no edits):
codex exec --sandbox read-only "<tight task description>"

# Write-capable (implement / fix — default for an explicit fix request):
codex exec --sandbox workspace-write "<tight task description>"
```

Keep the prompt self-contained: Codex does **not** see this conversation. Include the relevant file paths, the symptom, what was already tried, and the success criterion.

### 4. PR overview image

Don't hand-roll this — invoke the **`pr-overview-image`** skill. It already branches on runtime (native `image_gen` when you're Codex; `codex exec` delegation when you're Claude).

## Background launch pattern (Claude Code)

Fetch and resolve `$BASE` in the outer command (the Codex sandbox can't), then launch:

```typescript
// Plain review against the PR base (no positional prompt allowed with --base):
Bash({
  command: `git fetch origin -q && BASE=$(gh pr view --json baseRefName -q .baseRefName 2>/dev/null || echo stg) && codex exec review --base "origin/$BASE"`,
  description: "Codex review (background)",
  run_in_background: true,
})

// Adversarial / custom-framing review against the base — fetch outside, plain read-only exec:
Bash({
  command: `git fetch origin -q && BASE=$(gh pr view --json baseRefName -q .baseRefName 2>/dev/null || echo stg) && codex exec --sandbox read-only "Adversarial review vs origin/$BASE. Run \\\`git diff origin/$BASE...HEAD\\\` (already fetched; no network in sandbox). Critique the approach; do not patch."`,
  description: "Codex adversarial review (background)",
  run_in_background: true,
})
```

Then: tell the user "Codex review started in the background — I'll relay the findings when it's done." Do not poll in a tight loop; the harness notifies you on completion.

## Prefer the plugin when it's installed

If the `openai-codex` plugin is available in this Claude Code session, its slash commands wrap the same runtime with better status/result plumbing and the same background semantics:

- `/codex:review --background`
- `/codex:adversarial-review --background`
- `/codex:rescue` (forwards to the `codex:codex-rescue` subagent; picks foreground/background by task size)
- `/codex:status`, `/codex:result`, `/codex:cancel`

Either path is fine; the routing policy above (background-in-Claude, subagent-in-Codex) still governs.

## Hard constraints

- **Review modes are review-only.** When the user asks for a review (plain or adversarial), Codex critiques — it must not apply patches. Surface its findings; do not auto-act on them.
- **Don't block the turn.** Default to background in Claude for anything beyond a trivial bounded ask.
- **No recursion.** If you are Codex, never call `codex exec` — use a subagent.
- **Relay faithfully.** When relaying Codex output to the user, return its findings without inventing or softening them. For an explicit `--wait`/foreground review, the plugin convention is to return Codex's output verbatim.
