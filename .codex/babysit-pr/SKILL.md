---
name: babysit-pr
description: Persistently monitor a GitHub pull request's CI, reviews, and mergeability until it is ready, merged, closed, or genuinely blocked. Use when the user asks Codex to babysit, monitor, watch, or keep handling an open PR. Monitoring-only requests stay read-only; reruns, fixes, pushes, replies, and resolutions require the user to explicitly ask Codex to handle or fix failures and feedback.
---

# PR Babysitter

## Objective
Babysit a PR persistently until one of these terminal outcomes occurs:

- The PR is merged or closed.
- CI is successful, there are no unaddressed review comments surfaced by the watcher, required review approval is not blocking merge, and there are no potential merge conflicts (PR is mergeable / not reporting conflict risk).
- A situation requires user help (for example CI infrastructure issues, repeated flaky failures after retry budget is exhausted, permission problems, or ambiguity that cannot be resolved safely).

Do not stop merely because a single snapshot returns `idle` while checks are still pending.

## Action authority

This section takes precedence over later automation guidance.

- For **monitor**, **watch**, or **keep an eye on** requests, inspect and report only.
- For **babysit and handle**, **fix failures**, **address feedback**, or equivalent
  explicit requests, make clear branch-local fixes, commit, push, retry likely
  flaky checks, and resolve fully addressed threads as described below.
- Do not infer permission to merge, approve, close, force-push, change the base
  branch, or alter repository settings.
- Read `AGENTS.md` before changing code and inspect the worktree for unrelated
  user changes. Stop if safe isolation is not possible.

## Inputs
Accept any of the following:

- No PR argument: infer the PR from the current branch (`--pr auto`)
- PR number
- PR URL

## Core Workflow

1. When the user asks to monitor, watch, or babysit a PR, start with the watcher's continuous mode (`--watch`) unless you are intentionally doing a one-shot diagnostic snapshot.
2. Run the watcher script to snapshot PR/CI/review state (or consume each streamed snapshot from `--watch`).
3. Inspect the `actions` list in the JSON response.
4. If `diagnose_ci_failure` is present, inspect failed run logs and classify the failure.
5. If the failure is likely caused by the current branch and fix authority is explicit, patch code locally, commit, and push; otherwise report the diagnosis.
6. If `process_review_comment` is present, inspect surfaced review items and decide whether to address them.
7. If a review item is actionable and correct and fix authority is explicit, patch code locally, commit, and push; otherwise report it.
8. If the failure is likely flaky/unrelated, `retry_failed_checks` is present, and retry authority is explicit, rerun failed jobs with `--retry-failed-now`.
9. If both actionable review feedback and `retry_failed_checks` are present, prioritize review feedback first; a new commit will retrigger CI, so avoid rerunning flaky checks on the old SHA unless you intentionally defer the review change.
10. On every loop, verify mergeability / merge-conflict status (for example via `gh pr view`) in addition to CI and review state.
11. After any push or rerun action, immediately return to step 1 and continue polling on the updated SHA/state.
12. If you had been using `--watch` before pausing to patch/commit/push, relaunch `--watch` yourself in the same turn immediately after the push (do not wait for the user to re-invoke the skill).
13. Repeat polling until the PR is green + review-clean + mergeable, `stop_pr_closed` appears, or a user-help-required blocker is reached.
14. Maintain terminal/session ownership: while babysitting is active, keep consuming watcher output in the same turn; do not leave a detached `--watch` process running and then end the turn as if monitoring were complete.

## Commands

Resolve `SKILL_DIR` to the directory containing this `SKILL.md`; do not assume
the current working directory is the repository root.

### One-shot snapshot

```bash
python3 "$SKILL_DIR/scripts/gh_pr_watch.py" --pr auto --once
```

### Continuous watch (JSONL)

```bash
python3 "$SKILL_DIR/scripts/gh_pr_watch.py" --pr auto --watch
```

### Trigger flaky retry cycle (only when watcher indicates)

```bash
python3 "$SKILL_DIR/scripts/gh_pr_watch.py" --pr auto --retry-failed-now
```

### Explicit PR target

```bash
python3 "$SKILL_DIR/scripts/gh_pr_watch.py" --pr <number-or-url> --once
```

## CI Failure Classification
Use `gh` commands to inspect failed runs before deciding to rerun.

- `gh run view <run-id> --json jobs,name,workflowName,conclusion,status,url,headSha`
- `gh run view <run-id> --log-failed`

Prefer treating failures as branch-related when logs point to changed code (compile/test/lint/typecheck/snapshots/static analysis in touched areas).

Prefer treating failures as flaky/unrelated when logs show transient infra/external issues (timeouts, runner provisioning failures, registry/network outages, GitHub Actions infra errors).

If classification is ambiguous, perform one manual diagnosis attempt before choosing rerun.

Read [references/heuristics.md](references/heuristics.md) for a concise checklist.

## Review Comment Handling

### Reply and resolve when the fix is clear (default); ask only when it isn't

Replies and resolutions happen under the **user's GitHub identity**, so be deliberate — but when you've made a clearly-correct fix that unambiguously addresses a comment, just reply (cite the fix SHA) and resolve the thread. Don't stop the flow to ask. The gate is **how clear the fix is** and **who left the comment**:

- **Bot threads** (Codex, Devin, automated reviewers) with a clear-cut fix → reply + resolve autonomously. Report what you closed in your summary; no need to ask first.
- **Human threads** with a clearly-correct fix that *fully* addresses the point → you may also reply + resolve autonomously, **but you MUST surface it at the end of the session** — list which human thread(s) you auto-resolved and what you changed, so the reviewer can reopen if they disagree.
- **When the fix is NOT really clear** — judgment calls, design/architecture discussion, "should we…/consider…/what about…", ambiguous asks, or anything you're not confident *fully* resolves the comment → do **NOT** auto-resolve. Surface it in chat and let the user decide. (Same for any human comment where the right change isn't obvious.)
- **NEVER deflect-and-resolve.** A dismissive or explanatory reply that closes a thread *without* a real underlying fix is always forbidden. If you think a thread is non-actionable, surface your reasoning and ask — don't close it.

What you always do autonomously regardless: read/triage comments and fix clearly-correct bugs in code (commit + push). When a fix isn't clear enough to auto-resolve, push the correct code anyway, then surface the thread decision as a request and **keep babysitting** — continue polling CI and mergeability on the new SHA while you wait.

The watcher surfaces review items from:

- PR issue comments
- Inline review comments
- Review submissions (COMMENT / APPROVED / CHANGES_REQUESTED)

It intentionally surfaces AI reviewer bot feedback (for example comments/reviews from `chatgpt-codex-connector[bot]`, `devin-ai-integration[bot]`, or similar) in addition to human reviewer feedback. Most unrelated bot noise should still be ignored.
For safety, the watcher only auto-surfaces trusted human review authors (for example repo OWNER/MEMBER/COLLABORATOR, plus the authenticated operator) and approved AI review bots such as Codex and Devin.
On a fresh watcher state file, existing pending review feedback may be surfaced immediately (not only comments that arrive after monitoring starts). This is intentional so already-open review comments are not missed.

### Human vs Bot comments — different rules (CRITICAL)

Treat human and bot review comments differently. The author's `type == "Bot"` (or login ending in `[bot]`) is the dividing line.

**Bot comments** (Codex, Devin, automated reviewers): triage and act — fix real bugs locally (commit + push), ignore unrelated noise, and when your fix is clear-cut, reply + resolve the thread autonomously. Only hold off on resolving when the fix isn't clearly complete (then surface and ask).

**Human comments** (any author whose `type != "Bot"`, including OWNER/MEMBER/COLLABORATOR/CONTRIBUTOR and the PR author themselves): be more conservative than with bots, but you are no longer blanket-barred from acting.

- **Auto-fix only when the change is clear** — a fix the reviewer effectively pointed at, or an unambiguous correctness fix. Anything involving "should we…", "consider…", "what about…", a screenshot of broken UI, a feature-flag request, an architectural suggestion, or *any* judgement call → don't auto-fix; surface and let the user drive. **When in doubt, surface, don't fix.**
- **Reply + resolve a human thread only when your fix clearly and fully addresses it.** When you do, you **MUST** call it out in your end-of-session summary (which thread, what you changed, the comment URL) so the reviewer can reopen if they disagree. If the fix isn't clearly complete, or the comment is a discussion rather than a defect → reply/resolve waits; surface it as "needs your reply" and leave the thread open.
- **ALWAYS surface unresolved human comments** you did *not* close — every poll, list author, file:line (or "PR-level"), the body (or a tight summary if long), the comment URL. Frame as the user's action item.

Continue polling and stop conditions still apply. The merge-ready condition is satisfied once every thread you could resolve is resolved; any human thread you deliberately left open (unclear fix, or a discussion) still blocks merge-ready and is the user's to close.

### Querying ALL Review Comments

The watcher may not surface every comment. Always cross-check by querying ALL review threads directly via GraphQL to find unresolved comments:

```bash
gh api graphql -f query='
query {
  repository(owner: "OWNER", name: "REPO") {
    pullRequest(number: PR_NUMBER) {
      reviewThreads(first: 100) {
        nodes {
          id
          isResolved
          comments(first: 3) {
            nodes {
              author { login }
              body
              path
            }
          }
        }
      }
    }
  }
}'
```

Filter for `isResolved == false` to find threads that still need attention. This catches comments the watcher may miss, including bot review comments from Devin, Codex, and others. Do this check on every poll cycle, not just when the watcher surfaces items.

### Addressing Review Comments

The code-fix portion of this flow applies to **bot comments** and to **clear human fixes** (per "Human vs Bot" above). For human comments that need judgment, surface and stop — don't patch code on your own.

When you agree with a comment and the fix is actionable:

1. Patch code locally.
2. Commit with `codex: address PR review feedback (#<n>)`.
3. Push to the PR head branch.
4. **If the fix clearly and fully addresses the comment, reply + resolve the thread** — bots autonomously; humans autonomously too, but note it for the end-of-session summary. Reply citing the fix SHA, then resolve:
   ```bash
   # Reply:
   gh api repos/OWNER/REPO/pulls/PR_NUMBER/comments/COMMENT_ID/replies \
     -f body='Fixed in COMMIT_SHA: description of fix.'
   ```
   ```bash
   # Resolve the thread (replying alone does NOT resolve it):
   gh api graphql -f query='
   mutation {
     resolveReviewThread(input: { threadId: "THREAD_NODE_ID" }) {
       thread { isResolved }
     }
   }'
   ```
   The `THREAD_NODE_ID` comes from the `reviewThreads` query above (the `id` field on each thread node, e.g. `PRRT_kwDO...`). This is NOT the same as the REST API comment ID. **Auto reply + resolve applies to inline review threads only** — that's what the replies endpoint and `resolveReviewThread` target. PR-level issue comments and review-summary submissions aren't resolvable threads; act on their substance (fix code if needed) and surface them to the user rather than trying to resolve them.
5. **If the fix is NOT clearly complete, or the comment is a judgment/discussion call**, don't resolve. Surface it as a request — report the SHA, draft the reply you'd post, and let the user decide.
6. Resume watching on the new SHA immediately after the push (do not stop after reporting the push).
7. If monitoring was running in `--watch` mode, restart `--watch` immediately after the push in the same turn.

If you disagree or the comment is non-actionable/already addressed, **don't deflect-and-resolve**. Surface your reasoning to the user in chat (e.g. "I think this thread is moot because the file was deleted — want me to reply with that and resolve it?") and let them decide. Leaving a thread open is always safe; closing it without a real fix is not.

If a code review comment/thread is already marked as resolved in GitHub, treat it as non-actionable and safely ignore it unless new unresolved follow-up feedback appears.

### Threads you couldn't auto-resolve (don't loop on them)

A thread you've code-fixed + pushed but deliberately left open — because the fix wasn't clearly complete, or it's a judgment/discussion call you surfaced for the user — is **handled** even though it's still `isResolved == false`. Track these (by thread ID / commit SHA) and:

- **Do not re-process them.** On later polls, don't re-fix, re-diff, or re-surface a thread whose feedback you already addressed and reported. Re-surface only if genuinely new follow-up feedback arrives.
- **They block merge-ready until the user closes them.** In repos that require conversation resolution before merge, an open thread is a genuine blocker. If the only thing left is such threads, reach the **user-action-required** terminal state: stop looping and report "code done + CI green; these threads need your call to reply/resolve." Clearly-correct fixes should already have been resolved autonomously and won't be in this bucket.

## Git Safety Rules

- Work only on the PR head branch.
- Avoid destructive git commands.
- Do not switch branches unless necessary to recover context.
- Before editing, check for unrelated uncommitted changes. If present, stop and ask the user.
- After each successful fix, commit and `git push`, then re-run the watcher.
- If you interrupted a live `--watch` session to make the fix, restart `--watch` immediately after the push in the same turn.
- Do not run multiple concurrent `--watch` processes for the same PR/state file; keep one watcher session active and reuse it until it stops or you intentionally restart it.
- A push is not a terminal outcome; continue the monitoring loop unless a strict stop condition is met.

Commit message defaults:

- `codex: fix CI failure on PR #<n>`
- `codex: address PR review feedback (#<n>)`

## Monitoring Loop Pattern
Use this loop in a live Codex session:

1. Run `--once`.
2. Read `actions`.
3. First check whether the PR is now merged or otherwise closed; if so, report that terminal state and stop polling immediately.
4. Check CI summary, new review items, and mergeability/conflict status.
5. **Query ALL review threads via GraphQL** (see "Querying ALL Review Comments" above) to catch unresolved comments the watcher may have missed. Process any unresolved threads found.
6. Diagnose CI failures and classify branch-related vs flaky/unrelated.
7. Process actionable review comments before flaky reruns when both are present; if a review fix requires a commit, push it and skip rerunning failed checks on the old SHA. **After fixing, reply + resolve the thread when the fix is clearly complete** (bots autonomously; clear human fixes autonomously too, but note them for the end-of-session summary); only surface-and-ask when the fix isn't clear or the comment is a judgment call (see "Addressing Review Comments").
8. Retry failed checks only when `retry_failed_checks` is present and you are not about to replace the current SHA with a review/CI fix commit.
9. If you pushed a commit or triggered a rerun, report the action briefly and continue polling (do not stop).
10. After a review-fix push, proactively restart continuous monitoring (`--watch`) in the same turn unless a strict stop condition has already been reached.
11. If everything is passing, mergeable, not blocked on required review approval, and there are no unresolved threads at all, report success and stop. If the only thing left is threads you couldn't auto-resolve (unclear fix or judgment/discussion calls you've surfaced), **stop looping but report a user-action-required outcome, not success** — code is done + CI green, but those open threads still need the user's call before the PR can merge (see "Threads you couldn't auto-resolve"). Do not keep re-processing such a thread, and do not announce merge-ready while it's open. Clearly-correct fixes should already be resolved autonomously by this point.
12. If blocked on a user-help-required issue (infra outage, exhausted flaky retries, unclear reviewer request, permissions), report the blocker and stop.
13. Otherwise sleep according to the polling cadence below and repeat.

When the user explicitly asks to monitor/watch/babysit a PR, prefer `--watch` so polling continues autonomously in one command. Use repeated `--once` snapshots only for debugging, local testing, or when the user explicitly asks for a one-shot check.
Do not stop to ask the user whether to continue polling; continue autonomously until a strict stop condition is met or the user explicitly interrupts.
Do not hand control back to the user after a review-fix push just because a new SHA was created; restarting the watcher and re-entering the poll loop is part of the same babysitting task.
If a `--watch` process is still running and no strict stop condition has been reached, the babysitting task is still in progress; keep streaming/consuming watcher output instead of ending the turn.

## Polling Cadence
Use adaptive polling and continue monitoring even after CI turns green:

- While CI is not green (pending/running/queued or failing): poll every 1 minute.
- After CI turns green: start at every 1 minute, then back off exponentially when there is no change (for example 1m, 2m, 4m, 8m, 16m, 32m), capping at every 1 hour.
- Reset the green-state polling interval back to 1 minute whenever anything changes (new commit/SHA, check status changes, new review comments, mergeability changes, review decision changes).
- If CI stops being green again (new commit, rerun, or regression): return to 1-minute polling.
- If any poll shows the PR is merged or otherwise closed: stop polling immediately and report the terminal state.

## Stop Conditions (Strict)
Stop only when one of the following is true:

- PR merged or closed (stop as soon as a poll/snapshot confirms this).
- PR is ready to merge: CI succeeded, every thread you could resolve is resolved (clear bot *and* clear human fixes closed autonomously), no thread left open except ones you deliberately surfaced for the user's judgment, not blocked on required review approval, no merge conflict risk, **and AI reviewers (Codex, Devin) have posted their reviews** (wait at least 5 minutes after CI goes green for AI reviewer comments to arrive before declaring ready-to-merge). Any thread you intentionally left open for the user (unclear fix or a discussion) is a **user-action-required** terminal state (stop looping, report "needs your call to resolve"), not the merge-ready success condition (see "Threads you couldn't auto-resolve").
- User intervention is required and the agent cannot safely proceed alone — e.g. a new human review comment that needs judgment or discussion. (A *clear* human fix is handled autonomously per "Human vs Bot" — only ambiguous/judgment ones stop the loop.)

Keep polling when:

- `actions` contains only `idle` but checks are still pending.
- CI is still running/queued.
- Review state is quiet but CI is not terminal.
- CI is green but mergeability is unknown/pending.
- CI is green and mergeable, but the PR is still open and you are waiting for possible new review comments or merge-conflict changes per the green-state cadence.
- The PR is green but blocked on review approval (`REVIEW_REQUIRED` / similar); continue polling on the green-state cadence and surface any new review comments without asking for confirmation to keep watching.

## Output Expectations
Provide concise progress updates while monitoring and a final summary that includes:

- During long unchanged monitoring periods, avoid emitting a full update on every poll; summarize only status changes plus occasional heartbeat updates.
- Treat push confirmations, intermediate CI snapshots, and review-action updates as progress updates only; do not emit the final summary or end the babysitting session unless a strict stop condition is met.
- A user request to "monitor" is not satisfied by a couple of sample polls; remain in the loop until a strict stop condition or an explicit user interruption.
- A review-fix commit + push is not a completion event; immediately resume live monitoring (`--watch`) in the same turn and continue reporting progress updates.
- When CI first transitions to all green for the current SHA, emit a one-time celebratory progress update (do not repeat it on every green poll). Preferred style: `🚀 CI is all green! 33/33 passed. Still on watch for review approval.`
- Do not send the final summary while a watcher terminal is still running unless the watcher has emitted/confirmed a strict stop condition; otherwise continue with progress updates.
- **Always surface unresolved human review comments you did not auto-handle in every status update.** When any are present, lead with them under a `🟡 Needs your reply` (or similar) heading, listing each as a row: author, file:line (or "PR-level"), the full body (collapse if very long), and the comment URL. The user must see them as their action item. (Clear human fixes you *did* resolve autonomously go in the end-of-session summary instead — see "Human vs Bot".) Do not bury them at the bottom or summarize them out of existence.

- Final PR SHA
- CI status summary
- Mergeability / conflict status
- Fixes pushed
- Flaky retry cycles used
- Remaining unresolved failures or review comments

## References

- Heuristics and decision tree: [references/heuristics.md](references/heuristics.md)
- GitHub CLI/API details used by the watcher:
  [references/github-api-notes.md](references/github-api-notes.md)
