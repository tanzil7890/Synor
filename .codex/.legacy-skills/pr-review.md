<!-- Preserved pre-Codex/Synor import. -->
---
name: pr-review
description: |
  Comprehensive PR review skill that kicks off 5 parallel opus subagents (role-prompted as Staff Engineers)
  to review code changes across: simplicity, patterns, security, production readiness, and TEST QUALITY.
  The test quality review answers the critical question: "How confident should we be that this code is
  correct and won't break existing functionality?" Use when creating a PR, updating a PR with new changes,
  or when user asks to "review PR", "check my changes", "is this ready to merge?", or "/pr-review".
author: Claude Code
version: 1.0.0
allowed-tools:
  - Read
  - Write
  - Edit
  - Grep
  - Glob
  - Bash
  - Task
  - AskUserQuestion
last-verified: 2026-05-28
---

# PR Review Skill

Comprehensive, multi-dimensional PR review using 5 parallel opus subagents role-prompted as Staff Engineers.

## Unified Workflow with `pull-request` skill

**The `pull-request` skill automatically invokes this skill after PR creation.**

Use `pull-request` skill for the full workflow:
1. Creates/updates the PR with proper description
2. Runs this review automatically
3. Reports combined results

Use `pr-review` standalone when:
- PR already exists and you want to re-review after changes
- User asks: "review PR", "is this ready?", "check my changes"
- Before requesting human review on an existing PR

## Workflow

### Phase 1: Run CI Checks Locally

First, ensure all checks pass before wasting reviewer time:

```bash
# Run in parallel
pnpm typecheck &
pnpm lint &
pnpm run test &
wait

# Check for lint ratchet if it exists
pnpm lint:ratchet 2>/dev/null || true
```

**If any check fails, fix first before continuing.**

### Phase 2: Analyze the PR Scope

```bash
# Determine target branch: use existing PR base if available, otherwise use the
# workspace target from Conductor/system instructions or CONDUCTOR_TARGET_BRANCH; fall back to stg.
TARGET=$(gh pr view --json baseRefName -q '.baseRefName' 2>/dev/null || echo "")
if [ -z "$TARGET" ]; then
  TARGET="${CONDUCTOR_TARGET_BRANCH:-stg}"
fi

# ALWAYS fetch latest from origin to avoid diffing against stale local branch
git fetch origin "$TARGET"

# Get PR context against REMOTE target branch
git log --oneline "origin/$TARGET..HEAD"
git diff --stat "origin/$TARGET...HEAD"

# Check if PR exists
gh pr view 2>/dev/null || echo "No PR yet"
```

**CRITICAL: Always diff against `origin/<target>`, never the local branch — local branches go stale.**

Assess:
- How many files changed?
- How many insertions/deletions?
- Which areas of codebase are affected?
- Is this a single feature or multiple concerns?

### Phase 3: Launch Parallel Opus Subagents

**CRITICAL: Launch ALL 5 subagents in parallel using a single message with multiple Task tool calls.**

**IMPORTANT: Use `model: "opus"` and `subagent_type: "general-agent-opus"` for ALL subagents.**

Each subagent is role-prompted as a **Staff Engineer** and gets the full diff context to review a specific dimension:

#### Subagent 1: Simplicity Review

```
You are a Staff Engineer conducting a code review. Your standards are high. You've seen
codebases rot from unnecessary complexity and you won't let it happen here.

Review this PR for SIMPLICITY. Determine the target branch with: `TARGET=$(gh pr view --json baseRefName -q '.baseRefName' 2>/dev/null || echo "${CONDUCTOR_TARGET_BRANCH:-stg}")` then `git fetch origin "$TARGET"` and run `git diff "origin/$TARGET...HEAD"` to see changes.

Focus on:
1. Is the code as simple as possible for what it does?
2. Are there unnecessary abstractions or over-engineering?
3. Could complex logic be simplified?
4. Are there redundant code paths or duplicated logic?
5. Is the overall design the simplest approach?
6. Would a junior engineer understand this in 6 months?

Be direct. If something is over-engineered, say so. Provide specific file:line references.
```

#### Subagent 2: Patterns & Best Practices Review

```
You are a Staff Engineer conducting a code review. You maintain consistency across the
codebase and ensure patterns are followed. You've debugged too many issues caused by
inconsistent code to let sloppy patterns slip through.

Review this PR for CODING PATTERNS and BEST PRACTICES. Determine the target branch with: `TARGET=$(gh pr view --json baseRefName -q '.baseRefName' 2>/dev/null || echo "${CONDUCTOR_TARGET_BRANCH:-stg}")` then `git fetch origin "$TARGET"` and run `git diff "origin/$TARGET...HEAD"`.

Focus on:
1. Does code follow existing codebase patterns?
2. Consistent naming conventions?
3. Comprehensive error handling?
4. Proper TypeScript types (no any, proper generics)?
5. Consistent and useful logging?
6. Magic numbers/strings that should be constants?
7. Async/await correctness (no floating promises)?

Cross-reference with existing patterns in the codebase. Provide file:line references.
```

#### Subagent 3: Security Review

```
You are a Staff Engineer with a security focus. You assume every input is malicious,
every external call can fail, and every secret can leak. You've seen breaches happen
from "minor" oversights.

Review this PR for SECURITY. Determine the target branch with: `TARGET=$(gh pr view --json baseRefName -q '.baseRefName' 2>/dev/null || echo "${CONDUCTOR_TARGET_BRANCH:-stg}")` then `git fetch origin "$TARGET"` and run `git diff "origin/$TARGET...HEAD"`.

Focus on:
1. Input validation - all inputs validated?
2. Auth/authz - endpoints protected?
3. Secrets handling - any secrets exposed/logged?
4. Injection attacks - SQL, command, path traversal, SSRF?
5. If running user code - isolation and sandboxing?
6. Permissions - S3, IAM, K8s RBAC appropriate?
7. Env vars - sensitive data exposure?

Be paranoid. Provide specific file:line references for concerns.
```

#### Subagent 4: Production Readiness Review

```
You are a Staff Engineer who's been paged at 3am too many times. You review code
through the lens of "what will break in production and wake me up?"

Review this PR for PRODUCTION READINESS. Determine the target branch with: `TARGET=$(gh pr view --json baseRefName -q '.baseRefName' 2>/dev/null || echo "${CONDUCTOR_TARGET_BRANCH:-stg}")` then `git fetch origin "$TARGET"` and run `git diff "origin/$TARGET...HEAD"`.

Focus on:
1. Error handling and recovery - does it fail gracefully?
2. Logging and observability - can we debug this in prod?
3. Performance implications - will this scale?
4. Graceful degradation under load
5. Database migrations (if any) - reversible? Safe?
6. Feature flags for gradual rollout?
7. Monitoring/alerting needs

Think about failure modes. Provide specific file:line references.
```

#### Subagent 5: Test Quality Review

```
You are a Staff Engineer who believes tests are the contract that keeps code honest.
You've seen PRs with "100% coverage" that test nothing meaningful, and PRs with
minimal tests that catch every bug. Coverage numbers lie. Test quality doesn't.

Review this PR's TESTS. Determine the target branch with: `TARGET=$(gh pr view --json baseRefName -q '.baseRefName' 2>/dev/null || echo "${CONDUCTOR_TARGET_BRANCH:-stg}")` then `git fetch origin "$TARGET"` and run `git diff "origin/$TARGET...HEAD"` to see the changes, then examine
the test files in detail.

Your job is to answer ONE critical question: **How confident should we be that this
code is correct and won't break existing functionality?**

Analyze:
1. **Core Logic Coverage**: Do tests actually exercise the core business logic, or
   just the happy path? Identify the 2-3 most critical code paths and verify they're tested.

2. **Edge Cases**: Are boundary conditions tested? Empty inputs, nulls, max values,
   concurrent access, error conditions?

3. **Regression Protection**: If someone refactors this code in 6 months, will these
   tests catch breakages? Or are they testing implementation details that will break
   on any change?

4. **Integration Points**: For code that touches external systems (DB, APIs, K8s),
   are the integration points tested or mocked appropriately?

5. **What's NOT Tested**: Explicitly call out any new code paths that have NO test
   coverage. This is often more valuable than what IS tested.

6. **Test Quality Red Flags**:
   - Tests that just check "it doesn't throw"
   - Tests that mock everything including the thing being tested
   - Tests with no assertions or trivial assertions
   - Tests that duplicate each other
   - Tests that are flaky or timing-dependent

Output format:
- **Confidence Level**: HIGH / MEDIUM / LOW with explanation
- **Critical Gaps**: List untested code paths that concern you
- **Recommendations**: Specific tests that should be added (with code sketches if helpful)

Be honest. If tests are weak, say so. A PR with honest "MEDIUM confidence" is better
than false confidence that breaks prod.
```

### Phase 4: Synthesize Review Results

After all subagents complete, synthesize findings into a cohesive review:

```markdown
## PR Review Summary

### Status: [READY / NEEDS WORK / BLOCKED]

### CI Checks
- [ ] Typecheck: PASS/FAIL
- [ ] Lint: PASS/FAIL
- [ ] Tests: PASS/FAIL
- [ ] Lint Ratchet: PASS/FAIL/N/A

### Test Confidence: HIGH / MEDIUM / LOW
[Summary from Test Quality subagent - this is critical context for human reviewer]

### Key Review Areas (for human reviewer)

**High Priority (must review):**
- `path/to/file.ts:L50-100` - [reason]
- `path/to/other.ts:L20` - [reason]

**Medium Priority (should review):**
- ...

### Findings by Category

#### Simplicity
[Synthesized findings from subagent 1]

#### Patterns & Best Practices
[Synthesized findings from subagent 2]

#### Security
[Synthesized findings from subagent 3]

#### Production Readiness
[Synthesized findings from subagent 4]

#### Test Quality
[Synthesized findings from subagent 5 - include confidence level and critical gaps]

### Issues Found

**Blockers (must fix before merge):**
- [ ] Issue 1 at `file:line`

**Should Fix:**
- [ ] Issue 2 at `file:line`

**Nice to Have:**
- [ ] Issue 3 at `file:line`

### PR Description Quality

- [ ] Title is clear and follows conventional commits
- [ ] Summary explains the "why" not just "what"
- [ ] Test plan is adequate
- [ ] Breaking changes documented
- [ ] Migration steps if needed
```

### Phase 5: PR Description Check

If a PR exists, verify the description is reviewer-friendly:

```bash
gh pr view --json title,body | jq -r '.body'
```

A good PR description should:
1. **Summary**: 1-3 bullet points explaining what and why
2. **Key areas to review**: Explicitly flag which files/functions need careful review
3. **Test plan**: How to verify the changes work
4. **Screenshots/video**: For UI changes or complex flows

If missing, suggest improvements.

### Phase 6: Demo Video Prompt

For PRs with:
- UI changes
- Complex user flows
- Multi-step interactions
- Non-obvious functionality

**Prompt the user:**

```
Consider recording a demo video showing:
- [ ] The happy path working
- [ ] Any edge cases handled
- [ ] Before/after comparison (if refactoring)

Add `[VIDEO PLACEHOLDER]` to PR description if not ready yet.
```

## Output Format

After review, output a structured summary like:

```
## PR Review: [branch-name]

### Checks: ALL PASSING / X FAILING

### Test Confidence: HIGH / MEDIUM / LOW
[One-line summary of why - e.g., "Core retry logic thoroughly tested, but no integration tests for K8s client"]

### Review Status: READY / NEEDS WORK

### Key Areas for Human Review:
1. `file.ts:L50-100` - Complex business logic
2. `service.ts:L200` - New external API call

### Issues Found:
- **BLOCKER**: [description] at `file:line`
- **Should Fix**: [description] at `file:line`

### Test Gaps (if any):
- [Untested code path] at `file:line`

### PR Description: GOOD / NEEDS IMPROVEMENT
[If needs improvement, list what to add]

### Video: RECOMMENDED / NOT NEEDED
[If recommended, list what to show]
```

## Inline Comment Style (MANDATORY)

When posting inline PR comments via `gh` CLI, **always use this two-part format**:

### Part 1: Human-readable comment (top)
- Written casually, as if the user typed it themselves
- Tag the PR author with `@github-handle`
- Ask questions, challenge decisions, request changes in natural language
- Lowercase, conversational, direct — no corporate speak

### Part 2: Agent analysis (bottom, collapsible)
- Separated by `---` horizontal rule
- Wrapped in `<details><summary>🤖 <b>Agent analysis</b> (click to expand)</summary>`
- Contains thorough technical analysis:
  - Exact `file:line` references
  - Comparison tables when showing duplication or pattern differences
  - **Full code examples** for proposed fixes (not pseudocode)
  - Multiple options with tradeoffs when applicable
  - Severity/priority assessment
- Ends with `</details>`

### Example

```markdown
@author-handle hey — can you walk me through why we need a separate service here?
could this just be a route on the existing API behind service auth?

---

<details>
<summary>🤖 <b>Agent analysis</b> (click to expand)</summary>

### What the new service does

The entire service is **one endpoint**: `GET /v1/configurations/:serviceId/:environment`.
[detailed analysis...]

### Proposed fix

```typescript
// Add to existing API instead
export const configurationsRouter = router({
  get: serviceProtectedProcedure
    .input(z.object({ ... }))
    .query(async ({ ctx, input }) => { ... }),
});
```

</details>
```

### Posting comments via gh CLI

Batch all inline comments into a single review using the reviews API:

```bash
gh api repos/OWNER/REPO/pulls/PULL_NUMBER/reviews \
  --method POST \
  --input - <<'JSON'
{
  "commit_id": "COMMIT_SHA",
  "event": "COMMENT",
  "body": "",
  "comments": [
    {
      "path": "path/to/file.ts",
      "line": 42,
      "side": "RIGHT",
      "body": "human comment\n\n---\n\n<details>..."
    }
  ]
}
JSON
```

To update an existing comment: `gh api repos/OWNER/REPO/pulls/comments/COMMENT_ID --method PATCH -f body="..."`

**Note:** The update endpoint is `pulls/comments/ID`, NOT `pulls/PULL_NUMBER/comments/ID`.

## Anti-Patterns

**DON'T:**
- Skip subagent reviews for "small" PRs (bugs hide in small changes)
- Merge without all checks passing
- Create PRs without test plans
- Skip security review for "internal" code
- Accept "100% coverage" as proof of quality (coverage lies, test quality doesn't)
- Use non-opus subagents (these reviews require deep reasoning)

**DO:**
- Run all 5 opus subagents in parallel (faster, smarter)
- Be specific with file:line references
- Prioritize findings (blocker vs nice-to-have)
- Flag areas needing human judgment
- Prompt for videos on UI/UX changes
- Report honest test confidence levels (MEDIUM is fine if it's true)

## Integration

Use alongside:
- `plan-with-docs` - Planning before implementation
- `testing` - Deep dive into test setup/patterns if test quality subagent flags major gaps
- `security` - Deep security audit if security subagent flags concerns
- `observability` - Verify logging/metrics patterns
