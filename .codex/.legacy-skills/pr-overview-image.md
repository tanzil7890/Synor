<!-- Preserved pre-Codex/Synor import. -->
---
name: pr-overview-image
description: >-
  Create a polished GPT image generation prompt for a pull request overview image.
  Use this whenever the user asks for a PR overview image, PR architecture image,
  visual PR summary, GPT image gen for a PR, or a raster diagram that explains
  what a PR does, how it is built, where code lives, runtime flow, trust
  boundaries, and commit-history narrative. Also use for large architecture PRs
  when a generated visual would help reviewers understand the change faster than
  prose or Mermaid alone.
last-verified: 2026-05-28
---

# PR Overview Image

Use this skill to produce a reviewer-friendly raster overview image for a PR.
The output is an image-generation brief, then a GPT image generation call.

## Workflow

1. Gather live PR context before prompting:
   - `TARGET=$(gh pr view --json baseRefName -q '.baseRefName' 2>/dev/null || echo "${CONDUCTOR_TARGET_BRANCH:-stg}")`
   - `git fetch origin "$TARGET"`
   - `git status --short --branch`
   - `git log --oneline --decorate --graph "origin/$TARGET..HEAD"`
   - `git diff --stat "origin/$TARGET...HEAD"`
   - `git diff --name-status "origin/$TARGET...HEAD"`
   - `gh pr view --json number,title,url,body,headRefName,baseRefName`
2. Read the PR body, commit subjects, and the key files named by the diff.
3. Identify the few things a reviewer most needs to understand. Examples:
   runtime flow, data ownership, trust boundaries, state transitions, rollout
   shape, deleted/replaced systems, code map, or migration path.
4. Decide whether to make one image or multiple images:
   - One image when the PR has one clear story.
   - Multiple images when different views matter, such as runtime flow plus
     repo map, before/after architecture plus migration steps, or security
     boundary plus rollout plan.
5. Generate the image. The path depends on which runtime you are:

   **If you're Codex** — call the built-in `image_gen` tool directly, then
   move the output into `.context/generated_images/` with a descriptive
   versioned filename. You can do everything in this skill end-to-end.

   **If you're Claude** (no native image_gen tool) — delegate the generation
   to Codex via `codex exec`, passing a self-contained prompt that includes
   the PR context, the image brief, and instructions to save to
   `.context/generated_images/<descriptive-name>.png`. Example:
   ```bash
   codex exec --sandbox workspace-write < /tmp/codex-image-prompt.txt
   ```
   The prompt should include all PR context Codex needs (it won't see your
   conversation history). After Codex returns, the image will be at the path
   you specified.

6. Embed the image in the PR description. Again, path depends on runtime:

   **If you're Codex** — use the bundled helper:
   ```bash
   .agents/skills/pr-overview-image/scripts/upload-images-to-pr-body.sh \
     --pr <number> \
     .context/generated_images/<image>.png
   ```
   It uses `agent-browser --auto-connect` against a logged-in GitHub browser
   to upload through GitHub's normal attachment flow, then updates a marked
   PR body section with the resulting `github.com/user-attachments` URLs.

   **If you're Claude** — the bundled helper often fails for you because:
   (a) `agent-browser --auto-connect` requires Chrome on a remote-debugging
   port that your sandbox may not have available, and (b) the helper's file
   input selector `form.js-new-comment-form input[type=file], input[type=file]`
   greedily picks the first `<input type=file>`, which on GitHub PR pages is
   the edit-comment input, not the new-comment composer. Use this manual
   flow instead:

   ```bash
   # 1. Verify agent-browser has a Chrome session and is logged into GitHub
   agent-browser --auto-connect get cdp-url      # should print ws://...
   agent-browser --auto-connect eval 'document.querySelector("meta[name=user-login]")?.content'

   # 2. Navigate to the PR
   agent-browser --auto-connect open "https://github.com/<org>/<repo>/pull/<n>" && \
     agent-browser --auto-connect wait --load networkidle

   # 3. Upload to the CORRECT input (new-comment composer)
   agent-browser --auto-connect upload '#fc-new_comment_field' "$(pwd)/<image>.png"

   # 4. Poll the composer textarea until GitHub auto-injects the user-attachments URL
   for i in $(seq 1 15); do
     val=$(agent-browser --auto-connect eval --stdin <<'EOF'
   Array.from(document.querySelectorAll('form.js-new-comment-form textarea')).map(t=>t.value).find(Boolean) || ''
   EOF
   )
     echo "$val" | grep -q 'user-attachments/assets/' && break
     sleep 2
   done

   # 5. Extract the URL and clear the composer (don't post a stray comment)
   URL=$(echo "$val" | grep -oE 'https://github.com/user-attachments/assets/[a-f0-9-]+' | head -1)
   agent-browser --auto-connect eval --stdin <<'EOF' >/dev/null
   for (const t of document.querySelectorAll('form.js-new-comment-form textarea')) {
     t.value=''; t.dispatchEvent(new Event('input',{bubbles:true}));
   }
   EOF

   # 6. Embed in the PR body inside the marked block
   gh pr view <n> --json body -q '.body' > /tmp/body.md
   {
     echo "<!-- pr-overview-images:start -->"
     echo
     echo "## PR overview"
     echo
     echo "<img alt=\"<descriptive-alt>\" src=\"$URL\" />"
     echo
     echo "<!-- pr-overview-images:end -->"
     echo
     cat /tmp/body.md
   } > /tmp/body-new.md
   gh pr edit <n> --body-file /tmp/body-new.md
   ```

   Keep the `<!-- pr-overview-images:start --><!-- pr-overview-images:end -->`
   markers — future runs of the helper script will overwrite that block
   cleanly.

   Do not use draft release assets for inline PR images; GitHub renders them
   as broken images in PR descriptions.

## Prompt Shape

Use a concise, concrete prompt. Prefer a polished product-architecture
infographic over a dense screenshot-like diagram. Keep the prompt intentionally
flexible: describe what matters most for the PR, then let the image decide the
best composition.

Include:
- compact PR number/title metadata, not a dominant hero header
- a short outcome label if it helps orient the image
- the most important reviewer mental model for this PR
- real services, packages, routes, tables, or files only when they clarify that
  mental model
- trust or secret boundaries when they materially affect the change
- optional small generated timestamp annotation, only if useful
- a short commit-history arc only when the PR changed direction and that context
  explains the final shape

Avoid:
- wasting space on a large title block, branch/base strip, commit hash, or other
  metadata that does not explain the PR
- tiny bullet lists that will be unreadable
- more than 18 visible file paths in one image
- invented file paths, fake check names, or fake service names
- raw secret names unless they already appear in non-secret config/docs
- code snippets
- CI state, review state, mergeability, or temporary PR blockers unless the user
  explicitly asks for operational status in the image
- decorative gradients, dark bokeh, or stock-photo backgrounds

## Visual Style

Default style:
- 16:9 technical product infographic.
- White or very light warm-gray canvas with two or three strong panels.
- Clear editorial hierarchy where the diagram carries the page; keep title and
  metadata small.
- Flat vector-like cards, thin borders, subtle shadows, 8px corner radius.
- Use semantic colors consistently:
  - blue: Trifetch app/API/control plane
  - green: external provider or customer data plane
  - orange: infrastructure/Pulumi
  - purple: agent/workflow access
  - red: trust boundary, no-secret warning, or blocker
- Use crisp icons only when they simplify recognition.
- Make arrows thick enough to read, with short labels.
- Use 3-5 words per node where possible.
- Prefer grouped boxes and lane labels over many small bullets.
- Use the "two major panels plus tiny metadata annotation" composition only when it
  fits the PR. Other valid layouts include before/after, swimlanes, lifecycle
  timelines, layered maps, or two separate images.

## High-Signal Lens Prompts

When a PR has both code structure changes and user-facing workflow changes,
generate two complementary images before trying niche runtime or reliability
lenses. These two prompts have worked well because they give reviewers the
mental model first, then the actual user journey.

### Lens 1: Architecture Consolidation

Use this for PRs that collapse duplicated systems into one shared primitive,
replace wrappers with composition, or make two product areas share a behavior
contract. Keep the "before" panel honest about duplication and keep the "after"
panel centered on the new ownership boundary.

```text
Create a polished 16:9 PR overview infographic for reviewers.

Use case: infographic-diagram
Asset type: pull request overview image
Style: clean editorial technical infographic, white or very light gray
background, flat vector cards, thin borders, subtle shadows, 8px corner radius,
crisp readable labels, generous whitespace. Use semantic colors: blue for
Trifetch app/API, purple for agent UI, green for file/data plane, red only for
duplicate or removed complexity. No decorative blobs, no dark theme, no code
snippets.

Small metadata annotation in the corner, not a big hero:
PR #[number]: [short title]
Lens: architecture consolidation

Main composition: before/after architecture.

Left panel labeled BEFORE: two parallel stacks.
- [old stack A]
- [old stack B]
- duplicated [behavior 1]
- duplicated [behavior 2]
- divergent [user-visible behavior]
Show red dashed duplicate lines between the two stacks.

Right panel labeled AFTER: one shared primitive with product-specific slots.
Central component: [new shared primitive]
Connected slot cards: [slot 1], [slot 2], [slot 3], [slot 4].
Two consumers above/below: [consumer A] mounts [full behavior]; [consumer B]
mounts [slim or specialized behavior].
Add a small shared artifact row: [shared component 1] + [shared component 2].

Bottom outcome strip: one behavior contract, fewer divergent bugs, reused
product polish.

Text constraints: keep labels large, short, and legible; use at most 5 words
per node where possible; do not invent file paths; do not include commit hashes
or CI status.
```

### Lens 2: Product Workflow

Use this for PRs that introduce a new shell, full-page route, workspace, editor,
sidebar, wizard, or multi-step product flow. This image should feel like a
simplified app surface, not a code map. Show the user's movement through the
surface and the state that follows them.

```text
Create a polished 16:9 PR overview infographic for reviewers.

Use case: infographic-diagram
Asset type: pull request overview image
Style: clean editorial technical infographic, light warm-gray canvas, flat
vector UI panels, thin borders, subtle shadows, 8px corner radius, crisp
readable labels, no decorative gradients, no stock imagery, no code snippets.

Small metadata annotation in the corner:
PR #[number]: [short title]
Lens: product workflow

Main composition: a large simplified application layout diagram.

Left pane: [primary action pane].
Labels: [session or navigation control], [history or mode control], [main
interaction component], [shared input or action surface].

Center pane: [workspace or editor pane].
Labels: [tab behavior], [viewer or editor], [read/write mode], [state that
persists across refresh or context switches].

Right pane: [supporting resource pane].
Labels: [tree/list component], [resource group 1], [resource group 2],
[resource group 3], [create/upload/action], [destructive action boundary].

Show user flow arrows:
1. [entry point action] -> [route or state transition] -> [workspace result].
2. [resource click] -> [direct workspace result].
3. [context switch] -> [state restoration].
4. [escape/minimize action] -> [single active surface].

Add a small callout: [one product outcome, such as "one workspace, one live
stream, visible context"].

Text constraints: keep text large and readable; avoid tiny paragraphs; include
only the few paths or route names that clarify the flow; do not include branch
names, commit hashes, CI state, or fake screenshots.
```

## Prompt Template

```text
Create one polished PR overview infographic. Generate multiple images if one
image would become cramped or hide the important story.

Use case: infographic-diagram
Asset type: pull request overview image for reviewers
Style: clean editorial technical infographic, white/light-gray background, flat vector cards, thin borders, subtle shadows, generous whitespace, crisp readable labels, no decorative blobs, no dark theme.

Small annotation, not a large header:
- PR #[number]: [short title]
- Generated: [timestamp] (optional)

Outcome label: [one short sentence, optional]

Show whatever matters most for this PR:
- [primary reviewer mental model]
- [secondary supporting view, if useful]
- [trust/secret boundary, if important]
- [code locations, only the most useful real paths]

Text constraints:
- Keep text large and readable.
- Use at most 18 file paths total.
- Do not invent paths or statuses.
- Keep PR metadata minimal and visually small. Do not include commit hash,
  branch/base, or a metadata strip unless the user explicitly asks for it.
- Do not include CI state, review state, mergeability, or temporary blockers
  unless explicitly requested.
- Do not include code snippets.
- Use exact capitalization for product names and file paths.
```

## Quality Bar

Before accepting the image, inspect it for:
- Text is legible at normal PR-body size.
- The data flow is directionally correct.
- Trust boundaries do not imply secrets enter sandboxes if they do not.
- File paths are real and not hallucinated.
- Any commit-history arc is simple and matches `git log`.
- Header/title/metadata occupy little space compared to the diagram.
- Visual density is lower than a Mermaid diagram with the same information.

If the generated image has garbled labels, generate one targeted revision with
fewer labels and fewer paths rather than trying to cram in more text.
