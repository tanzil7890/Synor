---
name: pr-overview-image
description: Create a polished raster overview image for a pull request from its real diff, code paths, runtime flow, state ownership, and trust boundaries. Use when the user asks for a PR overview image, visual PR summary, PR architecture graphic, or generated infographic for reviewers.
---

# PR Overview Image

Create a reviewer-friendly image grounded in the actual pull request. Use the `imagegen` skill and image-generation tool for the raster asset.

## Gather context

1. Read `AGENTS.md`.
2. Read PR metadata and body through the connected GitHub capability or `gh`.
3. Resolve the real base branch from the PR or remote default branch.
4. Inspect the commit history, diff stat, changed paths, and the small set of source files that define the change.
5. Identify the one mental model a reviewer needs most: runtime flow, state ownership, before/after architecture, migration, code map, or trust boundary.

If Git or PR context is unavailable, ask for the PR URL or create the image from the supplied artifacts with the limitation stated.

## Choose the composition

- Use one image for one coherent story.
- Use two complementary images only when a single image would obscure separate concerns, such as runtime flow and repository map.
- Prefer a small diagram with legible labels over a dense inventory.
- Use real component, API, connector, table, and file names. Do not invent paths or statuses.

For Synor, useful semantic colors are:

- blue: public Python API and processing components
- orange: Rust core and execution engine
- purple: state, change detection, and component ownership
- green: connectors and external systems
- red: failure, deletion, or trust boundary only

## Generate

Create a concise image prompt containing:

- PR number and short title as a small annotation
- the reviewer mental model
- exact nodes and directional relationships
- a short outcome label
- the visual style and text-density constraints

Default style:

- 16:9 clean technical infographic
- white or light warm-gray canvas
- flat cards, thin borders, subtle shadows, generous whitespace
- large readable labels, usually no more than five words per node
- clear arrows with short verbs
- no code snippets, fake screenshots, decorative bokeh, or dominant title block

Use the image-generation result directly. If the host returns a local file path, place a copy under `.context/generated_images/` with a descriptive filename. If it returns only an in-conversation asset, deliver that asset rather than inventing a path.

## Prompt template

```text
Create a polished 16:9 pull-request overview infographic for reviewers.

Small metadata annotation:
PR #[number]: [short title]

Reviewer mental model:
[one sentence]

Show:
- [real component] -> [real component]: [relationship]
- [state owner] -> [external effect]: [relationship]
- [before/after or failure boundary when relevant]

Outcome:
[one short sentence]

Style: light editorial technical infographic, flat vector-like cards, thin
borders, subtle shadows, generous whitespace, large crisp labels, and clear
directional arrows. Use blue for Python/API, orange for Rust/core, purple for
state and ownership, green for connectors/external systems, and red sparingly
for failures or trust boundaries.

Do not invent file paths, components, CI state, review state, or mergeability.
Do not include commit hashes or code snippets. Keep labels readable at normal
GitHub PR-body size.
```

## Inspect

Before accepting:

- labels are legible and not garbled
- flow direction matches the code
- ownership and cleanup boundaries are correct
- file paths and component names are real
- the image does not imply unverified security or reliability properties
- the diagram occupies more space than metadata

Generate one targeted revision with fewer labels if needed.

## Attach only when asked

Creating an image does not automatically authorize editing the PR body. If the user explicitly asks to attach it and a local image file plus authenticated `gh` and `agent-browser` are available, run:

```bash
<skill-dir>/scripts/upload-images-to-pr-body.sh --pr <number> <image-path>
```

Otherwise return the image and a suggested alt text. Do not create release assets or other public hosting as a workaround.
