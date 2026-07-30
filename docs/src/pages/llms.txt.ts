// Generates /docs/llms.txt — a machine-readable index of the docs for LLMs and
// agents (see https://llmstxt.org/). Built from the same sidebar tree and
// per-page descriptions that drive the site, so it stays in sync automatically.
import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';
import {
  docSlug,
  pageUrl as url,
  pageMarkdownUrl as markdownUrl,
  LLMS_FULL_TXT_URL,
  SKILL_MD_URL,
} from '../consts';
import { sidebar, type SidebarDoc } from '../data/docs-sidebar';
import { EXAMPLE_CATALOG, EXAMPLE_CATALOG_GROUPS } from '../data/examples';
const oneLine = (s?: string) => (s ?? '').replace(/\s+/g, ' ').trim();

export const GET: APIRoute = async () => {
  const docs = await getCollection('docs');
  const desc = new Map<string, string>();
  for (const d of docs) desc.set(docSlug(d.id), oneLine(d.data.description));

  const line = (slug: string, label?: string) => {
    const d = desc.get(slug);
    return `- [${label ?? slug}](${url(slug)})${d ? `: ${d}` : ''}`;
  };

  const out: string[] = [
    '# Synor Docs',
    '',
    '> Synor keeps every derived file, row, and index aligned with the inputs ' +
      'that produced it. Stable work paths own declared outcomes, and the local ' +
      'engine reuses settled work.',
    '',
    '> Full docs text in one file: ' + LLMS_FULL_TXT_URL +
      `. Docs pages and example walkthroughs have raw Markdown twins by replacing the trailing slash with \`.md\`, e.g. ${markdownUrl('programming_guide/core_concepts')} — and they are all included in llms-full.txt.`,
    '',
    '> Coding agents: install the Synor skill before writing v1 code — ' +
      SKILL_MD_URL +
      ' (teaches the correct v1 API; without it, LLMs tend to hallucinate the deprecated v0 flow-builder DSL).',
    '',
  ];

  // Standalone top-level docs (Core Concepts, CLI, FAQ) surfaced first.
  const standalone = sidebar.filter((i): i is SidebarDoc => i.type === 'doc');
  if (standalone.length) {
    out.push('## Key pages');
    for (const d of standalone) out.push(line(d.slug, d.label));
    out.push('');
  }

  for (const item of sidebar) {
    if (item.type !== 'category') continue;
    out.push(`## ${item.label}`);
    if (item.slug) out.push(line(item.slug, item.label));
    for (const sub of item.items) {
      if (sub.type === 'doc') out.push(line(sub.slug, sub.label));
      else for (const s2 of sub.items) if (s2.type === 'doc') out.push(line(s2.slug, s2.label));
    }
    out.push('');
  }

  // Every runnable example in the local workspace.
  out.push('## Examples');
  out.push(
    `> ${EXAMPLE_CATALOG.length} runnable examples in the monorepo. ` +
      'From this workspace, `cd examples/<dir>`, copy `.env.example` if present, install its declared dependencies, and run the command shown for that example.',
  );
  out.push('');
  for (const group of EXAMPLE_CATALOG_GROUPS) {
    out.push(`### ${group.title}`);
    out.push(`> ${group.blurb}`);
    out.push('');
    for (const ex of group.entries) {
      const href = ex.docs ? url(`examples/${ex.docs}`) : url('examples');
      out.push(`- [${ex.title}](${href}): ${oneLine(ex.description)} (local path: examples/${ex.dir}; run: \`${ex.run ?? 'synor update main'}\`)`);
    }
    out.push('');
  }

  return new Response(out.join('\n'), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
};
