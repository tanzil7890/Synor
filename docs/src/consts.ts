// Public deployments must set SYNOR_DOCS_SITE_URL to an owned, verified
// origin. The localhost default keeps local canonical URLs truthful.
export const SITE_URL =
  (import.meta.env.SYNOR_DOCS_SITE_URL as string | undefined) ??
  'http://localhost:4321';

// `import.meta.env.BASE_URL` reflects `base` in astro.config.mjs.
export const DOCS_BASE = import.meta.env.BASE_URL.replace(/\/$/, '');
export const SITE_EXAMPLES = `${DOCS_BASE}/examples`;
export const pageUrl = (slug: string) =>
  new URL(`${DOCS_BASE}/${slug}/`, SITE_URL).toString();
export const pageMarkdownUrl = (slug: string) =>
  new URL(`${DOCS_BASE}/${slug}.md`, SITE_URL).toString();
export const LLMS_TXT_URL = new URL(
  `${DOCS_BASE}/llms.txt`,
  SITE_URL,
).toString();
export const LLMS_FULL_TXT_URL = new URL(
  `${DOCS_BASE}/llms-full.txt`,
  SITE_URL,
).toString();
export const SKILL_MD_URL = new URL(
  `${DOCS_BASE}/skill.md`,
  SITE_URL,
).toString();

export const docSlug = (id: string) => id.replace(/\/index$/, '');

const HTML_ESCAPES: Record<string, string> = {
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;',
};
const escapeHtml = (s: string) =>
  s.replace(/[&<>"']/g, (c) => HTML_ESCAPES[c]);

export const titleText = (s: string): string =>
  s.replace(/\*([^*]+)\*/g, '$1');

export const docTitle = (id: string, title: unknown): string =>
  titleText(
    typeof title === 'string' && title.length > 0 ? title : docSlug(id),
  );

export const titleMarkup = (s: string): string =>
  s.replace(
    /\*([^*]+)\*|([^*]+)/g,
    (_m, em, rest) =>
      em ? `<em>${escapeHtml(em)}</em>` : escapeHtml(rest),
  );

export const THEME_COLOR = '#f7f3ea';
