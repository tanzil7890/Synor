// Serves the Synor agent skill (skills/synor/SKILL.md) at /docs/skill.md
// so agents and users can fetch one file instead of cloning the repo — the
// Mintlify-style hosted skill.md. Served verbatim so it stays a valid SKILL.md
// (frontmatter intact); a source pointer is appended at the end. The skill's
// references/ are hosted alongside at /docs/references/<name>.md, so the
// relative links in the body resolve when fetched over HTTP.
import type { APIRoute } from 'astro';
import { readSkillFile } from '../lib/skill-files';

export const GET: APIRoute = () => {
  const skill = readSkillFile('SKILL.md').trimEnd();
  const note =
    `\n\n---\n\n> Generated from the local skills/synor/SKILL.md file. ` +
    `Its references/ are available at references/<name>.md relative to this URL.\n`;
  return new Response(skill + note, {
    headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
  });
};
