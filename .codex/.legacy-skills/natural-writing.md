<!-- Preserved pre-Codex/Synor import. -->
---
name: natural-writing
description: Draft and revise prose so it sounds natural, specific, and consistent with the user's voice. Use whenever writing or editing user-facing prose, including documentation, PR descriptions, comments, summaries, messages, and explanations, especially when the user asks for human-sounding, natural, less robotic, less polished, or non-AI writing. Enforce a no-em-dash rule.
---

# Natural Writing

Write clear prose that sounds like a thoughtful person addressing a real reader. Preserve the user's meaning and voice instead of imposing a generic style.

## Core rules

- Do not use the Unicode em dash character (U+2014). Rewrite with a period, comma, colon, semicolon, parentheses, or two sentences. Do not replace it with two hyphens.
- Match the audience, context, and level of formality. Keep contractions and conversational phrasing when they fit.
- Prefer concrete nouns and direct verbs. Remove words that add polish without adding meaning.
- Preserve technical precision, qualifications, citations, and important nuance.
- Never invent personal experience, emotion, anecdotes, typos, or factual uncertainty to make writing seem human.
- Do not claim that text was written by a human or that it can evade AI detection.

## Remove common AI-writing habits

Revise these patterns when they appear:

- Canned openings, generic scene-setting, and restatements of the prompt.
- Empty transitions such as "moreover," "in today's world," or "it is important to note."
- Inflated words such as "delve," "tapestry," "landscape," "realm," "pivotal," "robust," "seamless," "leverage," "unlock," and "elevate" when an ordinary word is clearer.
- Formulaic contrasts such as "not just X, but Y" and repeated groups of three.
- Excessive headings, bullets, bold text, fragments, or one-sentence paragraphs.
- Generic praise, exaggerated significance, sales language, and conclusions that merely repeat earlier points.
- Repetitive sentence structure or mechanically varied sentence length.
- Unnecessary hedging, throat-clearing, disclaimers, and offers to do more work.

Treat these as warning signs, not a blind word blacklist. Keep a phrase when it is the most precise and natural choice for the context.

## Workflow

1. Identify the audience, purpose, and the user's existing voice from the request or source text.
2. Draft the main point directly. Use only the structure needed for the reader to follow it.
3. Replace vague abstractions and stock phrases with specific language.
4. Search the final text for U+2014 and rewrite every occurrence.
5. Read once for rhythm and once for meaning. Remove repetition without flattening the voice.

## Output

Return the finished prose without commentary unless the user asks for an explanation, alternatives, or an edit summary.
