---
name: bug-report
description: Draft or file a high-quality bug report in the connected issue tracker, usually Linear. Use when the user asks to file, create, log, report, or draft a bug ticket from symptoms, screenshots, errors, or expected-versus-observed behavior. Search for duplicates when practical and perform a code-level root-cause pass only when requested or clearly useful.
---

# Bug Report

Turn the evidence already provided into an actionable ticket. Do not make the user repeat information that can be inferred safely.

## Intent

- If the user says **draft**, produce a filing-ready draft without creating anything.
- If the user says **file**, **create**, **log**, or **report**, create the issue through the connected tracker.
- If no tracker is connected, return the complete draft and identify the missing connection.

Use the installed Linear workflow or connector when Linear is the target. Do not hard-code MCP tool names, workspace IDs, teams, projects, labels, or ticket prefixes from another repository.

## Gather evidence

Extract:

- concise observed behavior
- expected behavior
- reproduction steps
- environment, version, URL, or commit when known
- frequency and impact
- logs, stack traces, screenshots, recordings, or related links
- first observed time and regression context when known

Ask at most the minimum blocking question. Unknown non-blocking details belong in an **Unknowns** section.

## Duplicates and regressions

Search open and recently closed issues using distinctive symptoms, errors, affected components, and related identifiers.

- If an active duplicate exists, report it instead of creating another unless the user wants a separate issue.
- If a matching fixed issue exists and the behavior returned, label the new report as a likely regression and link the earlier issue.
- Do not claim a duplicate from title similarity alone.

## Root-cause pass

When requested or clearly useful:

1. Read `AGENTS.md`.
2. Trace the user-visible entry point to the failing boundary.
3. Use `ccc search` for the concept when available and `rg` for exact symbols. Fall back to `rg` and directory inspection when `ccc` is unavailable.
4. Cite concrete file and line evidence.
5. Separate confirmed cause, likely cause, and unanswered questions.

Do not delay a useful bug report for speculative root-cause research.

## Ticket shape

Use:

```markdown
## Summary
[One clear paragraph]

## Observed behavior
[What happens]

## Expected behavior
[What should happen]

## Reproduction
1. ...

## Environment
- Version/commit:
- Platform:
- Configuration:

## Impact
[Who or what is blocked, severity, and frequency]

## Evidence
[Logs, screenshots, links, related issues]

## Root cause
[Confirmed or likely cause with file references, if investigated]

## Suggested verification
[Concrete checks that prove the fix]

## Unknowns
[Only unresolved facts]
```

Choose team, project, labels, and priority from connected workspace conventions and issue evidence. If more than one destination is plausible and the choice materially changes ownership, ask before filing.

## Safety

- Never include secrets, credentials, private customer data, or unnecessary personal information.
- For a suspected security vulnerability or data exposure, stop before creating a broadly visible ticket and ask for the approved private reporting path.
- Attach user-provided files only when the user asked to file the issue and the tracker supports attachments.
- Return the created issue identifier and link, or clearly state that only a draft was produced.
