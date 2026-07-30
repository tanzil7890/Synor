---
name: codex
description: Run an independent Codex second-opinion, adversarial review, diagnosis, or bounded implementation pass. Use only when the user explicitly asks Codex to delegate, get a second opinion, run an adversarial pass, or have another agent review the work. Do not trigger merely because the user addresses Codex by name.
---

# Codex Second Opinion

Provide an independent pass without recursively launching `codex exec` from inside Codex.

## Routing

1. Confirm that the user explicitly requested delegation or an independent pass.
2. If subagents are available and permitted, give one agent a concrete, bounded task with the minimum necessary context.
3. If subagents are unavailable, perform a clearly labeled second pass inline from the raw artifacts.
4. Never shell out to another `codex exec` process from an active Codex session.

For reviews, keep the pass read-only. For implementation, edit only when the user explicitly asked the delegated pass to implement or fix.

## Prompt shape

Include:

- objective and success criterion
- exact files, diff, PR, error, or artifact in scope
- relevant `AGENTS.md` constraints
- what has already been tried
- whether the pass is review-only or write-capable
- requested validation

Do not leak an expected answer or the primary agent’s suspected finding when independence matters.

## Synthesis

Evaluate the returned work against repository evidence. Report:

- what the independent pass found
- where it agrees or disagrees with the primary analysis
- evidence that resolves disagreements
- remaining uncertainty

Do not present a delegated answer as correct merely because it came from another agent.
