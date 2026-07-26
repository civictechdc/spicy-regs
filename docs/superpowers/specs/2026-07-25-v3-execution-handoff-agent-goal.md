# V3 Execution Handoff — Agent Goal

- **Date:** 2026-07-25
- **Role:** orchestrator prompt for the next working session
- **Orchestrator:** Claude Fable — plans, dispatches, verifies, integrates.
  Writes no implementation code itself.
- **Workers:** Opus subagents at xhigh reasoning effort — one implementer per
  task, one independent reviewer per landed task.
- **Authority:** [`TODO-RULE.md`](../../../TODO-RULE.md) (execution),
  [`2026-07-25-rulespec-spicy-regs-complete-vision-goal.md`](2026-07-25-rulespec-spicy-regs-complete-vision-goal.md)
  (architecture),
  [`2026-07-25-spicy-regs-document-pipeline-v3-design.md`](2026-07-25-spicy-regs-document-pipeline-v3-design.md)
  (implementation shape)
- **Worktrees required:** `/Users/mikewolfd/Work/spicy-regs` and
  `/Users/mikewolfd/Work/rulespec` (add the second with `/add-dir`)

## Goal

Advance the critical path in `TODO-RULE.md` until every remaining item is
finished, blocked, or waiting on a human gate:

1. **Milestone A — reshape the Rulespec contract.** Work the checklist in
   `../rulespec/TODO.md`, section "Assertion, concept, and analysis contract
   reshape (paired with Spicy Regs)". Prepare everything up to — but not
   through — the non-originating-consumer review and the release
   authorization. Those are human gates.
2. **In parallel: v3 build order steps 1–3** (v3 design, "Build order").
   Tests for work IDs, checkpoints, atomic rename, secrets, inventory,
   required failures, empty results, resume, validation, and rebuild come
   first; then the runtime and the OpenAI, Codex, Docling, embedding, sparse,
   and reranking adapters; then move v2 relationship extraction into
   `src/spicy_regs/docpipeline/`.

Do not release, tag, push, publish, promote a concept, or enable legal-effect
output. Those need explicit maintainer authorization (vision, "Agent goal").
Local implementation, tests, fixtures, and receipts are all in scope.

## How to run the work

- **Read before dispatching:** `TODO-RULE.md`, the v3 design including its
  Related contracts list, and `../rulespec/TODO.md`. These documents already
  made the decisions; do not re-derive or re-litigate them.
- **Orchestrator:** break each milestone into tasks one implementer can finish
  in one context window. Dispatch, then verify the result yourself against the
  task's acceptance criteria before marking it done. Keep `TODO-RULE.md`
  checkboxes current; its own rule applies — a checked item must link a
  commit, artifact, receipt, or dated record.
- **Implementers:** Opus subagents at xhigh effort, one task each,
  test-driven (write the failing test first), no scope creep. Each task
  prompt names the exact spec section that constrains it.
- **Reviewers:** a separate Opus xhigh subagent per landed task, reviewing
  adversarially against the constraining spec section. An implementer never
  reviews its own work. Confirmed findings go back to a fresh implementer.
- **Simplicity rules stay binding:** four small provider interfaces; no
  workflow language, plugin registry, event bus, or scheduler; provider
  libraries stay in `adapters/`; Docling only through `adapters/docling.py`
  and only from `source.py`; DuckDB used directly only in `retrieval.py`.
- **Conflicts:** if a supporting document contradicts the vision or a spec,
  stop that task and report the conflict. Do not maintain two active
  meanings and do not silently pick a side.

## Done for this session

- Milestone A items checked with evidence links up to the human gates, or a
  written blocker note per unfinished item.
- v3 steps 1–3 landed with passing tests from a clean checkout of the
  branch: resume reuses finished work, failures stay distinct from empty
  results, rebuild never calls a provider.
- Relation v2 fixtures pass through the new runner (v3 design, "Runner
  cutover", row 1), with expected differences recorded and approved in the
  migration test.
- No release, tag, push, or publication happened.

## Reporting style

Follow `~/.codex/AGENTS.md`: plain English, lead with the result or decision,
separate verified facts from plans and blockers, and keep updates brief
enough to scan.
