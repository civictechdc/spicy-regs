# V3 Execution Handoff, Second Half — Agent Goal

- **Date:** 2026-07-26
- **Status:** Ready to execute
- **Predecessor:** [`2026-07-25-v3-execution-handoff-agent-goal.md`](2026-07-25-v3-execution-handoff-agent-goal.md)
  (executed 2026-07-26; see its execution record)
- **Role:** orchestrator prompt for the next working session. The
  orchestrator plans, dispatches, verifies, integrates, and commits. It
  writes no implementation code itself.
- **Workers:** every subagent — implementer, reviewer, fixer, explorer —
  is Opus at xhigh reasoning effort. No other model or effort tier.
- **Skills:** do not invoke superpowers skills. Plain orchestration only.
- **Authority:** [`TODO-RULE.md`](../../../TODO-RULE.md) (execution),
  [`2026-07-25-rulespec-spicy-regs-complete-vision-goal.md`](2026-07-25-rulespec-spicy-regs-complete-vision-goal.md)
  (architecture),
  [`2026-07-25-spicy-regs-document-pipeline-v3-design.md`](2026-07-25-spicy-regs-document-pipeline-v3-design.md)
  (implementation shape)
- **Worktrees required:** `/Users/mikewolfd/Work/spicy-regs` and
  `/Users/mikewolfd/Work/rulespec` (add the second with `/add-dir`)
- **Posture:** keep everything local. No release, tag, push, publication,
  concept promotion, or legal-effect output. Milestone B stays untouched —
  it waits on a released Rulespec contract by design.

## Starting state (verified 2026-07-26)

- Rulespec `us-regulatory-identifiers` at the reshape completion
  (`56686d9` + docs): all seven automatable Milestone A items done;
  clean-checkout gates green (420 conformance fixtures, 0 divergences).
  Only the human gates remain — do not attempt them.
- Spicy Regs `feat/document-ai-pipeline` at `97bc462`: v3 build order
  steps 1–3 landed (`docpipeline` runtime, OpenAI/Codex and Sentence
  Transformers adapters, migrated v2 relationship extraction with the
  approved migration test); clean checkout at `56a2030` passed 904 tests.
- Old runners are all still active on purpose; removal is step 8.

## North star

Everything below serves one product goal: a universal metadata layer
over all Spicy Regs data — and, eventually, sources beyond it — that
lets people filter, query, sort, and evaluate documents to find
insights: reliable alerts on changes that matter, and surfacing
documents that look unrelated when they are in fact related or
dependent. Related and dependent are different claims, and the contract
keeps them distinct on purpose:

- **Relatedness** is suggestive metadata — retrieval scores, shared
  concepts, `ConceptAssignment`s. It powers discovery and never becomes
  a fact on its own.
- **Dependency** is an evidence-backed claim — `RelationshipAssertion`s,
  `RelationChangeEvent`s, checked graph edges with exact source
  evidence. This is what alerts and downstream conclusions may rely on.
- **Alerts are only as reliable as the pipeline is honest** about what
  it knows, what changed, and what failed — which is why receipts,
  resume, validation, and fail-closed gates exist. They are alert
  infrastructure, not ceremony.

## Engineering posture

Apply these when scoping and cutting tasks; the north star is the
tiebreaker:

- **DRY / KISS:** reuse the landed runtime, adapters, envelope, and
  composition machinery. No new frameworks, no parallel abstractions, no
  second way to do a thing that already has one.
- **80/20, don't boil the ocean:** prefer the smallest slice that makes
  real data queryable end-to-end over exhaustive coverage of any one
  step. When a task offers a choice, pick what moves
  filter/query/sort/evaluate/alert capability soonest; record the
  remainder as a follow-up instead of building it now.
- **Agile, forward-thinking:** land vertical slices that work, in
  cutover order, each independently committed and reversible. Never
  foreclose new data sources — a new document family must stay one
  source adapter plus a profile (already a design rule; keep it true).
- **Production-oriented:** bias toward the paths that will actually run
  in production — `build` mode and real source data over diagnostic-only
  affordances; gates that catch real failures over ceremonial checks.

## Goal

Advance the remaining local work until every item is finished, blocked,
or waiting on a human gate:

1. **V3 build order steps 4–8** (v3 design, "Build order"), one step at a
   time, in order:
   - **Step 4** — move source parsing and `structure-overlap-1800`
     segmentation into `docpipeline` (`source.py`, `segments.py`,
     `adapters/docling.py`; Docling enters only through that adapter and
     is used only by `source.py`).
   - **Step 5** — move document and section search, BGE dense, sparse
     search, reciprocal-rank merge, and reranking (`retrieval.py`; DuckDB
     is used directly only here).
   - **Step 6** — move tag and typed-value extraction,
     section-to-document tag combination, and approval (`approval.py`
     plus extraction task definitions).
   - **Step 7** — move relationship comparison (`comparison.py`; no
     provider or search imports; no AI calls).
   - **Step 8** — `workflow.py` and `cli.py` (the `run-pipeline` →
     `run-regulations-etl` rename lands in the same change as `cli.py`,
     touching `pyproject.toml`, the ETL workflow file, `CONTRIBUTING.md`,
     and the module docstring); run the migration parity gate; remove the
     replaced runners per the cutover procedure, keeping a read-only
     checker for immutable historical runs.
   Each step follows the design's "Runner cutover" procedure: same fixed
   inputs through old and new code, expected differences recorded and
   approved in a migration test, old fixtures passing through v3, and
   rollback via Git — never a second active implementation path.
2. **Migration parity gates are binding.** The design's "Measurable
   gates" table must hold on the existing frozen data before step 8
   completes: 35/35 gold spans contained, hybrid Recall@50 ≥ 0.8286,
   reranked BGE Recall@10 ≥ 0.7143 / Recall@50 ≥ 0.8000 / MRR ≥ 0.4639,
   OpenAI tag P/R/F1 ≥ 0.8857 each, evidence grounding 1.0000, zero
   false target relationships on unrelated controls, zero secret matches.
3. **Milestone C local preparation** (TODO-RULE, Milestone C), up to but
   not through the human gates: author `docs/retrieval-evaluation.md` as
   the single evaluation contract (scope, hashes, splits, metrics,
   thresholds, latency budget, result size, exclusion rules) — step 8's
   frozen mixed-data gate depends on it. Do not seal oracles, run blinded
   repetitions, or freeze anything requiring the two human reviewers.
4. **Close the carried follow-ups** from the first half, each with a
   regression test: Codex arm pre-flight schema validation (parity with
   the OpenAI arm); earlier-run mode-compatibility check in
   `runtime.check_earlier_run`; TypeScript closure of language-tagged
   value objects (Rulespec); `generate_negatives.py` relative context
   paths (Rulespec).

## How to run the work

- **Read before dispatching:** `TODO-RULE.md`, the v3 design (especially
  "Step decisions", "Runner cutover", "Measurable gates", and the
  "Best existing code to copy" table), and the first handoff's execution
  record. The documents already made the decisions; do not re-derive
  them.
- **Orchestrator:** break each step into tasks one implementer can finish
  in one context window; verify every task yourself against its
  acceptance criteria (run the tests and gates) before committing it;
  commit per landed step with evidence; keep `TODO-RULE.md` checkboxes
  current — a checked item links a commit, artifact, receipt, or dated
  record. Scope every task through the engineering posture above: if a
  piece of work does not move filter/query/sort/evaluate/alert
  capability or a binding gate, cut it and record it as a follow-up.
- **Dispatch mechanism:** chained workflows (implement → fix) with
  prompts embedded in the script body. Every agent is Opus xhigh.
- **Review cadence — checkpoints, not per task.** Run one independent
  adversarial Opus xhigh review at each checkpoint, covering everything
  landed since the previous one:
  1. after steps 4–5 land,
  2. after steps 6–7 land,
  3. after step 8, before the old runners are removed — this final
     review must also cover the parity-gate evidence and the removal
     plan itself.
  Confirmed findings go to a fresh implementer, as before. Between
  checkpoints the orchestrator's own verification (tests, gates, parity
  numbers, forbidden-import checks) is the quality bar. An implementer
  never reviews its own work.
- **Simplicity rules stay binding:** four small provider interfaces; no
  workflow language, plugin registry, event bus, or scheduler; provider
  libraries stay in `adapters/`; Docling only through
  `adapters/docling.py` and only from `source.py`; DuckDB used directly
  only in `retrieval.py`; steps never import another step's private
  helpers; `docpipeline` never imports `spicy_regs.corpora` at runtime
  (migration tests may import old runners test-only while both exist).
- **Conflicts:** if a supporting document contradicts the vision or a
  spec, stop that task and report the conflict. Do not maintain two
  active meanings and do not silently pick a side.

## Done for this session

- V3 steps 4–8 landed and committed, each old runner removed only after
  its cutover row is proven, with a read-only checker retained for
  immutable historical runs.
- The migration parity table holds, with the numbers recorded in a dated
  note or receipt linked from `TODO-RULE.md`.
- `docs/retrieval-evaluation.md` exists and is complete enough to freeze,
  with its human-gate items explicitly marked open.
- The four carried follow-ups are closed with regression tests.
- Full suites green from clean checkouts of both branches (spicy-regs
  needs `--extra embed --extra evaluation`; rulespec needs a cold
  `make compile && make test`).
- All three checkpoint reviews ran; confirmed findings fixed.
- No release, tag, push, or publication happened; Milestone B untouched.

## Operational notes (from the first half)

- Embed long prompts in the workflow script body as template literals;
  the `args` parameter can arrive as a JSON-encoded string and leave
  `args.prompt` undefined.
- Give reviewers explicit command timeouts and forbid cold cargo builds
  outside the repo's warm target directory; one unguarded reviewer hung
  for two hours on a cold build.
- The spicy-regs test environment needs the `embed` and `evaluation`
  extras; plain `uv sync --frozen` lacks sentence-transformers and torch,
  and the new adapter tests are hermetic against that but the legacy
  segmentation tests are not.
- Verify claimed "no verdict changes" by diffing full conformance or
  pytest reports, not by trusting summaries.

## Reporting style

Follow `~/.codex/AGENTS.md`: plain English, lead with the result or
decision, separate verified facts from plans and blockers, and keep
updates brief enough to scan.
