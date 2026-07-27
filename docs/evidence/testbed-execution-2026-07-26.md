# Testbed prototype execution record — 2026-07-26

Historical. Facts frozen as of the dates below; nothing here is a current
instruction. The active plan is `docs/rulespec-testbed-path-forward.md`.

## Prototype run (isolated worktree, branch `feat/rulespec-testbed-loop`)

- The new pipeline processed all 44 selected artifacts and 109 segments
  through new source parsing and `structure-overlap-1800` segmentation.
- A real `gpt-5.6-sol` run stored 109 successful calls with exact evidence.
- A provider-free rebuild applied two review-required scoring corrections
  without changing the original calls.
- One prompt-only refinement ("at most one central substantive topic")
  reduced accepted candidates 351→76 and counted false positives 260→55.
- Metrics: precision 0.0335→0.0678, recall 0.2571→0.1143, F1 0.0592→0.0851.
  Evidence grounding 1.0 in both runs. Prompt-injection control passed.
- Both runs were diagnostic-only and passed integrity and provider-free
  recomputation.
- The code (`tag_task.py`, `rulespec_testbed.py`, tests, ontology changes)
  was committed 2026-07-27 as `d3b8acb` on `feat/rulespec-testbed-loop`.

Full finding detail: `RULESPEC_FEEDBACK_ITERATION_2.md` (repo root).

## Disqualified prior baseline

The stored v4 tagging run
(`output/segmentation-tagging-document-openai-structure-overlap-1800-v4`)
reported P/R/F1 0.8857 with validation agreement 0.96 and grounding 1.0. It
is not an honest hidden-gold comparator: the old runner added the curated
gold labels to its effective concept registry before model execution. The
gold-free diagnostic above is the baseline for its own refinement. Only
`medicaid` among the 35 gold assignments had a natural exact-label match in
the 901-concept gold-free registry.

## Independent validation — scope-limited

On 2026-07-26, `claude-fable-5` reviewed the path-forward document *as it
existed that day* plus the directly relevant source, segmentation,
extraction, runtime, tagging, and stored baseline files, in a read-only
budget-capped call. Verdict: APPROVE, with three required clarifications
(triage the dirty extraction/runtime diffs before branching; run in
diagnostic mode; make gold-span coordinate translation explicit), all
incorporated the same day.

**This verdict covers only the 2026-07-26 revision.** The 2026-07-27
additions (functional end state, package slots, citation decisions) and the
MVP restructure were not part of the reviewed text.
