# Fast path to a working Rulespec testbed

- **Date:** 2026-07-26
- **Status:** Executed once; independently reviewed by `claude-fable-5` before
  implementation and by native Sol architecture/code reviewers after the first
  run
- **Near-term objective:** use real documents and model output to find and fix
  Rulespec and Spicy Regs accuracy problems
- **Execution rule:** new `spicy_regs.docpipeline` code runs the work; existing
  `spicy_regs.corpora` artifacts provide the benchmark and baseline
- **Not in the critical path:** MCP, publication, historical migration parity,
  retrieval, a formal Rulespec release, or a complete production platform

## Decision

Stop expanding the migration harness. Use the new document-pipeline source,
segmentation, model-adapter, extraction, and run-storage code to repeat the
useful experiment that already exists:

```text
selected real documents
  -> new source parsing and structure-overlap-1800 segmentation
  -> LLM tag candidates with exact source evidence
  -> stored provider requests and responses
  -> scoring plus direct review of mistakes
  -> one focused prompt, profile, local-taxonomy, or Rulespec correction
  -> rerun the same sample and compare
```

Determinism supports this loop by holding the inputs, segment boundaries, and
scoring steady. Accuracy is the outcome. We should accept model-dependent
results and iterative improvement; we should not accept ungrounded tags,
uninspectable failures, or changes that cannot be compared on the same sample.

## Execution result

The path completed on 2026-07-26 in the isolated
`feat/rulespec-testbed-loop` worktree:

- the new pipeline processed all 44 selected artifacts and 109 segments;
- a real `gpt-5.6-sol` run stored 109 successful calls with exact evidence;
- a provider-free rebuild applied two review-required scoring corrections
  without changing the original calls;
- one prompt-only refinement reduced accepted candidates from 351 to 76 and
  counted false positives from 260 to 55;
- precision rose from `0.0335` to `0.0678`, recall fell from `0.2571` to
  `0.1143`, and F1 rose from `0.0592` to `0.0851`;
- evidence grounding remained `1.0`;
- both runs remained diagnostic-only and passed integrity and provider-free
  recomputation.

`RULESPEC_FEEDBACK_ITERATION_2.md` records the important result: exact-label
scoring confuses semantically compatible local concepts with errors, and the
single-label gold cannot distinguish primary topics from useful substantive
and mention tags. Rulespec already represents both distinctions; the next
small loop belongs in Spicy Regs evaluation and task output, not new Rulespec
vocabulary.

## Verified starting point

The useful loop is not new:

- `spicy_regs.corpora` already builds the mixed real-data evaluation set, runs
  segmentation and LLM tagging, stores assignments and validations, and
  computes metrics (`pyproject.toml:95-103`).
- The selected stored reference is
  `output/segmentation-tagging-document-openai-structure-overlap-1800-v4`.
  It uses 10 document profiles, 44 selected artifacts, 35 gold artifacts, 9
  controls, 109 selected segments, and the `structure-overlap-1800`
  configuration.
- That historical run reported precision, recall, and F1 of `0.8857`,
  validation agreement of `0.96`, and evidence grounding of `1.0`. It is not
  an honest hidden-gold comparator: the old runner added the curated gold
  labels to its effective concept registry before model execution. The new
  gold-free diagnostic is the baseline for its own refinement.
- The new source and segmentation implementation already selects
  `structure-overlap-1800` and preserves exact source slices
  (`src/spicy_regs/docpipeline/segments.py:1-56`).
- The new extraction code already supplies a provider-neutral task interface,
  strict response checks, stored requests and responses, candidate and
  rejection tables, scoring, and provider-free recomputation
  (`src/spicy_regs/docpipeline/extraction.py:1-24`,
  `src/spicy_regs/docpipeline/extraction.py:167-229`).
- The new extraction layer has a relationship task, but no tag task yet. Its
  own interface explicitly anticipates a tag task
  (`src/spicy_regs/docpipeline/extraction.py:16-20`).
- Retrieval work is unfinished and uncommitted. Tagging the selected benchmark
  segments does not require retrieval, so retrieval must not block this loop.
- `extraction.py` and `runtime.py` also have uncommitted edits. Some may be
  useful extraction/runtime corrections mixed with Step 5 work, so the
  committed revision cannot be assumed to contain every capability cited here.
- The sibling Rulespec worktree currently has local changes. This plan reads
  from an exact clean Rulespec revision when it checks shared semantics and
  does not modify that worktree.

The mistake was treating the existing experiment as a legacy system that the
new code had to reproduce internally. It is only a benchmark: its real
documents, gold labels, and metrics matter; its run IDs, manifests, storage
layout, and intermediate tables do not.

## What “working” means

A working testbed completes one full accuracy iteration:

1. Run the selected 44 artifacts, including 35 gold artifacts and 9 controls,
   through the new source and segmentation code.
2. Generate tag candidates through the new extraction interface and a real
   model.
3. Store the exact request, response, model identity, evidence offsets,
   candidates, rejections, and metrics.
4. Score final tags against the existing hidden gold labels without including
   those labels in model input.
5. Review the false positives, false negatives, novel tags, and weak document
   profiles directly.
6. Classify each important error as one of:
   - source parsing or segmentation;
   - prompt or model behavior;
   - Spicy Regs profile or local taxonomy;
   - Rulespec vocabulary, constraint, or evidence-model problem;
   - incorrect or ambiguous gold label.
7. Make one focused correction, rerun the same sample, and show whether the
   reviewed errors decreased without losing evidence grounding.
8. Record the Rulespec-relevant findings in one short dated feedback report.

The first diagnostic run does not need to beat the old score. It must produce
usable tags, honest metrics, and inspectable errors. The first refinement is
successful when it reduces the adjudicated error set or demonstrates, with
source evidence, that a Rulespec or gold-label assumption is wrong.

## Smallest implementation

### 1. Isolate the current unfinished work

Leave the current dirty Step 5 files untouched. Execute this path in a separate
worktree or branch after classifying the diffs in `extraction.py` and
`runtime.py` against `HEAD`:

- keep any independently useful extraction/runtime correction required by the
  tag task;
- leave retrieval-only changes and the untracked retrieval files behind;
- do not bring migration tests, legacy identities, or historical storage
  support into the new branch.

Do not delete or reformat the unfinished retrieval migration as part of this
work. The stored v4 tagging outputs are the comparison artifact; rerunning the
old tagging runner is not part of this path.

### 2. Feed the benchmark into the new source and segment steps

Add only the evaluation-side reader needed to read the stored evaluation
dataset, select the existing 44 artifacts, and load the 35 gold artifacts'
labels. Do not rebuild the corpus for the first iteration.

It must not teach production code to understand old run IDs, old manifests, or
old table layouts. Production inputs remain `SourceRecord` and
`SourceArtifact`; benchmark translation stays in the evaluation command or
test support.

Map the stored field-coordinate gold spans onto the new `SourceArtifact`
coordinates as an explicit evaluation task. Preserve valid original offsets.
When offsets do not transfer, reuse the existing unique-exact-match resolution
behavior from `spicy_regs.ontology.llm.resolve_exact_evidence_offsets`. An
ambiguous or missing match is a reported benchmark-input failure, not a guessed
coordinate.

Use the selected new segmentation settings without running another segmentation
bakeoff. Validate:

- all selected artifacts reach a terminal source outcome;
- every evidence slice resolves exactly to source text;
- every gold span is either present in a segment or reported as a concrete
  source/segmentation failure;
- hidden gold fields do not enter model input.

Keep the current heading-region and `markup-prolog` segmentation behavior
through this first iteration. Although the code describes it as migration
parity, it now serves baseline stability: changing it would move the frozen
segment boundaries before the tag task has a comparable result.

### 3. Implement one tag extraction task

Implement a `TagExtractionTask` behind the existing `ExtractionTask` interface.
Import the current `TAG_INSTRUCTIONS` and
`resolve_exact_evidence_offsets` behavior directly from
`spicy_regs.ontology.llm`. Expose its existing private `_TAG_SCHEMA` as the
public `TAG_SCHEMA` needed by both paths. Reuse the existing tag normalization
and implement ungrounded model output as rejection rows. Do not invent a second
tag schema merely to fit the new runtime.

The task needs only:

- a gold-free payload containing the segment text, source identity, profile,
  allowed concept registry, and exact evidence coordinates;
- a strict tag-candidate response schema;
- response and evidence-grounding checks;
- candidate and rejection rows;
- final assignment aggregation at segment and document scope;
- benchmark scoring against separately supplied answers.

Use the existing typed model adapter and extraction storage. Do not add
provider-specific behavior to the task.

Run both the initial pass and the refinement in `diagnostic` mode with answers
supplied separately for scoring. Implement only the non-authorizing
`review_gate` result required by the task interface. Do not invoke benchmark
mode or its sealed human-review protocol; benchmark eligibility is deferred.

Automated approval, comparison, graph materialization, and a generalized task
registry are not required for this iteration. The outputs remain experimental
candidates reviewed through the benchmark.

### 4. Run the diagnostic baseline

Run the new tag task once in `diagnostic` mode on the same selected sample and
save it under a new run directory. Compare only outcomes that answer an
accuracy or safety question:

- precision, recall, and F1 overall and by profile;
- false-positive and false-negative cases;
- evidence-grounding rate;
- empty-tag rate;
- novel-tag rate;
- prompt-injection behavior;
- provider failures and rejected responses;
- model and prompt identity.

Do not compare byte layout, run identity, receipt shape, checkpoint layout,
provider-call accounting details, or intermediate table equality with the old
runner.

### 5. Perform one accuracy iteration

Start with the smallest high-signal error cluster. The first new diagnostic
showed broad over-tagging: 351 accepted candidates produced 260 counted false
positives on the 35 gold artifacts. That breadth, not the old runner's profile
metrics, is the first review set.

Read the source excerpts and model responses before choosing a fix. Change one
of the following at a time:

- prompt instruction or example;
- source/profile field selection;
- segment context;
- local concept label, definition, alias, or merge;
- Rulespec term or constraint, only when the correct real-world meaning cannot
  be represented cleanly.

Rerun the same selected sample. Stored provider output may be reused only for
unchanged requests; changed prompts or inputs require fresh calls.

### 6. Record the learning

Create one concise `RULESPEC_FEEDBACK_ITERATION_2.md` after the rerun. For each
Rulespec-relevant finding, include:

- the real source and exact excerpt;
- the expected meaning;
- the model or carrier result;
- why the issue belongs in Rulespec rather than the prompt, parser, profile, or
  local taxonomy;
- the smallest proposed Rulespec change;
- the before/after result when a candidate correction was tested.

Do not create a difference ledger, evidence-hash system, migration receipt, or
new schema for this report.

## Acceptance gates

The path is complete when all of these are true:

- The selected real-data benchmark runs through new source, segmentation, and
  tag extraction code without using the old runner for execution.
- A real model produces stored candidate and rejection outputs.
- Gold labels remain isolated from model requests.
- Every accepted tag points to exact source evidence; missing or ambiguous
  evidence becomes a rejection, not a guessed assignment.
- Overall and per-profile metrics recompute from the stored run.
- A maintainer can inspect every false positive and false negative without
  reconstructing hidden pipeline state.
- One focused correction is rerun on the same sample and its accuracy effect is
  reported.
- Rulespec findings are separated from Spicy Regs, model, source, and gold-label
  problems.
- Focused tests, the new diagnostic run, and the short feedback report are
  green and internally consistent.

These are not gates:

- exact parity with old manifests or intermediate tables;
- a full historical replay;
- provider-free recreation of old experiments;
- retrieval quality;
- automated approval or publication;
- formal Rulespec release metadata;
- MCP behavior;
- full-corpus model tagging.

## Scope disposition

**Use now**

- new source parsing and `structure-overlap-1800` segmentation;
- new typed model adapters;
- new extraction task interface and stored provider outputs;
- existing mixed real-data sample, gold labels, and baseline metrics;
- exact evidence grounding and answer isolation;
- simple metrics and direct error review.

**Park**

- unfinished Step 5 retrieval code and migration tests;
- automated approval, comparison, and materialization;
- full-corpus tagging;
- MCP and publication;
- formal Rulespec release work;
- provider-free rebuild except when a concrete scoring correction must be
  applied to already stored provider output.

**Remove later, after the new loop works once**

- migration-only fixtures and expected-difference files;
- legacy identity and storage compatibility;
- exhaustive intermediate-equivalence checks;
- any runtime accounting or validation machinery that exists only to prove
  compatibility with the one-day-old `corpora` runner.

No deletion is authorized by this document.

## Timebox and stop rules

Target one focused day for the new tag task and first diagnostic run, then one
short iteration on the highest-value errors. If the work starts requiring
retrieval, new storage formats, migration identities, or a generalized
workflow engine, stop: the scope has drifted.

If the tag task cannot directly import the existing prompt, schema, and offset
resolution, stop and identify the exact coupling. Do not respond by copying
the experiment framework or creating a generalized shared library.

If the new run is less accurate, keep the result. Use its concrete errors to
fix the new path. The testbed exists to make those failures visible, not to
protect a predetermined architecture.

## Independent validation

On 2026-07-26, `claude-fable-5` reviewed this document and the directly
relevant source, segmentation, extraction, runtime, tagging, and stored
baseline files in a read-only, budget-capped call.

**Verdict: APPROVE.** The reviewer found the core route to be the fastest safe
path and required three clarifications before implementation:

1. triage the dirty `extraction.py` and `runtime.py` diffs before branching;
2. use diagnostic mode so the sealed benchmark-review machinery cannot block
   the learning run;
3. read the stored evaluation dataset directly and make gold-span coordinate
   translation explicit.

This revision incorporates all three. It also adopts the reviewer's smaller
recommendations to import the existing tag behavior directly, use the stored
v4 outputs rather than preserve the old runner as a fallback, and keep current
segment boundaries for baseline stability through the first iteration.
