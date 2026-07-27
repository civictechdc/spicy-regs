# Rulespec MVP path

- **Date:** 2026-07-27, revision 2 — incorporates three independent plan
  reviews (feasibility-vs-code, measurement, scope; all
  SOUND-WITH-CORRECTIONS, reports in session record). Decisions live in
  [`decisions.md`](decisions.md); execution history in
  [`evidence/testbed-execution-2026-07-26.md`](evidence/testbed-execution-2026-07-26.md).
- **Goal:** the smallest end-to-end Rulespec implementation over real Spicy
  Regs data: source → segments → concept assignments with Rulespec roles and
  exact evidence → human-attested review → atomically published tables
  conforming to Rulespec L0.
- **Acceptance is split:** **MVP-local** = one locally published generation
  with identity tables plus reviewed, contract-shaped concept assignments,
  every local gate actually executing and green. **MVP-public** = the same
  generation uploaded, which additionally needs the Rulespec human gates and
  the release-preflight repair. Only the human gates are outside this repo.
- **Sequencing supersedes** the 2026-07-26 second-half handoff.

## Where this leads (one paragraph, not authority)

Rulespec records — concepts, aliases, definitions, mappings, assignments,
evidence, corrections — are the system's memory across model calls; models
are interchangeable readers and writers of that memory. The long-term shape
is a cascade that uses the cheapest accurate method per task. Every stage of
that future is gated in `decisions.md` and none of it is in the MVP.

## Current facts (2026-07-27, post-review)

- Tag prototype **integrated**: merge `ea0e6bc`, tag/testbed/extraction
  tests pass; full suite green (1730 passed) with two strict xfails marking
  unimplemented retrieval APIs (`method_policy`, `query_methods`) in the
  parked step-5 work.
- `concept_assignments` is **already published table 8 of 9**, built by the
  legacy `ontology/llm.py` path. No bridge exists from `docpipeline` to
  publication (`grep docpipeline src/spicy_regs/{transforms,ontology,pipelines}`
  is empty). Phase 4 is therefore a runner cutover, not a new table.
- Honest baseline: F1 0.0851 on 35 gold artifacts / 34 unique labels. The
  iteration-2 "improvement" is statistically indistinguishable from noise
  (McNemar p≈0.06 under the most favorable pairing); no tuning claim stands
  until phase 1 re-baselines.
- L0 mapping audit: 0/1, sole cause the stale pin digest (verified: bumping
  it yields 1/1, 34 mappings). The deeper `rulespec_release.py` algorithm
  drift (40-file set vs rulespec's 50) matters only for MVP-public upload.
- `--skip-upload` currently returns before `validate_before_publish`, so
  every local "gates green" claim to date has been vacuous on that gate
  (release preflight and upload-environment checks are legitimately
  upload-only).
- Known data defects: `authority_edges` drops parsed
  `statute_at_large`/`executive_order` *and* its dedup key collapses two EOs
  in one string to one row; U.S.C. section lists yield one citation;
  invalid-day clamping emits dates like `2024-02-30`. (Mirrulations ordinal
  sort: real, but in the excluded comments corpus — post-MVP.)

## Phase 0 — Consolidate and unblock the gates (now)

1. ~~Commit drafts~~ (`46e8206`, `d3b8acb`, rulespec `03ac8ed`) and
   ~~integrate the tag prototype~~ (`ea0e6bc`) — done.
2. Bump the pinned contract digest in `conformance/rulespec-l0.yaml` and the
   `docs/ontology.md` mapping block to the current rulespec contract digest;
   L0 mapping audit must return 1/1.
3. Make `validate_before_publish` run before the `--skip-upload` return in
   `pipelines/materialized.py`, with a test proving it executes in position
   on a local publish. Release preflight and upload-environment checks stay
   upload-only by design.
4. **Ask the human gate now (calendar time, not engineering):** request the
   non-originating-consumer Rulespec review so it runs during phases 1–3.
   Owner: maintainer.

## Phase 1 — Fix the measurement instrument (one schema change, one rerun)

`WorkIdentity.schema_digest` re-keys every stored call on any schema edit,
so adjudication, roles, and gold expansion land **together**, then one
rerun. Reader changes are three small edits (constants become parameters;
`split` column on `gold_spans.parquet`; one more scope dimension in
`TagExtractionTask.score`) — no new corpus machinery.

1. Adjudicate the 35 gold assignments as exact / close / broader / narrower /
   related / wrong, **blind**: the judge sees the gold concept and the fixed
   top-12 candidates, never the model output. Record the
   adequate-target-vs-abstention branch per item against the recorded
   registry generation; that branch assignment is frozen thereafter.
2. Add Rulespec assignment roles (primary, substantive, mention, contextual)
   to `TAG_SCHEMA`; score roles separately.
3. Expand gold to **~80 artifact assignments** (35 adjudicated + ~45 new) —
   sized for the MVP, not for accuracy claims. Generation protocol: gold
   drafted by a *different model family* than the tagger, blind to tagger
   output; the held-out slice is **100% human-adjudicated** (one sitting);
   train-side gold may remain model-drafted with disagreements adjudicated.
   Freeze `gold_sha256` before any tuning.
4. Split by **gold concept** (not artifact — alias edits leak across
   artifacts through the lexical selector). Pin `registry_sha256` per
   iteration; add a regression test asserting no new alias normalizes to a
   held-out gold label.
5. Metric names carry their selector and depth
   (`recall@12/lexical-overlap-v1`); items without an adequate registered
   target score abstention and local-concept creation; concepts created
   during tuning never leave the abstention branch.

Exit: metrics recomputed under adjudicated gold; the bar formula below is
instantiated with real numbers **in `decisions.md`, in a commit preceding
the re-baseline run**.

## Phase 2 — Accuracy loop (exploratory, honestly)

This loop is **exploratory**: at ~80 gold, the held-out slice vetoes
regressions; it cannot certify gains (MDE at this size is larger than any
effect yet observed — the sizing math is in the measurement review).
Accuracy *claims* require the expanded set in `decisions.md`.

One change per iteration — prompt, profile fields, segment context,
local-concept edit, or (only when real-world meaning cannot be represented)
a Rulespec term. Registry edits bump the metric version. Rerun the frozen
sample; stored provider output is reusable only for byte-identical requests.
Report by relation grade, role, and profile.

Exit bar, pre-committed as a formula: bar = max(trivial baselines computed
on the same held-out set — always-abstain, lexical top-1 — , baseline +
2×bootstrap SE), tested one-sided at α=0.05; held-out numbers are reported
only when they beat the prior best by more than one MDE, otherwise "no
change." Two "no change" iterations in a row → stop tuning and re-examine
the instrument.

**Human-task ledger (honest count to MVP-local):** phase 1 carries three
(adjudication sitting, held-out adjudication, disagreement pass); each
phase 2 iteration at most one confirmation; phase 4 carries the attestation
sampling and the deletion-review commit. Roughly 7–10 total; the "one per
iteration" budget applies to the loop, not the endpoints.

## Phase 3 — Cleanup track (parallel, MVP-relevant only)

1. `authority_edges`: carry `statute_at_large`/`executive_order` through
   `COLUMNS` and fix the dedup key that collapses distinct EOs. **Not
   mechanical** — touches published schema, `data_dictionary.py`,
   `descriptions.yaml`, generated `docs/tables/*.md`, and the receipt
   ordered-column check. Own commit, regression test.
2. U.S.C. section lists → one citation per section (copy the tail-expansion
   pattern from `parse_cfr_citation`). Mechanical.
3. Real calendar validation replacing invalid-day clamping. Mechanical.
4. After phase 2's first completed iteration: execute the deletion
   authorization in `decisions.md` in one reviewed commit.

Post-MVP (recorded in `decisions.md`, not forgotten): Mirrulations ordinal
sort; `rulespec_release.py` `_CONTRACT_FILES` becomes a rule (every `.cue`
under `constraints/{core,analysis,profiles}` recursively + every
`l0-ranges.cue` + the context file) so the preflight can pass a real
release.

## Phase 4 — MVP assembly (runner cutover + attested review)

1. **Approval as attestation, not a column.** The pinned contract
   (`rkaf-core.md` §4.7.3) forbids approval fields on the assignment;
   run directories are sealed and cannot be mutated post-hoc. Review is
   recorded as ontology-side rows using the existing `ATTESTATION_COLUMNS`
   (`method="human"`) + `supersedes_id` — genuinely no new subsystem, on the
   ontology side of the seam. Replace the `review_gate` stub's
   `eligible: False` only insofar as diagnostic mode requires.
2. **The bridge, named as scope:** replace the legacy
   `ontology/llm.py` → `build_concept_assignments` source path with the
   docpipeline tag-task output. Explicit identity mapping:
   `assignment_id` derives from docpipeline `candidate_id`/`work_id`,
   documented and tested. Add the contract-required columns
   (`assignmentRole`, `assignmentDerivation`, `inScheme`,
   `assignmentSubjectType`) to `ASSIGNMENT_COLUMNS`. The legacy tag path
   retires in the same commit (no two active tagging implementations).
   This identity mapping is exempt from the "migration identities" stop
   rule — it is the MVP's critical path, not harness expansion.
3. Rewrite the L0 carve-out (`conformance/rulespec-l0.yaml` lines 70–72)
   and claim the `ConceptAssignment` terms; omitting
   RelationshipAssertion/ValueAssertion narrows scope legitimately
   (verified: the audit has no class-level closure requirement). Local →
   *registered* concept promotion stays deferred.
4. Local publish with every local gate executing and green (phase 0.3 makes
   this falsifiable).
5. Retire the v2 `corpora` runners per `decisions.md`, keeping stored v4
   outputs read-only as the benchmark artifact.

**MVP-local acceptance:** the generation above, reproducible from receipts.
**MVP-public:** same generation uploaded — requires the non-originating
consumer review + maintainer-authorized Rulespec release (requested in
phase 0.4) and the preflight rule fix. Date it when the review request is
answered.

## Not in the MVP

Retrieval serving (latent coupling noted: retrieval candidates entering an
extraction payload will trip `refuse_retrieval_aids`'s global `score`/`rank`
ban — resolve *then*, not now); relation-benchmark unblocking; GLinker,
SetFit, cleanlab, HDBSCAN, Splink, GLiREL, pySHACL; graph engines and
embeddings; signed receipts; comments corpus; state bills; new MCP surface;
markup-prolog title-region rework (5 gold cues — schedule as a phase 2
iteration *after* the first loop completes, since it moves frozen segment
boundaries). Exact parity with old manifests, full historical replay, and
provider-free recreation of old experiments are permanently non-goals.

## Stop rules

- If work requires retrieval, new storage *carriers* (columns on existing
  carriers are fine), or a generalized workflow engine — stop; scope
  drifted. (The phase 4.2 bridge is the named exception.)
- If the tag task cannot directly import existing prompt/schema/offset
  behavior, stop and identify the coupling; do not copy frameworks.
- If a rerun is less accurate, keep the result and use its errors.
- Two consecutive held-out "no change" results → stop tuning; re-examine
  gold quality, scoring, and registry before the next change.
