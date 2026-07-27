# Rulespec MVP path

- **Date:** 2026-07-27, revision 2 — incorporates three independent plan
  reviews (feasibility-vs-code, measurement, scope; all
  SOUND-WITH-CORRECTIONS, reports in session record). Decisions live in
  [`decisions.md`](decisions.md); execution history in
  [`evidence/testbed-execution-2026-07-26.md`](evidence/testbed-execution-2026-07-26.md).
- **Goal:** the smallest end-to-end Rulespec implementation over real Spicy
  Regs data: source → segments → concept assignments with Rulespec roles and
  exact evidence → attested review (machine-graded until the wiki
  interface exists) → atomically published tables
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
2. ~~Bump the pinned contract digest~~ — done (`ced2b8e`; audit 1/1, 34
   mappings, 29 terms).
3. ~~Make `validate_before_publish` run under `--skip-upload`~~ — done
   (`ced2b8e`, with the in-position test). Release preflight and
   upload-environment checks stay upload-only by design.
4. **Ask the human gate now (calendar time, not engineering):** request the
   non-originating-consumer Rulespec review so it runs during phases 1–3.
   Owner: maintainer.

## Phase 1 — Fix the measurement instrument (one schema change, one rerun)

Only schema and prompt edits re-key stored provider calls
(`schema_digest`/`prompt_digest` feed `work_id`); adjudication and gold
expansion are identity-neutral — new gold artifacts mint new work items
without invalidating old ones (validated 2026-07-27). So: **the
adjudication sitting can start immediately**; the roles change (one
schema+prompt edit) lands once, then one rerun. Reader changes are three
small edits (constants become parameters; `split` column on
`gold_spans.parquet`; one more scope dimension in
`TagExtractionTask.score`) — no new corpus machinery.

1. Adjudicate the 35 gold assignments as exact / close / broader / narrower /
   related / wrong, **blind and machine-adjudicated**: two judge models
   from different families than the tagger, each seeing the gold concept
   and the fixed top-12 candidates, never the tagger output. Disagreements
   are recorded, the agreement rate is published as the residual-error
   estimate, and every adjudication row carries its machine attestor.
   Record the adequate-target-vs-abstention branch per item against the
   recorded registry generation; that branch assignment is frozen
   thereafter.
2. Add Rulespec assignment roles (primary, substantive, mention, contextual)
   to `TAG_SCHEMA`; score roles separately.
3. Expand gold to **~80 artifact assignments** (35 adjudicated + ~45 new) —
   sized for the MVP, not for accuracy claims. Generation protocol: gold
   drafted by a *different model family* than the tagger, blind to tagger
   output; the held-out slice gets **dual-model adjudication** (two judge
   families, agreement rate published; disagreements resolved by a third
   family or excluded, never silently kept); every gold row is labeled
   machine-adjudicated. Freeze `gold_sha256` before any tuning.
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

## Attestation capacity (no standing human review)

There is no human review capacity; the earlier human-task ledger (7–10
sittings) is void. The system does what it can with pipeline-generated
metadata, **honestly graded**: every adjudication and attestation records
its machine attestor, and no output is ever presented as human-verified.
The protections against the repo's two prior oracle failures move from
human sittings to structure: different-family judge models, blind
protocols (a judge never sees tagger output), dual-model adjudication with
published agreement rates, frozen digests. Human validation arrives later
through a **wiki-style interface for validating and discussing records**
(recorded in `decisions.md`); its judgments supersede machine attestations
through the same attestation table, so nothing built now is discarded. The
one human dependency left is the Rulespec release gate for MVP-public,
outside this repo.

## Phase 3 — Cleanup track (parallel, MVP-relevant only)

1. ~~`authority_edges`: carry `statute_at_large`/`executive_order`~~ — done
   (`91db195`). The dedup key is now derived (`IDENTITY_COLUMNS` = all
   non-attestation columns), so a future parsed field cannot publish
   without discriminating rows. An `executive_order` →
   `agendaAuthorityCitation` L0 mapping is *available* (canonical EO IRI
   exists) but is a new claim, not a carry-through — deliberately not made.
2. ~~U.S.C. section lists → one citation per section~~ — done (`538780c`,
   with title-boundary and citation-form-bleed hardening).
3. ~~Real calendar validation replacing invalid-day clamping~~ — done
   (`538780c`; impossible dates drop, matching `_iso_dates` semantics; the
   raw value survives in `timetable_json`).
4. After phase 2's first completed iteration: execute the deletion
   authorization in `decisions.md` in one reviewed commit.

Post-MVP (recorded in `decisions.md`, not forgotten): Mirrulations ordinal
sort; `rulespec_release.py` `_CONTRACT_FILES` becomes a rule (every `.cue`
under `constraints/{core,analysis,profiles}` recursively + every
`l0-ranges.cue` + the context file) so the preflight can pass a real
release.

## Phase 4 — MVP assembly (runner cutover + attested review)

1. **Approval is a real Attestation, in a minimal attestations table.**
   The contract forbids approval fields on the assignment (`rkaf-core.md`
   §4.7.3), and — validated 2026-07-27 — the per-row `ATTESTATION_COLUMNS`
   block is *provenance*, not an Attestation: `#Attestation` requires
   attestor, attestorKind, targets, a closed-enum decision, scope, and
   attestedAt, and rejection must be recordable ("missing tags remain
   unknown, never negative" — omission cannot mean rejected). So phase 4.1
   is one small contract-shaped `attestations` table targeting assignment
   rows. This is the second named stop-rule exception (a new carrier, on
   the ontology side; run directories stay sealed). **The MVP attestor is
   a machine**: a judge model from a different family than the extractor
   (a model never attests its own output), `attestorKind` recording it
   honestly; wiki-sourced human attestations later supersede through the
   same table. Supersession stays a separate correction path, not an
   approval. Replace the `review_gate` stub's `eligible: False` only
   insofar as diagnostic mode requires.
2. **The bridge, named as scope:** replace the legacy
   `ontology/llm.py` → `build_concept_assignments` source path with the
   docpipeline tag-task output. Explicit identity mapping:
   `assignment_id` derives from docpipeline `candidate_id`/`work_id`,
   documented and tested. Add the contract-required columns
   (`assignmentRole`, `assignmentDerivation`, `inScheme`,
   `assignmentSubjectType`, **and `assertionOrigin`** — validated count)
   to `ASSIGNMENT_COLUMNS`; `subject_type` values map to the closed
   `rkaf:Artifact|rkaf:SourceFragment` enum. `assignmentEvidence` stays
   **unclaimed at L0**: its registered range is `SourceFragment` and we
   publish no fragments table — offsets stay in `evidence_json`
   (legitimate narrowing; see the ledger's optional-future notes). The
   legacy tag path retires in the same commit (no two active tagging
   implementations).
   This identity mapping is exempt from the "migration identities" stop
   rule — it is the MVP's critical path, not harness expansion.
3. Rewrite the L0 carve-out (`conformance/rulespec-l0.yaml` lines 69–71)
   and claim the `ConceptAssignment` terms; omitting
   RelationshipAssertion/ValueAssertion narrows scope legitimately
   (verified: the audit has no class-level closure, and
   `rkaf-conformance.md` licenses mapping "whichever registered terms the
   carrier actually holds"). Concretely this is *authoring*: new
   `table: concept_assignments` mapping blocks in `docs/ontology.md` with
   `enum_map`s for role/derivation/subjectType, `terms_used` updated to
   exact equality, and `test_corpus_version`/`test_evidence_sha256`
   re-pinned to a corpus containing assignments. The rewritten note stops
   promising promotion (the contract permits LocalConcept targets); local
   → *registered* promotion stays deferred.
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
  drifted. (Two named exceptions: the phase 4.2 bridge and the phase 4.1
  attestations table.)
- If the tag task cannot directly import existing prompt/schema/offset
  behavior, stop and identify the coupling; do not copy frameworks.
- If a rerun is less accurate, keep the result and use its errors.
- Two consecutive held-out "no change" results → stop tuning; re-examine
  gold quality, scoring, and registry before the next change.
