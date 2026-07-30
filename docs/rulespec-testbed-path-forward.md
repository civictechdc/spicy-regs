# Rulespec MVP path

- **Status:** Historical fused-registry MVP record; superseded for vocabulary
  experiments by the
  [RefSpec managed vocabulary roadmap](../RefSpec/plans/managed-vocabulary-experiment-roadmap.md).
- **Date:** 2026-07-28, revision 4 — adds the current execution order
  (three tracks) after the selector ablation, evaluation boundary, and
  experiment-strategy validation. Revision 3 (2026-07-27) incorporated
  three independent plan reviews plus the registry-dimension and
  evaluation-boundary corrections. Decisions live in
  [`decisions.md`](decisions.md); execution history in
  [`evidence/testbed-execution-2026-07-26.md`](evidence/testbed-execution-2026-07-26.md).

## Historical execution order (2026-07-28)

**Track A — the holdout (critical path; nothing accuracy-shaped moves
without it).** In order: (1) draw candidate artifacts from several source
families, disjoint from the 35 and the dev corpus by artifact digest,
concept id, and normalized alias; pin membership and digests at draw
time in the boundary record. (2) Build blind drafting inputs (no tagger
output). (3) Gold drafted by a non-tagger family (claude-fable-5),
multi-label with roles and frames, including hard negatives, explicit
denials, and forbidden results (manual checks — the gate does not yet
enforce composition). (4) Freeze the evaluated configuration —
`registry_sha256` (fused), selector version, prompt, schema, token
budget — and instantiate the exit-bar formula in `decisions.md`,
BEFORE any label exposure. (5) Cross-family adjudication (GPT judge via
the existing adapter + independent Fable judge; disagreements to a
third family or excluded; agreement published). (6) Gate green →
certify selector adoption on holdout candidate recall, rerun the roles
schema on dev, then the one-shot holdout evaluation; used items move to
development. Target ~80 assignments (MVP tier per the ledger).

**Track B — deterministic discovery questions (parallel; product-level;
zero model dependency).** Activated by the maintainer's adoption of this
revision (satisfying the experiment-strategy subordination rule): (1)
record the "active rulemaking" definition in the ledger — no such
definition exists in any schema. (2) Freeze the `40 CFR 60` question:
snapshot, expected and forbidden identifiers, counts, evidence, and the
declared agenda-only recall boundary. (3) Run and score it on identity,
link, filter, and aggregate measures. (4) Repeat for `42 U.S.C. 7401`
via the corrected join (`authority_edges` → `agenda_item_proceedings` →
`proceedings`). Failures feed phase-3-style fixes.

**Track C — attestations table (parallel; no gold contact).** Build the
contract-shaped attestations carrier (phase 4.1, normative pattern
rulespec `b613ba3`) now: the holdout adjudication itself should be
stored as attestation rows, so track A becomes its first consumer
rather than a JSON side channel.

**Parked, unchanged:** round-3 judging of the 35 (dev-only, superseded);
the hyperbolic subsumption scorer (awaiting maintainer call — requires
training an encoder); retrieval; the wiki interface. **Maintainer-only
items:** the Rulespec non-originating-consumer review request (phase
0.4, still unrequested) and the spend nod for track A's judge calls.
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
- The original 35 are now manifest-pinned and permanently
  **train/development-only**. They were inspected and used to tune the prompt,
  registry, and selector, so neither freezing nor new adjudication can turn
  them back into holdout. No untouched cross-family-adjudicated holdout
  currently exists; adoption/accuracy readiness must fail.
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

1. ~~Adjudicate the 35 gold assignments for development diagnosis~~ — done 2026-07-27, blind
   dual-judge (claude-fable-5) + third-judge tiebreak:
   `docs/evidence/gold-adjudication-2026-07-27/`. Grade agreement 34/35,
   adequacy agreement 35/35. Resolved: 1 exact, 4 close, 20 broader,
   1 narrower, 8 related, 1 wrong; **adequate target 5/35 (14.3%),
   frozen as development evidence**. All three judges came from one model
   family, so this is not cross-family holdout adjudication. The old
   exact-label baseline measured registry coverage, not model quality.
2. ~~Add Rulespec assignment roles to `TAG_SCHEMA`~~ — done (`df2b177`;
   reader parameterization + split column `54d02de`). The single intended
   re-keying: TAG_SCHEMA and TAG_INSTRUCTIONS digests both moved.
   **Budget correction implemented:** preflight now builds the exact payload
   and response schema and removes only the lowest-ranked candidate until the
   prompt fits. The current 109-segment development preflight passes at a
   maximum 8,147/8,192 tokens. Both the production selector and
   `anchored-hybrid-v2` use this same path.
3. Draw a **new** gold dataset for validation/holdout; do not append the 35
   into a mixed file and call part of that file held out. Gold drafting stays
   blind to tagger output. Before any labels are exposed, freeze membership,
   source/selection digests, candidate-selector configuration, and the
   intended role of each partition. Holdout adjudication requires at least
   two independent model families (or humans), published agreement, and a
   third family or exclusion for disagreements. No such holdout exists today;
   provider credentials were unavailable, so no labels were fabricated.
4. Separate by **gold concept and every registered alias**, not merely by
   artifact. Pin `registry_sha256`; reject shared concept ids, normalized
   aliases, or artifact digests across train and holdout. A registry alias
   learned from training must never reveal a holdout target.
5. ~~Install the executable boundary~~ — done locally:
   `evaluation-boundary.json` pins the original files and forces all 35 to
   train; `rulespec_testbed --require-adoption-ready` refuses a verdict without
   an untouched holdout, complete cross-family adjudication, and frozen-before-
   labels controls. Metric output carries its eligibility and blockers.
6. Metric names carry their selector and depth
   (`recall@12/lexical-overlap-v1`); items without an adequate registered
   target score abstention and local-concept creation; concepts created
   during tuning never leave the abstention branch.

Exit: development metrics are recomputed under adjudicated gold, and the
tracked gate still refuses adoption. A final evaluation exit requires the
new holdout and an eligible boundary record.

## Phase 2 — Accuracy loop (exploratory, honestly)

This loop is **exploratory** and uses train/development data only. The
original 35 may guide changes because they are explicitly labeled as such.
A repeatedly consulted “holdout” is a development set, not a holdout; do not
use final holdout results to select the next prompt, threshold, registry, or
selector. Accuracy *claims* require the powered set in `decisions.md`.

One change per iteration — prompt, profile fields, segment context,
local-concept edit, or (only when real-world meaning cannot be represented)
a Rulespec term. Registry edits bump the metric version. Rerun the frozen
sample; stored provider output is reusable only for byte-identical requests.
Report by relation grade, role, and profile.

Before exposing final holdout labels, freeze one candidate configuration and
instantiate the exit bar with development-only estimates. Then evaluate the
holdout once: bar = max(trivial baselines computed on that same set —
always-abstain, lexical top-1 — , baseline + 2×bootstrap SE), tested
one-sided at α=0.05. A failed result is recorded as a failed adoption attempt;
its labels move to development before another configuration is designed.

## Attestation capacity (no standing human review)

There is no human review capacity; the earlier human-task ledger (7–10
sittings) is void. The system does what it can with pipeline-generated
metadata, **honestly graded**: every adjudication and attestation records
its machine attestor, and no output is ever presented as human-verified.
The protections against the repo's two prior oracle failures move from
human sittings to structure: different-family judge models, blind
protocols (a judge never sees tagger output), cross-family adjudication with
published agreement rates, and frozen digests. The current same-family
adjudication of 35 development items does not satisfy that gate.
Human validation arrives later
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
   Additional gate, **diagnostic not claimed**: project a sample of the
   generation (assignments, attestations, fragment URNs) to JSON-LD and
   run rulespec's own L2/L3 gates (`conformance_report.py`,
   `ci_validate.py`) — free machine checking of carriage invariants
   (URN grammar, attestation target joins, cross-property shapes) that
   nobody will hand-review. The public claim stays L0: L0 and L1+ are
   separate carrier paths and the audit rejects mixed claims; a public
   L1+ claim waits for a real RDF consumer (ledger trigger unchanged).
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
- Two consecutive development-set "no change" results → stop tuning;
  re-examine gold quality, scoring, and registry before the next change.
- Once final holdout labels are exposed, move that set to development before
  designing another configuration; never tune against it while retaining the
  holdout label.
