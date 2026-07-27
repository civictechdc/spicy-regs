# Rulespec MVP path

- **Date:** 2026-07-27. This revision restructures the 2026-07-26 testbed
  path into the MVP plan. Decisions moved to [`decisions.md`](decisions.md);
  execution history moved to
  [`evidence/testbed-execution-2026-07-26.md`](evidence/testbed-execution-2026-07-26.md).
- **Goal:** the smallest end-to-end Rulespec implementation over real Spicy
  Regs data: source → segments → concept assignments with Rulespec roles and
  exact evidence → reviewed scoring → atomically published tables conforming
  to Rulespec L0.
- **Method:** fix the measurement instrument first; iterate accuracy on a
  frozen sample with held-out confirmation; clean up in parallel; assemble
  the MVP only after the accuracy bar holds.
- **Sequencing supersedes** the 2026-07-26 second-half handoff. Its build
  steps 6–8 are absorbed here in reduced form (phase 4); its retrieval and
  parity gates stay parked per `decisions.md`.

## Where this leads (one paragraph, not authority)

Rulespec records — concepts, aliases, definitions, mappings, assignments,
evidence, corrections — are the system's memory across model calls; models
are interchangeable readers and writers of that memory. The long-term shape
is a cascade that uses the cheapest accurate method per task and reserves
frontier models for ambiguity. Every stage of that future is gated in
`decisions.md` and none of it is in the MVP.

## Current facts (2026-07-27)

- Identity layer: 9 published-table set, atomic snapshots, L0 conformance
  file; release fields deliberately null, publication blocked.
- Tag prototype: committed at `d3b8acb` on `feat/rulespec-testbed-loop`;
  **not yet integrated** into `feat/document-ai-pipeline` (phase 0).
- Honest baseline: F1 0.0851 on 35 gold artifacts / 34 unique labels —
  too small and too exact-label-scored to steer the loop (phase 1 fixes
  this before any tuning claims are made).
- Retrieval (step 5) is committed at `9591c6d` and parked. v2 `corpora`
  runners remain active as the benchmark harness until phase 4.
- Known defects queued in phase 3: `authority_edges` drops parsed
  `statute_at_large`/`executive_order`; U.S.C. lists collapse to one
  citation; Mirrulations attachment ordinals sort lexically; invalid-day
  date clamping; Rulespec release-preflight digest algorithm drifted from
  Rulespec's (`rulespec_release.py` can never match a real release); pinned
  digest stale (L0 audit 0/1); `validate_before_publish` unreachable under
  `--skip-upload`.

## Phase 0 — Consolidate (now)

1. ~~Commit the drafts~~ — done: `46e8206` (doc), `d3b8acb` (prototype),
   rulespec `03ac8ed` (TypeScript closure).
2. Integrate `d3b8acb` into `feat/document-ai-pipeline`: merge; keep
   diagnostic-only behavior; gold stays out of model input; run the tag and
   testbed tests.
3. This restructure: plan/decisions/evidence split, TODO-RULE pointer
   update, stale-handoff banner.

## Phase 1 — Fix the measurement instrument

1. Adjudicate the 35 gold assignments as exact / close / broader / narrower /
   related / wrong instead of normalized-label equality. This is the one
   human-review task of the iteration (see review budget).
2. Add Rulespec assignment roles (primary, substantive, mention, contextual)
   to the tag response schema; score roles separately.
3. Expand gold to ~150–300 artifact assignments: frontier-model first pass,
   stratified human spot-check. Define an artifact-level held-out split
   **before** any tuning and never tune on it.
4. Scoring rule: for an assignment with an adequate registered target, score
   candidate Recall@K and final identity; where the registry has no adequate
   target, score correct abstention and useful local-concept creation.
   (Only `medicaid` had a natural exact-label match; treating the rest as
   retrieval failures would recreate the old gold-label leakage.)

Exit: metrics recomputed under the adjudicated, expanded gold. That
re-baselined number — not F1 0.0851 — is the loop's starting point.

## Phase 2 — Accuracy loop (repeat)

One change per iteration — prompt instruction, profile field selection,
segment context, local-concept label/definition/alias/merge, or (only when
real-world meaning cannot be represented cleanly) a Rulespec term. Rerun the
frozen sample; reuse stored provider output only for unchanged requests.
Report by relation grade, role, and profile; confirm every claimed
improvement on the held-out split. Record Rulespec-relevant findings in one
short dated feedback report per iteration.

Exit bar: two consecutive iterations with held-out improvement and evidence
grounding stable at 1.0. Set the absolute accuracy target *after* phase 1
re-baselines the metrics; a bar chosen against a broken meter is theater.

## Phase 3 — Cleanup track (parallel with phases 1–2, mechanical)

1. The four data bugs (authority-edges fields, U.S.C. list splitting,
   Mirrulations ordinal sort, calendar validation), each with a regression
   test.
2. Rulespec pin repair: pin by commit + recorded digest until a real release
   exists; make the L0 mapping audit green against the current contract;
   align or retire the tarball-digest recomputation so the preflight can
   pass a real release when one is cut.
3. Make `validate_before_publish` fire under `--skip-upload` (or add an
   explicit local-publish mode that runs it), with a test proving it runs in
   position.
4. Doc sweep: banner the second-half handoff as superseded-in-sequencing;
   the APPROVE verdict is already date-scoped in the evidence file.
5. After phase 2's first completed iteration: execute the deletion
   authorization in `decisions.md` (migration-only fixtures,
   expected-difference files, legacy compatibility shims) in one reviewed
   commit.

## Phase 4 — MVP assembly (after the phase 2 exit bar)

1. Approval-lite: a human batch-review gate over a run's assignments —
   sampled review recorded as a disposition column on the assignment rows
   (approved / rejected / unreviewed), not a new subsystem. An AI model
   never approves its own output.
2. Materialize concept assignments (identity, role, evidence offsets,
   disposition, attestation) into the atomic snapshot set alongside the 9
   identity tables; update the L0 conformance mapping.
3. Local publish with every gate green (`validate_before_publish` now
   running in position).
4. Cutover: retire the v2 `corpora` runners per `decisions.md`, keeping the
   stored v4 outputs read-only as the benchmark artifact.

**MVP acceptance:** one locally published generation containing identity
tables + reviewed concept assignments, conforming to the pinned Rulespec
contract, every gate green, reproducible from receipts. Flipping to public
R2 additionally requires the Rulespec human gates (non-originating-consumer
review, maintainer-authorized release) — the first post-MVP milestone, and
the only items on the critical path that are not this repo's to do.

## Review budget

Human review is the scarce resource. Each iteration carries **exactly one**
human-review task; everything else is explicitly waived (relation-benchmark
oracle sealing — see `decisions.md`) or scheduled to a later phase
(promotion review, citation-bakeoff adjudication). A frontier model may
produce first-pass judgments anywhere, but a claimed accuracy improvement or
an approval disposition requires the human task of that iteration.

## Not in the MVP

Retrieval serving; relation-benchmark unblocking; GLinker, SetFit, cleanlab,
HDBSCAN, Splink, GLiREL, pySHACL (each gated in `decisions.md`); graph
engines and graph embeddings; signed receipts; the comments corpus; state
bills; new MCP surface. Exact parity with old manifests, full historical
replay, and provider-free recreation of old experiments are permanently
non-goals.

## Stop rules

- If work starts requiring retrieval, new storage formats, migration
  identities, or a generalized workflow engine, stop: scope has drifted.
- If the tag task cannot directly import existing prompt/schema/offset
  behavior, stop and identify the exact coupling; do not copy frameworks.
- If a rerun is less accurate, keep the result and use its errors; the
  testbed exists to make failures visible.
- If two consecutive iterations change nothing on held-out data, stop
  tuning and re-examine the instrument (gold quality, scoring, registry)
  before the next change.
