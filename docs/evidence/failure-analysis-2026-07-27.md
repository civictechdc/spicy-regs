# Failure analysis — 2026-07-27

Diagnosis and theory for every failure surfaced during the 2026-07-27
measurement-and-selector work. Sources: the adjudication rounds
(`gold-adjudication-2026-07-27/`), the ablation run (session record,
commits `2b91622`, `829b302`, `4ac2cdb`), the plan reviews, and the
evaluation-boundary correction (`a073926`). Each failure: symptom →
mechanism → theory → status.

## Layer 1 — Vocabulary: absence failures

- **Symptom:** 34/35 gold intents had no registered target (round 1).
- **Mechanism:** the 901-concept registry was harvested from topics
  *observed on ingested documents*, inheriting both the FR Thesaurus's
  coarse abstraction level and a sampling bias.
- **Theory:** the gold labels and the registry were authored at
  different granularities by different processes ("student loan
  forgiveness" vs "Loan programs—education"). Granularity mismatch is
  structural: it survives the fusion (presence went 1/35 → 8/35 exact,
  ~24/35 with defensible broader targets) and is why `broader` is the
  largest grade bucket.
- **Status:** presence largely fixed (fused registry); mismatch
  permanent → grade-aware scoring is permanent; never revert to
  exact-label equality.

## Layer 2 — Selector: silent operating-range failures

Four failures, one mechanism:

- v1's `allowed_schemes` gate admitted 936 of 513,236 rows (99.8%
  filtered) — the dominant round-2 cause, found only while building v2.
- Unanchored substring scoring: harmless at 901 aliases, catastrophic
  at 775k ("Ants" in "pollutants" scores 1.0) — noise floor scales
  with vocabulary size, signal does not.
- Dense channel read only the first 512 tokens of 1,200+-token
  segments — silent truncation, measured in the ablation run facts.
- Char-3-gram channel fed whole-segment term profiles into a recipe
  designed for short mention→name matching — zero targets retrieved on
  real segments.

**Theory:** components carry implicit operating ranges; a 500x input
shift breaks them *silently* because nothing asserts the assumption,
and all four sat upstream of any receipt. This is the repo's fail-closed
philosophy applied unevenly: invariants exist downstream (receipts,
checkpoints) but not inside scoring/selection code paths.
**Prediction:** the next silent failure lives wherever the next
order-of-magnitude shift happens (comments corpus, full-corpus runs) in
code tuned at today's sample sizes. Make assumptions executable
*before* the next scale jump.

## Layer 3 — Fusion arithmetic: priced tradeoffs

- RRF dilution: `judicial power` fell from fused rank 9 (two channels)
  to 29 (four) — channels that don't find a target average down the
  one that does. Known RRF property; weighted RRF/max-fusion are the
  mitigations if the price ever matters.
- Quotas cost that same target in the 2-channel config and were free at
  4 channels (richer pools fill quotas with real hits). Insurance
  priced at one target; the ablation made the price visible.

## Layer 4 — Gold: semantic failures

- Excerpts identifying a document without supporting its topic (the
  ABITRON party name "proving" trademark law).
- Single-label gold on dual-topic segments: the `immigration law` item
  is the *Hansen* case — an immigration statute under First Amendment
  attack; every channel and the model's own keywords say free speech,
  and neither answer is wrong.
- Exact-label scoring graded compatible concepts as failures (PFAS).

**Theory:** gold encodes the annotator's frame, not the text's content;
one label cannot carry a two-frame document. **Consequence for the new
holdout:** gold drafting must permit multi-label and role/frame
annotation or the same defect gets re-minted.

## Layer 5 — Measurement process: contamination failures

Three independent events: gold labels injected into the v2 runner's
registry (the disqualified 0.8857); single-family judges (correlated
priors, disclosed); and the 35 items becoming development data because
the registry sources and selector were tuned against their adjudication
(caught by the cross-model review, `a073926`).

**Theory:** information flows downhill from gold into every artifact it
touches — prompts, registries, selectors, fusion choices — and prose
discipline cannot stop it; only executable boundaries can. Note the
shape of the third event: the tagger was protected from gold rigorously
(payload bans, leak tests) while the *harness* got contaminated,
because no boundary was drawn around "things tuned using gold."
Contamination is the default state of a fast iteration loop.
**Status:** executable boundary installed (`evaluation-boundary.json`,
`--require-adoption-ready` red until an untouched, cross-family,
alias-disjoint holdout exists).

## Layer 6 — The tagger: mostly not a failure (the quiet headline)

F1 0.0851 decomposes almost entirely into instrument: identity ceiling
≈ 1/35 (registry absence), scorer punished compatible concepts, several
gold items defective, selector couldn't surface what existed. Actual
model sins so far: over-generation without an abstention prior (351
accepted candidates, 260 counted false positives — many of which
adjudication would grade broader-or-close), and a prompt seesaw inside
statistical noise (McNemar p≈0.06). **No measurement run to date has
been capable of detecting model capability.** Every "model improvement"
attempted before the instrument works would have tuned against noise.

## Meta-pattern

Every failure that mattered was caught by a *different mechanism than
the one that produced it*: plan reviewers caught the two approval-design
contract misreadings; the ablation caught the channel pathologies; the
adjudication caught the gold defects; a different model family caught
the harness contamination the operator had rationalized. Checker
diversity is the working control; the blind spot is always whatever the
current operator is optimizing.

## What each theory prescribes

1. Granularity mismatch → grade-aware scoring permanent.
2. Operating ranges → assert assumptions executable before the next
   scale jump.
3. Contamination-by-default → the new holdout is drawn under the
   executable boundary, cross-family adjudicated, and nothing
   downstream of it is ever tuned on it.
4. Instrument-before-model → no accuracy claims until the holdout
   exists; today a good tagger and a bad one are indistinguishable.
