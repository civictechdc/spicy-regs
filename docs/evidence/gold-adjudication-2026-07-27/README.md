# Gold adjudication record — 2026-07-27

Blind machine adjudication of the 35 stored gold artifact assignments
against the gold-free 901-concept registry (MVP plan phase 1.1).

> **Evaluation boundary:** all 35 items are permanently
> **train/development-only**. They have been inspected, adjudicated, and used
> to diagnose and tune registry/selector behavior. “Frozen” below means the
> recorded files are immutable; it does not mean untouched holdout. The
> tracked gate is `evaluation-boundary.json`, and it refuses an accuracy or
> adoption verdict until a separate untouched, cross-family-adjudicated
> holdout exists.

## Attestation facts

- **Judges:** three independent `claude-fable-5` subagent sessions (A, B,
  C), no shared context, launched from the working session. Machine
  adjudication — no output here is human-verified.
- **Family note:** the judge family differs from the tagger family
  (`gpt-5.6-sol`), satisfying "a model never grades its own family's
  output." Judges A and B are the *same* family as each other, so the
  agreement rate below measures within-family judge consistency, not
  cross-family consensus.
- **Blind protocol:** judges saw only the adjudication input file (gold
  label, exact evidence excerpts, artifact identity, and the fixed top-12
  candidates computed by the production selector). No tagger output, no
  diagnostic-run data, no other judge's verdict. Judge B read candidate
  lists bottom-up to break shared anchoring. Judge C tiebroke one item
  without seeing A's or B's verdicts.

## Frozen inputs

- Registry: `tagging_input_registry.parquet`, sha256 `f338b7c8a1e6aae1…`,
  901 rows (hash-matched to the gold-free iteration-2 run's plan).
- Gold: `output/segmented-real-data-evaluation-v2/gold_spans.parquet`,
  sha256 `8989994441e94f98…`, 35 rows / 34 unique labels.
- Adjudication input: sha256
  `3ef68ec7b18ccb802f41f07107da6dad6fe9a72a3769c34f022f9cd05126f8ab`,
  built by `tools/build_gold_adjudication_input.py` (commit `bb71c95`);
  selector parity 35/35 against the production payload path.

## Agreement

- Grade agreement (A vs B): **34/35 (97.1%)**.
- Adequate-target agreement (A vs B): **35/35 (100%)**.
- One grade disagreement (`…46af63a0…`, poultry inspection: related vs
  broader) resolved **broader** by 2-of-3 with Judge C.
- Best-candidate divergence within an agreed grade on 3 items (all in
  inadequate grades, so no branch effect): `…0b65e538…`, `…73e66810…`,
  `…46af63a0…` (C preferred "Safety" over the majority "Agriculture").

## Resolved distribution (35 items)

| Grade | Count | Share |
| --- | ---: | ---: |
| exact | 1 | 2.9% |
| close | 4 | 11.4% |
| broader | 20 | 57.1% |
| narrower | 1 | 2.9% |
| related | 8 | 22.9% |
| wrong | 1 | 2.9% |

**Adequate registered target (exact or close): 5/35 (14.3%), unanimous.**
The five: medicaid (exact), trademarks, immigration, science & technology,
fisheries (close). The single `wrong`: tariff/customs — no candidate bears
any meaningful relation. The 30 inadequate items are the abstention branch:
correct behavior is abstain-or-local-concept, and 20 of them additionally
have a defensible *broader* registry assignment available.

## What this changes

The prior exact-label baseline (F1 0.0851) measured registry coverage, not
model quality: 24/35 items (exact+close+broader) have a defensible
assignment the exact scorer counted as failure. Re-baselined scoring must
score the 5 adequate items on identity, the 30 others on abstention and
local-concept quality, and may report broader-assignment availability
separately. Per-item resolved grades: `resolved.json`; raw verdicts:
`judge-a.json`, `judge-b.json`, `judge-c.json`.

## Round 2 — fused registry, frozen selector (2026-07-27)

Same protocol, same 35 items, same frozen selector
(`lexical-overlap-v1`, limit 12); the only change is the registry:
`fused-concept-registry-v1` (513,236 rows, sha256 `a82cdebc…`; FR
Thesaurus enriched + crs-subjects + crs-policy-areas + epa-tsca +
fast-topical; built by `tools/fuse_concept_registries.py`, commit
`ce0a2e7`). Judges A2/B2 + tiebreak C2, agreement 31/35 grades,
34/35 adequacy; all four splits resolved 2-of-3.

| Measure | Round 1 (901 rows) | Round 2 (513,236 rows) |
| --- | ---: | ---: |
| exact | 1 | 1 |
| close | 4 | 4 |
| broader | 20 | 24 |
| narrower | 1 | 1 |
| related | 8 | 4 |
| wrong | 1 | 1 |
| **adequate (surfaced)** | **5/35** | **5/35 — identical items** |
| exact-alias present in registry (mechanical) | 1/35 | **8/35** |

**Finding:** the fusion fixed registry coverage (7 new exact-alias
targets: poultry inspection, immigration law, judicial power, human
rights, free speech, fisheries management, Army Corps of Engineers —
none surfaced) and improved candidate quality at the margins (4
related→broader), but surfaced adequacy did not move because the
frozen selector's unanchored substring scoring floods the top-12 with
short-label noise at 513k rows. The binding constraint moved from the
registry to the selector. Next single-change iteration: anchored/
embedding candidate selection, then re-run this protocol.

Verdicts: `judge-a2-fused.json`, `judge-b2-fused.json`,
`judge-c2-fused.json`, `resolved-fused.json`.

### Round-2 root-cause correction (2026-07-27, selector-v2 build)

The round-2 finding attributed non-surfacing to substring noise. Building v2
exposed a more basic defect: `fused-concept-registry-v1` used `scheme` for
two different dimensions. The original 901 rows stored the semantic
`subject` facet, while imported rows stored vocabulary names such as
`crs-subjects` and `fast-topical`. The profile's valid `subject` facet gate
therefore admitted only 936 of 513,236 rows. Removing the gate would hide the
schema error and could offer regulated entities to subject-only profiles.

The corrected shape keeps the gate and separates the dimensions:
`facet=subject|regulated_entity` controls tag policy;
`source_vocabulary` controls authority identity, provenance, and quotas; the
deprecated compatibility `scheme` mirrors `facet` on new rows.
`anchored-hybrid-v2` now runs through the real tag payload, strict response
schema, facet gate, and 8,192-token input budget. Same-label concepts from
different vocabularies remain separately selectable. The fusion may record an
unreviewed mapping for later review, but it never chooses a representative or
implies `skos:exactMatch`.

The earlier 1/8 → 4/8 (5/8 without quotas) result remains a useful
development observation about the old artifact. It is not a current
accuracy result and cannot trigger adoption because this entire 35-item set
was already used to design the selector.
