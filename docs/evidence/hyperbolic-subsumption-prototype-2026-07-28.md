# Hyperbolic subsumption scorer — zero-shot prototype, 2026-07-28

Ledger item 2 of `hierarchy-embedding-research-2026-07-27.md` proposes a
HiT-lineage hyperbolic scorer to replace the LLM judges' broader/narrower
calls, and sets the adoption gate at **agreement with the judges ≥ their own
self-consistency (31/35 grades = 88.6%, 34/35 adequacy = 97.1%)**.

**Verdict: FAILS.** Three pretrained HiT checkpoints, evaluated zero-shot and
again with split-half calibration, all land below the gate *and below a
constant predictor*. The bounded fine-tune (step 3) was skipped; the reason is
in [Why no training run](#why-no-training-run) and it is not "the number was
close".

Nothing was integrated. No `pyproject.toml` / `uv.lock` change; the dependency
runs out-of-tree.

## Reproduction

```
uv run --python 3.11 --with hierarchy_transformers --with pyarrow --no-project \
    python tools/prototype_hyperbolic_subsumption.py \
    --model-id Hierarchy-Transformers/HiT-MiniLM-L12-WordNetNoun \
    --report-path /tmp/hit-report.json
```

Hermetic half (pair construction, scoring algebra, decision rule, calibration,
metrics) runs with no optional dependency:
`python tools/prototype_hyperbolic_subsumption.py --self-test`.

| | |
| --- | --- |
| Package | `hierarchy_transformers` 0.1.1 (KRR-Oxford), `sentence-transformers` 5.6.1, `transformers` 5.14.1, `torch` 2.13.0, `geoopt` 0.5.0 |
| Python | 3.11.15 |
| Device | Apple M4 Pro (arm64), MPS |
| Paper | He, Yuan, Chen, Horrocks, *Language Models as Hierarchy Encoders*, NeurIPS 2024 (arXiv 2401.11374) |

### Models (id + pinned revision sha)

| Model | Revision | Base | Hierarchy |
| --- | --- | --- | --- |
| `Hierarchy-Transformers/HiT-MiniLM-L12-WordNetNoun` | `b170cbfa5bb770f144c69f75f826ccbd6e0c7b53` | all-MiniLM-L12-v2 | WordNet noun hypernyms |
| `Hierarchy-Transformers/HiT-MPNet-WordNetNoun` | `733a89bb4487d49304c976fdadb654ba1ecfb244` | all-mpnet-base-v2 | WordNet noun hypernyms |
| `Hierarchy-Transformers/HiT-MiniLM-L12-SnomedCT` | `f822e4351af6cab84e66cf673f29a801e426eafe` | all-MiniLM-L12-v2 | SNOMED CT |

All four published checkpoints were enumerated from the HF API; the fourth
(`HiT-MiniLM-L6-WordNetNoun`) is a strictly smaller sibling of the first and
was not run. Revisions are pinned by commit sha, not by `main`, matching the
Sentence Transformers adapter's convention.

## Evaluation set

Rebuilt from the frozen adjudication records by `load_graded_pairs()`:
`resolved.json` (round 1, 901-row registry) + `resolved-fused.json` (round 2,
513,236-row fused registry), joined to `gold_spans.parquet` for the gold label
text and to both registries for the candidate `pref_label` / `alt_labels_json`.

- **69 graded pairs**: 34 from round 1 + 35 from round 2.
- **1 excluded**: round-1 `gold_9699f26de500ef0bce70b53c` (tariff suspension)
  was graded `wrong` with `best_candidate_id: null`. There is no pair to score
  and inventing one would be a fabricated datum.
- All 29 distinct candidate concepts have empty `alt_labels_json`, so the
  alias-concatenation path is exercised but contributes nothing here.

Both rounds are kept because they are separate adjudications. 31 pairs are
byte-identical across rounds; the judges gave **3 of those 31 a different grade
in round 2** (independent dispute resolution / Health insurance, hazard
communication / Hazardous substances, health information privacy / Health —
all `related` → `broader`). That 28/31 = 90.3% cross-round stability is an
independent read on the same judge noise the 31/35 bar encodes.

### Truth distribution after the ledger's reduction

`broader→subsumes`, `narrower→subsumed_by`, `exact|close→equivalent`
(mutual proximity), `related|wrong→neither`.

| Relation | Grades | n | Share |
| --- | --- | ---: | ---: |
| subsumes | broader 44 | 44 | 63.8% |
| neither | related 12, wrong 1 | 13 | 18.8% |
| equivalent | close 8, exact 2 | 10 | 14.5% |
| subsumed_by | narrower 2 | 2 | 2.9% |

**This skew is the headline problem.** A constant "subsumes" predictor scores
63.8% four-way and 78.3% three-way with no model at all, and among the 46
*directional* pairs a constant "subsumes" scores 44/46 = 95.7%.

## Decision rule

The package publishes no `(centri_weight, threshold)` constant.
`HierarchyTransformerEvaluator` grid-searches both on a validation set and
`scripts/evaluation/hit/eval_hit.py` applies the winners to test data. The
score is fixed by the paper and copied verbatim into `subsumption_score()`:

```
score(child, parent) = -( d_H(child, parent) + w * (||parent||_H - ||child||_H) )
predict "child ⊑ parent"  iff  score > τ
```

Applied in **both directions** on each (gold label, candidate concept) pair and
read as a 2×2, which is where `equivalent` comes from without a second rule:

```
fwd = score(child=gold,      parent=candidate)   # candidate subsumes gold
rev = score(child=candidate, parent=gold)        # candidate subsumed by gold

fwd & rev → equivalent      fwd only → subsumes
rev only  → subsumed_by     neither  → neither
```

Both calibrations required by the brief are reported, always labelled:

- **native (genuinely zero-shot on our data).** `(w, τ)` grid-searched for best
  F1 on the model's *own* validation split,
  `Hierarchy-Transformers/WordNetNoun` / `MixedHop-RandomNegatives-Pairs`,
  seeded 20,000-pair subsample (seed 20260728). No graded pair is touched.
  Sanity check on the calibration itself: it reproduces the paper's regime —
  val F1 0.894 (MiniLM), 0.901 (MPNet); 0.686 for the SNOMED model, which is
  transfer, as expected.
- **split-half.** Two-fold, stratified by truth relation (necessary at n=69
  with two `subsumed_by` examples). `(w, τ)` fitted for maximum four-way
  agreement on one half, scored on the held-out half, folds swapped. Only
  held-out numbers are reported.
- **oracle upper bound.** `(w, τ)` fitted on all 69 pairs and scored on the
  same 69. Not a result — a ceiling on what this decision rule can express.

## Results

Bar: **88.6%** grade agreement (31/35), **97.1%** adequacy (34/35).

| Model | native 4-way | native 3-way | native adequacy | split-half held-out 4-way | split-half held-out 3-way | oracle 4-way | sanity |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| HiT-MiniLM-L12-WordNetNoun | 24/69 **34.8%** | 26/69 37.7% | 59/69 85.5% | 32/69 46.4% | 48/69 69.6% | 50.7% | 15/20 75% |
| HiT-MPNet-WordNetNoun | 26/69 **37.7%** | 32/69 46.4% | 57/69 82.6% | 29/69 42.0% | 49/69 71.0% | 47.8% | 13/20 65% |
| HiT-MiniLM-L12-SnomedCT | 30/69 **43.5%** | 46/69 66.7% | 51/69 73.9% | 39/69 56.5% | 51/69 73.9% | 59.4% | 14/20 70% |
| *constant predictor* | *63.8%* | *78.3%* | *85.5%* | *63.8%* | *78.3%* | *63.8%* | — |
| **bar** | **88.6%** | **88.6%** | **97.1%** | **88.6%** | **88.6%** | — | — |

Every cell is below the bar, and every cell is below the constant predictor —
including the oracle, which is fitted on the very pairs it is scored on. The
adequacy branch is worse than it looks: MiniLM's 59/69 is *exactly* the score
of always answering "inadequate" (59 of 69 pairs are inadequate). It carries no
information, and the bar is 97.1%.

Native calibration selected `w=2.2, τ=-11.92` (MiniLM), `w=1.4, τ=-7.28`
(MPNet), `w=1.2, τ=-21.61` (SNOMED).

### Confusion matrix — HiT-MiniLM-L12-WordNetNoun, native, four-way

Rows = judge truth, columns = geometry prediction.

| truth ↓ / pred → | subsumes | subsumed_by | equivalent | neither | n |
| --- | ---: | ---: | ---: | ---: | ---: |
| **subsumes** | 9 | 2 | 2 | **31** | 44 |
| **subsumed_by** | 0 | 0 | 0 | 2 | 2 |
| **equivalent** | 0 | 0 | 2 | 8 | 10 |
| **neither** | 0 | 0 | 0 | 13 | 13 |

The rule abstains on 54 of 69 pairs. The `neither` row scoring 13/13 is that
abstention, not a discrimination.

**The mechanism is the centripetal term collapsing, not the distance term.**
The `norm_gap_transfer` block of the report measures our pairs against the
model's own validation positives on the two quantities the rule is made of.
The hyperbolic *distances* transfer fine — it is the depth separation that does
not:

| | n | median d | median norm gap (child − parent) | p10 gap | p90 gap |
| --- | ---: | ---: | ---: | ---: | ---: |
| WordNet val, true child⊑parent | 1,809 | 22.09 | **7.47** | 3.06 | 10.59 |
| WordNet val, negatives | 18,191 | 32.02 | 0.12 | −2.87 | 3.40 |
| our 44 true `subsumes` pairs | 44 | 22.62 | **1.33** | −0.16 | 4.10 |

Our true pairs sit at the *negative* distribution's depth gap, not the
positive's. At the median distance the native rule (`w=2.2, τ=-11.92`) needs a
gap of 4.62 on WordNet and 4.86 on our pairs — near-identical requirements.
WordNet positives clear it **1,628/1,809 (90.0%)**; our true `subsumes` pairs
clear it **11/44 (25.0%)**. The encoder simply does not place `poultry inspection` much
deeper in the Poincaré ball than `Agriculture` (gap 3.30, one of the larger
ones), and places `religious accommodation` *shallower* than `Equal employment
opportunity` (gap −1.96). The depth signal that carries WordNet hypernymy
attenuates to roughly a fifth of its magnitude on regulatory subject phrases,
while the distance term is unchanged — so the centripetal term can no longer
lift true pairs over τ.

### Confusion matrix — HiT-MiniLM-L12-WordNetNoun, split-half held-out, four-way

Calibrating on our own data drops τ far enough (−24.09 / −21.75) that the rule
stops abstaining. It then over-predicts in the other direction.

| truth ↓ / pred → | subsumes | subsumed_by | equivalent | neither | n |
| --- | ---: | ---: | ---: | ---: | ---: |
| **subsumes** | 24 | 4 | 12 | 4 | 44 |
| **subsumed_by** | 1 | 0 | 1 | 0 | 2 |
| **equivalent** | 4 | 0 | 6 | 0 | 10 |
| **neither** | 10 | 1 | 0 | 2 | 13 |

`subsumed_by` is never once predicted correctly by any model in any
configuration — there are only two such pairs, which is itself a statement
about the evaluation set.

### Threshold-free direction probe

Strips the threshold out entirely: on the 46 directional pairs, does the norm
ordering alone (`||gold|| > ||candidate||` ⇒ candidate is the more general one)
name the right direction? This isolates "the geometry knows which entity is
more general" from "the calibrated threshold transfers", and depends on no
hyperparameter.

| Model | direction correct | accuracy | constant "subsumes" |
| --- | ---: | ---: | ---: |
| HiT-MiniLM-L12-WordNetNoun | 34/46 | 73.9% | **95.7%** |
| HiT-MPNet-WordNetNoun | 38/46 | 82.6% | **95.7%** |
| HiT-MiniLM-L12-SnomedCT | 34/46 | 73.9% | **95.7%** |

The geometry is well above a coin flip (50%) and well below the trivial answer.
That gap is the finding: the encoder *does* carry hierarchical direction
signal, but on a set where the judges' best candidate is a broader thesaurus
term 44 times out of 46, a direction call has to be near-perfect to be worth
making, and it is not.

## Sanity pairs (HiT-MiniLM-L12-WordNetNoun, native rule)

15/20 correct. The pattern in the failures matters more than the rate.

| gold | candidate | truth | predicted | d | ‖gold‖ | ‖cand‖ | |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| dog | animal | subsumes | subsumes | 8.38 | 18.46 | 13.83 | ok |
| animal | dog | subsumed_by | subsumed_by | 8.38 | 13.83 | 18.46 | ok |
| sedan | car | subsumes | subsumes | 12.25 | 19.84 | 17.51 | ok |
| oak | tree | subsumes | subsumes | 17.65 | 21.11 | 12.69 | ok |
| tree | oak | subsumed_by | subsumed_by | 17.65 | 12.69 | 21.11 | ok |
| mountain bike | bicycle | subsumes | subsumes | 14.41 | 21.96 | 17.49 | ok |
| bicycle | mountain bike | subsumed_by | subsumed_by | 14.41 | 17.49 | 21.96 | ok |
| copper | metal | subsumes | subsumes | 14.13 | 22.03 | 18.11 | ok |
| wetlands permitting | Environmental protection | subsumes | subsumes | 15.41 | 21.91 | 18.66 | ok |
| Environmental protection | wetlands permitting | subsumed_by | subsumed_by | 15.41 | 18.66 | 21.91 | ok |
| Medicaid | Medicaid | equivalent | equivalent | 0.00 | 21.69 | 21.69 | ok |
| attorney | lawyer | equivalent | equivalent | 5.55 | 17.72 | 17.01 | ok |
| physician | doctor | equivalent | equivalent | 3.29 | 17.42 | 18.14 | ok |
| piano | stapler | neither | neither | 25.44 | 19.18 | 22.35 | ok |
| rainfall | bank account | neither | neither | 28.61 | 19.76 | 20.40 | ok |
| PFAS | Hazardous substances | subsumes | **neither** | 25.34 | 22.47 | 17.89 | miss |
| oranges and grapefruit | Oranges | subsumed_by | **neither** | 17.91 | 22.88 | 22.02 | miss |
| car | vehicle | subsumes | **equivalent** | 4.91 | 17.51 | 16.28 | miss |
| vehicle | car | subsumed_by | **equivalent** | 4.91 | 16.28 | 17.51 | miss |
| dog | cat | neither | **equivalent** | 9.40 | 18.46 | 18.75 | miss |

The checkpoint behaves correctly on WordNet-shaped nouns, including the
domain-flavoured `wetlands permitting` ⊑ `Environmental protection` in both
directions. It breaks on exactly the things our data is made of: an acronym
outside the training vocabulary (`PFAS`, ‖·‖ pushed to 22.47 with d=25.34 to
its true parent), a coordinated compound (`oranges and grapefruit`), and
near-synonym/sibling pairs where the mutual-proximity test cannot separate
`car`/`vehicle` from `dog`/`cat`. The last is not a tuning artifact: `dog`/`cat`
(d=9.40, norm gap 0.25) is geometrically *tighter* than `car`/`vehicle`
(d=4.91, gap 1.23) is loose, so no `(w, τ)` orders them correctly.

## Why no training run

The brief authorises a ≤60-minute MPS fine-tune "only if zero-shot is promising
but below the bar". Zero-shot is not below the bar in the promising sense — it
is below a model-free constant predictor, in every configuration, on every
checkpoint. Four specific reasons not to spend the hour:

1. **The ceiling is already measured and it is below baseline.** The oracle row
   fits `(w, τ)` on the same 69 pairs it scores, and still reaches only
   47.8–59.4% against a 63.8% constant. Fine-tuning changes the embedding, not
   the fact that a two-parameter threshold over (distance, norm gap) is being
   asked to reproduce a six-way judge grade. A better encoder raises the
   geometry; it does not change what the rule can express.
2. **The threshold-free probe says the binding constraint is the label
   distribution, not the encoder.** 73.9–82.6% direction accuracy against a
   95.7% trivial answer. Improving the encoder to, say, 90% direction accuracy
   would still lose to "always broader". No amount of FAST training moves the
   95.7%.
3. **The evaluation set cannot certify anything at this bar.** 69 pairs, of
   which 2 `subsumed_by` and 10 `equivalent`. One flipped pair is 1.4 points;
   the gate is 88.6%. And the judges themselves flipped 3 of the 31 pairs they
   graded twice.
4. **The boundary forbids the verdict anyway.** `evaluation-boundary.json`
   marks all 35 items permanently train/development-only. Training against them
   and reporting the number would be fitting the set, which is the failure mode
   the boundary exists to prevent.

Training data availability was verified so this is a decision, not an excuse:
`FASTTopical.nt.zip` yields **142,475 `skos:broader` edges in the first 4M
lines alone** (plus 366,855 `skos:prefLabel` and 201,638 `skos:altLabel`),
which is ample for the HiT recipe. It remains available for the run described
below.

## Honest verdict

**FAILS.** As specified — a geometric replacement for the judges' relation
grade on a pre-selected best candidate — the HiT lineage does not clear
88.6%, does not clear 63.8%, and does not clear "always answer broader".
Ledger item 2 should not proceed to adoption on this evidence.

One qualification against over-reading it: this evaluation asked the lineage to
do something it does not claim. The Nov-2025 result the ledger cites
(*Hierarchical Retrieval with OOV Queries: SNOMED CT*, arXiv 2511.16698) is a
**retrieval** result — place free text in the space, retrieve its most specific
subsumers from a large vocabulary. It is not a six-way grading result on a pair
someone else already chose. The negative here is solid for the grading framing
and says little about the retrieval framing.

### What would change the verdict

1. **Re-target to retrieval, which is the published task.** Score "given a gold
   label, retrieve its most specific subsumers from the 513k fused registry"
   against recall@k, alongside the existing lexical and dense channels. That is
   the claim the paper makes and the one the selector actually needs — the
   round-2 root-cause note already identifies candidate selection, not
   grading, as the binding constraint. This is the recommended next move.
2. **A holdout where the trivial answer is not 78–96%.** An untouched,
   cross-family-adjudicated set with real `narrower`, `related`, and `wrong`
   mass. Until "always broader" stops scoring in the nineties on directional
   pairs, no scorer of any kind can demonstrate value here.
3. **Domain fine-tune, then re-run both probes.** The FAST edges are present
   and parseable, and the failure has a specific, trainable shape: the
   centripetal depth gap collapses from 7.47 to 1.33 on our phrases while
   distances transfer intact. Restoring depth separation on regulatory
   vocabulary is exactly what training on `skos:broader` edges does, and the
   norm-gap table above is the metric to watch — it is measurable without any
   graded pair. Worth doing *after* (1) and (2), because without them the
   downstream result is unmeasurable.
4. **A richer decision rule.** `(w, τ)` over (distance, norm gap) is the
   package's rule, not the only one. A small classifier over the same geometric
   features — which is ledger item 3's shape — could express things the
   two-parameter threshold cannot. Its ceiling should be measured before any
   more training compute is spent.

## Provenance

- Script: `tools/prototype_hyperbolic_subsumption.py` (this commit).
- Oracle: `docs/evidence/gold-adjudication-2026-07-27/resolved.json`,
  `resolved-fused.json`, `README.md`; boundary `evaluation-boundary.json`.
- Registries: `output/segmentation-tagging-document-openai-structure-overlap-1800-v4/tagging_input_registry.parquet`
  (901 rows), `output/fused-concept-registry-v1/registry.parquet` (513,236 rows,
  sha256 `a82cdebc…`).
- Gold: `output/segmented-real-data-evaluation-v2/gold_spans.parquet` (35 rows).
- Machine-generated evidence. No human verification of any number here.
