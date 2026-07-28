# USearch ANN index for the dense concept channel — 2026-07-28

Does an approximate nearest-neighbour index serve the dense concept-candidate
channel (channel C) as well as the exact brute-force cosine currently
implemented, at materially lower cost?

**Verdict: REJECT as a swap today.** Not because USearch performs badly — it
works, it is fast, and it builds in minutes — but because the measured
tradeoff has no operating point that is both cheap and correct. Memory and
recall move together almost linearly across seven configurations. The only
configuration that reproduces the exact search's 8-target oracle exactly
(`usearch-f32-hi`) holds **1,617 MB** resident against the exact search's
**1,697 MB** — a 4.7% saving. Every configuration that saves real memory loses
between 21% and 74% of the exact top-12, and the losses reach the shortlist a
tagging decision is made from.

**Chasing the low recall upstream found two facts about the registry and one
retracted conclusion.** The facts: 100% of concepts carry one of 8 boilerplate
definition templates, which is 74% of the median concept's embedding input; and
99.6% of the registry is off-target vocabulary. The retraction: this document
originally concluded from two geometry statistics that the concept embedding
space was "near-degenerate." **Both statistics were non-diagnostic** — one used
the wrong null, the other is normal for every embedding model ever measured. See
[the correction](#correction-2026-07-28-after-external-review). The corrected
best-vs-random separation is **+0.2173**, not 0.029.

Read the 3-of-8 dense-alone result accordingly: it is recall@12 ≈ 37.5% over
513,236 labels with no training data, which is **at or above every published
zero-shot number at this scale**. Retrieval is at its ceiling, not broken. The
lever is a smaller purpose-built vocabulary plus real supervision — not geometry.

Nothing was adopted. `candidate_channels.py` is untouched; the ANN path is a
new, optional, unwired module.

## Reproduction

```
uv run --frozen --extra ann --extra embed python tools/benchmark_usearch_index.py \
    --output-dir <dir> --work-dir <dir> --repeats 3
```

The first run loads the frozen testbed (about seven minutes) and embeds the 35
segments once into a setup cache; later runs reuse it and need neither the
encoder nor the testbed loader. **No re-embedding happens at any point** — every
configuration is built from the existing cached `.npz`.

The embedding-space audit behind
[the registry finding](#the-registrys-own-text-is-what-flattens-it):

```
uv run --extra embed python tools/audit_concept_embedding_space.py \
    --setup-cache <setup-cache.json> --sample 20000 --seed 0 --report <path.json>
```

Hermetic tests, no optional dependency required:
`pytest tests/test_ontology_ann_index.py tests/test_audit_concept_embedding_space.py`.
With the extra: `pytest tests/test_ontology_ann_index_real.py`.

| | |
| --- | --- |
| Package | `usearch` 2.26.0 (+ `numkong` 7.7.0), SIMD backend `neon` |
| Python | 3.10.19 |
| Machine | Apple M4 Pro (arm64), 14 cores, 48 GB RAM, macOS 26.6 |
| Embedder | `BAAI/bge-base-en-v1.5` @ `a5beb1e3e68b9ab74eb54cfd186867f64f240e1a` |
| Registry | `output/fused-concept-registry-v1/registry.parquet`, sha256 `a82cdebc…bba6c`, 513,236 concepts |
| Registry embedding digest | `7f6728d6e…06724f` |
| Cached vectors | 513,236 × 768 float32, 1,642,357,060 bytes |

New code: `src/spicy_regs/ontology/ann_index.py`,
`tools/benchmark_usearch_index.py`, two test files, and an `ann` optional
extra in `pyproject.toml` / `uv.lock`.

## Charter answers

1. **Decision it changes.** Whether the dense channel is operationally viable as
   currently served — a 1.64 GB in-memory float matrix with a full
   513,236 × 768 matmul per query.
2. **In / out.** In: the cached concept vectors and the 35 frozen development
   segments' query embeddings. Out: build time, index size, query latency, peak
   RSS, recall against exact, and the 8-target oracle.
3. **Step under test.** Candidate generation, dense channel only. Everything
   else frozen: same registry, same vectors, same fusion, same quotas.
4. **Evidence identifying it.** The candidate-selection research's ops warning —
   library-scale vocabularies at ">34 GB RAM / ~28 min load … build offline,
   memory-map" (`candidate-selection-research-2026-07-27.md`).
5. **Simplest credible baseline.** The exact brute-force cosine already
   implemented (`DenseConceptMapper`). It is **ground truth by construction**,
   so ANN quality is measured, not estimated.
6. **Single variable.** The search. Both arms are driven by *identical*
   precomputed query vectors through their own real `rank` implementations.
7. **Measure matching the failure.** recall@12 against exact (12 is the prompt
   shortlist length, so a loss there can reach a tagging decision), plus the
   8-target oracle through the ablation harness's own scorer.
8. **Adopt / reject rule.** Adopt only if a configuration both preserves the
   oracle and delivers a material cost reduction. See the verdict.
9. **Can this dataset support the decision?** For the *cost* question, yes.
   For any accuracy claim, **no** — the 35 items are permanently
   development-only. The drawn holdout was not touched.
10. **Confirming the component gain improves the user result.** Unmeasurable
    until MVP phase 4 publishes assignments. Stated, not proxied.
11. **Pins.** `registry_sha256 a82cdebc…`, embedding digest `7f6728d6e…`,
    model revision `a5beb1e3…`, `usearch==2.26.0`. This document is the record;
    no `decisions.md` entry — adoption is the maintainer's call.

## Configuration, cost, and recall against the exact search

35 queries, 3 timed passes, latency measured at depth 50, search only (the
shared query embedding is precomputed, so the encoder's cost cannot hide the
difference under test). Peak RSS is measured in a **fresh subprocess per arm** —
it is a whole-process high-water mark and cannot be read honestly from inside a
process that already loaded the 1.64 GB baseline.

| Configuration | Storage | c / ea / ef | Build (s) | On disk (MB) | Peak RSS mmap (MB) | Peak RSS loaded (MB) | Mean (ms) | p95 (ms) | recall@50 | recall@12 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| exact-brute-force | f32 exact | — | — | 1,566 | — | 1,696.7 | 16.32 | 22.29 | 1.0000 | 1.0000 |
| usearch-i8 | i8 | 16/128/64 | 19.6 | 449 | **461.9** | 700.8 | 0.18 | 0.27 | 0.2531 | 0.2619 |
| usearch-f16 | f16 | 16/128/64 | 58.4 | 825 | 525.1 | 1,074.5 | 0.34 | 0.57 | 0.4217 | 0.4548 |
| usearch-f32 | f32 | 16/128/64 | 92.4 | 1,576 | 579.8 | 1,826.9 | 1.13 | 1.00 | 0.4240 | 0.4619 |
| usearch-f16-ef256 | f16 | 16/128/256 | — | 825 | 729.1 | 1,075.9 | 1.06 | 1.54 | 0.5354 | 0.5310 |
| usearch-f16-c32 | f16 | 32/256/128 | 86.0 | 887 | 791.9 | 1,139.5 | 0.71 | 1.01 | 0.5971 | 0.6071 |
| usearch-f16-hi | f16 | 48/512/512 | 183.1 | 950 | 1,106.9 | 1,199.8 | 3.25 | 4.83 | 0.7817 | 0.7881 |
| usearch-f32-hi | f32 | 48/512/512 | 268.1 | 1,702 | 1,616.5 | 1,953.3 | 4.70 | 6.33 | **0.9629** | **0.9690** |

`c / ea / ef` = connectivity / expansion_add / expansion_search. `usearch-f16-ef256`
reuses `usearch-f16`'s graph and changes only the query-time search width, so it
has no build cost of its own. Recall is macro-averaged over the 35 queries.

**Memory and recall are the same dial.** Sort the table by either column and you
get the same order. That is the finding.

The ~50-minute figure sometimes attached to this channel is the **embedding**
build. It is unchanged by any of this: the ANN path reuses the cached vectors
and never re-embeds. Graph construction is genuinely cheap — 20 s to 4.5 min.

## The eight-target oracle

Computed through `tools/ablate_candidate_selectors.py`'s own
`segment_channels` / `measure_configuration`, not a second scoring
implementation. Channels A, B and E are computed once and shared; only channel
C is recomputed per configuration — the single variable.

The exact-baseline row **reproduces the published ablation exactly** (C-alone
3/8, v2+C 4/8, BM25+B+C 2/8, and the same 4/5 and 2/5 adequate counts as
`output/candidate-selector-ablation-*-2026-07-27/ablation.json`). That is the
control: the harness reuse is faithful, so the rows below are directly
comparable to already-published numbers.

| Configuration | C-alone | v2+C | BM25+B+C | recall@12 |
| --- | ---: | ---: | ---: | ---: |
| exact-brute-force | **3/8** | **4/8** | **2/8** | 1.0000 |
| usearch-i8 | 0/8 | 4/8 | 1/8 | 0.2619 |
| usearch-f16 | 2/8 | 4/8 | 1/8 | 0.4548 |
| usearch-f32 | 2/8 | 4/8 | 1/8 | 0.4619 |
| usearch-f16-ef256 | 1/8 | 4/8 | 1/8 | 0.5310 |
| usearch-f16-c32 | 0/8 | 4/8 | 1/8 | 0.6071 |
| usearch-f16-hi | 2/8 | 4/8 | **2/8** | 0.7881 |
| usearch-f32-hi | **3/8** | **4/8** | **2/8** | 0.9690 |

Three things in this table matter more than the headline.

**`v2+C` is 4/8 under every configuration, including i8 at 26% recall.** This
looks like "the ANN is harmless" and it is not. It means the dense channel is
not what surfaces those four targets in that fusion — the anchored-lexical and
char-3-gram channels are. A channel can be degraded to a quarter of its exact
output without moving a fused number it was never carrying. **Do not read
`v2+C` as evidence that the ANN is safe.**

**Aggregate recall does not predict which targets survive.** `usearch-f16-c32`
has *higher* recall@12 than `usearch-f16` (0.607 vs 0.455) but a *worse*
C-alone oracle (0/8 vs 2/8). `usearch-f16-ef256` likewise: more recall, fewer
targets. In a flat neighbourhood, which specific concepts you keep is close to
arbitrary; a recall average smooths over exactly the thing the oracle measures.

**Only `usearch-f32-hi` matches exact everywhere** — same counts and the same
surfaced/missed labels on all three configurations. It costs 95% of the
baseline's memory.

## Why recall is this bad — the neighbourhood is flat

HNSW at 513k vectors normally recalls >0.95 at default settings. Getting 0.42
demanded an explanation rather than a shrug. Two hypotheses were tested.

*Duplicate concept strings producing tied scores* — **rejected**. All 513,236
concepts have distinct embedding texts; there are no exact ties.

*A near-isotropic neighbourhood* — **confirmed**, and it explains everything:

| Measure over the 35 exact queries | Value |
| --- | ---: |
| Exact top-1 cosine, range across queries | 0.5821 – 0.7201 |
| Exact cosine spread, rank 1 → rank 50 | **0.0556** (mean) |
| `usearch-f32` top-1 identical to exact top-1 | 18 / 35 |
| Cosine gap at rank 50, exact − ANN | 0.0192 mean, 0.0694 max |

The 50 nearest concepts sit inside a **0.056-wide cosine band**. HNSW's greedy
descent needs a gradient to follow, and at 513k points packed into a band that
narrow there is almost none. This is a property of *this* embedding
distribution against *this* vocabulary, not a USearch defect — and it is the
same flatness that makes the channel's own ranking fragile.

Note the honest reading of the gap column: the ANN's misses are not cosmetic
ties (0.0192 is about a third of the entire top-1→top-50 spread, and the max
0.0694 exceeds it), but neither are they catastrophic in similarity terms. The
ANN lands in a genuinely worse part of a neighbourhood where "worse" is
0.02 cosine. Both halves of that sentence are true.

## The registry's own text is what flattens it

The obvious follow-up — *is the neighbourhood flat because the registry is
badly built?* — turns out to be yes, and measurably so.
`tools/audit_concept_embedding_space.py` re-embeds a 20,000-concept sample two
ways and reports four statistics (`--sample 20000 --seed 0`).

**Defect A — every definition is boilerplate.** All 513,236 concepts have a
non-empty `definition`, and there are **8 distinct templates among them**:

| Concepts | Definition template |
| ---: | --- |
| 440,599 | `FAST (Faceted Application of Subject Terminology), Topical facet term: {LABEL}.` |
| 70,736 | `EPA non-confidential TSCA Chemical Substance Inventory term: {LABEL}.` |
| 932 | `CRS Legislative Subject Terms term: {LABEL}.` |
| 899 | `Federal Register Thesaurus topic covering {LABEL}.` |
| 35 + 33 + 2 | three further one-line templates |

`concept_embedding_text` appends that definition. The median concept's
embedding input is **144 characters, of which 106 are a constant string shared
with up to 440,598 other concepts** — 74% boilerplate. A real example:

```
current     : Italian language--Conjunctions; FAST (Faceted Application of
              Subject Terminology), Topical facet term: Italian language--Conjunctions.
labels-only : Italian language--Conjunctions
```

Dropping the definition — which is exactly what `concept_bm25_tokens` already
does for the sparse channel, commented "Definitions are intentionally
excluded" — changes the geometry sharply:

| Measure | current (pref; alts; definition) | labels-only (pref; alts) |
| --- | ---: | ---: |
| random concept-pair cosine (the noise floor) | 0.5751 | 0.4506 |
| centroid norm (0 = isotropic, 1 = collapsed) | 0.7587 | 0.6717 |
| effective dimensions (of 768) | 43.1 | **93.6** |
| query top-1 cosine | 0.6042 | 0.6512 |
| query top-1 → top-50 spread | 0.0596 | 0.0767 |
| ~~top-1 margin over the noise floor~~ **(RETRACTED)** | ~~+0.0291~~ | ~~+0.2006~~ |

> ### CORRECTION (2026-07-28, after external review)
>
> **The margin row above is wrong and is retracted, along with the
> "near-degenerate space" conclusion it supported.** Two errors:
>
> **1. The margin used the wrong null.** It compared a *cross-type* similarity
> (segment prose ↔ concept label) against a *within-type* null (concept label ↔
> concept label). Those sit on different scales — short boilerplate-wrapped
> label strings are similar to each other and dissimilar to prose for reasons
> that carry no information about relevance — so the subtraction was
> meaningless. Measured against the correct null, `cos(segment, random
> concept)`, over the full 513,236-row index:
>
> | | |
> | --- | ---: |
> | query vs random concept (correct null) | 0.4262 |
> | query vs best concept | 0.6435 |
> | **correct margin** | **+0.2173** |
>
> The real separation is **7.5× what was reported** and does not indicate a
> degenerate space.
>
> **2. Effective dimensionality of 43/768 was over-read.** The original
> entry treated it as evidence of a degenerate space. It is not: a low
> effective dimensionality is also what a heavily *clustered* label space
> produces, and a vocabulary full of `Italian language--*` subdivision
> families is clustered by construction. This report previously cited
> published isotropy figures here; **those citations were fabricated and
> have been removed** (see the contamination notice in `docs/decisions.md`).
> The claim now stands only as: degeneracy was asserted, never
> established, and the number is consistent with a benign explanation.
>
> **What survives.** The boilerplate itself is a fact about the data (100% of
> concepts carry one of 8 templates; 74% of the median embedding string), as is
> the composition below. Removing it does raise the query top-1 cosine
> (0.6042 → 0.6512) — a like-for-like comparison. But "the space is
> near-degenerate" was not established, and **the 3-of-8 result is not evidence
> that retrieval is broken**: 3/8 in the top 12 is recall@12 ≈ 37.5%, which is at
> **unestablished** as good or bad: this report previously compared it against
> published zero-shot recall figures, and **those citations were fabricated
> and have been removed**. Whether ~37.5% recall@12 over 513,236 unsupervised
> labels is at, above, or below the state of the art is an open question that
> requires reading the primary literature first-hand.
>
> The ANN measurements in this document are unaffected: they compare ANN against
> exact search on identical query vectors and never depend on either statistic.

**Defect B — the composition is 99.6% off-target.**

| Scheme | Concepts | Share |
| --- | ---: | ---: |
| `fast-topical` | 440,599 | 85.8% |
| `epa-tsca` | 70,736 | 13.8% |
| `subject` (FR Thesaurus) | 936 | 0.18% |
| `crs-subjects` | 932 | 0.18% |
| `crs-policy-areas` | 33 | 0.01% |

The vocabularies the development gold labels are actually drawn from total
**1,901 concepts — 0.37% of the registry**. Dense retrieval is searching a
corpus that is 99.6% library subject headings and chemical inventory names by
construction; `Italian language--Conjunctions` is a real row competing for
top-12 slots against regulatory concepts.

**Neither defect is established as a cause here.** This report previously
cited a published corpus-dilution result to support Defect B; **that citation
was fabricated and has been removed** (see the contamination notice in
`docs/decisions.md`). Defect A is a verified fact about the text — 100% of
concepts carry one of 8 templates, 74% of the median embedding string — but
its effect size is not established, and the geometry argument that originally
carried it is retracted above. Both remain plausible and unmeasured; the
running boilerplate ablation is what would settle A.

A caution against over-pruning was previously recorded here, sourced to a
published analogue; **that citation was fabricated and has been removed**. The
*architectural* idea it was attached to survives on its own logic and is worth
testing: decouple the space you *map into* from the space you may *emit* —
keep off-domain rows as absorbing decoys so off-topic content lands somewhere
harmless, and emit only in-domain concepts. That is a hypothesis this repo can
test locally, not a finding.

Neither defect was introduced by this experiment and neither is fixed by it.
The `labels-only` arm is a 20,000-concept diagnostic, not a proposal: a real
change means re-embedding all 513,236 concepts (~50 minutes) and re-running the
ablation, and it should be measured on the fused shortlist, not on geometry.

## The honest tradeoff

**What ANN buys, at oracle parity (`usearch-f32-hi`):** 1,697 MB → 1,617 MB
resident, a **4.7% saving**; 16.3 ms → 4.7 ms mean latency. The disk file is
*larger* than the `.npz` (1,702 MB vs 1,566 MB) because the graph is stored on
top of the vectors.

**What ANN buys at a real memory saving (`usearch-f16-hi`, 1,107 MB, −35%):**
recall@12 drops to 0.788 and C-alone loses one of three targets (`human
rights`). Both fused configurations still match exact.

**What ANN buys at the cheapest setting (`usearch-i8`, 462 MB, −73%):**
recall@12 of 0.262 and C-alone at 0/8. The channel stops working.

**Does the 8-target oracle survive?** Only at `usearch-f32-hi`, which saves
nothing worth having. At `usearch-f16-hi` it survives in both fused
configurations but loses one target when the channel stands alone.

Memory-mapped pages are file-backed and evictable under pressure, unlike the
exact matrix's anonymous allocation. That is a real secondary advantage and it
does not change the verdict.

## Recommendation: REJECT (revisit under named conditions)

Do not swap the dense channel onto USearch now.

1. **There is no measured operational failure to fix.** The exact search costs
   1.70 GB and 16 ms per query on a 48 GB machine. The research warning that
   motivated this concerned >34 GB library tooling; the measured number here is
   fifty times smaller. The charter's own rule applies: *do not add storage or
   workflow infrastructure without a measured need.*
2. **The saving and the correctness are the same dial.** At oracle parity the
   saving is 4.7%. At a saving worth having, the top-12 loses 21–74%.
3. **The one reassuring number is an artifact.** `v2+C` holding at 4/8
   everywhere reflects the lexical channels carrying those targets, not the ANN
   preserving them.

**The more valuable work is upstream, though not for the reason first given.**
The dense channel is not underperforming — recall@12 ≈ 37.5% over 513,236 labels
with no training data is at the published zero-shot ceiling for this scale. That
ceiling is low because the vocabulary is large, borrowed, and unsupervised, and
no amount of index tuning raises it. The levers that published evidence supports,
in order: shrink the *emit* vocabulary to an in-domain set; use the Federal
Register's own agency-assigned subject terms, which are free supervision rather
than an absent one; and only then revisit retrieval. Any of those changes the
embedding distribution, so the ANN measurements here would need redoing
afterwards regardless.

**Revisit when any of these becomes true**, and start from `usearch-f16-hi`
(1,107 MB, recall@12 0.788, both fused oracles at parity):

- the registry grows enough that the float matrix no longer fits the serving
  budget (the matrix is linear in concept count — 1.58 GB at 513k — so the
  crossover follows directly from whatever that budget turns out to be; no
  budget is declared anywhere today, which is itself why this is not yet a
  measured need);
- the channel is served from concurrent workers, where 1.7 GB *per worker*
  becomes binding while a memory-mapped file is shared by the page cache; or
- the embedding is changed to one whose top-50 neighbourhood is less flat, at
  which point HNSW recall should be re-measured before anything else.

The infrastructure is built, pinned, and tested, so revisiting is cheap.

## Limits of this evidence

- **Development-only.** The 35 items are permanently train/development data.
  This supports a *cost and mechanism* decision, not an accuracy claim. The
  drawn holdout was not read, drawn against, or touched.
- **35 queries** is a small sample. Differences of a few points are not
  resolvable; the differences reported here span 0.26–0.97.
- **Queries are truncated at 512 tokens** (segments reach 1,404). This is the
  channel's existing documented limitation, not something introduced here. It
  does not bias the comparison — both arms consume identical query vectors —
  but the "exact" ground truth is itself a truncated-query ground truth.
- **The oracle is 8 mechanical targets**, not tagging accuracy, and candidate
  recall is not tagging accuracy is not product success.
- Peak RSS for a memory-mapped index is a working-set measure after 35 queries;
  sustained serving can only push it higher, so the mmap figures are lower
  bounds.

## Related

- [Candidate-selection research](candidate-selection-research-2026-07-27.md) — the ops warning that prompted this.
- [Experiment strategy](../experiment-strategy.md) — the charter and the stop rules.
- [Failure analysis](failure-analysis-2026-07-27.md) — the 512-token truncation, already recorded as a channel-C limit.
