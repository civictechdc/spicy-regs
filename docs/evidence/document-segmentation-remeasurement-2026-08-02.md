# Document segmentation re-measurement — 2026-08-02

- **Date:** 2026-08-02
- **Status:** Measured, receipted, unpublished. One gate fails on purpose.
- **Code state:** committed, `3a472f0` on `main`
- **Paid provider calls:** zero
- **Policy pin:** untouched — `SELECTED_POLICY` remains `structure-overlap-1800`

Receipt: `docs/evidence/document-segmentation-remeasurement-2026-08-02/receipt.json`
(`sha256:9ac51f66e53c910d151674770d4a0487be5715fca872b6b5505e23a844c117de`).

## What this settles, in one paragraph

The incumbent `structure-overlap-1800` still wins the direct comparison under
the July decision rule, and now by a slightly wider Recall@10 margin than in
July. It also **cannot be shown to win** — a paired exact test over the 35 gold
queries puts the best available p-value at 0.50, and the corpus is too small
for any direct-arm Recall@10 comparison to reach significance even in
principle. Separately, and more consequentially, the committed segmenter no
longer reproduces the frozen July baseline: 1,296 selected segments where the
baseline recorded 1,302. That divergence was undetectable until now because the
only prior execution of the corpus-scale gate ran against uncommitted code.

## The blocker that had to be cleared first

Both work items were blocked at the same choke point, and it is not a
segmentation defect.

The sealed dataset's `evaluation_id` is a digest over **every** non-model
member (`segmentation_evaluation._evaluation_id`). Commit `3a472f0` rebuilt
`fr_docket_links.parquet` in place across all 24 generations. That single
rewrite changed the recomputed identity of the whole corpus:

| | value |
|---|---|
| sealed identity (July) | `segmentation_eval_627ba96e04872d870a2ccd6e` |
| recomputed identity (now) | `segmentation_eval_21d9a09f13ad3b9bf5ea212b` |
| members changed | 1 of 27 (`fr_docket_links.parquet`) |
| members byte-identical | 26 of 27 |

**The sealed bytes are unrecoverable.** No copy survives on disk — every
generation was rebuilt — and `3a472f0`'s own message records that the previous
writer was byte-non-deterministic ("nine identical-input generations produced
nine distinct digests"), so re-running it cannot reproduce the row order. The
identity `segmentation_eval_627ba96e04872d870a2ccd6e` can never be reproduced
on this machine again.

`fr_docket_links` is in `EXCLUDED_SOURCE_TABLES` — a relationship carrier that
never becomes a `SourceArtifact`. It cannot move a segment boundary. So the
documents under measurement are untouched; only the seal naming them broke.

### The substitute, and why it is a legitimate one

`tools/reseal_segmentation_dataset.py` (new, TDD, 11 tests) copies a dataset,
recomputes the seal over the copy, and writes a `resealed_from` provenance
block naming the identity it replaced and the exact members that forced the
replacement. It never repairs, regenerates, or reorders data, and never writes
to the source.

The substitution is inert for this measurement, and that is checked rather than
asserted:

| set | July scope | re-sealed scope | identical |
|---|---|---|---|
| included artifact digests | 153 | 153 | yes |
| included gold ids | 35 | 35 | yes |
| included adversarial case ids | 7 | 7 | yes |

The corpus under measurement **is** the July corpus. Only its name changed.

## Work item 1 — the frozen-local gate: FAIL

```sh
SPICY_REGS_FROZEN_SEGMENTATION_ROOT="$PWD/output" R2_PUBLIC_URL='' \
  uv run --frozen pytest tests/test_docpipeline_segments_frozen_local.py -q -rs
# 6 passed, 6 errors — AssertionError: assert 1296 == 1302
```

The committed segmenter produces **1,296** selected `structure-overlap-1800`
segments where the frozen baseline recorded **1,302**.

The entire delta is one artifact:

| artifact | July segments | now |
|---|---:|---:|
| `congress-bill-v1` / `118-hr-8862` | 64 | 58 |
| every other accepted artifact | — | unchanged |

Its `congress_bills.xml_text` element stream went from **174 slices to 3,428** —
the same text, decomposed far more finely, then greedily packed into fewer,
fuller segments.

**Attribution.** `e0af2b9`, the commit that last changed
`src/spicy_regs/ontology/adapters.py`, landed 2026-07-24 at 20:37. The baseline
parquet was written the same day at 18:11 — two and a half hours earlier. The
frozen baseline was produced by code that was never committed in that form.
The `paragraph-sentence` arm, which ignores the element stream entirely, is
unchanged at exactly 1,276 segments, which independently confirms the corpus is
identical and localises the change to element extraction.

This is precisely the risk the single `local-uncommitted` execution receipt
implied, now realised: **nothing has attested corpus-scale segmentation
behaviour against committed code, and when finally asked, it diverged.**

The gate is **left failing**. Moving `1302` to `1296` would silently bless the
change. Whether finer XML element granularity is an improvement to accept or a
regression to revert is a human decision, and the gate's job is to hold the
divergence visible until that decision is made. The gate is opt-in, so CI is
unaffected.

## Work item 2 — the fair comparison, re-run

Protocol reproduced faithfully: same harness
(`spicy_regs.corpora.segmentation_experiment`), same pinned local BGE provider
`BAAI/bge-base-en-v1.5@a5beb1e3e68b9ab74eb54cfd186867f64f240e1a`, same
`ir-measures:0.4.3`, same 35 gold queries, same 153 artifacts, same 1,800-token
budget, same candidate limit of 200. Experiment
`segmentation_experiment_15331a57b818675faa1de316`; build and validate both
`pass` with no failures.

### Direct arms, incumbent BGE, budget 1,800

| Arm | Gold | Segments (Jul → now) | R@10 (Jul → now) | R@50 (Jul → now) | MRR (Jul → now) | nDCG@10 |
|---|---:|---:|---:|---:|---:|---:|
| **Structure + limited overlap** | 35/35 | 1,302 → **1,296** | 0.5429 → **0.5714** | 0.8000 → **0.8000** | 0.2940 → 0.2896 | 0.3440 |
| Structure first | 35/35 | 1,263 → 1,260 | 0.5143 → 0.5143 | 0.8000 → 0.8000 | 0.2948 → **0.2958** | 0.3332 |
| Paragraph + sentence fallback | 35/35 | 1,276 → 1,276 | 0.4857 → 0.4857 | 0.8000 → 0.8000 | 0.2624 → 0.2626 | 0.2982 |
| Semantic embedding | 34/35 | 1,597 → 1,594 | 0.5429 → 0.5714 | 0.8000 → **0.8857** | 0.2758 → 0.2883 | 0.3385 |
| LLM-guided (local heuristic selector) | 34/35 | — → 1,764 | 0.6000 → 0.6571 | 0.8857 → 0.9143 | 0.3409 → 0.3510 | 0.4099 |

The July ordering — Recall@50, then Recall@10, then MRR, requiring all 35 gold
spans contained — resolves the same way it did in July, one step earlier. All
three lossless arms tie at R@50 = 0.8000, and structure-overlap takes R@10
outright. Structure-first still edges MRR (0.2958 vs 0.2896), reproducing the
July pattern, but MRR is the third tiebreak and is never reached.

The semantic arm now has the best non-LLM R@50 (0.8857, up from 0.8000) but
still misses one gold span, so the lossless requirement excludes it exactly as
it did in July.

### Is the margin real? Paired exact test over the 35 queries

Recall@10 here is per-query binary, so the arms can be compared pairwise.

| Comparison | Hits | Discordant | Exact two-sided McNemar |
|---|---|---|---:|
| structure-overlap vs structure-first | 20/35 vs 18/35 | 2–0 for overlap | **p = 0.50** |
| structure-overlap vs paragraph-sentence | 20/35 vs 17/35 | 3–0 for overlap | p = 0.25 |
| structure-first vs paragraph-sentence | 18/35 vs 17/35 | 1–0 for first | p = 1.00 |

At Recall@50 all three lossless arms hit 28/35 — identical, not merely equal in
aggregate.

**The corpus is underpowered by construction.** With an exact two-sided sign
test, p < 0.05 needs at least **6 discordant queries all in one direction**. The
largest discordance observed anywhere is 3. No direct-arm Recall@10 comparison
on this 35-query corpus can reach conventional significance, whatever the true
effect. The July decision does not rest on noise exactly — the direction has
never reversed, and structure-overlap does not lose a single query to
structure-first — but it rests on a 2-query margin that this corpus cannot
resolve, and it always did.

## Work items 3 and 4 — the adapter: not measurable here, and why

The brief asked whether feeding the segmenter from sealed `DocumentRelease` v2
passages changes the answer, and warned that measuring beats reasoning. The
measurement was not attempted, because four independent structural facts make
it impossible to run *this* comparison through *that* path. These are code
facts, not judgements.

1. **The gold key does not survive the crossing.** Gold is
   `(artifact_digest, source_field, start_char, end_char)`. July's
   `artifact_digest` is a digest over the whole multi-field record identity
   (`ontology/subjects.py::_make_artifact`); the adapter mints
   `content_sha256 = sha256(unicode_text)` for one representation
   (`document_release_segments.py:602`). The `source_field` namespaces are
   disjoint too — `court_opinions.pdf_text` versus
   `derived-from-rendition:pdf`. Only the character offsets transfer.
2. **Multi-field segments cannot exist through the adapter.** 152 of the 1,302
   baseline segments carry slices from 2–6 different source fields; `_pack` has
   no same-field break by design. A one-representation-per-artifact adapter
   cannot produce them. That is ~12% of the baseline, structurally absent.
3. **No release exists over the 153 artifacts, and nothing builds one.** The
   only sealed `DocumentRelease` on this machine is the 722-document
   search-holdout exam corpus (title + abstract only, no segmentation gold).
   `build_document_release`'s fixture format requires a hand-authored passage
   list per record; the automated passage generators live only on the
   actual-file path, which covers 34 documents, not 153.
4. **Three of five arms are not expressible.** `docpipeline/segments.py`
   implements exactly one algorithm; `boundary_method` is a recorded label that
   nothing branches on. `paragraph-sentence`, `semantic-embedding`, and
   `llm-guided` do not exist there. `structure-first` is only approximable via
   `overlap_tokens=0`, which is a different code path from the frozen arm.

On the substantive question the brief actually posed — does passage-bounding
constrain one arm more than another — the code gives a partial answer that does
not need a run. A segment produced through the adapter **can** span adjacent
sealed passages (`_pack` merges consecutive in-budget passage regions), but a
*slice* never crosses a passage boundary, and characters between passages are
never carried. So the adapter replaces the region lattice with the passage
lattice: boundaries may only fall where the release sealed them, plus
intra-passage splits when a passage exceeds budget. That is not arm-neutral. It
constrains structure-derived arms mildly and makes `paragraph-sentence` — which
by design ignores structure — unrepresentable. A comparison run through the
adapter would therefore not be a fair comparison in the July sense; it would be
a different experiment that happens to reuse the word "arm".

**What a valid substitute would require**, stated so it can be scheduled rather
than improvised: a `DocumentRelease` built over the same 153 documents whose
text representations preserve each gold span's field text byte-identically, a
crosswalk from the 35 gold rows into representation coordinates, and a decision
about what to do with the 152 multi-field segments that have no counterpart.
That is a build, not a re-run.

## Recommendation

**Keep `structure-overlap-1800`.** I would not move the pin, and here is the
reasoning rather than the ceremony:

- It wins the declared ordering on fresh evidence, one tiebreak earlier than in
  July, and it is the only lossless arm that never loses a query to another
  lossless arm.
- Its nearest rival, structure-first, has one advantage — a 0.006 MRR edge that
  the ordering never consults, and that is far inside the noise floor of a
  35-query corpus.
- Overlap costs 142,632 duplicated characters against structure-first's zero.
  That is the real price, and it buys 2 queries of Recall@10 that cannot be
  shown to be real. If the pin were being chosen from scratch today, that trade
  would be genuinely arguable. But it is not being chosen from scratch: the
  incumbent is deployed, spicysearch indexes against it, and switching on an
  unmeasurable difference would spend real migration cost to buy nothing.
- The honest summary is that structure-first and structure-overlap are
  indistinguishable on this corpus, and when two options are indistinguishable
  the tiebreak should be inertia, not a coin flip dressed as a metric.

**The finding that should actually change something is the drift, not the
policy.** The segmenter's element extraction changed on 2026-07-24 and nobody
knew until today. That deserves a decision — accept the finer XML granularity
and re-baseline, or revert it — and a second execution receipt so the gate has
more than one data point in its life. Those are Mike's calls; nothing here
pre-empts them.

## What this does and does not settle

**Settles:**

- The incumbent still wins the direct comparison under the July rule, on
  committed code, on a corpus proven identical to July's.
- The margin is not statistically demonstrable, and cannot be made so at this
  corpus size — a fact about the corpus, not about the arms.
- The committed segmenter diverges from the frozen baseline by 6 segments on
  one artifact, attributable to `e0af2b9`.
- The sealed July evaluation identity is permanently unreproducible here.

**Does not settle:**

- Whether the OpenAI-side result still favours structure-first. No paid calls
  were made; that half of the July comparison is untouched and unverified.
- Whether the adapter changes the ranking. Not measured, and not measurable
  against this gold set without new construction.
- Whether the 1,296-segment behaviour is better or worse than 1,302. Only that
  it is different.
- Anything about rerank, sparse, hybrid, or whole-artifact stages.

## Reproduce

```sh
uv run python tools/reseal_segmentation_dataset.py \
    output/segmented-real-data-evaluation-v2 \
    output/segmented-real-data-evaluation-v2-resealed-2026-08-02
uv run python -m spicy_regs.corpora.document_acceptance_scope build \
    output/segmented-real-data-evaluation-v2-resealed-2026-08-02 \
    output/document-acceptance-scope-resealed-2026-08-02
uv run python -m spicy_regs.corpora.segmentation_experiment build \
    output/segmented-real-data-evaluation-v2-resealed-2026-08-02 \
    output/segmentation-experiment-document-bge-2026-08-02 \
    --provider incumbent-bge --budgets 1800 \
    --scope-dir output/document-acceptance-scope-resealed-2026-08-02
uv run pytest tests/test_reseal_segmentation_dataset.py   # targeted, never the full suite
```

Output artifacts are gitignored and pinned by digest in the receipt.
