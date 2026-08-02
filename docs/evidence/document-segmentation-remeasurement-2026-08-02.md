# Document segmentation re-measurement — 2026-08-02

- **Date:** 2026-08-02 (revised the same day after adversarial review of `ad7fcc2`)
- **Status:** Measured, receipted, unpublished. One gate fails on purpose.
- **Code state:** committed, `3a472f0` on `main`
- **Interpreter:** CPython 3.12.9 — now pinned (`.python-version`, `requires-python = ">=3.12,<3.13"`)
- **Paid provider calls:** zero
- **Policy pin:** untouched — `SELECTED_POLICY` remains `structure-overlap-1800`

Receipt: `docs/evidence/document-segmentation-remeasurement-2026-08-02/receipt.json`.

> **Revision note.** The first version of this document attributed the
> segment-count divergence to commit `e0af2b9`. **That attribution was wrong and
> is withdrawn** — see "The divergence is not a code change" below. The
> correction makes the finding worse, not better.

## What this settles, in one paragraph

The incumbent `structure-overlap-1800` still wins the July decision rule among
the arms that rule admits, and no test can distinguish it from `structure-first`
— the two even swap nominal winner depending on which metric you ask. But two
things now sit under the pin that did not before. First, the committed
segmenter produces 1,296 segments where the frozen baseline recorded 1,302, and
**identical code produces both numbers** — 1,302 on 2026-07-26 and 1,296 today —
so the cause is environmental, not a commit, and nothing in the repo pinned the
variable that moved. Second, `semantic-embedding` leads the ordering's *first*
criterion (Recall@50, 31/35 vs 28/35) and is disqualified only by a
slice-level containment rule applied to one synthetic adversarial span whose
text sits, verbatim and uncut, inside a single segment. Whether containment is
slice-level or segment-level is an open definitional question that decides
which arm wins.

## The blocker that had to be cleared first

The sealed dataset's `evaluation_id` digests **every** non-model member. Commit
`3a472f0` rebuilt `fr_docket_links.parquet` in place across all 24 generations,
changing the recomputed identity of the whole corpus:

| | value |
|---|---|
| sealed identity (July) | `segmentation_eval_627ba96e04872d870a2ccd6e` |
| recomputed identity (now) | `segmentation_eval_21d9a09f13ad3b9bf5ea212b` |
| members changed | 1 of 27 (`fr_docket_links.parquet`) |
| members byte-identical | 26 of 27 |

**The sealed bytes are unrecoverable.** No copy survives across 25 on-disk
copies, and `3a472f0` records that the previous writer was byte-non-deterministic
("nine identical-input generations produced nine distinct digests"), so
re-running it cannot reproduce the row order. `segmentation_eval_627ba96e…` can
never be reproduced on this machine again.

`fr_docket_links` is in `EXCLUDED_SOURCE_TABLES` — a relationship carrier that
never becomes a `SourceArtifact`, so it cannot move a segment boundary.

### The substitute, and why it is legitimate

`tools/reseal_segmentation_dataset.py` copies a dataset, recomputes the seal,
and writes a `resealed_from` provenance block. It never repairs, regenerates, or
reorders data, and never writes to the source. It **fails closed**: every
changed member must be licensed by name, and the CLI licenses automatically only
members whose table is in `EXCLUDED_SOURCE_TABLES`. Rewriting `gold_spans.parquet`
and re-sealing is refused rather than absorbed — without that, the tool would
emit an honest-looking identity with a passing receipt over changed evidence.

The substitution is inert for this measurement, and that is checked:

| set | July scope | re-sealed scope | identical |
|---|---|---|---|
| included artifact digests | 153 | 153 | yes |
| included gold ids | 35 | 35 | yes |
| included adversarial case ids | 7 | 7 | yes |

## Work item 1 — the frozen-local gate: FAIL

The committed segmenter produces **1,296** selected `structure-overlap-1800`
segments where the frozen baseline recorded **1,302**.

### The divergence is not a code change

The first version of this document blamed `e0af2b9`. Three facts falsify that,
and together they falsify *any* code explanation:

1. **`e0af2b9` was already in effect when the gate passed.** It is an ancestor
   of `414964d`, and the 2026-07-26 receipt records the gate passing at 1,302
   with `docpipeline/source.py` and `segments.py` digests byte-identical to
   `414964d` (`ee6af706…`, `c1fb7a97…`).
2. **The July tree reproduces today's number.** Exporting `414964d`'s `src/`
   and re-running the identical computation against the re-sealed corpus yields
   **1,296 segments** — the same code that yielded 1,302 on 2026-07-26.
3. **The only later change to `source.py` cannot do it.** `2e9ae9e` is
   +294/−0 on that file: purely additive, touching no existing path.
   `segments.py` is unchanged at HEAD. beautifulsoup4 and lxml are locked
   unchanged.

So identical code yields 1,302 then and 1,296 now. **The variable is the
environment, and nothing in the repo pinned it.** No receipt anywhere — the
July experiment's, the Step 4 parity receipt's — records an interpreter version.

**Leading hypothesis, with direct evidence.** Segmentation reads native markup
through `_MarkupBoundaryParser`, a stdlib `html.parser.HTMLParser` subclass.
`source.py::_markup_drafts` swallows `AssertionError`/`ValueError` from it and
returns `None`, silently switching to a coarser drafting strategy. Probing the
drifting artifact under today's interpreter: the parser **does not raise**, and
the markup path yields **3,427 drafts**. The July run recorded only 174
`xml_text` slices for that artifact — consistent with the fallback having fired
then and not now. A CPython `html.parser` patch is exactly the kind of change
that flips a swallowed exception. This is a well-supported hypothesis, not a
proven cause: July's interpreter cannot be re-run because it was never recorded.

**What actually changed, per arm** (complete — all 8 drifting artifacts):

| config | July | now | Δ | artifacts changed |
|---|---:|---:|---:|---|
| structure-overlap-1800 | 1,302 | 1,296 | −6 | `congress-bill-v1`/`118-hr-8862` (64→58) |
| structure-first-1800 | 1,263 | 1,260 | −3 | `congress-bill-v1`/`118-hr-8862` (61→58) |
| semantic-embedding-1800 | 1,597 | 1,594 | −3 | `federal-register-document-v1`/`2026-11140` (387→386); `gao-26-107693` (96→95); `gao-26-108625` (35→**36**); `gao-26-108641` (84→82) |
| llm-guided-1800 | 1,766 | 1,764 | −2 | `gao-26-108089` (61→60); `gao-26-108641` (103→102) |
| paragraph-sentence-1800 | 1,276 | 1,276 | 0 | none — a true no-op, not offsetting drift |

"The entire delta is one artifact" is true **only of the two structure arms**.
`gao-26-108625` is the one artifact anywhere that *gained* a segment.
`paragraph-sentence`, which ignores the element stream, is untouched — which
independently confirms the corpus is identical and localises the change to
element extraction.

**The decision this needs is not "accept or revert `e0af2b9`."** That commit is
not the cause, and is a mega-commit that could not be reverted anyway. The
decision is: **identify the environmental variable that moved, pin it, and
re-baseline deliberately.** The interpreter is now pinned to 3.12.9 as the
cheapest candidate fix, but that pin is a hypothesis under test, not a
confirmed remedy — 3.12.9 may already be the *new* behaviour.

Also corrected from the first version: "nothing has attested corpus-scale
behaviour against committed code" was wrong. The 2026-07-26 receipt attests it
**by bytes** — its recorded `source.py`/`segments.py` digests match `414964d`
exactly. The true statement is narrower and stranger: that attestation no
longer reproduces, without any code having changed.

The gate is **left failing**. Moving 1302 to 1296 would bless an environmental
drift as a decision. It is opt-in, so CI is unaffected.

## Work item 2 — the fair comparison, re-run

Same harness, same pinned local BGE
(`BAAI/bge-base-en-v1.5@a5beb1e3e68b9ab74eb54cfd186867f64f240e1a`), same
`ir-measures:0.4.3`, same 35 gold queries, same 153 artifacts, same 1,800-token
budget, same candidate limit. Experiment
`segmentation_experiment_15331a57b818675faa1de316`; build and validate both pass.

| Arm | Gold | Segments (Jul → now) | R@10 | R@50 | R@200 | MRR | nDCG@10 |
|---|---:|---:|---:|---:|---:|---:|---:|
| **Structure + limited overlap** | 35/35 | 1,302 → 1,296 | **0.5714** | 0.8000 | 0.8286 | 0.2896 | 0.3440 |
| Structure first | 35/35 | 1,263 → 1,260 | 0.5143 | 0.8000 | 0.8286 | **0.2958** | 0.3332 |
| Paragraph + sentence | 35/35 | 1,276 → 1,276 | 0.4857 | 0.8000 | 0.8286 | 0.2626 | 0.2982 |
| Semantic embedding | 34/35 | 1,597 → 1,594 | 0.5714 | **0.8857** | **0.9429** | 0.2883 | 0.3385 |
| LLM-guided (local heuristic) | 34/35 | 1,766 → 1,764 | 0.6571 | 0.9143 | 0.9429 | 0.3510 | 0.4099 |

Among the three arms the July rule admits (35/35 contained), all tie at
R@50 = 0.8000 (28/35 each) and structure-overlap takes the R@10 tiebreak,
20/35 to 18/35. Structure-first still edges MRR, reproducing the July pattern.

## Is any of it real? Two lenses, and they disagree usefully

### Binarised hits (what the July rule uses)

Exact two-sided McNemar over the 35 paired queries:

| Comparison | Hits | Discordant | p |
|---|---|---|---:|
| structure-overlap vs structure-first @10 | 20 vs 18 | 2–0 | 0.500 |
| structure-overlap vs paragraph-sentence @10 | 20 vs 17 | 3–0 | 0.250 |
| semantic-embedding vs structure-overlap @50 | 31 vs 28 | 4–1 | 0.375 |
| semantic-embedding vs structure-overlap @200 | 33 vs 29 | 4–0 | 0.125 |
| llm-guided vs paragraph-sentence @10 | 23 vs 17 | 7–1 | 0.070 |

Among the **lossless** arms the largest discordance is 3. Across all five arms
it is 7–1 (llm-guided vs paragraph-sentence), which lands at p = 0.070 — close,
but not there. A 35-query corpus *can* in principle reach p < 0.05 on a
binarised metric: it needs a 6–0 split or better. What it cannot resolve are
effects of the size actually observed among the lossless arms.

### Per-query magnitudes (the free power upgrade)

Binarising throws away how *well* each query did. Comparing per-query reciprocal
rank and nDCG@10 with a paired Wilcoxon signed-rank and a 200,000-resample
paired bootstrap extracts more from the same 35 queries at zero annotation cost:

| Comparison | Metric | mean Δ | Wilcoxon p | bootstrap p |
|---|---|---:|---:|---:|
| structure-overlap − structure-first | RR | **−0.0062** | 0.979 | 0.388 |
| structure-overlap − structure-first | nDCG@10 | **+0.0108** | 0.672 | 0.425 |
| semantic-embedding − structure-overlap | RR | −0.0011 | 0.989 | 0.983 |
| semantic-embedding − structure-overlap | nDCG@10 | −0.0055 | 0.796 | 0.869 |
| structure-overlap − paragraph-sentence | nDCG@10 | +0.0458 | **0.037** | **0.011** |
| llm-guided − structure-overlap | RR | +0.0616 | **0.015** | **0.001** |

**This changes a conclusion.** The magnitude-based tests do reach significance
where the binarised ones could not: structure-overlap genuinely beats
paragraph-sentence on nDCG@10, and llm-guided genuinely beats structure-overlap
on reciprocal rank. So the corpus is not uniformly hopeless — the first version
of this document overstated that, and the overstatement is withdrawn. (With
Bonferroni across the eight tests run, only llm-guided − structure-overlap on RR
survives at 0.05/8; the other two are suggestive, not established.)

**And it sharpens the incumbent question.** Under every test tried,
structure-overlap and structure-first remain indistinguishable — and they *swap
nominal winner by metric*: structure-first is ahead on reciprocal rank
(−0.0062), structure-overlap is ahead on nDCG@10 (+0.0108). Two arms that trade
the lead depending on which reasonable metric you pick, with p ≥ 0.39 either
way, are not distinguishable by this corpus. That was true in July and is true
now.

## The semantic-embedding result, stated honestly

The first version of this document spent a section proving a 2-query R@10 margin
unresolvable and dismissed semantic-embedding in two sentences. That was
asymmetric scrutiny. Applying the same lens:

**Semantic-embedding wins the declared ordering on its *first* criterion.**
R@50 is 31/35 against the incumbent's 28/35; R@200 is 33/35 against 29/35. It
also ties the incumbent at R@10 (20/35) and is a dead heat on early-ranking
magnitude (RR p = 0.98, nDCG@10 p = 0.87). Its advantage is specifically in
**deep recall**, and it is not statistically established (p = 0.375 at k=50,
0.125 at k=200) — the same verdict this document reached for the incumbent's
own margin, and it must be stated with the same force in both directions.

**Its sole disqualification is one span, and the disqualification is a
definitional artifact.** The missed gold row is
`gold_f6bda7557c925438539fafed`, case `adversarial-boundary-crossing` — a
hand-authored synthetic 45-character phrase, "perfluoroalkyl and polyfluoroalkyl
substances", at `documents.text_content` [9504, 9549). Semantic-embedding cuts a
slice at char 9523, 19 characters into the phrase. But:

- **both slices — `[7648, 9523)` and `[9523, 9909)` — belong to the same
  segment** (`experiment_segment_59e4e3ede3a703869eb4ac68`);
- that artifact produces **exactly one segment under every one of the five
  arms**, so there is no segment boundary anywhere in this document;
- the concatenated segment text contains the phrase **verbatim** at the exact
  gold offset.

Nothing is lost to retrieval. The span is only "missed" because
`_relevant(contain=True)` requires a single *slice* to enclose it. And the three
arms that "pass" do so by emitting one uncut `[0, 9909)` slice — **they pass by
not slicing at all, not by slicing better.**

This is not confined to semantic-embedding: `llm-guided` fails the identical
span, in both July and now. Two of five arms are disqualified by one synthetic
adversarial case that no arm actually loses information on.

**Whether containment is slice-level or segment-level is an open decision, and
it changes which arm wins.** Under segment-level containment, semantic-embedding
contains 35/35 and takes the ordering on R@50 outright. That decision is not
mine to make and is not made here.

## Work items 3 and 4 — the adapter: not measurable here

Four structural facts make this comparison impossible through the
`DocumentRelease` adapter. These are code facts, not judgements.

1. **The gold key does not survive the crossing.** July's `artifact_digest` is a
   digest over the whole multi-field record identity; the adapter mints
   `content_sha256 = sha256(unicode_text)` for one representation. The
   `source_field` namespaces are disjoint too (`court_opinions.pdf_text` vs
   `derived-from-rendition:pdf`). Only character offsets transfer.
2. **Multi-field segments cannot exist through it.** 152 of the 1,302 baseline
   segments carry slices from 2–6 different source fields. A
   one-representation-per-artifact adapter cannot produce them — ~12% of the
   baseline, structurally absent.
3. **No release exists over the 153 artifacts and nothing builds one.** The only
   sealed `DocumentRelease` here is the 722-document search-holdout exam corpus
   (title + abstract only, no segmentation gold).
4. **Three of five arms are not expressible.** `docpipeline/segments.py`
   implements one algorithm; `boundary_method` is a recorded label nothing
   branches on.

On the substantive question — does passage-bounding constrain one arm more than
another — the code answers partly without a run. A segment **can** span adjacent
sealed passages (`_pack` merges consecutive in-budget passage regions), but a
*slice* never crosses a passage boundary and inter-passage characters are never
carried. The adapter replaces the region lattice with the passage lattice. That
is not arm-neutral: it constrains structure-derived arms mildly and makes
`paragraph-sentence` unrepresentable. A run through the adapter would be a
different experiment reusing the word "arm".

Note the interaction with the section above: the adapter *always* slices at
sealed passage boundaries, so under a slice-level containment rule it would
manufacture exactly the kind of false miss that disqualifies semantic-embedding
today. Settling the containment definition is a prerequisite for the adapter
measurement, not merely adjacent to it.

**A valid substitute would require** a `DocumentRelease` over the same 153
documents whose representations preserve each gold span's field text
byte-identically, a crosswalk of the 35 gold rows into representation
coordinates, and a decision about the 152 multi-field segments. That is a build,
not a re-run.

## Recommendation

**Keep `structure-overlap-1800` today — but the pin is no longer the
interesting question, and I no longer think it is settled.**

On the incumbent versus structure-first, my position is unchanged and now
better supported: they are indistinguishable, they swap the lead by metric, and
overlap costs 142,632 duplicated characters for a difference no test can find.
When two options are indistinguishable, the tiebreak should be inertia — the
incumbent is deployed and spicysearch indexes against it.

What I got wrong first time, and now think matters more:

- **The containment definition is the live decision.** Semantic-embedding leads
  the first criterion of the declared ordering and is excluded by a rule that
  marks a span missed while its text sits whole inside one segment. Under the
  production cascade the July evidence actually describes — dense top-50 then a
  cross-encoder that materially fixes early ranking — **R@50 is the criterion
  that counts**, because the reranker repairs precisely what semantic-embedding
  is not better at. An arm that leads R@50 by 3 queries and R@200 by 4 deserves
  a real adjudication, not a footnote. I would resolve slice-level versus
  segment-level containment *before* treating the pin as settled.
- **The environmental drift outranks both.** A corpus-scale result moved by 6
  segments with no code change and no recorded interpreter. Until the variable
  is identified and pinned, every number in this document — including the ones
  I am recommending on — has an unmeasured reproducibility error bar.

Nothing here changes `SELECTED_POLICY` or any pinned constant, and none of these
decisions are mine to make.

## What this does and does not settle

**Settles:**

- The incumbent still wins the July rule among the arms that rule admits.
- Structure-overlap and structure-first are indistinguishable under McNemar,
  Wilcoxon, and a paired bootstrap, on both RR and nDCG@10.
- The 1,302 → 1,296 divergence is **not** attributable to any commit; identical
  code produces both numbers.
- The semantic-embedding gold miss is a slice-level containment artifact, not a
  retrieval loss, and it affects `llm-guided` identically.
- Magnitude-based paired tests do reach significance on this corpus for larger
  gaps; binarised R@10 discards that power.

**Does not settle:**

- What environmental variable moved. The interpreter pin is a hypothesis.
- Whether containment should be slice-level or segment-level — and therefore
  whether semantic-embedding should have won.
- Whether the OpenAI-side result still favours structure-first. No paid calls
  were made.
- Whether the adapter changes the ranking. Not measurable against this gold set
  without new construction.
- Whether 1,296 is better or worse than 1,302. Only that it is different.

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

To reproduce the falsification of the code hypothesis:

```sh
git archive 414964d src | tar -x -C /tmp/july414964d
# then run the gate computation with /tmp/july414964d/src first on sys.path
```

Output artifacts are gitignored and pinned by digest in the receipt.
