# Real-bodies retrieval corpus — 2026-08-02

- **Date:** 2026-08-02
- **Status:** Built, measured, receipted, unpublished. Nothing pushed.
- **Code state:** committed on `main`
- **Interpreter:** CPython 3.12.9 (pinned by `.python-version`, `requires-python >=3.12,<3.13`)
- **Paid provider calls:** zero
- **Network:** 993 GET requests to `www.federalregister.gov`, authorized for this build
- **Scope:** corpus and receipts only. No queries drafted, no judging, no retrieval experiments.

Receipt: `docs/evidence/body-retrieval-corpus-2026-08-02/receipt.json`.
Artifacts (gitignored, on disk): `output/body-retrieval-corpus-2026-08-02/`.

## What this settles, in one paragraph

The corpus we could reach offline was 34 documents across seven disjoint
publisher families, and its median pairwise Jaccard — re-measured here with our
own tokenizer at **0.1312**, reproducing the reported ~0.140 — meant no
retrieval configuration could be distinguished from any other, because
recall@50 over 34 documents is 1.0 by arithmetic. This build replaces it with
**993 Federal Register documents with real bodies**, drawn from a single
regulatory program so that vocabulary genuinely competes. Measured on the same
surface with the same instrument, the new corpus's median pairwise Jaccard is
**0.2947** — and its **10th percentile (0.1429) sits above the old corpus's
median**, so 90% of pairs here compete harder than the typical pair did there.
Bodies are long: median 164,381 characters of visible text, and **992 of 993
documents exceed 30,000 characters**, which is the length at which the current
one-chunk-per-document behaviour becomes the thing under test rather than an
implementation detail.

## The draw, and why this program

The offline inventory is larger than previously recorded. The figure of
"~80,000 rows, 15,617 Rules/Proposed Rules" describes
`output/mixed-real-data-corpus-v2/federal_register.parquet`. The full
inventory at `output/rulespec-stabilization-baseline-final/federal_register.parquet`
is **1,004,233 rows, of which 195,752 are Rules or Proposed Rules** — every one
carrying a `body_html_url`. The draw space was ~12x larger than assumed.

Candidate programs were scored offline on both axes that matter, before any
fetch. Coherence is title+abstract pairwise Jaccard; a random draw of
Rules/Proposed Rules scores **0.019** on the same measure and is the control.

| program | docs | median pages | median Jaccard (all) | median Jaccard (>=10pp) |
|---|---|---|---|---|
| FAA airworthiness (14 CFR 39) | 24,157 | 3 | 0.115 | 0.103 |
| Clean Air Act stationary (40 CFR 50–81) | 18,534 | 3 | 0.120 | 0.088 |
| Fisheries (50 CFR 622–679) | 9,604 | 2 | — | — |
| Pesticide tolerances (40 CFR 180) | 3,825 | 5 | 0.150 | 0.137 |
| **ESA species (50 CFR 17 et al.)** | **3,945** | **9** | **0.121** | **0.136** |
| NESHAP (40 CFR 63) | 1,453 | 7 | 0.107 | 0.118 |
| Medicare (42 CFR 405–423) | 1,207 | 20 | 0.076 | 0.090 |

**ESA was chosen because it is the only program whose coherence *rises* when
restricted to long documents** (0.121 → 0.136). Everywhere else, the long
documents are the miscellaneous ones and coherence falls. Medicare has the best
lengths and the worst competition; airworthiness directives are numerous, tightly
worded, and far too short to exercise chunking.

The rule was then tightened, iterating on the offline measure only:

```
document_type   in {Rule, Proposed Rule}
CFR reference   includes 50 CFR 17          (title and part matched as a pair)
topics          include a term containing "endangered"
page span       >= 12 pages (inclusive)     (offline proxy for body length)
publication     year >= 2005                (full-text HTML availability)
```

**Selected: 993 documents** — 520 Proposed Rules, 473 Rules. Achieved
title+abstract Jaccard median **0.163**, against the 0.019 random control.

Why each clause earns its place: 50 CFR 17 is the listing and critical-habitat
part, where every document shares a deep regulatory vocabulary (*endangered*,
*threatened*, *critical habitat*, *species status assessment*, *take*) while
each names a different taxon — shared vocabulary creates the competition, the
taxon creates a resolvable target. The page floor is the only offline signal
that correlates with body length. The year floor is empirical: a 1996 document
returned HTTP 404 for its full-text HTML.

### How well the offline length proxy worked

Well. Page span was the only signal available before fetching, and it held:
the draw's median of 27 pages produced a median of 164,381 visible characters,
i.e. **~6,100 characters per Federal Register page**, consistent across the
corpus. Not one of the 993 documents fell below the 3,000-character threshold
at which chunking is definitionally a no-op, and only one fell below 30,000.

## The fetch

| | |
|---|---|
| requested | 993 |
| fetched | 993 |
| quarantined | **0** |
| failures | **0** |
| resumed run | 993 skipped, **0 requests** |
| total source bytes | 301,787,891 (302 MB) |
| politeness | 1.2 s minimum interval, contactable User-Agent, ~20 min wall clock |

`robots.txt` disallows only search and auth paths; `/documents/full_text/` is
crawlable. The site publishes no `Crawl-delay` and returns no rate-limit
headers, so the interval was chosen conservatively rather than probed for.

Two guards were built that did not fire but should not be removed. A Cloudflare
interstitial returns HTTP 200 with HTML: sealed unchecked it would digest
cleanly, parse into structural passages, and place ~1000 identical copies in the
corpus, so every retrieval number computed on it would describe Cloudflare
rather than the Federal Register. And URLs are read from the parquet, never
reconstructed from a publication date — a hand-built path 404s whenever the
guessed date differs from the recorded one, which happened during probing.

## Measured properties

All measurements on `visible text` — tags stripped, whitespace collapsed — with
a lowercase alphanumeric tokenizer, minimum length 3, minus a deliberately
minimal stoplist. The baseline is re-measured with the identical instrument, so
the comparison is not between two tools.

### Body length (visible characters)

| | min | p10 | median | p90 | max | total |
|---|---|---|---|---|---|---|
| visible text chars | 22,047 | 92,119 | **164,381** | 379,091 | 1,432,103 | 210,535,931 |
| source bytes | 51,674 | 128,243 | 230,444 | 532,130 | 2,807,258 | 301,787,891 |

- documents over 3,000 characters: **993 / 993**
- documents over 30,000 characters: **992 / 993**

### Vocabulary competition — the number the corpus exists for

| | min | p10 | p25 | **median** | p75 | p90 | max | mean |
|---|---|---|---|---|---|---|---|---|
| **this corpus** (993 docs) | 0.011 | **0.143** | 0.249 | **0.2947** | 0.328 | 0.367 | 0.885 | 0.279 |
| baseline (34 docs, re-measured) | 0.005 | 0.047 | 0.082 | **0.1312** | 0.167 | 0.222 | 0.594 | 0.130 |

The median is 2.25x the baseline, but the distribution matters more than the
centre: **the new corpus's p10 exceeds the old corpus's median**. The old corpus
had a long easy tail — a tenth of its pairs shared under 5% of their vocabulary,
which is why any configuration could separate them. Here the easiest tenth of
pairs still shares 14%.

That the re-measurement of the 34-document corpus lands at 0.1312 against a
reported ~0.140 is the check that the instrument is sound. Had it not
reproduced, the corpus number would not have been trustworthy either.

### Program spread — deliberately narrow

| agency slugs | documents |
|---|---|
| interior-department, fish-and-wildlife-service | 986 |
| ...joint with commerce-department, NOAA | 7 |

Page span: min 12, median 27, p90 70, max 321, total 37,838 pages.
Publication years 2005–2026.

### Structural passages

`build_document_release_from_source_cache` produced a real v2 DocumentRelease:
**993 documents, 726,009 structural passages** (median ~700 per document).

Two costs worth recording, because they bear on ingestion: the release JSON is
**1.47 GB** and building it peaked at **17.1 GB resident**. A consumer that
expects to hold a release in memory will not hold this one.

## What this corpus can now answer

- **Whether BM25 length normalization penalises long bodies.** This was the
  urgent unmeasured prediction. With `k1·(1−b+b·len/avg_len)` and 992 of 993
  documents over 30,000 characters — median 164,381 against a prior single
  chunk of 30,487 — the corpus contains exactly the documents the term was
  suspected of punishing, and enough short-vs-long spread (p10 92k, p90 379k,
  max 1.43M) to separate the effect from noise.
- **Whether chunking changes retrieval at all.** Every document is far above
  the threshold below which chunking is a no-op, and 726,009 real structural
  passages exist as an alternative to the current one-projection-per-document
  behaviour.
- **Whether retrieval configurations differ.** Recall@50 over 993 competing
  documents is no longer 1.0 by arithmetic, so arms can now be separated on
  merit. Median pairwise Jaccard 0.2947 means near-misses are plentiful.
- **Ranking quality inside one regulatory program**, which is the realistic
  shape of a user's search session.

## What it still cannot answer

- **Anything cross-agency or cross-publisher.** 986 of 993 documents come from
  one agency. Coherence was bought by narrowing, and this is the price.
- **Anything about short documents.** The 12-page floor is structural; the
  corpus says nothing about how notices or one-page rules rank.
- **Whether findings generalise beyond ESA.** One program by design. A second
  program — NESHAP and pesticide tolerances are the next best candidates — is
  needed before any result can be called general.
- **Anything requiring labels.** No queries were drafted and nothing was judged;
  that is separate work, deliberately not started here.
- **Document types other than Rules and Proposed Rules.**

## Two findings for whoever consumes this

**`full_text_xml_url` is a cleaner source than `body_html_url`.** The Federal
Register publishes a GPO-tagged XML rendition (`<AGENCY> <CFR> <RIN> <SUM>
<EFFDATE> <REGTEXT> <HD> <FTNT> <PRTPAGE>`) with no site chrome and no
zero-width spaces. HTML was fetched anyway because it is the field the parquet
carries, the field `native_structural_passage_spans` handles, and the field the
brief specified; switching rendition mid-build would have been a scope change
rather than a fix. It is the highest-value follow-up.

> **Correction (same day, after measuring it).** An earlier version of this
> section said the HTML carries "~42 zero-width spaces per document". That was
> repeated from a survey rather than measured, and it is wrong in both kind and
> magnitude. There are **no raw U+200B characters at all**; there are `&#8203;`
> **HTML entities**, at a median of **2 per document** (max 24, min 0) across the
> 56 documents measured, and they appear **only inside displayed URLs**
> (`https://www.fws.gov/&#8203;sites/&#8203;default/…`). The defect is real —
> a consumer that resolves entities gets broken URL tokens, and one that does
> not gets a literal `8203` token — but it is confined to URLs, not prose. The
> corrected figures and the rendition comparison are in
> [the follow-up record](body-retrieval-corpus-followup-2026-08-02.md).

**Publisher boilerplate is present and constant.** All 993 documents carry the
same 584-character "Document Headings" explainer — 0.4–0.6% of a typical body.
It is counted, not stripped: this corpus keeps exact publisher bytes, so the
measurement states the constant rather than rewriting the evidence. Note that
`div.document-headings` is the *wrong* selector for removing it, because that
element also wraps the CFR part, docket number, and RIN; `div.fr-seal-block` is
the correct one.

## Reproducibility

The fetch is not reproducible — bytes on a public web server change. The lock
is, and everything downstream of it is. Verified by rebuild:

| artifact | sha256 | rebuild |
|---|---|---|
| `draw-manifest.json` | `cda211eb…7fa3` | byte-identical |
| `cache/source-lock.json` | `4990435c…c996` | byte-identical |
| `measurement.json` | re-measured in the follow-up | byte-identical |
| `document-release.json` | `4b4b0394…fbfd0` | byte-identical |

The draw is a pure function of the input parquet
(`ac18315f…22bf2`) and the recorded rule. `drawn_at` is deliberately absent from
the manifest so that a rebuild compares equal; wall-clock lives in the receipt.
Validation of the cache fails closed: a tampered body, a missing body, a
byte-count mismatch, and an extracted-text digest mismatch are each detected.
