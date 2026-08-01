# Sealed SEARCH holdout — content-blind draw, 2026-08-01

**Status: drawn, unlabelled, sealed.** This is the label-free half of the
search-holdout protocol: which matters are held out is now fixed, before any
deep tuning of the search spine. No label exists and none may be created
until the rules below are satisfied. Drawing this cost nothing and is
contamination insurance for everything after.

Tool: `tools/draw_search_holdout.py` (tested by
`tests/test_draw_search_holdout.py`; run targeted, never the full suite).
Sealed artifacts live in `output/search-holdout-draw-2026-08-01/`
(`sealed-manifest.json`, `draw-receipt.json`) — gitignored output, pinned
here by digest.

## Design

- **Unit = the whole matter.** A matter is a docket family / RIN family /
  cross-post cluster: one connected component over identity keys from three
  published tables — `proceedings` (proceeding ↔ docket / FR document / RIN),
  `agenda_item_proceedings` (RIN ↔ proceeding), `fr_docket_links`
  (FR document ↔ docket, FR document ↔ RIN). 1,861,427 identity keys
  partition into 580,738 matters; the partition proof (every key in exactly
  one matter) is recorded in the receipt. **Every matter lands wholly in
  exactly one split** — holdout or development — so no docket, RIN, FR
  document, or proceeding of a held-out matter can leak into tuning data.
- **Content-blind seeded order.** Protocol reimplemented from
  `tools/draw_holdout.py:538-548`: a matter is ranked by
  `sha256(seed \x1f procedure \x1f matter_key)`, where `matter_key` is the
  canonical JSON of its sorted identity members. Content never enters the
  key; the draw path reads identity and date columns only (declared as
  `*_DRAW_COLUMNS` in the tool — no title, no abstract, no text).
- **Strata** = source class × matter size × date era.
  - Source class: which tables evidence the matter — `proc-only`,
    `proc+agenda`, `proc+fr`, `proc+agenda+fr`, `fr-only`.
  - Size (identity-key count): single (1), small (2–4), medium (5–16),
    large (17–64). Matters above 64 keys are **oversize and ineligible**
    (288 of them, including the ~100k-node cross-post hairball); they stay in
    the development split by design.
  - Era (latest FR publication or agenda evidence date): pre-2010,
    2010-2017, 2018-2022, 2023-plus, undated.
- **Allocation** (all declared constants, no row ever named): target 240
  matters, proportional largest-remainder across strata with census ≥ 50,
  clamped to [2, 24] per stratum. 46 strata qualified; 93 matters sit in
  below-floor strata and stay in development.

## The draw (2026-08-01T21:16:48Z)

240 matters drawn to the holdout; 580,498 matters remain development.

| dimension | drawn |
|---|---|
| source class | fr-only 95 · proc+fr 54 · proc+agenda+fr 40 · proc+agenda 27 · proc-only 24 |
| size | small 144 · medium 57 · large 39 |
| era | pre-2010 68 · 2010-2017 58 · 2018-2022 47 · 2023-plus 43 · undated 24 |

The full 46-row stratum table (census / quota / drawn per stratum) is in the
receipt; membership (matter ids + member identity keys) is in the sealed
manifest.

## Seal

| constant | value |
|---|---|
| selection procedure | `search-holdout-matter-seeded-stratified-v1` |
| selection seed | `search-holdout-draw-2026-08-01` |
| dataset id | `search-holdout-matters-2026-08-01-v1` |
| draw schema | `search-holdout-draw-v1` |

| digest | sha256 |
|---|---|
| selection (procedure + strata + membership) | `e270fdde3c728d1b9a81aa78211186929838d5535103e6b897340af380968caf` |
| membership | `9edc34acb7a98ffbb58412380099425ef616157e16f43ebfaf38c35fa0328bdc` |
| sealed-manifest.json (file bytes) | `b4737fb07f0d5e70652286de8d1e61aa7b3b92d040aac1321e9f3b1fbfcadc6e` |

Inputs consumed (local materialization of ontology snapshot
`snapshot_0e4b4204bdfbd462a9270fcd766fb8dd`, asserted 2026-07-24, in
`output/rin-ontology-revision-candidate/`):

| input | sha256 |
|---|---|
| proceedings.parquet (511,643 rows) | `e49cb37ac2a97465a79408a0f04df2d5a983dc3bd2e1dbb024720ab62c829682` |
| agenda_item_proceedings.parquet (120,685 rows) | `e3dea44081313dde9af949220526f25f2d2036ea4d722aa51ef6d8e5d95d824a` |
| fr_docket_links.parquet (715,080 rows) | `b3409f0ada792a8c9534edcf87c290a8b39e482e4803f08656bfa9de4504fd45` |
| ontology-dataset-manifest.json | `271f88447fc5818b8b1d9deaca8c6e7df02cf3adcd1dfe9c9e680e556a63c45b` |

The proceedings and agenda digests equal the ones asserted in the pinned
ontology-dataset manifest — the draw consumed exactly the snapshot the
pipeline published.

## Blindness proof

`assert_blind` (pattern from `tools/draw_holdout.py:854-892`) ran **twice** —
once on the in-memory manifest, once on the re-parsed sealed bytes — and both
runs are recorded in the receipt (`blindness_first_run`,
`blindness_second_run`, `blindness_runs_match: true`). Two independent
checks each time:

1. **Banned keys**: no key anywhere in the manifest may carry any of the 26
   banned substrings (title, abstract, text, summar, snippet, concept,
   label, score, relevan, quer, judg, embedding, tagger, …). Found: none.
2. **Leaked scalars**: no string scalar anywhere may equal any of the
   817,578 title/abstract values in the inputs (compared by digest; the
   checker reads content solely to prove its own absence). Found: none.
   2,793 string scalars checked.

## Rules of the seal (bind at labeling time)

1. **Configuration freezes before labels.** The retrieval configuration
   under evaluation must be frozen and pinned before any label for this
   holdout exists. Tuning against these matters, their documents, or any
   statistic derived from them voids the seal.
2. **One-shot opening.** The holdout opens once: one scored evaluation per
   sealed configuration. Repeated peeking, threshold sweeps, or per-matter
   inspection after labels exist voids the seal.
3. **Two independent judge families.** Labeling requires two independent
   judge families — separate model families or vendors sharing no code and
   no world-model with each other or with the system under evaluation.
   Three sessions of one model count as one family.

## Reproduce / verify

```sh
uv run python tools/draw_search_holdout.py --verify   # recompute from inputs, compare digests
uv run pytest tests/test_draw_search_holdout.py       # hermetic protocol tests (targeted only)
```

Re-running the draw with the same seed, procedure, and inputs reproduces the
sealed manifest byte-for-byte (verified 2026-08-01: both digests match).
