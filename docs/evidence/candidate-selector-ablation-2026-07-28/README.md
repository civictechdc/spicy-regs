# Candidate-selector ablation — 2026-07-28

Development-only evidence, preserved from the session scratchpad. The
results here were produced by `tools/ablate_candidate_selectors.py`
(commit `4ac2cdb`) over the fused registry
(`output/fused-concept-registry-v1/registry.parquet`, sha256
`a82cdebc…`) and the frozen 35 dev segments.

**Why this file exists:** these numbers were cited in decisions all day
(notably `v2+C+D` at 5/8) while living only in an ephemeral scratchpad.
A later agent, reading only committed evidence, correctly reported that
no `D`-row result existed in the repo. It did exist — here — but had no
durable provenance. Committing it closes that gap.

## Files

- `ablation.md` / `ablation.json` — the nine-configuration table:
  exact-alias targets surfaced (the oracle), adequate targets kept, mean
  and median rank, and the scheme mix of the 12 slots.
- `keywords.json` + `keyword-calls/` (35 files) — channel D's actual run:
  35 provider calls to `openai:gpt-5.6-sol`, 291 generated keywords
  (5-10 per segment), with request and response stored per segment.
  **Channel D was run**; this is its record.

## Reading the table

`v1` is the production selector run whole. `v2` is the anchored-lexical
(A) + char-3-gram (B) pair fused by RRF k=60 with source-vocabulary
quotas. `+C` adds dense BGE retrieval, `+D` adds free-keyword
generate-then-map, `BM25+B+C` swaps BM25 (E) for the anchored channel.

Two readings that shaped later decisions:

1. **`v2+C` scores 4/8 — identical to `v2`** — but mean rank drops 5.25
   → 2.0. The dense channel *reorders* what the lexical channels already
   found rather than finding more. Judge candidate generation by the
   oracle, not by aggregate recall.
2. **`v2-noquota` also reaches 5/8, but adequate-kept collapses 4/5 →
   2/5** as fast-topical floods to 75% of slots. The quota buys scheme
   balance; at four channels it is free, at two it costs `judicial
   power`.

## Caveats

Development-only: the 35 items are permanently train/dev per
`evaluation-boundary.json`, so nothing here can support an adoption or
accuracy verdict. No selector is adopted on the strength of this table.
