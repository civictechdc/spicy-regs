# Candidate-selection research — 2026-07-27

Blind external research (claude-fable-5 subagent; web sources only, no
repo access) on how production systems generate concept candidates from
large controlled vocabularies. Commissioned after the fused-registry
round showed the lexical selector as the binding constraint
(`gold-adjudication-2026-07-27/README.md`, round 2).

## Per-system findings (condensed; sources at end)

- **NLM MTI/MTIX (MeSH, PubMed):** two parallel candidate generators
  (lexical MetaMap + k-NN over neighbor documents' labels), union,
  learned rank, rule filter. Two transferable lessons: recall-flooded
  candidate lists *degrade* the downstream consumer (precision rebalance
  0.30→0.50 drove adoption; 0.8646 by 2022), and suppressing highly
  ambiguous concepts plus ≤2-character aliases yields 89.5% precision at
  78.6% recall in MetaMap.
- **Annif (national libraries):** ensemble of heterogeneous backends —
  Omikuji/Bonsai (XMC label trees), MLLM (lexical matching scored by
  deterministic features: pref-vs-alt label, TF-IDF spread, token
  ambiguity across concepts, hierarchy support), fastText, XTransformer.
  Won SemEval-2025 quantitative track over 200,035 GND subjects with
  81,937 training records; lexical matching earned a real minority share
  (13–20% ensemble weight). Ops warning: LCSH-scale vocabularies at
  >34GB RAM / ~28min load in library tooling — build offline, memory-map.
- **SemEval-2025 LLMs4Subjects** (the exact retrieve-then-LLM
  architecture, 200+ teams): winners used dense retrieval (BGE-M3,
  Arctic-Embed) to guarantee ~50 candidates, then reranked with lexical
  evidence as a feature; the qualitative-track winner inverted the flow
  (LLM free-generates keywords → embed → nearest-neighbor into the
  vocabulary → LLM rescore).
- **EURLEX57K (EuroVoc):** trained classifiers fail on zero-shot labels
  (163 of 4.3k). At 500k concepts most of the vocabulary is zero-shot
  forever — the structural argument for retrieval over classification.
- **Entity linking (BLINK/GENRE/scispaCy/SapBERT):** standard recipe is
  alias table + priors → dense bi-encoder retrieval → cross-encoder
  rerank. scispaCy's generator: char-3-gram TF-IDF over 2.78M
  concept+alias strings with ANN, ~1.1GB — deterministic, zero training,
  and the only channel that behaves on chemical nomenclature.
- **Westlaw / CRS:** at highest stakes the industry still uses human
  editors (Thomson Reuters hired 250 attorneys for Precision tagging;
  CRS analysts assign Congress.gov subjects manually).

## The convergent architecture

(1) vocabulary conditioning (normalize, per-alias ambiguity + IDF,
suppress junk) → (2) 2–4 independent generators in parallel →
(3) fusion (union/RRF, never one ranker) → (4) cheap feature rerank →
(5) constrained decision from the shortlist, with candidate-stage
recall@K measured separately as the hard ceiling on end-to-end quality.

## Ranked recommendations (evidence-per-effort, zero training data)

1. **Anchored lexical matching + ambiguity suppression:** word-boundary
   matching only; suppress ≤2-char aliases; require an IDF-floor anchor
   token; down-weight aliases ambiguous across many concepts; score
   with MLLM-style deterministic features.
2. **Char-3-gram TF-IDF over label+alias strings** (scispaCy recipe) as
   a second generator — catches morphology/near-matches, handles
   chemicals.
3. **RRF fusion (k=60) of anchored + char-ngram + dense BGE** channel,
   cross-encoder rerank on the pooled 50–200 only. Hybrid union
   evidence: recall@16 0.717 lexical / 0.779 dense → 0.930 hybrid.
4. **Source-vocabulary-stratified quotas** for the top-12 — structurally
   prevents a 440k-row thesaurus from crowding out a 900-row authority
   vocabulary; semantic facets remain a separate profile-policy gate. No
   score calibration reliably fixes vocabulary-size imbalance.
5. Generate-then-map (LLM keywords → embed → map) as a later channel.
- **Defer XMC** (Omikuji/PECOS): needs tens of thousands of labeled
  documents and never reaches unseen labels. Harvest accepted
  assignments as silver data; revisit at ~50k labeled segments.

## Local BM25 baseline — development only

The missing sparse-search baseline was added on 2026-07-27 as channel E using
the pinned `bm25s==0.3.10` Lucene method. It indexes each active concept as its
preferred label plus registered aliases; definitions are excluded. The run
used the same 513,236-row registry, 35 development items, top-12 prompt limit,
facet filter, and real prompt preflight as v2; the fused variants also used
the same source-vocabulary quotas. It made no provider calls.

| Configuration | Exact-alias surfaced | Adequate target kept |
| --- | ---: | ---: |
| `anchored-hybrid-v2` (A+B) | 4/8 | 4/5 |
| v2 + dense (A+B+C) | 4/8 | 4/5 |
| BM25 alone (E) | 1/8 | 1/5 |
| BM25 + char n-gram (E+B) | 1/8 | 1/5 |
| BM25 + char n-gram + dense (E+B+C) | 2/8 | 4/5 |

BM25 built its full sparse index in 7.680 seconds, and ranking every requested
channel took 3.696 seconds. Its failure was retrieval quality, not runtime.
The global ranking was dominated by FAST (`BM25-alone`: 400/419 prompt slots),
and the source quotas could not recover candidates absent from the channel's
top 50. A diagnostic search to depth 5,000 found `human rights` at rank 824
and `free speech` at rank 4,815; five other missed exact targets remained
absent.

**Disposition:** keep BM25 as a fast regression baseline, but do not replace
the anchored matcher or promote any BM25 configuration. These are inspected
development results, not holdout or adoption evidence. Local record SHA-256:
`67c6565d93739045ad978242d5b7a4e1a595dacc17bfc5c984d53a2d94f08a6d`.

## Evaluation pitfalls

P@k gameable by head labels (use propensity-scored variants when
training arrives); R@K punishes documents with >K gold; gold absence is
not a negative; measure candidate recall@12 separately from end-to-end;
don't trust generic leaderboards for legal text.

## Sources

Annif (annif.org; MLLM/STWFSA wiki; SemEval-2025 paper
arxiv.org/html/2504.19675v1) · LLMs4Subjects overview
(arxiv.org/html/2504.07199) and DNB-AI-Project (arxiv.org/pdf/2504.21589)
· NLM MTI (ii.nlm.nih.gov/MTI/), 10-year review (Frontiers 2023),
MEDLINE 2022 transition, MTIX FAQ · UMLS content views
(S1532046410000201) · scispaCy (aclanthology.org/W19-5034) · SapBERT
(2021.naacl-main.334) · BLINK (2020.emnlp-main.519) · GENRE
(openreview.net/pdf?id=5k8F6UU39V) · TweetNERD hybrid retrieval
(arxiv 2210.07472) · RRF (Cormack et al. 2009) · JEX (L12-1519) ·
EURLEX57K (P19-1636) · OpenAlex Topics · PECOS · Jain et al. KDD 2016 ·
Westlaw Precision (lawnext.com 2022) · FlashText (arxiv 1711.00046)
