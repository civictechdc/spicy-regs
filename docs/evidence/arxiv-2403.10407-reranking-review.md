# Review of arXiv 2403.10407 for Spicy Regs

- **Reviewed:** 2026-07-24
- **Paper:** Déjean, Clinchant, and Formal, “A Thorough Comparison of
  Cross-Encoders and LLMs for Reranking SPLADE,” arXiv:2403.10407
- **Source:** [paper PDF](https://arxiv.org/pdf/2403.10407)
- **Decision:** Adapt; do not use this paper to select a chunker, embedding
  provider, or ontology identity rule.

## What the paper tests

The paper compares conventional cross-encoders with zero-shot listwise LLM
rerankers after a strong SPLADE first-stage retriever. It evaluates several
TREC, BEIR, and LoTTE passage-retrieval datasets, primarily with nDCG@10. It is
a reranking study: the documents or passages are already supplied, so it does
not compare structural, semantic, overlapping, or LLM-guided segmentation.

The evaluated pipeline is:

1. retrieve candidates with a SPLADE variant;
2. rerank them with a DeBERTa/ELECTRA cross-encoder or RankGPT;
3. in one limited experiment, cascade SPLADE, DeBERTa, and GPT-4.

RankGPT uses overlapping candidate windows, shortens documents, and repairs
incomplete permutations by appending omitted IDs in original-retriever order.
That repair avoids catastrophic rankings, but it can conceal malformed model
output unless the repair rate is reported separately.

## Findings that matter here

- A trained cross-encoder remains a strong production contender. GPT-4 wins
  some datasets and loses others; GPT-3.5 can materially degrade a strong
  first-stage ranking.
- A larger rerank depth can help, but the effect depends on the dataset and
  reranker. A fixed top-50 candidate limit is therefore an experimental choice,
  not a universal constant.
- Longer LLM context is not automatically better. Some reported top-100
  RankGPT configurations trail top-25 configurations.
- Metadata context matters. Removing titles substantially hurts the tested
  cross-encoder, supporting a separate retrieval representation containing
  document title and structural path.
- The ArguAna failure is especially relevant to legal retrieval: ordinary
  topical relevance is not the same task as finding a counterargument. Legal
  relations such as amends, repeals, distinguishes, criticizes, conflicts with,
  or cites must be evaluated separately rather than averaged away.
- The paper does not run significance tests, has small query sets for several
  comparisons, leaves many LLM experiment cells empty, and does not normalize
  cost or latency. Small score differences are not sufficient production
  evidence.

## Spicy Regs consequences

### Retrieval and segmentation

Treat segmentation, first-stage retrieval, candidate depth, and reranking as a
factorial system. A chunker that wins with dense top-50 retrieval may not win
with sparse or hybrid top-200 retrieval plus a cross-encoder.

Add learned-sparse and sparse+dense-hybrid retrieval contenders. Exact legal
terms, citations, section numbers, acronyms, and defined phrases make lexical
matching an important complement to embeddings.

Test these retrieval-only representations while keeping canonical evidence text
unchanged:

1. raw segment;
2. title plus raw segment;
3. title and full heading path plus raw segment; and
4. title, heading path, and source type plus raw segment.

Every variant must resolve to the same source field, character offsets, and
source-text digest. Context is a derived retrieval aid, not evidence.

### Reranking

Keep a cross-encoder as the default production contender. Sweep rerank depth at
25, 50, 100, and 200 instead of fixing one depth. Hold candidate sets constant
when comparing rerankers.

Keep listwise LLM reranking offline or in a narrow final stage until it shows a
paired, profile-safe gain over the BGE and oMLX Qwen contenders. For listwise
models:

- repeat candidate-order permutations;
- require a complete, duplicate-free output permutation;
- count repaired or malformed outputs as failures;
- record token use, dollar cost, p50/p95 latency, throughput, and peak memory;
  and
- do not let a ranking establish artifact identity or ontology relationships.

The current thin Sentence Transformers adapter remains the preferred
package-first implementation. Use the Apache-2.0
[BAAI/bge-reranker-v2-m3](https://huggingface.co/BAAI/bge-reranker-v2-m3)
baseline rather than reproducing the paper’s research checkpoints. Evaluate
[FlagEmbedding](https://github.com/FlagOpen/FlagEmbedding) only if its combined
dense/sparse/multi-vector path or BGE-specific fine-tuning is needed. If
RankGPT is evaluated, use [RankLLM](https://github.com/castorini/rank_llm)
instead of owning listwise windows, permutation parsing, and repair code.

The exact Naver research checkpoints cited by the paper require licensing
review and should not become production defaults.

## Required evaluation additions

1. Measure first-stage candidate Recall@10/25/50/100/200 before reranking.
2. Report paired nDCG@10 and MRR intervals, not only point estimates.
3. Stratify by document profile and query relation: topical relevance, exact
   citation, authority, amendment/repeal, contrary authority, date/version, and
   cross-document relationship.
4. Test head-only, head-plus-tail, and structure-aware truncation. Persist exact
   model-token counts and never truncate silently.
5. Preserve 100% evidence-coordinate resolution across every contextual
   retrieval representation.
6. Report whether the ordering of segmentation arms is stable across BGE,
   OpenAI, and oMLX embeddings and before versus after reranking. Publish the
   interaction when no universal winner exists.

## Acceptance rule

A production reranking change must improve paired nDCG@10 or MRR without
reducing Recall@50 by more than one percentage point overall or three points in
any document profile, must preserve all evidence coordinates, and must meet a
predeclared latency and cost envelope. Relation-specific failures remain visible
even when excluding them would improve the aggregate.
