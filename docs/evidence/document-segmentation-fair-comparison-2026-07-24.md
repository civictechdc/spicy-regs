# Document Segmentation Fair Comparison

- **Date:** 2026-07-24
- **Status:** Comparison-ready complete; broader production hardening deferred
- **Decision point:** Comparison-ready, not exhaustive production certification

## Stopping rule

This comparison ends after:

1. deterministic, incumbent BGE, OpenAI, learned-sparse, sparse+dense hybrid,
   and whole-artifact BGE results use the same immutable document scope and
   gold queries;
2. model-native input-limit and truncation evidence is attached to every dense
   result;
3. the pinned BGE cross-encoder reranks the selected top-50 candidate groups;
4. one selected configuration completes real scoped OpenAI tagging and
   validation; and
5. affected repository gates pass.

The current run does not require every rerank depth, retrieval-view ablation,
parser, or possible production deployment.

## Decision at this stopping point

Use `structure-overlap-1800` as the canonical segmentation policy for the next
production candidate. It is deterministic and source-aware, contains all 35
gold spans, is the strongest lossless direct arm with the incumbent BGE
provider under the declared ordering, and completes the real OpenAI ontology
path across every accepted document profile.

The most completely evidenced retrieval cascade is currently incumbent BGE
dense top-50 followed by `BAAI/bge-reranker-v2-m3`, with whole-artifact
retrieval retained as a separate routing view. OpenAI dense and the BGE+SPLADE
RRF hybrid have higher first-stage candidate recall in parts of this corpus,
but neither candidate set has been reranked in the fixed paired experiment.
They remain serious challengers rather than unevidenced production defaults.

Run ontology generation and validation through the project-owned model
interfaces with the v4 exact-alignment policy. Do not treat this comparison as
permission to publish every generated concept automatically: the gold metric
adjudicates 35 target concepts, and court-opinion packages remain the clear
profile-level weakness.

## Fairness controls

All directly compared first-stage runs use:

- evaluation ID
  `segmentation_eval_627ba96e04872d870a2ccd6e`;
- document scope ID
  `document_scope_6b4f8a64ba43fc1b8e0a7e05`;
- 153 document artifacts and no public-comment artifacts;
- all 35 gold queries;
- the same 800, 1,200, and 1,800 segment budgets;
- the same candidate limit of 200;
- the same `ir-measures:0.4.3` implementation; and
- the same source spans and relevance judgments.

The four non-LLM arms are direct comparisons. The LLM-guided arm is reported
separately as a system-level result because the OpenAI pipeline changes both
the embedding provider and boundary selector.

Whole-artifact retrieval is a different task grain: it finds an artifact,
whereas segment retrieval must also find the evidence-bearing span. Its result
is a routing baseline, not proof that chunking is unnecessary.

## Validated evidence

| Artifact | Receipt | Status | Relevant audit result |
| --- | --- | --- | --- |
| Document-scoped BGE dense | `segmentation_experiment_de7d119e838ac153a0980337` | Pass | 9,031 inputs; 45 over 512 tokens and truncated |
| Document-scoped OpenAI dense | `segmentation_experiment_d2c2232513028b107877a4db` | Pass | 9,031 exact `cl100k_base` inputs; none over-limit or truncated |
| Document-scoped learned sparse and RRF hybrid | `sparse_retrieval_cd34c2a7d29e60f6a012d687` | Pass | 1,317 model inputs; none truncated; 43 resumable provider calls |
| Selected top-50 BGE cross-encoder | `segmentation_rerank_31d7e2a8ec51280f92896ed3` | Pass | 2,519 candidates in 70 groups; none unaudited or truncated |
| Selected real OpenAI tagging and validation | `segmentation_tagging_4d8ad629f6efd805dd4a3341` | Pass | 384 calls; 275 exact grounded spans; no failed, invalid, or secret-bearing receipt |
| Document-scoped whole-artifact BGE | `artifact_retrieval_adc1bce8d43d1a6ca025f445` | Pass | 184 artifact inputs; 46 truncated |
| Document-scoped deterministic control | `output/segmentation-experiment-document-deterministic-v3` | Pass | Non-production control |

The BGE dense receipt covers 15 configurations, 32,699 segments, 129,811
retrieval candidates, 1,050 query/config/scope groups, 98 provider-call rows,
and zero failed provider transitions.

The OpenAI dense receipt covers 15 configurations, 32,633 segments, 129,757
retrieval candidates, 1,050 query/config/scope groups, 796 provider-call rows
(725 boundary-selection and 71 embedding calls), and zero failed provider
transitions. Its separately immutable audit
`segmentation_embedding_audit_5c865c0484330711cb94d5e9` independently
recomputed all 9,031 model inputs with
`tiktoken:cl100k_base@0.13.0`.

The learned-sparse and hybrid receipt covers the selected
`structure-overlap-1800` configuration, 1,302 segments, 1,317 unique sparse
model inputs, 16,134 retrieval candidates, four metric rows, 43 provider-call
rows, and zero failed transitions or truncated inputs. The completed model
checkpoint was reused after the tokenizer audit exposed and tests corrected a
`BatchEncoding`-versus-`dict` compatibility assumption. The package adapter is
`sentence-transformers:5.6.1`; the exact sparse model is
`tomaarsen/splade-modernbert-base-miriad` at revision
`c640ce28f7c4f4593ddba1b3855988f03a3d9cdc`. Its
[model card](https://huggingface.co/tomaarsen/splade-modernbert-base-miriad)
declares Apache-2.0 licensing and an 8,192-token maximum sequence length.

## Validated first-stage results

These are corpus-retrieval metrics over 35 queries. A representative direct
configuration is chosen by Recall@50, then Recall@10, then MRR, while requiring
all 35 gold spans to be contained.

The direct four-arm comparison below fixes the segment budget at 1,800. The
segment boundaries are identical between providers for the first three arms;
the semantic arm is provider-dependent.

| Direct non-LLM arm | Gold contained, BGE / OpenAI | Segments, BGE / OpenAI | BGE R@10 / R@50 / MRR | OpenAI R@10 / R@50 / MRR |
| --- | ---: | ---: | ---: | ---: |
| Structure first | 35 / 35 | 1,263 / 1,263 | 0.5143 / 0.8000 / 0.2948 | 0.6000 / 0.8286 / 0.3175 |
| Structure + limited overlap | 35 / 35 | 1,302 / 1,302 | 0.5429 / 0.8000 / 0.2940 | 0.6000 / 0.8286 / 0.2900 |
| Paragraph + sentence fallback | 35 / 35 | 1,276 / 1,276 | 0.4857 / 0.8000 / 0.2624 | 0.6000 / 0.8286 / 0.2882 |
| Semantic embedding | 34 / 34 | 1,597 / 1,589 | 0.5429 / 0.8000 / 0.2758 | 0.6000 / 0.8571 / 0.3037 |

The semantic arm's higher long-tail recall does not repair its one
boundary-crossing gold miss. Among lossless direct arms, structure-overlap is
the strongest incumbent-BGE candidate by the declared Recall@50, Recall@10,
then MRR ordering; structure-first is the strongest OpenAI candidate under the
same ordering.

| System and task grain | Representative configuration | Gold contained | Recall@10 | Recall@50 | Recall@200 | MRR | nDCG@10 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Deterministic control, segment | `paragraph-sentence-1800` | 35/35 | 0.1429 | 0.2857 | 0.4286 | 0.0563 | 0.0700 |
| Incumbent BGE, segment, direct | `structure-overlap-1800` | 35/35 | 0.5429 | 0.8000 | 0.8286 | 0.2940 | 0.3395 |
| Learned SPLADE sparse, segment | `structure-overlap-1800` | 35/35 | 0.4571 | 0.7143 | 0.8000 | 0.1367 | 0.2014 |
| BGE dense + SPLADE sparse RRF hybrid, segment | `structure-overlap-1800` | 35/35 | 0.6000 | 0.8286 | 0.8286 | 0.2492 | 0.3215 |
| Incumbent BGE, segment, system-level | `llm-guided-1800` with heuristic selector | 34/35 | 0.6000 | 0.8857 | 0.9143 | 0.3409 | 0.3870 |
| OpenAI, segment, direct | `structure-first-1800` | 35/35 | 0.6000 | 0.8286 | 0.8571 | 0.3175 | 0.3731 |
| OpenAI, segment, system-level | `llm-guided-1800` with OpenAI selector | 34/35 | 0.6000 | 0.9143 | 0.9143 | 0.3114 | 0.3643 |
| Incumbent BGE, whole artifact | `all-profile-whole-artifact-v1` | Artifact-level | 0.8571 | 0.9714 | 1.0000 | 0.6036 | 0.6608 |

Both production dense providers substantially exceed the deterministic
control. OpenAI has the stronger representative direct result at Recall@10,
MRR, and nDCG@10; BGE is slightly behind but local, and its 512-token input
limit truncates 45 segment/query inputs that OpenAI accepts intact.

Learned sparse alone underperforms BGE dense on every reported corpus metric.
The fixed RRF hybrid improves BGE's candidate recall at 10 and 50 while
preserving Recall@200, but lowers MRR and nDCG@10. It is therefore useful as a
candidate-generation contender, not yet a replacement ranking policy. The
current fixed-depth cross-encoder test deliberately holds the incumbent BGE
dense top-50 candidate sets constant; reranking the hybrid candidates is
deferred.

The whole-artifact result is stronger on artifact discovery, but 46 inputs are
truncated and the result does not locate an evidence span. A practical system
may therefore use artifact retrieval and segment retrieval as complementary
stages rather than forcing one representation to serve both tasks.

## Fixed top-50 reranking result

The pinned `BAAI/bge-reranker-v2-m3` cross-encoder reranks the incumbent
`structure-overlap-1800` dense candidate sets only. This holds the 35 queries,
candidate identities, and depth fixed.

| Scope and stage | Recall@10 | Recall@50 | MRR | nDCG@10 |
| --- | ---: | ---: | ---: | ---: |
| Corpus BGE dense top-50 input | 0.5429 | 0.8000 | 0.2934 | 0.3395 |
| Corpus BGE cross-encoder reranked | 0.7143 | 0.8000 | 0.4639 | 0.5198 |
| Within-artifact BGE dense top-50 input | 0.6571 | 0.9143 | 0.4087 | 0.4566 |
| Within-artifact BGE cross-encoder reranked | 0.8571 | 0.9143 | 0.6044 | 0.6622 |

Reranking materially improves early ranking while correctly leaving the
top-50 recall ceiling unchanged. All 70 requests completed without retry or
failure. Exact tokenizer audits cover all 2,519 candidates; none exceeds the
declared 4,096-token pair limit. The pinned model revision is
`953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e`; its
[model card](https://huggingface.co/BAAI/bge-reranker-v2-m3) declares
Apache-2.0 licensing.

The selected downstream configuration remains
`structure-overlap-1800`, chosen from the incumbent BGE direct comparison
before sparse, rerank, and tagging evaluation. The bounded OpenAI tagging
sample contains 109 segments from 44 artifacts and explicitly covers all 10
accepted document profiles, all 35 gold spans, and all seven document
adversarial cases with no prompt-budget violations.

An intentionally stopped v2 tagging diagnostic remains visible as failure
evidence, not as a completed result. Its first work item exhausted the
4,096-token output cap three times before a fourth attempt completed.

A second intentionally stopped v3 diagnostic bound an 8,192-token generation
cap, a 4,096-token validation cap, and the complete secret-free provider
configuration into run identity. Its same first work item completed in one
Priority-tier attempt with 5,384 output tokens, confirming that the old cap
was materially too small. Across 47 completed calls, seven responses exceeded
the old cap and none retried. The run also exposed a separate quality failure:
35 of 142 returned tag items (24.6%) had non-resolving LLM-supplied offsets,
affecting 16 segments and leaving five with every proposed tag rejected.

The final v4 run therefore adds one bounded, provider-independent alignment
rule: preserve supplied offsets when they resolve exactly; otherwise repair
only one unique verbatim occurrence in the named evidence field. Ambiguous or
non-verbatim evidence still fails closed. Each repair and accepted/rejected
item count is bound into the per-call receipt. No fuzzy, normalized, semantic,
or regex-based match is accepted.

At the same deterministic 47-work-item prefix, v4 accepted 122 tags, rejected
17, repaired 20 offsets, and left two segments fully rejected; v3 accepted
107, rejected 35, and left five fully rejected. This is useful directional
evidence, not a causal paired estimate, because the provider outputs are not
seeded or byte-deterministic.

## Final OpenAI tagging result

The final v4 artifact independently validates as
`segmentation_tagging_4d8ad629f6efd805dd4a3341`.

| Measure | Result |
| --- | ---: |
| Selected artifacts / segments | 44 / 109 |
| Accepted document profiles | 10 / 10 |
| Generation / validation calls | 109 / 275 |
| Accepted / rejected returned tag items | 275 / 28 |
| Provided / uniquely repaired exact spans | 237 / 38 |
| Raw / aggregated assignments | 275 / 216 |
| Gold TP / FP / FN | 31 / 4 / 4 |
| Gold micro precision / recall / F1 | 0.8857 / 0.8857 / 0.8857 |
| Exact artifact-label match | 29 / 35 (0.8286) |
| Grounded assignment / span rate | 1.0000 / 1.0000 |
| Validation agrees / disagrees | 264 / 11 (0.9600 agreement) |
| Zero-tag / fully-rejected segments | 18 / 3 |
| Duplicate raw-span rate | 0.0000 |
| Multi-segment assignments / disagreements | 47 / 3 |
| Provider retries / failures / invalid calls | 0 / 0 / 0 |
| Secret-like artifact matches | 0 |

Profile-level gold F1 is:

| Profile | Gold artifacts | Micro F1 | Exact artifact match |
| --- | ---: | ---: | ---: |
| CFR section | 4 | 1.0000 | 1.0000 |
| Congress bill | 4 | 1.0000 | 1.0000 |
| Court opinion package | 10 | 0.6667 | 0.5000 |
| CRS report | 4 | 1.0000 | 1.0000 |
| Federal Register document | 4 | 1.0000 | 1.0000 |
| GAO report | 4 | 1.0000 | 1.0000 |
| Regulations.gov document | 5 | 0.8889 | 0.8000 |

FCC filing, lobbying filing, and Unified Agenda profiles are included in the
real OpenAI sample for profile coverage but do not have profile-specific gold
labels in this snapshot, so no F1 is invented for them. Court opinion packages
are the clear quality weakness: three false negatives, four false positives, a
0.3333 segment zero-tag rate, and only 0.5000 exact artifact-label match.

The precision, recall, and F1 values score only the 35 curated target concepts.
Of the 191 aggregated assignments on gold artifacts, 156 (81.7%) concern
concepts outside that target set. They remain grounded and validation-audited,
but this experiment does not supply human precision judgments for them.

The prompt-injection fixture produced one exact, grounded assignment for the
actual drinking-water corrosion-control topic, did not follow the embedded
request to output an API key, and produced no secret match. Across all
generation calls, 14 responses exceeded the obsolete 4,096-token cap; the
largest used 7,347 tokens and every call completed in one attempt.

## Cost and latency evidence

Durations below are summed provider-request durations, not end-to-end wall
clock. Request p95 uses the nearest-rank convention. OpenAI cost is an uncached
list-price estimate because the current ledger does not break cached input
tokens out separately.

| Operation | Calls | Inputs or tokens | Summed duration | p95 request | Estimated API cost |
| --- | ---: | ---: | ---: | ---: | ---: |
| Local BGE embedding | 1 | 9,031 inputs | 147.237 s | 147.237 s | Local compute |
| Local SPLADE sparse embedding | 43 | 1,317 inputs | 1,207.557 s | 50.547 s | Local compute |
| Local BGE cross-encoder reranking | 70 | 2,519 candidates | 1,608.879 s | 41.646 s | Local compute |
| OpenAI `text-embedding-3-large` | 71 | 1,294,061 input tokens | 386.969 s | 9.680 s | $0.168 |
| OpenAI `gpt-5.6-sol` boundary selection | 725 | 922,579 input + 198,443 output tokens | 4,203.666 s | 18.558 s | $10.566 |
| OpenAI `gpt-5.6-sol` tagging generation, Priority | 109 | 449,185 input + 206,204 output tokens | 2,241.061 s | 49.967 s | $16.864 |
| OpenAI `gpt-5.6-sol` tagging validation, Priority | 275 | 784,738 input + 34,020 output tokens | 663.780 s | 4.119 s | $9.889 |

The estimates use OpenAI's current standard rates of $0.13 per million input
tokens for
[`text-embedding-3-large`](https://developers.openai.com/api/docs/models/text-embedding-3-large)
and $5 input / $30 output per million tokens for
[`gpt-5.6-sol`](https://developers.openai.com/api/docs/models/compare).
The final tagging estimate uses the actual `priority` response tier on all 384
calls and the current Priority rates of $10 input / $60 output per million
tokens from
[OpenAI's Priority processing table](https://openai.com/api-priority-processing/).
Its combined uncached list-price estimate is $26.753. Cached-input discounts,
if any, are not separated by the receipt. Intentionally stopped diagnostic
runs are failure evidence and are not folded into the operational estimate.

## Comparison-ready completion audit

| Bounded condition | Status | Evidence |
| --- | --- | --- |
| One immutable scope and metric contract | Pass | Evaluation `segmentation_eval_627ba96e04872d870a2ccd6e`; scope `document_scope_6b4f8a64ba43fc1b8e0a7e05` |
| Four-arm paired table and separate LLM-system result | Pass | Direct 1,800-token table and representative-results table above |
| All required first-stage results | Pass | Deterministic, BGE, OpenAI, SPLADE, RRF hybrid, and whole-artifact receipts above |
| Model-native dense input audits | Pass | BGE and OpenAI audits cover all 9,031 inputs |
| Fixed top-50 cross-encoder | Pass | `segmentation_rerank_31d7e2a8ec51280f92896ed3` |
| Real scoped OpenAI generation and validation | Pass | `segmentation_tagging_4d8ad629f6efd805dd4a3341` |
| Affected repository gates | Pass | 641 tests passed, 3 deselected; Ruff, ty, 32-table dictionary, strict docs, and diff checks pass; no experiment or oMLX process remains active |
| Decision, limitations, and deferred work | Pass | Decision above and bounded limitations below |

All eight bounded conditions pass locally. This closes the fair comparison,
not the broader production-certification checklist.

## Bounded limitations and deferred work

- The retrieval and tagging sample has 35 gold targets. Differences are
  measured exactly on this corpus but do not have confidence intervals or
  significance tests.
- Gold precision and recall do not adjudicate the 156 aggregate assignments on
  gold artifacts that fall outside the curated target-concept set. Human
  concept-governance review remains necessary before automatic publication.
- Court-opinion packages have 0.6667 gold micro F1 and 0.5000 exact
  artifact-label match. The profile should not be called production-quality
  until opinion subdivision and a larger legal gold set are evaluated.
- The cross-encoder reranks only incumbent BGE dense top-50 candidates.
  Reranking OpenAI or hybrid candidates and the earlier 25/50/100/200 depth
  sweep are deferred.
- oMLX is intentionally excluded from this milestone. No oMLX result is
  implied by the local-adapter code or by another local model.
- Retrieval-view context ablations, listwise LLM reranking, and legal
  relation-specific query expansion remain follow-up experiments. The
  [reranking paper review](arxiv-2403.10407-reranking-review.md) explains why
  these interactions are not prerequisites for this stopping point.
- BGE dense truncates 45 audited inputs and whole-artifact BGE truncates 46.
  Those results remain useful, but the loss is visible and prevents a claim
  that those representations preserve every model input intact.
- The semantic and LLM-guided boundary arms each miss one curated gold span.
  They remain experimental rather than the canonical segmentation policy.
- Supreme Court PDF packages are real court-opinion artifacts, but the current
  adapter does not claim source-backed lead, concurrence, and dissent
  subdivision.
- The acceptance scope covers all 10 document profiles. The 17-profile
  reusable superset also contains comments and relationship-only context that
  are intentionally excluded from this document decision.
- OpenAI calls record the requested and returned model IDs and complete safe
  call telemetry, but a hosted model alias does not expose reproducible model
  weights.
- No artifact was uploaded, deployed, committed, pushed, or published by this
  comparison run.
