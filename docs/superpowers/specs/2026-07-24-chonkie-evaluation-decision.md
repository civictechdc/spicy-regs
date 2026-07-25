# Decision: Treat Chonkie as an Optional Benchmark

- **Date:** 2026-07-24
- **Status:** Accepted
- **Scope:** Document segmentation experiments for Spicy Regs
- **Related goal:** `2026-07-24-production-document-segmentation-agent-goal.md`
- **Upstream reviewed:** Chonkie 1.7.0 at commit
  `0a6baea1a42c9afe9b3bc31ecb37739e744bb1ec`

## Decision

Spicy Regs does not need Chonkie.

The production ontology, source adapters, segment ledger, evidence model,
OpenAI path, local-model path, retrieval evaluation, and reranking pipeline
must work without Chonkie. Chonkie may serve as an isolated experiment arm. It
may enter production only if the real-corpus evaluation shows a material data-
quality improvement that justifies the extra dependency and operational
surface.

If Chonkie ties an existing approach, Spicy Regs will keep the simpler existing
approach. If one Chonkie algorithm wins, the implementation review will choose
between:

1. using a pinned Chonkie adapter;
2. implementing the underlying published method behind the Spicy Regs
   interface; or
3. adopting only the useful output as an offline retrieval representation.

No experiment result may make Chonkie's object model part of the ontology.

## Why evaluate it

Chonkie provides credible implementations of several methods that Spicy Regs
needs to compare:

| Chonkie component | Possible experimental use | Production presumption |
| --- | --- | --- |
| `RecursiveChunker` | Deterministic hierarchical baseline | Unnecessary unless it beats or simplifies the incumbent |
| `SemanticChunker` | Embedding-based boundary comparator | Evaluate with our pinned embeddings and tokenizer |
| `LateChunker` | Contextual retrieval-embedding experiment | Store as a retrieval representation, not source identity |
| `SlumberChunker` | Independent LLM-guided boundary comparator | Use only with our call ledger, cache, and safety controls |
| Token, sentence, and fast chunkers | Performance references | Our deterministic fallbacks remain sufficient by default |

These implementations can test whether the in-house methods leave quality on
the table. They are evidence sources, not architecture requirements.

## Ontological boundary

The general processing model remains:

```text
Artifact
  -> ArtifactVersion
    -> StructuralElement
      -> SegmentPolicy
        -> Segment
          -> RetrievalRepresentation
```

Chonkie can implement a `SegmentPolicy` or produce a
`RetrievalRepresentation`. It does not define:

- artifact identity;
- document versioning;
- source-native structure;
- RIN, docket, proceeding, or regulation identity;
- claims or concept assignments;
- evidence provenance;
- supersession;
- validation state; or
- publication semantics.

Regulatory documents may specialize the general artifact and element model.
They must not change the meaning of a segment to match a third-party library.

## Integration boundary

Any experiment must run behind a small Spicy Regs adapter:

```text
ArtifactVersion + ordered StructuralElements
  -> optional Chonkie policy
  -> untrusted SegmentCandidates
  -> Spicy Regs invariant checks
  -> canonical Segments
```

The adapter must:

1. pass raw, unnormalized element text;
2. use the pinned Spicy Regs `o200k_base` token counter;
3. translate element-relative character offsets into source-field offsets;
4. reject any candidate whose text differs from its declared source slice;
5. enforce complete, ordered source coverage without unexplained gaps;
6. enforce the configured 800-, 1,200-, or 1,800-token hard ceiling;
7. derive deterministic Spicy Regs segment IDs;
8. separate overlap or generated context from evidence text; and
9. record the library version, commit, policy, models, tokenizer, inputs,
   outputs, timings, failures, and digests in the experiment receipt.

Chonkie's generated chunk UUID must never become a canonical segment ID.
Canonical IDs derive from the artifact version, element identity, exact source
coordinates, text digest, and segment-policy version.

## Source-offset caution

Chonkie exposes `start_index` and `end_index` on its public `Chunk` type.
Spicy Regs must still verify every offset.

In the reviewed SemanticChunker source, intermediate `Sentence` records use
enumeration positions rather than cumulative character positions. The current
implementation later reconstructs final chunks from concatenated sentence
text, but this internal behavior does not meet our provenance standard by
itself.

The adapter must establish these properties independently:

```python
candidate.text == element.raw_text[candidate.start : candidate.end]
```

For a canonical non-overlapping segmentation:

```python
"".join(candidate.text for candidate in candidates) == element.raw_text
```

Tests must cover repeated passages, blank lines, tabs, CRLF, Unicode combining
characters, emoji, HTML and XML markup, tables, very long unbroken text,
prompt-injection text, and documents without a terminal newline.

## Fair experiment

Chonkie must run against the same immutable artifact versions, gold spans,
queries, and downstream OpenAI tasks as every other segmentation arm.

Control the variables:

- Use the same token budgets.
- Use the same embedding model when comparing boundary algorithms.
- Store vectors from different embedding models in separate spaces.
- Use the same dense-retrieval candidate depth.
- Apply the same reranker to each candidate set.
- Use the same OpenAI tagging prompt, model, registry, and validation policy.
- Cache by exact input and configuration digest.
- Report retries and failures rather than silently falling back.

Do not use Chonkie's default embedding model as the only semantic comparison.
At minimum, compare the semantic method with the incumbent pinned BGE model and
the selected production-quality embedding model. Local MLX and hosted OpenAI
embeddings must produce separate, labeled results.

## Metrics

The experiment must report both segmentation integrity and downstream utility.

### Integrity gates

Every accepted output must have:

- exact source-slice equality;
- complete and ordered canonical coverage;
- no cross-artifact or cross-element leakage;
- no segment above its hard token limit;
- stable IDs for identical inputs and policy;
- explicit behavior for empty and non-content elements; and
- a receipt that contains no secret material.

An integrity failure disqualifies that configuration, regardless of its
retrieval score.

### Quality measures

Measure:

- boundary precision, recall, and F1 against reviewed boundaries;
- gold-span containment and fragmentation;
- dense retrieval MRR, recall, and nDCG;
- candidate recall before reranking;
- MRR, recall, and nDCG after reranking;
- ontology-tag precision, recall, and F1;
- evidence-grounding and validation failure rates;
- zero-tag completion coverage;
- stability across repeated runs;
- latency, throughput, model calls, tokens, and cost; and
- results by document family and source profile.

The final choice should optimize downstream evidence and ontology quality, not
the visual plausibility of chunk boundaries.

## Adoption gate

Chonkie remains outside the production dependency set unless all of these
conditions hold:

1. The adapter passes every integrity gate on the complete evaluation corpus.
2. The relevant Chonkie arm improves the primary downstream metric over the
   incumbent on the corpus as a whole.
3. It does not hide a material regression for any document family.
4. The improvement repeats from cached identical inputs and from an independent
   rerun.
5. The OpenAI tagging and validation path passes with strict provider receipts.
6. The local MLX path and reranker produce complete, labeled receipts when they
   are part of the selected configuration.
7. Dependency, model-license, security, upgrade, and failure-mode reviews pass.
8. The benefit exceeds the maintenance cost of the adapter and dependency.

If these conditions do not hold, remove the experiment dependency and retain
the evidence report.

## Recommended experiment scope

Run Chonkie in an isolated optional environment rather than adding it
immediately to the production runtime.

The first useful comparison is:

1. incumbent structure-first segmentation;
2. Chonkie Recursive with the same tokenizer and budgets;
3. incumbent semantic segmentation;
4. Chonkie Semantic with the same embedding model;
5. Chonkie Late as a separate retrieval representation; and
6. Chonkie Slumber as an independent LLM-guided comparator.

Run the comparison across the complete immutable mixed-document corpus. Report
unsupported or failed artifacts explicitly. Do not reduce the corpus until all
methods appear green.

## Production fallback

Production must retain a dependency-free path:

1. honor source-native structural boundaries;
2. group elements within the token budget;
3. split oversized elements by paragraph, line, sentence, word, and hard token
   boundaries;
4. preserve exact source spans;
5. attach deterministic non-evidentiary context; and
6. record every segment and zero-tag result in the immutable ledger.

This path is the operational baseline even if a more sophisticated policy wins
for some document families.

## References

- [Chonkie repository](https://github.com/feyninc/chonkie)
- [Reviewed Chonkie commit](https://github.com/feyninc/chonkie/tree/0a6baea1a42c9afe9b3bc31ecb37739e744bb1ec)
- [Chunk type and generated UUID](https://github.com/feyninc/chonkie/blob/0a6baea1a42c9afe9b3bc31ecb37739e744bb1ec/src/chonkie/types/base.py)
- [Semantic Chunker documentation](https://docs.chonkie.ai/oss/chunkers/semantic-chunker)
- [Late Chunker documentation](https://docs.chonkie.ai/oss/chunkers/late-chunker)
- [Slumber Chunker documentation](https://docs.chonkie.ai/oss/chunkers/slumber-chunker)
- [MIT license](https://github.com/feyninc/chonkie/blob/0a6baea1a42c9afe9b3bc31ecb37739e744bb1ec/LICENSE)
