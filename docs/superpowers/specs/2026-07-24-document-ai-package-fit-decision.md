# Decision: Use Packages for Document-AI Mechanics

- **Date:** 2026-07-24
- **Status:** Accepted for implementation
- **Scope:** Parsing, segmentation experiments, embeddings, retrieval,
  reranking, and evaluation
- **Related goal:** `2026-07-24-production-document-segmentation-agent-goal.md`

## Decision

Spicy Regs will use maintained packages for general document-AI mechanics and
write only the adapters and invariants that express its ontology contract.

Before adding substantial custom code, implementation must search for a package
that covers roughly 80 percent of the requirement. A package wins when it meets
the quality, maintenance, licensing, performance, security, and failure-mode
requirements. A thin adapter must not conceal a package's identity, version,
model, configuration, or errors.

Spicy Regs still owns:

- artifact, version, element, segment, and assignment identity;
- source-native adapter selection;
- exact evidence coordinates and grounding;
- deterministic policy and input digests;
- immutable segment and provider-call ledgers;
- zero-result and failure accounting;
- checkpoint compatibility and safe resume;
- ontology aggregation and supersession;
- secret scanning; and
- generation receipts.

Generic packages do not provide those domain contracts.

## Selected package boundary

| Capability | Selected package or service | Decision |
| --- | --- | --- |
| Native structured sources | Source XML, HTML, JSON, and API clients | Keep source-native adapters; native structure outranks inferred layout |
| Unstructured documents | Docling 2.115.0 | Use as the PDF, Office, image, and otherwise-unstructured fallback; map its output into Spicy Regs elements |
| Deterministic fallback splitting | Existing small Spicy Regs segmenter plus tiktoken | Keep because exact spans, deterministic IDs, and ledger semantics are project-specific |
| Alternative chunking methods | Chonkie 1.7.0 | Optional isolated benchmark only; no production dependency without a measured win |
| Token accounting | tiktoken with pinned `o200k_base` policy | Use as the hard OpenAI prompt and segment budget authority |
| Local embeddings | Sentence Transformers 5.6.1 | Use `SentenceTransformer`; do not implement transformer inference |
| Hosted embeddings | Official OpenAI Python SDK | Use `text-embedding-3-large` with exact model and dimension receipts |
| Apple-Silicon embeddings | oMLX 0.5.3 | Use its `/v1/embeddings` service; do not build MLX model-loading code |
| Local reranking | Sentence Transformers 5.6.1 | Use `CrossEncoder.rank` on dense top-k candidates |
| Apple-Silicon reranking | oMLX 0.5.3 | Use its `/v1/rerank` service with pinned MLX model weights |
| Retrieval measures | ir-measures 0.4.3 | Use its standard precision, recall, reciprocal-rank, and nDCG implementations |
| Ontology classification measures | scikit-learn 1.7.2 | Use its multilabel precision, recall, and F1 implementations |
| Structured OpenAI output | Official OpenAI Python SDK | Keep the existing typed structured-output provider and strict receipts |
| Parquet artifacts | PyArrow and Polars | Keep deterministic project schemas and manifests |

Package versions and model revisions must be pinned in a lock file, manifest,
or external-service receipt. The table records the evaluated versions; a
future upgrade requires the same compatibility and deterministic-rerun gates.

## Why these packages

### Docling

Docling converts PDF, DOCX, PPTX, images, HTML, XML variants, spreadsheets,
email, EPUB, and other formats into one document representation. Its
hierarchical and hybrid chunkers preserve headings and captions, split
oversized chunks by token budget, merge compatible peers, and support repeated
table headers.

Spicy Regs will not use Docling when a government source publishes better
native XML, HTML, or structured JSON. It will use Docling instead of building
new PDF-layout, OCR, table, or Office parsers.

Docling coordinates remain parser evidence until the adapter proves their
relationship to locked source bytes. The Docling object model does not become
the ontology.

#### Implementation clarification (2026-07-26): Office first, PDF and images deferred

This clarifies staging, not the package choice. Docling remains the selected
fallback for unstructured documents; what the implemented adapter serves today
is narrower than the table row above.

`src/spicy_regs/docpipeline/adapters/docling.py` serves **DOCX, PPTX, and XLSX
only**, through Docling's model-free `SimplePipeline`, so a receipt can name
every input that affected the output. PDF and image inputs are *recognized and
refused by name* with a `format_not_implemented` record, before the provider is
invoked: they run the paginated pipeline, whose layout, table, and OCR models
this adapter cannot yet identify. Serving them needs four things first — a
content-addressed model manifest, one explicitly chosen OCR engine, the process
containment `source.py` now owns, and real model-backed tests.

Nothing that already works loses ground in the meantime. The existing
pypdf-derived text columns (`pdf_text`, and the other body columns) stay
**native**: they are exact fields of the immutable source record, so `source.py`
reads them through its native-prose branch and never hands them to a parser. The
Office fallback is reachable only for a record that publishes no usable native
text at all.

Containment moved where it can be enforced. An in-process adapter cannot bound
a library call it is inside of, so `adapters/docling.py` records wall-clock,
CPU, memory, and archive-expansion limits as *unenforced* and `source.py` runs
the adapter as a child process — its own session, a credential-stripped
environment, a wall timeout, SIGTERM-then-SIGKILL over the whole process group,
and a result byte cap checked before that file is read. Stderr is never read;
its byte count and over-threshold flag are observed facts and do not turn a
successful parse into a failure. CPU,
resident memory, archive expansion, temporary disk, descendant count, network,
and filesystem scope stay explicitly unenforced there too, and are named as
such in every gate receipt.

### Sentence Transformers

Sentence Transformers already implements:

- pinned bi-encoder model loading;
- batched document and query encoding;
- normalized embeddings;
- device selection;
- CrossEncoder inference;
- `CrossEncoder.rank`; and
- retrieve-then-rerank workflows.

Spicy Regs needs only a provider adapter that supplies model provenance,
validates dimensions and finite values, and emits safe call telemetry.

Embedding and reranking limits are model-specific. A segment that fits the
OpenAI `o200k_base` tagging budget may exceed a BGE tokenizer's limit, as the
unbroken-text adversarial case demonstrates. Embedding and whole-artifact
representations therefore record their tokenizer, untruncated model-token
count, input limit, and truncation status. Cross-encoder request identity
includes its explicit maximum sequence length; a separate tokenizer audit must
identify candidates that reached that limit. The Sentence Transformers adapter
uses the packaged Hugging Face pair tokenizer without truncation and writes
those values on every reranked candidate.

The incumbent comparison remains
`BAAI/bge-base-en-v1.5@a5beb1e3e68b9ab74eb54cfd186867f64f240e1a`.
The initial cross-encoder baseline is
`BAAI/bge-reranker-v2-m3@953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e`.
The experiment may add stronger models, but it must report each space
separately.

### oMLX

oMLX supplies a maintained Apple-Silicon server for language models,
embeddings, and rerankers. Spicy Regs will call its HTTP endpoints and record:

- oMLX version and server identity;
- model repository and immutable revision;
- quantization and dimensions;
- request digest and candidate count;
- token usage when reported;
- duration, retries, and terminal status; and
- response-model metadata.

The initial quality-oriented MLX candidates are:

- embedding:
  `mlx-community/bge-m3-mlx-8bit@7eca4a1c6ea1a0c5efc37598b369012f3985910f`;
- reranking:
  `mlx-community/Qwen3-Reranker-4B-mxfp8@25f203a237b822a90f38763843562b93a5baf82f`.

The experiment may also run a smaller reranker for throughput comparison.
Model licenses require separate verification even when the server is
Apache-2.0.

Spicy Regs will not import oMLX as a library or recreate its schedulers, model
loaders, quantization support, memory guard, or inference kernels.

The local service binds to `127.0.0.1:8012`. Port 8000 remains available for
the repository's documented MkDocs and MCP development servers and is also
used by an existing SSH listener on the evaluation host. Both oMLX adapters
fail closed on non-loopback URLs unless remote access is explicitly enabled.

oMLX 0.5.3's reranker engine accepts a `max_length`, but its public
`/v1/rerank` request model does not expose that parameter. The selected Qwen
causal reranker therefore uses oMLX's packaged 8,192-token default. The
experiment records that service default and aggregate processed tokens. It
must not claim a per-document untruncated-token or truncation audit from the
HTTP response, because the endpoint does not provide one. Until that gap is
closed upstream or by a package-backed audit, a candidate that may reach the
limit is a declared MLX-provider limitation rather than silently accepted
evidence.

### ir-measures

ir-measures accepts standard qrels and scored runs and supplies a common
interface to established IR measure implementations. It covers the current
handwritten ranking metrics and the planned candidate-recall measurement.

The experiment will use ir-measures for:

- `P@1`, `P@3`, `P@5`, and `P@10`;
- `R@1`, `R@3`, `R@5`, `R@10`, and candidate `R@50`;
- reciprocal rank; and
- `nDCG@5` and `nDCG@10`.

Spicy Regs will retain a few small fixture calculations as independent
cross-checks. Production reports will use the package results.

## What remains custom

### Exact retrieval candidates

The corpus is small enough for exact dense ranking. A NumPy matrix product is
the appropriate glue for tens of thousands of segments and dozens of gold
queries. An approximate vector database would add nondeterminism and index
state without improving this evaluation.

Production serving may use LanceDB or another index, but the acceptance
experiment must preserve an exact-ranking reference result.

### Immutable caches and receipts

Embedding, boundary, tagging, validation, and reranker caches are audit
artifacts. Their keys include exact input text, artifact version, policy,
provider, model revision, and parameters. They must record successful
zero-results and failures.

Generic function caches do not meet this contract. PyArrow and Polars will
write the project-owned schemas; no custom storage engine is needed.

### Source and ontology contracts

No package decides whether two records are the same artifact, proceeding,
agenda item, regulation, organization, or concept. Regex, embeddings,
rerankers, and LLMs may propose relationships. Source identifiers and
provenance-bearing assertions determine what the pipeline publishes.

## Packages considered but not adopted as core

### Chonkie

Chonkie offers useful recursive, semantic, late, and LLM-guided chunkers. It
also generates its own chunk identifiers and does not supply the complete
source-version, exact-evidence, ledger, and ontology contracts.

Use it only under the separate Chonkie evaluation decision. If it ties the
incumbent, retain the simpler dependency-free path.

### Full RAG frameworks

LangChain, LlamaIndex, Haystack, and similar frameworks cover orchestration
well, but Spicy Regs already has source pipelines, Parquet artifacts,
checkpoints, receipts, and ontology transforms. Adding a second orchestration
model would increase the production surface without replacing the
project-specific work.

Individual algorithms or integrations may still serve as experiment
comparators.

### LLM-judge evaluation frameworks

RAGAS and similar tools can supplement qualitative review. They cannot replace
the locked gold spans, source relationship expectations, exact retrieval
metrics, evidence grounding, or independent validation required here.

## Integration rules

Every package adapter must:

1. accept project-owned immutable inputs;
2. return data that can be validated before publication;
3. preserve package, model, and configuration provenance;
4. reject missing, malformed, dimensionally inconsistent, or non-finite
   outputs;
5. expose retries and terminal failures;
6. avoid logging credentials or source text unnecessarily;
7. use content-addressed cache keys;
8. support safe restart without repeating completed calls; and
9. fail closed when the configured package or service is unavailable.

No adapter may silently switch a model, tokenizer, parser, chunker, device, or
provider.

OpenAI 429 responses require code-aware handling. Ordinary request-rate 429s
remain retryable with pacing and backoff, while `insufficient_quota` is a
project or organization spend/credit hard limit and must fail after one
recorded attempt. Retrying an unchanged request cannot clear that condition.
[OpenAI API errors](https://developers.openai.com/api/docs/guides/error-codes#api-errors),
[OpenAI spend limits](https://developers.openai.com/api/docs/guides/spend-limits#understand-hard-limit-behavior)

The SPLADE reranking review adds two package-first experiment options without
putting them on the production path:

- use Sentence Transformers `SparseEncoder`, or FlagEmbedding only when its
  BGE-specific combined dense/sparse/multi-vector path is necessary, for the
  learned-sparse and hybrid first-stage arms; and
- use RankLLM for any RankGPT/listwise experiment so Spicy Regs does not own
  sliding windows, permutation parsing, and output repair.

## Immediate implementation consequence

The next experiment work will:

1. pin Sentence Transformers and ir-measures as experiment dependencies;
2. replace handwritten aggregate IR calculations with ir-measures;
3. use `CrossEncoder.rank` for the non-MLX reranker baseline;
4. use oMLX HTTP endpoints for MLX embeddings and reranking;
5. keep exact NumPy dense ranking as the reference retrieval stage;
6. retain the legacy three-table whole-row BGE result and a general
   all-profile whole-artifact result as separate retrieval objects;
7. write resumable embedding, boundary-selection, candidate, and reranker
   artifacts around those package calls; and
8. compare artifact, pre-rerank segment, and post-rerank segment results on the
   same immutable corpus.

## References

- [Docling supported formats](https://docling-project.github.io/docling/usage/supported_formats/)
- [Docling chunking](https://docling-project.github.io/docling/concepts/chunking/)
- [Sentence Transformers CrossEncoder usage](https://www.sbert.net/docs/cross_encoder/usage/usage.html)
- [Sentence Transformers retrieve and rerank](https://www.sbert.net/examples/sentence_transformer/applications/retrieve_rerank/README.html)
- [oMLX 0.5.3](https://github.com/jundot/omlx/releases/tag/v0.5.3)
- [ir-measures](https://github.com/terrierteam/ir_measures)
- [ir-measures Python API](https://ir-measur.es/en/latest/getting-started.html)
- [FlagEmbedding](https://github.com/FlagOpen/FlagEmbedding)
- [RankLLM](https://github.com/castorini/rank_llm)
- [arXiv 2403.10407 reranking review](../../evidence/arxiv-2403.10407-reranking-review.md)
- [Chonkie evaluation decision](2026-07-24-chonkie-evaluation-decision.md)
