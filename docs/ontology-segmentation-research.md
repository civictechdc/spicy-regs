# Document segmentation for ontology tagging and AI retrieval

Research date: July 24, 2026

## Decision

Spicy Regs should use a **source-aware, structure-first, token-bounded
segmenter**. It should not use one regex splitter, one fixed window, or an LLM
to choose every boundary.

The production path should:

1. parse each source into native structural elements;
2. keep short records as one segment;
3. split long elements at paragraph, line, sentence, word, and finally hard
   token boundaries;
4. attach the document title and heading path as separate context;
5. retain exact source offsets, digests, hierarchy, and adjacent-segment links;
6. tag leaf segments with the LLM, then aggregate supported assignments back to
   the source artifact; and
7. evaluate semantic and LLM-guided chunkers as optional variants, not as the
   default.

Regex remains useful for formal syntax such as RINs, CFR citations, U.S.C.
citations, Public Laws, and fallback sentence boundaries. Regex must not decide
document identity, topic, semantic relatedness, or whether two proceedings are
the same.

## Empirical result

The bounded real-data comparison selects `structure-overlap-1800` as the
canonical segmentation policy. It preserves all 35 curated gold spans and is
the strongest lossless direct arm under the incumbent BGE ordering. The
fully paired retrieval cascade is incumbent BGE dense top-50 followed by
`BAAI/bge-reranker-v2-m3`; the reranker raises corpus nDCG@10 from 0.3395 to
0.5198 without changing the fixed Recall@50 ceiling.

The real OpenAI ontology path independently validates across all ten accepted
document profiles. Its 35-target result is 0.8857 micro F1, every accepted
evidence span resolves exactly, and all 384 provider calls complete without
retry or failure. Court-opinion packages remain the main quality weakness, and
most generated assignments fall outside the small adjudicated target set, so
the result supports a comparison-ready architecture rather than automatic
ontology publication.

The implementation follows the package-first boundary described below:
`tiktoken`, Sentence Transformers, SPLADE, and the BGE cross-encoder remain
behind project-owned protocols, while source identity, exact offsets,
aggregation, receipts, and fail-closed rules stay under project control. The
full measurements and limitations are in the
[fair-comparison report](evidence/document-segmentation-fair-comparison-2026-07-24.md).

## Why this is the common production shape

Current document-AI systems generally separate **parsing** from **chunking**.
They first recover titles, narrative blocks, lists, tables, captions, pages, and
hierarchy. They then combine whole elements until a size budget requires a
split.

- Unstructured chunks the elements produced by its format-aware partitioners.
  Its `by_title` policy closes a chunk at a new section, keeps tables separate,
  and preserves the original elements in chunk metadata. It warns that blanket
  overlap can pollute otherwise clean semantic units.
  [Unstructured chunking documentation](https://docs.unstructured.io/open-source/core-functionality/chunking)
- Docling's `HierarchicalChunker` uses document hierarchy and carries headings
  and captions. Its `HybridChunker` then splits only oversized chunks and
  merges undersized peers with the same headings. It can repeat table headers
  when a table must span chunks.
  [Docling chunking documentation](https://docling-project.github.io/docling/concepts/chunking/)
- Haystack exposes word, sentence, paragraph, page, line, and custom splitters,
  while retaining a source identifier and page number. This is a useful minimum
  provenance contract even when a richer parser is unavailable.
  [Haystack `DocumentSplitter`](https://docs.haystack.deepset.ai/docs/documentsplitter)
- LlamaIndex's semantic splitter embeds neighboring sentence groups and inserts
  a boundary when their cosine dissimilarity crosses a configured percentile.
  This illustrates an important tradeoff: semantic boundaries depend on an
  embedding model, library version, and threshold.
  [LlamaIndex `SemanticSplitterNodeParser`](https://developers.llamaindex.ai/python/framework-api-reference/node_parsers/semantic_splitter/)

OpenAI File Search provides a useful generic baseline, not a complete
source-aware policy. It currently defaults to 800-token chunks with 400-token
overlap and permits static chunks from 100 through 4,096 tokens. It also
supports file attributes and hybrid semantic/text ranking. Spicy Regs needs to
control segmentation itself because ontology evidence requires source fields,
offsets, hierarchy, and deterministic reprocessing that a generic file-level
index does not model.
[OpenAI Retrieval guide](https://developers.openai.com/api/docs/guides/retrieval)

## What the evidence says about "smarter" chunking

No chunking method wins every task.

A February 2026 reproduction study compared paragraph, fixed-size, sentence,
semantic, proposition, and LLM-guided methods. For **retrieval across a
corpus**, the simple structure-based methods consistently beat semantic and
LLM-guided methods; paragraph splitting processed about 1,854 documents per
second in one reported comparison versus 1.11 for the LLM-guided method. For
**retrieval within one long narrative**, the ordering reversed and the
LLM-guided LumberChunker performed best. Contextualized embeddings helped many
corpus-retrieval settings but hurt within-document retrieval in that study.
The paper is a recent preprint, so its exact numbers should inform an Spicy Regs
evaluation rather than become a universal rule.
[Beyond Chunk-Then-Embed](https://arxiv.org/abs/2602.16974)

Legal retrieval also rewards precise spans. LegalBench-RAG uses 6,858
expert-annotated query and answer pairs and treats minimal, directly relevant
snippets as the target. The benchmark's authors argue that broad chunks add
latency and distracting text and make source verification harder.
[LegalBench-RAG](https://arxiv.org/abs/2408.10343)

Large model context windows do not remove the need to segment. The
Lost-in-the-Middle experiments found that models often use evidence at the
beginning or end of a long prompt better than evidence in the middle, and that
adding more retrieved documents can saturate downstream performance before
retriever recall saturates.
[Lost in the Middle](https://arxiv.org/abs/2307.03172)

Several techniques try to restore context lost at boundaries:

- **Context prefixes:** prepend a short explanation of the document and the
  chunk before indexing. Anthropic reported lower retrieval failure rates from
  contextual embeddings plus BM25 and reranking, but the contextual text is
  model-generated and must never be confused with source evidence.
  [Contextual Retrieval](https://www.anthropic.com/engineering/contextual-retrieval)
- **Late chunking:** encode a longer document first, then pool token embeddings
  for individual chunks so their vectors retain surrounding context.
  [Late Chunking](https://arxiv.org/abs/2409.04701)
- **Hierarchical retrieval:** index leaf passages and recursively generated
  summaries at several levels. RAPTOR improved multi-step and thematic
  question answering in its experiments, but its authors also found minor
  hallucinations in about 4% of an audited summary sample. Generated summaries
  are therefore retrieval aids, not authoritative evidence.
  [RAPTOR](https://arxiv.org/abs/2401.18059)

These approaches address representation and retrieval after segmentation. They
do not justify discarding source structure or provenance.

The 2024 SPLADE reranking comparison provides a separate retrieval lesson. Its
cross-encoders remain highly competitive with listwise GPT reranking, larger
candidate depths sometimes help, and title metadata materially affects
cross-encoder quality. It does not compare chunkers or embedding providers.
For Spicy Regs, it supports sparse or hybrid first-stage contenders, rerank
depth sweeps, retrieval-only title/heading context, and keeping listwise LLM
reranking outside the production hot path until it wins a paired evaluation.
The detailed review and limitations are in
[the arXiv 2403.10407 evidence note](evidence/arxiv-2403.10407-reranking-review.md).

## The right abstraction for Spicy Regs

The ontology should distinguish four things:

| Layer | Meaning | Stable identity? | May support evidence? |
| --- | --- | --- | --- |
| Artifact | A source record or document, such as one comment, bill version, or report | Yes | Yes |
| Element | A source-native section, paragraph, list, table, or field | Yes within an artifact version | Yes |
| Segment | A bounded processing view over one or more contiguous elements | Reproducible, but subordinate to the artifact | Yes, through its original spans |
| Context | Heading paths, titles, summaries, or neighboring text added for a model call | No | No, unless separately present in the source |

A segment is not a new document, proceeding, regulation, or ontology concept.
It is a processing unit. Several segments can support one document-level
concept assignment, and one segment can support several assignments.

The segment contract should record:

- artifact type and ID;
- subject-profile version;
- source table, field, and artifact-version digest;
- segment ID, ordinal, and segmenter-policy version;
- exact start and end offsets for every source span;
- page or source-element coordinates when available;
- heading and ancestor path;
- previous, next, and parent segment IDs;
- tokenizer name and token count;
- boundary type and whether a hard split was required; and
- source-text digest distinct from any added context.

Assignments should quote only original source spans. A deterministic heading
prefix or an LLM-generated contextual prefix may improve the model's
understanding, but it cannot become quoted evidence.

## Source-profile policy

The production registry currently has seventeen taggable profiles. They need
three segmentation modes, not seventeen independent algorithms. The reusable
ontology may process every profile, but the current production acceptance run
evaluates document roles only. Public comments are excluded; docket,
proceeding, and entity rows may supply relationship context but cannot enter
document queries, retrieval candidates, gold, or policy-selection metrics.

| Profile | Default processing unit | Long-content policy |
| --- | --- | --- |
| Regulations.gov docket | One metadata record | Keep as one segment; attachments remain separate artifacts |
| Regulations.gov document | Title/metadata anchor plus body | Split body by recovered headings and paragraphs; retain docket and document IDs |
| Regulations.gov comment | One comment | Generic adapter remains supported; excluded from the document acceptance run |
| Federal Register document | One article with title/abstract anchor | Prefer article XML/HTML sections; keep agency, action, summary, dates, addresses, supplementary information, and amendatory text distinguishable |
| Unified Agenda observation | One RIN-plus-edition observation | Keep structured fields whole; do not merge editions merely because the RIN matches |
| CFR section | One section as parent | Use title/chapter/part/subpart/section hierarchy; leaf at paragraph or subparagraph; isolate tables with repeated headers |
| Congressional bill | One bill version as artifact | Use XML section/subsection/paragraph hierarchy; never mix versions of a bill |
| SAM entity | One entity row | No text chunking; encode fields explicitly |
| Lobbying filing | One filing as parent | Treat each lobbying activity as a child element; keep government entities structured |
| FEC committee | One committee row | No text chunking; encode fields explicitly |
| GAO report | Title/abstract anchor plus report body | Heading-aware report sections; tables and figures retain captions and page coordinates |
| CRS report | Title/summary anchor plus report body | Heading-aware report sections; retain report version and page coordinates |
| Court docket | One docket metadata record | Relationship context only; never substitute it for a separately identified opinion |
| Court opinion | One official Supreme Court opinion package | Preserve the official PDF bytes and extracted text as one artifact; use heading-aware narrative segments, but do not claim reliable lead, concurrence, or dissent separation without source-backed structure |
| USAspending recipient | One recipient row | No text chunking; encode fields explicitly |
| FCC proceeding | One proceeding metadata record | Keep as one segment unless a long description exceeds the budget |
| FCC filing | One filing as parent | Keep short express comments whole; split long text and attachments by headings and paragraphs |

Native government structure should outrank inferred PDF layout when both exist.
The eCFR XML already nests title, chapter, subchapter, part, subpart, subject
group, section, and appendix divisions. GovInfo exposes CFR sections and
congressional bill text in XML, while Federal Register issues are available in
XML and individual documents in HTML.
[eCFR XML guide](https://github.com/usgpo/bulk-data/blob/main/ECFR-XML-User-Guide.md),
[CFR on GovInfo](https://www.govinfo.gov/help/cfr),
[Congressional bills on GovInfo](https://www.govinfo.gov/help/bills),
[Federal Register on GovInfo](https://www.govinfo.gov/help/fr)

The current evaluation ingests official Supreme Court PDF opinion packages and
preserves each package as one artifact. This supplies real long-form judicial
text without guessing where a lead opinion, concurrence, or dissent begins.
For broader future case-law ingestion, CourtListener recommends
`html_with_citations` as its most reliable opinion-text field. That structured
HTML should precede PDF-to-text fallback. When the source identifies separate
lead, concurrence, and dissent opinions, each should remain a separate artifact.
[CourtListener case-law API](https://www.courtlistener.com/help/api/rest/v4/case-law/)

Docling is a reasonable PDF, DOCX, and scanned-document fallback because it
provides structural elements, hierarchy, captions, tables, and token-aware
chunking. It should be an adapter behind Spicy Regs' own element and segment
schema, not the ontology's data model.

## Initial budgets to evaluate

Chunk size is a model-and-task parameter, not an ontological fact. Use these as
an experiment grid:

| Task | Candidate leaf budgets | Overlap |
| --- | --- | --- |
| Ontology tagging/extraction | 800, 1,200, and 1,800 input tokens | None by default; overlap only a structurally oversized element |
| Corpus retrieval | 400, 800, and 1,200 tokens | Compare 0%, 10%, and OpenAI's 50% default baseline |
| Long-document question answering | Retrieve small leaves, then expand their parent section or adjacent leaves | Expansion, not duplicated indexing text |

Reserve a separate prompt budget for instructions, the concept registry, title,
heading path, and model output. Enforce the hard limit with the tokenizer used
for the actual model. Character counts can remain a fast preflight estimate,
but they must not be the final production limit.

Blanket overlap should not be the default for ontology extraction. It increases
cost, produces duplicate evidence, and can create duplicate assignments.
Adjacent links and parent expansion preserve context without pretending copied
text has two source locations.

## OpenAI pipeline implications

The existing Responses API tagging loop should operate on explicit Spicy Regs
segments:

1. deterministically build and persist the segment ledger;
2. mark every segment processed, including segments that yield zero tags;
3. send one bounded segment plus non-evidentiary artifact context to the model;
4. require structured output with exact quoted spans;
5. translate quoted spans back to artifact-level offsets;
6. reject evidence found only in an added context prefix;
7. deduplicate equivalent assignments across neighboring segments;
8. aggregate leaf assignments to the artifact without erasing their evidence;
9. validate a stratified sample across every source profile; and
10. record request ID, model, tokenizer, prompt-policy version, token use,
    latency, status, and retry behavior.

OpenAI File Search can serve as a comparison retrieval implementation. It
should not replace the segment ledger for ontology materialization.

## Evaluation gate

Choose a policy from evidence on the mixed real-data corpus, not intuition.
Create gold spans and expected document-level tags across every in-scope
document profile, with extra long examples for regulations, notices, CFR text,
bills, reports, separately identified court opinions, agendas, lobbying
filings, and FCC filings. Keep the all-profile corpus as a reusable superset,
but bind acceptance results to an immutable document-scope membership artifact.

Compare at least:

1. structure-first, token-bounded segmentation;
2. structure-first with limited overlap on oversized elements;
3. paragraph/sentence-only fallback;
4. semantic splitting; and
5. LLM-guided boundaries for long narrative documents only.

Measure:

- byte and character coverage, with no silent loss;
- deterministic IDs and rerun equality;
- hard token-limit compliance;
- gold-span containment and boundary-crossing misses;
- retrieval recall, precision, MRR, and nDCG at several values of `k`;
- document-level micro and macro F1 for ontology tags;
- evidence-grounding precision and offset validity;
- duplicate-assignment and cross-segment disagreement rates;
- profile-level zero-tag and failure rates;
- prompt-injection resistance;
- model tokens, dollars, latency, and retry rate; and
- aggregation accuracy for facts that require more than one section.

Do not declare one universal winner unless it passes both corpus-wide ontology
tagging and within-document retrieval gates. The evidence strongly suggests
that those tasks may need different views over the same canonical segments.

## Implementation consequence

The implemented pipeline now replaces silent field truncation with:

- an artifact-and-element model;
- token-aware budgets;
- segment metadata and a zero-result processing ledger;
- heading and parent context kept separate from evidence;
- source-specific element adapters; and
- document-level assignment aggregation.

The comparative harness keeps five boundary policies behind one evaluation
interface and uses the same locked source artifacts, gold spans, candidate
registry, tagging prompt, validation policy, and aggregation rules. Semantic
and LLM-guided boundaries remain experiment arms rather than ontology types.
The expensive model should spend most of its effort interpreting content, not
rediscovering boundaries already encoded by government XML, HTML, JSON fields,
headings, and paragraphs.

Retrieval evaluation also preserves two whole-artifact baselines:

1. the exact legacy BGE input composition for docket, document, and comment
   rows; and
2. one general all-profile whole-artifact representation.

These are separate derived objects from source elements and processing
segments. Their receipts record source-version and input-text digests, model
revision, dimensions, normalization, model-native token counts, input limits,
and truncation status. A whole-artifact score therefore cannot be mistaken for
a segment score or joined to a different model space.

The Sentence Transformers rerank view likewise records the exact untruncated
pair-token count, explicit cross-encoder limit, and truncation flag on each
candidate. The current milestone intentionally excludes oMLX; no oMLX quality,
latency, or per-candidate audit result is inferred.

OpenAI embeddings are cached as a completed stage before boundary selection.
Every OpenAI boundary batch is content-addressed and checkpointed, including
retry-exhausted transitions. A resumed run skips completed batches but retains
their safe call telemetry. This is necessary because a successful embedding
stage followed by an incomplete structured response is otherwise both costly
and unauditable.

The execution-ready implementation handoff is
[Production document segmentation agent goal](superpowers/specs/2026-07-24-production-document-segmentation-agent-goal.md).
