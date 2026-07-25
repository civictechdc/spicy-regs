# Agent Goal: Production Document Segmentation for the Spicy Regs Ontology Pipeline

- **Date:** 2026-07-24
- **Status:** Comparison-ready milestone complete; broader production hardening is deferred
- **Repository:** `civictechdc/spicy-regs`
- **Research basis:** `docs/ontology-segmentation-research.md`

## Goal

Replace silent text truncation with a production-grade, source-aware document
segmentation system for every Spicy Regs ontology subject profile.

The finished pipeline must preserve the identity of the source artifact,
represent its structural elements and bounded processing segments separately,
send each eligible segment through the real OpenAI tagging and validation path,
and aggregate supported concepts back to the source artifact without losing
evidence.

Complete enough implementation, documentation, real-data evaluation, provider
evidence, and repository verification to make a fair, reproducible decision
among the serious pipeline contenders. Do not stop after adding a generic text
splitter, passing synthetic unit tests, or proving that one small OpenAI batch
completes.

## Current comparison-ready stopping rule

On 2026-07-24, the user explicitly chose a fair decision point over exhaustive
perfection. The current agent run stops when the following bounded comparison
is complete:

1. Use the same immutable v2 document-only scope, 35 gold queries, three
   segment budgets, candidate limit, and metric implementation for every
   compared first-stage configuration.
2. Compare deterministic control, incumbent BGE dense retrieval, OpenAI dense
   retrieval, learned sparse retrieval, sparse+dense hybrid retrieval, and the
   incumbent whole-artifact BGE baseline.
3. Compare the four non-LLM segmentation arms directly. Report the LLM-guided
   arm separately as a whole-pipeline result because it changes both the
   embedding provider and boundary selector.
4. Attach model-native tokenizer, input-limit, and truncation evidence keyed to
   each dense artifact. This evidence may be a separate immutable audit; valid
   dense vectors do not need to be regenerated solely to move audit columns
   into their original Parquet file.
5. Rerank candidate groups to one fixed depth of 50 with the pinned BGE
   cross-encoder. Do not run oMLX or require the earlier 25/50/100/200 sweep for
   this milestone.
6. Run one real, scoped OpenAI tagging-and-validation pass on the selected
   configuration with strict receipts, complete profile coverage, grounding,
   and secret-safety checks.
7. Publish one comparison table covering quality, coverage, truncation,
   latency, provider calls, and failures, plus an explicit limitations and
   deferred-work list.
8. Pass the focused tests and repository gates affected by the selected path.

This milestone is sufficient to choose a pipeline and end the current
experiment. It is not permission to label deferred ablations, untested
adapters, or a deployment as production-complete.

## Live progress ledger

This section records the current local candidate state. It is not a production,
publication, release, or deployment claim.

### Complete and evidenced locally

- The general artifact, element, segment, context, and evidence contracts are
  implemented and covered by focused tests.
- Source-aware adapters and a lossless segment ledger are connected to ontology
  generation, checkpoints, assignments, validation, and receipts.
- The production profile registry contains seventeen profiles, including
  `court-opinion-v1`.
- The immutable v2 evaluation contains 241 artifacts across seventeen profiles,
  including ten official Supreme Court opinion packages and seven
  document-role adversarial cases.
- The immutable document-only acceptance view contains 153 documents, includes
  all 35 gold spans, and excludes public comments and relationship-only context
  from queries, candidates, tagging, and policy-selection metrics.
- The v2 evaluation rerun reproduced all 29 files byte-for-byte.
- Provider error handling distinguishes retryable rate limits from
  non-retryable `insufficient_quota` responses without persisting secrets.
- Experiment, retrieval, sparse, reranking, and ontology-tagging orchestration
  depend on project-owned provider protocols and explicit capabilities; current
  OpenAI, Sentence Transformers, and deterministic implementations remain
  injected boundary adapters with concrete provenance in receipts. The oMLX
  adapter is intentionally unexercised and outside this milestone.
- Document-scoped deterministic, incumbent BGE, and OpenAI dense experiments
  independently validate on the same 153-document scope and 35 gold queries.
  Separate exact input audits validate all 9,031 BGE and OpenAI inputs; BGE
  truncates 45 inputs at 512 tokens, while OpenAI truncates none at its
  `cl100k_base`-measured limit.
- The document-scoped whole-artifact BGE baseline independently validates.
- The selected learned-SPLADE and dense+sparse RRF comparison independently
  validates: 1,317 model inputs, 16,134 candidates, 43 resumable provider
  calls, zero failed transitions, and zero truncated inputs.
- Reranker format v2 can now bind both one fixed depth and an explicit upstream
  configuration; the selected top-50 preflight contains 2,519 candidates in 70
  paired query/scope groups.
- The selected BGE top-50 cross-encoder run independently validates. All 70
  groups and 2,519 candidates are audited, with zero failed transitions,
  retries, unaudited candidates, or truncation.
- Tagging format v4 binds one explicit upstream configuration, the complete
  secret-free model run configuration, both output-token caps, and the exact
  evidence-alignment policy while retaining the five-arm default. Supplied
  offsets must resolve exactly; a wrong offset may be repaired only for one
  unique verbatim field match, while ambiguous and non-verbatim evidence still
  fails closed. The deterministic sample guarantees at least one artifact from
  every accepted profile in addition to all gold and adversarial artifacts.
- The selected real OpenAI v4 run independently validates as
  `segmentation_tagging_4d8ad629f6efd805dd4a3341`: 44 artifacts, 109 segments,
  all ten accepted document profiles, 275 exact grounded spans, 216 aggregated
  assignments, 384 Priority-tier calls, and no retries, provider failures,
  invalid call receipts, or secret matches.
- The gold-target result is 31 true positives, four false positives, and four
  false negatives (0.8857 micro F1). Court-opinion packages remain the clear
  profile weakness at 0.6667 F1 and 0.5000 exact artifact-label match.

### Implemented with bounded limitations

- Experiment format v3 retrieves up to 200 candidates and measures
  Recall@10/25/50/100/200.
- Scope binding reaches segmentation experiments, artifact retrieval, reranking,
  and tagging.
- The official Supreme Court adapter preserves each downloaded PDF as one
  opinion package. It does not yet claim reliable lead, concurrence, and dissent
  separation within a package.

### Milestone result

All eight comparison-ready conditions pass locally. The measured decision,
quality, latency, cost, truncation, provider, adversarial, and limitation
evidence is published in the
[fair-comparison report](../../evidence/document-segmentation-fair-comparison-2026-07-24.md).
The current experiment stops here. Broader production hardening and oMLX remain
intentionally deferred.

## Required result

Implement this processing model:

```text
Source artifact
  ├── source-native elements
  │     ├── heading / section
  │     ├── paragraph / list item
  │     ├── table / row
  │     └── structured source field
  ├── deterministic bounded segments
  │     ├── original source spans
  │     ├── artifact and heading context
  │     └── segment-processing result, including zero tags
  └── artifact-level concept assignments
        └── one or more exact segment-backed evidence spans
```

Preserve these distinctions:

1. **An artifact is a source identity.** A comment, Federal Register document,
   bill version, CFR section, report, filing, docket, proceeding, or entity row
   keeps its source-scoped identity.
2. **An element is part of an artifact.** It represents source structure such
   as a section, paragraph, list, table, or structured field.
3. **A segment is a processing view.** It is subordinate to one artifact and
   must never become a new regulation, proceeding, document, concept, or
   organization.
4. **Context helps the model but does not become evidence.** Titles, ancestor
   headings, neighboring text, or generated context must remain distinguishable
   from the original span that supports an assignment.
5. **Assignments belong to the artifact.** Segment-level model results are
   deduplicated and aggregated without erasing their segment and source-span
   provenance.

The design must apply to arbitrary public-sector and legal documents. Regulatory
profiles may specialize the general model, but they must not redefine what an
artifact, element, segment, context view, or evidence span means.

## Acceptance scope: documents, not public comments

This goal evaluates source documents themselves. Public-comment records are
out of scope for acceptance, including Regulations.gov comment rows and any
other artifact whose source role is explicitly a public comment. Do not use
comments as queries, retrieval candidates, gold examples, tagging inputs, or
policy-selection evidence. Dockets and proceedings may remain as relationship
context, but the evaluated content is the associated regulation, notice,
filing, report, opinion, agenda, legislative, or other document.

Keep the general comment adapter and segmentation policy working; they are part
of the reusable ontology, not this acceptance run. Existing all-profile runs
may be retained as superset evidence, but every reported acceptance metric and
selected policy must come from an immutable document-only view. Do not inspect
or depend on the remote LanceDB `comments` table for this work.

## Why this work is necessary

The current subject builder in `src/spicy_regs/ontology/subjects.py` bounds each
field to 4,000 characters and each subject to 12,000 characters. It records
`truncated_fields`, but the omitted text never reaches the model. That behavior
can hide relevant topics, entities, qualifications, definitions, and evidence.

`src/spicy_regs/ontology/segmentation.py` provides a deterministic paragraph,
line, sentence, word, and hard-boundary foundation. The current local candidate
connects this foundation to subject construction, checkpoints, model calls,
assignments, validation, and receipts. Focused tests prove reversible character
coverage, stable IDs, source offsets, prompt-injection preservation, and
document-scope isolation. The final comparative and acceptance receipts pass
for the bounded comparison-ready milestone recorded above; broader production
certification remains deferred.

The real all-profile OpenAI baseline at
`output/mixed-real-data-all-profile-openai-v1/openai-run-receipt.json` proves
that the provider path processed the sixteen profiles that existed in that
baseline. The current registry contains seventeen profiles. The baseline does
not prove segmentation quality:

- 48 subjects were generated, three from each profile;
- 143 structured provider calls were evidenced;
- 95 generated assignments received 100% validation coverage;
- no grounding failures or persisted API keys were found; and
- the reported F1 was 0.5833 against only three Federal Register documents.

Treat those figures as a pre-segmentation baseline. Recompute them from live
artifacts before comparison.

## Starting authority

Before editing:

1. Read every applicable `AGENTS.md`.
2. Inspect the live worktree and preserve all user changes, including unrelated
   untracked directories.
3. Read:
   - `docs/ontology-segmentation-research.md`;
   - `docs/ontology.md`;
   - `docs/rulespec-profile.md`;
   - `docs/mixed-real-data-corpus-report.md`;
   - `src/spicy_regs/ontology/subjects.py`;
   - `src/spicy_regs/ontology/segmentation.py`;
   - `src/spicy_regs/ontology/llm.py`;
   - `src/spicy_regs/ontology/checkpoint.py`;
   - `src/spicy_regs/ontology/concepts.py`;
   - `src/spicy_regs/transforms/build_concepts.py`;
   - `src/spicy_regs/transforms/build_concept_assignments.py`;
   - `src/spicy_regs/corpora/profile_evaluation.py`;
   - `src/spicy_regs/corpora/mixed_real_data.py`;
   - the ontology schemas, materialization manifest, and receipt validators; and
   - every focused test for subjects, segmentation, concepts, assignments,
     evaluation, receipts, and the mixed real-data corpus.
4. Reinspect the current seventeen `SUBJECT_PROFILES` and both explicitly excluded
   source tables. The live code and source schemas supersede this handoff.
5. Confirm current OpenAI SDK and API behavior from official OpenAI
   documentation before changing provider parameters.

Do not expose, print, copy into fixtures, or persist an API key. If a key was
ever pasted into chat, recommend rotation even when the local `.env` is ignored
and permission-restricted.

## Design gates

Write a short decision record before broad implementation. Resolve each gate
with evidence from the current schemas and real source files.

### 1. Canonical carrier

Compare:

1. a published `document_segments` table;
2. an immutable internal segment-ledger artifact stored with each ontology
   generation; and
3. evidence spans embedded only inside assignments, plus a separate processing
   ledger.

The result must support resumability, zero-tag records, deterministic audit,
failure recovery, and artifact-level queries without forcing consumers to treat
segments as ontology entities. Prefer an internal immutable segment ledger
unless publishing segments has a demonstrated consumer benefit.

### 2. Source text and offsets

Decide how to retain exact offsets when prompt text is normalized. Acceptable
approaches include:

- preserve raw text and avoid destructive normalization;
- maintain a reversible raw-to-prompt offset map; or
- store source-native element coordinates and exact text selectors.

Whitespace collapsing without a reversible map is not acceptable for
evidence-bearing text. Every quoted span must resolve unambiguously to the
versioned source artifact.

### 3. Token budget

Use the tokenizer that corresponds to the model or an explicitly documented,
conservative compatible encoding. Pin the tokenizer and policy version.
Character counts may provide a fast preflight estimate, but the final hard
limit must use tokens.

Evaluate at least 800, 1,200, and 1,800 input-token leaf budgets for ontology
tagging. Reserve explicit budgets for instructions, registry candidates,
artifact context, structured output, and safety margin.

### 4. Context policy

Compare:

- deterministic artifact title and heading-path prefixes;
- neighboring or parent expansion;
- limited overlap only when one structural element is itself oversized; and
- optional model-generated contextual prefixes.

Prefer deterministic context. If model-generated context is evaluated, store
it as a derived aid with its own model provenance and forbid it from satisfying
the evidence-grounding check.

### 5. Parser and adapter boundary

Prefer native source structure over inferred layout:

- source API and JSON fields for atomic records;
- eCFR or GovInfo XML for CFR and legislative hierarchy;
- source HTML or XML for Federal Register documents;
- structured activity arrays for lobbying filings;
- citation-bearing HTML for future broader case-law ingestion; and
- a document-layout parser such as Docling only for PDF, DOCX, image, or
  otherwise unstructured fallback.

Do not make a third-party parser's object model the ontology contract. Map
parsers into a small Spicy Regs artifact-and-element interface.

### 6. Aggregation semantics

Define:

- how duplicate artifact-and-concept proposals from several segments combine;
- how multiple evidence spans remain available;
- how contradictory segment judgments are represented;
- how candidate concepts proposed in separate segments deduplicate;
- how validation samples and validates multi-span assignments; and
- how stable IDs and supersession behave when segment policy or source bytes
  change.

An assignment must not disappear because a later segment produced zero tags.
A duplicated overlap span must not create a duplicated current assignment.

### 7. Dependency inversion

Treat dependency inversion as a system rule. High-level orchestration,
ontology semantics, evidence validation, and acceptance policy must depend on
project-owned protocols rather than concrete provider SDKs.

Keep OpenAI, Sentence Transformers, MLX/oMLX, rerankers, tokenizers, document
parsers, and storage implementations behind replaceable boundary adapters.
Inject clients, models, token auditors, parsers, clocks, and test doubles where
practical. Concrete adapter, package, model, and revision provenance must remain
visible in receipts, but no provider may redefine the artifact, element,
segment, context, evidence, or assignment contracts.

## Required implementation

### Artifact and element model

Add a versioned, general processing model with at least:

- artifact type and source ID;
- subject-profile ID and source table;
- artifact-version digest;
- source element ID, kind, ordinal, and parent or ancestor path;
- exact source field and source coordinates;
- raw text digest;
- segment ID, ordinal, policy version, boundary kind, and token count;
- previous, next, and parent segment links where applicable; and
- context fields stored separately from evidence-bearing text.

Stable IDs must derive from source identity, source-version bytes or digest,
source coordinates, and segment-policy version. They must not depend on Parquet
row order, current wall-clock time, or model output.

### Subject construction

Replace `_bounded_fields` and `truncated_fields` as the mechanism for fitting a
prompt. Subject construction must preserve every eligible source element and
yield one or more processing segments.

For each source field, either:

- include every byte or character in exactly one canonical source span; or
- record an explicit, tested exclusion reason for non-content such as an empty
  field or known duplicate representation.

No non-empty eligible text may vanish silently.

### Profile policies

Classify every current profile as an atomic record, repeated structured child,
or hierarchical document:

| Profile | Required default |
| --- | --- |
| Regulations.gov docket | One atomic metadata segment |
| Regulations.gov document | Metadata anchor plus structurally segmented body |
| Regulations.gov comment | Whole when short; paragraph-aware when long |
| Federal Register document | Article metadata now; native article sections when full text is present |
| Unified Agenda observation | One editioned structured record |
| CFR section | Section parent; paragraph, subparagraph, and table children when text is present |
| Congressional bill | One bill version; section and subsection hierarchy when text is present |
| SAM entity | One structured entity record |
| Lobbying filing | Filing parent with individual lobbying-activity children |
| FEC committee | One structured committee record |
| GAO report | Metadata or abstract anchor plus heading-aware report body |
| CRS report | Metadata anchor plus heading-aware report body |
| Court docket | One docket record; future filings and opinions remain separate artifacts |
| Court opinion | One official opinion package; preserve the PDF and extracted text as one artifact until source structure supports reliable lead, concurrence, and dissent separation |
| USAspending recipient | One structured recipient record |
| FCC proceeding | One proceeding record unless its description exceeds the budget |
| FCC filing | Whole express comment when short; heading and paragraph segmentation when long |

Keep `comments_index` excluded because it is aggregate partition metadata.
Keep `fr_docket_links` excluded as a relationship carrier. Tag their endpoint
artifacts, not the carrier rows.

If a current table contains metadata but no full body, prove its atomic policy
and add representative real full-text documents of that family to the
segmentation evaluation corpus. Do not pretend metadata-only rows exercise
long-document behavior.

### Model execution

Update the real OpenAI Responses path so each model call receives:

- one bounded segment;
- artifact identity and profile;
- allowed concept schemes;
- deterministic title and heading context;
- untrusted-content delimiters;
- exact evidence instructions; and
- enough segment metadata to translate a quote to artifact-level coordinates.

Continue to use structured output, `store=False`, explicit timeouts, bounded
retries, output-token caps, and safe call telemetry. Reject:

- incomplete responses;
- invalid structured output;
- schemes outside the profile's allowed schemes;
- evidence found only in added context;
- quotes that do not resolve to the declared source span;
- coordinates outside the artifact version; and
- model instructions found inside source content.

### Checkpoints and processing ledger

Checkpoint identity must include the segment ID and source-version digest.
Resuming a run must skip only the exact successfully processed segment version.

Persist a result for every selected segment, including:

- successful tags;
- successful zero-tag completion;
- rejected ungrounded output;
- validation result;
- retry-exhausted failure; and
- explicitly skipped non-content.

A new run must not mistake absence of an assignment for proof that a segment
was processed.

### Assignment aggregation

Aggregate segment proposals into artifact-level current assignments. Preserve:

- every accepted evidence span;
- the segment IDs that supplied it;
- the source coordinates and artifact digest;
- proposal and validation model provenance;
- disagreements or rejected spans; and
- run and supersession history.

Document and test the assignment grain. Do not introduce collisions when two
segments support the same concept, and do not multiply assignments merely
because a fallback split used overlap.

### Receipts

Extend receipts to prove:

- every required profile has a declared segmentation policy;
- every selected artifact has a complete segment ledger;
- every eligible source character is covered or explicitly excluded;
- no segment exceeds its declared token budget;
- segment IDs and source offsets validate;
- every model-processed segment has safe provider telemetry;
- every current assignment resolves to a source artifact and accepted segment;
- zero-tag segments remain visible;
- no secret-like value appears in artifacts;
- all required profiles occur in the real OpenAI evaluation; and
- the evaluation did not silently fall back to a mock model.

## Real evaluation dataset

Build a new immutable evaluation snapshot from real public data. It must be
larger and more varied than the 48-subject all-profile baseline.

At minimum:

1. include every taggable document profile and document role in scope;
2. include at least ten artifacts per in-scope profile;
3. stratify long-text profiles by short, medium, long, and extreme length;
4. include native structured text, HTML/XML, JSON fields, ordinary prose,
   tables or lists, and PDF-extracted text where those forms exist;
5. include related groups such as docket, document, Federal Register record,
   agenda item, CFR target, and bill authority;
6. include deliberately unrelated controls from the same agency or broad
   subject area so lexical similarity does not substitute for relatedness;
7. include repeated-RIN siblings without treating the RIN as document identity;
8. include empty, duplicate, malformed, adversarial, and prompt-injection
   examples;
9. record source URLs, retrieval dates, content digests, licenses or public
   status, and selection policy; and
10. rerun deterministically to prove byte-identical non-model artifacts.

A court docket is relationship context, not an opinion surrogate. The locked
supplemental Supreme Court opinion must become a separately profiled artifact
with gold, and the final snapshot must reach the same ten-real-artifact minimum
for that opinion profile. Move prompt-injection and other adversarial acceptance
cases onto document-role fixtures or locked documents; the existing synthetic
comment cases do not satisfy this document-only gate.

Use source-provided labels and identifiers as weak or exact gold where
appropriate: Federal Register topics, NAICS, bill policy metadata, court nature
of suit, agency fields, citations, and explicit source relationships. Add
hand-curated gold spans for long documents and boundary-crossing cases. The
same OpenAI model that generated a tag cannot serve as the sole gold judge.

## Segmentation experiment

Treat the existing Spicy Regs vector path as the incumbent retrieval baseline,
not as nonexistent work. `src/spicy_regs/vectordb/embed.py` currently uses
`BAAI/bge-base-en-v1.5` by default and embeds whole docket, document, or comment
rows. The companion notebook queries a `comments` LanceDB table in that same
768-dimensional model space. For this acceptance run, reproduce only the
document-row incumbent; the comment path is historical context and is out of
scope. Before selecting a replacement:

- inventory any materialized local or remote incumbent vectors and state
  plainly when their live availability cannot be verified;
- reproduce the pinned BGE model in the common evaluation harness;
- distinguish whole-row vectors from source-element or processing-segment
  vectors, which are different derived objects even when their source artifact
  is the same;
- never compare or join vectors from different embedding models as though they
  occupied one coordinate space; and
- retain the incumbent artifact-level retrieval result alongside the new
  segment-level result when both serve useful query modes.

Legacy embedding files that contain only a source ID and vector are not a
production segment cache: safe reuse also requires the exact model revision,
input-text digest, embedding-policy version, dimensions, normalization policy,
and source-artifact version. Preserve usable legacy data, but do not infer
those missing facts.

Compare at least:

1. source-aware structure-first segmentation;
2. structure-first segmentation with limited oversized-element overlap;
3. paragraph and sentence fallback without source hierarchy;
4. embedding-based semantic segmentation; and
5. LLM-guided boundary selection for long narrative documents only.

Use the same source artifacts, candidate registry, tagging prompt, validation
policy, and aggregation logic. Record configuration and cost for every arm.

The review of
[arXiv:2403.10407](../../evidence/arxiv-2403.10407-reranking-review.md)
adds these retrieval controls:

- include learned-sparse and sparse+dense-hybrid first-stage contenders;
- measure candidate Recall@10/25/50/100/200 before reranking;
- use one top-50 BGE cross-encoder rerank for the current milestone; retain
  oMLX and the 25/50/100/200 sweep as optional follow-up;
- hold candidate sets constant for paired reranker comparisons;
- ablate raw segment, title, title-plus-heading-path, and
  title-plus-heading-path-plus-source-type retrieval views without changing
  evidence coordinates;
- stratify legal relation queries rather than reporting topical relevance only;
  and
- keep listwise LLM reranking offline until it produces a profile-safe paired
  gain, with malformed or repaired permutations counted as failures.

Measure:

- complete source coverage and exclusion counts;
- deterministic segment IDs and boundaries;
- hard token-limit compliance;
- segment count and size distribution by profile;
- gold-span containment and boundary-crossing misses;
- retrieval recall, precision, MRR, and nDCG at several values of `k`;
- artifact-level micro and macro F1;
- profile-level F1 and zero-tag rate;
- evidence-grounding and coordinate accuracy;
- duplicate-assignment and cross-segment disagreement rates;
- aggregation accuracy for facts supported across sections;
- prompt-injection resistance;
- provider calls, input and output tokens, latency, retries, and estimated cost;
- resume behavior after controlled interruption; and
- behavior when the provider times out, rate-limits, returns invalid JSON,
  returns incomplete output, exhausts retries, or returns a non-retryable
  `insufficient_quota` hard-limit error.

Select policies by task. Corpus-wide ontology tagging and within-document
retrieval may use different views over the same canonical elements. Do not
declare a universal winner merely because one method leads one metric.

## OpenAI acceptance run

After the deterministic and mock-backed gates pass:

1. load the API key only from the ignored, permission-restricted environment;
2. create a new run ID and immutable input snapshot;
3. print a preflight count and token or cost estimate without exposing the key;
4. run the selected batch through real generation and validation;
5. validate every in-scope document profile and selected boundary class;
6. interrupt and resume at least one bounded run or reproduce this behavior
   with a controlled provider double before the paid final run;
7. generate the ontology receipt and strict OpenAI run receipt;
8. scan every output for key prefixes and secret matches;
9. compare quality, coverage, cost, and failures with the pre-segmentation
   baseline; and
10. keep the run local unless the user separately authorizes upload or
    publication.

The final report must state the exact number of artifacts, segments,
assignments, zero-tag segments, generation calls, validation calls, tokens,
latency, retries, failures, and evaluated gold examples. It must distinguish
pipeline conformance from semantic quality.

## Documentation and contract updates

Update:

- `docs/ontology-segmentation-research.md` with the selected implementation;
- `docs/ontology.md` with artifact, element, segment, context, and evidence
  semantics;
- `docs/rulespec-profile.md` with the local-versus-shared ontology boundary;
- `docs/mixed-real-data-corpus-report.md` with the larger segmented evaluation;
- data-dictionary descriptions for affected assignment or ledger artifacts;
- materialization and receipt documentation;
- query examples showing artifact-level tags with exact segment evidence; and
- migration notes for removal of hard truncation and old checkpoint keys.

Keep operational segmentation metadata local to Spicy Regs unless an
independent interoperability analysis justifies a reusable Rulespec term. A
segmenter implementation detail is not automatically shared ontology
vocabulary.

## Non-goals

This goal does not authorize:

- treating a segment as a source document or domain entity;
- merging documents or proceedings because they share a RIN, citation, topic,
  title, embedding, or segment;
- using regex to infer semantic identity or relatedness;
- using an LLM-generated summary as source evidence;
- hiding dropped text behind a truncation flag;
- replacing exact source identifiers and structured relations with vector
  similarity;
- ingesting every historical document when a representative real full-text
  evaluation can prove the adapter;
- promoting retrieval-grade concepts or segment metadata into normative
  Rulespec vocabulary without its own review;
- lowering a quality threshold merely to make a run pass;
- committing, pushing, uploading, publishing, or deploying without a separate
  user request; or
- editing unrelated user work.

## Verification

Run focused red-green tests during implementation, then all relevant gates:

```bash
R2_PUBLIC_URL='' uv run pytest
uv run ruff check src tests
uv run ty check src tests
uv run spicy-regs-dict check
uv run --group docs mkdocs build --strict
git diff --check
```

Also run:

- deterministic evaluation-snapshot generation twice;
- source-coverage and offset-integrity audits;
- tokenizer hard-limit tests;
- checkpoint interruption and resume tests;
- provider failure-injection tests;
- assignment deduplication and aggregation tests;
- strict ontology and OpenAI receipt generation; and
- the bounded real OpenAI acceptance run.

If `R2_PUBLIC_URL` is present in the shell, unset it explicitly for hermetic
tests. Report live-source integration tests separately from hermetic unit and
corpus gates.

Recorded comparison-ready result: the hermetic suite passed with 641 tests and
3 deselections; Ruff, `ty`, the 32-table data-dictionary check, the strict
documentation build, and `git diff --check` passed. A final process audit found
no active segmentation experiment or oMLX job.

## Comparison-ready definition of done

End the current agent run when all of these conditions hold:

1. The immutable document scope and every compared output validate against the
   same dataset, gold queries, budgets, candidate rules, and metric provider.
2. A paired results table reports the four directly comparable non-LLM arms;
   the LLM-guided arm is clearly labeled as a system-level comparison.
3. Deterministic, incumbent BGE, OpenAI, learned-sparse, hybrid, and
   whole-artifact results have passing receipts or an explicit failed result
   that remains visible in the table.
4. Every dense result has keyed model-native input-limit and truncation
   evidence, whether embedded in the original result or attached as an
   immutable audit artifact.
5. The pinned BGE cross-encoder scores the selected top-50 candidate groups and
   reports quality, latency, truncation, and failure evidence; oMLX is named as
   intentionally deferred.
6. One selected configuration completes real scoped OpenAI generation and
   validation with grounding, profile coverage, provider, and secret-safety
   receipts.
7. Focused affected tests, lint, type checking, data-dictionary checks, strict
   documentation, and diff checks pass.
8. The final handoff identifies the chosen configuration, exact evidence,
   limitations, and deferred production work without claiming the deferred
   work passed.

This bounded definition supersedes the broader checklist below for the current
comparison run.

Completion record: all eight bounded conditions pass locally. The selected
configuration is `structure-overlap-1800`; the fully evidenced retrieval
cascade is incumbent BGE dense top-50 followed by
`BAAI/bge-reranker-v2-m3`; the real ontology path uses
`openai:gpt-5.6-sol` with the exact v4 alignment policy. This is a
comparison-ready selection, not a deployment or a claim that the broader
checklist below passed.

## Broader production definition of done (deferred)

Report completion only when all of these conditions hold:

1. Every live subject profile has a versioned atomic, structured-child, or
   hierarchical segmentation policy, while acceptance metrics include only
   the document scope declared above.
2. Eligible source text is preserved completely; every omission has an
   explicit, tested reason.
3. Segment limits use a pinned tokenizer and no segment exceeds the declared
   budget.
4. Artifact, element, segment, context, and evidence identities remain
   distinct in code, storage, prompts, and documentation.
5. Checkpoints and the processing ledger represent successful zero-tag work,
   failures, retries, validation, and resumability.
6. Artifact-level assignments retain exact source coordinates and all accepted
   segment provenance without duplicates.
7. The immutable real-data document view covers all in-scope profiles, related
   and unrelated records, long documents, structural formats, and adversarial
   cases without public-comment artifacts.
8. The selected segmentation policy beats or justifiably trades against the
   pre-segmentation baseline on declared quality, coverage, cost, and latency
   gates.
9. A real OpenAI generation-and-validation run passes strict provider metadata,
   grounding, profile coverage, secret-safety, and ontology receipts.
10. Focused tests, the hermetic full suite, lint, type checking, data-dictionary
    checks, strict docs, deterministic reruns, and diff checks pass.
11. The final handoff names every changed file, command, result, corpus count,
    quality metric, cost, remaining limitation, and intentionally deferred
    adapter.
12. High-level pipeline and validation code depends on project-owned protocols;
    provider packages remain replaceable adapters with injected test doubles
    and explicit receipt provenance.

If any condition remains unmet, report the pipeline as partial. Do not call it
production-ready because the fallback splitter, provider transport, or a small
balanced sample passes independently.
