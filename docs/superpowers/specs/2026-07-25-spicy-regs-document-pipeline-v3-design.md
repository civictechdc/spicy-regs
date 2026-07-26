# Spicy Regs Document Pipeline v3

- **Date:** 2026-07-25
- **Status:** Candidate implementation spec
- **Scope:** Document processing, search, AI extraction, approval, comparison,
  testing, and run records
- **Governing vision:** [`2026-07-25-rulespec-spicy-regs-complete-vision-goal.md`](2026-07-25-rulespec-spicy-regs-complete-vision-goal.md)
- **Related contracts:**
  [`2026-07-25-relation-comparison-resolver-contract.md`](2026-07-25-relation-comparison-resolver-contract.md)
  (comparison protocols),
  [`2026-07-25-relation-exclusion-v2-human-adjudication-protocol.md`](2026-07-25-relation-exclusion-v2-human-adjudication-protocol.md)
  (benchmark reviewer rules),
  [`2026-07-24-document-ai-package-fit-decision.md`](2026-07-24-document-ai-package-fit-decision.md)
  (Docling and package boundaries),
  [`2026-07-24-production-document-segmentation-agent-goal.md`](2026-07-24-production-document-segmentation-agent-goal.md)
  (segmentation baselines), and
  [`TODO-RULE.md`](../../../TODO-RULE.md) (Milestone C owns
  `docs/retrieval-evaluation.md`; Milestone D wires this pipeline into the
  fair comparison)
- **Approach:** Greenfield, reuse shared code, keep the design simple, use
  maintained packages, and keep providers replaceable

## Decision

Build v3 as one small pipeline with:

1. one way to start, resume, finish, and check a run;
2. one manifest and one final receipt per run;
3. one fixed order of plainly named steps;
4. small interfaces for OpenAI, Codex, embeddings, and rerankers; and
5. separate, strongly checked data for search, AI suggestions, approval, and
   comparison.

V3 will copy the best existing behavior. It will not preserve an old API
merely because it exists. After v3 reproduces the important behavior and
historical run checks, it will replace the active v1, v2, segmentation,
artifact-search, sparse-search, reranking, and tagging runners.

This is not a new general-purpose AI search framework. It is a clean assembly
of working parts.

## What the pipeline does

```text
source records
  -> exact document versions and meaningful source sections
  -> model-sized text segments
  -> direct, database, keyword, and meaning-based search
  -> reranked evidence candidates
  -> structured AI suggestions
  -> exact source checks and approval rules
  -> local output tables
  -> optional neutral comparison findings
```

The stages answer different questions:

| Stage | Question |
| --- | --- |
| Search | What documents or sections should we inspect? |
| Extraction | What does this exact source text appear to say? |
| Approval | May this suggestion enter this named output for this use and time? |
| Comparison | What differs between approved statements? |

An embedding score can answer the first question. It cannot answer the other
three.

## Key terms

| Term | Plain meaning |
| --- | --- |
| `Artifact` | One exact, immutable version of a document or record |
| `SourceFragment` | One stable section, clause, paragraph, table, list item, or other meaningful region inside an Artifact |
| `ProcessingSegment` | Temporary text prepared for a model or search index |
| `RetrievalHit` | One search result from one search method |
| Candidate | A suggestion that has not been approved |
| Evidence | Exact source text and coordinates supporting or challenging a candidate |
| Approval record | Who or what approved, rejected, or could not decide a candidate, for which use and time |
| Proof record | The inputs, rule version, result, and explanation for one automated check |
| Receipt | A checkable summary of what a run used, produced, skipped, failed, and passed |
| Hash or digest | A fixed fingerprint used to prove that a file or setting has not changed |
| Parquet | The column-based file format used for Spicy Regs data tables |
| JSONL | One JSON record per line, used for append-only work history |
| BGE | The current local model that turns text into vectors for meaning-based search |
| Learned sparse search | A model-assisted keyword method that still produces explainable term-based matches |
| Reciprocal-rank merge | A simple way to combine result lists by rank without treating unlike scores as equal |
| Cross-encoder reranker | A model that reads a query and candidate together, then reorders a small candidate list |

`Artifact` and `SourceFragment` are durable data. A `ProcessingSegment` is
usually temporary.

## Rules v3 must not break

### Identity and source text

- One `Artifact` represents one exact source state.
- A `SourceFragment` points to one Artifact and exact source coordinates.
- A `ProcessingSegment` points back to the fragments and text used to create
  it.
- Stable work identity comes from the source or a public or profile-specific
  vocabulary.
- A shared title, RIN, URL fragment, docket number, embedding score, or search
  rank does not prove identity or version history.
- V3 never invents version history when the source does not provide enough
  evidence.
- Accepted data always names the Artifact hash and exact source region used.

### Tags and typed data

- Documents and meaningful source sections can each have their own tags.
- A section tag needs evidence from that section.
- A documented rule may combine approved section tags into a document tag.
- A document tag may suggest likely tags for another section. It cannot prove
  them.
- Dates, organizations, identifiers, authorities, and document states stay
  typed. They are not fuzzy topic tags.
- Model confidence and usage counts never promote a concept into the shared
  reviewed vocabulary.

### Relationships and missing information

- Explicit support becomes an affirmed `RelationshipAssertion`.
- Explicit rejection becomes a denied `RelationshipAssertion`.
- Adoption, removal, suspension, and replacement become a
  `RelationChangeEvent`.
- Disagreement between approved assertions may become a neutral
  `RelationFinding`.
- Silence, missing output, failed search, and incomplete coverage remain
  `unknown`.
- V3 has no generic negative tag.
- V3 does not enable omission findings or automated legal conclusions.

### AI, evidence, and approval

- AI may suggest source text and offsets. Code checks them.
- Code may fix offsets only when the quoted text appears exactly once in the
  named source field.
- Fuzzy, normalized, meaning-based, or ambiguous quote repair fails.
- Exact evidence, creation history, confidence, and approval stay separate
  from the statement itself.
- An AI model cannot approve its own suggestion.
- Asking the same model to review its answer adds another opinion, not
  approval.
- A failed automated check is not a negative fact about the source.
- An undecided check remains undecided.

## Code shape

```text
src/spicy_regs/docpipeline/
├── runtime.py
├── source.py
├── segments.py
├── retrieval.py
├── extraction.py
├── approval.py
├── comparison.py
├── workflow.py
├── cli.py
└── adapters/
    ├── openai.py
    ├── codex_cli.py
    ├── docling.py
    └── sentence_transformers.py
```

The package is named `docpipeline` to avoid colliding with the existing
`spicy_regs.pipelines` ETL package. The legacy `run-pipeline` entry point is
renamed `run-regulations-etl` in the same change that lands `cli.py`, so
`spicy-regs pipeline …` unambiguously means this pipeline.

| File | Job |
| --- | --- |
| `runtime.py` | Plans, work IDs, checkpoints, files, hashes, secret scans, receipts, resume, checking, and rebuild |
| `source.py` | Convert source records into Artifacts and SourceFragments |
| `segments.py` | Create model-sized ProcessingSegments |
| `retrieval.py` | Filter, search, merge results, rerank, and measure search quality |
| `extraction.py` | Build prompts and schemas, call a text model, check responses, and produce candidates |
| `approval.py` | Check evidence, identity, scope, access, vocabulary, review, and output rules |
| `comparison.py` | Compare approved relationship statements and produce neutral findings |
| `workflow.py` | Call the steps in their fixed order |
| `cli.py` | Expose pipeline commands |
| `adapters/` | Connect OpenAI, Codex, Sentence Transformers, and Docling to the small interfaces and source records above |

`workflow.py` uses normal Python calls. V3 does not add a workflow language,
plugin registry, event bus, scheduler, or dependency graph.

Rules for imports:

- `runtime.py` never imports a pipeline step.
- A step may use public data from an earlier step, but not its private helpers.
- Provider libraries stay inside `adapters/`. Docling enters through
  `adapters/docling.py` and is used only by `source.py`.
- DuckDB is storage, not a provider; `retrieval.py` may use it directly for
  exact and database search.
- Comparison code does not import search or provider implementations.
- General code never imports a regulatory, judicial, or other document
  profile.

## One run

Every run starts from a secret-free `RunPlan`. It records:

- mode and requested steps;
- source snapshot or frozen test dataset;
- Rulespec version and schema digest;
- active document profiles and access rules;
- active concept schemes, local and registered vocabulary snapshot, and their
  version and digest;
- segmentation rule and tokenizer;
- search methods, models, model revisions, merge rule, and rerank depth;
- extraction prompt, output schema, and response checks;
- approval and comparison rule versions;
- provider settings without credentials;
- human-review file hashes when testing requires them;
- code commit; and
- required versus optional work.

The normal order is:

```text
source -> segment -> retrieve -> extract -> approve -> compare -> materialize
```

### Prerequisites

| Step | Required input | May use an earlier checked run? |
| --- | --- | --- |
| `source` | Source snapshot | No |
| `segment` | Artifacts and SourceFragments | Yes |
| `retrieve` | Artifacts, fragments, or segments, depending on search level | Yes |
| `extract` | Exact fragments or segments; retrieval is optional when the target is already known | Yes |
| `approve` | Extraction candidates and their source data | Yes |
| `compare` | Approved relationship statements | Yes |
| `materialize` | Approved records and completed required checks | Yes |

A run may stop after any step. It may start from an earlier run only after
checking that run's receipt and required file hashes.

### Modes

`build` processes real source data without test answers and creates a local
candidate generation.

`diagnostic` uses exposed examples, provisional answers, or a prompt under
development. It records:

```json
{
  "benchmark_eligible": false,
  "publication_eligible": false
}
```

Diagnostic scores cannot rank providers or set production thresholds.

`benchmark` uses a frozen dataset, hidden test set, fixed prompts and settings,
and sealed human-reviewed answers where needed. It never publishes data.

Repeated provider calls remain separate runs connected by one comparison ID.

`rebuild` is a command, not another mode. It recomputes all possible files from
stored inputs and provider responses without calling a provider.

### Materialize is not publish

`materialize` writes a complete local generation. It never changes the active
generation pointer.

Publishing is a separate command that requires explicit maintainer
authorization. Before publishing, it must prove:

- the required reviewed Rulespec release is reachable;
- Spicy Regs pins that exact release and schema digest;
- the Rulespec-to-Spicy-Regs data mapping passes;
- the local run passes all required checks; and
- a separate authorization record names the run and publisher.

The pipeline must never publish automatically after a successful build.

## Run files and behavior

Each completed run has one directory:

```text
run/
├── manifest.json
├── transitions.jsonl
├── source/
│   ├── artifacts.parquet
│   └── fragments.parquet
├── processing/segments.parquet
├── retrieval/
│   ├── hits.parquet
│   └── exclusions.parquet
├── extraction/
│   ├── provider-calls.parquet
│   ├── local-concept-candidates.parquet
│   ├── concept-assignment-candidates.parquet
│   ├── value-candidates.parquet
│   ├── relationship-candidates.parquet
│   ├── change-event-candidates.parquet
│   ├── rejections.parquet
│   └── calls/<work-id>/{request,response}.json
├── approval/
│   ├── decisions.parquet
│   ├── evidence.parquet
│   └── proofs.parquet
├── output/
│   ├── local-concepts.parquet
│   ├── concept-assignments.parquet
│   ├── value-assertions.parquet
│   ├── relationship-assertions.parquet
│   ├── relation-change-events.parquet
│   └── relation-findings.parquet
├── metrics.json
└── receipt.json
```

Only requested steps need files. A requested step with no rows writes an empty,
correctly shaped table. `metrics.json` appears only when the run has test
answers.

The receipt records:

- run ID, mode, final state, and plan hash;
- publication and benchmark eligibility;
- source files and earlier runs used;
- Rulespec, profile, concept vocabulary, prompt, schema, rule, and provider
  versions and digests;
- planned, completed, empty, rejected, skipped, failed, and unresolved counts;
- every file's path, bytes, rows when applicable, and SHA-256;
- provider calls, retries, failures, time, and token totals;
- each step's check results;
- secret and access-control results;
- test-answer and metric file hashes; and
- failures, warnings, and the receipt's own hash.

Each step checks its own data. The runtime checks file completeness, hashes,
cross-file references, secrets, access scope, and required-work counts.

### Work, failure, and resume

Copy `BatchCheckpoint`'s append-only JSONL, torn-line recovery, exact work IDs,
and duplicate protection.

A work ID includes the step, task, input hashes, settings, prompt and schema
hashes, provider configuration, and earlier run ID.

Every planned item has one state:

- `completed`;
- `completed_empty`;
- `rejected`;
- `skipped`;
- `failed`; or
- `unknown`.

`completed_empty` means success with no result. `failed` never becomes empty.
A required skipped, failed, or unresolved item prevents the run from passing.
Optional work must be declared optional before the run starts.

The runner uses a sibling work directory. A crash leaves it for resume. A
failure writes a safe failure receipt. Running the same plan again reuses
completed work and retries only incomplete work. After all checks pass, the
work directory is renamed atomically.

### Check and rebuild

```text
spicy-regs pipeline preflight PLAN
spicy-regs pipeline run PLAN --output RUN
spicy-regs pipeline validate RUN
spicy-regs pipeline rebuild RUN --output REBUILT
spicy-regs pipeline publish RUN --authorization AUTHORIZATION
```

`validate` does not trust `receipt.json`. It reloads the plan and source files,
checks earlier runs, regenerates cleaned candidates, approval decisions,
metrics, and hashes, and compares them with stored data.

`rebuild` uses stored requests, responses, and deterministic inputs. It records
`provider_invoked: false` and never changes historical request or response
files.

`publish` is not part of automated testing and cannot run without a separate
authorization file.

## Step decisions

### Source and segment

Source adapters return exact Artifacts and SourceFragments. Use native XML,
HTML, JSON, and API structure first. Use Docling only when a source does not
provide better structure. Docling objects never become ontology data.

Unknown identity, version, coordinates, or access status produces quarantine
or failure according to the plan.

Use `structure-overlap-1800` as the active segment baseline:

- keep source structure together when possible;
- apply token limits after structure;
- record exact source slices and parent headings;
- record tokenizer, token count, model limit, truncation, settings, and hash;
- keep title and heading context separate from evidence; and
- record exclusions and successful zero-work cases.

Other segmenters stay test options until they beat this baseline without
losing evidence spans.

### Retrieve

Use exact identifiers and checked database or graph links first. For broader
search, use:

```text
keyword or model-assisted keyword search
  + BGE meaning-based search
  -> merge both result lists by rank
  -> fixed candidate depth
  -> a cross-encoder reads and reorders the top candidates
```

Apply identity, version, authority, jurisdiction, time, access, graph, and tag
filters before meaning-based ranking.

Keep document search for routing and section search for evidence. Store the
search method, level, rank, score, source region, and exclusion reasons in
`RetrievalHit`. A score or rank never creates a relationship or tag.

Inferred lookup — model-assisted multi-document relationship lookup, the
vision's fourth lookup class — is not a v3 retrieval method. The fair
comparison's inferred arm runs over approved extraction outputs and checked
graph links that v3 materializes.

### Extract

Each extraction task defines one prompt, one strict output schema, input data,
hidden test fields, response checks, exact evidence rules, abstention, and
metrics.

For relationships, copy the best v2 rules:

- the model does not generate known target IDs;
- assertion and change event are separate;
- assertion polarity is separate from change operation and stage;
- time, intended effect, attribution, claimant, and condition are separate;
- the model checks the strongest competing interpretation;
- reviewers may accept several sufficient evidence boundaries;
- required and optional answers score differently; and
- relationship meaning, secondary fields, and evidence quality score
  separately.

Tag and typed-value extraction use the same provider runner but keep their own
schemas and checks.

### Approve

Every approval names a consumer, scope, policy version, and evaluation time.
Each check returns `pass`, `fail`, or `unknown` plus its inputs, rule version,
result, and explanation.

| Output | Retrieval-grade local output | Decision-grade output |
| --- | --- | --- |
| Existing `LocalConcept` assignment | Code may approve with exact evidence, valid subject, and passing profile rules | Human or source-based review required when the consumer requires it |
| New `LocalConcept` | Code may create a local, provisional concept with source evidence, scheme, definition, and no ID collision | Cannot become `RegisteredConcept` without human promotion review |
| `ValueAssertion` | Code may approve trusted imported values; AI values also need exact evidence and type checks | Human or profile-approved source rule required |
| `RelationshipAssertion` | Code may approve for a named source-extraction output when identity, predicate, evidence, scope, and access pass | Human or profile-approved source rule required |
| `RelationChangeEvent` | Code may approve for a named source-extraction output with exact event evidence, time, operation, and stage | Human or profile-approved source rule required |
| `RelationFinding` | Code may produce a neutral finding from approved inputs and complete proof records | Any legal or policy interpretation requires a separate human-reviewed profile |

New local concepts live in `output/local-concepts.parquet`. Their IDs include
the local scheme and normalized meaning. Their assignments can be used for
local retrieval. Promotion to the shared vocabulary creates a separate
reviewed record; it does not rewrite the local history.

### Compare and materialize

Comparison uses approved relationship statements only. It checks the
relationship, approval, evidence, baseline, document pairing, time, scope, and
version history.

It may return:

- `satisfied`;
- `affirmed_denied_discrepancy`;
- `conflict`;
- `not_comparable`; or
- `unknown`.

It does not call an AI model. V3 does not return
`expected_relation_not_observed` and does not decide legal effect.

Materialization writes only approved records and never deletes history. It
includes counts for approved, rejected, unknown, quarantined, empty, skipped,
and failed work.

## Provider choices

Do not create one interface for text models, embeddings, sparse encoders,
rerankers, parsers, databases, and graph stores.

Use four small interfaces:

| Interface | Input | Output |
| --- | --- | --- |
| Structured text model | Prompt, schema, source data, output limit | Checked JSON plus call details |
| Dense embedder | Exact texts and model settings | Vectors plus tokenizer and call details |
| Sparse encoder | Exact texts and model settings | Sparse vectors plus tokenizer and call details |
| Reranker | Query, fixed candidates, and model settings | One score per candidate plus tokenizer and call details |

Every call returns its output and call details together. V3 removes the mutable
`last_call_metadata` side channel.

Copy OpenAI's working strict-output, retry, token-budget, and safe-call code
from `OpenAIOntologyModel`. Move tag-specific behavior out of that class.

Keep the Codex adapter's ignored user settings, disabled optional features,
temporary read-only environment, removed credentials, strict event allowlist,
local schema check, and safe call details.

OpenAI and Codex use the same prompt and schema but remain separate test arms.

Keep Sentence Transformers for BGE embeddings, learned sparse search, and the
`BAAI/bge-reranker-v2-m3` cross-encoder. Keep exact NumPy ranking as the test
reference, reciprocal-rank merge, OpenAI `text-embedding-3-large` as a
challenger, and `ir-measures` for search metrics.

Each provider uses its own tokenizer check. `tiktoken` controls OpenAI prompt
budgets. It does not prove that BGE input fits.

## Measurable gates

Detailed dataset membership, splits, final metric thresholds, and exclusion
rules will live in `docs/retrieval-evaluation.md`. That file must be frozen
before the hidden benchmark or provider comparison begins. Thresholds cannot
change after seeing candidate results.

### Migration parity on the existing frozen data

V3 must meet or beat the current checked baselines:

| Measure | Minimum |
| --- | ---: |
| Gold spans contained by `structure-overlap-1800` | 35/35 |
| Hybrid search Recall@50 | 0.8286 |
| Reranked BGE Recall@10 | 0.7143 |
| Reranked BGE Recall@50 | 0.8000 |
| Reranked BGE MRR | 0.4639 |
| OpenAI tag precision / recall / F1 | 0.8857 / 0.8857 / 0.8857 |
| Accepted evidence grounding | 1.0000 |
| False target relationships on unrelated controls | 0 |
| Secret matches | 0 |

These numbers prove migration parity only. They do not certify broader
production quality.

### Final mixed-data gate

The final evaluation file must set numerical thresholds for:

- search recall and ranking;
- extraction precision, recall, and abstention;
- approval accuracy;
- comparison accuracy;
- each source family and relationship type;
- repeated-run variation; and
- maximum allowed regression from the incumbent.

Unless that file sets a stricter rule:

- Recall@50 may not fall by more than one percentage point overall or three
  points in any source family;
- evidence resolution, source identity, access scope, and file references must
  be 100 percent valid;
- required work completion must be 100 percent;
- false relationships on unrelated controls must be zero;
- secret leaks and partial publication must be zero; and
- omission and legal-effect output must remain zero because they are disabled.

Use one frozen, mixed, document-only dataset with real related, unrelated,
ambiguous, denial, change, hard-negative, version-mismatch,
jurisdiction-mismatch, and format-duplicate examples. Exclude comments.

Freeze the dataset, prompt, schema, settings, and expected answers before paid
calls. Use two model-blind human reviewers and a third person for
disagreements. Keep exposed development examples separate from the hidden
test set. Run three identical blinded OpenAI repetitions and treat Codex as a
separate provider.

Report search, extraction, evidence, approval, and comparison separately and
show weak source families instead of hiding them in one score.

## Implementation appendix

### Best existing code to copy

| Need | Best source |
| --- | --- |
| Stable JSON and IDs | `ontology/common.py` |
| Resume checkpoints | `ontology/checkpoint.py` |
| Streaming file hash | `ontology/receipt.py` |
| File inventory with bytes, rows, and hash | `corpora/document_acceptance_scope.py` |
| Safe success, failure, validation, and rebuild | `corpora/relation_exclusion_evaluation.py` |
| OpenAI strict output | transport and retry code in `ontology/llm.py` |
| Tool-free Codex | `ontology/codex_cli.py` |
| Source parsing and document records | `ontology/adapters.py` and `ontology/subjects.py` |
| Selected segmenter | `structure-overlap-1800` path in `corpora/segmentation_experiment.py` |
| Exact quote alignment | `resolve_exact_evidence_offsets` in `ontology/llm.py` |
| Dense embeddings | `SentenceTransformerEmbeddingProvider` |
| Sparse search and result merge | selected code in `segmentation_sparse_retrieval.py` |
| Cross-encoder reranking | `SentenceTransformersReranker` |
| Relationship extraction | v2 prompt, schema, response checks, scoring, and human-review gate |
| Relationship comparison | rule checkers and comparison code in `ontology/relation_findings.py` |

Copy behavior and focused tests. Do not copy private names, hard-coded case
counts, or old receipt assumptions.

### Runner cutover

| Old runner | V3 must prove before removal |
| --- | --- |
| Relation v1 and v2 | Gold-free model input, strict schema, exact evidence, safe provider failure, validation, rebuild, scoring, and human gate |
| Segmentation experiment | Same selected segments, source coverage, offsets, token records, and gold-span containment |
| Artifact and segment search | Same input records, candidate IDs, ranking inputs, and checked metrics |
| Sparse and hybrid search | Same sparse vectors or checked score tolerance, result merge, candidate recall, and resume |
| Reranking | Same fixed candidate set, model revision, tokenizer checks, scores within declared tolerance, and metrics |
| Tagging | Same selected inputs, exact evidence rule, empty/failure states, tag combination, provider records, and metrics |
| Relationship comparison | Same neutral outcomes and proof records for all fixtures |

Cut over one runner at a time:

1. run old and new code on the same fixed inputs;
2. record expected differences and approve them in the migration test;
3. pass the old fixtures through v3;
4. keep a read-only checker for immutable historical runs;
5. remove the old active command in the same change; and
6. use Git and the prior generation pointer for rollback, not a second active
   implementation path.

### Build order

1. Test work IDs, checkpoints, atomic rename, secrets, inventory, required
   failures, empty results, resume, validation, and rebuild.
2. Build the runtime and OpenAI, Codex, embedding, sparse, and reranking
   adapters.
3. Move v2 relationship extraction first.
4. Move source parsing and `structure-overlap-1800`.
5. Move document and section search, BGE dense, sparse search, result merge,
   and reranking.
6. Move tag and typed-value extraction, section-to-document tag combination,
   and approval.
7. Move relationship comparison.
8. Run the frozen mixed-data test and remove replaced runners.

Local implementation may proceed before the Rulespec release. Publishing
cannot proceed until the reviewed Rulespec release, exact schema digest,
Spicy Regs mapping, paired checks, and explicit maintainer authorization all
pass.

## Done means

V3 is complete when:

- build, diagnostic, and benchmark use one runner;
- one receipt checks every requested step and file;
- each step keeps its own strongly checked data;
- provider libraries stay inside adapters;
- no step imports another step's private helper;
- successful empty results differ from failures;
- resume does not repeat finished paid work;
- required failures prevent materialization;
- validation recomputes results instead of trusting the receipt;
- rebuild never calls a provider;
- document and section tags remain separate and traceable;
- document tags never prove section tags;
- search scores never become facts;
- an AI model never approves itself;
- denial, change, disagreement, silence, and omission remain distinct;
- the current baseline numbers pass;
- the frozen mixed-data thresholds pass;
- clean-checkout tests cover providers, failures, resume, secrets, access, and
  rollback;
- a new document family needs only a source adapter, profile, examples, and
  tests; and
- publishing remains a separate authorized action after the paired Rulespec
  release and Spicy Regs checks.

## Reasons to stop and reshape

Stop and change the design if:

1. a run cannot reproduce every local output row from stored source data,
   provider responses, rules, and review records;
2. a new document family requires changes to search, extraction, approval, or
   comparison instead of only a source adapter and profile;
3. shared runtime code needs to understand a prompt, expected answer, ranking
   metric, or ontology rule;
4. one provider interface hides important retry, token, or output differences;
   or
5. v3 creates more active run formats than it removes.

## Recommendation

Implement this design.

It keeps the working parts, removes repeated pipeline code, and makes the
central distinction clear:

```text
possibly relevant
  is not the same as
the source appears to say this
  is not the same as
approved for this use
```
