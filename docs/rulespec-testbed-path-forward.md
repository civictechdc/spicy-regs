# Fast path to a working Rulespec testbed

- **Date:** 2026-07-26
- **Updated:** 2026-07-27 — added the functional semantic-learning end state,
  package-first extension slots, and parser bakeoff decisions
- **Status:** Prototype executed once in an isolated worktree but not yet
  integrated; independently reviewed by `claude-fable-5` before implementation
  and by native Sol architecture/code reviewers after the first run
- **Near-term objective:** use real documents and model output to find and fix
  Rulespec and Spicy Regs accuracy problems
- **Execution rule:** new `spicy_regs.docpipeline` code runs the work; existing
  `spicy_regs.corpora` artifacts provide the benchmark and baseline
- **Not in the critical path:** MCP, publication, historical migration parity,
  document-retrieval serving, a formal Rulespec release, or a complete
  production platform

## Decision

Stop expanding the migration harness. Use the new document-pipeline source,
segmentation, model-adapter, extraction, and run-storage code to repeat the
useful experiment that already exists:

```text
selected real documents
  -> new source parsing and structure-overlap-1800 segmentation
  -> LLM tag candidates with exact source evidence
  -> stored provider requests and responses
  -> scoring plus direct review of mistakes
  -> one focused prompt, profile, local-taxonomy, or Rulespec correction
  -> rerun the same sample and compare
```

Determinism supports this loop by holding the inputs, segment boundaries, and
scoring steady. Accuracy is the outcome. We should accept model-dependent
results and iterative improvement; we should not accept ungrounded tags,
uninspectable failures, or changes that cannot be compared on the same sample.

## Functional end state

Rulespec is the system's structured semantic memory. Spicy Regs exercises and
improves that memory against real regulatory data. A frontier model does not
remember prior API calls; the combined system remembers through concepts,
aliases, definitions, mappings, assignments, evidence, examples, corrections,
and measured outcomes.

The same semantic records must shape model input, constrained output,
retrieval, evaluation, and later training. If they exist only as metadata after
inference, the system has built a glossary rather than a learning loop.

### Semantic records and their jobs

| Record or property | Functional job |
| --- | --- |
| `RegisteredConcept` | Stable concept identity and meaning for prompts, retrieval, evaluation, and downstream models |
| `LocalConcept` | Immediate workspace-owned representation of useful new meaning that does not fit the shared registry |
| `ConceptScheme` | Keeps facets such as topic, industry, regulated entity, authority, and document role distinct |
| `skos:prefLabel` | Canonical output label |
| `skos:altLabel` | Synonyms, abbreviations, source wording, lexical expansion, and entity-linking candidates |
| Definition and scope notes | Positive meaning boundary for classification and retrieval |
| Examples and counterexamples | Prompt demonstrations, hard negatives, and later supervised-training rows |
| Exact, close, broader, narrower, and related mappings | Semantic comparison, query expansion, partial credit, and diagnosis of answers that are too broad, too narrow, or merely related |
| Primary, substantive, mention, and contextual assignment roles | Separate a document's central topic from useful secondary material and incidental mentions |
| Exact evidence | Localized supervision linking an assignment to the text that supports it |
| Provenance and structured review | Attribute an error to the source, parser, model, prompt, registry generation, profile, or evaluator |
| Promotion, merge, split, replacement, and deprecation events | Preserve what the system learned while evolving the vocabulary used by later runs |

Promotion functions as semantic compression. Repeated local discoveries,
aliases, evidence, and corrections become one stable concept that later models
can reuse. A model may propose and use a `LocalConcept` immediately. Human
review of promotion can happen asynchronously without blocking the local
learning loop.

### Graph projection

The semantic records form a typed graph even when they remain stored in
Parquet:

```text
Artifact
  -> contains -> SourceFragment
  -> has assignment -> Concept
  -> mentions -> Entity
  -> participates in -> Proceeding
  -> cites / amends / implements -> Artifact

Concept
  -> broader / narrower / related -> Concept
  -> exact / close / broader / narrower / related match -> Concept
  -> assigned to -> Artifact or SourceFragment

Assignment
  -> supported by -> SourceFragment
  -> derived from -> Assignment
  -> produced by -> Model, parser, or ruleset
```

The graph helps in five functional ways:

1. **Candidate generation.** A segment about one concept can consider its
   aliases, parent, children, related concepts, co-assigned concepts, entities,
   and nearby regulatory records without loading the whole registry.
2. **Retrieval.** Exact links and short typed paths answer direct questions.
   Graph neighborhoods also seed lexical and dense retrieval for material that
   lacks a direct citation.
3. **Model context.** A model receives a small relevant neighborhood with typed
   relationships instead of a flat list of hundreds of labels.
4. **Learning features.** Co-assignment counts, path types, shared entities,
   hierarchy distance, source family, and document role become inputs to
   classifiers and rankers.
5. **Error discovery.** Cycles, incompatible types, contradictory mappings,
   isolated concepts, unexpected co-assignment, and sudden graph changes expose
   ontology or extraction errors.

The graph remains a projection of the authoritative records, as required by the
[complete vision](superpowers/specs/2026-07-25-rulespec-spicy-regs-complete-vision-goal.md).
Use DuckDB joins and recursive common table expressions over published Parquet
first. The existing
[graph carrier decision](superpowers/specs/2026-07-24-graph-engine-carrier-decision.md)
defers a dedicated graph engine until a real query proves DuckDB inadequate.
A graph database would change the access method, not the semantic model.

Do not begin with graph embeddings or a graph neural network. They become useful
only after the graph contains enough accurate, diverse training edges to beat
typed traversal, ordinary graph features, and embedding baselines on held-out
data.

### Model cascade

Use the cheapest method that can perform each bounded task accurately. Reserve
frontier models for ambiguity, novelty, semantic alignment, and synthesis.

```text
source parsing and deterministic identifier extraction
  -> named-entity recognition
  -> entity linking and concept candidate retrieval
  -> small classifiers and rankers
  -> graph expansion and consistency features
  -> frontier model for unresolved or open-world cases
  -> independent structured critique
  -> new assignments, mappings, and local-concept proposals
  -> evaluation and the next immutable semantic generation
```

| Method | Best initial jobs |
| --- | --- |
| Deterministic parsers and patterns | Citations, dates, docket numbers, RINs, CFR and U.S.C. references, source structure, and exact identifiers |
| Named-entity recognition (NER) | Span detection for organizations, agencies, people, programs, places, chemicals, laws, courts, and other profile-defined entity types |
| Entity linker | Map detected spans through aliases and definitions to known entity or concept identifiers; preserve unresolved spans |
| Multi-label classifier | Propose likely concepts and an explicit zero-tag result |
| Assignment-role classifier | Distinguish primary, substantive, mention, and contextual assignments |
| Mapping classifier | Predict exact, close, broader, narrower, related, or wrong relationships between concepts |
| Sparse and dense retrieval | Produce a high-recall candidate set from text, aliases, definitions, and graph neighborhoods |
| Cross-encoder reranker | Reorder the small candidate set after reading each query-candidate pair |
| Frontier model | Resolve ambiguous references, explain difficult semantic distinctions, propose new concepts, extract complex relationships, and critique uncertain outputs |
| Calibration and drift models | Convert raw scores into useful probabilities and detect changing source or model behavior |

NER finds a mention; it does not establish identity. Entity linking resolves the
mention against aliases, definitions, source identifiers, and graph context.
The system keeps an unresolved mention when it cannot link confidently.

Classical models need not replace frontier models. They form a high-volume
student layer:

1. Frontier models generate structured provisional labels, roles, mappings,
   evidence spans, and error diagnoses.
2. Later human corrections improve those labels asynchronously.
3. Spicy Regs trains or calibrates smaller classifiers from the accumulated
   records.
4. The smaller models handle common cases.
5. Uncertain, novel, or disagreeing cases return to a frontier model.
6. Each iteration measures whether routing increased accuracy, consistency,
   coverage, latency, or cost efficiency.

Do not assume that this cascade needs a new NER or entity-linking dependency.
Start concept candidate selection with the current lexical method and the
existing Sentence Transformers adapter. Test
[GLinker](https://github.com/Knowledgator/GLinker) only if error analysis shows
that explicit mention detection or linking remains a material source of missed
concepts.

If tested, use GLinker through a small project-owned interface. Map each
registered or local concept to its `entity_id`, `label`, `aliases`,
`description`, and `entity_type` fields. Treat its spans and links as untrusted
candidates. Spicy Regs still owns exact source alignment, unresolved mentions,
stable identifiers, assignment roles, local-concept creation, evidence checks,
and publication decisions. Do not adopt GLinker's workflow graph, cache
hierarchy, or database layers.

### Package-first extension slots

A slot is one bounded computation behind a project-owned interface. It is not a
new pipeline, storage model, or source of semantic truth. A package enters the
default path only when a paired run on frozen and held-out real data improves
accuracy or removes substantial owned code without weakening source evidence.

The accepted
[document-AI package decision](superpowers/specs/2026-07-24-document-ai-package-fit-decision.md)
already fills parsing, token budgeting, embeddings, sparse encoding,
reranking, metrics, provider transport, response validation, and analytical
query slots with Docling, `tiktoken`, Sentence Transformers, `ir-measures`,
scikit-learn, official provider SDKs, `jsonschema`, and DuckDB over Parquet.
Keep those boundaries rather than selecting another framework for the same
work.

The remaining useful extension slots are:

| Slot | First established candidate | Functional use | Disposition |
| --- | --- | --- | --- |
| Legal citation recognition | [CiteURL](https://raindrum.github.io/citeurl/) for U.S.C./CFR-style citations and [eyecite](https://github.com/freelawproject/eyecite) for judicial and Federal Register volume/page citations | Extract exact spans and parsed parts from regulatory text and opinions | **Run one bounded regulatory bakeoff** with CiteURL; evaluate eyecite only for an active judicial or Federal Register volume/page need |
| Mention detection and concept/entity linking | Existing lexical and Sentence Transformers adapters; [GLinker](https://github.com/Knowledgator/GLinker) as an optional comparator | Find explicit spans and rank them against Rulespec and local concept IDs using labels, aliases, definitions, and types | **Use existing baselines first**; test GLinker only if candidate recall or span detection remains a measured problem |
| Bounded multi-label, role, and mapping classifiers | scikit-learn linear/one-versus-rest models; [SetFit](https://github.com/huggingface/setfit) only if the linear baseline underfits | Handle common closed-set assignments before routing uncertain or novel cases to a frontier model | **Use scikit-learn first** after role and mapping labels are corrected; defer fine-tuning until a held-out comparison is meaningful |
| Probability calibration and label-error diagnosis | scikit-learn `CalibratedClassifierCV` and [cleanlab](https://docs.cleanlab.ai/stable/) | Calibrate bounded classifier scores and rank questionable gold labels, assignments, roles, or entity spans | **Add after out-of-sample probabilities exist**; a warning creates a review item, never an automatic correction |
| Recurring unknown and local-concept grouping | scikit-learn [`HDBSCAN`](https://scikit-learn.org/stable/modules/generated/sklearn.cluster.HDBSCAN.html) over existing embeddings | Group repeated unresolved mentions and similar local proposals while leaving isolated cases as noise | **Use when enough unresolved examples accumulate**; clusters are candidate review bundles, not concepts |
| Structured cross-source record linkage | [Splink](https://moj-analytical-services.github.io/splink/) | Propose matches among organizations, people, or records that lack one shared identifier, using several structured fields in DuckDB | **Defer until a real cross-source identity set exists**; scores may propose links but never merge records automatically |
| Typed relation extraction | [GLiREL](https://github.com/jackboyla/GLiREL) or a later measured equivalent | Propose bounded relations between already detected entities before the existing evidence and relation checks | **Defer until entity linking and relation labels are stable**; output remains extraction candidates |
| RDF graph conformance | [pySHACL](https://github.com/RDFLib/pySHACL) | Validate an RDF projection against Rulespec-generated SHACL shapes | **Add only when that projection serves a real consumer**; Rulespec's CUE source remains authoritative, and conformance does not prove semantic correctness |

The packages do not own Artifact or SourceFragment identity, Rulespec meanings,
assignment roles, exact evidence, concept promotion, approval, run history, or
publication. Their internal IDs and object models do not enter the durable
tables. Every accepted span must still resolve to the exact locked source text.

This table does not reopen the parked document-retrieval migration. A
GLinker comparison, if needed, would cover tag extraction rather than document
indexing or serving. Chonkie, RankLLM, Kuzu, vector databases, full
retrieval-augmented-generation frameworks, experiment platforms, and workflow
engines remain behind their existing measured-need gates. Do not retain any
adapter merely because its package is capable.

### Citation parsing decision

Use a supplement-first design. Legal citation work has three separate jobs:

1. **Recognize free-text spans.** A package may propose the location, family,
   and parsed parts of a citation.
2. **Parse source-specific fields.** Existing code handles structured Federal
   Register fields, Unified Agenda variants, RINs, Federal Register document
   numbers, Regulations.gov IDs, and other source grammar that general legal
   packages do not understand.
3. **Assign stable identity.** Project-owned `canonical_*` functions validate
   the parsed parts and create Rulespec identities. Package URLs and internal
   identifiers never become durable identity or provenance.

The common seam should stay small:

```text
exact SourceFragment text
  -> CitationRecognizer.recognize(text, source profile)
  -> in-memory CitationMention(raw text, start, end, kind, components, package version)
  -> verify raw text == SourceFragment[start:end]
  -> project supported families into CfrCitation or AuthorityCitation
  -> apply the existing project-owned canonical identifier rules
  -> materialize through existing citation, assertion, and evidence structures
```

Do not coerce CFR subsection or range detail, Federal Register volume/page
references, or judicial citations into types that cannot represent them.
Keep those results in the in-memory comparison form until an active consumer
justifies a family-specific projector and durable representation.

A local exploratory comparison over all 4,777 distinct Unified Agenda
authority strings in
`output/rulespec-realworld-iteration-2/authority_edges.parquet` produced this
coverage:

| Result | Distinct strings |
| --- | ---: |
| Recognized by both the current parser and CiteURL | 4,157 |
| Recognized only by the current parser | 233 |
| Recognized only by CiteURL | 108 |
| Recognized by neither | 279 |

These counts measure detection coverage, not correctness. The current parser
recognizes more of this regulatory source, especially Executive Orders and
U.S.C. forms without a section marker. CiteURL adds useful CFR subsections,
ranges, spans, Federal Register volume/page citations, and other legal families,
but it can also interpret adjacent numbers or `3 CFR` compilation references
incorrectly. A blind union would raise both coverage and false positives.
The probe supports a bakeoff decision, but no committed command or result table
yet makes the package differential reproducible.

When judicial extraction becomes active, use
[eyecite](https://pypi.org/project/eyecite/) for that separate profile. It
understands reporter citations, pin cites, short citations, `supra`, and `Id.`
context that the current parser does not attempt. It may also supplement
Federal Register volume/page recognition. It is not a general Unified Agenda or
CFR parser, and it must not resolve a reporter citation directly to a Spicy Regs
opinion identity without a separate, evidence-backed match.

Keep [CiteURL](https://pypi.org/project/citeurl/12.0.3/) experimental until the
bakeoff proves a net gain. Its current package imports `markdown` without
declaring that dependency, so a trial must pin the package, declare the missing
dependency explicitly, and test installation as part of the gate.

The first citation bakeoff should stay regulatory:

1. Freeze the existing citation fixtures, all 4,777 distinct authority strings,
   and the real CFR-reference corpus.
2. Compare the current parser with the current parser plus CiteURL candidates.
   Run CiteURL alone only to diagnose disagreements.
3. Adjudicate a stratified disagreement sample covering each citation family,
   lists, ranges, subsections, punctuation, malformed source text, and ambiguous
   number sequences. A frontier model may perform the first review; later human
   review can correct it asynchronously.
4. Score exact span, citation family, parsed components, list/range
   multiplicity, stable target identity, false positives, unresolved results,
   and repeatability.
5. Require a material held-out accuracy gain before retiring any overlapping
   regular expression. Package results must pass project canonicalization, and
   every changed target identity must appear in the adjudicated disagreement
   set. Block unreviewed identity changes; allow reviewed corrections to wrong
   identities.
6. Save the exact command, pinned package versions, and one small result and
   disagreement table. Do not build a general experiment system.

Evaluate eyecite separately only when judicial citation extraction or Federal
Register volume/page resolution serves an active experiment. Use the available
Supreme Court opinions then. Do not build one shared multi-profile citation
benchmark runner.

One immediate correction does not depend on a package. `AuthorityCitation`
already parses `statute_at_large` and `executive_order`, but
`build_authority_edges.py` omits both fields from `authority_edges.parquet`.
Preserve that information before expanding recognition. Also fix the current
parser so a U.S.C. section list yields one citation per section, widen parsing
and canonical validation for each normalized multi-letter U.S.C. section,
apply the same CFR component checks to dictionary and text input, and describe
Regulations.gov token handling as normalization rather than proof that an
identifier exists.

### Other manual parsing decisions

The repo contains several other hand-written parsers. Most are either small
source mappings or exact project identifiers, so replacing all of them would
add more maintenance than it removes. These are source-reader hardening and
later experiment decisions, not work in the current tagging critical path:

| Current work | Established option | Decision |
| --- | --- | --- |
| HTML tag removal in `build_search_index.py` | Beautiful Soup, already a core dependency | **Reuse when this transform is next changed**, after benchmarking the 273,000-record path; a parser handles malformed and embedded markup more safely than `<[^>]+>` |
| GAO RSS parsing in `sources/gao_reports.py` | [feedparser](https://pypi.org/project/feedparser/) | **Keep the four-field ElementTree mapping for now**; use feedparser if more RSS/Atom variants, namespaces, date forms, or recurring malformed feeds appear |
| XML safety checks for external feeds | [defusedxml](https://pypi.org/project/defusedxml/) | **Defer to source-reader hardening**; use it when these feeds enter a production threat model or an unsafe-input test fails, replacing the hand-written GAO `DOCTYPE` check and protecting the streaming Unified Agenda reader while leaving source mapping custom |
| Large SAM.gov JSON extracts | [ijson](https://pypi.org/project/ijson/) with streamed HTTP and decompression | **Use only for an observed bulk-extract need or memory failure**; the current path buffers the download, decompressed text, and full JSON value before yielding records |
| CAS Registry Number checksum in `ontology/llm.py` | [python-stdnum](https://pypi.org/project/python-stdnum/) | **Keep the eight-line validator now**; use python-stdnum if the system adds more standard external identifiers |
| NAICS syntax checking | Official Census NAICS code list, pinned by edition | **Use registry data when semantic validation matters**; no generic parser can prove that a syntactically valid code exists in the intended edition |
| Unified Agenda timetable dates and other fixed source dates | Python `datetime` | **Keep the standard library**; fix the current invalid-day clamping with real calendar validation rather than adding a permissive date parser |
| Sentence-boundary selection inside long segments | [syntok](https://pypi.org/project/syntok/) as an offset-preserving comparator | **Run only as a segmentation-policy experiment**; abbreviation-aware boundaries may improve segments, but changing them also changes segment identities and the frozen baseline |
| Exact-offset HTML/XML boundaries in `ontology/adapters.py` and `docpipeline/source.py` | Existing `HTMLParser` logic and source-specific adapters | **Keep project-owned** because source offsets are part of the evidence model; a generic text cleaner would lose that alignment |
| RINs, Federal Register document numbers, Regulations.gov IDs, GovInfo granule IDs, source keys, safe file names, and media tokens | Source specifications and Rulespec identifier schemes | **Keep project-owned**; these are small identity and safety grammars, not general document parsing |
| Mirrulations attachment filenames | Current source grammar | **Keep project-owned, but sort the captured attachment ordinal numerically**; lexical order currently places attachment 10 before attachment 2 |
| Repeated RIN and CFR helper expressions | Existing shared citation and identity functions | **Consolidate internally when touched**; a third-party dependency would not improve the rule |

This audit also confirms that established packages already handle the expensive
formats: Beautiful Soup for general semantic HTML cleanup, pypdf for PDF, the
current Docling adapter for DOCX, PPTX, and XLSX, `tiktoken` for token counts,
official SDKs for provider responses, `jsonschema` for structured output, and
DuckDB/Parquet for stored data. The remaining custom code should focus on source
meaning, exact evidence, and Rulespec identity.

### Iterative semantic-learning loop

```text
registered concepts + current local concepts
  -> retrieve a small concept and entity neighborhood
  -> extract assignments, roles, mappings, entities, and evidence
  -> classify each result as exact, close, broader, narrower, related, or wrong
  -> cluster recurring errors and useful novel concepts
  -> refine aliases, definitions, examples, counterexamples, and graph edges
  -> train or recalibrate bounded classifiers
  -> build the next immutable semantic generation
  -> rerun frozen and held-out data
```

Keep four functional safeguards:

- A document-level assignment may suggest segment candidates, but each segment
  still needs local evidence.
- Outputs from one generation cannot become their own evidence or evaluation
  answers in that generation.
- Local concepts may evolve quickly; registered concepts remain stable inputs
  until a later promotion, replacement, split, or merge.
- Every claimed improvement must appear on frozen or held-out real data, not
  only on the examples that caused the change.

### Next functional experiment

Do not activate every open slot at once. Run the smallest comparisons in this
order:

1. Preserve, review, and integrate the existing uncommitted
   `TagExtractionTask`, `rulespec_testbed.py`, tests, and required ontology
   changes from the isolated `feat/rulespec-testbed-loop` worktree. Rerun its
   focused tests and provider-free rebuild of the frozen diagnostic before
   changing behavior.
2. Add Rulespec assignment roles to the tag response and score primary,
   substantive, mention, and contextual assignments separately.
3. Adjudicate the 35 gold artifact assignments, spanning 34 unique labels, as
   exact, close, broader, narrower, related, or wrong instead of relying on
   normalized-label equality.
4. Compare the current lexical selector with an embedding baseline using the
   existing Sentence Transformers adapter on the same frozen segments and fixed
   candidate depth.
5. Feed each candidate arm into the same frontier-model tag task. Measure
   candidate recall separately from final assignment and role accuracy.
6. Test GLinker only if the comparison shows that missed explicit mentions or
   concept links still limit candidate recall. Export the fixed Rulespec and
   local registry without changing concept IDs or labels, and remove the
   experiment if it does not beat the simpler baselines.
7. Add a scikit-learn classifier and calibration arm only after the corrected
   labels can support a leakage-free artifact-level split.
8. Use HDBSCAN or cleanlab only after the run has enough recurring unknowns or
   out-of-sample predictions to make their results meaningful.
9. Add a graph-derived concept neighborhood only if error review shows that a
   flat candidate set is missing useful hierarchy or relationship context.
10. Route uncertain or novel cases to a frontier model, store its structured
   diagnosis, and confirm any gain on held-out documents.

Candidate evaluation must distinguish two cases. For an assignment with an
adequate registered target, score candidate Recall@K and final identity. When
the 901-concept gold-free registry has no adequate target, score correct
abstention and useful local-concept creation instead. Only `medicaid` had a
natural exact-label match among the 35 gold assignments in the completed
diagnostic; treating every other case as a retrieval failure would recreate the
old gold-label leakage.

The citation bakeoff above, Splink, GLiREL, and pySHACL belong to separate
profile or capability evaluations. They are not dependencies of this tagging
experiment.

Report concept accuracy by semantic relation and assignment role, NER span
precision and recall by entity type, entity-linking accuracy, retrieval
Recall@K and ranking quality, routing coverage, abstention, and frontier-model
lift over the cheaper baselines. Stop expanding this design if those structures
do not improve held-out accuracy or reduce repeated frontier-model work.

## Execution result

The path completed on 2026-07-26 in the isolated
`feat/rulespec-testbed-loop` worktree:

This result is a validated local prototype, not a capability on the current
branch. The isolated worktree still contains uncommitted `tag_task.py`,
`rulespec_testbed.py`, their tests, and related ontology changes. Preserve and
integrate that work before starting the next experiment.

- the new pipeline processed all 44 selected artifacts and 109 segments;
- a real `gpt-5.6-sol` run stored 109 successful calls with exact evidence;
- a provider-free rebuild applied two review-required scoring corrections
  without changing the original calls;
- one prompt-only refinement reduced accepted candidates from 351 to 76 and
  counted false positives from 260 to 55;
- precision rose from `0.0335` to `0.0678`, recall fell from `0.2571` to
  `0.1143`, and F1 rose from `0.0592` to `0.0851`;
- evidence grounding remained `1.0`;
- both runs remained diagnostic-only and passed integrity and provider-free
  recomputation.

`RULESPEC_FEEDBACK_ITERATION_2.md` records the important result: exact-label
scoring confuses semantically compatible local concepts with errors, and the
single-label gold cannot distinguish primary topics from useful substantive
and mention tags. Rulespec already represents both distinctions; the next
small loop belongs in Spicy Regs evaluation and task output, not new Rulespec
vocabulary.

## Verified starting point

The useful loop is not new:

- `spicy_regs.corpora` already builds the mixed real-data evaluation set, runs
  segmentation and LLM tagging, stores assignments and validations, and
  computes metrics (`pyproject.toml:95-103`).
- The selected stored reference is
  `output/segmentation-tagging-document-openai-structure-overlap-1800-v4`.
  It uses 10 document profiles, 44 selected artifacts, 35 gold artifacts, 9
  controls, 109 selected segments, and the `structure-overlap-1800`
  configuration.
- That historical run reported precision, recall, and F1 of `0.8857`,
  validation agreement of `0.96`, and evidence grounding of `1.0`. It is not
  an honest hidden-gold comparator: the old runner added the curated gold
  labels to its effective concept registry before model execution. The new
  gold-free diagnostic is the baseline for its own refinement.
- The new source and segmentation implementation already selects
  `structure-overlap-1800` and preserves exact source slices
  (`src/spicy_regs/docpipeline/segments.py:1-56`).
- The new extraction code already supplies a provider-neutral task interface,
  strict response checks, stored requests and responses, candidate and
  rejection tables, scoring, and provider-free recomputation
  (`src/spicy_regs/docpipeline/extraction.py:1-24`,
  `src/spicy_regs/docpipeline/extraction.py:167-229`).
- On the current branch, the extraction layer has a relationship task but no
  tag task. Its interface anticipates one
  (`src/spicy_regs/docpipeline/extraction.py:16-20`); the isolated worktree
  contains the uncommitted `TagExtractionTask` that must be integrated.
- The document-retrieval stage is now committed at `9591c6d`, including dense,
  learned-sparse, reciprocal-rank fusion, reranking, and `ir-measures`
  evaluation. It remains parked for this tag loop because tagging the selected
  benchmark segments does not require document retrieval.
- The extraction and runtime changes are also committed at `9591c6d`. In the
  current `feat/document-ai-pipeline` worktree, this document is the only local
  change; the separate testbed worktree remains dirty as described above.
- The sibling Rulespec worktree currently has local changes. This plan reads
  from an exact clean Rulespec revision when it checks shared semantics and
  does not modify that worktree.

The mistake was treating the existing experiment as a legacy system that the
new code had to reproduce internally. It is only a benchmark: its real
documents, gold labels, and metrics matter; its run IDs, manifests, storage
layout, and intermediate tables do not.

## What “working” means

A working testbed completes one full accuracy iteration:

1. Run the selected 44 artifacts, including 35 gold artifacts and 9 controls,
   through the new source and segmentation code.
2. Generate tag candidates through the new extraction interface and a real
   model.
3. Store the exact request, response, model identity, evidence offsets,
   candidates, rejections, and metrics.
4. Score final tags against the existing hidden gold labels without including
   those labels in model input.
5. Review the false positives, false negatives, novel tags, and weak document
   profiles directly.
6. Classify each important error as one of:
   - source parsing or segmentation;
   - prompt or model behavior;
   - Spicy Regs profile or local taxonomy;
   - Rulespec vocabulary, constraint, or evidence-model problem;
   - incorrect or ambiguous gold label.
7. Make one focused correction, rerun the same sample, and show whether the
   reviewed errors decreased without losing evidence grounding.
8. Record the Rulespec-relevant findings in one short dated feedback report.

The first diagnostic run does not need to beat the old score. It must produce
usable tags, honest metrics, and inspectable errors. The first refinement is
successful when it reduces the adjudicated error set or demonstrates, with
source evidence, that a Rulespec or gold-label assumption is wrong.

## Smallest implementation

### 1. Isolate the current unfinished work

Leave the current dirty Step 5 files untouched. Execute this path in a separate
worktree or branch after classifying the diffs in `extraction.py` and
`runtime.py` against `HEAD`:

- keep any independently useful extraction/runtime correction required by the
  tag task;
- leave retrieval-only changes and the untracked retrieval files behind;
- do not bring migration tests, legacy identities, or historical storage
  support into the new branch.

Do not delete or reformat the unfinished retrieval migration as part of this
work. The stored v4 tagging outputs are the comparison artifact; rerunning the
old tagging runner is not part of this path.

### 2. Feed the benchmark into the new source and segment steps

Add only the evaluation-side reader needed to read the stored evaluation
dataset, select the existing 44 artifacts, and load the 35 gold artifacts'
labels. Do not rebuild the corpus for the first iteration.

It must not teach production code to understand old run IDs, old manifests, or
old table layouts. Production inputs remain `SourceRecord` and
`SourceArtifact`; benchmark translation stays in the evaluation command or
test support.

Map the stored field-coordinate gold spans onto the new `SourceArtifact`
coordinates as an explicit evaluation task. Preserve valid original offsets.
When offsets do not transfer, reuse the existing unique-exact-match resolution
behavior from `spicy_regs.ontology.llm.resolve_exact_evidence_offsets`. An
ambiguous or missing match is a reported benchmark-input failure, not a guessed
coordinate.

Use the selected new segmentation settings without running another segmentation
bakeoff. Validate:

- all selected artifacts reach a terminal source outcome;
- every evidence slice resolves exactly to source text;
- every gold span is either present in a segment or reported as a concrete
  source/segmentation failure;
- hidden gold fields do not enter model input.

Keep the current heading-region and `markup-prolog` segmentation behavior
through this first iteration. Although the code describes it as migration
parity, it now serves baseline stability: changing it would move the frozen
segment boundaries before the tag task has a comparable result.

### 3. Implement one tag extraction task

Implement a `TagExtractionTask` behind the existing `ExtractionTask` interface.
Import the current `TAG_INSTRUCTIONS` and
`resolve_exact_evidence_offsets` behavior directly from
`spicy_regs.ontology.llm`. Expose its existing private `_TAG_SCHEMA` as the
public `TAG_SCHEMA` needed by both paths. Reuse the existing tag normalization
and implement ungrounded model output as rejection rows. Do not invent a second
tag schema merely to fit the new runtime.

The task needs only:

- a gold-free payload containing the segment text, source identity, profile,
  allowed concept registry, and exact evidence coordinates;
- a strict tag-candidate response schema;
- response and evidence-grounding checks;
- candidate and rejection rows;
- final assignment aggregation at segment and document scope;
- benchmark scoring against separately supplied answers.

Use the existing typed model adapter and extraction storage. Do not add
provider-specific behavior to the task.

Run both the initial pass and the refinement in `diagnostic` mode with answers
supplied separately for scoring. Implement only the non-authorizing
`review_gate` result required by the task interface. Do not invoke benchmark
mode or its sealed human-review protocol; benchmark eligibility is deferred.

Automated approval, comparison, graph materialization, and a generalized task
registry are not required for this iteration. The outputs remain experimental
candidates reviewed through the benchmark.

### 4. Run the diagnostic baseline

Run the new tag task once in `diagnostic` mode on the same selected sample and
save it under a new run directory. Compare only outcomes that answer an
accuracy or safety question:

- precision, recall, and F1 overall and by profile;
- false-positive and false-negative cases;
- evidence-grounding rate;
- empty-tag rate;
- novel-tag rate;
- prompt-injection behavior;
- provider failures and rejected responses;
- model and prompt identity.

Do not compare byte layout, run identity, receipt shape, checkpoint layout,
provider-call accounting details, or intermediate table equality with the old
runner.

### 5. Perform one accuracy iteration

Start with the smallest high-signal error cluster. The first new diagnostic
showed broad over-tagging: 351 accepted candidates produced 260 counted false
positives on the 35 gold artifacts. That breadth, not the old runner's profile
metrics, is the first review set.

Read the source excerpts and model responses before choosing a fix. Change one
of the following at a time:

- prompt instruction or example;
- source/profile field selection;
- segment context;
- local concept label, definition, alias, or merge;
- Rulespec term or constraint, only when the correct real-world meaning cannot
  be represented cleanly.

Rerun the same selected sample. Stored provider output may be reused only for
unchanged requests; changed prompts or inputs require fresh calls.

### 6. Record the learning

Create one concise `RULESPEC_FEEDBACK_ITERATION_2.md` after the rerun. For each
Rulespec-relevant finding, include:

- the real source and exact excerpt;
- the expected meaning;
- the model or carrier result;
- why the issue belongs in Rulespec rather than the prompt, parser, profile, or
  local taxonomy;
- the smallest proposed Rulespec change;
- the before/after result when a candidate correction was tested.

Do not create a difference ledger, evidence-hash system, migration receipt, or
new schema for this report.

## Acceptance gates

The path is complete when all of these are true:

- The selected real-data benchmark runs through new source, segmentation, and
  tag extraction code without using the old runner for execution.
- A real model produces stored candidate and rejection outputs.
- Gold labels remain isolated from model requests.
- Every accepted tag points to exact source evidence; missing or ambiguous
  evidence becomes a rejection, not a guessed assignment.
- Overall and per-profile metrics recompute from the stored run.
- A maintainer can inspect every false positive and false negative without
  reconstructing hidden pipeline state.
- One focused correction is rerun on the same sample and its accuracy effect is
  reported.
- Rulespec findings are separated from Spicy Regs, model, source, and gold-label
  problems.
- Focused tests, the new diagnostic run, and the short feedback report are
  green and internally consistent.

These are not gates:

- exact parity with old manifests or intermediate tables;
- a full historical replay;
- provider-free recreation of old experiments;
- retrieval quality;
- automated approval or publication;
- formal Rulespec release metadata;
- MCP behavior;
- full-corpus model tagging.

## Scope disposition

**Use now**

- new source parsing and `structure-overlap-1800` segmentation;
- new typed model adapters;
- new extraction task interface and stored provider outputs;
- existing mixed real-data sample, gold labels, and baseline metrics;
- exact evidence grounding and answer isolation;
- simple metrics and direct error review.

**Park**

- unfinished Step 5 retrieval code and migration tests;
- automated approval, comparison, and materialization;
- full-corpus tagging;
- MCP and publication;
- formal Rulespec release work;
- provider-free rebuild except when a concrete scoring correction must be
  applied to already stored provider output.

**Remove later, after the new loop works once**

- migration-only fixtures and expected-difference files;
- legacy identity and storage compatibility;
- exhaustive intermediate-equivalence checks;
- any runtime accounting or validation machinery that exists only to prove
  compatibility with the one-day-old `corpora` runner.

No deletion is authorized by this document.

## Timebox and stop rules

Target one focused day for the new tag task and first diagnostic run, then one
short iteration on the highest-value errors. If the work starts requiring
retrieval, new storage formats, migration identities, or a generalized
workflow engine, stop: the scope has drifted.

If the tag task cannot directly import the existing prompt, schema, and offset
resolution, stop and identify the exact coupling. Do not respond by copying
the experiment framework or creating a generalized shared library.

If the new run is less accurate, keep the result. Use its concrete errors to
fix the new path. The testbed exists to make those failures visible, not to
protect a predetermined architecture.

## Independent validation

On 2026-07-26, `claude-fable-5` reviewed this document and the directly
relevant source, segmentation, extraction, runtime, tagging, and stored
baseline files in a read-only, budget-capped call.

**Verdict: APPROVE.** The reviewer found the core route to be the fastest safe
path and required three clarifications before implementation:

1. triage the dirty `extraction.py` and `runtime.py` diffs before branching;
2. use diagnostic mode so the sealed benchmark-review machinery cannot block
   the learning run;
3. read the stored evaluation dataset directly and make gold-span coordinate
   translation explicit.

This revision incorporates all three. It also adopts the reviewer's smaller
recommendations to import the existing tag behavior directly, use the stored
v4 outputs rather than preserve the old runner as a fallback, and keep current
segment boundaries for baseline stability through the first iteration.
