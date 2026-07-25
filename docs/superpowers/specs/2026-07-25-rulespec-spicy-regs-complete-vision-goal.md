# Rulespec and Spicy Regs Complete Vision and Execution Goal

- **Date:** 2026-07-25
- **Status:** Canonical program vision and executable goal
- **Scope:** Rulespec, Spicy Regs, and their versioned interface
- **Supersedes as parent vision:** `2026-07-23-regulatory-ontology-program-overview.md`
- **Preserves as detailed decisions:** the application profile, segmentation,
  relationship-assertion, comparison, omission, provider, and evaluation
  specifications linked below
- **Design posture:** Greenfield, quality-first, package-first, and
  dependency-inverted

## Agent goal

Build Rulespec and Spicy Regs into one evidence-preserving knowledge system for
heterogeneous legal, regulatory, administrative, judicial, legislative,
scientific, contractual, procurement, oversight, participation, organization,
and spending records.

Rulespec defines the small reusable semantic contracts. Spicy Regs ingests real
sources, preserves their distinct grains, extracts and validates metadata,
builds retrieval and graph projections, and proves the contracts against
versioned corpora.

The system must let a user start with any supported document, segment, entity,
identifier, concept, or question and:

1. find directly linked, deterministically connected, semantically related, and
   carefully inferred material;
2. filter results by document type, source, version, authority, jurisdiction,
   time, concept, entity, proceeding, legal status, and access scope;
3. inspect the exact source evidence, graph path, model or ruleset lineage,
   confidence, attestation, and comparison proof behind every derived result;
4. compare document states without collapsing revision, denial, removal,
   omission, and silence into one negative relation;
5. tag documents and meaningful document segments while preserving the
   direction of evidence and avoiding self-reinforcing circular inference; and
6. improve retrieval metadata over time without rewriting history or promoting
   model output into decision-grade truth without review.

Change either repository when its current contract obstructs this goal. No
external consumer depends on the present pre-1.0 Rulespec design, so correctness
outranks backward compatibility. Do not preserve a known semantic defect through
an adapter, parallel vocabulary, deprecated field, or permanent Spicy Regs
workaround.

Do not release, publish, push, promote a shared concept, enable an inferred
legal effect, or claim independent review without explicit maintainer
authorization. Local implementation, generation, testing, corpus evaluation,
and paired migration preparation remain in scope.

## Product vision

Legal and regulatory information rarely lives in one document or one source.
A rule may begin in an agenda entry, appear in several dockets and publication
systems, rely on statutory authority, receive comments, change across drafts,
face litigation, and later be amended, stayed, vacated, or replaced. Related
reports, filings, contracts, scientific evidence, and oversight records may
never cite each other.

The product must make these relationships visible without pretending that
similarity is fact, that a shared identifier means shared identity, or that
silence proves exclusion.

The durable outcome is a version-aware, evidence-preserving metadata and
relationship compiler:

```text
real source records
  -> immutable source artifacts and meaningful source regions
  -> typed metadata, concepts, entities, and relationship candidates
  -> evidence, provenance, confidence, and scoped attestations
  -> deterministic accepted projections
  -> direct, deterministic, semantic, and inferred retrieval
  -> neutral comparison findings
  -> optional domain interpretation
  -> inspectable user results
```

The graph is a projection of these records, not a replacement for them.
Parquet remains Spicy Regs' primary analytical carrier. JSON-LD, SQL, MCP, and
graph indexes expose the same semantics through different access paths.

## Success criteria

The program succeeds when:

- a user can discover relevant material across all supported corpus families,
  including material with no direct citation;
- every result identifies the exact document state and source region used;
- every derived tag, value, edge, comparison, and interpretation exposes how
  the system produced and accepted it;
- documents and source segments receive distinct but connected concept
  assignments;
- the system distinguishes source statements, analytical findings, consumer
  judgments, and legal interpretations;
- corpus updates append new states and assertions without deleting history;
- model, prompt, parser, embedding, reranker, and ruleset changes can be
  evaluated against the same frozen data;
- missing inputs, incomplete processing, uncertain identity, unresolved scope,
  and failed providers produce explicit `unknown`, quarantine, or failed-run
  records instead of silent partial publication; and
- a new document family requires a source/profile adapter and tests, not a new
  ontology or tagging pipeline.

## Architectural commitments

### One canonical semantic source

Rulespec CUE is authoritative for Rulespec-owned structural constraints.
Rulespec generates JSON Schema, Rust, TypeScript, SHACL, and other supported
targets from that source. Code generation must support composition so a
projector limitation never forces duplicated or divergent ontology shapes.

Spicy Regs pins a reachable Rulespec version and immutable contract digest. It
may use specialized Parquet tables, but every durable semantic field must have
one documented outcome:

1. a Rulespec projection;
2. a composition with a public standard recognized by the profile; or
3. a named Spicy Regs extension with a reason and promotion criteria.

### Small core, explicit modules, domain profiles

Rulespec must not place U.S. regulatory identifiers, proceedings, rulemaking
stages, judicial effects, or other domain-specific terms in the universal
kernel.

The target module boundaries are:

```text
Rulespec
├── kernel
│   ├── Artifact and source identity
│   ├── SourceFragment and structural containment
│   ├── AssertionEnvelope
│   ├── RelationshipAssertion and ValueAssertion
│   ├── EvidenceBinding
│   ├── provenance and derivation lineage
│   ├── ConfidenceRecord
│   ├── Attestation
│   ├── ApplicabilityScope and time
│   └── AccessScope
├── concepts
│   ├── ConceptScheme
│   ├── LocalConcept and RegisteredConcept
│   ├── ConceptAssignment
│   ├── ConceptMapping
│   └── promotion and replacement records
├── document-analysis
│   ├── RelationChangeEvent
│   ├── RelationComparisonContext
│   ├── ResolverProofRecord
│   ├── neutral RelationFinding
│   └── experimental ClosureClaim
├── runtime-and-conformance
│   ├── generated validators
│   ├── behavior contracts
│   └── versioned conformance receipts
└── profiles
    ├── US regulatory and rulemaking
    ├── legislative
    ├── judicial
    ├── contractual and procurement
    ├── scientific and evidence
    └── future document families
```

The modules may use different physical directories if the names above conflict
with established build conventions. Their dependency direction must remain
clear: profiles depend on reusable contracts; the kernel never depends on a
profile.

### Dependency inversion

High-level orchestration and semantic policy depend on project-owned protocols.
OpenAI, Codex CLI, Sentence Transformers, CrossEncoder implementations, parser
packages, tokenizers, databases, graph engines, and storage clients implement
those protocols.

Provider capabilities and provenance remain visible in receipts. Provider
types, response shapes, SDK objects, billing records, and configuration never
define core artifact, segment, concept, evidence, assertion, or finding
identity.

### Package-first implementation

Before writing substantial infrastructure, evaluate maintained packages that
cover roughly 80 percent of the needed behavior. Prefer a thin owned adapter
plus project-specific invariants when the package's fit, maintenance, license,
extensibility, performance, and failure behavior are acceptable.

Apply this rule to parsing, segmentation, tokenization, embedding, reranking,
caching, validation, model serving, evaluation, and graph projection.

Reuse public semantic standards where they own the meaning:

- Dublin Core and PROV-O for version, revision, format, and derivation
  relationships;
- Web Annotation selectors for exact source regions;
- SKOS for concept schemes, labels, hierarchy, and mappings;
- ELI for legal-resource and legal-expression identity where applicable;
- Akoma Ntoso and USLM for native legal-document structure;
- ODRL or LegalRuleML inside profiles that need deontic semantics; and
- OWL-Time or equivalent time primitives when profiles need richer temporal
  relationships.

Do not add a framework such as Chonkie, Haystack, Qdrant, GraphRAG, or LinkML
without a measured gap that the current adapters cannot meet. Do not create a
Rulespec-specific constraint language while CUE satisfies the contract.

## Semantic model

### Identity and versioning

Rulespec `Artifact` represents one immutable, addressable source state: an
edition, publication, snapshot, content payload, or source posting.

Separate these identities:

| Object | Meaning |
| --- | --- |
| Stable resource | The work, registry object, or durable real-world subject across versions; owned by a public or profile vocabulary |
| Artifact | One immutable document or record state |
| SourceFragment | One addressable region within an Artifact |
| Semantic entity | A person, organization, proceeding, docket, place, authority, program, regulated entity, or other non-document thing |
| Processing segment | A bounded model input derived from source structure; operational unless it corresponds to a stable SourceFragment |

Compose:

- `dcterms:isVersionOf` from an Artifact to a stable resource;
- `prov:wasRevisionOf` from a later Artifact to an exact earlier Artifact; and
- `dcterms:isFormatOf` and `dcterms:hasFormat` between substantially identical
  content in different formats or source postings.

Do not infer lineage from a shared title, topic, RIN, identifier fragment,
embedding score, or retrieval rank. Do not invent a universal Rulespec
`DocumentWork`, `Expression`, `Manifestation`, or `Item` hierarchy. Profiles may
compose ELI, BIBFRAME, Schema.org, or another public model.

Any Artifact used as comparison evidence must resolve to an immutable source
state and content digest. Publication, observation, applicability, effective,
recorded, extraction, and attestation times remain separate axes.

### Document structure and processing segments

Parse source structure before applying token limits. Preserve headings,
sections, clauses, paragraphs, list items, tables, captions, footnotes, and
native identifiers when the source exposes them.

A stable, meaningful region is a `SourceFragment` and may receive durable
metadata or concept assignments. A temporary processing segment may combine or
overlap source regions to fit a model context window. It may produce proposals,
but every accepted assertion must resolve back to the actual source fragments
that support it.

Processing records must preserve:

- Artifact identifier and content digest;
- source-region identifiers and hierarchy;
- source and segment character offsets;
- exact quoted text with prefix or suffix anchors when needed;
- segment policy, tokenizer, token count, and segment digest;
- truncation and omitted-region records;
- processing status, including successful zero-result work;
- attempt, retry, checkpoint, and resume lineage; and
- non-evidentiary context separately from evidence.

Regex is appropriate for deterministic headings, identifiers, citations,
normalization, and source-specific structural cues. Regex must not decide
semantic scope, concept equivalence, deontic force, claimant intent, or legal
effect.

### Assertions and typed metadata

The generic assertion model separates immutable proposition content from
evidence, generation lineage, confidence, social judgment, and consumer state.

Use a shared `AssertionEnvelope` with two primary forms:

| Assertion | Object |
| --- | --- |
| `RelationshipAssertion` | IRI for an entity, concept, Artifact, SourceFragment, or other semantic resource |
| `ValueAssertion` | Typed literal such as text, language-tagged label, enum, date, time, integer, decimal, or boolean |

Each assertion has:

- a stable, content-addressed identifier;
- subject, predicate, object, and affirmed or denied polarity;
- construction origin, such as human extraction, model extraction,
  deterministic derivation, or import;
- optional source claimant or issuer, distinct from the extraction system;
- applicability and temporal references when needed;
- links to evidence, provenance, warrants, confidence, and attestations; and
- supersession links that append history instead of rewriting it.

Expected and observed are comparison roles, not assertion modes. Approval,
rejection, dispute, qualification, revocation, and consumer eligibility do not
belong in immutable proposition content.

### Evidence, provenance, confidence, and attestation

Keep four records separate:

1. `EvidenceBinding` says which exact source regions support or challenge an
   assertion.
2. Derivation lineage says which parser, ruleset, human, model, prompt, schema,
   inputs, and run produced it.
3. `ConfidenceRecord` says who measured confidence, by which method, against
   which basis and calibration corpus.
4. `Attestation` says who accepted, rejected, qualified, disputed, reviewed, or
   revoked an assertion for a declared consumer, scope, policy, and time.

Document-derived, decision-grade assertions require exact source evidence.
Generic axioms or consensus records may use a separately governed no-evidence
path; they must not make an extracted document relation look sourced.

AI lineage must not require a human approver. The system must represent an
unreviewed model candidate honestly. Human approval belongs in a separate
Attestation.

Schema descriptions and LLM hints may improve human understanding, but they do
not substitute for prompt instructions, examples, counterexamples, validation,
or evaluation. Persist a secret-free digest of the complete request contract:
instructions, schema, model configuration, and input payload.

## Concepts, tags, and categories

### Concept vocabulary

Use SKOS-backed concepts:

- `LocalConcept` for workspace-owned, provisional, or retrieval-grade meaning;
- `RegisteredConcept` for reviewed, shared, decision-grade meaning;
- `ConceptScheme` for a facet or controlled category system;
- `skos:prefLabel`, `skos:altLabel`, and `skos:definition` for meaning;
- `skos:broader`, `skos:narrower`, and `skos:related` for structure; and
- explicit SKOS mappings for external thesauri or registered concepts.

Keep facets explicit. Topic, industry, regulated entity, affected population,
legal authority, place, organization, document role, obligation, outcome, and
legal status must not merge because their labels resemble one another.

Authoritative structured values remain typed profile properties rather than
fuzzy tags. For example:

- `PFAS` may be a topic concept;
- `manufacturing` may belong to an industry scheme;
- `EPA` is an organization relationship;
- `final rule` is a regulatory profile state;
- `2026-07-25` is a typed date value; and
- a U.S.C. citation is a normalized authority or citation relationship.

### Document and segment assignments

Both Artifacts and meaningful SourceFragments are taggable subjects.

A segment assignment means that the exact section, clause, paragraph, list
item, or table supports a concept. A document assignment means that the
document as a whole is materially associated with the concept.

Every `ConceptAssignment` records:

- subject IRI and subject type;
- concept and concept scheme;
- assignment role, such as primary, substantive, mention, or contextual;
- direct or derived origin;
- exact evidence for direct assignments;
- supporting assignment identifiers for aggregated assignments;
- confidence, generation lineage, and attestation;
- profile and aggregation-policy versions; and
- superseded assignment, when any.

The feedback loop is directional:

```text
accepted segment assignments
  -> policy-bound document aggregation
  -> document assignment with supporting-assignment proof
  -> candidate context for other segment passes
  -> fresh segment proposals
  -> local evidence requirement
  -> accepted new segment assignments
```

Segment evidence may support a document tag. A document tag may shortlist
candidate concepts for a segment, but it cannot prove the segment tag. The
segment still needs local evidence. This rule prevents one mistaken document
tag from spreading across every segment and confirming itself.

Inherited document context remains non-evidentiary context unless the segment
contains its own support. A zero-tag segment cannot remove another segment's
assignment or the document's supported tag. Missing tags remain unknown.

### Concept evolution and promotion

Concept and assignment history is append-only. Preserve merge, split, rename,
deprecate, replace, reject, and promote events. Keep hierarchy and replacement
graphs acyclic.

The normal path is:

```text
retrieval candidate
  -> LocalConcept
  -> evidence-backed assignments
  -> measured usefulness and quality
  -> human-reviewed promotion packet
  -> RegisteredConcept
```

Promotion is rare. It requires a definition, scope, examples,
counterexamples, mappings, usage evidence, conflicts, lineage, steward, human
approver, and rationale. Query popularity and clicks may guide review but never
establish truth.

## Relationships, changes, and absence

### Keep distinct semantic cases distinct

Do not create a generic negative tag. Represent each evidence situation
according to its meaning:

| Evidence situation | Representation |
| --- | --- |
| A source explicitly affirms a relation | Affirmed `RelationshipAssertion` |
| A source explicitly denies a relation | Denied `RelationshipAssertion` |
| Accepted assertions disagree | Neutral discrepancy finding |
| A later version adds, removes, replaces, suspends, or modifies a relation | `RelationChangeEvent` |
| A closed later observation lacks an expected relation | Neutral omission finding plus `ClosureClaim` |
| Evidence, identity, scope, lineage, pairing, or closure is incomplete | `unknown`, `not_comparable`, or `evidence_insufficient` |
| A rule gives a change legal or policy effect | Profile-owned domain interpretation |

Polarity answers only whether the source affirms or denies the canonical
predicate. It does not encode the claimant, attribution, conditionality,
consumer acceptance, lifecycle state, deontic force, or legal effect.

### Comparison kernel

The deterministic comparison kernel depends on narrow, project-owned
protocols:

- `PredicateCatalog`;
- `AssertionStateResolver`;
- `EvidenceResolver`;
- `BaselineResolver`;
- `PairingResolver`;
- `ScopeComparator`;
- `VersionLineageResolver`;
- `ExpectedCoverageResolver`;
- `ClosureResolver`; and
- an optional profile-owned `NormEvaluator` after generic comparison.

Every resolver returns `pass`, `fail`, or `unknown` with a rationale and
resolvable proof records. `Fail` is a gate result, not a negative source fact.
`Unknown` never becomes `fail`.

A `RelationComparisonContext` binds the artifact pair, exact versions,
consumer, scope, evaluation time, detector and profile versions, source
snapshot, and proof records. A neutral `RelationFinding` binds that context and
all accepted assertions used.

### Bounded omission

A `ClosureClaim` says that a named process completely enumerated a bounded
relation set in a specific source region, Artifact version, scope, profile, and
run. Closure is local and revocable. It is never a Boolean property of an
entire document or corpus.

Keep omission disabled until a frozen real dataset measures closure precision
and recall independently from extraction. Silence remains unknown outside a
proven boundary.

### Domain interpretation

The generic layer emits neutral findings. A regulatory profile may interpret a
qualified finding as policy exclusion, rescission, suspension, or another
domain result only after separate authority, applicability, deontic, source,
and closure rules pass.

Scientific, judicial, legislative, contractual, procurement, and oversight
profiles may interpret the same generic finding differently or not at all.

## U.S. regulatory profile

The U.S. regulatory module is the first profile, not the universal ontology.

Preserve these distinctions:

- a RIN identifies one durable `RegulatoryAgendaItem`;
- a Unified Agenda edition row is an immutable
  `RegulatoryAgendaObservation` whose primary topic is the agenda item;
- a `Proceeding` is one independently evidenced regulatory action;
- a `Docket` is a mutable administrative container;
- an `Artifact` is one immutable source posting or document state;
- a `CommentPeriod` is one evidenced continuous interval;
- agenda-item-to-Proceeding links are qualified, action-specific, and
  provenance-bearing; and
- CFR, U.S.C., Public Law, Federal Register, regulations.gov, and other
  identifiers retain their distinct identity roles.

RIN equality never merges Proceedings, Dockets, or Artifacts. It never fans out
agenda stage, authority, target, or legal-effect facts. Missing stage evidence
means unknown.

Move U.S. identifier grammars, rulemaking shapes, proceeding lifecycle values,
and judicial or congressional effects out of the Rulespec universal kernel and
into the profile.

## Retrieval and lookup

### Four lookup classes

Evaluate and expose four lookup classes separately:

| Class | Purpose | Required explanation |
| --- | --- | --- |
| Direct | Exact identifiers, citations, links, containment, and source references | matched value, source Artifact and version, evidence |
| Deterministic | Typed joins and validated graph paths | path, direction, ruleset, snapshot, exclusions |
| Semantic | Related material without a direct citation | lexical and dense candidates, fusion, rerank, spans, scores |
| Inferred | Implicit or multi-document relationship | inputs, plan or graph path, model lineage, validation, attestation |

Apply identity, version, authority, jurisdiction, applicability, access, and
time filters before ranking. Semantic similarity never upgrades itself into a
source assertion. Inferred relationships remain candidates until evidence and
policy resolvers accept them.

### Retrieval stack

Use the existing 80/20 stack until measured limits justify a replacement:

- DuckDB for exact lookup, joins, analytical indexes, and receipts;
- source-aware structural segmentation;
- pinned `tiktoken` accounting for OpenAI prompt and segment budgets;
- lexical retrieval;
- incumbent BGE Sentence Transformer embeddings;
- reciprocal-rank or another documented fusion method;
- an injected CrossEncoder reranker; and
- OpenAI or Codex CLI structured-output adapters for bounded reasoning tasks.

MLX and oMLX are outside the active baseline. Retain their existing adapters and
historical evidence, but do not spend the next comparison budget on them unless
a measured accuracy, privacy, latency, or deployment requirement reopens the
question.

Use an LLM when a request needs ambiguous reference extraction, semantic
alignment, query classification, multi-document planning, or synthesis over
accepted evidence. Deterministic code still validates identity, source spans,
versions, scope, chronology, access, comparison gates, and receipts.

Retrieval receipts must record:

- query text, classification, requested time, and active profile;
- pre-ranking filters;
- each retrieval leg's candidates, ranks, and scores;
- fusion and reranking configuration;
- returned source spans and Artifact digests;
- graph paths or generated subqueries;
- excluded candidates and exclusion reasons;
- model and prompt lineage for inferred work; and
- completeness or closure claims, when any.

### Document and segment feedback in retrieval

Document tags may guide segment candidate generation and document-level
retrieval. Segment tags may improve document aggregation, focused retrieval,
and evidence display.

Keep three indexes available:

1. Artifact-level identity, metadata, concepts, and relationships;
2. SourceFragment-level text, structure, concepts, and evidence; and
3. graph edges connecting Artifacts, fragments, entities, concepts,
   assertions, proceedings, versions, and provenance.

Measure whether query classes need Artifact-first, segment-first, or combined
retrieval. Do not assume one grain wins every task.

## Model and provider boundary

An LLM may:

- propose structured value, concept, relationship, or change candidates;
- identify exact evidence spans;
- normalize ambiguous references subject to deterministic checks;
- classify query intent;
- propose multi-document lookup plans;
- challenge a provisional human baseline; and
- summarize accepted evidence.

An LLM may not:

- approve its own assertion;
- establish document identity or version pairing;
- claim closure;
- decide legal authority or effect;
- create the final neutral comparison finding;
- convert missing data into a denied assertion; or
- replace deterministic receipt validation.

The OpenAI API and Codex CLI are peer adapters behind the same project-owned
structured-output contract. Use one active prompt style per fair evaluation.
Do not tune repeatedly against the same diagnostic oracle.

The bare Codex CLI adapter must disable optional skills, MCP, tools, plugins,
apps, browser access, memory, hooks, and multi-agent behavior; run in an
ephemeral read-only environment; and reject non-message events. Codex still
adds runtime context that the adapter cannot remove. Treat Codex CLI and the
direct API as separate provider arms, not equivalent repetitions.

Use the lean strict output schema without descriptions for the active fair
comparison. Keep definitions, examples, counterexamples, and reasoning
instructions in the versioned prompt contract. Retain older described schemas
only to revalidate historical receipts.

Every real provider run must persist a strict, secret-free receipt with:

- provider, model, model version or resolved alias, and response identifier;
- complete instruction, schema, and payload digests;
- decoding parameters and seed when supported;
- input Artifact and segment digests;
- raw response and normalized output digests;
- validation results, retries, failures, latency, and token use; and
- code commit, profile, ruleset, and dataset generation.

Never claim an OpenAI-backed result from deterministic or replayed output. A
reconstructed receipt must say that the provider was not invoked.

## Data and evaluation

### Quality objective

Optimize for the best data and the most trustworthy result. Financial cost is
not an optimization target. Cost, latency, and token use remain recorded so
the runs are reproducible and operationally intelligible.

### Frozen evaluation corpus

Maintain an immutable, versioned corpus containing real:

- regulations.gov dockets, documents, and public submissions when the
  evaluation purpose permits them;
- Federal Register publications;
- Unified Agenda observations;
- CFR and U.S.C. material;
- legislation and legislative reports;
- court opinions and orders;
- GAO, CRS, and oversight reports;
- procurement or contract records;
- scientific or technical reports;
- organization, lobbying, and spending records; and
- paired related, weakly related, unrelated, temporal-mismatch,
  jurisdiction-mismatch, format-duplicate, and adversarial control cases.

Comments and submissions may belong in general corpus coverage, but an
evaluation focused on document semantics must not use them unless the
document-family goal requires them.

Bind every item to source authority, retrieval date, immutable identifier,
content digest, license or access constraints, document role, version
relationships, expected relevant set, and review provenance.

### Separate evaluation layers

Never compress quality into one score. Report:

- source acquisition completeness;
- identity and version accuracy;
- structural parsing and segment coverage;
- exact evidence alignment;
- concept proposal, assignment, and aggregation precision and recall;
- zero-tag accuracy;
- direct and deterministic lookup recall;
- semantic retrieval recall at K, ranking quality, and reranker lift;
- assertion extraction precision, recall, and abstention;
- acceptance and attestation accuracy;
- comparison accuracy;
- closure precision and recall;
- domain-interpretation accuracy;
- calibration;
- determinism and repeated-run variance; and
- failure, retry, quarantine, and unresolved rates.

Stratify results by source family, document role, subject type, concept facet,
relation type, evidence length, version condition, jurisdiction, and direct
versus implicit connection.

The model that generated a candidate cannot serve as the sole gold judge.
Freeze human-reviewed or source-derived oracles before final paid comparisons.
Preserve reviewer disagreements and adjudication records.

### Fair comparison

Use one frozen corpus generation and one fixed evaluation contract to compare:

- direct lookup;
- deterministic graph lookup;
- lexical retrieval;
- dense retrieval;
- hybrid retrieval;
- hybrid plus reranking;
- model-assisted inferred lookup; and
- incumbent versus candidate segment, embedding, reranking, and model adapters.

Keep model input, candidate pool, filters, and evaluation data identical where
the comparison question requires them. Separate extraction, acceptance,
retrieval, comparison, and domain-judgment scores.

## Reliability, security, and publication

The pipeline must prove:

- safe checkpoint and resume behavior;
- no silent partial publication after provider or parser failure;
- durable successful zero-result records;
- deterministic IDs and content-addressed generations;
- append-only assertion, assignment, concept, and event history;
- atomic publication of all linked tables;
- referential integrity across every published record;
- access-scope preservation through retrieval and model input;
- secret exclusion from prompts, logs, receipts, fixtures, and artifacts;
- quarantine for unresolved identity, evidence, scope, or lineage;
- reproducible rollback to an earlier generation pointer; and
- clean repository gates from fresh checkouts.

Provider content is data, not instruction. Treat retrieved documents as
untrusted input. Validate structured output strictly and reject undeclared tool
events or fields.

## Cross-repository ownership

| Concern | Rulespec | Spicy Regs |
| --- | --- | --- |
| Universal artifact, fragment, assertion, evidence, concept, attestation, and comparison meaning | Owns | Consumes and proves |
| Domain profiles and reusable identifier semantics | Owns versioned contracts | Supplies corpus evidence and mappings |
| CUE constraints and generated targets | Owns | Audits against carrier |
| Source acquisition and source-specific grains | Does not own | Owns |
| Parquet schemas and atomic generations | Does not own | Owns |
| Parsing, segmentation, tokenization, embedding, reranking, and model execution | Defines no provider dependency | Owns behind protocols |
| Query syntax, pagination, ranking, and serving | Defines semantic terms only | Owns |
| Provider receipts and evaluation artifacts | Defines reusable proof semantics when justified | Produces |
| Concept discovery | Defines Local and Registered concept contracts | Operates retrieval-grade loop |
| Shared-concept promotion | Defines reviewed promotion contract | Produces evidence and proposals |
| Legal or regulatory interpretation | Defines profile contract | Runs profile resolver and proves results |

For every cross-repository change:

1. capture the failing real example or query in Spicy Regs;
2. determine whether a public standard already owns the meaning;
3. change the authoritative Rulespec specification and CUE when the gap is
   reusable;
4. regenerate every Rulespec target and run its full gates;
5. cut a reachable reviewed Rulespec release and immutable digest;
6. migrate the Spicy Regs profile, carrier mapping, code, tests, and
   self-certification;
7. run the paired corpus gate against one immutable generation; and
8. record one receipt binding both commits, the contract digest, source
   snapshot, output hashes, commands, and results.

Do not patch generated Rulespec artifacts. Do not publish Spicy Regs data that
claims an unreleased Rulespec contract.

## Required Rulespec reshape

Before releasing the current relationship and lineage candidates:

1. move U.S. regulatory identifiers and `publishedInProceeding` out of the
   universal `Artifact` shape;
2. move proceeding, judicial, and congressional lifecycle values out of the
   universal lifecycle enum;
3. fix CUE shape composition in every projector;
4. replace the compatibility-driven split assertion envelope with
   `AssertionEnvelope`, `RelationshipAssertion`, and `ValueAssertion`;
5. remove mutable consumer state from immutable assertion content;
6. separate AI generation lineage from human approval;
7. add source-claimant semantics distinct from extraction provenance;
8. require exact source evidence for document-derived decision-grade
   assertions;
9. add `ConceptScheme`, richer SKOS concept fields, and evidence-bearing
   `ConceptAssignment`;
10. let both Artifacts and meaningful SourceFragments receive concept
    assignments;
11. add generic relation-change, comparison-context, proof-record, and neutral
    finding contracts outside the kernel; and
12. keep `ClosureClaim` Experimental and disabled until real closure evaluation
    passes.

## Required Spicy Regs migration

After the Rulespec release:

1. pin the exact version and contract digest;
2. map Artifacts, SourceFragments, value assertions, relationship assertions,
   concepts, schemes, assignments, evidence, confidence, lineage,
   attestations, comparisons, and findings without shadow vocabulary;
3. retain specialized source and Parquet tables;
4. bind every assignment and assertion to exact source-state evidence;
5. make document- and segment-level assignments separately queryable;
6. implement policy-bound segment-to-document aggregation with supporting
   assignment proofs;
7. use document tags only as non-evidentiary candidate context for segment
   passes;
8. install production resolvers that emit dereferenceable proof records;
9. keep static adapters as test fixtures;
10. backfill only records reconstructable from immutable source artifacts;
11. quarantine unresolved records rather than manufacturing facts; and
12. publish one atomic generation with accepted, rejected, unknown,
    quarantined, zero-result, and failure counts.

## Fair stopping point

The next stopping point does not require the complete long-term system. It must
produce a fair, inspectable baseline:

1. Rulespec has a reviewed, reachable pre-1.0 release containing:
   - the corrected universal/profile boundary;
   - immutable Artifact lineage;
   - composable relationship and value assertions;
   - concept schemes and Artifact/SourceFragment concept assignments;
   - exact evidence, confidence, derivation lineage, and separate attestations;
   - provider-neutral comparison contexts and proof records; and
   - neutral explicit affirmation-versus-denial comparison.
2. Spicy Regs pins and consumes that release without shadow vocabulary.
3. One frozen, mixed, real-data generation contains related and unrelated
   documents from several source families with Artifact and segment tags.
4. Segment assignments roll up into document assignments through one recorded
   aggregation policy; document assignments only guide new segment candidates.
5. Direct, deterministic, lexical, dense, hybrid, reranked, and inferred lookup
   arms run against the same corpus and evaluation contract.
6. A real OpenAI adapter run and a Codex CLI adapter run use the same strict
   structured-output contract and complete receipts.
7. Two independent reviewers seal the explicit-denial oracle, and three
   identical blinded provider repetitions report separate extraction,
   acceptance, evidence, and comparison metrics.
8. Failure, resume, determinism, access, secret, rollback, and clean-checkout
   gates pass.
9. Omission and domain legal-effect interpretation remain disabled.

This point is sufficient to compare approaches fairly and decide the next
investment. It need not solve every corpus family, closure policy, legal-effect
profile, or online-serving problem.

## Definition of done

The complete program is done when:

1. every supported source family has documented grain, identity, version,
   authority, access, and profile semantics;
2. every eligible Artifact and meaningful SourceFragment participates in one
   tagging architecture;
3. every derived value, concept assignment, and relation resolves its source
   evidence and complete lineage;
4. every cross-document result identifies whether it is direct,
   deterministic, semantic, or inferred;
5. every comparison distinguishes denial, discrepancy, change, bounded
   omission, and unknown;
6. every profile fails closed outside its validated identity, scope,
   authority, and closure boundaries;
7. retrieval and extraction quality are measured on frozen, mixed, real data
   with related and unrelated controls;
8. the feedback loop improves retrieval without circular evidence,
   disappearing history, or autonomous promotion;
9. Rulespec changes flow through generated constraints, reviewed releases, and
   paired Spicy Regs migrations;
10. Spicy Regs publishes reproducible atomic generations and strict receipts;
11. users can inspect the full path from a result to its document state,
    segment, evidence, derivation, attestation, and governing semantic contract;
    and
12. a decision-grade consumer can reconstruct the legal or policy state that
    applied at a requested time without relying on an unreviewed model
    conclusion.

## Explicit non-goals

Do not:

- flatten documents, dockets, proceedings, organizations, concepts, events,
  evidence, and assertions into one universal node or table;
- reuse a RIN as document or Proceeding identity;
- invent a universal Rulespec work/expression hierarchy;
- treat chunks as stable source structure when they are temporary model inputs;
- use a document tag as evidence for a segment tag;
- convert missing assignments or edges into negative facts;
- place global acceptance or lifecycle disposition on immutable proposition
  content;
- treat bare confidence scores, opaque evidence IDs, or model explanations as
  proof;
- put embeddings, reranker scores, prompt prose, provider SDK types, or query
  execution plans into ontology identity;
- use regex or an LLM as the final authority for identity, pairing, closure, or
  legal effect;
- introduce infrastructure because it is fashionable rather than because a
  measured requirement needs it;
- optimize the final evaluation against its own oracle; or
- call local, replayed, simulated, or partially gated evidence released or
  production-ready.

## Supporting decisions and evidence

This vision governs the program. These files retain detailed contracts,
research, and evidence:

- `docs/rulespec-profile.md`
- `docs/ontology.md`
- `TODO-RULE.md`
- `docs/superpowers/specs/2026-07-23-metadata-ontology-layer-design.md`
- `docs/superpowers/specs/2026-07-24-document-segmentation-carrier-decision.md`
- `docs/superpowers/specs/2026-07-24-production-document-segmentation-agent-goal.md`
- `docs/superpowers/specs/2026-07-25-relationship-assertion-release-migration.md`
- `docs/superpowers/specs/2026-07-25-relation-comparison-resolver-contract.md`
- `docs/superpowers/specs/2026-07-25-longitudinal-relation-omission-design.md`
- `docs/superpowers/specs/2026-07-25-deontic-relation-profile-boundary.md`
- `docs/superpowers/specs/2026-07-25-relation-exclusion-v2-human-adjudication-protocol.md`
- `docs/evidence/recent-document-relation-lookup-research-2026-07-25.md`
- `docs/evidence/relation-assertion-adversarial-review-2026-07-25.md`
- `docs/evidence/document-segmentation-fair-comparison-2026-07-24.md`
- `docs/evidence/arxiv-2403.10407-reranking-review.md`

When a supporting file conflicts with this vision, resolve the conflict in the
authoritative Rulespec contract or active Spicy Regs profile and update the
supporting file. Do not maintain two active meanings.
