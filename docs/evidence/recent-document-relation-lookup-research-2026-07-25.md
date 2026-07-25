# Recent document, relation, and lookup research

- **Date:** 2026-07-25
- **Research window:** 2025-07-25 through 2026-07-25
- **Status:** Architecture input; implementation gates remain open
- **Scope:** Versioned documents, relation extraction and comparison, bounded
  absence, retrieval, reranking, and evidence-backed AI workflows

## Decision

Keep a small document-agnostic core and place legal or regulatory meaning in
profiles.

The core should distinguish:

1. a stable work identity owned by a public or domain vocabulary;
2. an immutable Rulespec artifact for each document state;
3. explicit version, revision, and format links;
4. a sourced relationship assertion;
5. a deterministic comparison or change event;
6. a neutral finding; and
7. an optional domain judgment.

Use four lookup classes: direct, deterministic, semantic, and inferred. Apply
identity, version, authority, scope, and access filters before ranking. Use
hybrid lexical and dense retrieval plus a reranker for broad discovery. Invoke
graph planning or an LLM only when the question requires cross-document
reasoning.

Do not model a generic negative tag. Explicit denial, contradiction, removal,
and bounded non-observation have different evidence and semantics.

## What recent work changes

### Version identity is part of relevance

[VersionRAG](https://arxiv.org/abs/2510.08109) treats evolving documents as a
separate retrieval problem. Its hierarchical version graph and query routing
outperformed naive RAG and GraphRAG on version-sensitive questions. The result
supports a first-class work/version seam and temporal filtering before
semantic ranking.

[LegDiff](https://aclanthology.org/2026.acl-srw.86/) shows why character diffs
are insufficient for revised legal text: paraphrases and semantic changes
require span-aware comparison. An LLM may propose aligned spans, but a durable
change event must retain both source versions and the exact evidence.

[FourCorners](https://aclanthology.org/2026.acl-industry.124.pdf) combines
version, hierarchy, reference, cross-source, and reading-order edges in a
production legal graph. It uses structural parsing for predictable relations,
LLM-assisted extraction for ambiguous references, and deterministic inventory
checks to reject impossible targets. This division matches Rulespec's
dependency-inverted resolver boundary.

### Retrieval sets the quality ceiling

[Legal RAG Bench](https://arxiv.org/abs/2603.01710) decomposes end-to-end legal
RAG failures and finds retrieval more influential than the answer model.
[Benchmarking Legal RAG](https://arxiv.org/abs/2603.03300) likewise reports
retrieval omissions, concept confusion, and exception errors in statutory
surveys. Stronger generation cannot repair absent or temporally wrong
evidence.

The practical lookup order is:

| Class | Use | Required evidence |
| --- | --- | --- |
| Direct | Exact identifiers and source citations | Matched identifier, source artifact, version |
| Deterministic | Typed joins and validated graph paths | Path, direction, ruleset, snapshot |
| Semantic | Uncited but related material | Lexical and dense candidates, fusion, rerank, spans |
| Inferred | Multi-document or implicit relation | Inputs, plan or graph path, model lineage, validation |

Semantic similarity never upgrades itself to a source assertion. Inferred
relationships remain candidates until evidence, scope, and policy resolvers
accept them.

### Missing is not negative

[Semantic Units](https://www.nature.com/articles/s41597-026-07588-3) separates
statement content from provenance, epistemic status, time, and negation. Its
statement-unit approach supports immutable `RelationshipAssertion` records
with separate evidence and acceptance state.

[NEI-CAP](https://arxiv.org/abs/2605.26663) shows that easy constructions of
“not enough information” can conceal catastrophic failure on difficult
absence cases. A closure record must therefore name the observed sources,
scope, method, expected set, and coverage proof. An aggregate confidence score
is not closure.

[Re²-DocRED](https://aclanthology.org/2026.eacl-long.213/) finds pervasive
false negatives in document-level relation extraction benchmarks. Absence in
an extracted graph may reflect annotation or extraction failure, not a
negative source fact.

Use these distinct records:

| Evidence situation | Representation |
| --- | --- |
| A source explicitly denies a relation | Denied `RelationshipAssertion` |
| Accepted assertions disagree | Neutral discrepancy finding |
| A later version removes or changes text | `RelationChangeEvent` |
| An expected relation was not found under proven coverage | Neutral omission finding plus `ClosureClaim` |
| Coverage or alignment is incomplete | `unknown` or `evidence_insufficient` |
| A regulation gives the change legal force | Profile-owned `NormEvaluator` judgment |

### Structure should bound model input

[Legal-DC](https://arxiv.org/abs/2603.11772) reports gains from clause-boundary
segmentation and reflective retrieval. [COMPACT](https://aclanthology.org/2026.eacl-long.377/)
models definitions, exceptions, conditions, and temporal sequences as typed
clause graphs. Both support the current source-aware segmenter: preserve
document, section, clause, table, and list boundaries before applying token
limits.

Regex remains useful for deterministic headings, citations, and source
normalization. It should not decide semantic scope, deontic force, or relation
equivalence.

## Generic document identity

Rulespec already defines `Artifact` as an immutable edition, publication,
snapshot, or content payload. Keep that class and compose public relation
terms:

| Relation | Identity rule |
| --- | --- |
| `dcterms:isVersionOf` | Links an immutable Artifact to a stable work resource when content differs substantively |
| `prov:wasRevisionOf` | Links a later Artifact to the exact earlier Artifact from which it was revised |
| `dcterms:isFormatOf` / `dcterms:hasFormat` | Links substantially identical content in different formats or source postings |

The stable work resource may use a public or profile-owned class. Rulespec
does not need to own `DocumentWork`, `DocumentExpression`, `Manifestation`, or
`Item` classes.

This 80/20 split composes established models without importing them:

- [ELI](https://op.europa.eu/en/web/eu-vocabularies/eli) separates a legal
  resource from legal expressions and remains the legal-profile alignment;
  legal profiles should use its native realization and version terms.
- [BIBFRAME](https://www.loc.gov/bibframe/docs/bibframe2-model.html) separates
  a conceptual Work from published Instances and concrete Items.
- [Schema.org `CreativeWork`](https://schema.org/CreativeWork) provides broad
  work/example links but is too permissive to enforce Rulespec identity.
- IFLA's Work, Expression, Manifestation, and Item model supplies the deeper
  conceptual precedent. Rulespec needs only the first two distinctions now.

The base ontology must not assume that every stable resource is legislation,
that every revision is legally effective, or that publication time equals
valid time.
Regulatory, judicial, contractual, scientific, and administrative profiles may
add their own authority and lifecycle rules.

## Resolver and receipt contract

Each comparison gate now emits a content-addressed `ResolverProofRecord`. A
record binds:

- resolver and policy identity;
- comparison and source snapshot identity;
- evaluation time and outcome;
- input identifiers and digests;
- supporting evidence, attestations, warrants, or declarations; and
- a human-readable rationale.

The persisted OpenAI diagnostic was rebuilt from its existing response under
`resolver-proof-record-v1`. The provider was not invoked. Its integrity passes,
but its quality remains a failure: 9 of 12 outcomes match, 2 of 4 direct
denials produce findings, and no control produces a false finding.

Future lookup receipts should also retain:

- query classification and requested time;
- pre-ranking authority, scope, access, and version filters;
- candidates, ranks, and scores from each retrieval leg;
- fusion and reranking parameters;
- returned spans and artifact hashes;
- graph paths or generated subqueries;
- excluded candidates and exclusion reasons; and
- completeness or closure claims, when any exist.

## Package-first implementation

The repository already has the needed 80/20 components:

- DuckDB for exact and structured lookup;
- Sentence Transformers for the incumbent BGE embeddings;
- a dependency-inverted CrossEncoder reranker;
- source-aware segmentation and model-input audits; and
- OpenAI structured-output adapters with strict receipts.

Do not add Chonkie, Haystack, Qdrant, GraphRAG, or another framework by default.
Evaluate a package only when a measured need exceeds the current adapters:
index scale, online serving, graph planning, or segmentation quality. Keep any
provider behind project-owned protocols.

The OpenAI role stays narrow:

- classify complex query intent;
- propose source-grounded assertions or semantic alignments;
- propose multi-document lookup plans; and
- summarize accepted evidence.

Deterministic code must still validate identifiers, source spans, versions,
scope, chronology, comparison gates, and receipts.

## Near-term gates

1. Add and release generic Artifact version and revision relations in
   Rulespec by composing Dublin Core and PROV-O.
2. Map existing Spicy Regs artifact identities and lineage without inventing
   missing links or a universal document-work class.
3. Keep `ClosureClaim` and omission findings disabled until a reviewed dataset
   measures closure precision and recall.
4. Evaluate direct, deterministic, hybrid semantic, and inferred lookups as
   separate arms on one frozen corpus generation.
5. Record all retrieval legs and exclusions so errors can be assigned to
   identity, coverage, ranking, reasoning, or ontology.
6. Add a domain `NormEvaluator` only when real regulatory examples require a
   legal conclusion beyond the neutral finding.

## Bottom line

The recent literature supports the direction already emerging in Rulespec and
Spicy Regs. The missing structural piece is explicit version and revision
lineage between immutable Artifacts and stable resources. Once that exists,
assertions, change events, omissions, retrieval results, and domain judgments
can refer to the correct document state without collapsing absence into
falsity.
