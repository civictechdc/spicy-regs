<!-- markdownlint-disable MD013 -->

# Regulatory Evidence Framework 1.0

## Editor's Draft, 28 July 2026

> **Short name:** REF
>
> **This version:** [regulatory-evidence-framework.md](regulatory-evidence-framework.md)
>
> **Implementation plan:** [Regulatory Evidence Framework implementation plan](regulatory-evidence-framework-implementation-plan.md)
>
> **Status:** Rulespec-dependent Editor's Draft
>
> **Editors:** Spicy Regs project
>
> **Feedback:** [civictechdc/spicy-regs issues](https://github.com/civictechdc/spicy-regs/issues)
>
> **License:** [MIT](https://github.com/civictechdc/spicy-regs/blob/main/LICENSE)

## Abstract

The Regulatory Evidence Framework (REF) defines an acquisition, processing,
and application profile for regulatory evidence systems. It preserves exact
source material, resolves source records into versions and renditions, creates
reproducible evidence addresses, runs deterministic and probabilistic
processing, governs registry import and deployment, and publishes auditable
query products.

REF depends normatively on Rulespec for portable semantic records. Rulespec is
the single source of truth for artifacts used as assertion evidence, source
fragments, evidence bindings, assertions, concept assignments, confidence, AI
lineage, attestations, local adoption, authority, lifecycle, access, retention,
reference-resource releases, and semantic conformance. REF does not redefine
those records. It specifies how an operational regulatory pipeline creates,
evaluates, and serves them.

The specification does not require a storage engine, graph database, search
engine, controlled vocabulary, embedding model, or language-model provider.

## Status of This Document

This document is a W3C-style project specification. It is not a W3C Standard,
has not undergone the W3C Process, and does not imply W3C endorsement.

This draft starts from the project's source inventories and recovered external
research. The dated row and resource universe in the two inventories below is
the minimum normative portfolio-coverage and stress-test baseline under
Section 2.6, not a closed list of what REF can process. Their proposed
architectures, classifications, priorities, and adoption recommendations
remain research inputs, not adopted design:

- [Source Vocabulary, Ontology, and Authority Catalog](source-vocabulary-ontology-thesaurus-catalog-2026-07-28.md)
- [Source and Document Type Matrix](source-document-type-matrix-2026-07-28.md)
- [Blind External Research Recovery](evidence/blind-external-research-recovery-2026-07-28/README.md)
- [When to Abandon a Controlled Vocabulary, and What US Federal Policy Vocabularies Exist](evidence/blind-external-research-recovery-2026-07-28/when-to-abandon-controlled-vocabulary-and-federal-vocabulary-inventory.md)

REF and Rulespec are controlled by the same project. This draft therefore
places reusable meaning in Rulespec even when an upstream change is required,
rather than defining a temporary REF substitute. Section 4 identifies the
binding and any upstream dependency that must be resolved before a conforming
release.

Requirements may change before version 1.0. Implementers should identify the
exact REF draft and immutable Rulespec release in conformance claims.

## Table of Contents

1. [Introduction](#1-introduction)
2. [Conformance](#2-conformance)
3. [Terminology](#3-terminology)
4. [Rulespec dependency and conceptual model](#4-rulespec-dependency-and-conceptual-model)
5. [Operational information model](#5-operational-information-model)
6. [Identity, versions, and time](#6-identity-versions-and-time)
7. [Evidence addressing and operational provenance](#7-evidence-addressing-and-operational-provenance)
8. [Processing model](#8-processing-model)
9. [Semantic enrichment](#9-semantic-enrichment)
10. [Relationship discovery and publication](#10-relationship-discovery-and-publication)
11. [Policy threads](#11-policy-threads)
12. [Registry operations and concept governance](#12-registry-operations-and-concept-governance)
13. [Publication and query behavior](#13-publication-and-query-behavior)
14. [Privacy, security, rights, and safety](#14-privacy-security-rights-and-safety)
15. [Validation and evaluation](#15-validation-and-evaluation)
16. [Binding manifest and interoperability](#16-binding-manifest-and-interoperability)
17. [References](#17-references)
18. [Appendix A: Example operational and Rulespec records](#appendix-a-example-operational-and-rulespec-records)
19. [Appendix B: Relationship predicate ownership](#appendix-b-relationship-predicate-ownership)
20. [Appendix C: Requirement index](#appendix-c-requirement-index)

## 1. Introduction

### 1.1 Problem statement

Regulatory information arrives as records, web pages, XML, PDFs, attachments,
legal text, measurements, comments, dockets, cases, and external indexes. These
inputs do not share one identity model, version model, document taxonomy, or
subject vocabulary.

A document-first classifier hides those differences. It also makes generated
tags and links appear more authoritative than their evidence supports. REF
instead asks a narrower first question:

> What can the system preserve and verify before it interprets the material?

The answer creates a stable evidence layer. Search, classification, summaries,
entity resolution, relationship discovery, and future models remain replaceable
derived services.

### 1.2 Goals

REF has nine goals:

1. Preserve exact source content and retrieval context.
2. Distinguish captures, source-record revisions, source-issued versions, and
   renditions.
3. Type documents, participation records, containers, entities, observations,
   events, and external references before semantic enrichment.
4. Project accepted semantic results into Rulespec records bound to exact
   evidence or declared input assertions.
5. Keep processing state separate from Rulespec origin, attestation,
   authority, lifecycle, and product adoption.
6. Represent explicit and implicit relationships without collapsing
   similarity, dependency, identity, causation, or legal effect.
7. Support controlled concepts, grounded open labels, concept proposals, human
   governance, and abstention without redefining Rulespec concept semantics.
8. Publish historical and current views that users can audit and reproduce.
9. Account explicitly for every source and controlled resource in the dated
   portfolio baseline without requiring every one to be ingested or adopted.
10. Admit future source families, data products, controlled resources,
    executable models, standards, and external systems through governed
    extension profiles without coercing them into an inaccurate core route or
    redefining portable Rulespec semantics.

### 1.3 Non-goals

REF does not define:

- assertions, evidence bindings, confidence records, attestations, adoption,
  authority, lifecycle, access scopes, retention policies, concepts, concept
  assignments, concept mappings, or AI lineage;
- a universal regulatory topic vocabulary;
- the optimum number of concepts in a vocabulary;
- mandatory ingestion or adoption of every inventoried federal source;
- a legal reasoning or legal-advice method;
- a required database, graph engine, vector index, or queue;
- a required parser, optical character recognition system, embedding model,
  reranker, or language model;
- a single score that establishes truth or production readiness; or
- a rule that every artifact must receive a subject or relationship.

Topic tagging is an optional REF module. A conforming evidence implementation
may provide no automated subject assignments.

### 1.4 Design principles

REF follows these principles:

- **Preserve first.** Store source bytes and source-native values before
  normalization.
- **Type second.** Identify what a record represents before processing its
  text.
- **Link third.** Resolve official identifiers, citations, versions, and
  lifecycle structure before adding probabilistic links.
- **Enrich fourth.** Publish accepted semantic labels and inferred
  relationships as evidence-bound Rulespec assertions.
- **Append decisions.** Record review, adoption, and lifecycle through the
  applicable Rulespec records without erasing history.
- **Keep authority visible.** Rulespec attestations and local adoption do not
  convert an inference into a source statement or legal authority.
- **Allow no answer.** Abstention and explicit failure are valid outputs.
- **Rebuild derived views.** Search, vector, and graph indexes are disposable
  views, not the only record of truth.

## 2. Conformance

### 2.1 Normative language

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**,
**RECOMMENDED**, **MAY**, and **OPTIONAL** in this document are to be interpreted
as described in BCP 14 when, and only when, they appear in all capitals.

Normative requirements carry stable identifiers such as `REF-CORE-001`.
Examples, notes, diagrams, and appendices are informative unless they state
otherwise.

### 2.2 Conformance classes

**REF-CONF-011:** An implementation MAY claim one or more classes:

| Class | Required capability | Required classes |
| --- | --- | --- |
| `REF-Core-Producer` | Produces baseline-enumeration reports, inventory-coverage manifests, captures, source-record revisions, source-resource versions, rendition-processing records, selector-resolution records, run receipts, REF publication-release manifests, and their required Rulespec records. | None |
| `REF-Relationship-Producer` | Discovers and adjudicates relationship candidates and publishes accepted durable results as Rulespec records. | `REF-Core-Producer` |
| `REF-Enrichment-Producer` | Runs typed, open-set enrichment and publishes accepted results as Rulespec records. | `REF-Core-Producer` |
| `REF-Reference-Resource-Registry` | Imports, snapshots, validates, selects for deployment, and rolls back subject schemes, ontologies, identifier authorities, entity registries, code lists or classifications, schemas, and mapping sets whose portable release records conform to Rulespec. | `REF-Core-Producer` |
| `REF-Policy-Thread-Publisher` | Publishes versioned application views and Rulespec membership assertions under Section 11. | `REF-Core-Producer`, `REF-Relationship-Producer` |
| `REF-Query-Service` | Returns current, historical, evidence, and supported query-time association views under Section 13. | None; see `REF-CONF-010` |
| `REF-Participation-Processor` | Processes public participation under the additional controls in Section 14. | `REF-Core-Producer` |
| `REF-Validator` | Validates REF-owned operational records and behavior, invokes a pinned Rulespec validator for Rulespec records, and reports both result sets without merging them. | None |

A full capability-set implementation conforms to
`REF-Core-Producer`, `REF-Relationship-Producer`,
`REF-Enrichment-Producer`, `REF-Reference-Resource-Registry`, and
`REF-Query-Service`. The participation class remains optional and separately
governed. Complete portfolio accounting covers every row in the Section 2.6
minimum baseline and every additional item in the implementation's declared
portfolio; it is not a requirement to ingest or adopt every item. A claim that
the full-framework design can represent that complete portfolio additionally
requires `supported` representability for every coverage component.

### 2.3 Conformance claims

**REF-CONF-001:** A conformance claim MUST identify:

- the REF version and draft date;
- the immutable Rulespec release or commit, constraint-bundle digest,
  conformance level, adopted Rulespec profiles, validator version, and
  validator result;
- each claimed class;
- the serialization and media type;
- every implemented extension profile;
- each immutable registry, mapping, output-profile, or other release assessed
  by the claim, identified by resource type, identifier, version, and content
  digest;
- the validator and test-suite version;
- the validation date and result;
- the immutable inventory-coverage manifest; minimum baseline inventory
  identifiers and digests; every additional declared inventory or item;
  extension profiles; and row, occurrence, component, and fixture-accounting
  results; and
- known limits or failed optional recommendations.

**REF-CONF-002:** An implementation MUST satisfy every MUST and MUST NOT
requirement applicable to each class it claims.

**REF-CONF-003:** A validator MUST report results by requirement identifier. It
MUST NOT replace failed class-specific results with one aggregate pass score.

**REF-CONF-004:** An implementation MUST use Rulespec's distinct records for
assertion origin, attestation, authority, confidence, lifecycle, and local
adoption. It MUST NOT mint REF fields or value sets that duplicate them.

### 2.4 Extensions

**REF-CONF-005:** Extensions MUST use stable, documented names and MUST NOT
change the meaning of REF-defined fields or Rulespec terms.

**REF-CONF-006:** A consumer SHOULD preserve unknown extension fields during a
lossless read-write round trip.

**REF-CONF-007:** A profile MAY add stricter requirements. It MUST identify
those requirements and MUST NOT weaken REF's operational requirements or the
pinned Rulespec requirements.

**REF-CONF-008:** A producer claiming a class MUST also claim and pass every
required class listed in the conformance table.

**REF-CONF-014:** The two dated inventories are a required minimum coverage
suite, not a closed-world type registry. A conforming implementation MAY add
sources, resources, standards, models, services, media, jurisdictions, and
other portfolio items through versioned extension profiles. Every added item
MUST pass the same enumeration, routing, representation, fixture, rights, and
release-accounting controls as a baseline item.

**REF-CONF-015:** An extension profile MUST use stable absolute IRIs for new
route or processing values, define their meaning and boundary from every
overlapping core value, identify their REF processing binding and portable
Rulespec or external-standard binding, and provide positive, negative, and
lossless round-trip fixtures. It MUST NOT use a new value merely to avoid an
applicable core route or upstream semantic requirement.

### 2.5 Requirement applicability and data bindings

Requirement applicability follows this table:

| Conformance claim or feature | Applicable requirement groups |
| --- | --- |
| Every producer and `REF-Query-Service` | `REF-CONF`, `REF-BIND`, `REF-PORT`, applicable `REF-SEC`, `REF-SAFE`, `REF-RIGHTS`, `REF-TEST`, and `REF-INT` |
| `REF-Core-Producer` | `REF-CAP`, `REF-SRC`, `REF-ART`, `REF-EVID`, `REF-TYPE`, `REF-ID`, `REF-VER`, `REF-TIME`, `REF-PROV`, and `REF-PIPE` |
| Semantic-reference candidate output | `REF-SEM` |
| `REF-Enrichment-Producer` | `REF-SEM`, `REF-ENR`, `REF-CAND`, `REF-ACC`, and `REF-ASSIGN` |
| Registered assignment output | `REF-ACC-008` and a passing `REF-Reference-Resource-Registry` manifest for the referenced Rulespec reference-resource release |
| `REF-Relationship-Producer` | `REF-REL`; plus `REF-SIM`, `REF-DEP`, `REF-PATH`, or `REF-ABS` when that feature is emitted |
| `REF-Reference-Resource-Registry` | `REF-VOC`; `REF-GOV` and Rulespec concept and mapping requirements apply only when those semantic payloads are present |
| `REF-Policy-Thread-Publisher` | `REF-THR` |
| `REF-Query-Service` | `REF-QRY` and `REF-EXP` |
| `REF-Participation-Processor` | `REF-PRIV` and every applicable `REF-SEC`, `REF-SAFE`, and `REF-RIGHTS` requirement |
| `REF-Validator` | `REF-CONF`, `REF-TEST`, `REF-BIND`, applicable `REF-SEC`, `REF-SAFE`, `REF-RIGHTS`, and the REF requirements named by its validator profile |
| Accepted automated output | `REF-EVAL` for its source, facet, predicate, and output profile |

**REF-CONF-009:** A conformance manifest MUST list every normative requirement
in the claimed class closure as `pass`, `fail`, or `notApplicable`, with test
evidence or a reason. An implementation MUST NOT mark a requirement not applicable
when the implementation emits the record type or feature that triggers it.

**REF-CONF-010:** A `REF-Query-Service` conformance claim MUST name the
producer classes, versioned REF profiles, and Rulespec binding represented in
the service's data.

**REF-CONF-012:** A `REF-Validator` conformance claim MUST name the REF version,
serialization bindings, classes, profiles, and requirement set it validates.
The validator MUST pass a published validator-conformance suite containing
valid and intentionally invalid REF fixtures for that declared scope, accept
every valid REF reference fixture, reject every invalid REF reference fixture,
and report the applicable requirement identifiers. It MUST invoke, not
reimplement, the pinned Rulespec validator for Rulespec records.

**REF-CONF-013:** A conformance claim MUST distinguish `portfolioAccounting`
from `fullFrameworkDesignCoverage`. The former requires exhaustive row and
component accounting. The latter additionally requires `supported`
representability for every component, including the concrete mappings and
passing component fixtures required by `REF-PORT-012`. Neither status implies
adapter or import implementation, release inclusion, or rights/use
authorization. Both claims cover the mandatory dated baseline plus the
implementation's complete declared portfolio and all active items under
`REF-PORT-013`; neither claim means that the two dated inventories are REF's
closed universe.

The REF abstract model is normative, but an exchange needs a concrete data
binding.

**REF-BIND-001:** A conforming REF serialization profile MUST define field names,
datatypes, cardinalities, identifier grammar, ordering and canonicalization
rules, null and absence behavior, extension handling, value-set bindings, and
the mapping from serialized values to REF-owned operational records.

**REF-BIND-002:** A conformance claim MUST identify and validate against one
serialization profile. It MUST NOT claim serialization interoperability from
the abstract model alone.

**REF-BIND-003:** Two REF serialization profiles MAY claim operational
interoperability only when a published round-trip test preserves every
applicable REF identifier, type, source-native value, evidence address, time
value, rights-policy reference, release decision, and supersession link.
Semantic interoperability is determined by the pinned Rulespec binding and
Rulespec conformance, not by a parallel REF semantic model.

**REF-BIND-004:** A serialization profile that supports deterministic
processing MUST define the canonical deterministic payload, its digest
algorithm, which run-instance provenance fields are excluded from that digest,
and how each run receipt links to the stable payload.

### 2.6 Portfolio coverage

The portfolio baseline consists of the row and resource universe in these
dated inputs:

- [Source and Document Type Matrix, 28 July 2026](source-document-type-matrix-2026-07-28.md)
- [Source Vocabulary, Ontology, Thesaurus, and Authority Catalog, 28 July 2026](source-vocabulary-ontology-thesaurus-catalog-2026-07-28.md)

The inventories are proposed research artifacts. This specification adopts
their enumerated universe as the minimum coverage corpus only. It does not adopt
their current architecture, proposed classifications, priorities, source
roles, vocabulary roles, or recommendations. Each implementation profile
independently validates route, type, authority, rights, permitted use, and
production suitability and preserves unknown or disputed values. Additional
versioned inventories and individual portfolio items may extend this corpus
under `REF-CONF-014`; they do not require a new REF version when existing core
routes fit.

An `InventoryCoverageManifest` is an immutable REF operational record. It
references each baseline row by a stable inventory-local key or locator rather
than copying the row. Each row account decomposes the row into one or more
coverage components. A component is one source role, controlled-resource role,
named feed, reference spine, external system, or other semantic unit that can
receive one unambiguous route. Every component declares exactly one route
family, `source` or `controlledResource`, before selecting a route from that
family. `source` covers evidence-producing or externally joined inputs;
`controlledResource` covers governed resources used to identify, structure,
interpret, map, validate, calculate, or constrain them. The named core routes
are not a closed list: a versioned extension profile may add an absolute-IRI
route within either family under `REF-CONF-015`. A compound row such as “Document plus
Observation” therefore has at least two components. Each source component also
has one acquisition mode: `captured` when REF obtains source material, or
`externalJoin` when REF retains an identifier-based join to the authoritative
external system. A mixed Entity plus External-join row is therefore an
`entity` component with `externalJoin` acquisition, not a combined semantic
type. Each component keeps four decisions independent:

1. whether the framework can represent the item;
2. whether an adapter or import path exists;
3. whether a named release or environment includes it; and
4. whether the intended acquisition, processing, model, display, and
   redistribution uses are authorized.

The manifest also pins a deterministic `BaselineEnumerationReport` by
identifier, version, and digest. The report classifies every data row in every
GitHub Flavored Markdown table in both baseline files as:

- `coverageRow`, which receives exactly one manifest row account;
- `constituentRow`, which names one `coverageRow` parent and whose distinct
  named constituents and roles become components under that account; or
- `definitionRow`, which defines explanatory structure and receives no
  manifest row account.

The report also enumerates, from table cells as well as prose and lists, every
occurrence of a named source, feed, reference spine, external system,
controlled resource, distinct subtype group, or separately stated semantic
role. Each occurrence has a row, cell, list-item, or source-span locator and is
classified as a `namedPortfolioItem` or `descriptiveMention`. Each named
portfolio item either resolves to an existing `coverageRow` and one or more
components under that account or receives its own stable manifest row account.
A descriptive mention remains visible with its locator and reason; it cannot
hide an item or role that the inventories propose to source, join, govern,
map, classify, or otherwise use. A definition row likewise remains visible
with its locator and reason. The report publishes raw table-row and occurrence
counts, counts by classification, and the exact parsing, item-discovery,
normalization, and review procedure used to derive them.

Route, semantic use, and delivery status are separate. In addition to its one
route, each component declares one or more applicable semantic/use modes:

| Mode | Meaning |
| --- | --- |
| `directAuthority` | The resource is an approved direct authority for a portable value or identity |
| `sourceEvidence` | The source supplies evidence that remains attributable to it |
| `deterministicControl` | The resource supplies identifiers, codes, structure, or other deterministic control values |
| `mappingOnly` | The resource is used only for translation, retrieval, or expansion, not as a direct output authority |
| `externalReference` | The system retains an identifier-based reference to the authoritative external resource |

Status values are constrained by dimension:

| Dimension | Meaning of `supported` | Allowed statuses |
| --- | --- | --- |
| Representability | The framework can represent the component without semantic loss | `supported`, `planned`, `deferred`, `unsupportedWithReason`, `notAssessed` |
| Adapter or import implementation | The adapter or import path is implemented | `supported`, `planned`, `deferred`, `unsupportedWithReason`, `notApplicable`, `notAssessed` |
| Release inclusion | The named release or environment includes the component | `supported`, `planned`, `deferred`, `unsupportedWithReason`, `notApplicable`, `notAssessed` |
| Rights/use authorization | Non-authoritative summary of whether the stated use is authorized | `supported`, `rightsBlocked`, `notApplicable`, `notAssessed` |

`supported` means the named dimension is implemented or approved for the
declared scope. For the rights/use dimension, it reports the outcome found in
the referenced authoritative records; it does not make that decision.
`planned` means approved work is scheduled but unavailable.
`deferred` means deliberately outside the current delivery sequence.
`unsupportedWithReason` records a concrete capability limit.
`notAssessed` means no evidence-backed decision exists yet.
`notApplicable` means the dimension does not apply to the declared component
and mode. Neither means supported. `externalJoin` is a source acquisition
mode, `mappingOnly` is a semantic/use mode, and `rightsBlocked` is only a
rights/use authorization summary. The manifest never authorizes use:
`RightsAssessment` plus the adopted Rulespec and external rights policy remain
authoritative.

`supported` in one dimension does not imply `supported` in another. For
example, an ontology may be representable and imported while production use
remains `rightsBlocked`.

**REF-PORT-001:** An inventory-coverage manifest MUST identify the exact two
baseline files, their dates, digest algorithms, and content digests and MUST
pin the exact `BaselineEnumerationReport` and its Rulespec audit attestation.
It MUST contain exactly one row
account for every `coverageRow` and every `namedPortfolioItem` that does not
resolve to an existing `coverageRow`. Each account MUST contain one or more
coverage components that exhaustively represent that row, its linked
`constituentRow` entries, every `namedPortfolioItem` resolved to it, and all of
their distinct named constituents and roles.
Repeated references to the same real resource MAY point to one shared
component identity, but no required row account, named portfolio item,
constituent, or role may disappear through deduplication.

**REF-PORT-002:** Every source coverage component MUST select exactly one route:
the core route `document`, `participation`, `container`, `entity`,
`observation`, or `event`, or one absolute-IRI source route registered by a
conforming extension profile.
It MUST also select exactly one acquisition mode: `captured` or
`externalJoin`. A row with more than one actual semantic role or acquisition
pattern MUST decompose into multiple components. Each route and acquisition
mode MUST be independently justified from the source's actual role and MUST
NOT be copied blindly from a proposed inventory classification.

**REF-PORT-003:** Every controlled-resource coverage component MUST select
exactly one route: the core route `subjectScheme`, `ontology`,
`identifierAuthority`, `entityRegistry`, `codeList`, `classification`,
`schema`, or `mappingSet`, or one absolute-IRI controlled-resource route
registered by a conforming extension profile. A
row that bundles multiple named resources or roles MUST decompose into multiple
components. The route MUST describe the resource's role in the named profile;
the same external resource MAY have a separately justified route in another
profile. These routes are REF coverage roles, not a closed Rulespec
`dcterms:type` value set.

**REF-PORT-004:** Every coverage component MUST record its semantic/use modes
and representability, adapter or import implementation, release inclusion,
and rights/use authorization as separate dimensions. Each dimension MUST use
only its allowed status and MUST include scope, evidence, owner, decision time,
and reason when the status is not `supported`. Rights/use status MUST be scoped
to the stated acquisition, processing, model, display, or redistribution use.
Both `supported` and `rightsBlocked` MUST reference the exact
`RightsAssessment` and applicable adopted Rulespec and external policy
evidence. A consumer MUST resolve authorization from those records, not from
the coverage-manifest summary.

**REF-PORT-005:** An `externalJoin` acquisition mode MUST identify the external
authority, join identifiers, and versioning strategy. A `mappingOnly` mode MUST
identify the mapping path and exact releases. `rightsBlocked` MUST identify
the blocked uses without exposing restricted terms. An
`unsupportedWithReason` status MUST describe the missing capability and the
condition for reconsideration.

**REF-PORT-006:** Complete portfolio accounting requires 100 percent of table
data rows and named portfolio items to be validly dispositioned in the pinned
`BaselineEnumerationReport`; exactly one valid, non-placeholder row account
for every `coverageRow` and otherwise unrepresented `namedPortfolioItem`;
exactly one valid parent for every `constituentRow`; no row account for a
`definitionRow`; and routed components for 100 percent of the distinct named
constituents and roles under the accounts. A component may expose a
representability gap. Full-framework design coverage additionally requires
representability status `supported` and the proof required by `REF-PORT-012`
for every component. Neither claim requires 100 percent adapter
implementation, ingestion, registry import, release inclusion, or rights
authorization.

**REF-PORT-007:** Each release report MUST publish the manifest identifier and
digest; the pinned `BaselineEnumerationReport` identifier and digest; raw row
and named-item counts; counts by `coverageRow`, `constituentRow`,
`definitionRow`, and `namedPortfolioItem`; expected and actual row-account and
component counts; counts by route family, route, acquisition mode,
semantic/use mode, and status within each dimension; changes from the prior
manifest; and stable keys for blocking entries. An unaccounted count applies
only to entries that require a row account or component under
`REF-PORT-001`; definition rows are reported, not unaccounted. A release MUST
NOT claim full-framework design coverage while any required row account,
named constituent, or role is missing or any component has an unspecified
route, applicable acquisition mode, semantic/use mode, or dimension, or a
representability status other than `supported`.

**REF-PORT-008:** A changed baseline inventory, row decomposition, component,
route family, route, acquisition mode, semantic/use mode, status dimension,
reason, scope, evidence, or ownership decision MUST create a new immutable
manifest version.
Historical `PublicationReleaseManifest` records MUST continue to resolve the exact
inventory-coverage manifest they used.

**REF-PORT-009:** The baseline enumeration MUST include, where present in the
two dated inventories, all current source rows, all roadmap tiers, `E01`–`E05`,
`G01`–`G09`, every named feed or reference spine, every adjacent external
system, every external-join row, and every table row or out-of-table item that
names a controlled resource or distinct constituent. Genuine definition and
completeness-ledger rows remain in the enumeration report but do not receive
coverage accounts solely because they are table rows. Coverage MUST NOT be
limited to `C`, `T`, or `L` identifier series.

**REF-PORT-010:** Every coverage component MUST select exactly one route
family, `source` or `controlledResource`, and then exactly one route allowed by
`REF-PORT-002` or `REF-PORT-003` for that family. A named feed, reference
spine, external system, adjacent resource, or other semantic unit MUST
decompose when necessary and classify by its actual role; it MUST NOT remain
outside both route families.

**REF-PORT-011:** The pinned baseline-enumeration report MUST assign a stable
locator and exactly one classification to every data row in every GitHub
Flavored Markdown table in both baseline files, excluding only the header and
delimiter rows defined by that syntax. A `constituentRow` MUST name exactly one
`coverageRow` parent. A row MAY be a `definitionRow` only when it defines a
role, status, format, code pattern, decision state, completeness total, or
other explanatory structure and does not itself name a source, feed, reference
spine, external system, controlled resource, or distinct constituent requiring
coverage. The report MUST also enumerate every occurrence of a named source,
feed, reference spine, external system, controlled resource, distinct subtype
group, or separately stated semantic role inside table cells and outside
tables. Each occurrence MUST have an exact source locator, be exactly one of
`namedPortfolioItem` or `descriptiveMention`, and state the account and
component resolution for a named portfolio item. A `descriptiveMention` MUST
state why the occurrence is not a proposed source, join, governed resource,
mapping, classification, or use. The accounting validator MUST independently
recompute the raw table-row universe, verify every occurrence locator and
expected constituent count, and reject a count mismatch, an unclassified or
multiply classified entry, an invalid parent or resolution, or a definition
or descriptive classification that hides a portfolio item or role. An
independent reviewer MUST audit source-text exhaustiveness and record the
result as a Rulespec attestation targeting the report; a full-framework
design-coverage claim requires a passing attestation.

**REF-PORT-012:** A representability status of `supported` MUST reference a
versioned, concrete, lossless representation mapping for that component. The
mapping MUST identify the applicable REF operational records and fields, the
pinned Rulespec or external types and predicates, the handling of every named
constituent and role, and any declared non-semantic source-native values. It
MUST also reference at least one passing positive fixture and one passing
round-trip fixture that exercise the component's actual structure. A
full-framework design-coverage claim MUST fail when any mapping or fixture is
missing, stale relative to the manifest or pinned specifications, lossy, or
does not cover every named constituent and role.

**REF-PORT-013:** Every additional inventory or individually onboarded
portfolio item declared by an implementation MUST be pinned by identifier,
version, and digest and incorporated into a new `BaselineEnumerationReport`
and `InventoryCoverageManifest` version. Complete accounting and
full-framework design coverage apply to the dated minimum baseline plus the
implementation's entire declared portfolio; an implementation MUST NOT keep
an active source or controlled resource outside that portfolio to preserve a
coverage claim.

**REF-PORT-014:** An extension route MUST belong to exactly one core route
family and MUST declare why the core routes in that family would lose or
misstate information. Its profile MUST define component boundaries,
acquisition-mode applicability, operational record and processing bindings,
source-native value preservation, portable Rulespec or external-standard
bindings, conformance requirements, and migration behavior. Generic labels
such as `other`, `miscellaneous`, or `custom` MUST NOT be registered as
extension routes.

## 3. Terminology

The terms `rkaf:Artifact`, `rkaf:SourceFragment`, `rkaf:EvidenceBinding`,
`rkaf:RelationshipAssertion`, `rkaf:ValueAssertion`,
`rkaf:ConceptAssignment`, `rkaf:ExtractionActivity`, `rkaf:AILineage`,
`rkaf:ConfidenceRecord`, `rkaf:Attestation`, `rkaf:LocalAdoption`,
`rkaf:Warrant`, `rkaf:Authority`, `rkaf:LifecycleEvent`,
`rkaf:AccessScope`, `rkaf:RetentionPolicy`, `rkaf:RegisteredConcept`,
`rkaf:LocalConcept`, `rkaf:ConceptMapping`, and
`rkaf:ReferenceResourceRelease` have exactly the meanings defined by the
pinned Rulespec release. REF does not define aliases for them.

REF defines the following operational terms.

**Baseline enumeration report**
: An immutable, content-digested REF operational record that deterministically
  enumerates and classifies every declared-portfolio table data row plus every
  source-located named-item, subtype-group, and role occurrence inside cells
  and outside tables. It is the auditable input to an inventory coverage
  manifest, not a replacement copy of the inventories.

**Capture**
: The exact bytes or canonical response obtained from a source during one
  retrieval activity.

**Concept proposal**
: A source-grounded proposal awaiting vocabulary governance. It is a workflow
  record, not an `rkaf:LocalConcept`, `rkaf:RegisteredConcept`, or permissible
  `rkaf:ConceptAssignment` value.

**Enrichment decision**
: A durable record of an attempted enrichment, including its target, profile,
  policy, candidates considered, outcome, and any abstention or failure reason.
  It records workflow state; any portable semantic result is a separate
  Rulespec record.

**Deterministic payload**
: The canonical output content that a deterministic stage produces from fixed
  inputs and versions. It excludes declared run-instance provenance such as the
  new receipt identifier and execution time, while each run still preserves
  that provenance.

**Evidence address**
: An operational selector and rendition binding used to create or resolve an
  `rkaf:SourceFragment`. The address is not a second portable fragment type.

**Evidence-collection policy**
: A versioned rule that defines which sources, fragments, time range, retrieval
  methods, and materiality criteria a processor uses when collecting support,
  qualification, or contradiction for a candidate or assertion.

**Inventory coverage manifest**
: An immutable REF operational record that accounts for every coverage row,
  constituent row, and named portfolio item in the dated source and
  controlled-resource baseline through exhaustive routed components, while
  retaining definition rows in its pinned baseline-enumeration report and
  keeping route family, semantic route, acquisition mode, semantic/use mode,
  representability, adapter or import implementation, release inclusion, and
  rights/use authorization separate.

**External reference**
: An operational `externalReference` record-kind value for a pointer to a
  source resource, semantic resource, observation, model result, or identifier
  maintained outside the captured source corpus. It carries
  references to applicable Rulespec authority, access, and provenance records
  and does not copy the external object into the source corpus.

**Open-label role**
: An REF workflow designation for a grounded phrase that is published as an
  `rkaf:ValueAssertion` under the predicate pinned by the application profile.
  REF does not define an `OpenLabel` class.

**Output profile**
: An immutable, versioned policy that defines the facets, assignment roles,
  Rulespec reference-resource releases, registry import snapshots, mapping
  relations, open-label modes,
  acceptance policies, publication views, and other output choices a producer
  may use.

**Output-profile decision**
: An append-only operational record that stages, selects, deselects, replaces,
  fails, or rolls back deployment selection of an immutable output-profile
  version. Approval, revocation of approval, and authorization are recorded
  separately with Rulespec.

**Participation record**
: The REF `participation` record-kind route for a public comment, testimony,
  petition signature, or similar submission. It requires a separate privacy
  profile and is not a portable semantic class.

**Policy thread**
: A versioned, scoped view that groups records concerning an evolving
  real-world matter. Durable membership is an
  `rkaf:RelationshipAssertion`.

**Publication release manifest**
: An immutable REF `PublicationReleaseManifest` that identifies one published
  output set, its operational profiles and receipts, and its exact Rulespec and
  inventory-coverage pins. It is distinct from a Rulespec
  `rkaf:ReferenceResourceRelease`.

**Query-time association**
: A transient relevance, similarity, co-occurrence, ranking, or clustering
  result produced for a request. It is not a durable assertion.

**Registry import snapshot**
: An immutable REF operational record that connects one controlled-resource
  import to its `Capture` or explicit external reference, transformation,
  exclusions, validation, rights assessment, and applicable Rulespec release
  and distribution artifacts. It does not own retrieved bytes or repeat any
  capture, release, or artifact identity or digest.

**Rendition**
: The REF application role played by one `rkaf:Artifact` that represents a
  concrete immutable form of a source-resource version, such as XML, HTML,
  PDF, image, Office file, or extracted text. `Rendition` is not an REF class
  or second durable record.

**Rendition processing record**
: An REF operational record about parsing, extraction, optical character
  recognition, or quality for one rendition-role `rkaf:Artifact`. It references
  that artifact and does not repeat its identity or content digest.

**Rights assessment**
: An append-only operational assessment of observed source or registry terms
  for specified uses. Its evidence, review, authorization, access, and
  retention are represented using Rulespec and the external rights vocabulary
  selected by the binding profile.

**Semantic reference candidate**
: A workflow candidate for a grounded definition, requirement, obligation,
  exception, threshold, regulated population, program, mechanism, outcome,
  dataset, standard, policy problem, or other referable resource. Acceptance
  creates an externally typed resource plus Rulespec assertions; it does not
  create a generic REF semantic-object class.

**Semantic digest**
: A digest computed over a canonical deterministic payload under the declared
  serialization profile, excluding only that profile's run-instance
  provenance fields.

**Source-record revision**
: One decoded state of a source-native API or feed record. A changed record does
  not necessarily create a new source-resource version.

**Source-precedence policy**
: A versioned operational policy that selects among source observations for a
  named jurisdiction, record kind, field, predicate, and time range. Rulespec
  represents the source's warrant or authority and any approval of the policy.

**Source-field locator**
: A source-native field path and value digest used to create a Rulespec
  `SourceFragment` when support comes from structured data rather than prose.

**Source resource**
: The REF operational identity for a bounded source-issued communicative work,
  such as a rule, notice, report, filing, opinion, guidance document, or legal
  provision. It is not an `rkaf:Artifact`.

**Source-resource version**
: One publisher-recognized edition, revision, correction, or point-in-time
  state of a source resource. It groups one or more renditions and is not an
  `rkaf:Artifact`.

## 4. Rulespec dependency and conceptual model

### 4.1 Four layers

REF separates four layers:

```text
Layer 4  REF application products
         search, timelines, similarity, policy threads, release views

Layer 3  Rulespec semantic records
         assertions, assignments, evidence, attestations, authority, concepts

Layer 2  REF processing records
         source revisions, version resolution, candidates, decisions, receipts

Layer 1  Source evidence
         captures, source-native records, source-resource versions, renditions
```

Each higher layer depends on lower-layer evidence. No higher layer may rewrite
the source evidence that supports it. Layer 3 is not an REF-owned semantic
layer; it is a Rulespec-conforming output of REF processing.

### 4.2 One owner for each reusable semantic record

REF owns operational facts about acquisition, processing, evaluation, and
publication. Rulespec owns portable meaning and trust.

| Concern | Canonical owner |
| --- | --- |
| Capture, source-record revision, completeness, source-resource and version resolution | REF |
| Rendition processing state, selector resolution, run receipt, candidate and adjudication workflow | REF |
| Baseline enumeration, inventory coverage, source/reference-resource onboarding, and release-inclusion status | REF |
| Registry import snapshot, deployment selection, rollback, candidate index, and publication packaging | REF |
| Search, query-time similarity, policy-thread view, product explanation | REF |
| Immutable evidence artifact | Rulespec `rkaf:Artifact` |
| Addressable source region | Rulespec `rkaf:SourceFragment` |
| Proposition or relationship | Rulespec `rkaf:ValueAssertion` or `rkaf:RelationshipAssertion` |
| Concept assignment | Rulespec `rkaf:ConceptAssignment` |
| Evidence binding, extraction provenance, model lineage, confidence | Rulespec |
| Review, approval, dispute, and rejection | Rulespec `rkaf:Attestation` |
| Product authorization | Rulespec `rkaf:LocalAdoption` and `rkaf:usageEligibility` |
| Warrant, legal or source authority, lifecycle, access, and retention | Rulespec |
| Concepts, concept schemes, mappings, and concept resolution | Rulespec and SKOS as incorporated by Rulespec |

**REF-BIND-005:** A producer MUST NOT serialize an REF-owned substitute for a
Rulespec-owned concern in this table.

**REF-BIND-006:** An REF workflow MAY retain internal candidate data in a
provider-neutral operational record. If it exchanges that candidate as a
portable semantic record, it MUST use the applicable Rulespec type and
eligibility state.

**REF-BIND-007:** A review interface MUST write `rkaf:Attestation` records.
Product approval that authorizes use MUST additionally use
`rkaf:LocalAdoption`. REF MUST NOT store a mutable `reviewStatus`,
`authorityScope`, or equivalent field on a semantic record.

**REF-BIND-008:** Assertion construction origin, evidence, confidence, AI
lineage, epistemic basis, authority, lifecycle, access, retention, and use
eligibility MUST be represented only by the applicable Rulespec records and
properties. `rkaf:assertionOrigin` and `rkaf:epistemicBasis` MUST remain
independent.

### 4.3 Normative Rulespec profile

The [REF Rulespec Application Profile](regulatory-evidence-rulespec-profile.md)
is a normative dependency of this specification. It defines the concrete
projection from REF-owned records to Rulespec without restating Rulespec
definitions.

**REF-BIND-009:** Every REF `PublicationReleaseManifest` MUST pin:

- the REF version and operational serialization profile;
- the Rulespec semantic version;
- the immutable Rulespec release or Git commit identifier;
- the digest algorithm and digest of the exact Rulespec constraint bundle;
- every adopted Rulespec profile and claimed conformance level;
- the Rulespec validator and conformance-suite versions; and
- the machine-readable Rulespec validation result.

**REF-BIND-010:** A producer MUST validate Rulespec records with the pinned
Rulespec validator. An REF validator MAY invoke and report that validator; it
MUST NOT reimplement the Rulespec constraints as REF schemas.

**REF-BIND-011:** The rendition role MUST be played directly by one
`rkaf:Artifact`. REF MUST NOT create a parallel rendition object.
`SourceResource`, `SourceResourceVersion`, and
`RenditionProcessingRecord` remain REF operational records and MUST NOT also
be typed as `rkaf:Artifact`.

**REF-BIND-012:** A successfully published evidence address MUST create or
resolve one `rkaf:SourceFragment` whose `oa:hasSource` names the exact
rendition-role `rkaf:Artifact` and whose source and fragment digests satisfy the
pinned Rulespec profile. REF MUST NOT publish an `EvidenceFragment` class.

**REF-BIND-013:** An accepted durable relationship MUST be an
`rkaf:RelationshipAssertion`; an accepted literal proposition MUST be an
`rkaf:ValueAssertion`; and an accepted controlled-concept assignment MUST be
an `rkaf:ConceptAssignment`. Their review, adoption, evidence, lineage,
confidence, authority, and lifecycle MUST remain separate Rulespec records.

**REF-BIND-014:** REF processing and release records MAY point to Rulespec
records, and Rulespec provenance MAY point to REF run records. Neither side
MUST copy the other's canonical fields.

### 4.4 Upstream-first rule

The development compatibility target for this draft is Rulespec
`0.2.0-pre.8`. Because that release is under active development, the
application profile records its exact compatibility status and unresolved
upstream requirements. It does not claim an immutable final commit until that
commit exists.

**REF-BIND-015:** When REF needs reusable semantics that the pinned Rulespec
release cannot express, the project MUST add or clarify them in Rulespec or an
adopted external standard. It MUST NOT mint a competing REF primitive.

**REF-BIND-016:** Until an upstream requirement has landed and the binding
profile has a passing fixture, an REF `PublicationReleaseManifest` MUST retain
the fact as operational data, mark the semantic projection unsupported, and
exclude it from any conformance or product claim that depends on the missing
meaning.

## 5. Operational information model

### 5.1 Common record fields

**REF-CORE-008:** Every durable REF-owned operational record MUST contain:

| Field | Meaning |
| --- | --- |
| `id` | Stable identifier within a declared namespace |
| `type` | REF type or documented extension type |
| `recordedAt` | Time the REF record was first recorded |
| `recordedBy` | Agent or activity responsible for the record |
| `schemaVersion` | Version of the validating schema or profile |
| `operationalState` | Profile-defined processing or release state |

**REF-CORE-005:** Durable identifiers MUST NOT be reused for a different
record.

**REF-CORE-006:** A correction MUST create a new decision, version, or
superseding operational record. It MUST NOT silently replace historical state.

**REF-CORE-001:** REF operational state MUST describe workflow or publication
state only. It MUST NOT encode Rulespec assertion origin, attestation,
consumer lifecycle, authority, or use eligibility.

**REF-CORE-002:** An operational transition that affects a portable semantic
record MUST append the applicable Rulespec `Attestation`, `LocalAdoption`, or
`LifecycleEvent`; changing an REF workflow record is not a substitute.

**REF-CORE-003:** REF schemas MUST reference, not copy, the identifiers of
Rulespec semantic records used as inputs or outputs.

**REF-CORE-004:** REF implementation metadata MAY include provider-specific
details in access-controlled receipts. Rulespec records MUST remain
provider-neutral and use the pinned Rulespec provenance types.

**REF-CORE-007:** Processing status, publication status, registry deployment selection,
query-time persistence, and Rulespec consumer disposition MUST remain separate
dimensions.

### 5.2 Capture

A `Capture` records an acquisition attempt and its result.

Required fields are:

- source identifier;
- source locator, request method, and request parameters safe to retain;
- retrieval start and end time;
- response status;
- representation-relevant request and response headers, including content
  negotiation, ETag, and last-modified values when supplied;
- media type, when known;
- byte digest and digest algorithm, when bytes were obtained;
- byte length;
- storage reference or access-controlled inline content;
- acquisition activity and run receipt; and
- references to applicable Rulespec access and retention records and the
  external rights expression selected by the binding profile.

**REF-CAP-001:** A core producer MUST retain the exact obtained bytes or a
canonical response sufficient for byte-identical replay.

**REF-CAP-002:** A failed or partial acquisition MUST produce an explicit
failure or partial status. Empty content MUST NOT represent success.

**REF-CAP-003:** A capture digest MUST support fixity and duplicate detection.
It MUST NOT establish artifact identity by itself.

**REF-CAP-004:** A source connector MUST record pagination, cursor, window,
attachment, retry, exclusion, and completeness information applicable to the
source.

**REF-CAP-005:** A producer MUST preserve the obtained payload bytes before
decoding or normalization. When transport framing cannot be retained, the
capture MUST state that limit and preserve the exact application payload plus
the metadata needed to interpret it.

### 5.3 Source-record revision

A `SourceRecordRevision` contains one decoded source-native record and points to
the capture from which it came.

**REF-SRC-001:** A producer MUST preserve the source namespace, native
identifier, raw type, raw status, raw field names or a lossless raw payload,
and capture reference.

**REF-SRC-002:** Normalized values MUST appear beside source-native values and
MUST identify the mapping version.

**REF-SRC-003:** Unknown source values MUST remain unknown or enter an explicit
mapping queue. A producer MUST NOT coerce them to the nearest known value.

**REF-SRC-004:** A changed source-record revision MUST NOT automatically create
a new source-resource version.

**REF-SRC-005:** A source-precedence policy MUST govern conflicts among official
sources, mirrors, aggregators, and external indexes. A lower-authority record
MUST NOT silently overwrite a higher-authority source or erase the conflict.

**REF-SRC-006:** A `SourcePrecedencePolicy` MUST identify the source,
jurisdiction, record kinds, fields or predicates, precedence, effective
interval, rule, and version. The source's warrant or authority and the policy's
review and authorization for use MUST use Rulespec records.

**REF-SRC-007:** Each accepted source-aligned Rulespec assertion MUST link,
through the binding profile, to the applicable source-precedence policy or
state that no precedence policy exists.

**REF-SRC-008:** Selecting one of two conflicting Rulespec assertions for a current view
MUST preserve both Rulespec assertions, append the appropriate
`rkaf:Attestation` and, when authorized for product use,
`rkaf:LocalAdoption`, and identify the source-precedence policy used. The
losing assertion MUST remain available in history.

### 5.4 Source resource, source-resource version, and rendition

A `SourceResource` represents the operational source identity. A
`SourceResourceVersion` represents a source-issued state. One or more
`rkaf:Artifact` records play the rendition role for that version. REF does not
create a separate `Rendition` record.

**REF-ART-001:** A source resource MUST retain its source namespace and native
identifier. Cross-source identity MUST be represented as an
`rkaf:RelationshipAssertion`, not an overwrite.

**REF-ART-002:** A source-issued correction, edition, or point-in-time legal
state MUST remain a distinct source-resource version when the source treats it as
distinct.

**REF-ART-003:** XML, HTML, PDF, image, and extracted-text forms of one
source-resource version MUST be distinct `rkaf:Artifact` records in the
rendition role, not separate source-resource versions or parallel REF
rendition objects.

**REF-ART-004:** Each rendition-role artifact MUST record its immutable
identity, media type, digest, format relations, and applicable access and
retention references through Rulespec. REF MAY attach a
`RenditionProcessingRecord` containing source locator, byte length, extraction
state, parser version, and quality state; that record MUST reference the
artifact and MUST NOT copy its semantic identity or digest.

Core extraction states are:

- `fullNativeText`;
- `fullExtractedText`;
- `ocrText`;
- `abstractOnly`;
- `metadataOnly`;
- `unsupportedFormat`;
- `retrievalFailure`;
- `extractionFailure`; and
- `accessRestricted`.

**REF-ART-005:** A producer MUST expose extraction state through the
rendition-processing record. It MUST NOT make a metadata-only record appear
equivalent to full source text.

### 5.5 Evidence addressing and selector resolution

An `EvidenceAddress` is a transient REF operational input to Rulespec fragment
publication. A durable `SelectorResolution` records whether that address
resolved against one rendition-role `rkaf:Artifact` and whether a conforming
`rkaf:SourceFragment` was created or found.

The operational address and resolution record include:

- the rendition-role `rkaf:Artifact` identifier;
- selector type and selector value;
- extraction method and version;
- text or value digest;
- quoted text or source value when permitted; and
- resolution and quality status; and
- the resulting `rkaf:SourceFragment` identifier when successful.

The selector, quote, and digest values in an attempted address are processing
inputs, not a second portable source-fragment record.

**REF-EVID-005:** Selectors MAY use:

- a structured field path;
- source-native element or provision identifier;
- character offsets in a named text rendition;
- page and bounding region;
- table, row, column, and cell coordinates;
- media time range; or
- a documented compound selector.

**REF-EVID-001:** An evidence address MUST bind to one rendition-role
`rkaf:Artifact` and the selector resolution MUST verify that artifact's
Rulespec content digest.

**REF-EVID-002:** An extracted-text offset MUST NOT be presented as a source
PDF, image, HTML, or XML offset unless a verified mapping connects them.

**REF-EVID-003:** If an address no longer resolves, the producer MUST append an
unresolved or superseded selector-resolution record. It MUST NOT silently
retarget the address or its prior `rkaf:SourceFragment`.

**REF-EVID-004:** A compound package MUST remain intact unless the source
provides reliable component boundaries or a reviewed rule documents the split.

**REF-EVID-006:** On successful resolution, every overlapping selector,
coordinate, quote, source-artifact digest, and fragment digest in the REF
attempt MUST match the canonical `rkaf:SourceFragment`. After publication, the
Rulespec fragment is the only portable evidence address.

### 5.6 Source-processing record kinds

REF uses `recordKind` as an operational routing discriminator. These values are
not RDF classes and MUST NOT appear as portable semantic types:

| `recordKind` value | Examples | Processing default |
| --- | --- | --- |
| `container` | Docket, proceeding, case, hearing | No inherited subjects |
| `participation` | Comment, testimony, petition signature | Separate privacy profile |
| `entityRecord` | Agency, person, facility, program, chemical record | Entity-resolution route |
| `observationRecord` | Burden estimate, amount, status, measurement | Observation route |
| `eventRecord` | Publication, meeting, vote, decision, withdrawal | Event route |
| `externalReference` | External index, simulation, model result, identifier spine | Join or pointer, not captured source truth |

Portable objects use the applicable Rulespec profile, such as its US
rulemaking proceeding types, or an adopted external ontology.

An extension profile may add an absolute-IRI `recordKind` only when the core
processing paths would lose a material operational distinction. It must map
that value to the same common REF record requirements and to independently
selected portable semantics.

Inventory coverage routes map to processing as follows:

| Coverage route | REF processing route |
| --- | --- |
| `document` | `SourceResource` and `SourceResourceVersion`, with rendition-role artifacts |
| `participation` | `participation` |
| `container` | `container` |
| `entity` | `entityRecord` |
| `observation` | `observationRecord` |
| `event` | `eventRecord` |

Source acquisition modes map separately:

| Acquisition mode | REF processing |
| --- | --- |
| `captured` | `Capture` and the applicable decoded processing route |
| `externalJoin` | `externalReference`; a later retrieval requires a separate `Capture` |

**REF-TYPE-001:** A producer MUST determine the operational `recordKind`
before enrichment and MUST select a Rulespec or external semantic type
independently when it publishes a portable object.

**REF-TYPE-002:** A record MUST NOT become a source resource or Rulespec
artifact merely because it has a title, description, or text field.

**REF-TYPE-003:** A Rulespec concept assignment on one rendition artifact MUST
NOT propagate to its container, participants, entities, observations, later
source-resource versions, or related renditions without independent evidence
and an explicit derivation rule.

**REF-TYPE-004:** An `externalReference` operational record MUST identify the external system,
native identifier, version or observation time, authority, access and rights
state, and provenance. It MUST NOT be presented as a captured source resource
or imported result unless a separate capture records that material.

**REF-TYPE-005:** An implemented adapter MUST follow the route family,
semantic route, and acquisition mode declared by its inventory-coverage
component. A changed route family, route, or acquisition mode MUST create a
new coverage-manifest version and pass typing, rights, and regression review
before production use.

**REF-TYPE-006:** An extension route or `recordKind` MUST NOT bypass capture,
identity, version, evidence, provenance, rights, failure, publication, or
evaluation requirements that apply to its actual behavior. Its extension
profile MUST declare the applicable core requirements and any additional
requirements, and its validator MUST reject an instance whose declared
portable type or processing behavior conflicts with that profile.

### 5.7 Semantic-reference candidates

A `SemanticReferenceCandidate` lets a relationship workflow refer to a
possible definition, threshold, population, dataset, or policy mechanism
before the project decides how to type and publish that resource.

**REF-SEM-001:** A candidate MUST identify its proposed external type, wording,
originating source-resource version, evidence addresses, and generating
activity.

**REF-SEM-002:** A generated candidate MUST retain the generated wording and
the exact input evidence addresses.

**REF-SEM-003:** Two candidates MUST NOT be merged solely because their labels
or embeddings are similar.

**REF-SEM-004:** Acceptance MUST create an externally typed resource and the
applicable Rulespec assertions. REF MUST NOT publish a generic
`SemanticObject` class or duplicate a proposition in both an REF record and a
Rulespec assertion.

### 5.8 Rulespec semantic records

REF produces portable propositions only through the pinned Rulespec profile.

**REF-SEMOUT-001:** A durable derived semantic result MUST be the applicable
Rulespec assertion or assignment and MUST identify exact
`rkaf:SourceFragment` evidence or its Rulespec derivation inputs and
provenance.

**REF-SEMOUT-002:** An REF `EvidenceCollectionPolicy` MUST define the searched
evidence universe and materiality rules for an adjudication. The operational
decision MUST preserve every encountered conflicting or qualifying item that
meets the policy and link the resulting Rulespec records.

**REF-SEMOUT-003:** A numeric or categorical confidence attached to a semantic
result MUST use `rkaf:ConfidenceRecord`.

**REF-SEMOUT-004:** A source-aligned semantic result MUST identify the capture and
exact `rkaf:SourceFragment` in which the source expresses the proposition. It
MUST use Rulespec `rkaf:epistemicBasis: rkaf:sourceExplicit` independently of
the record's `rkaf:assertionOrigin`.

## 6. Identity, versions, and time

### 6.1 Identity

**REF-ID-001:** Source identity MUST begin with the source namespace and native
identifier, plus source-defined version information where required.

**REF-ID-002:** Shared text, title, RIN, docket number, citation, URL, or hash
MAY generate an identity candidate. None proves identity without the
applicable source rule or reviewed evidence.

**REF-ID-003:** A probabilistic identity match MUST remain a reversible
Rulespec relationship assertion or an operational candidate. It MUST NOT
replace either source record.

**REF-ID-004:** A system MAY publish a preferred display record. It MUST keep
all source identities, Rulespec assertions, attestations, and local adoptions
used to select that display record.

### 6.2 Version levels

REF distinguishes these version levels:

1. a new `Capture`;
2. a new `SourceRecordRevision`; and
3. a new `SourceResourceVersion`; and
4. a new immutable `rkaf:Artifact` in the rendition role.

**REF-VER-001:** Producers MUST represent these version levels separately.

**REF-VER-002:** A metadata refresh MUST NOT become a new legal or documentary
version unless source semantics support that conclusion.

**REF-VER-003:** Version, correction, replacement, withdrawal, amendment, and
supersession predicates MUST remain distinct.

### 6.3 Time

REF uses separate time dimensions:

| Time | Question answered | Canonical owner |
| --- | --- | --- |
| Publication or issuance time | When did the source issue this material? | Source-native metadata and adopted Rulespec domain profile |
| Valid time | When was an assertion or state valid in the represented world? | Rulespec applicability or adopted domain profile |
| Effective time | When did a legal or operational effect apply? | Rulespec effective period or adopted domain profile |
| Assertion or observation time | When did the source or asserting agent observe or assert the state? | Rulespec and PROV-O |
| Retrieval time | When did the framework obtain the source? | REF `Capture` |
| REF recorded time | When did the framework record the operational object? | REF operational record |

**REF-TIME-001:** A producer MUST NOT collapse these times into one generic
date when the source supplies more than one meaning, and REF MUST NOT duplicate
a Rulespec-owned time as independently authoritative operational state.

**REF-TIME-002:** Current views MUST be derived from retained history.

**REF-TIME-003:** An as-of query MUST declare whether it uses valid time,
effective time, observation time, retrieval time, framework-recorded time,
release-publication time, or a declared combination. The response MUST
distinguish "what was legally or operationally effective" from "what this
release knew or displayed."

**REF-TIME-004:** Deletion, disappearance, and access loss MUST create explicit
operational events or tombstones and the applicable Rulespec lifecycle and
access records. They MUST NOT erase earlier captures that retention and rights
policies permit the system to keep.

## 7. Evidence addressing and operational provenance

### 7.1 Evidence roles

Rulespec `rkaf:EvidenceBinding` owns both the evidence role and how that
evidence bears on an assertion. REF adjudication records may propose the
Rulespec evidentiary functions:

- `supports`;
- `qualifies`;
- `contradicts`;
- `definesScope`; and
- `providesContext`.

The accepted semantic result MUST publish those functions and the applicable
Rulespec evidence-role value on `rkaf:EvidenceBinding`; REF MUST NOT maintain a
parallel portable value set.

**REF-PROV-001:** Every accepted machine-generated assignment or durable
inferred relationship MUST cite at least one `rkaf:SourceFragment` or a
Rulespec-supported no-evidence reason.

**REF-PROV-002:** An adjudication record MUST retain qualifying or
contradicting evidence that met its declared evidence-collection policy at
decision time and link the resulting Rulespec evidence bindings.

**REF-PROV-003:** A producer MUST distinguish absent evidence from evidence of
absence.

### 7.2 Activities, agents, and receipts

Rulespec and PROV-O own portable activity and agent semantics. REF does not
define `Activity` or `Agent`. An REF run has an operational identifier and
links the applicable `prov:Activity`, `prov:Agent`, and
`rkaf:ExtractionActivity` records.

A `RunReceipt` contains:

- input captures, REF snapshots, and Rulespec release references;
- source and coverage window;
- references to the canonical Rulespec extraction activities, AI lineage,
  agents, reference-resource releases, and semantic outputs;
- provider-native request or response identifiers, retry history, cost,
  latency, and other operational details that Rulespec intentionally omits;
- environment or dependency lock reference;
- REF outputs and their digests;
- counts, exclusions, failures, and quarantined items;
- start and end times;
- reproducibility classification.

If a receipt snapshots a Rulespec value for audit convenience, that copy is
non-authoritative and MUST match the referenced Rulespec record.

**REF-PROV-004:** Every REF-derived durable operational record MUST identify
the activity and agent that produced it. Every portable semantic result MUST
use the applicable Rulespec extraction provenance and, when applicable,
`rkaf:AILineage`.

**REF-PROV-005:** A receipt MUST identify every nondeterministic stage.

**REF-PROV-006:** A receipt MUST NOT contain credentials, secrets, or protected
content unless it receives controls at least as strict as the source.

**REF-PROV-007:** A system MUST retain enough information to reproduce
deterministic stages from fixed inputs or to explain why replay is impossible.

### 7.3 Decision history

**REF-PROV-008:** Source-aligned extraction, deterministic processing,
model processing, human authorship, and review MUST remain distinguishable
through REF run records and the applicable Rulespec origin, extraction,
lineage, and attestation records.

**REF-PROV-009:** Rejection, dispute, correction, retraction, and supersession
of a semantic record MUST append the applicable Rulespec records.

**REF-PROV-010:** A consumer MUST be able to trace a current accepted semantic
result to its Rulespec assertion or assignment, source fragments, generating
activity, attestations, local adoption, lifecycle events, and earlier states.

## 8. Processing model

### 8.1 Required stage boundaries

**REF-PIPE-010:** A core producer MUST implement the following logical stages.
An implementation MAY combine physical services, but it MUST preserve each
stage's observable input and output.

```text
source registration
  → capture
  → decode and record typing
  → source-resource/version/rendition resolution
  → source-aligned parsing and Rulespec source fragments
  → deterministic identifiers, citations, and structure
  → optional semantic-reference and concept candidates
  → optional relation-specific adjudication
  → Rulespec validation
  → versioned REF publication
```

**REF-PIPE-001:** Each stage MUST append operational outputs or Rulespec
records. It MUST NOT overwrite source evidence or semantic history.

**REF-PIPE-002:** A failed stage MUST emit a typed failure, preserve usable
earlier outputs, and prevent incomplete results from appearing complete.

**REF-PIPE-003:** Deterministic stages MUST produce the same canonical payload,
stable payload identifier, and semantic digest for identical frozen inputs and
versions. Run receipts and run-instance records MAY differ in declared
provenance fields such as `recordedAt`, activity identifier, and execution time;
those fields MUST remain linked to the stable payload and MUST NOT enter its
semantic digest.

**REF-PIPE-004:** A nondeterministic stage MUST preserve its inputs, provider
and model identity, configuration, output, and execution time.

**REF-PIPE-005:** Materialization and publication MUST be separate decisions.
A completed processing run MUST NOT become a published release automatically.
Rulespec validation and required adoption MUST complete before semantic output
enters an accepted publication view.

### 8.2 Source registration

**REF-PIPE-011:** Before production acquisition, a source profile MUST define:

- responsible publisher and source authority;
- jurisdiction and coverage;
- access method and cadence;
- native identifiers and type values;
- source version, correction, deletion, and withdrawal semantics;
- pagination and completeness checks;
- body and attachment discovery;
- expected formats and parser policy;
- access, license, retention, and privacy rules; and
- source-specific validation fixtures.

**REF-PIPE-012:** Before its first production capture, a source profile MUST
have an immutable identifier, version, and digest; an approving decision and
agent represented through Rulespec attestation; and a `RightsAssessment` whose
adopted policy explicitly permits acquisition and storage for the declared
purpose.

**REF-PIPE-006:** A connector MUST fail visibly when schema drift, pagination,
access restrictions, or source limits prevent the declared coverage.

### 8.3 Publication

**REF-PIPE-007:** Publication MUST bind outputs to an immutable
`PublicationReleaseManifest` and run receipt. When semantic outputs are
included, the manifest MUST also carry the complete Rulespec pin and
conformance result required by `REF-BIND-009`.

**REF-PIPE-008:** Publication MUST be atomic or expose an explicit incomplete
release state that consumers cannot mistake for complete.

**REF-PIPE-009:** A publisher MUST support rollback to a previous release
without deleting the rejected release's history.

## 9. Semantic enrichment

### 9.1 Typed facets

REF separates semantic outputs by facet:

| Facet | Examples |
| --- | --- |
| General subject | Housing policy, air quality, workplace safety |
| Specialist subject | Clinical procedure, chemical process, aerospace technology |
| Entity | Organization, person, chemical, facility, program, place |
| Legal location | USC, Public Law, CFR, court citation |
| Industry classification | NAICS industry |
| Affected population | Regulated facilities, benefit recipients |
| Genre | Rule, guidance, complaint, report |
| Regulatory action | Proposes, amends, withdraws, decides |
| Administrative process stage | Unified Agenda stage, OIRA review stage, comment period |
| Code-list value | Source-native status or classification code |
| Ontology class | Class membership in a named ontology |
| Observation and measure | Amount, count, burden, modeled estimate |

**REF-ENR-001:** Subjects, entities, legal citations, industry
classifications, affected populations, genres, actions, process stages,
code-list values, ontology classes, and observations or measures MUST remain
distinct facets.

**REF-ENR-002:** A readable label on a code, identifier, schema element, or
ontology class MUST NOT make that value a subject concept.

**REF-ENR-003:** An enrichment profile MUST declare the facet IRIs, Rulespec
concept schemes, entity registries, and Rulespec assignment roles it may emit.

### 9.2 Open-set behavior

Valid enrichment results include:

- an accepted Rulespec concept assignment or entity assertion;
- an accepted grounded open-label value assertion;
- a review-required candidate;
- a local concept proposal awaiting governance;
- an abstention;
- a rejected candidate; or
- an unresolved conflict.

**REF-ENR-004:** A producer MUST support zero accepted assignments for a
rendition artifact or source fragment.

**REF-ENR-005:** A nearest or highest-scoring candidate MUST NOT be accepted
solely because it ranks first.

**REF-ENR-006:** A `ConceptProposal` MUST NOT be presented as an
`rkaf:LocalConcept`, `rkaf:RegisteredConcept`, accepted
`rkaf:ConceptAssignment`, or accepted open-label value assertion.

**REF-ENR-007:** An abstention MUST state one or more reasons:
`noCandidate`, `insufficientEvidence`, `belowPolicy`,
`conflictingEvidence`, `wrongFacet`, `outOfProfile`,
`licenseUnavailable`, `poorExtraction`, or `unsupportedRendition`.

**REF-ENR-008:** Every attempted combination of target, facet, and assignment
role MUST produce a durable `EnrichmentDecision`. The decision MUST record the
target, facet, assignment role, input snapshot, immutable output-profile
identifier, version, and digest, acceptance-policy release, candidate count,
outcome, result references, activity, and time.

Core decision outcomes are `accepted`, `reviewRequired`,
`localConceptProposed`, `abstained`, `failed`, and `cancelled`.

**REF-ENR-009:** A successful abstention, processing failure, cancelled run,
and unprocessed target MUST remain distinguishable. An empty assignment list
MUST NOT represent all four states.

**REF-ENR-010:** A `ConceptProposal` MUST have a stable operational identifier,
facet, wording, evidence addresses, generating activity, workflow state, and
supersession history. It MAY identify proposed anchors or mappings to Rulespec
concepts. Promotion MUST create a separate `rkaf:LocalConcept` or
`rkaf:RegisteredConcept`, the applicable `rkaf:Attestation`, and explicit
Rulespec provenance. REF MUST NOT copy Rulespec concept lifecycle into the
proposal record.

**REF-ENR-011:** An `OutputProfile` MUST have a stable identifier, immutable
version, and content digest. It MUST declare its permitted facets, assignment
roles, Rulespec reference-resource releases, registry import snapshots,
mapping relations, open-label modes, acceptance policies, and
publication views.

**REF-ENR-012:** An accepted open-label output MUST be an
`rkaf:ValueAssertion`. The Rulespec record set or an adopted standard in the
pinned Rulespec profile MUST preserve its exact wording, language and script
when known, evidence, and generating activity. Its linked REF
`EnrichmentDecision` MUST preserve the facet, stable local value identifier,
output profile, and workflow provenance. REF MAY retain detected language or
script as candidate-processing data, but that data MUST NOT be the only copy
of portable accepted meaning. Until the pinned Rulespec profile can represent
the required language and script, the producer MUST block that projection or
limit the output profile to a declared language-neutral or default-language
mode. The value assertion MUST NOT assert concept-scheme membership.

Core REF output-profile selection states are `staged`, `selected`,
`deselected`, and `failed`. Approval, revocation of approval, and authorization
for use are Rulespec attestations and local adoptions.

**REF-ENR-013:** Output-profile staging, selection, deselection, replacement,
and rollback MUST append an `OutputProfileDecision`.
The decision MUST identify the profile identifier, version, and digest;
selection state; effective and recorded times; reason; and predecessor or
superseding decision when applicable. Review and authorization MUST be linked
Rulespec records, not duplicated fields.

**REF-ENR-014:** An `accepted` enrichment decision MUST reference one or more
resulting Rulespec assertion or assignment identifiers. A
`localConceptProposed` decision MUST reference one or more resulting
`ConceptProposal` identifiers. A
`reviewRequired` decision MUST reference its review candidates. An
`abstained`, `failed`, or `cancelled` decision MUST reference no accepted
assignment and MUST record its applicable reason.

### 9.3 Candidate generation

Candidate generation and acceptance are separate activities.

**REF-CAND-008:** A profile MAY use:

- exact aliases and source labels;
- lexical retrieval;
- dense retrieval;
- grounded open phrases;
- source-assigned concepts;
- specialist schemes;
- hierarchy or mapping neighbors; and
- source, agency, CFR, genre, or other metadata priors.

**REF-CAND-001:** An enrichment producer MUST preserve each candidate's
generating channel, rank, and indexed representation version. It MUST preserve
the raw score when the channel produces one and an explicit no-score state
otherwise. A registered candidate MUST identify its scheme and release; an
open-label candidate MUST identify its stable local namespace or generating
activity.

**REF-CAND-002:** Candidate fusion and truncation MUST be deterministic for
fixed deterministic inputs and MUST be visible in the run receipt.

**REF-CAND-003:** Entity types and subjects MUST NOT compete in one
undifferentiated candidate ranking.

**REF-CAND-004:** Metadata conditioning MUST preserve a global candidate path
unless an evaluation has proved that a hard restriction preserves required
recall for the exact profile and release.

**REF-CAND-005:** A hierarchy prediction MUST NOT eliminate all descendants
without a measured recall-preserving fallback.

**REF-CAND-006:** A profile MUST treat shortlist size and channel quotas as
versioned policy, not framework constants.

**REF-CAND-007:** A generated phrase used for canonicalization MUST remain
available as evidence of what the generator proposed.

### 9.4 Acceptance policy

An `AcceptancePolicy` defines which candidates a producer may accept, which
require review, and which require abstention.

**REF-ACC-001:** Acceptance policy MUST be versioned independently of candidate
retrieval and adjudication models.

**REF-ACC-002:** Acceptance rules MUST be scoped by facet and output profile.
They SHOULD also account for source family, subtype, extraction quality, and
risk.

**REF-ACC-003:** A raw similarity score, reranker score, or model self-rating
MUST NOT be labeled a probability or calibrated confidence unless an identified
calibration method supports that interpretation.

**REF-ACC-004:** The acceptance policy MUST define which rejected and competing
candidates the decision record retains for explanation. The decision MUST
retain that set and the policy version.

**REF-ACC-005:** Machine agreement MUST NOT be represented as human review,
source assignment, or vocabulary promotion.

**REF-ACC-006:** A producer MUST NOT create new assignments to a deprecated
concept unless a declared historical profile permits them.

**REF-ACC-007:** Every accepted registered assignment MUST use a facet,
assignment-role predicate IRI, scheme or registry, Rulespec
reference-resource release, and any mapping-set `RegistryImportSnapshot`
selected by the output profile at the decision time. The assignment-role predicate MUST
be defined by the pinned Rulespec profile; REF MUST NOT mint a parallel role
value set. Every accepted open-label assignment MUST use a facet,
assignment-role predicate, and open-label mode authorized by the profile
selected at that time. Mapping-only resources, unauthorized releases, and
wrong-facet candidates MUST NOT enter the accepted view.

**REF-ACC-008:** A Rulespec concept assignment MUST use
`rkaf:assignedConceptRelease` to reference an
`rkaf:ReferenceResourceRelease` with a passing
`REF-Reference-Resource-Registry`
conformance manifest for the
applicable profile. The manifest's assessed resource identifier and version
MUST exactly match the referenced release, and its assessed content digest
MUST match that release's Rulespec `rkaf:referenceReleaseDigest`. The registry
MAY be operated by the enrichment producer or by an external conforming
registry. The referenced release MUST use `rkaf:completeMembership`; a partial
or non-enumerated release cannot prove that the assigned concept is a member.

### 9.5 Rulespec assignment publication

**REF-ASSIGN-004:** An accepted registered assignment MUST be an
`rkaf:ConceptAssignment`. An accepted open label MUST be an
`rkaf:ValueAssertion` under a predicate declared by the REF Rulespec
Application Profile. A concept assignment MUST target the exact member IRI in
the referenced `rkaf:ReferenceResourceRelease`, whose
`rkaf:membershipMode` MUST be `rkaf:completeMembership`. The REF
`EnrichmentDecision` MUST link the output to its output-profile version,
applicable registry import snapshots, acceptance-policy version,
candidate-generation activity, and run receipt; those workflow fields MUST NOT
be copied onto the Rulespec record.

**REF-ASSIGN-001:** Every accepted concept assignment's evidence MUST use
`rkaf:EvidenceBinding`. Each cited evidence reference MUST resolve to an
`rkaf:SourceFragment` bound to the exact rendition artifact and digest.

**REF-ASSIGN-002:** An assignment on one Rulespec artifact or source fragment
MUST NOT transfer to a related source resource, rendition, or later version
without independent evidence and a Rulespec-supported derivation.

**REF-ASSIGN-003:** A `ConceptProposal` MUST be represented by its enrichment
decision and governance workflow. It MUST NOT be used as the value of an
`rkaf:ConceptAssignment`.

## 10. Relationship discovery and publication

### 10.1 General model

REF separates operational candidates and query-time associations from accepted
durable semantic relationships. Only the latter are
`rkaf:RelationshipAssertion` records.

**REF-REL-015:** An accepted durable relationship MUST validate as an
`rkaf:RelationshipAssertion` under the pinned Rulespec release. Its evidence,
origin, extraction provenance, AI lineage, confidence, attestation, adoption,
authority, applicability, lifecycle, and access MUST use their canonical
Rulespec records. The REF candidate and adjudication decision MUST retain
method, snapshot, policy, outcome, and run-receipt details and link the
Rulespec result without copying those portable fields.

**REF-REL-001:** Every durable relationship MUST use a predicate from a
versioned predicate registry incorporated by the REF Rulespec Application
Profile or another adopted ontology.

**REF-REL-002:** A predicate definition MUST declare:

- subject and object types;
- direction;
- whether it is symmetric, asymmetric, transitive, or non-transitive;
- temporal meaning;
- material inverse, if any.

Those semantic traits MUST be defined by Rulespec or the adopted external
ontology. The REF publication policy for that predicate MUST separately
declare:

- evidence requirements;
- allowed Rulespec origins and required attestations or adoption;
- default persistence class;
- risk or review policy.

**REF-REL-003:** An implementation MUST NOT apply inverse, symmetric, or
transitive closure unless the predicate definition permits it.

**REF-REL-004:** A computed path or closure MUST remain query-time output unless
a new durable assertion independently meets its predicate's requirements.

### 10.2 Evidence, inference, and editorial families

Different processing routes produce different Rulespec origin, provenance,
attestation, and adoption records:

| Processing route | Portable representation |
| --- | --- |
| A source states a relation | Rulespec assertion plus source claimant, source fragment, evidence binding, and extraction provenance |
| A deterministic parser or join derives it | Rulespec deterministic origin plus extraction provenance and derivation inputs |
| A model proposes it | Rulespec AI-touched origin plus extraction activity and AI lineage |
| An analyst authors it | Rulespec human origin plus attestation |
| A team authorizes product use | Separate Rulespec local adoption |

**REF-REL-005:** A consumer MUST expose the Rulespec construction origin,
epistemic basis, evidence, attestations, and local adoption applicable to a
durable relationship.

**REF-REL-006:** Human review MAY attest to an inferred relationship, and a
local adoption MAY authorize product use. An implementation MUST NOT use
either record to rewrite the assertion's origin, epistemic basis, extraction
provenance, or AI lineage.

**REF-REL-007:** Shared identifiers, citations, timing, co-occurrence, and
similarity MAY propose relationships. They MUST NOT, without an applicable
source rule or further evidence, prove identity, dependency, causation,
amendment, supersession, or legal effect.

### 10.3 Similarity

Similarity is usually symmetric, continuous, dimension-specific, and
query-relative.

**REF-SIM-005:** A `SimilarityObservation` MUST identify:

- compared records, fragments, or query;
- comparison dimension;
- representation and input snapshot;
- algorithm or model and version;
- score, scale, and ranking context;
- evaluation time; and
- query or cache identifier.

Useful dimensions include subject, affected population, legal authority, legal
text, policy mechanism, intended outcome, procedure, operational dependency,
evidentiary role, contradiction, and temporal episode.

**REF-SIM-006:** A query MAY weight the declared similarity dimensions
differently for legal research, compliance, advocacy, program management, or
another declared task.

**REF-SIM-001:** A similarity observation MUST NOT satisfy a query for
dependency, identity, authority, amendment, supersession, causation, or legal
effect.

**REF-SIM-002:** General nearest-neighbor and topical-similarity results SHOULD
be computed at query time.

**REF-SIM-003:** Caching a similarity result MUST NOT change its persistence
class or authority.

**REF-SIM-004:** A system MAY promote a query-time result only by creating a
new `rkaf:RelationshipAssertion` with the evidence, provenance, attestation,
adoption, and predicate requirements of the pinned Rulespec and REF profiles.

### 10.4 Dependency

Dependency is directed and scoped. It states that one resource's
interpretation, validity, implementation, or operation materially relies on
another resource.

**REF-DEP-005:** An accepted dependency MUST be an
`rkaf:RelationshipAssertion` whose predicate identifies the dependency kind.
Its applicability identifies the affected scope, its Rulespec evidence
bindings preserve supporting and qualifying source fragments, and its
Rulespec temporal and provenance records preserve validity and construction.
When possible, its object SHOULD be the externally typed definition,
requirement, dataset, procedure, standard, or finding involved rather than a
whole-document proxy.

**REF-DEP-006:** Core dependency kinds MAY include:

- `dependsOnDefinition`;
- `dependsOnRequirement`;
- `dependsOnDataset`;
- `dependsOnProcedure`;
- `dependsOnStandard`;
- `dependsOnFinding`; and
- `operationallyImplements`.

**REF-DEP-001:** Dependency and similarity MUST be separate,
non-substitutable predicates.

**REF-DEP-002:** A confirmed document-level dependency MUST identify its scope
or point to the externally typed resource involved. A bare `dependsOn` edge is
insufficient.

**REF-DEP-003:** A dependency processor SHOULD apply this counterfactual test:
if the target changed, would the subject's interpretation, validity,
implementation, or operation materially change?

**REF-DEP-004:** A processor MUST ask a relation-specific question against
specific Rulespec source fragments. It MUST NOT confirm dependency from an
unconstrained "are these related?" judgment.

### 10.5 Candidate discovery and adjudication

**REF-REL-016:** Relationship candidate generation MAY use:

- shared official identifiers and citations;
- shared entities, programs, legal provisions, standards, datasets, or
  definitions;
- lexical passage retrieval;
- dense passage retrieval;
- extracted concepts or Rulespec assertions;
- source structure and lifecycle sequence; and
- temporal, agency, or jurisdictional priors.

**REF-REL-008:** Candidate generation MUST remain separate from relation
adjudication.

**REF-REL-009:** A relationship candidate MUST retain every generating channel
and the input snapshot.

**REF-REL-010:** Adjudication MUST evaluate one declared predicate or
predicate family at a time and return accepted, review-required, candidate,
rejected, disputed, or abstained.

**REF-REL-011:** An adjudicator MUST cite the exact
`rkaf:SourceFragment` used for each subject and object role. A structured
source field MUST first resolve to a conforming source fragment.

**REF-REL-012:** A model-generated relationship MUST be retractable and MUST
have a complete REF run receipt and Rulespec extraction and AI lineage. It MUST
be recomputable when its recorded
provider, model, configuration, inputs, and other required dependencies remain
available. Otherwise, the receipt MUST state the limitation and the producer
MUST NOT claim that the result can be regenerated.

### 10.6 Durable and query-time relationships

Good durable candidates include:

- source-explicit citations and lifecycle links;
- verified identity or equivalence;
- supported version lineage;
- scoped dependencies;
- accepted concept mappings;
- specified contradictions;
- uses of the same named dataset or standard; and
- approved policy-thread membership.

Good query-time candidates include:

- nearest neighbors;
- general topical similarity;
- weak co-occurrence;
- transient clusters;
- per-user relevance; and
- unreviewed heuristic associations.

**REF-REL-013:** Storage in a graph, index, cache, or table MUST NOT by itself
make a relationship durable.

**REF-REL-014:** A durable correction MUST use Rulespec supersession,
attestation, adoption, or lifecycle records as applicable. It MUST NOT rewrite
the earlier assertion in place.

### 10.7 Multi-hop reasoning

**REF-PATH-004:** A system MAY explain an indirect connection as a path, for
example:

```text
Artifact B → Program Y → Eligibility definition X ← Artifact A
```

**REF-PATH-001:** A derived path MUST identify every supporting Rulespec
assertion, predicate, attestation, adoption, and consumer lifecycle state.

**REF-PATH-002:** A path MUST NOT be presented as a direct relationship.

**REF-PATH-003:** A path evaluator MUST account for time, access controls,
retracted Rulespec assertions, predicate semantics, and maximum path length.

### 10.8 Absence and bounded negative search results

A missing edge can mean that no relationship exists, that the source omitted
it, that acquisition was incomplete, or that the processor failed to find it.
REF therefore represents negative search results as bounded operational
`AbsenceEvaluation` records, not assertions about the represented world.

**REF-ABS-004:** An `AbsenceEvaluation` MUST identify:

- the proposition or predicate not found;
- the searched corpus, source families, record kinds, and time range;
- the release and capture-completeness state;
- the search or derivation method and version;
- excluded, restricted, failed, and unprocessed material;
- the time of evaluation; and
- the evaluation activity and any Rulespec attestation of the evaluation.

**REF-ABS-001:** "No relationship found" MUST NOT be presented as "no
relationship exists" without a bounded absence evaluation and a completeness
rule that supports that conclusion.

**REF-ABS-002:** A later source acquisition, parser repair, registry release,
or method change MUST create a new absence evaluation. It MUST NOT silently
rewrite the earlier result.

**REF-ABS-003:** A query service MUST distinguish `notFound`,
`notApplicable`, `notProcessed`, `incompleteSource`, `restricted`, and
`processingFailed`.

## 11. Policy threads

A policy thread is an REF application view grouping source resources and
portable semantic records that concern a scoped, evolving matter. It avoids a
dense set of unsupported pairwise links and is not a new ontology class.

**REF-THR-007:** A durable `PolicyThread` MUST state:

- stable identifier;
- purpose and scope;
- supporting `rkaf:SourceFragment` identifiers;
- jurisdiction;
- temporal bounds;
- inclusion and exclusion rules;
- owner or responsible agent;
- version and operational state;
- membership method; and
- supersession history.

An ephemeral cluster is a query-time association, not a durable policy thread.
Review and approval of a durable thread use `rkaf:Attestation`; authorization
for product use uses `rkaf:LocalAdoption` over the thread's membership
assertions. Supersession uses Rulespec lifecycle records where applicable.

**REF-THR-001:** Each durable member MUST have a separate
`rkaf:RelationshipAssertion` using the profile's membership predicate, with
its own Rulespec provenance, evidence, applicability, attestation, and
adoption.

**REF-THR-002:** Membership MUST NOT imply identity, dependency, causation,
shared legal action, or agreement among all members.

**REF-THR-003:** A machine-generated cluster MUST remain a query-time
association until it has coherent scope, representative evidence, a Rulespec
attestation, and any required local adoption.

**REF-THR-004:** A record MAY belong to multiple competing or overlapping
threads.

**REF-THR-005:** Thread merge, split, retirement, and scope change MUST create
new thread versions and the applicable Rulespec attestations, relationship
assertions, and lifecycle events.

**REF-THR-006:** Each durable thread version MUST expose its supporting
Rulespec source fragments, membership assertions, attestations, local
adoptions, and supersession history. Approval MUST NOT rewrite the origin or
lineage of machine-generated membership.

## 12. Registry operations and concept governance

### 12.1 Resource kinds and scheme identity

REF distinguishes:

- subject thesauri and taxonomies;
- ontologies;
- identifier authorities;
- entity registries;
- code lists and classifications; and
- document or data schemas; and
- mapping sets.

The controlled-resource coverage routes are independently justified:

| Route | Distinguishing role |
| --- | --- |
| `subjectScheme` | Governed concepts intended for subject assignment or navigation |
| `ontology` | Formal classes, properties, axioms, or entailment rules |
| `identifierAuthority` | Governed identifiers whose primary role is resolving referents |
| `entityRegistry` | Governed entity records and attributes, not only identifier issuance |
| `codeList` | Enumerated operational values without broader classification meaning |
| `classification` | Codes that organize members into a governed classification |
| `schema` | Document, message, or data structure and validation rules |
| `mappingSet` | Governed cross-resource mapping statements |

Rulespec and SKOS own concept, scheme, status, and mapping meaning. Rulespec
owns portable release identity and membership for every managed reference
resource through `rkaf:ReferenceResourceRelease`; the release preserves its
version, resource kind, membership mode and any permitted membership claims,
distributions, and RDFC-1.0 semantic `rkaf:referenceReleaseDigest`.
`rkaf:completeMembership` and `rkaf:partialMembership` enumerate members;
`rkaf:membershipNotEnumerated` does not. Only complete membership can support
a concept-assignment or concept-mapping endpoint pin. Distribution
`rkaf:Artifact` records preserve their own byte digests. Rulespec keeps
`dcterms:type` open; these REF routes do not close or redefine its value set.

**REF-VOC-001:** Every Rulespec-owned `rkaf:LocalConcept`,
`rkaf:RegisteredConcept`, concept assignment, and concept mapping in a registry
payload MUST validate under the pinned Rulespec release. Native SKOS, OWL,
code-system, or schema distributions remain canonical for external resources;
their `rkaf:ReferenceResourceRelease` pins the distribution and declares its
membership mode. A complete-membership release pins its exact members, and
assignments target those member IRIs. REF MUST NOT define a
`ConceptVersion` or parallel semantic record. A `RegistryImportSnapshot`
records which immutable source snapshot contained the Rulespec or external
resource.

**REF-VOC-002:** The import and indexing pipeline MUST preserve distinct
Rulespec concept identifiers when labels are identical.

**REF-VOC-003:** A reference-resource import MUST preserve in its native
distribution all supplied notations or codes; preferred, alternate, and hidden
labels; language tags and scripts; definitions; scope, editorial, and history
notes; status; hierarchy; replacements; source mappings; source identifiers;
scheme membership; and other source notes that affect meaning. An
implementation MUST NOT claim that Rulespec `rkaf:LocalConcept` or
`rkaf:RegisteredConcept` constraints preserve those fields unless the exact
pinned Rulespec release supports them.

**REF-VOC-004:** An output profile MUST declare which schemes it may emit and
which schemes serve only retrieval, mapping, or search expansion.

**REF-VOC-005:** REF candidate generation MUST NOT treat a
Rulespec-incorporated SKOS broader or narrower link as a logical subclass
relation, legal fact, or automatic concept assignment.

### 12.2 Registry import and deployment

An REF `RegistryImportSnapshot` is the generic operational import record. It
connects the acquisition and transformation history for an external controlled
resource to its canonical release, but it does not own the acquired bytes or
the release. A retrieved input is one or more REF `Capture` records. A
non-retrieved input is an explicit external reference. Rulespec
`rkaf:ReferenceResourceRelease` owns the release identifier, version,
resource kind, membership mode and claims, distribution references, and
semantic `rkaf:referenceReleaseDigest` for each imported subject scheme, ontology,
identifier authority, entity registry, code list or classification, schema,
or mapping set. The release is the semantic manifest. Its distribution
`rkaf:Artifact` records retain their byte digests. REF does not mint a
competing release, version, digest, member list, or distribution description.

**REF-VOC-016:** A `RegistryImportSnapshot` MUST record:

- its inventory-coverage component and import profile;
- every REF `Capture` used for a retrieved input and every explicit external
  reference used for a non-retrieved input;
- the referenced `rkaf:ReferenceResourceRelease` and applicable distribution
  `rkaf:Artifact` records;
- import-time rights-assessment and adopted-policy references;
- transformation version;
- exclusions and failures;
- Rulespec and REF validation results;
- expected refresh cadence; and
- predecessor import snapshot, when applicable.

Source locator, retrieval time, obtained bytes, transport metadata, and
acquisition digest remain in `Capture`. Source identifiers, labels, and
semantic content remain in the native distribution and Rulespec release.
Observed license and permitted-use terms remain in `RightsAssessment` and the
adopted policy. The snapshot MUST NOT duplicate those values, the referenced
release's canonical identity, version, membership mode or claims,
distributions, or `rkaf:referenceReleaseDigest`, or the distribution
artifacts' canonical identities or byte digests, as independently
authoritative REF fields.

A `RegistryDeploymentDecision` records operational selection for a target
environment and output profile. Core states are `quarantined`, `staged`,
`selected`, `deselected`, and `failed`. Review and authority to use the release
are separate Rulespec attestations and local adoptions.

**REF-VOC-017:** A `RegistryDeploymentDecision` MUST identify the import
snapshot, target environment and output profile, selection state, effective
and recorded times, responsible activity, reason, applicable rights assessment
and adopted policy, and predecessor or superseding deployment decision when
applicable.

**REF-VOC-018:** A producer MUST compute current operational selection from
append-only `RegistryDeploymentDecision` records and permitted use from
applicable Rulespec access, retention, attestation, and local-adoption records
plus the adopted external rights expression. It MUST NOT mutate either the
import snapshot or Rulespec reference-resource release to represent deployment
or rights change.

**REF-VOC-006:** Each import MUST create an immutable import snapshot.

**REF-VOC-007:** A refresh MUST detect additions, removals, renames, hierarchy
changes, replacements, mapping changes, identifier reuse, publisher changes,
license changes, access changes, and permitted-use changes.

**REF-VOC-008:** Identifier reuse or unexplained deletion MUST fail closed.

**REF-VOC-009:** Historical `rkaf:ConceptAssignment` records MUST remain
resolvable against their referenced complete-membership
`rkaf:ReferenceResourceRelease`.

**REF-VOC-010:** A release MUST pass structural validation and declared
regression tests before deployment selection.

**REF-VOC-011:** Deployment selection MUST be atomic and rollback-capable. A
failed selection MUST leave the previous release logically selected. The producer
MAY continue using that Rulespec release only when its current adopted rights policy
permits the use. If those rights are revoked, conflicting, or unknown, the producer
MUST retain the release as restricted audit history and fail closed for the
affected use.

**REF-VOC-012:** Deployment selection MUST rebuild or invalidate every affected
candidate index, mapping view, acceptance cache, and export before the new
release enters an accepted view.

**REF-VOC-013:** A producer MUST retain import snapshots and receipts named by
quarantined and failed deployment decisions and MUST NOT expose their contents
as selected registry values.

**REF-VOC-014:** Ontologies, identifier authorities, entity registries, code
lists, classifications, schemas, and mapping imports MUST receive the same REF
snapshot, rights-assessment, refresh, historical-resolution, deployment, and
rollback controls as concept schemes. Each MUST reference its exact
`rkaf:ReferenceResourceRelease` and applicable distribution artifacts.
Concept assignments and mappings MUST use the Rulespec release-pin properties
defined for their roles and MUST pin only complete-membership releases. REF
MUST NOT copy the release version, membership mode or claims, distributions,
or semantic digest, or a distribution artifact's byte digest.

**REF-VOC-019:** A release for an identifier authority, schema authority, or
other resource whose members are not enumerated MUST use
`rkaf:membershipNotEnumerated` and pin the exact authoritative grammar,
resolver definition, or native content as a distribution and digest. It MUST
NOT assert `prov:hadMember`, support a concept assignment or mapping endpoint
pin, or serve as proof that an individual identifier was issued by that
authority.

**REF-VOC-020:** Every controlled-resource import, including a mapping set,
MUST use `RegistryImportSnapshot`. REF MUST NOT define or emit a separate
`MappingImportSnapshot` record or duplicate the generic snapshot's acquisition,
transformation, exclusion, validation, rights, or predecessor fields.

**REF-VOC-015:** A project-authored concept scheme MUST mint an immutable
scheme-native identifier when a concept proposal is promoted to a Rulespec
concept. It MUST NOT reuse the proposal identifier as a source-assigned
identifier from another scheme.

### 12.3 Cross-scheme mapping operations

Rulespec and SKOS own mapping relations and their semantic constraints. REF
uses the same `RegistryImportSnapshot` as every other controlled-resource
import, with resource route `mappingSet`. REF also owns deployment, indexing,
path recording, and rollback.

**REF-MAP-001:** Every published mapping MUST be an
`rkaf:ConceptMapping` that passes the pinned Rulespec validator. Its
mapping-set `RegistryImportSnapshot` MUST identify the exact source and target
`rkaf:ReferenceResourceRelease` records used to build that mapping payload.

**REF-MAP-002:** Lexical equality MUST NOT establish `exactMatch`.

**REF-MAP-003:** Mapping changes MUST follow the review, supersession, and
lifecycle rules in Rulespec and the REF immutable-snapshot and rollback
operations for registry changes.

**REF-MAP-004:** A mapping relation MUST NOT authorize inference by itself.
The output profile MUST declare which Rulespec relation, attestation decision,
local-adoption scope, and direction may support canonicalization or an accepted
assignment.
`closeMatch`, `broadMatch`, `narrowMatch`, and `relatedMatch` MUST NOT be
treated as `exactMatch`.

**REF-MAP-005:** An assignment produced through one or more mappings MUST
identify the mapping or ordered mapping path used. Every path element MUST
identify its `rkaf:ConceptMapping` identifier, relation, source and target
`rkaf:ReferenceResourceRelease` identifiers, and REF
mapping-set `RegistryImportSnapshot`.

**REF-MAP-006:** An REF query policy MAY define which Rulespec or SKOS mapping
relations it follows for candidate expansion, maximum path length, and
materialization. It MUST preserve the relation on every path element and MUST
NOT redefine SKOS inverse, symmetry, or entailment semantics. Operational path
expansion is not semantic transitive closure.

**REF-MAP-007:** Every imported `rkaf:ConceptMapping` MUST be traceable to one
immutable mapping-set `RegistryImportSnapshot`. Historical resolution MUST use
the exact mapping identifier and source and target Rulespec release pins
recorded by the REF enrichment decision, not the current mapping view.

### 12.4 Concept-proposal workflow

An REF registry workflow has concept proposals and published Rulespec concepts.
A concept proposal is not a semantic tier in the registry.

**REF-GOV-001:** Automated processing MUST NOT turn a concept proposal into an
`rkaf:LocalConcept` or `rkaf:RegisteredConcept`.

**REF-GOV-002:** Promotion MUST include:

- a definition;
- inclusion and exclusion cues;
- preferred and alternate labels;
- a proposed hierarchy position, top-concept declaration, nonhierarchical
  declaration, or documented not-applicable result;
- duplicate and mapping analysis;
- representative evidence that meets a versioned governance-policy rule;
- expected effect on existing assignments;
- rights review; and
- an `rkaf:Attestation` by the authorized concept-minting authority.

**REF-GOV-003:** Frequency MAY prioritize review. It MUST NOT establish meaning
or approval.

**REF-GOV-004:** Governance policy MUST name who may propose, map, approve,
deprecate, supersede, split, merge, and resolve disputes.

**REF-GOV-005:** A merge or split MUST preserve redirects, prior identifiers,
historical assignments, and an impact record.

**REF-GOV-006:** A promotion MUST NOT rewrite prior Rulespec assignment origin,
lineage, attestations, or adoption.

**REF-GOV-007:** Governance policy MUST define the evidence sufficiency for
promotion, including any exception for a new concept supported by one
authoritative rendition artifact. The framework MUST NOT infer sufficiency
from document count alone.

## 13. Publication and query behavior

### 13.1 Required views

**REF-QRY-010:** A query service claiming REF conformance MUST provide:

- immutable release metadata;
- current accepted view;
- assertion history;
- source and derived provenance;
- evidence resolution;
- as-of query behavior; and
- filters by Rulespec assertion origin, attestation, local adoption, consumer
  lifecycle, and usage eligibility.

**REF-QRY-001:** A response MUST expose the Rulespec origin, epistemic basis,
extraction provenance, AI lineage, attestations, and local adoption needed to distinguish
source-aligned extraction, deterministic processing, model suggestions, human
assertion, and authorized product use. It MUST NOT derive that distinction
from an REF-only basis field.

**REF-QRY-002:** A current view MUST retain links to applicable Rulespec
attestations, adoptions, supersession, lifecycle, and prior assertions.

**REF-QRY-003:** An as-of response MUST state its time semantics and data
release.

**REF-QRY-004:** Query-time associations MUST be labeled as query-time and MUST
identify their method and snapshot.

### 13.2 User-facing explanation

**REF-QRY-011:** For each inferred or editorial connection, a consumer-facing
service SHOULD answer:

> Why am I seeing this?

**REF-QRY-005:** A displayed inferred relationship MUST show its predicate,
Rulespec origin and lineage, attestations, local adoption, applicability, and
supporting source fragments.

**REF-QRY-006:** A user MUST be able to filter or exclude inferred and
editorial relationships.

**REF-QRY-007:** A service SHOULD let authorized users report, dispute, or
correct a connection without deleting the original assertion.

**REF-QRY-008:** A confidence number MUST NOT substitute for an explanation,
evidence, or provenance.

**REF-QRY-009:** A response containing source-aligned or conflict-resolved
assertions MUST expose the applicable source-precedence policy, Rulespec
authority or warrant, attestations, local adoption, and any unresolved
conflicting assertions.

### 13.3 Export

**REF-EXP-001:** An export MUST identify its REF version, profile, release,
serialization, extension namespaces, and complete Rulespec pin from
`REF-BIND-009`.

**REF-EXP-002:** An export MUST preserve stable identifiers, source-native
identifiers, source-precedence policies, evidence addresses, time semantics,
rights-assessment references, and supersession history. It MUST preserve
Rulespec records losslessly rather than copying origin, review, authority,
lifecycle, access, retention, or use fields into REF records.

**REF-EXP-003:** A public export MUST enforce the access and use restrictions
of every included record and derived assertion.

## 14. Privacy, security, rights, and safety

### 14.1 Access and derived disclosure

**REF-SEC-001:** Access and use controls MUST apply at capture,
source-resource, source-resource-version, rendition artifact, source-fragment,
Rulespec semantic-record, policy-thread, embedding, and export levels.

**REF-SEC-002:** A derived object MUST NOT weaken restrictions that apply to
its source evidence.

**REF-SEC-003:** A producer MUST evaluate whether summaries, embeddings,
entity links, inferred attributes, relationship paths, or graph neighborhoods
disclose protected information even when source text is hidden.

**REF-SEC-004:** Access checks MUST apply before relationship traversal and
evidence expansion, not only before final rendering.

Portable access, retention, and use eligibility use `rkaf:AccessScope`,
`rkaf:RetentionPolicy`, and `rkaf:usageEligibility`. Rights not covered by
those Rulespec records, including acquisition, indexing, model use, display,
redistribution, attribution, and permitted purpose, use the external rights
vocabulary pinned by the REF Rulespec Application Profile. REF does not define
a `UsePolicy`.

**REF-SEC-005:** A derived record with multiple inputs MUST compute an
operational effective-use decision from the canonical Rulespec and external
policy records: a denial overrides an
allowance, permitted audiences and purposes narrow to their common authorized
set, every compatible retention constraint applies, and all compatible
attribution duties accumulate. If retention constraints cannot all be
satisfied, the policies conflict under `REF-SEC-006`.

**REF-SEC-006:** If policies conflict or a required permission is unknown, the
derived record MUST fail closed for that use and record the conflict. An
authorized policy decision MAY resolve the conflict prospectively; it MUST NOT
rewrite the earlier source policies.

**REF-SEC-007:** A security and derived-disclosure evaluation MUST identify its
threat model, tested record and derivative types, methods, results, and
unresolved risks. Approval and authorization MUST use Rulespec attestation and
local adoption.

### 14.2 Public participation

Public comments and similar participation records can contain names, contact
details, health information, sensitive narratives, duplicated campaigns, and
content submitted by third parties.

**REF-PRIV-001:** A participation processor MUST use a separately versioned
profile, approved and adopted through Rulespec, covering purpose, minimization,
personally identifiable information,
sensitive-attribute inference, retention, deletion, entity resolution,
aggregation, reviewer access, and public release.

**REF-PRIV-002:** Participation records MUST NOT enter the general document
pipeline or public graph by default.

**REF-PRIV-003:** A participation profile MUST define whether and how deletion
or redaction requests affect captures, derived objects, embeddings, caches, and
published releases.

**REF-PRIV-004:** A participation profile MUST state a specific collection and
processing purpose, limit processing to the minimum data needed for that
purpose, define a retention period, and link the authorized privacy attestation
and local adoption.

**REF-PRIV-005:** Sensitive-attribute inference, cross-context entity
resolution, model training, and public release are prohibited unless the
approved profile separately authorizes the exact use, evidence, audience,
retention, and review controls.

### 14.3 Untrusted content and model safety

**REF-SAFE-001:** Source text, markup, metadata, and attachments MUST be treated
as untrusted data, not executable instructions.

**REF-SAFE-002:** File processing MUST use media sniffing, size and
decompression limits, parser isolation, malware controls, and resource limits
appropriate to the risk.

**REF-SAFE-003:** Model output MUST be schema-validated. It MUST NOT directly
authorize publication, concept promotion, identity merge, access change, or
other durable high-impact action.

**REF-SAFE-004:** A producer MUST prevent untrusted content from altering
system prompts, tool permissions, acceptance rules, or provenance records.

### 14.4 Rights and permitted use

**REF-RIGHTS-001:** A source or vocabulary review MUST decide acquisition,
storage, indexing, model use, redistribution, and display rights separately.
Permission for one use MUST NOT imply permission for another.

**REF-RIGHTS-002:** Every release MUST identify applicable attribution,
license, access, retention, and redistribution conditions.

**REF-RIGHTS-003:** When rights are unclear, a publisher MUST fail closed for
the unclear use while retaining the review decision.

**REF-RIGHTS-004:** A `RightsAssessment` MUST identify its target release or
source, the observed terms and supporting source fragments, proposed
acquisition, storage, indexing, model-use, display, redistribution, retention,
purpose, attribution, and audience permissions, effective and recorded times,
and any prior assessment. Its accepted policy MUST use the Rulespec and
external rights records named above; review and authorization MUST use
Rulespec attestation and local adoption. A rights change MUST append a new
assessment and policy records.

## 15. Validation and evaluation

### 15.1 Structural conformance

The REF validator tests operational records and cross-system references.
Rulespec's validator is the only validator of Rulespec semantic records.

**REF-TEST-002:** An REF validator MUST test:

- required fields and operational value sets;
- identifier stability;
- source-native value preservation;
- capture, source-record, source-resource-version, and rendition-role
  separation;
- evidence-address resolution to `rkaf:SourceFragment`;
- reference integrity between REF processing records and Rulespec records;
- run-receipt completeness;
- time semantics;
- registry and publication history;
- exact inventory-baseline digests, row accounting, coverage routes, and
  independent status dimensions;
- access-control enforcement;
- class-specific REF requirements; and
- presence and success of the exact pinned Rulespec validation report.

**REF-TEST-003:** Rulespec-owned field cardinalities, value sets, semantic
invariants, and behavior MUST be tested by the pinned Rulespec conformance
suite. REF fixtures MAY exercise them end to end but MUST NOT copy those
constraints into an REF schema.

### 15.2 Required negative tests

**REF-TEST-001:** A conformance suite MUST report each independently numbered
negative case below when its claimed classes or emitted features trigger that
case. It MUST mark every other case `notApplicable` under `REF-CONF-009`.

**REF-TEST-101:** Same-label concepts from different schemes MUST survive a
round trip as distinct concepts.

**REF-TEST-102:** A chemical entity MUST NOT silently become a policy subject.

**REF-TEST-103:** A label rename MUST NOT change concept identity.

**REF-TEST-104:** A deprecated concept MUST remain historically resolvable but
MUST receive no new assignment under a current profile.

**REF-TEST-105:** A no-fit passage MUST produce an abstention, an authorized
grounded open label, or a local concept proposal, not a forced nearest concept.

**REF-TEST-106:** An agency or CFR prior MUST NOT suppress the global
candidate path without a profile-specific recall gate.

**REF-TEST-107:** A concept assignment on one rendition artifact MUST NOT
propagate automatically to its source resource, docket, another rendition, or
a later source-resource version.

**REF-TEST-108:** A vocabulary refresh MUST NOT silently delete or reuse an
identifier.

**REF-TEST-109:** Source-aligned extraction, deterministic processing,
model-suggested output, human attestation, and local adoption MUST remain
distinguishable through REF and Rulespec records.

**REF-TEST-110:** Evidence MUST remain bound to its rendition digest.

**REF-TEST-111:** Similarity MUST NOT satisfy dependency or identity queries.

**REF-TEST-112:** Thread membership MUST NOT imply pairwise dependency.

**REF-TEST-113:** Caching MUST NOT promote a query-time association.

**REF-TEST-114:** Restricted evidence MUST NOT leak through a relationship
path or embedding.

**REF-TEST-115:** Replay MUST identify deterministic and nondeterministic
stages separately.

**REF-TEST-116:** Preferred labels in different languages and scripts MUST
survive without collision.

**REF-TEST-117:** `closeMatch`, `broadMatch`, `narrowMatch`, or `relatedMatch`
MUST NOT act as `exactMatch` without an explicit profile rule that preserves
its real relation.

**REF-TEST-118:** A mapping-only scheme, unauthorized release, or wrong-facet
value MUST NOT enter an accepted assignment view.

**REF-TEST-119:** Abstention, processing failure, cancellation, and no
attempted processing MUST remain distinct.

**REF-TEST-120:** After a failed registry deployment selection, the prior release MUST
remain logically selected and historical assignments MUST remain resolvable.
The release MUST remain usable only when its current adopted Rulespec and
external rights records permit that use.

**REF-TEST-121:** An external reference MUST NOT appear as captured source
material without a capture.

**REF-TEST-122:** `notFound` MUST NOT appear as proof of absence without a
bounded absence evaluation.

**REF-TEST-123:** A lower-precedence source MUST NOT silently replace a
conflicting Rulespec assertion selected under a higher-precedence source
policy.

**REF-TEST-124:** A derived record MUST NOT widen the audience, purpose, or
permitted use of any input policy.

**REF-TEST-125:** Registry selection, deselection, failure, and rights changes
MUST append REF operational records and applicable Rulespec records and MUST
NOT mutate the immutable `rkaf:ReferenceResourceRelease`, its distribution
artifacts, or the REF import snapshot.

**REF-TEST-126:** A historical assignment that used a mapping path MUST resolve
every exact `rkaf:ConceptMapping`, source and target
`rkaf:ReferenceResourceRelease`, and REF mapping-set
`RegistryImportSnapshot` used at its decision time.

**REF-TEST-127:** A staged, deselected, or failed output
profile MUST NOT be selected for a new accepted pipeline result. A selected
profile still requires the applicable Rulespec attestation and local adoption.

**REF-TEST-128:** Each enrichment outcome MUST satisfy the result-reference
cardinality in `REF-ENR-014`.

**REF-TEST-129:** A registered assignment MUST NOT enter an accepted view
without a passing registry conformance manifest for its referenced release.

**REF-TEST-130:** A source adapter MUST NOT perform a production capture
without an approved source profile and explicit acquisition and storage rights.

**REF-TEST-131:** A validator MUST accept every valid reference fixture and
reject every intentionally invalid reference fixture in its declared scope
with the applicable requirement identifier.

**REF-TEST-132:** Two deterministic replays over fixed inputs and versions MUST
produce identical canonical payload identifiers and semantic digests while
preserving each run's distinct provenance in linked receipts.

**REF-TEST-133:** A native REF export round trip MUST preserve all REF
operational records, Rulespec records, source-precedence policies,
rights-assessment references, and the exact Rulespec pin without copying
Rulespec semantic state into REF fields.

**REF-TEST-134:** A registry conformance manifest whose assessed identifier,
version, or `rkaf:referenceReleaseDigest` differs from the referenced
reference-resource release MUST NOT authorize a registered assignment.

**REF-TEST-135:** An inventory-coverage manifest with a missing, duplicate,
placeholder, unclassified, or unknown enumeration entry, required row account,
named constituent, role, or component, or with an account for a
`definitionRow`, MUST NOT satisfy complete portfolio accounting or
full-framework design coverage.

**REF-TEST-136:** `supported` representability or adapter implementation MUST
NOT imply release inclusion or rights/use authorization. A coverage entry
that omits a route family, semantic route, applicable source acquisition mode,
semantic/use mode, or any of the four status dimensions MUST fail validation.

**REF-TEST-137:** A `rightsBlocked` source or controlled resource MUST NOT
enter a production use whose authorization remains blocked.

**REF-TEST-138:** A completely accounted component whose representability
status is `planned`, `deferred`, `unsupportedWithReason`, or `notAssessed` MAY
pass portfolio accounting but MUST prevent a full-framework design-coverage
claim.

**REF-TEST-139:** A concept assignment or mapping endpoint that pins a
`rkaf:partialMembership` or `rkaf:membershipNotEnumerated` release MUST fail
validation even when the target IRI appears in a native distribution or a
partial member list.

**REF-TEST-140:** A conforming `rkaf:membershipNotEnumerated` release with
an exact authoritative grammar, resolver definition, or native content
distribution and digest, and with no `prov:hadMember` claims, MUST be
representable as an identifier or schema authority without inventing member
IRIs.

**REF-TEST-141:** A `RegistryImportSnapshot` that copies source bytes,
transport metadata, or an acquisition digest from `Capture`, or copies a
distribution artifact's identity or byte digest as independently authoritative
snapshot fields, MUST fail validation.

**REF-TEST-142:** A mapping-set import represented by a distinct
`MappingImportSnapshot` rather than the generic `RegistryImportSnapshot` MUST
fail validation.

**REF-TEST-143:** An inventory-coverage component whose rights/use status is
`supported` or `rightsBlocked` without references to the exact
`RightsAssessment` and applicable adopted Rulespec and external policy
evidence MUST fail validation. The status alone MUST NOT authorize use.

**REF-TEST-144:** The invoked Rulespec conformance path MUST recompute every
`rkaf:ReferenceResourceRelease` semantic digest and reject a wrong but
lexically valid `rkaf:referenceReleaseDigest`. An REF checksum comparison
without that upstream verification MUST NOT satisfy combined conformance.

**REF-TEST-145:** The portfolio-accounting validator MUST reject a baseline
enumeration that omits any table data row or named portfolio item, leaves one
unclassified, or marks a row that names a source, feed, reference spine,
external system, controlled resource, or distinct constituent as an
explanatory definition.

**REF-TEST-146:** A component marked with `supported` representability MUST
fail validation when its concrete representation mapping, positive fixture, or
round-trip fixture is missing, stale, lossy, or does not exercise every named
constituent and role. Aggregate fixtures that do not identify the covered
component MUST NOT satisfy `REF-PORT-012`.

**REF-TEST-147:** The portfolio-accounting validator MUST reject a compound
row or cell whose named feed, resource, subtype group, or semantic role has no
source-located enumeration occurrence and resolved component. It MUST also
reject a full-framework design-coverage claim whose pinned
`BaselineEnumerationReport` lacks a passing independent Rulespec audit
attestation.

**REF-TEST-148:** A newly onboarded item outside the two dated inventories MUST
fail complete portfolio accounting when it is active in an implementation but
absent from the implementation's pinned enumeration report and coverage
manifest.

**REF-TEST-149:** An extension route or `recordKind` MUST fail validation when
it uses a non-IRI or generic catch-all value; lacks a versioned extension
profile, core-route non-fit rationale, operational and portable bindings, or
required fixtures; or weakens an applicable REF or Rulespec requirement.

### 15.3 Evaluation corpus

Evaluation is distinct from schema conformance.

**REF-EVAL-001:** Before automated assignments or inferred relationships enter
an accepted production view, the publisher MUST use a frozen evaluation corpus
with:

- a development set and an untouched holdout;
- time-separated examples;
- source-family and subtype strata;
- record-kind and evidence-depth strata;
- rare, new, cross-domain, and no-fit cases;
- linked versions and renditions kept in the same split;
- independent review;
- frozen source, vocabulary, mapping, model, prompt, and policy versions; and
- a separate privacy-approved sample for participation records, if used.

**REF-EVAL-002:** Source-assigned labels MAY serve as source evidence or silver
labels within their actual scope. They MUST NOT be treated as universal gold
labels.

**REF-EVAL-003:** The implementer of a probabilistic component SHOULD NOT be
the sole owner of its sealed holdout or release decision.

### 15.4 Stage-specific measures

**REF-EVAL-010:** Evaluation MUST separate:

- capture and text coverage;
- extraction and optical character recognition quality;
- identity and deterministic-link precision and recall;
- registry coverage;
- candidate recall at declared shortlist sizes;
- final assignment precision and recall;
- strict matches from broader, narrower, related, or merely defensible matches;
- unsupported-assignment rate;
- correct abstention and risk-versus-coverage;
- cross-facet confusion;
- rare, emerging, and time-shifted topics;
- inferred-relation precision by predicate;
- reviewer time, disagreement, and correction rate;
- vocabulary-update stability;
- per-source and per-subtype worst-case performance;
- latency and cost; and
- product outcomes for search, alerts, browse, comparison, timelines, and
  cross-source joins.

**REF-EVAL-004:** A global average or composite score MUST NOT waive a failed
source family, facet, predicate, privacy profile, or high-risk use case.

**REF-EVAL-005:** Candidate recall MUST be evaluated before reranker or
adjudicator quality. A later stage cannot recover a missing candidate.

**REF-EVAL-006:** Correct abstention MUST count as a measured result, not
missing output.

### 15.5 Research hypotheses

The following remain hypotheses until the product holdout proves them:

- a low-thousands general subject layer is optimal;
- Federal Register and CRS concepts form the best product core;
- one product overlay improves on serving source schemes separately;
- facet-separated retrieval improves final quality;
- lexical and dense fusion beats either method for every source;
- open phrase generation followed by mapping beats direct assignment;
- metadata priors improve ranking without harmful leakage;
- specialist-module activation improves precision without recall loss;
- hierarchy expansion, definitions, aliases, or generated label text improve a
  given scheme;
- a language model or cross-encoder adds enough value to justify its cost;
- corpus-induced concepts improve user outcomes; and
- controlled concepts improve search, alerts, navigation, joins, or reporting
  enough to justify governance cost.

**REF-EVAL-007:** A production profile MUST keep these choices replaceable and
testable. It MUST NOT present them as conformance facts.

**REF-EVAL-008:** Before evaluation, a production profile MUST publish metric
definitions, thresholds, target universes, minimum sample sizes, uncertainty
or confidence-interval treatment, source and predicate strata, exclusion
rules, and the consequence of failure.

**REF-EVAL-009:** Once holdout results are revealed, that holdout MUST become
audit-only. Any model, mapping, registry, threshold, prompt, policy, or scope
change informed by those results MUST use a newly sealed holdout before a new
independent release claim.

## 16. Binding manifest and interoperability

The REF operational abstract model is normative. Each REF publication release
uses a concrete operational serialization profile and is identified by a
`PublicationReleaseManifest`. Portable semantic records use the serialization
and standards composition defined by the pinned Rulespec release and
[REF Rulespec Application Profile](regulatory-evidence-rulespec-profile.md).

**REF-INT-004:** REF implementations SHOULD preserve source-native standards
and use Rulespec's standards composition for portable semantics:

| Standard | Ownership |
| --- | --- |
| SKOS, PROV-O, Web Annotation, Dublin Core | Incorporated and constrained by Rulespec; REF does not remap them |
| DCAT 3 | Optional REF release-catalog export |
| W3C Organization Ontology | External organization typing when adopted by the Rulespec profile |
| USLM and Akoma Ntoso | Source-native legal-document structure |

**REF-INT-001:** The application profile MUST document each REF-to-Rulespec
projection, every operational field intentionally not projected, and every
blocked projection awaiting an upstream Rulespec change.

**REF-INT-002:** An export mapping MUST NOT collapse capture,
source-record-revision, source-resource, source-resource-version, and rendition
roles. It MUST preserve Rulespec records as Rulespec records without
re-encoding their origin, evidence, review, authority, lifecycle, access,
retention, or use semantics in REF.

**REF-INT-003:** Source-native structure SHOULD remain authoritative when an
official source supplies it. Interoperability mappings SHOULD supplement, not
replace, that structure.

## 17. References

### 17.1 Normative references

- [RFC 2119 — Key words for use in RFCs to Indicate Requirement Levels](https://www.rfc-editor.org/rfc/rfc2119)
- [RFC 8174 — Ambiguity of Uppercase vs Lowercase in RFC 2119 Key Words](https://www.rfc-editor.org/rfc/rfc8174)
- [Rulespec](https://github.com/Formspec-Labs/rulespec)
- [REF Rulespec Application Profile](regulatory-evidence-rulespec-profile.md)
- [Source and Document Type Matrix, 28 July 2026](source-document-type-matrix-2026-07-28.md), for its enumerated row universe only
- [Source Vocabulary, Ontology, Thesaurus, and Authority Catalog, 28 July 2026](source-vocabulary-ontology-thesaurus-catalog-2026-07-28.md), for its enumerated resource universe only

### 17.2 Informative standards

- [SKOS Simple Knowledge Organization System Reference](https://www.w3.org/TR/skos-reference/)
- [PROV-O: The PROV Ontology](https://www.w3.org/TR/prov-o/)
- [Web Annotation Data Model](https://www.w3.org/TR/annotation-model/)
- [Data Catalog Vocabulary 3](https://www.w3.org/TR/vocab-dcat-3/)
- [Dublin Core Metadata Terms](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/)
- [W3C Organization Ontology](https://www.w3.org/TR/vocab-org/)

### 17.3 Project evidence

- [Industry and LLM-era large-label-space tagging](evidence/blind-external-research-recovery-2026-07-28/01-industry-and-llm-era-large-label-space-tagging.md)
- [Extreme multilabel classification](evidence/blind-external-research-recovery-2026-07-28/02-extreme-multilabel-classification.md)
- [Taxonomy induction](evidence/blind-external-research-recovery-2026-07-28/03-taxonomy-induction.md)
- [Label text and embedding geometry](evidence/blind-external-research-recovery-2026-07-28/04-label-text-and-embedding-geometry.md)
- [Controlled-vocabulary scoping](evidence/blind-external-research-recovery-2026-07-28/05-controlled-vocabulary-scoping.md)
- [Source partitioning and metadata priors](evidence/blind-external-research-recovery-2026-07-28/06-source-partitioning-and-metadata-priors.md)
- [US federal controlled vocabularies](evidence/blind-external-research-recovery-2026-07-28/07-us-federal-controlled-vocabularies.md)
- [Corpus-driven vocabulary development](evidence/blind-external-research-recovery-2026-07-28/08-corpus-driven-vocabulary-development.md)
- [When to Abandon a Controlled Vocabulary](evidence/blind-external-research-recovery-2026-07-28/when-to-abandon-controlled-vocabulary-and-federal-vocabulary-inventory.md)

## Appendix A: Example operational and Rulespec records

This informative outline shows the ownership split for an accepted inferred
dependency. Exact Rulespec shapes come only from the pinned release.

```text
REF RelationshipAdjudicationDecision
  id: urn:ref:adjudication:9b31
  candidate: urn:ref:relationship-candidate:9b31
  inputSnapshot: urn:ref:snapshot:2026-07-28
  evidenceCollectionPolicy: urn:ref:evidence-policy:dependency-v2
  outputProfile: urn:ref:output-profile:relationships:v3
  outcome: accepted
  result: urn:rkaf:assertion:9b31
  runReceipt: urn:ref:run:relationship-2026-07-28

Rulespec record set
  rkaf:RelationshipAssertion: urn:rkaf:assertion:9b31
  rkaf:Artifact:              urn:rkaf:artifact:guidance-b-html-sha256
  rkaf:SourceFragment:        urn:rkaf:fragment:guidance-b:p14
  rkaf:EvidenceBinding:       urn:rkaf:evidence-binding:9b31
  rkaf:ExtractionActivity:    urn:rkaf:extraction:9b31
  rkaf:AILineage:             urn:rkaf:ai-lineage:9b31
  rkaf:ConfidenceRecord:      urn:rkaf:confidence:9b31
  rkaf:Attestation:           urn:rkaf:attestation:9b31
  rkaf:LocalAdoption:         urn:rkaf:adoption:9b31
```

The REF record explains the run, policy, and outcome. The Rulespec records
carry the proposition, source regions, derivation, review, and authorization.
Neither record set copies the other's canonical fields.

## Appendix B: Relationship predicate ownership

Relationship predicate IRIs and their definitions, domains, ranges, direction,
inverse, symmetry, transitivity, and temporal meaning belong in the Rulespec
regulatory-evidence profile or an adopted external ontology. The
[REF Rulespec Application Profile](regulatory-evidence-rulespec-profile.md)
only enumerates and pins the adopted predicates and defines REF candidate
persistence, materiality, review, evaluation, and publication policy. REF does
not duplicate the canonical predicate inventory.

## Appendix C: Requirement index

The requirement prefixes identify the area under test:

| Prefix | Area |
| --- | --- |
| `REF-CONF` | Conformance and extension behavior |
| `REF-BIND` | Rulespec dependency, pinning, and ownership boundary |
| `REF-PORT` | Full-inventory accounting and design coverage |
| `REF-CORE` | Common REF operational records and semantic-boundary rules |
| `REF-CAP` | Capture and completeness |
| `REF-SRC` | Source-record revision and normalization |
| `REF-ART` | Source resources, versions, rendition processing, and Rulespec artifact binding |
| `REF-EVID` | Evidence addressing and Rulespec source-fragment resolution |
| `REF-TYPE` | Operational record-kind routing |
| `REF-SEM` | Semantic-reference candidates |
| `REF-SEMOUT` | Rulespec semantic-result publication |
| `REF-ID`, `REF-VER`, `REF-TIME` | Identity, versions, and time |
| `REF-PROV` | Run receipts and Rulespec provenance linkage |
| `REF-PIPE` | Processing and publication |
| `REF-ENR`, `REF-CAND`, `REF-ACC`, `REF-ASSIGN` | Enrichment |
| `REF-REL`, `REF-SIM`, `REF-DEP`, `REF-PATH`, `REF-ABS` | Relationship workflow, query associations, and bounded absence |
| `REF-THR` | Policy threads |
| `REF-VOC`, `REF-MAP`, `REF-GOV` | Reference-resource import and deployment, mappings, and concept workflow |
| `REF-QRY`, `REF-EXP` | Queries and export |
| `REF-SEC`, `REF-PRIV`, `REF-SAFE`, `REF-RIGHTS` | Privacy, security, and rights |
| `REF-TEST`, `REF-EVAL` | Conformance tests and evaluation |
| `REF-INT` | Interoperability |
