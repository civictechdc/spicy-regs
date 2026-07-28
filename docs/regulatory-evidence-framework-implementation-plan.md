<!-- markdownlint-disable MD013 -->

# Regulatory Evidence Framework implementation plan

> **Status:** Rulespec-dependent delivery plan
>
> **Date:** 2026-07-28
>
> **Normative specification:** [Regulatory Evidence Framework 1.0](regulatory-evidence-framework.md)
>
> **Normative semantic binding:** [REF Rulespec Application Profile](regulatory-evidence-rulespec-profile.md)
>
> **Planning basis:** Source inventories and recovered external research; current designs and proposals excluded

## Result

Deliver the Regulatory Evidence Framework (REF) through four staged releases:

1. **Evidence release:** exact captures, typed records, source-issued history,
   Rulespec artifacts and source fragments, official identifiers, explicit
   Rulespec assertions, and point-in-time queries.
2. **Typed enrichment:** REF candidates and decisions, Rulespec concepts and
   assignments, review through attestations, and governed registry deployment.
3. **Approved inferred relationships:** only predicates that pass their
   independent gates.
4. **Optional policy threads:** durable editorial groupings only when their
   separate product and governance gate passes.

Automated tagging is not a dependency for the evidence release. The first
useful product must answer a complete federal rule-history question, show the
source evidence for every answer, and reproduce what the system would have
shown on a past date.

Every release also carries a versioned inventory-coverage manifest for the
complete row and resource universe in the two dated planning inventories plus
every additional inventory or item declared by that implementation. The dated
inventories are the minimum breadth test, not the framework's outer limit. A
release must disposition every table data row and named portfolio item in a
pinned baseline-enumeration report, create exactly the required coverage
accounts, decompose compound items into exhaustively routed components, and
report source acquisition mode, semantic/use mode, representability, adapter
or import implementation, release inclusion, and rights/use authorization
separately. Definition rows stay visible but require no route. This does not
require every component to be ingested or included in a release.

This plan uses exit gates instead of calendar promises. The team should estimate
dates after Work Package 0 fixes the product questions, source scope, staffing,
and service levels.

## 1. Delivery principles

The implementation follows eight rules:

1. Build one complete vertical slice before adding source families.
2. Keep original captures and canonical history independent from search,
   vector, and graph indexes.
3. Keep deterministic and probabilistic work on separate release paths.
4. Measure each pipeline stage before measuring end-to-end quality.
5. Publish only immutable, receipted releases.
6. Treat every unproved architecture choice as an experiment with a stop rule.
7. Put every reusable semantic type and invariant in Rulespec; REF owns only
   acquisition, processing, evaluation, and application behavior.
8. Treat the dated inventories as the first conformance corpus, not a closed
   type registry; onboard new kinds through governed, fixture-backed extension
   profiles without catch-all buckets.

The clean-slate implementation should use:

- immutable object storage for captures and source renditions;
- append-only REF records for operational history and Rulespec records for
  portable semantic history;
- project-owned REF schemas and interfaces for operational records only;
- thin, source-specific adapters;
- rebuildable lexical, vector, and graph views;
- a review and governance service that writes Rulespec attestations, local
  adoptions, concepts, mappings, and lifecycle events; and
- an REF validator that invokes, without reimplementing, the pinned Rulespec
  validator.

Vendor and package choices remain open until the responsible work package
evaluates maintained packages against the actual requirement. A chosen package
should cover most of the need behind a thin project-owned adapter. Storage or
index technology must not become the only source of canonical truth.

## 2. Releases and boundaries

### 2.1 Release 1: Federal Final-Rule Evidence Trail

Release 1 proves:

- source acquisition and completeness;
- source-resource and source-resource-version grouping, operational record-kind
  routing, and rendition-role `rkaf:Artifact` records;
- exact evidence addressing resolved to `rkaf:SourceFragment`;
- source-native and normalized type preservation;
- corrections, withdrawals, effective dates, and point-in-time history;
- official identifier and citation links;
- deterministic replay;
- current and as-of query behavior; and
- evidence packages containing validated Rulespec records that an independent
  analyst can verify.

Release 1 may expose source-assigned topics only as raw source-native fields.
Presenting them as semantic concepts or assignments requires a conforming
`rkaf:ReferenceResourceRelease`, `rkaf:epistemicBasis`, Rulespec source-fragment
evidence, and a release-pinned `rkaf:ConceptAssignment`. Release 1 may expose
machine suggestions behind a development flag.
Those suggestions remain non-release experimental output until WP7
conformance and evaluation pass. Release 1 must not depend on automated topic
acceptance, inferred dependency publication, or policy-thread automation.

Release 1 claims `REF-Core-Producer`, the explicit and deterministic profile of
`REF-Relationship-Producer`, and an evidence-only `REF-Query-Service` profile.
The query profile includes immutable, current, historical, evidence, as-of, and
epistemic-basis-filtered views. It emits no query-time semantic associations.
Each claimed profile is an immutable, versioned deliverable rather than an
informal feature description.

Release 1 also passes the applicable Rulespec L3 conformance and pins the exact
Rulespec version, revision, constraint digest, profiles, validator, and result.
Any current-view behavior that relies on local adoption, lifecycle reduction,
or usage eligibility additionally passes Rulespec L4.

### 2.2 Release 2A: Typed enrichment

Release 2A adds:

- grounded semantic-reference candidates that publish externally typed
  resources only after acceptance;
- typed subject and entity candidates;
- open labels and abstention;
- REF registry import snapshots and deployment decisions around Rulespec
  reference-resource releases and mappings;
- Rulespec concept assignments, attestations, local adoptions, and dispute
  history; and
- product-level search, browse, alert, and comparison evaluation.

It claims the approved `REF-Enrichment-Producer` and
`REF-Reference-Resource-Registry` operational profiles and the applicable
Rulespec reference-resource conformance, including concept-specific
requirements when concept payloads are present. It can ship even if no inferred
relationship or policy-thread experiment passes.

### 2.3 Release 2B: Approved inferred relationships

Release 2B adds only the inferred predicates that independently pass their
predicate-specific gates and exist in an adopted Rulespec regulatory-evidence
profile or external ontology. Accepted durable outputs are
`rkaf:RelationshipAssertion` records; multidimensional similarity remains
query-time. A failed predicate remains review-required or query-time and does
not block predicates that pass.

### 2.4 Release 2C: Optional policy threads

Release 2C adds versioned policy threads only if the thread experiment proves
coherence, scope stability, governance capacity, and user value. Failure
removes the `REF-Policy-Thread-Publisher` claim; it does not block Releases 2A
or 2B. Durable membership is a Rulespec relationship assertion, and review and
authorization use Rulespec attestation and local adoption.

### 2.5 Deferred from all release gates

The initial releases do not require:

- adapter implementation, ingestion, or release inclusion for every
  inventoried federal source or controlled resource;
- public comment classification;
- automatic legal conclusions;
- a universal subject vocabulary;
- automatic concept promotion;
- automatic cross-source identity merging;
- every possible relationship predicate;
- a dedicated graph database; or
- state and local expansion in the initial releases; the extension mechanism
  still has to represent it without a core redesign.

Complete inventory accounting is not deferred. Rows outside the active
delivery scope remain explicit through routed components, semantic/use modes
such as `mappingOnly` or `externalReference`, and dimension-specific
`planned`, `deferred`, `rightsBlocked`, `unsupportedWithReason`,
`notApplicable`, or `notAssessed` statuses in the versioned coverage manifest.
Any non-`supported` representability status blocks `G1` and a
full-framework design-coverage claim, but it does not make the inventory row
disappear.

## 3. First vertical slice

### 3.1 Product question

The first slice answers:

> For one federal regulatory action, what did the agency propose, what did it
> finally adopt, which materials support that answer, what legal provisions
> changed, when did those changes take effect, and what did the record show on
> a specified past date?

### 3.2 Inputs

Use official sources for:

- Unified Agenda edition records and Regulation Identifier Numbers;
- Office of Information and Regulatory Affairs review records when present;
- Federal Register public-inspection and published XML, HTML, and PDF;
- Regulations.gov dockets, agency documents, and complete attachments;
- eCFR or GovInfo point-in-time text before and after an amendment;
- Congressional Review Act records when applicable; and
- official United States Code, Public Law, Executive Order, or other cited
  authority.

Comments may enter only as protected `ParticipationRecord` fixtures needed to
prove the boundary. Release 1 does not classify comments or publish participant
entity links.

### 3.3 Selection rule

Before implementation tuning, publish a rule for selecting three matters:

1. A normal single-agency proposed-to-final rule with a RIN, docket, and eCFR
   amendment.
2. A matter with a correction, withdrawal, replacement, or delayed effective
   date.
3. A joint-agency, multi-docket, multipart-CFR, or
   extraction-challenging matter.

The combined slice must include:

- a public-inspection version;
- more than one rendition of one published version;
- a supporting attachment;
- a scanned or otherwise difficult file;
- an unknown or anomalous source type or identifier;
- a legal citation;
- a provision-level before-and-after change; and
- an explicit incomplete, conflicting, or unresolved item.

Selection belongs to the independent evaluation lead and regulatory-domain
owner, not solely to implementers.

### 3.4 What happens

```text
capture official bytes
  → reconcile coverage
  → type each record
  → resolve source-resource versions and rendition-role Rulespec artifacts
  → preserve source structure
  → resolve exact Rulespec source fragments
  → extract official identifiers and citations
  → build deterministic Rulespec assertions for timelines and legal links
  → validate the pinned Rulespec graph
  → publish current, historical, and evidence views
```

### 3.5 Outputs

For each matter, publish:

- capture manifest and completeness ledger;
- typed record inventory;
- source-resource-version history and rendition-role Rulespec artifacts;
- source structure, selector-resolution records, and Rulespec source fragments;
- process, publication, and effective-date timeline;
- docket and attachment relationships;
- cited-authority relationships;
- point-in-time legal text and deterministic diff;
- Rulespec assertions with separate epistemic basis, construction origin,
  attestation, adoption, and unresolved state;
- current and as-of query results; and
- one REF `PublicationReleaseManifest` and run receipt that binds inputs, operational
  configuration, canonical Rulespec provenance records, mappings, outputs,
  exclusions, failures, and the exact Rulespec pin and validation result.

### 3.6 Slice exit gate

Release 1 passes only when:

- every selected source window reconciles or declares a specific gap;
- every discovered attachment reconciles to captured bytes or a typed
  retrieval, access, or exclusion failure, and every captured attachment has
  an extraction state;
- every published Rulespec assertion resolves to captured
  `rkaf:SourceFragment` evidence or a Rulespec-supported deterministic
  derivation over identified source fields;
- the independent gold review finds zero false identity merges;
- every expected version, correction, withdrawal, and legal-history link is
  correct;
- two complete deterministic replays produce identical deterministic outputs;
- as-of answers match the independently prepared timeline;
- all missing, unsupported, restricted, and failed items appear explicitly;
- a restore exercise reconstructs the release; and
- an independent analyst can reproduce each answer from the release and
  receipts.

## 4. Program gates

| Gate | Decision | Required evidence |
| --- | --- | --- |
| `G0 — Scope` | Is the first slice worth building and bounded? | Approved product questions, source list, non-goals, authority rules, privacy boundary, metric definitions, and complete baseline inventory trace |
| `G1 — Model` | Can the REF/Rulespec boundary represent the slice and the full declared portfolio, including future kinds, without semantic collapse or duplicate fields? | Complete row, cell, source-span, named-item, subtype-group, and role accounting with an independent Rulespec audit attestation; a concrete lossless REF plus Rulespec/external mapping, passing positive fixture, and passing round-trip fixture for every component; `supported` representability for every component; extension-route non-fit and boundary fixtures; REF operational schemas; Rulespec profiles; ownership audit; and separate REF and Rulespec conformance reports |
| `G2 — Capture` | Can the team obtain and replay complete source windows? | Approved source profiles, rights assessments, adopted Rulespec/ODRL policies, captures, manifests, count reconciliation, failures, drift tests, and two replay results |
| `G3 — Evidence` | Can every assertion resolve to exact source material? | Parser gold set, selector-resolution tests, Rulespec Artifact/SourceFragment/EvidenceBinding validation, extraction states, and quality report |
| `G4 — Identity and time` | Are identity, versions, legal effect, and history correct? | Independent timeline and link gold set, zero false merges, as-of tests, and bitemporal checks |
| `G5 — Evidence release` | Can an analyst answer the priority questions from a receipted release? | Complete slice outputs, analyst verification, security review, restore test, and REF `PublicationReleaseManifest` |
| `G6 — Enrichment` | Does typed open-set enrichment improve user work? | Registry coverage, candidate recall, final quality, abstention, grounding, cost, and product-task comparison |
| `G7 — Relationship predicates` | Are enabled inferred relationships precise, scoped, explainable, and useful? | Predicate-specific blind review, evidence explanations, dispute results, and query-task outcomes |
| `G7T — Optional policy threads` | Do durable threads have coherent scope, defensible membership, governance capacity, and user value? | Blind analyst tasks, membership review, scope stability, correction cost, and thread-history tests |
| `G8 — Production` | Can the service operate safely and repeatably? | Capacity, failure recovery, freshness, access control, privacy, cost, rollback, and source-drift evidence |

No global score may waive a failed source-family, predicate, privacy, or
high-risk gate.

### 4.1 Requirement and product traceability

WP1 creates a generated requirement traceability matrix and every later work
package updates it. Each row contains:

- normative requirement ID;
- owning specification, conformance class, and dependency closure;
- REF operational field or Rulespec profile behavior;
- implementing work package and deliverable;
- positive, negative, and runtime test IDs;
- owning gate and release;
- implementation status; and
- `notApplicable` reason and approving profile, when allowed.

No release may claim an REF or Rulespec conformance class while an applicable
requirement lacks an owner, test result, or explicit failure. A row MUST NOT
assign the same semantic field to both specifications.

WP0 also creates a product-question trace. Each priority question maps to:

- source inputs and authority decisions;
- expected operational record kinds and Rulespec semantic records;
- current and as-of query behavior;
- required Rulespec evidence bindings and conflict handling;
- failure and absence semantics; and
- acceptance test and independent reviewer.

WP0 creates a third, portfolio-level inventory trace from the exact dated
[source matrix](source-document-type-matrix-2026-07-28.md) and
[controlled-resource catalog](source-vocabulary-ontology-thesaurus-catalog-2026-07-28.md).
Those files are the required minimum corpus. The trace also pins every
additional inventory or individually onboarded item declared by the
implementation.
It references rather than copies each row and records:

- the baseline file identifier, date, digest, and stable row key;
- the baseline-enumeration report entry that classifies every table data row as
  a `coverageRow`, `constituentRow`, or `definitionRow`, links every
  constituent row to one parent, and separately enumerates source-located named
  portfolio items, subtype groups, and roles inside table cells and outside
  tables;
- exactly one route family (`source` or `controlledResource`), an independently
  justified route from that family, and, for a source component, acquisition
  mode;
- separate representability, adapter or import implementation, release
  inclusion, and rights/use authorization states;
- for `supported` representability, a concrete lossless representation mapping
  and component-specific positive and round-trip fixture references;
- evidence, owner, decision time, reason, and intended release where
  applicable; and
- the requirement, work package, gate, and release report that governs the
  entry.

WP1 makes this trace machine-validatable. WP10B maintains it as sources and
reference resources change. Every release pins the exact trace version and
reports its coverage. The three traces connect product value, portfolio
coverage, and normative conformance without treating one as a substitute for
another.

## 5. Work packages

### WP0 — Product decisions and governance

**Purpose:** Fix the decisions that change the architecture or evaluation.

**Work:**

- Select 8–12 priority questions, including the first-slice question.
- Define answer shapes and evidence requirements.
- Approve the first-slice sources and selection rule.
- Freeze the exact dated source-matrix and controlled-resource-catalog bytes,
  digests, and stable row-locator method as the portfolio baseline.
- Generate a versioned, content-digested `BaselineEnumerationReport`. Classify
  every GitHub Flavored Markdown table data row as a `coverageRow`,
  `constituentRow`, or `definitionRow`; link each constituent row to exactly one
  parent coverage row; and publish total and per-class counts. Give every row a
  stable locator and evidence-backed classification. Within every table cell
  and outside tables, enumerate each occurrence of a named source, feed,
  reference spine, external system, controlled resource, distinct subtype
  group, or separately stated role with an exact source locator. Resolve each
  portfolio item to an account and component or justify it as a descriptive
  mention. Do not let a definition or descriptive classification hide a
  portfolio item or role.
- Include every current row and roadmap tier, `E01`–`E05`, `G01`–`G09`, named
  feed and reference spine, adjacent external system, external-join row, and
  controlled-resource table row and named constituent; do not limit extraction
  to familiar identifier series.
- Create exactly one account for each `coverageRow` and otherwise
  unrepresented named portfolio item. Link each `constituentRow` to one parent
  account; do not create accounts or routes for `definitionRow` entries.
  Decompose every account into one or more components that exhaust its named
  constituents and roles, including named portfolio items resolved to that
  account. Assign every component exactly one route family, `source` or
  `controlledResource`. Independently assign exactly one source route
  (`document`, `participation`, `container`, `entity`, `observation`, or
  `event`) or controlled-resource route (`subjectScheme`, `ontology`,
  `identifierAuthority`, `entityRegistry`, `codeList`, `classification`,
  `schema`, or `mappingSet`) from that family, or use an absolute-IRI route
  defined by a passing extension profile when every core route would misstate
  the item. Assign each source component exactly one acquisition mode:
  `captured` or `externalJoin`.
- Assign each component its applicable semantic/use modes, then record
  representability, adapter or import implementation, release inclusion, and
  rights/use authorization separately with dimension-valid statuses. Do not
  adopt the inventories' proposed classifications or priorities without
  validation. Treat rights/use status only as a summary that references the
  exact `RightsAssessment` and adopted Rulespec and external rights policy;
  never as an authorization source.
- Establish REF source-precedence policies and the applicable Rulespec warrants,
  authorities, attestations, and local adoptions.
- Define record-kind, privacy, access, retention, and permitted-use boundaries.
- Define development, blind-review, and sealed-holdout rules.
- Set provisional numeric thresholds and who may change them.
- Name owners for the data model, each source, evaluation, security, and
  release, plus separate owners for Rulespec kernel/profile changes and REF
  operational implementation.
- Record non-goals and expansion rules.
- Define the extension-profile review process. Require absolute-IRI route and
  processing values, a core-route non-fit rationale, precise boundaries,
  operational and portable bindings, migration rules, and positive, negative,
  and lossless round-trip fixtures; prohibit `other`, `miscellaneous`, and
  equivalent catch-all routes.

**Deliverables:**

- program charter;
- product-question acceptance suite;
- initial immutable inventory-coverage manifest and portfolio trace;
- pinned baseline-enumeration report and independently recomputed count report;
- independent Rulespec attestation that the enumeration exhausts the two
  baseline source texts;
- source-precedence register with Rulespec authority bindings;
- evaluation protocol;
- privacy and rights assessment with Rulespec and ODRL bindings;
- threshold register; and
- responsibility matrix.

**Exit gate:** Every priority question has named inputs, expected output,
evidence standard, negative cases, and a release test. Every baseline row has a
stable locator and explicit enumeration classification. Every named portfolio
item has an exhaustive component decomposition; each component has one route
family, one independently justified route from that family, a source
acquisition mode when applicable, semantic/use modes, all four status
dimensions, an owner, and a non-placeholder reason where required. No
explanatory classification hides a portfolio item. Gold-set owners and
implementers are independent.

**REF coverage:** `REF-CONF`, `REF-PORT`, `REF-EVAL`, `REF-PRIV`,
`REF-RIGHTS`.

### WP1 — REF operational schemas and Rulespec integration

**Purpose:** Make the ownership boundary executable without generating a
second semantic model.

**Work:**

- Define `REF JSON Binding 1.0` only for REF-owned operational records:
  captures, source-record revisions, source resources and versions,
  rendition-processing records, selector-resolution records, candidates,
  adjudication decisions, import snapshots, deployment decisions, run
  receipts, baseline-enumeration reports, inventory-coverage manifests,
  absence evaluations, policy-thread views, and
  `PublicationReleaseManifest` records.
- Define their datatypes, cardinalities, identifier grammar, canonicalization,
  null and absence behavior, extension handling, and deterministic payload
  digests.
- Publish REF JSON Schema and generated implementation types only for those
  operational records.
- Implement a portfolio-accounting check that reads the exact two pinned
  minimum inventories plus every additional declared inventory or item,
  independently recomputes their raw table-row and named-item universe where
  applicable, resolves their stable locators, and proves that the immutable
  `InventoryCoverageManifest` has exactly one row account per `coverageRow` and
  otherwise unrepresented `namedPortfolioItem`, plus an exhaustive component
  set for every linked constituent and role, without copying or normatively
  adopting the inventories' proposed architecture or classifications.
- Make the baseline extractor emit and validate the complete row-classification
  report required by `REF-PORT-011`, including source-located occurrences
  inside table cells and outside tables, expected constituent counts, and
  negative fixtures for hidden, unresolved, or unclassified entries.
- Define one versioned, concrete representation mapping for every component.
  Map every named constituent and role to REF operational fields plus pinned
  Rulespec or external types and predicates, and add component-identified
  positive and lossless round-trip fixtures. Do not accept a narrative
  representability statement or an aggregate fixture as proof.
- Validate the source and controlled-resource route value sets, all four
  independent status dimensions and their allowed values, semantic/use modes,
  source acquisition modes, evidence, reasons, and transition history.
- Validate extension route and `recordKind` profiles without hard-coding a
  closed future type list. Require every extension to stay in one core route
  family, bind to common REF controls, identify its Rulespec or external
  semantics, and prove that no existing core route represents it without loss.
- Complete the Rulespec `0.2.0-pre.8` upstream gate identified by the
  [REF Rulespec Application Profile](regulatory-evidence-rulespec-profile.md),
  including authoritative CUE, generated artifacts, prose, context, shapes,
  vocabulary, and fixtures.
- Consume Rulespec-generated schemas and types directly. Do not copy
  `Artifact`, `SourceFragment`, `EvidenceBinding`, assertion, assignment,
  concept, mapping, registry-release, lineage, confidence, attestation,
  adoption, lifecycle, access, or retention types into REF.
- Publish the versioned REF Rulespec Application Profile and machine-readable
  compatibility manifest.
- Define canonical cross-record mappings and reference-integrity checks,
  including rendition-role artifact, selector-resolution-to-source-fragment,
  enrichment-decision-to-assignment, and adjudication-to-assertion.
- Require every conformance manifest to pin the exact REF binding plus
  Rulespec version, immutable revision, constraint-bundle digest, profiles,
  validator, conformance level, graph digest, and result, plus the exact
  inventory-coverage manifest and its two baseline inventory digests.
- Build an REF validator that reports REF requirement identifiers and invokes
  the pinned Rulespec validator without translating its constraints.
- Build separate REF, Rulespec, and cross-boundary positive and negative
  fixture suites.
- Publish later capability profiles before those capabilities are claimed;
  profiles enumerate adopted upstream predicate IRIs and define only REF
  persistence, materiality, review, evaluation, and publication policy.

**Deliverables:**

- REF operational JSON binding, schemas, and generated types;
- inventory-coverage manifest schema, baseline row-key extractor, portfolio
  trace, baseline-enumeration report generator, independently recomputed count
  report, and accounting report;
- pinned Rulespec generated artifacts consumed as a dependency;
- REF Rulespec Application Profile and compatibility manifest;
- versioned REF conformance profiles;
- REF validator profile and validator-conformance suite;
- cross-boundary fixture corpus;
- separate REF and Rulespec machine-readable reports; and
- a combined validation command that preserves both result sets.

**Exit gate:** Every representative REF operational fixture validates, every
Rulespec fixture passes the upstream suite, and every cross-boundary fixture
proves one canonical owner. The REF validator rejects duplicate portable
semantic fields in REF records. It accepts every valid REF fixture, rejects
every invalid REF fixture with the applicable REF requirement identifier, and
fails the combined profile when the exact pinned Rulespec report is missing,
stale, scoped to another graph digest, or failed. Raw source values round-trip
without loss. The portfolio accounting report proves 100 percent of baseline
rows and named constituents are present with exhaustive component
decomposition and no duplicate, missing, placeholder, unrouted, or unclassified
component. `G1` additionally requires `supported` representability for every
component, backed by its current concrete lossless mapping and passing
component-specific positive and round-trip fixtures; it does not require every
adapter or import path to exist. An independent ownership audit finds no
parallel semantic type or validator.

**Dependencies:** WP0.

**REF coverage:** Sections 2–7, 10, 15, and 16, including `REF-PORT`.

### WP2 — Storage, replay, source registry, and receipts

**Purpose:** Create the stable evidence foundation.

**Work:**

- Implement immutable capture storage.
- Implement append-only REF operational records and
  `PublicationReleaseManifest` records.
- Store validated Rulespec records as a separately versioned semantic graph,
  even if both stores share one physical database.
- Implement versioned evidence-collection policies and bind REF adjudication
  decisions to the policy used and to their Rulespec outputs.
- Use Rulespec assertions, attestations, local adoptions, and lifecycle events
  for semantic rejection, dispute, correction, retraction, and supersession;
  do not build a generic REF proposition or review ledger.
- Implement the source-profile registry.
- Implement idempotent run, checkpoint, resume, quarantine, and retry behavior.
- Implement capture and run receipts.
- Make run receipts reference canonical Rulespec extraction activities, AI
  lineage, agents, reference-resource releases, and outputs; retain only
  operational and provider-native extras as non-authoritative audit data.
- Separate materialization from publication.
- Add backup, restore, digest verification, and retention handling.
- Define rebuild procedures for search, vector, and graph views.

**Deliverables:**

- capture store;
- REF operational record store;
- validated Rulespec graph store;
- source registry;
- evidence-collection-policy registry;
- run ledger;
- quarantine queue;
- receipt generator;
- publication mechanism; and
- restore runbook.

**Exit gate:** Two clean replays from fixed captures produce identical
canonical deterministic payload identifiers and semantic digests. Their
receipts preserve distinct run identifiers, `recordedAt` values, activities,
and execution times outside those digests. Interrupted work resumes without
duplicates or loss. Every source-derived canonical record resolves to a capture
or declared external reference; every semantic output resolves to its canonical
Rulespec provenance and evidence. Storage fixtures append Rulespec attestations,
adoptions, lifecycle events, and superseding assertions without overwriting
the proposition, evidence, or prior decisions. Backup and restore reconstruct
both stores and their exact release binding.

**Dependencies:** WP1.

**REF coverage:** `REF-CAP`, `REF-PROV`, `REF-PIPE`, `REF-EXP`.

### WP3 — Federal-slice source adapters

**Purpose:** Acquire the official source material needed for the first slice.

**Entry gate:** Before an adapter's first live capture, its versioned source
profile and `RightsAssessment` are attested and locally adopted for acquisition
and storage. The profile defines source identity and precedence, coverage and completeness rules,
version semantics, parser and media handling, license and access limits,
retention, privacy, permitted uses, publication limits, and references to
applicable Rulespec warrants or authorities.

**Work:**

- Implement thin adapters for the approved Unified Agenda, OIRA, Federal
  Register, Regulations.gov, eCFR/GovInfo, Congressional Review Act, and legal
  authority sources.
- Capture raw responses, headers, attachments, and source metadata.
- Reconcile pages, cursors, date windows, counts, and attachment inventories.
- Detect schema drift, truncation, deletion, rate limits, and partial runs.
- Create hermetic recorded fixtures for normal and adversarial source behavior.
- Add live, read-only source-smoke checks that do not update acceptance data.

**Deliverables:**

- source adapters;
- source-specific type dictionaries;
- capture manifests;
- completeness ledgers;
- drift alerts;
- fixture packages; and
- source operation notes.

**Exit gate:** Every run declares its coverage window, page or cursor ledger,
expected and observed counts when available, attachment inventory, exclusions,
and failures. Retry, truncation, schema drift, deletion, and rate-limit fixtures
pass. No partial run can appear complete.

**Dependencies:** WP2 and the WP10A acquisition gate.

**REF coverage:** `REF-CAP`, `REF-SRC`, `REF-PIPE`.

### WP4 — Record routing and source-resource resolution

**Purpose:** Decide what each source record represents before parsing its body.

**Work:**

- Implement record-kind resolution from source-native records.
- Implement source-specific raw-to-normalized type maps.
- Separate source-record revision, source-resource version, and immutable
  rendition identity.
- Create one `rkaf:Artifact` for each immutable concrete rendition before
  content extraction. Do not create an REF rendition object.
- Create REF `RenditionProcessingRecord` objects that reference those
  artifacts and contain only extraction and quality state.
- Route `container`, `participation`, `entityRecord`, `observationRecord`,
  `eventRecord`, and `externalReference` values as operational discriminators;
  use the applicable Rulespec profile or external ontology for portable types.
- Route a governed absolute-IRI extension value only through its pinned
  extension profile. Apply the same identity, version, evidence, provenance,
  rights, failure, publication, and evaluation controls; never route an
  unknown value into a generic catch-all.
- Record REF retrieval and recorded times separately from Rulespec or
  source-profile publication, assertion, effective, and applicability times.
- Preserve unknown values and route unresolved mappings to review.

**Deliverables:**

- typed source and canonical records;
- source type-map snapshots;
- source-resource and source-resource-version records;
- rendition-role Rulespec artifacts and REF processing records;
- routed non-document operational records;
- type-map review queue; and
- preliminary identity/version report.

**Exit gate:** Raw values remain available for every slice record. No unknown
value is silently coerced. Every acquired payload is registered as the correct
record kind or an explicit unresolved kind before document parsing. Review
finds no false collapse among the source records, source-resource versions, and
rendition-role artifacts that source metadata can distinguish. An ownership
fixture proves no REF object duplicates a Rulespec artifact identity or digest.

**Dependencies:** WP1 and WP3.

**REF coverage:** `REF-SRC`, `REF-ART`, `REF-TYPE`, `REF-ID`, `REF-VER`,
`REF-TIME`.

### WP5 — Rendition processing, source fragments, and historical resolution

**Purpose:** Resolve exact source content and finish the canonical historical
record.

**Entry gate:** WP4 has routed every input. The general document parser accepts
only rendition-role `rkaf:Artifact` records. Other operational `recordKind`
values require their own declared processors and cannot enter through
incidental text fields.

**Work:**

- Parse source-native XML and HTML before fallback formats.
- Extract text and structure from PDF and Office files.
- Add optical character recognition fallback for scanned pages.
- Preserve headings, provisions, paragraphs, tables, footnotes, pages, and
  attachment boundaries.
- Implement field, element, character, page-region, and table-cell selectors.
- Record extraction method, quality, errors, and unsupported states in
  `RenditionProcessingRecord`.
- Resolve successful addresses to `rkaf:SourceFragment` records and verify the
  attempt fields match the canonical fragment.
- Run untrusted files in an isolated processing environment.
- Finalize REF source-history records and applicable Rulespec assertions and
  lifecycle events for correction, withdrawal, replacement, supersession,
  deletion, and tombstones.
- Resolve valid and effective intervals and build current and as-of views from
  append-only history.

**Deliverables:**

- parser adapters;
- structure trees;
- Rulespec source-fragment records;
- selector resolver and REF selector-resolution records;
- extraction-quality report;
- parser fixtures;
- safe-file-processing controls;
- finalized version and rendition history;
- bitemporal views;
- tombstone and correction records; and
- identity/version gold report.

**Exit gate:** Every rendition-role artifact has an explicit linked extraction
state. Every golden `rkaf:SourceFragment` resolves to the correct artifact
digest, source node or page, and text, and the REF selector attempt matches it.
Compound packages remain intact unless the source supplies a reliable
boundary. Low-quality optical character recognition is visible and cannot
silently support an accepted assertion. Independent review finds zero false
source-resource/version/artifact collapses. Every selected correction, withdrawal,
replacement, delay, and before/after legal state reproduces correctly for its
gold date. The report gives, by source and format, discovered attachment count,
bytes retrieved, full native text, extracted text, optical character
recognition, abstract-only, metadata-only, unsupported, restricted, and failed
counts; selector resolution; and accepted Rulespec assertions that use
low-quality text.

**Provisional first-slice targets, subject to WP0 approval:**

- every supported fixture passes or carries an approved expected-failure
  result;
- every real-slice file has a declared extraction state;
- median character error rate no worse than 2% on the approved modern-document
  optical character recognition set;
- 95th-percentile character error rate no worse than 8%;
- 100% explicit low-quality status for pages outside the accepted threshold;
  and
- zero accepted assertions based only on low-quality text without the
  attestation level
  required by the active policy.

These targets are implementation gates for one slice, not universal REF
requirements.

**Dependencies:** WP4 and the WP10A file-processing gate.

**REF coverage:** `REF-ART`, `REF-EVID`, `REF-ID`, `REF-VER`, `REF-TIME`,
`REF-SAFE`.

### WP6 — Explicit and deterministic relationship spine

**Purpose:** Connect the regulatory lifecycle with evidence-backed links.

**Work:**

- Implement official identifier resolvers.
- Implement citation extraction and resolution.
- Produce `rkaf:RelationshipAssertion` records using only predicates defined
  by an adopted Rulespec profile or external ontology.
- Apply versioned REF evidence-collection policies to adjudication and publish
  material supporting, qualifying, and contradictory evidence through
  `rkaf:EvidenceBinding`.
- Apply REF source-precedence policies plus Rulespec warrants, authority,
  epistemic basis, attestations, and local adoption.
- Link agenda entries, reviews, Federal Register artifacts, dockets,
  attachments, CFR provisions, Congressional Review Act events, and legal
  authority without merging them.
- Implement relationship history, retraction, and supersession.
- Exercise Rulespec assertion lineage, attestation, adoption, and lifecycle
  records for source-aligned and deterministic assertions.
- Build rule-action and legal-change timelines.

**Deliverables:**

- identifier registry;
- citation parser and resolver;
- validated Rulespec assertion graph;
- pinned upstream predicate definitions and REF publication policies;
- action timeline;
- legal-change path; and
- link-quality and decision-history report.

**Exit gate:** On the independently prepared first-slice gold set:

- the evaluation manifest freezes release-critical predicates, linkable cases,
  expected edges, exclusions, genuinely ambiguous cases, sample sizes, and
  uncertainty treatment before scoring;
- deterministic-link precision is 1.00;
- recall for marked legal-citation mentions is at least 98%, and every detected
  release-critical citation either resolves correctly or has a typed unresolved
  state;
- every slice-critical identity, version, rendition, publication, docket,
  citation, authority, correction, withdrawal, and amendment link in the
  frozen expected-edge inventory is present and correct;
- every link resolves to exact evidence or a declared deterministic rule;
- `authorizedBy` and other legal-effect predicates use authority-specific
  evidence rather than citation alone;
- no shared RIN, docket number, citation, title, URL, or hash causes an
  unsupported identity merge;
- every unresolved probabilistic candidate remains a reversible candidate;
- every durable assertion links the REF adjudication decision that names its
  evidence-collection policy and retains every
  material qualifying or contradictory item encountered under that policy; and
- an end-to-end fixture appends Rulespec attestations, adoptions, lifecycle
  events, and superseding assertions, changes the current materialized state,
  and preserves the prior proposition, evidence, reviewer action, and history.

Optional predicates report per-predicate precision, recall, sample size, and
uncertainty. A predicate that misses its preregistered threshold leaves the
Release 1 profile; the release does not waive the threshold.

**Dependencies:** WP5.

**REF coverage:** `REF-SEMOUT`, `REF-PROV`, `REF-REL`, `REF-PATH`.

### WP7 — Typed enrichment and registry operations

**Purpose:** Add useful semantic access without forcing every passage into a
fixed vocabulary.

WP7 has four separately gated capabilities.

#### WP7A — Registry import, deployment, and refresh

- Import approved subject schemes, ontologies, entity registries, identifier
  authorities, code lists or classifications, schemas, and mapping sets with
  REF receipts.
- Create the canonical `rkaf:ReferenceResourceRelease` identity, version,
  resource kind, membership mode and permitted membership claims,
  distributions, and RDFC-1.0 semantic `rkaf:referenceReleaseDigest` in
  Rulespec. Use `rkaf:completeMembership` for a complete explicit set,
  `rkaf:partialMembership` for an explicitly incomplete set, and
  `rkaf:membershipNotEnumerated` for a dynamic or non-enumerable authority.
  Treat the release as the semantic manifest and preserve byte digests on its
  distribution `rkaf:Artifact` records. Store retrieved bytes, transport
  metadata, and acquisition digest only in REF `Capture`. The
  `RegistryImportSnapshot` references those captures or explicit external
  references and owns only import transformation, exclusions, failures,
  rights-assessment references, and validation.
- Preserve native SKOS, OWL, code-system, and schema distributions as the
  canonical source for multilingual labels, scripts, notes, notations, status,
  hierarchy, and source identity. Pin them through
  `rkaf:ReferenceResourceRelease`; use Rulespec-owned concept constraints only
  for actual `rkaf:LocalConcept` or `rkaf:RegisteredConcept` records.
- Store operational selection in append-only `RegistryDeploymentDecision`
  records; use Rulespec attestations and local adoptions for review and
  authorization.
- Implement `RightsAssessment` and reference the adopted Rulespec and ODRL
  policy records.
- Detect data, publisher, license, access, and permitted-use changes.
- Deploy atomically, rebuild affected indexes, keep the prior Rulespec release
  logically selected after failure, and support rights-aware rollback.

**WP7A gate:** Refresh fixtures cover rename, deletion, identifier reuse,
hierarchy and mapping change, rights change, failed deployment, index
invalidation, rollback, and historical resolution. Same-label concepts and
multilingual preferred labels round-trip through Rulespec without collision.
Membership fixtures cover complete, partial, and non-enumerated releases,
require member claims for complete and partial modes, forbid member claims for
non-enumerated mode, and reject assignment or mapping pins unless the pinned
release has complete membership. A non-enumerated identifier or schema
authority pins its exact authoritative grammar, resolver definition, or
native content as a distribution without inventing members.
The standard Rulespec conformance path recomputes the RDFC-1.0 release digest
and rejects a wrong but lexically valid value; REF does not reimplement that
check.
Every selected deployment has an import snapshot, exact
`rkaf:ReferenceResourceRelease`, applicable distribution artifacts, Rulespec
validation result, rights assessment, attestation, and local adoption. Every
import snapshot references its input captures or explicit external references.
It does not copy capture bytes, transport metadata, or acquisition digests. No
REF record duplicates the release's canonical identifier, version, membership
mode or claims, distributions, or semantic digest, or a distribution
artifact's identity or byte digest.
External multilingual content round-trips through its native distribution.
Project-authored multilingual Rulespec concepts ship only if the pinned
Rulespec profile explicitly preserves their labels, notes, and notations.

#### WP7B — Semantic-reference extraction

- Extract `SemanticReferenceCandidate` records for definitions, requirements,
  thresholds, populations, programs, mechanisms, datasets, standards, and
  outcomes.
- On acceptance, select a type from an adopted Rulespec profile or external
  ontology and create the applicable Rulespec assertions. Do not publish a
  generic REF semantic-resource or proposition class.
- Preserve exact `rkaf:SourceFragment` evidence, Rulespec extraction
  provenance and AI lineage, and REF run receipts.

**WP7B gate:** Every accepted resource and assertion resolves to exact
Rulespec evidence. Negative fixtures reject a generic REF semantic object,
duplicate proposition, or provider-specific portable lineage. Attestation and
adoption do not change assertion origin or epistemic basis.

#### WP7C — Candidate generation, acceptance, and abstention

- Implement source-assigned concepts against conforming
  `rkaf:ReferenceResourceRelease` records.
- Implement lexical, dense, source-label, mapping, and open-phrase candidate
  channels behind project-owned operational interfaces.
- Implement deterministic candidate union, observable truncation, and a global
  escape path.
- Implement REF `AcceptancePolicy`, typed abstention, grounded open-label
  candidates, and stable `ConceptProposal` records.
- Freeze each REF `OutputProfile` as an immutable identifier, version, and
  digest, including authorized Rulespec reference-resource releases, mapping
  import snapshots and relations, Rulespec assignment-role predicate IRIs, and
  open-label modes.
- Implement append-only REF output-profile deployment decisions. Use Rulespec
  attestation and local adoption for approval and authorization.
- Emit an REF `EnrichmentDecision` for every attempted target × facet ×
  assignment role, including failure, cancellation, and abstention.
- Publish accepted controlled values as `rkaf:ConceptAssignment` and accepted
  open labels as the profile-defined `rkaf:ValueAssertion`; attach canonical
  Rulespec wording, language and script when known, evidence, provenance,
  confidence, attestation, and adoption. Use the normalized Rulespec
  relationship-assertion shape, `rkaf:assignedConceptRelease`, and
  `rkaf:EvidenceBinding`; do not create an REF assignment shape.
- Record complete REF receipts for nondeterministic generation, retrieval, and
  adjudication and link canonical Rulespec extraction activities and AI
  lineage.

**WP7C gate:** Before scoring, freeze the target universe, Rulespec registry
releases, REF output-profile identifier, version, digest, source strata,
shortlist values, and risk thresholds. Report registry coverage,
reachable-gold Recall@K curves, unconditional target recovery, final precision
and recall, correct abstention, unsupported output, reviewer volume, cost, and
product value. Facets remain separate. Accepted decisions reference their
Rulespec assignments or value assertions; concept-proposal outcomes reference
their REF proposals; review-required decisions reference candidates. Empty
output cannot hide failure. No assignment enters the accepted view without
an exact complete-membership release pin, passing Rulespec conformance,
required attestation, and local adoption.
An accepted multilingual open label cannot ship until the pinned Rulespec
profile preserves its language and script; a declared language-neutral or
default-language profile may ship earlier.

#### WP7D — Mapping and concept governance

- Keep REF concept proposals separate from Rulespec concepts.
- Implement proposal, promotion, rejection, deprecation, merge, split,
  supersession, dispute, and correction workflows.
- On promotion, mint a distinct `rkaf:LocalConcept` or
  `rkaf:RegisteredConcept` and preserve explicit Rulespec provenance from the
  REF proposal.
- Publish mappings only as `rkaf:ConceptMapping`, with exact source and target
  `rkaf:ReferenceResourceRelease` pins.
- Use Rulespec attestation, lifecycle, and local adoption for mapping and
  concept decisions; prohibit machine promotion.
- Let REF query policies select mapping paths without redefining SKOS inverse,
  symmetry, or entailment semantics.

**WP7D gate:** Only the five Rulespec-incorporated SKOS mapping predicates are
accepted for cross-scheme mappings. No operational query expansion becomes
semantic closure. Mapping-only or unauthorized schemes cannot enter accepted
output. Every mapping endpoint pins a complete-membership release. Promotion,
deprecation, merge, split, and rollback fixtures preserve history. A promoted
concept proposal remains an REF proposal record and never becomes the
Rulespec concept in place.

**Deliverables:**

- REF registry import snapshots for every controlled-resource kind, including
  mapping sets;
- Rulespec reference-resource releases and distribution artifacts, concepts,
  assignments, and mappings;
- REF registry and output-profile deployment ledgers;
- rights assessments and linked Rulespec/ODRL policy records;
- typed candidate indexes and receipts;
- acceptance-policy and output-profile releases;
- enrichment-decision and abstention ledger;
- concept-proposal queue;
- governance service that writes Rulespec attestations, adoptions, and
  lifecycle records; and
- enrichment evaluation report.

**Combined WP7 exit gate:** Before any automated assignment enters the accepted
view:

- WP7A–WP7D pass;
- every semantic result has independent Rulespec assertion origin and
  epistemic basis, exact source-fragment evidence, extraction provenance, and
  applicable warrant or authority;
- no-fit cases produce typed abstention, an authorized open-label value
  assertion, or an REF concept proposal;
- no machine action promotes a concept;
- every accepted result has the required Rulespec attestation and local
  adoption and was allowed by the selected REF output profile;
- semantic rejection, dispute, correction, retraction, and supersession append
  Rulespec records without erasing history;
- every nondeterministic stage has an REF receipt linked to canonical Rulespec
  extraction and AI-lineage records;
- historical assignments resolve against their exact
  `rkaf:ReferenceResourceRelease`; and
- final acceptance clears the predeclared per-profile gates on the sealed
  holdout.

The plan does not assume which scheme, model, channel mix, shortlist size, or
general-subject concept count will pass.

**Dependencies:** WP7A may begin after WP1. WP7B requires WP5. WP7C requires
WP5, WP7A, the relevant WP7B outputs, and a frozen evaluation corpus. WP7D
requires WP7A and the governance decisions from WP0. Accepted assignments
require WP6 source-authority and provenance behavior.

**REF coverage:** `REF-SEM`, `REF-ENR`, `REF-CAND`, `REF-ACC`, `REF-ASSIGN`,
`REF-VOC`, `REF-MAP`, `REF-GOV`.

### WP8 — Inferred relationship workflow and policy threads

**Purpose:** Connect artifacts that do not explicitly reference one another
without turning relevance into fact.

#### WP8A — Inferred relationships

- Select predicate IRIs whose canonical semantic definitions live in an
  adopted Rulespec profile or external ontology; define REF-specific evidence
  collection, persistence, materiality, review, evaluation, and publication
  policy.
- Freeze predicate target universes, thresholds, sample sizes, and uncertainty
  treatment before evaluation.
- Generate candidates from shared anchors, lexical retrieval, dense retrieval,
  semantic-reference candidates, structure, and temporal priors.
- Implement relation-specific adjudication and abstention.
- Keep REF `SimilarityObservation` records query-time.
- Publish accepted scoped dependencies as `rkaf:RelationshipAssertion`
  records pointing to externally typed definitions, requirements, datasets, or
  other resources when available.
- Implement query-time multidimensional similarity.
- Implement REF `AbsenceEvaluation` records and path explanations over
  Rulespec assertions.
- Extend bindings, validators, and fixtures for `REF-REL`, `REF-SIM`,
  `REF-DEP`, `REF-PATH`, and `REF-ABS`.
- Record complete receipts for nondeterministic work, including provider,
  model, prompt or configuration, raw output, execution time, reproducibility
  class, and secret-scan result.

**WP8A gate:** For every predicate allowed in an accepted view:

- the predicate has a reviewed definition, evidence-collection policy, and
  acceptance policy, and the definition is pinned from Rulespec or the adopted
  ontology;
- blind precision, abstention, uncertainty, and reviewer-volume thresholds pass
  by predicate;
- accepted dependencies state direction, scope, time, and evidence;
- similarity never satisfies dependency, identity, authority, amendment,
  supersession, causation, or legal-effect queries;
- query-time results remain query-time after caching;
- every multi-hop explanation names its supporting Rulespec assertions; and
- `notFound` cannot appear as proof of absence without a bounded,
  completeness-aware REF absence evaluation.

Predicates that miss their automatic-publication threshold remain
review-required or query-time. They do not block predicates that pass.

#### WP8B — Optional policy threads

- Implement query-time clusters and REF policy-thread application views as
  separate operational records.
- Publish each durable membership as an `rkaf:RelationshipAssertion` using the
  upstream membership predicate.
- Use Rulespec source fragments, epistemic basis, extraction or AI lineage,
  attestations, local adoption, and lifecycle for durable membership.
- Implement membership review, correction, and dispute handling through
  Rulespec.
- Implement REF thread-version merge and split plus Rulespec assertion and
  lifecycle history.
- Extend bindings, validators, and fixtures for `REF-THR`.

**WP8B gate:** Thread scope, supporting Rulespec source fragments, assertions,
attestations, local adoptions, inclusion and exclusion rules, ownership,
version, membership evidence, and history validate. Thread
membership never implies identity, causation, dependency, or undocumented
pairwise relationships.
Blind product tests show enough analyst value and governance capacity to claim
`REF-Policy-Thread-Publisher`; otherwise WP8B remains experimental.

**Deliverables:**

- relation candidate service;
- predicate-specific adjudicators;
- Rulespec dependency assertions;
- similarity service;
- path explainer;
- absence-evaluation service;
- policy-thread service;
- reviewer interface; and
- relationship evaluation report.

**WP8 relationship decision-history gate:** End-to-end inferred-relationship
tests append a rejection, dispute, correction, retraction, and supersession;
change the current view without deleting history; trace each accepted
relationship through Rulespec evidence, provenance, attestation, adoption, and
prior assertions;
preserve qualifying and contradictory evidence; and distinguish absent
evidence from evidence of absence.

**Dependencies:** WP8A requires WP6 and the relevant WP7B accepted semantic
resources.
WP8B requires the relationship and query capabilities its membership rules
use, but remains optional for Releases 2A and 2B.

**REF coverage:** `REF-REL`, `REF-SIM`, `REF-DEP`, `REF-PATH`, `REF-ABS`,
`REF-THR`, `REF-QRY`.

### WP9 — Query, review, and consumer products

**Purpose:** Make evidence and interpretation usable without hiding their
authority.

#### WP9A — Release 1 evidence queries

- Publish immutable, current, and as-of views.
- Implement evidence expansion and Rulespec assertion history.
- Implement filters by `rkaf:epistemicBasis`, `rkaf:assertionOrigin`,
  attestation, local adoption, consumer lifecycle, usage eligibility, warrant
  or authority, predicate, and time.
- Implement rule, docket, and legal-text timelines.
- Expose unresolved source conflicts, incomplete processing, and bounded
  absence states.
- Publish source completeness, extraction quality, and freshness ledgers.
- Publish an REF operational export plus its lossless Rulespec graph and exact
  binding manifest.

#### WP9B — Release 2 intelligence queries

- Add filters by facet, REF workflow state, Rulespec confidence policy, and query-time
  association method.
- Add policy-thread timelines only when WP8B passes.
- Implement document and passage search.
- Implement "Why am I seeing this?" explanations.
- Implement correction and dispute intake.
- Publish Rulespec JSON-LD or RDF directly. Add other interoperability
  mappings only when a product or partner requires them.
- For each added mapping, document semantic loss and test round trips for REF
  source identity and version roles plus Rulespec epistemic basis, origin,
  attestation, adoption, lifecycle, authority, evidence, time, access,
  retention, usage eligibility, and supersession.

**Deliverables:**

- stable query API;
- analyst interface;
- current and historical datasets;
- evidence package;
- search and timeline views;
- review and correction queues;
- quality dashboard; and
- export profiles.

**WP9A gate:** Every Release 1 priority question returns the expected answer,
as-of state, Rulespec evidence and authority, conflict state, completeness state, and
uncertainty on the gold slice. An independent analyst can reconstruct each
answer without privileged implementation knowledge. The export identifies the
REF version, binding, profile, release, extension namespaces, stable and
source-native identifiers, the exact Rulespec pin and graph, and all canonical
Rulespec epistemic, review, authority, lifecycle, evidence, time, access,
retention, and use records. Public exports enforce rights and access rules.
The conflicting first-slice case exposes its evidence-collection policy,
qualifying and contradictory evidence, and append-only Rulespec attestation,
adoption, lifecycle, and assertion history in both current and as-of views.

**WP9B gate:** Users can exclude inferred and editorial links, inspect "Why am
I seeing this?" evidence, and submit corrections without erasing history.
Every added interoperability mapping passes its declared semantic-loss and
round-trip tests.

**Dependencies:** WP9A follows WP6 and is required for Release 1. WP9B follows
the WP7 and WP8 capabilities it exposes.

**REF coverage:** `REF-QRY`, `REF-EXP`, `REF-PROV`, `REF-TIME`.

### WP10 — Security, reliability, operations, and source expansion

**Purpose:** Operate safely and make new sources repeatable.

#### WP10A — Release 1 safety, rights, publication, and reliability

**Work:**

- Implement append-only REF `RightsAssessment` records for observed terms and
  workflow; publish accepted access, retention, and usage eligibility through
  Rulespec and the remaining permissions through the adopted ODRL overlay.
- Attest and locally adopt each source profile and its acquisition and storage
  policy before the first live capture.
- Implement effective-policy enforcement for captures, source resources,
  rendition-role artifacts, source fragments, assertions, threads, embeddings,
  caches, and exports from canonical Rulespec and ODRL records.
- Implement the protected participation boundary.
- Add sandboxing, malware checks, decompression limits, and parser resource
  limits before processing untrusted files.
- Add source freshness, completeness, drift, and failure monitoring.
- Add capacity, latency, cost, backup, restore, and disaster-recovery controls.
- Add atomic publication and rollback exercises.

**Deliverables:**

- approved source-profile and rights-assessment register;
- security and privacy controls;
- Rulespec/ODRL policy enforcement service;
- threat model and derived-disclosure report;
- safe-file-processing evidence;
- operational service levels;
- monitoring and alerting;
- capacity and cost report; and
- disaster-recovery and publication-rollback evidence.

**Acquisition gate:** Each live Release 1 adapter has an approved, versioned
source profile, REF rights assessment, and explicit Rulespec/ODRL policy
attested and locally adopted for acquisition and storage. Unknown or conflicting
permissions fail closed.

**File-processing gate:** Before WP5 handles an untrusted file, media sniffing,
malware controls, decompression and size limits, parser isolation, and resource
limits pass adversarial fixtures.

**WP10A exit gate:** Expected peak slice volume plus an agreed safety factor
completes inside the capacity budget. Source outage, malformed payload, parser
crash, retry storm, interrupted publication, corrupted object, and restore
tests lose no accepted data. Access-control and public-export tests expose no
protected content. Negative tests cover restricted relationship traversal,
evidence expansion, summaries, embeddings, inferred attributes, graph
neighborhoods, caches, prompt injection, schema-invalid model output, and
public exports. Effective policy enforcement fails closed when permissions
conflict or remain unknown. Real participation fixtures require a tested limited
`REF-Participation-Processor` profile; otherwise tests use synthetic restricted
fixtures.

Release 1 remains a controlled evaluation artifact until its applicable `G8`
production controls pass. Public or production distribution requires `G8`.

**Dependencies:** Governance work begins in WP0. Schemas and executable
controls follow WP1 and WP2. The acquisition gate blocks WP3 live capture, the
file-processing gate blocks WP5 untrusted-file processing, and the final gate
blocks Release 1.

**REF coverage:** `REF-SEC`, `REF-PRIV`, `REF-SAFE`, `REF-RIGHTS`, `REF-PIPE`.

#### WP10B — Governed source expansion

**Work:**

- Build source and reference-resource onboarding checklists plus adapter and
  import conformance suites.
- Define source deprecation and ownership-transfer procedures.
- Maintain the versioned `InventoryCoverageManifest` and portfolio trace for
  every source and controlled-resource baseline row and every later declared
  inventory or individually onboarded item. Revalidate each row's
  component decomposition and each component's route family, route, source
  acquisition mode when applicable, semantic/use modes, representability,
  adapter or import implementation, release inclusion, and rights/use
  authorization independently.
- Record authoritative source, external joins, text and formats, version
  semantics, rights state, completion gaps, intended onboarding release, gate
  status, and evidence by stable row reference without copying the inventory
  row into the ledger.
- Compare each manifest to its predecessor and publish decomposition, route,
  mode, and dimension-status changes, newly added or removed inventory rows,
  blocked uses, and remaining work.
- Generate and independently re-attest the complete enumeration whenever an
  additional inventory, item, named constituent, role, or extension route
  enters or leaves the declared portfolio.
- Block production onboarding when the source or resource is absent from the
  manifest or when a novel route lacks its governed extension profile and
  conformance fixtures.
- Prioritize missing text and history before breadth: Regulations.gov
  attachments, Federal Register body and correction history, point-in-time CFR
  text, congressional editions and proceedings, and report, court, and FCC
  bodies.

**Deliverables:**

- onboarding kit;
- per-source conformance dashboard; and
- versioned source and reference-resource portfolio dashboard, manifest, and
  change report.

**WP10B exit gate:** Every table data row and named portfolio item remains
classified and every named item, subtype group, and role occurrence remains
source-located and resolved in the pinned baseline-enumeration report. Every
required row account exists exactly once, every constituent row has exactly one
parent, definition and descriptive entries remain visible without routes, the
independent Rulespec audit attestation passes, and every covered account has an
exhaustive component set. Each component has exactly one route family, one
allowed route from that family, one source acquisition mode when applicable,
semantic/use modes, and all four status dimensions. A full-framework
design-coverage claim also requires
`supported` representability for every component, a current concrete lossless
representation mapping, and passing component-specific positive and round-trip
fixtures. Each source family or reference resource proposed for production has
a named owner, approved profile, rights assessment, adopted
Rulespec/ODRL policies, product question, coverage rule, operating budget, and
passing applicable capture or import, typing, parsing, history, link, rights,
privacy, and quality gates before production publication. Components may
remain planned, external-reference-only, mapping-only, deferred,
rights-blocked, not assessed, or unsupported with a reason; none may disappear
from the portfolio trace.

**Dependencies:** WP10B begins after `G5` and reuses the WP10A controls. A new
source family returns through the applicable earlier work-package gates.

**REF coverage:** `REF-PORT`, `REF-CAP`, `REF-SRC`, `REF-TYPE`, `REF-VOC`,
`REF-SEC`, `REF-PRIV`, `REF-SAFE`, `REF-RIGHTS`, `REF-PIPE`.

## 6. Critical path and parallel work

```text
WP0 → Rulespec pre.8/profile gate → WP1 → WP2 → WP10A acquisition gate → WP3 → WP4
WP4 + WP10A file-processing gate → WP5 → WP6 → WP9A
WP9A + WP10A final gate → G5 → G8 → Release 1

WP1 → WP7A registry preparation
WP5 + WP6 + WP7A → WP7B–WP7D → WP9B → G6/G8 → Release 2A
WP6 + WP7B → WP8A → WP9B → G7/G8 → Release 2B
WP8A + product evidence → WP8B → WP9B → G7T → Release 2C
G5 → WP10B source expansion
```

Safe parallel work:

- Source legal/rights review can run during WP1 and WP2.
- Vocabulary acquisition and license review can begin after WP1.
- Query mockups can use fixtures before production data exists.
- Security controls can begin with capture and file processing.
- Gold-data preparation can run beside implementation if evaluators remain
  independent and the holdout stays sealed.

Unsafe shortcuts:

- accepting annotations before evidence selectors work;
- inferring relationships before source-resource history and rendition-role
  Rulespec artifact identity are
  stable;
- tuning on the sealed holdout;
- adding new sources before the first-slice gate;
- copying assignments across linked rendition artifacts; and
- making a graph or search index the only canonical store.

## 7. Evaluation plan

### 7.1 Evaluation sets

Maintain four separate sets:

1. **Schema fixtures:** small, synthetic or license-permitted examples for
   conformance.
2. **Parser and identity gold:** real examples marked for structure, evidence,
   versions, and links.
3. **Development corpus:** examples used to tune retrieval, prompts, models,
   and policy.
4. **Sealed product holdout:** time-separated, source-stratified examples
   opened once for one evaluation generation.

Keep related versions, renditions, and near-duplicate artifacts in the same
split. Keep public participation in a separate privacy-approved corpus.

### 7.2 Required baselines

For semantic enrichment, compare on the same frozen holdout:

1. source metadata and deterministic fields only;
2. lexical closed-vocabulary assignment;
3. dense closed-vocabulary assignment;
4. direct grounded open-phrase generation;
5. open phrase generation followed by mapping and abstention; and
6. the typed hybrid.

For relationship discovery, compare:

1. explicit identifiers and citations only;
2. shared-anchor and lexical candidates;
3. dense passage candidates;
4. combined candidate generation without model adjudication;
5. predicate-specific adjudication; and
6. human-reviewed product output.

### 7.3 Measures

Report, at minimum:

- capture completeness and unaccounted gaps;
- text and attachment coverage;
- parsing and optical character recognition quality;
- evidence-selector resolution;
- identity and deterministic-link precision and recall;
- registry coverage;
- candidate Recall@K curves;
- final strict and concept-level precision and recall;
- unsupported-assignment rate;
- correct abstention and risk-versus-coverage;
- cross-facet confusion;
- rare, new, and time-shifted performance;
- inferred-relation precision by predicate;
- reviewer time, disagreement, correction rate, and queue size;
- vocabulary-release stability;
- per-source and per-subtype worst case;
- deterministic replay;
- latency, throughput, and cost; and
- user success on the priority questions.

### 7.4 Evaluation discipline

- Freeze inputs, releases, mappings, prompts, model versions, and policies
  before final scoring.
- Keep official topics as source evidence or silver labels within their real
  assignment scope.
- Blind reviewers to system identity when comparing approaches.
- Keep candidate recall separate from final adjudication quality.
- Report no-fit and abstention outcomes.
- Report source-family and predicate results before aggregates.
- Record every exclusion.
- After results are revealed, retire that holdout to audit-only status. Any
  model, mapping, threshold, registry, prompt, policy, or source-scope change
  informed by the result requires a newly sealed holdout generation before a
  new release claim.
- A mechanical rerun that changes no evaluated behavior may use the retired set
  to diagnose execution, but it does not count as a new independent release
  result.
- Reject any release argument based only on a development-set improvement or
  one global F1 score.

## 8. Decision and hypothesis register

WP0 creates the register; later work records evidence and outcomes.

| ID | Open decision | Required experiment or evidence |
| --- | --- | --- |
| `H01` | Does a governed general subject layer add product value? | Compare search, browse, alert, and reporting tasks against source-only and open-search baselines |
| `H02` | Which schemes belong in the output profile? | Registry coverage, strict relevance, rights, governance cost, and source-specific results |
| `H03` | What concept count is governable and useful? | Holdout coverage, reviewer load, overlap, stability, and product outcomes |
| `H04` | Does facet-separated retrieval improve quality? | Same-corpus ablation with entity/subject confusion and candidate recall |
| `H05` | Which lexical and dense channels should remain? | Recall@K, complementarity, latency, cost, and worst-source results |
| `H06` | Do metadata priors help without hiding cross-cutting concepts? | Prior-on/off comparison with a global escape path and time-split holdout |
| `H07` | Does open-phrase-then-map beat direct assignment? | Blind comparison with strict relevance, abstention, and reviewer time |
| `H08` | Does model reranking justify its cost? | Fixed candidate sets, calibrated acceptance, latency, cost, and product lift |
| `H09` | Which dependency predicates can be automated? | Predicate-specific blind precision, evidence sufficiency, abstention, and correction rate |
| `H10` | Which policy threads should become durable? | Coherence, scope stability, evidence, review cost, and user-task value |
| `H11` | Is a dedicated graph store needed? | Query workload and operating evidence after canonical data and rebuildable graph views exist |
| `H12` | Does one product overlay outperform serving source schemes separately? | Same-task comparison of source-native facets, reviewed mappings, and overlay output |
| `H13` | Does specialist-module activation improve quality without recall loss? | Per-domain activation-on/off test with cross-domain documents and global escape path |
| `H14` | Do hierarchy, definitions, aliases, scope notes, or generated label text improve retrieval? | Independently versioned ablations by scheme, source, and label representation |
| `H15` | Do corpus-induced concepts add value beyond open labels and registered concepts? | Blind product-task lift, stability, duplication, reviewer cost, and update behavior |

A failed hypothesis should remove or narrow a component. It should not trigger
threshold tuning on an exposed holdout; further work returns to development and
requires a new sealed evaluation generation.

## 9. Team and ownership

| Role | Accountable for |
| --- | --- |
| Program and product owner | Priority questions, scope, value, service levels, and release decision |
| Regulatory-domain owner | Source precedence, Rulespec warrants and authority, legal and process meaning, timelines, and disputed relationships |
| Rulespec kernel and profile steward | Portable types, predicates, epistemic basis, evidence, review, adoption, lifecycle, access, retention, generated artifacts, and semantic conformance |
| REF operational steward | Captures, source revisions, source-resource grouping, processing records, candidates, receipts, release operations, and Rulespec compatibility |
| Source steward per family | Native identifiers and types, completeness rules, rights, fixtures, and drift response |
| Data ingestion engineers | Capture, manifests, replay, storage, connectors, and monitoring |
| Document-processing engineer | Native parsing, PDF and Office files, optical character recognition, structure, and selectors |
| Knowledge-organization specialist | Rulespec schemes, facets, mappings, REF concept proposals, and concept governance |
| Information-retrieval and ML engineer | Candidate generation, adjudication, calibration, model interfaces, and inference receipts |
| Independent evaluation lead | Gold data, sealed holdout, blind review, metrics, and gate enforcement |
| Security and privacy owner | Participation, personally identifiable information, access, retention, untrusted files, and export review |
| Platform and reliability owner | Capacity, observability, backup, restore, publication, and operating cost |
| Human reviewers | Evidence-based attestations, adoption recommendations, correction, mapping, and dispute resolution |

The builder of a probabilistic component cannot serve as the only approver of
its gold data, release threshold, or production result.

### 9.1 Suggested minimum team

The evidence release needs sustained capacity from:

- one product/program owner;
- one regulatory-domain owner;
- one framework/data engineer;
- two source and document-processing engineers;
- one platform/reliability engineer; and
- one independent evaluation lead with protected holdout ownership; and
- shared security/privacy support.

The intelligence release adds sustained knowledge-organization and
information-retrieval/ML capacity plus a funded reviewer pool.

This is a staffing model, not a schedule estimate. Smaller teams can deliver
the same gates by reducing source scope and sequence speed.

## 10. Risk register

| Risk | Control and stop rule |
| --- | --- |
| Scope expands before one complete answer works | Freeze the three-matter slice. Admit no new family before `G5`. |
| Pagination or source limits silently omit records | Require coverage manifests and explicit incomplete status. Stop publication on unaccounted gaps. |
| Shared identifiers or hashes collapse distinct source resources or artifacts | Keep REF source identity and Rulespec assertions separate. Require zero false merges on gold. |
| Metadata refresh becomes a legal version | Enforce capture, source-record, source-resource-version, and rendition-role artifact separation. |
| Current views destroy history | Build views from append-only history and test as-of answers. |
| Metadata-only records look like full text | Require extraction state in every output. |
| Unknown values are guessed into familiar classes | Preserve raw values and quarantine unmapped values. |
| An aggregator overrides an official source | Apply REF source-precedence policy, preserve both Rulespec assertions, and expose applicable warrants and authority. |
| Parsing or OCR shifts evidence | Bind `rkaf:SourceFragment` selectors to rendition-role artifact digests and expose quality failures. |
| Model output becomes apparent fact | Require independent Rulespec assertion origin and epistemic basis, exact source-fragment evidence, safe provisional eligibility, and separate attestation and adoption. |
| Metadata priors hide cross-cutting concepts | Preserve a global candidate path and test recall. |
| A vocabulary crowds out correct labels | Separate schemes and facets; measure registry coverage before model quality. |
| Machine-generated concepts multiply without control | Keep REF concept proposals separate from Rulespec concepts and prohibit automatic promotion. |
| Review becomes the bottleneck | Set a review-volume budget and keep low-precision outputs review-only or query-time. |
| Similarity becomes dependency | Use disjoint predicates and negative query tests. |
| Cached links become permanent graph facts | Preserve query-time state; require a new Rulespec assertion for promotion. |
| Policy threads imply unsupported pairwise links | Represent membership with separate Rulespec assertions and explain the scope. |
| Public comments expose sensitive information | Keep participation protected and out of public output by default. |
| Attachments compromise processing | Isolate parsers and enforce file, size, decompression, and malware controls. |
| Evaluation leaks into tuning | Give the sealed holdout to an independent owner, open it once per evaluation generation, then retire it; any informed change requires a new sealed generation. |
| Search or graph technology becomes canonical | Rebuild all indexes from canonical releases and receipts. |
| REF and Rulespec drift into parallel semantic models | Run the ownership fixture suite; block any REF schema or field duplicating a Rulespec concern. |
| Source drift breaks a small but important family | Use per-source gates; never waive them with a global average. |
| The dated inventories become an accidental closed universe | Require every active later item in the portfolio manifest and use governed absolute-IRI extension routes with non-fit and round-trip proof. |

## 11. Release checklists

Every release checklist includes these portfolio controls:

- [ ] The release pins the exact immutable `InventoryCoverageManifest`, both
  baseline inventory identifiers and digests, and the
  `BaselineEnumerationReport` identifier, version, digest, counts, and
  extraction algorithm, plus its independent Rulespec audit attestation.
- [ ] Every table data row and named portfolio item has exactly one valid
  enumeration classification. Every named item, subtype group, and role inside
  table cells or outside tables has a source-located occurrence and valid
  resolution. Every required coverage account exists exactly once, every
  constituent row resolves to one parent, every definition or descriptive
  entry remains visible without a route, and every covered account has an
  exhaustive component set. Each component has exactly one route family, one
  independently justified route from that family, one source acquisition mode
  when applicable, semantic/use modes, and separate representability, adapter
  or import implementation, release inclusion, and rights/use authorization
  statuses.
- [ ] Every active source and controlled resource beyond the two dated minimum
  inventories is pinned in the same enumeration and coverage records. Every
  non-core route or processing value has a passing governed extension profile;
  no active item lives in an untracked side registry or catch-all bucket.
- [ ] The release report publishes accounted and unaccounted counts, counts by
  row, component, route family, route, acquisition mode, use mode, and status
  within each dimension; changes from the previous manifest; and stable keys
  for blocking entries.
- [ ] Unimplemented, deferred, external-join-only, mapping-only,
  rights-blocked, and unsupported components remain visible; the report does
  not describe full accounting as full ingestion or production coverage.
- [ ] If the release claims full-framework design coverage, every component
  has `supported` representability, a current concrete lossless representation
  mapping, and passing component-specific positive and round-trip fixtures.
  Implementation, release-inclusion, and rights statuses remain independently
  reported and may still be planned, deferred, blocked, not applicable, or not
  assessed.

### 11.1 Evidence release

- [ ] `G0` through `G5` and applicable `G8` controls pass.
- [ ] `REF-Core-Producer`, the explicit and deterministic
  `REF-Relationship-Producer` profile, the evidence-only `REF-Query-Service`
  profile, and applicable security and rights requirements validate.
- [ ] The release pins exact REF and Rulespec versions, immutable revisions,
  constraint and graph digests, profiles, validators, conformance levels, and
  machine-readable results, plus the inventory-coverage manifest required
  above.
- [ ] The REF validator and pinned Rulespec validator each pass their own
  conformance suite; the combined check preserves both result sets.
- [ ] The requirement trace has no unowned or untested applicable requirement.
- [ ] The ownership audit finds no REF duplicate of a Rulespec type, field,
  value set, or semantic validator.
- [ ] The three-matter slice meets its selection rule.
- [ ] Every source window reconciles or records a specific gap.
- [ ] Every discovered attachment resolves to bytes or a typed failure and has
  an extraction state when captured.
- [ ] Every concrete rendition is one `rkaf:Artifact`; REF stores only its
  processing state and source-resource grouping.
- [ ] Every published assertion resolves to `rkaf:SourceFragment` evidence or
  a Rulespec-supported derivation.
- [ ] Current-view conflict choices expose REF source-precedence policy plus
  Rulespec warrants, authority, attestations, and local adoption.
- [ ] `notFound` remains distinct from an REF `AbsenceEvaluation`.
- [ ] False identity merges equal zero on the gold set.
- [ ] Two deterministic replays produce identical canonical payload identifiers
  and semantic digests while retaining distinct run provenance.
- [ ] As-of answers match the gold timeline.
- [ ] Exports preserve REF operational records and the lossless Rulespec graph;
  they do not copy Rulespec epistemic, review, authority, lifecycle, access,
  retention, or use fields into REF.
- [ ] Backup, restore, atomic publication, and rollback pass.
- [ ] The independent analyst signs the evidence package.
- [ ] Known limits and excluded or non-included sources and reference resources
  appear in the release notes by stable inventory key.

### 11.2 Release 2A: typed enrichment

- [ ] `G6` and applicable `G8` controls pass for each enabled facet and profile.
- [ ] `REF-Enrichment-Producer`, `REF-Reference-Resource-Registry`, and the
  extended `REF-Query-Service` operational profiles validate, and the
  applicable Rulespec reference-resource graph passes L3/L4.
- [ ] The requirement trace has no unowned or untested applicable requirement.
- [ ] Rulespec epistemic basis, assertion origin, attestation, local adoption,
  lifecycle, confidence, authority, and use eligibility remain distinct from
  REF workflow state.
- [ ] No-fit cases abstain, produce an authorized open-label
  `rkaf:ValueAssertion`, or create an REF concept proposal.
- [ ] Every attempted target × facet × assignment role has a visible
  REF enrichment decision, whose `accepted` outcome is pipeline state rather
  than review approval.
- [ ] Every decision links its immutable REF output profile and resulting
  Rulespec assertion or assignment.
- [ ] Every accepted controlled assignment is an
  `rkaf:ConceptAssignment` using a Rulespec assignment-role predicate, exact
  complete-membership `rkaf:assignedConceptRelease` pin,
  `rkaf:EvidenceBinding`, required attestation, and local adoption.
- [ ] Every registry import has an REF `RegistryImportSnapshot`; every
  deployment has an REF `RegistryDeploymentDecision`; neither duplicates the
  source bytes, transport metadata, or acquisition digest from `Capture`; the
  Rulespec release identifier, version, membership mode or claims,
  distributions, or semantic digest; or a distribution artifact's canonical
  identity or byte digest.
- [ ] Every mapping path resolves exact `rkaf:ConceptMapping` identifiers,
  source and target complete-membership Rulespec reference-resource-release
  pins, and its REF mapping-set `RegistryImportSnapshot`.
- [ ] Every non-enumerated identifier or schema authority pins its exact
  authoritative grammar, resolver definition, or native content as a
  distribution without copying a member list into REF or authorizing
  concept-assignment or mapping pins.
- [ ] Registry coverage, reachable-gold Recall@K curves, end-to-end recovery,
  and final quality pass by source and subtype.
- [ ] Every accepted assignment has exact `rkaf:SourceFragment` and
  `rkaf:EvidenceBinding` records or a Rulespec-supported deterministic
  derivation.
- [ ] Registry refresh, failed selection, rollback, and historical resolution
  tests pass.
- [ ] Registry and output-profile deployment changes append REF operational
  decisions; review, authorization, lifecycle, access, retention, and use
  changes append Rulespec and ODRL records without mutating releases.
- [ ] Review volume and correction rate fit the operating budget.
- [ ] Product-task results beat the approved baselines on the sealed holdout.
- [ ] Privacy, rights, model-safety, and derived-disclosure reviews pass.

### 11.3 Release 2B: approved inferred relationships

- [ ] `G7` and applicable `G8` controls pass for each enabled predicate.
- [ ] Each predicate IRI and semantic definition is pinned from Rulespec or an
  adopted ontology; REF defines only persistence, materiality, review,
  evaluation, and publication policy.
- [ ] Each claimed inferred-relationship profile and resulting Rulespec graph
  validate.
- [ ] Every accepted inferred relationship is an
  `rkaf:RelationshipAssertion` with exact Rulespec evidence or declared input
  assertions and conforming provenance.
- [ ] Every automated dependency is scoped and predicate-specific.
- [ ] Similarity remains query-time by default.
- [ ] Review volume and correction rate fit the operating budget.
- [ ] Product-task results beat the approved baselines on the sealed holdout.
- [ ] Privacy, rights, model-safety, and derived-disclosure reviews pass.

### 11.4 Release 2C: optional policy threads

- [ ] `G7T` passes.
- [ ] `REF-Policy-Thread-Publisher` validates.
- [ ] Every durable membership is an `rkaf:RelationshipAssertion` with
  applicability, epistemic basis, source-fragment evidence, provenance,
  attestation, local adoption, lifecycle, and history.
- [ ] Thread membership creates no implied identity, causation, dependency, or
  pairwise relationship.
- [ ] Competing and overlapping threads remain representable.

## 12. Definition of done

The framework is implemented for a release only when:

1. The release answers its approved product questions.
2. Every answer exposes Rulespec assertion origin, epistemic basis,
   attestation, local adoption, lifecycle, and unresolved REF processing state
   without collapsing them.
3. Every durable semantic result is a validated Rulespec record that resolves
   to Rulespec evidence and provenance.
4. The system reproduces history and as-of state.
5. The declared REF classes and pinned Rulespec levels pass separate,
   requirement-level validation for the exact release and graph digests.
6. Independent evaluation clears every source, facet, and predicate gate in
   scope.
7. Security, privacy, rights, backup, restore, rollback, and failure tests pass.
8. An REF `PublicationReleaseManifest`, REF receipt, Rulespec conformance
   report, limitations statement, and correction path are published.
9. The ownership audit finds no duplicate semantic type, field, value set,
   validator, or decision record across REF and Rulespec.
10. The release pins a baseline-enumeration report that validly dispositions
    100 percent of table data rows and source-located named-item, subtype-group,
    and role occurrences in both dated inventories and carries a passing
    independent Rulespec audit attestation. It also pins an inventory-coverage
    manifest with exactly every required row account, one parent for every
    constituent row, no route for definition or descriptive entries, and
    exhaustively routed components for every covered constituent and role. No
    required account or component is missing, duplicate, placeholder, or
    unclassified, and semantic/use modes and all four status dimensions remain
    separate. The same proof includes every additional inventory and active
    item declared by the implementation, not only the two dated minimum
    inventories.
11. A full-framework design-coverage claim is made only when every component
    has `supported` representability backed by a current concrete lossless
    mapping and passing component-specific positive and round-trip fixtures; it
    does not imply that every adapter or import is implemented, every resource
    is included in a release, or every use is authorized.

Passing this definition for the first federal slice authorizes controlled
source expansion. It does not establish comprehensive federal coverage,
universal legal accuracy, or automatic acceptance of new models,
vocabularies, predicates, or source families.
