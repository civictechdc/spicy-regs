<!-- markdownlint-disable MD013 -->

# REF Rulespec Application Profile

## Editor's Draft, 28 July 2026

> **Short name:** REF-RKAF
>
> **REF specification:** [Regulatory Evidence Framework 1.0](regulatory-evidence-framework.md)
>
> **Implementation plan:** [REF implementation plan](regulatory-evidence-framework-implementation-plan.md)
>
> **Development compatibility target:** Rulespec `0.2.0-pre.8`
>
> **Status:** Normative profile under joint REF and Rulespec development

## 1. Purpose

This profile binds the Regulatory Evidence Framework (REF) to Rulespec. It
keeps one canonical representation for every portable semantic fact:

- REF records how source material was acquired, processed, evaluated, and
  published.
- Rulespec records what an accepted or exchangeable semantic result says, what
  supports it, how it was produced, who reviewed or adopted it, and how it may
  be used.

This profile does not restate Rulespec class definitions or field
cardinalities. The exact Rulespec constraint bundle pinned by a release is the
authority for those definitions.

## 2. Dependency and pin status

The development target is Rulespec `0.2.0-pre.8`. At the date of this draft,
the required upstream changes are still uncommitted. This document therefore
does not claim a final compatible Git commit or constraint-bundle digest.

**REF-RKAF-PIN-001:** Before an REF implementation publishes a conformance
claim, its immutable REF `PublicationReleaseManifest` MUST contain:

| Field | Required value |
| --- | --- |
| REF version | Exact version and draft date |
| REF binding | Exact operational serialization-profile identifier, version, and digest |
| Portfolio coverage | Exact inventory-coverage manifest identifier and digest plus both baseline inventory identifiers and digests |
| Portfolio result | Separate `portfolioAccounting` and `fullFrameworkDesignCoverage` results |
| Rulespec version | Exact semantic version |
| Rulespec revision | Immutable release identifier or Git commit |
| Constraint digest | Algorithm and digest of the exact Rulespec constraint bundle |
| Rulespec profiles | Every adopted core, jurisdiction, analysis, or application profile |
| Rulespec conformance | Claimed level and machine-readable result |
| Rulespec validator | Validator and conformance-suite versions |
| REF validator | Validator and conformance-suite versions |
| Application profile | This profile's immutable identifier, version, and digest |

**REF-RKAF-PIN-002:** A development build MAY identify
`0.2.0-pre.8` as its compatibility target while the upstream work is
uncommitted. It MUST mark the Rulespec revision, constraint digest, and
conformance result `pending` and MUST NOT publish a production conformance
claim.

**REF-RKAF-PIN-003:** Changing any pinned Rulespec constraint, context, shape,
profile, behavior, or fixture MUST create a new REF compatibility result. A
version string without the exact revision and digest is insufficient.

## 3. Ownership rules

This profile uses three ownership labels:

- **Rulespec-owned:** the type, field, value set, invariant, and semantic
  validator live only in Rulespec.
- **REF-owned:** the operational record and behavior live only in REF.
- **Profile-owned:** this document specifies how REF operations select or
  reference Rulespec records; it does not redefine either record.

**REF-RKAF-OWN-001:** A Rulespec-owned field MUST NOT appear as a second,
independently authoritative field on an REF record.

**REF-RKAF-OWN-002:** An REF-owned workflow state MUST NOT be interpreted as
Rulespec attestation, adoption, authority, lifecycle, access, retention,
epistemic basis, or use eligibility.

**REF-RKAF-OWN-003:** If a required portable meaning is absent from Rulespec,
the project MUST change Rulespec or adopt an external standard through a
Rulespec profile. REF MUST retain the information as operational data and mark
the projection blocked; it MUST NOT create a substitute semantic type.

## 4. Resource mapping

| REF input or output | Canonical portable representation | Owner and rule |
| --- | --- | --- |
| `Capture` | None | REF-owned exact retrieval record |
| `SourceRecordRevision` | None | REF-owned decoded source state |
| `SourceResource` | None | REF-owned logical source grouping |
| `SourceResourceVersion` | None | REF-owned publisher-recognized version grouping |
| `BaselineEnumerationReport` | Rulespec attestation reference | REF owns the content-digested row, cell, source-span, named-item, subtype-group, and role enumeration; Rulespec owns the independent audit decision |
| `InventoryCoverageManifest` | None | REF-owned portfolio accounting that references inventory rows and canonical resources without copying them |
| `SourcePrecedencePolicy` | Rulespec warrant, authority, attestation, and adoption references | REF owns source-selection logic; Rulespec owns trust and authorization |
| Rendition role | `rkaf:Artifact` | The Rulespec artifact is the rendition; no REF rendition object exists |
| `RenditionProcessingRecord` | None | REF-owned extraction and quality state referencing one artifact |
| Transient `EvidenceAddress` | None | REF-owned selector input |
| Successful `SelectorResolution` | `rkaf:SourceFragment` | REF record points to the Rulespec fragment; it does not duplicate it |
| Evidence on an assertion | `rkaf:EvidenceBinding` | Rulespec-owned role, evidentiary function, and fragment binding |
| `EvidenceCollectionPolicy` | None | REF-owned search universe and materiality policy linked by adjudication |
| Relationship candidate | None | REF-owned candidate and retrieval signals |
| Relationship adjudication decision | Resulting Rulespec assertion identifier | REF-owned outcome; `accepted` is pipeline state, not review approval |
| Accepted resource relationship | `rkaf:RelationshipAssertion` | Rulespec-owned proposition and semantic context |
| Accepted typed literal | `rkaf:ValueAssertion` | Rulespec-owned proposition and semantic context |
| Accepted open-label role | `rkaf:ValueAssertion` | The profile pins the upstream predicate; REF defines no `OpenLabel` class |
| Enrichment candidate | None | REF-owned candidate, scores, and channels |
| `SemanticReferenceCandidate` | Externally typed resource plus Rulespec assertions after acceptance | REF candidate is not a portable semantic class |
| `AcceptancePolicy` | None | REF-owned candidate disposition policy |
| `EnrichmentDecision` | Resulting Rulespec assertion or assignment identifiers | REF-owned attempt and pipeline outcome |
| `OutputProfile` and deployment decision | Rulespec attestation and local adoption references | REF owns allowed pipeline output and deployment; Rulespec owns approval and use |
| Accepted controlled-concept assignment | `rkaf:ConceptAssignment` | Rulespec-owned assignment |
| Concept proposal | None | REF-owned workflow record until promotion |
| Promoted concept | `rkaf:LocalConcept` or `rkaf:RegisteredConcept` | Rulespec-owned concept selected by governance scope |
| Accepted concept mapping | `rkaf:ConceptMapping` | Rulespec-owned mapping |
| `RegistryImportSnapshot` | None | REF-owned import provenance for every controlled-resource kind, including mapping sets; references captures or external references, transformations, exclusions, validation, Rulespec releases, and distributions without owning their bytes or digests |
| Reference-resource release | `rkaf:ReferenceResourceRelease` | Rulespec-owned semantic manifest with release identity, version, resource kind, membership mode and claims, distributions, and `rkaf:referenceReleaseDigest` |
| Registry deployment | `RegistryDeploymentDecision` | REF-owned environment and index selection |
| `PublicationReleaseManifest` | None | REF-owned published output package; pins Rulespec and inventory coverage but is distinct from `rkaf:ReferenceResourceRelease` |
| `RunReceipt` | Rulespec provenance references | REF-owned operational/provider-native detail; copied Rulespec values are non-authoritative |
| Extraction run projection | `rkaf:ExtractionActivity` | Rulespec portable subset; REF `RunReceipt` retains operational detail |
| Model derivation | `rkaf:AILineage` | Rulespec-owned |
| Confidence | `rkaf:ConfidenceRecord` | Rulespec-owned |
| Review or approval | `rkaf:Attestation` | Rulespec-owned |
| Authorization for product use | `rkaf:LocalAdoption` and `rkaf:usageEligibility` | Rulespec-owned |
| Warrant or legal/source authority | `rkaf:Warrant` or `rkaf:Authority` | Rulespec-owned |
| Semantic lifecycle | `rkaf:LifecycleEvent` and Rulespec consumer lifecycle | Rulespec-owned |
| Access and retention | `rkaf:AccessScope` and `rkaf:RetentionPolicy` | Rulespec-owned |
| `SimilarityObservation` | None | REF-owned transient association |
| `AbsenceEvaluation` | None | REF-owned bounded operational result, not a claim about the world |
| Policy thread | None | REF application view; durable membership uses `rkaf:RelationshipAssertion` |
| Participation processing record | Applicable Rulespec profile or external type after privacy approval | REF-owned protected routing record, not a semantic class |
| `RightsAssessment` | Rulespec access, retention, usage, attestation, adoption, and ODRL records | REF owns observed terms and workflow only |

**REF-RKAF-MAP-001:** A record listed as having no portable representation MUST
remain outside the Rulespec graph unless another adopted profile supplies a
non-duplicative type.

**REF-RKAF-MAP-002:** Cross-record links MUST be identifiers. A serializer MUST
not embed a copied Rulespec object as mutable REF state or copy an REF workflow
record into a Rulespec semantic field.

**REF-RKAF-MAP-003:** An REF extension route or `recordKind` is an operational
dispatch value, not a portable semantic class. Its profile MUST bind portable
outputs to existing Rulespec records, an adopted external standard, or an
upstream Rulespec extension. It MUST NOT mint an REF semantic substitute when
none of those can carry the required meaning.

**REF-RKAF-PORT-001:** The independent audit of a
`BaselineEnumerationReport` MUST be an `rkaf:Attestation` that targets the
exact immutable report identifier, uses `rkaf:formalReviewer` as
`rkaf:attestorKind`, uses
`urn:ref:attestation-scope:baseline-enumeration-exhaustiveness:v1` as
`rkaf:attestationScope`, and identifies the independent reviewer and audit
time. The attestor MUST differ from the report's `recordedBy` agent and
declared coverage owner. Its `rkaf:targets` MUST also identify the versioned
separation-of-duties policy and immutable audit-evidence artifact or packet,
and its `rkaf:rationale` MUST explain the independence check.
Only an `rkaf:approved` decision that the pinned applicable Rulespec L4
attestation-effectivity behavior finds effective and unrevoked satisfies
`REF-PORT-011` and `REF-RKAF-CONF-002`. REF MAY report that result in its
conformance view, but it MUST NOT copy the decision, reviewer, independence
evidence, or effective state into the report.

## 5. Rendition and source-fragment binding

### 5.1 Rendition role

One immutable `rkaf:Artifact` plays the rendition role for one concrete XML,
HTML, PDF, image, Office, extracted-text, or other representation. A
`SourceResourceVersion` may group several such artifacts.

**REF-RKAF-ART-001:** The artifact's Rulespec identifier, identifier scheme,
content digest, format relations, and version-lineage evidence MUST be recorded
only under the pinned Rulespec shape.

**REF-RKAF-ART-002:** `SourceResource` and `SourceResourceVersion` identifiers
MAY be targets of Dublin Core version relations when the pinned Rulespec
profile permits them. They MUST NOT be typed `rkaf:Artifact` merely to satisfy
the relation.

**REF-RKAF-ART-003:** `RenditionProcessingRecord` MAY contain extraction state,
parser version, source locator, byte length, and quality observations. It MUST
reference the artifact and MUST NOT repeat the artifact's semantic identifier
or content digest as a second authority.

### 5.2 Fragment publication

**REF-RKAF-FRAG-001:** A successful selector resolution MUST publish or resolve
one `rkaf:SourceFragment` whose `oa:hasSource` is the rendition-role artifact.

**REF-RKAF-FRAG-002:** The fragment MUST use a selector kind and coordinate
system permitted by the pinned Rulespec profile. Comparison evidence and
evidence for an accepted assertion MUST carry the source-artifact and fragment
digests Rulespec requires.

**REF-RKAF-FRAG-003:** A field path, page region, table cell, text quote, or
text position that cannot be represented by the current Rulespec selector
profile MUST remain an unresolved REF selector record until Rulespec or an
adopted Web Annotation extension supports it.

## 6. Assertion and assignment binding

### 6.1 Independent axes

Rulespec `rkaf:assertionOrigin` records what constructed the record.
Rulespec `rkaf:epistemicBasis` records why the proposition may be believed.
Attestation, local adoption, confidence, and lifecycle remain separate records.

| REF processing situation | Rulespec construction origin | Rulespec epistemic basis |
| --- | --- | --- |
| Imported source assertion | `rkaf:imported` | `rkaf:sourceExplicit` |
| Deterministic parser extracts a source statement | `rkaf:deterministicExtraction` | `rkaf:sourceExplicit` |
| Deterministic join derives a new proposition | `rkaf:deterministicExtraction` | `rkaf:deterministicDerivation` |
| Model extracts a proposition stated by the source | `rkaf:aiSuggested` | `rkaf:sourceExplicit` |
| Model infers an unstated relationship | `rkaf:aiSuggested` | `rkaf:statisticalInference` |
| Analyst authors a scoped product assertion | `rkaf:humanAsserted` | `rkaf:editorialAssertion` |
| User directly proposes a connection | `rkaf:humanAsserted` | `rkaf:userAssertion` |
| User proposal imported from another system | `rkaf:imported` | `rkaf:userAssertion` |

The table deliberately does not collapse construction and belief. A model can
extract a source-explicit statement, and a human can record a statistical
inference.

**REF-RKAF-ASSERT-001:** Durable assertions and concept assignments MUST carry
the Rulespec epistemic basis required by the pinned `0.2.0-pre.8` constraint
bundle.

**REF-RKAF-ASSERT-002:** An `rkaf:aiSuggested` result MUST retain the provisional
usage ceiling required by Rulespec. A later attestation or local adoption MUST
not rewrite its origin or epistemic basis.

**REF-RKAF-ASSERT-003:** An accepted relationship MUST be an
`rkaf:RelationshipAssertion`; a literal result MUST be an
`rkaf:ValueAssertion`; and a controlled-concept result MUST be an
`rkaf:ConceptAssignment`. REF schemas MUST not reproduce their fields.

**REF-RKAF-ASSERT-004:** The `0.2.0-pre.8` construction-origin values are
`rkaf:humanAsserted`, `rkaf:aiSuggested`, `rkaf:imported`, and
`rkaf:deterministicExtraction`. Promotion, qualification, review, and
revalidation MUST use the applicable `rkaf:Attestation`,
`rkaf:LocalAdoption`, `rkaf:LifecycleEvent`, successor assertion, and
derivation records; they MUST NOT be encoded as construction origins.

**REF-RKAF-ASSERT-005:** An accepted open-label value's portable wording,
language, and script MUST be represented in the Rulespec graph or an adopted
standard in the pinned Rulespec profile. Candidate-stage language or script on
an REF decision MAY support processing but MUST NOT be the sole portable copy.
If the pinned profile cannot represent that meaning, the projection is blocked
under `REF-RKAF-OWN-003` or limited to a declared language-neutral or
default-language output mode.

**REF-RKAF-ASSERT-006:** A concept assignment MUST use the normalized
Rulespec `rkaf:ConceptAssignment` specialization of
`rkaf:RelationshipAssertion`: the assignment role is its predicate IRI, its
polarity is affirmed, all evidence uses `rkaf:EvidenceBinding`, and derivation
uses Rulespec's PROV, justification, and warrant paths. REF MUST NOT add a
parallel assignment shape.

### 6.2 Evidence

**REF-RKAF-EVID-001:** Evidence for a durable Rulespec assertion MUST use
`rkaf:EvidenceBinding`.

**REF-RKAF-EVID-002:** When a binding cites source fragments, it MUST carry the
Rulespec evidence role and evidentiary function required by the pinned
`0.2.0-pre.8` constraint bundle. Candidate-stage evidence labels are not a
portable substitute.

**REF-RKAF-EVID-003:** A structured source field MUST resolve to an
`rkaf:SourceFragment` before it can serve as cited evidence. REF does not
define a separate structured-warrant evidence primitive.

### 6.3 Review, adoption, and lifecycle

**REF-RKAF-DEC-001:** Review decisions MUST be `rkaf:Attestation` records
targeting the Rulespec semantic record.

**REF-RKAF-DEC-002:** Authorization for local operational, publication, or
official use MUST be a scoped `rkaf:LocalAdoption` and corresponding Rulespec
usage eligibility. Attestation alone does not authorize use.

**REF-RKAF-DEC-003:** Retraction, supersession, amendment, or revalidation MUST
use Rulespec assertion lineage and `rkaf:LifecycleEvent` as applicable. REF
workflow-state changes do not replace those records.

## 7. Registry binding

Rulespec `rkaf:ReferenceResourceRelease` is the only canonical release
identity, version, resource-kind value, membership mode and claims,
distributions, and RDFC-1.0 semantic `rkaf:referenceReleaseDigest` for an
imported subject scheme,
ontology, identifier authority, entity registry, code list or classification,
schema, or mapping set. The release is the semantic manifest. Its distribution
`rkaf:Artifact` records retain their byte digests. Rulespec keeps
`dcterms:type` open; the REF coverage routes do not define a closed upstream
value set.

The compatibility target uses `dcterms:isVersionOf`, `dcat:version`,
`dcterms:type`, `rkaf:membershipMode`, conditional `prov:hadMember`,
`dcat:distribution`, optional `dcterms:issued`, and
`rkaf:referenceReleaseDigest`. The pinned Rulespec constraints, not REF,
define their cardinalities and digest scope.

Native SKOS, OWL, code-system, and schema distributions remain canonical for
external reference resources. `rkaf:ReferenceResourceRelease` pins those
distributions and states how membership is represented:

- `rkaf:completeMembership` enumerates the complete member set and is the only
  mode that may back `rkaf:ConceptAssignment` or `rkaf:ConceptMapping`
  endpoint pins;
- `rkaf:partialMembership` enumerates an explicitly incomplete member set and
  cannot satisfy those pin constraints; and
- `rkaf:membershipNotEnumerated` pins a release's authoritative grammar,
  resolver definition, or native content as a distribution without
  `prov:hadMember`. It cannot prove that an individual identifier is a release
  member.

Rulespec `rkaf:LocalConcept` and `rkaf:RegisteredConcept` constraints apply
only when those Rulespec-owned classes are actually used. This profile does
not imply that their current shapes preserve every multilingual label, note,
or notation in an external distribution.

Every REF controlled-resource coverage route—`subjectScheme`, `ontology`,
`identifierAuthority`, `entityRegistry`, `codeList`, `classification`,
`schema`, and `mappingSet`—uses this release mechanism. The adopted Rulespec
profile or external standard selects the actual `dcterms:type` IRI; the REF
route string is not copied into the Rulespec graph as a new semantic type.

**REF-RKAF-REG-001:** Every `RegistryImportSnapshot` MUST reference one
`rkaf:ReferenceResourceRelease`, its applicable distribution artifacts, and
the REF `Capture` records for retrieved inputs or explicit external-reference
records for inputs not retrieved by REF. It MAY record transformations,
exclusions, failures, rights-assessment references, and validation reports. It
MUST NOT copy capture bytes or digests or mint or copy an independently
authoritative release version, membership mode or claims, distributions,
`rkaf:referenceReleaseDigest`, or distribution artifact identity or byte
digest.

**REF-RKAF-REG-002:** Every `rkaf:ConceptAssignment` MUST use
`rkaf:assignedConceptRelease` to identify the exact
`rkaf:ReferenceResourceRelease` containing the concept definition used and
MUST target the exact member IRI from that release. The release MUST use
`rkaf:completeMembership`.

**REF-RKAF-REG-003:** Every `rkaf:ConceptMapping` MUST use the Rulespec source
and target release-pin properties that identify the exact
`rkaf:ReferenceResourceRelease` records used. Both releases MUST use
`rkaf:completeMembership`.

**REF-RKAF-REG-004:** `RegistryDeploymentDecision` controls REF index and
publication selection only. Review and authorization use Rulespec attestation
and local adoption; deployment state is not concept or mapping lifecycle.

**REF-RKAF-REG-005:** A compound catalog row MUST decompose into separate REF
coverage components for each named resource and role. Each imported component
MUST reference its exact `rkaf:ReferenceResourceRelease` and distributions.
No constituent may disappear because another constituent shares a publisher,
row, label, or distribution.

**REF-RKAF-REG-006:** An identifier authority, schema authority, or other
reference resource whose members are dynamic, unbounded, confidential, or not
published as a complete set MAY use `rkaf:membershipNotEnumerated`. Its
release MUST pin the exact authoritative grammar, resolver definition, or
native content as a distribution. REF MUST NOT treat that release as proof of
individual identifier membership or use it as an assignment or mapping release
pin.

**REF-RKAF-REG-007:** A mapping-set import MUST use the same
`RegistryImportSnapshot` record shape as every other controlled-resource
import. REF MUST NOT define a separate `MappingImportSnapshot` type.

## 8. Relationship predicate profile

Durable predicate meaning belongs upstream in a Rulespec regulatory-evidence
profile or in an adopted external ontology. REF owns candidate generation,
relation-specific adjudication, evaluation, and query-time association.

The upstream profile must cover the product's approved subset of these
families:

| Family | Required distinctions |
| --- | --- |
| Format and version | Format-of, version-of, and revision must remain distinct |
| Lifecycle | Correction, withdrawal, supersession, and effective-date delay must remain distinct |
| Structure | Part, attachment, filing container, and publication form |
| Citation | Citation and non-authoritative mention |
| Legal authority | Authorization, requirement, and delegated authority |
| Legal action | Amendment, codification, repeal, challenge, and decision |
| Operational dependency | Definition, requirement, dataset, procedure, standard, and finding dependencies |
| Evidentiary relation | Support, qualification, and contradiction at assertion scope |
| Editorial grouping | Policy-thread membership and recommended context |
| Identity candidate | Possible identity without destructive merge |

**REF-RKAF-PRED-001:** This table does not mint predicate IRIs. A predicate is
publishable only after the Rulespec profile or adopted ontology defines its
IRI, domain and range, direction, inverse, symmetry, transitivity or other
closure behavior, and temporal meaning. REF separately owns
evidence-collection, persistence, materiality, review, evaluation, and
publication policy for using that predicate.

**REF-RKAF-PRED-002:** General similarity remains an REF query-time result and
MUST NOT be published as a durable Rulespec assertion unless a separately
defined predicate and evidence policy apply.

## 9. Rights binding

Rulespec owns access scope, retention, privacy classifications incorporated
from DPV, and usage eligibility. Rulespec's overlay mechanism is the extension
point for full ODRL rights expressions.

**REF-RKAF-RIGHTS-001:** Acquisition, storage, indexing, model use, display,
redistribution, attribution, purpose, and audience permissions MUST use the
Rulespec-approved ODRL overlay selected by this profile. Until that exact
overlay and its fixtures land, the projection is blocked under
`REF-RKAF-OWN-003`.

**REF-RKAF-RIGHTS-002:** REF `RightsAssessment` records observed terms,
evidence, and workflow. It MUST reference, not duplicate, the resulting
Rulespec and ODRL policy records, attestations, and local adoptions.

## 10. Conformance composition

An REF validator validates only REF operational records and cross-record
integrity. The Rulespec validator validates only Rulespec semantic records and
behavior.

**REF-RKAF-CONF-001:** A release containing portable semantic records MUST pass
Rulespec L3 conformance for its pinned graph and every adopted profile.

**REF-RKAF-CONF-002:** A release or service claiming approved current views,
local operational use, publication eligibility, cascade behavior, or
point-in-time consumer behavior MUST also pass the applicable Rulespec L4
conformance.

**REF-RKAF-CONF-003:** The REF validator MUST:

1. validate REF operational records;
2. verify identifier references from REF records to Rulespec records;
3. invoke the pinned Rulespec validator without translating its constraints;
4. verify that the Rulespec result covers the exact graph digest in the REF
   release;
5. report REF and Rulespec failures separately; and
6. fail the combined profile when either required result fails.

**REF-RKAF-CONF-004:** Generated REF implementation types MUST exclude
Rulespec-owned types. Implementations MUST consume the generated types or
schemas from the pinned Rulespec release.

## 11. Upstream completion gate

The `rkaf:ReferenceResourceRelease` name and shape, semantic digest algorithm
and scope, and assignment and mapping release-pin property names in this draft
are compatibility targets. They remain pending until the Rulespec editor
confirms them in authoritative CUE and all generated artifacts.

The following changes are required in Rulespec `0.2.0-pre.8` before this
profile can claim compatibility:

- a required epistemic-basis axis on every durable assertion form;
- a construction-only `rkaf:assertionOrigin` value set, with promotion,
  qualification, review, and revalidation represented through attestation,
  adoption, lifecycle, successor, and derivation records;
- a safe provisional-use ceiling for unreviewed AI suggestions;
- evidence role and evidentiary function on `rkaf:EvidenceBinding`;
- `dcterms:format` media type on rendition-role `rkaf:Artifact`;
- `rkaf:hasAccessScope` and `rkaf:hasRetentionPolicy` on `rkaf:Artifact`,
  `rkaf:SourceFragment`, and `rkaf:EvidenceBinding`, with conservative
  parent/child enforcement;
- immutable `rkaf:ReferenceResourceRelease` identity, version, resource-kind
  value, membership mode and claims, and distributions using
  `dcterms:isVersionOf`, `dcat:version`, `dcterms:type`,
  `rkaf:membershipMode`, conditional `prov:hadMember`,
  `dcat:distribution`, and optional `dcterms:issued` for
  subject schemes, ontologies, identifier authorities, entity registries, code
  lists or classifications, schemas, and mapping sets;
- membership-mode constraints and fixtures proving complete, partial, and
  non-enumerated membership, including the prohibition on assignment or
  mapping pins to a release without complete membership;
- an RDFC-1.0 semantic `rkaf:referenceReleaseDigest` on that release and byte
  digests on distribution `rkaf:Artifact` records, with fixtures proving their
  distinct scopes, and a standard Rulespec conformance hook that recomputes the
  semantic digest and rejects a wrong but well-formed value;
- exact reference-resource release pins on concept assignments and source and
  target release pins on concept mappings;
- mutually exclusive typed-literal and BCP 47 language-tagged-string branches
  on `rkaf:ValueAssertion`, with validation parity across generated targets;
- normalized `rkaf:ConceptAssignment` as an affirmed
  `rkaf:RelationshipAssertion` specialization, with assignment roles as
  predicate IRIs, evidence through `rkaf:EvidenceBinding`, derivation through
  Rulespec provenance, justification, and warrant paths, and one
  `rkaf:assignedConceptRelease` pin;
- `rkaf:ConceptMapping` composition with the durable assertion envelope;
- only the five SKOS mapping predicates on cross-scheme concept mappings;
- fixtures and generated artifacts for every change above; and
- prose, vocabulary, context, shape, behavior, and conformance parity.

The following application-profile work remains upstream after those kernel
changes:

- regulatory-evidence predicate IRIs and their constraints;
- an open-label `rkaf:ValueAssertion` predicate and profile rules for required,
  optional, or declared default language;
- multilingual label, note, and notation support for project-authored
  `rkaf:LocalConcept` and `rkaf:RegisteredConcept` records when the REF profile
  claims that capability, or an explicit profile limitation that leaves such
  content canonical only in native distributions;
- a policy-thread membership predicate;
- any domain types needed for accepted semantic-reference candidates;
- the exact ODRL overlay and derived-use behavior; and
- profile fixtures proving all mappings in this document.

**REF-RKAF-GATE-001:** A listed item is complete only when the authoritative
Rulespec source, generated artifacts, positive and negative fixtures, and
applicable conformance tests agree. Documentation alone does not close it.

**REF-RKAF-GATE-002:** REF MUST NOT ship a local substitute while an item is
open. The dependent REF capability remains unsupported or internal-only.
