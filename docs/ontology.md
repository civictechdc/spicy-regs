# Metadata ontology and Rulespec Level-0 mapping

Spicy Regs is a Rulespec **L0 Vocabulary** consumer. Its carrier is flat Apache
Parquet, not JSON-LD: compact identifiers and enum values expand
deterministically to Rulespec terms, but the dataset does not claim Rulespec
L1–L4 parsing, shape, constraint, or runtime conformance.

The implementation targets the repaired candidate US identifier and
Experimental rulemaking contract whose content digest is
`sha256:2aefd3fad7782a7b16a7fa8fc08e8ceb26b5db741e0371b8fa8a9ccc1982124d`
(2026-07-24). This digest pins the exact local contract used by the carrier
audit; it is not a release claim. The local paired corpus receipt is complete.
The module remains Experimental
until a maintainer publishes the contract and a non-originating consumer
reviews or ratifies it.

See the measured [stabilization report](rulespec-repair-report.md) for the
paired before/after corpus evidence. The earlier
[full-corpus friction report](ontology-friction-report.md) records the
pre-review consumer run. The stabilization findings are recorded in the sibling Rulespec repository at
`thoughts/reviews/2026-07-24-rulemaking-condition2-adversarial-review.md`.

## Identifier expansion

Parquet keeps the compact join keys users already query. Expand them as follows:

| Carrier value | Rulespec scheme | Canonical identifier |
| --- | --- | --- |
| CFR `40-60` / `40-60.1` | `rkaf:us-cfr` | `urn:rkaf:us:cfr:40:60` / `urn:rkaf:us:cfr:40:60.1` |
| U.S.C. title `42` + section `7401` | `rkaf:us-usc` | `urn:rkaf:us:usc:42:7401` |
| Proceeding id `pr-2060-AV16-0c9a` | `rkaf:partner-defined` | `urn:spicy-regs:proceeding:pr-2060-AV16-0c9a` |
| RIN `2060-AV16` | `rkaf:us-rin` | `urn:rkaf:us:rin:2060-AV16` (identity of a durable Regulatory Agenda item, never a Proceeding) |
| FR document `2024-00366` | `rkaf:us-frdoc` | `urn:rkaf:us:frdoc:2024-00366` |
| FR document resource, including legacy/correction ids | `rkaf:urn-persistent` | `https://www.federalregister.gov/d/<document-number>` |
| regulations.gov id `EPA-HQ-OAR-2021-0317` | `rkaf:us-regsgov` | `urn:rkaf:us:regsgov:EPA-HQ-OAR-2021-0317` |
| Public Law `117-58` | `rkaf:us-pl` | `urn:rkaf:us:pl:117-58` |

For local carrier provenance, `actor_id` values expand beneath
`urn:spicy-regs:actor:` after percent-encoding the stored value. `run_id`
values similarly expand beneath `urn:spicy-regs:run:`. Those fields are not
part of the L0 claim until the carrier publishes the distinct Assertion,
ConfidenceRecord, and Finding node construction described below. An absent
compact value means the mapped relationship is absent; consumers must not mint
an identifier for a null. In particular, a null `proceedings.current_stage`
leaves `rkaf:proceedingStage` absent and does not mean `rkaf:prerule`.

The Federal Register distinction is deliberate. Rulespec's `rkaf:us-frdoc`
lexical space accepts only `YYYY-NNNNN`, while 348,835 rows in the 2026-07-23
public corpus use official legacy or correction forms. Its normative fallback
requires the permanent federalregister.gov document URL as immutable Artifact
identity and forbids labeling a nonmatching value `rkaf:us-frdoc`. The inverse
L0 `publishedInProceeding` mapping therefore constructs that permanent URL for
every form. The local helper `federal_register_identifier()` retains its
conditional citation behavior outside the claimed mapping.

Each `comment_periods` row has at least one Proceeding or Docket anchor.
Repeatable anchor arrays preserve joint windows, and a Docket-only row retains
a known interval while Proceeding identity remains unresolved. Separate
`opened_by_artifact_ids_json` and `evidence_ids_json` fields distinguish the
Artifact that opened a period from later extension evidence. Dates are
inclusive calendar days in the source deadline's governing timezone.

The document/subject distinction is intentionally broader than regulation.
An immutable `rkaf:Artifact` may use `foaf:primaryTopic` to name its one
durable main subject, regardless of document genre. When a relation needs its
own role or provenance, the carrier uses the DCAT qualified-relation pattern:
`dcat:qualifiedRelation` → `dcat:Relationship` →
`dcterms:relation`/`dcat:hadRole`. The US rulemaking profile specializes those
general seams as `RegulatoryAgendaObservation`,
`RegulatoryAgendaItem`, and `AgendaProceedingRelationship`; it does not turn
every document subject into a regulatory agenda item.

## Carrier mapping

The fenced block below is normative and machine-audited by Rulespec's
`tools/l0_mapping_audit.py`.

```yaml rkaf-l0-mapping
rulespec_version: "sha256:2aefd3fad7782a7b16a7fa8fc08e8ceb26b5db741e0371b8fa8a9ccc1982124d"
mappings:
  - table: rule_targets
    column: docket_id
    subject_type: https://rulespec.org/ns/v1#Docket
    term: https://rulespec.org/ns/v1#hasDocketIdentifier
    direction: forward
    value_kind: iri
    transform:
      template: "urn:rkaf:us:regsgov:{docket_id}"
      identifier_scheme: https://rulespec.org/ns/v1#us-regsgov
    samples:
      - input:
          docket_id: EPA_FRDOC_0001
        output: urn:rkaf:us:regsgov:EPA_FRDOC_0001
  - table: rule_targets
    column: cfr_ref
    subject_type: https://rulespec.org/ns/v1#Artifact
    term: https://rulespec.org/ns/v1#hasRegulatoryIdentifier
    direction: forward
    value_kind: iri
    transform:
      pattern: '^([1-9][0-9]*)-([0-9]+(?:\.[0-9]+[a-z]{0,3}(?:-[0-9a-z]+)*)?)$'
      replacement: 'urn:rkaf:us:cfr:\1:\2'
      identifier_scheme: https://rulespec.org/ns/v1#us-cfr
    samples:
      - input:
          cfr_ref: 40-60.5375a
        output: urn:rkaf:us:cfr:40:60.5375a
  - table: authority_edges
    columns: [rin, agenda_edition]
    subject_type: https://rulespec.org/ns/v1#RegulatoryAgendaObservation
    term: https://rulespec.org/ns/v1#hasArtifactIdentifier
    direction: forward
    value_kind: iri
    transform:
      template: "https://www.reginfo.gov/public/do/eAgendaViewRule?RIN={rin}&pubId={agenda_edition}"
      identifier_scheme: https://rulespec.org/ns/v1#urn-persistent
    samples:
      - input:
          rin: 2060-AV16
          agenda_edition: "202510"
        output: https://www.reginfo.gov/public/do/eAgendaViewRule?RIN=2060-AV16&pubId=202510
  - table: authority_edges
    columns: [usc_title, usc_section]
    subject_type: https://rulespec.org/ns/v1#RegulatoryAgendaObservation
    term: https://rulespec.org/ns/v1#agendaAuthorityCitation
    direction: forward
    value_kind: iri
    transform:
      template: "urn:rkaf:us:usc:{usc_title}:{usc_section}"
    samples:
      - input:
          usc_title: "42"
          usc_section: "7411"
        output: urn:rkaf:us:usc:42:7411
  - table: authority_edges
    column: pl_number
    subject_type: https://rulespec.org/ns/v1#RegulatoryAgendaObservation
    term: https://rulespec.org/ns/v1#agendaAuthorityCitation
    direction: forward
    value_kind: iri
    transform:
      pattern: '^([1-9][0-9]*)-([1-9][0-9]*)$'
      replacement: 'urn:rkaf:us:pl:\1-\2'
    samples:
      - input:
          pl_number: 117-58
        output: urn:rkaf:us:pl:117-58
  - table: documents
    column: document_id
    subject_type: https://rulespec.org/ns/v1#Artifact
    term: https://rulespec.org/ns/v1#hasArtifactIdentifier
    direction: forward
    value_kind: iri
    transform:
      template: "https://www.regulations.gov/document/{document_id}"
      identifier_scheme: https://rulespec.org/ns/v1#urn-persistent
    samples:
      - input:
          document_id: EPA-HQ-OAR-2021-0317-0001
        output: https://www.regulations.gov/document/EPA-HQ-OAR-2021-0317-0001
  - table: documents
    column: document_id
    subject_type: https://rulespec.org/ns/v1#Artifact
    term: https://rulespec.org/ns/v1#hasRegulatoryIdentifier
    direction: forward
    value_kind: iri
    transform:
      template: "urn:rkaf:us:regsgov:{document_id}"
      identifier_scheme: https://rulespec.org/ns/v1#us-regsgov
    samples:
      - input:
          document_id: EPA-HQ-OAR-2021-0317-0001
        output: urn:rkaf:us:regsgov:EPA-HQ-OAR-2021-0317-0001
  - table: documents
    column: fr_doc_num
    subject_type: https://rulespec.org/ns/v1#Artifact
    term: http://purl.org/dc/terms/isFormatOf
    direction: forward
    object_type: https://rulespec.org/ns/v1#Artifact
    value_kind: iri
    source_membership:
      table: federal_register
      column: document_number
    transform:
      template: "https://www.federalregister.gov/d/{fr_doc_num}"
    samples:
      - input:
          fr_doc_num: 2021-24202
        output: https://www.federalregister.gov/d/2021-24202
  - table: documents
    column: fr_doc_num
    subject_type: https://rulespec.org/ns/v1#Artifact
    term: http://purl.org/dc/terms/hasFormat
    direction: inverse
    object_type: https://rulespec.org/ns/v1#Artifact
    value_kind: iri
    source_membership:
      table: federal_register
      column: document_number
    transform:
      template: "https://www.federalregister.gov/d/{fr_doc_num}"
    samples:
      - input:
          fr_doc_num: 2021-24202
        output: https://www.federalregister.gov/d/2021-24202
  - table: regulatory_agenda_items
    column: agenda_item_id
    subject_type: https://rulespec.org/ns/v1#RegulatoryAgendaItem
    term: https://rulespec.org/ns/v1#hasAgendaItemIdentifier
    direction: forward
    value_kind: iri
    transform:
      template: "{agenda_item_id}"
      identifier_scheme: https://rulespec.org/ns/v1#us-rin
    samples:
      - input:
          agenda_item_id: urn:rkaf:us:rin:2060-AV16
        output: urn:rkaf:us:rin:2060-AV16
  - table: regulatory_agenda_items
    column: scope_status
    subject_type: https://rulespec.org/ns/v1#RegulatoryAgendaItem
    term: https://rulespec.org/ns/v1#agendaScopeStatus
    direction: forward
    value_kind: vocab
    enum_map:
      recurring: https://rulespec.org/ns/v1#agendaScopeRecurring
      single_observed: https://rulespec.org/ns/v1#agendaScopeSingleObserved
      unresolved: https://rulespec.org/ns/v1#agendaScopeUnresolved
  - table: unified_agenda
    column: url
    subject_type: https://rulespec.org/ns/v1#RegulatoryAgendaObservation
    term: https://rulespec.org/ns/v1#hasArtifactIdentifier
    direction: forward
    value_kind: iri
    transform:
      template: "{url}"
      identifier_scheme: https://rulespec.org/ns/v1#urn-persistent
    samples:
      - input:
          url: https://www.reginfo.gov/public/do/eAgendaViewRule?RIN=2060-AV16&pubId=202510
        output: https://www.reginfo.gov/public/do/eAgendaViewRule?RIN=2060-AV16&pubId=202510
  - table: unified_agenda
    column: rin
    subject_type: https://rulespec.org/ns/v1#RegulatoryAgendaObservation
    term: http://xmlns.com/foaf/0.1/primaryTopic
    direction: forward
    value_kind: iri
    transform:
      template: "urn:rkaf:us:rin:{rin}"
    samples:
      - input:
          rin: 2060-AV16
        output: urn:rkaf:us:rin:2060-AV16
  - table: unified_agenda
    column: rule_stage
    subject_type: https://rulespec.org/ns/v1#RegulatoryAgendaObservation
    term: https://rulespec.org/ns/v1#agendaStage
    direction: forward
    value_kind: vocab
    enum_map:
      "Prerule Stage": https://rulespec.org/ns/v1#agendaPrerule
      "Proposed Rule Stage": https://rulespec.org/ns/v1#agendaProposed
      "Final Rule Stage": https://rulespec.org/ns/v1#agendaFinal
      "Long-Term Actions": https://rulespec.org/ns/v1#agendaLongterm
      "Completed Actions": https://rulespec.org/ns/v1#agendaCompleted
  - table: unified_agenda
    column: priority_category
    subject_type: https://rulespec.org/ns/v1#RegulatoryAgendaObservation
    term: https://rulespec.org/ns/v1#agendaPriority
    direction: forward
    value_kind: vocab
    enum_map:
      "Economically Significant": https://rulespec.org/ns/v1#agendaPriorityEconomicallySignificant
      "Other Significant": https://rulespec.org/ns/v1#agendaPriorityOtherSignificant
      "Substantive, Nonsignificant": https://rulespec.org/ns/v1#agendaPrioritySubstantiveNonsignificant
      "Routine and Frequent": https://rulespec.org/ns/v1#agendaPriorityRoutineFrequent
      "Info./Admin./Other": https://rulespec.org/ns/v1#agendaPriorityInfoAdminOther
  - table: proceedings
    column: proceeding_id
    subject_type: https://rulespec.org/ns/v1#Proceeding
    term: https://rulespec.org/ns/v1#hasProceedingIdentifier
    direction: forward
    value_kind: iri
    transform:
      template: "urn:spicy-regs:proceeding:{proceeding_id}"
      identifier_scheme: https://rulespec.org/ns/v1#partner-defined
    samples:
      - input:
          proceeding_id: pr-2060-AV16-0c9a
        output: urn:spicy-regs:proceeding:pr-2060-AV16-0c9a
  - table: proceedings
    column: docket_ids_json
    subject_type: https://rulespec.org/ns/v1#Proceeding
    term: https://rulespec.org/ns/v1#hasDocket
    direction: forward
    object_type: https://rulespec.org/ns/v1#Docket
    value_kind: iri
    collection: json-list
    transform:
      template: "urn:rkaf:us:regsgov:{value}"
    samples:
      - input:
          docket_ids_json: '["EPA-HQ-OAR-2021-0317", "EPA-HQ-OAR-2025-0192"]'
        output:
          - urn:rkaf:us:regsgov:EPA-HQ-OAR-2021-0317
          - urn:rkaf:us:regsgov:EPA-HQ-OAR-2025-0192
  - table: proceedings
    column: current_stage
    subject_type: https://rulespec.org/ns/v1#Proceeding
    term: https://rulespec.org/ns/v1#proceedingStage
    direction: forward
    value_kind: vocab
    enum_map:
      prerule: https://rulespec.org/ns/v1#proceedingPrerule
      proposed: https://rulespec.org/ns/v1#proceedingProposed
      supplemental: https://rulespec.org/ns/v1#proceedingSupplemental
      final: https://rulespec.org/ns/v1#proceedingFinal
      withdrawn: https://rulespec.org/ns/v1#proceedingWithdrawn
      longterm: https://rulespec.org/ns/v1#proceedingLongterm
  - table: proceedings
    column: fr_document_numbers_json
    subject_type: https://rulespec.org/ns/v1#Proceeding
    term: https://rulespec.org/ns/v1#publishedInProceeding
    direction: inverse
    object_type: https://rulespec.org/ns/v1#Artifact
    value_kind: iri
    collection: json-list
    transform:
      template: "https://www.federalregister.gov/d/{value}"
    samples:
      - input:
          fr_document_numbers_json: '["2024-00366", "E7-21559"]'
        output:
          - https://www.federalregister.gov/d/2024-00366
          - https://www.federalregister.gov/d/E7-21559
  - table: proceedings
    column: cfr_target_iris_json
    subject_type: https://rulespec.org/ns/v1#Proceeding
    term: https://rulespec.org/ns/v1#proceedingAffectsCitation
    direction: forward
    value_kind: iri
    collection: json-list
    transform:
      template: "{value}"
    samples:
      - input:
          cfr_target_iris_json: '["urn:rkaf:us:cfr:40:60.5375a"]'
        output:
          - urn:rkaf:us:cfr:40:60.5375a
  - table: proceedings
    column: identity_predecessors_json
    subject_type: https://rulespec.org/ns/v1#Proceeding
    term: https://rulespec.org/ns/v1#proceedingSupersedes
    direction: forward
    object_type: https://rulespec.org/ns/v1#Proceeding
    value_kind: iri
    collection: json-list
    transform:
      template: "urn:spicy-regs:proceeding:{value}"
    samples:
      - input:
          identity_predecessors_json: '["pr-2060-AV16-legacy"]'
        output:
          - urn:spicy-regs:proceeding:pr-2060-AV16-legacy
  - table: agenda_item_proceedings
    column: agenda_item_id
    subject_type: https://rulespec.org/ns/v1#AgendaProceedingRelationship
    term: http://www.w3.org/ns/dcat#qualifiedRelation
    direction: inverse
    object_type: https://rulespec.org/ns/v1#RegulatoryAgendaItem
    value_kind: iri
    transform:
      template: "{agenda_item_id}"
    samples:
      - input:
          agenda_item_id: urn:rkaf:us:rin:2060-AV16
        output: urn:rkaf:us:rin:2060-AV16
  - table: agenda_item_proceedings
    column: proceeding_id
    subject_type: https://rulespec.org/ns/v1#AgendaProceedingRelationship
    term: http://purl.org/dc/terms/relation
    direction: forward
    object_type: https://rulespec.org/ns/v1#Proceeding
    value_kind: iri
    transform:
      template: "urn:spicy-regs:proceeding:{proceeding_id}"
    samples:
      - input:
          proceeding_id: pr-2060-AV16-0c9a
        output: urn:spicy-regs:proceeding:pr-2060-AV16-0c9a
  - table: agenda_item_proceedings
    column: relationship_role
    subject_type: https://rulespec.org/ns/v1#AgendaProceedingRelationship
    term: http://www.w3.org/ns/dcat#hadRole
    direction: forward
    value_kind: vocab
    transform:
      template: "https://rulespec.org/ns/v1#agendaTracksProceeding"
    samples:
      - input:
          relationship_role: agenda_tracks_proceeding
        output: https://rulespec.org/ns/v1#agendaTracksProceeding
  - table: agenda_item_proceedings
    column: evidence_uri
    subject_type: https://rulespec.org/ns/v1#AgendaProceedingRelationship
    term: http://www.w3.org/ns/prov#wasDerivedFrom
    direction: forward
    object_type: http://www.w3.org/ns/prov#Entity
    value_kind: iri
    transform:
      template: "{evidence_uri}"
    samples:
      - input:
          evidence_uri: https://www.federalregister.gov/d/2024-00366
        output: https://www.federalregister.gov/d/2024-00366
  - table: agenda_item_proceedings
    column: run_id
    subject_type: https://rulespec.org/ns/v1#AgendaProceedingRelationship
    term: http://www.w3.org/ns/prov#wasGeneratedBy
    direction: forward
    value_kind: iri
    transform:
      template: "urn:spicy-regs:run:{run_id}"
    samples:
      - input:
          run_id: ontology-20260724
        output: urn:spicy-regs:run:ontology-20260724
  - table: agenda_item_proceedings
    column: actor_id
    subject_type: https://rulespec.org/ns/v1#AgendaProceedingRelationship
    term: http://www.w3.org/ns/prov#wasAttributedTo
    direction: forward
    value_kind: iri
    transform:
      template: "urn:spicy-regs:actor:{actor_id}"
    samples:
      - input:
          actor_id: agenda-item-proceedings-v1
        output: urn:spicy-regs:actor:agenda-item-proceedings-v1
  - table: agenda_item_proceedings
    column: asserted_at
    subject_type: https://rulespec.org/ns/v1#AgendaProceedingRelationship
    term: http://www.w3.org/ns/prov#generatedAtTime
    direction: forward
    value_kind: literal
  - table: comment_periods
    column: proceeding_ids_json
    subject_type: https://rulespec.org/ns/v1#CommentPeriod
    term: https://rulespec.org/ns/v1#commentPeriodFor
    direction: forward
    object_type: https://rulespec.org/ns/v1#Proceeding
    value_kind: iri
    collection: json-list
    transform:
      template: "urn:spicy-regs:proceeding:{value}"
    samples:
      - input:
          proceeding_ids_json: '["pr-2060-AV16-0c9a"]'
        output:
          - urn:spicy-regs:proceeding:pr-2060-AV16-0c9a
  - table: comment_periods
    column: docket_ids_json
    subject_type: https://rulespec.org/ns/v1#CommentPeriod
    term: https://rulespec.org/ns/v1#commentPeriodDocket
    direction: forward
    object_type: https://rulespec.org/ns/v1#Docket
    value_kind: iri
    collection: json-list
    transform:
      template: "urn:rkaf:us:regsgov:{value}"
    samples:
      - input:
          docket_ids_json: '["EPA-HQ-OAR-2021-0317"]'
        output:
          - urn:rkaf:us:regsgov:EPA-HQ-OAR-2021-0317
  - table: comment_periods
    column: opened_by_artifact_ids_json
    subject_type: https://rulespec.org/ns/v1#CommentPeriod
    term: https://rulespec.org/ns/v1#commentPeriodOpenedBy
    direction: forward
    object_type: https://rulespec.org/ns/v1#Artifact
    value_kind: iri
    collection: json-list
    transform:
      template: "{value}"
    samples:
      - input:
          opened_by_artifact_ids_json: '["https://www.federalregister.gov/d/2021-24202"]'
        output:
          - https://www.federalregister.gov/d/2021-24202
  - table: comment_periods
    column: open_date
    subject_type: https://rulespec.org/ns/v1#CommentPeriod
    term: https://rulespec.org/ns/v1#commentPeriodStart
    direction: forward
    value_kind: date
  - table: comment_periods
    column: close_date
    subject_type: https://rulespec.org/ns/v1#CommentPeriod
    term: https://rulespec.org/ns/v1#commentPeriodEnd
    direction: forward
    value_kind: date
  - table: comment_periods
    column: evidence_ids_json
    subject_type: https://rulespec.org/ns/v1#CommentPeriod
    term: http://www.w3.org/ns/prov#wasDerivedFrom
    direction: forward
    object_type: http://www.w3.org/ns/prov#Entity
    value_kind: iri
    collection: json-list
    transform:
      template: "urn:spicy-regs:evidence:comment-period:{value}"
    samples:
      - input:
          evidence_ids_json: '["EPA-HQ-OAR-2021-0317-0184", "2021-24202"]'
        output:
          - urn:spicy-regs:evidence:comment-period:EPA-HQ-OAR-2021-0317-0184
          - urn:spicy-regs:evidence:comment-period:2021-24202
```

The map deliberately omits carrier areas that cannot yet produce the
declared RDF semantics without inventing information:

- `cfr_target_iris_json` supports citation-level targets. The compact CFR and
  authority references still cannot identify immutable pre-amendment or
  resulting editions for `rkaf:proceedingAffects` or
  `rkaf:proceedingProduces`; an edition resolver is required.
- `stage_events_json` contains nested event records, not a list of enum values;
  a row-level column mapping would lose the event subject and evidence. The
  carrier derives `current_stage` only when the latest dated stage-family
  events agree.
- the relationship table's `run_id`, `actor_id`, and `asserted_at` map directly
  to PROV properties because each row is already a qualified relationship
  node. Uniform attestation columns on other tables remain local carrier
  provenance until an explicit node-construction map preserves their domains
  and links.
- `concepts` and `concept_assignments` are retrieval-grade tags, not
  `rkaf:LocalConcept` nodes. Their labels, hierarchy, confidence, and evidence
  remain local until a separate human-reviewed promotion creates a Rulespec
  concept IRI and an explicit `skos:exactMatch` link.

The inverse direction on `fr_document_numbers_json` is intentional: each
Federal Register `rkaf:Artifact` points through
`rkaf:publishedInProceeding` to the carrier's `rkaf:Proceeding`.
The paired `documents.fr_doc_num` mappings likewise express the canonical
Federal Register → regulations.gov `dcterms:hasFormat` direction and its
inverse, but only when the raw value has exact membership in
`federal_register.document_number`. Malformed or stale raw source strings remain
available in `documents` and are counted as excluded projection values; they do
not mint Artifact IRIs. `agency_code` is deliberately not mapped to
`rkaf:hasAuthority`; agency identity alone is not legal authority.

## Local descriptive-tag terms

`concepts.scheme`, `status`, and `replaced_by`, along with event payloads, are
retrieval-grade Spicy Regs carrier mechanics. They intentionally do not claim
decision-grade Rulespec concept-registry semantics. Promotion to a
`rkaf:LocalConcept` or `rkaf:RegisteredConcept` remains a separate,
human-reviewed, attested event.

The registry applies these invariants before publishing:

- prior concepts and assignments cannot disappear;
- assignment revisions append a row linked by `supersedes_id`;
- `broader_id` and `replaced_by` graphs are acyclic;
- every replacement resolves to an existing concept;
- every LLM, embedding, or human row has complete provenance.

## Operating the tagging loop

One materialized-dataset DAG runs the registry stages in dependency order:
`concepts` refreshes Federal Register Thesaurus seeds and performs
merge/re-score convergence; `concept_assignments` tags new or changed subjects
and validates a stable sample; `concept_events` performs an idempotent audit-log
reconciliation. The nine identity and ontology tables are uploaded under one
immutable snapshot prefix, then one `materialized/ontology/latest.json` pointer
is replaced. Candidate concepts, events, and assignments therefore become
visible as one generation rather than through independently scheduled jobs.

An absent `OPENAI_API_KEY` makes model generation and validation a no-op; all
deterministic seeds and convergence checks still run. The optional model
settings are:

| Variable | Default | Effect |
| --- | --- | --- |
| `SPICY_REGS_ONTOLOGY_MODEL` | `gpt-5.6-sol` | Structured-output tagging and validation model. |
| `SPICY_REGS_ONTOLOGY_REASONING_EFFORT` | `medium` | Responses API reasoning effort. |
| `OPENAI_ONTOLOGY_SERVICE_TIER` | `auto` | Requested Responses API service tier; the request and actual response tiers are retained in provider evidence. |
| `OPENAI_ONTOLOGY_TIMEOUT_SECONDS` | `120` | Per-attempt client timeout. |
| `OPENAI_ONTOLOGY_MAX_RETRIES` | `3` | Application-owned retries after the first attempt; SDK retries remain disabled so every physical call is visible. |
| `OPENAI_ONTOLOGY_RETRY_BASE_SECONDS` | `1` | Base delay for exponential retry backoff. |
| `ONTOLOGY_GENERATION_LIMIT` | `500` | Maximum new/changed subjects tagged in one assignment run. |
| `ONTOLOGY_VALIDATION_PERCENT` | `10` | Stable hash-selected percentage of current LLM assertions re-checked. |
| `ONTOLOGY_DISCOVERY_LIMIT` | `0` | Optional extra candidate-only discovery in the concepts pass; disabled normally to avoid duplicate model calls. |
| `ONTOLOGY_RUN_ID` | generated | Stable run/checkpoint id to reuse when resuming the same local batch. |

Tagging and validation use strict structured output with respective 8,192- and
4,096-token output ceilings. Comparison artifacts bind these ceilings and the
secret-free provider configuration into run identity; provider metadata
records each physical attempt, retry, requested service tier, and actual
response tier. Evidence offsets are verified deterministically. A wrong
provider offset may be repaired only when the quoted evidence has exactly one
verbatim occurrence in the named field; ambiguous, normalized, fuzzy, or
non-verbatim matches are rejected. The evidence span records its alignment
method, and tagging receipts reconcile returned, accepted, rejected, and
repaired item counts.

`concept_merge_review.jsonl` is the current human-review queue for high-usage,
below-auto-threshold merge candidates. Tag drift is measurable without an API
call:

```console
uv run spicy-regs-evaluate-tags output --minimum-f1 0.50
```

Proceeding identity is independent of RIN. `proceedings` forms components from
source-backed dockets and merges them only when a single Federal Register
artifact explicitly co-identifies those dockets. A docket-less rulemaking
artifact receives a provisional artifact-based Proceeding. RIN values remain
denormalized evidence only when an action has exactly one; zero or several
leave `proceedings.rin` null.

`regulatory_agenda_items` is the separate RIN identity layer.
`agenda_item_proceedings` relates an item to zero or more independently
identified actions only when a docket, regulations.gov document, or Federal
Register artifact directly reports that RIN. Unified Agenda stage, priority,
CFR, and authority remain on the editioned observation. They do not flow to
child Proceedings through string equality.

The current component shape does not define the public id. Each materialization
loads the prior `proceedings` artifact from the same atomic generation and
reuses the strongest compatible predecessor id by docket or Federal Register
artifact overlap. This keeps an id stable when a backfill adds a lexically
earlier docket or resolves a provisional artifact to its docket. Every distinct
compatible prior identity is retained in `identity_predecessors_json`, so
merges and splits have explicit semantic continuity without emitting a
Proceeding-to-itself `proceedingSupersedes` edge. The reused identity is tracked
only by the local row-version `supersedes_id`. `current_stage` is likewise null
when no action-specific stage event is evidenced.
