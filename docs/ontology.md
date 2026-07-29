# Metadata ontology and Rulespec Level-0 mapping

The existing Spicy Regs published-table carrier is a Rulespec **L0
Vocabulary** consumer. Its carrier is flat Apache Parquet, not JSON-LD:
compact identifiers and enum values expand deterministically to Rulespec
terms, but those tables do not claim Rulespec L1–L4 parsing, shape,
constraint, or runtime conformance.

The separate document-to-RKAF projection validates JSON-LD through the current
sibling Rulespec L1–L4 implementation. Its controlled-vocabulary input uses
the normalized label, relation, and lifecycle-participant tables described
below. It does not treat the flat `concepts` table or fused registry as
production authority. The sibling RefSpec application profile pins the exact
tested local Rulespec revision and digest. This command does not hardcode that
pair; each run requires the caller to supply the version and digest and may
also record the exact revision.

The implementation targets the repaired candidate US identifier and
Experimental rulemaking contract whose content digest is
`sha256:6e5506001343c55af2530c89070c79c4f74f54f666ef823c2687bd1460d173ce`
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
rulespec_version: "sha256:6e5506001343c55af2530c89070c79c4f74f54f666ef823c2687bd1460d173ce"
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
- `authority_edges.usc_section_end` closes a cited section *range*
  (`42 U.S.C. 7401-7671q`). The mapped pair `usc_title`/`usc_section` projects
  such a row to its first section — a section the source text names, so the
  IRI is honest — and the interval is not projected: Rulespec has no range
  term, and expanding one would mint `us-usc` IRIs for sections that may not
  exist. Both endpoints stay readable in the carrier; a consumer that needs
  containment applies the interval predicate documented on the table.

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

## Relationship assertions and comparison findings

The relationship-comparison work is Experimental and is not part of the
published L0 carrier. Its generic model applies across document types. The
planned document identity seam composes existing vocabularies:

| Object | Meaning |
| --- | --- |
| Stable work resource | Public or profile-owned identity across versions; Rulespec does not mint a universal work class. |
| `Artifact` | One immutable edition, publication, snapshot, content payload, or source posting. |
| `dcterms:isVersionOf` | Substantive version membership from an Artifact to its stable resource. |
| `prov:wasRevisionOf` | Exact lineage from a later Artifact to an earlier Artifact. |
| `dcterms:isFormatOf` / `dcterms:hasFormat` | Substantially identical content in another format or source posting. |

Legal and regulatory profiles may use ELI's LegalResource and LegalExpression
model. Other profiles may use BIBFRAME, Schema.org, or a domain vocabulary.
The generic Rulespec core constrains only its Artifact endpoints and does not
redefine those public models or infer legal effect.

Relationship analysis uses these records:

| Object | Meaning |
| --- | --- |
| `RelationshipAssertion` / `RelationAssertion` | An immutable subject-predicate-object proposition with affirmed or denied polarity and construction origin. |
| `RelationEvidenceBinding` | An exact source span bound to one assertion occurrence and artifact version. |
| `AssertionAttestation` | A consumer's scoped, temporal approval, rejection, abstention, or review state for an assertion. |
| `RelationComparisonContext` | The artifact pair, consumer scope, evaluation time, detector version, and snapshot used for one comparison. |
| `ResolverProofRecord` | A content-addressed decision record that binds one resolver outcome to its inputs, policy, version, evidence, and rationale. |
| `RelationFinding` | A neutral, evidence-backed analytic result from the deterministic comparator. |
| `RelationChangeEvent` | A proposed or effected adoption, removal, suspension, or supersession of a relation; currently evaluation-local. |
| `ClosureClaim` | A bounded, revocable claim that a named observation process covered a declared subject, predicate, expected set, source set, and time range. |

Assertion polarity answers only whether a source affirms or denies the
canonical relation. It does not encode attribution, conditionality, lifecycle
state, consumer acceptance, deontic force, or legal effect. Those concerns
remain separate records or profile-owned interpretations.

The extraction profile likewise keeps relation assertions separate from
relation change events. A proposal to adopt, remove, suspend, or supersede a
relation is an event with its own stage and intended-effect time; it is not a
denied assertion. Applicability time, attribution, and conditionality remain
orthogonal. `source_voice` identifies the document or issuing source speaking
for itself. `attributed_source` identifies a distinct reported person,
organization, instrument, opinion, amendment, or other claimant. Stored
claimant wording remains verbatim, while comparison may normalize superficial
determiners such as `the FCC` and `FCC`.

One source span may support multiple claims. A rhetorical sentence can carry
both the source speaker's proposition and a separately attributed embedded
proposition with the opposite polarity. Each claim receives its own identity,
claimant, polarity, time, and condition while retaining the shared exact
evidence span.

Evidence grounding and evidence scoring are separate. Submitted spans must
remain exact substrings with exact offsets. The evaluator may recognize a
terminal-punctuation-only boundary difference as equivalent, but reports it as
boundary-equivalent rather than exact. Core relation/event semantics,
orthogonal dimensions, evidence sufficiency, and boundary preference receive
separate scores.

The current comparator can emit an `affirmed_denied_discrepancy` when all
predicate, state, evidence, baseline, pairing, and scope gates pass. A failed
or unknown gate never becomes a negative fact. The model proposes candidates
and exact evidence; deterministic resolvers and attestations decide whether a
candidate may enter comparison.

Silence remains unknown. A future longitudinal comparator may emit the neutral
`expected_relation_not_observed` finding only after it proves artifact
lineage, expected coverage, comparable scope, and an evidence-bound closure
claim for the later observation. It must not turn omission into a denied
assertion.

See the
[relationship assertion design](superpowers/specs/2026-07-24-relation-exclusion-findings-design.md),
[resolver contract](superpowers/specs/2026-07-25-relation-comparison-resolver-contract.md),
[longitudinal omission design](superpowers/specs/2026-07-25-longitudinal-relation-omission-design.md),
[domain-profile boundary](superpowers/specs/2026-07-25-deontic-relation-profile-boundary.md),
and [recent research synthesis](evidence/recent-document-relation-lookup-research-2026-07-25.md).

## Anchor semantics: what an offset addresses and what a digest covers

Cross-project evidence references only translate mechanically when both sides
agree on the unit, the interval, and the exact bytes each digest covers. The v3
`source` step (`src/spicy_regs/docpipeline/source.py`) states all three, and
these are the semantics any consumer of its `Artifact`, `SourceFragment`, or
evidence records should assume.

**Offsets are Python unicode codepoints over half-open `[start, end)`
intervals.** Not bytes, not UTF-16 code units, and never an inclusive end. A
section sign, an em dash, a curly quote, and an astral emoji are each exactly
one codepoint, so `region.text == field_text[start_char:end_char]` holds for
every record — and the step refuses to emit one where it does not. Consecutive
sibling regions abut: one region's `end_char` is the next one's `start_char`,
and they share no codepoint. Every record carries its own
`coordinate_target` / `coordinate_unit` / `coordinate_interval`, so nothing has
to be inferred. Two targets exist and stay distinct:
`artifact-source-field` means the offsets index one exact field of one
Artifact, and `adapter-parsed-text` means they index text a parser built, which
is graded `parser-derived` and never `source-exact`.

**Each digest names its own scope.** `Artifact.content_sha256` covers the
canonical JSON of the profile id, source table, subject type, subject id, and
every declared source value in declaration order — nulls included, and each
attached byte rendition contributed as its own content digest. It is the exact
immutable source state and nothing else; it is not a digest of the concatenated
text. `field_sha256` covers the UTF-8 bytes of one whole source field's exact
text, unnormalized and untrimmed. `text_sha256` covers the UTF-8 bytes of one
region's or fragment's own slice — that is, `field_text[start_char:end_char]` —
so it agrees with `field_sha256` only when the region spans the whole field.
Region and fragment ids are derived, not covering digests: they hash the
adapter version, subject identity, artifact digest, source field, span, kind,
and ordinal for native source fields, preserving the frozen native identity
recipe. Parser-derived region ids bind those facts plus `parser_id` and the
parsed field digest. The fragment id inherits the region id. The same Office
bytes parsed under two mapping revisions therefore cannot produce colliding
region or fragment ids.

Parser metadata remains explicit on every derived region, fragment, and segment
slice. `content_layer=body` may supply durable evidence. `furniture` and
`notes` remain durable `context_only` fragments but do not enter processing or
evidence slices. `background` and `invisible` stay held, appear in exclusion
accounting, and never become fragments or segments. Native fields use
`content_layer=body` and `coordinate_grade=source-exact`; parser-derived fields
retain the adapter's closed `content_layer` and `coordinate_grade` values.

`ProcessingSegment.text` is migration-compatible processing text, so it still
contains context-only heading slices where the frozen segment boundary requires
them. `ProcessingSegment.evidence_slices` is the citable subset and excludes
headings, syntax, and non-body context. `content_digest` identifies the slice
content and segmentation settings without the Artifact version or adjacent
context. It is not a complete provider reuse identity. A later `WorkIdentity`
must also bind the prompt, schema, provider, model, revision, provider settings,
context digest, approval and evidence policies, and the earlier run before a
provider result may be reused.

These records are **local run outputs, not published tables.** `source/artifacts.parquet`,
`source/fragments.parquet`, `source/coverage.parquet`, and the immutable
`source/parser-attempts.parquet` live inside one run directory. The parser table
retains success, unavailability, declared failure, malformed output, timeout,
exit, signal, result oversize, input oversize, and preflight quarantine with
source and attachment digests, parser policy, the full flattened process-gate
receipt, and sanitized call JSON. Nothing in the R2 surface or the data
dictionary above publishes these tables yet. The anchor semantics are
documented now so the published projection, when it lands, inherits them rather
than inventing its own.

## Legacy descriptive-tag tables

`concepts.facet`, `source_vocabulary`, `status`, and `replaced_by`, along with
event payloads, are legacy development and migration mechanics. They cannot
authorize conforming REF or Rulespec output. `facet` controls the old tag
policy; `source_vocabulary` records the source key used during migration. The
deprecated `scheme` column mirrors `facet` on v2 rows.
`fused-concept-registry-v1` remains read-only migration input: its compatibility
reader interprets an external-valued `scheme` as `source_vocabulary` and
infers `facet`.

The production RKAF projection instead requires:

- `concept_labels`, one row per Unicode, language-preserving label expression;
- `concept_relations`, one row per exact in-scheme hierarchy relation;
- `concept_event_participants`, one row per predecessor or successor; and
- an authoritative JSON-LD manifest carrying the exact concept, scheme,
  release, distribution, and complete-membership records.

The projection rejects `registry.parquet`, retired inline concept status, a
hierarchy target outside the exact scheme and release, and any assignment
without complete release membership. A legacy proposal becomes a
`rkaf:LocalConcept` or `rkaf:RegisteredConcept` only through a separate,
reviewed and attested governance action.

The current document projection is a diagnostic review path, not an accepted
REF enrichment-output path. Every model assignment carries
`rkaf:reviewQueueOnly`. Its run record states that RefSpec candidate and
accepted-output authorization were not evaluated because this command does not
accept an `OutputProfile`, coverage report, evaluated configuration, or
deployment decision. The command also requires the exact Rulespec semantic
version and constraint digest. A missing source revision is recorded as a
local candidate and cannot support an immutable conformance claim.

The migration view applies these invariants before materializing a
development snapshot:

- prior concepts and assignments cannot disappear;
- assignment revisions append a row linked by `supersedes_id`;
- `broader_id` and `replaced_by` graphs are acyclic;
- every replacement resolves to an existing concept;
- every LLM, embedding, or human row has complete provenance.

## Operating the legacy tagging loop

One materialized-dataset DAG runs the registry stages in dependency order:
`concepts` refreshes Federal Register Thesaurus seeds and performs
merge/re-score convergence; `concept_assignments` tags new or changed subjects
and validates a stable sample; `concept_events` performs an idempotent audit-log
reconciliation. The nine identity and ontology tables are uploaded under one
immutable snapshot prefix, then one `materialized/ontology/latest.json` pointer
is replaced. Candidate concepts, events, and assignments therefore become
visible as one development generation rather than through independently
scheduled jobs. That snapshot is not a conforming registry, evaluation, or
publication authority.

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
