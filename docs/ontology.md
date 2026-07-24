# Metadata ontology and Rulespec Level-0 mapping

Spicy Regs is a Rulespec **L0 Vocabulary** consumer. Its carrier is flat Apache
Parquet, not JSON-LD: compact identifiers and enum values expand
deterministically to Rulespec terms, but the dataset does not claim Rulespec
L1–L4 parsing, shape, constraint, or runtime conformance.

The implementation targets the US identifier and experimental rulemaking
contract whose content digest is
`sha256:836968b28f3b86283f53c57ae5c9ab8ebd77e96531cd4751476f1a5ee3d296f2`
(2026-07-23). Spicy Regs has satisfied the full-corpus half of the stabilization
gate; the rulemaking module remains experimental until an independent review
completes the other half.

See the measured [full-corpus friction report](ontology-friction-report.md) for
the consumer-run results and the contract corrections required by the corpus.

## Identifier expansion

Parquet keeps the compact join keys users already query. Expand them as follows:

| Carrier value | Rulespec scheme | Canonical identifier |
| --- | --- | --- |
| CFR `40-60` / `40-60.1` | `rkaf:us-cfr` | `urn:rkaf:us:cfr:40:60` / `urn:rkaf:us:cfr:40:60.1` |
| U.S.C. title `42` + section `7401` | `rkaf:us-usc` | `urn:rkaf:us:usc:42:7401` |
| Proceeding id `pr-2060-AV16-0c9a` | `rkaf:partner-defined` | `urn:spicy-regs:proceeding:pr-2060-AV16-0c9a` |
| RIN `2060-AV16` | `rkaf:us-rin` | `urn:rkaf:us:rin:2060-AV16` (evidence/join key; not identity when reused) |
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

Each `comment_periods` row keeps a source qualifier beside a non-empty JSON
list of evidence identifiers. The L0 map expands those identifiers to local
`prov:Entity` IRIs and links the CommentPeriod through
`prov:wasDerivedFrom`; the row's `source` column continues to identify whether
the evidence came from regulations.gov, the Federal Register, or both.

## Carrier mapping

The fenced block below is normative and machine-audited by Rulespec's
`tools/l0_mapping_audit.py`.

```yaml rkaf-l0-mapping
rulespec_version: "sha256:836968b28f3b86283f53c57ae5c9ab8ebd77e96531cd4751476f1a5ee3d296f2"
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
          docket_id: EPA-HQ-OAR-2021-0317
        output: urn:rkaf:us:regsgov:EPA-HQ-OAR-2021-0317
  - table: rule_targets
    column: cfr_ref
    subject_type: https://rulespec.org/ns/v1#Artifact
    term: https://rulespec.org/ns/v1#hasRegulatoryIdentifier
    direction: forward
    value_kind: iri
    transform:
      pattern: '^([1-9][0-9]*)-([0-9]+(?:\.[0-9]+)?)$'
      replacement: 'urn:rkaf:us:cfr:\1:\2'
      identifier_scheme: https://rulespec.org/ns/v1#us-cfr
    samples:
      - input:
          cfr_ref: 40-60.1
        output: urn:rkaf:us:cfr:40:60.1
  - table: authority_edges
    columns: [usc_title, usc_section]
    subject_type: https://rulespec.org/ns/v1#Artifact
    term: https://rulespec.org/ns/v1#hasRegulatoryIdentifier
    direction: forward
    value_kind: iri
    transform:
      template: "urn:rkaf:us:usc:{usc_title}:{usc_section}"
      identifier_scheme: https://rulespec.org/ns/v1#us-usc
    samples:
      - input:
          usc_title: "42"
          usc_section: "7411"
        output: urn:rkaf:us:usc:42:7411
  - table: authority_edges
    column: pl_number
    subject_type: https://rulespec.org/ns/v1#Artifact
    term: https://rulespec.org/ns/v1#hasRegulatoryIdentifier
    direction: forward
    value_kind: iri
    transform:
      pattern: '^([1-9][0-9]*)-([1-9][0-9]*)$'
      replacement: 'urn:rkaf:us:pl:\1-\2'
      identifier_scheme: https://rulespec.org/ns/v1#us-pl
    samples:
      - input:
          pl_number: 117-58
        output: urn:rkaf:us:pl:117-58
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
    column: agency_code
    subject_type: https://rulespec.org/ns/v1#Proceeding
    term: https://rulespec.org/ns/v1#hasAuthority
    direction: forward
    object_type: https://rulespec.org/ns/v1#Authority
    value_kind: iri
    transform:
      template: "urn:spicy-regs:authority:agency:{agency_code}"
    samples:
      - input:
          agency_code: EPA
        output: urn:spicy-regs:authority:agency:EPA
  - table: proceedings
    column: current_stage
    subject_type: https://rulespec.org/ns/v1#Proceeding
    term: https://rulespec.org/ns/v1#proceedingStage
    direction: forward
    value_kind: vocab
    enum_map:
      prerule: https://rulespec.org/ns/v1#prerule
      proposed: https://rulespec.org/ns/v1#proposed
      supplemental: https://rulespec.org/ns/v1#supplemental
      final: https://rulespec.org/ns/v1#final
      withdrawn: https://rulespec.org/ns/v1#withdrawn
      longterm: https://rulespec.org/ns/v1#longterm
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
  - table: comment_periods
    column: proceeding_id
    subject_type: https://rulespec.org/ns/v1#CommentPeriod
    term: https://rulespec.org/ns/v1#commentPeriodFor
    direction: forward
    object_type: https://rulespec.org/ns/v1#Proceeding
    value_kind: iri
    transform:
      template: "urn:spicy-regs:proceeding:{proceeding_id}"
    samples:
      - input:
          proceeding_id: pr-2060-AV16-0c9a
        output: urn:spicy-regs:proceeding:pr-2060-AV16-0c9a
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

The map deliberately omits four carrier areas that cannot yet produce the
declared RDF semantics without inventing information:

- `cfr_refs_json` and `authority_refs_json` are unversioned citations. They can
  carry regulatory identifiers, but they cannot identify immutable
  `rkaf:Artifact` targets for `rkaf:proceedingAffects` or authorities until an
  edition resolver supplies the actual resource.
- `stage_events_json` contains nested event records, not a list of enum values;
  a row-level column mapping would lose the event subject and evidence.
- the uniform attestation columns span `rkaf:Assertion`,
  `rkaf:ConfidenceRecord`, and `rkaf:Finding`. They remain local carrier
  provenance until an explicit node-construction map preserves those domains
  and their links.
- `concepts` and `concept_assignments` are retrieval-grade tags, not
  `rkaf:LocalConcept` nodes. Their labels, hierarchy, confidence, and evidence
  remain local until a separate human-reviewed promotion creates a Rulespec
  concept IRI and an explicit `skos:exactMatch` link.

The inverse direction on `fr_document_numbers_json` is intentional: each
Federal Register `rkaf:Artifact` points through
`rkaf:publishedInProceeding` to the carrier's `rkaf:Proceeding`.

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
reconciliation. The seven identity and ontology tables are uploaded under one
immutable snapshot prefix, then one `materialized/ontology/latest.json` pointer
is replaced. Candidate concepts, events, and assignments therefore become
visible as one generation rather than through independently scheduled jobs.

An absent `OPENAI_API_KEY` makes model generation and validation a no-op; all
deterministic seeds and convergence checks still run. The optional model
settings are:

| Variable | Default | Effect |
| --- | --- | --- |
| `SPICY_REGS_ONTOLOGY_MODEL` | `gpt-5.6-luna` | Structured-output tagging and validation model. |
| `ONTOLOGY_GENERATION_LIMIT` | `500` | Maximum new/changed subjects tagged in one assignment run. |
| `ONTOLOGY_VALIDATION_PERCENT` | `10` | Stable hash-selected percentage of current LLM assertions re-checked. |
| `ONTOLOGY_DISCOVERY_LIMIT` | `0` | Optional extra candidate-only discovery in the concepts pass; disabled normally to avoid duplicate model calls. |
| `ONTOLOGY_RUN_ID` | generated | Stable run/checkpoint id to reuse when resuming the same local batch. |

`concept_merge_review.jsonl` is the current human-review queue for high-usage,
below-auto-threshold merge candidates. Tag drift is measurable without an API
call:

```console
uv run spicy-regs-evaluate-tags output --minimum-f1 0.50
```

Proceeding identity has one additional corpus-derived rule: a RIN is strong
evidence but not globally unique across time. Some agencies reuse a RIN for
recurring action families. `proceedings` therefore forms docket components
within each RIN and merges components only when a single Federal Register
document explicitly co-identifies their dockets. Evidence without a RIN is
attached through a docket only when that docket resolves to one component;
otherwise it remains in the source table instead of being copied across
proceedings.

The current component shape does not define the public id. Each materialization
loads the prior `proceedings` artifact from the same atomic generation and
reuses the strongest compatible predecessor id by docket overlap. This keeps an
id stable when a backfill adds a lexically earlier docket. Every compatible
prior id is retained in `identity_predecessors_json`, so merges and splits have
explicit continuity even though only one current component may reuse a given
id. A docket-less RIN component may also retain its id when it gains its first
docket, but only when that RIN identifies exactly one prior and one current
component. `supersedes_id` equals the reused id when the row replaces its
prior-generation assertion. `current_stage` is likewise null when no stage
event is evidenced.
