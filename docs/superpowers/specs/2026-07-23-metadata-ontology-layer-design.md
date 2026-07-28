# SpicyRegs Metadata & Ontology Layer — Design

- **Date:** 2026-07-23
- **Status:** Implemented locally; publication pending
- **Implementation:** [`docs/ontology.md`](../../ontology.md), `conformance/rulespec-l0.yaml`, the current [`RIN ontology revision report`](../../rin-ontology-revision-report.md), and the historical [`full-corpus friction report`](../../ontology-friction-report.md)
- **Parent:** `docs/superpowers/specs/2026-07-23-regulatory-ontology-program-overview.md`
- **Scope:** Spec 1 of 2. This spec covers the spicy-regs metadata layer: a rule-identity spine, statutory-authority edges, proceeding and comment-period identity, and an iterative concept-tagging system, all with explicit provenance. The sibling Rulespec spec defines the upstream vocabulary contract consumed here.

## Goal

Let users group, correlate, and filter dockets along dimensions the raw regulations.gov metadata cannot express:

- **By regulation:** "every docket that touches 40 CFR 60" (cross-agency, cross-year).
- **By statute:** "every active rulemaking that depends on 42 U.S.C. 7401."
- **By concept:** "every docket about PFAS" regardless of agency or CFR location.

Everything AI-derived carries provenance: how the assertion was made, by what, from what evidence, at what confidence.

## Context

Spicy-regs already holds the raw material, scattered:

| Exists today | Gap |
| --- | --- |
| `federal_register.cfr_references_json`, `unified_agenda.cfr_references_json`, `cfr_sections.cfr_ref` | CFR references live as JSON strings in three shapes. "All dockets touching a CFR part" takes a three-way JSON-unnest join. |
| `dockets.rin`, `documents.additional_rins`, `fr_docket_links` | RIN and FR↔docket edges exist but no single normalized edge table. |
| `unified_agenda.legal_authority_json` | Statutory-authority citations, unparsed and unexploited. |
| Cross-corpus tables (`congress_bills`, `court_dockets`, `gao_reports`, …) | Join keys exist; nothing connects them through rule identity. |
| — | No topic/concept layer at all. |
| — | No provenance model for derived metadata. |

## Vocabulary posture

Rulespec is the upstream vocabulary home; spicy-regs is a Level-0 consumer. The
current local implementation is pinned to the unreleased Rulespec candidate
with content digest
`sha256:2aefd3fad7782a7b16a7fa8fc08e8ceb26b5db741e0371b8fa8a9ccc1982124d`.
The earlier full-corpus exercise used
`sha256:836968b28f3b86283f53c57ae5c9ab8ebd77e96531cd4751476f1a5ee3d296f2`;
that digest remains historical evidence, not the current mapping authority.
The current candidate supplies:

1. **Identifier conventions:** canonical IRI templates for CFR citations,
   U.S.C. sections, RINs, Federal Register document numbers,
   regulations.gov docket IDs, and Public Laws. Parquet stores compact keys
   such as `cfr_ref=40-60.1`; the contract defines their expansion.
2. **Level-0 conformance:** vocabulary-only adoption in a non-JSON-LD carrier,
   with a machine-auditable mapping for every claimed term.
3. **Experimental rulemaking terms:** distinct `RegulatoryAgendaItem`,
   `RegulatoryAgendaObservation`, `Proceeding`, `Docket`, `Artifact`, and
   `CommentPeriod` entities, with qualified agenda-to-Proceeding relationships,
   evidence-bearing comment periods, and optional proceeding stages.

Spicy-regs does not mint or redefine Rulespec terms. Its local carrier mechanics
are documented in `docs/ontology.md` and omitted from the L0 claim where a flat
column cannot preserve a Rulespec term's subject, range, or direction. The
Rulespec runtime (Rust/CUE/SHACL) is not a spicy-regs runtime dependency.

Descriptive tags are retrieval-grade and live outside Rulespec's decision-grade
concept machinery. V1 makes no Rulespec concept or attestation claim for those
flat rows. A later, typed graph construction may wrap assignments in attestation
nodes, and a rare human-reviewed promotion may create a distinct decision-grade
concept with an explicit `skos:exactMatch` link.

## Architecture

Nine related Parquet tables are built by one materialized-dataset DAG and
published as one atomic generation. Ordinary `RollupPipeline` jobs retain their
single, independently schedulable output contract. One deterministic enrichment
adds Federal Register topics to an existing source table. All published columns
are VARCHAR, matching house style.

```
                    ┌─────────────┐
  federal_register ─┤             ├─ cfr_sections (targets)
  unified_agenda   ─┤ rule_targets├─ dockets / documents (subjects)
  fr_docket_links  ─┤             │
  dockets/documents─┴─────────────┘
                    ┌───────────────┐
  unified_agenda ───┤authority_edges├─ congress_bills (via Public Law only)
                    └───────────────┘
  rule_targets + authority_edges ──→ proceedings ──→ comment_periods
  unified_agenda ──→ regulatory_agenda_items ──→ agenda_item_proceedings
                                                    └─→ proceedings
  concepts ←── concept_assignments ──→ dockets / documents / cfr_sections
      ↑                                (polymorphic subjects)
  concept_events (audit log of the tagging loop)
```

### Materialized-dataset contract

`OntologyDatasetPipeline` owns the dependency and publication boundary:

- each upstream source is downloaded once and hashed; every stage reads those
  same local bytes;
- an explicit, cycle-checked DAG orders identity, proceeding, comment-period,
  concept, assignment, and event stages;
- all stateful inputs come from one prior immutable manifest and are verified by
  SHA-256, never from independently moving "latest" files;
- every output and its manifest are uploaded below one immutable snapshot
  prefix whose id binds the input, DAG, and output hashes; only after they are
  durable is `materialized/ontology/latest.json` replaced;
- MCP and data-dictionary readers resolve that pointer once and use only the
  artifact URLs in the referenced manifest.

A failed build or upload leaves the prior pointer intact. Readers therefore see
the complete old generation or the complete new generation, never a mixture.
Publishing also fails before the build when R2 configuration is incomplete,
refreshes remote inputs even when a local work directory is reused, and refuses
to treat a missing prior pointer as an implicit state reset. The first
publication or an intentional recovery must opt in with `--allow-bootstrap`.
Monday-through-Saturday runs refresh deterministic identity tables while
copying concept state from the same prior generation; Sunday runs the complete
concept DAG.

### 1. `rule_targets` — the spine (deterministic, no AI)

> **2026-07-24 revision:** the RIN/agenda-item ontology in
> `2026-07-24-rin-ontology-revision-agent-goal.md` supersedes this section
> wherever it treats Unified Agenda values as docket or Proceeding facts.

One row per (docket, CFR reference, source). Normalizes action-specific
docket↔CFR↔RIN evidence. Unified Agenda CFR values remain on the editioned
agenda observation.

| Column | Description |
| --- | --- |
| `docket_id` | Subject docket. FK to `dockets`. |
| `cfr_ref` | Compact CFR citation (`40-60` part level, `40-60.1` section level). Join key to `cfr_sections`. Null for RIN-only edges. |
| `cfr_title`, `cfr_part`, `cfr_section` | Decomposed citation. `cfr_section` usually null (FR metadata is part-level). |
| `rin` | RIN associated with this edge, when known. Join key to `unified_agenda`. |
| `source` | Provenance enum: `fr_cfr_ref` (via `fr_docket_links` + `federal_register.cfr_references_json`), `docket_rin`, `document_rin`, `document_fr_doc`. |
| `evidence_id` | The action-specific row that justifies the edge (FR `document_number`, docket id, or `document_id`). |
| `first_seen`, `last_seen` | Publication-date span of the evidence. |
| + attestation columns | See provenance model below. |

Dedup key: (`docket_id`, `cfr_ref`, `rin`, `source`). Same edge from multiple sources yields multiple rows deliberately — corroboration is signal.

### 2. `authority_edges` — statutes (deterministic parsing, quarantined)

Parses `unified_agenda.legal_authority_json` free text into U.S.C. citations. Kept separate from `rule_targets` so messy statute parsing cannot destabilize the clean CFR spine.

| Column | Description |
| --- | --- |
| `rin` | RIN whose agenda entry cites the authority. |
| `authority_raw` | Original citation string, always retained. |
| `usc_title`, `usc_section` | Parsed citation (e.g. `42`, `7401`). Null when unparsed. |
| `pl_number` | Parsed public-law number (e.g. `117-58`) when `authority_type=public_law`. Join key to `congress_bills`; carries the `us-pl` identifier scheme — without this column the scheme would freeze unexercised. |
| `authority_type` | `usc`, `public_law`, `statute_at_large`, `eo`, `other`. |
| `parse_status` | `ok`, `partial`, `failed`. Failed rows are kept — the raw string still supports search. |
| `agenda_edition` | Edition the citation came from. |
| + attestation columns | See provenance model below. |

Authority remains attached to the editioned agenda observation. It must not be
projected to a docket or Proceeding through RIN equality. Parser is rule-based
(regex grammar over common citation forms); an LLM fallback for `failed` rows
is a possible later pass and would carry attestation columns.

### 3. `concepts` — SKOS-style registry

One row per concept. Never hard-deleted, never renamed in place.

| Column | Description |
| --- | --- |
| `concept_id` | Stable opaque id. |
| `facet` | Semantic tag-policy facet. v1 ships `subject` and `regulated_entity` only. Profiles use this field to decide which kinds of tags are allowed. |
| `source_vocabulary` | Authority vocabulary used for concept identity, provenance, retrieval quotas, and Rulespec `inScheme` (for example, `federal-register-thesaurus`, `crs-subjects`, or `fast-topical`). |
| `scheme` | Deprecated compatibility mirror of `facet`. New rows must keep `scheme == facet`; no new code may use it as vocabulary identity. |
| `pref_label` | Preferred label. |
| `alt_labels_json` | JSON array of synonyms (grows on merge). |
| `definition` | One-sentence scope note. |
| `broader_id` | Parent concept (`subject` facet only). Must stay acyclic. |
| `status` | `active`, `deprecated`, `candidate`. |
| `replaced_by` | Merge target when deprecated. Queries resolve through the chain. |
| `external_ids_json` | Anchors: FR Thesaurus term, CAS number, NAICS code, `skos:exactMatch` IRIs. |
| + attestation columns | See provenance model below. |

**Seeding (v1, before any LLM runs):** the `subject` facet seeds from the Federal Register Thesaurus of Indexing Terms; `regulated_entity` starts empty and grows from extraction, anchored to CAS numbers where resolvable. The tagger extends a real taxonomy rather than inventing one.

**Facet decision:** `affected_party`, `policy_instrument`, and `program` are deferred until the merge/validation loop proves itself on two facets. The `facet` column makes adding them additive. A shared normalized label across two source vocabularies creates an unreviewed mapping record only; the selector keeps both ids selectable, and the record does not authorize a concept merge or `skos:exactMatch`.

**Compatibility boundary:** `fused-concept-registry-v1` overloaded `scheme`
with five source-vocabulary names. Readers may infer `facet` and
`source_vocabulary` for that immutable artifact only. New fusion writes the
v2 shape and keeps `scheme` equal to `facet`. Remove the inference shim when
`fused-concept-registry-v1` is no longer an accepted testbed/CLI input; rebuild
v2 from authoritative sources rather than rewriting v1 in place.

### 4. `concept_assignments` — the tag edges

One row per (subject, concept, assertion). Assignments are superseded, never updated in place.

| Column | Description |
| --- | --- |
| `assignment_id` | Stable id. |
| `subject_type` | `docket`, `document`, or `cfr_section`. v1 populates `docket` and `document` (FR abstracts + docket titles/abstracts are the input text); `cfr_section` follows once section text is ingested. |
| `subject_id` | FK into the subject table. |
| `concept_id` | FK to `concepts`. |
| `confidence` | 0–1, revised by validation passes via superseding rows. |
| `evidence_json` | Text span(s) and source field that justified the tag. |
| + attestation columns | See provenance model below. |

### 5. `concept_events` — audit log of the loop

One row per structural change: `merge`, `split`, `rename`, `deprecate`, `promote`, `seed`. Columns: `event_id`, `event_type`, `payload_json` (before/after concept ids and labels), plus attestation columns. This is the undo trail and the loop's memory.

### 6. Enrichment: `federal_register.topics_json`

New column on the existing table: the FR API's `topics` field (Thesaurus terms per document), currently dropped at ingest. Cheap, deterministic, and it powers both seeding and evaluation of the subject facet.

### 7. `proceedings` + `comment_periods`

These are first-class derived tables built directly from docket, document,
Federal Register, Unified Agenda, rule-target, and authority evidence. The
existing `rulemaking_lifecycles` table remains a separate duration rollup; it is
not treated as a proceeding and is not promoted into one.

- **`proceedings`** — one row per evidence-connected proceeding component:
  optional `rin`, one or more associated docket ids, current evidenced stage,
  stage-event history, and identity-lineage fields.
- **`comment_periods`** — one row per continuous or reopened comment window:
  proceeding/docket keys, open and close dates, source
  (`documents.comment_end_date`, `federal_register.comments_close_on`), and
  evidence-bearing provenance.

Proceeding identity is persistent state:

- a RIN is strong evidence but not globally unique, so dockets sharing a reused
  RIN remain separate unless one Federal Register document explicitly
  co-identifies them;
- ambiguous RIN-only or docket-only evidence remains unattached instead of
  fanning out across candidate proceedings;
- a new generation reuses the prior partner-scoped `proceeding_id` with the
  strongest compatible docket overlap, so a backfill or lexically earlier
  docket does not rename an existing proceeding;
- a docket-less RIN component keeps its id when it gains a first docket only if
  that RIN has one prior and one current component;
- `identity_predecessors_json` records distinct matched prior identities across
  merges or splits; the current reused id stays in the local row-version
  `supersedes_id` only, so semantic continuity never becomes a self-edge.

The full-corpus run in `docs/ontology-friction-report.md` is the consumer
exercise for Rulespec's experimental `Proceeding`, `CommentPeriod`,
`proceedingStage`, and `publishedInProceeding` terms. Publication remains
separate from that local validation.

## Provenance model (carrier columns)

Every AI- or rule-derived row carries the same local column block. Uniform
storage does not imply that every column is a direct Rulespec predicate:
Rulespec places provenance across typed `Assertion`, `ConfidenceRecord`, and
`Finding` nodes. `docs/ontology.md` claims only mappings whose subject, range,
direction, and value kind survive the flat carrier.

| Column | L0 status | Description |
| --- | --- | --- |
| `method` | local method | `deterministic`, `llm`, `embedding`, `human`. |
| `actor_id` | local actor identifier | Model id + version, ruleset version, or human identifier; not mapped directly to `rkaf:detectedBy`. |
| `run_id` | local run identifier | Pipeline run that produced the row. |
| `asserted_at` | local timestamp | ISO 8601 assertion time. |
| `supersedes_id` | local lineage | Prior assignment/row this one revises. Null for a first assertion. |

`rule_targets` and `authority_edges` are fully deterministic in v1; their `method` is `deterministic` and `actor_id` is the ruleset version, so the whole layer has uniform provenance from day one.

## The tagging loop

Batch, not streaming — the materialized dataset executes these dependent
stages:

1. **Generate:** LLM tags new/changed dockets and FR documents. Constraint: match an existing concept first; proposing a new one requires a justification and creates it as `status=candidate`.
2. **Merge pass:** over the grown cloud, propose merges from label/embedding similarity plus co-assignment evidence. Apply above threshold: loser → `deprecated` + `replaced_by`, labels absorbed into `alt_labels_json`, `merge` event logged. High-usage merges below threshold go to a human review queue (a report, not a UI, in v1).
3. **Validation pass:** an agent re-scores a sample of assignments against their evidence. Disagreement writes a superseding assignment with lower confidence — never a deletion.
4. **Re-score / converge:** candidate concepts with sustained usage and survived validation become `active`; stale candidates are `deprecated`.

**Invariants (tested):** no hard deletes; `replaced_by` chains are acyclic and resolvable; every non-deterministic row has complete attestation columns; `broader_id` graph is acyclic.

## Error handling

- Citation parse failures: row retained with `parse_status=failed` and raw text; never dropped.
- Malformed JSON in source columns: skipped rows counted and logged in the rollup summary, consistent with existing pipelines.
- Missing API keys (FR topics enrichment): keyless run is a no-op, matching `cfr_sections` behavior.
- LLM failures mid-batch: local checkpoints remain reusable by `run_id`, but no
  partial generation is published. The public pointer changes only after every
  required artifact and the generation manifest are durable.

## Testing

- **Citation parsers:** fixture suites of real messy strings for CFR and U.S.C. forms (`42 U.S.C. 7401 et seq.`, `sec. 553 of title 5`, PL numbers), asserting parsed keys and `parse_status`.
- **Spine joins:** golden-file tests per DAG stage over a small fixture Parquet
  set; known dockets must produce known edges from each `source`.
- **Generation contract:** tests reject stage cycles and unknown dependencies,
  bind source hashes into the snapshot id, verify prior hashes, and require the
  public pointer to upload last.
- **Proceeding continuity:** generation-to-generation fixtures prove that
  backfills preserve ids and that reused RINs and ambiguous evidence do not
  collapse or fan out.
- **Loop invariants:** property-style tests for the acyclicity and append-only rules above.
- **Tag quality:** evaluation harness comparing generated `subject` tags against `federal_register.topics_json` on documents that have both — Thesaurus terms are imperfect ground truth but catch drift cheaply.

## Out of scope (v1)

- Rulespec vocabulary or runtime changes. The frozen contract is an upstream
  dependency, not code owned by this repository.
- Facets beyond `subject` and `regulated_entity`; promotion of tags to decision-grade concepts.
- Comment-level tagging, campaign detection, commenter entity resolution.
- CFR section full text; OCR; any UI beyond MCP/SQL access.

## Implemented order and release gates

1. The sibling Rulespec branch froze the identifier, L0, and experimental
   rulemaking contract at the digest above. It is not yet an upstream release.
2. `rule_targets`, `authority_edges`, `proceedings`, and `comment_periods` were
   built and exercised against the full corpus. `authority_edges.pl_number`
   joins Congress bills through Public Law numbers; no U.S.C.-to-bill crosswalk
   is claimed.
3. Federal Register topics, the concept registry, assignments, validation, and
   event reconciliation were implemented as the stateful half of the same DAG.
4. The L0 audit and full-corpus friction report record the consumer evidence.
5. Public deployment waits for review of this repository and a reachable,
   versioned Rulespec contract. The first production run must be a reviewed,
   explicit `--allow-bootstrap` publication; the local implementation does not
   claim to have published either repository.
