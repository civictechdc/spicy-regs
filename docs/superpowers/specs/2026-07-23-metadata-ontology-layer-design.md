# SpicyRegs Metadata & Ontology Layer — Design

- **Date:** 2026-07-23
- **Status:** Draft for review
- **Parent:** `docs/superpowers/specs/2026-07-23-regulatory-ontology-program-overview.md`
- **Scope:** Spec 1 of 2. This spec covers the spicy-regs metadata layer: a rule-identity spine, statutory-authority edges, and an iterative concept-tagging system, all with explicit provenance. Spec 2 (separate, later) covers restructuring [Formspec-Labs/rulespec](https://github.com/Formspec-Labs/rulespec) into vocabulary modules, informed by what this implementation proves.

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

We own both repos. Rulespec is the vocabulary home; spicy-regs is its first consumer. This spec depends on two small rulespec deliverables (each roughly a page of spec, done first):

1. **Identifier conventions:** canonical IRI templates for CFR citations, U.S.C. sections, RINs, FR document numbers, and regulations.gov docket IDs. Parquet stores compact keys (e.g. `cfr_ref` `40-60.1`); the conventions page defines the IRI each key expands to.
2. **Level-0 conformance tier:** "vocabulary-only" adoption — use the terms and identifier schemes in flat/tabular data, no JSON-LD graph or SHACL/CUE validation required. Spicy-regs conforms at Level 0.

Rulespec attestation and concept terms are reused where they fit; docket-process terms we mint are documented in `docs/ontology.md` (new) with their intended future home in rulespec's rulemaking-process module (spec 2). The full rulespec runtime (Rust/CUE/SHACL) is never a spicy-regs dependency.

Descriptive tags (this spec) are retrieval-grade and live outside rulespec's decision-grade concept machinery. Two touchpoints only: attestation terms wrap tag assignments, and tags may link to decision-grade concepts via `skos:exactMatch`. Promotion of a tag into a decision-grade concept is a rare, human-reviewed, attested event (out of scope for v1).

## Architecture

Five new parquet tables, built as rollup pipelines in the existing `RollupPipeline` pattern (one module per table under `src/spicy_regs/pipelines/rollups/`, GitHub Actions workflow per rollup, published to R2, queryable via MCP `query_sql`). All columns VARCHAR, matching house style. One enrichment to an existing table.

```
                    ┌─────────────┐
  federal_register ─┤             ├─ cfr_sections (targets)
  unified_agenda   ─┤ rule_targets├─ dockets / documents (subjects)
  fr_docket_links  ─┤             │
  dockets/documents─┴─────────────┘
                    ┌───────────────┐
  unified_agenda ───┤authority_edges├─ congress_bills (via U.S.C./PL keys)
                    └───────────────┘
  concepts ←── concept_assignments ──→ dockets / documents / cfr_sections
      ↑                                (polymorphic subjects)
  concept_events (audit log of the tagging loop)
```

### 1. `rule_targets` — the spine (deterministic, no AI)

One row per (docket, CFR reference, source). Normalizes every docket↔CFR↔RIN edge currently latent in JSON columns.

| Column | Description |
| --- | --- |
| `docket_id` | Subject docket. FK to `dockets`. |
| `cfr_ref` | Compact CFR citation (`40-60` part level, `40-60.1` section level). Join key to `cfr_sections`. Null for RIN-only edges. |
| `cfr_title`, `cfr_part`, `cfr_section` | Decomposed citation. `cfr_section` usually null (FR metadata is part-level). |
| `rin` | RIN associated with this edge, when known. Join key to `unified_agenda`. |
| `source` | Provenance enum: `fr_cfr_ref` (via `fr_docket_links` + `federal_register.cfr_references_json`), `ua_cfr_ref` (via RIN + `unified_agenda.cfr_references_json`), `docket_rin`, `document_rin`, `document_fr_doc`. |
| `evidence_id` | The row that justifies the edge (FR `document_number`, UA `rin`+`agenda_edition`, or `document_id`). |
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
| `authority_type` | `usc`, `public_law`, `statute_at_large`, `eo`, `other`. |
| `parse_status` | `ok`, `partial`, `failed`. Failed rows are kept — the raw string still supports search. |
| `agenda_edition` | Edition the citation came from. |
| + attestation columns | See provenance model below. |

Docket-level authority queries go through `rule_targets` on `rin`. Parser is rule-based (regex grammar over common citation forms); an LLM fallback for `failed` rows is a possible later pass and would carry attestation columns.

### 3. `concepts` — SKOS-style registry

One row per concept. Never hard-deleted, never renamed in place.

| Column | Description |
| --- | --- |
| `concept_id` | Stable opaque id. |
| `scheme` | Facet: v1 ships `subject` and `regulated_entity` only. |
| `pref_label` | Preferred label. |
| `alt_labels_json` | JSON array of synonyms (grows on merge). |
| `definition` | One-sentence scope note. |
| `broader_id` | Parent concept (`subject` facet only). Must stay acyclic. |
| `status` | `active`, `deprecated`, `candidate`. |
| `replaced_by` | Merge target when deprecated. Queries resolve through the chain. |
| `external_ids_json` | Anchors: FR Thesaurus term, CAS number, NAICS code, `skos:exactMatch` IRIs. |
| + attestation columns | See provenance model below. |

**Seeding (v1, before any LLM runs):** the `subject` scheme seeds from the Federal Register Thesaurus of Indexing Terms; `regulated_entity` starts empty and grows from extraction, anchored to CAS numbers where resolvable. The tagger extends a real taxonomy rather than inventing one.

**Facet decision:** `affected_party`, `policy_instrument`, and `program` are deferred until the merge/validation loop proves itself on two facets. The `scheme` column makes adding them additive.

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

## Provenance model (attestation columns)

Every AI- or rule-derived row carries the same column block, aligned with rulespec's attestation terms (mapping table maintained in `docs/ontology.md`):

| Column | Rulespec alignment | Description |
| --- | --- | --- |
| `method` | attestation method | `deterministic`, `llm`, `embedding`, `human`. |
| `actor_id` | `rkaf:detectedBy` | Model id + version, ruleset version, or human identifier. |
| `run_id` | evidence binding | Pipeline run that produced the row. |
| `asserted_at` | attestation timestamp | ISO 8601. |
| `supersedes_id` | lineage | Prior assignment/row this one revises. Null for first assertion. |

`rule_targets` and `authority_edges` are fully deterministic in v1; their `method` is `deterministic` and `actor_id` is the ruleset version, so the whole layer has uniform provenance from day one.

## The tagging loop

Batch, not streaming — each phase is a rollup-style job:

1. **Generate:** LLM tags new/changed dockets and FR documents. Constraint: match an existing concept first; proposing a new one requires a justification and creates it as `status=candidate`.
2. **Merge pass:** over the grown cloud, propose merges from label/embedding similarity plus co-assignment evidence. Apply above threshold: loser → `deprecated` + `replaced_by`, labels absorbed into `alt_labels_json`, `merge` event logged. High-usage merges below threshold go to a human review queue (a report, not a UI, in v1).
3. **Validation pass:** an agent re-scores a sample of assignments against their evidence. Disagreement writes a superseding assignment with lower confidence — never a deletion.
4. **Re-score / converge:** candidate concepts with sustained usage and survived validation become `active`; stale candidates are `deprecated`.

**Invariants (tested):** no hard deletes; `replaced_by` chains are acyclic and resolvable; every non-deterministic row has complete attestation columns; `broader_id` graph is acyclic.

## Error handling

- Citation parse failures: row retained with `parse_status=failed` and raw text; never dropped.
- Malformed JSON in source columns: skipped rows counted and logged in the rollup summary, consistent with existing pipelines.
- Missing API keys (FR topics enrichment): keyless run is a no-op, matching `cfr_sections` behavior.
- LLM failures mid-batch: the run is resumable by `run_id`; partial output is valid because assignments are append-only.

## Testing

- **Citation parsers:** fixture suites of real messy strings for CFR and U.S.C. forms (`42 U.S.C. 7401 et seq.`, `sec. 553 of title 5`, PL numbers), asserting parsed keys and `parse_status`.
- **Spine joins:** golden-file tests per rollup (existing pattern) over a small fixture parquet set; known dockets must produce known edges from each `source`.
- **Loop invariants:** property-style tests for the acyclicity and append-only rules above.
- **Tag quality:** evaluation harness comparing generated `subject` tags against `federal_register.topics_json` on documents that have both — Thesaurus terms are imperfect ground truth but catch drift cheaply.

## Out of scope (v1)

- Rulespec modular restructure and rulemaking-process entities (spec 2).
- Facets beyond `subject` and `regulated_entity`; promotion of tags to decision-grade concepts.
- Comment-level tagging, campaign detection, commenter entity resolution.
- CFR section full text; OCR; any UI beyond MCP/SQL access.

## Delivery order

1. Rulespec: identifier-conventions page + Level-0 tier definition (prerequisite, ~1 page each).
2. `rule_targets` + `docs/ontology.md` — immediate filtering payoff, zero AI.
3. `authority_edges` + `congress_bills` join validation.
4. FR `topics_json` enrichment; seed `concepts`.
5. `concept_assignments` generate pass; then merge, validation, re-score jobs.
