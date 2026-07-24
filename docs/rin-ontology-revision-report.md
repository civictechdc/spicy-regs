# RIN agenda-item ontology: full-corpus report

**Run date:** 2026-07-24  
**Status:** Pass  
**Publication:** Local only; no upload, release, commit, or deployment  
**Snapshot:** `snapshot_0e4b4204bdfbd462a9270fcd766fb8dd`  
**Rulespec contract:** `sha256:2aefd3fad7782a7b16a7fa8fc08e8ceb26b5db741e0371b8fa8a9ccc1982124d`

## Result

The corpus supports the agenda-item model. A RIN identifies one durable
`RegulatoryAgendaItem`; it does not identify or merge Proceedings. The existing
`unified_agenda` rows are editioned `RegulatoryAgendaObservation` artifacts
whose `foaf:primaryTopic` is that item. Direct docket, regulations.gov
document, and Federal Register evidence creates qualified,
provenance-bearing `AgendaProceedingRelationship` rows to independently
identified Proceedings.

This is a specialization of two document-agnostic seams:

1. `Artifact` → `foaf:primaryTopic` → durable subject;
2. subject → `dcat:qualifiedRelation` → relationship node →
   `dcterms:relation` plus `dcat:hadRole`.

The US regulatory profile supplies the RIN grammar, agenda classes, scope
states, and fixed relationship role. Other document genres can reuse the two
general seams without becoming regulatory agenda records.

The machine-readable [corpus receipt](evidence/rin-ontology-revision-2026-07-24/corpus-receipt.json)
passes with zero failures. Its SHA-256 is
`3a609b10ded499578845483febd873b61ca221ecb131044e7f88e86693e58746`.

## Bound input snapshot

The run used the same five source files as the prior stabilization experiment.

| Input | Rows | SHA-256 |
| --- | ---: | --- |
| `dockets.parquet` | 276,326 | `b14cd488b7898391cff448ac4de19f85936072dcb1aa105da32eea88e6fd7938` |
| `documents.parquet` | 1,987,880 | `52f085f9ec2ee0c08fe3fb59bcd789bfef34000f87608ea36af9a6adbacfb04d` |
| `federal_register.parquet` | 1,004,233 | `ac18315faa8be4a8d3656e758597d672c5d85c23cc6f8fde0ac53c9295b22bf2` |
| `fr_docket_links.parquet` | 715,080 | `b3409f0ada792a8c9534edcf87c290a8b39e482e4803f08656bfa9de4504fd45` |
| `unified_agenda.parquet` | 3,954 | `e6862d5d6a5300f10c70eeaf321f1e82e1f5332f71069d07723cc584ee6a85ae` |

This snapshot contains one Unified Agenda edition (`202510`). Multiple-edition
behavior is therefore covered by Rulespec and Spicy Regs fixtures rather than
claimed as corpus evidence.

## Materialized results

| Surface | Rows | Interpretation |
| --- | ---: | --- |
| `regulatory_agenda_items` | 38,005 | One row per valid RIN observed anywhere in the bound sources |
| `agenda_item_proceedings` | 120,685 | Direct, evidence-bearing relationship assertions |
| `proceedings` | 511,643 | Action identities assembled without RIN grouping |
| `rule_targets` | 39,516 | Action-specific docket/RIN/CFR evidence |
| `authority_edges` | 10,618 | Agenda-observation authority citations; 903 failed parses retained |
| `comment_periods` | 302,300 | Action/docket comment intervals |

The prior rule-target table had 40,546 rows. Exactly 1,030
`ua_cfr_ref` projections were removed; the remaining sources are
`docket_rin` (285), `document_rin` (26), `fr_cfr_ref` (16,467), and
`document_fr_doc` (22,738). All 10,618 authority rows and all 3,954 Unified
Agenda observations remain available at their source grain.

## Invariant checks

| Check | Result |
| --- | --- |
| Dockets assigned to more than one Proceeding | 0 |
| Federal Register artifacts assigned to more than one Proceeding | 0 |
| Multi-docket Proceedings without an FR artifact explicitly linking at least two member dockets | 0 of 259 |
| Proceeding authority arrays populated from Unified Agenda | 0 |
| Proceeding stage events sourced from Unified Agenda | 0 |
| Rule-target rows sourced from Unified Agenda | 0 |
| Invalid or non-source docket identifiers in generated tables | 0 |
| Receipt referential, uniqueness, provenance, schema, or scope failures | 0 |

The new model exposes the pressure that the prior RIN-bounded assembler hid:
20,872 RINs now have direct evidence for more than one distinct Proceeding,
covering 102,331 child Proceedings. Multiplicity is not treated as recurrence.
Of these, 20,858 items with 67,500 children remain `unresolved`.

Scope classifications are:

| Scope | Basis | Items |
| --- | --- | ---: |
| `recurring` | Latest official priority is `Routine and Frequent` | 41 |
| `single_observed` | Exactly one directly evidenced Proceeding | 15,200 |
| `unresolved` | Multiple directly evidenced Proceedings | 20,858 |
| `unresolved` | No directly evidenced Proceeding | 1,906 |

The 41 officially recurring items cover 34,840 directly evidenced Proceedings
in this historical Federal Register corpus. Recurring status comes from the
official priority, never from that count.

## Required corpus cases

| RIN | Result |
| --- | --- |
| `0301-AA02` | One agenda item, one `202510` observation, one directly evidenced Proceeding; `single_observed` |
| `1625-AA00` | One item, official `Routine and Frequent` priority, 4,344 distinct Proceedings, 4,351 evidence rows; `recurring` |
| `2120-AA64` | One item, official `Routine and Frequent` priority, 23,281 distinct Proceedings; `recurring` |
| `2070-AB27` | One item, 194 directly evidenced Proceedings, no current UA observation proving recurrence; `unresolved` |

Examples for `1625-AA00` include separate artifact-backed Proceedings for
“Safety Zone; Balloon Glow Fireworks, Manitowoc River, Manitowoc, WI” and
“Safety Zone; Hawks Channel, Marathon, FL.” Examples for `2120-AA64` include
distinct airworthiness-directive Proceedings for Dassault Falcon aircraft,
McDonnell Douglas aircraft, and Pratt & Whitney engines. These are sibling
actions under an agenda item, not one merged process.

## Stable-ID migration

Of 312,298 prior Proceeding IDs, 307,988 (98.62%) survive unchanged. For prior
docket-bearing rows, 275,324 of 277,302 IDs (99.29%) survive. The remaining
changes correspond to evidence-proved merges or to splitting the former
RIN-bounded identity. Every resulting merge/split records predecessor
continuity; the receipt found 48,679 distinct predecessor edges and zero
self-edges.

## Deterministic rerun

The complete identity-only materialization was rerun with the same five source
hashes, prior-state files, run id, and assertion time. All nine artifact
SHA-256 values and the snapshot id matched byte-for-byte; the rerun again
produced `snapshot_0e4b4204bdfbd462a9270fcd766fb8dd`.

## Query cookbook

### Agenda meaning and scope

```sql
SELECT
  i.rin,
  i.scope_status,
  i.scope_basis,
  i.linked_proceeding_count,
  u.agenda_edition,
  u.title,
  u.abstract,
  u.rule_stage,
  u.priority_category,
  u.timetable_json,
  u.cfr_references_json,
  u.legal_authority_json
FROM regulatory_agenda_items AS i
LEFT JOIN unified_agenda AS u
  ON u.rin = i.rin
WHERE i.rin = '1625-AA00'
ORDER BY u.agenda_edition DESC;
```

This query answers what the RIN represents and whether its scope is officially
recurring, single-observed, or unresolved. Stage, priority, timetable, CFR, and
authority remain on `u`, the editioned observation.

### Proceedings with direct relationship evidence

```sql
SELECT
  r.rin,
  r.proceeding_id,
  r.source,
  r.evidence_id,
  r.evidence_uri,
  p.title,
  p.docket_ids_json,
  p.fr_document_numbers_json
FROM agenda_item_proceedings AS r
JOIN proceedings AS p USING (proceeding_id)
WHERE r.rin = '2120-AA64'
ORDER BY r.evidence_date DESC, r.proceeding_id;
```

Several rows may corroborate the same item/Proceeding relationship. Use
`SELECT DISTINCT rin, proceeding_id` when evidence detail is not needed.

### Selected child action facts

```sql
SELECT
  p.proceeding_id,
  p.current_stage,
  p.stage_events_json,
  p.cfr_target_iris_json,
  p.authority_refs_json,
  p.docket_ids_json,
  p.fr_document_numbers_json,
  c.comment_period_id,
  c.open_date,
  c.close_date
FROM proceedings AS p
LEFT JOIN comment_periods AS c
  ON json_contains(c.proceeding_ids_json, to_json(p.proceeding_id))
WHERE p.proceeding_id = ?;
```

These are action-specific facts. In the current carrier,
`authority_refs_json` remains empty unless a future action-specific authority
source is added.

### OIRA reviews or meetings

The current corpus does not ingest OIRA review or meeting-log products. A
future source must join first to the agenda item by RIN:

```sql
SELECT i.agenda_item_id, o.*
FROM oira_reviews_or_meetings AS o
JOIN regulatory_agenda_items AS i USING (rin);
```

That join says the OIRA record concerns the agenda item. It must not be joined
to every child Proceeding. An action-level link requires separate,
action-specific evidence and another qualified relationship assertion.

## Reproduction

```console
R2_PUBLIC_URL='' ONTOLOGY_RUN_ID='rin-ontology-revision-2026-07-24' \
  uv run materialize-ontology \
  --output-dir output/rin-ontology-revision-candidate \
  --no-full-refresh --skip-upload

R2_PUBLIC_URL='' uv run spicy-regs-ontology-receipt \
  output/rin-ontology-revision-candidate/ontology-dataset-manifest.json \
  --spicy-repo . --rulespec-repo ../rulespec \
  --output docs/evidence/rin-ontology-revision-2026-07-24/corpus-receipt.json
```
