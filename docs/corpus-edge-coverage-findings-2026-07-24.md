# Corpus edge-coverage findings (2026-07-24)

Incidental findings surfaced while building the graph-engine bake-off
(`docs/superpowers/specs/2026-07-24-graph-engine-carrier-decision.md`,
`docs/evidence/graph-engine-bakeoff-2026-07-24/`). Extracting a real typed graph
for the benchmark exposed gaps in the **deterministic edge layer** that no query
engine can fix — the edges have to be extracted and joinable first.

- **Snapshot:** `output/mixed-real-data-all-profile-openai-v1` (a small mixed-source
  evaluation generation), plus cross-checks on `mixed-real-data-openai-run-v2` and
  `rulespec-realworld-iteration-2`.
- **Spicy Regs commit:** `52bba99` (branch `main`).
- **Nature:** point-in-time observations, not a paired gate receipt.

> **Current disposition (2026-07-24):** Keep this file as Spicy Regs extraction
> evidence. It does not live in the sibling Rulespec repository. Findings 1 and
> 2 remain confirmed in current code. Finding 3 remains parser-quality evidence,
> but its percentages describe only the named small snapshot. Finding 4 predates
> the RIN agenda-item/Proceeding revision and requires a fresh measurement.
> Finding 5 remains an unclosed evaluation-assembly check, not a demonstrated
> production-generation defect.

## Summary

| # | Finding | Assessment | Triage |
| --- | --- | --- | --- |
| 1 | Docket identifiers stored unnormalized; deterministic FR↔docket links silently miss decorated-but-real IDs | Confirmed gap | Spicy Regs local — identity (`RULE-010`) |
| 2 | Concept hierarchy (`broader_id`) never populated in any generation | Confirmed absent; needs a decision | Spicy Regs local — retrieval (`RULE-031`) |
| 3 | Authority-citation parser: 23% failed, 33% partial | Confirmed; data friction | Spicy Regs local — parser (`RULE-005`/`RULE-006`) |
| 4 | ~Half of proceedings have no docket and no CFR target | Partly downstream of #1; re-measure after fix | Spicy Regs local |
| 5 | `fr_docket_links.parquet` out of sync with sibling `federal_register.parquet` in this eval dir | Likely eval-assembly artifact; verify | Investigate |
| — | `rule_targets.cfr_section` NULL for all rows | **Verified non-issue** — source is part-level | none |

## Current action ledger

| Finding | Current status | Next evidence |
| --- | --- | --- |
| 1. Docket normalization | Open and confirmed | Add a preserved raw value plus normalized registry key; rerun the 13-edge recovery fixture and full deterministic index |
| 2. Concept hierarchy | Open and confirmed | Decide whether hierarchy belongs in the current retrieval-grade registry; if deferred, record the flat taxonomy as an intentional limitation |
| 3. Authority parser | Open, snapshot-specific rate | Classify failures on the full corpus before changing parsing rules |
| 4. Proceedings without targets | Stale measurement | Recompute against the current independently identified Proceedings and agenda-item relationships |
| 5. Evaluation generation sync | Investigation open | Rebuild the evaluation atomically and compare every derived input digest |

## 1. Docket identifiers are stored unnormalized (confirmed)

`build_fr_docket_links.py` explodes `federal_register.docket_ids_json` into one
row per docket reference but does **not** normalize the emitted `docket_id`. The
Federal Register ingestion (a separate upstream path) supplies decorated
strings, so the link table carries values like:

```
'Doc. No. AMS-SC-24-0046'
'Docket No. FAA-2026-3485'
'Docket Number USCG-2026-0762'
'Docket No. OSM-2025-0007'
```

The docket spine (`dockets`, `rule_targets`) keys on the **bare** ID
(`FAA-2026-3485`). Any downstream match between the two therefore fails for
decorated references. Measured on this snapshot's 22 FR docs that carry docket
references:

| Match against `dockets` | Links |
| --- | --- |
| Raw string (current behavior) | 1 |
| After stripping `Docket No.`/`Doc. No.` and normalizing separators | **14** |

Thirteen correct deterministic edges (CPSC-2010-0075, FAA-2026-4820,
PHMSA-2025-0118, FSIS-2025-0012, …) are recoverable by normalization alone. The
remaining eight references (`REG-103193-26`, `CMS-9897-F`, `FRL-12765-02-OCSPP`,
`Amendment 39-…`, `FX…`, `Special Conditions No. 25-893-SC`) are genuinely
non-regulations.gov identifiers and **correctly** do not match a regs.gov docket
— so the fix is targeted normalization plus honest quarantine of the rest, not
force-matching everything.

This is the deterministic-identity discipline in `RULE-010`
("normalized identifiers", "Preserve raw source values beside normalized
identifiers", "Reject identifiers that match a lexical pattern but lack required
source-of-record evidence"). The full corpus builds 715,080 `fr_docket_links`,
so the pipeline is not broadly broken; this is a **long-tail normalization gap**
that suppresses real edges and directly thins the `RULE-026` deterministic index.

**Scope note.** The same unnormalized-identifier pattern likely affects other
cross-references (`documents.additional_rins`, proceeding docket lists, and some
of the "partial" authority parses in finding 3). Treat #1 as the first instance
of a systemic normalization pass, not a one-file fix.

**Suggested fix.** Add a normalized docket key beside the preserved raw value in
the identity layer; re-key FR↔docket matching on it.

**Acceptance criteria.** On this snapshot, deterministic FR↔docket linking
recovers the 13 decorated-but-real edges, the eight non-regs.gov references are
quarantined as unresolved (not silently dropped and not force-matched), and raw
source strings remain preserved.

## 2. Concept hierarchy is never populated (confirmed; needs a decision)

`concepts.broader_id` is NULL in **every** generation checked, including the
900-concept `rulespec-realworld-iteration-2`:

| Generation | Concepts | With `broader_id` |
| --- | ---: | ---: |
| `mixed-real-data-all-profile-openai-v1` | 97 | 0 |
| `mixed-real-data-openai-run-v2` | 78 | 0 |
| `rulespec-realworld-iteration-2` | 900 | 0 |

The SKOS taxonomy is entirely flat, so hierarchical retrieval ("subjects tagged
with concept X **or any descendant**") is impossible, and the taxonomy-traversal
workload a graph engine would most benefit does not yet exist. This is a
`RULE-031` concern. **Decision needed:** is broader/narrower inference in scope
for retrieval-grade concepts now, or deliberately deferred? If deferred, record
it so the flat taxonomy is a known state rather than an apparent defect.

## 3. Authority-citation parser quality (confirmed friction)

`authority_edges.parse_status` on this snapshot:

| Status | Rows | Share |
| --- | ---: | ---: |
| ok | 23 | 44% |
| partial | 17 | 33% |
| failed | 12 | 23% |

Only 44% parse cleanly; 23% fail outright and are retained as friction (correct
per the retain-failed-parses design). Some `partial`/`failed` cases are likely
normalization-adjacent to #1. This is `RULE-005`/`RULE-006` material: quantify
the failure classes and decide parser fix vs recorded data friction.

## 4. Proceedings with no deterministic targets (re-measure after #1)

23 of 47 proceedings (49%) have both empty `docket_ids_json` and empty
`cfr_refs_json` — near-isolated nodes that contribute little to traversal or
discovery, and part of why the bake-off's uncited-but-related query found only a
handful of related RINs. Some are legitimately agenda-only proceedings with no
downstream artifacts yet; others may be an artifact of the docket-linking gap in
#1. Re-measure after #1 before treating this as its own issue.

## 5. Generation consistency in the eval dir (verify)

In `mixed-real-data-all-profile-openai-v1`, `fr_docket_links.parquet` holds 1
row (`EPA-R05-OAR-2024-0461` / `2026-14318`), but running the builder over the
sibling `federal_register.parquet` in the same directory would emit 36 rows.
The link table is out of sync with its input. The atomic-generation contract
("no individual table workflow can expose a mixed generation") is a stated
invariant, so this is worth confirming — though it is most likely an artifact of
how this evaluation corpus was hand-assembled rather than a production pipeline
defect.

## Verified non-issue

`rule_targets.cfr_section` is NULL for all 57 rows, but the Unified Agenda source
cites at part level (`"9 CFR part 381"`, `"5 CFR 890"`). Part-level targets are
the correct grain here, not a builder defect. (Minor: `"Not Yet Determined"`
appears as a CFR reference and should be quarantined as unknown.)

## Recommended sequencing

The graph-engine decision parked the query-engine question correctly; the real
near-term work is **edge coverage before any retrieval/graph machinery**. That
means `RULE-010` deterministic-identity hardening — starting with docket
normalization (finding 1, measured 14× recovery) — which already precedes
`RULE-026` in the program start order. Findings 2–4 follow; finding 5 is a quick
verification.
