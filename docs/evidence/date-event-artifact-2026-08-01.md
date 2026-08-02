# Unified typed date-event artifact — built and pinned, 2026-08-01

**Status: built locally, digest-pinned, unpublished.** The GitHub publication
chain for these tables is blocked (see the handoff's task-5 ordering:
rulespec push → conformance nulls → branch merge), so the deadline data ships
as a local, digest-pinned file artifact — exactly the file-only seam shape.
Downstream consumers verify by digest; nothing here requires a remote.

Tool: `tools/build_date_event_artifact.py` (tested by
`tests/test_build_date_event_artifact.py`; run targeted, never the full
suite). Artifacts live in `output/date-event-artifact-2026-08-01/`
(`date-events.parquet`, `quarantine.parquet`, `receipt.json`, plus
`fixture-slice/`) — gitignored output, pinned here by digest. Rebuilding
from the pinned inputs reproduces every file byte-for-byte (verified twice;
no timestamps inside sealed surfaces, the `draw_search_holdout.py` pattern).

## Identity

| Surface | Value |
|---|---|
| artifact_id | `urn:spicyregs:date-event-artifact:f4ea0edc6100a99b5b559d18` |
| schema_version | `date-event-artifact-v1` |
| sanity_bounds_policy | `comment-period-sanity-bounds-v1` |
| `date-events.parquet` | `sha256:98d31c7db31704ae3617fd19ddd70dbaaf872e5316ed7459727a82eeb7fad005` (845,784 rows) |
| `quarantine.parquet` | `sha256:b02343946185daff81f3f1cb2437661f8eecfce544ab37956b577b4c2cdce21b` (3,966 rows) |
| `receipt.json` | `sha256:550f8e392cb0e72ab9e15a331faf5e48f4b6860a62a7d576caca92910b6295c1` |

Pinned inputs (paths repo-relative):

| Table | Path | Rows | Digest |
|---|---|---|---|
| comment_periods | `output/rin-ontology-revision-candidate/comment_periods.parquet` | 302,300 | `sha256:583ca4861ba66311618d315b6744d308ee734ac8ba0847c71d0b996255e62d77` |
| fr_docket_links | `output/rin-ontology-revision-candidate/fr_docket_links.parquet` | 715,080 | `sha256:b3409f0ada792a8c9534edcf87c290a8b39e482e4803f08656bfa9de4504fd45` |
| fcc_proceedings | `output/mixed-real-data-corpus-v2/fcc_proceedings.parquet` | 21,054 | `sha256:db831225dd869affa15ef94c11e6bbec77cea4be00f817506f30a359ea64532c` |

The comment_periods pin is the 302,300-row validated generation; the three
byte-identical fcc_proceedings copies (`mixed-real-data-corpus-v1`, `-v2`,
`-v2-rerun`) make that choice immaterial — the digest above is the fact.

## Event counts

| event_type | comment_periods | fr_docket_links | fcc_proceedings | total |
|---|---:|---:|---:|---:|
| comment_open | 298,360 | — | 0 | 298,360 |
| comment_close | 298,360 | 154,000 | 0 | 452,360 |
| effective | — | 95,064 | — | 95,064 |
| reply_comment_close | — | — | 0 | 0 |
| **total** | 596,720 | 249,064 | 0 | **845,784** |

Each event carries: `event_id` (content-derived), `event_type`,
`event_date`, `document_ref` (FR document number, when document-anchored),
`docket_refs_json`, `proceeding_refs_json`, `rin_refs_json`, `source`,
`evidence_field` (the exact source column asserting the date), and
`evidence_refs_json`. fr_docket_links events are deduplicated to one event
per (document, field, date) with docket anchors unioned across link rows.

## Quarantine (typed partition — nothing silently dropped)

| source | date_before_1994 | date_after_2028 | duration_over_5y | inverted_interval | rows |
|---|---:|---:|---:|---:|---:|
| comment_periods | 3,257 | 48 | 686 | 0 | 3,940 |
| fr_docket_links | 20 | 6 | — | — | 26 |
| fcc_proceedings | 0 | 0 | — | 0 | 0 |
| **total** | | | | | **3,966** |

- Reason counts exceed row counts where one row trips two bounds: all 48
  `date_after_2028` rows also trip `duration_over_5y`, and 3
  `date_before_1994` rows do too (measured from the quarantine partition).
- The 3,940 differs from the previously estimated ~3,941 by the exact rule
  now pinned: close year < 1994, close year > 2028, duration > 1825 days
  (5 x 365). The rule is in the tool as `MIN_YEAR`/`MAX_YEAR`/
  `MAX_DURATION_DAYS`.
- **Inverted intervals: 0** in this pin — `build_comment_periods.py:266-272`
  already drops inverted source intervals upstream, so the receipt's
  `inverted_intervals_by_source` honestly reports zero here; the check
  remains active (and tested) for future pins and for FCC windows.
- Bounds were externally validated on comment_periods only; applying them
  uniformly to the other sources is this artifact's pinned policy
  (`comment-period-sanity-bounds-v1`), receipted per source.

## Coverage labels (in the receipt, machine-readable)

- **FCC coverage floor**: fcc_filings ingestion begins 2026-06-30
  (`FILINGS_FIRST_RUN_DAYS=30` first run) — nothing earlier is represented.
- **fcc_proceedings windows**: ECFS records comment/reply windows only
  sometimes ("often null" per the data dictionary); in this pin **0 of
  21,054 rows carry any window**, so `reply_comment_close` is 0 events. The
  extraction is implemented and tested with synthetic rows — a future pin
  with populated windows lights it up without code changes.
- comment_periods intervals arrive extension-coalesced upstream; one
  interval emits one open and one close event.

## Fixture slice (the downstream vendoring surface)

`output/date-event-artifact-2026-08-01/fixture-slice/` — rule
`first-n-per-type-by-event-id-v1` (n=25 events per type, 5 quarantine rows
per source): 75 events, 10 quarantine rows.

| Surface | Value |
|---|---|
| slice artifact_id | `urn:spicyregs:date-event-artifact-slice:07b903b87e1d51c6a3c43405` |
| slice `date-events.parquet` | `sha256:432be41a66aae4c3368a8396bb5786153635b050eec696c6cb511c1a40e84d61` (75 rows) |
| slice `quarantine.parquet` | `sha256:5a8345ec43a0525d2532db8d32304c75f16a0b206c87da94285c0ca394084ca0` (10 rows) |
| slice `receipt.json` | `sha256:29d7fbfcf41bf583f9128fcd87dbc1b272190bad46e6710a3ab83dd69b6f2938` |

The slice receipt pins the parent artifact_id and parent digests, so a
vendored copy proves its lineage without the 30 MB parent.

## What production admission still needs

This artifact is a local development input. Publishing it into the product
contract still requires: (1) the task-5 publication chain (rulespec push +
tag, the two `conformance/rulespec-l0.yaml` nulls, `feat/document-ai-pipeline`
merged so `materialize-ontology.yml` exists on main); (2) a downstream
admission policy decision (spicysearch pins it dev/fixture-mode only); (3) a
populated fcc_proceedings window pin if FCC reply deadlines are to be real;
(4) a decision on whether unbounded/ongoing periods (task-6 caveat) get a
typed representation.
