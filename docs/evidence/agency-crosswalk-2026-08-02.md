# Agency crosswalk artifact — built and pinned, 2026-08-02

**Status: built locally, digest-pinned, unpublished.** This is the artifact
that unblocks the CFR-part soft-priors experiment (spicysearch task 1c). Like
the date-event artifact it ships as a local, digest-pinned file surface —
downstream consumers verify by digest; nothing here requires a remote.

Tool: `tools/build_agency_crosswalk_artifact.py` (tested by
`tests/test_build_agency_crosswalk_artifact.py`, 21 tests; run targeted,
never the full suite). Artifacts live in `output/agency-crosswalk-2026-08-02/`
— gitignored output, pinned here by digest. Rebuilding from the pinned inputs
reproduces every file byte-for-byte (verified: a second build to a scratch
directory matched all six files).

## Identity

| Surface | Value |
|---|---|
| artifact_id | `urn:spicyregs:agency-crosswalk-artifact:d37527713d8080a7e1c3643d` |
| schema_version | `agency-crosswalk-artifact-v1` |
| tier_policy | `share-and-support-tiers-v1` |
| `agency-crosswalk.parquet` | `sha256:3d8ff2d3b64b5d6725167382ea7ea2ef94af32a3d5707079333bf05196102e6a` (742 rows) |
| `agency-codes.parquet` | `sha256:4f2cdf6c19de04bf5d0efe69d62307026b60608eabb08d0d3d37b8fe0136b66e` (316 rows) |
| `agency-parents.parquet` | `sha256:8eef8ed4e0659db21ed5f04e2e07e49f0eae1a7dde77cd1063285980af47aefe` (448 rows) |
| `cfr-part-agencies.parquet` | `sha256:0e9615577a5705fbbb875ea087ea2f5ad01604b3360ec8c531b7c57706f310e7` (34,612 rows) |
| `quarantine.parquet` | `sha256:a740ca66e53c8b37c723e9a8d17dd2b72c1dd1321c293398cad80ff2f0cd048a` (35,662 rows) |
| `receipt.json` | `sha256:b6b5ec74829ef74b5eefcd411380768d9945fbeae525a6a8364579eaf8b09746` |

Pinned inputs (paths repo-relative):

| Table | Path | Rows | Digest |
|---|---|---:|---|
| federal_register | `output/rin-ontology-revision-candidate/federal_register.parquet` | 1,004,233 | `sha256:ac18315faa8be4a8d3656e758597d672c5d85c23cc6f8fde0ac53c9295b22bf2` |
| dockets | `output/rin-ontology-revision-candidate/dockets.parquet` | 276,326 | `sha256:b14cd488b7898391cff448ac4de19f85936072dcb1aa105da32eea88e6fd7938` |
| fr_docket_links | `output/rin-ontology-revision-candidate/fr_docket_links.parquet` | 715,080 | `sha256:b3409f0ada792a8c9534edcf87c290a8b39e482e4803f08656bfa9de4504fd45` |
| documents | `output/rin-ontology-revision-candidate/documents.parquet` | 1,987,880 | `sha256:52f085f9ec2ee0c08fe3fb59bcd789bfef34000f87608ea36af9a6adbacfb04d` |

The `fr_docket_links` digest is byte-identical to the one pinned by the
date-event artifact — the two artifacts read the same generation.

## Nothing new was parsed

`federal_register.cfr_references_json` **already carries parsed CFR
references** (`{"chapter", "citation_url", "part", "title"}` per reference) on
205,255 of the 1,004,233 documents. `agencies_json` already carries
`slug`/`id`/`parent_id` per agency. No parser was written and none was
needed; the ~15 identifier tables were checked first (`rule_targets` has
`cfr_title`/`cfr_part` but is docket-scoped and only 39,516 rows;
`cfr_sections` is a 40,000-row GPO granule index, not a citation surface).

## The join: agency FIELD, never docket-id prefixes

Two evidence paths, both through an agency *field*:

| Path | Route | Link rows joined |
|---|---|---:|
| `dockets_fr_links` | `dockets.agency_code` → `fr_docket_links` → `federal_register.agencies_json` | 47,338 |
| `documents_fr_doc_num` | `documents.agency_code` + `documents.fr_doc_num` → `federal_register.agencies_json` | 3,029 |

A test pins the prohibition directly: dockets whose ids all read `EPA-HQ-…`
but whose `agency_code` says `NRC` produce an `NRC` row and no `EPA` row.
This matters — the earlier naive cut derived codes from id prefixes and
produced **252 pseudo-codes**, more than the 196 agency codes that actually
exist in the dockets table.

**The dominant coverage fact**: 667,742 of 715,080 `fr_docket_links` rows
(93.4%) carry a docket id that is not in the dockets table at all — Federal
Register docket strings like `FRL-13416-01-OCSPP`, not regulations.gov
dockets. Only 19,893 of 500,275 distinct link docket ids intersect. These are
expected non-overlap, not defects, so they are **counted and named in the
receipt's `coverage` block** rather than materialized as 667k quarantine
rows. That boundary is itself pinned by a test.

## Tiers (pinned constants, in the receipt)

| Constant | Value |
|---|---|
| `CONFIDENT_SHARE` | 0.8 |
| `PROBABLE_SHARE` | 0.6 |
| `MIN_CONFIDENT_DOCUMENTS` | 5 |
| `MIN_PROBABLE_DOCUMENTS` | 2 |
| `SPECIFICITY_MARGIN` | 0.05 |

`share` is the fraction of a code's supporting FR documents whose
`agencies_json` names a given slug. Support floors are load-bearing: a
perfect share over one document is not confidence, and without the floors the
histogram collapses to 125 "confident" codes, 26 of which rest on a single
document.

**The specificity rule.** Sub-agency documents almost always name both the
department and the sub-agency, so several slugs legitimately reach share 1.0
and share alone cannot resolve the code. Candidates within
`SPECIFICITY_MARGIN` of the best share are treated as tied on evidence and
the deeper slug in the `parent_id` chain wins. Worked cases from this build:

- `FAA` — `federal-aviation-administration` and `transportation-department`
  both at share 1.000000; specificity picks the sub-agency.
- `NHTSA` — `transportation-department` actually out-polls
  `national-highway-traffic-safety-administration` (share 0.972222); they are
  within the margin, so the sub-agency still wins. Ranking on share alone
  would map `NHTSA` to its parent department.
- `DOT` — `transportation-department` at 1.000000 with sub-agencies far below
  the margin; the department correctly stays primary.

Both directions are pinned by tests, and both were mutation-checked: setting
`SPECIFICITY_MARGIN` to 0 fails the near-tie test, and setting
`MIN_CONFIDENT_DOCUMENTS` to 1 fails the thin-evidence test.

## Tier histogram (the headline numbers)

316 agency codes total — 196 from the dockets table, all of which also appear
in `documents`, plus 120 seen only in `documents`.

| Tier | In dockets table | documents-only | Total |
|---|---:|---:|---:|
| confident | 93 | 12 | **105** |
| probable | 18 | 6 | **24** |
| ambiguous | 14 | 12 | **26** |
| unmapped | 71 | 90 | **161** |
| **total** | **196** | **120** | **316** |

`agency-crosswalk.parquet` holds 742 candidate (code, slug) rows across the
155 codes that have any evidence.

**Against the naive cut** (92 confident ≥60%, 160 ambiguous): the comparison
is not apples-to-apples and should not be quoted as one. The naive cut scored
252 prefix-derived pseudo-codes; this build scores 316 real codes from agency
fields. Restricting to the 196 codes that exist in the dockets table, **111
reach share ≥ 0.6 with real support** (93 confident + 18 probable) against
the naive 92, 14 are genuinely contested, and 71 are simply never reached by
either join. The naive cut's "160 ambiguous" was mostly prefix noise; the
honest residual here is 71 unreached codes, not 160 uncertain ones.

The `ambiguous` tier is dominated by thin evidence rather than conflict: most
ambiguous codes (e.g. `CIA`, `CRB`, `DRBC`, `EAC`, `EIB`) sit at share
1.000000 over a single document. Their candidate slug is probably right —
the tier says the artifact will not vouch for it. Downstream decides; nothing
was dropped.

Sample of the confident tier:

| Code | Primary slug | Share | Docs | Candidates |
|---|---|---:|---:|---:|
| EPA | environmental-protection-agency | 0.999873 | 23,627 | 16 |
| NRC | nuclear-regulatory-commission | 0.999754 | 8,133 | 4 |
| USCG | coast-guard | 0.967880 | 1,868 | 9 |
| FMCSA | federal-motor-carrier-safety-administration | 1.000000 | 917 | 2 |
| FDA | food-and-drug-administration | 0.996904 | 323 | 3 |
| FAA | federal-aviation-administration | 1.000000 | 185 | 2 |
| NHTSA | national-highway-traffic-safety-administration | 0.972222 | 108 | 4 |
| DOT | transportation-department | 1.000000 | 84 | 6 |
| CMS | centers-for-medicare-medicaid-services | 0.981818 | 55 | 2 |
| OCC | comptroller-of-the-currency | 0.833333 | 6 | 10 |

## Parent-department mapping (its own table)

`agency-parents.parquet` — 448 slugs, of which **212 carry a `parent_id` and
all 212 resolve to a parent slug present in the same pin** (no dangling
lineage in this generation). Each row carries `agency_slug`, `agency_id`,
`parent_id`, `parent_slug`, `depth` (steps to the root of the chain, stopping
at anything unresolvable) and `documents` (FR documents naming that slug).

## CFR part → agency association (the priors input)

`cfr-part-agencies.parquet` — **9,320 distinct (title, part) pairs** across 50
CFR titles, yielding **34,612 (title, part, agency) rows** from the 205,255
documents carrying CFR references.

- 3,797 pairs (40.9%) are cited by exactly one agency; the widest pair is
  cited by 51.
- **8,957 of 9,284 populated pairs (96.5%) have a top agency at share ≥ 0.8** —
  the signal the soft-priors experiment is built on is strong.
- 36 of the 9,320 pairs produce no rows: every document citing them has no
  resolvable agency slug. 9,284 + 36 = 9,320, and the receipt counts both.

| CFR pair | Docs | Top agency (share) | Agencies |
|---|---:|---|---:|
| 14 CFR 39 | 24,833 | transportation-department (0.999758) | 7 |
| 40 CFR 52 | 15,224 | environmental-protection-agency (0.998949) | 22 |
| 14 CFR 71 | 11,114 | transportation-department (0.998740) | 11 |
| 33 CFR 165 | 7,507 | coast-guard (0.969628) | 5 |
| 47 CFR 73 | 5,616 | federal-communications-commission (0.997685) | 2 |

**`is_primary` here means "most-citing agency", not the crosswalk's
specificity-resolved primary — the two answer different questions and the
tool deliberately does not conflate them.** 14 CFR 39 (airworthiness
directives) ranks `transportation-department` (0.999758) above
`federal-aviation-administration` (0.943825) because the department is named
on marginally more documents, and the 0.056 gap is wider than
`SPECIFICITY_MARGIN`. A consumer wanting the sub-agency answer should join
through `agency-parents.parquet` or use the full ranked list, both of which
carry every citing slug with its share.

## Quarantine (typed partition — nothing silently dropped)

| source | reason | rows |
|---|---|---:|
| federal_register | `agency_entry_missing_slug` | 30,405 |
| federal_register | `cfr_reference_missing_part` | 5,225 |
| documents | `document_not_in_federal_register` | 32 |
| **total** | | **35,662** |

- **`agency_entry_missing_slug` (30,405)** — `agencies_json` entries carrying
  only a `raw_name`, across 2,093 distinct payloads: defunct or unregistered
  bodies (`Farmers Home Administration`), generic offices (`Office of the
  Secretary`) and outright junk (`Rule`). The Federal Register API resolved
  no agency record for these. The affected documents **still count in their
  code's share denominator**, so a code whose documents assert nothing is
  honestly scored down rather than silently flattered — a test pins this.
- **`cfr_reference_missing_part` (5,225)** — chapter-level citations such as
  `{"title": 48, "chapter": 52, "part": null}`. Structurally partless, not
  malformed; a (title, part) table cannot represent them.
- Unparseable JSON in either column: **0 rows** in this pin. The handling is
  implemented and tested with synthetic rows.

## Verification performed

- 21 targeted tests green; every fixture synthetic.
- Two full real builds compared file-by-file: all six outputs byte-identical.
- Receipt scanned: 0 absolute paths, 0 timestamps.
- Artifact and receipt scanned for credential-shaped strings (API keys, AWS
  keys, tokens, private-key headers, `secret=`/`password=` pairs): 0 matches.
  This tool reads four parquet tables of public regulatory data and opens no
  network, environment or credential surface.

## What this does not settle

1. **161 unmapped codes.** 71 of them exist in the dockets table and are
   simply never reached: their dockets carry no `fr_docket_links` row into a
   document in the pin. A wider FR generation, or an ingestion pass that
   populates `documents.fr_doc_num` beyond its current 3,061 of 1,987,880
   rows, is what would move them — not a smarter join.
2. **The thin `documents` bridge.** It contributes 12 confident codes the
   dockets path never reaches, but on 2,710 supporting documents against the
   dockets path's 37,732. Treat documents-only codes as the weaker half.
3. **Tier thresholds are a pinned policy choice, not an external validation.**
   0.8/0.6 with floors of 5/2 and a 0.05 specificity margin were chosen
   against this pin's measured distributions and are named constants for
   exactly that reason. No external ground truth was consulted.
4. **Production admission** is unresolved for the same reasons as the
   date-event artifact: this is a local development input, gitignored, pinned
   by digest here.
