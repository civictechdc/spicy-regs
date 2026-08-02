# Agency crosswalk artifact — built and pinned, 2026-08-02

**Status: built locally, digest-pinned, unpublished.** This is the artifact
that unblocks the CFR-part soft-priors experiment (spicysearch task 1c). Like
the date-event artifact it ships as a local, digest-pinned file surface —
downstream consumers verify by digest; nothing here requires a remote.

Tool: `tools/build_agency_crosswalk_artifact.py` (tested by
`tests/test_build_agency_crosswalk_artifact.py`, 32 tests; run targeted,
never the full suite). Artifacts live in `output/agency-crosswalk-2026-08-02/`
— gitignored output, pinned here by digest. Rebuilding from the pinned inputs
reproduces every file byte-for-byte (verified: a second build to a scratch
directory matched all six files).

> **Revision note.** The first build of this artifact (commit `2d00e97`)
> reported that 93.4% of `fr_docket_links` rows were "expected non-overlap".
> That was wrong: most of that population is the repo's own **confirmed
> upstream defect** (`docs/corpus-edge-coverage-findings-2026-07-24.md`
> finding #1 / RULE-010), and normalization recovers it. Every number below
> is from the corrected build.

## Identity

| Surface | Value |
|---|---|
| artifact_id | `urn:spicyregs:agency-crosswalk-artifact:80864133d2e5d484fef4afd0` |
| schema_version | `agency-crosswalk-artifact-v1` |
| tier_policy | `share-and-support-tiers-v1` |
| docket_normalization | `docket-id-normalization-v1` |
| `agency-crosswalk.parquet` | `sha256:daba53700ece7f489852368e9ebcfb67f2fbd9d2ed1ad602834c50ff7ddc8036` (914 rows) |
| `agency-codes.parquet` | `sha256:21df47a27d8db957c9c6102f158df3043d391a8f0fb74dba98bd026b9aed44f8` (316 rows) |
| `agency-parents.parquet` | `sha256:8eef8ed4e0659db21ed5f04e2e07e49f0eae1a7dde77cd1063285980af47aefe` (448 rows) |
| `cfr-part-agencies.parquet` | `sha256:3e4282558f82877850048754a71d789adeec0edef788ed55009743506327a2d6` (34,612 rows) |
| `quarantine.parquet` | `sha256:02f7be4c4dfcc7b3ec95fd1a0cab0b5b2c475f14ed8a7915673458d0e7d9b54c` (35,662 rows) |
| `receipt.json` | `sha256:5c7510d2a6249bb30f533b72c33524c59e42f65a74ddc1e4626d9b7eef4c800e` |

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

| Path | Route | Supporting documents |
|---|---|---:|
| `dockets_fr_links` | `dockets.agency_code` → `fr_docket_links` → `federal_register.agencies_json` | 124,938 |
| `documents_fr_doc_num` | `documents.agency_code` + `documents.fr_doc_num` → `federal_register.agencies_json` | 2,710 |

A test pins the prohibition directly: dockets whose ids all read `EPA-HQ-…`
but whose `agency_code` says `NRC` produce an `NRC` row and no `EPA` row.
This matters — the earlier naive cut derived codes from id prefixes and
produced **252 pseudo-codes**, more than the 196 agency codes that actually
exist in the dockets table.

### Docket-id normalization (the upstream defect, recovered)

`build_fr_docket_links.py` explodes `federal_register.docket_ids_json`
without normalizing the emitted `docket_id`, so the Federal Register's
decorated strings (`Docket No. FAA-2026-3485`, `Doc. No. AMS-SC-24-0046`)
never match the dockets spine, which keys on the bare id. This is
**`docs/corpus-edge-coverage-findings-2026-07-24.md` finding #1, confirmed,
triaged under RULE-010**, and the finding prescribes exactly what this tool
does: targeted normalization plus honest quarantine of the rest, never
force-matching.

`docket-id-normalization-v1`, pinned as constants in the receipt:

1. `strip_leading_docket_decorations` — pattern
   `^\s*(?:docket\s*(?:no|number)?|doc\.?\s*no)\.?[\s:]+`, applied repeatedly.
   The decoration **must** be followed by whitespace or a colon, so a real
   identifier like `DOC-2005-0010` (Commerce) cannot be truncated into a
   false match. A looser `\s*:?\s*` tail recovers 56 more rows (88,129) but
   buys latitude this artifact does not need.
2. `remove_internal_whitespace` — repairs splits inside real identifiers
   (`EPA- HQ-OAR-2007-0482`, `Docket No. CFPB-2 018-0001`).
3. `uppercase` — reconciles case-only divergence (`DoD-2006-OS-0005`).

**Ambiguity is refused, not guessed.** If a normalized key covered more than
one real docket the row would be quarantined as
`ambiguous_normalized_docket`. In this pin the normalized index over all
276,326 dockets produces **276,326 distinct keys — zero collisions** — so the
refusal path is exercised only by tests. It stays active for future pins.

### Link-row coverage (the two populations, counted apart)

| Population | Rows | Share |
|---|---:|---:|
| joined on the raw id | 47,338 | 6.6% |
| **joined after normalization** (the recovered defect) | **88,073** | **12.3%** |
| foreign identifiers (`FRL-*`, `REG-*`, `CMS-*-F`, `Special Conditions No. *`) | 579,669 | 81.1% |
| ambiguous normalized key (refused) | 0 | 0% |
| **total** | **715,080** | |

Normalization **tripled the dockets-path evidence: 37,732 → 124,938
supporting documents (3.31×)**. Only the third row is genuine expected
non-overlap; those are correctly not regulations.gov dockets and are counted
in the receipt's `coverage` block rather than quarantined.

## Tiers (pinned constants, in the receipt)

| Constant | Value |
|---|---|
| `CONFIDENT_SHARE` | 0.8 |
| `PROBABLE_SHARE` | 0.6 |
| `MIN_CONFIDENT_DOCUMENTS` | 5 |
| `MIN_PROBABLE_DOCUMENTS` | 2 |
| `SPECIFICITY_MARGIN` | 0.05 |

`share` is the fraction of a code's supporting FR documents whose
`agencies_json` names a given slug, and the tier scores **the primary slug's
share**, not the best share on offer. Support floors are load-bearing: a
perfect share over one document is not confidence.

**The specificity rule.** Sub-agency documents almost always name both the
department and the sub-agency, so several slugs legitimately reach share 1.0
and share alone cannot resolve the code. Candidates within
`SPECIFICITY_MARGIN` of the best share are treated as tied on evidence and
the deeper slug in the `parent_id` chain wins — `FAA` resolves to
`federal-aviation-administration`, `NHTSA` to its sub-agency even though
`transportation-department` out-polls it, while `DOT`, whose sub-agencies
fall outside the margin, correctly stays the department.

## Tier histogram (the headline numbers)

316 agency codes total — 196 from the dockets table, all of which also appear
in `documents`, plus 120 seen only in `documents`.

| Tier | dockets-path evidence | documents-bridge only | Total |
|---|---:|---:|---:|
| confident | 101 | 23 | **124** |
| probable | 16 | 13 | **29** |
| ambiguous | 5 | 18 | **23** |
| unmapped | — | — | **140** |
| **total** | **122** | **54** | **316** |

`agency-crosswalk.parquet` holds 914 candidate (code, slug) rows across the
176 codes that have any evidence.

Normalization moved the artifact substantially: **35 codes changed tier, 21
of them promoted out of `unmapped`** (TTB 0→370 documents and now confident,
FCIC 0→75 confident, ETA 0→48 confident, BSC 0→8 probable), and **31
`primary_slug` values changed**.

**Against the naive cut** (92 confident ≥60%, 160 ambiguous): the comparison
is not apples-to-apples and should not be quoted as one. The naive cut scored
252 prefix-derived pseudo-codes; this build scores 316 real codes from agency
fields. Restricting to the 196 codes in the dockets table: **135 reach share
≥ 0.6 with real support** (112 confident + 23 probable) against the naive 92,
11 are contested, and 50 remain unreached.

The `ambiguous` tier is entirely thin evidence, not conflict: **all 23
ambiguous codes rest on a single document**. Their candidate slug is probably
right — the tier says the artifact will not vouch for it.

Sample of the confident tier:

| Code | Primary slug | Share | Docs | Candidates |
|---|---|---:|---:|---:|
| FAA | federal-aviation-administration | 0.999056 | 25,417 | 10 |
| EPA | environmental-protection-agency | 0.999874 | 23,723 | 17 |
| FDA | food-and-drug-administration | 0.999187 | 13,529 | 7 |
| USCG | coast-guard | 0.991778 | 12,405 | 12 |
| NRC | nuclear-regulatory-commission | 0.999764 | 8,475 | 4 |
| FMCSA | federal-motor-carrier-safety-administration | 0.999271 | 4,113 | 5 |
| NHTSA | national-highway-traffic-safety-administration | 0.997184 | 2,841 | 7 |
| DOT | transportation-department | 0.995006 | 801 | 17 |
| TTB | alcohol-and-tobacco-tax-and-trade-bureau | 0.997297 | 370 | 2 |
| RHS | rural-housing-service | 0.992754 | 138 | 4 |

### Evidence strength is machine-readable, and membership is not evidence

`agency-codes.parquet` carries `dockets_path_documents`,
`documents_path_documents` and `evidence_is_documents_only`. This matters
because **`in_dockets_table` is a membership fact, not an evidence fact**: a
code can be registered in the dockets table yet draw all of its support from
the thin `documents` bridge.

**54 of the 176 evidenced codes draw 100% of their evidence from the
2,710-document bridge; 23 of those are `confident`, and 24 of them carry
`in_dockets_table=true`.** Filter on `evidence_is_documents_only`, not on
table membership. (Before normalization this population was 81 codes with 41
confident — recovering the decorated links moved 27 codes onto real
dockets-path evidence.)

### Where the specificity rule lands on the parent

31 primary slugs changed under the larger evidence base, and some now resolve
to the parent department. Two distinct causes, both honestly represented:

- **Thin evidence** — `WHD` (3 documents: `labor-department` 1.000,
  `wage-and-hour-division` 0.667) and `ERS` (2 documents) fall outside the
  margin, so the department wins. Both are tiered `probable`, not confident,
  and both candidates are in the artifact.
- **Genuinely multi-agency codes** — `FAR` (100 documents) has
  `defense-department`, `general-services-administration` and
  `national-aeronautics-and-space-administration` all at share 1.000 and all
  at depth 0. The Federal Acquisition Regulation really is jointly issued;
  there is no single owning agency to pick, and `candidate_count` says so.

`SPECIFICITY_MARGIN` was **not** widened to force `WHD` onto its
sub-agency. Tuning a pinned constant until a three-document code produces a
preferred answer would be fitting the policy to the anecdote; the tier and
the candidate list already carry the uncertainty.

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

- 3,797 of the 9,284 populated pairs (40.9%) are cited by exactly one agency;
  the widest pair is cited by 51.
- **8,957 of 9,284 populated pairs (96.5%) have a top agency at share ≥ 0.8** —
  the signal the soft-priors experiment is built on is strong.
- 36 of the 9,320 pairs produce no rows: every document citing them has no
  resolvable agency slug. 9,284 + 36 = 9,320, and the receipt counts both.

| CFR pair | Docs | Most-citing agency (share) | Agencies |
|---|---:|---|---:|
| 14 CFR 39 | 24,833 | transportation-department (0.999758) | 7 |
| 40 CFR 52 | 15,224 | environmental-protection-agency (0.998949) | 22 |
| 14 CFR 71 | 11,114 | transportation-department (0.998740) | 11 |
| 33 CFR 165 | 7,507 | coast-guard (0.969628) | 5 |
| 47 CFR 73 | 5,616 | federal-communications-commission (0.997685) | 2 |

**Read the flag's name literally.** The column is `is_most_citing`, not
`is_primary`, and its semantics are carried in the artifact itself
(`coverage_labels.cfr_primary_note`): *most-citing, then deepest*. Document
count decides rank 1; **where counts tie, the depth tie-break decides — and
it does so in 2,851 of the 9,284 populated pairs (30.7%)**, so it is not a
rare formality.

It is **not** the owning agency. 14 CFR 39 (airworthiness directives) ranks
`transportation-department` above `federal-aviation-administration` because
the department is named on marginally more documents, and the 0.056 gap is
wider than `SPECIFICITY_MARGIN`. This is a different question from the
crosswalk's specificity-resolved primary, and the tool deliberately does not
conflate them. A consumer wanting the responsible sub-agency should use the
full ranked list with `agency-parents.parquet`.

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
- Unparseable JSON and ambiguous normalized dockets: **0 rows** in this pin.
  Both are implemented and tested with synthetic rows.
- **Repeated defects stay distinct rows.** Identical facts (the same partless
  CFR reference listed twice on one document, the same dangling `fr_doc_num`
  on two rows) previously collided into one content-derived `quarantine_id`.
  Each row now carries an `occurrence` ordinal folded into its id, so
  `quarantine_id` is genuinely unique — 35,662 rows, 35,662 distinct ids.

## Verification performed

- 32 targeted tests green; every fixture synthetic.
- Two full real builds compared file-by-file: all six outputs byte-identical.
- **Mutation-checked**, each mutant killed by a named test: `CONFIDENT_SHARE`
  0.8→0.81, `PROBABLE_SHARE` 0.6→0.61, tiering the best share instead of the
  primary's, removing the CFR depth tie-break, removing the quarantine
  occurrence ordinal, removing the `uppercase` normalization rule, and
  resolving an ambiguous normalized key instead of refusing it.
- Receipt scanned: 0 absolute paths, 0 timestamps.
- Artifact and receipt scanned for credential-shaped strings: 0 matches. This
  tool reads four parquet tables of public regulatory data and opens no
  network, environment or credential surface.
- `ruff format` and `ruff check` clean on the tool and its tests.

## What this does not settle

1. **140 unmapped codes**, 50 of them registered in the dockets table. These
   are codes whose dockets carry no `fr_docket_links` row into any document
   in this pin — after normalization has already recovered everything the
   documented defect was hiding. What would move them is a wider FR
   generation, or an ingestion pass that populates `documents.fr_doc_num`
   beyond its current 3,061 of 1,987,880 rows.
2. **The `documents` bridge is still thin.** 2,710 supporting documents
   against the dockets path's 124,938, yet 54 codes rest on it entirely.
   `evidence_is_documents_only` marks them; treat those tiers as weaker than
   the label alone suggests.
3. **The foreign-identifier residue is unaudited in bulk.** 579,669 link rows
   carry identifiers that are correctly not regulations.gov dockets. Finding
   #1 warns that the same unnormalized-identifier pattern likely affects
   other cross-references, so a sample audit of that residue could still find
   a second recoverable class. This artifact does not claim otherwise.
4. **Tier thresholds are a pinned policy choice, not an external validation.**
   0.8/0.6 with floors of 5/2 and a 0.05 specificity margin were chosen
   against this pin's measured distributions and are named constants for
   exactly that reason. No external ground truth was consulted.
5. **Production admission** is unresolved for the same reasons as the
   date-event artifact: this is a local development input, gitignored, pinned
   by digest here.
