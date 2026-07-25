# Mixed real-data ontology corpus and OpenAI validation

- **Run date:** 2026-07-24
- **Status:** Pass, with a marginal model-quality result
- **Publication:** Local branch only; no upload, release, or deployment
- **Dataset:** `mixed_snapshot_c9c7dbdf2ddf6d2527d810d5`
- **OpenAI run:** `mixed-real-data-openai-v2`
- **Ontology snapshot:** `snapshot_44fe0deb47ffb7766d0670dd603c9d73`

> **Scope:** Historical pre-segmentation baseline. Its corpus and provider
> receipts remain valid for the named snapshots, but this pass does not satisfy
> the document-only segmentation goal or its final model-comparison gates.

## Result

This run creates a 147 MiB, 708,367-record ontology stress corpus from 18
record-bearing source, aggregate, and relationship tables. The bound inputs
contain 6,168,517 real public records. Each sampled table retains its native
schema, while `records.parquet` supplies a small, source-neutral envelope for
cross-source tests.

The corpus includes 309,210 explicit pair expectations:

| Label | Rows | Meaning |
| --- | ---: | --- |
| `related` | 151,258 | A direct source-issued key or relationship supports the pair |
| `no_declared_relation` | 152,952 | No source-issued join connects the pair in this bound snapshot |
| `unknown` | 5,000 | A lexical signal exists, but no source-issued crosswalk resolves it |

`no_declared_relation` is a negative test control, not a universal claim about
the real world. An exact title match without an identifier is `unknown`, not
`related`. This distinction lets the corpus test conservative relationship
inference without pretending that missing evidence proves non-relation.

The machine-readable [corpus receipt](evidence/mixed-real-data-corpus-2026-07-24/corpus-receipt.json)
passes with zero dangling endpoints, zero positive/negative pair overlap, all
18 sources present, and all three labels populated. Its SHA-256 is
`e8ad894b459db79ed49e6b65633be77a1aed36e1470e1ca4bf730f5986673534`.

## Source coverage

| Source table | Public source family | Sample rows |
| --- | --- | ---: |
| `dockets` | regulations.gov | 50,847 |
| `documents` | regulations.gov | 102,078 |
| `comments` | regulations.gov | 50,000 |
| `comments_index` | Spicy Regs aggregate | 30,000 |
| `federal_register` | FederalRegister.gov | 80,000 |
| `unified_agenda` | Reginfo.gov | 3,954 |
| `fr_docket_links` | Spicy Regs relationship record | 80,000 |
| `cfr_sections` | GovInfo.gov | 40,000 |
| `congress_bills` | Congress.gov | 40,000 |
| `sam_entities` | SAM.gov | 50,000 |
| `lobbying_filings` | Senate LDA | 40,000 |
| `fec_committees` | FEC.gov | 30,000 |
| `gao_reports` | GAO.gov | 47 |
| `crs_reports` | CRS reports | 13,978 |
| `court_dockets` | CourtListener | 7,623 |
| `usaspending_recipients` | USAspending.gov | 50,000 |
| `fcc_proceedings` | FCC ECFS | 21,054 |
| `fcc_filings` | FCC ECFS | 18,786 |

The five regulatory inputs use the locally bound RIN-corpus snapshot because it
contains the required `federal_register.topics_json` gold labels. The other
sources come from the public Spicy Regs R2 objects. The comment sample uses the
public ACF and USCG partitions instead of downloading the 2.5 GiB monolithic
comments object.

## Relationship coverage

Positive labels use direct keys or relationship records:

| Relationship | Rows |
| --- | ---: |
| Comment in regulations.gov docket | 49,287 |
| Comment-index partition describes docket | 5,866 |
| Docket reports an agenda item | 113 |
| Regulations.gov document in docket | 18,023 |
| FCC filing in FCC proceeding | 18,899 |
| Federal Register record links docket | 59 |
| Regulations.gov document has Federal Register format | 12 |
| CFR fragments share a GovInfo package | 39,777 |
| Lobbying filings share a source-issued client ID | 16,734 |
| SAM and USAspending records share a UEI | 2,488 |

The corpus deliberately distinguishes three ontological layers:

1. a source-scoped record identity;
2. the subject or artifact represented by that record;
3. an evidenced relationship claim between records or subjects.

This structure applies to documents, organizations, proceedings, legislation,
court cases, and document fragments. Regulatory concepts such as RIN remain a
profile on top: a RIN identifies a durable agenda item, while evidence links
that item to one or more independently identified Proceedings.

## Real OpenAI pipeline validation

The final run used the production OpenAI Responses path, not a mock. It
processed 48 regulatory subjects: 24 dockets and 24 Federal Register-backed
regulations.gov documents. The run made 175 evidenced structured calls:
48 generation calls and 127 independent validation calls.

| Check | Result |
| --- | ---: |
| Model | `openai:gpt-5.6-luna` |
| Generated, field-grounded assignments | 127 |
| Candidate concepts proposed | 5 |
| Subjects with no assignment | 0 |
| Validation coverage | 127 of 127 (100%) |
| Validator agreements | 117 |
| Validator disagreements | 10 |
| Persisted grounding failures | 0 |
| Append-only assignment rows | 137 |
| Current assignment rows | 127 |

The [OpenAI run receipt](evidence/mixed-real-data-corpus-2026-07-24/openai-run-receipt.json)
and the underlying [ontology receipt](evidence/mixed-real-data-corpus-2026-07-24/ontology-receipt.json)
both pass. Their SHA-256 values are
`af71a685a922e2c50ebd6445f77e4297c42cedf175420efa588d32a71eee4d4a`
and
`85482b6a65316e0ec1fda210b7fefb6797885f957b1a6b0f5a246ae49dcdd76a`.
The audit found no OpenAI key prefix in the run artifacts.

### Model-quality gate

Federal Register `topics_json` supplies an exact-label evaluation set for the
24 selected documents.

| Metric | Result |
| --- | ---: |
| True positives | 44 |
| False positives | 21 |
| False negatives | 63 |
| Precision | 0.6769 |
| Recall | 0.4112 |
| F1 | 0.5116 |
| Required F1 | 0.5000 |

The run passes the declared F1 floor, but only narrowly. Low recall makes this
a pipeline validation and baseline, not evidence that the present tagger has
production-grade semantic coverage.

The current OpenAI materializer supports regulatory docket and document
subjects. The 18-source deterministic corpus tests broader identity and
relationship semantics, but this run does not claim model coverage for GAO,
CRS, court, FCC, organization, lobbying, campaign-finance, legislative, or CFR
adapters. Those need source profiles and gold labels before a whole-corpus model
score would be meaningful.

## Defect found and repaired

The first real-model candidate run produced 129 assignments and an F1 of
0.5795, but its strict audit failed. One assignment quoted text that existed in
the combined subject while attributing it to the wrong source field. The prior
guard checked the combined text and only checked that the named field existed.

The OpenAI provider now requires each quoted span to occur in the exact field
named by the model. A regression test covers this case. The
[failed-run receipt](evidence/mixed-real-data-corpus-2026-07-24/openai-grounding-failure-receipt.json)
records the original grounding failure; its SHA-256 is
`c5e1c6376b9b6c846e3dbba90165d768d2e4559c3fc8ce01a6a7813205995e18`.
The corrected fresh run has zero persisted grounding failures.

## Determinism and artifact layout

The builder orders each source by a seeded hash of its source-scoped primary
key, adds only explicit closure records, and writes:

- 18 native-schema sample Parquets;
- `records.parquet`, the ontology-neutral record envelope;
- `relationship_expectations.parquet`, the three-label pair set;
- `record_membership.parquet`, each record's test role;
- `corpus-manifest.json` and `corpus-receipt.json`;
- an immutable five-file `openai-eval-inputs/` slice.

A clean rerun produced the same dataset ID and byte-identical SHA-256 values for
all 26 Parquet artifacts. Model output lives in a separate run directory, so an
OpenAI run cannot mutate the corpus identity.

## Reproduction

Build and revalidate the real-data corpus:

```console
uv run build-mixed-real-data-corpus build \
  output/mixed-real-data-corpus-v2 \
  --regulatory-source-dir output/rin-ontology-revision-candidate

uv run build-mixed-real-data-corpus validate \
  output/mixed-real-data-corpus-v2
```

Run the production OpenAI path in a separate directory:

```console
mkdir -p output/mixed-real-data-openai-run-v2
cp output/mixed-real-data-corpus-v2/openai-eval-inputs/*.parquet \
  output/mixed-real-data-openai-run-v2/

set -a
source .env
set +a

ONTOLOGY_RUN_ID='mixed-real-data-openai-v2' \
ONTOLOGY_GENERATION_LIMIT='48' \
ONTOLOGY_VALIDATION_PERCENT='100' \
ONTOLOGY_DISCOVERY_LIMIT='0' \
uv run materialize-ontology \
  --output-dir output/mixed-real-data-openai-run-v2 \
  --skip-upload --full-refresh

uv run build-mixed-real-data-corpus openai-receipt \
  output/mixed-real-data-openai-run-v2 \
  --minimum-f1 0.50 \
  --output output/mixed-real-data-openai-run-v2/openai-run-receipt.json
```

The run requires `OPENAI_API_KEY` at process time. Receipts and model artifacts
must never contain the key.
