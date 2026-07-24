# Rulespec feedback — iteration 1

**Run date:** 2026-07-24
**Result:** PASS after two consumer-side corrections
**Publication:** local only; no R2 upload
**Rulespec contract:** `sha256:836968b28f3b86283f53c57ae5c9ab8ebd77e96531cd4751476f1a5ee3d296f2`

> **Status:** Historical iteration evidence. This run remains valid for the
> exact contract and snapshot above. The later
> [`RIN ontology revision report`](docs/rin-ontology-revision-report.md)
> supersedes its RIN/Proceeding interpretation: a RIN identifies a durable
> `RegulatoryAgendaItem`, never a Proceeding. The current local mapping authority
> is [`docs/ontology.md`](docs/ontology.md).

## Executive result

Spicy Regs remains compatible with the pinned Rulespec Level-0 contract. The
Rulespec mapping audit and complete Rulespec test suite passed before the data
run. The full ontology DAG then completed against 3,987,473 real source rows,
including a bounded live OpenAI generation and validation batch.

The corpus exposed two Spicy Regs defects:

1. Federal Register `docket_ids` were treated as Regulations.gov identifiers
   even when they were administrative labels, trade case numbers, or synthetic
   feed ids.
2. The bounded concept loop sorted all dockets before all documents, so a normal
   generation limit could starve Federal Register-backed document subjects.

Both defects are fixed and covered by regression tests. No new Rulespec term or
shape is required. The contract-level feedback is that lexical validity alone
cannot establish a registry identifier: a consumer also needs source-of-record
membership or successful registry resolution.

## Provenance and parity gate

| Item | Value |
| --- | --- |
| Spicy Regs starting commit | `b3c268c5408ab9b4c1dff97ed63d0bd57ca5c765` |
| Rulespec commit | `957662e151c194ac81dd248727e0a550d7f75c55` |
| Rulespec branch | `us-regulatory-identifiers` |
| Rulespec worktree | clean |
| Mapping declaration | `conformance/rulespec-l0.yaml` |
| Mapping result | 1/1 block passed; 13 mappings; 11 terms |
| Rulespec suite | 276 fixtures; 0 divergences |

Commands:

```sh
python ../rulespec/tools/l0_mapping_audit.py conformance/rulespec-l0.yaml

cd ../rulespec
make PYTHON='uv run --with-requirements requirements.txt --with jsonschema --with pyld python' test
```

The Rulespec suite covered the Rust and Python implementations, reference
corpus, positive and negative fixtures, vocabulary, L0–L4 coverage, 57
constraint-parity pairs, projector parity, version sync, code-generation drift,
and conformance reporting.

## Real-world source matrix

The full DAG used one immutable local snapshot for every stage.

| Source | Rows | Ontology evidence exercised |
| --- | ---: | --- |
| Regulations.gov dockets | 276,326 | docket identity, RIN, title, agency, stage |
| Regulations.gov documents | 1,987,880 | docket joins, additional RINs, FR joins, comment dates |
| Federal Register | 1,004,233 | FR identity, CFR, RIN, topics, stages, comment dates |
| Unified Agenda | 3,954 | CFR, RIN, U.S.C., Public Law, Statutes at Large, Executive Orders |
| Federal Register docket links | 715,080 | explicit FR-to-docket correlation |
| **Total** | **3,987,473** | |

The source files and their hashes are recorded in local snapshot
`snapshot_b40ccfbd464a1bb8ee7b6a2d6f2310df`.

## Finding 1: Federal Register `docket_ids` are heterogeneous

The Federal Register feed preserves the source's `docket_ids` array
losslessly. It is not a Regulations.gov-only field. Its 715,080 rows divide as
follows:

| Classification | Rows | Share |
| --- | ---: | ---: |
| Rulespec syntax and present in a Regulations.gov docket/document source | 47,534 | 6.6% |
| Rulespec syntax but absent from the Regulations.gov source corpus | 114,595 | 16.0% |
| Invalid for `rkaf:us-regsgov` | 552,951 | 77.3% |

Examples include `Sequence No. 1`, `Item I`, `AID_FRDOC_0001`, and
antidumping case number `A-570-831`. The last value illustrates why the broad
`[A-Z0-9]+(-[A-Z0-9]+)+` lexical form is necessary but not sufficient.

Before correction:

- `rule_targets` contained 334,991 rows; 261,763 had a docket value that did
  not even satisfy the Rulespec lexical form.
- `proceedings.docket_ids_json` contained 374,617 values; 85,745 were
  lexically invalid.
- Syntactically plausible non-Regulations.gov values could still join unrelated
  evidence into false multi-docket proceedings.

The correction:

- normalizes and validates Regulations.gov identifiers in one shared helper;
- trusts ids observed in the Regulations.gov docket or document source;
- admits Federal Register links only when they resolve into that trusted set;
- applies the boundary consistently in rule targets, proceedings, comment
  periods, and docket concept subjects;
- retains the raw `fr_docket_links` table unchanged for lossless inspection.

This is a consumer correction, not a request to narrow the Rulespec grammar.
Rulespec guidance should state that scheme syntax does not replace registry
provenance or resolution.

## Finding 2: bounded model batches starved document subjects

`build_subjects()` returns stable order by `(subject_type, subject_id)`.
Selecting `pending[:limit]` therefore chose only dockets until the entire docket
corpus was processed. Federal Register-backed document subjects could remain
untagged for hundreds of bounded runs.

The correction uses deterministic round-robin selection across subject types.
The live run proved an even six-docket/six-document subject batch. Ordering
within each type remains stable, and a depleted type no longer limits the other
type.

## Corrected full-DAG output

| Output | Rows | Result |
| --- | ---: | --- |
| `rule_targets` | 28,038 | all docket ids valid and trusted |
| `authority_edges` | 10,618 | all structured U.S.C./Public Law ids canonical |
| `proceedings` | 311,917 | 277,080 valid/trusted docket values; 88 multi-docket rows |
| `comment_periods` | 251,300 | zero inverted output intervals; non-empty evidence |
| `concepts` | 901 | 900 deterministic active seeds; 1 LLM candidate |
| `concept_assignments` | 24 | 12 distinct real subjects; complete LLM attestation |
| `concept_events` | 901 | append-only seed history |

### Rule-identity evidence

| Source path | Rows |
| --- | ---: |
| Docket RIN | 284 |
| Document RIN | 19 |
| Federal Register CFR | 16,467 |
| Regulations.gov document ↔ FR document | 10,241 |
| Unified Agenda CFR | 1,027 |

Across those rows, 26,993 carry a CFR reference, 18,125 carry a RIN, and
17,816 carry both.

### Authority forms

| Form | Exact parse | Partial parse | Total |
| --- | ---: | ---: | ---: |
| U.S.C. | 5,724 | 2,854 | 8,578 |
| Public Law | 350 | 406 | 756 |
| Executive Order | 174 | 38 | 212 |
| Statutes at Large | 37 | 132 | 169 |
| Retained raw/unsupported | 0 | 903 failed | 903 |

### Federal Register identifier forms

The current 1,004,233-row Federal Register source contains:

| Form | Rows |
| --- | ---: |
| Rulespec `YYYY-NNNNN` | 451,704 |
| Two-digit-year legacy | 395,498 |
| `E` legacy | 119,517 |
| Corrections | 3,282 |
| Other official legacy/correction forms | 34,232 |

Thus 552,529 rows, or 55.0%, still require the permanent Federal Register URL
fallback and must not claim `rkaf:us-frdoc`. Among documents attached to
proceedings, 60,693 distinct numbers use `rkaf:us-frdoc` syntax and 53,107 use
the fallback.

### Proceedings and comment periods

| Current stage | Proceedings |
| --- | ---: |
| Unknown | 205,276 |
| Final | 82,448 |
| Proposed | 15,938 |
| Withdrawn | 6,583 |
| Supplemental | 814 |
| Long-term | 779 |
| Prerule | 79 |

Comment-period evidence:

| Evidence source | Rows |
| --- | ---: |
| Regulations.gov document | 210,867 |
| Federal Register | 25,511 |
| Both | 14,922 |

Of 251,300 output intervals, 227,121 are attached to a unique docket and 24,179
remain proceeding-scoped.

## Live OpenAI validation

The API key was supplied only through a hidden interactive shell prompt. It was
never written to a file, command argument, checkpoint, manifest, or report. The
environment variable was unset and the shell closed after each run. A final
repository scan found no credential-like text.

Model: `gpt-5.6-luna`

### Pipeline calls

| Run | Generation calls | Validation calls | Result |
| --- | ---: | ---: | --- |
| Smoke: one docket + one document | 2 | 6 | 6 grounded assignments; all validations agreed |
| Full DAG, bounded balanced batch | 12 | 4 | 24 grounded assignments; all validations agreed |
| **Pipeline total** | **14** | **10** | **24 successful Responses API calls** |

The full DAG ran every deterministic stage over the complete source corpus.
Only the cost-bearing model loop was intentionally bounded to 12 real subjects
(six of each type), using `ONTOLOGY_GENERATION_LIMIT=12` and
`ONTOLOGY_VALIDATION_PERCENT=25`. Assignment confidence ranged from 0.72 to
0.99. The run reused 900 Federal Register topic seeds and proposed one candidate
concept, `Native American programs`.

### Billing-visible receipt

Because the pipeline does not yet persist provider receipts, one additional
isolated Responses API call was made:

| Field | Value |
| --- | --- |
| Response id | `resp_0c1fbaa844cdaff7006a62f05823e8819fb7aadae58699cdc4` |
| Status | `completed` |
| Model | `gpt-5.6-luna` |
| Output | `SPICY_REGS_OPENAI_CREDIT_TEST_OK` |
| Input tokens | 19 |
| Output tokens | 14 |
| Total tokens | 33 |

This makes 25 successful OpenAI calls in the exercise. The response id can be
matched against the project-scoped OpenAI usage logs. Dashboard usage can lag,
and a project-scoped key appears only under its owning project.

## Row-level conformance audit

The corrected outputs passed all of these full-corpus checks:

- every rule-target and proceeding docket id satisfies `rkaf:us-regsgov` syntax
  and belongs to the trusted Regulations.gov source set;
- every compact CFR, RIN, U.S.C., and Public Law value expands through the
  corresponding canonical helper;
- proceeding ids, comment-period ids, concept ids, assignment ids, and event
  ids are unique;
- every comment period references an existing proceeding, has `open <= close`,
  and carries a non-empty evidence list;
- every assignment references an existing concept, has confidence in `[0, 1]`,
  contains grounded evidence and subject digest, and records
  `method=llm`/`actor_id=openai:gpt-5.6-luna`;
- every manifest source and artifact SHA-256 matches the file on disk;
- the local snapshot was complete and no upload path ran.

## Remaining data-quality friction

The conservative identity policy leaves uncertain evidence unattached rather
than manufacturing joins:

- 70,189 Regulations.gov document observations lacked a unique proceeding
  component;
- 34,696 Federal Register observations had a RIN but no matching component;
- 1,379 no-RIN Federal Register observations lacked a unique docket component;
- 244 authority rows and 68 Unified Agenda rows used reused RINs;
- 6,993 candidate comment intervals were inverted and quarantined
  (6,764 document, 229 Federal Register);
- 15,075 Federal Register intervals and 1,148 document intervals had no unique
  proceeding target.

These are source-resolution queues, not Rulespec conformance failures.

One operational gap should be addressed in the next iteration: persist or emit
the OpenAI response id, model, and token counts for each generation/validation
call. Checkpoints prove completion but currently do not provide a
billing-reconciliation receipt.

## Verification

```sh
# Full local ontology DAG without model spend
ONTOLOGY_GENERATION_LIMIT=0 \
ONTOLOGY_DISCOVERY_LIMIT=0 \
uv run materialize-ontology \
  --output-dir output/rulespec-realworld-iteration-2 \
  --skip-upload --full-refresh --allow-bootstrap

# Full DAG plus bounded real OpenAI batch (key supplied only in the environment)
ONTOLOGY_GENERATION_LIMIT=12 \
ONTOLOGY_VALIDATION_PERCENT=25 \
ONTOLOGY_DISCOVERY_LIMIT=0 \
uv run materialize-ontology \
  --output-dir output/rulespec-realworld-iteration-3-openai \
  --skip-upload --full-refresh --allow-bootstrap

# Repository gate; blank the locally configured production R2 URL so tests
# use their hermetic fixtures instead of downloading the 27M-key manifest.
R2_PUBLIC_URL= uv run pytest -q
git ls-files -z '*.py' | xargs -0 uv run ruff check
uv run ty check src tests
```

Verification results:

- `518 passed, 3 deselected`;
- ontology-focused suite: `94 passed`;
- tracked Python lint: pass;
- `ty` over `src` and `tests`: pass;
- changed-file formatting: pass;
- `git diff --check`: pass.

The untracked `frontend/` and `play/` directories predate this iteration and
were not modified or included in the verification claim.
