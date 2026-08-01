# Initial discovery slice — the two deterministic questions

- **Date:** 2026-07-28
- **Track:** MVP plan track B (`rulespec-testbed-path-forward.md`, "Current
  execution order")
- **Status:** run and scored. Question 1 passes; question 2 fails on recall,
  cause identified.
- **Model dependency:** none. Every number here is deterministic and
  reproducible offline from the pinned snapshot.

> **Addendum, 2026-08-01 — question 2 re-scored: end-to-end recall 0.8125 →
> 1.000.** The range defect diagnosed below was fixed by `0378a9a`
> ("publish U.S.C. section ranges as endpoints") three and a half hours after
> this document was written, and the harness was never re-run. It has now been
> re-run at HEAD against these same pinned snapshot bytes, with
> `authority_edges` (11,793 rows, SHA `f9bd79e0da25323c`) rebuilt from the same
> `unified_agenda.parquet` (3,954 rows, SHA `e6862d5d6a5300f1`).
> Authority-leg recall 0.923 → **1.000**
> (65/65 RINs), end-to-end link recall 0.8125 → **1.000** (16/16 proceedings),
> all three aggregate counts now match, and the harness exits 0. Precision was
> and remains 1.000, no forbidden near-miss is admitted, and no stage-unknown
> proceeding leaks in. The frozen expectation did not move at all — it is
> derived from raw authority text, not from the parser — so only the system
> side changed. The system query is unchanged too: a range now carries `7401`
> in `usc_section`, so the existing exact filter finds it. New record:
> [`question-2-usc-42-7401-rescore-2026-08-01.json`](discovery-slice-2026-07-28/question-2-usc-42-7401-rescore-2026-08-01.json).
> Everything below is left exactly as it was written on 2026-07-28; the
> question-2 scores, verdicts, and cleanup-track item in it are superseded by
> that record.

Reproduce the re-score:

```bash
mkdir -p output/discovery-slice-rescore-2026-08-01
cp output/rin-ontology-revision-candidate/unified_agenda.parquet \
   output/discovery-slice-rescore-2026-08-01/
python -c "from pathlib import Path; \
  from spicy_regs.transforms.build_authority_edges import build_authority_edges; \
  build_authority_edges(Path('output/discovery-slice-rescore-2026-08-01'), \
    run_id='authority-edges-rescore-2026-08-01', asserted_at='2026-08-01T00:00:00Z')"
python tools/discovery_question_usc7401.py \
  --snapshot output/rin-ontology-revision-candidate \
  --authority-edges output/discovery-slice-rescore-2026-08-01/authority_edges.parquet \
  --out docs/evidence/discovery-slice-2026-07-28/question-2-usc-42-7401-rescore-2026-08-01.json
```

The pinned `run_id` and `asserted_at` are what make the rebuilt table
byte-reproducible; without them the provenance columns move and the digest
changes while the scores do not.

## What was run

Two product-level experiments, each with an expectation derived **independently
of the system under test**. Neither expectation imports
`spicy_regs.ontology.citations` or any transform: the matchers are written in
the experiment scripts from the source formats and the table documentation, so
a parser defect appears as a score rather than as the system agreeing with
itself.

| Artifact | Path |
| --- | --- |
| Scoring primitives | `tools/discovery_scoring.py` |
| Question 1 harness | `tools/discovery_question_cfr60.py` |
| Question 2 harness | `tools/discovery_question_usc7401.py` |
| Hermetic tests | `tests/test_discovery_slice.py` (45 tests, no network, no snapshot) |
| Frozen records | `docs/evidence/discovery-slice-2026-07-28/*.json` |

Reproduce:

```bash
python tools/discovery_question_cfr60.py \
  --snapshot output/rin-ontology-revision-candidate \
  --out docs/evidence/discovery-slice-2026-07-28/question-1-cfr-40-60.json
python tools/discovery_question_usc7401.py \
  --snapshot output/rin-ontology-revision-candidate \
  --out docs/evidence/discovery-slice-2026-07-28/question-2-usc-42-7401.json
```

Each harness exits non-zero when any of its three scores falls short, so both
are usable as checks and not only as reports.

## Frozen snapshot identity

Ontology generation `snapshot_0e4b4204bdfbd462a9270fcd766fb8dd`, asserted
2026-07-24T13:32:50Z, materialized at `output/rin-ontology-revision-candidate`.
Every file below is pinned by SHA-256 inside both JSON records.

| File | Rows | SHA-256 (first 16) |
| --- | ---: | --- |
| `dockets.parquet` | 276,326 | `b14cd488b7898391` |
| `documents.parquet` | 1,987,880 | `52f085f9ec2ee0c0` |
| `federal_register.parquet` | 1,004,233 | `ac18315faa8be4a8` |
| `fr_docket_links.parquet` | 715,080 | `b3409f0ada792a8c` |
| `unified_agenda.parquet` | 3,954 | `e6862d5d6a5300f1` |
| `rule_targets.parquet` | 39,516 | `5cec12fb8a7fa1dc` |
| `authority_edges.parquet` | 10,618 | `0bc929b6e0b120b5` |
| `agenda_item_proceedings.parquet` | 120,685 | `e3dea44081313dde` |
| `proceedings.parquet` | 511,643 | `e49cb37ac2a97465` |

**Snapshot age is itself a finding.** The published ontology tables were built
2026-07-24; the phase-3 authority repairs landed 2026-07-27 (`538780c` U.S.C.
section lists, `91db195` `statute_at_large`/`executive_order` columns). The
published `authority_edges` therefore has neither fix — it still lacks the two
citation columns while emitting `eo` and `statute_at_large` in
`authority_type`, so 381 rows announce a citation kind whose value was
dropped. Question 2 was consequently scored twice: once against the published
table, and once against a table rebuilt at HEAD from the *same* frozen
`unified_agenda.parquet` (10,618 → 11,793 rows, SHA `1cb2b72a68af7758`,
record `question-2-usc-42-7401-authority-edges-head.json`). The two scores are
identical; see the diagnosis below.

## Recorded definition of "active rulemaking"

Appended to `docs/decisions.md` as **2026-07-28 — Active rulemaking: an
evidenced non-terminal proceeding stage**:

> `proceedings.current_stage IN ('prerule','proposed','supplemental','longterm')`

with a null stage reported as `stage_unknown` and never counted as active.

The measurements behind that choice, and behind rejecting the two obvious
alternatives, are in the ledger entry. The one worth repeating here: the
allowlist and `NOT IN ('final','withdrawn')` return the same 118,539
proceedings, but only because SQL drops NULLs from a `NOT IN`. Read as
English, the negation returns 334,487 — it would silently admit all 215,948
stage-unknown proceedings, 42.2% of the table. The two formulations agree
today by accident, not by contract.

---

# Question 1 — every docket touching `40 CFR 60`

**Verdict: PASS.** Exact set match, exact filter, matching aggregates.

## Charter answers

1. **Decision the result changes.** Whether the deterministic identity spine
   is good enough to serve regulation-targeting discovery now, or whether
   phase-3-style repairs must precede it.
2. **In / out.** In: the raw snapshot (`federal_register`, `fr_docket_links`,
   `documents`, `dockets`) and the published `rule_targets`. Out: a docket-id
   set, three scores, and a frozen expectation.
3. **Step under test.** `build_rule_targets` — CFR citation normalization and
   the FR-document-to-docket join.
4. **Why that step is a likely constraint.** It is the only step between a
   user's CFR filter and an answer; the strategy names it as the main path.
5. **Simplest credible baseline.** A direct scan of the raw sources, which is
   what the expectation is.
6. **Single variable.** None changed — this is a correctness measurement of
   the current build, not a comparison.
7. **Matching measure.** Link precision and recall (relationships), row-level
   predicate exactness (filter), distinct-id counts (aggregate) — reported
   separately, per the strategy's capability table.
8. **Adopt / reject / investigate.** Exact match ⇒ the path is usable for
   discovery. Any miss or near-miss admission ⇒ investigate before serving.
9. **Can this dataset support the decision?** Yes. The question is
   deterministic, the expectation is derivable from the same frozen bytes, and
   no model or holdout is involved.
10. **Confirming a component gain improves the whole result.** Not applicable:
    this *is* the complete user result. No component substitution was made.
11. **Pinned digests, metric version, ledger.** Six source/system digests in
    the JSON record; metrics `link/set-v1`, `filter/row-predicate-v1`,
    `aggregate/declared-level-v1` as implemented in
    `tools/discovery_scoring.py`; ledger entry for the active definition is
    question 2's, not this one's.

## Query and intended meaning

Return every regulations.gov docket for which **action evidence** places a
Federal Register document citing 40 CFR part 60 in that docket. "Touching part
60" means the reference's decomposed components are title `40` and part `60` —
every section under the part counts, and part 600 never does.

System query:

```sql
SELECT DISTINCT docket_id FROM rule_targets
WHERE cfr_title = '40' AND cfr_part = '60'
```

## Frozen expectation

| Set | Size | Derivation |
| --- | ---: | --- |
| Expected dockets | **135** | 756 FR documents carry a `{title: 40, part: "60"}` reference; joined to trusted dockets through `fr_docket_links` (135) and `documents.fr_doc_num` (3, a subset) |
| Forbidden dockets | **31** | 20 reach only `40 CFR 600`, 1 only `40 CFR 601`, 10 only `10 CFR 60` |
| Ambiguous dockets | **49** | reachable only through a title-40 reference whose `part` component is null (191 FR documents); unresolvable either way, so neither credited nor penalised |
| Expected aggregates | — | 135 distinct dockets; 135 via `fr_cfr_ref`; 3 via `document_fr_doc` |

Trusted dockets (the universe): 277,318, from `dockets` ∪ `documents`.

## Result

| Measure | Value |
| --- | --- |
| Link precision | **1.000** (135/135) |
| Link recall | **1.000** (135/135) |
| Missing / extra | 0 / 0 |
| Forbidden admitted | **0** of 31 |
| Ambiguous admitted | 0 of 49 |
| Filter exactness | **1.000** (224/224 rows carry title 40 / part 60) |
| Unknown-value admissions | **0** (1,120 CFR-null rows in the table; none entered) |
| Aggregates | match on all three names |

Returned rows split 221 `fr_cfr_ref` / 3 `document_fr_doc`, matching the two
independent evidence paths.

## Capability verdicts

| Capability | Verdict | Basis |
| --- | --- | --- |
| Identity | **Pass** | 135 docket ids matched exactly under an independently written normalizer; no unresolved ids |
| Relationships | **Pass** | precision = recall = 1.0; both evidence paths reproduced; no fan-out (224 rows over 135 dockets is the declared source/RIN key, not multiplication) |
| Filter | **Pass** | every returned row satisfies the predicate; no unknown-value leakage |
| Aggregate | **Pass at the declared level**, **limited below it** — see below |
| Explain | **Partial** — see below |
| Operate | **Pass** | frozen digests, deterministic rerun, non-zero exit on failure |

## Two findings that are not failures

**The prefix trap is real.** Three readings a user might plausibly write:

| Formulation | Distinct dockets |
| --- | ---: |
| `cfr_title = '40' AND cfr_part = '60'` | 135 |
| `cfr_ref = '40-60'` | 135 |
| `cfr_ref LIKE '40-60%'` | **156** |

The prefix form admits all 21 part-600/601 dockets. It agrees with the correct
form on nothing but this snapshot's absence of section-level rows — every
`cfr_section` in `rule_targets` is null, because Federal Register metadata is
part-level. The moment a section-level source arrives, `cfr_ref = '40-60'`
also breaks. Only the component form is safe in both directions, which is why
the harness uses it and records the other two.

**Evidence folding limits aggregation below the docket level.** 756 FR
documents cite 40 CFR 60; 409 of them link to a trusted docket; only 196
distinct `evidence_id` values survive in `rule_targets`. The table folds
repeated evidence for one logical edge into a date span and keeps a single
`evidence_id` — by design, and documented. The consequence is worth stating
plainly: **`rule_targets` cannot answer "how many Federal Register documents
evidence this docket's 40 CFR 60 target."** Aggregation is trustworthy at the
docket level and unavailable at the evidence level. This is recorded as a
declared limit, not scored as a miss.

That limit is also what caps the *Explain* capability: a user can see one
justifying document per edge, not the set, so "why did this docket appear" is
answerable but "on what basis, in full" is not.

## Declared recall boundary, measured

Unified Agenda CFR references are deliberately never projected through RIN
equality into `rule_targets`. Cost of that boundary on this snapshot:

- 20 agenda RINs name 40 CFR 60 in `cfr_references_json`;
- projecting them through RIN equality reaches 10 dockets;
- **all 10 are already in the expected 135** via action evidence.

So the boundary is declared and, on this snapshot, **costs nothing**. It is a
real limit on a snapshot with more agenda-only RINs; it is not one here. The
number is frozen in the record so a future refresh can show it moving.

---

# Question 2 — every active rulemaking depending on `42 U.S.C. 7401`

**Verdict: FAIL on recall (0.8125). Precision holds at 1.000. Cause
identified; it is a parser gap, not a join defect.**

## Charter answers

1. **Decision the result changes.** Whether the authority path can serve
   statute-driven discovery, and whether the phase-3 authority repairs are
   finished.
2. **In / out.** In: raw `unified_agenda.legal_authority_json`, raw RIN
   evidence from `dockets`/`documents`/`federal_register`, and the published
   `authority_edges` → `agenda_item_proceedings` → `proceedings` join. Out: a
   proceeding-id set, five scores, a fan-out report, and a frozen expectation.
3. **Step under test.** `parse_authority_citation` and the corrected
   three-table join, under the newly recorded active-state definition.
4. **Why that step is a likely constraint.** The MVP plan already lists U.S.C.
   section lists as a known data defect; the authority leg is the only
   model-free path from a statute to a rulemaking.
5. **Simplest credible baseline.** A regex scan of the raw agenda authority
   strings, which is what the expectation is.
6. **Single variable.** One deliberate change, scored twice: the published
   `authority_edges` versus the same table rebuilt at HEAD (post-`538780c`,
   post-`91db195`) from identical input bytes.
7. **Matching measure.** Authority-leg link precision/recall, proceeding-leg
   link precision/recall, end-to-end link precision/recall, filter exactness,
   aggregate counts — five, kept apart.
8. **Adopt / reject / investigate.** Recall below 1.0 with an identifiable
   parse cause ⇒ record the defect and route it to the cleanup track, not to a
   model.
9. **Can this dataset support the decision?** Yes for the authority leg. Not
   for proceeding *identity*: re-deriving 511,643 proceedings independently is
   a separate experiment, so that leg is held constant and checked only at RIN
   link level.
10. **Confirming a component gain improves the whole result.** Directly
    measurable here: the authority-leg score and the end-to-end score are both
    computed, so a parser fix can be shown to move (or not move) the user
    answer. It did not move it — see the diagnosis.
11. **Pinned digests, metric version, ledger.** Eight digests in the JSON
    record including the scored `authority_edges`; same metric versions as
    question 1; ledger entry `docs/decisions.md`, 2026-07-28 — Active
    rulemaking.

## Query and intended meaning

Return every **active** rulemaking whose agenda entry cites 42 U.S.C. 7401.
"Rulemaking" is a proceeding, not a RIN. "Cites 7401" means the raw authority
string puts the section's own digits under U.S. Code title 42 — including
`et seq.` forms and ranges whose first endpoint is 7401. "Active" is the
recorded definition.

System query:

```sql
SELECT DISTINCT p.proceeding_id
FROM authority_edges e
JOIN agenda_item_proceedings a ON a.rin = e.rin
JOIN proceedings p ON p.proceeding_id = a.proceeding_id
WHERE e.usc_title = '42' AND e.usc_section = '7401'
  AND p.current_stage IN ('prerule','proposed','supplemental','longterm')
```

## Frozen expectation

| Set | Size | Derivation |
| --- | ---: | --- |
| RINs naming 42 U.S.C. 7401 | **65** | independent scan of 10,432 raw authority strings across 3,954 agenda items; 14 distinct citation spellings |
| RINs whose range *spans* 7401 without naming it | 0 | none exist in this snapshot; the class is implemented and would be ambiguous |
| Near-miss RINs (other title-42 CAA sections, never 7401) | 25 | e.g. `42 U.S.C. 7411`, `7412`, `7414` |
| Proceedings tracked by the 65 | **35** | `final` 13, `proposed` 16, `withdrawn` 1, stage-unknown 5 |
| **Expected active proceedings** | **16** | the 16 at `proposed` |
| Forbidden active proceedings | **10** | active proceedings reachable only from the 25 near-miss RINs |
| Ambiguous active proceedings | 0 | — |

The 14 spellings that name 7401 include `42 U.S.C. 7401`,
`42 U.S.C. 7401 et seq. Clean Air Act`, `42 U.S.C. 7401 et. seq CAA`,
`42 U.S.C. 7401 to 7671q.`, and `42 U.S.C. 7401-7671q.` — the last two are
where the failure lives.

## Result

| Measure | Published `authority_edges` | Rebuilt at HEAD |
| --- | --- | --- |
| Authority-leg precision / recall | 1.000 / **0.923** (60/65) | 1.000 / **0.923** (60/65) |
| Proceeding-leg precision / recall | 1.000 / 1.000 (34/34) | 1.000 / 1.000 |
| **End-to-end link precision / recall** | 1.000 / **0.8125** (13/16) | 1.000 / **0.8125** |
| Forbidden admitted | 0 of 10 | 0 of 10 |
| Filter exactness | 1.000 (19/19 rows) | 1.000 |
| Unknown-value admissions | 0 (5 stage-unknown reachable) | 0 |
| Aggregates | 3 mismatches (65→60 RINs, 35→32 proceedings, 16→13 active) | identical |

Missing proceedings: `proceeding_0d7373ebe1771d36bc5b61f7`,
`proceeding_b5bdbd383f137cbc8458db45`, `proceeding_e4562534361c466f01dc0c27`
— all at stage `proposed`, so all three are answers a user asked for and did
not get.

*Superseded 2026-08-01: all three are returned at HEAD, and every score in
this table reaches 1.000. See the addendum at the top.*

## Diagnosis of the failure

The five missing RINs — `2060-AS32`, `2060-AU01`, `2060-AV95`, `2060-AW70`,
`2060-AW96` — all state their authority as a **range**:

```
42 U.S.C. 7401-7671q.
```

`parse_authority_citation` reads the hyphenated tail as part of the section
token and emits `usc_section = '7401-7671q'`, a single opaque string. An exact
filter on `usc_section = '7401'` cannot see it. This is not the U.S.C.
section-list defect: `538780c` expanded comma-separated lists
(`42 U.S.C. 1395, 1396, 1397`) and explicitly left ranges with their existing
single-citation reading. Rebuilding `authority_edges` at HEAD confirms it —
the table grows 10,618 → 11,793 rows, and the 7401 answer does not move at
all (60 RINs, 71 rows, both builds). **The repair that landed does not touch
this question.**

The Clean Air Act range `7401-7671q` is the whole Act, so these are exactly
the rulemakings most obviously dependent on 7401. A user asking the question
gets 13 of 16 with no signal that three are missing.

`42 U.S.C. 7401 to 7671q` (the `to` spelling, 9 occurrences) parses correctly
by accident: the standard expression stops at `7401` and the tail is ignored,
which yields the right answer for the wrong reason. Any tightening of that
expression would silently convert those RINs into the same failure.

## Fan-out behaviour

Reported explicitly, because the question is answered in proceedings and one
RIN can track many:

- 34 expected RINs carry a proceeding link; they track 35 proceedings.
- **Multiplication ratio 1.029.** Maximum fan-out is **2**, for three RINs:
  `2060-AO18`, `2060-AS13`, `2060-AW43`.
- The known hazard is present in the table but absent from this answer:
  `1625-AA00` tracks **4,344** proceedings and `2120-AA64` tracks **23,281**.
  Neither cites 42 U.S.C. 7401.

So this question is fan-out-safe, and that is a property of the question, not
of a guard. A statute cited by a Coast Guard or FAA blanket RIN would return
thousands of proceedings from one authority row, and nothing in the current
path would flag it. The harness now measures and records the ratio on every
run, which is the cheap version of a guard.

## Declared recall boundary, measured

`agenda_item_proceedings` rows exist only where a docket, regulations.gov
document, or Federal Register artifact **directly reports the RIN**; agenda
equality cannot manufacture a link. Cost on this snapshot: **31 of the 65
expected RINs have no raw action evidence at all** and therefore cannot yield
a rulemaking row. That is a large boundary — 48% of the matching agenda items
— and it is correct: those are planned actions with nothing published yet.
The remaining 34 RINs are linked with perfect fidelity (34/34, no missing, no
extra), which is the strongest single result in this slice.

## Capability verdicts

| Capability | Verdict | Basis |
| --- | --- | --- |
| Identity | **Pass** | 65/65 RINs normalized and matched; 35 proceeding ids resolved without ambiguity |
| Relationships | **Fail on the authority leg** (recall 0.923), **pass on the proceeding leg** (1.000) | range citations never produce a 7401 edge |
| Filter | **Pass** | every returned proceeding carries an active stage; none of the 5 stage-unknown reachable proceedings leaked in |
| Aggregate | **Fail** | all three frozen counts short by the same cause |
| Explain | **Pass** | every returned row carries `agenda_item_id`, `source`, and `evidence_id` back to a named artifact |
| Operate | **Pass** | scored twice against two pinned builds of the same table from identical input bytes |

---

# What this slice establishes, and what it does not

**Establishes.** The deterministic spine answers a CFR-targeting question
exactly, on real data, with near-misses excluded and counts matching at the
declared level of detail. The proceeding-link leg is exact. An active-state
definition now exists, is recorded, and was chosen from measurements rather
than from taste.

**Does not establish.** Nothing about tagging, retrieval, or any model —
question 3 (PFAS) remains blocked on MVP phase 4. Nothing about proceeding
*identity* correctness: that leg was held constant, and a separate graph
experiment should re-derive it. Nothing about behaviour under a source
refresh: both questions ran against a single frozen generation, so link
stability is untested.

**Sends to the cleanup track.** One defect, one shape:
`parse_authority_citation` must expand U.S.C. ranges the way it now expands
lists, or the query layer must stop assuming `usc_section` is a scalar. Either
fix is testable offline — `tests/test_discovery_slice.py` already encodes the
failing shape on a synthetic snapshot, so the repair has a test waiting for it
before any real data is touched. A second, smaller item: the published
ontology generation predates both phase-3 authority repairs and should be
rematerialized before anyone reads `authority_edges` for `statute_at_large` or
`executive_order`, which it announces in `authority_type` and does not carry.

## Related documents

- [Experiment strategy](../experiment-strategy.md) — the capability table these
  verdicts are written against
- [Rulespec MVP path](../rulespec-testbed-path-forward.md) — track B
- [Decision ledger](../decisions.md) — 2026-07-28, active rulemaking
- [`rule_targets`](../tables/rule_targets.md),
  [`authority_edges`](../tables/authority_edges.md),
  [`agenda_item_proceedings`](../tables/agenda_item_proceedings.md),
  [`proceedings`](../tables/proceedings.md)
