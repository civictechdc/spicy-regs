# Citation-parsing bakeoff — current parser vs current + CiteURL, 2026-08-02

**Verdict: do not wire CiteURL. Extend four owned grammars instead.**

The 2026-07-27 decision ("Citation parsing: supplement-first") recorded an
exploratory probe and said of it: *"no committed command yet makes it
reproducible."* This closes that gap and runs the protocol the decision
specified. It is a **detection** evaluation. No identity changed, nothing was
wired, no regex was retired.

Tool: `tools/run_citation_bakeoff.py` with
`tools/citation_bakeoff_citeurl_worker.py`, tested by
`tests/test_run_citation_bakeoff.py` (46 tests, run targeted). Artifacts live
in `output/citation-bakeoff-2026-08-02/` — gitignored, pinned here by digest.

## The probe reproduces exactly

The recorded four-cell table still holds at today's data. The corpus did not
move.

| | CiteURL detects | CiteURL does not |
|---|---:|---:|
| **current parser detects** | **4,157** | **233** |
| **current parser does not** | **108** | **279** |

Recorded 2026-07-27: 4,157 / 233 / 108 / 279. Measured 2026-08-02: identical.

"Current parser" here is `parse_authority_citation` alone — the single
function the original probe compared. Reproducing the table requires comparing
exactly that.

## Identity

| Surface | Value |
|---|---|
| detection schema_version | `citation-bakeoff-detection-v1` |
| authority-string set | `sha256:e880ea83bcf6768a567065fe2292a53f531bca47abb47a2d41884eecedb703f7` (4,777 distinct) |
| `authority-strings.json` | `sha256:940def0a82627ffe649b63a09f02d6e584d385f8e35e4cb742751a269f40f126` |
| `detection.json` | `sha256:6a6bbfe885beaa5430e4a92eb3a8a7a4a1298aa98a0580a79d943a163ce3cfc7` |
| `adjudication.jsonl` | `sha256:0af1ef8b5d0499a339300ded8610f56460ab6fef650dda977c41c77b9a433b8e` (620 lines) |
| citeurl worker | `sha256:89f2ebab946b8acb7904eae88e68050be47e18bacaef8da3382f6888ab24309b` |

Source: `output/rin-ontology-revision-candidate/unified_agenda.parquet`,
`sha256:e6862d5d6a5300f10c70eeaf321f1e82e1f5332f71069d07723cc584ee6a85ae` —
3,954 rows, column `legal_authority_json`, 10,432 authority values, 0
malformed rows, 0 empty rows, 4,777 distinct strings.

The population digest is taken over the **sorted distinct string set**, not
over the parquet. Several published copies of `unified_agenda.parquet` differ
byte-for-byte (`rin-ontology-revision-candidate` and `mixed-real-data-corpus-v2`
have different digests and different sizes) while carrying exactly the same
authorities. Pinning the set is what makes "the 4,777 strings" an object
rather than a property of one file — verified by rebuilding from both copies,
which produced byte-identical `authority-strings.json` and `detection.json`.

## The exact command

```sh
uv run python tools/run_citation_bakeoff.py detect \
    --unified-agenda output/rin-ontology-revision-candidate/unified_agenda.parquet \
    --output output/citation-bakeoff-2026-08-02 \
    --citeurl-venv /tmp/bakeoff-venv

uv run python tools/run_citation_bakeoff.py adjudicate \
    --output output/citation-bakeoff-2026-08-02 \
    --model gemini-3.6-flash --cost-cap-usd 5.0

uv run python tools/run_citation_bakeoff.py verdict \
    --output output/citation-bakeoff-2026-08-02
```

## Pinned versions

| Component | Pin |
|---|---|
| CiteURL | `12.0.3` (144 templates loaded) |
| markdown | `3.10.3` — **the workaround** |
| scratch venv Python | 3.12.9, built by `uv venv` at `/tmp/bakeoff-venv` |
| repo Python | 3.12.9 |
| judge | `gemini:gemini-3.6-flash`, `response_format` json_schema, `reasoning_effort=low`, k=1 |
| current parser | `src/spicy_regs/ontology/citations.py` at `b7a5632` |

CiteURL stays experimental and is **not** in `pyproject.toml`. It runs in a
throwaway venv, out-of-process, and the receipt pins the versions actually
imported rather than the ones requested.

The undeclared-import defect is confirmed, not assumed: `citeurl/__init__.py`
imports `.mdx` unconditionally and `citeurl/mdx.py` does
`from markdown.extensions import Extension`, but `markdown` is not a declared
dependency. A bare `pip install citeurl` in a clean venv produces a package
that raises `ModuleNotFoundError: No module named 'markdown'` on import. The
workaround is to install `markdown` alongside it.

## The probe compared one function; the project owns more

70 of the 108 apparent "CiteURL-only" wins are CFR citations the project
already parses — in `parse_cfr_citation`, which the probe did not call. That
is a materially different fact from "CiteURL sees something we cannot," and it
changes the recommendation.

The comparison a recommendation can rest on uses the project's **free-text**
grammars: `parse_authority_citation` (USC/PL/Stat/EO) plus `parse_cfr_citation`.

| | CiteURL detects | CiteURL does not |
|---|---:|---:|
| **project text grammars detect** | 4,227 | 239 |
| **project text grammars do not** | **38** | 273 |

`normalize_rin`, `canonical_frdoc_iri` and `normalize_docket_reference` are
deliberately excluded. They read a *column* whose every value is meant to be
one identifier; pointed at authority prose they over-fire. Scoring them here
would charge the project for false positives it does not make in production —
see "What this measured that production does not do" below.

## Adjudication

Every one of the 620 disagreement strings was judged — a census, not a sample
(the stratified draw supports capping, and none was needed). k=1, because
detection adjudication is a factual question about a short string, not
relevance grading.

- **620/620 adjudicated. Zero provider failures.** No item was retried.
- 448,836 input tokens, 44,083 output tokens.
- **Total spend: $0.245** against a $5.00 cap. Even at ten times the pinned
  price the run stays under the cap.

Prices are pinned estimates ($0.30/$2.50 per Mtok in and out); the token
counts the provider reported are the durable fact and are recorded next to
them, so the spend can be recomputed if published pricing differs.

Every call carries its own request digest, response digest, model id, token
counts and finish reason in `adjudication.jsonl`.

The **550** strings that disagree under the text-grammar arm are adjudicated
at 100%. A further 70 agreement-cell strings were adjudicated as a byproduct.

### Adjudicated verdicts, text-grammar arm

| Cell | n | current correct | CiteURL correct | both partial | neither | garbage |
|---|---:|---:|---:|---:|---:|---:|
| current only | 239 | 238 | — | — | — | 1 |
| CiteURL only | 38 | — | 33 | 1 | 4 | — |
| neither | 273 | — | 1 | — | 127 | 145 |
| both (partial coverage) | 70 of 4,227 | 37 | 31 | 2 | — | — |

"garbage" means the string contains no legal citation at all, so detecting
nothing is the correct answer.

### False positives — the safety half

| Arm | Claimed a citation in a string that has none |
|---|---:|
| project text grammars | **1** of 620 |
| CiteURL | **0** of 620 |

CiteURL never manufactured a citation on this corpus. The single project false
positive is `'5401-5405'`, read as 54 CFR 05 by `parse_cfr_citation`'s
compact-key branch, which matches any bare `N-M` string.

## Verdict per citation family

Counts are citation instances from adjudicated ground truth; one string may
carry several, so columns do not sum to string counts.

| Family | CiteURL adds (project cannot see) | Project detects, CiteURL misses | Both miss |
|---|---:|---:|---:|
| USC | 5 | 125 | 55 |
| CFR | 7 | 5 | 6 |
| PL | 2 | 21 | 9 |
| EO | **0** | **95** | 7 |
| Stat | 0 | 0 | 2 |
| FR volume/page | **14** | n/a — no project grammar | — |
| FR document number | 0 | 0 | 0 |
| docket | 0 | 0 | 0 |
| RIN | 0 | 0 | 0 |
| other (DC Code, caselaw, treaty, act-relative) | 10 | 0 | 57 |

Family notes:

- **EO — CiteURL has no Executive Order template at all.** 95 EO citations,
  every one detected by the project and missed by CiteURL. This is the single
  largest block in the disagreement population and it runs against CiteURL.
- **FR volume/page is CiteURL's largest genuine win (14).** The two "FR"
  families are different things: CiteURL reads `89 FR 1234` (volume/page); the
  project reads a document number (`2026-13078`). Neither reads the other's.
  Scoring them as one family would have credited each arm with the other's
  capability.
- **USC (5), CFR (7), PL (2) — all spelling variants**, not missing
  capability. These are the complete list of 14 in-scope wins:

  | Form | Strings |
  |---|---|
  | `3 CFR, YYYY Comp., p. N` — the EO compilation form | 5 |
  | `48 CFR 1.301-1.304`, `and 49 CFR 1.89.` — range and leading conjunction | 2 |
  | `49 U.S. Code 106`, `49 U.S. Code 44715` — "U.S. Code" spelled out | 2 |
  | `50 U.S.C.A. 4701(a)` — U.S.C. **Annotated** | 1 |
  | `I.R.C. 337(d)`, `IRC 382(m)` — Internal Revenue Code | 2 |
  | `Pub. Law 111–296`, `Pub. Law 119-21, …` — "Pub. Law" + en-dash | 2 |

- **"other" (10)** is DC Official Code (6) and U.S. caselaw (3) — outside the
  federal regulatory scope this parser serves, and the decision record already
  routes judicial citation to eyecite rather than CiteURL.
- **Act-relative sections are not a CiteURL win.** CiteURL fires its INA
  template on `INA sec. 103(a)(1)`, but the judge classified all four such
  strings as act-relative "other" and credited CiteURL on only one — the other
  three are the `neither` verdicts in the CiteURL-only cell. Act-relative
  references belong to the shared-miss problem below, not to the gain column.

## The measured gain

CiteURL correctly detects **34 strings of 4,777 (0.71%)** that the project's
free-text grammars cannot.

Strip what the decision record already routes elsewhere — FR volume/page and
caselaw are the eyecite trigger, DC Code is out of scope — and the **in-scope
federal gain is 14 strings, 0.29%**. Every one of those 14 is a spelling
variant of a form the project already parses.

Against that: the **shared-miss cell is 127 strings (2.7% of the population)**
— real citations neither arm detects, 3.7x CiteURL's total gain and 9x its
in-scope gain. CiteURL closes none of it. Its largest components are USC
chapter citations (`49 U.S.C. ch. 311`, `5 U.S.C. Ch. 63`) and act-relative
sections (`PHS Act secs. 2791(b)(5) and 2792`, `ERISA sec. 803`,
`Exchange Act 15(c)`, `sec. 3505 of the Modernization of Cosmetics Regulation
Act of 2022`), plus presidential proclamations and treaty citations.

## Recommendation

**Do not wire CiteURL as a recognizer.** The decision standard is "a material
held-out gain before retiring any regex." There is no held-out gain here:
0.29% in-scope, on a population that is now development data, against a
package that is experimental, cannot import itself without an undeclared
dependency, and has no Executive Order grammar at all.

Nothing is retired regardless — a supplement is a union, so it could only add.
But 0.29% does not buy a dependency, and it is smaller than the shared gap the
dependency does not close.

Do this instead, in order of measured value:

1. **Extend four owned grammars** to cover the spellings tabulated above,
   which is a few characters of regex each and keeps identity project-owned:
   `U.S. Code` spelled out, `U.S.C.A.` and `I.R.C.`/`IRC` in `_USC_STANDARD`;
   `Pub. Law` and the en-dash in `_PUBLIC_LAW`; the `3 CFR, YYYY Comp., p. N`
   compilation form in the CFR grammar. That recovers all 14 in-scope wins
   without adding a package — and each one is a case the bakeoff has already
   written down, so the change is testable against this artifact.
2. **Fix the one false positive.** Guard `parse_cfr_citation`'s compact-key
   branch so it does not fire on free authority text; `'5401-5405'` is not
   54 CFR 05.
3. **Aim at the 127-string shared miss**, which is the largest real gap and
   which no evaluated package addresses: USC chapter citations and
   act-relative section references.
4. **Leave the eyecite trigger exactly as the decision record writes it.** FR
   volume/page (14) plus caselaw (3) is the largest single CiteURL-only
   bucket, and the decision already routes it to a separate eyecite evaluation
   when FR volume-page extraction becomes active. This bakeoff supplies the
   number that trigger will need; it does not fire it.

Detection is complementary rather than substitutable, which is the honest
summary: inside the agreement cell there are 32 strings where both arms fire
and name *different* families — 14 of them the project reading an EO where
CiteURL reads the FR volume/page in the same string (`E.O. 11048, Sept. 1,
1962, 27 FR 8851`). That is both arms being right about different things, and
it is an argument for a second FR grammar, not for a second package.

## What this measured that production does not do

The extended arm pointed `normalize_docket_reference` at authority prose,
which production never does — it reads a docket column. Pointed at prose it
produced **18 false positives**, returning bare section numbers (`1255`,
`1464`, `93a`, and even `I`) as Regulations.gov dockets, because
`normalize_regsgov_identifier` accepts anything the syntax can spell and
`normalize_docket_reference` returns early on that path before the stricter
docket-shape check. This is **not a production defect** and is excluded from
every headline number above. It is recorded because it states the function's
real contract: `normalize_docket_reference` is only safe on a value already
believed to be a docket reference.

Related and also excluded: `normalize_docket_reference` accepts every RIN as a
docket, and `parse_cfr_citation`'s compact-key branch accepts every FR
document number as a CFR citation. Both are recorded in
`tests/test_run_citation_bakeoff.py` as known overlaps.

## What this does NOT settle

- **The 4,157-string agreement cell is unadjudicated.** Both arms may agree
  and both be wrong. This measured whether a citation was *noticed*, not
  whether it was read correctly — a system that detects `42 U.S.C. 7401` but
  extracts section 7671 scores as a correct detection here.
- **Extraction, normalization and identity are untouched.** CiteURL's URLs and
  internal IDs were never read. Nothing here bears on whether recognized raw
  text equals `SourceFragment[start:end]`, which remains the projection gate.
- **The human correction has not happened.** The protocol specifies "frontier
  model first pass, async human correction." This is the first pass only:
  k=1, one model, no second opinion, no human review. The verdicts are
  evidence, not an oracle.
- **There is no held-out split.** The full population was adjudicated, so
  under the 2026-07-27 gold and held-out protocol these 620 items are now
  permanently development data. A future "material held-out gain" claim needs
  a different population.
- **Family-level correctness inside the agreement cell** — the 32
  both-fire-different-families strings — is counted but unadjudicated.
- **eyecite was not evaluated.** The decision record scopes it to an active
  judicial or FR volume-page need, and neither has been declared.
- **One corpus, one source.** Unified Agenda authority strings only. Nothing
  here generalizes to Federal Register preamble prose or CFR body text, where
  the citation distribution is different.
