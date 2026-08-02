# Citation-parsing bakeoff — current parser vs current + CiteURL, 2026-08-02

**Verdict: do not wire CiteURL. Extend two owned grammars, and take two
identity questions to review separately.**

The 2026-07-27 decision ("Citation parsing: supplement-first") recorded an
exploratory probe and said of it: *"no committed command yet makes it
reproducible."* This closes that gap and runs the protocol the decision
specified. It is a **detection** evaluation. No identity changed, nothing was
wired, no regex was retired.

Tool: `tools/run_citation_bakeoff.py` with
`tools/citation_bakeoff_citeurl_worker.py`, tested by
`tests/test_run_citation_bakeoff.py` (46 tests, run targeted). Artifacts live
in `output/citation-bakeoff-2026-08-02/` — gitignored, pinned here by digest.

> **Corrections, 2026-08-02 (adversarial review).** Applied in place; struck
> text is the original claim, kept so the error is auditable. **No artifact
> byte changed** — every correction is a description of the same sealed data,
> and each is re-derivable from `adjudication.jsonl` and `detection.json`.
>
> 1. The single false positive was described as `54 CFR 05`. It is not: the
>    compact branch matches greedily and yields **title 5401, part 5405**
>    (`urn:rkaf:us:cfr:5401:5405`). Corrected below.
> 2. Two of the 14 in-scope wins are **not** regex-recoverable; they are a
>    normalization-policy question on the fenced identity surface. Corrected
>    below.
> 3. `_PUBLIC_LAW` already accepts an en-dash. The sole gap is the literal
>    spelling `Pub. Law`. Corrected below.
> 4. Three of the examples given for the 127-string shared-miss cell were not
>    in that cell. Replaced, and the cell is restated as a **floor**.
> 5. The headline gain is **33** clear wins plus 1 partial, not a flat 34.
> 6. The pilot calls are unreceipted and excluded from the reported spend.
>
> One review claim was **not** adopted, because the data contradicts it: the
> `3 CFR … Comp.` forms are not "detection-true/identity-false today." The
> project detects them not at all — which is *why* they are CiteURL-only. The
> wrong reading is CiteURL's, and it is a new finding recorded below.

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

**Two pilot runs preceded this and are not receipted.** A 6-call pilot
(~$0.0017) and a 12-call pilot (~$0.0046) ran to scratch directories under
`/tmp` to size the prompt and catch a budget defect — Gemini 3.6 Flash spends
output budget on thinking tokens, and the first pilot returned
`finish_reason="length"` with 13 answer tokens. Their receipts were not kept
and **their ~$0.006 is excluded from the $0.245 above**. Total actual spend
across all three runs is approximately **$0.251**; the cap was never at risk.
Recorded because a spend figure that quietly omits the runs that shaped the
prompt is not a spend figure.

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

**One judge error is known and located.** `auth-01871`, the string `'3102'`,
is the lone `citeurl_correct` in the `neither` row. CiteURL fired **zero**
templates on it, the judge itself answered `contains_citations: false` with an
empty citation list, and then returned `citeurl_correct` — internally
inconsistent, and impossible on the face of it, since an arm that detected
nothing cannot be the one that got it right. It does not touch the 33/1 gain
figures (those are drawn from the CiteURL-only cell, and `'3102'` is not in
it), but it is one measured defect in 620 k=1 judgments and it is the kind the
absent human-correction pass exists to catch.

### False positives — the safety half

| Arm | Claimed a citation in a string that has none |
|---|---:|
| project text grammars | **1** of 620 |
| CiteURL | **0** of 620 |

CiteURL never manufactured a citation on this corpus. The single project false
positive is `'5401-5405'`: `parse_cfr_citation`'s compact-key branch matches
any bare `N-M` string, ~~reading it as 54 CFR 05~~ and here matching **greedily**
to yield `CfrCitation(title='5401', part='5405')` →
`urn:rkaf:us:cfr:5401:5405`. There is no title 5401. (The original text said
"54 CFR 05"; a fixer sent after that would hunt a misparse that does not
exist.)

**It is latent, not live.** The compact branch is only reachable from a bare
string, and the column that feeds CFR parsing in production —
`federal_register.cfr_references_json` — carries structured objects
(`{"title": 10, "part": null, …}`), never bare `N-M` text. Zero of its 14,524
distinct values reach the compact branch. The defect is real and worth
closing, but nothing in the current pipeline triggers it.

## Verdict per citation family

Counts are citation instances from adjudicated ground truth, counting
`citeurl_correct` verdicts only; one string may carry several, so columns do
not sum to string counts. (~~The FR volume/page row previously read 14~~, which
silently mixed in a `both_partial` record while every other row counted clear
wins alone. Counted the same way as its neighbours it is **12 instances across
10 strings**.)

| Family | CiteURL adds (project cannot see) | Project detects, CiteURL misses | Both miss |
|---|---:|---:|---:|
| USC | 5 | 125 | 55 |
| CFR | 7 | 5 | 6 |
| PL | 2 | 21 | 9 |
| EO | **0** | **95** | 7 |
| Stat | 0 | 0 | 2 |
| FR volume/page | **12** | n/a — no project grammar | — |
| FR document number | 0 | 0 | 0 |
| docket | 0 | 0 | 0 |
| RIN | 0 | 0 | 0 |
| other (DC Code, caselaw, treaty, act-relative) | 10 | 0 | 57 |

Family notes:

- **EO — CiteURL has no Executive Order template at all.** 95 EO citations,
  every one detected by the project and missed by CiteURL. This is the single
  largest block in the disagreement population and it runs against CiteURL.
- **FR volume/page is CiteURL's largest genuine win (12 instances, 10
  strings).** The two "FR" families are different things: CiteURL reads
  `89 FR 1234` (volume/page); the project reads a document number
  (`2026-13078`). Neither reads the other's. Scoring them as one family would
  have credited each arm with the other's capability.
- **USC (5), CFR (7), PL (2) — all spelling variants**, not missing
  capability. These are the complete list of 14 in-scope wins:

  | Form | Strings | Recoverable by |
  |---|---|---|
  | `49 U.S. Code 106`, `49 U.S. Code 44715` — "U.S. Code" spelled out | 2 | regex |
  | `50 U.S.C.A. 4701(a)` — U.S.C. **Annotated** | 1 | regex |
  | `I.R.C. 337(d)`, `IRC 382(m)` — Internal Revenue Code | 2 | regex |
  | `Pub. Law 111–296`, `Pub. Law 119-21, …` — the spelling `Pub. Law` | 2 | regex |
  | | **7** | **pure regex** |
  | `3 CFR, YYYY Comp., p. N` — the EO compilation form | 5 | regex **+ identity rule** |
  | `48 CFR 1.301-1.304`, `and 49 CFR 1.89.` | 2 | **normalization policy — see below** |

  The last two are **already detected** by `_CFR_STANDARD` (title 48/part 1
  and title 49/part 1, with sections `301-1.304` and `89.`). They are then
  dropped by `_cfr_section`'s fail-closed normalization, which refuses a
  section token it cannot express, and `_cfr_from_match` correctly returns
  `None` rather than publish a citation with a section it had to discard.
  Recovering them means deciding what a sub-section range and a trailing-dot
  section normalize *to* — a change on the fenced identity surface the
  decision record protects ("project-owned `canonical_*` functions assign
  identity; block unreviewed identity changes"). That is a policy decision,
  not a regex, and this bakeoff does not make it.

- **"other" (10)** is DC Official Code (5), U.S. caselaw (3), one INA
  act-relative section, and one Secretary's Order matched through the Federal
  Register template — outside the federal regulatory scope this parser serves,
  and the decision record already routes judicial citation to eyecite rather
  than CiteURL. (~~Previously given as "DC Official Code (6) and U.S. caselaw
  (3)"~~.)
- **Act-relative sections are not a CiteURL win.** CiteURL fires its INA
  template on `INA sec. 103(a)(1)`, but the judge classified all four such
  strings as act-relative "other" and credited CiteURL on only one — the other
  three are the `neither` verdicts in the CiteURL-only cell. Act-relative
  references belong to the shared-miss problem below, not to the gain column.

- **CiteURL's largest in-scope "win" is a wrong parse** (found while checking
  a review claim). On the five `3 CFR … Comp.` strings CiteURL reads the
  **year as the section**:

  | String | CiteURL tokens | Matched text |
  |---|---|---|
  | `3 CFR, 1977 Comp., p. 123` | `title=3, section=1977` | `3 CFR, 1977` |
  | `3 CFR, 1980 Comp., p. 298` | `title=3, section=1980` | `3 CFR, 1980` |
  | `3 CFR, 1959-1963 Comp.` | `title=3, section=1959-1963` | `3 CFR, 1959-1963` |

  There is no 3 CFR § 1977. The string cites the Title 3 *annual compilation*
  for 1977 at page 123, which is how an Executive Order's compilation location
  is written. The judge scored these `citeurl_correct` on the question it was
  asked — *is there a citation here?* — and there is. But the parse behind the
  detection is wrong, and the page number, which is the part that identifies
  the order, is discarded.

  This sharpens the recommendation rather than changing it: whatever grammar
  covers this form must resolve to the **Executive Order** the compilation
  cites, not to a phantom CFR section. It also removes any remaining appeal in
  wiring CiteURL here — adopting it for this form would import the wrong
  reading along with the detection.

## The measured gain

CiteURL detects **33 strings of 4,777 (0.69%)** that the project's free-text
grammars cannot, plus 1 more scored `both_partial`. ~~34 strings (0.71%)~~ —
the original figure summed clear wins and the partial together.

Strip what the decision record already routes elsewhere — FR volume/page and
caselaw are the eyecite trigger, DC Code is out of scope — and the **in-scope
federal gain is 14 strings, 0.29%**: 12 spelling variants of forms the project
already parses, and 2 that it already detects and deliberately declines to
normalize.

Against that: the **shared-miss cell is at least 127 strings (2.7% of the
population)** — real citations neither arm detects, 3.8x CiteURL's clear gain
and 9x its in-scope gain. CiteURL closes none of it. Its largest measured
components are **USC chapter citations (31 strings** — `49 U.S.C. ch. 311`,
`5 U.S.C. Ch. 63`, `22 USC Ch. 34- The Peace Corps Act`) and act-relative
sections (`Clean Air Act sec. 112`, `Clean Air Act sec. 111(b)(1)(B)`,
`Exchange Act 15(c)`, `FAST Act sec. 3022`), plus presidential proclamations
and treaty citations.

**127 is a floor, not a point estimate**, because the judge's
`garbage`/`neither` boundary is unstable on exactly these act-relative forms.
The clearest evidence is a minimal pair — the same citation, two spellings,
opposite verdicts:

| String | Verdict |
|---|---|
| `Clean Air Act sec. 112` | `neither` (counted in the 127) |
| `Clean Air Act Section 112` | `garbage` (excluded from the 127) |
| `Clean Air Act sec. 111(b)(1)(B)` | `neither` |
| `Clean Air Act section 111` | `garbage` |

Whether an act-relative section reference "is a citation" is a real judgment
call, and the judge did not make it consistently. **The error direction
strengthens the conclusion**: every act-relative string wrongly sorted into
`garbage` belongs in the shared-miss cell, so the true gap is larger than 127
and the case for spending effort there rather than on a package is stronger,
not weaker.

~~Three strings were previously offered as examples of this cell —
`PHS Act secs. 2791(b)(5) and 2792`, `ERISA sec. 803`, and `sec. 3505 of the
Modernization of Cosmetics Regulation Act of 2022`. All three were adjudicated
`garbage` and are not in the 127.~~ They are, however, precisely the forms the
boundary instability affects, which is why they were miscited here.

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

1. **Extend two owned grammars** — **7** of the 14 in-scope wins, pure regex,
   no identity change: `U.S. Code` spelled out, `U.S.C.A.`, and `I.R.C.`/`IRC`
   in `_USC_STANDARD` (5 strings); the literal spelling `Pub. Law` in
   `_PUBLIC_LAW` (2 strings). ~~and the en-dash~~ — `_PUBLIC_LAW` already
   accepts `[-–—]`, and one of the two strings uses a plain hyphen, so the dash
   was never the gap.
2. **Decide two identity questions** covering the remaining **7**. These are
   policy, not pattern-matching, and the decision record fences them off from
   unreviewed change:
   - **The `3 CFR, YYYY Comp., p. N` compilation form (5 strings).** Needs both
     a new pattern *and* a rule for what it resolves to. It should yield the
     **Executive Order** identity, not a CFR section — CiteURL's reading
     (`title=3, section=1977`) is wrong, and copying it would be worse than
     detecting nothing.
   - **Sub-section ranges and trailing-dot sections (2 strings)** —
     `1.301-1.304`, `89.`. Both are already detected by `_CFR_STANDARD` and
     deliberately dropped by `_cfr_section`'s fail-closed rule. Changing that
     rule is an identity change and needs review, not a bakeoff verdict.

   7 + 7 = the 14 in-scope wins; half are regex, half are policy.
3. **Fix the one false positive.** Guard `parse_cfr_citation`'s compact-key
   branch so it does not fire on free text: `'5401-5405'` currently yields
   `urn:rkaf:us:cfr:5401:5405`. Latent rather than live today (nothing in the
   pipeline feeds it bare strings), so this is hygiene, not an incident.
4. **Aim at the shared miss (≥127 strings)**, the largest real gap and one no
   evaluated package addresses: USC chapter citations (31 measured) and
   act-relative section references. Settling the `garbage`/`neither` boundary
   for act-relative forms is a prerequisite — it is currently inconsistent,
   and the true cell is larger than 127.
5. **Leave the eyecite trigger exactly as the decision record writes it.** FR
   volume/page (12 instances over 10 strings) plus caselaw (3) is the largest
   single CiteURL-only bucket, and the decision already routes it to a separate
   eyecite evaluation when FR volume-page extraction becomes active. This
   bakeoff supplies the number that trigger will need; it does not fire it.

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
  evidence, not an oracle — and two measured defects show why: one internally
  inconsistent judgment (`auth-01871`), and an unstable `garbage`/`neither`
  boundary on act-relative section references, where the same citation in two
  spellings drew opposite verdicts.
- **Whether an act-relative reference is a citation was never settled.**
  `Clean Air Act sec. 112` is a real reference to a real provision, but it
  names no code, title, or section number that resolves without knowing which
  act. The rubric left this to the judge and the judge was inconsistent. Any
  future measurement of the shared-miss cell has to decide this first, because
  it moves the cell size materially.
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

## What was built from this, 2026-08-02

Recommendations 1–4 landed as five commits on `main`. Nothing in the sections
above was recomputed; the artifact is still the one this document pins.

| # | Landed | Recommendation |
|---|---|---|
| `1400140` | Compact-key title bound (1–50) | 3 |
| `2aa74b1` | Act-relative grammar against the OLRC name index | 4/6 |
| `a802985` | OLRC Popular Name Tool + Table III readers | 4/6 |
| `5d8dffd` | `U.S. Code` / `U.S.C.A.` / `I.R.C.` / `Pub. Law` spellings | 1 |
| `8e680d9` | `3 CFR … Comp.` recognized, never identified | 2 |
| `ef6c2d5` | U.S.C. chapter grammar + chapter URN | 4 |
| `a31605b` | Docket join key published on `fr_docket_links` | — |
| `52754a8` | USLM source-credit reader + sealed index (3,721 rows) | 4/6 |
| `d60dd6b` | Two-source resolution, disagreement refusal, re-derivation driver | 4/6 |

**12 of the 14 in-scope strings now parse.** The 7 regex ones reach the
identity their standard spelling already minted; the 5 compilation strings are
recognized as compilation locators. The remaining 2 — `48 CFR 1.301-1.304` and
`and 49 CFR 1.89.` — are the sub-section-range and trailing-dot normalization
question, which is still the unmade policy decision this bakeoff declined to
make.

**The measurement no longer describes HEAD.** The four-cell table is a fact
about the parser at `b7a5632`. Re-running `detect` after these commits produces
a different `detection.json`, and that is expected, not drift.

Two things the fixers found that this document did not:

- **The compilation form was already being misread, in spellings not in the
  citeurl-only cell.** The five strings above all write `3 CFR,` with a comma,
  which `_CFR_STANDARD` could not cross — so they were undetected, exactly as
  recorded. The same corpus writes the form in variant spellings **ten** more
  times, and every one of those was read as a CFR part: `3 CFR 1978 Comp. p.
  142` → `urn:rkaf:us:cfr:3:1978`. So the project did make CiteURL's mistake;
  it just made it on strings that landed in the agreement cell, where nothing
  was adjudicated. **Eleven** phantom CFR citations are withdrawn in total (the
  ten above plus `'5401-5405'`), and after the fix **no compilation string
  mints a CFR identity at all** — verified by re-running the full 4,777-string
  differential, where the CFR delta is 11 strings, all withdrawals, and the
  authority delta is unchanged at 9, all gains.

  Two of those ten were found by adversarial review *after* the first fix, in
  exactly the class it closed: `_EO_COMPILATION` could not cross a comma placed
  before "Comp" (`3 CFR 1979, Comp. p. 435`) nor a `to`-spelled volume range
  (`3 CFR 1949 to 1953, Comp, p. 1002`), so both still minted a phantom part.
  Every separator in that form is now optional and none of them decides
  anything. Both strings are named regressions.

  ~~The same corpus writes the form without the comma eight more times~~ and
  ~~nine phantom CFR citations were withdrawn~~ — the first fix withdrew nine;
  the population is ten plus the compact key.
- **The chapter slice is 35 strings, not 31.** Four more carry a chapter
  citation inside a string another grammar also fired on, so they were never in
  the `neither` cell. 38 chapter citations across 35 strings; 33 of the strings
  are in the shared-miss cell.
- **The chapter/section collision is attested, not hypothetical.** The argument
  for keeping chapters out of the `rkaf:us-usc` URN space was made from a
  constructed example (title 5 chapter 131 vs section 131). The corpus supplies
  four real ones: this same population cites **title 49 chapter 301 and title 49
  section 301**, and likewise (10, 55), (46, 701) and (5, 10). Under a shared
  URN space each of those four pairs would collapse to one identifier naming two
  different provisions. All four are now the fixture.

### Corrections to the implementation claims, 2026-08-02 (adversarial review)

Struck text is kept so the error is auditable, as in the corrections above.

1. **The compilation population is 19 locators across 18 strings** as of
   `8e680d9`, and **21 across 20** after the follow-up fix. ~~18 locators
   recognized across 16 strings~~ undercounted it.
2. **`'5401-5405'` was not unreachable from production.** The commit body for
   `1400140` says "nothing in the pipeline feeds the branch a bare string". That
   is false: `spicy_regs/ontology/receipt.py` (lines 1026, 1168, 1672) feeds
   `parse_cfr_citation` bare `cfr_ref` strings by design, as its identifier
   round-trip check. The true and stronger statement is: **the only bare-string
   caller is the receipt's own round-trip, and no published value has a title
   above 50 — verified across 12 generations, ~6,473 distinct keys.** The defect
   was therefore unreachable *by data* rather than *by call graph*, which is a
   weaker guarantee than the commit claimed and a real one nonetheless.
3. **`_PUBLIC_LAW` over-accepts two spellings.** Collapsing the alternation to
   `pub(?:lic)?\.?\s*l(?:aw)?\.?` also matches `publiclaw 117-58` and
   `publ 117-58`. Zero occurrences in the 4,777-string corpus, and neither
   spelling means anything else in legal prose, so it is recorded rather than
   tightened — tightening risks the real no-space spelling `Pub.L.`.
4. **`rkaf:partner-defined` is a kernel-level scheme.** The U.S. Code chapter
   identifier follows the `federal_register_identifier` precedent, but that
   precedent is itself off-profile: `partner-defined` is absent from the
   `#USRegulatoryIdentifierScheme` enumeration the us-rulemaking profile
   constrains. The chapter URN inherits that position knowingly. It remains
   preferable to a collision inside `rkaf:us-usc` (see the four attested
   collisions above), but it is a scheme question for RefSpec, not a settled
   one.

### Publication chain: two tables are now code-ahead-of-data

Neither is a defect and neither is publishable by this loop; both ride the same
decision.

* **`fr_docket_links` gained `docket_key`.** All 21 local generations carrying
  the table were rebuilt (56 s total; 893,766 rows in ~6 s each), so local state
  is consistent. **Published R2 data is not**, and the exposure is one-sided:
  `spicy_regs.ontology.receipt` is fail-closed and will refuse a stale
  generation, but `.github/workflows/deploy-docs.yml:60` runs the data-dictionary
  reconcile with `|| true`, so a docs deploy would publish a dictionary
  advertising `docket_key` while live R2 lacks it. The failure would land on a
  consumer, which is the wrong party. The workflow line now carries a comment
  naming this; the `|| true` is deliberately **not** changed here, because
  turning a docs deploy into a gate is a publication decision.
  On the pinned `rin-ontology-revision-candidate`: 893,766 rows, zero null keys,
  and the key joins 147,863 rows to the docket spine against 143,558 on the raw
  identifier. That gain is small precisely because the links build already
  applied the *label* grammar; the 88,073 rows 54f07a6 recovered (the sealed crosswalk receipt's own figure; `87,681` appeared in four places in code and tests and was never artifact-backed) were measured
  against a links table with no normalization at all. **Zero normalized keys
  cover more than one docket**, across all 276,326 — so the ambiguity refusal is
  still exercised only by tests.
* **`authority_edges` is value-stale by 9 rows**, all gains, from the spelling
  variants. The schema is unchanged, so nothing breaks; the rows are simply
  absent until a rebuild.
* **The rebuild invalidated manifest source-hash bindings.** `fr_docket_links`
  is a *source input* to downstream pipelines, not only a generation output, so
  rewriting it broke the recorded digest in **25 manifests** in this checkout
  (found by walking every `output/*/*manifest*.json`, not only `manifest.json`).
  No receipt on disk attests the rebuilt state. Regenerating them spans several
  unrelated evaluation pipelines with their own semantics and is not this
  change; the invalidation is recorded so a reader does not mistake a stale
  manifest for a corrupted one.
* **The rebuild is now deterministic.** It was not: `preserve_insertion_order =
  false` plus an `ORDER BY` with ties made nine identical-input generations
  produce nine distinct digests, which also defeated hardlink sharing across
  generations (83 MB → 982 MB on this checkout). Adding `document_number` as a
  final tiebreaker makes row order a function of the data. Verified across all
  24 rebuilt generations: every distinct row count now has exactly one digest.
* **One generation was missed** on the first pass —
  `output/mixed-real-data-corpus-v1/openai-eval/`, a nested directory the
  `output/*/` glob did not reach. All 24 are rebuilt now, in 58 s.

### Act-relative citations: measured, 2026-08-02

Superseded by measurement. The two joins named below were ingested; what
follows is what they actually resolve. Readers:
`spicy_regs.sources.uscode_olrc`. Grammar: `find_act_relative_citations`.

**The index is the grammar.** Recognition matches against the Popular Name
Tool's 13,626 names, not against a shape. The shape does not work: a generic
"capitalised words ending in Act" expression over the 4,777 sealed strings
matches `U.S.C.` 108 times. Against the index the same corpus yields **67
act-relative citations across 67 strings**, naming 26 distinct acts, with no
`U.S.C.` false positive at all.

**46 of the 67 resolve to a U.S.C. identifier**, through
popular name → Table III key → act section → title/section (~~47~~ — three
resolutions were withdrawn as ambiguous; see below):

| Citation | Resolves to |
|---|---|
| `Clean Air Act sec. 111` | `urn:rkaf:us:usc:42:7411` |
| `Clean Air Act sec. 112` | `urn:rkaf:us:usc:42:7412` |
| `ERISA sec. 803` | `urn:rkaf:us:usc:29:1193b` |
| `Social Security Act sec. 1173` | `urn:rkaf:us:usc:42:1320d-2` |
| `Federal Food, Drug, and Cosmetic Act sec. 505` | `urn:rkaf:us:usc:21:355` |
| `Wagner-Peyser Act sec. 12` | `urn:rkaf:us:usc:29:49k` |

`Clean Air Act sec. 111` → `42 U.S.C. 7411` is exactly what this document
predicted before the data existed, and it is now derived rather than asserted.

The 21 that do not resolve each say why, and none of them guesses:

| n | Reason |
|---:|---|
| 8 | `act_section_not_classified` — no Table III row (usually an amending section that classified nowhere) |
| 6 | `usc_section_not_expressible` — a note, a range, or an "et seq." |
| 4 | `source_incomplete` — one act's Table III page is unreadable (`119-21`) |
| 3 | `act_section_outside_act` — every candidate row lies outside the citing act's division |

**Table III is keyed by the enacting Public Law, not by the act, and that
matters.** Pub. L. 116-260 carries 94 popular names and 117-328 carries 70, so
one `(table3_key, act_section)` may name classifications belonging to different
acts with nothing in the citation to discriminate them — 471 of the 9,916 pairs
in the artifact. The first implementation mapped that pair to a single row,
discarding 1,060 of 10,976 rows and letting the artifact's row sort pick the
survivor. It produced a wrong identity: `sec. 107 of the Taxpayer Certainty and
Disaster Tax Relief Act of 2020` → `urn:rkaf:us:usc:49:60122`, pipeline-safety
civil penalties, for a tax act. All 471 pairs refuse; the discriminator added
below narrows them and, deliberately, still does not decide them.

**The acronym problem solved itself, and the alias rule came from the data.**
"ERISA" is an entry in the tool in its own right — 17 citations, the largest
act-relative token in the corpus — pointing at "Employee Retirement Income
Security Act". The tool lists no entry by that name: only "… Act **of 1974**".
The year is how the tool distinguishes acts, so it cannot be dropped from an
alias target; it can only be supplied, and only when exactly one act supplies
it. "Clean Air Act Amendments" would be 1966, 1970 and 1977, and picking one
would invent a citation. That refusal is a named test.

Still refused, correctly: `INA sec. 103(a)(1)`, `PHS Act secs. 2791(b)(5)`,
`ACA sec. 1557`. The tool lists none of those abbreviations, and inferring what
they stand for is the guess the identity fence exists to stop.

**Both tables are now a digest-pinned artifact**, `output/usc-act-index-2026-08-02`
(`tools/build_usc_act_index_artifact.py`, schema `usc-act-index-artifact-v2`,
parser `uscode-olrc-parser-v2`). Two real builds byte-identical across all four
files:

| Output | sha256 | Rows |
|---|---|---:|
| `usc-popular-names.parquet` | `603d5b072133d8fe…` | 20,865 |
| `usc-act-sections.parquet` | `93c4981ec437f08c…` | 10,976 |
| `quarantine.parquet` | `c2d956c67dd05203…` | 1 |
| `receipt.json` | `8f117a54c6e027e1…` | — |

13,626 distinct names; 24 acts reached, 1 incomplete. 8,451 of the 10,976 rows
carry a Statutes at Large page and 1,549 popular names carry a division — the
two halves of the discriminator, added in v2. The 67/46/21 above are
**re-derived through the sealed parquet**, not from the script that discovered
them, and the unresolved 21 carry declared codes rather than prose.

`spicy_regs.ontology.act_index` is the driver and `ActIndex.from_artifact` is
the loader, so the measurement is re-derived from the pinned bytes rather than
from a fixture. That mattered: before the loader existed the collapse policy
lived in no committed code, and first-wins and last-wins gave different answers
(67/48/19 and 67/47/20) from identical bytes. An `ActResolution` states an
identifier **or** a reason, never both and never neither. `ALIAS_YEAR_RULE` —
the rule that reaches ERISA — is pinned in the receipt with its derivation.

**One named coverage gap.** `uscode.house.gov` cannot render Table III for
Pub. L. 119-21 (the One Big Beautiful Bill Act). It returns HTTP 200 with a
truncated 16 KB body carrying zero classification rows, after exactly 30 s,
reproducibly across four httpx retries and three curl attempts with a session
cookie in both compressed and identity encodings. (HEAD returns 404 while GET
returns 200 — a JSF quirk, not the cause.) The receipt records it under
`source_incomplete` with the key, URL, what is missing, and an `observed` field
carrying a tolerant re-probe — `HTTP 200, 16134 bytes received before
RemoteProtocolError, 0 classification rows` — because "fetch failed after 4
attempts" says the client gave up, not what the server did. The 4 citations into
that act resolve to `source_incomplete` rather than the false
`act_section_not_classified`. A hole in the evidence is not the same fact
as a section going nowhere.

### The division discriminator, and the half of it that shipped

> **Superseded in part, 2026-08-02.** The closing paragraph's "next increment"
> is built — see **"USLM source credits: built, and what the build contradicts"**
> below. The measured cost figures it quotes are corrected there. Everything
> above that paragraph stands: the range rule, its asymmetry, and the 826
> withdrawn resolutions are unchanged, and the source credits now corroborate the
> asymmetry directly. The corpus counts in this section are superseded by
> **67 / 48 / 19**.

Table III cannot tell two acts apart because it is keyed by the enacting public
law. Both halves of a discriminator turned out to be **already in the pages the
build fetches, and both were being discarded**:

* The Popular Name Tool states each act's division and where it begins —
  `Pub. L. 116-260, div. EE, Dec. 27, 2020, 134 Stat. 3038`. The parser read the
  name and dropped the rest.
* Every Table III row states the page its classification sits on, in the
  statviewer link whose query string carries the volume the link text omits.

Artifact **v2** keeps both. The U.S. Code's own source credits confirm the join
from a second, independent OLRC surface — they state the division per section,
and their pages are exactly the three Table III gives for (116-260, §107):

    20 U.S.C. 80t-5  ← Pub. L. 116-260, div. T,  title I, §107, 134 Stat. 2276
    33 U.S.C. 701h-3 ← Pub. L. 116-260, div. AA, title I, §107, 134 Stat. 2623
    49 U.S.C. 60122  ← Pub. L. 116-260, div. R,  title I, §107, 134 Stat. 2221

**Validation against those credits found two defects, and only one was fixable.**

1. The range end was taken from the next *act's* start rather than the next
   *division's*. Many popular names are a title inside a division, not the whole
   of one — the AI in Government Act of 2020 begins at 134 Stat. 2286 inside
   div. U — so the range truncated the division. Fixed by keying the boundaries
   on the division at its earliest page. Agreement rose from 414 of 1,350 acts
   to **1,067 of 1,349 (79.1%)**.
2. The residual is structural and points the dangerous way. Popular names do not
   mark division boundaries at all, so a division whose acts the tool never
   names leaves the previous range overrunning into it. Measured against the
   credits, **2,240 of the 34,113 pages such a range accepts — 6.6% — belong to
   a different division.**

So the rule decides in one direction only, and the asymmetry is the point:

* **Every row outside the range → `act_section_outside_act`.** Sound despite the
  range being only an upper bound, because a page outside a range that is too
  *wide* is outside the true one.
* **A tie surviving the range → still refuses.** Narrowing on a rule measured
  wrong 6.6% of the time would mint exactly the identifier this work exists to
  prevent.

826 combinations would have resolved under the unsound half. They refuse. That
is the honest number, and it is the guardrail working rather than a shortfall.

Both named cases now carry the specific reason rather than a vague one. The
Taxpayer Certainty and Disaster Tax Relief Act of 2020 is div. EE from 134 Stat.
3038 and its three candidates sit at 2221/2276/2623; SECURE 2.0 is div. T from
136 Stat. 5275 and its two sit at 4926/5114. Every candidate is outside, so the
section belongs to a sibling act — which is the true answer, not a hedge.

The citation grammar also reads a stated division (`div. L`, `Division EE`) and
refuses with `act_division_conflict` when the citation's division contradicts
the act it names. Two corpus strings state a division; the
`(Pub. L. 116-260, Division EE)` spelling used to illustrate the problem is not
among them, which is recorded rather than assumed.

**What would resolve the remaining ties, and what it costs.** USLM's per-section
division, which needs no range at all. Measured rather than estimated: the whole
Code is one **109 MB** zip fetched in **49 s**, giving **51,548** source credits
across 58 titles, of which **13,720 (26.6%)** name `Pub. L. N-M, div. X`,
yielding **617** distinct (public law, division) pairs. That is the next
increment. It was deliberately not half-built here.

### USLM source credits: measured, parser proven, artifact NOT built

> **Superseded 2026-08-02 by "USLM source credits: built, and what the build
> contradicts" below.** It is built. Every count in this section was re-derived
> from the archive through committed code and **five of them are wrong** — the
> credit total, the loose and strict triple counts, the quarantine count, and the
> self-consistency claim. The corrected figures, and why each moved, are in the
> superseding section. The three verified cases, the strict rule, the `7652`
> false positive and both findings reproduce exactly and are unchanged. This
> section is kept because the reasoning that produced the rule is the durable
> part; its arithmetic is not.

Investigated to close the 826 combinations the page-range rule cannot soundly
decide. The mechanism works and is measured; **the artifact was not built** —
see the honest status at the end of this section.

**The parse, and the mess.** 50,998 source credits across 58 titles. A loose
expression that accepts any `Pub. L. N-M, div. X … §S … V Stat. P` inside a
credit yields 13,641 credit hits and 13,153 distinct `(public law, division, act
section)` triples — but **2,932 of those map to more than one U.S. Code
section**, and spot-checking showed why: a credit lists the original enactment
*and* every later amendment, so a loose match pairs a division with a section
number belonging to a different citation in the same credit. Reading role by
proximity to the word "amended" is not reliable — it credited `26 U.S.C. 7652`
to (116-260, div. EE, §107), which is wrong.

**Deciding conservatively rather than normalizing through it.** Requiring the
citation to sit in an explicit enactment construction — `Added Pub. L. …` or
`as added Pub. L. …` — collapses the population to something defensible:

| | Count |
|---|---:|
| source credits scanned | 50,998 |
| strict-enactment triples | 2,207 |
| **unambiguous (exactly one U.S.C. target)** | **1,882** |
| multi-target, refused | 325 |
| quarantined (section identifier unparsable) | 26 |

The strict rule removes the `7652` false positive, and it passes a
self-consistency check the loose rule fails: every retained citation's Statutes
at Large page falls inside its own division's range.

**It answers the named cases, and the answers are verified from the credits:**

    (116-260, div. EE, §107) -> 26 U.S.C. 6038E   at 134 Stat. 3048  (div. EE starts 3038)
        "(Added Pub. L. 116–260, div. EE, title I, § 107(d)(1), Dec. 27, 2020, 134 Stat. 3048.)"
    (117-328, div. T,  §120) -> nothing            -> correctly refuses
    (117-328, div. T,  §303) -> 29 U.S.C. 1153    at 136 Stat. 5339  (div. T starts 5275)
        "(Pub. L. 93–406, title I, § 523, as added Pub. L. 117–328, div. T, title III, § 303(a), …)"

**Two findings worth more than the counts.**

1. **Table III and the source credits are complementary, not redundant.**
   `26 U.S.C. 6038E` is not among Table III's three rows for (116-260, §107) at
   all. Table III simply lacks that classification, so no amount of
   discriminating between its rows would ever have found it. A disagreement
   between the two sources is a coverage fact about Table III, not noise.
2. **The range rule produces false refusals as predicted.** (117-328, div. T,
   §303) currently returns `act_section_outside_act`, but authority says
   29 U.S.C. 1153. That is the range-start-too-high failure mode this document
   already records, now observed rather than inferred — and it refuses rather
   than misidentifies, which is the direction it was designed to fail in.

**Status: not built.** ~~The parse above is a measurement script, not committed
code. The artifact, the loader wiring, the tests and the re-derivation are not
done, and nothing in the shipped resolver consults source credits — the corpus
counts remain 67/46/21 and the two named cases remain refusals.~~ **Built
2026-08-02** — `52754a8` (source + artifact) and `d60dd6b` (loader, resolver,
driver). See below. This section exists so the mechanism, the strict rule, the
counts and the verified answers survive; building it is the next increment.

### USLM source credits: built, and what the build contradicts

Supersedes the two sections above. Everything here is re-derived from the
archive through committed code — `spicy_regs.sources.uscode_uslm`,
`tools/build_usc_source_credit_artifact.py`,
`spicy_regs.ontology.act_index.SourceCreditIndex`, and
`tools/measure_act_relative_resolution.py` — rather than from the measurement
script that discovered it. That distinction already cost this project once: the
first act-index numbers depended on a collapse policy living in no committed
code, and first-wins and last-wins gave different answers from identical bytes.

**The design decision that shaped the build: two sources, not a tiebreaker.**
`26 U.S.C. 6038E` is not among Table III's three rows for (116-260, §107) at
all, so no rule that picks among Table III's rows could ever have found it. The
credit index is therefore a **second source with its own coverage**, and the
resolver consults both and states which one answered. Measured over the 222
unambiguous credit triples whose public law Table III was actually fetched for:

| | n |
|---|---:|
| Table III has **no** in-division row — the credits are the only source | **172** |
| the two agree | 30 |
| **the two disagree** | **5** |
| credit target `rkaf:us-usc` cannot spell, so not comparable | 15 |

**A disagreement is a coverage fact, not noise, and its cause is now known.** It
means the act section classified to more than one place and each source retained
a different one. All five:

| key | Table III | source credits |
|---|---|---|
| 114-94, div. C, §32101 | `22:2714a` | `26:7345` |
| 116-260, div. CC, §407 | `15:9097` | `42:1395i-6` |
| 116-260, div. S, §102 | `20:80t` | `42:16298` |
| 116-260, div. Z, §1007 | `42:17064` | `42:17086` |
| 117-328, div. FF, §3401 | `21:350a-1` | `21:350m` |

The first is checked to the bytes: FAST Act §32101 enacted both, and
`22 U.S.C. 2714a`'s credit reads `(Pub. L. 114–94, div. C, title XXXII, § 32101,
…)` with **no enactment construction**, so the strict rule does not retain it.
Both sources are right and neither is complete. The resolver publishes
`sources_disagree` and no identifier — never a third answer.

#### What the build contradicts in the section above

Re-measured from the same 109 MB archive
(`sha256:55c8d19543c4a972a33e33532b592ac3984c83fdcb04de9f5a64ef1f8483d300`,
release point 119-102).

| | recorded above | re-derived | why |
|---|---:|---:|---|
| source credits scanned | ~~50,998~~ | **51,548** | the same document already said 51,548 two sections earlier; 50,998 was the outlier |
| loose triples | ~~13,153~~ | **13,122** | a different expression over the same bytes; conclusion unchanged |
| loose multi-target | ~~2,932~~ | **2,916** | as above |
| strict triples | ~~2,207~~ | **2,202** | as above |
| unambiguous | ~~1,882~~ | **1,877** | as above |
| multi-target | 325 | **325** | reproduces exactly |
| quarantined | ~~26~~ | **0** | see below |
| (public law, division) pairs | ~~617~~ | **292** strict / 615 loose | 617 was the *loose* figure and is not the index's key space |

**Quarantine is zero, and that is a claim about attribution rather than about
tolerance.** A credit is attributed to the section that *contains* it, read from
the XML tree; on this release point every strict-rule credit lands in a
`/us/usc/tN/sX` section. 549 of the 51,548 credits sit under no section at all
(chapter-level and appendix credits) and none of them carries an enactment
construction. The quarantine machinery exists, has two declared reasons, and is
tested against synthetic unattributable credits — a zero here is a measurement,
not an absence of the check.

**The self-consistency claim was too strong, and it fails in the safe
direction.** The section above says "every retained citation's Statutes at Large
page falls inside its own division's range". Checked directly — 2,590
(triple, page) pairs whose division the act index bounds:

| | n |
|---|---:|
| inside the range | 1,950 |
| **below the range start** | **640** |
| above the range end | **0** |

Every failure is range-**start**-too-high, which is exactly the false-refusal
mode already recorded, and **not one is range-end-too-low**. The popular-name
tool states where an *act* begins, and an act may begin well inside its
division. So the asymmetry the range rule rests on — everything-outside refuses
soundly, a surviving tie still refuses — survives intact and is now corroborated
from the authoritative source rather than argued. The sentence that claimed a
clean pass does not.

#### Two findings the build produced that the measurement did not

1. **USLM and Table III spell a section suffix with different dashes.** USLM
   writes `/us/usc/t16/s824s–1` with an EN DASH (U+2013); Table III, the citation
   grammar and `rkaf:us-usc` all write a hyphen. Title 16 alone carries **1,487
   en-dash section identifiers and zero hyphen ones**, so it is a spelling
   convention of the source, not a distinction it draws. **278 of the artifact's
   3,721 rows carry it, and with the dash left in, zero of them are expressible
   as a `rkaf:us-usc` identifier at all** — 188 become expressible once it is
   straightened. In the source comparison the effect is 18 of the 222 comparable
   triples discarded as unspellable rather than joined, **10 of them agreements
   with Table III**. Straightened under a pinned rule, with the verbatim
   identifier carried alongside so nothing is lost.
2. **124 of the artifact's 3,721 rows name a section `rkaf:us-usc` cannot
   spell.** The space admits one letter of suffix (`\d+[a-z]?(-\d+[a-z]?)?`) and
   the Code writes three: `12 U.S.C. 1831cc`, `21 U.S.C. 360bbb-3`. They refuse
   with `usc_section_not_expressible` rather than being mangled into an
   identifier. Widening the lexical space is an identity decision and this does
   not make it.

#### The artifact

`output/usc-source-credit-index-2026-08-02`, schema
`usc-source-credit-artifact-v1`, parser `uscode-uslm-parser-v1`. Three real
builds byte-identical:

| Output | sha256 | Rows |
|---|---|---:|
| `usc-source-credits.parquet` | `d377545fe60d592a…` | 3,721 |
| `quarantine.parquet` | `69072c28c0a8c299…` | 0 |
| `receipt.json` | `98d39f27da5391a1…` | — |

```sh
uv run python tools/build_usc_source_credit_artifact.py \
    --output output/usc-source-credit-index-2026-08-02 \
    --archive /tmp/uscall.zip --release-point 119-102
```

3,721 rows over 2,202 triples: 1,877 unambiguous and 325 multi-target, whose
**1,844 rows are kept and marked** `refusal='multi_target'` rather than dropped.
"The source said two things" is a different fact from "the source said nothing"
and they call for different fixes. Every row carries its Statutes at Large page
(zero rows without one) and the verbatim USLM identifier. The receipt pins the
archive digest **and all 58 member digests**, so a count is attributable to the
bytes it was read from rather than only to the bundle they arrived in.

#### Re-derived through the committed loaders

```sh
uv run python tools/measure_act_relative_resolution.py \
    --act-index output/usc-act-index-2026-08-02 \
    --source-credits output/usc-source-credit-index-2026-08-02 \
    --detection output/citation-bakeoff-2026-08-02/detection.json
```

| | Table III alone | both sources |
|---|---:|---:|
| corpus citations found | 67 | 67 |
| **resolved** | 46 | **48** |
| **unresolved** | 21 | **19** |
| `act_section_not_classified` | 8 | 8 |
| `usc_section_not_expressible` | 6 | 6 |
| `source_incomplete` | 4 | 4 |
| **`act_section_outside_act`** | **3** | **1** |

Both gains are `answered_by: source_credits`, and they are the two named cases:

    sec. 107 of the Taxpayer Certainty and Disaster Tax Relief Act of 2020
      (Pub. L. 116-260, Division EE)   -> urn:rkaf:us:usc:26:6038e @ 134 Stat. 3048
    SECURE 2.0 Act of 2022, sec. 303   -> urn:rkaf:us:usc:29:1153  @ 136 Stat. 5339
    SECURE 2.0 Act of 2022, sec. 120   -> nothing from either source -> still refuses

**No existing resolution changed and none was lost.** All 46 Table III answers
survive as `answered_by: table3`, and the corpus produces zero
`sources_disagree`.

**The ambiguous combinations.** 471 `(table3_key, act_section)` pairs carry more
than one classification row, spanning **29,284 (act, act section)
combinations** — the ones the page-range rule narrows but deliberately does not
decide.

| | Table III alone | both sources |
|---|---:|---:|
| resolved | 0 | **71**, all `source_credits` |
| `act_section_outside_act` | 25,329 | 25,270 |
| `act_section_ambiguous` | 3,955 | 3,943 |

~~27,536 of 29,284 now carry a specific reason~~ — re-derived, the specific
reason `act_section_outside_act` covers **25,329**, and the other 3,955 are the
surviving ties. The earlier figure is not reproducible from the committed loader
and the sealed artifact.

**Why only 71.** The credit index is keyed by (public law, division, act
section), and only **292** (law, division) pairs carry an enactment construction
anywhere in the Code — 1,877 unambiguous triples against 29,284 combinations to
decide. This closes the cases the authority happens to state and no others,
which is the whole of what a second source can honestly do. The remaining 29,213
refuse.

**One named hole is not closed.** Pub. L. 119-21, whose Table III page
`uscode.house.gov` cannot render, has **zero rows** in the credit index. The 4
corpus citations into it still answer `source_incomplete`, which remains the true
statement.

#### What this does NOT settle

- **The comparison's denominator is Table III's coverage, not the Code's.** Only
  24 public laws have a Table III page in the act index, so 222 of the 1,877
  unambiguous triples are comparable at all. The 172/30/5 split is a fact about
  those 24 acts. A wider act index would move it and the direction is unknown.
- **The 5 disagreements are unadjudicated beyond the first.** (114-94, §32101)
  was checked to the credit bytes; the other four are consistent with the same
  mechanism and were not individually verified against the Statutes at Large.
- **The strict rule's recall is not measured.** It is known to exclude real
  classifications by construction (`22 U.S.C. 2714a`, `21 U.S.C. 350a-1` are two
  named examples). How many classifications the Code states without an
  enactment construction is not counted here, and a loose-rule recall figure
  would be meaningless while the loose rule is unsafe.
- **Nothing consumes the resolver in production.** As before, this is a driver
  and a measurement. No pipeline calls `resolve_act_relative_citation`.

### Queued, not built

Recorded so the next reader inherits the reasoning rather than rediscovering it.

* **Popular names are vocabulary-shaped.** The alias graph is prefLabel /
  altLabel structure. If act names should be searchable as concepts, the path is
  spicy-regs publishes the crosswalk and RefSpec ingests it through the normal
  atlas generation path. That is RefSpec's decision, made in RefSpec — the atlas
  asset id is its own sha256, so a merge would re-mint it and invalidate the
  pins in two repos.
* **The ~~three~~ four artifact tools now restate `canonical_json`,
  `file_sha256`, `_pin_path`, `_write_parquet` and the secret-scan expression.**
  `build_usc_source_credit_artifact.py` is the fourth and copies all five.
  Extracting them means touching ~~two~~ three digest-sealed tools and
  re-verifying their byte-identical rebuilds.
* **EO compilation, if it is ever built.** The investigation below argues the
  demand should be measured first, and that the two tiers must stay separate
  artifacts.

### The EO compilation page: sources evaluated, 2026-08-02

Investigated to close the gap flagged when `ExecutiveOrderCompilation` landed as
a non-identity recognition. Findings, each verified against fetched bytes:

* **GovInfo Title 3 works, but only from the 1996 Comp. onward.** Package
  `CFR-{Y+1}-title3-vol1` is the `Y` compilation. Title 3 starts at 1997
  (`CFR-1996-title3-vol1` 302s; 1997 is 200). MODS metadata carries the EO
  number but **no page**; the page lives only as `[[Page N]]` markers in the
  whole-volume HTML, which is keyless and parseable. Verified:
  `3 CFR, 1996 Comp., p. 157` → E.O. 12988, matching the FR text exactly.
* **The U.S. Code USLM XML does NOT carry the compilation citation.** This was
  the leading hypothesis and it is false. Across titles 3, 5, 10, 20, 42 and 50
  — ~4,561 `Ex. Ord. No.` references — there are **zero** `Comp., p.`
  citations; the notes are FR-only (`Ex. Ord. No. 11340, Mar. 30, 1967, 32 F.R.
  5453`). Ruled out.
* **NARA disposition tables do not carry it either.** `grep -c CFR` on the 1977
  table is 0. Ruled out. The federalregister.gov API has no `3 CFR` field and
  starts 1994-01-03.
* **Pre-1996 is resolvable only by inference**, by mining the FR corpus's own
  republished CFR authority notes, which do carry the triple. A hand-run demo
  reached 7 of the 8 citations probed — **not a coverage measurement**, since
  those citations were hand-picked and each was eyeballed across three
  documents. It is co-occurrence inference rather than a published crosswalk, it inherits agency citation errors (`20 CFR 402.75` cites
  "Executive Order No. 12958 (1995) (3 CFR, 1987 Comp., p. 235)" where the true
  answer is 12600), and so it requires voting across ≥3 documents.

**Two findings change the shape of the problem:**

1. **The premise "compilation page → Executive Order" is unsound.**
   `3 CFR, 1949-1953 Comp., p. 1002` is **Reorganization Plan No. 2 of 1950**,
   not an EO. Title 3 compilations also carry proclamations, reorganization
   plans, memoranda and determinations. A resolver must return a *presidential
   document with a type*, not an EO number — which means
   `ExecutiveOrderCompilation` is misnamed for what it will eventually resolve
   to, and the type should be settled before a resolver is built.
2. **`3 CFR, 1959-1963 Comp.` is unresolvable by any source**, because it names
   a volume and no page. The best any source gives is the bound (that volume
   holds EO 10798–11134). The existing decision to make `page` optional and
   publish no identifier without it is therefore correct and stays.

Consequence: the non-identity recognition already committed is the right
resting place, and the *next* step is a typed presidential-document resolver,
not an EO-number lookup.

**Three further findings, recorded and not acted on:**

1. **The compilation cite is the tail of a triple whose head is the answer.**
   `E.O. 11991, 42 FR 26967, 3 CFR, 1977 Comp., p. 123` is the Office of the
   Federal Register's house form. A citation parser that splits that triple
   manufactures the orphan it then needs a resolver for. **Measure the genuine
   residual before building anything** — distinct `(compilation, page)` pairs
   with no document identity within ~150 characters to the left. That number
   decides whether there is a project here at all.
2. **Page iv of any GovInfo Title 3 volume carries a compilation → EO-number
   range table covering 1936 to present** — roughly 40 rows, one keyless fetch,
   including every year GovInfo holds no volume for. It is a cheap deterministic
   *validator* over any inferential resolution, and it independently catches the
   known bad datum: `20 CFR 402.75` cites "Executive Order No. 12958 (1995)
   (3 CFR, 1987 Comp., p. 235)" where the true answer is 12600, and 12958 falls
   outside the 1987 range 12580–12622. Highest value per byte in the whole
   investigation.
3. **If built, two artifacts, never one.** GovInfo `[[Page N]]` transcription is
   a *fact* — deterministic, reproducible, digest-pinnable, and it starts at the
   1996 Comp. Pre-1996 resolution by Federal Register co-occurrence is
   *inference*, and it demonstrably inherits agency citation errors. Merging
   them would launder inference as fact, which is the failure the platform's
   `twoIndependentMachinesSearchOnly` posture exists to prevent. And no live API
   call at query time: build, digest, pin, ship.

### Act-relative citations: what would unlock them (superseded — see above)

Not built, on purpose. `Clean Air Act section 111` is the remaining bulk of the
≥127 shared miss, and it is a data problem rather than a grammar problem — the
string names no code, title or section number, so no expression can resolve it.
Two joins are missing, and they are separate:

1. **Popular name → code location.** "Clean Air Act" → 42 U.S.C. ch. 85. The
   Office of the Law Revision Counsel publishes this as the U.S. Code *Popular
   Name Tool* (`uscode.house.gov`). Not ingested. This half now has a landing
   surface: `urn:spicy-regs:usc-chapter:42:85` is exactly the identity a
   popular-name index would map to.
2. **Act section → U.S.C. section.** "Clean Air Act sec. 111" → 42 U.S.C.
   § 7411. This is *not* arithmetic and cannot be derived from the chapter — it
   is the OLRC classification tables (Table III, Statutes at Large → U.S. Code),
   keyed by Public Law and Statutes-at-Large section. Also not ingested, and
   this is the harder of the two.

A prerequisite stands ahead of both, and it is the one recorded above under
"What this does NOT settle": whether an act-relative reference *is* a citation
was never decided, and the judge answered inconsistently on minimal pairs
(`Clean Air Act sec. 112` → `neither`, `Clean Air Act Section 112` →
`garbage`). Ingesting either table before settling that would produce a
recall number no one can interpret.
