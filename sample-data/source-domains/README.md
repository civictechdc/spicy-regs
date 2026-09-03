# Documented column domains

GSA and reginfo.gov decide what `document_type`, `docket_type`, `rule_stage`,
`priority_category`, `rin_status` and `major` may hold; our builders copy those
strings through untouched. So this directory keeps both halves of the claim: the
publishers' own documents, byte for byte, and one dated observation of what our
published tables actually hold. `scripts/check_source_domain_drift.py` compares
them in both directions and fails on every disagreement nobody has recorded a
reason for.

## The check just failed

Run it and read the lines it marks `UNRECORDED`:

```
uv run python scripts/check_source_domain_drift.py
```

Each names one column and one value the two halves disagree about, in one of two
directions:

- **undocumented-value** — our data carries a value the publisher's list omits.
  Anyone switching on the documented list mishandles those rows.
- **unobserved-value** — the publisher documents a value no row carries. Either
  the observation is bounded (it holds one semiannual Unified Agenda edition, and
  no single edition reaches every stage) or the publisher dropped the value in
  silence.

Neither is automatically a bug. Read the publisher's document, decide which it
is, then either record the finding in `ACCEPTED_DOMAIN_FINDINGS`
(`src/spicy_regs/sources/source_domains.py`) with the reason it stands, or fix
what produced it. The ledger is closed both ways: an unrecorded finding fails,
and so does a recorded finding the data stopped producing, because an exception
nothing exercises is a claim nobody checked.

## The pinned publisher documents

| File | Bytes | SHA-256 | Publisher URL |
|---|---|---|---|
| `regulations-gov-openapi-v4-2026-08-03.yaml` | 60,826 | `be43c866f5ca424a456bde36ea03cb9326c454ef4e1894a13df80b6dc6e22488` | `https://open.gsa.gov/api/regulationsgov/v4/openapi.yaml` |
| `reginfo-rin-data-ver10262011.xsd` | 22,730 | `94fdcf4b382830cc44b9956c00439dc20a9643de402c298cee71293a14153b24` | `https://www.reginfo.gov/public/xml/REGINFO_XML_Ver10262011.xsd` |

`documented-enumeration-capture-manifest-v1.json` binds each capture to its
digest, byte length, publisher URL and capture time.
`spicy_regs.sources.source_domains.read_capture` recomputes the digest and length
on every read, and every documented value is parsed out of those bytes on each
run rather than transcribed — a transcription rots in silence, a parse against a
pinned digest cannot.

The pin proves the capture has not changed since someone pinned it, so no value
can be edited into or out of a publisher's document without the check failing. It
proves nothing about what the publisher serves today, because nothing here
refetches `source_url`. Both halves of the comparison are files in this
repository, so the verdict is a function of the tree — a lock on a dated finding,
not a live drift detector. The lock is the useful part: it fails the moment
someone moves one half and leaves the ledger behind.

### regulations.gov API v4: three enums

The whole 60 KB document holds exactly three `enum` keys, in one block under a
`#Regulations.gov documentation` comment: `DocumentType` (5 values, lines
893-898), `DocketType` (2, lines 902-904) and `SubmitterType` (3, lines 908-911).
Everything else the API categorizes by — `subtype`, `category`,
`organizationType` — it calls agency-specific, so no closed list exists to check
against. Two quirks are load-bearing, and tests pin both: `- Nonrulemaking `
carries a trailing space that a YAML plain scalar drops, and `Supporting &
Related Material` carries the literal ampersand the API returns.

### reginfo.gov Unified Agenda RIN data: twenty sentences

This XSD holds no `xs:enumeration` at all. Every controlled field is
`<xs:restriction base="xs:string"/>` with no facets, and states its value list
only as `xs:documentation` prose of the form *One of the following options: "…",
"…"*. Four of those twenty sentences are read here: `RULE_STAGE` (line 66),
`PRIORITY_CATEGORY` (line 50), `RIN_STATUS` (line 58) and `MAJOR` (line 74). The
publisher's own prose repeats itself — `PRIORITY_CATEGORY` lists `Not Major`
twice — so a literal duplicate folds into one value and the raw count is kept
beside the distinct one rather than thrown away.

## What is not covered

`TTBL_ACTION` (line 443) is a decided exclusion. It names 34 timetable actions;
the same snapshot's `timetable_json` carries 1,139 distinct actions over 10,533
entries. The publisher's own data treats that field as free text, so checking
against its list would report a thousand findings and gate nothing. Its sentence
also opens *One of the following:* with unquoted values, so the XSD reader could
not parse it anyway — the reason and the parser's reach happen to coincide, and
only the reason justifies anything.

`federal_register.document_type` has no pinned document stating its list. The FR
API documentation page is not captured here, and a domain nobody published is not
a documented domain.

`comments.category` (documented by `SubmitterType`) and `comments.document_type`
(by the same `DocumentType` list already checked against `documents`) are an open
gap, not a decision. `comments` ships as a flat `comments.parquet` monolith like
the other tables, so both are observable; they stay uncovered only because the
2026-08-03 observation skipped that table. Closing the gap takes a re-observation
that includes it.

## The observation

`observed-domain-snapshot-2026-08-03.json` is derived, not captured: for each
declared column it holds the distinct values with their row support, the null
count, and the row count of the table they came from.
`scripts/check_source_domain_drift.py --observe --write-snapshot` generates it,
so the code that writes it is the code that reads it back.

It names its inputs: the three published tables it scanned, at
`https://data.spicy-regs.dev/`, as of producer revision `f1fcb8c9c883`, each with
the SHA-256 and byte length the scan actually read. Those digests are provenance,
not verification — nothing recomputes them later, because the tables run to 70 MB
and stay out of the repository. That is the whole reason a 3.7 KB summary of
2,270,416 rows exists.

## Refreshing it

Two manual steps, neither of them scheduled. `tests/test_source_domain_drift.py`
runs the offline comparison in CI on every push, so a *changed* file here fails
immediately; a *stale* one does not, because staleness is invisible to a check
whose two inputs are both checked in.

Re-observe against a fresh download of the published tables:

```
scripts/check_source_domain_drift.py --observe --data-dir <dir of published parquet> \
    --observed-at <ISO-8601> --producer-revision <sha> --write-snapshot
```

Re-pin a capture by refetching it from the `source_url` its manifest entry names,
then recording the digest and byte length of what came back. Either step surfaces
a real change as a failed test, which is the point of doing them.
