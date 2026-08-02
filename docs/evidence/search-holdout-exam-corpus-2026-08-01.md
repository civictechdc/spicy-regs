# Sealed SEARCH holdout — exam corpus DocumentRelease, 2026-08-01

**Status: built, verified reproducible, config-freeze preceded content
exposure.** This is the exam corpus for labeling the sealed search holdout:
one immutable `DocumentRelease` covering exactly the 240 drawn matters'
Federal Register documents, built through the existing release machinery
(`src/spicy_regs/document_release.py`, the a388cd0 immutable-release path).

Protocol order held: the candidate configuration under judgment was frozen
and committed first (spicysearch
`evaluation/holdout-labeling/config-freeze.md`, commit `a0a2c89`). Holdout
document content past that freeze feeds judging inputs and receipts only.

Tool: `tools/build_search_holdout_exam_release.py` (tested by
`tests/test_build_search_holdout_exam_release.py`; run targeted, never the
full suite). Artifacts live in `output/search-holdout-exam-2026-08-01/`
(gitignored output, pinned here by digest).

## Coverage

| count | value |
|---|---|
| drawn matters | 240 |
| matters with ≥1 FR document | 219 |
| matters with no FR document (proc-only / proc+agenda) | 21 |
| unique FR documents sealed | **722** |
| document versions in the release | 722 |
| structural passages (title + abstract spans) | 1,327 |

The 21 documentless matters cannot host query→document gold and are recorded
as out of the exam's scope by construction; the drafting receipt in
spicysearch carries the same count.

## Sealed content recipe (deterministic)

- Per document, `content.text` = `title` + `"\n\n"` + `abstract` (title alone
  when the abstract is absent/blank); structural passages are the exact title
  span and abstract span. The text representation is `parser-derived`,
  method `title-abstract-concatenation@1`.
- Declared metadata columns only (`FEDERAL_REGISTER_COLUMNS` in the tool):
  document number, type, publication/effective/comments-close dates, agency
  names, docket ids, RINs, topics, html_url. Malformed source facts fail the
  build closed — nothing is repaired or dropped silently.
- Sealed metadata keys align with the spicysearch engine's supported request
  filter dimensions: docket ids land as `proceeding`, agency names as
  `agency`, sealed on **every** document (an empty list is the source's own
  "states none" fact) because the engine treats a metadata filter key as
  supported only when every eligible document carries it. Match semantics
  stay fail-closed: an empty list can never satisfy a filter.
- Declared constants: `observed_at` / `released_at` `2026-08-01T22:00:00Z`,
  fixture id `urn:spicyregs:source-fixture:search-holdout-exam-2026-08-01`.
- `links` is empty by design: the exam jobs (known-item / subject-ranked /
  temporal) are query→document shaped; link-shaped jobs are not drawn from
  this corpus.

## Seal

| digest | value |
|---|---|
| source fixture (`fixture_digest`) | `sha256:fa7518f58d0858e35e1d026ece1c1c142c35a10686e12ad536acfce43f01f100` |
| release (`release_digest`) | `sha256:236034f4e40dc2f5bb0ed74b676cf8b7bfb767a14692562a8622bcd02c236f93` |
| release id | `urn:spicyregs:document-release:236034f4e40dc2f5bb0ed74b676cf8b7bfb767a14692562a8622bcd02c236f93` |
| `source-fixture.json` file bytes | `sha256:75d266f793d1a552a4f6c1159f38671b847633c703ad1f4a9449b9c7ea620a69` |
| `document-release.json` file bytes | `sha256:91ad2c6a2105abd91556045962e371dad35bbc430712c526f3812a557b8e8af8` |

Inputs consumed:

| input | sha256 |
|---|---|
| sealed-manifest.json (draw membership) | `b4737fb07f0d5e70652286de8d1e61aa7b3b92d040aac1321e9f3b1fbfcadc6e` (pinned; verified before read) |
| federal_register.parquet | recorded in `output/search-holdout-exam-2026-08-01/receipt.json` |
| rulespec core release | `urn:rulespec:core:5ac6ba59929eca874ec603cab0e90f7b15ab1a008b394cec5aefebdafe22564b` |

## Reproduce / verify

```sh
uv run python tools/build_search_holdout_exam_release.py \
    --output output/search-holdout-exam-2026-08-01 --verify
uv run pytest tests/test_build_search_holdout_exam_release.py   # hermetic, targeted only
```

Rebuilding from the pinned inputs reproduces the fixture and release digests
byte-for-byte (verified 2026-08-01).
