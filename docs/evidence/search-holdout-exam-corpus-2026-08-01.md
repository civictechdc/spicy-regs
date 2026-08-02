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
- Declared constants: `observed_at` / `released_at` `2026-08-01T22:00:00Z`,
  fixture id `urn:spicyregs:source-fixture:search-holdout-exam-2026-08-01`.
- `links` is empty by design: the exam jobs (known-item / subject-ranked /
  temporal) are query→document shaped; link-shaped jobs are not drawn from
  this corpus.

## Seal

| digest | value |
|---|---|
| source fixture (`fixture_digest`) | `sha256:cb487442edd1a7855bca124d47d4591f97df32818c9f9d4f741b2f21faace28b` |
| release (`release_digest`) | `sha256:4c789818302c8116f1f39f3dc98b19de11949fc5e52a8bc2f2acc1f975137bb5` |
| release id | `urn:spicyregs:document-release:4c789818302c8116f1f39f3dc98b19de11949fc5e52a8bc2f2acc1f975137bb5` |
| `source-fixture.json` file bytes | `sha256:d8ed4f5aef21657cb3dddf9488b598b01d4981754ad8293de870bc50398e1866` |
| `document-release.json` file bytes | `sha256:d66a33a053761fa8af0a4ad75e1d8ac5cea0e4f2b8da9b3a2078d3b6735c862a` |

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
