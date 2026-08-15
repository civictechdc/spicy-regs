# SpicyRegs plan

Recorded 2026-08-11 against SpicyRegs `6dbe181ccec7` and RefSpec
`3c1b94ace91f`. Sections 1, 3, 3a and 4 rewritten 2026-08-12 against
`a8938b4`, as records of what was executed and of the owner's decision on how
the payload lands. This file carries the SpicyRegs-owned work: the two
decisions and their execution, the branch reset, the `citations.py` boundary
fixes, the payload landing, the `rkaf_projection.py` boundary freeze, the
editable-install crossing, the canonical-JSON profile rule, and the torn
applicability policy.

The cross-product ownership boundary and its payload rule are REF-024 in
`RefSpec/docs/decisions.md`. This plan cites REF-024 by identifier and does
not restate it.

Ordering, as a constraint of this plan: sections 1 and 5 land before the
first commit of section 4. Section 3 rides in that first landing.

## 1. Two decisions, executed

Both were executed 2026-08-12 on `integrate/payload-prereqs`, branched from
`feat/rkaf-boundary-freeze` at `a8938b4`.

### 1a. URN spelling — respelled

`urn:spicy-regs:` stood at ~7,270 sites, `urn:spicyregs:` at 328 occurrences
over 129 lines in 65 tracked files (re-derived 2026-08-12 at the archive tip,
excluding `RefSpec/`, which carries 66 files of its own and is another
repository's to respell). The 2026-08-11 count recorded here — 209 lines, 341
occurrences, 78 files — was taken against a working tree that also held
untracked build output; the tracked set is what moved. The minority spelling
was the newer v3 writer, so containment decided for `urn:spicy-regs:` and the
minority moved.

Respelled: 11 modules under `src/spicy_regs/`, 3 tools, 9 test modules, both
`policies/` artifacts, both `src/spicy_regs/fixtures/spicyregs-m1-*` fixtures,
and both v3 source-selection ledgers under `tests/fixtures/`.
`document_release_v3_writer._stable_urn` and every `*_SCHEMA_ID` constant in
`document_release_v3.py` mint the new spelling.

Artifact identifiers moved, as the decision accepted:

- `spicyregs-m1-source-fixture-v1.json` reseals to `fixture_digest`
  `sha256:6fa62fa8af16754675cb77981191cdf94b98665e8f5c42609caf7256384e5ca7`.
- `spicyregs-m1-document-release-v1.json` regenerates from it through
  `build_document_release()`; its release id is now
  `urn:spicy-regs:document-release:424021f0c521d1f2e7f19cecea062e18cc1f75a247fac9603ddb5d625d88dae3`.
- Both checked-in v3 release fixtures were rebuilt from their committed
  sources rather than text-substituted: the URNs sit inside digested parquet
  and schema members, so only a rebuild keeps the seal honest. Base artifact
  digest `23d975ee516a24fcf8a4ce5056cfc307ee06461099b577f97a8bb533dddcd34a`,
  mixed `cd65b3e7340b769d0bcf6190624774fa96336f583fba13b3db3010f095971294`.
  The mixed ledger's four `oldDocumentVersionId` values are re-pinned to the
  rebuilt base's document-version ids; the digest pins in
  `tests/test_document_release_v3.py` and
  `tests/test_document_release_v3_incremental_fixture.py` follow.

The base fixture had no rebuild recipe on record and did not reproduce from
one: its parquet members carry 2 and 4 row groups where the current writer
produces 1, so it predates the writer it was built with. It is rebuilt here
with the same parameters the mixed fixture's rebuild test already uses —
`--row-batch-size 2000`, `--compression zstd`, `--memory-limit 512MB`, the
ids in its own `release.json` and `receipts/build.json`, and
`--build-run-id document-release-v3-fixture-v1` at
`2026-08-04T00:00:00Z`/`2026-08-04T00:00:01Z` — so it is reproducible from
now on.

No identifier-format validator or regex constrains this prefix. The only
compiled URN patterns in the tree are `urn:rkaf:*`
(`docpipeline/rkaf_projection.py:193`, `ontology/receipt.py:54`) and
`urn:ref:vocabulary-atlas:*` (`candidate_release.py:30`); nothing splits or
slices a `urn:spicy-regs:` string positionally, and RFC 8141 allows the
hyphen in the namespace identifier.

Two things deliberately keep the old spelling:

- Nine identifiers in dated records under `docs/evidence/` — two receipts and
  three reports from 2026-08-01 and 2026-08-02. Those are what those runs
  actually minted; rewriting them would make a dated record lie. The one
  `docs/` occurrence that stated the *format* rather than a past result
  (`scale-architecture-report-2026-08-04.md:181`) is respelled.
- Gitignored regenerable outputs are not regenerated here. Nineteen files
  under `output/` — in `agency-crosswalk-2026-08-02`,
  `body-retrieval-corpus-2026-08-02`, `date-event-artifact-2026-08-01`,
  `scale-dr-10k-2026-08-05` and `search-holdout-exam-2026-08-01` — predate the
  respell and still carry `urn:spicyregs:`. They regenerate with the new
  spelling on the next build, and no checked-in digest pins them.

The counts above are `git grep` over tracked files. A gitignore-aware search
of the whole worktree agrees on the tracked set, which is how the `output/`
residue was found separately rather than folded into the inventory.

### 1b. Byte versus codepoint offsets — audited, nothing to convert

The page the decision called for.

**What was audited.** Every offset-bearing site in the payload: the evidence
resolver `ontology/llm.py:resolve_exact_evidence_offsets`, the `start_char`/
`end_char` family (22 modules, ~380 sites, led by `docpipeline/source.py`,
`docpipeline/segments.py`, `docpipeline/retrieval.py`, `docpipeline/tag_task.py`,
`ontology/subjects.py`, `ontology/adapters.py`, `ontology/concepts.py`,
`ontology/relation_findings.py`, `ontology/ledger.py`, `rulespec_testbed.py`),
the citation and fragment spans in `docpipeline/rkaf_projection.py`, the
parser spans in `docpipeline/adapters/docling.py`, and the v3 sink
(`document_release_v3.py`, `document_release_v3_writer.py`,
`document_release_v3_verify.py`).

**What is already bytes.** The v3 sink, throughout and provably.
`_segment_utf8` (`document_release_v3_writer.py:707`) encodes to UTF-8 and
splits only on lead bytes; passages carry `normalized_start_utf8_byte` and
`normalized_end_utf8_byte` as `uint64`; `coordinate_scheme` is
`urn:spicy-regs:coordinate:rendition-utf8-byte-slice:1.0`. The verifier closes
it: `document_release_v3_verify.py:763-775` re-slices the normalized *bytes*
and fails on a split code point, and `:905-920` seeks
`byte_offset + startUtf8Byte` into the rendition pack and byte-compares.
Normalization is `rendition.decode("utf-8")` (`writer.py:881`), so the
normalized bytes *are* the rendition bytes and the coordinate's stated target
is the one it addresses.

**What counts code points.** Everything else, and each of them says so in a
constant: `docpipeline/source.py:132` `COORDINATE_UNIT = "unicode-codepoints"`,
`docpipeline/rkaf_projection.py:202` `_COORDINATE_SYSTEM =
"rkaf:unicode-codepoint"`, `enrichment/open_set.py:217`
`"rkaf:coordinateSystem"`, `docpipeline/relation_task.py:179` and
`corpora/relation_exclusion_evaluation_v2.py:179` `"offset_unit": {"const":
"unicode_codepoint"}`, `docpipeline/adapters/docling.py:989`
`unit="unicode-codepoints"`, and `document_release.py:43` (v2)
`COORDINATE_SYSTEM = "unicode-codepoints-half-open"`. Each is proved the same
way the v3 sink proves itself: by re-slicing the `str` it indexes
(`source.py:1759`, `segments.py:1017`, `rkaf_projection.py:281-288`,
`relation_findings.py:263-264`).

**Whether they disagree.** They do not, because no number crosses. The check,
run three ways:

1. No `start_char`-family field exists in any `document_release_v3*` module,
   and no `utf8_byte` field exists anywhere outside them. The two spaces share
   no column, no key, and no dataclass field.
2. The v3 module family imports nothing from `ontology/` or `docpipeline/`.
   The one edge in the other direction is `docpipeline/executor.py:25`, which
   takes `canonical_json_bytes`, `parse_canonical_json`, `require_sha256`,
   `sha256_file`, and `validate_object_key` — five helpers, no offset.
3. Every consumer of `normalized_*_utf8_byte` is inside v3's own verifier or
   its tests.

**What changed.** One site, and it is a declaration rather than a conversion.
`resolve_exact_evidence_offsets` was the only member of the code-point family
that did not name its unit, and it is the one the rest of the family calls.
It now carries `EVIDENCE_OFFSET_UNIT = "unicode-codepoints"` and
`EVIDENCE_OFFSET_INTERVAL = "half-open"`, and `EvidenceOffsetResolution`
records both on the value it returns. Nothing was converted to bytes, because
converting a self-consistent code-point space that never reaches the byte sink
would break every artifact digest in the ontology path to fix a disagreement
that does not exist.

**What the decision therefore means going forward.** "Offsets are bytes" binds
the live sink and anything new that writes into it: v3 is byte-counted and
stays byte-counted, and a new coordinate that reaches a release member is
bytes. It does not retroactively convert the code-point evidence space, which
addresses `str` values that never become release coordinates. The two spaces
stay separate and each keeps stating its unit; a future crossing has to
convert explicitly at the boundary, and there is no such boundary today.

### Landing constraint

Both decisions land before the first commit of section 4. The payload carries
`urn:spicy-regs:` in `src/spicy_regs/ontology/citations.py`,
`src/spicy_regs/ontology/attestations.py`, and
`src/spicy_regs/ontology/relation_findings.py`.

## 2. The branch reset, as a ref move

Executed 2026-08-11: `archive/local-work-2026-08-09` is a tag, and `main`
sits on `origin/main` (`f1fcb8c9c883`) plus this plan. The procedure, kept
for the record:

1. Rename `archive/local-work-2026-08-09` to a tag. An archive that moves is
   not an archive.
2. `git branch -f main origin/main` — no checkout, no `--hard`.
   `git clean -fdx` stays forbidden in that worktree.

Facts already answered on disk:

- `.github/workflows/deploy-mcp.yml` is tracked and byte-identical on
  `origin/main`.
- `mcp-server/api/index.py` and `mcp-server/api/_published.py` revert as a
  pair under the ref move.

Together those two facts mean the deployed function is unaffected by the ref
move. `_published.py`'s eventual destination is a standalone disposition
gating nothing here; it belongs to the pull-request train in section 4.

## 3. `citations.py` boundary fixes — landed

Landed 2026-08-12 on `integrate/payload-prereqs`. These ride in the first
landing of section 4, not as a second round.

- `_CFR_SECTION_CAPTURE` is ported verbatim from SpicySearch
  `identifiers.py:149`. The `_LEFT`/`_RIGHT` guards are not part of the port;
  they fix a different defect, and this module's 25 compiled patterns carry no
  boundary lookbehind at all. The port is scoped to `_CFR_STANDARD`, the one
  expression both regression cases reach — 1 of 25, as priced.
  `_CFR_TITLE_PART` and `_CFR_COMPACT` keep their own section spelling.
- The `:657` phantom loop is fixed with `(?!\d)`. The list expansion guarded
  the next citation with a negative lookahead for "CFR" and a bare `\d+`
  backtracked out from under it: on `'1CFR9,10CFR1'` the engine tried "10",
  failed the lookahead, gave back the "0", and matched "1", publishing a
  phantom `1 CFR 1` beside the two real citations. Refusing to stop mid-number
  leaves the lookahead nothing to backtrack into.
- Both regression cases are tests, both exact. Reproduced before the change:
  `'1CFR9,10CFR1'` returned three citations, `'49 CFR 900.42.'` returned none.
  Both now return exactly what they say, and
  `'40 CFR Parts 60 and 63'`, `'40 CFR 60, 61, and 63.'`, `'40 CFR 60.5-1.'`
  and the Title 3 compilation refusals are unchanged.
- `validate_body_cache` now checks the disk-to-lock direction as well. It
  walked the lock and asked whether each record's file was intact, which says
  nothing about a body on disk that the lock names nowhere — what a partial
  re-draw or an interrupted capture leaves, and what the next reader that
  globs `documents/` takes for corpus.
- The snapshot schema is at version 2, where `visibility` is a required
  artifact field. The pipeline always wrote it, but nothing required it, so a
  reader could not tell "internal" from "the producer forgot", and
  `published.py` never read it at all. `SUPPORTED_FORMAT_VERSIONS = (1, 2)` in
  both readers with the requirement keyed to the version, so a version-1
  snapshot is not held to a rule that postdates its seal.
  `mcp-server/api/_published.py` carries the same change; it is the deployed
  resolver, and leaving it at version 1 would have made it refuse every
  snapshot this build writes. Ten tests reach through
  `materialized/<dataset>/snapshots/<id>/`.

## 3a. `receipt.py` sibling-checkout default — removed

`src/spicy_regs/ontology/receipt.py:2210` defaulted `--rulespec-repo` to
`Path.cwd().parent / "rulespec"`. REF-024 forbids a product-internal path into
a sibling product regardless of destination, and a default is its worst form:
the caller who never passed the flag believes nothing about the layout, so the
wrong directory is read silently and the receipt states a provenance nobody
chose. The flag is required with no default; `build_parser()` is split out of
`main()` so the absence of a default is a fact a test reads rather than a
string a test scans for. `--spicy-repo` keeps `Path.cwd()` — the repository
the code runs inside is not a sibling escape. `build_receipt()` already took
`rulespec_repo` as a required keyword, and the one documented invocation
(`docs/rin-ontology-revision-report.md:228`) already passes the flag.

## 4. The payload lands as direct commits on local `main`

Decided by the product owner 2026-08-12: there is no pull-request train. The
payload lands as direct commits on the local `main`. No pull requests, and
with them nothing that only existed to serve a review: no per-request
blast-radius description (the 87.9% row collapse and the breaking
`comment_periods` schema are still true and still belong in the commit
messages that carry them, but they are not a review preamble), and no
raising of the pymupdf AGPL question inside a pull request. That question is
not answered here; it is unasked, because the venue it was to be asked in
does not exist.

What survives the decision:

- **The order, unchanged and still forced.** `ontology/` lands first: 14 of 18
  `docpipeline/` files import it, and `ontology/` itself carries zero
  `refspec` strings. `build_fr_docket_links.py` lands separately, on its own
  weaker evidence.
- **The REF-024 payload rule, dormant rather than repealed.** No import, path,
  or URN in a payload names an owned product. Nothing is shipping upstream, so
  the rule gates nothing today — a local commit on this repository's own
  `main` is not a crossing. It still governs any future upstream
  contribution, and the two crossings it named stay tracked so the rule has
  something to be true about when that day comes:
  - `src/spicy_regs/ontology/receipt.py:2210` is fixed — see section 3a. It
    was fixed as boundary hygiene the landing wants, not as a gate the landing
    requires.
  - `src/spicy_regs/docpipeline/rkaf_projection.py:2334` still does
    `from refspec import (...)`. It stays tracked under section 6's
    editable-install work, which is where an import of an installed `refspec`
    wheel stops being a sibling source-tree crossing at all.

## 5. `rkaf_projection.py` boundary freeze

`tests/test_rkaf_projection_boundary.py` carries the freeze, written
2026-08-11 against `src/spicy_regs/docpipeline/rkaf_projection.py` at
`6dbe181ccec7`, before the landing in section 4 moves `ontology/`. It runs in
the default suite: `testpaths = ["tests"]`, no marker, three tests, 0.1s.

The frozen surface, AST-derived from the file: 17 outbound import statements,
16 naming `spicy_regs` and 1 naming `refspec` at line 2334. The 16 reach 13
distinct `spicy_regs` modules across four subpackages — `candidate_release`,
`docpipeline`, `enrichment`, `ontology`. Three statements name
`spicy_regs.docpipeline.source`, two name `spicy_regs.ontology.common`, and
the other 11 modules are named once each. Every remaining import in the file
is standard library; the file carries no relative and no dynamic import.

The freeze records module and imported names, not line numbers or file order,
so imports may move within the file. It records every non-stdlib import, not
only the packages present today, so a new sibling product or third-party
dependency fails it too. One added crossing fails two of the three tests; a
relative or dynamic import fails the third. Each failure states that the list
is the priced boundary for the four-way split and that moving it is a
deliberate change to the freeze and to this section together.

The test shape came from `RefSpec/tests/test_atlas_index.py:490-497` and
DocSpec's `tests/test_package_boundary.py`.

## 6. The editable-install crossing

`pyproject.toml:147` sets `refspec = { path = "RefSpec", editable = true }` —
a sibling source-tree consumption REF-024 forbids. The exact pin at
`pyproject.toml:12` (`refspec==0.1.0.dev0`) is already correct. Delete the
`[tool.uv.sources]` override once the RefSpec package is consumable from an
installed wheel; RefSpec's `PLAN.md` schedules the input resolver that makes
it so. The REF-024 payload rule in section 4 does not cover this crossing —
it governs what ships upstream, and this line never ships. It is also where
`rkaf_projection.py:2334`'s `from refspec import (...)` is answered: an
import of an installed wheel is not a sibling source-tree crossing, so
deleting the override retires both at once.

## 7. Canonical-JSON profiles precede any dedup

Counted 2026-08-11 at the commits in this plan's preamble: the workspace
holds ~33 canonical-JSON implementations — 9 in `src/spicy_regs`, 7 in its
tools, 9 in `RefSpec/src`, 4 in `RefSpec/tools`, 2 in `rulespec/tools`, 2 in
SpicySearch, 2 in DocSpec. At least three profiles exist de facto:
SpicySearch `artifact_protocol.py:127-128` rejects every float and bounds
integers; SpicySearch `canonical.py:213` permits floats and has ~100 call
sites including float-bearing metric reports; SpicySearch
`atlas_search_view.py:279` encodes a third (REF-JSON).

Name the profiles before merging any implementation — RFC 8785 for artifact
identity, a float-permitting profile for reports — then converge each behind
a shared conformance vector set. Deleting `canonical.py:213` as a one-line
dedup would make eight subsystems raise on any float payload.

## 8. The applicability policy is torn against RefSpec's catalog

`policies/profile-resource-applicability-v0.json` (payload-side; the
`policies/` tree is not on `main`) pins `refspecResourceCatalog` at
`sha256:c0bcce7318ac6fedce2e0c51e77186ef32cf5c9d1039aa2170d7b28422de7dbe`,
while RefSpec's republished `portfolio/resource-catalog-v0.json` states
`catalogDigest`
`sha256:a731fef9a49b3af10813febea52a89a8e69c02b43fe6f938695bba98462b3515`.
The `profileCatalog` half of the policy set agrees; only the RefSpec catalog
moved. SpicySearch's `test_policy_inputs_cross_repository.py:78` suspends on
exactly this tear and converts to a pass when it closes.

Regenerate the applicability policy against RefSpec's catalog after the
validation-cost reset (`RefSpec/plans/validation-cost-reset-plan.md`) stops
moving that repository; regenerating against a moving catalog re-tears it.

## 9. Three document populations come home from RefSpec

Landed 2026-08-14 on `integrate/payload-prereqs`. RefSpec is removing three
Atlas units because they enumerate world-generated document populations, which
this repository's README already claims: SpicyRegs owns source acquisition and
source-addressable document structure. The shedding decision is REF-031 in
`RefSpec/docs/decisions.md`; this section cites it by identifier and does not
restate it. What follows is the SpicyRegs half — what arrived, what was built
for it, and what it is not.

### 9a. The captures, as exact publisher bytes

Five files under `sample-data/document-populations/`, byte-identical to the
captures RefSpec pinned and carrying the same digests and byte lengths:

- `cbo-119congress-cost-estimates-2026-08-04.xml`, 375,365 bytes,
  `sha256:edc957a1115320f1c0da4b02c33d1af146a3c508592ee20b4909e0a8db44d968`,
  from `https://www.cbo.gov/rss/119congress-cost-estimates.xml` at
  `2026-08-04T00:50:00Z`. 1,058 publications.
- `cbo-datadome-challenge-real-capture.html`, 770 bytes,
  `sha256:07d681cd0aa832c1132ba2b8d323693990cf27c818e8b064b0f92ebddda58e66`:
  the DataDome edge challenge `https://www.cbo.gov/cost-estimates/xml` returns
  in place of the feed that carries CBO's topic labels and fiscal facets.
- `fcc-ecfs-filings-2026-08-03.json`, 51,284 bytes,
  `sha256:4393e9c73ab5e12e25c79a707ca85856ba1d9cc1c3eccdfdfa235223f17773da`,
  from `https://publicapi.fcc.gov/ecfs/filings?limit=25&sort=date_disseminated,DESC`
  at `2026-08-03T19:20:00Z`. 25 filings, 40 proceeding embeddings, 15 distinct
  proceedings.
- `govinfo-package-summary-cfr-2023-title1-vol1-2026-08-03.json`, 1,532 bytes,
  `sha256:705a28865a4fba746e8deb4aff05a21bbd63534201e74c5320f56d505ca3d79e`,
  and `govinfo-premis-cfr-2023-title1-vol1-mini-2026-08-03.xml`, 4,268 bytes,
  `sha256:afeba6d9e48f502c911ef0ec1400accdbaa5cad5d7d056672dce6a54d1326417`,
  both at `2026-08-03T19:15:00Z`. One CFR package, and the two
  publisher-declared SHA-256 digests its PREMIS record carries.

`document-population-capture-manifest-v1.json` binds each file to its digest,
byte length, publisher URL, and observation timestamp, the way
`sample-data/mirrulations/document-release-file-manifest-v1.json` binds the
checked-in Regulations.gov pair. The captures sit beside `document-files/`
rather than inside it because they are a different kind: `document-files/`
holds one document's renditions, these hold the listings that say which
documents exist. That is where acquisition coverage is decided — a run cannot
state what it missed without a publisher-issued enumeration to miss it
against — so they land under `sample-data/` and their parsers under
`src/spicy_regs/sources/`, not in a corpus or an evaluation tree.
`.gitattributes` marks them `-text -whitespace`, extending the rule already
written there for `document-files/`: a digest a test re-derives cannot survive
git normalizing the bytes under it.

The `-mini-` in the PREMIS filename is RefSpec's and RefSpec explains it
nowhere. The file carries two file objects while the package summary lists six
rendition roles, and GovInfo is documented as computing fixity for only some
of a package's objects, so whether these are the whole PREMIS response or an
excerpt is not established here. The digest pins the bytes either way; nothing
treats the two digests as covering the package.

### 9b. What was built, and what checks it

`src/spicy_regs/sources/document_populations.py` verifies a capture against the
manifest on every read and parses the CBO feed, the GovInfo package summary,
and the GovInfo PREMIS fixity record. `proceedings_from_filings` went into
`src/spicy_regs/sources/fcc_ecfs.py` instead, next to the ECFS knowledge it
belongs to; the proceedings it returns feed the existing
`build_fcc_ecfs._shape_proceeding` unchanged, so the FCC capture reaches the
published `fcc_proceedings` column shape with no new shaping code at all.

The parsing is ported from RefSpec's readers and rewritten; nothing imports
`refspec`. The strictness is ported deliberately with it. A publisher that
changes its field set raises rather than yielding fewer records, because an
empty population and a refused request are indistinguishable to a lenient
parser, and that is the failure that loses a whole population silently. The
DataDome capture is kept as the fixture that proves the refusal: 16 tests in
`tests/test_document_populations.py` pin the counts (1,058 / 15 / 1 package /
2 fixity digests), the exact first CBO record, the 52 items CBO issues with an
empty `Bill_Number`, one shaped proceeding in full, GovInfo's own
`814758`-byte XML and `572151`-byte HTML digests, and the refusals — a
challenge body, a DOCTYPE, a drifted item shape, a repeated publication URL, a
conflicting proceeding identity, fixity for another package, a non-CFR
collection. Mutating one byte of the FCC capture fails three of them.

### 9c. No `cbo-publication-v1` profile, and why that is not an omission

`policies/source-profile-catalog-v0.json` is generated from
`src/spicy_regs/source_profiles.py`, and every one of its 17 profiles names a
`sourceTable` this repository actually publishes and documents. CBO has no
reader, no transform, no rollup pipeline, no schema, and no entry in
`data_dictionary.TABLES`, so a `cbo-publication-v1` profile would name a table
nothing produces.

The generator refuses it outright, which is the useful part: adding the profile
and running `tools/generate_source_profile_artifacts.py --write` fails with
`applicability profile coverage differs; missing=['cbo-publication-v1']`. The
entry cannot be written without also authoring a row in
`policies/profile-resource-applicability-input-v0.json` — a claim about which
RefSpec catalog resources CBO publications are source-natively related to, for
which these captures are no evidence. Measured, not assumed: the profile was
added, the generator run, and the change reverted; `policies/` and
`source_profiles.py` are byte-identical to `HEAD`.

`fcc-proceeding-v1` and `gao-report-v1` already exist and are untouched. The
FCC capture is a real-bytes regression for a profile and a table that were
already wired, which is why it needed no policy change to be worth landing.

### 9d. What did not arrive

- **The populations.** These are seed captures, not acquisitions. One CBO feed
  file covers one Congress; one ECFS filing page names the 15 proceedings its
  25 filings happened to touch, not ECFS's ~20,000; one GovInfo package is one
  volume of one title of one year. `FccEcfsProceedingsReader` can already walk
  the whole ECFS proceeding population, and `CfrSectionsReader` walks GovInfo
  CFR granules; neither was run here. CBO has no reader at all — only the feed
  URL, the parse, and the knowledge that `cost-estimates/xml` is walled while
  the per-Congress files are not.
- **Package fixity as a published fact.** The PREMIS parse yields GovInfo's own
  digest for a rendition SpicyRegs could download, which is the check that
  would let a CFR volume be verified against its publisher rather than against
  ourselves. Nothing consumes it yet; `sources/cfr_sections.py` deliberately
  reads granule metadata and not package bodies.
- **A `cbo_publications` table.** Per 9c, that is the whole chain — reader,
  transform, rollup, schema, dictionary entry, applicability row — and it is
  not started.

## 10. Documented enumerations become a gate here

Landed 2026-08-14 on `integrate/payload-prereqs`. RefSpec is removing ~29
"observed inventory" Atlas units, the largest being the 14-unit
`regulatory-native-*` family, which was distinct-value scans over four of this
repository's published Parquet tables. The shedding decision is REF-032 in
`RefSpec/docs/decisions.md`; this section cites it by identifier and does not
restate it.

Unlike section 9, almost nothing needed porting: the scanned data is already
ours, and `agency_stats` already publishes the agency-code inventories two of
those units enumerated. One thing was not already ours, and it is the only thing
those inventories ever produced that was worth keeping — the *comparison*. A
distinct-value scan is trivia until it is set beside what the publisher said the
values would be; then it is the difference between an enumeration and a claim.
Nothing in either repository ran that comparison. It runs here now.

### 10a. The check

`tools/check_source_domain_drift.py` diffs the value list a publisher documents
for a column against the values that column actually carries, in both
directions, for six columns of three published tables. It is a gate: exit 1 when
either direction produces something unrecorded.

**The documented half is parsed, not transcribed.** Two publisher documents are
checked in under `sample-data/source-domains/`, byte-identical to the captures
RefSpec pinned and carrying the same digests and byte lengths, bound to their
publisher URL and observation time by
`documented-enumeration-capture-manifest-v1.json` the way section 9a's captures
are bound:

- `regulations-gov-openapi-v4-2026-08-03.yaml`, 60,826 bytes,
  `sha256:be43c866f5ca424a456bde36ea03cb9326c454ef4e1894a13df80b6dc6e22488`,
  from `https://open.gsa.gov/api/regulationsgov/v4/openapi.yaml` at
  `2026-08-03T19:13:12Z`. Its `components.schemas.DocumentType.enum` (lines
  893-898) and `.DocketType.enum` (lines 902-904) are the only closed lists in
  it that govern a column we publish.
- `reginfo-rin-data-ver10262011.xsd`, 22,730 bytes,
  `sha256:94fdcf4b382830cc44b9956c00439dc20a9643de402c298cee71293a14153b24`,
  from `https://www.reginfo.gov/public/xml/REGINFO_XML_Ver10262011.xsd` at
  `2026-08-03T19:15:15Z`. Four `xs:documentation` sentences —
  `PRIORITY_CATEGORY` (line 50), `RIN_STATUS` (line 58), `RULE_STAGE` (line 66),
  `MAJOR` (line 74).

`src/spicy_regs/sources/source_domains.py` re-verifies both digests on every
read and parses the values out of the bytes each run, with the value count and
the raw option count pinned per domain. A hand-typed list rots silently; a parse
against a pinned digest cannot, and re-pinning a fresher capture makes the
publisher's own change show up as a test failure rather than as nothing.

Worth stating plainly, because it is what makes the reginfo half prose rather
than schema: **the XSD contains no `xs:enumeration` anywhere.** Every one of
those fields is declared `<xs:restriction base="xs:string"/>` with no facets.
The controlled list exists only in a sentence, which the parser reads and
refuses if it does not recognise. The publisher's prose also repeats itself —
`PRIORITY_CATEGORY` lists `Not Major` twice — so a literal duplicate is folded
into one value and the raw count is kept beside the distinct one.

**The observed half is a derived summary with its own provenance.**
`observed-domain-snapshot-2026-08-03.json` is 3,733 bytes and holds, per column,
the distinct values with row support, the null count, and the table's row count.
It names its inputs by digest: `dockets.parquet`
(`sha256:b14cd488…`, 276,326 rows), `documents.parquet` (`sha256:bb42f79e…`,
1,990,136 rows) and `unified_agenda.parquet` (`sha256:e6862d5d…`, 3,954 rows),
all from `https://r2.spicy-regs.dev/` at producer revision `f1fcb8c9c883` —
this repository's own `origin/main` per section 2. Those three tables are 70 MB
and are not checked in; that is the whole reason a 3.7 KB summary of their
2,270,416 rows exists. The tool regenerates it from any directory of published
Parquet with `--observe --write-snapshot`, so the code that writes the fixture
is the code that checks it, and `--write-snapshot` refuses to run without an
observation timestamp and a producer revision.

**Two directions, because they mean different things.** An *undocumented* value
says the publisher's documentation is incomplete, and a consumer that switches
on it mishandles those rows. An *unobserved* value says either the snapshot is
bounded — one semiannual agenda edition need not exercise every documented stage
— or the publisher retired a value without saying so. Neither is automatically
an error and neither is automatically fine, so both are carried in
`ACCEPTED_DOMAIN_FINDINGS`, closed in both directions in the shape
`tests/test_docpipeline_retrieval_migration.py:_assert_ledger_exact` already
uses: an unrecorded finding fails, and a recorded finding the data stopped
producing fails just as hard, because an exception nothing exercises is a claim
nobody checked.

`tests/test_source_domain_drift.py` runs it in the default suite — 29 tests,
1.0s, no network and no Parquet. Eighteen assert the check and the publisher
facts it rests on; eleven break an input and require it to fire. Adding one
undocumented value to the committed
snapshot fails seven of them and takes the tool to exit 1 with
`regulations-gov-docket-type undocumented-value 'Rulemaking - Legacy' is not in
ACCEPTED_DOMAIN_FINDINGS`. Changing one byte of the OpenAPI capture fails
earlier still, at the digest.

**What is deliberately out of scope, so the six are a decision and not an
accident.** `submitterType` (OpenAPI lines 905-911) governs `comments.category`,
and the snapshot the observed half is drawn from carries no comments table; a
documented domain with nothing to observe is not a check. `TTBL_ACTION` (XSD
line 443) documents 34 timetable actions, but the same snapshot's
`timetable_json` carries 1,139 distinct actions over 10,533 entries — the
publisher's own data treats that field as free text, and a gate against its list
would report a thousand findings and gate nothing.
`federal_register.document_type` is absent for a third reason: no pinned
publisher document states its list, and a domain nobody published is not a
documented domain.

### 10b. The findings

Seven findings over six columns; two columns agree completely, which matters,
because a check that only ever finds drift is not a check.

**`Public Submission` — undocumented, 373 rows.** regulations.gov returns this
`documentType` on 373 of 1,990,136 document rows and labels it in its own web
UI, but the pinned v4 OpenAPI `DocumentType` enum lists five values and does not
include it: `Notice`, `Rule`, `Proposed Rule`, `Supporting & Related Material`,
`Other`. "Public Submission" appears nowhere in the 60 KB document. The
documentation is incomplete; the data is not wrong. This is the finding
RefSpec's inventory produced and the reason the comparison was worth keeping.

**`No Stage` and `Not Major` — documented, unobserved.** Agenda edition 202510
is the only edition in the snapshot, and it carries five of the six documented
`RULE_STAGE` values and five of the six documented `PRIORITY_CATEGORY` values.
The absences are edition-boundedness, not retired values: an edition states the
stages its 3,954 RINs are in, and `unified_agenda` is keyed `(rin,
agenda_edition)` and accumulates editions, so this is a fact about the snapshot
rather than about the publisher. Recorded rather than ignored so that a *second*
edition landing without them would be visible.

**`rin_status` case drift — undocumented, 3,954 rows, and new here.** Not one of
RefSpec's findings; the check found it on its first run. The XSD documents
`"First time published in the Unified Agenda"` and `"Previously published in the
Unified Agenda"`. Every row carries the title-cased forms — `First Time
Published in The Unified Agenda` (1,119 rows) and `Previously Published in The
Unified Agenda` (2,835). Same values, different bytes, on the whole column: a
consumer built by copying the schema documentation matches no row at all. It is
recorded
in all four directions — two undocumented, two unobserved — because that is what
the data says.

**Two column descriptions are wrong, and this is how we know.**
`data_dictionary/descriptions.yaml:236` says `rin_status` is
"e.g. `Active`, `Completed`, `Long-Term`". None of those three strings occurs in
the column, in the XSD, or anywhere in the reginfo export. Line 240 says `major`
is "e.g. `Yes`/`No`. Often null" — the column is `Undetermined` on 865 of 3,954
rows (22%) and null on none. Recorded, not fixed: the dictionary's prose is not
what this gate is for, and correcting it pulls in a regeneration of
`docs/tables/*.md` that belongs to its own change. It is here because it is the
clearest evidence that the comparison earns its keep — nothing else in the tree
would have caught it.

**715 unresolved agency names in `federal_register.agencies_json` — a defect.**
This is what RefSpec's deleted "unresolved-agency-name" unit was looking at, and
it is a real data-quality defect rather than a vocabulary.

The evidence, from `federal_register.parquet` at `sha256:702018767f73b914…`
(800,619 rows): the column holds 1,289,014 agency entries, of which 22,181 carry
no `slug`, spanning 715 distinct `raw_name` values. Every unresolved entry
carries **only** `raw_name` — no `name`, no `id`, no `url`, no `parent_id` — so
federalregister.gov itself did not resolve the printed agency heading to an
agency record and returned the heading text alone. What lands in the column is
therefore what was printed at the top of the document, and a large minority of
it is not an agency name at all:

- 44 distinct values carry a stray `]`, the tail of a bracketed docket line:
  `Docket No. FR-4675-N-01]`, `EPA-HQ-OAR-2016-0546; FRL-9969-55-OAR]`,
  `Investigation No. 337-TA-1096]`, `OMB 3060-0760]`.
- 3 are CFR citations: `44 CFR Part 64` (document `2011-15520`),
  `Coast Guard CFR 33 CFR 165` (`03-14022`),
  `National Oceanic and Atmospheric Administration 50 CFR Parts 223 and 224`.
- 41 are longer than 80 characters and are plainly document titles: *"Vermont
  Yankee Nuclear Power Corporation; Notice of Consideration of Issuance of
  Amendment to Facility Operating License…"* at 212 characters.
- 1 is the bare word `Rule`, on three documents (`00-3`, `00-7`, `00-53`).
- Some are simply the publisher's typos: `DEPARTMENT OF HOUSING AND URBAN
  DEVELPMENT`.

Where it enters, on our side: `transforms/build_federal_register.py:92` stores
the API's `agencies` array verbatim (`json.dumps(agencies)`) and `:82` builds
`agency_slugs` as `",".join(a["slug"] for a in agencies if … a.get("slug"))` —
which drops every unresolved entry without a word. So the residue is upstream
(FR's own parse of the printed heading) but the *defect here* is that we
propagate it unvalidated and then discard it silently: 95 documents have a
non-empty `agencies_json` and a NULL `agency_slugs`, and are invisible to every
agency-faceted consumer with nothing recording that they were dropped.

Fixing the ingestion is out of scope for this landing. It is named here as a
defect with its evidence: a `raw_name` that resolves to nothing is a parse
residue, not an agency, and the transform should say so — quarantine it the way
`tools/build_agency_crosswalk_artifact.py` already quarantines malformed
crosswalk rows — rather than let it vanish into a dropped join.

**The agency-code inventories were already ours.** RefSpec's
`regulatory-native-regulations-gov-document-agency-code`,
`…-docket-agency-code` and `…-unified-agenda-agency-code` units enumerated
distinct `agency_code` values from tables this repository publishes.
`agency_stats` is that inventory with support attached: one row per agency code,
built as the `UNION` of distinct `agency_code` across `dockets`, `documents` and
`comments_index` (`transforms/build_agency_stats.py:46-97`), carrying
`docket_count`, `document_count` and `comment_count`. It is a published table,
MCP-queryable, documented at `docs/tables/agency_stats.md`, and refreshed by its
own rollup workflow. Nothing was lost and nothing needed porting.

### 10c. GAO: nothing lands now

RefSpec is also removing its GAO product-topic observation and the report
witness. The decision here is (ii) — record that `gao-report-v1` covers the
document class, and let the topic observation arrive when the pipeline actually
ingests GAO products. Nothing is ported.

The reasoning is the "structure must earn its keep" test, and GAO fails it three
ways at once:

- **Nothing here would consume it.** `gao_reports.parquet` already carries a
  `topics_json` column, and it is `[]` on every row *by design*: the RSS feed at
  `https://www.gao.gov/rss/reports.xml` is the only anonymously machine-readable
  GAO surface and it carries no topic tags
  (`transforms/build_gao_reports.py:18-21`). A parser for a product page's
  Topics field would populate nothing, because no product page ever reaches it.
- **There is no acquisition path to feed it.** RefSpec's
  `registry/gao_topics.py` parses one already-captured product page and states
  that unattended live acquisition still needs an injected Zyte-backed proxy
  fetcher, because gao.gov's Akamai edge returns "Access Denied" to a plain
  client. This repository has no Zyte transport — `grep -ri zyte src/ tools/
  tests/ pyproject.toml` returns nothing — and `sources/gao_reports.py` already
  documents GAO's sitemap, product JSON and search endpoints as bot-blocked. A
  ported parser would be a parser with one 2026-08-04 capture and no way to get
  a second.
- **The observation is capture-local by RefSpec's own account.** Every GAO topic
  observation carries an empty `identifiers` list, because gao.gov links an
  assigned topic only to a navigational `/topics/<slug>` page and publishes no
  stable topic code or IRI, and the 1998 GAO Thesaurus that once assigned codes
  is retired. There is no publisher identifier to port.

So the honest statement is the small one: `gao-report-v1` in
`source_profiles.py` covers the GAO product as a document class and is
untouched; `topics_json` stays reserved and empty; and the topic observation is
worth building on the day a GAO acquisition path exists to make it a population
rather than a single page. Building it today would add a parser, a capture, and
a test suite that no code path reaches — the unconsumed structure section 9c
already refused once for CBO.

### 10d. What this is not

- **Not a schema.** The gate reports; it does not constrain. No transform
  validates a value against a documented domain, and none should until there is
  a stated policy for what to do with a row that fails — dropping publisher data
  because the publisher's own documentation is incomplete would be worse than
  the drift.
- **Not a claim about the whole corpus.** The observed half is one dated
  snapshot of three tables at one producer revision. It can prove a value
  *occurs*; it can only prove a value is *absent from that snapshot*, which is
  precisely why the two agenda findings are recorded as edition-boundedness
  rather than as retirements.
- **Not a second home for RefSpec's units.** Six columns are checked because six
  columns have both a pinned publisher document and an observation. The other
  ~23 removed units enumerated things with neither, or things `agency_stats`
  already publishes. Re-creating them here would rebuild the inventory REF-032
  is removing, one repository to the left.
- **Not wired into CI.** The gate runs in the default pytest suite, which is
  where the `source_profile_artifacts` gate also lives; no CI job invokes the
  tool directly. A `.github/workflows/ci.yml` job beside `data-dictionary` is
  the obvious next step and is not taken here.
