# Disposition of the archived spicy-regs work

Written 2026-09-04. One record of where every piece of the July–August 2026
research program now lives, what replaced it, and what still needs a home. It
replaces three earlier documents that disagreed with each other and with the
code; see *Supersedes*.

Every claim below was checked against a pinned commit, not read from a
document. The pins:

| repo | ref | commit |
|---|---|---|
| spicy-regs | `origin/main` (live upstream) | `1f02a7f` |
| spicy-regs | `archive/landing-final` (was local `main`) | `9a79569` |
| spicy-regs | `integrate/payload-prereqs` | `8d9e7a2` |
| spicysearch | `main` | `fc1f5fa` |
| DocSpec | `main` | `0bb2add` |
| spicy-docs | `main` | `9f8c7ee` |
| RefSpec | `main` | `da4fe055` |
| rulespec | `main` | `a87d839` |

## Where the work is

Two branches carry all the content. They forked from `origin/main` at
`01ecbef` (2026-08-19) and were cut one day apart from the same program.
Neither removes anything from upstream; the nine files they "lack" are what
upstream added after the fork (`org_committee_links`, `seed_dockets_catalog`,
`check_purge_credential`).

| branch | commit | date | vs `origin/main` | holds |
|---|---|---|---|---|
| `archive/landing-final` | `9a79569` | 2026-08-29 | +609 files | the shared core plus `source_catalog/`, `universes/` |
| `integrate/payload-prereqs` | `8d9e7a2` | 2026-08-28 | +649 files | the shared core plus source-native, courts, bills, `public_table` |

Both are on `github.com/mikewolfd/spicy-regs` (a fork; remote `fork`), with
four older snapshots whose tip trees are copies of one or the other:
`archive/landing-main-pre-reorg` `31a4bfe`, `archive/integrate-payload-prereqs-pre-reorg`
`a6ab98a`, `archive/pre-strip-2026-08-26` `57d46bf`, `backup/pre-marker-fix`
`bc8f534`, `feat/rkaf-boundary-freeze` `a8938b4`. Nothing is single-disk.

Fifteen `src/` files exist on both branches with different content. Only two
matter after the dispositions below — `sources/supreme_court_opinions.py` and
`transforms/build_supreme_court_opinions.py` — and only when the courts PR is
cut. The rest belong to buckets that drop.

Neither branch has PRs #181, #182, or #183. Both carry the pre-fix
`uscode_uslm.py` (no memory bound) and `integrate` carries the pre-fix
`source_domains.py` (fabricated publisher URL). Landing `integrate` wholesale
would reintroduce both.

## Dispositions

**Drop** means: the code stays on the fork, nothing is ported, and the check
in the last column is the reason. **PR** means cut a branch from
`integrate/payload-prereqs` onto `origin/main` and treat it like #181 — rebase,
gate through `uv run`, validate against real data, one-paragraph description.
**Port** means the work belongs in another repo and goes through that repo's
own intake.

### A. Already re-homed — drop

| bucket | branch | files | replaced by | check |
|---|---|---|---|---|
| `source_native*.py`, `schemas/source_native_release/1.0/` | integrate | 22 | spicy-docs `src/spicy_docs/source_native.py` and siblings, shipped `70a5b16` 2026-08-29 | integrate's copy is byte-identical to `ff8d202`, the extraction point; zero commits touched it since |
| `sources/uscode_uslm.py`, `uscode_olrc.py` | both | 2 | RefSpec `tools/build_usc_source_credits.py` (`5b8f4d8b`), `registry/act_resolution.py` | both copies got the `element.clear()` memory bound independently: spicy-regs `5ecfd5a`, RefSpec `93d244a3` |
| `ontology/citations.py` | both | 1 | RefSpec `registry/iri_minting.py` (`582461fe`) — seven minters, 1,570 test lines | one minter did not travel; see D |
| `ontology/act_index.py` | both | 1 | RefSpec `registry/act_resolution.py`, which names it as provenance | — |
| `transforms/build_authority_edges.py` | both | 1 | RefSpec `registry/unified_agenda_parquet.py` | — |
| `document_release.py` (M1) | both | 1 | DocSpec `tests/fixtures/document_release_v2{,_docspec}/` (`bdab0b8`, 2026-08-30) — 49 sealed cases, FR-2026-03227 included | — |
| `document_release_v3*.py` (6) and `fixtures/releases/` (56) | both | 77 | DocSpec DocumentRelease 2.0: `src/docspec/schemas/document_release/2.0/`, `adapters/document_release_verify.py`, ADR `docs/decisions/0001-document-release-2-0.md`, three mint receipts | the two directories are one bucket; earlier counts listed them twice |
| `docpipeline/source.py`, `segments.py` | both | 2 | DocSpec `processing/bounded_segmentation.py`, `retention_floors.py` (`be3c865`, `9bcc9ba`, 2026-08-30) | DocSpec addresses UTF-8 bytes; spicy-regs addressed codepoints. Region ids do not compare equal across the two. |
| `sources/source_domains.py` | integrate | 1 | archived on `feat/source-domain-drift-gate-revived` `f8e9e35` (PR #192, closed) | the archived copy has the corrected publisher URL; integrate's does not |
| `enrichment/accepted_output.py` | landing-final | 1 | nothing | imports `refspec.accepted_output`, which RefSpec no longer provides (`9a79569` commit message says so) |
| `enrichment/managed_release.py` | landing-final | 1 | RefSpec `src/refspec/managed_release.py` (2,638 lines) — this was a consumer wrapper over it | — |

### B. Unowned — PR to spicy-regs

Absent from DocSpec, spicysearch, and spicy-docs at the pinned commits. Each
was grepped in all three.

| bucket | files | lines | why it matters | what downstream has instead |
|---|---|---|---|---|
| `sources/document_populations.py`, `sample-data/document-populations/` | 2 + 7 captures | 468 | the coverage denominator. Its docstring: "you cannot state what a run missed without a publisher-issued enumeration to miss it against." Digest-pinned captures of CBO, FCC ECFS, GovInfo PREMIS; refuses a bot-challenge body rather than yield an empty population. **No consumer anywhere.** | nothing |
| `public_table.py`, `public_table_profiles.py`, `publication.py` | 3 | 1,230 | the Parquet/DuckDB/Iceberg **producer**. The 2026-08-27 sweep ruled the public-view path "not safe to abandon." | spicy-docs has a **consumer** (`spicy_regs_public_tables_source_native.py`) whose supply-precedence ruling assumes these tables keep existing. No producer. |
| courts: `sources/courtlistener_bulk.py`, `transforms/build_court_opinion_{bodies,clusters}.py`, `court_scope.py`, `pipelines/court_opinion_{bodies,clusters}.py`, `sources/supreme_court_opinions.py`, `transforms/build_supreme_court_opinions.py`, `pipelines/supreme_court_opinions.py`, `transforms/pdf_text_pymupdf.py` | 10 | ~2,000 | rollup pipelines that emit parquet. A real run exists: `output/court-data-2026-08-22/`, 5.7 GB. | spicysearch consumes pinned court parquet and tags it (`derived_topic_passes/postings.py`); no fetcher, no PyMuPDF dependency. spicy-docs took `courtlistener_bulk.py` as a reader (`9c75f0b`) and built nothing on it. DocSpec: zero hits. |
| `sources/bill_subjects.py`, `transforms/enrich_bill_subjects.py`, `pipelines/bill_subjects.py` | 3 | 722 | wired with a cron entry point and workflow | nothing |

When cutting the courts PR, diff `sources/supreme_court_opinions.py` and
`transforms/build_supreme_court_opinions.py` between `8d9e7a2` and `9a79569`
first; they differ.

### C. Refused on the record — drop

| bucket | files | lines | who refused, where |
|---|---|---|---|
| `docpipeline/{extraction,runtime,tag_task,relation_task,executor}.py`, `docpipeline/adapters/` (Anthropic, OpenAI, OpenAI-compatible, Codex CLI, Docling, sentence-transformers) | 11 | — | rulespec `spec/rulespec-releases.md:340-361` §7 parks approval, selection, and baseline-validation execution with no owner and rejects the inference that the Extrapolator inherits them. `docs/decisions.md:170-175` (ADR 2026-08-02): "no producer of an `ExtrapolationRelease` outside the fixture path … do not implement." |
| `docpipeline/retrieval.py` | 1 | 5,479 | spicysearch reimplemented it as three lanes plus one optional (`src/spicysearch/search_application.py`, `POST /v1/search`). The interface the manifest named, `search_documents` / `SearchResultSet`, has zero hits in spicysearch source. |
| `ontology/concepts.py`, `concept_dimensions.py`, `transforms/build_concept{s,_assignments,_events}.py` | 5 | — | RefSpec REF-053 (2026-08-31): "Open-vocabulary concept lifecycle stays unported — no owner, no consumer, no check." |
| `candidate_release.py` | 1 | — | reads `ATLAS_FORMAT = "refspec-vocabulary-atlas-nquads-1.0"` (line 24). RefSpec retired Atlas 1.0 and 2.0 at `5c6d889a`, 2026-08-09. No producer of that format exists. |
| `docpipeline/rkaf_projection.py` | 1 | 3,124 | rulespec's refusal above. Also: `from refspec import` at line 2334 violates spicysearch `docs/decisions/0001-four-product-boundary.md:56-57`; its `RulespecCoreRelease` pin `urn:rulespec:core:5ac6ba59…` was re-keyed by rulespec `8bce779` on 2026-08-11. The largest single module in this group. |
| `feat/rkaf-boundary-freeze` fixture schemas | 26 | — | a superseded v3 fixture layout; nothing references them by name; `9a79569`'s 645 tests pass without them |

### D. Genuine gaps — port

| bucket | lines | to | why |
|---|---|---|---|
| `ontology/rulespec_release.py` | 178 | RefSpec | an executable publish gate that recomputes the L0 contract digest from the tagged rulespec archive and refuses on mismatch. RefSpec's pin is verified only against its own `RULESPEC_DEPENDENCY_SHA256`; `profiles/rulespec-dependency.json` says `releaseAvailability: localUnpublished`; `vendor/README.md` calls rc18 a provisional pin from an unmerged branch. |
| `canonical_usc_chapter_iri` and its lowercase-suffix rule (`citations.py:404-418`) | ~15 | RefSpec `iri_minting.py` | the one URN producer of eight that the port left behind |
| `ontology/attestations.py` | — | RefSpec, when REF-058 reopens | RefSpec considered and deferred `rkaf:Attestation` alignment. Hold on the fork until then. |

### E. No owner, no refusal — decision pending

Nothing downstream took these and nothing on the record rules them out. The
call is between dropping them with the evidence preserved on the fork, or
holding them as gaps to fill.

| bucket | files | note |
|---|---|---|
| `ontology/ann_index.py`, `candidate_channels.py` | 2 | RefSpec's ledger books them as spicysearch's. spicysearch has zero hits for usearch, hnsw, faiss, annoy, or `candidate_channels`. The older manifest's "inventoried-not-moved" was right. |
| `corpora/` | 16 | six console scripts point into it. No document addresses it. |
| `ontology/{ledger,invariants,receipt,relation_findings,evaluation,common,adapters,subjects,segmentation,checkpoint,llm,codex_cli}.py` | 12 | RefSpec ported one of five functions from `invariants.py` (`2b4960e1`). `ledger.py` is out of RefSpec's scope by REF-022. The rest are unaddressed. |
| `evaluation_boundary.py`, `rulespec_testbed.py`, `evaluate_tag_quality.py`, `source_profiles.py`, `source_profile_artifacts*.py`, `published.py`, `document_file_pipeline.py`, `pipelines/{materialized,ontology_dataset}.py` | 10 | `published.py` reads the ontology tables at `data.spicy-regs.dev/materialized/ontology/latest.json`, which has returned 404 since at least 2026-08-27. The `materialize-ontology` workflow is not on `origin/main`. |
| `transforms/{build_comment_periods,build_proceedings,build_regulatory_agenda,build_rule_targets}.py` | 4 | RefSpec's ledger assigns `build_rule_targets` to DocSpec's scope; DocSpec has nothing. |
| `universes/` | 3 JSON | 600 lines of the archived `PLAN.md` describe them |
| `docs/superpowers/specs/` (20), `docs/evidence/` (202), 12 of 14 notebooks | 234 | provenance for the decisions above. Stays on the fork either way. |

### F. `source_catalog/` — drop the code, keep the namespace

Ten files, 7,239 lines, on `archive/landing-final` only. DocSpec owns it:
`src/docspec/domain/source_catalog.py`, `schemas/source_catalog/1.0/`, about
1,800 live lines. The commit that added it (`4e9d192`) says "This is superseded
work. DocSpec owns SourceCatalog."

But DocSpec pins `urn:spicy-regs:source-catalog-release:v1:` in **83 files**
(`adapters/document_release_verify.py:138`, with a sealed
`catalog-pin-mismatch` refusal case). That URN prefix is DocSpec's contract
now. Never reuse it in spicy-regs for anything else.

## Supersedes

Three documents tried to do this before. Each was read against the pinned
commits above; none survives as an authority.

**`docs/migration/spicysearch-product-migration-manifest.json`** (spicy-regs,
2026-08-29; on `9a79569` and `8d9e7a2`, not on `origin/main`). Two copies exist
under one `manifest_version`, and they contradict each other on items 1, 6, and
16 and on `status`. Its own `retirement_authorized` is `false`; the archived
`PLAN.md` says it "cannot authorize deletion or shutdown by itself." Six of nine
RefSpec paths it names do not exist. Three destinations it assigns to the
rulespec Extrapolator were refused by rulespec in writing (C above). Its
pinning test (`tests/test_document_release.py:615-663`) asserts that sixteen
strings appear in the file it just read.

**`plans/2026-08-31-refspec-intake-ledger.md`** (RefSpec, last edited
2026-08-31 17:14, `18bf8a3d`). All six §1 ports landed on 2026-08-31 —
`5b8f4d8b` 03:52, `582461fe` 04:12, `2b4960e1` 21:38 — four of them before the
ledger's last edit. It still says none has. §2.2 was ruled the same day
(REF-053); the ledger still says "until ruled." §5 item 3 books `ann_index` and
`candidate_channels` as spicysearch's; spicysearch has neither.

**`docs/history/2026-09-01-script-product-disposition.md`** (spicysearch). Its
five deletion gates govern three spicysearch scripts and release no spicy-regs
file. Its premise that the source-native gate "has no survivor in any product"
was false three days before it was written (spicy-docs `70a5b16`). It says the
landing branch is "not merged"; `dc89c65` is an ancestor of `main`. It is
silent on nine of the thirteen buckets above.

All three defer to a "platform value ledger" or "platform-value validator" as
the reconciling authority. `git grep` for those terms at `fc1f5fa` returns no
files.

RefSpec and spicysearch are not pushed from this checkout. Each of those two
documents needs a one-line note pointing here, through its own repo's lane.

## Closing the checkout

In order, once B has been cut and D has been handed over:

1. Remove the worktrees for closed PRs: `spicy-regs-pr/feat/uscode-uslm-source-credits`, `spicy-regs-pr/feat/source-domain-drift-gate`. Their branches stay on origin.
2. Delete the local branches that are on the fork: `archive/*`, `backup/pre-marker-fix`, `feat/rkaf-boundary-freeze`, and `integrate/payload-prereqs` (also on origin).
3. Reset local `main` to `origin/main`. The archived `main` is `archive/landing-final` on the fork; its README banner declaring this checkout superseded goes with it.
4. What remains: one checkout tracking `origin/main`, and a worktree per open PR.
