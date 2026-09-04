# Disposition of the July–August research program

Written 2026-09-04. One record of what the program built, what each piece gives
a user of the platform, and where each piece now lives. Decisions were taken
2026-09-04 by the platform owner; this document records them.

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

## What the program was

Between 2026-07-23 and 2026-08-29 a document-AI research program ran on this
repository: read federal documents, segment them, tag what they are about,
extract how rules relate, and publish the result in a portable graph format
that other products can consume. It measured everything it did — 202 evidence
files, 20 design specs, 14 notebooks — and it left behind about 270,000 lines
across two branches.

Users of the platform ask four kinds of question this program answers:

- *Follow one rule* — from the Unified Agenda through its docket, CFR parts,
  RIN, comment periods, and final rule, across the four identifier systems the
  government uses for it.
- *What is this document about* — in words no 1995 thesaurus anticipated.
- *Which cases touch this rule* — court opinions cite statutes, not CFR parts,
  and nothing joined them.
- *Is this all of them* — whether a result set is complete or the platform
  never fetched the rest.

The sections below say which piece answers which question and where it ships.

## Where the work lives

Two branches carry the content. They forked from `origin/main` at `01ecbef`
(2026-08-19) and were cut one day apart from the same program. Neither removes
anything from upstream; the nine files they lack are what upstream added after
the fork.

| branch | commit | date | vs `origin/main` | holds |
|---|---|---|---|---|
| `integrate/payload-prereqs` | `8d9e7a2` | 2026-08-28 | +649 files | the shared core plus source-native, courts, bills, the table publisher |
| `archive/landing-final` | `9a79569` | 2026-08-29 | +609 files | the shared core plus `source_catalog/`, `universes/` |

Both are on `github.com/mikewolfd/spicy-regs` (a fork; remote `fork`) with
four earlier snapshots whose tip trees are copies of one or the other:
`archive/landing-main-pre-reorg` `31a4bfe`, `archive/integrate-payload-prereqs-pre-reorg`
`a6ab98a`, `archive/pre-strip-2026-08-26` `57d46bf`, `backup/pre-marker-fix`
`bc8f534`, `feat/rkaf-boundary-freeze` `a8938b4`. Everything has a remote.

Fifteen `src/` files exist on both branches with different content. Two matter:
`sources/supreme_court_opinions.py` and `transforms/build_supreme_court_opinions.py`,
when the courts PR is cut. The rest belong to pieces that ship from one branch
only.

Neither branch has PRs #181, #182, or #183. Both carry the earlier
`uscode_uslm.py` and `integrate` carries the earlier `source_domains.py`; the
corrected versions are on their own branches, and the PRs below cut from
`integrate` by file, never wholesale.

## Ships to spicy-regs

Each of these becomes one PR cut from `integrate/payload-prereqs` (or
`archive/landing-final` where noted) onto `origin/main`, with the same
treatment as #181: rebase, gate through `uv run`, validate against real data,
one-paragraph description. In this order.

### 1. The rulemaking join surface

`transforms/build_rule_targets.py`, `build_proceedings.py`,
`build_regulatory_agenda.py`, `build_comment_periods.py`, with `published.py`
and `pipelines/materialized.py` that serve their tables. On both branches.

**What a user gets.** Today the platform pairs a proposed rule with its final
rule (`rulemaking_lifecycles`) and links Federal Register documents to dockets
(`fr_docket_links`). It cannot follow one rule end to end, because a rule is a
RIN on reginfo, a docket on regulations.gov, a set of CFR parts in the Code, and
a document number in the Register, and nothing joins the four. These transforms
build that spine — docket ↔ CFR ↔ RIN — then promote each rulemaking to a
first-class proceeding with its actions, link agenda items to those actions,
and materialize every comment period including reopenings. A user follows one
rule from agenda to final under every name the government gives it.

This is the product the platform was built to be.

### 2. Courts

`sources/courtlistener_bulk.py`, `transforms/build_court_opinion_bodies.py`
(356 lines), `build_court_opinion_clusters.py` (410), `court_scope.py` (348),
`pipelines/court_opinion_{bodies,clusters}.py`, `sources/supreme_court_opinions.py`,
`transforms/build_supreme_court_opinions.py`, `pipelines/supreme_court_opinions.py`,
`transforms/pdf_text_pymupdf.py` (171). Ten files, about 2,000 lines, on
`integrate`. A real run exists: `output/court-data-2026-08-22/`, 5.7 GB.

**What a user gets.** Court opinions cite statutes, not CFR parts, so a user
asking "which cases touch this rule" gets nothing today. These pipelines bring
CourtListener's bulk dumps and the Supreme Court's own opinion PDFs into the
same tables as everything else — opinion text, cluster identity, and the court
that decided (which the cluster dump omits) — so the U.S.C. bridge can join
litigation to rulemaking. spicysearch already tags court opinions it is handed
as pinned parquet; this is what produces that parquet.

Ships whole. The 21 tables on `origin/main` each keep their fetch beside their
transform; courts follows the same shape.

### 3. Bill subjects

`sources/bill_subjects.py`, `transforms/enrich_bill_subjects.py`,
`pipelines/bill_subjects.py`. 722 lines on `integrate`, already wired with a
cron entry point and workflow.

**What a user gets.** Every bill carries a CRS policy area and legislative
subjects that Congress.gov assigns. With them in the tables, a user filters
bills by topic and relates bills to rules by subject — the first link between
the legislative and regulatory halves of the corpus that does not go through a
statute citation.

Ships whole, same reasoning as courts.

### 4. The table publisher

`public_table.py`, `public_table_profiles.py`, `publication.py`. 1,230 lines
on `integrate`.

**What a user gets.** Everything a user touches — the Parquet files at
`data.spicy-regs.dev`, the MCP server, the app — is a table something built.
Today that something is 21 hand-written scrapers. This is the path that builds
the same tables from verified, digest-pinned source-native releases instead: an
admitted release goes in, faithful Parquet/DuckDB/Iceberg views come out, and
the provenance of every row is the release it came from. spicy-docs' supply
rule already treats these tables as its first rung of acquisition and captures
them; this is what makes them.

### 5. The document-AI producer

`docpipeline/rkaf_projection.py` (3,124 lines), `docpipeline/{extraction,runtime,tag_task,relation_task,executor}.py`,
`docpipeline/adapters/` (Anthropic, OpenAI, OpenAI-compatible, Codex CLI,
Docling, sentence-transformers), and `corpora/` (16 modules) with
`evaluation_boundary.py` as their test bench. On both branches.

**What a user gets.** Two things no product provides today.

*What is this document about.* Search knows the Federal Register Thesaurus
(last revised 1995) and RefSpec's curated vocabularies. A document about a
topic no curator anticipated is invisible to a topic query. This pipeline reads
the document with a model, tags what it is about in open language, and extracts
how it relates to other rules — with identity minted deterministically and
every claim bound to an exact span of source text the model cannot alter.

*A format other products can read.* The result is written as Rulespec RKAF
JSON-LD. Seven production modules across rulespec, RefSpec, and spicysearch
already read that format; this module is the only thing that writes it from a
real document. Without a producer, the platform's portable, validated graph is
a schema with no instances.

`corpora/` ships with it: six console scripts run the segmentation and
relation-extraction experiments whose numbers appear in the evidence, and
`evaluation_boundary.py` freezes the train/holdout split so every accuracy
claim can be re-run. A quality number a user can check is worth more than one
they must trust.

Rulespec's own spec (`spec/rulespec-releases.md` §7, 2026-08-04) recorded
these duties as parked with no owner and assigned the decision to the platform
owner. Decided 2026-09-04: spicy-regs owns them. One line changes on the way
in — `rkaf_projection.py:2334` imports `refspec` directly; it takes a vendored
wheel instead, the pattern spicysearch uses.

## Ships to spicy-docs

### Publisher-issued document populations

`sources/document_populations.py` (468 lines) and seven digest-pinned captures
under `sample-data/document-populations/` — CBO's cost-estimate feed, FCC ECFS
filing pages, GovInfo PREMIS records. On `integrate`.

**What a user gets.** When a query returns 412 CBO cost estimates, the user
cannot tell today whether that is all of them. This captures what the publisher
itself says exists, so the platform answers "412 of 415" and can distinguish
"there are no more" from "we never fetched the rest." The parsers refuse a
bot-challenge page rather than report an empty population, so a blocked fetch
never reads as a complete one.

spicy-docs is the acquisition product — "spicy-docs gets; it does not
interpret" — and already carries `source_domains.py`, the documented-vs-observed
drift gate. Populations are the same kind of thing: acquisition metadata. It
goes through spicy-docs' own intake, commit-never-push.

## Ships to RefSpec

### The rulespec publish gate

`ontology/rulespec_release.py` (178 lines). On both branches.

**What a user gets.** Confidence that what RefSpec publishes was checked
against the rulespec release it claims to depend on. Today RefSpec verifies its
pin against its own recorded digest — proof the note did not change, not proof
the note is true — and its `profiles/rulespec-dependency.json` says
`localUnpublished`. This gate recomputes the L0 contract digest from the tagged
rulespec archive and refuses to publish on mismatch.

Committed to RefSpec, not pushed, once the current merge settles.

### One missing minter

`canonical_usc_chapter_iri` and its lowercase-suffix rule
(`ontology/citations.py:404-418`). The 2026-08-31 port of the citation grammar
into RefSpec's `iri_minting.py` carried seven of eight producers; this is the
eighth.

**What a user gets.** `26 U.S.C. chapter 13A` and `chapter 13a` resolve to the
same place.

Goes to whoever is editing `iri_minting.py`; it lives inside that file.

## Already delivered elsewhere

Users have these today through the product that maintains them. The
spicy-regs copies stay on the fork as the record of where each started.

| piece | where users get it now | what they get |
|---|---|---|
| `source_native*.py`, `schemas/source_native_release/1.0/` (22 files) | spicy-docs `src/spicy_docs/source_native.py` and siblings, from `70a5b16` 2026-08-29; integrate's copy is the byte-identical ancestor at `ff8d202` | faithful, digest-pinned captures from the Federal Register, regulations.gov, GAO product pages, and CourtListener — GAO capture the original never had |
| `sources/uscode_uslm.py`, `uscode_olrc.py` | RefSpec `tools/build_usc_source_credits.py` (`5b8f4d8b`), `registry/act_resolution.py` | from an act section to the U.S. Code section it created, with the division that public Table III lacks. Both copies gained the memory bound the same day: spicy-regs `5ecfd5a`, RefSpec `93d244a3` |
| `ontology/citations.py` | RefSpec `registry/iri_minting.py` (`582461fe`) — seven minters, 1,570 test lines, contract-tested across four compiled forms | every CFR, U.S.C., RIN, Federal Register, and docket identifier resolves to one stable IRI |
| `ontology/act_index.py` | RefSpec `registry/act_resolution.py`, which names it as its provenance | "section 107 of the X Act" resolves through two OLRC sources |
| `transforms/build_authority_edges.py` | RefSpec `registry/unified_agenda_parquet.py` | Unified Agenda legal-authority strings become edges a user can traverse |
| `document_release.py`, `document_release_v3*.py`, `fixtures/releases/` (77 files) | DocSpec DocumentRelease 2.0 — schemas, verifier, 49 sealed fixture cases, three mint receipts (8,284 documents, 220,582 segments) | sealed document releases with bodies and segments, minted and verified |
| `docpipeline/source.py`, `segments.py` | DocSpec `processing/bounded_segmentation.py`, `retention_floors.py` (2026-08-30) | bounded, retention-checked segmentation. DocSpec addresses UTF-8 bytes where spicy-regs addressed codepoints; ids from the two do not compare equal |
| `sources/source_domains.py` | `feat/source-domain-drift-gate-revived` `f8e9e35` (PR #192, ready to reopen) | a check that the values in `document_type`, `rule_stage`, `rin_status` still match what GSA and reginfo document — it already found that every `rin_status` row uses a spelling the publisher's schema does not |
| `enrichment/accepted_output.py`, `managed_release.py` | RefSpec `src/refspec/managed_release.py` (2,638 lines) | candidate lookup against curated, sealed vocabulary releases |
| `docpipeline/retrieval.py` (5,479 lines) | spicysearch `search_application.py`, `POST /v1/search` — lexical, semantic, and concept lanes in production | search over the corpus, served |
| `candidate_release.py` | RefSpec managed releases | the same lookup against a format RefSpec maintains, rather than the Atlas 1.0 format retired 2026-08-09 |
| `source_catalog/` (10 files, 7,239 lines; `archive/landing-final` only) | DocSpec `src/docspec/domain/source_catalog.py`, `schemas/source_catalog/1.0/` — about 1,800 live lines | the catalog of what exists, what was requested, and what was admitted |

One thing outlives the `source_catalog/` code: DocSpec pins
`urn:spicy-regs:source-catalog-release:v1:` in 83 files. That prefix is
DocSpec's contract. spicy-regs never reuses it.

## Preserved on the fork

These wait for a reader. Each names what would bring it back.

**The open-vocabulary concept lifecycle** — `ontology/concepts.py`,
`concept_dimensions.py`, `transforms/build_concept{s,_assignments,_events}.py`,
with `ann_index.py` and `candidate_channels.py` as its serving layer. A SKOS
registry that grows from the corpus: mint a term from what documents say,
promote it on multi-source evidence, deprecate it, merge within one facet.
What a user would get: a vocabulary that tracks what agencies write rather than
what a curator listed. Why it waits: RefSpec is closed-world by design and
spicysearch is closed-vocabulary by design, so no product reads a minted term
today (RefSpec REF-053, 2026-08-31). It returns when one does, and REF-053
asks that the quota and single-facet merge rules already paid for be weighed
then.

**The rest of `ontology/`** — `ledger`, `invariants`, `receipt`,
`relation_findings`, `evaluation`, `common`, `adapters`, `subjects`,
`segmentation`, `checkpoint`, `llm`, `codex_cli`. Supporting modules for the
lifecycle above and for experiments now concluded. RefSpec took what it needed
from `invariants.py` (`2b4960e1`).

**`universes/`** — three JSON files naming the requested regulations.gov
universes, with 600 lines of the archived `PLAN.md` describing them. DocSpec
owns the universe now.

**The record** — `docs/superpowers/specs/` (20), `docs/evidence/` (202), 12
of 14 notebooks. Every decision above has its measurement here.

**`feat/rkaf-boundary-freeze`'s 26 fixture schemas** — an earlier v3 fixture
layout.

## Order of work

1. Tell Eugene. Six PRs from a collaborator he has not heard from in five days
   now sit in his repo, one of them this document.
2. Merge #183 (changes no row of live data), then #181 (stops a full rebuild
   publishing 1% of the Federal Register as complete), watch one nightly, then
   #182 (stops a half-finished publish from retiring work it never uploaded).
3. Reset local `main` to `origin/main`. The archived `main` is
   `archive/landing-final` on the fork.
4. PRs to spicy-regs in the order above: join surface, courts, bills, table
   publisher, document-AI with corpora.
5. Document populations to spicy-docs through its intake.
6. The publish gate to RefSpec as a local commit; the minter to whoever holds
   `iri_minting.py`.
7. Delete `integrate/payload-prereqs` from civictechdc once step 4 has cut from
   it. It stays on the fork at `8d9e7a2`.
8. Remove the worktrees for closed PRs; delete the local branches that are on
   the fork. What remains: one checkout tracking `origin/main`, a worktree per
   open PR.

## Supersedes

Three documents recorded parts of this before, each from its own product's
side. This one reads across all of them against the pinned commits and adds
the decisions.

- spicy-regs `docs/migration/spicysearch-product-migration-manifest.json`
  (2026-08-29, on the archived branches only) inventoried sixteen product
  surfaces for the SpicySearch migration and marked itself
  `retirement_authorized: false`, pending a reconciliation. This is that
  reconciliation.
- RefSpec `plans/2026-08-31-refspec-intake-ledger.md` planned six ports from
  this program. All six landed the day it was written (`5b8f4d8b`, `582461fe`,
  `2b4960e1`). The ledger stands as the record of what was planned; the
  table above records what shipped.
- spicysearch `docs/history/2026-09-01-script-product-disposition.md` drew the
  product boundaries this document uses, and set deletion gates for
  spicysearch's own body-fetch scripts. Its boundaries hold; its premise that
  the source-native gate had no survivor predates spicy-docs' `70a5b16` by
  three days.

Each of the RefSpec and spicysearch documents takes a one-line note pointing
here, through its own repo.
