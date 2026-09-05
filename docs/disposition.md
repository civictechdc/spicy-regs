# Disposition of the July–August research program

Written 2026-09-04. One record of what the program built, what each piece gives
a user of the platform, and where each piece now lives. Decisions were taken
2026-09-04 by the platform owner and completed 2026-09-05; this document records
them. Two were revised after the first draft and are recorded as revised: the
table publisher's home is spicy-docs, not spicy-regs, and the document-AI
orchestration's home is spicy-docs (ruled 2026-09-05). Three of the spicy-regs
pieces have shipped; the *Order of work* section says where.

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
| spicy-docs | `main`, for the 2026-09-05 pricing | `86e8416` |
| rulespec | `main`, for the 2026-09-05 pricing | `a519d06` |
| RefSpec | `main`, for the 2026-09-05 pricing | `00f22e9c` |

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
`sources/supreme_court_opinions.py` and `transforms/build_supreme_court_opinions.py`.
The `8d9e7a2` versions are the fuller ones — `9a79569` stripped the measured
403 rate-limit guard and the bound-volume page-range logic — so `8d9e7a2` is
the branch to cut from. The rest belong to pieces that ship from one branch
only.

Neither branch has PRs #181, #182, or #183. Both carry the earlier
`uscode_uslm.py` and `integrate` carries the earlier `source_domains.py`; the
corrected versions are on their own branches, and the PRs below cut from
`integrate` by file, never wholesale.

## Ships to spicy-regs

Each of these becomes one PR cut from `integrate/payload-prereqs` (or
`archive/landing-final` where noted) onto `origin/main`, with the same
treatment as #181: rebase, gate through `uv run`, validate against real data,
one-paragraph description. Items 1–3 shipped on 2026-09-04 as PRs #194, #195,
#196 — open on civictechdc, merged on the fork; see *Order of work*.

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

Two sub-clusters, on `integrate`. **CourtListener** — `sources/courtlistener_bulk.py`,
`transforms/build_court_opinion_bodies.py` (356 lines),
`build_court_opinion_clusters.py` (410), `court_scope.py` (348),
`pipelines/rollups/court_opinion_{bodies,clusters}.py`,
`scripts/backfill_cluster_court_scope.py`, plus 84 additive lines in
`sources/courtlistener.py`. Shipped as PR #195. **Supreme Court** —
`sources/supreme_court_opinions.py`, `transforms/build_supreme_court_opinions.py`,
`pipelines/rollups/supreme_court_opinions.py` — held back: its source imports
`bs4`, which `origin/main` does not carry and no other source uses, so that
dependency is a separate PR and a separate decision. `transforms/pdf_text_pymupdf.py`
is not courts at all: it is imported by `docpipeline/source.py`, and it is
AGPL; it goes with the document-AI producer, where the licence choice is made
explicitly. A real run exists: `output/court-data-2026-08-22/`, 5.7 GB.

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

### 4. The document-AI producer

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
owner. Decided 2026-09-04 as a split; ruled complete 2026-09-05. The split is from
the call graph of `rkaf_projection.py` at `8d9e7a2`, which corrects the first
draft's count: `project_document` was listed as deterministic, but it calls the
model layer, so it belongs to the orchestration. Both receiving lanes reproduced
every count below by AST on 2026-09-05 and added the seams recorded here.

- **The deterministic layer** — 21 functions, 1,307 lines of
  `rkaf_projection.py`, plus 12 of its 13 data classes (209 lines;
  `NormalizedVocabulary` serves only the model path and stays). Roots:
  `assemble` (476), `verify_candidate_rows` (191), `_federal_register_facts`
  (204), `_unified_agenda_facts` (91), `verify_fragment`, `load_artifact`, and
  the IRI and fragment helpers under them. It becomes
  `rulespec/packages/rulespec-projection`, beside `rulespec-artifacts`, the
  only package there today: the format owner ships the reference producer next
  to its verifier and fifteen conformance bundles. Priced against rulespec at
  `a519d06` (`rulespec-artifacts`, `pyshacl`, `rdflib`, `rdfcanon`): zero new
  dependencies. The functions themselves import nothing outside the standard
  library. What they reach is copied at the name, not the module, because the
  host modules import pyarrow and loguru at the top:
  - `ontology.citations`: ten parser and IRI functions. The module is 1,025
    lines of standard library and can move whole.
  - `ontology.attestations`: `attestation_row` and `ATTESTOR_KIND_AI_MODEL`,
    which close over about 100 lines, plus `OntologyInvariantError` from
    `invariants`. Not the 544-line module.
  - `ontology.common`: `canonical_json`, `stable_id`, `text_digest`,
    `RunContext`; or `rulespec_artifacts.canonical_json_bytes` and
    `sha256_digest` for the first two.
  - `ontology.llm`: `resolve_exact_evidence_offsets` with its
    `EvidenceOffsetResolution` dataclass, 50 lines, no imports.

  Two seams stay with the caller as a Protocol or an injected mapping:
  `PublishedTables`, whose cache fills through `read_parquet_rows` (the one
  pyarrow path in the set), and `_source_table_for_profile`, which reads
  `SOURCE_PROFILES`. `SourceArtifact`, a 34-line dataclass, becomes the input
  contract, filled from DocSpec's DocumentRelease 2.0, with `AccessScope` and
  docling's default size limit inlined; IRIs come from RefSpec's `iri_minting`.

- **The orchestration** — the 13 functions only the model path uses, 1,150
  lines of `rkaf_projection.py` (`_run_model_layer_with_vocabulary` 355, the
  two vocabulary loaders 552, `project_document` 71), plus the six provider
  adapters (6,932 lines), `extraction` (954), `runtime` (1,792), `tag_task`
  (961), `relation_task` (2,102), and `executor` (536). Its home is
  **spicy-docs**, ruled 2026-09-05: it sits beside the source-native releases
  it reads and the table publisher it feeds, so one install runs the whole
  path. It depends on `rulespec-projection` for the 13 functions both layers
  share (878 lines).

  *The constraint that shapes it.* spicy-docs' read and verify path is
  contractually thin. DocSpec installs the spicy-docs wheel with `--no-deps`
  (`tests/test_source_catalog_installed_wheel.py:911` at `20fcb24`) and imports
  five of its modules, and spicy-docs' `tests/test_reader_closure.py` at
  `86e8416` fails if `polars`, `httpx`, `boto3`, `botocore`, `loguru`, or
  `tqdm` sit in `sys.modules` after importing `source_native` and
  `source_native_profiles`. That guard is the enforcement for this port. It
  gains the seven model names (`anthropic`, `openai`, `sentence_transformers`,
  `torch`, `docling`, `tiktoken`, `transformers`), and since DocSpec imports
  five modules on that path and all five are clean today, it should name all
  five. Import direction, after spicysearch's `AGENTS.md` table
  (`f97b904:100`):

  | From | Read/verify | Acquisition/publish | Orchestration | Experiments |
  |---|---|---|---|---|
  | Read/verify | Yes | No | No | No |
  | Acquisition/publish | Yes | Yes | No | No |
  | Orchestration | Yes | Yes | Yes | No |
  | Experiments | Yes | Yes | Yes | Yes |
  | Tests | Yes | Yes | Yes | Yes |

  Model output is nondeterministic by construction and spicy-docs' releases are
  byte-reproducible; the table keeps them apart in the direction that matters.

  *Dependencies*, priced against spicy-docs at `86e8416` (`rulespec-artifacts`,
  `boto3`, `httpx`, `jsonschema`, `loguru`, `polars`, `tqdm`; `pyarrow` as the
  `public-table` extra). Present: `jsonschema`, `loguru`, `pyarrow`. Absent at
  runtime: `anthropic`, `openai`, `tiktoken` (the model adapters);
  `sentence_transformers` and `torch` (the embedding adapter); `docling` (the
  layout adapter, imported lazily); `refspec` (the vocabulary loader); `rdflib`
  (through `candidate_release`); `numpy` and `scikit-learn` (through
  `ontology.concepts`, lazily). One extra per adapter, copying all four
  properties of `public-table`: the extra, the reason in the module docstring,
  a lazy CLI import that reports the missing dependency instead of crashing,
  and presence in the dev group so the default suite tests it. The embedding
  extra's note states its size: `pyarrow` is one wheel, `torch` is gigabytes.
  Test-only and absent: `docling_core`, `python-docx`, `openpyxl`,
  `python-pptx` (the real-Docling test); `refspec` and `duckdb` (the projection
  test). spicy-docs has no model or adapter code at `86e8416`, so nothing
  collides.

  *Seams.* The cost is the first-party closure, 20 modules and 19,209 lines
  before adapters, not the packages. Each module below is touched at a few
  names, and a module touched at a few names is a contract to define, not code
  to move:

  | module | lines | names the orchestration uses |
  |---|---|---|
  | `docpipeline/source.py` | 3,593 | 5: `SourceArtifact`, `build_source_artifact`, `profile_for_table`, `SOURCE_PROFILES`, `iter_source_records` |
  | `ontology/concepts.py` | 1,407 | 4 |
  | `docpipeline/segments.py` | 1,163 | 3 |
  | `ontology/llm.py` | 1,036 | 7: the prompt constants, the tag schema, the resolver |
  | `candidate_release.py` | 705 | 2 |
  | `document_release_v3.py` | 678 | 6: digest and key helpers |
  | `ontology/common.py` | 226 | 6: `canonical_json` alone from ten call sites |
  | `connected_concepts`, `ontology/segmentation`, `concept_dimensions` | 856 | 7 |

- **`corpora/`** — 16 modules, 23,798 lines of experiment scripts, over half
  the producer by volume. Experiments may import everything and nothing imports
  them, so they sequence last and defer at no cost. All 15 scripts were last
  touched 2026-08-28; 13 of them are registered as 13 console-script
  entries at `8d9e7a2`, and `mirrulations_document_corpus` is neither
  registered nor cited by the evidence. They move per module, when the evidence
  that cites one needs re-running, under an `experiments` extra
  (`transformers`, `scipy`, `ir_measures`, `pypdf`, `python-dotenv`,
  `duckdb`). None moves in the same change as the adapters.

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

### The table publisher

`public_table.py` (1,000 lines), `public_table_profiles.py` (165),
`publication.py` (65), and `tests/test_public_table.py`. Ruled 2026-09-04
afternoon: **canonical home spicy-docs**, superseding the morning's assignment
to spicy-regs. **Landed on spicy-docs `main` at `2bb1dcf`** the same evening:
`publication.py` was not copied because an identical module already existed
there; `SUPPORTED_PRODUCER_PRODUCTS` was reused rather than re-hardcoded;
`pyarrow` — a runtime import of the module, which spicy-docs did not carry and
this record's first version did not name — was added as an optional extra plus
a dev entry; the eight-ref provenance is in the commit.

**What a user gets.** Everything a user touches — the Parquet files at
`data.spicy-regs.dev`, the MCP server, the app — is a table something built.
Today that something is 21 hand-written scrapers. This is the path that builds
the same tables from verified, digest-pinned source-native releases instead: an
admitted release goes in, faithful Parquet/DuckDB/Iceberg views come out, and
the provenance of every row is the release it came from. Its own docstring
draws the line — "Rulespec owns the artifact root, member manifests, digests,
and admission; [the product] owns only the faithful flat source view" — and a
faithful flat view is no interpretation.

**Why spicy-docs.** `public_table_profiles.py` imports thirteen names from the
source-native readers spicy-docs owns, and every one resolves at spicy-docs
`9f8c7ee`; the publisher and `publication.py` import nineteen names from
`rulespec_artifacts`, every one present in 1.0.11; the six release schemas are
byte-identical between `8d9e7a2` and spicy-docs, so it reads what spicy-docs
emits today. Its CLI entry, `spicy-regs-source-native`, is already spicy-docs'
`source_native_cli.py`. `duckdb` is test-only — zero imports in the three
modules, one in the test, proving the "DuckDB reads the declared members
directly" claim; it stays a test dependency and the claim stays proven.

**Provenance and the rule that keeps one implementation.** The four files
exist on no live line — zero on `origin/main`, zero on `fork/main`, zero on
`archive/landing-final`. They exist on eight refs, all history:
`integrate/payload-prereqs` @ `8d9e7a2` on both civictechdc and the fork,
`archive/integrate-payload-prereqs-pre-reorg`, `archive/pre-strip-2026-08-26`,
`snapshots/pre-strip-2026-08-26`, and the fork's copies of those. spicy-docs
copies from `8d9e7a2` and becomes the only implementation. **No spicy-regs
branch re-cuts these modules.** There is nothing to delete and nothing to
freeze on a live line; the archived branches are the record, not a rival. The
lesson the landing taught: a port brief must list every *runtime* third-party
import per module against the target's actual dependency closure, not only
the test's — the first version of this record named `duckdb` and missed
`pyarrow`, and the module would not import until it was added.

One thing for spicy-docs' supply-precedence rule, recorded where the rule
lives: once spicy-docs both produces the public tables and captures them as its
first rung, "capture the pinned upstream artifact" and "capture our own output"
are the same operation for this one source. Benign — it is how every source is
treated — but a rule that appears to defer to an upstream authority must say
that here the upstream is itself, or it reads as corroboration when it is a
self-reference.

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

1. Tell Eugene. Seven PRs from a collaborator he has not heard from in five
   days now sit in his repo, one of them this document.
2. **Done 2026-09-04, on the fork and the local checkout only.** By the
   platform owner's ruling the seven PRs stay open on civictechdc and were
   merged onto `fork/main` and local `main` as merge commits, each gated
   through `uv run` before the next:
   `1918409` #181 (639 passed) → `1374f90` #182 (645) → `64aa35d` #183 (657)
   → `abd229f` #193 (657) → `a3dc1b6` #194 (941) → `4b041c0` #195 (979)
   → `765815a` #196 (1,005 passed, 3 deselected; baseline 634).
   After the seven merges `fork/main` = local `main` = `765815a`; this
   document's revisions were merged on top. `origin/main` untouched at `1f02a7f`.
3. **Done.** Local `main` was reset to `origin/main` before the merges; the
   archived `main` is `archive/landing-final` on the fork at `9a79569`.
4. PRs to spicy-regs: join surface (#194), CourtListener (#195), bill subjects
   (#196) — **done**, each validated against live data before opening. The
   Supreme Court sub-cluster waits on the `bs4` decision. The document-AI
   producer is split, priced, and ruled above; the brief went to the spicy-docs
   and rulespec lanes 2026-09-05.
5. The table publisher to spicy-docs through its intake (ruled afternoon
   2026-09-04; see *Ships to spicy-docs*). Document populations likewise.
6. The publish gate to RefSpec as a local commit; the minter to whoever holds
   `iri_minting.py`.
7. **Done 2026-09-05.** `integrate/payload-prereqs` deleted from civictechdc
   after spicy-docs landed the publisher at `2bb1dcf`; the fork holds it at
   `8d9e7a2`. A file-deletion PR was not the shape: `origin/main` has none of
   the files.
8. Register the eight new tables — five from #194, two from #195, one from
   #196 — in the data dictionary and MCP server as one follow-up PR, the way
   #175 exposed the lifecycle rollups after they existed. The first
   `materialize-rulemaking` run needs `--allow-bootstrap` via
   `workflow_dispatch`.
9. **Done 2026-09-05.** The nine worktrees for merged and closed PRs are
   removed. What remains: one checkout on `main` with the merges on top of
   `origin/main`, and a local branch per open PR, each at its PR head.

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
