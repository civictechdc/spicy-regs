# Decision ledger

Append-only. One entry per decision: what was decided, the evidence, and the
trigger that reopens it. Newer entries supersede older ones only when they say
so explicitly. Plans cite this file; this file cites evidence.

## 2026-07-24 — Segmentation policy: `structure-overlap-1800`

- **Decision:** all segmentation uses `structure-overlap-1800` (1,200-token
  leaves, pinned `o200k_base`, structural boundaries, bounded backward
  overlap).
- **Evidence:** `docs/evidence/document-segmentation-fair-comparison-2026-07-24.md`
  — 35/35 gold spans contained, hybrid Recall@50 0.800, rerank lifts R@10
  0.543→0.714.
- **Revisit trigger:** a segmentation-policy experiment (e.g. syntok
  sentence boundaries) that beats it on the frozen sample — noting that any
  boundary change also changes segment identities and the frozen baseline.

## 2026-07-24 — Document-AI package fit

- **Decision:** Docling 2.115.0 (DOCX/PPTX/XLSX only, PDF refused),
  `tiktoken`, sentence-transformers 5.6.1 (pinned models/revisions),
  `ir-measures` 0.4.3, scikit-learn, official provider SDKs, `jsonschema`,
  DuckDB over Parquet. Chonkie rejected as a production dependency.
- **Evidence:** `docs/superpowers/specs/2026-07-24-document-ai-package-fit-decision.md`.
- **Revisit trigger:** a paired run on frozen + held-out real data showing a
  package improves accuracy or removes substantial owned code without
  weakening source evidence.

## 2026-07-24 — No dedicated graph engine

- **Decision:** serve traversal from DuckDB over published Parquet; no second
  engine. Kuzu held as a deferred rebuildable projection.
- **Evidence:** `docs/evidence/graph-engine-bakeoff-2026-07-24/` — DuckPGQ
  variable-length operator crashes; Kuzu wins at 1M edges (2.1 ms vs 9.6 ms)
  but the corpus lacks the edges that would justify it
  (`docs/corpus-edge-coverage-findings-2026-07-24.md`).
- **Revisit trigger:** a real query DuckDB cannot serve, measured.

## 2026-07-25 — Relation benchmark publication blocked

- **Decision:** the v2 relation-exclusion benchmark is not publication
  eligible until two blind human reviews seal the oracle
  (`REQUIRED_BLIND_HUMAN_REVIEWS = 2`).
- **Evidence:** three adversarial reviews attributed the v1 score to oracle
  defects, not model failure; six Codex runs swung baseline F1 0.273→0.600 on
  identical inputs.
- **Revisit trigger:** the two reviews happen. Until then this work is
  formally waived from the review budget, not queued.

## 2026-07-27 — Package-first extension slots

- **Decision:** each slot below is one bounded computation behind a
  project-owned interface. A package enters the default path only when a
  paired run on frozen and held-out real data improves accuracy or removes
  substantial owned code without weakening source evidence. Package IDs and
  object models never enter durable tables; every accepted span must resolve
  to exact locked source text.

| Slot | Candidate | Disposition |
| --- | --- | --- |
| Legal citation recognition | CiteURL (U.S.C./CFR), eyecite (judicial, FR volume/page) | Run one bounded regulatory bakeoff with CiteURL; eyecite only for an active judicial need |
| Mention detection / concept linking | existing lexical + Sentence Transformers baselines; GLinker as comparator | Baselines first; GLinker only if candidate recall stays a measured problem |
| Bounded classifiers (multi-label, role, mapping) | scikit-learn linear/OvR; SetFit only if linear underfits | After corrected labels support a leakage-free artifact-level split |
| Calibration and label-error diagnosis | `CalibratedClassifierCV`, cleanlab | After out-of-sample probabilities exist; warnings create review items, never auto-corrections |
| Unknown/local-concept grouping | scikit-learn HDBSCAN over existing embeddings | When enough unresolved examples accumulate; clusters are review bundles, not concepts |
| Cross-source record linkage | Splink | Defer until a real cross-source identity set exists; never auto-merge |
| Typed relation extraction | GLiREL | Defer until entity linking and relation labels are stable |
| RDF conformance | pySHACL | Only when an RDF projection serves a real consumer; CUE stays authoritative |

- **Evidence:** `docs/rulespec-testbed-path-forward.md` revision of
  2026-07-27 (git history) carries the full rationale text.

## 2026-07-27 — Citation parsing: supplement-first

- **Decision:** three separate jobs — packages may *recognize* free-text
  spans; existing code parses source-specific fields; project-owned
  `canonical_*` functions assign identity. Package URLs and internal IDs
  never become durable identity or provenance. Recognized raw text must
  equal `SourceFragment[start:end]` before projection.
- **Evidence:** exploratory probe over all 4,777 distinct Unified Agenda
  authority strings: 4,157 recognized by both the current parser and CiteURL,
  233 current-parser-only, 108 CiteURL-only, 279 neither. Detection coverage,
  not correctness; no committed command yet makes it reproducible.
- **Bakeoff protocol (when run):** freeze fixtures + the 4,777 strings;
  compare current vs current+CiteURL; adjudicate a stratified disagreement
  sample (frontier model first pass, async human correction); require a
  material held-out gain before retiring any regex; block unreviewed identity
  changes; save the exact command and pinned versions. CiteURL stays
  experimental (its package imports `markdown` without declaring it).
- **Revisit trigger:** the bakeoff runs, or judicial/FR volume-page
  extraction becomes active (then evaluate eyecite separately).

## 2026-07-27 — Manual parser dispositions

- **Decision:** keep project-owned code for identity and safety grammars
  (RINs, FR document numbers, Regulations.gov IDs, source keys, exact-offset
  HTML/XML boundaries); adopt established packages only at the listed
  triggers: Beautiful Soup when `build_search_index.py` next changes;
  feedparser if GAO RSS variants multiply; defusedxml at source-reader
  hardening; ijson on an observed bulk-extract memory failure; python-stdnum
  if more standard identifiers arrive; official NAICS registry data when
  semantic validation matters; stdlib `datetime` with real calendar
  validation.
- **Evidence:** parser audit in the 2026-07-27 path-forward revision (git
  history); the expensive formats are already covered by packages.

## 2026-07-27 — Deletion authorization

- **Decision:** after the integrated tag loop completes one full iteration on
  this branch (MVP plan phase 2, first iteration), the following are
  authorized for removal in one reviewed commit: migration-only fixtures and
  expected-difference files, legacy identity/storage compatibility shims, and
  intermediate-equivalence checks that exist only to prove compatibility with
  the retired `corpora` runner. The v2 `corpora` runners themselves retire at
  MVP cutover (phase 4), with the stored v4 outputs kept read-only as the
  benchmark artifact. This supersedes the "no deletion is authorized"
  ambiguity in earlier revisions.
- **Revisit trigger:** none — this is the trigger.

## 2026-07-27 — MVP scope

- **Decision:** the MVP is source → segments → concept assignments with
  Rulespec roles and exact evidence → human-attested review → atomically
  published tables conforming to Rulespec L0, on the existing corpus.
  Acceptance splits into **MVP-local** (locally published generation, every
  local gate executing and green) and **MVP-public** (same generation
  uploaded; additionally requires the Rulespec human gates and the
  release-preflight rule fix). Out of MVP: retrieval serving, relation
  benchmark unblocking, cascade classifiers and every gated slot above,
  signed receipts, the comments corpus, state bills, MCP additions beyond
  the existing surface.
- **Evidence:** `docs/rulespec-testbed-path-forward.md` (the MVP plan);
  three independent plan reviews 2026-07-27, all SOUND-WITH-CORRECTIONS.
- **Revisit trigger:** MVP acceptance, or a gate proving a scoped-out item
  is on the critical path.

## 2026-07-27 — Gold and held-out protocol

- **Decision:** MVP gold is ~80 artifact assignments (35 adjudicated + ~45
  new), split by **gold concept** (artifact-level splitting leaks through
  alias edits and the lexical selector). Gold is drafted by a different
  model family than the tagger, blind to tagger output; the held-out slice
  is 100% human-adjudicated; `gold_sha256` and per-item
  adequate-target-vs-abstention branch assignments freeze before tuning;
  `registry_sha256` is pinned per iteration and a regression test asserts
  no new alias normalizes to a held-out gold label. At this size the
  held-out slice **vetoes regressions only** — it cannot certify gains.
- **Accuracy-claim tier (separate, post-MVP-local):** claims of improvement
  require the powered set (~780 assignments at a 30% held-out and ~0.3
  adequate-target branch rate for a 15-point MDE; sizing math in the
  2026-07-27 measurement review) and the pre-committed bar below.
- **Exit-bar form (instantiated with real numbers at re-baseline, in a
  commit preceding the re-baseline run):** bar = max(trivial baselines on
  the same held-out set [always-abstain, lexical top-1], baseline +
  2×bootstrap SE); one-sided paired test at α=0.05; held-out results are
  reported only when they beat the prior best by more than one MDE.
- **Revisit trigger:** the re-baseline run (instantiates the bar), or a
  gold-set expansion (re-derives the sizing).

## 2026-07-27 — Post-MVP repairs (recorded, not forgotten)

- Mirrulations attachment ordinal numeric sort
  (`sources/derived_text.py:82`) — waits for the comments corpus.
- `rulespec_release.py` `_CONTRACT_FILES` becomes a rule — every `.cue`
  under `constraints/{core,analysis,profiles}` recursively plus every
  `l0-ranges.cue` under `constraints/` plus the context file — so the
  tarball recomputation can match a real release (today: 40-file set →
  `sha256:f1e71af4…` vs rulespec's 50-file → `sha256:5f287a1e…`). Only
  MVP-public needs it.
- `refuse_retrieval_aids` bans bare `score`/`rank` at every depth; the ban
  fires the moment retrieval candidates enter an extraction payload —
  namespace the keys or scope the ban when retrieval unparks.
- Two strict xfails in the retrieval tests mark unimplemented API
  (`method_policy` plan fact, `query_methods` metrics parameter) — resolve
  when step 5 unparks.
- `retrieval.py` `_authority_id()` falls through to `authority_raw` for EO
  and statute rows, re-collapsing distinct citations that `91db195` fixed
  on the published side — same bug shape, retrieval side; fix when step 5
  unparks.
- `data_dictionary.py check --source r2` will report authority_edges
  column drift until the next publish (inherent to any published-schema
  change).
- **Revisit trigger:** the named feature unparks.
