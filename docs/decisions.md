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

- **Decision:** the adjudicated 35 are permanently train/development data.
  They have been inspected and used to change the prompt, registry, and
  selector; freezing them now cannot restore holdout status. A new evaluation
  dataset must be drawn without tagger output and frozen before labels are
  exposed.
- **Separation:** split by **gold concept and every registered alias**, not
  artifact alone. Reject shared concept ids, normalized aliases, and artifact
  digests across train and holdout. Pin source, selection, gold, registry, and
  configuration digests.
- **Adjudication:** final holdout labels require at least two independent
  model families (or humans), blind to tagger output, with agreement published
  and disagreements resolved by a third family or excluded. The existing
  three Claude sessions count as one family and do not satisfy this rule.
- **Use:** development data drives iteration. Final holdout is one-shot:
  configuration freezes before labels; `tuning_access` remains false. Once
  its labels inform another design decision, that set becomes development.
  The tracked `evaluation-boundary.json` and
  `--require-adoption-ready` command enforce these structural conditions.
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
  **and statute_at_large** rows (neither has a branch), re-collapsing
  distinct citations that `91db195` fixed on the published side — same bug
  shape, retrieval side; fix both when step 5 unparks. Deferral is safe:
  nothing under `src/` imports `retrieval` (validated 2026-07-27).
- `data_dictionary.py check --source r2` will report authority_edges
  column drift until the next publish (inherent to any published-schema
  change).
- **Revisit trigger:** the named feature unparks.

## 2026-07-27 — Contract-assumption validation results

- **Decision:** the adversarial validation of the plan's Rulespec-contract
  assumptions (seven claims, session record) is folded into the plan.
  Corrections adopted: approval requires a real contract-shaped
  `attestations` table (the per-row provenance block is not an
  Attestation; rejection must be recordable, never implied by omission);
  `assertionOrigin` joins the required assignment columns;
  `assignmentEvidence` stays unclaimed at L0 (SourceFragment range, no
  fragments carrier); phase 1 adjudication/gold expansion are
  identity-neutral and decoupled from the roles schema edit.
- **EO mapping deferral, corrected rationale:** adding `executive_order` →
  `agendaAuthorityCitation` is mechanically trivial (template + one
  sample passes the audit) — the real boundary is semantic: the
  `urn:rkaf:us:eo:{n}` pattern is registered under
  `hasRegulatoryIdentifier`/`us-eo`, and reusing it under a different
  predicate is a new, unchecked semantic claim. Deferred on those
  grounds, not tooling.
- **Optional future Rulespec simplifications (informational, Rulespec is
  accepted as-is; none block the plan):** a normative tabular/inlined
  Attestation pattern for L0 implementers; letting `assignmentEvidence`
  cite a carrier-local fragment URN derived from offsets; machine-legible
  scope carve-outs (`excluded_terms:`) instead of freeform notes prose.
- **Revisit trigger:** phase 4 implementation, or the next Rulespec
  contract revision.

## 2026-07-27 — Review capacity: machine-first attestation, wiki later

- **Decision (maintainer):** Spicy Regs has no standing human review
  capacity. The human-task ledger and "one human task per iteration"
  budget in earlier entries are void. Initially the system publishes what
  the pipeline can produce, honestly graded: adjudications and
  attestations record a machine attestor (`attestorKind`), judge models
  come from different families than the model being judged, blind
  protocols hold, dual-model adjudication publishes its agreement rate as
  the residual-error estimate, and nothing is ever presented as
  human-verified. Accuracy numbers are always labeled machine-adjudicated.
- **Wiki-style validation interface (future capability, not designed
  now):** a later interface for validating and discussing records is the
  standing channel for human judgment. It writes attestations into the
  same attestation table (superseding machine rows), which is why the
  phase 4.1 schema carries attestor/attestorKind/decision/scope from day
  one. Do not build any part of it before MVP-local.
- **Unchanged:** the Rulespec release human gates for MVP-public (outside
  this repo); "a model never attests its own output."
- **Revisit trigger:** the wiki interface becomes buildable, or human
  review capacity appears.

## 2026-07-27 — Multi-provider model backend

- **Decision:** running and comparing different model families happens
  through one new arm of the existing structured-text-model interface,
  `docpipeline/adapters/openai_compatible.py`, which speaks OpenAI
  chat-completions to an injected `base_url` using the already-declared
  `openai` SDK — **zero new dependencies**. A registry of provider
  profiles keys the only things that differ: `openrouter`
  (`OPENROUTER_API_KEY`), `anthropic` (its OpenAI-compatibility endpoint,
  `ANTHROPIC_API_KEY`), `gemini` (its OpenAI-compatibility endpoint,
  `GEMINI_API_KEY`), and `local` (base URL from
  `SPICY_REGS_LOCAL_LLM_BASE_URL`, loopback-only, no key). Model IDs are
  always caller-pinned; no provider gets a default model. Strict
  `response_format` json_schema is used where the endpoint honors it and
  schema-embedded instructions elsewhere; either way the response is
  validated locally against the caller's schema and a failure is a
  rejection, never a repair. `structured_mode` records which mechanism
  answered, `token_count_method` records that `tiktoken` counts are
  estimates off OpenAI models (the budget is still enforced), and
  provider label + base URL host + model ID make "claude-sonnet-5 via
  openrouter" distinguishable from "via anthropic" in a receipt.
- **Why this now:** the MVP's blind adjudication requires judges from
  families other than the tagger's, and model comparison is a standing
  need; both were blocked on having exactly one paid arm.
- **Why not LiteLLM (or another gateway) now:** the arms owe receipts the
  *exact* request they sent and the exact response body they received, and
  a translating layer owns that payload instead of this repo; and a new
  package must pass the package-first extension gate (a paired run
  improving accuracy or removing substantial owned code). An
  OpenAI-compatible client the project already ships costs neither.
- **Revisit trigger:** a provider we need has no OpenAI-compatible
  endpoint, or per-provider compatibility workarounds accumulate into
  substantial owned code — then adopt LiteLLM and re-verify exact request
  and response custody under it.

## 2026-07-27 — Native SDK arms where a compat surface cannot enforce

- **Decision (maintainer):** when a provider's OpenAI-compatible surface
  cannot enforce a strict JSON schema, the project takes that provider's
  native SDK as a new arm of the structured-text-model interface rather
  than shipping a per-provider workaround inside
  `docpipeline/adapters/openai_compatible.py`. The first such arm is
  `docpipeline/adapters/anthropic.py`, built on the `anthropic` SDK:
  Claude-family calls now go through the Messages API's
  `output_config.format` json_schema, which the endpoint actually
  enforces, instead of a `response_format` the compat endpoint accepts
  and ignores. The arm has one mode — there is no prompted fallback,
  because a schema the endpoint cannot enforce is a refusal (locally,
  before any paid call, when the unenforceable construct is detectable;
  on the API's own rejection otherwise). It counts tokens with the
  provider's own `messages.count_tokens` rather than a foreign-tokenizer
  estimate, derives the Messages API's required `max_tokens` from the
  caller's output budget, pins model IDs from the caller, and raises
  (naming `ANTHROPIC_API_KEY`, never its value) when asked to build
  itself from an unconfigured environment.
- **This supersedes** the "native-Anthropic-arm later" revisit trigger in
  the 2026-07-27 multi-provider entry. The zero-new-dependency principle
  stands for every provider whose compat surface is honest; it does not
  stand where honoring it would mean publishing an unenforced call as an
  enforced one.
- **Consequences in the compat arm:** the `anthropic` provider profile and
  its prompted-mode workaround are removed — they existed only while no
  native arm did. Claude via `openrouter` stays, which is what makes a
  cross-route comparison (enforced native vs brokered compat) possible.
  OpenRouter's structured requests now carry its documented provider
  routing field, `provider.require_parameters: true` (sent in
  `extra_body`, so it hashes into `request_sha256`), and only in
  `response_format` mode: without it a broker may route a json_schema
  request to an upstream provider that ignores the field, and the receipt
  would claim enforcement that never happened. `provider_routing` records
  the constraint that actually rode along.
- **Revisit trigger:** a second provider needs a native arm, at which point
  re-check whether the four arms have accumulated enough shared transport
  code to justify extracting it (still without letting any arm import
  another).

## 2026-07-27 — The three Rulespec simplifications landed

- **Landed on rulespec `us-regulatory-identifiers`:** `b613ba3` normative
  tabular attestation pattern (six-column table; `approved_by` columns may
  never map to Attestation terms; rejection is a row, absent = unreviewed;
  revocation is a timestamp, never a delete); `bc88c02` carrier-local
  fragment URN (`urn:rkaf:fragment:<artifact>:<start>:<end>:sha256-<hex>`,
  half-open codepoint offsets matching our anchor semantics) making
  `assignmentEvidence` claimable at L0 without a fragments table;
  `f2a939d` machine-legible `excluded_terms`/`excluded_tables` carve-outs.
  Contract digest moved to `sha256:5aaac340…`; spicy-regs re-pinned at
  `ce75ffe`, audit 1/1.
- **Breaking consequence for phase 4.2:** `assignmentEvidenceScheme` is
  REQUIRED whenever assignment evidence is present — add it to the
  contract-required columns (value: the fragment-URN scheme).
- **MVP-public checklist addition:** rulespec's gate expects a partner
  declaration at `conformance/partners/spicy-regs.yaml` (its L0 audit
  currently reports 0/0 — no declaration exists); authoring it is part of
  going public, alongside the phase 4.3 carve-out rewrite, which should
  now use `excluded_terms` instead of prose.
- **Revisit trigger:** phase 4 implementation.

## 2026-07-27 — Registry fusion: measured, not yet adopted

- **Historical v1 artifact:** `fused-concept-registry-v1` has 513,236
  rows (FR Thesaurus, CRS subjects and policy areas, EPA TSCA, and FAST).
  It preserved the original 901 ids, but it overloaded `scheme`: original
  rows used `subject` as a semantic facet while imported rows used source
  vocabulary names. That shape made the profile's legitimate `subject`
  facet gate reject imported subject concepts. The resulting 936/513,236
  reachability was a schema defect, not evidence that facet gates should be
  removed.
- **Corrected model:** `facet` now carries tag policy
  (`subject|regulated_entity`); `source_vocabulary` carries authority
  identity and retrieval quotas. New rows keep the deprecated `scheme`
  compatibility field equal to `facet`. Exact same-label collisions across
  vocabularies produce an unreviewed mapping artifact, but the selector keeps
  every authority id separately selectable. Convergence cannot merge them and
  no `skos:exactMatch` is inferred. The v1 external-valued `scheme` reader is a
  migration shim with the removal boundary recorded in the ontology design.
- **Historical measurement, development only:** the repeatedly inspected
  35-item set showed exact-alias coverage 1/35 → 8/35 and unchanged surfaced
  adequacy of 5/35 under the old selector. Those observations helped locate
  the seam; they cannot authorize an accuracy or adoption verdict. Earlier
  anchored-selector numbers on the same 35 are likewise tuning evidence.
- **BM25 baseline, development only:** a pinned `bm25s` Lucene index over
  preferred labels and aliases built in 7.680 seconds but surfaced only 1/8
  exact-alias targets and retained 1/5 adequate targets. Adding char n-grams
  stayed at 1/8 and 1/5; adding dense retrieval reached 2/8 and 4/5, still
  below v2's 4/8 and 4/5. Keep BM25 as a regression baseline, not a production
  selector. Evidence and limitations are recorded in
  `evidence/candidate-selection-research-2026-07-27.md`.
- **Disposition:** no fused registry or selector is adopted. The current
  implementation can execute `anchored-hybrid-v2` through the same payload,
  strict schema, facet policy, and token budget as the production tag path.
  Adoption still requires a newly drawn, digest-pinned, concept/alias-separated
  holdout, configuration frozen before labels, and completed blind
  adjudication by at least two independent model families (or humans).
- **Revisit trigger:** an untouched holdout satisfies the tracked evaluation
  boundary and the adoption-ready command passes.

## 2026-07-28 — Active rulemaking: an evidenced non-terminal proceeding stage

- **Decision:** a rulemaking is **active** when the proceeding's
  `current_stage` is an evidenced non-terminal stage — `prerule`, `proposed`,
  `supplemental`, or `longterm`. Write it as that allowlist, never as
  `NOT IN ('final','withdrawn')`. A null `current_stage` means no determinate
  stage evidence; it is reported as `stage_unknown` and never counted as
  active. The definition attaches to the **proceeding**, which is the unit a
  discovery answer returns; the Unified Agenda's own `rule_stage` describes a
  plan for a RIN and is never projected onto the proceedings that RIN tracks.
- **Evidence:** frozen ontology snapshot `snapshot_0e4b4204bdfbd462a9270fcd766fb8dd`,
  measured in `evidence/discovery-slice-2026-07-28.md`. Of 511,643
  proceedings: `final` 164,616, `proposed` 115,703, `withdrawn` 12,540,
  `supplemental` 2,836, null 215,948 (42.2%). `prerule` and `longterm` never
  occur — `current_stage` derives only from regulations.gov and Federal
  Register document types — but they stay in the allowlist because the stage
  vocabulary admits them and a future evidence path may reach them. The
  allowlist and `NOT IN ('final','withdrawn')` select the same 118,539 rows
  here, and only because SQL drops NULLs from a `NOT IN`: the predicate's
  English reading returns 334,487 rows (2.82×) by admitting every
  stage-unknown proceeding. Same extension today, worse contract.
- **Alternative rejected — agenda-edition presence:** the snapshot holds
  exactly one Unified Agenda edition (`202510`, 3,954 RINs) and
  `authority_edges` exists only for those RINs, so "present in the latest
  edition" is true of 100% of the candidate universe and discriminates
  nothing; `regulatory_agenda_items.latest_agenda_edition` is null for 34,051
  of 38,005 items for the same reason. Agenda stage also disagrees with action
  evidence: among proceedings tracked by the 42 U.S.C. 7401 RINs,
  `Completed Actions` RINs track two proceedings at `proposed` and
  `Proposed Rule Stage` RINs track one at `final`. Projecting a RIN-level
  plan onto every linked proceeding is the RIN-equality projection the
  ontology already forbids.
- **Revisit trigger:** a second Unified Agenda edition enters the snapshot
  (making edition presence informative), a stage-evidence path makes `prerule`
  or `longterm` reachable, or a discovery question needs "active" at the
  agenda-item level rather than the proceeding level.

## 2026-07-28 — Hyperbolic subsumption prototype: FAILS for grading

- **Decision:** the HiT-lineage hyperbolic scorer is rejected for
  relation grading. Zero-shot checkpoints (MiniLM-L12-WordNetNoun,
  MPNet-WordNetNoun, MiniLM-L12-SnomedCT, revisions pinned in the
  evidence) all scored below the 31/35 judge-agreement bar AND below a
  constant "always broader" predictor; fine-tuning was skipped by the
  pre-registered rule (below-baseline is not "promising").
- **Evidence:** `evidence/hyperbolic-subsumption-prototype-2026-07-28.md`
  (commit 61b01da) — measured mechanism: distances transfer, the
  centripetal depth gap collapses (7.47 → 1.33); encoder healthy on
  sanity pairs, breaks on OOV acronyms and coordinated compounds. The
  binding constraint is the eval set: 44/46 directional pairs are
  `broader` (a constant predictor scores 95.7%), n=69, 2 subsumed-by
  examples. Judges' cross-round stability on re-graded pairs: 90.3%.
- **Consequences:** holdout gold composition must include directional
  diversity (narrower/equivalent/denial pairs) or any future scorer
  evaluation stays trivially gameable; the evaluated framing (grade a
  pre-chosen candidate) is not the lineage's claim (retrieve subsumers).
- **Revisit trigger:** a recall@k retrieval test over the 513k registry
  (the lineage's actual claim), or a directionally balanced graded-pair
  set of meaningful size.

## 2026-07-28 — Two silent-failure classes in contract validation

- **A minted `requestContractDigest` is undetectable.** Core §2.4 (rulespec
  `e8794ba`) says a digest over an envelope invented to satisfy the field is
  non-conforming, but the fifth negative control proves no gate catches it:
  L1/L2/L3 all pass. Every compiled surface checks only
  `^sha256:[0-9a-f]{64}$`, and the conditional guard fires on
  `modelExtraction` to require *presence*, never on other methods to
  interrogate provenance. Verifying it would need the preimage of a digest
  the kernel treats as opaque by design. **Disposition:** accept as a
  producer obligation, not a checkable constraint; do not add a shape that
  pretends otherwise. Consumers must not read digest presence as evidence
  of an audited run.
- **A stale vendored context makes gates report green on unchecked edges.**
  Under the pre-`361348c` context copy, `rkaf:publishedInDocket` had no term
  definition, so it expanded as a string literal, `sh:class rkaf:Docket`
  never fired, and L3 would have reported 0 violations on an edge it never
  examined. Same mechanism for the ten untyped timestamp terms.
  **Disposition:** any projection or validation gate must pin the context to
  the same contract revision as the shapes, and treat "vendored artifacts
  age silently" as a first-class precondition. Recorded in the phase-4 gate
  preconditions.
- **Two corrections to the first projection pass, both recorded in its
  README:** offset drift is caught by the compiled `sh:class` on
  `assignmentEvidence`, not by `CarrierLocalFragmentUrnSourceAgreementShape`
  (which compares only the URN's artifact component against `oa:hasSource`);
  and rebuilding `compiled/` needs no `cue` toolchain —
  `tools/compile_all.sh` drives a pure-Python compiler, so gate freshness is
  cheaper than first reported.
- **Evidence:** `evidence/single-document-rulespec-projection-2026-07-28/`
  (commit `0d548d9`); five negative controls in `validation/`.
- **Revisit trigger:** a contract change that makes provenance verifiable
  (e.g. a published request-envelope schema), or a gate harness that pins
  context and shapes together automatically.

## 2026-07-28 — Formspec qualifies as the non-originating consumer

- **Decision (maintainer):** Formspec satisfies the "a non-originating
  consumer must review Rulespec" gate. Its Needs Specification Appendix C
  is a consumer review in substance — it exercised the boundary across
  seven points, recorded a verdict on each, kept four local, and filed
  three as proposals Formspec could not implement from its own side. The
  gate is no longer circular and no longer blocks a pre-1.0 release.
- **Recorded limitation:** Formspec and Rulespec share the Formspec-Labs
  umbrella, so this is independent-codebase review, not
  independent-organization review. Publish that distinction wherever the
  review is cited; do not describe it as third-party.
- **Consequence:** MVP-public's remaining blockers are the
  maintainer-authorized release itself and the release-preflight
  `_CONTRACT_FILES` rule fix (recorded under post-MVP repairs).
- **Revisit trigger:** a genuinely third-party consumer appears, at which
  point a real independent review supersedes this one.

## 2026-07-28 — usearch ANN: rejected as a swap for exact dense search

- **Decision:** keep exact brute-force cosine for the dense candidate
  channel. usearch (2.26.0) installs and runs fine; the experiment
  answered cleanly against it. Kept as an optional `ann` extra with its
  benchmark for the revisit case; not wired into any selector.
- **Evidence:** `evidence/usearch-ann-benchmark-2026-07-28.md` (commit
  `b893e7c`). Eight configurations over 513,236 concepts, scored against
  exact search as ground truth and against the 8-target oracle through
  the ablation harness's own scorer (its exact row reproduces the
  published ablation — 3/8, 4/8, 2/8, same labels — confirming faithful
  reuse). Memory and recall proved to be the same dial: the only
  configuration matching the exact oracle everywhere (`f32-hi`) holds
  1,617 MB against the baseline's 1,697 MB — a 4.7% saving. Cheaper
  settings lose 21-74% of the exact top-12.
- **Why recall is poor, measured not assumed:** duplicate concept strings
  ruled out (all 513,236 texts distinct); the exact top-50 sits inside a
  **0.056-wide cosine band**, so HNSW's greedy descent has almost no
  gradient to follow. Structural to this embedding distribution, not a
  usearch defect — any graph-based ANN will struggle on the same vectors.
- **Two traps recorded:** `v2+C` holds 4/8 even at 26% recall, which reads
  as "ANN is harmless" but actually means the dense channel is not
  carrying those targets — the lexical channels are. And aggregate recall
  does not predict target survival (`f16-c32` beats `f16` on recall,
  0/8 vs 2/8 on the oracle). Judge ANN by the oracle, never by recall
  alone.
- **No measured need exists:** 1.70 GB and 16 ms mean latency on a 48 GB
  machine; the research warning concerned >34 GB tooling. The ~50-minute
  cost is the *embedding* build, which ANN does not change (graph
  construction is 20 s - 4.5 min).
- **Revisit trigger:** the registry outgrows the serving memory budget, or
  concurrent workers make 1.7 GB/worker binding — start from
  `usearch-f16-hi` (1,107 MB, both fused oracles at parity).
