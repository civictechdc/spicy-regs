# Rulespec and Spicy Regs execution backlog

- **Status:** Active; local candidates exist, but the reshaped contract is
  unreleased and the fair comparison is open
- **Authority:** [`2026-07-25-rulespec-spicy-regs-complete-vision-goal.md`](docs/superpowers/specs/2026-07-25-rulespec-spicy-regs-complete-vision-goal.md)
- **Scope:** Rulespec, Spicy Regs, and their versioned interface
- **Active finish line:** the fair stopping point below
- **Archived backlog:** [`TODO-RULE-2026-07-25-pre-vision-rewrite.md`](docs/archive/TODO-RULE-2026-07-25-pre-vision-rewrite.md)
- **Last live validation:** 2026-07-25, on dirty local worktrees; not a release
  or clean-checkout certification
- **Labels:** verified locally, local and unreleased, open, human gate, deferred,
  or blocked

The vision owns the architecture and long-term definition of done. This file
contains only current state, executable work, gates, and deferred work.

An unchecked item is active unless its section says deferred. A checked item
must link a commit, immutable artifact, receipt, or dated validation record.
Do not convert an open release or review gate into “blocked” merely because it
needs coordination.

## Current state

- **Session-local validation, unreceipted:** the dirty Rulespec worktree passed
  312 fixtures with zero conformance divergences, code generation, and
  cross-target parity; the dirty Spicy Regs worktree passed 167 focused tests;
  `git diff --check` passed in both repositories. Final gates must replace
  these session results with durable receipts.
- **Verified historical evidence:** the bounded segmentation comparison chose
  `structure-overlap-1800` and completed its named OpenAI path; the RIN corpus
  proved durable `RegulatoryAgendaItem` identity without reusing a RIN for a
  document or `Proceeding`.
- **Local, unreleased:** Artifact lineage, SourceFragment, affirmed and denied
  assertions, source-aware segmentation, concept aggregation, comparison,
  checkpoints, resolver proofs, OpenAI, and Codex CLI candidates exist.
- **Open:** Rulespec still needs the core/profile split, `ValueAssertion`,
  evidence-bearing `ConceptAssignment`, composition repair, review, and a
  reachable release. Spicy Regs then needs the paired carrier migration.
- **Open evaluation:** production resolvers, the sealed v2 oracle, three
  blinded OpenAI repetitions, same-contract lookup arms, and final repository
  gates remain unfinished. Existing OpenAI and Codex diagnostics fail their
  declared quality gates.
- **Human gate:** a non-originating consumer must review Rulespec; two distinct
  humans must seal the oracle; a maintainer must authorize any Git tag,
  release, push, upload, publication, concept promotion, or legal-effect
  activation.

These checks are working-state evidence. They do not replace final lint, type,
documentation, full-suite, clean-checkout, release, or publication gates.

## Standing system rules

### RULE-005 — Drive shared contracts from consumer evidence

- Start each Rulespec change with a real carrier example, failed query,
  conformance fixture, or corpus receipt.
- Put reusable semantics, identifiers, constraints, and conformance behavior in
  Rulespec. Put source mapping, storage, ranking, and provider execution in
  Spicy Regs.
- Change normative meaning before CUE constraints and generated targets.
- Release Rulespec before Spicy Regs publishes data under the changed contract.
- Remove temporary consumer workarounds after migration.

### RULE-006 — Preserve upstream findings

- Search [`../rulespec/TODO.md`](../rulespec/TODO.md) before adding a shared
  finding.
- Fix a blocking shared defect in the current batch or add one deduplicated
  Rulespec item with corpus evidence and executable acceptance criteria.
- Keep source-specific limitations and ranking behavior in the Spicy Regs
  profile.
- Close a shared finding only after a released Rulespec contract passes against
  real Spicy Regs carrier data.

### Engineering constraints

- Treat CUE as the Rulespec constraint source and regenerate every affected
  target.
- Keep provider SDKs behind project-owned protocols and injected test doubles.
- Before writing substantial infrastructure, evaluate maintained packages that
  satisfy roughly 80 percent of the contract.
- Keep source tables authoritative and semantic projections append-only.
- Treat missing evidence and source silence as unknown.
- Derive task scope and output counts from the frozen contract and candidate
  manifest; never hard-code a historical artifact count.
- Keep temporary model chunks operational. Give ontology identity only to
  immutable Artifacts and meaningful source regions.
- Exclude public comments from the active document comparison. Do not delete
  or weaken the deferred participation profile.
- Keep oMLX outside the active baseline.

## Critical path

Complete the milestones in order. Corpus and reviewer preparation may run in
parallel, but Spicy Regs cannot complete migration before the Rulespec release,
and the comparison cannot freeze until the migrated carrier is stable.

## Milestone A — Reshape, review, and release Rulespec

**Preserved IDs:** `RULE-003`, `RULE-007`, `RULE-010`, `RULE-014`,
`RULE-015`, and the base contract from `RULE-040`.

- [ ] Update `../rulespec/TODO.md` with the complete reshape and link this
      backlog and its real carrier evidence.
- [ ] Move U.S. identifiers, `publishedInProceeding`, and domain lifecycle
      values from the universal kernel into explicit profiles.
- [ ] Fix CUE composition in every projector so generated formats preserve
      composed constraints.
- [ ] Define a small `AssertionEnvelope` plus distinct
      `RelationshipAssertion` and typed-literal `ValueAssertion` contracts.
- [ ] Keep immutable proposition content separate from acceptance,
      disposition, confidence, attestation, and mutable consumer state.
- [ ] Separate source claimant, extraction provenance, model derivation, and
      human approval.
- [ ] Define evidence bindings, confidence, derivation lineage, attestations,
      applicability, time, and access scope without provider-owned types.
- [ ] Finish immutable Artifact version and revision identity without
      inventing lineage absent from the source.
- [ ] Stabilize `SourceFragment` identity with exact artifact, selector,
      coordinate-system, and content-digest bindings.
- [ ] Add `ConceptScheme`, SKOS-compatible concepts and mappings, and
      evidence-bearing `ConceptAssignment` for Artifacts and SourceFragments.
- [ ] Place relation changes, comparison contexts, resolver proofs, and neutral
      findings outside the kernel.
- [ ] Keep `ClosureClaim` Experimental and disabled.
- [ ] Update normative prose, CUE, context, vocabulary, SHACL, SDK types,
      runtime behavior, fixtures, and reference corpora as each contract
      requires.
- [ ] Add semantic carrier tests for identity, direction, typed values,
      transformations, evidence resolution, composition, and profile
      isolation; shape-only parity is insufficient.
- [ ] Run the complete Rulespec compile, parity, fixture, runtime, and
      conformance gates from a clean checkout.
- [ ] Complete the non-originating-consumer review and resolve its findings.
- [ ] With maintainer authorization, publish one reachable pre-1.0 release and
      record its immutable contract digest.

**Done when:** a fresh checkout resolves one reviewed release containing the
kernel, concepts, analysis contracts, and profile boundary needed by the fair
comparison.

## Milestone B — Migrate Spicy Regs and complete the tag carrier

**Preserved IDs:** `RULE-011`, `RULE-012`, `RULE-013`, `RULE-030`, and the
carrier portion of `RULE-040`.

- [ ] Pin the released Rulespec version, canonical URL, archive, and contract
      digest.
- [ ] Update the application profile and carrier maps for Artifacts,
      SourceFragments, relationship assertions, value assertions, concepts,
      schemes, assignments, evidence, confidence, lineage, attestations,
      comparisons, and findings.
- [ ] Preserve specialized source tables and raw values; do not flatten source
      grains into one record.
- [ ] Expose Artifact and SourceFragment concept assignments as distinct,
      queryable, evidence-bearing records.
- [ ] Roll accepted fragment assignments into document assignments through one
      versioned aggregation policy and supporting-assignment proof.
- [ ] Use document assignments only to shortlist fragment candidates. Require
      fresh local source evidence before accepting a fragment assignment.
- [ ] Use `structure-overlap-1800` as the current segmentation baseline.
      Preserve exact offsets, hierarchy, tokenizer evidence, and truncation
      status.
- [ ] Install production resolvers that return dereferenceable, content-bound
      proof records; retain static adapters as fixtures.
- [ ] Backfill only records reconstructable from immutable source artifacts.
      Quarantine unresolved identity, lineage, evidence, or scope.
- [ ] Publish accepted, rejected, unknown, quarantined, zero-result, and failed
      counts for every output declared by the generation manifest.
- [ ] Make the semantic generation atomic and bind it to source snapshots,
      code, contract, policy, prompt, model, and artifact digests.
- [ ] Update table documentation, `docs/rulespec-profile.md`,
      `docs/ontology.md`, and `docs/index.md` to match the migrated carrier.
- [ ] Run full row-level conformance, referential-integrity, projection,
      aggregation, and round-trip tests.

**Done when:** Spicy Regs consumes the released contract without shadow
vocabulary and can trace every document or fragment tag to exact evidence.

## Milestone C — Freeze the mixed corpus and human oracle

**Preserved IDs:** evaluation portions of `RULE-028` and `RULE-033`.

- [ ] Create `docs/retrieval-evaluation.md` as the single evaluation contract:
      scope, hashes, splits, metrics, thresholds, latency budget, result size,
      and exclusion rules.
- [ ] Freeze one document-only generation with real related, unrelated,
      ambiguous, explicit-denial, change, and hard-negative cases from several
      legal and regulatory source families.
- [ ] Bind each item to immutable Artifact identity, version, stable
      SourceFragments, raw-source provenance, access scope, and content hashes.
- [ ] Reuse the strongest existing real data where compatible; do not treat
      `no_declared_relation` as proof that no real-world relationship exists.
- [ ] Exclude public comments and comment-derived records from this benchmark.
- [ ] Freeze train, development, and untouched holdout roles before final
      comparison. Never tune a prompt or threshold against the holdout oracle.
- [x] Treat the five disputed relation-exclusion cases as an exposed regression
      set. Integrate the provider-neutral proof-certificate v2 prompt,
      assertion/change-event split, orthogonal time/attribution/conditionality,
      claimant normalization, and independent evidence-boundary scoring.
- [x] Persist the focused OpenAI receipts as non-publication,
      non-benchmark diagnostics. Do not use their scores to rank providers or
      set production thresholds.
- [ ] Have two distinct blinded humans review the explicit-denial v2 corpus,
      seal both reviews, resolve disagreements, and freeze the exposed cases as
      a reviewed regression oracle.
- [ ] Draw and freeze a new untouched holdout under the same v2 contract before
      any fair provider comparison.
- [ ] Freeze the strict provider schema, prompt template, normalization rules,
      acceptance policy, and comparison policy.
- [ ] Emit a corpus receipt covering membership, source diversity, label
      balance, endpoint integrity, leakage, duplicates, and all content IDs.

**Done when:** every arm receives the same immutable inputs and the final oracle
cannot change in response to model output.

## Milestone D — Run the fair lookup and provider comparison

**Preserved IDs:** `RULE-025`, `RULE-026`, `RULE-027`, `RULE-028`, and
`RULE-033`.

- [ ] Run direct citation and deterministic graph-path lookup with typed
      direction and exact evidence.
- [ ] Implement the clean shared runner in the
      [Spicy Regs Document Pipeline v3](docs/superpowers/specs/2026-07-25-spicy-regs-document-pipeline-v3-design.md).
      Move the v2 relationship prompt, schema, response checks, scorer, and
      human-review gate into that runner instead of building a separate v2
      pipeline. The same code must handle exposed diagnostics and untouched
      benchmarks, use OpenAI or Codex through small adapters, preserve failed
      and empty work, resume safely, rebuild without another provider call,
      and record the final dataset and human-review file hashes.
- [ ] Run lexical, dense, hybrid, and fixed-depth reranked retrieval against the
      same candidate universe and prefilters.
- [ ] Keep whole-artifact retrieval as a routing view and SourceFragment
      retrieval as the evidence-finding view.
- [ ] Apply identity, version, authority, scope, access, graph, and concept
      filters before semantic ranking.
- [ ] Run the direct OpenAI adapter and tool-free Codex CLI adapter through the
      same project-owned structured-output contract. Treat them as separate
      provider arms.
- [ ] Run three identical blinded OpenAI repetitions after the oracle, prompt,
      schema, and policies are frozen.
- [ ] Record candidates, exclusions, fusion, reranking, graph paths, source
      spans, proof records, prompts, responses, usage, failures, and digests in
      strict receipts.
- [ ] Report retrieval quality, source diversity, latency, coverage, and
      explanation completeness.
- [ ] Report extraction, evidence, acceptance, and comparison quality
      separately; include calibration, abstention, and run variance.
- [ ] Preserve unrelated controls and weak-evidence cases. Similarity and
      missing joins must not become factual edges.
- [ ] Compare results with `structure-overlap-1800`, incumbent BGE dense
      embeddings, and the pinned `BAAI/bge-reranker-v2-m3` CrossEncoder; state
      whether another arm materially improves that baseline.

**Done when:** the comparison is reproducible, inspectable, and fair enough to
choose the next pipeline investment without claiming perfection.

## Milestone E — Prove reliability and prepare paired delivery

- [ ] Inject parser, provider, resolver, network, validation, checkpoint, and
      publication failures; prove fail-closed outcomes.
- [ ] Prove retry and resume produce no duplicate accepted records, lost
      failures, skipped inputs, or mixed generations.
- [ ] Prove deterministic outputs where promised and record nondeterministic
      provider variance where it exists.
- [ ] Prove zero-result, abstention, unknown, quarantine, and terminal-failure
      records survive publication.
- [ ] Enforce access scope through extraction, indexing, retrieval, receipts,
      caches, and exports.
- [ ] Prove secrets never enter prompts, subprocess arguments, logs, receipts,
      artifacts, or Git.
- [ ] Exercise backfill rollback and restore the prior published generation
      without rewriting history.
- [ ] Run complete tests, lint, types, documentation, secret scans, generated
      parity, semantic carrier checks, and clean-checkout reproduction in both
      repositories.
- [ ] Produce one paired receipt containing both commits, the reachable
      Rulespec release and digest, source snapshot IDs, evaluation contract ID,
      generation manifest, every manifest-declared artifact hash, and all gate
      results.
- [ ] Resolve or explicitly disposition the still-open edge findings for docket
      normalization, concept hierarchy, parser quality, stale targets, and
      atomic evaluation checks.
- [ ] Verify that Milestone A's released Rulespec tag and digest remain
      reachable and match the paired receipt. With maintainer authorization,
      commit the Spicy Regs migration. Do not upload a semantic generation
      without separate publication authorization.

**Done when:** a new operator can reproduce the pair from clean checkouts,
observe safe failure behavior, and roll back without semantic data loss.

## Fair stopping point

Stop and compare approaches when all items below hold:

- [ ] One reviewed, reachable Rulespec release contains the corrected
      core/profile boundary, immutable lineage, relationship and value
      assertions, concepts and assignments, evidence and attestations, and
      provider-neutral comparison proofs.
- [ ] Spicy Regs pins that release and uses no shadow vocabulary.
- [ ] One frozen mixed real-data generation contains related and unrelated
      documents from several source families with Artifact and SourceFragment
      tags.
- [ ] Fragment assignments roll up through a recorded proof; document tags
      only guide fragment candidates.
- [ ] Direct, deterministic, lexical, dense, hybrid, reranked, and inferred
      lookup arms use the same evaluation contract.
- [ ] Real OpenAI and Codex CLI arms complete with strict receipts.
- [ ] Two human reviews seal the oracle, and three blinded OpenAI repetitions
      report separate extraction, evidence, acceptance, and comparison metrics.
- [ ] Failure, resume, determinism, access, secret, rollback, paired-receipt,
      and clean-checkout gates pass.
- [ ] Omission findings and domain legal-effect interpretation remain disabled.

This finish line supports a fair decision. It does not require exhaustive
corpus coverage, closure, online serving, or decision-grade legal semantics.

## Deferred roadmap

- **`RULE-020`–`RULE-023`:** complete every regulatory, legislative,
  participation, judicial, oversight, organization, influence, and spending
  profile. Public comments return here after the document-only benchmark.
- **`RULE-031`–`RULE-034`:** automate concept evolution, relationship
  discovery, drift response, and reversible feedback after the base contracts
  pass across at least two source families.
- **`RULE-041`–`RULE-042`:** add human-governed concept promotion and an
  external or federated concept registry.
- **Longitudinal omission:** validate profile-specific expected coverage and
  bounded closure before enabling `expected_relation_not_observed`.
- **`RULE-050`–`RULE-052`:** resolve immutable legal authority, domain legal
  effects, and traceable decision packets with human review.
- **Serving:** choose an online vector store, graph engine, GraphRAG layer, or
  public API only after the frozen evaluation proves a measured need.
- **Local inference:** reconsider MLX or oMLX only as a separately measured
  provider arm.

## Legacy ID map

| Legacy IDs | Current location |
| --- | --- |
| `RULE-001`, `RULE-002`, `RULE-004`, `RULE-013` | Completed history in the archived backlog |
| `RULE-003`, `RULE-007`, `RULE-010`, `RULE-014`, `RULE-015`, base `RULE-040` | Milestone A |
| `RULE-011`, `RULE-012`, carrier `RULE-040` | Milestone B |
| `RULE-025`–`RULE-028`, `RULE-033` | Milestones C and D |
| `RULE-030` | Milestone B |
| `RULE-020`–`RULE-023`, `RULE-031`–`RULE-034`, `RULE-041`–`RULE-042`, `RULE-050`–`RULE-052` | Deferred roadmap |
| `RULE-005`, `RULE-006` | Standing system rules |

## Evidence and detailed contracts

- [`docs/rulespec-profile.md`](docs/rulespec-profile.md)
- [`docs/ontology.md`](docs/ontology.md)
- [`RIN ontology revision report`](docs/rin-ontology-revision-report.md)
- [`mixed real-data corpus report`](docs/mixed-real-data-corpus-report.md)
- [`corpus edge coverage findings`](docs/corpus-edge-coverage-findings-2026-07-24.md)
- [`document segmentation fair comparison`](docs/evidence/document-segmentation-fair-comparison-2026-07-24.md)
- [`relationship assertion release and migration`](docs/superpowers/specs/2026-07-25-relationship-assertion-release-migration.md)
- [`relationship comparison resolver contract`](docs/superpowers/specs/2026-07-25-relation-comparison-resolver-contract.md)
- [`longitudinal omission design`](docs/superpowers/specs/2026-07-25-longitudinal-relation-omission-design.md)
- [`v2 human adjudication protocol`](docs/superpowers/specs/2026-07-25-relation-exclusion-v2-human-adjudication-protocol.md)
- [`Codex CLI provider`](docs/codex-cli-provider.md)
- [`recent document relation and lookup research`](docs/evidence/recent-document-relation-lookup-research-2026-07-25.md)
- [`relation assertion adversarial review`](docs/evidence/relation-assertion-adversarial-review-2026-07-25.md)
- [`Spicy Regs Document Pipeline v3`](docs/superpowers/specs/2026-07-25-spicy-regs-document-pipeline-v3-design.md)

If a linked supporting document conflicts with the canonical vision or a
released Rulespec contract, update or archive the supporting document. Do not
maintain two active meanings.
