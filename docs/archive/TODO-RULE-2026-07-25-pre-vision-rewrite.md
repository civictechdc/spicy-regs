# Archived Rulespec-backed metadata architecture

> Archived on 2026-07-25 after the canonical vision separated program design
> from execution. Preserve this file for historical status, detailed runbooks,
> and legacy `RULE-*` references. Use the live root-level `TODO-RULE.md` for
> current work.

**Historical status at archive:** `RULE-007` automated release-candidate gates
complete; `RULE-014` design documented but unreleased
**Scope:** Spicy Regs, the sibling Rulespec repository, and their versioned interface
**Parent vision:** `docs/superpowers/specs/2026-07-25-rulespec-spicy-regs-complete-vision-goal.md`
**Design lineage:** `docs/superpowers/specs/2026-07-23-regulatory-ontology-program-overview.md`
**Current stabilization review:** `../rulespec/thoughts/reviews/2026-07-24-rulemaking-condition2-adversarial-review.md`

## Architectural rule

Rulespec is the canonical ontological and semantic model behind Spicy Regs.
Spicy Regs is the concrete data, pipeline, and query implementation of that
model.

Parquet remains the primary analytical carrier and does not need to mirror
Rulespec's JSON-LD structure. Every durable Spicy Regs entity, relationship,
assertion, category, lifecycle event, and provenance field must still have one
of these explicit outcomes:

1. a valid projection into Rulespec;
2. a documented composition with an external standard that Rulespec recognizes;
3. a documented `spicy-regs` extension with a reason it remains local and
   criteria for later standardization.

Rulespec L0 is the first carrier boundary, not the architectural ceiling.

This backlog may directly refactor the Rulespec repository at `../rulespec`.
Rulespec is an active part of the program, not a read-only vendor dependency.
When corpus evidence exposes a general semantic, validation, projection, or
runtime defect, fix the source contract in Rulespec and then migrate Spicy Regs
to it. Do not preserve a known upstream defect through a permanent local
workaround.

Preserve semantic roles. A proceeding, docket, published artifact, source
fragment, authority, organization, concept, assertion, and evidence record must
not collapse into one generic "document" type merely because they share a
physical table or query interface.

## Primary product goal

Given any document or question, a user can filter the entire Spicy Regs corpus
through shared concepts, entities, laws, regulations, agencies, proceedings,
dates, and legal status. The user can also discover related artifacts whether
or not the sources cite each other directly.

Every returned relationship must identify how Spicy Regs established it:

| Relationship class | Basis |
| --- | --- |
| **Direct** | A source explicitly cites, links, contains, or names another artifact. |
| **Deterministic** | Normalized identifiers or a typed graph path connect the artifacts through a docket, RIN, proceeding, CFR unit, statute, organization, or other stable entity. |
| **Semantic** | Shared concepts, entities, authorities, obligations, affected populations, outcomes, or measured content similarity make the artifacts relevant to the same inquiry. |
| **Inferred** | A model proposes a typed relationship that is not present in the source graph. The result remains an assertion with evidence, confidence, and provenance. |

Rulespec defines the shared meaning of filters, entities, and relationships.
Spicy Regs builds the indexes, graph traversal, embeddings, ranking, and
continuous improvement that make those semantics useful.

## Desired outcome

Spicy Regs should:

- filter all supported artifact types through one composable query surface;
- find and rank directly linked and uncited-but-related artifacts;
- explain every result through its relationship class, evidence, graph path,
  confidence, and provenance;
- represent every supported corpus type through one coherent semantic profile;
- connect artifacts across rulemaking, legislation, litigation, oversight,
  participation, organizations, and spending;
- derive metadata and relationships with complete evidence, confidence, and
  lineage;
- improve retrieval-grade tags and categorizations as the corpus changes;
- preserve every merge, split, replacement, rejection, and validation event;
- promote a small number of proven local concepts through human-reviewed
  Rulespec governance;
- expose the same semantics through Parquet, MCP, SQL, and optional graph
  projections.

## Guardrails

- Keep source tables authoritative for source-specific fields.
- Use a semantic projection to connect tables; do not replace distinct source
  grains with a universal record.
- Preserve raw source values beside normalized identifiers.
- Treat missing evidence as unknown.
- Treat source silence as unknown unless a profile proves a bounded closure
  claim for the exact artifact version, scope, and extraction run.
- Supersede assertions; never rewrite or delete their history.
- Keep provider SDKs behind project-owned protocols; core comparison and
  ontology records must not depend on a provider.
- Keep retrieval-grade concepts separate from decision-grade registered
  concepts.
- Require human approval for promotion into shared or decision-grade
  vocabularies.
- Keep ordinary Spicy Regs queries independent of the Rulespec runtime.
- Add vocabulary only after at least one real corpus and one concrete query
  demonstrate the need.
- Refactor Rulespec directly when a proven gap belongs in the shared substrate.
- Add deferred shared gaps to `../rulespec/TODO.md`; do not leave them only in a
  Spicy Regs report, chat, or local workaround.
- Keep Spicy-specific storage, ranking, model orchestration, and search behavior
  in Spicy Regs or its application profile.
- Never patch generated Rulespec artifacts without changing their normative or
  generated source.

## Current implementation slice

The immediate objective is to repair the Experimental US rulemaking module
against the 2026-07-24 review, prove the repaired contract on the Spicy Regs
corpus, and publish a reachable contract only after its gates hold.

A second Experimental workstream now covers generic relationship assertions,
explicit denials, neutral comparisons, and future longitudinal omission.
`RULE-014` tracks that work. It does not change the current rulemaking release
order or enable omission findings.

Execute this slice in this order:

1. Run the cross-repository preflight below.
2. Complete `RULE-001`, the regulatory portion of `RULE-002`, and `RULE-004`.
3. Execute `RULE-007`, applying the standing `RULE-005` refactor procedure and
   `RULE-006` feedback procedure to every workstream.
4. Run the paired Rulespec and Spicy Regs gates against one corpus generation.
5. Stop for the maintainer-operated review, release, and publication gates.
6. Complete `RULE-003` only after the maintainer confirms that a
   non-originating consumer reviewed the repaired contract or ratified the
   simulated review against it.

For `RULE-007`, "the corpus" means the nine outputs declared by
`OntologyDatasetPipeline.published_outputs` at the recorded Spicy Regs commit:
`rule_targets`, `authority_edges`, `proceedings`, `regulatory_agenda_items`,
`agenda_item_proceedings`, `comment_periods`, `concepts`,
`concept_assignments`, and `concept_events`. Bind every run to the generated
`ontology-dataset-manifest.json` snapshot id and artifact hashes. After
`RULE-002`, "supported corpus" means the sources listed in
`docs/rulespec-profile.md` at a recorded commit and generation digest.

One paired gate receipt must bind the Rulespec commit and contract digest, the
Spicy Regs commit, the candidate manifest snapshot id, every candidate artifact
hash, and every command result. A command that cannot name or record that
receipt's candidate is a local check, not evidence that the corpus gate passed.

Agents may edit, refactor, generate artifacts, and run local gates. Tagging a
release, publishing data, selecting or speaking for an external reviewer,
claiming the independent gate, promoting a shared concept, or approving an
inferred legal effect requires explicit maintainer or user authorization.

## Operating definitions

- **L0:** A version-pinned mapping from a non-JSON-LD carrier to registered
  Rulespec terms, identifiers, and enums. It does not claim L1-L4 validation or
  runtime conformance.
- **Application profile:** The documented Spicy Regs choices that bind its
  tables, identifiers, relationships, and local extensions to Rulespec.
- **Semantic generation:** One manifest-addressed, atomic publication of all
  related metadata tables and their evidence.
- **Retrieval-grade:** Metadata used to find and rank material; it may evolve
  with measured evidence and must expose uncertainty.
- **Decision-grade:** Semantics safe for legal or eligibility decisions; they
  require registered meaning, exact evidence, applicable versions, and the
  declared conformance gate.
- **Non-originating consumer:** A reviewer or implementation team that did not
  design the module or operate its originating corpus exercise. The Rulespec
  maintainer's simulated personas do not qualify.

## Cross-repository execution rule

Classify each task before implementation:

- **Spicy Regs change:** physical storage, ingestion, model execution, ranking,
  retrieval, or a consumer-specific profile.
- **Rulespec change:** reusable semantics, identifier schemes, conformance,
  constraints, projectors, registries, or runtime behavior.
- **Paired change:** a Rulespec contract change plus the Spicy Regs migration
  that proves and consumes it.

Rulespec owns reusable filter vocabulary and semantic meaning. Spicy Regs owns
the profile mapping, query syntax, pagination, ranking, and serving behavior.
Adding a reusable filter concept requires a paired change; changing how Spicy
Regs serves an existing concept does not.

Before editing either repository, run and record:

```bash
git status --short --branch
git rev-parse HEAD
git -C ../rulespec status --short --branch
git -C ../rulespec rev-parse HEAD
cat ../rulespec/VERSION
python3 ../rulespec/tools/l0_mapping_audit.py --print-contract-version
```

Preserve unrelated work in both trees. Record the paired task identifier,
branches, baseline Rulespec release and digest, Spicy Regs generation digest,
and baseline gate results. Do not overwrite another contributor's changes or
mix unrelated work into either commit.

Every Spicy Regs build, test, corpus run, and retrieval evaluation must triage
new semantic or conformance findings:

1. **Fix in Rulespec now** when the shared defect blocks current work and the
   evidence supports a complete correction.
2. **Add or update a Rulespec backlog item** when the gap belongs in the shared
   substrate but needs more design, another consumer, or later implementation.
3. **Keep in the Spicy Regs profile** when the behavior is specific to this
   corpus, carrier, ranking system, or model workflow.
4. **Record as data friction** when the source lacks enough evidence to support
   a semantic claim.

Before adding an item, search `../rulespec/TODO.md` for the same gap. Strengthen
an existing item with new evidence instead of creating a duplicate.

For a paired change:

1. Capture the failing corpus example, query, or conformance fixture in Spicy
   Regs.
2. Decide whether an existing public ontology or Rulespec primitive solves the
   problem.
3. Record the semantic decision in Rulespec before changing validation code.
4. Update the Rulespec specification, CUE source, context, vocabulary inventory,
   fixtures, reference corpus, projectors, SDK types, and runtime behavior that
   the decision affects.
5. Regenerate derived artifacts and run the complete Rulespec gate.
6. Update the Spicy Regs application profile, carrier mapping, code, tests,
   self-certification, and pinned Rulespec version.
7. Run the complete Spicy Regs gate and the Rulespec audit against the real
   carrier.
8. Commit each repository separately with the same task identifier and
   cross-references.
9. Release Rulespec before publishing Spicy Regs data that claims the changed
   contract.

Minimum paired verification:

```bash
# Run when a Rulespec source change affects generated targets.
make -C ../rulespec compile

# Run for every Rulespec refactor.
make -C ../rulespec test

# Run against the migrated Spicy Regs carrier.
python3 ../rulespec/tools/l0_mapping_audit.py docs/ontology.md
uv run pytest
uv run ruff check .
```

Additional `RULE-007` corpus and conformance gates:

```bash
# On the pre-repair commit, build the baseline without rerunning model work.
uv run materialize-ontology \
  --output-dir output/rulespec-stabilization-baseline \
  --no-full-refresh \
  --skip-upload

# On the repair commit, build the candidate from the same inputs and prior state.
uv run materialize-ontology \
  --output-dir output/rulespec-stabilization-candidate \
  --no-full-refresh \
  --skip-upload

# Audit the actual partner declaration and carrier map.
python3 ../rulespec/tools/l0_mapping_audit.py conformance/rulespec-l0.yaml

# Emit the Rulespec self-certification for review before replacing the tracked file.
(cd ../rulespec && python3 tools/conformance_report.py --self-certify)
```

Run the baseline and candidate in separate clean worktrees. The materialization
commands require the declared source inputs, a prior complete generation, and
configured source access. Their manifest `inputs` records must match exactly;
otherwise the before/after comparison is invalid and must be rerun. Record both
manifests, row counts, exclusions, quarantines, and review metrics in the repair
report.

`RULE-007` must add the narrow `RULE-013` corpus-receipt capability if no
existing command can validate the seven candidate artifacts by manifest path.
Until that receipt exists, the commands above are useful local checks but do
not complete the same-generation corpus gate. Missing dependencies,
credentials, source snapshots, or human approvals are blockers, not skipped
gates. Publication uses the reviewed `materialize-ontology` workflow and is
never part of an unattended validation run.

## Start order

1. Complete `RULE-001`, the regulatory inventory in `RULE-002`, and `RULE-004`
   before changing or reviewing the repaired contract.
2. Execute `RULE-007` with `RULE-005` and `RULE-006` applied as standing
   procedures, not one-time prerequisites.
3. Complete `RULE-003` only after the paired gates and non-originating-consumer
   gate hold.
4. Finish `RULE-002` before adding more ontology tables or terms.
5. Complete `RULE-010` through `RULE-013` for the existing regulatory slice.
6. Complete the release and migration gates in `RULE-014` before
   `RULE-032` publishes relation discrepancies or omissions.
7. Apply the same profile and projection to every remaining corpus family.
8. Complete `RULE-025` through `RULE-028` and prove the primary product goal.
9. Expand the learning loop only after the shared assertion and evidence model
   works across at least two distinct corpus families.

## Phase 0 — Establish the contract

### RULE-001 — State the architecture in the parent overview

- [x] Replace the weak "vocabulary substrate" framing with the architectural
      rule above.
- [x] State that Spicy Regs is a Rulespec application profile implemented in
      Parquet and pipelines.
- [x] State that the current regulatory ontology is the first vertical slice,
      not the final metadata boundary.
- [x] Distinguish fast-changing local semantics from stable shared semantics.

**Done when:** A contributor can read the overview and correctly explain which
repo owns semantic contracts, physical data, model execution, conformance, and
concept promotion.

### RULE-002 — Write the Spicy Regs Rulespec application profile

- [x] Create `docs/rulespec-profile.md`.
- [x] Inventory every published table and classify its grain and semantic role.
- [x] For each table, record:
  - local key and canonical IRI strategy;
  - Rulespec or composed class;
  - identity scheme and source authority;
  - version, edition, and snapshot semantics;
  - containment and cross-table relationships;
  - assertions the table can support;
  - evidence and provenance requirements;
  - current projection and conformance status;
  - deliberate local extensions and unresolved gaps.
- [x] Cover at least:
  - regulations.gov dockets, documents, and comments;
  - Federal Register documents and Unified Agenda records;
  - proceedings and comment periods;
  - CFR units, congressional bills, and public laws;
  - FCC proceedings and filings;
  - court dockets, current Supreme Court opinion packages, and later court
    orders;
  - GAO and CRS reports;
  - lobbying filings and FEC committees;
  - SAM entities and USASpending recipients.
- [x] Classify every gap as `compose`, `profile`, `Rulespec candidate`, or
      `source data unavailable`.

**Done when:** Every current table has a semantic home without flattening
containers, artifacts, versions, actors, and assertions into one type.

### RULE-003 — Publish the consumed Rulespec contract

- [x] Record the maintainer-operated adversarial simulated-consumer review
      without representing it as an independent review.
- [x] Record the review decisions for cross-postings, unknown authority, and
      proceeding-stage names.
- [x] Complete the `RULE-007` automated repair batch while the module remains
      Experimental.
- [ ] Obtain a non-originating consumer review of the repaired contract or
      ratification of the simulated review against it.
- [ ] Publish a reachable Rulespec release containing the identifier schemes,
      L0 contract, and rulemaking module.
- [ ] Replace the content-digest-only dependency with a released version plus
      its immutable digest.
- [x] Make production ontology publication fail closed until the declaration
      names a matching, reachable canonical release URL and its tag archive
      recomputes to the declared contract digest; retain `--skip-upload` for
      candidate review.
- [ ] File the Spicy Regs partner self-certification in the Rulespec repository.
- [ ] Regenerate Rulespec's stale reference self-certification.

**Done when:** A fresh checkout can resolve, audit, and reproduce the exact
Rulespec contract without access to a sibling worktree or private branch.

### RULE-004 — Define compatibility and change governance

- [x] Document which Rulespec terms Spicy Regs treats as stable.
- [x] Define migration rules for pre-1.0 term, shape, and identifier changes.
- [x] Require a compatibility report before Spicy Regs changes its pinned
      Rulespec release.
- [x] Record maintainership overlap and the independent-review requirement.
- [x] Define the evidence required before Spicy Regs proposes a new core term.

**Done when:** A Rulespec change cannot silently alter a published Spicy Regs
meaning.

### RULE-005 — Refactor Rulespec from consumer evidence (standing procedure)

Apply this checklist to each paired refactor batch. Do not mark the procedure
globally complete after one batch.

- [ ] Treat the live corpus, failed query, or missing projection as the
      reproducible input to each upstream refactor.
- [ ] Classify the change as a composition fix, application-profile addition,
      reusable Rulespec feature, validator defect, runtime defect, or spec
      ambiguity.
- [ ] Keep consumer-specific behavior out of the universal Rulespec core.
- [ ] Change normative prose before constraints when semantics change.
- [ ] Update CUE, JSON-LD context, vocabulary inventory, generated schemas and
      SDK types, fixtures, reference corpora, conformance tools, and runtime
      code as required by the affected layer.
- [ ] Add positive, negative, edge, and synthetic-defect coverage for each new
      or changed invariant.
- [ ] Run `make compile` when generated targets change and `make test` for every
      refactor.
- [ ] Publish a friction or batch report showing the initial failure,
      classified cause, correction, and final gate results.
- [ ] Cut a Rulespec release, then update Spicy Regs' version pin, digest,
      profile, mapping, implementation, tests, and self-certification.
- [ ] Remove temporary Spicy Regs workarounds after the released Rulespec
      contract replaces them.

**Applied correctly to a batch when:** The corpus-driven gap produces a tested
Rulespec refactor and verified Spicy Regs migration without shadow vocabulary,
copied validators, or an unreleased sibling dependency.

### RULE-006 — Feed Spicy Regs findings into the Rulespec backlog (standing procedure)

Apply this checklist to every build, test, corpus run, and retrieval evaluation.
Do not mark the procedure globally complete after one run.

- [ ] Add Rulespec-backlog triage to the completion checklist for Spicy Regs
      builds, tests, full-corpus runs, and retrieval evaluations.
- [ ] Search `../rulespec/TODO.md` before creating a new item.
- [ ] Create or update a Rulespec item when evidence reveals a reusable semantic,
      identifier, conformance, constraint, projector, registry, SDK, or runtime
      gap that will not be fixed in the current task.
- [ ] Give every upstream item:
  - a concise problem statement;
  - the originating Spicy Regs task, test, query, or corpus snapshot;
  - observed and expected behavior;
  - representative evidence and counts;
  - the affected Rulespec layer and artifacts;
  - the reason the gap belongs in Rulespec rather than the Spicy Regs profile;
  - current workaround and user impact;
  - dependencies, including any second-consumer or independent-review gate;
  - executable acceptance criteria.
- [ ] Link the Rulespec item from the originating Spicy Regs report or task.
- [ ] Update the same item when later corpus runs add evidence, disprove the
      premise, or change its priority.
- [ ] Close an upstream item only after Rulespec implements the change and Spicy
      Regs verifies the released contract against real carrier data.
- [ ] Keep speculative vocabulary ideas out of the backlog until a corpus,
      query, or consumer demonstrates the missing distinction.

**Applied correctly to a run when:** Every reusable gap found while building or
testing Spicy Regs is fixed immediately or preserved as a deduplicated,
evidence-backed, acceptance-testable item in the canonical Rulespec backlog.

### RULE-007 — Repair the experimental rulemaking module

The 2026-07-24 adversarial review concluded **do not graduate as-is**. It
validated the architecture but found repairable semantic, identity, and
enforcement defects. Its maintainer-operated personas do not satisfy the
non-originating-consumer gate.

| Workstream | Review findings |
| --- | --- |
| Shape and graph invariants | F-21, F-24, agenda item 1d |
| Producible and directed rule targets | F-8, F-15, F-16 |
| Complete CommentPeriod evidence | F-9, F-17, F-20 |
| Terminal state and external legal events | F-1, F-2, F-3, F-4 |
| Proceeding identity evidence and continuity | F-6, F-7 |
| Agenda decisions and cross-posting | F-19 and agenda items 1-3 |
| Identifier grammar and tooling | F-10, F-18, F-22, F-23 |
| Honest stabilization gate | Review precondition 8 |
| Non-blocking future trigger | F-13 |
| Refuted; no implementation work | F-5, F-11, F-12, F-14 |

- [x] Add the review's eight graduation workstreams and all finding references
      to the canonical `../rulespec/TODO.md`.
- [x] Complete the Rulespec repair batch for graph invariants, producible rule
      targets, complete comment periods, terminal and external legal events,
      identity continuity, the three agenda decisions, and grammar/tooling
      hygiene.
- [x] Add Spicy Regs regression and Rulespec conformance fixtures for every
      review finding
      grounded in this corpus, including the reproduced shape attacks.
- [x] Stop creating information-free agency Authority nodes merely to satisfy
      `hasAuthority`; absence must mean unknown.
- [x] Migrate the six stage enum mappings and enforce agreement with the latest
      stage-family lifecycle event.
- [x] Retain and project docket-anchored comment periods instead of dropping
      known intervals when Proceeding identity is unresolved.
- [x] Project RIN evidence, directional proceeding continuity, citation-level
      affected targets, and canonical cross-posting links under the repaired
      contract; support produced editions while reporting their source-backed
      absence in this snapshot.
- [x] Rerun the full corpus and publish before/after counts for target coverage,
      retained comment periods, placeholder authorities, invalid identifiers,
      shape violations, and unresolved evidence.
- [x] Prove the baseline and candidate manifests have identical input and prior
      state hashes; keep their outputs in separate directories.
- [x] Produce one paired gate receipt binding the Rulespec commit and contract
      digest, Spicy Regs commit, candidate snapshot id, all seven artifact
      hashes, and every Rulespec, Spicy Regs, L0, and corpus-validation result.
- [x] Regenerate the Spicy Regs conformance artifact against the final paired
      receipt.
- [ ] At the release cut, regenerate Rulespec's reference and partner
      conformance artifacts and replace the candidate digest with the released
      version plus digest.
- [ ] Obtain a non-originating consumer review or ratification against the
      repaired artifacts before graduation.

**Done when:** All review preconditions have normative Rulespec decisions and
generated enforcement; Spicy Regs consumes the released contract without local
shadow semantics or known data loss; both full gates pass; and the independent
gate is honestly satisfied.

## Phase 1 — Build the semantic carrier

### RULE-010 — Define stable semantic identity across source tables

- [ ] Specify canonical IRIs for every profiled entity and artifact type.
- [ ] Distinguish mutable containers from immutable artifacts and snapshots.
- [ ] Distinguish a work from its edition, posting, file, and source fragment.
- [ ] Preserve aliases and predecessor identities for merges and splits.
- [ ] Add deterministic identity tests for every source adapter.
- [ ] Reject identifiers that match a lexical pattern but lack required
      source-of-record evidence.

**Done when:** The same real-world thing resolves consistently across runs and
sources, and distinct semantic roles never collide.

### RULE-011 — Implement a typed assertion projection

- [ ] Write an ADR for the physical projection. Keep specialized tables as the
      source of truth.
- [ ] Project durable metadata into explicit subject-predicate-object
      assertions with typed IRI, enum, literal, date, and numeric objects.
- [ ] Give every assertion a stable identifier and lifecycle status.
- [ ] Link assertions to evidence, confidence, lineage, applicability, and
      superseded assertions without embedding incompatible node types in one
      flat row.
- [ ] Support a deterministic JSON-LD export for conformance and interchange.
- [ ] Preserve the atomic-generation contract across all semantic projection
      artifacts.

**Done when:** Current rule targets, authority edges, proceeding relationships,
comment periods, and concept assignments round-trip through a Rulespec-shaped
projection without losing domain, range, direction, identity, or provenance.

### RULE-012 — Project complete evidence and provenance

- [ ] Define typed construction rules for Rulespec assertions, confidence
      records, findings, evidence bindings, and AI lineage.
- [ ] Capture source artifact and source fragment identifiers.
- [ ] Capture deterministic ruleset versions and input snapshot hashes.
- [ ] Capture model provider, model and version, prompt-template reference,
      input hash, parameters, response identifier, and token counts.
- [ ] Record validation and human-review decisions as separate attestations.
- [ ] Keep billing receipts operational metadata unless they support a semantic
      provenance claim.

**Done when:** A consumer can reconstruct how any derived assertion was made,
from which evidence, by which process, at what confidence, and what later
happened to it.

### RULE-013 — Add corpus-level conformance evidence

- [x] Extend the L0 self-certification process with a corpus-bound validation
      receipt.
- [x] Bind the receipt to the complete materialized-generation digest.
- [x] Validate every projected value, not only mapping examples.
- [x] Check identifier syntax, source-of-record membership, domain, range,
      direction, enum values, referential integrity, and transformation output.
- [x] Publish row counts and failure counts by table, term, and identifier
      scheme.
- [x] Block publication when a claimed mapping has invalid carrier rows.

**Done when:** A green conformance result proves that the published generation,
not merely its mapping document, satisfies the declared semantic contract.

### RULE-014 — Stabilize relationship assertions and neutral comparison

- [x] Record the independent adversarial review as durable evidence.
- [x] Document the generic assertion, evidence, attestation, comparison, and
      neutral-finding boundaries in `docs/ontology.md`.
- [x] Define the dependency-inverted resolver and proof-record contract.
- [x] Emit content-addressed resolver proof records and migrate the persisted
      OpenAI diagnostic without reinvoking the provider.
- [x] Separate polarity, attribution, lifecycle change, social state, deontic
      force, and domain interpretation.
- [x] Define longitudinal omission as a neutral post-extraction finding gated
      by lineage, expected coverage, scope, and bounded closure.
- [x] Define the paired Rulespec release, Spicy Regs migration, backfill, and
      rollback path.
- [x] Add a concise operator runbook for the two blind v2 reviews.
- [x] Review recent document, relation, absence, and lookup research and record
      its architectural consequences.
- [x] Add a tool-free Codex CLI structured-output adapter behind the existing
      provider protocol, with strict event rejection and provider-specific
      receipts.
- [x] Compare repeated baseline, evidence-first, lean semi-formal, and
      paper-derived proof-certificate Codex Sol prompts; retain one active
      proof-certificate prompt with the lean strict schema for fair-v2
      evaluation without optimizing further against the diagnostic-v1 oracle.
- [ ] Obtain a non-originating review and release the Rulespec
      `RelationshipAssertion` contract.
- [ ] Pin that release in Spicy Regs and publish explicit carrier mappings for
      assertions, evidence, attestations, comparison contexts, and findings.
- [ ] Replace static test adapters with production resolvers that emit
      dereferenceable proof records.
- [ ] Complete and seal two independent v2 human reviews, resolve
      disagreements, and freeze the oracle.
- [ ] Run three identical blinded OpenAI repetitions and write the final v2
      evidence report.
- [ ] Implement and independently validate closure before enabling
      `expected_relation_not_observed` for any profile.
- [ ] Add a `NormEvaluator` only with the first domain profile and real corpus
      examples that require it.

**Done when:** The released generic contract and migrated carrier can compare
explicit affirmed and denied assertions with complete proofs; the fair v2 run
meets its gates; omission stays unknown outside independently validated
closure; and domain judgments remain profile-owned.

### RULE-015 — Compose document version and revision identity

- [x] Add `dcterms:isVersionOf` and `prov:wasRevisionOf` to the local Rulespec
      `Artifact` candidate instead of minting competing document classes.
- [ ] Release the composed Artifact lineage contract and pin its digest in
      Spicy Regs.
- [ ] Keep every Artifact immutable and bind its version or revision links to
      stable IRIs and content-addressed evidence.
- [ ] Map existing Spicy Regs source identifiers without inventing lineage when
      a source does not expose it.
- [ ] Use ELI natively in the regulatory profile and allow other profiles to
      compose BIBFRAME, Schema.org, or their own public domain vocabulary.
- [ ] Add point-in-time, translation, same-content/different-format, and
      ambiguous-lineage tests.

**Done when:** Every comparison and retrieval result can name the exact
document state it used, while heterogeneous documents share one small identity
contract and domain lifecycle semantics remain profile-owned.

## Phase 2 — Cover the whole corpus

Use the profile from `RULE-002` for each corpus family. A family is complete
only when it has stable identity, typed relationships, provenance, projection
tests, row-level conformance, and at least one cross-family query.

### RULE-020 — Complete the regulatory and legislative chain

- [ ] Finish Rulespec projections for regulations.gov, Federal Register,
      Unified Agenda, proceedings, comment periods, CFR units, bills, public
      laws, and executive orders.
- [ ] Resolve compact CFR and statutory citations to immutable editions.
- [ ] Preserve the chain from proceeding to rule target to statutory authority.
- [ ] Add point-in-time tests across editions and amendments.

### RULE-021 — Model participation without confusing containers and content

- [ ] Profile regulations.gov comments, FCC filings, attachments, submitters,
      and organizations.
- [ ] Keep a docket or proceeding distinct from a filing, comment, attachment,
      and commenter.
- [ ] Represent text fragments used as evidence without treating extracted text
      as the source artifact.
- [ ] Add privacy and access-scope rules before deriving person-level metadata.

### RULE-022 — Connect litigation, oversight, and analysis

- [x] Profile court dockets, official Supreme Court opinion packages, GAO
      reports, and CRS reports.
- [ ] Profile court orders and broader authored opinions when source-backed
      structure supports their identity and grain.
- [ ] Represent "mentions," "reviews," "challenges," "affects," and "invalidates"
      as distinct evidence-bearing relationships.
- [ ] Require exact rule or artifact identity before claiming legal effect.
- [ ] Keep uncertain correlations as reviewable assertions, not joins promoted
      to fact.

### RULE-023 — Connect organizations, influence, and spending

- [ ] Profile SAM entities, USASpending recipients, lobbying filings, FEC
      committees, agencies, and commenter organizations.
- [ ] Separate an organization from its account, registration, filing, award,
      and participation event.
- [ ] Record exact identifier joins separately from probabilistic entity
      resolution.
- [ ] Attach confidence and evidence to every inferred organization link.

**Done when:** Rulespec semantics connect every current corpus family without
erasing its source-specific grain.

## Phase 3 — Deliver cross-corpus retrieval and discovery

### RULE-025 — Define the shared filter and query contract

- [ ] Keep registered filter terms and their semantics in Rulespec; keep query
      syntax, pagination, ranking, and serving behavior in Spicy Regs.
- [ ] Define composable filters for artifact type, source, agency, date,
      effective period, CFR unit, statute, public law, RIN, docket, proceeding,
      concept, facet, organization, legal status, relationship class, and
      confidence.
- [ ] Preserve each result's concrete type, source identity, and source-specific
      fields.
- [ ] Define stable pagination, ordering, and tie-breaking.
- [ ] Support filtering before or after relationship traversal.
- [ ] Define how unknown, conflicting, and time-bounded values affect filters.
- [ ] Keep the query contract independent of the physical Parquet layout.

**Done when:** One query can filter heterogeneous artifacts without treating
them as interchangeable records.

### RULE-026 — Index direct and deterministic relationships

- [ ] Index source citations, explicit links, containment, publication,
      authority, proceeding, and lifecycle relationships.
- [ ] Derive deterministic relationships from normalized identifiers and typed
      graph paths.
- [ ] Store the relationship type, direction, path, evidence, and generation
      digest.
- [ ] Distinguish a one-edge source citation from a multi-edge derived path.
- [ ] Publish the relationship index atomically with the semantic generation.
- [ ] Prevent inferred or similarity-based links from appearing as direct
      evidence.

**Done when:** A user can traverse every explicit and deterministic connection
without writing source-specific joins.

### RULE-027 — Find related artifacts without direct citations

- [ ] Generate candidates from shared concepts, entities, authorities,
      proceedings, affected populations, obligations, outcomes, and source
      fragments.
- [ ] Add hybrid lexical and embedding retrieval only after identity, version,
      authority, scope, access, graph, and concept filters.
- [ ] Rerank a bounded semantic candidate set through the existing
      dependency-inverted reranker.
- [ ] Allow models to propose typed relationships when deterministic signals
      cannot express the connection.
- [ ] Label semantic and inferred results explicitly.
- [ ] Attach the signals, evidence, confidence, model lineage, and generation
      digest used to rank each result.
- [ ] Balance results across source and artifact types so a large corpus cannot
      crowd out smaller but relevant sources.
- [ ] Abstain when evidence does not support a useful relationship.

**Done when:** An artifact with no direct citation can rank as related for an
inspectable reason, and weak similarity cannot masquerade as a factual link.

### RULE-028 — Expose and evaluate discovery

- [ ] Before implementation, create `docs/retrieval-evaluation.md` with the
      evaluation-set path, corpus commit and generation digest, metric formulas,
      minimum thresholds, latency budget, and maximum result size.
- [ ] Expose shared filters and related-artifact retrieval through MCP and SQL;
      add an API only when a client requires one.
- [ ] Return each result's concrete type, relationship class, explanation,
      evidence, confidence, and provenance.
- [ ] Record per-leg candidates, exclusions, fusion, reranking, graph paths,
      source spans, hashes, and closure claims in a lookup receipt.
- [ ] Let users restrict results to direct, deterministic, semantic, inferred,
      or any combination of relationship classes.
- [ ] Build a reviewed evaluation set containing direct links, uncited related
      artifacts, ambiguous pairs, and unrelated controls.
- [ ] Measure filter correctness, relationship precision, coverage, ranking
      quality, source diversity, and explanation completeness.
- [ ] Set latency and result-size budgets for interactive retrieval.
- [ ] Version evaluation results with each published semantic generation.

**Acceptance queries:**

- [ ] Find every artifact that touches a specified CFR unit across dockets,
      Federal Register documents, proceedings, comments, court records, and
      oversight reports.
- [ ] Find every artifact connected to a specified statute, including material
      that discusses the same authority without citing the same identifier.
- [ ] Given one document, return its direct citations, deterministic graph
      neighbors, semantically related artifacts, and inferred relationships in
      separate groups.
- [ ] Filter a related-artifact result set by agency, date, source type,
      concept, proceeding stage, and legal status.
- [ ] Explain why each result appeared and identify which evidence would
      disappear if the relationship were removed.
- [ ] Keep reviewed unrelated controls out of the result set at the declared
      threshold.

**Done when:** Users can filter the whole corpus and discover useful uncited
relationships without losing the distinction between evidence and inference.

## Phase 4 — Generalize the self-improving metadata loop

### RULE-030 — Tag every eligible semantic subject

- [ ] Replace the docket/document-only assumption with profile-driven subject
      adapters.
- [ ] Give each subject adapter a stable IRI, type, content digest, evidence
      slices, and allowed facets.
- [ ] Tag both immutable Artifacts and meaningful SourceFragments; keep
      temporary model-input segments operational unless they resolve to a
      stable source region.
- [ ] Aggregate accepted fragment assignments into document assignments through
      a versioned policy and supporting-assignment proof.
- [ ] Use document assignments only to shortlist candidates for fragment
      processing; require fresh local evidence before accepting a fragment tag.
- [ ] Process changed subjects incrementally and preserve prior assignments.
- [ ] Balance bounded model batches across source and subject types.

**Done when:** Adding a profiled corpus type requires a subject adapter and
tests, not a new tagging architecture, and document/fragment feedback cannot
turn inherited context into circular evidence.

### RULE-031 — Evolve retrieval concepts from evidence

- [ ] Keep corpus-discovered concepts local and retrieval-grade by default.
- [ ] Propose new concepts only after attempting resolution against existing
      concepts and aliases.
- [ ] Support evidence-backed merge, split, rename, deprecate, and replacement
      proposals.
- [ ] Preserve all rejected proposals and prior concept states.
- [ ] Prevent cycles in broader, narrower, and replacement relationships.
- [ ] Keep facets explicit; never merge a topic, regulated entity, legal
      authority, industry, place, and outcome merely because their labels are
      similar.

### RULE-032 — Discover cross-document relationships

- [ ] Extend model output beyond labels to typed relationship proposals allowed
      by the application profile.
- [ ] Ground each proposal in source fragments or deterministic joins.
- [ ] Separate discovery from acceptance and publication.
- [ ] Route accepted proposals through the `RULE-014` assertion, attestation,
      resolver, and neutral-finding contract.
- [ ] Validate inverse, symmetric, temporal, and cardinality rules before
      accepting an edge.
- [ ] Queue ambiguous identity and high-impact legal relationships for review.
- [ ] Keep missing relations unknown unless a validated profile supplies
      lineage, expected-coverage, scope, and closure proofs.

### RULE-033 — Measure quality and drift

- [ ] Maintain holdout sets by corpus type, subject type, and facet.
- [ ] Measure precision, recall, calibration, abstention, novelty, and merge
      quality.
- [ ] Compare new model, prompt, embedding, and ruleset versions with the
      currently published generation.
- [ ] Block regressions that exceed declared thresholds.
- [ ] Detect distribution drift and schedule targeted revalidation.
- [ ] Publish compact evaluation receipts with each semantic generation.

### RULE-034 — Close the feedback loop safely

- [ ] Turn validation disagreements into superseding assertions.
- [ ] Use explicit reviewer decisions and corpus evidence as learning signals.
- [ ] Keep query popularity and click behavior advisory; never treat popularity
      as truth.
- [ ] Re-score old assignments when concepts, evidence, or models change.
- [ ] Make every automated change reversible through the event log.

**Done when:** The metadata layer improves continuously without hiding
uncertainty, rewriting history, or autonomously creating decision-grade truth.

## Phase 5 — Promote proven concepts

### RULE-040 — Project retrieval concepts as Rulespec local concepts

- [ ] Define the exact projection from Spicy Regs concepts and assignments to
      Rulespec local concepts and assertions.
- [ ] Preserve local scope, evidence, confidence, lifecycle, and replacement
      history.
- [ ] Use explicit SKOS mappings for external thesauri and registered concepts.
- [ ] Keep unresolved or ambiguous mappings visible.

### RULE-041 — Implement the promotion packet

- [ ] Define eligibility thresholds for a promotion proposal.
- [ ] Generate a review packet containing definition, scope, examples,
      counterexamples, mappings, usage evidence, conflict evidence, AI lineage,
      and proposed steward.
- [ ] Require an identified human approver and rationale.
- [ ] Mint a registered concept only through a declared minting authority.
- [ ] Link the local and registered concepts through an attested mapping.
- [ ] Preserve rejected, withdrawn, and superseded promotion packets.

### RULE-042 — Integrate a real concept registry

- [ ] Implement or adopt the required Rulespec registry client and federation
      surface.
- [ ] Resolve concepts with cache, freshness, trust, and disagreement status.
- [ ] Test registry failure and stale-cache behavior.
- [ ] Keep Spicy Regs useful when the registry is unavailable.

**Done when:** A concept can emerge from corpus analysis, earn human approval,
become portable, and retain its complete discovery history.

## Phase 6 — Reach decision-grade legal semantics

### RULE-050 — Resolve immutable legal artifacts

- [ ] Resolve CFR, U.S.C., public-law, executive-order, and Federal Register
      citations to exact editions or snapshots.
- [ ] Record resolver source, timestamp, method, and confidence.
- [ ] Preserve edition-independent citations as separate identifiers.
- [ ] Refuse decision-grade authority claims when edition resolution fails.

### RULE-051 — Bind legal-effect events

- [x] Define the source-document profile for official Supreme Court opinion
      packages without inferring authored-opinion identity or legal effect.
- [ ] Define legal-effect profiles for court orders, agency actions, and
      effective legal status.
- [ ] Link legal-effect events to exact rule artifacts and assertions.
- [ ] Distinguish vacatur, injunction, remand, stay, amendment, rescission, and
      supersession.
- [ ] Apply Rulespec lifecycle and cascade behavior with point-in-time
      exceptions.
- [ ] Require human review before publishing an inferred legal effect.

### RULE-052 — Produce traceable decision packets

- [ ] Generate a machine-readable chain from a decision or eligibility rule to
      its exact regulatory text, authority, proceeding, docket, comment period,
      evidence, and current legal status.
- [ ] Validate the chain at the Rulespec level required by the consuming system.
- [ ] Test historical decisions against the legal state effective at their
      decision time.

**Done when:** A downstream system can explain which rule applied, why it had
authority, how it was produced, whether it remained effective, and which
evidence supported the decision.

## Definition of done for a new corpus type

A new source is not semantically integrated until it has:

- [ ] a documented grain and source authority;
- [ ] stable identity and version semantics;
- [ ] distinct container, artifact, actor, and content roles;
- [ ] typed relationships to existing corpus entities;
- [ ] raw-value preservation and normalized identifiers;
- [ ] evidence and provenance for every derived field or edge;
- [ ] an entry in `docs/rulespec-profile.md`;
- [ ] deterministic projection tests;
- [ ] full-corpus row validation;
- [ ] at least one useful cross-corpus query;
- [ ] participation in shared filters and related-artifact discovery;
- [ ] tag-loop support where the source has usable text or metadata;
- [ ] an atomic publication path that cannot mix semantic generations.

## Program completion criteria

This backlog is complete when:

1. Users can filter the complete corpus and discover both directly linked and
   uncited-but-related artifacts with inspectable explanations.
2. Rulespec defines the semantic structure of every supported Spicy Regs corpus
   family.
3. Spicy Regs publishes a reproducible, typed projection of its metadata and
   relationships.
4. Every derived assertion carries evidence, confidence, and lineage.
5. The learning loop improves retrieval metadata across corpus types while
   preserving history and uncertainty.
6. Human-reviewed promotion turns proven local concepts into portable
   registered concepts.
7. Corpus evidence can drive released Rulespec refactors and verified Spicy
   Regs migrations without permanent local forks.
8. Deferred shared findings flow into the canonical Rulespec backlog with
   reproducible evidence and executable acceptance criteria.
9. Decision-grade consumers can trace current and historical legal authority
   through the same semantic graph.
