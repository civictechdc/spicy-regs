# Regulatory Ontology Program — Parent Reference

- **Date:** 2026-07-23
- **Status:** Reference (parent of two specs; not independently implementable)
- **Children:**
  - **Spec 1:** `docs/superpowers/specs/2026-07-23-metadata-ontology-layer-design.md` (this repo, `civictechdc/spicy-regs`)
  - **Spec 2 history:** `thoughts/specs/2026-07-23-us-regulatory-identifiers-and-rulemaking-module.md` (`Formspec-Labs/rulespec`)
  - **Current RIN authority:** `thoughts/specs/2026-07-24-rin-agenda-item-ontology-decision.md` (`Formspec-Labs/rulespec`)

## Vision

Give the US federal regulatory corpus machine-readable **identity** (which rule,
which statute, which proceeding), **correlation** (dockets grouped and filtered
through the rules and concepts they touch), and **provenance** (every derived
assertion says how it was made).

Rulespec is the canonical ontological and semantic model behind Spicy Regs.
Spicy Regs is a Rulespec application profile implemented as Parquet data,
pipelines, conformance receipts, and query surfaces. Parquet remains the
authoritative analytical carrier; it need not reproduce Rulespec's JSON-LD
shape. The profile must nevertheless give every durable entity, relationship,
assertion, category, event, and provenance field a Rulespec projection, a
recognized external-standard composition, or an explicitly justified local
extension.

One sentence per repo:

- **Rulespec** (`Formspec-Labs/rulespec`) owns reusable semantic contracts:
  identifier schemes, entity and relationship meaning, constraints,
  conformance tiers, projections, and promotion into shared or decision-grade
  vocabulary.
- **Spicy Regs** (`civictechdc/spicy-regs`) owns source-specific grains,
  physical storage, ingestion, model execution, retrieval-grade semantics,
  ranking, query behavior, and the corpus evidence that tests the shared
  contract.

**Governance note.** The repos are governed differently: spicy-regs is a Civic Tech DC community project; rulespec is currently maintained by a single maintainer who also contributes to spicy-regs. The blocking dependency below is deliberately minimal partly for this reason, and the overlap is disclosed here so spicy-regs contributors can weigh the dependency with full information.

The current US regulatory ontology is the first vertical slice of this
relationship, not the final metadata boundary. The same profile discipline
must eventually cover participation, legislation, litigation, oversight,
organizations, influence, and spending without flattening their distinct
containers, artifacts, actors, events, and evidence records.

## Division of responsibility

| Concern | Home | Why |
| --- | --- | --- |
| Identifier schemes (CFR, U.S.C., RIN, FR doc, regs.gov, PL) | Rulespec (spec 2, Deliverable A) | Closed enum, release-versioned, reusable beyond spicy-regs |
| L0 vocabulary-only conformance tier | Rulespec (spec 2, Deliverable B) | Conformance ladder is rulespec's contract |
| Rulemaking-process entities (RegulatoryAgendaItem, Proceeding, CommentPeriod) | Rulespec (spec 2, Deliverable C — experimental) | Vocabulary; stabilizes only after the corpus exercise and independent consumer review |
| Rule-identity spine tables (`rule_targets`, `authority_edges`) | Spicy-regs (spec 1) | Data product; deterministic ETL over the corpus |
| Descriptive tagging (SKOS facets, merge/validation loop) | Spicy-regs (spec 1) | Retrieval-grade, fast-churn — deliberately outside rulespec's decision-grade concept registry |
| Provenance columns on derived rows | Spicy-regs (spec 1) | Carrier is Parquet; only domain-safe, direction-safe Rulespec relationships enter the L0 map |
| Reference corpus + partner self-certification | Both (spec 2, Deliverable D) | The falsifiability loop between the repos |
| Agenda-item, Proceeding, and comment-period tables (`regulatory_agenda_items`, `agenda_item_proceedings`, `proceedings`, `comment_periods`) | Spicy-regs (spec 1 §7) | Corpus-scale consumer exercise built from source evidence; the separate `rulemaking_lifecycles` duration rollup is not a proceeding |

Fast-changing, corpus-discovered labels, ranking features, model prompts,
candidate links, and operational checkpoints remain local to Spicy Regs while
they are retrieval-grade. Reusable identifiers, semantic roles, relationship
direction, graph invariants, and decision-grade vocabulary belong in
Rulespec. Promotion from local to shared meaning is rare, evidence-backed,
human-reviewed, versioned in Rulespec, and followed by an explicit Spicy Regs
migration; shared maintainership is never a substitute for independent review.

## Interface contract

Spec 1 consumes the identifier schemes, L0 tier, and experimental rulemaking
terms from the sibling Rulespec contract. The current local candidate is pinned
by content digest:
`sha256:2aefd3fad7782a7b16a7fa8fc08e8ceb26b5db741e0371b8fa8a9ccc1982124d`.
The earlier digest
`sha256:836968b28f3b86283f53c57ae5c9ab8ebd77e96531cd4751476f1a5ee3d296f2`
remains bound to the first full-corpus feedback run. Neither digest is a
released rulemaking-contract claim.
It receives no Rulespec runtime dependency. Rulespec receives compact-to-
canonical identifier friction, rulemaking shape feedback, a curated reference
proceeding, a partner self-certification, and the full-corpus
`proceedings`/`comment_periods` exercise.

The application profile lives in **`docs/rulespec-profile.md`**. It inventories
the semantic role, identity, version semantics, evidence, projection status,
and gaps of every published table. The executable carrier mapping lives in
**`docs/ontology.md`** and **`conformance/rulespec-l0.yaml`**: every column that
claims Rulespec semantics, its term IRI, transform, and enum correspondence.
The machine-readable format is defined by Rulespec so prose, the partner
declaration, and the audit tool cannot silently drift.

## Current sequencing and gates

1. The sibling Rulespec branch has frozen its identifier, L0, and experimental
   rulemaking contract at the digest above. That state is local and unreleased.
2. Spicy-regs has implemented the deterministic identity layer, proceeding and
   comment-period tables, Federal Register topic enrichment, concept registry,
   assignment loop, and L0 self-certification.
3. The nine related tables build from one local input snapshot and publish as
   one manifest-addressed generation. No individual table workflow can expose a
   mixed generation.
4. The full-corpus run has been recorded in
   `docs/ontology-friction-report.md`; it changed both the implementation and
   the frozen Rulespec contract.
5. A maintainer-operated adversarial simulated-consumer review dated 2026-07-24
   resolved the three open agenda questions but concluded **do not graduate
   as-is** and defined a repair batch. Because no non-originating consumer
   operated or ratified it, it does not satisfy the independent-review gate.
6. Publication waits for repository review and a reachable versioned Rulespec
   contract. Stabilization requires the adversarial-review preconditions plus a
   non-originating consumer review or ratification of the repaired contract.

## Long-term goals (beyond these specs)

Horizon items the two specs deliberately do not schedule. They are the program's direction; each becomes its own spec when its prerequisites exist.

1. **Rulespec stable core (1.0 trajectory).** Once the rulemaking module has survived the full-corpus run, the adversarial-review repair batch, and external review, graduate it from experimental to normative, and freeze a small stable vocabulary core — identity schemes (including the US regulatory schemes), attestation, lifecycle, concepts — with the stability guarantee that makes third-party adoption safe. The 2026-07 audit found the modular *structure* largely present (doc-per-module, L-ladder isolating the runtime); what remains long-term is the *stability contract*, informed by what the spicy-regs tables proved.
2. **The promotion path.** Tooling and governance for the full ladder: descriptive tag (spicy-regs `concepts`) → `rkaf:LocalConcept` → `rkaf:RegisteredConcept`. Every promotion is a rare, human-approved, attested event — `rkaf:AILineage.humanApprover` exists for exactly this, and spicy-regs' `concept_events` `promote` event type is the hook. The payoff: concepts that *emerged* from corpus analysis (e.g. a contested definition of "small entity") become decision-grade registry entries with their discovery provenance intact.
3. **The eligibility-runtime story (full circle).** Rulespec's original motivating audience — benefits-eligibility systems and other L4/D4+ decision-grade consumers — eventually consume rules whose authority chains trace *through* the rulemaking module *to* the actual proceedings in spicy-regs' corpus. An eligibility decision then cites not just the rule but the docket, comment period, and statutory basis that produced it, and a rule struck down in litigation propagates through the same chain. Neither repo delivers this alone; it is the reason the program exists.

## Principles (shared, both repos)

- **Tables before vocabulary:** no term freezes until a real consumer has exercised it.
- **Compose, don't reinvent:** SKOS for concepts, PROV-O-aligned attestation, existing rulespec primitives before new ones (rulespec core §9.4 discipline, honored on both sides).
- **Decision-grade vs retrieval-grade stay separated:** promotion from tag to registered concept is a rare, attested, human-reviewed event — never a bulk migration.
- **Provenance from day one:** deterministic rows carry the same local
  provenance block, while Rulespec mappings are claimed only when a typed graph
  construction preserves the term's domain, range, and direction.
