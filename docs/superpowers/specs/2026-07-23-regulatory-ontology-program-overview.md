# Regulatory Ontology Program — Parent Reference

- **Date:** 2026-07-23
- **Status:** Reference (parent of two specs; not independently implementable)
- **Children:**
  - **Spec 1:** `docs/superpowers/specs/2026-07-23-metadata-ontology-layer-design.md` (this repo, `civictechdc/spicy-regs`)
  - **Spec 2:** `thoughts/specs/2026-07-23-us-regulatory-identifiers-and-rulemaking-module.md` (`Formspec-Labs/rulespec`)

## Vision

Give the US federal regulatory corpus machine-readable **identity** (which rule, which statute, which proceeding), **correlation** (dockets grouped and filtered through the rules and concepts they touch), and **provenance** (every derived assertion says how it was made). Rulespec supplies the vocabulary substrate; spicy-regs supplies the data, the pipeline, and the proof that the vocabulary works.

One sentence per repo:

- **Rulespec** (`Formspec-Labs/rulespec`) is the vocabulary home — identifier schemes, conformance tiers, and the rulemaking-process module live there and are versioned there.
- **Spicy-regs** (`civictechdc/spicy-regs`) is the first Level-0 consumer — parquet tables that *use* the vocabulary without adopting the JSON-LD carrier or runtime.

**Governance note.** The repos are governed differently: spicy-regs is a Civic Tech DC community project; rulespec is currently maintained by a single maintainer who also contributes to spicy-regs. The blocking dependency below is deliberately minimal partly for this reason, and the overlap is disclosed here so spicy-regs contributors can weigh the dependency with full information.

## Division of responsibility

| Concern | Home | Why |
| --- | --- | --- |
| Identifier schemes (CFR, U.S.C., RIN, FR doc, regs.gov, PL) | Rulespec (spec 2, Deliverable A) | Closed enum, release-versioned, reusable beyond spicy-regs |
| L0 vocabulary-only conformance tier | Rulespec (spec 2, Deliverable B) | Conformance ladder is rulespec's contract |
| Rulemaking-process entities (Proceeding, CommentPeriod) | Rulespec (spec 2, Deliverable C — experimental) | Vocabulary; stabilizes only after the corpus exercise and independent consumer review |
| Rule-identity spine tables (`rule_targets`, `authority_edges`) | Spicy-regs (spec 1) | Data product; deterministic ETL over the corpus |
| Descriptive tagging (SKOS facets, merge/validation loop) | Spicy-regs (spec 1) | Retrieval-grade, fast-churn — deliberately outside rulespec's decision-grade concept registry |
| Provenance columns on derived rows | Spicy-regs (spec 1) | Carrier is Parquet; only domain-safe, direction-safe Rulespec relationships enter the L0 map |
| Reference corpus + partner self-certification | Both (spec 2, Deliverable D) | The falsifiability loop between the repos |
| Proceeding + comment-period tables (`proceedings`, `comment_periods`) | Spicy-regs (spec 1 §7) | Corpus-scale consumer exercise built from source evidence; the separate `rulemaking_lifecycles` duration rollup is not a proceeding |

## Interface contract

Spec 1 consumes the identifier schemes, L0 tier, and experimental rulemaking
terms from the frozen sibling Rulespec contract. Spicy-regs pins that contract
by content digest:
`sha256:836968b28f3b86283f53c57ae5c9ab8ebd77e96531cd4751476f1a5ee3d296f2`.
It receives no Rulespec runtime dependency. Rulespec receives compact-to-
canonical identifier friction, rulemaking shape feedback, a curated reference
proceeding, a partner self-certification, and the full-corpus
`proceedings`/`comment_periods` exercise.

The mapping between the two lives in **`docs/ontology.md`** (spicy-regs, created by spec 1): every column that carries rulespec semantics, its term IRI, and the enum-value correspondences. That document is simultaneously spec 1's design artifact and spec 2's L0 self-certification input — one file, both obligations. Its machine-readable format is defined by spec 2 (Deliverable B, the fenced `rkaf-l0-mapping` blocks) so the document and the audit tool cannot drift.

## Current sequencing and gates

1. The sibling Rulespec branch has frozen its identifier, L0, and experimental
   rulemaking contract at the digest above. That state is local and unreleased.
2. Spicy-regs has implemented the deterministic identity layer, proceeding and
   comment-period tables, Federal Register topic enrichment, concept registry,
   assignment loop, and L0 self-certification.
3. The seven related tables build from one local input snapshot and publish as
   one manifest-addressed generation. No individual table workflow can expose a
   mixed generation.
4. The full-corpus run has been recorded in
   `docs/ontology-friction-report.md`; it changed both the implementation and
   the frozen Rulespec contract.
5. Publication waits for repository review and a reachable versioned Rulespec
   contract. Stabilization still requires a non-originating consumer review.

## Long-term goals (beyond these specs)

Horizon items the two specs deliberately do not schedule. They are the program's direction; each becomes its own spec when its prerequisites exist.

1. **Rulespec stable core (1.0 trajectory).** Once the rulemaking module has survived the full-corpus run and external review, graduate it from experimental to normative, and freeze a small stable vocabulary core — identity schemes (including the US regulatory schemes), attestation, lifecycle, concepts — with the stability guarantee that makes third-party adoption safe. The 2026-07 audit found the modular *structure* largely present (doc-per-module, L-ladder isolating the runtime); what remains long-term is the *stability contract*, informed by what the spicy-regs tables proved.
2. **The promotion path.** Tooling and governance for the full ladder: descriptive tag (spicy-regs `concepts`) → `rkaf:LocalConcept` → `rkaf:RegisteredConcept`. Every promotion is a rare, human-approved, attested event — `rkaf:AILineage.humanApprover` exists for exactly this, and spicy-regs' `concept_events` `promote` event type is the hook. The payoff: concepts that *emerged* from corpus analysis (e.g. a contested definition of "small entity") become decision-grade registry entries with their discovery provenance intact.
3. **The eligibility-runtime story (full circle).** Rulespec's original motivating audience — benefits-eligibility systems and other L4/D4+ decision-grade consumers — eventually consume rules whose authority chains trace *through* the rulemaking module *to* the actual proceedings in spicy-regs' corpus. An eligibility decision then cites not just the rule but the docket, comment period, and statutory basis that produced it, and a rule struck down in litigation propagates through the same chain. Neither repo delivers this alone; it is the reason the program exists.

## Principles (shared, both repos)

- **Tables before vocabulary:** no term freezes until a real consumer has exercised it.
- **Compose, don't reinvent:** SKOS for concepts, PROV-O-aligned attestation, existing rulespec primitives before new ones (rulespec core §9.4 discipline, honored on both sides).
- **Decision-grade vs retrieval-grade stay separated:** promotion from tag to registered concept is a rare, attested, human-reviewed event — never a bulk migration.
- **Provenance from day one:** deterministic rows carry the same local
  provenance block, while Rulespec mappings are claimed only when a typed graph
  construction preserves the term's domain, range, and direction.
