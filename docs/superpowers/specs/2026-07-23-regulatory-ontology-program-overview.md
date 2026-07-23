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
| Rulemaking-process entities (Proceeding, CommentPeriod) | Rulespec (spec 2, Deliverable C — experimental) | Vocabulary; stabilizes only after spicy-regs proves it |
| Rule-identity spine tables (`rule_targets`, `authority_edges`) | Spicy-regs (spec 1) | Data product; deterministic ETL over the corpus |
| Descriptive tagging (SKOS facets, merge/validation loop) | Spicy-regs (spec 1) | Retrieval-grade, fast-churn — deliberately outside rulespec's decision-grade concept registry |
| Provenance columns on derived rows | Spicy-regs (spec 1), terms from rulespec | Carrier is parquet; semantics are rulespec's attestation/confidence terms |
| Reference corpus + partner self-certification | Both (spec 2, Deliverable D) | The falsifiability loop between the repos |
| Proceeding + comment-period tables (`proceedings`, `comment_periods`) | Spicy-regs (spec 1 §7, follow-on) | Corpus-scale consumer exercise that gates the rulemaking module's stabilization; promotes the existing `rulemaking_lifecycles` rollup; delivers the comment-window goal |

## Interface contract

What spec 1 consumes from spec 2 (blocking): the identifier schemes and the L0 tier — nothing else. What spec 2 receives from spec 1 (feedback): compact↔canonical identifier expansion friction, shape feedback on the rulemaking module before it freezes, one curated real proceeding as `reference-corpora/us-rulemaking/`, `conformance/partners/spicy-regs.yaml`, and the full-corpus `proceedings`/`comment_periods` exercise that gates the rulemaking module's stabilization.

The mapping between the two lives in **`docs/ontology.md`** (spicy-regs, created by spec 1): every column that carries rulespec semantics, its term IRI, and the enum-value correspondences. That document is simultaneously spec 1's design artifact and spec 2's L0 self-certification input — one file, both obligations. Its machine-readable format is defined by spec 2 (Deliverable B, the fenced `rkaf-l0-mapping` blocks) so the document and the audit tool cannot drift.

## Combined sequencing

1. Rulespec lands its in-flight `v0.2.0-pre.7` consolidation (pre-existing TODO; not this program).
2. **Rulespec release N+1:** identifier schemes + L0 tier (spec 2, A+B). *Unblocks everything below.* Cadence fallback: if N+1 slips, spicy-regs proceeds on provisional `x-` local terms with a committed rename after release — tables before vocabulary applies to the schedule too.
3. **Spicy-regs:** `rule_targets` + `docs/ontology.md`, then `authority_edges` including `pl_number` (spec 1, deterministic layer). File L0 self-certification.
4. **Spicy-regs:** FR `topics_json` enrichment, concept seeding, tagging loop (spec 1, derived layer).
5. **Rulespec release N+2:** rulemaking module (experimental) + reference corpus, shaped by step 3–4 friction (spec 2, C+D).
6. **Spicy-regs:** `proceedings` + `comment_periods` follow-on (spec 1 §7) — the corpus-scale consumer run for the rulemaking module.
7. Rulemaking module stabilizes after step 6 and one non-spicy-regs review (candidate: the Axiom Foundation corpus pipeline — also the natural third consumer of the identifier schemes).

## Long-term goals (beyond these specs)

Horizon items the two specs deliberately do not schedule. They are the program's direction; each becomes its own spec when its prerequisites exist.

1. **Rulespec stable core (1.0 trajectory).** Once the rulemaking module has survived the full-corpus run and external review, graduate it from experimental to normative, and freeze a small stable vocabulary core — identity schemes (including the US regulatory schemes), attestation, lifecycle, concepts — with the stability guarantee that makes third-party adoption safe. The 2026-07 audit found the modular *structure* largely present (doc-per-module, L-ladder isolating the runtime); what remains long-term is the *stability contract*, informed by what the spicy-regs tables proved.
2. **The promotion path.** Tooling and governance for the full ladder: descriptive tag (spicy-regs `concepts`) → `rkaf:LocalConcept` → `rkaf:RegisteredConcept`. Every promotion is a rare, human-approved, attested event — `rkaf:AILineage.humanApprover` exists for exactly this, and spicy-regs' `concept_events` `promote` event type is the hook. The payoff: concepts that *emerged* from corpus analysis (e.g. a contested definition of "small entity") become decision-grade registry entries with their discovery provenance intact.
3. **The eligibility-runtime story (full circle).** Rulespec's original motivating audience — benefits-eligibility systems and other L4/D4+ decision-grade consumers — eventually consume rules whose authority chains trace *through* the rulemaking module *to* the actual proceedings in spicy-regs' corpus. An eligibility decision then cites not just the rule but the docket, comment period, and statutory basis that produced it, and a rule struck down in litigation propagates through the same chain. Neither repo delivers this alone; it is the reason the program exists.

## Principles (shared, both repos)

- **Tables before vocabulary:** no term freezes until a real consumer has exercised it.
- **Compose, don't reinvent:** SKOS for concepts, PROV-O-aligned attestation, existing rulespec primitives before new ones (rulespec core §9.4 discipline, honored on both sides).
- **Decision-grade vs retrieval-grade stay separated:** promotion from tag to registered concept is a rare, attested, human-reviewed event — never a bulk migration.
- **Provenance from day one:** deterministic rows carry attestation columns too, so uniform provenance is a property of the layer, not an aspiration.
