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

## Interface contract

What spec 1 consumes from spec 2 (blocking): the identifier schemes and the L0 tier — nothing else. What spec 2 receives from spec 1 (feedback): compact↔canonical identifier expansion friction, shape feedback on the rulemaking module before it freezes, one curated real proceeding as `reference-corpora/us-rulemaking/`, and `conformance/partners/spicy-regs.yaml`.

The mapping between the two lives in **`docs/ontology.md`** (spicy-regs, created by spec 1): every column that carries rulespec semantics, its term IRI, and the enum-value correspondences. That document is simultaneously spec 1's design artifact and spec 2's L0 self-certification input — one file, both obligations.

## Combined sequencing

1. Rulespec lands its in-flight `v0.2.0-pre.7` consolidation (pre-existing TODO; not this program).
2. **Rulespec release N+1:** identifier schemes + L0 tier (spec 2, A+B). *Unblocks everything below.*
3. **Spicy-regs:** `rule_targets` + `docs/ontology.md`, then `authority_edges` (spec 1, deterministic layer). File L0 self-certification.
4. **Spicy-regs:** FR `topics_json` enrichment, concept seeding, tagging loop (spec 1, derived layer).
5. **Rulespec release N+2:** rulemaking module (experimental) + reference corpus, shaped by step 3–4 friction (spec 2, C+D).
6. Rulemaking module stabilizes after the full-corpus run and one external review.

## Principles (shared, both repos)

- **Tables before vocabulary:** no term freezes until a real consumer has exercised it.
- **Compose, don't reinvent:** SKOS for concepts, PROV-O-aligned attestation, existing rulespec primitives before new ones (rulespec core §9.4 discipline, honored on both sides).
- **Decision-grade vs retrieval-grade stay separated:** promotion from tag to registered concept is a rare, attested, human-reviewed event — never a bulk migration.
- **Provenance from day one:** deterministic rows carry attestation columns too, so uniform provenance is a property of the layer, not an aspiration.
