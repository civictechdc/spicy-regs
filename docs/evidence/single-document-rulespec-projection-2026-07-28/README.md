# Single-document Rulespec (RKAF) projection — FR 2026-03227

Consumer evidence for the rulespec contract: one real Federal Register document,
hand-authored end-to-end as a complete RKAF JSON-LD object, validated with
rulespec's own gates. The question under test: does the contract actually carry
a real document, and what breaks when a careful author tries?

## The document

| | |
|---|---|
| FR document | **2026-03227** — "Maximum Line Speed Rates for Young Chicken and Turkey Establishments Operating Under the New Poultry Inspection System" (FSIS proposed rule, 91 FR 7926, 2026-02-19) |
| Gold anchor | `gold_46af63a049ee1964b9ae13f4` (`output/segmented-real-data-evaluation-v2/gold_spans.parquet`) — hand-curated span "Poultry Inspection System" at `[2282,2307)` of `federal_register.body_html` |
| Docket / RIN | FSIS-2025-0012 / 0583-AE01 |
| Deterministic edges | `output/rulespec-stabilization-candidate-final/{rule_targets,authority_edges,proceedings,dockets}.parquet` (run `ontology-20260724T065716Z`) |
| Concepts | `output/fused-concept-registry-v1/registry.parquet` — `concept_9bb8165887d1cb3edc54277b` "Poultry and poultry products" and `concept_10c6db73325f36bcc6d8b84a` "Meat inspection", both `scheme=subject` (Federal Register Thesaurus origin, per `external_ids_json`) |

The drawn holdout was not touched; everything here is development-corpus data.

## Contract under test

rulespec branch `us-regulatory-identifiers` @ `f2a939d` (contains the
ConceptAssignment contract, Core §4.7.3, and the carrier-local fragment URN
from commit `bc88c02`). The rulespec repo was treated as read-only: the tree
was exported with `git archive` into a scratch directory; the gitignored
`compiled/` JSON-Schema + SHACL outputs were taken from the working checkout
and proven consistent with the branch by running its own 426-fixture corpus
all-green (`validation/corpus-baseline-summary.txt`).

## Files

| File | What |
|---|---|
| `fsis-2026-03227.rulespec.jsonld` | The RKAF object: 34 graph nodes — 1 Artifact, 1 Proceeding, 1 Docket, 1 RegulatoryAgendaItem, 3 SourceFragments (+3 by-reference TextPositionSelectors), 1 ConceptScheme, 2 LocalConcepts, 2 ConceptAssignments, 3 RelationshipAssertions, 3 EvidenceBindings, 3 SourceClaimants, 4 ExtractionActivities, 5 prov:Entity provenance records, 1 Attestation |
| `build_projection.py` | Reproducibility companion: re-derives every digest/offset/edge from the stored tables, hard-fails on any mismatch, re-emits the JSON-LD byte-identically |
| `offset-verification.txt` | Captured verification transcript (see below) |
| `rkaf-context.jsonld` | Copy of `context/rkaf-context.jsonld` from the branch @ `f2a939d`, so the document is self-contained for JSON-LD/SHACL processing |
| `validation/conformance-gates.txt` | L1/L2/L3 verdict, per-type L2 dispatch coverage, and four negative controls |
| `validation/ci-validate-shacl.txt` | `tools/ci_validate.py` (the SHACL path) run against the committed file: 54 shape files, 196 triples, 0 violations, PASS |
| `validation/corpus-baseline-summary.txt` | Validator-environment consistency proof |

## What validated

| Gate | Result |
|---|---|
| L1 (JSON-LD parse) | pass |
| L2 (compiled JSON Schema per typed node, incl. `x-rkaf-order`) | pass — all 29 rkaf/oa-typed nodes dispatched to a real compiled schema; zero silent unbound types |
| L3 (pyshacl, 54 hand-authored + compiled shape files) | pass — 0 violations |
| L4 (behavior) | not applicable — not a behavior fixture |

One authoring iteration was needed: the first build failed L3 with six
`sh:class prov:Entity` violations (see finding G1); everything else passed on
the first attempt.

Four negative controls (mutations of this document) confirm the gates bite on
exactly this content (`validation/conformance-gates.txt`): declaring
`rkaf:published-fragment` over carrier-local URNs → L3 fail; drifting one URN
offset → L3 fail; uppercasing a URN digest → L2 fail; inverting a selector's
`oa:start`/`oa:end` → L2 fail.

## Offset-verification proof

Text-state convention (load-bearing, documented): the Artifact's
`rkaf:hasContentDigest` and all fragment coordinates are taken over the stored
**`federal_register.body_html`** field of
`output/segmented-real-data-evaluation-v2/federal_register.parquet` — SHA-256
over its UTF-8 bytes, offsets in Unicode code points, half-open `[start,end)`,
matching Core §4.2's carrier-local URN grammar (unit fixed by the scheme).

```
sha256(body_html) = d67993458a2b330cd9b53af0f0162d21aa78ea30b61b190926c21ea6b91ec921
F1 [2282,2307)   -> "Poultry Inspection System"
                    sha256 835e00f8… == gold exact_text_sha256   (gold-exact)
F2 [1578,1629)   -> "9 CFR Part 381</li>\n<li>[Docket No. FSIS-2025-0012]"
                    sha256 23c0703e…
F3 [23602,23615) -> "21 U.S.C. 451"
                    sha256 8fd1bad8…
```

Each slice was recomputed from the stored text and compared for exact string
equality before emission; `build_projection.py` aborts on any mismatch, and
`offset-verification.txt` is the captured transcript. The run's
`artifact_digest` (`9b3eb760…`) is **not** a content digest: it is spicy-regs'
producer-scoped version digest over a canonical-json envelope of all seven
profile text fields (`subjects.py _make_artifact`). The builder reproduces it
exactly from the stored row, tying this projection to the stored v4/gold
identity without misusing it as `rkaf:hasContentDigest`.

## What mapped cleanly

1. **FR-document Artifact identity** — permanent federalregister.gov URL as
   `rkaf:hasArtifactIdentifier` (`rkaf:urn-persistent`) plus the
   `urn:rkaf:us:frdoc:2026-03227` / `rkaf:us-frdoc` regulatory pair
   (`spec/rkaf-rulemaking.md` §5.2). Fixture-identical, zero friction.
2. **Carrier-local fragment URNs** (Core §4.2, commit `bc88c02`) — mintable
   mechanically from `(artifact IRI, start, end, sha256(region))`, exactly the
   columns spicy-regs already stores in `gold_spans.parquet`. The
   materialization pattern from
   `fixtures/conceptassignment-carrier-local-fragment-positive.jsonld`
   transfers directly, and both the L2 grammar and the L3
   declaration/source-agreement shapes demonstrably fire (controls 1–3).
3. **ConceptAssignment** (Core §4.7.3 / `constraints/core/concept-assignment.cue`)
   — every required and conditional property expressible; the
   `assignmentEvidenceScheme` declaration requirement is coherent in practice.
4. **Provenance-role split** (Core §2.4) — the real data has exactly the two
   shapes the spec separates: the pipeline run (`actor_id`/`run_id` columns →
   `rkaf:ExtractionActivity`, `rkaf:deterministicParse`) and the document
   issuer's own statements (FR heading/authority lines →
   `rkaf:SourceClaimant`, `rkaf:claimantIsDocumentIssuer`). The deterministic
   extractions are labeled as extractions, not as claims of FSIS.
5. **Rulemaking-profile objects** — Proceeding, Docket, RegulatoryAgendaItem
   map 1:1 onto spicy-regs' `proceedings`/`dockets` tables; notably the
   proceedings table already stores `urn:rkaf:us:cfr:9:381` in the contract's
   own `rkaf:us-cfr` grammar (`cfr_target_iris_json`), so the CFR-target
   object IRI required no translation at all.
6. **Attestation as separate record** (`constraints/core/attestation.cue`) —
   attaching an `rkaf:aiModel` attestation targeting a ConceptAssignment with
   a closed-enum decision and this exercise's scope IRI was trivial, and the
   proposition stayed byte-identical, as Core §2.3 promises.

## Judgment calls (each with the cite)

- **J1 — `assertionOrigin` for deterministic extractions.** The closed enum
  (`constraints/core/assertion.cue` `#AssertionOrigin`) has no
  machine-deterministic value; `rkaf:imported` was chosen (records
  re-serialized from published spicy-regs tables), with the honest method
  carried by `rkaf:hasExtractionProvenance` →
  `rkaf:extractionMethod: rkaf:deterministicParse` (Core §2.4). Seam: nothing
  forces an imported record to carry extraction provenance, so the
  deterministic-parse fact is droppable without any gate noticing.
- **J2 — `rkaf:requestContractDigest` for non-model runs** (Core §2.4,
  REQUIRED in `constraints/core/extraction-activity.cue`). There is no request
  contract for a deterministic table parse. A documented canonical-json
  envelope `{instructions, actor_id, run_id, input_row}` was hashed (recipes
  and digests in `offset-verification.txt`); real and reproducible, but the
  digest names a contract this exercise defined, not one the original run
  published.
- **J3 — authority modeled at citation level.** `authority_edges` is
  RIN+agenda-edition-keyed with `parse_status=partial`, so RA2 asserts
  `rkaf:agendaAuthorityCitation` on the `urn:rkaf:us:rin:0583-AE01` agenda
  item (`spec/rkaf-rulemaking.md` §7) rather than minting the stronger
  `rkaf:hasAuthority` → `rkaf:Authority` chain, which §3 warns against minting
  without action-specific evidence. Evidence binds to the FR document's own
  restatement (`F3`, "21 U.S.C. 451"), while `prov:wasDerivedFrom` pins the
  agenda-derived table row — two genuinely different sources, kept distinct.
- **J4 — docket membership routes through the Proceeding.** RA3 asserts
  `rkaf:hasDocket` (proceeding → docket, `spec/rkaf-rulemaking.md` §3.2);
  the FR document's own `[Docket No. FSIS-2025-0012]` heading (`F2`) is the
  evidence, and the document reaches the docket via
  `rkaf:publishedInProceeding`. See G2 for the gap this works around.
- **J5 — `rkaf:assertedAt` semantics.** For imported assertions the
  deterministic run's `asserted_at` (2026-07-24T06:57:16Z) was used; for the
  assignments and attestation, the authoring date. The spec ("when the
  assertion was made", `assertion.cue`) is ambiguous between original
  extraction time and record-minting time; documented rather than resolved.
- **J6 — concepts as `rkaf:LocalConcept`.** The fused registry is
  workspace-governed (no `rkaf:ConceptRegistry` object exists in spicy-regs),
  so `#LocalConcept` + `rkaf:definedInScope` (`constraints/core/concept.cue`)
  is the honest typing even though the subject-scheme rows originate in the
  Federal Register Thesaurus. The FR-Thesaurus identity
  (`external_ids_json`) has no slot on the concept node itself; it would need
  a SKOS mapping record (`spec/rkaf-concept-registry.md`), out of scope here.
- **J7 — shared evidence span.** Both assignments cite `F1`; "Meat
  inspection" would ideally cite its own span (e.g. "online carcass
  inspection"), but the deliverable's two-or-three-fragment budget was kept.
  Core §4.7.3 rule 3 (cite regions *of the subject*) is an explicit producer
  obligation, not a mechanical check — nothing pushed back, by design.

## What the contract could not express, or made awkward

- **G1 — `prov:wasDerivedFrom` requires materialized `prov:Entity` nodes, and
  only SHACL says so.** `compiled/shacl/core/assertion.ttl:26` (likewise
  `concept-assignment.ttl:26`, `relationship-assertion.ttl:26`) carries
  `sh:class prov:Entity`, but Core §2.4 and `assertion.cue` present the field
  as an IRI list with no class note. This was the single authoring failure (6
  violations); the fix — five typed `prov:Entity` record nodes — is mechanical
  but undiscoverable from the spec prose. Suggest a §2.4 note.
- **G2 — no document→docket predicate.** FR metadata natively says "this
  document belongs to docket FSIS-2025-0012" (`docket_ids_json`), but the only
  docket edge is `rkaf:hasDocket` on Proceeding (`spec/rkaf-rulemaking.md`
  §3/§3.2). A producer with dockets but no proceedings model cannot express
  the FR-native fact; this projection could only because spicy-regs happens to
  have the proceeding.
- **G3 — `#AssertionOrigin` has no deterministic-machine value** (see J1;
  `constraints/core/assertion.cue`). "imported" underdescribes and
  "aiSuggested" would be false; the honest combination leans on an optional
  edge.
- **G4 — `rkaf:requestContractDigest` presumes request-shaped extraction**
  (see J2; Core §2.4). A method-conditional recipe (or a normative envelope
  definition for `rkaf:deterministicParse` / `rkaf:importedRecord`) would
  remove the invent-your-own-contract step.
- **G5 — direct profile edges vs reified assertions are unreconciled.** The
  graph states proceeding→docket twice: as the profile's plain edge
  (`rkaf:hasDocket` on the Proceeding node, `spec/rkaf-rulemaking.md` §3) and
  as RA3's reified proposition (Core §2.1). The contract never says when a
  producer should emit which, or how consumers deduplicate; a
  provenance-stripping projection would double-count.
- **G6 — `rkaf:attestedAt` is missing from the context.**
  `context/rkaf-context.jsonld` coerces `rkaf:assertedAt` to `xsd:dateTime`
  but declares no term for `rkaf:attestedAt` (or `rkaf:rationale`), so
  attestation timestamps land in RDF as plain strings while assertion
  timestamps are typed — inconsistent typing for the same temporal semantics.
  The compiled attestation SHACL checks only cardinality
  (`compiled/shacl/core/attestation.ttl:23`), so nothing catches it.

## Verdict on the phase-4 diagnostic L2/L3 projection gate

**Realistic — with one operational precondition.** Evidence:

- A careful author starting from spicy-regs' published tables reached
  L1+L2+L3 green in **one** iteration; the only failure (G1) was mechanical.
- The gates are not vacuous on real content: all 29 typed nodes dispatch to
  real compiled schemas (no silent unbound types), and four
  single-field mutations of this document each flip the expected gate.
- Cost is diagnostic-friendly: seconds per document (L2 is in-process JSON
  Schema; L3 is 196 triples against 54 shape files).
- The spicy-regs data model is already contract-adjacent: gold spans carry
  exactly the carrier-local URN bindings, and proceedings already store
  `rkaf:us-cfr` IRIs verbatim.

The precondition: the gates depend on `compiled/` JSON-Schema + SHACL outputs
that are **gitignored** in rulespec and require the `cue` toolchain to
rebuild. A phase-4 gate must pin a rulespec commit *and* either vendored
compiled outputs or a CI `cue` install + `tools/compile_all.sh` step —
otherwise the gate silently validates against whatever stale compile is lying
around (this exercise had to prove compiled-output freshness via the
426-fixture corpus run). L3 is load-bearing and cannot be skipped: two of the
four negative controls (evidence-scheme mismatch, offset drift) are invisible
to L2.

## Reproduce

```sh
# regenerate the JSON-LD + verification transcript (hard-fails on any mismatch)
python3 docs/evidence/single-document-rulespec-projection-2026-07-28/build_projection.py

# SHACL gate, from a rulespec us-regulatory-identifiers tree with compiled/ built
python3 tools/ci_validate.py \
  /path/to/spicy-regs/docs/evidence/single-document-rulespec-projection-2026-07-28/fsis-2026-03227.rulespec.jsonld
```
