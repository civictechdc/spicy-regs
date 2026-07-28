# Single-document Rulespec (RKAF) projection — FR 2026-03227

Consumer evidence for the rulespec contract: one real Federal Register document,
hand-authored end-to-end as a complete RKAF JSON-LD object, validated with
rulespec's own gates. The question under test: does the contract actually carry
a real document, and what breaks when a careful author tries?

**Status, 2026-07-28 (second pass).** All six findings this exercise raised
(G1–G6) were answered in the contract, and this projection has been rebuilt
against the answers. Three of them changed the document: two workarounds the
old contract forced are now normatively non-conforming, and one fact the old
contract could not express is now written down. The findings section below
records each resolution and the commit that landed it.

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

rulespec branch `us-regulatory-identifiers` @ **`062fa79`**, contract digest
**`sha256:7d45dcd2f5ff6391b185fd98099740b34d3b6cac8ed66c99196e6ac368806553`**
— the digest spicy-regs pins as of commit `8d08882`. This revision carries the
ConceptAssignment contract, Core §4.7.3, the carrier-local fragment URN
(`bc88c02`), and the six commits that answer this exercise's findings.

The rulespec repo was again treated as read-only: a detached `git worktree` was
created at `062fa79`, `compiled/` was rebuilt inside it with `make compile`,
and the worktree was removed afterwards. The user's checkout was never touched
and stayed on its own branch. Two things improved over the first pass:
`tools/compile_all.sh` drives `tools/constraints_compile.py`, which is pure
Python — the `cue` binary is not needed — so compiled-output freshness is now
**by construction** rather than inferred, and `tools/repin_contract_digest.py`
independently reported `[OK] all pins current at sha256:7d45dcd2…`, matching
the pinned digest. The 431-fixture corpus was still run all-green as
corroboration (`validation/corpus-baseline-summary.txt`).

## Files

| File | What |
|---|---|
| `fsis-2026-03227.rulespec.jsonld` | The RKAF object: 34 graph nodes — 1 Artifact, 1 Proceeding, 1 Docket, 1 RegulatoryAgendaItem, 3 SourceFragments (+3 by-reference TextPositionSelectors), 1 ConceptScheme, 2 LocalConcepts, 2 ConceptAssignments, 3 RelationshipAssertions, 3 EvidenceBindings, 3 SourceClaimants, 4 ExtractionActivities, 5 prov:Entity provenance records, 1 Attestation |
| `build_projection.py` | Reproducibility companion: re-derives every digest/offset/edge from the stored tables, hard-fails on any mismatch, re-emits the JSON-LD byte-identically |
| `offset-verification.txt` | Captured verification transcript (see below) |
| `rkaf-context.jsonld` | Copy of `context/rkaf-context.jsonld` from the branch @ `062fa79`, so the document is self-contained for JSON-LD/SHACL processing. Refreshed in this pass — not cosmetic: under the `f2a939d` copy `rkaf:publishedInDocket` had no term, so it expanded as a string literal and `sh:class rkaf:Docket` could never fire, and ten timestamp terms expanded untyped (G6) |
| `validation/conformance-gates.txt` | L1/L2/L3 verdict, per-type L2 dispatch coverage, and five negative controls |
| `validation/ci-validate-shacl.txt` | `tools/ci_validate.py` (the SHACL path) run against the committed file: 54 shape files, 193 triples, 0 violations, PASS |
| `validation/corpus-baseline-summary.txt` | Validator-environment consistency proof |

## What validated

| Gate | Result |
|---|---|
| L1 (JSON-LD parse) | pass |
| L2 (compiled JSON Schema per typed node, incl. `x-rkaf-order`) | pass — all 29 rkaf/oa-typed nodes dispatched to a real compiled schema; zero silent unbound types |
| L3 (pyshacl, 54 hand-authored + compiled shape files) | pass — 0 violations |
| L4 (behavior) | not applicable — not a behavior fixture |

On the first pass one authoring iteration was needed: the first build failed L3
with six `sh:class prov:Entity` violations (see finding G1); everything else
passed on the first attempt. The second pass — the three document changes below
against contract `062fa79` — needed **zero** iterations: regenerate, re-run,
green.

Five negative controls (mutations of this document) probe whether the gates
bite on exactly this content (`validation/conformance-gates.txt`). Four do:

| Control | Verdict | What fires |
|---|---|---|
| declare `rkaf:published-fragment` over carrier-local URNs | L3 fail | `rkaf:ConceptAssignmentCarrierLocalEvidenceDeclaredShape` |
| drift one cited URN offset (2307 → 2308) so the fragment is never materialized | L3 fail | compiled `sh:class rkaf:SourceFragment` on `rkaf:assignmentEvidence` |
| uppercase the URN digest | L2 fail | compiled carrier-local pattern in `concept-assignment.schema.json` (L3 also fails) |
| invert a selector's `oa:start`/`oa:end` | L2 fail | `x-rkaf-order` on `TextPositionSelector` (L3 `sh:lessThanOrEquals` also fails) |
| **mint a `rkaf:requestContractDigest` on a non-model activity** | **L1 pass, L2 pass, L3 pass** | **nothing — see below** |

The fifth control is new and it is the honest one. Core §2.4 says outright that
"a digest over an envelope minted to satisfy the field is non-conforming", and
no gate catches it. Every compiled surface checks only the lexical form
`^sha256:[0-9a-f]{64}$`, and the conditional guard fires on
`rkaf:modelExtraction` to require *presence*, never on the other four methods
to interrogate *provenance*. Checking the claim would need the preimage of a
digest the kernel treats as opaque by design, so no shape can express it. It is
a producer obligation in the same class as Core §4.7.3 rule 3 and the §2.1
projected-edge rules: real, normative, unmechanized. Note the shape of the
improvement — the old contract *required* the fabrication this control now
performs, so before `e8794ba` the non-conforming state was the only conforming
state; the field is now removable, which is what makes an honest record
possible even though a dishonest one still validates.

The second control's attribution is corrected from the first pass, which named
`CarrierLocalFragmentUrnSourceAgreementShape`. That shape compares only the
artifact component of a URN against `oa:hasSource`; what actually catches a
dangling citation is the compiled `sh:class rkaf:SourceFragment` on
`rkaf:assignmentEvidence`. The verdict is unchanged; the diagnosis was wrong.

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
   transfers directly, and both the L2 grammar and the L3 shapes demonstrably
   fire (controls 1–3 — see the corrected attribution above for which shape
   catches which).
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

Judgment calls are what the *first* pass had to decide for itself. J1, J2 and
J4 no longer are judgment calls — the contract now decides them, and the
document follows. They are kept, struck through, because a judgment call the
contract later absorbed is the most useful kind of consumer evidence there is.

- ~~**J1 — `assertionOrigin` for deterministic extractions.**~~ **Decided by
  the contract** (`3c16018`). The v0.1 closed enum had no machine-deterministic
  value, so `rkaf:imported` was chosen with the honest method demoted to the
  optional `rkaf:hasExtractionProvenance` edge — droppable with no gate
  noticing. `#AssertionOrigin` now carries `rkaf:deterministicExtraction`, and
  the value makes `rkaf:hasExtractionProvenance` REQUIRED on every compiled
  target. The three deterministic relationship assertions carry it; the two
  ConceptAssignments stay on `rkaf:imported`, which is now a narrower and
  accurate claim about them — their activity is `rkaf:importedRecord`, a
  re-serialization of the Federal Register API's own `topics_json`, not a
  derivation.
- ~~**J2 — `rkaf:requestContractDigest` for non-model runs.**~~ **Decided by
  the contract** (`e8794ba`). The field was universally REQUIRED, so the only
  conforming move was to define a canonical-json envelope
  `{instructions, actor_id, run_id, input_row}`, hash it, and cite the result —
  a real digest naming a contract the run never published. The field is now
  conditional on `rkaf:modelExtraction`. None of the four activities here is a
  model call, so all four minted digests are **deleted**, along with the
  envelope recipe in `build_projection.py`. What the runs actually consumed is
  still pinned by `rkaf:inputDigest` beside `rkaf:extractedBy` and
  `rkaf:extractorVersion`, which Core §2.4 now names as the reproduction
  handles for a non-model method.
- **J3 — authority modeled at citation level.** `authority_edges` is
  RIN+agenda-edition-keyed with `parse_status=partial`, so RA2 asserts
  `rkaf:agendaAuthorityCitation` on the `urn:rkaf:us:rin:0583-AE01` agenda
  item (`spec/rkaf-rulemaking.md` §7) rather than minting the stronger
  `rkaf:hasAuthority` → `rkaf:Authority` chain, which §3 warns against minting
  without action-specific evidence. Evidence binds to the FR document's own
  restatement (`F3`, "21 U.S.C. 451"), while `prov:wasDerivedFrom` pins the
  agenda-derived table row — two genuinely different sources, kept distinct.
- ~~**J4 — docket membership routes through the Proceeding.**~~ **Decided by
  the contract** (`3644803`). RA3 still asserts `rkaf:hasDocket` (proceeding →
  docket, `spec/rkaf-rulemaking.md` §3.2) on the FR document's own
  `[Docket No. FSIS-2025-0012]` heading (`F2`) — that assertion was never the
  workaround. The workaround was that the *document's* own membership had
  nowhere to go. The Artifact now carries
  `rkaf:publishedInDocket: urn:rkaf:us:regsgov:FSIS-2025-0012` directly, which
  new §5.3 makes an independent statement rather than a restatement of
  `rkaf:hasDocket`. Both objects were re-verified against the published tables
  by `build_projection.py` rather than taken on trust: `federal_register`'s
  `docket_ids_json` is `["Docket No. FSIS-2025-0012"]` and `dockets.parquet`
  independently carries the `FSIS-2025-0012` row (agency FSIS, type
  Rulemaking), so the edge points at a Docket with its own identity and is not
  minted from the document — §5.3's explicit prohibition.
- **J5 — `rkaf:assertedAt` semantics.** For the three deterministic
  relationship assertions the run's `asserted_at` (2026-07-24T06:57:16Z) was
  used; for the assignments and attestation, the authoring date. The spec ("when the
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

## Findings G1–G6 — all resolved in the contract

Every finding this exercise raised was answered on rulespec branch
`us-regulatory-identifiers`. The table is the index; the notes below say what
each resolution cost this document.

| # | Finding | Resolved by | Document change |
|---|---|---|---|
| G1 | `prov:wasDerivedFrom` requires materialized `prov:Entity` nodes, and only SHACL said so | `921d1ff` | none — the five typed records already conformed |
| G2 | no document→docket predicate | `3644803` | **added** `rkaf:publishedInDocket` on the Artifact |
| G3 | `#AssertionOrigin` had no deterministic-machine value | `3c16018` | **changed** three relationship assertions to `rkaf:deterministicExtraction` |
| G4 | `rkaf:requestContractDigest` presumed request-shaped extraction | `e8794ba` | **deleted** four fabricated digests |
| G5 | direct profile edges vs reified assertions unreconciled | `062fa79` | none — the existing pair is exactly what §2.1 now prescribes |
| G6 | `rkaf:attestedAt` (and nine more) missing from the context | `361348c` | none in the graph; the vendored context was **refreshed** |

- **G1 → `921d1ff`, "state the `prov:wasDerivedFrom` class range in §2.4".**
  The class requirement lived only in `compiled/shacl/core/{assertion,
  concept-assignment,relationship-assertion}.ttl` while §2.4 presented the
  field as a bare IRI list; that mismatch was this exercise's one authoring
  failure (6 violations). §2.4 now states the `prov:Entity` range and the
  materialization rule in prose, citing this projection as the case that found
  it. Documentation-only, so the five typed record nodes are unchanged and
  still conform. The gap was in discoverability, and discoverability is what
  was fixed.
- **G2 → `3644803`, "add `rkaf:publishedInDocket`".** Domain `rkaf:Artifact`,
  range `rkaf:Docket`, 0..*, registered in the profile's `l0-ranges.cue` so the
  compiled SHACL carries `sh:class rkaf:Docket`. New §5.3 states that it and
  `rkaf:hasDocket` are independent and neither implies the other. The Artifact
  node now carries it (see J4 for the verification). Note the dependency: this
  change is only *effective* because the vendored context was refreshed in the
  same pass — under the stale copy `rkaf:publishedInDocket` had no term
  definition, so it expanded as a string literal and `sh:class rkaf:Docket`
  could never fire on it. A refreshed context is a precondition for the
  predicate, not a cosmetic sync.
- **G3 → `3c16018`, "add `rkaf:deterministicExtraction`".** The new value means
  a mechanically reproducible derivation, and it makes
  `rkaf:hasExtractionProvenance` REQUIRED — closing exactly the seam J1 named,
  where the deterministic-parse fact sat on an optional edge. The three
  relationship assertions already carried provenance to a `deterministicParse`
  activity, so switching the origin was a one-line change per node with the
  requirement satisfied on arrival. The producer obligation that the activity's
  method be `deterministicParse` or `ruleBasedExtraction` holds here and is
  unchecked by design (the activity may live in another document).
- **G4 → `e8794ba`, "make `requestContractDigest` conditional".** The field is
  now REQUIRED only for `rkaf:modelExtraction`, and Core §2.4 adds that a
  digest over an envelope minted to satisfy the field is non-conforming — which
  turned this document's four digests from the only conforming move into a
  violation. All four are deleted and the envelope recipe is gone from
  `build_projection.py`. Consumers are told not to read absence as an unaudited
  run. Negative control 5 shows the new rule is normative but unmechanized; see
  the controls table above.
- **G5 → `062fa79`, "decide the direct-edge / reified-assertion pair in §2.1".**
  Both forms stay: the direct edge is the queryable projection, the assertion
  is the provenance-bearing source of truth, and where both name the same
  triple a consumer counts **one** statement. A producer SHOULD project an
  affirmed assertion and MUST NOT project a denied, superseded, or retracted
  one. This document's proceeding→docket pair — RA3 affirmed, plus
  `rkaf:hasDocket` on the Proceeding — is precisely the prescribed shape, so
  nothing changed. The double-counting risk is now a stated consumer rule
  rather than an open question. Like G3's method agreement, it is explicitly
  unmechanizable: matching a direct predicate against a reified triple compares
  a predicate IRI to a property value, which SHACL does not express.
- **G6 → `361348c`, "type the ten timestamp terms".** The sweep found the
  defect was ten terms, not one: every property the CUE annotates
  `// xsd:dateTime` whose term the context omitted, plus `rkaf:rationale` as
  `xsd:string`. `rkaf:attestedAt` and `rkaf:rationale` are both used by this
  document's Attestation, so refreshing `rkaf-context.jsonld` is what makes
  those two values expand as typed literals here. A new carrier test reads the
  `// xsd:…` annotation off each CUE declaration and fails the build if a
  coercion goes missing again — the convention was ungated before.

## Verdict on the phase-4 diagnostic L2/L3 projection gate

**Realistic — with one operational precondition, and the precondition is
cheaper than the first pass thought.** Evidence:

- A careful author starting from spicy-regs' published tables reached
  L1+L2+L3 green in **one** iteration; the only failure (G1) was mechanical.
  Re-targeting the document at a revised contract six commits later took
  **zero** iterations.
- The gates are not vacuous on real content: all 29 typed nodes dispatch to
  real compiled schemas (no silent unbound types), and four single-field
  mutations of this document each flip the expected gate.
- Cost is diagnostic-friendly: seconds per document (L2 is in-process JSON
  Schema; L3 is 193 triples against 54 shape files).
- The spicy-regs data model is already contract-adjacent: gold spans carry
  exactly the carrier-local URN bindings, and proceedings already store
  `rkaf:us-cfr` IRIs verbatim.

The precondition: the gates depend on `compiled/` JSON-Schema + SHACL outputs
that are **gitignored** in rulespec, so a phase-4 gate must pin a rulespec
commit *and* build the outputs from that commit — otherwise it silently
validates against whatever stale compile is lying around. The first pass
recorded this as needing "a CI `cue` install"; that was wrong, and the
correction makes the gate cheaper. `tools/compile_all.sh` drives
`tools/constraints_compile.py`, which is pure Python: a detached worktree plus
`make compile` is the whole setup, and it self-reports freshness by re-pinning
the contract digest. The 431-fixture corpus run is now corroboration rather
than the freshness argument.

Two things the second pass added to the precondition:

- **Vendored artifacts age silently.** The stale `rkaf-context.jsonld` would
  have made `rkaf:publishedInDocket` expand as a string literal, so `sh:class
  rkaf:Docket` never fires and the gate reports green on an edge it never
  checked. A gate that pins a contract commit must pin the *context* to the
  same commit, and re-vendor it whenever the pin moves.
- **L3 is load-bearing and cannot be skipped.** Two of the four biting controls
  (evidence-scheme mismatch, offset drift) are invisible to L2. And the fifth
  control marks the ceiling honestly: some obligations Core states normatively
  — minted request-contract digests (§2.4), evidence regions of the subject
  (§4.7.3 rule 3), projected-edge polarity (§2.1), deterministic-origin method
  agreement (§2.4) — are invisible to *every* level. A green ladder means the
  document is well-formed against the contract, not that its producer was
  honest.

## Reproduce

```sh
# regenerate the JSON-LD + verification transcript (hard-fails on any mismatch)
python3 docs/evidence/single-document-rulespec-projection-2026-07-28/build_projection.py

# build a read-only view of the pinned contract (leaves the user's checkout alone)
git -C /path/to/rulespec worktree add --detach /tmp/rulespec-062fa79 062fa79
cd /tmp/rulespec-062fa79 && make compile   # pure Python; no `cue` binary needed

# SHACL gate (@context must resolve; stage the file in-tree or vendor the
# refreshed context beside it)
python3 tools/ci_validate.py \
  /path/to/spicy-regs/docs/evidence/single-document-rulespec-projection-2026-07-28/fsis-2026-03227.rulespec.jsonld

git -C /path/to/rulespec worktree remove /tmp/rulespec-062fa79
```
