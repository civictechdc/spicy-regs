# Relationship Assertion Adversarial Review Synthesis

- **Date:** 2026-07-25
- **Status:** Accepted design input; implementation remains Experimental
- **Scope:** Rulespec relationship assertions and the Spicy Regs comparison
  prototype
- **Source:** Three independent subagent reviews supplied by the project owner

## Decision

Keep the relationship-assertion idea, but keep four concerns separate:

1. a source's proposition;
2. evidence and provenance for that proposition;
3. a consumer's scoped acceptance or rejection; and
4. a neutral finding produced by comparing accepted assertions.

Do not model a `negative_tag`. An explicit denial is a sourced assertion with
denied polarity. A relation missing from a later document is an analytic
candidate whose meaning depends on document lineage, scope, and proven
closure. Silence without those proofs remains unknown.

This design works for regulations, legislation, court opinions, reports,
contracts, policies, scientific publications, and procurement records.
Domain profiles may add predicates and interpretation rules without changing
the generic assertion contract.

## Findings and disposition

| Review finding | Disposition | Remaining work |
| --- | --- | --- |
| Rulespec prose and authoritative CUE disagreed about proposition fields. | The local Rulespec candidate now requires subject, predicate, object, polarity, and origin for `RelationshipAssertion`. | Review, release, and consume the contract through a versioned migration. |
| Assertion content contained a global social disposition. | Spicy Regs now keeps immutable assertion content separate from scoped, temporal `AssertionAttestation` records. | Map the generic pattern into released Rulespec terms and storage. |
| Comparison identity omitted scope, time, and document pairing. | `RelationComparisonContext` and injected resolvers now carry those gates. | Replace static test adapters with production proof-producing resolvers. |
| “Complete enumeration” was an unproved Boolean. | Omission stays disabled. The longitudinal design requires an evidence-bound closure claim. | Implement and independently measure closure before enabling omission findings. |
| Polarity, attribution, lifecycle state, and deontic force were conflated. | V2 separates assertions from change events and models time, attribution, and conditionality independently. | Add domain interpretation only where a profile needs legal or policy meaning. |
| Opaque evidence and warrant identifiers were treated as proof. | Resolver results must name proof records, not unexplained strings. | Define proof-record persistence and dereference checks. |
| Some proposed finding names implied intent. | The generic kernel emits only neutral discrepancies. | Keep labels such as `policyExclusion` in reviewed domain profiles. |
| An LLM was allowed to decide the final finding. | The OpenAI adapter now proposes structured candidates and exact evidence only. | Preserve the boundary in every provider adapter and receipt. |

## Failure propagation

The review identified three levels of failure:

- A direct failure misstates one assertion, scope, or evidence span.
- A secondary failure admits or rejects that assertion under the wrong
  consumer, time, or comparison.
- A tertiary failure turns the bad comparison into a false exclusion,
  omission, legal effect, retrieval feature, or training label.

The architecture stops propagation at each boundary. Failed or unknown gates
produce `unknown` or `not_comparable`; they never become negative facts.

## Minimal retained architecture

```text
immutable artifact version
  -> candidate assertion plus exact evidence
  -> predicate, evidence, scope, state, baseline, and pairing gates
  -> deterministic neutral comparison
  -> optional domain interpretation
  -> reviewer attestation
```

The core depends on project-owned protocols. Rulespec graphs, databases,
registries, OpenAI, and other providers remain adapters. This preserves
dependency inversion and lets a profile replace one resolver without
rewriting the comparator.

## Package and standards boundary

The 80/20 approach is to reuse mature tools for established carrier concerns:

- Rulespec CUE and generated JSON Schema or SHACL for structural validation;
- Web Annotation-style selectors for exact source spans;
- PROV-O concepts for lineage and attribution;
- OWL-Time concepts for instants and intervals;
- ODRL or LegalRuleML patterns inside legal profiles; and
- `jsonschema`, DuckDB, and content-addressed receipts for local validation.

No package found in the review owns the crucial project rule: when two
evidence-backed, scoped assertions may be compared and what neutral result
follows. Spicy Regs should own that small kernel behind adapters rather than
introduce another schema authority.

## Evaluation evidence

The review described an early 14-case OpenAI probe as useful extraction
evidence, not a benchmark. That probe asked the model for a finding label and
did not bind its full instructions and schema into the request receipt.

The later canonical diagnostic is the 12-case v1 run recorded in
[the relationship assertion design](../superpowers/specs/2026-07-24-relation-exclusion-findings-design.md).
It proved real structured extraction and exposed oracle defects. The
[v2 adjudication protocol](../superpowers/specs/2026-07-25-relation-exclusion-v2-human-adjudication-protocol.md)
must pass before another paid comparison run.

## Open gates

1. Release the generic Rulespec assertion contract.
2. Migrate Spicy Regs to that released contract.
3. Implement production resolvers with dereferenceable proof records.
4. Complete two blind v2 reviews, resolution, and three identical model runs.
5. Prove closure accuracy before enabling longitudinal omission.
6. Add domain interpretation only after the generic evidence gates pass.
