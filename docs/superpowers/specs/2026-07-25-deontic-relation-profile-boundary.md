# Deontic Relationship Profile Boundary

- **Date:** 2026-07-25
- **Status:** Design accepted; no generic legal inference enabled
- **Scope:** Domain interpretation after generic assertion comparison

## Decision

Generic relationship assertions record what a source expresses. They do not
record what the law requires, permits, prohibits, revokes, or exempts unless a
domain profile defines and proves that interpretation.

Keep these axes separate:

| Axis | Example | Owner |
| --- | --- | --- |
| Assertion polarity | a source affirms or denies `appliesTo` | Generic assertion contract |
| Source stance | source voice or attributed claimant | Evidence and attribution model |
| Social state | a consumer approves or rejects an assertion | Scoped attestation |
| Lifecycle change | a source proposes removal or effects suspension | Relation change event |
| Deontic force | required, permitted, or prohibited | Domain profile |
| Analytic result | affirmed and denied assertions differ | Generic comparator |
| Domain judgment | policy exclusion, exception, or legal effect | Domain profile plus review |

Conflating these axes creates reversals. For example, a prohibition on a
relationship is not the same proposition as a source's denial that the
relationship exists. A denied applicability statement is not automatically a
prohibition, and a proposed removal is not a present denial.

## Generic output

The generic comparator may emit only neutral outcomes:

- `satisfied`;
- `affirmed_denied_discrepancy`;
- `conflict`;
- `not_comparable`;
- `unknown`; and, after closure is proven,
- `expected_relation_not_observed`.

These outcomes do not imply intent, fault, severity, discrimination, legal
effect, or normative force.

## Profile-owned interpretation

A profile may implement `NormEvaluator`:

```text
evaluate(
  comparison or change event,
  accepted assertions,
  authority and warrant proofs,
  applicability scope and time,
  profile policy version
) -> DomainInterpretation
```

`DomainInterpretation` is a new, evidence-backed record. It references the
generic inputs and never mutates them. Its identity binds the profile and
policy version so a later legal interpretation can supersede it without
rewriting history.

The evaluator returns `applicable`, `not_applicable`, or `unknown` before it
returns a domain label. A published interpretation also records its rationale,
proofs, evaluation time, reviewer attestation, and any required authority.

## Regulatory profile

A regulatory profile may define labels such as:

- `policyExclusion`;
- `scopeException`;
- `applicabilityChange`;
- `revocationCandidate`;
- `supersessionCandidate`; or
- `legalEffectUnknown`.

It may emit one only when the profile proves:

1. the relevant source and actor have the required authority;
2. the exact artifact version and effective time are known;
3. the assertion, event, or omission is accepted and evidence-backed;
4. jurisdictional, population, conditional, and temporal scope match;
5. the profile contains an explicit interpretation rule; and
6. any required human review has approved the interpretation.

The generic term `terminologySuppression` is excluded because it implies
intent. A profile may adopt an intent-bearing label only with separate,
source-backed evidence of intent.

## Other profiles

The same boundary supports other domains:

- a scientific profile may distinguish reported absence from evidence of no
  effect;
- a judicial profile may distinguish a court's quoted argument from its
  holding;
- a procurement profile may distinguish bidder eligibility from an agency's
  exclusion decision;
- a contract profile may distinguish a missing clause from an express waiver.

Each profile defines its own authority, scope, and interpretation policies.
The generic comparator stays unchanged.

## 80/20 implementation rule

Do not add a universal deontic taxonomy to the core. Add `NormEvaluator` only
with the first concrete profile and corpus example that needs it. Reuse ODRL
or LegalRuleML concepts where they fit, but keep Rulespec as the contract
authority and preserve project-specific evidence and review gates.

The first profile needs:

- one small set of domain labels;
- explicit mapping rules and counterexamples;
- injected test adapters;
- at least one real positive and one confusing negative example per rule; and
- a rule that unknown inputs remain unknown.

No profile interpretation enters retrieval, publication, or training data
until its own precision gate passes.
