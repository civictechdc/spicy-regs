# Relationship Comparison Resolver Contract

- **Date:** 2026-07-25
- **Status:** Candidate production contract
- **Scope:** Generic assertion comparison in Spicy Regs
- **Architecture:** Dependency-inverted core with profile-owned adapters

## Purpose

The comparator should contain only deterministic orchestration. It must not
know how a predicate registry, database, Rulespec graph, document parser,
OpenAI client, or legal profile works. Those systems implement narrow
project-owned protocols and return evidence-backed decisions.

This contract preserves the current comparison kernel while defining what a
production resolver must prove.

## Common decision envelope

Every resolver returns one of:

- `pass`: the requested proposition is proved under the named policy;
- `fail`: the proposition is disproved or ineligible under the named policy;
- `unknown`: available evidence cannot decide it.

Each decision includes a concise rationale and one or more proof-record
identifiers when evidence exists. The comparator treats `fail` as a gate
result, not as a negative source fact. `unknown` never becomes `fail`.

## Core protocols

| Protocol | Question |
| --- | --- |
| `PredicateCatalog` | Is this canonical subject-predicate-object relation valid, and do two assertions describe the same relation? |
| `AssertionStateResolver` | Is this assertion accepted for this consumer, scope, and evaluation time? |
| `EvidenceResolver` | Does exact evidence in this artifact version support this assertion occurrence? |
| `BaselineResolver` | May this assertion serve as the expected baseline under an active warrant? |
| `PairingResolver` | May these artifact versions be compared for this purpose? |
| `ScopeComparator` | Are the assertions' temporal, jurisdictional, conditional, and applicability scopes comparable? |

Longitudinal omission adds:

| Protocol | Question |
| --- | --- |
| `VersionLineageResolver` | What source-backed version or peer relation connects the artifacts? |
| `ExpectedCoverageResolver` | Which baseline relations should this observed boundary address? |
| `ClosureResolver` | Is the observed relation set complete for the exact declared boundary? |

A domain profile may add a `NormEvaluator` after comparison. It does not alter
generic gate results.

## Proof records

An opaque string is not proof. Every proof identifier used in a published
result must resolve inside the same immutable generation or through a pinned,
reachable external record.

A proof record binds:

- proof type and stable identifier;
- resolver and policy version;
- input identifiers and content digests;
- evidence, warrant, or attestation identifiers;
- evaluation time and applicable scope;
- `pass`, `fail`, or `unknown`;
- rationale;
- producer or run lineage; and
- record digest or signature.

Validation fails when a proof cannot be resolved, its digest differs, its
inputs do not match the comparison, or its policy is not permitted by the
active profile.

## Comparison rule

The comparator follows a fail-closed sequence:

1. validate the expected assertion;
2. resolve its current attestation, evidence, and baseline warrant;
3. prove artifact pairing;
4. validate each observed assertion and its evidence;
5. compare relation identity and scope;
6. classify affirmed and denied observations; and
7. emit a neutral result with all proof records.

Required `unknown` decisions stop the comparison as `unknown`. Disjoint or
ineligible comparisons become `not_comparable`. No observation is accepted
because another candidate resembles it.

## LLM boundary

An LLM adapter may:

- segment or receive bounded source text;
- propose structured assertions or change events;
- identify exact evidence spans;
- report uncertainty; and
- challenge a provisional human baseline.

It may not:

- approve its own assertion;
- establish document identity or pairing;
- claim closure;
- decide legal authority or effect;
- create a final comparison finding; or
- replace deterministic receipt validation.

Provider SDKs stay behind the existing `StructuredOutputModel` seam. Provider
configuration and response telemetry belong in run receipts, not in assertion
or finding identity.

## Package-first implementation

Use maintained packages for standard mechanics before adding custom code:

- `jsonschema` for candidate validation;
- Rulespec-generated schemas and SHACL for contract validation;
- Web Annotation-compatible selectors for evidence;
- content digests and DuckDB constraints for immutable local proofs; and
- provider SDKs only inside adapters.

Own the small comparison and proof policy because no general package can know
which documents, scopes, warrants, and closure claims this project accepts.

## Production minimum

Static adapters remain test fixtures. A production adapter is ready only when
it:

1. returns all three decision states;
2. emits resolvable proof records;
3. binds inputs, policy, time, and scope;
4. has positive, negative, and unknown tests;
5. fails closed on missing or stale dependencies;
6. participates in a deterministic run receipt; and
7. can be replaced without changing the comparator.

This is the complete 80/20 contract. New protocols require a distinct question
that the existing interfaces cannot answer without mixing responsibilities.
