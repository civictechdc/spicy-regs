# Evidence-Backed Relationship Assertions and Exclusion Findings

- **Date:** 2026-07-24
- **Status:** Experimental stopping point
- **Decision:** Retain the general assertion and comparison kernel; reject the
  v1 benchmark as a fair model-comparison instrument
- **Scope:** Rulespec core, Spicy Regs experimental implementation, and one
  bounded OpenAI validation

## Outcome

The ontology can represent an explicit denial without inventing a
`negative_tag`. It now separates:

1. what a source asserts;
2. the evidence for that assertion;
3. who accepted or rejected it, for which consumer and time;
4. which baseline and document pairing make a comparison valid; and
5. the analytic finding produced by that comparison.

This structure applies to regulations, legislation, court opinions, reports,
guidance, contracts, policies, procurement records, and other documents.
Regulatory profiles may add domain predicates and warrants without changing
the core.

The first real OpenAI run also found a benchmark defect. Its output was
grounded and conservative, but the v1 oracle conflated time, attribution, and
conditionality and demanded one exact evidence span. Those choices converted
several defensible model answers into false errors. Preserve v1 as diagnostic
evidence; do not use its headline score to compare models.

## Ontological decision

### Model assertions, not negative tags

An assertion carries an affirmative predicate and an explicit polarity:

```text
subject --appliesTo / affirmed--> object
subject --appliesTo / denied----> object
```

The second statement means that a source explicitly denies the canonical
`appliesTo` relation. It does not mean:

- the relation was absent from a search result;
- the pipeline failed to extract it;
- the object is globally excluded;
- the source intended to discriminate; or
- another document should contain the relation.

Silence remains unknown. Omission analysis requires a separately proven
closed-world boundary and remains outside this experiment.

### Keep identity and association distinct

A filename, URL, RIN, docket number, citation, or source-issued identifier may
support a document relationship. None of these values alone establishes
document identity or a valid comparison pair. Two unrelated artifacts may
reuse a string; two related artifacts may use different identifiers.

The core therefore treats identifiers as claims with provenance. A
domain-specific pairing resolver decides whether two immutable artifact
versions may be compared.

### Keep immutable content separate from social state

`RelationshipAssertion` records immutable proposition content. Evidence,
confidence, attestation, and supersession remain separate records. The core
does not store one global `accepted`, `rejected`, or `current` state on the
assertion because those decisions depend on consumer, time, and policy.

`expected` and `observed` are comparison roles, not intrinsic assertion types.
The same assertion may serve either role in another comparison.

## Rulespec contract

Rulespec restores `rkaf:RelationshipAssertion` as a specialization of
`rkaf:Assertion`. Its CUE source of truth requires:

| Field | Cardinality | Meaning |
| --- | ---: | --- |
| `rkaf:assertsSubject` | 1 | IRI-valued subject |
| `rkaf:assertsPredicate` | 1 | IRI-valued canonical affirmative predicate |
| `rkaf:assertsObject` | 1 | IRI-valued object in v1 |
| `rkaf:assertionPolarity` | 1 | `rkaf:affirmed` or `rkaf:denied` |
| `rkaf:assertionOrigin` | 1 | Human, imported, or AI lineage class |

The general contract does not add `assertionMode`, `normativeForce`,
disposition, or a generic state field. Regulatory and legal profiles own
deontic meaning such as required, permitted, and prohibited. A future
`Proposition` resource may support quoted propositions or occurrence grouping,
but the present design does not need it.

Evidence bindings, attestations, confidence records, warrants, applicability,
and AI lineage remain first-class records. AI-touched assertions require AI
lineage.

The implementation includes CUE, JSON-LD context, SHACL classification,
positive fixtures, required-field negative fixtures, an AI-lineage edge
fixture, generated Rust, and TypeScript cross-file enum imports. The
TypeScript import fix is generic compiler infrastructure, not a
relationship-specific workaround.

These Rulespec changes form a local experimental candidate with contract
digest
`sha256:8ba09e0e7ea1eec9d6a3c8d5566d564d872e299611a7828d4ebaceaec27801b2`.
Spicy Regs remains pinned to its earlier released candidate in
`conformance/rulespec-l0.yaml`; it does not yet claim
`RelationshipAssertion` in published ontology tables. Update that pin only
through a versioned Rulespec release and an explicit Spicy Regs migration.

## Spicy Regs comparison kernel

Spicy Regs projects the Rulespec contract into immutable Python carriers:

- `RelationAssertion`;
- `RelationEvidenceBinding`;
- `AssertionAttestation`;
- `RelationComparisonContext`; and
- `RelationFinding`.

The only current finding kind is the neutral
`affirmed_denied_discrepancy`. A later domain profile may classify or explain
the discrepancy. The kernel does not infer intent, fault, severity, or legal
effect.

### Dependency inversion

The comparator depends on narrow protocols:

| Core protocol | Profile responsibility |
| --- | --- |
| `PredicateCatalog` | Approve a canonical subject-predicate-object relation |
| `AssertionStateResolver` | Resolve effective attestations for consumer and time |
| `EvidenceResolver` | Validate exact evidence against an artifact version |
| `BaselineResolver` | Prove why an assertion may serve as the expected baseline |
| `PairingResolver` | Prove why two artifact versions may be compared |
| `ScopeComparator` | Compare temporal and applicability scopes |

Static adapters support tests. Production adapters may use Rulespec graphs,
registries, databases, or external services. The kernel imports none of them.
It also imports no OpenAI SDK.

### Deterministic comparison

The comparator evaluates this sequence:

```text
validate expected assertion and affirmative polarity
resolve expected attestation, warrant, evidence, and predicate
prove baseline-to-observed artifact pairing
resolve each observation's attestation, evidence, predicate, and scope

affirmed only  -> satisfied
denied only    -> affirmed_denied_discrepancy
both           -> conflict
disjoint scope -> not_comparable
failed/unknown gate or no accepted observation -> unknown
```

No failed gate becomes a negative fact. No absence becomes an omission.

## Diagnostic v1 dataset

`tests/fixtures/relation_exclusion_explicit_denial_v1.json` locks 12 real
document cases:

- four direct denials;
- four affirmative controls;
- two temporal or proposed-change controls; and
- two unrelated or prompt-injection controls.

The sources cover at least eight document types. Each case includes an exact
source excerpt and digest. The model payload excludes role, gold candidates,
baseline polarity, pairing decisions, scope decisions, expected outcomes, and
assertion IDs.

The extraction adapter uses the existing OpenAI structured-output transport
behind a `StructuredOutputModel` protocol. The `jsonschema` package performs
local Draft 2020-12 validation. Exact evidence alignment and relation-ID
checks run before any candidate enters the comparator.

V1 deliberately preserves its original flat `modality` enum and single exact
gold span so the completed run remains reproducible. The module and future
receipts mark it `diagnostic-v1` and `publication_eligible: false`.

## Real OpenAI run

The fair-input run is preserved at
`docs/evidence/relation-exclusion-openai-candidate-run-2026-07-24-02/`.

| Property | Result |
| --- | ---: |
| Model | `gpt-5.6-sol` |
| Reasoning / service | medium / Priority |
| Storage | `false` |
| Attempts | 1 |
| Duration | 69,090 ms |
| Input / output / total tokens | 4,033 / 7,303 / 11,336 |
| Returned case records | 12 of 12, all unique |
| Candidate assertions | 9 |
| Exact source-substring quotes after alignment | 9 of 9 |
| Provider offsets already exact | 7 of 9 |
| Deterministically repaired unique offsets | 2 of 9 |
| False target candidates on unrelated controls | 0 |

The provider response ID is
`resp_0087dba3264fa08d016a64265acd648192939cc3d6fd895eb8`.
The request ID is `56b878e0-714c-4f69-ac86-fcd6afdeb6dc`.

### V1 automated score

The locked v1 scorer reports:

| Metric | V1 result |
| --- | ---: |
| Target-presence accuracy | 11/12, or 0.916667 |
| Exact polarity + modality + one gold quote F1 | 0.60 |
| Polarity + modality F1 | 0.70 |
| Comparison outcome accuracy | 9/12, or 0.75 |
| Direct-denial findings after oracle rejection | 2/4 |
| False control findings | 0 |

The quality receipt correctly fails its strict v1 gates. Revalidation now
rebuilds the payload and schema, validates the response, recomputes normalized
candidates, scores, and comparisons, and distinguishes
`integrity_status: pass` from `quality_status: fail`. The artifact hashes prove
internal consistency, not third-party provenance; the receipt remains
unsigned.

The earlier
`docs/evidence/relation-exclusion-openai-probe-2026-07-24/` run is historical
prompt-development evidence. It is not part of the fair-input comparison.

## Independent adversarial findings

Three independent reviews reached the same conclusion: the completed model
call is real and grounded, but v1 is not a fair model-comparison instrument.

### 1. One modality field combines different dimensions

`current`, `proposed`, and `historical` describe lifecycle or time.
`attributed` describes who owns a statement. `conditional` describes logical
force. A passage may be current, attributed, and conditional at once.

This collision creates direct GAO and FCC scoring disagreements and propagates
them into rejected attestations and missed findings.

### 2. One exact gold quote over-penalizes valid evidence

The CFR mining response supplied a shorter exact span that entails the target
relation more precisely than the gold span. V1 counted one boundary choice as
a false positive, a false negative, a rejected assertion, an `unknown`
comparison, and a missed denial.

Evidence grounding, semantic entailment, and preferred boundary agreement
must be separate metrics. The oracle should accept multiple sufficient spans.

### 3. Proposed change is not present denial

A proposed removal or suspension describes a change event. It does not always
assert that the timeless relationship is presently denied. V1 collapses these
meanings into polarity plus modality and can manufacture a false current
discrepancy.

V2 needs explicit lifecycle events or time-scoped assertions with a declared
temporal anchor.

### 4. At least one gold relation overreaches the text

One bill excerpt does not explicitly state the gold `imports` relation. The
model's abstention follows the no-inference rule. The mining gold also
over-spans an alternative outside the target object. GAO and FCC disagreements
are taxonomy conflicts, not clean extraction errors.

The oracle needs independent adjudicator identities, timestamps, rationales,
accepted alternative spans, disagreement state, and a blind second review.

### 5. The end-to-end score hides oracle injection

The v1 comparator receives baseline, pairing, scope, and attestation decisions
from the oracle. Its result validates deterministic execution conditional on
those inputs. It does not measure the precision of baseline discovery,
document pairing, scope comparison, or acceptance policy.

Future reports must separate comparator-on-gold evaluation from end-to-end
system evaluation.

### Defensible human re-adjudication

The reviews found all four direct-denial semantics in the raw response. Across
the defensible required candidates, the response produced nine of ten with no
unsupported target candidates: 100% precision and 90% recall under that
informal re-adjudication. All nine quotes occur verbatim in their source
excerpts.

These values diagnose the v1 oracle; they are not replacement benchmark
scores.

## Stopping-point decision

Keep:

- the Rulespec `RelationshipAssertion` specialization;
- explicit affirmed or denied polarity;
- separate evidence, confidence, warrants, attestations, and AI lineage;
- explicit comparison context;
- dependency-inverted resolvers;
- neutral deterministic findings;
- open-world handling of absence; and
- candidate-only LLM extraction.

Reject or defer:

- `negative_tag`;
- global assertion disposition;
- generic `assertionMode` and `normativeForce`;
- intent inference;
- omission without proven closure;
- a timeless interpretation of proposed changes;
- v1's flat modality;
- single-span exact-match scoring; and
- another OpenAI run against the unchanged v1 benchmark.

This is a fair stopping point. The ontology kernel has enough evidence to
continue. The evaluation ontology needs revision before another model,
chunker, embedding model, or reranker comparison would mean anything.

## V2 acceptance contract

Freeze a new dataset and schema before the next benchmark-eligible paid run:

1. Replace `modality` with independent applicability time, textual
   attribution, and conditionality records.
2. Represent proposed adoption, removal, suspension, and supersession only as
   relation change events. A separate affirmed or denied assertion is allowed
   only when the source independently expresses that proposition.
3. Record source-relative or explicit applicability time. Derive
   `current_at_evaluation`; insufficient temporal facts produce `unknown`.
4. Allow multiple accepted evidence spans; score source alignment, entailment,
   and boundary preference separately.
5. Score relation identity and polarity independently from temporal,
   attribution, and conditional dimensions.
6. Use case-specific output constraints or attach target IDs
   deterministically after extraction.
7. Require two distinct, sealed human reviews of the full corpus, blind to
   provider output, machine proposals, and each other. Bind both reviews to
   the same corpus and protocol digests, then record agreement or a third-human
   resolution.
8. Separate extraction, grounding, attestation, comparator-on-gold, and
   end-to-end metrics.
9. Require zero unrelated target candidates and zero false current
   discrepancies.
10. Keep omission disabled.
11. Recompute every derived artifact during validation and preserve malformed
    completed responses with failure receipts.
12. After freezing v2, run three identical blinded repetitions before
    comparing providers or configurations.

Do not optimize regexes, segmentation, embeddings, or reranking against v1's
known label defects. Those components may improve retrieval, but they cannot
repair an incoherent target ontology.

### Implemented v2 scaffold

The repository now has a separate, gold-free v2 source corpus:

- `tests/fixtures/relation_exclusion_explicit_denial_v2_corpus.json`;
- `tests/fixtures/relation_exclusion_explicit_denial_v2_oracle.provisional.json`;
- `src/spicy_regs/corpora/relation_exclusion_evaluation_v2.py`; and
- `tests/test_relation_exclusion_evaluation_v2.py`.

The corpus is physically independent of v1 and contains no role, expected
candidate, baseline polarity, pairing, attestation, or finding fields. Its
content ID is
`ad39e0c2a96cd5c89b9727163e9494882cf476046c84953ab772513a84bcff36`.
Case and artifact-version identifiers are opaque or content-derived; they do
not disclose evaluation roles. Python character offsets are declared as
Unicode code-point offsets.

The candidate contract now distinguishes:

- `relation_assertion`, with affirmed or denied polarity;
- `relation_change_event`, with an operation and lifecycle stage;
- assertion applicability or event time;
- a change event's intended effect time;
- source voice, attributed source, or unclear attribution; and
- explicit, not-explicit, or unclear conditionality.

`not_explicit` does not claim logical unconditionality. A proposal cannot carry
assertion polarity. Textual attribution remains distinct from Rulespec
`assertionOrigin`, which records human, AI, or import lineage.

The canonical v2 extraction prompt is a lean proof-certificate prompt. Before
emitting an item it checks subject, predicate, object, assertion-versus-event
kind, time, voice, condition, and evidence boundary, then tests the strongest
opposite reading. This method is part of the provider-independent extraction
contract; it contains no benchmark roles, oracle labels, or case-specific
answers.

Target identifiers are never generated by the model. The normalizer attaches
the one locked target for each case, validates or uniquely repairs exact source
offsets, and derives `current_at_evaluation` only from sufficient temporal
facts. Document-relative present tense alone remains `unknown` at evaluation
time.

Scoring now separates:

- target-relation and polarity or event-operation matching;
- applicability-time, intended-effect-time, attribution, and conditionality;
- exact grounding and native-versus-repaired offsets;
- human-adjudicated evidence sufficiency, accepted alternatives, and
  preferred boundaries;
- unrelated or unsupported target candidates; and
- false current discrepancies.

Candidates with the same subject-predicate-object relation but different
claimant, condition, or temporal scope remain separate variants. Evidence
boundaries are not part of the semantic candidate identity.

The v2 scorer compares claimant surface forms case-insensitively and ignores a
leading article such as `the FCC` versus `FCC`, while preserving the submitted
claimant text. Allowed semantic variants are matched independently from their
evidence grade. Evidence remains exact-grounded and is scored separately.
Terminal-punctuation-only boundary differences receive an explicit
boundary-equivalent grade; they do not become exact matches, and unreviewed
enclosing spans still do not inherit entailment.

The checked-in oracle is explicitly `provisional-machine-assisted`. It records
no human review and no resolution. The run gate therefore fails closed with:

1. oracle is not final human-adjudicated;
2. no freeze time;
3. zero of two required blind human reviews; and
4. no human resolution.

A future final gate also validates complete corpus coverage and candidate
semantics in both reviews, corpus and protocol digests, distinct review content
digests, review and resolution chronology, exact-agreement identity, or an
exhaustive third-human disagreement ledger. An allowed candidate is neutral:
emitting it cannot repair recall for a missing required candidate. Ambiguous
reviews must enumerate at least two readings; final `ambiguous` and `abstain`
cases are retained but excluded from every score.

Human review decisions do not contain denial/control roles. Those
post-resolution reporting strata never participate in candidate matching and
cannot cue reviewers toward the provisional oracle. Reviewers also do not
assign candidate IDs; the validator derives opaque IDs from the complete
semantic variant after submission.

The gate recomputes and exposes the final-oracle and resolution content
digests for a future provider receipt. Evidence sufficiency is credited for an
exact adjudicated span or a separately reported terminal-punctuation-equivalent
boundary. An unreviewed enclosing span remains separately visible but cannot
inherit entailment from a contained quote.

Two paid, five-case v2 diagnostics now exist. Both are explicitly exposed-case,
non-publication, non-benchmark evidence. The plain v2 prompt reached `0.800`
provisional semantic F1. The canonical proof-certificate v2 prompt recovered
all five required outputs with `1.000` recall, `0.909` reported F1, exact
grounding, and five preferred-exact required evidence spans. Its one reported
false positive was the allowed FCC embedded denial expressed as `the FCC`
where the provisional oracle stored `FCC`; the integrated claimant
normalization makes that variant neutral.

The focused runs also justified two provisional contract corrections:

- the CFR `where` clause is explicit conditional scope; and
- attribution accepts a person, organization, instrument, document, or other
  source rather than requiring an actor.

These changes do not create a fair benchmark result. The pilot cases were
already exposed by v1 and focused prompt development. Two humans must still
independently annotate them under the
[human adjudication protocol](2026-07-25-relation-exclusion-v2-human-adjudication-protocol.md)
to create a reviewed regression oracle, and a new untouched holdout must be
frozen before provider comparison. Human extraction decisions must not become
substantive approval of the propositions.

The current v2 module is a contract, normalizer, scorer, and human-review gate,
not yet an end-to-end provider pipeline. Its CLI validates corpus, oracle, and
run eligibility only. The focused OpenAI artifacts were produced by bounded
one-off orchestration and correctly retain the earlier
`attributed_actor` schema and pre-normalization scores as immutable historical
evidence. A reusable v2 runner still needs injected OpenAI and Codex adapters,
atomic success and failure receipts, validation and derived-artifact rebuild,
and final-oracle and resolution digest binding.

The automated gate proves artifact integrity, not real-world human identity.
A project steward must verify the reviewers and their blindness outside the
JSON audit trail. The pilot remains non-publication-eligible until a trusted
identity or signature mechanism is chosen.

`relation_change_event` remains evaluation-local. Rulespec's generic
`LifecycleEvent` describes resource lifecycle and is not a safe publication
carrier for a proposed change to a relation. Likewise, an attributed
third-party assertion cannot enter the comparator until a separate
observed-use policy proves that claimant is eligible for the comparison.

### Standards alignment

The split is consistent with existing general-purpose standards:

- [PROV-O](https://www.w3.org/TR/prov-o/) separates entities, activities,
  agents, attribution, and qualified roles;
- [OWL-Time](https://www.w3.org/TR/owl-time/) provides explicit instants,
  intervals, and temporal relations instead of a flat lifecycle label;
- [Web Annotation Selectors and States](https://www.w3.org/TR/selectors-states/)
  permits exact text and position selectors to coexist;
- [ODRL 2.2](https://www.w3.org/TR/odrl-model/) separates permissions,
  prohibitions, duties, and constraints rather than placing legal force on a
  generic relationship; and
- [LegalRuleML 1.0](https://docs.oasis-open.org/legalruleml/legalruleml-core-spec/v1.0/legalruleml-core-spec-v1.0.html)
  models legal status, temporal development, source, role, and deontic
  semantics as distinct concerns.

These standards inform the separation of concerns; they do not replace the
project's immutable artifact identity, evidence, review, or comparison
contracts.

## Verification evidence

Rulespec's canonical compiler regenerates every target from CUE. Constraint
parity covers the new positive, negative, and edge fixtures across generated
targets. Rust round-trip tests exercise both affirmed and denied fixtures.

Spicy Regs tests cover:

- immutable assertion and evidence invariants;
- scoped and temporal attestation;
- every comparator outcome and failed gate;
- locked-corpus validation and oracle isolation;
- cross-case relation-ID rejection;
- exact evidence alignment;
- provider-failure receipts;
- derived-artifact recomputation after coherent hash-map tampering; and
- the bounded OpenAI artifact validator.

The durable request contains no credential. Repository secret scans cover the
dataset and both evidence directories.
