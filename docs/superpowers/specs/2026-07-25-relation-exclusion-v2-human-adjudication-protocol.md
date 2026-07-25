# Relation-Exclusion V2 Human Adjudication Protocol

- **Status:** Candidate protocol; no reviews submitted
- **Corpus:** `tests/fixtures/relation_exclusion_explicit_denial_v2_corpus.json`
- **Corpus content ID:** `ad39e0c2a96cd5c89b9727163e9494882cf476046c84953ab772513a84bcff36`
- **Offset unit:** Unicode code points, zero-based, half-open
- **Omission analysis:** Disabled
- **Required reviewers:** Two distinct humans

## Purpose

Create a reviewed regression oracle for explicit target-relation assertions
and relation change events, and define the annotation contract that a future
untouched benchmark must reuse. These cases were exposed during v1 diagnosis
and later focused prompt development, so they are development cases rather
than a model-comparison holdout. The review establishes what the supplied
source text expresses. It does not decide whether a proposition is true,
legally controlling, comparable to another document, or accepted by a
consumer.

## Blindness

Each reviewer receives only:

1. the immutable v2 corpus;
2. this protocol; and
3. a blank review form.

Until both reviews are sealed, a reviewer must not see:

- v1 or v2 model output;
- the provisional machine-assisted oracle;
- another review;
- candidate scores or comparison findings; or
- prompts that identify expected cases or roles.

Record reviewer identity, start and submission times, the corpus content ID,
and a digest of this protocol. A review is sealed when its complete canonical
JSON digest is recorded and the file is made read-only for the resolution
step.

Corpus case IDs are opaque. Reviewers must not try to recover historical case
names or evaluation roles from another artifact.

The software can verify distinct declared identities, chronology, and content
digests; it cannot prove that two strings belong to two real people. Before
review files are accepted, a project steward must verify reviewer identity and
blindness outside the artifact and retain that administrative evidence. This
pilot remains ineligible for publication until a trusted identity or signature
mechanism is selected.

## Per-case decision

Assign one status:

- `annotated`: the source explicitly supports at least one candidate;
- `no_explicit_support`: it does not support the complete target relation;
- `ambiguous`: two or more readings remain defensible;
- `abstain`: the reviewer cannot adjudicate the case.

An empty candidate list means only that no explicit support was found in the
frozen excerpt. It never means the target relation is denied.

Also assess `target_quality`:

- `valid`: subject, predicate, and object form a source-answerable target;
- `underspecified`: the target needs a narrower scope or referent;
- `unsupported_argument`: a target argument is absent from the excerpt; or
- `invalid`: the target is malformed or incoherent.

## Candidate types

### Relation assertion

Use `relation_assertion` only when the excerpt independently affirms or denies
the exact target proposition.

Required fields:

- `polarity`: `affirmed` or `denied`;
- `temporal_scope`;
- `attribution`;
- `conditionality`;
- one or more exact evidence spans; and
- a concise rationale.

Polarity never represents proposed, historical, suspended, withdrawn, or
superseded status.

### Relation change event

Use `relation_change_event` for a change to the target relation.

Required fields:

- `operation`: `adopt`, `remove`, `suspend`, or `supersede`;
- `stage`: `proposed`, `decided`, `effective`, `withdrawn`, or `unclear`;
- event time;
- intended effect time;
- attribution;
- conditionality;
- one or more exact evidence spans; and
- a concise rationale.

A proposed event never enters the affirmed-versus-denied comparator. Add a
separate assertion only if the source independently expresses it.

## Orthogonal dimensions

### Time

Record the relation of the assertion, event, or intended effect to:

- `document_time`;
- `evaluation_time`;
- `explicit_time`; or
- `unknown`.

Use `before`, `includes`, `after`, `atemporal`, or `unknown`. For an explicit
time, record timezone-aware start or end bounds. Preserve the relevant
temporal wording when present.

Do not label a proposition current merely because the source uses present
tense. `current_at_evaluation` is derived later. If the excerpt does not prove
current applicability at the evaluation instant, the result is `unknown`.

### Attribution

Use:

- `source_voice`;
- `attributed_source`, with the person, organization, instrument, document, or
  other claimant's exact source wording; or
- `unclear`.

This field identifies who the document presents as expressing the proposition.
`source_voice` includes an issuing organization speaking in its own document;
`attributed_source` is reserved for a distinct reported claimant. A comparison
may normalize superficial determiners such as `the FCC` versus `FCC`, but the
stored claimant text remains verbatim. Entity resolution is a later,
evidence-bearing operation.
It is not Rulespec `assertionOrigin`, which records human, AI, or import
lineage.

### Conditionality

Use:

- `explicit`, with the condition text;
- `not_explicit`; or
- `unclear`.

`not_explicit` means only that the supplied excerpt expresses no condition. It
does not prove logical unconditionality.

## Evidence

Every non-unclear structured decision must have exact source support.

- Text must equal the declared source substring.
- Offsets are zero-based, half-open Unicode code-point positions.
- Prefer the smallest jointly sufficient span or span set.
- Record multiple alternative sufficient boundaries when appropriate.
- Record an exact but insufficient boundary when it is a plausible extraction
  boundary that omits a load-bearing qualifier; acceptance of the boundary
  does not imply semantic sufficiency.
- Do not transfer sufficiency to an unreviewed enclosing span merely because
  it contains a sufficient quote; additional context can reverse, reject, or
  qualify the embedded statement.
- Keep source alignment, semantic sufficiency, and preferred boundary choice
  as separate judgments.
- A scorer may recognize a terminal-punctuation-only boundary difference as
  equivalent while retaining the exact submitted span and reporting that
  boundary grade separately from exact agreement.
- A quoted claimant statement proves that the claimant expressed a
  proposition. It does not by itself establish substantive acceptance.

## Independent review output

Each review must contain:

- review and reviewer IDs;
- corpus and protocol digests;
- start and submission times;
- explicit statements that model output and the other review were hidden;
- one review record for every corpus case, containing `case_id`,
  `target_quality`, `case_status`, a nonempty review rationale, and the
  structured candidate decision;
- exact evidence and rationale; and
- a canonical content digest.

The two humans independently produce full candidate sets. They do not approve
or reject model-generated candidates.

Review decisions contain only `case_id` and `expected_outputs`; they never
contain benchmark roles such as denial or control. Reporting roles are
assigned after the sealed resolution, remain outside the review comparison,
and affect stratified reporting only—not candidate matching.

Review candidates likewise contain no `candidate_id`. After validation, the
review tool derives an opaque identity from the case and complete semantic
variant. This prevents provisional-oracle names from entering a blind review
and prevents arbitrary reviewer-local identifiers from creating false
disagreements.

Each review must cover every opaque corpus case exactly once. Its declared
case-review digest is computed after schema validation and canonical case
ordering. `annotated` requires at least one candidate;
`no_explicit_support` and `abstain` require none. An `invalid` or
`unsupported_argument` target cannot carry a candidate. The separate content
digest binds reviewer identity, timestamps, blindness declarations, corpus
and protocol digests, and the complete case reviews.

`ambiguous` requires at least two recorded candidate readings. A final
`ambiguous` or `abstain` case is unresolved: the resolution must list its
opaque case ID in `excluded_case_ids`, and the scorer must omit every output
from that case from all quality metrics.

## Resolution

Compare the sealed reviews only after both are submitted.

- Exact structured agreement may be copied into the resolution.
- Any difference in target quality, case status, candidate kind, polarity,
  operation, stage, attribution, time, condition, or evidence sufficiency is a
  disagreement.
- A third human, distinct from both reviewers, resolves each disagreement.
- Record both inputs, the resolved value, and a rationale.
- Keep unresolved cases in the artifact but exclude them from scoring.

The resolution time must follow both submissions, and the freeze time must
follow resolution. When the two canonical reviews agree exactly, no third
resolver may be claimed. When they differ, the resolver must be a third human
and the disagreement ledger must cover every differing JSON path, both input
values, the resolved value, and a rationale.

The final resolution binds:

- the corpus content ID;
- the protocol digest;
- both review IDs and content digests;
- the complete disagreement ledger;
- the resolved case reviews when the sealed reviews differ;
- the exact set of unresolved case IDs excluded from scoring;
- the resolved case digest; and
- the freeze time.

All start, submission, resolution, and freeze instants must be timezone-aware,
ordered as described above, and no later than the instant at which the run
gate is evaluated. Future-dated audit records fail closed.

## Development diagnostics and benchmark run gate

The complete v2 pilot and the five previously disputed cases are exposed
development material. A paid diagnostic may use an explicitly named exposed
subset only when its receipt:

1. marks the run `publication_eligible: false` and
   `benchmark_eligible: false`;
2. records the exact selected case IDs, prompt, schema, provisional oracle,
   provider configuration, response, scores, and artifact digests;
3. reports core semantics, dimensions, and evidence separately;
4. states that case and prompt selection followed observed failures; and
5. is never used to rank providers, estimate generalization, or set production
   thresholds.

No benchmark-eligible v2 provider call or provider comparison may occur until:

1. the corpus and protocol are frozen;
2. two distinct, complete, model-blind human reviews exist;
3. their digests validate;
4. every disagreement is resolved or explicitly excluded;
5. the final oracle is content-addressed; and
6. the request receipt binds the corpus, protocol, and resolution digests.

The resolution records a canonical content digest, and the final oracle
records a canonical digest computed with its own digest field omitted. The run
gate recomputes both and returns them as the only values a future request
receipt may bind.

Because this pilot is already exposed, its completed human review produces a
regression oracle, not an untouched holdout. After the gate passes for a new
holdout drawn under the same contract, run three identical blinded
repetitions. Report provider completion, exact grounding, native offsets,
semantic extraction, each orthogonal dimension, evidence sufficiency,
unsupported controls, false current discrepancies, and variance separately.
