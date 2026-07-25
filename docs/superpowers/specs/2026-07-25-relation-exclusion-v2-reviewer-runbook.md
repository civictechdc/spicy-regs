# Relation-Exclusion V2 Reviewer Runbook

- **Date:** 2026-07-25
- **Status:** Ready for operator use; no reviews submitted
- **Authority:** The
  [human adjudication protocol](2026-07-25-relation-exclusion-v2-human-adjudication-protocol.md)

## Purpose

This is the short operating checklist for producing the two independent human
reviews required to turn the exposed v2 pilot into a reviewed regression
oracle. A new, untouched corpus following the same contract is required for a
provider-comparison benchmark. The protocol controls when this checklist and
the protocol differ.

## Steward preparation

- [ ] Verify the corpus content ID against the protocol.
- [ ] Freeze the protocol digest.
- [ ] Choose two distinct human reviewers and record administrative proof of
      identity and independence outside the review files.
- [ ] Give each reviewer only the source corpus, protocol, and blank review
      form.
- [ ] Hide model output, the provisional oracle, benchmark roles, prompts, and
      the other review.
- [ ] Record timezone-aware start times.

## Reviewer worksheet

For every opaque case ID, record:

- target quality;
- case status;
- each explicit relation assertion or relation change event;
- independent time, attribution, and conditionality fields;
- exact source spans with zero-based, half-open Unicode code-point offsets;
- evidence sufficiency and any accepted alternative boundaries; and
- a concise rationale.

Use `no_explicit_support` for source silence. Never convert an empty candidate
list into denied polarity. Use `ambiguous` with at least two readings, or
`abstain`, when the source does not support one defensible annotation.

## Seal each review

- [ ] Cover every corpus case exactly once.
- [ ] Validate field values, offsets, and exact substrings.
- [ ] Confirm that no benchmark role or reviewer-assigned candidate ID appears.
- [ ] Record blindness declarations and submission time.
- [ ] Compute the canonical review digest.
- [ ] Make the submitted review immutable for resolution.
- [ ] Keep the two reviews hidden from each other until both are sealed.

## Resolve

- [ ] Compare the two sealed reviews structurally.
- [ ] Copy exact agreement without a third reviewer.
- [ ] Send every disagreement to a distinct third human.
- [ ] Record each differing path, both values, the resolution, and rationale.
- [ ] Exclude unresolved `ambiguous` and `abstain` cases from all scores.
- [ ] Compute the resolution and final-oracle digests.
- [ ] Freeze only after both submissions and resolution.

## Benchmark provider run gate

Focused paid diagnostics on explicitly exposed cases must remain labeled
non-benchmark and non-publication-eligible under the protocol. Before any
benchmark-eligible paid call, verify:

- [ ] corpus and protocol are frozen;
- [ ] two distinct complete reviews validate;
- [ ] disagreements are resolved or excluded;
- [ ] final oracle and resolution digests recompute; and
- [ ] the request receipt will bind both digests and the corpus content ID.

After the gate passes for a new untouched holdout, run three identical blinded
repetitions. Report extraction, grounding, acceptance, and deterministic
comparison separately. Do not create a final evaluation report until all three
receipts validate.
