# Relationship Assertion Release and Migration

- **Date:** 2026-07-25
- **Status:** Planned; local candidates are not released
- **Repositories:** Rulespec and Spicy Regs
- **Change class:** Paired semantic-contract migration

## Current state

Rulespec contains a local Experimental `RelationshipAssertion` candidate with
required subject, predicate, object, polarity, and origin fields. Its generated
targets and fixtures are also local changes.

Spicy Regs contains an Experimental Python projection and deterministic
comparison kernel. It is not part of the published L0 carrier, and the current
Rulespec pin does not claim this new contract.

The v1 OpenAI run is diagnostic evidence. The v2 corpus and protocol exist, but
human review and the three-run comparison remain incomplete. Longitudinal
omission remains design-only and disabled.

## Migration principles

- Release Rulespec before Spicy Regs publishes data under the new contract.
- Keep CUE authoritative and regenerate every derived target.
- Commit the repositories separately and cross-reference the same task.
- Append new semantic records; do not rewrite existing concept assignments or
  historical provider evidence.
- Keep provider SDKs behind Spicy Regs adapters.
- Fail closed when a release, digest, proof record, or migration gate is
  missing.

## Phase 1: release the Rulespec contract

1. Reconcile normative prose, vocabulary, CUE, context, SHACL, generated SDK
   types, and runtime behavior.
2. Keep assertion content separate from attestations, evidence, confidence,
   applicability, warrants, and AI lineage.
3. Add positive, required-field negative, invalid-polarity, and AI-lineage
   fixtures.
4. Run the canonical compile, parity, fixture, runtime, and conformance gates.
5. Obtain the required non-originating review.
6. Cut a reachable versioned release and record its immutable contract digest.

Do not release generic closure or deontic terms in this phase unless another
consumer and complete fixtures justify them. The first release needs only the
stable relationship-assertion seam.

## Phase 2: pin and project in Spicy Regs

1. Update the Rulespec version and digest in the application profile and L0
   declaration.
2. Map `RelationAssertion` to the released contract without adding shadow
   vocabulary.
3. Preserve separate records for evidence bindings, confidence, AI lineage,
   and attestations.
4. Bind comparison contexts and findings to immutable generation and proof
   records.
5. Regenerate the Spicy Regs self-certification and run the Rulespec carrier
   audit.
6. Run focused relation tests, the full test suite, lint, and secret scans.

The physical carrier may use specialized Parquet tables. It does not need to
flatten all records into one universal assertion table.

## Phase 3: backfill safely

Backfill only assertions that can be reconstructed from immutable source
artifacts and validated evidence spans.

- Give the backfill a versioned run and receipt.
- Preserve the original source and extraction result.
- Resolve attestations separately from extraction.
- Quarantine unresolved identity, scope, evidence, and warrant cases.
- Publish one atomic generation with counts for accepted, rejected, unknown,
  and quarantined records.
- Do not manufacture denied assertions from nulls or missing edges.

Existing v1 candidates remain diagnostic artifacts. They do not become
production assertions merely because the migration introduces a compatible
schema.

## Phase 4: enable comparison by profile

Start with explicit affirmation-versus-denial comparison:

1. install production predicate, state, evidence, baseline, pairing, and scope
   resolvers;
2. require dereferenceable proof records;
3. evaluate the frozen v2 corpus after human adjudication;
4. run three identical blinded OpenAI repetitions; and
5. publish separate extraction, acceptance, and comparison metrics.

Enable longitudinal omission only after a profile independently proves closure
accuracy. Enable deontic interpretation only after a domain profile proves its
authority and interpretation rules.

## Compatibility and rollback

The migration is additive until consumers explicitly opt into the new profile.
Old generations remain readable under their pinned Rulespec version.

Rollback means:

- stop publishing new relationship generations;
- restore the prior latest-generation pointer;
- retain all candidate, receipt, quarantine, and review artifacts;
- leave the Rulespec release intact if its contract is sound; and
- supersede faulty assertions or interpretations in a later generation.

Never delete or silently rewrite published assertions to simulate rollback.

## Release gate

The paired migration is complete when:

- Rulespec has a reachable reviewed release and reproducible digest;
- Spicy Regs pins and audits that exact release;
- every published assertion and finding resolves its evidence and proofs;
- full repository gates pass from clean checkouts;
- an atomic candidate generation can be reproduced;
- rollback has been exercised against that candidate; and
- documentation distinguishes released, Experimental, and disabled features.
