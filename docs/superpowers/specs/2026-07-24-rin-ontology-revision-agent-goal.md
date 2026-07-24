# Agent Goal: Correct RIN Semantics Across Rulespec and Spicy Regs

- **Date:** 2026-07-24
- **Status:** Implemented and validated locally
- **Repositories:** `Formspec-Labs/rulespec` and `civictechdc/spicy-regs`

## Goal

Design, implement, and validate a cross-repository model in which a Regulation
Identifier Number (RIN) identifies a durable regulatory agenda item rather than
serving as an unconditional global identifier for one rulemaking proceeding.
Relate that agenda item to zero, one, or many proceedings through explicit,
provenance-bearing assertions.

The finished system must represent ordinary one-action RINs, intentional
“Routine and Frequent” umbrella RINs, and unresolved repeated RINs without
collapsing distinct proceedings or copying agenda-level facts onto child
proceedings.

Do not stop after renaming fields or updating documentation. Complete the
Rulespec contract, Spicy Regs carrier model and transforms, fixtures, full-corpus
exercise, migration notes, generated artifacts, and repository gates.

## Why this work is necessary

Rulespec currently permits `rkaf:us-rin` to establish `rkaf:Proceeding`
identity. Its reuse rule tells producers to assign partner identifiers to split
proceedings and retain the RIN as non-identity evidence. This prevents a false
merge, but it leaves the RIN's actual referent unmodeled.

Spicy Regs implements the safe half of that contract. It creates separate
proceeding components and refuses to fan Unified Agenda or authority evidence
across several components. That choice preserves truth, but it strands useful
agenda-level context.

Official records establish that at least some repetition is intentional:

- [Coast Guard RIN 1625-AA00](https://www.reginfo.gov/public/do/eAgendaViewRule?RIN=1625-AA00&pubId=202510)
  covers about 550 safety-zone actions each year and explicitly says that
  actions issued under the RIN receive individual docket numbers.
- [FAA RIN 2120-AA64](https://www.reginfo.gov/public/do/eAgendaViewRule?RIN=2120-AA64&pubId=202510)
  covers about 550 airworthiness-directive actions.
- [Reginfo's RIN guidance](https://www.reginfo.gov/public/jsp/eAgenda/StaticContent/UA_HowTo.jsp)
  describes a RIN as the unique key for a Unified Agenda entry and as a way to
  follow the regulatory action or proceeding represented by that entry.

The current repaired candidate snapshot also shows that the modeling pressure
is not a corner case:

- 242 RINs occur on more than one proceeding component.
- Those RINs cover 1,094 proceeding components.
- Eight RINs explicitly classified as “Routine and Frequent” cover 304
  components.
- 143 Unified Agenda rows and 458 parsed authority rows share RINs with multiple
  components and therefore cannot attach safely to a child proceeding.

Recompute these figures from the live corpus. Treat them as a baseline, not
fixed acceptance values.

## Starting authority

Before changing either repository:

1. Read every applicable `AGENTS.md`.
2. Inspect both worktrees and preserve all user changes.
3. Read the live versions of:
   - Rulespec `spec/rkaf-rulemaking.md`;
   - Rulespec `constraints/core/rulemaking.cue`;
   - Rulespec `spec/rkaf-vocabulary.md`;
   - Rulespec rulemaking fixtures and L0 semantic-range declarations;
   - Spicy Regs `src/spicy_regs/transforms/build_proceedings.py`;
   - Spicy Regs `docs/rulespec-profile.md`;
   - Spicy Regs `docs/ontology-friction-report.md`;
   - Spicy Regs `docs/rulespec-repair-report.md`;
   - Spicy Regs `conformance/rulespec-l0.yaml`;
   - Spicy Regs table schemas, ontology receipt code, and materialization
     pipeline.
4. Recheck the official records above and inspect current examples of ordinary,
   recurring, and ambiguous RINs.

The worktrees and official sources supersede this handoff when they provide
newer evidence.

## Required semantic result

The implementation must preserve these invariants:

1. **A RIN never merges proceedings by itself.** Shared RIN text is evidence of
   a relationship, not proof of proceeding identity.
2. **A durable agenda item owns the RIN.** Each Unified Agenda edition contains
   an observation or source artifact describing that item.
3. **Agenda items and proceedings remain distinct.** An explicit relationship
   connects them, even in the ordinary one-item/one-proceeding case.
4. **Cardinality reflects evidence.** One agenda item may cover one proceeding,
   a recurring family of proceedings, or no resolved proceedings.
5. **Multiplicity does not prove recurrence.** Several components sharing a RIN
   may indicate an intentional series, a joint action, incomplete linkage, bad
   source data, or unresolved identity.
6. **Agenda state is not proceeding state.** An umbrella item's “Final Rule
   Stage” or “Long-Term Actions” value must not become the stage of every child
   proceeding.
7. **Context is not inherited silently.** Agenda-level titles, CFR references,
   legal-authority citations, timetables, and priority values remain on the
   agenda item or its editioned observation unless action-specific evidence
   supports a child-level assertion.
8. **Source grain remains visible.** Keep a durable agenda item separate from
   each editioned Unified Agenda record, docket, Federal Register publication,
   and proceeding.
9. **Every derived relationship carries provenance.** Preserve its source,
   evidence identifier, method, actor or ruleset, run, assertion time, and
   supersession state where the carrier supports them.
10. **Existing stable proceeding identifiers survive.** Migrate them only when
    new source evidence proves a merge, split, or identity correction.

The preferred conceptual shape is:

```text
RIN
 └── identifies ──> RegulatoryAgendaItem
                         ├── observed in ──> UnifiedAgendaEntry (edition Artifact)
                         ├── covers ──> Proceeding A ──> Docket / publications
                         ├── covers ──> Proceeding B ──> Docket / publications
                         └── covers ──> Proceeding C ──> Docket / publications
```

`RegulatoryAgendaItem`, `covers`, and related names are provisional. Preserve
the semantics if a composition audit finds better terms in an established
public ontology.

## Design decision gate

Before implementation, write a short decision record that compares at least
these shapes:

1. A first-class regulatory agenda item for every RIN, with an optional
   recurring-series classification.
2. A `RulemakingSeries` node only for proven umbrella RINs while ordinary RINs
   continue to identify proceedings directly.
3. A qualified identifier-assignment assertion that relates one RIN to one or
   more agenda items, series, or proceedings.

Evaluate each shape against:

- uniform RIN meaning;
- ordinary and recurring cases;
- unresolved evidence;
- editioned Unified Agenda history;
- agenda-stage versus proceeding-stage separation;
- authority and CFR provenance;
- OIRA review and meeting joins;
- query simplicity;
- migration cost;
- compatibility with Rulespec's composition discipline.

Prefer the first-class agenda-item model unless the comparison demonstrates a
smaller model that satisfies every invariant. Search existing public ontologies
before minting shared Rulespec vocabulary. Record why any imported, aligned, or
new terms fit their declared domains, ranges, and direction.

The term names may change at this gate. The semantic invariants may not.

## Rulespec work

Update Rulespec's Experimental US rulemaking module and every generated or
audited surface that depends on it.

At minimum:

1. Add the selected agenda-item entity and its RIN identity contract.
2. Add directional, cardinality-safe relationships between agenda items,
   editioned source artifacts, and proceedings.
3. Remove or narrowly qualify the claim that a RIN alone establishes
   `Proceeding` identity. Document compatibility behavior for existing
   producers.
4. Keep `rkaf:Proceeding`, `rkaf:Docket`, and `rkaf:Artifact` distinct.
5. Model agenda-level status separately from `rkaf:proceedingStage`.
6. Define how agenda-level CFR and authority evidence may be exposed without
   becoming unproved child-level assertions.
7. Update the CUE source of truth, vocabulary table, JSON-LD context, semantic
   ranges, conformance mappings, reference corpus, and narrative specification.
8. Regenerate every compiled target with `tools/compile_all.sh`; do not edit
   generated outputs by hand.
9. Keep the module Experimental. This work does not satisfy the independent
   non-originating-consumer gate.
10. Document the migration from direct RIN-as-Proceeding identity and from
    `hasProceedingEvidenceIdentifier`.

Add positive and negative fixtures for:

- one agenda item linked to one ordinary proceeding;
- `1625-AA00` linked to several distinct safety-zone proceedings;
- `2120-AA64` linked to several distinct airworthiness-directive proceedings;
- a repeated RIN with no proved series classification;
- multiple editioned observations of one agenda item;
- an agenda stage that does not set child proceeding stages;
- agenda-level authority that does not become child authority;
- rejection of a RIN-only proceeding merge;
- rejection of a malformed or wrongly typed RIN relationship;
- compatibility or migration behavior for the previous carrier shape.

## Spicy Regs work

Implement the selected contract as source-faithful Parquet tables and
provenance-bearing transforms.

The physical design must provide:

1. One durable agenda-item row per RIN.
2. Editioned Unified Agenda observations. The existing `unified_agenda` table
   may remain this surface if its grain and projection stay explicit.
3. A separate relationship table between agenda items and proceedings. Each
   asserted edge must identify its evidence and derivation method.
4. An explicit representation of unresolved scope. Do not emit a child edge
   merely because the same RIN appears on both rows.
5. Agenda-level title, stage, priority, timetable, CFR, and authority context
   that remains queryable when several proceedings share the RIN.
6. Action-specific proceeding facts derived only from action-specific evidence.
7. Stable IDs and supersession behavior consistent with other derived ontology
   tables.

The exact table and column names follow the Rulespec decision. A relationship
row will normally need the agenda-item identity or RIN, proceeding identity,
relationship kind or scope, evidence identifier, method, actor or ruleset,
run identifier, assertion time, and supersession fields.

Integrate the new surfaces with:

- ontology materialization and dependency ordering;
- snapshot manifests and receipts;
- catastrophic-shrink or completeness checks where applicable;
- data-dictionary schemas and generated table documentation;
- the MCP table allowlist and query guidance;
- `conformance/rulespec-l0.yaml`;
- Rulespec profile and friction reports;
- focused unit, integration, and full-corpus tests.

Preserve the conservative proceeding assembler. It may retain `proceedings.rin`
as a documented compatibility/evidence column, but it must not treat that
column as unique identity. Remove code that discards agenda-level information
only after the new agenda-item surface retains that information safely.

## Corpus cases

Exercise at least these three categories against current source data:

### Ordinary one-to-one candidate

Use a current RIN with one agenda item and one well-evidenced proceeding.
`0301-AA02` was a candidate in the 2026-07-24 snapshot; revalidate it before
using it as a fixture or acceptance case.

Expected result: one agenda item, one evidenced proceeding relationship, and no
identity collapse between the two nodes.

### Proven recurring family

Use `1625-AA00` and `2120-AA64`.

Expected result: one agenda item per RIN, several distinct child proceedings
with distinct docket or publication evidence, shared agenda context available
at the parent, and no automatic child inheritance.

### Repeated but unresolved

Choose a current repeated RIN without official recurring-family evidence.
`2070-AB27` was unresolved in the 2026-07-24 candidate snapshot; revalidate it.

Expected result: preserve the agenda item and RIN evidence without declaring a
series, merging proceedings, or manufacturing child relationships.

## User-facing query requirements

Prove that a consumer can answer:

1. What does this RIN represent in the Unified Agenda?
2. Which proceedings have evidence linking them to this agenda item?
3. Is the item an officially supported recurring family, an ordinary
   single-action item, or unresolved?
4. Which stage, authority, CFR, and timetable facts belong to the agenda item?
5. Which stage, authority, CFR, dockets, publications, and comment periods
   belong to a selected child proceeding?
6. Which OIRA reviews or meetings join through the RIN without pretending that
   all linked proceedings are one action?

Add documented SQL examples for the ordinary, recurring, and unresolved cases.

## Language corrections

Replace descriptions that call all same-RIN components “unrelated.” Use precise
language:

- “distinct sibling proceedings under a recurring agenda item” when recurrence
  is proved;
- “distinct proceeding components sharing RIN evidence” when the relationship
  remains unresolved.

Do not imply that a shared RIN is meaningless. Do not imply that it proves
proceeding identity.

## Non-goals

This goal does not authorize:

- merging proceedings from a RIN alone;
- classifying every repeated RIN as a series;
- building an unevaluated title, date, embedding, or LLM matcher to resolve
  ambiguous components;
- ingesting the full OIRA review or meeting-log products;
- redesigning docket, publication, comment-period, or legal-authority identity;
- promoting the Experimental rulemaking module to normative status;
- changing unrelated ontology or product surfaces;
- committing, pushing, publishing, or deploying unless the invoking user asks.

## Verification

Run the smallest focused tests during red-green development, then run the full
relevant gates.

Rulespec:

```bash
tools/compile_all.sh
make test
git diff --check
```

Spicy Regs:

```bash
uv run pytest
uv run ruff check .
uv run ty check
uv run spicy-regs-dict check
uv run --group docs mkdocs build --strict
git diff --check
```

Run a hermetic full-corpus ontology materialization with `R2_PUBLIC_URL=''`.
Validate the generated receipt, row counts, referential integrity, uniqueness,
provenance, ambiguity counters, and deterministic rerun behavior. Compare the
new snapshot with the pre-change baseline.

The final evidence must show:

- no RIN-only proceeding merges;
- no loss of Unified Agenda or authority source rows;
- agenda context retained for repeated RINs;
- no agenda-stage or authority fan-out to children;
- ordinary one-to-one queries still work;
- recurring-family queries work through the parent item;
- unresolved repetition remains unresolved;
- Rulespec generated artifacts and Spicy Regs data-dictionary artifacts are
  current;
- both repositories pass their full relevant gates.

## Definition of done

Report completion only when all of the following hold:

1. The decision record selects and justifies one semantic model.
2. Rulespec expresses that model in its source-of-truth constraints,
   specification, vocabulary, fixtures, generated targets, and migration notes.
3. Spicy Regs expresses that model in physical tables, transforms, receipts,
   profile mappings, generated documentation, and query examples.
4. The full corpus proves the ordinary, recurring, and unresolved cases without
   false merges or inherited child facts.
5. Focused and full test gates pass in both repositories.
6. The final handoff lists exact files changed, commands run, test results,
   corpus counts, remaining ambiguity, and any compatibility debt.

If an invariant cannot be met without a larger semantic change, stop and
present the evidence and alternatives. Do not silently weaken the goal.

## Execution result

The selected model is implemented in both repositories. The accepted Rulespec
decision record is
`thoughts/specs/2026-07-24-rin-agenda-item-ontology-decision.md` in the sibling
Rulespec worktree. It adopts the general `foaf:primaryTopic` document-to-subject
seam and the DCAT qualified-relation pattern, then specializes them for
Regulatory Agenda items, editioned observations, and action-specific
Proceeding relationships.

The hermetic full-corpus run produced:

- 38,005 durable agenda items;
- 120,685 provenance-bearing relationship assertions;
- 511,643 independently assembled Proceedings;
- 41 officially `Routine and Frequent` recurring items;
- 20,858 unresolved repeated items covering 67,500 Proceedings;
- zero RIN-only docket or Federal Register identity collisions;
- zero Unified Agenda stage, authority, or CFR fan-out to Proceedings.

`0301-AA02`, `1625-AA00`, `2120-AA64`, and `2070-AB27` satisfy the required
ordinary, recurring, and unresolved cases. The complete counts, SQL queries,
stable-ID migration analysis, deterministic rerun, source hashes, and receipt
are in `docs/rin-ontology-revision-report.md`.

Rulespec's full gate passes 301 fixtures with zero conformance divergences.
Spicy Regs passes 546 hermetic tests, the L0 audit, data-dictionary check,
source/test lint and type checks, strict documentation build, receipt
validation, and diff checks. The literal repository-wide Ruff and `ty check`
commands also inspect pre-existing user files under untracked `play/` (and,
for Ruff, tracked notebooks when passed as an explicit root); those unrelated
diagnostics were preserved rather than edited. No commit, push, release,
upload, or deployment was performed.
