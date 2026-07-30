<!-- markdownlint-disable MD013 -->

# SpicyRegs Experiment Strategy

- **Date:** 2026-07-29
- **Status:** Working decision guide
- **Purpose:** Test whether SpicyRegs helps people find, filter, connect, and
  aggregate regulatory records. Choose tools only after identifying the user
  question and the failing stage.
- **Authority:** the
  [RefSpec managed vocabulary roadmap](../RefSpec/plans/managed-vocabulary-experiment-roadmap.md)
  controls vocabulary experiments; [`decisions.md`](decisions.md) records
  project decisions. The earlier
  [`rulespec-testbed-path-forward.md`](rulespec-testbed-path-forward.md) is a
  historical record of the fused-registry MVP.

## Decision

SpicyRegs will judge experiments by their effect on useful, explainable query
results. A component benchmark may diagnose a problem, but it cannot establish
product success by itself.

The program will follow this order:

1. Define the user question and the expected result.
2. Trace the question through the data and processing steps.
3. Identify the step that limits the result.
4. Choose the simplest credible tool for that step.
5. Compare it with a fixed baseline while changing one variable.
6. Adopt it only when the complete user result improves.

BM25, dense retrieval, language models, rerankers, and graph engines are
options. None is the program's goal or default center of attention.

## Two lanes

SpicyRegs uses two lanes so an ambitious experiment does not inherit
production-release ceremony before it has taught us anything.

### Experiment lane

Use this lane for development-only comparisons of parsers, expressions,
indexes, retrieval channels, fusion, reranking, prompts, models, and provider
settings. The run may exercise the complete data model and real managed
vocabulary release, but it cannot authorize accepted output or deployment.

The runner creates the evidence that used to be assembled by hand:

- `experiment.json` pins the managed release, expression corpus, lookup index,
  code, managed target set, configuration, and evaluation boundary;
- `candidates.parquet` preserves candidate and expression lineage;
- `metrics.json` keeps availability, retrieval, assignment, and product
  measures separate; and
- `decision.md` records a development decision to continue, investigate, or
  stop.

Focused tests are the normal gate. A failed or incomplete experiment remains
useful evidence and does not require a release record.

The active lookup dataset reuses the 35 pinned source artifacts and evidence
spans from the earlier experiment, but none of its fused-registry identifiers
or verdicts. Each represented answer names an exact member of one pinned
RefSpec release and carries a directional grade. `notRepresented` is an
explicit outcome and stays outside reachable-candidate recall. These targets
were prepared with candidate runs visible, so they support development
comparisons only; independent review is deferred until promotion.

### Promotion lane

Enter this lane only when a result will change accepted output, an externally
used interface, a published claim, or a stable deployed configuration.
Promotion adds the applicable RefSpec and Rulespec schemas, permission rows,
independent evaluation, reviewed deployment decision, cross-repository
validation, and migration evidence. An automatically generated
`developmentOnly` candidate selection used to open a local managed release
does not by itself enter this lane.

The promotion lane consumes an experiment directory; it does not ask the
experimenter to reconstruct the run. The active division of responsibility is
defined in the
[managed vocabulary experiment roadmap](../RefSpec/plans/managed-vocabulary-experiment-roadmap.md).

## Product goal

SpicyRegs contains many kinds of public-sector data with different identities
and levels of detail. It should expose an evidence-backed metadata graph that
lets people:

- find likely relevant records;
- filter records by known metadata and relationships;
- connect records across sources;
- aggregate records at a declared level, such as document, docket, or
  proceeding; and
- understand why each record or relationship appears.

The graph is a set of typed records and relationships. It does not require a
graph database. Parquet and DuckDB remain appropriate until a real, measured
query shows that they cannot serve the need.

The system must preserve each source's natural level of detail. A docket,
document, comment, proceeding, agenda item, regulation, statute, and concept
must not become interchangeable generic "documents." Stable identities and
explicit relationship types make filtering and aggregation trustworthy.

## What goes in, what happens, and what comes out

### What goes in

- Immutable source records and source metadata.
- Stable artifact identities and versions.
- Text fragments tied to exact locations in the source.
- Deterministic identifiers and citations where available.

### What happens

1. Deterministic processing normalizes identifiers, citations, dates, and
   source relationships.
2. The system creates high-trust links for facts that rules can establish.
3. Retrieval methods may reduce a large concept or document collection to a
   manageable candidate set.
4. A language model may judge semantic questions that deterministic rules
   cannot answer reliably.
5. Every derived tag or relationship retains its evidence, method, version,
   and review state (review state carried as attestations in a separate
   table, never as fields on the record — `rkaf-core.md` §4.7.3).

### What comes out

- Stable identity tables.
- Deterministic relationships, such as a document targeting a CFR section.
- Evidence-backed concept assignments.
- Query views that support finding, filtering, connecting, and aggregating.
- Explanations that show why each result was included.

### How we check it

We test each layer separately, then test the complete user query. A lower-level
test explains failures; only the complete query establishes user value.

## Evaluation hierarchy

### 1. Discovery experiments

These experiments ask whether a user receives the right records and counts.
They are the product-level tests.

Examples:

- Which dockets touch `40 CFR 60`?
- Which active rulemakings depend on `42 U.S.C. 7401`?
- Which dockets and documents concern PFAS?
- How many qualifying proceedings did each agency have in a given year?

### 2. Graph experiments

These experiments test stable identities and relationships:

- Does each link join the correct records?
- Is its direction correct?
- Does source evidence support it?
- Does it avoid accidental many-to-many fan-out?
- Does it remain stable across a source refresh?

### 3. Tagging experiments

These experiments test whether the system assigns concepts correctly:

- Is the concept relevant to the record?
- Is it a primary topic, a substantive secondary topic, or a mention?
- Does the cited text support the assignment?
- Can the system assign multiple valid concepts?
- Can it propose a local concept or abstain instead of forcing a poor match?
- Does the assignment include its method, model or ruleset version, confidence,
  and review state?

### 4. Component experiments

These experiments test individual steps such as segmentation, candidate
generation, fusion, reranking, prompting, or model choice.

A candidate-selection experiment may ask whether a usable concept appears in
the top 12 candidates. That result describes the tagger's opportunity to
succeed. It does not show that the tagger chose the concept, that the evidence
supports the assignment, or that a user query returned the right records.

Component experiments should run only when evidence identifies that component
as a likely constraint.

## Match each capability to the right measure

| Capability | Promise to the user | Main measures |
| --- | --- | --- |
| Identity | Records refer to the right real-world object and version. | Exact identifier matches, version continuity, unresolved-rate reporting. |
| Relationships | Links are correct and supported. | Link precision and recall, direction checks, evidence checks, fan-out checks. |
| Tagging | Concepts describe the right records for the right reasons. | Role-aware, multi-label precision and recall; evidence support; calibration; abstention. |
| Find | Relevant records appear near the top. | Recall and precision at useful result depths, ranking quality, source diversity. |
| Filter | Every result satisfies the selected condition. | Predicate precision and recall, unknown-value behavior, stable result membership. |
| Aggregate | Counts and groups reflect the declared level of detail. | Expected counts, distinct-ID checks, duplicate and fan-out checks. |
| Explain | A person can inspect why a result appeared. | Evidence coverage, relationship path correctness, method and version completeness. |
| Operate | Another run can reproduce and inspect the result. | Frozen inputs, configuration hashes, receipts, latency, failure reporting. |

One score cannot represent all these capabilities. Reports must keep them
separate.

## Choose tools for the work

### Deterministic code

Use parsers, normalizers, and explicit joins for known identifiers, citations,
dates, agencies, and source relationships. These tools are cheap, inspectable,
and exact when the source supplies enough information.

### DuckDB and SQL

Use SQL for filtering, relationship traversal, aggregation, and saved query
examples. These operations should not depend on a language model.

### Lexical, dense, and hybrid retrieval

Use retrieval when a large search space must become a small candidate set.
Choose lexical, dense, hybrid, or another method through a fair comparison on
the same inputs and filters. The best method may differ by task:

- user query to document or fragment;
- document fragment to registered concept;
- concept to related records; or
- evidence lookup within an already selected record.

Results from one task do not establish performance on another.

### Language models

Use language models for semantic judgments that rules cannot make reliably.
Constrain their inputs, require exact source evidence, allow abstention, and
record model and prompt versions. A model should not resolve an exact
identifier when deterministic source data can do so.

### Human review

Use independent human judgment to define ambiguous meaning, adjudicate
holdouts, approve high-impact changes, and correct the system. Human review
must allow multiple valid topics and distinguish a primary topic from a
secondary topic or mention. Until the wiki interface exists there is no
standing human review capacity: this role is filled by cross-family machine
adjudication, honestly labeled as such (`decisions.md`, machine-first
attestation).

### Rulespec and provenance records

Use Rulespec-compatible structures to record identities, assignments,
evidence, lineage, and review state. These records make results inspectable;
they do not replace accuracy or usefulness tests.

### Graph engines

Add a dedicated graph engine only after a real query exceeds DuckDB's measured
capabilities. Engine comparisons without such a query do not answer a product
question.

## Minimum experiment start card

Before a material experiment runs, state five things:

1. the user or engineering decision the result could change;
2. the processing step that appears to limit that result, with the evidence
   that points to it;
3. the baseline and the one intended change;
4. whether the data are development-only or eligible for an independent
   decision; and
5. the result that means continue, investigate, or stop.

The runner, not the author, records input and output shapes, exact release and
artifact digests, metric versions, candidate lineage, timings, and the output
directory. A run refuses an adoption or accepted-output claim when its
evaluation boundary is development-only.

For a component experiment, the start card must also name the later end-to-end
check. If that check is not yet measurable, record the gap instead of treating
a component proxy as product success.

## Evaluation data

The original 35 tagging items are permanently development-only. They remain
useful for debugging and regression checks but cannot support an accuracy or
adoption decision.

A new decision-quality holdout must:

- pin membership, artifact digests, and source/selection digests at draw
  time; expose labels only after the evaluated configuration (including
  `registry_sha256`, prompt, schema, and token budget) freezes — the exit
  bar's trivial baselines are computed on the holdout, so "freeze before
  labels," not "freeze before drawing"; one-shot: once used for a decision,
  items move to development (per the executable boundary);
- include several relevant source families;
- bind each item to immutable artifact identities, versions, source fragments,
  and hashes;
- keep holdout concepts, aliases, and artifact content separate from
  development data;
- include related, unrelated, ambiguous, explicit-denial, multi-topic, and
  hard-negative examples;
- allow multiple labels and assignment roles;
- receive blind adjudication by at least two independent model families
  (gate-enforced), disagreements resolved by a third family or excluded; and
- record expected, forbidden, and ambiguous results.

The source-family diversity, hard-negative composition, and forbidden-result
requirements above are this document's requirements; the executable gate does
not yet enforce them — check them by hand until it does.

Mixed-source coverage must follow user questions. The first evaluation need not
include every table merely because the data exist.

## Initial discovery slice

The first slice should answer three existing design questions:

| Question | Main path | Test emphasis |
| --- | --- | --- |
| Every docket touching `40 CFR 60` | Docket or document → `rule_targets` → CFR section | Deterministic identity, link correctness, complete filtering, exact counts. |
| Every active rulemaking depending on `42 U.S.C. 7401` | Agenda item → `authority_edges` (RIN) → `agenda_item_proceedings` → proceeding | Authority evidence, proceeding identity, a recorded active-state definition (none exists yet — must be defined and recorded first), no RIN fan-out. |
| Every docket or document about PFAS | Docket or document → `concept_assignments` → concept | End-to-end tagging, evidence, multi-label handling, query precision and recall. **Blocked on MVP phase 4** (docpipeline→publication bridge, attestations table, adopted selector). |

For each question, freeze:

- the source snapshot;
- the query and intended meaning;
- expected record identifiers;
- forbidden record identifiers;
- ambiguous records;
- expected aggregate counts; and
- the evidence that supports each expected result.

One recall boundary to freeze into the expected counts: agenda-only CFR
references live in `unified_agenda.cfr_references_json` and are deliberately
excluded from `rule_targets` (never projected through RIN equality), so
"every docket touching 40 CFR 60" has a declared recall boundary, not
perfect coverage.

Run the two deterministic questions first. They test the identity spine and can
deliver useful discovery without waiting for model-based tagging. Then run the
PFAS question through segmentation, candidate generation, semantic judgment,
assignment creation, and the final query.

## BM25 as an example, not a program

The current BM25 experiment tests a long document fragment against short
concept labels and aliases. It establishes how that configuration behaves as a
concept candidate generator on development data.

Conventional document search asks a different question: a short user query
against document fragments. The existing result does not establish BM25's
fitness for that task. If document search becomes the identified constraint,
BM25 may serve as one baseline beside dense and hybrid methods.

This same rule applies to every tool: state the task, input direction, corpus,
filters, and decision before interpreting the score.

## Next sequence

1. Replace the planned retrieval-only evaluation document with one discovery
   evaluation plan covering identity, relationships, tagging, retrieval,
   filtering, aggregation, and explanations.
2. Freeze the initial questions, snapshots, expected results, and untouched
   holdout.
3. Validate the deterministic regulation and authority paths.
4. Run the PFAS path from source text through the final query.
5. Locate the measured failure before comparing component alternatives.
6. Adopt a component only when it improves the complete query without
   weakening evidence, reproducibility, latency, or cost.
7. Expose successful questions through saved SQL or existing MCP access before
   adding a new serving system.

## Stop rules

- Do not run a tool comparison without a decision it can change.
- Do not treat a development-set result as adoption evidence.
- Do not call candidate recall tagging accuracy.
- Do not call tagging accuracy product success.
- Do not tune a model while upstream data, registry, or evaluation failures
  make model quality unmeasurable.
- Do not flatten different record types to simplify a benchmark.
- Do not add storage or workflow infrastructure without a measured need.
- Do not adopt a component gain until the complete user result improves.

## Current status

As of 2026-07-29:

- The metadata and ontology design already defines the initial regulation,
  statute, and concept questions.
- The Rulespec testbed defines and has partially validated a narrower path
  from source records through evidence-backed concept assignments and atomic
  publication; MVP phase 4 (the docpipeline→publication bridge and
  attestations) is not built. Retrieval serving remains outside that MVP.
- The selector harness tests whether a usable registered concept reaches a
  fixed candidate list. It does not test the final tag or user query.
- The active managed-release development set no longer uses fused-registry
  identifiers or verdicts. Complete dense evidence windows remove hidden
  512-token truncation and improve the dense-only arm, but the current
  equal-weight fusion loses that gain. Query construction and fusion remain
  separate component experiments.
- Local changes now separate semantic facets from source vocabularies and
  enforce a frozen development boundary.
- No untouched, independently adjudicated holdout exists. Accuracy and
  adoption claims remain blocked.
- Docpipeline assignment publication and query-level discovery acceptance
  remain unfinished (the deterministic relationship spine is published).

## Related documents

- [Metadata and ontology layer design](superpowers/specs/2026-07-23-metadata-ontology-layer-design.md)
- [Rulespec MVP path](rulespec-testbed-path-forward.md)
- [Failure analysis](evidence/failure-analysis-2026-07-27.md)
- [Candidate-selection research](evidence/candidate-selection-research-2026-07-27.md)
- [Architecture and experiment decisions](decisions.md)
- [Rulespec work plan](../TODO-RULE.md)
