# Decision: Carry document segments as an internal generation ledger

- **Date:** 2026-07-24
- **Status:** Accepted for implementation
- **Scope:** Spicy Regs ontology tagging and validation
- **Goal:** `2026-07-24-production-document-segmentation-agent-goal.md`

## Decision

Spicy Regs will keep source artifacts, source elements, processing segments,
prompt context, and concept assignments as separate records.

Each ontology generation will contain an immutable
`ontology_segment_ledger.parquet` artifact. The generation manifest and receipt
will cover this ledger, but public table discovery and Rulespec mappings will
not advertise it. A segment is an operational processing unit, not a source
document or ontology entity.

The ledger will contain one row for every selected segment version. It will
record successful tags, successful zero-tag results, rejected ungrounded
output, validation results, retry-exhausted failures, and explicit non-content
exclusions. Checkpoints remain mutable local recovery aids; the ledger is the
immutable account of the completed generation.

## Source and evidence

Subject adapters will preserve raw field text. They may create separate,
deterministic prompt context, but they will not collapse whitespace or truncate
evidence-bearing text.

Every accepted quote will carry:

- the artifact type, source ID, profile, source table, and artifact digest;
- the source-element ID and kind;
- the source field and exact start and end character offsets;
- the segment ID and segmentation-policy version; and
- the exact source text selected by those offsets.

Grounding will resolve the quote inside the declared segment and translate its
local offsets to artifact-field offsets. Context fields cannot satisfy this
check.

## Token and context policy

The canonical policy is `source-aware-o200k-v1`. It uses the pinned
`o200k_base` encoding from `tiktoken` and a 1,200-token leaf-text budget.
Experiments will also evaluate 800 and 1,800 tokens. Prompt assembly will
reserve separate budgets for instructions, candidate concepts, deterministic
artifact context, structured output, and a safety margin.

Native source structure sets the preferred boundaries. Paragraph, line,
sentence, word, and hard splits are deterministic fallbacks. Canonical source
coverage is non-overlapping. An experimental arm may add overlap for an
oversized element, but duplicated overlap text will retain one canonical
source-span identity.

The default context consists of deterministic artifact-title and ancestor-
heading fields. Parent or neighboring expansion remains non-evidentiary.
Model-generated contextual prefixes are disabled in production; an experiment
may enable them only as derived data with model provenance.

## Adapter boundary

All source adapters map into a small internal contract:

```text
Artifact
  identity + version digest + profile
  Element[]
    identity + kind + ordinal + parent path
    source field + exact coordinates + raw text
```

Structured API and JSON fields remain native elements. HTML, XML, PDF, DOCX,
and image parsers may supply hierarchy, but parser-specific objects do not
cross this boundary.

The seventeen current profiles declare one of three policies:

- `atomic-record`: docket, Federal Register metadata, Unified Agenda
  observation, SAM entity, FEC committee, court docket, USAspending recipient,
  and FCC proceeding;
- `structured-children`: lobbying filing and future repeated source arrays;
- `hierarchical-document`: Regulations.gov document or comment, CFR section,
  congressional bill, GAO report, CRS report, court opinion package, and FCC
  filing.

The court-opinion adapter preserves one official Supreme Court PDF package as
one artifact. Segmentation may use recovered headings and paragraphs, but the
carrier does not infer separate lead, concurrence, or dissent identities from
layout alone.

Metadata-only rows still use their declared policy. Real full-text fixtures
exercise the hierarchical adapter for each document family.

## Assignment aggregation

Model proposals belong first to a segment result. The pipeline groups accepted
proposals by artifact version and resolved concept ID, then writes one current
artifact-level assignment per group.

Aggregation will:

1. deduplicate spans by source-element ID and exact coordinates;
2. retain every distinct accepted span and contributing segment ID;
3. use the maximum accepted proposal confidence as the assignment confidence;
4. retain proposal, rejection, disagreement, and validation provenance in the
   ledger;
5. deduplicate candidate concepts by normalized label, scheme, and external
   identifier before minting; and
6. derive assignment IDs from the artifact version, concept, evidence-set
   digest, run, and supersession lineage.

A zero-tag segment cannot remove another segment's support. A policy or source
change creates new segment versions. Existing append-only assignments remain
auditable and may be superseded after the new artifact version completes.

Validation runs once per distinct accepted span. An artifact-level assignment
passes when at least one span agrees; disagreed spans remain in the ledger and
do not count as accepted evidence. If no span agrees, the pipeline appends a
lower-confidence superseding assertion with the validation record.

## Why this carrier

A public `document_segments` table would invite consumers to treat processing
views as source entities and would enlarge the public contract without a
demonstrated query need. Evidence embedded only in assignments cannot account
for zero-tag segments or safe resume after interruption. The internal immutable
ledger supports both requirements while artifact-level assignments remain the
public query surface.

## Consequences

- The materialized-dataset framework needs explicit internal generation
  artifacts covered by manifests, uploads, and receipts but omitted from public
  table discovery.
- Legacy artifact-only checkpoint keys require a migration boundary; new keys
  include segment ID and artifact digest.
- `truncated_fields` becomes migration-only evidence for old assertions.
- Rulespec keeps shared artifact and assertion semantics. Spicy Regs keeps
  tokenizer, segment, prompt-context, and processing-ledger details local.
