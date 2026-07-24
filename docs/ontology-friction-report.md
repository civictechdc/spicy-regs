# Rulespec full-corpus consumer report

This report records the 2026-07-23 local consumer run of the metadata and
rulemaking layers against the complete public Spicy Regs Parquet snapshots. No
objects were uploaded or otherwise published by the exercise.

!!! warning "Historical RIN interpretation"

    The measurements below are preserved as the initial consumer evidence, but
    the recommendation that an ordinary RIN may identify a Proceeding is
    superseded. The [RIN ontology revision report](rin-ontology-revision-report.md)
    models every RIN uniformly as a durable Regulatory Agenda item and relates
    it to independently identified Proceedings through qualified,
    provenance-bearing assertions.

## Corpus and outputs

| Input | Rows |
| --- | ---: |
| `dockets` | 276,326 |
| `documents` | 1,987,864 |
| `federal_register` | 799,759 |
| `unified_agenda` | 3,954 |
| `fr_docket_links` | 715,080 |

| Derived table | Rows | Key result |
| --- | ---: | --- |
| `rule_targets` | 334,989 | 161,585 distinct dockets |
| `authority_edges` | 10,618 | 903 failed parses retained |
| `proceedings` | 334,753 | 21,007 explicitly connected multi-docket components |
| `comment_periods` | 278,817 | 150,319 proceedings; zero inverted output intervals |

All four outputs used their declared all-VARCHAR schemas and complete
deterministic attestation blocks. The Rulespec L0 mapping audit passed the
version-pinned mapping block in `docs/ontology.md` against contract digest
`sha256:836968b28f3b86283f53c57ae5c9ab8ebd77e96531cd4751476f1a5ee3d296f2`.
That audit executes identifier examples and checks every claimed predicate's
subject domain, object range, direction, and value kind.

## What the corpus changed

### A RIN is not globally unique to one proceeding

The first implementation grouped every docket carrying the same RIN. The full
corpus disproved that model: RIN `2120-AA64`, used for recurring FAA
airworthiness directives, collapsed 40,620 dockets into one proceeding.

The implemented assembler now creates connected docket components within a RIN. It
merges dockets only when one Federal Register document explicitly co-identifies
them. On the same corpus, the largest component fell from 40,620 dockets to 21.
This is the recommended Rulespec interpretation: a RIN identifies a rulemaking
series or proceeding in the ordinary case, but consumers must permit multiple
Proceedings to carry the same RIN when reuse is evidenced.

The conservative split leaves genuinely ambiguous RIN-only evidence
unattached, rather than copying it onto unrelated components:

- 4,410 Federal Register documents with a reused RIN but no unique linked
  component;
- 532 Unified Agenda entries on reused RINs;
- 1,539 authority rows on reused RINs.

The raw source and edge tables retain all of those records. A later,
independently evaluated title/time matcher may resolve them; v1 does not guess.
Every output row nevertheless has a stable, partner-scoped `proceeding_id`.
That id is generation-stable state rather than a fresh hash of the current
component anchor: the next run reuses the strongest compatible prior id by
docket overlap, and records every compatible prior id in
`identity_predecessors_json`. A backfill that introduces a lexically earlier
docket therefore does not rename the proceeding; merges and splits retain an
explicit predecessor trail. The L0 map uses the partner id for
`rkaf:Proceeding` identity and treats RINs as evidence and join keys. Docket IDs
materialize separate mutable `rkaf:Docket` resources linked through
`rkaf:hasDocket`; neither a docket nor a reused RIN is allowed to collapse
distinct proceedings.
The same rule also prevents no-RIN evidence from fanning out through a docket
that participates in more than one proceeding: 86,191 regulations.gov document
rows and 10,644 Federal Register rows were left unattached rather than copied
across every candidate component.

### The `us-frdoc` lexical space needs a normative fallback

Rulespec currently defines Federal Register document numbers as
`YYYY-NNNNN`. Only 450,924 of 799,759 corpus rows match that form. The remaining
348,835 (43.6%) are official identifiers too:

| Form | Rows | Example |
| --- | ---: | --- |
| Two-digit-year legacy | 198,887 | `05-23965` |
| `E` legacy | 119,388 | `E7-21559` |
| Corrections | 1,203 | `C1-2026-13078` |
| Other official legacy/correction forms | 29,357 | source-preserved |

Rulespec retains the narrow normalized grammar and now defines the alternative
recommended here: every form uses the permanent Federal Register document URL
as immutable Artifact identity, while only `YYYY-NNNNN` values may also claim
`rkaf:us-frdoc`. Spicy Regs' inverse `publishedInProceeding` mapping constructs
the permanent URL for every form and does not make a false regulatory-scheme
claim for legacy or correction values.

### Comment dates need quarantine

The run found 7,178 candidate intervals whose close date preceded their
available opening proxy: 6,807 from regulations.gov documents and 371 from
Federal Register rows. They are counted, logged with bounded examples, and
excluded. Another 8,297 document intervals and 5,966 FR intervals had no unique
proceeding component and were also excluded. The local candidate output
contained no inverted interval. No regulations.gov evidence id was assigned to
more than one proceeding; 466 Federal Register evidence ids remained
many-to-many only where their explicit RIN/docket metadata supported that
relationship.

This validates separate `CommentPeriod` entities and source attribution. A
single unqualified deadline property cannot represent extensions, reopenings,
source disagreement, or an invalid historical date pair safely.

Rulespec now requires `prov:wasDerivedFrom` on every CommentPeriod. The Spicy
Regs L0 map expands `comment_periods.evidence_ids_json` to `prov:Entity` IRIs;
the adjacent `source` value continues to qualify whether those identifiers
came from regulations.gov, the Federal Register, or both.

### Unknown stage is not prerule

The corpus contains 204,831 proceedings with no stage event in the available
metadata. Their `current_stage` remains null; absence of evidence is not a
`prerule` assertion. Similarly, 628 Unified Agenda rows use the category
`Completed Actions`, which does not by itself mean `withdrawn`. The stage mapper
therefore requires an explicit stage signal instead of converting that category
to a lifecycle state.

Rulespec now makes `rkaf:proceedingStage` optional and defines absence as
unknown. It explicitly forbids inferring prerule, withdrawn, or any other stage
from missing evidence.

### Authority parsing is useful but intentionally incomplete

Of 10,618 authority edges, 8,578 were parsed U.S.C. citations and 756 were
Public Law citations. The 903 retained failures (8.5%) are dominated by honest
non-citations such as `Not Yet Determined` and `...`, plus unsupported chapter
forms and CFR delegations. Keeping `authority_raw` and `parse_status` separate
from the clean rule spine proved necessary.

The corpus also lacks edition metadata on its compact CFR and authority
rollups. The L0 map therefore exposes their regulatory identifiers but does not
claim that `cfr_refs_json` identifies immutable `rkaf:Artifact` targets or that
`authority_refs_json` directly materializes `rkaf:Authority` resources. Those
relationships require a later resolver to bind each citation to a specific
edition.

## Contract resolution

The flat Level-0 carrier mapping and the experimental `Proceeding`,
`proceedingStage`, `CommentPeriod`, and `publishedInProceeding` terms survived
the corpus exercise. The findings have these contract dispositions:

| Finding | Resolution |
| --- | --- |
| Reused RINs cannot establish global identity | Incorporated: stable partner-scoped proceeding identity is normative, and a RIN uniqueness constraint is forbidden. |
| Official Federal Register numbers exceed `YYYY-NNNNN` | Incorporated: nonmatching values use permanent publication URLs and MUST NOT claim `rkaf:us-frdoc`. |
| Comment windows require qualified evidence | Incorporated: CommentPeriod requires one or more `prov:wasDerivedFrom` entities, and the carrier mapping preserves the evidence identifiers. |
| Missing stage evidence is not prerule | Incorporated: `rkaf:proceedingStage` is optional, and absence means unknown. |
| Independent consumer review | Open: the rulemaking module remains Experimental until a non-originating consumer reviews the component and ambiguity rules above. |
