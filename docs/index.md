# Spicy Regs Data Dictionary

This is the schema reference for the **Spicy Regs** dataset — an open mirror of
[regulations.gov](https://www.regulations.gov) federal regulatory data,
published as Apache Parquet on a public Cloudflare R2 bucket.

It documents every table supported by the current code, column by column.
Tables awaiting their first production generation are marked below. The schema
is generated directly from the code that defines and produces the data, and a
CI check fails whenever the schema and these descriptions drift apart.

## Where the data comes from

```
regulations.gov  →  Mirrulations S3 mirror  →  Spicy Regs ETL  →  Parquet on R2
                                                                   (r2.spicy-regs.dev)
```

The ETL flattens the raw regulations.gov JSON into a handful of flat tables and
publishes them, plus small pre-computed rollups, to `https://r2.spicy-regs.dev`.
Alongside them it ingests a set of **complementary federal data sources** — the
Federal Register, the Unified Agenda, Congress.gov, the CFR, SAM.gov, lobbying
disclosures, the FEC, USASpending, federal-court litigation, and GAO/CRS reports
— so the rulemaking lifecycle, the organizations that engage in it, and its
downstream context can all be queried from one place.

## The tables

Most tables below use the stable object
`https://r2.spicy-regs.dev/<name>.parquet`. The nine rule-identity and ontology
tables publish together under an immutable snapshot prefix; clients resolve
`materialized/ontology/latest.json` and use the artifact URLs in its manifest.
The MCP server (`list_sources` / `describe_table` / `query_sql`) performs that
resolution automatically and never mixes ontology generations.

### Core regulations.gov tables

| Table | Grain | Key |
| --- | --- | --- |
| [`dockets`](tables/dockets.md) | one row per docket | `docket_id` |
| [`documents`](tables/documents.md) | one row per document | `document_id` |
| [`comments`](tables/comments.md) | one row per public comment | `comment_id` |
| [`comments_index`](tables/comments_index.md) | one row per comment partition | — |

### Rollups (pre-aggregated views of the core tables)

| Table | Grain |
| --- | --- |
| [`feed_summary`](tables/feed_summary.md) | one row per docket |
| [`agency_stats`](tables/agency_stats.md) | one row per agency |
| [`agency_monthly_volume`](tables/agency_monthly_volume.md) | one row per agency / month / document type |
| [`rulemaking_lifecycles`](tables/rulemaking_lifecycles.md) | one row per measured proposal-to-final lifecycle |
| [`fr_docket_links`](tables/fr_docket_links.md) | one row per Federal Register document / regulations.gov docket link |

### Rulemaking lifecycle (external sources)

| Table | Grain | Key |
| --- | --- | --- |
| [`federal_register`](tables/federal_register.md) | one row per Federal Register document | `document_number` |
| [`unified_agenda`](tables/unified_agenda.md) | one row per RIN per agenda edition | `rin` |
| [`congress_bills`](tables/congress_bills.md) | one row per bill | `bill_id` |
| [`cfr_sections`](tables/cfr_sections.md) | one row per CFR granule | `granule_id` |
| [`fcc_proceedings`](tables/fcc_proceedings.md) | one row per FCC proceeding (docket) | `name` |
| [`fcc_filings`](tables/fcc_filings.md) | one row per FCC ECFS filing (comment) | `id_submission` |

### Rule identity and ontology

These tables are implemented locally and await their first production
materialized generation.

| Table | Grain | Key |
| --- | --- | --- |
| [`rule_targets`](tables/rule_targets.md) | one row per docket / CFR target / RIN / evidence source | composite |
| [`authority_edges`](tables/authority_edges.md) | one row per parsed or retained legal-authority citation | composite |
| [`proceedings`](tables/proceedings.md) | one row per independently evidenced regulatory action | `proceeding_id` |
| [`regulatory_agenda_items`](tables/regulatory_agenda_items.md) | one durable agenda item per RIN | `agenda_item_id` |
| [`agenda_item_proceedings`](tables/agenda_item_proceedings.md) | one evidence-qualified agenda-item-to-proceeding relationship | `relationship_id` |
| [`comment_periods`](tables/comment_periods.md) | one row per continuous or reopened comment window | `comment_period_id` |
| [`concepts`](tables/concepts.md) | one row per retrieval concept | `concept_id` |
| [`concept_assignments`](tables/concept_assignments.md) | one row per append-only tag assertion | `assignment_id` |
| [`concept_events`](tables/concept_events.md) | one row per structural registry event | `event_id` |

### Organizations & influence

| Table | Grain | Key |
| --- | --- | --- |
| [`sam_entities`](tables/sam_entities.md) | one row per SAM-registered entity | `uei` |
| [`lobbying_filings`](tables/lobbying_filings.md) | one row per LDA filing | `filing_uuid` |
| [`fec_committees`](tables/fec_committees.md) | one row per FEC committee / PAC | `committee_id` |

### Outcomes & context

| Table | Grain | Key |
| --- | --- | --- |
| [`usaspending_recipients`](tables/usaspending_recipients.md) | one row per federal-award recipient | `recipient_id` |
| [`court_dockets`](tables/court_dockets.md) | one row per federal-court docket | `cl_docket_id` |
| [`gao_reports`](tables/gao_reports.md) | one row per GAO report | `report_id` |
| [`crs_reports`](tables/crs_reports.md) | one row per CRS report | `report_id` |

## How the tables relate

The three core tables form a simple hierarchy keyed by id:

```
dockets (docket_id)
  └── documents (document_id, docket_id →)
  └── comments  (comment_id,  docket_id →)
```

- `documents.docket_id` and `comments.docket_id` reference `dockets.docket_id`.
- `agency_code` appears on every table and is the join key for the agency rollups.
- The rollups (`comments_index`, `feed_summary`, `agency_stats`,
  `agency_monthly_volume`) are pre-aggregated views built from the three core
  tables so consumers don't have to scan the tens-of-millions-of-rows comments
  dataset.

The complementary sources join through normalized identity tables:

- **`rule_targets`** normalizes RIN, CFR, Federal Register, document, and docket
  evidence while preserving corroborating sources as separate rows.
- **`authority_edges`** parses U.S.C. and Public Law citations without dropping
  failed parses; `pl_number` joins enacted authorities to `congress_bills`.
- **`proceedings`** threads stages and documents without assuming that a reused
  RIN is globally unique; **`comment_periods`** carries extensions and reopenings.
- **`concept_assignments`** connects dockets/documents to the append-only
  **`concepts`** registry, with provenance and validation history in every row.
- **UEI** (Unique Entity ID) links `sam_entities` and `usaspending_recipients`,
  and is the anchor for resolving commenter/organization names to a canonical
  entity.
- **Organization name** bridges the softer influence sources — `lobbying_filings`
  (registrant/client), `fec_committees`, and comment filers — where no shared id
  exists.
- **`agency_code` / agency name** appears across nearly every table.

The compact identifiers, provenance mapping, and Rulespec Level-0 posture are
documented in [Ontology and Rulespec L0](ontology.md). The
[RIN ontology revision report](rin-ontology-revision-report.md) records the
current local agenda-item/Proceeding corpus result.

> Coverage notes: `sam_entities` is a partial sample (~5K of ~765K — the SAM API
> caps pagination; full coverage needs chunked ingestion), `lobbying_filings`
> covers 2024-onward, `usaspending_recipients` is the top ~100K recipients by
> award amount, and `gao_reports` tracks GAO's recent-items RSS window (it grows
> as the daily job runs). Each table page notes its own scope.

## How to query it

=== "AI assistant (MCP)"

    The hosted MCP server exposes `list_sources`, `describe_table`, and
    `query_sql` over the tables currently published above. The nine ontology
    tables appear only after their first complete generation. Add
    `https://mcp.spicy-regs.dev/mcp` as a connector, or run it locally:

    ```bash
    claude mcp add spicy-regs -- uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs-mcp
    ```

=== "CLI"

    ```bash
    uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs download
    uv run spicy-regs stats
    ```

=== "DuckDB (SQL)"

    ```sql
    INSTALL httpfs; LOAD httpfs;
    SELECT agency_code, COUNT(*) AS dockets
    FROM read_parquet('https://r2.spicy-regs.dev/dockets.parquet')
    GROUP BY agency_code
    ORDER BY dockets DESC
    LIMIT 20;
    ```

=== "Python"

    ```python
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")
    con.execute(
        "SELECT agency_code, docket_count "
        "FROM read_parquet('https://r2.spicy-regs.dev/agency_stats.parquet') "
        "ORDER BY docket_count DESC LIMIT 20"
    ).df()   # -> pandas DataFrame
    ```

    See **[Querying with Python](querying-python.md)** for a full walkthrough,
    including cross-source joins (RIN, UEI) and working with the large
    `comments` table.

!!! note "Keeping this current"
    Column names and types are the source of truth in code
    (`RECORD_TYPES` for the core tables, `DERIVED_SCHEMAS` for the rollups). The
    prose lives in `data_dictionary/descriptions.yaml`. Run
    `uv run spicy-regs-dict generate` to rebuild the table pages, and
    `uv run spicy-regs-dict check` to verify the two are in sync — the same
    check runs in CI on every pull request.
