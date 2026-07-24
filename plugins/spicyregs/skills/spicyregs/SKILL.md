---
name: spicyregs
description: Query regulatory data with Spicy Regs using DuckDB against the public Cloudflare R2 parquet bucket. Use when the user asks about dockets, comments, documents, agency activity, filing timelines, or cross-docket analysis and the answer should come from the remote Spicy Regs dataset.
---

# Spicy Regs

Use this skill to answer natural-language questions about regulatory data in this repo.

Prefer direct DuckDB queries against the public Cloudflare R2 bucket:

- Start with the public R2 parquet files used by the notebooks.
- Treat R2 as the required source unless the user explicitly asks for local parquet or sample JSON.
- Do not silently fall back to local parquet or sample JSON when R2 is unavailable.
- Do not browse the web for the answer when the question should be answered from the dataset.

## Provider Portability

Keep this workflow provider-neutral.

- Rely only on local files, shell commands, `uv`, Python, and DuckDB.
- Do not assume OpenAI-only connectors, tool names, or response formats.
- The `agents/openai.yaml` file is only UI metadata for Codex-compatible surfaces. Non-OpenAI runtimes can ignore it and reuse the workflow in this skill plus `references/provider-agnostic-prompt.md`.

## Sources

- Structured querying helper for the core regulations.gov surface:
  - `uv run --script plugins/spicyregs/skills/spicyregs/scripts/query_spicy_regs.py --source r2 --list-sources`
  - `uv run --script plugins/spicyregs/skills/spicyregs/scripts/query_spicy_regs.py --source r2 --describe dockets`
  - `uv run --script plugins/spicyregs/skills/spicyregs/scripts/query_spicy_regs.py --source r2 --sql "<SQL>"`
  - `uv run --script plugins/spicyregs/skills/spicyregs/scripts/find_duplicate_regulations.py --source r2 --limit 20`
- Remote MCP surface, including complementary and materialized tables (when
  running without a local Python toolchain, e.g. claude.ai):
  - `list_sources`, `describe_table`, `query_sql` tools exposed by the Spicy Regs MCP server in `mcp-server/`.
  - The seven materialized ontology tables appear only after their first
    complete production generation; trust `list_sources` for current
    availability.
  - Prefer the MCP tools when the skill is loaded in a chat surface that has the connector attached; prefer the `uv` scripts when running locally where they're faster and cover the duplicate-finder workflow.
- Optional alternate sources, only when explicitly requested:
  - `uv run --script plugins/spicyregs/skills/spicyregs/scripts/query_spicy_regs.py --source local --list-sources`
- Reference notes:
  - `references/data-layout.md`
  - `references/provider-agnostic-prompt.md`
  - `notebooks/README.md`

## Workflow

1. Resolve the data scope.
   - Default to the Cloudflare R2 bucket used in the notebooks.
   - If the user explicitly asks for local files, switch to `--source local` and inspect `./spicy-regs-data/` or the path they provide.
   - Use sample JSON only when the user explicitly asks for sample data or a task is specifically about the examples in `sample-data/mirrulations/`.
2. Start broad, then narrow.
   - Use `--list-sources` or `--describe` to confirm the remote tables and schema.
   - Use SQL for keyword discovery when the user gives phrases, issue names, or themes.
   - Use SQL for counts, joins, timelines, top-N lists, and filtering by agency, docket, or date.
3. Cite concrete records.
   - Include docket IDs, document IDs, comment IDs, agency codes, titles, and dates when available.
   - If the answer comes from only part of the dataset, say that explicitly.
4. Be honest about limits.
   - If the remote bucket is unavailable, stop and say so clearly instead of falling back.
   - Only use local parquet or sample JSON after the user explicitly requests that fallback.
   - If a question needs semantics beyond keyword matching, use SQL or direct record inspection instead of pretending a `LIKE` match proves the claim.

## Table Guide

Use the helper script's `--describe` output for the exact schema. In this repo, the core datasets typically map to:

- `dockets`: `docket_id`, `agency_code`, `title`, `docket_type`, `modify_date`, `abstract`
- `documents`: `document_id`, `docket_id`, `agency_code`, `title`, `document_type`, `posted_date`, `modify_date`, `comment_start_date`, `comment_end_date`, `file_url`
- `comments`: `comment_id`, `docket_id`, `agency_code`, `title`, `comment`, `document_type`, `posted_date`, `modify_date`, `receive_date`, `attachments_json`
- Optional summaries / rollups:
  - `comments_index`
  - `feed_summary`
  - `agency_stats`
  - `agency_monthly_volume`

Complementary federal sources (queryable by name through MCP when published;
see the Data Dictionary for columns):

- Rulemaking identity: `rule_targets` (normalized docket/CFR/RIN evidence), `authority_edges` (U.S.C./Public Law citations), `proceedings`, `comment_periods`
- Retrieval ontology: `concepts`, append-only `concept_assignments`, and structural `concept_events`
- Source lifecycle: `federal_register` (published rules; RIN + CFR refs), `unified_agenda` (planned actions), `fr_docket_links`, `rulemaking_lifecycles`, `congress_bills`, `cfr_sections`
- Organizations & influence: `sam_entities` (entity registry, keyed by `uei`), `lobbying_filings`, `fec_committees`
- Outcomes & context: `usaspending_recipients` (keyed by `uei`), `court_dockets`, `gao_reports`, `crs_reports`

Join keys across sources: `docket_id`, `rin`, and `cfr_ref` through
`rule_targets`; statutory citations through `authority_edges`; `pl_number`
joins enacted authorities to `congress_bills`, while U.S.C. citations do not
claim a bill crosswalk; `proceeding_id`; polymorphic
`(subject_type, subject_id)`; `uei` (sam_entities ↔ usaspending_recipients ↔
commenter orgs); and `agency_code`.

## Query Patterns

- Counts by agency: group on `agency_code`
- Questions about a docket's timeline: join `dockets` and `documents` on `docket_id`
- Questions about public feedback: query `comments` by `docket_id`, date, or keywords in `title` and `comment`
- Questions about open or reopened comment windows: query `comment_periods`
- Questions spanning dockets by regulation or statute: start with `rule_targets` or `authority_edges`
- Questions by topic regardless of agency/CFR location: resolve current `concept_assignments` through `concepts.replaced_by`
- Follow notebook-style access patterns from `notebooks/query_data.ipynb` and `notebooks/cross_docket_analysis.ipynb` when the question spans agencies or dockets.
- Questions about duplicate or coordinated rulemaking across agencies: use `find_duplicate_regulations.py` first, then verify promising clusters with targeted SQL against `dockets` or `documents`.

Keep result sets small while exploring. Add `LIMIT` unless the user explicitly wants a full export.

## Output Expectations

- Lead with the answer, then the evidence.
- Mention the exact data source you used:
  - Cloudflare R2 parquet
  - helper SQL query
  - local parquet
  - sample JSON
- If the question could not be answered because R2 was unavailable, say that directly instead of substituting another source.
- When useful, suggest the next query that would tighten uncertainty.
