# Changelog

Notable changes to the Spicy Regs data pipeline and the tables it publishes.
Entries link to the pull request that introduced the change. The canonical
per-release copy lives in [`CHANGELOG.md`](https://github.com/civictechdc/spicy-regs/blob/main/CHANGELOG.md)
at the repository root and on the
[GitHub Releases](https://github.com/civictechdc/spicy-regs/releases) page.

## 2026-08-26

This cycle was mostly about how the data is served rather than what's in it: the
MCP server moved off Vercel onto **Cloud Run**, load-tested to **100 concurrent
users at ~0% errors**, with the deployment codified as infrastructure-as-code.
The corpus itself gained the **FCC**, and several documented access paths that
didn't actually work were fixed.

!!! note "New tables"
    Both tables are published as `https://data.spicy-regs.dev/<name>.parquet`
    and documented under **Tables** in the nav.

### New data sources

| Table | Source | PR |
| --- | --- | --- |
| `fcc_proceedings` | FCC ECFS — the FCC's docket equivalent, keyed by `name` (e.g. `17-108`) | [#149](https://github.com/civictechdc/spicy-regs/pull/149) |
| `fcc_filings` | FCC ECFS — the comment equivalent, keyed by `id_submission` | [#149](https://github.com/civictechdc/spicy-regs/pull/149) |

The FCC does not participate in regulations.gov, so its rulemaking record lives
only here. `fcc_filings.proceeding_names_json` joins to `fcc_proceedings.name`,
and `text_data` carries the full comment text for express comments.

### Serving and performance

- **The MCP server now runs on Cloud Run** at `mcp.spicy-regs.dev` — load-tested
  to c=100 (p50 2.0s, 0.3% errors), with `count(DISTINCT comment_id)` over 25.7M
  rows in ~7s ([#164](https://github.com/civictechdc/spicy-regs/pull/164)). The Vercel deploy is retired ([#166](https://github.com/civictechdc/spicy-regs/pull/166)).
- **DuckDB connections are cached across tool calls** instead of rebuilt per
  request: **34.5s → 0.21s** warm ([#157](https://github.com/civictechdc/spicy-regs/pull/157)).
- **Parquet is served `no-cache`, deliberately.** Edge-caching it corrupts
  DuckDB's concurrent byte-range reads — including in the browser DuckDB-WASM UI
  ([#165](https://github.com/civictechdc/spicy-regs/pull/165), [#167](https://github.com/civictechdc/spicy-regs/pull/167)). Non-Parquet artifacts like the search
  `json.gz` stay cacheable, and purge-on-publish ([#158](https://github.com/civictechdc/spicy-regs/pull/158)) keeps them
  fresh.

### Fixed

- **The `spicy-regs` CLI now works.** Cloudflare 403s the default urllib
  User-Agent, so `download`, `stats`, `sample`, `search`, and `agencies` all
  failed ([#156](https://github.com/civictechdc/spicy-regs/pull/156)).
- **Query docs corrected.** The recommended comments glob pointed at a tree that
  is no longer written, and R2's public HTTPS endpoint can't expand a glob at
  all. Use `comments/agency/agency_code={X}/part-0.parquet`, and
  `comments_index.parquet` for counts ([#156](https://github.com/civictechdc/spicy-regs/pull/156)).
- **Notebooks refreshed** against live production, plus a new
  `getting_started.ipynb` covering the rollups, the non-core tables, and
  cross-source joins ([#156](https://github.com/civictechdc/spicy-regs/pull/156)).
- Comment-text backfill gained `--discover-from-derived`, so it can see rows
  ingested before `attachments_json` was recorded ([#156](https://github.com/civictechdc/spicy-regs/pull/156)).
- Unit tests no longer download the production corpus ([#154](https://github.com/civictechdc/spicy-regs/pull/154),
  [#155](https://github.com/civictechdc/spicy-regs/pull/155)).

### Infrastructure

- A new `deploy/` folder with **Terraform** owning the R2 bucket, its
  `data.spicy-regs.dev` domain, CORS, the Iceberg catalog, and the (now
  disabled) cache rule ([#159](https://github.com/civictechdc/spicy-regs/pull/159)), with state in a private R2 bucket
  ([#161](https://github.com/civictechdc/spicy-regs/pull/161)). Cloudflare Containers is kept as a documented fallback
  ([#162](https://github.com/civictechdc/spicy-regs/pull/162)).
- Docs: the README is now a project front page ([#150](https://github.com/civictechdc/spicy-regs/pull/150)) and MCP server
  rationale lives in `mcp-server/INTERNALS.md` ([#153](https://github.com/civictechdc/spicy-regs/pull/153)).

## 2026-07-22

This cycle expanded the dataset from a regulations.gov + Federal Register mirror
into a broader federal-data corpus: **ten new complementary sources** now ship
as their own tables, covering the rulemaking lifecycle, the organizations that
engage in it, and its downstream context.

!!! note "New tables"
    Every table below is published as `https://data.spicy-regs.dev/<name>.parquet`
    and documented under **Tables** in the nav.

### New data sources

| Table | Source | PR |
| --- | --- | --- |
| `federal_register` | Federal Register (now ingested in-repo) | [#125](https://github.com/civictechdc/spicy-regs/pull/125) |
| `unified_agenda` | Unified Agenda (RegInfo) | [#126](https://github.com/civictechdc/spicy-regs/pull/126) |
| `congress_bills` | Congress.gov | [#127](https://github.com/civictechdc/spicy-regs/pull/127) |
| `cfr_sections` | GovInfo CFR | [#128](https://github.com/civictechdc/spicy-regs/pull/128) |
| `fec_committees` | OpenFEC | [#132](https://github.com/civictechdc/spicy-regs/pull/132) |
| `lobbying_filings` | Senate Lobbying Disclosure (LDA) | [#133](https://github.com/civictechdc/spicy-regs/pull/133) |
| `sam_entities` | SAM.gov entity registry | [#134](https://github.com/civictechdc/spicy-regs/pull/134) |
| `usaspending_recipients` | USASpending | [#135](https://github.com/civictechdc/spicy-regs/pull/135) |
| `court_dockets` | CourtListener litigation | [#137](https://github.com/civictechdc/spicy-regs/pull/137) |
| `gao_reports`, `crs_reports` | GAO + CRS reports | [#138](https://github.com/civictechdc/spicy-regs/pull/138) |

### Changed

- Made the R2 parquet corpus **edge-cacheable** and pruned docket scans ([#124](https://github.com/civictechdc/spicy-regs/pull/124)).
- **SAM.gov:** full-coverage ingestion via a partitioned walk ([#141](https://github.com/civictechdc/spicy-regs/pull/141)).
- **OpenFEC:** keyset pagination to walk all committees ([#136](https://github.com/civictechdc/spicy-regs/pull/136)).
- **Unified Agenda:** fetch and parse the real `REGINFO_RIN_DATA` XML export ([#131](https://github.com/civictechdc/spicy-regs/pull/131)).
- **CFR:** use the GovInfo `/published` endpoint and derive section fields from IDs ([#130](https://github.com/civictechdc/spicy-regs/pull/130)).
- **Congress.gov:** drop the always-null `policy_area` ([#129](https://github.com/civictechdc/spicy-regs/pull/129)).
- **CI:** weekly comments-catalog compaction with serialized writers ([#145](https://github.com/civictechdc/spicy-regs/pull/145)); forward `SAM_API_KEY` to rollup jobs ([#140](https://github.com/civictechdc/spicy-regs/pull/140)).
- **Docs:** Python query walkthrough ([#142](https://github.com/civictechdc/spicy-regs/pull/142)) and refreshed table catalog ([#139](https://github.com/civictechdc/spicy-regs/pull/139)).

### Fixed

- Hardened the external-source rollups ([#147](https://github.com/civictechdc/spicy-regs/pull/147)).
- Dedup the catalog comments view on read ([#143](https://github.com/civictechdc/spicy-regs/pull/143)).
- Sub-batch catalog dedup by key hash to avoid runner OOM ([#123](https://github.com/civictechdc/spicy-regs/pull/123)).
- Matrix sweep + full agency coverage to stop batch starvation ([#121](https://github.com/civictechdc/spicy-regs/pull/121)).
- Stop marking failed downloads as processed in the ETL manifest ([#120](https://github.com/civictechdc/spicy-regs/pull/120)).
