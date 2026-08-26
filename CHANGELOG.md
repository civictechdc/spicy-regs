# Changelog

All notable changes to the Spicy Regs data pipeline are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Entries link to the pull request that introduced the change.

## [2026.08.26]

The headline of this cycle: the MCP server moved off Vercel onto **Cloud Run**,
where it is load-tested to **100 concurrent users at ~0% errors**, and the
deployment itself became infrastructure-as-code under a new `deploy/` folder.
Along the way the **FCC** joined the corpus as its own two tables, and a pass
over "how do I actually use this data" turned up — and fixed — several
documented access paths that did not work.

### Added

- **FCC ECFS** ingestion — `fcc_proceedings` and `fcc_filings`, the FCC's docket
  and comment equivalents for the rulemaking that never reaches
  regulations.gov ([#149]).
- **Cloud Run** as the primary MCP host, captured as a reproducible deploy
  script plus a runbook for the billing-quota and org-policy hurdles
  ([#164]).
- **`deploy/` folder with Terraform IaC** for the R2 bucket, its
  `data.spicy-regs.dev` domain, CORS, the Iceberg catalog, and the edge cache
  rule — import-first, so it adopts existing production rather than recreating
  it ([#159]); Terraform state moved into a private R2 bucket over the `s3`
  backend with native locking ([#161]).
- **Cloudflare Containers** deploy, authored in [#159] and made actually
  deployable in [#162] (build context, generated Worker bindings); kept as the
  documented single-instance fallback.
- **Cloudflare purge-on-publish** (`sources/cloudflare.py`), a no-op without
  credentials and never able to fail a publish that already wrote its data
  ([#158]).
- Docs: `mcp-server/INTERNALS.md` as the committed home for server rationale
  ([#153]); a README reworked into a project front page ([#150]); a
  `getting_started.ipynb` covering the rollups, the non-core tables, and
  cross-source joins ([#156]).

### Changed

- **Performance:** the MCP server caches its DuckDB connection across tool
  calls instead of reinstalling extensions, reattaching the catalog, and
  recreating 20 views per request — **34.5s → 0.21s** warm ([#157]).
- **`.env` loads in CLI entry points, not at import time**, so importing the
  package no longer mutates `os.environ`; one AST test locks it in ([#155]).
- The public corpus hostname is `data.spicy-regs.dev` ([#150]).
- **MCP:** all code comments moved out of the server modules into
  `INTERNALS.md`, keeping the `@mcp.tool()` docstrings that FastMCP reflects
  over to build client-facing tool descriptions ([#153]).
- **Vercel:** pinned `mcp<2` and raised `maxDuration` to the real 800s Pro
  ceiling ([#151], [#152]) — both superseded when that deploy was retired
  ([#166]).

### Removed

- The **Vercel MCP deployment** and its dependency-light parallel copy of the
  server, now that `mcp.spicy-regs.dev` points at Cloud Run running the
  canonical `spicy_regs.mcp_server` ([#166]).

### Fixed

- **Parquet must be `no-cache`.** Edge-caching it corrupts DuckDB's concurrent
  byte-range reads — observed as decode and ETag-mismatch errors from ~c=10 on
  a revision that otherwise ran c=100 cleanly. Reverted the [#158] cache policy
  for Parquet ([#165]) and set the Terraform-managed cache rule to
  `enabled = false` so an apply cannot re-enable it ([#167]). Non-Parquet
  artifacts, which DuckDB never reads, stay cacheable.
- **The `spicy-regs` CLI was broken end to end** — Cloudflare 403s the default
  urllib User-Agent, so `download`, `stats`, `sample`, `search`, and `agencies`
  all failed; downloads now stream to a `.partial` sibling so a truncated write
  can't masquerade as a complete one. Also repointed the published query docs
  off a comments tree that no longer gets written, and added
  `--discover-from-derived` so the comment-text backfill can see the 99.8% of
  rows the `attachments_json` gate hid from it ([#156]).
- **Unit tests were downloading the production corpus** — a developer's `.env`
  reached "hermetic" tests through an import-time `load_dotenv()`, making them
  838s locally and green-by-accident in CI ([#154]).

[2026.08.26]: https://github.com/civictechdc/spicy-regs/releases/tag/2026.08.26
[#149]: https://github.com/civictechdc/spicy-regs/pull/149
[#150]: https://github.com/civictechdc/spicy-regs/pull/150
[#151]: https://github.com/civictechdc/spicy-regs/pull/151
[#152]: https://github.com/civictechdc/spicy-regs/pull/152
[#153]: https://github.com/civictechdc/spicy-regs/pull/153
[#154]: https://github.com/civictechdc/spicy-regs/pull/154
[#155]: https://github.com/civictechdc/spicy-regs/pull/155
[#156]: https://github.com/civictechdc/spicy-regs/pull/156
[#157]: https://github.com/civictechdc/spicy-regs/pull/157
[#158]: https://github.com/civictechdc/spicy-regs/pull/158
[#159]: https://github.com/civictechdc/spicy-regs/pull/159
[#161]: https://github.com/civictechdc/spicy-regs/pull/161
[#162]: https://github.com/civictechdc/spicy-regs/pull/162
[#164]: https://github.com/civictechdc/spicy-regs/pull/164
[#165]: https://github.com/civictechdc/spicy-regs/pull/165
[#166]: https://github.com/civictechdc/spicy-regs/pull/166
[#167]: https://github.com/civictechdc/spicy-regs/pull/167

## [2026.07.22]

The headline of this cycle: the pipeline went from mirroring regulations.gov and
the Federal Register to ingesting **ten complementary federal data sources**, so
the full rulemaking lifecycle — the organizations that engage in it and its
downstream context — can be queried from one place. Alongside the new sources,
this cycle hardened the rollups and made the published corpus edge-cacheable.

### Added

- **Federal Register** ingestion brought fully in-repo ([#125]).
- **Unified Agenda** (RegInfo) ingestion — `unified_agenda` ([#126]).
- **Congress.gov** bill ingestion — `congress_bills` ([#127]).
- **GovInfo CFR** section ingestion — `cfr_sections` ([#128]).
- **OpenFEC** committees ingestion — `fec_committees` ([#132]).
- **Senate Lobbying Disclosure (LDA)** filings ingestion — `lobbying_filings` ([#133]).
- **SAM.gov** entity registry ingestion — `sam_entities` ([#134]).
- **USASpending** recipients ingestion — `usaspending_recipients` ([#135]).
- **CourtListener** litigation ingestion — `court_dockets` ([#137]).
- **GAO + CRS** reports ingestion — `gao_reports`, `crs_reports` ([#138]).
- Docs: a Python query walkthrough ([#142]) and a refreshed table catalog for
  the eleven new data sources ([#139]).

### Changed

- **Performance:** made the R2 parquet corpus edge-cacheable and pruned docket
  scans ([#124]).
- **SAM.gov:** upgraded to full-coverage ingestion via a partitioned walk ([#141]).
- **OpenFEC:** switched to keyset pagination to walk all committees ([#136]).
- **Unified Agenda:** now fetches and parses the real `REGINFO_RIN_DATA` XML
  export ([#131]).
- **CFR:** use the GovInfo `/published` endpoint and derive section fields from
  IDs ([#130]).
- **Congress.gov:** drop the always-null `policy_area` (the list endpoint omits
  it) ([#129]).
- **CI:** schedule weekly comments-catalog compaction and serialize catalog
  writers ([#145]); forward `SAM_API_KEY` to rollup jobs ([#140]).

### Fixed

- Harden the external-source rollups ([#147]).
- Dedup the catalog comments view on read ([#143]).
- Sub-batch catalog dedup by key hash to avoid runner OOM ([#123]).
- Matrix sweep + full agency coverage to stop batch starvation ([#121]).
- Stop marking failed downloads as processed in the ETL manifest ([#120]).

[2026.07.22]: https://github.com/civictechdc/spicy-regs/releases/tag/2026.07.21
[#120]: https://github.com/civictechdc/spicy-regs/pull/120
[#121]: https://github.com/civictechdc/spicy-regs/pull/121
[#123]: https://github.com/civictechdc/spicy-regs/pull/123
[#124]: https://github.com/civictechdc/spicy-regs/pull/124
[#125]: https://github.com/civictechdc/spicy-regs/pull/125
[#126]: https://github.com/civictechdc/spicy-regs/pull/126
[#127]: https://github.com/civictechdc/spicy-regs/pull/127
[#128]: https://github.com/civictechdc/spicy-regs/pull/128
[#129]: https://github.com/civictechdc/spicy-regs/pull/129
[#130]: https://github.com/civictechdc/spicy-regs/pull/130
[#131]: https://github.com/civictechdc/spicy-regs/pull/131
[#132]: https://github.com/civictechdc/spicy-regs/pull/132
[#133]: https://github.com/civictechdc/spicy-regs/pull/133
[#134]: https://github.com/civictechdc/spicy-regs/pull/134
[#135]: https://github.com/civictechdc/spicy-regs/pull/135
[#136]: https://github.com/civictechdc/spicy-regs/pull/136
[#137]: https://github.com/civictechdc/spicy-regs/pull/137
[#138]: https://github.com/civictechdc/spicy-regs/pull/138
[#139]: https://github.com/civictechdc/spicy-regs/pull/139
[#140]: https://github.com/civictechdc/spicy-regs/pull/140
[#141]: https://github.com/civictechdc/spicy-regs/pull/141
[#142]: https://github.com/civictechdc/spicy-regs/pull/142
[#143]: https://github.com/civictechdc/spicy-regs/pull/143
[#145]: https://github.com/civictechdc/spicy-regs/pull/145
[#147]: https://github.com/civictechdc/spicy-regs/pull/147
