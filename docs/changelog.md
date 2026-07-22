# Changelog

Notable changes to the Spicy Regs data pipeline and the tables it publishes.
Entries link to the pull request that introduced the change. The canonical
per-release copy lives in [`CHANGELOG.md`](https://github.com/civictechdc/spicy-regs/blob/main/CHANGELOG.md)
at the repository root and on the
[GitHub Releases](https://github.com/civictechdc/spicy-regs/releases) page.

## 2026-07-22

This cycle expanded the dataset from a regulations.gov + Federal Register mirror
into a broader federal-data corpus: **ten new complementary sources** now ship
as their own tables, covering the rulemaking lifecycle, the organizations that
engage in it, and its downstream context.

!!! note "New tables"
    Every table below is published as `https://r2.spicy-regs.dev/<name>.parquet`
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
