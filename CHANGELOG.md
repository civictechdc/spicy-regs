# Changelog

All notable changes to the Spicy Regs data pipeline are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Entries link to the pull request that introduced the change.

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

[2026.07.22]: https://github.com/civictechdc/spicy-regs/releases/tag/v2026.07.22
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
