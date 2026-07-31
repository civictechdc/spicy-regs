<div align="center">

<img src="assets/icon.png" alt="Spicy Regs" width="96" height="96">

# Spicy Regs

**An open, queryable mirror of U.S. federal regulatory data — and the pipeline that builds it.**

<a href="https://www.civictechdc.org/">
  <img src="assets/civictechdc-logo.png" alt="Civic Tech DC" width="22" height="22" align="top">
</a>
&nbsp;A <a href="https://www.civictechdc.org/"><b>Civic Tech DC</b></a> project

[![CI](https://github.com/civictechdc/spicy-regs/actions/workflows/ci.yml/badge.svg)](https://github.com/civictechdc/spicy-regs/actions/workflows/ci.yml)
[![Integration](https://github.com/civictechdc/spicy-regs/actions/workflows/integration.yml/badge.svg)](https://github.com/civictechdc/spicy-regs/actions/workflows/integration.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Slack](https://img.shields.io/badge/Slack-join%20us-4A154B?logo=slack&logoColor=white)](https://join.slack.com/t/civictechdc/shared_invite/zt-43eotbj04-QLQ_Ria296PtRYJU2EgwxQ)

[Explore the data](https://app.spicy-regs.dev) ·
[Data dictionary](https://docs.spicy-regs.dev/) ·
[MCP server](#use-it-from-an-ai-assistant) ·
[Contributing](CONTRIBUTING.md) ·
[Changelog](CHANGELOG.md)

</div>

---

Every federal rule that gets proposed generates a public record: a docket, the
agency's documents, and the comments people file on it. That record is public
but awkward to work with — paginated APIs, rate limits, no bulk access, and no
way to join it to the rest of the federal picture.

Spicy Regs turns it into files you can query. A nightly pipeline reads
[regulations.gov](https://www.regulations.gov) data (via the public
[Mirrulations](https://github.com/MoravianUniversity/mirrulations) mirror) plus a
dozen complementary federal sources, and publishes the result as Parquet and
Apache Iceberg on Cloudflare R2 — public, anonymous read, no API key.

**You can query ~25M public comments from a laptop, a browser tab, or an AI
assistant, without downloading a database or asking anyone for access.**

This repo is the pipeline, the rollups, and the read-only MCP server. It's a
[Civic Tech DC](https://www.civictechdc.org/) project and new contributors are
welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## Contents

- [Try it without installing anything](#try-it-without-installing-anything)
- [What's in the corpus](#whats-in-the-corpus)
- [Quickstart](#quickstart)
- [Working with the data locally](#working-with-the-data-locally)
- [Running the pipeline yourself](#running-the-pipeline-yourself)
- [Use it from an AI assistant](#use-it-from-an-ai-assistant)
- [Project layout](#project-layout)
- [Related repositories](#related-repositories)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)
- [Acknowledgments](#acknowledgments)

## Try it without installing anything

| I want to… | Go here |
|---|---|
| Browse dockets, agencies, and comment activity | **[app.spicy-regs.dev](https://app.spicy-regs.dev)** |
| Read every column of every published table | **[docs.spicy-regs.dev](https://docs.spicy-regs.dev/)** |
| Ask an AI assistant questions about the data | **[MCP server](#use-it-from-an-ai-assistant)** (`https://mcp.spicy-regs.dev/mcp`) |
| Run SQL in a notebook | **[Binder](https://mybinder.org/v2/gh/civictechdc/spicy-regs/HEAD)** or [`docs/querying-python.md`](docs/querying-python.md) |

One line of SQL against the public bucket — no credentials, no download:

```sql
-- DuckDB, anywhere: CLI, notebook, or the browser
SELECT agency_code, comment_count
FROM read_parquet('https://r2.spicy-regs.dev/agency_stats.parquet')
ORDER BY comment_count DESC
LIMIT 10;
```

## What's in the corpus

Everything is published under `https://r2.spicy-regs.dev` with public,
anonymous read. Per-column reference and exact row counts live in the
[data dictionary](https://docs.spicy-regs.dev/) — it's generated from the
schemas in this repo and kept in sync by CI, so it never drifts from what's
actually published.

**Core regulations.gov tables**

| Table | What it is | Scale |
|---|---|---|
| `dockets` | Regulatory proceedings | ~276K rows |
| `documents` | Documents within dockets | ~2.0M rows |
| `comments` | Public comments (Hive-partitioned Parquet + an Iceberg table) | ~25.4M rows |

**Rollups** — small, denormalized, meant to be read whole: `feed_summary`,
`agency_stats`, `agency_monthly_volume`, `comments_index`, `docket_search`,
`rulemaking_lifecycles`, `discovery_signals`, `fr_docket_links`.

**Complementary federal sources** — each ingested from its own API so the
rulemaking lifecycle, the organizations engaged in it, and its downstream
context are all joinable in one place:

- *Lifecycle:* `federal_register`, `unified_agenda`, `congress_bills`, `cfr_sections`
- *Organizations & influence:* `sam_entities`, `lobbying_filings`, `fec_committees`
- *Outcomes & context:* `usaspending_recipients`, `court_dockets`, `gao_reports`, `crs_reports`
- *Telecom:* `fcc_proceedings`, `fcc_filings`

Cross-source join keys: **RIN**, **CFR citation**, **UEI**, **`agency_code`**.
Some sources are deliberately bounded or sampled (e.g. `lobbying_filings` is
2024+, `usaspending_recipients` is the top 100K by award dollars) — the data
dictionary documents the scope of each.

## Quickstart

Prerequisites: Python 3.10+ and [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
git clone https://github.com/civictechdc/spicy-regs.git
cd spicy-regs
uv sync                       # install dependencies into .venv
uv run pytest                 # run the test suite
uv run ruff check .           # lint
```

No credentials are needed to run the tests, download the published Parquet, or
run the pipeline against the public Mirrulations mirror. Copy `.env.example` to
`.env` only if you want to publish output to Cloudflare R2 or ingest a source
that requires an API key.

## Working with the data locally

Download the published Parquet with the bundled CLI — no credentials:

```bash
uv run spicy-regs download                        # dockets, documents, comments
uv run spicy-regs download --types comments       # comments only
uv run spicy-regs download -o ./my-data           # custom output dir
```

Files land in `./spicy-regs-data/` by default. Then poke around:

```bash
uv run spicy-regs stats                # row counts + top agencies per file
uv run spicy-regs sample comments -n 5 # 5 random rows from comments
uv run spicy-regs search "climate"     # substring search across files
uv run spicy-regs agencies             # list every agency code
```

> Don't want to clone? Run it one-shot:
> `uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs download --types comments`

For SQL-first exploration, [`docs/querying-python.md`](docs/querying-python.md)
walks through querying the bucket directly with DuckDB.

## Running the pipeline yourself

The pipeline reads raw JSON from the public Mirrulations S3 mirror, flattens it,
and writes Parquet to `./output/`. Scope your first run tight so it finishes in
minutes instead of hours:

```bash
# Smallest useful run: one agency, recent dockets, comments only, no upload.
uv run run-pipeline --agency EPA --only-comments --since-year 2025
```

What you get:

- `output/comments.parquet` — merged and deduplicated comments
- `output/manifest.parquet` — a Bloom filter of already-processed source keys,
  so the next run is incremental. Delete it or pass `--full-refresh` to rebuild
  from scratch.

Useful flags (`uv run run-pipeline --help` for the full list):

| Flag | What it does |
|---|---|
| `--agency EPA` | Process a single agency instead of all of them |
| `--since-year 2025` | Skip dockets older than the given year |
| `--only-comments` | Stage comments only (skip dockets + documents) |
| `--skip-comments` | Inverse — dockets + documents only (much faster) |
| `--max-workers 8` | Agencies processed in parallel (default 4) |
| `--full-refresh` | Ignore the existing manifest and rebuild from scratch |
| `--no-skip-upload` | Also publish to R2 (needs credentials in `.env`) |
| `--use-iceberg` | Route dockets + comments through the R2 Data Catalog |
| `--chunk-size 50000` | Bounded-memory comment ingest for very large agencies |
| `--no-enrich-text` | Skip filling comment `text_content` from Mirrulations' pre-extracted attachment text |

### Rollups

Rollups are decoupled from the main ETL — each is its own console script and its
own cron workflow, so one failing source can't block the rest:

```bash
uv run run-rollup-feed-summary        # derived from the core R2 tables
uv run run-rollup-federal-register    # ingests an external API → its own Parquet
```

Each defaults to `--skip-upload`. See [CONTRIBUTING.md](CONTRIBUTING.md) for the
full list and for what adding a new external source touches.

### Backfilling comment text

The ETL fills `text_content` inline only for comments it processes fresh — the
incremental manifest skips comments ingested before that feature landed, so they
stay `NULL`. Backfill from Mirrulations' pre-extracted text (read straight from
the bucket's `derived-data` prefix — no PDF download, no JSON re-ingest):

```bash
uv run spicy-regs download --types comments     # grab the published parquet
uv run backfill-comment-text                     # fill text_content in place
uv run backfill-comment-text --limit 5000        # cap work for a trial run
uv run backfill-comment-text --upload            # republish to R2 (needs credentials)
```

It's incremental and re-runnable — rows with a `text_extraction_status` are
skipped unless you pass `--overwrite`. Document text isn't published to
`derived-data`; backfill those with `uv run enrich-pdf-text --target documents`.

### Working on the data dictionary

```bash
uv run spicy-regs-dict check        # verify descriptions match the schema
uv run spicy-regs-dict generate     # regenerate docs/tables/*.md
uv run --group docs mkdocs serve    # preview at 127.0.0.1:8000
```

Edit descriptions in `data_dictionary/descriptions.yaml`. CI fails if they drift
from the schema.

## Use it from an AI assistant

A read-only MCP server exposes SQL over the corpus with three tools:
`list_sources()`, `describe_table(table)`, and `query_sql(sql)`.

| Client | Setup |
|---|---|
| **Claude.ai** or any remote MCP client | Add `https://mcp.spicy-regs.dev/mcp` as a Custom Connector. See [`mcp-server/README.md`](mcp-server/README.md). |
| **Claude Code** (plugin) | `/plugin marketplace add civictechdc/spicy-regs` then `/plugin install spicyregs@spicy-regs-local` |
| **Claude Code** (stdio, no deploy) | `claude mcp add spicy-regs -- uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs-mcp` |
| **Cursor / Continue / OpenAI / others** | See [`plugins/spicyregs/INSTALL.md`](plugins/spicyregs/INSTALL.md) for the full matrix, including a prompt-only fallback for assistants without MCP support. |

## Project layout

```
src/spicy_regs/
├── pipelines/        # Pipeline contract, the main ETL, and one module per rollup
│   └── rollups/      # Derived rollups + external-source ingests
├── sources/          # Readers/writers: Mirrulations S3, R2, Iceberg, external APIs
├── transforms/       # Merge, partition, enrich, and the rollup builders
├── manifest.py       # Incremental state (Bloom filter over processed source keys)
├── mcp_server.py     # Read-only SQL MCP server
├── data_dictionary.py# Generates the docs site from the schemas
└── cli.py            # Local data CLI (download / stats / sample / search)

mcp-server/           # Vercel deployment of the MCP server
scripts/              # Operational tooling (dedupe, seed, freshness checks)
notebooks/            # Example analyses (runnable on Binder)
tests/                # Unit suite; integration tests are opt-in
.github/workflows/    # ETL cron, per-rollup crons, CI, deploys
```

The ETL is assembled from small composable pieces — `Reader → Transform →
Writer`, wired by a `Pipeline`. The
[architecture section of CONTRIBUTING.md](CONTRIBUTING.md#architecture-the-etl-building-blocks)
explains the contract, and `tests/test_example_pipeline.py` is a runnable
reference for adding your own.

## Related repositories

| Repo | What it is |
|---|---|
| **spicy-regs** (this repo) | ETL, rollups, MCP server. Publishes the public corpus. |
| [spicy-regs-ui](https://github.com/ekim1394/spicy-regs-ui) | The public explorer at [app.spicy-regs.dev](https://app.spicy-regs.dev) — Next.js + DuckDB-WASM, queries the corpus live in the browser with no backend. |

## Contributing

New contributors are welcome, technical or not. **[CONTRIBUTING.md](CONTRIBUTING.md)**
covers getting set up, a glossary of regulatory terms, a map of where things
live, and how to add your own reader / transform / writer / pipeline.

Good places to start:

- [Good first issues](https://github.com/civictechdc/spicy-regs/labels/good%20first%20issue)
- [Help wanted](https://github.com/civictechdc/spicy-regs/labels/help%20wanted)
- Open a [new issue](https://github.com/civictechdc/spicy-regs/issues/new/choose) — bug, feature, or a question

Before opening a PR:

```bash
uv run ruff format . && uv run ruff check . && uv run ty check && uv run pytest
```

CI runs those same checks on every PR, plus a guard that the data dictionary
still matches the schema. Live-data integration tests run weekly.

## License

[MIT](LICENSE) © Civic Tech DC.

The underlying data is public U.S. federal government information. Mirrulations
S3 is read with unsigned (anonymous) access, and the published Parquet/Iceberg
on R2 is openly readable.

## Contact

- **Slack** — [join the Civic Tech DC workspace](https://join.slack.com/t/civictechdc/shared_invite/zt-43eotbj04-QLQ_Ria296PtRYJU2EgwxQ)
  (open invite, anyone welcome), then say hello in
  [#spicy-regs](https://civictechdc.slack.com/archives/C09H576E6LU). This is the
  fastest way to reach us.
- **GitHub** — open an
  [issue](https://github.com/civictechdc/spicy-regs/issues/new/choose) for a bug,
  a feature idea, or a question about the data.
- **Email** — [eugene.kim@civictechdc.com](mailto:eugene.kim@civictechdc.com) for
  sponsorships, partnerships, or enterprise use cases.

## Acknowledgments

- [Mirrulations](https://github.com/MoravianUniversity/mirrulations) — the public
  regulations.gov mirror this pipeline reads from.
- [regulations.gov](https://www.regulations.gov) and the agencies that publish
  their dockets there.
- Everyone who has contributed code, issues, and analysis.

---

<div align="center">

<a href="https://www.civictechdc.org/">
  <img src="assets/civictechdc-logo.png" alt="Civic Tech DC" width="72" height="72">
</a>

Built and maintained by **[Civic Tech DC](https://www.civictechdc.org/)** — a
volunteer community using technology to serve the DC region.

**[Come build with us.](https://join.slack.com/t/civictechdc/shared_invite/zt-43eotbj04-QLQ_Ria296PtRYJU2EgwxQ)**

</div>
