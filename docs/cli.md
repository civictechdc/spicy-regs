# CLI

The `spicy-regs` command-line tool downloads the published parquet files and
runs SQL against every table in this data dictionary — no credentials, no
setup beyond Python.

## Install

Run it one-shot with [uv](https://docs.astral.sh/uv/getting-started/installation/)
(nothing to install):

```bash
uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs --help
```

Or from a clone of the repo:

```bash
git clone https://github.com/civictechdc/spicy-regs.git
cd spicy-regs && uv sync
uv run spicy-regs --help
```

The examples below use `uv run spicy-regs …`; substitute the `uvx` form if you
haven't cloned the repo.

## Where commands read data from

Every command that reads tables takes a `--source` flag:

| Source | Behavior |
| --- | --- |
| `auto` (default) | Per table: use your local download when present, otherwise stream from the public bucket |
| `local` | Only files downloaded to your data directory (default `./spicy-regs-data/`, override with `-o`) |
| `r2` | Only the public bucket (`https://r2.spicy-regs.dev`) |

Because of `auto`, **you can query everything without downloading anything** —
DuckDB reads the remote parquet over HTTPS and only fetches the row groups a
query needs. Download the tables you use heavily to make repeated queries fast
and offline.

## Explore the tables

```bash
uv run spicy-regs tables                 # every table + whether it resolves locally or to R2
uv run spicy-regs describe dockets       # column names and types (matches this data dictionary)
uv run spicy-regs describe comments --format json
```

## Query with SQL

`spicy-regs query` runs [DuckDB](https://duckdb.org/docs/stable/sql/introduction)
SQL with one view per published table, so you can filter, aggregate, and join
across all of them:

```bash
# Top agencies by docket volume
uv run spicy-regs query "SELECT agency_code, count(*) AS n FROM dockets GROUP BY 1 ORDER BY n DESC LIMIT 10"

# Join: most-commented dockets with their titles
uv run spicy-regs query "
  SELECT d.docket_id, d.title, f.comment_count
  FROM feed_summary f JOIN dockets d USING (docket_id)
  ORDER BY f.comment_count DESC LIMIT 10"

# Machine-readable output for scripts
uv run spicy-regs query "SELECT * FROM agency_stats LIMIT 5" --format json
uv run spicy-regs query "SELECT * FROM agency_stats" --format csv --output agency_stats.csv --max-rows 0
```

Options:

| Flag | Meaning |
| --- | --- |
| `--format table\|json\|csv` | Output format (default: aligned table) |
| `--max-rows N` | Cap returned rows; `0` = unlimited (default 25) |
| `--output FILE` | Write results to a file instead of stdout |
| `--source`, `--r2-url`, `-o` | Data source controls (see above) |

Keep a `LIMIT` on exploratory queries — `comments` in particular is tens of
millions of rows.

## Quick looks without SQL

```bash
uv run spicy-regs stats                  # row counts + top agencies for the core tables
uv run spicy-regs sample comments -n 5   # random rows from any table (--agency EPA to filter)
uv run spicy-regs search "climate"       # substring search across dockets/documents/comments
uv run spicy-regs agencies               # every agency code in the dataset
```

## Download the parquet files

```bash
uv run spicy-regs download                                  # core trio: dockets, documents, comments
uv run spicy-regs download --tables feed_summary agency_stats
uv run spicy-regs download --all                            # every table (comments alone is multiple GB)
uv run spicy-regs download -o ./my-data                     # custom directory
```

Downloads stream with a progress bar and are written atomically, so an
interrupted download never leaves a truncated file. Files whose size still
matches the bucket are skipped on re-runs; `--force` re-downloads
unconditionally.

## Extending the CLI

Each subcommand is a small module in
[`src/spicy_regs/cli/`](https://github.com/civictechdc/spicy-regs/tree/main/src/spicy_regs/cli)
— adding one is a copy-a-template-and-register-it change. See the
[contributing guide](https://github.com/civictechdc/spicy-regs/blob/main/CONTRIBUTING.md)
for the recipe.
