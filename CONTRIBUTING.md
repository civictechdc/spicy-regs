# Contributing

Thanks for your interest in contributing to spicy-regs! This project is
maintained by [Civic Tech DC](https://civictechdc.org/), and we welcome
contributors of all experience levels — including first-time open source
contributors.

If you find this project useful, please consider giving it a star on GitHub — it helps others discover the project and motivates continued development.

## Getting started

Prerequisites: **Python 3.10+** and [**uv**](https://docs.astral.sh/uv/getting-started/installation/).

1. Fork the repo on GitHub, then clone your fork:
   ```bash
   git clone https://github.com/<your-username>/spicy-regs.git
   cd spicy-regs
   ```
2. Create a feature branch:
   ```bash
   git checkout -b my-change
   ```
3. Install dependencies (creates a `.venv` in the project directory):
   ```bash
   uv sync
   ```
4. Run the tests to confirm your environment works:
   ```bash
   uv run pytest
   ```
5. Run the linter:
   ```bash
   uv run ruff check .
   uv run ruff format .   # auto-format
   ```
6. (Recommended) Install the pre-commit hooks so ruff and ty run
   automatically on `git commit`:
   ```bash
   uv run pre-commit install
   ```
   The hooks run `ruff check --fix`, `ruff format`, and `ty check`. You
   can also run them manually:
   ```bash
   uv run pre-commit run --all-files
   ```

You don't need any credentials to run the tests or hack on most of the code.
A `.env` file (copy from `.env.example`) is only required to *upload* to live
Cloudflare R2 storage — reading from the public Mirrulations mirror and
downloading our published parquet files both work anonymously.

### Get the data on your machine

Two ways to get real data locally — pick whichever fits your task.

**A. Download the published parquet files (fast, ~minutes).** Pulls our
processed output from the public R2 bucket:

```bash
uv run spicy-regs download                       # dockets + documents + comments
uv run spicy-regs download --types comments      # just comments
uv run spicy-regs stats                          # sanity-check what you got
uv run spicy-regs sample comments -n 5           # peek at a few rows
```

Files default to `./spicy-regs-data/`; override with `-o some/dir`.

**B. Run the ETL pipeline yourself (slower, but it's the real thing).**
Reads JSON from the Mirrulations S3 mirror, flattens it, writes Parquet to
`./output/`. Keep the first run tiny so it finishes quickly:

```bash
# One agency, recent only, comments only — finishes in a few minutes.
uv run run-pipeline --agency EPA --only-comments --since-year 2025
```

Outputs land in `./output/` (e.g. `output/comments.parquet`) alongside an
incremental `manifest.json` — re-running picks up where the last run left
off. Drop `--only-comments` / `--agency` / `--since-year` to widen the scope;
add `--full-refresh` to ignore the manifest. `uv run run-pipeline --help`
lists every flag. Upload to R2 stays off by default (`--skip-upload`); only
the maintainers publish.

### Stuck?

- Open a [GitHub issue](https://github.com/civictechdc/spicy-regs/issues/new/choose) — we don't bite.
- Ping us on [Slack](https://civictechdc.slack.com/archives/C09H576E6LU).

## Making changes

- Keep changes focused and scoped to one concern per PR.
- Follow existing code style and conventions (ruff handles most of this).
- Add or update tests for any behavior change.
- Run `uv run pytest` and `uv run ruff check .` before pushing.

## Glossary

Some terms you'll see throughout the codebase:

| Term            | What it means |
|-----------------|---------------|
| **Regulations.gov** | The U.S. federal government's public docket portal. Our upstream data source. |
| **Mirrulations** | A public mirror of regulations.gov data hosted on AWS S3 ([repo](https://github.com/MoravianUniversity/mirrulations)). We read from it instead of hitting the regulations.gov API directly. |
| **R2**          | Cloudflare R2 — S3-compatible object storage. We use it as our processed-data store. |
| **S3**          | Amazon S3 — where Mirrulations hosts the raw upstream data. |
| **Staging files** | Intermediate per-agency files written during a pipeline run, before final merge/partition. |
| **Manifest**    | A small file listing already-processed source keys, used to make ETL runs incremental. |
| **Bulk transforms** | Whole-dataset operations (dedup, partition, summarize) that need every row at once. Live in `transforms/merge.py` as plain functions. |
| **Agency**      | A federal agency (EPA, DOL, etc.). Pipelines fan out per-agency. |

## Where things live

```
src/spicy_regs/
├── cli.py                # main CLI entrypoint (`spicy-regs` script)
├── schemas/              # RecordType definitions — one per data shape
├── sources/              # Reader and Writer subclasses (S3, R2, parquet, …)
├── transforms/           # Transform subclasses + bulk-transform helpers
├── pipelines/            # Pipeline subclasses + the `run-pipeline` CLI app
├── pipeline/             # (alternate pipeline framework — see its README)
├── manifest.py           # tracks already-processed keys for incremental runs
├── mcp_server.py         # stdio MCP server (`spicy-regs-mcp` script)
└── vectordb/             # embedding pipeline (optional `embed` extra)

mcp-server/               # standalone HTTP MCP server (Vercel-deployable)
notebooks/                # exploratory Jupyter notebooks (Binder-runnable)
sample-data/              # tiny fixtures used by tests
scripts/                  # one-off operational scripts
tests/                    # pytest suite + shared fixtures in conftest.py
plugins/                  # Claude Code plugin packaging
```

The CLI entrypoints are defined in `pyproject.toml` under `[project.scripts]`:
- `spicy-regs` — main CLI
- `run-pipeline` / `etl` — pipeline runners
- `spicy-regs-mcp` — stdio MCP server
- `embed` — vector embedding (requires the `embed` extra)

## Architecture: the ETL building blocks

The ETL is built from a few small, composable abstractions. Data flows through
them in one direction:

```
Reader  ->  Transform  ->  Writer        (a record at a time)
   \____________ Pipeline ____________/   (wires them together + runs)
```

| Concept       | Base class                              | What it does                                  | Example |
|---------------|-----------------------------------------|-----------------------------------------------|---------|
| `RecordType`  | `spicy_regs.schemas` (a value, not a subclass) | Describes one data shape: schema, primary key, extractor | `schemas/regulations.py` |
| `Reader`      | `spicy_regs.sources.base.Reader`        | A **source**: yields raw records              | `sources/mirrulations.py` |
| `Transform`   | `spicy_regs.transforms.base.Transform`  | Maps a record stream → record stream          | `transforms/extract.py` |
| `Writer`      | `spicy_regs.sources.base.Writer`        | A **sink**: persists records                  | `sources/parquet.py` |
| `Pipeline`    | `spicy_regs.pipelines.base.Pipeline`    | Composes the above and exposes `run()`        | `pipelines/regulations.py` |

Start with **`tests/test_example_pipeline.py`** — a runnable, minimal pipeline
with one of each base class wired together. Copy it.

Supporting pieces a pipeline can reuse:

- `pipelines/staging.py` — `stage_agencies(...)` fans agencies out in parallel,
  pumping each `Reader → Transform → Writer`.
- `manifest.py` — `Manifest` tracks already-processed keys for incremental runs.
- `sources/r2.py` — Cloudflare R2 download/upload helpers.

**Two kinds of "transform."** A `Transform` shapes records one at a time
(`apply(records) -> records`). Whole-dataset operations that need every row at
once — deduplicating, partitioning, summaries — are *bulk* transforms and live
in `transforms/merge.py`; they are functions, not `Transform` subclasses.

**Convention:** `Reader`, `Writer`, and `Transform` are classes you subclass.
Plain helper modules (e.g. `sources/r2.py`) are storage/connection utilities,
not connectors — don't subclass them.

**Experimental: Apache Beam.** A proof-of-concept exploring whether these same
building blocks could be expressed with Apache Beam lives in
`src/spicy_regs/beam/` (behind the optional `beam` extra). See its
[evaluation README](src/spicy_regs/beam/README.md) for how the model maps onto
Beam and why it's not adopted in production today.

### Recipes

- **Add a record shape:** construct a new `RecordType` in `schemas/` (set
  `path_pattern` only if your source addresses files by path).
- **Add a source:** subclass `Reader`, implement `iter_records()` to yield raw
  payloads (keep extraction out of the reader). See `sources/mirrulations.py`.
- **Add a transform:** subclass `Transform`, implement `apply()`. See
  `transforms/extract.py`.
- **Add a sink:** subclass `Writer`, implement `write()`. See `sources/parquet.py`.
- **Add a pipeline:** subclass `Pipeline`, set a `name`, compose your pieces in
  `run()`. Register it for the CLI in `pipelines/regulations.py`'s `app` if it
  should be runnable via `run-pipeline`.

## Commit messages

- Use concise, imperative subject lines (e.g. `fix: handle empty batch`).
- Reference issues where relevant.

## Pull requests

- Describe the problem and the solution.
- Include a test plan.
- Ensure CI passes before requesting review.

## Reporting issues

Open a GitHub issue with steps to reproduce, expected vs. actual behavior, and environment details. Pick a template from [the issue chooser](https://github.com/civictechdc/spicy-regs/issues/new/choose).
