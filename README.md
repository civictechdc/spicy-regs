# Spicy Regs

Spicy Regs goal is to build an open, contributor-friendly platform for exploring and analyzing regulations.gov data, usable by both technical and non-technical users. The platform should enable rapid prototyping, reproducible analysis, and modular app extensions.

## Quickstart

Prerequisites: Python 3.10+ and [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
git clone https://github.com/civictechdc/spicy-regs.git
cd spicy-regs
uv sync                       # install dependencies into a .venv
uv run pytest                 # run the test suite
uv run ruff check .           # lint
```

You don't need any credentials to run the tests, download the published
parquet files, or run the pipeline against the public Mirrulations mirror. A
`.env` file (copy `.env.example` to `.env`) is only required if you want to
upload your output to live Cloudflare R2 storage.

### Download the published data locally

The processed dockets / documents / comments parquet files are published to a
public Cloudflare R2 bucket. Grab them with the bundled CLI — no credentials
needed:

```bash
uv run spicy-regs download                        # all three (dockets, documents, comments)
uv run spicy-regs download --types comments       # comments only
uv run spicy-regs download -o ./my-data           # custom output dir
```

Files land in `./spicy-regs-data/` by default. Once downloaded, poke around:

```bash
uv run spicy-regs stats                # row counts + top agencies per file
uv run spicy-regs sample comments -n 5 # 5 random rows from comments.parquet
uv run spicy-regs search "climate"     # substring search across files
uv run spicy-regs agencies             # list every agency code
```

> Don't have the repo cloned? You can also run it one-shot with
> `uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs download --types comments`.

### Run the ETL pipeline yourself

The pipeline reads raw JSON from the public Mirrulations S3 mirror, flattens
it, and writes Parquet to `./output/`. For a first run, scope it tight so it
finishes in minutes instead of hours:

```bash
# Smallest useful run: one agency, recent dockets, comments only, no upload.
uv run run-pipeline --agency EPA --only-comments --since-year 2025
```

What you get when it finishes:
- `output/comments.parquet` — the merged + deduplicated comments
- `output/manifest.json` — tracks already-processed source keys so the next
  run is incremental (delete it for a full refresh, or pass `--full-refresh`)

Other useful flags (see `uv run run-pipeline --help` for the full list):

| Flag                    | What it does                                              |
|-------------------------|-----------------------------------------------------------|
| `--agency EPA`          | Process a single agency instead of all of them            |
| `--since-year 2025`     | Skip dockets older than the given year                    |
| `--only-comments`       | Stage comments only (skip dockets + documents)            |
| `--skip-comments`       | Inverse — dockets + documents only (much faster)          |
| `--max-workers 8`       | Agencies processed in parallel (default 4)                |
| `--full-refresh`        | Ignore the existing manifest and rebuild from scratch     |
| `--no-skip-upload`      | Also publish to R2 (needs credentials in `.env`)          |
| `--no-enrich-text`      | Skip filling comment `text_content` from Mirrulations' pre-extracted attachment text |

Next steps:
- Open the runnable example pipeline at `tests/test_example_pipeline.py`.
- Read [CONTRIBUTING.md](CONTRIBUTING.md) for architecture and how to add your
  own reader / transform / writer / pipeline.

## Data dictionary

A full, column-by-column reference for every published table lives at the
[**Spicy Regs Data Dictionary**](https://civictechdc.github.io/spicy-regs/). It
is generated from the schema in this repo and kept in sync by CI, so it always
matches what's published to R2. To work on it locally:

```bash
uv run spicy-regs-dict check        # verify descriptions match the schema
uv run spicy-regs-dict generate     # regenerate docs/tables/*.md
uv run --group docs mkdocs serve    # preview the site at 127.0.0.1:8000
```

Edit descriptions in `data_dictionary/descriptions.yaml`.

## Open Example Notebooks under /notebooks

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/civictechdc/spicy-regs/HEAD)

## Use with Claude or any AI assistant

- **Claude Code plugin:** `/plugin marketplace add civictechdc/spicy-regs` then `/plugin install spicyregs@spicy-regs-local`. Bundles the skill in this repo. See `plugins/spicyregs/skills/spicyregs/SKILL.md`.
- **Claude Code via uvx (stdio MCP):** `claude mcp add spicy-regs -- uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs-mcp`. No deploy needed.
- **Claude.ai or any remote MCP client:** add `https://mcp.spicy-regs.dev/mcp` as a Custom Connector (hosted on Vercel from `mcp-server/`). See [`mcp-server/README.md`](mcp-server/README.md).
- **OpenAI / Cursor / Continue / other providers:** see [`plugins/spicyregs/INSTALL.md`](plugins/spicyregs/INSTALL.md) for the full install matrix across providers, including the provider-agnostic prompt fallback for assistants without MCP or skill support.

## Contributing

The ETL is built from small, composable building blocks (`Reader → Transform →
Writer`, wired by a `Pipeline`). See the [Architecture section in
CONTRIBUTING.md](CONTRIBUTING.md#architecture-the-etl-building-blocks) and the
runnable reference in `tests/test_example_pipeline.py` for how to add your own.

New contributors welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for the full
guide, including a glossary of terms and a map of where things live in the
repo.

## Contact us

Join our [slack channel](https://civictechdc.slack.com/archives/C09H576E6LU)!
Don't have access? Open a [GitHub issue](https://github.com/civictechdc/spicy-regs/issues/new/choose) and we'll get back to you.
