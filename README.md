# Spicy Regs

SpicyRegs captures public regulatory sources and publishes immutable source
records, exact document versions, Unicode text representations, structural
passages, source observations, verified links, and acquisition coverage. It
provides the source facts that downstream products can reproduce and audit.

## Product boundary

SpicyRegs owns source acquisition and source-addressable document structure.
It does not own managed vocabulary policy, extracted semantic assertions, or
search ranking and serving:

- **RefSpec** owns vocabulary releases, concepts, labels, mappings, redirects,
  and explicit resolution of source terms.
- **Rulespec Core** owns portable evidence and semantic record shapes;
  **Rulespec Extrapolator** owns candidate extraction and validation.
- **SpicySearch** owns query planning, document retrieval, ranking,
  explanations, search receipts, indexes, and query-time coverage.

Build a release from the checked-in Regulations.gov JSON record and its exact
four-page PDF:

```bash
uv run --frozen build-document-release-from-files \
  --manifest sample-data/mirrulations/document-release-file-manifest-v1.json \
  --output-dir ./output/mirrulations-document-release
```

The command verifies both source-file digests, extracts embedded PDF text,
creates page-derived Unicode passages, validates the release, and writes a
source-complete distribution. `document-release.json` points to
content-addressed copies of the exact JSON and PDF bytes under `renditions/`
and to the captured-file manifest under `receipts/`. The Rulespec Core release
is a pinned dependency, not a copied file in this distribution; a validator
must receive the matching Core file through `--rulespec-core`. The repository
default is a fixture, so this command produces a `conformance` release rather
than production evidence. The source-byte closure check is also available as a
separate command:

```bash
uv run --frozen validate-document-release-distribution \
  --distribution ./output/mirrulations-document-release
```

The same publication path handles exact source-native HTML and XML. This
checked representative contains one congressional bill and one Code of Federal
Regulations section:

```bash
uv run --frozen build-document-release-from-files \
  --manifest sample-data/document-files/document-release-representative-manifest-v1.json \
  --output-dir output/markup-document-release
```

The local 34-document evaluation cache exercises PDF, HTML, and XML across
seven source families and four size bands. It remains evaluation input: its
lock refers to code-defined source specifications and lacks complete
source-issued version metadata, so the publication command does not accept it.
This actual-file release path claims only embedded-text PDF and UTF-8 HTML/XML.
Scanned PDFs without embedded text fail closed; it does not claim optical
character recognition or Office-document support. HTML semantic isolation
recognizes a literal single `<main>` plus `<title>`; publisher layouts that use
another main-content convention need a source-specific capture adapter before
publication. Malformed HTML that depends on HTML5 implicit tag closing is also
outside this conformance slice. PDF parsing currently runs in process, so this
command is for controlled, digest-pinned capture jobs rather than arbitrary
user uploads; production intake still needs resource limits and process
isolation.

The synthetic M1 builder remains a small conformance fixture:

```bash
uv run build-document-release --output ./output/document-release-m1.json
```

It reads repository-local, digest-pinned source and Rulespec Core fixtures and
rejects any invalid digest, coordinate, classification, projection, or
reference. It is not evidence that acquired source files were processed.

The checked-in M1 release is
`src/spicy_regs/fixtures/spicyregs-m1-document-release-v1.json`; consumers pin
its `release_id` and `release_digest`, not a source-tree path.

## Quickstart

Prerequisites: Python 3.10+ and [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
git clone --recurse-submodules https://github.com/civictechdc/spicy-regs.git
cd spicy-regs
uv sync                       # install dependencies into a .venv
uv run pytest                 # run the test suite
uv run ruff check .           # lint
```

You don't need any credentials to run the tests, download the published
parquet files, or run the pipeline against the public Mirrulations mirror. A
`.env` file (copy `.env.example` to `.env`) is only required if you want to
upload your output to live Cloudflare R2 storage.

## Historical managed-vocabulary incubation

This repository incubated managed-vocabulary and search experiments before the
four-product boundary above. That evidence remains useful for migration, but
it is not SpicyRegs runtime authority. RefSpec now owns the managed vocabulary
capability and SpicySearch owns its search read models. The historical
[active roadmap](RefSpec/plans/managed-vocabulary-experiment-roadmap.md)
records the evidence and remaining decisions.

This proves the specification and lookup mechanics against real sources. It
does not claim product accuracy, a sealed holdout, production deployment, or
real cross-scheme mapping; the selected native sources contain no authored
SKOS mapping assertions.

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

The current `spicy-regs search` command is a legacy exploratory surface. It
still searches dockets and comments and remains available only while its
consumers migrate; it is not the document-only SpicySearch API.

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

### Backfill comment text for already-published data

The ETL fills `text_content` inline only for comments it processes fresh — the
incremental manifest skips comments ingested before that, so they stay `NULL`.
To backfill the existing dataset from Mirrulations' pre-extracted text (reading
straight from the bucket's `derived-data` prefix — no PDF download, no JSON
re-ingest), download the comments and run:

```bash
uv run spicy-regs download --types comments     # grab the published parquet
uv run backfill-comment-text                     # fill text_content in place
uv run backfill-comment-text --limit 5000        # cap work for a trial run
uv run backfill-comment-text --upload            # also republish to R2 (needs credentials)
```

It is incremental and re-runnable (rows that already have a
`text_extraction_status` are skipped unless you pass `--overwrite`). Document
text isn't published to `derived-data`; backfill those via
`uv run enrich-pdf-text --target documents`.

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
