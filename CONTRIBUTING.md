# Contributing

Thanks for your interest in contributing to spicy-regs!

If you find this project useful, please consider giving it a star on GitHub — it helps others discover the project and motivates continued development.

## Getting started

1. Fork and clone the repo.
2. Create a branch: `git checkout -b my-change`.
3. Install dependencies and set up your environment (see `README.md`).

## Making changes

- Keep changes focused and scoped to one concern per PR.
- Follow existing code style and conventions.
- Add or update tests for any behavior change.
- Run the test suite locally before pushing.

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

Open a GitHub issue with steps to reproduce, expected vs. actual behavior, and environment details.
