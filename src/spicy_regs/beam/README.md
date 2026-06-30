# Apache Beam — evaluation & proof-of-concept

This module is an **exploration**, not part of the production ETL. It answers a
single question: *would Apache Beam be a good way to define this project's
pipelines?* It does so with a small runnable spike plus the honest write-up
below.

Everything here lives behind the optional `beam` extra so apache-beam's heavy
transitive deps never touch the core install:

```bash
uv sync --extra beam
uv run run-beam-example                 # run the POC on the DirectRunner
uv run pytest tests/test_beam_pipeline.py -v
```

## How the existing model maps onto Beam

The project's `Reader → Transform → Writer` model (see the
[Architecture section in CONTRIBUTING.md](../../../CONTRIBUTING.md#architecture-the-etl-building-blocks))
maps onto Beam almost one-to-one. The adapters in [`adapters.py`](./adapters.py)
are the whole bridge:

| Current abstraction | Beam equivalent | Adapter |
|---|---|---|
| `Reader.iter_records()` — bounded iterable of dicts | bounded source via `beam.Create(...)` | `ReadWith` |
| `Transform.apply(records) -> records` — one record at a time | `beam.ParDo` / `beam.FlatMap` | `ApplyTransform` |
| bulk dedup/merge — today in DuckDB | `beam.GroupByKey` (+ pick-latest) / `CombinePerKey` | `DedupBy` |
| `Writer.write(records)` | a sink `beam.DoFn` (buffer in bundle, flush in `finish_bundle`) | `WriteWith` |
| `Pipeline.run()` | `beam.Pipeline(runner="DirectRunner")` + `p.run()` | `BeamExamplePipeline` |

The key insight: the streaming `Transform` contract (`apply` over a
one-record-at-a-time stream) is an almost exact match for `ParDo`, and the
*bulk* dedup that today needs DuckDB — the step deliberately kept out of the
`Transform` contract — becomes a native `GroupByKey`. The runnable proof is
[`example_pipeline.py`](./example_pipeline.py): it reuses the real `Pipeline`
base and the real `ExtractRecords` transform, adds an example transform and a
whole-dataset `DedupBy`, and runs the lot on the DirectRunner.

## What Beam would buy us

- **One model for batch *and* stream.** If ingestion ever becomes
  continuous (tailing new regulations.gov activity instead of daily batch),
  the same transforms run unchanged with windowing and triggers on top.
- **Native shuffle for the merge.** Dedup/merge/partition — the columnar
  whole-dataset work currently hand-rolled in DuckDB — are exactly what
  `GroupByKey` / `CombinePerKey` exist for.
- **Horizontal scale via a real runner.** The same pipeline can run on
  Dataflow, Flink, or Spark with autoscaling and exactly-once semantics — no
  rewrite, just a runner flag.
- **Built-in IO connectors** (`fileio`, Parquet, S3) and a large transform
  library, instead of bespoke reader/writer code.

## What it would cost

- **Heavy, tightly-pinned dependencies.** apache-beam pins specific versions of
  `pyarrow`, `numpy`, `protobuf`, and `dill` that can clash with this project's
  `polars` / `duckdb` / `pyarrow` stack. That risk is the entire reason this is
  an isolated optional extra rather than a core dep — note the resolver output
  when you `uv sync --extra beam`.
- **The DirectRunner is not a production engine.** It's slow and memory-hungry
  and exists for local development/testing. Real benefit needs **Dataflow** (a
  GCP project + billing + IAM) or a self-hosted **Flink/Spark** cluster — a
  meaningful infra and cost step up from today's *free* GitHub Actions runners.
  (Even running locally has sharp edges: Beam 2.72's default `DirectRunner` now
  shells out to a platform-specific Prism binary, so the POC pins the in-process
  `BundleBasedDirectRunner` instead.)
- **The workload doesn't need it yet.** Per-agency work is embarrassingly
  parallel and already handled by a `ThreadPoolExecutor` fan-out
  (`pipelines/staging.py`) batched across the day by cron
  (`etl-new-pipeline.yml`). DuckDB already does the merge/dedup out-of-core in a
  single process. Comments are already Hive-partitioned.
- **The incremental manifest doesn't map cleanly.** The Bloom-filter
  processed-key manifest (`manifest.py`) is a custom stateful pattern that a
  Beam *batch* pipeline doesn't model naturally.
- **It cuts against the project's grain.** The team already *removed* a Prefect
  orchestrator in favor of "small composable building blocks." Adding a large
  framework reverses that deliberate choice.
- **Type checking.** apache-beam ships without complete type stubs, so the
  `beam` module and its test are excluded from `ty` (see `[tool.ty.src]` in
  `pyproject.toml`). That's an accepted gap for an experimental module.

## Recommendation

The model maps cleanly and this spike proves it runs on the DirectRunner — but
**adopting Beam in production is not justified at the current scale.** The
existing polars/duckdb pipeline is simpler, lighter, free to run, and a good fit
for a daily, embarrassingly-parallel batch job. Keep Beam as this optional,
experimental path.

**Revisit when any of these become true:**

1. **Streaming ingestion** is needed (continuous regulations.gov updates rather
   than daily batch).
2. The dataset **outgrows a single GitHub Actions runner** and a managed runner
   (Dataflow) is worth its cost and operational overhead.
3. The **merge needs distributed shuffle** beyond what DuckDB handles in one
   process.

## Files

- [`adapters.py`](./adapters.py) — the `Reader`/`Transform`/`Writer` ↔ Beam bridge.
- [`example_pipeline.py`](./example_pipeline.py) — the runnable DirectRunner POC (`run-beam-example`).
- [`../../../tests/test_beam_pipeline.py`](../../../tests/test_beam_pipeline.py) — hermetic test, skipped unless the `beam` extra is installed.

### Possible follow-up (not built here)

If the team wants the spike continuously verified, add an opt-in CI job that
mirrors `integration.yml`: install the extra (`uv sync --extra beam`) and run
`pytest tests/test_beam_pipeline.py`. It's left out by default to keep
apache-beam off the critical CI path.
