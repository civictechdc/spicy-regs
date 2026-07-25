# Graph-engine bake-off — evidence (2026-07-24)

> **Status:** Historical, reproducible spike evidence. The source generation
> predates the RIN agenda-item revision. Keep these files as evidence for the
> recorded carrier decision; rerun the extraction against
> `regulatory_agenda_items` and `agenda_item_proceedings` before reporting
> current real-graph counts.

Reproducible spike comparing three ways to serve typed graph traversal
(RULE-026 deterministic paths, RULE-027 uncited-but-related discovery) over the
Spicy Regs Parquet carrier:

- **plain DuckDB 1.4.4** recursive CTE (the project's existing engine);
- **DuckPGQ** SQL/PGQ community extension, loaded into that same DuckDB;
- **Kuzu 0.11.3**, a separate embedded property-graph engine, in an isolated venv.

Decision that consumes this evidence:
`docs/superpowers/specs/2026-07-24-graph-engine-carrier-decision.md`.

## Files

| File | What it does |
| --- | --- |
| `extract_edges.py` | Extracts a normalized typed graph (RIN→CFR, RIN→USC, Proceeding→RIN, Subject→Concept) from a real generation; both engines consume identical Parquet. |
| `bench_duck.py` | Real corpus: Q_A fan-out (plain SQL vs DuckPGQ fixed-length) + Q_B variable-length (recursive CTE). |
| `probe_duckpgq_varlen.py` | Isolates DuckPGQ path-pattern support, one fresh connection per case. |
| `bench_kuzu.py` | Real corpus: Q_A + Q_B in Cypher; reports COPY load cost. |
| `gen_scale.py` | Generates clustered synthetic RIN↔CFR graphs at 1k / 10k / 100k / 1M edges. |
| `scale_duck.py` / `scale_kuzu.py` | Variable-length reachability across scales. |
| `results.txt` | Captured console output of a full run (numbers cited in the decision). |

## Reproduce

```bash
# from repo root; project env already has duckdb 1.4.4 + can INSTALL duckpgq
uv run python docs/evidence/graph-engine-bakeoff-2026-07-24/extract_edges.py \
  output/mixed-real-data-all-profile-openai-v1
uv run python docs/evidence/graph-engine-bakeoff-2026-07-24/bench_duck.py
uv run python docs/evidence/graph-engine-bakeoff-2026-07-24/probe_duckpgq_varlen.py
uv run python docs/evidence/graph-engine-bakeoff-2026-07-24/gen_scale.py
uv run python docs/evidence/graph-engine-bakeoff-2026-07-24/scale_duck.py

# Kuzu is NOT a project dependency — install it in a throwaway venv:
uv venv /tmp/kuzu-venv && uv pip install --python /tmp/kuzu-venv/bin/python kuzu duckdb pyarrow
/tmp/kuzu-venv/bin/python docs/evidence/graph-engine-bakeoff-2026-07-24/bench_kuzu.py
/tmp/kuzu-venv/bin/python docs/evidence/graph-engine-bakeoff-2026-07-24/scale_kuzu.py
```

Generated `graph/`, `scale/`, and Kuzu databases are `.gitignore`d — only the
scripts and captured `results.txt` are tracked. The scripts regenerate them.

## Headline numbers (see `results.txt`)

- **Correctness parity** where every engine runs: Q_A → 4 RIN hits; Q_B → 3
  uncited-but-related RINs (identical set); scaling → 22 RINs reached at every
  size.
- **DuckPGQ variable-length is broken** on this build: fixed-length `MATCH`
  works, but quantified `{lo,hi}` paths raise `INTERNAL Error: Attempted to
  access index 22 within vector of size 22` and invalidate the connection.
- **Plain DuckDB stays interactive to 1M edges**: 3-hop reachability in ~9.6 ms
  with no new infrastructure and no separate load step.
- **Kuzu's query latency is flat** (~2 ms at 1M edges) but carries a per-generation
  build cost (~37–221 ms in-memory) and a second engine to keep in sync.
