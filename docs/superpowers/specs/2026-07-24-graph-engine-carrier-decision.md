# Decision: Serve graph traversal from DuckDB now; hold Kuzu as a deferred projection

- **Date:** 2026-07-24
- **Status:** Recorded — spike result; no new engine adopted
- **Scope:** Spicy Regs relationship indexing, traversal, and retrieval serving
- **Backlog reference:** `TODO-RULE.md` — `RULE-011` (physical projection ADR),
  `RULE-026` (direct/deterministic relationship index), `RULE-027`
  (uncited-but-related discovery)
- **Evidence:** `docs/evidence/graph-engine-bakeoff-2026-07-24/`

> **Evidence scope:** The real-graph extraction below predates the RIN
> agenda-item revision. Its synthetic scale test through one million edges still
> supports the carrier decision, but the real extraction must be rerun against
> the current `regulatory_agenda_items` and `agenda_item_proceedings` surfaces
> before its small real-graph counts are cited as current.

## Question

The relationship traversal in `RULE-026`/`RULE-027` is graph-shaped: typed
multi-edge paths through RIN, CFR unit, statute, docket, and proceeding, plus
"related but uncited" discovery over shared endpoints. Does this need a
dedicated graph database (e.g. Kuzu) alongside the Parquet carrier, or can the
existing DuckDB engine serve it?

## Decision

Serve graph traversal from the **existing DuckDB engine over published
Parquet** for the current program. Do not add a second query engine now.

- **Direct and deterministic paths (`RULE-026`)** and the bounded-neighborhood
  part of **uncited-but-related discovery (`RULE-027`)** use DuckDB recursive
  CTEs over the published relationship Parquet. No new runtime, no separate
  load step, no cross-engine sync.
- **DuckPGQ (SQL/PGQ) is admissible only for fixed-length patterns.** Its
  variable-length path operator is currently broken (see backlog note), so it
  must not be relied on for recursive discovery.
- **Kuzu remains a deferred projection option**, not an adoption. It is
  reconsidered only when a concrete query demonstrates that DuckDB traversal is
  inadequate — matching the standing guardrail that vocabulary and machinery are
  added only after a real corpus and a concrete query demonstrate the need.

Any future graph store stays a **rebuildable index off the manifest-addressed
generation**, never a source of truth. The specialized Parquet tables remain
authoritative, consistent with `RULE-011`.

## Evidence

Bake-off over a real generation (`mixed-real-data-all-profile-openai-v1`): a
normalized typed graph of 20 RINs, 23 CFR units, 32 USC sections, 35 target
edges, 34 authority edges. Both engines consumed identical extracted Parquet.
Full output in `docs/evidence/graph-engine-bakeoff-2026-07-24/results.txt`.

**Correctness parity** wherever an engine ran:

| Query | DuckDB | DuckPGQ | Kuzu |
| --- | --- | --- | --- |
| Q_A — artifacts touching a CFR unit | 4 RINs | 4 RINs | 4 RINs |
| Q_B — uncited-but-related RINs (2–3 hops) | 3 RINs | crash | 3 RINs |

**Scaling** — variable-length 3-hop reachability on clustered synthetic graphs
(answer is 22 RINs at every size; median query time):

| Edges | DuckDB recursive CTE | Kuzu native | Kuzu build/gen |
| --- | --- | --- | --- |
| 1k | 1.5 ms | 1.3 ms | 37 ms |
| 10k | 2.1 ms | 1.7 ms | 29 ms |
| 100k | 6.1 ms | 2.3 ms | 65 ms |
| 1M | 9.6 ms | 2.1 ms | 221 ms |

DuckDB's "load" is reading the Parquet it already publishes (2–28 ms). Kuzu's
query latency is effectively flat because traversal touches only the local
neighborhood, but it pays a per-generation build cost and adds a second engine.

## Why this carrier

- At the measured pre-revision real-corpus scale (tens of edges), every engine
  answered in well under a millisecond; Kuzu's build cost alone dwarfed any
  query saving. These counts are historical, not the current graph size.
- Even at **1M edges — ~10,000× the current corpus** — DuckDB answers 3-hop
  discovery in under 10 ms, comfortably interactive. The full US regulatory edge
  count is plausibly reached long before DuckDB stops being adequate.
- Kuzu's structural advantage (size-independent traversal, one-line variable-length
  Cypher versus a recursive CTE) is real but bites only at graph sizes and
  traversal depths beyond this program's near-term horizon, and only justifies a
  second engine once a query proves it.
- Keeping traversal in DuckDB preserves the guardrail that ordinary Spicy Regs
  queries stay independent of external runtimes and that the query contract stays
  independent of physical layout.

## Consequences

- `RULE-026`/`RULE-027` implement traversal as recursive CTEs (and DuckPGQ
  fixed-length patterns where clearer) over the published relationship Parquet;
  no dependency is added to `pyproject.toml`.
- The bake-off harness in `docs/evidence/graph-engine-bakeoff-2026-07-24/` is
  retained as the re-runnable trigger: the day a `RULE-027` query is awkward or
  slow in DuckDB, re-run it to produce the concrete evidence that would justify
  adopting Kuzu as a rebuildable projection.
- If Kuzu is later adopted, it is published atomically with the semantic
  generation and rebuilt from the manifest digest, never queried as truth
  (`RULE-011`, `RULE-026`).

## Backlog / follow-ups

### DuckPGQ variable-length traversal crashes (RULE-006 triage)

- **Finding.** On `duckdb 1.4.4` with the `duckpgq` community extension,
  fixed-length `MATCH` works, but a quantified variable-length path
  (`(a)-[t:Targets]->{1,6}(b)`) raises
  `INTERNAL Error: Attempted to access index 22 within vector of size 22` and
  invalidates the DuckDB connection. The undirected form raises a
  `Non-existent/non-unique vertices` constraint error. Reproduced with one fresh
  connection per case in `probe_duckpgq_varlen.py`.
- **Observed vs expected.** Expected: a bounded variable-length path returns the
  reachable set (as the recursive CTE and Kuzu both do — 3 RINs). Observed: an
  internal engine error that also poisons the connection for later queries.
- **Impact / workaround.** Blocks using DuckPGQ for the recursive
  `RULE-027` case. Workaround in place: recursive CTEs cover variable-length
  traversal; DuckPGQ is restricted to fixed-length patterns.
- **Triage classification: Spicy Regs local — carrier/serving.** Per the
  cross-repository execution rule this is query-engine behavior, which Spicy Regs
  owns; it is not a reusable Rulespec semantic, identifier, conformance,
  constraint, projector, registry, SDK, or runtime gap. `../rulespec/TODO.md` was
  searched and contains no related item; **do not** add one there. The genuine
  upstream is the DuckPGQ project; file an issue there if/when DuckPGQ adoption
  is pursued.
- **Acceptance criteria (if revisited).** A bounded variable-length
  `MATCH ... {1,k}` over the RIN↔CFR property graph returns the same reachable
  RIN set as the recursive CTE, without an internal error and without
  invalidating the connection, on the pinned DuckDB version.
