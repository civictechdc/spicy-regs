"""DuckDB (plain SQL) vs DuckPGQ (SQL/PGQ) on the real spicy-regs graph.

Q_A  heterogeneous fan-out: everything touching a CFR unit  (RULE-028 #1)
Q_B  variable-length reachability: uncited-but-related RINs (RULE-027 / RULE-028 #2)
"""
from __future__ import annotations
import os, statistics, time
import duckdb

G = os.path.join(os.path.dirname(__file__), "graph")
con = duckdb.connect()
con.execute("INSTALL duckpgq FROM community; LOAD duckpgq;")

# load into base tables (DuckPGQ needs base tables, not read_parquet inline)
for t in ["node_rin","node_cfr","node_usc","node_proc","node_subject","node_concept",
          "edge_targets","edge_authority","edge_proc_rin","edge_about"]:
    con.execute(f"CREATE TABLE {t} AS SELECT * FROM read_parquet('{G}/{t}.parquet')")

# undirected RIN-CFR bipartite edge list, both directions, for reachability
con.execute("""
CREATE TABLE bip AS
  SELECT rin AS src, cfr AS dst FROM edge_targets
  UNION
  SELECT cfr AS src, rin AS dst FROM edge_targets
""")

def timed(fn, reps=200):
    fn()  # warm
    ts=[]
    for _ in range(reps):
        t=time.perf_counter(); fn(); ts.append((time.perf_counter()-t)*1000)
    return statistics.median(ts)

# pick a well-connected seed CFR + seed RIN
SEED_CFR = con.execute("SELECT cfr FROM edge_targets GROUP BY cfr ORDER BY count(*) DESC LIMIT 1").fetchone()[0]
SEED_RIN = con.execute("SELECT rin FROM edge_targets GROUP BY rin ORDER BY count(*) DESC LIMIT 1").fetchone()[0]
print(f"seed CFR={SEED_CFR!r}  seed RIN={SEED_RIN!r}\n")

# ---------- Q_A: everything touching a CFR unit ----------
def qa_sql():
    return con.execute("""
      WITH hit AS (SELECT rin FROM edge_targets WHERE cfr = ?)
      SELECT 'rin' k, rin id FROM hit
      UNION ALL SELECT 'proceeding', proceeding FROM edge_proc_rin WHERE rin IN (SELECT rin FROM hit)
      UNION ALL SELECT 'usc', usc FROM edge_authority WHERE rin IN (SELECT rin FROM hit)
    """, [SEED_CFR]).fetchall()

def qa_pgq():
    # fixed 2-hop through the CFR unit, then union the auth/proc hops in SQL
    return con.execute(f"""
      SELECT DISTINCT rin_id FROM GRAPH_TABLE (g
        MATCH (c:node_cfr WHERE c.id = '{SEED_CFR}')<-[t:edge_targets]-(r:node_rin)
        COLUMNS (r.id AS rin_id))
    """).fetchall()

# ---------- Q_B: variable-length reachability (uncited-but-related) ----------
def qb_recursive(depth):
    def run():
        return con.execute("""
          WITH RECURSIVE reach(node, d) AS (
            SELECT ? , 0
            UNION
            SELECT b.dst, r.d+1 FROM reach r JOIN bip b ON b.src = r.node
            WHERE r.d < ?
          )
          SELECT DISTINCT node FROM reach
          WHERE node IN (SELECT id FROM node_rin) AND node <> ?
        """, [SEED_RIN, depth, SEED_RIN]).fetchall()
    return run

# DuckPGQ variable-length is covered by probe_duckpgq_varlen.py (it crashes the
# connection), so Q_B here compares only recursive-CTE — the working in-DuckDB path.

# build property graph
con.execute("""
CREATE PROPERTY GRAPH g
VERTEX TABLES (node_rin LABEL node_rin, node_cfr LABEL node_cfr, node_usc LABEL node_usc)
EDGE TABLES (
  edge_targets SOURCE KEY (rin) REFERENCES node_rin (id)
               DESTINATION KEY (cfr) REFERENCES node_cfr (id) LABEL edge_targets
);
""")

print("Q_A  everything touching a CFR unit")
a1 = qa_sql(); a2 = qa_pgq()
rins_sql = sorted({r[1] for r in a1 if r[0]=='rin'})
rins_pgq = sorted({r[0] for r in a2})
print(f"  plain SQL : {len(a1):>3d} rows total ({len(rins_sql)} RIN hits)  {timed(lambda: qa_sql()):.3f} ms")
print(f"  DuckPGQ   : {len(a2):>3d} RIN hits                {timed(lambda: qa_pgq()):.3f} ms")
print(f"  RIN-hit parity: {rins_sql == rins_pgq}\n")

print("Q_B  variable-length reachability (uncited-but-related RINs) — recursive CTE")
print("     (DuckPGQ variable-length: see probe_duckpgq_varlen.py — it crashes)")
for depth in (1,2,3):
    r = qb_recursive(depth)
    rr = sorted({x[0] for x in r()})
    print(f"  {depth} co-target hop(s): recursiveCTE={len(rr):>2d} RINs ({timed(r):.3f} ms)")
