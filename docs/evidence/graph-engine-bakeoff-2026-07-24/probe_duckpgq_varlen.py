"""Isolate DuckPGQ path-pattern support on duckdb 1.4.4 + duckpgq (community).

Each candidate runs in a FRESH connection because a failing variable-length
query raises an INTERNAL error that invalidates the DuckDB connection, which
would otherwise cascade into false failures for later candidates.

Finding: fixed-length MATCH works; quantified variable-length {lo,hi} crashes.
"""
from __future__ import annotations
import os, duckdb

G = os.path.join(os.path.dirname(__file__), "graph")

def fresh():
    con = duckdb.connect(); con.execute("INSTALL duckpgq FROM community; LOAD duckpgq;")
    for t in ["node_rin", "node_cfr", "edge_targets"]:
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM read_parquet('{G}/{t}.parquet')")
    con.execute("""CREATE PROPERTY GRAPH g
      VERTEX TABLES (node_rin LABEL Rin, node_cfr LABEL Cfr)
      EDGE TABLES (edge_targets SOURCE KEY (rin) REFERENCES node_rin (id)
         DESTINATION KEY (cfr) REFERENCES node_cfr (id) LABEL Targets);""")
    return con

seed = fresh().execute(
    "SELECT rin FROM edge_targets GROUP BY rin ORDER BY count(*) DESC LIMIT 1").fetchone()[0]
print(f"duckpgq path-pattern probe (seed RIN={seed!r})\n")

cands = [
    ("fixed 2-hop  (a)-[T]->(Cfr)<-[T]-(b)",
     f"SELECT DISTINCT b FROM GRAPH_TABLE(g MATCH (a:Rin WHERE a.id='{seed}')"
     f"-[t:Targets]->(c:Cfr)<-[t2:Targets]-(b:Rin) COLUMNS(b.id AS b))"),
    ("var-length   (a)-[T]->{1,6}(b)  DIRECTED",
     f"SELECT DISTINCT b FROM GRAPH_TABLE(g MATCH p=ANY SHORTEST (a:Rin WHERE a.id='{seed}')"
     f"-[t:Targets]->{{1,6}}(b:Rin) COLUMNS(b.id AS b))"),
    ("var-length   (a)-[T]-{2,6}(b)   UNDIRECTED",
     f"SELECT DISTINCT b FROM GRAPH_TABLE(g MATCH p=ANY SHORTEST (a:Rin WHERE a.id='{seed}')"
     f"-[t:Targets]-{{2,6}}(b:Rin) COLUMNS(b.id AS b))"),
]
for name, sql in cands:
    try:
        con = fresh(); r = con.execute(sql).fetchall()
        print(f"  OK    {name}: {len(r)} rows")
    except Exception as e:
        print(f"  FAIL  {name}: {type(e).__name__}: {repr(e)[:70]}")
