"""Kuzu on the identical extracted graph. Measures load (ETL) + query cost,
and whether native variable-length traversal works where DuckPGQ crashed."""
from __future__ import annotations
import os, statistics, time
import kuzu

G = os.path.join(os.path.dirname(__file__), "graph")

t0 = time.perf_counter()
db = kuzu.Database(":memory:"); conn = kuzu.Connection(db)
conn.execute("CREATE NODE TABLE Rin(id STRING, kind STRING, PRIMARY KEY(id))")
conn.execute("CREATE NODE TABLE Cfr(id STRING, PRIMARY KEY(id))")
conn.execute("CREATE NODE TABLE Usc(id STRING, PRIMARY KEY(id))")
conn.execute("CREATE REL TABLE Targets(FROM Rin TO Cfr)")
conn.execute("CREATE REL TABLE HasAuthority(FROM Rin TO Usc)")
conn.execute(f"COPY Rin FROM '{G}/node_rin.parquet'")
conn.execute(f"COPY Cfr FROM '{G}/node_cfr.parquet'")
conn.execute(f"COPY Usc FROM '{G}/node_usc.parquet'")
conn.execute(f"COPY Targets FROM '{G}/edge_targets.parquet'")
conn.execute(f"COPY HasAuthority FROM '{G}/edge_authority.parquet'")
load_ms = (time.perf_counter() - t0) * 1000

def rows(res):
    out = []
    while res.has_next():
        out.append(res.get_next())
    return out

def timed(sql, reps=200):
    rows(conn.execute(sql))  # warm
    ts = []
    for _ in range(reps):
        t = time.perf_counter(); rows(conn.execute(sql)); ts.append((time.perf_counter()-t)*1000)
    return statistics.median(ts)

SEED_CFR = "5-890"; SEED_RIN = "3206-AO48"
print(f"Kuzu {kuzu.__version__}")
print(f"schema + COPY load (5 tables): {load_ms:.1f} ms\n")

# Q_A everything touching a CFR unit (RIN + its authorities)
qa = f"""MATCH (c:Cfr {{id:'{SEED_CFR}'}})<-[:Targets]-(r:Rin)
         OPTIONAL MATCH (r)-[:HasAuthority]->(u:Usc)
         RETURN DISTINCT r.id, u.id"""
ra = rows(conn.execute(qa))
print(f"Q_A everything touching CFR {SEED_CFR}: {len({x[0] for x in ra})} RIN hits  {timed(qa):.3f} ms")

# Q_B native variable-length reachability (the query DuckPGQ crashed on)
print("\nQ_B variable-length reachability (native Kuzu *hops):")
for depth in (1,2,3):
    hi = 2*depth
    q = f"""MATCH (a:Rin {{id:'{SEED_RIN}'}})-[:Targets*2..{hi}]-(b:Rin)
            WHERE b.id <> '{SEED_RIN}' RETURN DISTINCT b.id"""
    rb = rows(conn.execute(q))
    print(f"  depth {depth} (*2..{hi}): {len(rb):>2d} RINs  {timed(q):.3f} ms  -> {sorted(x[0] for x in rb)[:6]}")
