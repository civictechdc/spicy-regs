"""Kuzu native variable-length reachability across scales (same edges/parquet)."""
import os, statistics, time, kuzu
S = os.path.join(os.path.dirname(__file__), "scale")
print(f"Kuzu {kuzu.__version__} native *variable-length reachability (seed R0, 3 co-target hops)")
print(f"{'scale':>6} {'load_ms':>9} {'query_ms(med)':>14} {'RINs_reached':>13}")
def rows(res):
    o=[]
    while res.has_next(): o.append(res.get_next())
    return o
for tag in ("1k","10k","100k","1M"):
    t=time.perf_counter()
    db=kuzu.Database(":memory:"); conn=kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE Rin(id STRING, PRIMARY KEY(id))")
    conn.execute("CREATE NODE TABLE Cfr(id STRING, PRIMARY KEY(id))")
    conn.execute("CREATE REL TABLE Targets(FROM Rin TO Cfr)")
    # distinct node loads
    conn.execute(f"COPY Rin FROM (LOAD FROM '{S}/edges_{tag}.parquet' RETURN DISTINCT rin)")
    conn.execute(f"COPY Cfr FROM (LOAD FROM '{S}/edges_{tag}.parquet' RETURN DISTINCT cfr)")
    conn.execute(f"COPY Targets FROM '{S}/edges_{tag}.parquet'")
    load=(time.perf_counter()-t)*1000
    q="MATCH (a:Rin {id:'R0'})-[:Targets*2..6]-(b:Rin) RETURN count(DISTINCT b.id)"
    n=rows(conn.execute(q))[0][0]  # warm+answer
    reps = 20 if tag in ("1k","10k") else (8 if tag=="100k" else 3)
    ts=[]
    for _ in range(reps):
        t=time.perf_counter(); rows(conn.execute(q)); ts.append((time.perf_counter()-t)*1000)
    print(f"{tag:>6} {load:>9.1f} {statistics.median(ts):>14.2f} {n:>13,}")
    conn.close(); db.close()
