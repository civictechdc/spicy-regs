"""DuckDB recursive-CTE variable-length reachability across scales."""
import os, statistics, time, duckdb
S = os.path.join(os.path.dirname(__file__), "scale")
MAXD = 6  # bipartite hops = 3 RIN->RIN co-target hops
print("DuckDB recursive-CTE reachability (seed R0, up to 3 co-target hops)")
print(f"{'scale':>6} {'load_ms':>9} {'query_ms(med)':>14} {'RINs_reached':>13}")
for tag in ("1k","10k","100k","1M"):
    con = duckdb.connect()
    t=time.perf_counter()
    con.execute(f"CREATE TABLE e AS SELECT * FROM read_parquet('{S}/edges_{tag}.parquet')")
    con.execute("CREATE TABLE bip AS SELECT rin src,cfr dst FROM e UNION ALL SELECT cfr,rin FROM e")
    load=(time.perf_counter()-t)*1000
    sql=f"""WITH RECURSIVE reach(node,d) AS (
              SELECT 'R0',0 UNION
              SELECT b.dst,r.d+1 FROM reach r JOIN bip b ON b.src=r.node WHERE r.d<{MAXD})
            SELECT count(DISTINCT node) FROM reach WHERE node LIKE 'R%'"""
    n=con.execute(sql).fetchone()[0]  # warm + answer
    reps = 20 if tag in ("1k","10k") else (8 if tag=="100k" else 3)
    ts=[]
    for _ in range(reps):
        t=time.perf_counter(); con.execute(sql).fetchone(); ts.append((time.perf_counter()-t)*1000)
    print(f"{tag:>6} {load:>9.1f} {statistics.median(ts):>14.2f} {n:>13,}")
    con.close()
