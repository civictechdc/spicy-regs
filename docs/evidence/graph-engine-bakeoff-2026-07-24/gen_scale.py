"""Generate clustered bipartite RIN-CFR graphs (mimics rule_targets shape) at
several scales. Clustering keeps variable-length reachable sets non-trivial but
bounded (a 'rulemaking topic cluster'), like the real corpus."""
import os, duckdb
OUT = os.path.join(os.path.dirname(__file__), "scale")
os.makedirs(OUT, exist_ok=True)
con = duckdb.connect()
# K target-edges per RIN; RINs grouped into clusters of CSIZE sharing a CFR pool
K, CSIZE, CFR_PER_CLUSTER = 3, 200, 60
for tag, N in [("1k",333),("10k",3333),("100k",33333),("1M",333333)]:
    con.execute(f"""
    COPY (
      SELECT 'R'||r AS rin,
             'C'|| ( (r/{CSIZE})::BIGINT * {CFR_PER_CLUSTER}
                     + ((r*7 + k*13) % {CFR_PER_CLUSTER}) ) AS cfr
      FROM range(0,{N}) t(r), range(0,{K}) g(k)
    ) TO '{OUT}/edges_{tag}.parquet' (FORMAT parquet)
    """)
    e = con.execute(f"SELECT count(*), count(DISTINCT rin), count(DISTINCT cfr) FROM read_parquet('{OUT}/edges_{tag}.parquet')").fetchone()
    print(f"{tag:>4}: {e[0]:>9,} edges  {e[1]:>7,} RINs  {e[2]:>6,} CFRs")
