"""Extract a normalized typed graph from a real spicy-regs generation.

Writes node/edge parquet that BOTH DuckDB/DuckPGQ and Kuzu consume identically,
so the engine comparison is apples-to-apples on the same corpus.
"""
from __future__ import annotations
import json, os, sys
import duckdb

GEN = sys.argv[1] if len(sys.argv) > 1 else "output/mixed-real-data-all-profile-openai-v1"
OUT = os.path.join(os.path.dirname(__file__), "graph")
os.makedirs(OUT, exist_ok=True)
con = duckdb.connect()

def rp(t): return f"read_parquet('{GEN}/{t}.parquet')"

# ---- Edges (typed, normalized keys) ----
# RIN -TARGETS-> CFR unit
con.execute(f"""
COPY (
  SELECT DISTINCT upper(trim(rin)) AS rin, cfr_ref AS cfr
  FROM {rp('rule_targets')}
  WHERE rin IS NOT NULL AND cfr_ref IS NOT NULL
) TO '{OUT}/edge_targets.parquet' (FORMAT parquet)
""")
# RIN -HAS_AUTHORITY-> USC (title-section key)
con.execute(f"""
COPY (
  SELECT DISTINCT upper(trim(rin)) AS rin,
         usc_title || '-' || usc_section AS usc
  FROM {rp('authority_edges')}
  WHERE rin IS NOT NULL AND parse_status <> 'failed'
        AND usc_title IS NOT NULL AND usc_section IS NOT NULL
) TO '{OUT}/edge_authority.parquet' (FORMAT parquet)
""")
# Proceeding -HAS_RIN-> RIN
con.execute(f"""
COPY (
  SELECT DISTINCT proceeding_id AS proceeding, upper(trim(rin)) AS rin
  FROM {rp('proceedings')} WHERE rin IS NOT NULL
) TO '{OUT}/edge_proc_rin.parquet' (FORMAT parquet)
""")
# Subject -ABOUT-> Concept
con.execute(f"""
COPY (
  SELECT DISTINCT subject_id AS subject, concept_id AS concept
  FROM {rp('concept_assignments')} WHERE subject_id IS NOT NULL AND concept_id IS NOT NULL
) TO '{OUT}/edge_about.parquet' (FORMAT parquet)
""")

# ---- Nodes (union of every id that appears as an endpoint) ----
con.execute(f"""
COPY (
  SELECT rin AS id, 'RIN' AS kind FROM read_parquet('{OUT}/edge_targets.parquet')
  UNION SELECT rin,'RIN' FROM read_parquet('{OUT}/edge_authority.parquet')
  UNION SELECT rin,'RIN' FROM read_parquet('{OUT}/edge_proc_rin.parquet')
) TO '{OUT}/node_rin.parquet' (FORMAT parquet)
""")
con.execute(f"COPY (SELECT DISTINCT cfr AS id FROM read_parquet('{OUT}/edge_targets.parquet')) TO '{OUT}/node_cfr.parquet' (FORMAT parquet)")
con.execute(f"COPY (SELECT DISTINCT usc AS id FROM read_parquet('{OUT}/edge_authority.parquet')) TO '{OUT}/node_usc.parquet' (FORMAT parquet)")
con.execute(f"COPY (SELECT DISTINCT proceeding AS id FROM read_parquet('{OUT}/edge_proc_rin.parquet')) TO '{OUT}/node_proc.parquet' (FORMAT parquet)")
con.execute(f"COPY (SELECT DISTINCT subject AS id FROM read_parquet('{OUT}/edge_about.parquet')) TO '{OUT}/node_subject.parquet' (FORMAT parquet)")
con.execute(f"COPY (SELECT DISTINCT concept AS id FROM read_parquet('{OUT}/edge_about.parquet')) TO '{OUT}/node_concept.parquet' (FORMAT parquet)")

def n(t): return con.execute(f"SELECT count(*) FROM read_parquet('{OUT}/{t}.parquet')").fetchone()[0]
print("=== extracted graph (real generation:", GEN, ") ===")
for f in ["node_rin","node_cfr","node_usc","node_proc","node_subject","node_concept",
          "edge_targets","edge_authority","edge_proc_rin","edge_about"]:
    print(f"  {f:16s} {n(f):>5d}")
# how connected is the RIN-CFR bipartite (matters for variable-length reach)
comp = con.execute(f"""
  SELECT count(*) FROM read_parquet('{OUT}/edge_targets.parquet') a
  JOIN read_parquet('{OUT}/edge_targets.parquet') b USING (cfr)
  WHERE a.rin <> b.rin
""").fetchone()[0]
print("  co-target RIN pairs (1 hop via shared CFR):", comp)
