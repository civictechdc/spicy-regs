"""Add the court scope to an already-built ``court_opinion_clusters`` table.

``build_court_opinion_clusters`` resolves ``court_id`` / ``court_jurisdiction``
/ ``court_is_federal`` while it shapes each row, so any table built from now on
carries them. A table built before that does not, and re-streaming the 2.3 GiB
dump to add three columns it already has the key for is 23 minutes of reading
for no new facts.

Two output modes, chosen by what the volume can afford rather than by taste:

* ``scope`` writes a small ``(cluster_id, cl_docket_id, court_id,
  court_jurisdiction, court_is_federal)`` table — four narrow columns over ten
  million rows, tens of megabytes — which any query can join.
* ``full`` rewrites the whole clusters table with the columns inline. That is
  the better artifact and it costs a second copy of a 3.9 GB file, which is why
  it is guarded by ``check_headroom`` rather than attempted hopefully.

The default is whichever fits. A run that silently produced the lesser artifact
would be the sort of quiet degradation this codebase keeps getting bitten by,
so the mode actually used is logged and lands in the receipt.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.transforms.build_court_opinion_bodies import DISK_HEADROOM_FLOOR, check_headroom
from spicy_regs.transforms.court_scope import CourtScope, court_jurisdictions

SCOPE_COLUMNS = (
    "cluster_id",
    "cl_docket_id",
    "court_id",
    "court_jurisdiction",
    "court_is_federal",
)
_SCOPE_SCHEMA = pa.schema([(c, pa.string()) for c in SCOPE_COLUMNS])
BATCH_ROWS = 250_000


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _fits(needed: int, path: Path) -> bool:
    try:
        check_headroom(needed, path=path)
    except RuntimeError as exc:
        logger.warning("Court scope backfill: {}", exc)
        return False
    return True


def backfill(
    *,
    clusters: Path,
    docket_court_map: Path,
    courts_dump: Path,
    output_dir: Path,
    dump_date: date,
    mode: str = "auto",
) -> dict:
    """Write the scoped table and return its receipt."""
    output_dir.mkdir(parents=True, exist_ok=True)
    source = pq.ParquetFile(clusters)
    total_rows = source.metadata.num_rows

    if mode == "auto":
        mode = "full" if _fits(clusters.stat().st_size, output_dir) else "scope"
    elif mode == "full":
        check_headroom(clusters.stat().st_size, path=output_dir)
    logger.info(
        "Court scope backfill: {:,} clusters, mode={} (floor {:.0f} GiB)",
        total_rows,
        mode,
        DISK_HEADROOM_FLOOR / 2**30,
    )

    args = argparse.Namespace(
        clusters=clusters,
        docket_court_map=docket_court_map,
        courts_dump=courts_dump,
        output_dir=output_dir,
        dump_date=dump_date,
    )
    scope = CourtScope.from_map(args.docket_court_map, court_jurisdictions(local_file=args.courts_dump))

    existing = [field.name for field in source.schema_arrow]
    if mode == "full":
        out_columns = list(existing)
        # Keep the published order: the scope sits next to the join key it is
        # derived from, not bolted onto the end. Inserted as one slice — three
        # separate inserts at the same position reverse them.
        missing = [
            column for column in ("court_id", "court_jurisdiction", "court_is_federal") if column not in out_columns
        ]
        at = out_columns.index("cl_docket_id") + 1
        out_columns[at:at] = missing
        out_schema = pa.schema([(c, pa.string()) for c in out_columns])
        out_file = args.output_dir / "court_opinion_clusters.parquet"
    else:
        out_columns = list(SCOPE_COLUMNS)
        out_schema = _SCOPE_SCHEMA
        out_file = args.output_dir / "court_cluster_scope.parquet"

    staging = out_file.with_suffix(".partial.parquet")
    writer = pq.ParquetWriter(staging, out_schema, compression="zstd")
    jurisdictions: Counter[str] = Counter()
    federal = unknown = 0
    written = 0
    # Scope mode needs two columns out of thirty-six, and the thirty-four it
    # does not need include syllabus, headmatter and summary — kilobytes of
    # prose per row. Materializing those as Python objects, 250,000 rows at a
    # time, is gigabytes of memory to answer a question about docket ids.
    # Full mode has to carry every column through, so its batches are a tenth
    # the size for the same peak memory.
    full = mode == "full"
    read_columns = None if full else list(SCOPE_COLUMNS[:2])
    batch_rows = BATCH_ROWS // 10 if full else BATCH_ROWS
    try:
        for batch in source.iter_batches(batch_size=batch_rows, columns=read_columns):
            rows = batch.to_pylist()
            shaped = []
            for row in rows:
                court_id, jurisdiction, is_fed = scope.for_docket(row.get("cl_docket_id"))
                jurisdictions[jurisdiction or "<unknown>"] += 1
                if is_fed == "t":
                    federal += 1
                elif is_fed is None:
                    unknown += 1
                enriched = {
                    "court_id": court_id,
                    "court_jurisdiction": jurisdiction,
                    "court_is_federal": is_fed,
                }
                if mode == "full":
                    shaped.append({**row, **enriched})
                else:
                    shaped.append(
                        {
                            "cluster_id": row.get("cluster_id"),
                            "cl_docket_id": row.get("cl_docket_id"),
                            **enriched,
                        }
                    )
            writer.write_table(pa.Table.from_pylist(shaped, schema=out_schema))
            written += len(shaped)
            if written % (BATCH_ROWS * 8) == 0:
                logger.info("Court scope backfill: {:,} / {:,} rows", written, total_rows)
    finally:
        writer.close()
    staging.replace(out_file)

    receipt = {
        "artifact": out_file.name,
        "mode": mode,
        "written_at": datetime.now(UTC).isoformat(),
        "inputs": {
            "clusters": {"path": str(args.clusters), "sha256": _sha256(args.clusters)},
            "docket_court_map": {
                "path": str(args.docket_court_map),
                "sha256": _sha256(args.docket_court_map),
                "dockets": pq.ParquetFile(args.docket_court_map).metadata.num_rows,
                "dump_date": args.dump_date.isoformat(),
            },
            "courts_dump": {
                "path": str(args.courts_dump),
                "sha256": _sha256(args.courts_dump),
            },
        },
        "coverage": {
            "denominator": "clusters in court_opinion_clusters",
            "denominator_count": total_rows,
            "rows_written": written,
            "clusters_placed_in_a_court": total_rows - unknown,
            "clusters_with_no_court": unknown,
            "clusters_in_a_federal_court": federal,
            "federal_share": round(federal / total_rows, 6) if total_rows else None,
            "by_jurisdiction": dict(jurisdictions.most_common()),
        },
    }
    receipt_path = args.output_dir / "cluster_court_scope_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    logger.info("Receipt written to {}", receipt_path)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clusters", type=Path, required=True)
    parser.add_argument("--docket-court-map", type=Path, required=True)
    parser.add_argument("--courts-dump", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--dump-date", type=date.fromisoformat, default=date(2026, 6, 30))
    parser.add_argument("--mode", choices=("auto", "scope", "full"), default="auto")
    options = parser.parse_args()
    print(
        json.dumps(
            backfill(
                clusters=options.clusters,
                docket_court_map=options.docket_court_map,
                courts_dump=options.courts_dump,
                output_dir=options.output_dir,
                dump_date=options.dump_date,
                mode=options.mode,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
