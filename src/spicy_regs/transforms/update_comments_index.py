"""Transform: maintain the per-partition row-count index for comments."""

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from loguru import logger


def update_comments_index(output_dir: Path, changed_files: list[Path]) -> Path:
    """Update the comments index with changed partition files.

    The index (``comments_index.parquet``) maps each partition to its
    row count so the frontend can discover partition files and compute
    comment counts without scanning the actual data.
    """
    comments_dir = output_dir / "comments"
    index_file = output_dir / "comments_index.parquet"

    # Build set of changed partition keys for fast lookup.
    changed_keys: set[tuple[str, str, int, int]] = set()
    new_rows: list[dict] = []

    required_keys = {"agency_code", "docket_id", "year", "month"}

    for pf in changed_files:
        parts = pf.relative_to(comments_dir).parts
        vals: dict[str, str] = {}
        for part in parts[:-1]:  # skip "part-0.parquet"
            if "=" in part:
                k, v = part.split("=", 1)
                vals[k] = v

        if not required_keys.issubset(vals):
            logger.warning("Skipping non-conforming partition path: {}", pf)
            continue

        key = (
            vals["agency_code"],
            vals["docket_id"],
            int(vals["year"]),
            int(vals["month"]),
        )
        changed_keys.add(key)
        row_count = pq.ParquetFile(pf).metadata.num_rows
        new_rows.append(
            {
                "agency_code": key[0],
                "docket_id": key[1],
                "year": key[2],
                "month": key[3],
                "row_count": row_count,
            }
        )

    # Keep existing rows that weren't changed.
    kept_rows: list[dict] = []
    if index_file.exists():
        existing_df = pl.read_parquet(index_file)
        for row in existing_df.iter_rows(named=True):
            k = (row["agency_code"], row["docket_id"], row["year"], row["month"])
            if k not in changed_keys:
                kept_rows.append(row)

    all_rows = kept_rows + new_rows
    if all_rows:
        df = pl.DataFrame(all_rows, schema={
            "agency_code": pl.Utf8,
            "docket_id": pl.Utf8,
            "year": pl.Int64,
            "month": pl.Int64,
            "row_count": pl.Int64,
        })
        df.write_parquet(index_file, compression="zstd")

    total_rows = sum(r["row_count"] for r in all_rows)
    logger.info(
        "Comments index: {} partitions, {:,} total rows",
        len(all_rows),
        total_rows,
    )
    return index_file
