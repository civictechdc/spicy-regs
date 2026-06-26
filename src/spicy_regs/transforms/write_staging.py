"""Transform: write parsed records to a per-agency staging Parquet file."""

from pathlib import Path

import polars as pl


def write_staging(
    agency: str,
    data_type: str,
    records: list[dict],
    staging_dir: Path,
    schema: dict,
) -> int:
    """Write parsed records to a staging Parquet file for one agency/data_type."""
    if not records:
        return 0

    staging_type_dir = staging_dir / data_type
    staging_type_dir.mkdir(parents=True, exist_ok=True)
    staging_file = staging_type_dir / f"{agency}.parquet"

    df = pl.DataFrame(records, schema=schema)
    df.write_parquet(staging_file, compression="zstd")
    return len(records)
