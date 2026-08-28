"""Tests for StagingWriter (Writer connector over write_staging)."""

from pathlib import Path

import polars as pl

from spicy_regs.schemas import DOCKET
from spicy_regs.sources import StagingWriter


def test_write_creates_staging_parquet(tmp_output: Path, sample_dockets: list[dict]) -> None:
    staging_dir = tmp_output / "staging"
    writer = StagingWriter("EPA", DOCKET, staging_dir)

    writer.write(sample_dockets)

    staging_file = staging_dir / "dockets" / "EPA.parquet"
    assert staging_file.exists()
    assert writer.rows_written == len(sample_dockets)

    df = pl.read_parquet(staging_file)
    assert df.height == len(sample_dockets)
    assert set(df.columns) == set(DOCKET.schema.keys())


def test_write_accepts_a_generator(tmp_output: Path, sample_dockets: list[dict]) -> None:
    staging_dir = tmp_output / "staging"
    writer = StagingWriter("EPA", DOCKET, staging_dir)

    writer.write(r for r in sample_dockets)  # generator, not a list

    assert writer.rows_written == len(sample_dockets)
    assert (staging_dir / "dockets" / "EPA.parquet").exists()


def test_write_empty_records_writes_nothing(tmp_output: Path) -> None:
    staging_dir = tmp_output / "staging"
    writer = StagingWriter("EPA", DOCKET, staging_dir)

    writer.write([])

    assert writer.rows_written == 0
    assert not (staging_dir / "dockets" / "EPA.parquet").exists()
