"""Tests for transform module: staging, merging, partitioning, feed summary."""

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipeline.transform import (
    build_feed_summary,
    merge_staging_files,
    partition_comments,
    write_staging,
)
from tests.conftest import (
    COMMENT_SCHEMA,
    DOCKET_SCHEMA,
    DOCUMENT_SCHEMA,
    write_parquet_from_dicts,
)


class TestWriteStaging:
    def test_writes_parquet_file(self, tmp_path, sample_dockets):
        staging = tmp_path / "staging"
        staging.mkdir()
        row_count = write_staging("EPA", "dockets", sample_dockets, staging, DOCKET_SCHEMA)
        assert row_count == 3
        assert (staging / "dockets" / "EPA.parquet").exists()

    def test_returns_zero_for_empty_records(self, tmp_path):
        staging = tmp_path / "staging"
        staging.mkdir()
        assert write_staging("EPA", "dockets", [], staging, DOCKET_SCHEMA) == 0

    def test_written_file_is_readable(self, tmp_path, sample_dockets):
        staging = tmp_path / "staging"
        staging.mkdir()
        write_staging("EPA", "dockets", sample_dockets, staging, DOCKET_SCHEMA)
        df = pl.read_parquet(staging / "dockets" / "EPA.parquet")
        assert len(df) == 3
        assert set(df.columns) == set(DOCKET_SCHEMA.keys())


class TestMergeStagingFiles:
    def test_merges_multiple_agencies(self, tmp_path, sample_dockets):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        epa = [d for d in sample_dockets if d["agency_code"] == "EPA"]
        fda = [d for d in sample_dockets if d["agency_code"] == "FDA"]

        write_staging("EPA", "dockets", epa, staging, DOCKET_SCHEMA)
        write_staging("FDA", "dockets", fda, staging, DOCKET_SCHEMA)

        schemas = {"dockets": DOCKET_SCHEMA}
        merge_staging_files(staging, output, ["dockets"], schemas)

        merged = pl.read_parquet(output / "dockets.parquet")
        assert len(merged) == 3

    def test_appends_to_existing_output(self, tmp_path, sample_dockets):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        # Write an existing output file with EPA data
        epa = [d for d in sample_dockets if d["agency_code"] == "EPA"]
        write_parquet_from_dicts(output / "dockets.parquet", epa, DOCKET_SCHEMA)

        # Stage FDA data
        fda = [d for d in sample_dockets if d["agency_code"] == "FDA"]
        write_staging("FDA", "dockets", fda, staging, DOCKET_SCHEMA)

        schemas = {"dockets": DOCKET_SCHEMA}
        merge_staging_files(staging, output, ["dockets"], schemas)

        merged = pl.read_parquet(output / "dockets.parquet")
        assert len(merged) == 3

    def test_handles_schema_evolution(self, tmp_path):
        """When an existing file is missing a column, nulls are added."""
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        # Old file with fewer columns
        old_schema = {"docket_id": pl.Utf8, "agency_code": pl.Utf8, "title": pl.Utf8}
        old_records = [{"docket_id": "OLD-001", "agency_code": "OLD", "title": "Old record"}]
        write_parquet_from_dicts(output / "dockets.parquet", old_records, old_schema)

        # New staging file with full schema
        new_records = [{"docket_id": "NEW-001", "agency_code": "NEW", "title": "New", "docket_type": "Rulemaking", "modify_date": "2024-01-01", "abstract": "Test"}]
        write_staging("NEW", "dockets", new_records, staging, DOCKET_SCHEMA)

        schemas = {"dockets": DOCKET_SCHEMA}
        merge_staging_files(staging, output, ["dockets"], schemas)

        merged = pl.read_parquet(output / "dockets.parquet")
        assert len(merged) == 2
        # Old record should have null for new columns
        old_row = merged.filter(pl.col("docket_id") == "OLD-001")
        assert old_row["docket_type"][0] is None

    def test_skips_missing_staging_dir(self, tmp_path):
        """No error when staging dir doesn't exist for a data type."""
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()
        # staging/dockets doesn't exist — should silently skip
        merge_staging_files(staging, output, ["dockets"], {"dockets": DOCKET_SCHEMA})
        assert not (output / "dockets.parquet").exists()


class TestPartitionComments:
    def test_creates_per_agency_partitions(self, tmp_output, sample_comments):
        write_parquet_from_dicts(tmp_output / "comments.parquet", sample_comments, COMMENT_SCHEMA)
        partition_dir = partition_comments(tmp_output)

        assert (partition_dir / "agency_code=EPA" / "part-0.parquet").exists()
        assert (partition_dir / "agency_code=FDA" / "part-0.parquet").exists()

    def test_partition_row_counts(self, tmp_output, sample_comments):
        write_parquet_from_dicts(tmp_output / "comments.parquet", sample_comments, COMMENT_SCHEMA)
        partition_dir = partition_comments(tmp_output)

        epa = pl.read_parquet(partition_dir / "agency_code=EPA" / "part-0.parquet")
        fda = pl.read_parquet(partition_dir / "agency_code=FDA" / "part-0.parquet")
        assert len(epa) == 3  # C-001, C-002, C-004
        assert len(fda) == 1  # C-003

    def test_partitions_are_sorted(self, tmp_output, sample_comments):
        write_parquet_from_dicts(tmp_output / "comments.parquet", sample_comments, COMMENT_SCHEMA)
        partition_dir = partition_comments(tmp_output)

        epa = pl.read_parquet(partition_dir / "agency_code=EPA" / "part-0.parquet")
        docket_ids = epa["docket_id"].to_list()
        # EPA-2024-0001 comments should come before EPA-2024-0002
        assert docket_ids == sorted(docket_ids)

    def test_preserves_all_rows(self, tmp_output, sample_comments):
        write_parquet_from_dicts(tmp_output / "comments.parquet", sample_comments, COMMENT_SCHEMA)
        partition_dir = partition_comments(tmp_output)

        total = 0
        for part_dir in partition_dir.iterdir():
            if part_dir.is_dir():
                df = pl.read_parquet(part_dir / "part-0.parquet")
                total += len(df)
        assert total == len(sample_comments)

    def test_raises_on_missing_file(self, tmp_output):
        with pytest.raises(FileNotFoundError):
            partition_comments(tmp_output)

    def test_agency_code_not_in_partition_columns(self, tmp_output, sample_comments):
        """agency_code should be in the directory name, not in the parquet columns."""
        write_parquet_from_dicts(tmp_output / "comments.parquet", sample_comments, COMMENT_SCHEMA)
        partition_dir = partition_comments(tmp_output)

        # Check the raw Parquet schema (not Polars, which re-adds hive partition columns)
        pf = pq.ParquetFile(partition_dir / "agency_code=EPA" / "part-0.parquet")
        col_names = [f.name for f in pf.schema_arrow]
        assert "agency_code" not in col_names


class TestBuildFeedSummary:
    def test_basic_feed_summary(self, tmp_output, sample_dockets, sample_comments, sample_documents):
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "comments.parquet", sample_comments, COMMENT_SCHEMA)
        write_parquet_from_dicts(tmp_output / "documents.parquet", sample_documents, DOCUMENT_SCHEMA)

        summary_file = build_feed_summary(tmp_output)
        assert summary_file.exists()

        summary = pl.read_parquet(summary_file)
        assert len(summary) == 3  # 3 dockets

    def test_comment_counts(self, tmp_output, sample_dockets, sample_comments, sample_documents):
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "comments.parquet", sample_comments, COMMENT_SCHEMA)
        write_parquet_from_dicts(tmp_output / "documents.parquet", sample_documents, DOCUMENT_SCHEMA)

        summary = pl.read_parquet(build_feed_summary(tmp_output))

        epa_0001 = summary.filter(pl.col("docket_id") == "EPA-2024-0001")
        assert epa_0001["comment_count"][0] == 2

        fda = summary.filter(pl.col("docket_id") == "FDA-2024-0010")
        assert fda["comment_count"][0] == 1

        epa_0002 = summary.filter(pl.col("docket_id") == "EPA-2024-0002")
        assert epa_0002["comment_count"][0] == 1

    def test_comment_end_date_from_documents(self, tmp_output, sample_dockets, sample_comments, sample_documents):
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "comments.parquet", sample_comments, COMMENT_SCHEMA)
        write_parquet_from_dicts(tmp_output / "documents.parquet", sample_documents, DOCUMENT_SCHEMA)

        summary = pl.read_parquet(build_feed_summary(tmp_output))

        epa = summary.filter(pl.col("docket_id") == "EPA-2024-0001")
        assert epa["comment_end_date"][0] == "2024-07-01"

    def test_date_created_from_documents(self, tmp_output, sample_dockets, sample_comments, sample_documents):
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "comments.parquet", sample_comments, COMMENT_SCHEMA)
        write_parquet_from_dicts(tmp_output / "documents.parquet", sample_documents, DOCUMENT_SCHEMA)

        summary = pl.read_parquet(build_feed_summary(tmp_output))

        epa = summary.filter(pl.col("docket_id") == "EPA-2024-0001")
        assert epa["date_created"][0] == "2024-06-01"

    def test_sorted_by_modify_date_desc(self, tmp_output, sample_dockets, sample_comments, sample_documents):
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "comments.parquet", sample_comments, COMMENT_SCHEMA)
        write_parquet_from_dicts(tmp_output / "documents.parquet", sample_documents, DOCUMENT_SCHEMA)

        summary = pl.read_parquet(build_feed_summary(tmp_output))
        dates = summary["modify_date"].to_list()
        assert dates == sorted(dates, reverse=True)

    def test_without_comments_file(self, tmp_output, sample_dockets, sample_documents):
        """Feed summary should work with zero comments."""
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "documents.parquet", sample_documents, DOCUMENT_SCHEMA)

        summary = pl.read_parquet(build_feed_summary(tmp_output))
        assert len(summary) == 3
        # All comment_counts should be 0
        assert all(c == 0 for c in summary["comment_count"].to_list())

    def test_without_documents_file(self, tmp_output, sample_dockets, sample_comments):
        """Feed summary should work without documents."""
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "comments.parquet", sample_comments, COMMENT_SCHEMA)

        summary = pl.read_parquet(build_feed_summary(tmp_output))
        assert len(summary) == 3
        assert all(v is None for v in summary["comment_end_date"].to_list())

    def test_dockets_only(self, tmp_output, sample_dockets):
        """Feed summary with only dockets — no comments, no documents."""
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)

        summary = pl.read_parquet(build_feed_summary(tmp_output))
        assert len(summary) == 3
        assert all(c == 0 for c in summary["comment_count"].to_list())

    def test_raises_on_missing_dockets(self, tmp_output):
        with pytest.raises(FileNotFoundError):
            build_feed_summary(tmp_output)

    def test_handles_quoted_docket_ids(self, tmp_output):
        """Docket IDs with surrounding quotes should still match."""
        dockets = [{"docket_id": '"EPA-2024-0001"', "agency_code": "EPA", "title": "Test", "docket_type": "Rulemaking", "modify_date": "2024-01-01", "abstract": None}]
        comments = [{"comment_id": "C-001", "docket_id": '"EPA-2024-0001"', "agency_code": "EPA", "title": "T", "comment": "C", "document_type": "Public Comment", "posted_date": "2024-01-02", "modify_date": "2024-01-02", "receive_date": "2024-01-02", "attachments_json": None}]

        write_parquet_from_dicts(tmp_output / "dockets.parquet", dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "comments.parquet", comments, COMMENT_SCHEMA)

        summary = pl.read_parquet(build_feed_summary(tmp_output))
        assert len(summary) == 1
        assert summary["comment_count"][0] == 1
