"""Tests for transform module: staging, merging, partitioning, feed summary."""

from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipeline.transform import (
    build_feed_summary,
    merge_comments_partitioned,
    merge_staging_files,
    partition_comments,
    update_comments_index,
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
        dedup_keys = {"dockets": "docket_id"}
        merge_staging_files(staging, output, ["dockets"], schemas, dedup_keys)

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
        dedup_keys = {"dockets": "docket_id"}
        merge_staging_files(staging, output, ["dockets"], schemas, dedup_keys)

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
        dedup_keys = {"dockets": "docket_id"}
        merge_staging_files(staging, output, ["dockets"], schemas, dedup_keys)

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
        merge_staging_files(
            staging, output, ["dockets"], {"dockets": DOCKET_SCHEMA}, {"dockets": "docket_id"}
        )
        assert not (output / "dockets.parquet").exists()

    def test_deduplicates_dockets_keeping_latest_modify_date(self, tmp_path):
        """Merging should collapse repeated docket_ids, keeping the row
        with the most recent modify_date (and its associated title/abstract)."""
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        # Existing output: stale version of the same docket
        existing = [{
            "docket_id": "EPA-2024-0001",
            "agency_code": "EPA",
            "title": "Stale Title",
            "docket_type": "Rulemaking",
            "modify_date": "2024-01-01T00:00:00Z",
            "abstract": "old",
        }]
        write_parquet_from_dicts(output / "dockets.parquet", existing, DOCKET_SCHEMA)

        # New staging: updated version of the same docket
        updated = [{
            "docket_id": "EPA-2024-0001",
            "agency_code": "EPA",
            "title": "Fresh Title",
            "docket_type": "Rulemaking",
            "modify_date": "2024-06-15T00:00:00Z",
            "abstract": "new",
        }]
        write_staging("EPA", "dockets", updated, staging, DOCKET_SCHEMA)

        merge_staging_files(
            staging, output, ["dockets"], {"dockets": DOCKET_SCHEMA}, {"dockets": "docket_id"}
        )

        merged = pl.read_parquet(output / "dockets.parquet")
        assert len(merged) == 1
        row = merged.row(0, named=True)
        assert row["docket_id"] == "EPA-2024-0001"
        assert row["modify_date"] == "2024-06-15T00:00:00Z"
        assert row["title"] == "Fresh Title"
        assert row["abstract"] == "new"

    def test_deduplicates_dockets_within_single_merge(self, tmp_path):
        """Duplicates inside a single staging file should also be collapsed."""
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        # Multiple copies of the same docket in a single staging file —
        # mirrors what happens when the same JSON key is re-downloaded.
        records = [
            {"docket_id": "FDA-2015-N-0030", "agency_code": "FDA", "title": "v1",
             "docket_type": "Rulemaking", "modify_date": "2023-05-31T12:53:19Z", "abstract": None},
            {"docket_id": "FDA-2015-N-0030", "agency_code": "FDA", "title": "v2",
             "docket_type": "Rulemaking", "modify_date": "2024-04-30T14:51:09Z", "abstract": None},
            {"docket_id": "FDA-2015-N-0030", "agency_code": "FDA", "title": "v3",
             "docket_type": "Rulemaking", "modify_date": "2025-12-18T15:39:35Z", "abstract": None},
        ]
        write_staging("FDA", "dockets", records, staging, DOCKET_SCHEMA)

        merge_staging_files(
            staging, output, ["dockets"], {"dockets": DOCKET_SCHEMA}, {"dockets": "docket_id"}
        )

        merged = pl.read_parquet(output / "dockets.parquet")
        assert len(merged) == 1
        assert merged["modify_date"][0] == "2025-12-18T15:39:35Z"
        assert merged["title"][0] == "v3"

    def test_deduplicates_documents_keeping_latest_modify_date(self, tmp_path):
        """Documents should dedupe on document_id, keeping latest modify_date."""
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        existing = [{
            "document_id": "D-001", "docket_id": "EPA-2024-0001", "agency_code": "EPA",
            "title": "old", "document_type": "Proposed Rule",
            "posted_date": "2024-06-01", "modify_date": "2024-06-01",
            "comment_start_date": "2024-06-01", "comment_end_date": "2024-07-01",
            "file_url": None,
        }]
        write_parquet_from_dicts(output / "documents.parquet", existing, DOCUMENT_SCHEMA)

        updated = [{
            "document_id": "D-001", "docket_id": "EPA-2024-0001", "agency_code": "EPA",
            "title": "new", "document_type": "Proposed Rule",
            "posted_date": "2024-06-01", "modify_date": "2024-09-15",
            "comment_start_date": "2024-06-01", "comment_end_date": "2024-07-01",
            "file_url": None,
        }]
        write_staging("EPA", "documents", updated, staging, DOCUMENT_SCHEMA)

        merge_staging_files(
            staging, output, ["documents"],
            {"documents": DOCUMENT_SCHEMA},
            {"documents": "document_id"},
        )

        merged = pl.read_parquet(output / "documents.parquet")
        assert len(merged) == 1
        assert merged["modify_date"][0] == "2024-09-15"
        assert merged["title"][0] == "new"

    def test_deduplicates_comments_keeping_latest_modify_date(self, tmp_path):
        """Comments should dedupe on comment_id, keeping latest modify_date."""
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        existing = [{
            "comment_id": "C-001", "docket_id": "EPA-2024-0001", "agency_code": "EPA",
            "title": "old", "comment": "old body", "document_type": "Public Comment",
            "posted_date": "2024-06-20", "modify_date": "2024-06-20",
            "receive_date": "2024-06-20", "attachments_json": None,
        }]
        write_parquet_from_dicts(output / "comments.parquet", existing, COMMENT_SCHEMA)

        updated = [{
            "comment_id": "C-001", "docket_id": "EPA-2024-0001", "agency_code": "EPA",
            "title": "updated", "comment": "new body", "document_type": "Public Comment",
            "posted_date": "2024-06-20", "modify_date": "2024-08-01",
            "receive_date": "2024-06-20", "attachments_json": None,
        }]
        write_staging("EPA", "comments", updated, staging, COMMENT_SCHEMA)

        merge_staging_files(
            staging, output, ["comments"],
            {"comments": COMMENT_SCHEMA},
            {"comments": "comment_id"},
        )

        merged = pl.read_parquet(output / "comments.parquet")
        assert len(merged) == 1
        assert merged["modify_date"][0] == "2024-08-01"
        assert merged["comment"][0] == "new body"

    def test_dedup_preserves_unrelated_rows(self, tmp_path):
        """Dedup must not drop rows with different ids."""
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        records = [
            {"docket_id": "EPA-2024-0001", "agency_code": "EPA", "title": "a",
             "docket_type": "Rulemaking", "modify_date": "2024-01-01", "abstract": None},
            {"docket_id": "EPA-2024-0001", "agency_code": "EPA", "title": "a2",
             "docket_type": "Rulemaking", "modify_date": "2024-06-01", "abstract": None},
            {"docket_id": "EPA-2024-0002", "agency_code": "EPA", "title": "b",
             "docket_type": "Rulemaking", "modify_date": "2024-02-01", "abstract": None},
            {"docket_id": "FDA-2024-0010", "agency_code": "FDA", "title": "c",
             "docket_type": "Rulemaking", "modify_date": "2024-03-01", "abstract": None},
        ]
        write_staging("MIX", "dockets", records, staging, DOCKET_SCHEMA)

        merge_staging_files(
            staging, output, ["dockets"], {"dockets": DOCKET_SCHEMA}, {"dockets": "docket_id"}
        )

        merged = pl.read_parquet(output / "dockets.parquet").sort("docket_id")
        assert merged["docket_id"].to_list() == ["EPA-2024-0001", "EPA-2024-0002", "FDA-2024-0010"]
        epa1 = merged.filter(pl.col("docket_id") == "EPA-2024-0001")
        assert epa1["modify_date"][0] == "2024-06-01"
        assert epa1["title"][0] == "a2"


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

    def test_uses_comments_index_over_monolithic(self, tmp_output, sample_dockets, sample_documents):
        """When comments_index.parquet exists, feed summary should use it for counts."""
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "documents.parquet", sample_documents, DOCUMENT_SCHEMA)

        # Write a comments index (no monolithic comments.parquet)
        index_data = [
            {"agency_code": "EPA", "docket_id": "EPA-2024-0001", "year": 2024, "month": 6, "row_count": 2},
            {"agency_code": "FDA", "docket_id": "FDA-2024-0010", "year": 2024, "month": 5, "row_count": 1},
            {"agency_code": "EPA", "docket_id": "EPA-2024-0002", "year": 2024, "month": 7, "row_count": 1},
        ]
        pl.DataFrame(index_data, schema={
            "agency_code": pl.Utf8, "docket_id": pl.Utf8,
            "year": pl.Int64, "month": pl.Int64, "row_count": pl.Int64,
        }).write_parquet(tmp_output / "comments_index.parquet")

        summary = pl.read_parquet(build_feed_summary(tmp_output))
        assert len(summary) == 3
        epa_0001 = summary.filter(pl.col("docket_id") == "EPA-2024-0001")
        assert epa_0001["comment_count"][0] == 2
        fda = summary.filter(pl.col("docket_id") == "FDA-2024-0010")
        assert fda["comment_count"][0] == 1


class TestMergeCommentsPartitioned:
    @patch("spicy_regs.pipeline.download_r2.download_from_r2", return_value=False)
    def test_partitions_by_agency_docket_year_month(self, mock_dl, tmp_path, sample_comments):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        write_staging("EPA", "comments", [c for c in sample_comments if c["agency_code"] == "EPA"], staging, COMMENT_SCHEMA)
        write_staging("FDA", "comments", [c for c in sample_comments if c["agency_code"] == "FDA"], staging, COMMENT_SCHEMA)

        changed = merge_comments_partitioned(staging, output, COMMENT_SCHEMA, "comment_id")

        assert len(changed) > 0
        # Verify partition structure
        for p in changed:
            parts = p.relative_to(output / "comments").parts
            assert any("agency_code=" in part for part in parts)
            assert any("docket_id=" in part for part in parts)
            assert any("year=" in part for part in parts)
            assert any("month=" in part for part in parts)

    @patch("spicy_regs.pipeline.download_r2.download_from_r2", return_value=False)
    def test_preserves_all_rows(self, mock_dl, tmp_path, sample_comments):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        write_staging("ALL", "comments", sample_comments, staging, COMMENT_SCHEMA)
        changed = merge_comments_partitioned(staging, output, COMMENT_SCHEMA, "comment_id")

        total = sum(pq.ParquetFile(f).metadata.num_rows for f in changed)
        assert total == len(sample_comments)

    @patch("spicy_regs.pipeline.download_r2.download_from_r2", return_value=False)
    def test_deduplicates_comments(self, mock_dl, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        records = [
            {"comment_id": "C-001", "docket_id": "EPA-2024-0001", "agency_code": "EPA",
             "title": "old", "comment": "old body", "document_type": "Public Comment",
             "posted_date": "2024-06-20T00:00:00Z", "modify_date": "2024-06-20",
             "receive_date": "2024-06-20", "attachments_json": None},
            {"comment_id": "C-001", "docket_id": "EPA-2024-0001", "agency_code": "EPA",
             "title": "new", "comment": "new body", "document_type": "Public Comment",
             "posted_date": "2024-06-20T00:00:00Z", "modify_date": "2024-08-01",
             "receive_date": "2024-06-20", "attachments_json": None},
        ]
        write_staging("EPA", "comments", records, staging, COMMENT_SCHEMA)
        changed = merge_comments_partitioned(staging, output, COMMENT_SCHEMA, "comment_id")

        assert len(changed) == 1
        df = pl.read_parquet(changed[0])
        assert len(df) == 1
        assert df["modify_date"][0] == "2024-08-01"
        assert df["comment"][0] == "new body"

    @patch("spicy_regs.pipeline.download_r2.download_from_r2", return_value=False)
    def test_merges_with_existing_partition(self, mock_dl, tmp_path):
        """When an existing partition file is present, new data should merge with it."""
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        # Pre-create an existing partition
        partition_dir = output / "comments" / "agency_code=EPA" / "docket_id=EPA-2024-0001" / "year=2024" / "month=6"
        partition_dir.mkdir(parents=True)
        existing = [
            {"comment_id": "C-EXIST", "docket_id": "EPA-2024-0001", "agency_code": "EPA",
             "title": "existing", "comment": "already here", "document_type": "Public Comment",
             "posted_date": "2024-06-15T00:00:00Z", "modify_date": "2024-06-15",
             "receive_date": "2024-06-15", "attachments_json": None},
        ]
        write_parquet_from_dicts(partition_dir / "part-0.parquet", existing, COMMENT_SCHEMA)

        # Stage new comment in the same partition
        new_comment = [
            {"comment_id": "C-NEW", "docket_id": "EPA-2024-0001", "agency_code": "EPA",
             "title": "new", "comment": "just added", "document_type": "Public Comment",
             "posted_date": "2024-06-20T00:00:00Z", "modify_date": "2024-06-20",
             "receive_date": "2024-06-20", "attachments_json": None},
        ]
        write_staging("EPA", "comments", new_comment, staging, COMMENT_SCHEMA)

        changed = merge_comments_partitioned(staging, output, COMMENT_SCHEMA, "comment_id")
        assert len(changed) == 1

        df = pl.read_parquet(changed[0])
        assert len(df) == 2  # existing + new
        assert set(df["comment_id"].to_list()) == {"C-EXIST", "C-NEW"}

    @patch("spicy_regs.pipeline.download_r2.download_from_r2", return_value=False)
    def test_returns_empty_for_no_staging(self, mock_dl, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        result = merge_comments_partitioned(staging, output, COMMENT_SCHEMA, "comment_id")
        assert result == []


class TestUpdateCommentsIndex:
    def test_builds_index_from_changed_files(self, tmp_path):
        output = tmp_path / "output"
        comments_dir = output / "comments"

        # Create partition files
        for agency, docket, year, month, rows in [
            ("EPA", "EPA-2024-0001", 2024, 6, 10),
            ("EPA", "EPA-2024-0002", 2024, 7, 5),
            ("FDA", "FDA-2024-0010", 2024, 5, 3),
        ]:
            pdir = comments_dir / f"agency_code={agency}" / f"docket_id={docket}" / f"year={year}" / f"month={month}"
            pdir.mkdir(parents=True)
            records = [{"comment_id": f"C-{i}", "docket_id": docket, "agency_code": agency,
                        "title": "T", "comment": "C", "document_type": "PC",
                        "posted_date": f"{year}-{month:02d}-01", "modify_date": f"{year}-{month:02d}-01",
                        "receive_date": f"{year}-{month:02d}-01", "attachments_json": None}
                       for i in range(rows)]
            write_parquet_from_dicts(pdir / "part-0.parquet", records, COMMENT_SCHEMA)

        changed = list(comments_dir.rglob("part-0.parquet"))
        index_path = update_comments_index(output, changed)

        assert index_path.exists()
        idx = pl.read_parquet(index_path)
        assert len(idx) == 3
        assert idx.filter(pl.col("docket_id") == "EPA-2024-0001")["row_count"][0] == 10
        assert idx["row_count"].sum() == 18

    def test_incremental_update_preserves_existing(self, tmp_path):
        output = tmp_path / "output"
        output.mkdir()
        comments_dir = output / "comments"

        # Create initial index with one entry
        pl.DataFrame([
            {"agency_code": "OLD", "docket_id": "OLD-001", "year": 2023, "month": 1, "row_count": 100},
        ], schema={"agency_code": pl.Utf8, "docket_id": pl.Utf8, "year": pl.Int64, "month": pl.Int64, "row_count": pl.Int64}).write_parquet(output / "comments_index.parquet")

        # Add a new partition
        pdir = comments_dir / "agency_code=NEW" / "docket_id=NEW-001" / "year=2024" / "month=1"
        pdir.mkdir(parents=True)
        records = [{"comment_id": "C-1", "docket_id": "NEW-001", "agency_code": "NEW",
                    "title": "T", "comment": "C", "document_type": "PC",
                    "posted_date": "2024-01-01", "modify_date": "2024-01-01",
                    "receive_date": "2024-01-01", "attachments_json": None}]
        write_parquet_from_dicts(pdir / "part-0.parquet", records, COMMENT_SCHEMA)

        changed = [pdir / "part-0.parquet"]
        index_path = update_comments_index(output, changed)

        idx = pl.read_parquet(index_path)
        assert len(idx) == 2  # old entry preserved + new entry
        assert idx.filter(pl.col("docket_id") == "OLD-001")["row_count"][0] == 100
        assert idx.filter(pl.col("docket_id") == "NEW-001")["row_count"][0] == 1
