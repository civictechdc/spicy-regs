"""Tests for transform module: staging, merging, partitioning, feed summary."""

import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipeline.transform import (
    build_agency_rollups,
    build_feed_summary,
    merge_comments_partitioned,
    merge_staging_files,
    partition_comments,
    update_comments_index,
    write_staging,
)
from spicy_regs.schemas import DOCUMENT
from tests.conftest import (
    COMMENT_SCHEMA,
    DOCKET_SCHEMA,
    DOCUMENT_SCHEMA,
    write_parquet_from_dicts,
)

SAMPLE_DATA = Path(__file__).resolve().parents[1] / "sample-data" / "mirrulations"


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


COMMENTS_INDEX_SCHEMA = {
    "agency_code": pl.Utf8,
    "docket_id": pl.Utf8,
    "year": pl.Int64,
    "month": pl.Int64,
    "row_count": pl.Int64,
}


class TestBuildAgencyRollups:
    def _write_index(self, output_dir, rows):
        pl.DataFrame(rows, schema=COMMENTS_INDEX_SCHEMA).write_parquet(output_dir / "comments_index.parquet")

    def test_writes_both_artifacts(self, tmp_output, sample_dockets, sample_documents):
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "documents.parquet", sample_documents, DOCUMENT_SCHEMA)

        stats_file, volume_file = build_agency_rollups(tmp_output)
        assert stats_file.exists()
        assert volume_file.exists()

    def test_agency_stats_counts(self, tmp_output, sample_dockets, sample_documents):
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "documents.parquet", sample_documents, DOCUMENT_SCHEMA)
        self._write_index(
            tmp_output,
            [
                {"agency_code": "EPA", "docket_id": "EPA-2024-0001", "year": 2024, "month": 6, "row_count": 2},
                {"agency_code": "EPA", "docket_id": "EPA-2024-0002", "year": 2024, "month": 7, "row_count": 1},
                {"agency_code": "FDA", "docket_id": "FDA-2024-0010", "year": 2024, "month": 5, "row_count": 1},
            ],
        )

        stats_file, _ = build_agency_rollups(tmp_output)
        stats = pl.read_parquet(stats_file)

        epa = stats.filter(pl.col("agency_code") == "EPA")
        assert epa["docket_count"][0] == 2  # EPA-2024-0001, EPA-2024-0002
        assert epa["document_count"][0] == 1  # D-001
        assert epa["comment_count"][0] == 3  # 2 + 1

        fda = stats.filter(pl.col("agency_code") == "FDA")
        assert fda["docket_count"][0] == 1
        assert fda["document_count"][0] == 1
        assert fda["comment_count"][0] == 1

    def test_agency_stats_one_row_per_agency(self, tmp_output, sample_dockets, sample_documents):
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "documents.parquet", sample_documents, DOCUMENT_SCHEMA)

        stats = pl.read_parquet(build_agency_rollups(tmp_output)[0])
        assert sorted(stats["agency_code"].to_list()) == ["EPA", "FDA"]

    def test_agency_stats_falls_back_to_monolithic_comments(self, tmp_output, sample_dockets, sample_comments):
        """Without a comments index, counts come from comments.parquet."""
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "comments.parquet", sample_comments, COMMENT_SCHEMA)

        stats = pl.read_parquet(build_agency_rollups(tmp_output)[0])
        epa = stats.filter(pl.col("agency_code") == "EPA")
        assert epa["comment_count"][0] == 3  # C-001, C-002, C-004
        fda = stats.filter(pl.col("agency_code") == "FDA")
        assert fda["comment_count"][0] == 1  # C-003

    def test_agency_monthly_volume_by_type(self, tmp_output, sample_dockets, sample_documents):
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        write_parquet_from_dicts(tmp_output / "documents.parquet", sample_documents, DOCUMENT_SCHEMA)

        volume = pl.read_parquet(build_agency_rollups(tmp_output)[1])

        # D-001: EPA / Proposed Rule / 2024-06
        epa = volume.filter(pl.col("agency_code") == "EPA")
        assert epa["year"][0] == 2024
        assert epa["month"][0] == 6
        assert epa["document_type"][0] == "Proposed Rule"
        assert epa["document_count"][0] == 1

        # D-002: FDA / Notice / 2024-04
        fda = volume.filter(pl.col("agency_code") == "FDA")
        assert fda["document_type"][0] == "Notice"
        assert fda["month"][0] == 4

    def test_monthly_volume_aggregates_same_bucket(self, tmp_output, sample_dockets):
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)
        docs = [
            {"document_id": f"D-{i}", "docket_id": "EPA-2024-0001", "agency_code": "EPA", "title": "t", "document_type": "Notice", "posted_date": "2024-06-15", "modify_date": "2024-06-15", "comment_start_date": None, "comment_end_date": None, "file_url": None, "attachments_json": None, "fr_doc_num": None, "withdrawn": None, "reason_withdrawn": None, "additional_rins": None}
            for i in range(3)
        ]
        write_parquet_from_dicts(tmp_output / "documents.parquet", docs, DOCUMENT_SCHEMA)

        volume = pl.read_parquet(build_agency_rollups(tmp_output)[1])
        assert len(volume) == 1
        assert volume["document_count"][0] == 3

    def test_without_documents_emits_empty_volume(self, tmp_output, sample_dockets):
        write_parquet_from_dicts(tmp_output / "dockets.parquet", sample_dockets, DOCKET_SCHEMA)

        stats_file, volume_file = build_agency_rollups(tmp_output)
        stats = pl.read_parquet(stats_file)
        assert all(c == 0 for c in stats["document_count"].to_list())

        volume = pl.read_parquet(volume_file)
        assert len(volume) == 0
        assert volume.columns == ["agency_code", "year", "month", "document_type", "document_count"]

    def test_raises_on_missing_dockets(self, tmp_output):
        with pytest.raises(FileNotFoundError):
            build_agency_rollups(tmp_output)


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
    def test_merges_existing_partition_missing_new_columns(self, mock_dl, tmp_path):
        """An existing partition written before submitter columns were added
        must still merge: its missing columns surface as NULL rather than
        raising a binder error."""
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        # Existing partition with the OLD schema (no submitter columns).
        old_schema = {
            c: t for c, t in COMMENT_SCHEMA.items()
            if c not in {"first_name", "last_name", "organization", "category"}
        }
        partition_dir = output / "comments" / "agency_code=EPA" / "docket_id=EPA-2024-0001" / "year=2024" / "month=6"
        partition_dir.mkdir(parents=True)
        existing = [
            {"comment_id": "C-OLD", "docket_id": "EPA-2024-0001", "agency_code": "EPA",
             "title": "existing", "comment": "already here", "document_type": "Public Comment",
             "posted_date": "2024-06-15T00:00:00Z", "modify_date": "2024-06-15",
             "receive_date": "2024-06-15", "attachments_json": None},
        ]
        write_parquet_from_dicts(partition_dir / "part-0.parquet", existing, old_schema)

        # New staging row carrying the full (evolved) schema.
        new_comment = [
            {"comment_id": "C-NEW", "docket_id": "EPA-2024-0001", "agency_code": "EPA",
             "first_name": "Ada", "last_name": "Lovelace", "organization": None, "category": "Individual",
             "title": "new", "comment": "just added", "document_type": "Public Comment",
             "posted_date": "2024-06-20T00:00:00Z", "modify_date": "2024-06-20",
             "receive_date": "2024-06-20", "attachments_json": None},
        ]
        write_staging("EPA", "comments", new_comment, staging, COMMENT_SCHEMA)

        changed = merge_comments_partitioned(staging, output, COMMENT_SCHEMA, "comment_id")
        assert len(changed) == 1

        df = pl.read_parquet(changed[0])
        assert set(df["comment_id"].to_list()) == {"C-OLD", "C-NEW"}
        # The old row backfills the new columns as NULL.
        old_row = df.filter(pl.col("comment_id") == "C-OLD")
        assert old_row["organization"][0] is None
        assert old_row["category"][0] is None
        # The new row keeps its recovered submitter values.
        new_row = df.filter(pl.col("comment_id") == "C-NEW")
        assert new_row["first_name"][0] == "Ada"
        assert new_row["category"][0] == "Individual"

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


class TestDocumentAttachmentColumns:
    """End-to-end coverage that the document attachment columns survive the
    transform layer: extract -> staging Parquet -> DuckDB merge -> read back."""

    def test_attachment_columns_survive_staging_and_merge(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        # Start from the real regulations.gov sample so the staging schema cast
        # and merge are exercised against an actual payload.
        raw = json.loads((SAMPLE_DATA / "document-ACF-2025-0038-0001.json").read_text())
        record = DOCUMENT.extract(raw)
        write_staging("ACF", "documents", [record], staging, DOCUMENT_SCHEMA)

        merge_staging_files(
            staging, output, ["documents"],
            {"documents": DOCUMENT_SCHEMA},
            {"documents": "document_id"},
        )

        merged = pl.read_parquet(output / "documents.parquet")
        assert merged.height == 1
        row = merged.row(0, named=True)
        assert row["document_id"] == "ACF-2025-0038-0001"
        assert row["fr_doc_num"] == "2025-13790"
        assert json.loads(row["attachments_json"]) == [
            {
                "url": "https://downloads.regulations.gov/ACF-2025-0038-0001/content.pdf",
                "format": "pdf",
                "size": 239826,
            }
        ]

    def test_merge_unions_new_attachment_columns_into_existing_output(self, tmp_path):
        # Schema evolution: an existing documents.parquet written before these
        # columns existed should merge cleanly, with the new columns NULL on the
        # old row and populated on the freshly-extracted one.
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()

        legacy_schema = {k: v for k, v in DOCUMENT_SCHEMA.items() if k not in ("attachments_json", "fr_doc_num")}
        existing = [{
            "document_id": "OLD-001", "docket_id": "ACF-2025-0038", "agency_code": "ACF",
            "title": "old", "document_type": "Notice",
            "posted_date": "2024-01-01", "modify_date": "2024-01-01",
            "comment_start_date": None, "comment_end_date": None, "file_url": None,
            "withdrawn": "false", "reason_withdrawn": None, "additional_rins": None,
        }]
        write_parquet_from_dicts(output / "documents.parquet", existing, legacy_schema)

        raw = json.loads((SAMPLE_DATA / "document-ACF-2025-0038-0001.json").read_text())
        write_staging("ACF", "documents", [DOCUMENT.extract(raw)], staging, DOCUMENT_SCHEMA)

        merge_staging_files(
            staging, output, ["documents"],
            {"documents": DOCUMENT_SCHEMA},
            {"documents": "document_id"},
        )

        merged = pl.read_parquet(output / "documents.parquet").sort("document_id")
        assert merged.height == 2
        assert "attachments_json" in merged.columns
        assert "fr_doc_num" in merged.columns
        old_row = merged.filter(pl.col("document_id") == "OLD-001").row(0, named=True)
        assert old_row["attachments_json"] is None
        assert old_row["fr_doc_num"] is None
        new_row = merged.filter(pl.col("document_id") == "ACF-2025-0038-0001").row(0, named=True)
        assert new_row["fr_doc_num"] == "2025-13790"
