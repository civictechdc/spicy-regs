"""Shared fixtures for pipeline tests."""

from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def tmp_output(tmp_path: Path) -> Path:
    """Return a temporary output directory."""
    out = tmp_path / "output"
    out.mkdir()
    return out


@pytest.fixture
def sample_dockets() -> list[dict]:
    return [
        {
            "docket_id": "EPA-2024-0001",
            "agency_code": "EPA",
            "title": "Clean Air Standards",
            "docket_type": "Rulemaking",
            "modify_date": "2024-06-15",
            "abstract": "Proposed rule for clean air",
        },
        {
            "docket_id": "FDA-2024-0010",
            "agency_code": "FDA",
            "title": "Drug Labeling",
            "docket_type": "Rulemaking",
            "modify_date": "2024-05-01",
            "abstract": "Updated drug labeling requirements",
        },
        {
            "docket_id": "EPA-2024-0002",
            "agency_code": "EPA",
            "title": "Water Quality",
            "docket_type": "Nonrulemaking",
            "modify_date": "2024-07-20",
            "abstract": None,
        },
    ]


@pytest.fixture
def sample_comments() -> list[dict]:
    return [
        {"comment_id": "C-001", "docket_id": "EPA-2024-0001", "agency_code": "EPA", "title": "Support", "comment": "I support this", "document_type": "Public Comment", "posted_date": "2024-06-20", "modify_date": "2024-06-20", "receive_date": "2024-06-20", "attachments_json": None},
        {"comment_id": "C-002", "docket_id": "EPA-2024-0001", "agency_code": "EPA", "title": "Oppose", "comment": "I oppose this", "document_type": "Public Comment", "posted_date": "2024-06-21", "modify_date": "2024-06-21", "receive_date": "2024-06-21", "attachments_json": None},
        {"comment_id": "C-003", "docket_id": "FDA-2024-0010", "agency_code": "FDA", "title": "Question", "comment": "What about X?", "document_type": "Public Comment", "posted_date": "2024-05-10", "modify_date": "2024-05-10", "receive_date": "2024-05-10", "attachments_json": None},
        {"comment_id": "C-004", "docket_id": "EPA-2024-0002", "agency_code": "EPA", "title": "Feedback", "comment": "More data needed", "document_type": "Public Comment", "posted_date": "2024-07-25", "modify_date": "2024-07-25", "receive_date": "2024-07-25", "attachments_json": None},
    ]


@pytest.fixture
def sample_documents() -> list[dict]:
    return [
        {"document_id": "D-001", "docket_id": "EPA-2024-0001", "agency_code": "EPA", "title": "Proposed Rule", "document_type": "Proposed Rule", "posted_date": "2024-06-01", "modify_date": "2024-06-01", "comment_start_date": "2024-06-01", "comment_end_date": "2024-07-01", "file_url": None},
        {"document_id": "D-002", "docket_id": "FDA-2024-0010", "agency_code": "FDA", "title": "Notice", "document_type": "Notice", "posted_date": "2024-04-15", "modify_date": "2024-04-15", "comment_start_date": "2024-04-15", "comment_end_date": "2024-05-15", "file_url": None},
    ]


DOCKET_SCHEMA = {
    "docket_id": pl.Utf8,
    "agency_code": pl.Utf8,
    "title": pl.Utf8,
    "docket_type": pl.Utf8,
    "modify_date": pl.Utf8,
    "abstract": pl.Utf8,
}

COMMENT_SCHEMA = {
    "comment_id": pl.Utf8,
    "docket_id": pl.Utf8,
    "agency_code": pl.Utf8,
    "title": pl.Utf8,
    "comment": pl.Utf8,
    "document_type": pl.Utf8,
    "posted_date": pl.Utf8,
    "modify_date": pl.Utf8,
    "receive_date": pl.Utf8,
    "attachments_json": pl.Utf8,
}

DOCUMENT_SCHEMA = {
    "document_id": pl.Utf8,
    "docket_id": pl.Utf8,
    "agency_code": pl.Utf8,
    "title": pl.Utf8,
    "document_type": pl.Utf8,
    "posted_date": pl.Utf8,
    "modify_date": pl.Utf8,
    "comment_start_date": pl.Utf8,
    "comment_end_date": pl.Utf8,
    "file_url": pl.Utf8,
}


def write_parquet_from_dicts(path: Path, records: list[dict], schema: dict) -> None:
    """Helper to write a list of dicts as a Parquet file using Polars."""
    df = pl.DataFrame(records, schema=schema)
    df.write_parquet(path, compression="zstd")
