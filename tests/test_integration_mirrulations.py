"""Live integration test against real regulations.gov data.

Fetches a real document JSON straight from the public Mirrulations S3 mirror
(``s3://mirrulations`` — the pipeline's actual upstream) and runs it through the
production extract -> staging -> merge path, asserting the document attachment
metadata (``attachments_json`` / ``fr_doc_num``) survives end to end.

Marked ``integration`` so it is excluded from the default hermetic suite (see
``addopts`` in pyproject.toml); the dedicated "Integration (live data)" CI
workflow runs it via ``pytest -m integration``. It needs outbound network access
to the anonymous S3 bucket.
"""

import json

import boto3
import polars as pl
import pytest
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from loguru import logger

from spicy_regs.sources.mirrulations import download_and_parse
from spicy_regs.transforms import merge_staging_files, write_staging
from spicy_regs.schemas import DOCUMENT
from tests.conftest import DOCUMENT_SCHEMA

BUCKET = "mirrulations"

# A real, published EPA notice in the mirror. Posted documents are immutable, so
# the identifiers below are stable; file sizes are asserted structurally (present
# and positive) rather than exactly, in case regulations.gov re-renders a file.
DOC_KEY = (
    "raw-data/EPA/EPA-HQ-OA-2025-0172/text-EPA-HQ-OA-2025-0172/"
    "documents/EPA-HQ-OA-2025-0172-0001.json"
)
EXPECTED_DOCUMENT_ID = "EPA-HQ-OA-2025-0172-0001"
EXPECTED_DOCKET_ID = "EPA-HQ-OA-2025-0172"
EXPECTED_FR_DOC_NUM = "2025-16478"


@pytest.mark.integration
def test_real_document_attachment_metadata_through_transform(tmp_path):
    s3 = boto3.resource(
        "s3", region_name="us-east-1", config=BotoConfig(signature_version=UNSIGNED)
    )

    logger.info("Fetching live document s3://{}/{}", BUCKET, DOC_KEY)
    # download_and_parse is the exact function the pipeline uses per file.
    record = download_and_parse(s3, BUCKET, DOC_KEY, DOCUMENT.extract)
    assert record is not None, f"could not fetch {DOC_KEY} from s3://{BUCKET}"
    logger.success(
        "Fetched + extracted {} (docket {}, fr_doc_num {})",
        record["document_id"], record["docket_id"], record["fr_doc_num"],
    )

    staging = tmp_path / "staging"
    output = tmp_path / "output"
    output.mkdir()

    logger.info("Writing staging Parquet under {}", staging)
    write_staging("EPA", "documents", [record], staging, DOCUMENT_SCHEMA)
    logger.info("Merging staging -> {}/documents.parquet", output)
    merge_staging_files(
        staging, output, ["documents"],
        {"documents": DOCUMENT_SCHEMA},
        {"documents": "document_id"},
    )

    merged = pl.read_parquet(output / "documents.parquet")
    logger.info("Merged output: {} row(s), columns={}", merged.height, merged.columns)
    assert merged.height == 1
    row = merged.row(0, named=True)

    assert row["document_id"] == EXPECTED_DOCUMENT_ID
    assert row["docket_id"] == EXPECTED_DOCKET_ID
    assert row["fr_doc_num"] == EXPECTED_FR_DOC_NUM

    attachments = json.loads(row["attachments_json"])
    logger.info(
        "Recovered {} attachment(s): {}",
        len(attachments),
        ", ".join(f"{a['format']}={a['size']}B" for a in attachments),
    )
    assert len(attachments) >= 2
    assert {a["format"] for a in attachments} >= {"pdf", "html"}
    for a in attachments:
        assert a["url"].startswith("https://downloads.regulations.gov/")
        assert isinstance(a["size"], int) and a["size"] > 0

    # file_url stays the first usable rendition for backward compatibility.
    assert row["file_url"] == attachments[0]["url"]
    logger.success(
        "Document attachment metadata survived extract -> staging -> merge "
        "for {}", row["document_id"],
    )
