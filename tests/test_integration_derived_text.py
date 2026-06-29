"""Live integration test for Mirrulations derived-data comment text.

Reads a real comment JSON from the public Mirrulations S3 mirror, flattens it,
then runs the production ``ExtractRecords -> EnrichCommentText`` chain and
asserts the comment's attachment text was pulled straight from the bucket's
``derived-data`` prefix (no PDF download).

ACF-2025-0038-0004 is a Public Submission whose inline ``comment`` is just
"See attached file(s)"; its substance lives in an attachment that Mirrulations
has already extracted. Posted comments are immutable, so the identifiers are
stable.

Marked ``integration`` so it is excluded from the default hermetic suite (see
``addopts`` in pyproject.toml); the "Integration (live data)" workflow runs it
via ``pytest -m integration``. Needs outbound access to the anonymous S3 bucket.
"""

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from loguru import logger

from spicy_regs.pipeline.extract import download_and_parse
from spicy_regs.schemas import COMMENT
from spicy_regs.sources.derived_text import DerivedCommentText
from spicy_regs.transforms import Chain, EnrichCommentText, ExtractRecords

BUCKET = "mirrulations"

COMMENT_KEY = (
    "raw-data/ACF/ACF-2025-0038/text-ACF-2025-0038/"
    "comments/ACF-2025-0038-0004.json"
)
EXPECTED_COMMENT_ID = "ACF-2025-0038-0004"
EXPECTED_DOCKET_ID = "ACF-2025-0038"


@pytest.mark.integration
def test_real_comment_text_filled_from_derived_data() -> None:
    s3 = boto3.resource(
        "s3", region_name="us-east-1", config=BotoConfig(signature_version=UNSIGNED)
    )

    logger.info("Fetching live comment s3://{}/{}", BUCKET, COMMENT_KEY)
    payload = download_and_parse(s3, BUCKET, COMMENT_KEY, lambda d: d)
    assert payload is not None, f"could not fetch {COMMENT_KEY} from s3://{BUCKET}"

    fetcher = DerivedCommentText(s3)
    chain = Chain(ExtractRecords(COMMENT), EnrichCommentText(fetcher))
    (record,) = list(chain.apply([payload]))

    assert record["comment_id"] == EXPECTED_COMMENT_ID
    assert record["docket_id"] == EXPECTED_DOCKET_ID
    # Inline body is just the placeholder; the real content comes from the
    # extracted attachment text.
    assert record["text_extraction_status"] == "ok"
    assert record["text_content"]
    assert len(record["text_content"]) > 200
    logger.success(
        "Filled text_content from derived-data for {} ({} chars)",
        record["comment_id"],
        len(record["text_content"]),
    )
