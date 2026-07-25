"""Build ``court_opinions.parquet`` from official Supreme Court PDFs.

One row represents one opinion package published on the Court's term index.
The package can contain the Court opinion plus separate concurrences or
dissents; those source-native parts are not split heuristically. The PDF digest
and extracted representation make source revisions explicit.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Sequence
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.supreme_court_opinions import (
    SupremeCourtOpinionsReader,
)
from spicy_regs.transforms.pdf_text import extract_pdf_text

OUTPUT = "court_opinions.parquet"

COLUMNS = (
    "opinion_id",
    "court_id",
    "term_year",
    "release_number",
    "date_decided",
    "docket_number",
    "case_name",
    "holding",
    "author_code",
    "citation",
    "opinion_type",
    "source_index_url",
    "source_url",
    "source_etag",
    "source_last_modified",
    "source_bytes",
    "pdf_sha256",
    "pdf_page_count",
    "text_extraction_status",
    "text_extraction_method",
    "text_extraction_version",
    "pdf_text",
)
SCHEMA = pa.schema([(column, pa.string()) for column in COLUMNS])


def _slug(value: object) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value).casefold()).strip("-")
    if not slug:
        raise ValueError("Supreme Court opinion identity component is blank")
    return slug


def _shape(record: dict[str, Any]) -> dict[str, str | None]:
    """Map one reader record into the pinned published schema."""
    source_bytes = record.get("source_bytes")
    if not isinstance(source_bytes, bytes):
        raise ValueError("Supreme Court opinion record lacks PDF bytes")
    extraction = extract_pdf_text(source_bytes)
    if not extraction.ok:
        raise ValueError(
            "Supreme Court opinion PDF did not yield text: "
            f"{record.get('source_url')} ({extraction.status.value})"
        )
    term_year = str(record.get("term_year") or "")
    release_number = str(record.get("release_number") or "")
    docket_number = str(record.get("docket_number") or "")
    opinion_id = "-".join(
        (
            "scotus",
            _slug(term_year),
            _slug(release_number),
            _slug(docket_number),
        )
    )
    return {
        "opinion_id": opinion_id,
        "court_id": "scotus",
        "term_year": term_year,
        "release_number": release_number,
        "date_decided": str(record.get("date_decided") or ""),
        "docket_number": docket_number,
        "case_name": str(record.get("case_name") or ""),
        "holding": str(record.get("holding") or ""),
        "author_code": str(record.get("author_code") or ""),
        "citation": str(record.get("citation") or ""),
        "opinion_type": "official-opinion-package",
        "source_index_url": str(record.get("source_index_url") or ""),
        "source_url": str(record.get("source_url") or ""),
        "source_etag": (
            str(record["etag"]) if record.get("etag") is not None else None
        ),
        "source_last_modified": (
            str(record["last_modified"])
            if record.get("last_modified") is not None
            else None
        ),
        "source_bytes": str(len(source_bytes)),
        "pdf_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "pdf_page_count": str(extraction.page_count),
        "text_extraction_status": extraction.status.value,
        "text_extraction_method": "pypdf-embedded-text",
        "text_extraction_version": version("pypdf"),
        "pdf_text": extraction.text,
    }


def _merged_rows(
    prior: Iterable[dict[str, Any]],
    current: Iterable[dict[str, str | None]],
) -> list[dict[str, Any]]:
    by_id = {
        str(row["opinion_id"]): {
            column: row.get(column) for column in COLUMNS
        }
        for row in prior
        if row.get("opinion_id")
    }
    for row in current:
        by_id[str(row["opinion_id"])] = dict(row)
    return sorted(
        by_id.values(),
        key=lambda row: (
            str(row.get("date_decided") or ""),
            str(row.get("opinion_id") or ""),
        ),
        reverse=True,
    )


def build_supreme_court_opinions(
    output_dir: Path,
    *,
    term_years: Sequence[int] | None = None,
    max_records: int | None = None,
    records: Iterable[dict[str, Any]] | None = None,
) -> Path:
    """Fetch selected terms, merge prior rows, and write the opinion table."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / OUTPUT
    prior_file = output_dir / ".court-opinions-prior.parquet"
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    prior_rows = (
        pq.read_table(prior_file).to_pylist()
        if have_prior
        else []
    )
    source_records = (
        records
        if records is not None
        else SupremeCourtOpinionsReader(
            term_years=term_years,
            max_records=max_records,
        ).iter_records()
    )
    shaped = [_shape(record) for record in source_records]
    rows = _merged_rows(prior_rows, shaped)
    if not rows:
        raise RuntimeError("Supreme Court opinion ingest produced no rows")
    pq.write_table(
        pa.Table.from_pylist(rows, schema=SCHEMA),
        out_file,
        compression="zstd",
    )
    prior_file.unlink(missing_ok=True)
    logger.info(
        "Court opinions: {:,} rows ({} fetched this run)",
        len(rows),
        len(shaped),
    )
    return out_file
