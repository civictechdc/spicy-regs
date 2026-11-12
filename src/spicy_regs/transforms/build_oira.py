"""Transforms: build ``oira_reviews.parquet`` and ``oira_meetings.parquet`` from reginfo.gov.

Both tables cover OIRA's Executive Order 12866 review of significant rules, the
lifecycle stage between an agency finishing a draft and the rule appearing in
the Federal Register. Both are keyed by RIN, so they join to ``unified_agenda``
and ``federal_register``. The reginfo.gov XML exports are documented in
:mod:`spicy_regs.sources.oira`.

Both builds are **full refreshes**, not incremental merges. The whole review
history (1981 onward) is ~20 MB of XML across ~50 files, so re-reading it every
run is cheap. A full refresh also picks up reginfo.gov's later corrections
(``DATE_PUBLISHED`` is filled in months after review concludes) and drops
pending reviews once they conclude, with no merge key needed. That matters
because reviews have no ID in the export (see :func:`_shape_review`).

If any listed export fails to download, the reader raises and nothing is
written, so a partial fetch can never be published as the whole table.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources.oira import OiraMeetingsReader, OiraReviewsReader, OiraUnavailable

REVIEWS_OUTPUT = "oira_reviews.parquet"
MEETINGS_OUTPUT = "oira_meetings.parquet"

REVIEW_COLUMNS = (
    "rin",
    "agency_code",
    "agency_name",
    "title",
    "stage",
    "review_status",
    "received_date",
    "concluded_date",
    "review_days",
    "review_outcome",
    "economically_significant",
    "major",
    "legal_deadline",
    "review_extended",
    "published_date",
    "flags_json",
    "source_file",
)
_REVIEW_SCHEMA = pa.schema([(c, pa.int64() if c == "review_days" else pa.string()) for c in REVIEW_COLUMNS])

MEETING_COLUMNS = (
    "meeting_id",
    "rin",
    "agency_code",
    "agency_name",
    "title",
    "rule_stage",
    "meeting_date",
    "meeting_status",
    "requestor_organization",
    "requestor_name",
    "summary",
    "source_file",
    "url",
)
_MEETING_SCHEMA = pa.schema([(c, pa.string()) for c in MEETING_COLUMNS])

UNDER_REVIEW = "Under Review"
CONCLUDED = "Concluded"

AgencyMap = dict[str, tuple[str | None, str | None]]


def _days_between(start: str | None, end: str | None) -> int | None:
    """Calendar days from ``start`` to ``end`` (ISO dates), or None if either is missing."""
    if not start or not end:
        return None
    return (date.fromisoformat(end) - date.fromisoformat(start)).days


def _agency(
    agencies: AgencyMap, number: str | None, fallback_acronym: str | None = None
) -> tuple[str | None, str | None]:
    """Resolve ``(agency_code, agency_name)`` for a numeric agency code.

    ``agency_code`` is the acronym, matching ``unified_agenda.agency_code``.
    Without a match in the agency list it falls back to ``fallback_acronym``'s
    last path segment (``USDA/AMS`` -> ``AMS``), then to the numeric code.
    """
    acronym, name = agencies.get(number or "", (None, None))
    if acronym:
        return acronym, name
    if fallback_acronym:
        return fallback_acronym.rsplit("/", 1)[-1], name
    return number, name


def _shape_review(doc: dict, agencies: AgencyMap) -> dict:
    """Map one normalized review dict onto the published ``oira_reviews`` columns.

    There is no review ID in the export, and (rin, stage, received_date) is
    not unique in old files, so the table has no primary key. Pending reviews
    carry no ``concluded_date``/``review_outcome``; ``review_days`` is the
    calendar days OIRA held the rule and is null while it is still pending.
    """
    agency_code, agency_name = _agency(agencies, doc.get("agency_number"))
    received, concluded = doc.get("received_date"), doc.get("concluded_date")
    flags = doc.get("flags") or {}
    return {
        "rin": doc.get("rin"),
        "agency_code": agency_code,
        "agency_name": agency_name,
        "title": doc.get("title"),
        "stage": doc.get("stage"),
        "review_status": UNDER_REVIEW if doc.get("under_review") else CONCLUDED,
        "received_date": received,
        "concluded_date": concluded,
        "review_days": _days_between(received, concluded),
        "review_outcome": doc.get("decision"),
        "economically_significant": doc.get("economically_significant"),
        "major": doc.get("major"),
        "legal_deadline": doc.get("legal_deadline"),
        "review_extended": doc.get("review_extended"),
        "published_date": doc.get("published_date"),
        "flags_json": json.dumps(flags, sort_keys=True) if flags else None,
        "source_file": doc.get("source_file"),
    }


def _meeting_url(rin: str | None, meeting_id: str | None) -> str | None:
    if not rin or not meeting_id:
        return None
    return f"https://www.reginfo.gov/public/do/viewEO12866Meeting?viewRule=true&rin={rin}&meetingId={meeting_id}"


def _shape_meeting(doc: dict, agencies: AgencyMap) -> dict:
    """Map one normalized meeting dict onto the published ``oira_meetings`` columns."""
    agency_code, agency_name = _agency(agencies, doc.get("agency_number"), doc.get("agency_acronym"))
    rin, meeting_id = doc.get("rin"), doc.get("meeting_id")
    return {
        "meeting_id": meeting_id,
        "rin": rin,
        "agency_code": agency_code,
        "agency_name": agency_name,
        "title": doc.get("title"),
        "rule_stage": doc.get("rule_stage"),
        "meeting_date": doc.get("meeting_date"),
        "meeting_status": doc.get("meeting_status"),
        "requestor_organization": doc.get("requestor_organization"),
        "requestor_name": doc.get("requestor_name"),
        "summary": doc.get("summary"),
        "source_file": doc.get("source_file"),
        "url": _meeting_url(rin, meeting_id),
    }


def build_oira_reviews(output_dir: Path, *, reader: OiraReviewsReader | None = None) -> Path:
    """Build ``oira_reviews.parquet``: every EO 12866 review, concluded and pending."""
    reader = reader or OiraReviewsReader()
    docs = [doc for doc in reader.iter_records() if doc.get("rin")]
    if not docs:
        raise OiraUnavailable("no EO 12866 review records fetched")
    rows = [_shape_review(doc, reader.agencies) for doc in docs]
    rows.sort(key=lambda r: (r["received_date"] or "", r["rin"] or ""), reverse=True)

    out_file = output_dir / REVIEWS_OUTPUT
    pq.write_table(pa.Table.from_pylist(rows, schema=_REVIEW_SCHEMA), out_file, compression="zstd")
    pending = sum(1 for r in rows if r["review_status"] == UNDER_REVIEW)
    logger.info(
        "OIRA reviews: {:,} rows ({:,} under review) from {} files", len(rows), pending, len(reader.file_counts)
    )
    return out_file


def build_oira_meetings(output_dir: Path, *, reader: OiraMeetingsReader | None = None) -> Path:
    """Build ``oira_meetings.parquet``: OIRA's EO 12866 meeting log, one row per log entry.

    Rows are kept as published, since ``meeting_id`` is not always present and
    a meeting with several requesting parties legitimately spans several rows.
    """
    reader = reader or OiraMeetingsReader()
    docs = [doc for doc in reader.iter_records() if doc.get("rin")]
    if not docs:
        raise OiraUnavailable("no EO 12866 meeting records fetched")
    rows = [_shape_meeting(doc, reader.agencies) for doc in docs]
    rows.sort(key=lambda r: (r["meeting_date"] or "", r["rin"] or "", r["requestor_organization"] or ""), reverse=True)

    out_file = output_dir / MEETINGS_OUTPUT
    pq.write_table(pa.Table.from_pylist(rows, schema=_MEETING_SCHEMA), out_file, compression="zstd")
    logger.info("OIRA meetings: {:,} rows from {} files", len(rows), len(reader.file_counts))
    return out_file
