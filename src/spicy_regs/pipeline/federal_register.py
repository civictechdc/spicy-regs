"""
Federal Register API extractor.

Walks https://www.federalregister.gov/api/v1/documents.json by month,
flattens each document into the all-Utf8 staging schema, and dedups
against the shared bloom-filter manifest using `fr:{document_number}`
as the key namespace.

Why month chunks: the API caps each filter combination at 10,000
results (`page * per_page <= 10000`). One month never exceeds ~5K docs,
so month-chunked queries comfortably fit under the cap.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from json import dumps as json_dumps
from pathlib import Path
from time import sleep
from typing import Any, Iterator

import httpx
from loguru import logger

from spicy_regs.pipeline.transform import write_staging


API_BASE = "https://www.federalregister.gov/api/v1"
DOCUMENTS_URL = f"{API_BASE}/documents.json"
PER_PAGE = 1000

# All four FR document type codes.
DOC_TYPES = ("RULE", "PRORULE", "NOTICE", "PRESDOCU")

# Only request fields we actually persist — shrinks each response by ~70%
# and keeps us off the slowest API codepath.
API_FIELDS = (
    "document_number",
    "title",
    "abstract",
    "type",
    "publication_date",
    "effective_on",
    "comments_close_on",
    "signing_date",
    "agencies",
    "docket_ids",
    "regulation_id_numbers",
    "cfr_references",
    "html_url",
    "pdf_url",
    "body_html_url",
    "volume",
    "start_page",
    "end_page",
    "subtype",
    "executive_order_number",
)


def _fr_get(client: httpx.Client, url: str, params: dict | None = None) -> dict:
    """GET with retry + 429-aware backoff. Honors `Retry-After` when set."""
    for attempt in range(5):
        try:
            r = client.get(url, params=params, timeout=60.0)
            if r.status_code == 429:
                wait = float(r.headers.get("Retry-After", "30"))
                logger.warning("FR API 429; sleeping {}s", wait)
                sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            if attempt == 4:
                raise
            backoff = 2 ** attempt
            logger.warning("FR API error ({}), retry {}/5 in {}s", e, attempt + 1, backoff)
            sleep(backoff)
    raise RuntimeError("unreachable")  # for type-checker


def _extract_fr(d: dict) -> dict | None:
    """Flatten one Federal Register document into the staging schema.

    Returns None if document_number is missing — the API occasionally
    returns malformed entries during corrections.
    """
    doc_num = d.get("document_number")
    if not doc_num:
        return None

    agencies = d.get("agencies") or []
    agency_slugs = ",".join(a.get("slug", "") for a in agencies if a.get("slug"))
    pub_date = d.get("publication_date")

    return {
        "document_number": doc_num,
        "title": d.get("title"),
        "abstract": d.get("abstract"),
        "document_type": d.get("type"),
        "publication_date": pub_date,
        "effective_on": d.get("effective_on"),
        "comments_close_on": d.get("comments_close_on"),
        "signing_date": d.get("signing_date"),
        "agencies_json": json_dumps(agencies) if agencies else None,
        "agency_slugs": agency_slugs or None,
        "docket_ids_json": json_dumps(d["docket_ids"]) if d.get("docket_ids") else None,
        "regulation_id_numbers_json": (
            json_dumps(d["regulation_id_numbers"])
            if d.get("regulation_id_numbers") else None
        ),
        "cfr_references_json": (
            json_dumps(d["cfr_references"]) if d.get("cfr_references") else None
        ),
        "html_url": d.get("html_url"),
        "pdf_url": d.get("pdf_url"),
        "body_html_url": d.get("body_html_url"),
        # ints stored as strings to match the all-Utf8 merge CAST pattern.
        "volume": str(v) if (v := d.get("volume")) is not None else None,
        "start_page": str(v) if (v := d.get("start_page")) is not None else None,
        "end_page": str(v) if (v := d.get("end_page")) is not None else None,
        "subtype": d.get("subtype"),
        "executive_order_number": d.get("executive_order_number"),
        # Use publication_date so the existing dedup query
        # (`ORDER BY modify_date DESC`) keeps the latest version when
        # a doc is corrected and re-fetched.
        "modify_date": pub_date,
    }


def _iter_month(
    client: httpx.Client,
    year: int,
    month: int,
) -> Iterator[dict]:
    """Yield raw FR document dicts for one month, following next_page_url."""
    start = date(year, month, 1)
    # End-of-month: roll to next month then back one day.
    if month == 12:
        next_first = date(year + 1, 1, 1)
    else:
        next_first = date(year, month + 1, 1)
    end = date.fromordinal(next_first.toordinal() - 1)

    params: dict[str, Any] = {
        "per_page": PER_PAGE,
        "conditions[publication_date][gte]": start.isoformat(),
        "conditions[publication_date][lte]": end.isoformat(),
        "fields[]": list(API_FIELDS),
        "conditions[type][]": list(DOC_TYPES),
    }

    url: str | None = DOCUMENTS_URL
    first = True
    while url:
        # First request uses params; subsequent requests follow next_page_url
        # which already encodes everything.
        payload = _fr_get(client, url, params=params if first else None)
        first = False
        for result in payload.get("results", []):
            yield result
        url = payload.get("next_page_url")


def _process_month(
    year: int,
    month: int,
    staging_dir: Path,
    processed_keys: Any,
    schema: dict,
) -> tuple[int, list[str]]:
    """Fetch one month, dedup against bloom filter, write staging parquet."""
    records: list[dict] = []
    new_keys: list[str] = []
    skipped = 0

    # One client per worker — httpx clients aren't thread-safe to share.
    with httpx.Client(http2=False, headers={"User-Agent": "spicy-regs-etl/0.1"}) as client:
        for raw in _iter_month(client, year, month):
            doc_num = raw.get("document_number")
            if not doc_num:
                continue
            key = f"fr:{doc_num}"
            if processed_keys and key in processed_keys:
                skipped += 1
                continue
            row = _extract_fr(raw)
            if row is None:
                continue
            records.append(row)
            new_keys.append(key)

    rows_written = write_staging(
        agency=f"{year}-{month:02d}",
        data_type="federal_register",
        records=records,
        staging_dir=staging_dir,
        schema=schema,
    )
    logger.info(
        "[FR {:04d}-{:02d}] {} new, {} skipped",
        year, month, rows_written, skipped,
    )
    return rows_written, new_keys


def fetch_federal_register(
    staging_dir: Path,
    processed_keys: Any,
    schema: dict,
    since_year: int = 2000,
    until_year: int | None = None,
    max_workers: int = 4,
) -> tuple[int, list[str]]:
    """Walk Federal Register publications by month, write staging parquet per month.

    Returns (total_rows_written, list_of_new_manifest_keys).
    """
    today = date.today()
    end_year = until_year if until_year is not None else today.year
    end_month = 12 if until_year is not None and until_year < today.year else today.month

    months: list[tuple[int, int]] = []
    for y in range(since_year, end_year + 1):
        m_start = 1
        m_end = 12
        if y == end_year:
            m_end = end_month
        for m in range(m_start, m_end + 1):
            months.append((y, m))

    logger.info(
        "Fetching Federal Register: {} months ({}-01 → {}-{:02d}), {} workers",
        len(months), since_year, end_year, end_month, max_workers,
    )

    total_rows = 0
    all_new_keys: list[str] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_process_month, y, m, staging_dir, processed_keys, schema): (y, m)
            for y, m in months
        }
        for future in as_completed(futures):
            y, m = futures[future]
            try:
                rows, new_keys = future.result()
                total_rows += rows
                all_new_keys.extend(new_keys)
            except Exception as e:
                logger.error("[FR {:04d}-{:02d}] failed: {}", y, m, e)

    logger.info("FR total: {:,} new rows, {:,} new keys", total_rows, len(all_new_keys))
    return total_rows, all_new_keys
