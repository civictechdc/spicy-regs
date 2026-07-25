"""Read official Supreme Court opinion metadata and PDF source bytes.

The Court's term index is the authority for case identity, docket number,
decision date, reporter citation, holding summary, and the official PDF URL.
Beautiful Soup handles the source HTML; this adapter owns only the small,
source-specific mapping into raw records. PDF text extraction belongs to the
transform layer.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Sequence
from contextlib import nullcontext
from datetime import date, datetime
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from loguru import logger

from spicy_regs.sources.base import Reader

SCOTUS_BASE_URL = "https://www.supremecourt.gov"
TERM_INDEX_URL = SCOTUS_BASE_URL + "/opinions/slipopinion/{term_code}"
_TIMEOUT = httpx.Timeout(120.0, connect=30.0)
_MAX_RETRIES = 5


def current_term_year(today: date | None = None) -> int:
    """Return the calendar year in which the current October Term began."""
    value = today or date.today()
    return value.year if value.month >= 10 else value.year - 1


def term_code(term_year: int) -> str:
    """Return the two-digit official-site code for a term year."""
    if not 2000 <= term_year <= 2099:
        raise ValueError("Supreme Court term year must be between 2000 and 2099")
    return str(term_year)[-2:]


def _iso_date(value: str) -> str:
    try:
        return datetime.strptime(value.strip(), "%m/%d/%y").date().isoformat()
    except ValueError as exc:
        raise ValueError(f"invalid Supreme Court decision date: {value!r}") from exc


def parse_term_index(html: str, *, term_year: int) -> list[dict[str, str]]:
    """Parse one official term index into stable source metadata."""
    code = term_code(term_year)
    index_url = TERM_INDEX_URL.format(term_code=code)
    soup = BeautifulSoup(html, "html.parser")
    records: list[dict[str, str]] = []
    for row in soup.select("tr"):
        cells = row.find_all("td", recursive=False)
        if len(cells) < 6:
            continue
        anchor = cells[3].find("a", href=True)
        if anchor is None:
            continue
        href = str(anchor.get("href") or "").strip()
        pdf_url = urljoin(SCOTUS_BASE_URL, href)
        parsed = urlparse(pdf_url)
        expected_prefix = f"/opinions/{code}pdf/"
        if (
            parsed.scheme != "https"
            or parsed.netloc.casefold() != "www.supremecourt.gov"
            or not parsed.path.startswith(expected_prefix)
            or not parsed.path.casefold().endswith(".pdf")
        ):
            raise ValueError(f"unsafe Supreme Court opinion URL: {pdf_url}")
        records.append(
            {
                "term_year": str(term_year),
                "release_number": cells[0].get_text(" ", strip=True),
                "date_decided": _iso_date(cells[1].get_text(" ", strip=True)),
                "docket_number": cells[2].get_text(" ", strip=True),
                "case_name": anchor.get_text(" ", strip=True),
                "holding": str(anchor.get("title") or "").strip(),
                "author_code": cells[4].get_text(" ", strip=True),
                "citation": cells[5].get_text(" ", strip=True),
                "source_index_url": index_url,
                "source_url": pdf_url,
            }
        )
    if not records:
        raise ValueError(
            f"Supreme Court term index {term_year} contained no opinion rows"
        )
    return records


class SupremeCourtOpinionsReader(Reader):
    """Yield official opinion metadata plus exact PDF bytes for selected terms."""

    def __init__(
        self,
        *,
        term_years: Sequence[int] | None = None,
        max_records: int | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        years = tuple(
            term_years
            if term_years is not None
            else (current_term_year(),)
        )
        if not years:
            raise ValueError("at least one Supreme Court term year is required")
        if max_records is not None and max_records <= 0:
            raise ValueError("max_records must be positive")
        for year in years:
            term_code(year)
        self.term_years = tuple(sorted(set(years), reverse=True))
        self.max_records = max_records
        self.client = client

    def _get(self, client: httpx.Client, url: str) -> httpx.Response:
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                response = client.get(url)
                if response.status_code == 429 or response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        "retryable Supreme Court response",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                return response
            except httpx.HTTPError:
                if attempt == _MAX_RETRIES:
                    raise
                time.sleep(min(2**attempt, 30))
        raise AssertionError("unreachable retry loop")

    def iter_records(self) -> Iterator[dict[str, Any]]:
        owned = self.client is None
        context = (
            httpx.Client(
                timeout=_TIMEOUT,
                follow_redirects=True,
                headers={"User-Agent": "spicy-regs/0.1 court-opinion-ingest"},
            )
            if owned
            else nullcontext(self.client)
        )
        count = 0
        with context as client:
            assert client is not None
            for year in self.term_years:
                index_url = TERM_INDEX_URL.format(
                    term_code=term_code(year)
                )
                index_response = self._get(client, index_url)
                records = parse_term_index(
                    index_response.text,
                    term_year=year,
                )
                logger.info(
                    "Supreme Court: term {} index has {} opinions",
                    year,
                    len(records),
                )
                for record in records:
                    pdf_response = self._get(
                        client,
                        record["source_url"],
                    )
                    media_type = (
                        pdf_response.headers.get("content-type", "")
                        .split(";", 1)[0]
                        .strip()
                        .casefold()
                    )
                    if media_type not in {
                        "application/pdf",
                        "application/octet-stream",
                    }:
                        raise ValueError(
                            "Supreme Court opinion response is not a PDF: "
                            f"{record['source_url']} ({media_type or 'missing'})"
                        )
                    yield {
                        **record,
                        "source_bytes": pdf_response.content,
                        "etag": pdf_response.headers.get("etag"),
                        "last_modified": pdf_response.headers.get(
                            "last-modified"
                        ),
                    }
                    count += 1
                    if (
                        self.max_records is not None
                        and count >= self.max_records
                    ):
                        return
