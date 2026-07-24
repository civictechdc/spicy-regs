"""Reader connector for the federalregister.gov public REST API (v1).

Brings Federal Register ingestion *in-repo*. Previously ``federal_register.parquet``
was produced by a separate ingestion path and only consumed here (see
``transforms/build_fr_docket_links``); this reader lets this repo produce it.

The reader is a *pure source*: it yields raw FR document payloads (dicts) exactly
as the API returns them. Shaping them into the published 22-column schema is the
job of :func:`~spicy_regs.transforms.build_federal_register.build_federal_register`.

The FR documents endpoint truncates any single query to a bounded number of
results, so a naive "page through everything" loop silently drops documents on
busy date ranges. Instead we fetch **date windows** and, whenever a window comes
back truncated (fewer collected than the reported ``count``), split it in half
and recurse — down to a single day. This makes correctness independent of the
API's exact page cap.

No API key is required; federalregister.gov is fully open.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import date, timedelta

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

API_BASE = "https://www.federalregister.gov/api/v1"

# Max the API accepts per page. Windows are still subdivided on truncation, so
# this is only a round-trip-count optimization, not a correctness knob.
PER_PAGE = 1000

# The API caps both pagination and its reported ``count`` at 10,000. A response
# with exactly this count is therefore ambiguous and must be subdivided even
# when all 10,000 visible rows were collected.
RESULT_CAP = 10_000

# Avoid paying for a capped 10,000-row traversal of the full archive before
# discovering that it must be split. Quarter-sized top-level windows remain
# below the cap in normal Federal Register history; unusually busy quarters are
# still split recursively by ``_fetch_window``.
MAX_WINDOW_DAYS = 90

# FR's oldest documents. A full backfill with no prior table walks from here.
FR_EPOCH = date(1994, 1, 1)

# Fields requested from the documents endpoint. Names are the API's; the
# transform maps them onto the published column names (e.g. ``type`` ->
# ``document_type``, array fields -> ``*_json``).
FIELDS = (
    "document_number",
    "title",
    "abstract",
    "type",
    "subtype",
    "publication_date",
    "effective_on",
    "comments_close_on",
    "signing_date",
    "agencies",
    "agency_names",
    "docket_ids",
    "regulation_id_numbers",
    "cfr_references",
    "html_url",
    "pdf_url",
    "body_html_url",
    "volume",
    "start_page",
    "end_page",
    "executive_order_number",
)

# Transport hygiene: bound every request and retry transient failures with
# backoff so a flaky window fails slow-then-recovers rather than dropping docs.
_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 5
_PROGRESS_EVERY = 20_000


class FederalRegisterReader(Reader):
    """Yields raw FR document dicts published in ``[since, until]`` (inclusive).

    ``since`` / ``until`` default to the full FR archive and today. Callers doing
    incremental runs pass ``since`` = (max published date already stored) so only
    new documents are fetched; the transform handles merging with the prior table.
    """

    def __init__(
        self,
        *,
        since: date | None = None,
        until: date | None = None,
        per_page: int = PER_PAGE,
        verbose: bool = False,
    ) -> None:
        self.since = since or FR_EPOCH
        self.until = until or date.today()
        self.per_page = per_page
        self.verbose = verbose
        self._client: httpx.Client | None = None
        self._seen = 0

    def iter_records(self) -> Iterator[dict]:
        if self.since > self.until:
            logger.info("FR: since {} is after until {} — nothing to fetch", self.since, self.until)
            return
        logger.info("FR: fetching documents published {} .. {}", self.since, self.until)
        with httpx.Client(timeout=_TIMEOUT, headers={"Accept": "application/json"}) as client:
            self._client = client
            window_start = self.since
            while window_start <= self.until:
                window_end = min(
                    window_start + timedelta(days=MAX_WINDOW_DAYS - 1),
                    self.until,
                )
                yield from self._fetch_window(window_start, window_end)
                window_start = window_end + timedelta(days=1)
        logger.info("FR: yielded {:,} documents", self._seen)

    # -- window fetching -----------------------------------------------------

    def _fetch_window(self, gte: date, lte: date) -> Iterator[dict]:
        """Fetch one publication-date window, subdividing on truncation."""
        collected, count = self._page_window(gte, lte)
        truncated = len(collected) < count or count >= RESULT_CAP
        if truncated and gte < lte:
            # The API truncated this window; halve it and recurse so no document
            # in the range is lost.
            mid = gte + (lte - gte) // 2
            if self.verbose:
                logger.debug(
                    "FR: window {}..{} truncated ({}/{}), splitting at {}", gte, lte, len(collected), count, mid
                )
            yield from self._fetch_window(gte, mid)
            yield from self._fetch_window(mid + timedelta(days=1), lte)
            return
        if truncated:
            # A single day cannot be subdivided further. Publishing it would
            # silently turn a transport/API limit into corpus data loss.
            raise RuntimeError(f"Federal Register API truncated {gte} at {len(collected)}/{count} documents")
        for doc in collected:
            self._seen += 1
            if self._seen % _PROGRESS_EVERY == 0:
                logger.info("FR: {:,} documents so far...", self._seen)
            yield doc

    def _page_window(self, gte: date, lte: date) -> tuple[list[dict], int]:
        """Page through a window, following ``next_page_url``. Returns (docs, count)."""
        params: dict[str, object] = {
            "per_page": self.per_page,
            "order": "oldest",
            "conditions[publication_date][gte]": gte.isoformat(),
            "conditions[publication_date][lte]": lte.isoformat(),
        }
        params["fields[]"] = list(FIELDS)  # httpx repeats list values as fields[]=a&fields[]=b
        url = f"{API_BASE}/documents.json"
        docs: list[dict] = []
        count = 0
        first = True
        while url:
            payload = self._get(url, params if first else None)
            first = False
            count = payload.get("count", count)
            docs.extend(payload.get("results", []))
            url = payload.get("next_page_url") or ""
        return docs, count

    def _get(self, url: str, params: dict | None) -> dict:
        """GET with bounded retries; exhaustions abort the source snapshot."""
        assert self._client is not None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = self._client.get(url, params=params)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                if attempt == _MAX_RETRIES:
                    raise RuntimeError(f"Federal Register request failed after {attempt} attempts: {url}") from exc
                backoff = min(2**attempt, 30)
                logger.warning("FR: {} (attempt {}/{}), retrying in {}s", exc, attempt, _MAX_RETRIES, backoff)
                time.sleep(backoff)
        raise AssertionError("unreachable")
