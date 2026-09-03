"""Reader connector for the federalregister.gov public REST API (v1).

Brings Federal Register ingestion *in-repo*. A separate ingestion path used to
produce ``federal_register.parquet``, which this repo only consumed (see
``transforms/build_fr_docket_links``); this reader produces it here.

The reader is a *pure source*: it yields raw FR document payloads (dicts) exactly
as the API returns them. Shaping them into the published 22-column schema is the
job of :func:`~spicy_regs.transforms.build_federal_register.build_federal_register`.

The 10,000-result cap
---------------------
The documents endpoint clamps both pagination and its reported ``count`` at
10,000, so an overfull window looks exactly like a complete one. Ask it for the
whole archive and it answers ``count: 10000`` and hands back 10,000 rows; the
archive really holds 1,007,747 documents. Every single year since 1994 reports
10,000 as well. A loop that pages "until the rows run out" therefore publishes
about 1% of the corpus and calls the run a success.

The reader instead fetches bounded **date windows** and calls a window ambiguous
when the rows fall short of the reported ``count`` or that count reaches the cap.
It halves an ambiguous window and refetches both halves, down to a single day. A
single day that still reads ambiguous aborts the run rather than publish a hole.

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

# Largest page the API accepts. It saves round trips only: an ambiguous window is
# subdivided at any page size, so this is not a correctness knob.
PER_PAGE = 1000

# The API clamps both pagination and its reported ``count`` at 10,000, so a window
# holding a million documents reports the same count as one holding exactly
# 10,000. Any window reporting this count must be subdivided, even after all
# 10,000 visible rows arrive.
RESULT_CAP = 10_000

# A cost bound, not a correctness knob: ``_fetch_window`` bisects whatever span it
# is handed. Give it 1994..today in one piece and it pays a full capped traversal
# (10,000 rows, 10 pages) at each of the ~127 nodes above the split threshold —
# ~1,270 requests whose rows all get thrown away. Sized off the live archive,
# whose busiest 90-day window holds 9,122 documents, 91% of the cap and a thin
# margin; a quarter that does exceed the cap still gets split by ``_fetch_window``.
MAX_WINDOW_DAYS = 90

# The oldest documents the API serves. A backfill with no prior table starts here.
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

# Transport hygiene: bound every request, and retry transient failures with
# backoff so a flaky window recovers slowly instead of dropping documents.
_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 5

# Emit a progress line every this many documents yielded.
_PROGRESS_EVERY = 20_000


class FederalRegisterReader(Reader):
    """Yields raw FR document dicts published in ``[since, until]`` (inclusive).

    ``since`` defaults to the start of the archive and ``until`` to today. An
    incremental run passes ``since`` = the last publication date already stored,
    less a re-check overlap, so it fetches only new documents; the transform
    merges what comes back with the prior table.
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
        """Walk ``[since, until]`` in ``MAX_WINDOW_DAYS`` chunks, subdividing each as needed."""
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
        """Yield one publication-date window, halving it while its contents stay ambiguous."""
        collected, count = self._page_window(gte, lte)
        truncated = len(collected) < count or count >= RESULT_CAP
        if truncated and gte < lte:
            # These rows may be a truncated view of the window, so throw them
            # away and ask for each half instead. Nothing in the range is lost.
            mid = gte + (lte - gte) // 2
            if self.verbose:
                logger.debug(
                    "FR: window {}..{} truncated ({}/{}), splitting at {}", gte, lte, len(collected), count, mid
                )
            yield from self._fetch_window(gte, mid)
            yield from self._fetch_window(mid + timedelta(days=1), lte)
            return
        if truncated:
            # A single day cannot be split further, so nothing here can reveal
            # what the cap hides. Publishing it would turn an API limit into a
            # silent hole in the corpus.
            raise RuntimeError(f"Federal Register API truncated {gte} at {len(collected)}/{count} documents")
        for doc in collected:
            self._seen += 1
            if self._seen % _PROGRESS_EVERY == 0:
                logger.info("FR: {:,} documents so far...", self._seen)
            yield doc

    def _page_window(self, gte: date, lte: date) -> tuple[list[dict], int]:
        """Follow ``next_page_url`` through one window; return its rows and the reported ``count``."""
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
        """GET with bounded retries. Exhausting the budget raises, aborting the snapshot."""
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
        # Unreachable while ``_MAX_RETRIES >= 1``; a non-positive budget makes
        # ``range(1, 1)`` empty and drops through to here. Raise rather than fall
        # off the end: ``ty`` rejects the implicit ``None`` against the ``-> dict``
        # return, and the tempting way to silence that — returning ``{}`` — is
        # worse, because ``_page_window`` would read it as an empty final page and
        # publish the window as complete.
        raise RuntimeError(f"Federal Register retry budget _MAX_RETRIES={_MAX_RETRIES} makes no request: {url}")
