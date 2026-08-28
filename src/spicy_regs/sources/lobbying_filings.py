"""Reader connector for the U.S. Senate Lobbying Disclosure Act (LDA) REST API.

Brings federal lobbying-disclosure ingestion *in-repo* as an external source
complementary to the regulations.gov view. Every registrant that files a comment
campaign on a rulemaking often *also* files quarterly LDA reports naming the same
agencies and issues they lobby directly — so these filings let the corpus link
public comment activity to direct agency lobbying.

The reader is a *pure source*: it yields raw filing payloads (dicts) exactly as
the API returns them. Shaping them into the published schema is the job of
:func:`~spicy_regs.transforms.build_lobbying_filings.build_lobbying_filings`.

The ``/filings/`` list endpoint is paginated by ``page``/``page_size``
(``page_size`` caps at 25) and returns a DRF envelope
(``count``/``next``/``previous``/``results``). We follow the ``next`` URL until
it is null, which is robust to the exact page cap.

**API key.** The LDA API works **keyless** at a lower rate limit; an optional
``LDA_API_KEY`` sent as an ``Authorization: Token <key>`` header raises the
limit. This reader is fully functional with no key — it just fetches more
slowly. When a key is present it is used automatically.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from datetime import date

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

# The Senate-hosted API sunsets on 2026-07-31. lda.gov is its official,
# API-compatible successor and is already live.
API_BASE = "https://lda.gov/api/v1"

# Max the API accepts per page.
PER_PAGE = 25

# Env var holding the optional LDA API key (Authorization: Token <key>).
API_KEY_ENV_VAR = "LDA_API_KEY"

# Transport hygiene: bound every request and retry transient failures with
# backoff so a flaky page fails slow-then-recovers rather than dropping filings.
# Keyless requests are rate-limited more aggressively, so 429s are expected and
# retried with backoff.
_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 6
_PROGRESS_EVERY = 2_500


def _resolve_api_key() -> str | None:
    """Return the ``LDA_API_KEY`` if set, else None (keyless is still functional)."""
    value = os.environ.get(API_KEY_ENV_VAR)
    return value or None


class LobbyingFilingsReader(Reader):
    """Yields raw LDA filing dicts from the ``/filings/`` list endpoint.

    ``since``/``until`` scope the fetch by posting date (server-side via the
    API's before/after filters). Bounded windows let a stale incremental ingest
    catch up monotonically without exceeding its scheduled-run timeout.
    """

    def __init__(
        self,
        *,
        since: date | None = None,
        until: date | None = None,
        filing_year: int | None = None,
        max_records: int | None = None,
        page_size: int = PER_PAGE,
        api_key: str | None = None,
        verbose: bool = False,
    ) -> None:
        self.since = since
        self.until = until
        self.filing_year = filing_year
        self.max_records = max_records
        self.page_size = min(page_size, PER_PAGE)
        self.api_key = api_key or _resolve_api_key()
        self.verbose = verbose
        self._client: httpx.Client | None = None
        self._seen = 0

    def iter_records(self) -> Iterator[dict]:
        keyed = "with API key" if self.api_key else "keyless (lower rate limit)"
        logger.info(
            "LDA filings: fetching {} (since={}, until={}, filing_year={}, max_records={})",
            keyed,
            self.since or "the beginning",
            self.until or "today",
            self.filing_year or "any",
            self.max_records or "unbounded",
        )
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Token {self.api_key}"
        with httpx.Client(timeout=_TIMEOUT, headers=headers) as client:
            self._client = client
            yield from self._paginate()
        logger.info("LDA filings: yielded {:,} filings", self._seen)

    # -- pagination ----------------------------------------------------------

    def _paginate(self) -> Iterator[dict]:
        """Follow the DRF ``next`` URL from the first page until it is null."""
        params: dict[str, object] = {"page": 1, "page_size": self.page_size}
        if self.filing_year is not None:
            params["filing_year"] = self.filing_year
        if self.since is not None:
            params["filing_dt_posted_after"] = self.since.isoformat()
        if self.until is not None:
            params["filing_dt_posted_before"] = self.until.isoformat()
        # Oldest-first makes a partial/retried catch-up run move its watermark
        # forward instead of repeatedly spending its budget on the newest page.
        params["ordering"] = "dt_posted"
        url = f"{API_BASE}/filings/"
        first = True
        while url:
            payload = self._get(url, params if first else None)
            first = False
            if payload is None:
                break
            for filing in payload.get("results") or []:
                self._seen += 1
                if self._seen % _PROGRESS_EVERY == 0:
                    logger.info("LDA filings: {:,} filings so far...", self._seen)
                yield filing
                if self.max_records is not None and self._seen >= self.max_records:
                    if self.verbose:
                        logger.debug("LDA filings: reached max_records {} — stopping", self.max_records)
                    return
            url = payload.get("next") or ""

    def _get(self, url: str, params: dict | None) -> dict | None:
        """GET with bounded retries + exponential backoff. Returns parsed JSON or None."""
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
                    logger.error("LDA filings: giving up on {} after {} attempts: {}", url, attempt, exc)
                    return None
                backoff = min(2**attempt, 60)
                logger.warning("LDA filings: {} (attempt {}/{}), retrying in {}s", exc, attempt, _MAX_RETRIES, backoff)
                time.sleep(backoff)
        return None
