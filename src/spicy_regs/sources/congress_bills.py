"""Reader connector for the Congress.gov public REST API (v3).

Brings congressional bill ingestion *in-repo* as an external source complementary
to the regulations.gov ``dockets``/``documents`` view — the legislative record
that sits upstream of the rulemakings this dataset tracks.

The reader is a *pure source*: it yields raw bill payloads (dicts) exactly as the
list endpoint returns them. Shaping them into the published 11-column schema is
the job of
:func:`~spicy_regs.transforms.build_congress_bills.build_congress_bills`.

The ``/bill`` list endpoint is paginated by ``offset``/``limit`` (``limit`` caps
at 250) and sorted ``updateDate+desc`` so the newest activity comes first; an
incremental run stops as soon as it walks past its ``since`` watermark.

**API key.** Congress.gov requires an api.data.gov key sent as the ``api_key``
query param. The same key works across regulations.gov, Congress.gov, and
GovInfo, so we resolve it from a fallback chain of the env vars this repo already
uses (:func:`_resolve_api_key`). If no key is set the reader logs a clear warning
and yields nothing — a keyless CI run is a no-op, not a crash.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from datetime import date

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

API_BASE = "https://api.congress.gov/v3"

# Max the API accepts per page.
PER_PAGE = 250

# Env vars checked in order for the api.data.gov key (one key works across
# regulations.gov, Congress.gov, and GovInfo).
API_KEY_ENV_VARS = (
    "DATA_GOV_API_KEY",
    "CONGRESS_GOV_API_KEY",
    "REGULATIONS_GOV_API_KEY",
)

# Transport hygiene: bound every request and retry transient failures with
# backoff so a flaky page fails slow-then-recovers rather than dropping bills.
_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 5
_PROGRESS_EVERY = 5_000

# The list endpoint refuses very large offsets; stop paging past this to avoid a
# runaway loop on an unexpectedly large window. Incremental runs stop far sooner.
_MAX_OFFSET = 200_000


def _resolve_api_key() -> str | None:
    """Return the first api.data.gov key set in :data:`API_KEY_ENV_VARS`, or None.

    The same api.data.gov key is valid across regulations.gov, Congress.gov, and
    GovInfo, so we accept whichever the environment already provides.
    """
    for var in API_KEY_ENV_VARS:
        value = os.environ.get(var)
        if value:
            return value
    return None


class CongressBillsReader(Reader):
    """Yields raw Congress.gov bill dicts, newest ``updateDate`` first.

    ``since`` lets incremental runs stop once they page past the newest
    ``updateDate`` already stored; the transform handles merging with the prior
    table. With no key configured the reader yields nothing.
    """

    def __init__(
        self,
        *,
        since: date | None = None,
        per_page: int = PER_PAGE,
        api_key: str | None = None,
        verbose: bool = False,
    ) -> None:
        self.since = since
        self.per_page = min(per_page, PER_PAGE)
        self.api_key = api_key or _resolve_api_key()
        self.verbose = verbose
        self._client: httpx.Client | None = None
        self._seen = 0

    def iter_records(self) -> Iterator[dict]:
        if not self.api_key:
            logger.warning(
                "Congress bills: no API key found (set one of {}) — yielding nothing",
                ", ".join(API_KEY_ENV_VARS),
            )
            return
        logger.info(
            "Congress bills: fetching bills updated since {}",
            self.since or "the beginning",
        )
        with httpx.Client(timeout=_TIMEOUT, headers={"Accept": "application/json"}) as client:
            self._client = client
            yield from self._paginate()
        logger.info("Congress bills: yielded {:,} bills", self._seen)

    # -- pagination ----------------------------------------------------------

    def _paginate(self) -> Iterator[dict]:
        """Walk ``offset``/``limit`` pages, stopping at the ``since`` watermark."""
        offset = 0
        while offset < _MAX_OFFSET:
            payload = self._get_page(offset)
            if payload is None:
                break
            bills = payload.get("bills") or []
            if not bills:
                break
            for bill in bills:
                # Bills come newest-updated first; once we cross the watermark
                # everything after is older, so we can stop early.
                if self.since is not None and _older_than(bill, self.since):
                    if self.verbose:
                        logger.debug("Congress bills: reached watermark {} — stopping", self.since)
                    return
                self._seen += 1
                if self._seen % _PROGRESS_EVERY == 0:
                    logger.info("Congress bills: {:,} bills so far...", self._seen)
                yield bill
            # A short final page means the server has nothing more to give.
            if len(bills) < self.per_page:
                break
            offset += self.per_page

    def _get_page(self, offset: int) -> dict | None:
        params: dict[str, object] = {
            "offset": offset,
            "limit": self.per_page,
            "sort": "updateDate+desc",
            "format": "json",
            "api_key": self.api_key,
        }
        return self._get(f"{API_BASE}/bill", params)

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
                    logger.error("Congress bills: giving up on {} after {} attempts: {}", url, attempt, exc)
                    return None
                backoff = min(2**attempt, 30)
                logger.warning(
                    "Congress bills: {} (attempt {}/{}), retrying in {}s", exc, attempt, _MAX_RETRIES, backoff
                )
                time.sleep(backoff)
        return None


def _older_than(bill: dict, since: date) -> bool:
    """True if the bill's ``updateDate`` is strictly before ``since``."""
    raw = bill.get("updateDate")
    if not raw:
        return False
    try:
        return date.fromisoformat(str(raw)[:10]) < since
    except ValueError:
        return False
