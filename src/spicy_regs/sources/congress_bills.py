"""Reader connector for the Congress.gov public REST API (v3).

Brings congressional bill ingestion *in-repo* as an external source complementary
to the regulations.gov ``dockets``/``documents`` view — the legislative record
that sits upstream of the rulemakings this dataset tracks.

The reader is a *pure source*: it yields raw bill payloads (dicts) exactly as the
list endpoint returns them. Shaping them into the published 11-column schema is
the job of
:func:`~spicy_regs.transforms.build_congress_bills.build_congress_bills`.

The ``/bill`` list endpoint is paginated by ``offset``/``limit`` (``limit`` caps
at 250). An incremental run bounds its window *server-side* with ``fromDateTime``
and pages until the window is exhausted, newest ``updateDate`` first.

**Do not bound the window client-side by stopping at the first out-of-window
row.** That is what this reader used to do, and it froze the published table for
510 days: the ``sort`` parameter was being sent as ``updateDate%2Bdesc``, which
the API ignores while still answering ``200``, so rows arrived in arbitrary
order. The second row of the first page was older than the watermark, the walk
stopped there, and every run "succeeded" having yielded exactly one bill. The
window is now the server's job; ordering is only a paging-stability concern.

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

# Written with a SPACE, never a literal "+". httpx form-encodes a space to "+"
# (``sort=updateDate+desc`` — what the API wants) but percent-encodes a literal
# "+" to "%2B", which the API accepts with a 200 and silently ignores. See the
# module docstring for what that cost.
SORT_NEWEST_FIRST = "updateDate desc"

# Server-side window bound. The API wants a full RFC3339 instant.
_FROM_DATETIME_FMT = "%Y-%m-%dT00:00:00Z"

# Warn when a walk returns materially less than the server said it would; the
# slack absorbs bills whose updateDate shifts mid-walk.
_COMPLETENESS_TOLERANCE = 0.9

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

# Backstop against a runaway loop, not an expected limit: deep offsets page fine
# (verified past 238k). Must clear a full-archive backfill — ~430k bills as of
# 2026-08 — or a first run would silently truncate. Hitting it is logged as an
# error, never a quiet stop.
_MAX_OFFSET = 500_000


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

    ``since`` becomes the ``fromDateTime`` bound on the request, so the server
    decides what is in the window and the walk simply runs to exhaustion; the
    transform handles merging with the prior table. With no key configured the
    reader yields nothing.
    """

    def __init__(
        self,
        *,
        since: date | None = None,
        per_page: int = PER_PAGE,
        api_key: str | None = None,
    ) -> None:
        self.since = since
        self.per_page = min(per_page, PER_PAGE)
        self.api_key = api_key or _resolve_api_key()
        self._client: httpx.Client | None = None
        self._seen = 0
        self._expected: int | None = None

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
        # A walk that returns far less than the server advertised is the shape
        # of the 510-day freeze. Say so loudly rather than reporting success.
        if self._expected is not None and self._seen < self._expected * _COMPLETENESS_TOLERANCE:
            logger.warning(
                "Congress bills: yielded {:,} of the {:,} bills the server reported "
                "for this window — the walk ended early",
                self._seen,
                self._expected,
            )

    # -- pagination ----------------------------------------------------------

    def _paginate(self) -> Iterator[dict]:
        """Walk ``offset``/``limit`` pages until the server-bounded window runs out."""
        offset = 0
        while offset < _MAX_OFFSET:
            payload = self._get_page(offset)
            if payload is None:
                break
            if self._expected is None:
                self._expected = _pagination_count(payload)
                if self._expected is not None:
                    logger.info(
                        "Congress bills: server reports {:,} bills in this window",
                        self._expected,
                    )
            bills = payload.get("bills") or []
            # An empty page ends the walk; `fromDateTime` already excluded
            # everything outside the window, so there is no watermark to check.
            if not bills:
                break
            for bill in bills:
                self._seen += 1
                if self._seen % _PROGRESS_EVERY == 0:
                    logger.info("Congress bills: {:,} bills so far...", self._seen)
                yield bill
            # A short final page means the server has nothing more to give.
            if len(bills) < self.per_page:
                break
            offset += self.per_page
        else:
            # while/else: reached only when the condition goes false, i.e. the
            # offset backstop — every ordinary exit above is a `break`.
            logger.error(
                "Congress bills: hit the {:,}-row offset backstop with pages still "
                "coming — the window is too wide for one run; narrow it with --since",
                _MAX_OFFSET,
            )

    def _get_page(self, offset: int) -> dict | None:
        params: dict[str, object] = {
            "offset": offset,
            "limit": self.per_page,
            "sort": SORT_NEWEST_FIRST,
            "format": "json",
            "api_key": self.api_key,
        }
        # Bound the window server-side. Without this the walk would have to
        # trust the row order to know when to stop — the exact assumption that
        # failed silently for 510 days.
        if self.since is not None:
            params["fromDateTime"] = self.since.strftime(_FROM_DATETIME_FMT)
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


def _pagination_count(payload: dict) -> int | None:
    """Total rows the server says match the window, or None if absent/unparseable."""
    raw = (payload.get("pagination") or {}).get("count")
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        try:
            return int(raw)
        except ValueError:
            return None
    return None
