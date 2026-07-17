"""Reader connector for the OpenFEC ``/committees`` endpoint (v1).

Brings Federal Election Commission committee/PAC ingestion *in-repo* as an
external **reference dimension** complementary to the regulations.gov corpus:
the political committees (PACs, party committees, campaign committees) whose
filings, endorsements, and money flows sit alongside the organizations that
comment on rulemakings. Downstream this feeds the dashboard's ally/opposition
(stance) map — a name to resolve commenters and co-filers against.

The reader is a *pure source*: it yields raw committee payloads (dicts) exactly
as the ``/committees`` list endpoint returns them. Shaping them into the pinned
16-column schema is the job of
:func:`~spicy_regs.transforms.build_fec_committees.build_fec_committees`.

**Scope.** Committees are a *reference dimension* (~89K rows), not itemized
contributions. Itemized ``/schedules/schedule_a`` receipts run into the hundreds
of millions of rows and are deliberately out of scope for this pass; a future
bounded-by-organization contributions pass could follow, keyed on the
``committee_id`` this table establishes.

The endpoint is paginated by ``page``/``per_page`` (``per_page`` caps at 100);
the response envelope carries ``pagination.page`` / ``pagination.pages`` so the
reader walks from page 1 to the reported last page.

**API key.** OpenFEC is fronted by api.data.gov, so the same api.data.gov key
this repo already uses for regulations.gov / Congress.gov / GovInfo works here,
sent as the ``api_key`` query param. We resolve it from a fallback chain of the
env vars this repo already provides (:func:`_resolve_api_key`). If no key is set
the reader logs a clear warning and yields nothing — a keyless CI run is a no-op,
not a crash.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

API_BASE = "https://api.open.fec.gov/v1"

# Max the API accepts per page.
PER_PAGE = 100

# Env vars checked in order for the api.data.gov key (one key works across
# regulations.gov, Congress.gov, GovInfo, and OpenFEC).
API_KEY_ENV_VARS = (
    "DATA_GOV_API_KEY",
    "FEC_API_KEY",
    "REGULATIONS_GOV_API_KEY",
)

# Transport hygiene: bound every request and retry transient failures with
# backoff so a flaky page fails slow-then-recovers rather than dropping rows.
_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 5
_PROGRESS_EVERY = 5_000

# Safety valve: stop paging past this many pages to avoid a runaway loop if the
# API ever reports a nonsensical page count. ~89K committees / 100 per page is
# well under 1,000 pages, so this only guards against pathology.
_MAX_PAGES = 5_000


def _resolve_api_key() -> str | None:
    """Return the first api.data.gov key set in :data:`API_KEY_ENV_VARS`, or None.

    OpenFEC is fronted by api.data.gov, so the same key that works for
    regulations.gov, Congress.gov, and GovInfo is valid here — we accept
    whichever the environment already provides.
    """
    for var in API_KEY_ENV_VARS:
        value = os.environ.get(var)
        if value:
            return value
    return None


class FecCommitteesReader(Reader):
    """Yields raw OpenFEC committee dicts by walking every page of ``/committees``.

    Committees are a reference dimension, not a time series, so there is no
    ``since`` watermark: each run walks the full committee list and the transform
    merges it with the prior published table (dedup on ``committee_id``). With no
    key configured the reader yields nothing.
    """

    def __init__(
        self,
        *,
        per_page: int = PER_PAGE,
        max_pages: int | None = None,
        api_key: str | None = None,
        verbose: bool = False,
    ) -> None:
        self.per_page = min(per_page, PER_PAGE)
        # ``max_pages`` lets bounded validation / tests fetch just a page or two;
        # None means "walk to the reported last page" (capped by _MAX_PAGES).
        self.max_pages = max_pages
        self.api_key = api_key or _resolve_api_key()
        self.verbose = verbose
        self._client: httpx.Client | None = None
        self._seen = 0

    def iter_records(self) -> Iterator[dict]:
        if not self.api_key:
            logger.warning(
                "FEC committees: no API key found (set one of {}) — yielding nothing",
                ", ".join(API_KEY_ENV_VARS),
            )
            return
        logger.info("FEC committees: fetching committee reference dimension")
        with httpx.Client(timeout=_TIMEOUT, headers={"Accept": "application/json"}) as client:
            self._client = client
            yield from self._paginate()
        logger.info("FEC committees: yielded {:,} committees", self._seen)

    # -- pagination ----------------------------------------------------------

    def _paginate(self) -> Iterator[dict]:
        """Walk ``page``/``per_page`` pages, following ``pagination.pages``."""
        page = 1
        last_page = self.max_pages if self.max_pages is not None else _MAX_PAGES
        while page <= last_page and page <= _MAX_PAGES:
            payload = self._get_page(page)
            if payload is None:
                break
            results = payload.get("results") or []
            if not results:
                break
            for committee in results:
                self._seen += 1
                if self._seen % _PROGRESS_EVERY == 0:
                    logger.info("FEC committees: {:,} committees so far...", self._seen)
                yield committee
            # Trust the envelope's reported total page count when the caller
            # hasn't capped it, so we stop exactly at the last real page.
            if self.max_pages is None:
                reported = (payload.get("pagination") or {}).get("pages")
                if isinstance(reported, int) and reported > 0:
                    last_page = min(reported, _MAX_PAGES)
            # A short page also means the server has nothing more to give.
            if len(results) < self.per_page:
                break
            page += 1

    def _get_page(self, page: int) -> dict | None:
        params: dict[str, object] = {
            "page": page,
            "per_page": self.per_page,
            "sort": "committee_id",
            "api_key": self.api_key,
        }
        return self._get(f"{API_BASE}/committees/", params)

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
                    logger.error("FEC committees: giving up on {} after {} attempts: {}", url, attempt, exc)
                    return None
                backoff = min(2**attempt, 30)
                logger.warning(
                    "FEC committees: {} (attempt {}/{}), retrying in {}s", exc, attempt, _MAX_RETRIES, backoff
                )
                time.sleep(backoff)
        return None
