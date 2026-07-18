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

**Pagination.** We walk with a stable sort key (``sort=committee_id``) and prefer
OpenFEC's documented **keyset (seek)** pagination: when the response envelope
carries a non-null ``pagination.last_indexes`` cursor we pass those keys straight
back as query params on the next request instead of incrementing ``page``. Not
every OpenFEC endpoint offers seek pagination — ``/committees`` in particular
returns ``last_indexes: null`` and is walked by incrementing ``page`` (the sort
key keeps that walk stable and duplicate-free). Either way the walk terminates
only when a page comes back **empty** (guarded by ``_MAX_PAGES``); it never stops
early on a short page. A short page mid-walk was the old bug: OpenFEC's deep
offset pages occasionally return fewer than ``per_page`` rows even when more
exist, and the previous ``len(results) < per_page`` early-break truncated the
backfill at ~26K of ~89K committees.

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

# Safety valve: stop after this many page fetches to avoid a runaway loop if the
# API ever fails to return an empty terminal page. ~89K committees / 100 per page
# is well under 1,000 pages, so this only guards against pathology.
_MAX_PAGES = 5_000

# The stable, unique sort key we page against. Sorting by the primary key keeps
# both the offset walk and the keyset (seek) walk deterministic and dup-free.
_SORT_KEY = "committee_id"


def _clean_cursor(last_indexes: object) -> dict[str, object]:
    """Return a usable keyset cursor from ``pagination.last_indexes``, or ``{}``.

    OpenFEC returns ``last_indexes`` as an object like
    ``{"last_index": "...", "last_committee_id": "..."}`` on endpoints that
    support seek pagination, and ``null`` on those that don't (e.g.
    ``/committees``). Drop null-valued keys; an all-null / non-dict cursor means
    "no seek pagination — walk by offset page instead".
    """
    if not isinstance(last_indexes, dict):
        return {}
    return {str(k): v for k, v in last_indexes.items() if v is not None}


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
        """Walk every page, preferring keyset (seek) cursors, else offset pages.

        Terminates only on an empty ``results`` page (or the ``_MAX_PAGES`` /
        ``max_pages`` guard). A short page is *not* a stop signal — that was the
        bug that truncated the backfill at ~26K of ~89K committees.
        """
        page = 1
        # Keyset cursor carried forward from ``pagination.last_indexes`` when the
        # endpoint offers seek pagination; empty means "walk by offset page".
        keyset: dict[str, object] = {}
        # Cap the number of *fetches*, honoring a caller-supplied ``max_pages``.
        max_fetches = _MAX_PAGES if self.max_pages is None else min(self.max_pages, _MAX_PAGES)
        for _ in range(max_fetches):
            payload = self._get_page(page, keyset)
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
            # Prefer the documented keyset cursor when the envelope provides one;
            # otherwise fall back to incrementing the offset page (what
            # ``/committees`` uses — it returns ``last_indexes: null``).
            last_indexes = (payload.get("pagination") or {}).get("last_indexes")
            cursor = _clean_cursor(last_indexes)
            if cursor:
                keyset = cursor
            else:
                keyset = {}
                page += 1

    def _get_page(self, page: int, keyset: dict[str, object]) -> dict | None:
        params: dict[str, object] = {
            "per_page": self.per_page,
            "sort": _SORT_KEY,
            "api_key": self.api_key,
        }
        if keyset:
            # Seek pagination: the ``last_indexes`` keys (e.g. ``last_index``,
            # ``last_committee_id``) are the exact query-param names OpenFEC wants.
            params.update(keyset)
        else:
            params["page"] = page
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
