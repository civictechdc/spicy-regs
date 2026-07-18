"""Reader connector for the SAM.gov Entity Management API (v4).

Brings the federal *entity registry* in-repo as an external source: the
authoritative directory of organizations registered to do business with (or
receive assistance from) the U.S. government, keyed by the Unique Entity ID
(``uei``). This anchors entity resolution across the rest of the corpus — the
same UEI the dashboard uses to tie a commenting organization to its registered
identity.

The reader is a *pure source*: it yields raw entity payloads (dicts) exactly as
the list endpoint returns them under ``entityData[]``. Shaping them into the
published 18-column schema is the job of
:func:`~spicy_regs.transforms.build_sam_entities.build_sam_entities`.

The ``/entities`` list endpoint is paginated by a 0-based ``page`` and a ``size``
(the API caps a synchronous page at 10 records — ``size`` values above 10 are
rejected with HTTP 400). Each response carries a ``links.nextLink`` pointing at
the next page; the reader follows that link until it is absent (or a page comes
back empty), so a full run can walk the entire active registry. We filter to
public active registrations (``registrationStatus=A``) so the published table
stays to the public-display surface.

Note: SAM masks the ``api_key`` in the returned ``nextLink`` (it comes back as a
placeholder), so the reader re-injects the real key when following it.

**API key.** SAM.gov requires an api.data.gov key sent as the ``api_key`` query
param — but note the key must additionally be *associated with a SAM.gov account
that has the Entity API role*; a generic api.data.gov key that works against
regulations.gov / Congress.gov is **not** automatically authorized here, and
SAM's gateway returns a bare ``404`` (not ``401``/``403``) for an unauthorized or
invalid key. We resolve the key from a fallback chain of the env vars this repo
already uses (:func:`_resolve_api_key`). If no key is set the reader logs a clear
warning and yields nothing — a keyless CI run is a no-op, not a crash.

**Unbounded by default, bounded on request.** The reader walks every page by
default (a full extract is ~hundreds of thousands of entities, capped only by the
runaway page guard). Callers that only need a bounded slice — the transform's
scheduled window, validation probes, tests — pass ``max_records`` to stop early;
a full backfill is never run implicitly by the scheduled transform, which always
passes a bounded ``max_records``.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

API_BASE = "https://api.sam.gov/entity-information/v4"

# Max records the synchronous /entities page accepts per request.
PER_PAGE = 10

# Env vars checked in order for the api.data.gov key. SAM.gov needs a key that is
# specifically associated with a SAM.gov account holding the Entity API role, so
# the SAM-dedicated var is preferred first; a generic api.data.gov key (which
# works against regulations.gov / Congress.gov) is only a fallback and returns a
# bare 404 here if it isn't SAM-authorized.
API_KEY_ENV_VARS = (
    "SAM_API_KEY",
    "DATA_GOV_API_KEY",
    "REGULATIONS_GOV_API_KEY",
)

# Transport hygiene: bound every request and retry transient failures with
# backoff so a flaky page fails slow-then-recovers rather than dropping entities.
_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 5
_PROGRESS_EVERY = 5_000

# Safety cap on pages walked in a single run, independent of ``max_records`` — a
# backstop against a runaway loop on an unexpectedly large window.
_MAX_PAGES = 100_000


def _resolve_api_key() -> str | None:
    """Return the first api.data.gov key set in :data:`API_KEY_ENV_VARS`, or None."""
    for var in API_KEY_ENV_VARS:
        value = os.environ.get(var)
        if value:
            return value
    return None


class SamEntitiesReader(Reader):
    """Yields raw SAM.gov entity dicts (the ``entityData[]`` records), page by page.

    ``max_records`` bounds a run so a scheduled ingest never attempts a full
    backfill; ``registration_status`` filters the extract (``A`` = active). The
    transform handles merging with the prior published table. With no key
    configured the reader yields nothing.
    """

    def __init__(
        self,
        *,
        registration_status: str = "A",
        max_records: int | None = None,
        per_page: int = PER_PAGE,
        api_key: str | None = None,
        verbose: bool = False,
    ) -> None:
        self.registration_status = registration_status
        self.max_records = max_records
        self.per_page = min(per_page, PER_PAGE)
        self.api_key = api_key or _resolve_api_key()
        self.verbose = verbose
        self._client: httpx.Client | None = None
        self._seen = 0

    def iter_records(self) -> Iterator[dict]:
        if not self.api_key:
            logger.warning(
                "SAM entities: no API key found (set one of {}) — yielding nothing",
                ", ".join(API_KEY_ENV_VARS),
            )
            return
        logger.info(
            "SAM entities: fetching registrationStatus={} (max_records={})",
            self.registration_status,
            self.max_records if self.max_records is not None else "all",
        )
        with httpx.Client(timeout=_TIMEOUT, headers={"Accept": "application/json"}) as client:
            self._client = client
            yield from self._paginate()
        logger.info("SAM entities: yielded {:,} entities", self._seen)

    # -- pagination ----------------------------------------------------------

    def _paginate(self) -> Iterator[dict]:
        """Follow ``links.nextLink`` until exhausted, ``max_records``, or the cap.

        The first request is built from ``page=0``; every subsequent page is the
        server-provided ``nextLink`` (with the real api_key re-injected, since SAM
        masks it in the returned link). Walking stops when a page is empty, when
        there is no ``nextLink``, or at the ``_MAX_PAGES`` runaway backstop.
        """
        url = f"{API_BASE}/entities"
        params: dict[str, object] | None = {
            "api_key": self.api_key,
            "page": 0,
            "size": self.per_page,
        }
        if self.registration_status:
            params["registrationStatus"] = self.registration_status

        for _ in range(_MAX_PAGES):
            payload = self._get(url, params)
            if payload is None:
                break
            records = payload.get("entityData") or []
            if not records:
                break
            for record in records:
                if self.max_records is not None and self._seen >= self.max_records:
                    if self.verbose:
                        logger.debug("SAM entities: reached max_records={} — stopping", self.max_records)
                    return
                self._seen += 1
                if self._seen % _PROGRESS_EVERY == 0:
                    logger.info("SAM entities: {:,} entities so far...", self._seen)
                yield record
            # Prefer the server's own pagination link; fall back to page-count
            # exhaustion when a short page signals there is nothing more.
            next_link = (payload.get("links") or {}).get("nextLink")
            if not next_link or len(records) < self.per_page:
                break
            url = self._authorize_next_link(next_link)
            params = None  # nextLink already carries page/size/filter as a query string
        else:
            logger.warning("SAM entities: hit the _MAX_PAGES={} runaway guard — stopping", _MAX_PAGES)

    def _authorize_next_link(self, next_link: str) -> str:
        """Return ``next_link`` with the real api_key re-injected.

        SAM returns the ``nextLink`` with its ``api_key`` masked as a placeholder,
        so following it verbatim would 404. We overwrite that query param with the
        configured key and leave the rest of the link (page/size/filter) intact.
        """
        parts = urlparse(next_link)
        query = parse_qs(parts.query, keep_blank_values=True)
        query["api_key"] = [self.api_key or ""]
        return urlunparse(parts._replace(query=urlencode(query, doseq=True)))

    def _get(self, url: str, params: dict | None) -> dict | None:
        """GET with bounded retries + exponential backoff. Returns parsed JSON or None."""
        assert self._client is not None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = self._client.get(url, params=params)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                if resp.status_code == 404:
                    # SAM returns a bare 404 for an unauthorized/invalid api_key
                    # (rather than 401/403). Non-retryable: stop and no-op.
                    logger.error(
                        "SAM entities: 404 from {} — the api.data.gov key is likely not "
                        "authorized for the SAM.gov Entity API (associate it with a SAM.gov "
                        "account that has the Entity API role). Yielding nothing.",
                        url,
                    )
                    return None
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                if attempt == _MAX_RETRIES:
                    logger.error("SAM entities: giving up on {} after {} attempts: {}", url, attempt, exc)
                    return None
                backoff = min(2**attempt, 30)
                logger.warning("SAM entities: {} (attempt {}/{}), retrying in {}s", exc, attempt, _MAX_RETRIES, backoff)
                time.sleep(backoff)
        return None
