"""Reader connectors for the FCC ECFS public REST API.

The FCC does not participate in regulations.gov — its rulemaking dockets
("proceedings") and public comments ("filings") live in ECFS, the Electronic
Comment Filing System, exposed at ``https://publicapi.fcc.gov/ecfs``. These
readers bring that universe into the pipeline alongside the regulations.gov
mirror.

Two record streams, mirroring the regulations.gov docket/comment split:

* :class:`FccEcfsProceedingsReader` — one record per proceeding (the FCC's
  docket equivalent, e.g. ``17-108``).
* :class:`FccEcfsFilingsReader` — one record per filing (comments, replies,
  ex-parte notices, ...), optionally scoped to specific proceedings.

Both are *pure sources*: they yield raw API payload dicts. Shaping them into
the published schemas is the job of
:mod:`spicy_regs.transforms.build_fcc_ecfs`.

**Pagination.** ECFS is Elasticsearch-backed and rejects ``offset + limit``
beyond 10,000 results per query ("Parameters incorrectly formatted"). Range
filters (``date_received=[gte]YYYY-MM-DD[lte]YYYY-MM-DD``) accept only
date-granular values — full timestamps 400. So, like the Federal Register
reader, we fetch **date windows** and split any window that hits the result
ceiling in half, down to a single day. A single day that *still* exceeds the
ceiling is fetched from both ends (sorted ascending then descending); if the
two halves overlap the day is fully covered, otherwise the gap is logged
loudly rather than dropped silently.

**API key.** ECFS is served through api.data.gov and requires a key sent as
the ``api_key`` query parameter. The key is resolved from the environment
with the same fallback chain the other api.data.gov sources use
(:func:`_resolve_api_key`). If no key is set the readers log a clear warning
and yield nothing, so a pipeline run without the secret degrades to a no-op
instead of crashing.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from datetime import date, timedelta

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

API_BASE = "https://publicapi.fcc.gov/ecfs"

# Env vars consulted for the api.data.gov key, in order.
API_KEY_ENV_VARS = ("DATA_GOV_API_KEY", "FCC_API_KEY", "REGULATIONS_GOV_API_KEY")

# Page size. The API accepts more, but 250 keeps individual responses small
# enough that a retry after a mid-transfer failure is cheap.
PER_PAGE = 250

# Elasticsearch result window: the API rejects any page where
# offset + limit > 10,000, so this bounds how much of one query is reachable.
MAX_RESULT_WINDOW = 10_000

# ECFS's oldest proceedings date from the early 1990s. A full backfill with no
# explicit ``since`` walks windows from here.
ECFS_EPOCH = date(1990, 1, 1)

# Transport hygiene: bound every request and retry transient failures with
# backoff so a flaky window fails slow-then-recovers rather than dropping rows.
_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 5
_PROGRESS_EVERY = 10_000


def _resolve_api_key() -> str | None:
    """Return the first non-empty api.data.gov key in :data:`API_KEY_ENV_VARS`, or None."""
    for name in API_KEY_ENV_VARS:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return None


def _range_param(gte: date, lte: date) -> str:
    """Render ECFS's bracketed date-range filter value, e.g. ``[gte]2024-01-01[lte]2024-01-31``."""
    return f"[gte]{gte.isoformat()}[lte]{lte.isoformat()}"


class _EcfsReader(Reader):
    """Shared plumbing: keyed client, retrying GET, window walk with subdivision.

    Subclasses set the endpoint, the record key in the response payload, the
    date field used for windowing/sorting, and any extra query params.
    """

    endpoint: str
    record_key: str
    date_field: str

    def __init__(
        self,
        *,
        since: date | None = None,
        until: date | None = None,
        api_key: str | None = None,
        per_page: int = PER_PAGE,
    ) -> None:
        self.since = since or ECFS_EPOCH
        self.until = until or date.today()
        self.api_key = api_key if api_key is not None else _resolve_api_key()
        self.per_page = per_page
        self._client: httpx.Client | None = None
        self._seen = 0

    # -- subclass hooks --------------------------------------------------

    def _extra_params(self) -> dict[str, str]:
        """Additional query params applied to every request (e.g. a proceeding filter)."""
        return {}

    # -- Reader ----------------------------------------------------------

    def iter_records(self) -> Iterator[dict]:
        if not self.api_key:
            logger.warning(
                "ECFS: no api.data.gov key found in {} — skipping ingest (set DATA_GOV_API_KEY). "
                "Free keys: https://api.data.gov/signup/",
                ", ".join(API_KEY_ENV_VARS),
            )
            return
        if self.since > self.until:
            logger.info("ECFS: since {} is after until {} — nothing to fetch", self.since, self.until)
            return
        logger.info(
            "ECFS {}: fetching records with {} in {} .. {}",
            self.endpoint,
            self.date_field,
            self.since,
            self.until,
        )
        with httpx.Client(timeout=_TIMEOUT, headers={"Accept": "application/json"}) as client:
            self._client = client
            yield from self._fetch_window(self.since, self.until)
        logger.info("ECFS {}: yielded {:,} records", self.endpoint, self._seen)

    # -- window fetching ---------------------------------------------------

    def _fetch_window(self, gte: date, lte: date) -> Iterator[dict]:
        """Fetch one date window, subdividing whenever it hits the result ceiling."""
        records, exhausted = self._page_window(gte, lte, ascending=True)
        if not exhausted and gte < lte:
            # The window has more rows than one query can reach; halve and recurse
            # so nothing past the ceiling is lost.
            mid = gte + (lte - gte) // 2
            logger.debug("ECFS {}: window {}..{} hit result ceiling, splitting at {}", self.endpoint, gte, lte, mid)
            yield from self._fetch_window(gte, mid)
            yield from self._fetch_window(mid + timedelta(days=1), lte)
            return
        if not exhausted:
            # A single day exceeds the ceiling. Fetch the same day descending too:
            # if the two 10K slices overlap, the union covers the whole day.
            records = self._merge_day_ends(gte, records)
        for rec in records:
            self._seen += 1
            if self._seen % _PROGRESS_EVERY == 0:
                logger.info("ECFS {}: {:,} records so far...", self.endpoint, self._seen)
            yield rec

    def _merge_day_ends(self, day: date, head: list[dict]) -> list[dict]:
        """Union the ascending slice of an overloaded day with its descending slice."""
        tail, _ = self._page_window(day, day, ascending=False)
        head_ids = {self._record_id(r) for r in head}
        overlap = sum(1 for r in tail if self._record_id(r) in head_ids)
        if overlap == 0:
            logger.warning(
                "ECFS {}: single day {} exceeds 2x the {} result window — "
                "records in the middle of the day are unreachable and were skipped",
                self.endpoint,
                day,
                MAX_RESULT_WINDOW,
            )
        merged = list(head)
        merged.extend(r for r in tail if self._record_id(r) not in head_ids)
        return merged

    def _record_id(self, record: dict) -> str:
        """Identity used to dedup the two slices of an overloaded day."""
        return str(record.get("id_submission") or record.get("id_proceeding") or id(record))

    def _page_window(self, gte: date, lte: date, *, ascending: bool) -> tuple[list[dict], bool]:
        """Page one window with offsets. Returns (records, exhausted).

        ``exhausted`` is False when paging stopped at the result ceiling while
        pages were still coming back full — i.e. the window was truncated.
        """
        direction = "ASC" if ascending else "DESC"
        records: list[dict] = []
        offset = 0
        while True:
            params: dict[str, str] = {
                self.date_field: _range_param(gte, lte),
                "sort": f"{self.date_field},{direction}",
                "limit": str(self.per_page),
                "offset": str(offset),
                **self._extra_params(),
            }
            payload = self._get(params)
            if payload is None:
                # Retries exhausted mid-window; treat as exhausted so the caller
                # doesn't subdivide forever — the gap has already been logged.
                return records, True
            page = payload.get(self.record_key) or []
            records.extend(page)
            if len(page) < self.per_page:
                return records, True
            offset += self.per_page
            if offset + self.per_page > MAX_RESULT_WINDOW:
                return records, False

    def _get(self, params: dict[str, str]) -> dict | None:
        """GET with the api_key attached, bounded retries + exponential backoff."""
        assert self._client is not None
        url = f"{API_BASE}/{self.endpoint}"
        query = {**params, "api_key": self.api_key}
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = self._client.get(url, params=query)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                resp.raise_for_status()
                # Bad-parameter errors come back as HTTP 200 with a plain-text
                # body; json() raising ValueError routes them into the retry
                # loop, which fails loudly after _MAX_RETRIES.
                return resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                if attempt == _MAX_RETRIES:
                    logger.error("ECFS: giving up on {} {} after {} attempts: {}", url, params, attempt, exc)
                    return None
                backoff = min(2**attempt, 30)
                logger.warning("ECFS: {} (attempt {}/{}), retrying in {}s", exc, attempt, _MAX_RETRIES, backoff)
                time.sleep(backoff)
        return None


class FccEcfsProceedingsReader(_EcfsReader):
    """Yields raw ECFS proceeding dicts created in ``[since, until]`` (inclusive).

    Proceedings are the FCC's docket equivalent (~20K records total), windowed
    on ``date_proceeding_created``. Proceedings with a NULL created date are
    not reachable through a date-range query and are therefore not returned;
    they are legacy shells with no filing activity.
    """

    endpoint = "proceedings"
    record_key = "proceeding"
    date_field = "date_proceeding_created"


class FccEcfsFilingsReader(_EcfsReader):
    """Yields raw ECFS filing dicts received in ``[since, until]`` (inclusive).

    ``proceedings`` optionally scopes the fetch to specific proceeding names
    (e.g. ``("17-108",)``); one full window walk runs per proceeding so each
    stays under the result ceiling independently. Unscoped, one walk covers
    all of ECFS — fine for incremental windows, prohibitive for a full
    backfill (ECFS holds tens of millions of filings), which is why the
    transform bounds the first run instead of walking from the epoch.
    """

    endpoint = "filings"
    record_key = "filing"
    date_field = "date_received"

    def __init__(
        self,
        *,
        since: date | None = None,
        until: date | None = None,
        api_key: str | None = None,
        per_page: int = PER_PAGE,
        proceedings: tuple[str, ...] = (),
    ) -> None:
        super().__init__(since=since, until=until, api_key=api_key, per_page=per_page)
        self.proceedings = proceedings
        self._current_proceeding: str | None = None

    def _extra_params(self) -> dict[str, str]:
        if self._current_proceeding:
            return {"proceedings.name": self._current_proceeding}
        return {}

    def iter_records(self) -> Iterator[dict]:
        if not self.proceedings:
            yield from super().iter_records()
            return
        for name in self.proceedings:
            self._current_proceeding = name
            logger.info("ECFS filings: proceeding {}", name)
            yield from super().iter_records()
        self._current_proceeding = None
