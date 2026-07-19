"""Reader connector for the SAM.gov Entity Management API (v4).

Brings the federal *entity registry* in-repo as an external source: the
authoritative directory of organizations registered to do business with (or
receive assistance from) the U.S. government, keyed by the Unique Entity ID
(``uei``). This anchors entity resolution across the rest of the corpus — the
same UEI the dashboard uses to tie a commenting organization to its registered
identity.

The reader is a *pure source*: it yields raw entity payloads (dicts) exactly as
the API returns them under ``entityData[]``. Shaping them into the published
18-column schema is the job of
:func:`~spicy_regs.transforms.build_sam_entities.build_sam_entities`.

Full coverage — the pagination ceiling problem
-----------------------------------------------
The active registry is ~765,000 entities, but the *synchronous* ``/entities``
list endpoint cannot walk it: ``size`` hard-caps at 10 records/page and a single
filtered query only paginates to ~5,000 records (``links.nextLink`` stops there),
regardless of ``totalRecords``. Naively following ``nextLink`` therefore tops out
around 5K — a tiny slice of the registry. To get (near-)full coverage this reader
supports two mechanisms:

**A. Bulk extract (default, ``mode="extract"``).** The same ``/entities`` endpoint,
called with ``format=json``, switches to SAM's *asynchronous extract* path: instead
of an inline page it returns a download URL (with the literal ``REPLACE_WITH_API_KEY``
placeholder to swap for a real key), and that file can hold up to 1,000,000 records —
far past the 5K synchronous ceiling. One extract request thus returns an entire
partition of the registry. To keep any single downloaded file bounded in memory,
the reader loops the extract over ``registrationDate`` *year windows* by default
(each year is well under the 1M cap and json-loadable); each window is one extract
request, and windows are disjoint by registration date so no dedup is needed within
a run. A single unwindowed extract is also supported (``year_windows=False``).

**B. Partitioned walk (``mode="partition"``, fallback).** When the extract path is
unavailable, the reader partitions the query space by ``registrationDate`` window and
walks each window with the paginated endpoint, **adaptively subdividing** any window
whose ``totalRecords`` exceeds the ~5K reachable ceiling (halving down to a single
day, like ``federal_register``'s date-window recursion). Because each sub-window
stays under the ceiling, every matching entity in it is reachable via ``nextLink``.
Windows are disjoint by registration date, so the union is the full registry.

Both mechanisms accept ``max_records`` to bound a run, and an explicit
``[since_year, until_year]`` range so scheduled runs can advance coverage window by
window; the transform's merge accretes coverage across runs (dedup on ``uei``).

Note: SAM masks the ``api_key`` in returned links (``nextLink`` for pages, the
``REPLACE_WITH_API_KEY`` placeholder for extracts), so the reader re-injects the
real key when following them.

**API key.** SAM.gov requires an api.data.gov key sent as the ``api_key`` query
param — but note the key must additionally be *associated with a SAM.gov account
that has the Entity API role* (a role-holding non-federal key is rate-limited to
1,000 requests/day; the bulk-extract path is what keeps a full backfill within that
budget). A generic api.data.gov key that works against regulations.gov / Congress.gov
is **not** automatically authorized here, and SAM's gateway returns a bare ``404``
(not ``401``/``403``) for an unauthorized or invalid key. We resolve the key from a
fallback chain of the env vars this repo already uses (:func:`_resolve_api_key`). If
no key is set the reader logs a clear warning and yields nothing — a keyless CI run
is a no-op, not a crash.

**Bounded by default, full on request.** Callers that only need a bounded slice — the
transform's scheduled window, validation probes, tests — pass ``max_records`` to stop
early. A full backfill is never run implicitly by the scheduled transform, which
always passes a bounded ``max_records``.
"""

from __future__ import annotations

import gzip
import io
import json
import os
import time
import zipfile
from collections.abc import Iterator
from datetime import date
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

API_BASE = "https://api.sam.gov/entity-information/v4"

# Max records the synchronous /entities page accepts per request.
PER_PAGE = 10

# The synchronous list endpoint stops paginating around this many records for a
# single filtered query, no matter how large ``totalRecords`` is. The partitioned
# walk subdivides any window whose ``totalRecords`` exceeds this so every matching
# entity stays reachable via ``nextLink``.
PAGE_CEILING = 5_000

# Earliest plausible registrationDate year to window over for a full extract. SAM
# registrations (carried over from the CCR/DUNS era) predate the UEI transition, so
# we start comfortably early; empty early windows are cheap (one extract each).
_MIN_REGISTRATION_YEAR = 2000

# The literal placeholder SAM embeds in the extract download URL in place of the key.
_API_KEY_PLACEHOLDER = "REPLACE_WITH_API_KEY"

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
_TIMEOUT = httpx.Timeout(120.0, connect=30.0)
_MAX_RETRIES = 5
_PROGRESS_EVERY = 5_000

# Safety cap on pages walked in a single run, independent of ``max_records`` — a
# backstop against a runaway loop on an unexpectedly large window.
_MAX_PAGES = 100_000

# Async extracts are generated server-side; the download URL may not be ready on
# the first GET. Poll it with backoff up to this many attempts before giving up.
_EXTRACT_POLL_MAX = 60
_EXTRACT_POLL_INTERVAL = 10.0


def _resolve_api_key() -> str | None:
    """Return the first api.data.gov key set in :data:`API_KEY_ENV_VARS`, or None."""
    for var in API_KEY_ENV_VARS:
        value = os.environ.get(var)
        if value:
            return value
    return None


class SamEntitiesReader(Reader):
    """Yields raw SAM.gov entity dicts (the ``entityData[]`` records).

    ``mode`` selects the ingest mechanism: ``"extract"`` (default) uses SAM's bulk
    async extract (``format=json``) — one request per ``registrationDate`` window,
    up to 1M records each, well past the 5K synchronous ceiling; ``"partition"``
    walks the paginated endpoint, adaptively subdividing windows so each stays under
    that ceiling. ``max_records`` bounds a run; ``since_year``/``until_year`` bound
    the window range so scheduled runs can advance coverage. With no key configured
    the reader yields nothing.
    """

    def __init__(
        self,
        *,
        mode: str = "extract",
        registration_status: str = "A",
        since_year: int | None = None,
        until_year: int | None = None,
        year_windows: bool = True,
        max_records: int | None = None,
        per_page: int = PER_PAGE,
        api_key: str | None = None,
        verbose: bool = False,
    ) -> None:
        if mode not in ("extract", "partition"):
            raise ValueError(f"mode must be 'extract' or 'partition', got {mode!r}")
        self.mode = mode
        self.registration_status = registration_status
        self.since_year = since_year
        self.until_year = until_year
        self.year_windows = year_windows
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
            "SAM entities: mode={} registrationStatus={} years={}..{} (max_records={})",
            self.mode,
            self.registration_status,
            self.since_year if self.since_year is not None else "min",
            self.until_year if self.until_year is not None else "now",
            self.max_records if self.max_records is not None else "all",
        )
        # follow_redirects: extract download URLs commonly 302 to a signed blob URL.
        with httpx.Client(timeout=_TIMEOUT, headers={"Accept": "application/json"}, follow_redirects=True) as client:
            self._client = client
            if self.mode == "extract":
                yield from self._iter_extract()
            else:
                yield from self._iter_partitioned()
        logger.info("SAM entities: yielded {:,} entities", self._seen)

    # -- shared helpers ------------------------------------------------------

    def _year_range(self) -> tuple[int, int]:
        """Inclusive [since, until] year range to window over."""
        since = self.since_year if self.since_year is not None else _MIN_REGISTRATION_YEAR
        until = self.until_year if self.until_year is not None else date.today().year
        return since, until

    def _budget_left(self) -> bool:
        return self.max_records is None or self._seen < self.max_records

    def _emit(self, record: dict) -> Iterator[dict]:
        """Yield one record honouring ``max_records`` and progress logging."""
        if not self._budget_left():
            return
        self._seen += 1
        if self._seen % _PROGRESS_EVERY == 0:
            logger.info("SAM entities: {:,} entities so far...", self._seen)
        yield record

    def _base_params(self) -> dict[str, object]:
        params: dict[str, object] = {"api_key": self.api_key}
        if self.registration_status:
            params["registrationStatus"] = self.registration_status
        return params

    # -- A. bulk extract -----------------------------------------------------

    def _iter_extract(self) -> Iterator[dict]:
        """Fetch the registry via SAM's async extract, one window per request.

        By default windows over ``registrationDate`` years so no single downloaded
        file is unbounded in memory; ``year_windows=False`` requests a single extract
        for the whole ``registrationStatus`` filter.
        """
        if not self.year_windows:
            yield from self._extract_window(None)
            return
        since, until = self._year_range()
        for year in range(since, until + 1):
            if not self._budget_left():
                return
            if self.verbose:
                logger.debug("SAM entities: extract window year={}", year)
            yield from self._extract_window(year)

    def _extract_window(self, year: int | None) -> Iterator[dict]:
        """Request + download one extract (optionally scoped to a registration year)."""
        params = self._base_params()
        params["format"] = "json"
        if year is not None:
            params["registrationDate"] = _year_range_literal(year)
        trigger = self._get(f"{API_BASE}/entities", params)
        if trigger is None:
            return
        download_url = _find_download_url(trigger)
        if download_url:
            records: Iterator[dict] = self._download_extract(download_url)
        else:
            # No extract URL: a small window may have come back inline instead. Fall
            # back to any ``entityData`` in the trigger; only warn if there's nothing.
            inline = trigger.get("entityData")
            if not inline:
                logger.warning(
                    "SAM entities: extract response for year={} had no download URL or inline data — skipping",
                    year,
                )
                return
            records = iter(r for r in inline if isinstance(r, dict))
        for record in records:
            if not self._budget_left():
                return
            yield from self._emit(record)

    def _download_extract(self, download_url: str) -> Iterator[dict]:
        """Download an extract file and yield its entity records (defensive parse)."""
        assert self._client is not None
        url = self._reinject_key(download_url)
        for attempt in range(1, _EXTRACT_POLL_MAX + 1):
            try:
                resp = self._client.get(url)
            except httpx.HTTPError as exc:
                logger.warning("SAM entities: extract download error {} (attempt {})", exc, attempt)
                time.sleep(_EXTRACT_POLL_INTERVAL)
                continue
            # The file may still be generating: SAM answers 202/404 until ready.
            if resp.status_code in (202, 404, 429) or resp.status_code >= 500:
                if attempt == _EXTRACT_POLL_MAX:
                    logger.error("SAM entities: extract not ready after {} polls — giving up", attempt)
                    return
                time.sleep(_EXTRACT_POLL_INTERVAL)
                continue
            resp.raise_for_status()
            yield from _parse_extract_bytes(resp.content)
            return

    # -- B. partitioned walk -------------------------------------------------

    def _iter_partitioned(self) -> Iterator[dict]:
        """Adaptive ``registrationDate`` window recursion over the paginated endpoint."""
        since, until = self._year_range()
        yield from self._fetch_window(date(since, 1, 1), date(until, 12, 31))

    def _fetch_window(self, gte: date, lte: date) -> Iterator[dict]:
        """Walk one registration-date window, subdividing when it exceeds the ceiling.

        We first read the window's ``totalRecords``; if it is above
        :data:`PAGE_CEILING` the paginated walk cannot reach every record, so we halve
        the window and recurse (down to a single day). Otherwise we page it fully.
        """
        if not self._budget_left():
            return
        total = self._window_total(gte, lte)
        if total is None:
            return
        if total > PAGE_CEILING and gte < lte:
            mid = gte + (lte - gte) // 2
            if self.verbose:
                logger.debug(
                    "SAM entities: window {}..{} has {} > {}, splitting at {}", gte, lte, total, PAGE_CEILING, mid
                )
            yield from self._fetch_window(gte, mid)
            yield from self._fetch_window(_next_day(mid), lte)
            return
        if total > PAGE_CEILING:
            # A single day still exceeds the ceiling — accept the reachable slice but
            # make the residual gap loud rather than silent.
            logger.warning(
                "SAM entities: single day {} has {} entities > {} ceiling — some unreachable", gte, total, PAGE_CEILING
            )
        yield from self._page_window(gte, lte)

    def _window_total(self, gte: date, lte: date) -> int | None:
        """Return ``totalRecords`` for a registration-date window (a cheap size=1 probe)."""
        params = self._base_params()
        params["registrationDate"] = _range_literal(gte, lte)
        params["page"] = 0
        params["size"] = 1
        payload = self._get(f"{API_BASE}/entities", params)
        if payload is None:
            return None
        return int(payload.get("totalRecords") or 0)

    def _page_window(self, gte: date, lte: date) -> Iterator[dict]:
        """Follow ``links.nextLink`` through one window until exhausted or the ceiling."""
        url = f"{API_BASE}/entities"
        params: dict[str, object] | None = self._base_params()
        assert params is not None
        params["registrationDate"] = _range_literal(gte, lte)
        params["page"] = 0
        params["size"] = self.per_page

        for _ in range(_MAX_PAGES):
            if not self._budget_left():
                return
            payload = self._get(url, params)
            if payload is None:
                return
            records = payload.get("entityData") or []
            if not records:
                return
            for record in records:
                if not self._budget_left():
                    return
                yield from self._emit(record)
            next_link = (payload.get("links") or {}).get("nextLink")
            if not next_link or len(records) < self.per_page:
                return
            url = self._reinject_key(next_link)
            params = None  # nextLink already carries page/size/filter as a query string
        else:
            logger.warning("SAM entities: hit the _MAX_PAGES={} runaway guard — stopping", _MAX_PAGES)

    # -- transport -----------------------------------------------------------

    def _reinject_key(self, link: str) -> str:
        """Return ``link`` with the real api_key re-injected.

        SAM returns links with their ``api_key`` masked — either as a query-param
        placeholder (``nextLink``) or as the literal ``REPLACE_WITH_API_KEY`` token
        (extract download URLs). We overwrite the query param and swap the token so
        the link is usable, leaving the rest (page/size/filter/fileName) intact.
        """
        link = link.replace(_API_KEY_PLACEHOLDER, self.api_key or "")
        parts = urlparse(link)
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


# -- module-level pure helpers (unit-testable without a client) ---------------


def _us_date(d: date) -> str:
    """Format a date as SAM's MM/DD/YYYY."""
    return f"{d.month:02d}/{d.day:02d}/{d.year}"


def _range_literal(gte: date, lte: date) -> str:
    """SAM ``registrationDate`` range literal ``[MM/DD/YYYY,MM/DD/YYYY]``."""
    return f"[{_us_date(gte)},{_us_date(lte)}]"


def _year_range_literal(year: int) -> str:
    """SAM ``registrationDate`` range literal spanning a whole calendar year."""
    return _range_literal(date(year, 1, 1), date(year, 12, 31))


def _next_day(d: date) -> date:
    return date.fromordinal(d.toordinal() + 1)


def _find_download_url(payload: object) -> str | None:
    """Recursively locate an extract download URL in a trigger response.

    SAM returns the async-extract download link embedded in the JSON envelope,
    carrying the literal ``REPLACE_WITH_API_KEY`` placeholder. The exact key path
    has varied across API versions, so we walk the structure and return the first
    string that looks like that download URL (placeholder preferred; otherwise any
    http(s) URL that mentions download/extract).
    """
    fallback: str | None = None

    def walk(node: object) -> str | None:
        nonlocal fallback
        if isinstance(node, str):
            if _API_KEY_PLACEHOLDER in node and node.startswith("http"):
                return node
            if (
                fallback is None
                and node.startswith("http")
                and ("download" in node.lower() or "extract" in node.lower())
            ):
                fallback = node
            return None
        if isinstance(node, dict):
            for value in node.values():
                hit = walk(value)
                if hit:
                    return hit
        elif isinstance(node, list):
            for value in node:
                hit = walk(value)
                if hit:
                    return hit
        return None

    return walk(payload) or fallback


def _parse_extract_bytes(raw: bytes) -> Iterator[dict]:
    """Yield entity dicts from a downloaded extract file (defensive across formats).

    Handles gzip- and zip-compressed payloads, then parses the inner text as either
    a JSON envelope (``{"entityData": [...]}``), a bare JSON array of entities, or
    newline-delimited JSON. Anything unrecognisable yields nothing.
    """
    text = _decompress_extract(raw)
    if not text:
        return
    stripped = text.lstrip()
    if stripped[:1] in ("{", "["):
        try:
            doc = json.loads(stripped)
        except ValueError:
            yield from _iter_ndjson(text)
            return
        if isinstance(doc, dict):
            records = doc.get("entityData")
            if isinstance(records, list):
                yield from (r for r in records if isinstance(r, dict))
            elif _looks_like_entity(doc):
                yield doc
            return
        if isinstance(doc, list):
            yield from (r for r in doc if isinstance(r, dict))
            return
    yield from _iter_ndjson(text)


def _decompress_extract(raw: bytes) -> str:
    """Return the extract's inner text, transparently decompressing gzip/zip."""
    if raw[:2] == b"\x1f\x8b":  # gzip magic
        try:
            return gzip.decompress(raw).decode("utf-8", "replace")
        except OSError:
            return ""
    if raw[:2] == b"PK":  # zip magic
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                names = zf.namelist()
                if not names:
                    return ""
                return zf.read(names[0]).decode("utf-8", "replace")
        except zipfile.BadZipFile:
            return ""
    return raw.decode("utf-8", "replace")


def _iter_ndjson(text: str) -> Iterator[dict]:
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        if isinstance(obj, dict):
            yield obj


def _looks_like_entity(doc: dict) -> bool:
    return "entityRegistration" in doc or "ueiSAM" in doc
