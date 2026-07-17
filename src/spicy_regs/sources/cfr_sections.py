"""Reader connector for GovInfo CFR (Code of Federal Regulations) section metadata.

Brings CFR *structure* ingestion in-repo. The CFR is the codified, subject-organized
body of federal regulations; this reader yields one raw record per CFR *granule*
(a structural unit — section, appendix, node, TOC, … — within a title's annual
edition) so downstream consumers can join regulations.gov activity and Federal
Register citations to the codified rules they touch.

The reader is a *pure source*: it yields raw GovInfo granule payloads (dicts)
roughly as the API returns them, plus the enclosing package's ``_package_id`` /
``_package_last_modified`` / ``_package_title`` (stamped onto each granule).
Shaping them into the published all-VARCHAR schema is the job of
:func:`~spicy_regs.transforms.build_cfr_sections.build_cfr_sections`.

Scope — SECTION METADATA + CITATIONS ONLY, *not* full section text.
    We collect each granule's identity and citation (title / part / section /
    heading / CFR reference / structure level / edition + last-modified stamps +
    canonical URL). The full regulatory *text* of each section is far heavier
    (hundreds of MB per title-year of XML/HTML) and is deliberately **out of
    scope** for this pass. A future enrichment pass could hydrate section bodies
    the way ``enrich_pdf_text`` does for attachments.

API key.
    GovInfo's ``api.govinfo.gov`` requires an api.data.gov key. We resolve it
    from the environment with a fallback chain (see :func:`_resolve_api_key`):
    ``DATA_GOV_API_KEY`` → ``GOVINFO_API_KEY`` → ``REGULATIONS_GOV_API_KEY``
    (the same api.data.gov key powers all three). With **no** key configured the
    reader logs a clear warning and yields nothing, so a keyless CI run is a
    no-op rather than a crash.

Traversal (live-validated).
    Package listing uses the ``/published/{START}/{END}?collection=CFR`` endpoint
    (``/collections/CFR`` alone 500s — do not use it). START/END are dates in
    ``yyyy-MM-dd`` form; the API rejects the ``…THH:mm:ssZ`` datetime form. Each
    package carries ``packageId`` / ``lastModified`` / ``title``. Granules come
    from ``/packages/{packageId}/granules``. Both endpoints page via
    ``offset`` + ``pageSize`` and signal more with ``nextPage``.

    Everything the published schema needs is derived from **list-level** fields
    plus the ID grammar — we deliberately do **not** call the per-granule
    ``/summary`` endpoint (the only place ``cfrTitle`` / ``cfrPart`` / ``heading``
    live), because that is an N+1 fetch across thousands of granules per package.
    Granule IDs look like ``CFR-2024-title48-vol5-chap7-appA`` (edition year, CFR
    title number, and — for many granules — ``part`` / ``sec`` tokens); the
    transform's ``_shape`` parses them.

    Reference: https://api.govinfo.gov/docs/ and the keyless bulkdata mirror at
    https://www.govinfo.gov/bulkdata/CFR (an alternative that needs no key but
    exposes the data as per-title-year XML rather than a granule API).
"""

from __future__ import annotations

import datetime as dt
import os
import time
from collections.abc import Iterator

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

# Primary (keyed) GovInfo API. Traversal: /published (packages) -> /packages/{id}/granules.
API_BASE = "https://api.govinfo.gov"

# Keyless alternative (per-title-year bulk XML). Documented for operators; this
# reader targets the keyed API above. See module docstring.
BULKDATA_BASE = "https://www.govinfo.gov/bulkdata/CFR"

# The CFR collection code in the GovInfo published/collections API.
COLLECTION = "CFR"

# Max records the published/granules endpoints accept per page.
PAGE_SIZE = 100

# api.data.gov env var fallback chain — the same key powers all three services.
API_KEY_ENV_VARS = ("DATA_GOV_API_KEY", "GOVINFO_API_KEY", "REGULATIONS_GOV_API_KEY")

# Transport hygiene: bound every request and retry transient failures with
# backoff so a flaky page fails slow-then-recovers rather than dropping granules.
_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 5
_PROGRESS_EVERY = 5_000


def _resolve_api_key() -> str | None:
    """Resolve the api.data.gov key from the environment fallback chain.

    Returns the first non-empty value among ``DATA_GOV_API_KEY``,
    ``GOVINFO_API_KEY``, ``REGULATIONS_GOV_API_KEY``, or ``None`` if none is set.
    """
    for name in API_KEY_ENV_VARS:
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    return None


class CfrSectionsReader(Reader):
    """Yields raw GovInfo CFR granule dicts for editions in ``[since_year, until_year]``.

    Each yielded dict carries the granule's own list-level fields plus the
    enclosing package's ``_package_id`` / ``_package_last_modified`` /
    ``_package_title``. The transform maps these onto the published all-VARCHAR
    schema, deriving the CFR title/part/section from the granule ID grammar.

    CFR editions are annual, so the default window is *last year through this
    year* (``since_year = current_year - 1``, ``until_year = current_year``); a
    full backfill passes an early ``since_year``.

    With no api.data.gov key resolvable from the environment the reader logs a
    warning and yields nothing — a keyless run (e.g. CI) is a no-op, not a crash.
    """

    def __init__(
        self,
        *,
        since_year: int | None = None,
        until_year: int | None = None,
        api_key: str | None = None,
        page_size: int = PAGE_SIZE,
        verbose: bool = False,
    ) -> None:
        current_year = dt.date.today().year
        self.until_year = until_year if until_year is not None else current_year
        self.since_year = since_year if since_year is not None else current_year - 1
        self.api_key = api_key if api_key is not None else _resolve_api_key()
        self.page_size = page_size
        self.verbose = verbose
        self._client: httpx.Client | None = None
        self._seen = 0

    def iter_records(self) -> Iterator[dict]:
        if not self.api_key:
            logger.warning(
                "CFR: no api.data.gov key found in {} — skipping ingest (set DATA_GOV_API_KEY). "
                "Keyless runs are a no-op, not an error.",
                ", ".join(API_KEY_ENV_VARS),
            )
            return
        logger.info("CFR: fetching {} granules (editions {}..{})", COLLECTION, self.since_year, self.until_year)
        with httpx.Client(timeout=_TIMEOUT, headers={"Accept": "application/json"}) as client:
            self._client = client
            for package in self._iter_packages():
                yield from self._iter_granules(package)
        logger.info("CFR: yielded {:,} granules", self._seen)

    # -- traversal (live-validated; see module docstring) ----------------------

    def _iter_packages(self) -> Iterator[dict]:
        """Page the ``/published/{START}/{END}?collection=CFR`` endpoint.

        Yields one small dict per CFR package with ``packageId``, ``lastModified``
        and ``title`` so the transform can populate ``last_modified`` without an
        extra per-package call. START/END are ``yyyy-MM-dd`` (the API rejects the
        ``…THH:mm:ssZ`` datetime form).
        """
        start = f"{self.since_year}-01-01"
        end = f"{self.until_year}-12-31"
        url = f"{API_BASE}/published/{start}/{end}"
        offset = 0
        while True:
            payload = self._get(url, {"collection": COLLECTION, "offset": offset, "pageSize": self.page_size})
            if payload is None:
                return
            packages = payload.get("packages") or []
            for pkg in packages:
                if not isinstance(pkg, dict):
                    continue
                package_id = pkg.get("packageId")
                if package_id:
                    yield {
                        "packageId": package_id,
                        "lastModified": pkg.get("lastModified"),
                        "title": pkg.get("title"),
                    }
            if not payload.get("nextPage") or not packages:
                return
            offset += self.page_size

    def _iter_granules(self, package: dict) -> Iterator[dict]:
        """Page one package's granules, yielding raw granule dicts.

        Each granule is stamped with ``_package_id`` / ``_package_last_modified``
        / ``_package_title`` from the enclosing package so the transform never
        needs a second lookup.
        """
        package_id = package["packageId"]
        offset = 0
        while True:
            payload = self._get(
                f"{API_BASE}/packages/{package_id}/granules",
                {"offset": offset, "pageSize": self.page_size},
            )
            if payload is None:
                return
            granules = payload.get("granules") or []
            for granule in granules:
                if not isinstance(granule, dict):
                    continue
                granule = {
                    **granule,
                    "_package_id": package_id,
                    "_package_last_modified": package.get("lastModified"),
                    "_package_title": package.get("title"),
                }
                self._seen += 1
                if self._seen % _PROGRESS_EVERY == 0:
                    logger.info("CFR: {:,} granules so far...", self._seen)
                yield granule
            if not payload.get("nextPage") or not granules:
                return
            offset += self.page_size

    def _get(self, url: str, params: dict) -> dict | None:
        """GET with the api_key attached, bounded retries + exponential backoff."""
        assert self._client is not None
        query = {**params, "api_key": self.api_key}
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = self._client.get(url, params=query)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                if attempt == _MAX_RETRIES:
                    logger.error("CFR: giving up on {} after {} attempts: {}", url, attempt, exc)
                    return None
                backoff = min(2**attempt, 30)
                logger.warning("CFR: {} (attempt {}/{}), retrying in {}s", exc, attempt, _MAX_RETRIES, backoff)
                time.sleep(backoff)
        return None
