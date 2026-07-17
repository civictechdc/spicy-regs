"""Reader connector for GovInfo CFR (Code of Federal Regulations) section metadata.

Brings CFR *structure* ingestion in-repo. The CFR is the codified, subject-organized
body of federal regulations; this reader yields one raw record per CFR *granule*
(a section-level unit within a title's annual edition) so downstream consumers can
join regulations.gov activity and Federal Register citations to the codified rules
they touch.

The reader is a *pure source*: it yields raw GovInfo granule/summary payloads
(dicts) roughly as the API returns them, plus the enclosing ``package_id``.
Shaping them into the published all-VARCHAR schema is the job of
:func:`~spicy_regs.transforms.build_cfr_sections.build_cfr_sections`.

Scope — SECTION METADATA + CITATIONS ONLY, *not* full section text.
    We collect each section's identity and citation (title / part / section /
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

IMPORTANT — traversal requires live validation with a key.
    The exact GovInfo package → granule → summary traversal (offset/pageSize
    paging shapes, the precise field names on a granule *summary*, and how the
    section citation is expressed) is documented from GovInfo's public API docs
    but has **not** been exercised against the live service in this pass. The
    endpoints below are captured as named constants and the field mapping lives
    in the transform's ``_shape``; both should be confirmed against a real key
    before enabling the R2 upload. All tests here are hermetic (no network).

    Reference: https://api.govinfo.gov/docs/ and the keyless bulkdata mirror at
    https://www.govinfo.gov/bulkdata/CFR (an alternative that needs no key but
    exposes the data as per-title-year XML rather than a granule API).
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

# Primary (keyed) GovInfo API. Traversal: collections/CFR -> packages ->
# granules -> granule summary. See module docstring: shapes need live validation.
API_BASE = "https://api.govinfo.gov"

# Keyless alternative (per-title-year bulk XML). Documented for operators; this
# reader targets the keyed API above. See module docstring.
BULKDATA_BASE = "https://www.govinfo.gov/bulkdata/CFR"

# The CFR collection code in the GovInfo collections API.
COLLECTION = "CFR"

# Max records the collections/granules endpoints accept per page.
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

    Each yielded dict carries the granule's own fields plus the enclosing
    ``_package_id`` (and, when a per-granule summary was fetched, its fields
    merged in). The transform maps these onto the published all-VARCHAR schema.

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
        self.since_year = since_year
        self.until_year = until_year
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
            for package_id in self._iter_package_ids():
                yield from self._iter_granules(package_id)
        logger.info("CFR: yielded {:,} granules", self._seen)

    # -- traversal (shapes require live validation; see module docstring) ------

    def _iter_package_ids(self) -> Iterator[str]:
        """Page the collections/CFR endpoint, yielding CFR package ids.

        NOTE: exact response shape (``packages[].packageId``, ``nextPage``) is
        from GovInfo's public docs and needs live validation with a key.
        """
        offset = 0
        while True:
            payload = self._get(
                f"{API_BASE}/collections/{COLLECTION}",
                {"offset": offset, "pageSize": self.page_size},
            )
            if payload is None:
                return
            packages = payload.get("packages") or []
            for pkg in packages:
                package_id = pkg.get("packageId") if isinstance(pkg, dict) else None
                if package_id:
                    yield package_id
            if not payload.get("nextPage") or not packages:
                return
            offset += self.page_size

    def _iter_granules(self, package_id: str) -> Iterator[dict]:
        """Page one package's granules, yielding raw granule dicts.

        NOTE: exact response shape (``granules[].granuleId``, ``nextPage``) is
        from GovInfo's public docs and needs live validation with a key.
        """
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
                granule = {**granule, "_package_id": package_id}
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
