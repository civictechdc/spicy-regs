"""Reader connector for the USASpending.gov recipient endpoint (API v2).

Brings federal-award **recipient** ingestion *in-repo* as an external
**reference dimension** complementary to the regulations.gov corpus: the
organizations that receive federal money — keyed by their Unique Entity
Identifier (UEI) and name — a clean org-resolution dimension that complements
the SAM entity registry and the FEC committee/PAC table for resolving and
enriching the organizations that comment on rulemakings.

The reader is a *pure source*: it yields raw recipient payloads (dicts) exactly
as the ``/api/v2/recipient/`` list endpoint returns them. Shaping them into the
pinned 6-column schema is the job of
:func:`~spicy_regs.transforms.build_usaspending_recipients.build_usaspending_recipients`.

**Keyless.** api.usaspending.gov requires no API key. Unlike the Congress.gov /
OpenFEC / GovInfo readers (which resolve an api.data.gov key), this reader always
fetches — there is nothing to gate on.

**POST, not GET.** The recipient endpoint is a POST that takes a JSON body
(``limit``/``page``/``sort``/``order``/``award_type``) and returns a
``results`` array plus a ``page_metadata`` envelope carrying ``hasNext``.

**Bounded scope.** The endpoint reports ~18M recipients across all history — far
too many for a daily full walk. We deliberately bound to the **top-N recipients
by all-time federal award amount** (``sort=amount``, ``order=desc``): the largest
federally funded organizations are both the most resolution-useful (big orgs that
show up in rulemaking comments) and a naturally bounded, tractable daily fetch
(``page_size`` 1000 × ``max_pages`` 100 ≈ 100K recipients in ~100 requests). The
transform merges each run's top-N with the prior published table, so coverage is
monotonic even as the ranking drifts.
"""

from __future__ import annotations

import time
from collections.abc import Iterator

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

API_BASE = "https://api.usaspending.gov/api/v2"

# Max rows the recipient endpoint returns per page.
PER_PAGE = 1000

# Default page cap → top ~100,000 recipients by all-time award amount. Bounds the
# daily fetch to ~100 requests; override via ``max_pages`` for validation/tests.
DEFAULT_MAX_PAGES = 100

# Transport hygiene: bound every request and retry transient failures with
# backoff so a flaky page fails slow-then-recovers rather than dropping rows.
_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 5
_PROGRESS_EVERY = 5_000

# Hard safety valve: never page past this, whatever ``max_pages`` is set to, so a
# misconfiguration can't turn into a runaway walk of the full 18M-row universe.
_MAX_PAGES_HARD = 5_000


class UsaSpendingRecipientsReader(Reader):
    """Yields raw USASpending recipient dicts, highest all-time award $ first.

    Recipients are a reference dimension, not a time series, so there is no
    ``since`` watermark: each run walks the top-N pages (by award amount) and the
    transform merges the result with the prior published table (dedup on
    ``recipient_id``). The endpoint is keyless, so the reader always fetches.
    """

    def __init__(
        self,
        *,
        per_page: int = PER_PAGE,
        max_pages: int = DEFAULT_MAX_PAGES,
        award_type: str = "all",
        verbose: bool = False,
    ) -> None:
        self.per_page = min(per_page, PER_PAGE)
        self.max_pages = min(max_pages, _MAX_PAGES_HARD)
        self.award_type = award_type
        self.verbose = verbose
        self._client: httpx.Client | None = None
        self._seen = 0

    def iter_records(self) -> Iterator[dict]:
        logger.info(
            "USASpending recipients: fetching top {:,} recipients by all-time award amount",
            self.per_page * self.max_pages,
        )
        with httpx.Client(timeout=_TIMEOUT, headers={"Accept": "application/json"}) as client:
            self._client = client
            yield from self._paginate()
        logger.info("USASpending recipients: yielded {:,} recipients", self._seen)

    # -- pagination ----------------------------------------------------------

    def _paginate(self) -> Iterator[dict]:
        """Walk ``page``/``limit`` pages (sorted by amount desc) up to ``max_pages``."""
        page = 1
        while page <= self.max_pages:
            payload = self._get_page(page)
            if payload is None:
                break
            results = payload.get("results") or []
            if not results:
                break
            for recipient in results:
                self._seen += 1
                if self._seen % _PROGRESS_EVERY == 0:
                    logger.info("USASpending recipients: {:,} recipients so far...", self._seen)
                yield recipient
            # Stop when the envelope says there's nothing more, or on a short page.
            if not (payload.get("page_metadata") or {}).get("hasNext"):
                break
            if len(results) < self.per_page:
                break
            page += 1

    def _get_page(self, page: int) -> dict | None:
        body: dict[str, object] = {
            "limit": self.per_page,
            "page": page,
            "order": "desc",
            "sort": "amount",
            "award_type": self.award_type,
        }
        return self._post(f"{API_BASE}/recipient/", body)

    def _post(self, url: str, body: dict) -> dict | None:
        """POST with bounded retries + exponential backoff. Returns parsed JSON or None."""
        assert self._client is not None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = self._client.post(url, json=body)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                if attempt == _MAX_RETRIES:
                    logger.error("USASpending recipients: giving up on {} after {} attempts: {}", url, attempt, exc)
                    return None
                backoff = min(2**attempt, 30)
                logger.warning(
                    "USASpending recipients: {} (attempt {}/{}), retrying in {}s",
                    exc,
                    attempt,
                    _MAX_RETRIES,
                    backoff,
                )
                time.sleep(backoff)
        return None
