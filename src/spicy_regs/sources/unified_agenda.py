"""Reader connector for the OIRA/OMB Unified Agenda published at reginfo.gov.

The Unified Agenda of Regulatory and Deregulatory Actions is the semiannual
catalog, edited by the Office of Information and Regulatory Affairs (OIRA), of
the rulemakings each federal agency has under active development. Every entry is
keyed by a **Regulation Identifier Number (RIN)** — the same RIN that appears in
the Federal Register (``regulation_id_numbers``) — which makes the Unified Agenda
the upstream, forward-looking view of the rulemaking lifecycle: it lists actions
that are *planned* long before any proposed or final rule reaches the Federal
Register or opens a comment period on regulations.gov.

The reader is a *pure source*: it yields raw Unified Agenda entry payloads (dicts)
exactly as reginfo.gov returns them. Shaping them into the published 17-column
schema is the job of
:func:`~spicy_regs.transforms.build_unified_agenda.build_unified_agenda`.

reginfo.gov exposes the Unified Agenda through a public XML/JSON export keyed by
"agenda edition" (e.g. ``202404`` for the Spring 2024 edition). No API key is
required; reginfo.gov is fully open.

.. important::
   **The exact reginfo.gov endpoint/params require live validation against
   reginfo.gov with a real run.** The base URL and request parameters below are
   encoded as clearly-named module constants precisely so a human can confirm
   and adjust them once against the live service. The publicly documented export
   lives under ``https://www.reginfo.gov/public/do/eAgendaXmlReport`` (the
   "Agenda XML Report"); the pagination shape reginfo.gov uses for large editions
   is likewise unverified here. Every network path is exercised only through
   ``httpx`` with the constants below, and the tests are hermetic (no network),
   so validating later is a matter of confirming these constants and the
   response-envelope keys (:data:`_RESULTS_KEY`, :data:`_COUNT_KEY`,
   :data:`_NEXT_KEY`) against a real response.
"""

from __future__ import annotations

import time
from collections.abc import Iterator

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

# --------------------------------------------------------------------------- #
# Endpoint constants — REQUIRE LIVE VALIDATION against reginfo.gov (see module
# docstring). Grouped here so a human can confirm/adjust them in one place.
# --------------------------------------------------------------------------- #
API_BASE = "https://www.reginfo.gov/public/do"

# The Unified Agenda XML/JSON export endpoint. reginfo.gov publishes the agenda
# as the "Agenda XML Report"; we request the JSON rendition where available.
AGENDA_ENDPOINT = f"{API_BASE}/eAgendaXmlReport"

# Query-parameter names reginfo.gov uses to scope the export. Unverified — a
# human validating against the live service should confirm these keys.
PARAM_EDITION = "agendaEditionId"  # e.g. "202404" (Spring 2024)
PARAM_OUTPUT = "output"  # request JSON rather than raw XML where supported

# Response-envelope keys. Unverified against a live payload; kept as constants so
# validation is a one-line change rather than a code hunt.
_RESULTS_KEY = "results"
_COUNT_KEY = "count"
_NEXT_KEY = "next_page_url"

# Max entries requested per page. Only a round-trip-count optimization; the
# reader follows ``next_page_url`` until exhausted regardless.
PER_PAGE = 1000

# The most recent agenda edition to fetch when a caller does not specify one.
# Editions are ``YYYYMM`` with MM in {04 (Spring), 10 (Fall)}. This default is a
# placeholder for the "current edition" and should be validated live.
DEFAULT_EDITION = "202404"

# Transport hygiene: bound every request and retry transient failures with
# backoff so a flaky fetch fails slow-then-recovers rather than dropping entries.
_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 5
_PROGRESS_EVERY = 5_000


class UnifiedAgendaReader(Reader):
    """Yields raw Unified Agenda entry dicts for one or more agenda editions.

    ``editions`` defaults to :data:`DEFAULT_EDITION`. Callers doing incremental
    runs pass only the edition(s) newer than what is already stored; the
    transform handles merging with the prior table and dedup on
    (``rin``, ``agenda_edition``).
    """

    def __init__(
        self,
        *,
        editions: tuple[str, ...] | None = None,
        per_page: int = PER_PAGE,
        verbose: bool = False,
    ) -> None:
        self.editions = editions or (DEFAULT_EDITION,)
        self.per_page = per_page
        self.verbose = verbose
        self._client: httpx.Client | None = None
        self._seen = 0

    def iter_records(self) -> Iterator[dict]:
        logger.info("Unified Agenda: fetching editions {}", ", ".join(self.editions))
        with httpx.Client(timeout=_TIMEOUT, headers={"Accept": "application/json"}) as client:
            self._client = client
            for edition in self.editions:
                yield from self._fetch_edition(edition)
        logger.info("Unified Agenda: yielded {:,} entries", self._seen)

    # -- edition fetching ----------------------------------------------------

    def _fetch_edition(self, edition: str) -> Iterator[dict]:
        """Page through one agenda edition, following ``next_page_url``.

        Each yielded dict is annotated with the ``agenda_edition`` it came from,
        since the per-entry payload may not repeat the edition and it is half of
        the (``rin``, ``agenda_edition``) dedup key.
        """
        params: dict[str, object] = {
            PARAM_EDITION: edition,
            PARAM_OUTPUT: "json",
            "per_page": self.per_page,
        }
        url = AGENDA_ENDPOINT
        first = True
        count = 0
        collected = 0
        while url:
            payload = self._get(url, params if first else None)
            first = False
            if payload is None:
                break
            count = payload.get(_COUNT_KEY, count)
            for entry in payload.get(_RESULTS_KEY, []) or []:
                # Stamp the edition so the transform always has the dedup key.
                entry.setdefault("agenda_edition", edition)
                collected += 1
                self._seen += 1
                if self._seen % _PROGRESS_EVERY == 0:
                    logger.info("Unified Agenda: {:,} entries so far...", self._seen)
                yield entry
            url = payload.get(_NEXT_KEY) or ""
        if count and collected < count:
            # Make an incomplete edition loud rather than silent.
            logger.warning("Unified Agenda: edition {} returned {}/{} entries", edition, collected, count)

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
                    logger.error("Unified Agenda: giving up on {} after {} attempts: {}", url, attempt, exc)
                    return None
                backoff = min(2**attempt, 30)
                logger.warning(
                    "Unified Agenda: {} (attempt {}/{}), retrying in {}s", exc, attempt, _MAX_RETRIES, backoff
                )
                time.sleep(backoff)
        return None
