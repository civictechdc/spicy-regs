"""Reader connector for the CourtListener REST API (v4, RECAP search).

Brings federal-litigation ingestion *in-repo* as an external source
complementary to the regulations.gov view. When an agency finalizes a rule, the
rule is frequently challenged in court under the Administrative Procedure Act
(APA); those suits name the same agencies (and, in the pleadings, the same rules)
that the corpus already tracks. Capturing them lets the corpus link a rulemaking
to the litigation it provoked.

The reader is a *pure source*: it yields raw docket payloads (dicts) exactly as
the API returns them. Shaping them into the published schema is the job of
:func:`~spicy_regs.transforms.build_courtlistener.build_courtlistener`.

**Endpoint + scope.** We query the public ``/search/`` endpoint with ``type=r``
(RECAP dockets) filtered to ``nature_of_suit=899`` — "Other Statutes:
Administrative Procedures Act/Review or Appeal of Agency Decision", the precise
docket classification for APA challenges to federal agency action. That is a
bounded, high-signal corpus (~7.6K dockets across all federal courts) rather than
the whole PACER firehose.

**Cursor pagination.** The v4 search endpoint paginates with an opaque cursor
carried in the ``next`` URL (deep pagination requires ``order_by=dateFiled asc``
or ``desc``; score ordering does not support cursors). We follow ``next`` until
it is null, which is robust to the exact page size.

**API token (required).** ``COURTLISTENER_API_TOKEN`` is sent as an
``Authorization: Token <token>`` header. CourtListener used to serve ``/search/``
keyless, but stopped: from 2026-10-27 keyless requests got ``405 Not Allowed``,
and from 2026-11-03 ``401 Authentication credentials were not provided``. Tokens
are free with a CourtListener account (https://www.courtlistener.com/profile/api/).

**Failing loudly.** Without a token, or on any non-retryable 4xx, the reader
raises :class:`CourtListenerUnavailable` instead of yielding nothing. Unlike the
keyed sources that treat a keyless run as a no-op, a no-op here would still
re-upload the prior table and report success; that hid this outage for 17 days.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from datetime import date

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

API_BASE = "https://www.courtlistener.com/api/rest/v4"

# Nature-of-suit code for APA / review-or-appeal-of-agency-decision dockets. This
# is the semantic filter that scopes the ingest to litigation over federal rules.
APA_NATURE_OF_SUIT = "899"

# Env var holding the optional CourtListener API token (Authorization: Token ...).
API_TOKEN_ENV_VAR = "COURTLISTENER_API_TOKEN"

# Transport hygiene: bound every request and retry transient failures with
# backoff so a flaky page fails slow-then-recovers rather than dropping dockets.
# 429s and 5xx are retried with backoff; any other 4xx is a configuration
# problem (bad or missing token, blocked client) and fails immediately.
_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 6
_PROGRESS_EVERY = 1_000


class CourtListenerUnavailable(RuntimeError):
    """CourtListener could not be queried: no token, a rejected request, or retries exhausted."""


def _resolve_api_token() -> str | None:
    """Return the ``COURTLISTENER_API_TOKEN`` if set, else None."""
    value = os.environ.get(API_TOKEN_ENV_VAR)
    return value or None


class CourtListenerReader(Reader):
    """Yields raw CourtListener RECAP-docket search-result dicts (nature-of-suit 899).

    ``since`` scopes the fetch to dockets filed on/after a date (server-side via
    the ``filed_after`` search param); incremental runs pass the max ``date_filed``
    already stored. ``max_records`` bounds the fetch for validation. Raises
    :class:`CourtListenerUnavailable` if no token is configured or a request fails.
    """

    def __init__(
        self,
        *,
        since: date | None = None,
        nature_of_suit: str = APA_NATURE_OF_SUIT,
        max_records: int | None = None,
        api_token: str | None = None,
        verbose: bool = False,
    ) -> None:
        self.since = since
        self.nature_of_suit = nature_of_suit
        self.max_records = max_records
        self.api_token = api_token or _resolve_api_token()
        self.verbose = verbose
        self._client: httpx.Client | None = None
        self._seen = 0

    def iter_records(self) -> Iterator[dict]:
        if not self.api_token:
            raise CourtListenerUnavailable(
                f"{API_TOKEN_ENV_VAR} is not set. CourtListener no longer serves /search/ without a token; "
                "create one at https://www.courtlistener.com/profile/api/ and add it as a repository secret."
            )
        logger.info(
            "CourtListener: fetching RECAP dockets (nature_of_suit={}, since={}, max_records={})",
            self.nature_of_suit,
            self.since or "the beginning",
            self.max_records or "unbounded",
        )
        headers = {"Accept": "application/json", "Authorization": f"Token {self.api_token}"}
        with httpx.Client(timeout=_TIMEOUT, headers=headers) as client:
            self._client = client
            yield from self._paginate()
        logger.info("CourtListener: yielded {:,} dockets", self._seen)

    # -- pagination ----------------------------------------------------------

    def _paginate(self) -> Iterator[dict]:
        """Follow the cursor ``next`` URL from the first page until it is null."""
        params: dict[str, object] = {
            "type": "r",  # RECAP dockets
            "nature_of_suit": self.nature_of_suit,
            # dateFiled ordering is required for cursor (deep) pagination.
            "order_by": "dateFiled asc",
        }
        if self.since is not None:
            # The search endpoint's date filter takes MM/DD/YYYY.
            params["filed_after"] = self.since.strftime("%m/%d/%Y")
        url = f"{API_BASE}/search/"
        first = True
        while url:
            payload = self._get(url, params if first else None)
            first = False
            for docket in payload.get("results") or []:
                self._seen += 1
                if self._seen % _PROGRESS_EVERY == 0:
                    logger.info("CourtListener: {:,} dockets so far...", self._seen)
                yield docket
                if self.max_records is not None and self._seen >= self.max_records:
                    if self.verbose:
                        logger.debug("CourtListener: reached max_records {} — stopping", self.max_records)
                    return
            url = payload.get("next") or ""

    def _get(self, url: str, params: dict | None) -> dict:
        """GET with bounded retries + exponential backoff on 429/5xx/transport errors.

        Any other 4xx raises at once: retrying a rejected token or a blocked
        client only delays the failure. Exhausted retries also raise, so a
        failed page can never be mistaken for the end of the results.
        """
        assert self._client is not None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = self._client.get(url, params=params)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                if resp.status_code >= 400:
                    raise CourtListenerUnavailable(
                        f"CourtListener rejected {url} with HTTP {resp.status_code}: {resp.text[:200]}"
                        + (f" (check {API_TOKEN_ENV_VAR})" if resp.status_code in (401, 403) else "")
                    )
                return resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                if attempt == _MAX_RETRIES:
                    raise CourtListenerUnavailable(
                        f"CourtListener: giving up on {url} after {attempt} attempts: {exc}"
                    ) from exc
                backoff = min(2**attempt, 60)
                logger.warning(
                    "CourtListener: {} (attempt {}/{}), retrying in {}s", exc, attempt, _MAX_RETRIES, backoff
                )
                time.sleep(backoff)
        raise AssertionError("unreachable")  # pragma: no cover
