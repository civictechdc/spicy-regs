"""Reader connector for U.S. Government Accountability Office (GAO) reports.

GAO reports are the federal watchdog's audits, evaluations, and recommendations
on how agencies implement laws and rules — the *oversight* layer over the
rulemakings this dataset tracks. Every GAO product carries a ``GAO-##-######``
id, a title, a publication date, and a two-part narrative ("What GAO Found" /
"Why GAO Did This Study").

**Data access.** GAO's site sits behind a bot-protection layer that 403s its
sitemap, product JSON, and search endpoints to non-browser clients, so there is
no bulk/JSON API a pipeline can rely on. The one machine-readable surface that
serves anonymously is the public RSS feed at :data:`RSS_URL`, which lists the
~25 most recently published products with title, link (the product id lives in
the URL), description, and publication date. This reader parses that feed.

Because the feed is a recent-items window (not the full archive), the pipeline
is an **incremental accumulator**: each daily run appends any new products to
the prior published table (see
:func:`~spicy_regs.transforms.build_gao_reports.build_gao_reports`). Over time
the table grows into a rolling history; a full historical backfill would need a
different (currently bot-blocked) source.

The reader is a *pure source*: it yields raw item dicts parsed from the feed.
Shaping them into the published schema is the transform's job.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from xml.etree import ElementTree as ET

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

# GAO's public "Reports" RSS feed. The broader product endpoints (sitemap,
# ``?_format=json``, the search API) are Cloudflare-403'd for non-browser
# clients; this feed is the one anonymous, machine-readable surface.
RSS_URL = "https://www.gao.gov/rss/reports.xml"

# A descriptive UA — the /rss/ path is not UA-gated, but we identify the client
# rather than send httpx's default.
_USER_AGENT = "spicy-regs-etl/1.0 (+https://github.com/civictechdc/spicy-regs)"

_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_RETRIES = 5


class GaoReportsReader(Reader):
    """Yields raw GAO product dicts parsed from the reports RSS feed.

    Each yielded dict has the item's ``title``, ``link``, ``description``, and
    ``pub_date`` (the raw RFC-822 ``pubDate`` string). ``max_records`` bounds the
    fetch for validation. The feed is a fixed recent-items window, so there is no
    pagination and no ``since`` filter — incremental accumulation happens in the
    transform's merge with the prior table.
    """

    def __init__(
        self,
        *,
        url: str = RSS_URL,
        max_records: int | None = None,
        verbose: bool = False,
    ) -> None:
        self.url = url
        self.max_records = max_records
        self.verbose = verbose
        self._seen = 0

    def iter_records(self) -> Iterator[dict]:
        logger.info("GAO reports: fetching RSS feed {}", self.url)
        raw = self._fetch()
        if raw is None:
            logger.warning("GAO reports: feed fetch failed — yielding nothing")
            return
        if _has_doctype(raw):
            # A well-formed RSS feed carries no DOCTYPE; its presence is where
            # entity-expansion ("billion laughs") / XXE payloads live. Reject
            # rather than hand it to the stdlib parser.
            logger.error("GAO reports: feed contains a DOCTYPE declaration — refusing to parse")
            return
        try:
            root = ET.fromstring(raw)
        except ET.ParseError as exc:
            logger.error("GAO reports: could not parse feed XML: {}", exc)
            return
        for item in root.iter("item"):
            self._seen += 1
            yield _item_to_dict(item)
            if self.max_records is not None and self._seen >= self.max_records:
                if self.verbose:
                    logger.debug("GAO reports: reached max_records {} — stopping", self.max_records)
                return
        logger.info("GAO reports: yielded {:,} items", self._seen)

    def _fetch(self) -> bytes | None:
        """GET the feed with bounded retries + exponential backoff."""
        headers = {"User-Agent": _USER_AGENT, "Accept": "application/rss+xml, application/xml, text/xml"}
        with httpx.Client(timeout=_TIMEOUT, headers=headers, follow_redirects=True) as client:
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    resp = client.get(self.url)
                    if resp.status_code == 429 or resp.status_code >= 500:
                        raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                    resp.raise_for_status()
                    return resp.content
                except httpx.HTTPError as exc:
                    if attempt == _MAX_RETRIES:
                        logger.error("GAO reports: giving up on {} after {} attempts: {}", self.url, attempt, exc)
                        return None
                    backoff = min(2**attempt, 30)
                    logger.warning(
                        "GAO reports: {} (attempt {}/{}), retrying in {}s", exc, attempt, _MAX_RETRIES, backoff
                    )
                    time.sleep(backoff)
        return None


def _has_doctype(raw: bytes) -> bool:
    """True if the document prolog contains a ``<!DOCTYPE`` declaration.

    Entity-expansion and XXE payloads require a DOCTYPE; a legitimate RSS feed
    has none. In XML the DOCTYPE must appear before the root element, so we scan
    only the prolog (up to the first ``<rss``) — case-insensitively.
    """
    prolog = raw.split(b"<rss", 1)[0]
    return b"<!doctype" in prolog.lower()


def _item_to_dict(item: ET.Element) -> dict:
    """Project one RSS ``<item>`` element to a flat dict of its text children."""

    def text(tag: str) -> str | None:
        el = item.find(tag)
        return el.text.strip() if el is not None and el.text else None

    return {
        "title": text("title"),
        "link": text("link"),
        "description": text("description"),
        "pub_date": text("pubDate"),
    }
