"""Fetch a Federal Register document's full-text HTML (``body_html_url``).

Used by the ``state_program_approvals`` rollup to read state code citations
out of state-program approvals. federalregister.gov is open (no key). Each call
either returns the HTML or raises :class:`FrBodyUnavailable`; it never turns a
failure into an empty body, so the caller can count failures and abort rather
than publish a table that silently lost its citations.
"""

from __future__ import annotations

import time

import httpx
from loguru import logger

_TIMEOUT = httpx.Timeout(60.0, connect=30.0)
_MAX_ATTEMPTS = 4
_USER_AGENT = "spicy-regs-etl/1.0 (+https://github.com/civictechdc/spicy-regs)"


class FrBodyUnavailable(RuntimeError):
    """A document body could not be fetched (non-retryable status or retries exhausted)."""


def make_client() -> httpx.Client:
    return httpx.Client(timeout=_TIMEOUT, follow_redirects=True, headers={"User-Agent": _USER_AGENT})


def fetch_body_html(client: httpx.Client, url: str) -> str:
    """GET one body, retrying 429 / 5xx / transport errors with backoff."""
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            resp = client.get(url)
        except httpx.TransportError as exc:
            reason = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code == 200:
                return resp.text
            if resp.status_code != 429 and resp.status_code < 500:
                raise FrBodyUnavailable(f"{url}: HTTP {resp.status_code}")
            reason = f"HTTP {resp.status_code}"
        if attempt == _MAX_ATTEMPTS:
            raise FrBodyUnavailable(f"{url}: {reason} after {attempt} attempts")
        backoff = min(2**attempt, 30)
        logger.warning("FR body: {} ({}/{}), retrying in {}s", reason, attempt, _MAX_ATTEMPTS, backoff)
        time.sleep(backoff)
    raise AssertionError("unreachable")
