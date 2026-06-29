"""Fetch PDF bytes over HTTP.

The I/O half of the PDF text-extraction step (issue #9): given a document's
``fileUrl`` (e.g. a regulations.gov ``content.pdf`` rendition), download the
bytes so :func:`spicy_regs.transforms.extract_pdf_text` can turn them into
text. Network access lives here in ``sources`` so the transform stays pure.
"""

from __future__ import annotations

import httpx
from loguru import logger

# Regulations.gov attachments are almost always well under this; the cap is a
# guard against a pathological multi-hundred-MB file blowing up a batch run.
DEFAULT_MAX_BYTES = 100 * 1024 * 1024
DEFAULT_TIMEOUT = 30.0


def fetch_pdf_bytes(
    url: str,
    *,
    client: httpx.Client | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> bytes | None:
    """Download ``url`` and return its bytes, or ``None`` on any failure.

    Follows redirects. Returns ``None`` (rather than raising) on HTTP errors,
    timeouts, connection problems, or a body exceeding ``max_bytes`` so a
    batch enrichment run can skip the document and keep going. Pass a shared
    ``client`` to reuse a connection pool across many fetches.
    """
    owns_client = client is None
    client = client or httpx.Client(timeout=timeout, follow_redirects=True)
    try:
        resp = client.get(url)
        resp.raise_for_status()
        content = resp.content
        if len(content) > max_bytes:
            logger.warning("PDF at {} is {} bytes (> {} cap), skipping", url, len(content), max_bytes)
            return None
        return content
    except httpx.HTTPError as exc:
        logger.warning("Failed to fetch PDF {}: {}", url, exc)
        return None
    finally:
        if owns_client:
            client.close()
