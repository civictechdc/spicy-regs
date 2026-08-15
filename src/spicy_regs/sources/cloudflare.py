"""Cloudflare cache purge — invalidate the edge cache after a publish.

The data is served from R2 behind Cloudflare at ``data.spicy-regs.dev``. Once a
Cloudflare Cache Rule marks the corpus eligible for edge caching (Cache-Control
headers alone don't cache ``.parquet``/``.json.gz`` — those extensions aren't in
Cloudflare's default set), a cached object would otherwise serve until its TTL
expires even after the ETL republishes it. This module purges the exact URLs we
just wrote so a fresh publish is visible immediately, letting us cache
aggressively (fast reads for many concurrent clients) without serving stale data.

Purging is **best-effort and never fails a publish**: a purge error logs a
warning and returns, because the upload already succeeded and the moderate
origin ``max-age`` is a self-healing backstop — stale data ages out on its own
even if a purge is missed. It is a **no-op without credentials** (no
``CLOUDFLARE_API_TOKEN`` / ``CLOUDFLARE_ZONE_ID``), mirroring how ``upload_file``
no-ops without R2 credentials, so local runs and tests need no Cloudflare setup.
"""

from __future__ import annotations

from os import getenv

import httpx
from loguru import logger

# Cloudflare caps purge-by-URL at 30 URLs per request on Free/Pro/Business plans.
_MAX_URLS_PER_CALL = 30
_PURGE_TIMEOUT = 15.0


def _purge_config() -> tuple[str, str] | None:
    """Return (zone_id, token) if both are configured, else None."""
    token = getenv("CLOUDFLARE_API_TOKEN")
    zone_id = getenv("CLOUDFLARE_ZONE_ID")
    if not token or not zone_id:
        return None
    return zone_id, token


def purge_urls(urls: list[str]) -> None:
    """Purge specific URLs from the Cloudflare edge cache (best-effort).

    No-ops (with a debug log) when Cloudflare credentials aren't configured.
    Batches into groups of 30 to respect the purge-by-URL limit. Any HTTP or
    transport error is logged and swallowed — invalidation must never break the
    publish that already wrote the data.
    """
    if not urls:
        return
    config = _purge_config()
    if config is None:
        logger.debug("Cloudflare purge skipped (CLOUDFLARE_API_TOKEN / CLOUDFLARE_ZONE_ID not set)")
        return
    zone_id, token = config
    endpoint = f"https://api.cloudflare.com/client/v4/zones/{zone_id}/purge_cache"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    for start in range(0, len(urls), _MAX_URLS_PER_CALL):
        batch = urls[start : start + _MAX_URLS_PER_CALL]
        try:
            resp = httpx.post(endpoint, headers=headers, json={"files": batch}, timeout=_PURGE_TIMEOUT)
            if resp.status_code != 200 or not resp.json().get("success", False):
                logger.warning("Cloudflare purge failed ({}): {}", resp.status_code, resp.text[:300])
            else:
                logger.info("Purged {} URL(s) from Cloudflare cache", len(batch))
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning("Cloudflare purge error (upload already succeeded): {}", exc)
