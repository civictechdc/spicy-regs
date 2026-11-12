"""Resolve where a comment-derived rollup reads ``comments.parquet`` from.

``comments.parquet`` is the public monolith the comments mirror publishes
(~4.6 GB, almost all of it comment and attachment text). Rollups derived from
comments need only a few narrow columns, so instead of priming the whole file to
disk they read it straight from the public R2 URL over ``httpfs`` and let Parquet
projection pushdown fetch just the column chunks their query names. A local
``comments.parquet`` in ``output_dir`` still wins when present, so local dev,
tests, and a primed run never touch the network.
"""

from __future__ import annotations

import os
from pathlib import Path

from loguru import logger

DEFAULT_R2_BASE_URL = "https://data.spicy-regs.dev"


def resolve_comments_source(output_dir: Path, *, consumer: str) -> str:
    """Return the ``read_parquet`` target for comments: local file, else R2 URL.

    ``consumer`` names the rollup in the log line. The public URL comes from
    ``R2_PUBLIC_URL`` (falling back to the production bucket) and is validated
    before it is interpolated into SQL.
    """
    local = output_dir / "comments.parquet"
    if local.exists():
        logger.info("{}: using local {}", consumer, local)
        return str(local)

    base_url = os.environ.get("R2_PUBLIC_URL") or DEFAULT_R2_BASE_URL
    base_url = base_url.rstrip("/")
    if not base_url.startswith("https://"):
        raise RuntimeError(f"R2_PUBLIC_URL must be an https:// URL, got {base_url!r}")
    if any(c in base_url for c in ("'", "\\", "\x00", "\n", "\r")):
        raise RuntimeError(f"R2_PUBLIC_URL contains illegal characters: {base_url!r}")
    url = f"{base_url}/comments.parquet"
    logger.info("{}: reading comments remotely from {} (column projection)", consumer, url)
    return url
