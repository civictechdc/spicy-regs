"""Build docket_search.json: the data blob powering client-side search.

Produces a single gzipped JSON file the frontend loads into MiniSearch at
runtime. We emit raw docket records (not a pre-serialized MiniSearch index)
to avoid coupling the ETL to the frontend's search-library version.

Shape:
    {
      "version": "2026-04-14T16:00:00Z",
      "generated_from": "dockets.parquet",
      "count": 273163,
      "docs": [
        {"id": "EPA-HQ-OW-2022-0801", "a": "EPA", "t": "Lead and Copper ...",
         "x": "Proposed Rule", "d": "2023-12-06T05:00:00Z", "s": "The U.S. ..."},
        ...
      ]
    }

Field names are abbreviated (`id`, `a`, `t`, `x`, `d`, `s`) to shave a
~5% JSON size win across 273K records. Frontend expands them back at load
time — see `frontend/src/lib/search/index.ts`.
"""

from __future__ import annotations

import gzip
import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from loguru import logger

INDEX_FILENAME = "docket_search.json.gz"

# Strip HTML tags (the `<br/>` kind) but keep entity decoding so that
# `&amp;` becomes `&` etc. The abstract field from regulations.gov has
# both injected frequently.
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _clean(text: str | None) -> str:
    if not text:
        return ""
    s = html.unescape(text)
    s = _TAG_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def build_search_index(output_dir: Path) -> Path:
    """Read dockets.parquet and emit docket_search.json.gz."""
    dockets_path = output_dir / "dockets.parquet"
    if not dockets_path.exists():
        raise FileNotFoundError(f"{dockets_path} not found; run the ETL first")

    out_path = output_dir / INDEX_FILENAME

    conn = duckdb.connect(":memory:")
    rows = conn.execute(
        f"""
        SELECT
            docket_id,
            agency_code,
            title,
            docket_type,
            modify_date,
            abstract
        FROM read_parquet('{dockets_path}')
        WHERE docket_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY docket_id ORDER BY modify_date DESC) = 1
        ORDER BY modify_date DESC NULLS LAST
        """
    ).fetchall()

    logger.info("Building search index from {:,} unique dockets...", len(rows))

    docs = []
    for docket_id, agency_code, title, docket_type, modify_date, abstract in rows:
        t = _clean(title)
        s = _clean(abstract)
        # Skip rows that have no searchable text at all — they would just
        # be noise in the index and never match a query anyway.
        if not t and not s:
            continue
        doc = {
            "id": docket_id,
            "a": agency_code or "",
            "t": t,
            "x": docket_type or "",
            "d": modify_date or "",
        }
        if s:
            doc["s"] = s
        docs.append(doc)

    version = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = {
        "version": version,
        "generated_from": "dockets.parquet",
        "count": len(docs),
        "docs": docs,
    }

    # Gzip at compression level 9 — the file is written once per ETL run
    # and served millions of times from CDN, so spend the CPU.
    raw_bytes = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with gzip.open(out_path, "wb", compresslevel=9) as f:
        f.write(raw_bytes)

    raw_mb = len(raw_bytes) / 1024 / 1024
    gz_mb = out_path.stat().st_size / 1024 / 1024
    logger.info(
        "Wrote {} — {:,} docs, {:.1f} MB raw → {:.1f} MB gzipped ({:.0%} ratio)",
        out_path.name,
        len(docs),
        raw_mb,
        gz_mb,
        gz_mb / raw_mb,
    )
    return out_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    args = parser.parse_args()
    build_search_index(args.output_dir)
