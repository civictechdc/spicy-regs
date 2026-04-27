"""Build federal_register_search.json.gz: data blob for client-side FR search.

Mirrors build_search_index.py's pattern (gzipped JSON, abbreviated keys)
so the frontend can use the same MiniSearch wiring with a different
field map.

Shape:
    {
      "version": "2026-04-22T16:00:00Z",
      "generated_from": "federal_register.parquet",
      "count": 1850000,
      "docs": [
        {"id": "2024-12345", "a": "EPA", "t": "Lead and Copper Rule ...",
         "x": "Rule", "d": "2024-01-15", "s": "The U.S. ...",
         "u": "https://www.federalregister.gov/documents/..."},
        ...
      ]
    }

Field name map: id, a (agency_slugs), t (title), x (document_type),
d (publication_date), s (abstract), u (html_url).
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

INDEX_FILENAME = "federal_register_search.json.gz"

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _clean(text: str | None) -> str:
    if not text:
        return ""
    s = html.unescape(text)
    s = _TAG_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def build_fr_search_index(output_dir: Path) -> Path:
    """Read federal_register.parquet and emit federal_register_search.json.gz."""
    fr_path = output_dir / "federal_register.parquet"
    if not fr_path.exists():
        raise FileNotFoundError(f"{fr_path} not found; run the ETL first")

    out_path = output_dir / INDEX_FILENAME

    conn = duckdb.connect(":memory:")
    rows = conn.execute(
        f"""
        SELECT
            document_number,
            agency_slugs,
            title,
            document_type,
            publication_date,
            abstract,
            html_url
        FROM read_parquet('{fr_path}')
        WHERE document_number IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY document_number
            ORDER BY publication_date DESC NULLS LAST
        ) = 1
        ORDER BY publication_date DESC NULLS LAST
        """
    ).fetchall()

    logger.info("Building FR search index from {:,} unique documents...", len(rows))

    docs = []
    for doc_num, agency_slugs, title, doc_type, pub_date, abstract, html_url in rows:
        t = _clean(title)
        s = _clean(abstract)
        if not t and not s:
            continue
        doc = {
            "id": doc_num,
            "a": agency_slugs or "",
            "t": t,
            "x": doc_type or "",
            "d": pub_date or "",
        }
        if s:
            doc["s"] = s
        if html_url:
            doc["u"] = html_url
        docs.append(doc)

    version = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = {
        "version": version,
        "generated_from": "federal_register.parquet",
        "count": len(docs),
        "docs": docs,
    }

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
        gz_mb / raw_mb if raw_mb else 0,
    )
    return out_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    args = parser.parse_args()
    build_fr_search_index(args.output_dir)
