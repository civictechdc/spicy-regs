"""Backfill ``documents.text_content`` from each document's PDF rendition.

This wires the two halves of issue #9 together:

    sources.fetch_pdf_bytes (download)  →  transforms.extract_pdf_text (parse)

over the rows of ``documents.parquet``. It is intentionally a *separate* step
from the metadata ETL: that pipeline only moves JSON and is fast, whereas this
downloads potentially tens of thousands of PDFs and is run on demand / in its
own job. The metadata ETL leaves ``text_content`` as ``NULL``; this fills it.

``enrich_documents_with_pdf_text`` takes the fetch + extract callables as
parameters so it can be exercised without touching the network.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl
from loguru import logger

from spicy_regs.sources.pdf import fetch_pdf_bytes
from spicy_regs.transforms.pdf_text import (
    PAGE_SEPARATOR,
    PdfTextResult,
    PdfTextStatus,
    extract_pdf_text,
)

FetchFn = Callable[[str], bytes | None]
ExtractFn = Callable[[bytes], PdfTextResult]


def pdf_urls_for_document(attachments_json: str | None, file_url: str | None) -> list[str]:
    """Return the PDF download URLs for one document, in order, de-duplicated.

    Prefers the structured ``attachments_json`` (picking renditions whose
    ``format`` is ``pdf``); falls back to ``file_url`` when it points at a
    ``.pdf``. Non-PDF renditions (htm, docx, ...) are ignored — extracting
    those is out of scope.
    """
    urls: list[str] = []
    if attachments_json:
        try:
            for att in json.loads(attachments_json):
                fmt = (att.get("format") or "").lower()
                url = att.get("url")
                if url and (fmt == "pdf" or url.lower().endswith(".pdf")):
                    urls.append(url)
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass
    if not urls and file_url and file_url.lower().endswith(".pdf"):
        urls.append(file_url)
    # De-dupe while preserving order.
    return list(dict.fromkeys(urls))


def _extract_document_text(urls: list[str], fetch: FetchFn, extract: ExtractFn) -> tuple[str | None, str]:
    """Fetch + extract every PDF for a document and combine into one result.

    Returns ``(text_or_None, status)``. A document can have more than one PDF
    rendition; their texts are concatenated. The combined status is the best
    outcome seen: ``ok`` if any PDF yielded text, otherwise ``error`` >
    ``encrypted`` > ``empty`` in that order of informativeness.
    """
    texts: list[str] = []
    statuses: list[str] = []
    for url in urls:
        data = fetch(url)
        if data is None:
            statuses.append(PdfTextStatus.ERROR.value)
            continue
        result = extract(data)
        statuses.append(result.status.value)
        if result.text:
            texts.append(result.text)

    if texts:
        return PAGE_SEPARATOR.join(texts), PdfTextStatus.OK.value
    for candidate in (PdfTextStatus.ERROR.value, PdfTextStatus.ENCRYPTED.value, PdfTextStatus.EMPTY.value):
        if candidate in statuses:
            return None, candidate
    return None, PdfTextStatus.EMPTY.value


def enrich_documents_with_pdf_text(
    df: pl.DataFrame,
    *,
    fetch: FetchFn = fetch_pdf_bytes,
    extract: ExtractFn = extract_pdf_text,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Fill ``text_content`` / ``text_extraction_status`` for PDF documents.

    Each unique ``document_id`` with a PDF rendition is processed once. Unless
    ``overwrite`` is set, documents that already have a
    ``text_extraction_status`` are skipped, so repeated runs are incremental.
    Returns the updated DataFrame and a stats counter.
    """
    # Ensure the target columns exist even on older Parquet files.
    for col in ("text_content", "text_extraction_status"):
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    candidates = df.select("document_id", "attachments_json", "file_url", "text_extraction_status").unique(
        subset="document_id", keep="first"
    )

    work: list[tuple[str, list[str]]] = []
    for row in candidates.iter_rows(named=True):
        doc_id = row["document_id"]
        if doc_id is None:
            continue
        if not overwrite and row["text_extraction_status"] is not None:
            continue
        urls = pdf_urls_for_document(row["attachments_json"], row["file_url"])
        if urls:
            work.append((doc_id, urls))

    if limit is not None:
        work = work[:limit]

    stats = {"selected": len(work), "ok": 0, "empty": 0, "encrypted": 0, "error": 0}
    if not work:
        logger.info("No PDF documents to enrich")
        return df, stats

    logger.info("Enriching {} documents with PDF text ({} workers)...", len(work), max_workers)

    def _process(item: tuple[str, list[str]]) -> tuple[str, str | None, str]:
        doc_id, urls = item
        text, status = _extract_document_text(urls, fetch, extract)
        return doc_id, text, status

    ids: list[str] = []
    texts: list[str | None] = []
    statuses: list[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for doc_id, text, status in executor.map(_process, work):
            ids.append(doc_id)
            texts.append(text)
            statuses.append(status)
            stats[status] = stats.get(status, 0) + 1

    updates = pl.DataFrame(
        {"document_id": ids, "_new_text": texts, "_new_status": statuses},
        schema={"document_id": pl.Utf8, "_new_text": pl.Utf8, "_new_status": pl.Utf8},
    )

    enriched = (
        df.join(updates, on="document_id", how="left")
        .with_columns(
            text_content=pl.coalesce(["_new_text", "text_content"]),
            text_extraction_status=pl.coalesce(["_new_status", "text_extraction_status"]),
        )
        .drop("_new_text", "_new_status")
    )

    logger.info(
        "PDF enrichment: {} ok, {} empty, {} encrypted, {} error",
        stats["ok"],
        stats["empty"],
        stats["encrypted"],
        stats["error"],
    )
    return enriched, stats


def enrich_documents_parquet(
    documents_path: Path,
    *,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> dict[str, int]:
    """Read ``documents.parquet``, enrich it in place, and write it back."""
    if not documents_path.exists():
        raise FileNotFoundError(f"{documents_path} not found; run the ETL first")

    df = pl.read_parquet(documents_path)
    enriched, stats = enrich_documents_with_pdf_text(df, limit=limit, max_workers=max_workers, overwrite=overwrite)
    enriched.write_parquet(documents_path, compression="zstd")
    logger.info("Wrote {} ({} rows)", documents_path, len(enriched))
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--limit", type=int, default=None, help="Max documents to process this run")
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--overwrite", action="store_true", help="Re-extract documents that already have a status")
    args = parser.parse_args()
    enrich_documents_parquet(
        args.output_dir / "documents.parquet",
        limit=args.limit,
        max_workers=args.max_workers,
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    main()
