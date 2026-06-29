"""Backfill ``text_content`` from PDF attachments on documents and comments.

This wires the two halves of issue #9 together:

    sources.fetch_pdf_bytes (download)  →  transforms.extract_pdf_text (parse)

over the rows of ``documents.parquet`` / the comments dataset. It is
intentionally a *separate* step from the metadata ETL: that pipeline only moves
JSON and is fast, whereas this downloads potentially many PDFs and is run on
demand / in its own job. The metadata ETL leaves ``text_content`` as ``NULL``;
this fills it.

The enrichment functions take the fetch + extract callables as parameters so
they can be exercised without touching the network.

Documents and comments pack their attachment renditions differently:

  * documents:  ``[{url, format, size}, ...]``                       (flat)
  * comments:   ``[{title, formats: [{url, format, size}]}, ...]``   (nested)

so each has its own URL extractor; both feed the same generic core.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Mapping
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
UrlsFn = Callable[[Mapping[str, object]], list[str]]


def _is_pdf(url: str | None, fmt: str | None) -> bool:
    if not url:
        return False
    return (fmt or "").lower() == "pdf" or url.lower().endswith(".pdf")


def _dedupe(urls: list[str]) -> list[str]:
    """Drop duplicate URLs while preserving first-seen order."""
    return list(dict.fromkeys(urls))


def _opt_str(value: object) -> str | None:
    """Narrow a polars row value (typed ``object``) to ``str | None``."""
    return value if isinstance(value, str) else None


def pdf_urls_for_document(attachments_json: str | None, file_url: str | None) -> list[str]:
    """PDF download URLs for one *document*, in order, de-duplicated.

    Prefers the structured ``attachments_json`` (a flat list of renditions);
    falls back to ``file_url`` when it points at a ``.pdf``. Non-PDF renditions
    (htm, docx, ...) are ignored — extracting those is out of scope.
    """
    urls: list[str] = []
    if attachments_json:
        try:
            for att in json.loads(attachments_json):
                if _is_pdf(att.get("url"), att.get("format")):
                    urls.append(att["url"])
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass
    if not urls and file_url and file_url.lower().endswith(".pdf"):
        urls.append(file_url)
    return _dedupe(urls)


def pdf_urls_for_comment(attachments_json: str | None) -> list[str]:
    """PDF download URLs for one *comment*, in order, de-duplicated.

    Comment attachments nest their renditions: each attachment carries a
    ``formats`` list of ``{url, format, size}``.
    """
    urls: list[str] = []
    if attachments_json:
        try:
            for att in json.loads(attachments_json):
                for fmt in att.get("formats") or []:
                    if _is_pdf(fmt.get("url"), fmt.get("format")):
                        urls.append(fmt["url"])
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass
    return _dedupe(urls)


def _extract_combined_text(urls: list[str], fetch: FetchFn, extract: ExtractFn) -> tuple[str | None, str]:
    """Fetch + extract every PDF for one row and combine into a single result.

    Returns ``(text_or_None, status)``. A row can have more than one PDF
    rendition; their texts are concatenated. The combined status is ``ok`` if
    any PDF yielded text, otherwise ``error`` > ``encrypted`` > ``empty`` in
    that order of informativeness.
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


def _enrich_with_pdf_text(
    df: pl.DataFrame,
    *,
    id_col: str,
    url_cols: list[str],
    urls_fn: UrlsFn,
    fetch: FetchFn,
    extract: ExtractFn,
    limit: int | None,
    max_workers: int,
    overwrite: bool,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Generic core: fill ``text_content`` / ``text_extraction_status`` for the
    rows of ``df`` whose attachments (via ``urls_fn``) include a PDF.

    Each unique ``id_col`` is processed once. Unless ``overwrite`` is set, rows
    that already have a ``text_extraction_status`` are skipped, so repeated runs
    are incremental.
    """
    for col in ("text_content", "text_extraction_status"):
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    candidates = df.select(id_col, *url_cols, "text_extraction_status").unique(subset=id_col, keep="first")

    work: list[tuple[str, list[str]]] = []
    for row in candidates.iter_rows(named=True):
        row_id = row[id_col]
        if row_id is None:
            continue
        if not overwrite and row["text_extraction_status"] is not None:
            continue
        urls = urls_fn(row)
        if urls:
            work.append((row_id, urls))

    if limit is not None:
        work = work[:limit]

    stats = {"selected": len(work), "ok": 0, "empty": 0, "encrypted": 0, "error": 0}
    if not work:
        logger.info("No PDF attachments to enrich")
        return df, stats

    logger.info("Enriching {} rows with PDF text ({} workers)...", len(work), max_workers)

    def _process(item: tuple[str, list[str]]) -> tuple[str, str | None, str]:
        row_id, urls = item
        text, status = _extract_combined_text(urls, fetch, extract)
        return row_id, text, status

    ids: list[str] = []
    texts: list[str | None] = []
    statuses: list[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for row_id, text, status in executor.map(_process, work):
            ids.append(row_id)
            texts.append(text)
            statuses.append(status)
            stats[status] = stats.get(status, 0) + 1

    updates = pl.DataFrame(
        {id_col: ids, "_new_text": texts, "_new_status": statuses},
        schema={id_col: pl.Utf8, "_new_text": pl.Utf8, "_new_status": pl.Utf8},
    )

    enriched = (
        df.join(updates, on=id_col, how="left")
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


def enrich_documents_with_pdf_text(
    df: pl.DataFrame,
    *,
    fetch: FetchFn = fetch_pdf_bytes,
    extract: ExtractFn = extract_pdf_text,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Fill ``text_content`` / ``text_extraction_status`` for PDF documents."""
    return _enrich_with_pdf_text(
        df,
        id_col="document_id",
        url_cols=["attachments_json", "file_url"],
        urls_fn=lambda row: pdf_urls_for_document(_opt_str(row["attachments_json"]), _opt_str(row["file_url"])),
        fetch=fetch,
        extract=extract,
        limit=limit,
        max_workers=max_workers,
        overwrite=overwrite,
    )


def enrich_comments_with_pdf_text(
    df: pl.DataFrame,
    *,
    fetch: FetchFn = fetch_pdf_bytes,
    extract: ExtractFn = extract_pdf_text,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Fill ``text_content`` / ``text_extraction_status`` for comment PDF attachments."""
    return _enrich_with_pdf_text(
        df,
        id_col="comment_id",
        url_cols=["attachments_json"],
        urls_fn=lambda row: pdf_urls_for_comment(_opt_str(row["attachments_json"])),
        fetch=fetch,
        extract=extract,
        limit=limit,
        max_workers=max_workers,
        overwrite=overwrite,
    )


def _enrich_parquet_file(
    path: Path,
    enrich: Callable[[pl.DataFrame], tuple[pl.DataFrame, dict[str, int]]],
) -> dict[str, int]:
    df = pl.read_parquet(path)
    enriched, stats = enrich(df)
    enriched.write_parquet(path, compression="zstd")
    logger.info("Wrote {} ({} rows)", path, len(enriched))
    return stats


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
    return _enrich_parquet_file(
        documents_path,
        lambda df: enrich_documents_with_pdf_text(df, limit=limit, max_workers=max_workers, overwrite=overwrite),
    )


def enrich_comments_parquet(
    comments_path: Path,
    *,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> dict[str, int]:
    """Read a single comments Parquet file, enrich it in place, write it back."""
    if not comments_path.exists():
        raise FileNotFoundError(f"{comments_path} not found; run the ETL first")
    return _enrich_parquet_file(
        comments_path,
        lambda df: enrich_comments_with_pdf_text(df, limit=limit, max_workers=max_workers, overwrite=overwrite),
    )


def enrich_comment_partitions(
    partition_dir: Path,
    *,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> dict[str, int]:
    """Enrich every ``agency_code=*/*.parquet`` partition under ``partition_dir``.

    Comments are Hive-partitioned by agency; each partition is enriched and
    rewritten independently. ``limit`` (if given) is the total budget across
    all partitions, so a capped run won't silently process every agency.
    """
    parts = sorted(partition_dir.glob("agency_code=*/*.parquet"))
    if not parts:
        raise FileNotFoundError(f"No comment partitions found under {partition_dir}")

    totals = {"selected": 0, "ok": 0, "empty": 0, "encrypted": 0, "error": 0}
    remaining = limit
    for part in parts:
        if remaining is not None and remaining <= 0:
            logger.info("Reached --limit budget; {} partitions left unprocessed", len(parts) - parts.index(part))
            break
        stats = _enrich_parquet_file(
            part,
            lambda df: enrich_comments_with_pdf_text(df, limit=remaining, max_workers=max_workers, overwrite=overwrite),
        )
        for k, v in stats.items():
            totals[k] += v
        if remaining is not None:
            remaining -= stats["selected"]

    logger.info("Comment partition enrichment totals: {}", totals)
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill documents/comments text_content from PDF attachments.")
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument(
        "--target",
        choices=["documents", "comments"],
        default="documents",
        help="Which dataset to enrich (default: documents)",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max rows to process this run")
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--overwrite", action="store_true", help="Re-extract rows that already have a status")
    args = parser.parse_args()

    if args.target == "documents":
        enrich_documents_parquet(
            args.output_dir / "documents.parquet",
            limit=args.limit,
            max_workers=args.max_workers,
            overwrite=args.overwrite,
        )
        return

    # Comments: prefer the Hive-partitioned layout, fall back to the monolithic file.
    partition_dir = args.output_dir / "comments" / "agency"
    if partition_dir.exists():
        enrich_comment_partitions(
            partition_dir,
            limit=args.limit,
            max_workers=args.max_workers,
            overwrite=args.overwrite,
        )
    else:
        enrich_comments_parquet(
            args.output_dir / "comments.parquet",
            limit=args.limit,
            max_workers=args.max_workers,
            overwrite=args.overwrite,
        )


if __name__ == "__main__":
    main()
