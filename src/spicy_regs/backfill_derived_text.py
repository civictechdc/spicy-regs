"""Backfill comment ``text_content`` from Mirrulations derived-data extracted text.

The ETL fills ``text_content`` inline for *new* comments
(:class:`~spicy_regs.transforms.enrich_derived_text.EnrichCommentText`), but
every comment published before that wiring still has ``text_content`` NULL. This
is the one-time / re-runnable backfill: it reads the already-published comment
Parquet, fills ``text_content`` from the bucket's ``derived-data`` prefix (no PDF
download, no JSON re-ingest), and writes it back.

It is the derived-data sibling of :mod:`spicy_regs.enrich_pdf`: same
in-place, incremental, partition-aware shape, but the text source is
Mirrulations' pre-extracted ``.txt`` rather than a downloaded PDF. Rows are
skipped unless they have an attachment and no ``text_extraction_status`` yet, so
repeated runs only do new work.

A wrinkle the PDF path doesn't have: the Hive comment partitions drop the
``agency_code`` column (it is encoded in the ``agency_code=<X>`` directory name),
but the derived-data S3 path is keyed by agency. So the partition walker reads
the agency from the directory and passes it in explicitly; the monolithic
``comments.parquet`` still carries the column and is read per row.
"""

from __future__ import annotations

import argparse
import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from spicy_regs.sources import mirrulations
from spicy_regs.sources.derived_text import DerivedCommentText
from spicy_regs.transforms.pdf_text import PdfTextStatus

ResourceFactory = Callable[[], Any]

_AGENCY_DIR_RE = re.compile(r"agency_code=([^/]+)")


def enrich_comments_with_derived_text(
    df: pl.DataFrame,
    *,
    agency: str | None = None,
    resource_factory: ResourceFactory = mirrulations.s3_resource,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Fill ``text_content`` / ``text_extraction_status`` from derived-data S3.

    Only attachment-bearing comments are candidates; unless ``overwrite`` is set,
    rows that already have a ``text_extraction_status`` are skipped so repeated
    runs are incremental. ``agency`` overrides the per-row ``agency_code`` (the
    Hive partitions don't carry that column); when ``None`` it is read from the
    frame. Work is grouped by docket and fanned out across ``max_workers`` so
    each docket's extraction prefix is listed once.
    """
    for col in ("text_content", "text_extraction_status"):
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    select_cols = ["comment_id", "docket_id", "attachments_json", "text_extraction_status"]
    if agency is None:
        select_cols.append("agency_code")
    candidates = df.select(select_cols).unique(subset="comment_id", keep="first")

    # Group candidate comment ids by (agency, docket) so each docket's
    # derived-data prefix is listed exactly once, honoring the row budget.
    work_by_docket: dict[tuple[str, str], list[str]] = {}
    selected = 0
    for row in candidates.iter_rows(named=True):
        if limit is not None and selected >= limit:
            break
        comment_id = row["comment_id"]
        if comment_id is None or not row["attachments_json"]:
            continue
        if not overwrite and row["text_extraction_status"] is not None:
            continue
        row_agency = agency or row.get("agency_code")
        docket_id = row["docket_id"]
        if not (row_agency and docket_id):
            continue
        work_by_docket.setdefault((row_agency, docket_id), []).append(comment_id)
        selected += 1

    stats = {"selected": selected, "ok": 0, "missing": 0}
    if not work_by_docket:
        logger.info("No comments to backfill from derived-data")
        return df, stats

    logger.info(
        "Backfilling {} comments across {} dockets from derived-data ({} workers)...",
        selected, len(work_by_docket), max_workers,
    )

    def _fetch_docket(item: tuple[tuple[str, str], list[str]]) -> dict[str, str]:
        (docket_agency, docket_id), comment_ids = item
        # One fetcher (and S3 resource) per task keeps the per-docket listing
        # cache thread-local; DerivedCommentText is not shared-safe.
        fetcher = DerivedCommentText(resource_factory())
        found: dict[str, str] = {}
        for comment_id in comment_ids:
            text = fetcher.text_for(docket_agency, docket_id, comment_id)
            if text:
                found[comment_id] = text
        return found

    ids: list[str] = []
    texts: list[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for found in executor.map(_fetch_docket, work_by_docket.items()):
            for comment_id, text in found.items():
                ids.append(comment_id)
                texts.append(text)

    stats["ok"] = len(ids)
    stats["missing"] = selected - len(ids)

    if not ids:
        logger.info("Backfill: 0 ok, {} missing (no derived-data text found)", stats["missing"])
        return df, stats

    updates = pl.DataFrame(
        {"comment_id": ids, "_new_text": texts, "_new_status": [PdfTextStatus.OK.value] * len(ids)},
        schema={"comment_id": pl.Utf8, "_new_text": pl.Utf8, "_new_status": pl.Utf8},
    )
    enriched = (
        df.join(updates, on="comment_id", how="left")
        .with_columns(
            text_content=pl.coalesce(["_new_text", "text_content"]),
            text_extraction_status=pl.coalesce(["_new_status", "text_extraction_status"]),
        )
        .drop("_new_text", "_new_status")
    )

    logger.info("Backfill: {} ok, {} missing", stats["ok"], stats["missing"])
    return enriched, stats


def _backfill_file(
    path: Path,
    *,
    agency: str | None,
    resource_factory: ResourceFactory,
    limit: int | None,
    max_workers: int,
    overwrite: bool,
) -> dict[str, int]:
    df = pl.read_parquet(path)
    enriched, stats = enrich_comments_with_derived_text(
        df,
        agency=agency,
        resource_factory=resource_factory,
        limit=limit,
        max_workers=max_workers,
        overwrite=overwrite,
    )
    if stats["ok"]:
        enriched.write_parquet(path, compression="zstd")
        logger.info("Wrote {} ({} rows, {} filled)", path, len(enriched), stats["ok"])
    return stats


def backfill_comments_parquet(
    comments_path: Path,
    *,
    resource_factory: ResourceFactory = mirrulations.s3_resource,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> dict[str, int]:
    """Backfill the monolithic ``comments.parquet`` (carries ``agency_code``)."""
    if not comments_path.exists():
        raise FileNotFoundError(f"{comments_path} not found; download or build the dataset first")
    return _backfill_file(
        comments_path,
        agency=None,
        resource_factory=resource_factory,
        limit=limit,
        max_workers=max_workers,
        overwrite=overwrite,
    )


def backfill_comment_partitions(
    partition_dir: Path,
    *,
    resource_factory: ResourceFactory = mirrulations.s3_resource,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> tuple[dict[str, int], list[Path]]:
    """Backfill every ``agency_code=*/*.parquet`` partition; agency comes from the path.

    Returns the aggregate stats and the list of partition files that were
    actually modified (so a caller can upload only those to R2). ``limit`` (if
    given) is the total comment budget across all partitions.
    """
    parts = sorted(partition_dir.glob("agency_code=*/*.parquet"))
    if not parts:
        raise FileNotFoundError(f"No comment partitions found under {partition_dir}")

    totals = {"selected": 0, "ok": 0, "missing": 0}
    changed: list[Path] = []
    remaining = limit
    for part in parts:
        if remaining is not None and remaining <= 0:
            logger.info("Reached --limit budget; {} partitions left unprocessed", len(parts) - parts.index(part))
            break
        match = _AGENCY_DIR_RE.search(str(part.parent))
        if not match:
            logger.warning("Skipping {} — no agency_code in path", part)
            continue
        stats = _backfill_file(
            part,
            agency=match.group(1),
            resource_factory=resource_factory,
            limit=remaining,
            max_workers=max_workers,
            overwrite=overwrite,
        )
        for key, value in stats.items():
            totals[key] += value
        if stats["ok"]:
            changed.append(part)
        if remaining is not None:
            remaining -= stats["selected"]

    logger.info("Backfill totals: {} (changed {} partitions)", totals, len(changed))
    return totals, changed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill comment text_content from Mirrulations derived-data extracted text."
    )
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--limit", type=int, default=None, help="Max comments to backfill this run")
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--overwrite", action="store_true", help="Re-fill rows that already have a status")
    parser.add_argument(
        "--upload", action="store_true", help="Upload changed comment partitions + index to R2 (needs credentials)"
    )
    args = parser.parse_args()

    # Prefer the Hive-partitioned layout, fall back to the monolithic file.
    partition_dir = args.output_dir / "comments" / "agency"
    if partition_dir.exists():
        _, changed = backfill_comment_partitions(
            partition_dir,
            limit=args.limit,
            max_workers=args.max_workers,
            overwrite=args.overwrite,
        )
        if args.upload and changed:
            from spicy_regs.sources.r2 import upload_comment_partitions

            upload_comment_partitions(args.output_dir, changed)
    else:
        backfill_comments_parquet(
            args.output_dir / "comments.parquet",
            limit=args.limit,
            max_workers=args.max_workers,
            overwrite=args.overwrite,
        )
        if args.upload:
            from spicy_regs.sources.r2 import upload_file

            upload_file(args.output_dir / "comments.parquet")


if __name__ == "__main__":
    main()
