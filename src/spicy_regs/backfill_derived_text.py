"""Backfill comment ``text_content`` from Mirrulations derived-data extracted text.

The ETL fills ``text_content`` inline for *new* comments
(:class:`~spicy_regs.transforms.enrich_derived_text.EnrichCommentText`), but
every comment published before that wiring still has ``text_content`` NULL. This
is the one-time / re-runnable backfill: it fills ``text_content`` from the
bucket's ``derived-data`` prefix (no PDF download, no JSON re-ingest). By
default, rows are candidates only if they already carry an ``attachments_json``
and have no ``text_extraction_status`` yet, so repeated runs only do new work.

That default has a blind spot: comments ingested before the pipeline started
recording ``attachments_json`` never get a chance, even though Mirrulations may
already have extracted text for their attachments. Pass
``discover_from_derived=True`` (CLI: ``--discover-from-derived``) to select
candidates by ``text_extraction_status`` alone and let the derived-data listing
itself decide whether extracted text exists — this is strictly more expensive
(every docket in scope gets listed, not just ones already known to have
attachments) so scope it with ``--agency``/``--limit``.

There are two write targets, because comments now live in two places:

* **Catalog (``--use-iceberg``)** — the R2 Data Catalog (Iceberg) is the
  system of record. Under the Iceberg pipeline the public mirror is regenerated
  daily from the catalog (see :func:`spicy_regs.sources.iceberg.export_public_comments`),
  so a mirror-only backfill is transient — the next ``publish-comments-mirror``
  run overwrites it. This path upserts the filled rows straight into the catalog
  (one agency at a time, the DELETE+INSERT idiom of ``iceberg._merge``), so the
  fill survives. Run ``publish-comments-mirror`` afterwards to surface it.
* **Published Parquet (default)** — the pre-Iceberg, in-place path: it reads the
  already-published comment Parquet (monolith or the ``agency_code=<X>``
  partition tree) and writes it back. Kept for the legacy layout / local use.
  NB: under the live Iceberg pipeline this is *not durable* — prefer
  ``--use-iceberg``.

It is the derived-data sibling of :mod:`spicy_regs.enrich_pdf`: same incremental,
attachment-only shape, but the text source is Mirrulations' pre-extracted
``.txt`` rather than a downloaded PDF. A wrinkle the PDF path doesn't have: the
Hive comment partitions drop the ``agency_code`` column (it is encoded in the
``agency_code=<X>`` directory name), but the derived-data S3 path is keyed by
agency — so the partition walker (and the catalog path) supply the agency
explicitly; the monolithic ``comments.parquet`` still carries the column per row.
"""

from __future__ import annotations

import argparse
import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import load_dotenv
from loguru import logger

from spicy_regs.schemas import RecordType
from spicy_regs.sources import mirrulations
from spicy_regs.sources.derived_text import DerivedCommentText
from spicy_regs.transforms.pdf_text import PdfTextStatus

ResourceFactory = Callable[[], Any]

_AGENCY_DIR_RE = re.compile(r"agency_code=([^/]+)")


_UPDATES_SCHEMA = {"comment_id": pl.Utf8, "_new_text": pl.Utf8, "_new_status": pl.Utf8}


def _derived_text_updates(
    df: pl.DataFrame,
    *,
    agency: str | None,
    resource_factory: ResourceFactory,
    limit: int | None,
    max_workers: int,
    overwrite: bool,
    discover_from_derived: bool = False,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Fetch derived-data text for a frame's candidate comments.

    Returns ``(updates, stats)`` where ``updates`` is a
    ``(comment_id, _new_text, _new_status)`` frame of the rows that were filled
    (empty when nothing was found). This is the shared core of both write paths:
    the published-Parquet path (:func:`enrich_comments_with_derived_text`) joins
    it back onto the whole frame; the catalog path upserts exactly these rows.

    By default only attachment-bearing comments (``attachments_json`` truthy)
    are candidates. When ``discover_from_derived`` is set, that gate is
    dropped — every comment without a ``text_extraction_status`` is a
    candidate, and the per-docket derived-data listing (via
    :meth:`~spicy_regs.sources.derived_text.DerivedCommentText.text_for`)
    is what actually decides whether extracted text exists. This is how rows
    ingested before ``attachments_json`` was recorded get found at all. Unless
    ``overwrite`` is set, rows that already have a ``text_extraction_status``
    are skipped either way so repeated runs are incremental. ``agency``
    overrides the per-row ``agency_code`` (the Hive partitions don't carry
    that column); when ``None`` it is read from the frame. Work is grouped by
    docket and fanned out across ``max_workers`` so each docket's extraction
    prefix is listed once.
    """
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
        if comment_id is None:
            continue
        if not discover_from_derived and not row["attachments_json"]:
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
    empty = pl.DataFrame(schema=_UPDATES_SCHEMA)
    if not work_by_docket:
        logger.info("No comments to backfill from derived-data")
        return empty, stats

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
        return empty, stats

    updates = pl.DataFrame(
        {"comment_id": ids, "_new_text": texts, "_new_status": [PdfTextStatus.OK.value] * len(ids)},
        schema=_UPDATES_SCHEMA,
    )
    logger.info("Backfill: {} ok, {} missing", stats["ok"], stats["missing"])
    return updates, stats


def enrich_comments_with_derived_text(
    df: pl.DataFrame,
    *,
    agency: str | None = None,
    resource_factory: ResourceFactory = mirrulations.s3_resource,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
    discover_from_derived: bool = False,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Fill ``text_content`` / ``text_extraction_status`` on a comments frame.

    The published-Parquet path: fetch derived text for the frame's candidates
    (:func:`_derived_text_updates`) and join it back onto the whole frame,
    returning the enriched frame and stats. See ``discover_from_derived`` there
    for the opt-in mode that finds legacy rows with no ``attachments_json``.
    """
    for col in ("text_content", "text_extraction_status"):
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    updates, stats = _derived_text_updates(
        df,
        agency=agency,
        resource_factory=resource_factory,
        limit=limit,
        max_workers=max_workers,
        overwrite=overwrite,
        discover_from_derived=discover_from_derived,
    )
    if updates.is_empty():
        return df, stats

    enriched = (
        df.join(updates, on="comment_id", how="left")
        .with_columns(
            text_content=pl.coalesce(["_new_text", "text_content"]),
            text_extraction_status=pl.coalesce(["_new_status", "text_extraction_status"]),
        )
        .drop("_new_text", "_new_status")
    )
    return enriched, stats


def _backfill_file(
    path: Path,
    *,
    agency: str | None,
    resource_factory: ResourceFactory,
    limit: int | None,
    max_workers: int,
    overwrite: bool,
    discover_from_derived: bool = False,
) -> dict[str, int]:
    df = pl.read_parquet(path)
    enriched, stats = enrich_comments_with_derived_text(
        df,
        agency=agency,
        resource_factory=resource_factory,
        limit=limit,
        max_workers=max_workers,
        overwrite=overwrite,
        discover_from_derived=discover_from_derived,
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
    discover_from_derived: bool = False,
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
        discover_from_derived=discover_from_derived,
    )


def backfill_comment_partitions(
    partition_dir: Path,
    *,
    resource_factory: ResourceFactory = mirrulations.s3_resource,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
    discover_from_derived: bool = False,
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
            discover_from_derived=discover_from_derived,
        )
        for key, value in stats.items():
            totals[key] += value
        if stats["ok"]:
            changed.append(part)
        if remaining is not None:
            remaining -= stats["selected"]

    logger.info("Backfill totals: {} (changed {} partitions)", totals, len(changed))
    return totals, changed


def _backfill_agency_in_catalog(
    con,
    record_type: RecordType,
    agency: str,
    *,
    resource_factory: ResourceFactory = mirrulations.s3_resource,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
    discover_from_derived: bool = False,
) -> dict[str, int]:
    """Fill + upsert one agency's candidate comments in the attached catalog.

    Selects the agency's candidate rows from the catalog table (by default,
    attachment-bearing with no ``text_extraction_status`` unless ``overwrite``;
    with ``discover_from_derived`` the ``attachments_json`` requirement is
    dropped so legacy rows ingested before that column existed are candidates
    too — see :func:`_derived_text_updates`), fetches derived text, and upserts
    the filled rows back with the same per-agency DELETE+INSERT idiom as
    :func:`spicy_regs.sources.iceberg._merge` (Iceberg has no ``MERGE INTO`` in
    DuckDB). Scoping every statement to one agency keeps it off the whole
    tens-of-millions-row table. ``con`` is the catalog connection (injected so
    this is testable against a local DuckDB attached under the ``reg_catalog``
    alias). Returns fill stats.
    """
    from spicy_regs.sources import iceberg

    tbl = iceberg._qualified(record_type)
    ag = iceberg._sql_str(agency)
    status_filter = "" if overwrite else "AND (text_extraction_status IS NULL OR text_extraction_status = '')"
    attachment_filter = (
        "" if discover_from_derived else "AND attachments_json IS NOT NULL AND TRIM(attachments_json) NOT IN ('', '[]')"
    )
    limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
    candidates = con.execute(
        f"""
        SELECT comment_id, docket_id, agency_code, attachments_json, text_extraction_status
        FROM {tbl}
        WHERE agency_code = '{ag}'
          {attachment_filter}
          {status_filter}
        {limit_sql}
        """
    ).pl()
    if candidates.is_empty():
        return {"selected": 0, "ok": 0, "missing": 0}

    updates, stats = _derived_text_updates(
        candidates,
        agency=agency,
        resource_factory=resource_factory,
        limit=limit,
        max_workers=max_workers,
        overwrite=overwrite,
        discover_from_derived=discover_from_derived,
    )
    if updates.is_empty():
        return stats

    # Durable per-agency upsert into the catalog. The shared helper builds the
    # replacement rows in a self-contained temp table before the INSERT — a rule
    # that matters on the R2 Data Catalog (see iceberg.upsert_comment_text / PR
    # #117). The derived-text `updates` frame only ever carries non-null
    # _new_text / _new_status (rows are appended only when text was found), so the
    # helper's COALESCE is equivalent to a direct assignment here.
    iceberg.upsert_comment_text(con, record_type, agency, updates)

    logger.info("catalog[{}]: upserted {} filled row(s) ({} missing)", agency, stats["ok"], stats["missing"])
    return stats


def backfill_comments_catalog(
    record_type: RecordType,
    *,
    agencies: list[str] | None = None,
    resource_factory: ResourceFactory = mirrulations.s3_resource,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
    discover_from_derived: bool = False,
) -> dict[str, int]:
    """Backfill ``text_content`` directly in the R2 Data Catalog (durable path).

    Iterates agencies (all in the table, or the given subset) and upserts each
    one's filled rows via :func:`_backfill_agency_in_catalog`, honoring a global
    ``limit`` budget. Writing the catalog — the system of record — means the fill
    survives the daily ``publish-comments-mirror`` regeneration (a mirror-only
    backfill does not). Run ``publish-comments-mirror`` afterwards to surface it
    in the public read files. Returns aggregate stats.
    """
    from spicy_regs.sources import iceberg

    con = iceberg._connect()
    try:
        iceberg._ensure_table(con, record_type)
        tbl = iceberg._qualified(record_type)
        if agencies:
            agency_list = [a.strip().upper() for a in agencies if a.strip()]
        else:
            agency_list = [
                r[0]
                for r in con.execute(
                    f"SELECT DISTINCT agency_code FROM {tbl} WHERE agency_code IS NOT NULL ORDER BY 1"
                ).fetchall()
            ]

        totals = {"selected": 0, "ok": 0, "missing": 0}
        remaining = limit
        for agency in agency_list:
            if remaining is not None and remaining <= 0:
                logger.info("Reached --limit budget; {} agency bucket(s) left unprocessed", len(agency_list))
                break
            stats = _backfill_agency_in_catalog(
                con,
                record_type,
                agency,
                resource_factory=resource_factory,
                limit=remaining,
                max_workers=max_workers,
                overwrite=overwrite,
                discover_from_derived=discover_from_derived,
            )
            for key, value in stats.items():
                totals[key] += value
            if remaining is not None:
                remaining -= stats["selected"]

        logger.info("Catalog backfill totals: {} across {} agency bucket(s)", totals, len(agency_list))
        logger.info("Next: run publish-comments-mirror so the filled text reaches the public read mirror.")
        return totals
    finally:
        con.close()


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Backfill comment text_content from Mirrulations derived-data extracted text."
    )
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--limit", type=int, default=None, help="Max comments to backfill this run")
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--overwrite", action="store_true", help="Re-fill rows that already have a status")
    parser.add_argument(
        "--use-iceberg",
        action="store_true",
        help="Backfill directly in the R2 Data Catalog (durable; survives the mirror republish). "
        "Needs R2_CATALOG_* credentials. Run publish-comments-mirror afterwards.",
    )
    parser.add_argument(
        "--agency",
        default=None,
        help="Comma-separated agency codes to limit the --use-iceberg backfill (default: all)",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Published-Parquet path only: upload changed partitions + index to R2 (needs credentials)",
    )
    parser.add_argument(
        "--discover-from-derived",
        action="store_true",
        help="Find candidates by text_extraction_status alone, dropping the attachments_json "
        "requirement — catches comments ingested before that column was populated. The "
        "derived-data listing itself decides whether extracted text exists. More expensive "
        "(every docket in scope gets listed); scope with --agency/--limit.",
    )
    args = parser.parse_args()

    # Durable path: write the catalog (the system of record the mirror is
    # regenerated from). The published-Parquet paths below are not durable under
    # the live Iceberg pipeline — the next publish-comments-mirror overwrites them.
    if args.use_iceberg:
        from spicy_regs.schemas.regulations import RECORD_TYPES

        agencies = [a for a in args.agency.split(",")] if args.agency else None
        backfill_comments_catalog(
            RECORD_TYPES["comments"],
            agencies=agencies,
            limit=args.limit,
            max_workers=args.max_workers,
            overwrite=args.overwrite,
            discover_from_derived=args.discover_from_derived,
        )
        return

    # Prefer the Hive-partitioned layout, fall back to the monolithic file.
    partition_dir = args.output_dir / "comments" / "agency"
    if partition_dir.exists():
        _, changed = backfill_comment_partitions(
            partition_dir,
            limit=args.limit,
            max_workers=args.max_workers,
            overwrite=args.overwrite,
            discover_from_derived=args.discover_from_derived,
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
            discover_from_derived=args.discover_from_derived,
        )
        if args.upload:
            from spicy_regs.sources.r2 import upload_file

            upload_file(args.output_dir / "comments.parquet")


if __name__ == "__main__":
    main()
