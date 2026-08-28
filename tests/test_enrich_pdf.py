"""Tests for the documents PDF-text enrichment step."""

import json

import duckdb
import polars as pl

from tests.pdf_fixtures import make_pdf, make_textless_pdf

from spicy_regs.enrich_pdf import (
    _enrich_comment_agency_in_catalog,
    enrich_comments_with_pdf_text,
    enrich_documents_with_pdf_text,
    pdf_urls_for_comment,
    pdf_urls_for_document,
)
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg


def test_pdf_urls_prefers_pdf_renditions() -> None:
    attachments = json.dumps(
        [
            {"url": "https://x/doc.htm", "format": "htm"},
            {"url": "https://x/doc.pdf", "format": "pdf"},
        ]
    )
    assert pdf_urls_for_document(attachments, None) == ["https://x/doc.pdf"]


def test_pdf_urls_falls_back_to_file_url() -> None:
    assert pdf_urls_for_document(None, "https://x/content.pdf") == ["https://x/content.pdf"]
    assert pdf_urls_for_document(None, "https://x/content.htm") == []


def test_pdf_urls_dedupes_and_handles_bad_json() -> None:
    assert pdf_urls_for_document("not json", "https://x/a.pdf") == ["https://x/a.pdf"]
    dup = json.dumps([{"url": "https://x/a.pdf", "format": "pdf"}, {"url": "https://x/a.pdf", "format": "pdf"}])
    assert pdf_urls_for_document(dup, None) == ["https://x/a.pdf"]


def _docs_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "document_id": ["D-pdf", "D-scan", "D-none"],
            "attachments_json": [
                json.dumps([{"url": "https://x/good.pdf", "format": "pdf"}]),
                json.dumps([{"url": "https://x/scan.pdf", "format": "pdf"}]),
                None,
            ],
            "file_url": ["https://x/good.pdf", "https://x/scan.pdf", None],
            "text_content": [None, None, None],
            "text_extraction_status": [None, None, None],
        },
        schema={
            "document_id": pl.Utf8,
            "attachments_json": pl.Utf8,
            "file_url": pl.Utf8,
            "text_content": pl.Utf8,
            "text_extraction_status": pl.Utf8,
        },
    )


def test_enrich_fills_text_and_status() -> None:
    pdfs = {
        "https://x/good.pdf": make_pdf(["Important regulatory comment"]),
        "https://x/scan.pdf": make_textless_pdf(),
    }
    enriched, stats = enrich_documents_with_pdf_text(
        _docs_frame(),
        fetch=lambda url: pdfs.get(url),
        max_workers=2,
    )

    by_id = {r["document_id"]: r for r in enriched.iter_rows(named=True)}
    assert by_id["D-pdf"]["text_extraction_status"] == "ok"
    assert "Important regulatory comment" in by_id["D-pdf"]["text_content"]
    assert by_id["D-scan"]["text_extraction_status"] == "empty"
    assert by_id["D-scan"]["text_content"] is None
    # No PDF rendition → never selected, stays untouched.
    assert by_id["D-none"]["text_extraction_status"] is None

    assert stats == {"selected": 2, "ok": 1, "empty": 1, "encrypted": 0, "error": 0}


def test_enrich_records_error_when_fetch_fails() -> None:
    enriched, stats = enrich_documents_with_pdf_text(
        _docs_frame().head(1),
        fetch=lambda url: None,  # download always fails
    )
    row = enriched.row(0, named=True)
    assert row["text_extraction_status"] == "error"
    assert stats["error"] == 1


def test_enrich_skips_already_processed_unless_overwrite() -> None:
    df = _docs_frame().with_columns(
        text_extraction_status=pl.Series(["ok", None, None]),
        text_content=pl.Series(["existing text", None, None]),
    )
    calls: list[str] = []

    def fetch(url: str) -> bytes:
        calls.append(url)
        return make_pdf(["new text"])

    enriched, stats = enrich_documents_with_pdf_text(df, fetch=fetch)
    # D-pdf already has a status, so only D-scan is fetched.
    assert "https://x/good.pdf" not in calls
    assert enriched.row(0, named=True)["text_content"] == "existing text"
    assert stats["selected"] == 1


def test_enrich_overwrite_reprocesses() -> None:
    df = _docs_frame().with_columns(
        text_extraction_status=pl.Series(["ok", "ok", None]),
        text_content=pl.Series(["old", "old", None]),
    )
    enriched, stats = enrich_documents_with_pdf_text(
        df,
        fetch=lambda url: make_pdf(["fresh text"]),
        overwrite=True,
    )
    assert stats["selected"] == 2
    assert enriched.row(0, named=True)["text_content"] == "fresh text"


def test_enrich_respects_limit() -> None:
    _, stats = enrich_documents_with_pdf_text(
        _docs_frame(),
        fetch=lambda url: make_pdf(["t"]),
        limit=1,
    )
    assert stats["selected"] == 1


# --- Comment attachments (nested formats shape) ---------------------------


def test_comment_pdf_urls_reads_nested_formats() -> None:
    # Comment attachments nest renditions under each attachment's "formats".
    attachments = json.dumps(
        [
            {
                "title": "Exhibit A",
                "formats": [
                    {"url": "https://x/a.htm", "format": "htm"},
                    {"url": "https://x/a.pdf", "format": "pdf"},
                ],
            },
            {"title": "Exhibit B", "formats": [{"url": "https://x/b.pdf", "format": "pdf"}]},
        ]
    )
    assert pdf_urls_for_comment(attachments) == ["https://x/a.pdf", "https://x/b.pdf"]


def test_comment_pdf_urls_none_and_bad_json() -> None:
    assert pdf_urls_for_comment(None) == []
    assert pdf_urls_for_comment("not json") == []
    assert pdf_urls_for_comment(json.dumps([{"title": "no formats"}])) == []


def _comments_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "comment_id": ["C-pdf", "C-plain"],
            "attachments_json": [
                json.dumps([{"title": "Letter", "formats": [{"url": "https://x/c.pdf", "format": "pdf"}]}]),
                None,
            ],
            "text_content": [None, None],
            "text_extraction_status": [None, None],
        },
        schema={
            "comment_id": pl.Utf8,
            "attachments_json": pl.Utf8,
            "text_content": pl.Utf8,
            "text_extraction_status": pl.Utf8,
        },
    )


def test_enrich_comments_fills_attachment_text() -> None:
    enriched, stats = enrich_comments_with_pdf_text(
        _comments_frame(),
        fetch=lambda url: make_pdf(["Comment attachment body"]),
    )
    by_id = {r["comment_id"]: r for r in enriched.iter_rows(named=True)}
    assert by_id["C-pdf"]["text_extraction_status"] == "ok"
    assert "Comment attachment body" in by_id["C-pdf"]["text_content"]
    # Comment with no attachment is never selected.
    assert by_id["C-plain"]["text_extraction_status"] is None
    assert stats == {"selected": 1, "ok": 1, "empty": 0, "encrypted": 0, "error": 0}


def _pdf_attach(url: str) -> str:
    return json.dumps([{"title": "Letter", "formats": [{"url": url, "format": "pdf"}]}])


def _seed_catalog(con, rows: list[dict]) -> None:
    """Insert comment rows into the local catalog table (all COMMENT columns)."""
    iceberg._ensure_table(con, COMMENT)
    frame = pl.DataFrame([{**{c: None for c in COMMENT.schema}, **r} for r in rows], schema=COMMENT.schema)
    col_list = ", ".join(f'"{c}"' for c in COMMENT.schema)
    con.register("_seed_src", frame.to_arrow())
    con.execute(f"INSERT INTO {iceberg._qualified(COMMENT)} ({col_list}) SELECT {col_list} FROM _seed_src;")
    con.unregister("_seed_src")


def test_catalog_pdf_enrich_upserts_extracted_text_in_place() -> None:
    # Local DuckDB standing in for the attached R2 catalog (same alias the
    # connector uses), so the DELETE+INSERT upsert runs without network.
    con = duckdb.connect()
    con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS};")
    try:
        _seed_catalog(
            con,
            [
                {"comment_id": "ACF-1", "docket_id": "ACF-2025-0001", "agency_code": "ACF",
                 "attachments_json": _pdf_attach("https://x/c1.pdf"), "comment": "See attached file(s)"},
                # attachment but the download fails → status recorded (error), text stays NULL
                {"comment_id": "ACF-2", "docket_id": "ACF-2025-0001", "agency_code": "ACF",
                 "attachments_json": _pdf_attach("https://x/c2.pdf")},
                # no attachment → never a candidate
                {"comment_id": "ACF-3", "docket_id": "ACF-2025-0001", "agency_code": "ACF",
                 "attachments_json": None},
            ],
        )

        # Deterministic by URL (extraction runs on a thread pool): c1 downloads,
        # c2 is missing from the map → fetch returns None → error status.
        pdfs = {"https://x/c1.pdf": make_pdf(["Comment attachment body"])}
        stats = _enrich_comment_agency_in_catalog(con, COMMENT, "ACF", fetch=lambda url: pdfs.get(url))
        assert stats["selected"] == 2  # two attachment-bearing, no status
        assert stats["ok"] == 1
        assert stats["error"] == 1

        out = dict(
            con.execute(
                f"SELECT comment_id, text_content FROM {iceberg._qualified(COMMENT)} ORDER BY comment_id"
            ).fetchall()
        )
        assert out["ACF-1"] is not None and "Comment attachment body" in out["ACF-1"]
        assert out["ACF-2"] is None  # download failed → text stays NULL (status marks it attempted)
        assert out["ACF-3"] is None  # non-candidate untouched

        # Other columns preserved, no duplication.
        row = con.execute(
            f"SELECT comment, text_extraction_status FROM {iceberg._qualified(COMMENT)} WHERE comment_id = 'ACF-1'"
        ).fetchall()
        assert len(row) == 1
        assert row[0] == ("See attached file(s)", "ok")
        count = con.execute(f"SELECT count(*) FROM {iceberg._qualified(COMMENT)}").fetchone()
        assert count is not None and count[0] == 3

        # Idempotent: every attachment row now carries a status, so nothing is re-selected.
        again = _enrich_comment_agency_in_catalog(con, COMMENT, "ACF", fetch=lambda url: pdfs.get(url))
        assert again == {"selected": 0, "ok": 0, "empty": 0, "encrypted": 0, "error": 0}
    finally:
        con.close()
