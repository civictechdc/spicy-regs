"""Pure PDF byte → text extraction via PyMuPDF, with per-word coordinates.

A **new parser**, deliberately not an edit of :mod:`spicy_regs.transforms.pdf_text`.
The pypdf extractor stays exactly as it is because representations already sealed
under it must keep verifying against it byte for byte; adopting a better parser
may not retroactively move a single document.

Adopted on the measurement in
``docs/evidence/extraction-tooling-bakeoff-2026-08-02.md``: across 18 real PDFs
PyMuPDF recovered 2,851,308 characters against pypdf's 2,849,368 (+0.07%) at
0.152 s per document against 0.385 (2.5× faster), at comparable memory, and it
carries per-word bounding boxes that pypdf cannot provide at all. It runs
locally, loads no model, touches no network, and costs nothing per document, so
the adoption buys provenance without taking on a doctrinal cost.

The contract is deliberately identical to the pypdf extractor's — same
:class:`PdfTextResult`, same statuses, same never-raises rule — so the two can be
compared on the same axes and a caller can choose between them by name. What is
*not* identical is the identity a receipt records: this module reports
``pymupdf`` and its own imported version, and a representation built from it
carries its own key. See ``document_file_pipeline`` for that boundary.

Licensing note: PyMuPDF is AGPL-3.0 or a commercial Artifex licence. That was
the only thing blocking this adoption and the block was lifted deliberately;
it is recorded here so the constraint is not rediscovered as a surprise.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import Literal

from spicy_regs.transforms.pdf_text import PAGE_SEPARATOR, PdfTextResult, PdfTextStatus

PYMUPDF_EXTRACTION_METHOD = "pymupdf"
"""What a receipt records as the parser. Never ``pypdf``, never inherited."""

PYMUPDF_EXTRACTION_CONFIG = {
    "mode": "embedded-text-only",
    "text_flags": "default",
}
"""The effective configuration, recorded beside the version in every receipt.

``sort=False`` is deliberate and load-bearing: PyMuPDF's natural order is the
document's own content-stream order, and sorting by geometry would re-infer a
reading order the PDF already states. Inferring what is stated is the error this
project measured in other extractors.
"""


@dataclass(frozen=True, slots=True)
class PdfWordBox:
    """One word and the rectangle it occupies, in PDF points, origin top-left.

    This is what the swap actually buys. pypdf returns text and nothing else, so
    a passage extracted from a PDF could never be pointed at a place on a page.
    """

    page: int
    x0: float
    top: float
    x1: float
    bottom: float
    text: str


def pymupdf_version() -> str:
    """The version actually imported, for the receipt — not the one requested."""
    try:
        return version("pymupdf")
    except PackageNotFoundError as error:  # pragma: no cover - environment defect
        raise RuntimeError("pymupdf is required by this extractor") from error


def _open(data: bytes):  # noqa: ANN202 - the provider type never leaves this module
    import pymupdf

    return pymupdf.open(stream=data, filetype="pdf")


def extract_pdf_text_pymupdf(
    data: bytes,
    *,
    page_separator: str = PAGE_SEPARATOR,
    page_whitespace: Literal["preserve", "strip"] = "strip",
) -> PdfTextResult:
    """Extract embedded text from PDF ``data`` with PyMuPDF.

    Never raises on a bad document: every failure mode becomes a
    :class:`PdfTextResult` with a non-OK status, exactly as the pypdf extractor
    does, so a batch run records the outcome and moves on. Argument validation
    *does* raise, also as the incumbent does — a caller passing an empty
    separator has a bug, not a bad PDF.
    """
    if not isinstance(page_separator, str) or not page_separator:
        raise ValueError("page_separator must be a non-empty string")
    if page_whitespace not in {"preserve", "strip"}:
        raise ValueError("page_whitespace must be 'preserve' or 'strip'")

    try:
        document = _open(data)
    except Exception as error:  # noqa: BLE001 - a document this cannot open is a status, not a crash
        return PdfTextResult(status=PdfTextStatus.ERROR, text="", page_count=0, error=str(error)[:200])

    try:
        if document.needs_pass:
            return PdfTextResult(status=PdfTextStatus.ENCRYPTED, text="", page_count=0, error="password protected")
        pages: list[str] = []
        failed: list[int] = []
        for ordinal, page in enumerate(document):
            try:
                extracted = page.get_text(sort=False)
            except Exception:  # noqa: BLE001 - one unreadable page is recorded, not fatal
                failed.append(ordinal)
                extracted = ""
            pages.append(extracted.strip() if page_whitespace == "strip" else extracted)
        page_count = document.page_count
    finally:
        document.close()

    text = page_separator.join(pages)
    if not text.strip():
        return PdfTextResult(
            status=PdfTextStatus.EMPTY,
            text=text,
            page_count=page_count,
            pages=tuple(pages),
            failed_page_ordinals=tuple(failed),
        )
    return PdfTextResult(
        status=PdfTextStatus.OK,
        text=text,
        page_count=page_count,
        pages=tuple(pages),
        failed_page_ordinals=tuple(failed),
    )


def extract_pdf_word_boxes(data: bytes) -> tuple[PdfWordBox, ...]:
    """Every word and its rectangle, in the document's own order.

    Returned as this project's own immutable records: no PyMuPDF type crosses
    this boundary, for the same reason no Docling type crosses the Office
    adapter's.
    """
    try:
        document = _open(data)
    except Exception:  # noqa: BLE001 - an unopenable document has no boxes
        return ()
    boxes: list[PdfWordBox] = []
    try:
        for ordinal, page in enumerate(document):
            try:
                words = page.get_text("words", sort=False)
            except Exception:  # noqa: BLE001 - one unreadable page contributes nothing
                continue
            for x0, top, x1, bottom, word, *_ in words:
                boxes.append(
                    PdfWordBox(
                        page=ordinal,
                        x0=float(x0),
                        top=float(top),
                        x1=float(x1),
                        bottom=float(bottom),
                        text=str(word),
                    )
                )
    finally:
        document.close()
    return tuple(boxes)
