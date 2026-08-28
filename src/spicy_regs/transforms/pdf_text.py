"""Pure PDF byte → text extraction.

This is the core of the "extract PDF text" pipeline step (issue #9). It takes
the *bytes* of a PDF and returns the embedded text, page by page. Fetching
those bytes from a URL is a :mod:`spicy_regs.sources` concern, not a transform —
keeping this module pure (bytes in, text out) means it has no network or
filesystem dependency and is trivially testable.

Scope notes (matching the issue):
  * Embedded text only. Scanned/image-only PDFs carry no text layer and come
    back :attr:`PdfTextStatus.EMPTY` — OCR is explicitly out of scope.
  * Basic structure is preserved by joining pages with :data:`PAGE_SEPARATOR`;
    table/column reconstruction is out of scope.
  * Corrupt, truncated, or password-protected PDFs never raise — they return a
    result with a non-OK status so a batch enrichment run can record the
    outcome and move on.
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from enum import Enum

from pypdf import PdfReader
from pypdf.errors import PyPdfError

# Pages are joined with a blank line so the boundary survives in the stored
# text without inventing structure the source PDF didn't have.
PAGE_SEPARATOR = "\n\n"


class PdfTextStatus(str, Enum):
    """Outcome of an extraction attempt.

    ``str`` mixin so the value serialises straight into Parquet/JSON as a
    plain string (``"ok"``, ``"empty"``, ...).
    """

    OK = "ok"
    """Parsed and produced some text."""
    EMPTY = "empty"
    """Parsed fine but no extractable text — almost always a scanned/image PDF."""
    ENCRYPTED = "encrypted"
    """Password-protected and could not be opened with an empty password."""
    ERROR = "error"
    """Not a PDF, truncated, or otherwise unparseable."""


@dataclass(frozen=True)
class PdfTextResult:
    """Result of extracting text from one PDF's bytes."""

    status: PdfTextStatus
    text: str
    page_count: int
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status is PdfTextStatus.OK


def extract_pdf_text(data: bytes) -> PdfTextResult:
    """Extract embedded text from PDF ``data``.

    Never raises: every failure mode is mapped to a :class:`PdfTextResult`
    carrying a non-OK :class:`PdfTextStatus`.
    """
    if not data:
        return PdfTextResult(PdfTextStatus.ERROR, "", 0, error="empty input")

    try:
        reader = PdfReader(io.BytesIO(data))
    except (PyPdfError, OSError, ValueError) as exc:
        return PdfTextResult(PdfTextStatus.ERROR, "", 0, error=str(exc))

    if reader.is_encrypted:
        # Many regulations.gov PDFs are "encrypted" only with an empty owner
        # password (encrypted for permissions, not secrecy); try to open them.
        try:
            if reader.decrypt("") == 0:  # 0 == PasswordType.NOT_DECRYPTED
                return PdfTextResult(PdfTextStatus.ENCRYPTED, "", 0, error="password required")
        except (PyPdfError, NotImplementedError) as exc:
            return PdfTextResult(PdfTextStatus.ENCRYPTED, "", 0, error=str(exc))

    try:
        pages = reader.pages
        page_count = len(pages)
        parts: list[str] = []
        for page in pages:
            # One bad page shouldn't sink the whole document.
            try:
                parts.append(page.extract_text() or "")
            except Exception:  # noqa: BLE001 - pypdf raises a wide variety here
                parts.append("")
    except (PyPdfError, OSError, ValueError) as exc:
        return PdfTextResult(PdfTextStatus.ERROR, "", 0, error=str(exc))

    text = PAGE_SEPARATOR.join(p.strip() for p in parts).strip()
    if not text:
        return PdfTextResult(PdfTextStatus.EMPTY, "", page_count)
    return PdfTextResult(PdfTextStatus.OK, text, page_count)
