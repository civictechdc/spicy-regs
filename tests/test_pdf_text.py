"""Tests for the pure PDF text-extraction transform."""

from pathlib import Path

import pytest

from tests.pdf_fixtures import make_pdf, make_textless_pdf

from spicy_regs.transforms import PdfTextStatus, extract_pdf_text

SAMPLE_DATA = Path(__file__).resolve().parents[1] / "sample-data" / "mirrulations"


def test_extracts_text_from_single_page() -> None:
    result = extract_pdf_text(make_pdf(["Hello PDF world"]))
    assert result.status is PdfTextStatus.OK
    assert result.ok
    assert result.page_count == 1
    assert "Hello PDF world" in result.text


def test_preserves_page_boundaries() -> None:
    result = extract_pdf_text(make_pdf(["First page", "Second page"]))
    assert result.status is PdfTextStatus.OK
    assert result.page_count == 2
    assert "First page" in result.text
    assert "Second page" in result.text
    # The two pages are separated by a blank line, not run together.
    assert "\n\n" in result.text


def test_textless_pdf_is_empty_not_error() -> None:
    # Image-only / scanned PDFs parse fine but carry no text layer (OCR is
    # out of scope) — they must come back EMPTY, not ERROR.
    result = extract_pdf_text(make_textless_pdf())
    assert result.status is PdfTextStatus.EMPTY
    assert result.text == ""
    assert not result.ok


def test_corrupt_bytes_return_error() -> None:
    result = extract_pdf_text(b"%PDF-1.4 this is not really a pdf")
    assert result.status is PdfTextStatus.ERROR
    assert result.error is not None


def test_empty_input_returns_error() -> None:
    result = extract_pdf_text(b"")
    assert result.status is PdfTextStatus.ERROR


def test_non_pdf_bytes_return_error() -> None:
    result = extract_pdf_text(b"just some plain text, no PDF header at all")
    assert result.status is PdfTextStatus.ERROR


@pytest.mark.parametrize(
    ("filename", "needle"),
    [
        # A real comment attachment (Wisconsin DCF comment letter).
        ("comment-ACF-2025-0038-0004_attachment_1.pdf", "Indian Child Welfare Act"),
        # A real document content PDF (Federal Register notice).
        ("document-ACF-2025-0038-0001_content.pdf", "Federal Register"),
    ],
)
def test_extracts_real_regulations_gov_pdfs(filename: str, needle: str) -> None:
    path = SAMPLE_DATA / filename
    if not path.exists():
        pytest.skip(f"sample PDF {filename} not present")
    result = extract_pdf_text(path.read_bytes())
    assert result.status is PdfTextStatus.OK
    assert result.page_count >= 1
    assert needle in result.text
