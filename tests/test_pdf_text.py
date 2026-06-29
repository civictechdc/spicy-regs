"""Tests for the pure PDF text-extraction transform."""

from tests.pdf_fixtures import make_pdf, make_textless_pdf

from spicy_regs.transforms import PdfTextStatus, extract_pdf_text


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
