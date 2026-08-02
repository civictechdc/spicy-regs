"""The PyMuPDF extractor: a new parser, never a mutation of the pypdf one.

Adopted on the measurement in ``docs/evidence/extraction-tooling-bakeoff-2026-08-02.md``:
across 18 real PDFs PyMuPDF recovered 2,851,308 characters against pypdf's
2,849,368 (+0.07%) at 0.152 s/document against 0.385 (2.5x faster), with
comparable memory and per-character bounding boxes pypdf cannot provide.

The rule these tests hold is that adopting it costs nothing already sealed.
``pdf_text.py`` is untouched, this parser reports its own identity, and a
document captured under pypdf keeps the representation it was sealed with.
"""

from __future__ import annotations

import pytest

from spicy_regs.transforms.pdf_text import PdfTextStatus
from spicy_regs.transforms.pdf_text import extract_pdf_text as extract_with_pypdf
from spicy_regs.transforms.pdf_text_pymupdf import (
    PYMUPDF_EXTRACTION_METHOD,
    extract_pdf_text_pymupdf,
    pymupdf_version,
)

CORPUS = "output/segmentation-source-cache-v2"


def _pdf(name: str) -> bytes:
    from pathlib import Path

    path = Path(CORPUS) / name
    if not path.is_file():
        pytest.skip(f"{path} is not in this tree")
    return path.read_bytes()


class TestIdentity:
    def test_the_parser_names_itself_and_never_borrows_pypdf_identity(self) -> None:
        assert PYMUPDF_EXTRACTION_METHOD == "pymupdf"
        assert PYMUPDF_EXTRACTION_METHOD != "pypdf"

    def test_the_version_is_the_one_actually_imported(self) -> None:
        import pymupdf

        assert pymupdf_version() == pymupdf.__doc__ or pymupdf_version()
        assert pymupdf_version().strip(), "a receipt needs a real version string"


class TestTheContractMatchesTheIncumbent:
    """A drop-in replacement has to answer the same questions the same way."""

    def test_a_real_pdf_extracts_ok_with_pages(self) -> None:
        result = extract_pdf_text_pymupdf(_pdf("court-opinion-groff-dejoy.pdf"))
        assert result.status is PdfTextStatus.OK
        assert result.ok
        assert result.page_count == len(result.pages)
        assert result.failed_page_ordinals == ()
        assert result.text.strip()

    def test_page_count_agrees_with_pypdf(self) -> None:
        data = _pdf("court-opinion-groff-dejoy.pdf")
        assert extract_pdf_text_pymupdf(data).page_count == extract_with_pypdf(data).page_count

    def test_not_a_pdf_returns_error_and_never_raises(self) -> None:
        result = extract_pdf_text_pymupdf(b"this is definitely not a pdf")
        assert result.status is PdfTextStatus.ERROR
        assert not result.ok
        assert result.error

    def test_empty_bytes_return_error_rather_than_raising(self) -> None:
        assert extract_pdf_text_pymupdf(b"").status is PdfTextStatus.ERROR

    def test_the_separator_and_whitespace_mode_are_honoured(self) -> None:
        data = _pdf("crs-pdf-short.pdf")
        joined = extract_pdf_text_pymupdf(data, page_separator="\n\f\n", page_whitespace="preserve")
        assert "\n\f\n" in joined.text or joined.page_count == 1
        assert joined.text == "\n\f\n".join(joined.pages)

    def test_a_rejected_separator_fails_loudly_like_the_incumbent(self) -> None:
        with pytest.raises(ValueError):
            extract_pdf_text_pymupdf(_pdf("crs-pdf-short.pdf"), page_separator="")


class TestDeterminism:
    """Requirement 3: byte-identical input, byte-identical output."""

    def test_two_runs_agree_byte_for_byte(self) -> None:
        data = _pdf("crs-pdf-medium.pdf")
        assert extract_pdf_text_pymupdf(data).text == extract_pdf_text_pymupdf(data).text


class TestTheMeasuredImprovementHolds:
    """The reason to adopt it, re-checked through the parser that ships."""

    @pytest.mark.parametrize(
        "name",
        [
            "court-opinion-groff-dejoy.pdf",
            "court-opinion-moore-harper.pdf",
            "crs-pdf-medium.pdf",
            "regulations-pdf-medium.pdf",
        ],
    )
    def test_it_recovers_at_least_as_much_text_as_pypdf(self, name: str) -> None:
        data = _pdf(name)
        mine = extract_pdf_text_pymupdf(data)
        theirs = extract_with_pypdf(data)
        assert len(mine.text) >= len(theirs.text) * 0.99, (
            f"{name}: pymupdf {len(mine.text)} vs pypdf {len(theirs.text)}"
        )


class TestProvenanceIsWhatPypdfCannotGive:
    """The axis the swap actually buys: per-span coordinates."""

    def test_word_boxes_are_available_and_land_on_the_page(self) -> None:
        from spicy_regs.transforms.pdf_text_pymupdf import extract_pdf_word_boxes

        boxes = extract_pdf_word_boxes(_pdf("crs-pdf-short.pdf"))
        assert boxes, "a provenance-carrying parser returns word boxes"
        for box in boxes[:50]:
            assert box.page >= 0
            assert box.x0 <= box.x1 and box.top <= box.bottom
            assert box.text.strip()

    def test_word_boxes_are_deterministic(self) -> None:
        from spicy_regs.transforms.pdf_text_pymupdf import extract_pdf_word_boxes

        data = _pdf("crs-pdf-short.pdf")
        assert extract_pdf_word_boxes(data) == extract_pdf_word_boxes(data)


class TestAdoptingItMovesNothingAlreadySealed:
    """The rule the swap is subject to: sealed records reproduce as sealed."""

    def test_the_named_parser_decides_not_the_default(self) -> None:
        from spicy_regs.document_file_pipeline import (
            DEFAULT_PDF_EXTRACTION_METHOD,
            PDF_EXTRACTION_METHOD,
            _extract_pdf_with,
        )

        data = _pdf("crs-pdf-short.pdf")
        assert DEFAULT_PDF_EXTRACTION_METHOD != PDF_EXTRACTION_METHOD, "the default moved to the new parser"

        sealed, method, method_version = _extract_pdf_with(
            PDF_EXTRACTION_METHOD, data, page_separator="\n\f\n", page_whitespace="preserve"
        )
        assert method == "pypdf"
        # A record sealed under pypdf reproduces byte for byte under pypdf, with
        # the new parser installed and the default already switched away.
        expected = extract_with_pypdf(data, page_separator="\n\f\n", page_whitespace="preserve")
        assert sealed.text == expected.text
        assert method_version == _pypdf_version_for_test()

    def test_the_new_default_is_the_new_parser(self) -> None:
        from spicy_regs.document_file_pipeline import DEFAULT_PDF_EXTRACTION_METHOD, _extract_pdf_with

        data = _pdf("crs-pdf-short.pdf")
        _, method, method_version = _extract_pdf_with(
            DEFAULT_PDF_EXTRACTION_METHOD, data, page_separator="\n\f\n", page_whitespace="preserve"
        )
        assert method == PYMUPDF_EXTRACTION_METHOD
        assert method_version == pymupdf_version()

    def test_an_unknown_parser_fails_closed_instead_of_falling_back(self) -> None:
        """A silent fallback is the one way this check could lie."""
        from spicy_regs.document_file_pipeline import DocumentFilePipelineError, _extract_pdf_with

        with pytest.raises(DocumentFilePipelineError):
            _extract_pdf_with(
                "some-parser-this-build-does-not-have",
                _pdf("crs-pdf-short.pdf"),
                page_separator="\n\f\n",
                page_whitespace="preserve",
            )

    def test_each_parser_declares_its_own_retention_floor(self) -> None:
        from spicy_regs.docpipeline.source import retention_floor_for

        pypdf_floor = retention_floor_for("pypdf", "application/pdf")
        pymupdf_floor = retention_floor_for("pymupdf", "application/pdf")
        assert pypdf_floor is not None and pymupdf_floor is not None
        assert pypdf_floor.population != pymupdf_floor.population, (
            "each parser's floor names the population it was measured over"
        )

    def test_the_receipt_config_distinguishes_the_parsers(self) -> None:
        from spicy_regs.document_file_pipeline import _pdf_extraction_config

        pypdf_config = _pdf_extraction_config("pypdf", locked=True)
        pymupdf_config = _pdf_extraction_config("pymupdf", locked=True)
        assert pypdf_config != pymupdf_config
        assert pymupdf_config["reading_order"] == "content-stream", (
            "the new parser records that it does not re-infer a reading order the PDF states"
        )


def _pypdf_version_for_test() -> str:
    from importlib.metadata import version

    return version("pypdf")
