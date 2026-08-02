"""The coverage floor: a parse must justify its own volume, or fail closed.

The exposure this gate closes is measured in
``docs/evidence/extraction-tooling-bakeoff-2026-08-02.md``: Docling's HTML
backend returned 502 visible characters from a 257,998-byte Federal Register
rule — 0.27% of the document — and reported ``ConversionStatus.SUCCESS`` with an
empty error list. Nothing in this pipeline forced that parse to account for the
99.73% it dropped.

Every floor in these tests is derived from the measured retention distribution
recorded in that document, never chosen. The margin tests are the ones that
matter most: a floor with no daylight under the lowest legitimate document is a
future false refusal, and these tests are what stop one being introduced later.
"""

from __future__ import annotations

import pytest

from spicy_regs.docpipeline.source import (
    DENSITY_UNIT,
    RetentionFloor,
    MARKUP_UNIT,
    RETENTION_FLOORS,
    RetentionCheck,
    SourcePolicy,
    SourceRetentionError,
    check_extraction_retention,
    reference_visible_text,
    retention_floor_for,
    visible_retention,
)


def declared_floor(parser_id: str, source_format: str) -> RetentionFloor:
    """The floor, proven present — a missing one is a failure, not a skip."""
    floor = retention_floor_for(parser_id, source_format)
    assert floor is not None, f"{parser_id}:{source_format} must declare a floor"
    return floor


def measured_retention(text: str, *, media_type: str) -> float:
    """The retention, proven measurable — ``None`` here means the fixture is wrong."""
    measured = visible_retention("body", text, media_type=media_type)
    assert measured is not None, "this fixture must have visible text to measure"
    return measured


MARKUP = (
    "<html><body>"
    "<h1>SUMMARY:</h1>"
    "<p>We list the species as &#8203;endangered under &sect;&#8203;4.</p>"
    "<script>var hidden = 'never counted';</script>"
    "<p>Second paragraph here, with enough words to matter.</p>"
    "</body></html>"
)


class TestReferenceVisibleText:
    """The denominator has to be computed without asking the extractor."""

    def test_decodes_entities_and_excludes_non_content(self) -> None:
        reference = reference_visible_text(MARKUP)
        assert "§" in reference, "entity references are decoded before counting"
        assert "never counted" not in reference, "script text is not visible text"
        assert "SUMMARY:" in reference

    def test_text_with_no_markup_is_its_own_reference(self) -> None:
        assert reference_visible_text("plain prose") == "plain prose"

    def test_empty_input_has_no_reference(self) -> None:
        assert reference_visible_text("") == ""


class TestVisibleRetention:
    """Native structural passages keep the document, and the number says so."""

    def test_native_markup_retains_effectively_everything(self) -> None:
        measured = measured_retention(MARKUP, media_type="text/html")
        assert measured > 0.99, f"the native path drops almost nothing, got {measured}"

    def test_retention_is_never_reported_above_one(self) -> None:
        """Nested passages must not be double-counted into a false pass."""
        nested = "<div><section><p>alpha beta gamma delta</p></section></div>"
        assert measured_retention(nested, media_type="text/html") <= 1.0

    def test_a_field_with_no_visible_text_reports_none(self) -> None:
        assert visible_retention("body", "<html><body></body></html>", media_type="text/html") is None


class TestFloorsAreDerivedAndSafe:
    def test_every_floor_is_a_real_threshold(self) -> None:
        """A floor of zero admits everything and is not a gate."""
        assert RETENTION_FLOORS, "the gate needs at least one declared floor"
        for key, floor in RETENTION_FLOORS.items():
            assert 0.0 < floor.value < 1.0, f"{key} declares a floor that gates nothing"
            assert floor.unit in {MARKUP_UNIT, DENSITY_UNIT}
            assert floor.observed_minimum > floor.value, f"{key} has no margin under its observed minimum"

    def test_floors_are_per_format_and_per_parser(self) -> None:
        """One global number would be either useless or wrong."""
        markup = declared_floor("native", "text/html")
        density = declared_floor("pypdf", "application/pdf")
        assert markup.unit != density.unit, "a markup fraction and a text density are not the same measurement"

    def test_an_unknown_parser_and_format_has_no_floor(self) -> None:
        """A new extractor must declare its floor rather than inherit one."""
        assert retention_floor_for("some-future-parser", "text/html") is None


class TestTheGate:
    def test_a_healthy_parse_passes_and_records_its_measurement(self) -> None:
        check = check_extraction_retention("native", "text/html", 0.999, subject_id="fr-doc-1")
        assert isinstance(check, RetentionCheck)
        assert check.passed
        assert check.measured == 0.999
        assert check.unit == MARKUP_UNIT

    def test_a_parse_below_the_floor_fails_closed(self) -> None:
        with pytest.raises(SourceRetentionError):
            check_extraction_retention("native", "text/html", 0.0027, subject_id="fr-doc-1")

    def test_the_refusal_names_the_measurement_the_floor_and_the_format(self) -> None:
        """A failure has to be diagnosable without rerunning the parse."""
        with pytest.raises(SourceRetentionError) as raised:
            check_extraction_retention("native", "text/html", 0.0027, subject_id="fr-doc-1")
        message = str(raised.value)
        assert "0.0027" in message
        assert "text/html" in message
        assert "native" in message
        assert str(declared_floor("native", "text/html").value) in message

    def test_exactly_at_the_floor_passes(self) -> None:
        floor = declared_floor("native", "text/html").value
        assert check_extraction_retention("native", "text/html", floor, subject_id="x").passed

    def test_an_undeclared_parser_cannot_slip_through_ungated(self) -> None:
        with pytest.raises(SourceRetentionError):
            check_extraction_retention("some-future-parser", "text/html", 0.5, subject_id="x")

    def test_a_field_with_no_measurable_text_fails_closed(self) -> None:
        """Unmeasurable is a refusal, not a pass.

        A source with no visible text at all is not an extraction this gate can
        vouch for. The caller must not reach here with an empty field; if it
        does, the gate refuses rather than recording an unearned success.
        """
        with pytest.raises(SourceRetentionError) as raised:
            check_extraction_retention("native", "text/html", None, subject_id="x")
        assert "no measurable visible text" in str(raised.value), (
            "an unmeasurable refusal must read differently from a below-floor refusal"
        )

    def test_the_unmeasurable_refusal_is_distinct_from_a_below_floor_refusal(self) -> None:
        with pytest.raises(SourceRetentionError) as unmeasurable:
            check_extraction_retention("native", "text/html", None, subject_id="x")
        with pytest.raises(SourceRetentionError) as below:
            check_extraction_retention("native", "text/html", 0.01, subject_id="x")
        assert str(unmeasurable.value) != str(below.value)


class TestTheExemptionIsStatedAndReceipted:
    """A legitimate low-retention document gets through by being named, not by luck."""

    def test_a_stated_exemption_admits_the_document_and_records_why(self) -> None:
        policy = SourcePolicy(retention_exemptions=frozenset({"form-only-filing"}))
        check = check_extraction_retention("native", "text/html", 0.01, subject_id="form-only-filing", policy=policy)
        assert check.passed
        assert check.exempt
        assert check.subject_id == "form-only-filing"

    def test_an_exemption_for_another_document_does_not_admit_this_one(self) -> None:
        policy = SourcePolicy(retention_exemptions=frozenset({"some-other-document"}))
        with pytest.raises(SourceRetentionError):
            check_extraction_retention("native", "text/html", 0.01, subject_id="fr-doc-1", policy=policy)

    def test_the_default_policy_exempts_nothing(self) -> None:
        assert SourcePolicy().retention_exemptions == frozenset()


class TestDoclingHtmlRegression:
    """The measured failure that proves the gate works, pinned by name.

    Numbers from ``docs/evidence/extraction-tooling-bakeoff-2026-08-02.md``,
    reproduced on docling 2.115.0 and 2.117.0. They are pinned here rather than
    recomputed so this regression neither imports Docling nor needs the corpus.
    """

    #: name -> (source bytes, reference visible chars, Docling visible chars)
    MEASURED = {
        "docling-html-federal-register-04-28286": (257_998, 187_776, 502),
        "docling-html-federal-register-05-15486": (267_570, 197_787, 1_969),
        "docling-html-federal-register-05-17755": (284_251, 206_297, 1_693),
        "docling-html-federal-register-05-20049": (154_699, 115_281, 1_410),
    }

    @pytest.mark.parametrize("name", sorted(MEASURED))
    def test_the_docling_html_failure_is_refused(self, name: str) -> None:
        _, reference_chars, docling_chars = self.MEASURED[name]
        measured = docling_chars / reference_chars
        with pytest.raises(SourceRetentionError) as raised:
            check_extraction_retention("native", "text/html", measured, subject_id=name)
        assert "text/html" in str(raised.value)

    def test_the_worst_case_retained_under_one_percent(self) -> None:
        """The headline number, so a later change cannot quietly restate it."""
        _, reference_chars, docling_chars = self.MEASURED["docling-html-federal-register-04-28286"]
        assert docling_chars / reference_chars < 0.01

    def test_the_floor_sits_well_clear_of_the_docling_failures(self) -> None:
        floor = declared_floor("native", "text/html")
        worst_docling = max(chars / reference for _, reference, chars in self.MEASURED.values())
        assert worst_docling < floor.value / 2, (
            "the floor must be clearly above the measured failures, not adjacent to them"
        )


class TestTheGateIsWiredAtTheBoundary:
    """The gate has to fire where a representation is sealed, not only in isolation."""

    def test_a_thin_markup_parse_is_refused_by_the_release_pipeline(self) -> None:
        from spicy_regs.document_file_pipeline import (
            DocumentFilePipelineError,
            _refuse_thin_markup_parse,
        )

        # One <main> narrows the passages to it, so everything the document says
        # outside that element is dropped while the reference still counts it.
        # That is the real mechanism by which a parse can retain a sliver of a
        # document and report success — the shape of the Docling failure.
        thin = (
            "<html><body><main><p>the only passage kept</p></main>"
            + "".join(f"<p>substantive paragraph {index} of the rule</p>" for index in range(400))
            + "</body></html>"
        )
        with pytest.raises(DocumentFilePipelineError):
            _refuse_thin_markup_parse(
                thin,
                media_type="text/html",
                source_field="body",
                subject_id="docling-shaped-thin-parse",
            )

    def test_a_healthy_markup_parse_passes_the_boundary(self) -> None:
        from spicy_regs.document_file_pipeline import _refuse_thin_markup_parse

        _refuse_thin_markup_parse(MARKUP, media_type="text/html", source_field="body", subject_id="healthy")

    def test_an_empty_pdf_payload_is_refused_rather_than_divided_by_zero(self) -> None:
        from spicy_regs.document_file_pipeline import (
            DocumentFilePipelineError,
            _refuse_thin_pdf_parse,
        )

        with pytest.raises(DocumentFilePipelineError):
            _refuse_thin_pdf_parse("", b"", subject_id="empty")

    def test_a_pdf_yielding_almost_no_text_is_refused(self) -> None:
        from spicy_regs.document_file_pipeline import (
            DocumentFilePipelineError,
            _refuse_thin_pdf_parse,
        )

        with pytest.raises(DocumentFilePipelineError):
            _refuse_thin_pdf_parse("x" * 10, b"0" * 100_000, subject_id="scanned-no-ocr")

    def test_xml_media_types_share_one_declared_floor(self) -> None:
        """The same document must not be gated differently by header spelling."""
        from spicy_regs.docpipeline.source import retention_format_for

        assert retention_format_for("text/xml") == retention_format_for("application/xml")
        assert retention_format_for("application/rss+xml") == "application/xml"
