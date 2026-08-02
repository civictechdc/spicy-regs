"""The extraction bakeoff's metrics have to mean what the evidence doc says.

Every test here pins one claim the doc makes. The metrics are the argument, so a
metric that quietly changed meaning would invalidate the recommendation without
failing anything else in the tree.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "extraction_bakeoff_worker",
    Path(__file__).resolve().parents[1] / "tools" / "extraction_bakeoff_worker.py",
)
assert _SPEC and _SPEC.loader
worker = importlib.util.module_from_spec(_SPEC)
sys.modules["extraction_bakeoff_worker"] = worker
_SPEC.loader.exec_module(worker)


SOURCE = (
    "<html><body>"
    "<h1>SUMMARY:</h1>"
    "<p>We list the species as &#8203;endangered under &sect;&#8203;4.</p>"
    "<script>var hidden = 'never text';</script>"
    "<p>Second paragraph here.</p>"
    "</body></html>"
)


class TestReference:
    def test_decodes_entities_and_drops_non_content(self) -> None:
        reference = worker.reference_text(SOURCE)
        assert "​" in reference, "the reference decodes numeric character references"
        assert "§" in reference
        assert "never text" not in reference, "script content is not reference text"
        assert "SUMMARY:" in reference

    def test_malformed_markup_yields_empty_rather_than_raising(self) -> None:
        assert worker.reference_text("") == ""


class TestAnchoring:
    def test_source_slices_anchor_perfectly(self) -> None:
        """The property the platform actually depends on."""
        units = [SOURCE[0:30], SOURCE[30:80], SOURCE[80:140]]
        measured = worker.anchoring(units, SOURCE)
        assert measured["unit_exact"] == 1.0
        assert measured["anchor_tok"] == 1.0
        assert measured["anchor_tri"] == 1.0

    def test_entity_decoding_breaks_anchoring(self) -> None:
        """The finding the whole evaluation turns on.

        The decoded text is what a reader sees and what every third-party
        extractor returns. It is not what the source says, so it cannot be found
        in the source.
        """
        decoded = "endangered under §​4."
        assert decoded not in SOURCE
        measured = worker.anchoring([decoded], SOURCE)
        assert measured["unit_exact"] == 0.0

    def test_probes_never_cross_a_unit_boundary(self) -> None:
        """A separator this harness chose must not count against a candidate."""
        units = ["alpha beta gamma", "delta epsilon zeta"]
        joined_source = "alpha beta gamma delta epsilon zeta"
        assert worker.anchoring(units, joined_source)["anchor_tri"] == 1.0

    def test_empty_output_reports_none_not_zero(self) -> None:
        assert worker.anchoring([], SOURCE)["anchor_tok"] is None

    def test_whitespace_only_output_reports_none_not_a_perfect_score(self) -> None:
        """An empty string is a substring of everything; it must not score 1.0."""
        assert worker.anchoring(["", "   "], SOURCE)["unit_exact"] is None

    def test_a_blank_unit_among_real_ones_does_not_count_as_located(self) -> None:
        measured = worker.anchoring(["", "SUMMARY", "not-in-the-source-at-all"], SOURCE)
        assert measured["unit_exact"] == pytest.approx(1 / 3)


class TestLoss:
    def test_dropping_a_section_is_measured_as_deletion(self) -> None:
        reference = "SUMMARY: we list the species DATES: effective today"
        measured = worker.loss("DATES: effective today", reference)
        assert measured["deletion"] == pytest.approx(5 / 8)
        assert measured["insertion"] == 0.0

    def test_invented_text_is_measured_as_insertion(self) -> None:
        measured = worker.loss("| a | b |", "a b")
        assert measured["deletion"] == 0.0
        assert measured["insertion"] > 0.0, "markdown table syntax is text that is not in the source"

    def test_lossless_extraction_scores_zero_both_ways(self) -> None:
        assert worker.loss("a b c", "a b c") == {
            "deletion": 0.0,
            "insertion": 0.0,
            "ref_tokens": 3,
            "out_tokens": 3,
        }


class TestProbePositions:
    def test_stride_is_fixed_so_a_rerun_probes_the_same_places(self) -> None:
        assert list(worker._probe_positions(1000, 10)) == list(worker._probe_positions(1000, 10))

    def test_small_inputs_are_probed_exhaustively(self) -> None:
        assert list(worker._probe_positions(5, 300)) == [0, 1, 2, 3, 4]


class TestIncumbentContract:
    """The incumbent's claim is provable, not merely measured well."""

    def test_native_spans_are_exact_source_slices(self) -> None:
        from spicy_regs.docpipeline.source import native_structural_passage_spans

        spans = native_structural_passage_spans("body", SOURCE, media_type="text/html")
        assert spans, "the fixture has structural markup"
        units = [SOURCE[start:end] for start, end in spans]
        for unit in units:
            assert unit in SOURCE
        assert worker.anchoring(units, SOURCE)["unit_exact"] == 1.0

    def test_script_content_is_not_a_passage(self) -> None:
        from spicy_regs.docpipeline.source import native_structural_passage_spans

        spans = native_structural_passage_spans("body", SOURCE, media_type="text/html")
        assert all("never text" not in SOURCE[start:end] for start, end in spans)
