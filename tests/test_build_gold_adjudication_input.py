"""Tests for the blind gold-adjudication input builder.

Two properties carry the phase 1.1 protocol and are asserted here on a tiny
synthetic fixture, with no file or provider access:

* **blindness** — a tagger-output column present in the input data must not
  reach the emitted document, by key or by value;
* **selector parity** — candidates come from the production selector itself,
  asserted by import identity rather than by comparing behaviour to a copy.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from spicy_regs.docpipeline.extraction import ExtractionUnit
from spicy_regs.ontology import concepts as ontology_concepts
from spicy_regs import rulespec_testbed

REPO_ROOT = Path(__file__).resolve().parents[1]
BUILDER_PATH = REPO_ROOT / "tools" / "build_gold_adjudication_input.py"

# Sentinel values planted on every input structure. None may appear in output.
TAGGER_KEY = "tagger_predicted_concept_id"
TAGGER_VALUE = "TAGGER-LEAK-SENTINEL"
TAGGER_KEYS = (TAGGER_KEY, "tagger_confidence", "tagger_assignment_id", "predicted_label")


def _load_builder():
    spec = importlib.util.spec_from_file_location("build_gold_adjudication_input", BUILDER_PATH)
    assert spec and spec.loader, f"could not load {BUILDER_PATH}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def builder():
    return _load_builder()


def _registry_row(concept_id: str, pref_label: str, alt: list[str], definition: str) -> dict:
    return {
        "concept_id": concept_id,
        "facet": "subject",
        "source_vocabulary": "spicy-regs-local",
        "scheme": "subject",
        "pref_label": pref_label,
        "alt_labels_json": json.dumps(alt),
        "definition": definition,
        "broader_id": None,
        "status": "active",
        "replaced_by": None,
        "external_ids_json": "[]",
        # Attestation and (planted) tagger columns ride along on the row the
        # selector returns, exactly as a real table would carry extra columns.
        "actor_id": "openai:gpt-5.6-sol",
        "run_id": "some-diagnostic-run",
        "method": "llm",
        "asserted_at": "2026-07-26T00:00:00Z",
        TAGGER_KEY: TAGGER_VALUE,
        "tagger_confidence": TAGGER_VALUE,
    }


@pytest.fixture
def registry_rows():
    return [
        _registry_row("concept_aaa", "steep-slope mining", ["steep slope mining"], "Mining on steep slopes."),
        _registry_row("concept_bbb", "surface mining", [], "Mining at the surface."),
        _registry_row("concept_ccc", "aviation safety", [], "Safety of aircraft operations."),
        _registry_row("concept_ddd", "postal service", [], "Delivery of mail."),
    ]


@pytest.fixture
def units():
    return [
        ExtractionUnit(
            unit_id="processing_segment_synthetic_one",
            input={
                "subject": {
                    "type": "cfr_section",
                    "id": "SYNTHETIC-1",
                    "profile": "cfr-section-v1",
                    "source_table": "cfr_sections",
                    "allowed_schemes": ["subject"],
                    "artifact_digest": "digest-one",
                    TAGGER_KEY: TAGGER_VALUE,
                },
                "processing_segment": {
                    "segment_id": "processing_segment_synthetic_one",
                    "ordinal": 0,
                    "segment_count": 1,
                    "policy": "structure-overlap-v1",
                    "tagger_assignment_id": TAGGER_VALUE,
                },
                "non_evidentiary_context": {
                    "artifact_context": {"artifact_title": "Steep-slope mining performance standards"},
                    "headings": ["Performance standards"],
                },
                "untrusted_evidence_fields": {
                    "fields": {"evidence_0": "Steep-slope mining operations must control surface runoff."},
                },
                "available_concepts": [
                    {"concept_id": "concept_aaa"},
                    {"concept_id": "concept_bbb"},
                    {"concept_id": "concept_ccc"},
                    {"concept_id": "concept_ddd"},
                ],
                "tagger_output": [{"concept_id": TAGGER_VALUE, "predicted_label": TAGGER_VALUE}],
            },
        )
    ]


@pytest.fixture
def answers():
    return {
        "artifacts": [
            {
                "profile_id": "cfr-section-v1",
                "subject_type": "cfr_section",
                "subject_id": "SYNTHETIC-1",
                "artifact_digest": "digest-one",
                "expected_tags": [
                    {
                        "gold_id": "gold_synthetic_one",
                        "scheme": "subject",
                        "label": "steep-slope mining",
                        "concept_id": "concept_aaa",
                        "source_field": "cfr_sections.xml_text",
                        "start_char": 0,
                        "end_char": 18,
                        "exact_text": "Steep-slope mining",
                        "coordinate_resolution": "provided-offsets",
                        "containing_segment_ids": ["processing_segment_synthetic_one"],
                        TAGGER_KEY: TAGGER_VALUE,
                        "predicted_label": TAGGER_VALUE,
                    }
                ],
                "tagger_assignment_id": TAGGER_VALUE,
            }
        ],
        "segments": [{"segment_id": "processing_segment_synthetic_one", "tagger_confidence": TAGGER_VALUE}],
    }


def _build(builder, *, answers, units, registry_rows, limit=4):
    return builder.build_document(
        answers=answers,
        units=units,
        registry_rows=registry_rows,
        file_metadata={"registry_row_count": len(registry_rows)},
        generated_at="2026-07-27T00:00:00+00:00",
        limit=limit,
    )


def test_output_contains_no_tagger_output(builder, answers, units, registry_rows):
    document = _build(builder, answers=answers, units=units, registry_rows=registry_rows)
    serialized = json.dumps(document, sort_keys=True)
    assert TAGGER_VALUE not in serialized
    for key in TAGGER_KEYS:
        assert key not in serialized
    assert "tagger_output" not in serialized
    # The blindness claim is stated in the file itself.
    assert document["blind"].startswith("blind: contains no tagger output")


def test_registry_attestation_columns_do_not_leak_into_candidates(builder, answers, units, registry_rows):
    document = _build(builder, answers=answers, units=units, registry_rows=registry_rows)
    candidate = document["items"][0]["candidates"][0]
    assert set(candidate) == {
        "concept_id",
        "facet",
        "source_vocabulary",
        "scheme",
        "pref_label",
        "alt_labels",
        "definition",
        "broader_id",
        "status",
        "from_segments",
        "rank",
    }


def test_module_binds_the_production_selector_by_identity(builder):
    assert builder.SELECT_CANDIDATES is ontology_concepts.select_candidate_concepts_for_text
    assert builder.PROMPT_CONCEPT_LIMIT is rulespec_testbed.PROMPT_CONCEPT_LIMIT
    assert builder.PROMPT_CONCEPT_LIMIT == 12


def test_candidates_come_from_the_bound_selector(builder, answers, units, registry_rows, monkeypatch):
    """Replacing the module's selector binding changes the output, so the real
    selector is on the live path rather than shadowed by a local copy."""
    calls: list[tuple] = []

    def spy(text, allowed_schemes, concepts, *, limit):
        calls.append((text, tuple(allowed_schemes), tuple(str(c["concept_id"]) for c in concepts), limit))
        return list(concepts)[:1]

    monkeypatch.setattr(builder, "SELECT_CANDIDATES", spy)
    document = _build(builder, answers=answers, units=units, registry_rows=registry_rows)

    assert len(calls) == 1
    text, schemes, concept_ids, limit = calls[0]
    assert text == "Steep-slope mining operations must control surface runoff."
    assert schemes == ("subject",)
    assert concept_ids == ("concept_aaa", "concept_bbb", "concept_ccc", "concept_ddd")
    assert limit == 4
    assert [item["concept_id"] for item in document["items"][0]["candidates"]] == ["concept_aaa"]


def test_real_selector_reproduces_the_payload_candidate_order(builder, answers, units, registry_rows):
    document = _build(builder, answers=answers, units=units, registry_rows=registry_rows)
    assert document["candidate_selector"]["payload_parity_mismatches"] == []
    assert document["candidate_selector"]["payload_parity_checked"] == 1
    assert document["items"][0]["segment_context"][0]["selector_matches_payload"] is True


def test_payload_parity_mismatch_is_reported_not_hidden(builder, answers, units, registry_rows):
    stale = ExtractionUnit(
        unit_id=units[0].unit_id,
        input={**units[0].input, "available_concepts": [{"concept_id": "concept_zzz"}]},
    )
    document = _build(builder, answers=answers, units=[stale], registry_rows=registry_rows)
    assert document["candidate_selector"]["payload_parity_mismatches"] == [stale.unit_id]
    assert document["items"][0]["segment_context"][0]["selector_matches_payload"] is False


def test_gold_evidence_and_identity_survive_verbatim(builder, answers, units, registry_rows):
    item = _build(builder, answers=answers, units=units, registry_rows=registry_rows)["items"][0]
    assert item["item_id"] == "gold-adjudication-gold_synthetic_one"
    assert item["artifact"] == {
        "profile_id": "cfr-section-v1",
        "subject_type": "cfr_section",
        "subject_id": "SYNTHETIC-1",
        "artifact_digest": "digest-one",
        "title": "Steep-slope mining performance standards",
    }
    assert item["gold_concept"] == {
        "scheme": "subject",
        "label": "steep-slope mining",
        "registered_concept_id": "concept_aaa",
    }
    assert item["gold_evidence"] == [
        {
            "source_field": "cfr_sections.xml_text",
            "start_char": 0,
            "end_char": 18,
            "exact_text": "Steep-slope mining",
            "coordinate_resolution": "provided-offsets",
        }
    ]


def test_missing_segment_unit_is_reported_not_guessed(builder, answers, units, registry_rows):
    answers["artifacts"][0]["expected_tags"][0]["containing_segment_ids"] = ["processing_segment_absent"]
    document = _build(builder, answers=answers, units=units, registry_rows=registry_rows)
    assert document["unbuilt_item_count"] == 1
    assert document["unbuilt_items"][0]["gold_id"] == "gold_synthetic_one"
    assert "processing_segment_absent" in document["unbuilt_items"][0]["reason"]
    assert document["items"][0]["candidates"] == []


def test_multi_segment_gold_takes_a_capped_union_with_provenance(builder, answers, registry_rows):
    def unit(unit_id: str, text: str) -> ExtractionUnit:
        return ExtractionUnit(
            unit_id=unit_id,
            input={
                "subject": {"allowed_schemes": ["subject"], "id": "SYNTHETIC-1"},
                "processing_segment": {"segment_id": unit_id, "ordinal": 0, "segment_count": 2},
                "non_evidentiary_context": {"artifact_context": {"artifact_title": "T"}, "headings": []},
                "untrusted_evidence_fields": {"fields": {"evidence_0": text}},
                "available_concepts": [],
            },
        )

    units = [
        unit("segment_a", "Steep-slope mining operations."),
        unit("segment_b", "Aviation safety inspections."),
    ]
    answers["artifacts"][0]["expected_tags"][0]["containing_segment_ids"] = ["segment_a", "segment_b"]
    document = _build(builder, answers=answers, units=units, registry_rows=registry_rows, limit=2)

    candidates = document["items"][0]["candidates"]
    assert len(candidates) == 2
    assert [item["rank"] for item in candidates] == [1, 2]
    sources = {item["concept_id"]: item["from_segments"] for item in candidates}
    assert all(source for source in sources.values())
    assert {entry["segment_id"] for entries in sources.values() for entry in entries} <= {"segment_a", "segment_b"}
    assert len(document["items"][0]["segment_context"]) == 2


def test_alt_labels_are_parsed_from_the_registry_json_column(builder, answers, units, registry_rows):
    document = _build(builder, answers=answers, units=units, registry_rows=registry_rows)
    by_id = {item["concept_id"]: item for item in document["items"][0]["candidates"]}
    assert by_id["concept_aaa"]["alt_labels"] == ["steep slope mining"]
    assert by_id["concept_bbb"]["alt_labels"] == []
    assert by_id["concept_aaa"]["definition"] == "Mining on steep slopes."
