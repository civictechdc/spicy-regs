"""The FR↔docket link table normalizes the docket reference it publishes.

The measured examples are the ones in
``docs/corpus-edge-coverage-findings-2026-07-24.md`` §1: on that snapshot the
raw strings matched 1 docket and the label-stripped ones matched 14.
"""

from __future__ import annotations

import json

import pyarrow.parquet as pq

from spicy_regs.ontology.common import write_parquet_rows
from spicy_regs.transforms.build_fr_docket_links import build_fr_docket_links

_FR_COLUMNS = (
    "document_number",
    "title",
    "abstract",
    "document_type",
    "subtype",
    "publication_date",
    "effective_on",
    "comments_close_on",
    "signing_date",
    "agency_slugs",
    "docket_ids_json",
    "regulation_id_numbers_json",
    "html_url",
    "pdf_url",
    "executive_order_number",
)


def _write_federal_register(directory, docket_ids_by_document):
    write_parquet_rows(
        directory / "federal_register.parquet",
        columns=_FR_COLUMNS,
        rows=[
            {
                "document_number": document_number,
                "title": f"Document {document_number}",
                "abstract": None,
                "document_type": "Rule",
                "subtype": None,
                "publication_date": "2026-01-01",
                "effective_on": None,
                "comments_close_on": None,
                "signing_date": None,
                "agency_slugs": "federal-aviation-administration",
                "docket_ids_json": json.dumps(docket_ids),
                "regulation_id_numbers_json": "[]",
                "html_url": None,
                "pdf_url": None,
                "executive_order_number": None,
            }
            for document_number, docket_ids in docket_ids_by_document.items()
        ],
    )


def _links(output_dir):
    table = pq.read_table(output_dir / "fr_docket_links.parquet")
    return [(row["document_number"], row["docket_id"], row["docket_ids_json"]) for row in table.to_pylist()]


def _keys(output_dir):
    table = pq.read_table(output_dir / "fr_docket_links.parquet")
    return {(row["docket_id"], row["docket_key"]) for row in table.to_pylist()}


def test_a_decorated_docket_reference_still_publishes_the_bare_identifier(tmp_path):
    """ "Docket No. FAA-2026-3485" links the same docket "FAA-2026-3485" does."""
    _write_federal_register(
        tmp_path,
        {
            "2026-00001": ["Docket No. FAA-2026-3485"],
            "2026-00002": ["Doc. No. AMS-SC-24-0046"],
            "2026-00003": ["Docket Number USCG-2026-0762"],
            "2026-00004": ["Docket No. OSM-2025-0007"],
            "2026-00005": ["CPSC-2010-0075"],
        },
    )

    build_fr_docket_links(tmp_path)

    assert {docket_id for _, docket_id, _ in _links(tmp_path)} == {
        "FAA-2026-3485",
        "AMS-SC-24-0046",
        "USCG-2026-0762",
        "OSM-2025-0007",
        "CPSC-2010-0075",
    }


def test_a_non_regulations_gov_reference_is_quarantined_not_force_matched(tmp_path):
    """The remaining references name no regs.gov docket, so they stay as stated.

    They are neither dropped (the row survives, inspectable) nor rewritten into
    something that would collide with a real docket id.
    """
    stated = [
        "Special Conditions No. 25-893-SC",
        "Amendment 39-21234",
        "REG-103193-26",
    ]
    _write_federal_register(tmp_path, {"2026-00001": stated})

    build_fr_docket_links(tmp_path)

    assert {docket_id for _, docket_id, _ in _links(tmp_path)} == set(stated)


def test_the_raw_docket_array_is_preserved_beside_the_normalized_key(tmp_path):
    """RULE-010: the normalized identifier never replaces the raw source value."""
    _write_federal_register(tmp_path, {"2026-00001": ["Docket No. FAA-2026-3485"]})

    build_fr_docket_links(tmp_path)

    (_, docket_id, raw), *rest = _links(tmp_path)
    assert not rest
    assert docket_id == "FAA-2026-3485"
    assert json.loads(raw) == ["Docket No. FAA-2026-3485"]


def test_empty_and_label_only_references_are_dropped(tmp_path):
    """A label with no identifier behind it names nothing, so it emits no row.

    "Docket No." is the only kind of unreadable reference that is dropped rather
    than quarantined: a label with nothing behind it is presentation with
    nothing to present, so keeping it would publish a docket key made entirely
    of decoration.
    """
    _write_federal_register(
        tmp_path,
        {"2026-00001": ["", "   ", "Docket No.", "Docket Number", "Doc. No. ", "FAA-2026-3485"]},
    )

    build_fr_docket_links(tmp_path)

    assert [docket_id for _, docket_id, _ in _links(tmp_path)] == ["FAA-2026-3485"]


def test_a_stringified_null_states_no_docket_at_either_call_site(tmp_path):
    """The link table and the RKAF projection clean the same sentinels.

    ``rkaf_projection._clean`` turns "None"/"nan"/"null" into "" before the
    docket grammar sees them. This transform did its own pre-cleaning in SQL
    (``TRIM``), so a stringified null published a docket literally named "NAN".
    One helper now does the cleaning for both readers.
    """
    from spicy_regs.docpipeline.rkaf_projection import _clean
    from spicy_regs.ontology.citations import docket_reference_as_stated

    for sentinel in ("", "   ", "None", "nan", "null"):
        assert docket_reference_as_stated(sentinel) == _clean(sentinel) == ""
    for stated in ("Docket No. FAA-2026-3485", "REG-103193-26"):
        assert docket_reference_as_stated(stated) == _clean(stated) == stated

    _write_federal_register(tmp_path, {"2026-00001": ["nan", "None", "null", "FAA-2026-3485"]})

    build_fr_docket_links(tmp_path)

    assert [docket_id for _, docket_id, _ in _links(tmp_path)] == ["FAA-2026-3485"]


def test_the_table_carries_the_join_key_beside_the_identifier_and_the_raw_array(tmp_path):
    """RULE-010: normalized key, preserved raw value, both published.

    ``tools/build_agency_crosswalk_artifact.py`` derived this key itself on every
    read, recovering 88,073 link rows the raw join dropped
    (docs/corpus-edge-coverage-findings-2026-07-24.md §1, proven in 54f07a6).
    Deriving it here means the table a joiner reads already carries it, and one
    implementation decides what it is.
    """
    _write_federal_register(tmp_path, {"2026-00001": ["Docket No. FAA-2026-3485"]})

    build_fr_docket_links(tmp_path)

    table = pq.read_table(tmp_path / "fr_docket_links.parquet")
    assert "docket_key" in table.column_names
    (row,) = table.to_pylist()
    assert row["docket_key"] == "FAA-2026-3485"
    assert row["docket_id"] == "FAA-2026-3485"
    assert json.loads(row["docket_ids_json"]) == ["Docket No. FAA-2026-3485"]


def test_the_join_key_recovers_what_the_identifier_grammar_refuses(tmp_path):
    """The key is a comparison key, so it forms where an identifier does not.

    A reference the Regulations.gov scheme cannot express keeps its stated value
    in ``docket_id`` — unchanged, quarantined for inspection. Its ``docket_key``
    is still derived, because comparing is not the same act as identifying, and
    a key that matches no docket licenses nothing.
    """
    _write_federal_register(
        tmp_path,
        {
            "2026-00001": [
                "Special Conditions No. 25-893-SC",
                "Docket No. FSIS 2025-0009",
                "  faa - 2026 - 3485  ",
            ]
        },
    )

    build_fr_docket_links(tmp_path)

    assert _keys(tmp_path) == {
        ("Special Conditions No. 25-893-SC", "SPECIALCONDITIONSNO.25-893-SC"),
        ("Docket No. FSIS 2025-0009", "FSIS2025-0009"),
        ("faa - 2026 - 3485", "FAA-2026-3485"),
    }


def test_the_join_key_matches_what_the_crosswalk_derives_for_itself(tmp_path):
    """The column and the tool must agree, or the rebuild changes the artifact.

    ``build_agency_crosswalk_artifact`` keys on ``normalize_docket_id`` applied
    to the emitted ``docket_id``. The column is that same expression, evaluated
    once at build time instead of once per read.
    """
    from spicy_regs.ontology.citations import normalize_docket_id

    stated = [
        "Docket No. FAA-2026-3485",
        "DHS Docket No. USCIS-2025-0004",
        "DOC-2005-0010",
        "MM Docket No. 98-213",
        "REG-103193-26",
    ]
    _write_federal_register(tmp_path, {"2026-00001": stated})

    build_fr_docket_links(tmp_path)

    assert all(key == normalize_docket_id(docket_id) for docket_id, key in _keys(tmp_path))


def test_the_decorated_and_bare_forms_of_one_docket_emit_one_row(tmp_path):
    """Both forms name the same docket, so the exploded table links it once.

    Without DISTINCT the docket page renders the same FR document twice for
    every array that states its docket both ways.
    """
    stated = ["Docket No. FAA-2026-3485", "FAA-2026-3485"]
    _write_federal_register(tmp_path, {"2026-00001": stated})

    build_fr_docket_links(tmp_path)

    (document_number, docket_id, raw), *rest = _links(tmp_path)
    assert not rest
    assert (document_number, docket_id) == ("2026-00001", "FAA-2026-3485")
    assert json.loads(raw) == stated
