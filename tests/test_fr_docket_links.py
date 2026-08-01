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
    return [
        (row["document_number"], row["docket_id"], row["docket_ids_json"])
        for row in table.to_pylist()
    ]


def test_a_decorated_docket_reference_still_publishes_the_bare_identifier(tmp_path):
    """"Docket No. FAA-2026-3485" links the same docket "FAA-2026-3485" does."""
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
    """A label with no identifier behind it names nothing, so it emits no row."""
    _write_federal_register(
        tmp_path,
        {"2026-00001": ["", "   ", "FAA-2026-3485"]},
    )

    build_fr_docket_links(tmp_path)

    assert [docket_id for _, docket_id, _ in _links(tmp_path)] == ["FAA-2026-3485"]
