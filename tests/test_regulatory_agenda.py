"""Agenda-item identity and qualified Proceeding relationship tests."""

from __future__ import annotations

import json

import pyarrow.parquet as pq

from spicy_regs.ontology.common import write_parquet_rows
from spicy_regs.transforms.build_regulatory_agenda import (
    ITEM_COLUMNS,
    RELATIONSHIP_COLUMNS,
    build_regulatory_agenda,
)


def _write(path, columns, rows):
    write_parquet_rows(path, columns=columns, rows=rows)


def test_agenda_items_classify_scope_without_using_rin_as_action_identity(
    tmp_path,
):
    recurring_rin = "2120-AA64"
    ordinary_rin = "2060-AV16"
    ua_only_rin = "2070-AB27"
    dockets = ("FAA-2025-0001", "FAA-2026-0002", "EPA-2026-0003")
    proceedings = ("proceeding_faa_1", "proceeding_faa_2", "proceeding_epa")

    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "modify_date"),
        [
            {
                "docket_id": dockets[0],
                "rin": recurring_rin,
                "modify_date": "2025-01-01",
            },
            {
                "docket_id": dockets[1],
                "rin": recurring_rin,
                "modify_date": "2026-01-01",
            },
            {
                "docket_id": dockets[2],
                "rin": ordinary_rin,
                "modify_date": "2026-02-01",
            },
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        (
            "document_id",
            "docket_id",
            "additional_rins",
            "posted_date",
            "modify_date",
        ),
        [
            {
                "document_id": "EPA-2026-0003-0001",
                "docket_id": dockets[2],
                "additional_rins": f'["{ordinary_rin}"]',
                "posted_date": "2026-02-02",
                "modify_date": "2026-02-03",
            }
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        (
            "document_number",
            "regulation_id_numbers_json",
            "publication_date",
        ),
        [
            {
                "document_number": "2025-00001",
                "regulation_id_numbers_json": f'["{recurring_rin}"]',
                "publication_date": "2025-01-02",
            },
            {
                "document_number": "2026-00002",
                "regulation_id_numbers_json": f'["{recurring_rin}"]',
                "publication_date": "2026-01-02",
            },
        ],
    )
    _write(
        tmp_path / "unified_agenda.parquet",
        (
            "rin",
            "agenda_edition",
            "priority_category",
            "url",
            "first_action_date",
            "next_action_date",
        ),
        [
            {
                "rin": recurring_rin,
                "agenda_edition": "202404",
                "priority_category": "Other Significant",
                "url": "https://example.test/2120-AA64/202404",
            },
            {
                "rin": recurring_rin,
                "agenda_edition": "202510",
                "priority_category": "Routine and Frequent",
                "url": "https://example.test/2120-AA64/202510",
            },
            {
                "rin": ordinary_rin,
                "agenda_edition": "202510",
                "priority_category": "Other Significant",
                "url": "https://example.test/2060-AV16/202510",
            },
            {
                "rin": ua_only_rin,
                "agenda_edition": "202510",
                "priority_category": "Other Significant",
                "url": "https://example.test/2070-AB27/202510",
            },
        ],
    )
    _write(
        tmp_path / "proceedings.parquet",
        (
            "proceeding_id",
            "docket_ids_json",
            "fr_document_numbers_json",
        ),
        [
            {
                "proceeding_id": proceedings[0],
                "docket_ids_json": json.dumps([dockets[0]]),
                "fr_document_numbers_json": '["2025-00001"]',
            },
            {
                "proceeding_id": proceedings[1],
                "docket_ids_json": json.dumps([dockets[1]]),
                "fr_document_numbers_json": '["2026-00002"]',
            },
            {
                "proceeding_id": proceedings[2],
                "docket_ids_json": json.dumps([dockets[2]]),
                "fr_document_numbers_json": "[]",
            },
        ],
    )

    items_file, relationships_file = build_regulatory_agenda(
        tmp_path,
        run_id="agenda-fixture",
        asserted_at="2026-07-24T12:00:00Z",
    )
    items = {row["rin"]: row for row in pq.read_table(items_file).to_pylist()}
    relationships = pq.read_table(relationships_file).to_pylist()

    assert pq.ParquetFile(items_file).schema_arrow.names == list(ITEM_COLUMNS)
    assert pq.ParquetFile(relationships_file).schema_arrow.names == list(RELATIONSHIP_COLUMNS)
    assert items[recurring_rin]["agenda_item_id"] == (f"urn:rkaf:us:rin:{recurring_rin}")
    assert items[recurring_rin]["scope_status"] == "recurring"
    assert items[recurring_rin]["linked_proceeding_count"] == "2"
    assert items[recurring_rin]["observation_count"] == "2"
    assert items[ordinary_rin]["scope_status"] == "single_observed"
    assert items[ua_only_rin]["scope_status"] == "unresolved"
    assert items[ua_only_rin]["scope_basis"] == ("zero_evidence_linked_proceedings")
    assert {row["proceeding_id"] for row in relationships if row["rin"] == recurring_rin} == set(proceedings[:2])
    assert not any(row["rin"] == ua_only_rin for row in relationships)
    assert {row["source"] for row in relationships} == {
        "docket_rin",
        "document_rin",
        "federal_register_rin",
    }
    assert all(row["relationship_role"] == "agenda_tracks_proceeding" for row in relationships)
    assert all(row["method"] == "deterministic" for row in relationships)
