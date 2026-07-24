"""Golden-file-style fixture test for every rule-target evidence source."""

from __future__ import annotations

import pyarrow.parquet as pq

from spicy_regs.ontology.common import write_parquet_rows
from spicy_regs.transforms.build_rule_targets import COLUMNS, build_rule_targets


def _write(path, columns, rows):
    write_parquet_rows(path, columns=columns, rows=rows)


def test_rule_target_spine_emits_known_edges_from_every_source(tmp_path):
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "modify_date"),
        [{"docket_id": "EPA-HQ-OAR-2024-0001", "rin": "2060-AV12", "modify_date": "2024-01-10"}],
    )
    _write(
        tmp_path / "documents.parquet",
        ("document_id", "docket_id", "additional_rins", "fr_doc_num", "posted_date", "modify_date"),
        [
            {
                "document_id": "EPA-HQ-OAR-2024-0001-0001",
                "docket_id": "EPA-HQ-OAR-2024-0001",
                "additional_rins": '["2060-AV13"]',
                "fr_doc_num": "2024-00001",
                "posted_date": "2024-02-01",
                "modify_date": "2024-02-02",
            }
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        (
            "document_number",
            "cfr_references_json",
            "regulation_id_numbers_json",
            "publication_date",
        ),
        [
            {
                "document_number": "2024-00001",
                "cfr_references_json": '[{"title": 40, "part": 60}]',
                "regulation_id_numbers_json": '["2060-AV12"]',
                "publication_date": "2024-02-01",
            }
        ],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "docket_id"),
        [{"document_number": "2024-00001", "docket_id": "EPA-HQ-OAR-2024-0001"}],
    )
    _write(
        tmp_path / "unified_agenda.parquet",
        (
            "rin",
            "agenda_edition",
            "cfr_references_json",
            "first_action_date",
            "next_action_date",
        ),
        [
            {
                "rin": "2060-AV12",
                "agenda_edition": "202404",
                "cfr_references_json": '["40 CFR 63"]',
                "first_action_date": "2024-03-01",
                "next_action_date": "2024-05-01",
            },
            {
                "rin": "2060-AV13",
                "agenda_edition": "202404",
                "cfr_references_json": '["40 CFR 64"]',
                "first_action_date": "2024-03-02",
                "next_action_date": None,
            },
        ],
    )

    output = build_rule_targets(
        tmp_path,
        run_id="golden-run",
        asserted_at="2026-07-23T12:00:00Z",
    )
    rows = pq.read_table(output).to_pylist()
    observed = {(row["source"], row["cfr_ref"], row["rin"]) for row in rows}
    assert observed == {
        ("docket_rin", None, "2060-AV12"),
        ("document_rin", None, "2060-AV13"),
        ("fr_cfr_ref", "40-60", "2060-AV12"),
        ("document_fr_doc", "40-60", "2060-AV12"),
        ("ua_cfr_ref", "40-63", "2060-AV12"),
        ("ua_cfr_ref", "40-64", "2060-AV13"),
    }
    assert pq.ParquetFile(output).schema_arrow.names == list(COLUMNS)
    assert all(row["method"] == "deterministic" for row in rows)
    assert all(row["actor_id"] == "spicy-regs:rule-targets:v1" for row in rows)
    assert all(row["run_id"] == "golden-run" for row in rows)


def test_rule_targets_skip_and_count_malformed_json_without_dropping_other_sources(tmp_path):
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "modify_date"),
        [{"docket_id": "EPA-X", "rin": "2060-AV12", "modify_date": "2024-01-01"}],
    )
    _write(
        tmp_path / "documents.parquet",
        ("document_id", "docket_id", "additional_rins", "fr_doc_num", "posted_date", "modify_date"),
        [{"document_id": "D1", "docket_id": "EPA-X", "additional_rins": "{bad", "fr_doc_num": None}],
    )
    _write(
        tmp_path / "federal_register.parquet",
        ("document_number", "cfr_references_json", "regulation_id_numbers_json", "publication_date"),
        [],
    )
    _write(tmp_path / "fr_docket_links.parquet", ("document_number", "docket_id"), [])
    _write(
        tmp_path / "unified_agenda.parquet",
        ("rin", "agenda_edition", "cfr_references_json", "first_action_date", "next_action_date"),
        [],
    )

    rows = pq.read_table(
        build_rule_targets(tmp_path, run_id="malformed", asserted_at="2026-07-23T12:00:00Z")
    ).to_pylist()
    assert [(row["source"], row["rin"]) for row in rows] == [("docket_rin", "2060-AV12")]


def test_rule_targets_accept_source_backed_underscores_and_reject_untrusted_links(tmp_path):
    valid_docket = "EPA-HQ-OAR-2026-0001"
    underscore_docket = "EPA_FRDOC_0001"
    rin = "2060-ZZ01"
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "modify_date"),
        [
            {"docket_id": valid_docket, "rin": rin, "modify_date": "2026-01-01"},
            {"docket_id": underscore_docket, "rin": rin, "modify_date": "2026-01-01"},
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        ("document_id", "docket_id", "additional_rins", "fr_doc_num", "posted_date"),
        [
            {
                "document_id": f"{valid_docket}-0001",
                "docket_id": valid_docket,
                "additional_rins": f'["{rin}"]',
                "fr_doc_num": "2026-00001",
                "posted_date": "2026-01-02",
            }
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        ("document_number", "cfr_references_json", "regulation_id_numbers_json", "publication_date"),
        [
            {
                "document_number": "2026-00001",
                "cfr_references_json": '[{"title": 40, "part": 60}]',
                "regulation_id_numbers_json": f'["{rin}"]',
                "publication_date": "2026-01-02",
            }
        ],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "docket_id"),
        [
            {"document_number": "2026-00001", "docket_id": valid_docket},
            {"document_number": "2026-00001", "docket_id": underscore_docket},
            {"document_number": "2026-00001", "docket_id": "AID_FRDOC_0001"},
            {"document_number": "2026-00001", "docket_id": "Sequence No. 1"},
            # Syntactically plausible, but an antidumping case number rather
            # than a docket present in the Regulations.gov source corpus.
            {"document_number": "2026-00001", "docket_id": "A-570-831"},
        ],
    )
    _write(
        tmp_path / "unified_agenda.parquet",
        ("rin", "agenda_edition", "cfr_references_json"),
        [],
    )

    rows = pq.read_table(
        build_rule_targets(
            tmp_path,
            run_id="fr-label-boundary",
            asserted_at="2026-07-24T12:00:00Z",
        )
    ).to_pylist()

    assert rows
    assert {row["docket_id"] for row in rows} == {valid_docket, underscore_docket}
    assert ("fr_cfr_ref", "40-60") in {(row["source"], row["cfr_ref"]) for row in rows}
