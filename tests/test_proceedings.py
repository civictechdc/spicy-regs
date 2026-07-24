"""Golden reference-corpus test for proceedings and reopened comment periods."""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest

from spicy_regs.ontology.common import write_parquet_rows
from spicy_regs.transforms.build_comment_periods import (
    COLUMNS as COMMENT_PERIOD_COLUMNS,
    build_comment_periods,
)
from spicy_regs.transforms.build_proceedings import (
    COLUMNS as PROCEEDING_COLUMNS,
    _current_stage_from_events,
    build_proceedings,
)


def _write(path, columns, rows):
    write_parquet_rows(path, columns=columns, rows=rows)


def test_current_stage_requires_unique_latest_stage_family_event():
    events = [
        {"stage": "proposed", "event_kind": "proceedingProposed", "effective_date": "2026-01-01"},
        {"stage": "final", "event_kind": "proceedingFinal", "effective_date": "2026-02-01"},
    ]
    assert _current_stage_from_events(events) == "final"

    events.append(
        {"stage": "withdrawn", "event_kind": "proceedingWithdrawn", "effective_date": "2026-02-01"}
    )
    assert _current_stage_from_events(events) is None
    assert _current_stage_from_events(
        [{"stage": "final", "event_kind": "proceedingFinal", "effective_date": None}]
    ) is None
    with pytest.raises(ValueError, match="disagrees"):
        _current_stage_from_events(
            [{"stage": "final", "event_kind": "proceedingProposed", "effective_date": "2026-02-01"}]
        )


def test_reference_proceeding_threads_rinless_docket_and_preserves_reopening(tmp_path):
    docket_id = "EPA-HQ-OAR-2021-0044"
    rin = "2060-AV16"
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [
            {
                "docket_id": docket_id,
                "rin": None,
                "docket_type": "Rulemaking",
                "title": "Methane Emissions Standards",
                "agency_code": "EPA",
                "modify_date": "2024-03-08",
            }
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        (
            "document_id",
            "docket_id",
            "additional_rins",
            "document_type",
            "title",
            "agency_code",
            "posted_date",
            "comment_start_date",
            "comment_end_date",
        ),
        [
            {
                "document_id": "D-PROPOSAL",
                "docket_id": docket_id,
                "additional_rins": "[]",
                "document_type": "Proposed Rule",
                "title": "Standards proposal",
                "agency_code": "EPA",
                "posted_date": "2021-11-15",
                "comment_start_date": "2021-11-15",
                "comment_end_date": "2022-01-14",
            },
            {
                "document_id": "D-EXTENSION",
                "docket_id": docket_id,
                "additional_rins": "[]",
                "document_type": "Notice",
                "title": "Comment period extension",
                "agency_code": "EPA",
                "posted_date": "2021-12-17",
                "comment_start_date": "2021-12-17",
                "comment_end_date": "2022-01-31",
            },
            {
                "document_id": "D-SUPPLEMENTAL",
                "docket_id": docket_id,
                "additional_rins": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": "Supplemental proposal",
                "agency_code": "EPA",
                "posted_date": "2022-12-06",
                "comment_start_date": "2022-12-06",
                "comment_end_date": "2023-01-05",
            },
            {
                "document_id": "D-FINAL",
                "docket_id": docket_id,
                "additional_rins": f'["{rin}"]',
                "document_type": "Rule",
                "title": "Final standards",
                "agency_code": "EPA",
                "posted_date": "2024-03-08",
                "comment_start_date": None,
                "comment_end_date": None,
            },
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        (
            "document_number",
            "regulation_id_numbers_json",
            "document_type",
            "title",
            "publication_date",
            "comments_close_on",
        ),
        [
            {
                "document_number": "2021-24202",
                "regulation_id_numbers_json": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": "Standards proposal",
                "publication_date": "2021-11-15",
                "comments_close_on": "2022-01-14",
            },
            {
                "document_number": "2021-27312",
                "regulation_id_numbers_json": f'["{rin}"]',
                "document_type": "Notice",
                "title": "Comment period extension",
                "publication_date": "2021-12-17",
                "comments_close_on": "2022-01-31",
            },
            {
                "document_number": "2022-24675",
                "regulation_id_numbers_json": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": "Supplemental proposal",
                "publication_date": "2022-12-06",
                "comments_close_on": "2023-01-05",
            },
            {
                "document_number": "2024-00366",
                "regulation_id_numbers_json": f'["{rin}"]',
                "document_type": "Rule",
                "title": "Final standards",
                "publication_date": "2024-03-08",
                "comments_close_on": None,
            },
        ],
    )
    _write(
        tmp_path / "unified_agenda.parquet",
        ("rin", "agenda_edition", "title", "agency_code", "rule_stage", "first_action_date"),
        [
            {
                "rin": rin,
                "agenda_edition": "202404",
                "title": "Methane Emissions Standards",
                "agency_code": "EPA",
                "rule_stage": "Final Rule Stage",
                "first_action_date": "2021-11-15",
            }
        ],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "docket_id"),
        [
            {"document_number": number, "docket_id": docket_id}
            for number in ("2021-24202", "2021-27312", "2022-24675", "2024-00366")
        ],
    )
    _write(
        tmp_path / "rule_targets.parquet",
        ("docket_id", "rin", "cfr_ref", "cfr_title", "cfr_part", "cfr_section"),
        [
            {
                "docket_id": docket_id,
                "rin": rin,
                "cfr_ref": "40-60",
                "cfr_title": "40",
                "cfr_part": "60",
                "cfr_section": None,
            }
        ],
    )
    _write(
        tmp_path / "authority_edges.parquet",
        ("rin", "usc_title", "usc_section", "pl_number", "authority_raw"),
        [{"rin": rin, "usc_title": "42", "usc_section": "7401", "pl_number": None}],
    )

    proceedings_file = build_proceedings(
        tmp_path,
        run_id="reference-corpus",
        asserted_at="2026-07-23T12:00:00Z",
    )
    proceedings = pq.read_table(proceedings_file).to_pylist()
    assert pq.ParquetFile(proceedings_file).schema_arrow.names == list(PROCEEDING_COLUMNS)
    assert len(proceedings) == 1
    proceeding = proceedings[0]
    assert proceeding["rin"] == rin
    assert json.loads(proceeding["docket_ids_json"]) == [docket_id]
    assert proceeding["current_stage"] == "final"
    assert json.loads(proceeding["cfr_refs_json"]) == ["40-60"]
    assert json.loads(proceeding["cfr_target_iris_json"]) == ["urn:rkaf:us:cfr:40:60"]
    assert json.loads(proceeding["authority_refs_json"]) == ["usc:42-7401"]
    stages = {event["stage"] for event in json.loads(proceeding["stage_events_json"])}
    assert {"proposed", "supplemental", "final"} <= stages

    periods_file = build_comment_periods(
        tmp_path,
        run_id="reference-corpus",
        asserted_at="2026-07-23T12:00:00Z",
    )
    periods = pq.read_table(periods_file).to_pylist()
    assert pq.ParquetFile(periods_file).schema_arrow.names == list(COMMENT_PERIOD_COLUMNS)
    assert [(row["open_date"], row["close_date"]) for row in periods] == [
        ("2021-11-15", "2022-01-31"),
        ("2022-12-06", "2023-01-05"),
    ]
    assert all(
        json.loads(row["proceeding_ids_json"]) == [proceeding["proceeding_id"]]
        for row in periods
    )
    assert all(json.loads(row["docket_ids_json"]) == [docket_id] for row in periods)
    assert json.loads(periods[0]["opened_by_artifact_ids_json"]) == [
        "https://www.federalregister.gov/d/2021-24202",
        "https://www.regulations.gov/document/D-PROPOSAL",
    ]
    assert "D-EXTENSION" in json.loads(periods[0]["evidence_ids_json"])
    assert "https://www.regulations.gov/document/D-EXTENSION" not in json.loads(
        periods[0]["opened_by_artifact_ids_json"]
    )
    assert all(row["method"] == "deterministic" for row in periods)
    assert all(row["actor_id"] == "spicy-regs:comment-periods:v2" for row in periods)


def test_reused_rin_does_not_collapse_or_cross_assign_distinct_dockets(tmp_path):
    rin = "2120-AA64"
    dockets = ("FAA-2025-0001", "FAA-2026-0002")
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [
            {
                "docket_id": docket,
                "rin": rin,
                "docket_type": "Rulemaking",
                "title": f"Airworthiness directive {index}",
                "agency_code": "FAA",
                "modify_date": f"202{index + 4}-01-01",
            }
            for index, docket in enumerate(dockets, start=1)
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        (
            "document_id",
            "docket_id",
            "additional_rins",
            "document_type",
            "title",
            "agency_code",
            "posted_date",
            "comment_start_date",
            "comment_end_date",
        ),
        [
            {
                "document_id": f"DOC-{index}",
                "docket_id": docket,
                "additional_rins": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": f"Proposal {index}",
                "agency_code": "FAA",
                "posted_date": f"202{index + 4}-01-01",
                "comment_start_date": f"202{index + 4}-01-01",
                "comment_end_date": f"202{index + 4}-02-01",
            }
            for index, docket in enumerate(dockets, start=1)
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        (
            "document_number",
            "regulation_id_numbers_json",
            "document_type",
            "title",
            "publication_date",
            "comments_close_on",
        ),
        [
            {
                "document_number": f"202{index + 4}-0000{index}",
                "regulation_id_numbers_json": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": f"Proposal {index}",
                "publication_date": f"202{index + 4}-01-01",
                "comments_close_on": f"202{index + 4}-02-01",
            }
            for index in range(1, 3)
        ],
    )
    _write(
        tmp_path / "unified_agenda.parquet",
        ("rin", "agenda_edition", "title", "agency_code", "rule_stage", "first_action_date"),
        [],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "docket_id"),
        [
            {"document_number": f"202{index + 4}-0000{index}", "docket_id": docket}
            for index, docket in enumerate(dockets, start=1)
        ],
    )
    _write(
        tmp_path / "rule_targets.parquet",
        ("docket_id", "rin", "cfr_ref"),
        [{"docket_id": docket, "rin": rin, "cfr_ref": "14-39"} for docket in dockets],
    )
    _write(
        tmp_path / "authority_edges.parquet",
        ("rin", "usc_title", "usc_section", "pl_number", "authority_raw"),
        [],
    )

    proceeding_rows = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="reused-rin",
            asserted_at="2026-07-23T12:00:00Z",
        )
    ).to_pylist()
    assert len(proceeding_rows) == 2
    assert {tuple(json.loads(row["docket_ids_json"])) for row in proceeding_rows} == {
        (dockets[0],),
        (dockets[1],),
    }

    period_rows = pq.read_table(
        build_comment_periods(
            tmp_path,
            run_id="reused-rin",
            asserted_at="2026-07-23T12:00:00Z",
        )
    ).to_pylist()
    assert len(period_rows) == 2
    assert {
        tuple(json.loads(row["docket_ids_json"])) for row in period_rows
    } == {(dockets[0],), (dockets[1],)}


def test_untrusted_fr_administrative_labels_do_not_become_proceeding_dockets(tmp_path):
    valid_docket = "EPA-HQ-OAR-2026-0001"
    rin = "2060-ZZ01"
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [
            {
                "docket_id": valid_docket,
                "rin": rin,
                "docket_type": "Rulemaking",
                "title": "Valid proceeding",
                "agency_code": "EPA",
                "modify_date": "2026-01-01",
            },
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        (
            "document_id",
            "docket_id",
            "additional_rins",
            "document_type",
            "title",
            "agency_code",
            "posted_date",
            "comment_start_date",
            "comment_end_date",
        ),
        [
            {
                "document_id": f"{valid_docket}-0001",
                "docket_id": valid_docket,
                "additional_rins": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": "Valid proposal",
                "agency_code": "EPA",
                "posted_date": "2026-01-02",
                "comment_start_date": "2026-01-02",
                "comment_end_date": "2026-02-02",
            }
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        (
            "document_number",
            "regulation_id_numbers_json",
            "document_type",
            "title",
            "publication_date",
            "comments_close_on",
        ),
        [
            {
                "document_number": "2026-00001",
                "regulation_id_numbers_json": f'["{rin}"]',
                "document_type": "Proposed Rule",
                "title": "Valid proposal",
                "publication_date": "2026-01-02",
                "comments_close_on": "2026-02-02",
            }
        ],
    )
    _write(
        tmp_path / "unified_agenda.parquet",
        ("rin", "agenda_edition", "title", "agency_code", "rule_stage"),
        [],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "docket_id"),
        [
            {"document_number": "2026-00001", "docket_id": valid_docket},
            {"document_number": "2026-00001", "docket_id": "AID_FRDOC_0001"},
            {"document_number": "2026-00001", "docket_id": "Sequence No. 1"},
            {"document_number": "2026-00001", "docket_id": "A-570-831"},
        ],
    )
    _write(
        tmp_path / "rule_targets.parquet",
        ("docket_id", "rin", "cfr_ref"),
        [{"docket_id": valid_docket, "rin": rin, "cfr_ref": "40-60"}],
    )
    _write(
        tmp_path / "authority_edges.parquet",
        ("rin", "usc_title", "usc_section", "pl_number", "authority_raw"),
        [],
    )

    proceeding_rows = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="fr-label-boundary",
            asserted_at="2026-07-24T12:00:00Z",
        )
    ).to_pylist()
    assert len(proceeding_rows) == 1
    assert json.loads(proceeding_rows[0]["docket_ids_json"]) == [valid_docket]

    period_rows = pq.read_table(
        build_comment_periods(
            tmp_path,
            run_id="fr-label-boundary",
            asserted_at="2026-07-24T12:00:00Z",
        )
    ).to_pylist()
    assert len(period_rows) == 1
    assert json.loads(period_rows[0]["docket_ids_json"]) == [valid_docket]


def test_rinless_evidence_does_not_fan_out_across_a_multi_proceeding_docket(
    tmp_path,
):
    docket_id = "EPA-HQ-OAR-2026-9999"
    rins = ("2060-AA01", "2060-BB02")
    _write(
        tmp_path / "dockets.parquet",
        ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
        [
            {
                "docket_id": docket_id,
                "rin": rins[0],
                "docket_type": "Rulemaking",
                "title": "Shared administrative docket",
                "agency_code": "EPA",
                "modify_date": "2026-01-01",
            }
        ],
    )
    _write(
        tmp_path / "documents.parquet",
        (
            "document_id",
            "docket_id",
            "additional_rins",
            "document_type",
            "title",
            "agency_code",
            "posted_date",
            "comment_start_date",
            "comment_end_date",
        ),
        [
            {
                "document_id": "D-SECOND-RIN",
                "docket_id": docket_id,
                "additional_rins": f'["{rins[1]}"]',
                "document_type": "Notice",
                "title": "Second rulemaking",
                "agency_code": "EPA",
                "posted_date": "2026-01-02",
                "comment_start_date": None,
                "comment_end_date": None,
            },
            {
                "document_id": "D-RINLESS-COMMENT",
                "docket_id": docket_id,
                "additional_rins": "[]",
                "document_type": "Notice",
                "title": "Unscoped comment notice",
                "agency_code": "EPA",
                "posted_date": "2026-02-01",
                "comment_start_date": "2026-02-01",
                "comment_end_date": "2026-03-01",
            },
        ],
    )
    _write(
        tmp_path / "federal_register.parquet",
        (
            "document_number",
            "regulation_id_numbers_json",
            "document_type",
            "title",
            "publication_date",
            "comments_close_on",
        ),
        [],
    )
    _write(
        tmp_path / "unified_agenda.parquet",
        ("rin", "agenda_edition", "title", "agency_code", "rule_stage", "first_action_date"),
        [],
    )
    _write(
        tmp_path / "fr_docket_links.parquet",
        ("document_number", "docket_id"),
        [],
    )
    _write(
        tmp_path / "rule_targets.parquet",
        ("docket_id", "rin", "cfr_ref"),
        [{"docket_id": docket_id, "rin": rin, "cfr_ref": "40-60"} for rin in rins],
    )
    _write(
        tmp_path / "authority_edges.parquet",
        ("rin", "usc_title", "usc_section", "pl_number", "authority_raw"),
        [],
    )

    proceeding_rows = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="multi-proceeding-docket",
            asserted_at="2026-07-23T12:00:00Z",
        )
    ).to_pylist()
    assert len(proceeding_rows) == 2
    assert {row["rin"] for row in proceeding_rows} == set(rins)
    assert all(row["current_stage"] is None for row in proceeding_rows)
    assert all(json.loads(row["stage_events_json"]) == [] for row in proceeding_rows)

    period_rows = pq.read_table(
        build_comment_periods(
            tmp_path,
            run_id="multi-proceeding-docket",
            asserted_at="2026-07-23T12:00:00Z",
        )
    ).to_pylist()
    assert len(period_rows) == 1
    assert json.loads(period_rows[0]["proceeding_ids_json"]) == []
    assert json.loads(period_rows[0]["docket_ids_json"]) == [docket_id]
    assert json.loads(period_rows[0]["opened_by_artifact_ids_json"]) == [
        "https://www.regulations.gov/document/D-RINLESS-COMMENT"
    ]


def test_proceeding_id_survives_new_earlier_docket_and_records_continuity(tmp_path):
    rin = "2060-ZZ99"
    original_docket = "ZZZ-2026-0001"
    added_docket = "AAA-2025-0001"

    def write_sources(dockets, links):
        _write(
            tmp_path / "dockets.parquet",
            ("docket_id", "rin", "docket_type", "title", "agency_code", "modify_date"),
            [
                {
                    "docket_id": docket,
                    "rin": rin,
                    "docket_type": "Rulemaking",
                    "title": "Stable identity fixture",
                    "agency_code": "EPA",
                    "modify_date": "2026-01-01",
                }
                for docket in dockets
            ],
        )
        _write(
            tmp_path / "documents.parquet",
            (
                "document_id",
                "docket_id",
                "additional_rins",
                "document_type",
                "title",
                "agency_code",
                "posted_date",
            ),
            [],
        )
        _write(
            tmp_path / "federal_register.parquet",
            (
                "document_number",
                "regulation_id_numbers_json",
                "document_type",
                "title",
                "publication_date",
            ),
            [
                {
                    "document_number": "2026-00001",
                    "regulation_id_numbers_json": f'["{rin}"]',
                    "document_type": "Proposed Rule",
                    "title": "Stable identity fixture",
                    "publication_date": "2026-01-01",
                }
            ],
        )
        _write(
            tmp_path / "unified_agenda.parquet",
            ("rin", "agenda_edition", "title", "agency_code", "rule_stage"),
            [],
        )
        _write(
            tmp_path / "fr_docket_links.parquet",
            ("document_number", "docket_id"),
            [{"document_number": "2026-00001", "docket_id": docket} for docket in links],
        )
        _write(
            tmp_path / "rule_targets.parquet",
            ("docket_id", "rin", "cfr_ref"),
            [{"docket_id": docket, "rin": rin, "cfr_ref": "40-60"} for docket in dockets],
        )
        _write(
            tmp_path / "authority_edges.parquet",
            ("rin", "usc_title", "usc_section", "pl_number", "authority_raw"),
            [],
        )

    write_sources((original_docket,), (original_docket,))
    first = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="identity-first",
            asserted_at="2026-07-23T12:00:00Z",
        )
    ).to_pylist()
    assert len(first) == 1
    stable_proceeding_id = first[0]["proceeding_id"]

    # The new docket sorts before the original and would change a stateless
    # min-docket hash. One FR document explicitly links both into one component.
    write_sources(
        (added_docket, original_docket),
        (added_docket, original_docket),
    )
    second = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="identity-second",
            asserted_at="2026-07-24T12:00:00Z",
        )
    ).to_pylist()

    assert len(second) == 1
    assert second[0]["proceeding_id"] == stable_proceeding_id
    assert json.loads(second[0]["docket_ids_json"]) == [
        added_docket,
        original_docket,
    ]
    assert json.loads(second[0]["identity_predecessors_json"]) == []
    assert second[0]["supersedes_id"] == stable_proceeding_id


def test_unscoped_rin_keeps_identity_when_one_docket_becomes_known(tmp_path):
    rin = "2060-YY98"
    docket_id = "EPA-HQ-OAR-2026-0098"

    def write_sources(*, include_docket: bool) -> None:
        _write(
            tmp_path / "dockets.parquet",
            ("docket_id", "rin", "docket_type", "title", "agency_code"),
            (
                [
                    {
                        "docket_id": docket_id,
                        "rin": rin,
                        "docket_type": "Rulemaking",
                        "title": "Scope transition fixture",
                        "agency_code": "EPA",
                    }
                ]
                if include_docket
                else []
            ),
        )
        _write(
            tmp_path / "documents.parquet",
            (
                "document_id",
                "docket_id",
                "additional_rins",
                "document_type",
                "title",
                "agency_code",
                "posted_date",
            ),
            [],
        )
        _write(
            tmp_path / "federal_register.parquet",
            (
                "document_number",
                "regulation_id_numbers_json",
                "document_type",
                "title",
                "publication_date",
            ),
            [
                {
                    "document_number": "2026-00098",
                    "regulation_id_numbers_json": f'["{rin}"]',
                    "document_type": "Proposed Rule",
                    "title": "Scope transition fixture",
                    "publication_date": "2026-01-01",
                }
            ],
        )
        _write(
            tmp_path / "unified_agenda.parquet",
            ("rin", "agenda_edition", "title", "agency_code", "rule_stage"),
            [],
        )
        _write(
            tmp_path / "fr_docket_links.parquet",
            ("document_number", "docket_id"),
            ([{"document_number": "2026-00098", "docket_id": docket_id}] if include_docket else []),
        )
        _write(
            tmp_path / "rule_targets.parquet",
            ("docket_id", "rin", "cfr_ref"),
            ([{"docket_id": docket_id, "rin": rin, "cfr_ref": "40-98"}] if include_docket else []),
        )
        _write(
            tmp_path / "authority_edges.parquet",
            ("rin", "usc_title", "usc_section", "pl_number", "authority_raw"),
            [],
        )

    write_sources(include_docket=False)
    first = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="unscoped-first",
            asserted_at="2026-07-23T12:00:00Z",
        )
    ).to_pylist()
    assert len(first) == 1
    stable_id = first[0]["proceeding_id"]
    assert json.loads(first[0]["docket_ids_json"]) == []

    write_sources(include_docket=True)
    second = pq.read_table(
        build_proceedings(
            tmp_path,
            run_id="unscoped-second",
            asserted_at="2026-07-24T12:00:00Z",
        )
    ).to_pylist()
    assert len(second) == 1
    assert second[0]["proceeding_id"] == stable_id
    assert json.loads(second[0]["docket_ids_json"]) == [docket_id]
    assert json.loads(second[0]["identity_predecessors_json"]) == []
    assert second[0]["supersedes_id"] == stable_id
