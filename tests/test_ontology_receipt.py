"""Corpus-bound receipt validation for the ontology generation."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq

from spicy_regs.data_dictionary import expected_schemas
from spicy_regs.ontology.common import write_parquet_rows
from spicy_regs.ontology.receipt import validate_generation
from spicy_regs.pipelines.ontology_dataset import OntologyDatasetPipeline

SCHEMAS = expected_schemas()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_table(directory: Path, table: str, rows: list[dict]) -> Path:
    path = directory / f"{table}.parquet"
    write_parquet_rows(
        path,
        columns=tuple(column for column, _ in SCHEMAS[table]),
        rows=rows,
    )
    return path


def _source_record(path: Path) -> dict[str, int | str]:
    return {"bytes": path.stat().st_size, "sha256": _sha256(path)}


def _fixture_manifest(tmp_path: Path) -> Path:
    docket_id = "EPA_FRDOC_0001"
    proceeding_id = "proceeding_fixture"
    _write_table(tmp_path, "dockets", [{"docket_id": docket_id}])
    _write_table(
        tmp_path,
        "documents",
        [
            {
                "document_id": f"{docket_id}-0001",
                "docket_id": docket_id,
                "fr_doc_num": "2026-00001",
            },
            {
                "document_id": f"{docket_id}-0002",
                "docket_id": docket_id,
                "fr_doc_num": "FR Doc 2026-00001",
            },
        ],
    )
    _write_table(
        tmp_path,
        "federal_register",
        [{"document_number": "2026-00001"}],
    )
    _write_table(tmp_path, "unified_agenda", [])
    _write_table(tmp_path, "fr_docket_links", [])
    _write_table(
        tmp_path,
        "rule_targets",
        [
            {
                "docket_id": docket_id,
                "cfr_ref": "40-60.5375a",
                "cfr_title": "40",
                "cfr_part": "60",
                "cfr_section": "5375a",
                "rin": "2060-AV16",
                "source": "fr_cfr_ref",
            }
        ],
    )
    _write_table(tmp_path, "authority_edges", [])
    _write_table(
        tmp_path,
        "proceedings",
        [
            {
                "proceeding_id": proceeding_id,
                "rin": "2060-AV16",
                "docket_ids_json": json.dumps([docket_id]),
                "current_stage": "proposed",
                "stage_events_json": json.dumps(
                    [
                        {
                            "stage": "proposed",
                            "event_kind": "proceedingProposed",
                            "effective_date": "2026-01-01",
                        }
                    ]
                ),
                "fr_document_numbers_json": '["2026-00001"]',
                "cfr_refs_json": '["40-60.5375a"]',
                "cfr_target_iris_json": '["urn:rkaf:us:cfr:40:60.5375a"]',
                "authority_refs_json": "[]",
                "identity_predecessors_json": "[]",
            }
        ],
    )
    _write_table(
        tmp_path,
        "regulatory_agenda_items",
        [
            {
                "agenda_item_id": "urn:rkaf:us:rin:2060-AV16",
                "rin": "2060-AV16",
                "scope_status": "single_observed",
                "scope_basis": "one_evidence_linked_proceeding",
                "linked_proceeding_count": "1",
                "observation_count": "0",
            }
        ],
    )
    _write_table(
        tmp_path,
        "agenda_item_proceedings",
        [
            {
                "relationship_id": "agenda_relationship_fixture",
                "agenda_item_id": "urn:rkaf:us:rin:2060-AV16",
                "rin": "2060-AV16",
                "proceeding_id": proceeding_id,
                "relationship_role": "agenda_tracks_proceeding",
                "source": "docket_rin",
                "evidence_id": docket_id,
                "evidence_uri": (
                    f"https://www.regulations.gov/docket/{docket_id}"
                ),
                "evidence_date": "2026-01-01",
            }
        ],
    )
    _write_table(
        tmp_path,
        "comment_periods",
        [
            {
                "comment_period_id": "comment_period_fixture",
                "proceeding_ids_json": json.dumps([proceeding_id]),
                "rins_json": '["2060-AV16"]',
                "docket_ids_json": json.dumps([docket_id]),
                "open_date": "2026-01-01",
                "close_date": "2026-02-01",
                "source": "federal_register.comments_close_on",
                "opened_by_artifact_ids_json": (
                    '["https://www.federalregister.gov/d/2026-00001"]'
                ),
                "evidence_ids_json": '["2026-00001"]',
            }
        ],
    )
    for table in ("concepts", "concept_assignments", "concept_events"):
        _write_table(tmp_path, table, [])

    snapshot_id = "snapshot_fixture"
    prefix = f"materialized/ontology/snapshots/{snapshot_id}"
    artifacts = {}
    for name in OntologyDatasetPipeline.published_outputs:
        path = tmp_path / name
        artifacts[name] = {
            **_source_record(path),
            "remote_key": f"{prefix}/{name}",
        }
    sources = {
        name: _source_record(tmp_path / name)
        for name in OntologyDatasetPipeline.source_inputs
    }
    manifest = {
        "format_version": 1,
        "dataset": "ontology",
        "snapshot_id": snapshot_id,
        "inputs": {
            "sources": sources,
            "prior_artifacts": {},
            "previous_snapshot_id": None,
        },
        "artifacts": artifacts,
    }
    path = tmp_path / "ontology-dataset-manifest.json"
    path.write_text(json.dumps(manifest))
    return path


def test_generation_receipt_validates_every_manifest_artifact(tmp_path):
    manifest = _fixture_manifest(tmp_path)
    result = validate_generation(manifest)

    assert result["status"] == "pass"
    assert result["failures"]["total"] == 0
    assert set(result["artifacts"]) == set(OntologyDatasetPipeline.published_outputs)
    assert result["metrics"]["proceedings"]["citation_target_iris"] == 1
    assert result["metrics"]["source_membership"]["cross_posting_links"] == 1
    assert (
        result["metrics"]["source_membership"]["cross_posting_values_filtered"]
        == 1
    )


def test_generation_receipt_rejects_unknown_comment_period_reference(tmp_path):
    manifest = _fixture_manifest(tmp_path)
    rows = [
        {
            "comment_period_id": "comment_period_fixture",
            "proceeding_ids_json": '["missing"]',
            "rins_json": '["2060-AV16"]',
            "docket_ids_json": '["EPA_FRDOC_0001"]',
            "open_date": "2026-01-01",
            "close_date": "2026-02-01",
            "source": "federal_register.comments_close_on",
            "opened_by_artifact_ids_json": (
                '["https://www.federalregister.gov/d/2026-00001"]'
            ),
            "evidence_ids_json": '["2026-00001"]',
        }
    ]
    _write_table(tmp_path, "comment_periods", rows)

    result = validate_generation(manifest)

    assert result["status"] == "fail"
    assert result["failures"]["by_check"]["comment_periods.proceeding_range"] == 1
    assert result["failures"]["by_check"]["manifest.sha256"] == 1


def test_generation_receipt_rejects_unknown_agenda_proceeding_reference(
    tmp_path,
):
    manifest = _fixture_manifest(tmp_path)
    rows = pq.read_table(
        tmp_path / "agenda_item_proceedings.parquet"
    ).to_pylist()
    rows[0]["proceeding_id"] = "missing"
    _write_table(tmp_path, "agenda_item_proceedings", rows)

    result = validate_generation(manifest)

    assert result["status"] == "fail"
    assert (
        result["failures"]["by_check"][
            "agenda_item_proceedings.proceeding_range"
        ]
        == 1
    )


def test_generation_receipt_rejects_inconsistent_agenda_scope_basis(tmp_path):
    manifest = _fixture_manifest(tmp_path)
    rows = pq.read_table(
        tmp_path / "regulatory_agenda_items.parquet"
    ).to_pylist()
    rows[0]["scope_basis"] = "multiple_evidence_linked_proceedings"
    _write_table(tmp_path, "regulatory_agenda_items", rows)

    result = validate_generation(manifest)

    assert result["status"] == "fail"
    assert (
        result["failures"]["by_check"][
            "regulatory_agenda_items.scope_basis"
        ]
        == 1
    )


def test_generation_receipt_requires_identical_baseline_inputs(tmp_path):
    manifest = _fixture_manifest(tmp_path)
    baseline = json.loads(manifest.read_text())
    baseline["inputs"]["previous_snapshot_id"] = "different"
    baseline_path = tmp_path / "baseline-manifest.json"
    baseline_path.write_text(json.dumps(baseline))

    result = validate_generation(
        manifest,
        baseline_manifest_path=baseline_path,
    )

    assert result["status"] == "fail"
    assert result["failures"]["by_check"]["manifest.baseline_inputs"] == 1


def test_generation_receipt_rejects_unknown_comment_evidence(tmp_path):
    manifest = _fixture_manifest(tmp_path)
    rows = [
        {
            "comment_period_id": "comment_period_fixture",
            "proceeding_ids_json": '["proceeding_fixture"]',
            "rins_json": '["2060-AV16"]',
            "docket_ids_json": '["EPA_FRDOC_0001"]',
            "open_date": "2026-01-01",
            "close_date": "2026-02-01",
            "source": "documents.comment_end_date",
            "opened_by_artifact_ids_json": (
                '["https://www.regulations.gov/document/NOT-A-SOURCE-ROW"]'
            ),
            "evidence_ids_json": '["NOT-A-SOURCE-ROW"]',
        }
    ]
    _write_table(tmp_path, "comment_periods", rows)

    result = validate_generation(manifest)

    assert result["status"] == "fail"
    assert result["failures"]["by_check"]["comment_periods.evidence_membership"] == 1
    assert result["failures"]["by_check"]["comment_periods.opened_by_transform"] == 1


def test_generation_receipt_rejects_proceeding_self_supersession(tmp_path):
    manifest = _fixture_manifest(tmp_path)
    rows = pq.read_table(tmp_path / "proceedings.parquet").to_pylist()
    rows[0]["identity_predecessors_json"] = '["proceeding_fixture"]'
    _write_table(tmp_path, "proceedings", rows)

    result = validate_generation(manifest)

    assert result["status"] == "fail"
    assert result["failures"]["by_check"]["proceedings.predecessor_self_edge"] == 1


def test_generation_receipt_records_unreadable_parquet(tmp_path):
    manifest = _fixture_manifest(tmp_path)
    artifact = tmp_path / "concepts.parquet"
    artifact.write_bytes(b"not parquet")
    payload = json.loads(manifest.read_text())
    payload["artifacts"]["concepts.parquet"].update(_source_record(artifact))
    manifest.write_text(json.dumps(payload))

    result = validate_generation(manifest)

    assert result["status"] == "fail"
    assert result["failures"]["by_check"]["manifest.parquet"] == 1
