"""Hermetic checks for the segmentation-evaluation dataset re-seal tool.

Why this tool exists: the sealed evaluation dataset's ``evaluation_id`` is a
digest over *every* non-model member, so rewriting any member — even one the
segmenter never reads — invalidates the whole seal and blocks every downstream
gate. The tool never repairs data. It copies a dataset, recomputes the seal
over what is actually on disk, and reports exactly which members moved, so a
reader can judge whether the movement touches the measurement.

Every fixture here is synthetic; nothing reads the real corpus.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = REPO_ROOT / "tools" / "reseal_segmentation_dataset.py"


def _load_module() -> Any:
    spec = importlib.util.spec_from_file_location("reseal_segmentation_dataset", TOOL_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


MODULE = _load_module()


def _write_parquet(path: Path, values: list[str]) -> None:
    pq.write_table(pa.table({"value": values}), path)


def _dataset(directory: Path, *, links: list[str]) -> dict[str, Any]:
    """A miniature stand-in for the sealed dataset directory shape."""
    directory.mkdir(parents=True, exist_ok=True)
    _write_parquet(directory / "gold_spans.parquet", ["a", "b"])
    _write_parquet(directory / "fr_docket_links.parquet", links)
    (directory / "source-lock.json").write_text('{"retrieved_on": "2026-07-24"}\n', encoding="utf-8")
    members = MODULE.dataset_members(directory)
    manifest = {
        "format_version": 1,
        "evaluation_id": MODULE.evaluation_id(members),
        "purpose": "test",
        "nonmodel_artifacts": members,
    }
    (directory / MODULE.MANIFEST_NAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (directory / MODULE.RECEIPT_NAME).write_text(
        json.dumps({"status": "pass", "evaluation_id": manifest["evaluation_id"]}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


LINKS = ("fr_docket_links.parquet",)


def _passing_validator(directory: Path) -> dict[str, Any]:
    members = MODULE.dataset_members(directory)
    return {"status": "pass", "evaluation_id": MODULE.evaluation_id(members), "failures": []}


# --------------------------------------------------------------------------
# seal delta
# --------------------------------------------------------------------------


def test_evaluation_id_is_a_digest_over_member_digests_only(tmp_path: Path) -> None:
    manifest = _dataset(tmp_path / "one", links=["x"])
    members = MODULE.dataset_members(tmp_path / "one")
    assert manifest["evaluation_id"].startswith("segmentation_eval_")
    # byte size and row count must not move the identity; only digests do
    inflated = {name: dict(record, rows=999, bytes=1) for name, record in members.items()}
    assert MODULE.evaluation_id(inflated) == manifest["evaluation_id"]


def test_seal_delta_reports_no_movement_for_an_untouched_dataset(tmp_path: Path) -> None:
    manifest = _dataset(tmp_path / "one", links=["x"])
    delta = MODULE.seal_delta(manifest["nonmodel_artifacts"], MODULE.dataset_members(tmp_path / "one"))
    assert delta["changed"] == []
    assert delta["missing"] == []
    assert delta["unchanged_count"] == 3


def test_seal_delta_names_exactly_the_member_that_moved(tmp_path: Path) -> None:
    directory = tmp_path / "one"
    manifest = _dataset(directory, links=["x"])
    _write_parquet(directory / "fr_docket_links.parquet", ["x", "y"])
    delta = MODULE.seal_delta(manifest["nonmodel_artifacts"], MODULE.dataset_members(directory))
    assert [one["name"] for one in delta["changed"]] == ["fr_docket_links.parquet"]
    assert delta["changed"][0]["sealed_sha256"] != delta["changed"][0]["current_sha256"]
    assert delta["unchanged_count"] == 2
    assert delta["missing"] == []


def test_seal_delta_separates_a_missing_member_from_a_moved_one(tmp_path: Path) -> None:
    directory = tmp_path / "one"
    manifest = _dataset(directory, links=["x"])
    (directory / "fr_docket_links.parquet").unlink()
    delta = MODULE.seal_delta(manifest["nonmodel_artifacts"], MODULE.dataset_members(directory))
    assert delta["missing"] == ["fr_docket_links.parquet"]
    assert delta["changed"] == []


# --------------------------------------------------------------------------
# re-seal
# --------------------------------------------------------------------------


def test_reseal_copies_every_member_byte_for_byte_and_leaves_the_source_alone(tmp_path: Path) -> None:
    source = tmp_path / "one"
    _dataset(source, links=["x", "y"])
    before = {path.name: path.read_bytes() for path in sorted(source.glob("*.parquet"))}
    output = tmp_path / "resealed"

    MODULE.reseal_segmentation_dataset(source, output, allow_changed=LINKS, validator=_passing_validator)

    assert {path.name: path.read_bytes() for path in sorted(source.glob("*.parquet"))} == before
    for name, payload in before.items():
        assert (output / name).read_bytes() == payload
    assert (output / "source-lock.json").read_bytes() == (source / "source-lock.json").read_bytes()


def test_reseal_of_an_untouched_dataset_reproduces_the_same_evaluation_id(tmp_path: Path) -> None:
    source = tmp_path / "one"
    manifest = _dataset(source, links=["x"])
    result = MODULE.reseal_segmentation_dataset(
        source, tmp_path / "resealed", allow_changed=LINKS, validator=_passing_validator
    )
    assert result["sealed_evaluation_id"] == result["resealed_evaluation_id"] == manifest["evaluation_id"]
    assert result["delta"]["changed"] == []


def test_reseal_records_the_new_identity_and_the_member_that_caused_it(tmp_path: Path) -> None:
    source = tmp_path / "one"
    manifest = _dataset(source, links=["x"])
    _write_parquet(source / "fr_docket_links.parquet", ["x", "y", "z"])
    output = tmp_path / "resealed"

    result = MODULE.reseal_segmentation_dataset(source, output, allow_changed=LINKS, validator=_passing_validator)

    assert result["sealed_evaluation_id"] == manifest["evaluation_id"]
    assert result["resealed_evaluation_id"] != manifest["evaluation_id"]
    assert [one["name"] for one in result["delta"]["changed"]] == ["fr_docket_links.parquet"]

    written = json.loads((output / MODULE.MANIFEST_NAME).read_text(encoding="utf-8"))
    assert written["evaluation_id"] == result["resealed_evaluation_id"]
    assert written["nonmodel_artifacts"] == MODULE.dataset_members(output)
    assert written["purpose"] == "test", "unrelated manifest facts are carried, not invented"
    assert written["resealed_from"]["evaluation_id"] == manifest["evaluation_id"]
    assert [one["name"] for one in written["resealed_from"]["changed_members"]] == ["fr_docket_links.parquet"]


def test_reseal_refuses_to_overwrite_an_existing_directory(tmp_path: Path) -> None:
    source = tmp_path / "one"
    _dataset(source, links=["x"])
    output = tmp_path / "resealed"
    output.mkdir()
    with pytest.raises(MODULE.ResealError, match="already exists"):
        MODULE.reseal_segmentation_dataset(source, output, allow_changed=LINKS, validator=_passing_validator)


def test_reseal_fails_closed_when_a_sealed_member_is_missing(tmp_path: Path) -> None:
    source = tmp_path / "one"
    _dataset(source, links=["x"])
    (source / "fr_docket_links.parquet").unlink()
    with pytest.raises(MODULE.ResealError, match="fr_docket_links.parquet"):
        MODULE.reseal_segmentation_dataset(
            source, tmp_path / "resealed", allow_changed=LINKS, validator=_passing_validator
        )


def test_reseal_fails_closed_and_leaves_nothing_behind_when_validation_fails(tmp_path: Path) -> None:
    source = tmp_path / "one"
    _dataset(source, links=["x"])
    output = tmp_path / "resealed"

    def _failing(directory: Path) -> dict[str, Any]:
        return {"status": "fail", "failures": ["coverage gap"]}

    with pytest.raises(MODULE.ResealError, match="coverage gap"):
        MODULE.reseal_segmentation_dataset(source, output, validator=_failing)
    assert not output.exists()


def test_reseal_writes_the_receipt_the_validator_returned(tmp_path: Path) -> None:
    source = tmp_path / "one"
    _dataset(source, links=["x"])
    output = tmp_path / "resealed"
    MODULE.reseal_segmentation_dataset(source, output, allow_changed=LINKS, validator=_passing_validator)
    receipt = json.loads((output / MODULE.RECEIPT_NAME).read_text(encoding="utf-8"))
    assert receipt["status"] == "pass"
    assert receipt["evaluation_id"] == MODULE.evaluation_id(MODULE.dataset_members(output))


# --------------------------------------------------------------------------
# fail closed on an unlicensed change
# --------------------------------------------------------------------------


def test_reseal_refuses_a_changed_member_that_is_not_allowlisted(tmp_path: Path) -> None:
    """The whole point: a re-seal must never launder a measurement input.

    Rewriting gold_spans and re-sealing would otherwise produce an
    honest-looking identity with a passing receipt over changed evidence.
    """
    source = tmp_path / "one"
    _dataset(source, links=["x"])
    _write_parquet(source / "gold_spans.parquet", ["a", "b", "TAMPERED"])

    with pytest.raises(MODULE.ResealError, match="gold_spans.parquet"):
        MODULE.reseal_segmentation_dataset(source, tmp_path / "resealed", validator=_passing_validator)


def test_reseal_refuses_an_unlicensed_change_even_when_another_is_allowed(tmp_path: Path) -> None:
    source = tmp_path / "one"
    _dataset(source, links=["x"])
    _write_parquet(source / "fr_docket_links.parquet", ["x", "y"])
    _write_parquet(source / "gold_spans.parquet", ["a", "b", "TAMPERED"])

    with pytest.raises(MODULE.ResealError, match="gold_spans.parquet"):
        MODULE.reseal_segmentation_dataset(
            source, tmp_path / "resealed", allow_changed=LINKS, validator=_passing_validator
        )


def test_reseal_with_no_allowlist_refuses_any_change_at_all(tmp_path: Path) -> None:
    source = tmp_path / "one"
    _dataset(source, links=["x"])
    _write_parquet(source / "fr_docket_links.parquet", ["x", "y"])
    with pytest.raises(MODULE.ResealError, match="fr_docket_links.parquet"):
        MODULE.reseal_segmentation_dataset(source, tmp_path / "resealed", validator=_passing_validator)


def test_reseal_leaves_nothing_behind_when_it_refuses_a_change(tmp_path: Path) -> None:
    source = tmp_path / "one"
    _dataset(source, links=["x"])
    _write_parquet(source / "gold_spans.parquet", ["a", "b", "TAMPERED"])
    output = tmp_path / "resealed"
    with pytest.raises(MODULE.ResealError):
        MODULE.reseal_segmentation_dataset(source, output, validator=_passing_validator)
    assert not output.exists()


def test_cli_default_allowlist_covers_only_excluded_source_tables() -> None:
    """The CLI's default licence is exactly 'tables the segmenter cannot read'."""
    changed = ["fr_docket_links.parquet", "comments_index.parquet", "gold_spans.parquet"]
    allowed = MODULE.default_allowed_changes(changed)
    assert set(allowed) == {"fr_docket_links.parquet", "comments_index.parquet"}
    assert "gold_spans.parquet" not in allowed
