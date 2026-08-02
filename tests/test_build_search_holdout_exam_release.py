"""Hermetic checks for the sealed search-holdout exam DocumentRelease builder.

Every fixture here is synthetic. Nothing reads the real ontology snapshot or
the real sealed draw; the tests state what the tool guarantees: it covers
exactly the drawn matters' Federal Register documents, composes the sealed
text deterministically, fails closed on malformed source facts, and produces
a source fixture the immutable ``document_release`` machinery accepts
byte-for-byte reproducibly.

Release machinery origin: ``src/spicy_regs/document_release.py`` (commit
a388cd0, immutable DocumentRelease). The tool builds that module's source
fixture format from the sealed draw's ``fr_documents`` membership.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = REPO_ROOT / "tools" / "build_search_holdout_exam_release.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("build_search_holdout_exam_release", TOOL_PATH)
    assert spec and spec.loader, f"could not load {TOOL_PATH}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


mod = _load_module()

ExamReleaseError = mod.ExamReleaseError


def _row(number: str, **overrides):
    row = {
        "document_number": number,
        "title": f"Sealed Exam Title {number}",
        "abstract": f"Sealed exam abstract for {number}.",
        "document_type": "Notice",
        "publication_date": "2020-06-15",
        "effective_on": None,
        "comments_close_on": None,
        "agencies_json": json.dumps([{"raw_name": "TEST AGENCY", "name": "Test Agency", "id": 1}]),
        "docket_ids_json": json.dumps([f"TEST-{number}"]),
        "regulation_id_numbers_json": json.dumps([]),
        "topics_json": json.dumps(["Test topic"]),
        "html_url": f"https://example.test/d/{number}",
    }
    row.update(overrides)
    return row


def _manifest(matters):
    return {
        "schema_version": "search-holdout-draw-v1",
        "matter_total": len(matters),
        "matters": matters,
        "holdout": {"dataset_id": "search-holdout-matters-test-v1"},
    }


def _matter(matter_id: str, fr_documents: list[str]):
    return {
        "matter_id": matter_id,
        "fr_documents": fr_documents,
        "dockets": [],
        "proceedings": [],
        "rins": [],
        "node_count": max(len(fr_documents), 1),
        "size_bucket": "small",
        "source_class": "fr-only" if fr_documents else "proc-only",
        "era_bucket": "2018-2022",
    }


def test_drawn_documents_are_the_union_of_matter_memberships():
    manifest = _manifest(
        [
            _matter("m1", ["2020-11111", "2020-22222"]),
            _matter("m2", ["2020-22222", "2020-33333"]),
            _matter("m3", []),
        ]
    )
    drawn = mod.drawn_fr_documents(manifest)
    assert drawn == ("2020-11111", "2020-22222", "2020-33333")


def test_fixture_covers_exactly_the_drawn_documents_and_is_deterministic():
    numbers = ["2020-11111", "2020-22222"]
    manifest = _manifest([_matter("m1", numbers)])
    rows = {n: _row(n) for n in numbers}
    fixture_a = mod.build_exam_source_fixture(manifest, rows)
    fixture_b = mod.build_exam_source_fixture(manifest, rows)
    assert fixture_a == fixture_b
    assert fixture_a["fixture_digest"] == fixture_b["fixture_digest"]
    keys = [record["key"] for record in fixture_a["records"]]
    assert keys == [f"fr-{n}" for n in numbers]
    assert fixture_a["links"] == []
    assert fixture_a["requested_sources"] == ["federal-register:documents"]


def test_sealed_text_is_title_then_abstract_with_exact_passage_offsets():
    manifest = _manifest([_matter("m1", ["2020-11111"])])
    rows = {"2020-11111": _row("2020-11111", title="A Title", abstract="An abstract.")}
    fixture = mod.build_exam_source_fixture(manifest, rows)
    record = fixture["records"][0]
    text = record["content"]["text"]
    assert text == "A Title\n\nAn abstract."
    title_passage, abstract_passage = record["passages"]
    assert text[title_passage["start"] : title_passage["end"]] == "A Title"
    assert title_passage["expected_text"] == "A Title"
    assert text[abstract_passage["start"] : abstract_passage["end"]] == "An abstract."
    assert record["document"]["content_path"] == "text"


def test_missing_abstract_seals_title_only_text():
    manifest = _manifest([_matter("m1", ["2020-11111"])])
    rows = {"2020-11111": _row("2020-11111", title="Only Title", abstract=None)}
    fixture = mod.build_exam_source_fixture(manifest, rows)
    record = fixture["records"][0]
    assert record["content"]["text"] == "Only Title"
    assert "abstract" not in record["content"]
    assert [p["expected_text"] for p in record["passages"]] == ["Only Title"]


def test_optional_metadata_is_parsed_and_empty_collections_are_omitted():
    manifest = _manifest([_matter("m1", ["2020-11111"])])
    rows = {
        "2020-11111": _row(
            "2020-11111",
            topics_json=json.dumps([]),
            docket_ids_json=json.dumps(["EPA-HQ-1"]),
            effective_on="2020-07-01",
        )
    }
    fixture = mod.build_exam_source_fixture(manifest, rows)
    content = fixture["records"][0]["content"]
    assert "topics" not in content
    assert content["docket_ids"] == ["EPA-HQ-1"]
    assert content["agencies"] == ["Test Agency"]
    assert content["effective_on"] == "2020-07-01"


def test_missing_row_for_a_drawn_document_fails_closed():
    manifest = _manifest([_matter("m1", ["2020-11111", "2020-99999"])])
    rows = {"2020-11111": _row("2020-11111")}
    with pytest.raises(ExamReleaseError, match="2020-99999"):
        mod.build_exam_source_fixture(manifest, rows)


def test_malformed_publication_date_fails_closed():
    manifest = _manifest([_matter("m1", ["2020-11111"])])
    rows = {"2020-11111": _row("2020-11111", publication_date="June 15, 2020")}
    with pytest.raises(ExamReleaseError, match="publication_date"):
        mod.build_exam_source_fixture(manifest, rows)


def test_empty_title_fails_closed():
    manifest = _manifest([_matter("m1", ["2020-11111"])])
    rows = {"2020-11111": _row("2020-11111", title="")}
    with pytest.raises(ExamReleaseError, match="title"):
        mod.build_exam_source_fixture(manifest, rows)


def test_manifest_without_any_fr_document_fails_closed():
    manifest = _manifest([_matter("m1", [])])
    with pytest.raises(ExamReleaseError, match="no Federal Register documents"):
        mod.build_exam_source_fixture(manifest, {})


def test_fixture_is_accepted_by_the_immutable_release_machinery():
    from spicy_regs import document_release

    numbers = ["2020-11111", "2020-22222"]
    manifest = _manifest([_matter("m1", numbers)])
    rows = {n: _row(n) for n in numbers}
    fixture = mod.build_exam_source_fixture(manifest, rows)
    release_a = mod.build_release_from_fixture(fixture)
    release_b = mod.build_release_from_fixture(fixture)
    assert release_a["release_digest"] == release_b["release_digest"]
    assert release_a["record_type"] == "DocumentRelease"
    assert len(release_a["document_versions"]) == 2
    document_release.validate_document_release(release_a)


def test_sealed_manifest_digest_verification_fails_closed(tmp_path):
    manifest = _manifest([_matter("m1", ["2020-11111"])])
    path = tmp_path / "sealed-manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    loaded = mod.load_sealed_manifest(path, expected_sha256=actual)
    assert loaded["matter_total"] == 1
    with pytest.raises(ExamReleaseError, match="digest"):
        mod.load_sealed_manifest(path, expected_sha256="0" * 64)
