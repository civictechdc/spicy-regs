"""Actual-file gates for the SpicyRegs DocumentRelease pipeline."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import spicy_regs.document_file_pipeline as file_pipeline_module

from spicy_regs.document_file_pipeline import (
    DEFAULT_FILE_MANIFEST_PATH,
    DocumentFilePipelineError,
    build_document_release_from_file_manifest,
    build_document_release_from_source_cache,
    publish_document_release_from_file_manifest,
    validate_document_release_distribution,
)
from spicy_regs.document_release import (
    DEFAULT_RULESPEC_CORE_PATH,
    DocumentReleaseError,
    canonical_digest,
    canonical_json,
    seal_document_release,
    stable_record_id,
    validate_document_release,
)
from spicy_regs.transforms.pdf_text import PdfTextResult, PdfTextStatus


SAMPLE_ROOT = Path("sample-data/mirrulations")
SAMPLE_JSON = SAMPLE_ROOT / "document-ACF-2025-0038-0001.json"
SAMPLE_PDF = SAMPLE_ROOT / "document-ACF-2025-0038-0001_content.pdf"
SOURCE_CACHE = Path("output/segmentation-source-cache-v2")
REPRESENTATIVE_MARKUP_MANIFEST = (
    Path("sample-data/document-files/document-release-representative-manifest-v1.json")
)
GAO_NORMALIZED_HTML = Path("sample-data/document-files/gao-html-3.html")


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def test_checked_in_regulations_files_reach_a_valid_document_release() -> None:
    release = build_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH)
    validate_document_release(release)

    assert len(release["source_record_versions"]) == 1
    assert len(release["document_versions"]) == 1
    assert len(release["source_renditions"]) == 2
    assert release["format_version"] == "spicyregs-document-release/v2"
    assert release["release_status"] == "conformance"
    assert {item["observed_at"] for item in release["source_rendition_captures"]} == {
        "2025-10-27T00:48:19-04:00"
    }
    assert release["policies"] == {
        "document_eligibility": "spicyregs-captured-documents/v2",
        "passage_generation": ["spicyregs-pdf-page-text-passages/v1"],
    }
    assert "source_fixture" not in release
    assert release["source_input"]["input_type"] == "CapturedFileManifest"
    selection_digest = _sha256(DEFAULT_FILE_MANIFEST_PATH)
    assert release["acquisition_coverage"]["requested_sources"] == [
        f"regulations.gov:documents#selection={selection_digest}"
    ]

    renditions = {item["media_type"]: item for item in release["source_renditions"]}
    assert renditions["application/json"]["bytes_digest"] == _sha256(SAMPLE_JSON)
    assert renditions["application/pdf"]["bytes_digest"] == _sha256(SAMPLE_PDF)

    representation = release["text_representations"][0]
    document = release["document_versions"][0]
    assert document["content_digest"] == renditions["application/pdf"]["bytes_digest"]
    assert document["artifact_projection"]["media_type"] == "application/pdf"
    assert document["artifact_projection"]["coordinate_system"] == "source-bytes"
    assert representation["source_rendition_ref"] == renditions["application/pdf"]["rendition_id"]
    assert representation["evidence_grade"] == "parser-derived"
    assert representation["method"] == "pypdf"
    assert len(representation["unicode_text"]) > 20_000
    assert "Indian Child Welfare Act" in representation["unicode_text"]

    passages = release["structural_passages"]
    assert len(passages) == 4
    assert all(item["text_representation_ref"] == representation["representation_id"] for item in passages)
    assert all(
        representation["unicode_text"][item["start"] : item["end"]].strip()
        for item in passages
    )


def test_checked_in_actual_html_and_xml_are_searchable_and_addressable() -> None:
    release = build_document_release_from_file_manifest(REPRESENTATIVE_MARKUP_MANIFEST)
    validate_document_release(release)

    assert len(release["document_versions"]) == 2
    assert {item["artifact_projection"]["media_type"] for item in release["document_versions"]} == {
        "application/xml",
        "text/html",
    }
    assert release["policies"]["passage_generation"] == [
        "spicyregs-visible-native-markup-passages/v1"
    ]
    assert all(item["evidence_grade"] == "source-exact" for item in release["text_representations"])
    assert all(item["method"] == "raw-utf8" for item in release["text_representations"])
    assert {item["observed_at"] for item in release["source_rendition_captures"]} == {
        "2026-07-24"
    }
    source_records = {
        item["source_record_id"]: item for item in release["source_record_versions"]
    }
    assert source_records["109-s-2977"]["content"]["edition"] == "is"
    assert source_records["CFR-2025-title30-vol3-sec716-2"]["content"]["citation"] == "30 CFR 716.2"
    document_versions = {
        item["source_record_id"]: item for item in release["document_versions"]
    }
    assert document_versions["109-s-2977"]["source_issued_version_id"] == "BILLS-109s2977is"
    assert (
        document_versions["CFR-2025-title30-vol3-sec716-2"]["source_issued_version_id"]
        == "CFR-2025-title30-vol3-sec716-2"
    )

    representations = {
        item["document_version_ref"]: item for item in release["text_representations"]
    }
    passages: dict[str, list[dict]] = {}
    for passage in release["structural_passages"]:
        passages.setdefault(passage["text_representation_ref"], []).append(passage)
    expected = {
        "109-s-2977": "bench grinders",
        "CFR-2025-title30-vol3-sec716-2": "Steep-slope mining",
    }
    for document in release["document_versions"]:
        representation = representations[document["document_version_id"]]
        phrase = expected[document["source_record_id"]]
        assert phrase.casefold() in representation["unicode_text"].casefold()
        assert any(
            phrase.casefold()
            in representation["unicode_text"][passage["start"] : passage["end"]].casefold()
            for passage in passages[representation["representation_id"]]
        )


def test_real_gao_html_passages_keep_report_content_and_drop_site_furniture() -> None:
    assert _sha256(GAO_NORMALIZED_HTML) == (
        "sha256:fe43fca7a7efdc47dd46442e07585b4ebe34f6b1a96d8770fe3db10097ccd6f9"
    )
    text = GAO_NORMALIZED_HTML.read_text(encoding="utf-8")

    spans = file_pipeline_module.native_structural_passage_spans(
        "gao_reports.full_text",
        text,
        media_type="text/html",
    )
    pieces = [text[start:end] for start, end in spans]
    selected = "\n".join(pieces)

    assert len(spans) == 249
    assert max(end - start for start, end in spans) == 2659
    assert "The U.S. Army Corps of Engineers enters into" in selected
    assert "Project Partnership Agreements" in selected
    assert all(
        marker not in selected.casefold()
        for marker in ("<script", "<style", "<noscript", "<nav", "<footer", "<!--")
    )
    assert all(any(character.isalnum() for character in piece) for piece in pieces)


def test_parser_output_change_does_not_create_a_new_source_document_version(monkeypatch) -> None:
    original = build_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH)
    real_extract = file_pipeline_module.extract_pdf_text

    def changed_extract(payload: bytes) -> PdfTextResult:
        result = real_extract(payload)
        pages = result.pages[:-1] + (result.pages[-1] + "\nParser revision marker.",)
        return PdfTextResult(
            status=result.status,
            text=result.text + "\nParser revision marker.",
            page_count=result.page_count,
            error=result.error,
            pages=pages,
        )

    monkeypatch.setattr(file_pipeline_module, "extract_pdf_text", changed_extract)
    changed = build_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH)

    assert original["document_versions"] == changed["document_versions"]
    assert original["text_representations"] != changed["text_representations"]
    assert original["structural_passages"] != changed["structural_passages"]
    assert original["release_id"] != changed["release_id"]


def test_pdf_page_output_must_close_against_the_combined_text(monkeypatch) -> None:
    real_extract = file_pipeline_module.extract_pdf_text

    def inconsistent_extract(payload: bytes, **options: object) -> PdfTextResult:
        result = real_extract(payload, **options)
        return PdfTextResult(
            status=result.status,
            text=result.text + "not present in pages",
            page_count=result.page_count,
            error=result.error,
            pages=result.pages,
            failed_page_ordinals=result.failed_page_ordinals,
        )

    monkeypatch.setattr(file_pipeline_module, "extract_pdf_text", inconsistent_extract)

    with pytest.raises(DocumentFilePipelineError, match="does not close against page text"):
        build_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH)


def test_pdf_page_parser_failures_fail_the_file_release(monkeypatch) -> None:
    real_extract = file_pipeline_module.extract_pdf_text

    def partial_extract(payload: bytes, **options: object) -> PdfTextResult:
        result = real_extract(payload, **options)
        return PdfTextResult(
            status=PdfTextStatus.OK,
            text=result.text,
            page_count=result.page_count,
            error=None,
            pages=result.pages,
            failed_page_ordinals=(1,),
        )

    monkeypatch.setattr(file_pipeline_module, "extract_pdf_text", partial_extract)

    with pytest.raises(DocumentFilePipelineError, match="failed page ordinals"):
        build_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH)


def test_published_distribution_contains_every_exact_source_rendition(tmp_path: Path) -> None:
    distribution = tmp_path / "document-release"
    release = publish_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH, distribution)
    assert validate_document_release_distribution(distribution) == release

    assert (distribution / "document-release.json").is_file()
    for rendition in release["source_renditions"]:
        source_path = distribution / rendition["source_native_path"]
        assert source_path.is_file()
        assert _sha256(source_path) == rendition["bytes_digest"]
    receipt_refs = {
        item["retrieval_receipt_ref"] for item in release["source_rendition_captures"]
    }
    assert len(receipt_refs) == 1
    for receipt_ref in receipt_refs:
        receipt_path = distribution / receipt_ref
        assert receipt_path.is_file()
        assert hashlib.sha256(receipt_path.read_bytes()).hexdigest() == receipt_path.stem


def test_distribution_validator_rejects_changed_published_bytes(tmp_path: Path) -> None:
    distribution = tmp_path / "document-release"
    release = publish_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH, distribution)
    rendition_path = distribution / release["source_renditions"][0]["source_native_path"]
    rendition_path.write_bytes(rendition_path.read_bytes() + b"tampered")

    with pytest.raises(DocumentFilePipelineError, match="rendition bytes digest differs"):
        validate_document_release_distribution(distribution)


def test_distribution_validator_rejects_text_not_reproduced_from_pinned_pdf(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    distribution = tmp_path / "document-release"
    publish_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH, distribution)
    real_extract = file_pipeline_module.extract_pdf_text

    fabricated_text = "Fabricated passage unrelated to the captured PDF."

    def fabricated_extract(payload: bytes, **options: object) -> PdfTextResult:
        del payload, options
        return PdfTextResult(
            status=PdfTextStatus.OK,
            text=fabricated_text,
            page_count=1,
            pages=(fabricated_text,),
        )

    monkeypatch.setattr(file_pipeline_module, "extract_pdf_text", fabricated_extract)
    fabricated_release = build_document_release_from_file_manifest(
        DEFAULT_FILE_MANIFEST_PATH
    )
    monkeypatch.setattr(file_pipeline_module, "extract_pdf_text", real_extract)
    (distribution / "document-release.json").write_text(
        canonical_json(fabricated_release) + "\n"
    )

    with pytest.raises(
        DocumentFilePipelineError,
        match="not reproducible from its captured manifest and renditions",
    ):
        validate_document_release_distribution(distribution)


def test_distribution_resolves_the_source_input_digest_to_published_bytes(tmp_path: Path) -> None:
    distribution = tmp_path / "document-release"
    publish_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH, distribution)
    release_path = distribution / "document-release.json"
    release = json.loads(release_path.read_text())
    body = {key: value for key, value in release.items() if key not in {"release_id", "release_digest"}}
    body["source_input"]["input_digest"] = "sha256:" + "0" * 64
    changed = seal_document_release(body)
    release_path.write_text(canonical_json(changed) + "\n")

    with pytest.raises(DocumentReleaseError, match="not bound to the source input"):
        validate_document_release_distribution(distribution)


def test_requested_source_selection_must_match_the_resolved_input(tmp_path: Path) -> None:
    distribution = tmp_path / "document-release"
    publish_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH, distribution)
    release_path = distribution / "document-release.json"
    release = json.loads(release_path.read_text())
    body = {key: value for key, value in release.items() if key not in {"release_id", "release_digest"}}
    acquisition = body["acquisition_coverage"]
    acquisition["requested_sources"] = [
        "regulations.gov:documents#selection=sha256:" + "0" * 64
    ]
    identity = {
        "capture_refs": acquisition["capture_refs"],
        "entries": acquisition["entries"],
        "policy_version": acquisition["policy_version"],
        "requested_sources": acquisition["requested_sources"],
    }
    acquisition["coverage_id"] = stable_record_id("acquisition-coverage", identity)
    release_path.write_text(canonical_json(seal_document_release(body)) + "\n")

    with pytest.raises(DocumentReleaseError, match="selection differs from the source input"):
        validate_document_release_distribution(distribution)


def test_source_input_id_must_match_the_resolved_manifest(tmp_path: Path) -> None:
    distribution = tmp_path / "document-release"
    publish_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH, distribution)
    release_path = distribution / "document-release.json"
    release = json.loads(release_path.read_text())
    body = {key: value for key, value in release.items() if key not in {"release_id", "release_digest"}}
    body["source_input"]["input_id"] = "unbacked-input-id"
    release_path.write_text(canonical_json(seal_document_release(body)) + "\n")

    with pytest.raises(DocumentFilePipelineError, match="ID differs from the captured manifest"):
        validate_document_release_distribution(distribution)


def test_every_requested_source_requires_a_coverage_entry(tmp_path: Path) -> None:
    distribution = tmp_path / "document-release"
    publish_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH, distribution)
    release_path = distribution / "document-release.json"
    release = json.loads(release_path.read_text())
    body = {key: value for key, value in release.items() if key not in {"release_id", "release_digest"}}
    acquisition = body["acquisition_coverage"]
    selection = body["source_input"]["input_digest"]
    acquisition["requested_sources"].append(f"cfr:sections#selection={selection}")
    acquisition["requested_sources"].sort()
    identity = {
        "capture_refs": acquisition["capture_refs"],
        "entries": acquisition["entries"],
        "policy_version": acquisition["policy_version"],
        "requested_sources": acquisition["requested_sources"],
    }
    acquisition["coverage_id"] = stable_record_id("acquisition-coverage", identity)
    release_path.write_text(canonical_json(seal_document_release(body)) + "\n")

    with pytest.raises(DocumentReleaseError, match="every requested source"):
        validate_document_release_distribution(distribution)


def test_file_manifest_fails_closed_when_exact_bytes_change(tmp_path: Path) -> None:
    copied_pdf = tmp_path / SAMPLE_PDF.name
    copied_pdf.write_bytes(SAMPLE_PDF.read_bytes() + b"tampered")

    with pytest.raises(DocumentFilePipelineError, match="bytes digest differs"):
        build_document_release_from_file_manifest(
            DEFAULT_FILE_MANIFEST_PATH,
            file_overrides={SAMPLE_PDF.name: copied_pdf},
        )


def test_file_manifest_rejects_an_invalid_capture_date(tmp_path: Path) -> None:
    manifest = json.loads(REPRESENTATIVE_MARKUP_MANIFEST.read_text())
    manifest["documents"][0]["captures"][0]["observed_at"] = "2026-02-30"
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(canonical_json(manifest) + "\n")

    with pytest.raises(DocumentFilePipelineError, match="observed_at must be an exact ISO date"):
        build_document_release_from_file_manifest(
            manifest_path,
            file_overrides={
                "bill-html-short.html": Path("sample-data/document-files/bill-html-short.html"),
                "cfr-xml-short.xml": Path("sample-data/document-files/cfr-xml-short.xml"),
            },
        )


@pytest.mark.skipif(not (SOURCE_CACHE / "source-lock.json").is_file(), reason="local real-file cache is absent")
def test_evaluation_source_lock_cannot_be_promoted_by_a_rulespec_core_status(
    tmp_path: Path,
) -> None:
    core = json.loads(DEFAULT_RULESPEC_CORE_PATH.read_text())
    core["release_status"] = "published"
    core_body = {
        key: value
        for key, value in core.items()
        if key not in {"release_id", "release_digest"}
    }
    core["release_digest"] = canonical_digest(core_body)
    core["release_id"] = (
        "urn:rulespec:core:" + core["release_digest"].removeprefix("sha256:")
    )
    core_path = tmp_path / "rulespec-core-published.json"
    core_path.write_text(canonical_json(core) + "\n")

    release = build_document_release_from_source_cache(
        SOURCE_CACHE,
        released_at="2026-08-01T00:00:00Z",
        rulespec_core_path=core_path,
    )

    assert release["source_input"]["input_type"] == "EvaluationSourceLock"
    assert release["release_status"] == "conformance"


@pytest.mark.skipif(not (SOURCE_CACHE / "source-lock.json").is_file(), reason="local real-file cache is absent")
def test_local_real_file_corpus_is_addressable_across_every_source_family() -> None:
    from spicy_regs.corpora.segmentation_evaluation import FULL_DOCUMENT_SPECS

    release = build_document_release_from_source_cache(
        SOURCE_CACHE,
        released_at="2026-08-01T00:00:00Z",
    )
    documents = {item["source_record_id"]: item for item in release["document_versions"]}
    representations = {
        item["document_version_ref"]: item for item in release["text_representations"]
    }
    passages_by_representation: dict[str, list[dict]] = {}
    for passage in release["structural_passages"]:
        passages_by_representation.setdefault(passage["text_representation_ref"], []).append(passage)

    assert len(documents) == len(FULL_DOCUMENT_SPECS) == 34
    assert {item["publisher"] for item in documents.values()} == {
        "cfr",
        "congress",
        "congressional-research-service",
        "federal-register",
        "gao",
        "regulations.gov",
        "supreme-court",
    }
    assert sum(item["evidence_grade"] == "parser-derived" for item in representations.values()) == 18
    assert sum(item["evidence_grade"] == "source-exact" for item in representations.values()) == 16

    lock = file_pipeline_module.json.loads((SOURCE_CACHE / "source-lock.json").read_text())
    lock_by_case = {item["case_id"]: item for item in lock["sources"]}
    specs_by_record_id = {item.key_value: item for item in FULL_DOCUMENT_SPECS}
    for record_id, document in documents.items():
        spec = specs_by_record_id[record_id]
        locked = lock_by_case[spec.case_id]
        representation = representations[document["document_version_id"]]
        assert len(representation["unicode_text"]) == locked["extracted_chars"]
        assert representation["text_digest"] == "sha256:" + locked["extracted_sha256"]
        assert representation["method"] == locked["extraction_method"]
        assert representation["method_version"] == locked["extraction_version"]

    missing: list[str] = []
    unaddressable: list[str] = []
    for spec in FULL_DOCUMENT_SPECS:
        representation = representations[documents[spec.key_value]["document_version_id"]]
        phrase = spec.gold_phrase.casefold()
        if phrase not in representation["unicode_text"].casefold():
            missing.append(spec.case_id)
            continue
        if not any(
            phrase
            in representation["unicode_text"][passage["start"] : passage["end"]].casefold()
            for passage in passages_by_representation[representation["representation_id"]]
        ):
            unaddressable.append(spec.case_id)
    assert missing == []
    assert unaddressable == []
