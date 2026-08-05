"""Focused fake-S3 checks for the permanent Mirrulations file bridge."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from spicy_regs.corpora import mirrulations_document_corpus as corpus

BUCKET = "mirrulations"
PREFIX = "raw-data/SEC/SEC-202"
STAMP = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)


class _Body:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.closed = False

    def read(self, amount: int | None = None) -> bytes:
        return self.payload if amount is None else self.payload[:amount]

    def close(self) -> None:
        self.closed = True


class _Object:
    def __init__(self, store: dict[str, bytes], metadata: dict[str, dict[str, Any]], key: str, gets: list[str]) -> None:
        self.store = store
        self.metadata = metadata
        self.key = key
        self.gets = gets

    def get(self, **kwargs: Any) -> dict[str, Any]:
        self.gets.append(self.key)
        expected = self.metadata[self.key]["ETag"]
        assert kwargs.get("IfMatch") == expected
        payload = self.store[self.key]
        return {
            "Body": _Body(payload),
            "ETag": expected,
            "LastModified": self.metadata[self.key]["LastModified"],
            "ContentLength": len(payload),
        }


class _Resource:
    def __init__(self, store: dict[str, bytes], metadata: dict[str, dict[str, Any]]) -> None:
        self.store = store
        self.metadata = metadata
        self.gets: list[str] = []

    def Object(self, bucket: str, key: str) -> _Object:  # noqa: N802 - boto3 API
        assert bucket == BUCKET
        return _Object(self.store, self.metadata, key, self.gets)


def _keys(document_id: str, *, revision: int = 0, extension: str = "htm") -> tuple[str, str]:
    docket = "-".join(document_id.split("-")[:3])
    base = f"{PREFIX}/{docket}/text-{docket}/documents"
    suffix = f"({revision})" if revision else ""
    return f"{base}/{document_id}{suffix}.json", f"{base}/{document_id}_content.{extension}"


def _document_bytes(document_id: str, html: bytes, *, title: str | None = None, extension: str = "htm") -> bytes:
    return json.dumps(
        {
            "data": {
                "id": document_id,
                "type": "documents",
                "links": {"self": f"https://api.regulations.gov/v4/documents/{document_id}"},
                "attributes": {
                    "agencyId": "SEC",
                    "docketId": None,
                    "title": title or f"Title {document_id}",
                    "documentType": "Notice",
                    "postedDate": "2025-01-02T05:00:00Z",
                    "modifyDate": "2025-01-03T05:00:00Z",
                    "fileFormats": [
                        {
                            "fileUrl": f"https://downloads.regulations.gov/{document_id}/content.{extension}",
                            "format": extension,
                            "size": len(html),
                        }
                    ],
                },
            }
        },
        sort_keys=True,
    ).encode()


def _etag(payload: bytes) -> str:
    return f'"{hashlib.md5(payload, usedforsecurity=False).hexdigest()}"'  # noqa: S324 - fake S3 ETag


def _inventory(count: int = 5) -> tuple[list[dict[str, Any]], dict[str, bytes], dict[str, dict[str, Any]]]:
    store: dict[str, bytes] = {}
    metadata: dict[str, dict[str, Any]] = {}
    for ordinal in range(1, count + 1):
        document_id = f"SEC-2025-{ordinal:04d}-0001"
        extension = "html" if ordinal == 2 else "htm"
        html = f"<html><body>Actual document {ordinal}</body></html>".encode()
        json_key, html_key = _keys(document_id, extension=extension)
        store[json_key] = _document_bytes(document_id, html, extension=extension)
        store[html_key] = html
    for key, payload in store.items():
        metadata[key] = {"Key": key, "Size": len(payload), "ETag": _etag(payload), "LastModified": STAMP}
    return list(metadata.values()), store, metadata


def _draw_and_write(tmp_path: Path, count: int = 5) -> tuple[Path, dict[str, bytes], _Resource]:
    objects, store, metadata = _inventory(count)
    manifest = corpus.build_draw(objects, max_documents=count)
    draw = tmp_path / "draw.json"
    corpus.write_draw(draw, manifest)
    return draw, store, _Resource(store, metadata)


def test_draw_pairs_documents_excludes_unpaired_and_uses_highest_json_revision() -> None:
    objects, store, metadata = _inventory(3)
    first_id = "SEC-2025-0001-0001"
    revision_key, _ = _keys(first_id, revision=2)
    old_key, _ = _keys(first_id)
    revised = _document_bytes(first_id, store[_keys(first_id)[1]], title="Revised")
    store[revision_key] = revised
    metadata[revision_key] = {
        "Key": revision_key,
        "Size": len(revised),
        "ETag": _etag(revised),
        "LastModified": STAMP,
    }
    unpaired_id = "SEC-2025-9999-0001"
    unpaired_key, _ = _keys(unpaired_id)
    unpaired = _document_bytes(unpaired_id, b"<p>not mirrored</p>")
    metadata[unpaired_key] = {
        "Key": unpaired_key,
        "Size": len(unpaired),
        "ETag": _etag(unpaired),
        "LastModified": STAMP,
    }
    tombstone_key = old_key.removesuffix(".json") + "_UNAVAILABLE.json"
    metadata[tombstone_key] = {
        "Key": tombstone_key,
        "Size": 0,
        "ETag": '"empty"',
        "LastModified": STAMP,
    }

    manifest = corpus.build_draw(metadata.values(), max_documents=3)

    assert [item["document_id"] for item in manifest["documents"]] == [
        "SEC-2025-0001-0001",
        "SEC-2025-0002-0001",
        "SEC-2025-0003-0001",
    ]
    assert manifest["documents"][0]["metadata_object"]["key"] == revision_key
    assert manifest["documents"][0]["json_revision"] == 2
    assert manifest["counts"]["unpaired_json_documents"] == 1
    assert manifest["counts"]["superseded_json_revisions"] == 1
    assert manifest["counts"]["selected_superseded_json_revisions"] == 1
    assert manifest["counts"]["excluded_tombstones"] == 1


def test_fetch_preserves_exact_bytes_metadata_and_resumes_without_gets(tmp_path: Path) -> None:
    draw, store, resource = _draw_and_write(tmp_path, 3)
    cache = tmp_path / "cache"

    result = corpus.fetch_pairs(
        draw,
        cache,
        resource=resource,
        workers=3,
        retrieved_at="2026-08-05T12:30:00Z",
    )

    assert result["sealed_pairs"] == 3
    assert len(resource.gets) == 6
    receipt = json.loads((cache / "receipts" / "SEC-2025-0001-0001.json").read_text())
    metadata_bytes = (cache / receipt["metadata"]["cache_file"]).read_bytes()
    rendition_bytes = (cache / receipt["rendition"]["cache_file"]).read_bytes()
    assert metadata_bytes == store[receipt["metadata"]["source"]["key"]]
    assert rendition_bytes == store[receipt["rendition"]["source"]["key"]]
    assert receipt["metadata"]["source"]["etag"] == _etag(metadata_bytes)
    assert receipt["metadata"]["source"]["last_modified"] == "2026-08-05T12:00:00Z"
    lock = json.loads((cache / "source-lock.json").read_text())
    assert lock["sources"][0]["metadata_object"] == receipt["metadata"]["source"]
    assert lock["sources"][0]["rendition_object"] == receipt["rendition"]["source"]
    assert corpus.validate_cache(draw, cache)["status"] == "pass"

    before = list(resource.gets)
    resumed = corpus.fetch_pairs(draw, cache, resource=resource, workers=3)
    assert resumed["fetched_pairs"] == 0
    assert resumed["skipped_pairs"] == 3
    assert resource.gets == before


def test_incomplete_cache_refuses_selection_then_resumes(tmp_path: Path) -> None:
    draw, _store, resource = _draw_and_write(tmp_path, 5)
    cache = tmp_path / "cache"
    corpus.fetch_pairs(draw, cache, resource=resource, workers=2, max_pairs=2)

    report = corpus.validate_cache(draw, cache)
    assert report["status"] == "fail"
    assert report["verified_documents"] == 2
    with pytest.raises(corpus.MirrulationsCorpusError, match="source cache is incomplete"):
        corpus.write_v3_selection(
            draw,
            cache,
            tmp_path / "selection.jsonl",
            partition_id="part-00",
        )

    resumed = corpus.fetch_pairs(draw, cache, resource=resource, workers=3)
    assert resumed["sealed_pairs"] == 5
    assert resumed["quarantined_pairs"] == 0
    assert corpus.validate_cache(draw, cache)["status"] == "pass"


def test_three_split_ledgers_are_disjoint_complete_and_reference_cached_html(tmp_path: Path) -> None:
    draw, _store, resource = _draw_and_write(tmp_path, 5)
    cache = tmp_path / "cache"
    corpus.fetch_pairs(draw, cache, resource=resource, workers=3)
    all_records: list[dict[str, Any]] = []
    counts: list[int] = []
    partition_inputs: dict[str, Path] = {}
    for index in range(3):
        output = tmp_path / f"part-{index:02d}.jsonl"
        partition_inputs[f"part-{index:02d}"] = output
        summary = corpus.write_v3_selection(
            draw,
            cache,
            output,
            partition_id=f"part-{index:02d}",
            split_count=3,
            split_index=index,
        )
        records = [json.loads(line) for line in output.read_text().splitlines()]
        counts.append(summary["selected_documents"])
        all_records.extend(records)
        assert all(record["sourcePartition"] == f"part-{index:02d}" for record in records)

    assert counts == [2, 2, 1]
    assert len({record["documentId"] for record in all_records}) == 5
    assert all(record["eligibilityState"] == "unverified" for record in all_records)
    assert all(Path(record["renditionPath"]).is_file() for record in all_records)
    assert all("comment" not in record and "firstName" not in record for record in all_records)
    assert all(record["sourceInputId"].startswith("urn:spicyregs:mirrulations-pair:sha256:") for record in all_records)

    from spicy_regs.document_release_v3_verify import verify_release_or_raise
    from spicy_regs.document_release_v3_writer import BuildConfig, build_release_from_partition_jsonl

    config = BuildConfig(
        implementation_id="spicyregs.document-release-v3.test",
        implementation_version="1.0",
        runtime_profile_id="pytest-local-python-3.12",
        source_revision="pytest",
        processing_policy_id="spicyregs.processing.test.v1",
        normalizer_id="spicyregs.normalizer.utf8-identity.v1",
        segmenter_id="spicyregs.segmenter.utf8-bounded.v1",
        rendition_policy_id="spicyregs.rendition.pack.v1",
        eligibility_policy_id="spicyregs.eligibility.test.v1",
        failure_policy_id="spicyregs.failure.test.v1",
        diagnostic_registry_id="spicyregs.diagnostics.test.v1",
        selection_id=json.loads(draw.read_text())["draw_id"],
        selector_type="mirrulations-document-corpus-draw-v1",
        selector_digest=hashlib.sha256(draw.read_bytes()).hexdigest(),
        effective_at="2026-08-05T00:00:00Z",
        partition_id="unused",
        row_batch_size=2,
        row_batch_utf8_bytes=1024,
        max_passage_utf8_bytes=1024,
        max_rendition_pack_bytes=1024 * 1024,
        max_document_bytes=1024 * 1024,
        max_oversized_document_bytes=1024 * 1024,
        build_run_id="pytest-mirrulations",
        created_at="2026-08-05T00:00:00Z",
        build_started_at="2026-08-05T00:00:00Z",
        build_completed_at="2026-08-05T00:00:01Z",
    )
    release = build_release_from_partition_jsonl(partition_inputs, tmp_path / "release", config)
    verified = verify_release_or_raise(release)
    assert verified.counts["partitionManifestCount"] == 3
    assert verified.counts["activeDocumentCount"] == 5
