"""Tests for the Mirrulations derived-data comment-text enrichment.

Covers the S3 fetcher (:class:`DerivedCommentText`), the streaming transform
(:class:`EnrichCommentText`), and the :class:`Chain` combinator, all against a
fake in-memory S3 resource (same shape as ``test_mirrulations_reader``).
"""

from __future__ import annotations

import json

from spicy_regs.sources.derived_text import DerivedCommentText, comments_extracted_prefix
from spicy_regs.transforms import Chain, EnrichCommentText, ExtractRecords
from spicy_regs.transforms.base import Transform

BUCKET = "mirrulations"


# --- fake S3 (mirrors the boto3 resource surface the fetcher uses) -----------


class _FakeBody:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeObj:
    def __init__(self, key: str, content: bytes) -> None:
        self.key = key
        self._content = content

    def get(self) -> dict:
        return {"Body": _FakeBody(self._content)}


class _FakeObjects:
    def __init__(self, store: dict[str, bytes]) -> None:
        self._store = store

    def filter(self, Prefix: str):  # noqa: N803 — mirrors boto3 kwarg
        for key, content in self._store.items():
            if key.startswith(Prefix):
                yield _FakeObj(key, content)


class _FakeBucket:
    def __init__(self, store: dict[str, bytes]) -> None:
        self.objects = _FakeObjects(store)


class _FakeS3Resource:
    def __init__(self, store: dict[str, bytes]) -> None:
        self._store = store

    def Bucket(self, name: str) -> _FakeBucket:  # noqa: N802 — mirrors boto3 API
        return _FakeBucket(self._store)

    def Object(self, name: str, key: str) -> _FakeObj:  # noqa: N802 — mirrors boto3 API
        return _FakeObj(key, self._store[key])


def _extracted_key(agency: str, docket: str, tool: str, filename: str) -> str:
    return comments_extracted_prefix(agency, docket) + f"{tool}/{filename}"


def _store() -> dict[str, bytes]:
    # ACF docket: comment 0004 has one attachment; comment 0015 has two
    # (pypdf tool). EPA docket: comment 0009 uses a different tool (pdfminer).
    return {
        _extracted_key("ACF", "ACF-2025-0038", "pypdf", "ACF-2025-0038-0004_attachment_1_extracted.txt"): b"Wisconsin DCF comment body",
        _extracted_key("ACF", "ACF-2025-0038", "pypdf", "ACF-2025-0038-0015_attachment_1_extracted.txt"): b"first attachment",
        _extracted_key("ACF", "ACF-2025-0038", "pypdf", "ACF-2025-0038-0015_attachment_2_extracted.txt"): b"second attachment",
        # An empty extraction (e.g. a scanned/image PDF) — should not count as text.
        _extracted_key("ACF", "ACF-2025-0038", "pypdf", "ACF-2025-0038-0020_attachment_1_extracted.txt"): b"   \n  ",
        _extracted_key("EPA", "EPA-HQ-OA-2024-0001", "pdfminer", "EPA-HQ-OA-2024-0001-0009_attachment_1_extracted.txt"): b"EPA comment text",
    }


# --- DerivedCommentText ------------------------------------------------------


def test_text_for_single_attachment() -> None:
    fetcher = DerivedCommentText(_FakeS3Resource(_store()), BUCKET)
    assert fetcher.text_for("ACF", "ACF-2025-0038", "ACF-2025-0038-0004") == "Wisconsin DCF comment body"


def test_text_for_concatenates_multiple_attachments_in_order() -> None:
    fetcher = DerivedCommentText(_FakeS3Resource(_store()), BUCKET)
    text = fetcher.text_for("ACF", "ACF-2025-0038", "ACF-2025-0038-0015")
    assert text == "first attachment\n\nsecond attachment"


def test_text_for_tool_subfolder_is_discovered_not_hardcoded() -> None:
    fetcher = DerivedCommentText(_FakeS3Resource(_store()), BUCKET)
    # EPA's docket uses pdfminer rather than pypdf — still found.
    assert fetcher.text_for("EPA", "EPA-HQ-OA-2024-0001", "EPA-HQ-OA-2024-0001-0009") == "EPA comment text"


def test_text_for_empty_extraction_returns_none() -> None:
    fetcher = DerivedCommentText(_FakeS3Resource(_store()), BUCKET)
    assert fetcher.text_for("ACF", "ACF-2025-0038", "ACF-2025-0038-0020") is None


def test_text_for_missing_comment_returns_none() -> None:
    fetcher = DerivedCommentText(_FakeS3Resource(_store()), BUCKET)
    assert fetcher.text_for("ACF", "ACF-2025-0038", "ACF-2025-0038-9999") is None


def test_text_for_handles_missing_identifiers() -> None:
    fetcher = DerivedCommentText(_FakeS3Resource(_store()), BUCKET)
    assert fetcher.text_for(None, "ACF-2025-0038", "ACF-2025-0038-0004") is None
    assert fetcher.text_for("ACF", None, "ACF-2025-0038-0004") is None
    assert fetcher.text_for("ACF", "ACF-2025-0038", None) is None


def test_docket_listing_is_cached() -> None:
    calls = {"n": 0}

    class _CountingObjects(_FakeObjects):
        def filter(self, Prefix: str):  # noqa: N803
            calls["n"] += 1
            return super().filter(Prefix)

    class _CountingBucket(_FakeBucket):
        def __init__(self, store: dict[str, bytes]) -> None:
            self.objects = _CountingObjects(store)

    class _CountingResource(_FakeS3Resource):
        def Bucket(self, name: str) -> _CountingBucket:  # noqa: N802
            return _CountingBucket(self._store)

    fetcher = DerivedCommentText(_CountingResource(_store()), BUCKET)
    fetcher.text_for("ACF", "ACF-2025-0038", "ACF-2025-0038-0004")
    fetcher.text_for("ACF", "ACF-2025-0038", "ACF-2025-0038-0015")
    assert calls["n"] == 1, "same docket should be listed only once"


# --- EnrichCommentText transform --------------------------------------------


def _comment_record(comment_id: str, *, attachments: bool, text: str | None = None) -> dict:
    return {
        "comment_id": comment_id,
        "docket_id": "ACF-2025-0038",
        "agency_code": "ACF",
        "comment": "See attached file(s)",
        "attachments_json": json.dumps([{"title": "x"}]) if attachments else None,
        "text_content": text,
        "text_extraction_status": "ok" if text else None,
    }


def test_enrich_fills_text_content_and_status() -> None:
    fetcher = DerivedCommentText(_FakeS3Resource(_store()), BUCKET)
    records = [_comment_record("ACF-2025-0038-0004", attachments=True)]
    (out,) = list(EnrichCommentText(fetcher).apply(records))
    assert out["text_content"] == "Wisconsin DCF comment body"
    assert out["text_extraction_status"] == "ok"


def test_enrich_skips_comments_without_attachments() -> None:
    fetcher = DerivedCommentText(_FakeS3Resource(_store()), BUCKET)
    # No attachments: even if a stray extraction existed, we don't look it up.
    records = [_comment_record("ACF-2025-0038-0004", attachments=False)]
    (out,) = list(EnrichCommentText(fetcher).apply(records))
    assert out["text_content"] is None
    assert out["text_extraction_status"] is None


def test_enrich_leaves_status_none_when_no_derived_text() -> None:
    # An attachment-bearing comment with no derived-data extraction stays
    # untouched so the PDF-download fallback can backfill it later.
    fetcher = DerivedCommentText(_FakeS3Resource(_store()), BUCKET)
    records = [_comment_record("ACF-2025-0038-9999", attachments=True)]
    (out,) = list(EnrichCommentText(fetcher).apply(records))
    assert out["text_content"] is None
    assert out["text_extraction_status"] is None


def test_enrich_does_not_overwrite_existing_text() -> None:
    fetcher = DerivedCommentText(_FakeS3Resource(_store()), BUCKET)
    records = [_comment_record("ACF-2025-0038-0004", attachments=True, text="already here")]
    (out,) = list(EnrichCommentText(fetcher).apply(records))
    assert out["text_content"] == "already here"


# --- Chain -------------------------------------------------------------------


class _AddOne(Transform):
    def apply(self, records):
        for r in records:
            yield {**r, "n": r.get("n", 0) + 1}


def test_chain_applies_transforms_in_order() -> None:
    out = list(Chain(_AddOne(), _AddOne()).apply([{"n": 0}, {"n": 5}]))
    assert [r["n"] for r in out] == [2, 7]


def test_chain_extract_then_enrich_end_to_end() -> None:
    """The exact composition the pipeline wires for comments."""
    from spicy_regs.schemas import COMMENT

    payload = {
        "data": {
            "id": "ACF-2025-0038-0004",
            "attributes": {
                "docketId": "ACF-2025-0038",
                "agencyId": "ACF",
                "comment": "See attached file(s)",
            },
            "relationships": {"attachments": {"data": [{"id": "a", "type": "attachments"}]}},
        },
        "included": [
            {
                "id": "a",
                "type": "attachments",
                "attributes": {
                    "title": "t",
                    "fileFormats": [{"fileUrl": "https://x/a.pdf", "format": "pdf", "size": 10}],
                },
            }
        ],
    }
    fetcher = DerivedCommentText(_FakeS3Resource(_store()), BUCKET)
    chain = Chain(ExtractRecords(COMMENT), EnrichCommentText(fetcher))
    (out,) = list(chain.apply([payload]))
    assert out["comment_id"] == "ACF-2025-0038-0004"
    assert out["text_content"] == "Wisconsin DCF comment body"
    assert out["text_extraction_status"] == "ok"
