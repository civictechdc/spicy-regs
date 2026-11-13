"""Tests for the documents.subtype backfill (#221)."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.backfill_subtype import (
    apply_subtypes,
    harvest_keys,
    select_candidate_keys,
    subtype_row,
)


def _key(agency: str, docket: str, document_id: str, suffix: str = ".json") -> str:
    return f"raw-data/{agency}/{docket}/text-{docket}/documents/{document_id}{suffix}"


def _doc(document_id: str, docket_id: str, **overrides) -> dict:
    row = {
        "document_id": document_id,
        "docket_id": docket_id,
        "agency_code": docket_id.split("-")[0].split("_")[0],
        "title": f"title {document_id}",
        "document_type": "Proposed Rule",
        "subtype": None,
        "modify_date": "2024-01-01T00:00:00Z",
        "text_content": f"text of {document_id}",
    }
    row.update(overrides)
    return row


def _write_documents(path: Path, rows: list[dict], *, drop_subtype: bool = False) -> Path:
    table = pa.Table.from_pylist(rows, schema=pa.schema([(k, pa.string()) for k in rows[0]]))
    if drop_subtype:
        table = table.drop_columns(["subtype"])
    pq.write_table(table, path)
    return path


def _write_manifest(path: Path, keys: list[str]) -> Path:
    pq.write_table(pa.table({"key": pa.array(keys, pa.large_string())}), path)
    return path


def _payload(document_id: str, subtype: str | None, modify_date: str = "2024-01-01T00:00:00Z") -> bytes:
    return json.dumps(
        {"data": {"id": document_id, "attributes": {"subtype": subtype, "modifyDate": modify_date}}}
    ).encode()


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data

    def close(self) -> None:
        pass


class _Obj:
    def __init__(self, store: dict, key: str) -> None:
        self._store = store
        self._key = key

    def get(self) -> dict:
        value = self._store[self._key]
        if isinstance(value, Exception):
            raise value
        return {"Body": _Body(value)}


class _FakeS3:
    def __init__(self, store: dict) -> None:
        self.store = store

    def Object(self, bucket: str, key: str) -> _Obj:  # noqa: N802 — mirrors boto3
        return _Obj(self.store, key)


# --- subtype_row -------------------------------------------------------------


def test_subtype_row_reads_id_subtype_and_modify_date() -> None:
    row = subtype_row(json.loads(_payload("EPA-1-0001", "Interim Final Rule", "2025-02-02")))
    assert row == {"document_id": "EPA-1-0001", "subtype": "Interim Final Rule", "modify_date": "2025-02-02"}


def test_subtype_row_skips_payload_without_id() -> None:
    assert subtype_row({"data": {"attributes": {"subtype": "x"}}}) is None


# --- select_candidate_keys ---------------------------------------------------


@pytest.fixture
def published(tmp_path: Path) -> tuple[Path, Path]:
    docs = _write_documents(
        tmp_path / "documents.parquet",
        [
            _doc("EPA-2024-0001-0001", "EPA-2024-0001"),
            _doc("EPA-2024-0001-0002", "EPA-2024-0001", subtype="Final Rule"),
            _doc("EPA-2024-0001-0003", "EPA-2024-0001", document_type="Supporting & Related Material"),
            # FRDOC dockets live under the agency directory, not the docket prefix.
            _doc("CMS_FRDOC_0001-0100", "CMS_FRDOC_0001", agency_code="CMS", document_type="Notice"),
        ],
    )
    manifest = _write_manifest(
        tmp_path / "manifest.parquet",
        [
            _key("EPA", "EPA-2024-0001", "EPA-2024-0001-0001"),
            _key("EPA", "EPA-2024-0001", "EPA-2024-0001-0001", "_content.htm"),
            _key("EPA", "EPA-2024-0001", "EPA-2024-0001-0002"),
            _key("EPA", "EPA-2024-0001", "EPA-2024-0001-0003"),
            _key("CMS", "CMS_FRDOC_0001", "CMS_FRDOC_0001-0100"),
            "raw-data/EPA/EPA-2024-0001/text-EPA-2024-0001/comments/EPA-2024-0001-0001.json",
            "raw-data/EPA/EPA-2024-0001/text-EPA-2024-0001/docket/EPA-2024-0001.json",
        ],
    )
    return docs, manifest


def test_candidates_are_null_subtype_document_json_keys(published) -> None:
    docs, manifest = published
    assert select_candidate_keys(docs, manifest) == [
        _key("CMS", "CMS_FRDOC_0001", "CMS_FRDOC_0001-0100"),
        _key("EPA", "EPA-2024-0001", "EPA-2024-0001-0001"),
        _key("EPA", "EPA-2024-0001", "EPA-2024-0001-0003"),
    ]


def test_candidates_filter_by_document_type_agency_and_limit(published) -> None:
    docs, manifest = published
    assert select_candidate_keys(docs, manifest, document_types=["Proposed Rule", "Notice"]) == [
        _key("CMS", "CMS_FRDOC_0001", "CMS_FRDOC_0001-0100"),
        _key("EPA", "EPA-2024-0001", "EPA-2024-0001-0001"),
    ]
    assert select_candidate_keys(docs, manifest, agencies=["CMS"]) == [
        _key("CMS", "CMS_FRDOC_0001", "CMS_FRDOC_0001-0100"),
    ]
    assert len(select_candidate_keys(docs, manifest, limit=1)) == 1


def test_candidates_include_every_row_when_file_predates_subtype(tmp_path: Path, published) -> None:
    _, manifest = published
    docs = _write_documents(
        tmp_path / "old.parquet",
        [_doc("EPA-2024-0001-0002", "EPA-2024-0001")],
        drop_subtype=True,
    )
    assert select_candidate_keys(docs, manifest) == [_key("EPA", "EPA-2024-0001", "EPA-2024-0001-0002")]


# --- harvest_keys ------------------------------------------------------------


def test_harvest_writes_parts_and_resumes(tmp_path: Path, published) -> None:
    docs, manifest = published
    work = tmp_path / "work"
    store = {
        _key("EPA", "EPA-2024-0001", "EPA-2024-0001-0001"): _payload("EPA-2024-0001-0001", "NPRM"),
        _key("EPA", "EPA-2024-0001", "EPA-2024-0001-0003"): _payload("EPA-2024-0001-0003", None),
        _key("CMS", "CMS_FRDOC_0001", "CMS_FRDOC_0001-0100"): ConnectionError("reset"),
    }
    keys = select_candidate_keys(docs, manifest, harvested_dir=work)
    result = harvest_keys(keys, work, s3_resource=_FakeS3(store), workers=1, chunk_size=2)

    assert (result.candidates, result.harvested, result.with_subtype) == (3, 2, 1)
    assert result.transient_failures == 1
    rows = duckdb.sql(f"SELECT document_id, subtype FROM '{work}/parts/*.parquet' ORDER BY 1").fetchall()
    # A NULL-subtype document is still recorded, so a re-run doesn't refetch it.
    assert rows == [("EPA-2024-0001-0001", "NPRM"), ("EPA-2024-0001-0003", None)]

    # Re-run: only the transient failure is left, and it now succeeds.
    store[_key("CMS", "CMS_FRDOC_0001", "CMS_FRDOC_0001-0100")] = _payload("CMS_FRDOC_0001-0100", "Notice")
    keys = select_candidate_keys(docs, manifest, harvested_dir=work)
    assert keys == [_key("CMS", "CMS_FRDOC_0001", "CMS_FRDOC_0001-0100")]
    harvest_keys(keys, work, s3_resource=_FakeS3(store), workers=1)
    assert sorted(p.name for p in (work / "parts").glob("*.parquet")) == [
        "part-00000.parquet",
        "part-00001.parquet",
        "part-00002.parquet",
    ]


# --- apply_subtypes ----------------------------------------------------------


def _write_parts(work: Path, rows: list[dict]) -> None:
    (work / "parts").mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), work / "parts" / "part-00000.parquet")


def test_apply_fills_null_subtypes_only_and_keeps_everything_else(tmp_path: Path) -> None:
    docs = _write_documents(
        tmp_path / "documents.parquet",
        [
            _doc("B-1-0001", "B-1"),
            _doc("A-1-0001", "A-1", subtype="Final Rule"),
            _doc("A-1-0002", "A-1"),
            _doc("C-1-0001", "C-1"),
        ],
    )
    work = tmp_path / "work"
    _write_parts(
        work,
        [
            # Already set on the published row: never overwritten.
            {"document_id": "A-1-0001", "subtype": "Something Else", "modify_date": "2024-01-01"},
            # Two JSONs for one document: the latest modifyDate wins.
            {"document_id": "A-1-0002", "subtype": "Old", "modify_date": "2023-01-01"},
            {"document_id": "A-1-0002", "subtype": "Interim Final Rule", "modify_date": "2025-01-01"},
            {"document_id": "B-1-0001", "subtype": "NPRM", "modify_date": "2024-01-01"},
            {"document_id": "C-1-0001", "subtype": None, "modify_date": "2024-01-01"},
            # Harvested but no longer published: must not add a row.
            {"document_id": "Z-9-0001", "subtype": "Ghost", "modify_date": "2024-01-01"},
        ],
    )
    out = tmp_path / "patched.parquet"

    result = apply_subtypes(docs, work, out)

    assert (result.rows, result.filled, result.subtype_before, result.subtype_after) == (4, 2, 1, 3)
    assert pq.ParquetFile(out).schema_arrow.names == pq.ParquetFile(docs).schema_arrow.names
    rows = duckdb.sql(f"SELECT document_id, docket_id, subtype, text_content FROM '{out}'").fetchall()
    # Clustered by docket_id, like merge_staging_files.
    assert [r[1] for r in rows] == ["A-1", "A-1", "B-1", "C-1"]
    assert {r[0]: r[2] for r in rows} == {
        "A-1-0001": "Final Rule",
        "A-1-0002": "Interim Final Rule",
        "B-1-0001": "NPRM",
        "C-1-0001": None,
    }
    assert {r[0]: r[3] for r in rows} == {r[0]: f"text of {r[0]}" for r in rows}


def test_apply_adds_subtype_column_to_file_that_predates_it(tmp_path: Path) -> None:
    docs = _write_documents(tmp_path / "documents.parquet", [_doc("A-1-0001", "A-1")], drop_subtype=True)
    work = tmp_path / "work"
    _write_parts(work, [{"document_id": "A-1-0001", "subtype": "NPRM", "modify_date": "2024-01-01"}])
    out = tmp_path / "patched.parquet"

    result = apply_subtypes(docs, work, out)

    assert result.filled == 1
    assert duckdb.sql(f"SELECT subtype FROM '{out}'").fetchall() == [("NPRM",)]


def test_apply_requires_a_harvest(tmp_path: Path) -> None:
    docs = _write_documents(tmp_path / "documents.parquet", [_doc("A-1-0001", "A-1")])
    with pytest.raises(FileNotFoundError):
        apply_subtypes(docs, tmp_path / "work", tmp_path / "out.parquet")
