"""Tests for the R2 storage connector (sources/r2.py)."""

from pathlib import Path

import pytest

import spicy_regs.sources.r2 as r2


def test_download_delegates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []
    monkeypatch.setattr(r2, "download_from_r2", lambda key, path: calls.append((key, path)) or True)

    assert r2.download("manifest.parquet", tmp_path / "manifest.parquet") is True
    assert calls == [("manifest.parquet", tmp_path / "manifest.parquet")]


def test_upload_dataset_uploads_existing_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """upload_dataset publishes the existing {type}.parquet files (+ manifest)."""
    (tmp_path / "dockets.parquet").write_bytes(b"d")
    (tmp_path / "documents.parquet").write_bytes(b"x")
    (tmp_path / "manifest.parquet").write_bytes(b"m")
    # comments.parquet intentionally absent — it's published as partitions.

    uploaded: list[Path] = []
    monkeypatch.setattr(r2, "upload_file", lambda p, remote_key=None: uploaded.append(p))

    r2.upload_dataset(tmp_path, ["dockets", "documents", "comments"])

    assert set(uploaded) == {
        tmp_path / "dockets.parquet",
        tmp_path / "documents.parquet",
        tmp_path / "manifest.parquet",
    }


def test_upload_comment_partitions_uploads_changed_and_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """upload_comment_partitions publishes each changed partition and the index."""
    changed = tmp_path / "comments" / "agency_code=EPA" / "part-0.parquet"
    changed.parent.mkdir(parents=True)
    changed.write_bytes(b"c")
    (tmp_path / "comments_index.parquet").write_bytes(b"i")

    uploaded: list[tuple[Path, str | None]] = []
    monkeypatch.setattr(r2, "upload_file", lambda p, remote_key=None: uploaded.append((p, remote_key)))

    r2.upload_comment_partitions(tmp_path, [changed])

    assert (changed, str(changed.relative_to(tmp_path))) in uploaded
    assert (tmp_path / "comments_index.parquet", "comments_index.parquet") in uploaded
