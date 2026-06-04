"""Tests for the R2 storage connector (delegation to the pipeline helpers)."""

from pathlib import Path

import pytest

import spicy_regs.sources.r2 as r2


def test_download_delegates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []
    monkeypatch.setattr(r2, "download_from_r2", lambda key, path: calls.append((key, path)) or True)

    assert r2.download("manifest.parquet", tmp_path / "manifest.parquet") is True
    assert calls == [("manifest.parquet", tmp_path / "manifest.parquet")]


def test_upload_dataset_delegates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []
    monkeypatch.setattr(r2, "_upload_to_r2", lambda out, types: calls.append((out, types)))

    r2.upload_dataset(tmp_path, ["dockets", "documents"])
    assert calls == [(tmp_path, ["dockets", "documents"])]


def test_upload_comment_partitions_delegates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []
    monkeypatch.setattr(r2, "_upload_comment_partitions", lambda out, files: calls.append((out, files)))

    changed = [tmp_path / "comments" / "EPA.parquet"]
    r2.upload_comment_partitions(tmp_path, changed)
    assert calls == [(tmp_path, changed)]
