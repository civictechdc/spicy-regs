"""Tests for the R2 storage connector (sources/r2.py)."""

from pathlib import Path

import pytest

import spicy_regs.sources.r2 as r2


def test_download_delegates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []
    monkeypatch.setattr(r2, "download_from_r2", lambda key, path: calls.append((key, path)) or True)

    assert r2.download("manifest.parquet", tmp_path / "manifest.parquet") is True
    assert calls == [("manifest.parquet", tmp_path / "manifest.parquet")]


def test_upload_dataset_uploads_only_existing_base_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The dataset publisher leaves the manifest to its pipeline caller."""
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
    }


def test_upload_comment_partitions_uploads_changed_and_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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


def _upload_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Minimal R2 env so upload_file runs its body (isolate_env strips these)."""
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "s")
    monkeypatch.setenv("R2_BUCKET_NAME", "spicy-regs")
    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.spicy-regs.dev")


def test_upload_file_purges_exact_object_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """After a successful upload, the edge cache is purged for that one URL."""
    _upload_env(monkeypatch)
    (tmp_path / "agency_stats.parquet").write_bytes(b"x" * 100)

    monkeypatch.setattr(r2, "get_r2_client", lambda: __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock())
    monkeypatch.setattr(r2, "_get_remote_size", lambda *a, **k: None)
    purged: list[list[str]] = []
    monkeypatch.setattr(r2, "purge_urls", lambda urls: purged.append(urls))

    r2.upload_file(tmp_path / "agency_stats.parquet")

    assert purged == [["https://data.spicy-regs.dev/agency_stats.parquet"]]


def test_upload_file_survives_unreachable_edge(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end: a dead Cloudflare edge must not fail a successful publish.

    Uses the real ``purge_urls`` (creds set) with ``httpx.post`` raising, so this
    exercises the actual composition rather than a mock — the property that
    matters is that the ETL never dies because the cache couldn't be purged.
    """
    _upload_env(monkeypatch)
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "tok")
    monkeypatch.setenv("CLOUDFLARE_ZONE_ID", "zone")
    (tmp_path / "agency_stats.parquet").write_bytes(b"x" * 100)

    monkeypatch.setattr(r2, "get_r2_client", lambda: __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock())
    monkeypatch.setattr(r2, "_get_remote_size", lambda *a, **k: None)

    import httpx

    def _raise(*a, **k):
        raise httpx.ConnectError("edge down")

    monkeypatch.setattr(httpx, "post", _raise)

    # Must complete normally despite the purge failing underneath.
    r2.upload_file(tmp_path / "agency_stats.parquet")


def test_upload_dataset_raises_when_an_upload_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failing publish must surface, not be swallowed by the executor.

    Regression: `executor.map`'s lazy iterator was discarded, so the shrink
    guard's refusal to overwrite dockets.parquet never propagated and the ETL
    reported success while the table sat frozen for 8 weeks.
    """
    (tmp_path / "dockets.parquet").write_bytes(b"d")
    (tmp_path / "documents.parquet").write_bytes(b"x")

    def fake_upload(path: Path, remote_key: str | None = None) -> None:
        if path.name == "dockets.parquet":
            raise RuntimeError("Refusing to upload dockets.parquet: would shrink remote")

    monkeypatch.setattr(r2, "upload_file", fake_upload)

    with pytest.raises(RuntimeError, match="dockets.parquet"):
        r2.upload_dataset(tmp_path, ["dockets", "documents"])


def test_upload_dataset_preflights_all_guards_before_uploading(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A known shrink refusal must stop the whole dataset before the first write."""
    _upload_env(monkeypatch)
    (tmp_path / "documents.parquet").write_bytes(b"x" * 100)
    (tmp_path / "dockets.parquet").write_bytes(b"d")
    (tmp_path / "manifest.parquet").write_bytes(b"m" * 100)

    monkeypatch.setattr(r2, "get_r2_client", lambda: object())
    checked: list[str] = []

    def fake_remote_size(client: object, bucket: str, key: str) -> int:
        checked.append(key)
        return 100

    monkeypatch.setattr(
        r2,
        "_get_remote_size",
        fake_remote_size,
    )
    uploaded: list[Path] = []
    monkeypatch.setattr(r2, "upload_file", lambda path, remote_key=None: uploaded.append(path))

    with pytest.raises(RuntimeError, match="Publication preflight failed.*dockets.parquet"):
        r2.upload_dataset(tmp_path, ["documents", "dockets"])

    assert uploaded == []
    assert checked == ["documents.parquet", "dockets.parquet"]


def test_upload_dataset_reports_every_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """One broken table must not hide another behind it."""
    (tmp_path / "dockets.parquet").write_bytes(b"d")
    (tmp_path / "documents.parquet").write_bytes(b"x")

    def fake_upload(path: Path, remote_key: str | None = None) -> None:
        raise RuntimeError(f"boom: {path.name}")

    monkeypatch.setattr(r2, "upload_file", fake_upload)

    with pytest.raises(RuntimeError) as excinfo:
        r2.upload_dataset(tmp_path, ["dockets", "documents"])

    message = str(excinfo.value)
    assert "dockets.parquet" in message
    assert "documents.parquet" in message
    assert "2 of 2" in message


def test_upload_dataset_is_a_noop_with_nothing_to_publish(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty publish set must not blow up on max_workers=0."""
    monkeypatch.setattr(r2, "upload_file", lambda p, remote_key=None: None)

    r2.upload_dataset(tmp_path, ["dockets"])
