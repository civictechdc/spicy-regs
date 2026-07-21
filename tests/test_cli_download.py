"""Tests for `spicy-regs download` using httpx.MockTransport (no network)."""

from pathlib import Path

import httpx
import pytest

from spicy_regs.cli import download


def make_client(files: dict[str, bytes]) -> httpx.Client:
    """Client whose transport serves `files` keyed by URL path (e.g. '/dockets.parquet')."""

    def handler(request: httpx.Request) -> httpx.Response:
        content = files.get(request.url.path)
        if content is None:
            return httpx.Response(404)
        if request.method == "HEAD":
            return httpx.Response(200, headers={"content-length": str(len(content))})
        return httpx.Response(200, content=content)

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_download_writes_atomically(tmp_path: Path):
    client = make_client({"/dockets.parquet": b"PARQUET-BYTES"})
    dest = tmp_path / "dockets.parquet"
    assert download.download_file(client, "https://bucket.test/dockets.parquet", dest) == "downloaded"
    assert dest.read_bytes() == b"PARQUET-BYTES"
    assert not list(tmp_path.glob("*.tmp"))


def test_download_skips_when_size_matches(tmp_path: Path, capsys):
    client = make_client({"/dockets.parquet": b"12345"})
    dest = tmp_path / "dockets.parquet"
    dest.write_bytes(b"12345")
    assert download.download_file(client, "https://bucket.test/dockets.parquet", dest) == "skipped"
    assert "up to date" in capsys.readouterr().out


def test_download_refreshes_when_size_differs(tmp_path: Path):
    client = make_client({"/dockets.parquet": b"new-longer-content"})
    dest = tmp_path / "dockets.parquet"
    dest.write_bytes(b"stale")
    assert download.download_file(client, "https://bucket.test/dockets.parquet", dest) == "downloaded"
    assert dest.read_bytes() == b"new-longer-content"


def test_force_redownloads_even_when_current(tmp_path: Path):
    client = make_client({"/dockets.parquet": b"12345"})
    dest = tmp_path / "dockets.parquet"
    dest.write_bytes(b"12345")
    assert download.download_file(client, "https://bucket.test/dockets.parquet", dest, force=True) == "downloaded"


def test_unpublished_table_is_missing_not_failed(tmp_path: Path, capsys):
    client = make_client({})
    dest = tmp_path / "court_dockets.parquet"
    assert download.download_file(client, "https://bucket.test/court_dockets.parquet", dest) == "missing"
    assert not dest.exists()
    assert "not published" in capsys.readouterr().out


def test_network_failure_leaves_existing_file_intact(tmp_path: Path):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    dest = tmp_path / "dockets.parquet"
    dest.write_bytes(b"precious")
    assert download.download_file(client, "https://bucket.test/dockets.parquet", dest) == "failed"
    assert dest.read_bytes() == b"precious"
    assert not list(tmp_path.glob("*.tmp"))


def test_run_end_to_end_via_main(tmp_path: Path, monkeypatch, capsys):
    files: dict[str, bytes] = {
        f"/{name}.parquet": f"data-{name}".encode() for name in ("dockets", "documents", "comments")
    }
    monkeypatch.setattr(download, "_build_client", lambda transport=None: make_client(files))

    from spicy_regs.cli import main

    with pytest.raises(SystemExit) as excinfo:
        main(["download", "-o", str(tmp_path)])
    assert excinfo.value.code == 0
    for name in ("dockets", "documents", "comments"):
        assert (tmp_path / f"{name}.parquet").read_bytes() == f"data-{name}".encode()
    assert "Done!" in capsys.readouterr().out


def test_run_reports_failure_exit_code(tmp_path: Path, monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    monkeypatch.setattr(
        download, "_build_client", lambda transport=None: httpx.Client(transport=httpx.MockTransport(handler))
    )

    from spicy_regs.cli import main

    with pytest.raises(SystemExit) as excinfo:
        main(["download", "--tables", "dockets", "-o", str(tmp_path)])
    assert excinfo.value.code == 1
