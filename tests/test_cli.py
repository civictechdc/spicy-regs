"""Tests for the ``spicy-regs`` CLI's download path.

Regression coverage for the Cloudflare 403: this bucket rejects the default
``Python-urllib/*`` (and blank) User-Agent, so ``download_file`` must send an
explicit, non-default one. Hermetic — the network call is monkeypatched, same
pattern as ``tests/test_download_r2.py``.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock

import httpx

from spicy_regs import cli


def _fake_stream(*, status_code: int = 200, body: bytes = b"", capture: dict | None = None):
    """Build a callable that mimics ``httpx.stream(...)`` as a context manager."""

    @contextmanager
    def fake_stream(method, url, *, headers=None, follow_redirects=True, timeout=None):
        if capture is not None:
            capture["headers"] = headers
            capture["url"] = url
        resp = MagicMock()
        resp.status_code = status_code
        resp.raise_for_status = MagicMock()
        if status_code >= 400:
            resp.raise_for_status.side_effect = httpx.HTTPStatusError(
                f"{status_code}", request=MagicMock(), response=resp
            )
        resp.iter_bytes = lambda: iter([body]) if body else iter([])
        yield resp

    return fake_stream


class TestDownloadFile:
    def test_sends_non_default_user_agent(self, tmp_path, monkeypatch):
        """The default urllib/httpx UA gets a 403 from Cloudflare on this
        bucket; the request must carry an explicit, honest identifier."""
        captured: dict = {}
        monkeypatch.setattr(cli.httpx, "stream", _fake_stream(status_code=200, body=b"data", capture=captured))

        cli.download_file("dockets", tmp_path)

        ua = captured["headers"]["User-Agent"]
        assert ua, "no User-Agent sent"
        assert ua.startswith("spicy-regs/")
        assert "python-urllib" not in ua.lower()
        assert "python-httpx" not in ua.lower()

    def test_writes_file_on_success(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cli.httpx, "stream", _fake_stream(status_code=200, body=b"parquet-bytes"))

        result = cli.download_file("dockets", tmp_path)

        assert result is not None
        assert result == tmp_path / "dockets.parquet"
        assert result.read_bytes() == b"parquet-bytes"

    def test_returns_none_on_403(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cli.httpx, "stream", _fake_stream(status_code=403))

        result = cli.download_file("dockets", tmp_path)

        assert result is None
        assert not (tmp_path / "dockets.parquet").exists() or (tmp_path / "dockets.parquet").stat().st_size == 0

    def test_skips_existing_file_without_network(self, tmp_path, monkeypatch):
        existing = tmp_path / "dockets.parquet"
        existing.write_bytes(b"already here")

        def boom(*args, **kwargs):
            raise AssertionError("should not touch the network when the file already exists")

        monkeypatch.setattr(cli.httpx, "stream", boom)

        result = cli.download_file("dockets", tmp_path)

        assert result == existing
        assert existing.read_bytes() == b"already here"


def test_package_version_resolved_to_nonempty_string():
    """The version substituted into the User-Agent must never be blank."""
    assert isinstance(cli._PACKAGE_VERSION, str) and cli._PACKAGE_VERSION
