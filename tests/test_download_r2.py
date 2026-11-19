"""Tests for download_r2: error handling and atomic writes.

These guard against the March 2026 incident where a silent download
failure caused the ETL to overwrite a 3.3 GB historical comments.parquet
with a fresh empty file.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock

import httpx
import pytest

from spicy_regs.sources import r2 as download_r2
from spicy_regs.sources.r2 import download_from_r2


def _fake_stream(*, status_code: int = 200, body: bytes = b"", raise_exc: Exception | None = None):
    """Build a callable that mimics ``httpx.stream(...)`` as a context manager."""

    @contextmanager
    def fake_stream(method, url, follow_redirects=True, **kwargs):
        if raise_exc is not None:
            raise raise_exc
        resp = MagicMock()
        resp.status_code = status_code
        resp.iter_bytes = lambda: iter([body]) if body else iter([])
        yield resp

    return fake_stream


@pytest.fixture(autouse=True)
def _no_backoff_sleep(monkeypatch):
    monkeypatch.setattr(download_r2.time, "sleep", lambda _s: None)


class TestDownloadFromR2:
    def test_returns_false_when_r2_url_not_set(self, tmp_path, monkeypatch):
        monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
        target = tmp_path / "foo.parquet"

        assert download_from_r2("foo.parquet", target) is False
        assert not target.exists()

    def test_returns_false_on_404(self, tmp_path, monkeypatch):
        """404 is a legitimate 'not on R2 yet' signal (first run)."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        monkeypatch.setattr(download_r2.httpx, "stream", _fake_stream(status_code=404))

        target = tmp_path / "missing.parquet"
        assert download_from_r2("missing.parquet", target) is False
        assert not target.exists()

    def test_raises_on_server_error(self, tmp_path, monkeypatch):
        """5xx means the object may exist — we must not silently continue,
        or the pipeline will happily overwrite production data."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        monkeypatch.setattr(download_r2.httpx, "stream", _fake_stream(status_code=503))

        target = tmp_path / "foo.parquet"
        with pytest.raises(RuntimeError, match="503"):
            download_from_r2("foo.parquet", target)
        assert not target.exists()

    def test_raises_on_connection_error(self, tmp_path, monkeypatch):
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        monkeypatch.setattr(
            download_r2.httpx,
            "stream",
            _fake_stream(raise_exc=httpx.ConnectError("conn refused")),
        )

        target = tmp_path / "foo.parquet"
        with pytest.raises(httpx.ConnectError):
            download_from_r2("foo.parquet", target)
        assert not target.exists()

    def test_writes_file_on_success(self, tmp_path, monkeypatch):
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        monkeypatch.setattr(
            download_r2.httpx,
            "stream",
            _fake_stream(status_code=200, body=b"parquet-bytes"),
        )

        target = tmp_path / "foo.parquet"
        assert download_from_r2("foo.parquet", target) is True
        assert target.read_bytes() == b"parquet-bytes"

    def test_no_partial_file_on_mid_stream_error(self, tmp_path, monkeypatch):
        """If the stream raises mid-download, the destination file must not
        exist — otherwise callers will treat the corrupt partial as valid."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")

        @contextmanager
        def dying_stream(method, url, follow_redirects=True, **kwargs):
            resp = MagicMock()
            resp.status_code = 200

            def iter_bytes():
                yield b"first chunk"
                raise httpx.ReadError("connection dropped")

            resp.iter_bytes = iter_bytes
            yield resp

        monkeypatch.setattr(download_r2.httpx, "stream", dying_stream)

        target = tmp_path / "foo.parquet"
        with pytest.raises(httpx.ReadError):
            download_from_r2("foo.parquet", target)
        assert not target.exists()

    def test_preserves_existing_file_on_failure(self, tmp_path, monkeypatch):
        """If target already exists and download fails, the old file must
        remain untouched."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        target = tmp_path / "foo.parquet"
        target.write_bytes(b"original content")

        monkeypatch.setattr(download_r2.httpx, "stream", _fake_stream(status_code=503))

        with pytest.raises(RuntimeError):
            download_from_r2("foo.parquet", target)

        assert target.read_bytes() == b"original content"

    def test_no_leftover_tmp_file_on_failure(self, tmp_path, monkeypatch):
        """The ``.tmp`` staging file must be cleaned up on any failure so
        it doesn't leak disk across retries."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")

        @contextmanager
        def dying_stream(method, url, follow_redirects=True, **kwargs):
            resp = MagicMock()
            resp.status_code = 200

            def iter_bytes():
                yield b"partial"
                raise httpx.ReadError("dropped")

            resp.iter_bytes = iter_bytes
            yield resp

        monkeypatch.setattr(download_r2.httpx, "stream", dying_stream)

        target = tmp_path / "foo.parquet"
        with pytest.raises(httpx.ReadError):
            download_from_r2("foo.parquet", target)

        # No lingering .tmp file either
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []


class TestDownloadRetries:
    def test_retries_read_timeout_then_succeeds(self, tmp_path, monkeypatch):
        """A mid-stream stall on a large object must not abort the ETL batch
        (run 44429554235 died on httpx.ReadTimeout with no retry)."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        attempts = []

        @contextmanager
        def flaky_stream(method, url, follow_redirects=True, **kwargs):
            attempts.append(kwargs.get("timeout"))
            resp = MagicMock()
            resp.status_code = 200

            def iter_bytes():
                yield b"part"
                if len(attempts) == 1:
                    raise httpx.ReadTimeout("The read operation timed out")
                yield b"-rest"

            resp.iter_bytes = iter_bytes
            yield resp

        monkeypatch.setattr(download_r2.httpx, "stream", flaky_stream)

        target = tmp_path / "foo.parquet"
        assert download_from_r2("foo.parquet", target) is True
        assert target.read_bytes() == b"part-rest"
        assert len(attempts) == 2
        assert attempts[0] is download_r2._DOWNLOAD_TIMEOUT
        assert list(tmp_path.glob("*.tmp")) == []

    def test_retries_5xx_then_succeeds(self, tmp_path, monkeypatch):
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        codes = iter([503, 500, 200])

        @contextmanager
        def stream(method, url, follow_redirects=True, **kwargs):
            resp = MagicMock()
            resp.status_code = next(codes)
            resp.iter_bytes = lambda: iter([b"ok"])
            yield resp

        monkeypatch.setattr(download_r2.httpx, "stream", stream)

        target = tmp_path / "foo.parquet"
        assert download_from_r2("foo.parquet", target) is True
        assert target.read_bytes() == b"ok"

    def test_gives_up_after_max_attempts(self, tmp_path, monkeypatch):
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        calls = []

        @contextmanager
        def stream(method, url, follow_redirects=True, **kwargs):
            calls.append(1)
            raise httpx.ReadTimeout("timed out")
            yield  # pragma: no cover

        monkeypatch.setattr(download_r2.httpx, "stream", stream)

        with pytest.raises(httpx.ReadTimeout):
            download_from_r2("foo.parquet", tmp_path / "foo.parquet")
        assert len(calls) == download_r2._DOWNLOAD_MAX_ATTEMPTS

    def test_does_not_retry_client_errors(self, tmp_path, monkeypatch):
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        calls = []

        @contextmanager
        def stream(method, url, follow_redirects=True, **kwargs):
            calls.append(1)
            resp = MagicMock()
            resp.status_code = 403
            yield resp

        monkeypatch.setattr(download_r2.httpx, "stream", stream)

        with pytest.raises(RuntimeError, match="403"):
            download_from_r2("foo.parquet", tmp_path / "foo.parquet")
        assert len(calls) == 1
