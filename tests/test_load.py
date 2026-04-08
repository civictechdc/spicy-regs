"""Tests for the upload-side guardrails in load.py / upload_r2.py."""

from unittest.mock import MagicMock

import pytest

from spicy_regs.pipeline import upload_r2
from spicy_regs.pipeline.upload_r2 import upload_to_r2


class TestUploadShrinkGuard:
    """Prevent a local file from silently overwriting a much larger R2 object.

    Guards against the March 2026 incident: a transient download error
    produced an empty local ``comments.parquet``, which was then uploaded
    to R2 and overwrote the 3.3 GB historical file.
    """

    def _mock_r2_client(self, monkeypatch, remote_size: int | None):
        """Install a fake R2 client that reports ``remote_size`` bytes for
        any HEAD and records upload_file calls on ``uploads``."""
        from botocore.exceptions import ClientError

        uploads: list[tuple[str, str]] = []
        fake = MagicMock()

        if remote_size is None:
            fake.head_object.side_effect = ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )
        else:
            fake.head_object.return_value = {"ContentLength": remote_size}

        def fake_upload_file(local, bucket, key, ExtraArgs=None):
            uploads.append((local, key))

        fake.upload_file.side_effect = fake_upload_file

        monkeypatch.setattr(upload_r2, "get_r2_client", lambda: fake)
        return fake, uploads

    def _setup_env(self, monkeypatch):
        monkeypatch.setenv("R2_ACCESS_KEY_ID", "fake")
        monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "fake")
        monkeypatch.setenv("R2_BUCKET_NAME", "spicy-regs")

    def test_allows_upload_when_remote_missing(self, tmp_path, monkeypatch):
        """First upload (remote doesn't exist yet) should succeed."""
        self._setup_env(monkeypatch)
        _, uploads = self._mock_r2_client(monkeypatch, remote_size=None)

        local = tmp_path / "dockets.parquet"
        local.write_bytes(b"x" * 1000)

        upload_to_r2(local)
        assert len(uploads) == 1

    def test_allows_upload_when_new_file_is_larger(self, tmp_path, monkeypatch):
        """Normal incremental growth should succeed."""
        self._setup_env(monkeypatch)
        _, uploads = self._mock_r2_client(monkeypatch, remote_size=1_000_000)

        local = tmp_path / "comments.parquet"
        local.write_bytes(b"x" * 1_100_000)

        upload_to_r2(local)
        assert len(uploads) == 1

    def test_blocks_upload_on_catastrophic_shrink(self, tmp_path, monkeypatch):
        """Reproduces the March 2026 incident: 3.3 GB → 8 MB is a 0.003
        ratio, far below the 0.5 default. Must raise and not upload."""
        self._setup_env(monkeypatch)
        _, uploads = self._mock_r2_client(monkeypatch, remote_size=3_300_000_000)

        local = tmp_path / "comments.parquet"
        local.write_bytes(b"x" * 8_000_000)

        with pytest.raises(RuntimeError, match="shrink"):
            upload_to_r2(local)

        assert uploads == []

    def test_allows_small_shrink_within_threshold(self, tmp_path, monkeypatch):
        """Dedup can legitimately shrink a file; a modest shrink (e.g. 30%)
        must still be allowed."""
        self._setup_env(monkeypatch)
        _, uploads = self._mock_r2_client(monkeypatch, remote_size=1_000_000)

        local = tmp_path / "dockets.parquet"
        local.write_bytes(b"x" * 700_000)  # 0.7 ratio, above default 0.5

        upload_to_r2(local)
        assert len(uploads) == 1

    def test_escape_hatch_allows_shrink(self, tmp_path, monkeypatch):
        """Recovery flows need to bypass the guard."""
        self._setup_env(monkeypatch)
        monkeypatch.setenv("R2_ALLOW_SHRINK", "1")
        _, uploads = self._mock_r2_client(monkeypatch, remote_size=3_300_000_000)

        local = tmp_path / "comments.parquet"
        local.write_bytes(b"x" * 8_000_000)

        upload_to_r2(local)
        assert len(uploads) == 1

    def test_custom_shrink_threshold_from_env(self, tmp_path, monkeypatch):
        """R2_MIN_SIZE_RATIO lets ops tune the threshold without a code
        change. 0.9 means the new file must be at least 90% of the old."""
        self._setup_env(monkeypatch)
        monkeypatch.setenv("R2_MIN_SIZE_RATIO", "0.9")
        _, uploads = self._mock_r2_client(monkeypatch, remote_size=1_000_000)

        local = tmp_path / "comments.parquet"
        local.write_bytes(b"x" * 800_000)  # 0.8 ratio, below 0.9

        with pytest.raises(RuntimeError, match="shrink"):
            upload_to_r2(local)
        assert uploads == []
