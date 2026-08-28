"""Tests for the Cloudflare cache-purge helper (sources/cloudflare.py)."""

from __future__ import annotations

import httpx
import pytest

from spicy_regs.sources import cloudflare


def _creds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "tok-123")
    monkeypatch.setenv("CLOUDFLARE_ZONE_ID", "zone-abc")


def test_purge_noop_without_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """No token/zone => no HTTP call at all (uploads must work without CF setup)."""
    monkeypatch.delenv("CLOUDFLARE_API_TOKEN", raising=False)
    monkeypatch.delenv("CLOUDFLARE_ZONE_ID", raising=False)

    def _boom(*a, **k):
        raise AssertionError("httpx.post must not be called without credentials")

    monkeypatch.setattr(httpx, "post", _boom)
    cloudflare.purge_urls(["https://data.spicy-regs.dev/agency_stats.parquet"])


def test_purge_noop_with_partial_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only one of the two vars set => still a no-op, not a malformed call."""
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "tok-123")
    monkeypatch.delenv("CLOUDFLARE_ZONE_ID", raising=False)
    monkeypatch.setattr(httpx, "post", lambda *a, **k: pytest.fail("should not call"))
    cloudflare.purge_urls(["https://data.spicy-regs.dev/x.parquet"])


def test_purge_empty_list_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    _creds(monkeypatch)
    monkeypatch.setattr(httpx, "post", lambda *a, **k: pytest.fail("should not call"))
    cloudflare.purge_urls([])


def test_purge_posts_expected_request(monkeypatch: pytest.MonkeyPatch) -> None:
    _creds(monkeypatch)
    calls: list[dict] = []

    def _fake_post(url, headers=None, json=None, timeout=None):
        calls.append({"url": url, "headers": headers, "json": json})
        return httpx.Response(200, json={"success": True, "result": {"id": "x"}})

    monkeypatch.setattr(httpx, "post", _fake_post)
    cloudflare.purge_urls(["https://data.spicy-regs.dev/agency_stats.parquet"])

    assert len(calls) == 1
    assert calls[0]["url"] == "https://api.cloudflare.com/client/v4/zones/zone-abc/purge_cache"
    assert calls[0]["headers"]["Authorization"] == "Bearer tok-123"
    assert calls[0]["json"] == {"files": ["https://data.spicy-regs.dev/agency_stats.parquet"]}


def test_purge_batches_over_thirty_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cloudflare caps purge-by-URL at 30; 70 URLs => 3 calls (30/30/10)."""
    _creds(monkeypatch)
    batch_sizes: list[int] = []

    def _fake_post(url, headers=None, json: dict | None = None, timeout=None):
        assert json is not None
        batch_sizes.append(len(json["files"]))
        return httpx.Response(200, json={"success": True})

    monkeypatch.setattr(httpx, "post", _fake_post)
    cloudflare.purge_urls([f"https://data.spicy-regs.dev/f{i}.parquet" for i in range(70)])

    assert batch_sizes == [30, 30, 10]


def test_purge_swallows_api_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-success API response is logged, not raised (publish already happened)."""
    _creds(monkeypatch)
    monkeypatch.setattr(
        httpx,
        "post",
        lambda *a, **k: httpx.Response(403, json={"success": False, "errors": [{"message": "nope"}]}),
    )
    cloudflare.purge_urls(["https://data.spicy-regs.dev/x.parquet"])  # must not raise


def test_purge_swallows_transport_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A transport error must never propagate out of a best-effort purge."""
    _creds(monkeypatch)

    def _raise(*a, **k):
        raise httpx.ConnectError("edge unreachable")

    monkeypatch.setattr(httpx, "post", _raise)
    cloudflare.purge_urls(["https://data.spicy-regs.dev/x.parquet"])  # must not raise
