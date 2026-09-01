"""Tests for the Cloudflare cache-purge helper (sources/cloudflare.py)."""

from __future__ import annotations

from datetime import date

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


# --- purge-credential watchdog (evaluate_token_status / verify_token) --------


ACTIVE = {"success": True, "result": {"id": "t1", "status": "active"}}
TODAY = date(2026, 9, 1)


def test_healthy_token_has_no_problems() -> None:
    assert cloudflare.evaluate_token_status(200, ACTIVE, TODAY) == []


def test_401_is_reported_with_cloudflare_message() -> None:
    """The exact shape the live API returned once the token lapsed."""
    payload = {"success": False, "errors": [{"code": 10000, "message": "Authentication error"}]}
    problems = cloudflare.evaluate_token_status(401, payload, TODAY)
    assert len(problems) == 1
    assert "Authentication error" in problems[0]


def test_non_200_without_error_body_still_reports() -> None:
    problems = cloudflare.evaluate_token_status(500, {}, TODAY)
    assert problems == ["token rejected by Cloudflare (HTTP 500)"]


def test_inactive_status_is_a_problem() -> None:
    payload = {"success": True, "result": {"status": "disabled"}}
    assert "disabled" in cloudflare.evaluate_token_status(200, payload, TODAY)[0]


def test_expired_token_is_a_problem() -> None:
    payload = {"success": True, "result": {"status": "active", "expires_on": "2026-08-17T00:00:00Z"}}
    problems = cloudflare.evaluate_token_status(200, payload, TODAY)
    assert "expired on 2026-08-17" in problems[0]
    assert "15d ago" in problems[0]


def test_imminent_expiry_is_a_problem_before_it_bites() -> None:
    """The whole point: catch it while the token still works."""
    payload = {"success": True, "result": {"status": "active", "expires_on": "2026-09-08T00:00:00Z"}}
    problems = cloudflare.evaluate_token_status(200, payload, TODAY)
    assert "expires on 2026-09-08" in problems[0]
    assert "in 7d" in problems[0]


def test_distant_expiry_is_fine() -> None:
    payload = {"success": True, "result": {"status": "active", "expires_on": "2027-09-01T00:00:00Z"}}
    assert cloudflare.evaluate_token_status(200, payload, TODAY) == []


def test_unparseable_expiry_is_surfaced_not_swallowed() -> None:
    payload = {"success": True, "result": {"status": "active", "expires_on": "whenever"}}
    assert "could not parse" in cloudflare.evaluate_token_status(200, payload, TODAY)[0]


def test_verify_token_returns_none_without_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """None is 'unconfigured', distinct from [] meaning 'configured and healthy'."""
    monkeypatch.delenv("CLOUDFLARE_API_TOKEN", raising=False)
    monkeypatch.delenv("CLOUDFLARE_ZONE_ID", raising=False)
    monkeypatch.setattr(httpx, "get", lambda *a, **k: pytest.fail("should not call"))
    assert cloudflare.verify_token() is None


def test_verify_token_sends_bearer_and_reads_result(monkeypatch: pytest.MonkeyPatch) -> None:
    _creds(monkeypatch)
    seen: dict = {}

    def _get(url, headers=None, timeout=None):
        seen["url"] = url
        seen["headers"] = headers
        return httpx.Response(200, json=ACTIVE)

    monkeypatch.setattr(httpx, "get", _get)
    assert cloudflare.verify_token(today=TODAY) == []
    assert seen["url"] == cloudflare.TOKEN_VERIFY_URL
    assert seen["headers"]["Authorization"] == "Bearer tok-123"


def test_verify_token_reports_transport_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _creds(monkeypatch)

    def _boom(*a, **k):
        raise httpx.ConnectError("no route")

    monkeypatch.setattr(httpx, "get", _boom)
    problems = cloudflare.verify_token(today=TODAY)
    assert problems is not None and "could not reach Cloudflare" in problems[0]


def test_verify_token_survives_a_non_json_body(monkeypatch: pytest.MonkeyPatch) -> None:
    """An HTML error page from a proxy must report, not raise."""
    _creds(monkeypatch)
    monkeypatch.setattr(httpx, "get", lambda *a, **k: httpx.Response(502, text="<html>bad gateway"))
    problems = cloudflare.verify_token(today=TODAY)
    assert problems is not None and "HTTP 502" in problems[0]
