"""Tests for the PDF fetch source (hermetic via httpx MockTransport)."""

import httpx

from spicy_regs.sources.pdf import fetch_pdf_bytes


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=True)


def test_fetch_returns_bytes_on_200() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"%PDF-1.4 fake")

    with _client(handler) as client:
        assert fetch_pdf_bytes("https://example.com/a.pdf", client=client) == b"%PDF-1.4 fake"


def test_fetch_returns_none_on_404() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    with _client(handler) as client:
        assert fetch_pdf_bytes("https://example.com/missing.pdf", client=client) is None


def test_fetch_returns_none_on_oversize_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 100)

    with _client(handler) as client:
        assert fetch_pdf_bytes("https://example.com/big.pdf", client=client, max_bytes=10) is None


def test_fetch_returns_none_on_transport_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    with _client(handler) as client:
        assert fetch_pdf_bytes("https://example.com/a.pdf", client=client) is None


def test_fetch_sends_browser_user_agent() -> None:
    # downloads.regulations.gov 403s the default python-httpx UA, so a
    # browser-like User-Agent must be sent on the request.
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["ua"] = request.headers.get("user-agent", "")
        return httpx.Response(200, content=b"%PDF-1.4 fake")

    with _client(handler) as client:
        fetch_pdf_bytes("https://example.com/a.pdf", client=client)

    assert seen["ua"].startswith("Mozilla/")
    assert "python-httpx" not in seen["ua"]
