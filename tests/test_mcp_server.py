"""Tests for the Spicy Regs MCP server.

Covers the canonical MCP server implementation in ``spicy_regs.mcp_server``
(the single source of truth since the Vercel copy was retired).
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID

import duckdb
import httpx
import pytest

from spicy_regs import mcp_server


def test_jsonify_primitives_pass_through():
    assert mcp_server._jsonify(None) is None
    assert mcp_server._jsonify("hi") == "hi"
    assert mcp_server._jsonify(7) == 7
    assert mcp_server._jsonify(1.5) == 1.5
    assert mcp_server._jsonify(True) is True


def test_jsonify_coerces_non_json_types():
    j = mcp_server._jsonify
    assert j(date(2024, 1, 2)) == "2024-01-02"
    assert j(datetime(2024, 1, 2, 3, 4, 5)) == "2024-01-02T03:04:05"
    assert j(Decimal("1.50")) == "1.50"
    assert j(UUID("12345678-1234-5678-1234-567812345678")) == ("12345678-1234-5678-1234-567812345678")
    assert j(b"\x00\xff") == "00ff"
    assert j([Decimal("1"), date(2024, 1, 1)]) == ["1", "2024-01-01"]
    assert j({"a": Decimal("2"), "b": [b"\xab"]}) == {"a": "2", "b": ["ab"]}


def test_resolve_r2_base_url_rejects_http(monkeypatch):
    monkeypatch.setenv("SPICY_REGS_R2_URL", "http://example.com")
    with pytest.raises(RuntimeError, match="https://"):
        mcp_server._resolve_r2_base_url()


def test_resolve_r2_base_url_rejects_injection_chars(monkeypatch):
    monkeypatch.setenv("SPICY_REGS_R2_URL", "https://evil.example/'); DROP")
    with pytest.raises(RuntimeError, match="illegal characters"):
        mcp_server._resolve_r2_base_url()


def test_resolve_r2_base_url_strips_trailing_slash(monkeypatch):
    monkeypatch.setenv("SPICY_REGS_R2_URL", "https://example.com/bucket/")
    assert mcp_server._resolve_r2_base_url() == "https://example.com/bucket"


def _tool_names(fastmcp) -> set[str]:
    tools = asyncio.run(fastmcp.list_tools())
    return {t.name for t in tools}


def test_build_server_registers_expected_tools():
    server = mcp_server.build_server()
    assert _tool_names(server) == {"list_sources", "describe_table", "query_sql", "prepare_comment", "get_rule_text", "get_data_freshness"}


# --- catalog config resolution ----------------------------------------------

_CATALOG_ENV = ("R2_CATALOG_URI", "R2_CATALOG_WAREHOUSE", "R2_CATALOG_TOKEN")


def test_resolve_catalog_config_none_when_unset(monkeypatch):
    module = mcp_server
    for var in (*_CATALOG_ENV, "R2_CATALOG_NAMESPACE"):
        monkeypatch.delenv(var, raising=False)
    assert module._resolve_catalog_config() is None


def test_resolve_catalog_config_partial_is_none(monkeypatch):
    """Missing any one of the three required vars => disabled (fall back)."""
    module = mcp_server
    monkeypatch.setenv("R2_CATALOG_URI", "https://catalog.example/x")
    monkeypatch.setenv("R2_CATALOG_WAREHOUSE", "wh")
    monkeypatch.delenv("R2_CATALOG_TOKEN", raising=False)
    assert module._resolve_catalog_config() is None


def test_resolve_catalog_config_full(monkeypatch):
    module = mcp_server
    monkeypatch.setenv("R2_CATALOG_URI", "https://catalog.example/x")
    monkeypatch.setenv("R2_CATALOG_WAREHOUSE", "wh")
    monkeypatch.setenv("R2_CATALOG_TOKEN", "secret-token")
    monkeypatch.delenv("R2_CATALOG_NAMESPACE", raising=False)
    config = module._resolve_catalog_config()
    assert config == {
        "uri": "https://catalog.example/x",
        "warehouse": "wh",
        "token": "secret-token",
        "namespace": module.DEFAULT_CATALOG_NAMESPACE,
    }


def test_resolve_catalog_config_empty_namespace_defaults(monkeypatch):
    """An empty R2_CATALOG_NAMESPACE (e.g. an unset GH secret -> "") -> default."""
    module = mcp_server
    monkeypatch.setenv("R2_CATALOG_URI", "https://catalog.example/x")
    monkeypatch.setenv("R2_CATALOG_WAREHOUSE", "wh")
    monkeypatch.setenv("R2_CATALOG_TOKEN", "t")
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "")
    config = module._resolve_catalog_config()
    assert config is not None
    assert config["namespace"] == module.DEFAULT_CATALOG_NAMESPACE


def test_resolve_catalog_config_rejects_injection(monkeypatch):
    """Values are inlined into CREATE SECRET / ATTACH, so quotes are rejected."""
    module = mcp_server
    monkeypatch.setenv("R2_CATALOG_URI", "https://catalog.example/x")
    monkeypatch.setenv("R2_CATALOG_WAREHOUSE", "wh'); DROP")
    monkeypatch.setenv("R2_CATALOG_TOKEN", "t")
    with pytest.raises(RuntimeError, match="illegal characters"):
        module._resolve_catalog_config()


# --- Connection sandbox -----------------------------------------------------
#
# These exercise the real ``_apply_security_settings`` pragmas, which is where
# both shipped runtime crashes lived (the bogus ``statement_timeout`` SET and
# the spill-to-disabled-LocalFileSystem error). They are hermetic: the sandbox
# is applied to a plain in-memory connection, so no httpfs install or network
# is needed. The live ``_build_connection`` + R2 path is covered by the integration test
# below.


def _sandboxed_connection(memory_limit: str | None = None):
    """A connection with the production read-only sandbox applied.

    ``memory_limit`` is set before the sandbox locks the configuration, so a
    test can force spilling behavior.
    """
    con = duckdb.connect()
    if memory_limit is not None:
        con.execute(f"SET memory_limit='{memory_limit}'")
    mcp_server._apply_security_settings(con)
    return con


def test_security_settings_apply_cleanly():
    """Every pragma must be accepted by the installed DuckDB (no Catalog Error).

    The original ``SET statement_timeout`` regression failed exactly here, on a
    parameter DuckDB does not recognize.
    """
    module = mcp_server
    con = duckdb.connect()
    module._apply_security_settings(con)  # must not raise


def test_sandbox_allows_in_memory_query():
    con = _sandboxed_connection()
    assert con.execute("SELECT 1 + 1").fetchone() == (2,)
    assert con.execute("SELECT count(*) FROM range(100)").fetchone() == (100,)


def test_sandbox_survives_temp_spill():
    """A query that exceeds memory must not raise the LocalFileSystem error.

    Regression for ``Permission Error: File system LocalFileSystem has been
    disabled by configuration``: ``temp_directory`` defaulted to a local
    ``.tmp`` that the sandbox forbids, so any spilling query crashed. With
    spilling disabled the query either runs in memory or fails with a clear
    out-of-memory error — never the confusing permission error.
    """
    con = _sandboxed_connection(memory_limit="20MB")
    spilling_sql = "SELECT i, count(*) AS c FROM range(3_000_000) r(i) GROUP BY i ORDER BY c, i DESC"
    try:
        con.execute(spilling_sql).fetchall()
    except duckdb.OutOfMemoryException:
        pass  # acceptable: spilling disabled, no local temp touched
    except duckdb.PermissionException as exc:
        pytest.fail(f"sandbox crashed a spilling query on local temp: {exc}")


def test_sandbox_does_not_disable_local_filesystem():
    """LocalFileSystem must stay enabled, or httpfs HTTPS reads break.

    Regression for ``Permission Error: File system LocalFileSystem has been
    disabled by configuration`` raised when a view binds: httpfs reads the
    system CA bundle off the local filesystem for the TLS handshake, so
    ``disabled_filesystems='LocalFileSystem'`` makes every R2 read fail. Guard
    against re-adding it.
    """
    con = _sandboxed_connection()
    disabled = con.execute("SELECT current_setting('disabled_filesystems')").fetchone()
    assert disabled[0] == ""


def test_sandbox_locks_configuration():
    """User SQL must not be able to relax the sandbox once it is applied."""
    con = _sandboxed_connection()
    with pytest.raises(duckdb.Error):
        con.execute("SET allow_unsigned_extensions=true")


# --- read-only statement guard ----------------------------------------------


READ_FORMS = [
    "SELECT 1",
    "WITH a AS (SELECT 1) SELECT * FROM a",
    "FROM range(3)",
    "DESCRIBE SELECT 1",
    "EXPLAIN SELECT 1",
    "SHOW ALL TABLES",
    "SUMMARIZE SELECT 1",
    "VALUES (1), (2)",
    "/* lead */ SELECT 1",
    "SELECT 1 -- COPY (SELECT 1) TO '/tmp/x.csv'",
    "SELECT 'COPY (SELECT 1) TO /tmp/x.csv' AS s",
]

WRITE_FORMS = [
    ("COPY (SELECT 1) TO '/tmp/probe.csv'", "COPY"),
    ("ATTACH '/tmp/probe.db' AS z", "ATTACH"),
    ("EXPORT DATABASE '/tmp/probe'", "EXPORT"),
    ("CREATE TABLE t (i INTEGER)", "CREATE"),
    ("CREATE VIEW v AS SELECT 1", "CREATE"),
    ("DROP TABLE IF EXISTS t", "DROP"),
    ("INSERT INTO t VALUES (1)", "INSERT"),
    ("UPDATE t SET i = 1", "UPDATE"),
    ("DELETE FROM t", "DELETE"),
    ("SET memory_limit='1GB'", "SET"),
    ("LOAD httpfs", "LOAD"),
    ("CALL pragma_version()", "CALL"),
    ("PREPARE p AS SELECT 1", "PREPARE"),
    ("BEGIN TRANSACTION", "TRANSACTION"),
]


@pytest.mark.parametrize("sql", READ_FORMS)
def test_read_forms_pass_the_guard(sql):
    """Every read shape a client might send must survive the allowlist.

    The allowlist is only {SELECT, EXPLAIN} because DuckDB's parser folds
    DESCRIBE/SHOW/SUMMARIZE/VALUES and the FROM-first shorthand into SELECT.
    If that ever stops holding, these are the cases that break.
    """
    con = _sandboxed_connection()
    assert mcp_server._first_write_statement(con, sql) is None


@pytest.mark.parametrize("sql,expected", WRITE_FORMS)
def test_write_forms_are_named_and_rejected(sql, expected):
    con = _sandboxed_connection()
    assert mcp_server._first_write_statement(con, sql) == expected


def test_guard_catches_a_write_stacked_behind_a_select():
    """A trailing write must not ride in on a leading SELECT.

    ``execute`` runs every statement in the string but returns only the last
    result, so a stacked write would otherwise land silently.
    """
    con = _sandboxed_connection()
    assert mcp_server._first_write_statement(con, "SELECT 1; DROP TABLE t") == "DROP"


def test_guard_ignores_empty_sql():
    con = _sandboxed_connection()
    assert mcp_server._first_write_statement(con, "   ") is None


def test_guard_lets_parser_errors_through():
    """Malformed SQL keeps surfacing DuckDB's message, which names the token."""
    con = _sandboxed_connection()
    with pytest.raises(duckdb.ParserException):
        mcp_server._first_write_statement(con, "SELECT ((")


def test_query_sql_refuses_a_write_without_executing_it(monkeypatch, tmp_path):
    """End-to-end: the tool returns an error and the COPY never lands on disk."""
    module = mcp_server
    module._reset_connection_cache()
    _make_local_connection(monkeypatch, module)
    server = module.build_server()
    target = tmp_path / "written.csv"

    result = asyncio.run(server.call_tool("query_sql", {"sql": f"COPY (SELECT 1) TO '{target}'", "max_rows": 1}))

    assert not target.exists()
    assert "read-only" in str(result)
    module._reset_connection_cache()


def test_query_sql_still_runs_a_select(monkeypatch):
    module = mcp_server
    module._reset_connection_cache()
    _make_local_connection(monkeypatch, module)
    server = module.build_server()

    result = asyncio.run(server.call_tool("query_sql", {"sql": "SELECT docket_count FROM agency_stats", "max_rows": 1}))

    assert "read-only" not in str(result)
    module._reset_connection_cache()


def test_comments_catalog_view_dedups_on_read():
    """The catalog-backed ``comments`` view keeps one row per comment_id.

    The R2 Data Catalog can physically carry duplicate comment_id rows: the ETL
    upsert removes superseded rows with a plain ``DELETE``, which does not reliably
    take on the catalog, so a re-merged comment_id can leave its old row behind.
    ``_build_connection`` wraps the catalog table in a per-comment_id QUALIFY so the read
    surface stays single-valued regardless. This locks in that dedup expression:
    the newest ``modify_date`` wins, exactly one row survives per id, and
    non-duplicated ids are untouched — which is also the ``count(*) ==
    count(DISTINCT comment_id)`` invariant the freshness check relies on.
    """
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE raw (comment_id VARCHAR, modify_date VARCHAR, comment VARCHAR);
        INSERT INTO raw VALUES
            ('c1', '2024-01-01', 'old'),
            ('c1', '2024-03-01', 'new'),
            ('c1', '2024-02-01', 'mid'),
            ('c2', NULL, 'only-null'),
            ('c3', '2024-05-01', 'unique');
        """
    )
    # The exact dedup wrapper used by the catalog `comments` view in `_build_connection`.
    con.execute(
        "CREATE VIEW comments AS SELECT * FROM raw "
        "QUALIFY ROW_NUMBER() OVER "
        "(PARTITION BY comment_id ORDER BY modify_date DESC NULLS LAST) = 1"
    )
    rows = con.execute("SELECT comment_id, comment FROM comments ORDER BY comment_id").fetchall()
    assert rows == [("c1", "new"), ("c2", "only-null"), ("c3", "unique")]
    counts = con.execute("SELECT count(*), count(DISTINCT comment_id) FROM comments").fetchone()
    assert counts is not None
    total, distinct = counts
    assert total == distinct == 3


@pytest.mark.integration
def test_connect_queries_r2_end_to_end():
    """Live: the real ``_build_connection`` (httpfs + R2 views) serves the MCP tools.

    Covers the full path the hermetic tests cannot — httpfs install, the R2
    parquet views, and a spilling aggregation over real data — asserting none
    of it raises. Needs outbound network; run via ``pytest -m integration``.
    """
    server = mcp_server.build_server()

    sources = asyncio.run(server.call_tool("list_sources", {}))
    assert mcp_server.TABLES[0] in str(sources)

    schema = asyncio.run(server.call_tool("describe_table", {"table": "agency_stats"}))
    assert "column" in str(schema).lower()

    # An ORDER BY over a full remote table is the spill-prone shape that
    # crashed in production (sorts everything before applying the limit);
    # ``ORDER BY 1`` keeps it schema-agnostic. Assert it returns without error.
    result = asyncio.run(
        server.call_tool(
            "query_sql",
            {"sql": "SELECT * FROM agency_stats ORDER BY 1", "max_rows": 5},
        )
    )
    assert result is not None


# --- connection caching ------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_connection_cache():
    """Keep the module-level connection cache from leaking across tests."""
    mcp_server._reset_connection_cache()
    yield
    mcp_server._reset_connection_cache()


def _make_local_connection(monkeypatch, module) -> dict[str, int]:
    """Force ``_build_connection`` to hand back a bare in-memory DuckDB.

    Keeps these tests hermetic: no httpfs, no R2, no catalog — just enough of a
    connection to prove the caching, cursor-isolation, and TTL logic wrapping it.
    Returns a ``{"n": build_count}`` dict so callers can assert how many times
    the (patched) builder actually ran.
    """
    build_count = {"n": 0}

    def _fake_build() -> duckdb.DuckDBPyConnection:
        build_count["n"] += 1
        con = duckdb.connect()
        con.execute("CREATE TABLE agency_stats AS SELECT 1 AS docket_count")
        return con

    monkeypatch.setattr(module, "_build_connection", _fake_build)
    return build_count


def test_get_connection_reuses_within_ttl(monkeypatch):
    module = mcp_server
    module._reset_connection_cache()
    build_count = _make_local_connection(monkeypatch, module)

    first = module._get_connection()
    second = module._get_connection()

    assert first is second
    assert build_count["n"] == 1
    module._reset_connection_cache()


def test_get_connection_rebuilds_past_ttl(monkeypatch):
    module = mcp_server
    module._reset_connection_cache()
    build_count = _make_local_connection(monkeypatch, module)
    # A zero TTL forces every call to treat the cache as expired.
    monkeypatch.setattr(module, "_CONNECTION_TTL_SECONDS", 0.0)

    module._get_connection()
    module._get_connection()

    assert build_count["n"] == 2
    module._reset_connection_cache()


def test_cursor_from_cached_connection_survives_rebuild(monkeypatch):
    """A cursor taken before a TTL rebuild must keep serving its query.

    This is the safety property the rebuild relies on: swapping the cached
    connection only drops the module's reference, so an in-flight request's
    cursor (which still references the old connection) is never closed.
    """
    module = mcp_server
    module._reset_connection_cache()
    _make_local_connection(monkeypatch, module)
    monkeypatch.setattr(module, "_CONNECTION_TTL_SECONDS", 0.0)

    old_cursor = module._get_connection().cursor()
    module._get_connection()  # TTL expired -> builds and caches a new connection

    # The old cursor's parent is no longer referenced by the module, but the
    # cursor holds it alive and still answers.
    assert old_cursor.execute("SELECT docket_count FROM agency_stats").fetchone() == (1,)
    module._reset_connection_cache()


def test_query_sql_reuses_one_connection_across_calls(monkeypatch):
    """Two ``query_sql`` calls should build the backing connection once."""
    module = mcp_server
    module._reset_connection_cache()
    build_count = _make_local_connection(monkeypatch, module)
    server = module.build_server()

    for _ in range(2):
        asyncio.run(server.call_tool("query_sql", {"sql": "SELECT docket_count FROM agency_stats", "max_rows": 1}))

    assert build_count["n"] == 1
    module._reset_connection_cache()


# --- memory-limit / spill env gating (Cloud Run) -----------------------------


def test_resolve_memory_limit_unset_is_none(monkeypatch):
    module = mcp_server
    monkeypatch.delenv("SPICY_REGS_MEMORY_LIMIT", raising=False)
    assert module._resolve_memory_limit() is None


@pytest.mark.parametrize("value", ["12GB", "2048MB", "75%", "16GiB"])
def test_resolve_memory_limit_valid(value, monkeypatch):
    module = mcp_server
    monkeypatch.setenv("SPICY_REGS_MEMORY_LIMIT", value)
    assert module._resolve_memory_limit() == value


@pytest.mark.parametrize("value", ["12GB'; SET x=1", "lots", "'"])
def test_resolve_memory_limit_rejects_junk(value, monkeypatch):
    module = mcp_server
    monkeypatch.setenv("SPICY_REGS_MEMORY_LIMIT", value)
    with pytest.raises(RuntimeError, match="valid size/percent"):
        module._resolve_memory_limit()


def test_resolve_temp_dir_default_empty(monkeypatch):
    """Unset => '' => spilling disabled (the safe serverless default)."""
    module = mcp_server
    monkeypatch.delenv("SPICY_REGS_TEMP_DIR", raising=False)
    assert module._resolve_temp_dir() == ""


def test_resolve_temp_dir_rejects_injection(monkeypatch):
    module = mcp_server
    monkeypatch.setenv("SPICY_REGS_TEMP_DIR", "/tmp'; SET memory_limit='1kB")
    with pytest.raises(RuntimeError, match="illegal characters"):
        module._resolve_temp_dir()


def _setting(con: duckdb.DuckDBPyConnection, name: str) -> str:
    row = con.execute(f"SELECT current_setting('{name}')").fetchone()
    assert row is not None
    return row[0]


def test_security_settings_default_disables_spill(monkeypatch):
    """With no env set, temp_directory stays '' — byte-identical to the prior default."""
    monkeypatch.setattr(mcp_server, "MEMORY_LIMIT", None)
    monkeypatch.setattr(mcp_server, "TEMP_DIR", "")
    con = duckdb.connect()
    mcp_server._apply_security_settings(con)
    assert _setting(con, "temp_directory") == ""


def test_security_settings_honor_memory_and_temp(tmp_path, monkeypatch):
    """When the env-derived module constants are set, the pragmas reflect them.

    This is the contract of the change — the actual spill/OOM behavior at a given
    limit is DuckDB's, and is exercised manually against the built container
    rather than pinned to a version-specific memory threshold here.
    """
    monkeypatch.setattr(mcp_server, "MEMORY_LIMIT", "1GB")
    monkeypatch.setattr(mcp_server, "TEMP_DIR", str(tmp_path))
    con = duckdb.connect()
    mcp_server._apply_security_settings(con)
    assert _setting(con, "temp_directory") == str(tmp_path)
    assert _setting(con, "memory_limit") not in ("", None)


def test_landing_page_renders_every_table():
    """The view list is substituted from TABLES, not hand-maintained in the HTML.

    The restored page shipped a hardcoded list that had drifted seven tables
    behind; this pins the substitution so it cannot silently rot again.
    """
    html = mcp_server._landing_page().decode("utf-8")
    assert "<!--VIEWS-->" not in html
    for table in mcp_server.TABLES:
        assert f'<code class="inline">{table}</code>' in html


def test_landing_assets_exist_in_package():
    """Both assets must ship inside the installed package — the container
    installs the wheel and has no repo checkout to read them from."""
    assert (mcp_server.STATIC_DIR / "index.html").is_file()
    assert mcp_server._landing_icon()[:8] == b"\x89PNG\r\n\x1a\n"


def test_app_serves_landing_page_and_mcp_endpoint():
    """/ serves the setup page and /mcp still speaks the protocol."""
    from starlette.testclient import TestClient

    with TestClient(mcp_server.build_app()) as client:
        page = client.get("/")
        assert page.status_code == 200
        assert page.headers["content-type"].startswith("text/html")
        assert b"Spicy Regs MCP Server" in page.content

        icon = client.get("/icon.png")
        assert icon.status_code == 200
        assert icon.headers["content-type"] == "image/png"

        handshake = client.post(
            "/mcp",
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
            },
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1"},
                },
            },
        )
        assert handshake.status_code == 200
        assert b'"spicy-regs"' in handshake.content


# --- prepare_comment ----------------------------------------------------------

_NOW = datetime(2026, 10, 12, 16, 0, tzinfo=timezone.utc)


def _comment_fixture_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE dockets AS SELECT * FROM (VALUES "
        "('USBC-2026-0628', 'USBC', 'Decennial Census residence criteria'), "
        "('EPA-HQ-OAR-2001-0001', 'EPA', 'An old rule')) t(docket_id, agency_code, title)"
    )
    con.execute(
        "CREATE TABLE documents AS SELECT * FROM (VALUES "
        "('USBC-2026-0628-0001', 'USBC-2026-0628', 'Proposed Rule', 'Residence criteria', "
        " '2026-09-10T04:00:00Z', '2026-11-03T04:59:59Z', NULL), "
        "('USBC-2026-0628-0003', 'USBC-2026-0628', 'Notice', 'Information collection', "
        " '2026-09-20T04:00:00Z', '2026-10-20T03:59:59Z', NULL), "
        "('USBC-2026-0628-0004', 'USBC-2026-0628', 'Notice', 'Withdrawn notice', "
        " '2026-09-20T04:00:00Z', '2026-12-01T04:59:59Z', 'true'), "
        "('USBC-2026-0628-0005', 'USBC-2026-0628', 'Notice', 'Not open yet', "
        " '2026-11-01T04:00:00Z', '2026-12-01T04:59:59Z', NULL), "
        "('USBC-2026-0628-0002', 'USBC-2026-0628', 'Supporting & Related Material', 'Impact analysis', "
        " '2026-09-10T04:00:00Z', NULL, NULL), "
        "('EPA-HQ-OAR-2001-0001-0001', 'EPA-HQ-OAR-2001-0001', 'Proposed Rule', 'Old', "
        " '2001-01-01T05:00:00Z', '2001-03-01T04:59:59Z', NULL)"
        ") t(document_id, docket_id, document_type, title, comment_start_date, comment_end_date, withdrawn)"
    )
    return con


def test_build_server_registers_prepare_comment():
    assert "prepare_comment" in _tool_names(mcp_server.build_server())


def test_prepare_comment_lists_open_documents_proposed_rule_first():
    result = mcp_server._prepare_comment(_comment_fixture_connection(), "USBC-2026-0628", "", _NOW)

    assert result["status"] == "open"
    assert [d["document_id"] for d in result["open_documents"]] == ["USBC-2026-0628-0001", "USBC-2026-0628-0003"]
    first = result["open_documents"][0]
    assert first["comment_url"] == "https://www.regulations.gov/commenton/USBC-2026-0628-0001"
    assert first["days_left"] == 21
    assert "draft_check" not in result
    assert "does not submit" in result["how_to_submit"]


def test_prepare_comment_reports_closed_docket_with_last_deadline():
    result = mcp_server._prepare_comment(_comment_fixture_connection(), "EPA-HQ-OAR-2001-0001", "", _NOW)

    assert result["status"] == "closed"
    assert result["open_documents"] == []
    assert result["last_comment_deadline"] == "2001-03-01T04:59:59+00:00"
    assert "comment_url" not in result["how_to_submit"]


def test_prepare_comment_unknown_docket():
    result = mcp_server._prepare_comment(_comment_fixture_connection(), "NOPE-2026-0001", "", _NOW)
    assert "not in the Spicy Regs mirror" in result["error"]


@pytest.mark.parametrize("bad", ["", "x", "USBC'; DROP TABLE dockets; --", "USBC 2026 0628"])
def test_prepare_comment_rejects_malformed_ids(bad):
    con = _comment_fixture_connection()
    result = mcp_server._prepare_comment(con, bad, "", _NOW)
    assert "Not a docket ID" in result["error"]
    assert con.execute("SELECT count(*) FROM dockets").fetchone() == (2,)


def test_prepare_comment_checks_draft_length():
    con = _comment_fixture_connection()
    fits = mcp_server._prepare_comment(con, "USBC-2026-0628", "I oppose this rule.", _NOW)["draft_check"]
    assert fits["fits_comment_box"] is True
    assert "advice" not in fits

    too_long = mcp_server._prepare_comment(con, "USBC-2026-0628", "a" * 5001, _NOW)["draft_check"]
    assert too_long["fits_comment_box"] is False
    assert "attached file" in too_long["advice"]


def test_prepare_comment_counts_utf8_bytes_against_the_limit():
    check = mcp_server._check_draft("é" * 2600)
    assert check["characters"] == 2600
    assert check["utf8_bytes"] == 5200
    assert check["fits_comment_box"] is False


def test_prepare_comment_tool_runs_through_the_server(monkeypatch):
    monkeypatch.setattr(mcp_server, "_build_connection", _comment_fixture_connection)
    server = mcp_server.build_server()

    result = asyncio.run(server.call_tool("prepare_comment", {"docket_id": "USBC-2026-0628"}))

    assert "commenton/USBC-2026-0628-0001" in str(result)


# --- get_rule_text -------------------------------------------------------------

_GOVINFO_PAGE = """<html><head><title>Federal Register</title></head><body><pre>
[Federal Register Volume 91, Number 174 (Thursday, September 10, 2026)]
[Proposed Rules]
From the Federal Register Online via the Government Publishing Office [<a href="http://www.gpo.gov">www.gpo.gov</a>]
[FR Doc No: 2026-18481]

SUMMARY: The Census Bureau is proposing to add two new parts.

DATES: Comments must be received by October 13, 2026.

SUPPLEMENTARY INFORMATION:

    The preamble discusses the residence rule in
Sec.  60.4 at length, and &ldquo;usual residence&rdquo; generally.

[[Page 57529]]

PART 60--RESIDENCE CRITERIA FOR THE DECENNIAL CENSUS OF THE
AMERICAN POPULATION

Sec.
60.1 Purpose.
60.4 Foreign citizens in the United States.

Sec.  60.1   Purpose.

    This part sets forth the criteria.


Sec.  60.4  Foreign citizens in the United States.

    (a) Citizens or lawful permanent residents. Counted at their usual
residence in accordance with

[[Page 57530]]

Sec.  60.2.
    (b) All other citizens of foreign countries. Not counted for
apportionment.

0
2. Amend Sec.  70.2 by revising paragraph (a) to read as follows:


Sec.  70.2  Restrictions on the inclusion of certain demographic
questions.

    (a) No question about race shall appear.

[FR Doc. 2026-18481 Filed 9-9-26; 8:45 am]
</pre></body></html>"""


def _rule_fixture_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE documents AS SELECT * FROM (VALUES "
        "('USBC-2026-0628-0001', 'USBC-2026-0628', 'Proposed Rule', 'Residence criteria', "
        " '2026-09-10T04:00:00Z', '2026-18481'), "
        "('USBC-2026-0628-0099', 'USBC-2026-0628', 'Notice', 'Public hearing', '2026-10-01T04:00:00Z', '2026-19999'), "
        "('USBC-2026-0628-0002', 'USBC-2026-0628', 'Supporting & Related Material', 'Analysis', "
        " '2026-09-10T04:00:00Z', NULL), "
        "('FDA_FRDOC_0001-0001', 'FDA_FRDOC_0001', 'Rule', 'Feed additive', '2026-10-14T04:00:00Z', '2026-20979')"
        ") t(document_id, docket_id, document_type, title, posted_date, fr_doc_num)"
    )
    con.execute(
        "CREATE TABLE federal_register AS SELECT * FROM (VALUES "
        "('2026-18481', 'Decennial Census residence criteria', 'Proposed Rule', '2026-09-10', '2026-10-13', "
        " 'The Census Bureau is proposing to add two new parts.', "
        ' \'[{"title": 15, "part": "60"}, {"title": 15, "part": "70"}]\', \'["0607-AA75"]\', '
        " 'https://www.federalregister.gov/d/2026-18481'), "
        "('2026-19999', 'Public hearing', 'Notice', '2026-10-01', NULL, NULL, '[]', '[]', "
        " 'https://www.federalregister.gov/d/2026-19999')"
        ") t(document_number, title, document_type, publication_date, comments_close_on, abstract, "
        "cfr_references_json, regulation_id_numbers_json, html_url)"
    )
    return con


def _fixture_fetch(calls: list | None = None):
    def fetch(number: str, published: str) -> str:
        if calls is not None:
            calls.append((number, published))
        return mcp_server._govinfo_text(_GOVINFO_PAGE)

    return fetch


def _rule(document, section="", offset=0, max_chars=8000, fetch=None):
    return mcp_server._get_rule_text(
        _rule_fixture_connection(), document, section, offset, max_chars, fetch or _fixture_fetch()
    )


def test_build_server_registers_get_rule_text():
    assert "get_rule_text" in _tool_names(mcp_server.build_server())


def test_get_rule_text_outline_from_a_docket_prefers_the_proposed_rule():
    calls: list = []
    result = _rule("USBC-2026-0628", fetch=_fixture_fetch(calls))

    assert calls == [("2026-18481", "2026-09-10")]
    assert result["mode"] == "outline"
    assert result["fr_document_number"] == "2026-18481"
    assert result["rins"] == ["0607-AA75"]
    assert [c["part"] for c in result["cfr_references"]] == ["60", "70"]
    assert result["comments_close_on_in_notice"] == "2026-10-13"
    assert result["summary"] == "The Census Bureau is proposing to add two new parts."
    assert result["dates"] == "Comments must be received by October 13, 2026."
    assert result["parts"] == [
        {"part": "60", "title": "RESIDENCE CRITERIA FOR THE DECENNIAL CENSUS OF THE AMERICAN POPULATION"}
    ]
    assert [(s["section"], s["title"]) for s in result["sections"]] == [
        ("60.1", "Purpose."),
        ("60.4", "Foreign citizens in the United States."),
        ("70.2", "Restrictions on the inclusion of certain demographic questions."),
    ]
    assert [n["document_id"] for n in result["other_notices_in_docket"]] == ["USBC-2026-0628-0099"]
    assert result["source"] == "https://www.govinfo.gov/content/pkg/FR-2026-09-10/html/2026-18481.htm"


@pytest.mark.parametrize("ref", ["USBC-2026-0628-0001", "2026-18481", "FR Doc. 2026-18481"])
def test_get_rule_text_accepts_document_ids_and_fr_numbers(ref):
    result = _rule(ref)
    assert result["fr_document_number"] == "2026-18481"
    assert "other_notices_in_docket" not in result


@pytest.mark.parametrize("section", ["60.4", "§ 60.4", "Sec. 60.4", "section 60.4"])
def test_get_rule_text_returns_one_section(section):
    text = _rule("USBC-2026-0628", section)["text"]

    assert text.startswith("Sec.  60.4  Foreign citizens in the United States.")
    assert text.endswith("Not counted for\napportionment.")
    assert "[[Page" not in text
    assert "Amend" not in text
    assert "70.2" not in text


def test_get_rule_text_ignores_section_mentions_inside_paragraphs():
    sections = [s["section"] for s in _rule("USBC-2026-0628")["sections"]]
    assert sections.count("60.4") == 1
    assert "60.2" not in sections


def test_get_rule_text_strips_markup_and_entities():
    text = _rule("2026-18481", "full", max_chars=20000)["text"]
    assert "<a" not in text and "&ldquo;" not in text
    assert "“usual residence”" in text
    assert "[[Page" not in text


def test_get_rule_text_returns_a_part_and_stops_at_the_fr_doc_trailer():
    text = _rule("2026-18481", "part 60")["text"]
    assert text.startswith("PART 60--RESIDENCE CRITERIA")
    assert "No question about race" in text
    assert "[FR Doc." not in text


def test_get_rule_text_pages_long_text():
    first = _rule("2026-18481", "full", max_chars=500)
    second = _rule("2026-18481", "full", offset=first["next_offset"], max_chars=500)

    assert len(first["text"]) == 500
    assert first["next_offset"] == 500
    assert second["offset"] == 500
    assert first["text"] + second["text"] == _rule("2026-18481", "full", max_chars=1000)["text"]
    last = _rule("2026-18481", "full", offset=first["total_chars"] - 10, max_chars=500)
    assert last["next_offset"] is None


@pytest.mark.parametrize(
    "ref, message",
    [
        ("x'; DROP TABLE documents; --", "Not a docket, document, or Federal Register number"),
        ("FDA_FRDOC_0001", "catch-all docket"),
        ("NOPE-2026-0001", "No Federal Register notice is linked"),
        ("2001-12345", "not in the mirror"),
    ],
)
def test_get_rule_text_explains_unresolvable_references(ref, message):
    result = _rule(ref)
    assert message in result["error"]


def test_get_rule_text_lists_valid_sections_for_an_unknown_one():
    result = _rule("2026-18481", "99.9")
    assert "No section 99.9" in result["error"]
    assert "60.4" in result["error"]


@pytest.mark.parametrize("kwargs", [{"max_chars": 10}, {"max_chars": 50_000}, {"offset": -1}])
def test_get_rule_text_validates_paging_arguments(kwargs):
    assert "error" in _rule("2026-18481", "full", **kwargs)


def test_get_rule_text_reports_a_fetch_failure_with_a_link():
    def failing(number: str, published: str) -> str:
        raise mcp_server.RuleTextError("govinfo has no text for this document yet")

    result = _rule("2026-18481", fetch=failing)
    assert "govinfo has no text" in result["error"]
    assert result["federal_register_url"] == "https://www.federalregister.gov/d/2026-18481"


def _patch_httpx(monkeypatch, handler):
    real_client = httpx.Client

    def client(*args, **kwargs):
        return real_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(mcp_server.httpx, "Client", client)


def test_fetch_refuses_a_redirect_off_govinfo(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "www.govinfo.gov":
            return httpx.Response(302, headers={"Location": "https://evil.example/x"})
        return httpx.Response(200, text="<pre>owned</pre>")

    _patch_httpx(monkeypatch, handler)
    with pytest.raises(mcp_server.RuleTextError, match="off its own host"):
        mcp_server._fetch_url_text("https://www.govinfo.gov/content/pkg/FR-2026-09-10/html/2026-18481.htm")


def test_fetch_caps_the_download_size(monkeypatch):
    monkeypatch.setattr(mcp_server, "RULE_TEXT_MAX_BYTES", 100)
    _patch_httpx(monkeypatch, lambda request: httpx.Response(200, content=b"x" * 1000))
    with pytest.raises(mcp_server.RuleTextError, match="larger than"):
        mcp_server._fetch_url_text("https://www.govinfo.gov/content/pkg/FR-2026-09-10/html/2026-18481.htm")


def test_fetch_turns_a_404_into_a_readable_error(monkeypatch):
    _patch_httpx(monkeypatch, lambda request: httpx.Response(404))
    with pytest.raises(mcp_server.RuleTextError, match="no text"):
        mcp_server._fetch_url_text("https://www.govinfo.gov/content/pkg/FR-2026-09-10/html/2026-18481.htm")


def test_get_rule_text_tool_runs_through_the_server(monkeypatch):
    monkeypatch.setattr(mcp_server, "_build_connection", _rule_fixture_connection)
    monkeypatch.setattr(mcp_server, "_fetch_rule_text", _fixture_fetch())
    server = mcp_server.build_server()

    result = asyncio.run(server.call_tool("get_rule_text", {"document": "USBC-2026-0628", "section": "60.4"}))

    assert "Not counted for" in str(result)


def test_observed_redacts_inputs_and_logs_failure(caplog):
    import json

    @mcp_server._observed("govinfo_http")
    def fail(secret):
        raise TimeoutError(secret)

    with pytest.raises(TimeoutError), caplog.at_level("WARNING"):
        fail("private-draft")
    event = json.loads(caplog.records[-1].message)
    assert event["outcome"] == "error"
    assert event["error_type"] == "TimeoutError"
    assert event["elapsed_ms"] >= 0
    assert "private-draft" not in caplog.text


def test_freshness_partial_failure(monkeypatch):
    for key in ("ALL_PROXY", "HTTPS_PROXY", "HTTP_PROXY", "all_proxy", "https_proxy", "http_proxy"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: (_ for _ in ()).throw(TimeoutError()))
    monkeypatch.setattr(httpx.Client, "head", lambda *a, **k: (_ for _ in ()).throw(httpx.ConnectError("offline")))
    server = mcp_server.build_server()
    result = asyncio.run(server.call_tool("get_data_freshness", {"table": "federal_register"}))
    assert "watermark_error" in str(result)
    assert "artifact_error" in str(result)
    assert "coverage_complete_through" in str(result)
