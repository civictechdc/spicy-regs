"""Tests for the Spicy Regs MCP server.

Covers the canonical MCP server implementation in ``spicy_regs.mcp_server``
(the single source of truth since the Vercel copy was retired).
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import duckdb
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
    assert _tool_names(server) == {"list_sources", "describe_table", "query_sql"}


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
