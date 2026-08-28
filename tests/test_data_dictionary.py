"""Tests for the data dictionary generator/checker.

Hermetic (no network): everything here runs against the in-code schema
(``RECORD_TYPES`` + ``DERIVED_SCHEMAS``) and the committed
``data_dictionary/descriptions.yaml``. The live-data reconciliation
(``check --source r2``) is exercised in the docs deploy workflow, not here.
"""

from __future__ import annotations

from spicy_regs import data_dictionary as dd
from spicy_regs.schemas.regulations import RECORD_TYPES


def test_record_types_covered_by_expected_schemas():
    """Every core RecordType column flows into the expected schema."""
    expected = dd.expected_schemas()
    for name, rt in RECORD_TYPES.items():
        assert name in expected
        assert [c for c, _ in expected[name]] == list(rt.schema.keys())


def test_all_tables_have_a_schema():
    expected = dd.expected_schemas()
    assert set(expected) == set(dd.TABLES)
    for cols in expected.values():
        assert cols, "schema should be non-empty"


def test_descriptions_match_schema():
    """The shipped descriptions.yaml must fully cover the in-code schema."""
    descriptions = dd.load_descriptions()
    errors = dd.check_descriptions(dd.expected_schemas(), descriptions)
    assert errors == [], "\n".join(errors)


def test_check_detects_missing_description():
    descriptions = dd.load_descriptions()
    # Drop a real column's description -> should be flagged.
    broken = {t: {**v, "columns": dict(v.get("columns") or {})} for t, v in descriptions.items()}
    broken["dockets"]["columns"].pop("rin")
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("rin" in e for e in errors)


def test_check_detects_orphan_description():
    descriptions = dd.load_descriptions()
    broken = {t: {**v, "columns": dict(v.get("columns") or {})} for t, v in descriptions.items()}
    broken["dockets"]["columns"]["not_a_real_column"] = "x"
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("not_a_real_column" in e for e in errors)


def test_check_detects_missing_summary():
    descriptions = dd.load_descriptions()
    broken = {t: {**v} for t, v in descriptions.items()}
    broken["comments"] = {**broken["comments"], "summary": ""}
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("summary" in e for e in errors)


def test_generate_writes_a_page_per_table(tmp_path):
    descriptions = dd.load_descriptions()
    written = dd.generate(descriptions, dd.expected_schemas(), out_dir=tmp_path)
    assert {p.stem for p in written} == set(dd.TABLES)
    dockets = (tmp_path / "dockets.md").read_text(encoding="utf-8")
    assert "# `dockets`" in dockets
    assert "`docket_id`" in dockets
    # The primary key is flagged.
    assert "🔑" in dockets


def test_committed_pages_are_up_to_date(tmp_path):
    """The committed docs/tables/*.md must equal a fresh generation."""
    descriptions = dd.load_descriptions()
    dd.generate(descriptions, dd.expected_schemas(), out_dir=tmp_path)
    for table in dd.TABLES:
        committed = dd.DEFAULT_DOCS_TABLES_DIR / f"{table}.md"
        assert committed.exists(), f"missing committed page for {table}"
        fresh = (tmp_path / f"{table}.md").read_text(encoding="utf-8")
        assert committed.read_text(encoding="utf-8") == fresh, (
            f"{table}.md is stale; run 'uv run spicy-regs-dict generate'"
        )


def test_mcp_queryable_subset_of_tables():
    assert dd.MCP_QUERYABLE <= set(dd.TABLES)


def test_mcp_server_tables_match_dictionary():
    """The MCP server must expose exactly the dictionary's published tables."""
    from spicy_regs import mcp_server

    assert set(mcp_server.TABLES) == set(dd.TABLES)


def test_mcp_queryable_matches_mcp_server():
    """The docs' 'queryable via MCP' flag must track what the server serves."""
    from spicy_regs import mcp_server

    assert dd.MCP_QUERYABLE == set(mcp_server.TABLES)
