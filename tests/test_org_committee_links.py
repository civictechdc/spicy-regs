"""Hermetic tests for the commenter-org ↔ FEC committee link table (no network).

The matcher is almost entirely SQL, so testing the Python helpers in isolation
would prove very little. Instead these tests write small ``comments.parquet`` /
``fec_committees.parquet`` fixtures and run the *real published query* over them
with DuckDB, asserting on the rows it produces: the tier a pair matches at, the
junk guards, fan-out-driven confidence, and the comment-side rollups.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.build_org_committee_links import (
    COLUMNS,
    GENERIC_ORG_CORES,
    MIN_CORE_LENGTH,
    MIN_CORE_TOKENS,
    NAME_SOURCE_ORGANIZATION_FIELD,
    OUTPUT,
    PREFIX_FANOUT_MEDIUM_MAX,
    _resolve_comments_source,
    build_org_committee_links,
    build_query,
)

_COMMENT_FIELDS = ("comment_id", "docket_id", "agency_code", "organization", "posted_date", "modify_date")


def _comment(comment_id: str, organization: str | None, **overrides: str) -> dict:
    row = {
        "comment_id": comment_id,
        "docket_id": "EPA-HQ-OAR-2025-0001",
        "agency_code": "EPA",
        "organization": organization,
        "posted_date": "2025-03-01",
        "modify_date": "2025-03-01T00:00:00Z",
    }
    row.update(overrides)
    return row


def _committee(committee_id: str, name: str, **overrides: str | None) -> dict:
    row = {
        "committee_id": committee_id,
        "name": name,
        "committee_type_full": "PAC - Qualified",
        "designation_full": "Unauthorized",
        "party_full": None,
        "organization_type_full": "Trade Association",
        "state": "DC",
    }
    row.update(overrides)
    return row


def _write(path: Path, rows: list[dict]) -> None:
    pq.write_table(pa.Table.from_pylist(rows), path)


def _run(tmp_path: Path, comments: list[dict], committees: list[dict]) -> list[dict]:
    """Materialize the fixtures and run the real published query over them."""
    _write(tmp_path / "comments.parquet", comments)
    _write(tmp_path / "fec_committees.parquet", committees)
    out = tmp_path / OUTPUT
    con = duckdb.connect()
    con.execute(build_query(str(tmp_path / "comments.parquet"), str(tmp_path / "fec_committees.parquet"), str(out)))
    rows = con.execute(f"SELECT * FROM read_parquet('{out}') ORDER BY organization, committee_id").fetchall()
    names = [d[0] for d in con.description]
    con.close()
    return [dict(zip(names, row)) for row in rows]


# --- schema ---------------------------------------------------------------


def test_published_schema_matches_the_declared_columns(tmp_path: Path) -> None:
    """The parquet the query writes has exactly COLUMNS, in order, with the declared types."""
    _write(tmp_path / "comments.parquet", [_comment("C-1", "National Association of Realtors")])
    _write(
        tmp_path / "fec_committees.parquet",
        [_committee("C00030718", "NATIONAL ASSOCIATION OF REALTORS POLITICAL ACTION COMMITTEE")],
    )
    out = tmp_path / OUTPUT
    con = duckdb.connect()
    con.execute(build_query(str(tmp_path / "comments.parquet"), str(tmp_path / "fec_committees.parquet"), str(out)))
    described = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{out}')").fetchall()
    con.close()
    assert tuple((name, dtype) for name, dtype, *_ in described) == COLUMNS


# --- match tiers ----------------------------------------------------------


def test_core_tier_strips_pac_decoration_from_the_committee_name(tmp_path: Path) -> None:
    """The workhorse case: "<ORG> POLITICAL ACTION COMMITTEE" resolves to <ORG>."""
    rows = _run(
        tmp_path,
        [_comment("C-1", "National Association of Realtors")],
        [_committee("C00030718", "NATIONAL ASSOCIATION OF REALTORS POLITICAL ACTION COMMITTEE")],
    )
    assert len(rows) == 1
    assert rows[0]["committee_id"] == "C00030718"
    assert rows[0]["match_method"] == "core"
    assert rows[0]["confidence"] == "high"
    assert rows[0]["organization_core"] == "NATIONAL ASSOCIATION OF REALTORS"
    assert rows[0]["name_source"] == NAME_SOURCE_ORGANIZATION_FIELD


def test_exact_tier_outranks_core_for_the_same_pair(tmp_path: Path) -> None:
    """A pair reachable by two tiers is published once, at the strongest tier."""
    rows = _run(
        tmp_path,
        [_comment("C-1", "Pipeline Safety Trust")],
        [_committee("C00000001", "Pipeline Safety Trust")],
    )
    assert [(r["match_method"], r["confidence"]) for r in rows] == [("exact", "high")]


def test_prefix_tier_matches_a_decorated_committee_name(tmp_path: Path) -> None:
    """APTA → "... PHYSICAL THERAPY POLITICAL ACTION COMMITTEE" is a prefix match."""
    rows = _run(
        tmp_path,
        [_comment("C-1", "American Physical Therapy Association")],
        [_committee("C00000002", "AMERICAN PHYSICAL THERAPY ASSOCIATION PHYSICAL THERAPY POLITICAL ACTION COMMITTEE")],
    )
    assert [(r["match_method"], r["confidence"]) for r in rows] == [("prefix", "medium")]


def test_normalization_ignores_punctuation_case_and_parentheticals(tmp_path: Path) -> None:
    """ "Natural Resources Defense Council (NRDC)" resolves despite the acronym aside."""
    rows = _run(
        tmp_path,
        [_comment("C-1", "Natural Resources Defense Council (NRDC)")],
        [_committee("C00000003", "Natural Resources Defense Council, Inc.")],
    )
    assert len(rows) == 1
    assert rows[0]["organization_core"] == "NATURAL RESOURCES DEFENSE COUNCIL"


def test_ampersand_and_apostrophes_normalize_the_same_on_both_sides(tmp_path: Path) -> None:
    """`&` expands to AND and apostrophes (straight, curly, doubled) drop out."""
    rows = _run(
        tmp_path,
        [_comment("C-1", "America’s Bricks & Mortar Association")],
        [_committee("C00000004", "AMERICA''S BRICKS AND MORTAR ASSOCIATION PAC")],
    )
    assert len(rows) == 1
    assert rows[0]["organization_core"] == "AMERICAS BRICKS AND MORTAR ASSOCIATION"


# --- junk guards ----------------------------------------------------------


def test_blocklisted_core_does_not_match(tmp_path: Path) -> None:
    """ "New Mexico" as an organization prefix-matched 51 committees in live data."""
    rows = _run(
        tmp_path,
        [_comment("C-1", "New Mexico")],
        [_committee("C00000005", "NEW MEXICO CATTLE GROWERS ASSOCIATION PAC")],
    )
    assert rows == []


def test_blocklist_only_blocks_the_whole_core_not_a_prefix_of_a_real_name(tmp_path: Path) -> None:
    """Blocking "NEW MEXICO" must not suppress a real org whose name starts with it."""
    rows = _run(
        tmp_path,
        [_comment("C-1", "New Mexico Cattle Growers Association")],
        [_committee("C00000005", "NEW MEXICO CATTLE GROWERS ASSOCIATION PAC")],
    )
    assert [r["match_method"] for r in rows] == ["core"]


def test_bare_acronym_is_below_the_token_guard(tmp_path: Path) -> None:
    """A one-token core ("NRDC") is too thin to match on and is dropped."""
    rows = _run(
        tmp_path,
        [_comment("C-1", "NRDC")],
        [_committee("C00000006", "NRDC ACTION FUND PAC")],
    )
    assert rows == []


def test_short_core_is_below_the_length_guard(tmp_path: Path) -> None:
    """Two tokens but under MIN_CORE_LENGTH characters still fails the guard."""
    rows = _run(
        tmp_path,
        [_comment("C-1", "A B")],
        [_committee("C00000007", "A B POLITICAL ACTION COMMITTEE")],
    )
    assert rows == []


def test_null_and_blank_organizations_are_skipped(tmp_path: Path) -> None:
    rows = _run(
        tmp_path,
        [_comment("C-1", None), _comment("C-2", "   ")],
        [_committee("C00000008", "SOME TRADE ASSOCIATION PAC")],
    )
    assert rows == []


# --- fan-out / confidence -------------------------------------------------


def test_prefix_fanout_above_the_threshold_is_demoted_to_low(tmp_path: Path) -> None:
    """Affiliate networks are kept but labelled, not truncated."""
    affiliates = [
        _committee(f"C0000010{i}", f"PLANNED PARENTHOOD OF STATE {i} POLITICAL ACTION COMMITTEE")
        for i in range(PREFIX_FANOUT_MEDIUM_MAX + 2)
    ]
    rows = _run(tmp_path, [_comment("C-1", "Planned Parenthood")], affiliates)
    assert len(rows) == len(affiliates)
    assert {r["confidence"] for r in rows} == {"low"}
    assert {r["committee_match_count"] for r in rows} == {len(affiliates)}


def test_small_fanout_stays_medium(tmp_path: Path) -> None:
    affiliates = [
        _committee(f"C0000020{i}", f"PLANNED PARENTHOOD OF STATE {i} POLITICAL ACTION COMMITTEE")
        for i in range(PREFIX_FANOUT_MEDIUM_MAX)
    ]
    rows = _run(tmp_path, [_comment("C-1", "Planned Parenthood")], affiliates)
    assert {r["confidence"] for r in rows} == {"medium"}


def test_high_confidence_tiers_ignore_fanout(tmp_path: Path) -> None:
    """An exact/core match stays `high` however many committees share the name."""
    twins = [_committee(f"C0000030{i}", "DEFENDERS OF WILDLIFE ACTION FUND") for i in range(7)]
    rows = _run(tmp_path, [_comment("C-1", "Defenders of Wildlife Action Fund")], twins)
    assert {r["confidence"] for r in rows} == {"high"}
    assert {r["committee_match_count"] for r in rows} == {7}


# --- comment-side rollups -------------------------------------------------


def test_comment_rollups_count_dockets_agencies_and_dates(tmp_path: Path) -> None:
    rows = _run(
        tmp_path,
        [
            _comment("C-1", "Pipeline Safety Trust", docket_id="EPA-1", agency_code="EPA", posted_date="2025-01-05"),
            _comment("C-2", "Pipeline Safety Trust", docket_id="EPA-1", agency_code="EPA", posted_date="2025-02-05"),
            _comment("C-3", "Pipeline Safety Trust", docket_id="DOI-9", agency_code="DOI", posted_date="2025-03-05"),
        ],
        [_committee("C00000009", "PIPELINE SAFETY TRUST PAC")],
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["comment_count"] == 3
    assert row["docket_count"] == 2
    assert json.loads(row["agency_codes_json"]) == ["DOI", "EPA"]
    assert row["first_comment_date"] == "2025-01-05"
    assert row["last_comment_date"] == "2025-03-05"


def test_duplicate_comment_ids_are_deduplicated_newest_modify_date_wins(tmp_path: Path) -> None:
    """Matches the MCP `comments` view, so counts agree between the two."""
    rows = _run(
        tmp_path,
        [
            _comment("C-1", "Pipeline Safety Trust", modify_date="2025-01-01T00:00:00Z"),
            _comment("C-1", "Pipeline Safety Trust", modify_date="2025-06-01T00:00:00Z"),
            _comment("C-2", "Pipeline Safety Trust"),
        ],
        [_committee("C00000009", "PIPELINE SAFETY TRUST PAC")],
    )
    assert [r["comment_count"] for r in rows] == [2]


# --- plumbing -------------------------------------------------------------


def test_comments_source_prefers_a_local_file(tmp_path: Path) -> None:
    local = tmp_path / "comments.parquet"
    local.touch()
    assert _resolve_comments_source(tmp_path) == str(local)


def test_comments_source_falls_back_to_the_public_bucket(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.example.dev/")
    assert _resolve_comments_source(tmp_path) == "https://data.example.dev/comments.parquet"


@pytest.mark.parametrize("bad", ["http://data.example.dev", "https://data.example.dev/'; DROP TABLE x --"])
def test_comments_source_rejects_an_unsafe_public_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bad: str
) -> None:
    monkeypatch.setenv("R2_PUBLIC_URL", bad)
    with pytest.raises(RuntimeError):
        _resolve_comments_source(tmp_path)


def test_build_requires_the_committees_input(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="fec_committees.parquet"):
        build_org_committee_links(tmp_path)


def test_guards_and_blocklist_are_self_consistent() -> None:
    """Every blocklist entry would otherwise clear the guards (else it is dead weight)."""
    for entry in GENERIC_ORG_CORES:
        assert entry == entry.upper(), f"{entry!r} must be normalized (uppercase)"
        if len(entry) >= MIN_CORE_LENGTH and len(entry.split(" ")) >= MIN_CORE_TOKENS:
            continue
        pytest.fail(f"{entry!r} is already excluded by the length/token guards")
