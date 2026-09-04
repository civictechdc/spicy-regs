"""Contracts for the court scope a cluster cannot state on its own.

``court_opinion_clusters`` is the whole CourtListener corpus — ten million
decisions from 3,361 courts — and the ``opinion-clusters`` dump carries no
``court_id`` at all, so "what have the federal courts said" was not a question
the table could answer. The answer lives on the docket, in a different 4.67 GiB
file, and these are the pieces that carry it across.

Two properties are worth pinning beyond the obvious lookup. The index must be a
dense array rather than a dict, because seventy-two million Python string keys
is gigabytes before a single value; and an unknown court must come back NULL
rather than non-federal, because "we do not know" and "not federal" are
different claims and only one of them is supported.
"""

from __future__ import annotations

import bz2
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.transforms.build_court_opinion_clusters import _shape_bulk, _shape_search
from spicy_regs.transforms.court_scope import (
    CourtScope,
    build_docket_court_map,
    court_jurisdictions,
    is_federal,
)

_JURISDICTIONS = {
    "ca9": "F",  # Ninth Circuit
    "dcd": "FD",  # District of D.C. — where APA suits mostly land
    "nyeb": "FB",  # bankruptcy
    "cavc": "FS",  # special
    "bap9": "FBP",  # bankruptcy appellate panel
    "nc": "S",  # Supreme Court of North Carolina
    "cal": "ST",
    "navajo": "TRS",  # tribal
    "asssup": "TT",  # territorial
}


def _map(tmp_path: Path, pairs: list[tuple[str, str | None]]) -> Path:
    path = tmp_path / "docket_courts.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [{"cl_docket_id": d, "court_id": c} for d, c in pairs],
            schema=pa.schema([("cl_docket_id", pa.string()), ("court_id", pa.string())]),
        ),
        path,
    )
    return path


def test_federal_is_every_jurisdiction_code_that_starts_with_f():
    """Someone else's taxonomy, decided in one place rather than at each call site."""
    assert [is_federal(code) for code in ("F", "FD", "FB", "FBP", "FS")] == [True] * 5
    assert not any(is_federal(code) for code in ("S", "ST", "SA", "SS", "TRS", "TT", "MA", "I", "C"))
    # Absence is not a jurisdiction.
    assert not is_federal(None)
    assert not is_federal("")


def test_scope_resolves_a_docket_to_its_court_and_says_when_it_cannot(tmp_path: Path):
    scope = CourtScope.from_map(
        _map(
            tmp_path,
            [("10", "ca9"), ("11", "nc"), ("12", "dcd"), ("13", None), ("14", "xyzzy")],
        ),
        _JURISDICTIONS,
    )

    assert scope.for_docket("10") == ("ca9", "F", "t")
    assert scope.for_docket("12") == ("dcd", "FD", "t")
    assert scope.for_docket("11") == ("nc", "S", "f")

    # A docket the dump does not place at all.
    assert scope.for_docket("13") == (None, None, None)
    # A docket id beyond anything the map saw, and one that is not a number.
    assert scope.for_docket("999999") == (None, None, None)
    assert scope.for_docket("not-an-id") == (None, None, None)
    assert scope.for_docket(None) == (None, None, None)

    # A court the courts dump does not describe: named, but not classified.
    # Calling it non-federal would be a claim this data cannot support.
    assert scope.for_docket("14") == ("xyzzy", None, None)


def test_index_zero_is_reserved_so_an_unplaced_docket_is_not_the_first_court(
    tmp_path: Path,
):
    """Docket 0 must not inherit whichever court happened to be seen first."""
    scope = CourtScope.from_map(_map(tmp_path, [("7", "ca9")]), _JURISDICTIONS)
    assert scope.for_docket("7") == ("ca9", "F", "t")
    assert scope.for_docket("0") == (None, None, None)


def test_index_is_dense_not_a_dict_of_seventy_two_million_strings(tmp_path: Path):
    """The whole reason this is an array: a sparse high id must not cost a row each.

    A dict would be ~100 bytes per docket; the real dump holds about 72 million
    of them. The dense index is two bytes per *addressable id*, so a single
    docket numbered 300,000 costs 600 KB and not one entry per integer below it.
    """
    scope = CourtScope.from_map(_map(tmp_path, [("300000", "dcd")]), _JURISDICTIONS)
    assert scope.for_docket("300000") == ("dcd", "FD", "t")
    assert scope.for_docket("299999") == (None, None, None)
    assert scope.size > 300_000


def test_search_rows_are_classified_by_the_same_rule_as_bulk_rows(tmp_path: Path):
    """The catch-up names its court outright; it must still mean the same thing.

    If the two halves of the table disagreed about what federal means, the
    column would be worse than absent.
    """
    scope = CourtScope.from_map(_map(tmp_path, [("500", "ca9")]), _JURISDICTIONS)

    bulk = _shape_bulk({"id": "1", "docket_id": "500", "slug": "x"}, scope=scope)
    search = _shape_search({"cluster_id": "2", "docket_id": "500", "court_id": "ca9"}, scope=scope)

    assert (bulk["court_id"], bulk["court_jurisdiction"], bulk["court_is_federal"]) == (
        "ca9",
        "F",
        "t",
    )
    assert (
        search["court_id"],
        search["court_jurisdiction"],
        search["court_is_federal"],
    ) == ("ca9", "F", "t")


def test_shaping_without_a_scope_leaves_the_columns_null_rather_than_guessing():
    """``skip_court_scope`` must produce absence, not a default."""
    row = _shape_bulk({"id": "1", "docket_id": "500", "slug": "x"})
    assert row["court_id"] is None
    assert row["court_jurisdiction"] is None
    assert row["court_is_federal"] is None


def test_map_build_streams_two_columns_and_caches_by_dump_date(tmp_path: Path):
    """46 minutes of reading for two columns — a second build must not repeat it."""
    from datetime import date

    body = (
        "id,court_id,docket_number,case_name,nature_of_suit\n"
        "1,ca9,1:20-cv-01,Alpha,\n"
        "2,nc,1:20-cv-02,Beta,899\n"
        ",dcd,1:20-cv-03,Missing id,\n"
    )
    dump = tmp_path / "dockets-2026-06-30.csv.bz2"
    dump.write_bytes(bz2.compress(body.encode()))

    out = build_docket_court_map(tmp_path, dump_date=date(2026, 6, 30), local_file=dump)
    rows = pq.read_table(out).to_pylist()
    # docket_number rides along free: the pass that reads court_id has the row in
    # hand, and it is the exact key for the duplicate docket records that hid 249
    # APA decisions behind a case-name guess.
    assert rows == [
        {"cl_docket_id": "1", "court_id": "ca9", "docket_number": "1:20-cv-01"},
        {"cl_docket_id": "2", "court_id": "nc", "docket_number": "1:20-cv-02"},
    ]

    # A cached map is reused rather than re-streamed...
    assert build_docket_court_map(tmp_path, dump_date=date(2026, 6, 30)) == out
    # ...and an interrupted pass never leaves a file that looks cached.
    assert not list(tmp_path.glob("*.partial.parquet"))

    # 46 minutes of someone else's bandwidth is a capture, and a capture that
    # cannot say what it read is a file.
    receipt = json.loads((out.with_suffix(".receipt.json")).read_text())
    assert receipt["result"]["dockets"] == 2
    assert receipt["bounds"]["rows_scanned"] == 3  # the row with no id was seen
    assert receipt["bounds"]["resumes"] == 0
    assert receipt["bounds"]["columns"] == ["cl_docket_id", "court_id", "docket_number"]
    assert receipt["source"]["local_file"] == str(dump)


def test_a_map_captured_before_docket_number_still_loads_and_says_so(tmp_path: Path):
    """111 minutes of the publisher's bandwidth is not this function's to spend.

    A cached map from before ``docket_number`` existed is still a perfectly good
    docket→court map, so it is used. What it cannot do is the duplicate-docket
    reconciliation, and a reconciliation that silently cannot run is exactly the
    failure mode this ingest keeps finding in its own record.
    """
    from datetime import date

    legacy = tmp_path / "docket_courts-2026-06-30.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [{"cl_docket_id": "10", "court_id": "ca9"}],
            schema=pa.schema([("cl_docket_id", pa.string()), ("court_id", pa.string())]),
        ),
        legacy,
    )

    from loguru import logger

    warnings: list[str] = []
    sink = logger.add(warnings.append, level="WARNING")
    try:
        assert build_docket_court_map(tmp_path, dump_date=date(2026, 6, 30)) == legacy
    finally:
        logger.remove(sink)
    assert any("docket_number" in line for line in warnings), warnings

    # And it still answers the question it was built for.
    scope = CourtScope.from_map(legacy, _JURISDICTIONS)
    assert scope.for_docket("10") == ("ca9", "F", "t")


def test_courts_dump_reads_the_publisher_s_own_jurisdiction_codes(tmp_path: Path):
    body = "id,short_name,jurisdiction\nca9,Ninth Circuit,F\nnc,North Carolina,S\nbap9,Ninth Circuit BAP,FBP\n"
    dump = tmp_path / "courts-2026-06-30.csv.bz2"
    dump.write_bytes(bz2.compress(body.encode()))
    assert court_jurisdictions(local_file=dump) == {
        "ca9": "F",
        "nc": "S",
        "bap9": "FBP",
    }
