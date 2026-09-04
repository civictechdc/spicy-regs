"""Contracts for scoping an already-built cluster table.

Adding three derived columns by re-streaming the 2.3 GiB dump is 23 minutes of
reading for facts the table already has the key for, so the backfill joins the
docket→court map against the table on disk instead. It has one interesting
decision in it: the better artifact — the whole cluster table rewritten with the
columns inline — costs a second copy of a 3.9 GB file, and on the machine this
was written for that crosses the project's free-space floor. So the mode is
chosen by what fits, and the mode actually used is recorded, because a run that
quietly produced the lesser artifact is the shape of degradation this ingest
keeps having to guard against.
"""

from __future__ import annotations

import bz2
import importlib.util
import sys
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

_SPEC = importlib.util.spec_from_file_location(
    "backfill_cluster_court_scope",
    Path(__file__).resolve().parents[1] / "scripts" / "backfill_cluster_court_scope.py",
)
assert _SPEC and _SPEC.loader
backfill_module = importlib.util.module_from_spec(_SPEC)
sys.modules["backfill_cluster_court_scope"] = backfill_module
_SPEC.loader.exec_module(backfill_module)


def _fixtures(tmp_path: Path) -> tuple[Path, Path, Path]:
    clusters = tmp_path / "court_opinion_clusters.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"cluster_id": "1", "cl_docket_id": "10", "case_name": "Federal"},
                {"cluster_id": "2", "cl_docket_id": "11", "case_name": "State"},
                {"cluster_id": "3", "cl_docket_id": "99", "case_name": "Unplaced"},
                {"cluster_id": "4", "cl_docket_id": None, "case_name": "No docket"},
            ],
            schema=pa.schema(
                [
                    ("cluster_id", pa.string()),
                    ("cl_docket_id", pa.string()),
                    ("case_name", pa.string()),
                ]
            ),
        ),
        clusters,
    )
    docket_map = tmp_path / "docket_courts.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"cl_docket_id": "10", "court_id": "dcd"},
                {"cl_docket_id": "11", "court_id": "nc"},
            ],
            schema=pa.schema([("cl_docket_id", pa.string()), ("court_id", pa.string())]),
        ),
        docket_map,
    )
    courts = tmp_path / "courts-2026-06-30.csv.bz2"
    courts.write_bytes(bz2.compress(b"id,jurisdiction\ndcd,FD\nnc,S\n"))
    return clusters, docket_map, courts


def _run(tmp_path: Path, mode: str, monkeypatch) -> dict:
    clusters, docket_map, courts = _fixtures(tmp_path)
    if mode == "full":
        # The real guard refuses this on the machine it was written for; the
        # behaviour under test here is the rewrite, not the arithmetic.
        monkeypatch.setattr(backfill_module, "check_headroom", lambda *a, **k: None)
        monkeypatch.setattr(backfill_module, "_fits", lambda *a, **k: True)
    return backfill_module.backfill(
        clusters=clusters,
        docket_court_map=docket_map,
        courts_dump=courts,
        output_dir=tmp_path / "out",
        dump_date=date(2026, 6, 30),
        mode=mode,
    )


def test_scope_mode_writes_a_joinable_side_table(tmp_path: Path, monkeypatch):
    receipt = _run(tmp_path, "scope", monkeypatch)
    rows = pq.read_table(tmp_path / "out" / "court_cluster_scope.parquet").to_pylist()

    assert [r["cluster_id"] for r in rows] == ["1", "2", "3", "4"]
    assert rows[0] == {
        "cluster_id": "1",
        "cl_docket_id": "10",
        "court_id": "dcd",
        "court_jurisdiction": "FD",
        "court_is_federal": "t",
    }
    assert rows[1]["court_is_federal"] == "f"
    # A docket the map does not place, and a cluster with no docket at all: both
    # NULL. "We do not know" and "not federal" are different claims.
    assert rows[2]["court_is_federal"] is None
    assert rows[3]["court_id"] is None

    assert receipt["mode"] == "scope"
    assert receipt["coverage"]["denominator_count"] == 4
    assert receipt["coverage"]["clusters_in_a_federal_court"] == 1
    assert receipt["coverage"]["clusters_with_no_court"] == 2
    assert receipt["coverage"]["by_jurisdiction"]["FD"] == 1


def test_full_mode_keeps_every_original_column_and_the_published_order(tmp_path: Path, monkeypatch):
    """The scope belongs next to the key it is derived from, not bolted on the end."""
    receipt = _run(tmp_path, "full", monkeypatch)
    table = pq.read_table(tmp_path / "out" / "court_opinion_clusters.parquet")

    assert table.schema.names == [
        "cluster_id",
        "cl_docket_id",
        "court_id",
        "court_jurisdiction",
        "court_is_federal",
        "case_name",
    ]
    rows = table.to_pylist()
    assert rows[0]["case_name"] == "Federal"
    assert rows[0]["court_jurisdiction"] == "FD"
    assert receipt["mode"] == "full"
    assert receipt["coverage"]["rows_written"] == 4


def test_auto_falls_back_to_the_side_table_when_the_rewrite_would_cross_the_floor(tmp_path: Path, monkeypatch):
    """The choice is made by arithmetic, and the choice made is recorded."""
    monkeypatch.setattr(backfill_module, "_fits", lambda *a, **k: False)
    receipt = _run(tmp_path, "auto", monkeypatch)
    assert receipt["mode"] == "scope"
    assert (tmp_path / "out" / "court_cluster_scope.parquet").exists()
    assert not (tmp_path / "out" / "court_opinion_clusters.parquet").exists()

    monkeypatch.setattr(backfill_module, "_fits", lambda *a, **k: True)
    receipt = _run(tmp_path, "auto", monkeypatch)
    assert receipt["mode"] == "full"
