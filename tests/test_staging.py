"""Tests for the reusable stage_agencies engine (spicy_regs.pipelines.staging)."""

from collections.abc import Iterator
from pathlib import Path

import polars as pl

from spicy_regs.pipelines.staging import StageResult, stage_agencies
from spicy_regs.schemas import DOCKET, RecordType
from spicy_regs.sources.base import Reader


class _FakeReader(Reader):
    """Yields canned records and reports the keys it 'consumed'."""

    def __init__(self, records: list[dict], keys: list[str]) -> None:
        self._records = records
        self.last_keys = keys

    def iter_records(self) -> Iterator[dict]:
        yield from self._records


def _docket(docket_id: str) -> dict:
    return {
        "docket_id": docket_id,
        "agency_code": docket_id.split("-")[0],
        "title": docket_id,
        "docket_type": "Rulemaking",
        "modify_date": "2024-01-01",
        "abstract": None,
    }


def test_stage_agencies_aggregates_rows_and_keys(tmp_output: Path) -> None:
    data = {
        "EPA": ([_docket("EPA-1")], ["k-epa"]),
        "FDA": ([_docket("FDA-1"), _docket("FDA-2")], ["k-fda-1", "k-fda-2"]),
    }

    def read(agency: str, record_type: RecordType) -> _FakeReader:
        records, keys = data[agency]
        return _FakeReader(records, keys)

    result = stage_agencies(["EPA", "FDA"], [DOCKET], tmp_output / "staging", read, max_workers=2)

    assert isinstance(result, StageResult)
    assert result.rows_by_type == {"dockets": 3}
    assert result.consumed_keys == {"k-epa", "k-fda-1", "k-fda-2"}

    # Each agency wrote its own staging file.
    epa = pl.read_parquet(tmp_output / "staging" / "dockets" / "EPA.parquet")
    fda = pl.read_parquet(tmp_output / "staging" / "dockets" / "FDA.parquet")
    assert epa.height == 1
    assert fda.height == 2


def test_stage_agencies_empty_agency_is_zero(tmp_output: Path) -> None:
    def read(agency: str, record_type: RecordType) -> _FakeReader:
        return _FakeReader([], [])

    result = stage_agencies(["EPA"], [DOCKET], tmp_output / "staging", read)

    assert result.rows_by_type == {"dockets": 0}
    assert result.consumed_keys == set()
    assert not (tmp_output / "staging" / "dockets" / "EPA.parquet").exists()
