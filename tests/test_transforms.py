"""Tests for the Transform base class and the ExtractRecords transform."""

import pytest

from spicy_regs.schemas import COMMENT, DOCKET
from spicy_regs.transforms import ExtractRecords, Transform


def test_transform_is_abstract() -> None:
    with pytest.raises(TypeError):
        Transform()  # type: ignore[abstract]


def test_extract_records_flattens_raw_payloads() -> None:
    raw = {
        "data": {
            "id": '"EPA-2024-0001"',
            "attributes": {
                "agencyId": "EPA",
                "title": "Clean Air",
                "docketType": "Rulemaking",
                "modifyDate": "2024-06-15",
                "dkAbstract": "Proposed rule",
                "rin": "2060-AG12",
            },
        }
    }
    out = list(ExtractRecords(DOCKET).apply([raw]))
    assert out == [
        {
            "docket_id": "EPA-2024-0001",  # surrounding quotes stripped by the extractor
            "agency_code": "EPA",
            "title": "Clean Air",
            "docket_type": "Rulemaking",
            "modify_date": "2024-06-15",
            "abstract": "Proposed rule",
            "rin": "2060-AG12",
        }
    ]


def test_extract_records_is_lazy_and_streams() -> None:
    raw = {"data": {"id": "C-1", "attributes": {"docketId": "D-1"}}}
    result = ExtractRecords(COMMENT).apply(r for r in [raw])
    # apply returns an iterator, not a materialized list.
    assert iter(result) is result
    rows = list(result)
    assert rows[0]["comment_id"] == "C-1"


def test_extract_records_preserves_order_and_count() -> None:
    raws = [
        {"data": {"id": f"EPA-{i}", "attributes": {"agencyId": "EPA"}}}
        for i in range(3)
    ]
    out = list(ExtractRecords(DOCKET).apply(raws))
    assert [r["docket_id"] for r in out] == ["EPA-0", "EPA-1", "EPA-2"]
