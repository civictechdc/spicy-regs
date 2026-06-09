"""Tests for the RecordType dataclass."""

from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from spicy_regs.schemas import RecordType


def _identity(d: dict) -> dict:
    return d


def _valid_kwargs(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "name": "widgets",
        "path_pattern": "/widgets/",
        "schema": {"widget_id": str, "modify_date": str},
        "dedup_key": "widget_id",
        "extract": _identity,
    }
    base.update(overrides)
    return base


def test_valid_instance_round_trips() -> None:
    rt = RecordType(**_valid_kwargs())
    assert rt.name == "widgets"
    assert rt.path_pattern == "/widgets/"
    assert rt.dedup_key == "widget_id"
    assert rt.extract({"widget_id": "1", "modify_date": "2024-01-01"}) == {
        "widget_id": "1",
        "modify_date": "2024-01-01",
    }


def test_is_frozen() -> None:
    rt = RecordType(**_valid_kwargs())
    with pytest.raises(FrozenInstanceError):
        rt.name = "other"  # ty: ignore[invalid-assignment]


def test_dedup_key_must_be_in_schema() -> None:
    with pytest.raises(ValueError, match="dedup_key"):
        RecordType(**_valid_kwargs(dedup_key="missing"))


def test_modify_date_must_be_in_schema() -> None:
    with pytest.raises(ValueError, match="modify_date"):
        RecordType(
            **_valid_kwargs(
                schema={"widget_id": str},
                dedup_key="widget_id",
            )
        )


def test_equality() -> None:
    a = RecordType(**_valid_kwargs())
    b = RecordType(**_valid_kwargs())
    assert a == b


def test_inequality_on_different_field() -> None:
    a = RecordType(**_valid_kwargs())
    b = RecordType(**_valid_kwargs(name="other"))
    assert a != b
