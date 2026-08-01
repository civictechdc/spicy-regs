"""The primitives every digest, stable id, and JSON column is built out of."""

from __future__ import annotations

import json

import pytest

from spicy_regs.ontology.common import canonical_json


def test_canonical_json_is_deterministic_and_compact():
    """Key order and separators are fixed, because digests are taken over this."""
    assert canonical_json({"b": 1, "a": [2, {"d": 4, "c": 3}]}) == '{"a":[2,{"c":3,"d":4}],"b":1}'
    assert canonical_json({"é": "ü"}) == '{"é":"ü"}'


@pytest.mark.parametrize(
    ("value", "default_spelling"),
    [(float("nan"), "NaN"), (float("inf"), "Infinity"), (float("-inf"), "-Infinity")],
)
def test_canonical_json_refuses_a_number_json_cannot_express(value, default_spelling):
    """A digest may not be taken over text no JSON reader would accept.

    ``json.dumps`` spells these ``NaN``/``Infinity`` by default: outside JSON's
    grammar, so a parquet JSON column or a receipt carrying one is unreadable
    while its digest looks perfectly stable. Refusing is the only honest answer;
    a NaN in a digested payload is a bug upstream, not a value to serialize.
    """
    with pytest.raises(ValueError):
        canonical_json({"score": value})

    assert json.dumps({"score": value}) == f'{{"score": {default_spelling}}}', "what the default would have written"
