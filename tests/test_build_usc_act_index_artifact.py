"""The act index seals what it read, and names what it could not read."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "build_usc_act_index_artifact", REPO_ROOT / "tools" / "build_usc_act_index_artifact.py"
)
mod = importlib.util.module_from_spec(_spec)
sys.modules["build_usc_act_index_artifact"] = mod
_spec.loader.exec_module(mod)

POPULAR_NAMES = """
<div id='CleanAirAct' class='popular-name-table-entry' release-point='119-102'>
    <p class='popular-name'>Clean Air Act</p>
    <p class='popular-name-information' content-type='cite' t3searchkey='1955:360' usckey='42:7401'>July 14, 1955, ch. 360</p>
  </div>
<div id='AirPollutionControlAct' class='popular-name-table-entry' release-point='119-102'>
    <p class='popular-name'>Air Pollution Control Act</p>
    <p class='popular-name-information' content-type='see'>See Clean Air Act</p>
  </div>
<div id='Broken' class='popular-name-table-entry' release-point='119-102'>
    <p class='popular-name'>Broken Act</p>
    <p class='popular-name-information' content-type='cite' t3searchkey='99-999'>x</p>
  </div>
"""
TABLE3 = """
  <tr class="table3row_odd">
   <td class="actsection">111</td><td class="statutesatlargepage"></td>
   <td class="unitedstatescodetitle">42</td>
   <td class="unitedstatescodesection">7411</td><td class="unitedstatescodestatus"></td>
  </tr>
"""


@pytest.fixture
def stub_fetch(monkeypatch):
    """Serve the fixtures; make one act's page fail, as 119-21 really does."""

    def _fetch(url, *, cache_dir=None):
        if url == mod.POPULAR_NAMES_URL:
            return POPULAR_NAMES
        if url.endswith("1955_360.htm"):
            return TABLE3
        raise RuntimeError("peer closed connection without sending complete message body")

    monkeypatch.setattr(mod, "fetch", _fetch)


def _build(tmp_path, acts):
    return mod.build(tmp_path, cache_dir=None, detection_path=None, act_keys=acts)


def test_the_artifact_seals_both_tables_and_a_receipt(tmp_path, stub_fetch):
    receipt = _build(tmp_path, ["clean air act"])

    assert receipt["schema_version"] == "usc-act-index-artifact-v2"
    assert receipt["parser_version"] == "uscode-olrc-parser-v2"
    assert receipt["coverage"]["distinct_names"] == 3
    assert receipt["coverage"]["act_section_rows"] == 1
    names = pq.read_table(tmp_path / "usc-popular-names.parquet").to_pylist()
    assert {r["name_key"] for r in names} == {"clean air act", "air pollution control act", "broken act"}
    (section,) = pq.read_table(tmp_path / "usc-act-sections.parquet").to_pylist()
    assert (section["table3_key"], section["act_section"], section["usc_section"]) == ("1955:360", "111", "7411")


def test_every_output_is_pinned_by_digest_and_row_count(tmp_path, stub_fetch):
    receipt = _build(tmp_path, ["clean air act"])

    for pinned, entry in receipt["outputs"].items():
        assert entry["digest"].startswith("sha256:")
        assert entry["rows"] == pq.ParquetFile(tmp_path / Path(pinned).name).metadata.num_rows
    assert receipt["inputs"]["popular_names_digest"].startswith("sha256:")


def test_a_rebuild_is_byte_identical(tmp_path, stub_fetch):
    """Determinism is the whole claim a digest makes."""
    first, second = tmp_path / "a", tmp_path / "b"
    _build(first, ["clean air act"])
    _build(second, ["clean air act"])

    for name in ("usc-popular-names.parquet", "usc-act-sections.parquet", "receipt.json"):
        assert (first / name).read_bytes() == (second / name).read_bytes(), name


def test_an_unreadable_page_is_a_named_hole_not_a_missing_row(tmp_path, stub_fetch):
    """The 119-21 case. Downstream must see a hole, never a wrong answer.

    `uscode.house.gov` times out rendering the One Big Beautiful Bill Act's
    Table III page — reproducibly, after 30 s, returning a truncated body with
    no classification rows. Recording it as `source_incomplete` is what lets
    `act_index` answer `source_incomplete` for citations into that act instead
    of the false `act_section_not_classified`.
    """
    receipt = _build(tmp_path, ["clean air act", "broken act"])

    (hole,) = receipt["source_incomplete"]
    assert hole["table3_key"] == "99-999"
    assert hole["url"].endswith("99_999.htm")
    assert hole["missing"] == "all classifications for this act"
    assert "peer closed connection" in hole["detail"]
    # The receipt says what the SERVER did, not only that the client gave up.
    assert "observed" in hole
    assert receipt["coverage"]["acts_incomplete"] == 1
    assert receipt["coverage"]["quarantine_reasons"] == {"source_incomplete": 1}
    quarantined = pq.read_table(tmp_path / "quarantine.parquet").to_pylist()
    assert [(r["source"], r["reason"], r["table3_key"]) for r in quarantined] == [
        ("table3", "source_incomplete", "99-999")
    ]


def test_the_division_discriminator_is_pinned_with_its_derivation(tmp_path, stub_fetch):
    """v2 exists for this: the discriminator, and where each half comes from."""
    rules = _build(tmp_path, ["clean air act"])["rules"]

    assert rules["division_discriminator"] == "act-division-statutes-at-large-range-v1"
    derivation = rules["division_discriminator_derivation"]
    assert "neither is inferred" in derivation
    assert "div. EE" in derivation and "134 Stat. 3038" in derivation
    # The one derived step is named as derived.
    assert "division END is derived" in derivation


def test_the_alias_year_rule_is_pinned_with_its_derivation(tmp_path, stub_fetch):
    """The rule that reaches ERISA is a receipt fact, not folklore in a docstring."""
    rules = _build(tmp_path, ["clean air act"])["rules"]

    assert rules["alias_year_rule"] == "supply-trailing-year-when-exactly-one-act-supplies-it"
    assert "SUPPLIED but never DROPPED" in rules["alias_year_rule_derivation"]
    assert "1966, 1970 and 1977" in rules["alias_year_rule_derivation"]
    assert rules["unresolved_reasons"] == list(mod.UNRESOLVED_REASONS)


def test_an_act_the_index_cannot_name_is_quarantined_with_a_reason(tmp_path, stub_fetch):
    receipt = _build(tmp_path, ["clean air act", "no such act"])

    quarantined = pq.read_table(tmp_path / "quarantine.parquet").to_pylist()
    assert ("requested_acts", "act_not_in_index", "no such act") in {
        (r["source"], r["reason"], r["raw_value"]) for r in quarantined
    }
    assert receipt["coverage"]["quarantine_reasons"]["act_not_in_index"] == 1


def test_an_alias_reaches_the_act_it_points_at(tmp_path, stub_fetch):
    """Requesting the alias pulls the same Table III page the act does."""
    receipt = _build(tmp_path, ["air pollution control act"])
    assert receipt["coverage"]["act_section_rows"] == 1


def test_the_receipt_records_repo_relative_paths(tmp_path, stub_fetch):
    """Absolute scratch paths would make two rebuilds differ for no reason."""
    receipt = _build(tmp_path, ["clean air act"])
    assert all(not Path(p).is_absolute() for p in receipt["outputs"])


def test_a_secret_like_value_is_refused_rather_than_sealed(tmp_path, stub_fetch, monkeypatch):
    leaked = POPULAR_NAMES.replace("Clean Air Act</p>", "Clean Air Act sk-proj-AAAABBBBCCCCDDDDEEEEFFFF</p>", 1)
    monkeypatch.setattr(mod, "fetch", lambda url, *, cache_dir=None: leaked if url == mod.POPULAR_NAMES_URL else TABLE3)
    with pytest.raises(SystemExit, match="secret"):
        _build(tmp_path, ["clean air act"])


def test_the_sealed_artifact_on_disk_matches_its_own_receipt():
    """The committed receipt describes the committed bytes, or it describes nothing."""
    artifact = REPO_ROOT / "output" / "usc-act-index-2026-08-02"
    if not (artifact / "receipt.json").exists():
        pytest.skip("artifact not built in this checkout (output/ is gitignored)")
    receipt = json.loads((artifact / "receipt.json").read_text())
    for pinned, entry in receipt["outputs"].items():
        path = REPO_ROOT / pinned
        assert mod.file_sha256(path) == entry["digest"], pinned
        assert pq.ParquetFile(path).metadata.num_rows == entry["rows"], pinned
