"""The sealed U.S. Code source-credit artifact, built from a synthetic release zip.

The builder's whole claim is that one archive yields one set of bytes: the
receipt names digests, so a build that is not reproducible is a receipt that
lies. These tests exercise that claim and the two refusals the build carries --
a triple naming several sections is marked rather than dropped, and a
secret-shaped value stops the seal.

pyarrow writes the parquet and polars reads it back, so the read-back cannot
agree with the writer merely by sharing its assumptions.
"""

from __future__ import annotations

import os
import subprocess
import sys
import zipfile
from pathlib import Path

import polars as pl
import pytest

from scripts.build_usc_source_credit_artifact import build

REPO_ROOT = Path(__file__).resolve().parents[1]

USLM = "http://xml.house.gov/schemas/uslm/1.0"


def _title(identifier: str, body: str) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<uscDoc xmlns="{USLM}" identifier="{identifier}"><main>{body}</main></uscDoc>""".encode()


def _section(identifier: str, credit: str) -> str:
    return f"""<section identifier="{identifier}"><num>§ x.</num>
<sourceCredit>{credit}</sourceCredit></section>"""


_ADDED_107 = (
    '(Added <ref href="/us/pl/116/260/dEE/tI/s107">Pub. L. 116–260, div. EE, title I, § 107(d)(1)</ref>, '
    '<ref href="/us/stat/134/3048">134 Stat. 3048</ref>.)'
)
_ADDED_303 = (
    '(Added <ref href="/us/pl/117/328/dT/tIII/s303">Pub. L. 117–328, div. T, title III, § 303</ref>, '
    '<ref href="/us/stat/136/5339">136 Stat. 5339</ref>.)'
)


def _archive(tmp_path: Path, name: str, members: dict[str, bytes]) -> Path:
    path = tmp_path / name
    with zipfile.ZipFile(path, "w") as bundle:
        for member, payload in sorted(members.items()):
            bundle.writestr(member, payload)
    return path


@pytest.fixture
def archive(tmp_path: Path) -> Path:
    """Two titles: act section 107 names two Code sections, 303 names one, plus one appendix identifier."""
    return _archive(
        tmp_path,
        "uscall.zip",
        {
            "usc26.xml": _title(
                "/us/usc/t26",
                _section("/us/usc/t26/s6038E", _ADDED_107) + _section("/us/usc/t26/s6038F", _ADDED_107),
            ),
            "usc16.xml": _title(
                "/us/usc/t16",
                _section("/us/usc/t16/s824s–1", _ADDED_303) + _section("/us/usc/t18a/pl/91/538/s1", _ADDED_303),
            ),
        },
    )


def test_a_triple_naming_two_sections_is_kept_and_marked_not_dropped(tmp_path: Path, archive: Path):
    """ "The source said two things" is a different fact from "it said nothing"."""
    receipt = build(tmp_path / "out", archive=archive, release_point="119-102")

    rows = pl.read_parquet(tmp_path / "out" / "usc-source-credits.parquet").sort("usc_section")
    assert rows["usc_section"].to_list() == ["6038E", "6038F", "824s-1"]
    # 107 named two sections, so both its rows carry the refusal; 303 named one.
    assert rows["refusal"].to_list() == ["multi_target", "multi_target", None]
    assert rows["target_count"].to_list() == ["2", "2", "1"]
    # The en dash is straightened in usc_section and kept verbatim alongside it.
    assert rows["usc_identifier"].to_list()[2] == "/us/usc/t16/s824s–1"
    assert receipt["coverage"]["multi_target_triples"] == 1
    assert receipt["coverage"]["unambiguous_triples"] == 1


def test_a_section_identifier_the_usc_shape_cannot_spell_reaches_the_quarantine_file(tmp_path: Path, archive: Path):
    receipt = build(tmp_path / "out", archive=archive, release_point="119-102")

    quarantine = pl.read_parquet(tmp_path / "out" / "quarantine.parquet")
    assert quarantine["raw_value"].to_list() == ["/us/usc/t18a/pl/91/538/s1"]
    assert quarantine["reason"].to_list() == ["section_identifier_unparsable"]
    assert receipt["coverage"]["quarantine_reasons"] == {"section_identifier_unparsable": 1}


def test_two_builds_of_one_archive_write_the_same_bytes(tmp_path: Path, archive: Path):
    """The receipt names digests, so a non-reproducible build is a receipt that lies.

    The second build runs in a fresh interpreter under a different
    ``PYTHONHASHSEED``. Rebuilding in this one would compare the process against
    itself and could not see set or dict iteration order reaching the bytes.
    """
    build(tmp_path / "a", archive=archive, release_point="119-102")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.build_usc_source_credit_artifact",
            "--output",
            str(tmp_path / "b"),
            "--archive",
            str(archive),
            "--release-point",
            "119-102",
        ],
        cwd=REPO_ROOT,
        env={**os.environ, "PYTHONHASHSEED": "1"},
        check=True,
        capture_output=True,
    )

    for name in ("usc-source-credits.parquet", "quarantine.parquet", "receipt.json"):
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes()


def test_the_receipt_pins_the_bytes_of_every_title_and_the_rules_applied(tmp_path: Path, archive: Path):
    receipt = build(tmp_path / "out", archive=archive, release_point="119-102")

    # Per-title digests, not only the archive's: the counts were read from these.
    assert [t["member"] for t in receipt["inputs"]["titles"]] == ["usc16.xml", "usc26.xml"]
    assert all(t["digest"].startswith("sha256:") for t in receipt["inputs"]["titles"])
    assert receipt["inputs"]["release_url"].endswith("xml_uscAll@119-102.zip")
    assert receipt["rules"]["strict_enactment_rule"] == "added-or-as-added-pub-law-division-act-section-v1"
    assert receipt["rules"]["section_dash_rule"] == "straighten-uslm-en-dash-to-hyphen-v1"
    assert receipt["rules"]["multi_target_policy"] == "retain-and-mark-refusal-multi_target-v1"


def test_a_secret_shaped_value_stops_the_seal(tmp_path: Path):
    """A credential must not reach a sealed file, whatever path carried it there."""
    leaky = _archive(
        tmp_path,
        "leaky.zip",
        {"usc26.xml": _title("/us/usc/t26", _section("/us/usc/t18a/api_key=deadbeef1234", _ADDED_107))},
    )

    with pytest.raises(SystemExit, match="refusing to seal"):
        build(tmp_path / "out", archive=leaky, release_point="119-102")
