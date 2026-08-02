"""The source-credit index seals what it read, and marks what it refuses.

The artifact is the second of the two independent sources the act-relative
resolver consults. It is built the way the act index and the agency crosswalk
are built -- deterministic rows, digest-pinned receipt, repo-relative paths,
secret-scanned, byte-identical rebuilds -- and it differs from them in one
deliberate way: a triple that names several U.S. Code sections is **kept and
marked refusing**, never dropped. A consumer must be able to see that the source
had more than one answer, which is a different fact from the source having none.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "build_usc_source_credit_artifact", REPO_ROOT / "tools" / "build_usc_source_credit_artifact.py"
)
mod = importlib.util.module_from_spec(_spec)
sys.modules["build_usc_source_credit_artifact"] = mod
_spec.loader.exec_module(mod)

USLM = "http://xml.house.gov/schemas/uslm/1.0"


def _title(identifier: str, body: str) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<uscDoc xmlns="{USLM}" identifier="{identifier}"><main>{body}</main></uscDoc>""".encode()


# 26 U.S.C. 6038E, added by (116-260, div. EE, sec. 107), and 26 U.S.C. 7652,
# which the same act section only AMENDED -- the measured false positive.
TITLE_26 = _title(
    "/us/usc/t26",
    """<section identifier="/us/usc/t26/s6038E"><num value="6038E">§ 6038E.</num>
<sourceCredit>(Added <ref href="/us/pl/116/260/dEE/tI/s107/d/1">Pub. L. 116–260, div. EE, title I, § 107(d)(1)</ref>, <date date="2020-12-27">Dec. 27, 2020</date>, <ref href="/us/stat/134/3048">134 Stat. 3048</ref>.)</sourceCredit>
</section>
<section identifier="/us/usc/t26/s7652"><num value="7652">§ 7652.</num>
<sourceCredit>(<date date="1954-08-16">Aug. 16, 1954</date>, ch. 736, 68A Stat. 907; <ref href="/us/pl/116/260/dEE/tI/s107/a/2">Pub. L. 116–260, div. EE, title I, § 107(a)(2)</ref>, <date date="2020-12-27">Dec. 27, 2020</date>, <ref href="/us/stat/134/3046">134 Stat. 3046</ref>.)</sourceCredit>
</section>""",
)

# 29 U.S.C. 1153, added by (117-328, div. T, sec. 303) -- the "as added" shape.
# Plus a triple that names two sections, which must be kept and marked.
TITLE_29 = _title(
    "/us/usc/t29",
    """<section identifier="/us/usc/t29/s1153"><num value="1153">§ 1153.</num>
<sourceCredit>(<ref href="/us/pl/93/406/tI/s523">Pub. L. 93–406, title I, § 523</ref>, as added <ref href="/us/pl/117/328/dT/tIII/s303/a">Pub. L. 117–328, div. T, title III, § 303(a)</ref>, <date date="2022-12-29">Dec. 29, 2022</date>, <ref href="/us/stat/136/5339">136 Stat. 5339</ref>.)</sourceCredit>
</section>
<section identifier="/us/usc/t29/s1201"><num value="1201">§ 1201.</num>
<sourceCredit>(Added <ref href="/us/pl/117/328/dT/tIV/s401">Pub. L. 117–328, div. T, title IV, § 401(a)</ref>, <ref href="/us/stat/136/5400">136 Stat. 5400</ref>.)</sourceCredit>
</section>
<section identifier="/us/usc/t29/s1202"><num value="1202">§ 1202.</num>
<sourceCredit>(Added <ref href="/us/pl/117/328/dT/tIV/s401">Pub. L. 117–328, div. T, title IV, § 401(b)</ref>, <ref href="/us/stat/136/5401">136 Stat. 5401</ref>.)</sourceCredit>
</section>""",
)

# An appendix title, whose section identifiers the U.S.C. shape cannot spell.
TITLE_18A = _title(
    "/us/usc/t18a",
    """<section identifier="/us/usc/t18a/pl/91/538/s1"><num value="1">§ 1.</num>
<sourceCredit>(Added <ref href="/us/pl/116/260/dQ/tI/s211">Pub. L. 116–260, div. Q, title I, § 211</ref>, <ref href="/us/stat/134/2100">134 Stat. 2100</ref>.)</sourceCredit>
</section>""",
)


@pytest.fixture
def archive(tmp_path) -> Path:
    path = tmp_path / "uscall.zip"
    with zipfile.ZipFile(path, "w") as bundle:
        for name, payload in (("usc26.xml", TITLE_26), ("usc29.xml", TITLE_29), ("usc18a.xml", TITLE_18A)):
            bundle.writestr(name, payload)
    return path


def _build(output_dir: Path, archive: Path):
    return mod.build(output_dir, archive=archive, release_point="119-102")


def test_the_artifact_seals_the_index_and_a_receipt(tmp_path, archive):
    receipt = _build(tmp_path / "out", archive)

    assert receipt["schema_version"] == "usc-source-credit-artifact-v1"
    assert receipt["parser_version"] == "uscode-uslm-parser-v1"
    rows = pq.read_table(tmp_path / "out" / "usc-source-credits.parquet").to_pylist()
    added = next(r for r in rows if r["usc_section"] == "6038E")
    assert (added["public_law"], added["division"], added["act_section"]) == ("116-260", "EE", "107")
    assert (added["statutes_at_large_volume"], added["statutes_at_large_page"]) == ("134", "3048")
    assert added["refusal"] is None
    assert added["usc_identifier"] == "/us/usc/t26/s6038E"


def test_the_amend_only_credit_is_not_in_the_index(tmp_path, archive):
    """26 U.S.C. 7652 stays out. This is the named false positive."""
    _build(tmp_path / "out", archive)

    rows = pq.read_table(tmp_path / "out" / "usc-source-credits.parquet").to_pylist()
    assert "7652" not in {r["usc_section"] for r in rows}


def test_a_multi_target_triple_is_kept_and_marked_refusing(tmp_path, archive):
    """Dropping it would say the source was silent. It was not; it was plural."""
    receipt = _build(tmp_path / "out", archive)

    rows = pq.read_table(tmp_path / "out" / "usc-source-credits.parquet").to_pylist()
    plural = [r for r in rows if (r["public_law"], r["division"], r["act_section"]) == ("117-328", "T", "401")]
    assert {r["usc_section"] for r in plural} == {"1201", "1202"}
    assert {r["refusal"] for r in plural} == {"multi_target"}
    assert {r["target_count"] for r in plural} == {"2"}
    assert receipt["coverage"]["multi_target_triples"] == 1
    assert receipt["coverage"]["unambiguous_triples"] == 2
    assert receipt["coverage"]["rows_refusing"] == 2


def test_an_unattributable_credit_is_quarantined_with_a_reason(tmp_path, archive):
    receipt = _build(tmp_path / "out", archive)

    quarantined = pq.read_table(tmp_path / "out" / "quarantine.parquet").to_pylist()
    assert [(r["reason"], r["raw_value"]) for r in quarantined] == [
        ("section_identifier_unparsable", "/us/usc/t18a/pl/91/538/s1")
    ]
    assert receipt["coverage"]["quarantine_reasons"] == {"section_identifier_unparsable": 1}


def test_every_output_is_pinned_by_digest_and_row_count(tmp_path, archive):
    receipt = _build(tmp_path / "out", archive)

    for pinned, entry in receipt["outputs"].items():
        assert entry["digest"].startswith("sha256:")
        assert entry["rows"] == pq.ParquetFile(tmp_path / "out" / Path(pinned).name).metadata.num_rows


def test_the_inputs_pin_the_archive_and_every_title_in_it(tmp_path, archive):
    """A zip digest says which bundle; the member digests say which bytes."""
    receipt = _build(tmp_path / "out", archive)

    inputs = receipt["inputs"]
    assert inputs["release_point"] == "119-102"
    assert inputs["release_url"].endswith("xml_uscAll@119-102.zip")
    assert inputs["archive_digest"].startswith("sha256:")
    assert [t["member"] for t in inputs["titles"]] == ["usc18a.xml", "usc26.xml", "usc29.xml"]
    assert all(t["digest"].startswith("sha256:") for t in inputs["titles"])


def test_a_rebuild_is_byte_identical(tmp_path, archive):
    """Determinism is the whole claim a digest makes."""
    first, second = tmp_path / "a", tmp_path / "b"
    _build(first, archive)
    _build(second, archive)

    for name in ("usc-source-credits.parquet", "quarantine.parquet", "receipt.json"):
        assert (first / name).read_bytes() == (second / name).read_bytes(), name


def test_the_receipt_records_repo_relative_paths(tmp_path, archive):
    """Absolute scratch paths would make two rebuilds differ for no reason."""
    receipt = _build(tmp_path / "out", archive)
    assert all(not Path(p).is_absolute() for p in receipt["outputs"])
    assert not Path(receipt["inputs"]["archive"]).is_absolute()


def test_the_strict_rule_is_pinned_with_its_derivation(tmp_path, archive):
    rules = _build(tmp_path / "out", archive)["rules"]

    assert rules["strict_enactment_rule"] == "added-or-as-added-pub-law-division-act-section-v1"
    derivation = rules["strict_enactment_rule_derivation"]
    assert "7652" in derivation, "the derivation names the false positive it removes"
    assert "amended" in derivation
    assert rules["section_dash_rule"] == "straighten-uslm-en-dash-to-hyphen-v1"
    assert "multi_target" in rules["multi_target_policy"]
    assert "plural" in rules["multi_target_policy_derivation"]


def test_a_secret_like_value_is_refused_rather_than_sealed(tmp_path):
    """Aimed at the one value that reaches a row verbatim: the USLM identifier.

    The artifact carries structured fields and no credit prose, so a secret in
    the credit text cannot reach the file at all -- the scan is over what is
    written, and this proves it fires on the surface that is.
    """
    leaked = TITLE_26.replace(b"/us/usc/t26/s6038E", b"/us/usc/t26/s6038E-sk-proj-AAAABBBBCCCCDDDDEEEEFFFF")
    path = tmp_path / "leaky.zip"
    with zipfile.ZipFile(path, "w") as bundle:
        bundle.writestr("usc26.xml", leaked)
    with pytest.raises(SystemExit, match="secret"):
        _build(tmp_path / "out", path)


def test_the_sealed_artifact_on_disk_matches_its_own_receipt():
    """The committed receipt describes the committed bytes, or it describes nothing."""
    artifact = REPO_ROOT / "output" / "usc-source-credit-index-2026-08-02"
    if not (artifact / "receipt.json").exists():
        pytest.skip("artifact not built in this checkout (output/ is gitignored)")
    receipt = json.loads((artifact / "receipt.json").read_text())
    for pinned, entry in receipt["outputs"].items():
        path = REPO_ROOT / pinned
        assert mod.file_sha256(path) == entry["digest"], pinned
        assert pq.ParquetFile(path).metadata.num_rows == entry["rows"], pinned
