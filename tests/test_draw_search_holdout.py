"""Hermetic checks for the content-blind SEARCH holdout draw.

Every fixture here is synthetic. Nothing reads the real ontology snapshot, so
these tests state what the tool guarantees rather than what one particular
draw happened to do. The unit under test is the *matter* — a docket family /
RIN family / cross-post cluster assembled from ``proceedings`` +
``agenda_item_proceedings`` + ``fr_docket_links`` identity keys only.

Protocol origin: ``tools/draw_holdout.py`` (content-blind seeded ``rank_key``
at :538-548, ``assert_blind`` at :854-892), re-keyed to search matters.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DRAW_PATH = REPO_ROOT / "tools" / "draw_search_holdout.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("draw_search_holdout", DRAW_PATH)
    assert spec and spec.loader, f"could not load {DRAW_PATH}"
    module = importlib.util.module_from_spec(spec)
    # Registered before execution: ``dataclasses`` resolves a class's module
    # through ``sys.modules`` while it processes the class body.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


mod = _load_module()

SearchHoldoutBlindnessError = mod.SearchHoldoutBlindnessError
SearchHoldoutPartitionError = mod.SearchHoldoutPartitionError


# --------------------------------------------------------------------------
# synthetic dataset helpers
# --------------------------------------------------------------------------


def proceeding_row(pid, *, rin=None, dockets=(), frdocs=(), title="a proceeding title"):
    return {
        "proceeding_id": pid,
        "rin": rin,
        "docket_ids_json": json.dumps(list(dockets)),
        "fr_document_numbers_json": json.dumps(list(frdocs)),
        "title": title,
    }


def agenda_row(rin, pid, *, evidence_date=None):
    return {"rin": rin, "proceeding_id": pid, "evidence_date": evidence_date}


def fr_row(frdoc, docket, *, rins=(), publication_date=None, title="an fr title", abstract="an fr abstract"):
    return {
        "docket_id": docket,
        "document_number": frdoc,
        "regulation_id_numbers_json": json.dumps(list(rins)),
        "publication_date": publication_date,
        "title": title,
        "abstract": abstract,
    }


PROCEEDING_FIELDS = ["proceeding_id", "rin", "docket_ids_json", "fr_document_numbers_json", "title"]
AGENDA_FIELDS = ["rin", "proceeding_id", "evidence_date"]
FR_FIELDS = ["docket_id", "document_number", "regulation_id_numbers_json", "publication_date", "title", "abstract"]


def write_dataset(directory: Path, proceedings=(), agenda=(), fr=()):
    directory.mkdir(parents=True, exist_ok=True)
    for name, fields, rows in (
        ("proceedings", PROCEEDING_FIELDS, proceedings),
        ("agenda_item_proceedings", AGENDA_FIELDS, agenda),
        ("fr_docket_links", FR_FIELDS, fr),
    ):
        columns = {field: [row.get(field) for row in rows] for field in fields}
        table = pa.table({field: pa.array(values, type=pa.string()) for field, values in columns.items()})
        pq.write_table(table, directory / f"{name}.parquet")
    return directory


def many_distinct_matters(count: int, *, era="2021-03-01"):
    """``count`` disjoint one-proceeding matters, each with its own docket + FR doc."""
    proceedings, fr = [], []
    for index in range(count):
        proceedings.append(proceeding_row(f"P{index:04d}", dockets=[f"D{index:04d}"], title=f"synthetic title {index}"))
        fr.append(
            fr_row(
                f"F{index:04d}",
                f"D{index:04d}",
                publication_date=era,
                title=f"synthetic fr title {index}",
                abstract=f"synthetic fr abstract {index}",
            )
        )
    return proceedings, fr


# --------------------------------------------------------------------------
# matter assembly: the unit is the whole matter
# --------------------------------------------------------------------------


def test_matter_assembly_merges_shared_identities():
    proceedings = [
        proceeding_row("P1", rin="R1", dockets=["D1"], frdocs=["F9"]),
        proceeding_row("P2", dockets=["D2"]),
    ]
    agenda = [agenda_row("R1", "P1", evidence_date="2020-05-01")]
    fr = [fr_row("F1", "D1", publication_date="2021-01-15")]

    matters = mod.assemble_matters(proceedings, agenda, fr)
    assert len(matters) == 2
    by_size = sorted(matters, key=lambda matter: matter.node_count)
    small, big = by_size
    assert small.proceedings == ("P2",) and small.dockets == ("D2",)
    assert big.proceedings == ("P1",)
    assert big.dockets == ("D1",)
    assert set(big.fr_documents) == {"F1", "F9"}
    assert big.rins == ("R1",)
    assert big.node_count == 5


def test_cross_post_cluster_is_one_matter():
    # One FR document posted to two dockets pulls both proceedings together.
    proceedings = [
        proceeding_row("P1", dockets=["D1"]),
        proceeding_row("P2", dockets=["D2"]),
    ]
    fr = [fr_row("F1", "D1"), fr_row("F1", "D2")]
    matters = mod.assemble_matters(proceedings, [], fr)
    assert len(matters) == 1
    assert set(matters[0].proceedings) == {"P1", "P2"}


def test_isolated_fr_document_is_its_own_matter():
    fr = [fr_row("F1", None)]
    matters = mod.assemble_matters([], [], fr)
    assert len(matters) == 1
    assert matters[0].fr_documents == ("F1",)
    assert matters[0].node_count == 1


def test_every_identity_key_lands_in_exactly_one_matter():
    proceedings, fr = many_distinct_matters(10)
    matters = mod.assemble_matters(proceedings, [], fr)
    facts = mod.verify_partition(matters)
    assert facts["passed"] is True
    assert facts["duplicated_identity_keys"] == []
    # Doctored overlap must refuse.
    with pytest.raises(SearchHoldoutPartitionError):
        mod.verify_partition(list(matters) + [matters[0]])


# --------------------------------------------------------------------------
# the content-blind seeded order
# --------------------------------------------------------------------------


def test_matter_key_is_identity_only():
    rows_a = [proceeding_row("P1", dockets=["D1"], title="title one")]
    rows_b = [proceeding_row("P1", dockets=["D1"], title="a completely different title")]
    key_a = mod.matter_key(mod.assemble_matters(rows_a, [], [])[0])
    key_b = mod.matter_key(mod.assemble_matters(rows_b, [], [])[0])
    assert key_a == key_b
    assert "title one" not in key_a


def test_rank_key_is_seeded_and_deterministic():
    key = mod.matter_key(mod.assemble_matters([proceeding_row("P1", dockets=["D1"])], [], [])[0])
    first = mod.rank_key(key, seed="seed-a", procedure="proc-v1")
    again = mod.rank_key(key, seed="seed-a", procedure="proc-v1")
    other_seed = mod.rank_key(key, seed="seed-b", procedure="proc-v1")
    other_procedure = mod.rank_key(key, seed="seed-a", procedure="proc-v2")
    assert first == again
    assert first != other_seed
    assert first != other_procedure


# --------------------------------------------------------------------------
# strata: source class, matter size, date era
# --------------------------------------------------------------------------


def test_source_class_classification():
    proceedings = [
        proceeding_row("P1", rin="R1", dockets=["D1"]),
        proceeding_row("P2", dockets=["D2"]),
    ]
    agenda = [agenda_row("R1", "P1")]
    fr = [fr_row("F1", "D1"), fr_row("F2", None)]
    matters = {matter.source_class for matter in mod.assemble_matters(proceedings, agenda, fr)}
    assert matters == {"proc+agenda+fr", "proc-only", "fr-only"}


def test_size_and_era_buckets():
    assert mod.size_bucket(1) == "single"
    assert mod.size_bucket(4) == "small"
    assert mod.size_bucket(16) == "medium"
    assert mod.size_bucket(64) == "large"
    assert mod.size_bucket(65) is None  # oversize: ineligible for the holdout
    assert mod.era_bucket(None) == "undated"
    assert mod.era_bucket("2009-12-31") == "pre-2010"
    assert mod.era_bucket("2010-01-01") == "2010-2017"
    assert mod.era_bucket("2022-06-30") == "2018-2022"
    assert mod.era_bucket("2026-01-01") == "2023-plus"


def test_matter_era_uses_latest_evidence():
    proceedings = [proceeding_row("P1", rin="R1", dockets=["D1"])]
    agenda = [agenda_row("R1", "P1", evidence_date="2011-04-01")]
    fr = [fr_row("F1", "D1", publication_date="2024-02-02")]
    (matter,) = mod.assemble_matters(proceedings, agenda, fr)
    assert matter.latest_evidence == "2024-02-02"
    assert mod.era_bucket(matter.latest_evidence) == "2023-plus"


# --------------------------------------------------------------------------
# quota allocation: declared, deterministic, clamped
# --------------------------------------------------------------------------


def test_quota_allocation_is_deterministic_and_clamped():
    census = {("a", "small", "undated"): 100, ("b", "small", "undated"): 100, ("c", "small", "undated"): 3}
    quotas = mod.allocate_quotas(census, target_total=10, min_census=10, min_quota=2, max_quota=8)
    assert quotas == mod.allocate_quotas(census, target_total=10, min_census=10, min_quota=2, max_quota=8)
    assert ("c", "small", "undated") not in quotas  # below the census floor
    assert sum(quotas.values()) == 10
    assert all(2 <= quota <= 8 for quota in quotas.values())


def test_quota_allocation_never_exceeds_census():
    census = {("a", "small", "undated"): 4}
    quotas = mod.allocate_quotas(census, target_total=10, min_census=1, min_quota=2, max_quota=40)
    assert quotas[("a", "small", "undated")] <= 4


# --------------------------------------------------------------------------
# the draw: reproducible, seed-sensitive, whole matters in one split
# --------------------------------------------------------------------------


def _draw(matters, *, seed, target_total=6):
    return mod.draw_search_holdout(
        matters,
        seed=seed,
        procedure="test-procedure-v1",
        target_total=target_total,
        min_census=1,
        min_quota=1,
        max_quota=50,
    )


def test_draw_is_reproducible_and_seed_sensitive():
    proceedings, fr = many_distinct_matters(60)
    matters = mod.assemble_matters(proceedings, [], fr)
    first = _draw(matters, seed="seed-a")
    again = _draw(matters, seed="seed-a")
    other = _draw(matters, seed="seed-b")
    assert [matter.matter_id for matter in first.drawn] == [matter.matter_id for matter in again.drawn]
    assert [matter.matter_id for matter in first.drawn] != [matter.matter_id for matter in other.drawn]
    assert len(first.drawn) == 6


def test_draw_splits_are_disjoint_and_exhaustive():
    proceedings, fr = many_distinct_matters(30)
    matters = mod.assemble_matters(proceedings, [], fr)
    draw = _draw(matters, seed="seed-a")
    drawn_ids = {matter.matter_id for matter in draw.drawn}
    development_ids = {matter.matter_id for matter in draw.development}
    assert drawn_ids.isdisjoint(development_ids)
    assert drawn_ids | development_ids == {matter.matter_id for matter in matters}
    facts = draw.split_facts()
    assert facts["holdout"] == len(drawn_ids)
    assert facts["development"] == len(development_ids)
    assert facts["holdout"] + facts["development"] == len(matters)


def test_oversize_matters_stay_in_development():
    # One matter with far more than max_nodes identity keys.
    dockets = [f"D{index}" for index in range(80)]
    proceedings = [proceeding_row("P-big", dockets=dockets)]
    small_proceedings, fr = many_distinct_matters(5)
    matters = mod.assemble_matters(proceedings + small_proceedings, [], fr)
    draw = _draw(matters, seed="seed-a", target_total=6)
    drawn_ids = {matter.matter_id for matter in draw.drawn}
    oversize = [matter for matter in matters if matter.node_count > mod.MAX_MATTER_NODES]
    assert len(oversize) == 1
    assert oversize[0].matter_id not in drawn_ids
    assert draw.oversize_count == 1


# --------------------------------------------------------------------------
# blindness: banned keys and leaked scalars, run twice, recorded
# --------------------------------------------------------------------------


def test_assert_blind_flags_banned_keys():
    document = {"matters": [{"matter_id": "m1", "title": "leak"}]}
    with pytest.raises(SearchHoldoutBlindnessError):
        mod.assert_blind(document, forbidden_value_digests=frozenset())
    clean = {"matters": [{"matter_id": "m1", "dockets": ["D1"]}]}
    facts = mod.assert_blind(clean, forbidden_value_digests=frozenset())
    assert facts["passed"] is True
    assert facts["banned_key_paths"] == []
    assert facts["leaked_value_paths"] == []
    assert facts["string_values_checked"] >= 2


def test_assert_blind_flags_leaked_content_scalar():
    leaked = "An Actual Federal Register Title"
    forbidden = frozenset({mod.content_digest(leaked)})
    document = {"matters": [{"matter_id": leaked}]}
    with pytest.raises(SearchHoldoutBlindnessError):
        mod.assert_blind(document, forbidden_value_digests=forbidden)
    clean = {"matters": [{"matter_id": "m1"}]}
    assert mod.assert_blind(clean, forbidden_value_digests=forbidden)["passed"] is True


def test_forbidden_content_digests_cover_titles_and_abstracts(tmp_path):
    dataset = write_dataset(
        tmp_path / "dataset",
        proceedings=[proceeding_row("P1", dockets=["D1"], title="proceeding title leak")],
        fr=[fr_row("F1", "D1", title="fr title leak", abstract="fr abstract leak")],
    )
    digests = mod.load_forbidden_content_digests(dataset)
    for value in ("proceeding title leak", "fr title leak", "fr abstract leak"):
        assert mod.content_digest(value) in digests


# --------------------------------------------------------------------------
# end to end: sealed manifest + receipt from a synthetic dataset directory
# --------------------------------------------------------------------------


def _run_end_to_end(tmp_path, *, seed="test-seed"):
    proceedings, fr = many_distinct_matters(40)
    dataset = write_dataset(tmp_path / "dataset", proceedings=proceedings, fr=fr)
    output = tmp_path / "out"
    return mod.run_draw(
        dataset_dir=dataset,
        output_dir=output,
        seed=seed,
        procedure="test-procedure-v1",
        dataset_id="test-holdout-v1",
        target_total=8,
        min_census=1,
        min_quota=1,
        max_quota=50,
        drawn_at="2026-08-01T00:00:00Z",
    )


def test_end_to_end_writes_blind_sealed_manifest_and_receipt(tmp_path):
    result = _run_end_to_end(tmp_path)
    manifest = json.loads(Path(result.manifest_path).read_text())
    receipt = json.loads(Path(result.receipt_path).read_text())

    assert manifest["holdout"]["selection_seed"] == "test-seed"
    assert manifest["holdout"]["selection_procedure"] == "test-procedure-v1"
    assert len(manifest["matters"]) == 8
    for row in manifest["matters"]:
        assert set(row) == {
            "matter_id",
            "source_class",
            "size_bucket",
            "era_bucket",
            "node_count",
            "proceedings",
            "dockets",
            "fr_documents",
            "rins",
        }

    # Blindness ran twice and both runs are recorded in the receipt.
    for run in ("blindness_first_run", "blindness_second_run"):
        assert receipt[run]["passed"] is True
    assert receipt["blindness_runs_match"] is True

    # The receipt pins a digest for every input consumed.
    inputs = {entry["table"]: entry for entry in receipt["inputs"]}
    for table in ("proceedings", "agenda_item_proceedings", "fr_docket_links"):
        recorded = inputs[table]["sha256"]
        actual = hashlib.sha256((tmp_path / "dataset" / f"{table}.parquet").read_bytes()).hexdigest()
        assert recorded == actual

    # The sealed manifest digest in the receipt matches the file bytes.
    sealed = hashlib.sha256(Path(result.manifest_path).read_bytes()).hexdigest()
    assert receipt["sealed_manifest_sha256"] == sealed

    # Sealing rules travel with the receipt: frozen config, one-shot opening,
    # two independent judge families. No labels exist.
    rules = receipt["sealing"]
    assert rules["labels"] == "none"
    for rule in ("configuration_freeze", "one_shot_opening", "judge_families"):
        assert isinstance(rules[rule], str) and rules[rule]


def test_end_to_end_manifest_carries_no_content(tmp_path):
    result = _run_end_to_end(tmp_path)
    manifest_text = Path(result.manifest_path).read_text()
    assert "synthetic title" not in manifest_text
    assert "synthetic fr title" not in manifest_text
    assert "synthetic fr abstract" not in manifest_text


def test_end_to_end_is_reproducible(tmp_path):
    first = _run_end_to_end(tmp_path / "one")
    second = _run_end_to_end(tmp_path / "two")
    manifest_one = json.loads(Path(first.manifest_path).read_text())
    manifest_two = json.loads(Path(second.manifest_path).read_text())
    assert manifest_one == manifest_two


def test_draw_path_declares_no_content_columns():
    content = {"title", "abstract", "summary", "text", "body"}
    for columns in (mod.PROCEEDINGS_DRAW_COLUMNS, mod.AGENDA_DRAW_COLUMNS, mod.FR_DRAW_COLUMNS):
        assert content.isdisjoint(set(columns))
