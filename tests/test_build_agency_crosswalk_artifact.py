"""Hermetic checks for the agency-crosswalk artifact builder.

Every fixture here is synthetic. Nothing reads the real ontology outputs, so
these tests state what the tool guarantees rather than what one particular
build happened to contain.

The builder answers two questions the CFR-part-priors experiment needs:

1. Which Federal Register agency **slug** does a regulations.gov agency
   **code** correspond to, and how much do we trust that? Evidence is joined
   through the agency *field* on ``dockets`` (and on ``documents``), never
   through docket-id string prefixes.
2. Which agencies cite a given (CFR title, CFR part)?

Codes with weak or contested evidence are tiered and kept, never dropped;
malformed source rows land in a typed quarantine partition with
machine-readable reasons.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = REPO_ROOT / "tools" / "build_agency_crosswalk_artifact.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("build_agency_crosswalk_artifact", TOOL_PATH)
    assert spec and spec.loader, f"could not load {TOOL_PATH}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


mod = _load_module()


# --------------------------------------------------------------------------
# synthetic dataset helpers
# --------------------------------------------------------------------------


FEDERAL_REGISTER_FIELDS = [
    "document_number",
    "agencies_json",
    "cfr_references_json",
]

DOCKET_FIELDS = ["docket_id", "agency_code"]

FR_LINK_FIELDS = ["docket_id", "document_number"]

DOCUMENT_FIELDS = ["docket_id", "agency_code", "fr_doc_num"]


def agency(slug, agency_id, parent_id=None):
    """One entry as it appears inside ``federal_register.agencies_json``."""

    return {
        "raw_name": slug.replace("-", " ").upper(),
        "name": slug.replace("-", " ").title(),
        "id": agency_id,
        "parent_id": parent_id,
        "slug": slug,
    }


#: The two-level lineage used across these fixtures: a parent department and
#: two child sub-agencies, plus one unrelated independent agency.
TRANSPORT = agency("transportation-department", 497)
AVIATION = agency("federal-aviation-administration", 162, 497)
HIGHWAY = agency("national-highway-traffic-safety-administration", 375, 497)
NUCLEAR = agency("nuclear-regulatory-commission", 383)


def fr_row(document_number, *, agencies=(), cfr_refs=None, raw_agencies=None):
    if raw_agencies is not None:
        agencies_json = raw_agencies
    else:
        agencies_json = json.dumps(list(agencies))
    return {
        "document_number": document_number,
        "agencies_json": agencies_json,
        "cfr_references_json": json.dumps(cfr_refs) if cfr_refs is not None else "[]",
    }


def cfr_ref(title, part, chapter=None):
    return {"chapter": chapter, "citation_url": None, "part": part, "title": title}


def docket_row(docket_id, agency_code):
    return {"docket_id": docket_id, "agency_code": agency_code}


def link_row(docket_id, document_number):
    return {"docket_id": docket_id, "document_number": document_number}


def document_row(document_id_docket, agency_code, fr_doc_num=None):
    return {
        "docket_id": document_id_docket,
        "agency_code": agency_code,
        "fr_doc_num": fr_doc_num,
    }


def write_inputs(tmp_path, *, federal_register=(), dockets=(), fr_links=(), documents=()):
    paths = {}
    for name, fields, rows in (
        ("federal_register", FEDERAL_REGISTER_FIELDS, federal_register),
        ("dockets", DOCKET_FIELDS, dockets),
        ("fr_docket_links", FR_LINK_FIELDS, fr_links),
        ("documents", DOCUMENT_FIELDS, documents),
    ):
        schema = pa.schema([(field, pa.string()) for field in fields])
        arrays = [pa.array([row.get(field) for row in rows], type=pa.string()) for field in fields]
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.Table.from_arrays(arrays, schema=schema), path)
        paths[name] = path
    return paths


def build(tmp_path, out_name="artifact", **inputs):
    paths = write_inputs(tmp_path, **inputs)
    output_dir = tmp_path / out_name
    receipt = mod.build_artifact(
        federal_register=paths["federal_register"],
        dockets=paths["dockets"],
        fr_docket_links=paths["fr_docket_links"],
        documents=paths["documents"],
        output_dir=output_dir,
    )
    return output_dir, receipt


def read_rows(path):
    return pq.read_table(path).to_pylist()


def codes_by_name(output_dir):
    return {row["agency_code"]: row for row in read_rows(output_dir / "agency-codes.parquet")}


def crosswalk_for(output_dir, code):
    rows = [
        row
        for row in read_rows(output_dir / "agency-crosswalk.parquet")
        if row["agency_code"] == code
    ]
    return sorted(rows, key=lambda row: int(row["rank"]))


def _aviation_corpus(document_count=8, code="FAA"):
    """``document_count`` documents that every source agrees are FAA rules."""

    federal_register = [
        fr_row(f"2026-{index:05d}", agencies=[TRANSPORT, AVIATION])
        for index in range(1, document_count + 1)
    ]
    dockets = [docket_row(f"FAA-2026-{index:04d}", code) for index in range(1, document_count + 1)]
    fr_links = [
        link_row(f"FAA-2026-{index:04d}", f"2026-{index:05d}")
        for index in range(1, document_count + 1)
    ]
    return {"federal_register": federal_register, "dockets": dockets, "fr_links": fr_links}


# --------------------------------------------------------------------------
# the code -> slug join
# --------------------------------------------------------------------------


def test_a_code_whose_documents_agree_is_confident_and_picks_the_specific_slug(tmp_path):
    """FAA documents always carry both DOT and FAA; the crosswalk must pick FAA.

    Both slugs have share 1.0, so share alone cannot decide. The pinned
    specificity rule breaks the tie toward the deeper (child) slug — mapping
    the code to the sub-agency rather than the parent department.
    """

    output_dir, receipt = build(tmp_path, **_aviation_corpus())
    code = codes_by_name(output_dir)["FAA"]
    assert code["tier"] == "confident"
    assert code["primary_slug"] == "federal-aviation-administration"
    assert code["confidence_share"] == "1.000000"
    assert code["support_documents"] == "8"
    assert code["candidate_count"] == "2"

    candidates = crosswalk_for(output_dir, "FAA")
    assert [row["agency_slug"] for row in candidates] == [
        "federal-aviation-administration",
        "transportation-department",
    ]
    assert [row["is_primary"] for row in candidates] == ["true", "false"]
    assert candidates[0]["parent_slug"] == "transportation-department"
    assert candidates[0]["depth"] == "1"
    assert candidates[1]["depth"] == "0"
    assert all(row["share"] == "1.000000" for row in candidates)
    assert receipt["counts"]["tier_histogram"]["confident"] == 1


def test_a_near_tie_within_the_margin_still_resolves_to_the_specific_slug(tmp_path):
    """The parent out-polls the child slightly; specificity must still win.

    This is the real NHTSA shape: every document names the department, and
    all but one also name the sub-agency. Ranking on share alone would map
    the code to ``transportation-department``; the pinned SPECIFICITY_MARGIN
    treats the two as tied on evidence and prefers the deeper slug.
    """

    federal_register = [
        fr_row(f"2026-{index:05d}", agencies=[TRANSPORT, HIGHWAY]) for index in range(1, 20)
    ]
    federal_register.append(fr_row("2026-00020", agencies=[TRANSPORT]))
    dockets = [docket_row(f"NHTSA-2026-{index:04d}", "NHTSA") for index in range(1, 21)]
    fr_links = [
        link_row(f"NHTSA-2026-{index:04d}", f"2026-{index:05d}") for index in range(1, 21)
    ]

    output_dir, _ = build(
        tmp_path, federal_register=federal_register, dockets=dockets, fr_links=fr_links
    )
    candidates = crosswalk_for(output_dir, "NHTSA")
    assert [row["share"] for row in candidates] == ["0.950000", "1.000000"], (
        "the child is strictly behind the parent on share"
    )
    assert candidates[0]["agency_slug"] == "national-highway-traffic-safety-administration"
    assert candidates[0]["is_primary"] == "true"

    code = codes_by_name(output_dir)["NHTSA"]
    assert code["primary_slug"] == "national-highway-traffic-safety-administration"
    assert code["confidence_share"] == "0.950000"
    assert code["tier"] == "confident"


def test_a_gap_wider_than_the_margin_is_not_a_tie(tmp_path):
    """Specificity only breaks near-ties; a genuinely weak child stays second.

    A department code whose documents mention one sub-agency occasionally
    must map to the department, not to that sub-agency.
    """

    federal_register = [
        fr_row(f"2026-{index:05d}", agencies=[TRANSPORT]) for index in range(1, 19)
    ]
    federal_register += [
        fr_row(f"2026-{index:05d}", agencies=[TRANSPORT, HIGHWAY]) for index in range(19, 21)
    ]
    dockets = [docket_row(f"DOT-2026-{index:04d}", "DOT") for index in range(1, 21)]
    fr_links = [link_row(f"DOT-2026-{index:04d}", f"2026-{index:05d}") for index in range(1, 21)]

    output_dir, _ = build(
        tmp_path, federal_register=federal_register, dockets=dockets, fr_links=fr_links
    )
    candidates = crosswalk_for(output_dir, "DOT")
    assert candidates[0]["agency_slug"] == "transportation-department"
    assert candidates[0]["share"] == "1.000000"
    assert candidates[1]["agency_slug"] == "national-highway-traffic-safety-administration"
    assert candidates[1]["share"] == "0.100000"
    assert codes_by_name(output_dir)["DOT"]["primary_slug"] == "transportation-department"


def test_a_code_split_across_unrelated_agencies_is_tiered_down_not_dropped(tmp_path):
    """Contested evidence must survive in the artifact, marked, never silently dropped."""

    federal_register = [fr_row(f"2026-{index:05d}", agencies=[NUCLEAR]) for index in range(1, 4)]
    federal_register += [fr_row(f"2026-{index:05d}", agencies=[TRANSPORT]) for index in range(4, 7)]
    dockets = [docket_row(f"MIX-2026-{index:04d}", "MIX") for index in range(1, 7)]
    fr_links = [link_row(f"MIX-2026-{index:04d}", f"2026-{index:05d}") for index in range(1, 7)]

    output_dir, receipt = build(
        tmp_path, federal_register=federal_register, dockets=dockets, fr_links=fr_links
    )
    code = codes_by_name(output_dir)["MIX"]
    assert code["tier"] == "ambiguous", "a 50/50 split is below the probable threshold"
    assert code["confidence_share"] == "0.500000"
    assert code["support_documents"] == "6"
    assert code["candidate_count"] == "2"

    candidates = crosswalk_for(output_dir, "MIX")
    assert len(candidates) == 2, "both contested slugs stay in the artifact"
    assert {row["agency_slug"] for row in candidates} == {
        "nuclear-regulatory-commission",
        "transportation-department",
    }
    assert receipt["counts"]["tier_histogram"]["ambiguous"] == 1


def test_thin_evidence_caps_the_tier_even_at_a_perfect_share(tmp_path):
    """One supporting document is a perfect share but not confident evidence."""

    output_dir, _ = build(
        tmp_path,
        federal_register=[fr_row("2026-00001", agencies=[NUCLEAR])],
        dockets=[docket_row("NRC-2026-0001", "NRC")],
        fr_links=[link_row("NRC-2026-0001", "2026-00001")],
    )
    code = codes_by_name(output_dir)["NRC"]
    assert code["confidence_share"] == "1.000000"
    assert code["support_documents"] == "1"
    assert code["tier"] == "ambiguous", "below MIN_PROBABLE_DOCUMENTS"

    output_dir_b, _ = build(
        tmp_path,
        out_name="artifact-b",
        federal_register=[
            fr_row("2026-00001", agencies=[NUCLEAR]),
            fr_row("2026-00002", agencies=[NUCLEAR]),
        ],
        dockets=[docket_row("NRC-2026-0001", "NRC"), docket_row("NRC-2026-0002", "NRC")],
        fr_links=[
            link_row("NRC-2026-0001", "2026-00001"),
            link_row("NRC-2026-0002", "2026-00002"),
        ],
    )
    code_b = codes_by_name(output_dir_b)["NRC"]
    assert code_b["support_documents"] == "2"
    assert code_b["tier"] == "probable", "meets MIN_PROBABLE_DOCUMENTS, below MIN_CONFIDENT_DOCUMENTS"


def test_a_code_with_no_federal_register_evidence_is_kept_as_unmapped(tmp_path):
    """A code the join never reaches is still a row — downstream decides."""

    output_dir, receipt = build(
        tmp_path,
        federal_register=[fr_row("2026-00001", agencies=[NUCLEAR])],
        dockets=[docket_row("ORPHAN-2026-0001", "ORPHAN")],
        fr_links=[],
    )
    code = codes_by_name(output_dir)["ORPHAN"]
    assert code["tier"] == "unmapped"
    assert code["support_documents"] == "0"
    assert code["primary_slug"] is None
    assert code["in_dockets_table"] == "true"
    assert crosswalk_for(output_dir, "ORPHAN") == []
    assert receipt["counts"]["tier_histogram"]["unmapped"] == 1


def test_the_join_uses_the_docket_agency_field_not_the_docket_id_prefix(tmp_path):
    """A docket whose id prefix disagrees with its agency_code follows the field."""

    output_dir, _ = build(
        tmp_path,
        federal_register=[
            fr_row(f"2026-{index:05d}", agencies=[NUCLEAR]) for index in range(1, 6)
        ],
        # Every docket id starts with "EPA-HQ" but the agency field says NRC.
        dockets=[docket_row(f"EPA-HQ-2026-{index:04d}", "NRC") for index in range(1, 6)],
        fr_links=[
            link_row(f"EPA-HQ-2026-{index:04d}", f"2026-{index:05d}") for index in range(1, 6)
        ],
    )
    codes = codes_by_name(output_dir)
    assert set(codes) == {"NRC"}, "no EPA code is invented from the docket-id prefix"
    assert codes["NRC"]["primary_slug"] == "nuclear-regulatory-commission"
    assert codes["NRC"]["tier"] == "confident"


def test_evidence_paths_are_counted_separately_and_documents_are_deduplicated(tmp_path):
    """Both agency-field joins contribute; a document seen twice counts once."""

    output_dir, receipt = build(
        tmp_path,
        federal_register=[
            fr_row("2026-00001", agencies=[NUCLEAR]),
            fr_row("2026-00002", agencies=[NUCLEAR]),
        ],
        dockets=[docket_row("NRC-2026-0001", "NRC")],
        fr_links=[link_row("NRC-2026-0001", "2026-00001")],
        documents=[
            # same document as the link path -> deduplicated
            document_row("NRC-2026-0001", "NRC", "2026-00001"),
            # a document the link path never reaches
            document_row("NRC-2026-0009", "NRC", "2026-00002"),
        ],
    )
    code = codes_by_name(output_dir)["NRC"]
    assert code["support_documents"] == "2", "the shared document is counted once"
    assert json.loads(code["support_by_path_json"]) == {
        "dockets_fr_links": 1,
        "documents_fr_doc_num": 2,
    }
    assert code["in_dockets_table"] == "true"
    assert code["in_documents_table"] == "true"
    paths = receipt["counts"]["support_documents_by_path"]
    assert paths["dockets_fr_links"] == 1
    assert paths["documents_fr_doc_num"] == 2


def test_a_code_seen_only_in_the_documents_table_is_flagged(tmp_path):
    output_dir, _ = build(
        tmp_path,
        federal_register=[fr_row("2026-00001", agencies=[NUCLEAR])],
        dockets=[],
        documents=[document_row("X-2026-0001", "XONLY", "2026-00001")],
    )
    code = codes_by_name(output_dir)["XONLY"]
    assert code["in_dockets_table"] == "false"
    assert code["in_documents_table"] == "true"
    assert code["support_documents"] == "1"


# --------------------------------------------------------------------------
# parent rollup
# --------------------------------------------------------------------------


def test_parent_department_mapping_is_its_own_table(tmp_path):
    output_dir, receipt = build(
        tmp_path,
        federal_register=[
            fr_row("2026-00001", agencies=[TRANSPORT, AVIATION]),
            fr_row("2026-00002", agencies=[TRANSPORT, HIGHWAY]),
            fr_row("2026-00003", agencies=[NUCLEAR]),
        ],
    )
    parents = {row["agency_slug"]: row for row in read_rows(output_dir / "agency-parents.parquet")}
    assert set(parents) == {
        "transportation-department",
        "federal-aviation-administration",
        "national-highway-traffic-safety-administration",
        "nuclear-regulatory-commission",
    }
    aviation = parents["federal-aviation-administration"]
    assert aviation["agency_id"] == "162"
    assert aviation["parent_id"] == "497"
    assert aviation["parent_slug"] == "transportation-department"
    assert aviation["depth"] == "1"
    assert aviation["documents"] == "1"

    transport = parents["transportation-department"]
    assert transport["parent_id"] is None
    assert transport["parent_slug"] is None
    assert transport["depth"] == "0"
    assert transport["documents"] == "2"

    assert parents["nuclear-regulatory-commission"]["depth"] == "0"
    assert receipt["counts"]["agency_slugs_total"] == 4
    assert receipt["counts"]["agency_slugs_with_parent"] == 2


def test_an_unresolvable_parent_id_stops_the_depth_walk(tmp_path):
    """A parent_id with no slug in the pin is recorded, and depth stops there."""

    orphaned_child = agency("mystery-bureau", 900, 999)
    output_dir, _ = build(
        tmp_path, federal_register=[fr_row("2026-00001", agencies=[orphaned_child])]
    )
    parents = {row["agency_slug"]: row for row in read_rows(output_dir / "agency-parents.parquet")}
    row = parents["mystery-bureau"]
    assert row["parent_id"] == "999"
    assert row["parent_slug"] is None, "the parent id resolves to no slug in this pin"
    assert row["depth"] == "0"


# --------------------------------------------------------------------------
# CFR part -> agency association
# --------------------------------------------------------------------------


def test_cfr_parts_are_associated_with_the_agencies_citing_them(tmp_path):
    output_dir, receipt = build(
        tmp_path,
        federal_register=[
            fr_row("2026-00001", agencies=[TRANSPORT, AVIATION], cfr_refs=[cfr_ref(14, "39")]),
            fr_row("2026-00002", agencies=[TRANSPORT, AVIATION], cfr_refs=[cfr_ref(14, "39")]),
            fr_row("2026-00003", agencies=[NUCLEAR], cfr_refs=[cfr_ref(14, "39"), cfr_ref(10, "72")]),
        ],
    )
    rows = read_rows(output_dir / "cfr-part-agencies.parquet")
    part_39 = sorted(
        (row for row in rows if row["cfr_part"] == "39"), key=lambda row: int(row["rank"])
    )
    assert [row["agency_slug"] for row in part_39] == [
        "federal-aviation-administration",
        "transportation-department",
        "nuclear-regulatory-commission",
    ]
    assert all(row["cfr_title"] == "14" for row in part_39)
    assert [row["documents"] for row in part_39] == ["2", "2", "1"]
    assert all(row["part_documents"] == "3" for row in part_39)
    assert part_39[0]["share"] == "0.666667"
    assert part_39[2]["share"] == "0.333333"
    assert [row["is_primary"] for row in part_39] == ["true", "false", "false"]

    part_72 = [row for row in rows if row["cfr_part"] == "72"]
    assert len(part_72) == 1
    assert part_72[0]["cfr_title"] == "10"
    assert part_72[0]["agency_slug"] == "nuclear-regulatory-commission"

    counts = receipt["counts"]
    assert counts["cfr_title_part_pairs"] == 2
    assert counts["cfr_part_agency_rows"] == 4
    assert counts["documents_with_cfr_references"] == 3


def test_a_cfr_reference_without_a_part_is_quarantined_not_counted(tmp_path):
    output_dir, receipt = build(
        tmp_path,
        federal_register=[
            fr_row("2026-00001", agencies=[NUCLEAR], cfr_refs=[cfr_ref(10, None)]),
            fr_row("2026-00002", agencies=[NUCLEAR], cfr_refs=[cfr_ref(None, "72")]),
            fr_row("2026-00003", agencies=[NUCLEAR], cfr_refs=[cfr_ref(10, "72")]),
        ],
    )
    rows = read_rows(output_dir / "cfr-part-agencies.parquet")
    assert len(rows) == 1 and rows[0]["cfr_part"] == "72"

    quarantine = read_rows(output_dir / "quarantine.parquet")
    reasons = sorted(json.loads(row["reasons_json"])[0] for row in quarantine)
    assert reasons == ["cfr_reference_missing_part", "cfr_reference_missing_title"]
    by_reason = receipt["counts"]["quarantine_by_source_and_reason"]["federal_register"]
    assert by_reason["cfr_reference_missing_part"] == 1
    assert by_reason["cfr_reference_missing_title"] == 1


# --------------------------------------------------------------------------
# quarantine
# --------------------------------------------------------------------------


def test_an_agency_entry_without_a_slug_is_quarantined(tmp_path):
    """A missing slug cannot become a crosswalk row, and must not vanish."""

    output_dir, receipt = build(
        tmp_path,
        federal_register=[
            fr_row("2026-00001", agencies=[{"raw_name": "MYSTERY", "id": 1, "parent_id": None}]),
            fr_row("2026-00002", agencies=[NUCLEAR]),
        ],
        dockets=[docket_row("NRC-2026-0001", "NRC"), docket_row("NRC-2026-0002", "NRC")],
        fr_links=[
            link_row("NRC-2026-0001", "2026-00001"),
            link_row("NRC-2026-0002", "2026-00002"),
        ],
    )
    quarantine = read_rows(output_dir / "quarantine.parquet")
    assert len(quarantine) == 1
    row = quarantine[0]
    assert json.loads(row["reasons_json"]) == ["agency_entry_missing_slug"]
    assert row["document_ref"] == "2026-00001"
    assert row["source"] == "federal_register"
    assert row["evidence_field"] == "agencies_json"

    # The slugless document still counts toward the code's document total, so
    # the share honestly reflects that one of the two documents said nothing.
    code = codes_by_name(output_dir)["NRC"]
    assert code["support_documents"] == "2"
    assert code["confidence_share"] == "0.500000"
    assert receipt["counts"]["quarantine_by_source_and_reason"]["federal_register"] == {
        "agency_entry_missing_slug": 1
    }


def test_unparseable_json_columns_are_quarantined(tmp_path):
    output_dir, receipt = build(
        tmp_path,
        federal_register=[
            fr_row("2026-00001", raw_agencies="{not json"),
            {
                "document_number": "2026-00002",
                "agencies_json": json.dumps([NUCLEAR]),
                "cfr_references_json": "{not json",
            },
        ],
    )
    quarantine = read_rows(output_dir / "quarantine.parquet")
    by_document = {row["document_ref"]: json.loads(row["reasons_json"]) for row in quarantine}
    assert by_document == {
        "2026-00001": ["unparseable_agencies_json"],
        "2026-00002": ["unparseable_cfr_references_json"],
    }
    assert receipt["counts"]["quarantined_rows_total"] == 2


def test_a_docket_without_an_agency_code_is_quarantined(tmp_path):
    output_dir, receipt = build(
        tmp_path,
        federal_register=[fr_row("2026-00001", agencies=[NUCLEAR])],
        dockets=[docket_row("NRC-2026-0001", ""), docket_row("NRC-2026-0002", "NRC")],
        fr_links=[link_row("NRC-2026-0001", "2026-00001")],
    )
    quarantine = read_rows(output_dir / "quarantine.parquet")
    assert len(quarantine) == 1
    assert json.loads(quarantine[0]["reasons_json"]) == ["docket_missing_agency_code"]
    assert quarantine[0]["source"] == "dockets"
    assert quarantine[0]["docket_ref"] == "NRC-2026-0001"
    assert receipt["counts"]["quarantine_by_source_and_reason"]["dockets"] == {
        "docket_missing_agency_code": 1
    }


def test_a_document_reference_outside_the_pin_is_quarantined(tmp_path):
    output_dir, receipt = build(
        tmp_path,
        federal_register=[fr_row("2026-00001", agencies=[NUCLEAR])],
        dockets=[docket_row("NRC-2026-0001", "NRC")],
        fr_links=[link_row("NRC-2026-0001", "2026-09999")],
        documents=[document_row("NRC-2026-0001", "NRC", "2026-08888")],
    )
    quarantine = read_rows(output_dir / "quarantine.parquet")
    by_source = {row["source"]: json.loads(row["reasons_json"]) for row in quarantine}
    assert by_source == {
        "fr_docket_links": ["document_not_in_federal_register"],
        "documents": ["document_not_in_federal_register"],
    }
    assert receipt["counts"]["quarantined_rows_total"] == 2
    assert codes_by_name(output_dir)["NRC"]["tier"] == "unmapped"


def test_unjoinable_link_rows_are_a_receipted_coverage_fact_not_a_quarantine(tmp_path):
    """Docket ids absent from the dockets table are expected non-overlap.

    They are counted and named in the receipt rather than materialized as
    hundreds of thousands of quarantine rows.
    """

    output_dir, receipt = build(
        tmp_path,
        federal_register=[fr_row("2026-00001", agencies=[NUCLEAR])],
        dockets=[docket_row("NRC-2026-0001", "NRC")],
        fr_links=[
            link_row("NRC-2026-0001", "2026-00001"),
            link_row("FRL-13416-01-OCSPP", "2026-00001"),
        ],
    )
    coverage = receipt["coverage"]
    assert coverage["fr_docket_links_rows_with_unknown_docket"] == 1
    assert coverage["fr_docket_links_rows_joined"] == 1
    assert read_rows(output_dir / "quarantine.parquet") == []


# --------------------------------------------------------------------------
# receipt, determinism
# --------------------------------------------------------------------------


def _full_build(tmp_path, out_name="artifact"):
    tmp_path.mkdir(parents=True, exist_ok=True)
    corpus = _aviation_corpus()
    corpus["federal_register"] = [
        fr_row(row["document_number"], agencies=[TRANSPORT, AVIATION], cfr_refs=[cfr_ref(14, "39")])
        for row in corpus["federal_register"]
    ]
    corpus["federal_register"].append(
        fr_row("2026-00100", agencies=[NUCLEAR], cfr_refs=[cfr_ref(10, "72"), cfr_ref(10, None)])
    )
    corpus["federal_register"].append(fr_row("2026-00101", raw_agencies="{not json"))
    corpus["dockets"].append(docket_row("NRC-2026-0100", "NRC"))
    corpus["dockets"].append(docket_row("ORPHAN-2026-0001", "ORPHAN"))
    corpus["fr_links"].append(link_row("NRC-2026-0100", "2026-00100"))
    corpus["documents"] = [document_row("NRC-2026-0100", "NRC", "2026-00100")]
    return build(tmp_path, out_name=out_name, **corpus)


ARTIFACT_NAMES = (
    "agency-crosswalk.parquet",
    "agency-codes.parquet",
    "agency-parents.parquet",
    "cfr-part-agencies.parquet",
    "quarantine.parquet",
)


def test_receipt_pins_inputs_artifacts_thresholds_and_counts(tmp_path):
    output_dir, receipt = _full_build(tmp_path)

    for name in ("federal_register", "dockets", "fr_docket_links", "documents"):
        pinned = receipt["inputs"][name]
        assert pinned["sha256"].startswith("sha256:")
        assert pinned["rows"] >= 0
        assert Path(pinned["path"]).name == f"{name}.parquet"
        assert not Path(pinned["path"]).is_absolute(), "no absolute paths in the receipt"

    for name in ARTIFACT_NAMES:
        digest = receipt["artifacts"][name]["sha256"]
        assert digest == "sha256:" + hashlib.sha256((output_dir / name).read_bytes()).hexdigest()
        assert receipt["artifacts"][name]["rows"] == len(read_rows(output_dir / name))

    thresholds = receipt["thresholds"]
    assert thresholds["confident_share"] == mod.CONFIDENT_SHARE
    assert thresholds["probable_share"] == mod.PROBABLE_SHARE
    assert thresholds["min_confident_documents"] == mod.MIN_CONFIDENT_DOCUMENTS
    assert thresholds["min_probable_documents"] == mod.MIN_PROBABLE_DOCUMENTS
    assert thresholds["specificity_margin"] == mod.SPECIFICITY_MARGIN

    counts = receipt["counts"]
    assert counts["agency_codes_total"] == len(read_rows(output_dir / "agency-codes.parquet"))
    assert sum(counts["tier_histogram"].values()) == counts["agency_codes_total"]
    assert set(counts["tier_histogram"]) == {"confident", "probable", "ambiguous", "unmapped"}
    assert counts["quarantined_rows_total"] == len(read_rows(output_dir / "quarantine.parquet"))

    assert receipt["schema_version"] == mod.ARTIFACT_SCHEMA_VERSION
    assert receipt["tier_policy"] == mod.TIER_POLICY
    assert receipt["artifact_id"].startswith("urn:spicyregs:agency-crosswalk-artifact:")

    saved = json.loads((output_dir / "receipt.json").read_text())
    assert saved == receipt

    serialized = json.dumps(receipt)
    assert "/Users/" not in serialized and "/tmp" not in serialized


def test_two_builds_are_byte_identical(tmp_path):
    dir_a, receipt_a = _full_build(tmp_path / "a")
    dir_b, receipt_b = _full_build(tmp_path / "b")
    assert receipt_a == receipt_b
    for name in ARTIFACT_NAMES + ("receipt.json",):
        assert (dir_a / name).read_bytes() == (dir_b / name).read_bytes(), name


def test_row_identifiers_are_stable_and_unique(tmp_path):
    output_dir, _ = _full_build(tmp_path)
    for name, column, prefix in (
        ("agency-crosswalk.parquet", "crosswalk_id", "urn:spicyregs:agency-crosswalk:"),
        ("agency-codes.parquet", "agency_code_id", "urn:spicyregs:agency-code:"),
        ("cfr-part-agencies.parquet", "cfr_agency_id", "urn:spicyregs:cfr-part-agency:"),
        ("quarantine.parquet", "quarantine_id", "urn:spicyregs:agency-crosswalk-quarantine:"),
    ):
        identifiers = [row[column] for row in read_rows(output_dir / name)]
        assert identifiers, name
        assert len(identifiers) == len(set(identifiers)), name
        assert all(identifier.startswith(prefix) for identifier in identifiers), name
