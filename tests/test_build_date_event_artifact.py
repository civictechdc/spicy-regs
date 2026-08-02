"""Hermetic checks for the unified typed date-event artifact builder.

Every fixture here is synthetic. Nothing reads the real ontology outputs, so
these tests state what the tool guarantees rather than what one particular
build happened to contain.

The builder consumes three published tables — ``comment_periods`` (coalesced
comment intervals), ``fr_docket_links`` (effective / comments-close dates per
FR document), ``fcc_proceedings`` (FCC comment and reply-comment windows) —
and emits one typed event stream plus one typed quarantine partition and a
deterministic canonical-JSON receipt. Sanity bounds are the validated
comment-period bounds (close year < 1994, close year > 2028, duration >
5 x 365 days) applied uniformly and receipted per source; quarantine is a
typed partition, never a silent drop.
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
TOOL_PATH = REPO_ROOT / "tools" / "build_date_event_artifact.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("build_date_event_artifact", TOOL_PATH)
    assert spec and spec.loader, f"could not load {TOOL_PATH}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


mod = _load_module()


# --------------------------------------------------------------------------
# synthetic dataset helpers
# --------------------------------------------------------------------------


COMMENT_PERIOD_FIELDS = [
    "comment_period_id",
    "proceeding_ids_json",
    "rins_json",
    "docket_ids_json",
    "open_date",
    "close_date",
    "source",
    "evidence_ids_json",
]

FR_LINK_FIELDS = [
    "docket_id",
    "document_number",
    "publication_date",
    "effective_on",
    "comments_close_on",
]

FCC_FIELDS = [
    "name",
    "id_proceeding",
    "comment_start_date",
    "comment_end_date",
    "reply_comment_start_date",
    "reply_comment_end_date",
]


def comment_period_row(
    period_id="cp-1",
    *,
    proceedings=(),
    rins=(),
    dockets=("EPA-HQ-2026-0001",),
    open_date="2026-01-05",
    close_date="2026-03-06",
    source="federal_register.comments_close_on",
    evidence=("2026-00001",),
):
    return {
        "comment_period_id": period_id,
        "proceeding_ids_json": json.dumps(sorted(proceedings)),
        "rins_json": json.dumps(sorted(rins)),
        "docket_ids_json": json.dumps(sorted(dockets)),
        "open_date": open_date,
        "close_date": close_date,
        "source": source,
        "evidence_ids_json": json.dumps(sorted(evidence)),
    }


def fr_link_row(
    document_number="2026-00002",
    *,
    docket="EPA-HQ-2026-0001",
    publication_date="2026-01-05",
    effective_on=None,
    comments_close_on=None,
):
    return {
        "docket_id": docket,
        "document_number": document_number,
        "publication_date": publication_date,
        "effective_on": effective_on,
        "comments_close_on": comments_close_on,
    }


def fcc_row(
    name="26-108",
    *,
    id_proceeding="600001",
    comment_start=None,
    comment_end=None,
    reply_start=None,
    reply_end=None,
):
    return {
        "name": name,
        "id_proceeding": id_proceeding,
        "comment_start_date": comment_start,
        "comment_end_date": comment_end,
        "reply_comment_start_date": reply_start,
        "reply_comment_end_date": reply_end,
    }


def write_inputs(tmp_path, *, comment_periods=(), fr_links=(), fcc=()):
    paths = {}
    for name, fields, rows in (
        ("comment_periods", COMMENT_PERIOD_FIELDS, comment_periods),
        ("fr_docket_links", FR_LINK_FIELDS, fr_links),
        ("fcc_proceedings", FCC_FIELDS, fcc),
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
        comment_periods=paths["comment_periods"],
        fr_docket_links=paths["fr_docket_links"],
        fcc_proceedings=paths["fcc_proceedings"],
        output_dir=output_dir,
    )
    return output_dir, receipt


def read_rows(path):
    return pq.read_table(path).to_pylist()


# --------------------------------------------------------------------------
# comment_periods events
# --------------------------------------------------------------------------


def test_comment_period_interval_emits_open_and_close_events(tmp_path):
    output_dir, _ = build(
        tmp_path,
        comment_periods=[
            comment_period_row(
                proceedings=("urn:spicyregs:proceeding:p1",),
                rins=("2050-AA00",),
            )
        ],
    )
    events = read_rows(output_dir / "date-events.parquet")
    assert [event["event_type"] for event in events] == ["comment_close", "comment_open"]
    open_event = next(event for event in events if event["event_type"] == "comment_open")
    close_event = next(event for event in events if event["event_type"] == "comment_close")
    assert open_event["event_date"] == "2026-01-05"
    assert open_event["evidence_field"] == "open_date"
    assert close_event["event_date"] == "2026-03-06"
    assert close_event["evidence_field"] == "close_date"
    for event in (open_event, close_event):
        assert event["source"] == "comment_periods"
        assert json.loads(event["docket_refs_json"]) == ["EPA-HQ-2026-0001"]
        assert json.loads(event["proceeding_refs_json"]) == ["urn:spicyregs:proceeding:p1"]
        assert json.loads(event["rin_refs_json"]) == ["2050-AA00"]
        assert json.loads(event["evidence_refs_json"]) == ["2026-00001"]
        assert event["document_ref"] is None


def test_out_of_bounds_close_dates_are_quarantined_not_dropped(tmp_path):
    output_dir, receipt = build(
        tmp_path,
        comment_periods=[
            comment_period_row("cp-old", close_date="1909-02-12", open_date="1909-01-01"),
            comment_period_row("cp-far", close_date="3007-12-21", open_date="2026-01-01"),
            comment_period_row("cp-long", open_date="2018-01-01", close_date="2026-01-01"),
            comment_period_row("cp-good"),
        ],
    )
    events = read_rows(output_dir / "date-events.parquet")
    quarantine = read_rows(output_dir / "quarantine.parquet")
    assert len(events) == 2, "only the in-bounds interval emits events"
    assert len(quarantine) == 3
    reasons = {row["quarantine_id"]: json.loads(row["reasons_json"]) for row in quarantine}
    by_close = {row["close_date"]: json.loads(row["reasons_json"]) for row in quarantine}
    assert by_close["1909-02-12"] == ["date_before_1994"]
    assert by_close["3007-12-21"] == ["date_after_2028", "duration_over_5y"]
    assert by_close["2026-01-01"] == ["duration_over_5y"]
    assert all(reasons.values())
    counts = receipt["counts"]["quarantine_by_source_and_reason"]["comment_periods"]
    assert counts == {"date_after_2028": 1, "date_before_1994": 1, "duration_over_5y": 2}
    assert receipt["counts"]["quarantined_rows_by_source"]["comment_periods"] == 3


def test_inverted_intervals_are_quarantined_and_counted(tmp_path):
    output_dir, receipt = build(
        tmp_path,
        comment_periods=[
            comment_period_row("cp-inverted", open_date="2026-03-06", close_date="2026-01-05"),
            comment_period_row("cp-good"),
        ],
    )
    quarantine = read_rows(output_dir / "quarantine.parquet")
    assert len(quarantine) == 1
    assert json.loads(quarantine[0]["reasons_json"]) == ["inverted_interval"]
    assert receipt["counts"]["inverted_intervals_by_source"] == {
        "comment_periods": 1,
        "fcc_proceedings": 0,
    }


# --------------------------------------------------------------------------
# fr_docket_links events
# --------------------------------------------------------------------------


def test_fr_links_emit_effective_and_comment_close_events(tmp_path):
    output_dir, _ = build(
        tmp_path,
        fr_links=[
            fr_link_row(
                "2026-01000",
                effective_on="2026-09-01",
                comments_close_on="2026-05-01",
            )
        ],
    )
    events = read_rows(output_dir / "date-events.parquet")
    assert sorted(event["event_type"] for event in events) == ["comment_close", "effective"]
    effective = next(event for event in events if event["event_type"] == "effective")
    close = next(event for event in events if event["event_type"] == "comment_close")
    assert effective["event_date"] == "2026-09-01"
    assert effective["evidence_field"] == "effective_on"
    assert close["event_date"] == "2026-05-01"
    assert close["evidence_field"] == "comments_close_on"
    for event in (effective, close):
        assert event["source"] == "fr_docket_links"
        assert event["document_ref"] == "2026-01000"
        assert json.loads(event["docket_refs_json"]) == ["EPA-HQ-2026-0001"]
        assert json.loads(event["evidence_refs_json"]) == ["2026-01000"]


def test_fr_links_dedupe_per_document_and_union_dockets(tmp_path):
    output_dir, _ = build(
        tmp_path,
        fr_links=[
            fr_link_row("2026-01000", docket="EPA-HQ-2026-0001", effective_on="2026-09-01"),
            fr_link_row("2026-01000", docket="EPA-HQ-2026-0002", effective_on="2026-09-01"),
        ],
    )
    events = read_rows(output_dir / "date-events.parquet")
    assert len(events) == 1
    assert json.loads(events[0]["docket_refs_json"]) == ["EPA-HQ-2026-0001", "EPA-HQ-2026-0002"]


def test_fr_link_dates_are_bounded_and_quarantined_with_the_same_policy(tmp_path):
    output_dir, receipt = build(
        tmp_path,
        fr_links=[
            fr_link_row("2026-01001", effective_on="3017-11-13"),
            fr_link_row("2026-01002", effective_on="1008-07-14"),
            fr_link_row("2026-01003", comments_close_on="1986-09-23"),
            fr_link_row("2026-01004", effective_on="2026-09-01"),
        ],
    )
    events = read_rows(output_dir / "date-events.parquet")
    quarantine = read_rows(output_dir / "quarantine.parquet")
    assert len(events) == 1 and events[0]["document_ref"] == "2026-01004"
    assert len(quarantine) == 3
    by_document = {row["document_ref"]: json.loads(row["reasons_json"]) for row in quarantine}
    assert by_document == {
        "2026-01001": ["date_after_2028"],
        "2026-01002": ["date_before_1994"],
        "2026-01003": ["date_before_1994"],
    }
    assert receipt["counts"]["quarantined_rows_by_source"]["fr_docket_links"] == 3


# --------------------------------------------------------------------------
# fcc_proceedings events
# --------------------------------------------------------------------------


def test_fcc_windows_emit_typed_events_when_populated(tmp_path):
    output_dir, _ = build(
        tmp_path,
        fcc=[
            fcc_row(
                "26-108",
                comment_start="2026-07-01T00:00:00Z",
                comment_end="2026-07-31T23:59:59Z",
                reply_end="2026-08-30",
            )
        ],
    )
    events = read_rows(output_dir / "date-events.parquet")
    assert sorted(event["event_type"] for event in events) == [
        "comment_close",
        "comment_open",
        "reply_comment_close",
    ]
    reply = next(event for event in events if event["event_type"] == "reply_comment_close")
    assert reply["event_date"] == "2026-08-30"
    assert reply["evidence_field"] == "reply_comment_end_date"
    assert reply["source"] == "fcc_proceedings"
    assert json.loads(reply["proceeding_refs_json"]) == ["26-108"]
    assert json.loads(reply["evidence_refs_json"]) == ["26-108"]
    assert reply["document_ref"] is None


def test_fcc_all_null_windows_emit_nothing_and_are_labelled(tmp_path):
    _, receipt = build(tmp_path, fcc=[fcc_row("26-108"), fcc_row("26-109")])
    assert receipt["counts"]["events_by_source"].get("fcc_proceedings", 0) == 0
    coverage = receipt["coverage_labels"]
    assert coverage["fcc_proceedings_rows_with_any_window"] == 0
    assert "2026-06-30" in coverage["fcc_coverage_floor"]


# --------------------------------------------------------------------------
# receipt, determinism, fixture slice
# --------------------------------------------------------------------------


def _full_build(tmp_path, out_name="artifact"):
    tmp_path.mkdir(parents=True, exist_ok=True)
    return build(
        tmp_path,
        out_name=out_name,
        comment_periods=[
            comment_period_row("cp-good"),
            comment_period_row("cp-old", close_date="1909-02-12", open_date="1909-01-01"),
        ],
        fr_links=[fr_link_row("2026-01000", effective_on="2026-09-01", comments_close_on="2026-05-01")],
        fcc=[fcc_row("26-108", reply_end="2026-08-30")],
    )


def test_receipt_counts_match_parquet_contents_and_pin_inputs(tmp_path):
    output_dir, receipt = _full_build(tmp_path)
    events = read_rows(output_dir / "date-events.parquet")
    quarantine = read_rows(output_dir / "quarantine.parquet")
    counts = receipt["counts"]
    assert counts["events_total"] == len(events)
    assert counts["quarantined_rows_total"] == len(quarantine)
    by_type = {}
    for event in events:
        by_type[event["event_type"]] = by_type.get(event["event_type"], 0) + 1
    assert counts["events_by_type"] == by_type
    for name in ("comment_periods", "fr_docket_links", "fcc_proceedings"):
        source_pin = receipt["inputs"][name]
        assert source_pin["sha256"].startswith("sha256:")
        assert source_pin["rows"] >= 0
        assert Path(source_pin["path"]).name == f"{name}.parquet"
    for artifact_name in ("date-events.parquet", "quarantine.parquet"):
        digest = receipt["artifacts"][artifact_name]["sha256"]
        path = output_dir / artifact_name
        assert digest == "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    assert receipt["schema_version"] == mod.ARTIFACT_SCHEMA_VERSION
    assert receipt["sanity_bounds_policy"] == mod.SANITY_BOUNDS_POLICY
    saved = json.loads((output_dir / "receipt.json").read_text())
    assert saved == receipt


def test_build_is_deterministic(tmp_path):
    dir_a, receipt_a = _full_build(tmp_path / "a")
    dir_b, receipt_b = _full_build(tmp_path / "b")
    assert receipt_a == receipt_b
    for name in ("date-events.parquet", "quarantine.parquet", "receipt.json"):
        assert (dir_a / name).read_bytes() == (dir_b / name).read_bytes()


def test_event_ids_are_stable_and_unique(tmp_path):
    output_dir, _ = _full_build(tmp_path)
    events = read_rows(output_dir / "date-events.parquet")
    identifiers = [event["event_id"] for event in events]
    assert len(identifiers) == len(set(identifiers))
    assert all(identifier.startswith("urn:spicyregs:date-event:") for identifier in identifiers)


def test_fixture_slice_is_a_subset_with_its_own_receipt(tmp_path):
    output_dir, receipt = _full_build(tmp_path)
    slice_dir = tmp_path / "slice"
    slice_receipt = mod.build_fixture_slice(
        artifact_dir=output_dir,
        output_dir=slice_dir,
        events_per_type=1,
        quarantine_rows_per_source=1,
    )
    parent_events = {event["event_id"] for event in read_rows(output_dir / "date-events.parquet")}
    slice_events = read_rows(slice_dir / "date-events.parquet")
    assert slice_events, "the slice carries events"
    assert {event["event_id"] for event in slice_events} <= parent_events
    by_type = {}
    for event in slice_events:
        by_type[event["event_type"]] = by_type.get(event["event_type"], 0) + 1
    assert all(count == 1 for count in by_type.values())
    assert slice_receipt["parent_artifact_id"] == receipt["artifact_id"]
    assert slice_receipt["parent_events_sha256"] == receipt["artifacts"]["date-events.parquet"]["sha256"]
    assert slice_receipt["slice_rule"] == mod.SLICE_RULE
    saved = json.loads((slice_dir / "receipt.json").read_text())
    assert saved == slice_receipt
    slice_counts = slice_receipt["counts"]
    assert slice_counts["events_total"] == len(slice_events)
