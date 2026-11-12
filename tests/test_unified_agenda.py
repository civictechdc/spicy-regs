"""Hermetic tests for the Unified Agenda ingest (no network).

Covers the pieces with real logic: parsing the reginfo.gov ``REGINFO_RIN_DATA``
XML export into normalized RIN dicts (``UnifiedAgendaReader._fetch_edition`` /
``_normalize``), mapping those onto the published 17-column schema (``_shape``,
including timetable-date derivation and the deterministic per-RIN URL),
discovering the latest published edition (``candidate_editions`` /
``probe_edition`` / ``discover_editions``), and the incremental build fetching
only editions newer than what is stored. HTTP is either monkeypatched or served
by ``httpx.MockTransport``, so no network is touched.
"""

from __future__ import annotations

import importlib
import json
from datetime import date
from unittest.mock import patch

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.sources import unified_agenda
from spicy_regs.sources.unified_agenda import (
    UnifiedAgendaReader,
    UnifiedAgendaUnavailable,
    candidate_editions,
    discover_editions,
    probe_edition,
)
from spicy_regs.transforms.build_unified_agenda import COLUMNS, _SCHEMA, _prior_max_edition, _shape

# ``spicy_regs.transforms`` re-exports the build *function* under the module's
# name, so fetch the module itself to monkeypatch its globals.
build_mod = importlib.import_module("spicy_regs.transforms.build_unified_agenda")

# A two-record slice of the real export, matching the observed tag structure:
# <REGINFO_RIN_DATA> root, repeated <RIN_INFO> records, nested AGENCY / CFR_LIST /
# LEGAL_AUTHORITY_LIST / TIMETABLE_LIST. The second record exercises empty lists
# and an unparseable ("To Be Determined") timetable date.
_FIXTURE_XML = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<REGINFO_RIN_DATA RUN_DATE="2025-11-01-05:00">
    <RIN_INFO>
        <RIN>2060-AV12</RIN>
        <PUBLICATION>
            <PUBLICATION_ID>202510</PUBLICATION_ID>
            <PUBLICATION_TITLE>The Unified Agenda</PUBLICATION_TITLE>
        </PUBLICATION>
        <AGENCY>
            <CODE>2060</CODE>
            <NAME>Environmental Protection Agency</NAME>
            <ACRONYM>EPA</ACRONYM>
        </AGENCY>
        <PARENT_AGENCY>
            <CODE>2000</CODE>
            <NAME>Environmental Protection Agency</NAME>
            <ACRONYM>EPA</ACRONYM>
        </PARENT_AGENCY>
        <RULE_TITLE>A Planned Rule</RULE_TITLE>
        <ABSTRACT>Plans to do a thing.</ABSTRACT>
        <PRIORITY_CATEGORY>Other Significant</PRIORITY_CATEGORY>
        <RIN_STATUS>Active</RIN_STATUS>
        <RULE_STAGE>Proposed Rule Stage</RULE_STAGE>
        <MAJOR>Yes</MAJOR>
        <CFR_LIST>
            <CFR>40 CFR 60</CFR>
            <CFR>40 CFR 63</CFR>
        </CFR_LIST>
        <LEGAL_AUTHORITY_LIST>
            <LEGAL_AUTHORITY>42 U.S.C. 7401</LEGAL_AUTHORITY>
        </LEGAL_AUTHORITY_LIST>
        <TIMETABLE_LIST>
            <TIMETABLE>
                <TTBL_ACTION>NPRM</TTBL_ACTION>
                <TTBL_DATE>06/15/2024</TTBL_DATE>
                <FR_CITATION>89 FR 12345</FR_CITATION>
            </TIMETABLE>
            <TIMETABLE>
                <TTBL_ACTION>NPRM Comment Period End</TTBL_ACTION>
                <TTBL_DATE>08/14/2024</TTBL_DATE>
            </TIMETABLE>
            <TIMETABLE>
                <TTBL_ACTION>Final Rule</TTBL_ACTION>
                <TTBL_DATE>03/01/2025</TTBL_DATE>
            </TIMETABLE>
        </TIMETABLE_LIST>
    </RIN_INFO>
    <RIN_INFO>
        <RIN>1234-AB56</RIN>
        <PUBLICATION>
            <PUBLICATION_ID>202510</PUBLICATION_ID>
        </PUBLICATION>
        <AGENCY>
            <CODE>1234</CODE>
            <NAME>Some Agency</NAME>
        </AGENCY>
        <RULE_TITLE>Another Rule</RULE_TITLE>
        <RULE_STAGE>Long-Term Actions</RULE_STAGE>
        <CFR_LIST/>
        <LEGAL_AUTHORITY_LIST/>
        <TIMETABLE_LIST>
            <TIMETABLE>
                <TTBL_ACTION>Interim Final Rule</TTBL_ACTION>
                <TTBL_DATE>To Be Determined</TTBL_DATE>
            </TIMETABLE>
        </TIMETABLE_LIST>
    </RIN_INFO>
</REGINFO_RIN_DATA>
"""


def _read_fixture(edition: str = "202510") -> list[dict]:
    """Run the reader over the inline fixture with the network download stubbed."""
    reader = UnifiedAgendaReader(editions=(edition,))

    def fake_download(self, ed):
        assert ed == edition
        return _FIXTURE_XML

    # Patch the bound method on the class so iter_records exercises the real
    # iterparse + _normalize path against the fixture bytes. The stubbed
    # _download ignores the HTTP client, so none needs to be set.
    with patch.object(UnifiedAgendaReader, "_download", fake_download):
        return list(reader._fetch_edition(edition))


def test_reader_parses_records_and_stamps_edition():
    records = _read_fixture("202510")
    assert [r["rin"] for r in records] == ["2060-AV12", "1234-AB56"]
    # Edition is stamped on every record (half the dedup key).
    assert all(r["agenda_edition"] == "202510" for r in records)
    # AGENCY/ACRONYM wins for agency_code, falling back to CODE when absent.
    assert records[0]["agency_code"] == "EPA"
    assert records[1]["agency_code"] == "1234"
    assert records[0]["agency_name"] == "Environmental Protection Agency"


def test_reader_normalizes_lists_and_timetable():
    rec = _read_fixture("202510")[0]
    assert rec["cfr_references"] == ["40 CFR 60", "40 CFR 63"]
    assert rec["legal_authority"] == ["42 U.S.C. 7401"]
    assert rec["timetable"][0] == {
        "action": "NPRM",
        "date": "06/15/2024",
        "fr_citation": "89 FR 12345",
    }
    # Optional child (FR_CITATION) absent -> None.
    assert rec["timetable"][1]["fr_citation"] is None


def test_shape_produces_exact_schema():
    row = _shape(_read_fixture("202510")[0])
    assert set(row) == set(COLUMNS)


def test_shape_maps_serializes_and_derives():
    row = _shape(_read_fixture("202510")[0])
    assert row["rin"] == "2060-AV12"
    assert row["agenda_edition"] == "202510"
    assert row["rule_stage"] == "Proposed Rule Stage"
    assert row["priority_category"] == "Other Significant"
    assert row["major"] == "Yes"
    assert row["publication_id"] == "202510"
    # Array fields serialize to JSON strings.
    assert json.loads(row["timetable_json"])[0]["action"] == "NPRM"
    assert json.loads(row["cfr_references_json"]) == ["40 CFR 60", "40 CFR 63"]
    assert json.loads(row["legal_authority_json"]) == ["42 U.S.C. 7401"]
    # Timetable dates -> ISO; first = earliest, next = earliest later action.
    assert row["first_action_date"] == "2024-06-15"
    assert row["next_action_date"] == "2024-08-14"
    # Deterministic per-RIN reginfo detail URL.
    assert row["url"] == "https://www.reginfo.gov/public/do/eAgendaViewRule?pubId=202510&RIN=2060-AV12"


def test_shape_handles_missing_and_unparseable_dates():
    row = _shape(_read_fixture("202510")[1])
    # No parseable timetable dates -> both action dates null.
    assert row["first_action_date"] is None
    assert row["next_action_date"] is None
    # Empty lists still serialize to "[]".
    assert row["cfr_references_json"] == "[]"
    assert row["legal_authority_json"] == "[]"
    # Absent fields are null.
    assert row["abstract"] is None
    assert row["priority_category"] is None


def test_download_rejects_non_xml_body(monkeypatch):
    """A non-XML body (e.g. the old eAgendaXmlReport HTML page) yields nothing."""

    class _Resp:
        status_code = 200
        content = b"<!DOCTYPE html><html><body>listing page</body></html>"

        def raise_for_status(self):
            return None

    reader = UnifiedAgendaReader(editions=("202510",))

    class _Client:
        def get(self, url, params=None):
            return _Resp()

    monkeypatch.setattr(reader, "_client", _Client())
    assert reader._download("202510") is None
    assert list(reader._fetch_edition("202510")) == []


# --------------------------------------------------------------------------- #
# Edition discovery (hermetic via httpx.MockTransport).
# --------------------------------------------------------------------------- #

# What reginfo.gov actually returns for an unpublished edition: 200 + an HTML
# error page that starts with a run of blank lines.
_HTML_ERROR_PAGE = b"\n" * 20 + b'<html lang="en"><head><title>Regulatory Review Disclosure</title></head></html>'


def _edition_of(request: httpx.Request) -> str:
    name = request.url.params["f"]
    assert name.startswith("REGINFO_RIN_DATA_") and name.endswith(".xml")
    return name.removeprefix("REGINFO_RIN_DATA_").removesuffix(".xml")


def _reginfo(published: set[str], probed: list[str] | None = None) -> httpx.Client:
    """A MockTransport client serving XML for ``published`` editions, the HTML error page otherwise."""

    def handler(request: httpx.Request) -> httpx.Response:
        edition = _edition_of(request)
        if probed is not None:
            probed.append(edition)
        if edition in published:
            return httpx.Response(200, content=_FIXTURE_XML, headers={"content-type": "application/xml"})
        return httpx.Response(200, content=_HTML_ERROR_PAGE, headers={"content-type": "text/html"})

    return httpx.Client(transport=httpx.MockTransport(handler))


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(unified_agenda.time, "sleep", lambda _s: None)


_TODAY = date(2026, 11, 12)


def test_candidate_editions_newest_first_from_this_years_fall():
    assert candidate_editions(_TODAY) == [
        "202610",
        "202604",
        "202510",
        "202504",
        "202410",
        "202404",
        "202310",
        "202304",
    ]


def test_candidate_editions_bounded_and_well_formed():
    # Early in the year this year's editions are still candidates (cheap misses).
    assert candidate_editions(date(2027, 1, 5), count=3) == ["202710", "202704", "202610"]
    assert candidate_editions(_TODAY, count=1) == ["202610"]
    cands = candidate_editions(_TODAY)
    assert len(cands) == unified_agenda.MAX_LOOKBACK_EDITIONS
    assert all(len(c) == 6 and c.isdigit() and c[4:] in {"04", "10"} for c in cands)
    assert cands == sorted(cands, reverse=True)


def test_probe_detects_xml_vs_html_error_page():
    with _reginfo({"202510"}) as client:
        assert probe_edition(client, "202510") is True
        assert probe_edition(client, "202604") is False


def test_probe_treats_non_200_as_not_published():
    def handler(request):
        return httpx.Response(404, content=b"not found")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert probe_edition(client, "202604") is False


def test_probe_retries_transient_errors(no_sleep):
    calls = []

    def handler(request):
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(503)
        return httpx.Response(200, content=b"  \n" + _FIXTURE_XML)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert probe_edition(client, "202510") is True
    assert len(calls) == 2


@pytest.mark.parametrize("failure", ["503", "connect"])
def test_probe_raises_when_reginfo_is_down(no_sleep, failure):
    def handler(request):
        if failure == "503":
            return httpx.Response(503)
        raise httpx.ConnectError("down", request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(UnifiedAgendaUnavailable):
            probe_edition(client, "202510")


def test_discover_first_run_stops_at_newest_published():
    probed: list[str] = []
    with _reginfo({"202504", "202510"}, probed) as client:
        assert discover_editions(today=_TODAY, client=client) == ("202510",)
    # Newest-first, and nothing older than the first hit is requested.
    assert probed == ["202610", "202604", "202510"]


def test_discover_incremental_returns_only_newer_published():
    probed: list[str] = []
    with _reginfo({"202510", "202604"}, probed) as client:
        assert discover_editions("202510", today=_TODAY, client=client) == ("202604",)
    assert probed == ["202610", "202604"]


def test_discover_incremental_fills_a_multi_edition_gap_oldest_first():
    with _reginfo({"202504", "202510", "202604"}) as client:
        assert discover_editions("202504", today=_TODAY, client=client) == ("202510", "202604")


def test_discover_incremental_up_to_date():
    probed: list[str] = []
    with _reginfo({"202510"}, probed) as client:
        assert discover_editions("202510", today=_TODAY, client=client) == ()
    # Newer candidates miss, then the stored edition is re-probed as a sanity check.
    assert probed == ["202610", "202604", "202510"]


def test_discover_raises_when_nothing_probes_as_published():
    # Every response is the HTML page: the endpoint changed. Don't call that "up to date".
    with _reginfo(set()) as client:
        with pytest.raises(UnifiedAgendaUnavailable, match="changed"):
            discover_editions("202510", today=_TODAY, client=client)
        with pytest.raises(UnifiedAgendaUnavailable):
            discover_editions(today=_TODAY, client=client)


def test_discover_raises_when_reginfo_is_down(no_sleep):
    def handler(request):
        raise httpx.ConnectError("down", request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(UnifiedAgendaUnavailable):
            discover_editions("202510", today=_TODAY, client=client)


@pytest.fixture
def warnings_log():
    """Collect loguru WARNING-and-above messages (loguru bypasses caplog)."""
    messages: list[str] = []
    sink_id = unified_agenda.logger.add(lambda m: messages.append(str(m)), level="WARNING", format="{message}")
    yield messages
    unified_agenda.logger.remove(sink_id)


def test_discover_warns_about_an_older_edition_skipped_for_good(warnings_log):
    # Stored 202510; Fall 2026 is out but Spring 2026 still isn't. 202604 can't be
    # an error (real gaps exist, e.g. no 201804), but it must not vanish silently.
    with _reginfo({"202510", "202610"}) as client:
        assert discover_editions("202510", today=_TODAY, client=client) == ("202610",)
    assert len(warnings_log) == 1
    assert "202604" in warnings_log[0] and "202610" in warnings_log[0]


def test_discover_does_not_warn_without_a_gap(warnings_log):
    with _reginfo({"202510", "202604"}) as client:
        # 202610 is newer than the newest published edition, so it's not "skipped".
        assert discover_editions("202510", today=_TODAY, client=client) == ("202604",)
    assert warnings_log == []


class _OneChunkThenFail(httpx.SyncByteStream):
    """A streamed body whose reads past the first chunk fail the test."""

    def __init__(self, first: bytes) -> None:
        self.first = first
        self.chunks_read = 0

    def __iter__(self):
        self.chunks_read += 1
        yield self.first
        self.chunks_read += 1
        raise AssertionError("probe read past the first chunk")


@pytest.mark.parametrize("first", [_FIXTURE_XML[:200], _HTML_ERROR_PAGE[:200]])
def test_probe_stops_after_the_first_chunk(first):
    stream = _OneChunkThenFail(first)

    def handler(request):
        return httpx.Response(200, stream=stream)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert probe_edition(client, "202510") is first.lstrip().startswith(b"<?xml")
    assert stream.chunks_read == 1


def test_probe_uses_a_short_timeout():
    seen: list[dict] = []

    def handler(request):
        seen.append(request.extensions["timeout"])
        return httpx.Response(200, content=_FIXTURE_XML)

    # Even on a client configured with the long download timeout.
    with httpx.Client(transport=httpx.MockTransport(handler), timeout=300.0) as client:
        probe_edition(client, "202510")
    assert seen[0]["read"] == 30.0 and seen[0]["connect"] == 10.0


def test_reader_without_editions_discovers_latest(monkeypatch):
    monkeypatch.setattr(unified_agenda, "discover_editions", lambda **_kw: ("202604",))
    monkeypatch.setattr(UnifiedAgendaReader, "_download", lambda self, ed: _FIXTURE_XML)
    reader = UnifiedAgendaReader()
    records = list(reader.iter_records())
    assert {r["agenda_edition"] for r in records} == {"202604"}
    assert reader.edition_counts == {"202604": 2}


# --------------------------------------------------------------------------- #
# Incremental build: fetch only editions newer than what is stored.
# --------------------------------------------------------------------------- #


def _write_prior(output_dir, editions: list[str], rins: tuple[str, ...] = ("0000-AA00",)) -> None:
    """Write a prior table with one row per (rin, edition), each RIN distinct within an edition."""
    rows = [
        {c: None for c in COLUMNS} | {"rin": rin, "agenda_edition": ed, "title": f"prior {rin} {ed}"}
        for ed in editions
        for rin in rins
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=_SCHEMA), output_dir / "_ua_prior.parquet")


def _scratch_left(output_dir) -> list[str]:
    return sorted(p.name for p in output_dir.glob("_ua_*.parquet"))


def _editions_in(path) -> set[str]:
    return set(pq.read_table(path, columns=["agenda_edition"]).column("agenda_edition").to_pylist())


def test_prior_max_edition_ignores_malformed_values(tmp_output):
    _write_prior(tmp_output, ["202410", "202510", "2012", "999999"])
    assert _prior_max_edition(tmp_output / "_ua_prior.parquet") == "202510"
    assert _prior_max_edition(tmp_output / "missing.parquet") is None


def test_build_fetches_editions_newer_than_stored(tmp_output, monkeypatch):
    # Two distinct RINs in each of two stored editions = 4 prior rows.
    _write_prior(tmp_output, ["202504", "202510"], rins=("1111-AA01", "2222-AA02"))
    seen_after: list[str | None] = []

    def fake_discover(after=None, **_kw):
        seen_after.append(after)
        return ("202604",)

    fetched: list[str] = []

    def fake_download(self, ed):
        fetched.append(ed)
        return _FIXTURE_XML

    monkeypatch.setattr(build_mod, "discover_editions", fake_discover)
    monkeypatch.setattr(UnifiedAgendaReader, "_download", fake_download)

    out = build_mod.build_unified_agenda(tmp_output)
    assert seen_after == ["202510"]
    assert fetched == ["202604"]
    # Prior editions are kept alongside the newly ingested one: 4 prior rows + the
    # fixture's 2 RINs stamped 202604.
    table = pq.read_table(out)
    assert table.num_rows == 6
    assert _editions_in(out) == {"202504", "202510", "202604"}
    pairs = set(zip(table.column("rin").to_pylist(), table.column("agenda_edition").to_pylist(), strict=True))
    assert len(pairs) == 6
    assert ("2060-AV12", "202604") in pairs and ("1111-AA01", "202504") in pairs
    assert _scratch_left(tmp_output) == []


def test_build_new_row_replaces_same_rin_and_edition(tmp_output, monkeypatch):
    # The prior already holds 2060-AV12 @ 202604; re-fetching it refreshes in place.
    _write_prior(tmp_output, ["202510", "202604"], rins=("2060-AV12",))
    monkeypatch.setattr(UnifiedAgendaReader, "_download", lambda self, ed: _FIXTURE_XML)
    out = build_mod.build_unified_agenda(tmp_output, editions=("202604",))
    table = pq.read_table(out)
    # 202510/2060-AV12 (prior) + 202604/{2060-AV12, 1234-AB56} (new) = 3; no duplicate.
    assert table.num_rows == 3
    rows = {(r["rin"], r["agenda_edition"]): r["title"] for r in table.to_pylist()}
    assert rows[("2060-AV12", "202604")] == "A Planned Rule"
    assert rows[("2060-AV12", "202510")] == "prior 2060-AV12 202510"


def test_build_up_to_date_fetches_nothing_and_keeps_prior(tmp_output, monkeypatch):
    _write_prior(tmp_output, ["202510"])
    monkeypatch.setattr(build_mod, "discover_editions", lambda after=None, **_kw: ())

    def boom(self, ed):
        raise AssertionError("nothing should be downloaded")

    monkeypatch.setattr(UnifiedAgendaReader, "_download", boom)
    out = build_mod.build_unified_agenda(tmp_output)
    assert _editions_in(out) == {"202510"}


def test_build_first_run_discovers_latest(tmp_output, monkeypatch):
    monkeypatch.setattr(build_mod.r2, "download", lambda *_a, **_kw: False)
    seen_after: list[str | None] = []

    def fake_discover(after=None, **_kw):
        seen_after.append(after)
        return ("202510",)

    monkeypatch.setattr(build_mod, "discover_editions", fake_discover)
    monkeypatch.setattr(UnifiedAgendaReader, "_download", lambda self, ed: _FIXTURE_XML)
    out = build_mod.build_unified_agenda(tmp_output)
    assert seen_after == [None]
    assert _editions_in(out) == {"202510"}


def test_build_fails_loudly_when_reginfo_is_down(tmp_output, monkeypatch):
    _write_prior(tmp_output, ["202510"])

    def down(after=None, **_kw):
        raise UnifiedAgendaUnavailable("down")

    monkeypatch.setattr(build_mod, "discover_editions", down)
    with pytest.raises(UnifiedAgendaUnavailable):
        build_mod.build_unified_agenda(tmp_output)
    assert not (tmp_output / build_mod.OUTPUT).exists()
    # Scratch files are cleaned up on the raise path too (no stale prior left behind).
    assert _scratch_left(tmp_output) == []


def test_build_fails_when_a_published_edition_downloads_empty(tmp_output, monkeypatch):
    _write_prior(tmp_output, ["202510"])
    monkeypatch.setattr(build_mod, "discover_editions", lambda after=None, **_kw: ("202604",))
    monkeypatch.setattr(UnifiedAgendaReader, "_download", lambda self, ed: None)
    with pytest.raises(UnifiedAgendaUnavailable, match="202604"):
        build_mod.build_unified_agenda(tmp_output)
    assert not (tmp_output / build_mod.OUTPUT).exists()
    # Scratch files are cleaned up on the raise path too (no stale prior left behind).
    assert _scratch_left(tmp_output) == []
