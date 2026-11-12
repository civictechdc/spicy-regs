"""Hermetic tests for the OIRA EO 12866 ingest (no network).

Covers the parts with real logic: picking exports off reginfo.gov's report
list page, parsing review and meeting XML into normalized dicts (including the
quirks of the real files: no ``<?xml`` declaration in old files, double-escaped
entities and ``DD-MON-YY`` dates in the meeting log, a missing ``MEETING_ID``),
dropping YTD rows that a per-year file already covers, shaping into the
published columns, and failing loudly when an export is missing. HTTP is served
by ``httpx.MockTransport``.
"""

from __future__ import annotations

import json
from collections.abc import Callable

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_regs.sources import oira
from spicy_regs.sources.oira import (
    OiraMeetingsReader,
    OiraReviewsReader,
    OiraUnavailable,
    _is_xml,
    meeting_files,
    parse_report_list,
    review_files,
)
from spicy_regs.transforms.build_oira import (
    MEETING_COLUMNS,
    REVIEW_COLUMNS,
    build_oira_meetings,
    build_oira_reviews,
)

_REPORT_LIST = """
<html><body>
<a href="/public/do/XMLViewFileAction?f=REGINFO_RIN_DATA_202510.xml">agenda</a>
<a href="/public/do/XMLViewFileAction?f=AGY_AGENCY_LIST.xml">agencies</a>
<a href="/public/do/XMLViewFileAction?f=EO_RULE_COMPLETED_1995.xml">1995</a>
<a href="/public/do/XMLViewFileAction?f=EO_RULE_COMPLETED_2025.xml">2025</a>
<a href="/public/do/XMLViewFileAction?f=EO_RULE_COMPLETED_YTD.xml">ytd</a>
<a href="/public/do/XMLViewFileAction?f=EO_RULE_COMPLETED_30_DAYS.xml">30 days</a>
<a href="/public/do/XMLViewFileAction?f=EO_RULES_UNDER_REVIEW.xml">pending</a>
<a href="/public/do/XMLViewFileAction?f=EO_12866_Meeting_Data_First_Half_2024.xml">m1</a>
<a href="/public/do/XMLViewFileAction?f=EO_12866_Meeting_Data_Second_Half_2024.xml">m2</a>
</body></html>
"""

_AGENCIES = b"""<?xml version="1.0" encoding="UTF-8"?>
<OIRA_DATA RUNDATE="2026-11-12">
<AGENCY><NAME>Office of Air and Radiation</NAME><ACRONYM>OAR</ACRONYM><AGENCY_CODE>2060</AGENCY_CODE></AGENCY>
<AGENCY><NAME>Agricultural Marketing Service</NAME><ACRONYM>AMS</ACRONYM><AGENCY_CODE>0581</AGENCY_CODE></AGENCY>
</OIRA_DATA>
"""

# Old files open directly on the root element, with no XML declaration.
_REVIEWS_1995 = b"""<OIRA_DATA
xmlns:xsi="http://www.w3.org/2000/10/XMLSchema-instance">
<REGACT>
<AGENCY_CODE>2060</AGENCY_CODE>
<RIN>2060-AA01</RIN>
<TITLE>An Old
  Rule </TITLE>
<STAGE>Final Rule</STAGE>
<ECONOMICALLY_SIGNIFICANT>No</ECONOMICALLY_SIGNIFICANT>
<DATE_RECEIVED>1994-12-01</DATE_RECEIVED>
<LEGAL_DEADLINE>None</LEGAL_DEADLINE>
<DATE_COMPLETED>1995-01-30</DATE_COMPLETED>
<DECISION>Consistent w/no change</DECISION>
<DATE_PUBLISHED>1995-02-15</DATE_PUBLISHED>
</REGACT>
</OIRA_DATA>
"""

_REVIEWS_2025 = b"""<?xml version="1.0" encoding="UTF-8"?>
<OIRA_DATA RUNDATE="2025-12-31">
<REGACT>
<AGENCY_CODE>9999</AGENCY_CODE>
<RIN>9999-AA01</RIN>
<TITLE>Unknown Agency Rule</TITLE>
<STAGE>Proposed Rule</STAGE>
<ECONOMICALLY_SIGNIFICANT>Yes</ECONOMICALLY_SIGNIFICANT>
<DATE_RECEIVED>2025-03-01</DATE_RECEIVED>
<LEGAL_DEADLINE>Statutory</LEGAL_DEADLINE>
<DATE_COMPLETED>2025-06-01</DATE_COMPLETED>
<DECISION>Consistent with Change</DECISION>
<DATE_PUBLISHED></DATE_PUBLISHED>
<MAJOR>Yes</MAJOR>
<UNFUNDED_MANDATES>No</UNFUNDED_MANDATES>
<TCJA>No</TCJA>
</REGACT>
</OIRA_DATA>
"""

# The first record concluded in 2025, which has its own file: it must be dropped.
_REVIEWS_YTD = b"""<?xml version="1.0" encoding="UTF-8"?>
<OIRA_DATA RUNDATE="2026-01-05">
<REGACT>
<AGENCY_CODE>9999</AGENCY_CODE>
<RIN>9999-AA01</RIN>
<TITLE>Unknown Agency Rule</TITLE>
<STAGE>Proposed Rule</STAGE>
<DATE_RECEIVED>2025-03-01</DATE_RECEIVED>
<DATE_COMPLETED>2025-06-01</DATE_COMPLETED>
<DECISION>Consistent with Change</DECISION>
</REGACT>
<REGACT>
<AGENCY_CODE>2060</AGENCY_CODE>
<RIN>2060-AV12</RIN>
<TITLE>A New Rule</TITLE>
<STAGE>Final Rule</STAGE>
<DATE_RECEIVED>2025-12-20</DATE_RECEIVED>
<DATE_COMPLETED>2026-01-02</DATE_COMPLETED>
<DECISION>Withdrawn</DECISION>
<MAJOR> </MAJOR>
</REGACT>
</OIRA_DATA>
"""

_REVIEWS_PENDING = b"""<?xml version="1.0" encoding="UTF-8"?>
<OIRA_DATA RUNDATE="2026-01-05">
<REGACT>
<AGENCY_CODE>2060</AGENCY_CODE>
<RIN>2060-AV99</RIN>
<TITLE>Pending Rule</TITLE>
<STAGE>Proposed Rule</STAGE>
<ECONOMICALLY_SIGNIFICANT>No</ECONOMICALLY_SIGNIFICANT>
<DATE_RECEIVED>2026-01-03</DATE_RECEIVED>
<LEGAL_DEADLINE>None</LEGAL_DEADLINE>
</REGACT>
</OIRA_DATA>
"""

# The first-half file has no MEETING_ID; text is double-escaped in both.
_MEETINGS_H1 = b"""<?xml version='1.0'  encoding='Cp1252' ?>
<RESULTS>
	<ROW>
		<RIN>0581-AE30</RIN>
		<TITLE>Fed Cattle</TITLE>
		<AGENCYSUBAGENCYACRONYM>0581-USDA/AMS</AGENCYSUBAGENCYACRONYM>
		<RULE_STAGE_DESC>Prerule Stage</RULE_STAGE_DESC>
		<MEETINGDATETIME>08-MAY-24</MEETINGDATETIME>
		<REQUESTOR>Ranchers &amp;#040;R-CALF&amp;#041; &amp;amp; Friends</REQUESTOR>
		<REQUESTOR_NAME>Jane  Doe</REQUESTOR_NAME>
		<TYPE_CD>C</TYPE_CD>
		<MEETING_TYPE>Completed</MEETING_TYPE>
		<SUMMARY></SUMMARY>
	</ROW>
	<ROW>
		<RIN>0581-AE30</RIN>
		<TITLE>Fed Cattle</TITLE>
		<AGENCYSUBAGENCYACRONYM>0581-USDA/AMS</AGENCYSUBAGENCYACRONYM>
		<RULE_STAGE_DESC>Prerule Stage</RULE_STAGE_DESC>
		<MEETINGDATETIME>08-MAY-24</MEETINGDATETIME>
		<REQUESTOR>Second Group</REQUESTOR>
		<REQUESTOR_NAME>John Roe</REQUESTOR_NAME>
		<TYPE_CD>C</TYPE_CD>
		<MEETING_TYPE>Completed</MEETING_TYPE>
		<SUMMARY></SUMMARY>
	</ROW>
</RESULTS>
"""

_MEETINGS_H2 = b"""<?xml version='1.0'  encoding='Cp1252' ?>
<RESULTS>
	<ROW>
		<RIN>1018-BH58</RIN>
		<MEETING_ID>516723</MEETING_ID>
		<TITLE>Critical Habitat</TITLE>
		<AGENCYSUBAGENCYACRONYM>1018-DOI/FWS</AGENCYSUBAGENCYACRONYM>
		<RULE_STAGE_DESC>Proposed Rule Stage</RULE_STAGE_DESC>
		<MEETINGDATETIME>11-JUL-24</MEETINGDATETIME>
		<REQUESTOR>Center for Biological Diversity</REQUESTOR>
		<REQUESTOR_NAME>Stephanie Kurose</REQUESTOR_NAME>
		<TYPE_CD>N</TYPE_CD>
		<MEETING_TYPE>No Show</MEETING_TYPE>
		<SUMMARY>Discussed habitat.</SUMMARY>
	</ROW>
</RESULTS>
"""

_HTML_ERROR = b"\n\n\n<html lang='en'><head><title>Error</title></head></html>"

_FILES = {
    "AGY_AGENCY_LIST.xml": _AGENCIES,
    "EO_RULE_COMPLETED_1995.xml": _REVIEWS_1995,
    "EO_RULE_COMPLETED_2025.xml": _REVIEWS_2025,
    "EO_RULE_COMPLETED_YTD.xml": _REVIEWS_YTD,
    "EO_RULES_UNDER_REVIEW.xml": _REVIEWS_PENDING,
    "EO_12866_Meeting_Data_First_Half_2024.xml": _MEETINGS_H1,
    "EO_12866_Meeting_Data_Second_Half_2024.xml": _MEETINGS_H2,
}


@pytest.fixture(autouse=True)
def _no_delay(monkeypatch):
    monkeypatch.setattr(oira, "_REQUEST_DELAY_S", 0)
    monkeypatch.setattr(oira.time, "sleep", lambda _s: None)


def _client(files: dict[str, bytes] | None = None, *, on_file: Callable[[str], httpx.Response] | None = None):
    files = _FILES if files is None else files
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/XMLReportList"):
            return httpx.Response(200, text=_REPORT_LIST)
        name = request.url.params["f"]
        requested.append(name)
        if on_file is not None:
            return on_file(name)
        # reginfo.gov answers a missing file with 200 + an HTML error page.
        return httpx.Response(200, content=files.get(name, _HTML_ERROR))

    return httpx.Client(transport=httpx.MockTransport(handler)), requested


# --------------------------------------------------------------------------- #
# File discovery
# --------------------------------------------------------------------------- #


def test_parse_report_list_and_select_files():
    listed = parse_report_list(_REPORT_LIST)
    assert "EO_RULE_COMPLETED_30_DAYS.xml" in listed
    # Per-year files oldest-first, then YTD, then pending; the 30-day subset is skipped.
    assert review_files(listed) == [
        "EO_RULE_COMPLETED_1995.xml",
        "EO_RULE_COMPLETED_2025.xml",
        "EO_RULE_COMPLETED_YTD.xml",
        "EO_RULES_UNDER_REVIEW.xml",
    ]
    assert meeting_files(listed) == [
        "EO_12866_Meeting_Data_First_Half_2024.xml",
        "EO_12866_Meeting_Data_Second_Half_2024.xml",
    ]


def test_select_files_raise_when_exports_vanish():
    with pytest.raises(OiraUnavailable):
        review_files(["EO_RULE_COMPLETED_2025.xml"])  # no YTD / pending
    with pytest.raises(OiraUnavailable):
        review_files(["EO_RULE_COMPLETED_YTD.xml", "EO_RULES_UNDER_REVIEW.xml"])  # no years
    with pytest.raises(OiraUnavailable):
        meeting_files(["EO_RULE_COMPLETED_YTD.xml"])


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (b'<?xml version="1.0"?><OIRA_DATA/>', True),
        (b"\n<OIRA_DATA\nxmlns:xsi='x'>", True),  # 1981-style, no declaration
        (_HTML_ERROR, False),
        (b"<!DOCTYPE html><html></html>", False),
        (b"", False),
    ],
)
def test_is_xml(body, expected):
    assert _is_xml(body) is expected


# --------------------------------------------------------------------------- #
# Readers
# --------------------------------------------------------------------------- #


def test_reviews_reader_parses_and_drops_ytd_overlap():
    client, requested = _client()
    reader = OiraReviewsReader(client=client)
    docs = list(reader.iter_records())

    assert [d["rin"] for d in docs] == ["2060-AA01", "9999-AA01", "2060-AV12", "2060-AV99"]
    # The YTD copy of 9999-AA01 (concluded 2025, which has its own file) is dropped.
    assert reader.file_counts["EO_RULE_COMPLETED_YTD.xml"] == 1
    assert "EO_RULE_COMPLETED_30_DAYS.xml" not in requested
    assert reader.agencies["2060"] == ("OAR", "Office of Air and Radiation")

    old, recent, ytd, pending = docs
    assert old["title"] == "An Old Rule"  # whitespace collapsed
    assert old["decision"] == "Consistent w/no change"
    assert old["flags"] == {}
    assert recent["flags"] == {"tcja": "No", "unfunded_mandates": "No"}
    assert recent["published_date"] is None  # empty tag
    assert ytd["major"] is None  # whitespace-only tag
    assert pending["under_review"] is True
    assert pending["concluded_date"] is None and pending["decision"] is None


def test_meetings_reader_handles_missing_id_and_entities():
    client, _ = _client()
    docs = list(OiraMeetingsReader(client=client).iter_records())

    assert len(docs) == 3  # two requestors for one meeting stay as two rows
    first = docs[0]
    assert first["meeting_id"] is None
    assert first["requestor_organization"] == "Ranchers (R-CALF) & Friends"
    assert first["requestor_name"] == "Jane Doe"
    assert first["meeting_date"] == "2024-05-08"
    assert (first["agency_number"], first["agency_acronym"]) == ("0581", "USDA/AMS")
    assert docs[2]["meeting_id"] == "516723"
    assert docs[2]["meeting_status"] == "No Show"


def test_missing_export_fails_the_whole_fetch():
    files = dict(_FILES)
    del files["EO_RULE_COMPLETED_2025.xml"]  # listed, but serves the HTML error page
    client, _ = _client(files)
    with pytest.raises(OiraUnavailable, match="EO_RULE_COMPLETED_2025.xml did not return XML"):
        list(OiraReviewsReader(client=client).iter_records())


def test_transient_errors_are_retried():
    attempts: dict[str, int] = {}

    def flaky(name: str) -> httpx.Response:
        attempts[name] = attempts.get(name, 0) + 1
        if attempts[name] == 1:
            return httpx.Response(503)
        return httpx.Response(200, content=_FILES.get(name, _HTML_ERROR))

    client, _ = _client(on_file=flaky)
    docs = list(OiraMeetingsReader(client=client).iter_records())
    assert len(docs) == 3
    assert all(n == 2 for n in attempts.values())


def test_agency_list_failure_is_not_fatal():
    files = dict(_FILES)
    del files["AGY_AGENCY_LIST.xml"]
    client, _ = _client(files)
    reader = OiraReviewsReader(client=client)
    assert len(list(reader.iter_records())) == 4
    assert reader.agencies == {}


# --------------------------------------------------------------------------- #
# Builds
# --------------------------------------------------------------------------- #


def test_build_oira_reviews(tmp_path):
    client, _ = _client()
    out = build_oira_reviews(tmp_path, reader=OiraReviewsReader(client=client))
    table = pq.read_table(out)
    assert tuple(table.column_names) == REVIEW_COLUMNS
    rows = {r["rin"]: r for r in table.to_pylist()}

    old = rows["2060-AA01"]
    assert (old["agency_code"], old["agency_name"]) == ("OAR", "Office of Air and Radiation")
    assert old["review_status"] == "Concluded"
    assert old["review_days"] == 60
    assert old["review_outcome"] == "Consistent w/no change"
    assert old["flags_json"] is None
    assert old["source_file"] == "EO_RULE_COMPLETED_1995.xml"

    unknown = rows["9999-AA01"]
    assert (unknown["agency_code"], unknown["agency_name"]) == ("9999", None)  # not in agency list
    assert json.loads(unknown["flags_json"]) == {"tcja": "No", "unfunded_mandates": "No"}

    pending = rows["2060-AV99"]
    assert pending["review_status"] == "Under Review"
    assert pending["review_days"] is None and pending["review_outcome"] is None

    # Newest received first.
    assert table.column("received_date").to_pylist()[0] == "2026-01-03"


def test_build_oira_meetings(tmp_path):
    client, _ = _client()
    out = build_oira_meetings(tmp_path, reader=OiraMeetingsReader(client=client))
    table = pq.read_table(out)
    assert tuple(table.column_names) == MEETING_COLUMNS
    rows = table.to_pylist()
    assert len(rows) == 3

    fws = next(r for r in rows if r["rin"] == "1018-BH58")
    # FWS is not in the fixture agency list: fall back to the acronym's last segment.
    assert (fws["agency_code"], fws["agency_name"]) == ("FWS", None)
    assert fws["url"] == (
        "https://www.reginfo.gov/public/do/viewEO12866Meeting?viewRule=true&rin=1018-BH58&meetingId=516723"
    )
    ams = [r for r in rows if r["rin"] == "0581-AE30"]
    assert {r["agency_code"] for r in ams} == {"AMS"}
    assert all(r["url"] is None for r in ams)  # no meeting_id in the first-half file


def test_build_raises_when_nothing_fetched(tmp_path):
    empty = b'<?xml version="1.0"?><OIRA_DATA></OIRA_DATA>'
    files = {name: (empty if name.startswith("EO_RULE") else body) for name, body in _FILES.items()}
    client, _ = _client(files)
    with pytest.raises(OiraUnavailable, match="no EO 12866 review records"):
        build_oira_reviews(tmp_path, reader=OiraReviewsReader(client=client))
    assert not (tmp_path / "oira_reviews.parquet").exists()
