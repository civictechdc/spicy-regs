"""Hermetic tests for the pinned document-population captures (no network).

Every assertion here runs against the exact publisher bytes under
`sample-data/document-populations/`: the manifest's digests are re-derived from
those bytes, and each parser is driven over them so the counts, identities, and
publisher-declared digests this repository states are the ones the captures
actually carry.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from spicy_regs.sources.document_populations import (
    CBO_ITEM_TAGS,
    CBO_PUBLICATION_COLUMNS,
    DEFAULT_CAPTURE_MANIFEST_PATH,
    DocumentPopulationError,
    cbo_per_congress_feed_url,
    govinfo_package_premis_url,
    govinfo_package_summary_url,
    is_bot_challenge,
    load_capture_manifest,
    parse_cbo_publication_feed,
    parse_govinfo_cfr_package_summary,
    parse_govinfo_package_fixity,
    read_capture,
)
from spicy_regs.sources.fcc_ecfs import proceedings_from_filings
from spicy_regs.transforms.build_fcc_ecfs import PROCEEDING_COLUMNS, _shape_proceeding

CAPTURE_ROOT = DEFAULT_CAPTURE_MANIFEST_PATH.parent


@pytest.fixture(scope="module")
def captures() -> dict:
    return load_capture_manifest()


def _payload(captures: dict, key: str) -> bytes:
    return read_capture(captures[key], root=CAPTURE_ROOT)


# --- the pins themselves ----------------------------------------------------


def test_every_capture_hashes_to_the_digest_the_manifest_pins(captures: dict) -> None:
    assert set(captures) == {
        "cbo-119th-congress-publications",
        "cbo-cost-estimates-datadome-challenge",
        "fcc-ecfs-proceedings",
        "govinfo-cfr-package-fixity",
        "govinfo-cfr-package-summary",
    }
    for capture in captures.values():
        payload = (CAPTURE_ROOT / capture.path).read_bytes()
        assert len(payload) == capture.byte_length, capture.key
        assert "sha256:" + hashlib.sha256(payload).hexdigest() == capture.bytes_digest, capture.key
        # read_capture is the guard the parsers go through; prove it agrees.
        assert read_capture(capture, root=CAPTURE_ROOT) == payload


def test_read_capture_refuses_bytes_that_are_not_the_pinned_bytes(captures: dict, tmp_path: Path) -> None:
    capture = captures["govinfo-cfr-package-summary"]
    payload = read_capture(capture, root=CAPTURE_ROOT)

    (tmp_path / capture.path).write_bytes(payload.replace(b"General Provisions", b"General Provisionz"))
    with pytest.raises(DocumentPopulationError, match="hashes to"):
        read_capture(capture, root=tmp_path)

    (tmp_path / capture.path).write_bytes(payload + b"\n")
    with pytest.raises(DocumentPopulationError, match="bytes, pinned at"):
        read_capture(capture, root=tmp_path)


def test_the_manifest_carries_the_publisher_url_each_capture_came_from(captures: dict) -> None:
    assert captures["cbo-119th-congress-publications"].source_url == cbo_per_congress_feed_url(119)
    assert captures["govinfo-cfr-package-summary"].source_url == govinfo_package_summary_url("CFR-2023-title1-vol1")
    assert captures["govinfo-cfr-package-fixity"].source_url == govinfo_package_premis_url("CFR-2023-title1-vol1")
    assert captures["fcc-ecfs-proceedings"].source_url == (
        "https://publicapi.fcc.gov/ecfs/filings?limit=25&sort=date_disseminated,DESC"
    )


# --- CBO --------------------------------------------------------------------


def test_the_cbo_capture_enumerates_1058_publications(captures: dict) -> None:
    rows = parse_cbo_publication_feed(_payload(captures, "cbo-119th-congress-publications"))

    assert len(rows) == 1058
    assert len(rows) == captures["cbo-119th-congress-publications"].record_count
    assert all(set(row) == set(CBO_PUBLICATION_COLUMNS) for row in rows)
    # The publication URL is the document identity; 1,058 documents, 1,058 URLs.
    assert len({row["publication_url"] for row in rows}) == 1058
    assert all(row["publication_url"].endswith("/" + row["publication_id"]) for row in rows)
    assert "https://www.cbo.gov/publication/61150" in {row["publication_url"] for row in rows}


def test_the_first_cbo_publication_is_exactly_what_the_feed_says(captures: dict) -> None:
    rows = parse_cbo_publication_feed(_payload(captures, "cbo-119th-congress-publications"))

    assert rows[0] == {
        "publication_id": "62634",
        "publication_url": "https://www.cbo.gov/publication/62634",
        "title": ("H.R. 8844, U.S. Customs and Border Protection Officer Retirement Technical Corrections Act"),
        "published": "Mon, 03 Aug 2026 15:49:00 -0400",
        "description": (
            "As ordered reported by the House Committee on Oversight and Government Reform on May 20, 2026"
        ),
        "bill_number": "H.R. 8844",
        "feed_item_key": "0",
    }


def test_cbo_leaves_a_bill_number_empty_without_that_being_drift(captures: dict) -> None:
    rows = parse_cbo_publication_feed(_payload(captures, "cbo-119th-congress-publications"))

    # Procedural items (weekly House suspension-calendar notices and the like)
    # carry an empty <Bill_Number/>; 52 of the 1,058 do.
    assert sum(1 for row in rows if row["bill_number"] is None) == 52


def test_the_cbo_bot_challenge_body_is_refused_instead_of_read_as_an_empty_feed(captures: dict) -> None:
    payload = _payload(captures, "cbo-cost-estimates-datadome-challenge")

    assert is_bot_challenge(payload)
    with pytest.raises(DocumentPopulationError, match="bot-challenge"):
        parse_cbo_publication_feed(payload)


def test_the_cbo_parser_refuses_a_drifted_item_shape() -> None:
    feed = (
        b'<?xml version="1.0"?><response><item key="0">'
        + b"".join(f"<{tag}>x</{tag}>".encode() for tag in CBO_ITEM_TAGS if tag != "Description")
        + b"</item></response>"
    )
    with pytest.raises(DocumentPopulationError, match="carries"):
        parse_cbo_publication_feed(feed)

    duplicated = (
        b'<?xml version="1.0"?><response>'
        + 2
        * (
            b'<item key="0"><Title>t</Title><Date>d</Date>'
            b"<Link>https://www.cbo.gov/publication/1</Link>"
            b"<Description>x</Description><Bill_Number/></item>"
        )
        + b"</response>"
    )
    with pytest.raises(DocumentPopulationError, match="repeats publication"):
        parse_cbo_publication_feed(duplicated)


def test_the_xml_parsers_refuse_a_doctype() -> None:
    with pytest.raises(DocumentPopulationError, match="DOCTYPE"):
        parse_cbo_publication_feed(b'<?xml version="1.0"?><!DOCTYPE response []><response></response>')


# --- FCC ECFS ---------------------------------------------------------------


def test_the_ecfs_capture_names_15_proceedings_across_25_filings(captures: dict) -> None:
    payload = json.loads(_payload(captures, "fcc-ecfs-proceedings"))

    assert len(payload["filing"]) == 25
    assert sum(len(filing["proceedings"]) for filing in payload["filing"]) == 40

    proceedings = proceedings_from_filings(payload)
    assert len(proceedings) == 15
    assert len(proceedings) == captures["fcc-ecfs-proceedings"].record_count
    assert [item["name"] for item in proceedings] == [
        "02-6",
        "03-123",
        "10-90",
        "13-184",
        "21-455",
        "21-479",
        "21-93",
        "25-143",
        "26-124",
        "26-131",
        "26-133",
        "26-184",
        "26-189",
        "26-96",
        "26-97",
    ]


def test_captured_proceedings_shape_onto_the_published_proceeding_columns(captures: dict) -> None:
    payload = json.loads(_payload(captures, "fcc-ecfs-proceedings"))

    rows = [_shape_proceeding(item) for item in proceedings_from_filings(payload)]

    assert all(set(row) == set(PROCEEDING_COLUMNS) for row in rows)
    by_name = {row["name"]: row for row in rows}
    assert by_name["26-189"] == {
        "name": "26-189",
        "id_proceeding": "1784669453334",
        "description": (
            "Prohibiting the Importation and Marketing of Certain Foreign-Produced "
            "Military-Grade UAS and UAS Critical Components"
        ),
        "bureau_code": "PSHSB",
        "bureau_name": "Public Safety & Homeland Security Bureau",
        "rulemaking_or_docket": None,
        "filing_status": None,
        "date_created": None,
        "date_closed": None,
        "comment_start_date": None,
        "comment_end_date": None,
        "reply_comment_start_date": None,
        "reply_comment_end_date": None,
        "filed_by": None,
    }
    # An embedded proceeding states its creation date under `created_date`,
    # where the /proceedings endpoint uses `date_proceeding_created`. The
    # published column stays empty rather than borrowing the other spelling.
    assert by_name["26-189"]["date_created"] is None


def test_a_proceeding_embedded_with_a_conflicting_identity_is_drift() -> None:
    payload = {
        "filing": [
            {"proceedings": [{"name": "17-108", "id_proceeding": 1, "description": "a", "bureau_code": "WC"}]},
            {"proceedings": [{"name": "17-108", "id_proceeding": 2, "description": "a", "bureau_code": "WC"}]},
        ]
    }
    with pytest.raises(ValueError, match="conflicting identity"):
        proceedings_from_filings(payload)

    with pytest.raises(ValueError, match="names no proceedings"):
        proceedings_from_filings({"filing": [{"proceedings": []}]})


# --- GovInfo ----------------------------------------------------------------


def test_the_govinfo_capture_is_one_cfr_package(captures: dict) -> None:
    summary = parse_govinfo_cfr_package_summary(_payload(captures, "govinfo-cfr-package-summary"))

    assert summary["package_id"] == "CFR-2023-title1-vol1"
    assert summary["collection_code"] == "CFR"
    assert summary["collection_name"] == "Code of Federal Regulations (annual edition)"
    assert summary["title"] == "General Provisions"
    assert summary["title_number"] == "1"
    assert (summary["part_from"], summary["part_to"]) == ("1", "603")
    assert summary["date_issued"] == "2023-01-01"
    assert summary["last_modified"] == "2025-05-21T06:24:19Z"
    assert summary["sudoc_class_number"] == "AE 2.106/3:1/"
    assert summary["pages"] == "175"
    assert summary["details_url"] == "https://www.govinfo.gov/app/details/CFR-2023-title1-vol1"
    assert json.loads(summary["download_urls_json"])["xmlLink"] == (
        "https://api.govinfo.gov/packages/CFR-2023-title1-vol1/xml"
    )


def test_the_premis_record_carries_the_publishers_own_sha256_for_each_rendition(captures: dict) -> None:
    rows = parse_govinfo_package_fixity(
        _payload(captures, "govinfo-cfr-package-fixity"), package_id="CFR-2023-title1-vol1"
    )

    assert len(rows) == 2
    assert len(rows) == captures["govinfo-cfr-package-fixity"].record_count
    by_name = {row["original_name"]: row for row in rows}
    assert by_name["CFR-2023-title1-vol1.xml"] == {
        "package_id": "CFR-2023-title1-vol1",
        "object_identifier": "D09002ee1c7456569",
        "original_name": "CFR-2023-title1-vol1.xml",
        "media_type": "text/xml",
        "byte_length": 814758,
        "bytes_digest": "sha256:933d9cf35c4342d4d55cd4dc771a73b46a0c8f88423b935da470ad5862d13e0d",
        "content_url": "https://www.govinfo.gov/content/pkg/CFR-2023-title1-vol1/xml/CFR-2023-title1-vol1.xml",
    }
    assert by_name["CFR-2023-title1-vol1.htm"]["bytes_digest"] == (
        "sha256:7321767f07828dc822e81e9806a33280cb2860d4281ac91f1bc79439b1cfcb33"
    )
    assert by_name["CFR-2023-title1-vol1.htm"]["byte_length"] == 572151


def test_fixity_belonging_to_another_package_is_refused(captures: dict) -> None:
    payload = _payload(captures, "govinfo-cfr-package-fixity")

    with pytest.raises(DocumentPopulationError, match="does not belong to"):
        parse_govinfo_package_fixity(payload, package_id="CFR-2024-title1-vol1")


def test_a_non_cfr_package_summary_is_refused(captures: dict) -> None:
    summary = json.loads(_payload(captures, "govinfo-cfr-package-summary"))
    summary["collectionCode"] = "BILLS"

    with pytest.raises(DocumentPopulationError, match="collectionCode"):
        parse_govinfo_cfr_package_summary(json.dumps(summary).encode("utf-8"))
