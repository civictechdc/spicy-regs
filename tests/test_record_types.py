"""Tests for the RecordType registry in spicy_regs.schemas.regulations."""

from json import loads

from spicy_regs.schemas import COMMENT, DOCKET, DOCUMENT, RECORD_TYPES, RecordType


def test_registry_keys_and_names_match() -> None:
    assert list(RECORD_TYPES.keys()) == ["dockets", "documents", "comments"]
    for key, rt in RECORD_TYPES.items():
        assert isinstance(rt, RecordType)
        assert rt.name == key


def test_each_record_type_satisfies_invariants() -> None:
    # __post_init__ already enforces these at construction; assert explicitly
    # so the contract is documented and regressions are caught here.
    for rt in RECORD_TYPES.values():
        assert rt.dedup_key in rt.schema
        assert "modify_date" in rt.schema


def test_docket_extract() -> None:
    raw = {
        "data": {
            "id": '"EPA-2024-0001"',
            "attributes": {
                "agencyId": "EPA",
                "title": "Clean Air Standards",
                "docketType": "Rulemaking",
                "modifyDate": "2024-06-15",
                "dkAbstract": "Proposed rule",
            },
        }
    }
    assert DOCKET.extract(raw) == {
        "docket_id": "EPA-2024-0001",  # surrounding quotes stripped
        "agency_code": "EPA",
        "title": "Clean Air Standards",
        "docket_type": "Rulemaking",
        "modify_date": "2024-06-15",
        "abstract": "Proposed rule",
    }


def test_document_extract_takes_first_file_url() -> None:
    raw = {
        "data": {
            "id": "EPA-2024-0001-0002",
            "attributes": {
                "docketId": '"EPA-2024-0001"',
                "agencyId": "EPA",
                "title": "Proposed Rule",
                "documentType": "Proposed Rule",
                "postedDate": "2024-06-01",
                "modifyDate": "2024-06-01",
                "commentStartDate": "2024-06-01",
                "commentEndDate": "2024-07-01",
                "fileFormats": [
                    {"fileUrl": "https://example.gov/a.pdf"},
                    {"fileUrl": "https://example.gov/b.pdf"},
                ],
                "withdrawn": True,
                "reasonWithdrawn": "Superseded by revised proposal",
            },
        }
    }
    out = DOCUMENT.extract(raw)
    assert out["document_id"] == "EPA-2024-0001-0002"
    assert out["docket_id"] == "EPA-2024-0001"
    assert out["file_url"] == "https://example.gov/a.pdf"
    assert out["withdrawn"] is True
    assert out["reason_withdrawn"] == "Superseded by revised proposal"


def test_document_extract_handles_missing_file_formats() -> None:
    raw = {"data": {"id": "X-1", "attributes": {}}}
    assert DOCUMENT.extract(raw)["file_url"] is None


def test_document_extract_missing_withdrawal_fields_are_none() -> None:
    # withdrawn/reason_withdrawn come from the source attributes but are absent
    # on most documents; they should surface as None, not KeyError.
    raw = {"data": {"id": "X-1", "attributes": {}}}
    out = DOCUMENT.extract(raw)
    assert out["withdrawn"] is None
    assert out["reason_withdrawn"] is None


def test_comment_extract_packs_attachments_json() -> None:
    raw = {
        "data": {
            "id": "EPA-2024-0001-0050",
            "attributes": {
                "docketId": '"EPA-2024-0001"',
                "agencyId": "EPA",
                "firstName": "Ada",
                "lastName": "Lovelace",
                "organization": "Analytical Society",
                "category": "Individual",
                "title": "Support",
                "comment": "I support this",
                "documentType": "Public Comment",
                "postedDate": "2024-06-20",
                "modifyDate": "2024-06-20",
                "receiveDate": "2024-06-20",
            },
        },
        "included": [
            {
                "type": "attachments",
                "attributes": {
                    "title": "Exhibit A",
                    "fileFormats": [
                        {"fileUrl": "https://example.gov/x.pdf", "format": "pdf", "size": 123},
                        {"format": "pdf"},  # no fileUrl -> skipped
                    ],
                },
            },
            {"type": "other", "attributes": {}},  # non-attachment -> ignored
        ],
    }
    out = COMMENT.extract(raw)
    assert out["comment_id"] == "EPA-2024-0001-0050"
    assert out["docket_id"] == "EPA-2024-0001"
    assert out["first_name"] == "Ada"
    assert out["last_name"] == "Lovelace"
    assert out["organization"] == "Analytical Society"
    assert out["category"] == "Individual"
    attachments = loads(out["attachments_json"])
    assert attachments == [
        {
            "title": "Exhibit A",
            "formats": [{"url": "https://example.gov/x.pdf", "format": "pdf", "size": 123}],
        }
    ]


def test_comment_extract_no_attachments_is_none() -> None:
    raw = {"data": {"id": "C-1", "attributes": {"docketId": "D-1"}}}
    assert COMMENT.extract(raw)["attachments_json"] is None


def test_comment_extract_missing_submitter_fields_are_none() -> None:
    # Submitter fields are recovered from the source attributes but are often
    # absent (e.g. anonymous submissions); they should surface as None, not KeyError.
    raw = {"data": {"id": "C-1", "attributes": {"docketId": "D-1"}}}
    out = COMMENT.extract(raw)
    assert out["first_name"] is None
    assert out["last_name"] is None
    assert out["organization"] is None
    assert out["category"] is None
