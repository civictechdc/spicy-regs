"""Concrete `RecordType` instances for the regulations.gov data shapes.

Each entry pairs a name, S3 path pattern, Parquet schema, dedup key, and an
extract function that maps a raw regulations.gov JSON payload to a flat record
dict. The pipeline addresses these by their dict key (``"dockets"`` etc.); the
key matches ``RecordType.name`` so staging paths and merge logic stay stable.
"""

from json import dumps as json_dumps

import polars as pl

from spicy_regs.schemas.base import RecordType


def _extract_comment(d: dict) -> dict:
    attrs = d.get("data", {}).get("attributes", {})

    # Build compact attachments JSON from the included array
    attachments = []
    for inc in d.get("included", []):
        if inc.get("type") == "attachments":
            inc_attrs = inc.get("attributes", {})
            formats = [
                {"url": f["fileUrl"], "format": f.get("format"), "size": f.get("size")}
                for f in inc_attrs.get("fileFormats") or []
                if f.get("fileUrl")
            ]
            if formats:
                attachments.append({"title": inc_attrs.get("title", ""), "formats": formats})

    return {
        "comment_id": d.get("data", {}).get("id"),
        "docket_id": (v.strip('"') if (v := attrs.get("docketId")) else v),
        "agency_code": attrs.get("agencyId"),
        "first_name": attrs.get("firstName"),
        "last_name": attrs.get("lastName"),
        "organization": attrs.get("organization"),
        "category": attrs.get("category"),
        "title": attrs.get("title"),
        "comment": attrs.get("comment"),
        "document_type": attrs.get("documentType"),
        "posted_date": attrs.get("postedDate"),
        "modify_date": attrs.get("modifyDate"),
        "receive_date": attrs.get("receiveDate"),
        "attachments_json": json_dumps(attachments) if attachments else None,
    }


DOCKET = RecordType(
    name="dockets",
    path_pattern="/docket/",
    schema={
        "docket_id": pl.Utf8,
        "agency_code": pl.Utf8,
        "title": pl.Utf8,
        "docket_type": pl.Utf8,
        "modify_date": pl.Utf8,
        "abstract": pl.Utf8,
    },
    dedup_key="docket_id",
    extract=lambda d: {
        "docket_id": (v.strip('"') if (v := d.get("data", {}).get("id")) else v),
        "agency_code": d.get("data", {}).get("attributes", {}).get("agencyId"),
        "title": d.get("data", {}).get("attributes", {}).get("title"),
        "docket_type": d.get("data", {}).get("attributes", {}).get("docketType"),
        "modify_date": d.get("data", {}).get("attributes", {}).get("modifyDate"),
        "abstract": d.get("data", {}).get("attributes", {}).get("dkAbstract"),
    },
)


DOCUMENT = RecordType(
    name="documents",
    path_pattern="/documents/",
    schema={
        "document_id": pl.Utf8,
        "docket_id": pl.Utf8,
        "agency_code": pl.Utf8,
        "title": pl.Utf8,
        "document_type": pl.Utf8,
        "posted_date": pl.Utf8,
        "modify_date": pl.Utf8,
        "comment_start_date": pl.Utf8,
        "comment_end_date": pl.Utf8,
        "file_url": pl.Utf8,
        "withdrawn": pl.Utf8,
        "reason_withdrawn": pl.Utf8,
    },
    dedup_key="document_id",
    extract=lambda d: {
        "document_id": d.get("data", {}).get("id"),
        "docket_id": (v.strip('"') if (v := d.get("data", {}).get("attributes", {}).get("docketId")) else v),
        "agency_code": d.get("data", {}).get("attributes", {}).get("agencyId"),
        "title": d.get("data", {}).get("attributes", {}).get("title"),
        "document_type": d.get("data", {}).get("attributes", {}).get("documentType"),
        "posted_date": d.get("data", {}).get("attributes", {}).get("postedDate"),
        "modify_date": d.get("data", {}).get("attributes", {}).get("modifyDate"),
        "comment_start_date": d.get("data", {}).get("attributes", {}).get("commentStartDate"),
        "comment_end_date": d.get("data", {}).get("attributes", {}).get("commentEndDate"),
        "file_url": (d.get("data", {}).get("attributes", {}).get("fileFormats") or [{}])[0].get("fileUrl"),
        "withdrawn": d.get("data", {}).get("attributes", {}).get("withdrawn"),
        "reason_withdrawn": d.get("data", {}).get("attributes", {}).get("reasonWithdrawn"),
    },
)


COMMENT = RecordType(
    name="comments",
    path_pattern="/comments/",
    schema={
        "comment_id": pl.Utf8,
        "docket_id": pl.Utf8,
        "agency_code": pl.Utf8,
        "first_name": pl.Utf8,
        "last_name": pl.Utf8,
        "organization": pl.Utf8,
        "category": pl.Utf8,
        "title": pl.Utf8,
        "comment": pl.Utf8,
        "document_type": pl.Utf8,
        "posted_date": pl.Utf8,
        "modify_date": pl.Utf8,
        "receive_date": pl.Utf8,
        "attachments_json": pl.Utf8,
    },
    dedup_key="comment_id",
    extract=_extract_comment,
)


# Registry keyed by record-type name. Order matters: it drives the default
# set of data types the pipeline processes.
RECORD_TYPES: dict[str, RecordType] = {
    "dockets": DOCKET,
    "documents": DOCUMENT,
    "comments": COMMENT,
}
