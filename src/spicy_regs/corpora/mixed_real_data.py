"""Build and validate a heterogeneous real-data ontology test corpus.

The corpus is intentionally broader than the production rulemaking materializer:
it samples 18 record-bearing source, aggregate, and relationship tables,
preserves each table's native schema, creates a small ontology-neutral record
envelope, and emits explicit relationship expectations. Positive expectations
use only source-declared keys. Negative controls mean "no declared relation in
this bound snapshot"; they are not claims that two public records can never be
related in the real world.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import duckdb
import pyarrow.parquet as pq

from spicy_regs.ontology.common import (
    canonical_json,
    read_parquet_rows,
    write_parquet_rows,
)
from spicy_regs.ontology.concepts import latest_assignments
from spicy_regs.ontology.evaluation import evaluate_tag_quality
from spicy_regs.ontology.ledger import FINAL_STATUSES
from spicy_regs.ontology.llm import SUPPORTED_REASONING_EFFORTS
from spicy_regs.ontology.subjects import (
    SUBJECT_PROFILES,
    subjects_by_segment_id,
)

CORPUS_VERSION = 2
SELECTION_SEED = "mixed-real-data-v1"
DEFAULT_BASE_URL = "https://r2.spicy-regs.dev"
EXPECTED_SOURCE_TABLES = (
    "dockets",
    "documents",
    "comments",
    "comments_index",
    "federal_register",
    "unified_agenda",
    "fr_docket_links",
    "cfr_sections",
    "congress_bills",
    "sam_entities",
    "lobbying_filings",
    "fec_committees",
    "gao_reports",
    "crs_reports",
    "court_dockets",
    "court_opinions",
    "usaspending_recipients",
    "fcc_proceedings",
    "fcc_filings",
)
REGULATORY_LOCAL_TABLES = frozenset(
    {
        "dockets",
        "documents",
        "federal_register",
        "unified_agenda",
        "fr_docket_links",
    }
)
COMMENT_AGENCIES = ("ACF", "USCG")

RECORD_COLUMNS = (
    "record_id",
    "source_table",
    "source_family",
    "ontology_kind",
    "native_id",
    "title",
    "summary",
    "record_date",
    "source_url",
)
EXPECTATION_COLUMNS = (
    "expectation_id",
    "left_record_id",
    "left_source",
    "right_record_id",
    "right_source",
    "label",
    "relation_kind",
    "evidence_basis",
    "evidence_value",
    "evidence_strength",
)
MEMBERSHIP_COLUMNS = (
    "record_id",
    "source_table",
    "sample_role",
    "related_expectation_count",
    "control_expectation_count",
    "unknown_expectation_count",
)


@dataclass(frozen=True)
class SourceSpec:
    name: str
    family: str
    ontology_kind: str
    primary_key: tuple[str, ...]
    title_columns: tuple[str, ...]
    summary_columns: tuple[str, ...]
    date_columns: tuple[str, ...]
    url_columns: tuple[str, ...]
    target_rows: int


SOURCE_SPECS = (
    SourceSpec(
        "dockets",
        "regulations.gov",
        "domain_subject",
        ("docket_id",),
        ("title",),
        ("abstract",),
        ("modify_date",),
        (),
        50_000,
    ),
    SourceSpec(
        "documents",
        "regulations.gov",
        "document_artifact",
        ("document_id",),
        ("title",),
        ("text_content",),
        ("posted_date", "modify_date"),
        ("file_url",),
        100_000,
    ),
    SourceSpec(
        "comments",
        "regulations.gov",
        "document_artifact",
        ("comment_id",),
        ("title",),
        ("comment", "text_content"),
        ("posted_date", "receive_date", "modify_date"),
        (),
        50_000,
    ),
    SourceSpec(
        "comments_index",
        "spicy-regs",
        "aggregate_record",
        ("agency_code", "docket_id", "year", "month"),
        ("docket_id",),
        (),
        (),
        (),
        30_000,
    ),
    SourceSpec(
        "federal_register",
        "federalregister.gov",
        "document_artifact",
        ("document_number",),
        ("title",),
        ("abstract",),
        ("publication_date", "modify_date"),
        ("html_url", "pdf_url"),
        80_000,
    ),
    SourceSpec(
        "unified_agenda",
        "reginfo.gov",
        "editioned_record",
        ("rin", "agenda_edition"),
        ("title",),
        ("abstract",),
        ("agenda_edition",),
        ("url",),
        10_000,
    ),
    SourceSpec(
        "fr_docket_links",
        "spicy-regs",
        "relationship_record",
        ("document_number", "docket_id"),
        ("title",),
        ("abstract",),
        ("publication_date",),
        ("html_url", "pdf_url"),
        80_000,
    ),
    SourceSpec(
        "cfr_sections",
        "govinfo.gov",
        "document_fragment",
        ("granule_id",),
        ("heading",),
        ("cfr_ref",),
        ("edition_year", "last_modified"),
        ("url",),
        40_000,
    ),
    SourceSpec(
        "congress_bills",
        "congress.gov",
        "legislative_work",
        ("bill_id",),
        ("title",),
        ("latest_action_text",),
        ("latest_action_date", "update_date"),
        ("url",),
        40_000,
    ),
    SourceSpec(
        "sam_entities",
        "sam.gov",
        "organization",
        ("uei",),
        ("legal_business_name", "dba_name"),
        ("entity_type_desc", "purpose_of_registration_desc"),
        ("registration_date", "registration_expiration_date"),
        ("entity_url",),
        50_000,
    ),
    SourceSpec(
        "lobbying_filings",
        "lda.senate.gov",
        "document_artifact",
        ("filing_uuid",),
        ("client_name", "registrant_name"),
        ("lobbying_activities_json",),
        ("dt_posted", "filing_year"),
        ("url",),
        40_000,
    ),
    SourceSpec(
        "fec_committees",
        "fec.gov",
        "organization",
        ("committee_id",),
        ("name",),
        ("committee_type_full", "organization_type_full"),
        ("last_file_date", "first_file_date"),
        (),
        30_000,
    ),
    SourceSpec(
        "gao_reports",
        "gao.gov",
        "document_artifact",
        ("report_id",),
        ("title",),
        ("abstract",),
        ("published_date",),
        ("url",),
        10_000,
    ),
    SourceSpec(
        "crs_reports",
        "crsreports.congress.gov",
        "document_artifact",
        ("report_id",),
        ("title",),
        ("report_type", "status"),
        ("update_date", "published_date"),
        ("url",),
        20_000,
    ),
    SourceSpec(
        "court_dockets",
        "courtlistener.com",
        "domain_subject",
        ("cl_docket_id",),
        ("case_name_full", "case_name"),
        ("nature_of_suit", "cause"),
        ("date_filed", "date_created"),
        ("absolute_url",),
        20_000,
    ),
    SourceSpec(
        "court_opinions",
        "supremecourt.gov",
        "document_artifact",
        ("opinion_id",),
        ("case_name",),
        ("holding", "pdf_text"),
        ("date_decided",),
        ("source_url",),
        10_000,
    ),
    SourceSpec(
        "usaspending_recipients",
        "usaspending.gov",
        "organization_record",
        ("recipient_id",),
        ("name",),
        ("recipient_level", "total_award_amount"),
        (),
        (),
        50_000,
    ),
    SourceSpec(
        "fcc_proceedings",
        "fcc.gov/ecfs",
        "domain_subject",
        ("id_proceeding",),
        ("name",),
        ("description",),
        ("date_created", "date_closed"),
        (),
        30_000,
    ),
    SourceSpec(
        "fcc_filings",
        "fcc.gov/ecfs",
        "document_artifact",
        ("id_submission",),
        ("submission_type",),
        ("text_data", "express_comment"),
        ("date_submission", "date_received"),
        ("filing_url",),
        30_000,
    ),
)
SPEC_BY_NAME = {spec.name: spec for spec in SOURCE_SPECS}


@dataclass(frozen=True)
class PairExpectation:
    left_record_id: str
    left_source: str
    right_record_id: str
    right_source: str
    label: str
    relation_kind: str
    evidence_basis: str
    evidence_value: str | None
    evidence_strength: str

    def as_row(self) -> dict[str, str | None]:
        identity = "|".join(
            (
                self.label,
                self.relation_kind,
                self.left_record_id,
                self.right_record_id,
            )
        )
        return {
            "expectation_id": f"expectation_{hashlib.sha256(identity.encode()).hexdigest()[:24]}",
            **asdict(self),
        }


def _sql_string(value: str | Path) -> str:
    return str(value).replace("'", "''")


def _parquet_source(value: str | list[str]) -> str:
    if isinstance(value, list):
        items = ", ".join(f"'{_sql_string(item)}'" for item in value)
        return f"[{items}]"
    return f"'{_sql_string(value)}'"


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _coalesce(columns: Sequence[str]) -> str:
    if not columns:
        return "NULL"
    return "coalesce(" + ", ".join(f"nullif(trim(cast({_quoted(column)} AS VARCHAR)), '')" for column in columns) + ")"


def _native_id_expr(spec: SourceSpec, *, alias: str | None = None) -> str:
    prefix = f"{alias}." if alias else ""
    parts = [
        f"coalesce(cast({prefix}{_quoted(column)} AS VARCHAR), '<null>')"
        for column in spec.primary_key
    ]
    return " || '|' || ".join(parts)


def record_id(source_table: str, native_id: object) -> str:
    identity = f"{source_table}:{native_id}"
    return f"record_{hashlib.sha256(identity.encode()).hexdigest()[:24]}"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _remote_metadata(url: str) -> dict[str, Any]:
    try:
        with urlopen(Request(url, method="HEAD"), timeout=30) as response:
            return {
                "url": url,
                "etag": response.headers.get("etag"),
                "last_modified": response.headers.get("last-modified"),
                "content_length": int(response.headers.get("content-length") or 0),
            }
    except (HTTPError, URLError, TimeoutError):
        return {
            "url": url,
            "etag": None,
            "last_modified": None,
            "content_length": None,
        }


def _source_uris(
    *,
    base_url: str,
    regulatory_source_dir: Path | None,
) -> dict[str, str | list[str]]:
    result: dict[str, str | list[str]] = {}
    for spec in SOURCE_SPECS:
        local = (
            regulatory_source_dir / f"{spec.name}.parquet"
            if regulatory_source_dir is not None and spec.name in REGULATORY_LOCAL_TABLES
            else None
        )
        if local is not None and local.exists():
            result[spec.name] = str(local.resolve())
        elif spec.name == "comments":
            result[spec.name] = [
                f"{base_url.rstrip('/')}/comments/agency/agency_code={agency}/part-0.parquet"
                for agency in COMMENT_AGENCIES
            ]
        else:
            result[spec.name] = f"{base_url.rstrip('/')}/{spec.name}.parquet"
    return result


def _sample_source(
    con: duckdb.DuckDBPyConnection,
    *,
    spec: SourceSpec,
    source: str | list[str],
    target: Path,
) -> tuple[int, int, list[dict[str, str]]]:
    con.execute(
        "CREATE OR REPLACE TEMP VIEW corpus_source AS "
        f"SELECT * FROM read_parquet({_parquet_source(source)}, union_by_name=true)"
    )
    source_rows = int(_scalar(con, "SELECT count(*) FROM corpus_source"))
    schema = [
        {"name": str(row[0]), "type": str(row[1])}
        for row in con.execute("DESCRIBE SELECT * FROM corpus_source").fetchall()
    ]
    actual_columns = {column["name"] for column in schema}
    missing = sorted(set(spec.primary_key) - actual_columns)
    if missing:
        raise RuntimeError(f"{spec.name}: missing primary-key columns: {', '.join(missing)}")

    key = _native_id_expr(spec)
    present = " AND ".join(f"{_quoted(column)} IS NOT NULL" for column in spec.primary_key)
    sql = f"""
        COPY (
            SELECT * EXCLUDE (_corpus_rn)
            FROM (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY {key}
                        ORDER BY md5({key} || '{SELECTION_SEED}')
                    ) AS _corpus_rn
                FROM corpus_source
                WHERE {present}
            )
            WHERE _corpus_rn = 1
            ORDER BY md5({key} || '{SELECTION_SEED}'), {key}
            LIMIT {spec.target_rows}
        )
        TO '{_sql_string(target)}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)
    """
    con.execute(sql)
    sample_rows = pq.ParquetFile(target).metadata.num_rows
    if sample_rows == 0:
        raise RuntimeError(f"{spec.name}: deterministic selection produced no rows")
    return source_rows, sample_rows, schema


def _augment_regulatory_closure(
    con: duckdb.DuckDBPyConnection,
    *,
    source_uris: dict[str, str | list[str]],
    output_dir: Path,
) -> None:
    """Add explicit comment parents and well-known RIN cases to the samples."""
    comments = output_dir / "comments.parquet"
    dockets = output_dir / "dockets.parquet"
    full_dockets = source_uris["dockets"]
    full_dockets_sql = _parquet_source(full_dockets)
    known_rins = ("0301-AA02", "1625-AA00", "2070-AB27", "2120-AA64")
    rin_sql = ", ".join(f"'{value}'" for value in known_rins)
    tmp = dockets.with_suffix(".closure.parquet")
    con.execute(
        f"""
        COPY (
            SELECT * EXCLUDE (_corpus_rn)
            FROM (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY docket_id
                        ORDER BY md5(docket_id || '{SELECTION_SEED}')
                    ) AS _corpus_rn
                FROM (
                    SELECT * FROM read_parquet('{_sql_string(dockets)}')
                    UNION ALL BY NAME
                    SELECT d.*
                    FROM read_parquet({full_dockets_sql}) AS d
                    JOIN (
                        SELECT DISTINCT docket_id
                        FROM read_parquet('{_sql_string(comments)}')
                        WHERE docket_id IS NOT NULL
                    ) AS c USING (docket_id)
                    UNION ALL BY NAME
                    SELECT *
                    FROM read_parquet({full_dockets_sql})
                    WHERE rin IN ({rin_sql})
                )
            )
            WHERE _corpus_rn = 1
            ORDER BY docket_id
        )
        TO '{_sql_string(tmp)}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)
        """
    )
    tmp.replace(dockets)

    documents = output_dir / "documents.parquet"
    full_documents = source_uris["documents"]
    full_documents_sql = _parquet_source(full_documents)
    tmp = documents.with_suffix(".closure.parquet")
    con.execute(
        f"""
        COPY (
            SELECT * EXCLUDE (_corpus_rn)
            FROM (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY document_id
                        ORDER BY md5(document_id || '{SELECTION_SEED}')
                    ) AS _corpus_rn
                FROM (
                    SELECT * FROM read_parquet('{_sql_string(documents)}')
                    UNION ALL BY NAME
                    SELECT * EXCLUDE (_docket_rank)
                    FROM (
                        SELECT
                            doc.*,
                            row_number() OVER (
                                PARTITION BY doc.docket_id
                                ORDER BY md5(doc.document_id || '{SELECTION_SEED}')
                            ) AS _docket_rank
                        FROM read_parquet({full_documents_sql}) AS doc
                        JOIN (
                            SELECT DISTINCT docket_id
                            FROM read_parquet('{_sql_string(comments)}')
                            WHERE docket_id IS NOT NULL
                        ) AS c USING (docket_id)
                    )
                    WHERE _docket_rank <= 3
                )
            )
            WHERE _corpus_rn = 1
            ORDER BY document_id
        )
        TO '{_sql_string(tmp)}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)
        """
    )
    tmp.replace(documents)


def _write_records(con: duckdb.DuckDBPyConnection, output_dir: Path) -> Path:
    selects: list[str] = []
    for spec in SOURCE_SPECS:
        path = output_dir / f"{spec.name}.parquet"
        native = _native_id_expr(spec)
        title = _coalesce(spec.title_columns)
        summary = _coalesce(spec.summary_columns)
        date = _coalesce(spec.date_columns)
        url = _coalesce(spec.url_columns)
        selects.append(
            f"""
            SELECT
                'record_' || substr(sha256('{spec.name}:' || {native}), 1, 24) AS record_id,
                '{spec.name}' AS source_table,
                '{spec.family}' AS source_family,
                '{spec.ontology_kind}' AS ontology_kind,
                {native} AS native_id,
                cast({title} AS VARCHAR) AS title,
                cast({summary} AS VARCHAR) AS summary,
                cast({date} AS VARCHAR) AS record_date,
                cast({url} AS VARCHAR) AS source_url
            FROM read_parquet('{_sql_string(path)}')
            """
        )
    target = output_dir / "records.parquet"
    con.execute(
        f"""
        COPY (
            {" UNION ALL BY NAME ".join(selects)}
            ORDER BY source_table, native_id
        )
        TO '{_sql_string(target)}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)
        """
    )
    return target


def _pair(
    *,
    left_source: str,
    left_native: object,
    right_source: str,
    right_native: object,
    relation_kind: str,
    evidence_basis: str,
    evidence_value: object = None,
    evidence_strength: str = "direct_identifier",
) -> PairExpectation:
    return PairExpectation(
        left_record_id=record_id(left_source, left_native),
        left_source=left_source,
        right_record_id=record_id(right_source, right_native),
        right_source=right_source,
        label="related",
        relation_kind=relation_kind,
        evidence_basis=evidence_basis,
        evidence_value=None if evidence_value is None else str(evidence_value),
        evidence_strength=evidence_strength,
    )


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> Iterable[tuple]:
    cursor = con.execute(sql)
    while batch := cursor.fetchmany(20_000):
        yield from batch


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    row = con.execute(sql).fetchone()
    if row is None:
        raise RuntimeError(f"Scalar corpus query returned no row: {sql[:120]}")
    return row[0]


def _positive_expectations(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
) -> list[PairExpectation]:
    p = {name: _sql_string(output_dir / f"{name}.parquet") for name in EXPECTED_SOURCE_TABLES}
    related: list[PairExpectation] = []

    for document_id, docket_id in _rows(
        con,
        f"""
        SELECT doc.document_id, doc.docket_id
        FROM read_parquet('{p["documents"]}') AS doc
        JOIN read_parquet('{p["dockets"]}') AS docket USING (docket_id)
        WHERE doc.document_id IS NOT NULL
        ORDER BY doc.document_id
        LIMIT 100000
        """,
    ):
        related.append(
            _pair(
                left_source="documents",
                left_native=document_id,
                right_source="dockets",
                right_native=docket_id,
                relation_kind="document_in_docket",
                evidence_basis="documents.docket_id = dockets.docket_id",
                evidence_value=docket_id,
            )
        )

    for comment_id, docket_id in _rows(
        con,
        f"""
        SELECT comment.comment_id, comment.docket_id
        FROM read_parquet('{p["comments"]}') AS comment
        JOIN read_parquet('{p["dockets"]}') AS docket USING (docket_id)
        WHERE comment.comment_id IS NOT NULL
        ORDER BY comment.comment_id
        LIMIT 100000
        """,
    ):
        related.append(
            _pair(
                left_source="comments",
                left_native=comment_id,
                right_source="dockets",
                right_native=docket_id,
                relation_kind="comment_in_docket",
                evidence_basis="comments.docket_id = dockets.docket_id",
                evidence_value=docket_id,
            )
        )

    for agency, docket_id, year, month in _rows(
        con,
        f"""
        SELECT idx.agency_code, idx.docket_id, idx.year, idx.month
        FROM read_parquet('{p["comments_index"]}') AS idx
        JOIN read_parquet('{p["dockets"]}') AS docket USING (docket_id)
        ORDER BY idx.agency_code, idx.docket_id, idx.year, idx.month
        LIMIT 50000
        """,
    ):
        native = f"{agency}|{docket_id}|{year}|{month}"
        related.append(
            _pair(
                left_source="comments_index",
                left_native=native,
                right_source="dockets",
                right_native=docket_id,
                relation_kind="comment_index_describes_docket",
                evidence_basis="comments_index.docket_id = dockets.docket_id",
                evidence_value=docket_id,
            )
        )

    for document_number, docket_id in _rows(
        con,
        f"""
        SELECT DISTINCT link.document_number, link.docket_id
        FROM read_parquet('{p["fr_docket_links"]}') AS link
        JOIN read_parquet('{p["federal_register"]}') AS fr USING (document_number)
        JOIN read_parquet('{p["dockets"]}') AS docket USING (docket_id)
        ORDER BY link.document_number, link.docket_id
        LIMIT 100000
        """,
    ):
        related.append(
            _pair(
                left_source="federal_register",
                left_native=document_number,
                right_source="dockets",
                right_native=docket_id,
                relation_kind="federal_register_links_docket",
                evidence_basis="fr_docket_links(document_number,docket_id)",
                evidence_value=f"{document_number}|{docket_id}",
            )
        )

    for document_id, document_number in _rows(
        con,
        f"""
        SELECT doc.document_id, doc.fr_doc_num
        FROM read_parquet('{p["documents"]}') AS doc
        JOIN read_parquet('{p["federal_register"]}') AS fr
          ON fr.document_number = doc.fr_doc_num
        WHERE doc.fr_doc_num IS NOT NULL
        ORDER BY doc.document_id
        LIMIT 50000
        """,
    ):
        related.append(
            _pair(
                left_source="documents",
                left_native=document_id,
                right_source="federal_register",
                right_native=document_number,
                relation_kind="regulations_document_has_federal_register_format",
                evidence_basis="documents.fr_doc_num = federal_register.document_number",
                evidence_value=document_number,
            )
        )

    for rin, edition, docket_id in _rows(
        con,
        f"""
        SELECT agenda.rin, agenda.agenda_edition, docket.docket_id
        FROM read_parquet('{p["unified_agenda"]}') AS agenda
        JOIN read_parquet('{p["dockets"]}') AS docket USING (rin)
        WHERE agenda.rin IS NOT NULL
        ORDER BY agenda.rin, agenda.agenda_edition, docket.docket_id
        LIMIT 50000
        """,
    ):
        related.append(
            _pair(
                left_source="unified_agenda",
                left_native=f"{rin}|{edition}",
                right_source="dockets",
                right_native=docket_id,
                relation_kind="docket_reports_agenda_item",
                evidence_basis="unified_agenda.rin = dockets.rin; relation, not proceeding identity",
                evidence_value=rin,
            )
        )

    for uei, recipient_id in _rows(
        con,
        f"""
        SELECT sam.uei, spend.recipient_id
        FROM read_parquet('{p["sam_entities"]}') AS sam
        JOIN read_parquet('{p["usaspending_recipients"]}') AS spend USING (uei)
        WHERE sam.uei IS NOT NULL AND trim(sam.uei) <> ''
        ORDER BY sam.uei, spend.recipient_id
        LIMIT 50000
        """,
    ):
        related.append(
            _pair(
                left_source="sam_entities",
                left_native=uei,
                right_source="usaspending_recipients",
                right_native=recipient_id,
                relation_kind="same_registered_entity",
                evidence_basis="sam_entities.uei = usaspending_recipients.uei",
                evidence_value=uei,
            )
        )

    for submission, proceeding_id, proceeding_name in _rows(
        con,
        f"""
        SELECT filing.id_submission, proceeding.id_proceeding, proceeding.name
        FROM read_parquet('{p["fcc_filings"]}') AS filing,
             json_each(filing.proceeding_names_json) AS item
        JOIN read_parquet('{p["fcc_proceedings"]}') AS proceeding
          ON proceeding.name = trim(cast(item.value AS VARCHAR), '"')
        ORDER BY filing.id_submission, proceeding.id_proceeding
        LIMIT 50000
        """,
    ):
        related.append(
            _pair(
                left_source="fcc_filings",
                left_native=submission,
                right_source="fcc_proceedings",
                right_native=proceeding_id,
                relation_kind="fcc_filing_in_proceeding",
                evidence_basis="fcc_filings.proceeding_names_json contains fcc_proceedings.name",
                evidence_value=proceeding_name,
            )
        )

    for filing, previous, client_id in _rows(
        con,
        f"""
        WITH ordered AS (
            SELECT
                filing_uuid,
                client_id,
                lag(filing_uuid) OVER (
                    PARTITION BY client_id
                    ORDER BY filing_uuid
                ) AS previous_filing
            FROM read_parquet('{p["lobbying_filings"]}')
            WHERE client_id IS NOT NULL AND trim(client_id) <> ''
        )
        SELECT filing_uuid, previous_filing, client_id
        FROM ordered
        WHERE previous_filing IS NOT NULL
        ORDER BY client_id, filing_uuid
        LIMIT 40000
        """,
    ):
        related.append(
            _pair(
                left_source="lobbying_filings",
                left_native=filing,
                right_source="lobbying_filings",
                right_native=previous,
                relation_kind="same_lobbying_client",
                evidence_basis="equal source-issued lobbying client_id",
                evidence_value=client_id,
            )
        )

    for granule, previous, package in _rows(
        con,
        f"""
        WITH ordered AS (
            SELECT
                granule_id,
                package_id,
                lag(granule_id) OVER (
                    PARTITION BY package_id
                    ORDER BY granule_id
                ) AS previous_granule
            FROM read_parquet('{p["cfr_sections"]}')
            WHERE package_id IS NOT NULL AND trim(package_id) <> ''
        )
        SELECT granule_id, previous_granule, package_id
        FROM ordered
        WHERE previous_granule IS NOT NULL
        ORDER BY package_id, granule_id
        LIMIT 40000
        """,
    ):
        related.append(
            _pair(
                left_source="cfr_sections",
                left_native=granule,
                right_source="cfr_sections",
                right_native=previous,
                relation_kind="same_cfr_package",
                evidence_basis="equal GovInfo package_id",
                evidence_value=package,
            )
        )

    return list(
        {
            (
                row.left_record_id,
                row.right_record_id,
                row.relation_kind,
            ): row
            for row in related
            if row.left_record_id != row.right_record_id
        }.values()
    )


def negative_controls(positive: Sequence[PairExpectation]) -> list[PairExpectation]:
    """Rotate right endpoints inside each relation shape without colliding with gold."""
    by_shape: dict[tuple[str, str, str], list[PairExpectation]] = defaultdict(list)
    positive_pairs = {
        (row.left_record_id, row.right_record_id)
        for row in positive
    } | {
        (row.right_record_id, row.left_record_id)
        for row in positive
    }
    for row in positive:
        by_shape[(row.relation_kind, row.left_source, row.right_source)].append(row)

    controls: list[PairExpectation] = []
    for (kind, left_source, right_source), rows in sorted(by_shape.items()):
        ordered = sorted(rows, key=lambda row: (row.left_record_id, row.right_record_id))
        if len(ordered) < 2:
            continue
        rights = [(row.right_record_id, row.evidence_value) for row in ordered]
        for index, row in enumerate(ordered):
            replacement: tuple[str, str | None] | None = None
            for offset in range(1, len(rights)):
                candidate = rights[(index + offset) % len(rights)]
                pair = (row.left_record_id, candidate[0])
                if row.left_record_id != candidate[0] and pair not in positive_pairs:
                    replacement = candidate
                    break
            if replacement is None:
                continue
            controls.append(
                PairExpectation(
                    left_record_id=row.left_record_id,
                    left_source=left_source,
                    right_record_id=replacement[0],
                    right_source=right_source,
                    label="no_declared_relation",
                    relation_kind=kind,
                    evidence_basis=(
                        "deterministic endpoint rotation; source-issued join key "
                        "does not match in this corpus snapshot"
                    ),
                    evidence_value=None,
                    evidence_strength="negative_control",
                )
            )
    return controls


def _cross_source_controls(
    con: duckdb.DuckDBPyConnection,
    records_path: Path,
    *,
    per_source: int = 100,
) -> list[PairExpectation]:
    records: dict[str, list[str]] = {}
    for source in EXPECTED_SOURCE_TABLES:
        rows = con.execute(
            f"""
            SELECT record_id
            FROM read_parquet('{_sql_string(records_path)}')
            WHERE source_table = ?
            ORDER BY record_id
            LIMIT {per_source}
            """,
            [source],
        ).fetchall()
        records[source] = [str(row[0]) for row in rows]

    controls: list[PairExpectation] = []
    source_count = len(EXPECTED_SOURCE_TABLES)
    for index, source in enumerate(EXPECTED_SOURCE_TABLES):
        target_source = EXPECTED_SOURCE_TABLES[(index + 7) % source_count]
        lefts = records[source]
        rights = records[target_source]
        for ordinal in range(min(len(lefts), len(rights))):
            controls.append(
                PairExpectation(
                    left_record_id=lefts[ordinal],
                    left_source=source,
                    right_record_id=rights[-(ordinal + 1)],
                    right_source=target_source,
                    label="no_declared_relation",
                    relation_kind="cross_source_control",
                    evidence_basis=(
                        "deterministic cross-source pairing with no source-declared "
                        "identifier or relationship in the bound snapshot"
                    ),
                    evidence_value=None,
                    evidence_strength="negative_control",
                )
            )
    return controls


def _unknown_same_title_pairs(
    con: duckdb.DuckDBPyConnection,
    records_path: Path,
) -> list[PairExpectation]:
    rows = con.execute(
        f"""
        WITH titled AS (
            SELECT
                record_id,
                source_table,
                lower(trim(title)) AS normalized_title,
                row_number() OVER (
                    PARTITION BY lower(trim(title))
                    ORDER BY source_table, record_id
                ) AS title_rank
            FROM read_parquet('{_sql_string(records_path)}')
            WHERE title IS NOT NULL
              AND length(trim(title)) >= 24
        )
        SELECT
            left_row.record_id,
            left_row.source_table,
            right_row.record_id,
            right_row.source_table,
            left_row.normalized_title
        FROM titled AS left_row
        JOIN titled AS right_row
          ON left_row.normalized_title = right_row.normalized_title
         AND right_row.title_rank = left_row.title_rank + 1
         AND right_row.source_table <> left_row.source_table
        ORDER BY left_row.normalized_title, left_row.record_id
        LIMIT 5000
        """
    ).fetchall()
    return [
        PairExpectation(
            left_record_id=str(left_id),
            left_source=str(left_source),
            right_record_id=str(right_id),
            right_source=str(right_source),
            label="unknown",
            relation_kind="same_title_without_identifier",
            evidence_basis="exact normalized title match without a source-issued crosswalk",
            evidence_value=str(title),
            evidence_strength="ambiguous_lexical_signal",
        )
        for left_id, left_source, right_id, right_source, title in rows
    ]


def _write_expectations(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    records_path: Path,
) -> Path:
    positive = _positive_expectations(con, output_dir)
    negative = [
        *negative_controls(positive),
        *_cross_source_controls(con, records_path),
    ]
    unknown = _unknown_same_title_pairs(con, records_path)
    positive_pairs = {
        frozenset((row.left_record_id, row.right_record_id))
        for row in positive
    }
    negative = [
        row
        for row in negative
        if frozenset((row.left_record_id, row.right_record_id)) not in positive_pairs
    ]
    rows = [*positive, *negative, *unknown]
    rows.sort(
        key=lambda row: (
            row.label,
            row.relation_kind,
            row.left_record_id,
            row.right_record_id,
        )
    )
    target = output_dir / "relationship_expectations.parquet"
    write_parquet_rows(
        target,
        columns=EXPECTATION_COLUMNS,
        rows=(row.as_row() for row in rows),
    )
    return target


def _write_membership(
    con: duckdb.DuckDBPyConnection,
    *,
    records_path: Path,
    expectations_path: Path,
    output_dir: Path,
) -> Path:
    target = output_dir / "record_membership.parquet"
    con.execute(
        f"""
        COPY (
            WITH endpoints AS (
                SELECT left_record_id AS record_id, label
                FROM read_parquet('{_sql_string(expectations_path)}')
                UNION ALL
                SELECT right_record_id AS record_id, label
                FROM read_parquet('{_sql_string(expectations_path)}')
            ),
            counts AS (
                SELECT
                    record_id,
                    count(*) FILTER (WHERE label = 'related') AS related_count,
                    count(*) FILTER (WHERE label = 'no_declared_relation') AS control_count,
                    count(*) FILTER (WHERE label = 'unknown') AS unknown_count
                FROM endpoints
                GROUP BY record_id
            )
            SELECT
                record.record_id,
                record.source_table,
                CASE
                    WHEN coalesce(counts.related_count, 0) > 0 THEN 'related_endpoint'
                    WHEN coalesce(counts.control_count, 0) > 0 THEN 'unrelated_control'
                    WHEN coalesce(counts.unknown_count, 0) > 0 THEN 'ambiguous'
                    ELSE 'distractor'
                END AS sample_role,
                cast(coalesce(counts.related_count, 0) AS VARCHAR) AS related_expectation_count,
                cast(coalesce(counts.control_count, 0) AS VARCHAR) AS control_expectation_count,
                cast(coalesce(counts.unknown_count, 0) AS VARCHAR) AS unknown_expectation_count
            FROM read_parquet('{_sql_string(records_path)}') AS record
            LEFT JOIN counts USING (record_id)
            ORDER BY record.source_table, record.native_id
        )
        TO '{_sql_string(target)}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)
        """
    )
    return target


def _write_openai_eval_inputs(
    con: duckdb.DuckDBPyConnection,
    *,
    source_uris: dict[str, str | list[str]],
    output_dir: Path,
    document_limit: int = 24,
) -> dict[str, int]:
    """Write a closed regulatory slice with FR topic labels for real-model evaluation."""
    target = output_dir / "openai-eval-inputs"
    target.mkdir()
    docs = source_uris["documents"]
    dockets = source_uris["dockets"]
    fr = source_uris["federal_register"]
    links = source_uris["fr_docket_links"]
    agenda = source_uris["unified_agenda"]
    docs_sql = _parquet_source(docs)
    dockets_sql = _parquet_source(dockets)
    fr_sql = _parquet_source(fr)
    links_sql = _parquet_source(links)
    agenda_sql = _parquet_source(agenda)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE eval_documents AS
        WITH candidates AS (
            SELECT
                doc.document_id,
                doc.docket_id,
                doc.agency_code,
                doc.fr_doc_num,
                row_number() OVER (
                    PARTITION BY doc.agency_code
                    ORDER BY md5(doc.document_id || '{SELECTION_SEED}')
                ) AS agency_rank,
                row_number() OVER (
                    PARTITION BY doc.docket_id
                    ORDER BY md5(doc.document_id || '{SELECTION_SEED}')
                ) AS docket_rank
            FROM read_parquet({docs_sql}) AS doc
            JOIN read_parquet({dockets_sql}) AS docket USING (docket_id)
            JOIN read_parquet({fr_sql}) AS fr_doc
              ON fr_doc.document_number = doc.fr_doc_num
            WHERE doc.document_id IS NOT NULL
              AND doc.docket_id IS NOT NULL
              AND nullif(trim(coalesce(docket.title, docket.abstract, '')), '') IS NOT NULL
              AND nullif(trim(fr_doc.abstract), '') IS NOT NULL
              AND fr_doc.topics_json IS NOT NULL
              AND fr_doc.topics_json <> '[]'
        )
        SELECT document_id, docket_id, agency_code, fr_doc_num
        FROM candidates
        WHERE agency_rank <= 2 AND docket_rank = 1
        ORDER BY md5(document_id || '{SELECTION_SEED}')
        LIMIT {document_limit}
        """
    )
    selected = int(_scalar(con, "SELECT count(*) FROM eval_documents"))
    if selected < document_limit:
        raise RuntimeError(
            f"OpenAI evaluation selection found {selected} documents; expected {document_limit}"
        )

    copies = {
        "documents": (
            f"SELECT doc.* FROM read_parquet({docs_sql}) AS doc "
            "JOIN eval_documents AS selected USING (document_id) ORDER BY doc.document_id"
        ),
        "dockets": (
            f"SELECT DISTINCT docket.* FROM read_parquet({dockets_sql}) AS docket "
            "JOIN eval_documents AS selected USING (docket_id) ORDER BY docket.docket_id"
        ),
        "federal_register": (
            f"SELECT DISTINCT fr_doc.* FROM read_parquet({fr_sql}) AS fr_doc "
            "JOIN eval_documents AS selected ON selected.fr_doc_num = fr_doc.document_number "
            "ORDER BY fr_doc.document_number"
        ),
        "fr_docket_links": (
            f"SELECT DISTINCT link.* FROM read_parquet({links_sql}) AS link "
            "JOIN eval_documents AS selected "
            "ON selected.fr_doc_num = link.document_number "
            "AND selected.docket_id = link.docket_id "
            "ORDER BY link.document_number, link.docket_id"
        ),
    }
    for name, query in copies.items():
        con.execute("CREATE OR REPLACE TEMP VIEW eval_copy AS " + query)
        con.execute(
            f"""
            COPY (SELECT * FROM eval_copy)
            TO '{_sql_string(target / f"{name}.parquet")}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE eval_rins AS
        SELECT DISTINCT docket.rin
        FROM read_parquet({dockets_sql}) AS docket
        JOIN eval_documents AS selected USING (docket_id)
        WHERE docket.rin IS NOT NULL AND trim(docket.rin) <> ''
        UNION
        SELECT DISTINCT trim(cast(item.value AS VARCHAR), '"') AS rin
        FROM read_parquet({fr_sql}) AS fr_doc
        JOIN eval_documents AS selected
          ON selected.fr_doc_num = fr_doc.document_number,
             json_each(fr_doc.regulation_id_numbers_json) AS item
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT agenda.*
            FROM read_parquet({agenda_sql}) AS agenda
            JOIN eval_rins USING (rin)
            ORDER BY agenda.rin, agenda.agenda_edition
        )
        TO '{_sql_string(target / "unified_agenda.parquet")}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    return {
        name: pq.ParquetFile(target / f"{name}.parquet").metadata.num_rows
        for name in (
            "dockets",
            "documents",
            "federal_register",
            "unified_agenda",
            "fr_docket_links",
        )
    }


def _validate_and_receipt(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    *,
    source_records: Sequence[dict[str, Any]],
    openai_eval_rows: dict[str, int],
) -> dict[str, Any]:
    records = output_dir / "records.parquet"
    expectations = output_dir / "relationship_expectations.parquet"
    membership = output_dir / "record_membership.parquet"
    record_count = pq.ParquetFile(records).metadata.num_rows
    source_sample_total = sum(int(record["sample_rows"]) for record in source_records)
    missing_sources = sorted(
        set(EXPECTED_SOURCE_TABLES)
        - {
            str(row[0])
            for row in con.execute(
                f"SELECT DISTINCT source_table FROM read_parquet('{_sql_string(records)}')"
            ).fetchall()
        }
    )
    dangling = int(
        _scalar(
            con,
            f"""
            WITH endpoints AS (
                SELECT left_record_id AS record_id
                FROM read_parquet('{_sql_string(expectations)}')
                UNION ALL
                SELECT right_record_id AS record_id
                FROM read_parquet('{_sql_string(expectations)}')
            )
            SELECT count(*)
            FROM endpoints
            LEFT JOIN read_parquet('{_sql_string(records)}') AS record USING (record_id)
            WHERE record.record_id IS NULL
            """,
        )
    )
    label_counts = {
        str(label): int(count)
        for label, count in con.execute(
            f"""
            SELECT label, count(*)
            FROM read_parquet('{_sql_string(expectations)}')
            GROUP BY label
            ORDER BY label
            """
        ).fetchall()
    }
    positive_negative_overlap = int(
        _scalar(
            con,
            f"""
            WITH pairs AS (
                SELECT
                    least(left_record_id, right_record_id) AS a,
                    greatest(left_record_id, right_record_id) AS b,
                    bool_or(label = 'related') AS positive,
                    bool_or(label = 'no_declared_relation') AS negative
                FROM read_parquet('{_sql_string(expectations)}')
                GROUP BY 1, 2
            )
            SELECT count(*) FROM pairs WHERE positive AND negative
            """,
        )
    )
    relation_counts = {
        str(kind): int(count)
        for kind, count in con.execute(
            f"""
            SELECT relation_kind, count(*)
            FROM read_parquet('{_sql_string(expectations)}')
            WHERE label = 'related'
            GROUP BY relation_kind
            ORDER BY relation_kind
            """
        ).fetchall()
    }
    membership_rows = pq.ParquetFile(membership).metadata.num_rows
    failures: list[str] = []
    if missing_sources:
        failures.append(f"missing source tables: {', '.join(missing_sources)}")
    if record_count != source_sample_total:
        failures.append(
            f"record envelope has {record_count} rows but source samples have {source_sample_total}"
        )
    if dangling:
        failures.append(f"{dangling} relationship endpoints do not resolve")
    if positive_negative_overlap:
        failures.append(
            f"{positive_negative_overlap} record pairs are both related and negative controls"
        )
    if any(label_counts.get(label, 0) == 0 for label in ("related", "no_declared_relation", "unknown")):
        failures.append("relationship expectations do not contain all three labels")
    if membership_rows != record_count:
        failures.append("record membership row count does not match record envelope")
    if any(rows <= 0 for rows in openai_eval_rows.values()):
        failures.append("OpenAI evaluation slice has an empty required source")

    artifacts = {}
    for path in sorted(output_dir.glob("*.parquet")):
        artifacts[path.name] = {
            "rows": pq.ParquetFile(path).metadata.num_rows,
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
    for path in sorted((output_dir / "openai-eval-inputs").glob("*.parquet")):
        artifacts[f"openai-eval-inputs/{path.name}"] = {
            "rows": pq.ParquetFile(path).metadata.num_rows,
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
    dataset_id = "mixed_snapshot_" + hashlib.sha256(
        canonical_json(
            {name: record["sha256"] for name, record in sorted(artifacts.items())}
        ).encode()
    ).hexdigest()[:24]
    return {
        "format_version": CORPUS_VERSION,
        "dataset_id": dataset_id,
        "status": "pass" if not failures else "fail",
        "selection_seed": SELECTION_SEED,
        "source_table_count": len(source_records),
        "source_rows_available": sum(int(record["source_rows"]) for record in source_records),
        "sample_rows": source_sample_total,
        "record_rows": record_count,
        "expectation_rows": sum(label_counts.values()),
        "label_counts": label_counts,
        "related_counts_by_kind": relation_counts,
        "dangling_endpoint_count": dangling,
        "positive_negative_pair_overlap": positive_negative_overlap,
        "openai_eval_input_rows": openai_eval_rows,
        "artifacts": artifacts,
        "failures": failures,
    }


def build_corpus(
    output_dir: Path,
    *,
    regulatory_source_dir: Path | None = None,
    base_url: str = DEFAULT_BASE_URL,
) -> dict[str, Any]:
    """Build the corpus in a temporary directory and atomically install it."""
    output_dir = output_dir.resolve()
    if output_dir.exists():
        raise FileExistsError(f"Refusing to overwrite existing corpus directory: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}-", dir=output_dir.parent))
    source_uris = _source_uris(
        base_url=base_url,
        regulatory_source_dir=regulatory_source_dir,
    )
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='4GB'")
    source_records: list[dict[str, Any]] = []
    try:
        for spec in SOURCE_SPECS:
            source = source_uris[spec.name]
            source_rows, sample_rows, schema = _sample_source(
                con,
                spec=spec,
                source=source,
                target=temp_dir / f"{spec.name}.parquet",
            )
            metadata: list[dict[str, Any]]
            if isinstance(source, list):
                metadata = [_remote_metadata(url) for url in source]
            elif source.startswith(("https://", "http://")):
                metadata = [_remote_metadata(source)]
            else:
                path = Path(source)
                metadata = [
                    {
                        "path": str(path),
                        "bytes": path.stat().st_size,
                        "sha256": _sha256(path),
                    }
                ]
            source_records.append(
                {
                    "name": spec.name,
                    "family": spec.family,
                    "ontology_kind": spec.ontology_kind,
                    "primary_key": list(spec.primary_key),
                    "selection": (
                        f"deduplicated by primary key; ordered by md5(primary_key || "
                        f"{SELECTION_SEED!r}); limit {spec.target_rows}"
                    ),
                    "source_rows": source_rows,
                    "sample_rows": sample_rows,
                    "source_artifacts": metadata,
                    "schema": schema,
                }
            )

        _augment_regulatory_closure(
            con,
            source_uris=source_uris,
            output_dir=temp_dir,
        )
        for record in source_records:
            record["sample_rows"] = pq.ParquetFile(
                temp_dir / f"{record['name']}.parquet"
            ).metadata.num_rows

        records_path = _write_records(con, temp_dir)
        expectations_path = _write_expectations(con, temp_dir, records_path)
        _write_membership(
            con,
            records_path=records_path,
            expectations_path=expectations_path,
            output_dir=temp_dir,
        )
        openai_eval_rows = _write_openai_eval_inputs(
            con,
            source_uris=source_uris,
            output_dir=temp_dir,
        )
        receipt = _validate_and_receipt(
            con,
            temp_dir,
            source_records=source_records,
            openai_eval_rows=openai_eval_rows,
        )
        manifest = {
            "format_version": CORPUS_VERSION,
            "dataset_id": receipt["dataset_id"],
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "selection_seed": SELECTION_SEED,
            "purpose": (
                "Heterogeneous ontology stress corpus with real public records, "
                "source-declared relations, negative controls, and ambiguous pairs."
            ),
            "negative_label_semantics": (
                "no_declared_relation means no source-issued join connects the pair "
                "inside this bound snapshot; it is not a universal real-world non-relation."
            ),
            "sources": source_records,
            "openai_evaluation": {
                "directory": "openai-eval-inputs",
                "input_rows": openai_eval_rows,
                "contains_api_key": False,
            },
        }
        (temp_dir / "corpus-manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (temp_dir / "corpus-receipt.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if receipt["status"] != "pass":
            raise RuntimeError(
                "Mixed-source corpus validation failed: " + "; ".join(receipt["failures"])
            )
        temp_dir.replace(output_dir)
        return receipt
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    finally:
        con.close()


def validate_existing_corpus(output_dir: Path) -> dict[str, Any]:
    manifest_path = output_dir / "corpus-manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Invalid corpus manifest: {manifest_path}") from exc
    con = duckdb.connect()
    try:
        receipt = _validate_and_receipt(
            con,
            output_dir,
            source_records=manifest["sources"],
            openai_eval_rows=manifest["openai_evaluation"]["input_rows"],
        )
    finally:
        con.close()
    return receipt


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Invalid JSONL at {path}:{line_number}") from exc
            if not isinstance(value, dict):
                raise RuntimeError(f"JSONL row at {path}:{line_number} is not an object")
            rows.append(value)
    return rows


def _secret_prefix_matches(root: Path) -> list[str]:
    matches: list[str] = []
    needle = b"sk-" + b"proj-"
    for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file()):
        with path.open("rb") as handle:
            found = any(needle in chunk for chunk in iter(lambda: handle.read(1024 * 1024), b""))
        if found:
            matches.append(str(path.relative_to(root)))
    return matches


def _checkpoint_work_key(row: dict[str, Any]) -> tuple[str, ...]:
    return (
        str(row.get("subject_type") or ""),
        str(row.get("subject_id") or ""),
        str(row.get("artifact_digest") or ""),
        str(row.get("segment_id") or ""),
        str(row.get("work_id") or ""),
    )


def _latest_checkpoint_rows(
    rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    latest = {
        _checkpoint_work_key(row): row
        for row in rows
    }
    return [
        latest[key]
        for key in sorted(latest)
    ]


def _assignment_evidence_value(assignment: dict[str, Any]) -> dict[str, Any]:
    try:
        value = json.loads(
            str(assignment.get("evidence_json") or "{}")
        )
    except json.JSONDecodeError:
        return {}
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _valid_exhausted_call(metadata: dict[str, Any]) -> bool:
    try:
        attempts = _provider_attempts(metadata)
        attempt_count = int(str(metadata.get("attempt_count")))
        retry_count = int(str(metadata.get("retry_count")))
        max_retries = int(str(metadata.get("max_retries")))
        timeout = float(str(metadata.get("timeout_seconds")))
        prompt_tokens = int(
            str(metadata.get("prompt_token_estimate"))
        )
        safety_margin = int(
            str(metadata.get("prompt_safety_margin_tokens"))
        )
        input_budget = int(
            str(metadata.get("prompt_input_token_budget"))
        )
    except (TypeError, ValueError):
        return False
    return (
        metadata.get("status") == "retry_exhausted"
        and metadata.get("store") is False
        and metadata.get("reasoning_effort")
        in SUPPORTED_REASONING_EFFORTS
        and metadata.get("sdk_max_retries") == 0
        and timeout > 0
        and attempt_count == max_retries + 1
        and retry_count == attempt_count - 1
        and len(attempts) == attempt_count
        and all(
            isinstance(attempt, dict)
            and attempt.get("attempt") == index
            and attempt.get("status") == "error"
            and bool(attempt.get("error_code"))
            for index, attempt in enumerate(attempts, start=1)
        )
        and prompt_tokens > 0
        and prompt_tokens + safety_margin <= input_budget
    )


def _provider_attempts(
    metadata: dict[str, Any],
) -> list[dict[str, Any]]:
    attempts = metadata.get("attempts")
    if isinstance(attempts, list):
        return [
            cast(dict[str, Any], attempt)
            for attempt in attempts
            if isinstance(attempt, dict)
        ]
    return []


def _grounding_failures(
    assignments: Sequence[dict[str, Any]],
    subjects: dict[str, Any],
) -> list[str]:
    failures: list[str] = []
    for assignment in assignments:
        assignment_id = str(
            assignment.get("assignment_id") or ""
        )
        evidence = _assignment_evidence_value(assignment)
        artifact_digest = str(
            evidence.get("artifact_sha256")
            or evidence.get("subject_sha256")
            or ""
        )
        spans = evidence.get("spans")
        if not isinstance(spans, list) or not spans:
            failures.append(assignment_id)
            continue
        grounded = True
        for span_value in spans:
            if not isinstance(span_value, dict):
                grounded = False
                break
            span = cast(dict[str, Any], span_value)
            segment_id = str(span.get("segment_id") or "")
            subject = subjects.get(segment_id)
            field_key = str(
                span.get("evidence_field_key")
                or span.get("source_field")
                or ""
            )
            if subject is None or subject.version_digest != artifact_digest:
                grounded = False
                break
            field_text = subject.fields.get(field_key)
            source_span = (subject.source_spans or {}).get(field_key)
            canonical_field = (subject.field_sources or {}).get(
                field_key,
                field_key,
            )
            try:
                start = int(str(span.get("start_char")))
                end = int(str(span.get("end_char")))
                local_start = int(
                    str(span.get("segment_start_char"))
                )
                local_end = int(
                    str(span.get("segment_end_char"))
                )
            except (TypeError, ValueError):
                grounded = False
                break
            if (
                not isinstance(field_text, str)
                or source_span is None
                or canonical_field != span.get("source_field")
                or start != source_span[0] + local_start
                or end != source_span[0] + local_end
                or local_start < 0
                or local_end <= local_start
                or local_end > len(field_text)
                or field_text[local_start:local_end]
                != span.get("text")
                or (
                    span.get("source_sha256")
                    and (subject.source_sha256 or {}).get(field_key)
                    != span.get("source_sha256")
                )
            ):
                grounded = False
                break
        if not grounded:
            failures.append(assignment_id)
    return sorted(set(failures))


def _validation_work(
    assignment: dict[str, Any],
) -> set[tuple[str, str, str, int, int]]:
    assignment_id = str(assignment.get("assignment_id") or "")
    evidence = _assignment_evidence_value(assignment)
    work: set[tuple[str, str, str, int, int]] = set()
    for span_value in evidence.get("spans") or []:
        if not isinstance(span_value, dict):
            continue
        try:
            work.add(
                (
                    assignment_id,
                    str(span_value.get("segment_id") or ""),
                    str(span_value.get("source_field") or ""),
                    int(str(span_value.get("start_char"))),
                    int(str(span_value.get("end_char"))),
                )
            )
        except (TypeError, ValueError):
            continue
    return work


def build_openai_run_receipt(
    run_dir: Path,
    *,
    minimum_f1: float = 0.5,
    require_all_profiles: bool = False,
    require_call_metadata: bool = False,
) -> dict[str, Any]:
    """Audit a real segment generation and validation run without its key."""
    from spicy_regs.ontology.receipt import (
        _valid_completed_model_call,
    )

    checkpoints = run_dir / ".ontology-checkpoints"
    generation_paths = sorted(
        checkpoints.glob("*-assignment-generation.jsonl")
    )
    validation_paths = sorted(
        checkpoints.glob("*-assignment-validation.jsonl")
    )
    if len(generation_paths) != 1 or len(validation_paths) != 1:
        raise RuntimeError(
            "Expected exactly one generation and one validation "
            f"checkpoint in {checkpoints}"
        )
    generation_path = generation_paths[0]
    validation_path = validation_paths[0]
    run_id = generation_path.name.removesuffix(
        "-assignment-generation.jsonl"
    )
    generation_transitions = _read_jsonl(generation_path)
    validation_transitions = _read_jsonl(validation_path)
    generation = _latest_checkpoint_rows(generation_transitions)
    validation = _latest_checkpoint_rows(validation_transitions)

    segment_ids = {
        str(row.get("segment_id") or "")
        for row in generation
        if row.get("segment_id")
    }
    subjects = subjects_by_segment_id(run_dir, segment_ids)
    missing_segments = sorted(segment_ids - set(subjects))
    artifacts = {
        (
            subject.subject_type,
            subject.subject_id,
            subject.version_digest,
        ): subject
        for subject in subjects.values()
    }
    profile_counts = Counter(
        subject.profile_id for subject in artifacts.values()
    )
    segment_profile_counts = Counter(
        subject.profile_id for subject in subjects.values()
    )
    source_counts = Counter(
        str(subject.source_table or "")
        for subject in artifacts.values()
    )
    expected_profiles = {
        profile.profile_id
        for profile in SUBJECT_PROFILES
    }
    missing_profiles = sorted(expected_profiles - set(profile_counts))
    incomplete_artifacts: list[str] = []
    segments_by_artifact: dict[
        tuple[str, str, str],
        set[str],
    ] = defaultdict(set)
    for subject in subjects.values():
        key = (
            subject.subject_type,
            subject.subject_id,
            subject.version_digest,
        )
        segments_by_artifact[key].add(subject.segment_id)
    for key, selected in segments_by_artifact.items():
        expected_count = artifacts[key].segment_count
        if len(selected) != expected_count:
            incomplete_artifacts.append(":".join(key))

    raw_assignments = [
        cast(dict[str, Any], assignment)
        for row in generation
        for assignment in row.get("assignments", [])
        if isinstance(assignment, dict)
    ]
    generated_concepts_by_id = {
        str(concept.get("concept_id") or ""): concept
        for row in generation
        for concept in row.get("concepts", [])
        if isinstance(concept, dict) and concept.get("concept_id")
    }
    assignment_rows = read_parquet_rows(
        run_dir / "concept_assignments.parquet"
    )
    run_assignments = [
        cast(dict[str, Any], row)
        for row in assignment_rows
        if row.get("method") == "llm"
        and row.get("run_id") == run_id
    ]
    initial_assignments = [
        row
        for row in run_assignments
        if "validation" not in _assignment_evidence_value(row)
    ]
    validated_assignments = [
        row
        for row in run_assignments
        if "validation" in _assignment_evidence_value(row)
    ]

    grounding_failures = _grounding_failures(
        [*raw_assignments, *initial_assignments],
        subjects,
    )
    expected_validation_work = set().union(
        *(
            _validation_work(assignment)
            for assignment in initial_assignments
        )
    ) if initial_assignments else set()
    completed_validation: set[
        tuple[str, str, str, int, int]
    ] = set()
    for row in validation:
        if row.get("status") != "completed":
            continue
        try:
            completed_validation.add(
                (
                    str(row.get("assignment_id") or ""),
                    str(row.get("segment_id") or ""),
                    str(row.get("source_field") or ""),
                    int(str(row.get("start_char"))),
                    int(str(row.get("end_char"))),
                )
            )
        except ValueError:
            continue
    missing_validation_work = sorted(
        expected_validation_work - completed_validation
    )
    orphan_validation_work = sorted(
        completed_validation - expected_validation_work
    )

    call_rows = [
        *generation_transitions,
        *validation_transitions,
    ]
    call_metadata = [
        cast(dict[str, Any], row["model_call"])
        for row in call_rows
        if isinstance(row.get("model_call"), dict)
    ]
    invalid_call_metadata: list[dict[str, Any]] = []
    for row in call_rows:
        metadata = row.get("model_call")
        if not isinstance(metadata, dict):
            if require_call_metadata:
                invalid_call_metadata.append(row)
            continue
        typed_metadata = cast(dict[str, Any], metadata)
        if row.get("status") == "retry_exhausted":
            valid = _valid_exhausted_call(typed_metadata)
        else:
            valid = _valid_completed_model_call(typed_metadata)
        if require_call_metadata and not valid:
            invalid_call_metadata.append(row)

    attempts = [
        attempt
        for metadata in call_metadata
        for attempt in _provider_attempts(metadata)
    ]
    response_ids = [
        str(attempt["response_id"])
        for attempt in attempts
        if attempt.get("response_id")
    ]
    actor_ids = sorted(
        {
            str(row.get("actor_id") or "")
            for row in call_rows
            if row.get("actor_id")
        }
    )
    model_ids = [
        actor_id
        for actor_id in actor_ids
        if actor_id.startswith("openai:")
    ]
    non_openai_actor_ids = [
        actor_id
        for actor_id in actor_ids
        if not actor_id.startswith("openai:")
    ]

    quality = evaluate_tag_quality(
        run_dir,
        document_ids={
            subject_id
            for subject_type, subject_id, _ in artifacts
            if subject_type == "document"
        },
    )
    ontology_receipt_path = run_dir / "ontology-receipt.json"
    try:
        ontology_receipt = json.loads(
            ontology_receipt_path.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Invalid ontology receipt: {ontology_receipt_path}"
        ) from exc
    ledger_rows = read_parquet_rows(
        run_dir / "ontology_segment_ledger.parquet"
    )
    current_ledger = [
        row for row in ledger_rows if row.get("run_id") == run_id
    ]
    ledger_status_counts = Counter(
        str(row.get("status") or "") for row in current_ledger
    )
    boundary_counts: Counter[str] = Counter()
    for row in current_ledger:
        try:
            boundaries = json.loads(
                str(row.get("boundaries_json") or "{}")
            )
        except json.JSONDecodeError:
            boundaries = {}
        if isinstance(boundaries, dict):
            boundary_counts.update(map(str, boundaries.values()))
    ledger_segment_ids = {
        str(row.get("segment_id") or "")
        for row in current_ledger
    }
    missing_ledger_segments = sorted(segment_ids - ledger_segment_ids)

    secret_matches = _secret_prefix_matches(run_dir)
    failures: list[str] = []
    if not generation:
        failures.append("generation checkpoint is empty")
    if missing_segments:
        failures.append(
            f"{len(missing_segments)} generated segments do not resolve"
        )
    if incomplete_artifacts:
        failures.append(
            f"{len(incomplete_artifacts)} selected artifacts are partial"
        )
    if missing_ledger_segments:
        failures.append(
            f"{len(missing_ledger_segments)} segments lack ledger rows"
        )
    if any(
        row.get("status") not in FINAL_STATUSES
        for row in generation
    ):
        failures.append("generation has unresolved failed segments")
    if any(row.get("status") != "completed" for row in validation):
        failures.append("validation has unresolved failed spans")
    if require_all_profiles and missing_profiles:
        failures.append(
            f"{len(missing_profiles)} required subject profiles are absent"
        )
    if require_call_metadata and (
        len(call_metadata) != len(call_rows)
    ):
        failures.append(
            f"{len(call_rows) - len(call_metadata)} model calls lack telemetry"
        )
    if require_call_metadata and invalid_call_metadata:
        failures.append(
            f"{len(invalid_call_metadata)} model-call telemetry rows "
            "are invalid"
        )
    if (
        require_call_metadata
        and len(response_ids) != len(set(response_ids))
    ):
        failures.append("provider response ids are duplicated")
    if not model_ids:
        failures.append("no OpenAI actor_id appears in model work")
    if non_openai_actor_ids:
        failures.append(
            f"{len(non_openai_actor_ids)} non-OpenAI actors appear "
            "in model work"
        )
    if grounding_failures:
        failures.append(
            f"{len(grounding_failures)} generated assignments are "
            "not exactly grounded"
        )
    if missing_validation_work:
        failures.append(
            f"{len(missing_validation_work)} assignment spans were "
            "not validated"
        )
    if orphan_validation_work:
        failures.append(
            f"{len(orphan_validation_work)} validations do not resolve"
        )
    if quality.f1 < minimum_f1:
        failures.append(
            f"tag-quality F1 {quality.f1:.6f} is below "
            f"{minimum_f1:.6f}"
        )
    if ontology_receipt.get("status") != "pass":
        failures.append("ontology corpus receipt did not pass")
    if secret_matches:
        failures.append("OpenAI secret prefix appears in run artifacts")

    provider_usage = {
        "input_tokens": sum(
            int(attempt.get("input_tokens") or 0)
            for attempt in attempts
        ),
        "output_tokens": sum(
            int(attempt.get("output_tokens") or 0)
            for attempt in attempts
        ),
        "total_tokens": sum(
            int(attempt.get("total_tokens") or 0)
            for attempt in attempts
        ),
        "duration_ms": round(
            sum(
                float(metadata.get("duration_ms") or 0)
                for metadata in call_metadata
            ),
            3,
        ),
        "logical_calls": len(call_metadata),
        "physical_attempts": len(attempts),
        "retries": sum(
            int(metadata.get("retry_count") or 0)
            for metadata in call_metadata
        ),
        "failed_logical_calls": sum(
            metadata.get("status") != "completed"
            for metadata in call_metadata
        ),
    }
    validation_assignment_ids = {
        str(row.get("assignment_id") or "")
        for row in validation
        if row.get("status") == "completed"
    }
    initial_assignment_ids = {
        str(row.get("assignment_id") or "")
        for row in initial_assignments
    }
    return {
        "format_version": 2,
        "status": "pass" if not failures else "fail",
        "run_id": run_id,
        "model_ids": model_ids,
        "non_openai_actor_ids": non_openai_actor_ids,
        "generation_artifacts": len(artifacts),
        "generation_segments": len(generation),
        # Retained for readers of the pre-segmentation v1 receipt.
        "generation_subjects": len(generation),
        "generation_profile_count": len(profile_counts),
        "generation_profile_counts": dict(sorted(profile_counts.items())),
        "segment_profile_counts": dict(
            sorted(segment_profile_counts.items())
        ),
        "generation_source_counts": dict(sorted(source_counts.items())),
        "required_all_profiles": require_all_profiles,
        "missing_required_profiles": (
            missing_profiles if require_all_profiles else []
        ),
        "required_call_metadata": require_call_metadata,
        "provider_calls_with_metadata": len(call_metadata),
        "provider_call_metadata_failures": len(
            invalid_call_metadata
        ),
        "provider_response_id_count": len(set(response_ids)),
        "provider_response_id_digest": (
            hashlib.sha256(
                canonical_json(sorted(response_ids)).encode()
            ).hexdigest()
            if response_ids
            else None
        ),
        "provider_usage": provider_usage,
        "generation_assignments": len(initial_assignments),
        "raw_segment_assignments": len(raw_assignments),
        "validated_assignment_versions": len(
            validated_assignments
        ),
        "generation_candidate_concepts": len(
            generated_concepts_by_id
        ),
        "zero_assignment_subjects": ledger_status_counts.get(
            "zero_tags",
            0,
        ),
        "segment_status_counts": dict(
            sorted(ledger_status_counts.items())
        ),
        "segment_boundary_counts": dict(
            sorted(boundary_counts.items())
        ),
        "validation_calls": len(validation),
        "validation_agreements": sum(
            row.get("agrees") is True for row in validation
        ),
        "validation_disagreements": sum(
            row.get("agrees") is False for row in validation
        ),
        "validation_span_coverage": (
            len(expected_validation_work & completed_validation)
            / len(expected_validation_work)
            if expected_validation_work
            else 1.0
        ),
        "validation_coverage": (
            len(validation_assignment_ids & initial_assignment_ids)
            / len(initial_assignment_ids)
            if initial_assignment_ids
            else 1.0
        ),
        "structured_response_calls_evidenced": len(attempts),
        "grounding_failure_count": len(grounding_failures),
        "append_only_assignment_rows": len(assignment_rows),
        "current_assignment_rows": len(
            latest_assignments(assignment_rows)
        ),
        "tag_quality": quality.as_dict(),
        "minimum_f1": minimum_f1,
        "ontology_snapshot_id": (
            ontology_receipt.get("snapshot_id")
            or (
                ontology_receipt.get("generation", {}).get(
                    "snapshot_id"
                )
                if isinstance(
                    ontology_receipt.get("generation"),
                    dict,
                )
                else None
            )
        ),
        "ontology_receipt_status": ontology_receipt.get("status"),
        "checkpoint_artifacts": {
            str(generation_path.relative_to(run_dir)): {
                "rows": len(generation_transitions),
                "current_work_items": len(generation),
                "sha256": _sha256(generation_path),
            },
            str(validation_path.relative_to(run_dir)): {
                "rows": len(validation_transitions),
                "current_work_items": len(validation),
                "sha256": _sha256(validation_path),
            },
        },
        "api_key_persisted": bool(secret_matches),
        "secret_match_files": secret_matches,
        "failures": failures,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build", help="Build a new immutable corpus directory")
    build.add_argument("output_dir", type=Path)
    build.add_argument(
        "--regulatory-source-dir",
        type=Path,
        default=Path("output/rin-ontology-revision-candidate"),
        help="Local five-source snapshot with Federal Register topics_json",
    )
    build.add_argument("--base-url", default=DEFAULT_BASE_URL)
    validate = subparsers.add_parser("validate", help="Revalidate an existing corpus")
    validate.add_argument("output_dir", type=Path)
    openai_receipt = subparsers.add_parser(
        "openai-receipt",
        help="Audit a completed OpenAI ontology generation and validation run",
    )
    openai_receipt.add_argument("run_dir", type=Path)
    openai_receipt.add_argument("--minimum-f1", type=float, default=0.5)
    openai_receipt.add_argument(
        "--require-all-profiles",
        action="store_true",
    )
    openai_receipt.add_argument(
        "--require-call-metadata",
        action="store_true",
    )
    openai_receipt.add_argument("--output", type=Path)
    args = parser.parse_args()

    if args.command == "build":
        result = build_corpus(
            args.output_dir,
            regulatory_source_dir=args.regulatory_source_dir,
            base_url=args.base_url,
        )
    elif args.command == "validate":
        result = validate_existing_corpus(args.output_dir)
    else:
        result = build_openai_run_receipt(
            args.run_dir,
            minimum_f1=args.minimum_f1,
            require_all_profiles=args.require_all_profiles,
            require_call_metadata=args.require_call_metadata,
        )
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(
                json.dumps(result, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
