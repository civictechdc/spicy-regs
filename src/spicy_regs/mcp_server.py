from __future__ import annotations

import base64
import json
import logging
import os
import re
import tempfile
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from functools import lru_cache, wraps
from html import unescape
from pathlib import Path
from time import monotonic as _monotonic
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import duckdb
import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import Icon
from starlette.requests import Request
from starlette.responses import Response

from spicy_regs._icon import ICON_DATA_URI

DEFAULT_R2_BASE_URL = "https://data.spicy-regs.dev"
TABLES = (
    "dockets",
    "documents",
    "comments",
    "comments_index",
    "feed_summary",
    "agency_stats",
    "agency_monthly_volume",
    "rulemaking_lifecycles",
    "fr_docket_links",
    "discovery_signals",
    "cfr_sections",
    "congress_bills",
    "unified_agenda",
    "federal_register",
    "sam_entities",
    "lobbying_filings",
    "fec_committees",
    "org_committee_links",
    "gao_reports",
    "crs_reports",
    "court_dockets",
    "usaspending_recipients",
    "fcc_proceedings",
    "fcc_filings",
)
STATEMENT_TIMEOUT = os.environ.get("SPICY_REGS_STATEMENT_TIMEOUT", "790s")

logger = logging.getLogger(__name__)

CATALOG_ALIAS = "reg_catalog"
DEFAULT_CATALOG_NAMESPACE = "default"


def _observed(stage: str):
    def decorate(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            started = _monotonic()
            outcome = "ok"
            error_type = None
            try:
                result = fn(*args, **kwargs)
                if isinstance(result, dict) and "error" in result:
                    outcome = "error_response"
                return result
            except Exception as exc:
                outcome = "error"
                error_type = type(exc).__name__
                raise
            finally:
                logger.warning(json.dumps({
                    "event": "mcp_operation", "stage": stage,
                    "operation": fn.__name__, "outcome": outcome,
                    "elapsed_ms": round((_monotonic() - started) * 1000, 2),
                    "error_type": error_type,
                }))
        return wrapped
    return decorate


def _parse_timeout_seconds(raw: str) -> float | None:
    text = raw.strip().lower()
    if not text:
        return None
    multiplier = 1.0
    for suffix, factor in (("ms", 0.001), ("s", 1.0), ("m", 60.0)):
        if text.endswith(suffix):
            multiplier = factor
            text = text[: -len(suffix)].strip()
            break
    try:
        value = float(text)
    except ValueError as exc:
        raise RuntimeError(f"SPICY_REGS_STATEMENT_TIMEOUT is not a valid duration: {raw!r}") from exc
    if value <= 0:
        return None
    return value * multiplier


STATEMENT_TIMEOUT_SECONDS = _parse_timeout_seconds(STATEMENT_TIMEOUT)

INSTRUCTIONS = (
    "Query the Spicy Regs regulatory dataset (regulations.gov mirror) over "
    "the public Cloudflare R2 parquet bucket. Use list_sources to discover "
    "tables, describe_table for schemas, and query_sql for everything else. "
    "Always LIMIT result sets while exploring. Cite docket IDs, document "
    "IDs, comment IDs, agency codes, and dates from the rows you return. "
    "To read what a proposed or final rule actually says, use get_rule_text: "
    "call it once for the outline, then ask for a section such as '60.4'. "
    "When someone wants to comment on a rule, use prepare_comment: it finds "
    "the document open for comment and checks their draft, and they submit "
    "it themselves on regulations.gov. Help them say what they think in "
    "their own words; never submit or invent comments for anyone."
)

REGULATIONS_GOV = "https://www.regulations.gov"
USER_AGENT = "spicy-regs-mcp (+https://github.com/civictechdc/spicy-regs)"
COMMENT_MAX_CHARS = 5000
DOCKET_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{2,63}")

GOVINFO_FR_URL = "https://www.govinfo.gov/content/pkg/FR-{date}/html/{number}.htm"
FR_DOC_NUMBER_PATTERN = re.compile(r"(?:FR\s*Doc\.?\s*)?(\d{4}-\d{4,6})", re.IGNORECASE)
RULE_TEXT_MAX_BYTES = 8 * 1024 * 1024
RULE_TEXT_DEFAULT_CHARS = 8000
RULE_TEXT_MAX_CHARS = 20000
RULE_TYPE_PRIORITY = ("Proposed Rule", "Rule", "Interim Final Rule", "Direct Final Rule")
_SECTION_HEADING = re.compile(r"^Sec\.\s+(\d+[A-Za-z]?\.\d+[A-Za-z0-9-]*)\s+(\S.*)$")
_PART_HEADING = re.compile(r"^PART\s+(\d+[A-Za-z]?)\s*--\s*(.*)$")
_PAGE_MARKER = re.compile(r"^\s*\[\[Page \d+\]\]\s*$")
_SECTION_ARG = re.compile(r"^(?:§+|sec(?:tion)?\.?)?\s*(\d+[A-Za-z]?\.\d+[A-Za-z0-9-]*)$", re.IGNORECASE)
_PART_ARG = re.compile(r"^part\s+(\d+[A-Za-z]?)$", re.IGNORECASE)
_AMENDATORY_INSTRUCTION = re.compile(
    r"^\d+\.\s+(Amend|Revise|Add|Remove|Redesignate|Republish|Suspend|Reserve|Effective|"
    r"The authority citation|In Sec\.|Section|Part|Subpart|Appendix)\b"
)

ICONS = [Icon(src=ICON_DATA_URI, mimeType="image/png", sizes=["512x512"])]


def _resolve_r2_base_url() -> str:
    raw = os.environ.get("SPICY_REGS_R2_URL", DEFAULT_R2_BASE_URL).rstrip("/")
    parsed = urlparse(raw)
    if parsed.scheme != "https" or not parsed.netloc:
        raise RuntimeError(f"SPICY_REGS_R2_URL must be an https:// URL, got: {raw!r}")
    if any(c in raw for c in ("'", "\\", "\x00", "\n", "\r")):
        raise RuntimeError(f"SPICY_REGS_R2_URL contains illegal characters: {raw!r}")
    return raw


R2_BASE_URL = _resolve_r2_base_url()


def _resolve_home_directory() -> str:
    raw = os.environ.get("SPICY_REGS_HOME_DIR", tempfile.gettempdir())
    if any(c in raw for c in ("\x00", "\n", "\r")):
        raise RuntimeError(f"SPICY_REGS_HOME_DIR contains illegal characters: {raw!r}")
    return raw


HOME_DIRECTORY = _resolve_home_directory()


def _resolve_memory_limit() -> str | None:
    """DuckDB memory ceiling from SPICY_REGS_MEMORY_LIMIT (e.g. '12GB', '75%').

    Unset => None => DuckDB's own default (~80% of detected RAM). Set it on hosts
    where DuckDB can't see the real allocation (containers detect host RAM, not
    the cgroup limit) so it spills/errors before the platform OOM-kills the process.
    Interpolated into a SET, so the value is format-validated.
    """
    raw = os.environ.get("SPICY_REGS_MEMORY_LIMIT", "").strip()
    if not raw:
        return None
    if not re.fullmatch(r"\d+(\.\d+)?\s*(%|[KMGT]?i?B)?", raw, re.IGNORECASE):
        raise RuntimeError(f"SPICY_REGS_MEMORY_LIMIT is not a valid size/percent: {raw!r}")
    return raw


def _resolve_temp_dir() -> str:
    """DuckDB spill directory from SPICY_REGS_TEMP_DIR.

    Default '' disables spilling — the safe serverless behavior, since DuckDB's
    default temp dir is a relative '.tmp' that is read-only on serverless hosts,
    so a spilling query would fail there anyway. Supply a writable path (a
    container with real disk, or a mounted volume) to let big GROUP BY/ORDER BY
    spill instead of erroring. Interpolated into a SET, so injection chars are
    rejected.
    """
    raw = os.environ.get("SPICY_REGS_TEMP_DIR", "")
    if any(c in raw for c in ("'", "\\", "\x00", "\n", "\r")):
        raise RuntimeError(f"SPICY_REGS_TEMP_DIR contains illegal characters: {raw!r}")
    return raw


MEMORY_LIMIT = _resolve_memory_limit()
TEMP_DIR = _resolve_temp_dir()


def _resolve_catalog_config() -> dict[str, str] | None:
    uri = os.environ.get("R2_CATALOG_URI")
    warehouse = os.environ.get("R2_CATALOG_WAREHOUSE")
    token = os.environ.get("R2_CATALOG_TOKEN")
    if not (uri and warehouse and token):
        return None
    config = {
        "uri": uri,
        "warehouse": warehouse,
        "token": token,
        "namespace": os.environ.get("R2_CATALOG_NAMESPACE") or DEFAULT_CATALOG_NAMESPACE,
    }
    for key, value in config.items():
        if any(c in value for c in ("'", "\\", "\x00", "\n", "\r")):
            raise RuntimeError(f"R2 catalog {key} contains illegal characters")
    return config


def _jsonify(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonify(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    return str(value)


READ_ONLY_STATEMENT_TYPES = frozenset({"SELECT", "EXPLAIN"})


def _first_write_statement(cursor: duckdb.DuckDBPyConnection, sql: str) -> str | None:
    """Name of the first non-read-only statement in ``sql``, else None.

    The sandbox cannot express "no writes" on its own. ``disabled_filesystems``
    is the setting that would, but LocalFileSystem has to stay enabled for
    httpfs to read the CA bundle (see the security-settings notes), so
    ``COPY ... TO``, ``ATTACH``, and ``EXPORT DATABASE`` could all write to the
    container filesystem — on Cloud Run an in-memory one, where a large enough
    write evicts the instance. This is the gate that says no instead.

    Classification comes from DuckDB's own parser rather than a prefix regex,
    so leading comments, string literals, and stacked statements cannot smuggle
    a write past it. ``DESCRIBE``/``SHOW``/``SUMMARIZE``/``VALUES``/``TABLE``
    and the FROM-first shorthand all parse as SELECT, which is why a two-entry
    allowlist still admits every read form.

    Matching on ``StatementType.name`` rather than the enum member keeps this
    working against duckdb's incomplete type stubs, which do not declare the
    members, without a blanket type-ignore over the comparison.

    A ``ParserException`` propagates untouched: malformed SQL should surface
    DuckDB's own message, which names the offending token.
    """
    for statement in cursor.extract_statements(sql):
        name = statement.type.name
        if name not in READ_ONLY_STATEMENT_TYPES:
            return name
    return None


def _apply_security_settings(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET autoinstall_known_extensions=false")
    con.execute("SET autoload_known_extensions=false")
    con.execute("SET allow_unsigned_extensions=false")
    if MEMORY_LIMIT is not None:
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    con.execute("SET lock_configuration=true")


def _attach_catalog(con: duckdb.DuckDBPyConnection, config: dict[str, str]) -> bool:
    try:
        try:
            con.execute("INSTALL avro")
            con.execute("LOAD avro")
        except duckdb.Error as avro_exc:
            logger.info("avro not separately provisioned (%s); using iceberg's bundled path", avro_exc)
        con.execute("INSTALL iceberg")
        con.execute("LOAD iceberg")
        con.execute(f"CREATE OR REPLACE SECRET r2_catalog_secret (TYPE ICEBERG, TOKEN '{config['token']}');")
        con.execute(f"ATTACH '{config['warehouse']}' AS {CATALOG_ALIAS} (TYPE ICEBERG, ENDPOINT '{config['uri']}');")
        return True
    except duckdb.Error as exc:
        logger.warning("R2 catalog attach failed; comments fall back to monolith: %s", exc)
        return False


def _build_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET home_directory='{HOME_DIRECTORY.replace(chr(39), chr(39) * 2)}'")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    catalog = _resolve_catalog_config()
    catalog_attached = catalog is not None and _attach_catalog(con, catalog)

    _apply_security_settings(con)
    for name in TABLES:
        if name == "comments" and catalog_attached:
            namespace = catalog["namespace"]  # type: ignore[index]
            try:
                con.execute(
                    f"CREATE VIEW comments AS "
                    f'SELECT * FROM {CATALOG_ALIAS}."{namespace}"."comments" '
                    f"QUALIFY ROW_NUMBER() OVER "
                    f"(PARTITION BY comment_id ORDER BY modify_date DESC NULLS LAST) = 1"
                )
                continue
            except duckdb.Error as exc:
                logger.warning("comments not available in catalog; falling back to monolith: %s", exc)
        url = f"{R2_BASE_URL}/{name}.parquet"
        try:
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{url}')")
        except duckdb.Error as exc:
            logger.warning("table %s not available at %s; skipping view: %s", name, url, exc)
    return con


# Building a connection is the expensive part of a tool call — install httpfs +
# iceberg, attach the R2 catalog over REST, and CREATE VIEW over all 23 tables
# (each reads a parquet footer over HTTPS), ~35s on a cold serverless instance.
# The query that follows is milliseconds. So we build once and reuse: Fluid
# Compute keeps a warmed instance's module state across invocations, and the
# stdio server is a single long-lived process, so a module-level connection
# amortizes that cost across every request the instance serves.
#
# Concurrency: the cached connection is shared, but each request runs on its own
# `con.cursor()` — DuckDB's supported way to run overlapping queries on one
# connection — so the statement-timeout interrupt (below) hits only that cursor,
# never a sibling request.
#
# Staleness: the views hold parquet footers and the catalog attach pins an
# Iceberg snapshot, both taken at build time. The ETL republishes daily, so a
# connection older than the TTL is rebuilt to pick up new data. On rebuild we
# only drop the module reference to the old connection — never close it — so any
# cursor still mid-query keeps working (a cursor outlives its parent losing its
# last Python reference); the old connection is reclaimed once its cursors drain.
_CONNECTION_TTL_SECONDS = float(os.environ.get("SPICY_REGS_CONNECTION_TTL", "300"))
_connection_lock = threading.Lock()
_cached_connection: duckdb.DuckDBPyConnection | None = None
_cached_connection_at = 0.0


@_observed("connection_setup")
def _get_connection() -> duckdb.DuckDBPyConnection:
    """Return a shared, cached DuckDB connection, rebuilding past the TTL.

    Callers must run their query on ``.cursor()`` of the returned connection,
    not on the connection itself, so concurrent requests don't serialize and a
    per-request timeout interrupt stays scoped to that request.
    """
    global _cached_connection, _cached_connection_at
    with _connection_lock:
        age = _monotonic() - _cached_connection_at
        if _cached_connection is None or age >= _CONNECTION_TTL_SECONDS:
            # Drop (don't close) the previous connection: an in-flight cursor on
            # another thread still references it and must survive this swap.
            _cached_connection = _build_connection()
            _cached_connection_at = _monotonic()
        return _cached_connection


def _reset_connection_cache() -> None:
    """Forget the cached connection so the next call rebuilds. For tests."""
    global _cached_connection, _cached_connection_at
    with _connection_lock:
        _cached_connection = None
        _cached_connection_at = 0.0


@contextmanager
def _statement_timeout(cursor: duckdb.DuckDBPyConnection) -> Iterator[None]:
    started = _monotonic()
    error_type = None
    try:
        if STATEMENT_TIMEOUT_SECONDS is None:
            yield
            return
        tripped = threading.Event()

        def _interrupt() -> None:
            tripped.set()
            # Interrupt only this request's cursor, not the shared connection —
            # sibling requests run on their own cursors and must not be cancelled.
            cursor.interrupt()

        timer = threading.Timer(STATEMENT_TIMEOUT_SECONDS, _interrupt)
        timer.start()
        try:
            yield
        except duckdb.InterruptException as exc:
            if tripped.is_set():
                raise TimeoutError(f"Query exceeded the {STATEMENT_TIMEOUT} statement timeout") from exc
            raise
        finally:
            timer.cancel()
    except Exception as exc:
        error_type = type(exc).__name__
        raise
    finally:
        logger.warning(json.dumps({
            "event": "mcp_operation", "stage": "query_or_r2_read",
            "elapsed_ms": round((_monotonic() - started) * 1000, 2),
            "error_type": error_type,
            "outcome": "error" if error_type else "ok",
        }))


def _parse_utc(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _check_draft(draft: str) -> dict[str, Any]:
    chars = len(draft)
    utf8_bytes = len(draft.encode("utf-8"))
    over = utf8_bytes > COMMENT_MAX_CHARS
    check: dict[str, Any] = {
        "characters": chars,
        "utf8_bytes": utf8_bytes,
        "limit": COMMENT_MAX_CHARS,
        "fits_comment_box": not over,
    }
    if over:
        check["advice"] = (
            f"The regulations.gov comment box takes at most {COMMENT_MAX_CHARS} characters. "
            "Shorten it, or put the full text in an attached file and summarize it in the box."
        )
    return check


def _prepare_comment(
    cursor: duckdb.DuckDBPyConnection,
    docket_id: str,
    draft: str,
    now: datetime,
) -> dict[str, Any]:
    docket_id = docket_id.strip()
    if not DOCKET_ID_PATTERN.fullmatch(docket_id):
        return {"error": f"Not a docket ID: {docket_id!r}. Docket IDs look like 'USBC-2026-0628'."}

    docket = cursor.execute(
        "SELECT docket_id, agency_code, title FROM dockets WHERE docket_id = ? LIMIT 1",
        [docket_id],
    ).fetchone()
    if docket is None:
        return {"error": f"Docket {docket_id} is not in the Spicy Regs mirror."}

    rows = cursor.execute(
        "SELECT document_id, document_type, title, comment_start_date, comment_end_date, withdrawn "
        "FROM documents WHERE docket_id = ? AND comment_end_date IS NOT NULL",
        [docket_id],
    ).fetchall()

    open_documents = []
    latest_close: datetime | None = None
    for document_id, document_type, title, start_raw, end_raw, withdrawn in rows:
        if str(withdrawn).strip().lower() in ("true", "1", "yes"):
            continue
        closes = _parse_utc(end_raw)
        if closes is None:
            continue
        if latest_close is None or closes > latest_close:
            latest_close = closes
        opens = _parse_utc(start_raw)
        if closes <= now or (opens is not None and opens > now):
            continue
        open_documents.append(
            {
                "document_id": document_id,
                "document_type": document_type,
                "title": title,
                "comment_start_date": _jsonify(start_raw),
                "comment_end_date": _jsonify(end_raw),
                "days_left": max(0, (closes - now).days),
                "comment_url": f"{REGULATIONS_GOV}/commenton/{document_id}",
                "document_url": f"{REGULATIONS_GOV}/document/{document_id}",
            }
        )
    open_documents.sort(key=lambda d: (d["document_type"] != "Proposed Rule", d["comment_end_date"]))

    result: dict[str, Any] = {
        "docket_id": docket[0],
        "agency_code": docket[1],
        "docket_title": docket[2],
        "docket_url": f"{REGULATIONS_GOV}/docket/{docket[0]}",
        "checked_at": now.isoformat(timespec="seconds"),
        "status": "open" if open_documents else "closed",
        "open_documents": open_documents,
        "how_to_submit": (
            "Spicy Regs does not submit comments. Open comment_url, paste the comment, "
            "and submit it on regulations.gov. Deadlines change when agencies extend them, "
            "and this mirror refreshes nightly, so confirm the date on that page."
        ),
    }
    if not open_documents:
        result["last_comment_deadline"] = latest_close.isoformat(timespec="seconds") if latest_close else None
        result["how_to_submit"] = (
            "Nothing in this docket is open for comment in the mirror. Check docket_url in case "
            "the agency reopened or extended the comment period since the last nightly refresh."
        )
    if draft.strip():
        result["draft_check"] = _check_draft(draft)
    return result


class RuleTextError(RuntimeError):
    pass


@_observed("govinfo_http")
def _fetch_url_text(url: str) -> str:
    with httpx.Client(timeout=20.0, follow_redirects=True, headers={"User-Agent": USER_AGENT}) as client:
        with client.stream("GET", url) as response:
            if urlparse(str(response.url)).hostname != "www.govinfo.gov":
                raise RuleTextError(f"govinfo redirected off its own host to {response.url}")
            if response.status_code == 404:
                raise RuleTextError("govinfo has no text for this document yet")
            response.raise_for_status()
            chunks = []
            size = 0
            for chunk in response.iter_bytes():
                size += len(chunk)
                if size > RULE_TEXT_MAX_BYTES:
                    raise RuleTextError(f"the document is larger than {RULE_TEXT_MAX_BYTES // (1024 * 1024)} MB")
                chunks.append(chunk)
            encoding = response.encoding or "utf-8"
    return b"".join(chunks).decode(encoding, errors="replace")


def _govinfo_text(page: str) -> str:
    match = re.search(r"<pre>(.*)</pre>", page, re.DOTALL | re.IGNORECASE)
    body = unescape(re.sub(r"<[^>]+>", "", match.group(1) if match else page))
    lines = [line.rstrip() for line in body.splitlines() if not _PAGE_MARKER.match(line)]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


@lru_cache(maxsize=16)
def _fetch_rule_text(document_number: str, publication_date: str) -> str:
    return _govinfo_text(_fetch_url_text(GOVINFO_FR_URL.format(date=publication_date, number=document_number)))


def _rule_structure(text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    lines = text.splitlines()
    parts: list[dict[str, Any]] = []
    sections: list[dict[str, Any]] = []
    current_part = None
    for i, line in enumerate(lines):
        if i > 0 and lines[i - 1].strip():
            continue
        following = lines[i + 1].strip() if i + 1 < len(lines) else ""
        part = _PART_HEADING.match(line)
        if part:
            current_part = part.group(1)
            title = part.group(2).strip()
            if following and following.isupper():
                title = f"{title} {following}"
            parts.append({"part": current_part, "title": " ".join(title.split()), "line": i})
            continue
        section = _SECTION_HEADING.match(line)
        if section:
            title = section.group(2).strip()
            if following and not title.endswith("."):
                title = f"{title} {following}"
            sections.append(
                {"section": section.group(1), "title": " ".join(title.split()), "part": current_part, "line": i}
            )
    return parts, sections


def _slice_lines(text: str, start: int, stops: list[int], stop_at_instructions: bool) -> str:
    lines = text.splitlines()
    end = min([s for s in stops if s > start], default=len(lines))
    for j in range(start + 1, end):
        if lines[j].startswith("[FR Doc") or (stop_at_instructions and _AMENDATORY_INSTRUCTION.match(lines[j])):
            end = j
            break
    while end > start + 1 and lines[end - 1].strip() in ("", "0"):
        end -= 1
    return "\n".join(lines[start:end]).strip()


def _labeled_paragraph(text: str, label: str) -> str | None:
    match = re.search(rf"^{label}:\s*(.*?)(?:\n\s*\n|\Z)", text, re.MULTILINE | re.DOTALL)
    return " ".join(match.group(1).split())[:1500] if match else None


def _page_of(text: str, offset: int, max_chars: int) -> dict[str, Any]:
    chunk = text[offset : offset + max_chars]
    end = offset + len(chunk)
    return {
        "text": chunk,
        "offset": offset,
        "total_chars": len(text),
        "next_offset": end if end < len(text) else None,
    }


def _resolve_fr_number(
    cursor: duckdb.DuckDBPyConnection, ref: str
) -> tuple[str | None, list[dict[str, Any]], str | None]:
    fr_match = FR_DOC_NUMBER_PATTERN.fullmatch(ref)
    if fr_match:
        return fr_match.group(1), [], None
    if not DOCKET_ID_PATTERN.fullmatch(ref):
        return (
            None,
            [],
            (
                f"Not a docket, document, or Federal Register number: {ref!r}. "
                "Try 'USBC-2026-0628', 'USBC-2026-0628-0001', or '2026-18481'."
            ),
        )
    rows = cursor.execute(
        "SELECT document_id, document_type, title, posted_date, fr_doc_num FROM documents "
        "WHERE (document_id = ? OR docket_id = ?) AND fr_doc_num IS NOT NULL AND fr_doc_num <> ''",
        [ref, ref],
    ).fetchall()
    candidates = []
    for document_id, document_type, title, posted, fr_doc_num in rows:
        number = FR_DOC_NUMBER_PATTERN.search(str(fr_doc_num))
        if number:
            candidates.append(
                {
                    "document_id": document_id,
                    "document_type": document_type,
                    "title": title,
                    "posted_date": _jsonify(posted),
                    "fr_doc_num": number.group(1),
                }
            )
    if not candidates:
        return (
            None,
            [],
            (
                f"No Federal Register notice is linked to {ref} in the mirror. "
                "Pass its Federal Register document number (e.g. '2026-18481') instead."
            ),
        )
    exact = [c for c in candidates if c["document_id"] == ref]
    if exact:
        return exact[0]["fr_doc_num"], [], None
    if "_FRDOC_" in ref.upper():
        return (
            None,
            [],
            (
                f"{ref} is an agency catch-all docket holding many unrelated notices. "
                "Pass the regulations.gov document ID or the Federal Register document number instead."
            ),
        )

    def priority(c: dict[str, Any]) -> int:
        kind = c["document_type"] or ""
        return RULE_TYPE_PRIORITY.index(kind) if kind in RULE_TYPE_PRIORITY else len(RULE_TYPE_PRIORITY)

    candidates.sort(key=lambda c: c["posted_date"] or "", reverse=True)
    candidates.sort(key=priority)
    return candidates[0]["fr_doc_num"], candidates[1:10], None


def _get_rule_text(
    cursor: duckdb.DuckDBPyConnection,
    document: str,
    section: str,
    offset: int,
    max_chars: int,
    fetch: Callable[[str, str], str] | None = None,
) -> dict[str, Any]:
    if max_chars < 500 or max_chars > RULE_TEXT_MAX_CHARS:
        return {"error": f"max_chars must be between 500 and {RULE_TEXT_MAX_CHARS}"}
    if offset < 0:
        return {"error": "offset must be 0 or more"}

    number, others, error = _resolve_fr_number(cursor, document.strip())
    if error:
        return {"error": error}

    row = cursor.execute(
        "SELECT document_number, title, document_type, publication_date, comments_close_on, abstract, "
        "cfr_references_json, regulation_id_numbers_json, html_url "
        "FROM federal_register WHERE document_number = ? LIMIT 1",
        [number],
    ).fetchone()
    if row is None:
        return {"error": f"Federal Register document {number} is not in the mirror's federal_register table."}
    number, title, doc_type, published, closes, abstract, cfr_json, rin_json, html_url = row
    published = str(published or "")[:10]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", published):
        return {"error": f"Federal Register document {number} has no usable publication date."}

    result: dict[str, Any] = {
        "fr_document_number": number,
        "title": title,
        "document_type": doc_type,
        "publication_date": published,
        "comments_close_on_in_notice": _jsonify(closes),
        "cfr_references": _json_list(cfr_json),
        "rins": _json_list(rin_json),
        "federal_register_url": html_url,
        "source": GOVINFO_FR_URL.format(date=published, number=number),
    }
    if others:
        result["other_notices_in_docket"] = others

    try:
        text = (fetch or _fetch_rule_text)(number, published)
    except (RuleTextError, httpx.HTTPError) as exc:
        result["error"] = f"Could not fetch the text from govinfo: {exc}. Read it at federal_register_url."
        return result

    parts, sections = _rule_structure(text)
    wanted = section.strip()
    if not wanted:
        result.update(
            {
                "mode": "outline",
                "summary": abstract or _labeled_paragraph(text, "SUMMARY"),
                "dates": _labeled_paragraph(text, "DATES"),
                "parts": [{"part": p["part"], "title": p["title"]} for p in parts],
                "sections": [{"section": s["section"], "title": s["title"], "part": s["part"]} for s in sections],
                "total_chars": len(text),
                "next_step": (
                    "Call again with section='<number>' (e.g. '"
                    + (sections[0]["section"] if sections else "60.1")
                    + "') for one section, 'part <n>' for a whole part, or 'full' for the whole document in pages."
                    if sections
                    else "This document has no regulatory text sections; call again with section='full' to read it."
                ),
            }
        )
        return result

    stops = [p["line"] for p in parts] + [s["line"] for s in sections]
    part_arg = _PART_ARG.match(wanted)
    section_arg = _SECTION_ARG.match(wanted)
    if wanted.lower() == "full":
        body = text
    elif part_arg:
        match = next((p for p in parts if p["part"] == part_arg.group(1)), None)
        if match is None:
            result["error"] = f"No part {part_arg.group(1)} in this document. Parts: {[p['part'] for p in parts]}"
            return result
        body = _slice_lines(text, match["line"], [p["line"] for p in parts], stop_at_instructions=False)
    elif section_arg:
        match = next((s for s in sections if s["section"] == section_arg.group(1)), None)
        if match is None:
            result["error"] = (
                f"No section {section_arg.group(1)} in this document. Sections: {[s['section'] for s in sections][:60]}"
            )
            return result
        body = _slice_lines(text, match["line"], stops, stop_at_instructions=True)
    else:
        result["error"] = "section must be empty (outline), a section number like '60.4', 'part 60', or 'full'."
        return result

    result["section"] = wanted
    result.update(_page_of(body, offset, max_chars))
    return result


def _json_list(raw: Any) -> list[Any]:
    try:
        value = json.loads(raw) if raw else []
    except (TypeError, ValueError):
        return []
    return value if isinstance(value, list) else []


def _register_tools(mcp: FastMCP) -> None:
    @mcp.tool()
    @_observed("action")
    def get_data_freshness(table: str = "federal_register") -> dict[str, Any]:
        """Read one table's artifact timestamp and observed publication/update watermark.

        Artifact Last-Modified is not an ingestion timestamp or proof of complete
        source coverage. Unknown ingestion timestamps remain null. Each table is
        checked independently so a failed table does not hide other results.
        """
        if table not in TABLES:
            return {"error": "Unknown table", "available_tables": list(TABLES)}
        result: dict[str, Any] = {
            "table": table, "checked_at": datetime.now(timezone.utc).isoformat(),
            "ingested_at": None, "coverage_complete_through": None,
            "artifact_last_modified": None, "watermark": None,
            "limitations": "Observed maximum dates do not prove complete coverage; artifact modification is not ingestion.",
        }
        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.head(f"{R2_BASE_URL}/{table}.parquet")
                response.raise_for_status()
                result["artifact_last_modified"] = response.headers.get("last-modified")
                result["artifact_etag"] = response.headers.get("etag")
                result["ingested_at"] = response.headers.get("x-amz-meta-ingested-at")
        except httpx.HTTPError as exc:
            result["artifact_error"] = type(exc).__name__
        column = {"federal_register": "publication_date", "documents": "modify_date",
                  "dockets": "modify_date"}.get(table)
        if column:
            try:
                cursor = _get_connection().cursor()
                with _statement_timeout(cursor):
                    row = cursor.execute(
                        f"SELECT MAX(TRY_CAST({column} AS TIMESTAMP)) FROM {table}"
                    ).fetchone()
                result["watermark"] = {"column": column, "latest": _jsonify(row[0]) if row else None}
            except Exception as exc:
                result["watermark_error"] = type(exc).__name__
        return result

    @mcp.tool()
    @_observed("action")
    def list_sources() -> dict[str, Any]:
        """List the logical tables available in the Spicy Regs R2 dataset."""
        return {
            "source": "r2",
            "base_url": R2_BASE_URL,
            "tables": list(TABLES),
        }

    @mcp.tool()
    @_observed("action")
    def describe_table(table: str) -> dict[str, Any]:
        """Return the column schema for one Spicy Regs table.

        Call list_sources for the set of valid table names; an unknown name
        returns them in the error payload.
        """
        if table not in TABLES:
            return {
                "error": f"Unknown table '{table}'",
                "available_tables": list(TABLES),
            }
        cursor = _get_connection().cursor()
        with _statement_timeout(cursor):
            rows = cursor.execute(f"DESCRIBE {table}").fetchall()
        return {
            "table": table,
            "columns": [
                {
                    "column_name": row[0],
                    "column_type": row[1],
                    "null": row[2],
                    "key": row[3],
                    "default": row[4],
                }
                for row in rows
            ],
        }

    @mcp.tool()
    @_observed("action")
    def query_sql(sql: str, max_rows: int = 25) -> dict[str, Any]:
        """Run a read-only SQL query against the Spicy Regs R2 tables and return up to max_rows rows.

        Only SELECT and EXPLAIN run; DESCRIBE, SHOW, SUMMARIZE, VALUES and the
        FROM-first shorthand are accepted as SELECT. Statements that write
        (COPY TO, ATTACH, CREATE, INSERT, DROP, EXPORT, SET, ...) are refused.
        The connection is in-memory and read-only against R2. One view exists per
        table listed by list_sources. Always include a LIMIT in exploratory
        queries; results past max_rows are dropped.
        """
        if max_rows <= 0 or max_rows > 500:
            return {"error": "max_rows must be between 1 and 500"}

        cursor = _get_connection().cursor()
        write_statement = _first_write_statement(cursor, sql)
        if write_statement is not None:
            return {"error": f"query_sql is read-only; refusing {write_statement} statement"}

        with _statement_timeout(cursor):
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchmany(max_rows)
        result_rows = [{col: _jsonify(val) for col, val in zip(columns, row)} for row in rows]
        return {
            "source": "r2",
            "base_url": R2_BASE_URL,
            "columns": columns,
            "row_count_shown": len(result_rows),
            "max_rows": max_rows,
            "rows": result_rows,
        }

    @mcp.tool()
    @_observed("action")
    def prepare_comment(docket_id: str, draft: str = "") -> dict[str, Any]:
        """Get ready to comment on a docket: find where to submit, and check a draft.

        Returns the docket's documents that are open for public comment right
        now, each with its deadline, days left, and the regulations.gov
        comment_url where the person submits. If draft is given, also reports
        whether it fits the 5,000-character comment box.

        This tool never submits anything. The person reviews their comment and
        submits it themselves at comment_url. Help them write in their own
        words; agencies weigh the substance of comments, not their number.
        """
        cursor = _get_connection().cursor()
        with _statement_timeout(cursor):
            return _prepare_comment(cursor, docket_id, draft, datetime.now(timezone.utc))

    @mcp.tool()
    @_observed("action")
    def get_rule_text(
        document: str,
        section: str = "",
        offset: int = 0,
        max_chars: int = RULE_TEXT_DEFAULT_CHARS,
    ) -> dict[str, Any]:
        """Read the text of a proposed or final rule as published in the Federal Register.

        document is a docket ID ('USBC-2026-0628'), a regulations.gov document
        ID ('USBC-2026-0628-0001'), or a Federal Register document number
        ('2026-18481'). For a docket, the proposed or final rule is picked
        first; the docket's other notices are listed so you can pick another.

        Call it first with no section to get the outline: title, summary,
        dates, CFR parts, and every regulatory section with its title. Then
        pass section='60.4' for one section, 'part 60' for a whole part, or
        'full' for the whole document. Long text comes back in pages: call
        again with offset=next_offset until next_offset is null.

        Quote the section numbers you rely on. The text is GPO's official
        Federal Register copy on govinfo.gov.
        """
        cursor = _get_connection().cursor()
        with _statement_timeout(cursor):
            return _get_rule_text(cursor, document, section, offset, max_chars)


def build_server() -> FastMCP:
    mcp = FastMCP("spicy-regs", instructions=INSTRUCTIONS, icons=ICONS)
    _register_tools(mcp)
    return mcp


STATIC_DIR = Path(__file__).parent / "static"


@lru_cache(maxsize=1)
def _landing_page() -> bytes:
    """The setup page served at /, with its view list rendered from TABLES.

    The list used to be hand-maintained in the HTML and had drifted seven
    tables behind by the time this page was restored; substituting it here
    keeps the page honest as TABLES grows.
    """
    views = " ·\n        ".join(f'<code class="inline">{table}</code>' for table in TABLES)
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    return html.replace("<!--VIEWS-->", views).encode("utf-8")


@lru_cache(maxsize=1)
def _landing_icon() -> bytes:
    """Decoded from ICON_DATA_URI so the PNG has exactly one home in the package."""
    return base64.b64decode(ICON_DATA_URI.split(",", 1)[1])


def _register_landing_page(mcp: FastMCP) -> None:
    """Serve the human-facing setup page alongside the MCP endpoint.

    Vercel served this as a static file at the site root; when that deploy was
    retired the page went with it, leaving mcp.spicy-regs.dev/ a bare 404. It
    lives in the canonical server now so every host (Cloud Run, the Cloudflare
    container) gets it without host-specific static-file config.
    """

    @mcp.custom_route("/", methods=["GET"])
    async def landing(_request: Request) -> Response:
        return Response(
            _landing_page(),
            media_type="text/html; charset=utf-8",
            headers={"Cache-Control": "public, max-age=300"},
        )

    @mcp.custom_route("/icon.png", methods=["GET"])
    async def icon(_request: Request) -> Response:
        return Response(
            _landing_icon(),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400"},
        )


def build_app():
    mcp = FastMCP(
        "spicy-regs",
        instructions=INSTRUCTIONS,
        icons=ICONS,
        stateless_http=True,
        streamable_http_path="/mcp",
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )
    _register_tools(mcp)
    _register_landing_page(mcp)
    return mcp.streamable_http_app()


def main() -> None:
    build_server().run()


if __name__ == "__main__":
    main()
