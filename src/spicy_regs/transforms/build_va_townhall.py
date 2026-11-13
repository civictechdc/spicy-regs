"""Transform: build ``state_actions``, ``state_stages`` and ``state_comments`` from Virginia's Town Hall.

These are the first state-level rulemaking tables. Each has a ``jurisdiction``
column (``VA`` for now) so later states can add rows to the same tables. They
are kept apart from ``dockets`` / ``documents`` / ``comments`` because those
assume federal keys (``agency_code``, ``docket_id``, RIN), and existing queries
and MCP tools should keep working unchanged.

The crawl and the page formats are documented in
:mod:`spicy_regs.sources.va_townhall`.

Incremental and resumable
-------------------------
A full crawl takes about two days at the site's 5-second crawl delay, longer
than any one job. So each run:

1. Loads the prior tables (local copy in ``output_dir`` if present, else R2)
   and the crawl cursor from :data:`STATE_KEY`.
2. Crawls within ``max_minutes``: a refresh of open forums and in-progress
   stages, then a backfill from the cursor, skipping IDs already stored.
3. Merges the new rows into the prior tables, deduplicating on each table's key
   and preferring the newly fetched row, and writes the new cursor.

The caller publishes the three tables and then the cursor, so a failed upload
never advances the cursor past data that was not published. If the site goes
down mid-run, the crawl stops, the build still writes what it fetched, and
:attr:`VaTownHallBuild.error` is set so the caller can publish and then fail.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.va_townhall import (
    BASE_URL,
    FORUM_STAGE,
    JURISDICTION,
    KnownState,
    StoredStage,
    VaTownHallReader,
)

ACTIONS_OUTPUT = "state_actions.parquet"
STAGES_OUTPUT = "state_stages.parquet"
COMMENTS_OUTPUT = "state_comments.parquet"
OUTPUTS = (ACTIONS_OUTPUT, STAGES_OUTPUT, COMMENTS_OUTPUT)

#: R2 key (and local file name under ``output_dir``) of the crawl cursor.
STATE_KEY = "crawl_state/va_townhall.json"

#: Default crawl budget. The workflow's job timeout is 330 minutes; this leaves
#: room for downloading the priors, merging and uploading.
DEFAULT_MAX_MINUTES = 270

#: Fail the run when more pages than this share fail to parse: the site's
#: markup has probably changed.
MAX_PARSE_FAILURE_RATE = 0.02
MIN_PARSE_FAILURES_TO_FAIL = 10

ACTION_SCHEMA = pa.schema(
    [
        ("jurisdiction", pa.string()),
        ("action_id", pa.int64()),
        ("title", pa.string()),
        ("summary", pa.string()),
        ("agency", pa.string()),
        ("agency_id", pa.int64()),
        ("board", pa.string()),
        ("board_id", pa.int64()),
        ("chapter_citation", pa.string()),
        ("chapter_title", pa.string()),
        ("chapter_id", pa.int64()),
        ("chapters_affected", pa.string()),
        ("withdrawn_date", pa.string()),
        ("stage_ids_json", pa.string()),
        ("url", pa.string()),
        ("fetched_at", pa.string()),
    ]
)

STAGE_SCHEMA = pa.schema(
    [
        ("jurisdiction", pa.string()),
        ("stage_id", pa.int64()),
        ("action_id", pa.int64()),
        ("stage_type", pa.string()),
        ("status", pa.string()),
        ("agency", pa.string()),
        ("board", pa.string()),
        ("chapter_citation", pa.string()),
        ("comment_start_date", pa.string()),
        ("comment_end_date", pa.string()),
        ("comment_count", pa.int64()),
        ("register_volume", pa.int64()),
        ("register_issue", pa.int64()),
        ("register_date", pa.string()),
        ("effective_date", pa.string()),
        ("documents_json", pa.string()),
        ("url", pa.string()),
        ("fetched_at", pa.string()),
    ]
)

COMMENT_SCHEMA = pa.schema(
    [
        ("jurisdiction", pa.string()),
        ("comment_id", pa.int64()),
        ("forum_type", pa.string()),
        ("forum_id", pa.int64()),
        ("stage_id", pa.int64()),
        ("commenter", pa.string()),
        ("organization", pa.string()),
        ("commenter_raw", pa.string()),
        ("title", pa.string()),
        ("comment", pa.string()),
        ("posted_date", pa.string()),
        ("posted_at", pa.string()),
        ("url", pa.string()),
        ("fetched_at", pa.string()),
    ]
)

#: (output, schema, dedup key, sort order) per table.
_TABLES = {
    "action": (ACTIONS_OUTPUT, ACTION_SCHEMA, ("jurisdiction", "action_id"), "jurisdiction, action_id DESC"),
    "stage": (STAGES_OUTPUT, STAGE_SCHEMA, ("jurisdiction", "stage_id"), "jurisdiction, stage_id DESC"),
    # Sorted by forum so `WHERE stage_id = ?` / `forum_id = ?` prunes row groups.
    "comment": (
        COMMENTS_OUTPUT,
        COMMENT_SCHEMA,
        ("jurisdiction", "comment_id"),
        "jurisdiction, forum_type, forum_id DESC, comment_id",
    ),
}


@dataclass
class VaTownHallBuild:
    """What one build wrote, and whether the crawl behind it fell short."""

    tables: list[Path]
    state_file: Path
    new_rows: dict[str, int] = field(default_factory=dict)
    error: str | None = None


# --------------------------------------------------------------------------- #
# Shaping
# --------------------------------------------------------------------------- #


def _shape_action(doc: dict, fetched_at: str) -> dict:
    return {
        "jurisdiction": JURISDICTION,
        "action_id": doc["action_id"],
        "title": doc.get("title"),
        "summary": doc.get("summary"),
        "agency": doc.get("agency"),
        "agency_id": doc.get("agency_id"),
        "board": doc.get("board"),
        "board_id": doc.get("board_id"),
        "chapter_citation": doc.get("chapter_citation"),
        "chapter_title": doc.get("chapter_title"),
        "chapter_id": doc.get("chapter_id"),
        "chapters_affected": doc.get("chapters_affected"),
        "withdrawn_date": doc.get("withdrawn_date"),
        "stage_ids_json": json.dumps(doc.get("stage_ids") or []),
        "url": f"{BASE_URL}ViewAction.cfm?actionid={doc['action_id']}",
        "fetched_at": fetched_at,
    }


def _shape_stage(doc: dict, fetched_at: str) -> dict:
    return {
        "jurisdiction": JURISDICTION,
        "stage_id": doc["stage_id"],
        "action_id": doc.get("action_id"),
        "stage_type": doc.get("stage_type"),
        "status": doc.get("status"),
        "agency": doc.get("agency"),
        "board": doc.get("board"),
        "chapter_citation": doc.get("chapter_citation"),
        "comment_start_date": doc.get("comment_start_date"),
        "comment_end_date": doc.get("comment_end_date"),
        "comment_count": doc.get("comment_count"),
        "register_volume": doc.get("register_volume"),
        "register_issue": doc.get("register_issue"),
        "register_date": doc.get("register_date"),
        "effective_date": doc.get("effective_date"),
        "documents_json": json.dumps(doc.get("documents") or []),
        "url": f"{BASE_URL}ViewStage.cfm?stageid={doc['stage_id']}",
        "fetched_at": fetched_at,
    }


def _shape_comment(doc: dict, fetched_at: str) -> dict:
    return {
        "jurisdiction": JURISDICTION,
        "comment_id": doc["comment_id"],
        "forum_type": doc.get("forum_type"),
        "forum_id": doc.get("forum_id"),
        "stage_id": doc.get("stage_id"),
        "commenter": doc.get("commenter"),
        "organization": doc.get("organization"),
        "commenter_raw": doc.get("commenter_raw"),
        "title": doc.get("title"),
        "comment": doc.get("comment"),
        "posted_date": doc.get("posted_date"),
        "posted_at": doc.get("posted_at"),
        "url": f"{BASE_URL}ViewComments.cfm?commentid={doc['comment_id']}",
        "fetched_at": fetched_at,
    }


_SHAPERS = {"action": _shape_action, "stage": _shape_stage, "comment": _shape_comment}


# --------------------------------------------------------------------------- #
# Priors and crawl state
# --------------------------------------------------------------------------- #


def _prior(output_dir: Path, name: str) -> Path | None:
    """The prior copy of ``name``: a local file from an earlier run, else R2, else None."""
    local = output_dir / name
    if local.exists():
        logger.info("Town Hall: using local {} as the prior table", name)
        return local
    prior = output_dir / f"_prior_{name}"
    if prior.exists() or r2.download(name, prior):
        return prior
    logger.info("Town Hall: no prior {}; starting it fresh", name)
    return None


def _load_state(output_dir: Path) -> tuple[Path, dict]:
    path = output_dir / STATE_KEY
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or r2.download(STATE_KEY, path):
        state = json.loads(path.read_text())
        logger.info("Town Hall: resuming from cursors {}", state.get("cursors"))
        return path, state
    logger.info("Town Hall: no crawl state; the backfill starts from the newest IDs")
    return path, {}


def _known(priors: dict[str, Path | None]) -> KnownState:
    """What the prior tables hold, so the crawl skips it."""
    import duckdb

    known = KnownState()
    if stages := priors.get(STAGES_OUTPUT):
        rows = duckdb.sql(
            f"SELECT stage_id, fetched_at, comment_count, status FROM read_parquet('{stages}')"
        ).fetchall()
        known.stages = {int(s): StoredStage(f, c, st) for s, f, c, st in rows}
    if actions := priors.get(ACTIONS_OUTPUT):
        known.actions = {int(a) for (a,) in duckdb.sql(f"SELECT action_id FROM read_parquet('{actions}')").fetchall()}
    if comments := priors.get(COMMENTS_OUTPUT):
        rows = duckdb.sql(
            f"SELECT forum_type, forum_id, count(*) FROM read_parquet('{comments}') GROUP BY ALL"
        ).fetchall()

        # A stage forum with fewer stored comments than its stage reports is
        # incomplete, so leave it out and let the crawl refetch it.
        def complete(forum_type: str, forum_id: int, stored: int) -> bool:
            stage = known.stages.get(forum_id) if forum_type == FORUM_STAGE else None
            return stage is None or stored >= (stage.comment_count or 0)

        known.forums = {(t, int(i)) for t, i, n in rows if complete(t, int(i), n)}
    logger.info(
        "Town Hall: prior tables hold {:,} stages, {:,} actions, {:,} forums with comments",
        len(known.stages),
        len(known.actions),
        len(known.forums),
    )
    return known


def _merge(output_dir: Path, kind: str, rows: list[dict], prior: Path | None) -> Path:
    """Write ``rows`` merged over ``prior`` (newest row wins per key) to the table's output file."""
    import duckdb

    name, schema, key, order = _TABLES[kind]
    out_file = output_dir / name
    new_file = output_dir / f"_new_{name}"
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), new_file, compression="zstd")

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(schema.names)
    sources = [f"SELECT {cols}, 1 AS _src, row_number() OVER () AS _ord FROM read_parquet('{new_file}')"]
    if prior is not None:
        sources.append(f"SELECT {cols}, 0 AS _src, 0 AS _ord FROM read_parquet('{prior}')")
    union = " UNION ALL BY NAME ".join(sources)
    tmp = output_dir / f"_merged_{name}"
    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM ({union})
            QUALIFY row_number() OVER (PARTITION BY {", ".join(key)} ORDER BY _src DESC, _ord DESC) = 1
            ORDER BY {order}
        ) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)
        """
    )
    con.close()
    tmp.replace(out_file)
    new_file.unlink()
    if prior is not None and prior != out_file and prior.name.startswith("_prior_"):
        prior.unlink()
    logger.info("Town Hall: wrote {} ({:,} new rows)", name, len(rows))
    return out_file


# --------------------------------------------------------------------------- #
# Build
# --------------------------------------------------------------------------- #


def build_va_townhall(
    output_dir: Path,
    *,
    max_minutes: float = DEFAULT_MAX_MINUTES,
    reader: VaTownHallReader | None = None,
) -> VaTownHallBuild:
    """Crawl Town Hall within ``max_minutes`` and merge the result into the three state tables."""
    output_dir.mkdir(parents=True, exist_ok=True)
    priors = {name: _prior(output_dir, name) for name in OUTPUTS}
    state_file, state = _load_state(output_dir)

    if reader is None:
        reader = VaTownHallReader(
            known=_known(priors),
            cursors=state.get("cursors"),
            deadline=time.monotonic() + max_minutes * 60,
        )

    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: dict[str, list[dict]] = {kind: [] for kind in _TABLES}
    for doc in reader.iter_records():
        kind = doc["kind"]
        rows[kind].append(_SHAPERS[kind](doc, fetched_at))

    tables = [_merge(output_dir, kind, rows[kind], priors[_TABLES[kind][0]]) for kind in _TABLES]

    state = {
        "jurisdiction": JURISDICTION,
        "cursors": reader.cursors,
        "backfill_complete": all(v <= 0 for v in reader.cursors.values()),
        "updated_at": fetched_at,
    }
    state_file.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")

    if reader.forum_count_mismatches:
        logger.warning(
            "Town Hall: {} forums printed a comment count that differs from the comments parsed, e.g. {}",
            len(reader.forum_count_mismatches),
            reader.forum_count_mismatches[:5],
        )

    error = None
    if reader.error is not None:
        error = f"crawl stopped early: {reader.error}"
    elif len(reader.parse_failures) >= max(MIN_PARSE_FAILURES_TO_FAIL, MAX_PARSE_FAILURE_RATE * reader.pages):
        error = (
            f"{len(reader.parse_failures)} of {reader.pages} pages failed to parse "
            f"(e.g. {reader.parse_failures[:5]}); has the Town Hall markup changed?"
        )
    new_rows = {_TABLES[kind][0]: len(rows[kind]) for kind in _TABLES}
    logger.info("Town Hall: new rows {}; state {}", new_rows, state)
    return VaTownHallBuild(tables=tables, state_file=state_file, new_rows=new_rows, error=error)
