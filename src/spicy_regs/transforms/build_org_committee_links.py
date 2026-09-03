"""Transform: build the commenter-organization ↔ FEC committee link table.

Materializes the name bridge that ``docs/index.md`` describes but nothing
implemented: *"Organization name bridges the softer influence sources —
``lobbying_filings`` (registrant/client), ``fec_committees``, and comment filers
— where no shared id exists."* Until now that join was left to whoever wrote the
query, so every consumer re-invented its own normalization (and got different
answers). This rollup does it once, publishes the result, and shows its work in
``match_method`` / ``confidence`` so a consumer can pick a precision bar instead
of trusting an opaque score.

**Grain.** One row per (``organization`` string as filed on a comment,
``committee_id``). Not one row per comment: the same organization string is
reused across many comments, so the org string is the natural unit of
resolution. ``comment_count`` / ``docket_count`` / ``agency_codes_json`` carry
the corpus-side weight, and ``organization`` joins straight back to
``comments.organization``.

**Coverage is small, and that is the upstream reality, not a bug.**
``comments.organization`` is populated on ~20.7K of ~25.8M comments (0.08%) —
regulations.gov only captures it when a submitter fills the field in, and mass
comment campaigns leave it blank. Of the ~14.2K distinct organization strings,
~12.7K clear the junk guards and a few hundred resolve to a committee. Most
commenting organizations (hospitals, clinics, local firms) simply do not run a
federal PAC, so a low match rate is the correct answer rather than a matcher to
tune harder. ``name_source`` is stamped on every row so that names recovered
from other fields later (comment title, letterhead, signature block) can be
added as extra rows without breaking existing consumers.

**Matching.** Both sides are normalized the same way (uppercase, drop
parenthetical asides and apostrophes, ``&`` → ``AND``, punctuation → space), then
the committee side is stripped of PAC decorations (``... POLITICAL ACTION
COMMITTEE``, ``... PAC``, ``... GOOD GOVERNMENT FUND``, ...) to recover the
sponsoring organization's name. Three tiers, strongest first:

``exact``
    Full normalized names are equal. Rare, and unambiguous.
``core``
    The decoration-stripped cores are equal — the workhorse.
    ``NATIONAL ASSOCIATION OF REALTORS POLITICAL ACTION COMMITTEE`` → core
    ``NATIONAL ASSOCIATION OF REALTORS`` matches the commenter of that name.
``prefix``
    The committee core *starts with* the whole organization core on a token
    boundary — ``AMERICAN PHYSICAL THERAPY ASSOCIATION`` → ``AMERICAN PHYSICAL
    THERAPY ASSOCIATION PHYSICAL THERAPY POLITICAL ACTION COMMITTEE``. Matched
    by equality against pre-generated committee token prefixes rather than a
    ``LIKE`` nested loop, which would be ~1.3B comparisons.

**Fan-out is a signal, not an error.** One organization legitimately matching
many committees is usually a real affiliate network: *Planned Parenthood* hits
~90 state affiliate committees, SEIU ~16 locals, IBEW ~15. Truncating those
would discard true positives, so every row instead carries
``committee_match_count`` and a ``confidence`` that degrades with fan-out —
consumers filter on the bar they need.

**Junk guards.** Free-text ``organization`` is full of non-organizations. A core
must be ≥ :data:`MIN_CORE_LENGTH` chars and ≥ :data:`MIN_CORE_TOKENS` tokens
(which also blocks bare acronyms like ``NRDC`` from matching wildly), and must
not be a :data:`GENERIC_ORG_CORES` entry. That blocklist is matched on the
*whole* core only, so blocking ``NEW MEXICO`` (a commenter's state, which
otherwise prefix-matched 51 committees) still leaves ``NEW MEXICO CATTLE
GROWERS ASSOCIATION`` free to match.

**Reading comments.** ``comments.parquet`` is ~3.3 GB of mostly comment text,
and this transform needs five narrow columns from it. Downloading the whole
monolith to a rollup runner to extract them would dominate the job, so comments
are read straight from the public R2 URL over ``httpfs``: Parquet projection
pushdown fetches only the column chunks named in the SELECT. A local
``comments.parquet`` in ``output_dir`` is preferred when present, so local dev
and a primed run work without touching the network. ``fec_committees.parquet``
is small and is primed to disk by the pipeline as usual.

Comment rows are deduplicated on ``comment_id`` (newest ``modify_date`` wins),
matching the MCP server's ``comments`` view so counts here agree with counts a
consumer computes there.
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow.parquet as pq
from loguru import logger

OUTPUT = "org_committee_links.parquet"

DEFAULT_R2_BASE_URL = "https://data.spicy-regs.dev"

# The published schema, in a fixed order.
COLUMNS: tuple[tuple[str, str], ...] = (
    ("organization", "VARCHAR"),
    ("organization_norm", "VARCHAR"),
    ("organization_core", "VARCHAR"),
    ("name_source", "VARCHAR"),
    ("committee_id", "VARCHAR"),
    ("committee_name", "VARCHAR"),
    ("committee_type_full", "VARCHAR"),
    ("designation_full", "VARCHAR"),
    ("party_full", "VARCHAR"),
    ("organization_type_full", "VARCHAR"),
    ("committee_state", "VARCHAR"),
    ("match_method", "VARCHAR"),
    ("confidence", "VARCHAR"),
    ("committee_match_count", "BIGINT"),
    ("comment_count", "BIGINT"),
    ("docket_count", "BIGINT"),
    ("agency_codes_json", "VARCHAR"),
    ("first_comment_date", "VARCHAR"),
    ("last_comment_date", "VARCHAR"),
)

# Where a matched name came from. Only the structured field today; text-derived
# names (title, letterhead, signature block) would arrive as additional rows
# under their own source rather than displacing these.
NAME_SOURCE_ORGANIZATION_FIELD = "organization_field"

# An organization core shorter/thinner than this is too generic to match on.
# Two tokens also keeps bare acronyms ("NRDC", "NASW") out of the core/prefix
# tiers, where a three-letter string would match unrelated committees.
MIN_CORE_LENGTH = 8
MIN_CORE_TOKENS = 2

# Prefix tier: compare the first 2..8 tokens of a committee core. Beyond eight
# tokens the remaining suffix is long enough that the match is no longer a
# recognizable "<org> <decoration>" shape.
MAX_PREFIX_TOKENS = 8

# Fan-out above which a prefix match is demoted to "low". Real affiliate
# networks exceed it (Planned Parenthood ~90, SEIU ~16) and are kept — the
# confidence label is what tells them apart from an over-broad core.
PREFIX_FANOUT_MEDIUM_MAX = 5

# Trailing committee decorations, stripped from the FEC side to recover the
# sponsoring organization's name. Longest phrases first so the alternation
# prefers them, and applied over several passes to peel stacked suffixes
# ("... POLITICAL ACTION COMMITTEE FUND").
_DECORATIONS = (
    "POLITICAL ACTION COMMITTEE",
    "POLITICAL ACTION CMTE",
    "POLITICAL PARTICIPATION FUND",
    "SEPARATE SEGREGATED FUND",
    "POLITICAL EDUCATION COMMITTEE",
    "GOOD GOVERNMENT COMMITTEE",
    "GOOD GOVERNMENT FUND",
    "POLITICAL ACTION FUND",
    "POLITICAL COMMITTEE",
    "POLITICAL ACTION",
    "POLITICAL FUND",
    "FEDERAL PAC",
    "PAC FUND",
    "PAC",
    "COMMITTEE",
    "FUND",
    "EMPLOYEES",
    "EMPLOYEE",
    "VOLUNTARY",
    "FEDERAL",
    "POLITICAL",
)
_DECORATION_PASSES = 3

# Corporate/legal suffixes stripped from both sides. Deliberately excludes the
# ambiguous short ones (CO, PC, PA, US) that also occur as real name tokens.
_LEGAL_SUFFIXES = (
    "INCORPORATED",
    "CORPORATION",
    "COMPANY",
    "LIMITED",
    "PLLC",
    "CORP",
    "INC",
    "LLC",
    "LLP",
    "LTD",
)
_LEGAL_PASSES = 2

# Cores that are never an organization. Matched against the *whole* core, so a
# blocked entry never suppresses a real name that merely starts with it.
# Single-token entries are already excluded by MIN_CORE_TOKENS; the multi-token
# ones are what matter (each was an observed false positive or is an obvious
# near-miss of one).
GENERIC_ORG_CORES: frozenset[str] = frozenset(
    {
        # Self-descriptions in place of an organization.
        "SELF EMPLOYED",
        "SELF ONLY",
        "MYSELF ONLY",
        "PRIVATE CITIZEN",
        "PRIVATE INDIVIDUAL",
        "PRIVATE PERSON",
        "PRIVATE PRACTICE",
        "PRIVATE SECTOR",
        "CONCERNED CITIZEN",
        "CONCERNED CITIZENS",
        "INDIVIDUAL CITIZEN",
        "SENIOR CITIZEN",
        "US CITIZEN",
        "GENERAL PUBLIC",
        "THE PUBLIC",
        "WE THE PEOPLE",
        "BUSINESS OWNER",
        "SMALL BUSINESS OWNER",
        "RETIRED TEACHER",
        "NO ORGANIZATION",
        "NOT AFFILIATED",
        "NOT APPLICABLE",
        "NONE OF THE ABOVE",
        # Government employers. A commenter naming their employer is not an
        # organization→PAC link, and these prefix-match aggressively.
        "UNITED STATES",
        "UNITED STATES SENATE",
        "UNITED STATES CONGRESS",
        "UNITED STATES GOVERNMENT",
        "UNITED STATES HOUSE OF REPRESENTATIVES",
        "US SENATE",
        "US CONGRESS",
        "US GOVERNMENT",
        "US HOUSE OF REPRESENTATIVES",
        "HOUSE OF REPRESENTATIVES",
        "FEDERAL GOVERNMENT",
        "STATE GOVERNMENT",
        "LOCAL GOVERNMENT",
        "US MILITARY",
        # Multi-token states/territories a commenter gives as their location.
        # (Single-token ones fall to MIN_CORE_TOKENS.)
        "NEW HAMPSHIRE",
        "NEW JERSEY",
        "NEW MEXICO",
        "NEW YORK",
        "NORTH CAROLINA",
        "NORTH DAKOTA",
        "RHODE ISLAND",
        "SOUTH CAROLINA",
        "SOUTH DAKOTA",
        "WEST VIRGINIA",
        "DISTRICT OF COLUMBIA",
        "PUERTO RICO",
        "VIRGIN ISLANDS",
        "AMERICAN SAMOA",
        "NORTHERN MARIANA ISLANDS",
    }
)


def _sql_str(value: str) -> str:
    """Render a Python string as a single-quoted SQL literal."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _normalize(expr: str) -> str:
    """SQL for the shared normalization applied to both sides of the join.

    Uppercase, drop parenthetical asides (``(BANKPAC)``, ``(A.K.A. ...)``) and
    apostrophes (straight and curly — the FEC data contains both, and one row
    even doubles them), expand ``&``, then collapse every other run of
    non-alphanumerics to a single space.
    """
    out = f"upper({expr})"
    out = f"regexp_replace({out}, '\\([^)]*\\)', ' ', 'g')"
    out = f"regexp_replace({out}, '''', '', 'g')"
    out = f"replace({out}, '’', '')"
    out = f"regexp_replace({out}, '&', ' AND ', 'g')"
    out = f"regexp_replace({out}, '[^A-Z0-9]+', ' ', 'g')"
    return f"trim({out})"


def _strip_suffixes(expr: str, suffixes: tuple[str, ...], passes: int) -> str:
    """SQL that peels trailing ``suffixes`` off ``expr``, ``passes`` times."""
    pattern = " (" + "|".join(suffixes) + ")$"
    out = expr
    for _ in range(passes):
        out = f"regexp_replace({out}, {_sql_str(pattern)}, '')"
    return out


def _core(expr: str, *, decorations: bool) -> str:
    """SQL for the comparable core of a normalized name.

    Strips legal suffixes and a leading ``THE`` from both sides; ``decorations``
    additionally peels the committee-only PAC suffixes.
    """
    out = _strip_suffixes(expr, _LEGAL_SUFFIXES, _LEGAL_PASSES)
    if decorations:
        out = _strip_suffixes(out, _DECORATIONS, _DECORATION_PASSES)
        # A decoration can uncover another legal suffix beneath it
        # ("... INC POLITICAL ACTION COMMITTEE" → "... INC").
        out = _strip_suffixes(out, _LEGAL_SUFFIXES, _LEGAL_PASSES)
    out = f"regexp_replace({out}, '^THE ', '')"
    return f"trim({out})"


def _resolve_comments_source(output_dir: Path) -> str:
    """Return the ``read_parquet`` target for comments: local file, else R2 URL.

    A local ``comments.parquet`` (a primed run, or a developer's copy) wins so
    the transform never reaches the network unnecessarily. Otherwise the public
    bucket is read directly and projection pushdown keeps the transfer to the
    handful of columns this query names.
    """
    local = output_dir / "comments.parquet"
    if local.exists():
        logger.info("org_committee_links: using local {}", local)
        return str(local)

    base_url = os.environ.get("R2_PUBLIC_URL") or DEFAULT_R2_BASE_URL
    base_url = base_url.rstrip("/")
    if not base_url.startswith("https://"):
        raise RuntimeError(f"R2_PUBLIC_URL must be an https:// URL, got {base_url!r}")
    if any(c in base_url for c in ("'", "\\", "\x00", "\n", "\r")):
        raise RuntimeError(f"R2_PUBLIC_URL contains illegal characters: {base_url!r}")
    url = f"{base_url}/comments.parquet"
    logger.info("org_committee_links: reading comments remotely from {} (column projection)", url)
    return url


def build_query(comments_source: str, committees_file: str, out_file: str) -> str:
    """Return the full COPY ... TO statement that materializes the link table.

    Split out from :func:`build_org_committee_links` so the exact published SQL
    can be inspected and exercised without a DuckDB connection or R2 access.
    """
    # Escape the interpolated paths the way `data_dictionary.discover_schemas`
    # does; the remote URL is separately validated in _resolve_comments_source.
    comments_source = comments_source.replace("'", "''")
    committees_file = committees_file.replace("'", "''")

    org_norm = _normalize("r.organization")
    org_core = _core("o.organization_norm", decorations=False)
    cm_norm = _normalize("f.name")
    cm_core = _core("n.committee_norm", decorations=True)
    blocklist = ", ".join(_sql_str(entry) for entry in sorted(GENERIC_ORG_CORES))

    return f"""
    COPY (
        WITH comment_orgs AS (
            -- Dedup on comment_id the same way the MCP `comments` view does, so
            -- counts published here agree with counts computed there.
            SELECT
                c.organization AS organization,
                c.docket_id AS docket_id,
                c.agency_code AS agency_code,
                c.posted_date AS posted_date
            FROM read_parquet('{comments_source}') c
            WHERE c.organization IS NOT NULL
              AND trim(c.organization) <> ''
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY c.comment_id ORDER BY c.modify_date DESC NULLS LAST
            ) = 1
        ),
        orgs_rolled AS (
            SELECT
                organization,
                count(*)::BIGINT AS comment_count,
                count(DISTINCT docket_id)::BIGINT AS docket_count,
                to_json(list_sort(list(DISTINCT agency_code)
                    FILTER (WHERE agency_code IS NOT NULL))) AS agency_codes_json,
                min(posted_date) AS first_comment_date,
                max(posted_date) AS last_comment_date
            FROM comment_orgs
            GROUP BY organization
        ),
        orgs_normed AS (
            SELECT r.*, {org_norm} AS organization_norm
            FROM orgs_rolled r
        ),
        orgs AS (
            SELECT o.*, {org_core} AS organization_core
            FROM orgs_normed o
        ),
        orgs_eligible AS (
            SELECT * FROM orgs
            WHERE length(organization_core) >= {MIN_CORE_LENGTH}
              AND len(string_split(organization_core, ' ')) >= {MIN_CORE_TOKENS}
              AND organization_core NOT IN ({blocklist})
        ),
        committees_normed AS (
            SELECT
                f.committee_id,
                f.name AS committee_name,
                f.committee_type_full,
                f.designation_full,
                f.party_full,
                f.organization_type_full,
                f.state AS committee_state,
                {cm_norm} AS committee_norm
            FROM read_parquet('{committees_file}') f
            WHERE f.committee_id IS NOT NULL AND f.name IS NOT NULL
        ),
        committees AS (
            SELECT n.*, {cm_core} AS committee_core
            FROM committees_normed n
        ),
        -- Token prefixes of each committee core, so the prefix tier is an
        -- equality (hash) join instead of a 14K x 89K LIKE nested loop.
        committee_prefixes AS (
            SELECT t.committee_id, array_to_string(t.tokens[1:g.k], ' ') AS prefix_key
            FROM (
                SELECT committee_id, string_split(committee_core, ' ') AS tokens
                FROM committees
            ) t,
                 generate_series({MIN_CORE_TOKENS}, {MAX_PREFIX_TOKENS}) AS g(k)
            WHERE g.k < len(t.tokens)
        ),
        matched AS (
            SELECT o.organization, c.committee_id, 'exact' AS match_method, 1 AS method_rank
            FROM orgs_eligible o
            JOIN committees c ON o.organization_norm = c.committee_norm
            UNION ALL
            SELECT o.organization, c.committee_id, 'core', 2
            FROM orgs_eligible o
            JOIN committees c ON o.organization_core = c.committee_core
            UNION ALL
            SELECT o.organization, p.committee_id, 'prefix', 3
            FROM orgs_eligible o
            JOIN committee_prefixes p ON o.organization_core = p.prefix_key
        ),
        -- One row per (organization, committee), keeping the strongest tier.
        best AS (
            SELECT organization, committee_id, match_method
            FROM matched
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY organization, committee_id ORDER BY method_rank
            ) = 1
        ),
        counted AS (
            SELECT b.*,
                   count(*) OVER (PARTITION BY b.organization)::BIGINT AS committee_match_count
            FROM best b
        )
        -- Every column is cast explicitly so the published schema is pinned by
        -- this query rather than inherited from the input parquet (an all-null
        -- passthrough column would otherwise read back as INTEGER).
        SELECT
            o.organization::VARCHAR AS organization,
            o.organization_norm::VARCHAR AS organization_norm,
            o.organization_core::VARCHAR AS organization_core,
            {_sql_str(NAME_SOURCE_ORGANIZATION_FIELD)}::VARCHAR AS name_source,
            c.committee_id::VARCHAR AS committee_id,
            c.committee_name::VARCHAR AS committee_name,
            c.committee_type_full::VARCHAR AS committee_type_full,
            c.designation_full::VARCHAR AS designation_full,
            c.party_full::VARCHAR AS party_full,
            c.organization_type_full::VARCHAR AS organization_type_full,
            c.committee_state::VARCHAR AS committee_state,
            m.match_method::VARCHAR AS match_method,
            CASE
                WHEN m.match_method IN ('exact', 'core') THEN 'high'
                WHEN m.committee_match_count <= {PREFIX_FANOUT_MEDIUM_MAX} THEN 'medium'
                ELSE 'low'
            END::VARCHAR AS confidence,
            m.committee_match_count::BIGINT AS committee_match_count,
            o.comment_count::BIGINT AS comment_count,
            o.docket_count::BIGINT AS docket_count,
            o.agency_codes_json::VARCHAR AS agency_codes_json,
            o.first_comment_date::VARCHAR AS first_comment_date,
            o.last_comment_date::VARCHAR AS last_comment_date
        FROM counted m
        JOIN orgs_eligible o ON o.organization = m.organization
        JOIN committees c ON c.committee_id = m.committee_id
        -- Sorted by organization so `WHERE organization = ?` prunes row groups.
        ORDER BY o.organization, m.match_method, c.committee_id
    ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
    """


def build_org_committee_links(output_dir: Path) -> Path:
    """Build ``org_committee_links.parquet`` (commenter org → FEC committee)."""
    import duckdb

    committees_file = output_dir / "fec_committees.parquet"
    if not committees_file.exists():
        raise FileNotFoundError(f"fec_committees.parquet not found in {output_dir}")

    comments_source = _resolve_comments_source(output_dir)
    out_file = output_dir / OUTPUT

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")
    if comments_source.startswith("https://"):
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")

    logger.info("Building org ↔ committee links via DuckDB...")
    con.execute(build_query(comments_source, str(committees_file), str(out_file)))
    con.close()

    rows = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("org_committee_links: {:,} rows", rows)
    return out_file
