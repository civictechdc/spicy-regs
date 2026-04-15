#!/usr/bin/env python3
# /// script
# dependencies = [
#   "duckdb>=1.2.0",
# ]
# ///
#
"""Detect likely duplicate or coordinated cross-agency regulations in Spicy Regs."""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from itertools import combinations
from pathlib import Path

R2_BASE_URL = "https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev"

STOPWORDS = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "by",
    "for",
    "from",
    "in",
    "into",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
    "under",
    "using",
    "act",
    "acts",
    "agency",
    "agencies",
    "federal",
    "rule",
    "rulemaking",
    "regulation",
    "regulations",
    "regulatory",
    "program",
    "programs",
    "notice",
    "notices",
    "request",
    "requests",
    "proposed",
    "proposal",
    "proposals",
    "final",
    "standards",
    "requirements",
    "implementation",
}

BOILERPLATE_PATTERNS = [
    r"^agency information collection",
    r"^submission for omb review",
    r"^proposed collection comment request$",
    r"^notice of request for (extension|revision)",
    r"^notice and request for comments$",
    r"^notice request for comments$",
    r"^semiannual regulatory agenda",
    r"^unified agenda",
    r"^privacy act",
    r"^meetings sunshine act",
    r"^ses performance review board",
    r"^senior executive service performance review board membership$",
    r"^civil monetary penalty inflation adjustment$",
    r"^civil monetary penalty adjustments for inflation$",
    r"^civil penalties inflation adjustments$",
    r"^freedom of information act$",
    r"^freedom of information act regulations$",
    r"^notice of proposed rulemaking$",
    r"^notice of proposed rulemaking .*request for comments$",
    r"^advance notice of proposed rulemaking.*request for comments$",
    r"^.*petition for rulemaking$",
    r"^.*request for comments$",
]

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
WHITESPACE_RE = re.compile(r"\s+")


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _import_duckdb():
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError:
        print(
            json.dumps(
                {
                    "error": "duckdb is not installed in the current Python environment",
                    "hint": "Run this helper as a standalone uv script, for example: uv run --script plugins/spicyregs/skills/spicyregs/scripts/find_duplicate_regulations.py --source r2",
                },
                indent=2,
            )
        )
        raise SystemExit(1)
    return duckdb


def _local_view_specs(output_dir: Path) -> dict[str, str]:
    specs: dict[str, str] = {}

    dockets = output_dir / "dockets.parquet"
    if dockets.exists():
        specs["dockets"] = f"SELECT * FROM read_parquet('{_escape_sql_string(str(dockets))}')"

    documents = output_dir / "documents.parquet"
    if documents.exists():
        specs["documents"] = f"SELECT * FROM read_parquet('{_escape_sql_string(str(documents))}')"

    comments = output_dir / "comments.parquet"
    if comments.exists():
        specs["comments"] = f"SELECT * FROM read_parquet('{_escape_sql_string(str(comments))}')"
    else:
        partitioned = output_dir / "comments"
        if partitioned.exists():
            specs["comments"] = (
                "SELECT * FROM read_parquet("
                f"'{_escape_sql_string(str(partitioned / '**' / '*.parquet'))}', "
                "union_by_name=true, hive_partitioning=true)"
            )

    comments_index = output_dir / "comments_index.parquet"
    if comments_index.exists():
        specs["comments_index"] = f"SELECT * FROM read_parquet('{_escape_sql_string(str(comments_index))}')"

    feed_summary = output_dir / "feed_summary.parquet"
    if feed_summary.exists():
        specs["feed_summary"] = f"SELECT * FROM read_parquet('{_escape_sql_string(str(feed_summary))}')"

    return specs


def _remote_view_specs(base_url: str) -> dict[str, str]:
    url = base_url.rstrip("/")
    return {
        "dockets": f"SELECT * FROM read_parquet('{_escape_sql_string(f'{url}/dockets.parquet')}')",
        "documents": f"SELECT * FROM read_parquet('{_escape_sql_string(f'{url}/documents.parquet')}')",
        "comments": f"SELECT * FROM read_parquet('{_escape_sql_string(f'{url}/comments.parquet')}')",
        "comments_index": f"SELECT * FROM read_parquet('{_escape_sql_string(f'{url}/comments_index.parquet')}')",
        "feed_summary": f"SELECT * FROM read_parquet('{_escape_sql_string(f'{url}/feed_summary.parquet')}')",
    }


def _connect_with_views(source: str, output_dir: Path, base_url: str):
    duckdb = _import_duckdb()
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    if source == "r2":
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        specs = _remote_view_specs(base_url)
    else:
        specs = _local_view_specs(output_dir)
    for name, sql in specs.items():
        con.execute(f"CREATE VIEW {name} AS {sql}")
    return con, specs


def normalize_text(value: str) -> str:
    lowered = value.lower()
    cleaned = NON_ALNUM_RE.sub(" ", lowered)
    return WHITESPACE_RE.sub(" ", cleaned).strip()


def informative_tokens(text: str) -> list[str]:
    tokens = normalize_text(text).split()
    return [token for token in tokens if token not in STOPWORDS and len(token) > 2]


def parse_modify_date(value: str | None) -> datetime | None:
    if not value:
        return None
    match = DATE_RE.match(value)
    if not match:
        return None
    try:
        return datetime.fromisoformat(f"{match.group(0)}T00:00:00+00:00")
    except ValueError:
        return None


def is_boilerplate_title(norm_title: str, patterns: list[re.Pattern[str]]) -> bool:
    return any(pattern.search(norm_title) for pattern in patterns)


@dataclass(slots=True)
class DocketRecord:
    docket_id: str
    agency_code: str
    title: str
    docket_type: str | None
    modify_date: str | None
    abstract: str | None
    norm_title: str
    title_tokens: frozenset[str]
    phrase_keys: tuple[str, ...]
    modify_ts: datetime | None


@dataclass(slots=True)
class PairScore:
    left_idx: int
    right_idx: int
    score: float
    title_ratio: float
    token_jaccard: float
    abstract_ratio: float
    date_gap_days: int | None


class UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, item: int) -> int:
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        if self.rank[left_root] < self.rank[right_root]:
            self.parent[left_root] = right_root
            return
        if self.rank[left_root] > self.rank[right_root]:
            self.parent[right_root] = left_root
            return
        self.parent[right_root] = left_root
        self.rank[left_root] += 1


def build_phrase_keys(tokens: list[str], shingle_size: int) -> tuple[str, ...]:
    if len(tokens) < shingle_size:
        return ()
    keys = {" ".join(tokens[i : i + shingle_size]) for i in range(len(tokens) - shingle_size + 1)}
    return tuple(sorted(keys))


def load_dockets(
    source: str,
    output_dir: Path,
    base_url: str,
    *,
    rulemaking_only: bool,
    min_title_length: int,
    min_year: int | None,
    exclude_boilerplate: bool,
) -> list[DocketRecord]:
    con, specs = _connect_with_views(source, output_dir, base_url)
    if "dockets" not in specs:
        raise SystemExit("The selected source does not expose a dockets table.")

    filters = ["title IS NOT NULL", f"length(trim(title)) >= {min_title_length}"]
    if rulemaking_only:
        filters.append("docket_type = 'Rulemaking'")
    if min_year is not None:
        filters.append(f"try_cast(substr(modify_date, 1, 4) AS INTEGER) >= {min_year}")
    where_sql = " AND ".join(filters)

    rows = con.execute(
        f"""
        SELECT
            docket_id,
            agency_code,
            title,
            docket_type,
            modify_date,
            abstract
        FROM dockets
        WHERE {where_sql}
        """
    ).fetchall()

    patterns = [re.compile(pattern) for pattern in BOILERPLATE_PATTERNS]
    results: list[DocketRecord] = []
    for row in rows:
        docket_id, agency_code, title, docket_type, modify_date, abstract = row
        title_str = title or ""
        norm_title = normalize_text(title_str)
        if not norm_title:
            continue
        if exclude_boilerplate and is_boilerplate_title(norm_title, patterns):
            continue
        title_token_list = informative_tokens(title_str)
        results.append(
            DocketRecord(
                docket_id=docket_id,
                agency_code=agency_code or "",
                title=title_str,
                docket_type=docket_type,
                modify_date=modify_date,
                abstract=abstract,
                norm_title=norm_title,
                title_tokens=frozenset(title_token_list),
                phrase_keys=build_phrase_keys(title_token_list, shingle_size=3),
                modify_ts=parse_modify_date(modify_date),
            )
        )
    return results


def iter_candidate_pairs(
    records: list[DocketRecord],
    *,
    max_block_size: int,
) -> set[tuple[int, int]]:
    by_exact_title: dict[str, list[int]] = defaultdict(list)
    by_phrase: dict[str, list[int]] = defaultdict(list)

    for idx, record in enumerate(records):
        by_exact_title[record.norm_title].append(idx)
        for key in record.phrase_keys:
            by_phrase[key].append(idx)

    pairs: set[tuple[int, int]] = set()
    for indexes in by_exact_title.values():
        if 1 < len(indexes) <= max_block_size:
            for left, right in combinations(indexes, 2):
                pairs.add((left, right))

    for indexes in by_phrase.values():
        if 1 < len(indexes) <= max_block_size:
            for left, right in combinations(indexes, 2):
                pairs.add((left, right))

    return pairs


def jaccard(left: frozenset[str], right: frozenset[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def score_pair(
    left: DocketRecord,
    right: DocketRecord,
    *,
    title_similarity_floor: float,
    abstract_similarity_floor: float,
    allow_same_agency: bool,
) -> PairScore | None:
    if not allow_same_agency and left.agency_code == right.agency_code:
        return None

    title_ratio = SequenceMatcher(None, left.norm_title, right.norm_title).ratio()
    token_jaccard = jaccard(left.title_tokens, right.title_tokens)
    if title_ratio < title_similarity_floor and token_jaccard < title_similarity_floor:
        return None

    abstract_ratio = 0.0
    if left.abstract and right.abstract:
        left_abstract = normalize_text(left.abstract)
        right_abstract = normalize_text(right.abstract)
        if left_abstract and right_abstract:
            abstract_ratio = SequenceMatcher(None, left_abstract[:1200], right_abstract[:1200]).ratio()

    if abstract_ratio and abstract_ratio < abstract_similarity_floor and title_ratio < 0.92 and token_jaccard < 0.92:
        return None

    date_gap_days: int | None = None
    date_bonus = 0.0
    if left.modify_ts and right.modify_ts:
        date_gap_days = abs((left.modify_ts - right.modify_ts).days)
        if date_gap_days <= 7:
            date_bonus = 0.12
        elif date_gap_days <= 30:
            date_bonus = 0.08
        elif date_gap_days <= 180:
            date_bonus = 0.04

    exact_title_bonus = 0.20 if left.norm_title == right.norm_title else 0.0
    abstract_bonus = min(abstract_ratio, 0.85) * 0.15
    score = (
        title_ratio * 0.50
        + token_jaccard * 0.25
        + abstract_bonus
        + exact_title_bonus
        + date_bonus
    )
    score = min(score, 1.0)

    return PairScore(
        left_idx=-1,
        right_idx=-1,
        score=score,
        title_ratio=title_ratio,
        token_jaccard=token_jaccard,
        abstract_ratio=abstract_ratio,
        date_gap_days=date_gap_days,
    )


def cluster_pairs(
    records: list[DocketRecord],
    scored_pairs: list[PairScore],
    *,
    min_score: float,
    min_agencies: int,
    max_dominant_agency_share: float,
) -> list[dict[str, object]]:
    uf = UnionFind(len(records))
    for pair in scored_pairs:
        if pair.score >= min_score:
            uf.union(pair.left_idx, pair.right_idx)

    members_by_root: dict[int, list[int]] = defaultdict(list)
    for idx in range(len(records)):
        members_by_root[uf.find(idx)].append(idx)

    pair_lookup: dict[tuple[int, int], PairScore] = {}
    for pair in scored_pairs:
        key = (min(pair.left_idx, pair.right_idx), max(pair.left_idx, pair.right_idx))
        pair_lookup[key] = pair

    clusters: list[dict[str, object]] = []
    for member_indexes in members_by_root.values():
        if len(member_indexes) < 2:
            continue
        member_records = [records[idx] for idx in member_indexes]
        agencies = sorted({record.agency_code for record in member_records if record.agency_code})
        if len(agencies) < min_agencies:
            continue
        agency_counts: dict[str, int] = defaultdict(int)
        for record in member_records:
            agency_counts[record.agency_code] += 1
        dominant_agency_share = max(agency_counts.values()) / len(member_records)
        if dominant_agency_share > max_dominant_agency_share:
            continue

        pair_scores: list[PairScore] = []
        for left, right in combinations(sorted(member_indexes), 2):
            pair = pair_lookup.get((left, right))
            if pair and pair.score >= min_score:
                pair_scores.append(pair)

        member_dates = [record.modify_ts for record in member_records if record.modify_ts]
        span_days = None
        if member_dates:
            span_days = (max(member_dates) - min(member_dates)).days

        clusters.append(
            {
                "cluster_score": round(sum(pair.score for pair in pair_scores) / max(len(pair_scores), 1), 3),
                "pair_count": len(pair_scores),
                "docket_count": len(member_records),
                "agency_count": len(agencies),
                "agencies": agencies,
                "dominant_agency_share": round(dominant_agency_share, 3),
                "span_days": span_days,
                "sample_title": sorted(record.title for record in member_records)[0],
                "records": [
                    {
                        "docket_id": record.docket_id,
                        "agency_code": record.agency_code,
                        "title": record.title,
                        "docket_type": record.docket_type,
                        "modify_date": record.modify_date,
                    }
                    for record in sorted(
                        member_records,
                        key=lambda record: (record.modify_date or "9999-99-99", record.agency_code, record.docket_id),
                    )
                ],
            }
        )

    clusters.sort(
        key=lambda cluster: (
            -float(cluster["cluster_score"]),
            -int(cluster["agency_count"]),
            int(cluster["span_days"] if cluster["span_days"] is not None else 10**9),
        )
    )
    return clusters


def format_text_report(clusters: list[dict[str, object]], source: str, total_records: int) -> str:
    lines = [
        f"source: {source}",
        f"records_scanned: {total_records}",
        f"clusters_found: {len(clusters)}",
    ]
    for idx, cluster in enumerate(clusters, start=1):
        lines.append("")
        lines.append(
            f"{idx}. score={cluster['cluster_score']} agencies={cluster['agency_count']} dockets={cluster['docket_count']} span_days={cluster['span_days']}"
        )
        lines.append(f"   sample_title: {cluster['sample_title']}")
        lines.append(f"   agencies: {', '.join(cluster['agencies'])}")
        lines.append(f"   dominant_agency_share: {cluster['dominant_agency_share']}")
        for record in cluster["records"]:
            lines.append(
                "   - "
                f"{record['docket_id']} | {record['agency_code']} | {record['modify_date'] or 'unknown'} | {record['title']}"
            )
    return "\n".join(lines)


def format_csv(clusters: list[dict[str, object]]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=[
            "cluster_rank",
            "cluster_score",
            "agency_count",
            "docket_count",
            "span_days",
            "dominant_agency_share",
            "sample_title",
            "docket_id",
            "agency_code",
            "modify_date",
            "title",
        ],
    )
    writer.writeheader()
    for rank, cluster in enumerate(clusters, start=1):
        for record in cluster["records"]:
            writer.writerow(
                {
                    "cluster_rank": rank,
                    "cluster_score": cluster["cluster_score"],
                    "agency_count": cluster["agency_count"],
                    "docket_count": cluster["docket_count"],
                    "span_days": cluster["span_days"],
                    "dominant_agency_share": cluster["dominant_agency_share"],
                    "sample_title": cluster["sample_title"],
                    "docket_id": record["docket_id"],
                    "agency_code": record["agency_code"],
                    "modify_date": record["modify_date"],
                    "title": record["title"],
                }
            )
    return buffer.getvalue()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["r2", "local"], default="r2")
    parser.add_argument("--r2-url", default=R2_BASE_URL, help="Base URL for the public Cloudflare R2 bucket")
    parser.add_argument(
        "--output-dir",
        default="./spicy-regs-data",
        help="Directory containing local Spicy Regs parquet outputs when --source=local",
    )
    parser.add_argument("--format", choices=["text", "json", "csv"], default="text")
    parser.add_argument("--limit", type=int, default=15, help="Maximum number of clusters to print")
    parser.add_argument("--min-agencies", type=int, default=2, help="Minimum number of agencies per cluster")
    parser.add_argument("--min-score", type=float, default=0.82, help="Minimum pair score to keep a cluster edge")
    parser.add_argument(
        "--title-similarity-floor",
        type=float,
        default=0.65,
        help="Minimum title similarity or token overlap needed before scoring a pair",
    )
    parser.add_argument(
        "--abstract-similarity-floor",
        type=float,
        default=0.25,
        help="Minimum abstract similarity once abstracts are present and the title match is not already strong",
    )
    parser.add_argument("--max-block-size", type=int, default=25, help="Skip phrase blocks larger than this size")
    parser.add_argument("--min-title-length", type=int, default=25)
    parser.add_argument("--min-year", type=int, default=None, help="Only scan dockets modified in this year or later")
    parser.add_argument("--include-nonrulemaking", action="store_true", help="Include nonrulemaking dockets")
    parser.add_argument("--allow-same-agency", action="store_true", help="Allow clusters within one agency")
    parser.add_argument(
        "--max-dominant-agency-share",
        type=float,
        default=0.75,
        help="Drop clusters where one agency accounts for more than this share of member dockets",
    )
    parser.add_argument(
        "--include-boilerplate",
        action="store_true",
        help="Do not filter out recurring procedural titles such as information collections",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    records = load_dockets(
        args.source,
        Path(args.output_dir),
        args.r2_url,
        rulemaking_only=not args.include_nonrulemaking,
        min_title_length=args.min_title_length,
        min_year=args.min_year,
        exclude_boilerplate=not args.include_boilerplate,
    )

    candidate_pairs = iter_candidate_pairs(records, max_block_size=args.max_block_size)
    scored_pairs: list[PairScore] = []
    for left_idx, right_idx in sorted(candidate_pairs):
        pair = score_pair(
            records[left_idx],
            records[right_idx],
            title_similarity_floor=args.title_similarity_floor,
            abstract_similarity_floor=args.abstract_similarity_floor,
            allow_same_agency=args.allow_same_agency,
        )
        if pair is None:
            continue
        pair.left_idx = left_idx
        pair.right_idx = right_idx
        scored_pairs.append(pair)

    clusters = cluster_pairs(
        records,
        scored_pairs,
        min_score=args.min_score,
        min_agencies=args.min_agencies,
        max_dominant_agency_share=args.max_dominant_agency_share,
    )[: args.limit]

    payload = {
        "source": args.source,
        "scanned_records": len(records),
        "candidate_pair_count": len(candidate_pairs),
        "scored_pair_count": len(scored_pairs),
        "cluster_count": len(clusters),
        "clusters": clusters,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    if args.format == "json":
        print(json.dumps(payload, indent=2))
        return 0
    if args.format == "csv":
        print(format_csv(clusters), end="")
        return 0

    print(format_text_report(clusters, args.source, len(records)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
