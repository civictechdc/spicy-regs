"""Draw the content-blind, sealed SEARCH holdout — the label-free half only.

The unit is a whole **matter**: the docket family / RIN family / cross-post
cluster assembled as a connected component over identity keys from three
published tables — ``proceedings`` (proceeding ↔ docket / FR document / RIN),
``agenda_item_proceedings`` (RIN ↔ proceeding), and ``fr_docket_links``
(FR document ↔ docket, FR document ↔ RIN). Every matter lands wholly in
exactly one split: ``holdout`` (drawn and sealed here) or ``development``
(everything else, including oversize clusters and below-floor strata).

Protocol reimplemented from ``tools/draw_holdout.py`` (the concept-tagging
holdout), re-keyed from artifacts to search matters:

* content-blind seeded order — ``subject_key``/``rank_key`` at
  ``tools/draw_holdout.py:538-548``. A matter is ranked by
  ``sha256(seed | procedure | matter_key)`` where ``matter_key`` is the
  canonical JSON of its sorted identity members. Content never enters it.
* blindness proof — ``assert_blind`` at ``tools/draw_holdout.py:854-892``.
  Two independent checks: no key anywhere in the sealed manifest may carry a
  banned substring, and no string scalar anywhere may equal a title or
  abstract from the inputs (compared by digest). The proof runs twice — once
  on the in-memory document and once on the re-parsed sealed bytes — and both
  runs are recorded in the receipt.

The draw path reads **identity and date columns only** (declared below as
``*_DRAW_COLUMNS``); the title/abstract columns are read solely by the
blindness checker, to prove their values are absent from the output.

**No labels are created here.** The sealing rules travel with the receipt:
the evaluated retrieval configuration freezes before any label exists; the
holdout opens once; labeling requires two independent judge families
("three sessions of one model count as one family").

Re-running with the same seed, procedure, and inputs reproduces the same
draw byte-for-byte (``drawn_at`` excluded from the sealed manifest for that
reason — it lives in the receipt).
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from math import floor
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]

DRAW_SCHEMA_VERSION = "search-holdout-draw-v1"

#: The recorded selection-procedure constant. Changing it changes every draw.
SELECTION_PROCEDURE = "search-holdout-matter-seeded-stratified-v1"
#: The recorded seed constant. Changing it changes every draw.
SELECTION_SEED = "search-holdout-draw-2026-08-01"

HOLDOUT_DATASET_ID = "search-holdout-matters-2026-08-01-v1"

DEFAULT_DATASET_DIR = REPO_ROOT / "output" / "rin-ontology-revision-candidate"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output" / "search-holdout-draw-2026-08-01"

MANIFEST_FILE_NAME = "sealed-manifest.json"
RECEIPT_FILE_NAME = "draw-receipt.json"
ONTOLOGY_MANIFEST_NAME = "ontology-dataset-manifest.json"

#: Declared allocation constants — the whole of the human input, per stratum.
TARGET_TOTAL_MATTERS = 240
MIN_STRATUM_CENSUS = 50
MIN_STRATUM_QUOTA = 2
MAX_STRATUM_QUOTA = 24
#: Matters with more identity keys than this stay in the development split:
#: the giant cross-post hairball (largest observed component: ~100k nodes)
#: would swallow the corpus if it could be drawn.
MAX_MATTER_NODES = 64

#: The draw path may read these columns and no others. Content columns
#: (title, abstract) are deliberately absent.
PROCEEDINGS_DRAW_COLUMNS = ("proceeding_id", "rin", "docket_ids_json", "fr_document_numbers_json")
AGENDA_DRAW_COLUMNS = ("rin", "proceeding_id", "evidence_date")
FR_DRAW_COLUMNS = ("docket_id", "document_number", "regulation_id_numbers_json", "publication_date")

#: Read by the blindness checker only, never by the draw path.
FORBIDDEN_CONTENT_COLUMNS = {
    "proceedings": ("title",),
    "fr_docket_links": ("title", "abstract"),
}

#: Size buckets by identity-key count, upper bound inclusive. Above the last
#: bound a matter is oversize and ineligible.
SIZE_BUCKETS: tuple[tuple[str, int], ...] = (
    ("single", 1),
    ("small", 4),
    ("medium", 16),
    ("large", MAX_MATTER_NODES),
)

#: Era buckets over a matter's latest evidence date (FR publication dates and
#: agenda evidence dates), exclusive ISO upper bound; ``None`` is open-ended.
ERA_BUCKETS: tuple[tuple[str, str | None], ...] = (
    ("pre-2010", "2010-01-01"),
    ("2010-2017", "2018-01-01"),
    ("2018-2022", "2023-01-01"),
    ("2023-plus", None),
)
UNDATED_ERA = "undated"

#: Keys that may never appear anywhere in the sealed manifest, as substrings.
#: The concept-tagging bans are kept (tagger output must not leak either) and
#: the search-content bans are added. ``selection`` is deliberately not
#: banned; ``selector`` is.
BANNED_OUTPUT_KEY_SUBSTRINGS: tuple[str, ...] = (
    "abstract",
    "alias",
    "body",
    "candidate",
    "concept",
    "confidence",
    "content",
    "embedding",
    "excerpt",
    "gold",
    "judg",
    "label",
    "predict",
    "quer",
    "registry",
    "relevan",
    "score",
    "selector",
    "snippet",
    "summar",
    "tagger",
    "taxonomy",
    "text",
    "title",
    "vector",
    "vocabulary",
)

BLINDNESS_STATEMENT = (
    "blind: contains no document titles, abstracts, body text, summaries or snippets, no "
    "concept ids or labels, no tagger or ranker output, no scores or relevance judgments, "
    "and no query of any kind. Every field derives from identity keys (proceeding ids, "
    "docket ids, FR document numbers, RINs), declared strata metadata (source class, size "
    "bucket, date era), and the recorded selection constants."
)

SEALING_RULES = {
    "labels": "none",
    "configuration_freeze": (
        "The retrieval configuration under evaluation must be frozen and pinned before any "
        "label for this holdout exists. Tuning against these matters, their documents, or "
        "any statistic derived from them voids the seal."
    ),
    "one_shot_opening": (
        "The holdout opens once: one scored evaluation per sealed configuration. Repeated "
        "peeking, threshold sweeps, or per-matter inspection after labels exist voids the seal."
    ),
    "judge_families": (
        "Labeling, when it happens, requires two independent judge families — separate model "
        "families or vendors sharing no code and no world-model with each other or with the "
        "system under evaluation. Three sessions of one model count as one family."
    ),
}


class SearchHoldoutDrawError(RuntimeError):
    """The inputs cannot support a search holdout draw."""


class SearchHoldoutPartitionError(SearchHoldoutDrawError):
    """An identity key would land in more than one matter or split."""


class SearchHoldoutBlindnessError(SearchHoldoutDrawError):
    """The sealed manifest would carry content, tagger, or judgment data."""


def canonical_json(value: object) -> str:
    """Deterministic JSON for stable ids and digests (no NaN, sorted keys)."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


# --------------------------------------------------------------------------
# matters: connected components over identity keys
# --------------------------------------------------------------------------


def _json_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item]
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if item]


def _iso_date(value: Any) -> str | None:
    if value is None:
        return None
    date = str(value).strip()
    return date if len(date) >= 4 and date[:4].isdigit() else None


class _DisjointSet:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, node: str) -> str:
        parent = self.parent
        if node not in parent:
            parent[node] = node
            return node
        root = node
        while parent[root] != root:
            root = parent[root]
        while parent[node] != root:
            parent[node], node = root, parent[node]
        return root

    def union(self, left: str, right: str) -> None:
        root_left, root_right = self.find(left), self.find(right)
        if root_left != root_right:
            self.parent[root_right] = root_left


@dataclass(frozen=True)
class Matter:
    """One whole matter: sorted identity members plus stratum metadata."""

    proceedings: tuple[str, ...]
    dockets: tuple[str, ...]
    fr_documents: tuple[str, ...]
    rins: tuple[str, ...]
    has_agenda_link: bool
    has_fr_link: bool
    latest_evidence: str | None

    @property
    def node_count(self) -> int:
        return len(self.proceedings) + len(self.dockets) + len(self.fr_documents) + len(self.rins)

    @property
    def source_class(self) -> str:
        if not self.proceedings:
            return "fr-only"
        parts = ["proc"]
        if self.has_agenda_link:
            parts.append("agenda")
        if self.has_fr_link:
            parts.append("fr")
        return "+".join(parts) if len(parts) > 1 else "proc-only"

    @property
    def matter_id(self) -> str:
        return sha256_text(matter_key(self))

    def identity_keys(self) -> Iterator[str]:
        for kind, members in (
            ("proceeding", self.proceedings),
            ("docket", self.dockets),
            ("frdoc", self.fr_documents),
            ("rin", self.rins),
        ):
            for member in members:
                yield f"{kind}:{member}"

    def manifest_row(self) -> dict[str, Any]:
        return {
            "matter_id": self.matter_id,
            "source_class": self.source_class,
            "size_bucket": size_bucket(self.node_count),
            "era_bucket": era_bucket(self.latest_evidence),
            "node_count": self.node_count,
            "proceedings": list(self.proceedings),
            "dockets": list(self.dockets),
            "fr_documents": list(self.fr_documents),
            "rins": list(self.rins),
        }


def matter_key(matter: Matter) -> str:
    """The identity-only key a matter is ranked by. Content never enters it."""
    return canonical_json(
        {
            "unit": "search-matter",
            "proceedings": list(matter.proceedings),
            "dockets": list(matter.dockets),
            "fr_documents": list(matter.fr_documents),
            "rins": list(matter.rins),
        }
    )


def rank_key(key: str, *, seed: str = SELECTION_SEED, procedure: str = SELECTION_PROCEDURE) -> str:
    """The seeded deterministic rank of one matter key."""
    return hashlib.sha256(f"{seed}\x1f{procedure}\x1f{key}".encode("utf-8")).hexdigest()


def assemble_matters(
    proceeding_rows: Iterable[Mapping[str, Any]],
    agenda_rows: Iterable[Mapping[str, Any]],
    fr_rows: Iterable[Mapping[str, Any]],
) -> list[Matter]:
    """Union identity keys from the three tables into whole matters."""
    dsu = _DisjointSet()
    agenda_proceedings: set[str] = set()
    fr_documents_seen: set[str] = set()
    node_dates: dict[str, str] = {}

    def note_date(node: str, value: Any) -> None:
        date = _iso_date(value)
        if date is not None and (node not in node_dates or date > node_dates[node]):
            node_dates[node] = date

    for row in proceeding_rows:
        proceeding = "proceeding:" + str(row["proceeding_id"])
        dsu.find(proceeding)
        rin = row.get("rin")
        if rin:
            dsu.union(proceeding, "rin:" + str(rin))
        for docket in _json_list(row.get("docket_ids_json")):
            dsu.union(proceeding, "docket:" + docket)
        for fr_document in _json_list(row.get("fr_document_numbers_json")):
            dsu.union(proceeding, "frdoc:" + fr_document)

    for row in agenda_rows:
        proceeding = "proceeding:" + str(row["proceeding_id"])
        dsu.union(proceeding, "rin:" + str(row["rin"]))
        agenda_proceedings.add(proceeding)
        note_date(proceeding, row.get("evidence_date"))

    for row in fr_rows:
        fr_document = "frdoc:" + str(row["document_number"])
        dsu.find(fr_document)
        fr_documents_seen.add(fr_document)
        if row.get("docket_id"):
            dsu.union(fr_document, "docket:" + str(row["docket_id"]))
        for rin in _json_list(row.get("regulation_id_numbers_json")):
            dsu.union(fr_document, "rin:" + rin)
        note_date(fr_document, row.get("publication_date"))

    components: dict[str, list[str]] = {}
    for node in dsu.parent:
        components.setdefault(dsu.find(node), []).append(node)

    matters: list[Matter] = []
    for members in components.values():
        kinds: dict[str, list[str]] = {"proceeding": [], "docket": [], "frdoc": [], "rin": []}
        latest: str | None = None
        has_agenda = False
        has_fr = False
        for node in members:
            kind, _, identifier = node.partition(":")
            kinds[kind].append(identifier)
            date = node_dates.get(node)
            if date is not None and (latest is None or date > latest):
                latest = date
            if node in agenda_proceedings:
                has_agenda = True
            if node in fr_documents_seen:
                has_fr = True
        matters.append(
            Matter(
                proceedings=tuple(sorted(kinds["proceeding"])),
                dockets=tuple(sorted(kinds["docket"])),
                fr_documents=tuple(sorted(kinds["frdoc"])),
                rins=tuple(sorted(kinds["rin"])),
                has_agenda_link=has_agenda,
                has_fr_link=has_fr,
                latest_evidence=latest,
            )
        )
    matters.sort(key=matter_key)
    return matters


def verify_partition(matters: Sequence[Matter]) -> dict[str, Any]:
    """Prove every identity key lands in exactly one matter, or refuse."""
    seen: set[str] = set()
    duplicated: set[str] = set()
    for matter in matters:
        for key in matter.identity_keys():
            if key in seen:
                duplicated.add(key)
            seen.add(key)
    facts = {
        "matter_count": len(matters),
        "identity_key_count": len(seen),
        "duplicated_identity_keys": sorted(duplicated)[:50],
        "passed": not duplicated,
    }
    if duplicated:
        raise SearchHoldoutPartitionError(
            "identity keys land in more than one matter: " + canonical_json(sorted(duplicated)[:50])
        )
    return facts


# --------------------------------------------------------------------------
# strata
# --------------------------------------------------------------------------


def size_bucket(node_count: int) -> str | None:
    """The declared size bucket, or ``None`` when the matter is oversize."""
    for name, upper in SIZE_BUCKETS:
        if node_count <= upper:
            return name
    return None


def era_bucket(latest_evidence: str | None) -> str:
    if latest_evidence is None:
        return UNDATED_ERA
    for name, upper in ERA_BUCKETS:
        if upper is None or latest_evidence < upper:
            return name
    return ERA_BUCKETS[-1][0]


StratumKey = tuple[str, str, str]  # (source_class, size_bucket, era_bucket)


def stratum_of(matter: Matter) -> StratumKey | None:
    bucket = size_bucket(matter.node_count)
    if bucket is None:
        return None
    return (matter.source_class, bucket, era_bucket(matter.latest_evidence))


def allocate_quotas(
    census: Mapping[StratumKey, int],
    *,
    target_total: int = TARGET_TOTAL_MATTERS,
    min_census: int = MIN_STRATUM_CENSUS,
    min_quota: int = MIN_STRATUM_QUOTA,
    max_quota: int = MAX_STRATUM_QUOTA,
) -> dict[StratumKey, int]:
    """Deterministic proportional allocation with declared floors and caps.

    Strata below ``min_census`` get no quota (their matters stay in the
    development split). Shares are proportional with largest-remainder
    rounding; every included stratum gets at least ``min_quota`` (clamped to
    its census) and at most ``max_quota``. Floors may push the total above
    ``target_total``; that overshoot is accepted and recorded, never shaved.
    """
    eligible = {key: count for key, count in census.items() if count >= min_census}
    if not eligible:
        return {}
    total = sum(eligible.values())
    quotas: dict[StratumKey, int] = {}
    remainders: list[tuple[float, StratumKey]] = []
    for key in sorted(eligible):
        raw = target_total * eligible[key] / total
        base = min(eligible[key], max_quota, max(min(min_quota, eligible[key]), floor(raw)))
        quotas[key] = base
        remainders.append((raw - floor(raw), key))
    remaining = target_total - sum(quotas.values())
    if remaining > 0:
        order = sorted(remainders, key=lambda item: (-item[0], item[1]))
        while remaining > 0:
            progressed = False
            for _, key in order:
                if remaining == 0:
                    break
                if quotas[key] < min(eligible[key], max_quota):
                    quotas[key] += 1
                    remaining -= 1
                    progressed = True
            if not progressed:
                break
    return quotas


# --------------------------------------------------------------------------
# the draw
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class StratumDraw:
    """One stratum's census, quota, and accepted matters."""

    key: StratumKey
    census: int
    quota: int
    drawn: tuple[Matter, ...]

    def facts(self) -> dict[str, Any]:
        source_class, size_name, era_name = self.key
        return {
            "source_class": source_class,
            "size_bucket": size_name,
            "era_bucket": era_name,
            "census": self.census,
            "quota": self.quota,
            "drawn": len(self.drawn),
        }


@dataclass(frozen=True)
class SearchHoldoutDraw:
    """Every stratum's result plus the constants that produced it."""

    dataset_id: str
    seed: str
    procedure: str
    strata: tuple[StratumDraw, ...]
    development: tuple[Matter, ...]
    oversize_count: int
    below_floor_count: int
    partition_facts: Mapping[str, Any]

    @property
    def drawn(self) -> tuple[Matter, ...]:
        return tuple(matter for stratum in self.strata for matter in stratum.drawn)

    def split_facts(self) -> dict[str, Any]:
        return {
            "holdout": len(self.drawn),
            "development": len(self.development),
            "oversize_in_development": self.oversize_count,
            "below_census_floor_in_development": self.below_floor_count,
        }

    def membership(self) -> list[dict[str, Any]]:
        return [matter.manifest_row() for matter in self.drawn]

    def membership_sha256(self) -> str:
        return sha256_text(canonical_json(self.membership()))

    def selection_sha256(self) -> str:
        """One digest over the procedure, the strata, and the membership."""
        return sha256_text(
            canonical_json(
                {
                    "dataset_id": self.dataset_id,
                    "draw_schema_version": DRAW_SCHEMA_VERSION,
                    "selection_procedure": self.procedure,
                    "selection_seed": self.seed,
                    "strata": [stratum.facts() for stratum in self.strata],
                    "membership": self.membership(),
                }
            )
        )


def draw_search_holdout(
    matters: Sequence[Matter],
    *,
    seed: str = SELECTION_SEED,
    procedure: str = SELECTION_PROCEDURE,
    dataset_id: str = HOLDOUT_DATASET_ID,
    target_total: int = TARGET_TOTAL_MATTERS,
    min_census: int = MIN_STRATUM_CENSUS,
    min_quota: int = MIN_STRATUM_QUOTA,
    max_quota: int = MAX_STRATUM_QUOTA,
) -> SearchHoldoutDraw:
    """Partition-check, stratify, and draw whole matters in seeded order."""
    partition_facts = verify_partition(matters)

    by_stratum: dict[StratumKey, list[Matter]] = {}
    oversize = 0
    for matter in matters:
        key = stratum_of(matter)
        if key is None:
            oversize += 1
            continue
        by_stratum.setdefault(key, []).append(matter)

    census = {key: len(members) for key, members in by_stratum.items()}
    quotas = allocate_quotas(
        census,
        target_total=target_total,
        min_census=min_census,
        min_quota=min_quota,
        max_quota=max_quota,
    )
    below_floor = sum(count for key, count in census.items() if key not in quotas)

    strata: list[StratumDraw] = []
    drawn_ids: set[str] = set()
    for key in sorted(quotas):
        members = by_stratum[key]
        ordered = sorted(
            ((rank_key(matter_key(m), seed=seed, procedure=procedure), matter_key(m), m) for m in members),
            key=lambda item: (item[0], item[1]),
        )
        accepted = tuple(matter for _, _, matter in ordered[: quotas[key]])
        drawn_ids.update(matter.matter_id for matter in accepted)
        strata.append(StratumDraw(key=key, census=census[key], quota=quotas[key], drawn=accepted))

    development = tuple(matter for matter in matters if matter.matter_id not in drawn_ids)
    return SearchHoldoutDraw(
        dataset_id=dataset_id,
        seed=seed,
        procedure=procedure,
        strata=tuple(strata),
        development=development,
        oversize_count=oversize,
        below_floor_count=below_floor,
        partition_facts=partition_facts,
    )


# --------------------------------------------------------------------------
# blindness: the assert_blind pattern, re-keyed to search
# --------------------------------------------------------------------------


def content_digest(value: str) -> bytes:
    """The digest a forbidden content scalar is compared by (memory-light)."""
    return hashlib.sha256(value.encode("utf-8")).digest()[:16]


def _walk(value: Any, path: str = "") -> Iterator[tuple[str, str | None, Any]]:
    if isinstance(value, Mapping):
        for key, item in value.items():
            child = f"{path}.{key}" if path else str(key)
            yield child, str(key), item
            yield from _walk(item, child)
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            child = f"{path}[{index}]"
            yield child, None, item
            yield from _walk(item, child)


def assert_blind(
    document: Mapping[str, Any],
    *,
    forbidden_value_digests: frozenset[bytes] | set[bytes] = frozenset(),
    banned_key_substrings: Sequence[str] = BANNED_OUTPUT_KEY_SUBSTRINGS,
) -> dict[str, Any]:
    """Prove no content, tagger, or judgment information reached the document.

    Two independent checks, because a whitelist alone is only as good as the
    person who wrote it: no key anywhere may carry a banned substring, and no
    scalar string anywhere may equal a known title or abstract from the
    inputs (compared by :func:`content_digest`).
    """
    banned_keys: list[str] = []
    leaked_values: list[str] = []
    scalars = 0
    for path, key, item in _walk(document):
        if key is not None:
            folded = key.casefold()
            if any(banned in folded for banned in banned_key_substrings):
                banned_keys.append(path)
        if isinstance(item, str):
            scalars += 1
            if item.strip() and content_digest(item) in forbidden_value_digests:
                leaked_values.append(path)
    facts = {
        "banned_key_paths": sorted(banned_keys),
        "leaked_value_paths": sorted(leaked_values),
        "banned_key_substrings": list(banned_key_substrings),
        "forbidden_value_count": len(forbidden_value_digests),
        "string_values_checked": scalars,
        "passed": not banned_keys and not leaked_values,
    }
    if not facts["passed"]:
        raise SearchHoldoutBlindnessError(
            "sealed manifest is not blind: "
            + canonical_json({"banned_key_paths": sorted(banned_keys), "leaked_value_paths": sorted(leaked_values)})
        )
    return facts


# --------------------------------------------------------------------------
# inputs
# --------------------------------------------------------------------------


def _read_rows(path: Path, columns: Sequence[str]) -> list[dict[str, Any]]:
    import pyarrow.parquet as pq

    if not path.exists():
        raise SearchHoldoutDrawError(f"dataset is missing {path}")
    available = {field.name for field in pq.read_schema(path)}
    wanted = [column for column in columns if column in available]
    missing = [column for column in columns if column not in available]
    if missing and not wanted:
        raise SearchHoldoutDrawError(f"{path} carries none of the columns {list(columns)}")
    return pq.read_table(path, columns=wanted).to_pylist()


def load_matters(dataset_dir: Path) -> list[Matter]:
    """Assemble matters reading identity and date columns only."""
    base = Path(dataset_dir)
    return assemble_matters(
        _read_rows(base / "proceedings.parquet", PROCEEDINGS_DRAW_COLUMNS),
        _read_rows(base / "agenda_item_proceedings.parquet", AGENDA_DRAW_COLUMNS),
        _read_rows(base / "fr_docket_links.parquet", FR_DRAW_COLUMNS),
    )


def load_forbidden_content_digests(dataset_dir: Path) -> frozenset[bytes]:
    """Digest every title/abstract in the inputs. Checker-only: this content
    is used solely to prove its own absence from the sealed manifest."""
    base = Path(dataset_dir)
    digests: set[bytes] = set()
    for table, columns in FORBIDDEN_CONTENT_COLUMNS.items():
        for row in _read_rows(base / f"{table}.parquet", columns):
            for column in columns:
                value = row.get(column)
                if value is not None and str(value).strip():
                    digests.add(content_digest(str(value)))
    return frozenset(digests)


def input_facts(dataset_dir: Path) -> list[dict[str, Any]]:
    """Pin a digest for every input file consumed."""
    base = Path(dataset_dir)
    facts: list[dict[str, Any]] = []
    tables = ("proceedings", "agenda_item_proceedings", "fr_docket_links")
    for table in tables:
        path = base / f"{table}.parquet"
        facts.append({"table": table, "file": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path)})
    ontology_manifest = base / ONTOLOGY_MANIFEST_NAME
    if ontology_manifest.exists():
        facts.append(
            {
                "table": "ontology-dataset-manifest",
                "file": ontology_manifest.name,
                "bytes": ontology_manifest.stat().st_size,
                "sha256": sha256_file(ontology_manifest),
            }
        )
    return facts


# --------------------------------------------------------------------------
# the sealed manifest and its receipt
# --------------------------------------------------------------------------


def sealed_manifest(draw: SearchHoldoutDraw, *, inputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Assemble the sealed manifest. Deliberately excludes any timestamp so
    the same seed, procedure, and inputs reproduce the same bytes."""
    return {
        "schema_version": DRAW_SCHEMA_VERSION,
        "blind": BLINDNESS_STATEMENT,
        "holdout": {
            "dataset_id": draw.dataset_id,
            "draw_schema_version": DRAW_SCHEMA_VERSION,
            "selection_procedure": draw.procedure,
            "selection_seed": draw.seed,
            "selection_sha256": draw.selection_sha256(),
            "membership_sha256": draw.membership_sha256(),
            "allocation": {
                "unit": "matter",
                "max_matter_nodes": MAX_MATTER_NODES,
                "strata_dimensions": ["source_class", "size_bucket", "era_bucket"],
            },
            "strata": [stratum.facts() for stratum in draw.strata],
        },
        "inputs": [dict(entry) for entry in inputs],
        "splits": draw.split_facts(),
        "matter_total": len(draw.drawn),
        "matters": draw.membership(),
    }


def build_receipt(
    draw: SearchHoldoutDraw,
    *,
    drawn_at: str,
    inputs: Sequence[Mapping[str, Any]],
    sealed_manifest_sha256: str,
    blindness_first_run: Mapping[str, Any],
    blindness_second_run: Mapping[str, Any],
) -> dict[str, Any]:
    first = dict(blindness_first_run)
    second = dict(blindness_second_run)
    return {
        "schema_version": DRAW_SCHEMA_VERSION,
        "drawn_at": drawn_at,
        "dataset_id": draw.dataset_id,
        "selection_procedure": draw.procedure,
        "selection_seed": draw.seed,
        "selection_sha256": draw.selection_sha256(),
        "membership_sha256": draw.membership_sha256(),
        "sealed_manifest_sha256": sealed_manifest_sha256,
        "inputs": [dict(entry) for entry in inputs],
        "partition": dict(draw.partition_facts),
        "splits": draw.split_facts(),
        "strata": [stratum.facts() for stratum in draw.strata],
        "blindness_first_run": first,
        "blindness_second_run": second,
        "blindness_runs_match": first == second,
        "sealing": dict(SEALING_RULES),
        "status": "drawn_unadjudicated",
        "reason": (
            "A search holdout has been drawn and sealed, and no label exists for it. It is not "
            "adjudicated and it can authorize nothing. The evaluated retrieval configuration "
            "freezes at label-exposure time; until then this draw is contamination insurance only."
        ),
    }


@dataclass(frozen=True)
class DrawRunResult:
    manifest_path: Path
    receipt_path: Path
    draw: SearchHoldoutDraw


def run_draw(
    *,
    dataset_dir: Path,
    output_dir: Path,
    seed: str = SELECTION_SEED,
    procedure: str = SELECTION_PROCEDURE,
    dataset_id: str = HOLDOUT_DATASET_ID,
    target_total: int = TARGET_TOTAL_MATTERS,
    min_census: int = MIN_STRATUM_CENSUS,
    min_quota: int = MIN_STRATUM_QUOTA,
    max_quota: int = MAX_STRATUM_QUOTA,
    drawn_at: str | None = None,
) -> DrawRunResult:
    """Draw, prove blindness twice, and seal the manifest + receipt."""
    dataset_dir = Path(dataset_dir)
    output_dir = Path(output_dir)
    matters = load_matters(dataset_dir)
    draw = draw_search_holdout(
        matters,
        seed=seed,
        procedure=procedure,
        dataset_id=dataset_id,
        target_total=target_total,
        min_census=min_census,
        min_quota=min_quota,
        max_quota=max_quota,
    )
    inputs = input_facts(dataset_dir)
    manifest = sealed_manifest(draw, inputs=inputs)

    forbidden = load_forbidden_content_digests(dataset_dir)
    first_run = assert_blind(manifest, forbidden_value_digests=forbidden)

    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / MANIFEST_FILE_NAME
    manifest_path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")

    # Second, independent run: re-parse the sealed bytes from disk.
    reloaded = json.loads(manifest_path.read_text(encoding="utf-8"))
    second_run = assert_blind(reloaded, forbidden_value_digests=forbidden)

    receipt = build_receipt(
        draw,
        drawn_at=drawn_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        inputs=inputs,
        sealed_manifest_sha256=sha256_file(manifest_path),
        blindness_first_run=first_run,
        blindness_second_run=second_run,
    )
    receipt_path = output_dir / RECEIPT_FILE_NAME
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return DrawRunResult(manifest_path=manifest_path, receipt_path=receipt_path, draw=draw)


def verify_existing(dataset_dir: Path, output_dir: Path) -> dict[str, Any]:
    """Recompute the draw from the inputs and compare to the sealed files."""
    manifest_path = Path(output_dir) / MANIFEST_FILE_NAME
    if not manifest_path.exists():
        raise SearchHoldoutDrawError(f"nothing to verify: {manifest_path} does not exist")
    sealed = json.loads(manifest_path.read_text(encoding="utf-8"))
    holdout = sealed.get("holdout", {})
    matters = load_matters(Path(dataset_dir))
    draw = draw_search_holdout(
        matters,
        seed=str(holdout.get("selection_seed")),
        procedure=str(holdout.get("selection_procedure")),
        dataset_id=str(holdout.get("dataset_id")),
    )
    facts = {
        "selection_sha256_matches": draw.selection_sha256() == holdout.get("selection_sha256"),
        "membership_sha256_matches": draw.membership_sha256() == holdout.get("membership_sha256"),
        "sealed_manifest_sha256": sha256_file(manifest_path),
    }
    facts["passed"] = facts["selection_sha256_matches"] and facts["membership_sha256_matches"]
    return facts


# --------------------------------------------------------------------------
# command line
# --------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Recompute the draw from the inputs and compare against the sealed manifest.",
    )
    arguments = parser.parse_args(argv)

    if arguments.verify:
        facts = verify_existing(arguments.dataset_dir, arguments.output_dir)
        print(json.dumps(facts, indent=2, sort_keys=True))
        return 0 if facts["passed"] else 1

    result = run_draw(dataset_dir=arguments.dataset_dir, output_dir=arguments.output_dir)
    receipt = json.loads(result.receipt_path.read_text(encoding="utf-8"))
    print(
        json.dumps(
            {
                "sealed_manifest": str(result.manifest_path),
                "receipt": str(result.receipt_path),
                "matter_total": len(result.draw.drawn),
                "splits": receipt["splits"],
                "selection_sha256": receipt["selection_sha256"],
                "membership_sha256": receipt["membership_sha256"],
                "sealed_manifest_sha256": receipt["sealed_manifest_sha256"],
                "blindness_runs_match": receipt["blindness_runs_match"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
