"""Build the agency-crosswalk artifact from published parquet tables.

The CFR-part soft-priors experiment needs to know which Federal Register
**agency slug** a regulations.gov **agency code** stands for, and which
agencies cite a given CFR part. Both facts are derivable from tables already
on disk; this tool materializes them as one digest-pinned artifact.

Two questions, four outputs:

* **Agency code -> FR slug**, with confidence tiers. Evidence is joined
  through the agency *field* on ``dockets`` (``agency_code``) and on
  ``documents`` (``agency_code`` + ``fr_doc_num``) — never through docket-id
  string prefixes, which manufacture codes that do not exist (a docket id
  reading ``EPA-HQ-…`` may carry any agency code, and half a million link
  rows carry docket ids that are not regulations.gov dockets at all).
  ``agency-crosswalk.parquet`` holds one row per candidate (code, slug) pair;
  ``agency-codes.parquet`` holds the per-code tier decision.
* **Parent-department mapping** — ``agency-parents.parquet``, the
  slug/id/parent_id lineage carried by ``federal_register.agencies_json``,
  with the resolved parent slug and the depth of each slug in its chain.
* **CFR part -> agency** — ``cfr-part-agencies.parquet``, per (title, part):
  every agency slug citing it, the document count, and the share. This is the
  direct input the priors experiment consumes. ``cfr_references_json`` is
  already parsed on the ``federal_register`` table, so nothing here parses
  citation text.

Tiering is share-and-support, both pinned. For an agency code, ``share`` is
the fraction of that code's supporting Federal Register documents whose
``agencies_json`` names a given slug. Sub-agency documents routinely name
both the department and the sub-agency, so several slugs legitimately reach
share 1.0; the pinned ``SPECIFICITY_MARGIN`` breaks such near-ties toward the
deeper (more specific) slug, which is what a crosswalk wants — ``FAA`` should
resolve to ``federal-aviation-administration``, not to its parent
``transportation-department``. A perfect share over one document is not
confidence, so the tiers also carry document floors. Codes that end up
contested (``ambiguous``) or unreached (``unmapped``) stay in the artifact,
marked: downstream decides, this tool never silently drops.

Malformed source rows land in a typed quarantine partition
(``quarantine.parquet``) with machine-readable reasons. Rows that are merely
*unjoinable* — a link row whose docket id is not a regulations.gov docket —
are expected non-overlap rather than a defect, so they are counted and named
in the receipt's ``coverage`` block instead of materializing hundreds of
thousands of quarantine rows. Both surfaces are receipted; neither is silent.

Outputs (under ``--output``): the four tables above, ``quarantine.parquet``
and a deterministic canonical-JSON ``receipt.json`` pinning input digests,
artifact digests, thresholds and counts. Rebuilding from byte-identical
inputs with the same library versions reproduces every file byte-for-byte
(no timestamps and no absolute paths inside sealed surfaces — the pattern
from ``build_date_event_artifact.py``).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

# The docket join key this tool proved is now the one the link table publishes,
# so the grammar lives beside the other identifier grammars rather than here.
# Values unchanged: the receipt pins DOCKET_DECORATION_PATTERN and
# DOCKET_NORMALIZATION_RULES, and a rebuild must reproduce it byte for byte.
from spicy_regs.ontology.citations import (
    DOCKET_DECORATION_PATTERN,
    DOCKET_NORMALIZATION_RULES,
    normalize_docket_id,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

ARTIFACT_SCHEMA_VERSION = "agency-crosswalk-artifact-v1"
TIER_POLICY = "share-and-support-tiers-v1"

#: A code whose primary slug reaches this share, over at least
#: ``MIN_CONFIDENT_DOCUMENTS`` documents, is ``confident``.
CONFIDENT_SHARE = 0.8
#: The same at a lower bar, over at least ``MIN_PROBABLE_DOCUMENTS``
#: documents, is ``probable``.
PROBABLE_SHARE = 0.6
#: Share is meaningless over a handful of documents: floors, not just ratios.
MIN_CONFIDENT_DOCUMENTS = 5
MIN_PROBABLE_DOCUMENTS = 2
#: Candidates within this much of the best share are treated as tied, and the
#: tie is broken toward the deeper (more specific) slug.
SPECIFICITY_MARGIN = 0.05

#: Ordered so the receipt's histogram always reads best-to-worst.
TIERS = ("confident", "probable", "ambiguous", "unmapped")

#: The two agency-*field* joins. Neither reads a docket-id string prefix.
EVIDENCE_PATHS = ("dockets_fr_links", "documents_fr_doc_num")

DOCKET_NORMALIZATION_POLICY = "docket-id-normalization-v1"

#: The upstream defect this normalization exists to work around. Without it
#: the join silently drops real edges, which is a data defect and not the
#: "expected non-overlap" it superficially resembles.
DOCKET_NORMALIZATION_DEFECT = (
    "docs/corpus-edge-coverage-findings-2026-07-24.md finding #1 (RULE-010, confirmed): "
    "build_fr_docket_links.py explodes federal_register.docket_ids_json without "
    "normalizing the emitted docket_id, so the Federal Register's decorated strings "
    "('Docket No. FAA-2026-3485') never match the dockets spine, which keys on the bare "
    "id. The finding prescribes targeted normalization plus honest quarantine of the "
    "rest -- never force-matching."
)

CROSSWALK_COLUMNS = (
    "crosswalk_id",
    "agency_code",
    "agency_slug",
    "agency_id",
    "parent_id",
    "parent_slug",
    "depth",
    "support_documents",
    "support_by_path_json",
    "share",
    "rank",
    "is_primary",
    "tier",
)

CODE_COLUMNS = (
    "agency_code_id",
    "agency_code",
    "in_dockets_table",
    "in_documents_table",
    "tier",
    "primary_slug",
    "confidence_share",
    "support_documents",
    "support_by_path_json",
    "dockets_path_documents",
    "documents_path_documents",
    "evidence_is_documents_only",
    "candidate_count",
    "candidate_slugs_json",
)

PARENT_COLUMNS = (
    "agency_slug",
    "agency_id",
    "parent_id",
    "parent_slug",
    "depth",
    "documents",
)

CFR_COLUMNS = (
    "cfr_agency_id",
    "cfr_title",
    "cfr_part",
    "agency_slug",
    "documents",
    "part_documents",
    "share",
    "rank",
    "is_most_citing",
)

QUARANTINE_COLUMNS = (
    "quarantine_id",
    "source",
    "evidence_field",
    "document_ref",
    "agency_code",
    "docket_ref",
    "raw_value",
    "occurrence",
    "reasons_json",
)

_FEDERAL_REGISTER_COLUMNS = ("document_number", "agencies_json", "cfr_references_json")
_DOCKET_COLUMNS = ("docket_id", "agency_code")
_FR_LINK_COLUMNS = ("docket_id", "document_number")
_DOCUMENT_COLUMNS = ("agency_code", "fr_doc_num")

TIER_LABEL = (
    "tiers are share AND support: confident = primary share >= 0.8 over >= 5 "
    "supporting documents; probable = >= 0.6 over >= 2; ambiguous = any code "
    "with evidence that meets neither; unmapped = a code the join never "
    "reached. Ambiguous and unmapped codes are kept in the artifact, marked."
)

SPECIFICITY_LABEL = (
    "sub-agency documents name both the department and the sub-agency, so "
    "several slugs can share the top rank; candidates within "
    "SPECIFICITY_MARGIN of the best share are tied and the tie is broken "
    "toward the deeper slug in the parent_id chain, then by share, then by "
    "slug. The full candidate list is retained either way."
)

JOIN_LABEL = (
    "agency codes come from the agency FIELD on dockets (agency_code) and on "
    "documents (agency_code + fr_doc_num), never from docket-id string "
    "prefixes. Link rows that do not match the dockets spine fall into two "
    "populations that must not be conflated: (1) decorated-but-resolvable ids, "
    "a known upstream defect recovered by docket-id-normalization-v1 -- see "
    "docs/corpus-edge-coverage-findings-2026-07-24.md finding #1 (RULE-010); "
    "and (2) genuine foreign identifiers (FRL-*, REG-*, CMS-*-F, Special "
    "Conditions No. *) which correctly match no regulations.gov docket and are "
    "counted in coverage, not quarantined. A normalized key that maps to more "
    "than one docket is quarantined, never guessed."
)

CFR_PRIMARY_LABEL = (
    "cfr-part-agencies.is_most_citing marks the agency named on the most "
    "documents citing that (title, part). Count ties are broken by greater "
    "depth in the parent_id chain (the deeper, more specific slug wins), so "
    "the flag is 'most-citing, then deepest', not a crosswalk resolution. "
    "It is NOT the owning agency: on "
    "14 CFR 39 (airworthiness directives) transportation-department outranks "
    "federal-aviation-administration because the department is named on "
    "marginally more documents. Consumers wanting the responsible sub-agency "
    "should use the full ranked candidate list with agency-parents.parquet, "
    "not this flag."
)

DENOMINATOR_LABEL = (
    "share denominator is the count of distinct Federal Register documents "
    "reached for the code across both evidence paths. A document whose "
    "agencies_json yields no usable slug still counts in the denominator, so "
    "the share reflects that the document asserted nothing."
)


def canonical_json(value: object) -> str:
    """Serialize deterministically (origin: spicy_regs/ontology/common.py:84)."""

    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_id(prefix: str, payload: object, *, length: int = 24) -> str:
    """Content-derived identifier (origin: spicy_regs/ontology/common.py:71)."""

    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return f"{prefix}:{digest[:length]}"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return f"sha256:{digest.hexdigest()}"


def _pin_path(path: Path) -> str:
    """Record a repo-relative path when possible, else the basename.

    Keeping absolute scratch paths out of the receipt keeps rebuilds from
    different working directories byte-identical.
    """

    resolved = path.resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        return resolved.name


def _text(value: object) -> str:
    return "" if value is None else str(value).strip()


def _share(numerator: int, denominator: int) -> str:
    """Format a share at fixed precision so parquet bytes are reproducible."""

    if denominator <= 0:
        return "0.000000"
    return f"{numerator / denominator:.6f}"


def _flag(value: bool) -> str:
    return "true" if value else "false"


def _read_rows(path: Path, columns: tuple[str, ...]) -> list[dict[str, Any]]:
    return pq.read_table(path, columns=list(columns)).to_pylist()


class _Quarantine:
    """Accumulate typed quarantine rows with per-source reason counters."""

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.reasons: dict[str, Counter[str]] = defaultdict(Counter)
        #: Identical defects recur (the same partless CFR reference listed
        #: twice, the same dangling fr_doc_num on two rows). Content-derived
        #: ids would collide, so each repeat carries an occurrence ordinal.
        self._seen: Counter[str] = Counter()

    def add(
        self,
        *,
        source: str,
        evidence_field: str,
        reasons: list[str],
        document_ref: str | None = None,
        agency_code: str | None = None,
        docket_ref: str | None = None,
        raw_value: str | None = None,
    ) -> None:
        ordered_reasons = sorted(set(reasons))
        row = {
            "source": source,
            "evidence_field": evidence_field,
            "document_ref": document_ref,
            "agency_code": agency_code,
            "docket_ref": docket_ref,
            "raw_value": raw_value,
            "reasons_json": canonical_json(ordered_reasons),
        }
        fact = canonical_json(row)
        self._seen[fact] += 1
        row["occurrence"] = str(self._seen[fact])
        row["quarantine_id"] = stable_id("urn:spicy-regs:agency-crosswalk-quarantine", row)
        self.rows.append(row)
        for reason in ordered_reasons:
            self.reasons[source][reason] += 1


class _AgencyDirectory:
    """slug -> (id, parent_id) as asserted by ``agencies_json``, plus depth."""

    def __init__(self) -> None:
        self._observed: dict[str, Counter[tuple[str | None, str | None]]] = defaultdict(Counter)
        self.documents: Counter[str] = Counter()
        self._resolved: dict[str, tuple[str | None, str | None]] = {}
        self._by_id: dict[str, str] = {}

    def observe(self, slug: str, agency_id: object, parent_id: object) -> None:
        identity = (
            _text(agency_id) or None,
            _text(parent_id) or None,
        )
        self._observed[slug][identity] += 1

    def seal(self) -> None:
        """Freeze one identity per slug, deterministically.

        A slug's id/parent_id are repeated on every document that names it.
        Should a pin disagree with itself, the most frequently asserted
        identity wins, ties broken lexicographically rather than by row order.
        """

        for slug, counts in self._observed.items():
            self._resolved[slug] = min(
                counts.items(),
                key=lambda item: (-item[1], str(item[0][0]), str(item[0][1])),
            )[0]
        for slug in sorted(self._resolved):
            agency_id = self._resolved[slug][0]
            if agency_id is not None:
                self._by_id.setdefault(agency_id, slug)

    @property
    def slugs(self) -> list[str]:
        return sorted(self._resolved)

    def identity(self, slug: str) -> tuple[str | None, str | None]:
        return self._resolved.get(slug, (None, None))

    def parent_slug(self, slug: str) -> str | None:
        parent_id = self.identity(slug)[1]
        if parent_id is None:
            return None
        return self._by_id.get(parent_id)

    def depth(self, slug: str) -> int:
        """Steps to the root of the parent chain, stopping at anything unresolvable."""

        depth = 0
        seen = {slug}
        current = slug
        while True:
            parent = self.parent_slug(current)
            if parent is None or parent in seen:
                return depth
            depth += 1
            seen.add(parent)
            current = parent


def _collect_federal_register(
    path: Path, quarantine: _Quarantine
) -> tuple[dict[str, list[str]], _AgencyDirectory, dict[tuple[str, str], set[str]], int, int]:
    """Read the pin once: per-document slugs, the directory, and CFR pairs."""

    rows = _read_rows(path, _FEDERAL_REGISTER_COLUMNS)
    document_slugs: dict[str, list[str]] = {}
    directory = _AgencyDirectory()
    cfr_documents: dict[tuple[str, str], set[str]] = defaultdict(set)
    documents_with_cfr_references = 0

    for row in rows:
        document_number = _text(row.get("document_number"))
        if not document_number:
            continue

        raw_agencies = _text(row.get("agencies_json"))
        slugs: set[str] = set()
        if raw_agencies and raw_agencies not in ("[]", "null"):
            try:
                parsed = json.loads(raw_agencies)
            except json.JSONDecodeError:
                parsed = None
            if not isinstance(parsed, list):
                quarantine.add(
                    source="federal_register",
                    evidence_field="agencies_json",
                    reasons=["unparseable_agencies_json"],
                    document_ref=document_number,
                    raw_value=raw_agencies[:200],
                )
            else:
                for entry in parsed:
                    if not isinstance(entry, dict):
                        quarantine.add(
                            source="federal_register",
                            evidence_field="agencies_json",
                            reasons=["agency_entry_not_an_object"],
                            document_ref=document_number,
                            raw_value=canonical_json(entry)[:200],
                        )
                        continue
                    slug = _text(entry.get("slug"))
                    if not slug:
                        quarantine.add(
                            source="federal_register",
                            evidence_field="agencies_json",
                            reasons=["agency_entry_missing_slug"],
                            document_ref=document_number,
                            raw_value=canonical_json(entry)[:200],
                        )
                        continue
                    slugs.add(slug)
                    directory.observe(slug, entry.get("id"), entry.get("parent_id"))
        document_slugs[document_number] = sorted(slugs)
        for slug in slugs:
            directory.documents[slug] += 1

        raw_cfr = _text(row.get("cfr_references_json"))
        if not raw_cfr or raw_cfr in ("[]", "null"):
            continue
        try:
            references = json.loads(raw_cfr)
        except json.JSONDecodeError:
            references = None
        if not isinstance(references, list):
            quarantine.add(
                source="federal_register",
                evidence_field="cfr_references_json",
                reasons=["unparseable_cfr_references_json"],
                document_ref=document_number,
                raw_value=raw_cfr[:200],
            )
            continue
        if references:
            documents_with_cfr_references += 1
        for reference in references:
            if not isinstance(reference, dict):
                quarantine.add(
                    source="federal_register",
                    evidence_field="cfr_references_json",
                    reasons=["cfr_reference_not_an_object"],
                    document_ref=document_number,
                    raw_value=canonical_json(reference)[:200],
                )
                continue
            title = _text(reference.get("title"))
            part = _text(reference.get("part"))
            reasons = []
            if not title:
                reasons.append("cfr_reference_missing_title")
            if not part:
                reasons.append("cfr_reference_missing_part")
            if reasons:
                quarantine.add(
                    source="federal_register",
                    evidence_field="cfr_references_json",
                    reasons=reasons,
                    document_ref=document_number,
                    raw_value=canonical_json(reference)[:200],
                )
                continue
            cfr_documents[(title, part)].add(document_number)

    directory.seal()
    return document_slugs, directory, cfr_documents, len(rows), documents_with_cfr_references


def _collect_code_evidence(
    *,
    dockets: Path,
    fr_docket_links: Path,
    documents: Path,
    document_slugs: dict[str, list[str]],
    quarantine: _Quarantine,
) -> tuple[dict[str, dict[str, set[str]]], set[str], set[str], dict[str, int]]:
    """Walk both agency-field joins into ``code -> path -> {document}``."""

    docket_rows = _read_rows(dockets, _DOCKET_COLUMNS)
    docket_codes: dict[str, str] = {}
    for row in docket_rows:
        docket_id = _text(row.get("docket_id"))
        if not docket_id:
            continue
        code = _text(row.get("agency_code"))
        if not code:
            quarantine.add(
                source="dockets",
                evidence_field="agency_code",
                reasons=["docket_missing_agency_code"],
                docket_ref=docket_id,
            )
            continue
        docket_codes[docket_id] = code

    evidence: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    codes_in_dockets = set(docket_codes.values())

    # The normalized index is what recovers the upstream defect's decorated
    # references. A key covering more than one real docket is a collision the
    # tool refuses rather than resolves.
    normalized_index: dict[str, set[str]] = defaultdict(set)
    for docket_id in docket_codes:
        normalized_index[normalize_docket_id(docket_id)].add(docket_id)

    link_rows = _read_rows(fr_docket_links, _FR_LINK_COLUMNS)
    link_foreign = 0
    link_ambiguous = 0
    link_joined_raw = 0
    link_joined_normalized = 0
    for row in link_rows:
        docket_id = _text(row.get("docket_id"))
        code = docket_codes.get(docket_id)
        via_normalization = False
        if code is None:
            key = normalize_docket_id(docket_id)
            matches = normalized_index.get(key) if key else None
            if not matches:
                link_foreign += 1
                continue
            if len(matches) > 1:
                link_ambiguous += 1
                quarantine.add(
                    source="fr_docket_links",
                    evidence_field="docket_id",
                    reasons=["ambiguous_normalized_docket"],
                    document_ref=_text(row.get("document_number")) or None,
                    docket_ref=docket_id,
                    raw_value=canonical_json(sorted(matches))[:200],
                )
                continue
            code = docket_codes[next(iter(matches))]
            via_normalization = True
        document_number = _text(row.get("document_number"))
        if not document_number:
            continue
        if document_number not in document_slugs:
            quarantine.add(
                source="fr_docket_links",
                evidence_field="document_number",
                reasons=["document_not_in_federal_register"],
                document_ref=document_number,
                agency_code=code,
                docket_ref=docket_id,
            )
            continue
        evidence[code]["dockets_fr_links"].add(document_number)
        if via_normalization:
            link_joined_normalized += 1
        else:
            link_joined_raw += 1

    document_rows = _read_rows(documents, _DOCUMENT_COLUMNS)
    codes_in_documents: set[str] = set()
    documents_with_fr_doc_num = 0
    documents_joined = 0
    for row in document_rows:
        code = _text(row.get("agency_code"))
        if code:
            codes_in_documents.add(code)
        document_number = _text(row.get("fr_doc_num"))
        if not document_number:
            continue
        documents_with_fr_doc_num += 1
        if not code:
            continue
        if document_number not in document_slugs:
            quarantine.add(
                source="documents",
                evidence_field="fr_doc_num",
                reasons=["document_not_in_federal_register"],
                document_ref=document_number,
                agency_code=code,
            )
            continue
        evidence[code]["documents_fr_doc_num"].add(document_number)
        documents_joined += 1

    coverage = {
        "dockets_rows": len(docket_rows),
        "dockets_rows_with_agency_code": len(docket_codes),
        "fr_docket_links_rows": len(link_rows),
        "fr_docket_links_rows_joined": link_joined_raw + link_joined_normalized,
        "fr_docket_links_rows_joined_raw": link_joined_raw,
        "fr_docket_links_rows_joined_after_normalization": link_joined_normalized,
        "fr_docket_links_rows_with_foreign_identifier": link_foreign,
        "fr_docket_links_rows_with_ambiguous_normalized_docket": link_ambiguous,
        "documents_rows": len(document_rows),
        "documents_rows_with_fr_doc_num": documents_with_fr_doc_num,
        "documents_rows_joined": documents_joined,
    }
    return evidence, codes_in_dockets, codes_in_documents, coverage


def _rank_candidates(supports: Counter[str], total_documents: int, directory: _AgencyDirectory) -> list[str]:
    """Order candidate slugs: share, then specificity within the margin.

    Candidates within ``SPECIFICITY_MARGIN`` of the best share are treated as
    tied on evidence, so the deeper slug wins — a crosswalk wants the
    sub-agency, not the department it always co-occurs with.
    """

    if not supports:
        return []
    shares = {slug: supports[slug] / total_documents for slug in supports}
    best = max(shares.values())
    tied = {slug for slug, share in shares.items() if share >= best - SPECIFICITY_MARGIN}

    def sort_key(slug: str) -> tuple[int, int, float, str]:
        return (
            0 if slug in tied else 1,
            -directory.depth(slug) if slug in tied else 0,
            -shares[slug],
            slug,
        )

    return sorted(shares, key=sort_key)


def _tier_for(share: float, support_documents: int) -> str:
    if support_documents <= 0:
        return "unmapped"
    if share >= CONFIDENT_SHARE and support_documents >= MIN_CONFIDENT_DOCUMENTS:
        return "confident"
    if share >= PROBABLE_SHARE and support_documents >= MIN_PROBABLE_DOCUMENTS:
        return "probable"
    return "ambiguous"


def _build_code_tables(
    *,
    evidence: dict[str, dict[str, set[str]]],
    codes_in_dockets: set[str],
    codes_in_documents: set[str],
    document_slugs: dict[str, list[str]],
    directory: _AgencyDirectory,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Counter[str], Counter[str]]:
    crosswalk_rows: list[dict[str, Any]] = []
    code_rows: list[dict[str, Any]] = []
    tier_histogram: Counter[str] = Counter({tier: 0 for tier in TIERS})
    support_by_path: Counter[str] = Counter({path: 0 for path in EVIDENCE_PATHS})

    universe = sorted(codes_in_dockets | codes_in_documents | set(evidence))
    for code in universe:
        by_path = evidence.get(code, {})
        documents: set[str] = set()
        for path_documents in by_path.values():
            documents |= path_documents
        total_documents = len(documents)

        path_counts = {path: len(by_path.get(path, set())) for path in EVIDENCE_PATHS}
        for path, count in path_counts.items():
            support_by_path[path] += count

        supports: Counter[str] = Counter()
        for document_number in documents:
            for slug in document_slugs.get(document_number, ()):
                supports[slug] += 1

        ordered = _rank_candidates(supports, total_documents, directory)
        primary = ordered[0] if ordered else None
        confidence_share = supports[primary] / total_documents if primary else 0.0
        tier = _tier_for(confidence_share, total_documents)
        tier_histogram[tier] += 1

        support_by_path_json = canonical_json(path_counts)
        for rank, slug in enumerate(ordered, start=1):
            agency_id, parent_id = directory.identity(slug)
            row = {
                "agency_code": code,
                "agency_slug": slug,
                "agency_id": agency_id,
                "parent_id": parent_id,
                "parent_slug": directory.parent_slug(slug),
                "depth": str(directory.depth(slug)),
                "support_documents": str(supports[slug]),
                "support_by_path_json": support_by_path_json,
                "share": _share(supports[slug], total_documents),
                "rank": str(rank),
                "is_primary": _flag(rank == 1),
                "tier": tier,
            }
            row["crosswalk_id"] = stable_id("urn:spicy-regs:agency-crosswalk", row)
            crosswalk_rows.append(row)

        # Membership in the dockets table does not mean the evidence came
        # from it: a code can be dockets-registered yet drawn entirely from
        # the thin documents bridge. Downstream needs that distinction.
        code_row = {
            "agency_code": code,
            "in_dockets_table": _flag(code in codes_in_dockets),
            "in_documents_table": _flag(code in codes_in_documents),
            "tier": tier,
            "primary_slug": primary,
            "confidence_share": _share(supports[primary] if primary else 0, total_documents),
            "support_documents": str(total_documents),
            "support_by_path_json": support_by_path_json,
            "dockets_path_documents": str(path_counts["dockets_fr_links"]),
            "documents_path_documents": str(path_counts["documents_fr_doc_num"]),
            "evidence_is_documents_only": _flag(total_documents > 0 and path_counts["dockets_fr_links"] == 0),
            "candidate_count": str(len(ordered)),
            "candidate_slugs_json": canonical_json(ordered),
        }
        code_row["agency_code_id"] = stable_id("urn:spicy-regs:agency-code", code_row)
        code_rows.append(code_row)

    crosswalk_rows.sort(key=lambda row: (row["agency_code"], int(row["rank"]), row["agency_slug"]))
    code_rows.sort(key=lambda row: row["agency_code"])
    return crosswalk_rows, code_rows, tier_histogram, support_by_path


def _build_parent_table(directory: _AgencyDirectory) -> list[dict[str, Any]]:
    rows = []
    for slug in directory.slugs:
        agency_id, parent_id = directory.identity(slug)
        rows.append(
            {
                "agency_slug": slug,
                "agency_id": agency_id,
                "parent_id": parent_id,
                "parent_slug": directory.parent_slug(slug),
                "depth": str(directory.depth(slug)),
                "documents": str(directory.documents.get(slug, 0)),
            }
        )
    return rows


def _build_cfr_table(
    cfr_documents: dict[tuple[str, str], set[str]],
    document_slugs: dict[str, list[str]],
    directory: _AgencyDirectory,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (title, part), documents in sorted(cfr_documents.items()):
        part_documents = len(documents)
        supports: Counter[str] = Counter()
        for document_number in documents:
            for slug in document_slugs.get(document_number, ()):
                supports[slug] += 1
        # Most-citing first; ties broken toward the deeper slug. See
        # CFR_PRIMARY_LABEL -- rank 1 is not the "owning" agency.
        ordered = sorted(
            supports,
            key=lambda slug: (-supports[slug], -directory.depth(slug), slug),
        )
        for rank, slug in enumerate(ordered, start=1):
            row = {
                "cfr_title": title,
                "cfr_part": part,
                "agency_slug": slug,
                "documents": str(supports[slug]),
                "part_documents": str(part_documents),
                "share": _share(supports[slug], part_documents),
                "rank": str(rank),
                "is_most_citing": _flag(rank == 1),
            }
            row["cfr_agency_id"] = stable_id("urn:spicy-regs:cfr-part-agency", row)
            rows.append(row)
    return rows


def _write_string_table(path: Path, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    schema = pa.schema([(column, pa.string()) for column in columns])
    arrays = [pa.array([row.get(column) for row in rows], type=pa.string()) for column in columns]
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), path, compression="zstd")


def build_artifact(
    *,
    federal_register: Path,
    dockets: Path,
    fr_docket_links: Path,
    documents: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """Build the artifact and return its (already written) receipt."""

    quarantine = _Quarantine()
    (
        document_slugs,
        directory,
        cfr_documents,
        federal_register_rows,
        documents_with_cfr_references,
    ) = _collect_federal_register(federal_register, quarantine)

    evidence, codes_in_dockets, codes_in_documents, coverage = _collect_code_evidence(
        dockets=dockets,
        fr_docket_links=fr_docket_links,
        documents=documents,
        document_slugs=document_slugs,
        quarantine=quarantine,
    )

    crosswalk_rows, code_rows, tier_histogram, support_by_path = _build_code_tables(
        evidence=evidence,
        codes_in_dockets=codes_in_dockets,
        codes_in_documents=codes_in_documents,
        document_slugs=document_slugs,
        directory=directory,
    )
    parent_rows = _build_parent_table(directory)
    cfr_rows = _build_cfr_table(cfr_documents, document_slugs, directory)
    quarantine_rows = sorted(quarantine.rows, key=lambda row: (row["source"], row["quarantine_id"]))

    output_dir.mkdir(parents=True, exist_ok=True)
    tables = (
        ("agency-crosswalk.parquet", CROSSWALK_COLUMNS, crosswalk_rows),
        ("agency-codes.parquet", CODE_COLUMNS, code_rows),
        ("agency-parents.parquet", PARENT_COLUMNS, parent_rows),
        ("cfr-part-agencies.parquet", CFR_COLUMNS, cfr_rows),
        ("quarantine.parquet", QUARANTINE_COLUMNS, quarantine_rows),
    )
    for name, columns, rows in tables:
        _write_string_table(output_dir / name, columns, rows)

    slugs_with_parent = sum(1 for row in parent_rows if row["parent_id"] is not None)
    slugs_with_resolved_parent = sum(1 for row in parent_rows if row["parent_slug"] is not None)

    counts = {
        "agency_codes_total": len(code_rows),
        "agency_codes_in_dockets_table": len(codes_in_dockets),
        "agency_codes_in_documents_table": len(codes_in_documents),
        "tier_histogram": {tier: tier_histogram[tier] for tier in TIERS},
        "crosswalk_rows": len(crosswalk_rows),
        "support_documents_by_path": dict(sorted(support_by_path.items())),
        "agency_slugs_total": len(parent_rows),
        "agency_slugs_with_parent": slugs_with_parent,
        "agency_slugs_with_resolved_parent_slug": slugs_with_resolved_parent,
        "federal_register_documents": len(document_slugs),
        "documents_with_cfr_references": documents_with_cfr_references,
        "cfr_title_part_pairs": len(cfr_documents),
        "cfr_part_agency_rows": len(cfr_rows),
        "quarantined_rows_total": len(quarantine_rows),
        "quarantined_rows_by_source": dict(sorted(Counter(row["source"] for row in quarantine_rows).items())),
        "quarantine_by_source_and_reason": {
            source: dict(sorted(reasons.items())) for source, reasons in sorted(quarantine.reasons.items())
        },
    }

    receipt = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "tier_policy": TIER_POLICY,
        "thresholds": {
            "confident_share": CONFIDENT_SHARE,
            "probable_share": PROBABLE_SHARE,
            "min_confident_documents": MIN_CONFIDENT_DOCUMENTS,
            "min_probable_documents": MIN_PROBABLE_DOCUMENTS,
            "specificity_margin": SPECIFICITY_MARGIN,
        },
        "evidence_paths": list(EVIDENCE_PATHS),
        "docket_normalization": {
            "policy": DOCKET_NORMALIZATION_POLICY,
            "upstream_defect": DOCKET_NORMALIZATION_DEFECT,
            "rules": list(DOCKET_NORMALIZATION_RULES),
            "decoration_pattern": DOCKET_DECORATION_PATTERN,
        },
        "inputs": {
            "federal_register": {
                "path": _pin_path(federal_register),
                "sha256": file_sha256(federal_register),
                "rows": federal_register_rows,
            },
            "dockets": {
                "path": _pin_path(dockets),
                "sha256": file_sha256(dockets),
                "rows": coverage["dockets_rows"],
            },
            "fr_docket_links": {
                "path": _pin_path(fr_docket_links),
                "sha256": file_sha256(fr_docket_links),
                "rows": coverage["fr_docket_links_rows"],
            },
            "documents": {
                "path": _pin_path(documents),
                "sha256": file_sha256(documents),
                "rows": coverage["documents_rows"],
            },
        },
        "artifacts": {name: {"sha256": file_sha256(output_dir / name), "rows": len(rows)} for name, _, rows in tables},
        "counts": counts,
        "coverage": coverage,
        "coverage_labels": {
            "tier_policy_note": TIER_LABEL,
            "specificity_note": SPECIFICITY_LABEL,
            "join_note": JOIN_LABEL,
            "denominator_note": DENOMINATOR_LABEL,
            "cfr_primary_note": CFR_PRIMARY_LABEL,
        },
    }
    receipt["artifact_id"] = stable_id("urn:spicy-regs:agency-crosswalk-artifact", receipt)
    (output_dir / "receipt.json").write_text(canonical_json(receipt) + "\n", encoding="utf-8")
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    candidate = REPO_ROOT / "output/rin-ontology-revision-candidate"
    parser.add_argument("--federal-register", type=Path, default=candidate / "federal_register.parquet")
    parser.add_argument("--dockets", type=Path, default=candidate / "dockets.parquet")
    parser.add_argument("--fr-docket-links", type=Path, default=candidate / "fr_docket_links.parquet")
    parser.add_argument("--documents", type=Path, default=candidate / "documents.parquet")
    parser.add_argument("--output", type=Path, default=REPO_ROOT / "output/agency-crosswalk-2026-08-02")
    args = parser.parse_args(argv)

    receipt = build_artifact(
        federal_register=args.federal_register,
        dockets=args.dockets,
        fr_docket_links=args.fr_docket_links,
        documents=args.documents,
        output_dir=args.output,
    )
    print(f"artifact_id: {receipt['artifact_id']}", file=sys.stderr)
    print(f"receipt: {args.output / 'receipt.json'}", file=sys.stderr)
    print(canonical_json(receipt["counts"]), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
