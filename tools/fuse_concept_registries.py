"""Fuse public controlled vocabularies without confusing source with facet.

Every registry row carries a semantic ``facet`` used by tag policy and a
``source_vocabulary`` used for identity, provenance, shortlist quotas, and
Rulespec ``inScheme``. The legacy ``scheme`` column remains a compatibility
mirror of ``facet``; it never carries an external vocabulary in newly written
rows. Retrieval time, licence, and upstream record identity live in a sidecar.

Two rules govern the fusion and both are load-bearing:

* **Authority identities stay distinct.** "Trademarks" appearing in three
  vocabularies remains three concepts. Exact same-label copies receive an
  explicit unreviewed mapping artifact for later review. The selector does not
  collapse them: every authority id remains separately selectable, and no
  ``skos:exactMatch`` is asserted.
* **The prior registry's identity and content are frozen.** Every existing
  ``concept_id``, label, definition, hierarchy link, and attestation survives.
  The v2 schema adds explicit dimensions and migrates an external-valued
  compatibility ``scheme`` to its semantic facet. A source term whose
  normalized label already exists within the same source vocabulary is
  enrichment: its extra labels are recorded in the sidecar and no second
  authority concept is minted.

Concept ids are minted with the repository idiom
``stable_id("concept", source_vocabulary, normalize_label(pref_label))``. The
existing Federal Register ids predate that rule and remain stable through the
frozen-row compatibility path. Re-running the Federal Register thesaurus finds
those rows by inferred source vocabulary plus normalized label, so enrichment
retains their ids rather than trying to mint replacements.

Every parser takes already-decoded text or an iterable of lines, so the test
suite exercises each one on tiny synthetic fixtures with no network and no
large files.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import sys
import zipfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:  # pragma: no cover - import shim
    sys.path.insert(0, str(REPO_ROOT / "src"))

from spicy_regs.ontology.common import (  # noqa: E402
    RunContext,
    canonical_json,
    iso_now,
    read_parquet_rows,
    stable_id,
    write_parquet_rows,
)
from spicy_regs.ontology.concept_dimensions import (  # noqa: E402
    FEDERAL_REGISTER_SOURCE_VOCABULARY,
    concept_facet,
    concept_source_vocabulary,
    with_concept_dimensions,
)
from spicy_regs.ontology.concepts import CONCEPT_COLUMNS, normalize_label  # noqa: E402

SCHEMA_VERSION = "fused-concept-registry-v2"

# Source vocabularies. ``FR_SCHEME`` remains as a test/import compatibility
# alias; new code uses the explicit name.
FR_SOURCE_VOCABULARY = FEDERAL_REGISTER_SOURCE_VOCABULARY
FR_SCHEME = FR_SOURCE_VOCABULARY
CRS_SUBJECT_SOURCE_VOCABULARY = "crs-subjects"
CRS_POLICY_SOURCE_VOCABULARY = "crs-policy-areas"
TSCA_SOURCE_VOCABULARY = "epa-tsca"
FAST_SOURCE_VOCABULARY = "fast-topical"
# Import compatibility for tests and callers written against v1.
CRS_SUBJECT_SCHEME = CRS_SUBJECT_SOURCE_VOCABULARY
CRS_POLICY_SCHEME = CRS_POLICY_SOURCE_VOCABULARY
TSCA_SCHEME = TSCA_SOURCE_VOCABULARY
FAST_SCHEME = FAST_SOURCE_VOCABULARY

SIDECAR_COLUMNS: tuple[str, ...] = (
    "concept_id",
    "facet",
    "source_vocabulary",
    "source",
    "source_url",
    "source_id",
    "source_status",
    "retrieved_at",
    "license",
    "license_url",
    "relation",
    "enrichment_alt_labels_json",
    "note",
)

MAPPING_COLUMNS: tuple[str, ...] = (
    "mapping_id",
    "subject_concept_id",
    "object_concept_id",
    "facet",
    "subject_source_vocabulary",
    "object_source_vocabulary",
    "relation",
    "basis",
    "status",
)

# ``relation`` values on a sidecar row.
RELATION_MINTED = "minted"
RELATION_ENRICHMENT = "enriches-frozen-row"


@dataclass(frozen=True)
class SourceSpec:
    """Where one vocabulary came from and under what terms it may be used."""

    key: str
    facet: str
    source_vocabulary: str
    title: str
    url: str
    license: str
    license_url: str
    actor_id: str
    notice: str

    @property
    def scheme(self) -> str:
        """Compatibility alias for callers written before the seam fix."""
        return self.source_vocabulary


SOURCES: dict[str, SourceSpec] = {
    "fr-thesaurus": SourceSpec(
        key="fr-thesaurus",
        facet="subject",
        source_vocabulary=FR_SOURCE_VOCABULARY,
        title="Federal Register Thesaurus of Indexing Terms",
        url="https://www.archives.gov/files/federal-register/cfr/thesaurus-alpha.txt",
        license="public domain (US Government work, 17 U.S.C. 105)",
        license_url="https://www.usa.gov/government-copyright",
        actor_id="federal-register-thesaurus-alpha:v1",
        notice="Federal Register Thesaurus of Indexing Terms, U.S. National Archives and Records Administration. US Government work, public domain.",
    ),
    "crs-subjects": SourceSpec(
        key="crs-subjects",
        facet="subject",
        source_vocabulary=CRS_SUBJECT_SCHEME,
        title="CRS Legislative Subject Terms",
        url="https://www.govinfo.gov/bulkdata/BILLSTATUS",
        license="public domain (US Government work, 17 U.S.C. 105)",
        license_url="https://www.govinfo.gov/about/policies",
        actor_id="crs-legislative-subject-terms:v1",
        notice="CRS Legislative Subject Terms as carried in GPO govinfo BILLSTATUS bulk data. US Government work, public domain.",
    ),
    "crs-policy-areas": SourceSpec(
        key="crs-policy-areas",
        facet="subject",
        source_vocabulary=CRS_POLICY_SCHEME,
        title="CRS Policy Area Terms",
        url="https://www.govinfo.gov/bulkdata/BILLSTATUS",
        license="public domain (US Government work, 17 U.S.C. 105)",
        license_url="https://www.govinfo.gov/about/policies",
        actor_id="crs-policy-area-terms:v1",
        notice="CRS Policy Area Terms as carried in GPO govinfo BILLSTATUS bulk data. US Government work, public domain.",
    ),
    "epa-tsca": SourceSpec(
        key="epa-tsca",
        facet="regulated_entity",
        source_vocabulary=TSCA_SCHEME,
        title="EPA non-confidential TSCA Chemical Substance Inventory",
        url="https://www.epa.gov/tsca-inventory/how-access-tsca-inventory",
        license="public domain (US Government work, 17 U.S.C. 105)",
        license_url="https://www.epa.gov/web-policies-and-procedures",
        actor_id="epa-tsca-inventory:v1",
        notice="Non-confidential TSCA Chemical Substance Inventory, U.S. Environmental Protection Agency. US Government work, public domain.",
    ),
    "fast-topical": SourceSpec(
        key="fast-topical",
        facet="subject",
        source_vocabulary=FAST_SCHEME,
        title="FAST (Faceted Application of Subject Terminology), Topical facet",
        url="https://www.oclc.org/research/areas/data-science/fast/download.html",
        license="ODC-By 1.0 (Open Data Commons Attribution License)",
        license_url="https://opendatacommons.org/licenses/by/1-0/",
        actor_id="oclc-fast-topical:v1",
        notice=(
            "This product uses data from FAST (Faceted Application of Subject Terminology), "
            "made available by OCLC Online Computer Library Center, Inc. under the ODC "
            "Attribution License (ODC-By 1.0): https://opendatacommons.org/licenses/by/1-0/"
        ),
    ),
}


class FusionError(RuntimeError):
    """The inputs cannot produce a registry that satisfies the two rules."""


@dataclass
class SourceTerm:
    """One vocabulary entry, before it is minted into a registry row."""

    pref_label: str
    alt_labels: list[str] = field(default_factory=list)
    definition: str | None = None
    source_id: str | None = None
    source_status: str | None = None
    dropped_alt_labels: list[str] = field(default_factory=list)

    def normalized(self) -> str:
        return normalize_label(self.pref_label)


def _clean(value: object) -> str:
    return " ".join(str(value or "").split())


def retain_alt_label(alt: str) -> bool:
    """Reject an alias that would match nearly every document as a substring.

    ``normalize_label`` keeps only ASCII ``[a-z0-9]``, so a transliterated alias
    such as ``Ḥūr`` or ``Bīṇā`` collapses to ``r`` or ``b``. The consuming
    selector scores an alias that is a *substring* of the text at 1.0, and a
    one-letter alias is a substring of essentially every document -- so those
    handful of concepts would take a permanent share of every candidate list
    while carrying no information. Genuine short aliases (``TV``, ``Ox``) are
    lost with them; that is the cheaper error, and the raw files named in the
    manifest still hold them.

    Only whole-alias degeneracy is rejected. A multi-word alias keeps its short
    tokens, because ``Hansen's disease`` and ``4-H clubs`` are real labels that
    happen to normalize with a one-character token in the middle.
    """
    normalized = normalize_label(alt)
    return bool(normalized) and (" " in normalized or len(normalized) > 2)


def _merge_terms(terms: Iterable[SourceTerm]) -> list[SourceTerm]:
    """Collapse repeats of one label within a single source vocabulary.

    This function never receives terms from two authorities; each vocabulary
    is folded on its own before any terms meet a registry row.
    """
    merged: dict[str, SourceTerm] = {}
    for term in terms:
        key = term.normalized()
        if not key:
            continue
        existing = merged.get(key)
        if existing is None:
            merged[key] = SourceTerm(
                pref_label=term.pref_label,
                alt_labels=list(term.alt_labels),
                definition=term.definition,
                source_id=term.source_id,
                source_status=term.source_status,
            )
            continue
        for alt in term.alt_labels:
            if alt not in existing.alt_labels:
                existing.alt_labels.append(alt)
        existing.definition = existing.definition or term.definition
        existing.source_id = existing.source_id or term.source_id
        existing.source_status = existing.source_status or term.source_status
    for term in merged.values():
        own = term.normalized()
        candidates = {
            alt for alt in (_clean(value) for value in term.alt_labels) if alt and normalize_label(alt) != own
        }
        term.alt_labels = sorted((alt for alt in candidates if retain_alt_label(alt)), key=normalize_label)
        term.dropped_alt_labels = sorted(alt for alt in candidates if not retain_alt_label(alt))
    return sorted(merged.values(), key=lambda term: (term.normalized(), term.pref_label))


# --------------------------------------------------------------------------
# Federal Register Thesaurus
# --------------------------------------------------------------------------

_FR_CATEGORY_CODE = re.compile(r"\s*\(\s*\d[\d,\s]*\)\s*$")
_FR_MARKERS = frozenset({"see", "sa", "x", "xx"})
_FR_HEADER_END = "related terms:"


def _fr_term_label(line: str) -> str:
    """Drop the trailing ``(02, 13)`` category codes, keeping inline parens."""
    return _clean(_FR_CATEGORY_CODE.sub("", line))


def parse_fr_thesaurus(text: str) -> list[SourceTerm]:
    """Parse the alphabetic Federal Register Thesaurus into preferred terms.

    The file marks a non-preferred term with a ``see`` block pointing at the
    preferred term(s), and lists a preferred term's own variants under ``x``.
    Both directions land in ``alt_labels`` of the preferred term, which is what
    "cross-reference variants become altLabels" means for this file's notation.

    ``sa`` (see-also) and ``xx`` (broader) blocks relate two *preferred* terms
    rather than naming a label, so they are read and discarded here:
    ``broader_id`` must resolve inside the table (``assert_concept_graphs``) and
    a relation table is out of scope.

    A parenthesised scope note becomes the term's definition. Scope notes wrap
    onto unindented continuation lines, so an unbalanced ``(`` keeps consuming
    lines regardless of indentation -- without that, a continuation line reads
    as a new term.
    """
    terms: dict[str, SourceTerm] = {}
    order: list[str] = []
    variants: list[tuple[str, str]] = []  # (target label, variant label)

    def term_for(label: str) -> SourceTerm:
        key = normalize_label(label)
        if key not in terms:
            terms[key] = SourceTerm(pref_label=label)
            order.append(key)
        return terms[key]

    current: SourceTerm | None = None
    current_label = ""
    marker: str | None = None
    pending_note: list[str] | None = None
    started = False

    for raw in text.splitlines():
        line = raw.rstrip()
        stripped = line.strip()
        if pending_note is not None:
            pending_note.append(stripped)
            joined = " ".join(pending_note)
            if joined.count("(") <= joined.count(")"):
                if current is not None and not current.definition:
                    current.definition = _clean(joined.strip("()").strip())
                pending_note = None
            continue
        if not stripped:
            continue
        if not started:
            if stripped.lower().endswith(_FR_HEADER_END):
                started = True
            continue
        indented = line[:1].isspace()
        if indented and stripped in _FR_MARKERS:
            marker = stripped
            continue
        if indented and stripped.startswith("("):
            if stripped.count("(") > stripped.count(")"):
                pending_note = [stripped]
            elif current is not None and not current.definition:
                current.definition = _clean(stripped.strip("()").strip())
            continue
        if indented:
            if marker == "see" and current_label:
                # ``current_label`` is a non-preferred entry; it is a variant of
                # this target rather than a concept of its own.
                variants.append((stripped, current_label))
            elif marker == "x" and current is not None:
                current.alt_labels.append(stripped)
            continue
        label = _fr_term_label(line)
        if not label or not normalize_label(label):
            continue
        current_label = label
        marker = None
        # Every heading is provisional: a following ``see`` block turns it into
        # a variant of another term, and the entry created here is dropped.
        current = term_for(label)

    for target, variant in variants:
        target_key = normalize_label(target)
        variant_key = normalize_label(variant)
        holder = terms.get(target_key) or term_for(target)
        if variant_key and variant_key != target_key:
            holder.alt_labels.append(variant)

    referenced = {normalize_label(variant) for _, variant in variants}
    result = [terms[key] for key in order if key not in referenced]
    return _merge_terms(result)


# --------------------------------------------------------------------------
# CRS legislative subject terms and policy areas (GPO BILLSTATUS bulk data)
# --------------------------------------------------------------------------


def parse_billstatus_terms(xml_text: str) -> tuple[list[str], list[str]]:
    """Return ``(policy_areas, legislative_subjects)`` named in one bill record.

    congress.gov serves the published vocabulary pages behind a bot filter that
    refuses non-browser clients, so the terms are read from the same Library of
    Congress assignments as republished in GPO's BILLSTATUS bulk data. The terms
    are identical strings; only the carrier differs, and the manifest says so.
    """
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return ([], [])
    policy = [
        _clean(element.findtext("name")) for element in root.iter("policyArea") if _clean(element.findtext("name"))
    ]
    subjects = [
        _clean(item.findtext("name"))
        for container in root.iter("legislativeSubjects")
        for item in container.iter("item")
        if _clean(item.findtext("name"))
    ]
    return (policy, subjects)


def collect_billstatus_terms(documents: Iterable[str]) -> tuple[list[SourceTerm], list[SourceTerm]]:
    """Fold many bill records into the two vocabularies they draw on."""
    policy: set[str] = set()
    subjects: set[str] = set()
    for text in documents:
        found_policy, found_subjects = parse_billstatus_terms(text)
        policy.update(found_policy)
        subjects.update(found_subjects)
    return (
        _merge_terms(SourceTerm(pref_label=label) for label in sorted(policy)),
        _merge_terms(SourceTerm(pref_label=label) for label in sorted(subjects)),
    )


def iter_zip_members(path: Path, suffix: str) -> Iterator[str]:
    """Yield the decoded text of every member of a zip with ``suffix``."""
    with zipfile.ZipFile(path) as archive:
        for name in archive.namelist():
            if not name.lower().endswith(suffix):
                continue
            with archive.open(name) as handle:
                yield handle.read().decode("utf-8", errors="replace")


# --------------------------------------------------------------------------
# EPA non-confidential TSCA Inventory
# --------------------------------------------------------------------------


def parse_tsca_inventory(rows: Iterable[Mapping[str, Any]]) -> list[SourceTerm]:
    """Turn TSCA Inventory records into substance terms keyed by CAS number.

    The published inventory carries no synonym column, so ``alt_labels`` stays
    empty; the CAS Registry Number is kept as the term's ``source_id`` and is
    published on the row as an ``external_ids_json`` entry marked ``cas``,
    never smuggled into a label where it would perturb the lexical selector.
    The ACTIVE/INACTIVE commerce flag is recorded in the sidecar rather than
    mapped onto ``status``: an inactive substance is still a real substance, and
    ``deprecated`` would silently remove it from every candidate list.
    """
    terms: list[SourceTerm] = []
    for row in rows:
        name = _clean(row.get("ChemName"))
        if not name or not normalize_label(name):
            continue
        terms.append(
            SourceTerm(
                pref_label=name,
                source_id=_clean(row.get("CASRN")) or None,
                source_status=_clean(row.get("ACTIVITY")) or None,
            )
        )
    return _merge_terms(terms)


def read_tsca_csv(handle: io.TextIOBase) -> list[SourceTerm]:
    return parse_tsca_inventory(csv.DictReader(handle))


# --------------------------------------------------------------------------
# OCLC FAST, topical facet
# --------------------------------------------------------------------------

_NT_TRIPLE = re.compile(r'^<([^>]+)>\s+<([^>]+)>\s+(?:<([^>]+)>|"((?:[^"\\]|\\.)*)")')
_FAST_PREF = "http://www.w3.org/2004/02/skos/core#prefLabel"
_FAST_ALT = "http://www.w3.org/2004/02/skos/core#altLabel"
_FAST_DEPRECATED = "http://www.w3.org/2002/07/owl#deprecated"
_FAST_IDENTIFIER = "http://purl.org/dc/terms/identifier"
_NT_ESCAPES = {"\\": "\\", '"': '"', "n": "\n", "r": "\r", "t": "\t"}


def unescape_ntriples_literal(value: str) -> str:
    """Decode the escape forms an N-Triples literal may carry."""
    out: list[str] = []
    index = 0
    while index < len(value):
        char = value[index]
        if char != "\\" or index + 1 >= len(value):
            out.append(char)
            index += 1
            continue
        marker = value[index + 1]
        if marker in _NT_ESCAPES:
            out.append(_NT_ESCAPES[marker])
            index += 2
        elif marker in {"u", "U"}:
            width = 4 if marker == "u" else 8
            hexits = value[index + 2 : index + 2 + width]
            try:
                out.append(chr(int(hexits, 16)))
            except ValueError:
                out.append(marker)
            index += 2 + width
        else:
            out.append(marker)
            index += 2
    return "".join(out)


def parse_fast_ntriples(lines: Iterable[str]) -> list[SourceTerm]:
    """Read skos prefLabel/altLabel out of the FAST topical N-Triples dump.

    ``owl:deprecated`` subjects are dropped rather than published as
    ``status='deprecated'``: the selector skips deprecated rows anyway, and
    publishing them would demand a resolvable ``replaced_by`` graph that the
    registry does not carry.
    """
    pref: dict[str, str] = {}
    alts: dict[str, list[str]] = {}
    identifiers: dict[str, str] = {}
    deprecated: set[str] = set()
    for line in lines:
        match = _NT_TRIPLE.match(line.strip())
        if match is None:
            continue
        subject, predicate, iri_object, literal = match.groups()
        if predicate == _FAST_DEPRECATED:
            deprecated.add(subject)
        elif predicate == _FAST_PREF and literal is not None:
            pref.setdefault(subject, unescape_ntriples_literal(literal))
        elif predicate == _FAST_ALT and literal is not None:
            alts.setdefault(subject, []).append(unescape_ntriples_literal(literal))
        elif predicate == _FAST_IDENTIFIER and literal is not None:
            identifiers.setdefault(subject, unescape_ntriples_literal(literal))
        elif predicate == _FAST_IDENTIFIER and iri_object is not None:
            identifiers.setdefault(subject, iri_object)
    terms = [
        SourceTerm(
            pref_label=_clean(label),
            alt_labels=[_clean(value) for value in alts.get(subject, [])],
            source_id=identifiers.get(subject) or subject.rsplit("/", 1)[-1],
        )
        for subject, label in pref.items()
        if subject not in deprecated and _clean(label)
    ]
    return _merge_terms(terms)


def read_fast_archive(path: Path) -> list[SourceTerm]:
    """Stream the topical N-Triples out of the distributed zip (or a raw file)."""
    if path.suffix.lower() != ".zip":
        with path.open(encoding="utf-8", errors="replace") as handle:
            return parse_fast_ntriples(handle)
    with zipfile.ZipFile(path) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".nt")]
        if not names:
            raise FusionError(f"{path} carries no .nt member")
        with archive.open(names[0]) as raw:
            return parse_fast_ntriples(io.TextIOWrapper(raw, encoding="utf-8", errors="replace"))


# --------------------------------------------------------------------------
# Minting and fusion
# --------------------------------------------------------------------------


def mint_concept_id(source_vocabulary: str, pref_label: str) -> str:
    """Mint an authority-scoped concept id."""
    return stable_id("concept", source_vocabulary, normalize_label(pref_label))


def _definition(spec: SourceSpec, term: SourceTerm) -> str:
    if term.definition:
        return term.definition
    return f"{spec.title} term: {term.pref_label}."


def _external_ids(spec: SourceSpec, term: SourceTerm) -> str:
    entry: dict[str, str] = {"scheme": spec.key, "value": term.pref_label}
    if term.source_id:
        entry["id"] = term.source_id
    payload: list[dict[str, str]] = [entry]
    if spec.key == "epa-tsca" and term.source_id:
        payload.append({"scheme": "cas", "value": term.source_id})
    if spec.key == "fast-topical" and term.source_id:
        payload.append(
            {"scheme": "fast", "value": term.source_id, "iri": f"http://id.worldcat.org/fast/{term.source_id}"}
        )
    return canonical_json(sorted(payload, key=canonical_json))


def fuse(
    *,
    existing: Sequence[Mapping[str, Any]],
    contributions: Sequence[tuple[SourceSpec, Sequence[SourceTerm]]],
    context: RunContext,
    retrieved_at: Mapping[str, str],
) -> tuple[list[dict], list[dict], dict[str, dict[str, int]]]:
    """Return ``(registry rows, sidecar rows, per-vocabulary counts)``.

    Frozen identities come first. A source term is compared only with rows
    carrying its own source vocabulary; a label shared with another authority
    mints an independent concept.
    """
    registry: list[dict] = []
    for row in existing:
        explicit = with_concept_dimensions(row)
        explicit["scheme"] = explicit["facet"]
        registry.append({column: explicit.get(column) for column in CONCEPT_COLUMNS})
    sidecar: list[dict] = []
    counts: dict[str, dict[str, int]] = {}

    frozen_ids = {str(row["concept_id"]) for row in registry}
    by_vocabulary: dict[str, dict[str, str]] = {}
    for row in registry:
        vocabulary = concept_source_vocabulary(row)
        by_vocabulary.setdefault(vocabulary, {})[normalize_label(row.get("pref_label"))] = str(row["concept_id"])

    for spec, terms in contributions:
        seen = by_vocabulary.setdefault(spec.source_vocabulary, {})
        minted = 0
        enriched = 0
        dropped_aliases = 0
        for term in terms:
            dropped_aliases += len(term.dropped_alt_labels)
            key = term.normalized()
            if not key:
                continue
            existing_id = seen.get(key)
            base = {
                "facet": spec.facet,
                "source_vocabulary": spec.source_vocabulary,
                "source": spec.key,
                "source_url": spec.url,
                "source_id": term.source_id,
                "source_status": term.source_status,
                "retrieved_at": retrieved_at.get(spec.key),
                "license": spec.license,
                "license_url": spec.license_url,
            }
            if existing_id is not None:
                # Same authority vocabulary and normalized label: enrichment.
                # The frozen row
                # keeps its id, label, definition, and attestation; anything new
                # this source knows is recorded beside it.
                extra = [alt for alt in term.alt_labels if normalize_label(alt) != key]
                sidecar.append(
                    {
                        **base,
                        "concept_id": existing_id,
                        "relation": RELATION_ENRICHMENT,
                        "enrichment_alt_labels_json": canonical_json(extra),
                        "note": (
                            "exact normalized-label match within the same source vocabulary; "
                            "the authority concept id and frozen fields are retained"
                        ),
                    }
                )
                enriched += 1
                continue
            concept_id = mint_concept_id(spec.source_vocabulary, term.pref_label)
            if concept_id in frozen_ids:
                raise FusionError(f"minted id {concept_id} for {term.pref_label!r} collides with a frozen row")
            seen[key] = concept_id
            registry.append(
                {
                    "concept_id": concept_id,
                    "facet": spec.facet,
                    "source_vocabulary": spec.source_vocabulary,
                    "scheme": spec.facet,
                    "pref_label": term.pref_label,
                    "alt_labels_json": canonical_json(term.alt_labels),
                    "definition": _definition(spec, term),
                    "broader_id": None,
                    "status": "active",
                    "replaced_by": None,
                    "external_ids_json": _external_ids(spec, term),
                    **context.provenance(method="deterministic", actor_id=spec.actor_id),
                }
            )
            sidecar.append(
                {
                    **base,
                    "concept_id": concept_id,
                    "relation": RELATION_MINTED,
                    "enrichment_alt_labels_json": "[]",
                    "note": None,
                }
            )
            minted += 1
        counts[spec.key] = {
            "terms": len(terms),
            "minted": minted,
            "enriched_frozen_rows": enriched,
            "alt_labels_dropped_as_degenerate": dropped_aliases,
        }
    return registry, sidecar, counts


def assert_frozen_rows_survive(prior: Sequence[Mapping[str, Any]], fused: Sequence[Mapping[str, Any]]) -> None:
    """Fail unless prior identities and frozen content survive the v2 migration."""
    fused_by_id = {str(row.get("concept_id")): row for row in fused}
    for row in prior:
        concept_id = str(row.get("concept_id"))
        current = fused_by_id.get(concept_id)
        if current is None:
            raise FusionError(f"frozen concept {concept_id} disappeared from the fused registry")
        changed = [
            column
            for column in row
            if column not in {"facet", "source_vocabulary"}
            and not (
                column == "scheme"
                and str(row.get("scheme") or "") not in {"subject", "regulated_entity"}
                and current.get("scheme") == concept_facet(row)
            )
            and row.get(column) != current.get(column)
        ]
        if changed:
            raise FusionError(f"frozen concept {concept_id} was rewritten in columns {changed}")


def _identity_anchors(row: Mapping[str, Any]) -> frozenset[tuple[str, str]]:
    try:
        values = json.loads(str(row.get("external_ids_json") or "[]"))
    except json.JSONDecodeError:
        return frozenset()
    if not isinstance(values, list):
        return frozenset()
    return frozenset(
        (
            str(item.get("scheme") or "").strip().casefold(),
            str(item.get("value") or "").strip().casefold(),
        )
        for item in values
        if isinstance(item, dict)
        and str(item.get("scheme") or "").strip().casefold() in {"cas", "naics"}
        and str(item.get("value") or "").strip()
    )


def presentation_mappings(registry: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Record same-label display groups without asserting semantic equivalence."""
    grouped: dict[tuple[object, ...], list[Mapping[str, Any]]] = {}
    for row in registry:
        facet = concept_facet(row)
        label = normalize_label(row.get("pref_label"))
        if not label:
            continue
        anchors = _identity_anchors(row)
        key: tuple[object, ...] = (facet, label, anchors) if facet == "regulated_entity" and anchors else (facet, label)
        grouped.setdefault(key, []).append(row)

    mappings: list[dict[str, Any]] = []
    for rows in grouped.values():
        ordered = sorted(rows, key=lambda row: str(row.get("concept_id") or ""))
        if len({concept_source_vocabulary(row) for row in ordered}) < 2:
            continue
        representative = ordered[0]
        for other in ordered[1:]:
            subject_id = str(representative["concept_id"])
            object_id = str(other["concept_id"])
            mappings.append(
                {
                    "mapping_id": stable_id(
                        "concept_presentation_mapping",
                        subject_id,
                        object_id,
                        length=24,
                    ),
                    "subject_concept_id": subject_id,
                    "object_concept_id": object_id,
                    "facet": concept_facet(representative),
                    "subject_source_vocabulary": concept_source_vocabulary(representative),
                    "object_source_vocabulary": concept_source_vocabulary(other),
                    "relation": "same-normalized-label-for-presentation",
                    "basis": "exact-normalized-preferred-label",
                    "status": "unreviewed-presentation-only",
                }
            )
    return mappings


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


# --------------------------------------------------------------------------
# Command line
# --------------------------------------------------------------------------


def _load_sources(args: argparse.Namespace) -> tuple[list[tuple[SourceSpec, list[SourceTerm]]], list[str]]:
    contributions: list[tuple[SourceSpec, list[SourceTerm]]] = []
    skipped: list[str] = []

    if args.fr_thesaurus:
        text = Path(args.fr_thesaurus).read_text(encoding="utf-8", errors="replace")
        contributions.append((SOURCES["fr-thesaurus"], parse_fr_thesaurus(text)))
    else:
        skipped.append("fr-thesaurus: no --fr-thesaurus supplied")

    if args.billstatus:
        documents: list[str] = []
        for entry in args.billstatus:
            path = Path(entry)
            if path.is_dir():
                documents.extend(
                    child.read_text(encoding="utf-8", errors="replace") for child in sorted(path.glob("*.xml"))
                )
            else:
                documents.extend(iter_zip_members(path, ".xml"))
        policy, subjects = collect_billstatus_terms(documents)
        contributions.append((SOURCES["crs-policy-areas"], policy))
        contributions.append((SOURCES["crs-subjects"], subjects))
    else:
        skipped.append("crs-subjects/crs-policy-areas: no --billstatus supplied")

    if args.tsca_csv:
        path = Path(args.tsca_csv)
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as archive:
                names = [name for name in archive.namelist() if name.upper().startswith("TSCAINV")]
                if not names:
                    raise FusionError(f"{path} carries no TSCAINV member")
                with archive.open(names[0]) as raw:
                    terms = read_tsca_csv(io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace"))
        else:
            with path.open(encoding="utf-8-sig", errors="replace", newline="") as handle:
                terms = read_tsca_csv(handle)
        contributions.append((SOURCES["epa-tsca"], terms))
    else:
        skipped.append("epa-tsca: no --tsca-csv supplied")

    if args.fast_topical:
        contributions.append((SOURCES["fast-topical"], read_fast_archive(Path(args.fast_topical))))
    else:
        skipped.append("fast-topical: no --fast-topical supplied")

    return contributions, skipped


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--existing-registry", required=True, help="frozen registry parquet copied through unchanged")
    parser.add_argument("--fr-thesaurus", help="thesaurus-alpha.txt")
    parser.add_argument("--billstatus", nargs="*", help="BILLSTATUS zip files or directories of bill XML")
    parser.add_argument("--tsca-csv", help="TSCAINV csv, or the zip carrying it")
    parser.add_argument("--fast-topical", help="FASTTopical.nt or FASTTopical.nt.zip")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--retrieved-at", default=None, help="ISO-8601 instant the raw files were downloaded")
    parser.add_argument("--run-id", default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    retrieved = args.retrieved_at or iso_now()
    context = RunContext.resolve(run_id=args.run_id, prefix="fused-registry")

    existing_path = Path(args.existing_registry)
    existing = read_parquet_rows(existing_path)
    if not existing:
        raise FusionError(f"{existing_path} carries no rows")

    contributions, skipped = _load_sources(args)
    retrieved_at = {spec.key: retrieved for spec, _ in contributions}
    registry, sidecar, counts = fuse(
        existing=existing,
        contributions=contributions,
        context=context,
        retrieved_at=retrieved_at,
    )
    assert_frozen_rows_survive(existing, registry)
    mappings = presentation_mappings(registry)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    registry_path = output_dir / "registry.parquet"
    sidecar_path = output_dir / "provenance.parquet"
    mappings_path = output_dir / "presentation-mappings.parquet"
    write_parquet_rows(registry_path, columns=CONCEPT_COLUMNS, rows=registry)
    write_parquet_rows(sidecar_path, columns=SIDECAR_COLUMNS, rows=sidecar)
    write_parquet_rows(mappings_path, columns=MAPPING_COLUMNS, rows=mappings)

    raw_files = {
        name: {"path": str(Path(value)), "sha256": sha256_path(Path(value)), "bytes": Path(value).stat().st_size}
        for name, value in (
            ("fr-thesaurus", args.fr_thesaurus),
            ("tsca-csv", args.tsca_csv),
            ("fast-topical", args.fast_topical),
        )
        if value
    }
    for entry in args.billstatus or []:
        path = Path(entry)
        if path.is_file():
            raw_files[f"billstatus:{path.name}"] = {
                "path": str(path),
                "sha256": sha256_path(path),
                "bytes": path.stat().st_size,
            }

    per_facet: dict[str, int] = {}
    per_vocabulary: dict[str, int] = {}
    for row in registry:
        facet = concept_facet(row)
        vocabulary = concept_source_vocabulary(row)
        per_facet[facet] = per_facet.get(facet, 0) + 1
        per_vocabulary[vocabulary] = per_vocabulary.get(vocabulary, 0) + 1

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "run_id": context.run_id,
        "asserted_at": context.asserted_at,
        "retrieved_at": retrieved,
        "registry_path": str(registry_path),
        "registry_sha256": sha256_path(registry_path),
        "provenance_path": str(sidecar_path),
        "provenance_sha256": sha256_path(sidecar_path),
        "presentation_mappings_path": str(mappings_path),
        "presentation_mappings_sha256": sha256_path(mappings_path),
        "presentation_mapping_count": len(mappings),
        "presentation_mapping_semantics": (
            "same-normalized-label-for-presentation is an unreviewed display grouping only; "
            "it is not semantic equivalence, skos:exactMatch, merge authority, or identity."
        ),
        "existing_registry": {
            "path": str(existing_path),
            "sha256": sha256_path(existing_path),
            "rows": len(existing),
            "frozen_identity_and_content_stable": True,
            "schema_migration": (
                "fused-registry-v1 external-valued scheme is moved to source_vocabulary; "
                "facet is canonical and compatibility scheme mirrors facet"
            ),
        },
        "total_rows": len(registry),
        "rows_per_facet": dict(sorted(per_facet.items())),
        "rows_per_source_vocabulary": dict(sorted(per_vocabulary.items())),
        "source_counts": counts,
        "skipped_sources": skipped,
        "sources": {
            spec.key: {
                "facet": spec.facet,
                "source_vocabulary": spec.source_vocabulary,
                "title": spec.title,
                "url": spec.url,
                "license": spec.license,
                "license_url": spec.license_url,
                "actor_id": spec.actor_id,
            }
            for spec, _ in contributions
        },
        "raw_files": raw_files,
        "notice": [SOURCES[spec.key].notice for spec, _ in contributions],
        "vocabulary_policy": (
            "Authority vocabularies stay distinct. No concept identity was merged across sources; "
            "one label appearing in several sources remains several concepts. Same-label mappings "
            "are presentation-only and are not semantic equivalence."
        ),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"fused registry: {registry_path}")
    print(f"total rows: {len(registry):,}")
    for vocabulary, count in sorted(per_vocabulary.items()):
        print(f"  {vocabulary:<30} {count:>9,}")
    print("source contributions:")
    for key, values in sorted(counts.items()):
        print(
            f"  {key:<20} terms={values['terms']:>9,} minted={values['minted']:>9,} "
            f"enriched={values['enriched_frozen_rows']:>6,}"
        )
    for line in skipped:
        print(f"skipped: {line}")
    print(f"manifest: {manifest_path}")
    print(f"presentation mappings: {mappings_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
