"""Project one source document into a gate-valid Rulespec (RKAF) JSON-LD object.

This is the generalization of the hand-authored
``docs/evidence/single-document-rulespec-projection-2026-07-28/build_projection.py``:
same target shape, same offset-verification discipline, but the semantic layer
is supplied by a model through structured output instead of by an author.

The split is the whole point, and it is not negotiable:

**The deterministic layer mints every identity.** Artifact identity and digests
come from :mod:`spicy_regs.docpipeline.source` (never re-derived here). Fragment
coordinates are the stored source field's own ``[start, end)`` code-point
offsets, and every one of them is proven by re-slicing the stored text and
comparing the SHA-256 of the slice against the digest baked into the
carrier-local URN. Canonical CFR/USC/RIN/FR-doc/regs.gov IRIs come from
:mod:`spicy_regs.ontology.citations`. Relationship assertions are re-serialized
rows of the published spicy-regs tables — nothing is re-parsed out of prose to
rediscover an edge a transform already produced.

**The model layer supplies judgments only.** It runs through the existing,
tested concept-assignment path (:class:`~spicy_regs.docpipeline.tag_task.TagExtractionTask`
driven by :func:`~spicy_regs.docpipeline.extraction.run_extraction`), which
already refuses a concept id outside the supplied candidate set, already
resolves an exact evidence quote to offsets or rejects it, and already writes
the request and the response for every provider call. This module adds no second
prompt, no second schema, and no second matcher. It consumes accepted candidate
rows, re-verifies their offsets against the stored text one more time, and turns
the survivors into ``rkaf:ConceptAssignment`` nodes. A model-supplied value that
cannot be verified against source text or against a registry row is dropped with
a recorded reason; it is never repaired.

Text-state convention (load-bearing, and the one place this projection has to
choose): an RKAF ``rkaf:Artifact`` names ONE immutable state, while a spicy-regs
artifact spans several stored source fields with independent digests. Each
profile therefore declares one *projected evidence field*. ``rkaf:hasContentDigest``
and every fragment coordinate in the emitted document are taken over that field
alone, offsets in Unicode code points, half-open ``[start, end)``, matching
rulespec Core §4.2. Evidence landing in any other field is refused rather than
silently re-based, and the count of such refusals is reported.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from spicy_regs.docpipeline.source import (
    SourceArtifact,
    build_source_artifact,
    profile_for_table,
)
from spicy_regs.ontology.attestations import (
    ATTESTOR_KIND_AI_MODEL,
    DECISION_ENDORSED_FOR_REVIEW,
    attestation_row,
)
from spicy_regs.ontology.citations import (
    canonical_cfr_iri,
    canonical_pl_iri,
    canonical_regsgov_iri,
    canonical_rin_iri,
    canonical_usc_iri,
    federal_register_identifier,
    parse_authority_citation,
    parse_cfr_citation,
)
from spicy_regs.ontology.common import RunContext, canonical_json, stable_id, text_digest
from spicy_regs.ontology.llm import resolve_exact_evidence_offsets

# --------------------------------------------------------------------------- #
# Contract configuration.
#
# Six contract findings from the hand-authored projection's G1-G6 list have
# LANDED in the rulespec repo. This module validates against branch
# ``us-regulatory-identifiers`` @ ``062fa79``, contract digest
# ``sha256:7d45dcd2…`` — the revision spicy-regs pins as of ``8d08882``. Every
# one of the six was a VALUE or a SWITCH below rather than a code path, so the
# re-pin was a constant edit and a re-run, not a rewrite. Each constant names its
# finding and the rulespec commit that answered it.
#
#   G1  921d1ff  prov:wasDerivedFrom class range stated in §2.4
#                -> already satisfied: every cited row is a typed prov:Entity.
#   G2  3644803  rkaf:publishedInDocket added
#                -> EMIT_DOCUMENT_DOCKET_EDGE below, now True.
#   G3  3c16018  rkaf:deterministicExtraction added to #AssertionOrigin
#                -> ASSERTION_ORIGIN_DETERMINISTIC below, now that value.
#   G4  e8794ba  requestContractDigest made conditional on a model extraction
#                -> REQUEST_CONTRACT_DIGEST_REQUIRED_FOR below, now narrowed.
#   G5  062fa79  §2.1 decides the direct-edge / reified-assertion pair
#                -> EMIT_PROFILE_EDGE_PROJECTIONS below, unchanged at True.
#   G6  361348c  the ten untyped timestamp context terms typed
#                -> no change here; it is a context fix, and the emitted
#                   rkaf:attestedAt / rkaf:assertedAt literals are unchanged.
#                   The vendored context copy the CLI ships carries the fix.
# --------------------------------------------------------------------------- #

#: G3 — ``#AssertionOrigin`` gained ``rkaf:deterministicExtraction`` (rulespec
#: 3c16018): a mechanically reproducible derivation, not an interpretive
#: judgment. It replaces the ``rkaf:imported`` workaround, which said only that
#: the record came from somewhere else and left the parse method on an OPTIONAL
#: ``rkaf:hasExtractionProvenance`` edge — droppable, with no gate objecting.
#:
#: The value is not free: every compiled target now REQUIRES
#: ``rkaf:hasExtractionProvenance`` alongside it, so :func:`assemble` refuses to
#: emit an assertion at this origin that cannot name its activity.
ASSERTION_ORIGIN_DETERMINISTIC = "rkaf:deterministicExtraction"

#: An unreviewed model candidate. ``#AssertionEnvelope`` requires
#: ``rkaf:hasAILineage`` alongside it, which is why every model-derived
#: assignment emits an ``rkaf:AILineage`` node.
ASSERTION_ORIGIN_MODEL = "rkaf:aiSuggested"

#: G4 — ``rkaf:requestContractDigest`` is conditional on a request-shaped
#: extraction (rulespec e8794ba, Core §2.4). The field presumes a run that sent
#: instructions, a schema and a configuration somewhere and got an answer back;
#: a deterministic table parse sends nothing. The old universal requirement left
#: one conforming move — define an envelope, hash it, cite the result — which
#: yields a real digest naming a contract the run never published. That is now
#: explicitly non-conforming, so the set is narrowed to the one method that
#: genuinely issues a contract and :func:`_activity_node` emits nothing for the
#: rest. The other four MAY still carry the digest, but only when the run really
#: issued the contract it names, which none of this module's do.
REQUEST_CONTRACT_DIGEST_REQUIRED_FOR = frozenset({"rkaf:modelExtraction"})

#: G2 — ``rkaf:publishedInDocket`` exists (rulespec 3644803, rulemaking §5.3):
#: Artifact -> Docket, the source-native FR metadata fact. Before it, a document
#: reached its docket only through a Proceeding, so a producer without a
#: proceedings model had to mint a surrogate Proceeding or drop the fact.
#: ``federal_register.docket_ids_json`` is now directly expressible as an edge on
#: the Artifact.
#:
#: §5.3 forbids minting the Docket node from the document alone — an edge to a
#: container with no ``rkaf:hasDocketIdentifier`` names nothing — so
#: :func:`_document_docket_iris` emits the edge only for a docket whose identity
#: some OTHER published row establishes. It is deliberately not reified: §5.3
#: calls this a source-native fact rather than a derivation, and Core §2.1 makes
#: a direct edge with no matching assertion legal and explicitly unbacked. It is
#: therefore not governed by :data:`EMIT_PROFILE_EDGE_PROJECTIONS`, which
#: projects assertions; turning that flag off must not drop this fact.
EMIT_DOCUMENT_DOCKET_EDGE = True
DOCUMENT_DOCKET_PREDICATE = "rkaf:publishedInDocket"

#: G5 — the graph states profile edges twice: once as the profile's plain edge on
#: the node, once as a reified ``rkaf:RelationshipAssertion``. Rulespec 062fa79
#: makes the pair normative in Core §2.1 — the direct edge is the queryable
#: projection, the assertion is the provenance-bearing source of truth, a
#: consumer seeing both counts ONE statement, and a producer SHOULD emit both for
#: an affirmed assertion and MUST NOT emit the edge for a denied, superseded, or
#: retracted one. ``True`` is therefore what §2.1 now prescribes; this projection
#: emits only affirmed assertions. Setting it to ``False`` emits the reified half
#: alone, which stays conforming and is what proves no fact lives only in a plain
#: edge.
EMIT_PROFILE_EDGE_PROJECTIONS = True

#: A model attesting its own output is not approval. ``rkaf:approved`` would be
#: a self-grant of exactly the review this record exists to say has not
#: happened. ``rkaf:advisory`` reads as "take it or leave it" and asks for
#: nothing. ``rkaf:endorsedForReview`` is the only value in the closed enum that
#: says both halves honestly: the producer stands behind the candidate AND the
#: candidate is queued for someone else's decision.
MODEL_ATTESTATION_DECISION = DECISION_ENDORSED_FOR_REVIEW

#: Unreviewed model candidates may be queued for review and nothing more.
MODEL_USAGE_ELIGIBILITY = "rkaf:reviewQueueOnly"

#: Records re-serialized from published spicy-regs tables.
DETERMINISTIC_USAGE_ELIGIBILITY = "rkaf:localOperationalUse"

PROJECTION_SCHEMA_VERSION = "rkaf-document-projection-v1"

#: The rulespec revision these constants are set for, recorded in every run
#: record so an emitted document says which contract it was built against.
CONTRACT_REVISION = "us-regulatory-identifiers@062fa79"
CONTRACT_DIGEST = "sha256:7d45dcd2f5ff6391b185fd98099740b34d3b6cac8ed66c99196e6ac368806553"

#: The carrier-local fragment URN grammar, Core §4.2. Copied from the compiled
#: pattern so a test can prove the minted form satisfies it without a validator.
FRAGMENT_URN_PATTERN = re.compile(
    r"^urn:rkaf:fragment:([A-Za-z0-9._~-]|%[0-9A-F]{2})+:(0|[1-9][0-9]*):(0|[1-9][0-9]*):sha256-[0-9a-f]{64}$"
)

#: Only a fragment whose coordinates address an immutable STORED source field
#: can carry a carrier-local URN: a third party has to be able to re-slice it.
#: Parser-derived text (a PDF adapter's output) is not in any published table.
SOURCE_EXACT_EVIDENCE_GRADE = "source-exact"

_SELECTOR_KIND = "oa:TextPositionSelector"
_COORDINATE_SYSTEM = "rkaf:unicode-codepoint"
_EVIDENCE_SCHEME = "rkaf:carrier-local-fragment"


class ProjectionError(RuntimeError):
    """The projection cannot be assembled from the inputs it was given."""


class OffsetVerificationError(ProjectionError):
    """A fragment's stored offsets do not slice the text they claim to.

    This aborts. It is never repaired and never downgraded to a rejection row:
    a fragment whose coordinates lie is not a weaker fragment, it is a false
    statement about a document, and every digest downstream of it is worthless.
    """


# --------------------------------------------------------------------------- #
# Carrier-local fragment URNs (Core §4.2).
# --------------------------------------------------------------------------- #


def encode_for_uri(value: str) -> str:
    """Percent-encode outside the RFC 3986 unreserved set, uppercase hex.

    This is SPARQL's ``ENCODE_FOR_URI``, which is the encoding Core §4.2 names
    for the artifact component and the encoding
    ``CarrierLocalFragmentUrnSourceAgreementShape`` compares against.
    """
    out: list[str] = []
    for character in value:
        if (character.isascii() and character.isalnum()) or character in "-._~":
            out.append(character)
        else:
            out.extend(f"%{byte:02X}" for byte in character.encode("utf-8"))
    return "".join(out)


def fragment_urn(artifact_iri: str, start: int, end: int, region_sha256: str) -> str:
    """Mint the carrier-local fragment URN for one verified region."""
    return f"urn:rkaf:fragment:{encode_for_uri(artifact_iri)}:{start}:{end}:sha256-{region_sha256}"


@dataclass(frozen=True)
class ProjectedFragment:
    """One region of the projected evidence field, proven by re-slicing."""

    key: str
    source_field: str
    start: int
    end: int
    text: str
    text_sha256: str
    urn: str

    @property
    def selector_iri(self) -> str:
        return f"{self.urn}#selector"


def verify_fragment(
    artifact: SourceArtifact,
    *,
    key: str,
    source_field: str,
    start: int,
    end: int,
    artifact_iri: str,
    expected_text: str | None = None,
) -> ProjectedFragment:
    """Re-slice the stored field and mint the URN, or abort.

    Every value in the returned fragment is recomputed from
    ``artifact.raw_fields[source_field]``. Nothing is trusted: not the caller's
    offsets, not a model's quote, not a gold row's stored digest.
    """
    text = artifact.raw_fields.get(source_field)
    if text is None:
        raise OffsetVerificationError(f"{key}: the artifact carries no field {source_field!r}")
    if not (0 <= start <= end <= len(text)):
        raise OffsetVerificationError(
            f"{key}: [{start},{end}) is outside {source_field} (length {len(text)} code points)"
        )
    region = text[start:end]
    if expected_text is not None and region != expected_text:
        raise OffsetVerificationError(
            f"{key}: {source_field}[{start}:{end}] is {region!r}, not the expected {expected_text!r}"
        )
    digest = text_digest(region)
    urn = fragment_urn(artifact_iri, start, end, digest)
    if not FRAGMENT_URN_PATTERN.match(urn):
        raise OffsetVerificationError(f"{key}: minted URN violates the Core §4.2 grammar: {urn}")
    return ProjectedFragment(
        key=key,
        source_field=source_field,
        start=start,
        end=end,
        text=region,
        text_sha256=digest,
        urn=urn,
    )


def ground_literal(
    artifact: SourceArtifact,
    *,
    key: str,
    source_field: str,
    artifact_iri: str,
    surface_forms: Sequence[str],
) -> ProjectedFragment | None:
    """Locate a citation's own words in the projected field, or give up.

    Grounding reuses :func:`resolve_exact_evidence_offsets`, so a surface form
    that appears zero times or more than once is not grounded. An assertion
    whose evidence cannot be pinned to one unambiguous region simply gets no
    ``rkaf:EvidenceBinding``; it keeps its extraction provenance and says
    nothing it cannot show.
    """
    text = artifact.raw_fields.get(source_field)
    if not text:
        return None
    for form in surface_forms:
        if not form:
            continue
        resolution = resolve_exact_evidence_offsets(text, form, None, None)
        if resolution is None:
            continue
        return verify_fragment(
            artifact,
            key=key,
            source_field=source_field,
            start=resolution.start,
            end=resolution.end,
            artifact_iri=artifact_iri,
            expected_text=form,
        )
    return None


# --------------------------------------------------------------------------- #
# Deterministic facts.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ExtractionActivitySpec:
    """One ``rkaf:ExtractionActivity``: which run produced which candidates."""

    key: str
    method: str
    run_id: str
    actor_id: str
    version: str
    instructions: str
    input_row: Mapping[str, Any]
    model_ref: str | None = None
    prompt_ref: str | None = None


@dataclass(frozen=True)
class DeterministicEdge:
    """One relationship a published spicy-regs table already asserts."""

    key: str
    subject: str
    predicate: str
    object: str
    table: str
    record_key: str
    activity_key: str
    asserted_at: str
    surface_forms: tuple[str, ...] = ()
    claimant_identity: str | None = None
    profile_edge: tuple[str, str, str] | None = None
    """``(node IRI, predicate, object IRI)`` — the plain profile edge this
    assertion reifies, emitted alongside it while :data:`EMIT_PROFILE_EDGE_PROJECTIONS`
    holds (finding G5)."""


@dataclass(frozen=True)
class ProfileFacts:
    """Everything a profile contributes that is not the model's business."""

    profile_id: str
    artifact_iri: str
    evidence_field: str
    artifact_identifiers: tuple[str, ...]
    artifact_schemes: tuple[str, ...]
    regulatory_identifier: str | None = None
    regulatory_scheme: str | None = None
    published_in_proceeding: tuple[str, ...] = ()
    published_in_docket: tuple[str, ...] = ()
    """Docket IRIs this document was filed under (rulemaking §5.3). Every one is
    a Docket whose identity another published row establishes — never one minted
    from the document alone."""
    extra_nodes: tuple[Mapping[str, Any], ...] = ()
    edges: tuple[DeterministicEdge, ...] = ()
    activities: tuple[ExtractionActivitySpec, ...] = ()
    claimant_identity: str | None = None
    notes: tuple[str, ...] = ()


def request_contract_digest(spec: ExtractionActivitySpec) -> tuple[str, str]:
    """Digest the request contract and the input row for one extraction activity.

    Recipe: SHA-256 over the canonical JSON of
    ``{instructions, actor_id, run_id, input_row}`` with every input value
    stringified. Returns ``(contract digest, input-row digest)``.

    Since finding G4 landed only the contract digest of a genuinely
    request-shaped run is emitted (see
    :data:`REQUEST_CONTRACT_DIGEST_REQUIRED_FOR`); the input-row digest is
    unconditional, because every activity really does have inputs.
    """
    clean = {str(key): (None if value is None else str(value)) for key, value in spec.input_row.items()}
    contract = {
        "instructions": spec.instructions,
        "actor_id": spec.actor_id,
        "run_id": spec.run_id,
        "input_row": clean,
    }
    return text_digest(canonical_json(contract)), text_digest(canonical_json(clean))


def _json_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    text = str(value).strip()
    if not text or text in {"None", "null"}:
        return []
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError):
        return [text]
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return [str(parsed)]


def _clean(value: object) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text in {"None", "nan", "null"} else text


# --------------------------------------------------------------------------- #
# Published-table access.
# --------------------------------------------------------------------------- #


class PublishedTables:
    """Read-only view over the published spicy-regs parquet tables.

    Reads go through :func:`~spicy_regs.ontology.common.read_parquet_rows`
    rather than a query engine: ``spicy_regs.docpipeline`` keeps direct DuckDB
    use confined to ``retrieval.py``, and these edge tables are small enough
    that equality filtering in Python is the simpler honest answer.
    """

    def __init__(self, directory: Path) -> None:
        self.directory = Path(directory)
        self._cache: dict[str, list[dict[str, Any]]] = {}

    def rows(self, table: str, **equals: Any) -> list[dict[str, Any]]:
        """Return the rows of ``table`` whose columns equal every keyword."""
        from spicy_regs.ontology.common import read_parquet_rows

        if table not in self._cache:
            self._cache[table] = read_parquet_rows(self.directory / f"{table}.parquet")
        return [
            row
            for row in self._cache[table]
            if all(_clean(row.get(column)) == _clean(value) for column, value in equals.items())
        ]


# --------------------------------------------------------------------------- #
# Per-profile deterministic assembly.
# --------------------------------------------------------------------------- #

_FR_AGENCY_IRI = "https://www.federalregister.gov/agencies/"

_AGENDA_STAGE_BY_RULE_STAGE = {
    "prerule stage": "rkaf:agendaPrerule",
    "proposed rule stage": "rkaf:agendaProposed",
    "final rule stage": "rkaf:agendaFinal",
    "long-term actions": "rkaf:agendaLongterm",
    "completed actions": "rkaf:agendaCompleted",
}

_AGENDA_PRIORITY_BY_CATEGORY = {
    "economically significant": "rkaf:agendaPriorityEconomicallySignificant",
    "other significant": "rkaf:agendaPriorityOtherSignificant",
    "substantive, nonsignificant": "rkaf:agendaPrioritySubstantiveNonsignificant",
    "routine and frequent": "rkaf:agendaPriorityRoutineFrequent",
    "info./admin./other": "rkaf:agendaPriorityInfoAdminOther",
}

_PROCEEDING_STAGE_BY_CURRENT = {
    "prerule": "rkaf:proceedingPrerule",
    "proposed": "rkaf:proceedingProposed",
    "supplemental": "rkaf:proceedingSupplemental",
    "final": "rkaf:proceedingFinal",
    "withdrawn": "rkaf:proceedingWithdrawn",
    "longterm": "rkaf:proceedingLongterm",
    "long-term": "rkaf:proceedingLongterm",
    "concluded": "rkaf:proceedingConcluded",
}


def _cfr_iri(row: Mapping[str, Any]) -> str | None:
    title, part = _clean(row.get("cfr_title")), _clean(row.get("cfr_part"))
    if not title or not part:
        return None
    try:
        return canonical_cfr_iri(title, part, _clean(row.get("cfr_section")) or None)
    except ValueError:
        return None


def _authority_iri(row: Mapping[str, Any]) -> str | None:
    try:
        if _clean(row.get("authority_type")) == "usc":
            return canonical_usc_iri(_clean(row.get("usc_title")), _clean(row.get("usc_section")))
        if _clean(row.get("pl_number")):
            return canonical_pl_iri(_clean(row.get("pl_number")))
    except ValueError:
        return None
    return None


#: Federal Register metadata writes a docket id behind a human label — "Docket
#: No. FSIS-2025-0012", "Doc. No. AMS-SC-24-0046", "Docket Number X". The label
#: is presentation, not identity, so it is stripped before the remainder is
#: offered to :func:`canonical_regsgov_iri`. Nothing else is rewritten.
_DOCKET_LABEL_PREFIX = re.compile(r"^\s*(?:docket|doc\.?)\s*(?:no\.?|nos\.?|number|id)?\s*", re.IGNORECASE)


def _document_docket_iris(
    row: Mapping[str, Any],
    *,
    tables: PublishedTables,
    known_docket_iris: Sequence[str],
) -> tuple[list[str], list[dict[str, Any]], list[str]]:
    """Resolve ``federal_register.docket_ids_json`` into Docket IRIs (§5.3).

    Returns ``(iris, docket nodes to add, notes)``.

    Two refusals, both because §5.3 forbids minting the Docket node from the
    document alone — an edge to a container with no ``rkaf:hasDocketIdentifier``
    names nothing. A value whose label-stripped remainder is not a syntactically
    valid regulations.gov identifier is refused, and so is one that no OTHER
    published row establishes: a docket the proceedings path already
    materialized, or a ``dockets.parquet`` row carrying that id. The document's
    own say-so is never enough to bring a Docket into existence.
    """
    iris: list[str] = []
    nodes: list[dict[str, Any]] = []
    notes: list[str] = []
    already = set(known_docket_iris)
    for raw in _json_list(row.get("docket_ids_json")):
        stated = _clean(raw)
        if not stated:
            continue
        try:
            docket_iri = canonical_regsgov_iri(_DOCKET_LABEL_PREFIX.sub("", stated, count=1))
        except ValueError:
            notes.append(f"document docket identifier {stated!r} is not expressible in rkaf:us-regsgov")
            continue
        if docket_iri in iris:
            continue
        if docket_iri in already:
            iris.append(docket_iri)
            continue
        docket_id = docket_iri.rsplit(":", 1)[-1]
        if not tables.rows("dockets", docket_id=docket_id):
            notes.append(
                f"document docket {docket_id} is stated by the document but no published dockets row "
                "carries it, so rulemaking §5.3 forbids minting the Docket node and the edge is dropped"
            )
            continue
        iris.append(docket_iri)
        nodes.append(
            {
                "@id": docket_iri,
                "@type": "rkaf:Docket",
                "rkaf:hasDocketIdentifier": docket_iri,
                "rkaf:docketIdentifierScheme": "rkaf:us-regsgov",
            }
        )
    return iris, nodes, notes


def _authority_edge(
    tables: PublishedTables,
    *,
    rin: str,
    rin_iri: str,
) -> DeterministicEdge | None:
    rows = tables.rows("authority_edges", rin=rin) if rin else []
    if not rows:
        return None
    row = rows[0]
    target = _authority_iri(row)
    if target is None:
        return None
    raw = _clean(row.get("authority_raw"))
    forms = [raw]
    usc_title, usc_section = _clean(row.get("usc_title")), _clean(row.get("usc_section"))
    if usc_title and usc_section:
        forms.extend([f"{usc_title} U.S.C. {usc_section}", f"{usc_title} USC {usc_section}"])
    return DeterministicEdge(
        key="authority",
        subject=rin_iri,
        # J3 (hand-authored): authority_edges is RIN + agenda-edition keyed with
        # parse_status often partial, so this asserts the agenda item's cited
        # authority rather than minting the stronger rkaf:hasAuthority chain.
        predicate="rkaf:agendaAuthorityCitation",
        object=target,
        table="authority_edges",
        record_key=f"{rin}:{_clean(row.get('agenda_edition'))}",
        activity_key="authority-parser",
        asserted_at=_clean(row.get("asserted_at")),
        surface_forms=tuple(dict.fromkeys(form for form in forms if form)),
    )


def _authority_activity(tables: PublishedTables, *, rin: str) -> ExtractionActivitySpec | None:
    rows = tables.rows("authority_edges", rin=rin) if rin else []
    if not rows:
        return None
    row = rows[0]
    return ExtractionActivitySpec(
        key="authority-parser",
        method="rkaf:deterministicParse",
        run_id=_clean(row.get("run_id")),
        actor_id=_clean(row.get("actor_id")),
        version="v1",
        instructions=("spicy-regs deterministic Unified Agenda authority-citation parse (authority_edges.parquet row)"),
        input_row=row,
    )


def _federal_register_facts(
    artifact: SourceArtifact,
    row: Mapping[str, Any],
    *,
    tables: PublishedTables,
    partner: str,
) -> ProfileFacts:
    document_number = _clean(row.get("document_number"))
    artifact_iri = f"https://www.federalregister.gov/d/{document_number}"
    scheme, regulatory_iri = federal_register_identifier(document_number)
    notes: list[str] = []
    extra_nodes: list[dict[str, Any]] = []
    edges: list[DeterministicEdge] = []
    activities: list[ExtractionActivitySpec] = []

    agency_slugs = _json_list(row.get("agency_slugs"))
    claimant = f"{_FR_AGENCY_IRI}{agency_slugs[0]}" if agency_slugs else None

    # The proceedings table is the join: it is the only published row that names
    # this FR document, and everything else (the RIN, the dockets, the CFR
    # targets) hangs off it. Nothing is re-parsed out of the document to find it.
    proceeding_row: Mapping[str, Any] | None = None
    proceeding_iri: str | None = None
    rin = ""
    for proceeding in tables.rows("proceedings"):
        if document_number in _json_list(proceeding.get("fr_document_numbers_json")):
            proceeding_row = proceeding
            proceeding_iri = f"{partner}:proceeding:{_clean(proceeding.get('proceeding_id'))}"
            rin = _clean(proceeding.get("rin"))
            break
    rule_target_rows = tables.rows("rule_targets", rin=rin) if rin else []

    docket_iris: list[str] = []
    if proceeding_row is not None and proceeding_iri is not None:
        stage = _PROCEEDING_STAGE_BY_CURRENT.get(_clean(proceeding_row.get("current_stage")).lower())
        cfr_targets = [
            value
            for value in _json_list(proceeding_row.get("cfr_target_iris_json"))
            if value.startswith("urn:rkaf:us:cfr:")
        ]
        for docket_id in _json_list(proceeding_row.get("docket_ids_json")):
            try:
                docket_iris.append(canonical_regsgov_iri(docket_id))
            except ValueError:
                notes.append(f"docket identifier {docket_id!r} is not expressible in rkaf:us-regsgov")
        proceeding_node: dict[str, Any] = {
            "@id": proceeding_iri,
            "@type": "rkaf:Proceeding",
            "rkaf:hasProceedingIdentifier": proceeding_iri,
            "rkaf:proceedingIdentifierScheme": "rkaf:partner-defined",
        }
        if stage:
            proceeding_node["rkaf:proceedingStage"] = stage
        if EMIT_PROFILE_EDGE_PROJECTIONS and docket_iris:
            proceeding_node["rkaf:hasDocket"] = list(docket_iris)
        if EMIT_PROFILE_EDGE_PROJECTIONS and cfr_targets:
            proceeding_node["rkaf:proceedingAffectsCitation"] = list(cfr_targets)
        extra_nodes.append(proceeding_node)
        for docket_iri in docket_iris:
            extra_nodes.append(
                {
                    "@id": docket_iri,
                    "@type": "rkaf:Docket",
                    "rkaf:hasDocketIdentifier": docket_iri,
                    "rkaf:docketIdentifierScheme": "rkaf:us-regsgov",
                }
            )
        activities.append(
            ExtractionActivitySpec(
                key="proceedings",
                method="rkaf:deterministicParse",
                run_id=_clean(proceeding_row.get("run_id")),
                actor_id=_clean(proceeding_row.get("actor_id")) or "spicy-regs:proceedings:v1",
                version="v1",
                instructions="spicy-regs deterministic proceeding assembly (proceedings.parquet row)",
                input_row=proceeding_row,
            )
        )
        # The document -> proceeding link is reified too, so turning the profile
        # edges off (finding G5) never drops the fact — it only moves where it
        # is stated. Without this the document would be orphaned the moment the
        # plain edges become derived projections.
        edges.append(
            DeterministicEdge(
                key="published-in-proceeding",
                subject=artifact_iri,
                predicate="rkaf:publishedInProceeding",
                object=proceeding_iri,
                table="proceedings",
                record_key=_clean(proceeding_row.get("proceeding_id")),
                activity_key="proceedings",
                asserted_at=_clean(proceeding_row.get("asserted_at")),
                claimant_identity=claimant,
                profile_edge=(artifact_iri, "rkaf:publishedInProceeding", proceeding_iri),
            )
        )
        for docket_iri, raw_docket in zip(docket_iris, _json_list(proceeding_row.get("docket_ids_json"))):
            edges.append(
                DeterministicEdge(
                    key=f"docket-{raw_docket}",
                    subject=proceeding_iri,
                    predicate="rkaf:hasDocket",
                    object=docket_iri,
                    table="proceedings",
                    record_key=_clean(proceeding_row.get("proceeding_id")),
                    activity_key="proceedings",
                    asserted_at=_clean(proceeding_row.get("asserted_at")),
                    surface_forms=(f"Docket No. {raw_docket}", raw_docket),
                    claimant_identity=claimant,
                    profile_edge=(proceeding_iri, "rkaf:hasDocket", docket_iri),
                )
            )

    if rin:
        rin_iri = canonical_rin_iri(rin)
        extra_nodes.append(
            {
                "@id": rin_iri,
                "@type": "rkaf:RegulatoryAgendaItem",
                "rkaf:hasAgendaItemIdentifier": rin_iri,
                "rkaf:agendaItemIdentifierScheme": "rkaf:us-rin",
            }
        )
        authority = _authority_edge(tables, rin=rin, rin_iri=rin_iri)
        if authority is not None:
            edges.append(replace(authority, claimant_identity=claimant))
            activity = _authority_activity(tables, rin=rin)
            if activity is not None:
                activities.append(activity)

    for candidate in rule_target_rows:
        target = _cfr_iri(candidate)
        if target is None or proceeding_iri is None:
            continue
        title, part = _clean(candidate.get("cfr_title")), _clean(candidate.get("cfr_part"))
        edges.append(
            DeterministicEdge(
                key=f"cfr-target-{title}-{part}",
                subject=proceeding_iri,
                predicate="rkaf:proceedingAffectsCitation",
                object=target,
                table="rule_targets",
                record_key=f"{_clean(candidate.get('docket_id'))}:{_clean(candidate.get('cfr_ref'))}",
                activity_key="rule-targets",
                asserted_at=_clean(candidate.get("asserted_at")),
                surface_forms=(
                    f"{title} CFR Part {part}",
                    f"{title} CFR part {part}",
                    f"{title} CFR {part}",
                ),
                claimant_identity=claimant,
                profile_edge=(proceeding_iri, "rkaf:proceedingAffectsCitation", target),
            )
        )
        activities.append(
            ExtractionActivitySpec(
                key="rule-targets",
                method="rkaf:deterministicParse",
                run_id=_clean(candidate.get("run_id")),
                actor_id=_clean(candidate.get("actor_id")),
                version="v1",
                instructions=(
                    "spicy-regs deterministic rule-targets extraction over docket documents "
                    "and FR metadata (rule_targets.parquet row)"
                ),
                input_row=candidate,
            )
        )
        break

    # G2 / rulemaking §5.3: the document's own docket membership. This is the
    # document -> docket fact the FR record states outright, NOT a restatement of
    # the proceeding's rkaf:hasDocket: a proceeding may span dockets a given one
    # of its documents was not filed in, and neither edge implies the other.
    published_in_docket: tuple[str, ...] = ()
    if EMIT_DOCUMENT_DOCKET_EDGE:
        docket_edge_iris, docket_edge_nodes, docket_notes = _document_docket_iris(
            row, tables=tables, known_docket_iris=docket_iris
        )
        published_in_docket = tuple(docket_edge_iris)
        extra_nodes.extend(docket_edge_nodes)
        notes.extend(docket_notes)
    elif _json_list(row.get("docket_ids_json")):
        notes.append(
            "finding G2: the document's own docket_ids_json is not directly expressible — "
            "there is no document->docket predicate, so the docket is reached through the Proceeding"
        )

    return ProfileFacts(
        profile_id=artifact.profile_id,
        artifact_iri=artifact_iri,
        evidence_field="federal_register.body_html",
        artifact_identifiers=(artifact_iri,),
        artifact_schemes=("rkaf:urn-persistent",),
        regulatory_identifier=regulatory_iri,
        regulatory_scheme=scheme,
        published_in_proceeding=(proceeding_iri,) if proceeding_iri else (),
        published_in_docket=published_in_docket,
        extra_nodes=tuple(extra_nodes),
        edges=tuple(edges),
        activities=tuple({spec.key: spec for spec in activities}.values()),
        claimant_identity=claimant,
        notes=tuple(notes),
    )


def _unified_agenda_facts(
    artifact: SourceArtifact,
    row: Mapping[str, Any],
    *,
    tables: PublishedTables,
    partner: str,
) -> ProfileFacts:
    rin = _clean(row.get("rin"))
    edition = _clean(row.get("agenda_edition"))
    rin_iri = canonical_rin_iri(rin)
    artifact_iri = _clean(row.get("url")) or f"{partner}:agenda-observation:{rin}:{edition}"
    notes: list[str] = []
    edges: list[DeterministicEdge] = []
    activities: list[ExtractionActivitySpec] = []

    affects = []
    for reference in _json_list(row.get("cfr_references_json")):
        for citation in parse_cfr_citation(reference):
            try:
                affects.append(canonical_cfr_iri(citation.title, citation.part, citation.section))
            except ValueError:
                notes.append(f"CFR reference {reference!r} is not expressible in rkaf:us-cfr")
    authority = []
    for reference in _json_list(row.get("legal_authority_json")):
        for citation in parse_authority_citation(reference):
            try:
                if citation.authority_type == "usc":
                    authority.append(canonical_usc_iri(citation.usc_title, citation.usc_section))
                elif citation.pl_number:
                    authority.append(canonical_pl_iri(citation.pl_number))
            except ValueError:
                notes.append(f"authority reference {reference!r} is not expressible")

    observation: dict[str, Any] = {
        "@id": artifact_iri,
        "@type": "rkaf:RegulatoryAgendaObservation",
        "rkaf:hasArtifactIdentifier": [artifact_iri],
        "rkaf:artifactIdentifierScheme": ["rkaf:urn-persistent"],
        "foaf:primaryTopic": rin_iri,
    }
    stage = _AGENDA_STAGE_BY_RULE_STAGE.get(_clean(row.get("rule_stage")).lower())
    if stage:
        observation["rkaf:agendaStage"] = stage
    priority = _AGENDA_PRIORITY_BY_CATEGORY.get(_clean(row.get("priority_category")).lower())
    if priority:
        observation["rkaf:agendaPriority"] = priority
    if EMIT_PROFILE_EDGE_PROJECTIONS and affects:
        observation["rkaf:agendaAffectsCitation"] = list(dict.fromkeys(affects))
    if EMIT_PROFILE_EDGE_PROJECTIONS and authority:
        observation["rkaf:agendaAuthorityCitation"] = list(dict.fromkeys(authority))

    extra_nodes: list[dict[str, Any]] = [
        observation,
        {
            "@id": rin_iri,
            "@type": "rkaf:RegulatoryAgendaItem",
            "rkaf:hasAgendaItemIdentifier": rin_iri,
            "rkaf:agendaItemIdentifierScheme": "rkaf:us-rin",
        },
    ]
    # An rkaf:SourceFragment's oa:hasSource is class-ranged to rkaf:Artifact
    # (compiled/shacl/core/source-fragment.ttl). rkaf:RegulatoryAgendaObservation
    # is described in the profile as a subclass of rkaf:Artifact but no shape or
    # context file declares `rdfs:subClassOf`, so RDFS inference cannot reach it.
    # The observation therefore carries BOTH types, as two nodes on one IRI, so
    # each dispatches to a real compiled schema at L2 instead of an @type array
    # that L2 skips silently. See the report's finding list.
    notes.append(
        "finding: rkaf:RegulatoryAgendaObservation is documented as a profile subclass of "
        "rkaf:Artifact but no shapes file declares rdfs:subClassOf, so the observation must "
        "also be typed rkaf:Artifact for its own fragments to satisfy the oa:hasSource range"
    )

    edge = _authority_edge(tables, rin=rin, rin_iri=rin_iri)
    if edge is not None:
        edges.append(edge)
        activity = _authority_activity(tables, rin=rin)
        if activity is not None:
            activities.append(activity)

    return ProfileFacts(
        profile_id=artifact.profile_id,
        artifact_iri=artifact_iri,
        evidence_field="unified_agenda.abstract",
        artifact_identifiers=(artifact_iri,),
        artifact_schemes=("rkaf:urn-persistent",),
        extra_nodes=tuple(extra_nodes),
        edges=tuple(edges),
        activities=tuple(activities),
        notes=tuple(notes),
    )


def _congress_bill_facts(
    artifact: SourceArtifact,
    row: Mapping[str, Any],
    *,
    tables: PublishedTables,
    partner: str,
) -> ProfileFacts:
    bill_id = _clean(row.get("bill_id"))
    artifact_iri = _clean(row.get("url")) or f"urn:spicy-regs:congress-bill:{bill_id}"
    # #USRegulatoryIdentifierScheme covers cfr / usc / frdoc / regsgov / pl / eo.
    # A bill that has not been enacted is none of those: it has no public-law
    # number yet, and there is no us-bill scheme. Recorded as a finding rather
    # than forced into rkaf:partner-defined regulatory identity, which would
    # claim a regulatory citation the document does not have.
    return ProfileFacts(
        profile_id=artifact.profile_id,
        artifact_iri=artifact_iri,
        evidence_field="congress_bills.xml_text",
        artifact_identifiers=(artifact_iri,),
        artifact_schemes=("rkaf:urn-persistent",),
        notes=(
            "finding: #USRegulatoryIdentifierScheme has no value for a congressional bill "
            "(cfr/usc/frdoc/regsgov/pl/eo only), so the artifact carries no "
            "rkaf:hasRegulatoryIdentifier; the bill id survives only as the artifact identifier",
        ),
    )


_ProfileBuilder = Callable[..., ProfileFacts]

PROFILE_BUILDERS: dict[str, _ProfileBuilder] = {
    "federal-register-document-v1": _federal_register_facts,
    "unified-agenda-observation-v1": _unified_agenda_facts,
    "congress-bill-v1": _congress_bill_facts,
}


# --------------------------------------------------------------------------- #
# Model judgments.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ConceptJudgment:
    """One accepted concept assignment, with its verified evidence region."""

    concept_id: str
    concept_label: str
    definition: str
    facet: str
    role: str
    confidence: float
    fragment: ProjectedFragment
    candidate_id: str
    evidence_text: str
    alignment_method: str


@dataclass(frozen=True)
class ModelLayer:
    """What the model was asked, what it said, and what survived checking."""

    model_id: str
    instructions_sha256: str
    schema_sha256: str
    input_context_sha256: str
    run_directory: str
    receipt_sha256: str
    selector_version: str
    registry_sha256: str
    candidate_concept_count: int
    judgments: tuple[ConceptJudgment, ...]
    rejections: tuple[Mapping[str, Any], ...]
    call_count: int
    segment_count: int = 0
    segments_projected: int = 0
    temperature: float = 0.0


ASSIGNMENT_ROLE_IRIS: dict[str, str] = {
    "primary": "rkaf:assignmentPrimary",
    "substantive": "rkaf:assignmentSubstantive",
    "mention": "rkaf:assignmentMention",
    "contextual": "rkaf:assignmentContextual",
}


def verify_candidate_rows(
    artifact: SourceArtifact,
    rows: Sequence[Mapping[str, Any]],
    *,
    artifact_iri: str,
    evidence_field: str,
) -> tuple[list[ConceptJudgment], list[dict[str, Any]]]:
    """Re-verify accepted tag candidates and turn survivors into judgments.

    The tag task already grounded these quotes once. This pass exists because
    the projection makes a stronger claim than the tag table does: it mints a
    carrier-local URN whose digest a stranger will recompute. So the offsets are
    re-sliced against the stored field here, and anything that fails becomes a
    rejection row rather than a fragment.
    """
    judgments: list[ConceptJudgment] = []
    rejections: list[dict[str, Any]] = []
    for row in rows:
        concept_id = _clean(row.get("concept_id"))
        base = {
            "candidate_id": _clean(row.get("candidate_id")),
            "concept_id": concept_id or None,
            "role": _clean(row.get("role")),
            "source_field": _clean(row.get("source_field")),
            "evidence_text": row.get("evidence_text"),
        }
        if not concept_id:
            # The tag task admits a novel concept the model proposes. A novel
            # concept has no registry row, and this projection never mints
            # identity, so it is refused here rather than registered.
            rejections.append({**base, "reason": "model_proposed_concept_not_in_registry"})
            continue
        if _clean(row.get("source_field")) != evidence_field:
            rejections.append({**base, "reason": "evidence_outside_projected_text_state"})
            continue
        if _clean(row.get("evidence_grade")) != SOURCE_EXACT_EVIDENCE_GRADE:
            rejections.append({**base, "reason": "evidence_not_source_exact"})
            continue
        role = ASSIGNMENT_ROLE_IRIS.get(_clean(row.get("role")))
        if role is None:
            rejections.append({**base, "reason": "unknown_assignment_role"})
            continue
        start, end = int(row.get("source_start_char") or 0), int(row.get("source_end_char") or 0)
        try:
            fragment = verify_fragment(
                artifact,
                key=f"assignment-{_clean(row.get('candidate_id'))}",
                source_field=evidence_field,
                start=start,
                end=end,
                artifact_iri=artifact_iri,
                expected_text=str(row.get("evidence_text") or ""),
            )
        except OffsetVerificationError as error:
            rejections.append({**base, "reason": "offset_verification_failed", "detail": str(error)})
            continue
        judgments.append(
            ConceptJudgment(
                concept_id=concept_id,
                concept_label=_clean(row.get("concept_label")),
                definition=_clean(row.get("definition")),
                facet=_clean(row.get("facet")) or _clean(row.get("scheme")),
                role=role,
                confidence=float(row.get("confidence") or 0.0),
                fragment=fragment,
                candidate_id=_clean(row.get("candidate_id")),
                evidence_text=str(row.get("evidence_text") or ""),
                alignment_method=_clean(row.get("evidence_alignment_method")),
            )
        )
    return judgments, rejections


# --------------------------------------------------------------------------- #
# Assembly.
# --------------------------------------------------------------------------- #


@dataclass
class ProjectionResult:
    """The emitted JSON-LD document plus the record of how it was produced."""

    document: dict[str, Any]
    run_record: dict[str, Any]
    transcript: list[str]

    @property
    def node_count(self) -> int:
        return len(self.document.get("@graph", []))


@dataclass(frozen=True)
class ProjectionSettings:
    """Everything the assembler needs that is not the document itself."""

    corpus_dir: Path
    tables_dir: Path
    partner: str = "urn:rkaf:partner:spicy-regs"
    scope: str = "document-rkaf-projection"
    context_ref: str = "./rkaf-context.jsonld"
    asserted_at: str | None = None
    attestor_id: str = ""
    registry_path: Path | None = None
    prompt_concept_limit: int = 12
    max_segments: int = 0
    """Cap on the segments sent to the model; ``0`` means every segment. A cap
    bounds provider spend on a long document, and it changes what the emitted
    document can claim, so both the cap and the segment count are recorded."""
    extra_notes: tuple[str, ...] = ()


def load_artifact(
    profile_id: str,
    subject_id: str,
    *,
    corpus_dir: Path,
) -> tuple[SourceArtifact, dict[str, Any]]:
    """Resolve one corpus row into a :class:`SourceArtifact` and its raw row.

    Records come from :func:`~spicy_regs.docpipeline.source.iter_source_records`,
    so the profile's own reader is used — including the ``documents`` profile's
    join against ``federal_register`` — and identity, digests, regions and
    offsets all come from
    :func:`~spicy_regs.docpipeline.source.build_source_artifact`. This function
    only finds the record and hands it over.
    """
    from spicy_regs.docpipeline.source import iter_source_records

    table = _source_table_for_profile(profile_id)
    profile = profile_for_table(table)
    if not (Path(corpus_dir) / f"{table}.parquet").is_file():
        raise ProjectionError(f"corpus {corpus_dir} has no {table}.parquet")
    # A profile may key on more than one column (unified_agenda is rin +
    # agenda_edition, and its subject_id is the canonical JSON of both). Accept
    # either that JSON form or a bare first-column value, and refuse an
    # ambiguous match rather than silently taking a row.
    try:
        parsed = json.loads(subject_id)
    except (TypeError, ValueError):
        parsed = None
    if isinstance(parsed, Mapping):
        selectors = {str(key): str(value) for key, value in parsed.items()}
    else:
        selectors = {profile.id_columns[0]: subject_id}

    matches = [
        record
        for record in iter_source_records(Path(corpus_dir), active_source_tables={table})
        if all(_clean(record.row.get(column)) == _clean(value) for column, value in selectors.items())
    ]
    if not matches:
        raise ProjectionError(f"{table} has no row for {selectors} in {corpus_dir}")
    if len(matches) > 1:
        raise ProjectionError(
            f"{table} has {len(matches)} rows for {selectors}; name every id column "
            f"({list(profile.id_columns)}) as a JSON subject id"
        )
    record = matches[0]
    outcome = build_source_artifact(record)
    if outcome.artifact is None:
        raise ProjectionError(f"{profile_id}/{subject_id} did not build a source artifact: {outcome.reason}")
    return outcome.artifact, dict(record.row)


def _source_table_for_profile(profile_id: str) -> str:
    from spicy_regs.docpipeline.source import SOURCE_PROFILES

    for profile in SOURCE_PROFILES:
        if profile.profile_id == profile_id:
            return profile.source_table
    raise ProjectionError(f"unknown profile {profile_id!r}")


def build_profile_facts(
    artifact: SourceArtifact,
    row: Mapping[str, Any],
    *,
    tables: PublishedTables,
    partner: str,
) -> ProfileFacts:
    builder = PROFILE_BUILDERS.get(artifact.profile_id)
    if builder is None:
        raise ProjectionError(
            f"no RKAF profile projection for {artifact.profile_id!r}; known profiles: {sorted(PROFILE_BUILDERS)}"
        )
    return builder(artifact, row, tables=tables, partner=partner)


def _selector_node(fragment: ProjectedFragment) -> dict[str, Any]:
    return {
        "@id": fragment.selector_iri,
        "@type": _SELECTOR_KIND,
        "oa:start": fragment.start,
        "oa:end": fragment.end,
        "rkaf:coordinateSystem": _COORDINATE_SYSTEM,
    }


def _fragment_node(fragment: ProjectedFragment, *, artifact_iri: str, artifact_digest: str) -> dict[str, Any]:
    return {
        "@id": fragment.urn,
        "@type": "rkaf:SourceFragment",
        "oa:hasSource": artifact_iri,
        "oa:hasSelector": fragment.selector_iri,
        "rkaf:selectorKind": [_SELECTOR_KIND],
        "rkaf:fragmentContentDigest": f"sha256:{fragment.text_sha256}",
        "rkaf:sourceArtifactDigest": f"sha256:{artifact_digest}",
    }


def _activity_node(spec: ExtractionActivitySpec, *, partner: str) -> dict[str, Any]:
    contract, input_digest = request_contract_digest(spec)
    node: dict[str, Any] = {
        "@id": f"{partner}:activity:{spec.key}",
        "@type": "rkaf:ExtractionActivity",
        "rkaf:extractionMethod": spec.method,
        "rkaf:extractionRun": f"{partner}:run:{spec.run_id}" if spec.run_id else f"{partner}:run:unknown",
        "rkaf:extractedBy": f"{partner}:actor:{spec.key}",
        "rkaf:extractorVersion": spec.version,
        "rkaf:inputDigest": [f"sha256:{input_digest}"],
    }
    if spec.method in REQUEST_CONTRACT_DIGEST_REQUIRED_FOR:
        node["rkaf:requestContractDigest"] = f"sha256:{contract}"
    if spec.model_ref:
        node["rkaf:extractionModelRef"] = spec.model_ref
    if spec.prompt_ref:
        node["rkaf:extractionPromptRef"] = spec.prompt_ref
    return node


def assemble(
    artifact: SourceArtifact,
    facts: ProfileFacts,
    *,
    settings: ProjectionSettings,
    model_layer: ModelLayer | None = None,
) -> ProjectionResult:
    """Turn verified facts and verified judgments into the RKAF document."""
    partner = settings.partner
    context = RunContext.resolve(asserted_at=settings.asserted_at, prefix="rkaf-projection")
    artifact_iri = facts.artifact_iri
    evidence_field = facts.evidence_field
    if evidence_field not in artifact.raw_fields:
        raise ProjectionError(
            f"{facts.profile_id}: the projected evidence field {evidence_field!r} is absent from this artifact "
            f"(available: {sorted(artifact.raw_fields)})"
        )
    artifact_digest = artifact.field_sha256[evidence_field]
    transcript: list[str] = [
        "== Source text state ==",
        f"profile          : {facts.profile_id}",
        f"subject_id       : {artifact.subject_id}",
        f"artifact_id      : {artifact.artifact_id}",
        f"version digest   : {artifact.content_sha256}  (source.py _content_digest; NOT hasContentDigest)",
        f"projected field  : {evidence_field}",
        f"length           : {len(artifact.raw_fields[evidence_field])} Unicode code points",
        f"sha256(UTF-8)    : {artifact_digest}",
        "",
        "== SourceFragment offset verification "
        "(unicode code points, half-open [start,end), re-sliced from the stored field) ==",
    ]

    graph: list[dict[str, Any]] = []
    fragments: dict[str, ProjectedFragment] = {}
    provenance_records: set[str] = set()

    def note_fragment(fragment: ProjectedFragment) -> None:
        if fragment.urn in fragments:
            return
        fragments[fragment.urn] = fragment
        transcript.extend(
            [
                f"  {fragment.key} [{fragment.start},{fragment.end})",
                f"     slice: {json.dumps(fragment.text[:160], ensure_ascii=False)}",
                f"     sha256(region): {fragment.text_sha256}",
                f"     urn: {fragment.urn}",
            ]
        )

    # ------------------------------------------------------------- Artifact
    artifact_node: dict[str, Any] = {
        "@id": artifact_iri,
        "@type": "rkaf:Artifact",
        "rkaf:hasArtifactIdentifier": list(facts.artifact_identifiers),
        "rkaf:artifactIdentifierScheme": list(facts.artifact_schemes),
        "rkaf:hasContentDigest": f"sha256:{artifact_digest}",
    }
    if facts.regulatory_identifier and facts.regulatory_scheme:
        artifact_node["rkaf:hasRegulatoryIdentifier"] = facts.regulatory_identifier
        artifact_node["rkaf:regulatoryIdentifierScheme"] = facts.regulatory_scheme
    if EMIT_PROFILE_EDGE_PROJECTIONS and facts.published_in_proceeding:
        artifact_node["rkaf:publishedInProceeding"] = list(facts.published_in_proceeding)
    # Not gated on EMIT_PROFILE_EDGE_PROJECTIONS: this edge projects no
    # assertion, it is the source-native fact itself (rulemaking §5.3), so
    # turning the assertion projections off must not delete it.
    if EMIT_DOCUMENT_DOCKET_EDGE and facts.published_in_docket:
        artifact_node[DOCUMENT_DOCKET_PREDICATE] = list(facts.published_in_docket)
    graph.append(artifact_node)
    graph.extend(dict(node) for node in facts.extra_nodes)

    # ------------------------------------------ deterministic relationships
    activities: dict[str, ExtractionActivitySpec] = {spec.key: spec for spec in facts.activities}
    edge_records: list[dict[str, Any]] = []
    for edge in facts.edges:
        assertion_iri = (
            f"{partner}:assertion:{stable_id('assertion', edge.subject, edge.predicate, edge.object, length=16)}"
        )
        record_iri = f"{partner}:record:{edge.table}:{edge.record_key}"
        provenance_records.add(record_iri)
        assertion: dict[str, Any] = {
            "@id": assertion_iri,
            "@type": "rkaf:RelationshipAssertion",
            "rkaf:assertsSubject": edge.subject,
            "rkaf:assertsPredicate": edge.predicate,
            "rkaf:assertsObject": edge.object,
            "rkaf:assertionPolarity": "rkaf:affirmed",
            "rkaf:assertionOrigin": ASSERTION_ORIGIN_DETERMINISTIC,
            "rkaf:assertedAt": edge.asserted_at or context.asserted_at,
            "rkaf:usageEligibility": DETERMINISTIC_USAGE_ELIGIBILITY,
            "prov:wasDerivedFrom": [record_iri],
        }
        # Since G3 landed, rkaf:deterministicExtraction REQUIRES
        # rkaf:hasExtractionProvenance on every compiled target: a claim of
        # mechanical reproducibility that names no run is not checkable. An edge
        # whose activity is missing is a projection bug, not a weaker assertion,
        # so it aborts rather than emitting a non-conforming node.
        if edge.activity_key not in activities:
            raise ProjectionError(
                f"edge {edge.key!r} asserts {ASSERTION_ORIGIN_DETERMINISTIC} but names no extraction "
                f"activity {edge.activity_key!r}; the contract requires rkaf:hasExtractionProvenance "
                f"for that origin (known activities: {sorted(activities)})"
            )
        assertion["rkaf:hasExtractionProvenance"] = f"{partner}:activity:{edge.activity_key}"
        grounded = ground_literal(
            artifact,
            key=f"edge-{edge.key}",
            source_field=evidence_field,
            artifact_iri=artifact_iri,
            surface_forms=edge.surface_forms,
        )
        record = {
            "key": edge.key,
            "subject": edge.subject,
            "predicate": edge.predicate,
            "object": edge.object,
            "table": edge.table,
            "assertion": assertion_iri,
            "grounded": grounded is not None,
        }
        if grounded is not None:
            note_fragment(grounded)
            record["evidence"] = grounded.urn
            graph.append(
                {
                    "@id": f"{partner}:binding:{edge.key}",
                    "@type": "rkaf:EvidenceBinding",
                    "rkaf:bindsAssertion": assertion_iri,
                    "rkaf:bindsSourceFragment": [grounded.urn],
                }
            )
            if edge.claimant_identity:
                claimant_iri = f"{partner}:claimant:{edge.key}"
                assertion["rkaf:hasSourceClaimant"] = claimant_iri
                graph.append(
                    {
                        "@id": claimant_iri,
                        "@type": "rkaf:SourceClaimant",
                        "rkaf:claimsAssertion": assertion_iri,
                        "rkaf:claimantAttribution": "rkaf:claimantIsDocumentIssuer",
                        "rkaf:claimantIdentity": edge.claimant_identity,
                        "rkaf:attributedInFragment": [grounded.urn],
                    }
                )
        else:
            record["reason"] = "no unique verbatim restatement of this citation in the projected field"
        edge_records.append(record)
        graph.append(assertion)

    # -------------------------------------------------- concept assignments
    judgment_records: list[dict[str, Any]] = []
    assignment_iris: list[str] = []
    if model_layer is not None and model_layer.judgments:
        scheme_iri = f"{partner}:scheme:{model_layer.judgments[0].facet}"
        workspace_iri = f"{partner}:workspace:main"
        lineage_iri = f"{partner}:lineage:{settings.scope}"
        graph.append(
            {
                "@id": scheme_iri,
                "@type": "rkaf:ConceptScheme",
                "skos:prefLabel": f"spicy-regs fused registry — {model_layer.judgments[0].facet} facet",
                "rkaf:schemeFacet": f"{partner}:facet:{model_layer.judgments[0].facet}",
                "rkaf:conceptStatus": "rkaf:active",
                "rkaf:definedInScope": workspace_iri,
            }
        )
        graph.append(
            {
                "@id": lineage_iri,
                "@type": "rkaf:AILineage",
                "rkaf:modelId": model_layer.model_id,
                "rkaf:modelVersion": model_layer.model_id,
                "rkaf:promptTemplateRef": f"{partner}:prompt:concept-tags-v1:{model_layer.instructions_sha256[:16]}",
                # The pinned arms are reasoning-effort models with no sampling
                # temperature to report; #AILineage requires the field anyway.
                "rkaf:temperature": model_layer.temperature,
                "rkaf:inputContextHash": f"sha256:{model_layer.input_context_sha256}",
            }
        )
        graph.append(
            _activity_node(
                ExtractionActivitySpec(
                    key="concept-tags",
                    method="rkaf:modelExtraction",
                    run_id=model_layer.run_directory,
                    actor_id=f"{partner}:actor:concept-tags",
                    version="concept_tags_v1",
                    instructions=f"docpipeline concept_tags_v1 @ sha256:{model_layer.instructions_sha256}",
                    input_row={
                        "instructions_sha256": model_layer.instructions_sha256,
                        "schema_sha256": model_layer.schema_sha256,
                        "input_context_sha256": model_layer.input_context_sha256,
                        "selector_version": model_layer.selector_version,
                        "registry_sha256": model_layer.registry_sha256,
                    },
                    model_ref=f"{partner}:model:{model_layer.model_id}",
                    prompt_ref=f"{partner}:prompt:concept-tags-v1:{model_layer.instructions_sha256[:16]}",
                ),
                partner=partner,
            )
        )
        seen_concepts: set[str] = set()
        for judgment in model_layer.judgments:
            concept_iri = f"{partner}:concept:{judgment.concept_id}"
            if concept_iri not in seen_concepts:
                seen_concepts.add(concept_iri)
                concept_node: dict[str, Any] = {
                    "@id": concept_iri,
                    "@type": "rkaf:LocalConcept",
                    "skos:prefLabel": judgment.concept_label,
                    "skos:inScheme": scheme_iri,
                    "rkaf:definedInScope": workspace_iri,
                    "rkaf:conceptScope": f"{partner}:scope:{settings.scope}",
                    "rkaf:conceptStatus": "rkaf:active",
                }
                if judgment.definition:
                    concept_node["skos:definition"] = judgment.definition
                graph.append(concept_node)
            note_fragment(judgment.fragment)
            assignment_iri = f"{partner}:assignment:{judgment.candidate_id}"
            assignment_iris.append(assignment_iri)
            record_iri = f"{partner}:record:concept_registry:{judgment.concept_id}"
            provenance_records.add(record_iri)
            graph.append(
                {
                    "@id": assignment_iri,
                    "@type": "rkaf:ConceptAssignment",
                    "rkaf:assignmentSubject": artifact_iri,
                    "rkaf:assignmentSubjectType": "rkaf:Artifact",
                    "rkaf:assignedConcept": concept_iri,
                    "skos:inScheme": scheme_iri,
                    "rkaf:assignmentRole": judgment.role,
                    "rkaf:assignmentDerivation": "rkaf:directAssignment",
                    "rkaf:assignmentEvidence": [judgment.fragment.urn],
                    "rkaf:assignmentEvidenceScheme": _EVIDENCE_SCHEME,
                    "rkaf:assertionOrigin": ASSERTION_ORIGIN_MODEL,
                    "rkaf:hasAILineage": lineage_iri,
                    "rkaf:hasExtractionProvenance": f"{partner}:activity:concept-tags",
                    "rkaf:assertedAt": context.asserted_at,
                    "rkaf:usageEligibility": MODEL_USAGE_ELIGIBILITY,
                    "prov:wasDerivedFrom": [record_iri],
                }
            )
            judgment_records.append(
                {
                    "candidate_id": judgment.candidate_id,
                    "assignment": assignment_iri,
                    "concept_id": judgment.concept_id,
                    "concept_label": judgment.concept_label,
                    "role": judgment.role,
                    "evidence_urn": judgment.fragment.urn,
                    "evidence_text": judgment.evidence_text,
                    "alignment_method": judgment.alignment_method,
                    "verified": True,
                }
            )

    # ------------------------------------------------- selectors + fragments
    for fragment in fragments.values():
        graph.append(_selector_node(fragment))
        graph.append(_fragment_node(fragment, artifact_iri=artifact_iri, artifact_digest=artifact_digest))

    # ---------------------------------------------------------- activities
    for spec in activities.values():
        graph.append(_activity_node(spec, partner=partner))

    # ---------------------------------------------------- provenance records
    # L3 enforces `sh:class prov:Entity` on every prov:wasDerivedFrom value
    # (compiled/shacl/core/{assertion,concept-assignment,relationship-assertion}.ttl),
    # so each cited table row is materialized as a typed node. Finding G1.
    for record_iri in sorted(provenance_records):
        graph.append({"@id": record_iri, "@type": "prov:Entity"})

    # --------------------------------------------------------- attestation
    attestation_record: dict[str, Any] | None = None
    if model_layer is not None and assignment_iris:
        scope_iri = f"{partner}:scope:{settings.scope}"
        attestor = settings.attestor_id or f"{partner}:model:{model_layer.model_id}"
        rationale = (
            f"Produced by {model_layer.model_id} through the concept_tags_v1 structured-output contract "
            f"(instructions sha256:{model_layer.instructions_sha256[:16]}…, schema sha256:{model_layer.schema_sha256[:16]}…). "
            f"Every assignment's evidence quote was re-sliced from the stored {evidence_field} state "
            f"(sha256:{artifact_digest}) and its SHA-256 matched the carrier-local URN digest; "
            f"{len(model_layer.rejections)} judgment(s) were refused and are recorded with reasons. "
            "This attestation records production and requests review; it is not approval."
        )
        attestation_record = attestation_row(
            attestor_id=attestor,
            attestor_kind=ATTESTOR_KIND_AI_MODEL,
            targets=list(assignment_iris),
            decision=MODEL_ATTESTATION_DECISION,
            attestation_scope=scope_iri,
            context=context,
            rationale=rationale,
        )
        graph.append(
            {
                "@id": f"{partner}:attestation:{attestation_record['attestation_id']}",
                "@type": "rkaf:Attestation",
                "rkaf:attestor": attestation_record["attestor_id"],
                "rkaf:attestorKind": attestation_record["attestor_kind"],
                "rkaf:targets": json.loads(attestation_record["target_ids_json"]),
                "rkaf:decision": attestation_record["decision"],
                "rkaf:attestationScope": attestation_record["attestation_scope"],
                "rkaf:attestedAt": attestation_record["attested_at"],
                "rkaf:rationale": attestation_record["rationale"],
            }
        )

    document = {"@context": settings.context_ref, "@graph": graph}
    run_record = {
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "generated_at": context.asserted_at,
        "run_id": context.run_id,
        "inputs": {
            "profile_id": facts.profile_id,
            "subject_id": artifact.subject_id,
            "corpus_dir": str(settings.corpus_dir),
            "tables_dir": str(settings.tables_dir),
        },
        "artifact": {
            "artifact_id": artifact.artifact_id,
            "version_digest": f"sha256:{artifact.content_sha256}",
            "artifact_iri": artifact_iri,
            "projected_evidence_field": evidence_field,
            "content_digest": f"sha256:{artifact_digest}",
            "available_fields": sorted(artifact.raw_fields),
        },
        "contract_flags": {
            "contract_revision": CONTRACT_REVISION,
            "contract_digest": CONTRACT_DIGEST,
            "assertion_origin_deterministic": ASSERTION_ORIGIN_DETERMINISTIC,
            "request_contract_digest_required_for": sorted(REQUEST_CONTRACT_DIGEST_REQUIRED_FOR),
            "emit_document_docket_edge": EMIT_DOCUMENT_DOCKET_EDGE,
            "document_docket_predicate": DOCUMENT_DOCKET_PREDICATE,
            "emit_profile_edge_projections": EMIT_PROFILE_EDGE_PROJECTIONS,
            "model_attestation_decision": MODEL_ATTESTATION_DECISION,
        },
        "deterministic": {
            "fragments": [
                {
                    "key": fragment.key,
                    "source_field": fragment.source_field,
                    "start": fragment.start,
                    "end": fragment.end,
                    "sha256": fragment.text_sha256,
                    "urn": fragment.urn,
                }
                for fragment in fragments.values()
            ],
            "edges": edge_records,
            "activities": sorted(activities),
        },
        "model": None,
        "judgments": {"accepted": judgment_records, "rejected": []},
        "attestation": attestation_record,
        "notes": list(facts.notes) + list(settings.extra_notes),
        "offset_verification": transcript,
        "node_count": len(graph),
    }
    if model_layer is not None:
        run_record["model"] = {
            "model_id": model_layer.model_id,
            "instructions_sha256": model_layer.instructions_sha256,
            "schema_sha256": model_layer.schema_sha256,
            "input_context_sha256": model_layer.input_context_sha256,
            "extraction_run_directory": model_layer.run_directory,
            "extraction_receipt_sha256": model_layer.receipt_sha256,
            "candidate_selector_version": model_layer.selector_version,
            "candidate_registry_sha256": model_layer.registry_sha256,
            "candidate_concept_count": model_layer.candidate_concept_count,
            "provider_call_count": model_layer.call_count,
            "segment_count": model_layer.segment_count,
            "segments_projected": model_layer.segments_projected,
        }
        if model_layer.segments_projected < model_layer.segment_count:
            run_record["notes"].append(
                f"only {model_layer.segments_projected} of {model_layer.segment_count} segments were sent "
                "to the model (--max-segments); the concept assignments cover that prefix, not the document"
            )
        run_record["judgments"]["rejected"] = [dict(row) for row in model_layer.rejections]
    transcript.append("")
    transcript.append(f"assembled {len(graph)} graph nodes")
    return ProjectionResult(document=document, run_record=run_record, transcript=transcript)


# --------------------------------------------------------------------------- #
# The model layer: one real docpipeline extraction run, projected.
# --------------------------------------------------------------------------- #


def load_registry(path: Path) -> tuple[list[dict[str, Any]], str]:
    """Read the candidate concept registry and digest the file that was read."""
    import hashlib

    from spicy_regs.ontology.common import read_parquet_rows

    digest = hashlib.sha256(Path(path).read_bytes()).hexdigest()
    return read_parquet_rows(Path(path)), digest


def run_model_layer(
    artifact: SourceArtifact,
    *,
    model: Any,
    registry_path: Path,
    run_directory: Path,
    evidence_field: str,
    artifact_iri: str,
    prompt_concept_limit: int = 12,
    max_segments: int = 0,
    allowed_facets: Sequence[str] = ("subject",),
) -> ModelLayer:
    """Ask the model for concept judgments and verify every one of them.

    The call itself is the existing tag path end to end — the same segmenter,
    the same candidate selector, the same instructions, the same strict schema,
    the same grounding, and the same provider custody (``request.json`` and
    ``response.json`` per call under ``extraction/calls/``). Nothing about the
    prompt or the schema is redefined here.
    """
    from spicy_regs.docpipeline.extraction import (
        extraction_plan_facts,
        plan_extraction_items,
        run_extraction,
    )
    from spicy_regs.docpipeline.runtime import RunPlan
    from spicy_regs.docpipeline.segments import SegmentSettings, segment_artifact
    from spicy_regs.docpipeline.tag_task import TagExtractionTask, tag_unit
    from spicy_regs.ontology.concepts import (
        ANCHORED_SELECTOR_VERSION,
        select_candidate_concepts_anchored_v2,
    )
    from spicy_regs.ontology.segmentation import TiktokenCounter

    concepts, registry_sha256 = load_registry(registry_path)
    counter = TiktokenCounter()
    settings = SegmentSettings.selected(tokenizer_version=counter.version)
    segmented = segment_artifact(artifact, settings=settings, counter=counter)
    segments = [
        segment
        for segment in segmented.segments
        if any(slice_.source_field == evidence_field for slice_ in segment.slices)
    ]
    if not segments:
        raise ProjectionError(f"no processing segment covers the projected evidence field {evidence_field!r}")
    segment_count = len(segments)
    if max_segments > 0:
        segments = segments[:max_segments]

    task = TagExtractionTask()
    units = []
    candidate_total = 0
    for segment in segments:
        text = "\n".join(slice_.text for slice_ in segment.slices)
        candidates = select_candidate_concepts_anchored_v2(
            text,
            concepts,
            allowed_facets=tuple(allowed_facets),
            limit=prompt_concept_limit,
        )
        if not candidates:
            continue
        candidate_total += len(candidates)
        units.append(tag_unit(artifact, segment, candidates))
    if not units:
        raise ProjectionError("the candidate selector offered no concepts for any segment")

    items = plan_extraction_items(task, model, units)
    provider = getattr(model, "run_configuration", None)
    plan = RunPlan(
        run_id=Path(run_directory).name,
        mode="diagnostic",
        steps=("extract",),
        source_snapshot={
            "profile_id": artifact.profile_id,
            "subject_id": artifact.subject_id,
            "artifact_id": artifact.artifact_id,
            "content_sha256": artifact.content_sha256,
        },
        segmentation=settings.identity(),
        vocabulary={
            "registry_sha256": registry_sha256,
            "candidate_selector": ANCHORED_SELECTOR_VERSION,
            "prompt_concept_limit": prompt_concept_limit,
        },
        extraction=extraction_plan_facts(task, units),
        provider=(
            dict(provider) if isinstance(provider, Mapping) else {"model_id": str(getattr(model, "model_id", ""))}
        ),
        required_work=tuple(item.work_id for item in items),
    )
    outcome = run_extraction(plan, Path(run_directory), task=task, model=model, units=units)
    if not outcome.passed:
        raise ProjectionError(f"the concept-tag extraction run did not pass: {outcome.outcome.final_state}")

    rows = task.candidate_rows(outcome.candidates)
    judgments, rejections = verify_candidate_rows(
        artifact,
        rows,
        artifact_iri=artifact_iri,
        evidence_field=evidence_field,
    )
    for row in task.rejection_rows(outcome.candidates):
        rejections.append(
            {
                "candidate_id": None,
                "concept_id": _clean(row.get("concept_id")) or None,
                "role": None,
                "source_field": None,
                "evidence_text": None,
                "reason": _clean(row.get("reason")),
                "detail": _clean(row.get("item_json"))[:400],
            }
        )
    receipt = outcome.outcome.receipt
    return ModelLayer(
        model_id=str(getattr(model, "model_id", "")),
        instructions_sha256=text_digest(task.instructions),
        schema_sha256=text_digest(canonical_json(task.build_schema(task.build_payload(units[0].input)))),
        input_context_sha256=text_digest(canonical_json([dict(unit.input) for unit in units])),
        run_directory=str(run_directory),
        receipt_sha256=str(receipt.get("receipt_sha256", "")),
        selector_version=ANCHORED_SELECTOR_VERSION,
        registry_sha256=registry_sha256,
        candidate_concept_count=candidate_total,
        judgments=tuple(judgments),
        rejections=tuple(rejections),
        call_count=len(units),
        segment_count=segment_count,
        segments_projected=len(segments),
    )


def project_document(
    profile_id: str,
    subject_id: str,
    *,
    settings: ProjectionSettings,
    model: Any = None,
    model_run_directory: Path | None = None,
) -> ProjectionResult:
    """Project one corpus document into RKAF, with or without the model layer."""
    artifact, row = load_artifact(profile_id, subject_id, corpus_dir=settings.corpus_dir)
    tables = PublishedTables(settings.tables_dir)
    facts = build_profile_facts(artifact, row, tables=tables, partner=settings.partner)
    model_layer: ModelLayer | None = None
    if model is not None:
        if settings.registry_path is None:
            raise ProjectionError("the model layer needs a candidate concept registry")
        if model_run_directory is None:
            raise ProjectionError("the model layer needs a run directory for provider custody")
        model_layer = run_model_layer(
            artifact,
            model=model,
            registry_path=settings.registry_path,
            run_directory=model_run_directory,
            evidence_field=facts.evidence_field,
            artifact_iri=facts.artifact_iri,
            prompt_concept_limit=settings.prompt_concept_limit,
            max_segments=settings.max_segments,
        )
    return assemble(artifact, facts, settings=settings, model_layer=model_layer)
