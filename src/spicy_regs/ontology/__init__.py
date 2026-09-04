"""Metadata and ontology primitives for the regulatory corpus.

The public tables are built by transforms under :mod:`spicy_regs.transforms`.
This package contains the shared citation grammars, identifier expansion,
provenance helpers, concept-loop logic, and invariants used by those transforms.
"""

from spicy_regs.ontology.citations import (
    AuthorityCitation,
    CfrCitation,
    canonical_cfr_iri,
    canonical_frdoc_iri,
    canonical_pl_iri,
    canonical_regsgov_iri,
    canonical_rin_iri,
    canonical_usc_iri,
    federal_register_identifier,
    normalize_regsgov_identifier,
    parse_authority_citation,
    parse_cfr_citation,
)
from spicy_regs.ontology.common import ATTESTATION_COLUMNS, RunContext, stable_id

__all__ = [
    "ATTESTATION_COLUMNS",
    "AuthorityCitation",
    "CfrCitation",
    "RunContext",
    "canonical_cfr_iri",
    "canonical_frdoc_iri",
    "canonical_pl_iri",
    "canonical_regsgov_iri",
    "canonical_rin_iri",
    "canonical_usc_iri",
    "federal_register_identifier",
    "normalize_regsgov_identifier",
    "parse_authority_citation",
    "parse_cfr_citation",
    "stable_id",
]
