"""Product-local candidate lookup utilities.

Legacy RefSpec-backed adapters remain available from their explicit modules
while their callers migrate to published-file readers. They are deliberately
not imported here, so ordinary enrichment utilities have no RefSpec runtime
dependency.
"""

from spicy_regs.enrichment.connected_concepts import (
    CONNECTED_INDEXED_REPRESENTATION_VERSION,
    CONNECTED_SELECTOR_VERSION,
    ConnectedConceptSearchError,
    select_connected_candidate_concepts,
)

__all__ = [
    "CONNECTED_INDEXED_REPRESENTATION_VERSION",
    "CONNECTED_SELECTOR_VERSION",
    "ConnectedConceptSearchError",
    "select_connected_candidate_concepts",
]
