"""Candidate-only access to externally managed RefSpec releases.

RefSpec management, reconciliation, evaluation, and deployment APIs remain in
the RefSpec package. Spicy exposes only the read-only consumer boundary.
"""

from spicy_regs.enrichment.managed_release import (
    ManagedReleaseCandidateSource,
    ManagedReleaseConsumerError,
)
from spicy_regs.enrichment.accepted_output import (
    authorize_managed_accepted_assignment,
)
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
    "ManagedReleaseCandidateSource",
    "ManagedReleaseConsumerError",
    "authorize_managed_accepted_assignment",
    "select_connected_candidate_concepts",
]
