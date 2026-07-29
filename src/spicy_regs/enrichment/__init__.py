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

__all__ = [
    "ManagedReleaseCandidateSource",
    "ManagedReleaseConsumerError",
    "authorize_managed_accepted_assignment",
]
