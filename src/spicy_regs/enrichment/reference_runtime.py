"""Compatibility imports for the RefSpec reference package.

New Spicy Regs code imports these interfaces from :mod:`refspec`. This module
remains temporarily so older callers fail neither at import time nor during a
staged migration. It contains no vocabulary-management implementation.
"""

from refspec import *  # noqa: F401,F403
from refspec import __all__ as _REFSPEC_ALL

__all__ = list(_REFSPEC_ALL)
