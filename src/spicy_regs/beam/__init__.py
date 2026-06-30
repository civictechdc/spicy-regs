"""Experimental: an Apache Beam proof-of-concept for defining pipelines.

This package is a *spike*, not part of the production ETL. It exists to explore
how the project's ``Reader → Transform → Writer`` model maps onto Apache Beam
(see ``adapters.py``) and to back the written evaluation in ``README.md``.

It lives behind the optional ``beam`` extra so apache-beam's heavy transitive
deps never touch the core install. Importing the package without that extra
raises a clear, actionable error rather than an opaque ``ModuleNotFoundError``
from somewhere deep in the import chain.
"""

try:
    import apache_beam  # noqa: F401
except ModuleNotFoundError as exc:  # pragma: no cover - exercised only without the extra
    raise ModuleNotFoundError(
        "The Apache Beam proof-of-concept requires the optional 'beam' extra. "
        "Install it with:  uv sync --extra beam   "
        "(or: pip install 'spicy-regs[beam]'). "
        "See src/spicy_regs/beam/README.md for what this is and why it's optional."
    ) from exc
