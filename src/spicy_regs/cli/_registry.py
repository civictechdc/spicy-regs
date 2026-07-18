"""The ``spicy-regs`` command registry.

To add a command: create a module in this package with ``register(subparsers)``
and ``run(args) -> int`` functions (copy ``tables.py`` as a template), then add
the module here. The list order is the help-text order.
"""

from __future__ import annotations

from spicy_regs.cli import agencies, describe, download, query, sample, search, stats, tables

COMMANDS = [download, tables, describe, query, stats, sample, search, agencies]
