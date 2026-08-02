"""CiteURL detection arm, run in an isolated interpreter.

CiteURL is an **experimental** comparator, not a dependency. It never enters
this repo's environment: ``run_citation_bakeoff.py`` builds a throwaway venv,
installs a pinned CiteURL into it, and executes this file with *that*
interpreter. Nothing here may import ``spicy_regs``, because the scratch venv
does not have it.

The package also imports ``markdown`` without declaring it (``citeurl.mdx``
does ``from markdown.extensions import Extension`` at import time, and
``citeurl/__init__.py`` imports ``mdx`` unconditionally), so a bare
``pip install citeurl`` produces a package that cannot be imported. The driver
installs ``markdown`` alongside it and pins both versions in the receipt; this
file reports what it actually imported so the receipt states the truth rather
than the intent.

Protocol: read ``{"strings": [...]}`` on stdin, write one JSON object on
stdout: the pinned versions, and for each input string the sorted set of
CiteURL **template names** that matched it. Template names, not URLs — a
package URL is never identity here, and the driver maps names to citation
families under its own declared table.
"""

from __future__ import annotations

import json
import sys
import warnings


def _installed_version(package: str) -> str | None:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(package)
    except PackageNotFoundError:
        return None


def main() -> int:
    # CiteURL 12.0.3 emits SyntaxWarning for invalid escape sequences in its own
    # source at import time. That is the package's defect, not a signal, and it
    # would otherwise pollute the arm's stderr.
    warnings.simplefilter("ignore", SyntaxWarning)

    request = json.load(sys.stdin)
    strings = [str(item) for item in request["strings"]]

    from citeurl import Citator

    citator = Citator()
    templates = {text: sorted({cite.template.name for cite in citator.list_cites(text)}) for text in strings}

    json.dump(
        {
            "pin": {
                "citeurl": _installed_version("citeurl"),
                "markdown": _installed_version("markdown"),
                "python": ".".join(str(part) for part in sys.version_info[:3]),
                "templates_loaded": len(citator.templates),
            },
            "templates": templates,
        },
        sys.stdout,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
