"""Resolve an act-relative citation through the pinned OLRC act index.

"Clean Air Act section 111" names no code, title or section number. Resolving it
is two joins over the artifact ``tools/build_usc_act_index_artifact.py`` seals:

1. **popular name → act.** The Popular Name Tool's names, aliases, and each
   act's Table III key.
2. **act section → U.S.C. section.** Table III's classification of every section
   of that act.

Every answer here is *derived* from those tables at call time. There is no
checked-in lookup of answers: the tables are the evidence, and a resolution that
cannot be re-derived from them is a resolution this module will not give.

A refusal is a first-class result. :class:`ActResolution` always carries either
an identifier or an :data:`UNRESOLVED_REASONS` code — never neither, and never a
guess in place of one.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from spicy_regs.ontology.citations import (
    ActRelativeCitation,
    canonical_usc_iri,
    normalize_popular_name,
)

#: An alias may point at a name the Popular Name Tool does not itself list.
#: "ERISA" points at "Employee Retirement Income Security Act" and the only
#: entry by that name is "… Act **of 1974**".
#:
#: Derivation: the year is how the tool distinguishes one act from another, so
#: it cannot be *dropped* from an alias target — it can only be *supplied*, and
#: only when exactly one act supplies it. "Clean Air Act Amendments" would be
#: 1966, 1970 and 1977; choosing among them would invent a citation the source
#: never made. Measured on release point 119-102: this rule resolves ERISA, the
#: largest act-relative token in the sealed corpus at 17 citations.
ALIAS_YEAR_RULE = "supply-trailing-year-when-exactly-one-act-supplies-it"

#: How deep an alias chain may run before it is abandoned. The tool's own
#: chains are one or two hops; this bounds a cycle without licensing one.
ALIAS_MAX_DEPTH = 8

_YEAR_SUFFIX = re.compile(r"\s+of\s+(?:1[789]|20)\d{2}$")

#: Every way this module declines to publish an identifier. Codes are data, not
#: prose: a consumer counts them, and an artifact records them per citation.
UNRESOLVED_REASONS = (
    #: The popular name is not in the index, or its alias chain does not reach
    #: an act. "INA", "PHS Act" and "ACA" are the measured examples.
    "act_not_in_index",
    #: The act resolved, but its Table III page could not be read at build time.
    #: The receipt names which act and why.
    "source_incomplete",
    #: Table III has no row for that act section.
    "act_section_not_classified",
    #: Table III classified the section somewhere that no longer stands
    #: ("Rep.", "Elim.", "Omitted").
    "classification_not_current",
    #: Table III names a target the ``rkaf:us-usc`` lexical space cannot spell
    #: (a note, a range, an "et seq.").
    "usc_section_not_expressible",
)


@dataclass(frozen=True)
class ActResolution:
    """What an act-relative citation resolved to, or why it did not."""

    citation: ActRelativeCitation
    act_key: str | None = None
    table3_key: str | None = None
    usc_title: str | None = None
    usc_section: str | None = None
    iri: str | None = None
    unresolved_reason: str | None = None

    def __post_init__(self) -> None:
        if (self.iri is None) == (self.unresolved_reason is None):
            raise ValueError("a resolution states an identifier or a reason, never both or neither")
        if self.unresolved_reason is not None and self.unresolved_reason not in UNRESOLVED_REASONS:
            raise ValueError(f"undeclared unresolved reason: {self.unresolved_reason!r}")


class ActIndex(Protocol):
    """The two joins, however they are loaded."""

    #: normalized popular name -> Table III key, for acts that have one.
    table3_key_by_name: Mapping[str, str]
    #: normalized popular name -> the normalized name it redirects to.
    alias_by_name: Mapping[str, str]
    #: Table III key -> act section -> (usc_title, usc_section, status).
    classifications: Mapping[str, Mapping[str, tuple[str | None, str | None, str | None]]]
    #: Table III keys whose page could not be read. Distinguishes "this act
    #: classifies nothing here" from "we could not look".
    incomplete_sources: frozenset[str]


def resolve_act_name(name: str, index: ActIndex) -> str | None:
    """The act a popular name refers to, following aliases. ``None`` when none.

    Implements :data:`ALIAS_YEAR_RULE`. Refuses rather than guesses in all three
    failing cases: an unlisted name, a chain that cycles or runs past
    :data:`ALIAS_MAX_DEPTH`, and a target several acts could supply.
    """
    by_stem: dict[str, list[str]] = {}
    for known in index.table3_key_by_name:
        stem = _YEAR_SUFFIX.sub("", known)
        if stem != known:
            by_stem.setdefault(stem, []).append(known)

    seen: set[str] = set()
    current = normalize_popular_name(name)
    for _ in range(ALIAS_MAX_DEPTH):
        if current in index.table3_key_by_name:
            return current
        supplied = by_stem.get(current, ())
        if len(supplied) == 1:
            return supplied[0]
        if current in seen or current not in index.alias_by_name:
            return None
        seen.add(current)
        current = index.alias_by_name[current]
    return None


def resolve_act_relative_citation(citation: ActRelativeCitation, *, index: ActIndex) -> ActResolution:
    """Resolve one act-relative citation to a U.S.C. identifier, or say why not."""
    act_key = resolve_act_name(citation.act_key, index)
    if act_key is None:
        return ActResolution(citation, unresolved_reason="act_not_in_index")
    table3_key = index.table3_key_by_name[act_key]
    if table3_key in index.incomplete_sources:
        return ActResolution(citation, act_key=act_key, table3_key=table3_key, unresolved_reason="source_incomplete")
    row = index.classifications.get(table3_key, {}).get(citation.section)
    common = {"act_key": act_key, "table3_key": table3_key}
    if row is None:
        return ActResolution(citation, **common, unresolved_reason="act_section_not_classified")
    usc_title, usc_section, status = row
    if status:
        return ActResolution(citation, **common, unresolved_reason="classification_not_current")
    if not (usc_title and usc_section):
        return ActResolution(citation, **common, unresolved_reason="act_section_not_classified")
    try:
        iri = canonical_usc_iri(usc_title, usc_section)
    except ValueError:
        return ActResolution(
            citation,
            **common,
            usc_title=usc_title,
            usc_section=usc_section,
            unresolved_reason="usc_section_not_expressible",
        )
    return ActResolution(citation, **common, usc_title=usc_title, usc_section=usc_section, iri=iri)
