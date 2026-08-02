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
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

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
    #: Table III is keyed by the **enacting Public Law**, not by the act, and a
    #: Public Law may carry many acts: 116-260 carries 94 popular names, 117-328
    #: carries 70. So one ``(table3_key, act_section)`` can name several
    #: classifications belonging to different acts, and nothing in the citation
    #: discriminates them. 471 of 9,916 pairs in the sealed artifact are like
    #: this. Choosing among them is how ``sec. 107 of the Taxpayer Certainty and
    #: Disaster Tax Relief Act of 2020`` became ``urn:rkaf:us:usc:49:60122`` --
    #: pipeline-safety civil penalties, for a tax act.
    "act_section_ambiguous",
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


@dataclass(frozen=True)
class ActIndex:
    """The two joins, loaded from the pinned artifact or built for a test."""

    #: normalized popular name -> Table III key, for acts that have one.
    table3_key_by_name: Mapping[str, str] = field(default_factory=dict)
    #: normalized popular name -> the normalized name it redirects to.
    alias_by_name: Mapping[str, str] = field(default_factory=dict)
    #: Table III key -> act section -> **every** classification row for it.
    #:
    #: A tuple, not a single row, because Table III is keyed by the enacting
    #: Public Law rather than by the act. A single-valued mapping here silently
    #: discarded 1,060 of the sealed artifact's 10,976 rows and let the survivor
    #: be chosen by the row sort -- which is not a citation rule.
    classifications: Mapping[str, Mapping[str, tuple[tuple[str | None, str | None, str | None], ...]]] = field(
        default_factory=dict
    )
    #: Table III keys whose page could not be read. Distinguishes "this act
    #: classifies nothing here" from "we could not look".
    incomplete_sources: frozenset[str] = frozenset()

    @classmethod
    def from_artifact(cls, artifact_dir: Path) -> ActIndex:
        """Load the index from a sealed ``usc-act-index-artifact-v1`` directory.

        The measurement any caller reports has to be reproducible from the
        pinned bytes. Before this existed the only loader was a test fixture,
        so the published 67/47/20 depended on a collapse policy that appeared
        in no committed code -- and first-wins and last-wins gave different
        answers.
        """
        import pyarrow.parquet as pq

        directory = Path(artifact_dir)
        receipt = json.loads((directory / "receipt.json").read_text(encoding="utf-8"))
        table3_key_by_name: dict[str, str] = {}
        alias_by_name: dict[str, str] = {}
        for row in pq.read_table(directory / "usc-popular-names.parquet").to_pylist():
            if row["content_type"] == "cite" and row["table3_key"]:
                table3_key_by_name.setdefault(row["name_key"], row["table3_key"])
            if row["see_also_key"]:
                alias_by_name.setdefault(row["name_key"], row["see_also_key"])
        classifications: dict[str, dict[str, tuple]] = {}
        for row in pq.read_table(directory / "usc-act-sections.parquet").to_pylist():
            by_section = classifications.setdefault(row["table3_key"], {})
            by_section[row["act_section"]] = by_section.get(row["act_section"], ()) + (
                (row["usc_title"], row["usc_section"], row["status"]),
            )
        return cls(
            table3_key_by_name=table3_key_by_name,
            alias_by_name=alias_by_name,
            classifications=classifications,
            incomplete_sources=frozenset(hole["table3_key"] for hole in receipt.get("source_incomplete", ())),
        )


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
    rows = index.classifications.get(table3_key, {}).get(citation.section, ())
    common = {"act_key": act_key, "table3_key": table3_key}
    if not rows:
        return ActResolution(citation, **common, unresolved_reason="act_section_not_classified")
    if len(rows) > 1:
        # Several acts share this Public Law and nothing in the citation says
        # which one. Refuse: the source text often carries the discriminator
        # ("Division EE"), but reading it is a separate, reviewable change.
        return ActResolution(citation, **common, unresolved_reason="act_section_ambiguous")
    usc_title, usc_section, status = rows[0]
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
