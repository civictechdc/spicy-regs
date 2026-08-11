"""Freeze the outbound import boundary of `docpipeline/rkaf_projection.py`.

`rkaf_projection.py` is scheduled for a four-way split. The split is priceable
only while its outbound surface is a fixed, written-down list: the modules it
reaches for outside the standard library, and the exact names it takes from
each. Once that list drifts, the split has to be re-measured from scratch
before it can be quoted, which is the cost this freeze exists to prevent.

Outbound means every import whose root package is not in the standard library.
That is deliberately wider than the `spicy_regs` cross-subpackage surface plus
`refspec` the file carries today: a new sibling product or a new third-party
runtime dependency lands in this test too, instead of slipping past a filter
that only looked for the packages already present.

The freeze records module and imported names, not line numbers or file order,
so moving an import inside the file is free and changing what crosses the
boundary is not.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECTION_PATH = REPO_ROOT / "src" / "spicy_regs" / "docpipeline" / "rkaf_projection.py"

# One entry per outbound import statement, as (module, imported names). Two
# statements naming the same module stay two entries: the count of statements is
# part of the priced surface, not an artifact of how they were written. An empty
# name tuple is a plain `import module` with no names taken.
FROZEN_OUTBOUND_IMPORTS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "refspec",
        (
            "ConceptEventParticipant",
            "ConceptLabel",
            "ConceptRelation",
            "ReferenceRuntimeError",
            "ReferenceRuntimeStore",
            "assert_managed_vocabulary_row_integrity",
        ),
    ),
    ("spicy_regs.candidate_release", ("CandidateConceptBridge", "CandidateReleaseSource")),
    ("spicy_regs.docpipeline.extraction", ("extraction_plan_facts", "plan_extraction_items", "run_extraction")),
    ("spicy_regs.docpipeline.runtime", ("RunPlan",)),
    ("spicy_regs.docpipeline.segments", ("SegmentSettings", "segment_artifact")),
    ("spicy_regs.docpipeline.source", ("SOURCE_PROFILES",)),
    ("spicy_regs.docpipeline.source", ("SourceArtifact", "build_source_artifact", "profile_for_table")),
    ("spicy_regs.docpipeline.source", ("iter_source_records",)),
    ("spicy_regs.docpipeline.tag_task", ("TagExtractionTask", "tag_unit")),
    (
        "spicy_regs.enrichment.connected_concepts",
        (
            "CONNECTED_INDEXED_REPRESENTATION_VERSION",
            "CONNECTED_SELECTOR_VERSION",
            "select_connected_candidate_concepts",
        ),
    ),
    (
        "spicy_regs.ontology.attestations",
        ("ATTESTOR_KIND_AI_MODEL", "DECISION_ENDORSED_FOR_REVIEW", "attestation_row"),
    ),
    (
        "spicy_regs.ontology.citations",
        (
            "canonical_cfr_iri",
            "canonical_pl_iri",
            "canonical_regsgov_iri",
            "canonical_rin_iri",
            "canonical_usc_iri",
            "docket_reference_as_stated",
            "federal_register_identifier",
            "normalize_docket_reference",
            "parse_authority_citation",
            "parse_cfr_citation",
        ),
    ),
    ("spicy_regs.ontology.common", ("RunContext", "canonical_json", "stable_id", "text_digest")),
    ("spicy_regs.ontology.common", ("read_parquet_rows",)),
    ("spicy_regs.ontology.concepts", ("ANCHORED_SELECTOR_VERSION", "select_candidate_concepts_anchored_v2")),
    ("spicy_regs.ontology.llm", ("resolve_exact_evidence_offsets",)),
    ("spicy_regs.ontology.segmentation", ("TiktokenCounter",)),
)

# The counts PLAN.md section 5 prices the four-way split against, re-derived
# here from the file so the plan and the file cannot disagree in silence.
FROZEN_STATEMENT_COUNT = 17
FROZEN_DISTINCT_SPICY_REGS_MODULES = 13
FROZEN_SUBPACKAGES = frozenset({"candidate_release", "docpipeline", "enrichment", "ontology"})

_FREEZE_RULE = (
    "This list is the priced boundary for the four-way split of "
    "src/spicy_regs/docpipeline/rkaf_projection.py. It is frozen so the split stays "
    "quotable without re-measuring the file. Changing it is a deliberate act: decide "
    "the boundary move first, then update FROZEN_OUTBOUND_IMPORTS in "
    "tests/test_rkaf_projection_boundary.py and the counts in PLAN.md section 5 in the "
    "same change. Do not edit the freeze to make a red test go green."
)


def _outbound_imports(path: Path) -> list[tuple[str, tuple[str, ...]]]:
    """Every non-stdlib import in `path`, as sorted (module, imported names) pairs."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    outbound: list[tuple[str, tuple[str, ...]]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            outbound.extend(
                (alias.name, ()) for alias in node.names if alias.name.partition(".")[0] not in sys.stdlib_module_names
            )
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module and node.module.partition(".")[0] not in sys.stdlib_module_names:
                outbound.append((node.module, tuple(sorted(alias.name for alias in node.names))))
    return sorted(outbound)


def _describe(entries: list[tuple[str, tuple[str, ...]]]) -> str:
    return "\n".join(f"  {module}: {', '.join(names) or '<whole module>'}" for module, names in entries)


def test_rkaf_projection_outbound_imports_match_the_freeze() -> None:
    assert PROJECTION_PATH.is_file(), f"the frozen file is gone: {PROJECTION_PATH}"

    actual = _outbound_imports(PROJECTION_PATH)
    expected = sorted(FROZEN_OUTBOUND_IMPORTS)
    if actual == expected:
        return

    added = [entry for entry in actual if entry not in expected]
    removed = [entry for entry in expected if entry not in actual]
    raise AssertionError(
        f"{_FREEZE_RULE}\n\n"
        f"Crossings added ({len(added)}):\n{_describe(added) or '  none'}\n\n"
        f"Crossings removed ({len(removed)}):\n{_describe(removed) or '  none'}"
    )


def test_rkaf_projection_boundary_matches_the_counts_the_plan_prices() -> None:
    actual = _outbound_imports(PROJECTION_PATH)
    spicy_regs_modules = {module for module, _ in actual if module.startswith("spicy_regs.")}
    subpackages = {module.split(".")[1] for module in spicy_regs_modules}

    assert len(actual) == FROZEN_STATEMENT_COUNT, (
        f"{_FREEZE_RULE}\n\nPLAN.md section 5 prices {FROZEN_STATEMENT_COUNT} outbound import statements; "
        f"the file now has {len(actual)}."
    )
    assert len(spicy_regs_modules) == FROZEN_DISTINCT_SPICY_REGS_MODULES, (
        f"{_FREEZE_RULE}\n\nPLAN.md section 5 prices {FROZEN_DISTINCT_SPICY_REGS_MODULES} distinct spicy_regs "
        f"modules; the file now reaches {len(spicy_regs_modules)}: {sorted(spicy_regs_modules)}"
    )
    assert subpackages == FROZEN_SUBPACKAGES, (
        f"{_FREEZE_RULE}\n\nPLAN.md section 5 prices a split across {sorted(FROZEN_SUBPACKAGES)}; "
        f"the file now spans {sorted(subpackages)}."
    )


def test_rkaf_projection_cannot_route_around_the_freeze() -> None:
    """Relative and dynamic imports cross the same boundary without naming it.

    `from .source import x` and `importlib.import_module("spicy_regs.ontology.llm")`
    both reach outside this module while leaving FROZEN_OUTBOUND_IMPORTS untouched.
    The file uses neither today, and the freeze is only worth its line count while
    that stays true.
    """

    tree = ast.parse(PROJECTION_PATH.read_text(encoding="utf-8"), filename=str(PROJECTION_PATH))

    relative = sorted(
        f"line {node.lineno}: from {'.' * node.level}{node.module or ''} import ..."
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.level > 0
    )
    assert not relative, (
        f"{_FREEZE_RULE}\n\nRelative imports cross the boundary without appearing in the freeze:\n"
        + "\n".join(relative)
    )

    dynamic = sorted(
        f"line {node.lineno}: {name}(...)"
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for name in [node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")]
        if name in {"__import__", "import_module"}
    )
    assert not dynamic, (
        f"{_FREEZE_RULE}\n\nDynamic imports cross the boundary without appearing in the freeze:\n" + "\n".join(dynamic)
    )
