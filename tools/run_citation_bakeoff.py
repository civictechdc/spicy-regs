"""The citation-parsing bakeoff: current parser vs current + CiteURL.

`docs/decisions.md` 2026-07-27 ("Citation parsing: supplement-first") records an
exploratory probe over all 4,777 distinct Unified Agenda authority strings —
4,157 recognized by both arms, 233 current-parser-only, 108 CiteURL-only, 279
neither — and says of it: *"no committed command yet makes it reproducible."*
This is that command.

Three phases, deliberately separated by how deterministic they are:

* ``detect`` — freeze the authority strings out of ``unified_agenda.parquet``,
  run both detection arms over them, classify every string into one of four
  cells, and write a **deterministic** artifact. Rebuilding from the same
  inputs with the same pinned versions reproduces every byte; the receipt
  carries no timestamp and no absolute path.
* ``adjudicate`` — draw a seeded stratified sample from the three disagreement
  cells and ask a frontier model, once per string (k=1), what citations the
  string *actually* contains and which arm read it correctly. This half is
  **not** deterministic and does not pretend to be: every call carries its own
  receipt (request/response digests, model id, token counts, cost), and a
  per-item failure is recorded as a failure. Nothing is retried until it
  agrees.
* ``verdict`` — roll the adjudicated ground truth up per citation family.

Two arms, and a third reading published beside them:

* ``current`` — :func:`spicy_regs.ontology.citations.parse_authority_citation`,
  the single function the original probe used. Reproducing the recorded
  four-cell table requires comparing exactly this.
* ``current_extended`` — every project-owned grammar that can read an authority
  string: the authority forms above plus ``parse_cfr_citation``,
  ``normalize_rin``, ``canonical_frdoc_iri`` and ``normalize_docket_reference``.
  Published beside the probe arm because a "CiteURL-only" win against
  ``parse_authority_citation`` may be a citation the project already parses in
  a different function — which is a very different fact.
* ``citeurl`` — pinned CiteURL, executed in a scratch venv (see below).

CiteURL is an experimental comparator and never enters this repo's dependency
tree. ``ensure_citeurl_venv`` builds a throwaway environment, installs the
pinned CiteURL **and** the ``markdown`` it imports without declaring, and
``citation_bakeoff_citeurl_worker.py`` runs there. The receipt pins the
versions actually imported.

This tool evaluates *detection only*. It changes no identity, wires nothing,
and retires no regex.

Example::

    uv run python tools/run_citation_bakeoff.py detect \\
        --unified-agenda output/rin-ontology-revision-candidate/unified_agenda.parquet \\
        --output output/citation-bakeoff-2026-08-02 \\
        --citeurl-venv /tmp/bakeoff-venv

    uv run python tools/run_citation_bakeoff.py adjudicate \\
        --output output/citation-bakeoff-2026-08-02 \\
        --model gemini-3.6-flash --cost-cap-usd 5.0

    uv run python tools/run_citation_bakeoff.py verdict \\
        --output output/citation-bakeoff-2026-08-02
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

# Re-exported so a caller (and the test suite's stub models) can raise and
# build the adapter's own result types without importing the adapter package.
from spicy_regs.docpipeline.adapters import (  # noqa: E402, F401
    StructuredTextCallError,
    StructuredTextResult,
)
from spicy_regs.ontology.citations import (  # noqa: E402
    canonical_frdoc_iri,
    normalize_docket_reference,
    normalize_rin,
    parse_authority_citation,
    parse_cfr_citation,
)

DETECTION_SCHEMA_VERSION = "citation-bakeoff-detection-v1"
ADJUDICATION_SCHEMA_VERSION = "citation-bakeoff-adjudication-v1"
VERDICT_SCHEMA_VERSION = "citation-bakeoff-verdict-v1"

#: The source column. The Unified Agenda stores each RIN's authorities as a
#: JSON list of free-text strings; the bakeoff population is the distinct set.
AUTHORITY_COLUMN = "legal_authority_json"

WORKER_PATH = REPO_ROOT / "tools" / "citation_bakeoff_citeurl_worker.py"

#: Pinned comparator. CiteURL is experimental and stays out of pyproject.toml.
CITEURL_VERSION = "12.0.3"
#: CiteURL 12.0.3 imports ``markdown`` without declaring it, so a bare install
#: yields an unimportable package. This is the workaround, pinned.
MARKDOWN_VERSION = "3.10.3"
CITEURL_UNDECLARED_IMPORT_NOTE = (
    "citeurl/__init__.py imports .mdx unconditionally and citeurl/mdx.py does "
    "'from markdown.extensions import Extension', but markdown is not declared as a "
    "dependency; a bare 'pip install citeurl' produces a package that raises "
    "ModuleNotFoundError on import. markdown is installed alongside it in the scratch venv."
)

#: Every CiteURL template observed firing on the frozen 4,777-string corpus.
#: Pinned so a template that starts firing after a CiteURL upgrade shows up as
#: an unmapped family in the receipt rather than being silently folded in.
OBSERVED_CITEURL_TEMPLATES = (
    "Clean Water Act",
    "Code of Federal Regulations",
    "District of Columbia Official Code",
    "Federal Register",
    "Immigration & Nationality Act",
    "Internal Revenue Code",
    "U.S. Caselaw",
    "U.S. Code",
    "U.S. Public Laws",
    "U.S. Statutes at Large",
)

#: Template name -> citation family.
#:
#: ``fr_vol_page`` is deliberately not ``fr_doc``. CiteURL's Federal Register
#: template reads a volume/page reference ("89 FR 1234"); the project's Federal
#: Register grammar reads a *document number* ("2026-13078"). They name
#: different things, and scoring them as one family would credit each arm with
#: a capability the other has.
#:
#: The act-name templates (INA, IRC, Clean Water Act) resolve into the U.S.
#: Code, so they count as ``usc`` detections.
CITEURL_TEMPLATE_FAMILIES: dict[str, str] = {
    "Clean Air Act": "usc",
    "Clean Water Act": "usc",
    "Code of Federal Regulations": "cfr",
    "District of Columbia Official Code": "state_local",
    "Endangered Species Act": "usc",
    "Federal Register": "fr_vol_page",
    "Immigration & Nationality Act": "usc",
    "Internal Revenue Code": "usc",
    "National Labor Relations Act": "usc",
    "U.S. Caselaw": "caselaw",
    "U.S. Code": "usc",
    "U.S. Constitution": "constitution",
    "U.S. Public Laws": "pl",
    "U.S. Statutes at Large": "stat",
}

#: The families the evidence document reports on.
FAMILIES = ("usc", "cfr", "pl", "eo", "stat", "fr_doc", "fr_vol_page", "docket", "rin")

#: The families whose project-owned grammars are meant to read **free text**.
#:
#: The comparison a recommendation can rest on uses these and only these.
#: ``normalize_rin``, ``canonical_frdoc_iri`` and ``normalize_docket_reference``
#: read a *column* whose every value is supposed to be one identifier; pointed
#: at authority prose they over-fire (``normalize_docket_reference`` returns
#: early for anything the Regulations.gov syntax can spell, so a bare section
#: number such as "1255" comes back as a docket). Scoring them against CiteURL
#: would charge the project for false positives it does not make in
#: production, where those functions are never handed authority text.
TEXT_GRAMMAR_FAMILIES = frozenset({"usc", "cfr", "pl", "eo", "stat"})

CELLS = ("both", "current_only", "citeurl_only", "neither")
DISAGREEMENT_CELLS = ("current_only", "citeurl_only", "neither")

#: Environment variables whose values must never reach an artifact.
CREDENTIAL_ENVIRONMENT_VARIABLES = (
    "GEMINI_API_KEY",
    "OPENAI_API_KEY",
    "OPENROUTER_API_KEY",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
    "CLOUDFLARE_API_TOKEN",
)
#: A credential shorter than this is not distinctive enough to scan for: a
#: one-character value would make every artifact look like a leak.
MIN_CREDENTIAL_LENGTH = 8

# --------------------------------------------------------------------------
# adjudication configuration
# --------------------------------------------------------------------------

DEFAULT_PROVIDER = "gemini"
DEFAULT_MODEL = "gemini-3.6-flash"
#: Must cover thinking tokens *and* the answer; see the pilot note below.
DEFAULT_MAX_OUTPUT_TOKENS = 3000
DEFAULT_SEED = 20260802
DEFAULT_COST_CAP_USD = 5.0

#: Pinned prices, USD per million tokens. Recorded in the receipt so a reader
#: can recompute the spend if the published price moves; the token counts the
#: provider reported are recorded next to it, which is the durable fact.
PRICES_USD_PER_MTOK = {"input": 0.30, "output": 2.50}

#: Conservative per-call token estimate used only for the pre-flight projection.
#: Measured on a 6-call pilot, not guessed. Gemini 3.6 Flash is a reasoning
#: model: it spends output budget on thinking tokens before it emits the
#: answer, and they are billed as output. A pilot at ``max_output_tokens=700``
#: returned ``finish_reason="length"`` with 13 answer tokens and ~1,400 total —
#: the budget went entirely to thinking. The projection below reflects the
#: measured cost at ``reasoning_effort="low"`` with a budget that fits.
ESTIMATED_INPUT_TOKENS_PER_CALL = 800
ESTIMATED_OUTPUT_TOKENS_PER_CALL = 900

#: Low, and pinned. Detection adjudication over a short authority string is a
#: recognition question, not a research task; unbounded thinking multiplies the
#: bill without changing the answer.
DEFAULT_REASONING_EFFORT = "low"

ADJUDICATION_VERDICTS = (
    "current_parser_correct",
    "citeurl_correct",
    "both_partial",
    "neither",
    "garbage",
)

SCHEMA_NAME = "citation_adjudication"

RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["string_id", "contains_citations", "citations", "verdict", "reason"],
    "properties": {
        "string_id": {"type": "string"},
        "contains_citations": {"type": "boolean"},
        "citations": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["family", "text"],
                "properties": {
                    "family": {"type": "string", "enum": [*FAMILIES, "other"]},
                    "text": {"type": "string"},
                },
            },
        },
        "verdict": {"type": "string", "enum": list(ADJUDICATION_VERDICTS)},
        "reason": {"type": "string"},
    },
}

INSTRUCTIONS = """\
You adjudicate United States legal-citation DETECTION on a single string taken \
from the "legal authority" field of a Unified Agenda regulatory entry.

You are given one authority string and what two automated systems detected in \
it. Answer two questions.

1. GROUND TRUTH. What citations does the string ACTUALLY contain? List each \
one with its family and the exact substring that expresses it. Copy the \
substring verbatim from the string; never normalize, expand, or invent it. If \
the string contains no legal citation at all, return an empty list and set \
contains_citations to false.

Families:
  usc          - United States Code ("42 U.S.C. 7401", "section 552 of title 5")
  cfr          - Code of Federal Regulations ("40 CFR Part 60")
  pl           - Public Law number ("Pub. L. 117-2")
  eo           - Executive Order ("Executive Order 13563", "E.O. 12866")
  stat         - Statutes at Large ("136 Stat. 6156")
  fr_doc       - Federal Register DOCUMENT NUMBER ("2026-13078")
  fr_vol_page  - Federal Register VOLUME AND PAGE ("89 FR 1234")
  docket       - a Regulations.gov docket id ("FAA-2026-3485")
  rin          - a Regulation Identifier Number ("0648-AB12")
  other        - a real citation none of the above covers (a state code, a \
treaty, a case, a constitutional provision, an act name carrying no numeric \
citation)

2. VERDICT. Which system read the string correctly?
  current_parser_correct - the current parser's detections match ground truth \
and CiteURL's do not (it missed a citation, or claimed one that is not there)
  citeurl_correct        - CiteURL's detections match ground truth and the \
current parser's do not
  both_partial           - each system got part of it right and part wrong, or \
both missed something real
  neither                - the string contains a citation and NEITHER system \
detected it correctly
  garbage                - the string contains no legal citation at all, so \
detecting nothing is the correct answer

Judge only DETECTION: did the system notice the citation that is there, and \
not claim one that is not? Do not penalize a system for normalization style, \
for the URL it would build, or for how it spells an identifier.

An act name with no numbers ("the Small Business Act") is not a citation. \
"et seq." and "as amended" are not citations. A bare section number with no \
title or code ("section 7(a)") is not a citation on its own.

Be strict about false positives. If a system reports a citation the string \
does not contain, that system is wrong even if it also found a real one.

Echo back the string_id exactly as given. Give a one-sentence reason.
"""


# --------------------------------------------------------------------------
# determinism helpers (origin: tools/build_date_event_artifact.py)
# --------------------------------------------------------------------------


def canonical_json(value: object) -> str:
    """Serialize deterministically (origin: spicy_regs/ontology/common.py:84)."""

    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def sha256_text(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return f"sha256:{digest.hexdigest()}"


def _pin_path(path: Path) -> str:
    """Record a repo-relative path when possible, else the basename.

    Keeping absolute scratch paths out of the receipt keeps rebuilds from
    different working directories byte-identical.
    """

    resolved = Path(path).resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        return resolved.name


class SecretLeakError(RuntimeError):
    """An artifact contains the value of a credential environment variable."""


def assert_secret_free(text: str, variables: tuple[str, ...] = CREDENTIAL_ENVIRONMENT_VARIABLES) -> None:
    """Refuse to publish text carrying a live credential.

    Names the variable, never the value: a refusal has to be safe to log.
    """
    for name in variables:
        value = os.environ.get(name) or ""
        if len(value) >= MIN_CREDENTIAL_LENGTH and value in text:
            raise SecretLeakError(f"artifact contains the value of {name}")


def _write(path: Path, payload: object) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = canonical_json(payload)
    assert_secret_free(text)
    path.write_text(text, encoding="utf-8")
    return sha256_text(text)


# --------------------------------------------------------------------------
# phase 1a: freeze the authority strings
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class FrozenStrings:
    """The bakeoff population, and an honest account of how it was read."""

    strings: list[str]
    digest: str
    source_digest: str
    rows_read: int = 0
    values_read: int = 0
    malformed_rows: int = 0
    empty_rows: int = 0

    @property
    def count(self) -> int:
        return len(self.strings)


def extract_authority_strings(path: Path) -> FrozenStrings:
    """Freeze the distinct Unified Agenda authority strings, digest-pinned.

    The digest is taken over the **sorted distinct string set**, not over the
    parquet file. Several published copies of ``unified_agenda.parquet`` differ
    byte-for-byte (different column sets, different write settings) while
    carrying exactly the same authority population; pinning the set is what
    makes "the 4,777 strings" a reproducible object rather than a property of
    one file.

    A row whose JSON does not parse is counted, not dropped silently — the
    receipt reports it, so a malformed source can never quietly shrink the
    population.
    """
    path = Path(path)
    table = pq.read_table(path, columns=[AUTHORITY_COLUMN])
    values = table.column(AUTHORITY_COLUMN).to_pylist()

    distinct: set[str] = set()
    rows_read = 0
    values_read = 0
    malformed_rows = 0
    empty_rows = 0
    for raw in values:
        rows_read += 1
        text = "" if raw is None else str(raw).strip()
        if not text:
            empty_rows += 1
            continue
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            malformed_rows += 1
            continue
        if not isinstance(parsed, list):
            malformed_rows += 1
            continue
        for item in parsed:
            if item is None:
                continue
            values_read += 1
            value = str(item).strip()
            if value:
                distinct.add(value)

    strings = sorted(distinct)
    return FrozenStrings(
        strings=strings,
        digest=sha256_text(canonical_json(strings)),
        source_digest=file_sha256(path),
        rows_read=rows_read,
        values_read=values_read,
        malformed_rows=malformed_rows,
        empty_rows=empty_rows,
    )


# --------------------------------------------------------------------------
# phase 1b: the detection arms
# --------------------------------------------------------------------------


def current_authority_recognized(text: str) -> bool:
    """Whether ``parse_authority_citation`` recognized anything at all.

    That function always returns at least one result; a single ``other``/
    ``failed`` result is how it says it recognized nothing. This is the exact
    predicate the original probe used, and the reason its four-cell table
    reproduces.
    """
    citations = parse_authority_citation(text)
    if len(citations) == 1 and citations[0].authority_type == "other" and citations[0].parse_status == "failed":
        return False
    return bool(citations)


_AUTHORITY_TYPE_FAMILIES = {
    "usc": "usc",
    "public_law": "pl",
    "statute_at_large": "stat",
    "eo": "eo",
}


def current_families(text: str) -> list[str]:
    """Every family the project's own grammars read out of one authority string.

    This is the ``current_extended`` arm: not just
    :func:`parse_authority_citation` but every project-owned reader that can be
    pointed at a free-text authority value.

    ``parse_cfr_citation`` is applied exactly as the project defines it,
    including its compact-key branch (``"40-60"`` read as 40 CFR 60). Over free
    authority text that branch is a known over-read — any bare ``N-M`` string
    matches it — so :func:`cfr_compact_key_only` counts the strings whose only
    CFR evidence is that branch, and the receipt publishes the count rather
    than the tool quietly choosing a different parser than the project ships.
    """
    families: set[str] = set()
    for citation in parse_authority_citation(text):
        family = _AUTHORITY_TYPE_FAMILIES.get(citation.authority_type)
        if family is not None:
            families.add(family)
    if parse_cfr_citation(text):
        families.add("cfr")
    if normalize_rin(text) is not None:
        families.add("rin")
    if normalize_docket_reference(text) is not None:
        families.add("docket")
    try:
        canonical_frdoc_iri(text)
    except ValueError:
        pass
    else:
        families.add("fr_doc")
    return sorted(families)


def cfr_compact_key_only(text: str) -> bool:
    """Whether the only CFR evidence is the compact-key branch (``"40-60"``)."""
    if not parse_cfr_citation(text):
        return False
    from spicy_regs.ontology.citations import _CFR_STANDARD, _CFR_TITLE_PART  # noqa: PLC0415

    return not (_CFR_STANDARD.search(text) or _CFR_TITLE_PART.search(text))


def current_text_grammar_recognized(text: str) -> bool:
    """Whether a project-owned **free-text** grammar reads anything here."""
    return bool(TEXT_GRAMMAR_FAMILIES & set(current_families(text)))


def text_grammar_cells(records: list[dict[str, Any]]) -> dict[str, str]:
    """Re-cut published detection records against the text-grammar arm.

    Derived from ``current_families``, which the detection artifact already
    publishes per string, so this reclassification needs no rebuild and cannot
    disagree with the sealed artifact it reads.
    """
    cells: dict[str, str] = {}
    for record in records:
        current = bool(TEXT_GRAMMAR_FAMILIES & set(record.get("current_families", [])))
        cells[str(record["string_id"])] = classify_cell(
            current=current,
            citeurl=bool(record.get("citeurl_recognized", False)),
        )
    return cells


def false_positive_counts(records: list[dict[str, Any]]) -> dict[str, int]:
    """How often each arm claimed a citation the string does not contain.

    The safety half of the comparison. Detection coverage on its own rewards an
    arm for firing on everything; this is the column that charges it for doing
    so. Ground truth is the judge's ``contains_citations``.
    """
    current = 0
    citeurl = 0
    adjudicated = 0
    for record in records:
        if record.get("status") != "adjudicated":
            continue
        adjudicated += 1
        response = record.get("response") or {}
        if response.get("contains_citations"):
            continue
        if record.get("current_text_recognized"):
            current += 1
        if record.get("citeurl_recognized"):
            citeurl += 1
    return {"current_text_grammars": current, "citeurl": citeurl, "adjudicated": adjudicated}


def citeurl_families(template_names: list[str]) -> tuple[list[str], list[str]]:
    """Map CiteURL template names to families, surfacing anything undeclared.

    Returns ``(families, unmapped_template_names)``. An unmapped template is
    never folded into a family and never dropped: it comes back so the receipt
    can name it. A CiteURL upgrade that starts firing a new template therefore
    shows up as a receipted unknown rather than as a silent score change.
    """
    families: set[str] = set()
    unmapped: set[str] = set()
    for name in template_names:
        family = CITEURL_TEMPLATE_FAMILIES.get(name)
        if family is None:
            unmapped.add(name)
        else:
            families.add(family)
    return sorted(families), sorted(unmapped)


def classify_cell(*, current: bool, citeurl: bool) -> str:
    """The four-cell classification the decision record reported."""
    if current and citeurl:
        return "both"
    if current:
        return "current_only"
    if citeurl:
        return "citeurl_only"
    return "neither"


def four_cell_table(records: list[dict[str, Any]]) -> dict[str, int]:
    counts = {cell: 0 for cell in CELLS}
    for record in records:
        counts[str(record["cell"])] += 1
    return counts


# --------------------------------------------------------------------------
# the CiteURL arm, in its own interpreter
# --------------------------------------------------------------------------


def ensure_citeurl_venv(venv: Path, *, rebuild: bool = False) -> Path:
    """Build (or reuse) a scratch venv holding the pinned CiteURL.

    Deliberately not the repo environment. CiteURL is an experimental
    comparator; nothing here adds it to ``pyproject.toml``.
    """
    venv = Path(venv)
    python = venv / "bin" / "python"
    if rebuild or not python.exists():
        subprocess.run(["uv", "venv", str(venv), "--python", "3.12"], check=True, capture_output=True)
        environment = {**os.environ, "VIRTUAL_ENV": str(venv)}
        subprocess.run(
            [
                "uv",
                "pip",
                "install",
                f"citeurl=={CITEURL_VERSION}",
                f"markdown=={MARKDOWN_VERSION}",
            ],
            check=True,
            capture_output=True,
            env=environment,
        )
    return python


def run_citeurl_arm(strings: list[str], python: Path) -> dict[str, Any]:
    """Run the CiteURL arm out-of-process and return its pin plus per-string hits."""
    completed = subprocess.run(
        [str(python), str(WORKER_PATH)],
        input=canonical_json({"strings": strings}),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(completed.stdout)


# --------------------------------------------------------------------------
# phase 1c: the deterministic detection artifact
# --------------------------------------------------------------------------


def build_detection_artifact(
    *,
    frozen: FrozenStrings,
    citeurl_templates: dict[str, list[str]],
    citeurl_pin: dict[str, Any],
    source_path: Path,
    output: Path,
) -> dict[str, Any]:
    """Write the deterministic half of the bakeoff.

    No timestamps and no absolute paths inside any sealed surface, so a rebuild
    from byte-identical inputs is byte-identical (the pattern from
    ``build_date_event_artifact.py``).
    """
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)

    records: list[dict[str, Any]] = []
    unmapped_templates: Counter[str] = Counter()
    template_hits: Counter[str] = Counter()
    current_family_hits: Counter[str] = Counter()
    citeurl_family_hits: Counter[str] = Counter()
    compact_key_only = 0

    for index, text in enumerate(frozen.strings):
        templates = sorted(citeurl_templates.get(text, []))
        families_citeurl, unmapped = citeurl_families(templates)
        families_current = current_families(text)
        current_probe = current_authority_recognized(text)
        # A template that has no declared family still counts as a CiteURL
        # detection: the arm did recognize something. Only the *family* is
        # unknown, and the receipt says so.
        citeurl_hit = bool(templates)
        for name in templates:
            template_hits[name] += 1
        for name in unmapped:
            unmapped_templates[name] += 1
        for family in families_current:
            current_family_hits[family] += 1
        for family in families_citeurl:
            citeurl_family_hits[family] += 1
        if "cfr" in families_current and cfr_compact_key_only(text):
            compact_key_only += 1

        records.append(
            {
                "string_id": f"auth-{index:05d}",
                "text": text,
                "current_recognized": current_probe,
                "current_extended_recognized": bool(families_current),
                "current_families": families_current,
                "citeurl_recognized": citeurl_hit,
                "citeurl_templates": templates,
                "citeurl_families": families_citeurl,
                "citeurl_unmapped_templates": unmapped,
                "cell": classify_cell(current=current_probe, citeurl=citeurl_hit),
                "cell_extended": classify_cell(current=bool(families_current), citeurl=citeurl_hit),
            }
        )

    table = four_cell_table(records)
    table_extended = four_cell_table([{"cell": record["cell_extended"]} for record in records])

    strings_digest = _write(
        output / "authority-strings.json",
        {
            "schema_version": DETECTION_SCHEMA_VERSION,
            "count": frozen.count,
            "digest": frozen.digest,
            "strings": frozen.strings,
        },
    )
    detection = {
        "schema_version": DETECTION_SCHEMA_VERSION,
        "population": frozen.count,
        "four_cell": table,
        "four_cell_extended": table_extended,
        "current_family_hits": dict(sorted(current_family_hits.items())),
        "citeurl_family_hits": dict(sorted(citeurl_family_hits.items())),
        "citeurl_template_hits": dict(sorted(template_hits.items())),
        "records": records,
    }
    detection_digest = _write(output / "detection.json", detection)

    receipt = {
        "schema_version": DETECTION_SCHEMA_VERSION,
        "determinism": (
            "Rebuilding from the same authority-string digest with the same pinned "
            "versions reproduces every file byte-for-byte. No timestamps, no absolute paths."
        ),
        "scope": "detection evaluation only; no identity change, no wiring change, no regex retired",
        "inputs": {
            "unified_agenda": {"path": _pin_path(source_path), "digest": frozen.source_digest},
            "authority_strings": {
                "digest": frozen.digest,
                "count": frozen.count,
                "column": AUTHORITY_COLUMN,
                "rows_read": frozen.rows_read,
                "values_read": frozen.values_read,
                "malformed_rows": frozen.malformed_rows,
                "empty_rows": frozen.empty_rows,
            },
        },
        "arms": {
            "current": {
                "entry_point": "spicy_regs.ontology.citations.parse_authority_citation",
                "recognized_when": "the result is not a single other/failed citation",
                "note": "the single function the 2026-07-27 exploratory probe compared",
            },
            "current_extended": {
                "entry_points": [
                    "spicy_regs.ontology.citations.parse_authority_citation",
                    "spicy_regs.ontology.citations.parse_cfr_citation",
                    "spicy_regs.ontology.citations.normalize_rin",
                    "spicy_regs.ontology.citations.canonical_frdoc_iri",
                    "spicy_regs.ontology.citations.normalize_docket_reference",
                ],
                "cfr_compact_key_only_strings": compact_key_only,
                "cfr_compact_key_note": (
                    "parse_cfr_citation's compact-key branch reads any bare 'N-M' string as "
                    "title-part; over free authority text that is an over-read, counted here "
                    "rather than silently excluded"
                ),
            },
            "citeurl": {
                "worker": _pin_path(WORKER_PATH),
                "worker_digest": file_sha256(WORKER_PATH) if WORKER_PATH.exists() else None,
                "pin": citeurl_pin,
                "recognized_when": "citeurl.Citator().list_cites(text) returns at least one citation",
                "family_map": dict(sorted(CITEURL_TEMPLATE_FAMILIES.items())),
                "unmapped_templates": dict(sorted(unmapped_templates.items())),
            },
        },
        "counts": {
            "four_cell": table,
            "four_cell_extended": table_extended,
            "current_family_hits": dict(sorted(current_family_hits.items())),
            "citeurl_family_hits": dict(sorted(citeurl_family_hits.items())),
            "citeurl_template_hits": dict(sorted(template_hits.items())),
        },
        "outputs": {
            "authority-strings.json": strings_digest,
            "detection.json": detection_digest,
        },
    }
    _write(output / "detection-receipt.json", receipt)
    return detection


# --------------------------------------------------------------------------
# phase 2: adjudication
# --------------------------------------------------------------------------


class CostCapExceededError(RuntimeError):
    """The projected spend exceeds the declared cap, so nothing was called."""


def project_cost(*, calls: int, input_tokens: int, output_tokens: int, prices: dict[str, float]) -> float:
    """Projected spend, from per-call token estimates and pinned prices."""
    return (calls * input_tokens / 1_000_000) * prices["input"] + (calls * output_tokens / 1_000_000) * prices["output"]


def enforce_cost_cap(*, projected_usd: float, cap_usd: float, calls: int) -> float:
    """Check the projection against the cap before any call is made.

    Returns the headroom. Raising here rather than mid-run is the point: a cap
    checked after spending is not a cap.
    """
    if projected_usd > cap_usd:
        raise CostCapExceededError(
            f"projected ${projected_usd:.2f} for {calls} calls exceeds the ${cap_usd:.2f} cap; "
            f"sample down with --per-stratum and record that you did"
        )
    return cap_usd - projected_usd


def stratified_sample(
    records: list[dict[str, Any]],
    *,
    per_stratum: int | None,
    seed: int,
) -> list[dict[str, Any]]:
    """Draw from the three disagreement cells, seeded and reproducible.

    The agreement cell is never drawn: two systems that agree have nothing to
    adjudicate. ``per_stratum=None`` is a census of the disagreement
    population, which at 620 strings is affordable and strictly better than a
    sample. The result is returned in ``string_id`` order either way, so a
    census and a capped draw are read the same way downstream.
    """
    by_cell: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        cell = str(record["cell"])
        if cell in DISAGREEMENT_CELLS:
            by_cell[cell].append(record)

    drawn: list[dict[str, Any]] = []
    for cell in DISAGREEMENT_CELLS:
        stratum = sorted(by_cell.get(cell, []), key=lambda item: str(item["string_id"]))
        if per_stratum is None or per_stratum >= len(stratum):
            drawn.extend(stratum)
            continue
        rng = random.Random(f"{seed}:{cell}")
        drawn.extend(rng.sample(stratum, per_stratum))
    return sorted(drawn, key=lambda item: str(item["string_id"]))


def build_payload(record: dict[str, Any]) -> dict[str, Any]:
    """What the judge sees: the string, and what each arm claimed about it."""
    return {
        "string_id": record["string_id"],
        "authority_string": record["text"],
        "current_parser_detected": {
            "recognized": bool(record.get("current_extended_recognized", record.get("current_recognized", False))),
            "families": list(record.get("current_families", [])),
        },
        "citeurl_detected": {
            "recognized": bool(record.get("citeurl_recognized", False)),
            "families": list(record.get("citeurl_families", [])),
            "templates": list(record.get("citeurl_templates", [])),
        },
    }


def adjudicate_one(
    model: Any,
    record: dict[str, Any],
    *,
    instructions: str = INSTRUCTIONS,
    schema: dict[str, Any] | None = None,
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
) -> dict[str, Any]:
    """Adjudicate one string, once. k=1, and a failure is a receipt.

    Detection adjudication is a factual question about a short string, not a
    graded relevance judgment, so one call is the measurement. A provider
    failure is written down as a failure — never retried until it agrees,
    which would turn a flaky call into a manufactured consensus.
    """
    schema = schema or RESPONSE_SCHEMA
    payload = build_payload(record)
    request = model.secret_free_request(
        name=SCHEMA_NAME,
        schema=schema,
        instructions=instructions,
        payload=payload,
        max_output_tokens=max_output_tokens,
    )
    out: dict[str, Any] = {
        "string_id": record["string_id"],
        "text": record["text"],
        "cell": record["cell"],
        "request_sha256": sha256_text(canonical_json(request)),
        "model_id": getattr(model, "model_id", None),
        "model_requested": getattr(model, "model", None),
        "status": "adjudicated",
        "verdict": None,
        "response": None,
        "response_sha256": None,
        "call": None,
    }
    try:
        result = model.structured_json(
            name=SCHEMA_NAME,
            schema=schema,
            instructions=instructions,
            payload=payload,
            max_output_tokens=max_output_tokens,
        )
    except Exception as error:  # a provider failure is data, not a crash
        out.update(
            {
                "status": "failed",
                "error_code": type(error).__name__,
                # ``call`` is the only receipt-safe attribute on this error: the
                # message and its cause chain may carry provider text.
                "call": getattr(error, "call", None),
            }
        )
        return out

    response = dict(result.output)
    out["response"] = response
    out["response_sha256"] = sha256_text(canonical_json(response))
    out["call"] = dict(result.call)
    if str(response.get("string_id")) != str(record["string_id"]):
        # A model that answered about a different string has not judged this
        # one. Recording it as a mismatch keeps a mislabeled answer out of the
        # rollup without discarding the evidence that it happened.
        out["status"] = "id_mismatch"
        return out
    out["verdict"] = response.get("verdict")
    return out


def realized_cost(records: list[dict[str, Any]], *, prices: dict[str, float]) -> dict[str, Any]:
    """Spend actually incurred, from the token counts the provider reported.

    Failed calls are included. A call that failed after the prompt went out
    still burned input tokens, and a spend report that hides them understates
    what the run cost.
    """
    input_tokens = 0
    output_tokens = 0
    for record in records:
        call = record.get("call") or {}
        input_tokens += int(call.get("input_tokens") or 0)
        output_tokens += int(call.get("output_tokens") or 0)
    usd = (input_tokens / 1_000_000) * prices["input"] + (output_tokens / 1_000_000) * prices["output"]
    return {
        "calls": len(records),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "usd": usd,
        "prices_usd_per_mtok": dict(prices),
        "prices_are_pinned_estimates": True,
    }


def verdict_by_family(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Roll adjudicated ground truth up per citation family.

    Only adjudicated items count. Failures and id-mismatches are counted under
    ``_unadjudicated`` rather than being dropped, so the denominator stays
    honest.
    """
    tally: dict[str, Counter[str]] = defaultdict(Counter)
    unadjudicated = 0
    for record in records:
        if record.get("status") != "adjudicated" or not record.get("verdict"):
            unadjudicated += 1
            continue
        response = record.get("response") or {}
        citations = response.get("citations") or []
        families = sorted({str(item.get("family")) for item in citations if item.get("family")})
        if not families:
            families = ["_no_citation"]
        for family in families:
            tally[family][str(record["verdict"])] += 1
    out: dict[str, Any] = {family: dict(sorted(counts.items())) for family, counts in sorted(tally.items())}
    out["_unadjudicated"] = unadjudicated
    return out


def verdict_by_cell(records: list[dict[str, Any]]) -> dict[str, Any]:
    tally: dict[str, Counter[str]] = defaultdict(Counter)
    for record in records:
        status = record.get("status")
        key = str(record.get("verdict")) if status == "adjudicated" and record.get("verdict") else f"_{status}"
        tally[str(record.get("cell"))][key] += 1
    return {cell: dict(sorted(counts.items())) for cell, counts in sorted(tally.items())}


# --------------------------------------------------------------------------
# commands
# --------------------------------------------------------------------------


def command_detect(args: argparse.Namespace) -> int:
    frozen = extract_authority_strings(Path(args.unified_agenda))
    python = (
        Path(args.citeurl_python)
        if args.citeurl_python
        else ensure_citeurl_venv(Path(args.citeurl_venv), rebuild=args.rebuild_venv)
    )
    arm = run_citeurl_arm(frozen.strings, python)
    detection = build_detection_artifact(
        frozen=frozen,
        citeurl_templates=arm["templates"],
        citeurl_pin={**arm["pin"], "undeclared_import": CITEURL_UNDECLARED_IMPORT_NOTE},
        source_path=Path(args.unified_agenda),
        output=Path(args.output),
    )
    print(canonical_json({"population": detection["population"], **detection["four_cell"]}))
    print(canonical_json({"extended": detection["four_cell_extended"]}))
    return 0


def command_adjudicate(args: argparse.Namespace) -> int:
    from dotenv import load_dotenv  # noqa: PLC0415

    load_dotenv()

    output = Path(args.output)
    detection = json.loads((output / "detection.json").read_text())
    drawn = stratified_sample(detection["records"], per_stratum=args.per_stratum, seed=args.seed)

    projected = project_cost(
        calls=len(drawn),
        input_tokens=ESTIMATED_INPUT_TOKENS_PER_CALL,
        output_tokens=ESTIMATED_OUTPUT_TOKENS_PER_CALL,
        prices=PRICES_USD_PER_MTOK,
    )
    headroom = enforce_cost_cap(projected_usd=projected, cap_usd=args.cost_cap_usd, calls=len(drawn))
    print(f"projected ${projected:.3f} for {len(drawn)} calls, headroom ${headroom:.3f}", file=sys.stderr)

    from spicy_regs.docpipeline.adapters.openai_compatible import (  # noqa: PLC0415
        OpenAICompatibleStructuredTextModel,
    )

    model = OpenAICompatibleStructuredTextModel.from_environment(
        provider=args.provider,
        model=args.model,
        reasoning_effort=args.reasoning_effort,
    )

    records: list[dict[str, Any]] = []
    lines = output / "adjudication.jsonl"
    lines.parent.mkdir(parents=True, exist_ok=True)
    with lines.open("w", encoding="utf-8") as handle:
        for index, item in enumerate(drawn, start=1):
            record = adjudicate_one(model, item, max_output_tokens=args.max_output_tokens)
            text = canonical_json(record)
            assert_secret_free(text)
            handle.write(text + "\n")
            handle.flush()
            records.append(record)
            if index % 25 == 0 or index == len(drawn):
                spend = realized_cost(records, prices=PRICES_USD_PER_MTOK)
                print(f"  {index}/{len(drawn)}  ${spend['usd']:.3f}", file=sys.stderr)
                if spend["usd"] > args.cost_cap_usd:
                    print("cost cap reached mid-run; stopping", file=sys.stderr)
                    break

    statuses = Counter(str(record["status"]) for record in records)
    spend = realized_cost(records, prices=PRICES_USD_PER_MTOK)
    receipt = {
        "schema_version": ADJUDICATION_SCHEMA_VERSION,
        "determinism": (
            "NOT deterministic. This artifact records provider calls; a rebuild will not "
            "reproduce it byte-for-byte. Every call carries its own request/response digest."
        ),
        "detection_pin": {
            "authority_strings_digest": json.loads((output / "authority-strings.json").read_text())["digest"],
            "detection_digest": sha256_text((output / "detection.json").read_text()),
        },
        "draw": {
            "rule": "stratified over the three disagreement cells, seeded",
            "seed": args.seed,
            "per_stratum": args.per_stratum,
            "is_census": args.per_stratum is None,
            "drawn": len(drawn),
            "attempted": len(records),
            "by_cell": dict(sorted(Counter(str(item["cell"]) for item in drawn).items())),
        },
        "judge": {
            "provider": getattr(model, "provider", None),
            "model_id": getattr(model, "model_id", None),
            "model_requested": getattr(model, "model", None),
            "base_url_host": getattr(model, "base_url_host", None),
            "structured_mode": getattr(model, "structured_mode", None),
            "reasoning_effort": getattr(model, "reasoning_effort", None),
            "k": 1,
            "k_rationale": "detection adjudication is a factual question, not relevance grading",
            "instructions_sha256": sha256_text(INSTRUCTIONS),
            "schema_sha256": sha256_text(canonical_json(RESPONSE_SCHEMA)),
            "max_output_tokens": args.max_output_tokens,
        },
        "statuses": dict(sorted(statuses.items())),
        "spend": spend,
        "cost_cap_usd": args.cost_cap_usd,
        "projected_usd": projected,
        "outputs": {"adjudication.jsonl": sha256_text(lines.read_text())},
    }
    _write(output / "adjudication-receipt.json", receipt)
    print(canonical_json({"statuses": dict(statuses), "usd": round(spend["usd"], 4)}))
    return 0


def command_verdict(args: argparse.Namespace) -> int:
    output = Path(args.output)
    records = [json.loads(line) for line in (output / "adjudication.jsonl").read_text().splitlines() if line.strip()]
    detection = json.loads((output / "detection.json").read_text())

    # The comparison a recommendation rests on: project-owned *free-text*
    # grammars against CiteURL. Re-cut from what detection already published,
    # so it cannot disagree with the sealed artifact.
    cells = text_grammar_cells(detection["records"])
    text_table = four_cell_table([{"cell": cell} for cell in cells.values()])
    citeurl_by_id = {str(item["string_id"]): bool(item["citeurl_recognized"]) for item in detection["records"]}
    for record in records:
        string_id = str(record["string_id"])
        record["cell_text_grammars"] = cells.get(string_id)
        record["current_text_recognized"] = cells.get(string_id) in {"both", "current_only"}
        record["citeurl_recognized"] = citeurl_by_id.get(string_id, False)

    verdict = {
        "schema_version": VERDICT_SCHEMA_VERSION,
        "scope": "detection evaluation only; nothing here retires a regex or changes identity",
        "detection": {
            "population": detection["population"],
            "four_cell": detection["four_cell"],
            "four_cell_extended": detection["four_cell_extended"],
            "four_cell_text_grammars": text_table,
            "text_grammar_families": sorted(TEXT_GRAMMAR_FAMILIES),
        },
        "adjudicated": sum(1 for record in records if record.get("status") == "adjudicated"),
        "by_family": verdict_by_family(records),
        "by_cell": verdict_by_cell(records),
        "by_cell_text_grammars": verdict_by_cell(
            [{**record, "cell": record.get("cell_text_grammars")} for record in records]
        ),
        "false_positives": false_positive_counts(records),
        "spend": realized_cost(records, prices=PRICES_USD_PER_MTOK),
    }
    _write(output / "verdict.json", verdict)
    print(canonical_json(verdict["by_cell_text_grammars"]))
    print(canonical_json(verdict["false_positives"]))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    detect = sub.add_parser("detect", help="deterministic: freeze strings, run both arms, classify")
    detect.add_argument("--unified-agenda", required=True, help="path to unified_agenda.parquet")
    detect.add_argument("--output", required=True)
    detect.add_argument("--citeurl-venv", default="/tmp/bakeoff-venv")
    detect.add_argument("--citeurl-python", default=None, help="reuse an existing scratch interpreter")
    detect.add_argument("--rebuild-venv", action="store_true")
    detect.set_defaults(handler=command_detect)

    adjudicate = sub.add_parser("adjudicate", help="frontier-model first pass over the disagreement strata")
    adjudicate.add_argument("--output", required=True)
    adjudicate.add_argument("--provider", default=DEFAULT_PROVIDER)
    adjudicate.add_argument("--model", default=DEFAULT_MODEL)
    adjudicate.add_argument("--seed", type=int, default=DEFAULT_SEED)
    adjudicate.add_argument(
        "--per-stratum",
        type=int,
        default=None,
        help="cap per disagreement cell; omit for a census of all disagreements",
    )
    adjudicate.add_argument("--cost-cap-usd", type=float, default=DEFAULT_COST_CAP_USD)
    adjudicate.add_argument("--max-output-tokens", type=int, default=DEFAULT_MAX_OUTPUT_TOKENS)
    adjudicate.add_argument(
        "--reasoning-effort",
        default=DEFAULT_REASONING_EFFORT,
        help="pinned in the receipt; thinking tokens are billed as output",
    )
    adjudicate.set_defaults(handler=command_adjudicate)

    verdict = sub.add_parser("verdict", help="roll adjudicated ground truth up per family")
    verdict.add_argument("--output", required=True)
    verdict.set_defaults(handler=command_verdict)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
