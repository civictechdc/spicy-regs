#!/usr/bin/env python3
"""Build the sealed search-holdout exam DocumentRelease over the drawn matters.

The 2026-08-01 search-holdout draw sealed 240 matters
(``docs/evidence/search-holdout-draw-2026-08-01.md``). This tool builds the
*exam corpus* for labeling: one immutable :class:`DocumentRelease` covering
exactly those matters' Federal Register documents, through the existing
release machinery (``src/spicy_regs/document_release.py``, the a388cd0
immutable-release path) — never a parallel format.

Protocol position: the candidate configuration under judgment was frozen and
committed first (spicysearch ``evaluation/holdout-labeling/config-freeze.md``).
Content exposure past that freeze is per-protocol; the sealed text this tool
publishes feeds judging inputs and receipts only.

What the tool guarantees, enforced rather than promised:

1. **Exact coverage.** The release contains one document version per unique
   ``fr_documents`` member across the drawn matters — no more, no fewer.
   A drawn document number missing from the pinned Federal Register table
   fails the whole build.
2. **Deterministic sealing.** Content derives only from the pinned parquet
   values and declared constants, so rebuilding produces byte-identical
   fixture and release digests.
3. **Fail closed.** Malformed source facts (non-ISO dates, empty titles,
   unparseable JSON columns) are errors, never silently repaired or dropped.

Run::

    uv run python tools/build_search_holdout_exam_release.py \\
        --output output/search-holdout-exam-2026-08-01

``--verify`` rebuilds against an existing output directory and compares
digests instead of writing.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

TOOLS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TOOLS_DIR.parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spicy_regs.document_release import (  # noqa: E402
    DEFAULT_RULESPEC_CORE_PATH,
    FIXTURE_FORMAT_VERSION,
    build_document_release,
    canonical_digest,
    canonical_json,
    write_document_release,
)

EXAM_SCHEMA_VERSION = "search-holdout-exam-release-v1"
EXAM_DATASET_ID = "search-holdout-matters-2026-08-01-v1"
EXAM_FIXTURE_ID = "urn:spicyregs:source-fixture:search-holdout-exam-2026-08-01"
EXAM_ACQUISITION_RELEASE_REF = "urn:spicyregs:acquisition-release:search-holdout-exam-2026-08-01"
EXAM_OBSERVED_AT = "2026-08-01T22:00:00Z"
EXAM_RELEASED_AT = "2026-08-01T22:00:00Z"

#: File-byte digest of the sealed draw manifest, pinned in
#: ``docs/evidence/search-holdout-draw-2026-08-01.md`` and in the spicysearch
#: config-freeze record.
SEALED_MANIFEST_SHA256 = "b4737fb07f0d5e70652286de8d1e61aa7b3b92d040aac1321e9f3b1fbfcadc6e"

DEFAULT_MANIFEST_PATH = REPO_ROOT / "output" / "search-holdout-draw-2026-08-01" / "sealed-manifest.json"
DEFAULT_FEDERAL_REGISTER_PATH = REPO_ROOT / "output" / "rin-ontology-revision-candidate" / "federal_register.parquet"

#: Columns the exam reads from the Federal Register table. Everything the
#: sealed content carries must come from this declared set.
FEDERAL_REGISTER_COLUMNS: tuple[str, ...] = (
    "document_number",
    "title",
    "abstract",
    "document_type",
    "publication_date",
    "effective_on",
    "comments_close_on",
    "agencies_json",
    "docket_ids_json",
    "regulation_id_numbers_json",
    "topics_json",
    "html_url",
)


class ExamReleaseError(ValueError):
    """The exam corpus build failed closed."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_sealed_manifest(path: Path, *, expected_sha256: str) -> dict[str, Any]:
    """Read the sealed draw manifest, verifying its pinned file digest."""

    actual = _sha256_file(path)
    if actual != expected_sha256:
        raise ExamReleaseError(
            f"sealed manifest digest differs: expected {expected_sha256}, found {actual} at {path}"
        )
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or not isinstance(manifest.get("matters"), list):
        raise ExamReleaseError("sealed manifest must be an object carrying a matters array")
    return manifest


def drawn_fr_documents(manifest: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the sorted unique FR document numbers across drawn matters."""

    numbers: set[str] = set()
    for matter in manifest["matters"]:
        members = matter.get("fr_documents", [])
        if not isinstance(members, list):
            raise ExamReleaseError("matter fr_documents must be an array")
        for number in members:
            if not isinstance(number, str) or not number:
                raise ExamReleaseError("fr_documents members must be non-empty strings")
            numbers.add(number)
    return tuple(sorted(numbers))


def _require_iso_date(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ExamReleaseError(f"{label} must be a non-empty ISO date string")
    try:
        parsed = _dt.date.fromisoformat(value)
    except ValueError as error:
        raise ExamReleaseError(f"{label} is not an exact ISO date: {value!r}") from error
    if parsed.isoformat() != value:
        raise ExamReleaseError(f"{label} is not an exact ISO date: {value!r}")
    return value


def _optional_iso_date(value: Any, *, label: str) -> str | None:
    if value is None or value == "":
        return None
    return _require_iso_date(value, label=label)


def _json_string_list(value: Any, *, label: str) -> list[str]:
    if value is None or value == "":
        return []
    if not isinstance(value, str):
        raise ExamReleaseError(f"{label} must be a JSON-encoded string column")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise ExamReleaseError(f"{label} is not valid JSON") from error
    if parsed is None:
        return []
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise ExamReleaseError(f"{label} must decode to an array of strings")
    return parsed


def _agency_names(value: Any, *, label: str) -> list[str]:
    if value is None or value == "":
        return []
    if not isinstance(value, str):
        raise ExamReleaseError(f"{label} must be a JSON-encoded string column")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise ExamReleaseError(f"{label} is not valid JSON") from error
    if parsed is None:
        return []
    if not isinstance(parsed, list):
        raise ExamReleaseError(f"{label} must decode to an array")
    names: list[str] = []
    for entry in parsed:
        if not isinstance(entry, Mapping):
            raise ExamReleaseError(f"{label} entries must be objects")
        name = entry.get("name") or entry.get("raw_name")
        if isinstance(name, str) and name and name not in names:
            names.append(name)
    return names


def _exam_record(number: str, row: Mapping[str, Any]) -> dict[str, Any]:
    title = row.get("title")
    if not isinstance(title, str) or not title.strip():
        raise ExamReleaseError(f"document {number} title must be a non-empty string")
    document_type = row.get("document_type")
    if not isinstance(document_type, str) or not document_type:
        raise ExamReleaseError(f"document {number} document_type must be a non-empty string")
    publication_date = _require_iso_date(row.get("publication_date"), label=f"document {number} publication_date")
    html_url = row.get("html_url")
    if not isinstance(html_url, str) or not html_url:
        raise ExamReleaseError(f"document {number} html_url must be a non-empty string")

    abstract = row.get("abstract")
    if abstract is not None and not isinstance(abstract, str):
        raise ExamReleaseError(f"document {number} abstract must be a string when present")
    if isinstance(abstract, str) and not abstract.strip():
        abstract = None

    text = title if abstract is None else f"{title}\n\n{abstract}"
    passages: list[dict[str, Any]] = [
        {"representation_path": "text", "start": 0, "end": len(title), "expected_text": title}
    ]
    if abstract is not None:
        start = len(title) + 2
        passages.append(
            {"representation_path": "text", "start": start, "end": len(text), "expected_text": abstract}
        )

    content: dict[str, Any] = {
        "document_number": number,
        "document_type": document_type,
        "html_url": html_url,
        "publication_date": publication_date,
        "text": text,
        "title": title,
    }
    if abstract is not None:
        content["abstract"] = abstract
    agencies = _agency_names(row.get("agencies_json"), label=f"document {number} agencies_json")
    if agencies:
        content["agencies"] = agencies
    for column, field in (
        ("docket_ids_json", "docket_ids"),
        ("regulation_id_numbers_json", "regulation_id_numbers"),
        ("topics_json", "topics"),
    ):
        values = _json_string_list(row.get(column), label=f"document {number} {column}")
        if values:
            content[field] = values
    for column in ("effective_on", "comments_close_on"):
        value = _optional_iso_date(row.get(column), label=f"document {number} {column}")
        if value is not None:
            content[column] = value

    return {
        "key": f"fr-{number}",
        "publisher": "federal-register",
        "collection": "documents",
        "source_record_id": number,
        "source_url": html_url,
        "content": content,
        "document": {
            "content_path": "text",
            "document_type": document_type,
            "source_issued_version_id": number,
        },
        "captures": [
            {
                "observed_at": EXAM_OBSERVED_AT,
                "retrieval_receipt_ref": f"urn:spicyregs:retrieval-receipt:search-holdout-exam-2026-08-01:{number}",
            }
        ],
        "representations": [
            {
                "source_native_path": "text",
                "evidence_grade": "parser-derived",
                "method": "title-abstract-concatenation",
                "method_version": "1",
                "method_config": {"fields": ["title", "abstract"], "separator": "\n\n"},
            }
        ],
        "passages": passages,
    }


def build_exam_source_fixture(
    manifest: Mapping[str, Any],
    rows_by_number: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Build the deterministic source fixture covering the drawn documents."""

    numbers = drawn_fr_documents(manifest)
    if not numbers:
        raise ExamReleaseError("the sealed draw names no Federal Register documents")
    missing = [number for number in numbers if number not in rows_by_number]
    if missing:
        raise ExamReleaseError(
            f"{len(missing)} drawn documents are missing from the Federal Register table: "
            + ", ".join(missing[:10])
        )
    records = [_exam_record(number, rows_by_number[number]) for number in numbers]
    body = {
        "acquisition_release_ref": EXAM_ACQUISITION_RELEASE_REF,
        "coverage_gaps": [],
        "fixture_id": EXAM_FIXTURE_ID,
        "format_version": FIXTURE_FORMAT_VERSION,
        "links": [],
        "records": records,
        "released_at": EXAM_RELEASED_AT,
        "requested_sources": ["federal-register:documents"],
    }
    body["fixture_digest"] = canonical_digest({key: value for key, value in body.items() if key != "fixture_digest"})
    return body


def build_release_from_fixture(fixture: Mapping[str, Any]) -> dict[str, Any]:
    """Seal the fixture through the immutable release machinery."""

    with tempfile.TemporaryDirectory(prefix="search-holdout-exam-") as scratch:
        fixture_path = Path(scratch) / "source-fixture.json"
        fixture_path.write_text(canonical_json(fixture), encoding="utf-8")
        return build_document_release(fixture_path=fixture_path, rulespec_core_path=DEFAULT_RULESPEC_CORE_PATH)


def _read_federal_register_rows(path: Path, numbers: Sequence[str]) -> dict[str, dict[str, Any]]:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    table = pq.read_table(path, columns=list(FEDERAL_REGISTER_COLUMNS))
    mask = pc.is_in(table["document_number"], value_set=pa.array(list(numbers)))
    rows = table.filter(mask).to_pylist()
    by_number: dict[str, dict[str, Any]] = {}
    for row in rows:
        number = row["document_number"]
        if number in by_number:
            raise ExamReleaseError(f"Federal Register table repeats document number {number}")
        for key, value in list(row.items()):
            if isinstance(value, (_dt.date, _dt.datetime)):
                row[key] = value.date().isoformat() if isinstance(value, _dt.datetime) else value.isoformat()
        by_number[number] = row
    return by_number


def _matter_coverage(manifest: Mapping[str, Any]) -> dict[str, int]:
    with_documents = sum(1 for matter in manifest["matters"] if matter.get("fr_documents"))
    return {
        "matters_total": len(manifest["matters"]),
        "matters_with_fr_documents": with_documents,
        "matters_without_fr_documents": len(manifest["matters"]) - with_documents,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the sealed search-holdout exam DocumentRelease")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--manifest-sha256", default=SEALED_MANIFEST_SHA256)
    parser.add_argument("--federal-register", type=Path, default=DEFAULT_FEDERAL_REGISTER_PATH)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--verify",
        action="store_true",
        help="rebuild and compare digests against an existing output directory instead of writing",
    )
    args = parser.parse_args(argv)

    manifest = load_sealed_manifest(args.manifest, expected_sha256=args.manifest_sha256)
    numbers = drawn_fr_documents(manifest)
    rows = _read_federal_register_rows(args.federal_register, numbers)
    fixture = build_exam_source_fixture(manifest, rows)
    release = build_release_from_fixture(fixture)

    receipt = {
        "schema_version": EXAM_SCHEMA_VERSION,
        "dataset_id": EXAM_DATASET_ID,
        "inputs": {
            "sealed_manifest": {"path": str(args.manifest), "sha256": args.manifest_sha256},
            "federal_register_parquet": {
                "path": str(args.federal_register),
                "sha256": _sha256_file(args.federal_register),
            },
            "rulespec_core_release_path": str(DEFAULT_RULESPEC_CORE_PATH),
        },
        "constants": {
            "observed_at": EXAM_OBSERVED_AT,
            "released_at": EXAM_RELEASED_AT,
            "fixture_id": EXAM_FIXTURE_ID,
            "acquisition_release_ref": EXAM_ACQUISITION_RELEASE_REF,
            "columns": list(FEDERAL_REGISTER_COLUMNS),
        },
        "coverage": {
            **_matter_coverage(manifest),
            "unique_fr_documents": len(numbers),
            "document_versions": len(release["document_versions"]),
            "structural_passages": len(release["structural_passages"]),
        },
        "fixture_digest": fixture["fixture_digest"],
        "release_digest": release["release_digest"],
        "release_id": release["release_id"],
    }

    if args.verify:
        existing_receipt = json.loads((args.output / "receipt.json").read_text(encoding="utf-8"))
        for key in ("fixture_digest", "release_digest", "release_id"):
            if existing_receipt.get(key) != receipt[key]:
                raise ExamReleaseError(
                    f"verification failed: {key} differs (existing {existing_receipt.get(key)!r}, "
                    f"rebuilt {receipt[key]!r})"
                )
        print(json.dumps({"verified": True, **{k: receipt[k] for k in ('release_id', 'release_digest')}}, indent=2))
        return 0

    if args.output.exists():
        raise ExamReleaseError(f"output directory already exists (immutable): {args.output}")
    args.output.mkdir(parents=True)
    (args.output / "source-fixture.json").write_text(canonical_json(fixture), encoding="utf-8")
    write_document_release(args.output / "document-release.json", release)
    receipt["artifacts"] = {
        "source-fixture.json": "sha256:" + _sha256_file(args.output / "source-fixture.json"),
        "document-release.json": "sha256:" + _sha256_file(args.output / "document-release.json"),
    }
    (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"release_id": receipt["release_id"], "coverage": receipt["coverage"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
