"""Build and validate a bounded all-profile ontology evaluation snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from spicy_regs.ontology.common import canonical_json
from spicy_regs.ontology.subjects import (
    EXCLUDED_SOURCE_TABLES,
    SUBJECT_PROFILES,
    SubjectProfile,
    balanced_artifact_batch,
    iter_artifacts,
)

FORMAT_VERSION = 1
REGULATORY_EVAL_INPUTS = (
    "dockets",
    "documents",
    "federal_register",
    "unified_agenda",
    "fr_docket_links",
)
REGULATORY_PROFILE_SOURCES = frozenset(REGULATORY_EVAL_INPUTS[:-1])


def _sql_string(value: str | Path) -> str:
    return str(value).replace("'", "''")


def _quoted(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Invalid profile-evaluation JSON: {path}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"Profile-evaluation JSON must be an object: {path}")
    return value


def _profile_selection_sql(
    profile: SubjectProfile,
    *,
    source: Path,
    rows_per_profile: int,
) -> str:
    source_columns = set(
        pq.ParquetFile(source).schema_arrow.names
    )
    present = " AND ".join(
        f"nullif(trim(cast({_quoted(column)} AS VARCHAR)), '') IS NOT NULL"
        for column in profile.id_columns
    )
    text = ", ".join(
        f"nullif(trim(cast({_quoted(column)} AS VARCHAR)), '')"
        for column in profile.text_columns
        if column in source_columns
    )
    if not text:
        raise RuntimeError(
            f"{profile.profile_id} source has no declared text columns"
        )
    identity = " || '|' || ".join(
        f"cast({_quoted(column)} AS VARCHAR)"
        for column in profile.id_columns
    )
    return f"""
        SELECT *
        FROM read_parquet('{_sql_string(source)}')
        WHERE {present}
          AND coalesce({text}) IS NOT NULL
        ORDER BY md5({identity} || '{profile.profile_id}')
        LIMIT {rows_per_profile}
    """


def _artifact_records(output_dir: Path) -> dict[str, dict[str, int | str]]:
    return {
        path.name: {
            "rows": pq.ParquetFile(path).metadata.num_rows,
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in sorted(output_dir.glob("*.parquet"))
    }


def _profile_state(
    output_dir: Path,
    *,
    rows_per_profile: int,
) -> tuple[dict[str, int], dict[str, int]]:
    required = {profile.source_table for profile in SUBJECT_PROFILES}
    artifacts = list(
        iter_artifacts(
            output_dir,
            required_source_tables=required,
        )
    )
    subject_counts = Counter(
        artifact.profile_id for artifact in artifacts
    )
    batch = balanced_artifact_batch(
        artifacts,
        rows_per_profile * len(SUBJECT_PROFILES),
    )
    batch_counts = Counter(artifact.profile_id for artifact in batch)
    return (
        dict(sorted(subject_counts.items())),
        dict(sorted(batch_counts.items())),
    )


def validate_profile_evaluation(
    output_dir: Path,
    *,
    rows_per_profile: int | None = None,
) -> dict[str, Any]:
    """Validate source coverage, batch balance, and artifact hashes."""
    manifest = _read_json(output_dir / "profile-evaluation-manifest.json")
    expected_rows = int(
        rows_per_profile
        if rows_per_profile is not None
        else manifest.get("rows_per_profile") or 0
    )
    artifacts = _artifact_records(output_dir)
    evaluation_id = "profile_eval_" + hashlib.sha256(
        canonical_json(
            {
                name: record["sha256"]
                for name, record in sorted(artifacts.items())
            }
        ).encode()
    ).hexdigest()[:24]
    subject_counts, batch_counts = _profile_state(
        output_dir,
        rows_per_profile=expected_rows,
    )
    expected_profiles = {profile.profile_id for profile in SUBJECT_PROFILES}
    failures: list[str] = []
    if manifest.get("format_version") != FORMAT_VERSION:
        failures.append("manifest format version does not match")
    if manifest.get("evaluation_id") != evaluation_id:
        failures.append("evaluation id does not match current artifacts")
    if set(subject_counts) != expected_profiles:
        failures.append("not every taggable subject profile has eligible rows")
    if any(
        batch_counts.get(profile_id) != expected_rows
        for profile_id in expected_profiles
    ):
        failures.append("generation batch is not balanced across every profile")
    if artifacts != manifest.get("artifacts"):
        failures.append("artifact hashes or row counts differ from the manifest")
    return {
        "format_version": FORMAT_VERSION,
        "status": "pass" if not failures else "fail",
        "evaluation_id": evaluation_id,
        "source_corpus_id": manifest.get("source_corpus_id"),
        "rows_per_profile": expected_rows,
        "profile_count": len(expected_profiles),
        "subject_rows": sum(subject_counts.values()),
        "subject_counts_by_profile": subject_counts,
        "generation_batch_rows": sum(batch_counts.values()),
        "generation_batch_counts_by_profile": batch_counts,
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
        "excluded_source_tables": EXCLUDED_SOURCE_TABLES,
        "failures": failures,
    }


def build_profile_evaluation(
    corpus_dir: Path,
    output_dir: Path,
    *,
    rows_per_profile: int = 3,
) -> dict[str, Any]:
    """Create an immutable, runnable evaluation snapshot for every profile."""
    if rows_per_profile <= 0:
        raise ValueError("rows_per_profile must be positive")
    if output_dir.exists():
        raise FileExistsError(
            f"Refusing to replace existing profile evaluation: {output_dir}"
        )
    corpus_receipt = _read_json(corpus_dir / "corpus-receipt.json")
    if corpus_receipt.get("status") != "pass":
        raise RuntimeError("Source mixed-data corpus receipt did not pass")
    regulatory_dir = corpus_dir / "openai-eval-inputs"
    missing_regulatory = [
        name
        for name in REGULATORY_EVAL_INPUTS
        if not (regulatory_dir / f"{name}.parquet").exists()
    ]
    if missing_regulatory:
        raise FileNotFoundError(
            "Regulatory evaluation inputs are missing: "
            + ", ".join(missing_regulatory)
        )

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(
        tempfile.mkdtemp(
            prefix=f".{output_dir.name}.",
            dir=output_dir.parent,
        )
    )
    con = duckdb.connect()
    try:
        for name in REGULATORY_EVAL_INPUTS:
            shutil.copy2(
                regulatory_dir / f"{name}.parquet",
                temp_dir / f"{name}.parquet",
            )
        for profile in SUBJECT_PROFILES:
            if profile.source_table in REGULATORY_PROFILE_SOURCES:
                continue
            source = corpus_dir / f"{profile.source_table}.parquet"
            if not source.exists():
                raise FileNotFoundError(
                    f"Profile source is missing: {source}"
                )
            target = temp_dir / source.name
            query = _profile_selection_sql(
                profile,
                source=source,
                rows_per_profile=rows_per_profile,
            )
            con.execute(
                f"""
                COPY ({query})
                TO '{_sql_string(target)}'
                (FORMAT PARQUET, COMPRESSION ZSTD)
                """
            )
            selected = pq.ParquetFile(target).metadata.num_rows
            if selected < rows_per_profile:
                raise RuntimeError(
                    f"{profile.profile_id} supplied {selected} eligible rows; "
                    f"expected {rows_per_profile}"
                )

        artifacts = _artifact_records(temp_dir)
        evaluation_id = "profile_eval_" + hashlib.sha256(
            canonical_json(
                {
                    name: record["sha256"]
                    for name, record in sorted(artifacts.items())
                }
            ).encode()
        ).hexdigest()[:24]
        subject_counts, batch_counts = _profile_state(
            temp_dir,
            rows_per_profile=rows_per_profile,
        )
        manifest = {
            "format_version": FORMAT_VERSION,
            "evaluation_id": evaluation_id,
            "generated_at": (
                datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat()
            ),
            "source_corpus_id": corpus_receipt.get("dataset_id"),
            "rows_per_profile": rows_per_profile,
            "purpose": (
                "Bounded real-data OpenAI evaluation input spanning every "
                "taggable ontology subject profile."
            ),
            "profiles": [
                {
                    "profile_id": profile.profile_id,
                    "source_table": profile.source_table,
                    "subject_type": profile.subject_type,
                    "allowed_schemes": list(profile.allowed_schemes),
                    "eligible_rows": subject_counts.get(
                        profile.profile_id,
                        0,
                    ),
                    "generation_batch_rows": batch_counts.get(
                        profile.profile_id,
                        0,
                    ),
                }
                for profile in SUBJECT_PROFILES
            ],
            "excluded_source_tables": EXCLUDED_SOURCE_TABLES,
            "artifacts": artifacts,
        }
        (temp_dir / "profile-evaluation-manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        receipt = validate_profile_evaluation(temp_dir)
        (temp_dir / "profile-evaluation-receipt.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if receipt["status"] != "pass":
            raise RuntimeError(
                "Profile evaluation validation failed: "
                + "; ".join(receipt["failures"])
            )
        temp_dir.replace(output_dir)
        return receipt
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("corpus_dir", type=Path)
    build.add_argument("output_dir", type=Path)
    build.add_argument("--rows-per-profile", type=int, default=3)
    validate = subparsers.add_parser("validate")
    validate.add_argument("output_dir", type=Path)
    args = parser.parse_args()
    if args.command == "build":
        receipt = build_profile_evaluation(
            args.corpus_dir,
            args.output_dir,
            rows_per_profile=args.rows_per_profile,
        )
    else:
        receipt = validate_profile_evaluation(args.output_dir)
    print(json.dumps(receipt, indent=2, sort_keys=True))
    if receipt["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
