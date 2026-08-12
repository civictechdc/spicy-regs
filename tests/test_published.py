"""Public materialized-dataset pointer resolution tests."""

from __future__ import annotations

import pytest

from spicy_regs.published import (
    MATERIALIZED_TABLES,
    SUPPORTED_FORMAT_VERSIONS,
    resolve_materialized_table_urls,
)


def _snapshot(snapshot_id: str, *, format_version: int, artifacts: dict) -> dict:
    """One published snapshot, keyed the way the resolver actually reaches it.

    Both documents live under ``materialized/ontology/snapshots/<id>/``, which
    is the layout the resolver walks: pointer to manifest key, manifest key to
    per-table objects, every hop checked against that prefix.
    """
    prefix = f"materialized/ontology/snapshots/{snapshot_id}/"
    return {
        "https://r2.example/materialized/ontology/latest.json": {
            "format_version": format_version,
            "dataset": "ontology",
            "snapshot_id": snapshot_id,
            "manifest_key": f"{prefix}manifest.json",
        },
        f"https://r2.example/{prefix}manifest.json": {
            "format_version": format_version,
            "dataset": "ontology",
            "snapshot_id": snapshot_id,
            "artifacts": artifacts,
        },
    }


def test_resolves_every_table_from_one_snapshot_manifest() -> None:
    snapshot_id = "snapshot_abc"
    pointer_url = "https://r2.example/materialized/ontology/latest.json"
    manifest_key = f"materialized/ontology/snapshots/{snapshot_id}/manifest.json"
    manifest_url = f"https://r2.example/{manifest_key}"
    documents = {
        pointer_url: {
            "format_version": 1,
            "dataset": "ontology",
            "snapshot_id": snapshot_id,
            "manifest_key": manifest_key,
        },
        manifest_url: {
            "format_version": 1,
            "dataset": "ontology",
            "snapshot_id": snapshot_id,
            "artifacts": {
                f"{table}.parquet": {"remote_key": (f"materialized/ontology/snapshots/{snapshot_id}/{table}.parquet")}
                for table in MATERIALIZED_TABLES
            },
        },
    }

    urls = resolve_materialized_table_urls(
        "https://r2.example",
        fetch_json=documents.__getitem__,
    )

    assert set(urls) == set(MATERIALIZED_TABLES)
    assert all(f"/snapshots/{snapshot_id}/" in url for url in urls.values())


def test_rejects_manifest_artifact_outside_snapshot_prefix() -> None:
    snapshot_id = "snapshot_abc"

    def fetch(url: str) -> dict:
        if url.endswith("/latest.json"):
            return {
                "format_version": 1,
                "dataset": "ontology",
                "snapshot_id": snapshot_id,
                "manifest_key": (f"materialized/ontology/snapshots/{snapshot_id}/manifest.json"),
            }
        return {
            "format_version": 1,
            "dataset": "ontology",
            "snapshot_id": snapshot_id,
            "artifacts": {
                f"{table}.parquet": {
                    "remote_key": (
                        "../escape.parquet"
                        if table == "proceedings"
                        else f"materialized/ontology/snapshots/{snapshot_id}/{table}.parquet"
                    )
                }
                for table in MATERIALIZED_TABLES
            },
        }

    with pytest.raises(RuntimeError, match="Unsafe"):
        resolve_materialized_table_urls(
            "https://r2.example",
            fetch_json=fetch,
        )


def test_rejects_artifact_from_a_different_snapshot() -> None:
    snapshot_id = "snapshot_abc"

    def fetch(url: str) -> dict:
        if url.endswith("/latest.json"):
            return {
                "format_version": 1,
                "dataset": "ontology",
                "snapshot_id": snapshot_id,
                "manifest_key": (f"materialized/ontology/snapshots/{snapshot_id}/manifest.json"),
            }
        return {
            "format_version": 1,
            "dataset": "ontology",
            "snapshot_id": snapshot_id,
            "artifacts": {
                f"{table}.parquet": {
                    "remote_key": (
                        f"materialized/ontology/snapshots/snapshot_other/{table}.parquet"
                        if table == "proceedings"
                        else f"materialized/ontology/snapshots/{snapshot_id}/{table}.parquet"
                    )
                }
                for table in MATERIALIZED_TABLES
            },
        }

    with pytest.raises(RuntimeError, match="Unsafe"):
        resolve_materialized_table_urls(
            "https://r2.example",
            fetch_json=fetch,
        )


# The snapshot schema version bump, from the reader's side. Version 2 makes
# ``visibility`` a required artifact field; version 1 never had it, and the
# snapshots already published at version 1 must keep resolving.


def test_the_supported_versions_span_the_bump() -> None:
    assert SUPPORTED_FORMAT_VERSIONS == (1, 2)


def test_a_version_2_snapshot_resolves_when_every_table_is_public() -> None:
    snapshot_id = "snapshot_v2"
    prefix = f"materialized/ontology/snapshots/{snapshot_id}/"
    documents = _snapshot(
        snapshot_id,
        format_version=2,
        artifacts={
            f"{table}.parquet": {
                "remote_key": f"{prefix}{table}.parquet",
                "visibility": "public",
            }
            for table in MATERIALIZED_TABLES
        },
    )

    urls = resolve_materialized_table_urls("https://r2.example", fetch_json=documents.__getitem__)

    assert set(urls) == set(MATERIALIZED_TABLES)
    assert all(f"/snapshots/{snapshot_id}/" in url for url in urls.values())


def test_a_version_2_snapshot_never_hands_out_an_internal_object() -> None:
    """The field the bump made required is the one this function reads.

    Before it was required, a manifest that marked a table internal and a
    manifest that forgot to say anything both read as absent, so the resolver
    could not refuse either without refusing both.
    """
    snapshot_id = "snapshot_v2_internal"
    prefix = f"materialized/ontology/snapshots/{snapshot_id}/"
    documents = _snapshot(
        snapshot_id,
        format_version=2,
        artifacts={
            f"{table}.parquet": {
                "remote_key": f"{prefix}{table}.parquet",
                "visibility": "internal" if table == "proceedings" else "public",
            }
            for table in MATERIALIZED_TABLES
        },
    )

    with pytest.raises(RuntimeError, match="marks proceedings.parquet internal"):
        resolve_materialized_table_urls("https://r2.example", fetch_json=documents.__getitem__)


def test_a_version_2_snapshot_that_declares_no_visibility_is_refused() -> None:
    snapshot_id = "snapshot_v2_silent"
    prefix = f"materialized/ontology/snapshots/{snapshot_id}/"
    documents = _snapshot(
        snapshot_id,
        format_version=2,
        artifacts={f"{table}.parquet": {"remote_key": f"{prefix}{table}.parquet"} for table in MATERIALIZED_TABLES},
    )

    with pytest.raises(RuntimeError, match="declares no visibility"):
        resolve_materialized_table_urls("https://r2.example", fetch_json=documents.__getitem__)


def test_a_version_1_snapshot_still_resolves_without_the_field() -> None:
    """A published snapshot outlives the code that wrote it.

    Version 1 never carried ``visibility``; holding it to a rule that did not
    exist when it was sealed would strand every already-published dataset.
    """
    snapshot_id = "snapshot_v1"
    prefix = f"materialized/ontology/snapshots/{snapshot_id}/"
    documents = _snapshot(
        snapshot_id,
        format_version=1,
        artifacts={f"{table}.parquet": {"remote_key": f"{prefix}{table}.parquet"} for table in MATERIALIZED_TABLES},
    )

    urls = resolve_materialized_table_urls("https://r2.example", fetch_json=documents.__getitem__)

    assert set(urls) == set(MATERIALIZED_TABLES)


def test_an_unknown_snapshot_version_is_refused() -> None:
    snapshot_id = "snapshot_v3"
    prefix = f"materialized/ontology/snapshots/{snapshot_id}/"
    documents = _snapshot(
        snapshot_id,
        format_version=3,
        artifacts={f"{table}.parquet": {"remote_key": f"{prefix}{table}.parquet"} for table in MATERIALIZED_TABLES},
    )

    with pytest.raises(RuntimeError, match="pointer"):
        resolve_materialized_table_urls("https://r2.example", fetch_json=documents.__getitem__)


def test_a_pointer_and_manifest_that_disagree_on_version_are_refused() -> None:
    """Two documents, one snapshot. A version split between them is a torn read."""
    snapshot_id = "snapshot_split"
    prefix = f"materialized/ontology/snapshots/{snapshot_id}/"
    documents = _snapshot(
        snapshot_id,
        format_version=2,
        artifacts={
            f"{table}.parquet": {"remote_key": f"{prefix}{table}.parquet", "visibility": "public"}
            for table in MATERIALIZED_TABLES
        },
    )
    documents["https://r2.example/materialized/ontology/latest.json"]["format_version"] = 1

    with pytest.raises(RuntimeError, match="manifest"):
        resolve_materialized_table_urls("https://r2.example", fetch_json=documents.__getitem__)
