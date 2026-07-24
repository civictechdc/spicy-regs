"""Public materialized-dataset pointer resolution tests."""

from __future__ import annotations

import pytest

from spicy_regs.published import MATERIALIZED_TABLES, resolve_materialized_table_urls


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
