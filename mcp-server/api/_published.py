"""Standalone materialized-dataset resolver for the Vercel MCP function."""

from __future__ import annotations

import json
import re
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

MATERIALIZED_TABLES = frozenset(
    {
        "rule_targets",
        "authority_edges",
        "proceedings",
        "regulatory_agenda_items",
        "agenda_item_proceedings",
        "comment_periods",
        "concepts",
        "concept_assignments",
        "concept_events",
    }
)
_SAFE_SNAPSHOT_ID = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def _fetch_json(url: str) -> dict:
    try:
        with urlopen(url, timeout=10) as response:  # noqa: S310 - base URL is HTTPS-validated.
            value = json.load(response)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unable to resolve materialized dataset at {url}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"Materialized dataset document at {url} must be an object")
    return value


def _safe_key(value: object, *, prefix: str) -> str:
    key = str(value or "")
    if (
        not key.startswith(prefix)
        or key.startswith("/")
        or "://" in key
        or any(part in {"", ".", ".."} for part in key.split("/"))
        or any(char in key for char in ("\\", "'", "\x00", "\n", "\r"))
    ):
        raise RuntimeError(f"Unsafe materialized dataset key: {key!r}")
    return key


def resolve_materialized_table_urls(
    base_url: str,
    *,
    dataset: str = "ontology",
) -> dict[str, str]:
    base = base_url.rstrip("/")
    pointer = _fetch_json(f"{base}/materialized/{dataset}/latest.json")
    if pointer.get("format_version") != 1 or pointer.get("dataset") != dataset:
        raise RuntimeError(f"Invalid {dataset} materialized dataset pointer")
    snapshot_id = str(pointer.get("snapshot_id") or "")
    if not _SAFE_SNAPSHOT_ID.fullmatch(snapshot_id):
        raise RuntimeError(f"Invalid {dataset} materialized dataset snapshot id")
    prefix = f"materialized/{dataset}/snapshots/{snapshot_id}/"
    manifest_key = _safe_key(pointer.get("manifest_key"), prefix=prefix)
    if manifest_key != f"{prefix}manifest.json":
        raise RuntimeError(f"Invalid {dataset} materialized dataset manifest key")
    manifest = _fetch_json(f"{base}/{manifest_key}")
    if (
        manifest.get("format_version") != 1
        or manifest.get("dataset") != dataset
        or manifest.get("snapshot_id") != pointer.get("snapshot_id")
    ):
        raise RuntimeError(f"Invalid {dataset} materialized dataset manifest")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        raise RuntimeError(f"{dataset} materialized dataset manifest has no artifacts")

    urls: dict[str, str] = {}
    for table in MATERIALIZED_TABLES:
        record = artifacts.get(f"{table}.parquet")
        if not isinstance(record, dict):
            raise RuntimeError(f"{dataset} manifest is missing {table}.parquet")
        remote_key = _safe_key(record.get("remote_key"), prefix=prefix)
        if remote_key != f"{prefix}{table}.parquet":
            raise RuntimeError(f"{dataset} manifest has an invalid key for {table}.parquet")
        urls[table] = f"{base}/{remote_key}"
    return urls
