"""Freeze and capture exact Regulations.gov document files from Mirrulations.

The Mirrulations bucket is the acquisition source; SpicyRegs' verified cache is
the durable input to release construction.  A run has four explicit steps:

``draw`` freezes exact S3 object identities, ``fetch`` conditionally downloads
the selected JSON/HTML pairs into one content-addressed cache, ``validate``
rechecks complete closure, and ``v3-selection`` emits deterministic partition
ledgers that reference the cached HTML bytes without copying them.

Only ``/documents/`` records participate.  Dockets, comments, unpaired objects,
``_UNAVAILABLE`` tombstones, and superseded JSON revisions are excluded before
any bytes can reach a release ledger.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

from spicy_regs.corpora.body_retrieval_corpus import scan_for_secrets
from spicy_regs.ontology.common import canonical_json
from spicy_regs.schemas import DOCUMENT
from spicy_regs.sources.mirrulations import (
    BUCKET,
    PREFIX,
    download_object_bytes,
    s3_client,
    s3_resource,
)

DRAW_SCHEMA_VERSION = "mirrulations-document-corpus-draw-v1"
CACHE_SCHEMA_VERSION = "mirrulations-document-corpus-cache-v1"
DEFAULT_PREFIX = f"{PREFIX}/SEC/SEC-202"
DEFAULT_MAX_DOCUMENTS = 10_000
DEFAULT_WORKERS = 16
DEFAULT_MAX_OBJECT_BYTES = 64 * 1024 * 1024

_JSON_NAME = re.compile(
    r"^(?P<document_id>[A-Za-z0-9][A-Za-z0-9._-]*?)(?:\((?P<revision>[1-9][0-9]*)\))?\.json$"
)
_CONTENT_NAME = re.compile(r"^(?P<document_id>[A-Za-z0-9][A-Za-z0-9._-]*?)_content\.(?P<extension>html?)$")
_DOCUMENT_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}\Z")
_PARTITION_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_RFC3339_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z")


class MirrulationsCorpusError(RuntimeError):
    """The mirror draw or verified cache failed closed."""


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _timestamp(value: object) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise MirrulationsCorpusError("S3 lastModified must be timezone-aware")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, str) and value:
        return value
    raise MirrulationsCorpusError("S3 lastModified must be a non-empty instant")


def _object_record(value: Mapping[str, Any]) -> dict[str, Any]:
    key = value.get("Key", value.get("key"))
    size = value.get("Size", value.get("size"))
    etag = value.get("ETag", value.get("etag"))
    last_modified = value.get("LastModified", value.get("last_modified"))
    if not isinstance(key, str) or not key:
        raise MirrulationsCorpusError("listed S3 object has no key")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise MirrulationsCorpusError(f"listed S3 object {key} has an invalid size")
    if not isinstance(etag, str) or not etag:
        raise MirrulationsCorpusError(f"listed S3 object {key} has no ETag")
    return {"key": key, "size": size, "etag": etag, "last_modified": _timestamp(last_modified)}


def _draw_digest(value: Mapping[str, Any]) -> str:
    semantic = dict(value)
    semantic.pop("draw_id", None)
    return hashlib.sha256(canonical_json(semantic).encode("utf-8")).hexdigest()


def build_draw(
    objects: Iterable[Mapping[str, Any]],
    *,
    bucket: str = BUCKET,
    prefix: str = DEFAULT_PREFIX,
    max_documents: int = DEFAULT_MAX_DOCUMENTS,
) -> dict[str, Any]:
    """Select the first sorted paired document records from one frozen listing."""

    if not bucket or not prefix:
        raise MirrulationsCorpusError("bucket and prefix must be non-empty")
    if max_documents <= 0:
        raise MirrulationsCorpusError("max_documents must be greater than zero")

    json_by_pair: dict[tuple[str, str], list[tuple[int, dict[str, Any]]]] = {}
    content_by_pair: dict[tuple[str, str], list[dict[str, Any]]] = {}
    listed = document_json_objects = content_objects = tombstones = 0
    for raw in objects:
        listed += 1
        record = _object_record(raw)
        key = record["key"]
        if not key.startswith(prefix) or "/documents/" not in key:
            continue
        path = PurePosixPath(key)
        parent = str(path.parent)
        name = path.name
        if "_UNAVAILABLE" in name.upper():
            tombstones += 1
            continue
        json_match = _JSON_NAME.fullmatch(name)
        if json_match is not None:
            document_json_objects += 1
            document_id = json_match["document_id"]
            revision = int(json_match["revision"] or 0)
            json_by_pair.setdefault((parent, document_id), []).append((revision, record))
            continue
        content_match = _CONTENT_NAME.fullmatch(name)
        if content_match is not None:
            content_objects += 1
            content_by_pair.setdefault((parent, content_match["document_id"]), []).append(record)

    pair_keys = sorted(set(json_by_pair) & set(content_by_pair), key=lambda item: (item[1], item[0]))
    if len(pair_keys) < max_documents:
        raise MirrulationsCorpusError(
            f"draw requested {max_documents} paired documents, but the listing contains {len(pair_keys)}"
        )

    documents: list[dict[str, Any]] = []
    superseded_revisions = sum(len(values) - 1 for values in json_by_pair.values())
    duplicate_renditions = sum(len(values) - 1 for values in content_by_pair.values())
    selected_superseded_revisions = selected_duplicate_renditions = 0
    seen_ids: set[str] = set()
    for pair_key in pair_keys[:max_documents]:
        parent, document_id = pair_key
        if document_id in seen_ids:
            raise MirrulationsCorpusError(f"document ID appears in more than one mirror directory: {document_id}")
        seen_ids.add(document_id)
        revisions = sorted(json_by_pair[pair_key], key=lambda item: (item[0], item[1]["key"]))
        selected_superseded_revisions += len(revisions) - 1
        revision, metadata = revisions[-1]
        renditions = sorted(content_by_pair[pair_key], key=lambda item: item["key"])
        selected_duplicate_renditions += len(renditions) - 1
        rendition = renditions[0]
        documents.append(
            {
                "document_id": document_id,
                "json_revision": revision,
                "metadata_object": metadata,
                "rendition_object": rendition,
                "mirror_directory": parent,
            }
        )

    manifest: dict[str, Any] = {
        "schema_version": DRAW_SCHEMA_VERSION,
        "source": {"bucket": bucket, "prefix": prefix},
        "selection": {
            "record_type": "documents",
            "ordering": "document-id-then-mirror-directory-v1",
            "max_documents": max_documents,
            "json_revision_policy": "highest-numeric-suffix-v1",
            "rendition_extensions": ["htm", "html"],
        },
        "counts": {
            "listed_objects": listed,
            "document_json_objects": document_json_objects,
            "content_objects": content_objects,
            "paired_documents": len(pair_keys),
            "selected_documents": len(documents),
            "unpaired_json_documents": len(set(json_by_pair) - set(content_by_pair)),
            "unpaired_content_documents": len(set(content_by_pair) - set(json_by_pair)),
            "superseded_json_revisions": superseded_revisions,
            "selected_superseded_json_revisions": selected_superseded_revisions,
            "duplicate_content_renditions": duplicate_renditions,
            "selected_duplicate_content_renditions": selected_duplicate_renditions,
            "excluded_tombstones": tombstones,
        },
        "bytes": {
            "metadata": sum(item["metadata_object"]["size"] for item in documents),
            "renditions": sum(item["rendition_object"]["size"] for item in documents),
            "total": sum(
                item["metadata_object"]["size"] + item["rendition_object"]["size"] for item in documents
            ),
        },
        "documents": documents,
    }
    manifest["draw_id"] = "urn:spicyregs:mirrulations-document-draw:" + _draw_digest(manifest)[:24]
    scan_for_secrets(manifest, "mirrulations-document-draw")
    return manifest


def list_s3_objects(*, client: Any, bucket: str, prefix: str) -> Iterable[Mapping[str, Any]]:
    """Stream one anonymous S3 prefix listing with object metadata intact."""

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    scan_for_secrets(value, "mirrulations-document-corpus")
    return (canonical_json(value) + "\n").encode("utf-8")


def _write_new(path: Path, payload: bytes) -> None:
    """Atomically create an immutable output, accepting an identical race."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix=f".{path.name}.building-", dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.is_symlink() or not path.is_file() or path.read_bytes() != payload:
                raise MirrulationsCorpusError(f"refusing to replace existing immutable output: {path}") from None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _replace_json(path: Path, value: Mapping[str, Any]) -> None:
    payload = _json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix=f".{path.name}.building-", dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def write_draw(path: Path, manifest: Mapping[str, Any]) -> None:
    _write_new(Path(path), _json_bytes(manifest))


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise MirrulationsCorpusError(f"invalid JSON object: {path}") from error
    if not isinstance(value, dict):
        raise MirrulationsCorpusError(f"JSON must contain an object: {path}")
    return value


def _draw_documents(manifest: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    if manifest.get("schema_version") != DRAW_SCHEMA_VERSION:
        raise MirrulationsCorpusError("draw schema version differs")
    expected_id = "urn:spicyregs:mirrulations-document-draw:" + _draw_digest(manifest)[:24]
    if manifest.get("draw_id") != expected_id:
        raise MirrulationsCorpusError("draw identity does not match its content")
    documents = manifest.get("documents")
    if not isinstance(documents, list) or not all(isinstance(item, Mapping) for item in documents):
        raise MirrulationsCorpusError("draw documents must be an array of objects")
    ids = [item.get("document_id") for item in documents]
    if any(not isinstance(item, str) or _DOCUMENT_ID.fullmatch(item) is None for item in ids):
        raise MirrulationsCorpusError("draw contains an invalid document ID")
    if len(set(ids)) != len(ids):
        raise MirrulationsCorpusError("draw document IDs must be unique")
    return documents


def _attachment_metadata(metadata_bytes: bytes, rendition_bytes: bytes, entry: Mapping[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(metadata_bytes.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise MirrulationsCorpusError("Mirrulations document metadata is not UTF-8 JSON") from error
    if not isinstance(payload, Mapping):
        raise MirrulationsCorpusError("Mirrulations document metadata must be an object")
    data = payload.get("data")
    if not isinstance(data, Mapping) or data.get("type") != "documents":
        raise MirrulationsCorpusError("source record is not a Regulations.gov document")
    document_id = entry.get("document_id")
    flattened = DOCUMENT.extract(dict(payload))
    if data.get("id") != document_id or flattened.get("document_id") != document_id:
        raise MirrulationsCorpusError("path ID and DOCUMENT.extract ID differ")
    title = flattened.get("title")
    document_type = flattened.get("document_type")
    if not isinstance(title, str) or not title or not isinstance(document_type, str) or not document_type:
        raise MirrulationsCorpusError("document metadata lacks a title or document type")

    rendition = entry.get("rendition_object")
    if not isinstance(rendition, Mapping):
        raise MirrulationsCorpusError("draw rendition object is invalid")
    suffix = PurePosixPath(str(rendition.get("key"))).suffix.removeprefix(".").casefold()
    try:
        attachments = json.loads(str(flattened.get("attachments_json") or "[]"))
    except json.JSONDecodeError as error:
        raise MirrulationsCorpusError("DOCUMENT.extract attachments are invalid JSON") from error
    matched_url: str | None = None
    for attachment in attachments if isinstance(attachments, list) else []:
        if not isinstance(attachment, Mapping):
            continue
        url = attachment.get("url")
        if not isinstance(url, str):
            continue
        basename = PurePosixPath(urlparse(url).path).name.casefold()
        if (
            basename == f"content.{suffix}"
            and str(attachment.get("format") or "").casefold() == suffix
            and attachment.get("size") == len(rendition_bytes)
        ):
            matched_url = url
            break
    if matched_url is None:
        raise MirrulationsCorpusError("captured HTML does not match a declared fileFormats URL and size")
    try:
        rendition_bytes.decode("utf-8-sig")
    except UnicodeError as error:
        raise MirrulationsCorpusError("captured HTML is not UTF-8") from error
    if not rendition_bytes.strip():
        raise MirrulationsCorpusError("captured HTML is empty")

    source_url = data.get("links", {}).get("self") if isinstance(data.get("links"), Mapping) else None
    return {
        "document_id": document_id,
        "title": title,
        "document_type": document_type,
        "posted_date": flattened.get("posted_date"),
        "modify_date": flattened.get("modify_date"),
        "source_url": source_url if isinstance(source_url, str) and source_url else None,
        "rendition_url": matched_url,
    }


def _download_pair(
    entry: Mapping[str, Any],
    *,
    resource: Any,
    bucket: str,
    cache_dir: Path,
    draw_id: str,
    retrieved_at: str,
    max_object_bytes: int,
) -> dict[str, Any]:
    document_id = str(entry["document_id"])
    metadata_spec = entry.get("metadata_object")
    rendition_spec = entry.get("rendition_object")
    if not isinstance(metadata_spec, Mapping) or not isinstance(rendition_spec, Mapping):
        raise MirrulationsCorpusError("draw object pair is invalid")
    for label, spec in (("metadata", metadata_spec), ("rendition", rendition_spec)):
        if not isinstance(spec.get("size"), int) or spec["size"] > max_object_bytes:
            raise MirrulationsCorpusError(f"{document_id} {label} exceeds the object byte cap")

    metadata = download_object_bytes(
        resource,
        bucket,
        str(metadata_spec["key"]),
        if_match=str(metadata_spec["etag"]),
        max_bytes=max_object_bytes,
    )
    rendition = download_object_bytes(
        resource,
        bucket,
        str(rendition_spec["key"]),
        if_match=str(rendition_spec["etag"]),
        max_bytes=max_object_bytes,
    )
    if len(metadata.content) != metadata_spec["size"] or len(rendition.content) != rendition_spec["size"]:
        raise MirrulationsCorpusError("downloaded byte count differs from the frozen S3 listing")
    if metadata.last_modified is None or _timestamp(metadata.last_modified) != metadata_spec["last_modified"]:
        raise MirrulationsCorpusError("metadata lastModified differs from the frozen S3 listing")
    if rendition.last_modified is None or _timestamp(rendition.last_modified) != rendition_spec["last_modified"]:
        raise MirrulationsCorpusError("rendition lastModified differs from the frozen S3 listing")
    extracted = _attachment_metadata(metadata.content, rendition.content, entry)
    metadata_sha = _sha256_bytes(metadata.content)
    rendition_sha = _sha256_bytes(rendition.content)
    pair_sha = hashlib.sha256(
        canonical_json(
            {
                "document_id": document_id,
                "metadata_sha256": metadata_sha,
                "rendition_sha256": rendition_sha,
            }
        ).encode("utf-8")
    ).hexdigest()
    extension = PurePosixPath(str(rendition_spec["key"])).suffix.casefold()
    metadata_cache = f"objects/sha256/{metadata_sha}.json"
    rendition_cache = f"objects/sha256/{rendition_sha}{extension}"
    _write_new(cache_dir / metadata_cache, metadata.content)
    _write_new(cache_dir / rendition_cache, rendition.content)
    receipt = {
        "schema_version": CACHE_SCHEMA_VERSION,
        "status": "ok",
        "draw_id": draw_id,
        "document_id": document_id,
        "retrieved_at": retrieved_at,
        "metadata": {
            "source": dict(metadata_spec),
            "cache_file": metadata_cache,
            "sha256": metadata_sha,
            "observed_etag": metadata.etag,
            "observed_last_modified": _timestamp(metadata.last_modified),
        },
        "rendition": {
            "source": dict(rendition_spec),
            "cache_file": rendition_cache,
            "sha256": rendition_sha,
            "media_type": "text/html",
            "observed_etag": rendition.etag,
            "observed_last_modified": _timestamp(rendition.last_modified),
        },
        "pair_sha256": pair_sha,
        "source_input_id": f"urn:spicyregs:mirrulations-pair:sha256:{pair_sha}",
        "document": extracted,
    }
    _write_new(cache_dir / "receipts" / f"{document_id}.json", _json_bytes(receipt))
    return receipt


def _contained_cache_path(cache_dir: Path, relative: object) -> Path:
    if not isinstance(relative, str) or not relative:
        raise MirrulationsCorpusError("receipt cache path is invalid")
    pure = PurePosixPath(relative)
    if pure.is_absolute() or ".." in pure.parts:
        raise MirrulationsCorpusError("receipt cache path escapes the cache")
    root = cache_dir.resolve()
    path = (root / Path(*pure.parts)).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise MirrulationsCorpusError("receipt cache path escapes the cache") from error
    return path


def _validate_receipt(
    entry: Mapping[str, Any], receipt: Mapping[str, Any], *, cache_dir: Path, draw_id: str
) -> None:
    document_id = str(entry["document_id"])
    if (
        receipt.get("schema_version") != CACHE_SCHEMA_VERSION
        or receipt.get("status") != "ok"
        or receipt.get("draw_id") != draw_id
        or receipt.get("document_id") != document_id
    ):
        raise MirrulationsCorpusError(f"{document_id}: receipt identity differs")
    metadata = receipt.get("metadata")
    rendition = receipt.get("rendition")
    if not isinstance(metadata, Mapping) or not isinstance(rendition, Mapping):
        raise MirrulationsCorpusError(f"{document_id}: receipt objects are invalid")
    if metadata.get("source") != entry.get("metadata_object") or rendition.get("source") != entry.get(
        "rendition_object"
    ):
        raise MirrulationsCorpusError(f"{document_id}: receipt differs from the frozen draw")
    if (
        metadata.get("observed_etag") != entry["metadata_object"]["etag"]
        or metadata.get("observed_last_modified") != entry["metadata_object"]["last_modified"]
        or rendition.get("observed_etag") != entry["rendition_object"]["etag"]
        or rendition.get("observed_last_modified") != entry["rendition_object"]["last_modified"]
        or rendition.get("media_type") != "text/html"
    ):
        raise MirrulationsCorpusError(f"{document_id}: observed S3 metadata differs from the frozen draw")
    if not isinstance(receipt.get("retrieved_at"), str) or _RFC3339_UTC.fullmatch(receipt["retrieved_at"]) is None:
        raise MirrulationsCorpusError(f"{document_id}: retrieval instant is invalid")
    metadata_path = _contained_cache_path(cache_dir, metadata.get("cache_file"))
    rendition_path = _contained_cache_path(cache_dir, rendition.get("cache_file"))
    if not metadata_path.is_file() or not rendition_path.is_file():
        raise MirrulationsCorpusError(f"{document_id}: cached pair is missing")
    metadata_bytes = metadata_path.read_bytes()
    rendition_bytes = rendition_path.read_bytes()
    metadata_sha = _sha256_bytes(metadata_bytes)
    rendition_sha = _sha256_bytes(rendition_bytes)
    if metadata_sha != metadata.get("sha256") or rendition_sha != rendition.get("sha256"):
        raise MirrulationsCorpusError(f"{document_id}: cached pair digest differs")
    if len(metadata_bytes) != entry["metadata_object"]["size"] or len(rendition_bytes) != entry["rendition_object"][
        "size"
    ]:
        raise MirrulationsCorpusError(f"{document_id}: cached pair byte count differs")
    extracted = _attachment_metadata(metadata_bytes, rendition_bytes, entry)
    if receipt.get("document") != extracted:
        raise MirrulationsCorpusError(f"{document_id}: extracted document metadata differs")
    pair_sha = hashlib.sha256(
        canonical_json(
            {
                "document_id": document_id,
                "metadata_sha256": metadata_sha,
                "rendition_sha256": rendition_sha,
            }
        ).encode("utf-8")
    ).hexdigest()
    if receipt.get("pair_sha256") != pair_sha or receipt.get("source_input_id") != (
        f"urn:spicyregs:mirrulations-pair:sha256:{pair_sha}"
    ):
        raise MirrulationsCorpusError(f"{document_id}: pair identity differs")


def _existing_receipt(entry: Mapping[str, Any], *, cache_dir: Path, draw_id: str) -> dict[str, Any] | None:
    path = cache_dir / "receipts" / f"{entry['document_id']}.json"
    if not path.is_file():
        return None
    receipt = _read_json(path)
    _validate_receipt(entry, receipt, cache_dir=cache_dir, draw_id=draw_id)
    return receipt


def _write_cache_state(
    manifest: Mapping[str, Any],
    *,
    cache_dir: Path,
    outcomes: Mapping[str, str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    draw_id = str(manifest["draw_id"])
    sources: list[dict[str, Any]] = []
    quarantine_rows: list[dict[str, str]] = []
    for entry in _draw_documents(manifest):
        document_id = str(entry["document_id"])
        try:
            receipt = _existing_receipt(entry, cache_dir=cache_dir, draw_id=draw_id)
        except MirrulationsCorpusError as error:
            receipt = None
            outcomes = dict(outcomes) | {document_id: str(error)}
        if receipt is None:
            quarantine_rows.append(
                {"document_id": document_id, "reason": outcomes.get(document_id, "not-fetched")}
            )
            continue
        sources.append(
            {
                "document_id": document_id,
                "receipt_file": f"receipts/{document_id}.json",
                "source_input_id": receipt["source_input_id"],
                "pair_sha256": receipt["pair_sha256"],
                "metadata_object": receipt["metadata"]["source"],
                "metadata_sha256": receipt["metadata"]["sha256"],
                "metadata_cache_file": receipt["metadata"]["cache_file"],
                "rendition_object": receipt["rendition"]["source"],
                "rendition_sha256": receipt["rendition"]["sha256"],
                "rendition_cache_file": receipt["rendition"]["cache_file"],
            }
        )
    lock = {
        "schema_version": CACHE_SCHEMA_VERSION,
        "draw_id": draw_id,
        "source_count": len(sources),
        "sources": sources,
    }
    quarantine = {
        "schema_version": CACHE_SCHEMA_VERSION,
        "draw_id": draw_id,
        "total": len(quarantine_rows),
        "rows": quarantine_rows,
    }
    _replace_json(cache_dir / "source-lock.json", lock)
    _replace_json(cache_dir / "quarantine.json", quarantine)
    return lock, quarantine


def fetch_pairs(
    draw_path: Path,
    cache_dir: Path,
    *,
    resource: Any | None = None,
    workers: int = DEFAULT_WORKERS,
    max_pairs: int | None = None,
    max_object_bytes: int = DEFAULT_MAX_OBJECT_BYTES,
    retrieved_at: str | None = None,
) -> dict[str, Any]:
    """Fetch missing pairs concurrently into one resumable SpicyRegs cache."""

    if workers <= 0 or workers > 64:
        raise MirrulationsCorpusError("workers must be between 1 and 64")
    if max_pairs is not None and max_pairs <= 0:
        raise MirrulationsCorpusError("max_pairs must be greater than zero")
    if max_object_bytes <= 0:
        raise MirrulationsCorpusError("max_object_bytes must be greater than zero")
    manifest = _read_json(Path(draw_path))
    documents = _draw_documents(manifest)
    bucket = manifest.get("source", {}).get("bucket") if isinstance(manifest.get("source"), Mapping) else None
    if not isinstance(bucket, str) or not bucket:
        raise MirrulationsCorpusError("draw source bucket is invalid")
    cache = Path(cache_dir).resolve()
    cache.mkdir(parents=True, exist_ok=True)
    draw_id = str(manifest["draw_id"])
    stamp = retrieved_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if _RFC3339_UTC.fullmatch(stamp) is None:
        raise MirrulationsCorpusError("retrieved_at must be an RFC 3339 UTC instant")

    missing: list[Mapping[str, Any]] = []
    skipped = 0
    outcomes: dict[str, str] = {}
    for entry in documents:
        try:
            existing = _existing_receipt(entry, cache_dir=cache, draw_id=draw_id)
        except MirrulationsCorpusError as error:
            outcomes[str(entry["document_id"])] = f"existing-receipt-invalid: {error}"
            continue
        if existing is None:
            missing.append(entry)
        else:
            skipped += 1
    selected = missing[:max_pairs] if max_pairs is not None else missing
    for entry in missing[len(selected) :]:
        outcomes[str(entry["document_id"])] = "not-requested-by-pair-budget"

    source = resource or s3_resource(workers)
    fetched = 0
    with ThreadPoolExecutor(max_workers=min(workers, len(selected)) or 1) as executor:
        futures = {
            executor.submit(
                _download_pair,
                entry,
                resource=source,
                bucket=bucket,
                cache_dir=cache,
                draw_id=draw_id,
                retrieved_at=stamp,
                max_object_bytes=max_object_bytes,
            ): entry
            for entry in selected
        }
        for future in as_completed(futures):
            document_id = str(futures[future]["document_id"])
            try:
                future.result()
            except Exception as error:  # noqa: BLE001 - every failed pair is quarantined
                outcomes[document_id] = f"{type(error).__name__}: {error}"
            else:
                fetched += 1
    lock, quarantine = _write_cache_state(manifest, cache_dir=cache, outcomes=outcomes)
    return {
        "draw_id": draw_id,
        "requested_pairs": len(selected),
        "fetched_pairs": fetched,
        "skipped_pairs": skipped,
        "sealed_pairs": lock["source_count"],
        "quarantined_pairs": quarantine["total"],
    }


def validate_cache(draw_path: Path, cache_dir: Path) -> dict[str, Any]:
    """Re-read every exact pair and require complete draw/cache closure."""

    manifest = _read_json(Path(draw_path))
    documents = _draw_documents(manifest)
    cache = Path(cache_dir).resolve()
    failures: list[str] = []
    draw_id = str(manifest["draw_id"])
    valid_ids: list[str] = []
    expected_sources: list[dict[str, Any]] = []
    total_bytes = 0
    for entry in documents:
        document_id = str(entry["document_id"])
        try:
            receipt = _existing_receipt(entry, cache_dir=cache, draw_id=draw_id)
            if receipt is None:
                raise MirrulationsCorpusError("receipt is missing")
        except MirrulationsCorpusError as error:
            failures.append(f"{document_id}: {error}")
            continue
        valid_ids.append(document_id)
        expected_sources.append(
            {
                "document_id": document_id,
                "receipt_file": f"receipts/{document_id}.json",
                "source_input_id": receipt["source_input_id"],
                "pair_sha256": receipt["pair_sha256"],
                "metadata_object": receipt["metadata"]["source"],
                "metadata_sha256": receipt["metadata"]["sha256"],
                "metadata_cache_file": receipt["metadata"]["cache_file"],
                "rendition_object": receipt["rendition"]["source"],
                "rendition_sha256": receipt["rendition"]["sha256"],
                "rendition_cache_file": receipt["rendition"]["cache_file"],
            }
        )
        total_bytes += entry["metadata_object"]["size"] + entry["rendition_object"]["size"]

    try:
        lock = _read_json(cache / "source-lock.json")
        lock_ids = [item.get("document_id") for item in lock.get("sources", []) if isinstance(item, Mapping)]
        if lock.get("schema_version") != CACHE_SCHEMA_VERSION or lock.get("draw_id") != draw_id:
            failures.append("source lock identity differs")
        if (
            lock_ids != valid_ids
            or lock.get("source_count") != len(valid_ids)
            or lock.get("sources") != expected_sources
        ):
            failures.append("source lock does not close against verified receipts")
    except MirrulationsCorpusError as error:
        failures.append(str(error))
    try:
        quarantine = _read_json(cache / "quarantine.json")
        if quarantine.get("draw_id") != draw_id:
            failures.append("quarantine draw identity differs")
        if quarantine.get("total") != 0:
            failures.append(f"quarantine contains {quarantine.get('total')} unresolved document(s)")
    except MirrulationsCorpusError as error:
        failures.append(str(error))
    if len(valid_ids) != len(documents):
        failures.append(f"verified {len(valid_ids)} of {len(documents)} selected documents")
    return {
        "status": "pass" if not failures else "fail",
        "draw_id": draw_id,
        "selected_documents": len(documents),
        "verified_documents": len(valid_ids),
        "verified_source_bytes": total_bytes,
        "failures": failures,
    }


def write_v3_selection(
    draw_path: Path,
    cache_dir: Path,
    output_path: Path,
    *,
    partition_id: str,
    split_count: int = 1,
    split_index: int = 0,
) -> dict[str, Any]:
    """Write one disjoint ledger only after the entire capture verifies."""

    if _PARTITION_ID.fullmatch(partition_id) is None:
        raise MirrulationsCorpusError("partition_id must use portable lowercase filename characters")
    if split_count <= 0 or split_index < 0 or split_index >= split_count:
        raise MirrulationsCorpusError("split index must be inside a positive split count")
    validation = validate_cache(draw_path, cache_dir)
    if validation["status"] != "pass":
        raise MirrulationsCorpusError(f"source cache is incomplete: {validation['failures']}")
    manifest = _read_json(Path(draw_path))
    documents = _draw_documents(manifest)
    chosen = documents[split_index::split_count]
    cache = Path(cache_dir).resolve()
    output = Path(output_path).resolve()
    if output.exists() or output.is_symlink():
        raise MirrulationsCorpusError(f"refusing to replace existing v3 selection: {output}")

    digest = hashlib.sha256()
    size = 0
    from spicy_regs.document_release_v3_writer import SourceInput

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix=f".{output.name}.building-", dir=output.parent, delete=False) as stream:
            temporary = Path(stream.name)
            for entry in chosen:
                document_id = str(entry["document_id"])
                receipt = _read_json(cache / "receipts" / f"{document_id}.json")
                document = receipt["document"]
                rendition_path = _contained_cache_path(cache, receipt["rendition"]["cache_file"])
                record: dict[str, Any] = {
                    "documentId": f"https://www.regulations.gov/document/{document_id}",
                    "sourceInputId": receipt["source_input_id"],
                    "sourceId": "regulations.gov",
                    "sourcePartition": partition_id,
                    "disposition": "active",
                    "previousActive": False,
                    "sourceRecordId": document_id,
                    "sourceVersion": f"sha256:{receipt['pair_sha256']}",
                    "renditionPath": str(rendition_path),
                    "mediaType": "text/html",
                    "title": document["title"],
                    "documentType": document["document_type"],
                    "language": "en",
                    "eligibilityState": "unverified",
                    "eligibilityAuthorityId": "spicyregs.mirrulations-document-corpus.v1",
                    "eligibilityEvidenceKind": "sealed-qualification",
                    "eligibilityBasis": (
                        f"exact Mirrulations JSON and HTML pair verified under draw {manifest['draw_id']}; "
                        "this producer pilot grants no search eligibility"
                    ),
                    "eligibilityReasonCode": "spicyregs.eligibility.unverified-scale-pilot",
                }
                if isinstance(document.get("posted_date"), str) and _RFC3339_UTC.fullmatch(document["posted_date"]):
                    record["publishedAt"] = document["posted_date"]
                if isinstance(document.get("modify_date"), str) and _RFC3339_UTC.fullmatch(document["modify_date"]):
                    record["updatedAt"] = document["modify_date"]
                scan_for_secrets(record, f"v3-selection.{document_id}")
                SourceInput.from_dict(record, base_path=output.parent)
                line = (canonical_json(record) + "\n").encode("utf-8")
                stream.write(line)
                digest.update(line)
                size += len(line)
            stream.flush()
            os.fsync(stream.fileno())
        assert temporary is not None
        os.link(temporary, output)
    except FileExistsError:
        raise MirrulationsCorpusError(f"refusing to replace existing v3 selection: {output}") from None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return {
        "draw_id": manifest["draw_id"],
        "partition_id": partition_id,
        "split_count": split_count,
        "split_index": split_index,
        "selected_documents": len(chosen),
        "complete_draw_documents": len(documents),
        "output": str(output),
        "byte_size": size,
        "sha256": digest.hexdigest(),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    draw = commands.add_parser("draw", help="freeze a deterministic paired S3 inventory")
    draw.add_argument("--output", type=Path, required=True)
    draw.add_argument("--bucket", default=BUCKET)
    draw.add_argument("--prefix", default=DEFAULT_PREFIX)
    draw.add_argument("--max-documents", type=int, default=DEFAULT_MAX_DOCUMENTS)
    fetch = commands.add_parser("fetch", help="fetch exact pairs into one resumable cache")
    fetch.add_argument("--draw", type=Path, required=True)
    fetch.add_argument("--cache-dir", type=Path, required=True)
    fetch.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    fetch.add_argument("--max-pairs", type=int)
    fetch.add_argument("--max-object-bytes", type=int, default=DEFAULT_MAX_OBJECT_BYTES)
    fetch.add_argument("--retrieved-at")
    validate = commands.add_parser("validate", help="verify complete cache closure")
    validate.add_argument("--draw", type=Path, required=True)
    validate.add_argument("--cache-dir", type=Path, required=True)
    selection = commands.add_parser("v3-selection", help="write one deterministic v3 partition ledger")
    selection.add_argument("--draw", type=Path, required=True)
    selection.add_argument("--cache-dir", type=Path, required=True)
    selection.add_argument("--output", type=Path, required=True)
    selection.add_argument("--partition-id", required=True)
    selection.add_argument("--split-count", type=int, default=1)
    selection.add_argument("--split-index", type=int, default=0)
    args = parser.parse_args(argv)

    if args.command == "draw":
        manifest = build_draw(
            list_s3_objects(client=s3_client(), bucket=args.bucket, prefix=args.prefix),
            bucket=args.bucket,
            prefix=args.prefix,
            max_documents=args.max_documents,
        )
        write_draw(args.output, manifest)
        print(canonical_json({"output": str(args.output), **manifest["counts"], **manifest["bytes"]}))
        return 0
    if args.command == "fetch":
        print(
            canonical_json(
                fetch_pairs(
                    args.draw,
                    args.cache_dir,
                    workers=args.workers,
                    max_pairs=args.max_pairs,
                    max_object_bytes=args.max_object_bytes,
                    retrieved_at=args.retrieved_at,
                )
            )
        )
        return 0
    if args.command == "v3-selection":
        print(
            canonical_json(
                write_v3_selection(
                    args.draw,
                    args.cache_dir,
                    args.output,
                    partition_id=args.partition_id,
                    split_count=args.split_count,
                    split_index=args.split_index,
                )
            )
        )
        return 0
    result = validate_cache(args.draw, args.cache_dir)
    print(canonical_json(result))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
