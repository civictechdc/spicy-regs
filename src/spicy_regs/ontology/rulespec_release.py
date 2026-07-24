"""Fail-closed verification for a Rulespec release consumed by publication."""

from __future__ import annotations

import re
import tarfile
from hashlib import sha256
from io import BytesIO
from pathlib import Path, PurePosixPath

import httpx
import yaml

_CONTRACT_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_RELEASE_VERSION = re.compile(
    r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?(?:\+[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?$"
)
_RELEASE_URL_PREFIX = "https://github.com/Formspec-Labs/rulespec/releases/tag/v"
_RELEASE_ARCHIVE_URL_PREFIX = "https://github.com/Formspec-Labs/rulespec/archive/refs/tags/v"
_MAX_ARCHIVE_BYTES = 32 * 1024 * 1024
_MAX_CONTRACT_BYTES = 8 * 1024 * 1024
_CONTRACT_FILES = frozenset(
    {
        "context/rkaf-context.jsonld",
        "constraints/semantics/l0-ranges.cue",
    }
)


def _contract_digest_from_archive(payload: bytes) -> str:
    """Recompute the canonical L0 digest from a Rulespec tag archive."""
    contract_files: dict[str, bytes] = {}
    archive_root: str | None = None
    contract_bytes = 0
    try:
        with tarfile.open(fileobj=BytesIO(payload), mode="r:gz") as archive:
            for member in archive.getmembers():
                if not member.isfile():
                    continue
                parts = PurePosixPath(member.name).parts
                if len(parts) < 2 or any(part in {"", ".", ".."} for part in parts):
                    continue
                relative = PurePosixPath(*parts[1:])
                relative_name = relative.as_posix()
                is_core_constraint = (
                    relative.parent == PurePosixPath("constraints/core") and relative.suffix == ".cue"
                )
                if relative_name not in _CONTRACT_FILES and not is_core_constraint:
                    continue
                if archive_root is not None and archive_root != parts[0]:
                    raise RuntimeError("Rulespec release archive has multiple roots")
                archive_root = parts[0]
                if relative_name in contract_files:
                    raise RuntimeError(f"Rulespec release archive repeats contract file {relative_name}")
                contract_bytes += member.size
                if contract_bytes > _MAX_CONTRACT_BYTES:
                    raise RuntimeError("Rulespec contract files exceed the publication preflight size limit")
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise RuntimeError(f"Rulespec release archive cannot read contract file {relative_name}")
                contract_files[relative_name] = extracted.read()
    except (tarfile.TarError, OSError) as exc:
        raise RuntimeError(f"Invalid Rulespec release archive: {exc}") from exc

    missing = sorted(_CONTRACT_FILES - contract_files.keys())
    core_constraints = [name for name in contract_files if name.startswith("constraints/core/")]
    if missing or not core_constraints:
        detail = f"missing {', '.join(missing)}" if missing else "missing constraints/core/*.cue"
        raise RuntimeError(f"Rulespec release archive is incomplete: {detail}")

    digest = sha256()
    for name in sorted(contract_files):
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update(contract_files[name])
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


def _download_archive(client: httpx.Client, version: str) -> bytes:
    archive_url = f"{_RELEASE_ARCHIVE_URL_PREFIX}{version}.tar.gz"
    try:
        with client.stream("GET", archive_url) as response:
            response.raise_for_status()
            payload = bytearray()
            for chunk in response.iter_bytes():
                payload.extend(chunk)
                if len(payload) > _MAX_ARCHIVE_BYTES:
                    raise RuntimeError("Rulespec release archive exceeds the publication preflight size limit")
    except httpx.HTTPError as exc:
        raise RuntimeError(
            f"Cannot publish ontology dataset; Rulespec tag archive is not reachable at {archive_url}"
        ) from exc
    return bytes(payload)


def _verify_release(
    *,
    version: str,
    release_url: str,
    expected_digest: str,
    client: httpx.Client | None = None,
) -> None:
    def verify(active_client: httpx.Client) -> None:
        try:
            response = active_client.head(release_url)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise RuntimeError(
                f"Cannot publish ontology dataset; pinned Rulespec release is not reachable at {release_url}"
            ) from exc
        actual_digest = _contract_digest_from_archive(_download_archive(active_client, version))
        if actual_digest != expected_digest:
            raise RuntimeError(
                "Cannot publish ontology dataset; declared Rulespec digest "
                f"{expected_digest} does not match tagged Rulespec contract digest {actual_digest}"
            )

    if client is not None:
        verify(client)
        return
    with httpx.Client(follow_redirects=True, timeout=20) as active_client:
        verify(active_client)


def require_released_rulespec(
    path: Path,
    *,
    verify_reachable: bool = True,
    client: httpx.Client | None = None,
) -> None:
    """Fail unless an L0 declaration pins the exact reachable Rulespec release."""
    try:
        declaration = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise RuntimeError(f"Cannot publish ontology dataset; invalid Rulespec declaration at {path}: {exc}") from exc

    if not isinstance(declaration, dict):
        raise RuntimeError(f"Cannot publish ontology dataset; Rulespec declaration at {path} is not a mapping")

    digest = declaration.get("rulespec_version")
    if not isinstance(digest, str) or not _CONTRACT_DIGEST.fullmatch(digest):
        raise RuntimeError(
            f"Cannot publish ontology dataset; Rulespec declaration at {path} "
            "does not contain an immutable sha256 contract digest"
        )
    results = declaration.get("results")
    if (
        declaration.get("declared_levels") != ["L0"]
        or not isinstance(results, dict)
        or results.get("L0") != "pass"
    ):
        raise RuntimeError(
            f"Cannot publish ontology dataset; Rulespec declaration at {path} does not contain a passing L0 claim"
        )

    version = declaration.get("rulespec_release")
    release_url = declaration.get("rulespec_release_url")
    if not isinstance(version, str) or not _RELEASE_VERSION.fullmatch(version):
        raise RuntimeError(
            f"Cannot publish ontology dataset; Rulespec declaration at {path} "
            "does not pin a released semantic version"
        )

    expected_url = f"{_RELEASE_URL_PREFIX}{version}"
    if not isinstance(release_url, str) or release_url != expected_url:
        raise RuntimeError(
            f"Cannot publish ontology dataset; Rulespec release URL must be {expected_url!r}, got {release_url!r}"
        )

    if not verify_reachable:
        return
    _verify_release(
        version=version,
        release_url=release_url,
        expected_digest=digest,
        client=client,
    )
