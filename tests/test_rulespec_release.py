"""Rulespec release and contract-digest publication gates."""

import tarfile
from hashlib import sha256
from io import BytesIO
from pathlib import Path

import httpx
import pytest

import spicy_regs.ontology.rulespec_release as rulespec_release
from spicy_regs.ontology.rulespec_release import require_released_rulespec

CONTRACT_DIGEST = "sha256:ea9b899ba92955b83638ece811d7a4b744dd912f72e19290e32c97508674de1c"
RELEASE_VERSION = "0.2.0-pre.8"
RELEASE_URL = f"https://github.com/Formspec-Labs/rulespec/releases/tag/v{RELEASE_VERSION}"
RELEASE_ARCHIVE_URL = (
    f"https://github.com/Formspec-Labs/rulespec/archive/refs/tags/v{RELEASE_VERSION}.tar.gz"
)
CONTRACT_FILES = {
    "constraints/core/artifact.cue": b"package core\n#Artifact: {name: string}\n",
    "constraints/core/rulemaking.cue": b"package core\n#Proceeding: {name: string}\n",
    "constraints/semantics/l0-ranges.cue": b'package semantics\n"rkaf:name": "xsd:string"\n',
    "context/rkaf-context.jsonld": b'{"@context":{"rkaf":"https://rulespec.org/ns/v1#"}}\n',
}


def _write_declaration(
    path: Path,
    *,
    digest: str = CONTRACT_DIGEST,
    release: str | None = RELEASE_VERSION,
    release_url: str | None = RELEASE_URL,
) -> None:
    path.write_text(
        "\n".join(
            (
                f'rulespec_version: "{digest}"',
                f"rulespec_release: {release or 'null'}",
                f"rulespec_release_url: {release_url or 'null'}",
                "declared_levels: [L0]",
                "results:",
                "  L0: pass",
                "",
            )
        ),
        encoding="utf-8",
    )


def _contract_digest(files: dict[str, bytes] = CONTRACT_FILES) -> str:
    digest = sha256()
    for name in sorted(files):
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update(files[name])
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


def _release_archive(files: dict[str, bytes] = CONTRACT_FILES) -> bytes:
    payload = BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as archive:
        for name, contents in files.items():
            member = tarfile.TarInfo(f"rulespec-v{RELEASE_VERSION}/{name}")
            member.size = len(contents)
            archive.addfile(member, BytesIO(contents))
    return payload.getvalue()


def _release_client(
    *,
    archive: bytes = b"",
    release_status: int = 200,
    archive_status: int = 200,
) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD" and str(request.url) == RELEASE_URL:
            return httpx.Response(release_status)
        if request.method == "GET" and str(request.url) == RELEASE_ARCHIVE_URL:
            return httpx.Response(archive_status, content=archive)
        return httpx.Response(404)

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_release_declaration_accepts_matching_release_and_digest(tmp_path: Path) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(declaration)

    require_released_rulespec(declaration, verify_reachable=False)


def test_release_declaration_rejects_unreleased_candidate(tmp_path: Path) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(declaration, release=None, release_url=None)

    with pytest.raises(RuntimeError, match="does not pin a released semantic version"):
        require_released_rulespec(declaration, verify_reachable=False)


def test_release_declaration_rejects_url_for_another_version(tmp_path: Path) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(
        declaration,
        release_url="https://github.com/Formspec-Labs/rulespec/releases/tag/v0.2.0-pre.9",
    )

    with pytest.raises(RuntimeError, match="Rulespec release URL must be"):
        require_released_rulespec(declaration, verify_reachable=False)


def test_release_declaration_verifies_tagged_contract_digest(tmp_path: Path) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(declaration, digest=_contract_digest())

    with _release_client(archive=_release_archive()) as client:
        require_released_rulespec(declaration, client=client)


def test_release_declaration_rejects_unreachable_release(tmp_path: Path) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(declaration)

    with _release_client(release_status=404) as client:
        with pytest.raises(RuntimeError, match="pinned Rulespec release is not reachable"):
            require_released_rulespec(declaration, client=client)


def test_release_declaration_rejects_unreachable_tag_archive(tmp_path: Path) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(declaration)

    with _release_client(archive_status=404) as client:
        with pytest.raises(RuntimeError, match="Rulespec tag archive is not reachable"):
            require_released_rulespec(declaration, client=client)


def test_release_declaration_rejects_mismatched_tagged_contract(tmp_path: Path) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(declaration, digest=f"sha256:{'0' * 64}")

    with _release_client(archive=_release_archive()) as client:
        with pytest.raises(RuntimeError, match="does not match tagged Rulespec contract digest"):
            require_released_rulespec(declaration, client=client)


def test_release_declaration_rejects_incomplete_tag_archive(tmp_path: Path) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(declaration)
    incomplete = {
        name: contents
        for name, contents in CONTRACT_FILES.items()
        if name != "context/rkaf-context.jsonld"
    }

    with _release_client(archive=_release_archive(incomplete)) as client:
        with pytest.raises(RuntimeError, match="release archive is incomplete"):
            require_released_rulespec(declaration, client=client)


def test_release_declaration_rejects_invalid_tag_archive(tmp_path: Path) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(declaration)

    with _release_client(archive=b"not a tar archive") as client:
        with pytest.raises(RuntimeError, match="Invalid Rulespec release archive"):
            require_released_rulespec(declaration, client=client)


def test_release_declaration_rejects_oversize_contract_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(declaration)
    monkeypatch.setattr(rulespec_release, "_MAX_CONTRACT_BYTES", 8)

    with _release_client(archive=_release_archive()) as client:
        with pytest.raises(RuntimeError, match="contract files exceed the publication preflight size limit"):
            require_released_rulespec(declaration, client=client)


def test_release_declaration_rejects_oversize_tag_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    declaration = tmp_path / "rulespec-l0.yaml"
    _write_declaration(declaration)
    monkeypatch.setattr(rulespec_release, "_MAX_ARCHIVE_BYTES", 8)

    with _release_client(archive=b"123456789") as client:
        with pytest.raises(RuntimeError, match="archive exceeds the publication preflight size limit"):
            require_released_rulespec(declaration, client=client)
