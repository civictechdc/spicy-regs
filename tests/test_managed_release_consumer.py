"""Spicy's candidate-only adapter for immutable RefSpec releases."""

from __future__ import annotations

import hashlib
import runpy
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Callable, cast

import pytest

from refspec import ManagedReleaseView
from spicy_regs.enrichment import (
    ManagedReleaseCandidateSource,
    ManagedReleaseConsumerError,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
MEMBER_ID = "urn:rkaf:fixture:concept:income"
EXPRESSION_ID = "urn:test:indexed-expression:water:en"
DIGEST = "sha256:" + "c" * 64


def _build_bundle(root: Path) -> Path:
    support = runpy.run_path(
        str(REPO_ROOT / "RefSpec" / "tests" / "test_managed_release_view.py")
    )
    builder = cast(Callable[[Path], Path], support["build_bundle"])
    return builder(root)


def _manifest_digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def test_spicy_consumer_is_exact_read_only_and_candidate_use_only(
    tmp_path: Path,
) -> None:
    manifest_path = _build_bundle(tmp_path)
    source = ManagedReleaseCandidateSource.open(
        manifest_path,
        expected_manifest_digest=_manifest_digest(manifest_path),
        lookup_index_manifest={
            "id": "urn:test:lookup-index:subjects:v1",
            "digest": DIGEST,
        },
    )
    view = source.view

    assert source.lookup_member(MEMBER_ID) is not None
    assert source.lookup_member(MEMBER_ID.upper()) is None
    assert tuple(source.iter_expressions(member_iri=MEMBER_ID))[0].expression_id == (
        EXPRESSION_ID
    )
    assert source.usage_ceiling == "candidateUseOnly"

    with pytest.raises(TypeError):
        source.lookup_index_manifest["digest"] = DIGEST  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        source.view = view  # type: ignore[misc]
    for forbidden in ("mutate", "reconcile", "deploy", "authorize_output"):
        assert not hasattr(source, forbidden)


def test_spicy_consumer_rejects_expression_corpus_as_lookup_index(
    tmp_path: Path,
) -> None:
    manifest_path = _build_bundle(tmp_path)
    view = ManagedReleaseView.open(
        manifest_path,
        expected_manifest_digest=_manifest_digest(manifest_path),
    )

    with pytest.raises(ManagedReleaseConsumerError, match="must not reuse"):
        ManagedReleaseCandidateSource(
            view=view,
            lookup_index_manifest=dict(view.expression_corpus_snapshot),
        )
