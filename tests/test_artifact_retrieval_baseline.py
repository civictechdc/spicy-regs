"""Contract tests for the incumbent whole-artifact retrieval baseline."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from spicy_regs.corpora.artifact_retrieval_baseline import (
    ALL_PROFILE_MODE,
    CANDIDATE_COLUMNS,
    LEGACY_MODE,
    build_artifact_retrieval_baseline,
    validate_artifact_retrieval_baseline,
)
from spicy_regs.corpora.segmentation_evaluation import (
    build_segmentation_evaluation,
    fetch_source_cache,
)
from spicy_regs.corpora.segmentation_experiment import HashEmbeddingProvider
from spicy_regs.ontology.common import (
    read_parquet_rows,
    write_parquet_rows,
)
from tests.test_segmentation_evaluation import (
    _fake_fetch,
    _write_base,
    _write_corpus,
)


@pytest.fixture(scope="module")
def baseline_outputs(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, Path, Path]:
    root = tmp_path_factory.mktemp("artifact-retrieval")
    base = root / "base"
    corpus = root / "corpus"
    cache = root / "cache"
    evaluation = root / "evaluation"
    first = root / "baseline-one"
    second = root / "baseline-two"
    base.mkdir()
    corpus.mkdir()
    _write_base(base)
    _write_corpus(corpus)
    fetch_source_cache(cache, fetcher=_fake_fetch)
    build_segmentation_evaluation(base, corpus, cache, evaluation)
    build_artifact_retrieval_baseline(
        evaluation,
        first,
        embedding_provider=HashEmbeddingProvider(dimensions=32),
    )
    build_artifact_retrieval_baseline(
        evaluation,
        second,
        embedding_provider=HashEmbeddingProvider(dimensions=32),
    )
    return evaluation, first, second


def test_whole_artifact_baseline_is_complete_and_byte_deterministic(
    baseline_outputs: tuple[Path, Path, Path],
):
    evaluation, first, second = baseline_outputs
    receipt = json.loads(
        (first / "artifact-retrieval-receipt.json").read_text(
            encoding="utf-8"
        )
    )
    dataset_receipt = json.loads(
        (evaluation / "segmentation-evaluation-receipt.json").read_text(
            encoding="utf-8"
        )
    )

    assert receipt["status"] == "pass"
    assert receipt["production_provider"] is False
    assert receipt["artifact_counts_by_mode"][ALL_PROFILE_MODE] == (
        dataset_receipt["artifact_count"]
    )
    assert 0 < receipt["artifact_counts_by_mode"][LEGACY_MODE] < (
        receipt["artifact_counts_by_mode"][ALL_PROFILE_MODE]
    )
    assert receipt["metric_row_count"] == 2
    assert validate_artifact_retrieval_baseline(
        evaluation,
        first,
    ) == receipt
    assert {
        path.relative_to(first): path.read_bytes()
        for path in first.rglob("*")
        if path.is_file()
    } == {
        path.relative_to(second): path.read_bytes()
        for path in second.rglob("*")
        if path.is_file()
    }


def test_whole_artifact_baseline_detects_candidate_tampering(
    baseline_outputs: tuple[Path, Path, Path],
    tmp_path: Path,
):
    evaluation, first, _ = baseline_outputs
    tampered = tmp_path / "tampered"
    shutil.copytree(first, tampered)
    path = tampered / "retrieval_candidates.parquet"
    rows = read_parquet_rows(path)
    rows[0]["candidate_rank"] = "999"
    write_parquet_rows(path, columns=CANDIDATE_COLUMNS, rows=rows)

    receipt = validate_artifact_retrieval_baseline(
        evaluation,
        tampered,
    )

    assert receipt["status"] == "fail"
    assert "retrieval candidates differ from stored vectors" in receipt[
        "failures"
    ]
    assert "artifact hashes differ from manifest" in receipt["failures"]
