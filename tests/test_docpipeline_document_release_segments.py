"""The ``DocumentRelease`` → ``structure-overlap-1800`` segmenter adapter.

The v2 file pipeline seals :class:`StructuralPassage` records over one exact
:class:`TextRepresentation`; the measured segmenter consumes a
:class:`SourceArtifact` and its processing-region stream. These tests pin the
bridge between them, and the one property the bridge exists to keep: every
produced segment slice maps back to exact release bytes.

Fixtures come from the same factory the v2 emission gate uses
(``tests/test_document_file_pipeline.py``), never from an invented shape.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

import spicy_regs.docpipeline.document_release_segments as adapter_module
from spicy_regs.docpipeline.document_release_segments import (
    ADAPTER_STEP,
    ADAPTER_VERSION,
    JOIN_SEPARATOR,
    MODEL_INPUT_FORMAT_VERSION,
    RECEIPT_NAME,
    SELECTED_SETTINGS_ID,
    SUPPORTED_FORMAT_VERSIONS,
    DocumentReleaseSegmentError,
    ModelInputWriteError,
    PassageBindingError,
    PassageBoundaryError,
    PassageCoverageMismatchError,
    PassageDigestMismatchError,
    ReleaseSealError,
    ReleaseValidationError,
    UnknownFormatVersionError,
    UnsupportedCoordinateSystemError,
    adapt_document_release,
    check_release_reversibility,
    main,
    model_input_file_stem,
    segment_document_release,
    settings_id,
    write_model_input_segments,
)
from spicy_regs.docpipeline.segments import (
    BOUNDARY_METHOD,
    EXCLUDED_NOT_ELIGIBLE,
    SELECTED_MAX_TOKENS,
    SELECTED_MIN_TOKENS,
    SELECTED_OVERLAP_TOKENS,
    SELECTED_POLICY,
    SELECTED_TOKENIZER,
    SegmentSettings,
)
from spicy_regs.docpipeline.source import (
    check_region_coordinates,
    check_region_digests,
    processing_regions,
)
from spicy_regs.document_file_pipeline import (
    DEFAULT_FILE_MANIFEST_PATH,
    build_document_release_from_file_manifest,
    publish_document_release_from_file_manifest,
)
from spicy_regs.document_release import (
    DEFAULT_FIXTURE_PATH,
    build_document_release,
    canonical_digest,
    seal_document_release,
)
from spicy_regs.ontology.segmentation import TiktokenCounter


REPRESENTATIVE_MARKUP_MANIFEST = Path("sample-data/document-files/document-release-representative-manifest-v1.json")


class _CharacterCounter:
    """One character is one token, so every budget below is exact."""

    name = "character-test"
    version = "1"

    @staticmethod
    def count(text: str) -> int:
        return len(text)


COUNTER = _CharacterCounter()


def _settings(**overrides: Any) -> SegmentSettings:
    base: dict[str, Any] = {
        "max_tokens": 16_000,
        "min_tokens": 800,
        "overlap_tokens": 100,
        "tokenizer": COUNTER.name,
        "tokenizer_version": COUNTER.version,
    }
    return SegmentSettings(**{**base, **overrides})


@pytest.fixture(scope="module")
def release() -> dict[str, Any]:
    """The checked-in v2 actual-file release: one PDF text, four sealed passages."""
    return build_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH)


@pytest.fixture(scope="module")
def markup_release() -> dict[str, Any]:
    """The checked-in v2 markup release: two documents, two sealed texts."""
    return build_document_release_from_file_manifest(REPRESENTATIVE_MARKUP_MANIFEST)


def _reseal(release: Mapping[str, Any], mutate: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
    body = json.loads(
        json.dumps({key: value for key, value in release.items() if key not in {"release_id", "release_digest"}})
    )
    mutate(body)
    return seal_document_release(body)


def _representation(release: Mapping[str, Any]) -> dict[str, Any]:
    return dict(release["text_representations"][0])


# --------------------------------------------------------------------------
# adaptation
# --------------------------------------------------------------------------


def test_each_sealed_text_representation_becomes_one_source_artifact(release: dict[str, Any]) -> None:
    adapted = adapt_document_release(release)

    assert len(adapted) == len(release["text_representations"])
    one = adapted[0]
    representation = _representation(release)
    assert one.representation_id == representation["representation_id"]
    assert one.representation_digest == representation["text_digest"]
    assert one.document_version_ref == representation["document_version_ref"]
    assert one.artifact.artifact_id == representation["artifact_projection"]["artifact_id"]
    assert list(one.artifact.raw_fields.values()) == [representation["unicode_text"]]
    assert one.text == representation["unicode_text"]


def test_the_adapted_artifact_satisfies_the_source_step_own_region_invariants(
    release: dict[str, Any],
) -> None:
    for one in adapt_document_release(release):
        check_region_coordinates(one.artifact)
        check_region_digests(one.artifact)


def test_the_processing_stream_is_exactly_the_sealed_structural_passages(
    release: dict[str, Any],
) -> None:
    one = adapt_document_release(release)[0]
    sealed = sorted(
        (item for item in release["structural_passages"]),
        key=lambda item: int(item["start"]),
    )

    stream = processing_regions(one.artifact)

    assert [(region.start_char, region.end_char) for region in stream] == [
        (int(item["start"]), int(item["end"])) for item in sealed
    ]
    assert [one.passage_for_region(region.region_id).passage_id for region in stream] == [
        item["passage_id"] for item in sealed
    ]


def test_text_outside_a_sealed_passage_is_excluded_and_never_uncovered(
    release: dict[str, Any],
) -> None:
    outcomes = segment_document_release(release, settings=_settings(), counter=COUNTER)
    outcome = outcomes[0].outcome
    sealed_gaps = [
        (int(region["start"]), int(region["end"]))
        for coverage in release["passage_coverage"]
        for region in coverage["regions"]
        if region["state"] == "excluded"
    ]

    assert [(item.start_char, item.end_char) for item in outcome.excluded] == sealed_gaps
    assert {item.reason for item in outcome.excluded} == {EXCLUDED_NOT_ELIGIBLE}
    assert outcome.coverage.uncovered_chars == 0
    assert outcome.coverage.excluded_chars == sum(end - start for start, end in sealed_gaps)


# --------------------------------------------------------------------------
# the frozen settings, unchanged
# --------------------------------------------------------------------------


def test_the_settings_id_names_the_frozen_selected_configuration() -> None:
    counter = TiktokenCounter()
    settings = SegmentSettings.selected(tokenizer_version=counter.version)

    assert settings_id(settings) == SELECTED_SETTINGS_ID == "structure-overlap-1800"
    assert (settings.policy, settings.max_tokens) == (SELECTED_POLICY, SELECTED_MAX_TOKENS)
    assert (settings.min_tokens, settings.overlap_tokens) == (SELECTED_MIN_TOKENS, SELECTED_OVERLAP_TOKENS)
    assert settings.tokenizer == SELECTED_TOKENIZER
    assert settings.boundary_method == BOUNDARY_METHOD


def test_a_sealed_release_segments_under_the_frozen_selected_settings(release: dict[str, Any]) -> None:
    counter = TiktokenCounter()
    settings = SegmentSettings.selected(tokenizer_version=counter.version)

    results = segment_document_release(release, settings=settings, counter=counter)

    assert [result.outcome.state for result in results] == ["completed"]
    outcome = results[0].outcome
    assert outcome.segments
    assert outcome.settings.digest == settings.digest
    assert outcome.coverage.uncovered_chars == 0
    assert all(segment.token_count <= SELECTED_MAX_TOKENS for segment in outcome.segments)
    check_release_reversibility(release, results)


# --------------------------------------------------------------------------
# reversible offsets
# --------------------------------------------------------------------------


def test_every_segment_slice_maps_back_to_exact_release_bytes(release: dict[str, Any]) -> None:
    results = segment_document_release(release, settings=_settings(), counter=COUNTER)
    exact = _representation(release)["unicode_text"]

    slices = [one for result in results for segment in result.outcome.segments for one in segment.slices]

    assert slices
    for one in slices:
        assert one.text == exact[one.start_char : one.end_char]
        assert one.text_sha256 == hashlib.sha256(one.text.encode()).hexdigest()


def test_a_segment_joining_two_passages_keeps_each_slice_byte_exact(release: dict[str, Any]) -> None:
    results = segment_document_release(release, settings=_settings(max_tokens=16_000), counter=COUNTER)
    outcome = results[0].outcome
    exact = _representation(release)["unicode_text"]

    joined = [segment for segment in outcome.segments if len(segment.slices) > 1]

    assert joined, "the packing budget should have joined at least two whole passages"
    for segment in joined:
        passages = {results[0].passage_for_region(one.region_id).passage_id for one in segment.slices}
        assert len(passages) == len(segment.slices)
        # The join skips the sealed gap between passages, so the segment text is
        # not one contiguous source slice — but every slice still is.
        assert segment.text != exact[segment.slices[0].start_char : segment.slices[-1].end_char]
        for one in segment.slices:
            assert one.text == exact[one.start_char : one.end_char]


def test_a_split_passage_leaf_and_its_overlap_map_back_to_exact_bytes(release: dict[str, Any]) -> None:
    results = segment_document_release(
        release,
        settings=_settings(max_tokens=2_000, min_tokens=800, overlap_tokens=100),
        counter=COUNTER,
    )
    outcome = results[0].outcome
    exact = _representation(release)["unicode_text"]

    overlapped = [one for segment in outcome.segments for one in segment.slices if one.overlap_chars]

    assert overlapped, "a passage over the budget should have produced overlapping leaves"
    for one in overlapped:
        assert one.text == exact[one.start_char : one.end_char]
        assert one.overlap_chars <= 100
    assert outcome.coverage.uncovered_chars == 0
    check_release_reversibility(release, results)


def test_a_split_leaf_stays_inside_the_sealed_passage_that_owns_it(release: dict[str, Any]) -> None:
    results = segment_document_release(
        release,
        settings=_settings(max_tokens=2_000, min_tokens=800, overlap_tokens=100),
        counter=COUNTER,
    )
    result = results[0]

    for segment in result.outcome.segments:
        for one in segment.slices:
            passage = result.passage_for_region(one.region_id)
            assert passage.start <= one.start_char <= one.end_char <= passage.end


def test_a_two_document_release_adapts_to_one_artifact_per_representation(
    markup_release: dict[str, Any],
) -> None:
    adapted = adapt_document_release(markup_release)

    assert len(adapted) == len(markup_release["text_representations"]) == 2
    assert len({one.artifact.artifact_id for one in adapted}) == 2
    assert len({one.document_version_ref for one in adapted}) == 2
    results = segment_document_release(markup_release, settings=_settings(), counter=COUNTER)
    check_release_reversibility(markup_release, results)


# --------------------------------------------------------------------------
# fail closed
# --------------------------------------------------------------------------


def test_the_adapter_refuses_an_unknown_format_version(release: dict[str, Any]) -> None:
    assert "spicyregs-document-release/v3" not in SUPPORTED_FORMAT_VERSIONS
    tampered = _reseal(release, lambda body: body.__setitem__("format_version", "spicyregs-document-release/v3"))

    with pytest.raises(UnknownFormatVersionError):
        adapt_document_release(tampered)


def test_the_adapter_refuses_a_release_whose_seal_does_not_cover_its_body(
    release: dict[str, Any],
) -> None:
    tampered = dict(release)
    tampered["released_at"] = "1999-01-01"

    with pytest.raises(ReleaseSealError):
        adapt_document_release(tampered)


def test_the_adapter_refuses_a_passage_digest_that_does_not_cover_the_exact_text(
    release: dict[str, Any],
) -> None:
    def mutate(body: dict[str, Any]) -> None:
        body["structural_passages"][0]["selected_text_digest"] = "sha256:" + "0" * 64

    with pytest.raises(PassageDigestMismatchError):
        adapt_document_release(_reseal(release, mutate))


def test_the_adapter_refuses_a_representation_digest_that_does_not_cover_its_text(
    release: dict[str, Any],
) -> None:
    def mutate(body: dict[str, Any]) -> None:
        body["text_representations"][0]["unicode_text"] = body["text_representations"][0]["unicode_text"] + "tampered"

    with pytest.raises(PassageDigestMismatchError):
        adapt_document_release(_reseal(release, mutate))


def test_the_adapter_refuses_a_passage_that_names_another_representation_digest(
    release: dict[str, Any],
) -> None:
    def mutate(body: dict[str, Any]) -> None:
        body["structural_passages"][0]["representation_digest"] = "sha256:" + "1" * 64

    with pytest.raises(PassageDigestMismatchError):
        adapt_document_release(_reseal(release, mutate))


def test_the_adapter_refuses_overlapping_passages(release: dict[str, Any]) -> None:
    def mutate(body: dict[str, Any]) -> None:
        passages = sorted(body["structural_passages"], key=lambda item: int(item["start"]))
        text = body["text_representations"][0]["unicode_text"]
        second = passages[1]
        second["start"] = int(passages[0]["end"]) - 10
        second["selected_text_digest"] = (
            "sha256:" + hashlib.sha256(text[int(second["start"]) : int(second["end"])].encode("utf-8")).hexdigest()
        )

    with pytest.raises(PassageBoundaryError):
        adapt_document_release(_reseal(release, mutate))


def test_the_adapter_refuses_a_passage_that_leaves_the_representation(
    release: dict[str, Any],
) -> None:
    def mutate(body: dict[str, Any]) -> None:
        passages = sorted(body["structural_passages"], key=lambda item: int(item["start"]))
        passages[-1]["end"] = len(body["text_representations"][0]["unicode_text"]) + 5

    with pytest.raises(PassageBoundaryError):
        adapt_document_release(_reseal(release, mutate))


def test_the_adapter_refuses_a_passage_coverage_record_that_hides_a_gap(
    release: dict[str, Any],
) -> None:
    def mutate(body: dict[str, Any]) -> None:
        regions = body["passage_coverage"][0]["regions"]
        body["passage_coverage"][0]["regions"] = [region for region in regions if region["state"] != "excluded"]

    with pytest.raises(PassageCoverageMismatchError):
        adapt_document_release(_reseal(release, mutate))


def test_the_adapter_refuses_an_unknown_coordinate_system(release: dict[str, Any]) -> None:
    def mutate(body: dict[str, Any]) -> None:
        body["text_representations"][0]["coordinate_system"] = "source-bytes"

    with pytest.raises(UnsupportedCoordinateSystemError):
        adapt_document_release(_reseal(release, mutate))


def test_the_adapter_refuses_a_passage_that_names_an_unknown_representation(
    release: dict[str, Any],
) -> None:
    def mutate(body: dict[str, Any]) -> None:
        body["structural_passages"][0]["text_representation_ref"] = "urn:spicy-regs:text-representation:" + "2" * 64

    with pytest.raises(PassageBindingError):
        adapt_document_release(_reseal(release, mutate))


def test_the_adapter_refuses_a_fragment_projection_that_names_another_artifact(
    release: dict[str, Any],
) -> None:
    def mutate(body: dict[str, Any]) -> None:
        projection = body["structural_passages"][0]["source_fragment_projection"]
        projection["source_artifact_ref"] = "urn:spicy-regs:artifact:" + "3" * 64

    with pytest.raises(PassageBindingError):
        adapt_document_release(_reseal(release, mutate))


def test_every_typed_refusal_is_one_adapter_error() -> None:
    for error in (
        ReleaseSealError,
        UnknownFormatVersionError,
        UnsupportedCoordinateSystemError,
        PassageBindingError,
        PassageDigestMismatchError,
        PassageBoundaryError,
        PassageCoverageMismatchError,
        ModelInputWriteError,
    ):
        assert issubclass(error, DocumentReleaseSegmentError)


# --------------------------------------------------------------------------
# the model-input files and their receipt
# --------------------------------------------------------------------------


def test_writing_model_input_segments_emits_files_and_a_sealed_receipt(release: dict[str, Any], tmp_path: Path) -> None:
    counter = TiktokenCounter()
    settings = SegmentSettings.selected(tokenizer_version=counter.version)

    receipt = write_model_input_segments(
        release,
        tmp_path / "model-input",
        settings=settings,
        counter=counter,
    )

    assert receipt["step"] == ADAPTER_STEP
    assert receipt["adapter_version"] == ADAPTER_VERSION
    assert receipt["final_state"] == "pass"
    assert receipt["settings"]["settings_id"] == SELECTED_SETTINGS_ID
    assert receipt["settings"]["settings_sha256"] == settings.digest
    assert receipt["release"]["release_id"] == release["release_id"]
    assert receipt["release"]["release_digest"] == release["release_digest"]
    assert receipt["release"]["format_version"] == release["format_version"]
    assert receipt["inputs"][0]["representation_digest"] == _representation(release)["text_digest"]
    assert receipt["counts"]["representation_count"] == 1
    assert receipt["counts"]["passage_count"] == len(release["structural_passages"])
    assert receipt["counts"]["segment_count"] >= 1
    assert receipt["counts"]["uncovered_chars"] == 0

    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    assert (
        receipt["receipt_sha256"]
        == hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ).hexdigest()
    )

    written = json.loads((tmp_path / "model-input" / RECEIPT_NAME).read_text(encoding="utf-8"))
    assert written == receipt
    for output in receipt["outputs"]:
        target = tmp_path / "model-input" / output["path"]
        assert hashlib.sha256(target.read_bytes()).hexdigest() == output["sha256"]


def test_a_model_input_file_carries_the_release_lineage_of_every_slice(release: dict[str, Any], tmp_path: Path) -> None:
    receipt = write_model_input_segments(
        release,
        tmp_path / "model-input",
        settings=_settings(),
        counter=COUNTER,
    )
    document = json.loads((tmp_path / "model-input" / receipt["outputs"][0]["path"]).read_text(encoding="utf-8"))
    exact = _representation(release)["unicode_text"]
    passages = {item["passage_id"]: item for item in release["structural_passages"]}

    assert document["format_version"] == MODEL_INPUT_FORMAT_VERSION
    assert document["settings"]["settings_id"] == settings_id(_settings())
    assert document["release"]["release_digest"] == release["release_digest"]
    assert document["text_representation"]["representation_id"] == _representation(release)["representation_id"]
    assert document["segments"]

    for segment in document["segments"]:
        for one in segment["slices"]:
            passage = passages[one["passage_id"]]
            assert one["fragment_id"] == passage["source_fragment_projection"]["fragment_id"]
            assert exact[one["start"] : one["end"]] == one["text"]
            assert one["text_sha256"] == hashlib.sha256(one["text"].encode()).hexdigest()
            assert int(passage["start"]) <= one["start"] <= one["end"] <= int(passage["end"])


def test_writing_the_same_release_twice_is_byte_identical(release: dict[str, Any], tmp_path: Path) -> None:
    first = write_model_input_segments(release, tmp_path / "one", settings=_settings(), counter=COUNTER)
    second = write_model_input_segments(release, tmp_path / "two", settings=_settings(), counter=COUNTER)

    assert first == second
    for output in first["outputs"]:
        assert (tmp_path / "one" / output["path"]).read_bytes() == (tmp_path / "two" / output["path"]).read_bytes()


def test_writing_refuses_an_output_directory_that_already_holds_work(release: dict[str, Any], tmp_path: Path) -> None:
    target = tmp_path / "model-input"
    target.mkdir()
    (target / "leftover.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ModelInputWriteError):
        write_model_input_segments(release, target, settings=_settings(), counter=COUNTER)


def test_the_tool_entry_reads_a_release_path_and_writes_a_pass_receipt(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    distribution = tmp_path / "distribution"
    publish_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH, distribution)

    exit_code = main(
        [
            "--release",
            str(distribution / "document-release.json"),
            "--output-dir",
            str(tmp_path / "model-input"),
        ]
    )

    assert exit_code == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["status"] == "pass"
    assert printed["settings_id"] == SELECTED_SETTINGS_ID
    receipt = json.loads((tmp_path / "model-input" / RECEIPT_NAME).read_text(encoding="utf-8"))
    assert (
        receipt["release"]["release_path_sha256"]
        == hashlib.sha256((distribution / "document-release.json").read_bytes()).hexdigest()
    )
    assert printed["receipt_sha256"] == receipt["receipt_sha256"]


# --------------------------------------------------------------------------
# the release contract itself: the validator closes what this adapter cannot see
# --------------------------------------------------------------------------


def test_a_sealed_but_invalid_release_is_refused_by_the_release_validator(
    release: dict[str, Any],
) -> None:
    """Four shapes this adapter's own checks accept and the release contract does not.

    Each corrupts something at the Rulespec seam: ambiguous evidence resolution
    keys, a fragment selector that cites the wrong span, colliding output file
    names, and coverage that points at a passage nobody sealed.
    """

    def duplicate_passage_id(body: dict[str, Any]) -> None:
        passages = sorted(body["structural_passages"], key=lambda item: int(item["start"]))
        passages[1]["passage_id"] = passages[0]["passage_id"]
        body["structural_passages"] = sorted(passages, key=lambda item: str(item["passage_id"]))

    def fragment_selector_mismatch(body: dict[str, Any]) -> None:
        projection = body["structural_passages"][0]["source_fragment_projection"]
        projection["selector"] = {
            "coordinate_system": "unicode-codepoints-half-open",
            "end": 42,
            "selector_type": "TextPositionSelector",
            "start": 0,
        }

    def duplicate_representation_id(body: dict[str, Any]) -> None:
        representation = json.loads(json.dumps(body["text_representations"][0]))
        body["text_representations"] = [representation, representation]

    def dangling_coverage_passage_ref(body: dict[str, Any]) -> None:
        for region in body["passage_coverage"][0]["regions"]:
            if region["state"] == "processed":
                region["passage_ref"] = "urn:spicy-regs:structural-passage:" + "4" * 64
                return

    for mutate in (
        duplicate_passage_id,
        fragment_selector_mismatch,
        duplicate_representation_id,
        dangling_coverage_passage_ref,
    ):
        with pytest.raises(ReleaseValidationError):
            adapt_document_release(_reseal(release, mutate))


def test_the_checked_in_release_passes_the_validator_unchanged(release: dict[str, Any]) -> None:
    assert adapt_document_release(release)


# --------------------------------------------------------------------------
# file names may not escape the output directory
# --------------------------------------------------------------------------


def test_a_representation_id_that_would_escape_the_output_directory_is_hashed() -> None:
    hostile = "urn:spicy-regs:text-representation:../../escaped"

    stem = model_input_file_stem(hostile)

    assert "/" not in stem and ".." not in stem
    assert stem == hashlib.sha256(hostile.encode("utf-8")).hexdigest()
    assert model_input_file_stem("urn:spicy-regs:text-representation:" + "a" * 64) == "a" * 64


def test_a_hostile_representation_id_writes_inside_the_output_directory(tmp_path: Path) -> None:
    hostile = "urn:spicy-regs:text-representation:../../escaped"
    stem = model_input_file_stem(hostile)

    target = adapter_module._contained_output_path(tmp_path / "out", f"segments/{stem}.json")

    assert target.resolve().is_relative_to((tmp_path / "out").resolve())
    with pytest.raises(ModelInputWriteError):
        adapter_module._contained_output_path(tmp_path / "out", "segments/../../escaped.json")


# --------------------------------------------------------------------------
# the receipt does not depend on where it was run
# --------------------------------------------------------------------------


def test_the_receipt_is_identical_from_two_working_directories(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    distribution = tmp_path / "distribution"
    publish_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH, distribution)
    release_path = distribution / "document-release.json"

    first_home = tmp_path / "run-one"
    second_home = tmp_path / "run-two"
    first_home.mkdir()
    second_home.mkdir()

    monkeypatch.chdir(first_home)
    first = adapter_module.segment_release_path(
        release_path, first_home / "model-input", settings=_settings(), counter=COUNTER
    )
    monkeypatch.chdir(second_home)
    second = adapter_module.segment_release_path(
        release_path, second_home / "model-input", settings=_settings(), counter=COUNTER
    )

    assert first == second
    assert first["receipt_sha256"] == second["receipt_sha256"]
    assert "/" not in first["release"]["release_path"] or not Path(first["release"]["release_path"]).is_absolute()


# --------------------------------------------------------------------------
# secrets never reach a published model input
# --------------------------------------------------------------------------


SECRET_TOKEN = "sk-proj-AAAABBBBCCCCDDDDEEEEFFFF"


def _secret_bearing_release() -> dict[str, Any]:
    """A real M1 fixture release whose sealed passage text carries a fake key.

    The token goes inside a passage the fixture seals end to end, so it reaches a
    published model input rather than landing in an excluded span.
    """
    fixture = json.loads(DEFAULT_FIXTURE_PATH.read_text(encoding="utf-8"))
    for record in fixture["records"]:
        for passage in record.get("passages") or []:
            if passage.get("representation_path") == "title" and passage.get("end") == "full":
                record["content"]["title"] += f" contact {SECRET_TOKEN}"
                passage["expected_text"] = record["content"]["title"]
                fixture["fixture_digest"] = canonical_digest(
                    {key: value for key, value in fixture.items() if key != "fixture_digest"}
                )
                return build_document_release(fixture)
    raise AssertionError("the M1 fixture no longer seals a whole title passage")


def test_secret_like_content_in_a_sealed_release_reaches_the_artifact_scan() -> None:
    adapted = adapt_document_release(_secret_bearing_release())

    assert any(one.artifact.secret_rules for one in adapted)
    assert "openai-project-key" in {rule for one in adapted for rule in one.artifact.secret_rules}


def test_a_clean_release_claims_no_secret_rules(release: dict[str, Any]) -> None:
    assert all(one.artifact.secret_rules == () for one in adapt_document_release(release))


def test_publication_refuses_when_a_model_input_file_would_carry_a_secret(
    tmp_path: Path,
) -> None:
    with pytest.raises(ModelInputWriteError) as caught:
        write_model_input_segments(
            _secret_bearing_release(),
            tmp_path / "model-input",
            settings=_settings(),
            counter=COUNTER,
        )

    assert "openai-project-key" in str(caught.value)
    assert not (tmp_path / "model-input" / RECEIPT_NAME).exists()


# --------------------------------------------------------------------------
# every refusal is typed
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field",
    ["evidence_grade", "passage_policy_version", "selected_text_digest", "start", "end"],
)
def test_a_passage_missing_a_required_field_refuses_with_a_typed_error(release: dict[str, Any], field: str) -> None:
    def mutate(body: dict[str, Any]) -> None:
        del body["structural_passages"][0][field]

    with pytest.raises(DocumentReleaseSegmentError):
        adapt_document_release(_reseal(release, mutate))


def test_a_fragment_projection_missing_its_identifier_refuses_with_a_typed_error(
    release: dict[str, Any],
) -> None:
    def mutate(body: dict[str, Any]) -> None:
        del body["structural_passages"][0]["source_fragment_projection"]["fragment_id"]

    with pytest.raises(DocumentReleaseSegmentError):
        adapt_document_release(_reseal(release, mutate))


def test_an_artifact_projection_missing_its_identifier_refuses_with_a_typed_error(
    release: dict[str, Any],
) -> None:
    def mutate(body: dict[str, Any]) -> None:
        del body["text_representations"][0]["artifact_projection"]["artifact_id"]

    with pytest.raises(DocumentReleaseSegmentError):
        adapt_document_release(_reseal(release, mutate))


# --------------------------------------------------------------------------
# a receipt that can say no
# --------------------------------------------------------------------------


def test_a_release_that_yields_no_segments_is_a_failing_receipt(
    release: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(adapter_module, "segment_document_release", lambda *a, **k: ())

    receipt = write_model_input_segments(release, tmp_path / "model-input", settings=_settings(), counter=COUNTER)

    assert receipt["final_state"] == "fail"
    assert receipt["failures"]
    assert receipt["counts"]["segment_count"] == 0


def test_the_tool_entry_exits_nonzero_on_a_failing_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    distribution = tmp_path / "distribution"
    publish_document_release_from_file_manifest(DEFAULT_FILE_MANIFEST_PATH, distribution)
    monkeypatch.setattr(adapter_module, "segment_document_release", lambda *a, **k: ())

    exit_code = main(
        [
            "--release",
            str(distribution / "document-release.json"),
            "--output-dir",
            str(tmp_path / "model-input"),
        ]
    )

    assert exit_code == 1
    assert json.loads(capsys.readouterr().out)["status"] == "fail"


# --------------------------------------------------------------------------
# the reversibility guard proved as a guard
# --------------------------------------------------------------------------


def _drift(result: Any, **slice_changes: Any) -> Any:
    from dataclasses import replace

    segment = result.outcome.segments[0]
    drifted = replace(segment.slices[0], **slice_changes)
    segment = replace(segment, slices=(drifted, *segment.slices[1:]))
    outcome = replace(result.outcome, segments=(segment, *result.outcome.segments[1:]))
    return replace(result, outcome=outcome)


def test_the_reversibility_guard_refuses_a_slice_whose_text_drifted(
    release: dict[str, Any],
) -> None:
    """Self-consistent drift: the slice agrees with its own digest and lies anyway."""
    result = segment_document_release(release, settings=_settings(), counter=COUNTER)[0]
    forged = "not the release text"
    drifted = _drift(
        result,
        text=forged,
        text_sha256=hashlib.sha256(forged.encode()).hexdigest(),
    )

    with pytest.raises(PassageDigestMismatchError):
        check_release_reversibility(release, [drifted])


def test_the_reversibility_guard_refuses_a_slice_whose_digest_drifted(
    release: dict[str, Any],
) -> None:
    result = segment_document_release(release, settings=_settings(), counter=COUNTER)[0]
    drifted = _drift(result, text_sha256="0" * 64)

    with pytest.raises(PassageDigestMismatchError):
        check_release_reversibility(release, [drifted])


def test_the_reversibility_guard_refuses_a_slice_outside_its_sealed_passage(
    release: dict[str, Any],
) -> None:
    result = segment_document_release(release, settings=_settings(), counter=COUNTER)[0]
    binding = result.adapted.bindings[0]
    exact = _representation(release)["unicode_text"]
    beyond = binding.end + 20
    drifted = _drift(
        result,
        start_char=binding.start,
        end_char=beyond,
        text=exact[binding.start : beyond],
        text_sha256=hashlib.sha256(exact[binding.start : beyond].encode()).hexdigest(),
    )

    with pytest.raises(PassageBoundaryError):
        check_release_reversibility(release, [drifted])


# --------------------------------------------------------------------------
# mutations the first round left alive
# --------------------------------------------------------------------------


def test_a_representation_digest_lie_is_caught_even_when_every_passage_agrees_with_it(
    release: dict[str, Any],
) -> None:
    """A self-consistent lie: only the text-covering check can see it."""
    lie = "sha256:" + hashlib.sha256(b"another document entirely").hexdigest()

    def mutate(body: dict[str, Any]) -> None:
        representation = body["text_representations"][0]
        representation["text_digest"] = lie
        representation["artifact_projection"]["content_digest"] = lie
        for passage in body["structural_passages"]:
            passage["representation_digest"] = lie
            passage["source_fragment_projection"]["source_artifact_digest"] = lie

    with pytest.raises(PassageDigestMismatchError):
        adapt_document_release(_reseal(release, mutate))


def test_the_settings_id_is_derived_from_the_settings_not_the_frozen_constant() -> None:
    other = _settings(max_tokens=512, min_tokens=128, overlap_tokens=16)

    assert settings_id(other) == "structure-overlap-512"
    assert settings_id(other) != SELECTED_SETTINGS_ID


def test_field_coverage_is_derived_from_the_tiling_it_describes(
    release: dict[str, Any],
) -> None:
    one = adapt_document_release(release)[0]
    coverage = one.artifact.coverage[0]
    exact = _representation(release)["unicode_text"]
    sealed = sum(item.end - item.start for item in one.bindings)
    excluded = sum(
        int(region["end"]) - int(region["start"])
        for item in release["passage_coverage"]
        for region in item["regions"]
        if region["state"] == "excluded"
    )

    assert coverage.field_chars == len(exact)
    assert coverage.covered_chars == sealed + excluded == len(exact)
    assert coverage.covered_chars + coverage.uncovered_chars == len(exact)
    assert coverage.uncovered_chars == 0
    assert coverage.durable_chars == sealed
    assert coverage.syntax_chars == excluded
    assert coverage.region_count == len(one.artifact.regions)
    assert coverage.fragment_count == len(one.bindings)


# --------------------------------------------------------------------------
# seam honesty: joined text is not one contiguous source run
# --------------------------------------------------------------------------


def test_a_joined_segment_declares_that_its_text_is_not_contiguous(release: dict[str, Any], tmp_path: Path) -> None:
    receipt = write_model_input_segments(
        release,
        tmp_path / "model-input",
        settings=_settings(max_tokens=16_000),
        counter=COUNTER,
    )
    document = json.loads((tmp_path / "model-input" / receipt["outputs"][0]["path"]).read_text(encoding="utf-8"))

    joined = [segment for segment in document["segments"] if len(segment["slices"]) > 1]
    single = [segment for segment in document["segments"] if len(segment["slices"]) == 1]

    assert joined
    assert all(segment["contiguous"] is False for segment in joined)
    assert all(segment["join_separator"] == JOIN_SEPARATOR == "\n" for segment in joined)
    assert all(segment["contiguous"] is True for segment in single)
    for segment in joined:
        assert JOIN_SEPARATOR.join(one["text"] for one in segment["slices"]) == segment["text"]
