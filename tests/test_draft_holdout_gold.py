"""Hermetic checks for holdout gold drafting: blindness, quotes, composition.

Every fixture here is synthetic and every model is injected. Nothing reads the
real drafting input, nothing reads the drafted labels, and nothing opens a
socket — so these tests state what the tool guarantees rather than what one
particular drafting run happened to produce.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DRAFT_PATH = REPO_ROOT / "tools" / "draft_holdout_gold.py"


def _load_draft_holdout_gold():
    spec = importlib.util.spec_from_file_location("draft_holdout_gold", DRAFT_PATH)
    assert spec and spec.loader, f"could not load {DRAFT_PATH}"
    module = importlib.util.module_from_spec(spec)
    # Registered before execution: ``dataclasses`` and the sibling ``tools``
    # import both resolve through ``sys.modules`` while the module body runs.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


draft_module = _load_draft_holdout_gold()

DRAFT_SCHEMA_VERSION = draft_module.DRAFT_SCHEMA_VERSION
DraftingError = draft_module.DraftingError
HoldoutBlindnessError = draft_module.HoldoutBlindnessError
InputDigestMismatchError = draft_module.InputDigestMismatchError
OutputLocationError = draft_module.OutputLocationError
TaggerFamilyError = draft_module.TaggerFamilyError
REJECTED_EMPTY_QUOTE = draft_module.REJECTED_EMPTY_QUOTE
REJECTED_EMPTY_TEXT = draft_module.REJECTED_EMPTY_TEXT
REJECTED_QUOTE_UNRESOLVED = draft_module.REJECTED_QUOTE_UNRESOLVED
REJECTED_UNKNOWN_SEGMENT = draft_module.REJECTED_UNKNOWN_SEGMENT

SEGMENT_TEXT = (
    "The Administrator finds that perfluorooctanoic acid is a hazardous substance under the Act. "
    "This section does not apply to facilities holding a valid State permit. "
    "The rule takes effect on the date of publication."
)


def make_artifact(**overrides: Any) -> dict[str, Any]:
    """One drafting-input artifact in the shape ``draw_holdout`` emits."""
    artifact = {
        "artifact_id": "artifact_synthetic_0001",
        "artifact_digest": "d" * 64,
        "extracted_text_sha256": "e" * 64,
        "profile_id": "federal-register-document-v1",
        "source_table": "federal_register",
        "subject_id": "2026-00001",
        "subject_type": "federal_register_document",
        "title": "Hazardous Substance Designation",
        "segment_count": 1,
        "segments": [
            {
                "segment_id": "processing_segment_0001",
                "ordinal": 0,
                "segment_count": 1,
                "char_count": len(SEGMENT_TEXT),
                "token_count": 40,
                "headings": [],
                "text": SEGMENT_TEXT,
                "text_sha256": "f" * 64,
                "slices": [
                    {
                        "source_field": "federal_register.body",
                        "content_layer": "body",
                        "context_only": False,
                        "start_char": 0,
                        "end_char": len(SEGMENT_TEXT),
                        "segment_start_char": 0,
                        "segment_end_char": len(SEGMENT_TEXT),
                        "char_count": len(SEGMENT_TEXT),
                        "text_sha256": "f" * 64,
                    }
                ],
            }
        ],
    }
    artifact.update(overrides)
    return artifact


def topic(**overrides: Any) -> dict[str, Any]:
    quote = "perfluorooctanoic acid"
    start = SEGMENT_TEXT.index(quote)
    entry = {
        "text": "perfluorooctanoic acid",
        "role": "primary",
        "breadth": "narrow",
        "denial": False,
        "frame": "the substance the designation names",
        "ambiguity_group": "",
        "segment_id": "processing_segment_0001",
        "quote": quote,
        "quote_start": start,
        "quote_end": start + len(quote),
    }
    entry.update(overrides)
    return entry


class FakeModel:
    """An injected drafter that records what it was asked and answers a script.

    It mirrors only the two surfaces the tool uses — ``secret_free_request`` and
    ``structured_json`` — so a change in what the tool sends shows up here as a
    changed recording rather than as a network call.
    """

    provider = "fake-provider"
    model = "fake-drafter-1"
    model_id = "fake-provider:fake-drafter-1"
    base_url_host = "fake.invalid"
    structured_mode = "response_format"
    run_configuration = {"provider_family": "openai-compatible", "transport": "openai-chat-completions"}

    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.requests: list[dict[str, Any]] = []
        self.payloads: list[dict[str, Any]] = []

    def secret_free_request(self, *, name, schema, instructions, payload, max_output_tokens):
        return {
            "model": self.model,
            "messages": [
                {"role": "system", "content": instructions},
                {"role": "user", "content": json.dumps(payload, sort_keys=True)},
            ],
            "max_tokens": max_output_tokens,
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": name, "strict": True, "schema": dict(schema)},
            },
        }

    def structured_json(self, *, name, schema, instructions, payload, max_output_tokens):
        self.payloads.append(dict(payload))
        self.requests.append(
            self.secret_free_request(
                name=name,
                schema=schema,
                instructions=instructions,
                payload=payload,
                max_output_tokens=max_output_tokens,
            )
        )
        answer = self.responses.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return _FakeResult(answer, {"response_model": "fake-drafter-1-2026-07-28", "status": "completed"})


class _FakeResult:
    def __init__(self, output: dict[str, Any], call: dict[str, Any]) -> None:
        self.output = output
        self.call = call


class _FakeCallError(RuntimeError):
    """A provider failure carrying only its receipt-safe call details."""

    def __init__(self, call: dict[str, Any]) -> None:
        super().__init__("provider failed")
        self.call = call


def response(topics: list[dict[str, Any]] | None = None, **overrides: Any) -> dict[str, Any]:
    answer = {
        "artifact_id": "artifact_synthetic_0001",
        "abstained": False,
        "topics": topics if topics is not None else [topic()],
        "not_supported": [{"text": "State permitting programs", "reason": "named only as an exclusion"}],
    }
    answer.update(overrides)
    return answer


# --------------------------------------------------------------------------
# blindness
# --------------------------------------------------------------------------


def test_payload_carries_only_allowlisted_fields() -> None:
    payload = draft_module.build_payload(make_artifact())
    assert set(payload) == draft_module.PAYLOAD_KEYS
    assert set(payload["segments"][0]) == draft_module.PAYLOAD_SEGMENT_KEYS
    # Slice provenance and corpus digests are withheld: the drafter needs the
    # text, and every field not sent is a field that cannot anchor it.
    assert "slices" not in payload["segments"][0]
    assert "artifact_digest" not in payload
    assert "subject_id" not in payload


@pytest.mark.parametrize(
    "poison",
    [
        {"candidate_concepts": [{"concept_id": "urn:x:1", "label": "PFAS"}]},
        {"registry_candidates": ["urn:x:1"]},
        {"expected_tags": ["urn:x:1"]},
        {"gold_label": "hazardous substances"},
        {"tagger_output": {"score": 0.9}},
    ],
)
def test_registry_or_tagger_fields_cannot_enter_the_payload(poison: dict[str, Any]) -> None:
    """A banned field on the artifact never reaches the drafter.

    ``build_payload`` copies by name, so an unexpected field is dropped rather
    than refused — the stronger property, since it holds even for a field nobody
    thought to ban. The blindness walk then proves the result is clean, and the
    poison's own values are checked absent from the whole outgoing body.
    """
    artifact = make_artifact(**poison)
    payload = draft_module.build_payload(artifact)
    assert set(payload) == draft_module.PAYLOAD_KEYS
    assert not set(payload) & set(poison)
    facts = draft_module.assert_call_blind(payload, draft_module.request_preview("fake", payload))
    assert facts["passed"] is True
    body = json.dumps(draft_module.request_preview("fake", payload)).casefold()
    for leaked in ("urn:x:1", "pfas", "hazardous substances", "0.9"):
        assert leaked not in body


def test_a_widened_payload_allowlist_is_still_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    """The allowlist is a guard on future edits, not decoration.

    ``build_payload`` is the only place a field can be admitted, so the test
    shrinks the allowlist to prove the check fires rather than waiting for
    someone to widen it wrongly.
    """
    monkeypatch.setattr(draft_module, "PAYLOAD_KEYS", frozenset({"artifact_id"}))
    with pytest.raises(HoldoutBlindnessError):
        draft_module.build_payload(make_artifact())


def test_banned_field_nested_in_a_segment_is_caught_by_the_blindness_walk() -> None:
    payload = draft_module.build_payload(make_artifact())
    payload["segments"][0]["concept_candidates"] = ["urn:x:1"]
    with pytest.raises(HoldoutBlindnessError):
        draft_module.assert_call_blind(payload, draft_module.request_preview("fake", payload))


def test_the_contract_itself_supplies_no_vocabulary() -> None:
    """Blindness can leak through the contract as easily as through the data.

    The response schema's own property names are walked by the same banned-key
    list that guards the payload, and the instruction text is checked for the
    words that would mean a term list had been described to the drafter.
    """
    payload = draft_module.build_payload(make_artifact())
    facts = draft_module.assert_call_blind(payload, draft_module.request_preview("fake", payload))
    assert facts["passed"] is True

    banned = draft_module.BANNED_OUTPUT_KEY_SUBSTRINGS
    schema_keys = draft_module.RESPONSE_SCHEMA["properties"]
    item_keys = schema_keys["topics"]["items"]["properties"]
    assert not [key for key in list(schema_keys) + list(item_keys) for term in banned if term in key.casefold()]

    folded = draft_module.INSTRUCTIONS.casefold()
    for term in ("candidate", "registry", "vocabulary", "scheme", "alias", "concept id", "taxonomy"):
        assert term not in folded
    assert "none exists for you to guess at" in folded


def test_the_request_actually_sent_is_proved_blind() -> None:
    model = FakeModel([response()])
    draft_module.draft_artifact(model, make_artifact())
    sent = model.payloads[0]
    assert set(sent) == draft_module.PAYLOAD_KEYS
    body = json.dumps(model.requests[0])
    for term in ("candidate", "registry", "concept_id", "scheme", "alias"):
        assert term not in body.casefold()


# --------------------------------------------------------------------------
# quote verification: rejected, never repaired
# --------------------------------------------------------------------------


def test_exact_quote_with_correct_offsets_is_accepted_as_provided() -> None:
    accepted, rejected = draft_module.verify_topics([topic()], {"processing_segment_0001": SEGMENT_TEXT})
    assert rejected == []
    assert accepted[0]["evidence_alignment_method"] == "provided-offsets"
    assert SEGMENT_TEXT[accepted[0]["quote_start"] : accepted[0]["quote_end"]] == accepted[0]["quote"]


def test_exact_quote_with_wrong_offsets_is_relocated_not_rejected() -> None:
    """Wrong offsets on an otherwise verbatim quote are located, and it shows.

    This is offset resolution, not quote repair: the text is untouched, and the
    record says the offsets came from a search rather than from the drafter.
    """
    accepted, rejected = draft_module.verify_topics(
        [topic(quote_start=0, quote_end=5)], {"processing_segment_0001": SEGMENT_TEXT}
    )
    assert rejected == []
    assert accepted[0]["evidence_alignment_method"] == "unique-exact-match"
    assert SEGMENT_TEXT[accepted[0]["quote_start"] : accepted[0]["quote_end"]] == "perfluorooctanoic acid"


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"quote": "perfluorooctanoic acids"}, REJECTED_QUOTE_UNRESOLVED),
        ({"quote": "Perfluorooctanoic Acid"}, REJECTED_QUOTE_UNRESOLVED),
        ({"quote": "perfluorooctanoic  acid"}, REJECTED_QUOTE_UNRESOLVED),
        ({"quote": "a hazardous substance ... under the Act"}, REJECTED_QUOTE_UNRESOLVED),
        ({"quote": "The"}, REJECTED_QUOTE_UNRESOLVED),  # occurs more than once: no single span is meant
        ({"quote": ""}, REJECTED_EMPTY_QUOTE),
        ({"segment_id": "processing_segment_9999"}, REJECTED_UNKNOWN_SEGMENT),
        ({"text": "   "}, REJECTED_EMPTY_TEXT),
    ],
)
def test_unverifiable_quotes_are_rejected_with_a_reason(override: dict[str, Any], reason: str) -> None:
    accepted, rejected = draft_module.verify_topics([topic(**override)], {"processing_segment_0001": SEGMENT_TEXT})
    assert accepted == []
    assert [entry["rejection_reason"] for entry in rejected] == [reason]
    # The rejected entry keeps what the drafter said, unedited, so the reason
    # can be audited without re-running the model.
    assert rejected[0]["quote"] == override.get("quote", topic()["quote"])


def test_rejection_does_not_discard_the_artifact_or_its_other_labels() -> None:
    model = FakeModel([response([topic(), topic(text="hazardous substances", quote="no such words here")])])
    record = draft_module.draft_artifact(model, make_artifact())
    assert len(record["accepted"]) == 1
    assert len(record["rejected"]) == 1
    assert record["status"] == "drafted"


# --------------------------------------------------------------------------
# guards
# --------------------------------------------------------------------------


def test_output_inside_the_repository_is_refused(tmp_path: Path) -> None:
    with pytest.raises(OutputLocationError):
        draft_module.assert_output_outside_repo(REPO_ROOT / "output" / "holdout_gold.json")
    with pytest.raises(OutputLocationError):
        draft_module.assert_output_outside_repo(REPO_ROOT / "holdout_gold.json")
    assert draft_module.assert_output_outside_repo(tmp_path / "holdout_gold.json").parent == tmp_path.resolve()


def test_a_tagger_family_drafter_is_refused() -> None:
    for model_id in ("gpt-5.6-sol", "GPT-4.1", "openai/o3-pro"):
        with pytest.raises(TaggerFamilyError):
            draft_module.assert_not_tagger_family(model_id)
    draft_module.assert_not_tagger_family("gemini-3.1-pro-preview")


def test_input_digest_mismatch_aborts_before_the_json_is_trusted(tmp_path: Path) -> None:
    path = tmp_path / "input.json"
    path.write_text(json.dumps({"schema_version": draft_module.DRAFTING_SCHEMA_VERSION, "artifacts": []}))
    with pytest.raises(InputDigestMismatchError):
        draft_module.load_drafting_input(path, "0" * 64)
    document, digest = draft_module.load_drafting_input(path, "")
    assert document["schema_version"] == draft_module.DRAFTING_SCHEMA_VERSION
    assert draft_module.load_drafting_input(path, digest)[1] == digest


def test_a_foreign_input_schema_version_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "input.json"
    path.write_text(json.dumps({"schema_version": "something-else-v9", "artifacts": []}))
    with pytest.raises(DraftingError):
        draft_module.load_drafting_input(path, "")


def test_provider_failure_is_recorded_as_data_not_raised() -> None:
    model = FakeModel([_FakeCallError({"status": "retry_exhausted", "attempt_count": 3})])
    record = draft_module.draft_artifact(model, make_artifact())
    assert record["status"] == "failed"
    assert record["error_code"] == "_FakeCallError"
    assert record["call"] == {"status": "retry_exhausted", "attempt_count": 3}
    assert record["accepted"] == [] and record["response"] is None


# --------------------------------------------------------------------------
# composition and the run record
# --------------------------------------------------------------------------


def test_composition_counts_what_the_holdout_was_required_to_contain() -> None:
    quote_denial = "does not apply to facilities holding a valid State permit"
    quote_broad = "hazardous substance under the Act"
    model = FakeModel(
        [
            response(
                [
                    topic(),
                    topic(text="hazardous substances", breadth="broad", role="substantive", quote=quote_broad),
                    topic(
                        text="permitted State facilities",
                        role="substantive",
                        denial=True,
                        quote=quote_denial,
                        quote_start=0,
                        quote_end=0,
                    ),
                    topic(text="effective-date rulemaking", role="contextual", ambiguity_group="g1"),
                    topic(
                        text="publication practice",
                        role="contextual",
                        ambiguity_group="g1",
                        quote="the date of publication",
                        quote_start=0,
                        quote_end=0,
                    ),
                ]
            )
        ]
    )
    record = draft_module.draft_artifact(model, make_artifact())
    summary = draft_module.composition_summary([record])

    assert summary["labels_accepted"] == 5
    assert summary["artifacts_multi_label"] == 1 and summary["artifacts_single_label"] == 0
    assert summary["denial_count"] == 1
    assert summary["negative_count"] == 1
    assert summary["ambiguity_groups"] == 1 and summary["ambiguous_artifacts"] == 1
    # One broad label against four narrow ones is four gradeable directional
    # pairs — the point being that the number is not zero, which is what a
    # single-breadth draft would produce.
    assert summary["breadth_distribution"] == {"broad": 1, "narrow": 4}
    assert summary["directional_pairs_available"] == 4
    assert summary["role_distribution"]["contextual"] == 2


def test_abstention_is_a_recorded_answer_not_a_failure() -> None:
    model = FakeModel([response(topics=[], abstained=True, not_supported=[])])
    record = draft_module.draft_artifact(model, make_artifact())
    summary = draft_module.composition_summary([record])
    assert record["status"] == "drafted" and record["abstained"] is True
    assert summary["artifacts_abstained"] == 1 and summary["labels_accepted"] == 0


def test_run_record_pins_provenance_and_seals_itself() -> None:
    model = FakeModel([response()])
    records = [draft_module.draft_artifact(model, make_artifact())]
    document = draft_module.draft_document(
        drafting_input={
            "schema_version": draft_module.DRAFTING_SCHEMA_VERSION,
            "artifact_count": 1,
            "blind": "blind: ...",
            "holdout": {"dataset_id": "rulespec-holdout-synthetic-v1"},
            "corpus": {"corpus_dataset_id": "synthetic"},
            "artifacts_by_profile": {"federal-register-document-v1": 1},
        },
        input_path=Path("/elsewhere/holdout_drafting_input.json"),
        input_sha256="a" * 64,
        records=records,
        drafter=draft_module.drafter_identity(model, records),
        generated_at="2026-07-28T00:00:00+00:00",
    )

    assert document["schema_version"] == DRAFT_SCHEMA_VERSION
    assert document["status"] == "drafted_unadjudicated"
    assert document["input"]["sha256"] == "a" * 64
    assert document["drafter"]["model_id"] == "fake-provider:fake-drafter-1"
    # The revision the provider said it served is pinned next to the requested
    # id, because the two are allowed to differ.
    assert document["drafter"]["model_revisions_served"] == ["fake-drafter-1-2026-07-28"]
    assert document["drafter"]["cross_family_to_tagger"] is True
    assert document["prompt"]["vocabulary_supplied"] is False
    assert document["prompt"]["instructions_sha256"] == draft_module.sha256_text(draft_module.INSTRUCTIONS)
    assert document["artifacts"][0]["accepted"][0]["quote_sha256"]

    sealed = dict(document)
    manifest = sealed.pop("manifest_sha256")
    assert manifest == draft_module.sha256_text(draft_module.canonical_json(sealed))


def test_a_tagger_family_revision_served_flips_the_cross_family_claim() -> None:
    model = FakeModel([response()])
    records = [draft_module.draft_artifact(model, make_artifact())]
    records[0]["call"]["response_model"] = "gpt-5.6-sol"
    assert draft_module.drafter_identity(model, records)["cross_family_to_tagger"] is False


def test_dry_run_makes_no_call_and_writes_nothing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source = tmp_path / "input.json"
    source.write_text(
        json.dumps(
            {
                "schema_version": draft_module.DRAFTING_SCHEMA_VERSION,
                "artifact_count": 1,
                "artifacts": [make_artifact()],
            }
        )
    )
    destination = tmp_path / "draft.json"
    code = draft_module.main(
        [
            "--input",
            str(source),
            "--input-sha256",
            "",
            "--output",
            str(destination),
            "--model",
            "gemini-3.1-pro-preview",
            "--dry-run",
        ]
    )
    assert code == 0
    assert not destination.exists()
    assert "zero API calls" in capsys.readouterr().err


def test_cli_refuses_an_in_repo_destination(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source = tmp_path / "input.json"
    source.write_text(json.dumps({"schema_version": draft_module.DRAFTING_SCHEMA_VERSION, "artifacts": []}))
    code = draft_module.main(
        [
            "--input",
            str(source),
            "--input-sha256",
            "",
            "--output",
            str(REPO_ROOT / "output" / "leaked_gold.json"),
            "--dry-run",
        ]
    )
    assert code == 2
    assert "refusing to write drafted holdout labels inside the repository" in capsys.readouterr().err
