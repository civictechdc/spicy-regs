"""Draft free-form gold labels for the sealed holdout, blind and cross-family.

Track A step 3 of ``docs/rulespec-testbed-path-forward.md``, and the direct
successor to ``tools/draw_holdout.py``: that tool drew the holdout and built a
blind drafting input; this one asks a model to annotate it and verifies every
quote it returns.

Four properties this tool owes the holdout, each enforced rather than promised:

1. **Blind.** The drafter is shown one artifact's identity, title, and its
   segments' exact text — nothing else. No registry, no candidate list, no
   controlled vocabulary, no tagger output, no development gold. Registry
   framing would anchor the drafter and re-mint the very defect the new holdout
   exists to escape (layer 4 of ``docs/evidence/failure-analysis-2026-07-27.md``:
   *gold encodes the annotator's frame, not the text's content*). Blindness is
   proved twice per call by :func:`assert_call_blind` — over the payload as a
   structure, and over the whole secret-free request body — using the same
   :func:`draw_holdout.assert_blind` and the same banned-key list that guarded
   the drafting input.

2. **Cross-family.** The tagger is OpenAI-family, so gold drafted by an
   OpenAI-family model would share the tagger's priors and measure agreement
   rather than accuracy. The drafter is therefore reached through the
   ``openai_compatible`` arm's named provider profiles, and the run record
   pins the provider family, the model id, and the *revision the provider
   reported serving* (``response_model``) — not the id that was requested.

3. **Composed, not collapsed.** The response contract asks for several subjects
   per artifact where the text supports several; for broad *and* narrow framings
   of the same subject, so later subsumption grading sees narrower and
   equivalent cases rather than a set that a constant "broader" predictor wins
   (``docs/decisions.md``, 2026-07-28, hyperbolic subsumption prototype: 44/46
   directional pairs were ``broader``); for explicit denials as denials; for
   plausible-but-wrong subjects as declared negatives; and for both readings of
   a genuinely two-framed document rather than a pick between them.

4. **Verified, never repaired.** Every quote must resolve to an exact offset in
   the supplied segment text through
   :func:`spicy_regs.ontology.llm.resolve_exact_evidence_offsets` — the same
   resolver production extraction uses. A quote that does not resolve is a
   *rejected* entry carrying its reason. Nothing is trimmed, re-cased,
   whitespace-normalized, or fuzzy-matched into acceptance: a drafter that
   cannot quote the document is not evidence about the document.

**The drafted labels are not repository data.** They stay out of git until the
configuration freeze, because the exit bar's trivial baselines are computed on
this same set and a label visible to a tuning loop is a tuned label
(``pending_holdout.required_before_adoption``). This tool therefore *refuses*
to write its output inside the repository working tree.

Run::

    tools/draft_holdout_gold.py \\
        --input  /path/outside/repo/holdout_drafting_input.json \\
        --input-sha256 <digest> \\
        --output /path/outside/repo/holdout_gold_draft_v1.json \\
        --provider gemini --model gemini-3.1-pro-preview

``--dry-run`` builds and proves every payload and makes zero API calls.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spicy_regs.ontology.common import canonical_json
from spicy_regs.ontology.llm import resolve_exact_evidence_offsets

TOOLS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TOOLS_DIR.parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from draw_holdout import (  # noqa: E402  (path must be set before the sibling import)
    BANNED_OUTPUT_KEY_SUBSTRINGS,
    DRAFTING_SCHEMA_VERSION,
    HoldoutBlindnessError,
    assert_blind,
)

DRAFT_SCHEMA_VERSION = "rulespec-holdout-gold-draft-v1"

DEFAULT_PROVIDER = "gemini"
DEFAULT_MODEL = "gemini-3.1-pro-preview"
DEFAULT_MAX_OUTPUT_TOKENS = 16_384
DEFAULT_TIMEOUT_SECONDS = 300.0
DEFAULT_MAX_RETRIES = 3

#: Model families the tagger belongs to. Gold drafted by one of these measures
#: agreement with the tagger's own priors, not accuracy, so the tool refuses.
TAGGER_MODEL_FAMILY_MARKERS: tuple[str, ...] = ("gpt-", "o1", "o3", "o4", "openai")

ROLES: tuple[str, ...] = ("primary", "substantive", "mention", "contextual")
BREADTHS: tuple[str, ...] = ("broad", "narrow")

#: The only keys a drafting payload may carry. An allowlist rather than a
#: denylist: a new field in the drafting input cannot reach the drafter by
#: accident, it has to be admitted here on purpose.
PAYLOAD_KEYS: frozenset[str] = frozenset(
    {"artifact_id", "profile_id", "source_table", "subject_type", "title", "segment_count", "segments"}
)
PAYLOAD_SEGMENT_KEYS: frozenset[str] = frozenset({"segment_id", "ordinal", "char_count", "headings", "text"})

REJECTED_UNKNOWN_SEGMENT = "unknown_segment_id"
REJECTED_EMPTY_QUOTE = "empty_quote"
REJECTED_EMPTY_TEXT = "empty_subject_text"
REJECTED_QUOTE_UNRESOLVED = "quote_not_verbatim_or_not_unique_in_segment"

INSTRUCTIONS = """\
You are annotating one government document so that its subject matter can later \
be evaluated. Work only from the text supplied in this message.

No term list, code set, or shortlist of options is supplied, and none exists for \
you to guess at. Write every subject in your own words as free text. Do not \
invent identifiers, codes, or classification-system names.

Record, in `topics`, every subject the supplied text actually supports:

* Several subjects per document are expected. Do not collapse a document to one \
subject. A document that carries two frames gets both.
* Where the text supports both a specific thing and the wider family it belongs \
to, record BOTH as separate entries — the specific one with breadth "narrow", \
the wider one with breadth "broad". Example shape only, not a term list: a \
named substance and the regulatory family that governs it; a named company and \
the industry it is regulated as part of.
* `role` says how central the subject is: "primary" for what the document is \
principally about; "substantive" for a subject genuinely treated but not the \
main one; "mention" for a subject named without discussion; "contextual" for \
background framing only.
* Set `denial` to true when the text explicitly says something does NOT apply, \
is excluded, is exempt, or is withdrawn — and quote the words that say so. A \
denial is recorded as a subject with `denial` true, never omitted.
* When a passage genuinely supports two different framings and the text does \
not settle which is right, record BOTH entries and give them the same non-empty \
`ambiguity_group` string. Do not resolve the ambiguity yourself.
* `frame` is one short clause, in your own words, saying under what reading the \
subject applies.

Evidence rules, applied mechanically and without exception:

* `quote` must be copied CHARACTER FOR CHARACTER from the `text` of the segment \
named in `segment_id`. Do not fix spelling, spacing, capitalization, line \
breaks, or punctuation. Do not join text across segments. Do not paraphrase, \
elide, or add ellipses.
* `quote_start` and `quote_end` are character offsets into that segment's \
`text`, half-open, counted in Unicode codepoints, so that \
text[quote_start:quote_end] is exactly `quote`.
* Choose a quote long enough to be unique within the segment.
* An entry whose quote is not found verbatim is discarded in full. A shorter \
exact quote is always better than a longer approximate one.

Record, in `not_supported`, one or two subjects that a careless reader would \
plausibly assign to this document but that the text does not support — a \
subject suggested by a passing name, a legal citation, an agency's identity, or \
the document's genre rather than by what it says. Give the reason it is wrong. \
These are declared negatives; they take no quote.

If the supplied text supports no subject at all, set `abstained` to true and \
leave `topics` empty. Abstention is a real answer and is preferred over a \
guess."""

RESPONSE_SCHEMA: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["artifact_id", "abstained", "topics", "not_supported"],
    "properties": {
        "artifact_id": {"type": "string"},
        "abstained": {"type": "boolean"},
        "topics": {
            "type": "array",
            "maxItems": 24,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "text",
                    "role",
                    "breadth",
                    "denial",
                    "frame",
                    "ambiguity_group",
                    "segment_id",
                    "quote",
                    "quote_start",
                    "quote_end",
                ],
                "properties": {
                    "text": {"type": "string"},
                    "role": {"type": "string", "enum": list(ROLES)},
                    "breadth": {"type": "string", "enum": list(BREADTHS)},
                    "denial": {"type": "boolean"},
                    "frame": {"type": "string"},
                    "ambiguity_group": {"type": "string"},
                    "segment_id": {"type": "string"},
                    "quote": {"type": "string"},
                    "quote_start": {"type": "integer"},
                    "quote_end": {"type": "integer"},
                },
            },
        },
        "not_supported": {
            "type": "array",
            "maxItems": 6,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["text", "reason"],
                "properties": {"text": {"type": "string"}, "reason": {"type": "string"}},
            },
        },
    },
}

SCHEMA_NAME = "holdout_gold_draft"


class DraftingError(RuntimeError):
    """The drafting run cannot proceed on the inputs or destination it was given."""


class InputDigestMismatchError(DraftingError):
    """The drafting input is not the file whose digest the caller pinned."""


class OutputLocationError(DraftingError):
    """The requested output path is inside the repository working tree."""


class TaggerFamilyError(DraftingError):
    """The requested drafter belongs to the tagger's own model family."""


# --------------------------------------------------------------------------
# input, destination, and family guards
# --------------------------------------------------------------------------


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def load_drafting_input(path: Path, expected_sha256: str) -> tuple[dict[str, Any], str]:
    """Load the blind drafting input, refusing anything but the pinned file.

    The digest is checked against the bytes on disk before the JSON is even
    parsed: a holdout drafted from a different input is a different experiment,
    and it must not be possible to discover that afterwards from the run record.
    """
    raw = Path(path).read_bytes()
    digest = sha256_bytes(raw)
    if expected_sha256 and digest != expected_sha256:
        raise InputDigestMismatchError(
            f"drafting input digest mismatch: file is {digest}, caller pinned {expected_sha256}"
        )
    document = json.loads(raw.decode())
    if not isinstance(document, dict):
        raise DraftingError("drafting input root is not a JSON object")
    version = document.get("schema_version")
    if version != DRAFTING_SCHEMA_VERSION:
        raise DraftingError(f"drafting input schema_version is {version!r}, expected {DRAFTING_SCHEMA_VERSION!r}")
    return document, digest


def assert_output_outside_repo(path: Path, repo_root: Path = REPO_ROOT) -> Path:
    """Refuse to write drafted labels anywhere git could pick them up.

    Prose discipline is what failed last time (layer 5: information flows
    downhill from gold into everything it touches). This is the executable
    version of "the labels stay out of the repository until the freeze".
    """
    resolved = Path(path).expanduser().resolve()
    root = Path(repo_root).resolve()
    if resolved == root or root in resolved.parents:
        raise OutputLocationError(
            f"refusing to write drafted holdout labels inside the repository: {resolved} is under {root}"
        )
    return resolved


def tagger_family_markers_hit(model: str, markers: Sequence[str] = TAGGER_MODEL_FAMILY_MARKERS) -> list[str]:
    """Return the tagger-family markers a model id matches, if any."""
    folded = str(model).casefold()
    return [marker for marker in markers if marker in folded]


def assert_not_tagger_family(model: str, markers: Sequence[str] = TAGGER_MODEL_FAMILY_MARKERS) -> None:
    """Refuse a drafter from the tagger's family.

    Two models from one family share priors; agreement between them is not
    independent evidence, and publishing it as gold would launder the tagger's
    own frame into the answer key.
    """
    hit = tagger_family_markers_hit(model, markers)
    if hit:
        raise TaggerFamilyError(
            f"{model!r} looks like the tagger's own model family ({', '.join(hit)}); "
            "holdout gold must be drafted by a different family"
        )


# --------------------------------------------------------------------------
# the blind payload
# --------------------------------------------------------------------------


def build_payload(artifact: Mapping[str, Any]) -> dict[str, Any]:
    """Project one drafting-input artifact onto the allowlisted payload keys.

    Everything the drafter sees is built here, by name. Segment ``slices`` are
    deliberately dropped: they carry source-field provenance the drafter does
    not need, and every field withheld is a field that cannot anchor it.
    """
    segments = []
    for segment in artifact.get("segments", ()):
        segments.append(
            {
                "segment_id": segment["segment_id"],
                "ordinal": segment["ordinal"],
                "char_count": segment["char_count"],
                "headings": list(segment.get("headings", ())),
                "text": segment["text"],
            }
        )
    payload = {
        "artifact_id": artifact["artifact_id"],
        "profile_id": artifact["profile_id"],
        "source_table": artifact["source_table"],
        "subject_type": artifact["subject_type"],
        "title": artifact["title"],
        "segment_count": artifact["segment_count"],
        "segments": segments,
    }
    extra = set(payload) - PAYLOAD_KEYS
    if extra:
        raise HoldoutBlindnessError(f"payload carries keys outside the allowlist: {sorted(extra)}")
    for segment in segments:
        segment_extra = set(segment) - PAYLOAD_SEGMENT_KEYS
        if segment_extra:
            raise HoldoutBlindnessError(f"payload segment carries keys outside the allowlist: {sorted(segment_extra)}")
    return payload


def request_preview(
    model_id: str,
    payload: Mapping[str, Any],
    *,
    instructions: str = INSTRUCTIONS,
    schema: Mapping[str, Any] = RESPONSE_SCHEMA,
) -> dict[str, Any]:
    """Render the wire body shape without a provider, for credential-free checks.

    The authoritative body is the adapter's ``secret_free_request``; this mirror
    exists so ``--dry-run`` can prove blindness on a machine with no credential.
    It carries the same three things blindness is decided from — instructions,
    response schema, and payload — so a leak in any of them is caught here too.
    """
    return {
        "model": model_id,
        "messages": [
            {"role": "system", "content": instructions},
            {"role": "user", "content": canonical_json(dict(payload))},
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": SCHEMA_NAME, "strict": True, "schema": dict(schema)},
        },
    }


def assert_call_blind(payload: Mapping[str, Any], request: Mapping[str, Any]) -> dict[str, Any]:
    """Prove the whole outgoing call is blind, structurally and as sent.

    Two passes because they fail differently. The payload pass walks the
    artifact as a structure, so a registry field smuggled into a nested segment
    is caught by key name. The request pass walks the entire body the provider
    will receive — instructions, response schema, routing — so a vocabulary
    field introduced in the *contract* rather than the data is caught too.
    """
    payload_facts = assert_blind(payload, banned_key_substrings=BANNED_OUTPUT_KEY_SUBSTRINGS)
    request_facts = assert_blind(request, banned_key_substrings=BANNED_OUTPUT_KEY_SUBSTRINGS)
    return {
        "payload_key_allowlist": sorted(PAYLOAD_KEYS),
        "payload_segment_key_allowlist": sorted(PAYLOAD_SEGMENT_KEYS),
        "banned_key_substrings": list(BANNED_OUTPUT_KEY_SUBSTRINGS),
        "payload_string_values_checked": payload_facts["string_values_checked"],
        "request_string_values_checked": request_facts["string_values_checked"],
        "passed": bool(payload_facts["passed"] and request_facts["passed"]),
    }


# --------------------------------------------------------------------------
# quote verification
# --------------------------------------------------------------------------


def verify_topics(
    topics: Sequence[Mapping[str, Any]],
    segment_text_by_id: Mapping[str, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split drafted subjects into verified and rejected, repairing nothing.

    ``resolve_exact_evidence_offsets`` accepts the drafter's offsets when they
    already frame the quote, and otherwise locates the quote *if it occurs
    exactly once*. Both outcomes are exact; which one happened is recorded in
    ``evidence_alignment_method``. Anything else — absent, altered, or occurring
    more than once so no single span is meant — is a rejection with a reason.
    """
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for ordinal, topic in enumerate(topics):
        text = str(topic.get("text", "")).strip()
        segment_id = str(topic.get("segment_id", ""))
        quote = str(topic.get("quote", ""))
        entry = {
            "ordinal": ordinal,
            "text": text,
            "role": topic.get("role"),
            "breadth": topic.get("breadth"),
            "denial": bool(topic.get("denial", False)),
            "frame": str(topic.get("frame", "")),
            "ambiguity_group": str(topic.get("ambiguity_group", "")),
            "segment_id": segment_id,
            "quote": quote,
            "quote_start_drafted": topic.get("quote_start"),
            "quote_end_drafted": topic.get("quote_end"),
        }
        if not text:
            rejected.append({**entry, "rejection_reason": REJECTED_EMPTY_TEXT})
            continue
        if segment_id not in segment_text_by_id:
            rejected.append({**entry, "rejection_reason": REJECTED_UNKNOWN_SEGMENT})
            continue
        if not quote:
            rejected.append({**entry, "rejection_reason": REJECTED_EMPTY_QUOTE})
            continue
        resolution = resolve_exact_evidence_offsets(
            segment_text_by_id[segment_id],
            quote,
            _as_offset(topic.get("quote_start")),
            _as_offset(topic.get("quote_end")),
        )
        if resolution is None:
            rejected.append({**entry, "rejection_reason": REJECTED_QUOTE_UNRESOLVED})
            continue
        accepted.append(
            {
                **entry,
                "quote_start": resolution.start,
                "quote_end": resolution.end,
                "evidence_alignment_method": resolution.method,
                "quote_sha256": sha256_text(quote),
            }
        )
    return accepted, rejected


def _as_offset(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


# --------------------------------------------------------------------------
# one artifact
# --------------------------------------------------------------------------


def draft_artifact(
    model: Any,
    artifact: Mapping[str, Any],
    *,
    instructions: str = INSTRUCTIONS,
    schema: Mapping[str, Any] = RESPONSE_SCHEMA,
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
) -> dict[str, Any]:
    """Draft, verify, and record one artifact's labels.

    The record holds the request that went out and the response that came back
    alongside the verdicts, so a later reader can re-derive every acceptance
    from the same two documents this function saw.
    """
    payload = build_payload(artifact)
    request = model.secret_free_request(
        name=SCHEMA_NAME,
        schema=schema,
        instructions=instructions,
        payload=payload,
        max_output_tokens=max_output_tokens,
    )
    blindness = assert_call_blind(payload, request)
    segment_text_by_id = {segment["segment_id"]: segment["text"] for segment in payload["segments"]}

    record: dict[str, Any] = {
        "artifact_id": artifact["artifact_id"],
        "profile_id": artifact["profile_id"],
        "subject_type": artifact["subject_type"],
        "artifact_digest": artifact["artifact_digest"],
        "extracted_text_sha256": artifact["extracted_text_sha256"],
        "segment_ids": sorted(segment_text_by_id),
        "blindness": blindness,
        "request": request,
        "status": "drafted",
    }
    try:
        result = model.structured_json(
            name=SCHEMA_NAME,
            schema=schema,
            instructions=instructions,
            payload=payload,
            max_output_tokens=max_output_tokens,
        )
    except Exception as error:  # provider failure is data, not a crash
        record.update(
            {
                "status": "failed",
                "error_code": type(error).__name__,
                # ``call`` is the only receipt-safe attribute on this error:
                # the message and its cause chain may carry provider text.
                "call": getattr(error, "call", None),
                "response": None,
                "accepted": [],
                "rejected": [],
                "not_supported": [],
                "abstained": None,
            }
        )
        return record

    response = dict(result.output)
    accepted, rejected = verify_topics(response.get("topics", ()), segment_text_by_id)
    not_supported = [
        {"text": str(item.get("text", "")).strip(), "reason": str(item.get("reason", "")).strip()}
        for item in response.get("not_supported", ())
        if str(item.get("text", "")).strip()
    ]
    record.update(
        {
            "call": dict(result.call),
            "response": response,
            "artifact_id_echo_matches": response.get("artifact_id") == artifact["artifact_id"],
            "abstained": bool(response.get("abstained", False)),
            "accepted": accepted,
            "rejected": rejected,
            "not_supported": not_supported,
        }
    )
    return record


# --------------------------------------------------------------------------
# composition
# --------------------------------------------------------------------------


def composition_summary(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Report the composition actually achieved, against what was asked for.

    Every number here answers a specific prior failure: multi-label against
    single-label gold, broad/narrow pairs against a directionally trivial graded
    set, denials and declared negatives against a gold with no hard cases, and
    ambiguity groups against gold that resolved two-frame documents by fiat.
    """
    drafted = [record for record in records if record.get("status") == "drafted"]
    accepted_counts = [len(record["accepted"]) for record in drafted]
    roles: dict[str, int] = {role: 0 for role in ROLES}
    breadths: dict[str, int] = {breadth: 0 for breadth in BREADTHS}
    denial_count = 0
    directional_pairs = 0
    ambiguity_groups = 0
    ambiguous_artifacts = 0
    for record in drafted:
        broad = narrow = 0
        groups: dict[str, int] = {}
        for entry in record["accepted"]:
            role = str(entry.get("role", ""))
            if role in roles:
                roles[role] += 1
            breadth = str(entry.get("breadth", ""))
            if breadth in breadths:
                breadths[breadth] += 1
            broad += breadth == "broad"
            narrow += breadth == "narrow"
            denial_count += bool(entry.get("denial"))
            group = str(entry.get("ambiguity_group", "")).strip()
            if group:
                groups[group] = groups.get(group, 0) + 1
        directional_pairs += broad * narrow
        multi = sum(1 for size in groups.values() if size >= 2)
        ambiguity_groups += multi
        ambiguous_artifacts += multi > 0
    return {
        "artifacts_total": len(records),
        "artifacts_drafted": len(drafted),
        "artifacts_failed": sum(1 for record in records if record.get("status") == "failed"),
        "artifacts_abstained": sum(1 for record in drafted if record.get("abstained")),
        "labels_accepted": sum(accepted_counts),
        "labels_rejected": sum(len(record["rejected"]) for record in drafted),
        "labels_per_artifact_min": min(accepted_counts, default=0),
        "labels_per_artifact_max": max(accepted_counts, default=0),
        "labels_per_artifact_mean": round(sum(accepted_counts) / len(accepted_counts), 3) if accepted_counts else 0.0,
        "artifacts_multi_label": sum(1 for count in accepted_counts if count >= 2),
        "artifacts_single_label": sum(1 for count in accepted_counts if count == 1),
        "role_distribution": roles,
        "breadth_distribution": breadths,
        "denial_count": denial_count,
        "denial_artifacts": sum(1 for record in drafted if any(entry.get("denial") for entry in record["accepted"])),
        "negative_count": sum(len(record["not_supported"]) for record in drafted),
        "negative_artifacts": sum(1 for record in drafted if record["not_supported"]),
        "ambiguity_groups": ambiguity_groups,
        "ambiguous_artifacts": ambiguous_artifacts,
        "directional_pairs_available": directional_pairs,
        "rejection_reasons": _tally(entry["rejection_reason"] for record in drafted for entry in record["rejected"]),
        "evidence_alignment_methods": _tally(
            entry["evidence_alignment_method"] for record in drafted for entry in record["accepted"]
        ),
    }


def _tally(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[str(value)] = counts.get(str(value), 0) + 1
    return dict(sorted(counts.items()))


# --------------------------------------------------------------------------
# the run record
# --------------------------------------------------------------------------


def draft_document(
    *,
    drafting_input: Mapping[str, Any],
    input_path: Path,
    input_sha256: str,
    records: Sequence[Mapping[str, Any]],
    drafter: Mapping[str, Any],
    generated_at: str,
    instructions: str = INSTRUCTIONS,
    schema: Mapping[str, Any] = RESPONSE_SCHEMA,
) -> dict[str, Any]:
    """Assemble the drafted-gold document and seal it with a manifest digest."""
    holdout = dict(drafting_input.get("holdout", {}))
    document: dict[str, Any] = {
        "schema_version": DRAFT_SCHEMA_VERSION,
        "generated_at": generated_at,
        "generated_by": "tools/draft_holdout_gold.py",
        "status": "drafted_unadjudicated",
        "reason": (
            "Free-form labels drafted blind by one non-tagger-family model. This is a draft, not "
            "gold: it authorizes nothing until a second independent family drafts the same set, "
            "agreement is published, every disagreement is resolved or excluded, and the selector, "
            "registry, prompt, schema, and token-budget configuration are frozen and pinned."
        ),
        "drafter": dict(drafter),
        "input": {
            "path_basename": Path(input_path).name,
            "sha256": input_sha256,
            "schema_version": drafting_input.get("schema_version"),
            "blind": drafting_input.get("blind"),
            "artifact_count": drafting_input.get("artifact_count"),
            "artifacts_by_profile": dict(drafting_input.get("artifacts_by_profile", {})),
        },
        "holdout": holdout,
        "corpus": dict(drafting_input.get("corpus", {})),
        "prompt": {
            "instructions": instructions,
            "instructions_sha256": sha256_text(instructions),
            "schema": dict(schema),
            "schema_sha256": sha256_text(canonical_json(dict(schema))),
            "schema_name": SCHEMA_NAME,
            "annotation_form": "free-text",
            "vocabulary_supplied": False,
        },
        "verification": {
            "resolver": "spicy_regs.ontology.llm.resolve_exact_evidence_offsets",
            "target": "segment text",
            "unit": "unicode-codepoints",
            "interval": "half-open",
            "repair_policy": "none: a quote that does not resolve exactly is rejected with a reason",
            "rejection_reasons": [
                REJECTED_EMPTY_TEXT,
                REJECTED_UNKNOWN_SEGMENT,
                REJECTED_EMPTY_QUOTE,
                REJECTED_QUOTE_UNRESOLVED,
            ],
        },
        "composition": composition_summary(records),
        "artifacts": [dict(record) for record in records],
    }
    document["manifest_sha256"] = sha256_text(canonical_json(document))
    return document


def drafter_identity(model: Any, records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Pin who drafted: the requested model, and the revision actually served.

    ``response_model`` is what the provider says it ran. It is recorded next to
    the requested id, and separately, because they are allowed to differ and a
    reader must be able to see that they did.
    """
    revisions = sorted(
        {
            str(record["call"]["response_model"])
            for record in records
            if isinstance(record.get("call"), Mapping) and record["call"].get("response_model")
        }
    )
    return {
        "provider": getattr(model, "provider", None),
        "provider_family": getattr(model, "run_configuration", {}).get("provider_family"),
        "transport": getattr(model, "run_configuration", {}).get("transport"),
        "base_url_host": getattr(model, "base_url_host", None),
        "model_id": getattr(model, "model_id", None),
        "model_requested": getattr(model, "model", None),
        "model_revisions_served": revisions,
        "structured_mode": getattr(model, "structured_mode", None),
        # Computed from the ids that actually ran, not asserted: a provider that
        # served a different revision than the one requested must not be able to
        # inherit the requested id's clean bill of health.
        "cross_family_to_tagger": not any(
            tagger_family_markers_hit(candidate) for candidate in [str(getattr(model, "model", ""))] + revisions
        ),
        "tagger_family_markers_refused": list(TAGGER_MODEL_FAMILY_MARKERS),
    }


def build_model(provider: str, model_id: str, **overrides: Any) -> Any:
    """Construct the drafter through the compat arm's named provider profile.

    The arm owns the base URL and the credential's environment-variable name;
    this tool never sees either, and never learns the credential itself.
    """
    from spicy_regs.docpipeline.adapters.openai_compatible import (
        OpenAICompatibleStructuredTextModel,
    )

    return OpenAICompatibleStructuredTextModel.from_environment(provider=provider, model=model_id, **overrides)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, required=True, help="Blind drafting input from tools/draw_holdout.py")
    parser.add_argument("--input-sha256", required=True, help="Pinned digest of that file; a mismatch aborts the run.")
    parser.add_argument("--output", type=Path, required=True, help="Destination, which must be outside the repo.")
    parser.add_argument("--provider", default=DEFAULT_PROVIDER, help="openai_compatible provider profile label")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Caller-pinned model id; never defaulted per provider.")
    parser.add_argument("--max-output-tokens", type=int, default=DEFAULT_MAX_OUTPUT_TOKENS)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--limit", type=int, default=0, help="Draft only the first N artifacts (0 = every artifact).")
    parser.add_argument("--dry-run", action="store_true", help="Build and prove every payload; make zero API calls.")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing output file.")
    args = parser.parse_args(argv)

    try:
        output_path = assert_output_outside_repo(args.output)
        if output_path.exists() and not args.force:
            raise DraftingError(f"{output_path} exists; pass --force to overwrite")
        assert_not_tagger_family(args.model)
        drafting_input, input_digest = load_drafting_input(args.input, args.input_sha256)
    except DraftingError as error:
        print(f"refused: {error}", file=sys.stderr)
        return 2

    artifacts = list(drafting_input.get("artifacts", ()))
    if args.limit > 0:
        artifacts = artifacts[: args.limit]
    if not artifacts:
        print("refused: drafting input carries no artifacts", file=sys.stderr)
        return 2

    if args.dry_run:
        for artifact in artifacts:
            payload = build_payload(artifact)
            assert_call_blind(payload, request_preview(args.model, payload))
        print(
            f"dry run: {len(artifacts)} payloads built and proved blind; zero API calls; nothing written",
            file=sys.stderr,
        )
        return 0

    model = build_model(
        args.provider,
        args.model,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
    )
    records: list[dict[str, Any]] = []
    for index, artifact in enumerate(artifacts, start=1):
        record = draft_artifact(model, artifact, max_output_tokens=args.max_output_tokens)
        records.append(record)
        print(
            f"[{index}/{len(artifacts)}] {record['artifact_id']} {record['status']} "
            f"accepted={len(record['accepted'])} rejected={len(record['rejected'])}",
            file=sys.stderr,
        )

    document = draft_document(
        drafting_input=drafting_input,
        input_path=args.input,
        input_sha256=input_digest,
        records=records,
        drafter=drafter_identity(model, records),
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n")
    print(canonical_json(document["composition"]), file=sys.stderr)
    print(f"wrote {output_path} manifest_sha256={document['manifest_sha256']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
