"""Optional OpenAI structured-output provider for the concept tagging loop."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Protocol, Sequence, cast

from loguru import logger

from spicy_regs.ontology.common import canonical_json
from spicy_regs.ontology.subjects import Subject

DEFAULT_MODEL = "gpt-5.6-luna"


@dataclass(frozen=True)
class TagProposal:
    concept_id: str | None
    proposed_label: str | None
    scheme: str
    definition: str | None
    confidence: float
    evidence_text: str
    evidence_field: str
    justification: str
    external_ids: tuple[dict[str, str], ...] = ()


@dataclass(frozen=True)
class ValidationProposal:
    agrees: bool
    confidence: float
    rationale: str


class OntologyModel(Protocol):
    model_id: str

    def tag(self, subject: Subject, concepts: Sequence[dict]) -> list[TagProposal]: ...

    def validate(
        self,
        *,
        subject: Subject,
        concept: dict,
        assignment: dict,
    ) -> ValidationProposal: ...


_TAG_SCHEMA = {
    "type": "object",
    "properties": {
        "tags": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "concept_id": {"type": ["string", "null"]},
                    "proposed_label": {"type": ["string", "null"]},
                    "scheme": {"type": "string", "enum": ["subject", "regulated_entity"]},
                    "definition": {"type": ["string", "null"]},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "evidence_text": {"type": "string"},
                    "evidence_field": {"type": "string"},
                    "justification": {"type": "string"},
                    "external_ids": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "scheme": {
                                    "type": "string",
                                    "enum": ["cas", "naics", "skos:exactMatch"],
                                },
                                "value": {"type": "string"},
                                "iri": {"type": ["string", "null"]},
                            },
                            "required": ["scheme", "value", "iri"],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": [
                    "concept_id",
                    "proposed_label",
                    "scheme",
                    "definition",
                    "confidence",
                    "evidence_text",
                    "evidence_field",
                    "justification",
                    "external_ids",
                ],
                "additionalProperties": False,
            },
        }
    },
    "required": ["tags"],
    "additionalProperties": False,
}

_CAS_NUMBER = re.compile(r"^[1-9]\d{1,6}-\d{2}-\d$")
_NAICS = re.compile(r"^\d{2,6}$")


def _valid_cas_number(value: str) -> bool:
    if not _CAS_NUMBER.fullmatch(value):
        return False
    body, check = value.rsplit("-", 1)
    digits = body.replace("-", "")
    checksum = sum(index * int(digit) for index, digit in enumerate(reversed(digits), start=1))
    return checksum % 10 == int(check)


def validated_external_ids(values: object) -> tuple[dict[str, str], ...]:
    """Keep only syntactically grounded external registry anchors."""
    if not isinstance(values, list):
        return ()
    result: list[dict[str, str]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        fields = cast(dict[str, object], item)
        scheme = str(fields.get("scheme") or "")
        value = str(fields.get("value") or "").strip()
        iri = str(fields.get("iri") or "").strip()
        if scheme == "cas" and _valid_cas_number(value):
            result.append({"scheme": scheme, "value": value})
        elif scheme == "naics" and _NAICS.fullmatch(value):
            result.append({"scheme": scheme, "value": value})
        elif scheme == "skos:exactMatch" and iri.startswith(("https://", "http://")):
            result.append({"scheme": scheme, "value": value or iri, "iri": iri})
    return tuple(result)


_VALIDATION_SCHEMA = {
    "type": "object",
    "properties": {
        "agrees": {"type": "boolean"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "rationale": {"type": "string"},
    },
    "required": ["agrees", "confidence", "rationale"],
    "additionalProperties": False,
}


class OpenAIOntologyModel:
    """Responses API provider using strict JSON-schema output.

    The SDK import is lazy, so deterministic rollups and keyless CI do not need
    to initialize an API client. ``OPENAI_API_KEY`` enables the provider;
    ``SPICY_REGS_ONTOLOGY_MODEL`` overrides the cost-sensitive default.
    """

    def __init__(self, *, api_key: str, model: str = DEFAULT_MODEL) -> None:
        from openai import OpenAI

        self.model = model
        self.model_id = f"openai:{model}"
        self._client = OpenAI(api_key=api_key)

    @classmethod
    def from_environment(cls) -> OpenAIOntologyModel | None:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.warning("Ontology LLM: OPENAI_API_KEY is unset — generation/validation is a no-op")
            return None
        return cls(
            api_key=api_key,
            model=os.environ.get("SPICY_REGS_ONTOLOGY_MODEL", DEFAULT_MODEL),
        )

    def _structured(self, *, name: str, schema: dict, instructions: str, payload: dict) -> dict:
        response = self._client.responses.create(
            model=self.model,
            instructions=instructions,
            input=canonical_json(payload),
            text={
                "format": {
                    "type": "json_schema",
                    "name": name,
                    "strict": True,
                    "schema": schema,
                }
            },
        )
        return json.loads(response.output_text)

    def tag(self, subject: Subject, concepts: Sequence[dict]) -> list[TagProposal]:
        allowed_ids = {str(concept["concept_id"]) for concept in concepts}
        payload = {
            "subject": {
                "type": subject.subject_type,
                "id": subject.subject_id,
                "fields": subject.fields,
            },
            "available_concepts": [
                {
                    "concept_id": concept.get("concept_id"),
                    "scheme": concept.get("scheme"),
                    "pref_label": concept.get("pref_label"),
                    "alt_labels_json": concept.get("alt_labels_json"),
                    "definition": concept.get("definition"),
                }
                for concept in concepts
            ],
        }
        result = self._structured(
            name="ontology_tags",
            schema=_TAG_SCHEMA,
            instructions=(
                "Tag the regulatory subject for retrieval. Match an available concept first. "
                "Only propose a new concept when none is semantically equivalent; then set concept_id "
                "to null and provide a concise preferred label, one-sentence definition, and justification. "
                "Use the subject facet for policy topics and regulated_entity for chemicals, industries, "
                "products, or other regulated entities. Include CAS, NAICS, or exact-match anchors only when "
                "the supplied text makes them resolvable. Every tag needs a verbatim evidence span and source field."
            ),
            payload=payload,
        )
        proposals: list[TagProposal] = []
        for item in result.get("tags") or []:
            concept_id = item.get("concept_id")
            if concept_id is not None and concept_id not in allowed_ids:
                logger.warning("Ontology LLM returned unknown concept id {}; dropping tag", concept_id)
                continue
            evidence = str(item.get("evidence_text") or "").strip()
            field = str(item.get("evidence_field") or "").strip()
            if not evidence or evidence.lower() not in subject.text.lower() or field not in subject.fields:
                logger.warning(
                    "Ontology LLM returned ungrounded evidence for {} {}; dropping tag",
                    subject.subject_type,
                    subject.subject_id,
                )
                continue
            label = item.get("proposed_label")
            definition = item.get("definition")
            if concept_id is None and (not label or not definition):
                continue
            proposals.append(
                TagProposal(
                    concept_id=concept_id,
                    proposed_label=None if label is None else str(label).strip(),
                    scheme=str(item.get("scheme") or "subject"),
                    definition=None if definition is None else str(definition).strip(),
                    confidence=max(0.0, min(1.0, float(item.get("confidence") or 0))),
                    evidence_text=evidence,
                    evidence_field=field,
                    justification=str(item.get("justification") or "").strip(),
                    external_ids=validated_external_ids(item.get("external_ids")),
                )
            )
        return proposals

    def validate(
        self,
        *,
        subject: Subject,
        concept: dict,
        assignment: dict,
    ) -> ValidationProposal:
        result = self._structured(
            name="ontology_validation",
            schema=_VALIDATION_SCHEMA,
            instructions=(
                "Independently validate whether the supplied evidence supports assigning the concept "
                "to the regulatory subject. Score only the evidence and the concept scope. Disagree when "
                "the span is merely adjacent, negated, boilerplate, or outside the definition."
            ),
            payload={
                "subject": {
                    "type": subject.subject_type,
                    "id": subject.subject_id,
                    "fields": subject.fields,
                },
                "concept": {
                    "concept_id": concept.get("concept_id"),
                    "scheme": concept.get("scheme"),
                    "pref_label": concept.get("pref_label"),
                    "definition": concept.get("definition"),
                },
                "assignment": {
                    "confidence": assignment.get("confidence"),
                    "evidence_json": assignment.get("evidence_json"),
                },
            },
        )
        return ValidationProposal(
            agrees=bool(result.get("agrees")),
            confidence=max(0.0, min(1.0, float(result.get("confidence") or 0))),
            rationale=str(result.get("rationale") or "").strip(),
        )
