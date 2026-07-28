#!/usr/bin/env python3
"""Hand-authored Rulespec (RKAF) projection of one real FR document.

Document: FR 2026-03227 — "Maximum Line Speed Rates for Young Chicken and
Turkey Establishments Operating Under the New Poultry Inspection System"
(FSIS proposed rule), the document anchoring gold item
gold_46af63a049ee1964b9ae13f4 in the spicy-regs development corpus.

This script is the reproducibility companion to
fsis-2026-03227.rulespec.jsonld: it re-derives every digest, offset, and
deterministic relationship in that file from the stored tables, verifies them
(hard failure on any mismatch), and re-emits the JSON-LD byte-identically.
Nothing here fabricates data; every value is read from:

  output/segmented-real-data-evaluation-v2/federal_register.parquet   (text)
  output/segmented-real-data-evaluation-v2/gold_spans.parquet         (gold anchor)
  output/rulespec-stabilization-candidate-final/{rule_targets,authority_edges,
      proceedings,dockets}.parquet                                    (edges)
  output/fused-concept-registry-v1/registry.parquet                   (concepts)

Text-state convention (documented, load-bearing): the Artifact's
rkaf:hasContentDigest and every fragment offset are taken over the stored
`federal_register.body_html` field — SHA-256 over its UTF-8 bytes, offsets in
Unicode code points, half-open [start, end) — matching rulespec Core §4.2 and
the carrier-local fragment URN grammar registered in rulespec commit bc88c02.
The spicy-regs run's `artifact_digest` (9b3eb760…) is a different, producer-
scoped VERSION digest over a canonical-json envelope of all profile text
fields; it is reproduced here as corroboration, not used as the content digest.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import duckdb

SPICY = Path("/Users/mikewolfd/Work/spicy-regs")
EVAL = SPICY / "output" / "segmented-real-data-evaluation-v2"
TABLES = SPICY / "output" / "rulespec-stabilization-candidate-final"
REGISTRY = SPICY / "output" / "fused-concept-registry-v1" / "registry.parquet"
OUT_DIR = Path(__file__).resolve().parent
OUT_JSONLD = OUT_DIR / "fsis-2026-03227.rulespec.jsonld"
OUT_PROOF = OUT_DIR / "offset-verification.txt"

DOCNO = "2026-03227"
ARTIFACT_IRI = f"https://www.federalregister.gov/d/{DOCNO}"
P = "urn:rkaf:partner:spicy-regs"  # partner namespace for minted node ids
AUTHORED_AT = "2026-07-28T00:00:00Z"  # authoring timestamp for this exercise
RUN_ASSERTED_AT = "2026-07-24T06:57:16Z"  # asserted_at of the deterministic run

GOLD_ID = "gold_46af63a049ee1964b9ae13f4"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonical_json(value: object) -> str:
    """Mirror of spicy_regs.ontology.common.canonical_json."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def encode_for_uri(value: str) -> str:
    """SPARQL ENCODE_FOR_URI: percent-encode everything outside RFC 3986
    unreserved, uppercase hex."""
    out = []
    for ch in value:
        if (ch.isascii() and ch.isalnum()) or ch in "-._~":
            out.append(ch)
        else:
            out.extend(f"%{b:02X}" for b in ch.encode("utf-8"))
    return "".join(out)


def fragment_urn(artifact_iri: str, start: int, end: int, region_sha256: str) -> str:
    return (
        f"urn:rkaf:fragment:{encode_for_uri(artifact_iri)}:{start}:{end}"
        f":sha256-{region_sha256}"
    )


def main() -> int:
    proof: list[str] = []

    def note(line: str = "") -> None:
        proof.append(line)
        print(line)

    def check(label: str, ok: bool, detail: str = "") -> None:
        status = "OK " if ok else "FAIL"
        note(f"  [{status}] {label}" + (f" — {detail}" if detail else ""))
        if not ok:
            print("\nVERIFICATION FAILED — aborting without writing outputs.")
            sys.exit(1)

    con = duckdb.connect()

    # ------------------------------------------------------------- source text
    row = con.execute(
        f"SELECT * FROM read_parquet('{EVAL}/federal_register.parquet') "
        f"WHERE document_number = ?",
        [DOCNO],
    ).df().to_dict("records")[0]
    body = row["body_html"]
    body_sha = sha256_text(body)

    note("== Source text state ==")
    note(f"document_number : {DOCNO}")
    note(f"stored field    : federal_register.body_html "
         f"(output/segmented-real-data-evaluation-v2/federal_register.parquet)")
    note(f"length          : {len(body)} Unicode code points")
    note(f"sha256(UTF-8)   : {body_sha}")

    # Reproduce the producer-scoped version digest recorded by the run
    # (subjects.py `_make_artifact`: canonical-json envelope over the
    # federal-register-document-v1 profile's text columns).
    profile_cols = ("title", "abstract", "document_type", "agency_slugs",
                    "body_text", "body_html", "full_text")
    source_version = [
        {"source_field": f"federal_register.{c}",
         "value": None if row.get(c) is None else str(row.get(c))}
        for c in profile_cols
    ]
    envelope = {
        "profile": "federal-register-document-v1",
        "source_table": "federal_register",
        "subject_type": "federal_register_document",
        "subject_id": DOCNO,
        "source_values": source_version,
    }
    version_digest = sha256_text(canonical_json(envelope))

    gold = con.execute(
        f"SELECT * FROM read_parquet('{EVAL}/gold_spans.parquet') WHERE gold_id = ?",
        [GOLD_ID],
    ).df().to_dict("records")[0]

    note()
    note("== Gold anchor (gold_spans.parquet) ==")
    for k in ("gold_id", "subject_id", "source_field", "start_char", "end_char",
              "exact_text", "exact_text_sha256", "artifact_digest",
              "concept_scheme", "concept_label", "curation_status"):
        note(f"  {k}: {gold[k]}")
    check("gold subject is this document", gold["subject_id"] == DOCNO)
    check("gold source_field is federal_register.body_html",
          gold["source_field"] == "federal_register.body_html")
    check("producer version digest reproduces gold artifact_digest",
          version_digest == gold["artifact_digest"], version_digest)

    # -------------------------------------------------------------- fragments
    f1s, f1e = int(gold["start_char"]), int(gold["end_char"])
    f2s = body.find("9 CFR")
    f2e = body.find("]", body.find("FSIS-2025-0012")) + 1
    f3s = body.find("21 U.S.C. 451")
    f3e = f3s + len("21 U.S.C. 451")

    fragments = {}
    note()
    note("== SourceFragment offset verification "
         "(unicode code points, half-open [start,end) over body_html) ==")
    for name, (s, e, expect) in {
        "F1": (f1s, f1e, "Poultry Inspection System"),
        "F2": (f2s, f2e, "9 CFR Part 381</li>\n<li>[Docket No. FSIS-2025-0012]"),
        "F3": (f3s, f3e, "21 U.S.C. 451"),
    }.items():
        region = body[s:e]
        digest = sha256_text(region)
        urn = fragment_urn(ARTIFACT_IRI, s, e, digest)
        fragments[name] = {"start": s, "end": e, "text": region,
                           "sha256": digest, "urn": urn}
        note(f"  {name} [{s},{e})")
        note(f"     slice == expected text: {json.dumps(region)}")
        check(f"{name} slice equality", region == expect)
        note(f"     sha256(region): {digest}")
        note(f"     urn: {urn}")
    check("F1 matches gold exact_text", fragments["F1"]["text"] == gold["exact_text"])
    check("F1 digest matches gold exact_text_sha256",
          fragments["F1"]["sha256"] == gold["exact_text_sha256"])

    # ------------------------------------------------- deterministic relations
    rt = con.execute(
        f"SELECT * FROM read_parquet('{TABLES}/rule_targets.parquet') "
        f"WHERE docket_id='FSIS-2025-0012' AND rin='0583-AE01'").df().to_dict("records")
    ae = con.execute(
        f"SELECT * FROM read_parquet('{TABLES}/authority_edges.parquet') "
        f"WHERE rin='0583-AE01'").df().to_dict("records")
    pr = con.execute(
        f"SELECT * FROM read_parquet('{TABLES}/proceedings.parquet') "
        f"WHERE rin='0583-AE01'").df().to_dict("records")
    dk = con.execute(
        f"SELECT * FROM read_parquet('{TABLES}/dockets.parquet') "
        f"WHERE docket_id='FSIS-2025-0012'").df().to_dict("records")

    note()
    note("== Deterministic relationship rows "
         "(output/rulespec-stabilization-candidate-final) ==")
    check("exactly one rule_targets row", len(rt) == 1, canonical_json(
        {k: rt[0][k] for k in ("docket_id", "cfr_ref", "rin", "source",
                               "evidence_id", "actor_id", "run_id")}))
    check("exactly one authority_edges row", len(ae) == 1, canonical_json(
        {k: ae[0][k] for k in ("rin", "authority_raw", "usc_title",
                               "usc_section", "parse_status", "agenda_edition",
                               "actor_id", "run_id")}))
    check("exactly one proceedings row", len(pr) == 1, canonical_json(
        {k: pr[0][k] for k in ("proceeding_id", "rin", "docket_ids_json",
                               "fr_document_numbers_json",
                               "cfr_target_iris_json", "authority_refs_json",
                               "current_stage")}))
    check("exactly one dockets row", len(dk) == 1)
    rt, ae, pr, dk = rt[0], ae[0], pr[0], dk[0]
    check("rule_targets CFR ref is 9-381", rt["cfr_ref"] == "9-381")
    check("proceeding carries this FR document",
          json.loads(pr["fr_document_numbers_json"]) == [DOCNO])
    check("proceeding CFR target IRI is urn:rkaf:us:cfr:9:381",
          json.loads(pr["cfr_target_iris_json"]) == ["urn:rkaf:us:cfr:9:381"])
    check("proceeding docket is FSIS-2025-0012",
          json.loads(pr["docket_ids_json"]) == ["FSIS-2025-0012"])
    check("authority edge is 21 USC 451",
          (ae["usc_title"], ae["usc_section"]) == ("21", "451"))
    check("document lists docket FSIS-2025-0012",
          json.loads(row["docket_ids_json"]) == ["Docket No. FSIS-2025-0012"]
          or "FSIS-2025-0012" in row["docket_ids_json"])
    # rkaf:publishedInDocket (rulespec 3644803) is the Artifact -> Docket edge.
    # Its object is derived from the two published tables, not asserted here:
    # the FR row above names the docket, and dockets.parquet independently
    # carries that docket_id, so the urn:rkaf:us:regsgov: IRI names a Docket
    # node with its own identity rather than one minted from the document.
    check("dockets table independently carries FSIS-2025-0012",
          dk["docket_id"] == "FSIS-2025-0012", canonical_json(
              {"docket_id": dk["docket_id"], "agency_code": dk["agency_code"],
               "docket_type": dk["docket_type"]}))
    check("document docket id agrees with the dockets-table row",
          f"Docket No. {dk['docket_id']}" in row["docket_ids_json"])

    topics = json.loads(row["topics_json"])
    check("document official FR topics", topics == ["Meat inspection",
                                                    "Poultry and poultry products"],
          canonical_json(topics))

    # ------------------------------------------------------------- concepts
    concepts = con.execute(
        f"SELECT * FROM read_parquet('{REGISTRY}') WHERE concept_id IN "
        f"('concept_9bb8165887d1cb3edc54277b','concept_10c6db73325f36bcc6d8b84a')"
    ).df().to_dict("records")
    by_id = {c["concept_id"]: c for c in concepts}
    note()
    note("== Fused registry concepts (output/fused-concept-registry-v1) ==")
    for cid, expect_label in (
        ("concept_9bb8165887d1cb3edc54277b", "Poultry and poultry products"),
        ("concept_10c6db73325f36bcc6d8b84a", "Meat inspection"),
    ):
        c = by_id.get(cid)
        check(f"{cid} present, scheme=subject, label={expect_label!r}",
              c is not None and c["scheme"] == "subject"
              and c["pref_label"] == expect_label,
              c["external_ids_json"] if c is not None else "missing")

    # -------------------------------------------------------- input digests
    # rulespec Core §2.4 (commit e8794ba) makes rkaf:requestContractDigest
    # CONDITIONAL on rkaf:modelExtraction: none of the four activities below is
    # a model call, so none carries the field, and a digest over an envelope
    # minted to satisfy it would be non-conforming. What a deterministic run
    # DID consume is recorded by rkaf:inputDigest together with
    # rkaf:extractedBy / rkaf:extractorVersion — the reproduction handles the
    # spec names for a non-model method. See README resolution R4.
    def input_digest(input_row: dict) -> str:
        clean = {k: (None if v is None else str(v)) for k, v in input_row.items()}
        return sha256_text(canonical_json(clean))

    ea1_input = input_digest(rt)
    ea2_input = input_digest(ae)
    ea3_input = input_digest(pr)
    topics_row = {"document_number": DOCNO,
                  "source_field": "federal_register.topics_json",
                  "value": row["topics_json"]}
    ea4_input = input_digest(topics_row)

    note()
    note("== Input digests (recipe: sha256 over canonical-json of the "
         "stringified input row) ==")
    note("   No rkaf:requestContractDigest is emitted: Core §2.4 requires it "
         "only for")
    note("   rkaf:modelExtraction, and none of these four runs issued a "
         "request contract.")
    for label, i in (("EA1 rule-targets", ea1_input),
                     ("EA2 authority-parser", ea2_input),
                     ("EA3 proceedings", ea3_input),
                     ("EA4 fr-topics import", ea4_input)):
        note(f"  {label}: input row sha256:{i}")

    # ------------------------------------------------------------- graph ids
    proceeding_iri = f"{P}:proceeding:{pr['proceeding_id']}"
    docket_iri = "urn:rkaf:us:regsgov:FSIS-2025-0012"
    rin_iri = "urn:rkaf:us:rin:0583-AE01"
    cfr_iri = "urn:rkaf:us:cfr:9:381"
    usc_iri = "urn:rkaf:us:usc:21:451"
    scheme_iri = f"{P}:scheme:subject"
    workspace_iri = f"{P}:workspace:main"
    concept_poultry = f"{P}:concept:concept_9bb8165887d1cb3edc54277b"
    concept_meat = f"{P}:concept:concept_10c6db73325f36bcc6d8b84a"
    ca1_iri = f"{P}:assignment:frdoc-2026-03227-poultry-products-primary"
    ca2_iri = f"{P}:assignment:frdoc-2026-03227-meat-inspection-substantive"
    ra1_iri = f"{P}:assertion:frdoc-2026-03227-cfr-target-9-381"
    ra2_iri = f"{P}:assertion:rin-0583-AE01-authority-usc-21-451"
    ra3_iri = f"{P}:assertion:frdoc-2026-03227-docket-membership"
    scope_iri = f"{P}:scope:single-document-rulespec-projection-2026-07-28"

    def selector(n: str, s: int, e: int) -> dict:
        return {
            "@id": f"{P}:selector:{n}",
            "@type": "oa:TextPositionSelector",
            "oa:start": s,
            "oa:end": e,
            "rkaf:coordinateSystem": "rkaf:unicode-codepoint",
        }

    def source_fragment(n: str, f: dict) -> dict:
        return {
            "@id": f["urn"],
            "@type": "rkaf:SourceFragment",
            "oa:hasSource": ARTIFACT_IRI,
            "oa:hasSelector": f"{P}:selector:{n}",
            "rkaf:selectorKind": "oa:TextPositionSelector",
            "rkaf:fragmentContentDigest": f"sha256:{f['sha256']}",
            "rkaf:sourceArtifactDigest": f"sha256:{body_sha}",
        }

    def local_concept(iri: str, c: dict) -> dict:
        return {
            "@id": iri,
            "@type": "rkaf:LocalConcept",
            "skos:prefLabel": c["pref_label"],
            "skos:definition": c["definition"],
            "skos:inScheme": scheme_iri,
            "rkaf:definedInScope": workspace_iri,
            "rkaf:conceptScope": f"{P}:scope:fused-concept-registry-v1",
            "rkaf:conceptStatus": "rkaf:active",
        }

    graph: list[dict] = [
        # ------------------------------------------------ 1. immutable Artifact
        {
            "@id": ARTIFACT_IRI,
            "@type": "rkaf:Artifact",
            "rkaf:hasArtifactIdentifier": ARTIFACT_IRI,
            "rkaf:artifactIdentifierScheme": "rkaf:urn-persistent",
            "rkaf:hasContentDigest": f"sha256:{body_sha}",
            "rkaf:hasRegulatoryIdentifier": f"urn:rkaf:us:frdoc:{DOCNO}",
            "rkaf:regulatoryIdentifierScheme": "rkaf:us-frdoc",
            "rkaf:publishedInProceeding": [proceeding_iri],
            # Source-native FR metadata fact: federal_register.docket_ids_json
            # is ["Docket No. FSIS-2025-0012"] for this document, and
            # dockets.parquet carries the matching FSIS-2025-0012 row. Before
            # rulespec commit 3644803 the profile had no Artifact -> Docket
            # edge and this had to route through the Proceeding (README R2).
            "rkaf:publishedInDocket": docket_iri,
        },
        # ------------------------------------- rulemaking-profile graph objects
        {
            "@id": proceeding_iri,
            "@type": "rkaf:Proceeding",
            "rkaf:hasProceedingIdentifier": proceeding_iri,
            "rkaf:proceedingIdentifierScheme": "rkaf:partner-defined",
            "rkaf:proceedingStage": "rkaf:proceedingProposed",
            "rkaf:hasDocket": [docket_iri],
            "rkaf:proceedingAffectsCitation": [cfr_iri],
        },
        {
            "@id": docket_iri,
            "@type": "rkaf:Docket",
            "rkaf:hasDocketIdentifier": docket_iri,
            "rkaf:docketIdentifierScheme": "rkaf:us-regsgov",
        },
        {
            "@id": rin_iri,
            "@type": "rkaf:RegulatoryAgendaItem",
            "rkaf:hasAgendaItemIdentifier": rin_iri,
            "rkaf:agendaItemIdentifierScheme": "rkaf:us-rin",
        },
        # ------------------------------------------------- 2. source fragments
        selector("f1-gold-poultry-inspection-system",
                 fragments["F1"]["start"], fragments["F1"]["end"]),
        selector("f2-heading-cfr-part-and-docket",
                 fragments["F2"]["start"], fragments["F2"]["end"]),
        selector("f3-authority-21-usc-451",
                 fragments["F3"]["start"], fragments["F3"]["end"]),
        source_fragment("f1-gold-poultry-inspection-system", fragments["F1"]),
        source_fragment("f2-heading-cfr-part-and-docket", fragments["F2"]),
        source_fragment("f3-authority-21-usc-451", fragments["F3"]),
        # ------------------------------------------- concept scheme + concepts
        {
            "@id": scheme_iri,
            "@type": "rkaf:ConceptScheme",
            "skos:prefLabel": "spicy-regs fused registry — subject facet",
            "skos:definition": (
                "Topic facet of the spicy-regs fused concept registry "
                "(fused-concept-registry-v1); subject-scheme rows originate in "
                "the Federal Register Thesaurus of Indexing Terms."),
            "rkaf:schemeFacet": f"{P}:facet:topic",
            "rkaf:conceptStatus": "rkaf:active",
            "rkaf:definedInScope": workspace_iri,
        },
        local_concept(concept_poultry, by_id["concept_9bb8165887d1cb3edc54277b"]),
        local_concept(concept_meat, by_id["concept_10c6db73325f36bcc6d8b84a"]),
        # --------------------------------------------- 3. concept assignments
        {
            "@id": ca1_iri,
            "@type": "rkaf:ConceptAssignment",
            "rkaf:assignmentSubject": ARTIFACT_IRI,
            "rkaf:assignmentSubjectType": "rkaf:Artifact",
            "rkaf:assignedConcept": concept_poultry,
            "skos:inScheme": scheme_iri,
            "rkaf:assignmentRole": "rkaf:assignmentPrimary",
            "rkaf:assignmentDerivation": "rkaf:directAssignment",
            "rkaf:assignmentEvidence": [fragments["F1"]["urn"]],
            "rkaf:assignmentEvidenceScheme": "rkaf:carrier-local-fragment",
            "rkaf:assertionOrigin": "rkaf:imported",
            "rkaf:hasExtractionProvenance": f"{P}:activity:fr-topics-import",
            "rkaf:assertedAt": AUTHORED_AT,
            "rkaf:usageEligibility": "rkaf:localOperationalUse",
            "prov:wasDerivedFrom": [
                f"{P}:record:federal_register:{DOCNO}:topics_json",
                f"{P}:record:gold_spans:{GOLD_ID}",
            ],
        },
        {
            "@id": ca2_iri,
            "@type": "rkaf:ConceptAssignment",
            "rkaf:assignmentSubject": ARTIFACT_IRI,
            "rkaf:assignmentSubjectType": "rkaf:Artifact",
            "rkaf:assignedConcept": concept_meat,
            "skos:inScheme": scheme_iri,
            "rkaf:assignmentRole": "rkaf:assignmentSubstantive",
            "rkaf:assignmentDerivation": "rkaf:directAssignment",
            "rkaf:assignmentEvidence": [fragments["F1"]["urn"]],
            "rkaf:assignmentEvidenceScheme": "rkaf:carrier-local-fragment",
            "rkaf:assertionOrigin": "rkaf:imported",
            "rkaf:hasExtractionProvenance": f"{P}:activity:fr-topics-import",
            "rkaf:assertedAt": AUTHORED_AT,
            "rkaf:usageEligibility": "rkaf:localOperationalUse",
            "prov:wasDerivedFrom": [
                f"{P}:record:federal_register:{DOCNO}:topics_json",
            ],
        },
        # ------------------------------------------ 4. relationship assertions
        {
            "@id": ra1_iri,
            "@type": "rkaf:RelationshipAssertion",
            "rkaf:assertsSubject": proceeding_iri,
            "rkaf:assertsPredicate": "rkaf:proceedingAffectsCitation",
            "rkaf:assertsObject": cfr_iri,
            "rkaf:assertionPolarity": "rkaf:affirmed",
            "rkaf:assertionOrigin": "rkaf:deterministicExtraction",
            "rkaf:assertedAt": RUN_ASSERTED_AT,
            "rkaf:usageEligibility": "rkaf:localOperationalUse",
            "rkaf:hasSourceClaimant": f"{P}:claimant:ra1-issuer",
            "rkaf:hasExtractionProvenance": f"{P}:activity:rule-targets",
            "prov:wasDerivedFrom": [
                f"{P}:record:rule_targets:FSIS-2025-0012:9-381",
            ],
        },
        {
            "@id": ra2_iri,
            "@type": "rkaf:RelationshipAssertion",
            "rkaf:assertsSubject": rin_iri,
            "rkaf:assertsPredicate": "rkaf:agendaAuthorityCitation",
            "rkaf:assertsObject": usc_iri,
            "rkaf:assertionPolarity": "rkaf:affirmed",
            "rkaf:assertionOrigin": "rkaf:deterministicExtraction",
            "rkaf:assertedAt": RUN_ASSERTED_AT,
            "rkaf:usageEligibility": "rkaf:localOperationalUse",
            "rkaf:hasSourceClaimant": f"{P}:claimant:ra2-issuer",
            "rkaf:hasExtractionProvenance": f"{P}:activity:authority-parser",
            "prov:wasDerivedFrom": [
                f"{P}:record:authority_edges:0583-AE01:202510",
            ],
        },
        {
            "@id": ra3_iri,
            "@type": "rkaf:RelationshipAssertion",
            "rkaf:assertsSubject": proceeding_iri,
            "rkaf:assertsPredicate": "rkaf:hasDocket",
            "rkaf:assertsObject": docket_iri,
            "rkaf:assertionPolarity": "rkaf:affirmed",
            "rkaf:assertionOrigin": "rkaf:deterministicExtraction",
            "rkaf:assertedAt": RUN_ASSERTED_AT,
            "rkaf:usageEligibility": "rkaf:localOperationalUse",
            "rkaf:hasSourceClaimant": f"{P}:claimant:ra3-issuer",
            "rkaf:hasExtractionProvenance": f"{P}:activity:proceedings",
            "prov:wasDerivedFrom": [
                f"{P}:record:proceedings:{pr['proceeding_id']}",
            ],
        },
        # ------------------------------------------------- evidence bindings
        {
            "@id": f"{P}:binding:ra1-cfr-heading",
            "@type": "rkaf:EvidenceBinding",
            "rkaf:bindsAssertion": ra1_iri,
            "rkaf:bindsSourceFragment": [fragments["F2"]["urn"]],
        },
        {
            "@id": f"{P}:binding:ra2-authority-line",
            "@type": "rkaf:EvidenceBinding",
            "rkaf:bindsAssertion": ra2_iri,
            "rkaf:bindsSourceFragment": [fragments["F3"]["urn"]],
        },
        {
            "@id": f"{P}:binding:ra3-docket-heading",
            "@type": "rkaf:EvidenceBinding",
            "rkaf:bindsAssertion": ra3_iri,
            "rkaf:bindsSourceFragment": [fragments["F2"]["urn"]],
        },
        # -------------------------------------------------- source claimants
        {
            "@id": f"{P}:claimant:ra1-issuer",
            "@type": "rkaf:SourceClaimant",
            "rkaf:claimsAssertion": ra1_iri,
            "rkaf:claimantAttribution": "rkaf:claimantIsDocumentIssuer",
            "rkaf:claimantIdentity":
                "https://www.federalregister.gov/agencies/food-safety-and-inspection-service",
            "rkaf:attributedInFragment": [fragments["F2"]["urn"]],
        },
        {
            "@id": f"{P}:claimant:ra2-issuer",
            "@type": "rkaf:SourceClaimant",
            "rkaf:claimsAssertion": ra2_iri,
            "rkaf:claimantAttribution": "rkaf:claimantIsDocumentIssuer",
            "rkaf:claimantIdentity":
                "https://www.federalregister.gov/agencies/food-safety-and-inspection-service",
            "rkaf:attributedInFragment": [fragments["F3"]["urn"]],
        },
        {
            "@id": f"{P}:claimant:ra3-issuer",
            "@type": "rkaf:SourceClaimant",
            "rkaf:claimsAssertion": ra3_iri,
            "rkaf:claimantAttribution": "rkaf:claimantIsDocumentIssuer",
            "rkaf:claimantIdentity":
                "https://www.federalregister.gov/agencies/food-safety-and-inspection-service",
            "rkaf:attributedInFragment": [fragments["F2"]["urn"]],
        },
        # ---------------------------------------------- extraction activities
        {
            "@id": f"{P}:activity:rule-targets",
            "@type": "rkaf:ExtractionActivity",
            "rkaf:extractionMethod": "rkaf:deterministicParse",
            "rkaf:extractionRun": f"{P}:run:{rt['run_id']}",
            "rkaf:extractedBy": f"{P}:actor:rule-targets:v1",
            "rkaf:extractorVersion": "v1",
            "rkaf:inputDigest": [f"sha256:{ea1_input}"],
        },
        {
            "@id": f"{P}:activity:authority-parser",
            "@type": "rkaf:ExtractionActivity",
            "rkaf:extractionMethod": "rkaf:deterministicParse",
            "rkaf:extractionRun": f"{P}:run:{ae['run_id']}",
            "rkaf:extractedBy": f"{P}:actor:authority-parser:v1",
            "rkaf:extractorVersion": "v1",
            "rkaf:inputDigest": [f"sha256:{ea2_input}"],
        },
        {
            "@id": f"{P}:activity:proceedings",
            "@type": "rkaf:ExtractionActivity",
            "rkaf:extractionMethod": "rkaf:deterministicParse",
            "rkaf:extractionRun": f"{P}:run:{pr['run_id']}",
            "rkaf:extractedBy": f"{P}:actor:proceedings:v1",
            "rkaf:extractorVersion": "v1",
            "rkaf:inputDigest": [f"sha256:{ea3_input}"],
        },
        {
            "@id": f"{P}:activity:fr-topics-import",
            "@type": "rkaf:ExtractionActivity",
            "rkaf:extractionMethod": "rkaf:importedRecord",
            "rkaf:extractionRun": f"{P}:run:segmented-real-data-evaluation-v2",
            "rkaf:extractedBy": f"{P}:actor:corpus-lock",
            "rkaf:extractorVersion": "segmented-real-data-evaluation-v2",
            "rkaf:inputDigest": [f"sha256:{ea4_input}"],
        },
        # --------------------------------------------- provenance record nodes
        # L3 enforces `sh:class prov:Entity` on every prov:wasDerivedFrom value
        # (compiled/shacl/core/{assertion,concept-assignment,
        # relationship-assertion}.ttl), so the cited table rows must be
        # materialized as typed nodes. Each IRI names one row in a published
        # spicy-regs table; the row content is pinned by the corresponding
        # ExtractionActivity rkaf:inputDigest.
        {
            "@id": f"{P}:record:federal_register:{DOCNO}:topics_json",
            "@type": "prov:Entity",
        },
        {
            "@id": f"{P}:record:gold_spans:{GOLD_ID}",
            "@type": "prov:Entity",
        },
        {
            "@id": f"{P}:record:rule_targets:FSIS-2025-0012:9-381",
            "@type": "prov:Entity",
        },
        {
            "@id": f"{P}:record:authority_edges:0583-AE01:202510",
            "@type": "prov:Entity",
        },
        {
            "@id": f"{P}:record:proceedings:{pr['proceeding_id']}",
            "@type": "prov:Entity",
        },
        # ------------------------------------------------------ 5. attestation
        {
            "@id": f"{P}:attestation:ca1-approved-2026-07-28",
            "@type": "rkaf:Attestation",
            "rkaf:attestor": f"{P}:actor:claude-fable-5",
            "rkaf:attestorKind": "rkaf:aiModel",
            "rkaf:targets": [ca1_iri],
            "rkaf:decision": "rkaf:approved",
            "rkaf:attestationScope": scope_iri,
            "rkaf:attestedAt": AUTHORED_AT,
            "rkaf:rationale": (
                "Single-document Rulespec projection exercise "
                "(docs/evidence/single-document-rulespec-projection-2026-07-28). "
                "Verified: fragment offsets [2282,2307) slice the stored "
                "federal_register.body_html state (sha256:" + body_sha + ") to "
                "the exact text 'Poultry Inspection System' whose sha256 matches "
                "both the carrier-local fragment URN digest and the "
                "hand-curated gold row gold_46af63a049ee1964b9ae13f4; the "
                "assigned concept concept_9bb8165887d1cb3edc54277b "
                "('Poultry and poultry products', scheme=subject) matches the "
                "document's official Federal Register topic list."),
        },
    ]

    doc = {"@context": "./rkaf-context.jsonld", "@graph": graph}

    OUT_JSONLD.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n",
                          encoding="utf-8")
    note()
    note(f"wrote {OUT_JSONLD.name} ({len(graph)} graph nodes)")
    OUT_PROOF.write_text("\n".join(proof) + "\n", encoding="utf-8")
    note(f"wrote {OUT_PROOF.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
