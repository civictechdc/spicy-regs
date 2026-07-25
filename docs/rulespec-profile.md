# Spicy Regs Rulespec application profile

**Status:** Active application profile
**Carrier:** Apache Parquet
**Public table registry:** `spicy_regs.data_dictionary.TABLES`
**Executable L0 mapping:** `conformance/rulespec-l0.yaml`
**Human-readable mapping:** `docs/ontology.md`

## Profile contract

Rulespec is the canonical semantic model behind Spicy Regs. Spicy Regs keeps
source tables at their source-specific grains and projects shared meaning over
them; it does not turn every row into a generic document. A docket remains a
container, a proceeding remains a process, a posting remains an artifact, a
comment remains a participation artifact, and an analytical rollup remains a
view.

Every durable field has one of four dispositions:

- **`compose`** — use Rulespec together with a named external vocabulary or
  source registry;
- **`profile`** — a deliberate Spicy Regs carrier, query, ranking, or
  retrieval-grade choice;
- **`Rulespec candidate`** — reusable meaning that needs a released Rulespec
  term, constraint, projector, or conformance capability;
- **`source data unavailable`** — the source cannot support the claim and the
  carrier records unknown rather than inventing it.

Only the terms declared by the executable L0 mapping are current conformance
claims. Rows described below as planned or partial do not gain Rulespec meaning
from this prose alone.

## Identity, versions, and evidence

Canonical local IRIs use percent-encoded keys and the
`urn:spicy-regs:<type>:<key>` namespace. Registered identifiers use the
Rulespec expansion declared in `docs/ontology.md`. A lexical match is not
enough for a registry-backed scheme: the value must come from that registry,
resolve there, or be corroborated by a source-of-record row.

Source rows are mutable observations unless the source supplies an immutable
edition or artifact. A publication generation is the immutable snapshot
boundary. Derived rows must carry the generation snapshot, deterministic or
model method, actor, run, assertion time, evidence identifiers, and
supersession link appropriate to their claim. Absence means unknown; it never
authorizes a placeholder entity or a negative assertion.

The profile uses these shorthand version policies in the inventory:

- **`S` (source-current):** latest observed source row; prior state is retained
  only when the source or pipeline does so.
- **`E` (editioned):** the source edition is part of identity.
- **`A` (artifact):** immutable publication/posting identity; mutable metadata
  is a snapshot of that artifact.
- **`G` (generation):** a derived row belongs to one manifest-addressed
  generation and is superseded, never silently rewritten.
- **`V` (view):** a reproducible analytical view of named input generations,
  not a new real-world entity.

## Published-table inventory

The key/IRI column states the local key and canonical IRI strategy. “Supports”
names the strongest assertions the current grain could support; it is not a
conformance claim unless the status says mapped.

| Table | Grain and semantic role | Key and canonical IRI | Rulespec/composed class and source authority | Version; containment and relationships | Supports; evidence and provenance | Projection status; local extension or gap |
| --- | --- | --- | --- | --- | --- | --- |
| `dockets` | One Regulations.gov docket container | `docket_id`; `urn:rkaf:us:regsgov:{docket_id}` after registry provenance check | `rkaf:Docket`; Regulations.gov | S/G; contains documents and comments; participates in proceedings | Docket identity, title, agency, RIN evidence; raw source row plus snapshot | Partial through derived tables. `profile`: source metadata stays authoritative; direct table projection pending |
| `documents` | One Regulations.gov document posting, distinct from its files | `document_id`; `urn:rkaf:us:regsgov:{document_id}` | `rkaf:Artifact`; Regulations.gov | A/G; belongs to docket, may correspond to an FR posting, has attachment renditions | Posting identity, dates, withdrawal, RIN/FR links, comment-window evidence; source row and file URLs | L0 partial: posting identity and evidence-qualified FR cross-posting links map; raw `fr_doc_num` nonmembers remain unprojected. `Rulespec candidate`: explicit containment and artifact-edition rules |
| `comments` | One submitted comment/participation artifact, distinct from commenter and attachments | `comment_id`; `urn:rkaf:us:regsgov:{comment_id}` | `rkaf:Artifact` + PROV-O activity/entity composition; Regulations.gov | A/G; submitted to docket; may have attachment artifacts | Submission identity, dates, organization text, content evidence; source row and extraction status | `compose`: participation semantics use PROV-O; `Rulespec candidate` only if a reusable participation relation is proven |
| `comments_index` | Count view by agency, docket, year, and month | composite row key; no real-world entity IRI | Local analytical view | V; summarizes `comments` | Count assertion tied to input generation and query definition | `profile`: no ontology node; must not be mistaken for comment evidence |
| `feed_summary` | One current discovery/feed row per docket | `docket_id`; view IRI only if exported | Local analytical view over `rkaf:Docket` | V; joins docket, document deadline, and comment counts | Discovery facts and counts with generation/query provenance | `profile`: presentation view, not semantic source of truth |
| `agency_stats` | One agency aggregate row | `agency_code`; organization IRI unresolved | Organization composition; source codes come from participating agencies | V; aggregates dockets/documents/comments | Corpus counts with generation/query provenance | `Rulespec candidate`: organization identity registry; counts remain `profile` |
| `agency_monthly_volume` | Agency/month/document-type aggregate | composite row key; no entity IRI | Local analytical view | V; summarizes documents | Volume assertion with input generation and query definition | `profile` |
| `rulemaking_lifecycles` | Docket-level elapsed-time rollup, not a Proceeding | `(kind,docket_id)`; view IRI only | Local analytical view over `rkaf:Docket` and dates | V; derived from document events | Proposed/final dates and duration with deterministic rule provenance | `profile`: must not be projected as a complete lifecycle event history |
| `fr_docket_links` | One direct Federal Register posting-to-docket source link | `(document_number,docket_id)`; stable assertion IRI from both keys | `rkaf:Artifact` to `rkaf:Docket`; Federal Register + Regulations.gov | A/G; connects cross-posting and docket container | Direct relationship, publication metadata, RIN/CFR evidence; both source rows | Partially consumed. `profile`: raw heterogeneous FR docket labels retained; only registry-corroborated IDs project |
| `cfr_sections` | One GovInfo CFR granule for an edition | `granule_id`/`package_id`; GovInfo URL plus `urn:rkaf:us:cfr:{title}:{section}` citation | `rkaf:Artifact` + `rkaf:us-cfr`; GovInfo | E/A; section belongs to edition/package | Edition-scoped CFR identity and heading; GovInfo record and retrieval timestamp | `Rulespec candidate`: exact edition-bearing regulatory identifier/projector |
| `congress_bills` | One Congress.gov bill work summary | `bill_id`; canonical Congress.gov URL | `rkaf:Artifact` composed with legislative vocabularies; Congress.gov | S/E; bill work may become a public-law artifact | Bill identity, chamber, action, PL link; Congress.gov row | `compose`: legislative work/edition semantics; public-law link projection pending |
| `unified_agenda` | One RIN in one agenda edition; immutable observation, not a Proceeding | `(rin,agenda_edition)`; edition-specific Reginfo URL | `rkaf:RegulatoryAgendaObservation`; Reginfo.gov | E/A; `foaf:primaryTopic` names the durable agenda item | RIN, stage, priority, CFR/legal-authority citations, timetable; source edition | L0 maps identity, primary topic, stage, priority, and normalized authority. Heterogeneous raw CFR strings remain source data |
| `federal_register` | One Federal Register posting | `document_number`; permanent `https://www.federalregister.gov/d/{document_number}` | `rkaf:Artifact`; FederalRegister.gov / Office of the Federal Register | A/G; may be published in proceeding and be a format/cross-posting of another artifact | Publication identity, RIN, CFR, dates, topics, comment window; source row | Partially mapped. `profile`: legacy document numbers use permanent URL fallback, never false `us-frdoc` |
| `rule_targets` | One action-specific docket/RIN/target assertion | stable tuple of subject, target, source, and evidence | Typed assertion between `rkaf:Docket`/`Proceeding` and a CFR citation | G; joins dockets, FR artifacts, documents, and direct CFR targets; never UA-by-RIN | Affected citation, direct RIN evidence, method, evidence, first/last seen | L0 maps citation-level affected targets. `source data unavailable`: immutable pre/post-action editions remain unresolved |
| `authority_edges` | One parsed or retained authority citation from one agenda observation | stable tuple of RIN, raw citation, edition | `rkaf:RegulatoryAgendaObservation` composed with U.S.C./Public Law identifiers | G/E; remains on its editioned observation | Exact/partial/failed parse, raw value, edition, provenance | L0 maps normalized agenda authority; failed/raw evidence remains searchable and never fans out to Proceedings |
| `proceedings` | One independently evidenced regulatory action | `proceeding_id`; `urn:spicy-regs:proceeding:{proceeding_id}` | `rkaf:Proceeding`; derived from dockets and action artifacts | G; connects dockets, artifacts, direct targets, and predecessor/successor proceedings | Action identity, current stage plus event history, continuity, complete deterministic provenance | L0 partial/Experimental. RIN is an optional query aid only and never participates in grouping or stable-id reuse |
| `regulatory_agenda_items` | One durable registry item per RIN | `agenda_item_id`; canonical `urn:rkaf:us:rin:{rin}` | `rkaf:RegulatoryAgendaItem`; Reginfo.gov identifier authority | G over E observations; relates to zero or more action links | Scope status and explicit basis, observation/link counts, first/latest evidence | L0 maps item identity and evidence-state scope. Only official Routine and Frequent priority proves recurrence |
| `agenda_item_proceedings` | One evidence-qualified agenda-item-to-action relationship | `relationship_id`; local qualified-relation node | `rkaf:AgendaProceedingRelationship` / DCAT qualified relation | G; links an agenda item to an independently identified Proceeding | Direct docket/document/FR evidence URL and date plus PROV run, actor, and time | L0 mapped. RIN equality or Unified Agenda membership alone cannot create a row |
| `comment_periods` | One evidenced comment interval; may anchor to a proceeding, docket, or both | `comment_period_id`; `urn:spicy-regs:comment-period:{id}` | `rkaf:CommentPeriod`; Regulations.gov and Federal Register | G; opened by source artifact and anchored to at least one docket/proceeding | Inclusive dates, opening artifact, source evidence, deterministic provenance | L0 mapped against the repaired Experimental contract: repeatable proceeding/docket anchors, opening Artifacts, dates, and typed provenance |
| `concepts` | One retrieval-grade local concept state | `concept_id`; `urn:spicy-regs:concept:{id}` | SKOS concept; not a Rulespec registered concept | G; hierarchy and replacement graph | Label, definition, facet, status, source/model/human provenance | `compose`: SKOS; `profile`: retrieval-grade lifecycle; no promotion claim |
| `concept_assignments` | One versioned subject-to-concept assertion | `assignment_id`; `urn:spicy-regs:concept-assignment:{id}` | `rkaf:Assertion` candidate + SKOS association | G; subject can be docket or artifact; supersedes prior assignment | Evidence text/fields, confidence, model/ruleset lineage, validation result | `Rulespec candidate`: typed assertion/confidence/evidence node construction |
| `concept_events` | One append-only concept/assignment lifecycle event | `event_id`; `urn:spicy-regs:concept-event:{id}` | PROV-O activity/event composition | G; changes concept or assignment state | Before/after payload, actor, method, reason, run and time | `profile`: operational/retrieval lifecycle; promotion requires human-reviewed Rulespec process |
| `sam_entities` | One SAM registration/account observation, not the organization itself | `uei`; `urn:sam:entity:{uei}` for the registration identity | Organization/account composition; SAM.gov | S/G; registration describes an organization and classifications | UEI/CAGE, names, registration dates/status, source payload | `compose`: organization ontology plus account/registration role; do not equate account with organization |
| `lobbying_filings` | One LDA filing | `filing_uuid`; canonical Senate LDA filing URL/IRI | `rkaf:Artifact` + activity/organization composition; Senate LDA | A/G; filed by registrant for client, names lobbyists/issues | Filing identity, parties, dates, activities, source row | `compose`; inferred organization resolution remains a confidence-bearing `profile` assertion |
| `fec_committees` | One FEC committee registration observation | `committee_id`; `urn:fec:committee:{committee_id}` | Organization/account composition; FEC | S/G; committee registration distinct from an organization/person | Committee identity, type, status, party, candidate links; FEC row | `compose`; `Rulespec candidate` only for reusable organization identity assertions |
| `gao_reports` | One GAO published report/testimony | `report_id`; canonical GAO URL | `rkaf:Artifact`; GAO | A/G; may mention rules, agencies, laws, or programs | Publication identity, title, date, description; GAO feed record | `profile`: relationship discovery not yet projected; exact mentions require evidence fragments |
| `crs_reports` | One CRS report edition/summary record | `report_id`; canonical Congress.gov/CRS URL | `rkaf:Artifact`; Congress.gov/CRS | A/S/G; report editions must remain distinguishable when source exposes them | Publication identity, title, dates, summary; source row | `compose`; `source data unavailable` where list API omits edition history |
| `court_dockets` | One CourtListener court docket container, not an opinion or order | `docket_id`; canonical CourtListener URI | Docket/container composition; CourtListener | S/G; contains filings/opinions/orders when later ingested | Court identity, case name, jurisdiction, dates, source URL | `compose`; `Rulespec candidate`: typed legal-effect events only after exact affected-artifact evidence |
| `court_opinions` | One official Supreme Court opinion PDF package, not one inferred authored opinion | `opinion_id`; official Supreme Court PDF URL plus content digest | `rkaf:Artifact`; Supreme Court of the United States | A/G; may contain a lead opinion, concurrence, or dissent that remains unsplit without source-backed structure | Package identity, docket number, decision date, official PDF bytes, extracted text, and extraction provenance | `profile`: document processing and retrieval; no legal-effect assertion or authored-opinion identity is inferred from layout |
| `usaspending_recipients` | One recipient account/entity observation | `recipient_id`; canonical USASpending recipient identity | Organization/account composition; USASpending.gov | S/G; recipient record distinct from organization and award | Recipient identity/name/location with source update provenance | `compose`; probabilistic SAM joins must remain separate confidence-bearing assertions |
| `fcc_proceedings` | One ECFS proceeding container | `proceeding_id`; canonical FCC ECFS proceeding URI | `rkaf:Proceeding` candidate + official FCC registry composition; FCC | S/G; contains filings and may relate to federal rulemaking | Official proceeding identity, bureau, subject, dates; ECFS row | `Rulespec candidate`: official-registry identifier scheme; no `partner-defined` surrogate claim |
| `fcc_filings` | One ECFS filing/submission, distinct from proceeding and filer | `filing_id`; canonical FCC ECFS filing URI | `rkaf:Artifact` + participation composition; FCC | A/G; filed in FCC proceeding, may have attachments/filer | Filing identity, type, dates, status, filer text, source row | `compose`; privacy/access scope required before person-level enrichment |

All 32 entries above are generated from or reconciled with the public registry.
Adding a public table requires adding an inventory row in the same change.

## Current regulatory projection

The atomic ontology generation publishes `rule_targets`, `authority_edges`,
`proceedings`, `regulatory_agenda_items`, `agenda_item_proceedings`,
`comment_periods`, `concepts`, `concept_assignments`, and `concept_events`.
Its manifest binds all nine artifacts to the same source inputs, prior state,
code version, run, snapshot, row counts, and hashes.

The executable mapping may project only claims the carrier can construct
without losing domain, range, direction, evidence, or identity. In particular:

- raw CFR and authority citations do not identify immutable legal artifacts
  until an edition resolver succeeds;
- a missing legal authority remains unknown and never causes an agency-shaped
  placeholder Authority;
- a raw cross-posting identifier maps only after exact Federal Register
  source-membership corroboration; nonmembers remain preserved and counted;
- a comment period survives when either its docket or proceeding anchor is
  known;
- current proceeding stage must agree with the latest stage-family lifecycle
  event when such an event exists;
- RIN equality never merges Proceedings or projects agenda-level stage,
  priority, CFR, or authority claims onto an action;
- retrieval concepts remain local even when their labels resemble registered
  concepts.

Production publication has a separate release preflight. The L0 declaration
keeps the normative contract digest in `rulespec_version` and records the
released semantic version and canonical GitHub release page in
`rulespec_release` and `rulespec_release_url`. A real upload fails before source
retrieval unless the L0 claim passes, both release fields agree, and the release
page is reachable. The preflight also downloads the immutable tag archive under
strict size limits, recomputes Rulespec's canonical digest from
`constraints/core/*.cue`, the JSON-LD context, and the L0 range registry, and
requires it to equal `rulespec_version`. `--skip-upload` deliberately bypasses
this operational gate so an unreleased candidate can still be built and
reviewed locally.

## Compatibility and change governance

### Stability classes

Within a pinned Rulespec release and digest, Spicy Regs treats the L0 mapping
grammar, registered term IRIs, identifier-scheme IRIs and expansions, enum IRIs,
domain/range/direction, and conformance result semantics as stable. Core terms
currently consumed include `Artifact`, `Authority`, `Assertion`,
`ConfidenceRecord`, `Finding`, `LifecycleEvent`, and their declared provenance
relations. The rulemaking module (`Proceeding`, `Docket`, `CommentPeriod`, its
relations, stages, and event kinds) remains Experimental until its published
graduation gate holds; “Experimental” permits coordinated breaking migration,
not silent drift.

Local Parquet column names, query syntax, ranking, model prompts, checkpoints,
and retrieval-grade vocabularies follow Spicy Regs compatibility policy. A
local implementation detail becomes shared only through an accepted and
released Rulespec change.

### Pre-1.0 migration rule

A term, shape, enum, identifier grammar, or transform change requires one
paired migration:

1. capture the failing corpus example and classify ownership;
2. change normative Rulespec prose before generated enforcement;
3. regenerate every affected Rulespec artifact and pass its full gate;
4. produce a compatibility report listing added, removed, renamed, narrowed,
   widened, and behaviorally changed terms and affected carrier rows;
5. update this profile, both L0 mapping forms, Spicy Regs code and fixtures,
   and the pinned release plus immutable digest in one reviewed batch;
6. run the old and new contracts against identical manifest-addressed inputs
   and record before/after counts;
7. release Rulespec before publishing Spicy Regs data that claims the change.

Spicy Regs must reject an unknown digest, unmapped enum, invalid identifier, or
incompatible carrier row rather than silently coercing it. Removed meaning is
superseded with migration evidence; history is not rewritten.

### Compatibility report gate

Before changing the pin, the paired receipt must bind:

- old and new Rulespec versions, commits, and contract digests;
- the Spicy Regs commit and candidate manifest snapshot;
- every candidate artifact hash;
- a term/shape/enum/grammar diff and affected-row counts;
- Rulespec compile/test results, L0 audit, Spicy Regs tests/lint, and
  corpus-validation results;
- unresolved, quarantined, and intentionally local cases.

A green unit suite without the bound carrier generation is not this gate.

### Maintainer overlap and independent review

Rulespec currently has a single maintainer who also contributes to Spicy Regs.
That overlap must be disclosed in each stabilization or promotion packet. A
maintainer-operated persona review is useful adversarial evidence but is not a
non-originating consumer. Graduation requires review or explicit ratification
by a consumer who did not design the module or operate the originating corpus
exercise.

### Evidence for a new shared term

Spicy Regs proposes a Rulespec core term only when the packet contains:

- at least one real, manifest-addressed corpus example and row count;
- one concrete query, validation failure, or interoperability need that cannot
  be expressed without losing semantic role, direction, or evidence;
- a search of existing Rulespec and recognized external vocabularies;
- positive, negative, edge, and synthetic-defect fixtures;
- proposed domain, range, cardinality, identity, unknown-value, version, and
  compatibility semantics;
- evidence that the distinction is reusable beyond one source or a declared
  second-consumer/independent-review dependency.

Speculative labels and ranking features remain local. Human approval is
mandatory before promotion into shared or decision-grade vocabulary.

## Completion and Rulespec triage

Every Spicy Regs build, test run, corpus materialization, and retrieval
evaluation classifies each semantic finding as fixed in Rulespec now, added to
the deduplicated Rulespec backlog, retained in this profile, or recorded as
source-data friction. A run is not complete until that triage and its evidence
are recorded. An upstream item closes only after Rulespec implements it and a
Spicy Regs generation verifies the released contract against real carrier
data.
