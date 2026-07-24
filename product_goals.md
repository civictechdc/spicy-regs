# Product Goals: Data Source Roadmap

Spicy Regs began as an open mirror of regulations.gov. Its newer views —
Federal Register, Unified Agenda, Congress, courts, spending, lobbying —
point at a larger goal: **an open regulatory-intelligence commons**, the
connective tissue between "an agency is about to act" and "someone affected
finds out in time to participate."

The driving use case: small advocacy and service organizations (1–5 staff)
that lost their federal early-warning infrastructure and need to track
rules, waivers, guidance, grants, and litigation. What serves them serves
journalists, researchers, and every other under-resourced watcher of the
administrative state.

This is a roadmap of candidates and rationale, not a work plan — tiers
reflect our current read of value and effort, and contributors are welcome
to reorder it with better arguments. Where an idea has already moved from
roadmap to spec, the entry points there.

## What makes a source worth adding

1. **Early-warning value** — surfaces action before or as it happens, not after.
2. **Uniqueness** — gaps nobody else publishes as structured open data.
3. **Feasibility** — official API beats scraping; scraping beats FOIA.
4. **Joinability** — links to existing views by agency, docket, RIN, entity, or geography.

---

## Tier 1 — high value, feasible now

### OIRA review pipeline (reginfo.gov)
The semiannual Unified Agenda is already ingested. Missing are the live
signals: rules under EO 12866 review (the step before Federal Register
publication) and the 12866 meeting log (who met with OIRA about which
rule). These would be the earliest public signal a rule is coming, and
nobody publishes the meeting log as structured data.
**Access:** reginfo.gov XML/scrape. **Candidate views:** `oira_reviews`,
`oira_meetings`. Joins via RIN.

### Federal Register public inspection desk
Documents filed but not yet published — same-day warning, 1–3 days ahead of
the printed issue.
**Access:** official FR API. **Candidate view:** `public_inspection`. A
small extension of the existing pipeline.

### Medicaid waivers and state plan amendments
Section 1115 and 1915(b)/(c) waiver actions — applications, amendments,
renewals, and their own comment windows — live on Medicaid.gov, not
regulations.gov, and nobody publishes them as structured data. Waiver
actions decide who receives home- and community-based services, state by
state; this may be the highest-value gap in the civic data ecosystem.
**Access:** scrape Medicaid.gov pages + PDFs (no API). **Candidate views:**
`medicaid_waivers`, `medicaid_spa`.

### Sub-regulatory guidance
Policy increasingly moves through guidance that skips notice-and-comment:
State Medicaid Director letters, informational bulletins, FAQs, manual
updates. A starting set could be CMS, ACF, SSA, and ED OCR, growing by
contribution.
**Access:** per-agency scraping; change detection matters more than
backfill. **Candidate view:** `agency_guidance`.

### State legislation
Bills, sponsors, and hearing schedules across 50 states — the
most-requested expansion. Open States' openly licensed bulk data offers a
path that avoids building 50 bespoke scrapers.
**Access:** Open States API/bulk (LegiScan as gap-filler). **Candidate
views:** `state_bills`, `state_legislators`.

### Grants and assistance lifecycle
`usaspending_recipients` shows where money went. The other half is where
money opens, closes, and disappears: Grants.gov opportunities and
CFDA-level award flows. A quietly terminated funding stream is an
early-warning event equal to any rule.
**Access:** Grants.gov + USAspending APIs. **Candidate views:**
`grant_opportunities`, `assistance_awards`.

### Congressional hearings
Scheduled hearings, markups, and witness lists — the legislative
counterpart to the Unified Agenda.
**Access:** Congress.gov API. **Candidate view:** `congress_hearings`.

### Inspector General reports
All-agency IG reports, already aggregated at Oversight.gov. Complements
`gao_reports`.
**Access:** Oversight.gov API. **Candidate view:** `oig_reports`.

---

## Tier 2 — high value, moderate effort

### District demographics (ACS)
A join layer, not a corpus: Census tables keyed to congressional and state
legislative districts (population 65+, disability, poverty), so any view
with a geography can answer "how many people in this district does this
affect?" — the question that turns a docket alert into a fact sheet.
**Access:** Census API. **Candidate views:** `district_demographics`, plus
a static `geo_crosswalks` table (ZIP↔district, county↔district). The
crosswalks alone would unlock localization for every other table.

### Court opinions and litigation linkage
Linking `court_dockets` to opinions would let APA challenges read as a
thread: rule → challenge → outcome.
**Access:** CourtListener/RECAP API. **Candidate extension:**
`court_opinions`, rule↔case crosswalk.

### CBO cost estimates
The fiscal facts cited in every legislative fight; joins to
`congress_bills`.
**Access:** CBO site. **Candidate view:** `cbo_estimates`.

### CMS provider and facility data
Care Compare and Payroll-Based Journal staffing data — facility-level
quality, geocodable to districts.
**Access:** data.cms.gov bulk. **Candidate views:** `cms_facilities`,
`cms_staffing`.

### State AG multistate actions
Multistate suits and comment letters — a growing share of how federal
policy is contested.
**Access:** press-release scraping, perhaps the ~15 most active offices.
**Candidate view:** `ag_actions`.

---

## Tier 3 — exploratory

- **State administrative registers** (`state_regulations`) — fragmented,
  scrape-heavy; a pilot with a few states where partners exist would tell
  us whether it generalizes.
- **State budgets** (`state_budgets`) — could start as a documents corpus
  with extracted line items for a few programs.
- **Modeled program-enrollment estimates** (`district_program_estimates`) —
  district-level HCBS, nutrition, and caregiver estimates. These only work
  as clearly labeled estimates with published methodology. Much of this
  already exists at PolicyEngine (below).
- **Cross-corpus entity graph** — resolving organizations across
  `comments`, `lobbying_filings`, `sam_entities`, and
  `usaspending_recipients` into one spine: who is active on this issue, in
  what roles.

---

## Time-sensitive: legacy dataset rescue

When agencies are dismantled, their data portals go dark with little
notice. The AGID portal (Older Americans Act program reports), ombudsman
archives, and adult-maltreatment data now have uncertain custodianship.
Snapshotting at-risk datasets and republishing them as parquet follows
established civic practice (EDGI/Data Rescue) and preserves the only
baseline against which future cuts can be measured.
**Candidate view:** `legacy_program_reports`. Snapshot first, model later.

---

## Adjacent ecosystems

```
TEXT layer    what the government said, when, and who responded
              → Spicy Regs (dockets, rules, comments, bills, filings)
LOGIC layer   what the rules actually mean, as executable code
              → The Axiom Foundation (RuleSpec encodings of statutes/regs)
IMPACT layer  what the rules do to real households and populations
              → PolicyEngine (open-source tax/benefit microsimulation)

STANDING      crossing all three: which claims are authoritative, current,
(governance)  and usable for what — provenance, warrant, usage permission
              → Formspec-Labs rulespec (descriptive governance ontology)
```

Layers hold content; standing holds whether that content may be trusted and
used. The aim is joinability across all four, not absorption.

One fact shapes this landscape (verified July 2026): the logic and impact
layers share leadership. Axiom's founder, Max Ghenis, is PolicyEngine's
co-founder/CEO, and the two orgs have formalized their seam — shared engine
protocol, cross-validation oracles, joint ADRs. That leaves **text↔logic**
— feeding and cross-referencing the encoding pipeline from the documentary
record — as the open seam, and the one Spicy Regs is best placed to serve.

### The Axiom Foundation (github.com/TheAxiomFoundation)
Encodes statutes and regulations as executable, testable RuleSpec YAML:
scrapers → corpus → AI-assisted encoding → Rust engine, across federal,
state, and international jurisdictions. Young and fast-moving.

- **Ingestion overlap.** `axiom-corpus` hits the same Federal Register API
  we do. One of the two corpora could feed the other instead of both
  running parallel ingestion; regulations.gov comments remain uniquely
  ours either way.
- **Semantic links.** A crosswalk from `cfr_sections` to RuleSpec encodings
  would turn "this rule changed" into "these eligibility criteria changed"
  — a diff at the meaning level.
- **Change feeds.** Our change-detection goal (goal 2) produces exactly the
  signal an encoding project needs to know what to re-encode; their
  pipeline is a natural consumer to keep in mind when the feed schema takes
  shape.
- Early-stage: a partner to track and shape rather than a dependency, for
  now.

### PolicyEngine (policyengine.org)
Open-source microsimulation of US, UK, and Canadian tax and benefit policy:
program rules as reviewable code, run against calibrated microdata, with an
API and reform analysis by geography.

- **The impact join.** When a rule changes program parameters, PolicyEngine
  can quantify who gains and loses, by district — turning "comment window
  open" into "here's how many households in your district are affected."
- **Tier 3 estimates mostly exist there.** Their microdata pipeline
  calibrates national, state, and congressional-district geographies
  (county opt-in; city NYC-only), published on PyPI and Hugging Face with
  TRACE provenance — partnering may beat building. For state legislative
  districts, the calibration columns exist but no outputs ship yet, so the
  opportunity is shipping outputs rather than new plumbing.
  (`policyengine-us-data` is archived; its successor Populace is
  pre-release.)
- **Provenance kinship.** Their TRACE declarations and our goal 5 are the
  same discipline; aligned formats would let citations compose across
  layers.
- **Downstream proof.** MyFriendBen's benefit screener runs on their
  engine; they ship a Claude Code plugin (MIT) and AI explanations. Our MCP
  server plus their tooling can already join text to impact today.
- **Licensing.** Engines are AGPL-3.0; the hosted API avoids copyleft
  obligations.

### Formspec-Labs rulespec (github.com/Formspec-Labs/rulespec)
A descriptive governance ontology for legal and regulatory knowledge — "the
things around the rule, not the text of the rule." Despite the name,
unrelated to Axiom's RuleSpec: Axiom encodes what a rule says; this models
the *standing* of claims about rules — what is asserted, on whose
authority, and who may use it for what. Core objects: content-addressed
source artifacts, warrant chains, AI lineage with a named human approver, a
graded usage-permission lattice (search-only through official use), and
supersession as a propagating event.

Why it appears on this roadmap: several projects in this ecosystem have
independently reinvented fragments of that object — our goals 2 and 5,
Axiom's per-module encoding provenance, PolicyEngine's TRACE, and every
consumer's informal "is this citable?" bar. AI made plausible regulatory
content cheap; the scarce resource is increasingly warranted, permissioned,
lifecycle-aware content.

**Now in spec.** The first concrete slice of this collaboration has moved
from roadmap to spec: see the
[Regulatory Ontology Program](docs/superpowers/specs/2026-07-23-regulatory-ontology-program-overview.md)
— US regulatory identifier schemes and a vocabulary-only (L0) conformance
tier on the rulespec side; a rule-identity spine (`rule_targets`,
`authority_edges`), concept tagging, and proceeding/comment-period tables
here. The program doc also carries the longer-horizon direction (stable
vocabulary core, the tag→concept promotion path, the eligibility-runtime
story). Those specs own the details; still at the idea stage are:

- **A change-feed wire format (goal 2).** We detect changes; rulespec
  models their propagation. A shared lifecycle-event envelope could serve
  our subscribers, Axiom's re-encode triggers, and downstream staleness
  tracking with one format.
- **A TRACE ⇄ attestation crosswalk (goal 5).** Three provenance dialects —
  our source rows, rulespec's evidence bindings, PolicyEngine's TRACE —
  that a small crosswalk could make composable end to end.

The working posture is emit-and-reference rather than wholesale
conformance: Spicy Regs stays a parquet catalog and emits standing metadata
for curated subsets. One caveat worth designing around: permission metadata
only has force where something enforces it, and in an open commons
enforcement lives with consumers — so the job here is carrying the metadata
faithfully, not gatekeeping.

---

## Cross-cutting goals

1. **Freshness over depth.** A source polled daily with two fields beats a
   rich backfill updated quarterly. Early warning is the product.
2. **Change-detection feeds.** Every corpus emits "what's new since T" —
   the shape notification systems and agents actually consume. (The
   ontology program's lifecycle work is one path toward this.)
3. **Geographic keys everywhere.** Rows that can carry a state or district
   key gain leverage from it; the crosswalk table would make this cheap.
4. **Comment-window awareness.** Everything with a participation window
   exposes its deadline as a first-class, queryable field. The locally
   implemented `proceedings`/`comment_periods` materialized dataset delivers
   most of this once its first production generation is published.
5. **Provenance on every row.** Source URL and retrieval date, always — the
   dataset should be citable in testimony without apology.
6. **MCP parity.** Every new view lands in `list_sources`/`describe_table`
   the day it ships; assistants are a primary consumer, not an afterthought.
