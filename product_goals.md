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

## Current priority read (July 2026)

One contributor's ordering argument, per the invitation above. Five items
clear a higher bar than the rest — the driving use case fails without them,
or the window to act is closing, or they multiply everything else:

1. **Legacy dataset rescue** — irreversible once the portals go dark; days
   of work; snapshot first, model never-mind-when.
2. **Geo crosswalks + district demographics** — the join layer that
   unlocks localization for every other table; clean Census API.
3. **Medicaid waivers and SPAs** — the highest-value gap, and the
   `proceedings`/`comment_periods` model already fits waiver actions.
4. **State legislation** — hard external deadline: sessions open in January.
5. **Public inspection desk** — a near-free extension of the FR pipeline.

Everything else on this roadmap sharpens the commons; these five make it
viable for its driving use case.

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

### Apportionments and impoundment
OMB is now required to publish account-level apportionments, and GAO issues
Impoundment Control Act decisions. Funds being slow-walked or withheld are
invisible in every other source on this page — a quietly starved account is
upstream of every service cut — and nobody publishes apportionment diffs as
structured data.
**Access:** apportionment-public.max.gov files, diffed over time; GAO
legal-decisions scrape. **Candidate views:** `apportionments`,
`impoundment_decisions`. Joins by agency, account, CFDA.

### Agency web change monitoring
Guidance is not only issued — it is quietly edited. A monitored inventory
of high-value pages (guidance indexes, eligibility manuals, data portals)
with content diffs extends change detection from documents to the web
itself, and gives the legacy-rescue work below a standing sensor instead of
a one-time snapshot. EDGI's website-monitoring practice is the precedent.
**Access:** targeted crawling + content hashing. **Candidate view:**
`web_changes`.

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
The local candidate now ingests official Supreme Court PDF opinion packages as
`court_opinions`, which supplies real judicial text for document processing.
It does not yet connect the broader `court_dockets` corpus to authored opinions
or to challenged rules. That linkage would let APA challenges read as a thread:
rule → challenge → outcome. **Current access:** official Supreme Court PDFs.
**Remaining access:** CourtListener/RECAP API. **Remaining extension:**
broader opinions plus a rule↔case crosswalk.

### Information Collection Requests (PRA/ICR pipeline)
The prospective twin of legacy rescue: before a survey, form, or program
report is changed or killed, an ICR moves through OMB with its own comment
window. This is where measurement itself gets defended — and commenting is
open to anyone, including organizations restricted to education.
dataindex.us (Ross/Dick; GPL code, CC BY-SA content) already aggregates
ICRs, surveys open for comment, and dataset-loss signals — consume and
join rather than rebuild, and treat their loss signals as standing
triggers for the legacy-rescue snapshots below. Joins by agency and OMB
control number; reginfo.gov is the shared source system with the OIRA
entry above.
**Access:** dataindex.us; reginfo.gov ICR records for provenance.
**Candidate views:** `icr_actions`, `dataset_status`.

### CBO cost estimates
The fiscal facts cited in every legislative fight; joins to
`congress_bills`.
**Access:** CBO site. **Candidate view:** `cbo_estimates`.

### CMS provider and facility data
Care Compare and Payroll-Based Journal staffing data — facility-level
quality, geocodable to districts. Ownership files add the roll-up chains
behind facility operators, joining the entity graph.
**Access:** data.cms.gov bulk. **Candidate views:** `cms_facilities`,
`cms_staffing`, `cms_ownership`.

### State AG multistate actions
Multistate suits and comment letters — a growing share of how federal
policy is contested.
**Access:** press-release scraping, perhaps the ~15 most active offices.
**Candidate view:** `ag_actions`.

### State-side waiver notices
Before an 1115 or 1915 action reaches Medicaid.gov, the state must run its
own public comment period on the state Medicaid agency's site — the
earliest possible waiver signal, and a participation window open even to
organizations restricted to education. Fifty fragile pages, but the top
10–15 states would cover most of the caseload.
**Access:** per-state scraping. **Candidate view:** `state_waiver_notices`.

### Federal workforce and vacancies
Agency hollowing-out is service degradation announced nowhere: OPM FedScope
headcounts by agency and component as a time series, plus PLUM Act
appointee and vacancy data (acting-vs-confirmed also matters to
litigation).
**Access:** OPM FedScope cubes; plum.opm.gov. **Candidate views:**
`agency_headcount`, `appointee_vacancies`.

### Single audits
Grantee audit findings from the Federal Audit Clearinghouse — early warning
for both genuine grantee distress and pretexts for defunding.
**Access:** api.fac.gov (official API). **Candidate view:** `single_audits`.

### CRS reports
Free expert analysis — the policy shop a two-person organization doesn't
have. Modest uniqueness, high joinability to `congress_bills` and
hearings. EveryCRSReport.com already publishes a bulk machine-readable
index plus full text (public domain), including pre-2018 reports the
official portal lacks, and preserves report versions — consume that feed
rather than scraping the official site, which stays as provenance anchor
and freshness cross-check.
**Access:** everycrsreport.com bulk CSV + files; crsreports.congress.gov
for verification. **Candidate view:** `crs_reports`.

### Legislator and committee reference
Rosters, committee assignments, and stable IDs — the join layer that makes
"route this to the chair" computable. Complements `state_legislators`
(Tier 1, via Open States) with federal rosters and committee assignments
at both levels. Public facts only; who-knows-whom belongs to consumers,
never here.
**Access:** unitedstates/congress-legislators and openstates/people, both
openly licensed. **Candidate views:** `legislators`,
`committee_assignments`.

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
  what roles. Nonprofit filings (ProPublica's 990 API) and campaign finance
  (OpenFEC) join here as external spines — consume, don't re-ingest.
- **HCBS waitlist series** (`hcbs_waitlists`) — the most-cited advocacy
  number in the aging/disability field, published nowhere as a maintained
  series. Realistic production is attested contribution through downstream
  curation gateways (KFF's annual survey, state agency pages, verified
  field observations), with methodology and attribution carried per row.
- **SSA operational metrics** (`ssa_service_metrics`) — field-office
  closures, wait times, disability backlogs; service collapse as a time
  series.
- **FACA advisory committees** (`faca_committees`) — charters, membership,
  meeting cadence; purges and disbandments are cheap-to-detect leading
  indicators. GSA publishes exports.
- **State lobbying registrations** (`state_lobbying`) — fifty fragmented
  disclosure regimes nobody aggregates; pilot a few states.

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

**Current decision record:** [Axiom ecosystem assessment for Spicy
Regs](docs/axiom-ecosystem-analysis-2026-07-28.md). It supersedes repository
counts and live-service observations in the earlier summary below.

Earlier code review, 2026-07-26 (org-wide review of `axiom-corpus`,
`axiom-encode`, `axiom-scrapers`, `rulespec-us`, `axiom-bills`,
`axiom-rules-engine`):

- **Ingestion overlap is narrower than the earlier note assumed.** Their
  Federal Register adapter exists but has effectively run once (a
  term-scoped smoke test yielding 3 provision rows, docket/RIN fields in
  an untyped metadata blob). Their documentary layer is a stub: no
  dockets, RINs, comment windows, proceedings, litigation, or
  point-in-time text lineage (their own `historical-versioning.md`:
  `as_of` is a no-op outside eCFR; the version-aware migration was
  reverted). Nothing in our deterministic edge layer is duplicated
  there — do not cut scope on their account.
- **One genuine new overlap: `axiom-bills`** pulls Congress.gov plus 22
  state legislatures directly, overlapping `congress_bills` and the
  Tier-1 state-legislation plan. Decide feed-versus-parallel with them
  before building state bills.
- **Semantic crosswalk: consume, don't build.** `rulespec-us` ships a
  CI-enforced reverse index (`.axiom/index/provisions_to_rules.json`,
  ~1,184 regulation citation paths) mapping each corpus provision to
  every dependent encoding. It joins to our `rule_targets` /
  `cfr_sections` by rendering `us/regulation/{title}/{part}/{section}` —
  federal CFR only; their state paths are not uniform.
- **Change feeds: the seam is sharper than hoped.** They already have a
  staleness command (`axiom-encode check-source-staleness`) and a weekly
  CI job, but it is vacuous today (43 of ~4,484 modules pinned) and
  lagging by design — hash-only, firing after re-ingest, with no reason,
  dates, or lead time. Our leading signal — which FR documents amend a
  section, publication and effective dates, docket/RIN, comment window,
  proceeding stage — is precisely the trigger their own platform plan
  (A5) declares. Frame the feed as supplying that trigger, keyed on
  their citation-path identity; don't ask them to build a consumer.
- **Their provenance engineering exceeds the earlier assumption**:
  byte-verified provision anchors with char offsets and parent-body
  digests, `machine_asserted` vs `label_inferred` confidence grading
  (independent convergence on our deterministic-vs-inferred split), and
  Ed25519-signed content-addressed releases. Their new `receipt` repo
  (chained attestation manifests, RFC 3161 witnesses) is worth reviewing
  before we harden our own attestation chain. Do not offer them
  point-in-time *text* lineage — they have scoped that internally;
  documentary lineage is the complementary, non-colliding offer.
- Still: a partner to track and shape rather than a dependency, for
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
   Expirations belong in the same frame: waiver end dates, demonstration
   sunsets, and effective dates make the calendar forward-looking — "what's
   scheduled to happen," not just "what happened."
5. **Provenance on every row.** Source URL and retrieval date, always — the
   dataset should be citable in testimony without apology.
6. **MCP parity.** Every new view lands in `list_sources`/`describe_table`
   the day it ships; assistants are a primary consumer, not an afterthought.
7. **Aggregation safety.** Public record does not mean safe to publish in
   every shape. Commenter PII and membership-revealing aggregations get no
   convenient query paths — the commons makes the government legible, not
   the citizens who participated.
