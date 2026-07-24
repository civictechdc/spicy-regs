# Rulespec rulemaking stabilization report

**Task:** `RULE-007`
**Status:** Repaired release-candidate rehearsal passed; final clean paired receipt pending
**Run date:** 2026-07-24
**Publication:** Local only; no R2 upload, Rulespec release, tag, or package publication
**Independent review:** Open; the maintainer-operated simulation is not a non-originating-consumer review

## Result

The Experimental Rulespec US rulemaking contract and the Spicy Regs projection
have been repaired against every graduation precondition in the 2026-07-24
adversarial review. The paired full-corpus rehearsal passes with zero receipt
validation failures. The rulemaking architecture remains sound, but the module
must stay Experimental until a maintainer publishes the repaired Rulespec
release and a non-originating consumer reviews or ratifies it.

The repaired Rulespec L0 contract digest is
`sha256:ea9b899ba92955b83638ece811d7a4b744dd912f72e19290e32c97508674de1c`.
This is a content pin to an unreleased candidate, not a release claim.

The complete finding-by-finding disposition is in Rulespec
`thoughts/reviews/2026-07-24-rulemaking-repair-matrix.md`. It covers all 20
confirmed findings, F-13's deferred trigger, the three agenda decisions, their
normative changes, generated enforcement, and executable fixtures.

## Paired provenance

| Item | Rehearsal value |
| --- | --- |
| Pre-repair Spicy Regs commit | `b3c268c5408ab9b4c1dff97ed63d0bd57ca5c765` |
| Baseline runner commit | `f693c69c5961ea362890df084236be11c98bab3e` |
| Baseline runner delta | Behavior-preserving RIN/docket indexes for the otherwise quadratic prior-continuity scan |
| Baseline worktree | Clean detached worktree at `/tmp/spicy-regs-rule-baseline` |
| Starting Rulespec commit | `957662e151c194ac81dd248727e0a550d7f75c55` |
| Starting Rulespec digest | `sha256:836968b28f3b86283f53c57ae5c9ab8ebd77e96531cd4751476f1a5ee3d296f2` |
| Repaired Rulespec digest | `sha256:ea9b899ba92955b83638ece811d7a4b744dd912f72e19290e32c97508674de1c` |
| Baseline snapshot | `snapshot_56f2ce22dd4b59f599e684a86413ca63` |
| Candidate rehearsal snapshot | `snapshot_c0b15c6aee3780c0a40a924f59b591bf` |
| Candidate implementation commits | To be frozen and recorded by the final clean receipt |

The baseline performance patch changes lookup complexity only. Targeted
continuity tests pass at the baseline runner commit, and the report treats its
parent as the semantic pre-repair version.

## Identical input and prior-state proof

The receipt compares the complete `inputs` objects, not filenames or
timestamps. Baseline and candidate are equal byte-for-byte across all source
and prior-state records:

| Input | SHA-256 |
| --- | --- |
| `dockets.parquet` | `b14cd488b7898391cff448ac4de19f85936072dcb1aa105da32eea88e6fd7938` |
| `documents.parquet` | `52f085f9ec2ee0c08fe3fb59bcd789bfef34000f87608ea36af9a6adbacfb04d` |
| `federal_register.parquet` | `ac18315faa8be4a8d3656e758597d672c5d85c23cc6f8fde0ac53c9295b22bf2` |
| `fr_docket_links.parquet` | `b3409f0ada792a8c9534edcf87c290a8b39e482e4803f08656bfa9de4504fd45` |
| `unified_agenda.parquet` | `e6862d5d6a5300f10c70eeaf321f1e82e1f5332f71069d07723cc584ee6a85ae` |
| Prior `proceedings.parquet` | `c295b17b06d00a4a43221c82440642a3cbe09bba48cd907bad57f45d6ff7cb29` |
| Prior `concepts.parquet` | `f338b7c8a1e6aae1f938a50a7b22936085b9e41efd2315c33077a19e903f0ddf` |
| Prior `concept_assignments.parquet` | `7b9d122db5b0d6293fe082377fdd5d5a33309948492f21c54789e3d37fe63f77` |
| Prior `concept_events.parquet` | `06c4987dd2ec73af084fe0004b16da1d3ea1e8306758f3ea583d611cb26abe99` |
| Prior snapshot | `snapshot_b40ccfbd464a1bb8ee7b6a2d6f2310df` |

## Before and after

The apparent row reductions are correctness fixes. The earlier pipeline treated
heterogeneous Federal Register labels as regulations.gov identifiers and
connected unrelated dockets through reused RINs. The repaired pipeline requires
source-of-record membership and evidence-qualified component joins.

| Measure | Pre-repair | Repaired rehearsal | Interpretation |
| --- | ---: | ---: | --- |
| Rule-target rows | 335,008 | 40,546 | False heterogeneous docket edges removed |
| Rule-target invalid docket syntax | 247,229 | 0 | Registry grammar enforced |
| Rule-target docket absent from source | 47,222 | 0 | Source membership enforced |
| Rule-target invalid compact CFR | 12 | 0 | Canonical transform enforced |
| Proceedings | 341,492 | 312,298 | False components/joins removed |
| Multi-docket proceedings | 21,007 | 88 | Only explicit co-identification remains |
| Proceeding invalid docket syntax | 84,888 | 0 | Invalid labels stay in raw source only |
| Proceeding docket absent from source | 12,215 | 0 | No uncorroborated Docket identity |
| Proceeding self-supersession edges | 305,807 | 0 | Stable row identity no longer becomes a semantic self-edge |
| Agency-code values mapped as Authority placeholders | 280,444 | 0 | Agency identity is not legal authority |
| Comment-period rows | 283,367 | 254,445 | Correct anchors and coalescing replace false joins |
| Docket-only comment periods | 0 | 1,845 | Valid unresolved-Proceeding windows retained |
| Candidate receipt failures | Not available | 0 | Every claimed carrier value passed |

Candidate rule-target sources are 22,738 document/FR cross-posting edges,
16,467 Federal Register CFR citations, 1,030 Unified Agenda CFR citations, 285
docket RIN edges, and 26 document RIN edges. Of the 40,546 rows, 39,426 carry a
compact citation target.

The repaired `proceedings` artifact contains 38,857 RIN evidence values and
36,783 citation-level target IRIs. It intentionally emits zero immutable
`proceedingAffects` or `proceedingProduces` edition relationships because the
source snapshot cannot resolve the pre- and post-action editions. All 36,783
are therefore counted as unresolved edition targets rather than mislabeled
immutable Artifacts.

Current-stage coverage is 82,508 final, 13,929 proposed, 6,391 withdrawn, 779
long-term, 774 supplemental, and 79 prerule. Every non-null stage agrees with
the unique latest dated stage-family event; equal-date disagreement yields
unknown.

The repaired comment output includes 13 joint-Proceeding intervals and 682
multi-Docket intervals. It retains 15,222 ambiguous Federal Register source
intervals and 8,966 ambiguous regulations.gov-document intervals through
source-backed Docket anchors; coalescing produces the 1,845 Docket-only output
windows. It skips 8,151 inverted intervals and 216,333 intervals with neither a
uniquely resolved Proceeding nor a source-backed Docket. Those source records
remain in their authoritative tables. They are data friction, not silently
invented semantic claims.

## Projection corrections

- Proceeding, Docket, Artifact, Authority, and provenance roles remain distinct.
- `hasAuthority` is optional. `agency_code` remains useful metadata but never
  mints a legal Authority placeholder.
- RIN is repeatable identity evidence, not globally unique Proceeding identity.
- Proceeding stage uses the six `proceeding-*` event IRIs and must agree with
  the latest stage-family event.
- Compact CFR targets project through `proceedingAffectsCitation`; immutable
  pre/post-action edition links remain absent until resolved.
- Distinct merge/split identities may use `proceedingSupersedes`; reusing the
  same stable id is local row-version continuity only.
- Comment periods carry repeatable Proceeding and Docket anchors, at least one
  of which must exist; opening Artifacts are separate from later evidence.
- Cross-postings remain one Artifact per source posting. Of 3,061 non-null raw
  `documents.fr_doc_num` values, 3,029 have exact Federal Register
  source-membership evidence and project in both canonical directions. The 32
  nonmembers remain raw and unprojected.
- `us-regsgov` accepts real underscore and single-segment forms, but lexical
  validity never substitutes for source membership.

## Candidate artifact rehearsal

The rehearsal manifest binds exactly the seven declared ontology outputs:

| Artifact | Rows | SHA-256 |
| --- | ---: | --- |
| `rule_targets.parquet` | 40,546 | `10b2de8291fc82a06d6297ead8536691efc241a09834074048e617cb6a1633ac` |
| `authority_edges.parquet` | 10,618 | `52d8270114312491b59cf72b4054f1771779ca705d3b3f08580c3b14b02ef661` |
| `proceedings.parquet` | 312,298 | `c657cdf24d1a01d0a70da10b44198bc8b68b00968af89b6e879e9870d760ec20` |
| `comment_periods.parquet` | 254,445 | `0e68121b7ce9a287f2421cf7d0fc5007b6a3302e62f8fc95da588a9b328d84f2` |
| `concepts.parquet` | 901 | `f338b7c8a1e6aae1f938a50a7b22936085b9e41efd2315c33077a19e903f0ddf` |
| `concept_assignments.parquet` | 24 | `7b9d122db5b0d6293fe082377fdd5d5a33309948492f21c54789e3d37fe63f77` |
| `concept_events.parquet` | 901 | `06c4987dd2ec73af084fe0004b16da1d3ea1e8306758f3ea583d611cb26abe99` |

The receipt verifies manifest identity, exact artifact set, byte counts, hashes,
row counts, schemas, source/prior hashes, identifier transforms,
source-of-record membership, domain/range references, stage agreement,
cross-posting eligibility, comment anchors/dates/evidence, and semantic
continuity. The clean final receipt will additionally run and hash the baseline
and candidate builds, Rulespec compile/full test, Spicy Regs full test,
dictionary check, full lint, and partner L0 audit.

## Remaining friction and human gates

The following are explicit unknowns or external gates, not skipped work:

- 903 authority rows are unparsed and 3,430 are partial; raw source text and
  provenance remain available.
- Exact immutable CFR and authority editions need a source-backed resolver
  before decision-grade relationships can be claimed.
- 32 raw cross-posting strings lack exact membership in this Federal Register
  snapshot and remain unprojected.
- F-13's comment-period-kind distinction remains a trigger, not vocabulary,
  until an in-scope consumer exposes structured evidence.
- A maintainer must review and publish the Rulespec release, then update the
  Spicy Regs version pin from candidate digest to released version plus digest.
- A non-originating consumer must review the repaired terms/shapes or ratify the
  simulated review against them.
- Publication of the candidate corpus requires explicit maintainer approval;
  this exercise used `--skip-upload`.

No release, tag, upload, independent-review claim, concept promotion, or
decision-grade legal-effect assertion was made by this run.
