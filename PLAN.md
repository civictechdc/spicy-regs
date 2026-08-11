# SpicyRegs plan

Recorded 2026-08-11 against SpicyRegs `6dbe181ccec7` and RefSpec
`3c1b94ace91f`. This file carries the SpicyRegs-owned work: the two gating
decisions, the branch reset, the `citations.py` boundary fixes, the upstream
pull-request train, the `rkaf_projection.py` boundary freeze, the
editable-install crossing, and the canonical-JSON profile rule.

The cross-product ownership boundary and its payload rule are REF-024 in
`RefSpec/docs/decisions.md`. This plan cites REF-024 by identifier and does
not restate it.

Ordering, as a constraint of this plan: sections 1 and 5 land before the
first pull request of section 4. Section 3 rides inside that first pull
request.

## 1. Two decisions gating the pull-request train

### 1a. URN spelling

`urn:spicy-regs:` stands at ~7,270 sites, `urn:spicyregs:` at 209 lines (341
occurrences across 78 files; both counts re-derived 2026-08-11). The
minority spelling is the newer v3 writer. That makes this a containment
question, decidable by count, and the count decides for `urn:spicy-regs:`.

Respelling happens in the same change that decides it, accepting that
artifact identifiers move.

### 1b. Byte versus codepoint offsets

One written page. v3 counts bytes and v3 is the live sink path. Offsets are
bytes.

### Landing constraint

Both decisions land before the first upstream pull request. The payload
carries `urn:spicy-regs:` in `src/spicy_regs/ontology/citations.py`,
`src/spicy_regs/ontology/attestations.py`, and
`src/spicy_regs/ontology/relation_findings.py`. Respelling after those files
land upstream is a force-push that does not exist.

## 2. The branch reset, as a ref move

Executed 2026-08-11: `archive/local-work-2026-08-09` is a tag, and `main`
sits on `origin/main` (`f1fcb8c9c883`) plus this plan. The procedure, kept
for the record:

1. Rename `archive/local-work-2026-08-09` to a tag. An archive that moves is
   not an archive.
2. `git branch -f main origin/main` — no checkout, no `--hard`.
   `git clean -fdx` stays forbidden in that worktree.

Facts already answered on disk:

- `.github/workflows/deploy-mcp.yml` is tracked and byte-identical on
  `origin/main`.
- `mcp-server/api/index.py` and `mcp-server/api/_published.py` revert as a
  pair under the ref move.

Together those two facts mean the deployed function is unaffected by the ref
move. `_published.py`'s eventual destination is a standalone disposition
gating nothing here; it belongs to the pull-request train in section 4.

## 3. `citations.py` boundary fixes

These ship inside the first pull request of section 4, not as a second
upstream round. The defects: the `citations.py:657` phantom loop and the
`:32` sentence-final drop — one missing boundary discipline in one grammar.

- Port `_CFR_SECTION_CAPTURE` from SpicySearch `identifiers.py:149`. The
  `_LEFT`/`_RIGHT` guards are not part of this port; they fix a different
  defect.
- `src/spicy_regs/ontology/citations.py` carries 25 compiled patterns with
  zero boundary lookbehinds, so the single-pattern port fixes 1 of 25.
- Two regression cases, both mandatory: `'1CFR9,10CFR1'` and
  `'49 CFR 900.42.'`.
- Add the missing disk-to-lock direction in `validate_body_cache`.
- Bump the snapshot schema version, add a version-aware field check, and add
  at least one test that opens `snapshots/`.

## 4. The upstream pull-request train

The order is forced. `ontology/` leaves first: 14 of 18 `docpipeline/` files
import it, and `ontology/` itself carries zero `refspec` strings.

Each description leads with blast radius — the 87.9% row collapse and the
breaking `comment_periods` schema — and raises the pymupdf AGPL question in
the pull request rather than waiting to be asked.

`build_fr_docket_links.py` ships separately, on its own weaker evidence.

Every pull request is gated by the REF-024 payload test: no import, path, or
URN in the payload names an owned product. The test runs per pull request,
against that request's payload:

- `src/spicy_regs/ontology/receipt.py:2210` defaults to
  `Path.cwd().parent / "rulespec"` and fails the first (`ontology/`) pull
  request today.
- `src/spicy_regs/docpipeline/rkaf_projection.py:2334` does
  `from refspec import (...)` and fails when the train reaches
  `docpipeline/`.

## 5. `rkaf_projection.py` boundary freeze

`tests/test_rkaf_projection_boundary.py` carries the freeze, written
2026-08-11 against `src/spicy_regs/docpipeline/rkaf_projection.py` at
`6dbe181ccec7`, before the train in section 4 ships `ontology/`. It runs in
the default suite: `testpaths = ["tests"]`, no marker, three tests, 0.1s.

The frozen surface, AST-derived from the file: 17 outbound import statements,
16 naming `spicy_regs` and 1 naming `refspec` at line 2334. The 16 reach 13
distinct `spicy_regs` modules across four subpackages — `candidate_release`,
`docpipeline`, `enrichment`, `ontology`. Three statements name
`spicy_regs.docpipeline.source`, two name `spicy_regs.ontology.common`, and
the other 11 modules are named once each. Every remaining import in the file
is standard library; the file carries no relative and no dynamic import.

The freeze records module and imported names, not line numbers or file order,
so imports may move within the file. It records every non-stdlib import, not
only the packages present today, so a new sibling product or third-party
dependency fails it too. One added crossing fails two of the three tests; a
relative or dynamic import fails the third. Each failure states that the list
is the priced boundary for the four-way split and that moving it is a
deliberate change to the freeze and to this section together.

The test shape came from `RefSpec/tests/test_atlas_index.py:490-497` and
DocSpec's `tests/test_package_boundary.py`.

## 6. The editable-install crossing

`pyproject.toml:147` sets `refspec = { path = "RefSpec", editable = true }` —
a sibling source-tree consumption REF-024 forbids. The exact pin at
`pyproject.toml:12` (`refspec==0.1.0.dev0`) is already correct. Delete the
`[tool.uv.sources]` override once the RefSpec package is consumable from an
installed wheel; RefSpec's `PLAN.md` schedules the input resolver that makes
it so. The payload test in section 4 does not cover this crossing — it gates
what ships upstream, and this line never ships.

## 7. Canonical-JSON profiles precede any dedup

Counted 2026-08-11 at the commits in this plan's preamble: the workspace
holds ~33 canonical-JSON implementations — 9 in `src/spicy_regs`, 7 in its
tools, 9 in `RefSpec/src`, 4 in `RefSpec/tools`, 2 in `rulespec/tools`, 2 in
SpicySearch, 2 in DocSpec. At least three profiles exist de facto:
SpicySearch `artifact_protocol.py:127-128` rejects every float and bounds
integers; SpicySearch `canonical.py:213` permits floats and has ~100 call
sites including float-bearing metric reports; SpicySearch
`atlas_search_view.py:279` encodes a third (REF-JSON).

Name the profiles before merging any implementation — RFC 8785 for artifact
identity, a float-permitting profile for reports — then converge each behind
a shared conformance vector set. Deleting `canonical.py:213` as a one-line
dedup would make eight subsystems raise on any float payload.
