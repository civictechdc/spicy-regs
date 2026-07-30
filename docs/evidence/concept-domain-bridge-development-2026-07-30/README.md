# ICPSR to Federal Register concept bridge

## Result

The one-hop bridge improved all seven selected lookup cases against the real
629-concept Federal Register development release.

| Query phrase | Existing rank at 12 | Connected rank at 12 |
| --- | ---: | ---: |
| `constitutional rights` | 7 | 1 |
| `hazardous materials` | 4 | 1 |
| `medical professions` | 1 | 1 |
| `trade unions` | 1 | 1 |
| `housing projects` | 3 | 1 |
| `asylum seekers` | not returned | 1 |
| `vocational training` | 1 | 1 |

The existing selector reached 6 of 7 targets at 12. The connected selector
reached 7 of 7 and ranked every selected target first. `asylum seekers` is the
net-new result; three more targets moved from lower ranks to rank 1. The three
existing rank-1 cases remained rank 1.

These are selected development probes, not a sealed or independently reviewed
quality evaluation. They establish that the implementation can use a concept
from one domain as a search anchor and return a separately identified,
authorized concept from another domain.

## Inputs

- ICPSR Subject Thesaurus source:
  `sha256:1875e0331a8403c00fa47a3ededca98c902f55d0b84d70884543ed1d2db629ff`
  at Git revision `6e2651e55fb42b119a167f34000ec728d1206865`.
- RefSpec bridge artifact:
  `sha256:41b08a28a4bd13de7cd0dbd7929adf780768c2f394d44db50a9ea6f280011c52`.
- Federal Register managed release:
  `urn:ref:fr-thesaurus-1995:release:1995-11-16-preview`,
  release-content digest
  `sha256:cd2625d687ec56a7026fdd71c172719943d4b026d3d1279b9adaa2bfa9c57e63`.
- Managed-release bundle manifest byte digest:
  `sha256:7d7851672c49df8fc6c6d087c8be42c5b78d2de59245c128ac7a6190bcd0140c`.
- Mapping-path digest:
  `sha256:0832d5d8aa72769041532d1894f1c389cc7e28e06b261c32bdab5aa4351bf861`.
- Baseline selector: `anchored-hybrid-v2`.
- Connected selector: `anchored-hybrid-v2+mapped-neighbors-v2`.

The Federal Register source was reacquired from its pinned National Archives
URL and reproduced its expected byte digest before the comparison.

## Guard checks and iteration

- Source-domain anchors never enter the candidate output. Only members of the
  candidate-authorized Federal Register release are returned.
- Results retain the mapping identifier, relation, direction, source and target
  concepts, and both release identifiers.
- The diagnostic run keeps a digest-pinned candidate-selection ledger with the
  final rank, score state, indexed representation version, bridge artifact,
  mapping-set digest, and exact one-hop path. A selected model judgment joins
  back to that same path by segment and concept. When several routes generated
  one candidate, the ledger distinguishes every route from the route that
  determined the final rank.
- Expansion stops after one mapping edge and does not infer mappings from equal
  labels.
- Exact preferred labels receive more weight than alternate labels. A more
  specific exact phrase can still outrank a shorter preferred label.
- A label loses weight as the number of distinct source vocabularies that
  author the same normalized wording increases. This discounts broadly reused
  terminology without treating repeated wording as equivalence.
- Direct matches receive full route weight. `exactMatch` and `closeMatch`
  neighbors remain strong; `narrowMatch`, `broadMatch`, and `relatedMatch`
  receive progressively lower discovery weights. An exact specific output
  term therefore stays above a broader or merely related mapped neighbor.
- `warrants` produced the same results with and without the bridge because the
  ambiguous ICPSR alias was intentionally omitted.
- `low income housing assistance` produced the same top five results with and
  without the final bridge.
- The first bridge draft included ICPSR's `low income housing` alias. It moved
  Federal Register `Public housing` ahead of the more specific
  `Low and moderate income housing` result. The final bridge removed that query
  path while retaining the pinned source evidence.
- `hazardous materials transportation` kept the exact Federal Register
  `Hazardous materials transportation` concept at rank 1; the mapped
  `Hazardous substances` neighbor remained below it.
