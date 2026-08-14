# Document-population captures

Exact publisher bytes for responses that *enumerate* documents rather than
carry one. `document-files/` holds a single document's renditions; these hold
the listings that say which documents exist, which is where acquisition
coverage is decided.

Every file is bound to its digest, byte length, publisher URL, and observation
timestamp in `document-population-capture-manifest-v1.json`.
`spicy_regs.sources.document_populations.read_capture` re-verifies digest and
length on every read, and `tests/test_document_populations.py` parses each
capture and pins what it enumerates.

These arrived from RefSpec, which captured them; RefSpec pinned the same
digests and byte lengths, and the bytes here are identical to the bytes it
verified.

| File | Bytes | SHA-256 | Publisher URL |
|---|---|---|---|
| `cbo-119congress-cost-estimates-2026-08-04.xml` | 375,365 | `edc957a1115320f1c0da4b02c33d1af146a3c508592ee20b4909e0a8db44d968` | `https://www.cbo.gov/rss/119congress-cost-estimates.xml` |
| `cbo-datadome-challenge-real-capture.html` | 770 | `07d681cd0aa832c1132ba2b8d323693990cf27c818e8b064b0f92ebddda58e66` | `https://www.cbo.gov/cost-estimates/xml` |
| `fcc-ecfs-filings-2026-08-03.json` | 51,284 | `4393e9c73ab5e12e25c79a707ca85856ba1d9cc1c3eccdfdfa235223f17773da` | `https://publicapi.fcc.gov/ecfs/filings?limit=25&sort=date_disseminated,DESC` |
| `govinfo-package-summary-cfr-2023-title1-vol1-2026-08-03.json` | 1,532 | `705a28865a4fba746e8deb4aff05a21bbd63534201e74c5320f56d505ca3d79e` | `https://api.govinfo.gov/packages/CFR-2023-title1-vol1/summary` |
| `govinfo-premis-cfr-2023-title1-vol1-mini-2026-08-03.xml` | 4,268 | `afeba6d9e48f502c911ef0ec1400accdbaa5cad5d7d056672dce6a54d1326417` | `https://api.govinfo.gov/packages/CFR-2023-title1-vol1/premis` |

## What each one is

**CBO, 1,058 publications.** CBO's per-Congress publication feed for the 119th
Congress — its own `<response>`/`<item>` XML, not RSS 2.0, one item per
publication with title, date, publication URL, and bill number. The
publication URL (`https://www.cbo.gov/publication/61150`) is the document's
identity. This feed carries none of CBO's topic labels, budget-function codes,
mandate flags, or PAYGO facets; those live on `cost-estimates/xml`, which is
the second capture.

**CBO, refused.** The real body `https://www.cbo.gov/cost-estimates/xml`
returned instead of the feed: a DataDome edge challenge, captured
byte-for-byte. It is kept because a parser that reads it as an empty feed
loses the whole population silently, and this is the exact body that has to
fail closed.

**FCC ECFS, 15 proceedings.** One page of the ECFS filing search — 25 filings,
each embedding the proceedings it was filed into, 40 embeddings naming 15
distinct proceedings. `limit` and `sort` are part of the resource identity and
stay in the pinned URL; an API key is a credential, not part of the captured
resource, and is absent.

**GovInfo, 1 CFR package.** The package summary for `CFR-2023-title1-vol1` and
that package's PREMIS preservation record. The PREMIS record is where the
publisher states the SHA-256 its own renditions must hash to — 814,758 bytes
of XML and 572,151 bytes of HTML, each with its digest — so a downloaded CFR
volume can be checked against GovInfo rather than against ourselves.

The `-mini-` in that filename is RefSpec's and RefSpec explains it nowhere:
the file carries two file objects while the package summary lists six
rendition roles, and GovInfo is known to publish fixity for only some of a
package's objects, so whether these bytes are the whole PREMIS response or an
excerpt of it is not established. The digest pins the bytes either way; it does
not certify them as the complete preservation record, and nothing here treats
the two digests as covering the package.
