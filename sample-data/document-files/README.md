# Actual-file document fixtures

`bill-html-short.html` and `cfr-xml-short.xml` are exact publisher bytes from
the sealed 2026-07-24 source cache. Their capture manifest includes source
identity, source-issued edition, retrieval date, URL, and byte digest, so the
actual-file release command can publish them as source-complete conformance
data.

`gao-html-3.html` is a parser-quality regression, not a captured rendition for
publication. It contains the complete real GAO report from
`https://files.gao.gov/reports/GAO-26-108625/index.html`, with CRLF line endings
normalized to LF so it can be maintained as a text fixture. The original
captured bytes have SHA-256
`adf0b8ea4df109f68823c0998dffbaf149cd74bb64243e129ba2fba2a94390c`;
the normalized fixture has SHA-256
`fe43fca7a7efdc47dd46442e07585b4ebe34f6b1a96d8770fe3db10097ccd6f9`.
Tests use it to prove that report passages remain searchable while site
navigation, footer, scripts, styles, hidden content, and comments do not.
