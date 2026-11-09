# MCP server internals

> Engineering notes for contributors. Not user-facing — for how to *use* the
> server see <https://docs.spicy-regs.dev>; for deploying it see
> [`../deploy/cloudrun/`](../deploy/cloudrun/).

This is the rationale behind the MCP server implementation
(`src/spicy_regs/mcp_server.py`). The source is comment-free by project policy
(documentation lives in markdown), so this document explains what the code does
and why.

> **History:** there used to be a second, hand-mirrored copy at
> `mcp-server/api/index.py` — a dependency-light parallel of the canonical server
> for a Vercel deployment, kept in sync by `test_vercel_copy_in_sync`. The server
> now runs on Cloud Run (see `deploy/cloudrun/`) from the **canonical** module
> directly (importing it pulls only duckdb + mcp, no ETL deps), so the copy and
> its sync test were removed. Everything below is the single canonical server.

## What must never be deleted

**The five `@mcp.tool()` docstrings are not documentation — do not delete them.**
FastMCP reflects over `fn.__doc__` to build the tool descriptions sent to every
client during `list_tools`. `describe_table`'s docstring is how a client learns
which tables are valid; `query_sql`'s is how it learns the available views.
Strip them and the server still runs, but every client goes blind. Verify with
`asyncio.run(build_server().list_tools())`.

**Inline `# type: ignore` / `# noqa` are directives, not comments.** `ty` and
`ruff` gate merges and both read them. `_build_connection` needs `# type: ignore[index]`
on `catalog["namespace"]`.

## Connection setup (`_build_connection`, `_get_connection`, `_apply_security_settings`)

- `SET home_directory` **must precede** `INSTALL`/`LOAD`. DuckDB writes
  extensions under `<home_directory>/.duckdb`, and the default home is read-only
  or undefined on serverless hosts — hence `_resolve_home_directory` defaulting
  to the temp dir (`SPICY_REGS_HOME_DIR` overrides).
- **Do NOT disable `LocalFileSystem`.** It looks like an obvious guard against
  user SQL reading local files, but httpfs reads the system CA bundle off the
  local filesystem on every TLS handshake. Disabling it breaks the only thing
  this server does: the moment a view binds you get `File system LocalFileSystem
  has been disabled by configuration`.
- `SET temp_directory=''` is the sandbox that replaces it. `temp_directory`
  defaults to a local `.tmp` that is read-only on serverless hosts, so a spilling
  query (big GROUP BY/ORDER BY) fails there regardless. Empty disables spilling —
  queries run in memory or fail with a clear OOM.
- `_apply_security_settings` is deliberately separate from `_build_connection` so the
  sandbox is testable without network. Run it after httpfs loads and before any
  user SQL; `SET lock_configuration=true` is last because it freezes everything.

## The read-only statement guard

`query_sql` hands arbitrary SQL to `cursor.execute()`, so without a gate the
public endpoint accepts writes. `COPY ... TO`, `ATTACH`, and `EXPORT DATABASE`
all reach the container filesystem, and on Cloud Run that filesystem is
in-memory — a large enough `COPY` evicts the instance. The service is
`--allow-unauthenticated`, so that is an anonymous availability lever.

The obvious fix, `SET disabled_filesystems='LocalFileSystem'`, is the one thing
that must not be done (see above — httpfs needs the CA bundle). Hence
`_first_write_statement`, which classifies with DuckDB's own parser via
`extract_statements` and admits only `SELECT` and `EXPLAIN`.

**Do not swap the parser for a prefix regex.** The parser is what makes leading
comments (`/* c */ COPY ...`), `COPY` inside a string literal, and stacked
statements (`SELECT 1; DROP TABLE t`) classify correctly. The stacked case
matters most: `execute` runs every statement in the string but returns only the
last result, so a trailing write would otherwise land with nothing in the
response to show for it.

**The two-entry allowlist is not as narrow as it looks.** DuckDB folds
`DESCRIBE`, `SHOW`, `SUMMARIZE`, `VALUES`, `TABLE`, and the FROM-first
shorthand (`FROM comments LIMIT 1`) into `StatementType.SELECT`, so all of them
still run. `tests/test_mcp_server.py::test_read_forms_pass_the_guard` pins that
folding; if a DuckDB upgrade splits any of them into its own statement type,
that test fails rather than clients silently losing a query form.

Matching is on `StatementType.name`, not the enum member, because duckdb's type
stubs do not declare the members and `ty` gates merges — comparing names keeps
the check honest without a blanket type-ignore.

This guard does **not** stop local file *reads*: `read_text`, `read_csv`, and
`read_blob` are `SELECT`s. Nothing sensitive sits on the container filesystem
today (the R2 catalog token arrives as an env var, `getenv` does not exist in
DuckDB, and `duckdb_secrets()` redacts the token), so the exposure is latent
rather than live. It stops being latent the moment a secret is mounted as a
file — if the catalog token ever moves to a Cloud Run secret *volume*, this
needs a companion path check.

## The statement timeout

**DuckDB has no `statement_timeout` parameter.** `SET statement_timeout=...`
raises `Catalog Error: unrecognized configuration parameter` — this was a shipped
runtime crash once, and `tests/test_mcp_server.py` guards the regression. The cap
is a watchdog (`_statement_timeout`) that calls `cursor.interrupt()` and converts
DuckDB's `InterruptException` into a `TimeoutError` so the cause is unambiguous.

`SPICY_REGS_STATEMENT_TIMEOUT` defaults to `790s` in code; the Cloud Run
deployment sets it to `600s` via env (matching the service `--timeout`), so a
runaway query returns a clean `TimeoutError` rather than a platform-killed 5xx.
Keep the app timeout at or below the platform request timeout. In practice the
binding limit is usually the *MCP client*, which typically gives up at 60–120s.
The stdio entrypoint has no platform limit at all.

## Iceberg catalog attach (`_attach_catalog`)

Best-effort by design: a bad token, network failure, or missing table falls back
to the monolithic `comments.parquet` rather than taking the server down. Must run
**before** `_apply_security_settings` locks the configuration.

`INSTALL avro` / `LOAD avro` explicitly, before iceberg, is load-bearing. DuckDB
1.5's iceberg extension reads Avro manifests via a separate `avro` extension and
tries to auto-install it lazily during `LOAD` — but that nested install doesn't
inherit the session's `home_directory` in serverless sandboxes and dies with
`Can't find the home directory at ''`, failing the whole attach so comments
silently fall back to the frozen monolith. Installing it explicitly runs on the
same top-level path that already installs httpfs/iceberg fine. Wrapped in its own
try/except because on DuckDB <1.5 avro isn't separate and `INSTALL avro` errors.

## Why the comments view has a QUALIFY

The catalog table can physically hold duplicate `comment_id` rows. DuckDB's
Iceberg engine has no `MERGE INTO`, so the ETL upsert (`iceberg._merge`) removes
superseded rows with a plain `DELETE` — and `DELETE` does not reliably remove
prior rows on the R2 Data Catalog (the same limitation `dedupe_table` works
around by never deleting). Any re-merged `comment_id` can leave its old row
beside the new one. The `QUALIFY ROW_NUMBER() OVER (PARTITION BY comment_id ORDER
BY modify_date DESC NULLS LAST) = 1` makes the read surface single-valued
regardless, so counts aren't inflated. Physical compaction happens out-of-band via
`scripts/dedupe_comments_catalog.py`. A filter on `agency_code`/`docket_id` pushes
down ahead of the window, so point lookups only dedup rows they touch.

A table in `TABLES` whose parquet isn't published yet (a new source whose first
upload hasn't run) is skipped with a warning rather than breaking every query —
same degradation strategy as the catalog fallback.

## DNS rebinding protection is off in `build_app`

Deliberate. The deployment is reached via `mcp.spicy-regs.dev` and per-deploy Cloud Run
`*.run.app` hosts; FastMCP's default localhost-only allowlist would reject all
of them with **421**. The server is public, stateless, and read-only, so
rebinding protection buys nothing.

## `get_rule_text` and where the text comes from

`get_rule_text(document, section, offset, max_chars)` returns the text of a
Federal Register document, usually a proposed or final rule. It resolves the
argument in this order: a Federal Register document number (`2026-18481`) is
used as is; a regulations.gov document ID uses that row's `documents.fr_doc_num`;
a docket ID picks the docket's documents that have an `fr_doc_num`, preferring
Proposed Rule, then Rule, then the most recently posted, and lists the rest as
`other_notices_in_docket`. It does **not** match on
`federal_register.docket_ids_json`: the Federal Register records the agency's
own docket number there (the Census rule lists `Docket No. 260903-0005`, not
`USBC-2026-0628`), so that join misses. Agency catch-all dockets
(`*_FRDOC_0001`) hold thousands of unrelated notices, so they are refused with
a request for a specific document.

The text comes from GPO's copy on govinfo.gov,
`https://www.govinfo.gov/content/pkg/FR-<publication_date>/html/<number>.htm`,
not from federalregister.gov. The `full_text` URLs the Federal Register API
hands out answer automated clients with a CAPTCHA page ("Request Access"),
even though the JSON API itself works. govinfo serves the same document as
plain text inside one `<pre>`, which is also byte-identical to the `content.html`
regulations.gov attaches to the document. The fetch refuses to follow a
redirect off `www.govinfo.gov`, caps the download at `RULE_TEXT_MAX_BYTES`, and
keeps the last 16 documents in an `lru_cache`, so paging through one rule
fetches it once per instance.

Structure is read from the text's own conventions. A heading is a line, after
a blank line, that starts `PART <n>--` or `Sec.  <n.n>  <title>`; the blank-line
rule keeps inline references such as "in accordance with Sec.  60.2." from
being taken as headings. `[[Page n]]` markers are dropped. One section ends at
the next heading, the `[FR Doc.` trailer, or a numbered amendatory instruction
("3. Amend Sec. 751.307 by…"), because final rules interleave those between
the sections they change; a whole part keeps its instructions. Text is paged
with `offset` / `next_offset` so a 300-page rule never lands in one response.

`comments_close_on_in_notice` is the deadline printed in the notice and goes
stale when an agency extends the period; `prepare_comment` reads the current
deadline from `documents`.

## `prepare_comment` and why it never submits

`prepare_comment(docket_id, draft)` looks up the docket's documents whose
comment window contains "now", sorts any Proposed Rule first, and returns each
one's `https://www.regulations.gov/commenton/<document_id>` link. With a draft it
reports characters and UTF-8 bytes against the API's 5,000 limit (GSA states the
limit as "characters/bytes"; UTF-8 bytes are never fewer than characters, so the
byte count is the one checked).

It deliberately stops there. Regulations.gov does have a `POST /v4/comments`
API, but it needs a separate commenting key whose holder certifies that every
comment is from a real person, under a fixed 50/minute and 500/hour limit. On
this unauthenticated public server that key would be shared by every caller:
one person scripting comments could get it revoked for everyone, and Spicy Regs
would be certifying text it never saw a person write. So the person submits on
regulations.gov themselves. A bring-your-own-key submit tool, if it is ever
added, belongs in the local stdio server only, never in `build_app`.

The docket ID is checked against `DOCKET_ID_PATTERN` and both lookups are
parameterized, so this tool takes no SQL from the caller. Withdrawn documents
and windows that have not opened yet are skipped. Dates come from the nightly
mirror, so the response tells the caller to confirm the deadline on the linked
page; an extension published today will not show until tomorrow.

## Other invariants

- `ICONS` uses a base64 `data:` URI, not an `https://` URL, so it works on both
  the stdio and HTTP transports. Generated by `scripts/gen_icon.py`.
- `_resolve_catalog_config` and `_resolve_r2_base_url` reject quotes,
  backslashes, and control characters outright rather than escaping them, because
  the values are inlined into `CREATE SECRET`/`ATTACH`/`read_parquet`, which take
  no bind parameters.
- `R2_CATALOG_NAMESPACE` uses `or DEFAULT` rather than `get`'s default argument so
  an env var set to the empty string falls back to `default` instead of `""`.

## Operation diagnostics and freshness

All actions emit JSON `mcp_operation` log events with stage, operation, outcome,
elapsed milliseconds, and exception class. Connection setup and GovInfo HTTP
fetches emit separate stage events. Inputs, SQL, drafts, exception messages, and
response content are intentionally omitted. These logs describe server work;
a client disconnect or platform kill may prevent a completion event entirely.

`get_data_freshness(table)` checks one allowlisted table independently, exposing
artifact Last-Modified/ETag and observed date watermarks for federal_register,
documents, and dockets. HTTP or SQL failure leaves partial results usable.
Rollup uploads now attach `ingested-at` object metadata after the build completes,
before upload: it dates preparation of the published snapshot, not the newest
source record, and does not certify complete source ingestion. Older artifacts,
base-table publishers, or CDNs that omit custom metadata report null. The public
HTTP endpoint must forward `x-amz-meta-ingested-at` for it to be visible.
`coverage_complete_through` is deliberately null: a maximum publication/update
date alone cannot prove complete coverage. Future-dated records can also move a
maximum forward. No freshness or ingestion date is inferred from Last-Modified.
