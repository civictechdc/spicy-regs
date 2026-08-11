# MCP server internals

> Engineering notes for contributors. Not user-facing — for how to *use* the
> server see [`README.md`](README.md), and for table schemas see
> <https://docs.spicy-regs.dev>.

This is the rationale behind the MCP server implementation. Both source files are
comment-free by project policy (documentation lives in markdown), so this
document is the explanation for what the code does and why.

It covers **both copies**, which are hand-mirrored and must stay in sync:

| File | Role |
|---|---|
| `src/spicy_regs/mcp_server.py` | Canonical. Stdio via the `spicy-regs-mcp` console script, plus `build_app()` for ASGI. |
| `mcp-server/api/index.py` | Vercel function. A parallel copy so the deploy avoids the parent package's ETL dependencies (boto3, polars). |

`tests/test_mcp_server.py::test_vercel_copy_in_sync` enforces that the tool
surface, `TABLES`, `INSTRUCTIONS`, `DEFAULT_R2_BASE_URL`, `CATALOG_ALIAS`, and
`DEFAULT_CATALOG_NAMESPACE` match. It does **not** check dependency drift — the
Vercel copy has no lockfile, which is how an unpinned `mcp>=1.10.0` resolving to
2.0 took production down in July 2026. Pins in `mcp-server/requirements.txt`
carry upper bounds for that reason.

Everything below applies to both copies unless stated.

## What must never be deleted

**The three `@mcp.tool()` docstrings are not documentation — do not delete them.**
FastMCP reflects over `fn.__doc__` to build the tool descriptions sent to every
client during `list_tools`. `describe_table`'s docstring is how a client learns
which tables are valid; `query_sql`'s is how it learns the available views.
Strip them and the server still runs, but every client goes blind. Verify with
`asyncio.run(build_server().list_tools())`.

**Inline `# type: ignore` / `# noqa` are directives, not comments.** `ty` and
`ruff` gate merges and both read them. `_connect` needs `# type: ignore[index]`
on `catalog["namespace"]`; the Vercel copy needs `# noqa: E402` on its `_icon`
import, which must follow the `sys.path` insert.

## Connection setup (`_connect`, `_apply_security_settings`)

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
- `_apply_security_settings` is deliberately separate from `_connect` so the
  sandbox is testable without network. Run it after httpfs loads and before any
  user SQL; `SET lock_configuration=true` is last because it freezes everything.

## The statement timeout

**DuckDB has no `statement_timeout` parameter.** `SET statement_timeout=...`
raises `Catalog Error: unrecognized configuration parameter` — this was a shipped
runtime crash once, and `tests/test_mcp_server.py` guards the regression. The cap
is a watchdog (`_statement_timeout`) that calls `con.interrupt()` and converts
DuckDB's `InterruptException` into a `TimeoutError` so the cause is unambiguous.

`SPICY_REGS_STATEMENT_TIMEOUT` defaults to `790s`, kept just under the Vercel
`maxDuration` of **800s** so a runaway query returns a clean `TimeoutError`
instead of an opaque platform-killed 500. **Raise the two together or not at
all.** 800s is the generally-available Pro ceiling; 300s is only the all-plans
default. The 1800s extended tier is beta and a poor fit — a blocking DuckDB query
streams nothing, so Vercel's warning about intermediaries closing idle HTTP/1.1
connections applies squarely. In practice the binding limit is usually the *MCP
client*, which typically gives up at 60–120s. The canonical stdio copy has no
platform limit at all; its default matches purely to keep the mirrors identical.

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

Deliberate. The deployment is reached via `mcp.spicy-regs.dev` and per-deploy
`*.vercel.app` hosts; FastMCP's default localhost-only allowlist would reject all
of them with **421**. The server is public, stateless, and read-only, so
rebinding protection buys nothing.

## Other invariants

- `ICONS` uses a base64 `data:` URI, not an `https://` URL, so it works on both
  the stdio and HTTP transports. Generated by `scripts/gen_icon.py`.
- `_resolve_catalog_config` and `_resolve_r2_base_url` reject quotes,
  backslashes, and control characters outright rather than escaping them, because
  the values are inlined into `CREATE SECRET`/`ATTACH`/`read_parquet`, which take
  no bind parameters.
- `R2_CATALOG_NAMESPACE` uses `or DEFAULT` rather than `get`'s default argument so
  an env var set to the empty string falls back to `default` instead of `""`.
- The Vercel `app()` wrapper normalizes the ASGI path: `vercel.json` rewrites
  `/mcp` onto `/api/index`, and depending on how the rewrite is forwarded the path
  arrives as either one, so both are mapped onto the transport's `/mcp` mount.
