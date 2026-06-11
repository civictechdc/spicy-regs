# Spicy Regs MCP Server

A remote [Model Context Protocol](https://modelcontextprotocol.io) server that
exposes the Spicy Regs regulatory dataset (regulations.gov mirror) to any
MCP-compatible client — Claude.ai, Claude Code, Cursor, etc. — without
requiring a local Python install.

Under the hood it runs DuckDB queries against the public Cloudflare R2 parquet
bucket (`pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev`). Two transports are
shipped:

- **Streamable HTTP** — this directory, deployed as a Vercel Python
  serverless function. Use for claude.ai, web clients, anything remote.
- **Stdio (`uvx`)** — via the `spicy-regs-mcp` console script declared in
  the repo's root `pyproject.toml`. Use for Claude Code, Cursor, Continue,
  any client that can spawn a local process. No deploy needed.

The canonical FastMCP implementation lives in `src/spicy_regs/mcp_server.py`
and powers the stdio entry point. This Vercel function keeps its own copy of
the tool surface so it can deploy without pulling in the parent package's
ETL dependencies (boto3, polars, prefect, ...) — keep the two in sync.

## Tools exposed

| Tool | Purpose |
| --- | --- |
| `list_sources` | List logical tables in the R2 dataset |
| `describe_table(table)` | Get the column schema for one table |
| `query_sql(sql, max_rows=25)` | Run a SQL query against the R2 views |

Available views: `dockets`, `documents`, `comments`, `comments_index`,
`feed_summary`.

## Deploy to Vercel

The production deployment lives at **`https://mcp.spicy-regs.dev/mcp`** —
that's the MCP endpoint you give to clients.

### One-time project setup (dashboard)

1. In the Vercel dashboard, **Add New → Project** and import this repo
   (`civictechdc/spicy-regs`).
2. Name the project (e.g. `spicy-regs-mcp`) and set **Root Directory** to
   `mcp-server`. Leave Framework Preset as **Other** — `vercel.json` and the
   `api/` directory drive the build.
3. Deploy. Pushes to `main` now redeploy production automatically; PRs get
   preview deployments.
4. Under **Project → Settings → Domains**, add `mcp.spicy-regs.dev`. If the
   `spicy-regs.dev` DNS isn't managed by Vercel, add the CNAME record the
   dashboard shows you (the same setup already used for `app.spicy-regs.dev`).

> Note: this must be a **separate Vercel project** from the `spicy-regs-ui`
> frontend project that serves `app.spicy-regs.dev`.

### Manual deploys (optional)

From this directory, with the Vercel CLI authenticated:

```bash
cd mcp-server
npx vercel --prod
```

### Environment variables (optional)

| Variable | Default |
| --- | --- |
| `SPICY_REGS_R2_URL` | `https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev` |

Override `SPICY_REGS_R2_URL` if you fork the bucket.

## Install in Claude.ai (web/desktop)

Requires Pro, Max, Team, or Enterprise.

1. Open **Settings → Connectors → Add custom connector**.
2. Name it `Spicy Regs`.
3. URL: `https://mcp.spicy-regs.dev/mcp`.
4. Leave authentication as None (the bucket is public).
5. Save and toggle the connector on in any conversation.

## Install in Claude Code

Two transports work. Pick one.

**Remote HTTP:**

```bash
claude mcp add --transport http spicy-regs https://mcp.spicy-regs.dev/mcp
```

**Local stdio via `uvx` (no deploy needed):**

```bash
claude mcp add spicy-regs -- uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs-mcp
```

The `spicy-regs-mcp` console script is declared in the repo's root
`pyproject.toml` and runs `spicy_regs.mcp_server:main`, which serves the same
three tools over stdio. Once the package is published to PyPI you can drop the
`--from` flag: `uvx spicy-regs-mcp`.

Either way, the `list_sources`, `describe_table`, and `query_sql` tools become
available in any session. Pairs well with the `spicyregs` skill from this
repo, which adds workflow guidance on top of the raw tools.

## Local development

```bash
cd mcp-server
uv venv && source .venv/bin/activate
uv pip install -r requirements.txt
uv pip install uvicorn
uvicorn api.index:app --reload --port 8000
```

Then point a client at `http://localhost:8000/mcp`.

## Limitations

- Vercel serverless functions cap at 60s. The first query in a cold container
  pays a few seconds to install the DuckDB `httpfs` extension and fetch parquet
  metadata; subsequent queries within the same container are fast.
- The `find_duplicate_regulations.py` helper isn't exposed over MCP — its
  pairwise scan can exceed the function timeout. For that workload, use the
  local script via `uv run --script` (see the skill's SKILL.md).
- `query_sql` runs against an in-memory DuckDB connection. `CREATE TABLE`,
  `INSERT`, etc. don't persist anywhere. The R2 views are read-only.
