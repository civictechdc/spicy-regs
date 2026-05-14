# Spicy Regs MCP Server

A remote [Model Context Protocol](https://modelcontextprotocol.io) server that
exposes the Spicy Regs regulatory dataset (regulations.gov mirror) to any
MCP-compatible client — Claude.ai, Claude Code, Cursor, etc. — without
requiring a local Python install.

Under the hood it runs DuckDB queries against the public Cloudflare R2 parquet
bucket (`pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev`) and is deployed as a
Vercel Python serverless function.

## Tools exposed

| Tool | Purpose |
| --- | --- |
| `list_sources` | List logical tables in the R2 dataset |
| `describe_table(table)` | Get the column schema for one table |
| `query_sql(sql, max_rows=25)` | Run a SQL query against the R2 views |

Available views: `dockets`, `documents`, `comments`, `comments_index`,
`feed_summary`.

## Deploy to Vercel

From this directory:

```bash
cd mcp-server
npx vercel --prod
```

Or set the Vercel project's **Root Directory** to `mcp-server/` in the
dashboard and let Git pushes deploy automatically.

The deployed URL will look like `https://<project>.vercel.app/mcp` — that's
the MCP endpoint you give to clients.

### Environment variables (optional)

| Variable | Default |
| --- | --- |
| `SPICY_REGS_R2_URL` | `https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev` |

Override `SPICY_REGS_R2_URL` if you fork the bucket.

## Install in Claude.ai (web/desktop)

Requires Pro, Max, Team, or Enterprise.

1. Open **Settings → Connectors → Add custom connector**.
2. Name it `Spicy Regs`.
3. URL: `https://<your-vercel-deploy>.vercel.app/mcp`.
4. Leave authentication as None (the bucket is public).
5. Save and toggle the connector on in any conversation.

## Install in Claude Code

```bash
claude mcp add --transport http spicy-regs https://<your-vercel-deploy>.vercel.app/mcp
```

Then in any session, the `list_sources`, `describe_table`, and `query_sql`
tools will be available. Pairs well with the `spicyregs` skill from this
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
