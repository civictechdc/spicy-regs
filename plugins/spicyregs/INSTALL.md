# Installing the Spicy Regs Skill

The `spicyregs` skill lives at `plugins/spicyregs/skills/spicyregs/` and tells
an AI assistant how to answer questions from the Spicy Regs Cloudflare R2
parquet dataset using DuckDB. It ships alongside an MCP server (`mcp-server/`,
also exposed locally via the `spicy-regs-mcp` console script) so providers
that can't load the skill directly can still call the same tools.

Pick the section for the assistant you're using.

## Claude Code (CLI)

The skill ships as a Claude Code plugin in this repo. Two install paths:

**Plugin marketplace (recommended — bundles the skill):**

```bash
/plugin marketplace add civictechdc/spicy-regs
/plugin install spicyregs@spicy-regs-local
```

This loads `SKILL.md`, the helper scripts under `scripts/`, and the reference
notes under `references/` into every Claude Code session.

**MCP only (skip the skill, just the tools):**

```bash
# Local stdio — no deploy needed:
claude mcp add spicy-regs -- uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs-mcp

# Or remote HTTP (after deploying mcp-server/ to Vercel):
claude mcp add --transport http spicy-regs https://<your-vercel-deploy>.vercel.app/mcp
```

You get `list_sources`, `describe_table`, and `query_sql` either way. The
plugin install above pairs the skill's workflow guidance on top of these
tools.

## Claude.ai (web / desktop apps)

Requires Pro, Max, Team, or Enterprise (custom connectors aren't available on
free plans).

1. Deploy `mcp-server/` to Vercel (see [`mcp-server/README.md`](../../mcp-server/README.md)).
2. Open **Settings → Connectors → Add custom connector**.
3. Name: `Spicy Regs`. URL: `https://<your-vercel-deploy>.vercel.app/mcp`.
   Authentication: None.
4. Toggle the connector on in any conversation.

Claude.ai doesn't load repo skills directly, so paste the system prompt from
[`provider-agnostic-prompt.md`](skills/spicyregs/references/provider-agnostic-prompt.md)
into a project's custom instructions if you want the same workflow guardrails
the skill provides locally.

## OpenAI Codex / ChatGPT

The plugin includes a Codex-compatible manifest at
`plugins/spicyregs/.codex-plugin/plugin.json` and a UI metadata file at
`plugins/spicyregs/skills/spicyregs/agents/openai.yaml`. On Codex-style
surfaces that load plugin directories, point the loader at
`plugins/spicyregs/` and the skill is picked up automatically.

For ChatGPT proper (no plugin loader):

- If your workspace supports custom MCP connectors, point it at the deployed
  `mcp-server/` URL the same way Claude.ai does.
- Otherwise, paste the system prompt from
  [`provider-agnostic-prompt.md`](skills/spicyregs/references/provider-agnostic-prompt.md)
  into a Custom GPT or system message and run the helper scripts yourself,
  feeding output back in.

## Cursor, Continue, Cline, and other MCP-capable IDE clients

Any client that speaks MCP can use the server. Pick a transport:

**Local stdio (no deploy):**

```json
{
  "mcpServers": {
    "spicy-regs": {
      "command": "uvx",
      "args": [
        "--from",
        "spicy-regs @ git+https://github.com/civictechdc/spicy-regs",
        "spicy-regs-mcp"
      ]
    }
  }
}
```

**Remote HTTP (after deploying `mcp-server/` to Vercel):**

```json
{
  "mcpServers": {
    "spicy-regs": {
      "url": "https://<your-vercel-deploy>.vercel.app/mcp"
    }
  }
}
```

Drop that into the client's MCP config (`~/.cursor/mcp.json`,
`~/.continue/config.json`, etc. — check the client's docs for the exact
location and key name).

## Any other LLM (provider-agnostic fallback)

If your assistant can run shell commands but can't load skills or MCP, use
the provider-agnostic prompt directly:

1. Clone this repo and `uv sync`.
2. Copy the system prompt from
   [`provider-agnostic-prompt.md`](skills/spicyregs/references/provider-agnostic-prompt.md)
   into your assistant's system / instructions field.
3. Let it call the helper scripts:
   - `uv run --script plugins/spicyregs/skills/spicyregs/scripts/query_spicy_regs.py --source r2 --list-sources`
   - `uv run --script plugins/spicyregs/skills/spicyregs/scripts/query_spicy_regs.py --source r2 --describe <table>`
   - `uv run --script plugins/spicyregs/skills/spicyregs/scripts/query_spicy_regs.py --source r2 --sql "<SQL>"`

The workflow, behavior rules, and citation expectations match what
`SKILL.md` enforces inside Claude Code.

## Verifying the install

Ask the assistant something concrete and check that it cites real
identifiers from the dataset:

> "List the five most recent EPA dockets and their docket IDs."

A correctly installed skill / connector returns docket IDs, titles, and
modification dates from the R2 parquet — and says so explicitly if R2 was
unavailable rather than substituting another source.
