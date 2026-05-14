# Spicy Regs

Spicy Regs goal is to build an open, contributor-friendly platform for exploring and analyzing regulations.gov data, usable by both technical and non-technical users. The platform should enable rapid prototyping, reproducible analysis, and modular app extensions.

## Open Example Notebooks under /notebooks

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/civictechdc/spicy-regs/HEAD)

## Use with Claude

- **Claude Code plugin:** `/plugin marketplace add civictechdc/spicy-regs` then `/plugin install spicyregs@spicy-regs-local`. Bundles the skill in this repo. See `plugins/spicyregs/skills/spicyregs/SKILL.md`.
- **Claude Code via uvx (stdio MCP):** `claude mcp add spicy-regs -- uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs-mcp`. No deploy needed.
- **Claude.ai or any remote MCP client:** deploy `mcp-server/` to Vercel and add the URL as a Custom Connector. See [`mcp-server/README.md`](mcp-server/README.md).

## Contact us

Join our [slack channel](https://civictechdc.slack.com/archives/C09H576E6LU)!

