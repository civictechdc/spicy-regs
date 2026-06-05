# Spicy Regs

Spicy Regs goal is to build an open, contributor-friendly platform for exploring and analyzing regulations.gov data, usable by both technical and non-technical users. The platform should enable rapid prototyping, reproducible analysis, and modular app extensions.

## Open Example Notebooks under /notebooks

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/civictechdc/spicy-regs/HEAD)

## Use with Claude

- **Claude Code plugin:** `/plugin marketplace add civictechdc/spicy-regs` then `/plugin install spicyregs@spicy-regs-local`. Bundles the skill in this repo. See `plugins/spicyregs/skills/spicyregs/SKILL.md`.
- **Claude Code via uvx (stdio MCP):** `claude mcp add spicy-regs -- uvx --from "spicy-regs @ git+https://github.com/civictechdc/spicy-regs" spicy-regs-mcp`. No deploy needed.
- **Claude.ai or any remote MCP client:** deploy `mcp-server/` to Vercel and add the URL as a Custom Connector. See [`mcp-server/README.md`](mcp-server/README.md).

## Contributing

The ETL is built from small, composable building blocks (`Reader → Transform →
Writer`, wired by a `Pipeline`). See the [Architecture section in
CONTRIBUTING.md](CONTRIBUTING.md#architecture-the-etl-building-blocks) and the
runnable reference in `tests/test_example_pipeline.py` for how to add your own.

## Contact us

Join our [slack channel](https://civictechdc.slack.com/archives/C09H576E6LU)!

