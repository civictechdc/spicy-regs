# Spicy Regs

Spicy Regs goal is to build an open, contributor-friendly platform for exploring and analyzing regulations.gov data, usable by both technical and non-technical users. The platform should enable rapid prototyping, reproducible analysis, and modular app extensions.

## Quickstart

Prerequisites: Python 3.10+ and [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
git clone https://github.com/civictechdc/spicy-regs.git
cd spicy-regs
uv sync                       # install dependencies into a .venv
uv run pytest                 # run the test suite
uv run ruff check .           # lint
```

You don't need any credentials to run the tests or explore the code. A `.env`
file (copy `.env.example` to `.env`) is only required if you want to talk to
live Cloudflare R2 storage.

Next steps:
- Open the runnable example pipeline at `tests/test_example_pipeline.py`.
- Read [CONTRIBUTING.md](CONTRIBUTING.md) for architecture and how to add your
  own reader / transform / writer / pipeline.

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

New contributors welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for the full
guide, including a glossary of terms and a map of where things live in the
repo.

## Contact us

Join our [slack channel](https://civictechdc.slack.com/archives/C09H576E6LU)!
Don't have access? Open a [GitHub issue](https://github.com/civictechdc/spicy-regs/issues/new/choose) and we'll get back to you.
