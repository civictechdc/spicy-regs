# Spicy Regs

Spicy Regs goal is to build an open, contributor-friendly platform for exploring and analyzing regulations.gov data, usable by both technical and non-technical users. The platform should enable rapid prototyping, reproducible analysis, and modular app extensions.

## Open Example Notebooks under /notebooks

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/civictechdc/spicy-regs/HEAD)

## Local Development with Docker

1. Copy `.env.example` to `.env` and fill in your credentials
2. Start all services:

```bash
docker compose up
```

- **Frontend:** http://localhost:3000

Source files are volume-mounted for hot reload — edits to `frontend/src/` reflect immediately.

## Contact us

Join our [slack channel](https://civictechdc.slack.com/archives/C09H576E6LU)!

