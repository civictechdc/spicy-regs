# Deploy

Infrastructure and deployment for the parts of Spicy Regs that don't live on
GitHub Actions (the ETL) — i.e. the public data corpus and the MCP server.

| Dir | What | Tool | Touches |
|---|---|---|---|
| [`terraform/`](terraform/) | R2 bucket, public custom domain, CORS, Iceberg data catalog, and the edge **cache rule** | Terraform (Cloudflare provider) | Cloudflare account + `spicy-regs.dev` zone |
| [`cloudflare/`](cloudflare/) | The **MCP server** as a Cloudflare Container (Docker image + Worker front) | `wrangler` + Docker | Cloudflare Workers/Containers |

## Why here, why now

Two things drove this:

- **The R2 setup had no IaC.** The bucket, its `data.spicy-regs.dev` domain, the
  catalog, and (until recently) the cache rule were all hand-created in the
  dashboard — unreconstructable if the account holder was unavailable.
  `terraform/` closes that gap. It is **import-first**: those resources already
  exist, so Terraform *adopts* them (a clean `plan` proves the code matches prod)
  rather than recreating them. See `terraform/README.md`.

- **The MCP server needed a home that supports heavy analytics.** Vercel caps
  function memory and hard-limits requests; Cloudflare Containers give a real
  20 GB disk (so DuckDB can spill big aggregations) next to the R2 bucket, on the
  vendor that already hosts the data. See `cloudflare/README.md`.

Both are the same Docker image running the canonical `spicy_regs.mcp_server`, and
both rely on the platform-agnostic env knobs added alongside this
(`SPICY_REGS_MEMORY_LIMIT`, `SPICY_REGS_TEMP_DIR`).

## Order of operations for a fresh environment

1. `terraform/` — stand up (or import) the bucket, domain, CORS, catalog, cache rule.
2. Populate the corpus (the ETL / rollups on GitHub Actions).
3. `cloudflare/` — build + deploy the MCP container.

## Update 2026-08-18: Cloud Run is the primary MCP host

After load-testing, the MCP server runs on **Google Cloud Run** (`cloudrun/`) —
it autoscales to 100 concurrent at ~2s p50 with ~0% errors and supports a warm
floor (`--min-instances`) for events. The `cloudflare/` container target is kept
as a documented, working single-instance fallback (it thrashes multi-instance on
the current beta account limit). Terraform (`terraform/`) still manages the R2
bucket + cache rule regardless of where the MCP server runs.
