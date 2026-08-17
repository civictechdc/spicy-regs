# MCP server on Cloudflare Containers

Runs the canonical `spicy_regs.mcp_server:build_app` as a Cloudflare Container,
fronted by a Worker — colocated with the R2 bucket, on the vendor that already
hosts the data, cache rule, and DNS.

> **Beta.** Cloudflare Containers is in beta and the `wrangler`/`@cloudflare/containers`
> surface has been moving. The config here follows the current docs but was
> **not** deployable from the authoring environment (no `wrangler`, no Workers
> token). Treat the first deploy as a shakeout — expect to adjust field names.

## Why Containers (vs Cloud Run / Vercel)

Heavy analytics wants memory **and** room to spill. Cloudflare's `standard-4`
instance is 4 vCPU / 12 GiB RAM / **20 GB real disk** — the disk is the point:
`SPICY_REGS_TEMP_DIR=/tmp` gives DuckDB genuine disk-backed spill, so a big
aggregation that exceeds RAM completes instead of OOMing. (Cloud Run's `/tmp` is
RAM-backed, so its spill isn't real headroom; Vercel caps memory outright.)

## Files

- `Dockerfile` — the image (canonical server under uvicorn on `$PORT`). Generic;
  the same image ran under Cloud Run in an earlier iteration.
- `wrangler.jsonc` — container + Durable Object binding + `standard-4` instance.
- `worker/index.ts` — forwards `/mcp` to the container; injects the DuckDB/R2 env.
- `package.json` — `wrangler` + `@cloudflare/containers`.

## Deploy

```bash
cd deploy/cloudflare
npm install
wrangler login                       # or a Workers-scoped CLOUDFLARE_API_TOKEN
wrangler secret put R2_CATALOG_TOKEN  # the Iceberg catalog token
# set the non-secret catalog vars (R2_CATALOG_URI/WAREHOUSE/NAMESPACE) as wrangler
# vars or in worker/index.ts, then:
npm run deploy
```

`wrangler` builds the Dockerfile, pushes the image, and rolls out the container.
It prints a `*.workers.dev` URL; smoke-test the MCP handshake against `/mcp`.

## Before cutover (don't skip)

`mcp.spicy-regs.dev` points at Vercel today. Bring this up alongside it, then:

1. Smoke-test the handshake on the `*.workers.dev` URL.
2. Re-run `scratchpad/loadtest.py` against it **plus** a query that OOMs on Vercel
   (e.g. `SELECT count(DISTINCT comment_id) FROM comments`) — confirm it completes.
   That completion is the whole justification for the move; verify it.
3. Only then repoint `mcp.spicy-regs.dev` and retire the Vercel deploy + the
   `mcp-server/api/index.py` copy (a follow-up).

## Tuning

`instance_type`, `max_instances` (concurrency/scale), `sleepAfter` (idle → scale
to zero), and `SPICY_REGS_MEMORY_LIMIT` (keep under the instance RAM so heavy
queries spill to disk rather than being killed) are the knobs. Re-measure after
changes.
