# MCP server on Cloud Run

An alternative host for the read-only MCP server, chosen to support **heavy
analytics** — Cloud Run gives container-grade RAM (up to ~32 GB) and long
request timeouts, where Vercel caps memory and hard-limits requests at 800 s.

This runs the **canonical** `spicy_regs.mcp_server:build_app` directly under
uvicorn (see `Dockerfile`). Importing that module pulls only `duckdb` + `mcp`,
never the ETL deps, so the image is small and there's no hand-mirrored copy to
keep in sync — that's a structural win over the Vercel function.

## Why memory, not "spill", is the real lever here

The whole point of moving was to let big aggregations *complete*. The instinct
was disk spill — but **Cloud Run's `/tmp` is RAM-backed (tmpfs)**, so spilling
there consumes the same memory budget it's meant to relieve. The genuine
headroom is the instance's RAM. So the deploy:

- allocates generous RAM (`--memory 16Gi` default) — the actual heavy-analytics enabler;
- sets `SPICY_REGS_MEMORY_LIMIT=12GB` so DuckDB caps itself *below* the instance
  and degrades gracefully instead of being OOM-killed by the platform (containers
  otherwise detect host RAM, not the cgroup limit);
- points `SPICY_REGS_TEMP_DIR=/tmp` as a spill *safety valve*, understanding it's
  RAM-backed — it buys a little runway, not unbounded disk.

If you later need working sets bigger than one instance's RAM, mount a real
volume (Cloud Run 2nd-gen supports GCS FUSE / NFS) and point `SPICY_REGS_TEMP_DIR`
at it — that's true disk-backed spill. Not needed for the current corpus.

These knobs are env-gated in `_apply_security_settings`: **unset, behavior is
byte-identical to Vercel today** (no memory cap, spilling disabled). Only this
deploy opts in.

## Deploy

One-time, interactive (the agent can't do this — it needs a browser):

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT   # NOT playground-* for anything real
```

Then, from the repo root:

```bash
PROJECT=your-gcp-project ENV_FILE=/absolute/path/to/spicy-regs/.env \
  ./deploy/cloudrun/deploy.sh
```

The script enables the needed APIs, creates an Artifact Registry repo, builds and
pushes the image, deploys, and prints the URL + a smoke-test command. Tunables
(RAM, CPU, concurrency, timeout, min/max instances) are documented at the top of
`deploy.sh`.

## During the hackathon

- Set `MIN_INSTANCES=1` so the first user doesn't eat a cold start (a cold start
  rebuilds the DuckDB connection — ~35 s — even with the in-process cache).
- Consider bumping `CONCURRENCY` / `MAX_INSTANCES` after re-running the load test.

## Cutover (do NOT skip the measure step)

The current production endpoint `mcp.spicy-regs.dev` points at Vercel. Bring
Cloud Run up **alongside** it, then:

1. Deploy; smoke-test the printed `*.run.app` URL.
2. Re-run the load test (`scratchpad/loadtest.py`) against the `.run.app` URL and
   a heavy-analytics query that OOMs on Vercel today — confirm it completes.
3. Only then repoint `mcp.spicy-regs.dev` (Cloudflare CNAME → the run.app host,
   or a domain mapping) and update `deploy-mcp.yml` / retire it.
4. Once Cloud Run is the sole host, the Vercel copy (`mcp-server/api/index.py`)
   and its sync test can be deleted — a follow-up, not this PR.

## Secrets

This first cut passes `R2_CATALOG_TOKEN` via `--set-env-vars` (visible in the
service config). Before it's more than a prototype, move it to Secret Manager and
switch the deploy to `--set-secrets`.
