# MCP server on Google Cloud Run (primary)

Cloud Run is the primary host for the MCP server. It won on the two things every
other option tripped on:

- **Autoscaling that actually scales.** Load-tested to **c=100 at p50 ~2s, ~0%
  errors** (Cloudflare Containers thrashed at any real concurrency on a beta
  account limit; Vercel was memory-capped). Heavy `count(DISTINCT)` over 25.7M
  rows returns in ~7s.
- **A real warm floor.** `--min-instances=N` keeps N instances warm — set it 0
  normally (scale to zero, no idle cost) and bump it the morning of an event.

Same image as `deploy/cloudflare/` — the Dockerfile is platform-agnostic, so this
just points `gcloud` at it. 16 GiB RAM (`SPICY_REGS_MEMORY_LIMIT=12GB` leaves
headroom); `/tmp` spill is RAM-backed on Cloud Run, so RAM is the real ceiling.

## Deploy

```bash
gcloud auth login
PROJECT=spicy-regs-mcp ./deploy/cloudrun/deploy.sh
```

The script enables APIs, ensures the Artifact Registry repo, builds+pushes the
image (amd64), and deploys. It prints the service URL.

## Continuous deploy

`.github/workflows/deploy-mcp.yml` runs this same script on every push to
`main` that touches the server (`mcp_server.py`, `_icon.py`, `pyproject.toml`,
the Dockerfile, or this directory), after `tests/test_mcp_server.py` passes. It
can also be run by hand from the Actions tab. A failed smoke check fails the
job; the new revision is still serving, so roll back with the
`update-traffic` command the script prints.

Auth is keyless via Workload Identity Federation. One-time setup (already done
for `spicy-regs-mcp`; repeat only for a new project):

```bash
PROJECT=spicy-regs-mcp
PROJECT_NUMBER=$(gcloud projects describe $PROJECT --format='value(projectNumber)')
SA=spicy-regs-deployer@$PROJECT.iam.gserviceaccount.com

gcloud iam service-accounts create spicy-regs-deployer --project $PROJECT
for role in roles/run.admin roles/artifactregistry.writer roles/iam.serviceAccountUser; do
  gcloud projects add-iam-policy-binding $PROJECT --member "serviceAccount:$SA" --role $role
done

gcloud iam workload-identity-pools create github --project $PROJECT --location global
gcloud iam workload-identity-pools providers create-oidc github --project $PROJECT \
  --location global --workload-identity-pool github \
  --issuer-uri https://token.actions.githubusercontent.com \
  --attribute-mapping 'google.subject=assertion.sub,attribute.repository=assertion.repository' \
  --attribute-condition "assertion.repository == 'civictechdc/spicy-regs'"
gcloud iam service-accounts add-iam-policy-binding $SA --project $PROJECT \
  --role roles/iam.workloadIdentityUser \
  --member "principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/github/attribute.repository/civictechdc/spicy-regs"

gh variable set GCP_WORKLOAD_IDENTITY_PROVIDER \
  --body "projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/github/providers/github"
gh variable set GCP_DEPLOY_SERVICE_ACCOUNT --body "$SA"
```

## Two one-time hurdles we hit (documented so you don't rediscover them)

1. **Billing quota.** Linking a *new* project to the Primary billing account
   failed with `Cloud billing quota exceeded` (cap on projects per billing
   account). Fix: bill `spicy-regs-mcp` to the **secondary** billing account, or
   free a slot / use an existing billing-enabled project.
2. **Org policy blocks public access.** The kvec.ai org enforces
   `iam.allowedPolicyMemberDomains` (Domain Restricted Sharing), so `allUsers`
   can't be granted `run.invoker` and the service 403s. Fix (needs org-policy
   admin, via Console): IAM & Admin → Organization Policies → *Domain restricted
   sharing* → Manage policy → Override parent's policy → **Allow All** → Save.
   Scoped to this project only. Then the deploy's `allUsers` binding succeeds
   (allow ~1 min for propagation).

## Event day

```bash
gcloud run services update spicy-regs-mcp --region us-east1 --min-instances 5   # warm floor
# ... after the event ...
gcloud run services update spicy-regs-mcp --region us-east1 --min-instances 0   # back to scale-to-zero
```

## Status — this IS the canonical endpoint

- **Custom domain** — `mcp.spicy-regs.dev` maps to the service (Cloud Run domain
  mapping; DNS in Cloudflare, managed cert). Done.
- **Iceberg catalog** — `R2_CATALOG_TOKEN` in Secret Manager + the `R2_CATALOG_*`
  vars, so `comments` reads the deduped system-of-record (verified: `count(*)` ==
  `count(DISTINCT comment_id)`). Done.
- **Vercel** — retired: the deploy workflow and the `mcp-server/api/index.py`
  copy are gone; `spicy_regs.mcp_server` is the single source.

> Parquet is served `no-cache` (edge caching corrupts DuckDB range reads — see the
> r2.py policy). The Cloudflare cache rule for the corpus is disabled; only
> non-parquet artifacts are cache-eligible.
