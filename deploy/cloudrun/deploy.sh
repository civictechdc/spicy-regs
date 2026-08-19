#!/usr/bin/env bash
# Deploy the Spicy Regs MCP server to Google Cloud Run.
#
# Cloud Run is the primary host: it autoscales cleanly to 100+ concurrent (load-
# tested: c=100 at p50 ~2s, ~0% errors), supports a warm floor via --min-instances
# for event days, and gives 16 GiB RAM for heavy analytics. It reuses the SAME
# image as the Cloudflare target (deploy/cloudflare/Dockerfile) — the Dockerfile
# is platform-agnostic (canonical spicy_regs.mcp_server under uvicorn on $PORT).
#
# Prereqs (one-time, interactive / console):
#   - gcloud auth login
#   - a billing-enabled project (this repo used `spicy-regs-mcp`, billed to the
#     secondary account after the Primary hit its project-link quota)
#   - the org policy `iam.allowedPolicyMemberDomains` relaxed for the project
#     (Allow All) so `allUsers` can be granted run.invoker — the kvec.ai org
#     restricts public access by default. See README.
#   - local Docker (for the amd64 build/push)
#
# Usage:
#   PROJECT=spicy-regs-mcp ./deploy/cloudrun/deploy.sh
set -euo pipefail

: "${PROJECT:?set PROJECT to your GCP project id}"
REGION="${REGION:-us-east1}"
SERVICE="${SERVICE:-spicy-regs-mcp}"
AR_REPO="${AR_REPO:-spicy-regs}"
MEMORY="${MEMORY:-16Gi}"
CPU="${CPU:-4}"
CONCURRENCY="${CONCURRENCY:-8}"
TIMEOUT="${TIMEOUT:-600}"
MIN_INSTANCES="${MIN_INSTANCES:-0}"   # bump to N the morning of an event for a warm floor
MAX_INSTANCES="${MAX_INSTANCES:-10}"
MEM_LIMIT="${SPICY_REGS_MEMORY_LIMIT:-12GB}"   # under MEMORY so heavy queries spill to /tmp, not OOM-kill

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${AR_REPO}/mcp:$(git -C "$ROOT" rev-parse --short HEAD)"

echo "1/5 Enable APIs"
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com --project "$PROJECT"

echo "2/5 Ensure Artifact Registry repo"
gcloud artifacts repositories describe "$AR_REPO" --location "$REGION" --project "$PROJECT" >/dev/null 2>&1 || \
  gcloud artifacts repositories create "$AR_REPO" --repository-format=docker --location "$REGION" --project "$PROJECT"

echo "3/5 Build + push (amd64 — Cloud Run requires it; reuses the shared Dockerfile)"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
docker build --platform linux/amd64 -f "$ROOT/deploy/cloudflare/Dockerfile" -t "$IMAGE" "$ROOT"
docker push "$IMAGE"

echo "4/5 Deploy"
gcloud run deploy "$SERVICE" \
  --image "$IMAGE" --project "$PROJECT" --region "$REGION" \
  --allow-unauthenticated --port 8080 \
  --memory "$MEMORY" --cpu "$CPU" \
  --concurrency "$CONCURRENCY" --timeout "$TIMEOUT" \
  --min-instances "$MIN_INSTANCES" --max-instances "$MAX_INSTANCES" \
  --set-env-vars "SPICY_REGS_MEMORY_LIMIT=${MEM_LIMIT},SPICY_REGS_TEMP_DIR=/tmp,SPICY_REGS_HOME_DIR=/tmp,SPICY_REGS_STATEMENT_TIMEOUT=${TIMEOUT}s,SPICY_REGS_R2_URL=https://data.spicy-regs.dev"

# --allow-unauthenticated needs the allUsers binding, which the org policy blocks
# until relaxed. If the deploy warns the IAM binding failed, relax the policy
# (README) then: gcloud run services add-iam-policy-binding "$SERVICE" \
#   --region "$REGION" --member=allUsers --role=roles/run.invoker

echo "5/5 URL:"
gcloud run services describe "$SERVICE" --project "$PROJECT" --region "$REGION" --format='value(status.url)'
