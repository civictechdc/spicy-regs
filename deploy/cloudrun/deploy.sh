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

echo "1/6 Enable APIs"
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com --project "$PROJECT"

echo "2/6 Ensure Artifact Registry repo"
gcloud artifacts repositories describe "$AR_REPO" --location "$REGION" --project "$PROJECT" >/dev/null 2>&1 || \
  gcloud artifacts repositories create "$AR_REPO" --repository-format=docker --location "$REGION" --project "$PROJECT"

echo "3/6 Build + push (amd64 — Cloud Run requires it; reuses the shared Dockerfile)"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
docker build --platform linux/amd64 -f "$ROOT/deploy/cloudflare/Dockerfile" -t "$IMAGE" "$ROOT"
docker push "$IMAGE"

echo "4/6 Deploy"
gcloud run deploy "$SERVICE" \
  --image "$IMAGE" --project "$PROJECT" --region "$REGION" \
  --allow-unauthenticated --port 8080 \
  --memory "$MEMORY" --cpu "$CPU" \
  --concurrency "$CONCURRENCY" --timeout "$TIMEOUT" \
  --min-instances "$MIN_INSTANCES" --max-instances "$MAX_INSTANCES" \
  --update-env-vars "SPICY_REGS_MEMORY_LIMIT=${MEM_LIMIT},SPICY_REGS_TEMP_DIR=/tmp,SPICY_REGS_HOME_DIR=/tmp,SPICY_REGS_STATEMENT_TIMEOUT=${TIMEOUT}s,SPICY_REGS_R2_URL=https://data.spicy-regs.dev"

# --update-env-vars, NOT --set-env-vars: the latter "removes all existing
# environment variables first", which would strip the R2_CATALOG_* config wired
# up after this script was written. Losing those is SILENT — _resolve_catalog_config
# just returns None and `comments` falls back to the public parquet mirror,
# giving up the deduped Iceberg system-of-record with no error anywhere.

# --allow-unauthenticated needs the allUsers binding, which the org policy blocks
# until relaxed. If the deploy warns the IAM binding failed, relax the policy
# (README) then: gcloud run services add-iam-policy-binding "$SERVICE" \
#   --region "$REGION" --member=allUsers --role=roles/run.invoker

echo "5/6 URL:"
URL="$(gcloud run services describe "$SERVICE" --project "$PROJECT" --region "$REGION" --format='value(status.url)')"
echo "$URL"

echo "6/6 Smoke check (against the public domain, so it covers DNS + cert + routing)"
BASE="${SMOKE_BASE:-https://mcp.spicy-regs.dev}"
fail=0

code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 30 "$BASE/")"
[ "$code" = 200 ] && echo "  ok   GET /            $code" || { echo "  FAIL GET /            $code"; fail=1; }

tools="$(curl -sS --max-time 60 -X POST "$BASE/mcp" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_sources","arguments":{}}}')"
for t in rulemaking_lifecycles fr_docket_links discovery_signals; do
  case "$tools" in
    *"$t"*) echo "  ok   table $t" ;;
    *)      echo "  FAIL table $t missing from list_sources"; fail=1 ;;
  esac
done

# The catalog is the silent-failure mode above; this proves comments is still
# reading the deduped system-of-record. ~7s over 25.7M rows.
dedup="$(curl -sS --max-time 120 -X POST "$BASE/mcp" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"query_sql","arguments":{"sql":"SELECT count(*) = count(DISTINCT comment_id) AS deduped FROM comments"}}}')"
case "$dedup" in
  *'"deduped": true'*|*'"deduped":true'*) echo "  ok   comments deduped (catalog attached)" ;;
  *) echo "  WARN comments not deduped — R2_CATALOG_* may be missing; check: gcloud run services describe $SERVICE --region $REGION --format='value(spec.template.spec.containers[0].env)'"; fail=1 ;;
esac

[ "$fail" = 0 ] && echo "Smoke check passed." || { echo "Smoke check FAILED — consider: gcloud run services update-traffic $SERVICE --region $REGION --to-revisions PREVIOUS=100"; exit 1; }
