#!/usr/bin/env bash
# Deploy the Spicy Regs MCP server to Cloud Run.
#
# Prereqs (one-time): interactive `gcloud auth login`, a project with billing,
# and local Docker. Reads R2 / catalog config from a local .env — values are
# passed at deploy time and never committed.
#
# Usage:
#   PROJECT=my-gcp-project ENV_FILE=/abs/path/to/spicy-regs/.env \
#     ./deploy/cloudrun/deploy.sh
#
# Tunables (env, with defaults):
#   REGION=us-east1  SERVICE=spicy-regs-mcp  AR_REPO=spicy-regs
#   MEMORY=16Gi  CPU=4  CONCURRENCY=8  TIMEOUT=600
#   MIN_INSTANCES=0  MAX_INSTANCES=10  SPICY_REGS_MEMORY_LIMIT=12GB
set -euo pipefail

: "${PROJECT:?set PROJECT to your GCP project id}"
ENV_FILE="${ENV_FILE:-.env}"
REGION="${REGION:-us-east1}"
SERVICE="${SERVICE:-spicy-regs-mcp}"
AR_REPO="${AR_REPO:-spicy-regs}"
MEMORY="${MEMORY:-16Gi}"
CPU="${CPU:-4}"
CONCURRENCY="${CONCURRENCY:-8}"
TIMEOUT="${TIMEOUT:-600}"
MIN_INSTANCES="${MIN_INSTANCES:-0}"
MAX_INSTANCES="${MAX_INSTANCES:-10}"
MEM_LIMIT="${SPICY_REGS_MEMORY_LIMIT:-12GB}"

[ -f "$ENV_FILE" ] || { echo "ENV_FILE not found: $ENV_FILE" >&2; exit 1; }
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${AR_REPO}/${SERVICE}:$(date +%s)"

get() { grep -E "^$1=" "$ENV_FILE" | head -1 | cut -d= -f2- || true; }

echo "1/5 Enabling APIs (idempotent)..."
gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com --project "$PROJECT"

echo "2/5 Ensuring Artifact Registry repo '$AR_REPO' exists..."
gcloud artifacts repositories describe "$AR_REPO" --location "$REGION" --project "$PROJECT" >/dev/null 2>&1 || \
  gcloud artifacts repositories create "$AR_REPO" --repository-format=docker \
    --location "$REGION" --project "$PROJECT"

echo "3/5 Building + pushing image..."
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
docker build -f "$HERE/Dockerfile" -t "$IMAGE" "$ROOT"
docker push "$IMAGE"

# All env in ONE flag: repeated --set-env-vars REPLACE rather than accumulate in
# gcloud. `^@^` sets @ as the pair delimiter so values may contain commas.
ENV_VARS="^@^"
ENV_VARS+="SPICY_REGS_MEMORY_LIMIT=${MEM_LIMIT}"
ENV_VARS+="@SPICY_REGS_TEMP_DIR=/tmp"          # writable (RAM-backed) spill valve
ENV_VARS+="@SPICY_REGS_HOME_DIR=/tmp"          # DuckDB extension dir must be writable
ENV_VARS+="@SPICY_REGS_STATEMENT_TIMEOUT=${TIMEOUT}s"
ENV_VARS+="@SPICY_REGS_R2_URL=$(get R2_PUBLIC_URL)"
ENV_VARS+="@R2_CATALOG_URI=$(get R2_CATALOG_URI)"
ENV_VARS+="@R2_CATALOG_WAREHOUSE=$(get R2_CATALOG_WAREHOUSE)"
ENV_VARS+="@R2_CATALOG_TOKEN=$(get R2_CATALOG_TOKEN)"
ENV_VARS+="@R2_CATALOG_NAMESPACE=$(get R2_CATALOG_NAMESPACE)"

echo "4/5 Deploying to Cloud Run..."
gcloud run deploy "$SERVICE" \
  --project "$PROJECT" --region "$REGION" \
  --image "$IMAGE" \
  --allow-unauthenticated \
  --port 8080 \
  --memory "$MEMORY" --cpu "$CPU" \
  --concurrency "$CONCURRENCY" --timeout "$TIMEOUT" \
  --min-instances "$MIN_INSTANCES" --max-instances "$MAX_INSTANCES" \
  --set-env-vars "$ENV_VARS"

URL="$(gcloud run services describe "$SERVICE" --project "$PROJECT" --region "$REGION" --format='value(status.url)')"
echo "5/5 Deployed: $URL"
echo
echo "Smoke test (expect serverInfo in the response):"
echo "  curl -sS -X POST $URL/mcp -H 'Content-Type: application/json' \\"
echo "    -H 'Accept: application/json, text/event-stream' \\"
echo "    -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"clientInfo\":{\"name\":\"smoke\",\"version\":\"1\"}}}'"
