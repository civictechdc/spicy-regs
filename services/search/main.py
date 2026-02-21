import os
import logging

import httpx
import lancedb
import pyarrow as pa
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Spicy Regs Search API")

allowed_origins = os.environ.get(
    "ALLOWED_ORIGINS", "https://app.spicy-regs.dev,http://localhost:3000"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_methods=["GET"],
    allow_headers=["*"],
    expose_headers=["Cross-Origin-Resource-Policy"],
)


@app.middleware("http")
async def add_corp_header(request, call_next):
    response = await call_next(request)
    response.headers["Cross-Origin-Resource-Policy"] = "cross-origin"
    return response


# ---------------------------------------------------------------------------
# LanceDB connection (singleton)
# ---------------------------------------------------------------------------

_db = None
_table = None

SELECT_COLUMNS = [
    "id", "title", "text", "comment",
    "docket_id", "agency_code", "posted_date",
]


def get_db():
    global _db
    if _db is not None:
        return _db

    uri = os.environ.get("LANCE_DB_URI") or (
        f"s3://{os.environ['R2_BUCKET_NAME']}/lance-data"
        if os.environ.get("R2_BUCKET_NAME")
        else None
    )
    if not uri:
        raise RuntimeError(
            "Set LANCE_DB_URI (local path or s3:// URI) or R2_BUCKET_NAME"
        )

    storage_options = {}
    if uri.startswith("s3://"):
        storage_options = {
            "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "aws_endpoint": os.environ["R2_ENDPOINT"],
            "aws_region": "auto",
        }

    _db = lancedb.connect(uri, storage_options=storage_options)
    return _db


def get_table():
    global _table
    if _table is not None:
        return _table
    _table = get_db().open_table("comments")
    return _table


# ---------------------------------------------------------------------------
# Embeddings via Cloudflare Workers AI
# ---------------------------------------------------------------------------

_http_client = None


def _get_http_client():
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=30.0)
    return _http_client


async def get_embedding(text: str) -> list[float]:
    account_id = os.environ["CLOUDFLARE_ACCOUNT_ID"]
    api_token = os.environ["CLOUDFLARE_API_TOKEN"]

    client = _get_http_client()
    resp = await client.post(
        f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/@cf/baai/bge-base-en-v1.5",
        headers={"Authorization": f"Bearer {api_token}"},
        json={"text": [text]},
    )
    resp.raise_for_status()
    data = resp.json()

    if not data.get("success") or not data.get("result", {}).get("data"):
        raise RuntimeError(f"Cloudflare AI error: {data.get('errors')}")

    return data["result"]["data"][0]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/search")
async def hybrid_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=50),
    agency: str | None = Query(None),
):
    try:
        query_vector = await get_embedding(q)
        table = get_table()

        results = (
            table.search(query_type="hybrid")
            .vector(query_vector)
            .text(q)
            .select(SELECT_COLUMNS)
            .limit(limit)
        )

        if agency:
            safe_agency = agency.replace("'", "")
            results = results.where(f"agency_code = '{safe_agency}'")

        rows = results.to_pandas()

        return {
            "query": q,
            "results": [
                {
                    "id": row.get("id"),
                    "title": row.get("title"),
                    "text": row.get("text"),
                    "comment": row.get("comment"),
                    "docket_id": row.get("docket_id"),
                    "agency_code": row.get("agency_code"),
                    "posted_date": row.get("posted_date"),
                    "score": float(row.get("_relevance_score", 0)),
                }
                for _, row in rows.iterrows()
            ],
        }
    except Exception as e:
        logger.exception("Hybrid search failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search/similar")
async def similar_search(
    id: str = Query(..., alias="id", min_length=1),
    limit: int = Query(10, ge=1, le=30),
):
    try:
        table = get_table()
        safe_id = id.replace("'", "")

        # Look up the source comment's vector
        source = table.search().where(f"id = '{safe_id}'").select(SELECT_COLUMNS + ["vector"]).limit(1).to_pandas()

        if source.empty:
            raise HTTPException(status_code=404, detail="Comment not found in vector index")

        source_vector = source.iloc[0]["vector"].tolist()

        # Find nearest neighbors
        rows = (
            table.search(source_vector)
            .select(SELECT_COLUMNS)
            .where(f"id != '{safe_id}'")
            .limit(limit)
            .to_pandas()
        )

        return {
            "source_id": id,
            "results": [
                {
                    "id": row.get("id"),
                    "title": row.get("title"),
                    "text": row.get("text"),
                    "comment": row.get("comment"),
                    "docket_id": row.get("docket_id"),
                    "agency_code": row.get("agency_code"),
                    "posted_date": row.get("posted_date"),
                    "score": float(row.get("_distance", 0)),
                    "rank": idx + 1,
                }
                for idx, (_, row) in enumerate(rows.iterrows())
            ],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Similar search failed")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


@app.on_event("startup")
async def warmup():
    try:
        get_table()
        logger.info("LanceDB connection ready")
    except Exception as e:
        logger.error(f"LanceDB warmup failed: {e}")
