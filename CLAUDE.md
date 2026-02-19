# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Spicy Regs is a serverless civic tech platform for exploring federal regulations from regulations.gov (~31.6M comments, 2.1M documents, 393K dockets). All data queries run client-side in the browser — there is no backend API server.

## Commands

### Frontend (Next.js)
```bash
cd frontend
bun install
bun run dev      # Dev server at localhost:3000 (Turbopack)
bun run build    # Production build
bun run lint     # ESLint
```

### ETL Pipeline (Python)
```bash
cd etl
pip install -e .
python etl.py --output-dir ./output               # Full run
python etl.py --agency EPA --output-dir ./output  # Single agency
python etl.py --optimize --output-dir ./output    # With zstd + sorting
python etl.py --merge-only --output-dir ./output  # Merge staging only
```

### Cloudflare Worker (Iceberg proxy)
```bash
cd worker/iceberg-proxy
npm run dev      # Local dev
npm run deploy   # Deploy to Cloudflare Workers
```

## Architecture

### Data Flow

```
Mirrulations S3 (public)
    → ETL (Python/Polars/PyArrow)
    → Parquet files on Cloudflare R2 (public bucket)
    → DuckDB-WASM (runs in browser Web Worker)
    → React UI
```

All queries execute in the browser via DuckDB-WASM reading Parquet files over HTTP range requests. No API calls to backend services.

### Frontend Structure (`frontend/src/`)

- **`app/`** — Next.js App Router pages:
  - `/` landing, `/feed` docket feed, `/agencies` agency browser
  - `/analysis` dashboard, `/search` full-text search, `/bookmarks` saved items
  - `/sr/[code]/[id]` docket detail with documents + threaded comments
- **`lib/duckdb/context.tsx`** — Initializes DuckDB-WASM in a Web Worker; loads `httpfs` extension for R2 access; exports `DuckDBProvider`, `useDuckDB()`, and `R2_BASE_URL`
- **`lib/duckdb/useDuckDBService.ts`** — All query functions (`getRecentDocketsWithCounts`, `getCommentsForDocket`, `searchResources`, `getAgencyStats`, etc.)
- **`lib/db/models.ts`** — `RegulationsDataTypes` enum and `RegulationData` interface
- **`components/feed/`** — `DocketPost`, `DocketFeed`, `ThreadedComments`
- **`components/data-viewer/`** — Virtualized search results and generic data display

### Key Patterns

**DuckDB queries** reference remote Parquet via `read_parquet('${R2_BASE_URL}/...')`. Comments use year-based Hive partitioning (`comments_optimized/year=*/part-0.parquet`) to allow predicate pushdown.

**Bookmarks** are stored in `localStorage` under `'spicy-regs-bookmarks'` — no auth required.

**Virtualized lists** use `react-virtuoso` for large result sets.

**Cross-origin isolation** is required for DuckDB-WASM's SharedArrayBuffer support. `next.config.ts` sets `Cross-Origin-Opener-Policy: same-origin` and `Cross-Origin-Embedder-Policy: require-corp` on all routes.

### Parquet Schema on R2

| File | Rows | Notes |
|------|------|-------|
| `dockets.parquet` | ~393K | Includes enriched `document_count`, `comment_count`, `comment_start_date`, `comment_end_date` |
| `documents.parquet` | ~2.1M | |
| `comments_optimized/year=*/part-0.parquet` | ~31.6M total | Hive-partitioned by year (2000–2026) |

All fields are strings. Dates stored as strings; cast to TIMESTAMP in queries when needed.

### ETL Pipeline (`etl/`)

Reads from public Mirrulations S3 bucket (unsigned), writes per-agency staging Parquet files, then merges to R2. Uses `manifest.parquet` on R2 to track processed S3 keys for resumable incremental runs.

### Iceberg Proxy Worker (`worker/iceberg-proxy/`)

CORS proxy for the Apache Iceberg REST catalog hosted on Cloudflare, forwarding requests with R2 API token auth. Configured via `wrangler.toml`.

## Important Notes

- **AGENTS.md is outdated** — it references MotherDuck which is no longer used. The actual database layer is DuckDB-WASM + Parquet on R2.
- **Default query limit is 1000 rows** in `useDuckDBService.ts` to prevent browser OOM.
- **`castBigIntToDouble: true`** is set in DuckDB config to avoid BigInt serialization issues in React.
- Commits follow conventional format: `feat:`, `fix:`, `docs:`, `refactor:`
