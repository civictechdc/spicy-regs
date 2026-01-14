# AGENTS.md - Spicy Regs

## Project Overview

Spicy Regs is an open-source civic tech platform for exploring and analyzing federal regulations from regulations.gov. Built by CivicTechDC, it provides both technical and non-technical users with tools for rapid prototyping and reproducible analysis of regulatory data.

## Tech Stack

- **Framework**: Next.js 16 with App Router and Turbopack
- **Language**: TypeScript
- **Styling**: Tailwind CSS 4
- **Database**: MotherDuck (cloud) + DuckDB-WASM (client-side)
- **Data Source**: Mirrulations project (regulations.gov mirror)
- **Deployment**: Vercel

## Architecture

### 2-Tier Client-Side Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Browser (Client)                                       │
│  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │  Next.js App    │  │  DuckDB-WASM               │  │
│  │  React 19       │  │  (local query execution)   │  │
│  └────────┬────────┘  └──────────────┬──────────────┘  │
│           │                          │                  │
│           ▼                          ▼                  │
│  ┌─────────────────────────────────────────────────┐   │
│  │  MotherDuck Client (cloud data warehouse)       │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Directory Structure

```
/frontend
├── src/
│   ├── app/                    # Next.js App Router pages
│   │   ├── api/                # API routes
│   │   │   ├── agencies/       # List all agencies
│   │   │   ├── dockets/        # Get dockets for agency
│   │   │   ├── md-token/       # MotherDuck auth token
│   │   │   └── search/         # Full-text search
│   │   ├── dashboard/          # Main dashboard page
│   │   ├── bookmarks/          # User bookmarks page
│   │   └── search/             # Search results page
│   ├── components/
│   │   ├── AgencySelector.tsx  # Agency dropdown
│   │   ├── DataViewer.tsx      # Main data display (virtualized)
│   │   ├── DocketSelector.tsx  # Docket dropdown
│   │   └── SearchBar.tsx       # Search input
│   ├── lib/
│   │   ├── db/                 # Database layer
│   │   │   ├── client.ts       # DuckDB-WASM client
│   │   │   ├── service.ts      # Query service (getData, searchResources)
│   │   │   └── constants.ts    # Field mappings for JSON extraction
│   │   └── motherduck/         # MotherDuck integration
│   │       ├── context/        # React context for MD connection
│   │       ├── hooks/          # Custom hooks (useMotherDuckService)
│   │       └── functions/      # Query functions
│   └── utils/                  # Utility functions
├── public/                     # Static assets
└── sample-data/                # Local test data (mirrulations format)
```

## Data Model

The platform works with three primary data types from regulations.gov:

| Type | Description | Key Fields |
|------|-------------|------------|
| **Dockets** | Regulatory proceedings | docket_id, title, docket_type, agency_code |
| **Documents** | Official documents | document_id, title, document_type, posted_date |
| **Comments** | Public comments | comment_id, title, comment text, posted_date |

### Data Flow

1. Raw JSON files stored in S3 (via Mirrulations)
2. MotherDuck queries JSON directly using `json_extract()`
3. Results cached locally in DuckDB-WASM tables
4. Client queries local cache for fast subsequent access

## Key Patterns

### Database Caching
```typescript
// lib/db/service.ts - getData() pattern
1. Check local cache age
2. If fresh → return cached data
3. If stale → refresh from MotherDuck → cache locally → return
```

### Bookmarks (localStorage)
```typescript
// No authentication required
// Bookmarks stored in localStorage under 'spicy-regs-bookmarks'
const bookmarks = localStorage.getItem('spicy-regs-bookmarks');
```

### Virtualized Lists
Uses `react-virtuoso` for efficient rendering of large datasets.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `MOTHERDUCK_TOKEN` | MotherDuck authentication token |
| `MOTHERDUCK_DATABASE` | Database name to connect to |

## Development

```bash
cd frontend
bun install
bun run dev        # Start dev server on localhost:3000
bun run build      # Production build
bun run lint       # Run ESLint
```

## Coding Guidelines

1. **Keep files < 500 lines** - Split large components
2. **Use conventional commits** - `feat:`, `fix:`, `docs:`, `refactor:`
3. **Run formatters before commits** - ESLint configured
4. **No manual browser tests** - Use automated verification
5. **Update GEMINI.md** - Keep documentation current with changes

## Future Enhancements (see .agent/FEATURE_PLAN_AI_AND_TIMELINE.md)

- AI Context Awareness ("Explain This" feature)
- Timeline Visualization for docket history
