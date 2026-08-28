# Spicy Regs Analysis Notebooks

Jupyter notebooks for exploring and analyzing federal regulations data from [regulations.gov](https://www.regulations.gov/).

## Data Source

All notebooks query Parquet files hosted on Cloudflare R2 at `https://data.spicy-regs.dev`:

- **dockets.parquet** - 276K+ regulatory dockets
- **documents.parquet** - 2M+ documents
- **comments.parquet** - 25M+ public comments

That's the original three-table core. The published corpus has since grown to **20 tables** — rollups (`feed_summary`, `agency_stats`, ...) plus external federal sources (`federal_register`, `unified_agenda`, `sam_entities`, `usaspending_recipients`, ...) joinable on `rin`, `uei`, CFR citations, and `agency_code`. [`getting_started.ipynb`](getting_started.ipynb) covers the full picture; the rest of this page is mostly the original three-table view.

Data is sourced from the [Mirrulations](https://github.com/MoravianUniversity/mirrulations) project.

## Setup

```bash
pip install duckdb pandas jupyter
jupyter notebook
```

## Notebooks

### Core Data Access

| Notebook | Description |
|----------|-------------|
| [getting_started.ipynb](getting_started.ipynb) | Start here: the four access doors (DuckDB, MCP, CLI, docs site), rollups-first segment/filter, a cross-source join, and building/saving a dataset |
| [query_data.ipynb](query_data.ipynb) | Short intro to querying the three core tables directly |
| [data_explorer.ipynb](data_explorer.ipynb) | Schema docs, search utilities, export tools |
| [search_capabilities.ipynb](search_capabilities.ipynb) | Three no-server layers of search: prebuilt docket index, DuckDB metadata, partition-aware comment full-text |

### Analysis Tracks

| Track | Notebook | Problem |
|-------|----------|---------|
| **Campaign Detection** | [campaign_detection.ipynb](campaign_detection.ipynb) | Detect duplicate/template-driven comments and coordinated campaigns |
| **Entity Resolution** | [entity_resolution.ipynb](entity_resolution.ipynb) | Unify organization names and track commenters across dockets |
| **Position & Sentiment** | [position_sentiment.ipynb](position_sentiment.ipynb) | Extract nuanced positions beyond simple for/against |
| **Influence Mapping** | [influence_mapping.ipynb](influence_mapping.ipynb) | Link comments to regulatory outcomes |
| **Docket Analysis** | [docket_analysis.ipynb](docket_analysis.ipynb) | Summarize insights from thousands of comments |
| **Cross-Docket Analysis** | [cross_docket_analysis.ipynb](cross_docket_analysis.ipynb) | Map related dockets across agencies and cycles |
| **Document Navigation** | [document_navigation.ipynb](document_navigation.ipynb) | Find relevant sections in lengthy regulatory docs |

### Advanced / Maintainer-Only

These need credentials the public notebooks above don't (R2 / R2 Data Catalog API keys), so they're for people operating the pipeline rather than exploring the public data.

| Notebook | Description |
|----------|-------------|
| [iceberg_explorer.ipynb](iceberg_explorer.ipynb) | Query the R2 Data Catalog (Apache Iceberg) directly, and compare it against the public Parquet mirror |
| [vector_search.ipynb](vector_search.ipynb) | Vector + hybrid (BM25) search over comments via LanceDB |

## Quick Start

```python
import duckdb

R2_URL = "https://data.spicy-regs.dev"

conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs;")

# Search dockets
conn.execute(f"""
    SELECT docket_id, agency_code, title
    FROM read_parquet('{R2_URL}/dockets.parquet')
    WHERE LOWER(title) LIKE '%climate%'
    LIMIT 10
""").fetchdf()
```

## Problem Themes

These notebooks address key challenges identified by civic tech stakeholders:

1. **Data Accessibility** - Make regulations data easier to explore
2. **Campaign Detection** - Identify coordinated comment campaigns
3. **Entity Resolution** - Disambiguate organization names
4. **Position Analysis** - Extract nuanced stances from comments
5. **Influence Mapping** - Track comment impact on final rules
6. **Cross-Docket Analysis** - Map relationships between dockets
7. **Document Navigation** - Surface relevant sections of long documents

You can read more about it at <https://github.com/civictechdc/hackdc2025/blob/main/docs/README.md#problem-themes>

## Contributing

See the main [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.
