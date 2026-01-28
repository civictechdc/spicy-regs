#!/usr/bin/env python3
"""
Generate pre-computed analytics JSON files from Parquet data.
These are uploaded to R2 alongside the Parquet files for fast frontend access.
"""

import json
from pathlib import Path
import duckdb

# Default R2 URL (can be overridden for local testing)
R2_BASE_URL = "https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev"


def generate_analytics(parquet_dir: Path | None = None, output_dir: Path | None = None) -> dict[str, Path]:
    """
    Generate analytics JSON files from Parquet data.
    
    Args:
        parquet_dir: Directory containing parquet files. If None, reads from R2.
        output_dir: Directory to write JSON files. Defaults to parquet_dir or current dir.
    
    Returns:
        Dict mapping analytics name to output file path.
    """
    conn = duckdb.connect()
    
    # Determine data source
    if parquet_dir:
        comments_src = f"'{parquet_dir}/comments.parquet'"
        dockets_src = f"'{parquet_dir}/dockets.parquet'"
        documents_src = f"'{parquet_dir}/documents.parquet'"
    else:
        # Install httpfs for remote access
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        comments_src = f"'{R2_BASE_URL}/comments.parquet'"
        dockets_src = f"'{R2_BASE_URL}/dockets.parquet'"
        documents_src = f"'{R2_BASE_URL}/documents.parquet'"
    
    output_dir = output_dir or parquet_dir or Path.cwd()
    analytics_dir = output_dir / "analytics"
    analytics_dir.mkdir(parents=True, exist_ok=True)
    
    outputs = {}
    
    # 1. Statistics - dataset overview
    print("Generating statistics...")
    stats_query = f"""
    WITH stats AS (
        SELECT 
            (SELECT COUNT(*) FROM read_parquet({dockets_src})) as total_dockets,
            (SELECT COUNT(*) FROM read_parquet({documents_src})) as total_documents,
            (SELECT COUNT(*) FROM read_parquet({comments_src})) as total_comments
    ),
    top_agency AS (
        SELECT agency_code, COUNT(*) as cnt
        FROM read_parquet({comments_src})
        GROUP BY agency_code
        ORDER BY cnt DESC
        LIMIT 1
    )
    SELECT 
        s.total_dockets,
        s.total_documents,
        s.total_comments,
        t.agency_code as top_agency,
        t.cnt as top_agency_comments
    FROM stats s, top_agency t
    """
    result = conn.execute(stats_query).fetchall()
    columns = ["total_dockets", "total_documents", "total_comments", "top_agency", "top_agency_comments"]
    stats_data = [dict(zip(columns, row)) for row in result]
    
    stats_file = analytics_dir / "statistics.json"
    with open(stats_file, "w") as f:
        json.dump(stats_data, f)
    outputs["statistics"] = stats_file
    print(f"  ✓ statistics.json: {stats_data}")
    
    # 2. Campaigns - dockets with high duplicate comment rates
    print("Generating campaigns...")
    campaigns_query = f"""
    SELECT 
        docket_id,
        agency_code,
        COUNT(*) as total_comments,
        COUNT(DISTINCT comment) as unique_texts,
        ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT comment)) / COUNT(*), 1) as duplicate_percentage
    FROM read_parquet({comments_src})
    WHERE comment IS NOT NULL
    GROUP BY docket_id, agency_code
    HAVING COUNT(*) > 1000 AND COUNT(*) > COUNT(DISTINCT comment)
    ORDER BY duplicate_percentage DESC
    LIMIT 10
    """
    result = conn.execute(campaigns_query).fetchall()
    columns = ["docket_id", "agency_code", "total_comments", "unique_texts", "duplicate_percentage"]
    campaigns_data = [dict(zip(columns, row)) for row in result]
    
    campaigns_file = analytics_dir / "campaigns.json"
    with open(campaigns_file, "w") as f:
        json.dump(campaigns_data, f)
    outputs["campaigns"] = campaigns_file
    print(f"  ✓ campaigns.json: {len(campaigns_data)} rows")
    
    # 3. Organizations - most active commenters
    print("Generating organizations...")
    orgs_query = f"""
    SELECT 
        title,
        COUNT(*) as comment_count,
        COUNT(DISTINCT docket_id) as docket_count
    FROM read_parquet({comments_src})
    WHERE title IS NOT NULL
        AND title NOT LIKE 'Comment%'
        AND title NOT LIKE 'Anonymous%'
        AND LENGTH(title) > 5
    GROUP BY title
    HAVING COUNT(DISTINCT docket_id) > 50
    ORDER BY docket_count DESC
    LIMIT 15
    """
    result = conn.execute(orgs_query).fetchall()
    columns = ["title", "comment_count", "docket_count"]
    orgs_data = [dict(zip(columns, row)) for row in result]
    
    orgs_file = analytics_dir / "organizations.json"
    with open(orgs_file, "w") as f:
        json.dump(orgs_data, f)
    outputs["organizations"] = orgs_file
    print(f"  ✓ organizations.json: {len(orgs_data)} rows")
    
    conn.close()
    print(f"\nAnalytics generated in: {analytics_dir}")
    return outputs


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate analytics JSON from Parquet")
    parser.add_argument("--parquet-dir", type=Path, help="Directory with parquet files (default: read from R2)")
    parser.add_argument("--output-dir", type=Path, help="Output directory for JSON files")
    args = parser.parse_args()
    
    generate_analytics(args.parquet_dir, args.output_dir)
