#!/usr/bin/env python3
"""
Spicy Regs CLI - Download and explore federal regulations data.

Usage:
    uvx spicy-regs download           # Download all parquet files
    uvx spicy-regs stats              # Show dataset statistics
    uvx spicy-regs sample dockets     # Show sample rows from a dataset
    uvx spicy-regs search "climate"   # Search across datasets
"""

import argparse
import sys
from pathlib import Path
from urllib.request import urlretrieve
from urllib.error import URLError

# Public URL for the R2 bucket
PUBLIC_URL = "https://pub-e5960e2431b049dc9380ddecb88f1cc7.r2.dev"
DATA_TYPES = ["dockets", "documents", "comments", "manifest"]
DEFAULT_OUTPUT_DIR = Path("./spicy-regs-data")


def get_output_dir(args) -> Path:
    """Get output directory from args or default."""
    output_dir = Path(args.output_dir) if hasattr(args, "output_dir") and args.output_dir else DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def download_file(name: str, output_dir: Path, force: bool = False) -> Path | None:
    """Download a parquet file from R2."""
    url = f"{PUBLIC_URL}/{name}.parquet"
    local_path = output_dir / f"{name}.parquet"

    if local_path.exists() and not force:
        size_mb = local_path.stat().st_size / (1024 * 1024)
        print(f"  ✓ {name}.parquet already exists ({size_mb:.1f} MB)")
        return local_path

    print(f"  ⬇ Downloading {name}.parquet...")
    try:
        urlretrieve(url, local_path)
        size_mb = local_path.stat().st_size / (1024 * 1024)
        print(f"  ✓ {name}.parquet ({size_mb:.1f} MB)")
        return local_path
    except URLError as e:
        print(f"  ✗ Failed to download {name}.parquet: {e}")
        return None


def cmd_download(args):
    """Download parquet files from R2."""
    output_dir = get_output_dir(args)
    print(f"Downloading to: {output_dir.absolute()}")

    types_to_download = args.types if args.types else ["dockets", "documents", "comments"]

    for data_type in types_to_download:
        download_file(data_type, output_dir, force=args.force)

    print(f"\nDone! Data saved to: {output_dir.absolute()}")


def cmd_stats(args):
    """Show statistics for downloaded datasets."""
    try:
        import polars as pl
    except ImportError:
        print("Please install polars: pip install polars")
        sys.exit(1)

    output_dir = get_output_dir(args)

    print("=" * 60)
    print("Dataset Statistics")
    print("=" * 60)

    for data_type in ["dockets", "documents", "comments"]:
        parquet_file = output_dir / f"{data_type}.parquet"
        if not parquet_file.exists():
            print(f"\n{data_type.upper()}: Not downloaded yet (run: spicy-regs download)")
            continue

        df = pl.read_parquet(parquet_file)
        size_mb = parquet_file.stat().st_size / (1024 * 1024)

        print(f"\n{data_type.upper()} ({size_mb:.1f} MB)")
        print("-" * 40)
        print(f"  Rows: {len(df):,}")
        print(f"  Columns: {', '.join(df.columns)}")

        # Agency breakdown
        if "agency_code" in df.columns:
            agency_counts = df.group_by("agency_code").len().sort("len", descending=True)
            top_agencies = agency_counts.head(5)
            print("  Top agencies:")
            for row in top_agencies.iter_rows():
                print(f"    {row[0]}: {row[1]:,}")


def cmd_sample(args):
    """Show sample rows from a dataset."""
    try:
        import polars as pl
    except ImportError:
        print("Please install polars: pip install polars")
        sys.exit(1)

    output_dir = get_output_dir(args)
    parquet_file = output_dir / f"{args.data_type}.parquet"

    if not parquet_file.exists():
        print(f"File not found: {parquet_file}")
        print("Run: spicy-regs download")
        sys.exit(1)

    df = pl.read_parquet(parquet_file)

    if args.agency:
        df = df.filter(pl.col("agency_code") == args.agency)

    sample = df.sample(min(args.n, len(df)))

    print(f"\nSample from {args.data_type} ({len(df):,} total rows):")
    print("=" * 80)
    print(sample)


def cmd_search(args):
    """Search across datasets."""
    try:
        import polars as pl
    except ImportError:
        print("Please install polars: pip install polars")
        sys.exit(1)

    output_dir = get_output_dir(args)
    query = args.query.lower()

    print(f"Searching for: '{args.query}'")
    print("=" * 60)

    search_configs = {
        "dockets": ["title", "abstract"],
        "documents": ["title"],
        "comments": ["title", "comment"],
    }

    for data_type, columns in search_configs.items():
        parquet_file = output_dir / f"{data_type}.parquet"
        if not parquet_file.exists():
            continue

        df = pl.read_parquet(parquet_file)

        # Build filter for any column containing the query
        filters = None
        for col in columns:
            if col in df.columns:
                col_filter = pl.col(col).str.to_lowercase().str.contains(query, literal=True)
                filters = col_filter if filters is None else (filters | col_filter)

        if filters is not None:
            matches = df.filter(filters)
            if len(matches) > 0:
                print(f"\n{data_type.upper()}: {len(matches):,} matches")
                print("-" * 40)
                sample = matches.head(args.limit)
                for row in sample.iter_rows(named=True):
                    id_col = list(row.keys())[0]
                    title = row.get("title", "")[:80] if row.get("title") else "(no title)"
                    print(f"  {row[id_col]}: {title}")


def cmd_agencies(args):
    """List all agencies in the dataset."""
    try:
        import polars as pl
    except ImportError:
        print("Please install polars: pip install polars")
        sys.exit(1)

    output_dir = get_output_dir(args)

    # Try to get agency list from any available file
    for data_type in ["dockets", "documents", "comments"]:
        parquet_file = output_dir / f"{data_type}.parquet"
        if parquet_file.exists():
            df = pl.read_parquet(parquet_file, columns=["agency_code"])
            agencies = df["agency_code"].unique().sort().to_list()

            print(f"Agencies ({len(agencies)} total):")
            print("=" * 40)
            for agency in agencies:
                if agency:
                    print(f"  {agency}")
            return

    print("No data downloaded yet. Run: spicy-regs download")


def main():
    parser = argparse.ArgumentParser(
        prog="spicy-regs",
        description="Download and explore federal regulations data from Spicy Regs",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        help=f"Output directory for data files (default: {DEFAULT_OUTPUT_DIR})",
        default=None,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Download command
    download_parser = subparsers.add_parser("download", help="Download parquet files")
    download_parser.add_argument("--force", "-f", action="store_true", help="Force re-download")
    download_parser.add_argument(
        "--types", nargs="+", choices=["dockets", "documents", "comments"], help="Specific data types to download"
    )
    download_parser.set_defaults(func=cmd_download)

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show dataset statistics")
    stats_parser.set_defaults(func=cmd_stats)

    # Sample command
    sample_parser = subparsers.add_parser("sample", help="Show sample rows")
    sample_parser.add_argument("data_type", choices=["dockets", "documents", "comments"])
    sample_parser.add_argument("-n", type=int, default=5, help="Number of rows")
    sample_parser.add_argument("--agency", help="Filter by agency code")
    sample_parser.set_defaults(func=cmd_sample)

    # Search command
    search_parser = subparsers.add_parser("search", help="Search across datasets")
    search_parser.add_argument("query", help="Search query")
    search_parser.add_argument("--limit", "-l", type=int, default=10, help="Max results per type")
    search_parser.set_defaults(func=cmd_search)

    # Agencies command
    agencies_parser = subparsers.add_parser("agencies", help="List all agencies")
    agencies_parser.set_defaults(func=cmd_agencies)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    args.func(args)


if __name__ == "__main__":
    main()
