#!/usr/bin/env python3
"""
Local GPU Embedding Pipeline for Spicy Regs

Reads existing Parquet files and generates embeddings using sentence-transformers.
Supports two output formats:
  - parquet: SEPARATE Parquet files with only ID + embedding columns (legacy)
  - lancedb: LanceDB table with vectors, FTS index, and metadata for hybrid search

Requirements:
    pip install sentence-transformers torch polars tqdm lancedb

Usage:
    # Embed to Parquet (legacy)
    python embed.py --data-type comments --batch-size 512

    # Embed to LanceDB (hybrid search)
    python embed.py --data-type comments --output-format lancedb --lance-uri ./lance-data

    # Embed to LanceDB on S3/R2
    python embed.py --data-type comments --output-format lancedb \
        --lance-uri s3://bucket/lance --sample 1000

    # Filter to recent years only
    python embed.py --data-type comments --output-format lancedb \
        --lance-uri ./lance-data --year-filter 2020-2026
"""

import argparse
from pathlib import Path

import polars as pl
from tqdm import tqdm

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("Please install sentence-transformers: pip install sentence-transformers torch")
    exit(1)


# Model presets for easy selection
# Use --preset to select, or --model for custom HuggingFace model
MODEL_PRESETS = {
    "small": {
        "name": "BAAI/bge-small-en-v1.5",
        "dims": 384,
        "desc": "Fastest, good for testing/CPU",
    },
    "base": {
        "name": "BAAI/bge-base-en-v1.5",
        "dims": 768,
        "desc": "Balanced quality/speed (default)",
    },
    "large": {
        "name": "BAAI/bge-large-en-v1.5",
        "dims": 1024,
        "desc": "Highest quality, slowest",
    },
    "minilm": {
        "name": "sentence-transformers/all-MiniLM-L6-v2",
        "dims": 384,
        "desc": "Very fast, lightweight",
    },
    "nomic": {
        "name": "nomic-ai/nomic-embed-text-v1.5",
        "dims": 768,
        "desc": "Good quality, long context (8192 tokens)",
    },
}

DEFAULT_PRESET = "base"

# Text fields to embed for each data type
TEXT_FIELDS = {
    "dockets": ["title", "abstract"],
    "documents": ["title"],
    "comments": ["title", "comment"],
}

# ID field for each data type
ID_FIELDS = {
    "dockets": "docket_id",
    "documents": "document_id",
    "comments": "comment_id",
}

# Metadata columns to carry into LanceDB for each data type
METADATA_FIELDS = {
    "dockets": ["docket_id", "agency_code", "title"],
    "documents": ["document_id", "docket_id", "agency_code", "title"],
    "comments": ["comment_id", "docket_id", "agency_code", "title", "comment", "posted_date"],
}


def get_text_for_embedding(row: dict, data_type: str) -> str:
    """Combine relevant text fields for embedding."""
    fields = TEXT_FIELDS[data_type]
    parts = []
    for field in fields:
        value = row.get(field)
        if value and isinstance(value, str) and value.strip():
            parts.append(value.strip())
    return " ".join(parts)


def strip_quotes(value: str | None) -> str:
    """Strip surrounding double quotes from Parquet string values."""
    if not value or not isinstance(value, str):
        return value or ""
    return value.strip('"')


def read_input_data(
    input_dir: Path,
    data_type: str,
    sample: int | None = None,
    year_filter: tuple[int, int] | None = None,
) -> pl.DataFrame:
    """Read Parquet input data, handling Hive-partitioned comments."""
    if data_type == "comments" and (input_dir / "comments_optimized").exists():
        # Read from Hive-partitioned directory
        partition_dir = input_dir / "comments_optimized"
        if year_filter:
            start_year, end_year = year_filter
            paths = []
            for year in range(start_year, end_year + 1):
                p = partition_dir / f"year={year}" / "part-0.parquet"
                if p.exists():
                    paths.append(p)
            if not paths:
                print(f"Error: No partitions found for years {start_year}-{end_year}")
                exit(1)
            print(f"Reading {len(paths)} year partitions ({start_year}-{end_year})...")
            df = pl.read_parquet(paths, hive_partitioning=True)
        else:
            print(f"Reading all partitions from {partition_dir}...")
            df = pl.read_parquet(
                str(partition_dir / "**" / "*.parquet"),
                hive_partitioning=True,
            )
    else:
        input_path = input_dir / f"{data_type}.parquet"
        if not input_path.exists():
            print(f"Error: Input file not found: {input_path}")
            print("Make sure you've run the ETL pipeline first.")
            exit(1)
        print(f"Reading {input_path}...")
        df = pl.read_parquet(input_path)

    if sample and sample < len(df):
        print(f"Sampling {sample:,} records from {len(df):,}")
        df = df.sample(n=sample, seed=42)

    print(f"Processing {len(df):,} records...")
    return df


def generate_embeddings(
    texts: list[str],
    model: SentenceTransformer,
    batch_size: int = 512,
) -> list[list[float]]:
    """Generate normalized embeddings in batches."""
    print(f"Generating embeddings (batch_size={batch_size})...")
    embeddings = []

    for i in tqdm(range(0, len(texts), batch_size), desc="Batches"):
        batch = texts[i:i + batch_size]
        batch_embeddings = model.encode(
            batch,
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        embeddings.extend(batch_embeddings.tolist())

    return embeddings


def embed_parquet(
    input_path: Path,
    output_path: Path,
    data_type: str,
    model: SentenceTransformer,
    batch_size: int = 512,
    sample: int | None = None,
) -> int:
    """
    Read a Parquet file, generate embeddings, and write to SEPARATE embeddings file.

    Output file contains only: id column + embedding column
    This keeps the original Parquet small and allows joining only when needed.

    Returns number of records processed.
    """
    print(f"\nReading {input_path}...")
    df = pl.read_parquet(input_path)

    if sample and sample < len(df):
        print(f"Sampling {sample:,} records from {len(df):,}")
        df = df.sample(n=sample, seed=42)

    print(f"Processing {len(df):,} records...")

    # Extract text for embedding
    id_field = ID_FIELDS[data_type]
    records = df.to_dicts()

    ids = []
    texts = []
    for row in records:
        ids.append(row[id_field])
        text = get_text_for_embedding(row, data_type)
        texts.append(text if text else "")

    embeddings = generate_embeddings(texts, model, batch_size)

    # Create embeddings-only DataFrame (id + embedding only)
    embeddings_df = pl.DataFrame({
        id_field: ids,
        "embedding": embeddings,
    })

    # Write output
    print(f"Writing to {output_path}...")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    embeddings_df.write_parquet(output_path, compression="zstd")

    # Report sizes
    input_size = input_path.stat().st_size / (1024 * 1024)
    output_size = output_path.stat().st_size / (1024 * 1024)
    print(f"  Original data: {input_size:.1f} MB (unchanged)")
    print(f"  Embeddings:    {output_size:.1f} MB")

    return len(embeddings_df)


def embed_to_lancedb(
    input_dir: Path,
    lance_uri: str,
    table_name: str,
    data_type: str,
    model: SentenceTransformer,
    batch_size: int = 512,
    sample: int | None = None,
    year_filter: tuple[int, int] | None = None,
) -> int:
    """
    Read Parquet data, generate embeddings, and write to a LanceDB table.

    The table stores vectors alongside metadata for hybrid search (vector ANN + BM25 FTS).

    Returns number of records processed.
    """
    import lancedb

    df = read_input_data(input_dir, data_type, sample, year_filter)

    id_field = ID_FIELDS[data_type]
    metadata_fields = METADATA_FIELDS[data_type]
    records = df.to_dicts()

    # Build rows with text + metadata
    texts = []
    rows = []
    for row in records:
        text = get_text_for_embedding(row, data_type)
        texts.append(text if text else "")

        lance_row = {
            "id": strip_quotes(row[id_field]),
            "text": text if text else "",
        }
        for field in metadata_fields:
            if field != id_field:
                lance_row[field] = strip_quotes(row.get(field))
        rows.append(lance_row)

    # Generate embeddings
    embeddings = generate_embeddings(texts, model, batch_size)

    # Attach vectors to rows
    for i, emb in enumerate(embeddings):
        rows[i]["vector"] = emb

    # Write to LanceDB
    print(f"\nConnecting to LanceDB at {lance_uri}...")
    db = lancedb.connect(lance_uri)

    if table_name in db.table_names():
        print(f"Dropping existing table '{table_name}'...")
        db.drop_table(table_name)

    print(f"Creating table '{table_name}' with {len(rows):,} rows...")
    table = db.create_table(table_name, data=rows)

    # Build IVF-PQ vector index for ANN search
    num_rows = len(rows)
    if num_rows >= 256:
        num_partitions = min(256, max(1, int(num_rows ** 0.5)))
        print(f"Creating IVF-PQ vector index (num_partitions={num_partitions})...")
        table.create_index(
            metric="cosine",
            num_partitions=num_partitions,
            num_sub_vectors=48,
        )
    else:
        print("Skipping vector index (too few rows for IVF-PQ, brute-force will be used)")

    # Build native Lance inverted index on text column for BM25 search
    # use_tantivy=False creates a native Lance inverted index that works
    # with both Python and JS SDKs, and persists on object storage (S3/R2)
    print("Creating full-text search (inverted) index on 'text' column...")
    table.create_fts_index("text", use_tantivy=False)

    print(f"  Table: {table_name}")
    print(f"  Rows:  {num_rows:,}")
    print(f"  URI:   {lance_uri}")

    return num_rows


def parse_year_filter(value: str) -> tuple[int, int]:
    """Parse year filter string like '2020-2026' into a (start, end) tuple."""
    parts = value.split("-")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(f"Expected format YYYY-YYYY, got '{value}'")
    try:
        start, end = int(parts[0]), int(parts[1])
    except ValueError:
        raise argparse.ArgumentTypeError(f"Expected format YYYY-YYYY, got '{value}'")
    if start > end:
        raise argparse.ArgumentTypeError(f"Start year {start} > end year {end}")
    return (start, end)


def main():
    # Build preset help text
    preset_help = "Model presets:\n"
    for key, val in MODEL_PRESETS.items():
        preset_help += f"    {key:8} - {val['desc']} ({val['dims']}d)\n"

    parser = argparse.ArgumentParser(
        description="Generate embeddings for Spicy Regs Parquet files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
{preset_help}
Examples:
    # Quick test with small model on CPU
    python embed.py --data-type documents --preset small --device cpu

    # Full embedding with default (base) model on GPU
    python embed.py --data-type comments --device cuda --batch-size 1024

    # High quality embeddings
    python embed.py --data-type comments --preset large --device cuda

    # Test with samples
    python embed.py --data-type comments --preset minilm --sample 1000

    # Output to LanceDB for hybrid search
    python embed.py --data-type comments --output-format lancedb --lance-uri ./lance-data

    # LanceDB with year filter (recent comments only)
    python embed.py --data-type comments --output-format lancedb \\
        --lance-uri ./lance-data --year-filter 2020-2026 --device mps
        """
    )
    parser.add_argument(
        "--data-type",
        choices=["dockets", "documents", "comments"],
        required=True,
        help="Type of data to embed",
    )
    parser.add_argument(
        "--preset",
        choices=list(MODEL_PRESETS.keys()),
        default=DEFAULT_PRESET,
        help=f"Model preset (default: {DEFAULT_PRESET})",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Custom HuggingFace model (overrides --preset)",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("output"),
        help="Directory containing input Parquet files (default: output/)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for parquet format (default: same as input-dir)",
    )
    parser.add_argument(
        "--output-format",
        choices=["parquet", "lancedb"],
        default="parquet",
        help="Output format: parquet (legacy) or lancedb (hybrid search). Default: parquet",
    )
    parser.add_argument(
        "--lance-uri",
        default=None,
        help="LanceDB URI (local path or s3://bucket/path). Required when --output-format=lancedb",
    )
    parser.add_argument(
        "--year-filter",
        type=parse_year_filter,
        default=None,
        help="Filter comments by year range, e.g. 2020-2026. Only used with lancedb format",
    )
    parser.add_argument(
        "--device",
        default="cuda",
        help="Device to use (cuda, cpu, mps). Default: cuda",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=512,
        help="Batch size for embedding (default: 512)",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Only process N random samples (for testing)",
    )
    args = parser.parse_args()

    # Resolve model from preset or custom
    if args.model:
        model_name = args.model
        print(f"Using custom model: {model_name}")
    else:
        preset = MODEL_PRESETS[args.preset]
        model_name = preset["name"]
        print(f"Using preset '{args.preset}': {preset['desc']}")

    # Validate args
    if args.output_format == "lancedb" and not args.lance_uri:
        parser.error("--lance-uri is required when --output-format=lancedb")

    # Load model
    print(f"Loading model: {model_name}")
    print(f"Device: {args.device}")
    model = SentenceTransformer(model_name, device=args.device)

    # Get embedding dimensions
    dims = model.get_sentence_embedding_dimension()
    print(f"Embedding dimensions: {dims}")

    # Process based on output format
    if args.output_format == "lancedb":
        count = embed_to_lancedb(
            input_dir=args.input_dir,
            lance_uri=args.lance_uri,
            table_name=args.data_type,
            data_type=args.data_type,
            model=model,
            batch_size=args.batch_size,
            sample=args.sample,
            year_filter=args.year_filter,
        )
        print(f"\n Done! Embedded {count:,} {args.data_type} records to LanceDB")
        print(f"  URI: {args.lance_uri}")
        print(f"  Table: {args.data_type}")
    else:
        # Legacy parquet output
        input_path = args.input_dir / f"{args.data_type}.parquet"
        if not input_path.exists():
            print(f"Error: Input file not found: {input_path}")
            print("Make sure you've run the ETL pipeline first.")
            exit(1)

        output_dir = args.output_dir or args.input_dir
        output_path = output_dir / f"{args.data_type}_embeddings.parquet"

        count = embed_parquet(
            input_path=input_path,
            output_path=output_path,
            data_type=args.data_type,
            model=model,
            batch_size=args.batch_size,
            sample=args.sample,
        )
        print(f"\n Done! Embedded {count:,} {args.data_type} records")
        print(f"  Output: {output_path}")


if __name__ == "__main__":
    main()
