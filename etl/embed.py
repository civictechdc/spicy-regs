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

import numpy as np
import polars as pl
import pyarrow as pa
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


ENCODE_CHUNK = 10_000  # texts per encode() call to avoid tokenization overhead


def generate_embeddings(
    texts: list[str],
    model: SentenceTransformer,
    batch_size: int = 256,
) -> np.ndarray:
    """Generate normalized embeddings in chunks. Returns numpy array of shape (N, dims)."""
    print(f"Generating embeddings (batch_size={batch_size}, {len(texts):,} texts)...")

    if len(texts) <= ENCODE_CHUNK:
        return model.encode(
            texts,
            batch_size=batch_size,
            show_progress_bar=True,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )

    # Process in chunks to avoid sentence-transformers tokenizing/sorting the full list
    chunks = []
    for i in tqdm(range(0, len(texts), ENCODE_CHUNK), desc="Encoding"):
        chunk = model.encode(
            texts[i:i + ENCODE_CHUNK],
            batch_size=batch_size,
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        chunks.append(chunk)
    return np.vstack(chunks)


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

    # Extract text for embedding using Polars (no Python loops)
    id_field = ID_FIELDS[data_type]
    df = _prepare_text_column(df, data_type)
    texts = df["text"].to_list()

    embeddings = generate_embeddings(texts, model, batch_size)

    # Create embeddings-only table via PyArrow (avoids slow .tolist() conversion)
    arrow_table = pa.table({
        id_field: df[id_field].to_arrow(),
        "embedding": pa.FixedSizeListArray.from_arrays(
            pa.array(embeddings.ravel(), type=pa.float32()),
            list_size=embeddings.shape[1],
        ),
    })
    embeddings_df = pl.from_arrow(arrow_table)

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


def _prepare_text_column(df: pl.DataFrame, data_type: str) -> pl.DataFrame:
    """Add a 'text' column by concatenating text fields in Polars (no Python loops)."""
    fields = TEXT_FIELDS[data_type]
    # Fill nulls with empty string, strip whitespace, then concatenate with space separator
    exprs = [pl.col(f).fill_null("").str.strip_chars() for f in fields]
    return df.with_columns(
        pl.concat_str(exprs, separator=" ").str.strip_chars().alias("text"),
    )


def _build_lance_table(
    df: pl.DataFrame,
    data_type: str,
    id_field: str,
    metadata_fields: list[str],
    embeddings: np.ndarray,
) -> pa.Table:
    """Build a PyArrow table for LanceDB from Polars DataFrame + numpy embeddings."""
    # Strip surrounding quotes from string columns
    strip_cols = [id_field] + [f for f in metadata_fields if f != id_field]
    df = df.with_columns([
        pl.col(c).fill_null("").str.strip_chars('"').alias(c) for c in strip_cols if c in df.columns
    ])

    # Build the output columns: id, text, metadata, vector
    out_cols = {"id": df[id_field].to_arrow(), "text": df["text"].to_arrow()}
    for field in metadata_fields:
        if field != id_field and field in df.columns:
            out_cols[field] = df[field].to_arrow()

    # Add embeddings as a fixed-size list column directly from numpy
    out_cols["vector"] = pa.FixedSizeListArray.from_arrays(
        pa.array(embeddings.ravel(), type=pa.float32()),
        list_size=embeddings.shape[1],
    )

    return pa.table(out_cols)


# How many records to embed + write per chunk (keeps RAM under ~4GB)
CHUNK_SIZE = 100_000


def embed_to_lancedb(
    input_dir: Path,
    lance_uri: str,
    table_name: str,
    data_type: str,
    model: SentenceTransformer,
    batch_size: int = 512,
    sample: int | None = None,
    year_filter: tuple[int, int] | None = None,
    rebuild: bool = False,
) -> int:
    """
    Read Parquet data, generate embeddings, and write to a LanceDB table.

    Runs incrementally by default — skips records already in the table.
    Use rebuild=True to drop and recreate from scratch.

    Processes data in chunks to keep memory usage bounded.
    The table stores vectors alongside metadata for hybrid search (vector ANN + BM25 FTS).

    Returns number of records processed.
    """
    import lancedb

    df = read_input_data(input_dir, data_type, sample, year_filter)

    id_field = ID_FIELDS[data_type]
    metadata_fields = METADATA_FIELDS[data_type]

    print(f"\nConnecting to LanceDB at {lance_uri}...")
    db = lancedb.connect(lance_uri)

    table = None
    if table_name in db.table_names():
        if rebuild:
            print(f"Dropping existing table '{table_name}' (--rebuild)...")
            db.drop_table(table_name)
        else:
            table = db.open_table(table_name)
            existing_ids = set(
                table.to_lance_dataset()
                .to_table(columns=["id"])
                .column("id")
                .to_pylist()
            )
            print(f"Found existing table with {len(existing_ids):,} rows")
            df = df.filter(~pl.col(id_field).str.strip_chars('"').is_in(existing_ids))
            del existing_ids
            print(f"  {len(df):,} new records to embed")
            if len(df) == 0:
                print("No new records to process.")
                return 0

    # Pre-compute text column once for the entire DataFrame
    df = _prepare_text_column(df, data_type)

    total_rows = len(df)
    total_written = 0
    num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE

    for chunk_idx in range(num_chunks):
        start = chunk_idx * CHUNK_SIZE
        end = min(start + CHUNK_SIZE, total_rows)
        chunk_df = df.slice(start, end - start)

        print(f"\n--- Chunk {chunk_idx + 1}/{num_chunks} ({start:,}-{end:,} of {total_rows:,}) ---")

        texts = chunk_df["text"].to_list()
        embeddings = generate_embeddings(texts, model, batch_size)
        arrow_table = _build_lance_table(chunk_df, data_type, id_field, metadata_fields, embeddings)

        if table is None:
            print(f"Creating table '{table_name}'...")
            table = db.create_table(table_name, data=arrow_table)
        else:
            table.add(arrow_table)

        total_written += len(chunk_df)
        print(f"  Written so far: {total_written:,}/{total_rows:,}")

        # Free chunk memory
        del texts, embeddings, arrow_table, chunk_df

    # Build indexes after all data is written
    print(f"\nAll {total_written:,} rows written. Building indexes...")

    if total_written >= 256:
        num_partitions = min(256, max(1, int(total_written ** 0.5)))
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
    print(f"  Rows:  {total_written:,}")
    print(f"  URI:   {lance_uri}")

    return total_written


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
        default=256,
        help="Batch size for embedding (default: 256)",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Only process N random samples (for testing)",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Drop and rebuild LanceDB table from scratch (default: incremental)",
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

    # Use fp16 on GPU for ~2x faster inference with negligible quality loss
    if args.device in ("cuda", "mps"):
        model.half()
        print("Using fp16 precision")

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
            rebuild=args.rebuild,
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
