#!/usr/bin/env python3
"""
Local GPU Embedding Pipeline for Spicy Regs

Reads existing Parquet files and generates embeddings using sentence-transformers.
Outputs SEPARATE Parquet files with only ID + embedding columns (e.g., comments_embeddings.parquet).

Requirements:
    pip install sentence-transformers torch polars tqdm

Usage:
    # Embed documents and dockets (smaller, good for testing)
    python embed.py --data-type documents --batch-size 512
    
    # Embed comments with GPU
    python embed.py --data-type comments --batch-size 1024 --device cuda
    
    # Sample run for testing
    python embed.py --data-type comments --sample 1000
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


def get_text_for_embedding(row: dict, data_type: str) -> str:
    """Combine relevant text fields for embedding."""
    fields = TEXT_FIELDS[data_type]
    parts = []
    for field in fields:
        value = row.get(field)
        if value and isinstance(value, str) and value.strip():
            parts.append(value.strip())
    return " ".join(parts)


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
    
    # Generate embeddings in batches
    print(f"Generating embeddings (batch_size={batch_size})...")
    embeddings = []
    
    for i in tqdm(range(0, len(texts), batch_size), desc="Batches"):
        batch = texts[i:i + batch_size]
        batch_embeddings = model.encode(
            batch,
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,  # For cosine similarity
        )
        embeddings.extend(batch_embeddings.tolist())
    
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
        help="Output directory (default: same as input-dir)",
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

    # Setup paths
    input_path = args.input_dir / f"{args.data_type}.parquet"
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        print(f"Make sure you've run the ETL pipeline first.")
        exit(1)

    output_dir = args.output_dir or args.input_dir
    output_path = output_dir / f"{args.data_type}_embeddings.parquet"

    # Load model
    print(f"Loading model: {model_name}")
    print(f"Device: {args.device}")
    model = SentenceTransformer(model_name, device=args.device)
    
    # Get embedding dimensions
    dims = model.get_sentence_embedding_dimension()
    print(f"Embedding dimensions: {dims}")

    # Process
    count = embed_parquet(
        input_path=input_path,
        output_path=output_path,
        data_type=args.data_type,
        model=model,
        batch_size=args.batch_size,
        sample=args.sample,
    )

    print(f"\n✓ Done! Embedded {count:,} {args.data_type} records")
    print(f"  Output: {output_path}")


if __name__ == "__main__":
    main()
