"""``spicy-regs download`` — fetch published parquet files from the public bucket.

Downloads are streamed to a ``.tmp`` file and atomically renamed into place, so
an interrupted download never leaves a truncated parquet behind. Files that
already match the remote size (HEAD Content-Length) are skipped; ``--force``
re-downloads unconditionally.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import httpx
from tqdm import tqdm

from spicy_regs.cli import engine
from spicy_regs.cli._common import add_output_dir_argument, get_output_dir
from spicy_regs.data_dictionary import DEFAULT_R2_BASE_URL

# Everything published to the bucket: the queryable tables plus the pipeline
# manifest snapshot.
DOWNLOADABLE = (*engine.TABLES, "manifest")
DEFAULT_TABLES = ("dockets", "documents", "comments")


def register(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("download", help="Download parquet files from the public bucket")
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=DOWNLOADABLE,
        metavar="TABLE",
        help=f"Tables to download (default: {' '.join(DEFAULT_TABLES)}); one of: {', '.join(DOWNLOADABLE)}",
    )
    # Backwards-compatible alias for the pre-1.0 flag spelling.
    parser.add_argument("--types", nargs="+", choices=DOWNLOADABLE, dest="tables", help=argparse.SUPPRESS)
    parser.add_argument(
        "--all",
        action="store_true",
        dest="download_all",
        help="Download every published table (comments alone is multiple GB)",
    )
    parser.add_argument("--force", "-f", action="store_true", help="Re-download even if the local file looks current")
    parser.add_argument(
        "--r2-url",
        default=DEFAULT_R2_BASE_URL,
        help=f"Base URL of the public parquet bucket (default: {DEFAULT_R2_BASE_URL})",
    )
    add_output_dir_argument(parser)
    parser.set_defaults(run=run)


def _build_client(transport: httpx.BaseTransport | None = None) -> httpx.Client:
    """HTTP client for bucket downloads (``transport`` is a test seam)."""
    return httpx.Client(follow_redirects=True, timeout=httpx.Timeout(30.0, read=300.0), transport=transport)


def _is_up_to_date(client: httpx.Client, url: str, local_path: Path) -> bool:
    """True when the local file's size matches the remote Content-Length."""
    if not local_path.exists():
        return False
    try:
        response = client.head(url)
        response.raise_for_status()
        content_length = response.headers.get("content-length")
    except httpx.HTTPError:
        return False  # can't tell — re-download
    return content_length is not None and int(content_length) == local_path.stat().st_size


def download_file(client: httpx.Client, url: str, local_path: Path, force: bool = False) -> str:
    """Download one file; returns ``"downloaded"``, ``"skipped"``, ``"missing"``, or ``"failed"``."""
    name = local_path.name
    if not force and _is_up_to_date(client, url, local_path):
        size_mb = local_path.stat().st_size / (1024 * 1024)
        print(f"  ✓ {name} up to date ({size_mb:.1f} MB)")
        return "skipped"

    temp_path = local_path.with_suffix(local_path.suffix + ".tmp")
    try:
        with client.stream("GET", url) as response:
            if response.status_code == 404:
                print(f"  - {name} is not published at {url}; skipping")
                return "missing"
            response.raise_for_status()
            total = int(response.headers.get("content-length", 0)) or None
            with (
                temp_path.open("wb") as fh,
                tqdm(total=total, unit="B", unit_scale=True, desc=f"  ⬇ {name}", disable=None, leave=False) as bar,
            ):
                for chunk in response.iter_bytes():
                    fh.write(chunk)
                    bar.update(len(chunk))
        temp_path.replace(local_path)
    except (httpx.HTTPError, OSError) as exc:
        temp_path.unlink(missing_ok=True)
        print(f"  ✗ Failed to download {name}: {exc}", file=sys.stderr)
        return "failed"
    size_mb = local_path.stat().st_size / (1024 * 1024)
    print(f"  ✓ {name} ({size_mb:.1f} MB)")
    return "downloaded"


def run(args: argparse.Namespace) -> int:
    output_dir = get_output_dir(args)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Downloading to: {output_dir.absolute()}")

    if args.download_all:
        tables = DOWNLOADABLE
    elif args.tables:
        tables = tuple(dict.fromkeys(args.tables))  # de-dupe, keep order
    else:
        tables = DEFAULT_TABLES

    base_url = args.r2_url.rstrip("/")
    failures = 0
    with _build_client() as client:
        for table in tables:
            status = download_file(client, f"{base_url}/{table}.parquet", output_dir / f"{table}.parquet", args.force)
            if status == "failed":
                failures += 1

    print(f"\nDone! Data saved to: {output_dir.absolute()}")
    return 1 if failures else 0
