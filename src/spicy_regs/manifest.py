"""Incremental-processing manifest: which source keys have we already seen?

A :class:`Manifest` is the one piece of state that makes the ETL incremental.
It plays two composable roles:

* **Membership test** for a :class:`~spicy_regs.sources.base.Reader` — passed as
  ``processed_keys``, it answers ``key in manifest`` so already-processed files
  are skipped.
* **Accumulator** — :meth:`record` collects the keys seen this run (thread-safe,
  so agencies can be processed in parallel) and :meth:`save` appends them back to
  the persisted manifest.

Membership is backed by a Bloom filter loaded from ``manifest.parquet`` (fetched
from R2 when not present locally). A false positive only means a genuinely new
file is skipped this run and picked up on the next one — never data loss.
"""

from array import array
from collections.abc import Container, Iterable
from hashlib import md5, sha1
from math import log
from pathlib import Path
from threading import Lock

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources.r2 import download_from_r2


# ---------------------------------------------------------------------------
# Bloom filter — stdlib-only, ~34 MB for 30M keys at 1e-7 FP rate.
#
# A false positive only means we skip a file that was actually new; the
# next run will pick it up. This replaces a Python set that consumed
# ~5 GB for 27M strings.
# ---------------------------------------------------------------------------

class BloomFilter:
    """Memory-efficient probabilistic set membership using a bit array."""

    __slots__ = ("_bits", "_nbits", "_k")

    def __init__(self, capacity: int, fp_rate: float = 1e-7) -> None:
        self._nbits = max(1, int(-capacity * log(fp_rate) / (log(2) ** 2)))
        self._k = max(1, int((self._nbits / capacity) * log(2)))
        # 'L' = unsigned long (4 bytes each)
        self._bits = array("L", [0]) * (self._nbits // 32 + 1)

    def _hashes(self, key: str) -> list[int]:
        kb = key.encode()
        h1 = int.from_bytes(md5(kb).digest()[:8], "little")
        h2 = int.from_bytes(sha1(kb).digest()[:8], "little")
        return [(h1 + i * h2) % self._nbits for i in range(self._k)]

    def add(self, key: str) -> None:
        for pos in self._hashes(key):
            self._bits[pos >> 5] |= 1 << (pos & 31)

    def __contains__(self, key: str) -> bool:
        for pos in self._hashes(key):
            if not (self._bits[pos >> 5] & (1 << (pos & 31))):
                return False
        return True

    @property
    def size_bytes(self) -> int:
        return len(self._bits) * 4


def save_manifest(output_dir: Path, new_keys: set[str]) -> None:
    """Append new keys to the existing manifest Parquet file.

    Reads the old manifest (if any) in streaming batches, writes those
    plus the new keys to a temp file, then replaces the original.
    This avoids loading the full 27M-key manifest into memory.
    """
    manifest_file = output_dir / "manifest.parquet"
    temp_file = output_dir / "manifest_new.parquet"
    schema = pa.schema([("key", pa.large_string())])

    existing_rows = 0
    with pq.ParquetWriter(temp_file, schema, compression="zstd") as writer:
        # Stream existing manifest rows
        if manifest_file.exists():
            pf = pq.ParquetFile(manifest_file)
            for batch in pf.iter_batches(batch_size=500_000, columns=["key"]):
                table = pa.Table.from_batches([batch]).cast(schema)
                writer.write_table(table)
                existing_rows += batch.num_rows

        # Append new keys
        new_table = pa.table({"key": list(new_keys)}).cast(schema)
        writer.write_table(new_table)

    if manifest_file.exists():
        manifest_file.unlink()
    temp_file.rename(manifest_file)

    total = existing_rows + len(new_keys)
    logger.info("Saved manifest: {:,} keys ({:,} existing + {:,} new)", total, existing_rows, len(new_keys))


class Manifest:
    """Tracks processed source keys for incremental ETL runs.

    ``processed`` is anything supporting ``key in processed`` (a Bloom filter
    for a loaded manifest, or a set for the empty/bootstrap case).
    """

    def __init__(self, processed: Container[str]) -> None:
        self._processed = processed
        self._new_keys: set[str] = set()
        self._lock = Lock()

    @classmethod
    def empty(cls) -> "Manifest":
        """A manifest that has seen nothing — every key is treated as new."""
        return cls(set())

    @classmethod
    def load(cls, output_dir: Path) -> "Manifest":
        """Build from ``manifest.parquet`` (fetched from R2 if not local).

        Falls back to an empty manifest when none is available (first run, or
        R2 not configured), so callers never special-case bootstrapping.
        """
        manifest_file = output_dir / "manifest.parquet"
        if not manifest_file.exists() and not download_from_r2("manifest.parquet", manifest_file):
            logger.info("No manifest found — starting fresh")
            return cls.empty()

        pf = pq.ParquetFile(manifest_file)
        key_count = pf.metadata.num_rows
        # Size the filter to the keys actually loaded (with modest headroom);
        # new keys from this run are tracked separately in ``_new_keys``.
        bloom = BloomFilter(capacity=max(key_count + key_count // 10, 1000))
        for batch in pf.iter_batches(batch_size=500_000, columns=["key"]):
            for key in batch.column("key").to_pylist():
                bloom.add(key)
        logger.info("Loaded manifest: {:,} keys (~{:.0f} MB)", key_count, bloom.size_bytes / 1_048_576)
        return cls(bloom)

    def __contains__(self, key: str) -> bool:
        return key in self._processed

    def record(self, keys: Iterable[str]) -> None:
        """Mark ``keys`` as processed this run (thread-safe)."""
        with self._lock:
            self._new_keys.update(keys)

    @property
    def new_keys(self) -> set[str]:
        """Keys recorded during this run (a copy, safe to iterate)."""
        with self._lock:
            return set(self._new_keys)

    def save(self, output_dir: Path) -> None:
        """Append the keys recorded this run to the persisted manifest."""
        if self._new_keys:
            save_manifest(output_dir, self._new_keys)
