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

from collections.abc import Iterable
from pathlib import Path
from threading import Lock

import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.pipeline.download_r2 import download_from_r2
from spicy_regs.pipeline.extract import BloomFilter
from spicy_regs.pipeline.load import save_manifest


class Manifest:
    """Tracks processed source keys for incremental ETL runs.

    ``processed`` is anything supporting ``key in processed`` (a Bloom filter
    for a loaded manifest, or a set for the empty/bootstrap case).
    """

    def __init__(self, processed: object) -> None:
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
