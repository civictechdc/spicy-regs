"""Dataset-level (bulk) transforms.

These operate on the *whole* staged dataset at once — merging per-agency Parquet
files, deduplicating by key (keeping the latest ``modify_date``), partitioning
comments, and building the feed summary. They run columnar / out-of-core in
DuckDB, so unlike a record-stream :class:`~spicy_regs.transforms.base.Transform`
they cannot be expressed as ``apply(records) -> records`` without buffering
everything in memory.

They live in the production pipeline (``spicy_regs.pipeline.transform``) and are
re-exported here so every "transform" concept is discoverable from one package:

* per-record shaping  -> :mod:`spicy_regs.transforms.extract` (a ``Transform``)
* whole-dataset bulk  -> this module
"""

from spicy_regs.pipeline.transform import (
    build_feed_summary,
    merge_comments_partitioned,
    merge_staging_files,
    update_comments_index,
)

__all__ = [
    "merge_staging_files",
    "merge_comments_partitioned",
    "update_comments_index",
    "build_feed_summary",
]
