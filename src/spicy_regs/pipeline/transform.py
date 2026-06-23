"""Backward-compatibility shim.

These dataset-level (bulk) transforms now live in the
:mod:`spicy_regs.transforms` package — one module per primary function. This
module re-exports them so existing imports
(``from spicy_regs.pipeline.transform import ...``) keep working while the
legacy ``spicy_regs.pipeline`` package is retired. New code should import from
:mod:`spicy_regs.transforms` directly.
"""

from spicy_regs.transforms import (
    build_agency_rollups,
    build_feed_summary,
    merge_comments_partitioned,
    merge_staging_files,
    partition_comments,
    update_comments_index,
    write_staging,
)

__all__ = [
    "write_staging",
    "merge_staging_files",
    "merge_comments_partitioned",
    "update_comments_index",
    "partition_comments",
    "build_feed_summary",
    "build_agency_rollups",
]
