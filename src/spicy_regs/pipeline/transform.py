"""Backward-compatibility shim.

These dataset-level (bulk) transforms now live in
:mod:`spicy_regs.transforms.merge`. This module re-exports them so existing
imports (``from spicy_regs.pipeline.transform import ...``) keep working while
the legacy ``spicy_regs.pipeline`` package is retired. New code should import
from :mod:`spicy_regs.transforms.merge` directly.
"""

from spicy_regs.transforms.merge import (
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
