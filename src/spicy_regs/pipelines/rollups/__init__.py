"""Decoupled, per-rollup pipelines (one artifact each, own cron workflow)."""

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app

__all__ = ["RollupPipeline", "make_rollup_app"]
