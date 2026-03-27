"""Tests for manifest save/load cycle (load.save_manifest + extract.load_manifest)."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
import pytest

from spicy_regs.pipeline.extract import BloomFilter
from spicy_regs.pipeline.load import save_manifest


def _write_manifest(path: Path, keys: list[str]) -> None:
    """Helper to write a manifest Parquet file."""
    schema = pa.schema([("key", pa.large_string())])
    table = pa.table({"key": keys}).cast(schema)
    pq.write_table(table, path, compression="zstd")


def _read_manifest_keys(path: Path) -> set[str]:
    """Helper to read all keys from a manifest file."""
    df = pl.read_parquet(path)
    return set(df["key"].to_list())


class TestSaveManifest:
    def test_creates_new_manifest(self, tmp_output):
        new_keys = {"key-1", "key-2", "key-3"}
        save_manifest(tmp_output, new_keys)

        manifest = tmp_output / "manifest.parquet"
        assert manifest.exists()
        assert _read_manifest_keys(manifest) == new_keys

    def test_appends_to_existing(self, tmp_output):
        existing = ["old-key-1", "old-key-2"]
        _write_manifest(tmp_output / "manifest.parquet", existing)

        new_keys = {"new-key-1", "new-key-2"}
        save_manifest(tmp_output, new_keys)

        all_keys = _read_manifest_keys(tmp_output / "manifest.parquet")
        assert all_keys == set(existing) | new_keys

    def test_preserves_existing_rows(self, tmp_output):
        existing = [f"key-{i}" for i in range(1000)]
        _write_manifest(tmp_output / "manifest.parquet", existing)

        save_manifest(tmp_output, {"new-key"})

        all_keys = _read_manifest_keys(tmp_output / "manifest.parquet")
        assert len(all_keys) == 1001
        assert "new-key" in all_keys
        assert "key-0" in all_keys
        assert "key-999" in all_keys

    def test_no_temp_file_left_behind(self, tmp_output):
        save_manifest(tmp_output, {"key-1"})
        assert not (tmp_output / "manifest_new.parquet").exists()


class TestLoadManifest:
    def test_load_into_bloom_filter(self, tmp_output):
        """load_manifest should populate PROCESSED_KEYS with a BloomFilter."""
        import spicy_regs.pipeline.pipeline as pipeline_mod
        from spicy_regs.pipeline.extract import load_manifest

        keys = [f"raw-data/EPA/text-{i:04d}.json" for i in range(100)]
        _write_manifest(tmp_output / "manifest.parquet", keys)

        # Call the Prefect task function directly
        load_manifest.fn(tmp_output)

        assert isinstance(pipeline_mod.PROCESSED_KEYS, BloomFilter)
        for k in keys:
            assert k in pipeline_mod.PROCESSED_KEYS
        assert "nonexistent-key" not in pipeline_mod.PROCESSED_KEYS

    # def test_load_no_manifest(self, tmp_output):
    #     """load_manifest with no file should leave PROCESSED_KEYS as empty set."""
    #     import spicy_regs.pipeline.pipeline as pipeline_mod
    #     from spicy_regs.pipeline.extract import load_manifest

    #     original = pipeline_mod.PROCESSED_KEYS
    #     pipeline_mod.PROCESSED_KEYS = set()

    #     # No manifest file, no R2 (no env vars set)
    #     load_manifest.fn(tmp_output)

    #     # Should still be the empty set (not a BloomFilter)
    #     assert pipeline_mod.PROCESSED_KEYS == set()
    #     pipeline_mod.PROCESSED_KEYS = original


class TestManifestRoundTrip:
    def test_save_then_load(self, tmp_output):
        """Keys saved via save_manifest should be found via load_manifest."""
        import spicy_regs.pipeline.pipeline as pipeline_mod
        from spicy_regs.pipeline.extract import load_manifest

        keys = {f"raw-data/AGENCY-{i}/text-{j:04d}.json" for i in range(5) for j in range(50)}
        save_manifest(tmp_output, keys)

        load_manifest.fn(tmp_output)
        assert isinstance(pipeline_mod.PROCESSED_KEYS, BloomFilter)

        for k in keys:
            assert k in pipeline_mod.PROCESSED_KEYS

    def test_incremental_save_then_load(self, tmp_output):
        """Multiple save_manifest calls should accumulate keys."""
        import spicy_regs.pipeline.pipeline as pipeline_mod
        from spicy_regs.pipeline.extract import load_manifest

        batch1 = {f"batch1-{i}" for i in range(50)}
        batch2 = {f"batch2-{i}" for i in range(50)}

        save_manifest(tmp_output, batch1)
        save_manifest(tmp_output, batch2)

        load_manifest.fn(tmp_output)

        for k in batch1 | batch2:
            assert k in pipeline_mod.PROCESSED_KEYS
