"""Tests for the incremental-processing Manifest (spicy_regs.manifest.Manifest)."""

from pathlib import Path

import pytest

from spicy_regs.manifest import Manifest


def test_empty_manifest_contains_nothing() -> None:
    manifest = Manifest.empty()
    assert "any-key" not in manifest
    assert manifest.new_keys == set()


def test_record_is_additive_and_returns_a_copy() -> None:
    manifest = Manifest.empty()
    manifest.record(["a", "b"])
    manifest.record(["b", "c"])
    assert manifest.new_keys == {"a", "b", "c"}

    # new_keys returns a copy — mutating it must not affect the manifest.
    snapshot = manifest.new_keys
    snapshot.add("zzz")
    assert "zzz" not in manifest.new_keys


def test_save_then_load_roundtrips_keys(tmp_output: Path) -> None:
    manifest = Manifest.empty()
    keys = {"raw-data/EPA/a.json", "raw-data/EPA/b.json"}
    manifest.record(keys)
    manifest.save(tmp_output)
    assert (tmp_output / "manifest.parquet").exists()

    reloaded = Manifest.load(tmp_output)
    for key in keys:
        assert key in reloaded
    assert "raw-data/EPA/never.json" not in reloaded


def test_load_with_no_manifest_is_empty(tmp_output: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # No local manifest and R2 not configured -> empty (fail-soft bootstrap).
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    assert "anything" not in Manifest.load(tmp_output)


def test_empty_save_is_noop(tmp_output: Path) -> None:
    Manifest.empty().save(tmp_output)
    assert not (tmp_output / "manifest.parquet").exists()


def test_save_failed_keys_writes_parquet(tmp_output: Path) -> None:
    import polars as pl

    from spicy_regs.manifest import save_failed_keys

    save_failed_keys(tmp_output, ["t1", "t2"], ["p1"])
    failed_file = tmp_output / "failed_keys.parquet"
    assert failed_file.exists()

    df = pl.read_parquet(failed_file)
    assert set(df["key"].to_list()) == {"t1", "t2", "p1"}
    kinds = dict(zip(df["key"].to_list(), df["kind"].to_list()))
    assert kinds == {"t1": "transient", "t2": "transient", "p1": "parse"}
    assert df["run_at"].n_unique() == 1  # one timestamp for the whole run

    # A clean run removes any stale file so its presence signals "last run failed".
    save_failed_keys(tmp_output, [], [])
    assert not failed_file.exists()
