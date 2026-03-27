"""Tests for the BloomFilter used by the manifest loader."""

import pytest

from spicy_regs.pipeline.extract import BloomFilter


class TestBloomFilter:
    def test_add_and_contains(self):
        bf = BloomFilter(capacity=1000)
        bf.add("raw-data/EPA/text-001.json")
        assert "raw-data/EPA/text-001.json" in bf

    def test_missing_key_not_found(self):
        bf = BloomFilter(capacity=1000)
        bf.add("raw-data/EPA/text-001.json")
        assert "raw-data/FDA/text-999.json" not in bf

    def test_empty_filter_contains_nothing(self):
        bf = BloomFilter(capacity=1000)
        assert "anything" not in bf

    def test_many_keys(self):
        """Insert 10K keys; verify all are found and false-positive rate is low."""
        bf = BloomFilter(capacity=50_000)
        keys = [f"raw-data/AGENCY/text-{i:06d}.json" for i in range(10_000)]
        for k in keys:
            bf.add(k)

        # All inserted keys must be found
        for k in keys:
            assert k in bf, f"Key not found: {k}"

        # Check false-positive rate on 10K absent keys
        absent = [f"raw-data/OTHER/text-{i:06d}.json" for i in range(10_000)]
        false_positives = sum(1 for k in absent if k in bf)
        # With fp_rate=1e-7 and 50K capacity this should be ~0
        assert false_positives < 10, f"Too many false positives: {false_positives}"

    def test_size_bytes_scales_with_capacity(self):
        small = BloomFilter(capacity=1_000)
        large = BloomFilter(capacity=1_000_000)
        assert large.size_bytes > small.size_bytes

    def test_supports_in_operator(self):
        """Verify the bloom filter works with the `in` operator as used by list_json_files."""
        bf = BloomFilter(capacity=100)
        bf.add("key1")
        bf.add("key2")

        # Simulates the pattern: `if processed_keys and key in processed_keys`
        processed_keys = bf
        assert processed_keys and "key1" in processed_keys
        assert processed_keys and "key2" in processed_keys
        assert not ("key3" in processed_keys)
