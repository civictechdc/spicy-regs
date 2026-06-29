"""Backward-compatibility shim.

The search-index builder now lives in
:mod:`spicy_regs.transforms.build_search_index`. This module re-exports it so
existing imports (``from spicy_regs.pipeline.build_search_index import ...``)
and ``python -m spicy_regs.pipeline.build_search_index`` keep working while the
legacy ``spicy_regs.pipeline`` package is retired. New code should import from
:mod:`spicy_regs.transforms` directly.
"""

from spicy_regs.transforms.build_search_index import INDEX_FILENAME, build_search_index

__all__ = ["build_search_index", "INDEX_FILENAME"]


if __name__ == "__main__":
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    args = parser.parse_args()
    build_search_index(args.output_dir)
