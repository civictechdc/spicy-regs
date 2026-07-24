"""CLI for the Federal Register Thesaurus tag-drift evaluation harness."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.ontology.evaluation import evaluate_tag_quality


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", nargs="?", type=Path, default=Path("output"))
    parser.add_argument(
        "--minimum-f1",
        type=float,
        default=None,
        help="Exit non-zero when the exact-label micro F1 is below this threshold",
    )
    args = parser.parse_args()
    result = evaluate_tag_quality(args.data_dir)
    print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    if args.minimum_f1 is not None and result.f1 < args.minimum_f1:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
