# Transforms

Pure data transformations that flow between sources. Transforms have **no
I/O to external systems** — that's a `sources/` concern. Examples of work
that belongs here: deduplication, schema normalization, joining record
streams, aggregation, building summaries.

## Convention

Each transform lives in its own module under this directory. A transform
is a function or small class that takes inputs, returns outputs, and
makes no network calls or filesystem writes outside the working
directory passed to it.

```python
# src/spicy_regs/transforms/dedup.py
from collections.abc import Iterable, Iterator


def dedup_by(records: Iterable[dict], key: str) -> Iterator[dict]:
    seen: set = set()
    for r in records:
        k = r.get(key)
        if k in seen:
            continue
        seen.add(k)
        yield r
```

There is no `Transform` ABC today. The shapes of existing transforms
(records-in, records-out vs. files-in, files-out vs. directory-in,
file-out) are too varied to share one contract. If a common pattern
emerges, a base class can be added here without breaking anything.
