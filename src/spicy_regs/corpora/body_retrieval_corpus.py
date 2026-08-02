"""Build a topically coherent Federal Register corpus that has real bodies.

The corpus this module draws exists because the one we had could not answer a
retrieval question. The offline inventory reachable without a network fetch is
34 documents spanning seven disjoint publisher families; their median pairwise
Jaccard is ~0.13, so every body-answerable query hits its target under any
retrieval configuration and recall@50 over 34 documents is 1.0 by arithmetic
rather than by merit. A corpus whose documents do not compete cannot rank them.

So the draw optimises for *competition*, not for size:

* one regulatory program, not a broad random spread — 50 CFR 17, the Endangered
  Species Act listing and critical-habitat part, whose documents share a deep
  regulatory vocabulary while each names a different species;
* long bodies, because chunking is definitionally a no-op below ~3,000
  characters and the open question is whether BM25 length normalization
  penalises long documents against short metadata-only neighbours;
* measured before it is fetched, because a draw that reproduces the 0.140
  problem is a failed draw and should be discarded at zero network cost.

Three stages, each separately receipted:

``draw``
    Pure offline selection over ``federal_register.parquet``. Emits a
    deterministic ``draw-manifest.json`` carrying the selection rule, the
    documents, and the pre-fetch vocabulary-competition distribution.
``fetch``
    Polite, resumable retrieval of each ``body_html_url``. Every document gets
    its own receipt, so a failure at document 999 does not discard 998
    successes, and failures land in a typed quarantine rather than vanishing.
``validate``
    Re-verifies every cached body against the lock and fails closed.

**The fetch is not reproducible; the lock is.** That is the same boundary
``corpora/segmentation_evaluation.py`` draws, and it is deliberate: bytes on a
public web server change, so the reproducible artifact is the digest record,
not the act of retrieval.

Reuse notes (nothing here re-implements what the repo already had):

* ``FetchResult``/header/timeout policy and the lock-record shape follow
  ``corpora/segmentation_evaluation.py:833-947``.
* Retry with exponential backoff follows ``sources/federal_register.py:175-191``.
* The minimum-interval throttle with an injectable ``sleep`` and a hard request
  budget follows ``RefSpec/src/refspec/registry/icpsr_subject.py:586-651``.
* Typed quarantine rows with per-reason counters follow
  ``tools/build_agency_crosswalk_artifact.py:290-329``.
* ``utf-8-sig`` strict decoding follows ``document_file_pipeline.py:638-660``.

What is genuinely new is the resumable driver loop; the existing
``fetch_source_cache`` is all-or-nothing over a hardcoded 34-spec tuple.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import statistics
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import httpx

from spicy_regs.ontology.common import canonical_json

__all__ = [
    "DRAW_SCHEMA_VERSION",
    "LOCK_FORMAT_VERSION",
    "BodyCorpusError",
    "DrawRule",
    "FetchResult",
    "build_draw",
    "canonical_json",
    "fetch_bodies",
    "jaccard_distribution",
    "median",
    "select_rows",
    "sha256_bytes",
    "tokenize",
    "validate_body_cache",
]

REPO_ROOT = Path(__file__).resolve().parents[3]

DRAW_SCHEMA_VERSION = "body-retrieval-corpus-draw-v1"
LOCK_FORMAT_VERSION = 1

#: Politeness. federalregister.gov publishes no Crawl-delay and returns no
#: rate-limit headers, so we pick a conservative interval rather than probing
#: for the limit. One request per 1.2s is ~50/minute against a public federal
#: index that serves far more than that to ordinary browsers.
DEFAULT_MIN_INTERVAL_SECONDS = 1.2
DEFAULT_MAX_BYTES = 32 * 1024 * 1024
DEFAULT_TIMEOUT = httpx.Timeout(120.0, connect=30.0)
DEFAULT_MAX_RETRIES = 5

#: A contactable agent string. Government hosts 403 bare ``python-httpx/*``
#: (see ``sources/pdf.py:22-27``), and an anonymous crawler on a public index
#: is impolite regardless.
USER_AGENT = "spicy-regs-body-retrieval-corpus/1.0 (+https://github.com/civictechdc/spicy-regs; research corpus build)"

_TOKEN = re.compile(r"[a-z0-9]+")

#: Deliberately small. A large stoplist would flatter the coherence numbers by
#: deleting exactly the shared function words that make two documents look
#: similar; the measurement should survive a minimal one.
_STOPWORDS = frozenset(
    """the a an and or of to in for on by with from as at is are be been was were
    this that these those which shall may must not no any all such other than then
    if it its their there here we you they he she""".split()
)


class BodyCorpusError(RuntimeError):
    """Raised when the corpus cannot be drawn, fetched, or sealed honestly."""


# --------------------------------------------------------------------------
# vocabulary competition
# --------------------------------------------------------------------------


def tokenize(text: str | None) -> set[str]:
    """Lowercase alphanumeric tokens of length >= 3, minus a minimal stoplist."""

    return {token for token in _TOKEN.findall((text or "").lower()) if len(token) >= 3 and token not in _STOPWORDS}


def jaccard_distribution(
    token_sets: Sequence[set[str]],
    *,
    cap: int = 400,
    seed: int = 7,
) -> list[float]:
    """Pairwise Jaccard over ``token_sets``, sampled deterministically.

    Full pairwise cost is quadratic; at ~1000 documents that is ~500k pairs,
    which is affordable, but the sample keeps the measurement cheap enough to
    run on every candidate draw while iterating. ``seed`` makes the sample
    reproducible, so two runs of the same draw report the same distribution.

    Empty sets are skipped rather than scored 0.0: a document with no tokens
    says nothing about vocabulary competition, and counting it as maximally
    dissimilar would understate coherence.
    """

    indexes = list(range(len(token_sets)))
    if len(indexes) > cap:
        indexes = random.Random(seed).sample(indexes, cap)
    values: list[float] = []
    for position, left in enumerate(indexes):
        for right in indexes[position + 1 :]:
            first, second = token_sets[left], token_sets[right]
            if not first or not second:
                continue
            union = len(first | second)
            values.append(len(first & second) / union if union else 0.0)
    return values


def median(values: Sequence[float]) -> float:
    return float(statistics.median(values)) if values else float("nan")


def _quantile(sorted_values: Sequence[float], share: float) -> float | None:
    if not sorted_values:
        return None
    return float(sorted_values[min(len(sorted_values) - 1, int(share * len(sorted_values)))])


def summarize_distribution(values: Sequence[float]) -> dict[str, Any]:
    """Report the shape of a distribution, not just its centre.

    Small measured differences are not automatically noise, so the receipt
    carries the tails as well as the median.

    An empty distribution reports ``None``, never NaN: ``canonical_json`` sets
    ``allow_nan=False`` because ``NaN`` is not JSON any reader accepts, so a
    NaN here would produce a receipt that digests but will not parse. A draw
    of fewer than two documents has no pairs and must say so in JSON.
    """

    ordered = sorted(values)
    if not ordered:
        return dict.fromkeys(("min", "p10", "p25", "median", "p75", "p90", "max", "mean")) | {"pair_count": 0}
    return {
        "pair_count": len(ordered),
        "min": _quantile(ordered, 0.0),
        "p10": _quantile(ordered, 0.10),
        "p25": _quantile(ordered, 0.25),
        "median": median(ordered),
        "p75": _quantile(ordered, 0.75),
        "p90": _quantile(ordered, 0.90),
        "max": float(ordered[-1]),
        "mean": float(statistics.fmean(ordered)),
    }


# --------------------------------------------------------------------------
# selection
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class DrawRule:
    """The whole selection rule, as data, so the receipt can state it."""

    cfr_title: int
    cfr_parts: tuple[str, ...]
    topic_substring: str
    min_pages: int
    min_year: int
    document_types: tuple[str, ...]

    def as_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["cfr_parts"] = list(self.cfr_parts)
        record["document_types"] = list(self.document_types)
        return record


def _text(value: object) -> str:
    return "" if value is None else str(value)


def _page_span(row: Mapping[str, Any]) -> int | None:
    """Inclusive page count. ``end_page`` and ``start_page`` are VARCHAR upstream."""

    try:
        start = int(_text(row.get("start_page")))
        end = int(_text(row.get("end_page")))
    except (TypeError, ValueError):
        return None
    span = end - start + 1
    return span if span > 0 else None


def _cfr_pairs(row: Mapping[str, Any]) -> set[tuple[int, str]]:
    """(title, part) pairs, kept *paired*.

    Matching title and part independently would admit a document that cites
    40 CFR 17 and 50 CFR 52 to a 50 CFR 17 draw.
    """

    try:
        references = json.loads(_text(row.get("cfr_references_json")) or "[]")
    except (TypeError, ValueError):
        return set()
    if not isinstance(references, list):
        return set()
    pairs: set[tuple[int, str]] = set()
    for reference in references:
        if not isinstance(reference, Mapping):
            continue
        title, part = reference.get("title"), reference.get("part")
        if title is None or part is None:
            continue
        try:
            pairs.add((int(title), str(part)))
        except (TypeError, ValueError):
            continue
    return pairs


def _topics(row: Mapping[str, Any]) -> list[str]:
    try:
        topics = json.loads(_text(row.get("topics_json")) or "[]")
    except (TypeError, ValueError):
        return []
    return [str(topic) for topic in topics] if isinstance(topics, list) else []


def _year(row: Mapping[str, Any]) -> int | None:
    try:
        return int(_text(row.get("publication_date"))[:4])
    except (TypeError, ValueError):
        return None


def select_rows(rows: Iterable[Mapping[str, Any]], rule: DrawRule) -> list[dict[str, Any]]:
    """Apply the draw rule and return matching rows ordered by document number.

    Ordering by document number rather than input order makes the draw
    independent of parquet row order, so the manifest is stable.
    """

    wanted_parts = set(rule.cfr_parts)
    needle = rule.topic_substring.casefold()
    selected: list[dict[str, Any]] = []
    for row in rows:
        if _text(row.get("document_type")) not in rule.document_types:
            continue
        if not any(title == rule.cfr_title and part in wanted_parts for title, part in _cfr_pairs(row)):
            continue
        if needle and not any(needle in topic.casefold() for topic in _topics(row)):
            continue
        pages = _page_span(row)
        if pages is None or pages < rule.min_pages:
            continue
        year = _year(row)
        if year is None or year < rule.min_year:
            continue
        selected.append(dict(row))
    selected.sort(key=lambda row: _text(row.get("document_number")))
    return selected


def build_draw(
    rows: Iterable[Mapping[str, Any]],
    *,
    rule: DrawRule,
    source_digest: str,
    jaccard_cap: int = 400,
    jaccard_seed: int = 7,
) -> dict[str, Any]:
    """Select, measure, and return the deterministic draw manifest.

    ``drawn_at`` is deliberately absent: it would change the bytes on every
    run and defeat the byte-identical-rebuild check. Wall-clock lives in the
    receipt instead, the same split ``tools/draw_search_holdout.py`` uses.
    """

    selected = select_rows(rows, rule)
    with_url = [row for row in selected if _text(row.get("body_html_url")).startswith("http")]
    excluded_no_url = len(selected) - len(with_url)

    documents = [
        {
            "document_number": _text(row.get("document_number")),
            "body_html_url": _text(row.get("body_html_url")),
            "document_type": _text(row.get("document_type")),
            "publication_date": _text(row.get("publication_date")),
            "agency_slugs": _text(row.get("agency_slugs")),
            "pages": _page_span(row),
            "title": _text(row.get("title")),
        }
        for row in with_url
    ]

    token_sets = [tokenize(f"{_text(row.get('title'))} {_text(row.get('abstract'))}") for row in with_url]
    page_counts = sorted(document["pages"] or 0 for document in documents)
    agencies = Counter(document["agency_slugs"] for document in documents)
    types = Counter(document["document_type"] for document in documents)
    years = Counter(document["publication_date"][:4] for document in documents)

    manifest: dict[str, Any] = {
        "schema_version": DRAW_SCHEMA_VERSION,
        "selection_rule": rule.as_record(),
        "source_digest": source_digest,
        "documents": documents,
        "counts": {
            "selected": len(documents),
            "excluded_no_body_url": excluded_no_url,
            "by_document_type": dict(sorted(types.items())),
            "by_agency_slugs": dict(sorted(agencies.items())),
            "by_year": dict(sorted(years.items())),
        },
        "page_span": {
            "total": sum(page_counts),
            "min": page_counts[0] if page_counts else None,
            "median": median([float(count) for count in page_counts]) if page_counts else None,
            "p90": _quantile([float(count) for count in page_counts], 0.90),
            "max": page_counts[-1] if page_counts else None,
        },
        "vocabulary_competition": {
            "surface": "title+abstract",
            "tokenizer": "lowercase-alnum-min3-minus-minimal-stoplist",
            "sample_cap": jaccard_cap,
            "sample_seed": jaccard_seed,
            **summarize_distribution(jaccard_distribution(token_sets, cap=jaccard_cap, seed=jaccard_seed)),
        },
    }
    manifest["draw_id"] = (
        "urn:spicyregs:body-retrieval-draw:" + hashlib.sha256(canonical_json(manifest).encode("utf-8")).hexdigest()[:24]
    )
    return manifest


# --------------------------------------------------------------------------
# fetching
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class FetchResult:
    """One HTTP response, reduced to what the lock records."""

    content: bytes
    resolved_url: str
    media_type: str
    status_code: int
    etag: str | None = None
    last_modified: str | None = None


Fetcher = Callable[[str], FetchResult]


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def http_fetch(url: str, *, max_bytes: int = DEFAULT_MAX_BYTES) -> FetchResult:
    """Fetch one body with bounded exponential backoff on 429/5xx.

    Backoff policy copied from ``sources/federal_register.py:175-191`` so the
    two agree about what "retryable" means.
    """

    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml,*/*"}
    last_error: Exception | None = None
    for attempt in range(DEFAULT_MAX_RETRIES):
        try:
            response = httpx.get(url, headers=headers, timeout=DEFAULT_TIMEOUT, follow_redirects=True)
            if response.status_code == 429 or response.status_code >= 500:
                raise BodyCorpusError(f"retryable HTTP {response.status_code}")
            response.raise_for_status()
            if len(response.content) > max_bytes:
                raise BodyCorpusError(f"body exceeds the {max_bytes} byte cap")
            return FetchResult(
                content=response.content,
                resolved_url=str(response.url),
                media_type=response.headers.get("content-type", "").split(";", 1)[0],
                status_code=response.status_code,
                etag=response.headers.get("etag"),
                last_modified=response.headers.get("last-modified"),
            )
        except httpx.HTTPStatusError as error:
            # A 404 is an answer, not a hiccup. Retrying it wastes the budget.
            raise BodyCorpusError(f"HTTP {error.response.status_code}") from error
        except (BodyCorpusError, httpx.HTTPError) as error:
            last_error = error
            if attempt == DEFAULT_MAX_RETRIES - 1:
                break
            time.sleep(min(2**attempt, 30))
    raise BodyCorpusError(f"fetch failed after {DEFAULT_MAX_RETRIES} attempts: {last_error}")


#: Federal Register sits behind Cloudflare. From a reputation-flagged egress a
#: request is 302'd to an interstitial that returns HTTP 200 with HTML. Sealed
#: unchecked, that page digests cleanly, parses into structural passages, and
#: measures as a document -- roughly a thousand identical copies of it. Every
#: retrieval number computed on such a corpus would be describing Cloudflare.
#: This is the one failure mode that corrupts results silently, so it is
#: checked explicitly rather than left to the media-type test.
_INTERSTITIAL_HOSTS = ("unblock.federalregister.gov",)
_INTERSTITIAL_MARKERS = (
    b"cf-browser-verification",
    b"cf_chl_",
    b"Checking your browser before accessing",
    b"Just a moment...",
    b"Attention Required! | Cloudflare",
    b"unblock.federalregister.gov",
)


def _classify_body(payload: bytes, media_type: str, resolved_url: str = "") -> str | None:
    """Return a quarantine reason, or ``None`` when the body is usable.

    Checked before sealing because a corpus of error pages is worse than a
    smaller corpus: the bytes would digest cleanly and measure as documents.
    """

    if not payload.strip():
        return "empty-body"
    if payload.lstrip()[:5].startswith(b"%PDF"):
        return "not-markup"
    if any(host in resolved_url for host in _INTERSTITIAL_HOSTS):
        return "blocked-interstitial"
    window = payload[:8192]
    if any(marker in window for marker in _INTERSTITIAL_MARKERS):
        return "blocked-interstitial"
    if media_type and media_type not in {"", "text/html", "application/xhtml+xml", "text/plain"}:
        return "unexpected-media-type"
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeError:
        return "not-utf8"
    if "<" not in text[:2048] and not text.strip():
        return "empty-body"
    return None


def _receipt_path(cache_dir: Path, document_number: str) -> Path:
    return cache_dir / "receipts" / f"{document_number}.json"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BodyCorpusError(f"invalid JSON object: {path}") from error
    if not isinstance(value, dict):
        raise BodyCorpusError(f"JSON must contain an object: {path}")
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")


def fetch_bodies(
    draw_manifest_path: Path,
    cache_dir: Path,
    *,
    fetcher: Fetcher | None = None,
    sleep: Callable[[float], None] = time.sleep,
    min_interval_seconds: float = DEFAULT_MIN_INTERVAL_SECONDS,
    max_requests: int | None = None,
    stop_at_budget: bool = False,
    retrieved_on: str | None = None,
) -> dict[str, Any]:
    """Fetch every drawn body politely and resumably, then seal the lock.

    Resumability is per document: a body already on disk whose bytes still
    match its receipt is skipped without a request. Quarantined documents are
    *not* skipped — a failure is not an answer, and the next run retries it.

    ``max_requests`` is a hard budget. Exceeding it raises rather than
    continuing, so a loop that misbehaves stops against a public server
    instead of running away; ``stop_at_budget`` turns that into a clean
    partial stop for staged runs.
    """

    manifest = _read_json(Path(draw_manifest_path))
    documents = manifest.get("documents")
    if not isinstance(documents, list):
        raise BodyCorpusError("draw manifest documents must be an array")

    cache_dir = Path(cache_dir)
    (cache_dir / "documents").mkdir(parents=True, exist_ok=True)
    (cache_dir / "receipts").mkdir(parents=True, exist_ok=True)
    load = fetcher or http_fetch
    stamp = retrieved_on or time.strftime("%Y-%m-%d")

    quarantine_rows: list[dict[str, Any]] = []
    reasons: Counter[str] = Counter()
    fetched = skipped = 0
    requests_made = 0

    for entry in documents:
        if not isinstance(entry, Mapping):
            raise BodyCorpusError("draw manifest document entries must be objects")
        number = _text(entry.get("document_number"))
        url = _text(entry.get("body_html_url"))
        cache_file = f"{number}.html"
        body_path = cache_dir / "documents" / cache_file
        receipt_path = _receipt_path(cache_dir, number)

        if receipt_path.is_file() and body_path.is_file():
            receipt = _read_json(receipt_path)
            if receipt.get("status") == "ok" and _sha256_file(body_path) == receipt.get("source_sha256"):
                skipped += 1
                continue

        if max_requests is not None and requests_made >= max_requests:
            if stop_at_budget:
                break
            raise BodyCorpusError(f"request budget exhausted after {requests_made} requests; refusing to continue")
        if requests_made:
            sleep(min_interval_seconds)

        requests_made += 1
        try:
            result = load(url)
        except Exception as error:  # noqa: BLE001 - every failure is quarantined by reason
            reasons["fetch-failed"] += 1
            quarantine_rows.append(
                {
                    "document_number": number,
                    "source_url": url,
                    "reason": "fetch-failed",
                    "detail": f"{type(error).__name__}: {error}",
                }
            )
            receipt_path.unlink(missing_ok=True)
            continue

        reason = _classify_body(result.content, result.media_type, result.resolved_url)
        if reason is not None:
            reasons[reason] += 1
            quarantine_rows.append(
                {
                    "document_number": number,
                    "source_url": url,
                    "reason": reason,
                    "detail": f"{len(result.content)} bytes, media_type={result.media_type!r}",
                }
            )
            receipt_path.unlink(missing_ok=True)
            continue

        text = result.content.decode("utf-8-sig")
        body_path.write_bytes(result.content)
        _write_json(
            receipt_path,
            {
                "status": "ok",
                "document_number": number,
                "cache_file": cache_file,
                "source_url": url,
                "resolved_url": result.resolved_url,
                "media_type": result.media_type or "text/html",
                "retrieved_on": stamp,
                "source_bytes": len(result.content),
                "source_sha256": sha256_bytes(result.content),
                "extracted_chars": len(text),
                "extracted_sha256": sha256_bytes(text.encode("utf-8")),
                "extraction_method": "raw-utf8",
                "extraction_version": "1",
                "etag": result.etag,
                "last_modified": result.last_modified,
            },
        )
        fetched += 1

    lock = _seal_lock(cache_dir, manifest)
    _write_json(
        cache_dir / "quarantine.json",
        {
            "rows": sorted(quarantine_rows, key=lambda row: (row["reason"], row["document_number"])),
            "by_reason": dict(sorted(reasons.items())),
            "total": len(quarantine_rows),
        },
    )
    return {
        "fetched": fetched,
        "skipped_already_present": skipped,
        "quarantined": len(quarantine_rows),
        "requests_made": requests_made,
        "sealed_documents": len(lock["sources"]),
        "quarantine_by_reason": dict(sorted(reasons.items())),
    }


def _seal_lock(cache_dir: Path, manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Assemble the lock from whatever succeeded, in draw order.

    Deriving the lock from per-document receipts rather than from the fetch
    loop is what makes a resumed run produce the same bytes as a single-pass
    run: order comes from the manifest, content from disk.
    """

    records: list[dict[str, Any]] = []
    for entry in manifest.get("documents", []):
        number = _text(entry.get("document_number"))
        receipt_path = _receipt_path(cache_dir, number)
        if not receipt_path.is_file():
            continue
        receipt = _read_json(receipt_path)
        if receipt.get("status") != "ok":
            continue
        records.append({key: value for key, value in receipt.items() if key != "status"})
    lock = {
        "format_version": LOCK_FORMAT_VERSION,
        "draw_id": manifest.get("draw_id"),
        "draw_schema_version": manifest.get("schema_version"),
        "source_count": len(records),
        "sources": records,
    }
    _write_json(cache_dir / "source-lock.json", lock)
    return lock


def validate_body_cache(cache_dir: Path) -> dict[str, Any]:
    """Re-verify every cached body against the lock. Fails closed."""

    cache_dir = Path(cache_dir)
    lock = _read_json(cache_dir / "source-lock.json")
    failures: list[str] = []
    if lock.get("format_version") != LOCK_FORMAT_VERSION:
        failures.append("lock format version does not match")
    sources = lock.get("sources")
    if not isinstance(sources, list):
        return {"status": "fail", "source_count": 0, "failures": ["lock sources must be an array"]}

    for record in sources:
        number = _text(record.get("document_number"))
        path = cache_dir / "documents" / _text(record.get("cache_file"))
        if not path.is_file():
            failures.append(f"{number}: cached body is missing")
            continue
        if _sha256_file(path) != record.get("source_sha256"):
            failures.append(f"{number}: cached body digest does not match the lock")
            continue
        payload = path.read_bytes()
        if len(payload) != record.get("source_bytes"):
            failures.append(f"{number}: cached body byte count differs")
            continue
        try:
            text = payload.decode("utf-8-sig")
        except UnicodeError:
            failures.append(f"{number}: cached body is not UTF-8")
            continue
        if sha256_bytes(text.encode("utf-8")) != record.get("extracted_sha256"):
            failures.append(f"{number}: extracted text digest differs")

    return {
        "status": "pass" if not failures else "fail",
        "source_count": len(sources),
        "failures": failures,
    }


# --------------------------------------------------------------------------
# command line
# --------------------------------------------------------------------------


def _load_federal_register_rows(path: Path) -> list[dict[str, Any]]:
    import polars as pl

    columns = [
        "document_number",
        "title",
        "abstract",
        "document_type",
        "publication_date",
        "agency_slugs",
        "cfr_references_json",
        "topics_json",
        "start_page",
        "end_page",
        "body_html_url",
    ]
    return pl.scan_parquet(path).select(columns).collect().to_dicts()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    draw = commands.add_parser("draw", help="select and measure the corpus offline")
    draw.add_argument("--federal-register", type=Path, required=True)
    draw.add_argument("--output", type=Path, required=True)
    draw.add_argument("--cfr-title", type=int, default=50)
    draw.add_argument("--cfr-parts", default="17")
    draw.add_argument("--topic-substring", default="endangered")
    draw.add_argument("--min-pages", type=int, default=12)
    draw.add_argument("--min-year", type=int, default=2005)

    fetch = commands.add_parser("fetch", help="fetch the drawn bodies politely and resumably")
    fetch.add_argument("--draw", type=Path, required=True)
    fetch.add_argument("--cache-dir", type=Path, required=True)
    fetch.add_argument("--min-interval-seconds", type=float, default=DEFAULT_MIN_INTERVAL_SECONDS)
    fetch.add_argument("--max-requests", type=int, default=None)
    fetch.add_argument("--stop-at-budget", action="store_true")

    validate = commands.add_parser("validate", help="re-verify a fetched cache against its lock")
    validate.add_argument("--cache-dir", type=Path, required=True)

    args = parser.parse_args(argv)

    if args.command == "draw":
        rule = DrawRule(
            cfr_title=args.cfr_title,
            cfr_parts=tuple(part.strip() for part in args.cfr_parts.split(",") if part.strip()),
            topic_substring=args.topic_substring,
            min_pages=args.min_pages,
            min_year=args.min_year,
            document_types=("Rule", "Proposed Rule"),
        )
        source = Path(args.federal_register)
        manifest = build_draw(
            _load_federal_register_rows(source),
            rule=rule,
            source_digest="sha256:" + _sha256_file(source),
        )
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(canonical_json(manifest) + "\n", encoding="utf-8")
        print(
            canonical_json(
                {
                    "draw_id": manifest["draw_id"],
                    "selected": manifest["counts"]["selected"],
                    "median_jaccard": manifest["vocabulary_competition"]["median"],
                    "median_pages": manifest["page_span"]["median"],
                    "output": str(output),
                }
            )
        )
        return 0

    if args.command == "fetch":
        summary = fetch_bodies(
            args.draw,
            args.cache_dir,
            min_interval_seconds=args.min_interval_seconds,
            max_requests=args.max_requests,
            stop_at_budget=args.stop_at_budget,
        )
        print(canonical_json(summary))
        return 0

    report = validate_body_cache(args.cache_dir)
    print(canonical_json(report))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
