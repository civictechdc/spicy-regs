"""Reader-side helper for the Mirrulations *derived-data* extracted text.

regulations.gov comments frequently carry their substance in an attachment
("See attached file(s)") rather than the inline ``comment`` field. Mirrulations
already runs text extraction over every attachment and publishes the result to
the same public S3 bucket the ETL reads, under the ``derived-data`` prefix::

    derived-data/<agency>/<docket>/mirrulations/extracted_txt/
        comments_extracted_text/<tool>/<comment_id>_attachment_<n>_extracted.txt

This module reads that pre-extracted text so the pipeline can fill a comment's
``text_content`` straight from S3 — no PDF download, no local parsing. The
extraction ``<tool>`` (``pypdf``, ``pdfminer``, ...) varies by docket, so it is
discovered by listing rather than hardcoded; a comment with several attachments
has one ``_attachment_<n>_`` file each, concatenated in attachment order.

Network access lives here in ``sources`` (a Reader concern); the
:class:`~spicy_regs.transforms.enrich_derived_text.EnrichCommentText` transform
that uses it stays a thin stream-mapper.
"""

from __future__ import annotations

import re
from typing import Any

from loguru import logger

BUCKET = "mirrulations"
DERIVED_PREFIX = "derived-data"

# Multiple attachments on one comment are joined with a blank line — the same
# separator the PDF-text path uses (``transforms.pdf_text.PAGE_SEPARATOR``) so
# stored ``text_content`` reads consistently regardless of which source filled
# it. Defined locally to keep ``sources`` independent of ``transforms``.
PART_SEPARATOR = "\n\n"

# <comment_id>_attachment_<n>_extracted.txt — comment ids never contain the
# literal "_attachment_", so a greedy id capture up to the last such marker is
# unambiguous.
_EXTRACTED_RE = re.compile(r"(?P<comment_id>.+)_attachment_\d+_extracted\.txt$")


def comments_extracted_prefix(agency: str, docket_id: str) -> str:
    """S3 prefix holding one docket's comment-attachment extractions (all tools)."""
    return (
        f"{DERIVED_PREFIX}/{agency}/{docket_id}/mirrulations/"
        f"extracted_txt/comments_extracted_text/"
    )


class DerivedCommentText:
    """Fetches Mirrulations pre-extracted comment-attachment text from S3.

    Each docket's extraction prefix is listed once (lazily, on first access) and
    cached as a ``comment_id -> [object keys]`` map, so enriching a whole
    docket's comments costs one ``list_objects`` plus one ``GET`` per attachment.
    Build one per worker thread — the listing cache is plain dict state and is
    not shared-safe across threads.
    """

    def __init__(self, s3_resource: Any, bucket: str = BUCKET) -> None:
        self._resource = s3_resource
        self._bucket_name = bucket
        self._bucket = s3_resource.Bucket(bucket)
        self._docket_index: dict[str, dict[str, list[str]]] = {}

    def _index_for(self, agency: str, docket_id: str) -> dict[str, list[str]]:
        """Lazily build + cache the ``comment_id -> [keys]`` map for one docket."""
        cached = self._docket_index.get(docket_id)
        if cached is not None:
            return cached

        index: dict[str, list[str]] = {}
        prefix = comments_extracted_prefix(agency, docket_id)
        try:
            for obj in self._bucket.objects.filter(Prefix=prefix):
                match = _EXTRACTED_RE.search(obj.key.rsplit("/", 1)[-1])
                if match:
                    index.setdefault(match.group("comment_id"), []).append(obj.key)
        except Exception as exc:  # noqa: BLE001 — listing failure must not abort staging
            logger.warning("derived-data listing failed for {}: {}", prefix, exc)

        for keys in index.values():
            keys.sort()  # attachment_1 before attachment_2, ...
        self._docket_index[docket_id] = index
        return index

    def text_for(
        self, agency: str | None, docket_id: str | None, comment_id: str | None
    ) -> str | None:
        """Concatenated extracted text for one comment, or ``None`` if none exists."""
        if not (agency and docket_id and comment_id):
            return None

        keys = self._index_for(agency, docket_id).get(comment_id)
        if not keys:
            return None

        parts: list[str] = []
        for key in keys:
            try:
                body = self._resource.Object(self._bucket_name, key).get()["Body"].read()
            except Exception as exc:  # noqa: BLE001 — one bad object shouldn't sink the rest
                logger.warning("derived-data read failed for {}: {}", key, exc)
                continue
            text = body.decode("utf-8", "replace") if isinstance(body, bytes) else str(body)
            text = text.strip()
            if text:
                parts.append(text)

        return PART_SEPARATOR.join(parts) if parts else None
