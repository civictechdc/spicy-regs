"""Transform: fill comment ``text_content`` from Mirrulations derived-data.

Sits after :class:`~spicy_regs.transforms.extract.ExtractRecords` in the comment
stream. For each flattened comment that has an attachment but no text yet, it
pulls the attachment text Mirrulations already extracted (via
:class:`~spicy_regs.sources.derived_text.DerivedCommentText`) and sets
``text_content`` / ``text_extraction_status``.

This is the *primary* source of comment attachment text: it is free (the same
anonymous S3 bucket the ETL already reads), needs no PDF download or parsing,
and runs inline during staging — so ``comments.parquet`` ships with attachment
text already populated. Comments whose attachment text is absent from
derived-data are left untouched (``text_extraction_status`` stays ``None``) so
the on-demand PDF-download fallback (:mod:`spicy_regs.enrich_pdf`) can
still backfill them.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from spicy_regs.sources.derived_text import DerivedCommentText
from spicy_regs.transforms.base import Transform
from spicy_regs.transforms.pdf_text import PdfTextStatus


class EnrichCommentText(Transform):
    """Fills ``text_content`` from Mirrulations pre-extracted attachment text."""

    def __init__(self, fetcher: DerivedCommentText) -> None:
        self.fetcher = fetcher

    def apply(self, records: Iterable[dict]) -> Iterator[dict]:
        for record in records:
            # Only attachment-bearing comments have extractable text; comments
            # that already carry text (e.g. a re-run) are left as-is.
            if record.get("attachments_json") and record.get("text_content") is None:
                text = self.fetcher.text_for(
                    record.get("agency_code"),
                    record.get("docket_id"),
                    record.get("comment_id"),
                )
                if text:
                    record = {
                        **record,
                        "text_content": text,
                        "text_extraction_status": PdfTextStatus.OK.value,
                    }
            yield record
