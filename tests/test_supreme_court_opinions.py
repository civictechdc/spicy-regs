"""Hermetic contracts for official Supreme Court opinion ingestion."""

from __future__ import annotations

from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_regs.sources import r2
from spicy_regs.sources.supreme_court_opinions import (
    SupremeCourtOpinionsReader,
    parse_term_index,
)
from spicy_regs.transforms.build_supreme_court_opinions import (
    _shape,
    build_supreme_court_opinions,
)
from tests.pdf_fixtures import make_pdf

INDEX_HTML = """
<table>
  <tr>
    <td>58</td>
    <td>6/30/23</td>
    <td>21-476</td>
    <td><a href="/opinions/22pdf/600us1r58_7khn.pdf"
      title="The First Amendment limits compelled expression.">
      303 Creative LLC v. Elenis
    </a></td>
    <td>NG</td>
    <td><span>600 U.S. 570</span></td>
  </tr>
</table>
"""


def _reader_record() -> dict:
    pdf = make_pdf(["303 Creative LLC v. Elenis\nOfficial opinion text."])

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/opinions/slipopinion/22":
            return httpx.Response(
                200,
                text=INDEX_HTML,
                headers={"content-type": "text/html"},
                request=request,
            )
        if request.url.path == "/opinions/22pdf/600us1r58_7khn.pdf":
            return httpx.Response(
                200,
                content=pdf,
                headers={
                    "content-type": "application/pdf",
                    "etag": '"fixture-pdf"',
                    "last-modified": "Fri, 30 Jun 2023 12:00:00 GMT",
                },
                request=request,
            )
        return httpx.Response(404, request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        return next(
            SupremeCourtOpinionsReader(
                term_years=(2022,),
                max_records=1,
                client=client,
            ).iter_records()
        )


def test_term_index_uses_official_metadata_and_rejects_unsafe_pdf_urls():
    rows = parse_term_index(INDEX_HTML, term_year=2022)

    assert rows == [
        {
            "term_year": "2022",
            "release_number": "58",
            "date_decided": "2023-06-30",
            "docket_number": "21-476",
            "case_name": "303 Creative LLC v. Elenis",
            "holding": "The First Amendment limits compelled expression.",
            "author_code": "NG",
            "citation": "600 U.S. 570",
            "source_index_url": (
                "https://www.supremecourt.gov/opinions/slipopinion/22"
            ),
            "source_url": (
                "https://www.supremecourt.gov/opinions/22pdf/"
                "600us1r58_7khn.pdf"
            ),
        }
    ]
    with pytest.raises(ValueError, match="unsafe Supreme Court opinion URL"):
        parse_term_index(
            INDEX_HTML.replace(
                "/opinions/22pdf/600us1r58_7khn.pdf",
                "https://example.test/untrusted.pdf",
            ),
            term_year=2022,
        )


def test_reader_and_transform_preserve_identity_digest_and_extracted_text(
    tmp_path: Path,
    monkeypatch,
):
    record = _reader_record()
    row = _shape(record)
    monkeypatch.setattr(r2, "download", lambda *_: False)

    output = build_supreme_court_opinions(
        tmp_path,
        records=[record],
    )
    stored = pq.read_table(output).to_pylist()

    assert row["opinion_id"] == "scotus-2022-58-21-476"
    assert row["pdf_sha256"]
    assert row["pdf_text"] == (
        "303 Creative LLC v. Elenis\nOfficial opinion text."
    )
    assert row["source_etag"] == '"fixture-pdf"'
    assert stored == [row]
