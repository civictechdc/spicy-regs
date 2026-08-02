# Structured text extraction — buy vs build, 2026-08-02

**Verdict: keep what we have, everywhere. Adopt nothing for HTML, XML, or PDF.
The one library worth adopting is Chonkie, behind the segmentation interface
only, and only when there is a retrieval reason to move segment boundaries —
which there is not today.**

The PDF arm was expected to be where a modern tool wins, and it is the arm that
most changed my mind: pypdf recovers **as much text as pdfplumber, PyMuPDF, or
PyMuPDF4LLM**, within 2% across 18 real documents. The case for replacing it is
provenance alone, and the platform's contract does not need that provenance
today.

This is a **reversibility** evaluation, not a quality evaluation. The platform's
binding requirement is that an extracted span maps back to exact source
codepoints, because `check_region_coordinates` proves `region.text ==
field_text[start:end]` before an artifact is returned
(`src/spicy_regs/docpipeline/source.py`). A library that returns a *string*
rather than *spans* has already failed that requirement, however good the string
is. Most of this document is the measurement of how badly, and on what.

Nothing was wired. No dependency was added or removed. `pyproject.toml` is
unchanged.

## The single result that decides it

| | returns | `unit_exact` | `anchor_tri` | worst document |
|---|---|---:|---:|---:|
| **incumbent** (`native_structural_passage_spans`) | **spans** | **1.000** | **1.000** | **1.000** |
| every third-party HTML extractor measured | a string | 0.000 | 0.895–0.926 | 0.333 |

`unit_exact` is the fraction of emitted units that occur verbatim in the raw
source. `anchor_tri` is the fraction of three-token windows — carrying the
whitespace *between* the tokens — that occur verbatim in the raw source. A
citation resolves only when the answer is yes.

The incumbent scores 1.000 on 199 Federal Register documents, 318 Federal
Register XML documents, and 7 U.S. Code titles because its output *is* raw source slices; the property is
proven per document, not sampled to it. Every third-party extractor scores
0.000 on `unit_exact` because it emits one decoded string, and ~0.90 on
`anchor_tri` because ~10% of its three-token windows do not exist in the source
it came from.

That ~10% is not a defect in any library. It is what "return the decoded text"
means, and it is unfixable without changing what the library returns.

## Why ~10% of windows cannot be found, measured

Over 10 documents, 7,987 three-token windows of `lxml.html.text_content()`
output probed against their own source:

| cause | share of the 9.5% that fail |
|---|---:|
| the window spans an element boundary (markup or indentation between the two text nodes) | 71.7% |
| the window contains a character the source spelled as an entity reference | 15.9% |
| other cross-node joins | 12.5% |

The corpus-wide entity numbers, measured over all 993 documents:

| | count | per document |
|---|---:|---:|
| entity references, all kinds | 402,508 | 405.3 mean |
| `&#8203;` zero-width space | 10,114 | 10.2 mean, 6 median, 143 max |

`&#8203;` appears in 815 of 993 documents (82.1%), and 98.2% of occurrences are
inside URL tokens — which corroborates the correction committed the same day in
`docs/evidence/body-retrieval-corpus-2026-08-02.md` at a larger sample (993
documents rather than 56). That correction withdrew a "~42 per document" figure;
the full-corpus mean is **10.2**, and the brief that commissioned this document
repeated the withdrawn figure. It is withdrawn here too.

But the entity story is the *minority* cause. Even a corpus with zero entities
would lose ~8% of windows to element-boundary joins alone. **The reversibility
problem is structural to string-returning extractors, not specific to Federal
Register HTML.**

## Identity

| Surface | Value |
|---|---|
| schema_version | `extraction-bakeoff-v1` |
| FR HTML corpus, 993 documents | `sha256:8d2ac3524284dcacfbfd930c696ba04632db235bdf79884bf1ad66042dccdc83` |
| FR HTML metrics subset, 199 documents (stride 5, no RNG) | `sha256:07b91dde3c779dc339631b7ad841b112448dbfffd9a70afc148c11f0ce4f4878` |
| driver `tools/run_extraction_bakeoff.py` | `sha256:9661fe7ba6627205a4b0deeab3c9e6a0018a3ac67e8f85aab6920464aa2cc5e6` |
| worker `tools/extraction_bakeoff_worker.py` | `sha256:9d1e2e4162ab52059ab976a4c29eadb4e4ec29b11f1207bcca30fe44afb36c7f` |
| tests `tests/test_extraction_bakeoff.py` | `sha256:cd708cdd699398374df4b7833582b22a760a1539baad055ad3b226ba0eb1bd8e` (15 tests) |

Corpora, all already in the tree or already fetched — nothing new was downloaded:

| Format | Location | Scale |
|---|---|---|
| Federal Register HTML | `output/body-retrieval-corpus-2026-08-02/cache/documents/` | 993 files, 290 MB |
| Federal Register full-text XML | `output/body-retrieval-corpus-2026-08-02/cache-xml/documents/` | 318 files, `sha256:01b8430c…` |
| USLM / U.S. Code XML | `/private/tmp/uscall.zip` (109 MB), 7 small titles + Title 10 (55 MB) | 58 titles; subsets `sha256:719322bd…` and `sha256:c23322e6…` |
| PDFs | `output/segmentation-source-cache-v2/*.pdf` | 18 files, `sha256:6a816b04…` |

The 1.47 GB figure in the brief is the whole release directory
(`output/body-retrieval-corpus-2026-08-02/` is 1.7 GB). The HTML bodies the
extractors actually parse are **290 MB**. Reported as measured.

## The exact commands

```sh
# Federal Register HTML — metrics, 199-document stride subset
.venv/bin/python tools/run_extraction_bakeoff.py \
    --media text/html --files /tmp/xbake/fr-html-199.txt \
    --candidates incumbent,incumbent_visible,lxml,lxml_structural,bs4,selectolax,\
html_text,html_text_raw,inscriptis,resiliparse,resiliparse_main,unstructured \
    --output /tmp/xbake/results --label fr-html-199

# Federal Register HTML — throughput at full corpus
.venv/bin/python tools/run_extraction_bakeoff.py \
    --media text/html --files /tmp/xbake/fr-html-993.txt \
    --candidates incumbent,lxml,selectolax,resiliparse,inscriptis,html_text \
    --output /tmp/xbake/results --label fr-html-993 --no-full-metrics

# Federal Register full-text XML (the listing holds 318 documents; the label is
# the arm name, not the count)
.venv/bin/python tools/run_extraction_bakeoff.py \
    --media application/xml --files /tmp/xbake/fr-xml-306.txt \
    --candidates incumbent,incumbent_visible,lxml,lxml_structural,bs4,selectolax,\
inscriptis,resiliparse,unstructured_xml \
    --output /tmp/xbake/results --label fr-xml-306

# USLM XML — 7 small titles, then one 55 MB title for scale
.venv/bin/python tools/run_extraction_bakeoff.py \
    --media application/xml --files /tmp/xbake/uslm.txt \
    --candidates incumbent,incumbent_visible,lxml,lxml_structural,bs4,selectolax,\
inscriptis,resiliparse,unstructured_xml \
    --output /tmp/xbake/results --label uslm-7

.venv/bin/python tools/run_extraction_bakeoff.py \
    --media application/xml --files /tmp/xbake/uslm-big.txt \
    --candidates incumbent,lxml,lxml_structural,selectolax \
    --output /tmp/xbake/results --label uslm-big --no-full-metrics

# PDFs
.venv/bin/python tools/run_extraction_bakeoff.py \
    --media application/pdf --files /tmp/xbake/pdf.txt \
    --candidates pypdf,pdfplumber,pymupdf,pymupdf4llm,unstructured_pdf \
    --output /tmp/xbake/results --label pdf-18 --no-full-metrics
```

The scratch venv is rebuilt with:

```sh
uv venv --python 3.12 /tmp/xbake/venv
uv pip install --python /tmp/xbake/venv/bin/python \
    selectolax html-text inscriptis lxml beautifulsoup4 resiliparse \
    pypdf pdfplumber pymupdf pymupdf4llm chonkie semchunk \
    langchain-text-splitters unstructured
```

The incumbent arm was re-run after the harness was reformatted and reproduced
byte-identically: `corpus_digest 07b91dde…`, `run_digest 9536d40a21bd4771`,
`unit_exact 1.0`, `anchor_tri 1.0`, 727.8 structural passages per document. The
numbers below are reproducible from this tree, not only from the tree they were
first measured in.

Every candidate runs **twice, in two separate processes**, and the two per-file
digest lists are compared. Separate processes is the point: a same-process rerun
cannot catch output that depends on `PYTHONHASHSEED`, and this project has
already lost a day to a drift of exactly that shape
(`docs/evidence/document-segmentation-remeasurement-2026-08-02.md`).

## Pinned versions

Candidates live in a throwaway venv at `/tmp/xbake/venv` built by `uv venv`, out
of process, so an unadopted candidate never becomes a project dependency by
accident. The receipt pins what was **imported**, not what was requested.

| Component | Pin |
|---|---|
| repo Python / scratch Python | 3.12.9 / 3.12.9 |
| lxml | 6.1.1 (in the project env) |
| beautifulsoup4 | 4.15.0 (in the project env) |
| selectolax | 0.4.11 |
| html-text | 0.7.1 |
| inscriptis | 2.7.3 |
| resiliparse | 1.0.9 |
| unstructured | 0.25.0 |
| pypdf | 6.14.2 (the incumbent, in the project env) |
| pdfplumber | 0.11.10 |
| PyMuPDF / PyMuPDF4LLM | 1.28.0 / 1.28.0 |
| chonkie | 1.7.0 |
| semchunk | 4.1.1 |
| langchain-text-splitters | 1.1.2 |
| docling | **2.115.0** (the repo's pin) **and 2.117.0** (current, installed unpinned for the fairness check) |
| incumbent | `src/spicy_regs/docpipeline/source.py` at `f425365` |

## Federal Register HTML — 199 documents, full metrics

| candidate | `unit_exact` | `anchor_tri` | tri min | deletion | insertion | structure/doc | s/doc | peak RSS | deterministic |
|---|---:|---:|---:|---:|---:|---:|---:|---:|:--:|
| **incumbent** | **1.000** | **1.000** | **1.000** | *n/a †* | *n/a †* | **727.8** | 0.044 | 129 MB | yes |
| incumbent_visible | 0.627 | 0.941 | 0.512 | 0.0016 | 0.000 | 727.8 | 0.052 | 120 MB | yes |
| lxml `text_content()` | 0.000 | 0.899 | 0.333 | 0.000 | 0.000 | — | 0.0014 | 118 MB | yes |
| lxml + hand-written walk | 0.574 | 0.940 | 0.811 | 0.0021 | 0.000 | 758.1 | 0.0062 | 107 MB | yes |
| BeautifulSoup `get_text()` | 0.000 | 0.899 | 0.333 | 0.000 | 0.000 | — | 0.025 | 142 MB | yes |
| selectolax (Lexbor) | 0.000 | 0.899 | 0.333 | 0.000 | 0.000 | — | 0.0021 | 104 MB | yes |
| html-text | 0.000 | 0.898 | 0.384 | 0.0034 | 0.0023 | — | 0.0096 | 131 MB | yes |
| html-text, `guess_layout=False` | 0.000 | 0.898 | 0.384 | 0.0034 | 0.0023 | — | 0.010 | 125 MB | yes |
| inscriptis | 0.000 | 0.895 | 0.360 | 0.00008 | 0.0009 | — | 0.014 | 119 MB | yes |
| resiliparse | 0.000 | 0.895 | 0.353 | 0.000 | 0.0007 | — | 0.0019 | 129 MB | yes |
| resiliparse `main_content=True` | 0.000 | 0.897 | 0.340 | 0.0071 | 0.0003 | — | 0.0032 | 118 MB | yes |
| unstructured `partition_html` | 0.500 | 0.926 | 0.410 | 0.0019 | 0.0003 | 73.1 | **3.106** | **890 MB** | yes |
| **docling 2.115.0 / 2.117.0** | **0.000** | **0.017–0.447** | — | **0.993–0.998** | 0.24–0.77 | **0** | 1.2 | 488 MB | yes |

† The incumbent emits **raw markup slices**, and the deletion reference is
**decoded text**, so comparing them measures a units mismatch, not a loss. Run
against the same reference after decoding the same spans, the incumbent's real
deletion is **0.15%** (raw-slice comparison reports 4.27%) — and that 0.15% is
deliberate: `script`, `style`, `hidden`, `aria-hidden` content. The
`incumbent_visible` row is the honest loss row.

**Everything was deterministic.** Twelve candidates, two processes each, every
per-file digest list identical. Determinism did not discriminate here — an
honest negative, and worth recording so the next survey does not re-litigate it.

**Nothing lost a Federal Register section header.** All 987 available
`SUMMARY:` / `DATES:` / `ADDRESSES:` / `FOR FURTHER INFORMATION CONTACT:` /
`SUPPLEMENTARY INFORMATION:` markers survived in every candidate except Docling.
The trafilatura failure did **not** reproduce in this generation of
boilerplate-removing extractors — including `resiliparse main_content=True`,
which deletes 0.71% of tokens but kept all 987 markers. That is a real update to
the prior survey's finding and it should not be overstated in either direction:
these tools are much safer than trafilatura was, and still cannot carry an
offset.

**lxml and selectolax produced byte-identical output** on all 199 documents and
again on all 993 (`run_digest` `2d85ef99…` and `10dca376…` respectively). Two
independent HTML5 engines agreeing exactly is a useful cross-check that the
harness is measuring the libraries rather than itself.

## Throughput at real scale — 993 documents, 290 MB

| candidate | total | per document | peak RSS |
|---|---:|---:|---:|
| incumbent | **39.0 s** | 39 ms | 95 MB |
| lxml | 1.5 s | 1.2 ms | 78 MB |
| selectolax | 2.1 s | 1.8 ms | 105 MB |
| resiliparse | 1.8 s | 1.6 ms | 123 MB |
| html-text | 9.2 s | 8.8 ms | 109 MB |
| inscriptis | 12.9 s | 12.7 ms | 96 MB |
| unstructured (extrapolated from 3.106 s/doc) | **~51 min** | 3,106 ms | 890 MB |

The incumbent is 26× slower than raw lxml and processes the entire corpus in
**39 seconds at 95 MB**. Extraction is not a bottleneck at this scale and no
speed argument for switching survives contact with this table. `unstructured` is
79× slower than the incumbent, uses 7× the memory, finds 73 structural units
where the incumbent finds 728, and still cannot return an offset.

## Federal Register full-text XML — 318 documents

Corpus digest
`sha256:01b8430c9c0440464c32826c20d2fd83bbe37054bf193b2ad691e78780aa5d8f`.

| candidate | `unit_exact` | `anchor_tri` | tri min | deletion | insertion | structure/doc | s/doc | peak RSS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **incumbent** | **1.000** | **1.000** | **1.000** | *n/a †* | *n/a †* | **392.2** | 0.025 | 182 MB |
| incumbent_visible | 0.374 | 0.948 | 0.782 | **0.000003** | 0.000 | 392.2 | 0.033 | 135 MB |
| lxml `itertext()` | 0.000 | 0.927 | 0.738 | 0.000 | 0.000 | — | 0.0012 | 165 MB |
| lxml + element walk | 0.706 | 0.933 | 0.724 | 0.000 | **0.693** | 1486.9 | 0.0037 | 339 MB |
| BeautifulSoup (`xml`) | 0.000 | 0.927 | 0.738 | 0.000 | 0.000 | — | 0.023 | 177 MB |
| selectolax | 0.000 | 0.927 | 0.738 | 0.000 | 0.000 | — | 0.0019 | 198 MB |
| inscriptis | 0.000 | 0.926 | 0.770 | 0.0003 | 0.0003 | — | 0.012 | 133 MB |
| resiliparse | 0.000 | 0.927 | 0.767 | 0.000 | 0.0005 | — | 0.0020 | 184 MB |
| unstructured `partition_xml` | 0.998 | 0.9998 | 0.987 | **0.377** | 0.0002 | 567.2 | 3.137 | 497 MB |
| **docling** | — | — | — | — | — | — | **refused** | — |

The XML rendition is *better* for the incumbent than HTML — `incumbent_visible`
deletes **0.000003** of the reference here, against 0.0016 on HTML, because the
XML carries no hidden navigation chrome to exclude. That is independent support
for the XML-rendition work already in flight, arrived at from a different
direction.

`unstructured` is the trap in this table. It scores `unit_exact` 0.998 and
`anchor_tri` 0.9998 — better than anything except the incumbent, because XML
text nodes really are verbatim — and it **deletes 37.7% of every document** while
doing so. A reversibility metric alone would have recommended it. This is why
deletion is measured.

The naive "just walk the tree yourself" arm is the cautionary row: iterating
every element re-emits every ancestor's text, so **69% of its output is
duplicated text**. Building a structural walk is not free, and the incumbent's
`_markup_drafts` already solves this by taking gap-free sibling ranges instead of
nested subtrees.

The GPO FR-XML vocabulary confirmed against the real files: `<PREAMB>`, `<HD>`,
`<SUM>`, `<SUPLINF>`, `<PRTPAGE>`, `<AGENCY>`, `<CFR>`, `<RIN>`, `<SUBJECT>`,
`<P>` — and `<SECTION>` **is** present in these documents, contrary to a
documentation-only reading that placed `SECTION` in the eCFR schema alone. The
structure is stated. Nothing needs to infer it.

## USLM / U.S. Code XML — 7 titles, and a second trafilatura-class failure

Corpus digest `sha256:719322bd9f37208e…`, titles 1, 3, 4, 9, 13, 24, 27 from the
release-point zip (0.1–0.9 MB each).

| candidate | `unit_exact` | `anchor_tri` | deletion | insertion | structure/doc | s/doc |
|---|---:|---:|---:|---:|---:|---:|
| **incumbent** | **1.000** | **1.000** | *n/a †* | *n/a †* | **857.9** | 0.103 |
| incumbent_visible | 0.164 | 0.805 | **0.0082** | 0.016 | 857.9 | 0.118 |
| lxml `itertext()` | 0.000 | 0.754 | 0.000 | 0.000 | — | 0.0059 |
| lxml + element walk | 0.673 | 0.799 | 0.000 | **0.883** | 4402.6 | 0.016 |
| BeautifulSoup (`xml`) | 0.000 | 0.754 | 0.000 | 0.000 | — | 0.049 |
| selectolax | 0.000 | 0.997 | **0.146** | 0.328 | — | 0.0034 |
| resiliparse | 0.000 | 0.914 | **0.146** | 0.328 | — | 0.0054 |
| **inscriptis** | 0.000 | **0.000** | **0.9995** | 0.000 | — | 0.0036 |
| unstructured `partition_xml` | **1.000** | **1.000** | **0.321** | 0.085 | 838.0 | **6.821** |

Two results here matter more than the reversibility numbers.

**Pointing an HTML extractor at XML destroys the document, silently.**
`inscriptis` deletes **99.95%** of USLM — it is a CSS-aware HTML *renderer*, and
USLM tags carry no HTML display semantics, so it renders almost nothing and
raises nothing. selectolax and resiliparse each drop **14.6%**. None of them
error. This is the trafilatura failure again, in the format where structure
matters most, and it is invisible without a deletion metric — which is the whole
argument for measuring deletion rather than extraction.

**`unstructured` is the interesting near-miss.** It scores `unit_exact` 1.000
and `anchor_tri` 1.000 on USLM — its elements really are verbatim XML text nodes
— and finds 838 structural units against the incumbent's 858. And it **deletes
32.1% of the document** while doing it, at 6.8 s per small title (which
extrapolates to roughly 20 minutes for Title 42 alone). Scoring perfectly on
reversibility and still throwing away a third of the U.S. Code is exactly why
these two metrics are both required.

The incumbent finds 857.9 structural passages per title against markup that
states 4,345 `<section>` and 46,321 `<heading>` elements in Title 10 alone, each
with a `<num>` designator and an `identifier` attribute carrying the canonical
`/us/usc/t10/s…` path. There is nothing here for a layout model to contribute.

### USLM at scale — one 55 MB title (USC Title 10)

| candidate | wall time | peak RSS | structural units |
|---|---:|---:|---:|
| **incumbent** | 10.25 s | **414 MB** | **114,615** |
| lxml `itertext()` | 0.49 s | 612 MB | — |
| lxml + element walk | 2.66 s | **1,952 MB** | 621,345 *(inflated by nesting)* |
| selectolax | 0.54 s | 752 MB | — |

The incumbent is the **slowest and the leanest** here, and the second fact
matters more than the first. It streams through `html.parser` and holds no DOM,
so a 55 MB title costs 414 MB — where lxml costs 612 MB, selectolax 752 MB, and
the hand-written tree walk 1,952 MB. Title 42 is twice this size. A DOM-based
replacement would make memory the binding constraint on the largest documents
this platform ingests, to buy offsets it cannot return anyway.

## Docling — the finding that settles the layout-inference question

Docling's HTML backend on Federal Register HTML, at **both** the repo's pin
(2.115.0) and current (2.117.0, installed unpinned to rule out a stale-pin
artifact):

| document | source | Docling output | retained | items found | FR markers kept |
|---|---:|---:|---:|---|---:|
| `04-28286.html` | 257,998 B | 647 chars | **0.25%** | 1 table | **0 of 5** |
| `05-15486.html` | 267,570 B | 9,765 chars | **3.65%** | 6 text, 1 table | **0 of 5** |
| `05-17755.html` | 284,251 B | 4,779 chars | **1.68%** | 1 table | **0 of 5** |

Every one reports `ConversionStatus.SUCCESS` with `errors == []`. Byte-identical
across three separate processes — it is deterministic, and deterministically
wrong.

On a 258 KB endangered-species rule, Docling returns one acreage table and
discards the rest of the document, including the entire SUMMARY section. **This
is the trafilatura failure, in the tool this repository already ships as a
pinned extra, at 40× the severity, while reporting success.** A pipeline that
trusted `status == SUCCESS` would ingest 647 characters as the whole rule.

On XML it fails closed instead, which is the right behavior: FR-XML and USLM
match none of `XML_USPTO`, `XML_JATS`, `XML_XBRL`, `XML_DOCLANG`, so the
converter refuses with `File format not allowed`. Good.

**The repository is not exposed to any of this.** The pinned `docling` extra is
scoped in `src/spicy_regs/docpipeline/adapters/docling.py` to **DOCX, PPTX and
XLSX only** (`SUPPORTED_FORMATS`), PDF and image are recognized and refused with
`format_not_implemented` (`DEFERRED_FORMATS`), and `source.py`'s
`DISPATCH_PRIORITY` sends native markup down the native branch before the parser
is ever consulted. The adapter runs Docling's model-free `SimplePipeline` in a
child process and never lets a Docling object cross the boundary. That design
already anticipated this result. **Do not widen it to HTML.**

## Where the line falls

The brief asked for an opinion on whether layout inference is worse than parsing
stated markup. It is, and the measurement is not close:

> **For any format that states its own structure, a general-purpose
> document-AI extractor is not merely unnecessary — it is destructive, because
> its job is to guess at structure, and guessing is strictly worse than reading
> when the answer is written down.** Docling retained 0.25–3.65% of documents
> whose headings are literally `<h1>`–`<h6>` tags. `unstructured` found 73
> structural units where the markup states 728.

The line falls exactly at **"is the structure stated?"**:

| | structure is | correct approach |
|---|---|---|
| FR HTML, FR XML, USLM, eCFR XML, bills XML | **stated** in `<h1..h6>`, `<HD>`, `<SUM>`, `<section>`, `<heading>`, `<num>`, `DIV1..DIV9` | parse the markup; the incumbent already does |
| PDF | **not stated** — must be inferred from glyph geometry | inference is legitimate here — but measured, it does not pay: see below |

The brief expected the PDF half of this line to come out the other way, and so
did I. It did not. Inference is *permissible* on PDFs because there is no stated
structure to destroy — but permissible is not the same as better, and on 18 real
documents the rule-based extractors all land within 2% of each other while the
markdown-emitting ones land *below* pypdf. The line is real; it just marks where
inference stops being harmful, not where it starts being worthwhile.

USLM makes the case at its sharpest: `usc10.xml` states 4,345 `<section>` and
46,321 `<heading>` elements, each with a `<num>` designator and an `identifier`
attribute carrying the canonical `/us/usc/t10/s...` path. Any tool that inferred
a heading here would be re-deriving, with error, an answer the publisher already
gave with a URI attached.

## Recommendations per format

| Format | Verdict | Reason |
|---|---|---|
| **Federal Register HTML** | **keep the incumbent — reject all candidates** | Only the incumbent returns spans. `unit_exact` 1.000 vs 0.000; 39 s for the whole 993-document corpus at 95 MB. No candidate offers anything the incumbent lacks. |
| **Federal Register full-text XML** | **keep the incumbent — reject all candidates** | Same result, and `incumbent_visible` deletes 3e-06 here — the cleanest rendition measured. Docling refuses the format outright. |
| **USLM / U.S. Code XML** | **keep the incumbent — reject all candidates, and reject the domain libraries too** | The structure is stated with canonical identifiers. There is no maintained Python USLM library: `unitedstates/uscode` was archived 2025-05-30 with its full-content parser still incomplete; `usgpo/uslm` is a schema repo, not a parser; `opengovfoundation/USLM` is PHP. Adopting nothing is not a gap, it is the state of the ecosystem. |
| **eCFR / CFR XML** | **keep the incumbent** | Same reasoning. CFPB's `regulations-parser` and `regulations-xml-parser` are both marked DEPRECATED; everything else found is unpackaged hobby scripts over `ElementTree`. |
| **PDF** | **keep pypdf — reject pdfplumber, PyMuPDF4LLM, unstructured; PyMuPDF is blocked only by AGPL** | pypdf recovers within 2% of every alternative measured. pdfplumber costs 13× the memory for 2% *less* text. `unstructured`'s PDF path does not import. See below. |
| **DOCX / PPTX / XLSX** | **keep Docling, keep it scoped exactly as it is** | The adapter is model-free, out-of-process, and format-fenced. It is the correct shape. Do not widen it. |
| **Chunking** | **adopt Chonkie behind the segmentation interface only** | The one library measured that is both offset-exact and lossless. See below. |
| **Citation parsing** | **out of scope, but noted** | `eyecite` returns exact `.span()` offsets into text you already hold and is actively maintained — the one Free Law Project library that fits this platform's contract. `courts-db`, `juriscraper`, `x-ray` do not extract text. `lexnlp` and `Blackstone` are dormant. |

### PDF — measured, and the result reversed my expectation

18 real PDFs: 9 Supreme Court opinions, 4 CRS reports, 4 regulations PDFs, 1
more. Corpus digest
`sha256:6a816b04a271e226d83df18e60c74156da7356e288993b78027dc73316d5b415`.

| tool | total chars recovered | vs pypdf | s/doc | peak RSS | deterministic |
|---|---:|---:|---:|---:|:--:|
| **pypdf 6.14.2** *(incumbent)* | **2,849,368** | — | 0.385 | **122 MB** | yes |
| pdfplumber 0.11.10 | 2,793,104 | **−2.0%** | 2.513 | **1,582 MB** | yes |
| PyMuPDF 1.28.0 | **2,851,308** | **+0.07%** | **0.152** | 132 MB | yes |
| PyMuPDF4LLM 1.28.0 | 2,807,194 | −1.5% | 5.039 | 926 MB | yes |
| unstructured 0.25.0 `partition_pdf` | **0 — failed on all 18** | — | — | — | — |

`unstructured`'s PDF path does not run at all from a clean
`pip install unstructured`: every document raised
`ModuleNotFoundError: No module named 'pi_heif'`. An undeclared import in the
base install, the same class of defect the citation bakeoff found in CiteURL.

**pypdf is not losing text.** That was the assumption going in and it is wrong.
Every rule-based extractor lands within 2% of it, and pdfplumber and PyMuPDF4LLM
land *below* it — PyMuPDF4LLM recovers 5.4% less than pypdf on
`regulations-pdf-extreme.pdf` (623,286 vs 658,831 chars), because converting to
markdown discards what does not fit the markdown model.

So the case for replacing pypdf is **provenance alone**, and it is worth being
precise about whether this platform needs it. It does not, today:
`source.py` already models parser-derived text as its own coordinate target
(`PARSED_TEXT_TARGET`, `PARSER_COORDINATE_GRADES`), so offsets into pypdf's
extracted text are exact *by construction* against that target, and
`check_region_coordinates` passes. A bounding box would buy a page-rectangle
highlight in a reader UI. That is a product feature, not a correctness gap.

What the alternatives offer on the axis where they do differ:

| tool | per-span provenance | models | license |
|---|---|---|---|
| pypdf 6.14.2 *(incumbent)* | **none** | none | BSD-3 |
| **pdfplumber 0.11.10** | **per-character dicts** — `x0`, `x1`, `top`, `bottom`, `page_number`, font | none | MIT |
| pdfminer.six 20260107 | per-character `LTChar.bbox` | none | MIT |
| PyMuPDF 1.28.0 | per-character bbox via `get_text("rawdict")`, per-word via `"words"` | none | **AGPL-3.0 or commercial** |
| PyMuPDF4LLM 1.28.0 | markdown — headings inferred from *font-size popularity* | none | **AGPL-3.0 or commercial** |
| Docling PDF | `prov.charspan` + `bbox`, but charspan indexes Docling's *reconstructed* text | layout + TableFormer + OCR | MIT (weights separate) |
| marker 2.0.0 | block polygons; per-char only with `--keep_chars` on digital PDFs | Surya VLM **required** | code Apache-2.0, **weights OpenRAIL-M with a commercial threshold** |
| MinerU 3.4 | page-level | YOLO + PP-OCRv6 or VLM | custom license, formerly AGPL |
| olmOCR 0.4.0 | none documented | 7B VLM, **sampling** | Apache-2.0 |

**Recommendation: keep pypdf.** pdfplumber would cost **13× the memory**
(1,582 MB peak on 18 documents) and 6.5× the time to recover 2% *less* text,
in exchange for coordinates nothing currently consumes. That is not a trade
worth making on a survey.

**The one genuinely strict improvement is blocked by license, not engineering.**
PyMuPDF is 2.5× *faster* than pypdf, uses comparable memory, recovers marginally
more text, and gives per-character bounding boxes — it is better on every
measured axis at once. It is also **AGPL-3.0 or commercial (Artifex)**. That is
a licensing decision for Mike, not an engineering one, and it is the single
question that would change this recommendation. If the AGPL question is ever
resolved in PyMuPDF's favor, revisit this immediately; if it is not, pypdf
stays and nothing is lost.

PyMuPDF4LLM should be rejected regardless of license: it infers heading levels
from *font-size popularity* and recovers less text than pypdf. That is inference
where the incumbent does not pretend, and it loses content to buy it.

marker, MinerU and olmOCR are all disqualified by requirement 3 before license
even arises: marker's Surya has hardware-dependent batch sizing, olmOCR is
explicitly sampling-based, and none publishes a byte-identical-output guarantee.
Docling's own CI loosened its bounding-box comparison tolerance in 2.117.0
(`#3912`), which is the maintainers telling you the bboxes jitter.

### Chunking — Chonkie, behind the interface, nowhere else

Measured on 108,767 characters of real USLM (`usc09.xml`):

| library | chunks | offsets exact | source covered | `concat(chunks) == input` | deterministic |
|---|---:|---:|---:|:--:|:--:|
| **chonkie 1.7.0** `RecursiveChunker` | 418 | **418/418** | **100.000%** | **yes** | yes |
| semchunk 4.1.1 | 77 | 77/77 | 99.930% | no | yes |
| langchain-text-splitters 1.1.2 | 392 | 392/392 | — | no (`strip_whitespace=True` default) | yes |
| llama-index-core | — | best-effort `.find()`, unset when not found | — | no (hard-coded `.strip()`) | — |

Chonkie's pure-Python chunkers are the only surveyed library that satisfies the
platform's contract outright: `text[chunk.start_index:chunk.end_index] ==
chunk.text` held for every chunk, and concatenating every chunk reproduced the
input byte-for-byte.

Where it would sit is narrow and specific. `segments.py` splits **only a region
that is itself oversized**, into leaves of `SegmentSettings.leaf_budget` tokens
with backward overlap (`BOUNDARY_METHOD = "source-native-oversized-overlap"`).
Chonkie would be an alternative leaf splitter *there*, returning offsets that
become `SegmentSlice`s and still satisfy `check_segment_slices`. It must not
touch region selection — that is `source.py`'s native-structure branch and it is
the part that works.

Hard constraints on any adoption: only the pure-Python chunkers
(`TokenChunker`, `SentenceChunker`, `RecursiveChunker`, `CodeChunker`,
`TableChunker`, `FastChunker`). `SemanticChunker`, `LateChunker`,
`NeuralChunker`, `SDPMChunker` are model-based; `SlumberChunker` calls a hosted
Gemini API. Those are disqualified by requirements 3 and 4. And the default
`"character"` tokenizer must be kept, or a named tokenizer will download on
first use.

## What a bounded bakeoff would look like

Two things this survey cannot settle, each with a scoped protocol.

**1. PyMuPDF vs pypdf — but the gate is legal, not technical.** PyMuPDF is
better than pypdf on every axis measured here: 2.5× faster, +0.07% text,
per-character bounding boxes, comparable memory. The only thing standing in the
way is **AGPL-3.0 or a commercial Artifex license**. So the bakeoff is not the
next step; the license decision is. Protocol *after* that decision, and only if
it goes PyMuPDF's way: run PyMuPDF's `get_text("words")` over the 18 PDFs,
reconstruct a `SourceRegion`-shaped coordinate against the *parser-derived*
field target that `source.py` already defines (`PARSED_TEXT_TARGET`,
`PARSER_COORDINATE_GRADES`), and check whether the 1,296/1,302-segment baseline
moves. Stop rule: any movement in segment counts needs its own reseal and is a
separate decision. Cost: no provider spend, under an hour. **Do not run this
before the license question is answered — the engineering result is already
known and it is favourable, which is exactly why it should not be used to
pre-commit the licensing choice.**

**2. Chonkie as an oversized-leaf splitter.** The survey proves the offset
contract on one 108 KB USLM file. It does not prove behavior on the regions
`segments.py` actually splits, which are the *oversized* ones. Protocol: take
every region the current run classifies as oversized, split it both ways, and
compare (a) leaf count, (b) whether every leaf still satisfies
`check_segment_slices`, (c) whether the frozen 1,302-segment baseline holds.
Gate: this is only worth doing if it is expected to *change* boundaries, and
changing boundaries invalidates the P@1 0.438 → 0.938 measurement that made
passage-aligned chunking the selected policy. **The honest expectation is that
this bakeoff should not be run until there is a retrieval reason to move
boundaries.** Recorded as available, not as queued.

## Every candidate, one line each

Measured = run on this project's real files. Surveyed = assessed from current
documentation only, with the reason it did not need running.

| Candidate | Version | How | Offsets into source | Verdict |
|---|---|---|---|---|
| **incumbent** `source.py` | `f425365` | measured | **exact spans, proven per document** | **keep** |
| lxml | 6.1.1 | measured | none (`sourceline` is a line number) | reject |
| BeautifulSoup4 | 4.15.0 | measured | `sourcepos` is tag-level, and absent on the lxml backend | reject |
| selectolax | 0.4.11 | measured | none | reject |
| html-text | 0.7.1 | measured | none; normalizes whitespace by design | reject |
| inscriptis | 2.7.3 | measured | none; CSS-aware renderer, deletes 99.95% of XML | reject |
| resiliparse | 1.0.9 | measured | none | reject |
| unstructured | 0.25.0 | measured | element bbox only; deletes 32–38% of XML; PDF path does not import | reject |
| Docling | 2.115.0 + 2.117.0 | measured | `prov.charspan` indexes Docling's own text | **keep, scoped to Office only** |
| pypdf | 6.14.2 | measured | n/a for PDF | **keep** |
| pdfplumber | 0.11.10 | measured | per-character bbox | reject — 13× memory, 2% less text |
| PyMuPDF | 1.28.0 | measured | per-character bbox | **blocked on AGPL only** |
| PyMuPDF4LLM | 1.28.0 | measured | markdown; headings from font-size popularity | reject |
| chonkie | 1.7.0 | measured | **exact `start_index`/`end_index`, lossless** | **adopt behind the segmentation interface** |
| semchunk | 4.1.1 | measured | exact offsets, but drops separator whitespace | reject |
| langchain-text-splitters | 1.1.2 | measured | exact with `add_start_index=True`; `strip_whitespace=True` by default | reject |
| llama-index-core | 0.14.23 | surveyed | best-effort `.find()`, left unset when not found; hard-coded `.strip()` | reject |
| pdfminer.six | 20260107 | surveyed | per-character bbox — but it *is* pdfplumber's engine | subsumed |
| marker | 2.0.0 | surveyed | block polygons | reject — Surya VLM required; **weights are OpenRAIL-M with a commercial threshold** |
| MinerU | 3.4 | surveyed | page-level | reject — removes headers/footers by design, ~20 GB install |
| olmOCR | 0.4.0 | surveyed | none documented | reject — 7B VLM with sampling; fails requirement 3 outright |
| Extractous | — | surveyed | not verified | not pursued — Tika-derived, no offset contract found |
| trafilatura / readability-lxml / jusText | — | previously rejected | none | still rejected; reason restated as offsets, not deletion |
| `unitedstates/uscode` | archived 2025-05-30 | surveyed | n/a | reject — unmaintained, full-content parser never finished |
| `usgpo/uslm` | 2.1.0 | surveyed | n/a | not a parser — it is the schema |
| CFPB `regulations-parser` | — | surveyed | n/a | reject — marked DEPRECATED |
| eyecite | — | surveyed | exact `.span()` into text you hold | **fits the contract** — out of scope here, noted for citations |
| courts-db / juriscraper / x-ray | — | surveyed | n/a | not text extraction |
| lexnlp / Blackstone | — | surveyed | n/a | not text extraction, and dormant |
| Free Law `doctor` | — | surveyed | none | a service, not a library; wraps `pdftotext` |

## How this was run

Model tiering, per the standing rule. **Sonnet** did the token-heavy,
low-judgement work: reading current documentation for ~30 libraries across three
parallel research agents, building the throwaway venv, and bulk-running the
harness over the 993/318/18-file corpora. **Opus** did the judgement: choosing
what to measure and why, designing `unit_exact`/`anchor_tri`/deletion, writing
the harness and its tests, diagnosing the anchoring-failure causes, running the
Docling and chunker probes, and every verdict in this document.

Two results in this document exist only because Opus distrusted a Sonnet-clean
run: the first `anchor_tri` numbers were depressed by the harness's own unit
separator (fixed — probes now never cross a unit boundary), and the incumbent's
apparent 5.6% deletion is a units mismatch, not a loss (§ HTML, footnote †).

## What this does NOT settle

- **DOCX/PPTX/XLSX extraction quality was not measured.** Docling is the
  incumbent there and no alternative was run against it. The HTML finding says
  nothing about the Office backends, which use a different, declarative,
  model-free pipeline.
- **Scanned PDFs and OCR are entirely out of scope.** Every PDF measured is
  born-digital. If a scanned rule enters the corpus, none of this applies and
  the model/determinism questions reopen.
- **Tables.** `unstructured` and Docling both claim table structure the
  incumbent does not model. The incumbent emits `table` and `table-row` region
  kinds but does not reconstruct a grid. Whether that matters is a retrieval
  question no one has asked yet.
- **`anchor_tri` is sampled at fixed stride, 300 probes per document**, not
  exhaustive. The stride is deterministic so a rerun probes the same positions,
  but the number is an estimate with a real, unquantified interval.
- **The deletion reference is this harness's own definition** — every text node
  outside `script`/`style`/`noscript`/`iframe`/`svg`/`template`, entity-decoded.
  It is deliberately not the incumbent's extractor, so the numbers are not
  circular, but it is still a choice and another choice would move them.
- **No candidate was tested under a free-threaded interpreter.** selectolax
  0.4.8 enabled free-threading; whether output is stable there is unknown.

## Corrections

1. The brief's "~42 zero-width spaces per document" is withdrawn. Measured over
   all 993 documents: **10.2 mean, 6 median, 143 max**, present in 82.1% of
   documents, 98.2% inside URL tokens. This independently confirms the
   correction committed at `f425365` from a 56-document sample.
2. The brief's "1.47 GB" is the release directory, not the corpus the
   extractors parse. The HTML bodies are **290 MB**; the release directory is
   1.7 GB today.
3. A documentation-only reading placed `<SECTION>` in the eCFR schema and not
   the Federal Register schema. The real FR-XML files in
   `cache-xml/documents/` contain `<SECTION>`. Real files beat the user guide.
4. The prior survey's finding that boilerplate removers destroy regulatory text
   does not reproduce in this generation. `resiliparse main_content=True` kept
   all 987 FR section markers. The reason to reject these libraries is offsets,
   not deletion — and stating the wrong reason would have made this evaluation
   easy to overturn.

## What was built from this, 2026-08-02 — the coverage floor

**Built and wired.** `check_extraction_retention` in
`src/spicy_regs/docpipeline/source.py`, called at both markup and both PDF
boundaries in `src/spicy_regs/document_file_pipeline.py`, where a representation
is about to be sealed. Tested by `tests/test_docpipeline_extraction_floor.py`
(30 tests). The distribution is reproducible with
`tools/measure_extraction_retention.py`.

The exposure this closes is the one measured above: Docling returned 502 visible
characters from a 257,998-byte rule and reported `ConversionStatus.SUCCESS` with
an empty error list. Coverage did not catch it and **could not** — regions are
gap-free over whatever field they are given, so a field built from 0.27% of a
document is still 100% covered. The number that catches it compares what came
out against what the source independently says is there.

### The measured retention distribution

Retention is defined per coordinate system, because the formats do not share
one. `markup-visible` is extracted visible characters over the source's own
visible characters — both sides computed by the same collector, so the ratio
means "how much of the document survived". `parsed-per-source-byte` is extracted
characters per source byte, for formats that carry no source text to compare
against; it is a density, not a fraction, and the two may never be compared.

| corpus | unit | n | min | p01 | p05 | p50 | max |
|---|---|---:|---:|---:|---:|---:|---:|
| Federal Register HTML | markup-visible | 993 | **0.9598** | 0.9854 | 0.9931 | 0.9965 | 0.9987 |
| Federal Register full-text XML | markup-visible | 993 | **0.9936** | 0.9969 | 0.9977 | 0.9984 | 0.9997 |
| USLM / U.S. Code XML | markup-visible | 7 | 0.9963 | — | — | 0.9972 | 0.9974 |
| eCFR XML | markup-visible | 4 | 0.9947 | — | — | 0.9960 | 0.9980 |
| Bill XML | markup-visible | 3 | **0.9930** | — | — | 0.9982 | 1.0000 |
| GAO HTML | markup-visible | 4 | **0.9453** | — | — | 0.9645 | 0.9654 |
| segmentation-cache FR HTML | markup-visible | 4 | 0.9909 | — | — | 0.9961 | 0.9976 |
| PDF via pypdf | parsed-per-source-byte | 18 | **0.015584** | — | 0.019428 | 0.233897 | 0.558566 |

Against the measured failures: Docling's HTML backend retained **0.0027, 0.0080,
0.0098 and 0.0122** on the four Federal Register documents probed.

### The floors, and the margin under each

| key | floor | unit | lowest legitimate | margin | population |
|---|---:|---|---:|---:|---|
| `native:text/html` | **0.75** | markup-visible | 0.9453 (GAO) | **1.26×** | 1,001 HTML documents |
| `native:application/xml` | **0.85** | markup-visible | 0.9930 (bill XML) | **1.17×** | 1,007 XML documents |
| `pypdf:application/pdf` | **0.005** | parsed-per-source-byte | 0.015584 | **3.12×** | 18 PDFs |

HTML and XML get different floors because their distributions are different
shapes, not as a matter of taste: HTML carries navigation chrome a parse
legitimately excludes and spreads from 0.9453 to 0.9987, while XML carries none
and never fell below 0.9930 across 1,007 documents. One number across both would
have been either loose enough to be useless on XML or tight enough to be wrong
on HTML.

**Distance to the failures.** The worst Docling failure (0.0122) sits **61×
below** the HTML floor; the lowest legitimate document sits 1.26× above it.
`RetentionFloor.__post_init__` refuses any floor at or above its own observed
minimum, so a later edit cannot quietly remove the margin.

### Verified: zero false refusals

The live gate run over every real document reachable in this tree:

| corpus | n | refused | worst retention |
|---|---:|---:|---|
| Federal Register HTML | 993 | **0** | 0.9598 (`2016-17322.html`) |
| Federal Register full-text XML | 993 | **0** | 0.9936 (`2023-01025.xml`) |
| GAO HTML | 4 | **0** | 0.9453 (`gao-html-4.html`) |
| eCFR + bill XML | 7 | **0** | 0.9930 (`bill-xml-extreme.xml`) |
| USLM | 7 | **0** | 0.9963 (`usc24.xml`) |
| **total** | **2,004** | **0** | |

### The three ways it fails closed

1. **Below the floor** — the refusal names the measured retention, the floor,
   the parser, the format and the subject, so a failure is diagnosable from the
   receipt without rerunning the parse.
2. **No declared floor for this parser and format.** Undeclared is deliberately
   not "inherit a default": a new extractor states the population its floor came
   from before it may run. This is the same shape as the adapter's
   `format_not_implemented`.
3. **Unmeasurable** — a source with no visible text is not an extraction the
   gate can vouch for, and reads differently from a below-floor refusal.

**There is deliberately no floor for Docling on Office formats.** No DOCX, PPTX
or XLSX population exists in this tree to measure, and declaring a floor for a
population nobody measured is exactly the taste this gate replaces. Until one is
measured that parser has no floor and the gate refuses it — intended, not an
oversight, and the open item this build leaves behind.

### The exemption is stated, never silent

`SourcePolicy.retention_exemptions` is a frozenset of subject ids, **empty by
default**. A legitimate low-retention document — a form, a table-only filing —
gets through by being named in it, and the resulting `RetentionCheck` records
`exempt=True` and which id matched. An exemption nobody had to write down would
be indistinguishable from a gate that does not work, which is why there is no
wildcard and no threshold-lowering escape hatch.

## What was built from this, 2026-08-02 — PyMuPDF adopted for PDFs

**The AGPL block was lifted deliberately, so the swap was made.** The survey's
recommendation was "keep pypdf; PyMuPDF is better on every measured axis and
blocked solely by licence." With the licence question answered, PyMuPDF is now
the parser for **new** PDF captures.

`src/spicy_regs/transforms/pdf_text_pymupdf.py` is a **new parser**, not an edit
of `pdf_text.py`. Tested by `tests/test_transforms_pdf_text_pymupdf.py`
(20 tests). `pymupdf>=1.28,<1.29` is pinned in `pyproject.toml`, because a
parser change moves extracted text and extracted text is sealed.

### Re-measured through the parser that ships, not the survey harness

18 real PDFs, `spicy_regs.transforms.pdf_text_pymupdf` against
`spicy_regs.transforms.pdf_text`:

| | pypdf 6.14.2 | PyMuPDF 1.28.0 |
|---|---:|---:|
| characters recovered | 2,850,117 | **2,851,024** (+0.03%) |
| wall time, 18 documents | 7.07 s | **2.77 s** (2.55× faster) |
| page-count agreement | — | **18 / 18** |
| per-document delta | — | −0.05% … +0.11% |
| per-word bounding boxes | none | yes |

The survey's headline numbers hold through the real path: +0.07% became +0.03%,
2.5× became 2.55×. **The text-volume case remains a wash** — per document the
delta runs both ways and no document moved by more than 0.11%. The parser was
adopted for **speed and per-word coordinates**, not for volume, and the
retention floor records exactly that.

### What changes for documents already captured with pypdf: nothing

This is the constraint the swap was subject to, and it is enforced in code and
in tests rather than asserted:

* `_extract_pdf_with(method, ...)` takes the parser **by name**. The locked path
  reads `lock_record["extraction_method"]` and extracts with *that* parser, so a
  pypdf-sealed record keeps reproducing under pypdf byte for byte with the new
  parser installed and the default already switched away.
  `test_the_named_parser_decides_not_the_default` pins it.
* An unknown parser name **fails closed** rather than falling back to a default.
  A silent fallback is the one way this check could lie: a lock naming a parser
  this build lacks would otherwise verify against a different one.
* The receipt records `method`, `method_version` (the version *imported*, not
  requested) and `method_config`, which now carries
  `reading_order: content-stream` for PyMuPDF — recording that the parser does
  **not** re-infer a reading order the PDF already states. `sort=False` is
  load-bearing, not incidental.
* Re-extraction produces a **new release with its own digest**. No existing
  release is edited. Nothing retroactively moves.
* Each parser declares its **own** retention floor over its **own** measured
  population: `pymupdf:application/pdf` at 0.005 against an observed minimum of
  0.015571, margin 3.11× — indistinguishable from pypdf's, because the two
  recover the same text.

One process note, recorded because it affected attribution: the
`document_file_pipeline.py` half of this wiring landed inside commit `58b144c`,
authored by a concurrent agent that staged the shared file while this work was
in progress. The code is correct and tested; the commit message does not
describe it.

## The VLM parsing experiment — feasibility established, experiment not run

**Recommendation: architecturally fine, not worth the dependency yet. Do not
adopt. The honest next step is smaller than the one proposed.**

### It is wrong for markup formats, and this document's own data says so

Not tested there, deliberately. FR HTML, FR XML, USLM and eCFR carry
*authoritative stated structure*; a model would infer what the document already
declares. The measurement above shows generated text scores **0.000** on
`unit_exact` and loses ~10% of three-token windows. For those formats a VLM is
strictly worse than reading the markup, and no experiment is needed to know it.

### For PDFs the provenance objection genuinely does not apply

PDF text extraction is **already** a derivation — the platform records
`evidence_grade: parser-derived` and targets `PARSED_TEXT_TARGET` precisely
because there is no source text to be exact against. A different derivation does
not violate the contract; it changes which derivation produced the sealed text,
and that is recorded in `method`/`method_version`/`method_config`. Combined with
capture-once-seal-pin, run-to-run variation stops mattering. **The safety
argument holds.**

### Docling's VLM pipeline at our pin: verified, with a caveat

Verified by introspecting the installed package rather than assuming:

| Question | Answer at docling 2.115.0 |
|---|---|
| `VlmPipeline`, `VlmPipelineOptions`, `ApiVlmOptions` exist? | **Yes**, all import cleanly |
| Remote OpenAI-compatible endpoint supported? | **Yes** — `ApiVlmOptions.url`, gated behind `enable_remote_services` (default `False`, raises `OperationNotAllowed` otherwise) |
| Gemini supported specifically? | **No Gemini-aware code path** — `grep -rni gemini` over the installed package returns **zero matches**. Gemini's OpenAI-compatible endpoint (`https://generativelanguage.googleapis.com/v1beta/openai/`) is structurally usable, but is an **undocumented, untested user configuration**, not a supported path |
| Sends text or page images? | **Rasterized page images** — one PNG per page, base64-embedded, at `scale=2.0` default |

`enable_remote_services` defaults to `False` because "the main purpose of Docling
is to run local models which are not sharing any user data with remote
services." Turning it on is a deliberate act, which is the right shape.

### Estimated cost, and why the experiment was not run

At `gemini-2.5-flash-lite` ($0.10/1M input, $0.40/1M output), ~1,032 image tokens
per page at docling's default scale, 18 PDFs ≈ 1,500 pages: **≈$0.46–$1.06**,
well inside the $15 cap. Cost is not the blocker.

The blocker is that **the experiment as scoped would not answer the question it
was asked to answer**, for three reasons found while establishing feasibility:

1. **The comparison would measure docling's DocTags preset, not "a VLM".**
   Docling's default response format is DocTags — verbose, bounding-box-annotated
   XML-like output. A poor result would not distinguish "VLMs infer structure
   badly" from "this preset serialises badly", and a good one would be equally
   ambiguous.
2. **There is no gold structure for these 18 PDFs to score against.** "Better
   structure than PyMuPDF" needs an adjudicated answer for each document, and
   this project has a standing rule about what counts as human review. Scoring
   passage quality by eyeballing markdown is the "prose readability" measure the
   brief explicitly ruled out.
3. **The population is the wrong one for the decision.** PDFs are a minority of
   this corpus — the Federal Register bodies are 993 HTML and 993 XML documents
   against 18 PDFs in the segmentation cache. Even a large PDF structure
   improvement moves little retrieval quality here, which is the honest reading
   the brief asked for and it is the one the data supports.

### What would actually be worth running, and when

Not a docling VLM bakeoff. **A gold-anchored structure comparison on court
opinions and GAO reports**, the two formats where PDF structure is genuinely
ambiguous and where a real finding is plausible — with (a) an adjudicated
structural answer for a small sample, (b) a lean response format chosen by us
rather than DocTags, and (c) a retrieval metric downstream, not a readability
judgement. That is a bounded experiment with a real gate, and it should be
queued behind anything that touches the 1,986 markup documents, because that is
where this corpus actually lives.

**Spend on this line so far: $0.00.** No provider call was made.
