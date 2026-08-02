# Body corpus follow-up: admission, and the XML rendition — 2026-08-02

- **Date:** 2026-08-02
- **Status:** Measured, receipted, unpublished. Nothing pushed.
- **Interpreter:** CPython 3.12.9
- **Paid provider calls:** zero
- **Network:** 993 further GET requests (the XML rendition of the same draw)
- **spicysearch was read, never written.** Another agent is active in that
  repo; the admission probe imports its code and writes only into `/tmp`.

Follows [the corpus record](body-retrieval-corpus-2026-08-02.md). Two questions,
in order, because the first can invalidate the second: can spicysearch admit
this corpus at all, and is the GPO XML rendition the better instrument?

## Part 1 — admission

### The admission path materializes; it does not stream

`spicysearch.canonical.read_json` is `json.loads(path.read_text())`
(`src/spicysearch/canonical.py:386-392`), and `adapters.py` takes an
already-parsed `Mapping`. There is no streaming parser anywhere on the path —
no `ijson`, no incremental reader. The whole release becomes one Python object
graph before any adapter runs.

That is a real constraint, but **it is not the blocker**. The full corpus
admits, end to end, and the snapshot is queryable:

| build | documents | passages | wall time | peak RSS | index |
|---|---|---|---|---|---|
| 20 documents, HTML | 20 | 16,742 | 37.2 s | 0.98 GB | 41.7 MB |
| 20 documents, **XML** | 20 | 7,122 | **11.0 s** | 1.33 GB | 31.2 MB |
| **993 documents, HTML** (permissive) | **993** | **726,009** | **3,274 s (54.6 min)** | **15.81 GB** | **1.73 GB** |
| **993 documents, XML** (permissive) | **993** | **333,363** | **990 s (16.5 min)** | 20.9 GB | 1.33 GB |
| **993 documents, XML** (**production policy**) | **520 admitted** | 159,179 | **566 s** | 18.4 GB | 609 MB |
| **993 documents, HTML** (**production policy**) | **520 admitted** | 347,441 | 2,879 s | 12.0 GB | 798 MB |

Materializing the 1.47 GB release costs **5.36 GB** resident and 4.9 s — a ~3.6x
inflation from Python object overhead, which is ordinary. The remaining 10 GB
and essentially all 55 minutes are the lexical index build, not the parse.

**Time is the scaling problem here, not memory.** 20 documents to 993 is 50x the
documents and 43x the passages, but 37 s to 3,274 s is **88x the time** —
clearly superlinear. Memory, by contrast, grew only 16x. A corpus 10x this size
would not fail for want of RAM on this machine; it would take most of a day.

The XML rendition is markedly cheaper at every scale: **990 s against 3,274 s**
for the same 993 documents, because it carries 46% of the passages without
carrying less text.

### The snapshot is real, and it contains the thing under test

Querying the built `search.duckdb` directly:

| table | rows |
|---|---|
| documents | 993 |
| **chunks** | **993** |
| passages | 726,009 |

**Exactly one chunk per document**, confirming the shipped grouping behaviour on
real bodies at scale. And the chunk text is the point:

- median chunk text: **207,666 characters**
- max chunk text: **2,337,620 characters**

The prior corpus produced a single chunk of 30,487 characters. This one produces
a median chunk **6.8x longer**, in 993 competing documents. That is precisely
the BM25 length-normalization case — `k1·(1−b+b·len/avg_len)` with `len`
varying over an order of magnitude — and it is now instantiated, indexed, and
queryable rather than hypothesized.

### What actually blocked admission — two of them were defects in this release

Found by attempting the admission, not by reading. **The first two are
release-construction defects in spicy-regs, and neither is repairable
downstream**: patching a field in a copy breaks the release's own digest check.

1. **`observed_at` was a bare calendar date.** The release recorded
   `"2026-08-02"` — the *fetch* date at day granularity — and spicysearch
   refuses it at `SourceRenditionCapture`, **before any allowlist runs**.
   spicy-regs' own contract *permits either*: `_require_capture_observation`
   (`document_file_pipeline.py:87-106`) explicitly accepts "an exact calendar
   date **or** a timezone-aware ISO instant". The two products disagree, so a
   date seals cleanly upstream and is rejected downstream after a build that
   takes the better part of an hour.
   **Fixed at construction.** The fetcher now records a real per-document
   capture instant (`2026-08-02T20:28:06Z`), and `require_capture_instants`
   refuses to build a release from a dateless cache. A date cannot be upgraded
   to an instant without inventing precision, so the only honest fix is a
   refetch — which is exactly what the error says.

2. **`document_type` was a constant.** The release stamped
   `"Federal Register document"` on all 993, because
   `_SOURCE_CACHE_DOCUMENT_TYPES` carries one string per *source profile*. That
   is fine for a fixture corpus and fatal for a real one: **a constant makes
   every downstream type allowlist a no-op.** A policy admitting
   `{Notice, Proposed Rule}` matches nothing, so `Rule` and `Proposed Rule` are
   excluded *identically* and the corpus admits **0%**. The publisher's real
   type was in the parquet the draw read and simply never travelled.
   **Fixed at construction.** The release now carries 520 `Proposed Rule` and
   473 `Rule`.

3. **A v2 release requires its published distribution**, not a bare release
   JSON: "build from the directory, not the file". Only the file-manifest path
   had a publish counterpart, so a source-cache release could be built and then
   never admitted anywhere. `publish_document_release_from_source_cache` was
   added.

**A false lead, corrected.** An earlier version of this record said the
publisher spelling `federal-register` vs `federal_register` was a blocker. It is
not: `adapters.py:1461-1465` (`_source_key`) maps it explicitly. That claim is
withdrawn.

### The proof that matters: admission under the *real* policy

Not the permissive fixture policy the repo's experiments use — the production
`known-source-profiles-v1` / `search-document-types-v1` pair:

| | before the fix | after (XML) | after (HTML) |
|---|---|---|---|
| reaches the allowlist | **no** — refused at `observed_at` | **yes** | **yes** |
| documents admitted | **0** | **520** | **520** |
| chunks | 0 | 520 | 520 |
| passages | 0 | 159,179 | 347,441 |
| build | refused | 566 s, 609 MB index | 2,879 s, 798 MB index |

Both renditions admit **exactly the same 520 documents**, which is the check
that the fix is in the release construction and not in one format's quirks —
and XML gets there **5.1x faster**.

**520 of 993 admitted, and the other 473 are excluded by policy rather than by
defect** — `search-document-types-v1` admits `Notice` and `Proposed Rule` for
the Federal Register and does not admit `Rule`. That remains a live policy
question (half of any rulemaking corpus is invisible to a production snapshot),
but it is now a *decision* rather than a bug hiding behind one.

For context on why this matters: the "~12% admitted" figure this campaign has
been quoting is 12% of *records* on an 82-record synthetic benchmark, never
measured against real data. Against a real corpus it was **0%**, because of the
two defects above. It is now **52.4%** — which is 100% of the document types the
production policy actually admits.

### The fix is upstream, and it is honest

Both rebuilds record a **real per-document retrieval instant** at fetch time —
XML `2026-08-02T20:28:06Z`, HTML `2026-08-02T21:40:20Z` — rather than
normalizing a date into a fabricated midnight. That satisfies both contracts
without inventing precision after the fact.

The HTML rendition had to be **refetched** to get one, because a calendar date
cannot be upgraded to an instant without making the time up. That refetch is
also the strongest available check that the corpus is stable: 993 documents
fetched a second time, and the re-measurement is **byte-identical** to the
first (`measurement.json` digest `c5e20234…` both times). The publisher served
the same bytes.

### The incumbent 34-document corpus cannot be admitted at all

Run through the same probe, the existing segmentation corpus fails *earlier*
than this one, at `SourceRecordVersion … has an unsupported source kind` — it
spans seven publisher families (court opinions, CRS reports, bills) that
spicysearch does not model. The ESA corpus clears source kind, document
eligibility, and passage generation. **It is strictly closer to admissible than
what it replaces**, which is the more useful comparison than either result alone.

## Part 2 — the XML rendition

Same sealed draw, same 993 documents, second rendition. The draw was **not**
redrawn: each XML URL is derived from the publisher's own HTML URL
(`/full_text/html/…​.html` → `/full_text/xml/…​.xml`), verified against the API's
`full_text_xml_url` field for documents spanning 2005–2026, so both corpora are
provably over the same document set and the same manifest digest identifies both.

The XML fetch: **993 requested, 993 fetched, 0 quarantined, 0 failures.** That
the derivation held for all 993 is a stronger proof than the API sample was.

### Is the XML actually cleaner? Yes, but less dramatically than claimed

The earlier record said the HTML carries "~42 zero-width spaces per document".
**That was repeated from a survey, not measured, and it is wrong in kind and in
magnitude.** Measured across all 993 documents:

- there are **no raw U+200B characters at all** — zero, in every document;
- there are `&#8203;` **HTML entities**: **10,114 across 993 documents**, a mean
  of **10.2 per document** (on a 56-document sample the median was 2 and the
  max 24, so the distribution is skewed, not uniform);
- they occur **only inside displayed URLs**
  (`https://www.fws.gov/&#8203;sites/&#8203;default/…`), never in prose;
- the XML rendition has **zero**, entity or character, in all 993.

The defect is real — a consumer that resolves entities gets broken URL tokens,
one that does not gets a literal `8203` token — but it is confined to URLs. At
a mean of 10 per document rather than 42, the correction makes the case for XML
*weaker* than stated, not stronger. XML still wins; the reason is the next
section, not this one.

### Does the XML parse as well? It parses better

Full corpus, both renditions, 993 documents each, from committed code:

| | HTML | XML |
|---|---|---|
| visible text, total chars | 210,535,931 | 206,998,714 (**98.3%**) |
| visible text, median chars | 164,381 | 161,284 (98.1%) |
| source bytes, total | 301,787,891 | 271,605,502 |
| structural passages, total | **726,009** | **333,363** (45.9%) |
| structural passages, median per doc | 509 | 262 |
| passage length p10 / median / p90 | 47 / 135 / 1,099 | **132 / 610 / 1,515** |
| **passages under 100 chars** | **40.8%** | **6.8%** |
| `&#8203;` entities | 10,114 | **0** |
| publisher chrome blocks | 993 | **0** |
| median pairwise Jaccard | 0.2947 | **0.2945** |

A raw count says HTML yields 2.2x the passages and would read as XML being
sparser. It is not sparser in *content*: visible text is within 1.7%, and the
difference is exactly the site chrome that all 993 HTML documents carry and no
XML document does. It is less **fragmented** — **40.8% of HTML passages are
sub-100-character spans** (table cells, inline elements, chrome fragments)
against 6.8% for XML. HTML's p10 passage is 47 characters; XML's is 132.
A 47-character span is not a retrieval unit.

**The corpus property survives the rendition change.** Median pairwise Jaccard
is 0.2947 on HTML and 0.2945 on XML — a difference of 0.0002. Vocabulary
competition is a property of the documents drawn, not of the file format they
were fetched in, which is what makes the two renditions a controlled variable
rather than two different corpora.

### Does the XML lose the metadata BM25 needs? No

Nine metadata regions probed across 30 document pairs — RIN, CFR part, agency
name, ACTION, SUMMARY, DATES, FOR FURTHER INFORMATION, Federal Register page
number — and **XML and HTML are identical on every one**, present or absent
together. The GPO rendition carries them as semantic tags (`<AGENCY>`, `<RIN>`,
`<SUM>`, `<EFFDATE>`, `<REGTEXT>`, `<PRTPAGE>`) rather than as styled divs.

### And it is cheaper to admit

The 20-document XML distribution admits in **11.0 s against 37.2 s** for HTML,
producing a **31.2 MB** index against **41.7 MB** — a direct consequence of
carrying 42% of the passages without carrying less text.

## Verdict

**XML is the better instrument, and both renditions are sealed.** XML wins on
cleanliness (no entities, no chrome), ties on content (97.9% of text, 100% of
metadata regions), and wins on cost (3.4x faster admission, 25% smaller index).
The one thing HTML has more of is fragmentation.

Both are kept rather than one being discarded: they are the same 993 documents
under the same draw digest, so a retrieval experiment can use the rendition
difference as a controlled variable instead of a confound. That is worth more
than the disk.

## What this changes for whoever consumes it

- **spicysearch work item, not a corpus one:** the admission path materializes
  the entire release. Materialization is affordable — 5.36 GB for 1.47 GB of
  JSON — and is *not* what hurts. The lexical index build is: 55 minutes for
  993 documents, growing **superlinearly** (50x the documents cost 88x the
  time). The corpus is admissible today; a corpus 10x larger would be a
  day-long build, and that is the number to watch, not RAM.
- **spicysearch work item:** `search-document-types-v1` does not admit
  Federal Register `"Rule"`, only `"Notice"` and `"Proposed Rule"`. Half of any
  rulemaking corpus is invisible to a production snapshot.
- **Cross-product contract disagreement:** spicy-regs permits a calendar date
  for a capture observation; spicysearch requires an instant. One of the two
  should move. Recording the true instant, as the XML corpus now does, satisfies
  both and is better provenance regardless.
