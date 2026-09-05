> Restored 2026-09-05 from `8d9e7a2` (the second-pass copy; `ee95a7b` is the
> first pass). The rollup this describes ships in PR #195 with
> `DEFAULT_MAX_RECORDS = 250_000` in `pipelines/rollups/court_opinion_bodies.py`;
> the cluster table is the whole dump.

# Court data coverage — what was captured, what was not, 2026-08-22

**Verdict: the opinion-text seam is closed and the decision-to-docket join now
exists. Opinion *text* coverage is a bounded slice, not a backfill, and the two
things standing between here and full coverage are 8.6 hours of single-threaded
bandwidth and a 100 GiB free-space floor — not missing code.**

This is the verification section the court-data expansion owes. Every number is
measured, and each check names the denominator it was measured against, because
a coverage claim is worth exactly what its denominator is worth.

> **Second pass, same day.** Four of the nine gaps below were worked after this
> document was first written, and working them changed three of its findings.
> The corrections are folded into the sections they belong to and summarised in
> [What the second pass changed](#what-the-second-pass-changed) at the end. The
> largest of them: the 12,666 "unexplained orphans" in section 2 were not
> unexplained and were not orphans. They were an arithmetic error in this
> document, and there are zero orphans.

## Where the denominators come from

| Question | Denominator | Authority |
|---|---|---|
| How much bulk data exists? | The bucket's own S3 listing | `storage.courtlistener.com`, captured 2026-08-22 and pinned in DocSpec at `fixtures/courtlistener-bulk-v1/` |
| How many APA suits are there? | `court_dockets` on R2 | itself the complete result of a `nature_of_suit=899` search |
| How many Supreme Court opinions? | the Court's term index pages | `supremecourt.gov/opinions/slipopinion/<term>` |

The publisher's listing is not an index we assembled — it is CourtListener's own
statement of what exists, which is why it is captured verbatim and pinned rather
than summarized. Reproduce with `scripts/verify_court_coverage.py`.

### Who owns the population, and who owns the read

Worth stating explicitly, because the two were being conflated:

* **DocSpec owns the population.** `fixtures/courtlistener-bulk-v1/` captures
  the bucket's listing XML verbatim, digests every page, and — this is the part
  spicy-regs has no equivalent of — distinguishes an object the publisher
  *withdrew* (`DELETED`, a tombstone) from one *we* declined (`EXCLUDED`, our
  decision, reviewable). A population that shrinks without saying so is
  indistinguishable from a broken capture, and that is the failure DocSpec
  exists to prevent.
* **spicy-regs owns the read inside an object.** Streaming a 50.8 GiB bzip2 CSV,
  the `\"` escape, the row filter, resuming a dropped socket. DocSpec builds
  catalogs and acquires nothing, so none of that is duplicated.

What *was* being duplicated is the enumeration: spicy-regs re-lists the bucket on
every build and, until today, recorded none of the object identity it got back.
A receipt that said "streamed the 2026-06-30 opinions dump" had named a
filename, not a thing. Captures now pin the object by the publisher's own byte
size and last-modified stamp, and can be held against DocSpec's capture as a
*precondition* — which is the useful place to discover a re-cut file when
reading it costs 8.6 hours.

Checked today, live listing against DocSpec's 04:47Z capture:

| | Live | DocSpec capture |
|---|---:|---:|
| objects enumerated | 1,076 | 1,076 |
| `opinions-2026-06-30.csv.bz2` | 54,561,543,156 B, `2026-06-30T09:56:48Z` | identical |
| `dockets-2026-06-30.csv.bz2` | 5,014,469,248 B, `2026-06-30T09:03:56Z` | identical |
| `opinion-clusters-2026-06-30.csv.bz2` | 2,457,231,057 B, `2026-06-30T09:40:19Z` | identical |
| `courts-2026-06-30.csv.bz2` | 81,180 B, `2026-06-30T09:00:26Z` | identical |

Capture identity
`urn:docspec:courtlistener-bulk-capture:v1:ccf4cd25…4774d`, pins digest
`sha256:8c50ed18…a55cb`. The population has not moved since the capture, so the
8.6-hour pass is reading the object DocSpec pinned.

Two honest limits on that. Agreement here means "the listing has not changed
since 04:47Z today" — both sides read the same publisher, so it is not
independent evidence that the publisher is right. And spicy-regs still cannot
*import* DocSpec; consuming its catalog rather than re-listing the bucket is a
migration, not a patch, and is not attempted here.

## What the publisher offers

The listing holds **1,076 objects totalling 1,598.65 GiB across 46 datasets**.
The newest dump of every dataset is dated **2026-06-30**. Exact sizes for the
ones that matter here:

| Dataset | Compressed | Ratio | Decompressed (est.) | Single-pass stream |
|---|---:|---:|---:|---:|
| `opinions` | **50.814 GiB** (54,561,543,156 B) | 8.31x | ~422 GiB | **8.6 h** |
| `dockets` | 4.670 GiB (5,014,469,248 B) | 6.08x | ~28 GiB | 46 min |
| `opinion-clusters` | 2.288 GiB (2,457,231,057 B) | 4.48x | ~10 GiB | 23 min |
| `citation-map` | 0.490 GiB | — | — | 5 min |
| `parentheticals` | 0.268 GiB | — | — | 3 min |
| `citations` | 0.119 GiB | 13.72x | ~1.6 GiB | 72 s |
| `courts` | 81,180 B | 9.43x | ~765 KiB | instant |

Ratios are measured on the first megabytes of each dump, not assumed. Throughput
is **1.74–1.79 MiB/s on a single connection**, measured three independent ways
(raw `curl` range read, streamed decompression, and the real ingest). The bucket
does not go faster for one client; the ingest takes that as given rather than
opening parallel connections against a service that gives its data away free.

**The cap is per client, not per connection.** Two concurrent streams (the
`opinions` pass and the `dockets` pass, run side by side on 2026-08-22) settled
at **1.03 and 0.74 MiB/s — 1.77 MiB/s together**, which is the same number one
connection gets alone. So parallelism buys nothing here, and the total cost of
reading two dumps is the sum of their sizes divided by 1.77 MiB/s no matter how
they are scheduled. That is worth knowing before anyone tries to make the 8.6
hours shorter by opening sockets.

## The access facts that shaped every bound

Verified 2026-08-22, keyless:

| Endpoint | Status |
|---|---|
| `/api/rest/v4/search/?type=r` (dockets) | **200** |
| `/api/rest/v4/search/?type=o` (opinion clusters) | **200** |
| `/api/rest/v4/search/?type=rd` (RECAP documents) | **200** |
| `/api/rest/v4/courts/` | **200** |
| `/api/rest/v4/opinions/` | **401** |
| `/api/rest/v4/clusters/` | **401** |
| `/api/rest/v4/recap-documents/` | **401** |
| `/api/rest/v4/recap-query/` | **401** |

This corrects a premise the work started from. The REST opinions endpoint does
**not** serve `html_with_citations` / `plain_text` keylessly — it does not answer
keylessly at all. `/search/?type=o` is keyless but returns only a `snippet`, not
a body. **The bulk dumps are therefore the sole keyless source of opinion text**,
which is why the ingest is bulk-first by necessity rather than by preference, and
why the 50.8 GiB figure above governs everything downstream.

No `COURTLISTENER_API_TOKEN` is configured on this machine, so every number here
is a keyless number.

## What was ingested, with exact bounds

Disk headroom at session start was **118 GiB free against a 100 GiB floor** — 18
GiB of usable room, against an `opinions` dump that is 50.8 GiB compressed before
it is decompressed at all. That single comparison decided the shape of the whole
ingest.

| Table | Bound | Result |
|---|---|---|
| `court_opinion_clusters` | **full** `opinion-clusters` dump, 2026-06-30, streamed in 23 min | **10,070,727 rows**, 3.94 GB parquet |
| `court_opinion_bodies` | **250,000 opinions**, read from 1.242 GiB of the 2026-06-30 `opinions` dump | 250,000 rows, 1.74 GB parquet |
| `courts` (reference read) | full dump | 3,361 courts — 397 federal (127 `F` appellate, 125 `FD` district, 95 `FB` bankruptcy, 8 `FBP` bankruptcy appellate panel, 42 `FS` special), 2,618 `ST` state |

The `opinions` bound is the honest one to argue about, so here it is precisely.
250,000 opinions came out of 1.242 GiB of a 50.814 GiB dump, which puts the dump
at roughly **10.23M opinions** and this slice at **2.44%** of it.

The slice is a cross-section, not a prefix: ingested `opinion_id` values run from
**29 to 11,338,571**, essentially the full width of the corpus, because the dump
is not ordered by id. That makes the sample usable and representative. It does
not make it a backfill.

### The full backfill: implemented, costed, not run

`build_court_opinion_bodies` runs unbounded, and `check_headroom` refuses it
before a byte moves when the arithmetic does not work. On this machine it does
not work:

* **Disk.** Landing the compressed dump alone takes 118 GiB free down to 67 GiB,
  below the 100 GiB floor. Streaming avoids landing it, but the output table
  would be roughly 45–55 GiB of parquet at the measured bytes-per-opinion.
* **Time.** 8.6 hours of continuous transfer at the bucket's observed rate.

That is a scheduled-job cost on a machine with room, not a workstation cost. It
is refused here and recorded rather than half-attempted.

## Coverage verification

### 1. Bulk enumeration vs ingested rows

| Dataset | Publisher offers | Ingested | Coverage |
|---|---:|---:|---:|
| `opinion-clusters` 2026-06-30 | 10,070,727 rows (whole file read) | 10,070,727 | **100%** |
| `opinions` 2026-06-30 | ~10.23M rows (derived: 250,000 rows per 1.242 GiB over 50.814 GiB) | 250,000 | **2.44%** |
| `courts` 2026-06-30 | 3,361 rows | 3,361 | **100%** |
| `dockets` 2026-06-30 | not ingested | 0 | 0% — superseded by `court_dockets`, which is scoped to APA suits |

The clusters denominator is the dump's own row count, established by reading the
whole file rather than trusting a published figure, because CourtListener does
not publish row counts.

### 1a. What "has text" actually means

Judging opinion-text coverage by `plain_text` would have been wrong by a factor
of four, which is the single most useful thing this ingest measured:

| Body column | Rows populated (of 250,000) |
|---|---:|
| **any** text rendering | 249,988 (**100.0%**) |
| `html_with_citations` | 249,670 (**99.9%**) |
| `plain_text` | 55,335 (**22.1%**) |

CourtListener stores whichever rendering the upstream source supplied, and
`html_with_citations` is the one it computes for nearly everything. `plain_text`
is populated for under a quarter of opinions. A consumer that treated a null
`plain_text` as "no text available" would discard 78% of a corpus that is in fact
99.9% covered. That is why the table carries `available_text_fields` and
`text_char_count`: they make "the publisher holds no text" distinguishable from
"the publisher holds text, in a different column." The commonest combinations:

| `available_text_fields` | Rows |
|---|---:|
| `html_with_citations,xml_harvard` | 131,824 |
| `plain_text,html_with_citations` | 48,872 |
| `html_anon_2020,html_with_citations` | 28,631 |
| `html_lawbox,html_with_citations,xml_harvard` | 12,991 |

By role, the slice is 120,258 lead opinions, 96,293 combined, 13,290 trial-court,
10,182 dissents and 6,735 concurrences — so separately-authored opinions do come
through as their own rows, which is the grain the table promises.

### 2. APA docket set vs decisions matched

The join this expansion existed to create works, and it is far sparser than the
docket count suggests. Both facts are load-bearing.

| Measure | Count | Share |
|---|---:|---:|
| clusters ingested | 10,070,727 | — |
| clusters carrying a `cl_docket_id` | 10,070,727 | **100%** |
| APA dockets in `court_dockets` | 7,698 | — |
| **APA dockets with at least one decision** | **759** | **9.9%** |
| APA dockets with no decision at all | 6,939 | 90.1% |
| clusters sitting on an APA docket | 1,155 | 0.011% of all clusters |

**90% of the APA docket set has no decision attached** — and the explanation
first given for that was substantially wrong, so it is worth reading the
correction rather than the original.

The original reading: `court_dockets` is RECAP, sourced from PACER, and records
that a suit exists; opinion clusters are sourced from court-website scrapers and
reporters, and record that a decision was *published*. A challenge that settled,
was dismissed, or ended in an unpublished order leaves a docket and no cluster.
That mechanism is real and accounts for much of the 90%.

What it does not account for is **D.D.C.**

### 2a. The D.D.C. anomaly: same case, two docket records

Once every cluster could say which court decided it (gap 8), the per-court
decision rates became checkable, and one of them is not like the others:

| Court | APA dockets | With ≥1 decision | Rate |
|---|---:|---:|---:|
| `dcd` (D.D.C.) | **1,571** | **0** | **0.0%** |
| `nysd` (S.D.N.Y.) | 259 | 44 | 17.0% |
| `cand` (N.D. Cal.) | 351 | 53 | 15.1% |

D.D.C. is the **largest APA venue in the set** — 1,571 of 7,698 dockets, 20.4% —
and it is the classic forum for agency-review litigation. A genuine 0.0%
published-decision rate there, while its peers run 15–17%, is not credible.

It is not a broken id, either: 1,516 of those 1,571 docket ids are present in the
`dockets` dump and agree that the court is `dcd`. And D.D.C. is not missing from
the corpus — **46,581 clusters sit on a D.D.C. docket.** The decisions exist.
They are attached to a *different docket record for the same case*.

CourtListener carries duplicate dockets: a RECAP/PACER-sourced one, which is
what a `nature_of_suit=899` search returns, and a scraper-sourced one, which is
what the opinion cluster hangs off. For most districts these coincide. For
D.D.C. they frequently do not:

| Case | RECAP docket | Cluster's docket |
|---|---:|---:|
| `HALBIG v. SEBELIUS` | 4,211,989 | 120,436 |
| `PUBLIC CITIZEN HEALTH RESEARCH GROUP v. ACOSTA` | 14,523,291 | 16,255,289 |
| `BLUE CROSS AND BLUE SHIELD OF FLORIDA v. DEPT…` | 69,500,499 | 70,282,519 |
| `NORTH AMERICA'S BUILDING TRADES UNIONS v. DEPT…` | 69,864,612 | 70,284,623 |

**Conservatively, at least 249 of the 6,939 "no decision" APA dockets do have a
decision in this corpus**, reachable only through a different docket id — 184 of
them D.D.C. That count requires a distinctive case name (≥30 characters), unique
on both sides within the court, and a decision not predating the filing. Loosen
the name match and D.D.C. alone yields 499, so 249 is a floor and not an
estimate.

*The remedy, not applied here:* the `dockets` dump carries `docket_number`, and
two docket records for one case share it within a court. A `(court_id,
docket_number)` join is exact where case-name matching is fuzzy, and capturing
that column costs nothing extra because the dump is already being read for
`court_id`. It is **deliberately not** folded into the in-flight APA capture: a
fuzzy join has no business entering a capture silently, and restarting an
8.6-hour pass at 10% to add ~250 clusters is a bad trade.

So the ceiling is *partly* an upstream property and *partly* a join this ingest
cannot yet make. Both halves are real; only the first was recorded before.

Two smaller numbers worth stating rather than rounding away:

* **17** of the 250,000 ingested opinion bodies land on an APA docket. That is
  the arithmetic working exactly as it must — APA clusters are 0.011% of all
  clusters, so a 2.44% sample would be expected to catch roughly 28 of the 1,155,
  and it caught 17. It is not a defect; it is what a uniform sample of a corpus
  buys you when the target is one part in ten thousand. **Targeting the APA set
  specifically is a `cluster_ids` filter over the same dump**, which the builder
  already supports — the cost is the full 8.6-hour pass, not new code.
* **All 250,000 ingested opinions resolve to a cluster in the cluster table.
  There are no orphans.** An earlier draft of this section reported 12,666
  (5.1%) that did not, "most likely blocked or withdrawn clusters the two
  exports treat differently", and recorded it as unexplained. That was an error
  in this document, not a property of the data, and it is worth spelling out
  because the shape of it is instructive.

  The number came from subtracting **237,334 distinct clusters** from **250,000
  opinion rows** — a count of clusters taken away from a count of opinions. The
  two differ because opinions are not one per cluster:

  | Opinions in the cluster | Clusters |
  |---|---:|
  | 1 | 226,487 |
  | 2 | 9,466 |
  | 3 | 1,061 |
  | 4 | 229 |
  | 5 | 73 |
  | 6 | 11 |
  | 7 | 5 |
  | 8 | 2 |

  The siblings beyond the first come to 9,466 + 2,122 + 687 + 292 + 55 + 30 + 14
  = **12,666 exactly**. They are the separately-authored concurrences and
  dissents that section 1a two pages up celebrates as "the grain the table
  promises". The same fact was written down twice, once as a feature and once as
  a defect.

  Re-measured with rows on both sides: **250,000 of 250,000 opinions (100%)
  resolve; 0 do not.** `scripts/verify_court_coverage.py` now reports
  `opinions_not_resolving_to_a_cluster` alongside the distinct-cluster count and
  names `sibling_opinions_sharing_a_cluster` as the reason they differ, and a
  test pins both directions — siblings must not read as orphans, and a genuine
  orphan must still be seen.

### 3. Supreme Court term index vs captured opinions

The Court's own index is the denominator:

| Term | Slip opinions on the index |
|---|---:|
| OT2021 | 66 |
| OT2022 | 58 |
| OT2023 | 60 |
| OT2024 | 67 |
| OT2025 | 68 |
| **total OT2021–OT2025** | **319** |

**`court_opinions.parquet` returns 404 from R2.** The SCOTUS ingest exists, is
tested, and has a rollup, a workflow, and a data-dictionary entry — but its table
has never been published. Coverage against those 319 opinions is therefore **0%
published**, and the only copies on this machine are 10-row samples inside sealed
corpus artifacts. This was already visible in the freshness config, where
`court_opinions` sits in `SKIPPED` for a different stated reason ("seasonal"),
which masks the real one. That string is now corrected to say the table is
unpublished.

**And it is not the only one.** Checked against R2 directly on 2026-08-22:

| Table | R2 |
|---|---|
| `court_dockets.parquet` | **200**, 1,450,249 B, 7,698 rows |
| `court_opinions.parquet` | **404** |
| `court_opinion_clusters.parquet` | **404** |
| `court_opinion_bodies.parquet` | **404** |

So three of the four court tables are local-only. `court_opinion_clusters` and
`court_opinion_bodies` at least say so in the freshness config ("not yet
published to R2"); publishing all three is one decision, and it is not this
document's to make.

### 3a. The pre-2021 terms, measured rather than assumed

The gap list below said pre-2021 opinions were unreachable because
`parse_term_index` refused their URL shape. That is true of *some* of them and
the scope is smaller than it sounds in both directions. Measured against the
Court's own indexes:

| Term | Index rows | Slip PDFs | Rows pointing into a volume PDF |
|---|---:|---:|---:|
| ≤ OT2016 | **no slip index at all** — the URL redirects to `USReports.aspx` | — | — |
| OT2017 | 56 | 0 | 56 |
| OT2018 | 73 | 0 | 73 |
| OT2019 | 63 | 0 | 63 |
| OT2020 | 68 | **53** | 15 |
| OT2021+ | 66 | 66 | 0 |

Two corrections fall out of that table.

* **OT2016 and earlier are not a parser gap.** The Court does not publish a slip
  opinion index for them; the URL redirects to the bound-volume page. Reaching
  those terms means reading a different source, not adding a branch.
* **OT2020 was four-fifths parseable already.** 53 of its 68 rows are ordinary
  `/opinions/20pdf/` slip PDFs. One refused row raised for the whole term, so
  the term produced nothing and looked wholly unreachable.

The 207 volume rows point into **16 distinct PDFs** totalling 43.7 MiB — a
preliminary print or bound volume of the *U.S. Reports*, with the opinion
located by a `#page=N` fragment. Of those 16, **4 answer 404 at the Court's own
URL** (`584US1PP_final`, `584US2PP_final`, `585US1PP_final`, `585US2PP_final`),
covering **56 index rows**. That is a broken upstream link, not a fetch that can
be retried.

**So OT2017–OT2020 holds 260 index rows, of which 204 (78.5%) are reachable**
and 56 are not, for a reason nothing on this side can fix.

## Named gaps, and what closing each costs

1. **Opinion text is 2.44% covered.** ~9.98M of ~10.23M opinions unread.
   *Cost:* 8.6 h of single-connection streaming plus ~45–55 GiB of output. Needs
   a machine with ≥160 GiB free, or a partitioned run that publishes per-slice.
   No new code — remove the bound.

2. **`court_opinions` is unpublished** — and so are `court_opinion_clusters` and
   `court_opinion_bodies`. 319 Supreme Court opinions across OT2021–OT2025 exist
   upstream and zero are published.
   *Cost:* one `run-rollup-supreme-court-opinions --no-skip-upload`. Minutes.
   *Status:* **open, deliberately.** Publishing is a decision, not a task; the
   `SKIPPED` reason string is corrected in the meantime, because "seasonal" read
   as a policy when the truth was an absence.

3. ~~**OT2020 and earlier cannot be parsed at all.**~~ **Closed for OT2017–OT2020;
   OT2016 and earlier are a different source, not a parser branch.** See
   [3a](#3a-the-pre-2021-terms-measured-rather-than-assumed) for the measurements.
   `parse_term_index` now recognises `/opinions/preliminaryprint/` and
   `/opinions/boundvolumes/` alongside the slip-opinion path, refusing everything
   else exactly as before — including a volume link with no usable `#page=`
   anchor, because a row that names a volume and claims to be one opinion is the
   failure this branch exists to avoid.
   *What the URL shape alone would have bought:* a volume of the *U.S. Reports*
   stored as the text of each of its sixty opinions, each under a different case
   name and each wrong. So the anchor is carried through as `source_page_start`,
   closed against the next opinion in the same volume to give `source_page_end`,
   and the transform slices the text to that range. Anchors were checked against
   the real `591US2PP_web.pdf`: all twelve land on their opinion's first page,
   case name matching the index.
   *Also found while doing it:* the Court's site does not always serve
   `/opinions/slipopinion/{code}` the term that code names — a client that had
   already fetched one term got the **OT2023** index back from the OT2021 URL,
   sixty real opinions about to be stamped `term_year=2021`. That is silent
   mislabelling, not an error, and `parse_term_index` now refuses an index whose
   decisions fall outside the term's date window.
   *What was not done, and why:* **no end-to-end pre-2021 capture was run.**
   Partway through measuring, `supremecourt.gov` began answering `403 Access
   Denied` to this address — after roughly **80 requests over 25 minutes**,
   about three a minute, across two user agents. Requests are now spaced two
   seconds apart, but that is a mitigation and not a proof: the threshold is not
   published, was not isolated, and may not be request rate at all. A full
   OT2017–OT2025 capture is ~335 requests and **has never been run to
   completion from this machine** — not before this change either. The parser,
   the page-range slicing and the document caching are covered by hermetic
   tests and by anchors checked against the real `591US2PP_web.pdf`; the
   capture itself is still unproven at scale, and that is the honest state.

4. **RECAP documents are not captured, and are the expensive one.** There is **no
   `recap-documents` bulk dataset** — 46 datasets, and that is not among them —
   and `/recap-documents/` and `/recap-query/` are both 401. The only keyless
   path is `/search/?type=rd`.
   *Measured on 12 sampled APA dockets:* 11 have at least one RECAP entry, mean
   **47.7 documents per docket**, which projects to roughly **367,000 document
   rows** for the 7,698 dockets. Cost is **1.41 s per docket**, so **~3.0 hours
   for one page each** and materially more to paginate ~48 documents per docket —
   call it 6–9 hours keyless.
   The catch that decides it: only **4 of ~220 sampled document rows were marked
   `is_available`**, meaning the PDF is actually in RECAP. So ~6–9 hours buys
   mostly *docket-entry metadata*, and under 2% of it leads to a document.
   *Verdict:* not cheap, so not implemented. Worth revisiting only with an API
   token, and only if entry metadata alone is the goal.

5. **Only 17 opinion bodies land on an APA docket.** The sample is uniform; the
   target is 0.011% of the corpus. Nothing is wrong, but nobody should query
   `court_opinion_bodies` expecting APA coverage today.
   *Cost:* one targeted pass — `build_court_opinion_bodies(cluster_ids=...)` with
   the 1,155 APA cluster ids, which the builder already accepts. It still reads
   the whole 50.8 GiB dump to find them, so 8.6 hours; the *output* is tiny.
   This is the single highest-value follow-up.
   *Status:* **running.** `scripts/capture_apa_opinion_bodies.py` reproduces the
   target set (1,155 clusters over 759 dockets, joined from the published
   `court_dockets`) and streams the pass, writing a receipt with both inputs
   digest-pinned and coverage stated against the 1,155.
   Two things had to change before it could start, and neither was the filter.
   The disk guard charged every unbounded pass the dump's 50.8 GiB, which is the
   right stand-in when the output is the whole corpus and refuses a run that
   costs 141 MiB when it is not — the dump is streamed and never landed, so the
   volume pays for the parquet written. And a socket held open for 8.6 hours
   gets dropped, which used to end the run with nothing; the reader now resumes
   from the exact compressed offset with an HTTP `Range` (the bucket answers
   206, verified) and counts the resumes.
   *Where it is:* started 12:20 on 2026-08-22, detached, logging to
   `logs/apa-opinion-bodies-2026-08-22.log`, writing to
   `output/court-data-apa-2026-08-22/`. On completion it leaves
   `court_opinion_bodies.parquet` and `apa_opinion_bodies_receipt.json`;
   `source_pin.json` is already there, written out of band because the process
   predates the receipt carrying that block.
   **Read the receipt's `resumes` before trusting the output**: the running
   process loaded the reader *before* the guard that refuses a resume the
   server answered with a restarted stream rather than a range. `resumes: 0`
   makes that moot, and is the expected case. Anything else, rerun.
   *This slice does not replace the 250,000-row uniform sample* in
   `output/court-data-2026-08-22/` — they are complementary and deliberately
   left unmerged, because merging them runs the duckdb sort that already ran the
   4 GiB budget out of memory once.
   *Predicted result, written down before the pass finished so the receipt can
   be judged rather than admired:* **roughly 1,200–1,300 opinion rows over close
   to all 1,155 target clusters.** From the pass in flight, 73 kept out of
   700,000 scanned at 2.92 GiB; the same ratio over the whole dump is ~1,270,
   and 1,155 clusters at the measured 1.05 opinions per cluster is ~1,213. A
   result far below that means the filter or the target set is wrong, not that
   the APA docket set is emptier than section 2 says. Note also that the first
   250,000 rows yielded 17 — the same 17 the bounded 2026-08-22 run found, from
   the same 1.24 GiB, which is a free reproducibility check on the reader.
   *A qualification the capture cannot fix:* **the 1,155-cluster target set is an
   undercount.** It is every cluster reachable from an APA docket *by docket id*,
   and [2a](#2a-the-ddc-anomaly-same-case-two-docket-records) shows at least 249
   more APA decisions exist in this corpus behind a duplicate docket record, 184
   of them D.D.C. The capture's coverage should therefore be read as a fraction
   of *the clusters the docket-id join can see*, which is what its receipt says,
   and not as a fraction of APA decisions in the corpus. Widening the target set
   needs the `(court_id, docket_number)` join described in gap 7.

6. ~~**12,666 ingested opinions (5.1%) name a cluster the cluster dump does not
   contain.**~~ **Closed: there are none.** All 250,000 resolve. The 12,666 was
   this document subtracting a cluster count from an opinion count; the
   remainder is exactly the sibling concurrences and dissents. The arithmetic is
   in [section 2](#2-apa-docket-set-vs-decisions-matched). *No reconciliation
   against the publisher was needed, because there was nothing to reconcile.*

7. **90.1% of APA dockets have no decision.** ~~This is an upstream property, not
   a coverage failure.~~ **Partly wrong — reopened.** The upstream mechanism is
   real and explains most of the 90%, but not all of it: at least **249 of the
   6,939** do have a decision in this corpus, attached to a *duplicate docket
   record for the same case*. D.D.C. is the concentration — 1,571 APA dockets,
   the largest venue in the set, **0.0%** matched against peers at 15–17% — and
   its 46,581 clusters are all hanging off scraper-sourced dockets rather than
   the RECAP ones a nature-of-suit search returns. See
   [2a](#2a-the-ddc-anomaly-same-case-two-docket-records).
   *Cost to close:* capture `docket_number` alongside `court_id` in the
   docket→court map — free, since the dump is already being read for the court —
   and join on `(court_id, docket_number)`, which is exact where the case-name
   match used to diagnose this is fuzzy. Not done here, and deliberately kept out
   of the in-flight capture.
   *What is still upstream:* the remainder. Most district-court APA suits really
   do end without a published opinion, and no join recovers those.

8. ~~**Clusters are the whole corpus, not just federal.**~~ **Closed.**
   `court_opinion_clusters` now carries `court_id`, `court_jurisdiction` and
   `court_is_federal`, resolved from the `dockets` dump through `cl_docket_id`.
   Federal means the publisher's jurisdiction code begins with `F`, which
   reproduces the 397 in the `courts` dump exactly; the raw code is published
   next to the boolean so a consumer who disagrees can reclassify without
   re-reading 4.67 GiB, and a court the dump does not describe is NULL rather
   than `f` — this table is the whole corpus, so absence of evidence is recorded
   as absence.
   *What it cost:* the `dockets` read is one pass for two columns. Checked
   against the publisher's listing of 46 datasets first: **there is no smaller
   published docket→court map**, so the 4.67 GiB is the cheapest form the answer
   comes in. **Measured: 71,677,647 dockets, 4.67 GiB read, 0 resumes, 111
   minutes at 0.72 MiB/s** — not the predicted 46, because it shared the bucket's
   per-client cap with the gap-5 pass the whole way. Output 317.1 MiB.
   *What it found:*

   | | Clusters | Share |
   |---|---:|---:|
   | placed in a court | **10,070,727** | **100.0%** |
   | in a federal court | **3,397,753** | **33.7%** |
   | not federal | 6,672,974 | 66.3% |
   | **no court resolvable** | **0** | **0.0%** |

   Federal splits `F` 1,905,145 / `FD` 1,197,191 / `FS` 216,549 / `FB` 72,078 /
   `FBP` 6,790. The largest single bucket in the whole corpus is `SA` (state
   appellate) at 3,719,508, then `S` at 2,473,927 — which is the point: **two
   thirds of `court_opinion_clusters` is state-court output**, and until now
   there was no way to say so, let alone exclude it.

   *Zero unresolvable is the number worth pausing on.* Every one of the ten
   million clusters names a docket, and every one of those dockets is in the
   71.7M-row `dockets` dump. Given that this same document once reported 12,666
   phantom orphans on the opinions side, a genuine 100% here was checked rather
   than assumed, and it holds.
   *And an independent correctness check fell out of it:* all **1,155** APA
   clusters classify as `FD`, federal district — 100%, no exceptions. An APA
   docket set derived from a `nature_of_suit=899` RECAP search *should* be
   entirely federal district, so a join that placed any of them in a state court
   would be visibly wrong. None are. Top venues: `cand` 126, `nysd` 75, `mdd` 55,
   `mad` 50, `txnd` 50. The venue that is *missing* from that list is what opened
   [2a](#2a-the-ddc-anomaly-same-case-two-docket-records).
   *Artifacts:* `output/court-data-2026-08-22/court_cluster_scope.parquet` plus
   `cluster_court_scope_receipt.json`, and the map with its own receipt. Written
   as a side table rather than folded into `court_opinion_clusters.parquet`
   because the rewrite needs a second copy of a 3.9 GB file and free space is
   below the 100 GiB floor — and because another pass is currently digesting
   that exact file.
   *The design constraint worth recording:* the join happens while each row is
   shaped, not afterwards. A duckdb join of ten million clusters against
   seventy-odd million dockets would rewrite the whole 3.9 GB table, and the
   first build's promote path exists precisely because this machine cannot hold
   two copies of it. So the map is a dense array — one `unsigned short` per
   docket id indexing a 3,361-entry court vocabulary, about 150 MB — rather than
   seventy million Python string keys, which is gigabytes before a single value.
   *What it costs from now on:* a scheduled cluster run reads two dumps instead
   of one, about 70 minutes rather than 23. The map is cached by dump date, and
   `skip_court_scope` opts out and leaves the columns NULL.

9. **The search catch-up did not run.** `CourtListenerOpinionSearchReader` is
   implemented and tested, but a local run was skipped: the window from the
   2026-06-30 dump to today spans every court, and keyless cursor pagination over
   it is a scheduled-job cost. Decisions filed after 2026-06-23 are therefore
   absent from `court_opinion_clusters`.
   *Cost:* it already runs in the workflow. Nothing to build.

## One defect found and fixed

The dumps escape an embedded quote as `\"`, not as the doubled `""` the stdlib
CSV dialect assumes. That does not raise — it **desyncs**, silently. Measured on
the first 3,000 rows of the 2026-06-30 `opinion-clusters` dump:

| Dialect | Rows whose `id` is a prose fragment | Rows retaining `docket_id` |
|---|---:|---:|
| stdlib default | **1,987 / 3,000** | 1,001 / 3,000 |
| `escapechar='\\'` | **0 / 3,000** | 3,000 / 3,000 |

Two thirds of the join column — the one this entire ingest exists to create —
would have gone missing in a way indistinguishable from CourtListener not having
the data. It surfaced only because duckdb refused to cast
`'<author id=\"b1326-17\">'` to an opinion id. Fixed in `6c7a654` and pinned by a
regression test. Worth stating plainly: had the merge step been more forgiving,
this would have shipped as a coverage number that was merely wrong.

## What the second pass changed

Gaps 3, 5, 6 and 8 were worked on 2026-08-22 after this document was first
written. Three of its findings did not survive contact.

| Was recorded as | Actually |
|---|---|
| 12,666 opinions (5.1%) orphaned, cause unexplained | **Zero orphaned.** The figure was a cluster count subtracted from an opinion count; the remainder is exactly the sibling concurrences and dissents |
| 90.1% of APA dockets have no decision — "an upstream property, not a coverage failure" | **Partly a coverage failure.** At least 249 of the 6,939 have a decision here, behind a duplicate docket record — 184 of them D.D.C., a venue measured at **0.0%** against peers at 15–17% |
| Pre-2021 unreachable for want of a URL shape | **OT2017–OT2020**: 204 of 260 rows reachable once the volume layout is read, 56 blocked by 404s at the Court's own URLs. **OT2016 and earlier**: no slip index exists — a different source, not a branch |
| `court_opinions` unpublished | **Three** of the four court tables are unpublished; `court_opinion_clusters` and `court_opinion_bodies` are 404 on R2 too |

The D.D.C. one is the entry that most deserves attention, because of *how* it
was found. Nobody went looking for it. Gap 8 was closed for an unrelated reason —
"restrict decisions to federal courts" — and giving every cluster a `court_id`
made per-court decision rates computable for the first time, at which point a
0.0% sitting beside two 15–17%s was impossible to miss. The verdict it overturned
had been reached by reasoning about how the two upstream sources differ, which
was a correct mechanism applied to a number it did not fully explain. A plausible
mechanism is not a measurement, and this document had been treating one as the
other.

Three findings are new, and each is the kind that produces wrong data rather
than an error:

* **The Court's site serves the wrong term.** A client that had already fetched
  one term got the OT2023 index back from `/opinions/slipopinion/21`. The rows
  parse cleanly and `term_year` comes from the caller, so the table would simply
  have said 2021 about sixty OT2023 opinions. Now refused by a date-window check.
* **The Court's site blocks**, answering `403 Access Denied` after ~80 requests
  in 25 minutes — about three a minute — across two user agents. A full
  OT2017–OT2025 run is ~335 requests, so a capture at any pace is in doubt until
  someone runs one. Requests are now spaced two seconds apart as a mitigation,
  not as a solution.
* **The bucket's throughput cap is per client, not per connection.** Two
  concurrent streams totalled 1.77 MiB/s, the same as one alone.

And one methodological note, because it is the second time the same shape of
error has bitten this ingest. The `\"` CSV desync corrupted data without
raising; the 12,666 was a unit mismatch that produced a plausible number without
raising. Both were caught by something refusing to accept a value, not by a
check that was looking for them. The coverage script now reports the orphan
measure with rows on both sides of the comparison, which is the smallest change
that makes the mistake impossible to repeat by reading.

### Denominator note: how many opinions the dump actually holds

The ~10.23M figure above is extrapolated from the first 1.242 GiB, and rows are
not uniformly sized through the dump. Measured on the gap-5 pass as it ran:

| Compressed read | Rows scanned | Implied dump total |
|---:|---:|---:|
| 0.35 GiB | 50,000 | 7.26M |
| 0.61 GiB | 100,000 | 8.33M |
| 1.06 GiB | 200,000 | 9.59M |
| 1.24 GiB | 250,000 | 10.24M |

The estimate is still climbing at the point the original sample stopped, so
**10.23M is a floor derived from the first 2.4%, not a measurement**. The
gap-5 pass reads the whole file and will settle it exactly.
