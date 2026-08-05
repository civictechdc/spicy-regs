# Scale architecture report — 2026-08-04

A code-and-artifact survey of the activated pre-v3 path and the locally
implemented `DocumentRelease` v3 path: acquisition, storage, processing,
distribution, measured ceilings, and stated intent. This is an analysis, not a
receipted run record — line references are to the working tree on 2026-08-04
and drift with the code. Prepared during the SpicySearch millions-of-documents
scaling assessment (see
`spicysearch/docs/history/2026-08-04-pipeline-review/`), which consumes this
repository's `DocumentRelease` distributions.

**Status boundary.** The measurements in this report come from the existing v2
pipeline and its real artifacts. The v3 writer, verifier, maintenance commands,
and local task executor are implemented and covered by repository fixtures and
focused tests. They have not been activated, published as a real release, or
measured against the production-scale corpus. Code completion does not make the
v3 path an operational replacement for v2.

## 1. Acquisition

**Publisher acquisition uses synchronous `Reader` generators.** The source
layer has 17 concrete readers across 15 publisher modules (federal_register,
mirrulations, cfr_sections, congress_bills, courtlistener, fcc_ecfs,
uscode_olrc, …); auxiliary modules handle storage, PDFs, and derived text.
Each reader yields raw dicts (`sources/base.py:1-39`).

- **Federal Register**: date-window walk, not naive pagination. `PER_PAGE=1000`,
  `RESULT_CAP=10_000`, `MAX_WINDOW_DAYS=90`, epoch `1994-01-01`
  (`sources/federal_register.py:36-50`). Truncated windows recursively bisect
  down to a single day and *raise* rather than silently drop
  (`federal_register.py:129-152`). Incremental: caller passes `since` = max
  published date already stored (`federal_register.py:88-93`).
- **Rate limiting is per-source and ad hoc, not centralized.** Federal Register,
  CFR, Congress, CRS, and ECFS each hand-roll `_MAX_RETRIES=5` with
  `backoff = min(2**attempt, 30)`; CourtListener uses six attempts and a
  60-second cap (`federal_register.py:175-191`, `cfr_sections.py:217-233`,
  `courtlistener.py:145-162`). Only the body-corpus fetcher has an actual
  throttle: `DEFAULT_MIN_INTERVAL_SECONDS = 1.2` plus a hard `max_requests`
  budget that raises (`corpora/body_retrieval_corpus.py:100`, `:587-657`).
- **Mirrulations (regulations.gov mirror)** is anonymous S3,
  `DEFAULT_DOWNLOAD_WORKERS = 16` threads per agency, `_PROGRESS_EVERY =
  25_000`, botocore `max_attempts: 5` (`sources/mirrulations.py:33-68`).
- **Checkpointing is a Bloom-filter manifest, not a watermark.**
  `manifest.parquet` stores processed source keys. At 30M keys and a false-
  positive target of 1e-7, the current 64-bit implementation allocates about
  240 MiB: `array('L')` uses eight-byte words while the bit indexing and
  `size_bytes` property assume four, so the property reports only about 120
  MiB. The allocation remains far below a Python set reported at ~5 GB for 27M
  strings, but the source comments' 34 MB and 150x claims are false (`manifest.py:13-18`,
  `:37-56`, `:75-77`). **Known accepted behavior:** a false positive is
  *sticky* — the key is skipped forever until `--full-refresh`
  (`manifest.py:14-18`, `:38-44`). Transient download failures are deliberately
  excluded from the manifest so they retry; parse failures are marked processed
  and dumped to `failed_keys.parquet` (`manifest.py:112-144`,
  `pipelines/regulations.py:140-154`).
- **Batching for scale-out is agency-sharded.** `stage_agencies` fans agencies
  across `ThreadPoolExecutor(max_workers=4)` (`pipelines/staging.py:48-104`).
  CI splits ~327 agencies into 15 batches of 22 via a GH Actions matrix,
  `max-parallel: 1`, `timeout-minutes: 60`, two redundant daily crons
  (`.github/workflows/etl-new-pipeline.yml:32-45`, `:143`, `:201`). That file
  records a real operational scale failure: 15 separate hourly crons were
  dropped by GH Actions so 12 agencies (VA, USPS, USTR…) were **never
  ingested** and HHS ran ~5 days stale (`etl-new-pipeline.yml:14-36`).
- **Comments have a memory escape hatch:** `--chunk-size` ingests one agency's
  comments in bounded key-chunks, writing each through the Iceberg
  DELETE+INSERT path, "which would otherwise buffer every record and OOM"
  (`pipelines/regulations.py:100-110`, `:193-201`).

## 2. Storage

**Three distinct regimes, no single store.**

1. **Published corpus = one Parquet file per table, mutable in place.**
   `merge_staging_files` writes `{data_type}_merged.parquet` then
   `output_file.unlink(); temp.rename(output_file)` — whole-file rewrite each
   run, DuckDB `memory_limit='4GB'`, `threads=2`, spill to `output/.duckdb_tmp`
   (`transforms/merge_staging_files.py:78-119`).
2. **Comments are too big for that** — "tens of millions of rows, so there is
   no monolithic `comments.parquet` snapshot" (`sources/iceberg.py:22-27`).
   They are Hive-partitioned
   (`comments/agency_code=…/docket_id=…/year=…/month=…`) and streamed
   batch-by-batch to avoid holding "the full 24.7M-row table"
   (`transforms/merge_comments_partitioned.py:8-26`), or written into an
   **Apache Iceberg table on R2 Data Catalog** through DELETE+INSERT. R2 does
   not reliably apply the DELETE, so reads deduplicate by `comment_id` and an
   out-of-band rebuild reclaims physical duplicates
   (`sources/iceberg.py:134-161`).
3. **Experiment/release outputs are content-addressed, immutable directories**
   under `output/`. Renditions are stored at `renditions/<sha256><suffix>`
   (`document_file_pipeline.py:226-229`); receipts at `receipts/<digest>.json`
   (`document_file_pipeline.py:837`, `:1139`).

**Actual sizes on disk (top level, 2026-08-04):** `output/` = **14 G** across
71 visible run directories plus 8 hidden work directories; `RefSpec/` (submodule)
7.7 G; `frontend/` 699 M; `etl/` 898 M. Largest runs:
`body-retrieval-corpus-2026-08-02` 5.3 G,
`fused-concept-registry-v1` 3.1 G, `vocabulary-atlas` 1.2 G,
`elsst-r5-r6-*-gate` 743 M each. Largest single files: two 1.5 GB `.npz` dense
indexes, `document-release.json` 1.4 G, `document-release-xml.json` 765 M. A
typical corpus run (`mixed-real-data-corpus-v2`) is 139 M: 21 Parquet files and
two JSON control files, with `records.parquet` the largest at 60 MB.

R2 upload has a shrink guard added "after the March 2026 incident" — refuses
an upload that would catastrophically shrink the remote (`sources/r2.py:10-12`,
`:118-149`), and uploads with `ThreadPoolExecutor(max_workers=len(files_to_upload))`
(`sources/r2.py:245`).

## 3. Processing

**The activated v2 docpipeline is single-threaded and whole-corpus-in-memory.**
Before the new executor was added, a grep for
`ThreadPool|ProcessPool|multiprocessing|asyncio|concurrent.futures|threading`
across `src/spicy_regs/docpipeline/`, `src/spicy_regs/corpora/`,
`document_release.py`, `document_file_pipeline.py` returned **zero hits**. All
activated concurrency lives in the ETL half (`pipelines/staging.py`,
`sources/mirrulations.py`, `sources/r2.py`, `enrich_pdf.py`,
`backfill_derived_text.py`).

- `segment_artifacts` is a plain `for artifact in artifacts` loop accumulating
  `outcomes: list[SegmentOutcome]` for the whole corpus
  (`docpipeline/segments.py:887-917`). Failures are captured as
  `state="failed"` outcomes rather than aborting.
- `write_segment_table` / `write_table` take `Sequence[...]` of all rows and
  build one `pa.Table.from_pydict` (`docpipeline/segments.py:1157`,
  `docpipeline/source.py:2536`).
- **Incremental processing is per-work-item, checkpoint-driven.** `execute_run`
  reuses any item whose durable state is in `SETTLED_ITEM_STATES` and retries
  the rest (`docpipeline/runtime.py:1282-1348`, `:69-76`). The store is an
  append-only JSONL `WorkCheckpoint` with torn-tail repair
  (`runtime.py:603-716`).
- **Fetch-level incrementality:** `fetch_bodies` skips any document whose
  cached bytes still match its receipt digest — "a failure at document 999
  does not discard 998 successes" (`corpora/body_retrieval_corpus.py:599-648`).
- **Cost that grows with corpus size regardless of what changed:**
  `file_inventory` SHA-256s *every file* in the run directory at finalize,
  then `scan_tree_for_secrets` reads the whole tree again
  (`runtime.py:270-282`, `:1360-1362`). For the 5.3 GB body corpus this is a
  full re-read per run.
- **The local v3 executor defines a bounded replacement behavior, not an
  activated migration.** `docpipeline/executor.py` supplies stable task keys,
  finite classified retries, memory and temporary-disk admission, bounded
  in-flight work, attempt-scoped outputs, digest checks, and one conditional
  commit record. Its local thread-pool adapter proves those rules with fixtures;
  no maintained distributed adapter or legacy-pipeline integration exists yet.

## 4. Distribution

**The observed and activated DocumentRelease v2 is one monolithic canonical
JSON plus a content-addressed sidecar tree. It is not sharded or streamable.**

- `write_document_release` is `path.write_text(canonical_json(dict(release)) +
  "\n")` — the entire release serialized to a single Python `str` in memory
  (`document_release.py:2298-2308`). `canonical_json` is one `json.dumps`
  (`ontology/common.py:84-92`).
- The observed artifact:
  `output/body-retrieval-corpus-2026-08-02/distribution-xml/document-release.json`
  = **801,837,566 bytes**; the HTML sibling = **1,469,593,455 bytes**.
  Alongside them, `renditions/` holds 993 content-addressed files and
  `receipts/` one 678 KB receipt.
- **All rendition bytes are held in RAM before writing.**
  `_PreparedFileRelease.rendition_bytes: Mapping[str, bytes]` accumulates
  every document's payload (`document_file_pipeline.py:96-100`, `:1105-1123`),
  then `_publish_prepared_release` writes them out
  (`document_file_pipeline.py:1327-1332`).
- **The full release is canonicalized at least three times per publish:** once
  in `_prepare_*` (`validate_document_release`,
  `document_file_pipeline.py:1147`), once in `write_document_release`
  (`document_release.py:2306-2308`), once in
  `validate_document_release_distribution` which re-reads the file,
  re-serializes it, and compares strings (`document_file_pipeline.py:1225-1233`),
  and `_publish_prepared_release` then does `if validated != prepared.release`
  — a full deep-compare of the 801 MB object graph
  (`document_file_pipeline.py:1341-1346`).
- **Immutability discipline is strong and enforced.** Publish refuses a
  non-empty output dir: `"output directory is not empty"`
  (`document_file_pipeline.py:1324-1325`). Runs refuse to overwrite:
  `"refusing to overwrite an existing run directory"` (`runtime.py:1297-1298`).
  A run builds in a sibling `.{name}.work` dir and only
  `work_dir.replace(output_dir)` on pass (`runtime.py:933-936`, `:1418-1419`).
  Release identity is `release_digest = sha256(canonical_json(body))` with
  `release_id = urn:spicyregs:document-release:<hex>`
  (`document_release.py:11-13`, `:2291-2295`).
- **Contrast: the ETL half rebuilds `output/` in place**
  (`merge_staging_files.py:116-119`) — no versioned release, no immutability.
- **One sharded writer exists:** `write_model_input_segments` emits one JSON
  per text representation under `segments/<file_stem>.json` plus a sealed
  receipt (`docpipeline/document_release_segments.py:917-963`), also refusing
  a non-empty output dir.

**DocumentRelease v3 adds a bounded multi-file path in local code.** It does
not change the v2 evidence above.

- `document_release_v3_writer.py` reads a closed JSON Lines selection, writes
  bounded Parquet batches, groups members through global and partition
  manifests, and stores rendition bytes in size-limited packs with indexes. It
  writes into a temporary directory and exposes the release only after its own
  verifier passes.
- The release contains explicit current-document, document-version, passage,
  eligibility, source-disposition, failure, coverage, and change tables.
  `document_release_v3_verify.py` checks the complete declared membership,
  digests, schemas, identities, foreign keys, evidence links, coordinates, and
  predecessor reconciliation with a configured DuckDB memory limit.
- `spicy-regs document-release-v3` now provides `build`, `verify`, `diff`, and
  `compact` commands. The checked-in fixture is a complete sealed distribution
  used by independent consumer tests. It is executable conformance evidence,
  not a production publication or a scale result.

## 5. Scale evidence

- **Largest structured corpus: 708,367 records / 147 MiB** from 18 tables,
  bound over **6,168,517 real public records**, with 309,210 pair expectations
  (`docs/mixed-real-data-corpus-report.md:16-26`; receipt
  `docs/evidence/mixed-real-data-corpus-2026-07-24/corpus-receipt.json:152`).
  On disk as `output/mixed-real-data-corpus-v1|v2|v2-rerun`, 139 M each.
- **Largest single source table: 1,004,233 rows** in
  `output/rulespec-stabilization-baseline-final/federal_register.parquet`,
  195,752 of them Rules/Proposed Rules — the doc notes "the draw space was
  ~12x larger than assumed"
  (`docs/evidence/body-retrieval-corpus-2026-08-02.md:36-40`).
- **Largest document-body corpus: 993 Federal Register documents**, median
  body 164,381 chars (`docs/evidence/body-retrieval-corpus-2026-08-02.md:22-27`).
- **Recorded end-to-end performance**
  (`docs/evidence/body-retrieval-corpus-followup-2026-08-02.md:31-45`):

  | build | docs | passages | wall | peak RSS |
  |---|---|---|---|---|
  | 993 HTML permissive | 993 | 726,009 | **3,274 s (54.6 min)** | **15.81 GB** |
  | 993 XML permissive | 993 | 333,363 | 990 s | **20.9 GB** |
  | 993 XML production policy | 520 | 159,179 | 566 s | 18.4 GB |

- **The explicit bottleneck complaint**, verbatim: *"Time is the scaling
  problem here, not memory. 20 documents to 993 is 50x the documents and 43x
  the passages, but 37 s to 3,274 s is **88x the time** — clearly superlinear…
  A corpus 10x this size would not fail for want of RAM on this machine; it
  would take most of a day."*
  (`docs/evidence/body-retrieval-corpus-followup-2026-08-02.md:41-45`,
  restated at `:255-261`). Materializing 1.47 GB of JSON costs 5.36 GB RSS — a
  ~3.6x Python object inflation (`:39-41`).
- **No streaming parser anywhere on the consumer path** — "no `ijson`, no
  incremental reader. The whole release becomes one Python object graph before
  any adapter runs" (`:20-23`). `docs/decisions.md:101` records ijson being
  adopted elsewhere "on an observed bulk-extract memory failure."
- Other numbers: 513,236-concept BM25 index built in 7.680 s
  (`TODO-RULE.md:129-132`); re-embedding all 513,236 concepts ≈ 50 minutes
  (`docs/evidence/usearch-ann-benchmark-2026-07-28.md:330`); 1.64 GB in-memory
  float matrix for the served concept space, linear in concept count (`:76`,
  `:385`); frozen testbed load ≈ 7 minutes (`:42`); ATF 982K comments, BLM
  850K, CDC 305K (`.github/workflows/etl-new-pipeline.yml:19-20`).
- Synthetic graph scale test to **1M edges**
  (`docs/evidence/graph-engine-bakeoff-2026-07-24/gen_scale.py`, results in
  `results.txt`), cited in
  `docs/superpowers/specs/2026-07-24-graph-engine-carrier-decision.md:12`.

## 6. Scale intent

- **The pre-v3 planning record contained essentially no scale intent.** `grep -in
  "million|scale|shard|throughput|bottleneck|OOM|memory"` over
  `product_goals.md` returns zero hits; over `TODO-RULE.md` also zero. Those
  durable planning files still do not define document sharding or incremental
  release behavior; the graph-engine record remains the only measured
  large-scale test. The local v3 implementation now expresses a technical
  direction, but it is not an accepted activation decision or a measured scale
  campaign.
- **Serving is explicitly deferred:** *"choose an online vector store, graph
  engine, GraphRAG layer, or public API only after the frozen evaluation
  proves a measured need"* (`TODO-RULE.md:440-442`). Retrieval, approval,
  publication, and the "frozen mixed-data release gate" are all listed as
  Deferred (`TODO-RULE.md:59-62`).
- The nearest thing to a scale intent is a cross-cutting goal: *"Change-detection
  feeds. Every corpus emits 'what's new since T'"* (`product_goals.md:418`) —
  delta *distribution*, not stated anywhere in code.
- The one recorded forward-looking scale item is a **downstream** work item:
  the lexical index build's superlinearity is flagged as "a spicysearch work
  item, not a corpus one" (`docs/evidence/body-retrieval-corpus-followup-2026-08-02.md:255-261`).
  (That downstream superlinearity was removed on 2026-08-04; see the
  SpicySearch pipeline-review campaign record.)

## Net limits, from the code

The activated v2 path still has whole-release-in-memory JSON with 3-4x
canonicalization, no document-pipeline parallelism, per-run full-tree hashing
and secret scanning, a sticky Bloom-filter false positive plus an oversized and
misreported bit array, unreliable Iceberg deletes that leave physical
duplicates for later rebuilding, DuckDB merges capped at
`memory_limit='4GB'`/`threads=2` doing full-file rewrites, and a 60-minute CI
timeout per agency batch.

The local v3 path removes the monolithic release representation, bounds its
writer and verifier, and defines recoverable concurrent task behavior. Its
remaining limits are operational: no real-data scale run, no activated producer
migration, no maintained distributed executor, and no published v3 release.
