"""Hermetic checks for the real-bodies retrieval corpus builder.

Every fixture here is synthetic. Nothing reads the real Federal Register
parquet and nothing touches the network, so these tests state what the tool
guarantees rather than what one particular build happened to contain.

The builder answers three questions the retrieval work needs:

1. **Draw.** Which Federal Register documents form a *topically coherent*
   corpus — one where vocabulary genuinely competes — and can that draw be
   measured before a single byte is fetched?
2. **Fetch.** Can ~1000 bodies be retrieved politely, resumably, and with a
   receipt for every request, so that a failure at document 999 does not
   discard 998 successes?
3. **Seal.** Is the lock file a reproducible boundary — byte-identical across
   rebuilds, and fail-closed when a cached body is tampered with?

The network fetch itself is not reproducible; the lock is. That boundary is
the same one ``corpora/segmentation_evaluation.py`` draws, and these tests
hold this module to it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from spicy_regs.corpora import body_retrieval_corpus as brc


# --------------------------------------------------------------------------
# synthetic draw inputs
# --------------------------------------------------------------------------


def _row(
    document_number: str,
    *,
    title: str = "Endangered and Threatened Wildlife and Plants; Listing",
    abstract: str = "We list the species as endangered under the Act.",
    document_type: str = "Rule",
    cfr: str = '[{"part": "17", "title": 50}]',
    topics: str = '["Endangered and threatened species"]',
    start_page: str = "100",
    end_page: str = "140",
    publication_date: str = "2020-05-05",
    agency_slugs: str = "fish-and-wildlife-service",
) -> dict[str, object]:
    return {
        "document_number": document_number,
        "title": title,
        "abstract": abstract,
        "document_type": document_type,
        "cfr_references_json": cfr,
        "topics_json": topics,
        "start_page": start_page,
        "end_page": end_page,
        "publication_date": publication_date,
        "agency_slugs": agency_slugs,
        "body_html_url": (
            f"https://www.federalregister.gov/documents/full_text/html/2020/05/05/{document_number}.html"
        ),
    }


DEFAULT_RULE = brc.DrawRule(
    cfr_title=50,
    cfr_parts=("17",),
    topic_substring="endangered",
    min_pages=12,
    min_year=2005,
    document_types=("Rule", "Proposed Rule"),
)


# --------------------------------------------------------------------------
# selection
# --------------------------------------------------------------------------


class TestSelection:
    def test_keeps_a_row_matching_every_clause(self) -> None:
        assert [r["document_number"] for r in brc.select_rows([_row("A")], DEFAULT_RULE)] == ["A"]

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("document_type", "Notice"),
            ("cfr", '[{"part": "52", "title": 40}]'),
            ("topics", '["Air pollution control"]'),
            ("end_page", "105"),
            ("publication_date", "1999-05-05"),
        ],
    )
    def test_drops_a_row_failing_any_single_clause(self, field: str, value: str) -> None:
        assert brc.select_rows([_row("A", **{field: value})], DEFAULT_RULE) == []

    def test_page_span_is_inclusive_so_the_floor_is_exact(self) -> None:
        """A 12-page document at min_pages=12 is kept; an 11-page one is not."""
        keep = _row("A", start_page="100", end_page="111")  # 12 pages
        drop = _row("B", start_page="100", end_page="110")  # 11 pages
        assert [r["document_number"] for r in brc.select_rows([keep, drop], DEFAULT_RULE)] == ["A"]

    def test_cfr_title_and_part_must_match_the_same_reference(self) -> None:
        """40 CFR 17 plus 50 CFR 52 must not satisfy a 50 CFR 17 rule."""
        row = _row("A", cfr='[{"part": "17", "title": 40}, {"part": "52", "title": 50}]')
        assert brc.select_rows([row], DEFAULT_RULE) == []

    def test_unparseable_cfr_json_drops_the_row_rather_than_raising(self) -> None:
        assert brc.select_rows([_row("A", cfr="{not json")], DEFAULT_RULE) == []

    def test_selection_is_ordered_by_document_number_not_input_order(self) -> None:
        rows = [_row("C"), _row("A"), _row("B")]
        assert [r["document_number"] for r in brc.select_rows(rows, DEFAULT_RULE)] == ["A", "B", "C"]


# --------------------------------------------------------------------------
# vocabulary competition
# --------------------------------------------------------------------------


class TestJaccard:
    def test_identical_texts_score_one_and_disjoint_texts_score_zero(self) -> None:
        same = brc.jaccard_distribution([brc.tokenize("alpha beta gamma")] * 2)
        assert same == [1.0]
        disjoint = brc.jaccard_distribution([brc.tokenize("alpha beta"), brc.tokenize("gamma delta")])
        assert disjoint == [0.0]

    def test_half_overlap_scores_one_third(self) -> None:
        """{a,b} vs {b,c}: intersection 1, union 3."""
        assert brc.jaccard_distribution([brc.tokenize("alpha beta"), brc.tokenize("beta gamma")]) == [
            pytest.approx(1 / 3)
        ]

    def test_pair_count_is_n_choose_2(self) -> None:
        sets = [brc.tokenize(f"shared token{index}") for index in range(6)]
        assert len(brc.jaccard_distribution(sets)) == 15

    def test_sampling_is_seeded_and_reproducible(self) -> None:
        sets = [brc.tokenize(f"shared word{index}") for index in range(60)]
        first = brc.jaccard_distribution(sets, cap=10, seed=3)
        assert first == brc.jaccard_distribution(sets, cap=10, seed=3)
        assert len(first) == 45

    def test_a_coherent_draw_outscores_an_incoherent_one(self) -> None:
        """The property the corpus exists to have, stated as a test."""
        coherent = [
            brc.tokenize(f"endangered threatened species critical habitat designation taxon{index}")
            for index in range(12)
        ]
        incoherent = [
            brc.tokenize(f"alpha{index} beta{index} gamma{index} delta{index} epsilon{index}") for index in range(12)
        ]
        assert brc.median(brc.jaccard_distribution(coherent)) > brc.median(brc.jaccard_distribution(incoherent))

    def test_empty_token_sets_are_skipped_rather_than_dividing_by_zero(self) -> None:
        assert brc.jaccard_distribution([set(), brc.tokenize("alpha")]) == []

    def test_tokenizer_drops_short_tokens_and_stopwords(self) -> None:
        assert brc.tokenize("The species of a habitat") == {"species", "habitat"}


# --------------------------------------------------------------------------
# draw manifest
# --------------------------------------------------------------------------


class TestDrawManifest:
    def test_manifest_is_byte_identical_across_rebuilds(self, tmp_path: Path) -> None:
        rows = [_row(f"DOC-{index:03d}") for index in range(8)]
        first = brc.build_draw(rows, rule=DEFAULT_RULE, source_digest="sha256:aa")
        second = brc.build_draw(rows, rule=DEFAULT_RULE, source_digest="sha256:aa")
        assert brc.canonical_json(first) == brc.canonical_json(second)

    def test_manifest_records_the_rule_so_the_draw_is_auditable(self) -> None:
        manifest = brc.build_draw([_row("A")], rule=DEFAULT_RULE, source_digest="sha256:aa")
        assert manifest["selection_rule"]["cfr_title"] == 50
        assert manifest["selection_rule"]["min_pages"] == 12
        assert manifest["schema_version"] == brc.DRAW_SCHEMA_VERSION
        assert manifest["source_digest"] == "sha256:aa"

    def test_manifest_carries_the_measured_jaccard_distribution(self) -> None:
        manifest = brc.build_draw(
            [_row(f"DOC-{index}") for index in range(6)],
            rule=DEFAULT_RULE,
            source_digest="sha256:aa",
        )
        vocabulary = manifest["vocabulary_competition"]
        assert vocabulary["pair_count"] == 15
        for key in ("p10", "p25", "median", "p75", "p90", "mean"):
            assert isinstance(vocabulary[key], float)

    def test_draw_id_changes_when_the_draw_changes(self) -> None:
        one = brc.build_draw([_row("A")], rule=DEFAULT_RULE, source_digest="sha256:aa")
        two = brc.build_draw([_row("A"), _row("B")], rule=DEFAULT_RULE, source_digest="sha256:aa")
        assert one["draw_id"] != two["draw_id"]

    def test_draw_id_is_stable_for_an_unchanged_draw(self) -> None:
        rows = [_row("A"), _row("B")]
        assert (
            brc.build_draw(rows, rule=DEFAULT_RULE, source_digest="sha256:aa")["draw_id"]
            == brc.build_draw(rows, rule=DEFAULT_RULE, source_digest="sha256:aa")["draw_id"]
        )

    def test_documents_carry_the_url_actually_published_by_the_source(self) -> None:
        """URLs are read from the row, never reconstructed from a date path.

        A hand-built ``.../YYYY/MM/DD/<n>.html`` guess 404s whenever the guessed
        date differs from the publication date recorded upstream.
        """
        row = _row("A")
        row["body_html_url"] = "https://www.federalregister.gov/documents/full_text/html/1999/01/02/A.html"
        manifest = brc.build_draw([row], rule=DEFAULT_RULE, source_digest="sha256:aa")
        assert manifest["documents"][0]["body_html_url"].endswith("/1999/01/02/A.html")

    def test_a_row_without_a_body_url_is_excluded_and_counted(self) -> None:
        row = _row("A")
        row["body_html_url"] = None
        manifest = brc.build_draw([row, _row("B")], rule=DEFAULT_RULE, source_digest="sha256:aa")
        assert [d["document_number"] for d in manifest["documents"]] == ["B"]
        assert manifest["counts"]["excluded_no_body_url"] == 1


# --------------------------------------------------------------------------
# fetching
# --------------------------------------------------------------------------


BODY = b"<div class='preamble'><p>Critical habitat for the species.</p></div>"


class _Recorder:
    """A fetcher that serves canned responses and records politeness."""

    def __init__(self, responses: dict[str, bytes | Exception]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def __call__(self, url: str) -> brc.FetchResult:
        self.calls.append(url)
        outcome: bytes | Exception = self.responses.get(url, BODY)
        if isinstance(outcome, Exception):
            raise outcome
        return brc.FetchResult(
            content=outcome,
            resolved_url=url,
            media_type="text/html",
            status_code=200,
            etag=None,
            last_modified=None,
        )


def _manifest(tmp_path: Path, count: int = 3) -> Path:
    rows = [_row(f"DOC-{index:03d}") for index in range(count)]
    manifest = brc.build_draw(rows, rule=DEFAULT_RULE, source_digest="sha256:aa")
    path = tmp_path / "draw-manifest.json"
    path.write_text(brc.canonical_json(manifest) + "\n", encoding="utf-8")
    return path


class TestFetch:
    def test_writes_exact_bytes_and_a_lock_covering_every_document(self, tmp_path: Path) -> None:
        cache = tmp_path / "cache"
        result = brc.fetch_bodies(_manifest(tmp_path), cache, fetcher=_Recorder({}), sleep=lambda _: None)
        assert result["fetched"] == 3
        lock = json.loads((cache / "source-lock.json").read_text())
        assert len(lock["sources"]) == 3
        for record in lock["sources"]:
            body = (cache / record["cache_file"]).read_bytes()
            assert body == BODY
            assert record["source_sha256"] == brc.sha256_bytes(BODY)
            assert record["source_bytes"] == len(BODY)

    def test_a_second_run_refetches_nothing(self, tmp_path: Path) -> None:
        """Resumability: the point of receipting each document separately."""
        cache = tmp_path / "cache"
        manifest = _manifest(tmp_path)
        brc.fetch_bodies(manifest, cache, fetcher=_Recorder({}), sleep=lambda _: None)
        again = _Recorder({})
        result = brc.fetch_bodies(manifest, cache, fetcher=again, sleep=lambda _: None)
        assert again.calls == []
        assert result["skipped_already_present"] == 3
        assert result["fetched"] == 0

    def test_one_failure_does_not_discard_the_other_successes(self, tmp_path: Path) -> None:
        cache = tmp_path / "cache"
        manifest = _manifest(tmp_path)
        rows = json.loads(manifest.read_text())["documents"]
        doomed = rows[1]["body_html_url"]
        recorder = _Recorder({doomed: RuntimeError("HTTP 404")})
        result = brc.fetch_bodies(manifest, cache, fetcher=recorder, sleep=lambda _: None)
        assert result["fetched"] == 2
        assert result["quarantined"] == 1
        lock = json.loads((cache / "source-lock.json").read_text())
        assert len(lock["sources"]) == 2

    def test_quarantine_records_a_machine_readable_reason(self, tmp_path: Path) -> None:
        cache = tmp_path / "cache"
        manifest = _manifest(tmp_path)
        doomed = json.loads(manifest.read_text())["documents"][0]["body_html_url"]
        brc.fetch_bodies(
            manifest,
            cache,
            fetcher=_Recorder({doomed: RuntimeError("HTTP 404")}),
            sleep=lambda _: None,
        )
        quarantine = json.loads((cache / "quarantine.json").read_text())
        assert quarantine["rows"][0]["reason"] == "fetch-failed"
        assert "404" in quarantine["rows"][0]["detail"]
        assert quarantine["rows"][0]["document_number"] == "DOC-000"

    def test_a_quarantined_document_is_retried_on_the_next_run(self, tmp_path: Path) -> None:
        """Failures must not be cached as if they were answers."""
        cache = tmp_path / "cache"
        manifest = _manifest(tmp_path)
        doomed = json.loads(manifest.read_text())["documents"][0]["body_html_url"]
        brc.fetch_bodies(manifest, cache, fetcher=_Recorder({doomed: RuntimeError("boom")}), sleep=lambda _: None)
        recovered = _Recorder({})
        result = brc.fetch_bodies(manifest, cache, fetcher=recovered, sleep=lambda _: None)
        assert recovered.calls == [doomed]
        assert result["fetched"] == 1
        assert result["quarantined"] == 0

    @pytest.mark.parametrize(
        ("payload", "reason"),
        [
            (b"", "empty-body"),
            (b"%PDF-1.7 binary", "not-markup"),
            (b"   \n  ", "empty-body"),
        ],
    )
    def test_unusable_bodies_are_quarantined_rather_than_sealed(
        self, tmp_path: Path, payload: bytes, reason: str
    ) -> None:
        cache = tmp_path / "cache"
        manifest = _manifest(tmp_path, count=1)
        url = json.loads(manifest.read_text())["documents"][0]["body_html_url"]
        result = brc.fetch_bodies(manifest, cache, fetcher=_Recorder({url: payload}), sleep=lambda _: None)
        assert result["quarantined"] == 1
        assert json.loads((cache / "quarantine.json").read_text())["rows"][0]["reason"] == reason

    def test_a_cloudflare_interstitial_is_quarantined_not_sealed(self, tmp_path: Path) -> None:
        """The one failure that would corrupt every downstream number silently.

        An interstitial returns HTTP 200 with HTML. It digests cleanly and
        parses into passages, so nothing downstream would notice ~1000 copies
        of it; the retrieval baseline would be measuring Cloudflare.
        """
        cache = tmp_path / "cache"
        manifest = _manifest(tmp_path, count=1)
        url = json.loads(manifest.read_text())["documents"][0]["body_html_url"]
        challenge = b"<html><head><title>Just a moment...</title></head><body>Checking your browser before accessing</body></html>"
        result = brc.fetch_bodies(manifest, cache, fetcher=_Recorder({url: challenge}), sleep=lambda _: None)
        assert result["quarantined"] == 1
        assert json.loads((cache / "quarantine.json").read_text())["rows"][0]["reason"] == ("blocked-interstitial")
        assert json.loads((cache / "source-lock.json").read_text())["sources"] == []

    def test_a_redirect_to_the_unblock_host_is_quarantined(self, tmp_path: Path) -> None:
        cache = tmp_path / "cache"
        manifest = _manifest(tmp_path, count=1)

        def redirected(_: str) -> brc.FetchResult:
            return brc.FetchResult(
                content=b"<html><body>Access restricted</body></html>",
                resolved_url="https://unblock.federalregister.gov/",
                media_type="text/html",
                status_code=200,
            )

        result = brc.fetch_bodies(manifest, cache, fetcher=redirected, sleep=lambda _: None)
        assert result["quarantine_by_reason"] == {"blocked-interstitial": 1}

    def test_it_sleeps_between_requests_but_not_before_the_first(self, tmp_path: Path) -> None:
        slept: list[float] = []
        brc.fetch_bodies(
            _manifest(tmp_path),
            tmp_path / "cache",
            fetcher=_Recorder({}),
            sleep=slept.append,
            min_interval_seconds=1.5,
        )
        assert slept == [1.5, 1.5]

    def test_it_refuses_to_exceed_its_declared_request_budget(self, tmp_path: Path) -> None:
        with pytest.raises(brc.BodyCorpusError, match="request budget"):
            brc.fetch_bodies(
                _manifest(tmp_path, count=5),
                tmp_path / "cache",
                fetcher=_Recorder({}),
                sleep=lambda _: None,
                max_requests=2,
            )

    def test_the_lock_is_byte_identical_across_a_resumed_rebuild(self, tmp_path: Path) -> None:
        """The fetch is not reproducible; the lock is. That is the boundary."""
        manifest = _manifest(tmp_path)
        one, two = tmp_path / "one", tmp_path / "two"
        brc.fetch_bodies(manifest, one, fetcher=_Recorder({}), sleep=lambda _: None)
        # Fetch the second cache in two passes to prove resumption changes nothing.
        brc.fetch_bodies(
            manifest,
            two,
            fetcher=_Recorder({}),
            sleep=lambda _: None,
            max_requests=1,
            stop_at_budget=True,
        )
        brc.fetch_bodies(manifest, two, fetcher=_Recorder({}), sleep=lambda _: None)
        assert (one / "source-lock.json").read_bytes() == (two / "source-lock.json").read_bytes()


# --------------------------------------------------------------------------
# validation
# --------------------------------------------------------------------------


class TestReleaseCompatibility:
    """The lock must satisfy the *shared* contract, not a private one.

    Writing a second, divergent lock format would leave two things to keep in
    agreement. These tests hold the sealed cache to the validator that
    ``segmentation_evaluation`` and the release pipeline already use.
    """

    def test_the_sealed_lock_passes_the_shared_source_cache_validator(self, tmp_path: Path) -> None:
        from spicy_regs.corpora.segmentation_evaluation import validate_source_cache

        cache = tmp_path / "cache"
        manifest_path = _manifest(tmp_path)
        brc.fetch_bodies(manifest_path, cache, fetcher=_Recorder({}), sleep=lambda _: None)
        manifest = json.loads(manifest_path.read_text())
        numbers = [entry["document_number"] for entry in manifest["documents"]]
        report = validate_source_cache(cache, specs=brc.document_specs(manifest, numbers))
        assert report["status"] == "pass", report["failures"]
        assert report["source_count"] == 3

    def test_specs_cover_exactly_the_documents_that_sealed(self, tmp_path: Path) -> None:
        """A quarantined document must not appear in the spec list.

        The release builder raises when a spec has no lock record, so the two
        have to be derived from the same surviving set.
        """
        cache = tmp_path / "cache"
        manifest_path = _manifest(tmp_path)
        doomed = json.loads(manifest_path.read_text())["documents"][0]["body_html_url"]
        brc.fetch_bodies(
            manifest_path,
            cache,
            fetcher=_Recorder({doomed: RuntimeError("HTTP 404")}),
            sleep=lambda _: None,
        )
        lock = json.loads((cache / "source-lock.json").read_text())
        sealed = [record["case_id"] for record in lock["sources"]]
        assert len(sealed) == 2
        specs = brc.document_specs(json.loads(manifest_path.read_text()), sealed)
        assert [spec.case_id for spec in specs] == sealed

    def test_a_document_absent_from_the_draw_is_refused(self, tmp_path: Path) -> None:
        manifest = json.loads(_manifest(tmp_path).read_text())
        with pytest.raises(brc.BodyCorpusError, match="absent from the draw"):
            brc.document_specs(manifest, ["NOT-DRAWN"])

    def test_specs_target_the_body_html_field_the_parser_understands(self, tmp_path: Path) -> None:
        manifest = json.loads(_manifest(tmp_path, count=1).read_text())
        spec = brc.document_specs(manifest, ["DOC-000"])[0]
        assert spec.target_field == "body_html"
        assert spec.profile_id == "federal-register-document-v1"
        assert spec.representation == "html"


class TestSecretScan:
    def test_a_keyed_source_url_is_refused_rather_than_sealed(self, tmp_path: Path) -> None:
        """A signed URL in the draw would be copied verbatim into the lock."""
        row = _row("A")
        row["body_html_url"] = "https://www.federalregister.gov/x.html?api_key=abcd1234efgh"
        manifest = brc.build_draw([row], rule=DEFAULT_RULE, source_digest="sha256:aa")
        path = tmp_path / "draw.json"
        path.write_text(brc.canonical_json(manifest) + "\n", encoding="utf-8")
        with pytest.raises(brc.BodyCorpusError, match="secret-like"):
            brc.fetch_bodies(path, tmp_path / "cache", fetcher=_Recorder({}), sleep=lambda _: None)

    def test_ordinary_federal_register_urls_pass(self, tmp_path: Path) -> None:
        brc.scan_for_secrets({"url": _row("A")["body_html_url"]}, "draw")

    def test_the_scan_reaches_nested_values(self) -> None:
        with pytest.raises(brc.BodyCorpusError, match=r"payload\.rows\[1\]\.token"):
            brc.scan_for_secrets({"rows": [{"token": "fine"}, {"token": "sk-proj-" + "a" * 24}]}, "payload")


class TestMeasure:
    def test_visible_text_strips_markup_and_scripts(self) -> None:
        markup = "<div><script>var x = 'habitat';</script><p>Critical  habitat</p></div>"
        assert brc.visible_text(markup) == "Critical habitat"

    def test_visible_text_collapses_whitespace_so_layout_is_not_vocabulary(self) -> None:
        assert brc.visible_text("<p>a\n\n\tb</p>") == "a b"

    def test_measure_reports_counts_bytes_and_body_jaccard(self, tmp_path: Path) -> None:
        cache = tmp_path / "cache"
        brc.fetch_bodies(_manifest(tmp_path), cache, fetcher=_Recorder({}), sleep=lambda _: None)
        report = brc.measure_corpus(cache)
        assert report["document_count"] == 3
        assert report["source_bytes"]["total"] == len(BODY) * 3
        assert report["vocabulary_competition"]["pair_count"] == 3
        # Identical bodies compete perfectly; the metric must say so.
        assert report["vocabulary_competition"]["median"] == pytest.approx(1.0)

    def test_measure_counts_documents_above_the_chunking_threshold(self, tmp_path: Path) -> None:
        """Below ~3000 characters chunking is a no-op, so the count is the point."""
        cache = tmp_path / "cache"
        manifest = _manifest(tmp_path, count=2)
        urls = [d["body_html_url"] for d in json.loads(manifest.read_text())["documents"]]
        long_body = b"<p>" + b"habitat conservation " * 500 + b"</p>"
        brc.fetch_bodies(
            manifest,
            cache,
            fetcher=_Recorder({urls[0]: long_body}),
            sleep=lambda _: None,
        )
        report = brc.measure_corpus(cache)
        assert report["chunking_relevant"]["documents_over_3000_chars"] == 1

    def test_measure_counts_publisher_boilerplate_rather_than_removing_it(self, tmp_path: Path) -> None:
        """Exact publisher bytes are kept; the constant is reported, not deleted."""
        cache = tmp_path / "cache"
        manifest = _manifest(tmp_path, count=1)
        url = json.loads(manifest.read_text())["documents"][0]["body_html_url"]
        with_chrome = b"<div><p>Document headings vary by document type</p><p>Body</p></div>"
        brc.fetch_bodies(manifest, cache, fetcher=_Recorder({url: with_chrome}), sleep=lambda _: None)
        report = brc.measure_corpus(cache)
        assert report["documents_carrying_publisher_boilerplate"] == 1
        # and the bytes are still exactly what the publisher served
        lock = json.loads((cache / "source-lock.json").read_text())
        assert (cache / lock["sources"][0]["cache_file"]).read_bytes() == with_chrome


class TestValidate:
    def test_a_clean_cache_passes(self, tmp_path: Path) -> None:
        cache = tmp_path / "cache"
        brc.fetch_bodies(_manifest(tmp_path), cache, fetcher=_Recorder({}), sleep=lambda _: None)
        assert brc.validate_body_cache(cache)["status"] == "pass"

    def test_a_tampered_body_fails_closed(self, tmp_path: Path) -> None:
        cache = tmp_path / "cache"
        brc.fetch_bodies(_manifest(tmp_path), cache, fetcher=_Recorder({}), sleep=lambda _: None)
        lock = json.loads((cache / "source-lock.json").read_text())
        victim = cache / lock["sources"][0]["cache_file"]
        victim.write_bytes(BODY + b"<p>silently added</p>")
        report = brc.validate_body_cache(cache)
        assert report["status"] == "fail"
        assert any("digest" in failure for failure in report["failures"])

    def test_a_missing_body_fails_closed(self, tmp_path: Path) -> None:
        cache = tmp_path / "cache"
        brc.fetch_bodies(_manifest(tmp_path), cache, fetcher=_Recorder({}), sleep=lambda _: None)
        lock = json.loads((cache / "source-lock.json").read_text())
        (cache / lock["sources"][0]["cache_file"]).unlink()
        assert brc.validate_body_cache(cache)["status"] == "fail"
