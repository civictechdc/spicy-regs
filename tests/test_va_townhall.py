"""Hermetic tests for the Virginia Regulatory Town Hall ingest (no network).

Parsers run against trimmed copies of real Town Hall pages in
``tests/fixtures/va_townhall/`` (site chrome and agency staff contact details
removed, individual commenters' names replaced, long comment bodies cut to a
few paragraphs). The crawl runs against an ``httpx.MockTransport`` that serves
those fixtures and, like the real site, an empty 200 for any unknown ID.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines.rollups import va_townhall as rollup
from spicy_regs.sources import va_townhall as th
from spicy_regs.sources.va_townhall import (
    FORUM_GENERAL_NOTICE,
    FORUM_PETITION,
    FORUM_STAGE,
    KnownState,
    StoredStage,
    TownHallClient,
    TownHallParseError,
    TownHallUnavailable,
    VaTownHallReader,
    html_to_text,
    parse_action,
    parse_forum,
    parse_forum_list,
    parse_stage,
    parse_stage_list,
    split_commenter,
    vac_chapter,
)
from spicy_regs.transforms.build_va_townhall import (
    ACTIONS_OUTPUT,
    COMMENTS_OUTPUT,
    STAGES_OUTPUT,
    STATE_KEY,
    build_va_townhall,
)

# The transforms package re-exports the build function under the module's name.
build = importlib.import_module("spicy_regs.transforms.build_va_townhall")

FIXTURES = Path(__file__).parent / "fixtures" / "va_townhall"


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Parsers
# --------------------------------------------------------------------------- #


def test_vac_chapter_normalizes_town_hall_citation():
    assert vac_chapter("[24\xa0VAC\xa035\xa0‑\xa060]") == "24VAC35-60"
    assert vac_chapter("18 VAC 5 - 22") == "18VAC5-22"
    assert vac_chapter("no citation") is None


def test_parse_stage_open_comment_period():
    stage = parse_stage(fixture("stage_11305_open.html"), 11305)
    assert stage is not None
    assert stage["action_id"] == 7067
    assert stage["stage_type"] == "Proposed"
    assert stage["status"] == "Comment period is underway and will end on 12/18/2026."
    assert stage["agency"] == "Commission on the Virginia Alcohol Safety Action Program"
    assert stage["chapter_citation"] == "24VAC35-60"
    assert stage["chapter_title"] == "Ignition Interlock Program Regulations"
    assert (stage["register_volume"], stage["register_issue"], stage["register_date"]) == (43, 5, "2026-10-19")
    assert stage["comment_start_date"] == "2026-10-19"
    assert stage["comment_end_date"] == "2026-12-18"
    assert stage["comment_count"] == 0
    assert stage["effective_date"] is None
    docs = stage["documents"]
    assert docs[0] == {
        "title": "Proposed Text",
        "date": "2026-10-02",
        "url": "https://townhall.virginia.gov/L/ViewXML.cfm?textid=20973",
    }
    # GetFile paths use backslashes; they are percent-encoded into a valid URL.
    assert docs[1]["url"].startswith("https://townhall.virginia.gov/L/GetFile.cfm?File=130%5C7067%5C11305%5C")


def test_parse_stage_effective_with_comments():
    stage = parse_stage(fixture("stage_9926_effective.html"), 9926)
    assert stage is not None
    assert stage["stage_type"] == "Fast-Track"
    assert stage["comment_end_date"] == "2026-10-21"
    assert stage["comment_count"] == 6
    assert stage["effective_date"] == "2026-11-05"
    assert stage["chapter_citation"] == "12VAC30-122"
    # A "(modified ...)" note after a document date does not replace the date.
    assert {"title": "Agency Background Document", "date": "2023-02-07"}.items() <= stage["documents"][1].items()


def test_parse_stage_from_1999_with_unsubmitted_document():
    stage = parse_stage(fixture("stage_100_1999.html"), 100)
    assert stage is not None
    assert (stage["action_id"], stage["stage_type"]) == (58, "NOIRA")
    assert stage["register_date"] == "1999-06-21"
    assert stage["comment_end_date"] == "1999-07-21"
    assert stage["documents"][0] == {"title": "Preliminary Draft Text", "date": None, "url": None}


def test_parse_stage_empty_page_is_missing_and_wrong_page_raises():
    assert parse_stage("", 5) is None
    assert parse_stage("  \n", 5) is None
    with pytest.raises(TownHallParseError):
        parse_stage(fixture("stage_100_1999.html"), 101)
    with pytest.raises(TownHallParseError):
        parse_stage("<html><body>Something else entirely</body></html>", 5)


def test_parse_action():
    action = parse_action(fixture("action_7067.html"), 7067)
    assert action is not None
    assert action["title"] == "Amendments to Virginia's Ignition Interlock Regulations"
    assert action["summary"].startswith("The purpose of the proposed amended regulations")
    assert action["chapters_affected"] == "Only affects this chapter."
    assert action["withdrawn_date"] is None
    assert action["stage_ids"] == [11246, 11305]
    assert action["chapter_citation"] == "24VAC35-60"
    assert (action["agency_id"], action["board_id"], action["chapter_id"]) == (413, 130, 2616)


def test_parse_action_withdrawn():
    action = parse_action(fixture("action_10_withdrawn.html"), 10)
    assert action is not None
    assert action["withdrawn_date"] == "2009-08-20"
    assert action["board"] == "Board of Long-Term Care Administrators"
    assert action["stage_ids"] == [15]
    assert parse_action("", 10) is None


def test_parse_stage_forum():
    parsed = parse_forum(fixture("forum_stage_11256.html"), FORUM_STAGE, 11256)
    assert parsed is not None
    header_count, comments = parsed
    assert header_count == 15  # what the page prints; the fixture keeps 3 of them
    assert [c["comment_id"] for c in comments] == [242807, 242845, 242860]
    first = comments[0]
    assert first["stage_id"] == 11256
    assert (first["commenter"], first["organization"]) == ("Commenter A", "Southside Behavioral Health")
    assert first["commenter_raw"] == "Commenter A, Southside Behavioral Health"
    assert first["title"] == "Comments on CPST"
    assert (first["posted_date"], first["posted_at"]) == ("2026-10-22", "2026-10-22T14:20")
    # Each comment body is a whole pasted HTML document; only its text is kept,
    # one blank line between paragraphs, with no page markup leaking in.
    assert first["comment"].startswith("Southside Behavioral Health (SBH) supports")
    assert "\n\n" in first["comment"]
    for c in comments:
        assert "CommentID" not in (c["comment"] or "")
        assert "<" not in (c["comment"] or "")


def test_parse_general_notice_forum_with_empty_body():
    parsed = parse_forum(fixture("forum_general_notice_3470.html"), FORUM_GENERAL_NOTICE, 3470)
    assert parsed is not None
    header_count, comments = parsed
    assert header_count == 3
    assert all(c["stage_id"] is None and c["forum_id"] == 3470 for c in comments)
    assert comments[0]["commenter"] == "Anonymous"
    empty = [c for c in comments if c["comment"] is None]
    assert len(empty) == 1 and empty[0]["title"]


def test_parse_forum_without_comments_and_missing_forum():
    assert parse_forum(fixture("forum_stage_11305_empty.html"), FORUM_STAGE, 11305) == (None, [])
    assert parse_forum("", FORUM_PETITION, 1) is None
    with pytest.raises(TownHallParseError):
        parse_forum("<html><body>maintenance</body></html>", FORUM_PETITION, 1)


def test_parse_lists():
    assert parse_forum_list(fixture("forums_open.html")) == [
        (FORUM_GENERAL_NOTICE, 3500),
        (FORUM_PETITION, 461),
        (FORUM_STAGE, 11256),
        (FORUM_STAGE, 11305),
    ]
    assert parse_stage_list(fixture("now_in_progress.html")) == [9926, 11305]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Jane Doe, Acme Corp", ("Jane Doe", "Acme Corp")),
        ("Anonymous", ("Anonymous", None)),
        ("Jane Doe, MSN, APRN, Acme Clinic", ("Jane Doe, MSN, APRN", "Acme Clinic")),
        ("John Roe, Jr., Roe Farms, LLC", ("John Roe, Jr.", "Roe Farms, LLC")),
        ("Jada , Miss Jada LLC", ("Jada", "Miss Jada LLC")),
        (None, (None, None)),
    ],
)
def test_split_commenter(raw, expected):
    assert split_commenter(raw) == expected


def test_html_to_text_keeps_paragraphs_and_lists():
    text = html_to_text(
        "<html><head><title>x</title></head><body><p>One&nbsp;two</p><ul><li>a</li><li>b</li></ul>three<br>four</body></html>"
    )
    assert text == "One two\n\n- a\n\n- b\n\nthree\nfour"


# --------------------------------------------------------------------------- #
# Client
# --------------------------------------------------------------------------- #


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _client(handler: Callable[[httpx.Request], httpx.Response], clock: FakeClock | None = None) -> TownHallClient:
    clock = clock or FakeClock()
    return TownHallClient(client=httpx.Client(transport=httpx.MockTransport(handler)), sleep=clock.sleep, clock=clock)


def test_client_spaces_requests_by_crawl_delay():
    clock = FakeClock()
    paths = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        clock.now += 1.0  # each request takes a second
        return httpx.Response(200, text="ok")

    client = _client(handler, clock)
    for _ in range(3):
        client.stage(1)
    # First request goes straight out; the next two wait out the rest of 5s.
    assert clock.sleeps == [4.0, 4.0]
    # Every request stays under /L/ (robots.txt disallows /L./).
    assert all(p.startswith("/L/") for p in paths)


def test_client_posts_page_size_for_forums():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["query"] = dict(request.url.params)
        seen["body"] = request.content.decode()
        return httpx.Response(200, text="")

    _client(handler).forum(FORUM_GENERAL_NOTICE, 3470, page=2)
    assert seen["method"] == "POST"
    assert seen["query"] == {"generalnoticeid": "3470"}
    assert f"vPerPage={th.FORUM_PAGE_SIZE}" in seen["body"] and "vPage=2" in seen["body"]


def test_client_treats_login_redirect_as_missing_and_follows_other_redirects():
    def handler(request: httpx.Request) -> httpx.Response:
        stage = request.url.params.get("stageid")
        if stage == "1":  # a draft stage, as the real site answers
            return httpx.Response(
                302,
                headers={
                    "location": "statelogin.cfm?error=This stage is not yet publicly viewable."
                    "&forward=/L/ViewStage.cfm?stageid=1"
                },
            )
        if stage == "2":
            return httpx.Response(302, headers={"location": "ViewStage.cfm?stageid=3"})
        return httpx.Response(200, text=f"stage {stage}")

    client = _client(handler)
    assert client.stage(1) == ""
    assert client.requests == 1  # the login page is never fetched
    assert client.stage(2) == "stage 3"


def test_client_retries_server_errors_then_raises():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(503)

    with pytest.raises(TownHallUnavailable, match="giving up"):
        _client(handler).stage(1)
    assert len(calls) == th._MAX_RETRIES


def test_client_does_not_retry_client_errors():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(403)

    with pytest.raises(TownHallUnavailable, match="HTTP 403"):
        _client(handler).stage(1)
    assert len(calls) == 1


def test_client_recovers_from_transient_error():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(502) if len(calls) == 1 else httpx.Response(200, text="fine")

    assert _client(handler).stage(1) == "fine"


# --------------------------------------------------------------------------- #
# Crawl
# --------------------------------------------------------------------------- #

# The fake site: (page, id parameter, id) -> fixture. Anything else is an empty
# 200, as on the real site.
_PAGES = {
    ("ViewStage.cfm", "stageid", "11305"): "stage_11305_open.html",
    ("ViewStage.cfm", "stageid", "9926"): "stage_9926_effective.html",
    ("ViewStage.cfm", "stageid", "100"): "stage_100_1999.html",
    ("ViewAction.cfm", "actionid", "7067"): "action_7067.html",
    ("ViewComments.cfm", "stageid", "9926"): "forum_stage_11256.html",
    ("ViewComments.cfm", "stageid", "11305"): "forum_stage_11305_empty.html",
    ("ViewComments.cfm", "generalnoticeid", "3470"): "forum_general_notice_3470.html",
}


class FakeSite:
    def __init__(
        self, *, forums: str = "", in_progress: str = "", completed: str = "", fail_after: int | None = None
    ) -> None:
        self.forums = forums
        self.in_progress = in_progress
        self.completed = completed
        self.fail_after = fail_after
        self.requests: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        page = request.url.path.rsplit("/", 1)[-1]
        params = dict(request.url.params)
        self.requests.append(f"{page}?{'&'.join(f'{k}={v}' for k, v in sorted(params.items()))}")
        if self.fail_after is not None and len(self.requests) > self.fail_after:
            return httpx.Response(503)
        if page == "Forums.cfm":
            return httpx.Response(200, text=self.forums if not params else "")
        if page == "NowInProgress.cfm":
            if not params:
                return httpx.Response(200, text=self.in_progress)
            return httpx.Response(200, text=self.completed if params.get("Qtype") == "Completed" else "")
        for (name, key, value), fixture_name in _PAGES.items():
            if page == name and params.get(key) == value:
                text = fixture(fixture_name)
                if fixture_name == "forum_stage_11256.html":  # serve it as stage 9926's forum
                    text = text.replace("11256", "9926")
                return httpx.Response(200, text=text)
        return httpx.Response(200, text="")


def _reader(site: FakeSite, **kwargs) -> VaTownHallReader:
    clock = FakeClock()
    client = TownHallClient(
        client=httpx.Client(transport=httpx.MockTransport(site)), delay=0, sleep=clock.sleep, clock=clock
    )
    return VaTownHallReader(client=client, **kwargs)


_FORUMS = '<A href="comments.cfm?stageid=11305">View</A>'
_IN_PROGRESS = '<a href="viewstage.cfm?stageid=9926">Stage 9926</a>'


def test_refresh_fetches_listed_forums_stages_actions_and_comments():
    site = FakeSite(forums=_FORUMS, in_progress=_IN_PROGRESS)
    reader = _reader(site, cursors={FORUM_STAGE: 0, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0})
    records = list(reader.iter_records())

    kinds = [(r["kind"], r.get("stage_id") or r.get("action_id")) for r in records if r["kind"] != "comment"]
    assert ("stage", 11305) in kinds and ("stage", 9926) in kinds and ("action", 7067) in kinds
    comments = [r for r in records if r["kind"] == "comment"]
    # Stage 9926 reports 6 comments and was never stored, so its forum is fetched.
    assert {c["stage_id"] for c in comments} == {9926}
    assert reader.error is None and reader.parse_failures == []
    # The open forum (11305) is fetched exactly once even though it is also a stage.
    assert sum(r.startswith("ViewComments.cfm?stageid=11305") for r in site.requests) == 1


def test_refresh_skips_fresh_stages_and_unchanged_forums():
    site = FakeSite(in_progress=_IN_PROGRESS)
    known = KnownState(
        stages={9926: StoredStage("2999-01-01T00:00:00Z", 6, "Stage complete.")},
        actions={6186},
        forums={(FORUM_STAGE, 9926)},
    )
    reader = _reader(site, known=known, cursors={FORUM_STAGE: 0, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0})
    assert list(reader.iter_records()) == []
    assert not any(r.startswith(("ViewStage", "ViewComments")) for r in site.requests)


@pytest.mark.parametrize(
    ("stored_status", "refetched"), [("Comment period ended 10/21/2026.", True), ("Stage complete.", False)]
)
def test_completed_list_refetches_only_stages_not_yet_stored_complete(stored_status, refetched):
    site = FakeSite(completed=_IN_PROGRESS)
    known = KnownState(
        stages={9926: StoredStage("2999-01-01", 6, stored_status)}, actions={6186}, forums={(FORUM_STAGE, 9926)}
    )
    reader = _reader(site, known=known, cursors={FORUM_STAGE: 0, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0})
    list(reader.iter_records())
    assert ("ViewStage.cfm?stageid=9926" in site.requests) is refetched


def test_backfill_walks_down_skips_known_and_initializes_cursor_from_lists():
    site = FakeSite(
        forums='<A href="comments.cfm?generalnoticeid=3471">x</A>',
        in_progress='<a href="viewstage.cfm?stageid=102">s</a>',
    )
    known = KnownState(stages={101: StoredStage("2999-01-01", 0)})
    reader = _reader(site, known=known, cursors={FORUM_PETITION: 0})
    records = list(reader.iter_records())

    stage_ids = [r["stage_id"] for r in records if r["kind"] == "stage"]
    assert stage_ids == [100]
    assert not any(r == "ViewStage.cfm?stageid=101" for r in site.requests)  # already stored
    # General notices start at the highest listed ID and reach 3470's comments.
    assert {r["forum_id"] for r in records if r["kind"] == "comment"} == {3470}
    assert reader.cursors == {FORUM_STAGE: 0, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0}


def test_backfill_stops_at_deadline_and_keeps_cursor():
    site = FakeSite()
    clock = FakeClock()
    client = TownHallClient(
        client=httpx.Client(transport=httpx.MockTransport(site)), delay=1.0, sleep=clock.sleep, clock=clock
    )
    reader = VaTownHallReader(
        client=client, cursors={FORUM_STAGE: 50, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0}, deadline=10.0, clock=clock
    )
    list(reader.iter_records())
    # 5 discovery requests, then one stage per second until the 10s deadline.
    assert 0 < reader.cursors[FORUM_STAGE] < 50
    assert reader.cursors[FORUM_STAGE] == 50 - sum(r.startswith("ViewStage") for r in site.requests)


def test_site_outage_stops_crawl_without_raising():
    site = FakeSite(fail_after=7)
    reader = _reader(site, cursors={FORUM_STAGE: 50, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0})
    list(reader.iter_records())
    assert isinstance(reader.error, TownHallUnavailable)
    # The cursor stops at the first ID that was not finished.
    assert reader.cursors[FORUM_STAGE] == 48


def test_parse_failure_is_recorded_and_skipped():
    def site(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("ViewStage.cfm"):
            return httpx.Response(200, text="<html><body>Unexpected layout</body></html>")
        return httpx.Response(200, text="")

    clock = FakeClock()
    client = TownHallClient(
        client=httpx.Client(transport=httpx.MockTransport(site)), delay=0, sleep=clock.sleep, clock=clock
    )
    reader = VaTownHallReader(client=client, cursors={FORUM_STAGE: 3, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0})
    assert list(reader.iter_records()) == []
    assert reader.parse_failures == ["stage 3", "stage 2", "stage 1"]
    assert reader.cursors[FORUM_STAGE] == 0


def _paged_forum(per_page: int) -> Callable[[httpx.Request], httpx.Response]:
    """Stage 9926's forum with 3 comments, served ``per_page`` at a time whatever vPerPage asks for."""
    text = fixture("forum_stage_11256.html").replace("11256", "9926").replace("15 comments", "3 comments")
    starts = [m.start() for m in th._CBOX.finditer(text)]
    head, boxes = text[: starts[0]], [text[a:b] for a, b in zip(starts, [*starts[1:], len(text)], strict=True)]

    def handler(request: httpx.Request) -> httpx.Response:
        if not request.url.path.endswith("ViewComments.cfm"):
            return httpx.Response(200, text="")
        page = int(dict(x.split("=") for x in request.content.decode().split("&"))["vPage"])
        return httpx.Response(200, text=head + "".join(boxes[(page - 1) * per_page : page * per_page]))

    return handler


@pytest.mark.parametrize(("per_page", "requests"), [(1, 3), (2, 2), (50, 1)])
def test_forum_pages_until_header_count_is_reached(per_page, requests):
    clock = FakeClock()
    calls = []
    handler = _paged_forum(per_page)

    def site(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return handler(request)

    client = TownHallClient(
        client=httpx.Client(transport=httpx.MockTransport(site)), delay=0, sleep=clock.sleep, clock=clock
    )
    reader = VaTownHallReader(client=client, cursors={FORUM_STAGE: 0, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0})
    comments = list(reader._forum(FORUM_STAGE, 9926))
    assert [c["comment_id"] for c in comments] == [242807, 242845, 242860]
    assert len(calls) == requests
    assert reader.forum_count_mismatches == []


def test_incomplete_stored_forum_is_refetched():
    site = FakeSite()
    # Stage 9926 is stored with 6 comments, but its forum is not stored complete.
    known = KnownState(stages={9926: StoredStage("2999-01-01", 6, "Stage complete.")}, actions={6186})
    reader = _reader(site, known=known, cursors={FORUM_STAGE: 0, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0})
    records = list(reader.iter_records())
    assert {r["stage_id"] for r in records if r["kind"] == "comment"} == {9926}
    assert "ViewStage.cfm?stageid=9926" not in site.requests  # the stage page itself is not refetched


# --------------------------------------------------------------------------- #
# Build
# --------------------------------------------------------------------------- #


def test_known_leaves_out_stage_forums_with_fewer_comments_than_the_stage(tmp_path):
    stages, comments = tmp_path / "stages.parquet", tmp_path / "comments.parquet"
    pq.write_table(
        pa.table(
            {
                "stage_id": [1, 2],
                "fetched_at": ["2026-11-12T00:00:00Z"] * 2,
                "comment_count": [321, 2],
                "status": [None, None],
            }
        ),
        stages,
    )
    forum_ids = [1] * 50 + [2, 2, 7]
    pq.write_table(
        pa.table(
            {
                "forum_type": [FORUM_STAGE] * 52 + [FORUM_GENERAL_NOTICE],
                "forum_id": forum_ids,
            }
        ),
        comments,
    )
    known = build._known({STAGES_OUTPUT: stages, COMMENTS_OUTPUT: comments})
    assert known.forums == {(FORUM_STAGE, 2), (FORUM_GENERAL_NOTICE, 7)}


@pytest.fixture
def no_r2(monkeypatch):
    monkeypatch.setattr(build.r2, "download", lambda *_a, **_k: False)


def _rows(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()


def test_build_writes_three_tables_and_state(tmp_path, no_r2):
    site = FakeSite(forums=_FORUMS, in_progress=_IN_PROGRESS)
    reader = _reader(site, cursors={FORUM_STAGE: 0, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0})
    result = build_va_townhall(tmp_path, reader=reader)

    assert [p.name for p in result.tables] == [ACTIONS_OUTPUT, STAGES_OUTPUT, COMMENTS_OUTPUT]
    assert result.error is None
    stages = {r["stage_id"]: r for r in _rows(tmp_path / STAGES_OUTPUT)}
    assert set(stages) == {11305, 9926}
    assert stages[9926]["jurisdiction"] == "VA"
    assert stages[9926]["url"] == "https://townhall.virginia.gov/L/ViewStage.cfm?stageid=9926"
    assert json.loads(stages[11305]["documents_json"])[0]["title"] == "Proposed Text"
    actions = _rows(tmp_path / ACTIONS_OUTPUT)
    assert [a["action_id"] for a in actions] == [7067]
    assert json.loads(actions[0]["stage_ids_json"]) == [11246, 11305]
    comments = _rows(tmp_path / COMMENTS_OUTPUT)
    assert len(comments) == 3 and all(c["forum_type"] == "stage" for c in comments)
    assert comments[0]["url"].startswith("https://townhall.virginia.gov/L/ViewComments.cfm?commentid=")
    state = json.loads((tmp_path / STATE_KEY).read_text())
    assert state["cursors"] == {FORUM_STAGE: 0, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0}
    assert state["backfill_complete"] is True


def test_build_merges_over_prior_tables_newest_row_wins(tmp_path, no_r2):
    full = {FORUM_STAGE: 0, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0}
    build_va_townhall(tmp_path, reader=_reader(FakeSite(forums=_FORUMS, in_progress=_IN_PROGRESS), cursors=full))
    first = {r["stage_id"]: r["fetched_at"] for r in _rows(tmp_path / STAGES_OUTPUT)}

    # A second run that only sees the open forum refetches stage 11305 and keeps 9926.
    build_va_townhall(tmp_path, reader=_reader(FakeSite(forums=_FORUMS), cursors=full))
    stages = _rows(tmp_path / STAGES_OUTPUT)
    assert sorted(r["stage_id"] for r in stages) == [9926, 11305]  # no duplicates
    assert {r["stage_id"]: r["fetched_at"] for r in stages}[9926] == first[9926]
    assert len(_rows(tmp_path / COMMENTS_OUTPUT)) == 3


def test_build_resumes_cursor_from_state_file(tmp_path, no_r2, monkeypatch):
    (tmp_path / STATE_KEY).parent.mkdir(parents=True)
    (tmp_path / STATE_KEY).write_text(
        json.dumps({"cursors": {FORUM_STAGE: 100, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0}})
    )
    site = FakeSite()
    clock = FakeClock()
    client = TownHallClient(
        client=httpx.Client(transport=httpx.MockTransport(site)), delay=0, sleep=clock.sleep, clock=clock
    )
    # build_va_townhall constructs its own reader; route it to the fake site.
    original = build.VaTownHallReader
    monkeypatch.setattr(build, "VaTownHallReader", lambda **kwargs: original(client=client, **kwargs))
    build_va_townhall(tmp_path, max_minutes=60)
    assert site.requests[5] == "ViewStage.cfm?stageid=100"
    assert [r["stage_id"] for r in _rows(tmp_path / STAGES_OUTPUT)] == [100]


def test_build_reports_error_after_writing(tmp_path, no_r2):
    reader = _reader(
        FakeSite(in_progress=_IN_PROGRESS, fail_after=6),
        cursors={FORUM_STAGE: 0, FORUM_GENERAL_NOTICE: 0, FORUM_PETITION: 0},
    )
    result = build_va_townhall(tmp_path, reader=reader)
    assert result.error and "stopped early" in result.error
    assert [r["stage_id"] for r in _rows(tmp_path / STAGES_OUTPUT)] == [9926]


def test_rollup_uploads_tables_then_state_then_raises_on_error(tmp_path, monkeypatch):
    for name in (ACTIONS_OUTPUT, STAGES_OUTPUT, COMMENTS_OUTPUT):
        (tmp_path / name).write_bytes(b"x")
    (tmp_path / "state.json").write_text("{}")
    result = build.VaTownHallBuild(
        tables=[tmp_path / ACTIONS_OUTPUT, tmp_path / STAGES_OUTPUT, tmp_path / COMMENTS_OUTPUT],
        state_file=tmp_path / "state.json",
        error="crawl stopped early: boom",
    )
    uploads = []
    monkeypatch.setattr(rollup.VaTownHallRollup, "build_all", lambda self, _d: result)
    monkeypatch.setattr(rollup.r2, "upload_file", lambda path, remote_key, **_k: uploads.append(remote_key))

    with pytest.raises(RuntimeError, match="boom"):
        rollup.VaTownHallRollup(output_dir=tmp_path, skip_upload=False).run()
    assert uploads == [ACTIONS_OUTPUT, STAGES_OUTPUT, COMMENTS_OUTPUT, STATE_KEY]


def test_max_minutes_env(monkeypatch):
    monkeypatch.setenv("TOWNHALL_MAX_MINUTES", "2.5")
    assert rollup._max_minutes() == 2.5
    monkeypatch.setenv("TOWNHALL_MAX_MINUTES", "soon")
    with pytest.raises(ValueError):
        rollup._max_minutes()
