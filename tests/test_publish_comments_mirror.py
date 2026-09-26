from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import publish_comments_mirror as mirror


def _index(path: Path, counts: dict[str | None, int]) -> Path:
    # Two index rows per agency (as per-docket/month rows would be) so the check
    # has to sum them.
    agencies, rows = [], []
    for agency, n in counts.items():
        agencies += [agency, agency]
        rows += [n - n // 2, n // 2]
    pq.write_table(pa.table({"agency_code": agencies, "row_count": rows}), path)
    return path


def test_missing_agencies_lists_dropped_codes(tmp_path):
    published = _index(tmp_path / "old.parquet", {"EPA": 10, "VA": 10, "SSA": 10, None: 5})
    new = _index(tmp_path / "new.parquet", {"EPA": 10, "NEW": 1})
    assert mirror.missing_agencies(published, new, set()) == ["SSA", "VA"]
    assert mirror.missing_agencies(published, new, {"VA"}) == ["SSA"]


def test_shrunken_agencies_flags_a_gutted_agency(tmp_path):
    # VA: the #197 stub. EPA: a normal dedupe trickle. OMB: small agency halved.
    published = _index(tmp_path / "old.parquet", {"VA": 386_000, "EPA": 500_000, "OMB": 800})
    new = _index(tmp_path / "new.parquet", {"VA": 12, "EPA": 499_600, "OMB": 100})
    assert mirror.shrunken_agencies(published, new, set()) == [("VA", 386_000, 12)]
    assert mirror.shrunken_agencies(published, new, {"VA"}) == []


def test_shrunken_agencies_ignores_absent_agencies(tmp_path):
    published = _index(tmp_path / "old.parquet", {"VA": 386_000})
    new = _index(tmp_path / "new.parquet", {"EPA": 10})
    assert mirror.shrunken_agencies(published, new, set()) == []


def _serve(monkeypatch, source: Path | None):
    def fake_download(remote_key, local_path):
        assert remote_key == "comments_index.parquet"
        if source is None:
            return False
        local_path.write_bytes(source.read_bytes())
        return True

    monkeypatch.setattr(mirror.r2, "download_from_r2", fake_download)


def test_check_agencies_blocks_a_dropped_agency(tmp_path, monkeypatch):
    _serve(monkeypatch, _index(tmp_path / "old.parquet", {"EPA": 10, "VA": 10}))
    new = _index(tmp_path / "new.parquet", {"EPA": 10})
    assert mirror.check_agencies(new, set(), set()) is False
    assert mirror.check_agencies(new, {"VA"}, set()) is True


def test_check_agencies_blocks_a_gutted_agency(tmp_path, monkeypatch):
    _serve(monkeypatch, _index(tmp_path / "old.parquet", {"VA": 386_000}))
    new = _index(tmp_path / "new.parquet", {"VA": 12})
    assert mirror.check_agencies(new, set(), set()) is False
    assert mirror.check_agencies(new, set(), {"VA"}) is True


def test_check_agencies_allows_growth(tmp_path, monkeypatch):
    _serve(monkeypatch, _index(tmp_path / "old.parquet", {"EPA": 10}))
    assert mirror.check_agencies(_index(tmp_path / "new.parquet", {"EPA": 12, "VA": 5}), set(), set()) is True


def test_check_agencies_passes_without_a_published_index(tmp_path, monkeypatch):
    _serve(monkeypatch, None)
    assert mirror.check_agencies(_index(tmp_path / "new.parquet", {"EPA": 10}), set(), set()) is True
