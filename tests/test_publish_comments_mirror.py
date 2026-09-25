from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import publish_comments_mirror as mirror


def _index(path: Path, agencies: list[str | None]) -> Path:
    pq.write_table(pa.table({"agency_code": agencies, "row_count": [1] * len(agencies)}), path)
    return path


def test_missing_agencies_lists_dropped_codes(tmp_path):
    published = _index(tmp_path / "old.parquet", ["EPA", "VA", "VA", "SSA", None])
    new = _index(tmp_path / "new.parquet", ["EPA", "NEW"])
    assert mirror.missing_agencies(published, new, set()) == ["SSA", "VA"]
    assert mirror.missing_agencies(published, new, {"VA"}) == ["SSA"]


def _serve(monkeypatch, source: Path | None):
    def fake_download(remote_key, local_path):
        assert remote_key == "comments_index.parquet"
        if source is None:
            return False
        local_path.write_bytes(source.read_bytes())
        return True

    monkeypatch.setattr(mirror.r2, "download_from_r2", fake_download)


def test_check_agency_set_blocks_a_shrunken_export(tmp_path, monkeypatch):
    _serve(monkeypatch, _index(tmp_path / "old.parquet", ["EPA", "VA"]))
    new = _index(tmp_path / "new.parquet", ["EPA"])
    assert mirror.check_agency_set(new, set()) is False
    assert mirror.check_agency_set(new, {"VA"}) is True


def test_check_agency_set_allows_growth(tmp_path, monkeypatch):
    _serve(monkeypatch, _index(tmp_path / "old.parquet", ["EPA"]))
    assert mirror.check_agency_set(_index(tmp_path / "new.parquet", ["EPA", "VA"]), set()) is True


def test_check_agency_set_passes_without_a_published_index(tmp_path, monkeypatch):
    _serve(monkeypatch, None)
    assert mirror.check_agency_set(_index(tmp_path / "new.parquet", ["EPA"]), set()) is True
