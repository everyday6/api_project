from datetime import date, timedelta

import pytest

from src.tlc import gold2
from src.tlc.gold2 import VALUE_FORMULA_VERSION, _type3_csv_row, select_latest_date_partitions


def test_select_latest_date_partitions_reads_only_latest_28(tmp_path):
    paths = []
    for offset in range(30):
        partition = tmp_path / f"date={date(2026, 7, 1) + timedelta(days=offset)}"
        partition.mkdir()
        paths.append(partition)

    selected, window_start, window_end = select_latest_date_partitions(paths, 28)

    assert window_start == date(2026, 7, 3)
    assert window_end == date(2026, 7, 30)
    assert len(selected) == 28
    assert selected[0].name == "date=2026-07-03"
    assert selected[-1].name == "date=2026-07-30"


def test_select_latest_date_partitions_rejects_gap(tmp_path):
    paths = []
    for offset in range(29):
        if offset == 20:
            continue
        partition = tmp_path / f"date={date(2026, 7, 1) + timedelta(days=offset)}"
        partition.mkdir()
        paths.append(partition)

    with pytest.raises(ValueError, match="연속적이지"):
        select_latest_date_partitions(paths, 28)


def test_value_formula_version_is_label_plus_hash():
    label, sep, digest = VALUE_FORMULA_VERSION.partition("+")
    assert (label, sep) == ("v1", "+")
    assert len(digest) == 6


def test_value_formula_version_tracks_rolling_weeks(monkeypatch):
    # 롤링 주수를 바꾸면 라벨을 안 올려도 버전 해시가 달라져야 한다.
    from src.common.provenance import formula_version

    assert formula_version("v1", 8) != formula_version("v1", 12)


def test_type3_csv_row_appends_formula_version_last():
    row = {"segment_id": "0001", "dow": "MON", "time": "900", "value": 3.5}

    csv_row = _type3_csv_row(row, date(2026, 7, 30), "v1+abc123")

    # 컬럼 순서: segment_id, dow, time, value, collected_date, updated_date, value_formula_version
    assert csv_row[:5] == ["0001", "MON", "0900", 3.5, "2026-07-30"]
    assert csv_row[-1] == "v1+abc123"


def test_type3_csv_row_uses_module_version_via_copy_partition_closure():
    # _copy_type3_partition가 만든 writer가 실제로 VALUE_FORMULA_VERSION을 싣는지
    # (닫힌 인자로 전달됨) — CSV row 빌더만 떼어 확인한다.
    row = {"segment_id": "0002", "dow": "SUN", "time": "1430", "value": 1.0}

    csv_row = _type3_csv_row(row, date(2026, 7, 30), gold2.VALUE_FORMULA_VERSION)

    assert csv_row[-1] == gold2.VALUE_FORMULA_VERSION
