from datetime import date, timedelta

import pytest

from src.tlc.gold2 import select_latest_date_partitions


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
