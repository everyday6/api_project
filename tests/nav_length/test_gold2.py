from datetime import date
from unittest.mock import patch

import pandas as pd

from src.nav_length import gold2

_TODAY = date(2026, 8, 24)


def test_to_serving_items_rounds_length_to_int():
    df = pd.DataFrame([{"segment_id": "1", "length_ft": 120.7}])

    items = gold2.to_serving_items(df, today=_TODAY)

    assert {
        "segment_id": "1",
        "value": 121,
        "updated_date": "2026-08-24",
    } in items


def test_to_serving_items_multiple_rows():
    df = pd.DataFrame([
        {"segment_id": "1", "length_ft": 100.0},
        {"segment_id": "2", "length_ft": 200.0},
    ])

    items = gold2.to_serving_items(df, today=_TODAY)

    assert len(items) == 3  # 세그먼트 2개 + GLOBAL 기본값 1개
    assert {
        "segment_id": "2",
        "value": 200,
        "updated_date": "2026-08-24",
    } in items


def test_to_serving_items_adds_global_row_with_median_length():
    df = pd.DataFrame([
        {"segment_id": "1", "length_ft": 100.0},
        {"segment_id": "2", "length_ft": 200.0},
        {"segment_id": "3", "length_ft": 300.0},
    ])

    items = gold2.to_serving_items(df, today=_TODAY)

    by_segment = {item["segment_id"]: item for item in items}
    assert by_segment["GLOBAL"]["value"] == 200
    assert by_segment["GLOBAL"]["updated_date"] == "2026-08-24"


def test_to_serving_items_empty_input_produces_no_global_row():
    df = pd.DataFrame([], columns=["segment_id", "length_ft"])

    items = gold2.to_serving_items(df, today=_TODAY)

    assert items == []


def test_write_to_rds_calls_replace_table_snapshot_and_returns_count():
    items = [{"segment_id": "1", "value": 100}]

    with patch.object(gold2, "replace_table_snapshot") as mock_write, \
         patch.object(gold2, "gold_snapshot"):
        count = gold2.write_to_rds(items, "SegmentMetricsType2")

    mock_write.assert_called_once_with(
        "SegmentMetricsType2",
        items,
        key_columns=("segment_id",),
    )
    assert count == 1


def test_write_to_rds_exports_snapshot_including_global_row():
    items = [
        {"segment_id": "1", "value": 100},
        {"segment_id": "2", "value": 200},
        {"segment_id": "GLOBAL", "value": 150},
    ]

    with patch.object(gold2, "replace_table_snapshot"), \
         patch.object(gold2.gold_snapshot, "write_snapshot") as mock_snapshot:
        gold2.write_to_rds(items, "SegmentMetricsType2")

    mock_snapshot.assert_called_once_with("type2", {"1": 100, "2": 200, "GLOBAL": 150})


def test_write_to_rds_survives_snapshot_export_failure():
    items = [{"segment_id": "1", "value": 100}]

    with patch.object(gold2, "replace_table_snapshot"), \
         patch.object(gold2.gold_snapshot, "write_snapshot", side_effect=RuntimeError("S3 down")):
        count = gold2.write_to_rds(items, "SegmentMetricsType2")

    assert count == 1
