from datetime import date
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession

from src.nav_length import gold2

_TODAY = date(2026, 8, 24)


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("nav_length_gold2_test").getOrCreate()
    yield session
    session.stop()


def test_to_serving_items_rounds_length_to_int(spark):
    df = spark.createDataFrame([{"segment_id": "1", "length_ft": 120.7}])

    items = gold2.to_serving_items(df, today=_TODAY)

    assert items == [
        {
            "segment_id": "1",
            "value": 121,
            "collected_date": "2026-08-24",
            "updated_date": "2026-08-24",
        }
    ]


def test_to_serving_items_multiple_rows(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "length_ft": 100.0},
        {"segment_id": "2", "length_ft": 200.0},
    ])

    items = gold2.to_serving_items(df, today=_TODAY)

    assert len(items) == 2
    assert {
        "segment_id": "2",
        "value": 200,
        "collected_date": "2026-08-24",
        "updated_date": "2026-08-24",
    } in items


def test_write_to_rds_calls_batch_write_and_returns_count():
    items = [{"segment_id": "1", "value": 100}]

    with patch.object(gold2, "batch_write_items") as mock_write:
        count = gold2.write_to_rds(items, "SegmentMetricsType2")

    mock_write.assert_called_once_with(
        "SegmentMetricsType2",
        items,
        key_columns=("segment_id",),
    )
    assert count == 1
