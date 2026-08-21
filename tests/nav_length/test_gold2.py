from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession

from src.nav_length import gold2


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("nav_length_gold2_test").getOrCreate()
    yield session
    session.stop()


def test_to_dynamodb_items_rounds_length_to_int(spark):
    df = spark.createDataFrame([{"segment_id": "1", "length_ft": 120.7}])

    items = gold2.to_dynamodb_items(df)

    assert items == [{"segment_id": "1", "sk": "LENGTH", "value": 121}]


def test_to_dynamodb_items_multiple_rows(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "length_ft": 100.0},
        {"segment_id": "2", "length_ft": 200.0},
    ])

    items = gold2.to_dynamodb_items(df)

    assert len(items) == 2
    assert {"segment_id": "2", "sk": "LENGTH", "value": 200} in items


def test_write_to_dynamodb_calls_batch_write_and_returns_count():
    items = [{"segment_id": "1", "sk": "LENGTH", "value": 100}]

    with patch.object(gold2, "batch_write_items") as mock_write:
        count = gold2.write_to_dynamodb(items, "SegmentMetricsType2")

    mock_write.assert_called_once_with("SegmentMetricsType2", items)
    assert count == 1
