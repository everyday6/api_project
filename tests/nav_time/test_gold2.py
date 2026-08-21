from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.nav_time import gold2


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("nav_time_gold2_test").getOrCreate()
    yield session
    session.stop()


def test_compute_time_seconds_uses_length_and_speed(spark):
    # 길이 5280ft(1마일)를 30mph로 -> 1/30시간 = 120초
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert len(result) == 1
    assert result[0]["bucket"] == "1200"
    assert abs(result[0]["time_seconds"] - 120.0) < 0.01


def test_compute_time_seconds_buckets_to_30_minutes(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 47)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert result[0]["bucket"] == "1230"


def test_to_dynamodb_items_includes_bucket_and_avg(spark):
    bucket_df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0},
        {"segment_id": "1", "bucket": "1230", "time_seconds": 50.0},
    ])

    items = gold2.to_dynamodb_items(bucket_df)

    by_sk = {(i["segment_id"], i["sk"]): i["value"] for i in items}
    assert by_sk[("1", "1200")] == 30
    assert by_sk[("1", "1230")] == 50
    assert by_sk[("1", "AVG")] == 40  # (30+50)/2


def test_compute_time_seconds_excludes_zero_speed_segment(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
        {"segment_id": "2", "speed": 0.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_length_df = pd.DataFrame([
        {"segment_id": "1", "length_ft": 5280.0},
        {"segment_id": "2", "length_ft": 5280.0},
    ])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert len(result) == 1
    assert result[0]["segment_id"] == "1"


def test_write_to_dynamodb_calls_batch_write_and_returns_count():
    items = [{"segment_id": "1", "sk": "1200", "value": 30}]

    with patch.object(gold2, "batch_write_items") as mock_write:
        count = gold2.write_to_dynamodb(items, "SegmentMetricsType1")

    mock_write.assert_called_once_with("SegmentMetricsType1", items)
    assert count == 1
