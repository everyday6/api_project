from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from src.nav_time.gold1 import filter_recent_valid_speed


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("nav_time_gold1_test").getOrCreate()
    yield session
    session.stop()


def test_filter_excludes_old_readings(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 1, 12, 0)},  # 20일 전
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 20, 12, 0)},  # 1일 전
    ])

    result = filter_recent_valid_speed(df, as_of=datetime(2026, 8, 21, 12, 0), window_days=14).collect()

    assert len(result) == 1
    assert result[0]["observed_at"] == datetime(2026, 8, 20, 12, 0)


def test_filter_excludes_zero_or_negative_speed(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 0.0, "observed_at": datetime(2026, 8, 20, 12, 0)},
        {"segment_id": "1", "speed": 25.0, "observed_at": datetime(2026, 8, 20, 12, 0)},
    ])

    result = filter_recent_valid_speed(df, as_of=datetime(2026, 8, 21, 12, 0), window_days=14).collect()

    assert len(result) == 1
    assert result[0]["speed"] == 25.0
